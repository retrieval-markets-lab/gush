use anyhow::Result;
use asynchronous_codec::{Decoder, Encoder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body_util::{BodyExt, Either, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::service::Service;
use hyper::{
    body::{Body, Frame, Incoming as IncomingBody},
    Method, Request, Response,
};
use ipld_traversal::{
    blockstore::Blockstore, selector::RecursionLimit, unixfs::unixfs_path_selector, BlockLoader,
    BlockTraversal, IterError, LinkSystem, Prefix, Selector,
};
use libipld::Cid;
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_reader, from_slice, to_vec};
use serde_tuple::*;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use unsigned_varint::codec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GushRequest {
    root: Cid,
    selector: Option<Selector>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct GushBlock<'a> {
    #[serde(with = "serde_bytes")]
    pub prefix: &'a [u8],
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

impl GushRequest {
    pub fn select_all(root: Cid) -> Self {
        Self {
            root,
            selector: Some(Selector::ExploreRecursive {
                limit: RecursionLimit::None,
                sequence: Box::new(Selector::ExploreAll {
                    next: Box::new(Selector::ExploreRecursiveEdge),
                }),
                current: None,
            }),
        }
    }
}

impl FromStr for GushRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Some((root, selector)) = unixfs_path_selector(s.into()) {
            Ok(GushRequest {
                root,
                selector: Some(selector),
            })
        } else {
            Err(anyhow::format_err!("invalid unixfs path"))
        }
    }
}

pub struct Gush<S: Blockstore> {
    store: S,
}

impl<S: Blockstore> Gush<S> {
    pub fn new(store: S) -> Self {
        Gush { store }
    }
}

impl<S: Blockstore + Clone + Send + Sync + 'static> Service<Request<IncomingBody>> for Gush<S> {
    type Response = Response<Either<TraversalBody<S>, Full<Bytes>>>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, req: Request<IncomingBody>) -> Self::Future {
        let lsys = LinkSystem::new(self.store.clone());

        Box::pin(async move {
            let whole_body = req.collect().await?.aggregate();
            let greq: GushRequest = from_reader(whole_body.reader())?;

            if let Some(selector) = greq.selector {
                return Ok(Response::builder()
                    .body(Either::Left(TraversalBody::new(lsys, greq.root, selector)))
                    .unwrap());
            }
            let (_, data) = lsys.load_plus_raw(greq.root)?;

            Ok(Response::builder()
                .body(Either::Right(Full::new(Bytes::from(data))))
                .unwrap())
        })
    }
}

pin_project! {
    pub struct TraversalBody<S: Blockstore> {
        inner: BlockTraversal<LinkSystem<S>>,
        length_codec: codec::UviBytes,
    }
}

impl<S: Blockstore> TraversalBody<S> {
    pub fn new(lsys: LinkSystem<S>, root: Cid, selector: Selector) -> Self {
        let it = BlockTraversal::new(lsys, root, selector);
        let length_codec = codec::UviBytes::default();
        TraversalBody {
            inner: it,
            length_codec,
        }
    }
}

impl<S: Blockstore> Body for TraversalBody<S> {
    type Data = Bytes;
    type Error = IterError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.inner.next() {
            Some(result) => Poll::Ready(Some(result.and_then(|(cid, data)| {
                let prefix = Prefix::from(cid).to_bytes();
                let blk = GushBlock {
                    prefix: &prefix[..],
                    data: &data[..],
                };
                let buf = to_vec(&blk).map_err(|e| IterError::Decode(e.to_string()))?;
                let mut dest = BytesMut::new();
                self.length_codec
                    .encode(Bytes::from(buf), &mut dest)
                    .map_err(|e| IterError::Decode(e.to_string()))?;
                Ok(Frame::data(Bytes::from(dest.freeze())))
            }))),
            None => Poll::Ready(None),
        }
    }
}

pub struct Client<S: Blockstore> {
    sender: SendRequest<Full<Bytes>>,
    addr: &'static str,
    store: S,
}

impl<S: Blockstore + Clone> Client<S> {
    pub fn new(sender: SendRequest<Full<Bytes>>, addr: &'static str, store: S) -> Self {
        Client {
            sender,
            addr,
            store,
        }
    }
    pub async fn fetch_into_store(&mut self, root: Cid) -> Result<usize> {
        let greq = GushRequest::select_all(root);

        let req = Request::builder()
            .method(Method::GET)
            .uri(self.addr)
            .body(Full::new(Bytes::from(to_vec(&greq).unwrap())))
            .unwrap();

        let mut res = self.sender.send_request(req).await.unwrap();

        let mut length_codec: codec::UviBytes = codec::UviBytes::default();
        let mut buf = BytesMut::new();
        let mut size = 0;

        let store = self.store.clone();

        while let Some(next) = res.frame().await {
            if let Some(chunk) = next.unwrap().data_ref() {
                buf.put(&chunk[..]);

                if let Some(packet) = length_codec.decode(&mut buf).unwrap() {
                    let pack = packet.freeze();
                    let blk: GushBlock = from_slice(&pack).unwrap();
                    let prefix = Prefix::new_from_bytes(blk.prefix).unwrap();
                    let cid = prefix.to_cid(blk.data).unwrap();

                    size += blk.data.len();

                    store.put_keyed(&cid, blk.data)?;
                }
            }
        }

        Ok(size)
    }

    pub async fn fetch_raw(&mut self, root: Cid) -> Result<usize> {
        let greq = GushRequest::select_all(root);

        let req = Request::builder()
            .method(Method::GET)
            .uri(self.addr)
            .body(Full::new(Bytes::from(to_vec(&greq).unwrap())))
            .unwrap();

        let mut res = self.sender.send_request(req).await.unwrap();

        let mut buf = BytesMut::with_capacity(1024);
        let mut size = 0;

        while let Some(next) = res.frame().await {
            if let Some(chunk) = next.unwrap().data_ref() {
                buf.put(&chunk[..]);
                size += chunk.len();
            }
        }

        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;
    use http_body_util::{BodyExt, Empty, StreamBody};
    use hyper::server::conn::http1;
    use ipld_traversal::{blockstore::MemoryBlockstore, Prefix};
    use libipld::{ipld, Ipld};
    use rand::prelude::*;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn test_protocol() {
        let store = MemoryBlockstore::new();
        let lsys = LinkSystem::new(store.clone());

        const CHUNK_SIZE: usize = 250 * 1024;

        let mut bytes = vec![0u8; 3 * CHUNK_SIZE];
        thread_rng().fill(&mut bytes[..]);

        let chunks = bytes.chunks(CHUNK_SIZE);

        let links: Vec<Ipld> = chunks
            .map(|chunk| {
                let leaf = Ipld::Bytes(chunk.to_vec());
                let cid = lsys
                    .store(Prefix::new(0x55, 0x13), &leaf)
                    .expect("link system should store leaf node");
                let link = ipld!({
                    "Hash": cid,
                    "Tsize": CHUNK_SIZE,
                });
                link
            })
            .collect();

        let root_node = ipld!({
            "Links": links,
        });

        let root = lsys
            .store(Prefix::new(0x71, 0x13), &root_node)
            .expect("link system to store root node");

        let addr: SocketAddr = ([127, 0, 0, 1], 1337).into();

        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Listening on http://{}", addr);

        let server = tokio::task::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, Gush::new(store.clone()))
                .await
            {
                println!("Failed to serve connection: {:?}", err);
            }
        });

        let stream = TcpStream::connect(addr).await.unwrap();

        let (sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                println!("Connection error: {:?}", err);
            }
        });

        let mut client = Client::new(sender, "http://127.0.0.1:1337", MemoryBlockstore::new());

        let size = client.fetch_into_store(root).await.unwrap();

        assert_eq!(size, CHUNK_SIZE * 3 + 278);

        server.abort();
        server.await.ok();
    }

    #[tokio::test]
    async fn test_regular_http() {
        static S: &[&[u8]] = &[&[b'x'; 250 * 1024] as &[u8]; 40] as _;

        let addr: SocketAddr = "127.0.0.1:1339".parse().unwrap();

        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");

                http1::Builder::new()
                    .serve_connection(
                        stream,
                        hyper::service::service_fn(|_| async {
                            Ok::<_, hyper::Error>(
                                Response::builder()
                                    .header("transfer-encoding", "chunked")
                                    .header("content-type", "text/plain")
                                    .body(BodyExt::boxed(StreamBody::new(
                                        futures::stream::iter(S.iter())
                                            .map(|&s| Ok::<_, String>(Frame::data(s))),
                                    )))
                                    .unwrap(),
                            )
                        }),
                    )
                    .await
                    .unwrap();
            }
        });

        let stream = TcpStream::connect(addr).await.unwrap();

        let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

        tokio::spawn(async move {
            if let Err(err) = conn.await {
                println!("Connection error: {:?}", err);
            }
        });

        let req = Request::builder()
            .method(Method::GET)
            .uri("http://127.0.0.1:1339")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let mut res = sender.send_request(req).await.unwrap();
        while let Some(next) = res.frame().await {
            if let Some(chunk) = next.unwrap().data_ref() {
                println!("chunk size {}", chunk.len());
            }
        }
    }
}
