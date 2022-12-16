#![feature(test)]

extern crate test;

use bytes::Bytes;
use futures::prelude::*;
use gush::Gush;
use http_body_util::{BodyExt, Empty, StreamBody};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use ipld_traversal::{blockstore::MemoryBlockstore, LinkSystem, Prefix};
use libipld::{ipld, Cid, Ipld};
use rand::prelude::*;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

macro_rules! bench_server {
    ($b:ident, $content:expr) => {{
        let (_until_tx, until_rx) = oneshot::channel::<()>();

        let (store, root) = $content();

        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt build"),
        );
        let exec = rt.clone();

        let addr = {
            let (addr_tx, addr_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let addr: SocketAddr = "127.0.0.1:1338".parse().unwrap();

                let listener = rt.block_on(TcpListener::bind(addr)).unwrap();
                let addr = listener.local_addr().unwrap();

                rt.spawn(async move {
                    loop {
                        let (stream, _) = listener.accept().await.expect("accept");

                        http1::Builder::new()
                            .serve_connection(stream, Gush::new(store.clone()))
                            .await
                            .unwrap();
                    }
                });

                addr_tx.send(addr).unwrap();
                rt.block_on(until_rx).ok();
            });

            addr_rx.recv().unwrap()
        };

        let send_request = || async {
            let stream = TcpStream::connect(addr).await.unwrap();

            let (sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

            exec.spawn(async move {
                if let Err(err) = conn.await {
                    println!("Connection error: {:?}", err);
                }
            });

            let mut client =
                gush::Client::new(sender, "http://127.0.0.1:1338", MemoryBlockstore::new());

            client.fetch_into_store(root).await.unwrap()
        };

        let total_bytes = exec.block_on(send_request());

        $b.bytes = 35 + total_bytes as u64;
        $b.iter(|| {
            exec.block_on(send_request());
        });
    }};
}

fn chunks(size: usize) -> (MemoryBlockstore, Cid) {
    let mut data = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut data);

    const CHUNK_SIZE: usize = 250 * 1024;

    let store = MemoryBlockstore::new();
    let lsys = LinkSystem::new(store.clone());
    let chunks = data.chunks(CHUNK_SIZE);

    let links: Vec<Ipld> = chunks
        .map(|chunk| {
            let leaf = Ipld::Bytes(chunk.to_vec());
            // encoding as raw
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

    (store, root)
}

#[bench]
fn throughput_chunked_small_payload(b: &mut test::Bencher) {
    bench_server!(b, || chunks(1024 * 1024))
}

#[bench]
fn throughput_chunked_mid_payload(b: &mut test::Bencher) {
    bench_server!(b, || chunks(10 * 1024 * 1024))
}
// #[bench]
// fn throughput_regular_http(b: &mut test::Bencher) {
//     static S: &[&[u8]] = &[&[b'x'; 250 * 1024] as &[u8]; 40] as _;

//     let (_until_tx, until_rx) = oneshot::channel::<()>();

//     let rt = Arc::new(
//         tokio::runtime::Builder::new_current_thread()
//             .enable_all()
//             .build()
//             .expect("rt build"),
//     );
//     let exec = rt.clone();

//     let addr = {
//         let (addr_tx, addr_rx) = mpsc::channel();
//         std::thread::spawn(move || {
//             let addr: SocketAddr = "127.0.0.1:1339".parse().unwrap();

//             let listener = rt.block_on(TcpListener::bind(addr)).unwrap();
//             let addr = listener.local_addr().unwrap();

//             rt.spawn(async move {
//                 loop {
//                     let (stream, _) = listener.accept().await.expect("accept");

//                     http1::Builder::new()
//                         .serve_connection(
//                             stream,
//                             service_fn(|_| async {
//                                 Ok::<_, hyper::Error>(
//                                     Response::builder()
//                                         .header("transfer-encoding", "chunked")
//                                         .header("content-type", "text/plain")
//                                         .body(BodyExt::boxed(StreamBody::new(
//                                             stream::iter(S.iter())
//                                                 .map(|&s| Ok::<_, String>(Frame::data(s))),
//                                         )))
//                                         .unwrap(),
//                                 )
//                             }),
//                         )
//                         .await
//                         .unwrap();
//                 }
//             });

//             addr_tx.send(addr).unwrap();
//             rt.block_on(until_rx).ok();
//         });

//         addr_rx.recv().unwrap()
//     };

//     let send_request = || async {
//         let stream = TcpStream::connect(addr).await.unwrap();

//         let (mut sender, conn) = hyper::client::conn::http1::handshake(stream).await.unwrap();

//         exec.spawn(async move {
//             if let Err(err) = conn.await {
//                 println!("Connection error: {:?}", err);
//             }
//         });

//         let req = Request::builder()
//             .method(Method::GET)
//             .uri("http://127.0.0.1:1338")
//             .body(Empty::<Bytes>::new())
//             .unwrap();

//         let res = sender.send_request(req).await.unwrap();
//         BodyExt::collect(res.into_body())
//             .await
//             .unwrap()
//             .to_bytes()
//             .len()
//     };

//     let total_bytes = exec.block_on(send_request());

//     b.bytes = 35 + total_bytes as u64;
//     b.iter(|| {
//         exec.block_on(send_request());
//     });
// }
