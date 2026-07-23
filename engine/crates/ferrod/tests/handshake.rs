//! S3 Task 3: HELLO/HELLO_ACK handshake integration tests, against a real bound `UnixListener`
//! (see `tests/common`). Every `recv()` is timeout-guarded, so a hang here is a real bug, not a
//! flaky CI wait.

mod common;

use common::TestServer;
use ferro_proto::consts::{TYPE_REGISTRY_HASH, errc};
use ferro_proto::messages::Outcome;
use ferrod::epoch::{BootEpoch, EpochSource, RandomEpoch};

#[tokio::test]
async fn hello_ack_roundtrips_and_echoes_epoch_and_request_id() {
    let server = TestServer::spawn(BootEpoch(0xABCD));
    let mut client = server.connect().await;

    let result = client.hello(1).await;

    assert_eq!(result.request_id, 1);
    assert_eq!(result.ack.boot_epoch, 0xABCD);
    assert_eq!(result.ack.type_registry_hash, TYPE_REGISTRY_HASH);
}

#[tokio::test]
async fn non_hello_first_frame_is_fatal() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;

    // PING sent as the very first frame, before any HELLO.
    client.ping(9, 7).await;

    let frame = client.recv().await;
    assert_eq!(frame.header.request_id, 0);
    assert_eq!(frame.header.flags, ferro_proto::consts::flags::END);
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    // Exactly one frame, then the connection closes.
    client.recv_eof().await;
}

#[tokio::test]
async fn wrong_type_registry_hash_is_fatal_unsupported() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;

    client.send_hello(1, "not-the-real-hash").await;

    let frame = client.recv().await;
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    client.recv_eof().await;
}

#[tokio::test]
async fn two_connections_same_instance_get_same_epoch() {
    // A real getrandom-backed draw, taken exactly once — mirroring how `main` draws it once per
    // process and hands the same value to every accepted connection (SPEC §19.1).
    let epoch = RandomEpoch.epoch();
    let server = TestServer::spawn(epoch);

    let mut client_a = server.connect().await;
    let mut client_b = server.connect().await;

    let ack_a = client_a.hello(1).await.ack;
    let ack_b = client_b.hello(1).await.ack;

    assert_eq!(ack_a.boot_epoch, ack_b.boot_epoch);
    assert_eq!(ack_a.boot_epoch, epoch.0);
}

#[tokio::test]
async fn ping_after_hello_gets_pong() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;
    client.hello(1).await;

    client.ping(9, 7).await;

    let frame = client.recv().await;
    assert_eq!(frame.header.service, ferro_proto::consts::service::CORE);
    assert_eq!(frame.header.method, ferro_proto::consts::method_core::PONG);
    assert_eq!(frame.header.request_id, 9);
    let pong = ferro_proto::messages::Pong::decode(&frame.payload).expect("decode PONG");
    assert_eq!(pong.token, 7);
}
