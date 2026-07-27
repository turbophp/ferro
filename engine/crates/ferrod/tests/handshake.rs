//! S3 Task 3: HELLO/HELLO_ACK handshake integration tests, against a real bound `UnixListener`
//! (see `tests/common`). Every `recv()` is timeout-guarded, so a hang here is a real bug, not a
//! flaky CI wait.

mod common;

use std::time::Duration;

use common::TestServer;
use ferro_proto::consts::{
    MAX_FRAME_PAYLOAD, TYPE_REGISTRY_HASH, errc, flags, method_core, service,
};
use ferro_proto::header::Header;
use ferro_proto::messages::Outcome;
use ferrod::config::Config;
use ferrod::epoch::{BootEpoch, EpochSource, RandomEpoch};
use ferrod::session::default_handler_fn;

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

// ---------------------------------------------------------------------------------------------
// S3 fix pass, MAJOR 1: a codec-faulted FIRST frame must produce the same rid=0 fatal `END` a
// later frame would, not a silent close.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn codec_fault_as_first_frame_is_fatal_not_silent_close() {
    // A bare, oversize-`payload_len` header written as the CONNECTION'S VERY FIRST bytes (no
    // HELLO ever sent) -- before the S3 fix pass this hit the first-frame match's
    // `Some(Err(_)) | None => { ...silent close... }` arm, dropping the connection with no reply
    // at all. It must now behave exactly like `session_rules.rs`'s `oversize_payload_len_is_fatal`
    // does for a LATER frame: one rid=0 `Outcome::Error(PROTOCOL)` `END`, then close.
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;

    let header = Header {
        flags: 0,
        service: service::CORE,
        method: method_core::HELLO,
        request_id: 1,
        payload_len: MAX_FRAME_PAYLOAD + 1,
    };
    client.send_raw_bytes(&header.encode()).await;

    let fatal = client.recv().await;
    assert_eq!(fatal.header.request_id, 0);
    assert_eq!(fatal.header.flags, flags::END);
    match Outcome::decode(&fatal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    client.recv_eof().await;
}

// ---------------------------------------------------------------------------------------------
// S3 fix pass, m2: the HELLO first frame's flags ARE validated (falls out of the MAJOR 1 fix,
// since the first frame is now routed through the same `classify` that runs `flags::validate`).
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn reserved_flag_on_hello_first_frame_is_fatal() {
    let server = TestServer::spawn(BootEpoch(1));
    let mut client = server.connect().await;

    let hello = common::TestClient::hello_out_frame(1, TYPE_REGISTRY_HASH);
    client
        .send(ferrod::session::codec::OutFrame {
            header: Header {
                flags: flags::OOB_FD,
                ..hello.header
            },
            payload: hello.payload,
        })
        .await;

    let fatal = client.recv().await;
    assert_eq!(fatal.header.request_id, 0);
    assert_eq!(fatal.header.flags, flags::END);
    match Outcome::decode(&fatal.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::UNSUPPORTED),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }

    client.recv_eof().await;
}

// ---------------------------------------------------------------------------------------------
// S3 fix pass, MAJOR 2: a peer that never sends HELLO at all must be dropped at
// `config.handshake_timeout`, not pinned forever.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn handshake_timeout_drops_silent_peer() {
    let config = Config {
        handshake_timeout: Duration::from_millis(150),
        ..Config::default()
    };
    let (socket_path, session) =
        common::spawn_one_session_with_config(config, BootEpoch(1), default_handler_fn());
    let mut client = common::connect(&socket_path).await;

    // Never send HELLO -- the peer just sits there silently, as a peercred-passing-but-otherwise-
    // idle client would.
    tokio::time::timeout(Duration::from_secs(2), session)
        .await
        .expect("a silent peer must be dropped at handshake_timeout, not hang forever")
        .expect("session task must not panic");

    // Dropped silently -- no reply frame at all, since there was never a valid HELLO to fail.
    client.recv_eof().await;
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
