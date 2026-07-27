mod common;

use ferro_proto::consts::{TYPE_REGISTRY_HASH, errc, flags, method_core, service};
use ferro_proto::messages::Outcome;
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::peercred::peer_uid;
use ferrod::shutdown::Drain;

#[tokio::test]
async fn peer_uid_on_socketpair_returns_current_uid() {
    let (a, _b) = tokio::net::UnixStream::pair().expect("socketpair");
    let uid = peer_uid(&a).expect("peer_uid should succeed on a fresh socketpair");
    assert_eq!(uid, nix::unistd::getuid().as_raw());
}

#[test]
fn uid_allowed_empty_list_is_self_only() {
    let cfg = Config {
        peer_allow_uids: vec![],
        ..Config::default()
    };
    let self_uid = nix::unistd::getuid().as_raw();
    assert!(cfg.uid_allowed(self_uid));
    assert!(!cfg.uid_allowed(self_uid + 1));
}

#[test]
fn uid_allowed_explicit_list() {
    let cfg = Config {
        peer_allow_uids: vec![4242],
        ..Config::default()
    };
    assert!(cfg.uid_allowed(4242));
    let self_uid = nix::unistd::getuid().as_raw();
    if self_uid != 4242 {
        assert!(!cfg.uid_allowed(self_uid));
    }
}

// tokio::net::UnixListener::bind registers the fd with the tokio reactor, so it must run
// inside an active runtime.
#[tokio::test]
async fn bind_uds_unlinks_stale_socket() {
    let dir = tempdir();
    let path = dir.join("ferro-test.sock");
    std::fs::write(&path, b"stale").expect("write dummy stale file");

    let cfg = Config {
        socket_path: path.clone(),
        ..Config::default()
    };
    let listener = ferrod::listener::bind_uds(&cfg).expect("bind_uds should unlink stale socket");
    drop(listener);
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------------------------
// S3 Task 7 (M10): the end-to-end peercred-DENY path, through the REAL `serve` accept loop --
// not `Session::run` directly. `SO_PEERCRED` on a `UnixStream::pair`/`connect` always reports the
// TEST'S OWN uid, so an allow-list that EXCLUDES the current uid deterministically exercises the
// deny branch without needing to actually connect as a different user.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn peercred_deny_rejects_connection() {
    let self_uid = nix::unistd::getuid().as_raw();
    let config = Config {
        // Excludes the current uid -- SO_PEERCRED on this connection will report `self_uid`,
        // which is deliberately not in this list, so `serve` must reject it.
        peer_allow_uids: vec![self_uid + 1],
        ..Config::default()
    };
    let drain = Drain::new();
    let handler = ferrod::session::default_handler_fn();
    let (socket_path, _served) =
        common::spawn_serve_with_config(config, BootEpoch(1), drain, handler);

    let mut client = common::connect(&socket_path).await;

    // Send HELLO -- the send itself may fail (the server may have already dropped its side of
    // the socket right after sending its deny frame, causing a broken pipe): that is ALSO fine,
    // not a test bug -- the deny path never reads anything from the client, so whether this send
    // itself succeeds is incidental to what the test actually asserts below.
    let _ = client
        .try_send(common::TestClient::hello_out_frame(1, TYPE_REGISTRY_HASH))
        .await;

    // A peercred deny is session-fatal (SPEC G-4), never a silent close: the server sends exactly
    // one `rid=0, flags=END, Outcome::Error{code: AUTH}` frame -- and, in particular, never a
    // HELLO_ACK -- before the connection closes.
    let frame = client.recv().await;
    assert_eq!(frame.header.request_id, 0, "peercred deny must use rid=0");
    assert_eq!(frame.header.flags, flags::END);
    assert_eq!(frame.header.service, service::CORE);
    assert_ne!(
        frame.header.method,
        method_core::HELLO_ACK,
        "a peercred deny must never look like a HELLO_ACK"
    );
    match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::AUTH),
        other => panic!("expected Outcome::Error(AUTH), got {other:?}"),
    }

    // Nothing else follows. The client's own un-read HELLO bytes may still be sitting in the
    // kernel's receive buffer when the server drops its side, so the close can surface as either
    // a clean EOF or a reset (`ECONNRESET`) -- both mean "nothing else was ever sent" here, which
    // is exactly what `recv_or_none` treats alike (see its own doc comment).
    assert!(
        client
            .recv_or_none(std::time::Duration::from_millis(300))
            .await
            .is_none(),
        "expected nothing after the one deny frame"
    );
}

/// A unique temp directory under the OS temp dir, cleaned up best-effort by the OS.
fn tempdir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "ferrod-peercred-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).expect("create temp dir");
    p
}
