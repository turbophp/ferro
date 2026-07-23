use ferrod::config::Config;
use ferrod::peercred::peer_uid;

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
