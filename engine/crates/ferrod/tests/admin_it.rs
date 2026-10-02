//! **M2-C3-7b — the ADMIN service's `BACKUP` verb, end to end through a real `ferrod` session over a
//! real Unix socket (SPEC §7.6, D14, D15).**
//!
//! What only a daemon test can show, each with its control:
//!
//! * **D15's gate, on the kernel-attested uid.** The test process IS the peer, so its own uid is what
//!   `SO_PEERCRED` attests. `FERRO_ADMIN_UIDS` empty → `Forbidden`; a list WITHOUT that uid →
//!   `Forbidden`; a list WITH it → the snapshot is taken. Same request, same socket, only the
//!   engine's configuration differs.
//! * **The gate runs BEFORE the handler.** A refused peer naming a pool that does not exist is told
//!   `Forbidden`, not `Unsupported("unknown pool")` — so a refused verb never reaches pool lookup,
//!   checkout or the filesystem, and a non-admin cannot even probe which pools exist.
//! * **Exactly one END per refusal**, and the session survives it.
//! * **The destination policy and atomic finalisation**, observed on the filesystem.
//!
//! SQLite needs no server, so all but one test run everywhere; the non-SQLite refusal uses whichever
//! server backend is configured and runs in CI's integration lane.

mod common;

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::{TestClient, TestServer, assert_session_alive};
use ferro_proto::consts::{branch, errc, flags, method_admin, service};
use ferro_proto::messages::admin::{BackupRequest, BackupResponse};
use ferro_proto::messages::{ErrorPayload, Outcome};
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

/// A daemon with the given pools and `FERRO_ADMIN_UIDS`.
fn admin_server(pools: &[(&str, String)], admin_uids: Vec<u32>) -> TestServer {
    admin_server_allowing(pools, admin_uids, None)
}

/// [`admin_server`] with every pool's D14 `allow_dir` set to `allow_dir`.
fn admin_server_allowing(
    pools: &[(&str, String)],
    admin_uids: Vec<u32>,
    allow_dir: Option<&Path>,
) -> TestServer {
    let config = Config {
        admin_uids,
        pools: pools
            .iter()
            .map(|(name, dsn)| PoolSpec {
                name: (*name).to_string(),
                dsn: dsn.clone(),
                kind: ferrod::config::infer_pool_kind(dsn),
                pin_functions: Vec::new(),
                pin_on_unknown: true,
                allow_dir: allow_dir.map(|d| d.display().to_string()),
            })
            .collect(),
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory_and_config(BootEpoch(1), config, registry, tx_registry, factory)
}

/// A SQLite database of `rows` rows of `width`-character text, written directly (not through the
/// engine) so the fixture's size is the only thing about it that matters.
fn seed_db(path: &Path, rows: i64, width: usize) {
    let conn = rusqlite::Connection::open(path).expect("open fixture db");
    conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .expect("fixture schema");
    conn.execute(
        &format!(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < {rows}) \
             INSERT INTO t SELECT x, printf('%0{width}d', x) FROM c"
        ),
        [],
    )
    .expect("fixture rows");
}

fn rows_in(path: &Path) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("open snapshot");
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count rows in snapshot")
}

fn own_uid() -> u32 {
    Config::own_uid()
}

/// Every file in `dir` whose name marks it as a BACKUP temporary.
fn temporaries(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".ferro-backup-"))
        })
        .collect()
}

async fn backup(client: &mut TestClient, rid: u32, req: &BackupRequest) -> Outcome {
    client
        .send_request(rid, service::ADMIN, method_admin::BACKUP, req.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(
        t.header.request_id, rid,
        "the terminal echoes the request id"
    );
    assert_eq!(t.header.flags & flags::END, flags::END, "exactly one END");
    let outcome = Outcome::decode(&t.payload).expect("decode terminal Outcome");
    // A HANDLER's terminal echoes the request's service/method; a SESSION-layer refusal (the D15
    // gate) is deliberately generic (`CORE`/0, identified by its request id — `SessionError`'s
    // contract). Pinned both ways so a refusal that silently moved into the handler would show.
    if matches!(outcome, Outcome::Error(ref ep) if ep.code == errc::FORBIDDEN && ep.message.contains("SPEC D15"))
    {
        assert_eq!((t.header.service, t.header.method), (service::CORE, 0));
    } else {
        assert_eq!(
            (t.header.service, t.header.method),
            (service::ADMIN, method_admin::BACKUP)
        );
    }
    outcome
}

async fn backup_err(client: &mut TestClient, rid: u32, req: &BackupRequest) -> ErrorPayload {
    match backup(client, rid, req).await {
        Outcome::Error(ep) => ep,
        other => panic!("expected an error terminal, got {other:?}"),
    }
}

async fn backup_ok(client: &mut TestClient, rid: u32, req: &BackupRequest) -> BackupResponse {
    match backup(client, rid, req).await {
        Outcome::Ok(body) => BackupResponse::decode(&body).expect("decode BackupResponse"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

fn request(pool: &str, file: &str, replace: bool) -> BackupRequest {
    BackupRequest {
        pool: pool.into(),
        file: file.into(),
        replace,
        timeout_ms: Some(30_000),
    }
}

/// **D15, all three configurations, against one unchanged request.** The member case is the
/// control that makes the two refusals mean something: the same peer, the same request, the same
/// pool, and only `FERRO_ADMIN_UIDS` differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operate_is_refused_until_the_peers_uid_is_in_ferro_admin_uids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 50, 10);
    let dsn = format!("sqlite://{}", db.display());
    let req = request("main", "snap.db", false);
    let snap = dir.path().join("snap.db");

    // (1) The DEFAULT: empty → OPERATE disabled, even for the daemon's own uid.
    let server = admin_server(&[("main", dsn.clone())], Vec::new());
    let mut c = server.connect().await;
    c.hello(1).await;
    let ep = backup_err(&mut c, 2, &req).await;
    assert_eq!(ep.code, errc::FORBIDDEN, "{ep:?}");
    assert_eq!(ep.branch, branch::NON_RETRYABLE);
    let disabled_message = ep.message.clone();
    assert!(!snap.exists(), "a refused verb wrote a snapshot");
    assert_session_alive(&mut c, 77).await;

    // (2) A list that does not contain the peer.
    // A distinctive uid, so "the message does not name it" cannot be satisfied or broken by
    // accident (as root, `own + 1` is 1, which "D15" contains).
    let other = own_uid().wrapping_add(424_242);
    let server = admin_server(&[("main", dsn.clone())], vec![other]);
    let mut c = server.connect().await;
    c.hello(1).await;
    let ep = backup_err(&mut c, 2, &req).await;
    assert_eq!(ep.code, errc::FORBIDDEN, "{ep:?}");
    // The peer cannot tell "OPERATE is disabled" from "you are not a member": both are the same
    // refusal on the wire (the reason is logged server-side), and neither names a configured uid.
    assert_eq!(
        ep.message, disabled_message,
        "the two OPERATE refusals leak which one applies"
    );
    assert!(!ep.message.contains(&other.to_string()), "{}", ep.message);
    assert!(!snap.exists());

    // (3) CONTROL: the peer is a member → the snapshot is taken.
    let server = admin_server(&[("main", dsn)], vec![other, own_uid()]);
    let mut c = server.connect().await;
    c.hello(1).await;
    let resp = backup_ok(&mut c, 2, &req).await;
    assert_eq!(rows_in(&snap), 50, "the snapshot holds the database's rows");
    assert_eq!(
        resp.bytes,
        std::fs::metadata(&snap).unwrap().len(),
        "bytes is the snapshot's real size"
    );
    assert!(
        temporaries(dir.path()).is_empty(),
        "no temporary left behind"
    );
    assert_session_alive(&mut c, 78).await;
}

/// **The gate runs BEFORE the handler.** A refused peer is told `Forbidden` even for a pool that
/// does not exist — the refusal happens before pool lookup — while an admitted peer naming the same
/// missing pool gets the handler's `Unsupported("unknown pool")`. Without the ordering, a non-admin
/// could enumerate pool names by the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_peer_never_reaches_the_handler() {
    let req = request("no_such_pool", "snap.db", false);

    let refusing = admin_server(&[], Vec::new());
    let mut c = refusing.connect().await;
    c.hello(1).await;
    assert_eq!(backup_err(&mut c, 2, &req).await.code, errc::FORBIDDEN);

    let admitting = admin_server(&[], vec![own_uid()]);
    let mut c = admitting.connect().await;
    c.hello(1).await;
    let ep = backup_err(&mut c, 2, &req).await;
    assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
    assert!(ep.message.contains("unknown pool"), "{}", ep.message);
}

/// **The destination policy.** Only a plain file name, placed in the pool's allowed directory; each
/// refusal is `Forbidden` and creates nothing. The control (a plain name) succeeds on the same
/// session, so the refusals are the policy and not a broken verb.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_plain_file_name_inside_the_allowed_directory_is_written() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 10, 10);
    let server = admin_server(
        &[("main", format!("sqlite://{}", db.display()))],
        vec![own_uid()],
    );
    let mut c = server.connect().await;
    c.hello(1).await;

    let outside = tempfile::tempdir().expect("second tempdir");
    let absolute = outside.path().join("abs.db");
    let mut rid = 10;
    for bad in [
        "../escape.db".to_string(),
        "sub/dir.db".to_string(),
        absolute.display().to_string(),
        ".hidden.db".to_string(),
        String::new(),
    ] {
        rid += 1;
        let ep = backup_err(&mut c, rid, &request("main", &bad, false)).await;
        assert_eq!(ep.code, errc::FORBIDDEN, "{bad:?}: {ep:?}");
    }
    assert!(!absolute.exists(), "an absolute path was honoured");
    assert!(!dir.path().parent().unwrap().join("escape.db").exists());

    // A symlink in the allowed directory is refused even with `replace` — and not followed.
    let escaped = outside.path().join("escaped.db");
    std::os::unix::fs::symlink(&escaped, dir.path().join("link.db")).unwrap();
    let ep = backup_err(&mut c, 30, &request("main", "link.db", true)).await;
    assert_eq!(ep.code, errc::FORBIDDEN, "{ep:?}");
    assert!(!escaped.exists(), "the snapshot followed the symlink out");

    // CONTROL on the same session.
    backup_ok(&mut c, 31, &request("main", "ok.db", false)).await;
    assert_eq!(rows_in(&dir.path().join("ok.db")), 10);
}

/// **Atomic finalisation.** Without `replace` an existing snapshot is refused and left exactly as it
/// was; with it, the new snapshot is swapped in. Then the property delete-then-write could not give:
/// a backup that FAILS (here, timed out) under `replace` leaves the previous snapshot intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replace_swaps_atomically_and_a_failed_backup_keeps_the_previous_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 200_000, 200);
    let server = admin_server(
        &[("main", format!("sqlite://{}", db.display()))],
        vec![own_uid()],
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let snap = dir.path().join("nightly.db");

    std::fs::write(&snap, b"previous snapshot").unwrap();
    let ep = backup_err(&mut c, 2, &request("main", "nightly.db", false)).await;
    assert_eq!(ep.code, errc::FORBIDDEN, "{ep:?}");
    assert!(ep.message.contains("already exists"), "{}", ep.message);
    assert_eq!(std::fs::read(&snap).unwrap(), b"previous snapshot");

    // A backup that cannot finish: a 1 ms bound on a ~40 MB database. The previous file survives,
    // and the error is a known fate — never Indeterminate, since the source database is untouched.
    let mut timed = request("main", "nightly.db", true);
    timed.timeout_ms = Some(1);
    let ep = backup_err(&mut c, 3, &timed).await;
    assert_ne!(ep.branch, branch::INDETERMINATE, "{ep:?}");
    assert!(
        ep.code == errc::CANCELLED || ep.code == errc::QUERY_TIMEOUT,
        "a bounded backup reports cancellation/timeout, got {ep:?}"
    );
    assert_eq!(
        std::fs::read(&snap).unwrap(),
        b"previous snapshot",
        "a FAILED replace destroyed the previous snapshot"
    );
    assert!(
        temporaries(dir.path()).is_empty(),
        "the failed temporary was left behind"
    );

    // And the swap itself, on the same connection the interrupted one came back on.
    let resp = backup_ok(&mut c, 4, &request("main", "nightly.db", true)).await;
    assert_eq!(rows_in(&snap), 200_000);
    assert_eq!(resp.bytes, std::fs::metadata(&snap).unwrap().len());
    // The §13 split, by VALUE: a ~40 MB snapshot takes milliseconds of statement time, while the
    // wait for an idle pool's connection does not — so a swap of the two fields cannot pass.
    assert!(
        resp.exec_us >= 1_000,
        "exec_us is the snapshot's own time: {resp:?}"
    );
    assert!(
        resp.queue_us < resp.exec_us,
        "queue_us is the pool wait: {resp:?}"
    );
    assert!(temporaries(dir.path()).is_empty());
}

/// **`CANCEL` reaches a running backup** — the interrupt handle, not merely an abandoned future (the
/// C3-3d finding: `spawn_blocking` cannot be dropped, so a missed interrupt would run to completion).
/// The target must not appear and the temporary must be gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_interrupts_a_running_backup_and_leaves_nothing_behind() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 400_000, 200);
    let server = admin_server(
        &[("main", format!("sqlite://{}", db.display()))],
        vec![own_uid()],
    );
    let mut c = server.connect().await;
    c.hello(1).await;

    let mut req = request("main", "cancelled.db", false);
    req.timeout_ms = None;
    c.send_request(2, service::ADMIN, method_admin::BACKUP, req.encode())
        .await;
    // Wait until the snapshot is demonstrably under way — its temporary exists and is growing —
    // so the CANCEL lands on a RUNNING statement rather than racing the dispatch.
    let started = std::time::Instant::now();
    loop {
        let tmp = temporaries(dir.path());
        if tmp
            .iter()
            .any(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        {
            break;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "the backup never started writing"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    c.cancel(2).await;
    let t = c.recv().await;
    assert_eq!(t.header.request_id, 2);
    let ep = match Outcome::decode(&t.payload).expect("decode") {
        Outcome::Error(ep) => ep,
        other => panic!("a cancelled backup must not report success: {other:?}"),
    };
    assert_eq!(ep.code, errc::CANCELLED, "{ep:?}");
    assert_ne!(ep.branch, branch::INDETERMINATE);
    assert!(
        !dir.path().join("cancelled.db").exists(),
        "a cancelled backup was finalised"
    );
    assert!(
        temporaries(dir.path()).is_empty(),
        "the cancelled temporary was left behind"
    );
    assert_session_alive(&mut c, 79).await;
}

/// **A server database is refused, not approximated.** Runs against whichever server backend the
/// lane configures (CI's integration lane has both).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_backed_pool_is_unsupported() {
    let urls: Vec<String> = [common::pg_url(), common::mysql_url()]
        .into_iter()
        .flatten()
        .collect();
    if urls.is_empty() {
        eprintln!(
            "skip: no FERRO_TEST_PG_URL / FERRO_TEST_MYSQL_URL — server-pool BACKUP refusal not run"
        );
        return;
    }
    for url in urls {
        let server = admin_server(&[("srv", url)], vec![own_uid()]);
        let mut c = server.connect().await;
        c.hello(1).await;
        let ep = backup_err(&mut c, 2, &request("srv", "snap.db", false)).await;
        assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
        assert!(ep.message.contains("SQLite pools only"), "{}", ep.message);
    }
}

/// **A snapshot may never be written over a live database or its sidecars** (review finding,
/// reproduced: `replace` over `main.db` lost an acknowledged write, over `main.db-wal` it corrupted
/// the database). All four names are refused with or without `replace`, and the database is intact
/// and writable afterwards — with a control name in the same directory that succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_database_and_its_sidecars_are_never_a_backup_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 20, 10);
    let server = admin_server(
        &[("main", format!("sqlite://{}", db.display()))],
        vec![own_uid()],
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    // Touch the pool so its WAL and shared-memory files exist, as they do on a live database.
    backup_ok(&mut c, 2, &request("main", "warmup.db", false)).await;

    let before = std::fs::metadata(&db).unwrap().ino();
    let mut rid = 10;
    for name in ["main.db", "main.db-wal", "main.db-shm", "main.db-journal"] {
        for replace in [true, false] {
            rid += 1;
            let ep = backup_err(&mut c, rid, &request("main", name, replace)).await;
            assert_eq!(ep.code, errc::FORBIDDEN, "{name} replace={replace}: {ep:?}");
            assert!(ep.message.contains("live database"), "{}", ep.message);
        }
    }
    assert_eq!(
        std::fs::metadata(&db).unwrap().ino(),
        before,
        "the live database file was replaced"
    );
    assert!(!dir.path().join("main.db-journal").exists());
    assert_eq!(rows_in(&db), 20, "the live database is intact");
    assert!(temporaries(dir.path()).is_empty());
}

/// **The documented length limit is the real one** (review finding: names of 230–255 bytes passed
/// the 255-byte policy and then failed at the temporary as a RETRYABLE `ConnectionLost`). The
/// longest accepted name works end to end, one byte more is a clean `Forbidden`, and the snapshot is
/// private to `ferrod`'s user.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_longest_accepted_name_works_and_the_snapshot_is_private() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.db");
    seed_db(&db, 5, 10);
    let server = admin_server(
        &[("main", format!("sqlite://{}", db.display()))],
        vec![own_uid()],
    );
    let mut c = server.connect().await;
    c.hello(1).await;

    let longest = "x".repeat(200);
    backup_ok(&mut c, 2, &request("main", &longest, false)).await;
    let snap = dir.path().join(&longest);
    assert_eq!(rows_in(&snap), 5);
    let mode =
        std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&snap).unwrap().permissions());
    assert_eq!(
        mode & 0o777,
        0o600,
        "a snapshot is a full copy of the data; it is private"
    );

    let ep = backup_err(&mut c, 3, &request("main", &"x".repeat(201), false)).await;
    assert_eq!(ep.code, errc::FORBIDDEN, "{ep:?}");
    assert_ne!(ep.branch, branch::RETRYABLE);
    assert!(temporaries(dir.path()).is_empty());
}

/// **A failure BEFORE the snapshot statement is sent is a known fate** — the review found the
/// not-sent context untested end to end. The pool's database lives in a directory that does not
/// exist, so the checkout's dial fails, while the operator's allowed directory resolves and the
/// temporary is created there: the reply is Retryable (nothing was sent), never Indeterminate, and
/// the temporary is removed. And an allowed directory that does not resolve is refused up front.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failure_before_the_snapshot_is_sent_is_retryable_and_leaves_nothing() {
    let allow = tempfile::tempdir().expect("tempdir");
    let missing = allow.path().join("no-such-dir").join("main.db");
    let server = admin_server_allowing(
        &[("main", format!("sqlite://{}", missing.display()))],
        vec![own_uid()],
        Some(allow.path()),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let ep = backup_err(&mut c, 2, &request("main", "snap.db", false)).await;
    assert_eq!(ep.code, errc::CONNECTION_LOST, "{ep:?}");
    assert_eq!(ep.branch, branch::RETRYABLE, "nothing was sent: {ep:?}");
    assert!(
        temporaries(allow.path()).is_empty(),
        "the temporary was left behind"
    );
    assert!(!allow.path().join("snap.db").exists());

    let nowhere = allow.path().join("not-a-dir");
    let server = admin_server_allowing(
        &[("main", format!("sqlite://{}", missing.display()))],
        vec![own_uid()],
        Some(&nowhere),
    );
    let mut c = server.connect().await;
    c.hello(1).await;
    let ep = backup_err(&mut c, 2, &request("main", "snap.db", false)).await;
    assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
    assert!(ep.message.contains("does not resolve"), "{}", ep.message);
}
