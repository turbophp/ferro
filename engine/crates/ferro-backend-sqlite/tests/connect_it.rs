//! C3-3a: connection setup, against real files on disk. SQLite needs no server, so these are
//! ordinary tests — CI is not the authority for any of them.

use std::time::Duration;

use ferro_backend_sqlite::{SqliteBackend, resolve_path};
use ferro_pool::backend::Dialect;
use ferro_pool::error::PoolError;

fn backend_on(dir: &tempfile::TempDir, name: &str) -> SqliteBackend {
    SqliteBackend::new(format!("sqlite://{}", dir.path().join(name).display()))
}

/// WAL is CHECKED, not requested and hoped for.
///
/// D13's entire argument assumes many readers alongside one writer, and every spike premise was
/// proven in WAL mode. SQLite can decline the change and stay in rollback mode, reporting the mode
/// it actually applied — so a backend that issued the pragma without reading the answer would
/// believe it had reader concurrency it did not have.
#[tokio::test(flavor = "multi_thread")]
async fn connect_verifies_wal_rather_than_assuming_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "wal.db");
    let conn = backend.connect().await.expect("connect");

    let mode: String = conn
        .driver()
        .expect("live handle")
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("read journal_mode");
    assert_eq!(
        mode.to_ascii_lowercase(),
        "wal",
        "the connection must be in WAL mode after connect"
    );
}

/// The busy timeout is really armed on the connection, not merely passed to a constructor.
#[tokio::test(flavor = "multi_thread")]
async fn connect_arms_the_busy_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "busy.db").with_busy_timeout(Duration::from_millis(1234));
    let conn = backend.connect().await.expect("connect");

    let armed: i64 = conn
        .driver()
        .expect("live handle")
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
        .expect("read busy_timeout");
    assert_eq!(
        armed, 1234,
        "the timeout the backend was built with must be the one SQLite holds"
    );
}

/// **The `:memory:` refusal is justified by demonstrating the hazard first.**
///
/// A refusal with no proof behind it is superstition that a later reader deletes. So this shows the
/// actual failure — two in-memory connections are two SEPARATE databases, which under a pool means
/// different tenants silently seeing different data — and only then asserts that the DSN is
/// refused.
#[test]
fn memory_dsn_hazard_is_real_and_is_refused() {
    // THE HAZARD, demonstrated. Both connections are "the same" DSN.
    let a = rusqlite::Connection::open_in_memory().expect("open a");
    let b = rusqlite::Connection::open_in_memory().expect("open b");
    a.execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);")
        .expect("a writes");
    let seen_by_b = b
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='t'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .expect("b reads its own catalogue");
    assert_eq!(
        seen_by_b, 0,
        "THE HAZARD: b cannot see the table a created, because :memory: gives every connection its \
         own private database. In a pool these are two tenants and one of them is silently wrong."
    );

    // THEREFORE the DSN is refused, in every spelling.
    for dsn in [
        ":memory:",
        "sqlite://:memory:",
        "file::memory:?cache=shared",
        "sqlite://file::memory:?cache=shared",
        "sqlite://test.db?mode=memory",
    ] {
        match resolve_path(dsn) {
            Err(PoolError::Unsupported(msg)) => assert!(
                msg.contains("in-memory"),
                "the refusal must name the reason, got {msg:?}"
            ),
            other => panic!("{dsn:?} must be refused, got {other:?}"),
        }
    }
}

#[test]
fn resolve_path_accepts_the_scheme_and_a_bare_path() {
    assert_eq!(
        resolve_path("sqlite:///var/lib/ferro/app.db").expect("absolute"),
        std::path::PathBuf::from("/var/lib/ferro/app.db")
    );
    assert_eq!(
        resolve_path("sqlite://data/app.db").expect("relative"),
        std::path::PathBuf::from("data/app.db")
    );
    assert_eq!(
        resolve_path("/var/lib/ferro/app.db").expect("bare path"),
        std::path::PathBuf::from("/var/lib/ferro/app.db")
    );
    assert!(
        matches!(resolve_path("sqlite://"), Err(PoolError::Unsupported(_))),
        "a DSN naming no file is refused rather than resolving to an empty path"
    );
}

/// `query_only` really refuses writes and really releases them — the mechanism C3-4's `readonly`
/// seam will call, and the mitigation C3-2's `BEGIN DEFERRED` arm depends on.
#[tokio::test(flavor = "multi_thread")]
async fn query_only_arms_and_disarms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "ro.db");
    let mut conn = backend.connect().await.expect("connect");

    conn.driver()
        .expect("live")
        .execute_batch("CREATE TABLE t(v INTEGER)")
        .expect("seed");

    backend.set_query_only(&mut conn, true).await.expect("arm");
    assert!(conn.is_query_only());
    let err = conn
        .driver()
        .expect("live")
        .execute("INSERT INTO t VALUES (1)", [])
        .expect_err("an armed connection must refuse the write");
    assert!(
        matches!(
            err,
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::ReadOnly,
                    ..
                },
                _
            )
        ),
        "the refusal is SQLITE_READONLY, which is deterministic and provably not executed — NOT \
         the unretryable BUSY_SNAPSHOT a deferred upgrade would give: {err:?}"
    );

    // Re-arming is a no-op rather than a second statement.
    backend
        .set_query_only(&mut conn, true)
        .await
        .expect("re-arm is a no-op");

    backend
        .set_query_only(&mut conn, false)
        .await
        .expect("disarm");
    assert!(!conn.is_query_only());
    conn.driver()
        .expect("live")
        .execute("INSERT INTO t VALUES (1)", [])
        .expect("the same connection writes once disarmed — so a pooled conn can be recycled");
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_dialect_and_is_closed_on_a_healthy_conn() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "ping.db");
    let mut conn = backend.connect().await.expect("connect");

    backend.ping(&mut conn).await.expect("ping");
    assert!(
        !backend.is_closed(&conn),
        "a healthy connection is not closed"
    );
    assert_eq!(backend.dialect(), Dialect::Sqlite);
}

/// A connection is usable again after a blocking call hands it back — the park/unpark contract
/// every later slice's statement runner depends on.
#[tokio::test(flavor = "multi_thread")]
async fn the_connection_survives_repeated_blocking_calls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "park.db");
    let mut conn = backend.connect().await.expect("connect");

    for _ in 0..5 {
        backend.ping(&mut conn).await.expect("ping");
        assert!(!backend.is_closed(&conn));
    }
    assert!(
        conn.driver().expect("live").is_autocommit(),
        "and it comes back autocommit-clean, as the spike's p2 proved"
    );
}

/// **C3-6a: foreign-key enforcement is DECLARED, and this test says where it used to come from.**
///
/// The behavioural half is what matters to a user: every connection this pool dials refuses an
/// orphan row. The second half is the part worth writing down — it asserts what the BUILD would do
/// on its own, so that a future `cargo update` that flips libsqlite3-sys's
/// `SQLITE_DEFAULT_FOREIGN_KEYS` shows up here as a failing measurement rather than as advisory
/// foreign keys in production.
///
/// STATED PLAINLY: with today's `rusqlite = { features = ["bundled"] }` the raw default is already
/// ON, so deleting the pragma from `open_configured` leaves the first assertion GREEN. That is the
/// same shape as C3-3a's removed `lose_handle()` — a green test is not evidence the line beneath it
/// does anything — and it is exactly why the pragma is there: the guarantee must not rest on a
/// dependency's compile flag. The raw-default assertion below is the tripwire that makes the
/// pragma's value visible if that flag ever moves; today it records the redundancy honestly.
#[tokio::test(flavor = "multi_thread")]
async fn connect_enforces_foreign_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "fk.db");
    let conn = backend.connect().await.expect("connect");
    let driver = conn.driver().expect("live handle");

    let fk: i64 = driver
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .expect("read foreign_keys");
    assert_eq!(
        fk, 1,
        "the pool's own connections must enforce foreign keys"
    );

    driver
        .execute_batch(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY);
             CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id));",
        )
        .expect("fixture");
    let orphan = driver.execute("INSERT INTO child (id, p) VALUES (1, 999)", []);
    let err = orphan.expect_err("an orphan row must be refused, not stored");
    assert!(
        format!("{err}").contains("FOREIGN KEY constraint failed"),
        "expected a foreign-key violation, got {err}"
    );

    // The tripwire. `rusqlite`'s bundled build currently compiles SQLITE_DEFAULT_FOREIGN_KEYS=1,
    // which is why the pragma changes nothing today — and why it has to be written down.
    let raw = rusqlite::Connection::open(dir.path().join("raw.db")).expect("raw open");
    let raw_default: i64 = raw
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .expect("read raw default");
    assert_eq!(
        raw_default, 1,
        "the bundled SQLite build's own default for foreign_keys has CHANGED (it was 1). The \
         pragma in open_configured is now load-bearing rather than declarative — keep it, and \
         update this measurement."
    );
}
