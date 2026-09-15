//! **C3-4: the `readonly` declaration, enforced end to end on a SQLite pool.**
//!
//! This is the whole-path proof the seam exists for: a real client → `ferrod` session → SQL service
//! → `Pool::checkout_declared` → `PoolBackend::apply_readonly` → `PRAGMA query_only`. Nothing here
//! reaches into the pool or the backend directly, because the thing most likely to be wrong is not
//! the pragma — the C3-1 spike's `p5` already proved that — but whether the client's declared flag
//! actually ARRIVES. A backend test would pass with all three `ferrod` call sites passing `false`.
//!
//! **Why enforcement matters at all, restated so it is not mistaken for belt-and-braces.** The
//! engine never infers read-vs-write (charter rule 6), so `readonly` is a CLIENT DECLARATION — and
//! it is already trusted in two places that cost something when it lies: `fate.rs` suppresses the
//! `Indeterminate` branch for a declared read (§19.3), and under D13 `compose_begin_sql` gives a
//! declared-readonly transaction `BEGIN DEFERRED`, which is `p4a`'s exact deferred-upgrade setup —
//! the one failure class charter rule 3 forbids the engine resolving. So a lying declaration does
//! not merely mislabel a statement; it steers the engine into a state it cannot get out of.
//! `query_only` converts that into an up-front `SQLITE_READONLY`, deterministic and provably not
//! executed.
//!
//! **What this file deliberately does NOT test: the fresh-dial checkout exit.** A first attempt had
//! a test claiming to exercise it ("the pool's very first statement, on a connection it has just
//! dialled") and the mutation proved that claim FALSE — deleting the `apply_readonly` call from the
//! fresh-dial arm left it green. The reason is structural: the HELLO handshake calls
//! `PoolRegistry::pool_info`, whose version probe checks a connection out and returns it, so by the
//! time any EXEC runs the pool ALWAYS has an idle connection and every request here takes the
//! recycled exit. No test in this directory can reach the other one. That proof lives at pool level
//! in `ferro-backend-sqlite`'s `readonly_it.rs`, where the pool is driven directly.
//!
//! SQLite needs no server, so this runs everywhere including CI, with no gating env var — unlike
//! every other file in this directory.

mod common;

use common::{assert_session_alive, exec_err, exec_ok, exec_server, req};
use ferro_proto::messages::sql::ExecRequest;

/// A write statement, declared however the caller says. `common::req` declares `readonly: true` by
/// default (harmless on the wire backends, where `apply_readonly` is a no-op), so both arms set the
/// flag EXPLICITLY here rather than relying on that default — a test whose control differs from its
/// subject by an unstated default is one refactor away from testing nothing.
fn write(sql: &str, readonly: bool) -> ExecRequest {
    ExecRequest {
        readonly,
        ..req(sql)
    }
}

fn sqlite_dsn(dir: &tempfile::TempDir, name: &str) -> String {
    format!("sqlite://{}", dir.path().join(name).display())
}

/// **The subject and its control, in one test so they cannot drift apart.**
///
/// The identical INSERT is refused when declared `readonly` and accepted when not. Running both
/// against the same server, same pool and same table is what rules out the alternative explanations
/// — a malformed statement, a missing table, a pool that cannot write at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_declared_readonly_write_is_refused_and_an_undeclared_one_is_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = exec_server(sqlite_dsn(&dir, "readonly.db"));
    let mut client = server.connect().await;
    client.hello(1).await;

    exec_ok(
        &mut client,
        2,
        &write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", false),
    )
    .await;

    // SUBJECT: declared readonly, then writes. Refused before it can apply.
    let err = exec_err(
        &mut client,
        3,
        &write("INSERT INTO t(v) VALUES ('declared')", true),
    )
    .await;
    assert_eq!(
        err.errno,
        Some(8),
        "a declared-readonly write must be refused with SQLITE_READONLY (8), not merely fail: \
         {err:?}"
    );

    // CONTROL: the same statement, undeclared. If this failed too, the assertion above would be
    // observing a broken fixture rather than the declaration being honoured.
    let ok = exec_ok(
        &mut client,
        4,
        &write("INSERT INTO t(v) VALUES ('undeclared')", false),
    )
    .await;
    assert_eq!(ok.affected, 1, "the undeclared write applies normally");

    // And exactly one row landed — the refused write did NOT apply. `p5` proves non-execution at the
    // SQLite level; this proves it through the whole path, which is the claim a user cares about.
    let rows = exec_ok(&mut client, 5, &write("SELECT count(*) FROM t", true)).await;
    assert_eq!(
        rows.rows[0][0],
        ferro_proto::value::Value::I64(1),
        "exactly the undeclared write applied; the refused one left nothing behind"
    );

    assert_session_alive(&mut client, 77).await;
}

/// **The cross-tenant guarantee, which is the part a pool gets wrong.**
///
/// A connection armed `query_only` for one tenant must not reach the next one still armed. Arming
/// is `apply_readonly`'s job at checkout; DISARMING is the hygiene reset's (C3-3b's explicit
/// 4-item list), and this asserts the two actually meet.
///
/// The ordering is what makes it a real test: the readonly request runs FIRST so its connection goes
/// back to the pool armed, and the write that follows is served by that same recycled connection —
/// with `max_size` at the daemon default and one client, there is no second connection for it to be
/// served by instead.
///
/// MUTATION PROVEN: removing `PRAGMA query_only=OFF` from `SqliteBackend::reset` leaves the second
/// write refused with 8, and this fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_readonly_checkout_does_not_leave_the_connection_readonly_for_the_next_tenant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = exec_server(sqlite_dsn(&dir, "recycle.db"));
    let mut client = server.connect().await;
    client.hello(1).await;

    exec_ok(&mut client, 2, &write("CREATE TABLE t(id INTEGER)", false)).await;

    // Tenant 1 declares readonly. Its connection returns to the pool armed.
    exec_ok(&mut client, 3, &write("SELECT 1", true)).await;

    // Tenant 2 does not. It gets that same recycled connection and MUST be able to write.
    let ok = exec_ok(&mut client, 4, &write("INSERT INTO t VALUES (1)", false)).await;
    assert_eq!(
        ok.affected, 1,
        "the previous tenant's readonly arming leaked onto a recycled connection — the reset that \
         owns disarming did not run, or ran before the arming rather than after"
    );

    assert_session_alive(&mut client, 78).await;
}
