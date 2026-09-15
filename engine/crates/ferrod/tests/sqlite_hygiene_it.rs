//! **The cross-tenant PRAGMA leak, end to end through a real `ferrod`.**
//!
//! `ferro-backend-sqlite`'s `hygiene_pragma_it.rs` proves the mechanism at pool level, where this
//! file's counterpart can control which connection serves which checkout. What it cannot show is
//! that two SEPARATE CLIENT SESSIONS — which is what "cross-tenant" means to a user — are affected,
//! and that is how the defect was found in the first place: one PHP client armed
//! `PRAGMA foreign_keys = OFF`, disconnected, and a second client's orphan row was stored without
//! complaint.
//!
//! SQLite needs no server, so this runs everywhere including CI, with no gating env var.

mod common;

use common::{assert_session_alive, exec_err, exec_ok, exec_server, req};
use ferro_proto::messages::sql::ExecRequest;

fn write(sql: &str) -> ExecRequest {
    ExecRequest {
        readonly: false,
        ..req(sql)
    }
}

/// Two sessions, one pool, one connection between them.
///
/// The second session never issues a `PRAGMA` and never asks for anything unusual; it simply
/// inserts an orphan row and expects the database to refuse it, which a SQLite connection Ferro
/// dialled always does (C3-6a made that a declared guarantee rather than an inherited build flag).
///
/// MUTATION PROVEN at pool level: with `ResetProfile::Full` reduced to the C3-3b four-item list,
/// `foreign_keys` reads 0 for the second tenant and this INSERT succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_pragma_from_one_session_does_not_reach_the_next() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dsn = format!("sqlite://{}", dir.path().join("hygiene.db").display());
    let server = exec_server(dsn);

    {
        let mut tenant1 = server.connect().await;
        tenant1.hello(1).await;
        exec_ok(
            &mut tenant1,
            2,
            &write("CREATE TABLE parent (id INTEGER PRIMARY KEY)"),
        )
        .await;
        exec_ok(
            &mut tenant1,
            3,
            &write("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id))"),
        )
        .await;
        exec_ok(&mut tenant1, 4, &write("PRAGMA foreign_keys = OFF")).await;
    }

    let mut tenant2 = server.connect().await;
    tenant2.hello(5).await;

    let err = exec_err(
        &mut tenant2,
        6,
        &write("INSERT INTO child (id, p) VALUES (1, 999)"),
    )
    .await;
    assert!(
        err.message.contains("FOREIGN KEY constraint failed"),
        "the previous session's `PRAGMA foreign_keys = OFF` reached this one — the orphan row was \
         accepted instead of refused: {err:?}"
    );

    // And nothing landed: the refusal is the database's, before the write.
    let rows = exec_ok(&mut tenant2, 7, &write("SELECT count(*) FROM child")).await;
    assert_eq!(rows.rows[0][0], ferro_proto::value::Value::I64(0));

    assert_session_alive(&mut tenant2, 79).await;
}
