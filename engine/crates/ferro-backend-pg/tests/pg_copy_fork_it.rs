//! Live premises for the M3-D4 `tokio-postgres` COPY fork changes (`/UPSTREAM_PR.md` §COPY), run
//! against the vendored driver DIRECTLY — no pool, no engine — so each test is about the driver
//! behaviour the fork changed and nothing above it. SKIPS without `FERRO_TEST_PG_URL`.
//!
//! The headline (`a_deferred_constraint_failing_at_the_implicit_commit_is_not_reported_as_success`):
//! upstream's `CopyInSink::finish` returns at `CommandComplete`, but an autocommit COPY's implicit
//! transaction commits at the `Sync` that FOLLOWS it — so a `DEFERRABLE INITIALLY DEFERRED` constraint
//! the copied rows violate fails AFTER `finish` has already returned `Ok(n)`. The rows are gone and the
//! caller was told they landed. The fork reads through `ReadyForQuery`; the control row proves the
//! table itself accepts a valid COPY, so the test cannot pass by every COPY failing.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt, pin_mut};
use tokio_postgres::{Client, NoTls};

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

async fn connect(url: &str) -> Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn count(client: &Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("count")
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_constraint_failing_at_the_implicit_commit_is_not_reported_as_success() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS d4_fork_child; DROP TABLE IF EXISTS d4_fork_parent;
             CREATE TABLE d4_fork_parent (id int PRIMARY KEY);
             INSERT INTO d4_fork_parent VALUES (1);
             CREATE TABLE d4_fork_child (id int, parent int REFERENCES d4_fork_parent(id)
                 DEFERRABLE INITIALLY DEFERRED);",
        )
        .await
        .unwrap();

    // CONTROL: a valid row copies and commits.
    {
        let sink = client
            .copy_in::<_, Bytes>("COPY d4_fork_child FROM STDIN")
            .await
            .unwrap();
        pin_mut!(sink);
        sink.send(Bytes::from_static(b"1\t1\n")).await.unwrap();
        assert_eq!(sink.as_mut().finish().await.unwrap(), 1);
    }
    assert_eq!(
        count(&client, "d4_fork_child").await,
        1,
        "control row landed"
    );

    // A row whose parent does not exist: the FK is checked at the implicit COMMIT, not per row.
    let sink = client
        .copy_in::<_, Bytes>("COPY d4_fork_child FROM STDIN")
        .await
        .unwrap();
    pin_mut!(sink);
    sink.send(Bytes::from_static(b"2\t999\n")).await.unwrap();
    let r = sink.as_mut().finish().await;
    let err = r.expect_err("a COPY whose implicit COMMIT failed must not report success");
    assert_eq!(
        err.as_db_error().map(|d| d.code().code()),
        Some("23503"),
        "the commit-time FK violation is the error: {err}"
    );
    assert_eq!(count(&client, "d4_fork_child").await, 1, "nothing applied");
    // The connection is in step: the RFQ was consumed by `finish`, not left for the next query.
    assert_eq!(client.transaction_status(), b'I');
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_out_reports_the_exported_row_count_and_consumes_ready_for_query() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    let stream = client
        .copy_out("COPY (SELECT g FROM generate_series(1, 5) g) TO STDOUT")
        .await
        .unwrap();
    pin_mut!(stream);
    let mut got = Vec::new();
    while let Some(chunk) = stream.next().await {
        got.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(got, b"1\n2\n3\n4\n5\n");
    assert_eq!(stream.rows_affected(), Some(5));
    assert_eq!(client.transaction_status(), b'I');
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_out_whose_commit_fails_ends_in_an_error_not_a_clean_end() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS d4_fork_out_child; DROP TABLE IF EXISTS d4_fork_out_parent;
             CREATE TABLE d4_fork_out_parent (id int PRIMARY KEY);
             CREATE TABLE d4_fork_out_child (id int, parent int REFERENCES d4_fork_out_parent(id)
                 DEFERRABLE INITIALLY DEFERRED);",
        )
        .await
        .unwrap();
    // A WRITING COPY TO: the INSERT … RETURNING streams its row, then the implicit COMMIT fails.
    let stream = client
        .copy_out("COPY (INSERT INTO d4_fork_out_child VALUES (1, 999) RETURNING id) TO STDOUT")
        .await
        .unwrap();
    pin_mut!(stream);
    let mut saw_err = None;
    while let Some(item) = stream.next().await {
        if let Err(e) = item {
            saw_err = Some(e);
            break;
        }
    }
    let e = saw_err.expect("the stream must end in the commit's error, not a clean end");
    assert_eq!(e.as_db_error().map(|d| d.code().code()), Some("23503"));
    assert_eq!(count(&client, "d4_fork_out_child").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_before_done_is_acknowledged_and_applies_nothing() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    client
        .batch_execute("DROP TABLE IF EXISTS d4_fork_abort; CREATE TABLE d4_fork_abort (id int);")
        .await
        .unwrap();
    let sink = client
        .copy_in::<_, Bytes>("COPY d4_fork_abort FROM STDIN")
        .await
        .unwrap();
    pin_mut!(sink);
    sink.send(Bytes::from_static(b"1\n2\n3\n")).await.unwrap();
    sink.as_mut()
        .abort()
        .await
        .expect("the server acknowledges the abort");
    assert_eq!(client.transaction_status(), b'I', "ReadyForQuery consumed");
    assert_eq!(count(&client, "d4_fork_abort").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_error_mid_copy_is_reported_by_the_next_send() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    client
        .batch_execute("DROP TABLE IF EXISTS d4_fork_early; CREATE TABLE d4_fork_early (id int);")
        .await
        .unwrap();
    let sink = client
        .copy_in::<_, Bytes>("COPY d4_fork_early FROM STDIN")
        .await
        .unwrap();
    pin_mut!(sink);
    // A malformed row, flushed past the driver's 4 KiB batching so the server sees it.
    let mut bad = b"not-an-int\n".to_vec();
    bad.resize(8192, b'\n');
    sink.send(Bytes::from(bad)).await.unwrap();
    // Keep sending until the server's ErrorResponse surfaces (bounded: it is already on its way).
    let mut early = None;
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        match sink.send(Bytes::from_static(b"1\n")).await {
            Ok(()) => {}
            Err(e) => {
                early = Some(e);
                break;
            }
        }
    }
    let e = early.expect("the server's 22P02 surfaces before the end of data");
    assert_eq!(e.as_db_error().map(|d| d.code().code()), Some("22P02"));
    sink.as_mut()
        .abort()
        .await
        .expect("abort after an early error");
    assert_eq!(client.transaction_status(), b'I');
    assert_eq!(count(&client, "d4_fork_early").await, 0);
}

/// Upstream `copy_in` on a statement the server rejects BEFORE copy-in mode (an unknown table)
/// dropped its data sender, which sends `CopyFail` + `Sync`; that second `Sync` drew a second
/// `ReadyForQuery` no request was waiting for, and the connection died. The fork abandons the data
/// stream instead, so the connection survives the error like any other statement's.
#[tokio::test(flavor = "multi_thread")]
async fn a_copy_rejected_before_copy_mode_does_not_kill_the_connection() {
    let Some(url) = test_url() else {
        return;
    };
    let client = connect(&url).await;
    let err = match client
        .copy_in::<_, Bytes>("COPY d4_fork_no_such_table FROM STDIN")
        .await
    {
        Ok(_) => panic!("an unknown table must fail"),
        Err(e) => e,
    };
    assert_eq!(err.as_db_error().map(|d| d.code().code()), Some("42P01"));
    let one: i32 = client
        .query_one("SELECT 1", &[])
        .await
        .expect("still usable")
        .get(0);
    assert_eq!(one, 1);
    assert!(!client.is_closed());
}
