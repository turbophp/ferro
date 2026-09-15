//! C3-3c: `query`, and the storage-class ↔ §9 tag mapping.

use ferro_backend_sqlite::SqliteBackend;
use ferro_proto::value::Value;

fn backend_on(dir: &tempfile::TempDir, name: &str) -> SqliteBackend {
    SqliteBackend::new(format!("sqlite://{}", dir.path().join(name).display()))
}

/// **THE DECISION, demonstrated: one column, two storage classes.**
///
/// SQLite does not enforce a declared type, so `v INTEGER` holds both an integer and a string. A
/// per-COLUMN tag cannot describe both cells; a per-VALUE tag describes each one exactly. This is
/// the measurement the mapping rests on, so it is a test rather than a comment.
#[tokio::test(flavor = "multi_thread")]
async fn one_column_can_hold_two_storage_classes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "dyn.db");
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE t(v INTEGER);
             INSERT INTO t VALUES (1);
             INSERT INTO t VALUES ('abc');",
        )
        .await
        .expect("seed");

    let r = backend
        .query(&mut conn, "SELECT v FROM t ORDER BY rowid", &[])
        .await
        .expect("query");

    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0][0],
        Value::I64(1),
        "the integer cell is tagged from ITS OWN storage class"
    );
    assert_eq!(
        r.rows[1][0],
        Value::Text("abc".into()),
        "THE LOAD-BEARING ASSERTION: the very next row of the SAME column is TEXT. A declared-type \
         mapping would have to call this an I64 and be wrong, or refuse a row SQLite accepted."
    );
}

/// Every storage class maps, and the bind direction round-trips.
#[tokio::test(flavor = "multi_thread")]
async fn all_five_storage_classes_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "classes.db");
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(&mut conn, "CREATE TABLE t(a, b, c, d, e)")
        .await
        .expect("seed");
    backend
        .query(
            &mut conn,
            "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
            &[
                Value::Null,
                Value::I64(-42),
                Value::F64(1.5),
                Value::Text("hi".into()),
                Value::Bytes(vec![0, 255]),
            ],
        )
        .await
        .expect("insert");

    let r = backend
        .query(&mut conn, "SELECT a, b, c, d, e FROM t", &[])
        .await
        .expect("select");
    assert_eq!(
        r.rows[0],
        vec![
            Value::Null,
            Value::I64(-42),
            Value::F64(1.5),
            Value::Text("hi".into()),
            Value::Bytes(vec![0, 255]),
        ]
    );
}

/// A bound `Bool` lands as INTEGER and reads back as `I64` — the asymmetry named in `rowmap`,
/// asserted so it cannot drift into a silent coercion later.
#[tokio::test(flavor = "multi_thread")]
async fn bool_binds_as_integer_and_is_unrecoverable_on_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "bool.db");
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(&mut conn, "CREATE TABLE t(flag BOOLEAN)")
        .await
        .expect("seed");
    backend
        .query(&mut conn, "INSERT INTO t VALUES (?)", &[Value::Bool(true)])
        .await
        .expect("insert");

    let r = backend
        .query(&mut conn, "SELECT flag FROM t", &[])
        .await
        .expect("select");
    assert_eq!(
        r.rows[0][0],
        Value::I64(1),
        "even a column DECLARED BOOLEAN reads back I64: SQLite has no boolean storage class, and \
         the declared type is not enforced, so there is nothing truthful to recover Bool from"
    );
}

/// A `U64` above the signed range is REFUSED pre-send, not wrapped.
#[tokio::test(flavor = "multi_thread")]
async fn u64_beyond_the_signed_range_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "u64.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(&mut conn, "CREATE TABLE t(v)")
        .await
        .expect("seed");

    // In range: fine.
    backend
        .query(&mut conn, "INSERT INTO t VALUES (?)", &[Value::U64(7)])
        .await
        .expect("a u64 inside i64 range binds");

    let err = backend
        .query(
            &mut conn,
            "INSERT INTO t VALUES (?)",
            &[Value::U64(u64::MAX)],
        )
        .await
        .expect_err("u64::MAX has no SQLite representation");
    assert!(
        matches!(&err, ferro_pool::error::PoolError::Unsupported(m) if m.contains("unsigned")),
        "refused loudly and pre-send, never wrapped to a negative: {err:?}"
    );
}

/// `affected` and `last_insert_id` on the row path, including the sticky-rowid guard.
#[tokio::test(flavor = "multi_thread")]
async fn affected_and_last_insert_id_are_not_stale() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "keys.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
        )
        .await
        .expect("seed");

    let ins = backend
        .query(&mut conn, "INSERT INTO t(v) VALUES (?)", &[Value::I64(1)])
        .await
        .expect("insert");
    assert_eq!(ins.affected, 1);
    assert_eq!(
        ins.last_insert_id,
        Some(1),
        "the INSERT reports the key it actually generated"
    );

    let sel = backend
        .query(&mut conn, "SELECT * FROM t", &[])
        .await
        .expect("select");
    assert_eq!(sel.affected, 0, "a SELECT affects nothing");
    assert_eq!(
        sel.last_insert_id, None,
        "THE STICKY GUARD: last_insert_rowid() still reports 1 here, exactly as changes() would. \
         Carrying it over would be a silently WRONG key on a statement that generated none — and \
         §22.2 already records (from PG's lastval()) that a wrong key is worse than no key."
    );
}

/// An empty result still describes its columns; the advisory tag is NULL with no row to describe.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_result_still_names_its_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "empty.db");
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(&mut conn, "CREATE TABLE t(a, b)")
        .await
        .expect("seed");

    let r = backend
        .query(&mut conn, "SELECT a, b FROM t", &[])
        .await
        .expect("query");
    assert!(r.rows.is_empty());
    let names: Vec<&str> = r.cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["a", "b"],
        "column NAMES come from the prepared statement, so they are correct even with no rows"
    );
}

/// An expression column has no declared type at all — the second half of why the mapping cannot
/// key on one.
#[tokio::test(flavor = "multi_thread")]
async fn expression_columns_carry_no_declared_type_yet_map_fine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = backend_on(&dir, "expr.db");
    let mut conn = backend.connect().await.expect("connect");

    let r = backend
        .query(
            &mut conn,
            "SELECT 1+1 AS sum, 'lit' AS s, 1.0*2 AS f, NULL AS nil",
            &[],
        )
        .await
        .expect("query");
    assert_eq!(
        r.rows[0],
        vec![
            Value::I64(2),
            Value::Text("lit".into()),
            Value::F64(2.0),
            Value::Null
        ],
        "SQLite reports NO declared type for any of these columns, yet every VALUE has a storage \
         class — which is the whole argument for tagging per value"
    );
}
