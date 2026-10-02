//! M2-C1g: SQLite advertises `literals_are_standard = Some(true)` — checked against the PROPERTY.

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::PoolBackend;
use ferro_proto::value::Value;

/// The bit claims a backslash is an ORDINARY character in a string literal and that a doubled `'`
/// is the whole quoting rule. Both are asserted against SQLite itself, because the bit is what a
/// client builds literals from: `'a\b'` must be three characters (a backslash escape would make it
/// two) and `'it''s'` must read back as `it's`.
#[tokio::test(flavor = "multi_thread")]
async fn literals_are_standard_is_a_constant_of_the_library() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = SqliteBackend::new(format!("sqlite://{}", dir.path().join("lit.db").display()));
    let mut conn = backend.connect().await.expect("connect");

    assert_eq!(backend.literals_are_standard(&conn), Some(true));

    let r = backend
        .query(
            &mut conn,
            r"SELECT length('a\b'), 'it''s', 'a\' || 'b'",
            &[],
        )
        .await
        .expect("query");
    assert_eq!(r.rows[0][0], Value::I64(3), "a backslash is an ordinary character");
    assert_eq!(r.rows[0][1], Value::Text("it's".into()), "a doubled quote is the escape");
    // A value ENDING in a backslash, quoted by doubling only: the backslash does not consume the
    // closing quote, which is the property a quoting function depends on.
    assert_eq!(r.rows[0][2], Value::Text(r"a\b".into()));
}
