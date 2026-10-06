//! M3-D2b: `PoolBackend::describe` on SQLite (no server). Columns have no §9 tag here — SQLite types
//! each value, not the column — so the declared type is what `ferro check` can record.

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;

#[tokio::test]
async fn describe_counts_params_and_passes_declared_types_through() {
    let dir = tempfile::tempdir().unwrap();
    let b = SqliteBackend::new(format!("sqlite://{}", dir.path().join("d.db").display()));
    let mut c = b.connect().await.expect("connect");
    b.simple_query(
        &mut c,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
    )
    .await
    .unwrap();
    let d = b
        .describe(
            &mut c,
            "SELECT id, email, id + 1 AS next FROM t WHERE email = ?",
        )
        .await
        .expect("describes");
    assert_eq!(d.params, vec![None]);
    let cols: Vec<_> = d
        .cols
        .iter()
        .map(|c| (c.name.as_str(), c.tag, c.type_name.as_str()))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", None, "INTEGER"),
            ("email", None, "TEXT"),
            ("next", None, "")
        ]
    );

    b.simple_query(&mut c, "INSERT INTO t VALUES (1, 'a')")
        .await
        .unwrap();
    b.describe(&mut c, "DELETE FROM t")
        .await
        .expect("describes");
    let left = b
        .query(&mut c, "SELECT count(*) FROM t", &[])
        .await
        .unwrap();
    assert_eq!(
        left.rows[0][0],
        ferro_proto::value::Value::I64(1),
        "describe did not run the DELETE"
    );

    assert!(matches!(
        b.describe(&mut c, "SELECT nope FROM t").await,
        Err(PoolError::Sql { .. })
    ));
}
