//! M3-D2b: `PoolBackend::describe` — prepare without running, against a real PostgreSQL
//! (`FERRO_TEST_PG_URL`). The table is a TEMP table on the same connection, so the test touches no
//! shared fixture.

use ferro_backend_pg::PgBackend;
use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;
use ferro_proto::consts::tag;

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

#[tokio::test]
async fn describe_reports_param_types_and_column_tags_without_running() {
    let Some(url) = test_url() else { return };
    let b = PgBackend::new(url);
    let mut c = b.connect().await.expect("connect");
    b.simple_query(
        &mut c,
        "CREATE TEMP TABLE d2b_t (id bigint PRIMARY KEY, email text NOT NULL, at timestamptz, n numeric(10,2))",
    )
    .await
    .expect("temp table");

    let d = b
        .describe(
            &mut c,
            "SELECT id, email, at, n FROM d2b_t WHERE email = ? AND id > ?",
        )
        .await
        .expect("describes");
    assert_eq!(
        d.params,
        vec![Some("text".to_string()), Some("int8".to_string())]
    );
    let cols: Vec<_> = d
        .cols
        .iter()
        .map(|c| (c.name.as_str(), c.tag, c.type_name.as_str()))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("id", Some(tag::I64), "int8"),
            ("email", Some(tag::TEXT), "text"),
            ("at", Some(tag::TIMESTAMPTZ), "timestamptz"),
            ("n", Some(tag::DECIMAL), "numeric"),
        ]
    );

    // NOT run: describing a DELETE leaves the row alone.
    b.simple_query(&mut c, "INSERT INTO d2b_t VALUES (1, 'a', now(), 1)")
        .await
        .unwrap();
    let d = b
        .describe(&mut c, "DELETE FROM d2b_t WHERE id = ?")
        .await
        .expect("describes");
    assert!(d.cols.is_empty());
    let left = b
        .query(&mut c, "SELECT count(*) FROM d2b_t", &[])
        .await
        .unwrap();
    assert_eq!(
        left.rows[0][0],
        ferro_proto::value::Value::I64(1),
        "describe did not run the DELETE"
    );
}

#[tokio::test]
async fn describe_fails_loudly_for_what_check_must_catch() {
    let Some(url) = test_url() else { return };
    let b = PgBackend::new(url);
    let mut c = b.connect().await.expect("connect");
    // An unknown relation and a syntax error are the statement's own SQL errors.
    assert!(matches!(
        b.describe(&mut c, "SELECT * FROM d2b_no_such_table").await,
        Err(PoolError::Sql { .. })
    ));
    assert!(matches!(
        b.describe(&mut c, "SELEC 1").await,
        Err(PoolError::Sql { .. })
    ));
    // A column type the engine cannot carry is refused at check time, not at the first run.
    match b.describe(&mut c, "SELECT point(1, 2) AS p").await {
        Err(PoolError::Unsupported(m)) => assert!(m.contains('p'), "{m}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}
