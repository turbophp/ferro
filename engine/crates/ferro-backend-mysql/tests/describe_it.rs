//! M3-D2b: `PoolBackend::describe` against a real MySQL/MariaDB (`FERRO_TEST_MYSQL_URL`). The table
//! is a TEMPORARY table on the same connection.

use ferro_backend_mysql::MysqlBackend;
use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;
use ferro_proto::consts::tag;

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MYSQL_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: FERRO_TEST_MYSQL_URL unset");
            None
        }
    }
}

#[tokio::test]
async fn describe_counts_params_and_tags_columns_without_running() {
    let Some(url) = test_url() else { return };
    let b = MysqlBackend::new(url);
    let mut c = b.connect().await.expect("connect");
    b.simple_query(
        &mut c,
        "CREATE TEMPORARY TABLE d2b_t (id BIGINT PRIMARY KEY, email VARCHAR(100) NOT NULL, active TINYINT(1))",
    )
    .await
    .expect("temp table");
    let d = b
        .describe(
            &mut c,
            "SELECT id, email, active FROM d2b_t WHERE email = ? AND id > ?",
        )
        .await
        .expect("describes");
    assert_eq!(
        d.params,
        vec![None, None],
        "MySQL infers no parameter types"
    );
    let tags: Vec<_> = d.cols.iter().map(|c| (c.name.as_str(), c.tag)).collect();
    assert_eq!(
        tags,
        vec![
            ("id", Some(tag::I64)),
            ("email", Some(tag::TEXT)),
            ("active", Some(tag::BOOL))
        ]
    );

    b.simple_query(&mut c, "INSERT INTO d2b_t VALUES (1, 'a', 1)")
        .await
        .unwrap();
    b.describe(&mut c, "DELETE FROM d2b_t WHERE id = ?")
        .await
        .expect("describes");
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
    let b = MysqlBackend::new(url);
    let mut c = b.connect().await.expect("connect");
    assert!(matches!(
        b.describe(&mut c, "SELECT * FROM d2b_no_such_table").await,
        Err(PoolError::Sql { .. })
    ));
    assert!(matches!(
        b.describe(&mut c, "SELEC 1").await,
        Err(PoolError::Sql { .. })
    ));
}
