//! One direct connection per pool for `ferro check` and `ferro schema sync` (M3-D2b), through the
//! SAME backend crates `ferrod` uses — so a statement is described with exactly the type mapping
//! the engine will apply when it runs it.
//!
//! Pools are configured exactly as for `ferrod`: `FERRO_POOLS` plus `FERRO_POOL_<NAME>_DSN`, read
//! through `ferrod::config`. A DSN therefore never appears on a command line (where `ps` shows it)
//! and is never printed: errors name the POOL only.

use ferro_backend_mysql::MysqlBackend;
use ferro_backend_pg::PgBackend;
use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::backend::{Describe, PoolBackend};
use ferro_proto::value::Value;
use ferrod::config::{PoolKind, PoolSpec};

/// A connected backend.
pub enum Db {
    Pg(PgBackend, <PgBackend as PoolBackend>::Conn),
    Mysql(MysqlBackend, <MysqlBackend as PoolBackend>::Conn),
    Sqlite(SqliteBackend, <SqliteBackend as PoolBackend>::Conn),
}

impl Db {
    pub async fn connect(spec: &PoolSpec) -> Result<Self, String> {
        // The caller already names the pool (review F12: it appeared twice). The backend's own
        // reason is logged at `warn` (RUST_LOG=warn), never a DSN.
        let fail = |e: ferro_pool::error::PoolError| {
            format!("cannot connect: {e} (set RUST_LOG=warn for the backend's reason)")
        };
        Ok(match spec.kind {
            PoolKind::Postgres => {
                let b = PgBackend::new(spec.dsn.clone());
                let c = b.connect().await.map_err(fail)?;
                Db::Pg(b, c)
            }
            PoolKind::Mysql => {
                let b = MysqlBackend::new(spec.dsn.clone());
                let c = b.connect().await.map_err(fail)?;
                Db::Mysql(b, c)
            }
            PoolKind::Sqlite => {
                // As `ferrod` builds it (`pools.rs`): the pool's D14 `allow_dir`, and a busy timeout,
                // so a migration behaves exactly as it would through the engine (review F11).
                let mut b = SqliteBackend::new(spec.dsn.clone())
                    .with_busy_timeout(std::time::Duration::from_secs(5));
                if let Some(dir) = spec.allow_dir.as_ref() {
                    b = b.with_allow_dir(dir);
                }
                let c = b.connect().await.map_err(fail)?;
                Db::Sqlite(b, c)
            }
        })
    }

    pub async fn describe(&mut self, sql: &str) -> Result<Describe, String> {
        let r = match self {
            Db::Pg(b, c) => b.describe(c, sql).await,
            Db::Mysql(b, c) => b.describe(c, sql).await,
            Db::Sqlite(b, c) => b.describe(c, sql).await,
        };
        r.map_err(|e| e.to_string())
    }

    /// Run a batch of statements (a migration file) in the text protocol.
    pub async fn batch(&mut self, sql: &str) -> Result<(), String> {
        let r = match self {
            Db::Pg(b, c) => b.simple_query(c, sql).await,
            Db::Mysql(b, c) => b.simple_query(c, sql).await,
            Db::Sqlite(b, c) => b.simple_query(c, sql).await,
        };
        r.map(|_| ()).map_err(|e| e.to_string())
    }

    /// Every row of `sql`.
    pub async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let r = match self {
            Db::Pg(b, c) => b.query(c, sql, &[]).await,
            Db::Mysql(b, c) => b.query(c, sql, &[]).await,
            Db::Sqlite(b, c) => b.query(c, sql, &[]).await,
        }
        .map_err(|e| e.to_string())?;
        Ok(r.rows)
    }

    /// The first column of every row of `sql`, as text. A value that is not text is an ERROR, never
    /// skipped: a skipped catalog name is an object the reset silently fails to drop (review F5).
    pub async fn texts(&mut self, sql: &str) -> Result<Vec<String>, String> {
        self.rows(sql)
            .await?
            .into_iter()
            .map(|row| match row.into_iter().next() {
                Some(Value::Text(s)) => Ok(s),
                Some(Value::Bytes(b)) => {
                    String::from_utf8(b).map_err(|_| "a catalog name is not UTF-8".to_string())
                }
                other => Err(format!("unexpected catalog value {other:?}")),
            })
            .collect()
    }

    /// One integer.
    pub async fn count(&mut self, sql: &str) -> Result<i64, String> {
        match self
            .rows(sql)
            .await?
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
        {
            Some(Value::I64(n)) => Ok(n),
            other => Err(format!("not a count: {other:?}")),
        }
    }

    /// The database the SERVER says this connection is in — never a name parsed out of the DSN.
    /// The drivers resolve a DSN their own way (a `dbname=` query parameter overrides the path on
    /// PostgreSQL; MySQL reads only the first path segment and ignores a `#fragment`), so the only
    /// name the destructive guard may trust is the one the connected server reports (review F1/F2).
    /// For SQLite, the stem of the file `main` is actually open on.
    pub async fn current_database(&mut self) -> Result<String, String> {
        match self {
            Db::Pg(..) => self
                .texts("SELECT current_database()::text")
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| "no current database".to_string()),
            Db::Mysql(..) => {
                let rows = self.rows("SELECT CAST(DATABASE() AS CHAR)").await?;
                match rows.into_iter().next().and_then(|r| r.into_iter().next()) {
                    Some(Value::Text(s)) => Ok(s),
                    _ => Err("the connection has no default database (name one in the DSN)".into()),
                }
            }
            Db::Sqlite(..) => {
                for row in self.rows("PRAGMA database_list").await? {
                    if let (Some(Value::Text(n)), Some(Value::Text(file))) =
                        (row.get(1), row.get(2))
                        && n == "main"
                    {
                        return std::path::Path::new(file)
                            .file_stem()
                            .map(|s| s.to_string_lossy().into_owned())
                            .ok_or_else(|| "the main database has no file".to_string());
                    }
                }
                Err("the main database has no file".to_string())
            }
        }
    }

    /// Whether the connection is inside a transaction (a migration must not leave one open).
    pub fn in_tx(&self) -> bool {
        use ferro_pool::backend::TxStatus;
        let st = match self {
            Db::Pg(b, c) => b.tx_status(c),
            Db::Mysql(b, c) => b.tx_status(c),
            Db::Sqlite(b, c) => b.tx_status(c),
        };
        st != TxStatus::Idle
    }
}

/// A double-quoted identifier (PostgreSQL, SQLite).
pub fn quote_dq(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// A backquoted identifier (MySQL).
pub fn quote_bq(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_dq("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_bq("a`b"), "`a``b`");
    }
}
