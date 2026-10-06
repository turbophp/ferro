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
        let fail =
            |e: ferro_pool::error::PoolError| format!("pool `{}`: cannot connect: {e}", spec.name);
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
                let b = SqliteBackend::new(spec.dsn.clone());
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

    /// The first column of every row of `sql`, as text (for the reset's catalog enumeration).
    pub async fn texts(&mut self, sql: &str) -> Result<Vec<String>, String> {
        let r = match self {
            Db::Pg(b, c) => b.query(c, sql, &[]).await,
            Db::Mysql(b, c) => b.query(c, sql, &[]).await,
            Db::Sqlite(b, c) => b.query(c, sql, &[]).await,
        }
        .map_err(|e| e.to_string())?;
        Ok(r.rows
            .into_iter()
            .filter_map(|row| match row.into_iter().next() {
                Some(Value::Text(s)) => Some(s),
                Some(Value::Bytes(b)) => String::from_utf8(b).ok(),
                _ => None,
            })
            .collect())
    }
}

/// The database a DSN names, for `schema sync`'s disposable-database check: the path segment of a
/// server DSN (`scheme://…/NAME?…`), the file stem of a SQLite one. `None` if it names none.
pub fn database_name(spec: &PoolSpec) -> Option<String> {
    let dsn = spec.dsn.as_str();
    match spec.kind {
        PoolKind::Sqlite => {
            let path = dsn.strip_prefix("sqlite://").unwrap_or(dsn);
            let path = path.split('?').next().unwrap_or(path);
            std::path::Path::new(path)
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
        }
        _ => {
            let rest = dsn.split_once("://")?.1;
            let rest = rest.rsplit_once('@').map_or(rest, |(_, after)| after);
            let path = rest.split_once('/')?.1;
            let name = path.split('?').next().unwrap_or(path);
            (!name.is_empty()).then(|| name.to_string())
        }
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

    fn spec(dsn: &str) -> PoolSpec {
        PoolSpec {
            name: "p".into(),
            dsn: dsn.into(),
            kind: ferrod::config::infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
            allow_dir: None,
        }
    }

    #[test]
    fn the_database_name_is_read_from_each_dsn_shape() {
        assert_eq!(
            database_name(&spec("postgres://u:p@h:5432/app_shadow")).as_deref(),
            Some("app_shadow")
        );
        assert_eq!(
            database_name(&spec("postgres://u:p@h/app_shadow?sslmode=disable")).as_deref(),
            Some("app_shadow")
        );
        // A password containing `/` and `@` must not be read as the path.
        assert_eq!(
            database_name(&spec("mysql://u:p/@x@h:3306/db_shadow")).as_deref(),
            Some("db_shadow")
        );
        assert_eq!(database_name(&spec("postgres://u:p@h:5432")), None);
        assert_eq!(
            database_name(&spec("sqlite:///tmp/x/app_shadow.db")).as_deref(),
            Some("app_shadow")
        );
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_dq("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_bq("a`b"), "`a``b`");
    }
}
