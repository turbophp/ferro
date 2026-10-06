//! `ferro` — the checked-SQL CLI (SPEC §11, D10).
//!
//! M3-D2a ships the manifest half:
//!
//! ```text
//! ferro manifest --sql <dir> [--sql <dir>…] [--php-queries <file.json>…] --out <manifest.json>
//! ferro manifest-hash <manifest.json>
//! ```
//!
//! M3-D2b adds the schema half:
//!
//! ```text
//! ferro schema-sync --migrations <dir> [--pool <name>] [--i-know-this-is-disposable]
//! ferro check --manifest <manifest.json> [--write <manifest.json>]
//! ```
//!
//! Both read their database connections exactly as `ferrod` does (`FERRO_POOLS` +
//! `FERRO_POOL_<NAME>_DSN`), so a DSN is never on a command line and never printed (`db.rs`).
//! `ferro gen` (DTOs, stubs) follows in D2c. Arguments are parsed by hand: a dozen flags do not
//! justify a dependency in a binary that ships beside a credential-holding daemon.
//!
//! Exit codes: 0 success, 1 the queries or manifest are invalid (every problem is printed), 2 usage.

use std::path::PathBuf;
use std::process::ExitCode;

use ferro_manifest::{Column, Manifest, Problem, collect_sql_dir};

mod db;

const USAGE: &str = "\
usage:
  ferro manifest --sql <dir> [--sql <dir>...] [--php-queries <file.json>...] --out <manifest.json>
      Collect every query from `.sql` files (with a `-- ferro:` front-matter block) and from the
      JSON that `vendor/bin/ferro-queries` prints for #[FerroQuery] attributes, validate them, and
      write the manifest. Prints the manifest hash.
  ferro manifest-hash <manifest.json>
      Load and validate a manifest and print the hash the engine and client will compare. The hash
      is recomputed from the queries; the copy recorded in the file is not trusted.
  ferro schema-sync --migrations <dir> [--pool <name>] [--i-know-this-is-disposable]
      EMPTY the pool's database and apply every `*.sql` file in <dir>, in file-name order. Refused
      unless the database's name ends in `_shadow` (a SQLite file's stem), because it destroys
      everything in it. Connections come from FERRO_POOLS / FERRO_POOL_<NAME>_DSN, as for ferrod.
  ferro check --manifest <manifest.json> [--write <manifest.json>]
      PREPARE every query against its pool's (shadow) database without running it: a syntax error,
      an unknown relation or a column type the engine cannot carry fails, every problem listed.
      --write records each query's parameter and column descriptions (not part of the hash).
";

fn main() -> ExitCode {
    // Backend diagnostics (a connect failure's real reason) are `tracing` events; shown only when
    // asked for with RUST_LOG, on stderr. The backends never put a DSN in them.
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }
    // `args_os`, not `args`: `std::env::args` PANICS on a non-UTF-8 argument (exit 101, outside
    // the documented codes). A path that is not UTF-8 is a usage error here (M3-D2a review F13).
    let mut args = Vec::new();
    for a in std::env::args_os().skip(1) {
        match a.into_string() {
            Ok(s) => args.push(s),
            Err(a) => {
                return usage_error(&format!("argument is not UTF-8: {}", a.to_string_lossy()));
            }
        }
    }
    match args.first().map(String::as_str) {
        Some("manifest") => cmd_manifest(&args[1..]),
        Some("manifest-hash") => cmd_manifest_hash(&args[1..]),
        Some("check") => block_on(cmd_check(&args[1..])),
        Some("schema-sync") => block_on(cmd_schema_sync(&args[1..])),
        Some("-V" | "--version") => {
            println!("ferro {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some("-h" | "--help" | "help") => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        _ => usage_error("missing or unknown command"),
    }
}

fn usage_error(why: &str) -> ExitCode {
    eprintln!("ferro: {why}\n\n{USAGE}");
    ExitCode::from(2)
}

fn report(problems: &[Problem]) -> ExitCode {
    for p in problems {
        eprintln!("error: {p}");
    }
    eprintln!("ferro: {} problem(s); nothing was written", problems.len());
    ExitCode::from(1)
}

fn cmd_manifest(args: &[String]) -> ExitCode {
    let mut sql_dirs: Vec<PathBuf> = Vec::new();
    let mut php_files: Vec<PathBuf> = Vec::new();
    let mut out: Option<PathBuf> = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let Some(v) = it.next() else {
            return usage_error(&format!("`{flag}` needs a value"));
        };
        match flag.as_str() {
            "--sql" => sql_dirs.push(PathBuf::from(v)),
            "--php-queries" => php_files.push(PathBuf::from(v)),
            "--out" if out.is_none() => out = Some(PathBuf::from(v)),
            "--out" => return usage_error("`--out` given twice"),
            other => return usage_error(&format!("unknown flag `{other}`")),
        }
    }
    let Some(out) = out else {
        return usage_error("`--out` is required");
    };
    if sql_dirs.is_empty() && php_files.is_empty() {
        return usage_error("give at least one `--sql` directory or `--php-queries` file");
    }

    let mut manifest = Manifest::new();
    let mut problems = Vec::new();
    for dir in &sql_dirs {
        collect_sql_dir(dir, &mut manifest, &mut problems);
    }
    for file in &php_files {
        let at = file.display().to_string();
        let bytes = match std::fs::read(file) {
            Ok(b) => b,
            Err(e) => {
                problems.push(Problem {
                    at,
                    message: format!("cannot read: {e}"),
                });
                continue;
            }
        };
        match ferro_manifest::parse_extracted(&bytes) {
            Ok(list) => {
                for q in list {
                    let (id, query) = q.into_query();
                    if let Err(p) = manifest.insert(id, query) {
                        problems.push(p);
                    }
                }
            }
            Err(e) => problems.push(Problem {
                at,
                message: format!("not a ferro-queries JSON list: {e}"),
            }),
        }
    }
    // `validate` also refuses an empty manifest.
    problems.extend(manifest.validate());
    if !problems.is_empty() {
        return report(&problems);
    }
    if let Err(e) = write_atomically(&out, manifest.to_json_with_hash().as_bytes()) {
        return report(&[Problem {
            at: out.display().to_string(),
            message: format!("cannot write: {e}"),
        }]);
    }
    println!("{}", manifest.hash());
    eprintln!(
        "ferro: wrote {} quer{} to {}",
        manifest.queries.len(),
        if manifest.queries.len() == 1 {
            "y"
        } else {
            "ies"
        },
        out.display()
    );
    ExitCode::SUCCESS
}

/// Write `bytes` to `path` so a reader sees the old file or the new one, never a truncated mix:
/// write a temporary beside it, flush it to disk, then rename over the target. `fs::write`
/// truncates first, so a failed write (a full disk, a file-size limit) used to replace a good
/// manifest with a partial one while the tool reported "nothing was written" (M3-D2a review F11).
fn write_atomically(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "manifest.json".into());
    let tmp = dir.join(format!(".{name}.tmp-{}", std::process::id()));
    let result = (|| {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn cmd_manifest_hash(args: &[String]) -> ExitCode {
    let [path] = args else {
        return usage_error("`manifest-hash` takes exactly one file");
    };
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            return report(&[Problem {
                at: path.clone(),
                message: format!("cannot read: {e}"),
            }]);
        }
    };
    match Manifest::from_json(&bytes) {
        Ok(m) => {
            println!("{}", m.hash());
            ExitCode::SUCCESS
        }
        Err(problems) => report(&problems),
    }
}

fn block_on(f: impl std::future::Future<Output = ExitCode>) -> ExitCode {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt.block_on(f),
        Err(e) => {
            eprintln!("ferro: cannot start the async runtime: {e}");
            ExitCode::from(1)
        }
    }
}

/// The configured pool named `name`, or a problem naming the variables to set.
fn pool_spec(name: &str) -> Result<ferrod::config::PoolSpec, Problem> {
    ferrod::config::Config::from_env()
        .pools
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| Problem {
            at: format!("pool `{name}`"),
            message: "is not configured: set FERRO_POOLS and FERRO_POOL_<NAME>_DSN as for ferrod"
                .into(),
        })
}

async fn cmd_check(args: &[String]) -> ExitCode {
    let mut manifest_path: Option<PathBuf> = None;
    let mut write: Option<PathBuf> = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let Some(v) = it.next() else {
            return usage_error(&format!("`{flag}` needs a value"));
        };
        match flag.as_str() {
            "--manifest" if manifest_path.is_none() => manifest_path = Some(PathBuf::from(v)),
            "--write" if write.is_none() => write = Some(PathBuf::from(v)),
            "--manifest" | "--write" => return usage_error(&format!("`{flag}` given twice")),
            other => return usage_error(&format!("unknown flag `{other}`")),
        }
    }
    let Some(manifest_path) = manifest_path else {
        return usage_error("`--manifest` is required");
    };
    let bytes = match std::fs::read(&manifest_path) {
        Ok(b) => b,
        Err(e) => {
            return report(&[Problem {
                at: manifest_path.display().to_string(),
                message: format!("cannot read: {e}"),
            }]);
        }
    };
    let mut manifest = match Manifest::from_json(&bytes) {
        Ok(m) => m,
        Err(problems) => return report(&problems),
    };

    let mut problems = Vec::new();
    let pools: std::collections::BTreeSet<String> =
        manifest.queries.values().map(|q| q.pool.clone()).collect();
    let mut conns = std::collections::BTreeMap::new();
    for pool in pools {
        match pool_spec(&pool) {
            Ok(spec) => match db::Db::connect(&spec).await {
                Ok(c) => {
                    conns.insert(pool, c);
                }
                Err(m) => problems.push(Problem {
                    at: format!("pool `{pool}`"),
                    message: m,
                }),
            },
            Err(p) => problems.push(p),
        }
    }
    if !problems.is_empty() {
        return report(&problems);
    }

    for (id, q) in manifest.queries.iter_mut() {
        let at = q.source.clone().unwrap_or_else(|| id.clone());
        let Some(conn) = conns.get_mut(&q.pool) else {
            continue;
        };
        match conn.describe(&q.sql).await {
            Ok(d) => {
                q.params = Some(d.params);
                q.columns = Some(
                    d.cols
                        .into_iter()
                        .map(|c| Column {
                            name: c.name,
                            tag: c.tag,
                            type_name: c.type_name,
                        })
                        .collect(),
                );
            }
            Err(m) => problems.push(Problem {
                at,
                message: format!("query `{id}` does not prepare on pool `{}`: {m}", q.pool),
            }),
        }
    }
    if !problems.is_empty() {
        return report(&problems);
    }
    if let Some(out) = write
        && let Err(e) = write_atomically(&out, manifest.to_json_with_hash().as_bytes())
    {
        return report(&[Problem {
            at: out.display().to_string(),
            message: format!("cannot write: {e}"),
        }]);
    }
    eprintln!(
        "ferro: {} quer{} checked",
        manifest.queries.len(),
        if manifest.queries.len() == 1 {
            "y"
        } else {
            "ies"
        }
    );
    ExitCode::SUCCESS
}

async fn cmd_schema_sync(args: &[String]) -> ExitCode {
    let mut pool = "default".to_string();
    let mut migrations: Option<PathBuf> = None;
    let mut disposable = false;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        if flag == "--i-know-this-is-disposable" {
            disposable = true;
            continue;
        }
        let Some(v) = it.next() else {
            return usage_error(&format!("`{flag}` needs a value"));
        };
        match flag.as_str() {
            "--pool" => pool = v.clone(),
            "--migrations" if migrations.is_none() => migrations = Some(PathBuf::from(v)),
            "--migrations" => return usage_error("`--migrations` given twice"),
            other => return usage_error(&format!("unknown flag `{other}`")),
        }
    }
    let Some(migrations) = migrations else {
        return usage_error("`--migrations` is required");
    };
    let spec = match pool_spec(&pool) {
        Ok(s) => s,
        Err(p) => return report(&[p]),
    };
    let at = format!("pool `{pool}`");

    let mut files: Vec<PathBuf> = match std::fs::read_dir(&migrations) {
        Ok(rd) => rd
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().is_some_and(|e| e.eq_ignore_ascii_case("sql")))
            .collect(),
        Err(e) => {
            return report(&[Problem {
                at: migrations.display().to_string(),
                message: format!("cannot read directory: {e}"),
            }]);
        }
    };
    files.sort();

    let mut conn = match db::Db::connect(&spec).await {
        Ok(c) => c,
        Err(m) => return report(&[Problem { at, message: m }]),
    };

    // THE GUARD: this EMPTIES a database. The name checked is the one the SERVER reports for the
    // connection — never one parsed out of the DSN, which the drivers resolve differently (a
    // `?dbname=` overrides the path on PostgreSQL; MySQL reads only the first path segment and
    // ignores a `#fragment`), so a parsed name let a crafted DSN empty a production database
    // (review F1/F2). The name is printed; the DSN never is.
    let name = match conn.current_database().await {
        Ok(n) => n,
        Err(m) => {
            return report(&[Problem {
                at,
                message: format!("cannot tell which database this is: {m}"),
            }]);
        }
    };
    if !disposable && !name.ends_with("_shadow") {
        return report(&[Problem {
            at,
            message: format!(
                "refusing to empty database `{name}`: its name does not end in `_shadow` (pass \
                 --i-know-this-is-disposable only for a database you can lose)"
            ),
        }]);
    }

    if let Err(m) = reset(&mut conn, &name).await {
        return report_after_reset(&at, &format!("could not empty database `{name}`: {m}"));
    }
    let mut applied = 0usize;
    for file in &files {
        let sql = match std::fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                return report_after_reset(
                    &file.display().to_string(),
                    &format!(
                        "cannot read: {e} — database `{name}` was emptied and {applied} of {} migrations applied",
                        files.len()
                    ),
                );
            }
        };
        if sql.trim().is_empty() {
            // An empty file is nothing to apply. (MySQL refuses an empty query; review F8.)
            continue;
        }
        if let Err(m) = conn.batch(&sql).await {
            return report_after_reset(
                &file.display().to_string(),
                &format!(
                    "migration failed: {m} — database `{name}` was emptied and {applied} of {} migrations applied before it",
                    files.len()
                ),
            );
        }
        if conn.in_tx() {
            // A migration that leaves a transaction open would be rolled back when this process
            // exits, losing it and every later migration while reporting success (review F7).
            return report_after_reset(
                &file.display().to_string(),
                "the migration left a transaction open: end it with COMMIT (or remove the BEGIN)",
            );
        }
        applied += 1;
    }
    eprintln!(
        "ferro: database `{name}` (pool `{pool}`) emptied and {applied} migration{} applied",
        if applied == 1 { "" } else { "s" }
    );
    ExitCode::SUCCESS
}

/// A failure AFTER the database was emptied: the usual "nothing was written" would be false.
fn report_after_reset(at: &str, message: &str) -> ExitCode {
    eprintln!("error: {at}: {message}");
    ExitCode::from(1)
}

/// Empty the connected database `name`, then VERIFY it is empty — a reset that silently left
/// objects behind used to report success (review F3/F5).
///
/// - **PostgreSQL:** every non-system schema, ENUMERATED from `pg_namespace` (a hand-kept list
///   measurably rotted in the DBAL harness), every publication, every large object, and the
///   database's own settings (`ALTER DATABASE … RESET ALL` — a stale `search_path` changes how
///   `check` resolves names); `public` is recreated with PUBLIC's USAGE, as a fresh database has it.
/// - **MySQL/MariaDB:** `DROP DATABASE` and `CREATE DATABASE` with the original character set and
///   collation — the only reset that also removes routines, events, sequences and versioned tables.
/// - **SQLite:** every table, view and trigger in `sqlite_master` is dropped in-database, with
///   foreign keys off; nothing deletes the file, so a path trick cannot point the drop and the open
///   at different files.
async fn reset(conn: &mut db::Db, name: &str) -> Result<(), String> {
    match conn {
        db::Db::Pg(..) => {
            let schemas = conn
                .texts(
                    "SELECT nspname::text FROM pg_namespace WHERE nspname NOT IN \
                     ('pg_catalog', 'information_schema') AND nspname NOT LIKE 'pg\\_%'",
                )
                .await?;
            let pubs = conn
                .texts("SELECT pubname::text FROM pg_publication")
                .await?;
            let mut sql: String = pubs
                .iter()
                .map(|p| format!("DROP PUBLICATION {};", db::quote_dq(p)))
                .collect();
            for s in &schemas {
                sql.push_str(&format!("DROP SCHEMA {} CASCADE;", db::quote_dq(s)));
            }
            sql.push_str("SELECT lo_unlink(oid) FROM pg_largeobject_metadata;");
            sql.push_str(&format!("ALTER DATABASE {} RESET ALL;", db::quote_dq(name)));
            sql.push_str("CREATE SCHEMA public; GRANT USAGE ON SCHEMA public TO PUBLIC;");
            conn.batch(&sql).await?;
            let left = conn
                .count(
                    "SELECT count(*)::int8 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') \
                     AND n.nspname NOT LIKE 'pg\\_%'",
                )
                .await?;
            if left != 0 {
                return Err(format!("{left} relation(s) survived the reset"));
            }
            Ok(())
        }
        db::Db::Mysql(..) => {
            let row = conn
                .rows(
                    "SELECT CAST(DEFAULT_CHARACTER_SET_NAME AS CHAR), CAST(DEFAULT_COLLATION_NAME AS CHAR) \
                     FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE()",
                )
                .await?;
            let text = |v: Option<&ferro_proto::value::Value>| match v {
                Some(ferro_proto::value::Value::Text(s)) => Ok(s.clone()),
                other => Err(format!("unexpected catalog value {other:?}")),
            };
            let first = row
                .first()
                .ok_or("the database is not in information_schema")?;
            let (charset, collation) = (text(first.first())?, text(first.get(1))?);
            let q = db::quote_bq(name);
            conn.batch(&format!(
                "DROP DATABASE {q}; CREATE DATABASE {q} CHARACTER SET {} COLLATE {}; USE {q};",
                db::quote_bq(&charset),
                db::quote_bq(&collation)
            ))
            .await?;
            let left = conn
                .count(
                    "SELECT CAST(COUNT(*) AS SIGNED) FROM information_schema.tables WHERE table_schema = DATABASE()",
                )
                .await?;
            if left != 0 {
                return Err(format!("{left} table(s) survived the reset"));
            }
            Ok(())
        }
        db::Db::Sqlite(..) => {
            let objects = conn
                .rows(
                    "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\' \
                     AND type IN ('table', 'view', 'trigger')",
                )
                .await?;
            let mut sql = String::from("PRAGMA foreign_keys = OFF;");
            for o in &objects {
                let (
                    Some(ferro_proto::value::Value::Text(kind)),
                    Some(ferro_proto::value::Value::Text(obj)),
                ) = (o.first(), o.get(1))
                else {
                    return Err(format!("unexpected catalog row {o:?}"));
                };
                let verb = match kind.as_str() {
                    "table" => "TABLE",
                    "view" => "VIEW",
                    _ => "TRIGGER",
                };
                sql.push_str(&format!("DROP {verb} IF EXISTS {};", db::quote_dq(obj)));
            }
            sql.push_str("PRAGMA foreign_keys = ON;");
            conn.batch(&sql).await?;
            let left = conn
                .count("SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'")
                .await?;
            if left != 0 {
                return Err(format!("{left} object(s) survived the reset"));
            }
            Ok(())
        }
    }
}
