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

    // The guard: this EMPTIES a database. Name-based, because the name is the one thing a shadow
    // database reliably has that a production one does not.
    let name = db::database_name(&spec);
    if !disposable && !name.as_deref().is_some_and(|n| n.ends_with("_shadow")) {
        return report(&[Problem {
            at,
            message: format!(
                "refusing to empty database {}: its name does not end in `_shadow` (pass \
                 --i-know-this-is-disposable only for a database you can lose)",
                name.map_or_else(|| "<unnamed>".to_string(), |n| format!("`{n}`"))
            ),
        }]);
    }

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

    if spec.kind == ferrod::config::PoolKind::Sqlite {
        let path = spec.dsn.strip_prefix("sqlite://").unwrap_or(&spec.dsn);
        let path = path.split('?').next().unwrap_or(path);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
    let mut conn = match db::Db::connect(&spec).await {
        Ok(c) => c,
        Err(m) => return report(&[Problem { at, message: m }]),
    };
    if let Err(m) = reset(&mut conn).await {
        return report(&[Problem {
            at,
            message: format!("cannot empty the database: {m}"),
        }]);
    }
    for file in &files {
        let sql = match std::fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                return report(&[Problem {
                    at: file.display().to_string(),
                    message: format!("cannot read: {e}"),
                }]);
            }
        };
        if let Err(m) = conn.batch(&sql).await {
            return report(&[Problem {
                at: file.display().to_string(),
                message: format!("migration failed: {m}"),
            }]);
        }
    }
    eprintln!(
        "ferro: pool `{pool}` emptied and {} migration{} applied",
        files.len(),
        if files.len() == 1 { "" } else { "s" }
    );
    ExitCode::SUCCESS
}

/// Empty the connected database: every non-system schema on PostgreSQL (enumerated, never a fixed
/// list — a hand-kept list measurably rotted in the DBAL harness), every table and view on MySQL.
/// A SQLite database was deleted before connecting.
async fn reset(conn: &mut db::Db) -> Result<(), String> {
    match conn {
        db::Db::Pg(..) => {
            let schemas = conn
                .texts(
                    "SELECT nspname::text FROM pg_namespace WHERE nspname NOT IN \
                     ('pg_catalog', 'information_schema') AND nspname NOT LIKE 'pg\\_%'",
                )
                .await?;
            let mut sql: String = schemas
                .iter()
                .map(|s| format!("DROP SCHEMA {} CASCADE;", db::quote_dq(s)))
                .collect();
            sql.push_str("CREATE SCHEMA public;");
            conn.batch(&sql).await
        }
        db::Db::Mysql(..) => {
            let views = conn
                .texts(
                    "SELECT CAST(table_name AS CHAR) FROM information_schema.tables \
                     WHERE table_schema = DATABASE() AND table_type = 'VIEW'",
                )
                .await?;
            let tables = conn
                .texts(
                    "SELECT CAST(table_name AS CHAR) FROM information_schema.tables \
                     WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
                )
                .await?;
            let mut sql = String::from("SET FOREIGN_KEY_CHECKS = 0;");
            for v in &views {
                sql.push_str(&format!("DROP VIEW IF EXISTS {};", db::quote_bq(v)));
            }
            for t in &tables {
                sql.push_str(&format!("DROP TABLE IF EXISTS {};", db::quote_bq(t)));
            }
            sql.push_str("SET FOREIGN_KEY_CHECKS = 1;");
            conn.batch(&sql).await
        }
        db::Db::Sqlite(..) => Ok(()),
    }
}
