//! The `ferro` CLI's contract (M3-D2a): exit 0 + the hash on success, exit 1 + every problem and
//! NOTHING written on invalid input, exit 2 on misuse; and the hash `manifest` prints is the one
//! `manifest-hash` recomputes from the written file.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn ferro(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ferro"))
        .args(args)
        .output()
        .expect("runs")
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ferro-cli-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("q")).unwrap();
    dir
}

fn write(dir: &Path, rel: &str, text: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

#[test]
fn a_valid_directory_writes_the_manifest_and_prints_the_hash_manifest_hash_recomputes() {
    let dir = scratch("ok");
    write(
        &dir,
        "q/users/find.sql",
        "-- ferro:\n--   id: users.find\n--   readonly: true\nSELECT 1\n",
    );
    write(
        &dir,
        "q/users/upsert.sql",
        "-- ferro:\n--   id: users.upsert\n--   idempotent: true\nINSERT INTO u VALUES (?)\n",
    );
    let out = dir.join("manifest.json");
    let o = ferro(&[
        "manifest",
        "--sql",
        dir.join("q").to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let printed = String::from_utf8(o.stdout).unwrap().trim().to_string();
    assert_eq!(printed.len(), 64);

    let again = ferro(&["manifest-hash", out.to_str().unwrap()]);
    assert!(again.status.success());
    assert_eq!(String::from_utf8(again.stdout).unwrap().trim(), printed);
}

#[test]
fn every_problem_is_reported_and_nothing_is_written() {
    let dir = scratch("bad");
    write(
        &dir,
        "q/a.sql",
        "-- ferro:\n--   id: a\n--   idempotent: yes\nSELECT 1\n",
    );
    write(&dir, "q/b.sql", "SELECT 2\n");
    write(&dir, "q/c.sql", "-- ferro:\n--   id: Bad Id\nSELECT 3\n");
    let out = dir.join("manifest.json");
    let o = ferro(&[
        "manifest",
        "--sql",
        dir.join("q").to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8(o.stderr).unwrap();
    assert!(err.contains("exactly `true` or `false`"), "{err}");
    assert!(err.contains("missing front-matter"), "{err}");
    assert!(err.contains("invalid query id `Bad Id`"), "{err}");
    assert!(!out.exists(), "an invalid manifest is never written");
}

#[test]
fn php_queries_merge_with_sql_files_and_a_duplicate_id_across_them_is_refused() {
    let dir = scratch("php");
    write(&dir, "q/a.sql", "-- ferro:\n--   id: shared\nSELECT 1\n");
    write(
        &dir,
        "php.json",
        r#"[{"id":"shared","sql":"SELECT 2","source":"Repo.php:5"}]"#,
    );
    let out = dir.join("manifest.json");
    let o = ferro(&[
        "manifest",
        "--sql",
        dir.join("q").to_str().unwrap(),
        "--php-queries",
        dir.join("php.json").to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        String::from_utf8(o.stderr)
            .unwrap()
            .contains("duplicate query id `shared`")
    );

    // An unknown field from the extractor is refused rather than dropped.
    write(
        &dir,
        "php.json",
        r#"[{"id":"p","sql":"SELECT 2","source":"R.php:1","idempotnet":true}]"#,
    );
    let o = ferro(&[
        "manifest",
        "--php-queries",
        dir.join("php.json").to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8(o.stderr).unwrap().contains("idempotnet"));
}

#[test]
fn misuse_is_exit_2() {
    assert_eq!(ferro(&[]).status.code(), Some(2));
    assert_eq!(
        ferro(&["manifest", "--sql", "x"]).status.code(),
        Some(2),
        "--out is required"
    );
    assert_eq!(ferro(&["nope"]).status.code(), Some(2));
    assert_eq!(ferro(&["manifest-hash"]).status.code(), Some(2));
}

// ---- M3-D2a review round -------------------------------------------------------------------

#[test]
fn an_invalid_php_extracted_id_is_refused_by_validation() {
    // F15: dropping the CLI's `validate()` call let an invalid id from PHP be written.
    let dir = scratch("php-bad-id");
    let json = dir.join("php.json");
    std::fs::write(
        &json,
        r#"[{"id":"Bad Id","sql":"SELECT 1","source":"x.php:3"}]"#,
    )
    .unwrap();
    let out = dir.join("manifest.json");
    let o = ferro(&[
        "manifest",
        "--php-queries",
        json.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("invalid query id `Bad Id`"));
    assert!(!out.exists());
}

#[test]
fn an_empty_manifest_is_refused() {
    let dir = scratch("empty");
    let json = dir.join("php.json");
    std::fs::write(&json, "[]").unwrap();
    let out = dir.join("manifest.json");
    let o = ferro(&[
        "manifest",
        "--php-queries",
        json.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("declares no queries"));
    assert!(!out.exists());
}

#[cfg(unix)]
#[test]
fn a_non_utf8_argument_is_a_usage_error_not_a_panic() {
    // F13: `std::env::args` panicked (exit 101).
    use std::os::unix::ffi::OsStrExt;
    let o = Command::new(env!("CARGO_BIN_EXE_ferro"))
        .arg("manifest-hash")
        .arg(std::ffi::OsStr::from_bytes(b"\xff.json"))
        .output()
        .unwrap();
    assert_eq!(
        o.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[cfg(unix)]
#[test]
fn a_failed_write_leaves_the_previous_manifest_intact() {
    // F11: `fs::write` truncates first, so a write that failed part-way (here: a 1-block file-size
    // limit) replaced a good manifest with a partial one while reporting "nothing was written".
    let dir = scratch("atomic");
    for i in 0..40 {
        write(
            &dir,
            &format!("q/q{i}.sql"),
            &format!(
                "-- ferro:\n--   id: q{i}\nSELECT {i} /* {} */\n",
                "x".repeat(60)
            ),
        );
    }
    let out = dir.join("manifest.json");
    std::fs::write(&out, "PREVIOUS GOOD MANIFEST").unwrap();
    let o = Command::new("sh")
        .arg("-c")
        .arg("trap '' XFSZ; ulimit -f 1; exec \"$0\" \"$@\"")
        .arg(env!("CARGO_BIN_EXE_ferro"))
        .args(["manifest", "--sql"])
        .arg(dir.join("q"))
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    assert_eq!(
        o.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "PREVIOUS GOOD MANIFEST"
    );
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
        .collect();
    assert!(leftovers.is_empty(), "the temporary is removed");
}

// ---- M3-D2b: schema-sync and check (SQLite: no server needed) --------------------------------

fn ferro_env(args: &[&str], dsn: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ferro"))
        .args(args)
        .env("FERRO_POOLS", "default")
        .env("FERRO_POOL_DEFAULT_DSN", dsn)
        .output()
        .expect("runs")
}

fn shadow(dir: &Path, stem: &str) -> String {
    format!("sqlite://{}", dir.join(format!("{stem}.db")).display())
}

/// The guard asks SQLite which file `main` is open on; refused, it touches nothing.
#[test]
fn schema_sync_refuses_a_database_not_named_shadow_and_leaves_it_alone() {
    let dir = scratch("sync-refuse");
    write(&dir, "mig/001.sql", "CREATE TABLE precious (id INTEGER);");
    let prod = shadow(&dir, "app");
    // Put a table there (disposable on purpose), then try again without the override.
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
            "--i-know-this-is-disposable",
        ],
        &prod,
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    write(&dir, "mig/001.sql", "CREATE TABLE other (id INTEGER);");
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &prod,
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("refusing to empty database `app`"));
    write(
        &dir,
        "q/a.sql",
        "-- ferro:\n--   id: a\nSELECT id FROM precious\n",
    );
    let m = dir.join("m.json");
    assert!(
        ferro(&[
            "manifest",
            "--sql",
            dir.join("q").to_str().unwrap(),
            "--out",
            m.to_str().unwrap()
        ])
        .status
        .success()
    );
    assert!(
        ferro_env(&["check", "--manifest", m.to_str().unwrap()], &prod)
            .status
            .success(),
        "the table survived"
    );
}

/// The suffix rule is a SUFFIX: `_shadow` elsewhere in the name does not make a database disposable.
#[test]
fn schema_sync_requires_the_shadow_suffix_not_the_substring() {
    let dir = scratch("sync-suffix");
    write(&dir, "mig/001.sql", "CREATE TABLE t (id INTEGER);");
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &shadow(&dir, "app_shadow_prod"),
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("refusing to empty database `app_shadow_prod`")
    );
}

/// Review F3/F7/F8 on SQLite: the reset drops tables, views and triggers in-database and verifies
/// it; an empty migration file is skipped; a migration that leaves a transaction open fails.
#[test]
fn schema_sync_resets_every_object_and_refuses_an_open_transaction() {
    let dir = scratch("sync-objects");
    write(
        &dir,
        "mig/001.sql",
        "CREATE TABLE a (id INTEGER); CREATE VIEW v AS SELECT id FROM a; CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END;",
    );
    write(&dir, "mig/002.sql", "   \n");
    let dsn = shadow(&dir, "o_shadow");
    for _ in 0..2 {
        let o = ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap(),
            ],
            &dsn,
        );
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }
    write(
        &dir,
        "mig/003.sql",
        "BEGIN; CREATE TABLE late (id INTEGER);",
    );
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &dsn,
    );
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("003.sql") && err.contains("transaction open"),
        "{err}"
    );
    assert!(
        !err.contains("nothing was written"),
        "the database WAS emptied: {err}"
    );
}

#[test]
fn schema_sync_applies_migrations_in_order_and_is_repeatable_then_check_records_shapes() {
    let dir = scratch("sync-ok");
    write(
        &dir,
        "mig/002_posts.sql",
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));",
    );
    write(
        &dir,
        "mig/001_users.sql",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);\nCREATE INDEX ue ON users(email);",
    );
    let dsn = shadow(&dir, "app_shadow");
    for _ in 0..2 {
        let o = ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap(),
            ],
            &dsn,
        );
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }

    write(
        &dir,
        "q/find.sql",
        "-- ferro:\n--   id: users.find\n--   readonly: true\nSELECT id, email FROM users WHERE email = ?\n",
    );
    let m = dir.join("m.json");
    assert!(
        ferro(&[
            "manifest",
            "--sql",
            dir.join("q").to_str().unwrap(),
            "--out",
            m.to_str().unwrap()
        ])
        .status
        .success()
    );
    let out = dir.join("checked.json");
    let o = ferro_env(
        &[
            "check",
            "--manifest",
            m.to_str().unwrap(),
            "--write",
            out.to_str().unwrap(),
        ],
        &dsn,
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let checked: serde_json::Value = serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
    let q = &checked["queries"]["users.find"];
    assert_eq!(q["params"], serde_json::json!([null]));
    assert_eq!(q["columns"][1]["name"], "email");
    assert_eq!(q["columns"][1]["type"], "TEXT");
    // The recorded shapes are not part of the hash.
    let h = |p: &Path| {
        String::from_utf8(ferro(&["manifest-hash", p.to_str().unwrap()]).stdout).unwrap()
    };
    assert_eq!(h(&m), h(&out));
}

#[test]
fn a_failing_migration_names_its_file() {
    let dir = scratch("sync-fail");
    write(&dir, "mig/001.sql", "CREATE TABLE t (id INTEGER);");
    write(&dir, "mig/002.sql", "CREATE TABLE broken (;");
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &shadow(&dir, "x_shadow"),
    );
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("002.sql") && err.contains("migration failed"),
        "{err}"
    );
}

#[test]
fn check_fails_on_a_query_that_does_not_prepare_and_writes_nothing() {
    let dir = scratch("check-fail");
    write(&dir, "mig/001.sql", "CREATE TABLE t (id INTEGER);");
    let dsn = shadow(&dir, "c_shadow");
    assert!(
        ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap()
            ],
            &dsn
        )
        .status
        .success()
    );
    write(
        &dir,
        "q/a.sql",
        "-- ferro:\n--   id: a\nSELECT nope FROM t\n",
    );
    write(
        &dir,
        "q/b.sql",
        "-- ferro:\n--   id: b\nSELECT id FROM missing_table\n",
    );
    let m = dir.join("m.json");
    assert!(
        ferro(&[
            "manifest",
            "--sql",
            dir.join("q").to_str().unwrap(),
            "--out",
            m.to_str().unwrap()
        ])
        .status
        .success()
    );
    let out = dir.join("checked.json");
    let o = ferro_env(
        &[
            "check",
            "--manifest",
            m.to_str().unwrap(),
            "--write",
            out.to_str().unwrap(),
        ],
        &dsn,
    );
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("query `a`") && err.contains("query `b`"),
        "every problem is listed: {err}"
    );
    assert!(!out.exists());
}

#[test]
fn an_unconfigured_pool_is_named_and_a_dsn_is_never_printed() {
    let dir = scratch("check-secret");
    write(
        &dir,
        "q/a.sql",
        "-- ferro:\n--   id: a\n--   pool: reports\nSELECT 1\n",
    );
    let m = dir.join("m.json");
    assert!(
        ferro(&[
            "manifest",
            "--sql",
            dir.join("q").to_str().unwrap(),
            "--out",
            m.to_str().unwrap()
        ])
        .status
        .success()
    );
    let o = ferro_env(
        &["check", "--manifest", m.to_str().unwrap()],
        "postgres://u:SECRETPW@127.0.0.1:1/x_shadow",
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("pool `reports`"));

    // A configured but unreachable pool: the error names the pool and never the credential.
    write(&dir, "q/a.sql", "-- ferro:\n--   id: a\nSELECT 1\n");
    assert!(
        ferro(&[
            "manifest",
            "--sql",
            dir.join("q").to_str().unwrap(),
            "--out",
            m.to_str().unwrap()
        ])
        .status
        .success()
    );
    let o = ferro_env(
        &["check", "--manifest", m.to_str().unwrap()],
        "postgres://u:SECRETPW@127.0.0.1:1/x_shadow",
    );
    assert_eq!(o.status.code(), Some(1));
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(all.contains("pool `default`"), "{all}");
    assert!(
        !all.contains("SECRETPW"),
        "the DSN's password was printed: {all}"
    );
}

// ---- M3-D2b review round: the guard against the REAL servers ----------------------------------

/// `url` with its database path replaced (test-side only; the CLI itself never parses a DSN).
fn with_db(url: &str, db: &str) -> String {
    let (base, _) = url.rsplit_once('/').expect("a DSN with a database path");
    format!("{base}/{db}")
}

fn live(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("skip: {var} unset");
            None
        }
    }
}

/// F1: PostgreSQL resolves `?dbname=` OVER the path, so a DSN whose path says `_shadow` connects to
/// the shared test database. The guard asks the server, which says `ferro`: refused, nothing dropped.
#[test]
fn pg_schema_sync_trusts_the_server_not_the_dsn_and_resets_completely() {
    let Some(url) = live("FERRO_TEST_PG_URL") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    use ferro_pool::backend::PoolBackend;
    let admin = ferro_backend_pg::PgBackend::new(url.clone());
    rt.block_on(async {
        let mut c = admin.connect().await.unwrap();
        let _ = admin
            .simple_query(&mut c, "CREATE DATABASE ferro_cli_shadow")
            .await;
        admin
            .simple_query(
                &mut c,
                "CREATE TABLE IF NOT EXISTS d2b_guard_canary (id int)",
            )
            .await
            .unwrap();
    });
    let dir = scratch("pg-sync");
    write(
        &dir,
        "mig/001.sql",
        "CREATE TABLE users (id bigserial PRIMARY KEY, email text); CREATE SCHEMA audit; CREATE TABLE audit.log (id int); CREATE PUBLICATION d2b_pub FOR TABLE users;",
    );

    let bypass = format!("{}?dbname=ferro", with_db(&url, "x_shadow"));
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &bypass,
    );
    assert_eq!(
        o.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(String::from_utf8_lossy(&o.stderr).contains("refusing to empty database `ferro`"));
    rt.block_on(async {
        let mut c = admin.connect().await.unwrap();
        let r = admin
            .query(&mut c, "SELECT to_regclass('d2b_guard_canary')::text", &[])
            .await
            .unwrap();
        assert_ne!(
            r.rows[0][0],
            ferro_proto::value::Value::Null,
            "the canary survived"
        );
    });

    let shadow_dsn = with_db(&url, "ferro_cli_shadow");
    for _ in 0..2 {
        // Twice: the second reset must remove the first run's schema AND publication.
        let o = ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap(),
            ],
            &shadow_dsn,
        );
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }
}

/// F2: MySQL reads only the first path segment and ignores a `#fragment`. Both bypasses are refused
/// because the server says `ferro`; the shadow database is reset with DROP/CREATE DATABASE, which
/// also removes routines.
#[test]
fn mysql_schema_sync_trusts_the_server_not_the_dsn_and_resets_completely() {
    let Some(url) = live("FERRO_TEST_MYSQL_URL") else {
        return;
    };
    let dir = scratch("my-sync");
    write(
        &dir,
        "mig/001.sql",
        "CREATE TABLE users (id BIGINT PRIMARY KEY); CREATE VIEW v AS SELECT id FROM users; CREATE PROCEDURE d2b_p() SELECT 1;",
    );
    // MySQL refuses an empty query (1065), so an empty file must be skipped, not sent (review F8).
    write(&dir, "mig/002.sql", "\n  \n");
    let base = with_db(&url, "ferro");
    for bypass in [format!("{base}/x_shadow"), format!("{base}#_shadow")] {
        let o = ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap(),
            ],
            &bypass,
        );
        assert_eq!(
            o.status.code(),
            Some(1),
            "{bypass}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        assert!(String::from_utf8_lossy(&o.stderr).contains("refusing to empty database `ferro`"));
    }
    let shadow_dsn = with_db(&url, "ferro_cli_shadow");
    for _ in 0..2 {
        // Twice: a surviving procedure would fail the second run with 1304 (already exists).
        let o = ferro_env(
            &[
                "schema-sync",
                "--migrations",
                dir.join("mig").to_str().unwrap(),
            ],
            &shadow_dsn,
        );
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }
}

// ---- M3-D2c: gen ------------------------------------------------------------------------------

fn checked_manifest(dir: &Path, queries: serde_json::Value) -> PathBuf {
    let p = dir.join("checked.json");
    std::fs::write(
        &p,
        serde_json::json!({"version": 1, "queries": queries}).to_string(),
    )
    .unwrap();
    p
}

fn col(name: &str, tag: Option<u8>, ty: &str) -> serde_json::Value {
    serde_json::json!({"name": name, "tag": tag, "type": ty})
}

fn query(sql: &str, dto: Option<&str>, cols: Option<serde_json::Value>) -> serde_json::Value {
    let mut q =
        serde_json::json!({"sql": sql, "pool": "default", "readonly": true, "idempotent": false});
    if let Some(d) = dto {
        q["dto"] = d.into();
    }
    if let Some(c) = cols {
        q["columns"] = c;
    }
    q
}

fn gen_cmd(m: &Path, out: &Path, extra: &[&str]) -> Output {
    let mut args = vec!["gen", "--manifest", m.to_str().unwrap(), "--out"];
    args.push(out.to_str().unwrap());
    args.extend_from_slice(extra);
    ferro(&args)
}

#[test]
fn gen_writes_a_nullable_dto_per_class_a_queries_class_and_the_manifest() {
    let dir = scratch("gen-ok");
    let m = checked_manifest(
        &dir,
        serde_json::json!({
            // A fully-qualified spelling (one leading `\`) names the same class.
            "users.find": query("SELECT 1", Some("\\App\\Dto\\UserRow"), Some(serde_json::json!([
                col("id", Some(2), "int8"), col("created_at", Some(11), "timestamptz"),
                col("balance", Some(5), "numeric"), col("extra", None, ""),
                // A server type name is copied into a comment: `*/` must not close it.
                col("odd", Some(6), "text */ ?> <?php echo 1; /*")]))),
            // Same names and tags, different server type names: one class serves both.
            "users.find2": query("SELECT 2", Some("App\\Dto\\UserRow"), Some(serde_json::json!([
                col("id", Some(2), "int4"), col("created_at", Some(11), "timestamptz"),
                col("balance", Some(5), "numeric"), col("extra", None, ""),
                col("odd", Some(6), "varchar")]))),
            "users.touch": {"sql": "UPDATE u SET t = 1", "pool": "default", "readonly": false, "idempotent": true}
        }),
    );
    let out = dir.join("gen");
    let o = gen_cmd(&m, &out, &["--queries-class", "\\App\\Ferro\\Queries"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let dto = std::fs::read_to_string(out.join("UserRow.php")).unwrap();
    assert!(dto.contains("namespace App\\Dto;\n"), "{dto}");
    assert!(dto.contains("final readonly class UserRow"));
    for want in [
        "public ?int $id,",
        "public \\DateTimeImmutable|string|null $createdAt,",
        "public ?\\Ferro\\Decimal $balance,",
        "public mixed $extra,",
        "/** `odd` (text *\\/ ?> <?php echo 1; /*) */",
    ] {
        assert!(dto.contains(want), "missing `{want}` in:\n{dto}");
    }
    assert_eq!(
        dto.matches("*/").count(),
        6,
        "only the comments' own closers:\n{dto}"
    );
    let q = std::fs::read_to_string(out.join("Queries.php")).unwrap();
    assert!(q.contains("namespace App\\Ferro;\n"), "{q}");
    assert!(
        q.contains("public const USERS_FIND = 'users.find';")
            && q.contains("public const USERS_TOUCH = 'users.touch';"),
        "{q}"
    );
    let hash = String::from_utf8(ferro(&["manifest-hash", m.to_str().unwrap()]).stdout).unwrap();
    assert!(q.contains(&format!("MANIFEST_HASH = '{}'", hash.trim())));
    assert!(out.join("manifest.json").exists());
    let marker = std::fs::read_to_string(out.join(".ferro-gen")).unwrap();
    for f in ["UserRow.php", "Queries.php", "manifest.json"] {
        assert!(marker.lines().any(|l| l == f), "{marker}");
    }
    if php_available() {
        for f in ["UserRow.php", "Queries.php"] {
            let lint = Command::new("php")
                .arg("-l")
                .arg(out.join(f))
                .output()
                .unwrap();
            assert!(
                lint.status.success(),
                "{f}: {}",
                String::from_utf8_lossy(&lint.stdout)
            );
        }
    }
}

#[test]
fn gen_refuses_what_it_cannot_generate_and_writes_nothing() {
    let dir = scratch("gen-bad");
    let i64c = |n: &str| col(n, Some(2), "int8");
    let m = checked_manifest(
        &dir,
        serde_json::json!({
            "a.unchecked": query("SELECT 1", Some("App\\A"), None),
            "b.one": query("SELECT 1", Some("App\\B"), Some(serde_json::json!([i64c("id")]))),
            "b.two": query("SELECT 2", Some("App\\B"), Some(serde_json::json!([col("name", Some(6), "text")]))),
            "b.tag": query("SELECT 3", Some("App\\Bt"), Some(serde_json::json!([i64c("id")]))),
            "b.tag2": query("SELECT 4", Some("App\\Bt"), Some(serde_json::json!([col("id", Some(6), "text")]))),
            "c.expr": query("SELECT count(*)", Some("App\\C"), Some(serde_json::json!([i64c("count(*)")]))),
            "d.camel": query("SELECT 1", Some("App\\D"), Some(serde_json::json!([i64c("user_id"), i64c("userId")]))),
            "e.one": query("SELECT 1", Some("App\\E"), Some(serde_json::json!([i64c("a"), i64c("b")]))),
            "e.two": query("SELECT 2", Some("App\\E"), Some(serde_json::json!([i64c("b"), i64c("a")]))),
            "f.one": query("SELECT 1", Some("App\\X\\Row"), Some(serde_json::json!([i64c("id")]))),
            "f.two": query("SELECT 2", Some("App\\Y\\Row"), Some(serde_json::json!([i64c("id")]))),
            "g.one": query("SELECT 1", Some("App\\user"), Some(serde_json::json!([i64c("id")]))),
            "g.two": query("SELECT 2", Some("App\\User"), Some(serde_json::json!([i64c("id")]))),
            "h.parent": query("SELECT 1", Some("App\\Parent"), Some(serde_json::json!([i64c("id")]))),
            "h.leading": query("SELECT 1", Some("\\\\App\\Two"), Some(serde_json::json!([i64c("id")]))),
            "i.dup": query("SELECT 1", Some("App\\I"), Some(serde_json::json!([i64c("id"), i64c("id")]))),
            "j.tag": query("SELECT 1", Some("App\\J"), Some(serde_json::json!([col("a", Some(14), "int4[]")]))),
            "k.this": query("SELECT 1", Some("App\\K"), Some(serde_json::json!([i64c("this")]))),
            "q.same": query("SELECT 1", Some("App\\Queries"), Some(serde_json::json!([i64c("id")]))),
            "class": query("SELECT 1", None, None),
            "manifest.hash": query("SELECT 1", None, None),
            "x.y": query("SELECT 1", None, None),
            "x-y": query("SELECT 1", None, None)
        }),
    );
    let out = dir.join("gen");
    let o = gen_cmd(&m, &out, &["--queries-class", "App\\Queries"]);
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    for want in [
        "run `ferro check --write`",
        "`App\\B` is declared by `b.one` and `b.two` with different columns",
        "`App\\Bt` is declared by `b.tag` and `b.tag2` with different columns",
        "the same columns in a different order",
        "column `count(*)` is not a PHP identifier: alias it in the SQL",
        "column `userId` cannot be matched back by the hydrator",
        "would both be written to `Row.php`",
        "are ONE PHP class",
        "`Parent` is reserved",
        "`\\App\\Two` is not a usable PHP class name",
        "column `id` appears twice",
        "column `a` has §9 tag Some(14), which the client cannot decode",
        "column `this` would be the parameter `$this`, which PHP forbids",
        "`App\\Queries` is also a declared dto",
        "query `class`: its constant would be `CLASS`, which PHP forbids",
        "query `manifest.hash`: its constant would be `MANIFEST_HASH`",
        "its constant `X_Y` collides with query",
        "nothing was written",
    ] {
        assert!(err.contains(want), "missing `{want}` in: {err}");
    }
    assert!(!out.exists(), "nothing was written");
}

#[test]
fn gen_refuses_an_unusable_queries_class() {
    let dir = scratch("gen-qc");
    let m = checked_manifest(
        &dir,
        serde_json::json!({"a.b": query("SELECT 1", None, None)}),
    );
    for bad in ["App\\9x", "App\\List", "\\\\App\\Q", "Namespace\\Q"] {
        let out = dir.join("gen");
        let o = gen_cmd(&m, &out, &["--queries-class", bad]);
        assert_eq!(o.status.code(), Some(1), "{bad}");
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(err.contains("--queries-class: "), "{bad}: {err}");
        assert!(!out.exists(), "{bad}");
    }
}

#[test]
fn gen_touches_nothing_when_a_target_cannot_be_written_and_a_rerun_removes_stale_files() {
    let dir = scratch("gen-rewrite");
    let one = |dto: &str| serde_json::json!({"a.q": query("SELECT 1", Some(dto), Some(serde_json::json!([col("id", Some(2), "int8")])))});
    let out = dir.join("gen");
    let ma = checked_manifest(&dir, one("App\\Old"));
    assert!(gen_cmd(&ma, &out, &[]).status.success());
    std::fs::write(out.join("Mine.php"), "<?php // hand-written").unwrap();
    let queries_before = std::fs::read_to_string(out.join("Queries.php")).unwrap();

    // A target that is not a regular file: refused before ANY file is replaced.
    std::fs::remove_file(out.join("manifest.json")).unwrap();
    std::fs::create_dir(out.join("manifest.json")).unwrap();
    let mb = checked_manifest(&dir, one("App\\Fresh"));
    let o = gen_cmd(&mb, &out, &[]);
    assert_eq!(o.status.code(), Some(1));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("is not a regular file") && err.contains("nothing was written"),
        "{err}"
    );
    assert!(!out.join("Fresh.php").exists());
    assert_eq!(
        std::fs::read_to_string(out.join("Queries.php")).unwrap(),
        queries_before
    );
    assert!(out.join("Old.php").exists());
    let leftovers: Vec<_> = std::fs::read_dir(&out)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".ferro-tmp-"))
        .collect();
    assert!(leftovers.is_empty(), "no temporary is left behind");

    // Fixed and re-run: the class no longer generated is removed; a hand-written file is not.
    std::fs::remove_dir(out.join("manifest.json")).unwrap();
    let o = gen_cmd(&mb, &out, &[]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(String::from_utf8_lossy(&o.stderr).contains("Old.php"));
    assert!(!out.join("Old.php").exists());
    assert!(out.join("Fresh.php").exists());
    assert!(out.join("Mine.php").exists());
}

fn php_available() -> bool {
    let ok = Command::new("php")
        .arg("-v")
        .output()
        .is_ok_and(|o| o.status.success());
    if !ok {
        // "skip:" so CI's no-skip gate (ci/assert-no-skips.sh) fails the lane if PHP is missing.
        eprintln!("skip: no `php` on PATH; the PHP half of this test did not run");
    }
    ok
}

/// The generated DTOs, filled by the REAL client: `ExecCodec::decodeRow` under the DEFAULT
/// `M1ValuePolicy`, then `ExecCodec::hydrateDto` (`PlanCache` → `HydrationPlan`). Rows are
/// hand-built wire cells, NOT read through `ferrod`: every one of the 14 tags, an all-NULL row,
/// PostgreSQL's sentinels (`infinity` timestamps, `NaN`, `24:00:00`, a U64 above PHP_INT_MAX) and
/// MySQL's zero timestamps; plus a DTO of column names whose camelCase does not round-trip.
#[test]
fn generated_dtos_hydrate_through_the_real_php_client() {
    if !php_available() {
        return;
    }
    let dir = scratch("gen-hydrate");
    let tags: [(&str, Option<u8>); 15] = [
        ("c_null", Some(0)),
        ("c_bool", Some(1)),
        ("c_i64", Some(2)),
        ("c_u64", Some(3)),
        ("c_f64", Some(4)),
        ("c_decimal", Some(5)),
        ("c_text", Some(6)),
        ("c_bytes", Some(7)),
        ("c_date", Some(8)),
        ("c_time", Some(9)),
        ("c_ts", Some(10)),
        ("c_tstz", Some(11)),
        ("c_uuid", Some(12)),
        ("c_json", Some(13)),
        ("c_untyped", None),
    ];
    let wide: Vec<_> = tags.iter().map(|(n, t)| col(n, *t, "t")).collect();
    let names = [
        "x_y_z",
        "a_b_c",
        "is_a_b",
        "__id",
        "userId",
        "ID",
        "a__b",
        "id_",
        "a_1",
        "v1_a_b",
        "created_at",
        "é_x",
        "plan_a_price",
        "col_A",
        "_",
    ];
    let named: Vec<_> = names.iter().map(|n| col(n, Some(2), "int8")).collect();
    let m = checked_manifest(
        &dir,
        serde_json::json!({
            "wide.q": query("SELECT 1", Some("App\\Gen\\Wide"), Some(serde_json::Value::Array(wide))),
            "names.q": query("SELECT 1", Some("App\\Gen\\Names"), Some(serde_json::Value::Array(named)))
        }),
    );
    let out = dir.join("gen");
    let o = gen_cmd(&m, &out, &[]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let script = dir.join("hydrate.php");
    std::fs::write(&script, HYDRATE_PHP).unwrap();
    let client_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../php/client/src");
    let r = Command::new("php")
        .arg(&script)
        .arg(&client_src)
        .arg(&out)
        .arg(serde_json::to_string(&names).unwrap())
        .output()
        .unwrap();
    assert!(
        r.status.success() && r.stdout == b"OK",
        "{}{}",
        String::from_utf8_lossy(&r.stdout),
        String::from_utf8_lossy(&r.stderr)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

const HYDRATE_PHP: &str = r#"<?php
declare(strict_types=1);

[, $src, $out, $namesJson] = $argv;
spl_autoload_register(static function (string $c) use ($src, $out): void {
    if (str_starts_with($c, 'Ferro\\')) {
        $p = $src . '/' . str_replace('\\', '/', substr($c, 6)) . '.php';
    } elseif (str_starts_with($c, 'App\\Gen\\')) {
        $p = $out . '/' . substr($c, 8) . '.php';
    } else {
        return;
    }
    if (is_file($p)) {
        require $p;
    }
});

use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Protocol\Msgpack\PurePacker;

$codec = new ExecCodec(new M1ValuePolicy(), new PlanCache(), new PurePacker(), new PurePacker());

/** Hydrate one row and prove column i landed in constructor parameter i, value for value. */
function check(ExecCodec $codec, string $class, array $cols, array $cells): void
{
    $row = $codec->decodeRow($cells);
    $dto = $codec->hydrateDto($class, $cols, $row);
    $params = (new ReflectionClass($class))->getConstructor()->getParameters();
    if (count($params) !== count($cols)) {
        throw new RuntimeException("$class: arity");
    }
    foreach ($params as $i => $p) {
        $got = $dto->{$p->getName()};
        $want = $row[$i];
        $nan = is_float($got) && is_float($want) && is_nan($got) && is_nan($want);
        if ($got !== $want && !$nan) {
            throw new RuntimeException("$class: column {$cols[$i]} is not in \${$p->getName()}");
        }
    }
}

$cell = static fn (int $tag, mixed $data): array => ['tag' => $tag, 'data' => $data];
$wideCols = ['c_null', 'c_bool', 'c_i64', 'c_u64', 'c_f64', 'c_decimal', 'c_text', 'c_bytes',
    'c_date', 'c_time', 'c_ts', 'c_tstz', 'c_uuid', 'c_json', 'c_untyped'];
$uuid = '0f8fad5b-d9cb-469f-a165-70867728950e';
$rows = [
    'all NULL' => array_fill(0, 15, $cell(0, null)),
    'plain' => [$cell(0, null), $cell(1, true), $cell(2, 7), $cell(3, 5), $cell(4, 1.5),
        $cell(5, '1.10'), $cell(6, 'x'), $cell(7, "\x00\xff"), $cell(8, '2026-10-06'),
        $cell(9, '12:34:56'), $cell(10, '2026-10-06 12:00:00.123456'),
        $cell(11, '2026-10-06T12:00:00Z'), $cell(12, $uuid), $cell(13, '{"a":1}'), $cell(6, 'any')],
    'postgres sentinels' => [$cell(0, null), $cell(1, false), $cell(2, PHP_INT_MIN),
        $cell(3, '18446744073709551615'), $cell(4, NAN), $cell(5, 'NaN'), $cell(6, ''),
        $cell(7, ''), $cell(8, 'infinity'), $cell(9, '24:00:00'), $cell(10, 'infinity'),
        $cell(11, '-infinity'), $cell(12, $uuid), $cell(13, 'null'), $cell(2, 1)],
    'mysql zeros' => [$cell(0, null), $cell(1, true), $cell(2, 0), $cell(3, 0), $cell(4, -INF),
        $cell(5, '-Infinity'), $cell(6, 'y'), $cell(7, 'z'), $cell(8, '-infinity'),
        $cell(9, '-838:59:59'), $cell(10, '0000-00-00 00:00:00'),
        $cell(11, '0000-00-00 00:00:00'), $cell(12, $uuid), $cell(13, '[]'), $cell(0, null)],
];
foreach ($rows as $label => $cells) {
    try {
        check($codec, App\Gen\Wide::class, $wideCols, $cells);
    } catch (Throwable $e) {
        throw new RuntimeException("row `$label`: " . get_class($e) . ': ' . $e->getMessage(), 0, $e);
    }
}

$names = json_decode($namesJson, true);
check($codec, App\Gen\Names::class, $names, array_map(static fn (int $i): array => $cell(2, 1000 + $i), array_keys($names)));
check($codec, App\Gen\Names::class, $names, array_fill(0, count($names), $cell(0, null)));
echo 'OK';
"#;
