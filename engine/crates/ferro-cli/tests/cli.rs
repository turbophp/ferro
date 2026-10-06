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

#[test]
fn schema_sync_refuses_a_database_not_named_shadow_and_leaves_it_alone() {
    let dir = scratch("sync-refuse");
    write(&dir, "mig/001.sql", "CREATE TABLE t (id INTEGER);");
    let db = dir.join("app.db");
    std::fs::write(&db, "precious").unwrap();
    let o = ferro_env(
        &[
            "schema-sync",
            "--migrations",
            dir.join("mig").to_str().unwrap(),
        ],
        &shadow(&dir, "app"),
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("does not end in `_shadow`"));
    assert_eq!(
        std::fs::read_to_string(&db).unwrap(),
        "precious",
        "nothing was touched"
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
