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
