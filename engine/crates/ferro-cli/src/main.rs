//! `ferro` — the checked-SQL CLI (SPEC §11, D10).
//!
//! M3-D2a ships the manifest half:
//!
//! ```text
//! ferro manifest --sql <dir> [--sql <dir>…] [--php-queries <file.json>…] --out <manifest.json>
//! ferro manifest-hash <manifest.json>
//! ```
//!
//! `ferro check` (PREPARE every query against a shadow schema) and `ferro gen` (DTOs, stubs) follow
//! in D2b/D2c. Arguments are parsed by hand: a dozen flags do not justify a dependency in a binary
//! that ships beside a credential-holding daemon.
//!
//! Exit codes: 0 success, 1 the queries or manifest are invalid (every problem is printed), 2 usage.

use std::path::PathBuf;
use std::process::ExitCode;

use ferro_manifest::{Manifest, Problem, collect_sql_dir};

const USAGE: &str = "\
usage:
  ferro manifest --sql <dir> [--sql <dir>...] [--php-queries <file.json>...] --out <manifest.json>
      Collect every query from `.sql` files (with a `-- ferro:` front-matter block) and from the
      JSON that `vendor/bin/ferro-queries` prints for #[FerroQuery] attributes, validate them, and
      write the manifest. Prints the manifest hash.
  ferro manifest-hash <manifest.json>
      Load and validate a manifest and print the hash the engine and client will compare. The hash
      is recomputed from the queries; the copy recorded in the file is not trusted.
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
