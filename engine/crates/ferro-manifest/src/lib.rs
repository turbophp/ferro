//! The checked-SQL manifest (SPEC §11, M3-D2).
//!
//! A manifest maps a stable `query_id` to the SQL the engine will run for it, the pool it runs on,
//! and two declarations the engine and client act on: `readonly` (the §19.3 fate declaration) and
//! `idempotent` (the ONLY licence to retry an `Indeterminate` write, §9.2).
//!
//! This crate is shared by the `ferro` CLI, which BUILDS a manifest, and `ferrod`, which LOADS one,
//! so both compute the same [`Manifest::hash`] from the same canonical bytes. The hash is what the
//! HELLO handshake compares (§5): a client built against a different manifest is refused before it
//! can run the wrong SQL under a known id.
//!
//! **The SQL is never rewritten** (charter rule 6). A query's `sql` is the text as written minus its
//! front-matter, trimmed of surrounding whitespace, and the engine executes exactly that string.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The manifest format version. A manifest of another version is refused, never guessed at.
pub const FORMAT_VERSION: u32 = 1;

/// The longest `query_id` accepted.
pub const MAX_ID_LEN: usize = 128;

/// One declared query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    /// The SQL the engine runs for this id, exactly as written (minus front-matter, trimmed).
    pub sql: String,
    /// The pool it runs on.
    pub pool: String,
    /// The §19.3 fate declaration: a lost `readonly` statement is Retryable, any other Indeterminate.
    pub readonly: bool,
    /// The licence to retry an `Indeterminate` write (§9.2). Defaults to `false`.
    pub idempotent: bool,
    /// A DTO class for `ferro gen` (code generation only; not part of the hash).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dto: Option<String>,
    /// Where the query was declared, for error messages (not part of the hash).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// A whole manifest: every declared query, keyed by id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub queries: BTreeMap<String, Query>,
}

/// A problem found while collecting or validating queries. Collection reports ALL of them, not
/// only the first, so one CI run names every mistake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// Where (a path, `path:line`, or an id).
    pub at: String,
    pub message: String,
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.at, self.message)
    }
}

/// The fields that decide what runs: exactly these are hashed, in this shape.
#[derive(Serialize)]
struct Hashed<'a> {
    sql: &'a str,
    pool: &'a str,
    readonly: bool,
    idempotent: bool,
}

impl Manifest {
    /// An empty manifest of the current version.
    pub fn new() -> Self {
        Self {
            version: FORMAT_VERSION,
            queries: BTreeMap::new(),
        }
    }

    /// The manifest hash: lowercase hex SHA-256 over the canonical bytes
    /// `{"v":1,"q":{<id>:{"sql":…,"pool":…,"readonly":…,"idempotent":…},…}}` with ids in byte
    /// order and no whitespace. `dto` and `source` are deliberately NOT in it: moving a file or
    /// renaming a DTO changes nothing the engine runs.
    pub fn hash(&self) -> String {
        hex(&Sha256::digest(self.canonical_bytes()))
    }

    /// The exact bytes [`Manifest::hash`] digests.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let q: BTreeMap<&str, Hashed<'_>> = self
            .queries
            .iter()
            .map(|(id, q)| {
                (
                    id.as_str(),
                    Hashed {
                        sql: &q.sql,
                        pool: &q.pool,
                        readonly: q.readonly,
                        idempotent: q.idempotent,
                    },
                )
            })
            .collect();
        #[derive(Serialize)]
        struct Canonical<'a> {
            v: u32,
            q: BTreeMap<&'a str, Hashed<'a>>,
        }
        // Serialising a struct of strings, bools and a BTreeMap cannot fail.
        serde_json::to_vec(&Canonical { v: self.version, q }).unwrap_or_default()
    }

    /// Add a query, refusing a duplicate id (both declarations are named).
    pub fn insert(&mut self, id: String, query: Query) -> Result<(), Problem> {
        if let Some(existing) = self.queries.get(&id) {
            return Err(Problem {
                at: query.source.clone().unwrap_or_else(|| id.clone()),
                message: format!(
                    "duplicate query id `{id}` (also declared at {})",
                    existing.source.as_deref().unwrap_or("an earlier source")
                ),
            });
        }
        self.queries.insert(id, query);
        Ok(())
    }

    /// Check every entry: ids, pools and SQL. Returns every problem found.
    pub fn validate(&self) -> Vec<Problem> {
        let mut problems = Vec::new();
        if self.version != FORMAT_VERSION {
            problems.push(Problem {
                at: "manifest".into(),
                message: format!(
                    "unsupported manifest version {} (this build reads {FORMAT_VERSION})",
                    self.version
                ),
            });
        }
        for (id, q) in &self.queries {
            let at = q.source.clone().unwrap_or_else(|| id.clone());
            if let Err(m) = check_id(id) {
                problems.push(Problem {
                    at: at.clone(),
                    message: m,
                });
            }
            if let Err(m) = check_pool(&q.pool) {
                problems.push(Problem {
                    at: at.clone(),
                    message: m,
                });
            }
            if q.sql.trim().is_empty() {
                problems.push(Problem {
                    at: at.clone(),
                    message: format!("query `{id}` has no SQL"),
                });
            }
            if q.sql != q.sql.trim() {
                problems.push(Problem {
                    at,
                    message: format!(
                        "query `{id}`'s SQL has surrounding whitespace (it must be stored trimmed)"
                    ),
                });
            }
        }
        problems
    }

    /// Parse a manifest from JSON and validate it. A manifest that does not validate is refused
    /// whole: the engine never runs part of one.
    pub fn from_json(bytes: &[u8]) -> Result<Self, Vec<Problem>> {
        let m: Manifest = serde_json::from_slice(bytes).map_err(|e| {
            vec![Problem {
                at: "manifest".into(),
                message: format!("not a valid manifest: {e}"),
            }]
        })?;
        let problems = m.validate();
        if problems.is_empty() {
            Ok(m)
        } else {
            Err(problems)
        }
    }

    /// The manifest as pretty JSON, with its hash recorded beside it for the client to read. The
    /// `hash` field is informational: a loader recomputes the hash and never trusts this copy.
    pub fn to_json_with_hash(&self) -> String {
        #[derive(Serialize)]
        struct WithHash<'a> {
            version: u32,
            hash: String,
            queries: &'a BTreeMap<String, Query>,
        }
        serde_json::to_string_pretty(&WithHash {
            version: self.version,
            hash: self.hash(),
            queries: &self.queries,
        })
        .unwrap_or_default()
            + "\n"
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

/// A query id: 1–128 characters of `a-z 0-9 _ . -`, starting with a letter or digit. Ids are
/// stable names used from PHP, so they are kept to a set that needs no escaping anywhere.
pub fn check_id(id: &str) -> Result<(), String> {
    let ok_first = id
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let ok_rest = id
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | '-'));
    if id.is_empty() || id.len() > MAX_ID_LEN || !ok_first || !ok_rest {
        return Err(format!(
            "invalid query id `{id}`: use 1-{MAX_ID_LEN} characters of a-z, 0-9, `_`, `.`, `-`, starting with a letter or digit"
        ));
    }
    Ok(())
}

/// A pool name: non-empty `A-Z a-z 0-9 _ -`, as `ferrod`'s pool configuration names them.
pub fn check_pool(pool: &str) -> Result<(), String> {
    if pool.is_empty()
        || !pool
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        return Err(format!(
            "invalid pool name `{pool}`: use A-Z, a-z, 0-9, `_`, `-`"
        ));
    }
    Ok(())
}

/// Parse one `.sql` file: a front-matter block, then the SQL.
///
/// ```sql
/// -- ferro:
/// --   id: users.find_by_email
/// --   pool: default
/// --   readonly: true
/// --   idempotent: false
/// --   dto: App\Dto\User
/// SELECT id, email FROM users WHERE email = ?
/// ```
///
/// The block starts with a line that is exactly `-- ferro:` (after leading blank lines) and runs
/// over the following `--` comment lines of `key: value`. `id` is required; `pool` defaults to
/// `default`, `readonly` and `idempotent` to `false`. An unknown key, a repeated key, or a boolean
/// that is not exactly `true`/`false` is an error — a typo in `idempotent` must not silently
/// license or forbid a retry. A file without the block is an error too: every file in a query
/// directory declares a query.
pub fn parse_sql_file(source: &str, text: &str) -> Result<(String, Query), Vec<Problem>> {
    let mut problems = Vec::new();
    let mut lines = text.lines().enumerate().peekable();
    while lines.peek().is_some_and(|(_, l)| l.trim().is_empty()) {
        lines.next();
    }
    match lines.next() {
        Some((_, l)) if l.trim_end() == "-- ferro:" => {}
        _ => {
            return Err(vec![Problem {
                at: source.into(),
                message: "missing front-matter: the file must start with a `-- ferro:` block"
                    .into(),
            }]);
        }
    }

    let mut fields: BTreeMap<String, (usize, String)> = BTreeMap::new();
    let mut body_start = None;
    for (idx, line) in lines.by_ref() {
        let Some(rest) = line.strip_prefix("--") else {
            body_start = Some(idx);
            break;
        };
        let rest = rest.trim();
        if rest.is_empty() {
            continue;
        }
        let at = format!("{source}:{}", idx + 1);
        let Some((key, value)) = rest.split_once(':') else {
            problems.push(Problem {
                at,
                message: format!("front-matter line is not `key: value`: `{rest}`"),
            });
            continue;
        };
        let (key, value) = (key.trim().to_string(), value.trim().to_string());
        if !matches!(
            key.as_str(),
            "id" | "pool" | "readonly" | "idempotent" | "dto"
        ) {
            problems.push(Problem {
                at,
                message: format!("unknown front-matter key `{key}`"),
            });
            continue;
        }
        if fields.contains_key(&key) {
            problems.push(Problem {
                at,
                message: format!("front-matter key `{key}` is repeated"),
            });
            continue;
        }
        fields.insert(key, (idx + 1, value));
    }

    let body: String = match body_start {
        Some(start) => text.lines().skip(start).collect::<Vec<_>>().join("\n"),
        None => String::new(),
    };
    let sql = body.trim().to_string();

    let flag = |key: &str, problems: &mut Vec<Problem>| -> bool {
        match fields.get(key) {
            None => false,
            Some((_, v)) if v == "true" => true,
            Some((_, v)) if v == "false" => false,
            Some((line, v)) => {
                problems.push(Problem {
                    at: format!("{source}:{line}"),
                    message: format!("`{key}` must be exactly `true` or `false`, got `{v}`"),
                });
                false
            }
        }
    };
    let readonly = flag("readonly", &mut problems);
    let idempotent = flag("idempotent", &mut problems);

    let id = match fields.get("id") {
        Some((_, id)) => id.clone(),
        None => {
            problems.push(Problem {
                at: source.into(),
                message: "front-matter has no `id`".into(),
            });
            String::new()
        }
    };
    let pool = fields
        .get("pool")
        .map(|(_, p)| p.clone())
        .unwrap_or_else(|| "default".into());
    let dto = fields
        .get("dto")
        .map(|(_, d)| d.clone())
        .filter(|d| !d.is_empty());

    let query = Query {
        sql,
        pool,
        readonly,
        idempotent,
        dto,
        source: Some(source.into()),
    };
    if !id.is_empty()
        && let Err(m) = check_id(&id)
    {
        problems.push(Problem {
            at: source.into(),
            message: m,
        });
    }
    if let Err(m) = check_pool(&query.pool) {
        problems.push(Problem {
            at: source.into(),
            message: m,
        });
    }
    if query.sql.is_empty() {
        problems.push(Problem {
            at: source.into(),
            message: "no SQL after the front-matter".into(),
        });
    }
    if problems.is_empty() {
        Ok((id, query))
    } else {
        Err(problems)
    }
}

/// A query declared by a `#[FerroQuery(...)]` attribute, as `ferro/client`'s `bin/ferro-queries`
/// extractor prints it (PHP's own tokenizer is the only robust PHP parser available, so the PHP
/// side extracts and this side validates).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractedQuery {
    pub id: String,
    pub sql: String,
    #[serde(default = "default_pool")]
    pub pool: String,
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub idempotent: bool,
    #[serde(default)]
    pub dto: Option<String>,
    pub source: String,
}

/// Parse the JSON list `bin/ferro-queries` prints. Unknown fields are refused, so a field the PHP
/// side adds later cannot be silently dropped here.
pub fn parse_extracted(bytes: &[u8]) -> Result<Vec<ExtractedQuery>, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}

fn default_pool() -> String {
    "default".into()
}

impl ExtractedQuery {
    /// Into a manifest entry: the SQL is trimmed (only), as for a `.sql` file.
    pub fn into_query(self) -> (String, Query) {
        (
            self.id,
            Query {
                sql: self.sql.trim().to_string(),
                pool: self.pool,
                readonly: self.readonly,
                idempotent: self.idempotent,
                dto: self.dto,
                source: Some(self.source),
            },
        )
    }
}

/// Collect every `*.sql` file under `dir` (recursively, in path order) into `manifest`.
pub fn collect_sql_dir(dir: &Path, manifest: &mut Manifest, problems: &mut Vec<Problem>) {
    let mut files = Vec::new();
    walk(dir, &mut files, problems);
    files.sort();
    for path in files {
        let source = path.display().to_string();
        match std::fs::read_to_string(&path) {
            Ok(text) => match parse_sql_file(&source, &text) {
                Ok((id, q)) => {
                    if let Err(p) = manifest.insert(id, q) {
                        problems.push(p);
                    }
                }
                Err(ps) => problems.extend(ps),
            },
            Err(e) => problems.push(Problem {
                at: source,
                message: format!("cannot read: {e}"),
            }),
        }
    }
}

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>, problems: &mut Vec<Problem>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            problems.push(Problem {
                at: dir.display().to_string(),
                message: format!("cannot read directory: {e}"),
            });
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out, problems);
        } else if path.extension().is_some_and(|e| e == "sql") {
            out.push(path);
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str =
        "-- ferro:\n--   id: users.find\n--   readonly: true\nSELECT id FROM users WHERE id = ?\n";

    #[test]
    fn a_sql_file_parses_its_front_matter_and_keeps_the_sql_verbatim() {
        let (id, q) = parse_sql_file("q.sql", FILE).expect("parses");
        assert_eq!(id, "users.find");
        assert_eq!(q.sql, "SELECT id FROM users WHERE id = ?");
        assert_eq!(q.pool, "default");
        assert!(q.readonly);
        assert!(
            !q.idempotent,
            "idempotent defaults to false: the retry licence is opt-in"
        );
    }

    #[test]
    fn the_sql_is_never_rewritten_beyond_trimming() {
        let text = "-- ferro:\n--   id: a\nSELECT  1\n  -- a trailing comment stays\n,   2\n";
        let (_, q) = parse_sql_file("q.sql", text).expect("parses");
        assert_eq!(q.sql, "SELECT  1\n  -- a trailing comment stays\n,   2");
    }

    #[test]
    fn a_boolean_must_be_exactly_true_or_false() {
        // A typo in `idempotent` must never silently license (or forbid) a retry.
        for bad in ["yes", "True", "1", "tru"] {
            let text = format!("-- ferro:\n--   id: a\n--   idempotent: {bad}\nSELECT 1\n");
            let err = parse_sql_file("q.sql", &text).expect_err(bad);
            assert!(
                err.iter()
                    .any(|p| p.message.contains("exactly `true` or `false`")),
                "{bad}: {err:?}"
            );
        }
    }

    #[test]
    fn unknown_and_repeated_keys_and_missing_ids_are_refused() {
        let unknown = parse_sql_file(
            "q.sql",
            "-- ferro:\n--   id: a\n--   idempotnet: true\nSELECT 1\n",
        )
        .expect_err("typo");
        assert!(
            unknown
                .iter()
                .any(|p| p.message.contains("unknown front-matter key `idempotnet`"))
        );
        let repeated = parse_sql_file("q.sql", "-- ferro:\n--   id: a\n--   id: b\nSELECT 1\n")
            .expect_err("repeat");
        assert!(repeated.iter().any(|p| p.message.contains("repeated")));
        let no_id =
            parse_sql_file("q.sql", "-- ferro:\n--   pool: x\nSELECT 1\n").expect_err("no id");
        assert!(no_id.iter().any(|p| p.message.contains("no `id`")));
        let no_block = parse_sql_file("q.sql", "SELECT 1\n").expect_err("no block");
        assert!(no_block[0].message.contains("missing front-matter"));
        let no_sql = parse_sql_file("q.sql", "-- ferro:\n--   id: a\n").expect_err("no sql");
        assert!(no_sql.iter().any(|p| p.message.contains("no SQL")));
    }

    #[test]
    fn ids_and_pools_are_checked() {
        assert!(check_id("users.find_by-email2").is_ok());
        for bad in ["", "Users", "_x", "a b", "a/b", &"a".repeat(MAX_ID_LEN + 1)] {
            assert!(check_id(bad).is_err(), "{bad:?}");
        }
        assert!(check_pool("default").is_ok());
        assert!(check_pool("").is_err());
        assert!(check_pool("a b").is_err());
    }

    #[test]
    fn a_duplicate_id_is_refused_naming_both_sources() {
        let mut m = Manifest::new();
        let (id, q) = parse_sql_file("a.sql", FILE).unwrap();
        m.insert(id, q).unwrap();
        let (id, q) = parse_sql_file("b.sql", FILE).unwrap();
        let p = m.insert(id, q).expect_err("duplicate");
        assert!(
            p.message.contains("duplicate query id `users.find`") && p.message.contains("a.sql"),
            "{p}"
        );
    }

    #[test]
    fn the_hash_covers_what_runs_and_nothing_else() {
        let mut a = Manifest::new();
        let (id, q) = parse_sql_file("a.sql", FILE).unwrap();
        a.insert(id, q).unwrap();
        let base = a.hash();
        assert_eq!(base.len(), 64);

        // Source and DTO do not change what runs, so they do not change the hash.
        let mut moved = a.clone();
        let q = moved.queries.get_mut("users.find").unwrap();
        q.source = Some("elsewhere.sql".into());
        q.dto = Some("App\\Dto\\User".into());
        assert_eq!(moved.hash(), base);

        // Each field that decides execution does.
        for change in 0..4 {
            let mut c = a.clone();
            let q = c.queries.get_mut("users.find").unwrap();
            match change {
                0 => q.sql.push_str(" LIMIT 1"),
                1 => q.pool = "other".into(),
                2 => q.readonly = !q.readonly,
                _ => q.idempotent = !q.idempotent,
            }
            assert_ne!(c.hash(), base, "change {change} must move the hash");
        }
    }

    #[test]
    fn the_hash_is_independent_of_insertion_order() {
        let mut a = Manifest::new();
        let mut b = Manifest::new();
        let q = |sql: &str| Query {
            sql: sql.into(),
            pool: "default".into(),
            readonly: false,
            idempotent: false,
            dto: None,
            source: None,
        };
        a.insert("x".into(), q("SELECT 1")).unwrap();
        a.insert("y".into(), q("SELECT 2")).unwrap();
        b.insert("y".into(), q("SELECT 2")).unwrap();
        b.insert("x".into(), q("SELECT 1")).unwrap();
        assert_eq!(a.hash(), b.hash());
    }

    #[test]
    fn the_canonical_bytes_are_pinned() {
        // The exact bytes the hash covers. If this changes, every deployed client/engine pair's
        // manifest hash changes with it, so it is pinned here rather than discovered in production.
        let mut m = Manifest::new();
        m.insert(
            "a".into(),
            Query {
                sql: "SELECT 1".into(),
                pool: "default".into(),
                readonly: true,
                idempotent: false,
                dto: None,
                source: None,
            },
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(m.canonical_bytes()).unwrap(),
            r#"{"v":1,"q":{"a":{"sql":"SELECT 1","pool":"default","readonly":true,"idempotent":false}}}"#
        );
    }

    #[test]
    fn a_loaded_manifest_is_validated_and_its_recorded_hash_is_not_trusted() {
        let mut m = Manifest::new();
        let (id, q) = parse_sql_file("a.sql", FILE).unwrap();
        m.insert(id, q).unwrap();
        let mut json: serde_json::Value = serde_json::from_str(&m.to_json_with_hash()).unwrap();
        json["hash"] = serde_json::Value::String("0".repeat(64));
        let loaded = Manifest::from_json(json.to_string().as_bytes())
            .expect("loads; the recorded hash is ignored");
        assert_eq!(loaded.hash(), m.hash());

        let mut bad = m.clone();
        bad.version = 2;
        let err = Manifest::from_json(serde_json::to_string(&bad).unwrap().as_bytes())
            .expect_err("version");
        assert!(err[0].message.contains("unsupported manifest version 2"));
    }
}
