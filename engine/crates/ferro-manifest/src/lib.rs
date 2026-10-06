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

/// One declared query. Unknown fields are refused when a manifest is loaded: a misspelt
/// `idempotnet: true` must not load as a query that is silently not idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
        if self.queries.is_empty() {
            problems.push(Problem {
                at: "manifest".into(),
                message: "the manifest declares no queries".into(),
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
    /// whole: the engine never runs part of one. Unknown fields and a query id that appears twice
    /// are refused too — JSON parsers keep the LAST of two equal keys without a word, so a second
    /// entry could replace a reviewed one (M3-D2a review F8).
    pub fn from_json(bytes: &[u8]) -> Result<Self, Vec<Problem>> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            version: u32,
            /// Informational (see [`Manifest::to_json_with_hash`]); recomputed, never trusted.
            #[serde(default)]
            #[allow(dead_code)]
            hash: Option<String>,
            queries: UniqueMap,
        }
        let f: File = serde_json::from_slice(bytes).map_err(|e| {
            vec![Problem {
                at: "manifest".into(),
                message: format!("not a valid manifest: {e}"),
            }]
        })?;
        let m = Manifest {
            version: f.version,
            queries: f.queries.0,
        };
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

/// The `queries` object, refusing a key that appears twice.
struct UniqueMap(BTreeMap<String, Query>);

impl<'de> Deserialize<'de> for UniqueMap {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = UniqueMap;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object of query id -> query")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut a: A,
            ) -> Result<UniqueMap, A::Error> {
                let mut m = BTreeMap::new();
                while let Some((k, v)) = a.next_entry::<String, Query>()? {
                    if m.contains_key(&k) {
                        return Err(serde::de::Error::custom(format!(
                            "query id `{k}` appears twice"
                        )));
                    }
                    m.insert(k, v);
                }
                Ok(UniqueMap(m))
            }
        }
        d.deserialize_map(V)
    }
}

/// A query id: 1–128 characters of `a-z 0-9 _ . -`, starting with a LETTER. Ids are stable names
/// used from PHP, so they are kept to a set that needs no escaping anywhere — and never all digits,
/// which PHP would turn into an integer array key (re-ordering `ksort`, and encoding `"0","1",…` as
/// a JSON list) the moment the client recomputes the hash (M3-D2a review F20).
pub fn check_id(id: &str) -> Result<(), String> {
    let ok_first = id.chars().next().is_some_and(|c| c.is_ascii_lowercase());
    let ok_rest = id
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | '-'));
    if id.is_empty() || id.len() > MAX_ID_LEN || !ok_first || !ok_rest {
        return Err(format!(
            "invalid query id `{id}`: use 1-{MAX_ID_LEN} characters of a-z, 0-9, `_`, `.`, `-`, starting with a letter"
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
/// over the following INDENTED comment lines (`--` then at least two spaces or a tab) of
/// `key: value`; the first line that is not one ends it, and everything from there on is the SQL,
/// byte for byte (line endings included). `id` is required; `pool` defaults to `default`,
/// `readonly` and `idempotent` to `false`.
///
/// Everything that could make the file mean something other than it looks is an ERROR, never a
/// guess (M3-D2a review): an unknown or repeated key, a boolean that is not exactly
/// `true`/`false` (a typo in `idempotent` must not silently license or forbid a retry), a
/// front-matter-looking line separated from the block by a blank line (it would otherwise become
/// an SQL comment and the query would run on the default pool), and a second `-- ferro:` line (one
/// file declares ONE query; a second block would be swallowed into the first query's SQL). A file
/// without the block is an error too: every file in a query directory declares a query.
pub fn parse_sql_file(source: &str, text: &str) -> Result<(String, Query), Vec<Problem>> {
    let mut problems = Vec::new();
    // Lines with their byte offsets, so the SQL is sliced out of `text` verbatim.
    let mut offset = 0usize;
    let lines: Vec<(usize, &str)> = text
        .split_inclusive('\n')
        .map(|raw| {
            let at = offset;
            offset += raw.len();
            (at, raw.strip_suffix('\n').unwrap_or(raw))
        })
        .map(|(at, l)| (at, l.strip_suffix('\r').unwrap_or(l)))
        .collect();

    let mut i = 0;
    while i < lines.len() && lines[i].1.trim().is_empty() {
        i += 1;
    }
    if lines.get(i).map(|(_, l)| l.trim_end()) != Some("-- ferro:") {
        return Err(vec![Problem {
            at: source.into(),
            message: "missing front-matter: the file must start with a `-- ferro:` block".into(),
        }]);
    }
    i += 1;

    let mut fields: BTreeMap<String, (usize, String)> = BTreeMap::new();
    while i < lines.len() {
        let line = lines[i].1;
        let Some(rest) = front_matter_line(line) else {
            break;
        };
        let at = format!("{source}:{}", i + 1);
        i += 1;
        let Some((key, value)) = rest.split_once(':') else {
            problems.push(Problem {
                at,
                message: format!("front-matter line is not `key: value`: `{rest}`"),
            });
            continue;
        };
        let (key, value) = (key.trim().to_string(), value.trim().to_string());
        if !is_front_matter_key(&key) {
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
        fields.insert(key, (i, value));
    }

    let body_offset = lines.get(i).map_or(text.len(), |(at, _)| *at);
    let sql = text[body_offset..].trim().to_string();

    // Lines after the block that a reader would take for front-matter.
    for (n, (_, line)) in lines.iter().enumerate().skip(i) {
        let at = format!("{source}:{}", n + 1);
        if line.trim_end() == "-- ferro:" {
            problems.push(Problem {
                at,
                message: "a second `-- ferro:` block: a `.sql` file declares exactly one query"
                    .into(),
            });
        } else if let Some(rest) = front_matter_line(line)
            && rest
                .split_once(':')
                .is_some_and(|(k, _)| is_front_matter_key(k.trim()))
        {
            problems.push(Problem {
                at,
                message: format!(
                    "`{}` looks like front-matter but is not in the block (the block ends at the \
                     first line that is not an indented `--   key: value`, e.g. a blank line)",
                    line.trim()
                ),
            });
        }
    }

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
        Some((_, id)) => {
            if let Err(m) = check_id(id) {
                problems.push(Problem {
                    at: source.into(),
                    message: m,
                });
            }
            id.clone()
        }
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

/// The text of an INDENTED front-matter line (`--` then two spaces or a tab), trimmed; `None` for
/// anything else. The indent is what separates `--   dto: X` from an SQL comment such as
/// `-- dto: computed below` on the first line of the SQL.
fn front_matter_line(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("--")?;
    (rest.starts_with("  ") || rest.starts_with('\t'))
        .then(|| rest.trim())
        .filter(|r| !r.is_empty())
}

fn is_front_matter_key(key: &str) -> bool {
    matches!(key, "id" | "pool" | "readonly" | "idempotent" | "dto")
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
///
/// Symlinked directories are followed, each real directory at most once, so a link cycle neither
/// hangs the walk nor reports one file many times (M3-D2a review F12). The extension is matched
/// case-insensitively, as `bin/ferro-queries` matches `.php`.
pub fn collect_sql_dir(dir: &Path, manifest: &mut Manifest, problems: &mut Vec<Problem>) {
    let mut files = Vec::new();
    let mut seen = std::collections::HashSet::new();
    walk(dir, &mut files, &mut seen, problems);
    files.sort();
    files.dedup();
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

fn walk(
    dir: &Path,
    out: &mut Vec<std::path::PathBuf>,
    seen: &mut std::collections::HashSet<std::path::PathBuf>,
    problems: &mut Vec<Problem>,
) {
    let real = match std::fs::canonicalize(dir) {
        Ok(r) => r,
        Err(e) => {
            problems.push(Problem {
                at: dir.display().to_string(),
                message: format!("cannot read directory: {e}"),
            });
            return;
        }
    };
    if !seen.insert(real) {
        return;
    }
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
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                problems.push(Problem {
                    at: dir.display().to_string(),
                    message: format!("cannot read directory entry: {e}"),
                });
                continue;
            }
        };
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out, seen, problems);
        } else if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("sql"))
        {
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

    // ---- M3-D2a review round -------------------------------------------------------------

    #[test]
    fn the_hash_is_a_known_answer_not_only_its_bytes() {
        // F15: the bytes were pinned but the digest was not, so an uppercase or otherwise
        // re-encoded hex — which the handshake compares as a string — passed. Both digests were
        // computed OUTSIDE this crate (`sha256sum` over the exact bytes).
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
            m.hash(),
            "7c185434dc7b0d35d0fc7c66490341463e7118bdec905fdcd76a3dfb4416c84b"
        );

        // F20: non-ASCII and control characters, so the escaping a client must reproduce to
        // recompute the hash (raw UTF-8, `\u0001` lowercase, `/` unescaped) is pinned too.
        let mut m = Manifest::new();
        m.insert(
            "a.x".into(),
            Query {
                sql: "SELECT 'éé' -- ☃\n\t\u{1} \"q\" \\ /".into(),
                pool: "p".into(),
                readonly: false,
                idempotent: true,
                dto: None,
                source: None,
            },
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(m.canonical_bytes()).unwrap(),
            "{\"v\":1,\"q\":{\"a.x\":{\"sql\":\"SELECT 'éé' -- ☃\\n\\t\\u0001 \\\"q\\\" \\\\ /\",\"pool\":\"p\",\"readonly\":false,\"idempotent\":true}}}"
        );
        assert_eq!(
            m.hash(),
            "87def7dc2e2b8a97d15156587806242864fc30b9b61c4119e709c1293f4fe274"
        );
    }

    #[test]
    fn an_id_starts_with_a_letter() {
        // F20: an all-digit id becomes an integer key in PHP.
        for bad in ["0", "123", "1abc", "9.users"] {
            assert!(check_id(bad).is_err(), "{bad}");
        }
        assert!(check_id("a1").is_ok());
    }

    #[test]
    fn front_matter_after_a_blank_line_is_refused_not_turned_into_a_comment() {
        // F6: the two lines after the blank one used to become SQL comments, and the query ran on
        // the DEFAULT pool, not `reports`.
        let text = "-- ferro:\n--   id: a\n\n--   pool: reports\n--   idempotent: true\nSELECT 1\n";
        let err = parse_sql_file("q.sql", text).expect_err("ambiguous");
        assert!(
            err.iter()
                .any(|p| p.at == "q.sql:4" && p.message.contains("looks like front-matter")),
            "{err:?}"
        );
        assert!(err.iter().any(|p| p.at == "q.sql:5"), "{err:?}");
        // A blank line between the block and the SQL is still fine.
        let ok = "-- ferro:\n--   id: a\n--   pool: reports\n\nSELECT 1\n";
        assert_eq!(parse_sql_file("q.sql", ok).unwrap().1.pool, "reports");
    }

    #[test]
    fn a_second_ferro_block_is_refused() {
        // F16: the second block was swallowed into the first query's SQL — a DELETE inside a
        // query declared readonly — and the second query was silently missing.
        let text = "-- ferro:\n--   id: a\n--   readonly: true\nSELECT 1;\n\n-- ferro:\n--   id: b\nDELETE FROM users;\n";
        let err = parse_sql_file("q.sql", text).expect_err("two blocks");
        assert!(
            err.iter()
                .any(|p| p.at == "q.sql:6" && p.message.contains("second `-- ferro:` block")),
            "{err:?}"
        );
    }

    #[test]
    fn an_unindented_comment_on_the_first_sql_line_is_sql_not_front_matter() {
        // F7: `-- dto: computed below` was consumed as the `dto` key.
        let text = "-- ferro:\n--   id: a\n-- dto: computed below\nSELECT 1\n";
        let (_, q) = parse_sql_file("q.sql", text).expect("parses");
        assert_eq!(q.dto, None);
        assert_eq!(q.sql, "-- dto: computed below\nSELECT 1");
        // A tab indent is front-matter.
        let tab = "-- ferro:\n--\tid: a\n--\tdto: App\\D\nSELECT 1\n";
        assert_eq!(
            parse_sql_file("q.sql", tab).unwrap().1.dto.as_deref(),
            Some("App\\D")
        );
    }

    #[test]
    fn crlf_line_endings_inside_the_sql_are_kept() {
        // F9: the body was re-joined with `\n`, changing a multi-line string literal.
        let text = "-- ferro:\r\n--   id: a\r\nSELECT 'x\r\ny'\r\n";
        let (id, q) = parse_sql_file("q.sql", text).expect("parses");
        assert_eq!(id, "a");
        assert_eq!(q.sql, "SELECT 'x\r\ny'");
    }

    #[test]
    fn an_empty_id_is_refused_by_the_parser_itself() {
        // F10: only the CLI's later `validate` caught it.
        let err = parse_sql_file("q.sql", "-- ferro:\n--   id:\nSELECT 1\n").expect_err("empty");
        assert!(
            err.iter().any(|p| p.message.contains("invalid query id")),
            "{err:?}"
        );
    }

    #[test]
    fn a_loaded_manifest_refuses_duplicate_ids_unknown_fields_and_emptiness() {
        // F8: JSON keeps the LAST of two equal keys, so a second entry silently replaced the
        // first; a misspelt field was silently ignored; an empty manifest loaded.
        let q = r#"{"sql":"SELECT 1","pool":"default","readonly":true,"idempotent":false}"#;
        let dup = format!(
            r#"{{"version":1,"queries":{{"a":{q},"a":{{"sql":"DELETE FROM u","pool":"default","readonly":false,"idempotent":true}}}}}}"#
        );
        let err = Manifest::from_json(dup.as_bytes()).expect_err("dup");
        assert!(err[0].message.contains("`a` appears twice"), "{err:?}");

        let typo = r#"{"version":1,"queries":{"a":{"sql":"SELECT 1","pool":"default","readonly":true,"idempotent":false,"idempotnet":true}}}"#;
        let err = Manifest::from_json(typo.as_bytes()).expect_err("typo");
        assert!(err[0].message.contains("idempotnet"), "{err:?}");

        let top = format!(r#"{{"version":1,"hsh":"x","queries":{{"a":{q}}}}}"#);
        assert!(Manifest::from_json(top.as_bytes()).is_err());

        let empty = r#"{"version":1,"queries":{}}"#;
        let err = Manifest::from_json(empty.as_bytes()).expect_err("empty");
        assert!(err[0].message.contains("no queries"), "{err:?}");

        let ok = format!(r#"{{"version":1,"hash":"anything","queries":{{"a":{q}}}}}"#);
        assert!(Manifest::from_json(ok.as_bytes()).is_ok());
    }

    #[test]
    fn validate_refuses_untrimmed_sql_and_into_query_trims_unicode_whitespace() {
        // F15: both survived mutation.
        let mut m = Manifest::new();
        m.insert(
            "a".into(),
            Query {
                sql: " SELECT 1".into(),
                pool: "default".into(),
                readonly: false,
                idempotent: false,
                dto: None,
                source: None,
            },
        )
        .unwrap();
        assert!(m.validate().iter().any(|p| p.message.contains("trimmed")));

        let (_, q) = ExtractedQuery {
            id: "a".into(),
            sql: "\u{a0}\u{2003}SELECT 1\n\u{3000}".into(),
            pool: "default".into(),
            readonly: false,
            idempotent: false,
            dto: None,
            source: "x.php:1".into(),
        }
        .into_query();
        assert_eq!(q.sql, "SELECT 1");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_cycle_is_walked_once() {
        // F12: two self-loops made the walk exponential (it hung); one produced 40 bogus
        // duplicate-id errors.
        let dir = std::env::temp_dir().join(format!("ferro-manifest-cycle-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.SQL"), "-- ferro:\n--   id: a\nSELECT 1\n").unwrap();
        std::os::unix::fs::symlink(".", dir.join("l1")).unwrap();
        std::os::unix::fs::symlink(".", dir.join("l2")).unwrap();
        let mut m = Manifest::new();
        let mut problems = Vec::new();
        collect_sql_dir(&dir, &mut m, &mut problems);
        assert!(problems.is_empty(), "{problems:?}");
        assert_eq!(
            m.queries.len(),
            1,
            "the upper-case `.SQL` file is found, once"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
