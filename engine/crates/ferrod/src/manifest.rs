//! The checked-SQL manifest, as the engine holds it (SPEC §11, M3-D2d).
//!
//! `FERRO_MANIFEST=<path>` names a `manifest.json` written by `ferro manifest` (M3-D2a). It is
//! loaded ONCE, at startup, through `ferro_manifest::Manifest::from_json` — the same parser and the
//! same validation the CLI used — and the hash is RECOMPUTED here from the queries, never read from
//! the file. A manifest that does not load, or that names a pool this daemon is not configured
//! with, stops the daemon from starting: a manifest is a deploy artefact, and a deploy that cannot
//! run its declared queries must fail at start, not on the first request for one of them.
//!
//! What the engine does with it:
//! - **HELLO:** a client that sends a `manifest_hash` claims to have been built against a manifest;
//!   if this engine has none, or a different one, the handshake is refused (session-fatal
//!   `Unsupported`, like a type-registry mismatch). A stale client therefore fails at connect, not
//!   by running a query id whose SQL or declarations moved under it — the client's `idempotent`
//!   retry licence (§9.2, D2e) is only sound if both ends read the same declaration.
//! - **HELLO_ACK** advertises `feature_engine::MANIFEST` when one is loaded.
//! - **EXEC with `query_id`** runs the manifest's SQL, exactly as written (charter rule 6), through
//!   the same paths as inline SQL. See [`resolve`].

use std::path::Path;
use std::sync::Arc;

use ferro_manifest::{Manifest, Query};

use crate::config::PoolSpec;

/// A manifest loaded and checked against this daemon's pools, with its hash computed once.
#[derive(Debug)]
pub struct LoadedManifest {
    manifest: Manifest,
    hash: String,
}

impl LoadedManifest {
    /// Read and validate `path`, and check that every query's pool is one of `pools`. The error is
    /// one human-readable line per problem, for the startup log.
    pub fn load(path: &Path, pools: &[PoolSpec]) -> Result<Arc<Self>, String> {
        let bytes = std::fs::read(path)
            .map_err(|e| format!("FERRO_MANIFEST {}: cannot read: {e}", path.display()))?;
        let manifest = Manifest::from_json(&bytes).map_err(|problems| {
            let lines: Vec<String> = problems.iter().map(|p| p.to_string()).collect();
            format!(
                "FERRO_MANIFEST {}: invalid manifest:\n  {}",
                path.display(),
                lines.join("\n  ")
            )
        })?;
        Self::from_manifest(manifest, pools)
    }

    /// [`LoadedManifest::load`] without the file: for a manifest already parsed (tests).
    pub fn from_manifest(manifest: Manifest, pools: &[PoolSpec]) -> Result<Arc<Self>, String> {
        let problems = manifest.validate();
        if !problems.is_empty() {
            let lines: Vec<String> = problems.iter().map(|p| p.to_string()).collect();
            return Err(format!("invalid manifest:\n  {}", lines.join("\n  ")));
        }
        let unknown: Vec<String> = manifest
            .queries
            .iter()
            .filter(|(_, q)| !pools.iter().any(|p| p.name == q.pool))
            .map(|(id, q)| {
                format!(
                    "query `{id}` runs on pool `{}`, which is not configured",
                    q.pool
                )
            })
            .collect();
        if !unknown.is_empty() {
            return Err(format!(
                "the manifest names pools this daemon does not have (FERRO_POOLS):\n  {}",
                unknown.join("\n  ")
            ));
        }
        let hash = manifest.hash();
        Ok(Arc::new(LoadedManifest { manifest, hash }))
    }

    /// The manifest hash, as `ferro manifest` printed it and the client sends it at HELLO.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// How many queries it declares.
    pub fn len(&self) -> usize {
        self.manifest.queries.len()
    }

    /// Whether it declares none (never true for a loaded manifest: validation refuses one).
    pub fn is_empty(&self) -> bool {
        self.manifest.queries.is_empty()
    }

    /// The declared query for `id`, if the manifest has one.
    pub fn get(&self, id: &str) -> Option<&Query> {
        self.manifest.queries.get(id)
    }
}

/// Why an EXEC's statement could not be resolved. Each is a per-request error; the session lives.
#[derive(Debug, PartialEq, Eq)]
pub enum ResolveError {
    /// The request is malformed: `sql` and `query_id` are both set (PROTOCOL.md §6: `sql` is nil
    /// iff a `query_id` is used). Maps to `Protocol`.
    BothSqlAndQueryId,
    /// Neither is set. Maps to `Unsupported` (the pre-M3 behaviour, kept).
    NoStatement,
    /// Everything else: the request asks for something this engine will not run. Maps to
    /// `Unsupported` (NonRetryable — resending the same request cannot succeed).
    Refused(String),
}

/// The SQL an EXEC runs.
///
/// For inline SQL this is the request's own text. For a `query_id` it is the manifest's SQL, and
/// the request must AGREE with the manifest on the two things the engine acts on, rather than
/// either side silently winning:
/// - **`pool`** — where it runs. `run_pool` is the pool the statement would actually run on: the
///   request's `pool` for autocommit, the transaction's pinned pool for a tx-scoped request (whose
///   `pool` field is otherwise ignored). A query declared for `reports` must never run on
///   `default`, which may be a different database.
/// - **`readonly`** — the §19.3 fate declaration. The engine trusts it (`fate.rs` suppresses
///   `Indeterminate`, a SQLite checkout enforces it); a request claiming `readonly` for a query the
///   manifest declares a write would turn an unknown write fate into a "safe to retry".
///
/// A mismatch means the client is reading a different manifest (or none), so it is refused, loudly.
///
/// **And the session must have AGREED on the manifest at HELLO** (`manifest_agreed`: it sent a
/// `manifest_hash` equal to the engine's). `readonly` and the pool travel with every request and are
/// checked above, but the third declaration — `idempotent`, the client's licence to re-send a lost
/// write (§9.2) — never does: only the hash proves the client read the engine's value of it. A
/// session that sent no hash could otherwise run an id under its own, different, `idempotent` and
/// re-send a write the engine's manifest says must not be (M3-D2d review F8).
pub fn resolve<'a>(
    sql: Option<&'a str>,
    query_id: Option<&str>,
    readonly: bool,
    run_pool: &str,
    manifest: Option<&'a LoadedManifest>,
    manifest_agreed: bool,
) -> Result<&'a str, ResolveError> {
    match (sql, query_id) {
        (Some(_), Some(_)) => Err(ResolveError::BothSqlAndQueryId),
        (None, None) => Err(ResolveError::NoStatement),
        (Some(sql), None) => Ok(sql),
        (None, Some(id)) => {
            let Some(manifest) = manifest else {
                return Err(ResolveError::Refused(
                    "query_id needs a manifest, and this engine has none loaded (FERRO_MANIFEST)"
                        .into(),
                ));
            };
            if !manifest_agreed {
                return Err(ResolveError::Refused(
                    "query_id needs a session whose HELLO carried this engine's manifest_hash \
                     (connect with the manifest loaded)"
                        .into(),
                ));
            }
            // The id is client-supplied: it is echoed only once it is known to be a well-formed
            // id, so an arbitrary string never lands in an error message (or a log line).
            if ferro_manifest::check_id(id).is_err() {
                return Err(ResolveError::Refused(
                    "query_id is not a valid query id".into(),
                ));
            }
            let Some(q) = manifest.get(id) else {
                return Err(ResolveError::Refused(format!(
                    "unknown query_id `{id}` (not in this engine's manifest)"
                )));
            };
            if q.pool != run_pool {
                return Err(ResolveError::Refused(format!(
                    "query `{id}` is declared for pool `{}`, but this request runs on pool `{run_pool}`",
                    q.pool
                )));
            }
            if q.readonly != readonly {
                return Err(ResolveError::Refused(format!(
                    "query `{id}` is declared readonly={}, but the request says readonly={readonly}",
                    q.readonly
                )));
            }
            Ok(q.sql.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pools(names: &[&str]) -> Vec<PoolSpec> {
        names
            .iter()
            .map(|n| PoolSpec {
                name: (*n).to_string(),
                dsn: String::new(),
                kind: crate::config::PoolKind::Postgres,
                pin_functions: Vec::new(),
                pin_on_unknown: true,
                allow_dir: None,
            })
            .collect()
    }

    fn manifest() -> Manifest {
        let mut m = Manifest::new();
        m.insert(
            "users.find".into(),
            Query {
                sql: "SELECT 1".into(),
                pool: "default".into(),
                readonly: true,
                idempotent: false,
                dto: None,
                source: None,
                params: None,
                columns: None,
            },
        )
        .unwrap();
        m.insert(
            "users.touch".into(),
            Query {
                sql: "UPDATE u SET t = now()".into(),
                pool: "reports".into(),
                readonly: false,
                idempotent: true,
                dto: None,
                source: None,
                params: None,
                columns: None,
            },
        )
        .unwrap();
        m
    }

    #[test]
    fn a_manifest_naming_an_unconfigured_pool_does_not_load() {
        let err = LoadedManifest::from_manifest(manifest(), &pools(&["default"])).unwrap_err();
        assert!(
            err.contains("`users.touch` runs on pool `reports`"),
            "{err}"
        );
        let ok =
            LoadedManifest::from_manifest(manifest(), &pools(&["default", "reports"])).unwrap();
        assert_eq!(ok.hash(), manifest().hash());
        assert_eq!(ok.len(), 2);
    }

    #[test]
    fn a_missing_or_invalid_file_does_not_load() {
        let dir = std::env::temp_dir().join(format!("ferrod-manifest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = LoadedManifest::load(&dir.join("nope.json"), &pools(&["default"]));
        assert!(missing.unwrap_err().contains("cannot read"));
        let bad = dir.join("bad.json");
        std::fs::write(&bad, r#"{"version":1,"queries":{}}"#).unwrap();
        let err = LoadedManifest::load(&bad, &pools(&["default"])).unwrap_err();
        assert!(err.contains("declares no queries"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn inline_sql_resolves_to_itself_and_both_or_neither_are_refused() {
        assert_eq!(
            resolve(Some("SELECT 2"), None, false, "default", None, true),
            Ok("SELECT 2")
        );
        assert_eq!(
            resolve(
                Some("SELECT 2"),
                Some("users.find"),
                false,
                "default",
                None,
                true
            ),
            Err(ResolveError::BothSqlAndQueryId)
        );
        assert_eq!(
            resolve(None, None, false, "default", None, true),
            Err(ResolveError::NoStatement)
        );
    }

    #[test]
    fn a_query_id_runs_the_manifest_sql_only_where_and_as_declared() {
        let m = LoadedManifest::from_manifest(manifest(), &pools(&["default", "reports"])).unwrap();
        let m = Some(&*m);
        assert_eq!(
            resolve(None, Some("users.find"), true, "default", m, true),
            Ok("SELECT 1")
        );
        assert_eq!(
            resolve(None, Some("users.touch"), false, "reports", m, true),
            Ok("UPDATE u SET t = now()")
        );

        let refused = |r: Result<&str, ResolveError>| match r {
            Err(ResolveError::Refused(m)) => m,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert!(
            refused(resolve(
                None,
                Some("users.touch"),
                false,
                "default",
                m,
                true
            ))
            .contains("declared for pool `reports`, but this request runs on pool `default`")
        );
        // A write claimed readonly would make an unknown fate look retryable.
        assert!(
            refused(resolve(None, Some("users.touch"), true, "reports", m, true))
                .contains("declared readonly=false")
        );
        assert!(
            refused(resolve(None, Some("users.find"), false, "default", m, true))
                .contains("declared readonly=true")
        );
        assert!(
            refused(resolve(None, Some("nope"), false, "default", m, true))
                .contains("unknown query_id `nope`")
        );
        assert!(
            refused(resolve(
                None,
                Some("users.find"),
                true,
                "default",
                None,
                true
            ))
            .contains("has none loaded")
        );
    }

    #[test]
    fn a_malformed_query_id_is_not_echoed() {
        let m = LoadedManifest::from_manifest(manifest(), &pools(&["default", "reports"])).unwrap();
        let junk = "x'; DROP TABLE users; --\u{7}";
        match resolve(None, Some(junk), false, "default", Some(&m), true) {
            Err(ResolveError::Refused(msg)) => assert!(!msg.contains("DROP"), "{msg}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_session_that_did_not_agree_on_the_manifest_cannot_run_a_query_id() {
        // F8: `idempotent` never travels per request, so only the HELLO hash proves the client read
        // the engine's declaration of it.
        let m = LoadedManifest::from_manifest(manifest(), &pools(&["default", "reports"])).unwrap();
        match resolve(None, Some("users.find"), true, "default", Some(&m), false) {
            Err(ResolveError::Refused(msg)) => assert!(msg.contains("manifest_hash"), "{msg}"),
            other => panic!("{other:?}"),
        }
        // Inline SQL is unaffected.
        assert_eq!(
            resolve(Some("SELECT 2"), None, false, "default", Some(&m), false),
            Ok("SELECT 2")
        );
    }
}
