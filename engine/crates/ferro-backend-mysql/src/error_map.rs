//! `mysql_async::Error` → `PoolError`, session-fatal-FIRST then **errno-keyed** (mirrors
//! `ferro-backend-pg`'s `error_map`, adapted for MySQL's error model — M1-S6 Task 4).
//!
//! The classification order is load-bearing, identical to the PG rationale: branch on the
//! connection's FATE first (`Error::is_fatal()`), so a transport/driver failure becomes the DISTINCT
//! [`PoolError::ConnectionLost`] variant (fate unknown → the §19.3 `Indeterminate` override is
//! eligible), never a `Sql` error. Only a `Server` error (the server answered and rejected the
//! statement — fate KNOWN) becomes `PoolError::Sql`, preserving the raw SQLSTATE + message verbatim.
//!
//! ## Why key on the ERRNO, not the SQLSTATE (the MySQL-specific twist)
//!
//! MySQL's five-character SQLSTATE is far coarser than its `errno`: the whole retryable-contention
//! class does NOT share one SQLSTATE. A **deadlock** (`1213`) carries SQLSTATE `40001`, but a
//! **lock-wait timeout** (`1205`) carries the catch-all `HY000` — so a "class-40 ⇒ retryable"
//! heuristic (which works for PG) would silently MISS `1205` and mis-report a retryable timeout as
//! NonRetryable. The three statement-cancel/timeout errnos are likewise scattered
//! (`1317`/`3024`/`1969`, see below). So the primary key here is the `errno`; the SQLSTATE class is
//! only a secondary fallback (and the raw SQLSTATE is ALWAYS preserved on the wire regardless).
//!
//! `classify_fate` (the S4 matrix, UNCHANGED) consumes the `(code, branch)` this module sets: it
//! passes a `Sql` error's `code`+`branch` through VERBATIM (it does NOT re-derive `Retryable` from
//! the SQLSTATE), and its existing `is_57014` override fires on `code == errc::CANCELLED` — which is
//! exactly why the cancel/timeout errnos below are mapped to `errc::CANCELLED` (no `fate.rs` edit).
//!
//! Note on `errno` reaching the wire (M1-S8a, closing the S6 deferral): `PoolError::Sql` now carries
//! an `errno: Option<i32>` slot alongside the proto `code`+`branch` + raw SQLSTATE + message, and
//! THIS is the one site that fills it — `se.code` is both the classification KEY below and the raw
//! value handed to `classify_fate`, which passes it to `ErrorPayload.errno` VERBATIM (no downstream
//! re-derivation). It has to reach the wire because MySQL's SQLSTATEs are far coarser than its
//! errnos: a duplicate key (`1062`) and a NOT NULL violation (`1048`) BOTH arrive as `23000`
//! (measured live on MySQL 8 and MariaDB 11), so a consumer keyed on SQLSTATE alone — e.g. Doctrine
//! DBAL's MySQL `ExceptionConverter`, which matches on the errno EXCLUSIVELY — cannot tell them
//! apart. PostgreSQL has no integer errno and stays `None` there by construction.
//!
//! Nothing here re-runs the statement — the engine never transparently retries (charter rule 3).

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use mysql_async::Error;

/// Whether a `mysql_async::Error` means "this session is over" (connection broken) rather than "this
/// statement was rejected". `Error::is_fatal()` is exactly this: `true` for `Driver`/`Io`/`Other`/
/// `Url` (the transport/driver itself failed — no server answer, fate unknown), `false` for `Server`
/// (a well-formed server error — the connection is fine, the statement's fate is known).
pub(crate) fn is_session_fatal(e: &Error) -> bool {
    e.is_fatal()
}

/// Map a query-path `mysql_async::Error` to a `PoolError`. See the module docs for the
/// `is_fatal()`-first, then errno-keyed, ordering.
pub(crate) fn map(e: &Error) -> PoolError {
    // (1) transport/driver failure → the distinct, Indeterminate-eligible ConnectionLost variant.
    if is_session_fatal(e) {
        return PoolError::ConnectionLost;
    }
    // (2) a server error — is_fatal() is false ONLY for `Error::Server`, so this always matches; the
    // fallback is a safety net that never fires (a non-fatal non-Server error is unreachable).
    match e {
        Error::Server(se) => {
            let sqlstate = se.state.clone();
            let (code, branch) = classify_errno(se.code, &sqlstate);
            PoolError::Sql {
                code,
                branch,
                sqlstate: Some(sqlstate),
                // The RAW vendor errno, carried alongside the classification rather than consumed by
                // it (M1-S8a). `u16` -> `i32` is lossless.
                errno: Some(i32::from(se.code)),
                message: se.message.clone(),
            }
        }
        _ => PoolError::ConnectionLost,
    }
}

/// The MySQL/MariaDB `errno` → (proto code, proto branch) table (primary key), with a SQLSTATE-class
/// fallback for unrecognized errnos. The raw SQLSTATE + message are preserved on the wire regardless,
/// so no classification is ever lost.
fn classify_errno(errno: u16, sqlstate: &str) -> (u16, u8) {
    match errno {
        // ---- Retryable contention (KEYED ON ERRNO — the whole point) --------------------------
        // 1213 ER_LOCK_DEADLOCK (SQLSTATE 40001): an InnoDB deadlock, the victim auto-rolled-back.
        1213 => (errc::DEADLOCK, errc::DEADLOCK_BRANCH),
        // 1205 ER_LOCK_WAIT_TIMEOUT (SQLSTATE **HY000** — a class-40 heuristic MISSES this): a lock
        // wait exceeded `innodb_lock_wait_timeout`. Retryable contention (no dedicated lock-timeout
        // proto code — SerializationFailure is the retryable-contention bucket).
        1205 => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),

        // ---- Statement cancel / timeout → CANCELLED (so the S4 `is_57014` override fires) -------
        // 1317 ER_QUERY_INTERRUPTED (KILL QUERY — the out-of-band cancel this backend fires),
        // 3024 ER_QUERY_TIMEOUT (MySQL `MAX_EXECUTION_TIME` / optimizer hint), and
        // 1969 ER_STATEMENT_TIMEOUT (MariaDB `max_statement_time`) — the MySQL/MariaDB errno
        // DIVERGENCE for the same "statement timed out" event (see the §22 note). All three set
        // `code == errc::CANCELLED`, which the UNCHANGED `fate.rs::is_57014` catches to yield the
        // §19.3 cell (autocommit write → Indeterminate, in-tx → Retryable, read → Cancelled).
        1317 | 3024 | 1969 => (errc::CANCELLED, errc::CANCELLED_BRANCH),

        // ---- Known-fate app / constraint errors (NonRetryable, and NOT the wire-protocol code) --
        // A duplicate key is a plain app error → UNIQUE, never PROTOCOL (the T3 fallback fix).
        1062 | 1586 => (errc::UNIQUE, errc::UNIQUE_BRANCH),
        // FK parent/child row violations.
        1451 | 1452 => (errc::FOREIGN_KEY, errc::FOREIGN_KEY_BRANCH),
        // NULL into a NOT NULL column / field has no default.
        1048 | 1364 => (errc::NOT_NULL, errc::NOT_NULL_BRANCH),
        // CHECK constraint (MySQL 3819 ER_CHECK_CONSTRAINT_VIOLATED / MariaDB 4025).
        3819 | 4025 => (errc::CHECK, errc::CHECK_BRANCH),
        // Parse error → SYNTAX.
        1064 => (errc::SYNTAX, errc::SYNTAX_BRANCH),
        // Access-denied family → AUTH.
        1044 | 1045 | 1142 | 1143 => (errc::AUTH, errc::AUTH_BRANCH),

        // ---- Unrecognized errno: fall back to the SQLSTATE class -------------------------------
        _ => classify_by_sqlstate_class(sqlstate),
    }
}

/// SQLSTATE-class fallback for an errno this table does not recognize. Preserves the existing
/// `40001`→Retryable arm (any serialization errno that isn't 1213), maps the syntax/access class,
/// and otherwise returns a GENERIC non-retryable SQL-error code.
fn classify_by_sqlstate_class(sqlstate: &str) -> (u16, u8) {
    match sqlstate {
        // A serialization failure by SQLSTATE (preserve the existing arm — any 40001 not caught by
        // an explicit errno above still classifies Retryable, matching PG).
        "40001" => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),
        // Class 42 — syntax error or access rule violation.
        c if c.starts_with("42") => (errc::SYNTAX, errc::SYNTAX_BRANCH),
        // A GENERIC, NON-retryable SQL statement error: the server ANSWERED and rejected the
        // statement, so its fate is KNOWN (never Indeterminate). Deliberately NOT `errc::PROTOCOL`
        // (which is reserved for a wire-protocol fault) — conflating a plain app error with a
        // protocol error is exactly the T3 fallback defect this replaces. `SYNTAX` (SQL class 42:
        // "syntax error or access rule violation") is the registry's broadest "the server rejected
        // this statement" NonRetryable bucket; the raw SQLSTATE is preserved on the wire for the
        // precise class.
        _ => (errc::SYNTAX, errc::SYNTAX_BRANCH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::branch;
    use mysql_async::ServerError;

    fn server(code: u16, state: &str, message: &str) -> Error {
        Error::Server(ServerError {
            code,
            state: state.to_string(),
            message: message.to_string(),
        })
    }

    #[test]
    fn server_error_preserves_sqlstate_and_message() {
        let e = server(1062, "23000", "Duplicate entry");
        assert!(!is_session_fatal(&e), "a Server error is not session-fatal");
        match map(&e) {
            PoolError::Sql {
                sqlstate, message, ..
            } => {
                assert_eq!(sqlstate.as_deref(), Some("23000"));
                assert_eq!(message, "Duplicate entry", "message preserved verbatim");
            }
            other => panic!("expected PoolError::Sql, got {other:?}"),
        }
    }

    /// The errno → (code, branch) table. The load-bearing rows: 1213/1205 → Retryable (1205's
    /// SQLSTATE is HY000, which a class-40 heuristic would miss), and the three cancel/timeout
    /// errnos (incl. MySQL 3024 vs MariaDB 1969) → CANCELLED so the S4 `is_57014` override fires.
    #[test]
    fn errno_table_classifies_the_called_out_classes() {
        // Retryable contention.
        assert_eq!(
            classify_errno(1213, "40001"),
            (errc::DEADLOCK, branch::RETRYABLE),
            "1213 deadlock → Retryable"
        );
        assert_eq!(
            classify_errno(1205, "HY000"),
            (errc::SERIALIZATION_FAILURE, branch::RETRYABLE),
            "1205 lock-wait-timeout (HY000!) → Retryable — errno-keyed, not SQLSTATE-class-keyed"
        );

        // Cancel / timeout → CANCELLED (both DBs' errnos), so `is_57014` (code == CANCELLED) fires.
        for (errno, state) in [(1317u16, "70100"), (3024, "HY000"), (1969, "70100")] {
            let (code, _b) = classify_errno(errno, state);
            assert_eq!(
                code,
                errc::CANCELLED,
                "errno {errno} must set code == CANCELLED so fate.rs::is_57014 fires"
            );
        }

        // A duplicate key is a plain app error → UNIQUE (NonRetryable), NEVER PROTOCOL (T3 fix).
        let (dup_code, dup_branch) = classify_errno(1062, "23000");
        assert_eq!(dup_code, errc::UNIQUE);
        assert_eq!(dup_branch, branch::NON_RETRYABLE);
        assert_ne!(
            dup_code,
            errc::PROTOCOL,
            "a duplicate key must NOT be PROTOCOL"
        );
    }

    /// The generic fallback for an unrecognized server error is a NON-retryable SQL error and is
    /// NEVER `PROTOCOL` (the T3 defect) — a plain app error is not a wire-protocol error.
    #[test]
    fn unknown_server_error_is_generic_nonretryable_not_protocol() {
        // Unknown errno + unknown SQLSTATE class → generic NonRetryable, not PROTOCOL.
        let (code, b) = classify_errno(9999, "HY000");
        assert_eq!(b, branch::NON_RETRYABLE);
        assert_ne!(code, errc::PROTOCOL, "the fallback must not be PROTOCOL");

        // A syntax error by SQLSTATE class (unknown errno) still classifies Syntax.
        assert_eq!(
            classify_errno(65000, "42000"),
            (errc::SYNTAX, errc::SYNTAX_BRANCH)
        );
        // The 40001-by-SQLSTATE arm is preserved (any serialization errno not explicitly listed).
        assert_eq!(
            classify_errno(65001, "40001"),
            (errc::SERIALIZATION_FAILURE, branch::RETRYABLE)
        );
    }

    /// The full `map` for a duplicate key: a known-fate `Sql{Unique, NonRetryable}`, SQLSTATE +
    /// message preserved. The pool-level `taxonomy_branch()` (what a caller keys retry off) is
    /// NonRetryable — proving classify_fate would report it NonRetryable (Sql passthrough).
    #[test]
    fn duplicate_key_maps_to_unique_nonretryable() {
        let e = server(1062, "23000", "Duplicate entry '1' for key 'PRIMARY'");
        match map(&e) {
            PoolError::Sql {
                code, branch: b, ..
            } => {
                assert_eq!(code, errc::UNIQUE);
                assert_eq!(b, branch::NON_RETRYABLE);
            }
            other => panic!("expected Sql{{Unique}}, got {other:?}"),
        }
        assert_eq!(
            map(&e).taxonomy_branch(),
            ferro_pool::error::Branch::NonRetryable
        );
    }

    /// A deadlock (1213) maps to a `Sql{branch: RETRYABLE}` — the pool-level `taxonomy_branch()` is
    /// Retryable, and since `classify_fate` passes a `Sql`'s branch through verbatim, this is the
    /// end-to-end proof that a deadlock reaches the §9.2 `Retryable` cell.
    #[test]
    fn deadlock_1213_is_retryable_end_to_end() {
        let e = server(1213, "40001", "Deadlock found when trying to get lock");
        match map(&e) {
            PoolError::Sql { branch: b, .. } => assert_eq!(b, branch::RETRYABLE),
            other => panic!("expected Sql, got {other:?}"),
        }
        assert_eq!(
            map(&e).taxonomy_branch(),
            ferro_pool::error::Branch::Retryable,
            "1213 must coarsen to Retryable (what classify_fate passes through verbatim)"
        );
    }
}
