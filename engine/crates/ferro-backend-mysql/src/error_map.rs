//! `mysql_async::Error` → `PoolError`, session-fatal-FIRST (mirrors `ferro-backend-pg`'s
//! `error_map`).
//!
//! **MINIMAL for M1-S6 Task 3.** Task 3 only needs `simple_query`/`reset`/`ping` to classify their
//! failures without swallowing the SQLSTATE, and — critically — so that a serialization
//! failure/deadlock surfacing AT COMMIT (routed here via `commit`/`rollback`/`tx_control`, exactly
//! like PG's `simple_query`) survives as `PoolError::Sql{ branch: RETRYABLE }`. The FULL MySQL
//! SQLSTATE → proto-code table lands in Task 4; this module is structured so Task 4 slots it into
//! [`classify_sqlstate`] without touching the ordering.
//!
//! The classification order is load-bearing, identical to the PG rationale: branch on the
//! connection's FATE first (`Error::is_fatal()`), so a transport/driver failure becomes the DISTINCT
//! [`PoolError::ConnectionLost`] variant (fate unknown → the §19.3 `Indeterminate` override is
//! eligible), never a `Sql` error. Only a `Server` error (the server answered and rejected the
//! statement — fate KNOWN) becomes `PoolError::Sql`, preserving the raw SQLSTATE + message verbatim.
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
/// `is_fatal()`-first ordering.
pub(crate) fn map(e: &Error) -> PoolError {
    // (1) transport/driver failure → the distinct, Indeterminate-eligible ConnectionLost variant.
    if is_session_fatal(e) {
        return PoolError::ConnectionLost;
    }
    // (2) a server error — is_fatal() is false ONLY for `Error::Server`, so this always matches; the
    // fallback is a safety net that never fires.
    match e {
        Error::Server(se) => {
            let sqlstate = se.state.clone();
            let (code, branch) = classify_sqlstate(&sqlstate);
            PoolError::Sql {
                code,
                branch,
                sqlstate: Some(sqlstate),
                message: se.message.clone(),
            }
        }
        _ => PoolError::ConnectionLost,
    }
}

/// MINIMAL SQLSTATE → (proto code, proto branch) table for Task 3 — Task 4 expands it into the full
/// MySQL table. The one class that MUST be right NOW is `40001` (MySQL deadlock err 1213 /
/// serialization) so a conflict caught at COMMIT classifies `Retryable`, matching PG's behavior.
/// Everything else is a generic NonRetryable — but the raw SQLSTATE is ALWAYS preserved on the wire
/// (in `PoolError::Sql.sqlstate`), so no classification is lost pending Task 4.
fn classify_sqlstate(code: &str) -> (u16, u8) {
    match code {
        "40001" => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),
        _ => (errc::PROTOCOL, errc::PROTOCOL_BRANCH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::branch;
    use mysql_async::ServerError;

    #[test]
    fn server_error_preserves_sqlstate_and_message() {
        let e = Error::Server(ServerError {
            code: 1062,
            state: "23000".to_string(),
            message: "Duplicate entry".to_string(),
        });
        assert!(!is_session_fatal(&e), "a Server error is not session-fatal");
        match map(&e) {
            PoolError::Sql {
                sqlstate, message, ..
            } => {
                assert_eq!(sqlstate.as_deref(), Some("23000"));
                assert_eq!(message, "Duplicate entry");
            }
            other => panic!("expected PoolError::Sql, got {other:?}"),
        }
    }

    #[test]
    fn deadlock_serialization_is_retryable() {
        // MySQL deadlock (err 1213) carries SQLSTATE 40001 — the class that MUST survive as Retryable
        // when caught at COMMIT.
        assert_eq!(
            classify_sqlstate("40001"),
            (errc::SERIALIZATION_FAILURE, branch::RETRYABLE)
        );
    }

    #[test]
    fn unknown_sqlstate_is_nonretryable_but_preserved() {
        let (_, b) = classify_sqlstate("23000");
        assert_eq!(b, branch::NON_RETRYABLE);
    }
}
