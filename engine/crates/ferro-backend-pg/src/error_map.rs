//! `tokio_postgres::Error` → `PoolError`, **branching on `as_db_error()` FIRST** (MAJOR-9).
//!
//! The classification order is load-bearing. A SQLSTATE-first map would misclassify a transport
//! failure (no SQLSTATE) or a server-initiated FATAL/PANIC session end as a plain statement error.
//! So we branch on the connection's fate FIRST, reusing `conn.rs::is_session_fatal`:
//!
//! 1. `as_db_error() == None` (transport failure) OR severity FATAL/PANIC → [`PoolError::ConnectionLost`]
//!    — a DISTINCT variant so the SQL service (S5 Task 3) can apply the §19.3 `readonly`→
//!    `Indeterminate` override ONLY to true connection loss (fate unknown), never to a SQL error.
//! 2. A present, non-fatal `DbError` → the v0 SQLSTATE table → a proto `code`+`branch`, PRESERVING
//!    the raw SQLSTATE + server message in [`PoolError::Sql`] so the service can build the wire
//!    `ErrorPayload` verbatim.
//!
//! Nothing in this module (or anywhere on the query path) re-runs the user statement — the engine
//! never transparently retries (charter rule 3). The retryable classes here only inform the
//! *caller's* policy.

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;

use crate::conn::is_session_fatal;

/// Maps a query-path `tokio_postgres::Error` to a `PoolError`. See the module docs for the
/// `as_db_error()`-first ordering.
pub fn map(e: &tokio_postgres::Error) -> PoolError {
    // (1) transport failure or FATAL/PANIC → connection lost (distinct, detectable variant).
    if is_session_fatal(e) {
        return PoolError::ConnectionLost;
    }
    // (2) a present, non-fatal DbError → SQLSTATE table. `is_session_fatal` already established
    // `as_db_error()` is `Some` and non-fatal here.
    let db = e
        .as_db_error()
        .expect("is_session_fatal(None) is true, so a non-fatal error has a DbError");
    let sqlstate = db.code().code().to_string();
    let (code, branch) = classify_sqlstate(&sqlstate);
    PoolError::Sql {
        code,
        branch,
        sqlstate: Some(sqlstate),
        // PostgreSQL has NO integer error code — its error identity IS the five-character SQLSTATE
        // (`DbError` exposes no numeric vendor code), so this is `None` on PG by construction, not
        // by omission. Proven against a real server by `pg_query_it`'s
        // `a_real_pg_server_error_carries_no_errno`.
        errno: None,
        message: db.message().to_string(),
    }
}

/// The v0 SQLSTATE → (proto code, proto branch) table. Explicit codes for the classes the plan
/// calls out; a `42xxx`/`08xxx` class prefix match; everything else is a generic NonRetryable
/// (the raw SQLSTATE is preserved on the wire regardless, so no classification is lost).
fn classify_sqlstate(code: &str) -> (u16, u8) {
    match code {
        "23505" => (errc::UNIQUE, errc::UNIQUE_BRANCH),
        "40001" => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),
        "40P01" => (errc::DEADLOCK, errc::DEADLOCK_BRANCH),
        "57014" => (errc::CANCELLED, errc::CANCELLED_BRANCH),
        // Class 42 — syntax error or access rule violation.
        c if c.starts_with("42") => (errc::SYNTAX, errc::SYNTAX_BRANCH),
        // Class 08 — connection exception (surfaced here as a non-fatal DbError; the fate is still
        // KNOWN — the server answered — so this is NOT the Indeterminate-eligible ConnectionLost
        // variant, only the ConnectionLost proto *code*).
        c if c.starts_with("08") => (errc::CONNECTION_LOST, errc::CONNECTION_LOST_BRANCH),
        // Anything else: generic NonRetryable. `PROTOCOL` is the same NonRetryable catch-all
        // `PoolError::errc()` already uses for `Backend`.
        _ => (errc::PROTOCOL, errc::PROTOCOL_BRANCH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::branch;

    #[test]
    fn sqlstate_table_maps_the_called_out_classes() {
        assert_eq!(
            classify_sqlstate("23505"),
            (errc::UNIQUE, errc::UNIQUE_BRANCH)
        );
        assert_eq!(
            classify_sqlstate("40001"),
            (errc::SERIALIZATION_FAILURE, branch::RETRYABLE)
        );
        assert_eq!(
            classify_sqlstate("40P01"),
            (errc::DEADLOCK, branch::RETRYABLE)
        );
        assert_eq!(
            classify_sqlstate("57014"),
            (errc::CANCELLED, branch::NON_RETRYABLE)
        );
        // 42xxx family → Syntax.
        assert_eq!(
            classify_sqlstate("42601"),
            (errc::SYNTAX, branch::NON_RETRYABLE)
        );
        assert_eq!(
            classify_sqlstate("42P01"),
            (errc::SYNTAX, branch::NON_RETRYABLE)
        );
        // 08xxx family → ConnectionLost code (still a known-fate SQL error, not the distinct variant).
        assert_eq!(
            classify_sqlstate("08006"),
            (errc::CONNECTION_LOST, branch::RETRYABLE)
        );
        // Else → generic NonRetryable.
        assert_eq!(
            classify_sqlstate("22012"),
            (errc::PROTOCOL, branch::NON_RETRYABLE)
        );
    }

    /// A transport failure (refused TCP connect) yields a `tokio_postgres::Error` with
    /// `as_db_error() == None` — the `as_db_error()`-FIRST branch (MAJOR-9) MUST classify it as the
    /// DISTINCT `ConnectionLost` variant (NOT a `Sql` error), so the SQL service can later apply the
    /// §19.3 `readonly`→`Indeterminate` override to it. No Docker needed — the connect just fails.
    /// (The FATAL/PANIC-severity branch shares the same `is_session_fatal` helper, exercised live by
    /// S4's `pg_killed_backend_evicted_no_retry`; a FATAL `DbError` cannot be synthesized offline.)
    #[tokio::test]
    async fn transport_failure_with_no_db_error_is_connection_lost() {
        // `(Client, Connection)` is not `Debug`, so match rather than `expect_err`.
        let err = match tokio_postgres::connect(
            "host=127.0.0.1 port=1 user=nobody dbname=nobody connect_timeout=2",
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok(_) => panic!("connecting to a refused port must fail"),
            Err(e) => e,
        };
        assert!(
            err.as_db_error().is_none(),
            "a transport failure carries no DbError"
        );
        assert_eq!(
            map(&err),
            PoolError::ConnectionLost,
            "a no-SQLSTATE transport failure must be the distinct ConnectionLost variant"
        );
    }
}
