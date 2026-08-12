use thiserror::Error;

/// Pool-level errors. These are a Rust enum, not wire types — `errc()` maps each variant to the
/// `/proto` error-code registry (SPEC §21 D9 charter rule: no hand-written protocol numbers) so
/// callers that DO produce a wire error use the registry constant, not a magic number.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PoolError {
    /// The caller waited longer than `checkout_timeout` for a permit/idle connection.
    #[error("timed out waiting for a pooled connection")]
    Timeout,
    /// The backend connection died (transport failure, FATAL/PANIC session end, driver task gone).
    ///
    /// `dispatched` records whether the USER STATEMENT this error is being reported for had
    /// already been TRANSMITTED when the loss surfaced (M1-S9a, finding 3):
    ///
    /// - `true` — the DEFAULT direction, and the one every site that cannot attribute the phase
    ///   MUST take: the statement may be executing, so on a sent, non-readonly, non-in-tx call
    ///   site §19.3 reports `Indeterminate`.
    /// - `false` — PROVABLE pre-dispatch ONLY: a connect failure (nothing was ever sent on this
    ///   socket), or a PREPARE-phase loss where Parse/Describe (PG) / `COM_STMT_PREPARE` (MySQL)
    ///   failed and the Execute never left the process. A known did-not-apply — always
    ///   `Retryable`, never `Indeterminate`.
    ///
    /// The asymmetry is deliberate and load-bearing: a wrong `true` cries wolf (a write that did
    /// not apply is reported unconfirmed); a wrong `false` LICENSES REPLAY of a possibly-applied
    /// write. Only mark `false` where non-dispatch is provable from the control flow, not merely
    /// likely. `dispatched` narrows the Indeterminate set; it can never widen it.
    #[error("connection to backend lost (dispatched: {dispatched})")]
    ConnectionLost { dispatched: bool },
    /// The pool has been shut down and is no longer accepting checkouts.
    #[error("pool is closed")]
    Closed,
    /// An operation this backend/pool does not support in M0 (e.g. bare tx-control SQL bypassing
    /// the pin stub — v2/M1; an out-of-M0 column/param type on the row-returning path — S5).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// Any other backend-reported failure surfaced through the raw `simple_query` path (S4). Kept
    /// distinct from the richer [`PoolError::Sql`] the row-returning `query` path produces so the
    /// S4 raw-path tests (which match on `Backend(_)`) stay valid.
    #[error("backend error: {0}")]
    Backend(String),
    /// A statement-level SQL error from a **present, non-fatal** `DbError` on the row-returning
    /// `query` path (S5, MAJOR-9). Carries the proto classification (`code` + `branch`, off the
    /// SQLSTATE table) AND the raw SQLSTATE + server message, so the SQL service (S5 Task 3) can
    /// build the wire `ErrorPayload` without re-deriving anything.
    ///
    /// This is deliberately a DISTINCT variant from [`PoolError::ConnectionLost`]: a `Sql` error
    /// means the statement's fate is *KNOWN* — either the server answered and rejected it, OR a
    /// client-side bind pre-validation (`ferro-backend-pg`'s `query.rs`: wrong param arity / an
    /// uncastable param type) rejected it BEFORE it was ever sent, so it provably never executed.
    /// Either way the service must NOT apply the §19.3 `readonly`→`Indeterminate` override to it.
    /// Only a true transport/FATAL `ConnectionLost` (no answer, fate unknown) gets that override.
    #[error("sql error {code:#06x} (sqlstate {sqlstate:?}, errno {errno:?}): {message}")]
    Sql {
        code: u16,
        branch: u8,
        sqlstate: Option<String>,
        /// The backend's own numeric error code, when it HAS one (M1-S8a).
        ///
        /// MySQL/MariaDB do: `mysql_async::ServerError.code` is a `u16`, widened losslessly here to
        /// match the wire field (`ErrorPayload.errno: Option<i32>`). **PostgreSQL does not** — its
        /// error identity is the five-character SQLSTATE, so this stays `None` there forever, and so
        /// it does on every client-side bind pre-flight rejection (no server ever saw the statement).
        ///
        /// Why it must reach the wire at all: MySQL's SQLSTATEs are far coarser than its errnos —
        /// a duplicate key and a NOT NULL violation BOTH arrive as `23000` (measured) — so a
        /// consumer keyed on SQLSTATE alone cannot tell them apart. `classify_fate` passes it
        /// through verbatim; nothing re-derives a classification from it.
        errno: Option<i32>,
        message: String,
    },
}

/// SPEC §9.2 taxonomy branch. The engine never transparently retries user statements (charter
/// rule 3); `Retryable` only licenses the *caller* to retry per its own policy.
///
/// **PRE-INDETERMINATE COARSENING (S4):** this two-branch mapping (`Retryable`/`NonRetryable`) is
/// a simplification of the full SPEC §9.2 error tree, not the whole thing. In particular, a
/// `ConnectionLost` classified here as `Retryable` may in reality be a lost write or a COMMIT
/// whose response never arrived — which per SPEC §19.3 is actually **Indeterminate** (the
/// statement's fate is unknown), not safely retryable at all. The pool itself never auto-retries
/// either way (charter rule 3), so this coarsening changes no pool-level behavior today. But S5/S6
/// (the SQL/TX services) MUST layer the real Indeterminate classification on TOP of this label
/// before deciding whether re-dispatching a statement is safe — do not treat a pool
/// `Retryable`/`PoolError::ConnectionLost` alone as license to blindly re-send a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Branch {
    Retryable,
    NonRetryable,
}

impl PoolError {
    pub fn taxonomy_branch(&self) -> Branch {
        match self {
            PoolError::Timeout | PoolError::ConnectionLost { .. } => Branch::Retryable,
            PoolError::Closed | PoolError::Unsupported(_) | PoolError::Backend(_) => {
                Branch::NonRetryable
            }
            // A `Sql` error carries its own proto branch (off the SQLSTATE table): the retryable
            // classes (serialization failure / deadlock / connection-exception SQLSTATE) coarsen to
            // `Retryable`; everything else (incl. the Indeterminate branch, which the pool itself
            // never mints) coarsens to `NonRetryable`. The pool still never auto-retries either way
            // (charter rule 3) — this label only informs the caller's own policy.
            PoolError::Sql { branch, .. } => {
                if *branch == ferro_proto::consts::branch::RETRYABLE {
                    Branch::Retryable
                } else {
                    Branch::NonRetryable
                }
            }
        }
    }

    /// Maps to a `ferro_proto::consts::errc` code (the `/proto` registry — SPEC §20.2). There is
    /// no dedicated wire code for `Closed`/`Backend` yet, so both fall back to `PROTOCOL`; a `Sql`
    /// error already carries its registry code verbatim.
    pub fn errc(&self) -> u16 {
        use ferro_proto::consts::errc;
        match self {
            PoolError::Timeout => errc::POOL_TIMEOUT,
            PoolError::ConnectionLost { .. } => errc::CONNECTION_LOST,
            PoolError::Unsupported(_) => errc::UNSUPPORTED,
            PoolError::Closed | PoolError::Backend(_) => errc::PROTOCOL,
            PoolError::Sql { code, .. } => *code,
        }
    }

    /// Mark a [`PoolError::ConnectionLost`] as PROVABLY pre-dispatch (M1-S9a finding 3; see the
    /// variant doc for the direction rule). Identity on every other variant, so a phase-aware call
    /// site can write `error_map::map(&e).undispatched()` without re-deriving the classification —
    /// the mapper keeps deciding WHAT the error is, this only records WHEN it happened.
    ///
    /// Correct at exactly one kind of site: one where the control flow proves the user statement
    /// was not yet transmitted — a backend `prepare()` that returned `Err` before the Bind/Execute
    /// (PG) or `COM_STMT_EXECUTE` (MySQL) call was ever reached. Do not use it to express a hunch.
    pub fn undispatched(self) -> PoolError {
        match self {
            PoolError::ConnectionLost { .. } => PoolError::ConnectionLost { dispatched: false },
            other => other,
        }
    }
}
