use thiserror::Error;

/// Pool-level errors. These are a Rust enum, not wire types — `errc()` maps each variant to the
/// `/proto` error-code registry (SPEC §21 D9 charter rule: no hand-written protocol numbers) so
/// callers that DO produce a wire error use the registry constant, not a magic number.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PoolError {
    /// The caller waited longer than `checkout_timeout` for a permit/idle connection.
    #[error("timed out waiting for a pooled connection")]
    Timeout,
    /// The backend connection died (ping/round-trip failure, driver task ended, etc).
    #[error("connection to backend lost")]
    ConnectionLost,
    /// The pool has been shut down and is no longer accepting checkouts.
    #[error("pool is closed")]
    Closed,
    /// An operation this backend/pool does not support in M0 (e.g. bare tx-control SQL bypassing
    /// the pin stub — v2/M1).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// Any other backend-reported failure (e.g. a SQL error surfaced through the raw path).
    #[error("backend error: {0}")]
    Backend(String),
}

/// SPEC §9.2 taxonomy branch. The engine never transparently retries user statements (charter
/// rule 3); `Retryable` only licenses the *caller* to retry per its own policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Branch {
    Retryable,
    NonRetryable,
}

impl PoolError {
    pub fn taxonomy_branch(&self) -> Branch {
        match self {
            PoolError::Timeout | PoolError::ConnectionLost => Branch::Retryable,
            PoolError::Closed | PoolError::Unsupported(_) | PoolError::Backend(_) => {
                Branch::NonRetryable
            }
        }
    }

    /// Maps to a `ferro_proto::consts::errc` code (the `/proto` registry — SPEC §20.2). There is
    /// no dedicated wire code for `Closed`/`Backend` yet, so both fall back to `PROTOCOL`.
    pub fn errc(&self) -> u16 {
        use ferro_proto::consts::errc;
        match self {
            PoolError::Timeout => errc::POOL_TIMEOUT,
            PoolError::ConnectionLost => errc::CONNECTION_LOST,
            PoolError::Unsupported(_) => errc::UNSUPPORTED,
            PoolError::Closed | PoolError::Backend(_) => errc::PROTOCOL,
        }
    }
}
