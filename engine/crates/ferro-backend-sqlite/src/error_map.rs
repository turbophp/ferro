//! `rusqlite::Error` → `PoolError`, handle-fatal first then **extended-code keyed** (C3-3d).
//!
//! Mirrors `ferro-backend-pg` and `ferro-backend-mysql`: branch on the connection's FATE first, so
//! a failure that leaves the handle unusable becomes the DISTINCT [`PoolError::ConnectionLost`]
//! (fate unknown → the §19.3 `Indeterminate` override is eligible), and only a statement the engine
//! definitely ran and definitely rejected becomes [`PoolError::Sql`]. The S4 `fate.rs` matrix is
//! **reused verbatim** — this module only maps a code to a `(code, branch)` pair, exactly as the
//! MySQL one does.
//!
//! # `ffi::ErrorCode` is NEVER cast to an integer
//!
//! It is a plain Rust enum whose discriminants are its own declaration order, NOT SQLite's numeric
//! codes: `DatabaseBusy` is **3** while `SQLITE_BUSY` is **5**, and `OperationInterrupted` is **7**
//! while `SQLITE_INTERRUPT` is **9**. `ErrorCode::X as i32` compiles cleanly and is always wrong.
//! Everything below keys on `extended_code`, whose numbers are spelled out as constants and
//! ASSERTED against errors a real SQLite produces (`error_map_it.rs`) rather than derived on paper.
//!
//! # SQLite has an errno but no SQLSTATE — the mirror image of PostgreSQL
//!
//! PG's identity is the five-character SQLSTATE and its `errno` is `None` forever; MySQL has both;
//! SQLite has only a number. So `sqlstate` is `None` here and `errno` carries the EXTENDED code,
//! which is what lets a consumer tell `SQLITE_BUSY` (5) from `SQLITE_BUSY_SNAPSHOT` (517) even
//! though both classify to the same retryable-contention bucket — the same reason M1-S8a put
//! MySQL's errno on the wire beside its coarser SQLSTATE.
//!
//! # Why `SQLITE_BUSY_SNAPSHOT` is Retryable, which is not where the reasoning started
//!
//! The obvious argument says NonRetryable: `p4a` proved `busy_timeout` never retries a 517, and
//! re-sending the statement inside the same still-open transaction fails identically forever,
//! because the transaction's snapshot does not advance while it is open. Only ROLLBACK and replay
//! of the whole transaction can succeed, and charter rule 3 forbids the engine performing that.
//!
//! **But that is exactly PostgreSQL's `40001`, which this codebase already classifies Retryable.**
//! A serialization failure also cannot be fixed by re-sending the statement — PG refuses everything
//! in the aborted transaction with `25P02` — and it also needs the caller to replay the
//! transaction. `Branch`'s own doc settles the meaning: *"`Retryable` only licenses the CALLER to
//! retry per its own policy"*, which is transaction replay, not statement re-send. Classifying 517
//! NonRetryable would say something different about SQLite than this engine says about PostgreSQL
//! for the identical semantic, so it is `SerializationFailure` — which is also what it IS: a write
//! refused because another transaction committed past this one's snapshot.
//!
//! **Under D13 a 517 should be unreachable anyway, and that makes it diagnostic.** Every undeclared
//! or declared-write transaction takes the writer lock at `BEGIN IMMEDIATE`, so it never holds a
//! stale snapshot to upgrade from. Reaching 517 means the transaction took a DEFERRED lock — i.e.
//! the client DECLARED it `readonly` — and then wrote. The message says so, because that is a
//! client bug the developer needs to see rather than a database condition to retry around. Once
//! C3-4 wires `PRAGMA query_only` for declared-readonly checkouts, such a write is refused earlier
//! still, as `SQLITE_READONLY`.

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;

// SQLite's PRIMARY result codes. Spelled out, never `ErrorCode::X as i32` (see the module docs).
const SQLITE_ERROR: i32 = 1;
const SQLITE_PERM: i32 = 3;
const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;
const SQLITE_NOMEM: i32 = 7;
const SQLITE_READONLY: i32 = 8;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_IOERR: i32 = 10;
const SQLITE_CORRUPT: i32 = 11;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_CONSTRAINT: i32 = 19;
const SQLITE_AUTH: i32 = 23;
const SQLITE_NOTADB: i32 = 26;

// EXTENDED codes = primary | (N << 8). Asserted against real errors in `error_map_it.rs`.
const SQLITE_BUSY_SNAPSHOT: i32 = 517; // 5 | (2<<8)
const SQLITE_CONSTRAINT_CHECK: i32 = 275; // 19 | (1<<8)
const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = 787; // 19 | (3<<8)
const SQLITE_CONSTRAINT_NOTNULL: i32 = 1299; // 19 | (5<<8)
const SQLITE_CONSTRAINT_PRIMARYKEY: i32 = 1555; // 19 | (6<<8)
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067; // 19 | (8<<8)

/// Does this error mean the connection HANDLE is no longer trustworthy?
///
/// The list is short on purpose. SQLite runs IN-PROCESS, so unlike a wire protocol there is no
/// "sent it and never heard back" class: an ordinary statement failure means the statement ran and
/// was rejected, and its fate is KNOWN. Only these leave the handle or the database itself
/// unusable, and only these should be eligible for the §19.3 `Indeterminate` override.
fn is_handle_fatal(extended: i32) -> bool {
    matches!(
        extended & 0xff,
        SQLITE_IOERR | SQLITE_CORRUPT | SQLITE_NOTADB | SQLITE_CANTOPEN | SQLITE_NOMEM
    )
}

/// Classify an extended code into the `/proto` `(code, branch)` pair. `fate.rs` consumes it
/// verbatim and re-derives nothing.
fn classify(extended: i32) -> (u16, u8) {
    match extended {
        // ---- Retryable contention -------------------------------------------------------------
        // A bounded lock wait that expired. `p4b` measured `busy_timeout` genuinely parking for its
        // full duration before yielding this, so it is a real timed-out wait and a later attempt
        // may well succeed — the same shape as MySQL's 1205, which maps here for the same reason
        // (there is no dedicated lock-timeout proto code; SerializationFailure is the bucket).
        SQLITE_BUSY => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),
        // The deferred upgrade. See the module docs for why this is Retryable and not NonRetryable;
        // the EXTENDED code on the wire is what distinguishes it from a plain 5.
        SQLITE_BUSY_SNAPSHOT => (
            errc::SERIALIZATION_FAILURE,
            errc::SERIALIZATION_FAILURE_BRANCH,
        ),

        // ---- Statement cancel → CANCELLED, so the UNCHANGED `fate.rs::is_57014` override fires --
        // `p3` proved `InterruptHandle::interrupt()` really ends a running statement with this, so
        // it is the code the out-of-band cancel produces. Mapping it to CANCELLED yields the §19.3
        // cell (autocommit write → Indeterminate, in-tx → Retryable, declared read → Cancelled)
        // with no `fate.rs` edit, exactly as MySQL's 1317/3024/1969 do.
        SQLITE_INTERRUPT => (errc::CANCELLED, errc::CANCELLED_BRANCH),

        // ---- Known-fate constraint errors -----------------------------------------------------
        SQLITE_CONSTRAINT_UNIQUE | SQLITE_CONSTRAINT_PRIMARYKEY => {
            (errc::UNIQUE, errc::UNIQUE_BRANCH)
        }
        SQLITE_CONSTRAINT_FOREIGNKEY => (errc::FOREIGN_KEY, errc::FOREIGN_KEY_BRANCH),
        SQLITE_CONSTRAINT_NOTNULL => (errc::NOT_NULL, errc::NOT_NULL_BRANCH),
        SQLITE_CONSTRAINT_CHECK => (errc::CHECK, errc::CHECK_BRANCH),

        other => match other & 0xff {
            // A write against a connection that cannot write. Under D13 this is what a declared-
            // `readonly` checkout gives a client that writes anyway, once C3-4 arms
            // `PRAGMA query_only` — `p5` proved it is refused UP FRONT, so the statement provably
            // did not execute. `Unsupported` rather than `Auth`: nothing is wrong with the
            // credentials, this connection simply cannot accept a write (the same code also covers
            // a database file on read-only media).
            SQLITE_READONLY => (errc::UNSUPPORTED, errc::UNSUPPORTED_BRANCH),
            // Table-level lock contention (shared cache). Retryable for the same reason as BUSY.
            SQLITE_LOCKED => (
                errc::SERIALIZATION_FAILURE,
                errc::SERIALIZATION_FAILURE_BRANCH,
            ),
            SQLITE_PERM | SQLITE_AUTH => (errc::AUTH, errc::AUTH_BRANCH),
            // A constraint whose extended code is not one of the five above (TRIGGER, ROWID,
            // VTAB …). `Check` is the least-wrong constraint bucket; it is a bucket, not a claim
            // that a CHECK clause fired.
            SQLITE_CONSTRAINT => (errc::CHECK, errc::CHECK_BRANCH),
            // SQLITE_ERROR is SQLite's catch-all for a malformed or unresolvable statement (bad
            // syntax, no such table, no such column). Everything else unmapped lands here too:
            // `Syntax` is this codebase's established NonRetryable fallback bucket (the MySQL map
            // ends the same way), NOT an assertion about syntax.
            SQLITE_ERROR => (errc::SYNTAX, errc::SYNTAX_BRANCH),
            _ => (errc::SYNTAX, errc::SYNTAX_BRANCH),
        },
    }
}

/// `rusqlite::Error` → `PoolError`.
pub fn map(err: &rusqlite::Error) -> PoolError {
    match err {
        rusqlite::Error::SqliteFailure(ffi_err, msg) => {
            let extended = ffi_err.extended_code;
            let message = msg.clone().unwrap_or_else(|| err.to_string());
            if is_handle_fatal(extended) {
                tracing::warn!(
                    extended, %message,
                    "ferro-backend-sqlite: handle-fatal error; connection will be discarded"
                );
                return PoolError::ConnectionLost;
            }
            let (code, branch) = classify(extended);
            PoolError::Sql {
                code,
                branch,
                // SQLite has no SQLSTATE — the mirror image of PostgreSQL, which has no errno.
                sqlstate: None,
                errno: Some(extended),
                message: if extended == SQLITE_AUTH {
                    format!(
                        "{message} (SQLITE_AUTH: the statement asked the engine to open a database                          file outside this pool's allowed directory. Under SPEC D14 the engine                          confines every file it opens on a client's behalf — ATTACH and                          VACUUM INTO — to one directory, which defaults to the database's own.                          Name a path inside it, or have the operator widen the pool's allow_dir)"
                    )
                } else if extended == SQLITE_BUSY_SNAPSHOT {
                    format!(
                        "{message} (SQLITE_BUSY_SNAPSHOT: this transaction holds a stale read \
                         snapshot and cannot upgrade to a writer. Under SPEC D13 that is reachable \
                         only when the request DECLARED itself readonly and then wrote — roll back \
                         and replay the transaction, or stop declaring it readonly)"
                    )
                } else {
                    message
                },
            }
        }
        // Everything else rusqlite can produce is a client-side/driver fault the statement never
        // survived — a bind arity mismatch, an invalid column type, a UTF-8 failure. Fate is KNOWN
        // (it provably did not execute), so it is a `Sql` error, never `ConnectionLost`, and must
        // not become eligible for the §19.3 Indeterminate override.
        other => PoolError::Sql {
            code: errc::SYNTAX,
            branch: errc::SYNTAX_BRANCH,
            sqlstate: None,
            errno: None,
            message: other.to_string(),
        },
    }
}

/// Test-only: classify a bare extended code, so the handle-fatal list can be asserted without
/// manufacturing an I/O error or a corrupt database file.
#[doc(hidden)]
pub fn map_extended_for_test(extended: i32) -> PoolError {
    if is_handle_fatal(extended) {
        return PoolError::ConnectionLost;
    }
    let (code, branch) = classify(extended);
    PoolError::Sql {
        code,
        branch,
        sqlstate: None,
        errno: Some(extended),
        message: String::new(),
    }
}
