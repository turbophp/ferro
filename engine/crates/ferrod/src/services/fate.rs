//! The write-fate matrix (SPEC §9.2 taxonomy + §19.3 `Indeterminate` rules): ONE pure function,
//! [`classify_fate`], decides `PoolError -> ErrorPayload` off an explicit per-call-site
//! [`OpContext`]. This is the single place that layers the §19.3 classification on top of the
//! pool's coarse taxonomy — every SQL/TX call site MUST route its `PoolError` through here rather
//! than hand-rolling a branch (charter rule 1: SPEC decisions are binding, don't route around
//! them).
//!
//! The engine NEVER retries a user statement (charter rule 3); the wire branch only informs the
//! client's own retry policy. A WRONG branch here is a Critical safety defect: a write
//! mis-classified as `Retryable`/`NonRetryable` when its true fate is unknown invites a client to
//! double-apply it.
//!
//! **`OpContext.in_tx`** means "this call site is reporting an in-transaction user STATEMENT",
//! NOT "a transaction happens to be open". A tx CONTROL boundary — BEGIN, COMMIT, ROLLBACK,
//! SAVEPOINT/RELEASE/ROLLBACK_TO — is always `in_tx: false` even though it runs on a pinned
//! in-transaction connection: those are control ops, not user statements, and a lost COMMIT in
//! particular MUST stay `Indeterminate` (a lost in-tx *statement*, by contrast, means the whole tx
//! is dead → `Retryable`, which passing `in_tx: true` yields — see [`OpContext`] docs and the
//! `sql.rs` call-site table for the exact per-site values).

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use ferro_proto::messages::ErrorPayload;

/// Per-call-site fate context. All three fields are load-bearing and MUST be set honestly by the
/// caller — `classify_fate` trusts them completely (it has no way to independently verify
/// `sent`/`in_tx`; get one wrong at a call site and the branch is wrong, silently).
///
/// - `readonly`: the client-declared read/write flag (SPEC charter rule 6: no SQL read/write
///   inference — this is the ONLY signal used, ever).
/// - `sent`: whether the statement was actually TRANSMITTED to the backend. `false` means a
///   provable "did-not-apply" (e.g. a checkout-time connect failure before any query ran) — such
///   a loss is always known-fate, never `Indeterminate`, even on a write.
/// - `in_tx`: whether this call site is an in-transaction user STATEMENT (as opposed to an
///   autocommit statement, or a tx CONTROL boundary — BEGIN/COMMIT/ROLLBACK/SAVEPOINT family,
///   which are always `in_tx: false`). A lost in-tx statement means the whole transaction is dead
///   (the actor rolls back + tombstones), so its fate is the KNOWN "the tx will never commit" →
///   `Retryable`, never `Indeterminate` for the statement itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpContext {
    pub readonly: bool,
    pub sent: bool,
    pub in_tx: bool,
}

/// Map a `PoolError` to a wire `ErrorPayload`. NEVER retries (charter rule 3); the branch only
/// informs the client's own policy.
///
/// Ordering (both load-bearing):
/// 1. The **57014 (cancel / `statement_timeout`) override** is checked FIRST, before the general
///    match — a cancelled/timed-out statement is reported via `errc::CANCELLED` on the wire
///    already (see `ferro-backend-pg`'s `classify_sqlstate("57014")`), so without this override it
///    would otherwise fall into the generic `Sql{..}` passthrough arm below and always report
///    `Cancelled{NonRetryable}` — wrong for an in-tx statement (must be `Retryable`, the tx is
///    dead) and wrong for a dispatched autocommit write (must be `Indeterminate`, unconfirmed
///    non-execution).
/// 2. Within the override, `in_tx` is tested **BEFORE** `readonly` (verification MAJOR): an in-tx
///    statement cancel is `Retryable` regardless of `readonly` — there is no `Cancelled/Retryable`
///    wire pairing (`/proto/errors.toml` pins `Cancelled` to `NonRetryable`), so an in-tx *read*
///    cancel must NOT fall through to `Cancelled{NonRetryable}`; it rides `TxDeadline{Retryable}`
///    exactly like an in-tx write cancel.
///
/// The override is **total over `sent`**: every one of the 8 `(readonly, sent, in_tx)` cells for a
/// 57014 resolves to one of the four defined branches below — none fall through to the general
/// match (proven by `fate_57014_total_over_all_axes`). Reaching a genuine 57014 at all means the
/// statement was necessarily on the wire (PG only emits `query_canceled` for a running query), so
/// the `sent=false` arm only arises once a caller's biased cancel-select can race a cancel in
/// BEFORE the query is ever dispatched (T2/T3 territory) — it is included here for totality and to
/// document the intended fate (`ConnectionLost{Retryable}` — an unsent write has no unknown fate),
/// not because this task's call sites can produce it.
pub fn classify_fate(err: PoolError, ctx: OpContext) -> ErrorPayload {
    if is_57014(&err) {
        return if ctx.in_tx {
            // In a transaction, ANY statement cancel/timeout means the whole tx is dead (the actor
            // rolls back + tombstones) -> Retryable. Checked before `readonly` on purpose: an in-tx
            // *read* cancel is still Retryable, never `Cancelled{NonRetryable}` (there is no
            // Cancelled/Retryable wire pairing to fall back on).
            payload(
                errc::TX_DEADLINE,
                errc::TX_DEADLINE_BRANCH,
                "transaction statement cancelled or timed out; the transaction was rolled back \
                 (retryable — the engine never re-runs)",
            )
        } else if ctx.readonly {
            // An autocommit READ cancel/timeout: safe to re-run, but the wire has no
            // Cancelled/Retryable pairing — the client's own read-retry policy rides on the
            // NonRetryable{Cancelled} label, not a fabricated Retryable.
            payload(
                errc::CANCELLED,
                errc::CANCELLED_BRANCH,
                "statement cancelled or timed out",
            )
        } else if ctx.sent {
            // An autocommit WRITE cancel/timeout that was DISPATCHED: unconfirmed non-execution —
            // §19.3 Indeterminate.
            payload(
                errc::WRITE_UNCONFIRMED,
                errc::WRITE_UNCONFIRMED_BRANCH,
                "write cancelled or timed out after dispatch; it may or may not have applied \
                 (§19.3 indeterminate — the engine never retries; retry is client policy)",
            )
        } else {
            // An autocommit WRITE cancel that fired BEFORE dispatch (sent=false): the statement
            // never reached the backend, so its fate is a KNOWN did-not-apply -> Retryable. (See
            // the module doc: reaching 57014 at all normally implies sent=true; this arm exists
            // for totality and for the future biased-select pre-dispatch race.)
            payload(
                errc::CONNECTION_LOST,
                errc::CONNECTION_LOST_BRANCH,
                "cancelled before the write was dispatched; it never reached the backend \
                 (retryable — the engine never retries)",
            )
        };
    }

    match err {
        // Known-fate: pass the proto classification through verbatim. Never Indeterminate — a
        // `Sql` error means the statement's fate is KNOWN (the server answered and rejected it, or
        // a client-side bind pre-validation rejected it before it was ever sent).
        PoolError::Sql {
            code,
            branch,
            sqlstate,
            errno,
            message,
        } => ErrorPayload {
            code,
            branch,
            sqlstate,
            // Verbatim. The taxonomy `code`/`branch` were already derived from it upstream
            // (`ferro-backend-mysql`'s `classify_errno`); this carries the RAW value so a consumer
            // that needs vendor-level identity — e.g. a Doctrine MySQL ExceptionConverter, which
            // matches on the errno EXCLUSIVELY — has it. Nothing downstream re-classifies from it.
            errno,
            message,
            detail: None,
            retry_after_ms: None,
        },
        // A connection loss. Indeterminate ONLY if the statement was transmitted, non-readonly,
        // AND not an in-tx statement:
        //  - !sent (checkout failed, never transmitted) -> known did-not-apply -> Retryable.
        //  - sent & readonly=false & !in_tx -> a possibly-applied autocommit write, fate UNKNOWN
        //    -> §19.3 Indeterminate.
        //  - sent & readonly=true -> a read that observed no result -> Retryable (client policy).
        //  - in_tx=true -> an in-tx statement link-loss means the WHOLE TX is dead (known outcome:
        //    it will never commit) -> Retryable, never Indeterminate for the statement itself.
        PoolError::ConnectionLost => {
            if ctx.sent && !ctx.readonly && !ctx.in_tx {
                payload(
                    errc::WRITE_UNCONFIRMED,
                    errc::WRITE_UNCONFIRMED_BRANCH,
                    "connection lost during an in-flight non-readonly statement; the write may or \
                     may not have applied (§19.3 indeterminate — the engine never retries; retry \
                     is client policy)",
                )
            } else {
                payload(
                    errc::CONNECTION_LOST,
                    errc::CONNECTION_LOST_BRANCH,
                    "connection lost with a known-fate outcome (statement not transmitted, a \
                     readonly read, or an in-tx statement whose transaction is now dead) — \
                     retryable; the engine never retries",
                )
            }
        }
        PoolError::Timeout => payload(
            errc::POOL_TIMEOUT,
            errc::POOL_TIMEOUT_BRANCH,
            "timed out waiting for a pooled connection",
        ),
        PoolError::Unsupported(m) => ErrorPayload {
            code: errc::UNSUPPORTED,
            branch: errc::UNSUPPORTED_BRANCH,
            sqlstate: None,
            errno: None,
            message: m,
            detail: None,
            retry_after_ms: None,
        },
        // No dedicated wire code for Closed/Backend yet — a generic NonRetryable Protocol, matching
        // `PoolError::errc()`'s own fallback. (Unchanged from the pre-T1 behavior.)
        PoolError::Closed => payload(errc::PROTOCOL, errc::PROTOCOL_BRANCH, "pool is closed"),
        PoolError::Backend(m) => ErrorPayload {
            code: errc::PROTOCOL,
            branch: errc::PROTOCOL_BRANCH,
            sqlstate: None,
            errno: None,
            message: m,
            detail: None,
            retry_after_ms: None,
        },
    }
}

/// A cancel or `statement_timeout`: `ferro-backend-pg`'s `classify_sqlstate` already maps SQLSTATE
/// `57014` to `(errc::CANCELLED, errc::CANCELLED_BRANCH)`, so detect either the raw sqlstate OR the
/// already-classified code — both identify the same server-side event, and a caller may construct
/// either shape (e.g. a synthetic test, or a future backend that skips the sqlstate string).
///
/// `pub(crate)`: reused by `tx::actor` (M1-S4 Task 3) to detect a BARE 57014 that resolved through
/// a tx-scoped statement's own `ExecStep::Completed` (an app-set `statement_timeout`, not the
/// actor's deadline/cancel select arms) — that case must route through the SAME
/// rollback+tombstone+`TxDeadline` exit the deadline/cancel arms use, so the actor needs the same
/// detection this module's override already performs, not a second hand-rolled copy of it.
pub(crate) fn is_57014(err: &PoolError) -> bool {
    matches!(
        err,
        PoolError::Sql { sqlstate, code, .. }
            if sqlstate.as_deref() == Some("57014") || *code == errc::CANCELLED
    )
}

fn payload(code: u16, branch: u8, message: &str) -> ErrorPayload {
    ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: message.to_string(),
        detail: None,
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::branch;

    fn sql_57014() -> PoolError {
        PoolError::Sql {
            code: errc::CANCELLED,
            branch: errc::CANCELLED_BRANCH,
            sqlstate: Some("57014".to_string()),
            errno: None,
            message: "canceling statement due to user request".to_string(),
        }
    }

    /// A 57014 constructed from the raw sqlstate alone (code/branch NOT yet classified as
    /// `Cancelled`) is detected the same as the fully-classified shape — `is_57014` must not
    /// silently depend on the caller having already set `code == errc::CANCELLED`.
    fn sql_57014_raw_sqlstate_only() -> PoolError {
        PoolError::Sql {
            code: 0,
            branch: 0,
            sqlstate: Some("57014".to_string()),
            errno: None,
            message: "canceling statement due to user request".to_string(),
        }
    }

    fn sql(code: u16, br: u8, sqlstate: &str) -> PoolError {
        PoolError::Sql {
            code,
            branch: br,
            sqlstate: Some(sqlstate.to_string()),
            errno: None,
            message: "x".to_string(),
        }
    }

    fn ctx(readonly: bool, sent: bool, in_tx: bool) -> OpContext {
        OpContext {
            readonly,
            sent,
            in_tx,
        }
    }

    // ---- The named fate-matrix table (readable, spec-traceable cases) --------------------------

    /// Exhaustive `(PoolError × readonly × sent × in_tx) -> (code, branch)` table — the S4 unit
    /// gate. Every row is independently meaningful; see the module doc for the ordering rules this
    /// proves.
    #[test]
    fn fate_matrix_table() {
        struct Case {
            name: &'static str,
            err: PoolError,
            ctx: OpContext,
            code: u16,
            branch: u8,
        }
        let cases = vec![
            // ---- 57014 (cancel / statement_timeout) override ----
            Case {
                name: "57014: autocommit write, dispatched, not in tx -> WriteUnconfirmed/Indeterminate",
                err: sql_57014(),
                ctx: ctx(false, true, false),
                code: errc::WRITE_UNCONFIRMED,
                branch: branch::INDETERMINATE,
            },
            Case {
                name: "57014: autocommit read -> Cancelled/NonRetryable",
                err: sql_57014(),
                ctx: ctx(true, true, false),
                code: errc::CANCELLED,
                branch: branch::NON_RETRYABLE,
            },
            Case {
                name: "57014: in_tx (write) -> TxDeadline/Retryable",
                err: sql_57014(),
                ctx: ctx(false, true, true),
                code: errc::TX_DEADLINE,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "57014: autocommit write, NOT dispatched -> ConnectionLost/Retryable",
                err: sql_57014(),
                ctx: ctx(false, false, false),
                code: errc::CONNECTION_LOST,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "57014: in_tx AND readonly -> Retryable (proves in_tx tested BEFORE readonly)",
                err: sql_57014(),
                ctx: ctx(true, true, true),
                code: errc::TX_DEADLINE,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "57014 via raw sqlstate only (code/branch not pre-classified) is still detected",
                err: sql_57014_raw_sqlstate_only(),
                ctx: ctx(false, true, false),
                code: errc::WRITE_UNCONFIRMED,
                branch: branch::INDETERMINATE,
            },
            // ---- ConnectionLost (the pre-existing rule, now gated by in_tx too) ----
            Case {
                name: "ConnectionLost: sent write, not in tx -> Indeterminate",
                err: PoolError::ConnectionLost,
                ctx: ctx(false, true, false),
                code: errc::WRITE_UNCONFIRMED,
                branch: branch::INDETERMINATE,
            },
            Case {
                name: "ConnectionLost: sent read -> Retryable",
                err: PoolError::ConnectionLost,
                ctx: ctx(true, true, false),
                code: errc::CONNECTION_LOST,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "ConnectionLost: not sent (checkout failure) -> Retryable",
                err: PoolError::ConnectionLost,
                ctx: ctx(false, false, false),
                code: errc::CONNECTION_LOST,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "ConnectionLost: in_tx statement -> Retryable, not Indeterminate (whole tx is dead)",
                err: PoolError::ConnectionLost,
                ctx: ctx(false, true, true),
                code: errc::CONNECTION_LOST,
                branch: branch::RETRYABLE,
            },
            // ---- lost-COMMIT: the declare_ctl path always passes in_tx:false; a commit-shaped
            // ConnectionLost (readonly:false, sent:true, in_tx:false) MUST stay Indeterminate ----
            Case {
                name: "lost COMMIT (in_tx:false, readonly:false, sent:true) -> Indeterminate",
                err: PoolError::ConnectionLost,
                ctx: ctx(false, true, false),
                code: errc::WRITE_UNCONFIRMED,
                branch: branch::INDETERMINATE,
            },
            // ---- known-fate Sql passthrough ----
            Case {
                name: "Sql 40001 (serialization failure) -> Retryable",
                err: sql(errc::SERIALIZATION_FAILURE, branch::RETRYABLE, "40001"),
                ctx: ctx(false, true, false),
                code: errc::SERIALIZATION_FAILURE,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "Sql 40P01 (deadlock) -> Retryable",
                err: sql(errc::DEADLOCK, branch::RETRYABLE, "40P01"),
                ctx: ctx(false, true, false),
                code: errc::DEADLOCK,
                branch: branch::RETRYABLE,
            },
            Case {
                name: "Sql 23505 (unique violation) -> NonRetryable",
                err: sql(errc::UNIQUE, branch::NON_RETRYABLE, "23505"),
                ctx: ctx(false, true, false),
                code: errc::UNIQUE,
                branch: branch::NON_RETRYABLE,
            },
            // ---- Timeout ----
            Case {
                name: "Timeout -> PoolTimeout/Retryable",
                err: PoolError::Timeout,
                ctx: ctx(false, false, false),
                code: errc::POOL_TIMEOUT,
                branch: branch::RETRYABLE,
            },
        ];

        for case in cases {
            let ep = classify_fate(case.err, case.ctx);
            assert_eq!(ep.code, case.code, "case [{}]: code", case.name);
            assert_eq!(ep.branch, case.branch, "case [{}]: branch", case.name);
        }
    }

    /// Totality proof over ALL 8 `(readonly, sent, in_tx)` cells for a 57014: every cell resolves
    /// to one of the four defined branches, in the documented `in_tx` > `readonly` > `sent` >
    /// else-order priority — none fall through to a stray `Indeterminate` (verification MINOR).
    #[test]
    fn fate_57014_total_over_all_axes() {
        for readonly in [false, true] {
            for sent in [false, true] {
                for in_tx in [false, true] {
                    let ep = classify_fate(sql_57014(), ctx(readonly, sent, in_tx));
                    let expected = if in_tx {
                        (errc::TX_DEADLINE, branch::RETRYABLE)
                    } else if readonly {
                        (errc::CANCELLED, branch::NON_RETRYABLE)
                    } else if sent {
                        (errc::WRITE_UNCONFIRMED, branch::INDETERMINATE)
                    } else {
                        (errc::CONNECTION_LOST, branch::RETRYABLE)
                    };
                    assert_eq!(
                        (ep.code, ep.branch),
                        expected,
                        "readonly={readonly} sent={sent} in_tx={in_tx}"
                    );
                    // Only the single (sent=true, readonly=false, in_tx=false) cell is Indeterminate.
                    let is_the_one_indeterminate_cell = sent && !readonly && !in_tx;
                    assert_eq!(
                        ep.branch == branch::INDETERMINATE,
                        is_the_one_indeterminate_cell,
                        "readonly={readonly} sent={sent} in_tx={in_tx}: Indeterminate must be \
                         exactly the dispatched-autocommit-write cell"
                    );
                }
            }
        }
    }

    /// The DETERMINISTIC proof of §19.3 for `ConnectionLost`, across ALL THREE axes (`sent`,
    /// `readonly`, `in_tx`). Indeterminate requires `sent && !readonly && !in_tx`; nothing else
    /// (and no SQL read/write inference) — ported from `sql.rs`'s
    /// `connection_lost_indeterminate_only_when_sent_and_write`, extended with the `in_tx` axis.
    #[test]
    fn connection_lost_indeterminate_only_when_sent_and_write_and_not_in_tx() {
        // sent + write + not in tx -> the ONE Indeterminate case.
        let sent_write = classify_fate(PoolError::ConnectionLost, ctx(false, true, false));
        assert_eq!(sent_write.code, errc::WRITE_UNCONFIRMED);
        assert_eq!(sent_write.branch, branch::INDETERMINATE);

        // sent + readonly -> Retryable.
        let sent_read = classify_fate(PoolError::ConnectionLost, ctx(true, true, false));
        assert_eq!(sent_read.code, errc::CONNECTION_LOST);
        assert_eq!(sent_read.branch, branch::RETRYABLE);

        // NOT sent (checkout failure) + write -> Retryable, NOT Indeterminate (a write that
        // provably never left the client is known-fate, not fate-unknown).
        let unsent_write = classify_fate(PoolError::ConnectionLost, ctx(false, false, false));
        assert_eq!(
            unsent_write.code,
            errc::CONNECTION_LOST,
            "a checkout-time (never-transmitted) loss must be known-fate Retryable, not Indeterminate"
        );
        assert_eq!(unsent_write.branch, branch::RETRYABLE);
        assert_ne!(unsent_write.code, errc::WRITE_UNCONFIRMED);
        assert_ne!(unsent_write.branch, branch::INDETERMINATE);

        // NOT sent + readonly -> Retryable.
        let unsent_read = classify_fate(PoolError::ConnectionLost, ctx(true, false, false));
        assert_eq!(unsent_read.code, errc::CONNECTION_LOST);
        assert_eq!(unsent_read.branch, branch::RETRYABLE);

        // sent + write + IN TX -> Retryable, NOT Indeterminate: an in-tx statement link-loss means
        // the whole transaction is dead (known outcome), never a lost-write-of-unknown-fate.
        let sent_write_in_tx = classify_fate(PoolError::ConnectionLost, ctx(false, true, true));
        assert_eq!(sent_write_in_tx.code, errc::CONNECTION_LOST);
        assert_eq!(sent_write_in_tx.branch, branch::RETRYABLE);
        assert_ne!(sent_write_in_tx.branch, branch::INDETERMINATE);
    }

    /// A known-fate `PoolError::Sql` passes through VERBATIM regardless of `readonly`/`sent`/
    /// `in_tx` — the Indeterminate override must NEVER touch it. This is what keeps a bind
    /// pre-validation error (`Sql{Unsupported}`) from being reclassified Indeterminate on a write.
    /// Ported from `sql.rs`'s `sql_error_passes_through_verbatim_ignoring_readonly`, extended with
    /// the `in_tx` axis.
    #[test]
    fn sql_error_passes_through_verbatim_ignoring_context() {
        let sql_err = sql(errc::SYNTAX, branch::NON_RETRYABLE, "42601");
        for readonly in [true, false] {
            for sent in [true, false] {
                for in_tx in [true, false] {
                    let ep = classify_fate(sql_err.clone(), ctx(readonly, sent, in_tx));
                    assert_eq!(ep.code, errc::SYNTAX);
                    assert_eq!(ep.branch, branch::NON_RETRYABLE);
                    assert_eq!(ep.sqlstate.as_deref(), Some("42601"));
                }
            }
        }

        // The exact bind-error shape (Sql{Unsupported}) on a WRITE (readonly=false), even at the
        // sent=true/in_tx=false site, stays Unsupported — NOT WriteUnconfirmed.
        let bind = PoolError::Sql {
            code: errc::UNSUPPORTED,
            branch: errc::UNSUPPORTED_BRANCH,
            sqlstate: None,
            errno: None,
            message: "parameter 0 type mismatch".to_string(),
        };
        let ep = classify_fate(bind, ctx(false, true, false));
        assert_eq!(ep.code, errc::UNSUPPORTED);
        assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);
        assert_ne!(ep.branch, branch::INDETERMINATE);
    }

    /// `classify_fate` is the ONE place a `PoolError` becomes a wire `ErrorPayload`. A MySQL `Sql`
    /// error's vendor errno must reach the wire VERBATIM alongside the SQLSTATE — DBAL's MySQL
    /// `ExceptionConverter` matches on the errno EXCLUSIVELY, and MySQL's SQLSTATEs cannot
    /// substitute (a duplicate key and a NOT NULL violation both arrive as `23000`).
    #[test]
    fn a_sql_errors_vendor_errno_reaches_the_wire_payload() {
        let dup = PoolError::Sql {
            code: errc::UNIQUE,
            branch: errc::UNIQUE_BRANCH,
            sqlstate: Some("23000".to_string()),
            errno: Some(1062),
            message: "Duplicate entry '1' for key 'PRIMARY'".to_string(),
        };
        let p = classify_fate(dup, ctx(false, true, false));
        assert_eq!(
            p.errno,
            Some(1062),
            "the vendor errno must pass through verbatim"
        );
        assert_eq!(p.sqlstate.as_deref(), Some("23000"));
        assert_eq!(p.code, errc::UNIQUE);
    }

    /// `classify_fate` MIRRORS the errno and never invents one.
    ///
    /// **Why this shape and not `a_postgres_sql_error_carries_no_errno`**: that test would feed in a
    /// `PoolError` the test itself built with `errno: None` (via the `sql()` helper) and assert
    /// `None` came out. **Correction (M1-S8a Task 3 review):** an earlier version of this comment
    /// called that "a TAUTOLOGY … it could not fail for any change to `fate.rs`", and that was
    /// OVERSTATED — the reviewer re-created the test verbatim and it went RED under the
    /// `errno.or(Some(code))` mutation. Its real defect is narrower: its **name and claim are about
    /// PostgreSQL** while it only exercises this file's mirror, so it proves a `fate.rs` property
    /// under a PG label. (Recorded rather than quietly reworded: in a repo whose dominant defect
    /// class is guards that cannot fail, a comment OVERSTATING a falsifiability proof is the same
    /// hazard inverted, and is worth catching in review.) This one drives
    /// BOTH arms of the mirror across a table, so hard-coding either `errno: None` or a derived
    /// value in the `Sql` arm goes RED; and it pins that the arms which have no backend behind them
    /// (`ConnectionLost`, `Timeout`) report `None` — the property that would break if someone
    /// "helpfully" defaulted the field.
    ///
    /// The claim that *PostgreSQL* never produces one is proven where the PG `PoolError` is BUILT,
    /// against a real server — see `ferro-backend-pg`'s `pg_query_it.rs`
    /// `a_real_pg_server_error_carries_no_errno` — not here.
    #[test]
    fn classify_fate_mirrors_the_errno_and_never_invents_one() {
        for want in [Some(1062), Some(1213), Some(1205), None] {
            let e = PoolError::Sql {
                code: errc::UNIQUE,
                branch: errc::UNIQUE_BRANCH,
                sqlstate: Some("23000".to_string()),
                errno: want,
                message: "x".to_string(),
            };
            let p = classify_fate(e, ctx(false, true, false));
            assert_eq!(p.errno, want, "the Sql arm must mirror the errno verbatim");
        }
        // The arms with no backend error behind them report None — they have nothing to report.
        for e in [PoolError::ConnectionLost, PoolError::Timeout] {
            let p = classify_fate(e, ctx(true, false, false));
            assert_eq!(
                p.errno, None,
                "a non-Sql PoolError has no vendor errno and must not fabricate one"
            );
        }
    }

    /// The §19.3 `is_57014` override DROPS the vendor `errno` and the raw SQLSTATE — deliberately,
    /// and this is the one exception to "`classify_fate` mirrors the errno" above.
    ///
    /// It matters because it is not a corner: `ferro-backend-mysql::classify_errno` maps `1317`
    /// (`KILL QUERY`), `3024` (MySQL `MAX_EXECUTION_TIME`) and `1969` (MariaDB `max_statement_time`)
    /// to `errc::CANCELLED` **precisely so this override fires**, so those three errnos — the entire
    /// MySQL cancel/timeout family — are exactly the ones that never reach the wire. A Doctrine
    /// `ExceptionConverter` keyed on them would never fire; the fate `code` is the signal. SPEC
    /// §22.2 (o) states this (M1-S8a review, finding F11 — the entry previously claimed the errno
    /// reached the wire verbatim with only PG and the bind pre-flight as exceptions), and this test
    /// is what stops that documented behaviour drifting silently in either direction.
    #[test]
    fn the_57014_override_drops_the_vendor_errno_and_sqlstate() {
        // The three errnos ferro-backend-mysql routes into this override, in the shape it builds.
        for errno in [1317, 3024, 1969] {
            let mysql_cancel = PoolError::Sql {
                code: errc::CANCELLED,
                branch: errc::CANCELLED_BRANCH,
                sqlstate: Some("HY000".to_string()),
                errno: Some(errno),
                message: "Query execution was interrupted".to_string(),
            };
            // Every reachable OpContext for a cancel: in-tx (TX_DEADLINE), autocommit read
            // (CANCELLED), autocommit dispatched write (WRITE_UNCONFIRMED).
            for c in [
                ctx(false, true, true),
                ctx(true, true, false),
                ctx(false, true, false),
            ] {
                let p = classify_fate(mysql_cancel.clone(), c);
                assert_eq!(
                    p.errno, None,
                    "the 57014 override replaces the vendor's account of what happened with the \
                     engine's own fate, so it must not forward errno {errno} beside it"
                );
                assert_eq!(
                    p.sqlstate, None,
                    "same for the raw SQLSTATE — the override's payload is engine-authored"
                );
                assert!(
                    matches!(
                        p.code,
                        errc::TX_DEADLINE | errc::CANCELLED | errc::WRITE_UNCONFIRMED
                    ),
                    "precondition: this case must actually take the override, else the assertions \
                     above are vacuous — got code {:#x}",
                    p.code
                );
            }
        }

        // CONTROL: a non-cancel MySQL error on the same axes keeps both, so the assertions above
        // are about the override and not about `classify_fate` losing the fields generally.
        let dup = PoolError::Sql {
            code: errc::UNIQUE,
            branch: errc::UNIQUE_BRANCH,
            sqlstate: Some("23000".to_string()),
            errno: Some(1062),
            message: "Duplicate entry".to_string(),
        };
        let p = classify_fate(dup, ctx(false, true, false));
        assert_eq!(p.errno, Some(1062));
        assert_eq!(p.sqlstate.as_deref(), Some("23000"));
    }

    /// `Timeout` (waiting for a pooled connection) always maps to `PoolTimeout{Retryable}`,
    /// regardless of context — there was never a statement in flight to have an unknown fate.
    #[test]
    fn timeout_is_always_pool_timeout_retryable() {
        for readonly in [true, false] {
            for sent in [true, false] {
                for in_tx in [true, false] {
                    let ep = classify_fate(PoolError::Timeout, ctx(readonly, sent, in_tx));
                    assert_eq!(ep.code, errc::POOL_TIMEOUT);
                    assert_eq!(ep.branch, branch::RETRYABLE);
                }
            }
        }
    }
}
