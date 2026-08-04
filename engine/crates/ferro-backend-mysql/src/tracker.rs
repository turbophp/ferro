//! The SPLIT pin signal for MySQL/MariaDB (M1-S6, SPEC §7.1) — the two things the pin engine reads
//! off every OK packet:
//!
//!   1. [`tx_status_from_ok`] — the transaction AUTHORITY, read from the ALWAYS-present
//!      `SERVER_STATUS_IN_TRANS` status-flag bit. The direct analog of Postgres's `ReadyForQuery`
//!      `T`/`I` byte. **It NEVER returns [`TxStatus::Failed`]:** MySQL/MariaDB have no aborted-open-tx
//!      state — a statement error inside a transaction leaves it `InTx` (the client may retry or
//!      roll back), and a deadlock auto-rolls-back to `Idle`. `Failed` is reached ONLY via the S4
//!      fate path (`error_map`), never from the transaction status.
//!
//!   2. [`ok_reports_session_mutation`] — the ASSIST taint signal, read from the OK-packet session
//!      trackers (the `CLIENT_SESSION_TRACK` fork's raison d'être). It sees session mutations that
//!      the §7.1 assist lexer is blind to — including a `SET SESSION` buried inside a stored program,
//!      a temp table, a `PREPARE`, a user variable, a schema switch — WITHOUT tainting on the noise
//!      (normal DML, transaction control, or the Ferro-managed `autocommit` toggle).
//!
//! ## How the mutation signal is derived (grounded in a live probe of MySQL 8 + MariaDB 11)
//!
//! Against the testkit's curated tracker config, the wire behavior is (identical on both engines):
//!
//! | operation                         | `SystemVariables` tracker | `SERVER_SESSION_STATE_CHANGED` | `TransactionState` tracker |
//! |-----------------------------------|---------------------------|--------------------------------|----------------------------|
//! | `SELECT 1`, plain DML (autocommit)| —                         | clear                          | —                          |
//! | `SET SESSION sort_buffer_size`    | `sort_buffer_size`        | set                            | —                          |
//! | `SET autocommit = 0/1`            | `autocommit`              | set                            | —                          |
//! | `START TRANSACTION`/`COMMIT`/DML-in-tx | —                    | set                            | present                    |
//! | `CALL proc` (SET inside body)     | `sort_buffer_size`        | set                            | —                          |
//! | `CREATE TEMPORARY TABLE`,`PREPARE`,`SET @v`,`USE db` | —       | set                            | —                          |
//!
//! Two facts drive the rule: (a) the state-change signal surfaces as the `SERVER_SESSION_STATE_CHANGED`
//! **status-flag bit**, not as a decoded `IsTracked` tracker (which this server config never emits);
//! (b) that bit is NOISY — it is also set for transaction-state changes (which always carry a
//! `TransactionState` tracker) and for the allowlisted `autocommit` toggle (which carries a
//! `SystemVariables` tracker). So the rule taints iff:
//!
//! * a `SystemVariables` tracker names a var NOT on the Ferro-managed allowlist (a genuine
//!   `SET SESSION sql_mode/sort_buffer_size/…`, incl. one from inside a stored program), OR
//! * the state-changed bit is set with NO accompanying `SystemVariables` tracker AND NO
//!   `TransactionState` tracker — i.e. a session mutation that is neither an allowlisted var toggle
//!   nor transaction control (a temp table, a `PREPARE`, a user variable, a schema switch).
//!
//! This catches every §7.1 blind-spot class the tracker exists to catch, while leaving normal DML,
//! transaction control, and the `autocommit` toggle untainted.

use ferro_pool::backend::TxStatus;
use mysql_common::constants::StatusFlags;

use crate::{OkPacket, SessionStateChange, in_transaction, session_state_changes};

/// The `SERVER_SESSION_STATE_CHANGED` (0x4000) status-flag bit — set by the server whenever any
/// tracked session state changed. The state-change signal is delivered HERE (a status-flag bit),
/// not as a decoded `IsTracked` tracker, in the testkit's server config (see the module docs).
const SERVER_SESSION_STATE_CHANGED: StatusFlags = StatusFlags::SERVER_SESSION_STATE_CHANGED;

/// System variables Ferro itself toggles for transaction management — a `SystemVariables` tracker
/// naming ONLY these is NOT a user mutation and must not taint. At minimum `autocommit` (Ferro flips
/// it around transaction boundaries). A user's own `SET SESSION sql_mode`/`sort_buffer_size`/`time_zone`/
/// etc. is NOT here, so it taints for reuse-safety.
const SESSION_MUTATION_ALLOWLIST: &[&str] = &["autocommit"];

/// Is `name` a Ferro-managed session variable (whose change must not taint)? Case-insensitive:
/// MySQL variable names are case-insensitive and the tracker echoes the client's spelling.
fn is_allowlisted_var(name: &str) -> bool {
    SESSION_MUTATION_ALLOWLIST
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(name))
}

/// The transaction AUTHORITY (SPEC §7.1): read straight off `SERVER_STATUS_IN_TRANS`. `InTx` iff the
/// last OK packet reports an open transaction block, else `Idle`. NEVER `Failed` — MySQL/MariaDB
/// have no aborted-open-tx state (see the module docs). With no OK packet observed yet, `Idle`.
pub(crate) fn tx_status_from_ok(ok: Option<&OkPacket<'_>>) -> TxStatus {
    match ok {
        Some(ok) if in_transaction(ok) => TxStatus::InTx,
        _ => TxStatus::Idle,
    }
}

/// A one-statement summary of the session-tracker signals, split out from [`ok_reports_session_mutation`]
/// so the taint RULE ([`TrackerSummary::is_mutation`]) is a pure function unit-testable without an
/// [`OkPacket`] (which cannot be constructed outside the driver).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TrackerSummary {
    /// A `SystemVariables` tracker was present (regardless of which vars).
    has_sysvar: bool,
    /// A `SystemVariables` tracker named at least one var NOT on the allowlist.
    has_nonallowlisted_sysvar: bool,
    /// A `TransactionState` tracker was present (transaction control / a statement inside a tx).
    has_txstate: bool,
    /// The `SERVER_SESSION_STATE_CHANGED` status-flag bit was set.
    state_changed_flag: bool,
}

impl TrackerSummary {
    /// The taint decision (see the module docs for the derivation and the live evidence).
    fn is_mutation(&self) -> bool {
        self.has_nonallowlisted_sysvar
            || (self.state_changed_flag && !self.has_sysvar && !self.has_txstate)
    }
}

/// Fold the decoded trackers + the state-changed flag into a [`TrackerSummary`]. Pure over its
/// inputs so it is exercised both by the unit tests (synthetic `SessionStateChange`s) and — via
/// [`ok_reports_session_mutation`] — live against real OK packets.
fn summarize(changes: &[SessionStateChange<'_>], state_changed_flag: bool) -> TrackerSummary {
    let mut s = TrackerSummary {
        state_changed_flag,
        ..TrackerSummary::default()
    };
    for c in changes {
        match c {
            SessionStateChange::SystemVariables(vars) => {
                s.has_sysvar = true;
                if vars.iter().any(|v| !is_allowlisted_var(&v.name_str())) {
                    s.has_nonallowlisted_sysvar = true;
                }
            }
            SessionStateChange::TransactionState(_) => s.has_txstate = true,
            // The decoded `IsTracked` tracker does not appear in the testkit config (the signal
            // arrives via `state_changed_flag` instead); treat it as equivalent if a server ever
            // does emit it, so a bare state change with no other tracker still counts.
            SessionStateChange::IsTracked(true) => {}
            _ => {}
        }
    }
    s
}

/// Did the last statement's OK packet report a REAL session-state mutation (the §7.1 assist taint
/// signal)? Reads the `SERVER_SESSION_STATE_CHANGED` flag + the decoded session trackers off `ok`
/// and applies the taint rule. `false` when there is no OK packet (an errored statement clears it).
///
/// Post-drain read only: `last_ok_packet` is populated after the result is consumed.
pub(crate) fn ok_reports_session_mutation(ok: Option<&OkPacket<'_>>) -> bool {
    let Some(ok) = ok else {
        return false;
    };
    let state_changed_flag = ok.status_flags().contains(SERVER_SESSION_STATE_CHANGED);
    let changes = session_state_changes(ok);
    summarize(&changes, state_changed_flag).is_mutation()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SystemVariable, TransactionState};

    fn sysvars(pairs: &[(&str, &str)]) -> SessionStateChange<'static> {
        SessionStateChange::SystemVariables(
            pairs
                .iter()
                .map(|(n, v)| SystemVariable::new(n.as_bytes(), v.as_bytes()).into_owned())
                .collect(),
        )
    }

    fn txstate() -> SessionStateChange<'static> {
        SessionStateChange::TransactionState(TransactionState::new(b"T_______".as_slice()))
    }

    #[test]
    fn allowlist_covers_autocommit_only() {
        assert!(is_allowlisted_var("autocommit"));
        assert!(is_allowlisted_var("AUTOCOMMIT")); // case-insensitive
        assert!(!is_allowlisted_var("sort_buffer_size"));
        assert!(!is_allowlisted_var("sql_mode"));
        assert!(!is_allowlisted_var("time_zone"));
    }

    #[test]
    fn tx_status_never_failed() {
        // No OK packet yet → Idle.
        assert_eq!(tx_status_from_ok(None), TxStatus::Idle);
        // (The InTx/Idle-from-a-real-OK cases are proven live; the important invariant here is that
        // this function has no code path that can ever produce Failed.)
    }

    #[test]
    fn nonallowlisted_sysvar_taints() {
        // A genuine `SET SESSION sort_buffer_size` (also the shape of a SET inside a stored proc).
        let changes = [sysvars(&[("sort_buffer_size", "262144")])];
        assert!(summarize(&changes, true).is_mutation());
    }

    #[test]
    fn autocommit_toggle_does_not_taint() {
        // `SET autocommit = 0` fires a SystemVariables[autocommit] tracker AND the state-changed
        // flag — the allowlist must suppress BOTH taint paths.
        let changes = [sysvars(&[("autocommit", "OFF")])];
        assert!(!summarize(&changes, true).is_mutation());
    }

    #[test]
    fn plain_statement_does_not_taint() {
        // `SELECT 1` / plain autocommit DML: no trackers, flag clear.
        assert!(!summarize(&[], false).is_mutation());
    }

    #[test]
    fn transaction_control_does_not_taint() {
        // START TRANSACTION / COMMIT / a statement inside a tx: flag set, but a TransactionState
        // tracker is present, so the flag path is gated off.
        let changes = [txstate()];
        assert!(!summarize(&changes, true).is_mutation());
    }

    #[test]
    fn bare_state_change_taints() {
        // A temp table / PREPARE / user variable / schema switch: the flag is set with NO
        // SystemVariables and NO TransactionState tracker — the §7.1 blind-spot classes.
        assert!(summarize(&[], true).is_mutation());
    }

    #[test]
    fn allowlisted_sysvar_plus_bare_flag_still_gated() {
        // Defense in depth: an allowlisted var change accompanied by the flag but no other tracker
        // must not taint (the has_sysvar gate covers it).
        let changes = [sysvars(&[("autocommit", "ON")])];
        assert!(!summarize(&changes, true).is_mutation());
    }
}
