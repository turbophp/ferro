//! Stubbed pin state machine (S4 Task 4, decision M-2).
//!
//! M0 pins on the TX-service lifecycle, NOT on the `ReadyForQuery` status byte — stock
//! `tokio-postgres` exposes no I/T/E byte, so a real pin engine driven off that signal is an M1
//! item (SPEC §21 open item). For S4 the pin hook is driven explicitly by `Checkout::begin_tx` /
//! `commit_tx` / `rollback_tx`, which the TX service (S6) will call in turn. `PinCause` only has
//! `Tx` in S4; other causes (e.g. session-level `SET`, advisory locks) are M1.
//!
//! A pinned connection is never handed to a second checkout: `Checkout` already holds its
//! connection exclusively (removed from the pool's idle stack for the lifetime of the guard), so
//! that invariant falls out of the existing checkout/Drop mechanics rather than needing separate
//! enforcement here.

/// Identifies a transaction for pinning purposes. Opaque to the pool — the TX service (S6) is the
/// one that allocates these; S4 only stores and reports the value it's given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxId(pub u64);

/// Whether a checked-out connection is pinned to a transaction, and to which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinState {
    Unpinned,
    PinnedTx(TxId),
}

/// Why a connection is (or was most recently) pinned. Only `Tx` is emitted in S4 — other causes
/// land with the real M1 pin engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinCause {
    Tx,
}

/// Single-word statements that open/close/manage a transaction on their own.
const SINGLE_WORD_TX_CONTROL: [&str; 7] = [
    "BEGIN",
    "SAVEPOINT",
    "COMMIT",
    "END",
    "ROLLBACK",
    "ABORT",
    "RELEASE",
];

/// Two-word tx-control verbs (Postgres synonyms) — v2/M2's load-bearing addition, since
/// `START TRANSACTION` is the missing open-tx verb a single-word check would miss.
const TWO_WORD_TX_CONTROL: [(&str, &str); 2] =
    [("START", "TRANSACTION"), ("PREPARE", "TRANSACTION")];

/// Extracts up to `max` leading "words" (maximal runs of ASCII alphabetic characters, uppercased)
/// from `sql`, treating everything else (whitespace, digits, punctuation) as a separator. This is
/// deliberately not a full SQL lexer (v2/M2 note) — matching the leading keyword(s) is enough to
/// keep bare tx-control statements from bypassing the pin stub via `Checkout::exec`.
fn leading_words(sql: &str, max: usize) -> Vec<String> {
    sql.split(|c: char| !c.is_ascii_alphabetic())
        .filter(|s| !s.is_empty())
        .take(max)
        .map(str::to_ascii_uppercase)
        .collect()
}

/// True if `sql` starts with a bare transaction-control verb (`BEGIN`, `START TRANSACTION`,
/// `SAVEPOINT`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `RELEASE`, `PREPARE TRANSACTION`),
/// case-insensitively, after skipping leading whitespace. Used by `Checkout::exec` (the guarded,
/// user-facing entry) to reject statements that would bypass the pin stub; the pin hook itself
/// (`begin_tx`/`commit_tx`/`rollback_tx`) goes through the raw, unguarded
/// `PoolBackend::simple_query` instead and is never subject to this check.
pub(crate) fn is_bare_tx_control(sql: &str) -> bool {
    let words = leading_words(sql, 2);
    let Some(first) = words.first() else {
        return false;
    };
    if SINGLE_WORD_TX_CONTROL.contains(&first.as_str()) {
        return true;
    }
    if let Some(second) = words.get(1) {
        return TWO_WORD_TX_CONTROL
            .iter()
            .any(|(a, b)| a == first && b == second);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::is_bare_tx_control;

    #[test]
    fn detects_single_word_verbs_case_insensitively() {
        assert!(is_bare_tx_control("BEGIN"));
        assert!(is_bare_tx_control("  RollBack  "));
        assert!(is_bare_tx_control("commit;"));
        assert!(is_bare_tx_control("Abort"));
        assert!(is_bare_tx_control("release"));
        assert!(is_bare_tx_control("End"));
        assert!(is_bare_tx_control("savepoint sp1"));
    }

    #[test]
    fn detects_two_word_verbs_case_insensitively() {
        assert!(is_bare_tx_control("start transaction"));
        assert!(is_bare_tx_control("START TRANSACTION"));
        assert!(is_bare_tx_control("Prepare Transaction 'foo'"));
    }

    #[test]
    fn allows_ordinary_statements() {
        assert!(!is_bare_tx_control("SELECT 1"));
        assert!(!is_bare_tx_control("insert into t values (1)"));
        assert!(!is_bare_tx_control(""));
        assert!(!is_bare_tx_control("   "));
    }
}
