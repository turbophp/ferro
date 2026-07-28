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

/// Skips leading ASCII whitespace and any leading `--` line comments / `/* ... */` block comments
/// (looping, since more than one can precede a statement) before the tx-control guard extracts its
/// leading keyword(s).
///
/// Without this, a leading comment's own letters become the "first word" under `leading_words`'s
/// non-alphabetic-is-a-separator rule — e.g. `/* x */ BEGIN` would extract `"X"` as the first
/// word, never seeing `BEGIN` at all, letting a commented-out-looking `BEGIN`/`ROLLBACK`/etc slip
/// past `is_bare_tx_control` and bypass the pin stub via `Checkout::exec` (MINOR 4, S4 review).
fn skip_leading_noise(sql: &str) -> &str {
    let mut rest = sql;
    loop {
        let trimmed = rest.trim_start();
        if let Some(after) = trimmed.strip_prefix("--") {
            // Line comment: skip to (but not including) the next newline, or to end-of-input if
            // there isn't one.
            rest = match after.find('\n') {
                Some(idx) => &after[idx + 1..],
                None => "",
            };
            continue;
        }
        if let Some(after) = trimmed.strip_prefix("/*") {
            // Block comment: skip to (but not including) the closing "*/". An unterminated block
            // comment consumes the rest of the input either way -- there's no keyword left to
            // find.
            rest = match after.find("*/") {
                Some(idx) => &after[idx + 2..],
                None => "",
            };
            continue;
        }
        return trimmed;
    }
}

/// True if `sql` starts with a bare transaction-control verb (`BEGIN`, `START TRANSACTION`,
/// `SAVEPOINT`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `RELEASE`, `PREPARE TRANSACTION`),
/// case-insensitively, after skipping leading whitespace and leading `--`/`/* */` comments. Used
/// by `Checkout::exec` (the guarded, user-facing entry) to reject statements that would bypass the
/// pin stub; the pin hook itself (`begin_tx`/`commit_tx`/`rollback_tx`) goes through the raw,
/// unguarded `PoolBackend::simple_query` instead and is never subject to this check.
pub(crate) fn is_bare_tx_control(sql: &str) -> bool {
    let sql = skip_leading_noise(sql);
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

/// Whether a leading transaction-control verb OPENS a transaction block (`BEGIN`,
/// `START TRANSACTION`) or CLOSES one (a bare `COMMIT`/`END`/`ABORT`, or a `ROLLBACK` NOT followed
/// by `TO`). Everything else tx-control-shaped is deliberately NOT classified here — see
/// [`leading_tx_verb`]'s doc comment for the full "preserve" list and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxVerb {
    Open,
    Close,
}

/// Classifies `sql`'s leading keyword(s) (comment/whitespace tolerant, same scan as
/// [`is_bare_tx_control`]) as [`TxVerb::Open`]/[`TxVerb::Close`], or `None` if the leading verb
/// must leave transaction status UNCHANGED — i.e. "preserve", not "idle by default".
///
/// This is the shared scan `FakeBackend` (Task 3) uses to model `TxStatus` per-connection from the
/// SQL it records, so the fake's modeled `I`/`T`/`E` stays faithful to what real Postgres's RFQ
/// byte would report for the same statement — NOT merely "matches `is_bare_tx_control`'s bare
/// tx-control list". In particular a SAVEPOINT operation does NOT end the surrounding
/// transaction on real Postgres (RFQ stays `T`), so:
///
/// - `BEGIN` / `START TRANSACTION` → [`TxVerb::Open`] (RFQ `I`→`T`).
/// - A BARE `COMMIT` / `END` / `ABORT`, or a `ROLLBACK` NOT immediately followed by `TO` (i.e.
///   `ROLLBACK` alone, `ROLLBACK;`, `ROLLBACK WORK`/`TRANSACTION`, ...) → [`TxVerb::Close`] (RFQ
///   →`I`).
/// - `SAVEPOINT <name>`, `RELEASE [SAVEPOINT] <name>`, and `ROLLBACK TO [SAVEPOINT] <name>` →
///   `None` (PRESERVE) — these manage a savepoint WITHIN an already-open transaction; real
///   Postgres's RFQ byte does not flip on any of them, so the model must not flip either.
/// - Any exotic/rare tx-control form this scan doesn't specifically classify (e.g.
///   `PREPARE TRANSACTION`) → `None` (PRESERVE-by-default, not "assume closed"). A test that needs
///   one of these to model a specific status drives `FakeConn::set_tx_status` explicitly instead.
/// - An ordinary, non-tx-control statement → `None` (PRESERVE, unchanged from before).
///
/// Does NOT change [`is_bare_tx_control`]'s own behavior — that guard's job (rejecting
/// `SAVEPOINT`/`RELEASE`/`ROLLBACK` at `Checkout::exec`/`query`) is a separate concern from this
/// status model, and its bare-tx-control list intentionally stays exactly as it was.
pub(crate) fn leading_tx_verb(sql: &str) -> Option<TxVerb> {
    let sql = skip_leading_noise(sql);
    let words = leading_words(sql, 2);
    let first = words.first()?;
    match first.as_str() {
        "BEGIN" => return Some(TxVerb::Open),
        "COMMIT" | "END" | "ABORT" => return Some(TxVerb::Close),
        "ROLLBACK" => {
            // `ROLLBACK TO <savepoint>` stays inside the transaction (RFQ stays `T`) -- only a
            // bare ROLLBACK (no `TO`, e.g. alone, `ROLLBACK;`, `ROLLBACK WORK`) ends it.
            return match words.get(1).map(String::as_str) {
                Some("TO") => None,
                _ => Some(TxVerb::Close),
            };
        }
        // Savepoint ops manage a transaction without opening/closing it -- preserve status.
        "SAVEPOINT" | "RELEASE" => return None,
        _ => {}
    }
    if let Some(second) = words.get(1)
        && first == "START"
        && second == "TRANSACTION"
    {
        return Some(TxVerb::Open);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{TxVerb, is_bare_tx_control, leading_tx_verb};

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

    #[test]
    fn guard_skips_leading_comments() {
        // MINOR 4 (S4 review): a leading block or line comment must not hide the real leading
        // keyword from the guard -- both of these are bare tx-control and must be rejected.
        assert!(
            is_bare_tx_control("/* c */ BEGIN"),
            "a leading block comment must not hide BEGIN from the guard"
        );
        assert!(
            is_bare_tx_control("-- c\nROLLBACK"),
            "a leading line comment must not hide ROLLBACK from the guard"
        );
        // A couple of adjacent variations, since the fix loops over multiple leading comments.
        assert!(is_bare_tx_control("/* a */ /* b */ COMMIT"));
        assert!(is_bare_tx_control("-- a\n-- b\nSTART TRANSACTION"));
        assert!(is_bare_tx_control("/* c */\n  Abort"));
        // An ordinary statement behind a leading comment must still be allowed.
        assert!(!is_bare_tx_control("/* c */ SELECT 1"));
    }

    #[test]
    fn leading_tx_verb_classifies_open_and_close() {
        assert_eq!(leading_tx_verb("BEGIN"), Some(TxVerb::Open));
        assert_eq!(
            leading_tx_verb("start transaction"),
            Some(TxVerb::Open),
            "START TRANSACTION is the two-word open verb"
        );
        assert_eq!(leading_tx_verb("COMMIT"), Some(TxVerb::Close));
        assert_eq!(leading_tx_verb("End"), Some(TxVerb::Close));
        assert_eq!(leading_tx_verb("Abort"), Some(TxVerb::Close));
    }

    #[test]
    fn leading_tx_verb_bare_rollback_closes_but_rollback_to_preserves() {
        // A bare ROLLBACK (alone, with a trailing `;`, or followed by WORK/TRANSACTION -- anything
        // that isn't `TO`) ends the transaction, matching real Postgres RFQ `T`/`E` -> `I`.
        assert_eq!(leading_tx_verb("ROLLBACK"), Some(TxVerb::Close));
        assert_eq!(leading_tx_verb("rollback;"), Some(TxVerb::Close));
        assert_eq!(leading_tx_verb("ROLLBACK WORK"), Some(TxVerb::Close));
        // ROLLBACK TO <savepoint> stays inside the transaction on real Postgres (RFQ stays `T`) --
        // the model must preserve, not close (verification finding on Task 3's review).
        assert_eq!(leading_tx_verb("ROLLBACK TO sp1"), None);
        assert_eq!(leading_tx_verb("rollback to savepoint sp1"), None);
    }

    #[test]
    fn leading_tx_verb_none_for_savepoint_ops_exotic_forms_and_ordinary_sql() {
        // SAVEPOINT/RELEASE manage a transaction WITHOUT opening/closing it -- real Postgres's RFQ
        // byte does not flip on either, so the model must preserve, not close (verification
        // finding on Task 3's review: these previously (wrongly) mapped to Close/Idle).
        assert_eq!(leading_tx_verb("SAVEPOINT sp1"), None);
        assert_eq!(leading_tx_verb("release sp1"), None);
        assert_eq!(leading_tx_verb("RELEASE SAVEPOINT sp1"), None);
        // Exotic tx-control this scan doesn't specifically classify -- preserve-by-default, not
        // "assume closed".
        assert_eq!(leading_tx_verb("Prepare Transaction 'foo'"), None);
        // An ordinary statement is not tx-control at all -- unchanged status either way.
        assert_eq!(leading_tx_verb("SELECT 1"), None);
        assert_eq!(leading_tx_verb(""), None);
    }
}
