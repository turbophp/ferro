//! Pin state machine (S4 Task 4 stub → M1-S1 Task 4 RFQ authority, SPEC §7.1 / §21 open item).
//!
//! M0 pinned on the TX-service lifecycle only. M1-S1 makes PostgreSQL's `ReadyForQuery` status byte
//! (I/T/E), surfaced as [`crate::backend::TxStatus`], the AUTHORITY: after every statement the pool
//! reads `tx_status` and `Checkout::apply_tx_status` updates the pin state from the real I/T/E (pin
//! on `T`/`E`, unpin on `I`). The explicit `begin_tx`/`commit_tx`/`rollback_tx` sets and the
//! `tx_control_class` guard remain as DEFENSE-IN-DEPTH, not the authority. M1-S2 (this module)
//! adds the ASSIST-lexer's 7 `PinCause` variants (`ferro-classify`'s `PinTrigger`, via
//! `From<PinTrigger>`) alongside the original RFQ-only `Tx` — the RFQ byte remains the sole
//! transaction-pin AUTHORITY; the lexer only ever ADDS a taint + cause label for protocol-invisible
//! session-state mutations (wiring into `Checkout` lands in Task 3, not here).
//!
//! A pinned connection is never handed to a second checkout: `Checkout` already holds its
//! connection exclusively (removed from the pool's idle stack for the lifetime of the guard), so
//! that invariant falls out of the existing checkout/Drop mechanics rather than needing separate
//! enforcement here.

use crate::backend::TxStatus;

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

/// Why a connection is (or was most recently) tainted/pinned. `Tx` is the RFQ-authoritative cause
/// (S1: an open/aborted transaction, set by `Checkout::apply_tx_status`). The seven variants in the
/// middle are M1-S2's assist-lexer causes (`ferro_classify::PinTrigger`, via [`From`] below) — set
/// by `Checkout::apply_classify` when a statement mutates protocol-invisible session state even
/// though the RFQ byte itself stays `Idle`. [`PinCause::SessionTracker`] is M1-S6's MySQL
/// OK-packet session-mutation cause (`Checkout::apply_session_tracker`), a distinct
/// protocol-derived assist signal — see its own doc below. `last_pin_cause` is "most recently
/// observed cause", not an exclusive state: setting an assist cause never clears/overrides `Tx`'s
/// `tainted`/`tx_open` bits (SPEC §7.1 — the assist signals are assist-only, never the authority).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinCause {
    /// An RFQ-detected open or aborted transaction (`T`/`E`) — the transaction-pin AUTHORITY.
    Tx,
    /// `LISTEN`/`UNLISTEN` — subscribes/unsubscribes the session to a notification channel.
    Listen,
    /// A session-scoped advisory lock function (`pg_advisory_lock` family, not `_xact`-scoped).
    AdvisoryLock,
    /// Raw client-side `PREPARE`/`EXECUTE`/`DEALLOCATE` of a named prepared statement.
    Prepare,
    /// Temp-object DDL (`CREATE TEMP/TEMPORARY ...`) or `SELECT ... INTO TEMP ...`.
    Temp,
    /// A non-local `SET` (persists past the current transaction/statement).
    Set,
    /// The statement referenced one of the pool's configured `pin_functions` escape-hatch names.
    PinFunction,
    /// Unrecognized/unclassifiable statement, tainted only when `pin_on_unknown` is set.
    Unknown,
    /// A MySQL/MariaDB **OK-packet session tracker** reported a session-state MUTATION on the last
    /// statement (M1-S6, `Checkout::apply_session_tracker` via `PoolBackend::take_session_mutated`).
    /// This is an ASSIST signal like the lexer causes above — it taints for reuse-safety without
    /// touching the transaction AUTHORITY (`tx_open`/`pin`) — but it is derived from the wire
    /// PROTOCOL (`OkPacket::session_state_info`), not from lexing the SQL, so it sees session
    /// mutations INSIDE stored programs (`SET SESSION …` in a proc body) that the assist lexer's
    /// §7.1 hard gate is blind to. It is deliberately distinct from `Set`: `Set` is the lexer's
    /// static guess from the SQL text, `SessionTracker` is the server's own OK-packet report.
    /// Postgres backends never raise it — `take_session_mutated` defaults to `false` there.
    SessionTracker,
}

impl From<ferro_classify::PinTrigger> for PinCause {
    /// Same-named 1:1 mapping from the assist lexer's trigger to the pool's pin-cause label.
    fn from(trigger: ferro_classify::PinTrigger) -> Self {
        match trigger {
            ferro_classify::PinTrigger::Listen => PinCause::Listen,
            ferro_classify::PinTrigger::AdvisoryLock => PinCause::AdvisoryLock,
            ferro_classify::PinTrigger::Prepare => PinCause::Prepare,
            ferro_classify::PinTrigger::Temp => PinCause::Temp,
            ferro_classify::PinTrigger::Set => PinCause::Set,
            ferro_classify::PinTrigger::PinFunction => PinCause::PinFunction,
            ferro_classify::PinTrigger::Unknown => PinCause::Unknown,
        }
    }
}

/// What KIND of transaction-control statement a SQL string leads with (M1-S8a).
///
/// The split exists because the two classes have different safety properties. A **boundary** verb
/// changes whether a transaction is OPEN — the pin AUTHORITY — so running one through a guarded
/// entry would let a client open a transaction the pool believes is not open, on a connection that
/// then returns to the pool for the next tenant (charter rule 6), with no `tx_id`, no actor, no
/// deadline and no rollback-on-session-death. A **savepoint** verb changes nothing about
/// transaction status — real Postgres's `ReadyForQuery` byte does not flip on any of them, which is
/// exactly what [`leading_tx_verb`] already models as "preserve" — and every savepoint dies with
/// its enclosing transaction, which the tx actor owns. So savepoints may pass through *inside a
/// transaction*, and Doctrine's nested-transaction emulation (`SAVEPOINT DOCTRINE_1` /
/// `RELEASE SAVEPOINT …` / `ROLLBACK TO SAVEPOINT …`, all plain `exec()` SQL) works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxControlClass {
    /// Opens or closes a transaction block. ALWAYS refused on the guarded entries.
    Boundary,
    /// Manages a savepoint WITHIN a transaction. Allowed iff the checkout already has one open.
    Savepoint,
}

/// Boundary verbs that stand alone.
const BOUNDARY_SINGLE: [&str; 4] = ["BEGIN", "COMMIT", "END", "ABORT"];
/// Boundary verbs spelled as two words.
const BOUNDARY_PAIR: [(&str, &str); 2] = [("START", "TRANSACTION"), ("PREPARE", "TRANSACTION")];
/// Savepoint verbs that stand alone (`SAVEPOINT n`, `RELEASE [SAVEPOINT] n`).
const SAVEPOINT_SINGLE: [&str; 2] = ["SAVEPOINT", "RELEASE"];

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
/// past [`tx_control_class`] and bypass the pin stub via `Checkout::exec` (MINOR 4, S4 review).
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

/// Classify `sql`'s leading keyword(s), comment/whitespace tolerant (the same scan the pre-M1-S8a
/// `is_bare_tx_control` used), as [`TxControlClass::Boundary`] / [`TxControlClass::Savepoint`], or
/// `None` for anything that is not transaction control at all.
///
/// `ROLLBACK` is the ONLY verb in both classes and the only one whose SECOND word decides:
/// `ROLLBACK TO [SAVEPOINT] n` is a savepoint operation, everything else spelled `ROLLBACK …`
/// (bare, `;`-terminated, `WORK`, `TRANSACTION`) ends the transaction.
///
/// **SCOPE — this is a leading-keyword classifier, NOT a parser.** It reads at most the first two
/// **contiguous** words, so a COMPOUND statement is classified by its leading verb only:
/// `SELECT 1; COMMIT` is `None`. "Contiguous" is load-bearing and narrower than it looks:
/// [`skip_leading_noise`] strips only LEADING comments, and [`leading_words`] then treats a
/// comment body's letters as a word — so an INTERIOR comment defeats every two-word rule
/// (`START /*x*/ TRANSACTION` reads `["START", "X"]` and classifies `None`). All of that is
/// pre-existing behaviour (the guard has always worked this way), pinned by table rows in
/// `s8a_tx_control_class_splits_boundary_from_savepoint`, and it is not a
/// leak — `crate::pool::Checkout::apply_tx_status` reads the real post-statement transaction status
/// off the protocol signal, so the pin engine still sees a transaction a compound statement opened.
/// `Checkout`'s guard adds ITS own single-statement requirement on top for the savepoint class,
/// because a savepoint passthrough runs on the multi-statement-capable text protocol.
pub(crate) fn tx_control_class(sql: &str) -> Option<TxControlClass> {
    let sql = skip_leading_noise(sql);
    let words = leading_words(sql, 2);
    let first = words.first()?.as_str();
    let second = words.get(1).map(String::as_str);

    if first == "ROLLBACK" {
        return Some(if second == Some("TO") {
            TxControlClass::Savepoint
        } else {
            TxControlClass::Boundary
        });
    }
    if BOUNDARY_SINGLE.contains(&first) {
        return Some(TxControlClass::Boundary);
    }
    if SAVEPOINT_SINGLE.contains(&first) {
        return Some(TxControlClass::Savepoint);
    }
    if let Some(second) = second
        && BOUNDARY_PAIR
            .iter()
            .any(|(a, b)| *a == first && *b == second)
    {
        return Some(TxControlClass::Boundary);
    }
    None
}

/// True if `sql` is a LONE statement — it carries no `;` other than an optional trailing one.
///
/// Deliberately conservative and deliberately NOT a parser: a `;` inside a quoted identifier or a
/// trailing comment (`SAVEPOINT "a;b"`) reads as a separator here and makes this `false`. Every
/// caller uses it to REFUSE, so the only possible error is refusing a statement that would have
/// been fine — never admitting one that would not. That direction is the whole point: it is the
/// condition [`crate::pool::Checkout`] puts on a savepoint PASSTHROUGH, which runs on the raw text
/// protocol, where both engines execute every statement in the string (PG `batch_execute`, MySQL
/// with `CLIENT_MULTI_STATEMENTS` negotiated). Without it, allowing a leading `SAVEPOINT` would
/// also allow the `COMMIT` riding behind it in `SAVEPOINT s; COMMIT`.
pub(crate) fn is_lone_statement(sql: &str) -> bool {
    let trimmed = sql.trim_end();
    let body = trimmed.strip_suffix(';').unwrap_or(trimmed);
    !body.contains(';')
}

/// True if `sql` starts with a bare transaction-control verb (`BEGIN`, `START TRANSACTION`,
/// `SAVEPOINT`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `RELEASE`, `PREPARE TRANSACTION`),
/// case-insensitively, after skipping leading whitespace and leading `--`/`/* */` comments.
///
/// This WAS the guard on `Checkout::exec`/`query`/`query_stream` until M1-S8a; those entries now
/// call the class-aware `Checkout::guard_tx_control` instead, so the boolean has no production
/// caller left. It is RETAINED, `#[cfg(test)]`, as a REGRESSION FIXTURE: it is derived from
/// [`tx_control_class`], so `s8a_is_bare_tx_control_is_unchanged_from_pre_s8a` (which asserts it
/// against the hand-written PRE-S8a expectations, not against `tx_control_class(..).is_some()`)
/// goes RED the moment a verb is dropped from, or added to, either class.
#[cfg(test)]
pub(crate) fn is_bare_tx_control(sql: &str) -> bool {
    tx_control_class(sql).is_some()
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
/// [`tx_control_class`]) as [`TxVerb::Open`]/[`TxVerb::Close`], or `None` if the leading verb
/// must leave transaction status UNCHANGED — i.e. "preserve", not "idle by default".
///
/// This is the shared scan `FakeBackend` (Task 3) uses to model `TxStatus` per-connection from the
/// SQL it records, so the fake's modeled `I`/`T`/`E` stays faithful to what real Postgres's RFQ
/// byte would report for the same statement — NOT merely "matches the guard's tx-control list". In particular a SAVEPOINT operation does NOT end the surrounding
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
/// Separate concern from [`tx_control_class`], which decides what the guarded `Checkout` entries
/// ADMIT. The two agree about savepoints for the same underlying reason — the RFQ byte does not
/// flip on them — but they answer different questions: this one models the resulting STATUS, that
/// one decides ADMISSION.
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

/// Maps an authoritative RFQ [`TxStatus`] to the two REUSE-SAFETY bits it dictates on a checked-out
/// connection — `(tx_open, force_taint)` — the bits that protect the NEXT tenant.
/// [`crate::pool::Checkout::apply_tx_status`] assigns `tx_open` from the first element
/// UNCONDITIONALLY (RFQ is the sole authority on whether a tx is open) and ORs the second into
/// `tainted` (so `Failed`/`E` can only ADD taint — it never clears one, and a clean `Idle`/`I`
/// never clears a prior taint here either; only the checkout-time recycle clears it).
///
/// The IDENTITY bits (`pin`/`last_pin_cause`) are deliberately NOT modelled here: "never clobber a
/// real `TxId`" needs `Checkout`'s own `self.pin`, so that logic lives in `apply_tx_status`, not in
/// this pure mapping.
///
/// - `Idle`   → `(false, false)` — no tx open; adds no taint (does NOT clear a prior one).
/// - `InTx`   → `(true, false)`  — a clean, open tx.
/// - `Failed` → `(true, true)`   — an open BUT aborted tx (RFQ `E`): must be `ROLLBACK`'d before reuse.
pub(crate) fn tx_status_bits(st: TxStatus) -> (bool, bool) {
    match st {
        TxStatus::Idle => (false, false),
        TxStatus::InTx => (true, false),
        TxStatus::Failed => (true, true),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PinCause, TxControlClass, TxVerb, is_bare_tx_control, is_lone_statement, leading_tx_verb,
        tx_control_class, tx_status_bits,
    };
    use crate::backend::TxStatus;
    use ferro_classify::PinTrigger;

    // ---- PinCause::from(PinTrigger) — M1-S2 Task 2 --------------------------------------------

    #[test]
    fn pin_cause_from_pin_trigger_maps_all_seven_same_named() {
        assert_eq!(PinCause::from(PinTrigger::Listen), PinCause::Listen);
        assert_eq!(
            PinCause::from(PinTrigger::AdvisoryLock),
            PinCause::AdvisoryLock
        );
        assert_eq!(PinCause::from(PinTrigger::Prepare), PinCause::Prepare);
        assert_eq!(PinCause::from(PinTrigger::Temp), PinCause::Temp);
        assert_eq!(PinCause::from(PinTrigger::Set), PinCause::Set);
        assert_eq!(
            PinCause::from(PinTrigger::PinFunction),
            PinCause::PinFunction
        );
        assert_eq!(PinCause::from(PinTrigger::Unknown), PinCause::Unknown);
    }

    #[test]
    fn tx_status_bits_maps_reuse_safety_bits() {
        // Idle: not open, forces no taint (must not clear a prior one).
        assert_eq!(tx_status_bits(TxStatus::Idle), (false, false));
        // InTx: open, clean.
        assert_eq!(tx_status_bits(TxStatus::InTx), (true, false));
        // Failed (E): open AND aborted -> must taint before reuse.
        assert_eq!(tx_status_bits(TxStatus::Failed), (true, true));
    }

    /// The split that makes savepoint passthrough safe. `ROLLBACK` is in BOTH classes and is the
    /// only verb whose SECOND word decides — bare `ROLLBACK` ends the transaction, `ROLLBACK TO …`
    /// does not (real PG's RFQ byte does not flip on the latter, `leading_tx_verb`'s own rationale).
    #[test]
    fn s8a_tx_control_class_splits_boundary_from_savepoint() {
        use TxControlClass::{Boundary, Savepoint};
        let cases: &[(&str, Option<TxControlClass>)] = &[
            ("BEGIN", Some(Boundary)),
            ("begin;", Some(Boundary)),
            ("START TRANSACTION READ ONLY", Some(Boundary)),
            ("COMMIT", Some(Boundary)),
            ("END", Some(Boundary)),
            ("ABORT", Some(Boundary)),
            ("ROLLBACK", Some(Boundary)),
            ("ROLLBACK;", Some(Boundary)),
            ("ROLLBACK WORK", Some(Boundary)),
            ("PREPARE TRANSACTION 'x'", Some(Boundary)),
            ("SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("savepoint doctrine_1", Some(Savepoint)),
            ("SavePoint DOCTRINE_1", Some(Savepoint)),
            ("RELEASE SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("RELEASE DOCTRINE_1", Some(Savepoint)),
            ("ROLLBACK TO SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("ROLLBACK TO DOCTRINE_1", Some(Savepoint)),
            ("rollback to savepoint doctrine_1", Some(Savepoint)),
            // Whitespace/newline tolerance — `skip_leading_noise` trims, `leading_words` treats any
            // non-alphabetic run as a separator, so a leading newline/tab must not hide the verb.
            ("   SAVEPOINT s   ", Some(Savepoint)),
            ("\n\tROLLBACK TO SAVEPOINT s\n", Some(Savepoint)),
            ("\n  COMMIT\n", Some(Boundary)),
            // Comment tolerance is inherited from `skip_leading_noise` and must survive.
            ("/* x */ ROLLBACK TO SAVEPOINT s", Some(Savepoint)),
            ("/* x */ SAVEPOINT s", Some(Savepoint)),
            ("-- x\nSAVEPOINT s", Some(Savepoint)),
            ("-- c\nBEGIN", Some(Boundary)),
            // A QUOTED savepoint identifier that is itself a keyword: `leading_words` reads
            // `["SAVEPOINT", "COMMIT"]` and the FIRST word decides, so the quoted `"commit"` can
            // never flip the class to Boundary. (Live-verified on PG: `SAVEPOINT "commit"` /
            // `RELEASE SAVEPOINT "commit"` both run and keep the transaction open.)
            (r#"SAVEPOINT "commit""#, Some(Savepoint)),
            (r#"RELEASE SAVEPOINT "commit""#, Some(Savepoint)),
            (r#"ROLLBACK TO SAVEPOINT "commit""#, Some(Savepoint)),
            // `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] n` is legal PG, but only the TWO leading
            // words are read, so the filler forms classify Boundary → REFUSED. That is a false
            // refusal in the SAFE direction (never the reverse), and no DBAL platform emits it:
            // `AbstractPlatform::createRollbackToSavepointSQL()` is `ROLLBACK TO SAVEPOINT <n>`.
            ("ROLLBACK WORK TO SAVEPOINT s", Some(Boundary)),
            ("ROLLBACK TRANSACTION TO SAVEPOINT s", Some(Boundary)),
            // Not tx control at all.
            ("SELECT 1", None),
            ("INSERT INTO t VALUES (1)", None),
            ("UPDATE savepoints SET x = 1", None),
            ("", None),
            ("   ", None),
            // COMPOUND statements: the classifier sees the LEADING verb ONLY, so a boundary verb in
            // a later position is invisible to it. Pinned here as the CURRENT, DELIBERATE behaviour
            // (hazard 64), not as an aspiration — this is what stops SPEC §22.2 (r) from claiming a
            // guarantee the guard does not provide. The pin AUTHORITY (`apply_tx_status`) is what
            // keeps a compound statement honest; this guard is not, and never was, a parser.
            // NOTE: the SAVEPOINT-leading compound rows classify `Savepoint` HERE and are still
            // REFUSED by `Checkout`'s guard, which additionally requires a savepoint passthrough to
            // be a LONE statement — see `pool.rs`'s `guard_tx_control`. Classification and the
            // guard's policy are deliberately separate concerns.
            ("SELECT 1; COMMIT", None),
            ("SAVEPOINT s2; START TRANSACTION", Some(Savepoint)),
            ("BEGIN; SELECT 1", Some(Boundary)),
            // INTERIOR comments: `skip_leading_noise` strips only LEADING noise, and `leading_words`
            // treats the comment body's letters as a word — so the two words this classifier reads
            // must be CONTIGUOUS. A comment BETWEEN them defeats every two-word rule. Pinned here as
            // the measured limit rather than left to be assumed (review F3); each row is the CURRENT
            // behaviour, not an aspiration.
            //
            // The two-word BOUNDARY forms fall to `None` — the only rows here in the permissive
            // direction. NOT a leak, and measured live on PG 17: the statement runs, then
            // `Checkout::apply_tx_status` reads the real RFQ `T` byte, sets `tx_open`, and the next
            // checkout of that conn recycles it (`tx_open=false` for the next tenant). The pin
            // AUTHORITY is the protocol signal; this classifier is defense-in-depth, never the
            // authority (SPEC §7.1). Widening it to skip interior comments would mean lexing SQL —
            // out of scope, and the authority already covers the case.
            ("START /*x*/ TRANSACTION", None),
            ("START -- c\nTRANSACTION", None),
            ("PREPARE /*x*/ TRANSACTION 'x'", None),
            // The `ROLLBACK`-family rows fall the SAFE way: the second word is no longer `TO`, so
            // they classify Boundary and the guarded entries REFUSE them. A false refusal, never a
            // false admission.
            ("ROLLBACK /*x*/ TO SAVEPOINT s", Some(Boundary)),
            ("ROLLBACK -- c\nTO SAVEPOINT s", Some(Boundary)),
            // A comment AFTER the pair is harmless — the two words were already contiguous.
            ("START TRANSACTION /*x*/ READ ONLY", Some(Boundary)),
            ("ROLLBACK TO /*x*/ SAVEPOINT s", Some(Savepoint)),
        ];
        for (sql, want) in cases {
            assert_eq!(tx_control_class(sql), *want, "tx_control_class({sql:?})");
        }
    }

    /// A savepoint PASSTHROUGH must be a lone statement, because it runs on the raw TEXT protocol
    /// where both engines execute every statement in the string. Conservative by design — a `;`
    /// inside a quoted identifier reads as a separator and is refused (the safe direction).
    #[test]
    fn s8a_is_lone_statement_rejects_anything_with_an_embedded_separator() {
        for sql in [
            "SAVEPOINT DOCTRINE_1",
            "SAVEPOINT DOCTRINE_1;",
            "  SAVEPOINT DOCTRINE_1 ;  ",
            "ROLLBACK TO SAVEPOINT DOCTRINE_1\n",
            "SELECT 1",
        ] {
            assert!(is_lone_statement(sql), "must be a lone statement: {sql:?}");
        }
        for sql in [
            "SAVEPOINT s; COMMIT",
            "SAVEPOINT s;COMMIT;",
            "SAVEPOINT s;;",
            "SELECT 1; COMMIT",
            r#"SAVEPOINT "a;b""#, // conservative: quoted `;` is refused, the safe direction
        ] {
            assert!(
                !is_lone_statement(sql),
                "must NOT be a lone statement: {sql:?}"
            );
        }
    }

    /// The boolean façade stays EXACTLY as strict as the pre-M1-S8a `is_bare_tx_control` was.
    ///
    /// **Asserted against the pre-S8a expected values, NOT against `tx_control_class(..).is_some()`**
    /// — the façade is now *defined* as that expression, so comparing the two would be a tautology
    /// (`assert_eq!(f(x), f(x))`) and could not fail for any classifier change. This table is the
    /// pre-change behaviour written out, so dropping a verb from either class goes RED here.
    #[test]
    fn s8a_is_bare_tx_control_is_unchanged_from_pre_s8a() {
        let cases: &[(&str, bool)] = &[
            ("BEGIN", true),
            ("COMMIT", true),
            ("END", true),
            ("ABORT", true),
            ("ROLLBACK", true),
            ("START TRANSACTION", true),
            ("PREPARE TRANSACTION 'x'", true),
            ("SAVEPOINT s", true),
            ("RELEASE s", true),
            ("RELEASE SAVEPOINT s", true),
            ("ROLLBACK TO s", true),
            ("ROLLBACK TO SAVEPOINT s", true),
            ("SELECT 1", false),
            ("UPDATE savepoints SET x = 1", false),
        ];
        for (sql, want) in cases {
            assert_eq!(
                is_bare_tx_control(sql),
                *want,
                "is_bare_tx_control({sql:?})"
            );
        }
    }

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
