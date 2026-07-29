//! Per-[`crate::Dialect`] classification rules (M1-S2 task T1b).
//!
//! Each `classify_one_*` function classifies a SINGLE already-split top-level statement (see
//! `scan::split_top_level_statements`) and is a plain keyword/identifier CLASSIFIER, not a SQL
//! parser: it looks at the leading keyword, a handful of well-known identifiers, and (for `SET`)
//! the token immediately following, and returns the first matching [`crate::PinTrigger`] rule.
//! First match wins; the ordering below is deliberate (see the per-function doc comments for why).
//!
//! Only `classify_one_pg` is wired to a live backend in M1-S2 (`Dialect::Postgres`, via
//! `ferro-backend-pg`); `classify_one_sqlite`/`classify_one_mysql` are stubs for a future slice.

use crate::PinTrigger;
use crate::scan;

/// Leading keywords that are known-safe: they never mutate protocol-invisible SESSION state (the
/// thing this crate exists to catch). Reused by all three dialects' stubs as the base safe list;
/// `Dialect::Postgres` layers its dialect-specific triggers (LISTEN, advisory locks, temp-object
/// DDL, ...) in front of this list (see `classify_one_pg`).
///
/// Rationale for the less-obvious entries (SPEC §7.1 assist rationale): `RESET`/`DISCARD` return
/// session state TOWARD default -- they don't accrue new cross-tenant state, and a freshly reset
/// connection is at default anyway. `LOCK` (bare `LOCK TABLE ...`) is transaction-scoped, already
/// covered by the RFQ authority (M1-S1). `CREATE`/`WITH` reach this list only via fall-through from
/// the earlier temp-object/`INTO TEMP` checks in `classify_one_pg`, so any `CREATE`/`WITH` that
/// gets here is already confirmed non-temp.
const SAFE_LEADING_KEYWORDS: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "WITH",
    "VALUES",
    "TABLE",
    "SHOW",
    "EXPLAIN",
    "ANALYZE",
    "VACUUM",
    "FETCH",
    "MOVE",
    "CLOSE",
    "COPY",
    // `CALL`/`DO` are safe-listed at the LEADING-KEYWORD level only: a session mutation (e.g.
    // `pg_advisory_lock`, `SET`/`set_config`) hidden INSIDE a `DO $$ ... $$` or procedure body is
    // NOT detected here. The scanner correctly masks dollar-quoted bodies (by design -- it must
    // not misparse `$$` contents as top-level SQL), so `contains_identifier_ci`/`pin_functions`
    // cannot see inside them, and the statement's own leading keyword (`DO`/`CALL`) never reaches
    // the function-reference or `SET` checks either. RFQ (M1-S1) does not help: PG does not emit a
    // separate RFQ per statement *inside* the procedure body, so an in-procedure session mutation
    // is invisible to both signals. This is the SPEC §7.4 documented transaction-mode limitation
    // (in-procedure/DO-body session mutation is unsupported except via session mode); the backstop
    // is S3 targeted hygiene + session mode, NOT `pin_functions` (which only matches top-level
    // statement text, not inside a masked dollar-quoted body). S3 follow-up: reconsider dropping
    // `DO`/`CALL` from this safe list so `pin_on_unknown` conservatively taints them and narrows
    // this window (at the cost of tainting every `DO`/`CALL`, including harmless ones).
    "CALL",
    "DO",
    "TRUNCATE",
    "MERGE",
    "CREATE",
    "ALTER",
    "DROP",
    "GRANT",
    "REVOKE",
    "COMMENT",
    "REFRESH",
    "REINDEX",
    "CLUSTER",
    "CHECKPOINT",
    "RESET",
    "LOCK",
    "DISCARD",
];

/// Session-scoped advisory-lock functions: acquiring one leaves the session holding a lock that
/// outlives the current statement/transaction, so a connection that ran one of these MUST be reset
/// before reuse. Deliberately excludes the `_xact` family (`pg_advisory_xact_lock`, ...) --
/// transaction-scoped, already released when the RFQ byte reports back to `I`(dle), so already
/// covered by the M1-S1 RFQ authority -- and every `pg_advisory_unlock*` (releasing a lock is
/// always safe, never a reason to taint).
const ADVISORY_SESSION_FUNCTIONS: &[&str] = &[
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
];

/// Postgres classification rules, IN ORDER (first match wins):
///
/// 1. `pin_functions` escape hatch (checked first: an operator-flagged function always wins,
///    regardless of what statement shape it appears in).
/// 2. leading `LISTEN`/`UNLISTEN` -> [`PinTrigger::Listen`].
/// 3. leading `PREPARE`/`EXECUTE`/`DEALLOCATE` -> [`PinTrigger::Prepare`].
/// 4. leading `SET` -> [`PinTrigger::Set`], UNLESS the next token is exactly `LOCAL` or
///    `TRANSACTION` (transaction-scoped, safe -- this fully decides the statement's fate, it does
///    not fall through to the generic safe-list/unknown rules below, since a bare `SET` is not
///    itself present in [`SAFE_LEADING_KEYWORDS`]).
/// 5. leading `CREATE` with `TEMP`/`TEMPORARY` before the object kind (skipping an optional `OR
///    REPLACE` and/or `GLOBAL`/`LOCAL` modifier, in that order -- PG's grammar is `CREATE [OR
///    REPLACE] [GLOBAL|LOCAL] {TEMP|TEMPORARY} ...`) -> [`PinTrigger::Temp`] for ANY temp object
///    kind. A non-temp `CREATE` (incl. plain `CREATE OR REPLACE VIEW ...`) falls through to the
///    safe-list (rule 8).
/// 6. `SELECT`/`WITH` containing `INTO TEMP`/`INTO TEMPORARY` -> [`PinTrigger::Temp`].
/// 7. a session-scoped advisory-lock function call -> [`PinTrigger::AdvisoryLock`]. This runs
///    BEFORE the leading-keyword safe-list (rule 8) is checked: a `SELECT` is otherwise safe, but
///    `SELECT pg_advisory_lock(1)` is still a real trigger regardless of its safe leading keyword.
/// 8. a known-safe leading keyword -> `None`.
/// 9. anything else (unrecognized/empty/unclassifiable) -> `Some(Unknown)` iff `pin_on_unknown`,
///    else `None`.
pub(crate) fn classify_one_pg(
    stmt: &str,
    pin_functions: &[String],
    pin_on_unknown: bool,
) -> Option<PinTrigger> {
    // 1. pin_functions escape hatch.
    if pin_functions
        .iter()
        .any(|f| scan::contains_identifier_ci(stmt, f))
    {
        return Some(PinTrigger::PinFunction);
    }

    let leading = scan::leading_keyword(stmt);

    // 2. LISTEN/UNLISTEN.
    if matches!(leading.as_deref(), Some("LISTEN") | Some("UNLISTEN")) {
        return Some(PinTrigger::Listen);
    }

    // 3. raw PREPARE/EXECUTE/DEALLOCATE.
    if matches!(
        leading.as_deref(),
        Some("PREPARE") | Some("EXECUTE") | Some("DEALLOCATE")
    ) {
        return Some(PinTrigger::Prepare);
    }

    // 4. SET, excluding SET LOCAL / SET TRANSACTION by exact token (not a second-word substring
    // match -- `SET local.foo`/`SET local_x` are dotted/underscored GUC names, NOT the bare
    // keyword LOCAL, so `next_token_after_keyword` correctly returns `None` for those and this
    // does NOT exclude them). This fully decides a leading-SET statement's fate: SET
    // LOCAL/TRANSACTION are transaction-scoped (same safety rationale as RESET/DISCARD), so they
    // resolve directly to `None` rather than falling through to the generic safe-list/unknown
    // rules (a bare "SET" is deliberately not itself in `SAFE_LEADING_KEYWORDS`, since an
    // unqualified `SET x=1` DOES persist and must trigger).
    if leading.as_deref() == Some("SET") {
        return match scan::next_token_after_keyword(stmt).as_deref() {
            Some("LOCAL") | Some("TRANSACTION") => None,
            _ => Some(PinTrigger::Set),
        };
    }

    // 5. CREATE ... TEMP/TEMPORARY (any object kind), e.g. `CREATE [GLOBAL|LOCAL] TEMP[ORARY]
    // TABLE/VIEW/SEQUENCE/... `. A non-temp CREATE falls through to the rule-8 safe-list.
    if leading.as_deref() == Some("CREATE") && create_is_temp(stmt) {
        return Some(PinTrigger::Temp);
    }

    // 6. SELECT/WITH ... INTO TEMP[ORARY] ... (`SELECT ... INTO TEMP t`).
    if matches!(leading.as_deref(), Some("SELECT") | Some("WITH")) && select_into_temp(stmt) {
        return Some(PinTrigger::Temp);
    }

    // 7. session-scoped advisory lock family -- MUST run before the rule-8 safe-list check: a
    // `SELECT` that calls `pg_advisory_lock` is still a trigger despite SELECT being an otherwise
    // safe leading keyword.
    if ADVISORY_SESSION_FUNCTIONS
        .iter()
        .any(|f| scan::contains_identifier_ci(stmt, f))
    {
        return Some(PinTrigger::AdvisoryLock);
    }

    // 8. known-safe leading keyword.
    if is_safe_leading_keyword(leading.as_deref()) {
        return None;
    }

    // 9. unrecognized/unclassifiable: conservative default per SPEC §7.1 (prefer a false taint to
    // a missed one) is the caller's `pin_on_unknown` flag.
    if pin_on_unknown {
        Some(PinTrigger::Unknown)
    } else {
        None
    }
}

/// Stub SQLite rules (not wired to a live backend in M1-S2). `ATTACH` brings a second database
/// file into the session's namespace; `PRAGMA` is treated conservatively as always state-changing
/// (some pragmas are query-only, but distinguishing them isn't worth the complexity for an
/// unwired stub -- SPEC §7.1's "prefer a false taint" principle covers this).
pub(crate) fn classify_one_sqlite(
    stmt: &str,
    pin_functions: &[String],
    pin_on_unknown: bool,
) -> Option<PinTrigger> {
    if pin_functions
        .iter()
        .any(|f| scan::contains_identifier_ci(stmt, f))
    {
        return Some(PinTrigger::PinFunction);
    }

    let leading = scan::leading_keyword(stmt);
    if matches!(leading.as_deref(), Some("ATTACH") | Some("PRAGMA")) {
        return Some(PinTrigger::Set);
    }

    if is_safe_leading_keyword(leading.as_deref()) {
        return None;
    }

    if pin_on_unknown {
        Some(PinTrigger::Unknown)
    } else {
        None
    }
}

/// Stub MySQL rules (not wired to a live backend in M1-S2; the real MySQL session-mutation signal
/// is the S6 tracker). Leading `SET` triggers unless the next token is exactly `LOCAL` or
/// `TRANSACTION` -- mirroring the Postgres exact-token exclusion for parity/future-proofing (MySQL
/// doesn't have `SET LOCAL`, but the guard is harmless). `SET SESSION ...` and `SET @@session...`
/// are deliberately NOT excluded (they fall into the general leading-SET trigger, since their next
/// token is `SESSION`/not-a-bare-keyword respectively) -- both persist for the session and must
/// taint.
pub(crate) fn classify_one_mysql(
    stmt: &str,
    pin_functions: &[String],
    pin_on_unknown: bool,
) -> Option<PinTrigger> {
    if pin_functions
        .iter()
        .any(|f| scan::contains_identifier_ci(stmt, f))
    {
        return Some(PinTrigger::PinFunction);
    }

    let leading = scan::leading_keyword(stmt);
    if leading.as_deref() == Some("SET") {
        return match scan::next_token_after_keyword(stmt).as_deref() {
            Some("LOCAL") | Some("TRANSACTION") => None,
            _ => Some(PinTrigger::Set),
        };
    }

    if is_safe_leading_keyword(leading.as_deref()) {
        return None;
    }

    if pin_on_unknown {
        Some(PinTrigger::Unknown)
    } else {
        None
    }
}

fn is_safe_leading_keyword(leading: Option<&str>) -> bool {
    matches!(leading, Some(kw) if SAFE_LEADING_KEYWORDS.contains(&kw))
}

/// True iff `stmt` (whose leading keyword is already confirmed `CREATE`) creates a TEMP/TEMPORARY
/// object of any kind: PG's grammar is `CREATE [OR REPLACE] [GLOBAL|LOCAL] {TEMP|TEMPORARY}
/// [RECURSIVE] <object-kind> ...` (`TABLE`, `VIEW`, `SEQUENCE`, `MATERIALIZED VIEW`, ...) -- `OR
/// REPLACE` legally precedes the `GLOBAL|LOCAL`/`TEMP|TEMPORARY` modifiers (e.g. `CREATE OR
/// REPLACE TEMP VIEW ...`), so both an optional `OR REPLACE` (two tokens) and an optional
/// `GLOBAL`/`LOCAL` (one token) must be skipped, IN THAT ORDER, before checking for
/// `TEMP`/`TEMPORARY`. Does not need to recognize the object-kind keyword itself (or `RECURSIVE`,
/// which comes AFTER `TEMP`/`TEMPORARY` and is therefore never in the way).
///
/// Built only from `scan`'s `pub(crate)` helpers via [`tokens_after_leading_keyword`] (itself built
/// only from `next_token_after_keyword`/`strip_leading_noise`, both boundary-checked/total) -- so
/// this stays panic-safe on any input without re-deriving the scanner's region-tracking logic.
fn create_is_temp(stmt: &str) -> bool {
    // Up to 4 tokens covers the longest legal prefix before TEMP/TEMPORARY: OR, REPLACE,
    // {GLOBAL|LOCAL}, {TEMP|TEMPORARY}.
    let tokens = tokens_after_leading_keyword(stmt, 4);
    let mut idx = 0usize;

    let tok = |i: usize| tokens.get(i).map(String::as_str);

    if tok(idx) == Some("OR") && tok(idx + 1) == Some("REPLACE") {
        idx += 2;
    }
    if matches!(tok(idx), Some("GLOBAL") | Some("LOCAL")) {
        idx += 1;
    }
    matches!(tok(idx), Some("TEMP") | Some("TEMPORARY"))
}

/// Returns up to `max` tokens following `stmt`'s OWN leading keyword (e.g. for `stmt` beginning
/// `CREATE OR REPLACE TEMP ...`, this returns `["OR", "REPLACE", "TEMP", ...]`), by repeatedly
/// re-anchoring: `next_token_after_keyword(rest)` gives the (boundary-checked) token right after
/// `rest`'s leading keyword, then [`skip_leading_keyword`] advances `rest` past that same leading
/// keyword so the token just found becomes the NEW leading keyword for the next iteration. Stops
/// early (returning fewer than `max` tokens) once `next_token_after_keyword` returns `None` (no
/// more complete tokens). Total/panic-safe: `rest` strictly shrinks each iteration (a `Some(_)`
/// token implies a non-empty leading-keyword run to skip past), and both underlying calls are
/// already boundary-checked.
fn tokens_after_leading_keyword(stmt: &str, max: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(max);
    let mut rest = stmt;
    for _ in 0..max {
        match scan::next_token_after_keyword(rest) {
            Some(tok) => {
                out.push(tok);
                rest = skip_leading_keyword(rest);
            }
            None => break,
        }
    }
    out
}

/// True iff `stmt` (whose leading keyword is already confirmed `SELECT`/`WITH`) contains an
/// `INTO TEMP`/`INTO TEMPORARY` clause (`SELECT ... INTO TEMP t`). Implemented as "contains `INTO`
/// AND contains `TEMP`-or-`TEMPORARY`", both whole-token/code-region checks via
/// `contains_identifier_ci` -- not a strict adjacency check (the two identifiers aren't confirmed
/// to be the SAME occurrence next to each other), which is a deliberate, documented bias toward
/// the safe direction (SPEC §7.1: prefer a false taint to a missed one) given the leaf crate's
/// scanner exposes whole-identifier matching, not phrase/adjacency matching.
fn select_into_temp(stmt: &str) -> bool {
    scan::contains_identifier_ci(stmt, "INTO")
        && (scan::contains_identifier_ci(stmt, "TEMP")
            || scan::contains_identifier_ci(stmt, "TEMPORARY"))
}

/// Advances `s` past its own leading keyword (after `strip_leading_noise`), returning the
/// remainder starting right where that keyword's maximal ASCII-alphabetic run ends (any noise
/// between the keyword and what follows is NOT re-stripped here -- `leading_keyword`/
/// `next_token_after_keyword` both call `strip_leading_noise` on their input first, so callers of
/// this function don't need to). Mirrors the exact token-extraction step used inside
/// `scan::leading_keyword`/`scan::next_token_after_keyword` themselves, built only from the
/// permitted `pub(crate)` primitive `scan::strip_leading_noise` plus `str::find`, which always
/// returns a char-boundary-safe index (or `None`, defaulted to the string's length) -- so this is
/// total/panic-safe on any input, including empty and multibyte strings.
fn skip_leading_keyword(s: &str) -> &str {
    let rest = scan::strip_leading_noise(s);
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    &rest[end..]
}
