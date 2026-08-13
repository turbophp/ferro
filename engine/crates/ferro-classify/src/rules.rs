//! Per-[`crate::Dialect`] classification rules (M1-S2 task T1b).
//!
//! Each `classify_one_*` function classifies a SINGLE already-split top-level statement (see
//! `scan::split_top_level_statements`) and is a plain keyword/identifier CLASSIFIER, not a SQL
//! parser: it looks at the leading keyword, a handful of well-known identifiers, and (for `SET`)
//! the token immediately following, and returns the first matching [`crate::PinTrigger`] rule.
//! First match wins; the ordering below is deliberate (see the per-function doc comments for why).
//!
//! `classify_one_pg` is wired to a live backend in M1-S2 (`Dialect::Postgres`, via
//! `ferro-backend-pg`); `classify_one_mysql` is wired to a live backend in M1-S6 (`Dialect::MySql`,
//! via `ferro-backend-mysql`) as defense-in-depth ASSIST alongside that slice's session-tracker
//! AUTHORITY (`PoolBackend::take_session_mutated`); `classify_one_sqlite` remains a stub for a
//! future slice.

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

/// Session-scoped MySQL/MariaDB user-level lock functions (the `GET_LOCK`/named-lock family):
/// acquiring OR releasing one of these changes the session's held-lock set, and a single-statement
/// lexer has no memory of what else the session might still be holding across other statements --
/// so, UNLIKE Postgres's advisory-lock check (which excludes `pg_advisory_unlock*`, since PG's
/// bookkeeping there is per-key and releasing is always safe to leave un-pinned), MySQL's
/// `RELEASE_LOCK`/`RELEASE_ALL_LOCKS` are included here too: a false "still might be holding
/// something" taint is the safe direction (SPEC §7.1), and there is no cross-statement state in
/// this leaf crate to reason about whether releasing THIS lock leaves the session clean.
const MYSQL_LOCK_FUNCTIONS: &[&str] = &["GET_LOCK", "RELEASE_LOCK", "RELEASE_ALL_LOCKS"];

/// MySQL/MariaDB classification rules (M1-S6 task 6), IN ORDER (first match wins) -- ASSIST only.
/// Unlike Postgres (where this lexer + the RFQ protocol byte are the only two signals), MySQL has a
/// THIRD, stronger signal: the S6 session tracker (`PoolBackend::take_session_mutated`, wired via
/// `Checkout::apply_session_tracker`) reads the server's own OK-packet session-state-change report,
/// which sees INSIDE stored-program bodies this lexer cannot. That tracker is the session-mutation
/// AUTHORITY for this dialect; this function is defense-in-depth, same relationship the RFQ byte
/// has to `classify_one_pg`.
///
/// 1. `pin_functions` escape hatch (checked first, same as PG).
/// 2. raw `PREPARE`/`EXECUTE`/`DEALLOCATE` -> [`PinTrigger::Prepare`].
/// 3. leading `SET`, UNLESS the next token is exactly `LOCAL` or `TRANSACTION` -- `SET SESSION
///    ...`/`SET @@session...`/`SET GLOBAL ...` are NOT excluded (their next token is
///    `SESSION`/not-a-bare-keyword/`GLOBAL`, none of which match the exclusion), so they fall
///    through to the trigger, as they must (both persist for the session/globally). MySQL has no
///    `SET LOCAL`; the guard is harmless dead-but-safe parity with PG. `SET TRANSACTION ISOLATION
///    LEVEL ...` (no SESSION/GLOBAL) only affects the NEXT transaction, so it is excluded like PG's
///    `SET TRANSACTION`. Fully decides a leading-SET statement's fate (same as `classify_one_pg`'s
///    rule 4).
/// 4. `CREATE TEMPORARY TABLE ...` (any temp DDL [`create_is_temp`] recognizes) ->
///    [`PinTrigger::Temp`]. A non-temp `CREATE` falls through to the rule-8 safe-list.
/// 5. `LOCK TABLES ...` -> [`PinTrigger::AdvisoryLock`] (reused: MySQL's `LOCK TABLES` implicitly
///    commits any open transaction and then holds an explicit, session-scoped table lock until
///    `UNLOCK TABLES`/another `LOCK TABLES`/session end -- the same "session holds a lock that
///    outlives the statement" shape `AdvisoryLock` already names; there is no dedicated
///    `PinTrigger` variant for it, and adding one is out of this task's file-scoped brief). Checked
///    BEFORE the rule-8 safe-list: bare `LOCK` (PG's transaction-scoped `LOCK TABLE`) IS in the
///    shared [`SAFE_LEADING_KEYWORDS`], and would otherwise resolve `LOCK TABLES ...` to `None`.
/// 6. the CALL/DO conservative-fallback pin (verification P2/P3 -- the false-safety fix this task
///    closes): `CALL`/`DO` are in the shared [`SAFE_LEADING_KEYWORDS`] (see that const's doc for
///    why PG safe-lists them -- a documented SPEC §7.4 limitation), but a stored-procedure/`DO`-
///    block body can mutate session state (`SET SESSION ...`, `GET_LOCK`, ...) INSIDE it, where
///    this single-statement lexer cannot see. MySQL-ONLY -- the shared list itself is untouched
///    (`classify_one_pg` still safe-lists `CALL`/`DO` unchanged): every top-level MySQL `CALL`/`DO`
///    is treated as tracker-ambiguous and pinned UNCONDITIONALLY here -- this check does NOT gate on
///    `pin_on_unknown` (a deliberate, modest over-pin: every `CALL`/`DO`, including harmless ones,
///    pins, so an in-proc session mutation is caught even if the S6 session tracker misses it --
///    belt-and-braces with the tracker authority, not a substitute for it). Reuses
///    [`PinTrigger::Unknown`] (no new variant needed) purely as "conservatively pinned, cause not
///    lexically knowable" -- a DIFFERENT meaning from rule 9's generic "we don't recognize this
///    statement at all" (which IS gated on `pin_on_unknown`, unlike this rule).
/// 7. a MySQL session-lock function ([`MYSQL_LOCK_FUNCTIONS`]) referenced anywhere in the statement
///    -> [`PinTrigger::AdvisoryLock`]. Runs BEFORE the rule-8 safe-list check for the same reason
///    PG's advisory-lock check does: `SELECT GET_LOCK(...)` has an otherwise-safe `SELECT` leading
///    keyword.
/// 8. a known-safe leading keyword (shared [`SAFE_LEADING_KEYWORDS`]) -> `None`.
/// 9. anything else (unrecognized/empty/unclassifiable) -> `Some(Unknown)` iff `pin_on_unknown`,
///    else `None` (same conservative default as PG).
pub(crate) fn classify_one_mysql(
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

    // 2. raw PREPARE/EXECUTE/DEALLOCATE.
    if matches!(
        leading.as_deref(),
        Some("PREPARE") | Some("EXECUTE") | Some("DEALLOCATE")
    ) {
        return Some(PinTrigger::Prepare);
    }

    // 3. SET, excluding SET LOCAL / SET TRANSACTION by exact token (see fn-level doc rule 3).
    if leading.as_deref() == Some("SET") {
        return match scan::next_token_after_keyword(stmt).as_deref() {
            Some("LOCAL") | Some("TRANSACTION") => None,
            _ => Some(PinTrigger::Set),
        };
    }

    // 4. CREATE TEMPORARY TABLE / other temp DDL.
    if leading.as_deref() == Some("CREATE") && create_is_temp(stmt) {
        return Some(PinTrigger::Temp);
    }

    // 5. LOCK TABLES ... (see fn-level doc rule 5 for the AdvisoryLock reuse rationale).
    if leading.as_deref() == Some("LOCK")
        && scan::next_token_after_keyword(stmt).as_deref() == Some("TABLES")
    {
        return Some(PinTrigger::AdvisoryLock);
    }

    // 6. CALL/DO conservative-fallback pin -- MySQL-ONLY, UNCONDITIONAL (does NOT check
    // `pin_on_unknown`; see fn-level doc rule 6). Must run before rule 8's safe-list check, since
    // CALL/DO are safe-listed there for PG's sake.
    if matches!(leading.as_deref(), Some("CALL") | Some("DO")) {
        return Some(PinTrigger::Unknown);
    }

    // 7. MySQL session-lock functions, anywhere in the statement.
    if MYSQL_LOCK_FUNCTIONS
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

/// One top-level MySQL statement: does it CAUSE AN IMPLICIT COMMIT? (MySQL manual, "Statements
/// That Cause an Implicit Commit".) See [`crate::implicit_commit_hazard`] for the directional
/// rationale — this list enumerates the SAFE side; everything else, including an unknown leading
/// keyword, is a hazard.
pub(crate) fn mysql_implicit_commit_hazard(stmt: &str) -> bool {
    // A MySQL/MariaDB EXECUTABLE COMMENT (`/*!` or `/*M!`) is not a comment on this family — the
    // server runs its contents, so `/*! CREATE TABLE ... */` is DDL wearing a comment's clothes and
    // implicitly commits. The scanner masks all block comments (right for PostgreSQL), so without
    // this check such a statement reaches the `None` arm below and is called safe.
    //
    // M1-S9a review BLOCKER, reproduced end to end through the engine and re-measured here on BOTH
    // engines: `START TRANSACTION; INSERT; /*! CREATE TABLE <existing> */; ROLLBACK` leaves the
    // INSERT SURVIVING (the DDL errors 1050 and STILL commits). With the hazard false the latch
    // never set, and a later in-tx loss minted `CONNECTION_LOST{RETRYABLE}` over already-persisted
    // writes — the §22.2 (ai) at-least-once, resurrected. `mysqldump` emits DDL inside versioned
    // comments, so this is ordinary traffic, not a corner.
    //
    // Deliberately checked BEFORE the keyword scan and without parsing the contents: this assist may
    // only ever move toward MORE conservative, and calling a `/*! SELECT 1 */` a hazard costs one
    // cry-wolf `Indeterminate` on a statement that is ALSO lost, while missing a `/*! DROP TABLE */`
    // licenses replay of committed writes.
    if scan::has_mysql_executable_comment(stmt) {
        return true;
    }
    let Some(kw) = scan::leading_keyword(stmt) else {
        // No leading keyword. Genuinely empty or comment-only means nothing dispatchable, so
        // nothing can have committed — but anything else here is a statement this scanner could not
        // read, and an unreadable statement takes the conservative side, exactly as an unknown
        // keyword does below.
        return !scan::strip_leading_noise(stmt).trim().is_empty();
    };
    match kw.as_str() {
        // Never implicitly commit: plain DML, reads, diagnostics, tx-internal verbs, and the
        // server-side prepared-statement verbs (they may TAINT via classify_one_mysql — that is
        // a different, orthogonal question).
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH" | "TABLE" | "VALUES"
        | "SHOW" | "EXPLAIN" | "DESCRIBE" | "DESC" | "USE" | "HANDLER" | "SAVEPOINT"
        | "RELEASE" | "ROLLBACK" | "COMMIT" | "PREPARE" | "DEALLOCATE" => false,
        // CREATE/DROP TEMPORARY do NOT commit; every other CREATE/DROP does.
        "CREATE" => !create_is_temp(stmt),
        "DROP" => scan::next_token_after_keyword(stmt).as_deref() != Some("TEMPORARY"),
        // Plain SET never commits; SET PASSWORD and any autocommit assignment DO.
        // Plain SET never commits; SET PASSWORD, SET DEFAULT ROLE and any autocommit assignment DO.
        // `SET DEFAULT ROLE` is MySQL's account-management family and was MISSED by S9a — measured
        // in the review on MySQL 8.4.11 AND MariaDB 11.8.8: `BEGIN; INSERT; SET DEFAULT ROLE ...;
        // ROLLBACK` leaves the INSERT surviving on both, while the `SET ROLE` control does NOT
        // commit on either. Exact-token match, so `SET default_storage_engine = ...` (an ordinary
        // session GUC) is untouched — `next_token_after_keyword` refuses ident-continued words.
        "SET" => {
            matches!(
                scan::next_token_after_keyword(stmt).as_deref(),
                Some("PASSWORD") | Some("DEFAULT")
            ) || scan::contains_identifier_ci(stmt, "autocommit")
        }
        // The documented committing families (ALTER/RENAME/TRUNCATE/GRANT/REVOKE/ANALYZE/CHECK/
        // FLUSH/OPTIMIZE/REPAIR/RESET/CACHE/LOAD/INSTALL/UNINSTALL/LOCK/UNLOCK/START/BEGIN/XA/
        // CHANGE/STOP/PURGE), CALL/DO (a routine may run DDL), `EXECUTE` (see below), and every
        // UNKNOWN keyword.
        //
        // `EXECUTE` is deliberately NOT on the safe list above, and this is a MEASURED deviation
        // from the M1-S9a plan's draft (which listed it beside `PREPARE`/`DEALLOCATE`). The
        // implicit commit is a property of the statement that RUNS, not of how it was dispatched:
        // `PREPARE s FROM 'DROP TABLE t'; EXECUTE s` commits the open transaction at the EXECUTE.
        // Confirmed live on MySQL 8.4.11 AND MariaDB 11.8.8 (task-2 journal, "adversarial pass"):
        // BEGIN; INSERT; PREPARE s FROM 'CREATE TABLE …'; EXECUTE s; ROLLBACK  ⇒ the INSERT
        // SURVIVES the ROLLBACK on both engines. `PREPARE` and `DEALLOCATE` stay safe — neither
        // runs the prepared text — and `EXECUTE` is reachable through tx-scoped EXEC (it is not
        // transaction control, so `ferro-pool`'s `guard_tx_control` does not refuse it, unlike the
        // structurally-unreachable leading `COMMIT` the plan-verify pass refuted).
        _ => true,
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

#[cfg(test)]
mod s9a_review_blocker_tests {
    use crate::{Dialect, implicit_commit_hazard};

    /// **The M1-S9a review BLOCKER.** A MySQL/MariaDB EXECUTABLE COMMENT is not a comment — the
    /// server runs it — so DDL wrapped in `/*! ... */` implicitly commits. Measured on both engines:
    /// `START TRANSACTION; INSERT; /*! CREATE TABLE <existing> */; ROLLBACK` leaves the INSERT
    /// SURVIVING (the DDL errors 1050 and commits anyway).
    ///
    /// MUTATION that must make this RED: delete the `has_mysql_executable_comment` early-return in
    /// `mysql_implicit_commit_hazard`. Without it the statement reaches the `None` arm, is called
    /// safe, the latch never sets, and a later in-tx loss mints `Retryable` over committed writes.
    #[test]
    fn a_versioned_executable_comment_is_a_hazard() {
        for sql in [
            "/*! CREATE TABLE t (id INT) */",
            "/*!50000 CREATE TABLE t (id INT) */",
            "/*M!50000 CREATE TABLE t (id INT) */",
            "  /*!40000 ALTER TABLE t DISABLE KEYS */",
            // mysqldump's own shape: the wrapper sits mid-statement, not at the front.
            "CREATE TABLE t (id INT) /*!50100 PARTITION BY HASH (id) */",
        ] {
            assert!(
                implicit_commit_hazard(sql, Dialect::MySql),
                "executable comment must be a HAZARD on the MySQL family: {sql:?}"
            );
        }
    }

    /// The conservative direction is MySQL-only: PostgreSQL has no executable-comment syntax and its
    /// DDL is transactional, so nothing here may make PG more conservative.
    #[test]
    fn an_executable_comment_is_never_a_hazard_on_postgres() {
        assert!(!implicit_commit_hazard(
            "/*! CREATE TABLE t (id INT) */",
            Dialect::Postgres
        ));
    }

    /// A genuinely comment-only or empty statement dispatches nothing, so nothing can have
    /// committed — the `None` arm must stay false for these, or every no-op cries wolf.
    /// MUTATION: make the `None` arm return `true` unconditionally — this goes RED.
    #[test]
    fn an_ordinary_comment_only_statement_is_not_a_hazard() {
        for sql in [
            "",
            "   ",
            "-- just a note",
            "/* plain block comment */",
            "\n\t",
        ] {
            assert!(
                !implicit_commit_hazard(sql, Dialect::MySql),
                "comment-only/empty dispatches nothing: {sql:?}"
            );
        }
    }

    /// `SET DEFAULT ROLE` is MySQL's account-management family and DOES implicitly commit —
    /// measured on MySQL 8.4.11 and MariaDB 11.8.8. `SET ROLE` does NOT, and an ordinary session
    /// GUC whose name merely starts with `default` must not be swept in (exact-token match).
    /// MUTATION: drop `Some("DEFAULT")` from the SET arm — the first assertion goes RED.
    #[test]
    fn set_default_role_is_a_hazard_but_set_role_and_default_gucs_are_not() {
        assert!(implicit_commit_hazard(
            "SET DEFAULT ROLE ALL TO u@h",
            Dialect::MySql
        ));
        assert!(!implicit_commit_hazard("SET ROLE admin", Dialect::MySql));
        assert!(!implicit_commit_hazard(
            "SET default_storage_engine = InnoDB",
            Dialect::MySql
        ));
    }
}
