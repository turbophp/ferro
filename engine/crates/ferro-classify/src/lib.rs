//! `ferro-classify` — the assist lexer (SPEC §7.1).
//!
//! A dialect-aware keyword *classifier* (NOT a SQL parser) that flags statements mutating
//! protocol-invisible session state, so `ferro-pool` can taint + reset the connection before the
//! next tenant. The RFQ protocol byte (M1-S1) remains the transaction-pin AUTHORITY; this crate is
//! ASSIST-only (see the M1-S2 plan). This is a leaf crate: std-only, no `ferro-pool` dependency.
//!
//! Public API (task T1b, built on the T1a scanner in `scan.rs`): [`Dialect`], [`PinTrigger`], and
//! [`classify`]. `classify` is TOTAL (never panics — inherited from the scanner's panic-safety
//! guarantee) and multi-statement-aware (it classifies every top-level statement in `sql` and
//! reports the highest-precedence trigger found, so `Checkout::exec`'s `batch_execute` path is
//! covered, not just the leading statement).

mod rules;
mod scan;

/// The upstream SQL dialect being classified against. Only [`Dialect::Postgres`] is wired to a
/// live backend in M1-S2; `MySql`/`Sqlite` have stub rules (`rules::classify_one_{mysql,sqlite}`)
/// for a future slice. Derives `Default` (with `Postgres` as the default) because
/// `ferro-pool`'s `FakeBackend` derives `Default` and gains a `Dialect` field (M1-S2 task 2) —
/// without this derive, that struct's `#[derive(Default)]` would fail to compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dialect {
    #[default]
    Postgres,
    MySql,
    Sqlite,
}

/// Why a statement was classified as mutating protocol-invisible session state (SPEC §7.1's
/// assist trigger set). Maps 1:1 onto `ferro-pool`'s `PinCause` assist variants (task T2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinTrigger {
    /// `LISTEN`/`UNLISTEN` — subscribes/unsubscribes the session to a notification channel.
    Listen,
    /// A session-scoped advisory lock function (`pg_advisory_lock` family, NOT the `_xact`
    /// variants, which are transaction-scoped and already covered by the RFQ authority).
    AdvisoryLock,
    /// Raw client-side `PREPARE`/`EXECUTE`/`DEALLOCATE` — a named prepared statement outlives the
    /// current statement and is invisible to the RFQ byte.
    Prepare,
    /// Temp-object DDL (`CREATE TEMP/TEMPORARY ...`) or `SELECT ... INTO TEMP ...`.
    Temp,
    /// A non-local `SET` (persists past the current transaction/statement).
    Set,
    /// The statement references one of the pool's configured `pin_functions` escape-hatch names.
    PinFunction,
    /// Unrecognized/unclassifiable statement. Only surfaced when `pin_on_unknown` is set —
    /// SPEC §7.1's conservative default (prefer a false taint to a missed one).
    Unknown,
}

impl PinTrigger {
    /// Precedence rank used by [`classify`] to pick a single trigger across multiple top-level
    /// statements: `PinFunction > Listen > Prepare > Set > Temp > AdvisoryLock > Unknown` (any
    /// real trigger beats `Unknown`; the `pin_functions` escape hatch always wins). Higher wins.
    fn precedence(self) -> u8 {
        match self {
            PinTrigger::PinFunction => 6,
            PinTrigger::Listen => 5,
            PinTrigger::Prepare => 4,
            PinTrigger::Set => 3,
            PinTrigger::Temp => 2,
            PinTrigger::AdvisoryLock => 1,
            PinTrigger::Unknown => 0,
        }
    }
}

/// Classifies `sql` (one or more `;`-separated top-level statements) under `dialect`, returning
/// the highest-precedence [`PinTrigger`] found across all of them, or `None` if every statement is
/// safe. `pin_functions` is the per-pool escape hatch (any statement referencing one of these
/// identifiers is always `PinFunction`, dialect-independent); `pin_on_unknown` controls whether an
/// unrecognized/unclassifiable statement taints (`Some(Unknown)`) or not (`None`).
///
/// TOTAL: never panics on any input (empty, multibyte, unterminated string/comment/dollar-quote —
/// see `scan.rs`'s panic-safety guarantee, which every helper this function transitively calls
/// upholds).
pub fn classify(
    sql: &str,
    dialect: Dialect,
    pin_functions: &[String],
    pin_on_unknown: bool,
) -> Option<PinTrigger> {
    scan::split_top_level_statements(sql)
        .into_iter()
        .filter_map(|stmt| classify_one(stmt, dialect, pin_functions, pin_on_unknown))
        .max_by_key(|trigger| trigger.precedence())
}

/// Dispatches a single already-split top-level statement to the per-dialect rule set.
fn classify_one(
    stmt: &str,
    dialect: Dialect,
    pin_functions: &[String],
    pin_on_unknown: bool,
) -> Option<PinTrigger> {
    match dialect {
        Dialect::Postgres => rules::classify_one_pg(stmt, pin_functions, pin_on_unknown),
        Dialect::MySql => rules::classify_one_mysql(stmt, pin_functions, pin_on_unknown),
        Dialect::Sqlite => rules::classify_one_sqlite(stmt, pin_functions, pin_on_unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shorthand: classify a single Postgres statement with no `pin_functions` escape hatch and
    /// `pin_on_unknown = true` (SPEC §7.1's default), matching the brief's TDD corpus.
    fn pg(sql: &str) -> Option<PinTrigger> {
        classify(sql, Dialect::Postgres, &[], true)
    }

    // ---- Dialect::default() ---------------------------------------------------------------

    #[test]
    fn dialect_default_is_postgres() {
        assert_eq!(Dialect::default(), Dialect::Postgres);
    }

    // ---- LISTEN/UNLISTEN --------------------------------------------------------------------

    #[test]
    fn listen_triggers() {
        assert_eq!(pg("LISTEN c"), Some(PinTrigger::Listen));
    }

    #[test]
    fn unlisten_triggers() {
        assert_eq!(pg("UNLISTEN *"), Some(PinTrigger::Listen));
    }

    // ---- PREPARE/EXECUTE/DEALLOCATE ----------------------------------------------------------

    #[test]
    fn prepare_triggers() {
        assert_eq!(pg("PREPARE s AS SELECT 1"), Some(PinTrigger::Prepare));
    }

    #[test]
    fn execute_triggers() {
        assert_eq!(pg("EXECUTE s"), Some(PinTrigger::Prepare));
    }

    #[test]
    fn deallocate_triggers() {
        assert_eq!(pg("DEALLOCATE s"), Some(PinTrigger::Prepare));
    }

    // ---- SET / SET LOCAL / SET TRANSACTION exact-token exclusion ----------------------------

    #[test]
    fn set_plain_guc_triggers() {
        assert_eq!(pg("SET search_path=a,b"), Some(PinTrigger::Set));
    }

    #[test]
    fn set_session_triggers() {
        assert_eq!(pg("SET SESSION x=1"), Some(PinTrigger::Set));
    }

    #[test]
    fn set_dotted_guc_not_excluded() {
        // `local.foo` is a dotted session GUC name, NOT the bare token `LOCAL` -- persists past
        // the transaction, so it MUST still trigger (SET LOCAL exact-token exclusion, not a
        // second-word substring match).
        assert_eq!(pg("SET local.foo='x'"), Some(PinTrigger::Set));
    }

    #[test]
    fn set_local_is_excluded() {
        assert_eq!(pg("SET LOCAL x=1"), None);
    }

    #[test]
    fn set_transaction_is_excluded() {
        assert_eq!(pg("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"), None);
    }

    #[test]
    fn set_local_inside_comment_is_not_the_real_next_token() {
        // The comment is transparent; the real next token is `x`, not `LOCAL` -- so this must NOT
        // be excluded as SET LOCAL.
        assert_eq!(pg("SET/* LOCAL */x=1"), Some(PinTrigger::Set));
    }

    // ---- CREATE TEMP/TEMPORARY (any object kind) ---------------------------------------------

    #[test]
    fn create_temp_table_triggers() {
        assert_eq!(pg("CREATE TEMP TABLE t(x int)"), Some(PinTrigger::Temp));
    }

    #[test]
    fn create_temporary_table_triggers() {
        assert_eq!(
            pg("CREATE TEMPORARY TABLE t(x int)"),
            Some(PinTrigger::Temp)
        );
    }

    #[test]
    fn create_global_temporary_table_triggers() {
        assert_eq!(
            pg("CREATE GLOBAL TEMPORARY TABLE t(x int)"),
            Some(PinTrigger::Temp)
        );
    }

    #[test]
    fn create_temp_view_triggers() {
        assert_eq!(pg("CREATE TEMP VIEW v AS SELECT 1"), Some(PinTrigger::Temp));
    }

    #[test]
    fn create_temporary_sequence_triggers() {
        assert_eq!(pg("CREATE TEMPORARY SEQUENCE s"), Some(PinTrigger::Temp));
    }

    #[test]
    fn select_into_temp_triggers() {
        assert_eq!(pg("SELECT 1 INTO TEMP t"), Some(PinTrigger::Temp));
    }

    #[test]
    fn create_table_non_temp_is_safe() {
        assert_eq!(pg("CREATE TABLE t(x int)"), None);
    }

    // ---- session advisory locks (NOT _xact, NOT unlock*) -------------------------------------

    #[test]
    fn advisory_lock_triggers() {
        assert_eq!(
            pg("SELECT pg_advisory_lock(1)"),
            Some(PinTrigger::AdvisoryLock)
        );
    }

    #[test]
    fn advisory_try_lock_shared_triggers() {
        assert_eq!(
            pg("SELECT pg_try_advisory_lock_shared(1)"),
            Some(PinTrigger::AdvisoryLock)
        );
    }

    #[test]
    fn advisory_xact_lock_is_safe() {
        assert_eq!(pg("SELECT pg_advisory_xact_lock(1)"), None);
    }

    #[test]
    fn advisory_xact_lock_shared_is_safe() {
        assert_eq!(pg("SELECT pg_advisory_xact_lock_shared(1)"), None);
    }

    #[test]
    fn advisory_unlock_is_safe() {
        assert_eq!(pg("SELECT pg_advisory_unlock(1)"), None);
    }

    #[test]
    fn advisory_unlock_all_is_safe() {
        assert_eq!(pg("SELECT pg_advisory_unlock_all()"), None);
    }

    #[test]
    fn advisory_lock_quoted_identifier_triggers() {
        // The advisory-lock check (rule 7) MUST run before the leading-keyword safe-list check
        // (rule 8): a plain SELECT is otherwise safe, but a SELECT that calls an advisory-lock
        // function -- even via a quoted identifier, which is CODE not a hidden literal -- must
        // still be caught as AdvisoryLock, not fall through to the SELECT safe-list as None.
        assert_eq!(
            pg(r#"SELECT "pg_advisory_lock"(1)"#),
            Some(PinTrigger::AdvisoryLock)
        );
    }

    #[test]
    fn advisory_lock_inside_string_is_safe() {
        assert_eq!(pg("SELECT 'pg_advisory_lock'"), None);
    }

    #[test]
    fn advisory_lock_inside_line_comment_is_safe() {
        assert_eq!(pg("-- pg_advisory_lock\nSELECT 1"), None);
    }

    // ---- plain safe statements ----------------------------------------------------------------

    #[test]
    fn plain_select_is_safe() {
        assert_eq!(pg("SELECT 1"), None);
    }

    #[test]
    fn plain_insert_is_safe() {
        assert_eq!(pg("INSERT INTO t VALUES(1)"), None);
    }

    #[test]
    fn plain_with_cte_is_safe() {
        assert_eq!(pg("WITH x AS (SELECT 1) SELECT * FROM x"), None);
    }

    #[test]
    fn plain_merge_is_safe() {
        assert_eq!(
            pg("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING"),
            None
        );
    }

    #[test]
    fn reset_is_safe() {
        assert_eq!(pg("RESET search_path"), None);
    }

    #[test]
    fn discard_all_is_safe() {
        assert_eq!(pg("DISCARD ALL"), None);
    }

    // ---- multi-statement precedence ------------------------------------------------------------

    #[test]
    fn multi_statement_later_trigger_wins() {
        assert_eq!(pg("SELECT 1; LISTEN c"), Some(PinTrigger::Listen));
    }

    #[test]
    fn multi_statement_all_safe_is_none() {
        assert_eq!(pg("SELECT 1; SELECT 2"), None);
    }

    // ---- pin_on_unknown ---------------------------------------------------------------------

    #[test]
    fn unknown_statement_triggers_when_pin_on_unknown() {
        assert_eq!(
            classify("FLUFF nonsense", Dialect::Postgres, &[], true),
            Some(PinTrigger::Unknown)
        );
    }

    #[test]
    fn unknown_statement_is_none_when_not_pin_on_unknown() {
        assert_eq!(
            classify("FLUFF nonsense", Dialect::Postgres, &[], false),
            None
        );
    }

    // ---- pin_functions escape hatch -----------------------------------------------------------

    #[test]
    fn pin_function_triggers() {
        assert_eq!(
            classify(
                "SELECT app_lock(1)",
                Dialect::Postgres,
                &["app_lock".to_string()],
                true
            ),
            Some(PinTrigger::PinFunction)
        );
    }

    #[test]
    fn pin_function_is_whole_ident_not_substring() {
        assert_eq!(
            classify(
                "SELECT my_app_lock(1)",
                Dialect::Postgres,
                &["app_lock".to_string()],
                true
            ),
            None
        );
    }

    #[test]
    fn unflagged_function_is_safe() {
        assert_eq!(
            classify("SELECT app_lock(1)", Dialect::Postgres, &[], true),
            None
        );
    }

    // ---- panic-safety -------------------------------------------------------------------------

    #[test]
    fn panic_safety_multibyte_string_content_is_safe() {
        assert_eq!(
            classify(
                "SELECT 'café pg_advisory_lock'",
                Dialect::Postgres,
                &[],
                true
            ),
            None
        );
    }

    #[test]
    fn panic_safety_empty_input_is_none() {
        assert_eq!(classify("", Dialect::Postgres, &[], true), None);
    }

    #[test]
    fn panic_safety_unterminated_string_does_not_panic() {
        // No closing quote: the statement is malformed and won't execute, but classify() must
        // still return without panicking. Either `Some(Unknown)` or `None` is an acceptable
        // outcome per the brief -- only the no-panic property is load-bearing here. (Our rules
        // recognize the leading `SELECT` keyword regardless of what unterminated content follows
        // it, so this resolves deterministically to `None` via the safe-list.)
        assert_eq!(classify("SELECT '", Dialect::Postgres, &[], true), None);
    }

    // ---- MySQL / SQLite stubs (not wired to a live backend in S2; sanity-only) -----------------

    #[test]
    fn sqlite_attach_triggers_set() {
        assert_eq!(
            classify(
                "ATTACH DATABASE 'foo.db' AS foo",
                Dialect::Sqlite,
                &[],
                true
            ),
            Some(PinTrigger::Set)
        );
    }

    #[test]
    fn sqlite_pragma_triggers_set() {
        assert_eq!(
            classify("PRAGMA foreign_keys=ON", Dialect::Sqlite, &[], true),
            Some(PinTrigger::Set)
        );
    }

    #[test]
    fn sqlite_plain_select_is_safe() {
        assert_eq!(classify("SELECT 1", Dialect::Sqlite, &[], true), None);
    }

    #[test]
    fn mysql_set_session_triggers_set() {
        assert_eq!(
            classify("SET SESSION x = 1", Dialect::MySql, &[], true),
            Some(PinTrigger::Set)
        );
    }

    #[test]
    fn mysql_plain_select_is_safe() {
        assert_eq!(classify("SELECT 1", Dialect::MySql, &[], true), None);
    }
}
