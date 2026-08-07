//! **M1-S8a Task 6 — the LIVE schema-introspection gate on BOTH engine families.**
//!
//! Doctrine's `AbstractSchemaManager` (and `doctrine/migrations`) run catalog SQL on every
//! introspection. Before this slice that SQL failed on both families:
//!
//! * **PostgreSQL** — `pg_type.typname` is `name` (OID 19), `pg_attribute.attidentity` is `"char"`
//!   (OID 18), `pg_class.oid` is `oid` (OID 26) and `atttypid::regtype` is `regtype` (2206). All
//!   four were a loud `Unsupported`, so the whole `selectTableColumns` query was unreadable.
//! * **MySQL / MariaDB** — `information_schema.COLUMNS.COLUMN_KEY` and
//!   `referential_constraints.UPDATE_RULE` are `ENUM` columns on MySQL 8, and every user-declared
//!   `ENUM(...)` column reaches the client as `MYSQL_TYPE_STRING` carrying `ENUM_FLAG`, which the
//!   classifier rejected outright.
//!
//! Everything below maps onto tags that ALREADY exist (`TEXT` / `I64`) — this slice adds no
//! canonical tag and makes no `/proto` change.
//!
//! **The assertions are DERIVED, not hard-coded lists.** For every column a query returns, the tag
//! `HEAD` promised (`cols[i].tag`) must equal the tag the producer emitted (`rows[0][i].tag()`).
//! That is the hazard-7 lockstep proof driven by real catalog data rather than by a fixture, and it
//! grows automatically with the query.
//!
//! **What each engine's ENUM proof is driven through, and why (hazard 66).** MariaDB's
//! `information_schema.COLUMNS.COLUMN_KEY` is a `MYSQL_TYPE_VAR_STRING`, **not** an ENUM — it read
//! fine before this slice, so a MariaDB arm driven through `information_schema` would have been
//! GREEN BEFORE THE FIX and proved nothing there. The PRIMARY proof is therefore a **user-declared
//! `ENUM(...)` column**, which arrives as `MYSQL_TYPE_STRING | ENUM_FLAG` on BOTH engines. The
//! `information_schema` read stays as the real-DBAL-traffic smoke test.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test catalog_it -- --nocapture
//! ```
//!
//! Every test SKIPS (prints `skip: <VAR> unset`, never fails) when its engine's DSN env var is
//! unset, so `cargo test --workspace` stays green offline.

mod common;

use common::{
    assert_session_alive, exec_err, exec_ok, exec_server, mariadb_url, mysql_url, pg_url, req,
};
use ferro_proto::consts::errc;
use ferro_proto::messages::sql::{ExecOk, ExecRequest};
use ferro_proto::value::Value;
use ferrod::services::sql::FETCH_NONE;

/// A statement that returns no rows (DDL / INSERT): the base `req` with `readonly = false` and
/// `fetch = FETCH_NONE`.
fn ddl(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.fetch = FETCH_NONE;
    r
}

/// The set of `(label, dsn)` MySQL-family targets under test — MySQL 8 and/or MariaDB 11, whichever
/// env var is set. Empty → the caller SKIPS (offline). Both set → the scenario runs against both.
/// Copied from `mysql_it.rs` (each `tests/*.rs` is its own crate, so there is nothing to import).
fn mysql_targets() -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(u) = mysql_url() {
        out.push(("mysql", u));
    }
    if let Some(u) = mariadb_url() {
        out.push(("mariadb", u));
    }
    out
}

/// The hazard-7 lockstep proof on REAL catalog data: `HEAD` is built pre-execution from
/// `oid_to_tag` / `column_to_tag`, the cells are produced per-cell by `extract_value`, and the two
/// are separate matches over one table. Derived from the query under test — no parallel tag list.
///
/// Sweeps **every cell of every row**, not just row 0. A SQL `NULL` is `Value::Null` (tag 0) in
/// every column regardless of its declared type, so those cells carry no tag information and are
/// skipped — which is why the count of cells actually compared is returned and asserted by the
/// caller against a MEASURED expectation: without it, a query whose columns all went `NULL` would
/// satisfy this function vacuously.
#[must_use]
fn assert_head_matches_producer(label: &str, r: &ExecOk) -> usize {
    assert!(!r.rows.is_empty(), "[{label}] the query returned no rows");
    let mut checked = 0usize;
    for (ri, row) in r.rows.iter().enumerate() {
        assert_eq!(
            row.len(),
            r.cols.len(),
            "[{label}] row {ri} has {} cells for {} HEAD columns",
            row.len(),
            r.cols.len()
        );
        for (i, col) in r.cols.iter().enumerate() {
            if row[i] == Value::Null {
                continue;
            }
            assert_eq!(
                col.tag,
                row[i].tag(),
                "[{label}] row {ri}: HEAD promised tag {} for {:?} but the producer emitted {}",
                col.tag,
                col.name,
                row[i].tag()
            );
            checked += 1;
        }
    }
    checked
}

/// Every value of the column named `name`, across all rows.
fn column_values<'a>(r: &'a ExecOk, name: &str) -> Vec<&'a Value> {
    let idx = r
        .cols
        .iter()
        .position(|c| c.name == name)
        .unwrap_or_else(|| panic!("the result has no column named {name}"));
    r.rows.iter().map(|row| &row[idx]).collect()
}

/// **PG:** the catalog scalars Doctrine's `PostgreSQLSchemaManager` selects are readable end to end.
///
/// Two queries, deliberately:
///  1. an EXPLICIT one that names each newly-admitted type (`name`, `"char"`, `oid`, `regtype`,
///     `regclass`) so a regression points straight at the type that broke;
///  2. **DBAL 4.4.4's VERBATIM `selectTableColumns` statement** (transcribed from
///     `vendor/doctrine/dbal/src/Schema/PostgreSQLSchemaManager.php:358-410` with
///     `PostgreSQLPlatform::getDefaultColumnValueSQLSnippet()` and the table predicate substituted
///     in), which is the traffic this task exists to unblock. S8a takes NO composer dependency —
///     the statement is inlined here as a string.
#[tokio::test]
async fn pg_catalog_introspection_columns_are_readable() {
    let Some(url) = pg_url() else { return }; // prints `skip: FERRO_TEST_PG_URL unset`
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_cat")).await;
    exec_ok(
        &mut c,
        2,
        &ddl("CREATE TABLE s8a_cat (id int GENERATED ALWAYS AS IDENTITY, plain text)"),
    )
    .await;

    // (1) The newly-admitted catalog types, named one by one:
    //   t.typname     -> name    (OID 19)
    //   a.attidentity -> "char"  (OID 18)   -- '\0' on a NON-identity column
    //   c.oid         -> oid     (OID 26)
    //   a.atttypid    -> oid
    //   a.atttypid::regtype      (OID 2206)
    //   'pg_class'::regclass     (OID 2205)
    let r = exec_ok(
        &mut c,
        3,
        &req(
            "SELECT t.typname, a.attidentity, c.oid, a.atttypid, a.atttypid::regtype AS rt, \
             'pg_class'::regclass AS rc \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE c.relname = 's8a_cat' AND a.attnum > 0 \
             ORDER BY a.attnum",
        ),
    )
    .await;

    assert_eq!(r.cols.len(), 6);
    assert_eq!(
        assert_head_matches_producer("pg/explicit", &r),
        12,
        "6 columns x 2 rows, none of them NULL"
    );

    // `name` carries PG's own type names verbatim.
    let typnames = column_values(&r, "typname");
    assert!(
        typnames.contains(&&Value::Text("int4".to_string()))
            && typnames.contains(&&Value::Text("text".to_string())),
        "`name` (OID 19) must read as TEXT: {typnames:?}"
    );
    // The identity column reports 'a'; a plain one reports the EMPTY string, not "\0".
    let identity = column_values(&r, "attidentity");
    assert!(
        identity.contains(&&Value::Text("a".to_string())),
        "the IDENTITY column reports 'a': {identity:?}"
    );
    assert!(
        identity.contains(&&Value::Text(String::new())),
        "a plain column reports \"\" (PG's own text output for '\\0'): {identity:?}"
    );
    // `oid` and the `reg*` aliases report the NUMERIC oid — their binary payload IS a 4-byte oid.
    // `atttypid` is checked against PG's own well-known builtin numbers so the assertion pins a
    // VALUE, not merely a tag: int4 is 23, text is 25, and `regclass`'s own oid is 2205.
    let atttypid = column_values(&r, "atttypid");
    assert!(
        atttypid.contains(&&Value::I64(23)) && atttypid.contains(&&Value::I64(25)),
        "`oid` (OID 26) must read as the numeric oid (int4 = 23, text = 25): {atttypid:?}"
    );
    assert_eq!(
        column_values(&r, "rt"),
        atttypid,
        "`regtype`'s binary payload IS the oid (`regtypesend` is `oidsend`), so it must read \
         identically to the `oid` column it was cast from"
    );
    assert!(
        column_values(&r, "rc")
            .iter()
            .all(|v| **v == Value::I64(1259)),
        "`regclass` reports the numeric oid too — `pg_class`'s own oid is the builtin 1259"
    );

    // (2) DBAL 4.4.4's VERBATIM selectTableColumns statement. `t.typname` / `bt.typname` are
    // `name`, `a.attidentity` is `"char"`, and `coll.collprovider` (a `"char"`) is compared inside
    // the CASE — every one of them unreadable before this slice.
    let r = exec_ok(
        &mut c,
        4,
        &req(
            "SELECT quote_ident(n.nspname)               AS schema_name, \
                    quote_ident(c.relname)               AS table_name, \
                    quote_ident(a.attname)               AS field, \
                    t.typname                            AS type, \
                    format_type(a.atttypid, a.atttypmod) AS complete_type, \
                    bt.typname                           AS domain_type, \
                    format_type(bt.oid, t.typtypmod)     AS domain_complete_type, \
                    a.attnotnull                         AS isnotnull, \
                    a.attidentity, \
                    (SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef \
                      WHERE c.oid = pg_attrdef.adrelid AND pg_attrdef.adnum = a.attnum) AS \"default\", \
                    dsc.description                      AS comment, \
                    CASE WHEN coll.collprovider = 'c' THEN coll.collcollate \
                         WHEN coll.collprovider = 'd' THEN NULL \
                         ELSE coll.collname END          AS collation \
             FROM pg_attribute a \
               JOIN pg_class c ON c.oid = a.attrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               JOIN pg_type t ON t.oid = a.atttypid \
               LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype \
               LEFT JOIN pg_collation coll ON coll.oid = a.attcollation \
               LEFT JOIN pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e' \
                 AND dep.classid = (SELECT oid FROM pg_class WHERE relname = 'pg_class') \
               LEFT JOIN pg_description dsc ON dsc.objoid = c.oid AND dsc.objsubid = a.attnum \
               LEFT JOIN pg_inherits i ON i.inhrelid = c.oid \
               LEFT JOIN pg_class p ON i.inhparent = p.oid AND p.relkind = 'p' \
             WHERE c.relname = 's8a_cat' \
               AND c.relkind IN ('r', 'p') AND a.attnum > 0 \
               AND dep.refobjid IS NULL AND p.oid IS NULL \
             ORDER BY n.nspname, c.relname, a.attnum",
        ),
    )
    .await;

    assert_eq!(r.rows.len(), 2, "DBAL sees both columns of s8a_cat");
    assert_eq!(
        assert_head_matches_producer("pg/dbal-4.4.4-selectTableColumns", &r),
        14,
        "12 columns x 2 rows = 24 cells, of which 10 are legitimately NULL on this fixture \
         (domain_type, domain_complete_type, default, comment and collation, x2) — MEASURED, so a \
         column that silently started returning NULL would drop the count and fail here rather \
         than pass this sweep vacuously"
    );
    assert_eq!(
        column_values(&r, "type"),
        vec![
            &Value::Text("int4".to_string()),
            &Value::Text("text".to_string())
        ],
        "DBAL's `type` column IS pg_type.typname, a `name`"
    );
    assert_eq!(
        column_values(&r, "attidentity"),
        vec![&Value::Text("a".to_string()), &Value::Text(String::new())],
        "DBAL reads `attidentity == 'd'` for autoincrement; a NON-identity column must render as \
         the empty string, exactly as PG's own text output does — never \"\\0\""
    );

    // Exactly one END per request all the way through (charter rule 4).
    assert_session_alive(&mut c, 0xca7a).await;
}

/// **MySQL / MariaDB:** ENUM columns read as their label, and `SET` stays a loud `Unsupported`.
///
/// **The user-table column is the PRIMARY case, not the secondary one (hazard 66).** Driving the
/// whole proof through `information_schema.COLUMNS.COLUMN_KEY` would be GREEN BEFORE THE FIX on
/// MariaDB, where that column is a `MYSQL_TYPE_VAR_STRING` rather than an ENUM. A user-declared
/// `ENUM(...)` column arrives as `MYSQL_TYPE_STRING | ENUM_FLAG` on BOTH engines and is therefore
/// the assertion that can fail on both. The `information_schema` read stays as well, because it is
/// the actual DBAL traffic (`MySQLSchemaManager::selectTableColumns` selects `c.COLUMN_KEY`;
/// `selectForeignKeyColumns` selects `referential_constraints.UPDATE_RULE`/`DELETE_RULE`) — but it
/// is asserted as a smoke test of readability, not as the ENUM proof.
#[tokio::test]
async fn mysql_enum_columns_read_as_their_label() {
    for (label, url) in mysql_targets() {
        let server = exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        // (1) THE proof, on both engines: a user ENUM column.
        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_enum")).await;
        exec_ok(
            &mut c,
            2,
            &ddl("CREATE TABLE s8a_enum (id INT PRIMARY KEY, mood ENUM('sad','ok','happy'))"),
        )
        .await;
        exec_ok(&mut c, 3, &ddl("INSERT INTO s8a_enum VALUES (1, 'happy')")).await;

        let r = exec_ok(&mut c, 4, &req("SELECT mood FROM s8a_enum WHERE id = 1")).await;
        assert_eq!(
            r.rows[0][0],
            Value::Text("happy".to_string()),
            "[{label}] an ENUM cell's wire value IS its label string"
        );
        assert_eq!(
            assert_head_matches_producer(&format!("{label}/user-enum"), &r),
            1,
            "[{label}] the one ENUM cell must actually have been compared"
        );

        // (2) The real DBAL traffic still reads end to end. Derived HEAD-vs-producer assertion over
        // every column the query returns — no parallel tag table.
        exec_ok(&mut c, 5, &ddl("DROP TABLE IF EXISTS s8a_cat")).await;
        exec_ok(
            &mut c,
            6,
            &ddl("CREATE TABLE s8a_cat (id INT PRIMARY KEY, v INT)"),
        )
        .await;
        let r = exec_ok(
            &mut c,
            7,
            &req(
                "SELECT COLUMN_NAME, COLUMN_KEY, IS_NULLABLE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 's8a_cat' \
                 ORDER BY ORDINAL_POSITION",
            ),
        )
        .await;
        assert_eq!(
            assert_head_matches_producer(&format!("{label}/information_schema"), &r),
            6,
            "[{label}] 3 columns x 2 rows, none of them NULL"
        );
        assert_eq!(
            r.rows[0][1],
            Value::Text("PRI".to_string()),
            "[{label}] COLUMN_KEY must read as its label (an ENUM on MySQL, a VAR_STRING on \
             MariaDB — either way the value is the same and both must work)"
        );

        // (3) SET is still out of scope, and still LOUD — asserted on the OFFENDING-type head of
        // the message, not on the trailing "Deferred: …" list, which contains every deferred type
        // name and would make a bare `contains("SET")` unfalsifiable (hazard 65).
        exec_ok(&mut c, 8, &ddl("DROP TABLE IF EXISTS s8a_set")).await;
        exec_ok(&mut c, 9, &ddl("CREATE TABLE s8a_set (s SET('a','b'))")).await;
        let e = exec_err(&mut c, 10, &req("SELECT s FROM s8a_set")).await;
        assert_eq!(e.code, errc::UNSUPPORTED, "[{label}] SET stays Unsupported");
        assert!(
            e.message.contains("MySQL SET ("),
            "[{label}] the refusal must name SET as the offending type: {}",
            e.message
        );
        assert!(
            !e.message.contains("ENUM"),
            "[{label}] ENUM must be gone from the deferred list too: {}",
            e.message
        );

        // Exactly one END per request, refusals included (charter rule 4).
        assert_session_alive(&mut c, 0xca7b).await;
    }
}
