//! **M1-S8c: the live acceptance for the three bind widenings, against real PostgreSQL.**
//!
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset, mirroring `pg_types_it.rs`
//! so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro cargo test -p ferro-backend-pg
//! ```
//!
//! What only a live server can prove, and why each of these is here:
//!
//! 1. **`I64` → `bool`.** Stock `Doctrine\DBAL\Types\BooleanType::convertToDatabaseValue` returns
//!    `(int) $value`, and any DBAL call site without an explicit `ParameterType` sends that int
//!    under `ParameterType::STRING`, where the PHP type decides — so a PHP `int` lands in a PG
//!    `bool` slot. Measured here: `1`/`0` really do store `true`/`false` (an offline byte fixture
//!    can pin `[0x01]`/`[0x00]`, but only PG can say what `[0x01]` MEANS), and `2` is refused
//!    PRE-SEND — proven by a READ-BACK showing the table unchanged, not merely by the error type,
//!    because an error alone would not distinguish a pre-send refusal from a post-send failure.
//! 2. **`I64` → `text`.** `PostgreSQLPlatform::getDateArithmeticIntervalExpression` emits
//!    `(<date> + (? || ' SECOND')::interval)` verbatim, and PG resolves that parameter to `text`
//!    (measured: `PREPARE pa AS SELECT $1 || ' SECOND'` → `parameter_types = {text}`). The whole
//!    stock expression is executed here, not a paraphrase of it.
//! 3. **D-S8b-6's mirror: a canonical `Text` into an UNMAPPED / custom-OID slot.** A native enum, a
//!    COMPOSITE type, `hstore`, `ltree`, `citext`, `int2vector`, `oidvector`, `interval`, `inet`,
//!    `xml`, `int4[]` and `timetz` — five different `Kind`s, four of them (`Composite` included)
//!    only reachable through a real `typeinfo` lookup, which is why an offline `Type::new` fixture
//!    cannot stand in for this. Each value round-trips BYTE-IDENTICALLY through its column.
//! 4. **PG ITSELF IS THE ORACLE for every dangerous literal in every widened slot.** S7 measured a
//!    MySQL case where a permissive server SILENTLY COERCED a non-representable value instead of
//!    erroring, and that is the failure mode this pass went hunting for. So for the full cross
//!    product of PG's special input words × every widened slot, this compares Ferro's BOUND
//!    parameter against PG's OWN literal cast of the same text into the same type, and demands they
//!    agree exactly — same stored text, or the same SQLSTATE. "Exactly what libpq/`pdo_pgsql` would
//!    do" stops being a claim and becomes a measurement. The deliberate exceptions are the five
//!    S7/S8b-GATED slots (`date`, `time`, `timestamp`, `timestamptz`, `numeric`), where a BARE text
//!    sentinel is refused pre-send on purpose — asserted as a divergence rather than hidden.

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};
use ferro_proto::value::Value;

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config() -> PoolConfig {
    PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// One text cell, or the `PoolError`. Everything below reads back through an explicit `::text` cast
/// so the assertions do NOT depend on Task 1's read-side fallback for an unmapped OID landing first:
/// `text` is a canonical tag on both sides today, and PG's own output function is what renders it.
async fn text_cell(
    co: &mut Checkout<PgBackend>,
    sql: &str,
    params: &[Value],
) -> Result<Option<String>, PoolError> {
    let r = co.query(sql, params).await?;
    assert_eq!(r.rows.len(), 1, "`{sql}` must return one row");
    assert_eq!(r.cols.len(), 1, "`{sql}` must return one column");
    Ok(match &r.rows[0][0] {
        Value::Text(s) => Some(s.clone()),
        Value::Null => None,
        other => panic!("`{sql}` returned {other:?}, expected TEXT (the ::text cast)"),
    })
}

async fn count(co: &mut Checkout<PgBackend>, table: &str) -> i64 {
    let r = co
        .query(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("count");
    match r.rows[0][0] {
        Value::I64(n) => n,
        ref other => panic!("count returned {other:?}"),
    }
}

/// **(1) `I64` → `bool`: `1`/`0` store `true`/`false`, and NOTHING else binds.**
///
/// The `2` case is the one that matters. `n != 0` — C's rule, and PHP's — would make it `true`, a
/// silent corrupt write of a boolean column. PG's own parser draws the same line (`'2'::bool` is
/// `22P02`), so the refusal is the engine being EARLY and known-fate, never stricter than the
/// database it fronts. That claim is checked here against this very server, not asserted.
#[tokio::test(flavor = "multi_thread")]
async fn s8c_i64_binds_a_bool_column_as_true_and_false_and_refuses_anything_else() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config());
    let mut co = pool.checkout().await.expect("checkout");

    for stmt in [
        "DROP TABLE IF EXISTS s8c_bool",
        "DROP DOMAIN IF EXISTS s8c_dflag CASCADE",
        "CREATE DOMAIN s8c_dflag AS bool",
        "CREATE TABLE s8c_bool (id serial PRIMARY KEY, b bool, d s8c_dflag)",
    ] {
        co.exec(stmt)
            .await
            .unwrap_or_else(|e| panic!("setup `{stmt}`: {e:?}"));
    }

    // ---- 1 and 0 land as the booleans they denote — through a bare `bool` AND through a DOMAIN
    // over one, which is the only pair that shows `check_range`/`to_sql` were handed the RESOLVED
    // base rather than the domain.
    for (n, want) in [(1_i64, "true"), (0, "false")] {
        co.query(
            "INSERT INTO s8c_bool (b, d) VALUES ($1, $2)",
            &[Value::I64(n), Value::I64(n)],
        )
        .await
        .unwrap_or_else(|e| panic!("I64({n}) must bind a bool slot: {e:?}"));
        let got = text_cell(
            &mut co,
            "SELECT b::text || '/' || d::text FROM s8c_bool ORDER BY id DESC LIMIT 1",
            &[],
        )
        .await
        .expect("read back");
        assert_eq!(
            got.as_deref(),
            Some(format!("{want}/{want}").as_str()),
            "I64({n}) must store {want} in both the bare bool and the domain over bool"
        );
        // ...and it reads back as the canonical BOOL tag, so a read → write-back round trip closes.
        let r = co
            .query("SELECT b FROM s8c_bool ORDER BY id DESC LIMIT 1", &[])
            .await
            .expect("read back typed");
        assert_eq!(r.rows[0][0], Value::Bool(n == 1));
    }

    // ---- Everything else is a PRE-SEND refusal. Proven by the row count, which is what separates
    // "refused before the statement was sent" from "the server rejected it" — the §19.3 distinction.
    let before = count(&mut co, "s8c_bool").await;
    assert_eq!(before, 2);
    for n in [2_i64, -1, 255, i64::MAX] {
        let err = co
            .query("INSERT INTO s8c_bool (b) VALUES ($1)", &[Value::I64(n)])
            .await
            .expect_err(&format!("I64({n}) must not bind a bool slot"));
        match &err {
            PoolError::Sql {
                code,
                sqlstate,
                message,
                ..
            } => {
                assert_eq!(*code, ferro_proto::consts::errc::UNSUPPORTED);
                assert_eq!(
                    *sqlstate, None,
                    "no SQLSTATE: the server never saw the statement"
                );
                assert!(message.contains("only 0 and 1"), "{message}");
                assert!(message.contains(&n.to_string()), "{message}");
            }
            other => panic!("expected a known-fate Sql refusal for I64({n}), got {other:?}"),
        }
        println!("  [pg] I64({n:<20}) -> bool: pre-send refusal");
    }
    assert_eq!(
        count(&mut co, "s8c_bool").await,
        before,
        "a pre-send refusal must leave the table untouched"
    );
    // ...and through the DOMAIN too, so the value gate is not accidentally bare-type-only.
    co.query("INSERT INTO s8c_bool (d) VALUES ($1)", &[Value::I64(2)])
        .await
        .expect_err("I64(2) must not bind a domain over bool either");
    assert_eq!(count(&mut co, "s8c_bool").await, before);

    // ---- PG's OWN parser agrees about `2`, on this same server: the engine is not inventing a
    // stricter rule than the database it fronts. `22P02` is `invalid_text_representation`.
    let pg_own = co
        .exec("INSERT INTO s8c_bool (b) VALUES ('2')")
        .await
        .expect_err("PG itself must refuse the literal '2' for a bool");
    match &pg_own {
        PoolError::Sql { sqlstate, .. } => assert_eq!(
            sqlstate.as_deref(),
            Some("22P02"),
            "PG's own invalid-text-representation SQLSTATE: {pg_own:?}"
        ),
        other => panic!("expected PG's own 22P02, got {other:?}"),
    }
    println!("  [pg] PG's own '2'::bool -> 22P02, same line drawn in the same place");

    // ---- The canonical BOOL tag still binds, and a bare TEXT still does NOT reach a bool slot
    // (`'t'` is PG's input syntax, but the canonical wire form for a boolean is TAG_BOOL).
    co.query("INSERT INTO s8c_bool (b) VALUES ($1)", &[Value::Bool(true)])
        .await
        .expect("the canonical BOOL tag is untouched by this widening");
    co.query(
        "INSERT INTO s8c_bool (b) VALUES ($1)",
        &[Value::Text("t".into())],
    )
    .await
    .expect_err("a bare TEXT must not bind a bool slot");

    for stmt in [
        "DROP TABLE IF EXISTS s8c_bool",
        "DROP DOMAIN IF EXISTS s8c_dflag CASCADE",
    ] {
        co.exec(stmt).await.expect("cleanup");
    }
}

/// **(2) `I64` → `text`: the expression stock DBAL date arithmetic actually emits.**
///
/// Not a paraphrase — `PostgreSQLPlatform::getDateArithmeticIntervalExpression` builds
/// `'(' . $date . ' ' . $operator . ' (' . $interval . " || ' " . $unit->value . "')::interval)"`,
/// i.e. `(<date> + (? || ' SECOND')::interval)`, and that is the SQL run below with a canonical
/// `I64` in the `?`. PG resolves the parameter to `text`, so before S8c this whole family of stock
/// expressions was a pre-send refusal.
#[tokio::test(flavor = "multi_thread")]
async fn s8c_i64_binds_the_text_slot_stock_dbal_date_arithmetic_emits() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config());
    let mut co = pool.checkout().await.expect("checkout");

    // The minimal shape named by the measurement.
    let got = text_cell(&mut co, "SELECT $1 || ' SECOND'", &[Value::I64(90)])
        .await
        .expect("an I64 must bind the `text` slot `? || ' SECOND'` creates");
    assert_eq!(got.as_deref(), Some("90 SECOND"));

    // The whole stock expression, with a NEGATIVE and a full-width magnitude so a decimal renderer
    // that dropped the sign or truncated through i32 could not pass.
    let got = text_cell(
        &mut co,
        "SELECT (CAST('2026-08-05 00:00:00' AS timestamp) + ($1 || ' SECOND')::interval)::text",
        &[Value::I64(-3661)],
    )
    .await
    .expect("the stock PG date-arithmetic expression must run with an I64 parameter");
    assert_eq!(
        got.as_deref(),
        Some("2026-08-04 22:58:59"),
        "-3661 seconds is 1 h 1 min 1 s before midnight; a dropped sign or a truncated width \
         yields a different, plausible timestamp rather than an error"
    );

    // Every string type the widening admits, plus a DOMAIN over one — and the full i64 range.
    for (ty, n) in [
        ("text", i64::MAX),
        ("varchar(64)", i64::MIN),
        ("char(24)", -1_i64),
        ("name", 42),
    ] {
        let got = text_cell(
            &mut co,
            &format!("SELECT btrim(($1::{ty})::text)"),
            &[Value::I64(n)],
        )
        .await
        .unwrap_or_else(|e| panic!("an I64 must bind a {ty} slot: {e:?}"));
        assert_eq!(
            got.as_deref(),
            Some(n.to_string().as_str()),
            "an I64 in a {ty} slot is its exact decimal rendering"
        );
    }

    // A DOMAIN over text: the format branch must resolve too, or PG reads UTF-8 digits as a binary
    // payload for the domain while reading them as text for bare `text`.
    for stmt in [
        "DROP DOMAIN IF EXISTS s8c_dlabel CASCADE",
        "CREATE DOMAIN s8c_dlabel AS text",
    ] {
        co.exec(stmt).await.expect("setup");
    }
    let got = text_cell(
        &mut co,
        "SELECT ($1::s8c_dlabel)::text",
        &[Value::I64(-200)],
    )
    .await
    .expect("an I64 must bind a DOMAIN over text");
    assert_eq!(got.as_deref(), Some("-200"));
    co.exec("DROP DOMAIN IF EXISTS s8c_dlabel CASCADE")
        .await
        .expect("cleanup");

    // Still NARROW: an integer does not become bindable into a numeric or temporal slot.
    for ty in ["numeric", "date", "timestamp", "uuid"] {
        co.query(&format!("SELECT $1::{ty}"), &[Value::I64(1)])
            .await
            .expect_err(&format!("an I64 must not bind a {ty} slot"));
    }
}

/// The five S7/S8b-GATED slots: a BARE text sentinel is refused PRE-SEND for these on purpose, and
/// the caller says what it means with a canonical tag instead. Everything else is oracle-compared
/// against PG.
const GATED_SLOTS: [&str; 5] = ["date", "time", "timestamp", "timestamptz", "numeric"];

/// PG's own special input words, plus three shapes that are merely non-representable. The whole
/// point is that none of them may acquire a meaning, or lose one, by travelling as a bound
/// parameter rather than as a literal.
const DANGEROUS: [&str; 12] = [
    "infinity",
    "-infinity",
    "Infinity",
    "NaN",
    "now",
    "today",
    "epoch",
    "allballs",
    "24:00:00",
    "0000-00-00",
    "1e-46",
    "-0",
];

/// How one `(literal, slot)` pair came out — the same three-way classification for PG's own literal
/// cast and for Ferro's bound parameter, so they can be compared as values.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The value PG stored, rendered by PG's own output function.
    Stored(Option<String>),
    /// The server refused it, with this SQLSTATE.
    ServerRefused(String),
    /// Ferro refused it before the statement was sent (no SQLSTATE — the server never saw it).
    PreSendRefused(String),
}

impl Outcome {
    /// The three-way class, without the payload. Used for the four CLOCK-READING literals, whose
    /// value is by definition different at two different instants, so an exact comparison of two
    /// separately-executed probes would be a flapping test rather than a guard.
    fn class(&self) -> &'static str {
        match self {
            Outcome::Stored(_) => "stored",
            Outcome::ServerRefused(_) => "server-refused",
            Outcome::PreSendRefused(_) => "pre-send-refused",
        }
    }
}

/// PG's input words that read the CLOCK. Two probes run as two separate autocommit statements are
/// two separate transactions, so `now` legitimately differs between them by microseconds — measured:
/// `07:34:37.629076+00` vs `07:34:37.630345+00`. For these the oracle compares the CLASS across the
/// two probes and then compares the VALUES inside ONE statement (one transaction, one
/// `transaction_timestamp()`), which is exact and deterministic.
const CLOCK_READING: [&str; 4] = ["now", "today", "tomorrow", "yesterday"];

async fn outcome(
    co: &mut Checkout<PgBackend>,
    sql: &str,
    params: &[Value],
) -> Result<Outcome, PoolError> {
    match text_cell(co, sql, params).await {
        Ok(v) => Ok(Outcome::Stored(v)),
        Err(PoolError::Sql {
            sqlstate: Some(s), ..
        }) => Ok(Outcome::ServerRefused(s)),
        Err(PoolError::Sql {
            sqlstate: None,
            message,
            ..
        }) => Ok(Outcome::PreSendRefused(message)),
        Err(other) => Err(other),
    }
}

/// **(3) + (4): D-S8b-6's mirror, byte-identical, and PG as the oracle for every dangerous value.**
///
/// Two properties in one fixture because they need the same custom types, and creating an extension
/// twice concurrently in a shared database is a race this file has no reason to run.
///
/// The custom types deliberately span FIVE `Kind`s — `Enum`, `Composite`, `Simple` (hstore),
/// `Array` (int4[]) and `Domain` (over a custom base) — and `Composite` in particular is only
/// reachable through a real `typeinfo` lookup, so an offline `Type::new` fixture cannot stand in for
/// it. `ltree` and `citext` are the counter-fixtures: equally extension-assigned, but
/// `<&str as ToSql>::accepts` admits them BY NAME, so they must keep the DELEGATED binary path
/// (ltree's payload is `0x01 || text`) — if the fallback ever won for them, clause (3) of
/// `bind::tests::s8a_every_arm_treats_a_domain_exactly_as_its_base` would silently stop guarding.
#[tokio::test(flavor = "multi_thread")]
async fn s8c_text_binds_every_unmapped_or_custom_oid_slot_and_pg_is_the_oracle() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config());
    let mut co = pool.checkout().await.expect("checkout");

    // The extensions are created IF NOT EXISTS and never dropped: this database is shared with
    // other suites, and `DROP EXTENSION` would break them.
    for stmt in [
        "CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE EXTENSION IF NOT EXISTS ltree",
        "CREATE EXTENSION IF NOT EXISTS citext",
    ] {
        co.exec(stmt)
            .await
            .unwrap_or_else(|e| panic!("`{stmt}` (postgres-contrib must be available): {e:?}"));
    }
    for stmt in [
        "DROP TABLE IF EXISTS s8c_wide",
        "DROP DOMAIN IF EXISTS s8c_dmood CASCADE",
        "DROP TYPE IF EXISTS s8c_mood CASCADE",
        "DROP TYPE IF EXISTS s8c_comp CASCADE",
        "CREATE TYPE s8c_mood AS ENUM ('sad','ok','happy')",
        "CREATE TYPE s8c_comp AS (n int4, s text)",
        "CREATE DOMAIN s8c_dmood AS s8c_mood",
        "CREATE TABLE s8c_wide (e s8c_mood, dm s8c_dmood, c s8c_comp, h hstore, l ltree, \
         ci citext, iv int2vector, ov oidvector, itv interval, ip inet, x xml, ia int4[], \
         ttz timetz)",
    ] {
        co.exec(stmt)
            .await
            .unwrap_or_else(|e| panic!("setup `{stmt}`: {e:?}"));
    }

    // ---- (3) ROUND-TRIP BYTE IDENTITY. Every payload below was verified against PG's own output
    // function before being written down (`SELECT ('<v>')::<type>::text`), so the expectation is
    // PG's rendering, not a guess about it — and every one is byte-identical to the input, which is
    // what makes "read as text → written back as text" a closed loop.
    //
    // `money` is deliberately absent: its output is locale-formatted (`$1.23` under
    // `lc_monetary = en_US.utf8`), so byte identity is not the right property for it. It is covered
    // by the PG-as-oracle sweep below, where PG's own rendering is the reference.
    let cells: &[(&str, &str)] = &[
        ("e", "happy"),
        ("dm", "ok"),
        ("c", "(1,hello)"),
        ("h", "\"k\"=>\"v\""),
        ("l", "a.b.c"),
        ("ci", "MiXeD"),
        ("iv", "1 2 3"),
        ("ov", "1 2 3"),
        ("itv", "1 day 02:03:04"),
        ("ip", "192.168.0.1/24"),
        ("x", "<a>1</a>"),
        ("ia", "{1,2,3}"),
        ("ttz", "12:34:56+02"),
    ];
    let cols = cells.iter().map(|(c, _)| *c).collect::<Vec<_>>().join(", ");
    let holes = (1..=cells.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let params: Vec<Value> = cells
        .iter()
        .map(|(_, v)| Value::Text((*v).to_string()))
        .collect();
    co.query(
        &format!("INSERT INTO s8c_wide ({cols}) VALUES ({holes})"),
        &params,
    )
    .await
    .expect(
        "a canonical TEXT must bind EVERY unmapped/custom-OID slot — D-S8b-6 reads them back as \
         TAG_TEXT, so they must be writable as TEXT",
    );
    for (col, sent) in cells {
        let got = text_cell(&mut co, &format!("SELECT {col}::text FROM s8c_wide"), &[])
            .await
            .unwrap_or_else(|e| panic!("read back {col}: {e:?}"));
        assert_eq!(
            got.as_deref(),
            Some(*sent),
            "column {col} must round-trip BYTE-IDENTICALLY: PG's own output function has to hand \
             back exactly the text the bind sent, or the read → write-back loop is one-way"
        );
        println!("  [pg] {col:<4} <- {sent:<20} -> {got:?}  byte-identical");
    }

    // ...and the pre-S8c behaviour is genuinely what changed: an integer still cannot reach any of
    // these slots (the fallback is a TEXT-tag door, not a universal one), and the mapped types are
    // still owned by their canonical tags.
    co.query("INSERT INTO s8c_wide (iv) VALUES ($1)", &[Value::I64(1)])
        .await
        .expect_err("an I64 must not bind an int2vector slot");
    co.query(
        "INSERT INTO s8c_wide (ia) VALUES ($1)",
        &[Value::Date("2026-08-05".into())],
    )
    .await
    .expect_err("a canonical DATE must not bind an int4[] slot");

    // ---- (4) PG IS THE ORACLE. For every dangerous literal × every widened slot: what Ferro's
    // BOUND parameter does must equal what PG's OWN literal cast of the same text into the same
    // type does. Identical stored text, or the identical SQLSTATE. A silent coercion — the failure
    // S7 measured on MySQL — shows up as a Stored(..) on one side and a refusal on the other, or as
    // two different Stored values.
    let widened: [&str; 15] = [
        "s8c_mood",
        "s8c_dmood",
        "s8c_comp",
        "hstore",
        "ltree",
        "citext",
        "int2vector",
        "oidvector",
        "interval",
        "inet",
        "xml",
        "money",
        "int4[]",
        "timetz",
        "text",
    ];
    let mut agreements = 0usize;
    let mut divergences = 0usize;
    let mut infinite_intervals = 0usize;
    for lit in DANGEROUS {
        for slot in widened {
            // PG's own answer: a LITERAL cast, no Ferro bind anywhere in it.
            let pg = outcome(&mut co, &format!("SELECT (('{lit}')::{slot})::text"), &[])
                .await
                .expect("the literal probe must not fail at the transport level");
            // Ferro's answer: the SAME text, as a BOUND parameter, into the SAME slot.
            let ferro = outcome(
                &mut co,
                &format!("SELECT ($1::{slot})::text"),
                &[Value::Text(lit.to_string())],
            )
            .await
            .expect("the bound probe must not fail at the transport level");
            if CLOCK_READING.contains(&lit) {
                // Same CLASS across the two probes...
                assert_eq!(
                    pg.class(),
                    ferro.class(),
                    "SILENT DIVERGENCE for the clock literal '{lit}' into {slot}: PG's own literal \
                     cast says {pg:?} but Ferro's bound parameter says {ferro:?}"
                );
                // ...and, when both store, the same VALUE — compared inside ONE statement so both
                // sides read the same clock, and with the literal wrapped in a scalar sub-query so
                // it is NOT constant-folded at PLAN time. Measured without the wrapper: a folded
                // `('now')::timetz` is evaluated when the statement is planned and the bound `$1`
                // when it is executed, which under the extended protocol is a separate round trip —
                // `07:34:37.629076+00` vs `07:34:37.630345+00`, a 1.2 ms difference that is PG's
                // evaluation timing and nothing to do with the bind under test.
                if matches!(pg, Outcome::Stored(_)) {
                    let same = co
                        .query(
                            &format!(
                                "SELECT (((SELECT '{lit}'::text))::{slot})::text IS NOT DISTINCT \
                                 FROM ($1::{slot})::text"
                            ),
                            &[Value::Text(lit.to_string())],
                        )
                        .await
                        .unwrap_or_else(|e| {
                            panic!("same-statement probe for '{lit}'/{slot}: {e:?}")
                        });
                    assert_eq!(
                        same.rows[0][0],
                        Value::Bool(true),
                        "SILENT DIVERGENCE for the clock literal '{lit}' into {slot}: within ONE \
                         transaction PG's own literal cast and Ferro's bound parameter must produce \
                         the identical value"
                    );
                }
            } else {
                assert_eq!(
                    pg, ferro,
                    "SILENT DIVERGENCE for '{lit}' into {slot}: PG's own literal cast says {pg:?} \
                     but Ferro's bound parameter says {ferro:?}. A widened bind must do exactly \
                     what libpq/pdo_pgsql do — anything else is either a coercion we invented or a \
                     refusal PG does not make"
                );
            }
            assert!(
                !matches!(ferro, Outcome::PreSendRefused(_)),
                "no widened slot has a pre-send gate: '{lit}' into {slot} gave {ferro:?}"
            );
            if slot == "interval"
                && matches!(&pg, Outcome::Stored(Some(s)) if s.contains("infinity"))
            {
                infinite_intervals += 1;
            }
            agreements += 1;
            println!("  [oracle] {lit:<12} -> {slot:<11} {pg:?}");
        }
    }
    // The PG-17 finding this pass went looking for, pinned so it cannot regress silently in either
    // direction: `interval` really does have infinite values, so a bare `Text("infinity")` in an
    // `interval` slot MEANS infinity. It is not gated, because `interval` has no canonical tag and
    // TEXT is therefore its only route — refusing would make the value unwritable and break the
    // very round trip D-S8b-6 mandates. It is recorded, not hidden.
    assert!(
        infinite_intervals >= 2,
        "PG 17 must accept both infinite interval sentinels; got {infinite_intervals}"
    );

    // ---- The five GATED slots are the DELIBERATE divergence, asserted as such rather than omitted.
    // A bare text sentinel is refused PRE-SEND there — no SQLSTATE, because the server never saw it
    // — while PG's own literal cast happily produces the sentinel. That is S7/S8b policy: a caller
    // who MEANS a sentinel says so with a canonical tag.
    for lit in ["infinity", "-infinity", "NaN", "now", "today"] {
        for slot in GATED_SLOTS {
            let pg = outcome(&mut co, &format!("SELECT (('{lit}')::{slot})::text"), &[])
                .await
                .expect("literal probe");
            let ferro = outcome(
                &mut co,
                &format!("SELECT ($1::{slot})::text"),
                &[Value::Text(lit.to_string())],
            )
            .await
            .expect("bound probe");
            if matches!(pg, Outcome::Stored(_)) {
                match &ferro {
                    Outcome::PreSendRefused(m) => {
                        assert!(m.contains("SPECIAL"), "{m}");
                        divergences += 1;
                        println!(
                            "  [gated]  {lit:<12} -> {slot:<11} PG {pg:?} / Ferro pre-send refusal"
                        );
                    }
                    other => panic!(
                        "'{lit}' into the GATED slot {slot} must be a PRE-SEND refusal (PG says \
                         {pg:?}), got {other:?} — the S7/S8b sentinel discipline has regressed"
                    ),
                }
                // ...and the TAGGED route binds the same sentinel deliberately, so nothing became
                // unwritable.
                if slot == "date" {
                    let tagged = text_cell(
                        &mut co,
                        "SELECT ($1::date)::text",
                        &[Value::Date(lit.to_string())],
                    )
                    .await;
                    if lit == "infinity" || lit == "-infinity" {
                        assert_eq!(
                            tagged.expect("a tagged sentinel still binds").as_deref(),
                            Some(lit)
                        );
                    }
                }
            }
        }
    }
    assert!(
        divergences >= 5,
        "the gated set must actually have fired: {divergences}"
    );
    println!(
        "  s8c oracle sweep: {agreements} bound-vs-literal agreements across the widened slots, \
         {divergences} deliberate pre-send divergences in the gated slots"
    );

    for stmt in [
        "DROP TABLE IF EXISTS s8c_wide",
        "DROP DOMAIN IF EXISTS s8c_dmood CASCADE",
        "DROP TYPE IF EXISTS s8c_mood CASCADE",
        "DROP TYPE IF EXISTS s8c_comp CASCADE",
    ] {
        co.exec(stmt).await.expect("cleanup");
    }
}
