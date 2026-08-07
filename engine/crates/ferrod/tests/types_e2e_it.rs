//! **M1-S7 Task 9 — THE SLICE ACCEPTANCE GATE.** The whole canonical-type path, end to end,
//! through the real daemon: client → `ferrod` session → `AnyPool` → live Postgres / MySQL 8 /
//! MariaDB 11 → back → client, for **every tag the registry declares implemented**, in **both**
//! directions (produced by a read, consumed by a bind).
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferrod --test types_e2e_it -- --nocapture
//! ```
//!
//! Every test SKIPS (never fails) when its engine's DSN env var is unset, so
//! `cargo test --workspace` stays green offline — the same discipline as `sql_exec_it.rs`.
//!
//! ## What this file proves that the backend-level suites cannot
//!
//! 1. **The per-tag assertion list is DERIVED from `/proto`, not hand-written.**
//!    [`implemented_tags`] reads `Registry::from_toml_dir(...).implemented` — the *same* list that
//!    feeds `registry.lock.json` and therefore `TYPE_REGISTRY_HASH`. If a tag is declared
//!    implemented and no live case exercises it, this suite FAILS. A hand-written parallel list is
//!    exactly how the dead `m0_scalar` key, the hardcoded vector count and the `RawBytes` text scan
//!    each went silently stale; the hash must never be able to claim coverage that does not exist
//!    (carry C5).
//! 2. **Both DIRECTIONS per tag.** A tag is only counted covered on an engine when it is both
//!    PRODUCED (a real column read back through the daemon) and CONSUMED (bound back as a param
//!    through `ExecCodec`'s wire shape). Read-only support would leave every DBAL write broken.
//! 3. **Read → re-bind → read is BYTE-STABLE** (F13). Phase B binds the *exact* values phase A
//!    read — no re-rendering, no reparsing — and asserts the second row reads back identical. This
//!    is the only shape that catches a naive/instant swap: a `TIMESTAMP` re-bound as `TIMESTAMPTZ`
//!    shifts by the session offset with no error anywhere.
//! 4. **The documented engine asymmetries are ASSERTED, not assumed** — and a *stale* exclusion
//!    fails too: if an engine ever starts producing a tag listed in [`read_exclusions`], the test
//!    fails and forces §22.2 to be updated.
//! 5. **The deferrals are still loud through the daemon** — a `NonRetryable{Unsupported}` terminal
//!    naming the column and its native type, never a silent miscast (charter rule 6).

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;

use common::{
    TestClient, assert_session_alive, exec_err, exec_ok, exec_server, mariadb_url, mysql_url,
    pg_url, req,
};
use ferro_proto::consts::{branch, errc, tag};
use ferro_proto::registry::Registry;
use ferro_proto::value::Value;

// -------------------------------------------------------------------------------------------------
// The registry-DERIVED required set (the whole point of this file).
// -------------------------------------------------------------------------------------------------

fn proto_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

/// `(name, tag byte)` for every tag `/proto/types.toml` DECLARES implemented — the single source of
/// truth that also feeds `registry.lock.json` and `TYPE_REGISTRY_HASH`.
///
/// Deliberately NOT a hand-written list: the whole failure mode this slice kept re-discovering is a
/// parallel list drifting away from the registry while every test stays green.
fn implemented_tags() -> Vec<(String, u8)> {
    let reg = Registry::from_toml_dir(&proto_dir());
    reg.implemented
        .iter()
        .map(|name| {
            let t = *reg.tags.get(name).unwrap_or_else(|| {
                panic!("implemented tag {name} has no entry in the [tags] table of types.toml")
            });
            (name.clone(), t)
        })
        .collect()
}

/// Which live cases exercised which tags, per direction.
#[derive(Default)]
struct Coverage {
    /// tags a real column PRODUCED (engine → wire → client).
    read: BTreeSet<u8>,
    /// tags a real param CONSUMED (client → wire → engine → DB).
    bound: BTreeSet<u8>,
}

impl Coverage {
    fn saw_read(&mut self, v: &Value) {
        self.read.insert(v.tag());
    }
    fn saw_bound(&mut self, v: &Value) {
        self.bound.insert(v.tag());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Engine {
    Pg,
    Mysql,
    MariaDb,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Pg => "pg",
            Engine::Mysql => "mysql",
            Engine::MariaDb => "mariadb",
        }
    }
}

/// Tags an engine legitimately never PRODUCES, each a §22.2-recorded asymmetry with its reason.
///
/// An exclusion is not a hole in the proof: the tag must still be covered by *some* engine (the
/// union check below), and if the engine ever starts producing it this list becomes stale and the
/// test FAILS — so the exclusions cannot quietly grow into a coverage escape hatch.
fn read_exclusions(e: Engine) -> &'static [(u8, &'static str)] {
    match e {
        Engine::Pg => &[(
            tag::U64,
            "PostgreSQL has no unsigned integer type — nothing can produce U64 (§22.2 (e))",
        )],
        Engine::Mysql => &[(
            tag::UUID,
            "MySQL 8 has no native UUID type: BINARY(16) stays BYTES and CHAR(36) stays TEXT \
             (hazard 25, §22.2)",
        )],
        Engine::MariaDb => &[
            (
                tag::UUID,
                "MariaDB's native UUID reaches the wire as MYSQL_TYPE_STRING/utf8mb4 — \
                 indistinguishable from CHAR(36) without extended metadata (§22.2)",
            ),
            (
                tag::JSON,
                "MariaDB JSON is LONGTEXT + a json_valid() CHECK with no recoverable metadata, so \
                 it classifies as TEXT by design — promoting it would be a silent miscast \
                 (charter rule 6, §22.2)",
            ),
        ],
    }
}

/// Tags an engine legitimately never CONSUMES as a bind param.
fn bind_exclusions(e: Engine) -> &'static [(u8, &'static str)] {
    match e {
        Engine::Pg => &[(
            tag::U64,
            "PgU64Text::accepts is false for EVERY PG type — a deliberate known-fate rejection, \
             since no PG type holds the top half of the u64 range (§22.2 (e))",
        )],
        Engine::Mysql | Engine::MariaDb => &[],
    }
}

fn excluded<'a>(list: &'a [(u8, &'static str)], t: u8) -> Option<&'a str> {
    list.iter().find(|(x, _)| *x == t).map(|(_, why)| *why)
}

// -------------------------------------------------------------------------------------------------
// One matrix column: what the engine must PRODUCE for it, and how the produced value is bound back.
// -------------------------------------------------------------------------------------------------

struct Case {
    col: &'static str,
    /// the tag `ColMeta` must promise for this column.
    want_tag: u8,
    /// the exact canonical value expected, when it is server-INDEPENDENT (`None` for a document
    /// the server itself normalizes, e.g. a `jsonb` / MySQL `JSON` value).
    want: Option<Value>,
    /// the canonical tag the phase-A value is RE-TAGGED to before it is bound back — set only where
    /// the engine cannot itself produce the tag but must still consume it (UUID/JSON on the MySQL
    /// family). `None` re-binds the value exactly as it was read.
    rebind_as: Option<u8>,
}

/// The value read for a named column. By NAME, never by index: an index into a 14-column matrix is
/// exactly the kind of off-by-one that reads as a type bug in the failure message.
fn value_of<'a>(cases: &[Case], read: &'a [Value], col: &str) -> &'a Value {
    let i = cases
        .iter()
        .position(|c| c.col == col)
        .unwrap_or_else(|| panic!("no case for column {col}"));
    &read[i]
}

/// Re-tag a canonical TEXT payload as `t` — the phase-B bind for a column whose engine cannot
/// produce the tag (a MySQL `CHAR(36)` holding a UUID, a MariaDB `LONGTEXT` holding JSON).
fn retag(v: &Value, t: u8) -> Value {
    if v.tag() == t {
        return v.clone(); // already the target tag (MySQL 8's JSON column) — nothing to re-tag
    }
    let s = match v {
        Value::Text(s) => s.clone(),
        other => panic!("retag expects a TEXT payload, got {other:?}"),
    };
    match t {
        x if x == tag::UUID => Value::Uuid(s),
        x if x == tag::JSON => Value::Json(s),
        other => panic!("retag has no rule for tag {other}"),
    }
}

fn pg_cases() -> Vec<Case> {
    let c = |col, want_tag, want| Case {
        col,
        want_tag,
        want,
        rebind_as: None,
    };
    vec![
        c("c_bool", tag::BOOL, Some(Value::Bool(true))),
        // 2^53+1 — a value a JSON number could not carry losslessly, so this is a real i64 path.
        c("c_i64", tag::I64, Some(Value::I64(9_007_199_254_740_993))),
        c("c_f64", tag::F64, Some(Value::F64(1.5))),
        c("c_text", tag::TEXT, Some(Value::Text("héllo".into()))),
        c(
            "c_bytes",
            tag::BYTES,
            Some(Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])),
        ),
        // Trailing zeros SURVIVE: the display scale is part of the payload (1.10 != 1.1).
        c(
            "c_decimal",
            tag::DECIMAL,
            Some(Value::Decimal("-12345.6700000000".into())),
        ),
        c("c_date", tag::DATE, Some(Value::Date("2026-08-05".into()))),
        c(
            "c_time",
            tag::TIME,
            Some(Value::Time("13:45:07.250000".into())),
        ),
        c(
            "c_ts",
            tag::TIMESTAMP,
            Some(Value::Timestamp("2026-08-05 13:45:07.250000".into())),
        ),
        // Inserted as `...13:45:07.25+02` — the INSTANT, normalized to UTC, is 11:45:07.25Z.
        c(
            "c_tstz",
            tag::TIMESTAMPTZ,
            Some(Value::TimestampTz("2026-08-05T11:45:07.250000Z".into())),
        ),
        // Inserted MIXED CASE; the canonical payload is lowercase.
        c(
            "c_uuid",
            tag::UUID,
            Some(Value::Uuid("a1b2c3d4-0000-4fff-8000-abcdefabcdef".into())),
        ),
        // jsonb re-renders the document (key order + spacing), so the server is the oracle.
        c("c_json", tag::JSON, None),
        // A NULL cell: ColMeta still promises the COLUMN's type (text), the value is Null.
        c("c_null", tag::TEXT, Some(Value::Null)),
    ]
}

fn mysql_cases(engine: Engine) -> Vec<Case> {
    let c = |col, want_tag, want| Case {
        col,
        want_tag,
        want,
        rebind_as: None,
    };
    vec![
        c("c_bool", tag::BOOL, Some(Value::Bool(true))),
        c("c_i64", tag::I64, Some(Value::I64(9_007_199_254_740_993))),
        c("c_f64", tag::F64, Some(Value::F64(1.5))),
        c("c_text", tag::TEXT, Some(Value::Text("héllo".into()))),
        c(
            "c_bytes",
            tag::BYTES,
            Some(Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])),
        ),
        // The top of the unsigned range — unrepresentable as an i64, the entire reason U64 exists.
        c("c_u64", tag::U64, Some(Value::U64(u64::MAX))),
        c(
            "c_decimal",
            tag::DECIMAL,
            Some(Value::Decimal("-12345.6700000000".into())),
        ),
        c("c_date", tag::DATE, Some(Value::Date("2026-08-05".into()))),
        c(
            "c_time",
            tag::TIME,
            Some(Value::Time("13:45:07.250000".into())),
        ),
        // DATETIME is NAIVE -> TIMESTAMP(10); TIMESTAMP is a UTC instant -> TIMESTAMPTZ(11), which
        // is truthful only because every Ferro MySQL session is pinned to time_zone='+00:00'.
        c(
            "c_ts",
            tag::TIMESTAMP,
            Some(Value::Timestamp("2026-08-05 13:45:07.250000".into())),
        ),
        c(
            "c_tstz",
            tag::TIMESTAMPTZ,
            Some(Value::TimestampTz("2026-08-05T13:45:07.250000Z".into())),
        ),
        // CHAR(36): TEXT on the way out (no native UUID type), re-bound as the UUID tag so the
        // CONSUME direction is genuinely exercised on this engine too.
        Case {
            col: "c_uuid",
            want_tag: tag::TEXT,
            want: Some(Value::Text("a1b2c3d4-0000-4fff-8000-abcdefabcdef".into())),
            rebind_as: Some(tag::UUID),
        },
        // MySQL 8 emits MYSQL_TYPE_JSON -> JSON; MariaDB's JSON is a LONGTEXT alias -> TEXT.
        Case {
            col: "c_json",
            want_tag: if engine == Engine::MariaDb {
                tag::TEXT
            } else {
                tag::JSON
            },
            want: None,
            rebind_as: Some(tag::JSON),
        },
        c("c_null", tag::TEXT, Some(Value::Null)),
    ]
}

// -------------------------------------------------------------------------------------------------
// Small daemon helpers.
// -------------------------------------------------------------------------------------------------

/// Run a one-column `SELECT` and return `(head tag, value)`, asserting the HEAD-vs-producer
/// agreement on the way through (the cols gate runs pre-execution, the cell gate per row).
async fn probe(client: &mut TestClient, rid: u32, sql: &str) -> (u8, Value) {
    let ok = exec_ok(client, rid, &req(sql)).await;
    assert_eq!(ok.cols.len(), 1, "`{sql}` must have exactly one column");
    assert_eq!(ok.rows.len(), 1, "`{sql}` must have exactly one row");
    let v = ok.rows[0][0].clone();
    if v != Value::Null {
        assert_eq!(
            ok.cols[0].tag,
            v.tag(),
            "HEAD promised tag {} for `{sql}` but the producer emitted {}",
            ok.cols[0].tag,
            v.tag()
        );
    }
    (ok.cols[0].tag, v)
}

/// Run a statement that returns no rows (DDL / INSERT), asserting an `Ok` terminal.
async fn ddl(client: &mut TestClient, rid: u32, sql: &str) {
    let mut r = req(sql);
    r.readonly = false;
    r.fetch = 1; // none
    let _ = exec_ok(client, rid, &r).await;
}

/// Read every `cols` of the fixture row `id`, in declaration order.
async fn read_row(
    client: &mut TestClient,
    rid: u32,
    table: &str,
    cases: &[Case],
    id: i64,
) -> Vec<Value> {
    let list = cases.iter().map(|c| c.col).collect::<Vec<_>>().join(", ");
    let ok = exec_ok(
        client,
        rid,
        &req(&format!("SELECT {list} FROM {table} WHERE id = {id}")),
    )
    .await;
    assert_eq!(ok.rows.len(), 1, "{table} row {id} must exist");
    for (i, case) in cases.iter().enumerate() {
        assert_eq!(
            ok.cols[i].tag, case.want_tag,
            "{table}.{} : HEAD promised tag {} but the case requires {}",
            case.col, ok.cols[i].tag, case.want_tag
        );
        let v = &ok.rows[0][i];
        if *v != Value::Null {
            assert_eq!(
                ok.cols[i].tag,
                v.tag(),
                "{table}.{} : HEAD-vs-producer disagreement (cols say {}, the cell is {})",
                case.col,
                ok.cols[i].tag,
                v.tag()
            );
        }
    }
    ok.rows[0].clone()
}

/// Phase A → phase B: bind the values just read straight back into a second row and read THAT back,
/// asserting byte-identical canonical text. Records both directions into `cov`.
async fn rebind_row(
    client: &mut TestClient,
    rid: &mut u32,
    table: &str,
    cases: &[Case],
    read: &[Value],
    cov: &mut Coverage,
    label: &str,
) {
    let cols = cases.iter().map(|c| c.col).collect::<Vec<_>>().join(", ");
    let holes = std::iter::repeat_n("?", cases.len() + 1)
        .collect::<Vec<_>>()
        .join(", ");
    let mut params = vec![Value::I64(2)];
    for (i, case) in cases.iter().enumerate() {
        let bound = match case.rebind_as {
            Some(t) => retag(&read[i], t),
            None => read[i].clone(),
        };
        cov.saw_bound(&bound);
        params.push(bound);
    }

    let mut insert = req(&format!(
        "INSERT INTO {table} (id, {cols}) VALUES ({holes})"
    ));
    insert.readonly = false;
    insert.fetch = 1;
    insert.params = params;
    *rid += 1;
    let _ = exec_ok(client, *rid, &insert).await;

    *rid += 1;
    let back = read_row(client, *rid, table, cases, 2).await;
    for (i, case) in cases.iter().enumerate() {
        assert_eq!(
            back[i], read[i],
            "[{label}] {table}.{}: read -> re-bind -> read is NOT byte-stable (read {:?}, got {:?} \
             back). A naive/instant swap or a re-rendering step is the usual cause.",
            case.col, read[i], back[i]
        );
    }
}

// -------------------------------------------------------------------------------------------------
// THE GATE: every implemented tag, both directions, on every configured engine.
// -------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn every_implemented_tag_round_trips_through_the_daemon_on_every_engine() {
    let mut per_engine: Vec<(Engine, Coverage)> = Vec::new();
    if let Some(url) = pg_url() {
        per_engine.push((Engine::Pg, pg_matrix(url).await));
    }
    if let Some(url) = mysql_url() {
        per_engine.push((Engine::Mysql, mysql_matrix(Engine::Mysql, url).await));
    }
    if let Some(url) = mariadb_url() {
        per_engine.push((Engine::MariaDb, mysql_matrix(Engine::MariaDb, url).await));
    }
    if per_engine.is_empty() {
        return; // fully offline: skip, never fail
    }

    let implemented = implemented_tags();
    assert!(
        !implemented.is_empty(),
        "the registry declares NO implemented tags — the derived assertion list would be vacuous"
    );

    // ---- per engine: every implemented tag is produced AND consumed, or documented-excluded.
    for (engine, cov) in &per_engine {
        for (name, t) in &implemented {
            match excluded(read_exclusions(*engine), *t) {
                Some(why) => assert!(
                    !cov.read.contains(t),
                    "[{}] tag {name} IS produced live, but it is listed in read_exclusions ({why}) \
                     — the exclusion is STALE: delete it and update SPEC §22.2",
                    engine.label()
                ),
                None => assert!(
                    cov.read.contains(t),
                    "[{}] no live end-to-end case READS implemented tag {name} ({t}). The required \
                     set is DERIVED from /proto/types.toml `implemented` (which feeds \
                     TYPE_REGISTRY_HASH), so either add a live case or stop declaring the tag \
                     implemented — the hash must never claim coverage that does not exist.",
                    engine.label()
                ),
            }
            match excluded(bind_exclusions(*engine), *t) {
                Some(why) => assert!(
                    !cov.bound.contains(t),
                    "[{}] tag {name} IS bound live, but it is listed in bind_exclusions ({why}) — \
                     the exclusion is STALE: delete it and update SPEC §22.2",
                    engine.label()
                ),
                None => assert!(
                    cov.bound.contains(t),
                    "[{}] no live end-to-end case BINDS implemented tag {name} ({t}) — read-only \
                     support leaves every DBAL write broken (hazard 31)",
                    engine.label()
                ),
            }
        }
        println!(
            "  [{}] live coverage: read {:?}, bound {:?}",
            engine.label(),
            cov.read,
            cov.bound
        );
    }

    // ---- the union: an excluded tag must still be covered SOMEWHERE, or the exclusion is a hole.
    if per_engine.len() < 3 {
        eprintln!(
            "skip: the cross-engine UNION check needs all three DSNs (pg + mysql + mariadb); \
             {} configured",
            per_engine.len()
        );
        return;
    }
    let read_union: BTreeSet<u8> = per_engine
        .iter()
        .flat_map(|(_, c)| c.read.iter().copied())
        .collect();
    let bound_union: BTreeSet<u8> = per_engine
        .iter()
        .flat_map(|(_, c)| c.bound.iter().copied())
        .collect();
    for (name, t) in &implemented {
        assert!(
            read_union.contains(t),
            "NO engine produces implemented tag {name} ({t}) — the registry's `implemented` list \
             (and therefore TYPE_REGISTRY_HASH) claims coverage that does not exist (carry C5)"
        );
        assert!(
            bound_union.contains(t),
            "NO engine consumes implemented tag {name} ({t}) as a bind param (carry C5)"
        );
    }
    println!(
        "  UNION over pg+mysql+mariadb: {} implemented tags, all produced AND consumed live",
        implemented.len()
    );
}

// -------------------------------------------------------------------------------------------------
// Postgres.
// -------------------------------------------------------------------------------------------------

async fn pg_matrix(url: String) -> Coverage {
    const T: &str = "ferro_s7_e2e_pg";
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;
    let mut cov = Coverage::default();
    let mut rid = 100u32;

    rid += 1;
    ddl(&mut client, rid, &format!("DROP TABLE IF EXISTS {T}")).await;
    rid += 1;
    ddl(
        &mut client,
        rid,
        &format!(
            "CREATE TABLE {T} (
               id int8 PRIMARY KEY, c_bool bool, c_i64 int8, c_f64 float8, c_text text,
               c_bytes bytea, c_decimal numeric, c_date date, c_time time,
               c_ts timestamp, c_tstz timestamptz, c_uuid uuid, c_json jsonb, c_null text)"
        ),
    )
    .await;
    rid += 1;
    ddl(
        &mut client,
        rid,
        &format!(
            "INSERT INTO {T} VALUES (1, true, 9007199254740993, 1.5, 'héllo', '\\xdeadbeef'::bytea,
               '-12345.6700000000'::numeric, DATE '2026-08-05', TIME '13:45:07.250000',
               TIMESTAMP '2026-08-05 13:45:07.250000',
               TIMESTAMPTZ '2026-08-05 13:45:07.25+02',
               'A1B2C3D4-0000-4FFF-8000-ABCDEFABCDEF'::uuid,
               '{{\"b\": 1, \"a\": [1, 2], \"n\": \"héllo\"}}'::jsonb, NULL)"
        ),
    )
    .await;

    // ---- Phase A: PRODUCE.
    let cases = pg_cases();
    rid += 1;
    let read = read_row(&mut client, rid, T, &cases, 1).await;
    for (i, case) in cases.iter().enumerate() {
        if let Some(want) = &case.want {
            assert_eq!(
                &read[i], want,
                "[pg] {T}.{}: canonical payload mismatch",
                case.col
            );
        }
        cov.saw_read(&read[i]);
    }
    // The one server-normalized payload: the oracle is PG's own renderer, not our literal.
    let json = match value_of(&cases, &read, "c_json") {
        Value::Json(s) => s.clone(),
        other => panic!("[pg] c_json must be a JSON tag, got {other:?}"),
    };
    rid += 1;
    let (_, server_json) = probe(
        &mut client,
        rid,
        &format!("SELECT c_json::text AS c FROM {T} WHERE id = 1"),
    )
    .await;
    assert_eq!(
        Value::Text(json.clone()),
        server_json,
        "[pg] the JSON payload must equal PG's own ::text rendering of the same jsonb value"
    );
    assert!(
        json.contains("héllo") && json.contains('['),
        "[pg] the JSON document must survive nesting + non-ASCII verbatim: {json}"
    );

    // ---- Phase B: CONSUME (read -> re-bind -> read, byte-stable).
    rebind_row(&mut client, &mut rid, T, &cases, &read, &mut cov, "pg").await;

    // ---- Precision-critical scalar probes (all PRODUCE-direction, recorded into coverage).
    for (sql, want) in [
        // NaN is a legal NUMERIC payload — it has no numeric-type representation at all.
        ("SELECT 'NaN'::numeric AS c", Value::Decimal("NaN".into())),
        (
            "SELECT '-Infinity'::numeric AS c",
            Value::Decimal("-Infinity".into()),
        ),
        // A 200-digit integer: far beyond any fixed-width decimal type's mantissa.
        (
            "SELECT repeat('9', 200)::numeric AS c",
            Value::Decimal("9".repeat(200)),
        ),
        // PG's legal 24:00:00 — chrono's NaiveTime would WRAP this to 00:00:00.
        (
            "SELECT time '24:00:00' AS c",
            Value::Time("24:00:00".into()),
        ),
        // A whole second emits NO fraction group (PROTOCOL.md §3.2).
        (
            "SELECT timestamp '2026-08-05 13:45:07' AS c",
            Value::Timestamp("2026-08-05 13:45:07".into()),
        ),
        // The ±infinity sentinels are literal payloads, never a parsed date.
        (
            "SELECT 'infinity'::date AS c",
            Value::Date("infinity".into()),
        ),
        (
            "SELECT '-infinity'::timestamptz AS c",
            Value::TimestampTz("-infinity".into()),
        ),
    ] {
        rid += 1;
        let (_, got) = probe(&mut client, rid, sql).await;
        assert_eq!(got, want, "[pg] `{sql}`");
        cov.saw_read(&got);
    }

    assert_session_alive(&mut client, 7).await;
    println!("  [pg] matrix + probes OK ({} columns)", cases.len());
    cov
}

// -------------------------------------------------------------------------------------------------
// MySQL 8 / MariaDB 11.
// -------------------------------------------------------------------------------------------------

async fn mysql_matrix(engine: Engine, url: String) -> Coverage {
    let t = format!("ferro_s7_e2e_{}", engine.label());
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;
    let mut cov = Coverage::default();
    let mut rid = 200u32;

    // The UTC session pin, observed THROUGH the daemon (M1-S7 Task 5a). Every canonical
    // TIMESTAMPTZ below is truthful only because this holds on the pooled connection.
    rid += 1;
    let (_, tz) = probe(&mut client, rid, "SELECT @@session.time_zone AS c").await;
    assert_eq!(
        tz,
        Value::Text("+00:00".into()),
        "[{}] every Ferro MySQL session must be pinned to time_zone='+00:00' — without it a \
         `timestamp` column reads differently per pooled connection",
        engine.label()
    );

    rid += 1;
    ddl(&mut client, rid, &format!("DROP TABLE IF EXISTS {t}")).await;
    rid += 1;
    ddl(
        &mut client,
        rid,
        &format!(
            "CREATE TABLE {t} (
               id BIGINT PRIMARY KEY, c_bool TINYINT(1), c_i64 BIGINT, c_f64 DOUBLE,
               c_text VARCHAR(64), c_bytes VARBINARY(64), c_u64 BIGINT UNSIGNED,
               c_decimal DECIMAL(30,10), c_date DATE, c_time TIME(6), c_ts DATETIME(6),
               c_tstz TIMESTAMP(6) NULL, c_uuid CHAR(36), c_json JSON, c_null VARCHAR(8))
             DEFAULT CHARSET=utf8mb4"
        ),
    )
    .await;
    rid += 1;
    ddl(
        &mut client,
        rid,
        &format!(
            "INSERT INTO {t} VALUES (1, 1, 9007199254740993, 1.5, 'héllo', X'DEADBEEF',
               18446744073709551615, '-12345.6700000000', '2026-08-05', '13:45:07.250000',
               '2026-08-05 13:45:07.250000', '2026-08-05 13:45:07.250000',
               'a1b2c3d4-0000-4fff-8000-abcdefabcdef',
               '{{\"b\": 1, \"a\": [1, 2], \"n\": \"héllo\"}}', NULL)"
        ),
    )
    .await;

    // ---- Phase A: PRODUCE.
    let cases = mysql_cases(engine);
    rid += 1;
    let read = read_row(&mut client, rid, &t, &cases, 1).await;
    for (i, case) in cases.iter().enumerate() {
        if let Some(want) = &case.want {
            assert_eq!(
                &read[i],
                want,
                "[{}] {t}.{}: canonical payload mismatch",
                engine.label(),
                case.col
            );
        }
        cov.saw_read(&read[i]);
    }
    // MySQL 8 normalizes a stored JSON document, so the server is the oracle either way.
    let json = match value_of(&cases, &read, "c_json") {
        Value::Json(s) | Value::Text(s) => s.clone(),
        other => panic!("[{}] c_json: unexpected {other:?}", engine.label()),
    };
    rid += 1;
    let (_, server_json) = probe(
        &mut client,
        rid,
        &format!("SELECT CAST(c_json AS CHAR) AS c FROM {t} WHERE id = 1"),
    )
    .await;
    assert_eq!(
        Value::Text(json.clone()),
        server_json,
        "[{}] the JSON payload must equal the server's own CAST(c_json AS CHAR)",
        engine.label()
    );
    assert!(
        json.contains("héllo") && json.contains('['),
        "[{}] the JSON document must survive nesting + non-ASCII verbatim: {json}",
        engine.label()
    );

    // ---- Phase B: CONSUME (read -> re-bind -> read, byte-stable).
    rebind_row(
        &mut client,
        &mut rid,
        &t,
        &cases,
        &read,
        &mut cov,
        engine.label(),
    )
    .await;

    // ---- U64 across the WHOLE range (F11). A BIGINT UNSIGNED <= i64::MAX arrives as
    // MyValue::Int, NOT MyValue::UInt — a UInt-only extractor breaks every ORDINARY row while a
    // suite testing only u64::MAX stays green.
    for (i, n) in [0u64, 5, 4_294_967_296, u64::MAX].into_iter().enumerate() {
        let id = 10 + i as i64;
        let mut ins = req(&format!("INSERT INTO {t} (id, c_u64) VALUES (?, ?)"));
        ins.readonly = false;
        ins.fetch = 1;
        ins.params = vec![Value::I64(id), Value::U64(n)];
        rid += 1;
        let _ = exec_ok(&mut client, rid, &ins).await;
        cov.saw_bound(&Value::U64(n));

        rid += 1;
        let (head, got) = probe(
            &mut client,
            rid,
            &format!("SELECT c_u64 AS c FROM {t} WHERE id = {id}"),
        )
        .await;
        assert_eq!(
            head,
            tag::U64,
            "[{}] BIGINT UNSIGNED -> U64",
            engine.label()
        );
        assert_eq!(
            got,
            Value::U64(n),
            "[{}] U64 {n} did not survive bind -> DB -> read",
            engine.label()
        );
        cov.saw_read(&got);
    }

    // ---- MySQL-family scalar probes.
    for (sql, want) in [
        // A MySQL TIME spans +/-838h and may be NEGATIVE — a plain time-of-day type cannot hold it.
        (
            "SELECT CAST('-100:20:30' AS TIME) AS c",
            Value::Time("-100:20:30".into()),
        ),
        (
            "SELECT CAST('838:59:59' AS TIME) AS c",
            Value::Time("838:59:59".into()),
        ),
        // A whole second emits NO fraction group.
        (
            "SELECT CAST('2026-08-05 13:45:07' AS DATETIME) AS c",
            Value::Timestamp("2026-08-05 13:45:07".into()),
        ),
    ] {
        rid += 1;
        let (_, got) = probe(&mut client, rid, sql).await;
        assert_eq!(got, want, "[{}] `{sql}`", engine.label());
        cov.saw_read(&got);
    }

    assert_session_alive(&mut client, 7).await;
    println!(
        "  [{}] matrix + U64 range + probes OK ({} columns)",
        engine.label(),
        cases.len()
    );
    cov
}

// -------------------------------------------------------------------------------------------------
// The deferrals are STILL LOUD through the daemon (charter rule 6 — never a silent miscast).
// -------------------------------------------------------------------------------------------------

/// Assert an EXEC terminates as a `NonRetryable{Unsupported}` naming the column and its type.
async fn assert_unsupported(client: &mut TestClient, rid: u32, sql: &str, col: &str, native: &str) {
    let ep = exec_err(client, rid, &req(sql)).await;
    assert_eq!(
        ep.code,
        errc::UNSUPPORTED,
        "`{sql}` must be a loud Unsupported, got {ep:?}"
    );
    assert_eq!(ep.branch, branch::NON_RETRYABLE, "`{sql}` branch");
    assert!(
        ep.message.contains(col),
        "`{sql}`: the message must NAME the column ({col}): {}",
        ep.message
    );
    assert!(
        ep.message.contains(native),
        "`{sql}`: the message must name the native type ({native}): {}",
        ep.message
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_deferrals_are_still_loud_through_the_daemon() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;
    let mut rid = 300u32;

    // A per-database custom OID: an enum and a domain must be real objects to be tested at all.
    for stmt in [
        "DROP TYPE IF EXISTS ferro_s7_e2e_mood CASCADE",
        "CREATE TYPE ferro_s7_e2e_mood AS ENUM ('ok', 'bad')",
        "DROP DOMAIN IF EXISTS ferro_s7_e2e_ttz CASCADE",
        "CREATE DOMAIN ferro_s7_e2e_ttz AS timetz",
        "DROP DOMAIN IF EXISTS ferro_s7_e2e_num CASCADE",
        "CREATE DOMAIN ferro_s7_e2e_num AS numeric(10,2)",
    ] {
        rid += 1;
        ddl(&mut client, rid, stmt).await;
    }

    for (sql, col, native) in [
        (
            "SELECT '1 day'::interval AS c_interval",
            "c_interval",
            "interval",
        ),
        ("SELECT '127.0.0.1'::inet AS c_inet", "c_inet", "inet"),
        ("SELECT ARRAY[1,2]::int4[] AS c_array", "c_array", "_int4"),
        // timetz must NEVER fall into the TIME arm: its payload is 12 bytes (i64 us + i32 zone),
        // so admitting it would fail mid-decode, after HEAD is already on the wire.
        (
            "SELECT '12:00:00+02'::timetz AS c_timetz",
            "c_timetz",
            "timetz",
        ),
        (
            "SELECT 'ok'::ferro_s7_e2e_mood AS c_enum",
            "c_enum",
            "ferro_s7_e2e_mood",
        ),
        // A DOMAIN over an UNSUPPORTED base: PG reports the BASE type (timetz), so this is refused
        // by the base OID — the domain itself never reaches the wire.
        (
            "SELECT '12:00:00+02'::ferro_s7_e2e_ttz AS c_domain",
            "c_domain",
            "timetz",
        ),
    ] {
        rid += 1;
        assert_unsupported(&mut client, rid, sql, col, native).await;
    }

    assert_session_alive(&mut client, 7).await;
    println!("  [pg] interval/inet/array/timetz/enum/domain-over-timetz still loud");
}

/// **A PG DOMAIN now READS *and* BINDS** — SPEC §22.2 (g) closed, live through the daemon.
///
/// **This test is a REGRESSION LOCK with a flipped expectation, not a new test** (M1-S8a Task 5).
/// It was `pg_domain_reads_but_does_not_bind`, and it pinned the asymmetry: PG resolves a domain to
/// its BASE type when it builds the `RowDescription`, so a domain over a supported base always read
/// fine (which is why domains are NOT on the deferral list), but it does **not** do that for
/// parameters — `stmt.params()` reports the DOMAIN's own OID, and `bind::accepts` matched on `Type`
/// IDENTITY, so binding the very value just read back into the same column was refused.
///
/// `bind::resolve_domain` now unwraps the domain — in `accepts` **and** inside every concrete
/// `ToSql` the value boxes as, which is the load-bearing half: `postgres-types` has zero
/// `Kind::Domain` handling of its own, so resolving only in the pre-flight would have made it
/// LOOSER than the impl it fronts and turned a type mismatch into a false `Indeterminate` (§19.3).
/// The refusal branch this test used to assert is kept below for the UNSUPPORTED base (`timetz`),
/// where a domain must still be a clean pre-send rejection: the unwrap widens nothing.
///
/// Deliberately its own server/session: a query against a CUSTOM OID makes `tokio-postgres` cache
/// an internal typeinfo statement on that physical connection, and the S3 full hygiene profile
/// (`DISCARD ALL` ⊃ `DEALLOCATE ALL`) deallocates it, so a *second* custom-OID query on a recycled
/// tainted connection fails with SQLSTATE 26000 ("prepared statement ... does not exist"). That is
/// a pre-existing engine/driver interaction, not a type-coverage fact; it is reported out of this
/// task rather than papered over, and this test avoids it by not sharing a pool with the enum /
/// domain-over-timetz probes above.
#[tokio::test(flavor = "multi_thread")]
async fn pg_domain_reads_and_binds() {
    let Some(url) = pg_url() else {
        return;
    };
    let server = exec_server(url);
    let mut client = server.connect().await;
    client.hello(1).await;
    let mut rid = 500u32;

    for stmt in [
        "DROP TABLE IF EXISTS ferro_s7_e2e_dom",
        "DROP DOMAIN IF EXISTS ferro_s7_e2e_num2 CASCADE",
        "CREATE DOMAIN ferro_s7_e2e_num2 AS numeric(10,2)",
        "CREATE TABLE ferro_s7_e2e_dom (v ferro_s7_e2e_num2)",
        "INSERT INTO ferro_s7_e2e_dom (v) VALUES ('12.34')",
    ] {
        rid += 1;
        ddl(&mut client, rid, stmt).await;
    }

    // READ: PG reports the BASE type in the RowDescription -> a supported base is supported.
    rid += 1;
    let (head, v) = probe(&mut client, rid, "SELECT v FROM ferro_s7_e2e_dom").await;
    assert_eq!(head, tag::DECIMAL, "a domain over numeric(10,2) -> DECIMAL");
    assert_eq!(v, Value::Decimal("12.34".into()));

    // BIND: the same value, back into the same column. M1-S8a: this now SUCCEEDS — the asymmetry
    // §22.2 (g) recorded is closed.
    let mut ins = req("INSERT INTO ferro_s7_e2e_dom (v) VALUES (?)");
    ins.readonly = false;
    ins.fetch = 1;
    ins.params = vec![v.clone()];
    rid += 1;
    let ok = exec_ok(&mut client, rid, &ins).await;
    assert_eq!(
        ok.affected, 1,
        "a value read from a domain column must bind straight back into it"
    );

    // ...and it landed as the SAME value, not a re-rendered one: two rows, both `12.34`.
    rid += 1;
    let read_back = exec_ok(
        &mut client,
        rid,
        &req("SELECT v FROM ferro_s7_e2e_dom ORDER BY v"),
    )
    .await;
    assert_eq!(
        read_back.rows,
        vec![
            vec![Value::Decimal("12.34".into())],
            vec![Value::Decimal("12.34".into())]
        ],
        "read -> bind -> read through a DOMAIN column must be byte-identical"
    );

    // The unwrap WIDENS NOTHING: a domain over a base Ferro does not support is still a clean,
    // pre-send, known-fate refusal — never a fate-unknown Indeterminate. This is the branch this
    // test used to assert for EVERY domain; keeping it here is what stops the fix from having
    // quietly turned the pre-flight into a rubber stamp.
    rid += 1;
    ddl(
        &mut client,
        rid,
        "CREATE DOMAIN ferro_s8a_e2e_ttz2 AS timetz",
    )
    .await;
    rid += 1;
    ddl(
        &mut client,
        rid,
        "CREATE TABLE ferro_s8a_e2e_ttz_t (c ferro_s8a_e2e_ttz2)",
    )
    .await;
    let mut bad = req("INSERT INTO ferro_s8a_e2e_ttz_t (c) VALUES (?)");
    bad.readonly = false;
    bad.fetch = 1;
    bad.params = vec![Value::Time("12:00:00".into())];
    rid += 1;
    let ep = exec_err(&mut client, rid, &bad).await;
    assert_eq!(
        ep.code,
        errc::UNSUPPORTED,
        "a domain over an UNSUPPORTED base is still a KNOWN-FATE rejection, got {ep:?}"
    );
    assert_ne!(
        ep.branch,
        branch::INDETERMINATE,
        "a pre-send bind rejection must never mint a false Indeterminate (§19.3)"
    );
    assert!(
        ep.message.contains("ferro_s8a_e2e_ttz2") && ep.message.contains("timetz"),
        "the rejection must name BOTH the domain and the base its constraint came from: {}",
        ep.message
    );

    // The connection is clean and the session alive: nothing was sent, nothing to unwind.
    assert_session_alive(&mut client, 7).await;
    println!("  [pg] domain READS {v:?} and now BINDS it back (§22.2 (g) closed)");
    println!(
        "  [pg] domain over an unsupported base is still loud: {}",
        ep.message
    );

    for stmt in [
        "DROP TABLE IF EXISTS ferro_s8a_e2e_ttz_t",
        "DROP DOMAIN IF EXISTS ferro_s8a_e2e_ttz2 CASCADE",
    ] {
        rid += 1;
        ddl(&mut client, rid, stmt).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_family_deferrals_are_still_loud_through_the_daemon() {
    let mut targets: Vec<(Engine, String)> = Vec::new();
    if let Some(u) = mysql_url() {
        targets.push((Engine::Mysql, u));
    }
    if let Some(u) = mariadb_url() {
        targets.push((Engine::MariaDb, u));
    }
    if targets.is_empty() {
        return;
    }

    for (engine, url) in targets {
        let t = format!("ferro_s7_defer_{}", engine.label());
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(1).await;
        let mut rid = 400u32;

        rid += 1;
        ddl(&mut client, rid, &format!("DROP TABLE IF EXISTS {t}")).await;
        rid += 1;
        ddl(
            &mut client,
            rid,
            &format!(
                "CREATE TABLE {t} (c_year YEAR, c_bit BIT(8), c_enum ENUM('a','b'), \
                 c_set SET('a','b'))"
            ),
        )
        .await;
        rid += 1;
        ddl(
            &mut client,
            rid,
            &format!("INSERT INTO {t} VALUES (2026, b'10101010', 'a', 'a,b')"),
        )
        .await;

        // M1-S8a (§22.2 (q)): `c_enum` left this loop — an ENUM now READS as its label. The
        // coverage MOVED to the positive assertion right after it, on the same live table.
        for (col, native) in [("c_year", "YEAR"), ("c_bit", "BIT"), ("c_set", "SET")] {
            rid += 1;
            assert_unsupported(
                &mut client,
                rid,
                &format!("SELECT {col} FROM {t}"),
                col,
                native,
            )
            .await;
        }

        // M1-S8a: the ENUM column is ADMITTED, end to end through the daemon, on BOTH engines —
        // its binary-protocol value IS the label string, so carrying it as TEXT is lossless.
        rid += 1;
        let (head, v) = probe(&mut client, rid, &format!("SELECT c_enum FROM {t}")).await;
        assert_eq!(
            head,
            tag::TEXT,
            "[{}] an ENUM column classifies as TEXT since M1-S8a (§22.2 (q))",
            engine.label()
        );
        assert_eq!(v, Value::Text("a".into()));

        // MariaDB's native UUID: measured, NOT Unsupported. It reaches the wire as
        // MYSQL_TYPE_STRING/utf8mb4 — byte-identical to a CHAR(36) — so it classifies TEXT by
        // design (§22.2). Promoting it would be the silent miscast charter rule 6 forbids; the
        // substantive rule that DOES hold is that tag UUID is never emitted by either engine.
        if engine == Engine::MariaDb {
            let u = "ferro_s7_maria_uuid";
            rid += 1;
            ddl(&mut client, rid, &format!("DROP TABLE IF EXISTS {u}")).await;
            rid += 1;
            ddl(&mut client, rid, &format!("CREATE TABLE {u} (c_uuid UUID)")).await;
            rid += 1;
            ddl(
                &mut client,
                rid,
                &format!("INSERT INTO {u} VALUES ('a1b2c3d4-0000-4fff-8000-abcdefabcdef')"),
            )
            .await;
            rid += 1;
            let (head, v) = probe(&mut client, rid, &format!("SELECT c_uuid FROM {u}")).await;
            assert_eq!(
                head,
                tag::TEXT,
                "[mariadb] a native UUID column reaches the wire as MYSQL_TYPE_STRING/utf8mb4 and \
                 classifies TEXT by design (§22.2) — tag UUID is never emitted by a MySQL-family \
                 backend"
            );
            assert_eq!(
                v,
                Value::Text("a1b2c3d4-0000-4fff-8000-abcdefabcdef".into())
            );
        }

        assert_session_alive(&mut client, 7).await;
        println!(
            "  [{}] YEAR/BIT/SET still loud; ENUM reads as TEXT",
            engine.label()
        );
    }
}

// -------------------------------------------------------------------------------------------------
// Non-vacuity: the derived list is real, and it is what the hash is built from.
// -------------------------------------------------------------------------------------------------

/// The derived assertion list must actually be derived — a regression that turned
/// [`implemented_tags`] into a hardcoded list would make every coverage assertion above a
/// tautology, which is precisely the drift class this slice eliminated four times.
#[test]
fn the_required_tag_list_is_derived_from_the_proto_registry() {
    let reg = Registry::from_toml_dir(&proto_dir());
    let derived = implemented_tags();
    assert_eq!(
        derived.len(),
        reg.implemented.len(),
        "implemented_tags() must mirror /proto/types.toml `implemented` exactly"
    );
    for (name, t) in &derived {
        assert!(reg.implemented.contains(name));
        assert_eq!(reg.tags[name], *t);
    }
    // The deferred tags must NOT be in the required set (they stay a loud Unsupported).
    for deferred in ["ARRAY", "INTERVAL", "INET", "VECTOR"] {
        assert!(
            !derived.iter().any(|(n, _)| n == deferred),
            "{deferred} is deferred and must not be required to round trip"
        );
    }
    // Every documented per-engine exclusion must name a tag that IS implemented — a stale
    // exclusion naming a dropped tag would silently weaken the matrix.
    for engine in [Engine::Pg, Engine::Mysql, Engine::MariaDb] {
        for (t, _) in read_exclusions(engine)
            .iter()
            .chain(bind_exclusions(engine))
        {
            assert!(
                derived.iter().any(|(_, x)| x == t),
                "[{}] exclusion names tag {t}, which is not in the implemented set",
                engine.label()
            );
        }
    }
}
