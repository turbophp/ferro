//! Live per-type round trips for the M1-S7 canonical tags (Task 4b) against a real Postgres.
//! Every test SKIPS (does not fail) when `FERRO_TEST_PG_URL` is unset — mirrors `pg_query_it.rs`
//! so `cargo test --workspace` stays green offline.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro cargo test -p ferro-backend-pg
//! ```
//!
//! What this file proves that the `pgtext`/`rowmap` unit tests cannot:
//!
//! 1. **The two gates agree on real cells (hazard 18/45).** `oid_to_tag` runs at cols-build,
//!    pre-execution; `extract_value` runs per-cell, MID-STREAM, after `HEAD` is already on the
//!    wire. `ALL_KNOWN_OIDS` does not exist and a set-equality test would be a tautology (both are
//!    matches over the one `oid_extract_type` table), so the assertion that actually bites is
//!    HEAD-vs-producer: `cols[i].tag == rows[0][i].tag()` on a real row, for **both** the buffered
//!    (`query.rs` `run`) and the streaming (`query.rs` `stream`) code paths — each has its own
//!    copy of the pair.
//! 2. **`TIMESTAMP` and `TIMESTAMPTZ` are not swapped.** They share an IDENTICAL 8-byte payload;
//!    only the column OID separates naive-local from UTC-instant, so a swap is a silent zone shift
//!    with no error anywhere. Every test here runs under a deliberately **non-UTC** session
//!    `TimeZone`, so a "the server just happens to be UTC" false green is impossible, and the
//!    instant is cross-checked against PG's own renderer.
//! 3. **The NUMERIC decoder is not self-referential (F23).** `pgtext`'s `num_bytes` helper is
//!    written by the decoder's own author. Here the oracle is PG: `v::text` in the same query for
//!    values PG can store, and — for wire shapes PG never *emits* but does *accept*, notably
//!    `dscale` truncation — a crafted binary payload pushed in through `COPY ... (FORMAT binary)`
//!    and rendered back by PG itself.
//! 4. **The deferrals are still refused, live.** `timetz` in particular must never fall into the
//!    `TIME` arm: its payload is 12 bytes (i64 µs + i32 zone), so admitting it would fail
//!    mid-decode, after `HEAD` is on the wire.

use std::time::Duration;

use bytes::Bytes;
use ferro_backend_pg::{PgBackend, pgtext};
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};
use ferro_proto::consts::tag;
use ferro_proto::value::Value;
use futures_util::{SinkExt, pin_mut};

/// Deliberately NOT UTC (and NOT a fixed offset — it observes DST), so every `timestamptz`
/// assertion below would fail if the decoder rendered the session-local wall clock instead of the
/// UTC instant. On 2026-08-05 New York is UTC-04:00.
const SESSION_TZ: &str = "America/New_York";

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// Pins the session to [`SESSION_TZ`] and PROVES it took. Without the proof a server that happened
/// to be UTC would make every `timestamptz` assertion pass for the wrong reason.
async fn pin_non_utc_session_zone(co: &mut Checkout<PgBackend>) {
    co.exec(&format!("SET TIME ZONE '{SESSION_TZ}'"))
        .await
        .expect("set a non-UTC session TimeZone");
    let r = co
        .query("SELECT current_setting('TimeZone')", &[])
        .await
        .expect("read back TimeZone");
    assert_eq!(
        r.rows[0][0],
        Value::Text(SESSION_TZ.to_string()),
        "the session zone must be genuinely non-UTC, or every timestamptz assertion below is a \
         false green"
    );
}

/// Runs `SELECT <expr>` and returns `(head_tag, value)`, asserting the HEAD-vs-producer agreement
/// (hazard 18) for every non-NULL cell on the way through.
async fn one(co: &mut Checkout<PgBackend>, expr: &str) -> (u8, Value) {
    let r = co
        .query(&format!("SELECT {expr}"), &[])
        .await
        .unwrap_or_else(|e| panic!("SELECT {expr} failed: {e:?}"));
    assert_eq!(r.cols.len(), 1, "SELECT {expr} must have one column");
    assert_eq!(r.rows.len(), 1, "SELECT {expr} must have one row");
    let v = r.rows[0][0].clone();
    if v != Value::Null {
        assert_eq!(
            r.cols[0].tag,
            v.tag(),
            "HEAD promised tag {} for `{expr}` but the producer emitted {} — the cols-build gate \
             and the per-cell gate disagree",
            r.cols[0].tag,
            v.tag()
        );
    }
    (r.cols[0].tag, v)
}

/// THE MATRIX: every M1-S7 canonical tag, read back as EXACT canonical text (`PROTOCOL.md` §3.2),
/// under a non-UTC session zone.
#[tokio::test(flavor = "multi_thread")]
async fn types_round_trip_exact_canonical_text() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    pin_non_utc_session_zone(&mut co).await;

    let cases: &[(&str, u8, Value)] = &[
        // ---- DECIMAL (tag 5). Display scale is PRESERVED: 1.10 and 1.1 are distinct payloads.
        (
            "'-12345.6700000000'::numeric(30,10)",
            tag::DECIMAL,
            Value::Decimal("-12345.6700000000".into()),
        ),
        (
            "'1.10'::numeric",
            tag::DECIMAL,
            Value::Decimal("1.10".into()),
        ),
        ("'1.1'::numeric", tag::DECIMAL, Value::Decimal("1.1".into())),
        ("'0'::numeric", tag::DECIMAL, Value::Decimal("0".into())),
        (
            "'0.00001'::numeric",
            tag::DECIMAL,
            Value::Decimal("0.00001".into()),
        ),
        ("'NaN'::numeric", tag::DECIMAL, Value::Decimal("NaN".into())),
        (
            "'Infinity'::numeric",
            tag::DECIMAL,
            Value::Decimal("Infinity".into()),
        ),
        (
            "'-Infinity'::numeric",
            tag::DECIMAL,
            Value::Decimal("-Infinity".into()),
        ),
        // ---- DATE (tag 8). The PG epoch is 2000-01-01, not 1970-01-01 — a Unix-epoch assumption
        // yields a plausible WRONG date rather than a crash, so both anchors are pinned.
        (
            "'2026-08-05'::date",
            tag::DATE,
            Value::Date("2026-08-05".into()),
        ),
        (
            "'2000-01-01'::date",
            tag::DATE,
            Value::Date("2000-01-01".into()),
        ),
        (
            "'1970-01-01'::date",
            tag::DATE,
            Value::Date("1970-01-01".into()),
        ),
        (
            "'infinity'::date",
            tag::DATE,
            Value::Date("infinity".into()),
        ),
        (
            "'-infinity'::date",
            tag::DATE,
            Value::Date("-infinity".into()),
        ),
        // ---- TIME (tag 9). `24:00:00` is legal PG and must NOT wrap to `00:00:00`.
        (
            "'24:00:00'::time",
            tag::TIME,
            Value::Time("24:00:00".into()),
        ),
        (
            "'00:00:00'::time",
            tag::TIME,
            Value::Time("00:00:00".into()),
        ),
        (
            "'13:45:07.25'::time",
            tag::TIME,
            Value::Time("13:45:07.250000".into()),
        ),
        // ---- TIMESTAMP (tag 10) — NAIVE, no zone suffix EVER, and never shifted by the session
        // zone (which is UTC-04:00 here).
        (
            "'2026-08-05 13:45:07.25'::timestamp",
            tag::TIMESTAMP,
            Value::Timestamp("2026-08-05 13:45:07.250000".into()),
        ),
        // Pre-epoch: the µs→days split must FLOOR, not truncate toward zero, or this lands a day late.
        (
            "'1999-12-31 23:59:59'::timestamp",
            tag::TIMESTAMP,
            Value::Timestamp("1999-12-31 23:59:59".into()),
        ),
        (
            "'infinity'::timestamp",
            tag::TIMESTAMP,
            Value::Timestamp("infinity".into()),
        ),
        // ---- TIMESTAMPTZ (tag 11) — RFC3339, ALWAYS normalized to UTC, always a literal `Z`.
        // `13:45:07.25+02` is 11:45:07.25 UTC (and 07:45:07.25 in the session's New York zone).
        (
            "'2026-08-05 13:45:07.25+02'::timestamptz",
            tag::TIMESTAMPTZ,
            Value::TimestampTz("2026-08-05T11:45:07.250000Z".into()),
        ),
        (
            "'-infinity'::timestamptz",
            tag::TIMESTAMPTZ,
            Value::TimestampTz("-infinity".into()),
        ),
        // ---- UUID (tag 12) — canonical 36-char LOWERCASE hyphenated, never raw bytes.
        (
            "'3F2B8C1A-0000-4FFF-8000-ABCDEFABCDEF'::uuid",
            tag::UUID,
            Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
        ),
        // ---- JSON (tag 13) — `json` is a BYTE-EXACT passthrough: PG stores the document text
        // verbatim, so the interior whitespace below must survive the round trip untouched.
        (
            r#"'{"a":1}'::json"#,
            tag::JSON,
            Value::Json(r#"{"a":1}"#.into()),
        ),
        (
            r#"'{ "b" : 2,  "c" : [1, 2] }'::json"#,
            tag::JSON,
            Value::Json(r#"{ "b" : 2,  "c" : [1, 2] }"#.into()),
        ),
        // ---- SQL NULL in a newly-admitted column: `Value::Null`, never a decode error and never
        // an empty-string payload. HEAD still promises the column's own tag (asserted below).
        ("NULL::numeric", tag::DECIMAL, Value::Null),
        ("NULL::timestamptz", tag::TIMESTAMPTZ, Value::Null),
        ("NULL::jsonb", tag::JSON, Value::Null),
    ];

    for (expr, want_tag, want_value) in cases {
        let (head, got) = one(&mut co, expr).await;
        assert_eq!(head, *want_tag, "HEAD tag for `{expr}`");
        assert_eq!(got, *want_value, "value for `{expr}`");
        println!("  {expr:<48} -> tag {head:>2}  {got:?}");
    }

    // The conn is clean after every one of them.
    let ok = co.query("SELECT 1", &[]).await.expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
}

/// **TIMESTAMP vs TIMESTAMPTZ are not swapped.** The two share an identical 8-byte payload, so the
/// only thing standing between "naive local" and "UTC instant" is the column OID — and a swap
/// produces no error at all, just silently shifted values.
///
/// Run under a non-UTC session zone, one query carries: the instant as `timestamptz` (ours), the
/// same instant as a naive `timestamp` in UTC (ours), and PG's OWN text rendering of both. If the
/// two renderers were swapped, the `timestamptz` column would come back as
/// `"2026-08-05 11:45:07.250000"` (naive form, no `Z`) and the `timestamp` column as
/// `"...T...Z"` — both caught below, on top of the tags themselves swapping.
#[tokio::test(flavor = "multi_thread")]
async fn timestamp_and_timestamptz_are_not_swapped() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    pin_non_utc_session_zone(&mut co).await;

    const INSTANT: &str = "'2026-08-05 13:45:07.25+02'::timestamptz";
    let sql = format!(
        "SELECT {INSTANT} AS tstz, \
                ({INSTANT} AT TIME ZONE 'UTC') AS naive_utc, \
                ({INSTANT})::text AS pg_session_render, \
                ({INSTANT} AT TIME ZONE 'UTC')::text AS pg_utc_render, \
                '2026-08-05 13:45:07.25'::timestamp AS naive_literal"
    );
    let r = co.query(&sql, &[]).await.expect("query");

    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![
            tag::TIMESTAMPTZ,
            tag::TIMESTAMP,
            tag::TEXT,
            tag::TEXT,
            tag::TIMESTAMP
        ],
        "a swap in `oid_extract_type` shows up here first"
    );
    let row = &r.rows[0];
    for (i, v) in row.iter().enumerate() {
        assert_eq!(r.cols[i].tag, v.tag(), "HEAD vs producer, column {i}");
    }

    // (a) The session zone is genuinely NOT UTC — PG's own local rendering of this instant is
    //     07:45:07.25 at offset -04. If the server were UTC this assertion fails and we learn the
    //     whole test was vacuous, rather than passing for the wrong reason.
    let pg_session = match &row[2] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(
        pg_session.contains("07:45:07.25") && pg_session.contains("-04"),
        "PG's session-local rendering must differ from UTC; got {pg_session}"
    );

    // (b) OUR timestamptz is the UTC instant with a literal Z — NOT the session wall clock.
    assert_eq!(
        row[0],
        Value::TimestampTz("2026-08-05T11:45:07.250000Z".into()),
        "timestamptz must render the UTC instant (a swap would drop the Z and the T)"
    );

    // (c) OUR naive timestamp of the same instant carries NO zone marker at all.
    assert_eq!(
        row[1],
        Value::Timestamp("2026-08-05 11:45:07.250000".into()),
        "timestamp is naive — no T, no Z, ever"
    );

    // (d) ORACLE: PG's own UTC rendering of the same instant equals ours, modulo the canonical
    //     form's fixed 6-digit fraction and RFC3339 separators. This is the assertion that does
    //     not trust our own arithmetic at all.
    let pg_utc = match &row[3] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert_eq!(
        rfc3339_to_pg_text("2026-08-05T11:45:07.250000Z"),
        pg_utc,
        "our UTC instant must match PG's own rendering of it"
    );

    // (e) A naive literal is NEVER shifted by the session zone: 13:45:07.25 in, 13:45:07.25 out.
    assert_eq!(
        row[4],
        Value::Timestamp("2026-08-05 13:45:07.250000".into()),
        "a naive timestamp must not be touched by the session TimeZone"
    );

    println!(
        "  tstz={:?}\n  naive_utc={:?}\n  pg_session_render={pg_session}\n  pg_utc_render={pg_utc}",
        row[0], row[1]
    );
}

/// Canonical `TIMESTAMPTZ` text → PG's `timestamp::text` form, for oracle comparison: drop the `Z`,
/// swap `T` for a space, and trim the fraction's trailing zeros (PG trims; the canonical form pads
/// to exactly six digits — see `PROTOCOL.md` §3.2).
fn rfc3339_to_pg_text(canonical: &str) -> String {
    let body = canonical.trim_end_matches('Z').replace('T', " ");
    match body.split_once('.') {
        Some((head, frac)) => {
            let frac = frac.trim_end_matches('0');
            if frac.is_empty() {
                head.to_string()
            } else {
                format!("{head}.{frac}")
            }
        }
        None => body,
    }
}

/// **Breaks the self-referential NUMERIC oracle (F23).** `pgtext`'s own test helper is written by
/// the decoder's author, so a shared misunderstanding of the base-10000 layout passes every unit
/// test. Here PG renders the very same stored value with `v::text` in the SAME query, so the only
/// shared assumption left is the wire format itself.
#[tokio::test(flavor = "multi_thread")]
async fn numeric_matches_pg_own_text_rendering() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("CREATE TEMP TABLE ferro_num (id int4, v numeric)")
        .await
        .expect("create temp table");

    let big = format!("{}.{}", "9".repeat(200), "1".repeat(50));
    // Every one of these is a shape the unit tests reason about structurally: ndigits == 0, a
    // negative weight (skipped leading groups), scale padding, preserved display scale, the
    // specials, and 250 significant digits (far past any fixed-width decimal type).
    let seed = [
        "0",
        "0.0000",
        "0.00001",
        "-12345.6700000000",
        "1.10",
        "1.1",
        "10000",
        "-0.5",
        "'NaN'",
        "'Infinity'",
        "'-Infinity'",
        &big,
    ];
    for (i, lit) in seed.iter().enumerate() {
        co.exec(&format!(
            "INSERT INTO ferro_num (id, v) VALUES ({i}, {lit}::numeric)"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {lit} failed: {e:?}"));
    }

    let r = co
        .query("SELECT v, v::text FROM ferro_num ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(r.cols[0].tag, tag::DECIMAL);
    assert_eq!(r.cols[1].tag, tag::TEXT);
    assert_eq!(r.rows.len(), seed.len(), "every seeded row read back");

    for (row, lit) in r.rows.iter().zip(seed) {
        let ours = match &row[0] {
            Value::Decimal(s) => s.clone(),
            other => panic!("expected Decimal for {lit}, got {other:?}"),
        };
        let pg = match &row[1] {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text for {lit}, got {other:?}"),
        };
        assert_eq!(
            ours, pg,
            "our NUMERIC rendering of {lit} must equal PG's own ::text"
        );
        let shown = if ours.len() > 60 {
            format!("{}… ({} chars)", &ours[..60], ours.len())
        } else {
            ours.clone()
        };
        println!("  numeric {lit:<24} -> {shown}");
    }
}

/// **PG as the oracle for wire shapes PG never EMITS but does ACCEPT (carry C11).** `dscale`
/// truncation is the case: PG's own stored values never carry base-10000 digits past their display
/// scale, so `v::text` cannot exercise it — but `numeric_recv` accepts such a payload, so pushing
/// one in through `COPY ... (FORMAT binary)` and reading PG's rendering back turns PG into the
/// authority for a shape a unit test could otherwise only confirm against itself.
///
/// Uses a RAW (non-pooled) client deliberately: `COPY FROM STDIN` puts the connection into copy
/// mode, which the pool's instrumented entry points do not model, and running it through
/// `Checkout::conn_mut()` would bypass the pin authority (a documented cross-tenant hazard). A
/// throwaway connection owned by the test has no such contract.
#[tokio::test(flavor = "multi_thread")]
async fn numeric_crafted_payloads_match_pg_own_rendering() {
    let Some(url) = test_url() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("raw connect");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let cases: &[Crafted] = &[
        // THE ONE PG CANNOT PRODUCE ITSELF: two groups of digits but a display scale of 1, so the
        // renderer must TRUNCATE (1.9), never round (2.0).
        Crafted {
            label: "dscale truncates, never rounds",
            ndigits: 2,
            weight: 0,
            sign: 0x0000,
            dscale: 1,
            digits: &[1, 9900],
            ours: "1.9",
        },
        // ndigits == 0 with a non-zero scale: a naive digit loop emits "" instead of "0.0000".
        Crafted {
            label: "zero renders at its declared scale",
            ndigits: 0,
            weight: 0,
            sign: 0x0000,
            dscale: 4,
            digits: &[],
            ours: "0.0000",
        },
        // weight < 0: leading all-zero groups were SKIPPED on the wire and must be re-emitted.
        Crafted {
            label: "skipped leading zero groups",
            ndigits: 1,
            weight: -2,
            sign: 0x0000,
            dscale: 5,
            digits: &[1000],
            ours: "0.00001",
        },
        // Scale padding out to numeric(30,10), negative sign, weight == 1 (NOT 0 — the digits are
        // [1, 2345, 6700], i.e. 1·10^4 + 2345 + 6700·10^-4).
        Crafted {
            label: "zero-pads out to the declared scale",
            ndigits: 3,
            weight: 1,
            sign: 0x4000,
            dscale: 10,
            digits: &[1, 2345, 6700],
            ours: "-12345.6700000000",
        },
        // An implied TRAILING integral group: digits [1] at weight 1 is 10000, not 1.
        Crafted {
            label: "implied trailing integral group",
            ndigits: 1,
            weight: 1,
            sign: 0x0000,
            dscale: 0,
            digits: &[1],
            ours: "10000",
        },
    ];

    client
        .batch_execute("CREATE TEMP TABLE ferro_num_oracle (id int4, v numeric)")
        .await
        .expect("create temp table");

    // PGCOPY binary stream: signature + flags + header-extension length, then one tuple per case,
    // then the -1 trailer.
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    buf.extend_from_slice(&0u32.to_be_bytes()); // flags
    buf.extend_from_slice(&0u32.to_be_bytes()); // header extension length
    for (i, c) in cases.iter().enumerate() {
        let payload = numeric_payload(c.ndigits, c.weight, c.sign, c.dscale, c.digits);
        buf.extend_from_slice(&2i16.to_be_bytes()); // two fields
        buf.extend_from_slice(&4i32.to_be_bytes()); // int4 id
        buf.extend_from_slice(&(i as i32).to_be_bytes());
        buf.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        buf.extend_from_slice(&payload);
    }
    buf.extend_from_slice(&(-1i16).to_be_bytes()); // trailer

    let sink = client
        .copy_in::<_, Bytes>("COPY ferro_num_oracle (id, v) FROM STDIN (FORMAT binary)")
        .await
        .expect("open binary COPY");
    pin_mut!(sink);
    sink.send(Bytes::from(buf)).await.expect("send copy data");
    let n = sink.finish().await.expect("finish copy");
    assert_eq!(n as usize, cases.len(), "every crafted tuple was accepted");

    let rows = client
        .query("SELECT v::text FROM ferro_num_oracle ORDER BY id", &[])
        .await
        .expect("read back PG's own rendering");
    assert_eq!(rows.len(), cases.len());

    for (row, c) in rows.iter().zip(cases) {
        let pg: String = row.get(0);
        let label = c.label;
        let decoded = pgtext::numeric_to_text(&numeric_payload(
            c.ndigits, c.weight, c.sign, c.dscale, c.digits,
        ))
        .unwrap_or_else(|e| panic!("{label}: our decoder rejected the payload: {e:?}"));
        assert_eq!(decoded, c.ours, "{label}: our decoder changed behaviour");
        assert_eq!(
            decoded, pg,
            "{label}: PG rendered the SAME crafted payload as {pg}, we say {decoded}"
        );
        println!("  crafted {label:<38} -> {decoded} (PG agrees)");
    }

    drop(client);
    handle.abort();
}

/// One crafted `numeric` wire payload plus what our decoder claims it renders to. A named struct
/// rather than a 7-tuple so each field is readable at the call site (and `clippy::type_complexity`
/// stays quiet).
struct Crafted {
    label: &'static str,
    ndigits: i16,
    weight: i16,
    sign: u16,
    dscale: u16,
    digits: &'static [i16],
    ours: &'static str,
}

fn numeric_payload(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + digits.len() * 2);
    v.extend_from_slice(&ndigits.to_be_bytes());
    v.extend_from_slice(&weight.to_be_bytes());
    v.extend_from_slice(&sign.to_be_bytes());
    v.extend_from_slice(&dscale.to_be_bytes());
    for d in digits {
        v.extend_from_slice(&d.to_be_bytes());
    }
    v
}

/// `json` is a byte-exact passthrough; `jsonb` is PG's NORMALIZED document (keys reordered,
/// whitespace dropped), so it is asserted SEMANTICALLY — against PG's own `::text` of the very
/// same jsonb value, which needs no JSON parser on our side.
#[tokio::test(flavor = "multi_thread")]
async fn json_is_byte_exact_and_jsonb_is_pg_normalized() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    const DOC: &str = r#"{ "b" : 2,  "a" : [1, 2],  "u" : "café" }"#;
    let sql = format!(
        "SELECT '{DOC}'::json AS j, '{DOC}'::jsonb AS jb, '{DOC}'::jsonb::text AS pg_jb_text"
    );
    let r = co.query(&sql, &[]).await.expect("query");

    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![tag::JSON, tag::JSON, tag::TEXT],
        "json and jsonb BOTH map to the one canonical JSON tag"
    );
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_eq!(r.cols[i].tag, v.tag(), "HEAD vs producer, column {i}");
    }

    assert_eq!(
        r.rows[0][0],
        Value::Json(DOC.to_string()),
        "json must be a BYTE-EXACT passthrough — spacing and key order untouched"
    );

    let jb = match &r.rows[0][1] {
        Value::Json(s) => s.clone(),
        other => panic!("expected Json, got {other:?}"),
    };
    let pg_jb = match &r.rows[0][2] {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert_eq!(
        jb, pg_jb,
        "jsonb must equal PG's own normalized rendering of the same value"
    );
    assert_ne!(
        jb, DOC,
        "jsonb IS normalized — this test would be vacuous otherwise"
    );
    println!("  json  = {}\n  jsonb = {jb}", DOC);
}

/// **HEAD-vs-producer across BOTH code paths (hazard 18/45).** `query.rs` carries two independent
/// copies of the gate pair — the buffered `run` (cols at `:67`, cells at `:108`) and the streaming
/// `stream` (cols at `:172`, cells at `:245`). One query covering every admitted OID is driven
/// through each, asserting the tag `HEAD` promised is the tag the producer actually emitted.
#[tokio::test(flavor = "multi_thread")]
async fn head_tag_equals_emitted_tag_on_both_paths() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    pin_non_utc_session_zone(&mut co).await;

    // Every OID `oid_extract_type` admits, M0 set included, all non-NULL so `Value::tag()` is
    // meaningful for each.
    const ALL: &str = "SELECT true::bool, 1::int2, 2::int4, 3::int8, 1.5::float4, 2.5::float8, \
         'a'::text, 'b'::varchar, 'c'::bpchar, '\\xdead'::bytea, \
         '1.10'::numeric, '2026-08-05'::date, '13:45:07.25'::time, \
         '2026-08-05 13:45:07.25'::timestamp, '2026-08-05 13:45:07.25+02'::timestamptz, \
         '3f2b8c1a-0000-4fff-8000-abcdefabcdef'::uuid, '{\"a\":1}'::json, '{\"a\":1}'::jsonb";

    let want = [
        tag::BOOL,
        tag::I64,
        tag::I64,
        tag::I64,
        tag::F64,
        tag::F64,
        tag::TEXT,
        tag::TEXT,
        tag::TEXT,
        tag::BYTES,
        tag::DECIMAL,
        tag::DATE,
        tag::TIME,
        tag::TIMESTAMP,
        tag::TIMESTAMPTZ,
        tag::UUID,
        tag::JSON,
        tag::JSON,
    ];

    // ---- buffered path
    let r = co.query(ALL, &[]).await.expect("buffered query");
    assert_eq!(r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(), want);
    assert_eq!(r.rows.len(), 1);
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_ne!(*v, Value::Null, "column {i} must be non-NULL for this test");
        assert_eq!(
            r.cols[i].tag,
            v.tag(),
            "buffered path: HEAD promised {} for column {i} ({}), producer emitted {}",
            r.cols[i].tag,
            r.cols[i].name,
            v.tag()
        );
    }
    println!(
        "  buffered: {} columns, HEAD == producer for all",
        want.len()
    );

    // ---- streaming path (its own cols-build + its own per-cell extraction)
    let mut handle = co.query_stream(ALL, &[]).await.expect("open stream");
    let cols: Vec<u8> = handle.cols().iter().map(|c| c.tag).collect();
    assert_eq!(
        cols, want,
        "streaming cols-build must match the buffered one"
    );
    let row = handle
        .next()
        .await
        .expect("one row")
        .expect("row must be Ok");
    for (i, v) in row.iter().enumerate() {
        assert_eq!(
            cols[i],
            v.tag(),
            "streaming path: HEAD promised {} for column {i}, producer emitted {}",
            cols[i],
            v.tag()
        );
    }
    assert!(handle.next().await.is_none(), "exactly one row");
    handle.finish().await.expect("finish");
    println!(
        "  streaming: {} columns, HEAD == producer for all",
        want.len()
    );

    // No transaction was opened by any of this (the S1 RFQ authority's word). NB `tainted()` is
    // deliberately NOT asserted: `pin_non_utc_session_zone` runs a non-local `SET`, which the S2
    // assist lexer correctly taints for hygiene — reading a new column type does not.
    assert!(!co.tx_open(), "no transaction was opened");
}

/// The live DEFERRAL guard. Each of these must be a loud `Unsupported` raised at cols-build —
/// BEFORE the query runs, so the connection stays clean and usable.
///
/// `timetz` carries the real trap: its payload is 12 bytes (i64 µs + i32 zone) against `time`'s 8,
/// so admitting it into the `TIME` arm would fail MID-DECODE, after `HEAD` is already on the wire.
#[tokio::test(flavor = "multi_thread")]
async fn deferred_column_types_are_refused_before_execution() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    for expr in [
        "'12:34:56+02'::timetz",
        "ARRAY[1,2]::int4[]",
        "'1 day'::interval",
        "'10.0.0.1'::inet",
        // NB the quotes: `::char` is `bpchar` (a SUPPORTED text type); `::"char"` is PG's internal
        // 1-byte `"char"` (OID 18), a named S8 catalog-scalar carry that is still refused today.
        "'a'::\"char\"",
    ] {
        let err = co
            .query(&format!("SELECT {expr}"), &[])
            .await
            .unwrap_err_or_panic(expr);
        assert!(
            matches!(err, PoolError::Unsupported(_)),
            "`{expr}` must be a loud Unsupported, got {err:?}"
        );
        // Raised at cols-build, before execution: the conn is untouched and immediately reusable.
        let ok = co
            .query("SELECT 1", &[])
            .await
            .unwrap_or_else(|e| panic!("conn must survive the refusal of `{expr}`: {e:?}"));
        assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
        println!("  deferred {expr:<28} -> Unsupported, conn clean");
    }
}

/// Tiny helper so the loop above reads cleanly.
trait UnwrapErrOrPanic<T> {
    fn unwrap_err_or_panic(self, what: &str) -> PoolError;
}

impl<T: std::fmt::Debug> UnwrapErrOrPanic<T> for Result<T, PoolError> {
    fn unwrap_err_or_panic(self, what: &str) -> PoolError {
        match self {
            Err(e) => e,
            Ok(v) => panic!("`{what}` unexpectedly succeeded: {v:?}"),
        }
    }
}
