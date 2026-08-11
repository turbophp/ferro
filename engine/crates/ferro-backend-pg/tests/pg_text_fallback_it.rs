//! **M1-S8c Task 1 — the TEXT FALLBACK on the PG read path, live (D-S8b-6).**
//!
//! An OID outside Ferro's canonical set is read as `TAG_TEXT` carrying **PostgreSQL's own text
//! rendering**, instead of the loud `Unsupported` that made 50 of PostgreSQL's 78 non-passing
//! upstream-DBAL tests fail on one cause (`pg_index.indkey`, an `int2vector`).
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
//!   cargo test -p ferro-backend-pg --test pg_text_fallback_it -- --nocapture
//! ```
//! Skips (prints, never fails) when `FERRO_TEST_PG_URL` is unset, like every live file here.
//!
//! # The oracle, and the trap it avoids
//!
//! "PostgreSQL's own text rendering" needs an oracle that is not written by this feature's author.
//! The obvious one — comparing to `(expr)::text` in the same query — is **WRONG**, measured on
//! PG 17.10: `'10.0.0.1'::inet` outputs `10.0.0.1` but `('10.0.0.1'::inet)::text` is
//! `10.0.0.1/32`, because PG ships an explicit `inet`→`text` cast that appends the netmask and it
//! disagrees with `inet_out`. A `::text` oracle would have blessed the wrong bytes for every type
//! with such a cast.
//!
//! The oracle here is the **SIMPLE QUERY protocol** ([`pg_text_oracle`]), which returns every
//! column in the text format by construction — i.e. exactly the mode libpq (and therefore
//! `pdo_pgsql`) runs in. It executes on an INDEPENDENT raw `tokio_postgres` connection, so it
//! shares no code path with the extended-query `Bind` the fallback rides on. The PHP tier adds a
//! real `pdo_pgsql` comparison on top (`php/doctrine-dbal`'s `TextFallbackLiveTest`).
//!
//! # What each test locks
//!
//! 1. `indkey_reads_as_pgs_space_separated_int2vector` — the measured blocker itself.
//! 2. `catalog_relation_types_read_as_pg_text` — every OID D-S8b-6 enumerates from the stock
//!    schema manager's catalog SQL.
//! 3. `custom_oid_types_read_as_their_text` — a native ENUM, a composite, and the
//!    EXTENSION-ASSIGNED oids (`hstore`, `ltree`, `citext`, and PostGIS `geometry` when the image
//!    has it) that no engine release could ever enumerate ahead of time.
//! 4. `the_canonical_types_are_not_swallowed_by_the_fallback` — the typed path is UNCHANGED, and
//!    the assertion is chosen so that routing a canonical type through the fallback changes the
//!    VALUE, not just the tag.
//! 5. `null_and_empty_string_survive_the_fallback`.
//! 6. `a_binary_payload_is_refused_not_rendered` — the safety gate: on a connection with NO
//!    result-format policy the fallback REFUSES rather than decoding binary bytes as a string.
//! 7. `invalid_utf8_text_output_is_a_loud_backend_error` — reachable, not theoretical.
//! 8. `the_streaming_path_takes_the_fallback_too` — `query_stream` has its own copy of the gate.

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::{Checkout, Pool};
use ferro_proto::consts::tag;
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

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// A raw `tokio_postgres` connection with **no result-format policy installed** — deliberately not
/// a pooled `PgBackend` one.
///
/// It serves two purposes and both need exactly this: it is the [`pg_text_oracle`]'s transport (an
/// independent session, so the oracle shares no state or code path with the connection under test),
/// and it is the subject of `a_binary_payload_is_refused_not_rendered`, where the ABSENCE of the
/// policy is the whole point.
async fn raw_client(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("raw oracle connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// PostgreSQL's own text output for the single column of `sql`, read through the **SIMPLE QUERY
/// protocol** — the mode in which the server always uses the type's output function, and the mode
/// libpq/`pdo_pgsql` use for every column. `None` is SQL NULL.
///
/// This is the whole point of the file: the value under test comes back over the EXTENDED query
/// protocol with a per-column `Bind` result format, and it must equal what this returns.
async fn pg_text_oracle(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("oracle `{sql}` failed: {e}"));
    for m in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            return r.get(0).map(str::to_string);
        }
    }
    panic!("oracle `{sql}` returned no row");
}

/// `SELECT <expr>` through the pooled, instrumented path; returns `(head_tag, value)` and asserts
/// the HEAD-vs-producer agreement (hazard 18) on the way through — which for a fallback column is
/// the statement that `oid_to_tag`'s new `TAG_TEXT` arm and `extract_value`'s new fallback arm
/// really are the same decision.
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
            "HEAD promised tag {} for `{expr}` but the producer emitted {}",
            r.cols[0].tag,
            v.tag()
        );
    }
    (r.cols[0].tag, v)
}

/// Asserts `expr` reads as `TAG_TEXT` whose bytes are IDENTICAL to PostgreSQL's own text output for
/// the same expression, taken from the independent simple-query oracle. Returns the value.
async fn assert_matches_pg_text(
    co: &mut Checkout<PgBackend>,
    oracle: &tokio_postgres::Client,
    label: &str,
    expr: &str,
) -> String {
    let (t, v) = one(co, expr).await;
    assert_eq!(
        t,
        tag::TEXT,
        "[{label}] a fallback column's tag is TAG_TEXT"
    );
    let want = pg_text_oracle(oracle, &format!("SELECT {expr}"))
        .await
        .unwrap_or_else(|| panic!("[{label}] the oracle returned NULL; pick a non-NULL fixture"));
    assert_eq!(
        v,
        Value::Text(want.clone()),
        "[{label}] `{expr}`: the fallback must be byte-identical to PostgreSQL's own text output"
    );
    println!("  {label:<14} {expr:<58} -> {want:?}");
    want
}

/// **(1) The measured blocker.** DBAL's stock `PostgreSQLSchemaManager` selects `pg_index.indkey`,
/// an `int2vector` (OID 22) — the single cause of 50 of PostgreSQL's 78 non-passing upstream tests
/// at the S8b acceptance run.
///
/// Two assertions, because they fail for different reasons: the bare
/// `SELECT indkey FROM pg_index LIMIT 1` of the task brief must simply READ (and match the
/// oracle), and a DETERMINISTIC two-column index must produce exactly the space-separated form
/// `"1 2"` — a literal, so "it returned some string" cannot pass.
#[tokio::test(flavor = "multi_thread")]
async fn indkey_reads_as_pgs_space_separated_int2vector() {
    let Some(url) = test_url() else {
        return;
    };
    let oracle = raw_client(&url).await;
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // The bare form from the task brief, ordered so the oracle sees the same row.
    let sql = "SELECT indkey FROM pg_index ORDER BY indexrelid LIMIT 1";
    let r = co.query(sql, &[]).await.expect("pg_index.indkey must read");
    assert_eq!(r.cols[0].tag, tag::TEXT, "int2vector reaches PHP as TEXT");
    let want = pg_text_oracle(&oracle, sql).await.expect("oracle row");
    assert_eq!(r.rows[0][0], Value::Text(want.clone()));
    println!("  pg_index.indkey (first index) -> {want:?}");

    // Deterministic: a two-column index on attnums 1 and 2 has indkey `1 2`, exactly.
    co.exec("DROP TABLE IF EXISTS ferro_s8c_idx").await.ok();
    co.exec("CREATE TABLE ferro_s8c_idx (a int, b int, c int)")
        .await
        .expect("create fixture table");
    co.exec("CREATE INDEX ferro_s8c_idx_ab ON ferro_s8c_idx (a, b)")
        .await
        .expect("create a two-column index");
    let r = co
        .query(
            "SELECT indkey FROM pg_index WHERE indexrelid = 'ferro_s8c_idx_ab'::regclass",
            &[],
        )
        .await
        .expect("read the two-column index's indkey");
    assert_eq!(r.cols[0].tag, tag::TEXT);
    assert_eq!(
        r.rows[0][0],
        Value::Text("1 2".to_string()),
        "an int2vector over attnums 1,2 is PG's space-separated `1 2` — the exact form the stock \
         PostgreSQLSchemaManager splits on"
    );
    println!("  ferro_s8c_idx_ab.indkey       -> \"1 2\"");

    co.exec("DROP TABLE IF EXISTS ferro_s8c_idx").await.ok();
}

/// **(2) Every catalog OID D-S8b-6 enumerates**, each against the oracle. These are the result
/// column types the stock `AbstractSchemaManager`/`PostgreSQLSchemaManager` reads, so this is the
/// list that has to be complete rather than representative.
///
/// `"char"` (18) and `name` (19) are deliberately ABSENT: M1-S8a made them CANONICAL, and they are
/// asserted as such in `the_canonical_types_are_not_swallowed_by_the_fallback`.
#[tokio::test(flavor = "multi_thread")]
async fn catalog_relation_types_read_as_pg_text() {
    let Some(url) = test_url() else {
        return;
    };
    let oracle = raw_client(&url).await;
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // A fixture with a DEFAULT (so `pg_attrdef.adbin`, a `pg_node_tree`, exists) and a skewed
    // column (so `pg_stats.most_common_vals`, an `anyarray`, is non-NULL after ANALYZE).
    co.exec("DROP TABLE IF EXISTS ferro_s8c_cat").await.ok();
    co.exec("CREATE TABLE ferro_s8c_cat (id int PRIMARY KEY, k int DEFAULT 7, label text)")
        .await
        .expect("create catalog fixture");
    co.exec(
        "INSERT INTO ferro_s8c_cat SELECT g, 7, CASE WHEN g % 10 = 0 THEN 'rare' ELSE 'common' \
         END FROM generate_series(1,500) g",
    )
    .await
    .expect("seed catalog fixture");
    co.exec("ANALYZE ferro_s8c_cat")
        .await
        .expect("ANALYZE so pg_stats has MCVs");

    for (label, expr) in [
        (
            "int2vector 22",
            "(SELECT indkey FROM pg_index ORDER BY indexrelid LIMIT 1)",
        ),
        (
            "oidvector 30",
            "(SELECT proargtypes FROM pg_proc WHERE proname = 'int4pl' LIMIT 1)",
        ),
        ("_int2 1005", "ARRAY[1,2]::int2[]"),
        ("_text 1009", "ARRAY['a','b']::text[]"),
        ("_oid 1028", "ARRAY[1,2]::oid[]"),
        ("_aclitem 1034", "'{ferro=arwdDxt/ferro}'::aclitem[]"),
        (
            "pg_node_tree 194",
            "(SELECT adbin FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid \
             WHERE c.relname = 'ferro_s8c_cat' LIMIT 1)",
        ),
        (
            "anyarray 2277",
            "(SELECT most_common_vals FROM pg_stats \
             WHERE tablename = 'ferro_s8c_cat' AND attname = 'label')",
        ),
        ("regproc 24", "'int4pl'::regproc"),
        ("xid 28", "'42'::xid"),
    ] {
        assert_matches_pg_text(&mut co, &oracle, label, expr).await;
    }

    // A couple of the values are pinned as LITERALS as well, so this test cannot degrade into
    // "whatever PG said equals whatever PG said" if the oracle itself ever regressed.
    assert_eq!(one(&mut co, "'42'::xid").await.1, Value::Text("42".into()));
    assert_eq!(
        one(&mut co, "ARRAY['a','b']::text[]").await.1,
        Value::Text("{a,b}".into()),
        "PG's array text output is brace-delimited and comma-separated"
    );

    co.exec("DROP TABLE IF EXISTS ferro_s8c_cat").await.ok();
}

/// **(3) CUSTOM (database-local) oids** — the open-ended tail D-S8b-6 exists to close, since no
/// engine release can enumerate them.
///
/// A native ENUM and a composite type are created here, so they are unconditional. `hstore`,
/// `ltree` and `citext` ship with the official `postgres:17` image's contrib and are created here
/// too — EXTENSION-ASSIGNED oids, which is the same class as PostGIS.
///
/// **PostGIS is not in the `postgres:17` image** (checked: `pg_available_extensions` has
/// `citext`/`hstore`/`ltree` and no `postgis`; the extension directory contains no postgis files).
/// If it has been installed into the container it is exercised for real — a `geometry` column must
/// read as EWKB hex, which is what `longitude-one/doctrine-spatial` et al. parse out of a plain
/// string. If it has not, the test SAYS SO on stdout and the custom-oid class is still covered by
/// the five types above; it never quietly skips.
#[tokio::test(flavor = "multi_thread")]
async fn custom_oid_types_read_as_their_text() {
    let Some(url) = test_url() else {
        return;
    };
    let oracle = raw_client(&url).await;
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("DROP TYPE IF EXISTS ferro_s8c_mood").await.ok();
    co.exec("DROP TYPE IF EXISTS ferro_s8c_pt").await.ok();
    co.exec("CREATE TYPE ferro_s8c_mood AS ENUM ('sad','ok','happy')")
        .await
        .expect("create a native enum");
    co.exec("CREATE TYPE ferro_s8c_pt AS (x int, y text)")
        .await
        .expect("create a composite type");
    for ext in ["hstore", "ltree", "citext"] {
        co.exec(&format!("CREATE EXTENSION IF NOT EXISTS {ext}"))
            .await
            .unwrap_or_else(|e| {
                panic!("the postgres:17 image ships contrib; {ext} must install: {e:?}")
            });
    }

    // Every one of these oids is >= 16384 (assigned at CREATE time, so different in every
    // install) — asserted, because "custom oid" is the property under test, not an assumption.
    for ty in [
        "ferro_s8c_mood",
        "ferro_s8c_pt",
        "hstore",
        "ltree",
        "citext",
    ] {
        let (t, v) = one(&mut co, &format!("'{ty}'::regtype::oid")).await;
        assert_eq!(t, tag::I64, "regtype::oid is canonical I64");
        let Value::I64(oid) = v else {
            panic!("{ty}: expected an oid")
        };
        assert!(
            oid >= 16384,
            "{ty} must have a DATABASE-LOCAL oid (>= 16384), got {oid} — otherwise this test is \
             not exercising the custom-oid path at all"
        );
    }

    // Requirement 3: an ENUM reads as its LABEL; hstore and ltree as their text form.
    let v = assert_matches_pg_text(&mut co, &oracle, "enum", "'happy'::ferro_s8c_mood").await;
    assert_eq!(v, "happy", "an enum cell IS its label string");
    let v = assert_matches_pg_text(&mut co, &oracle, "hstore", "'a=>1,b=>2'::hstore").await;
    assert_eq!(v, "\"a\"=>\"1\", \"b\"=>\"2\"", "PG's hstore_out form");
    let v = assert_matches_pg_text(&mut co, &oracle, "ltree", "'top.sub.leaf'::ltree").await;
    assert_eq!(v, "top.sub.leaf");
    let v = assert_matches_pg_text(&mut co, &oracle, "citext", "'MiXeD'::citext").await;
    assert_eq!(v, "MiXeD", "citext is case-INSENSITIVE, not case-folding");
    let v =
        assert_matches_pg_text(&mut co, &oracle, "composite", "ROW(1,'foo')::ferro_s8c_pt").await;
    assert_eq!(v, "(1,foo)", "PG's record_out form");

    // Requirement 4: PostGIS, for real if the container has it.
    let has_postgis = matches!(
        one(
            &mut co,
            "(SELECT count(*) FROM pg_available_extensions WHERE name = 'postgis')",
        )
        .await
        .1,
        Value::I64(n) if n > 0
    );
    if has_postgis {
        co.exec("CREATE SCHEMA IF NOT EXISTS ferro_ext").await.ok();
        co.exec("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA ferro_ext")
            .await
            .expect("postgis is available, so it must install");
        let (t, v) = one(&mut co, "'ferro_ext.geometry'::regtype::oid").await;
        assert_eq!(t, tag::I64);
        assert!(
            matches!(v, Value::I64(oid) if oid >= 16384),
            "postgis geometry must carry an EXTENSION-ASSIGNED oid, got {v:?}"
        );
        let hex = assert_matches_pg_text(
            &mut co,
            &oracle,
            "geometry",
            "'SRID=4326;POINT(1 2)'::ferro_ext.geometry",
        )
        .await;
        assert_eq!(
            hex, "0101000020E6100000000000000000F03F0000000000000040",
            "a geometry must arrive as EWKB HEX — the exact string doctrine-spatial parses"
        );
        assert!(
            hex.chars().all(|c| c.is_ascii_hexdigit()),
            "EWKB hex is hex digits only: {hex}"
        );
        println!("  PostGIS: PRESENT — geometry proved as EWKB hex");
    } else {
        println!(
            "  PostGIS: ABSENT from this image (postgres:17 ships no postgis; \
             pg_available_extensions lists only citext/hstore/ltree). The custom-oid path is \
             still exercised above by the enum, the composite, and the EXTENSION-ASSIGNED \
             hstore/ltree/citext oids — this arm is additional, not the coverage."
        );
    }

    co.exec("DROP TYPE IF EXISTS ferro_s8c_mood").await.ok();
    co.exec("DROP TYPE IF EXISTS ferro_s8c_pt").await.ok();
}

/// **(5) The canonical types are UNAFFECTED.** Requirement 5 of the task: a canonical type must not
/// silently take the fallback path.
///
/// Each row pins the CANONICAL tag and the canonical `Value` — and for every non-TEXT tag the
/// `Value` VARIANT is what does the work, because a column that took the fallback returns
/// `Value::Text(..)` and can never equal `Value::TimestampTz(..)`/`Decimal(..)`/`I64(..)`/….
///
/// **Which assertion actually fires, measured rather than claimed.** Dropping `TIMESTAMPTZ` from
/// `oid_extract_type` fails on the TAG line (`left: 6, right: 11`). "Helpfully" keeping the tag
/// right while leaving the decoder on the fallback fails one line earlier still, on `one()`'s
/// HEAD-vs-producer check (`HEAD promised tag 11 … producer emitted 6`). An earlier draft of this
/// test also carried `assert_ne!(v, Value::Text(pg_text))` per row; it is REMOVED because no
/// mutation could ever reach it — `assert_eq!(v, want)` already excludes every `Value::Text`, so it
/// was an assertion that could not fail. The `wants_binary_result` half of requirement 5 is pinned
/// where it IS falsifiable: `rowmap`'s `every_canonical_oid_stays_binary_and_keeps_its_own_tag`.
///
/// **The four TEXT-tagged canonical types are INDISTINGUISHABLE from the fallback here, by
/// construction** — `text`, `varchar`, `name` and `"char"` already produce `TAG_TEXT` holding PG's
/// own characters, so the fallback would emit the same tag and the same bytes. Stated rather than
/// papered over; they are pinned by the unit-level `wants_binary_result` assertion instead.
///
/// The session zone is deliberately non-UTC and the resulting `timestamptz` divergence
/// (`2026-08-11T10:00:00Z` vs PG's `2026-08-11 06:00:00-04`) is ASSERTED at the end — that check
/// fails if the zone pin silently did not take, which is what would make the divergence vacuous.
#[tokio::test(flavor = "multi_thread")]
async fn the_canonical_types_are_not_swallowed_by_the_fallback() {
    let Some(url) = test_url() else {
        return;
    };
    let oracle = raw_client(&url).await;
    oracle
        .simple_query("SET TIME ZONE 'America/New_York'")
        .await
        .expect("pin the oracle session to the same non-UTC zone");
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("SET TIME ZONE 'America/New_York'")
        .await
        .expect("pin a non-UTC session zone");
    assert_eq!(
        one(&mut co, "current_setting('TimeZone')").await.1,
        Value::Text("America/New_York".into()),
        "the session zone must genuinely be non-UTC or the timestamptz divergence is vacuous"
    );

    // (tag, expr, canonical value)
    for (want_tag, expr, want) in [
        (tag::BOOL, "true", Value::Bool(true)),
        (tag::I64, "1::int2", Value::I64(1)),
        (tag::I64, "1::int4", Value::I64(1)),
        (tag::I64, "1::int8", Value::I64(1)),
        (tag::F64, "1.5::float4", Value::F64(1.5)),
        (tag::F64, "1.5::float8", Value::F64(1.5)),
        (tag::TEXT, "'x'::text", Value::Text("x".into())),
        (tag::TEXT, "'x'::varchar", Value::Text("x".into())),
        (tag::TEXT, "'x'::name", Value::Text("x".into())),
        (tag::TEXT, "'x'::\"char\"", Value::Text("x".into())),
        (tag::BYTES, "'\\x0102'::bytea", Value::Bytes(vec![1, 2])),
        (
            tag::DECIMAL,
            "'1.50'::numeric(10,2)",
            Value::Decimal("1.50".into()),
        ),
        (
            tag::DATE,
            "'2026-08-11'::date",
            Value::Date("2026-08-11".into()),
        ),
        (
            tag::TIME,
            "'10:00:00'::time",
            Value::Time("10:00:00".into()),
        ),
        (
            tag::TIMESTAMP,
            "'2026-08-11 10:00:00'::timestamp",
            Value::Timestamp("2026-08-11 10:00:00".into()),
        ),
        (
            tag::TIMESTAMPTZ,
            "'2026-08-11 10:00:00+00'::timestamptz",
            Value::TimestampTz("2026-08-11T10:00:00Z".into()),
        ),
        (
            tag::UUID,
            "'0b2f5e1c-9b0f-4a1e-8c3d-1f2e3a4b5c6d'::uuid",
            Value::Uuid("0b2f5e1c-9b0f-4a1e-8c3d-1f2e3a4b5c6d".into()),
        ),
        (
            tag::JSON,
            "'{\"a\": 1}'::json",
            Value::Json("{\"a\": 1}".into()),
        ),
        (tag::I64, "1::oid", Value::I64(1)),
        (tag::I64, "'int4'::regtype", Value::I64(23)),
    ] {
        let (t, v) = one(&mut co, expr).await;
        assert_eq!(
            t, want_tag,
            "`{expr}` must keep its CANONICAL tag {want_tag}; {t} would mean the fallback \
             swallowed the typed path"
        );
        assert_eq!(v, want, "`{expr}` must decode to its canonical value");
    }

    // Spelled out for the headline case, with both renderings printed: a `timestamptz` read
    // through the fallback would carry the session zone's offset instead of the UTC instant —
    // a silent zone shift, which is exactly what the canonical path exists to prevent.
    let pg_text = pg_text_oracle(&oracle, "SELECT '2026-08-11 10:00:00+00'::timestamptz")
        .await
        .expect("oracle row");
    assert_eq!(
        pg_text, "2026-08-11 06:00:00-04",
        "under America/New_York PG renders the local wall clock; if this ever equals the \
         canonical Z form the divergence assertions above stop proving anything"
    );
    println!("  canonical timestamptz  ferro=\"2026-08-11T10:00:00Z\"  pg_text={pg_text:?}");
}

/// **(6) NULL and the empty string, through the fallback.**
///
/// A SQL NULL in a fallback column is `Value::Null` (the `-1` length short-circuits before any
/// rendering), and an empty text payload is `Value::Text("")` — NOT NULL. Those two are one byte
/// apart on the wire (`-1` vs `0` length) and confusing them is the classic text-format bug.
#[tokio::test(flavor = "multi_thread")]
async fn null_and_empty_string_survive_the_fallback() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    co.exec("CREATE EXTENSION IF NOT EXISTS ltree").await.ok();

    // NULL, on both a bare cast and a real table column (the introspection-shaped case).
    for expr in ["NULL::int2vector", "NULL::interval", "NULL::ltree"] {
        let (t, v) = one(&mut co, expr).await;
        assert_eq!(t, tag::TEXT, "`{expr}` still promises TAG_TEXT in HEAD");
        assert_eq!(v, Value::Null, "`{expr}` must be Null, not Text(\"\")");
    }

    // The empty string: `''::ltree` is a legal, EMPTY ltree whose text output is zero bytes.
    let (t, v) = one(&mut co, "''::ltree").await;
    assert_eq!(t, tag::TEXT);
    assert_eq!(
        v,
        Value::Text(String::new()),
        "a zero-length text payload is the EMPTY STRING, never NULL"
    );

    // Same pair through a real table, so the NULL/empty distinction is proved on stored data too.
    co.exec("DROP TABLE IF EXISTS ferro_s8c_nul").await.ok();
    co.exec("CREATE TABLE ferro_s8c_nul (id int, v ltree)")
        .await
        .expect("create fixture");
    co.exec("INSERT INTO ferro_s8c_nul VALUES (1, NULL), (2, ''::ltree)")
        .await
        .expect("seed fixture");
    let r = co
        .query("SELECT v FROM ferro_s8c_nul ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(r.cols[0].tag, tag::TEXT);
    assert_eq!(
        r.rows,
        vec![vec![Value::Null], vec![Value::Text(String::new())]],
        "stored NULL and stored empty must stay distinguishable"
    );
    co.exec("DROP TABLE IF EXISTS ferro_s8c_nul").await.ok();
}

/// **THE SAFETY GATE.** On a connection with **no result-format policy**, a fallback column comes
/// back in the BINARY format — and the fallback must REFUSE it rather than render binary bytes as
/// a string.
///
/// This is the one hazard the mechanism creates (the module docs of `rowmap` name it as hazard 16's
/// sibling), and it is not hypothetical: `int2vector`'s binary payload is an array header of mostly
/// printable-range bytes, so decoding it as text yields a plausible-looking garbage string with no
/// error anywhere. The gate is `Column::result_format()` — the SAME field the fork's `Bind` encoder
/// puts on the wire — so the format and the decoder cannot disagree.
///
/// The subject is a RAW `tokio_postgres::Client`, which is exactly the shape this repo already has
/// five of in its own tests, and the shape any future caller that bypasses `PgBackend::connect`
/// would have.
///
/// The refusal must be `PoolError::Backend` — NOT `ConnectionLost`, which §19.3 would turn into a
/// false `Indeterminate` on a write.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_payload_is_refused_not_rendered() {
    let Some(url) = test_url() else {
        return;
    };
    let unpoliced = raw_client(&url).await;

    // Sanity: the very same statement WORKS on a pooled connection, so the failure below is about
    // the missing policy and nothing else.
    let pool = Pool::new(PgBackend::new(url.clone()), config(1));
    let mut co = pool.checkout().await.expect("checkout");
    let sql = "SELECT indkey FROM pg_index ORDER BY indexrelid LIMIT 1";
    let ok = co.query(sql, &[]).await.expect("policed conn reads it");
    assert_eq!(ok.cols[0].tag, tag::TEXT);
    assert!(matches!(&ok.rows[0][0], Value::Text(_)));

    let err = ferro_backend_pg::query::run(&unpoliced, sql, &[])
        .await
        .expect_err("an unpoliced connection must REFUSE the fallback, not render binary as text");
    let msg = match &err {
        PoolError::Backend(m) => m.clone(),
        other => panic!(
            "a format mismatch is a client-side decode fault: it must be PoolError::Backend \
             (NonRetryable), never ConnectionLost — which §19.3 would turn into a false \
             Indeterminate on a write. Got {other:?}"
        ),
    };
    assert!(
        msg.contains("BINARY result format"),
        "the refusal must say the payload was binary: {msg}"
    );
    assert!(
        msg.contains("set_result_format_policy"),
        "the refusal must name the missing policy so it is actionable: {msg}"
    );
    assert!(
        msg.contains("indkey") && msg.contains("int2vector"),
        "the refusal must name the column and PG's own type name: {msg}"
    );
    println!("  unpoliced conn -> refused: {msg}");
}

/// **The UTF-8 net is reachable, not decorative.** PostgreSQL emits text in the session's
/// `client_encoding`; `tokio-postgres` fixes that at UTF8 at connect, but a session can change it.
/// Under `client_encoding = 'LATIN1'` a non-ASCII enum label arrives as a single `0xE9` byte, which
/// is not valid UTF-8 — measured, not assumed.
///
/// The refusal must be `PoolError::Backend` for the same §19.3 reason as the gate above, and it
/// must NAME the byte offset, so an operator can tell "your client_encoding is wrong" from "you hit
/// a bug". A lossy `from_utf8_lossy` here would silently corrupt the value instead.
#[tokio::test(flavor = "multi_thread")]
async fn invalid_utf8_text_output_is_a_loud_backend_error() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    co.exec("DROP TABLE IF EXISTS ferro_s8c_acc").await.ok();
    co.exec("DROP TYPE IF EXISTS ferro_s8c_accent").await.ok();
    co.exec("CREATE TYPE ferro_s8c_accent AS ENUM ('café')")
        .await
        .expect("create an enum with a non-ASCII label");
    co.exec("CREATE TABLE ferro_s8c_acc (mood ferro_s8c_accent)")
        .await
        .expect("create the fixture table");
    co.exec("INSERT INTO ferro_s8c_acc VALUES ('café')")
        .await
        .expect("store the non-ASCII label while the session is still UTF8");

    // Under UTF8 it reads fine — so the failure below is caused by the ENCODING, not by the label.
    // (The value is read from a stored ROW, never from a SQL literal: after the switch below the
    // server would interpret the literal's UTF-8 bytes as LATIN1 and reject the enum INPUT, which
    // would fail this test for a completely different reason — measured, and the reason this
    // fixture is a table.)
    let r = co
        .query("SELECT mood FROM ferro_s8c_acc", &[])
        .await
        .expect("reads under UTF8");
    assert_eq!(r.cols[0].tag, tag::TEXT);
    assert_eq!(r.rows[0][0], Value::Text("café".to_string()));

    co.exec("SET client_encoding TO 'LATIN1'")
        .await
        .expect("switch the session encoding");
    let err = co
        .query("SELECT mood FROM ferro_s8c_acc", &[])
        .await
        .expect_err("LATIN1 output is not valid UTF-8 and must be refused");
    let msg = match &err {
        PoolError::Backend(m) => m.clone(),
        other => panic!("a decode mismatch must be Backend, never ConnectionLost: {other:?}"),
    };
    assert!(
        msg.contains("not valid UTF-8") && msg.contains("mood"),
        "the refusal must name the problem and the column: {msg}"
    );
    println!("  LATIN1 enum label -> refused: {msg}");

    co.exec("SET client_encoding TO 'UTF8'").await.ok();
    co.exec("DROP TABLE IF EXISTS ferro_s8c_acc").await.ok();
    co.exec("DROP TYPE IF EXISTS ferro_s8c_accent").await.ok();
}

/// The STREAMING path (`query_stream`) has its own copy of the cols-build/per-cell pair, so the
/// fallback has to be proved there too — S5's `PgRowStream` carries its own `oids` vector and calls
/// `extract_value` from a different place.
#[tokio::test(flavor = "multi_thread")]
async fn the_streaming_path_takes_the_fallback_too() {
    let Some(url) = test_url() else {
        return;
    };
    let oracle = raw_client(&url).await;
    let pool = Pool::new(PgBackend::new(url), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    let sql = "SELECT indkey FROM pg_index ORDER BY indexrelid LIMIT 3";
    let want = pg_text_oracle(&oracle, sql).await.expect("oracle row");

    let (head_tag, got) = {
        let mut handle = co.query_stream(sql, &[]).await.expect("open the stream");
        assert_eq!(handle.cols().len(), 1);
        let head_tag = handle.cols()[0].tag;
        assert_eq!(head_tag, tag::TEXT, "HEAD promises TEXT for int2vector");
        let mut got = Vec::new();
        while let Some(r) = handle.next().await {
            got.push(r.expect("streamed row"));
        }
        handle.finish().await.expect("finish the stream");
        (head_tag, got)
    };
    assert_eq!(got.len(), 3, "three rows streamed");
    assert_eq!(
        got[0][0],
        Value::Text(want.clone()),
        "the first streamed cell must match PG's own text output"
    );
    for row in &got {
        assert_eq!(
            row[0].tag(),
            head_tag,
            "HEAD-vs-producer must agree on every streamed fallback cell"
        );
    }
    assert!(
        !co.tainted(),
        "a clean fallback stream leaves the conn clean"
    );
    println!("  streamed 3 int2vector cells, first = {want:?}");
}
