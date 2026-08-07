//! Live per-type round trips for the M1-S7 canonical tags (Task 5b) against REAL MySQL 8 and
//! MariaDB 11. Every test SKIPS (never fails) when its env var is unset, so
//! `cargo test --workspace` stays green offline — the `query_it.rs` / `utc_pin_it.rs` pattern.
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//! FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
//!   cargo test -p ferro-backend-mysql -- --nocapture
//! ```
//!
//! What this file proves that the `rowmap`/`mytext` unit tests cannot:
//!
//! 1. **The two gates agree on real cells (hazard 18/45).** `column_to_tag` runs at cols-build off
//!    the PREPARED STATEMENT, before the query executes; `extract_value` runs per cell, after the
//!    column metadata has already been turned into `HEAD`. The assertion that bites is
//!    HEAD-vs-producer on a real row: `cols[i].tag == rows[0][i].tag()`.
//! 2. **`DATETIME` and `TIMESTAMP` are not swapped, and the INSTANT is right — not just the
//!    suffix.** The two arrive as byte-identical driver components, so only the column type
//!    separates naive-local from UTC-instant and a swap produces no error at all. The proof writes
//!    a known wall clock through a SIDE connection pinned to a deliberately non-UTC zone and checks
//!    the engine renders the UTC instant that zone implies — an assertion that "it came back with a
//!    `Z`" would pass while the value was shifted.
//! 3. **`BIGINT UNSIGNED` works for SMALL magnitudes.** ≤ `i64::MAX` arrives as `MyValue::Int`, not
//!    `UInt` (hazard 23), so a suite testing only `u64::MAX` is green over the bug that breaks every
//!    real-world row.
//! 4. **The two engines' documented divergences are ASSERTED, never skipped** — MariaDB `JSON` is a
//!    `LONGTEXT` alias and reads as `TEXT` by design; MariaDB's native `UUID`/`INET4`/`INET6` reach
//!    the wire as plain utf8 strings; MySQL 8's default `sql_mode` blocks zero dates while
//!    MariaDB 11's does not; MySQL clamps an out-of-range `TIME` fraction to `.000000` while
//!    MariaDB keeps `.999999`.
//! 5. **The deferrals are still refused, live**, at cols-build, with the connection left clean.
//!
//! **Canonical text ≠ display text (carry C15).** Every expectation below is the canonical payload
//! `proto/PROTOCOL.md` §3.2 pins — a fraction group is absent when zero and exactly six digits
//! otherwise — which deliberately differs from what `SELECT CAST(col AS CHAR)` shows for a
//! `TIME(6)`/`DATETIME(6)` column (the server pads `.000000`). Comparing against the server's
//! display text would chase false failures.
//!
//! Binding is Task 8: every value here is written with a text literal through `simple_query`.

use ferro_pool::backend::PoolBackend;
use ferro_pool::error::PoolError;
use ferro_proto::consts::{branch, errc, tag};
use ferro_proto::value::Value;
use mysql_async::prelude::Queryable;

use ferro_backend_mysql::{MysqlBackend, MysqlConn};

/// A deliberately non-UTC zone for the side connection that WRITES the timestamp fixtures. Fixed
/// offsets (not named zones) because a bare MySQL/MariaDB image has no `mysql.time_zone` table
/// loaded, so `SET time_zone = 'America/New_York'` fails with errno 1298.
const WRITER_ZONES: [&str; 2] = ["+05:30", "-08:00"];

fn mysql_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MYSQL_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_MYSQL_URL unset");
            None
        }
    }
}

fn mariadb_url() -> Option<String> {
    match std::env::var("FERRO_TEST_MARIADB_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_MARIADB_URL unset");
            None
        }
    }
}

/// Read one text scalar off the raw handle — a verification-only read that deliberately bypasses
/// the canonical mapping (it is asserting SERVER state, not Ferro's rendering of it).
async fn read_text(conn: &mut MysqlConn, sql: &str) -> String {
    conn.mysql
        .query_first::<String, _>(sql)
        .await
        .unwrap_or_else(|e| panic!("read `{sql}` failed: {e:?}"))
        .unwrap_or_else(|| panic!("read `{sql}` returned no row"))
}

/// True on MariaDB. The engine split is real (JSON, zero dates, the `TIME` fraction clamp), so it
/// is measured per connection rather than inferred from which env var supplied the URL.
async fn is_mariadb(conn: &mut MysqlConn) -> bool {
    read_text(conn, "SELECT VERSION()")
        .await
        .to_ascii_lowercase()
        .contains("mariadb")
}

/// `SELECT <expr>` through the real data path, returning `(head_tag, value)` and asserting the
/// HEAD-vs-producer agreement (hazard 18) for every non-NULL cell on the way through.
async fn one(backend: &MysqlBackend, conn: &mut MysqlConn, expr: &str) -> (u8, Value) {
    let r = backend
        .query(conn, &format!("SELECT {expr}"), &[])
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

/// Every covered column type, read back as EXACT canonical text, plus the HEAD-vs-producer
/// agreement on each cell. One wide table so the cols-build gate and the per-cell gate are
/// exercised against the same prepared statement.
async fn types_round_trip_exact_canonical_text(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    let maria = is_mariadb(&mut conn).await;

    // The reading connection is the UTC-pinned one (Task 5a Step 0) — restated here because every
    // TIMESTAMPTZ assertion below is only meaningful under the pin.
    assert_eq!(
        read_text(&mut conn, "SELECT @@session.time_zone").await,
        "+00:00",
        "[{label}] the reading connection must be UTC-pinned"
    );

    // A TEMPORARY table: session-scoped, so it needs no cleanup and cannot collide with a
    // concurrently-running test on the same server.
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_types (\
               id INT, bu BIGINT UNSIGNED, iu INT UNSIGNED, tu TINYINT UNSIGNED, \
               t1 TINYINT(1), t1u TINYINT(1) UNSIGNED, \
               d DECIMAL(30,10), dt DATETIME(6), da DATE, tm TIME(6))",
        )
        .await
        .expect("create temp table");
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_types VALUES \
             (1, 18446744073709551615, 4294967295, 255, 1, 0, '-12345.67', \
              '2026-08-05 11:45:07.25', '2026-08-05', '-838:59:58.000001')",
        )
        .await
        .expect("seed");

    let r = backend
        .query(
            &mut conn,
            "SELECT bu, iu, tu, t1, t1u, d, dt, da, tm FROM ferro_types WHERE id = 1",
            &[],
        )
        .await
        .expect("read back");

    let want_tags = [
        tag::U64,  // BIGINT UNSIGNED — the ONLY column type that reaches U64
        tag::I64,  // INT UNSIGNED — lossless in i64, deliberately NOT U64
        tag::I64,  // TINYINT UNSIGNED (width > 1)
        tag::BOOL, // TINYINT(1)
        tag::BOOL, // TINYINT(1) UNSIGNED — hazard 44: display length wins over signedness
        tag::DECIMAL,
        tag::TIMESTAMP, // DATETIME is NAIVE
        tag::DATE,
        tag::TIME,
    ];
    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        want_tags.to_vec(),
        "[{label}] cols-build tags"
    );

    let want_values = vec![
        Value::U64(u64::MAX),
        Value::I64(4_294_967_295),
        Value::I64(255),
        Value::Bool(true),
        Value::Bool(false),
        // DECIMAL(30,10) keeps its DISPLAY SCALE — '-12345.67' comes back padded, verbatim.
        Value::Decimal("-12345.6700000000".into()),
        // Canonical, NOT the server's display text: exactly six fraction digits.
        Value::Timestamp("2026-08-05 11:45:07.250000".into()),
        Value::Date("2026-08-05".into()),
        // A MySQL TIME is a signed duration, not a time of day.
        Value::Time("-838:59:58.000001".into()),
    ];
    assert_eq!(r.rows, vec![want_values], "[{label}] canonical values");

    // HEAD-vs-producer on every cell of a real row.
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_eq!(
            r.cols[i].tag,
            v.tag(),
            "[{label}] HEAD promised {} for column {} ({}), producer emitted {}",
            r.cols[i].tag,
            i,
            r.cols[i].name,
            v.tag()
        );
        println!(
            "  [{label}] {:<4} -> tag {:>2}  {v:?}",
            r.cols[i].name, r.cols[i].tag
        );
    }

    // A SQL NULL in a newly-admitted column is `Value::Null`, never a decode error — and HEAD still
    // promises the column's own tag.
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_types (id) VALUES (2)", // every other column NULL
        )
        .await
        .expect("seed nulls");
    let r = backend
        .query(
            &mut conn,
            "SELECT bu, d, dt, da, tm FROM ferro_types WHERE id = 2",
            &[],
        )
        .await
        .expect("read nulls");
    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![tag::U64, tag::DECIMAL, tag::TIMESTAMP, tag::DATE, tag::TIME],
        "[{label}] a NULL cell does not change the column's HEAD tag"
    );
    assert_eq!(r.rows, vec![vec![Value::Null; 5]], "[{label}] NULL cells");

    // The connection survived all of it.
    let ok = backend
        .query(&mut conn, "SELECT 1", &[])
        .await
        .expect("conn still usable");
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    println!("  [{label}] maria={maria} canonical-text matrix PASSED");
    conn.mysql.disconnect().await.ok();
}

/// **Hazard 23 / F5, live.** A `BIGINT UNSIGNED` value ≤ `i64::MAX` arrives as `MyValue::Int`, not
/// `UInt` — so the SMALL magnitudes are the ones a `UInt`-only extractor breaks on, and a suite
/// that only checked `u64::MAX` would be green over the bug that breaks every real-world row.
async fn bigint_unsigned_covers_both_driver_forms(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_u64 (id INT, v BIGINT UNSIGNED)",
        )
        .await
        .expect("create temp table");

    // 0 / 5 / 2^32 / i64::MAX all arrive as `Int`; only the last two exceed i64::MAX -> `UInt`.
    let cases: [u64; 6] = [
        0,
        5,
        4_294_967_296,
        i64::MAX as u64,
        (i64::MAX as u64) + 1,
        u64::MAX,
    ];
    for (i, v) in cases.iter().enumerate() {
        backend
            .simple_query(
                &mut conn,
                &format!("INSERT INTO ferro_u64 (id, v) VALUES ({i}, {v})"),
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] seed {v} failed: {e:?}"));
    }

    let r = backend
        .query(&mut conn, "SELECT v FROM ferro_u64 ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(r.cols[0].tag, tag::U64, "[{label}] BIGINT UNSIGNED -> U64");
    assert_eq!(
        r.rows,
        cases
            .iter()
            .map(|v| vec![Value::U64(*v)])
            .collect::<Vec<_>>(),
        "[{label}] every magnitude round-trips, both driver forms"
    );
    for (row, v) in r.rows.iter().zip(cases) {
        assert_eq!(
            r.cols[0].tag,
            row[0].tag(),
            "[{label}] HEAD vs producer for {v}"
        );
    }
    println!("  [{label}] BIGINT UNSIGNED: {cases:?} all -> U64 (Int form AND UInt form)");
    conn.mysql.disconnect().await.ok();
}

/// **`DATETIME` is naive, `TIMESTAMP` is a UTC instant — and the INSTANT is proven, not the suffix
/// (F12).** The same wall clock is written through a SIDE connection pinned to a deliberately
/// non-UTC zone; MySQL interprets a `TIMESTAMP` literal in the WRITER's zone and converts it back
/// into the reader's, so the engine's canonical text must be the UTC instant that writer zone
/// implies — computed here independently of the engine. A `DATETIME` is stored verbatim and must be
/// byte-identical under both writer zones, with no `T` and no `Z`, ever.
///
/// Asserting only "the TIMESTAMP came back with a Z" would pass while the value was shifted by the
/// session offset, which is precisely the silent failure the UTC pin exists to prevent.
async fn timestamp_is_a_utc_instant_and_datetime_is_naive(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut reader = backend.connect().await.expect("connect");
    assert_eq!(
        read_text(&mut reader, "SELECT @@session.time_zone").await,
        "+00:00",
        "[{label}] the reading connection must be UTC-pinned, or this test is vacuous"
    );

    // A REAL (non-temporary) table: a TEMPORARY one is visible only to the connection that made it,
    // and the whole point here is that a DIFFERENT connection does the writing.
    backend
        .simple_query(&mut reader, "DROP TABLE IF EXISTS ferro_s7_ts")
        .await
        .expect("drop");
    backend
        .simple_query(
            &mut reader,
            "CREATE TABLE ferro_s7_ts (id INT PRIMARY KEY, ts TIMESTAMP(6) NULL, dt DATETIME(6))",
        )
        .await
        .expect("create");

    const WALL: &str = "2026-08-05 11:45:07.250000";
    // What 11:45:07.25 in each writer zone IS, as a UTC instant.
    let expect_utc = [
        ("+05:30", "2026-08-05T06:15:07.250000Z"),
        ("-08:00", "2026-08-05T19:45:07.250000Z"),
    ];

    for (i, zone) in WRITER_ZONES.iter().enumerate() {
        let mut side = mysql_async::Conn::from_url(url)
            .await
            .expect("side connection");
        side.query_drop(format!("SET SESSION time_zone = '{zone}'"))
            .await
            .expect("pin the writer's zone");
        let z: Option<String> = side
            .query_first("SELECT @@session.time_zone")
            .await
            .expect("read back");
        assert_eq!(
            z.as_deref(),
            Some(*zone),
            "[{label}] the writer's zone must genuinely be non-UTC"
        );
        side.query_drop(format!(
            "REPLACE INTO ferro_s7_ts (id, ts, dt) VALUES ({i}, '{WALL}', '{WALL}')"
        ))
        .await
        .expect("write the fixture under the writer's zone");
        side.disconnect().await.ok();

        let r = backend
            .query(
                &mut reader,
                &format!("SELECT ts, dt FROM ferro_s7_ts WHERE id = {i}"),
                &[],
            )
            .await
            .expect("read back through Ferro");
        assert_eq!(
            r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
            vec![tag::TIMESTAMPTZ, tag::TIMESTAMP],
            "[{label}] a swap in `column_kind` shows up here first"
        );
        for (n, v) in r.rows[0].iter().enumerate() {
            assert_eq!(
                r.cols[n].tag,
                v.tag(),
                "[{label}] HEAD vs producer, column {n}"
            );
        }

        let (_, want_utc) = expect_utc[i];
        assert_eq!(
            r.rows[0][0],
            Value::TimestampTz(want_utc.into()),
            "[{label}] a TIMESTAMP written as {WALL} under {zone} IS {want_utc} — asserting only \
             the `Z` suffix would bless a shifted value"
        );
        assert_eq!(
            r.rows[0][1],
            Value::Timestamp(WALL.into()),
            "[{label}] a DATETIME is NAIVE: identical text under every writer zone, no T, no Z"
        );
        println!("  [{label}] writer {zone}: ts -> {want_utc}   dt -> {WALL} (naive)");
    }

    // **The fraction-omission rule, live, for the OTHER two canonical temporal tags.** `PROTOCOL.md`
    // §3.2: no `.ffffff` group at all when the sub-second part is zero, exactly six digits
    // otherwise. `TIME` already has this locked live (`time_spans_the_full_signed_range`); these are
    // the `TIMESTAMP`/`TIMESTAMPTZ` halves, and the columns are declared `(6)` precisely so the
    // server's own DISPLAY text would pad `.000000` — which is exactly why the oracle here is the
    // canonical literal and NOT `CAST(col AS CHAR)` (carry C15). The write goes through the
    // UTC-pinned reader, so the whole-second wall clock IS the whole-second UTC instant.
    const WHOLE: &str = "2026-08-05 11:45:07";
    backend
        .simple_query(
            &mut reader,
            &format!("REPLACE INTO ferro_s7_ts (id, ts, dt) VALUES (9, '{WHOLE}', '{WHOLE}')"),
        )
        .await
        .expect("write the whole-second fixture");
    let r = backend
        .query(
            &mut reader,
            "SELECT ts, dt FROM ferro_s7_ts WHERE id = 9",
            &[],
        )
        .await
        .expect("read back the whole-second fixture");
    assert_eq!(
        r.rows[0][0],
        Value::TimestampTz("2026-08-05T11:45:07Z".into()),
        "[{label}] a whole-second TIMESTAMP(6) must OMIT the fraction group — the server's display \
         text pads `.000000`, the canonical payload does not"
    );
    assert_eq!(
        r.rows[0][1],
        Value::Timestamp(WHOLE.into()),
        "[{label}] a whole-second DATETIME(6) must OMIT the fraction group"
    );
    for (n, v) in r.rows[0].iter().enumerate() {
        assert_eq!(
            r.cols[n].tag,
            v.tag(),
            "[{label}] HEAD vs producer, whole-second column {n}"
        );
    }
    println!(
        "  [{label}] whole second: ts -> {:?}   dt -> {:?} (no .000000 group)",
        r.rows[0][0], r.rows[0][1]
    );

    backend
        .simple_query(&mut reader, "DROP TABLE IF EXISTS ferro_s7_ts")
        .await
        .expect("cleanup");
    reader.mysql.disconnect().await.ok();
}

/// **Expression columns classify too.** A `CAST(...)` or function result carries its own column
/// metadata with no table behind it, and a DBAL-generated statement is full of them — so the
/// classifier must work off the prepared statement's metadata alone, which is exactly the discipline
/// `column_kind` enforces. Each one also re-asserts HEAD-vs-producer (inside [`one`]).
async fn expression_columns_classify_off_metadata_alone(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");

    let maria = is_mariadb(&mut conn).await;
    let cases: &[(&str, u8, Value)] = &[
        // `CAST(x AS UNSIGNED)` is the most common way a real query produces an unsigned column
        // without declaring one. `-1` forces the FULL width on both engines and, as a bonus, is
        // the driver's `UInt` form arriving from an expression rather than a table column.
        (
            "CAST(-1 AS UNSIGNED)",
            tag::U64,
            Value::U64(18_446_744_073_709_551_615),
        ),
        (
            "CAST('2026-08-05' AS DATE)",
            tag::DATE,
            Value::Date("2026-08-05".into()),
        ),
        (
            "CAST('-12345.6700' AS DECIMAL(30,10))",
            tag::DECIMAL,
            Value::Decimal("-12345.6700000000".into()),
        ),
        // A naive wall clock from the server, under the UTC pin.
        (
            "CAST('2026-08-05 11:45:07.250000' AS DATETIME(6))",
            tag::TIMESTAMP,
            Value::Timestamp("2026-08-05 11:45:07.250000".into()),
        ),
    ];

    for (expr, want_tag, want_value) in cases {
        let (head, got) = one(&backend, &mut conn, expr).await;
        assert_eq!(head, *want_tag, "[{label}] HEAD tag for `{expr}`");
        assert_eq!(got, *want_value, "[{label}] value for `{expr}`");
        println!("  [{label}] {expr:<46} -> tag {head:>2}  {got:?}");
    }

    // **A measured engine divergence, asserted rather than avoided.** MariaDB NARROWS a
    // constant-folded `CAST(1 AS UNSIGNED)` to `MYSQL_TYPE_LONG` (an `INT UNSIGNED`, max
    // 4 294 967 295 — lossless in `i64`, so `I64` is the CORRECT classification), while MySQL 8
    // always reports `MYSQL_TYPE_LONGLONG + UNSIGNED_FLAG` and so reaches `U64`. Both are right for
    // the column they describe; the case above (`CAST(-1 AS UNSIGNED)`) is the one that forces the
    // full width on both. Locking this here means a future "why is MariaDB I64 here?" is answered
    // by a test instead of a debugging session.
    let (head, got) = one(&backend, &mut conn, "CAST(1 AS UNSIGNED)").await;
    if maria {
        assert_eq!(
            (head, got),
            (tag::I64, Value::I64(1)),
            "[{label}] MariaDB narrows a folded CAST(1 AS UNSIGNED) to INT UNSIGNED -> I64"
        );
    } else {
        assert_eq!(
            (head, got),
            (tag::U64, Value::U64(1)),
            "[{label}] MySQL keeps BIGINT UNSIGNED -> U64"
        );
    }
    println!(
        "  [{label}] CAST(1 AS UNSIGNED) -> tag {head}  ({})",
        if maria {
            "MariaDB narrows to INT UNSIGNED -> I64"
        } else {
            "MySQL keeps BIGINT UNSIGNED -> U64"
        }
    );
    conn.mysql.disconnect().await.ok();
}

/// **The `TIME` extremes, and the engine divergence at the very top of the range.** A MySQL/MariaDB
/// `TIME` is a signed duration spanning ±838:59:59.999999, not a time of day.
///
/// The maximum is where the engines split, MEASURED: `'838:59:59.999999'` is out of range for
/// MySQL 8 (it is the documented `838:59:59` maximum plus a fraction), so under the default
/// `STRICT_TRANS_TABLES` MySQL **rejects the insert** (`22007 Incorrect time value`) and under a
/// permissive `sql_mode` it **truncates the fraction** to `838:59:59` — while MariaDB 11 stores it
/// exactly. So the seed runs under an explicit `sql_mode = ''` and the expectation is per engine.
/// This is why `mytext`'s `MAX_TIME_US` carries the trailing `.999999`: it is REQUIRED for the
/// MariaDB case, not slack, and tightening it to MySQL's clamp would break MariaDB.
async fn time_spans_the_full_signed_range(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    let maria = is_mariadb(&mut conn).await;
    let sql_mode = read_text(&mut conn, "SELECT @@session.sql_mode").await;

    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_time (id INT, v TIME(6))",
        )
        .await
        .expect("create temp table");

    // (literal, canonical text). The fraction rule is all-or-exactly-six, never trimmed.
    let mut cases: Vec<(&str, &str)> = vec![
        ("00:00:00", "00:00:00"),
        ("13:45:07.25", "13:45:07.250000"),
        ("26:00:00", "26:00:00"), // exceeds 24 h: `days` must be folded into hours
        ("-00:00:01", "-00:00:01"), // the sign survives
        ("-838:59:58.000001", "-838:59:58.000001"),
    ];
    // The engines disagree at the top of the range — assert the divergence, do not skip it.
    cases.push(if maria {
        ("838:59:59.999999", "838:59:59.999999")
    } else {
        ("838:59:59.999999", "838:59:59")
    });

    // Permissive for the seed only: MySQL refuses the out-of-range literal outright under strict
    // mode, so without this the divergence could not be exercised on MySQL at all.
    backend
        .simple_query(&mut conn, "SET SESSION sql_mode = ''")
        .await
        .expect("permit the out-of-range TIME literal");
    for (i, (lit, _)) in cases.iter().enumerate() {
        backend
            .simple_query(
                &mut conn,
                &format!("INSERT INTO ferro_time (id, v) VALUES ({i}, '{lit}')"),
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] seed {lit} failed: {e:?}"));
    }
    backend
        .simple_query(&mut conn, &format!("SET SESSION sql_mode = '{sql_mode}'"))
        .await
        .expect("restore sql_mode");

    let r = backend
        .query(&mut conn, "SELECT v FROM ferro_time ORDER BY id", &[])
        .await
        .expect("read back");
    assert_eq!(r.cols[0].tag, tag::TIME);
    for (row, (lit, want)) in r.rows.iter().zip(&cases) {
        assert_eq!(
            row[0],
            Value::Time((*want).into()),
            "[{label}] TIME '{lit}' must render as the canonical {want}"
        );
        assert_eq!(r.cols[0].tag, row[0].tag(), "[{label}] HEAD vs producer");
        println!("  [{label}] TIME {lit:<20} -> {want}");
    }
    println!(
        "  [{label}] maximum-fraction divergence: {}",
        if maria {
            "MariaDB keeps .999999"
        } else {
            "MySQL clamps to .000000"
        }
    );
    conn.mysql.disconnect().await.ok();
}

/// **The DECIMAL display scale survives byte-for-byte**, cross-checked against the server's OWN
/// text rendering of the same stored value (`CAST(v AS CHAR)`) — which for a DECIMAL is exactly the
/// canonical payload, since the driver hands back the server's ASCII rendering untouched (hazard
/// 22). This is a legitimate oracle here precisely because DECIMAL, unlike the date/time family,
/// has no display-vs-canonical divergence (carry C15).
async fn decimal_matches_the_servers_own_rendering(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_dec (id INT, v DECIMAL(30,10), w DECIMAL(10,2), z DECIMAL(20,0))",
        )
        .await
        .expect("create temp table");
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_dec VALUES \
             (0, '-12345.67', '1.10', '0'), \
             (1, '0.0000000001', '-0.05', '99999999999999999999')",
        )
        .await
        .expect("seed");

    let r = backend
        .query(
            &mut conn,
            "SELECT v, CAST(v AS CHAR), w, CAST(w AS CHAR), z, CAST(z AS CHAR) \
             FROM ferro_dec ORDER BY id",
            &[],
        )
        .await
        .expect("read back");
    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![
            tag::DECIMAL,
            tag::TEXT,
            tag::DECIMAL,
            tag::TEXT,
            tag::DECIMAL,
            tag::TEXT
        ],
    );
    for row in &r.rows {
        for pair in [(0, 1), (2, 3), (4, 5)] {
            let (Value::Decimal(ours), Value::Text(server)) = (&row[pair.0], &row[pair.1]) else {
                panic!("[{label}] expected a Decimal/Text pair, got {:?}", row);
            };
            assert_eq!(
                ours, server,
                "[{label}] our DECIMAL rendering must equal the server's own CAST(.. AS CHAR)"
            );
            println!("  [{label}] decimal {ours}");
        }
    }
    // The display scale is genuinely preserved (this would be vacuous if the server trimmed).
    assert_eq!(r.rows[0][0], Value::Decimal("-12345.6700000000".into()));
    assert_eq!(r.rows[0][2], Value::Decimal("1.10".into()));
    conn.mysql.disconnect().await.ok();
}

/// **JSON is engine-conditional and asserted EXPLICITLY, never skipped (F15).** MySQL 8 emits a real
/// `MYSQL_TYPE_JSON` column type, so a `JSON` column carries tag `JSON`. MariaDB has **no JSON
/// type**: its `JSON` is an alias for `LONGTEXT` plus a `json_valid()` CHECK and reaches the wire
/// byte-identically to a plain `LONGTEXT`, so it classifies as `TEXT` **by design** — promoting a
/// utf8 `LONGTEXT` to `JSON` would be the silent miscast charter rule 6 forbids.
async fn json_classifies_per_engine(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    let maria = is_mariadb(&mut conn).await;

    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_json (id INT, j JSON, lt LONGTEXT)",
        )
        .await
        .expect("create temp table");
    backend
        .simple_query(
            &mut conn,
            r#"INSERT INTO ferro_json VALUES (0, '{ "b" : 2,  "a" : [1, 2] }', '{"plain":true}')"#,
        )
        .await
        .expect("seed");

    let r = backend
        .query(
            &mut conn,
            "SELECT j, CAST(j AS CHAR), lt FROM ferro_json",
            &[],
        )
        .await
        .expect("read back");

    let want_json_tag = if maria { tag::TEXT } else { tag::JSON };
    assert_eq!(
        r.cols[0].tag, want_json_tag,
        "[{label}] MariaDB JSON is LONGTEXT + a json_valid() CHECK with no recoverable metadata \
         (hazard 25) — it classifies as TEXT BY DESIGN. MySQL 8 emits MYSQL_TYPE_JSON -> JSON."
    );
    // A plain LONGTEXT is TEXT on BOTH engines — the negative half of the same rule.
    assert_eq!(
        r.cols[2].tag,
        tag::TEXT,
        "[{label}] a utf8 LONGTEXT must never be promoted to JSON"
    );
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_eq!(
            r.cols[i].tag,
            v.tag(),
            "[{label}] HEAD vs producer, column {i}"
        );
    }

    // The DOCUMENT is right either way: it equals the server's own rendering of the same value.
    // (MySQL normalizes a JSON document on storage — key order and spacing change — so the oracle
    // is the server, not the literal we wrote.)
    let ours = match &r.rows[0][0] {
        Value::Json(s) | Value::Text(s) => s.clone(),
        other => panic!("[{label}] unexpected {other:?}"),
    };
    let server = match &r.rows[0][1] {
        Value::Text(s) => s.clone(),
        other => panic!("[{label}] expected Text, got {other:?}"),
    };
    assert_eq!(
        ours, server,
        "[{label}] the JSON document must equal the server's own CAST(j AS CHAR)"
    );
    println!(
        "  [{label}] JSON column -> tag {} ({}), document {ours}",
        r.cols[0].tag,
        if maria {
            "MariaDB: TEXT by design"
        } else {
            "MySQL 8: JSON"
        }
    );
    conn.mysql.disconnect().await.ok();
}

/// **Zero dates, live (hazard 27 / F35).** `'0000-00-00'` is a legal MySQL value under a permissive
/// `sql_mode` and must surface as the verbatim, deliberately non-parseable sentinel text — never an
/// error and never an invented calendar date.
///
/// MySQL 8's DEFAULT `sql_mode` includes `NO_ZERO_DATE,NO_ZERO_IN_DATE` (measured:
/// `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,…`) while MariaDB 11's does
/// not (`STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`),
/// so on MySQL the insert is wrapped in an explicit per-statement `SET SESSION sql_mode = ''`. That
/// `SET` TAINTS the connection through the S6 tracker, which is correct and expected here. There is
/// deliberately no silent-skip branch: this case runs on BOTH engines.
async fn zero_dates_render_as_the_verbatim_sentinel(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    let before = read_text(&mut conn, "SELECT @@session.sql_mode").await;
    println!("  [{label}] sql_mode = {before}");

    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_zero (id INT, da DATE, dt DATETIME, ts TIMESTAMP NULL)",
        )
        .await
        .expect("create temp table");

    backend
        .simple_query(&mut conn, "SET SESSION sql_mode = ''")
        .await
        .expect("permit zero dates for this statement");
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_zero VALUES \
             (0, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00')",
        )
        .await
        .expect("seed the zero date");
    backend
        .simple_query(&mut conn, &format!("SET SESSION sql_mode = '{before}'"))
        .await
        .expect("restore sql_mode");

    let r = backend
        .query(&mut conn, "SELECT da, dt, ts FROM ferro_zero", &[])
        .await
        .expect("read back");
    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![tag::DATE, tag::TIMESTAMP, tag::TIMESTAMPTZ]
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Date("0000-00-00".into()),
            Value::Timestamp("0000-00-00 00:00:00".into()),
            // A zero TIMESTAMP carries the SAME verbatim sentinel — no `T`, no `Z`: it is not an
            // instant and must not be parseable as one (PROTOCOL.md §3.2).
            Value::TimestampTz("0000-00-00 00:00:00".into()),
        ]],
        "[{label}] zero dates are carried verbatim, never an error and never an invented date"
    );
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_eq!(
            r.cols[i].tag,
            v.tag(),
            "[{label}] HEAD vs producer, column {i}"
        );
    }
    println!("  [{label}] zero date/datetime/timestamp -> verbatim sentinels");
    conn.mysql.disconnect().await.ok();
}

/// The live DEFERRAL guard: each must be a loud `Unsupported` raised at cols-build — BEFORE the
/// query runs, so the connection stays clean and immediately reusable.
///
/// **M1-S8a removed `ENUM` from the refusal list and MOVED it, in this same function, to a positive
/// read assertion** — an ENUM cell's binary-protocol value IS its label string, so it now reads as
/// `TEXT`. The coverage is relocated, not deleted: `e` is still exercised on the same live table.
async fn deferred_column_types_are_refused_before_execution(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_deferred (\
               y YEAR, b BIT(8), e ENUM('a','b'), s SET('x','y'))",
        )
        .await
        .expect("create temp table");
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_deferred VALUES (2026, b'10101010', 'a', 'x')",
        )
        .await
        .expect("seed");

    // M1-S8a: `e` (ENUM) is no longer here — see the positive assertion after the loop.
    for (col, native) in [("y", "YEAR"), ("b", "BIT"), ("s", "SET")] {
        let err = backend
            .query(&mut conn, &format!("SELECT {col} FROM ferro_deferred"), &[])
            .await
            .expect_err("must be refused");
        let PoolError::Unsupported(msg) = &err else {
            panic!("[{label}] `{col}` must be a loud Unsupported, got {err:?}");
        };
        assert!(
            msg.contains(col) && msg.contains(native),
            "[{label}] the refusal must name the column and its native type: {msg}"
        );
        // Raised at cols-build, before execution — the conn is untouched and immediately reusable.
        let ok = backend
            .query(&mut conn, "SELECT 1", &[])
            .await
            .unwrap_or_else(|e| {
                panic!("[{label}] conn must survive the refusal of `{col}`: {e:?}")
            });
        assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
        println!("  [{label}] deferred {native:<5} -> Unsupported, conn clean");
    }

    // M1-S8a: ENUM is ADMITTED as its label string, on the same table the refusals above use.
    let enum_row = backend
        .query(&mut conn, "SELECT e FROM ferro_deferred", &[])
        .await
        .unwrap_or_else(|e| panic!("[{label}] an ENUM column must now READ, got {e:?}"));
    assert_eq!(
        enum_row.rows,
        vec![vec![Value::Text("a".into())]],
        "[{label}] an ENUM cell's wire value IS its label string"
    );
    assert_eq!(
        enum_row.cols[0].tag,
        enum_row.rows[0][0].tag(),
        "[{label}] HEAD vs producer on the ENUM column"
    );
    println!("  [{label}] admitted ENUM  -> TEXT(\"a\")");
    conn.mysql.disconnect().await.ok();
}

/// **MariaDB's extended types, MEASURED and recorded (hazard 25).** MariaDB 10.7+ has a native
/// `UUID` type (and 10.5+ has `INET6`, 10.10+ `INET4`), all of which the plan expected to stay a
/// loud `Unsupported` in S7. Live measurement says otherwise: without the
/// `MARIADB_CLIENT_EXTENDED_METADATA` capability (which `mysql_async` does not negotiate and
/// `ColumnMeta` does not expose), MariaDB reports every one of them as `MYSQL_TYPE_STRING` in a
/// **utf8mb4** charset carrying its 36-/39-/15-char TEXT form — byte-identically to a plain
/// `CHAR(n)`.
///
/// So they classify as `TEXT`, exactly like MariaDB's `JSON`, and the value delivered is correct.
/// Ferro still never emits tag `UUID` from a MySQL-family backend (hazard 25's substantive rule) —
/// what changes is only that this is a TEXT-by-design divergence rather than a refusal. Asserting
/// it here is what stops a future "improvement" from guessing the type off the flags.
async fn mariadb_extended_types_classify_as_text(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    assert!(
        is_mariadb(&mut conn).await,
        "[{label}] this case is MariaDB-only"
    );
    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_maria_ext (u UUID, i6 INET6, i4 INET4, c CHAR(36))",
        )
        .await
        .expect("create temp table");
    backend
        .simple_query(
            &mut conn,
            "INSERT INTO ferro_maria_ext VALUES \
             ('3f2b8c1a-0000-4fff-8000-abcdefabcdef', '::1', '10.0.0.1', \
              '3f2b8c1a-0000-4fff-8000-abcdefabcdef')",
        )
        .await
        .expect("seed");

    let r = backend
        .query(&mut conn, "SELECT u, i6, i4, c FROM ferro_maria_ext", &[])
        .await
        .expect("read back");
    assert_eq!(
        r.cols.iter().map(|c| c.tag).collect::<Vec<_>>(),
        vec![tag::TEXT; 4],
        "[{label}] MariaDB's UUID/INET6/INET4 reach the wire as plain utf8 strings — the driver \
         exposes no metadata to tell them from a CHAR(36), so they are TEXT by design (and the \
         UUID tag is still never emitted by a MySQL-family backend)"
    );
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Text("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
            Value::Text("::1".into()),
            Value::Text("10.0.0.1".into()),
            Value::Text("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
        ]],
        "[{label}] and the VALUES are the server's own text — no miscast, only a generic tag"
    );
    for (i, v) in r.rows[0].iter().enumerate() {
        assert_eq!(
            r.cols[i].tag,
            v.tag(),
            "[{label}] HEAD vs producer, column {i}"
        );
    }
    println!("  [{label}] MariaDB UUID/INET6/INET4 -> TEXT (measured, recorded in §22.2)");
    conn.mysql.disconnect().await.ok();
}

/// **Task 8b — the BIND path: read → bind straight back → read again is BYTE-IDENTICAL.**
///
/// Deliberately a round trip rather than a literal comparison: both reads come from the SAME column
/// type, so what is asserted is "a value Ferro handed you is a value Ferro can put back" — which is
/// what a DBAL insert of a hydrated entity actually does. The load-bearing case is `TIMESTAMPTZ`:
/// its canonical text carries a `Z` that no MySQL/MariaDB datetime literal accepts (hazard 41), so
/// it must reach the server as TYPED components — and those components are only correct because the
/// session is UTC-pinned, which is re-asserted here.
async fn bind_round_trip_is_byte_identical(url: &str, label: &str) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");
    let maria = is_mariadb(&mut conn).await;
    assert_eq!(
        read_text(&mut conn, "SELECT @@session.time_zone").await,
        "+00:00",
        "[{label}] the TIMESTAMPTZ bind is only correct under the UTC pin — the two are coupled"
    );
    let sql_mode = read_text(&mut conn, "SELECT @@session.sql_mode").await;

    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_s7_bind (
                 id BIGINT PRIMARY KEY,
                 d  DECIMAL(30,10),
                 d2 DECIMAL(10,2),
                 un BIGINT UNSIGNED,
                 da DATE,
                 tm TIME(6),
                 dt DATETIME(6),
                 ts TIMESTAMP(6) NULL,
                 j  JSON,
                 uu CHAR(36)
             )",
        )
        .await
        .expect("create temp table");

    const COLS: &str = "d, d2, un, da, tm, dt, ts, j";
    const PLACEHOLDERS: &str = "?, ?, ?, ?, ?, ?, ?, ?";
    // Seeds are written with TEXT LITERALS (no params), so the FIRST read is an independent oracle
    // and not an echo of the bind path. Row 1 is fractional, row 2 is whole-second (the canonical
    // payload must carry NO `.ffffff` group — and a re-bind must not grow one) with a SMALL
    // `BIGINT UNSIGNED` (hazard 23: ≤ i64::MAX arrives as the driver's `Int`, not `UInt`).
    let seeds: &[(i64, &str)] = &[
        (
            1,
            "'-12345.6700000000', '1.1', 18446744073709551615, '2026-08-05', \
             '-838:59:58.000001', '2026-08-05 11:45:07.250000', '2026-08-05 11:45:07.250000', \
             '{\"a\": [1, 2]}'",
        ),
        (
            2,
            "'0.0000000001', '0.00', 5, '1970-01-02', '26:00:00', \
             '2026-08-05 11:45:07', '2026-08-05 11:45:07', 'null'",
        ),
    ];

    for (id, literals) in seeds {
        backend
            .simple_query(
                &mut conn,
                &format!("INSERT INTO ferro_s7_bind (id, {COLS}) VALUES ({id}, {literals})"),
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] seed row {id}: {e:?}"));

        // (1) READ.
        let first = backend
            .query(
                &mut conn,
                &format!("SELECT {COLS} FROM ferro_s7_bind WHERE id = {id}"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] read seed row {id}: {e:?}"));
        let v1 = first.rows[0].clone();

        // (2) BIND those exact canonical values straight back.
        let echo_id = id + 100;
        let mut params = vec![Value::I64(echo_id)];
        params.extend(v1.iter().cloned());
        backend
            .query(
                &mut conn,
                &format!("INSERT INTO ferro_s7_bind (id, {COLS}) VALUES (?, {PLACEHOLDERS})"),
                &params,
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] BIND-BACK of row {id} failed: {e:?}"));

        // (3) READ AGAIN — byte-identical, per column.
        let second = backend
            .query(
                &mut conn,
                &format!("SELECT {COLS} FROM ferro_s7_bind WHERE id = {echo_id}"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("[{label}] read echo row {echo_id}: {e:?}"));

        for (i, (a, b)) in v1.iter().zip(second.rows[0].iter()).enumerate() {
            assert_eq!(
                a, b,
                "[{label}] row {id}, column {} ({i}): read -> bind -> read must be BYTE-IDENTICAL",
                first.cols[i].name
            );
            assert_eq!(
                first.cols[i].tag, second.cols[i].tag,
                "[{label}] row {id}, column {i}: the tag must survive the round trip too"
            );
        }
        println!("  [{label}] row {id} round trip: {v1:?}");
    }

    // The precision/instant claims, spelled out rather than left implicit in the equality above.
    let r = backend
        .query(
            &mut conn,
            "SELECT d2, un, tm, dt, ts FROM ferro_s7_bind WHERE id = 101",
            &[],
        )
        .await
        .expect("read the echoed row 1");
    assert_eq!(
        r.rows[0][0],
        Value::Decimal("1.10".into()),
        "[{label}] the DISPLAY SCALE survives a bind: `1.10` must not come back as `1.1`"
    );
    assert_eq!(
        r.rows[0][1],
        Value::U64(u64::MAX),
        "[{label}] the top of the BIGINT UNSIGNED range survives (never wrapped through i64)"
    );
    assert_eq!(
        r.rows[0][2],
        Value::Time("-838:59:58.000001".into()),
        "[{label}] a NEGATIVE, >24 h TIME survives — it is a signed duration, not a time of day"
    );
    assert_eq!(
        r.rows[0][3],
        Value::Timestamp("2026-08-05 11:45:07.250000".into()),
        "[{label}] the naive DATETIME survives unshifted"
    );
    assert_eq!(
        r.rows[0][4],
        Value::TimestampTz("2026-08-05T11:45:07.250000Z".into()),
        "[{label}] the UTC INSTANT survives a bind of the `Z`-suffixed canonical text — which is \
         only possible because it went out as TYPED components under the UTC pin (hazard 41)"
    );
    let r = backend
        .query(
            &mut conn,
            "SELECT tm, dt, ts FROM ferro_s7_bind WHERE id = 102",
            &[],
        )
        .await
        .expect("read the echoed row 2");
    assert_eq!(r.rows[0][0], Value::Time("26:00:00".into()));
    assert_eq!(
        r.rows[0][1],
        Value::Timestamp("2026-08-05 11:45:07".into()),
        "[{label}] a whole-second DATETIME(6) must NOT grow a fraction group through the bind"
    );
    assert_eq!(
        r.rows[0][2],
        Value::TimestampTz("2026-08-05T11:45:07Z".into()),
        "[{label}] a whole-second TIMESTAMP(6) must NOT grow a fraction group through the bind"
    );

    // **The two documented engine asymmetries, asserted on the BIND side too (hazard 25).** MySQL 8
    // has no native UUID type and MariaDB's `JSON` is a `LONGTEXT` alias, so neither column can
    // READ back as its canonical tag — but both must still accept the canonical payload byte-for-
    // byte, which is what the S8 DBAL tier will do with a `Ferro\Uuid` / `Ferro\Json`.
    const UUID_TEXT: &str = "3f2b8c1a-0000-4fff-8000-abcdefabcdef";
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_bind (id, uu, j) VALUES (?, ?, ?)",
            &[
                Value::I64(300),
                Value::Uuid(UUID_TEXT.to_string()),
                Value::Json(r#"{"a": [1, 2]}"#.to_string()),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] UUID/JSON canonical bind failed: {e:?}"));
    let r = backend
        .query(
            &mut conn,
            "SELECT uu, j FROM ferro_s7_bind WHERE id = 300",
            &[],
        )
        .await
        .expect("read the UUID/JSON row");
    assert_eq!(
        r.rows[0][0],
        Value::Text(UUID_TEXT.into()),
        "[{label}] a canonical UUID binds into CHAR(36) verbatim and reads back as TEXT — MySQL 8 \
         has no native UUID type, and MariaDB's native UUID is TEXT BY DESIGN, not deferred: it \
         reaches the wire as MYSQL_TYPE_STRING/utf8mb4, byte-indistinguishable from a CHAR(36) \
         (§22.2 (e)). No MySQL-family backend ever emits the UUID tag."
    );
    assert_eq!(r.cols[0].tag, tag::TEXT);
    assert_eq!(
        r.cols[1].tag,
        if maria { tag::TEXT } else { tag::JSON },
        "[{label}] MariaDB JSON is a LONGTEXT alias -> TEXT by design; MySQL 8 has a real JSON type"
    );
    println!(
        "  [{label}] canonical UUID -> CHAR(36) -> TEXT verbatim; JSON column tag = {}",
        r.cols[1].tag
    );

    // **The zero sentinels round-trip too** — under a permissive `sql_mode`, because MySQL 8's
    // default includes `NO_ZERO_DATE` while MariaDB 11's does not (F35). They are bound as the
    // all-zero driver components, never as a parsed calendar value and never as a panic.
    backend
        .simple_query(&mut conn, "SET SESSION sql_mode = ''")
        .await
        .expect("permit zero dates for this block");
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_bind (id, da, dt, ts) VALUES (?, ?, ?, ?)",
            &[
                Value::I64(400),
                Value::Date("0000-00-00".into()),
                Value::Timestamp("0000-00-00 00:00:00".into()),
                // The one canonical TIMESTAMPTZ payload that is NOT RFC3339.
                Value::TimestampTz("0000-00-00 00:00:00".into()),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] zero-sentinel bind failed: {e:?}"));
    // A zero-in-date (`2026-00-05`), legal without NO_ZERO_IN_DATE, carries through component-wise.
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_bind (id, da) VALUES (?, ?)",
            &[Value::I64(401), Value::Date("2026-00-05".into())],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] zero-in-date bind failed: {e:?}"));
    let r = backend
        .query(
            &mut conn,
            "SELECT da, dt, ts FROM ferro_s7_bind WHERE id = 400",
            &[],
        )
        .await
        .expect("read the zero-sentinel row");
    assert_eq!(
        r.rows[0],
        vec![
            Value::Date("0000-00-00".into()),
            Value::Timestamp("0000-00-00 00:00:00".into()),
            Value::TimestampTz("0000-00-00 00:00:00".into()),
        ],
        "[{label}] the zero sentinels survive a bind verbatim — never an invented date"
    );
    let r = backend
        .query(
            &mut conn,
            "SELECT da FROM ferro_s7_bind WHERE id = 401",
            &[],
        )
        .await
        .expect("read the zero-in-date row");
    assert_eq!(r.rows[0][0], Value::Date("2026-00-05".into()));
    backend
        .simple_query(&mut conn, &format!("SET SESSION sql_mode = '{sql_mode}'"))
        .await
        .expect("restore sql_mode");
    println!("  [{label}] zero sentinels + zero-in-date round-trip verbatim through the bind");

    // **A payload MySQL has no representation for is a CLEAN TYPED ERROR — never a panic and never
    // a substituted date.** PG's `infinity` / `-infinity` / `NaN` are legal canonical payloads that
    // a cross-engine application can genuinely hand to a MySQL pool (read from PG, written to
    // MySQL). The binder refuses them at the PRE-SEND pre-flight (fix round 1) — it will neither
    // fabricate components nor pass the text through, since a passthrough is silently COERCED under
    // a permissive `sql_mode` (see
    // `non_canonical_payloads_are_rejected_pre_send_under_permissive_sql_mode`, which proves that
    // half). Here the session still carries the engine's DEFAULT `sql_mode`, so this case asserts
    // only the outcome both modes now share: a known-fate SQL error, connection clean.
    let mut id = 500;
    for (v, col) in [
        (Value::Date("infinity".into()), "da"),
        (Value::Date("-infinity".into()), "da"),
        (Value::Timestamp("infinity".into()), "dt"),
        (Value::TimestampTz("-infinity".into()), "ts"),
        (Value::Decimal("NaN".into()), "d"),
        (Value::Time("839:00:00".into()), "tm"),
    ] {
        id += 1;
        let r = backend
            .query(
                &mut conn,
                &format!("INSERT INTO ferro_s7_bind (id, {col}) VALUES (?, ?)"),
                &[Value::I64(id), v.clone()],
            )
            .await;
        match r {
            Err(PoolError::Sql { .. }) => {}
            Err(PoolError::ConnectionLost) => panic!(
                "[{label}] {v:?} -> {col}: a non-representable payload must NEVER be \
                 ConnectionLost — that would mint a false §19.3 Indeterminate"
            ),
            other => panic!("[{label}] {v:?} -> {col}: expected a clean SQL error, got {other:?}"),
        }
        // The connection survives: the statement failed server-side, nothing was corrupted.
        let ok = backend
            .query(&mut conn, "SELECT 1", &[])
            .await
            .unwrap_or_else(|e| panic!("[{label}] conn must survive {v:?}: {e:?}"));
        assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
        println!("  [{label}] non-representable {v:?} -> {col}: clean SQL error, conn clean");
    }

    conn.mysql.disconnect().await.ok();
}

/// **Task 8b fix round 1 — a non-canonical payload is rejected PRE-SEND, and stays rejected under a
/// PERMISSIVE `sql_mode`.**
///
/// The original Task 8b binder passed a non-parseable canonical payload through as a byte-string
/// param, on the reasoning that the SERVER would then produce its own clean, known-fate error. That
/// reasoning holds ONLY under `STRICT_TRANS_TABLES`. Measured under `SET SESSION sql_mode = ''` —
/// which is what a legacy stack routinely runs in, since Doctrine sets no `sql_mode` — the server
/// does not error at all; it **silently coerces**:
///
/// ```text
/// [mysql]   Date("infinity")      -> ACCEPTED, stored 0000-00-00
/// [mysql]   Timestamp("infinity") -> ACCEPTED, stored 0000-00-00 00:00:00
/// [mysql]   Time("839:00:00")     -> ACCEPTED, stored 838:59:59
/// [mysql]   Decimal("NaN")        -> ACCEPTED, stored 0.0000
/// [mariadb] Time("839:00:00")     -> ACCEPTED, stored 838:59:59.999999
/// ```
///
/// A cross-engine app reading a PG `date 'infinity'` and writing it to a MySQL pool would therefore
/// store a wrong value with NO error anywhere — the silent miscast charter rule 6 forbids. The fix
/// makes the non-canonical branch a bind-time rejection alongside the arity pre-check, so the
/// outcome no longer depends on a server session variable at all.
///
/// Both halves are asserted here: the rejection is a KNOWN-FATE `Sql{Unsupported}` (branch
/// `NonRetryable`, no SQLSTATE — the server never saw it) with **nothing written** and the
/// connection immediately reusable; and, in the SAME permissive session, every payload that is a
/// legal MySQL value still binds — the zero-date sentinels above all, which are exactly what a
/// permissive `sql_mode` exists to allow (the over-rejection guard).
async fn non_canonical_payloads_are_rejected_pre_send_under_permissive_sql_mode(
    url: &str,
    label: &str,
) {
    let backend = MysqlBackend::new(url);
    let mut conn = backend.connect().await.expect("connect");

    backend
        .simple_query(
            &mut conn,
            "CREATE TEMPORARY TABLE ferro_s7_coerce (
                 id BIGINT PRIMARY KEY,
                 d  DECIMAL(30,10),
                 da DATE,
                 tm TIME(6),
                 dt DATETIME(6),
                 ts TIMESTAMP(6) NULL
             )",
        )
        .await
        .expect("create temp table");

    // The mode that turns every rejection below into a SILENT coercion. Asserted, not assumed: if
    // the server had ignored the `SET`, this whole test would be measuring STRICT mode instead and
    // would pass over the defect it exists to catch.
    backend
        .simple_query(&mut conn, "SET SESSION sql_mode = ''")
        .await
        .expect("permissive sql_mode");
    assert_eq!(
        read_text(&mut conn, "SELECT @@session.sql_mode").await,
        "",
        "[{label}] the coercion hazard only exists under a permissive sql_mode — the SET must have \
         taken effect or this test proves nothing"
    );

    let mut id = 600;
    for (v, col) in [
        // The PG DATE/TIMESTAMP range sentinels: legal canonical payloads MySQL cannot represent.
        (Value::Date("infinity".into()), "da"),
        (Value::Date("-infinity".into()), "da"),
        (Value::Timestamp("infinity".into()), "dt"),
        (Value::TimestampTz("infinity".into()), "ts"),
        (Value::TimestampTz("-infinity".into()), "ts"),
        // One µs past the documented MySQL TIME range, both signs.
        (Value::Time("839:00:00".into()), "tm"),
        (Value::Time("-839:00:00".into()), "tm"),
        // The PG NUMERIC specials.
        (Value::Decimal("NaN".into()), "d"),
        (Value::Decimal("Infinity".into()), "d"),
        (Value::Decimal("-Infinity".into()), "d"),
    ] {
        id += 1;
        let r = backend
            .query(
                &mut conn,
                &format!("INSERT INTO ferro_s7_coerce (id, {col}) VALUES (?, ?)"),
                &[Value::I64(id), v.clone()],
            )
            .await;
        match r {
            Err(PoolError::Sql {
                code,
                branch: b,
                ref sqlstate,
                ..
            }) => {
                assert_eq!(
                    code,
                    errc::UNSUPPORTED,
                    "[{label}] {v:?} -> {col}: a non-canonical payload is a bind-time Unsupported"
                );
                assert_eq!(
                    b,
                    branch::NON_RETRYABLE,
                    "[{label}] {v:?} -> {col}: the rejection must classify NonRetryable — retrying \
                     an unrepresentable payload can never succeed"
                );
                assert_eq!(
                    *sqlstate, None,
                    "[{label}] {v:?} -> {col}: the server never saw the statement, so there is no \
                     SQLSTATE to report"
                );
            }
            Err(PoolError::ConnectionLost) => panic!(
                "[{label}] {v:?} -> {col}: a rejected bind must NEVER be ConnectionLost — that \
                 would mint a false §19.3 Indeterminate for a write that never left the engine"
            ),
            Err(other) => panic!(
                "[{label}] {v:?} -> {col}: expected a known-fate Sql{{Unsupported}}, got {other:?}"
            ),
            Ok(_) => {
                let stored = read_text(
                    &mut conn,
                    &format!("SELECT CAST({col} AS CHAR) FROM ferro_s7_coerce WHERE id = {id}"),
                )
                .await;
                panic!(
                    "[{label}] SILENT COERCION: {v:?} was ACCEPTED into `{col}` and stored as \
                     {stored:?} under sql_mode='' — a corrupt write with no error anywhere"
                );
            }
        }
        // Nothing was written: the rejection happened before the statement reached the server, so
        // there is no row at all — not a row carrying a coerced value.
        assert_eq!(
            read_text(
                &mut conn,
                &format!("SELECT COUNT(*) FROM ferro_s7_coerce WHERE id = {id}")
            )
            .await,
            "0",
            "[{label}] {v:?} -> {col}: the rejection is PRE-SEND, so no row may exist"
        );
        // ...and the connection is immediately usable (no half-sent statement, no desync).
        let ok = backend
            .query(&mut conn, "SELECT 1", &[])
            .await
            .unwrap_or_else(|e| panic!("[{label}] conn must survive {v:?}: {e:?}"));
        assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
        println!("  [{label}] non-canonical {v:?} -> {col}: pre-send Unsupported, nothing written");
    }
    assert_eq!(
        read_text(&mut conn, "SELECT COUNT(*) FROM ferro_s7_coerce").await,
        "0",
        "[{label}] not one of the rejected payloads may have reached the table"
    );

    // **The over-rejection guard, in the SAME permissive session.** A legal MySQL sentinel is not a
    // non-canonical payload: the zero date / zero datetime and a zero-in-date are exactly the values
    // `sql_mode = ''` exists to allow, and they must still bind, verbatim.
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_coerce (id, d, da, tm, dt, ts) VALUES (?, ?, ?, ?, ?, ?)",
            &[
                Value::I64(700),
                Value::Decimal("-12345.6700000000".into()),
                Value::Date("0000-00-00".into()),
                Value::Time("-838:59:58.000001".into()),
                Value::Timestamp("0000-00-00 00:00:00".into()),
                // The one canonical TIMESTAMPTZ payload that is NOT RFC3339.
                Value::TimestampTz("0000-00-00 00:00:00".into()),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] the legal sentinels must still bind: {e:?}"));
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_coerce (id, da, tm) VALUES (?, ?, ?)",
            &[
                Value::I64(701),
                // A zero-in-date, legal without NO_ZERO_IN_DATE.
                Value::Date("2026-00-05".into()),
                // A >24 h TIME — a duration, not a time of day.
                Value::Time("26:00:00".into()),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] legal >24h/zero-in-date payloads must bind: {e:?}"));
    // `24:00:00` is likewise a legal TIME (the top of a day, as a duration) and must still bind.
    backend
        .query(
            &mut conn,
            "INSERT INTO ferro_s7_coerce (id, tm) VALUES (?, ?)",
            &[Value::I64(702), Value::Time("24:00:00".into())],
        )
        .await
        .unwrap_or_else(|e| panic!("[{label}] `24:00:00` is a legal TIME and must bind: {e:?}"));
    let r = backend
        .query(
            &mut conn,
            "SELECT d, da, dt, ts FROM ferro_s7_coerce WHERE id = 700",
            &[],
        )
        .await
        .expect("read the sentinel row");
    assert_eq!(
        r.rows[0],
        vec![
            Value::Decimal("-12345.6700000000".into()),
            Value::Date("0000-00-00".into()),
            Value::Timestamp("0000-00-00 00:00:00".into()),
            Value::TimestampTz("0000-00-00 00:00:00".into()),
        ],
        "[{label}] the legal zero sentinels round-trip verbatim — the fix must not over-reject them"
    );
    let r = backend
        .query(
            &mut conn,
            "SELECT da, tm FROM ferro_s7_coerce WHERE id = 701",
            &[],
        )
        .await
        .expect("read the zero-in-date row");
    assert_eq!(r.rows[0][0], Value::Date("2026-00-05".into()));
    assert_eq!(r.rows[0][1], Value::Time("26:00:00".into()));
    let r = backend
        .query(
            &mut conn,
            "SELECT tm FROM ferro_s7_coerce WHERE id = 702",
            &[],
        )
        .await
        .expect("read the 24:00:00 row");
    assert_eq!(r.rows[0][0], Value::Time("24:00:00".into()));
    println!(
        "  [{label}] the legal sentinels + 24:00:00/26:00:00 TIME still bind under sql_mode=''"
    );

    conn.mysql.disconnect().await.ok();
}

// ---------------------------------------------------------------------------------------------
// Entry points: every body above runs against BOTH engines, each skipping cleanly without its URL.
// ---------------------------------------------------------------------------------------------

macro_rules! both_engines {
    ($body:ident, $mysql_name:ident, $mariadb_name:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $mysql_name() {
            let Some(url) = mysql_url() else { return };
            $body(&url, "MYSQL").await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $mariadb_name() {
            let Some(url) = mariadb_url() else { return };
            $body(&url, "MARIADB").await;
        }
    };
}

both_engines!(
    types_round_trip_exact_canonical_text,
    mysql_types_round_trip_exact_canonical_text,
    mariadb_types_round_trip_exact_canonical_text
);
both_engines!(
    bigint_unsigned_covers_both_driver_forms,
    mysql_bigint_unsigned_covers_both_driver_forms,
    mariadb_bigint_unsigned_covers_both_driver_forms
);
both_engines!(
    timestamp_is_a_utc_instant_and_datetime_is_naive,
    mysql_timestamp_is_a_utc_instant_and_datetime_is_naive,
    mariadb_timestamp_is_a_utc_instant_and_datetime_is_naive
);
both_engines!(
    time_spans_the_full_signed_range,
    mysql_time_spans_the_full_signed_range,
    mariadb_time_spans_the_full_signed_range
);
both_engines!(
    decimal_matches_the_servers_own_rendering,
    mysql_decimal_matches_the_servers_own_rendering,
    mariadb_decimal_matches_the_servers_own_rendering
);
both_engines!(
    json_classifies_per_engine,
    mysql_json_classifies_per_engine,
    mariadb_json_classifies_per_engine
);
both_engines!(
    zero_dates_render_as_the_verbatim_sentinel,
    mysql_zero_dates_render_as_the_verbatim_sentinel,
    mariadb_zero_dates_render_as_the_verbatim_sentinel
);
both_engines!(
    deferred_column_types_are_refused_before_execution,
    mysql_deferred_column_types_are_refused_before_execution,
    mariadb_deferred_column_types_are_refused_before_execution
);
both_engines!(
    expression_columns_classify_off_metadata_alone,
    mysql_expression_columns_classify_off_metadata_alone,
    mariadb_expression_columns_classify_off_metadata_alone
);
both_engines!(
    bind_round_trip_is_byte_identical,
    mysql_bind_round_trip_is_byte_identical,
    mariadb_bind_round_trip_is_byte_identical
);
both_engines!(
    non_canonical_payloads_are_rejected_pre_send_under_permissive_sql_mode,
    mysql_non_canonical_payloads_are_rejected_pre_send_under_permissive_sql_mode,
    mariadb_non_canonical_payloads_are_rejected_pre_send_under_permissive_sql_mode
);

/// MariaDB-only (the types do not exist on MySQL 8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_extended_types_are_text_by_design() {
    let Some(url) = mariadb_url() else { return };
    mariadb_extended_types_classify_as_text(&url, "MARIADB").await;
}
