# Ferro M1-S7 — Canonical Type Coverage (§9/§9.1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the eight DBAL-critical canonical tags — `U64`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `JSON` — from "registry constants only" to working end-to-end on both PostgreSQL and MySQL/MariaDB, in both directions (read *and* bind), with the four §9.1 pool-level policies, so the S8 Doctrine tier and the S9 exit-gate suite stand on real type support instead of a loud `Unsupported`.

**Architecture:** The wire stays a `[tag, payload]` MessagePack pair and every new payload is **text-canonical** (msgpack `str`), except `U64` which rides the msgpack uint family. The engine renders each backend's native binary form into that canonical text losslessly; the PHP client's `ValuePolicy` seam turns canonical text into the §9 value objects (or, for the S8 DBAL tier, leaves it as driver-native strings). Both backends keep their existing single-classifier discipline — PG's `oid_extract_type` and MySQL's `column_kind` remain the ONE authority backing both the `ColMeta` tag and the cell extraction, so `cols` and `rows` can never disagree.

**Tech Stack:** Rust (edition 2024, tokio) for `ferro-proto` / `ferro-backend-{pg,mysql}` / `ferrod`; PHP ≥ 8.2 (dependency-free) for `ferro/client`; `/proto` TOML registry + JSON golden vectors as the cross-language lock.

---

## Global Constraints

Every task's requirements implicitly include this section. These are copied verbatim from the charter, the spec, and the S7 grounding research — each one is a real hazard that was verified against the code.

### Contract rules (non-negotiable)

- **Charter rule 2 — `/proto` is the single source of truth.** Any protocol change updates the registry, the golden vectors, and **both** codecs (Rust + PHP) **in the same change set**. Hand-written protocol constants anywhere are a defect.
- **Charter rule 3 — the engine never transparently retries** a user statement. Nothing in this slice may add a retry.
- **Charter rule 4 — every in-flight request terminates in exactly one `END` frame.** A new-type decode failure must not change that.
- **Charter rule 6 — no silent miscasts.** An out-of-scope type stays a **loud `PoolError::Unsupported` naming the column and its native type**. This is the whole point of §9.1 "policies over guesses".
- **Charter rule 7 — the PHP client stays runtime-dependency-free.** No new composer runtime requirement. `ext-msgpack`/`ext-sockets` stay optional and runtime-detected.
- **SPEC §9.1 decode-mismatch rule:** a cell whose driver value does not match its column kind is a client-side decode mismatch → `PoolError::Backend` (NonRetryable), **NEVER** `ConnectionLost` — so a decode bug can never mint a false §19.3 `Indeterminate`.

### The wire payload contract (USER-DECIDED, pin it in `proto/PROTOCOL.md` §3)

Text-canonical. Chosen because PHP's `PurePacker` cannot decode msgpack **maps or ext types at all** (`php/client/src/Protocol/PurePacker.php:110` throws on every fixmap/map16/map32/ext marker, and `proto/PROTOCOL.md` §2 bans ext types outright), and because `str` and `bin` are **indistinguishable** in PHP after unpack (the tag is the only discriminator), so a `bin` payload would need a `list<int>` special case *and* could not round-trip through the golden-vector JSON `message` field.

| Tag | # | msgpack family | Canonical payload | Notes |
|---|---|---|---|---|
| `U64` | 3 | uint | unsigned 64-bit integer | The ONLY non-`str` addition. See the U64 hazard below. |
| `DECIMAL` | 5 | `str` | `"-12345.6700"` — full precision, **display scale preserved** | `"NaN"`, `"Infinity"`, `"-Infinity"` are legal payloads (PG NUMERIC allows them). `1.10` and `1.1` are **distinct** payloads. |
| `DATE` | 8 | `str` | `"YYYY-MM-DD"` | `"infinity"` / `"-infinity"` for the PG sentinels. |
| `TIME` | 9 | `str` | `"HH:MM:SS"` or `"HH:MM:SS.ffffff"` | Hours may exceed 23 (PG `time '24:00:00'`; MySQL `TIME` spans ±838h and may be negative → a leading `-`). |
| `TIMESTAMP` | 10 | `str` | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` | **Naive** — no zone suffix, ever. |
| `TIMESTAMPTZ` | 11 | `str` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` | RFC3339, **always normalized to UTC**, always the literal `Z`. |
| `UUID` | 12 | `str` | 36-char canonical **lowercase** hyphenated | Never raw bytes (see the `bin` hazard). |
| `JSON` | 13 | `str` | the raw UTF-8 JSON document text | Not re-serialized, not validated by the engine; PHP decodes lazily. |

Fractional seconds: emit **no** `.ffffff` group when the sub-second part is zero; otherwise emit exactly 6 digits. Never emit trailing-zero-trimmed variants — the payload must be byte-stable for the golden vectors.

**Still deferred (must remain a loud `Unsupported`, documented in §22):** `ARRAY`(14), `INTERVAL`(15), `INET`(16), `VECTOR`(17) — PG-exotic, not required for a green DBAL suite. Also explicitly out of scope and still `Unsupported`: PG `TIMETZ`, PG enums/domains/composites/ranges, MySQL `YEAR`/`BIT`/`ENUM`/`SET`/`GEOMETRY`/`VECTOR`.

### Version skew (USER-DECIDED)

The implemented-tag set becomes **part of the hashed registry**, so `TYPE_REGISTRY_HASH` changes and an engine/client pair with different type coverage fails **fast at the handshake** with a clear registry-mismatch error, instead of throwing a confusing `ProtocolException` mid-query on the first `DECIMAL` row. Both hash implementations must agree byte-for-byte: the Rust `build.rs` `fnv1a_hex` and `proto/gen-php.php`'s limb-based implementation (`gen-php.php:8-31`).

### Verified hazards — a naive implementation is WRONG

**Rust codec**
1. **The `Value` codec is NOT generic over the tag.** `ferro_proto::value::Value` is a closed 6-variant enum; `Value::decode` (`engine/crates/ferro-proto/src/value.rs:50`) ends in `other => Err(CodecError::Malformed(...))`. Eight new variants + arms in `tag()` (`:23`), `encode` (`:34`) and `decode` (`:50`) are **mandatory**, and they cascade into exhaustive matches at `ferro-backend-pg/src/bind.rs:42` (`value_to_boxed`), `:67` (`accepts`), `:80` (`value_kind`), the MySQL `bind.rs` equivalents, `ferrod/src/services/sql.rs:1090` (`estimate_row_bytes`), `ferro-e2e/src/main.rs`, and `gen_vectors.rs` (`v_json`). `clippy -D warnings` will catch each — that is the guard, not a reason to add a `_ =>` catch-all.
2. **Reuse `read_str`/`read_bin`, never a hand-rolled reader.** They call `bound_len(len, remaining)` (`value.rs:122`) which rejects a lying length prefix *before* allocating. Regression test: `engine/crates/ferro-proto/tests/value.rs:85 lying_length_prefix_is_rejected_before_allocating`.
3. **The tag byte must stay a bare positive fixint.** `encode` uses `enc::write_pfix` and `decode` uses `dec::read_pfix` (`value.rs:37`, `:58`). Correct for 0..=17; keep the invariant documented rather than "fixing" it to a generic int read (which would let a non-canonical tag encoding through).
4. **`estimate_row_bytes` feeds the streaming batch-size bound.** A careless `_ => 9` would mis-size `DECIMAL`/`JSON` payloads and could push a DATA frame past `MAX_FRAME_PAYLOAD`. Every new variant needs a real length-proportional estimate.

**Golden vectors**
5. **There is NO completeness guard that a tag has a vector.** The only positive-side assertion is `assert!(count >= 7)` (`engine/crates/ferro-proto/tests/golden_vectors.rs:35`) against 21 existing vectors — permanently satisfied. The *negative* side has an explicit required-name list (`:175-185`); the positive side does not. **This task adds one**, or a tag can ship with zero cross-language byte lock and every test still passes.
6. **PHP byte-lock coverage is keyed on vector NAME PREFIX.** `VectorConformanceTest::sqlVectors()` matches `sql_exec_`, `streamVectors()` matches `stream_head_`/`stream_data_` (`php/client/tests/Conformance/VectorConformanceTest.php:102`, `:161`). A vector named e.g. `typedvalue_decimal.json` gets only the generic header/unpack tests and **no byte lock** — silently half-covered. **Name new vectors `sql_exec_response_*` / `stream_data_*`.**
7. **`VectorConformanceTest::hasBigUint` (`:364-377`) SKIPS a whole vector** when it finds any digits-only string exceeding `PHP_INT_MAX`. A `DECIMAL` literal that is a bare ≥20-digit integer string would spuriously skip the test — a false green. Choose DECIMAL vector literals containing `.` or `-`, or ≤19 digits.
8. **A large `U64` must be rendered as a decimal STRING in the vector JSON** — a JSON number > 2^53 is lossy through PHP `json_decode`. The established convention is already in the generator (`gen_vectors.rs:199` hard-codes `"boot_epoch":"18446744073709551600"`).
9. **There is no regenerate-and-diff guard for vectors** (unlike the two registry guards). A hand-written vector JSON that is not also in `gen_vectors.rs` survives forever and silently diverges. Every new vector goes through the generator.

**PostgreSQL**
10. **`postgres-types` has NO `NUMERIC` `FromSql` under any feature**, and `postgres-protocol` has no numeric decoder. `DECIMAL` must be hand-decoded from the base-10000 binary format. Do **not** route it through `rust_decimal`: a 96-bit mantissa (~28 digits) cannot hold PG's 131 072 integral digits, cannot represent `NaN`, and normalizing through any decimal type **loses the display scale** (`1.10` → `1.1`), which breaks DBAL string comparisons.
11. **`TIMESTAMP` and `TIMESTAMPTZ` share an identical 8-byte payload.** Only the column OID separates naive-local from UTC-instant. Do **not** use the featureless `SystemTime` `FromSql` — its `accepts!(TIMESTAMP, TIMESTAMPTZ)` erases the distinction by construction.
12. **The PG epoch is 2000-01-01, not 1970-01-01** — off by 946 684 800 s / 10 957 days. A Unix-epoch assumption yields a plausible-looking wrong date, not a crash.
13. **Infinity sentinels:** `DATE` ±infinity are `i32::MAX`/`i32::MIN`; `TIMESTAMP`/`TIMESTAMPTZ` ±infinity are `i64::MAX`/`i64::MIN`. Handle them explicitly (payload `"infinity"`/`"-infinity"`), never arithmetically.
14. **PG `time '24:00:00'` is legal** (86 400 000 000 µs). chrono's `NaiveTime` addition **wraps** it to `00:00:00` (`chrono_04.rs:136`). Hand-roll `i64 µs → "HH:MM:SS[.ffffff]"` so hours may exceed 23.
15. **`TIMETZ` (OID 1266) has no `FromSql` under any feature** and its payload is 12 bytes (i64 µs + i32 zone), so `time_from_sql` rejects it. Keep it explicitly `Unsupported` — never let it fall into the `TIME` arm.
16. **A raw-bytes `FromSql` with `accepts(_) -> true` DEFEATS tokio-postgres' own type check.** `oid_extract_type` must remain the sole authority; never call the raw getter without first passing the OID gate.
17. **Result format is BINARY and is NOT per-statement selectable** — `Some(1)` is hardcoded in the vendored fork (`vendor/tokio-postgres/src/query.rs:324`). Any "just request text format" approach needs a *second* fork divergence and is out of scope. (Asymmetry: *param* format IS per-param selectable via `ToSql::encode_format`.)
18. **Two `Unsupported` gates must move in lockstep.** cols-build runs pre-execution with the conn still clean (`ferro-backend-pg/src/query.rs:67` buffered, `:172` streaming); the per-cell gate fires **mid-stream after HEAD is already on the wire** (`:108`, `:245`). Adding a type to `oid_extract_type` but not `extract_value` yields a HEAD promising a tag the producer cannot fill.
19. **`bind::accepts` (`bind.rs:67`) is the §19.3 known-fate pre-flight and MUST mirror `value_to_boxed` (`:42`) arm-for-arm.** A variant with a `value_to_boxed` arm but no `accepts` arm turns valid binds into false known-fate rejections; the reverse lets `to_sql_checked` fail *post-send*, which is exactly the false-`Indeterminate` path the pre-validation exists to prevent.
20. **Pure-OID matching misses domains/enums/composites** (they arrive with custom OIDs via `Kind::Domain(inner)` etc.). They stay `Unsupported` in S7 — do not attempt `kind()` unwrapping in this slice; note it in §22.
21. **Pin `postgres-protocol` to the exact version the fork resolves (0.6.12, `Cargo.lock`).** A mismatched minor gives two distinct crates whose traits will not unify, and `deny.toml`'s `multiple-versions = "allow"` will NOT catch it.

**MySQL/MariaDB**
22. `DECIMAL`/`NEWDECIMAL` arrive as `MyValue::Bytes` holding the server's own ASCII rendering — that text **is** the canonical payload (display scale already preserved). Do not parse it into a numeric type and re-render.
23. **Only `BIGINT UNSIGNED` needs `U64`.** Unsigned `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT` all fit `i64` losslessly and should map to `I64` — this deliberately narrows the `U64` surface. (§9's table lists U64 against "bigint unsigned".)
24. **MySQL `DATETIME` is naive → `TIMESTAMP`(10); MySQL `TIMESTAMP` is UTC-normalized by the server → `TIMESTAMPTZ`(11).** This matches §9's table exactly (`TIMESTAMP | timestamp | datetime` / `TIMESTAMPTZ | timestamptz | timestamp`). Getting these two backwards is a silent zone shift.
25. **MySQL has no native UUID column type.** `BINARY(16)` stays `BYTES` and `CHAR(36)` stays `TEXT`; nothing maps to the `UUID` tag on MySQL. Document this asymmetry rather than guessing.
26. **`MyValue::Time` carries `(is_negative, days, hours, minutes, seconds, micros)`** — a MySQL `TIME` may be negative and may exceed 24 h. Render the sign and fold `days` into hours.
27. **Zero dates (`'0000-00-00'`, `'0000-00-00 00:00:00'`) are legal in MySQL** unless `NO_ZERO_DATE` is set, and arrive with `year=0`. They are not representable as a real date — surface them as the literal canonical text (`"0000-00-00"`), not as an error, and cover them in a test.

**PHP client**
28. **`U64` arrives as an int OR a decimal string depending on MAGNITUDE, not type.** `PurePacker::be()` (`PurePacker.php:154-166`) returns a decimal **string** for *every* `0xcf`-marked uint64, while rmp's narrowing ladder emits `0xcc/0xcd/0xce` for anything ≤ `0xffffffff`. So `5` arrives as `int 5` but `2^33` arrives as a **string**. A naive `is_int($data)` branch mishandles the whole 2^32..2^64 range — the policy must normalize **both** forms and compare against `PHP_INT_MAX` itself.
29. **Encoding a `U64` must use `packUint(int|string)`, never `packInt(int)`** — `packInt` physically cannot emit > `PHP_INT_MAX`, and `Protocol/Value.php:23` currently calls `packInt(self::toInt(...))`. A naive `TAG_U64 => $p->packInt(...)` arm is a data-corruption bug.
30. **Do NOT reuse the existing narrowing helpers.** `M0ValuePolicy::toInt/toFloat/toStr` return `0`/`0.0`/`''` for an unexpected payload and `bytesFromInts` returns `''` for a non-array (`M0ValuePolicy.php:40-78`); `SqlValueCodec`'s helpers behave identically (`SqlValueCodec.php:52-76`). Copying that idiom would turn a malformed `DECIMAL` into `Decimal('')` and a bad `TIMESTAMP` into epoch-zero — exactly the silent miscast §9.1 exists to prevent. **The M1 arms must throw `ProtocolException`.**
31. **The `ValuePolicy` seam is DECODE-ONLY.** Binding goes through an unrelated chokepoint: `ExecCodec::bindOne` (`ExecCodec.php:185-198`) throws for anything not null/bool/int/float/string, and below it `SqlValueCodec::encode` (`:28`) and `Protocol/Value.php` (`:27`) each throw for any tag > `BYTES`. Read-only support leaves writes broken; a DBAL suite binds `DateTime`s and decimals constantly. **Task 8 exists for this.**
32. **A decode-policy error must never look like a §19.3 fate signal.** It surfaces client-side inside `ExecCodec::decodeRow` — i.e. *after* the statement already succeeded, and on the streamed path after earlier rows were already yielded (`Connection.php:234-239` yields inside the loop; `stream()`'s `finally` then fires `abandonStream`, sending `CANCEL` + drain). Raise it in the `ProtocolException` family, matching the existing rationale at `ExecCodec.php:97-98`.
33. **`naive_datetime_zone: server` is NOT implementable client-side** — nothing on the wire carries the backend's session timezone (`HelloAck.php:24-30` has only `[engine_version, boot_epoch, features, pools, type_registry_hash]`). **S7 implements `utc` (default) and `error` only**; `server` is deferred with a §22 note, since it needs the `HELLO_ACK` pool metadata that is already an S8 carry.
34. **Passing both `codec:` and `values:` to `Connection` silently DISCARDS `values:`** — `Connection.php:63` is `$this->codec = $codec ?? new ExecCodec($values ?? new M0ValuePolicy(), ...)`. Any policy plumbing that keeps the `codec:` escape hatch inherits this trap. Make it impossible or loud.
35. **Value objects break DTO hydration silently-loudly.** `ExecCodec::hydrateDto` calls `newInstanceArgs` with no coercion (`:167-168`), so a `Ferro\Decimal` fed to a `readonly string $amount` throws a bare `\TypeError` that escapes the `FerroException` contract. Cover the native-API DTO path explicitly.
36. **PHPStan L9 runs over `src` only** (`phpstan.neon.dist`), no baseline. `ValuePolicy::decode` returns `mixed`, so constructing a value object from `mixed $data` needs explicit `is_string`/`is_int` guards (a bare cast is the lossy anti-pattern above). Tests are unanalyzed — an L9 violation in a test helper will not be caught by the gate.

### Definition of done (charter DoD, every task)

- `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` (green **offline** — live tests skip, never fail, when `FERRO_TEST_PG_URL` / `FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL` are unset).
- `(cd php/client && ./vendor/bin/phpunit)` green; `./vendor/bin/phpstan analyse src --level 9` clean.
- Protocol work adds/updates golden vectors **and** both codecs in the same commit.
- The relevant SPEC section still tells the truth; a forced deviation is amended in the spec text **plus** a §22 line in the same change.

### Live test environment

```
docker compose -f testkit/docker-compose.yml up -d
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro
```

---

## File Structure

**Created**
- `engine/crates/ferro-backend-pg/src/pgtext.rs` — the hand-rolled PG binary → canonical-text decoders (numeric base-10000, date/time/timestamp from the 2000 epoch, uuid hex, json passthrough) + a raw-bytes `FromSql` newtype. Isolated in its own file because it is pure, table-driven byte math with the densest unit-test surface in the slice.
- `engine/crates/ferro-backend-mysql/src/mytext.rs` — the MySQL `MyValue` → canonical-text renderers (date/time/timestamp formatting, negative/overlong `TIME`, zero dates).
- `php/client/src/Value/{Decimal,Date,Time,Uuid,Json,U64}.php` — the §9 value objects (namespace `Ferro\`, i.e. `Ferro\Decimal`).
- `php/client/src/Client/Value/M1ValuePolicy.php` — the M1 policy implementing all 14 tags + the §9.1 knobs.
- `php/client/src/Client/Value/TypePolicyOptions.php` — the §9.1 policy value object (`decimal`, `naive_datetime_zone`, `u64_overflow`, `uuid`).
- `engine/crates/ferro-backend-pg/tests/pg_types_it.rs`, `engine/crates/ferro-backend-mysql/tests/mysql_types_it.rs` — live per-type round-trip acceptance.
- `php/client/tests/Unit/M1ValuePolicyTest.php`, `php/client/tests/Unit/ValueObjectsTest.php`.

**Modified**
- `proto/PROTOCOL.md` §3 — the payload-family table above (the wire contract).
- `proto/types.toml` — replace the dead `m0_scalar` key with a real, lock-feeding `implemented` list.
- `engine/crates/ferro-proto/src/value.rs` — 8 new `Value` variants + `tag()`/`encode()`/`decode()` arms.
- `engine/crates/ferro-proto/src/registry.rs` + the hash generators (`build.rs`, `proto/gen-php.php`) — hash the implemented set.
- `engine/crates/ferro-proto/tests/{value.rs,golden_vectors.rs,registry_sync.rs}` + `engine/crates/ferro-proto/src/bin/gen_vectors.rs`.
- `engine/crates/ferro-backend-pg/src/{rowmap.rs,bind.rs,query.rs}`; `engine/crates/ferro-backend-mysql/src/{rowmap.rs,bind.rs,query.rs}`.
- `engine/crates/ferrod/src/services/sql.rs` (`estimate_row_bytes`); `engine/crates/ferro-e2e/src/main.rs`.
- `php/client/src/Protocol/{Value.php,SqlValueCodec.php,Constants.php}`; `php/client/src/Client/{ExecCodec.php,Connection.php}`; `php/client/src/Ferro.php`.
- `ferro-spec-v0.2.md` §9/§9.1/§22.2.

---

## Task 1: Pin the wire contract + widen the Rust TypedValue codec

**Files:**
- Modify: `proto/PROTOCOL.md` (§3 payload table, currently lines 91-108)
- Modify: `engine/crates/ferro-proto/src/value.rs:12` (enum), `:23` (`tag`), `:34` (`encode`), `:50` (`decode`)
- Modify (compile-cascade only): `engine/crates/ferro-backend-pg/src/bind.rs:42,67,80`; `engine/crates/ferro-backend-mysql/src/bind.rs` (same three fns); `engine/crates/ferrod/src/services/sql.rs:1090`; `engine/crates/ferro-e2e/src/main.rs`; `engine/crates/ferro-proto/src/bin/gen_vectors.rs` (`v_json`)
- Test: `engine/crates/ferro-proto/tests/value.rs`

**Interfaces:**
- Produces: `ferro_proto::value::Value::{U64(u64), Decimal(String), Date(String), Time(String), Timestamp(String), TimestampTz(String), Uuid(String), Json(String)}`. Every text variant holds the **canonical payload string** — the backends are responsible for producing it; the codec does no validation beyond UTF-8.
- Consumes: `ferro_proto::consts::tag::{U64,DECIMAL,DATE,TIME,TIMESTAMP,TIMESTAMPTZ,UUID,JSON}` — already generated, no registry change needed for the tag numbers.

**Design note for the implementer:** the eight text variants are deliberately `String`, not parsed types. The canonical text *is* the wire contract; giving the codec a `chrono::NaiveDate` would move the formatting decision out of the backend (where the source format is known) into the codec (where it is not), and would drag a date dependency into `ferro-proto`, which today has none.

- [ ] **Step 1: Write the failing codec tests**

Add to `engine/crates/ferro-proto/tests/value.rs`:

```rust
#[test]
fn s7_text_tags_roundtrip() {
    let cases = vec![
        Value::U64(u64::MAX),
        Value::U64(0),
        Value::Decimal("-12345.6700".into()),
        Value::Decimal("NaN".into()),
        Value::Date("2026-08-05".into()),
        Value::Date("-infinity".into()),
        Value::Time("24:00:00".into()),
        Value::Time("-838:59:58.000001".into()),
        Value::Timestamp("2026-08-05 13:45:07.250000".into()),
        Value::TimestampTz("2026-08-05T13:45:07.250000Z".into()),
        Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
        Value::Json(r#"{"a":[1,2],"b":null}"#.into()),
    ];
    for v in cases {
        let mut buf = Vec::new();
        v.encode(&mut buf);
        let mut rd = &buf[..];
        let got = Value::decode(&mut rd).expect("decodes");
        assert_eq!(got, v, "roundtrip mismatch");
        assert!(rd.is_empty(), "trailing bytes left for {v:?}");
    }
}

#[test]
fn s7_tags_report_their_registry_tag() {
    use ferro_proto::consts::tag;
    assert_eq!(Value::U64(1).tag(), tag::U64);
    assert_eq!(Value::Decimal("1".into()).tag(), tag::DECIMAL);
    assert_eq!(Value::Date("2026-01-01".into()).tag(), tag::DATE);
    assert_eq!(Value::Time("00:00:00".into()).tag(), tag::TIME);
    assert_eq!(Value::Timestamp("2026-01-01 00:00:00".into()).tag(), tag::TIMESTAMP);
    assert_eq!(Value::TimestampTz("2026-01-01T00:00:00Z".into()).tag(), tag::TIMESTAMPTZ);
    assert_eq!(Value::Uuid("00000000-0000-0000-0000-000000000000".into()).tag(), tag::UUID);
    assert_eq!(Value::Json("null".into()).tag(), tag::JSON);
}

/// The still-deferred tags MUST stay rejected — this is the §22 deferral, enforced.
#[test]
fn deferred_tags_are_still_rejected() {
    use ferro_proto::consts::tag;
    for t in [tag::ARRAY, tag::INTERVAL, tag::INET, tag::VECTOR] {
        let mut buf = Vec::new();
        rmp::encode::write_array_len(&mut buf, 2).unwrap();
        rmp::encode::write_pfix(&mut buf, t).unwrap();
        rmp::encode::write_nil(&mut buf).unwrap();
        let mut rd = &buf[..];
        assert!(Value::decode(&mut rd).is_err(), "tag {t} must still be unsupported");
    }
}

/// Hazard 2: every new str-payload tag must inherit the bounds discipline.
#[test]
fn s7_str_tags_reject_a_lying_length_prefix() {
    use ferro_proto::consts::tag;
    for t in [tag::DECIMAL, tag::DATE, tag::TIME, tag::TIMESTAMP, tag::TIMESTAMPTZ, tag::UUID, tag::JSON] {
        // str32 claiming 4 GiB with no bytes behind it.
        let buf = vec![0x92, t, 0xdb, 0xff, 0xff, 0xff, 0xff];
        let mut rd = &buf[..];
        assert!(Value::decode(&mut rd).is_err(), "tag {t} must reject a lying length");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-proto --test value -- s7_ deferred_tags`
Expected: FAIL — `no variant named U64 found for enum Value`.

- [ ] **Step 3: Add the enum variants**

In `engine/crates/ferro-proto/src/value.rs`, extend the enum (keep the M0 six first so their discriminants and Debug output are unchanged):

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    // ---- M1-S7: canonical type coverage. Each text variant holds the CANONICAL
    // payload string defined in proto/PROTOCOL.md §3; the backend produces it, the
    // codec only moves it. U64 is the one non-str addition (msgpack uint family).
    U64(u64),
    Decimal(String),
    Date(String),
    Time(String),
    Timestamp(String),
    TimestampTz(String),
    Uuid(String),
    Json(String),
}
```

- [ ] **Step 4: Add the `tag()`, `encode` and `decode` arms**

`tag()`:

```rust
Value::U64(_) => tag::U64,
Value::Decimal(_) => tag::DECIMAL,
Value::Date(_) => tag::DATE,
Value::Time(_) => tag::TIME,
Value::Timestamp(_) => tag::TIMESTAMP,
Value::TimestampTz(_) => tag::TIMESTAMPTZ,
Value::Uuid(_) => tag::UUID,
Value::Json(_) => tag::JSON,
```

`encode()` — note `write_uint` (NOT `write_sint`) for U64 so the full range survives:

```rust
Value::U64(n) => { enc::write_uint(out, *n).unwrap(); }
Value::Decimal(s)
| Value::Date(s)
| Value::Time(s)
| Value::Timestamp(s)
| Value::TimestampTz(s)
| Value::Uuid(s)
| Value::Json(s) => enc::write_str(out, s).unwrap(),
```

`decode()` — insert before the `other =>` arm; every str tag goes through `read_str` so it inherits `bound_len` (hazard 2):

```rust
t if t == tag::U64 => Ok(Value::U64(
    dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("u64: {e:?}")))?,
)),
t if t == tag::DECIMAL => Ok(Value::Decimal(read_str(rd)?)),
t if t == tag::DATE => Ok(Value::Date(read_str(rd)?)),
t if t == tag::TIME => Ok(Value::Time(read_str(rd)?)),
t if t == tag::TIMESTAMP => Ok(Value::Timestamp(read_str(rd)?)),
t if t == tag::TIMESTAMPTZ => Ok(Value::TimestampTz(read_str(rd)?)),
t if t == tag::UUID => Ok(Value::Uuid(read_str(rd)?)),
t if t == tag::JSON => Ok(Value::Json(read_str(rd)?)),
```

`dec::read_int` is generic over the integer target; verify it yields `u64` here without a lossy intermediate. If it will not resolve to `u64`, use rmp's unsigned reader directly rather than casting through `i64` — a cast would corrupt values above `i64::MAX`, which is the entire reason `U64` exists.

- [ ] **Step 5: Run the codec tests**

Run: `cargo test -p ferro-proto --test value`
Expected: PASS (all four new tests + the existing suite).

- [ ] **Step 6: Fix the compile cascade — no `_ =>` catch-alls**

`cargo build --workspace` now fails on the exhaustive matches. Fix each properly:

- `ferro-backend-pg/src/bind.rs:42` `value_to_boxed`, `:67` `accepts`, `:80` `value_kind` — Task 8 implements real binding. **For this task**, give the eight new variants an explicit *known-fate rejection* in all three, keeping `accepts` and `value_to_boxed` mirror-image (hazard 19). Add a `// M1-S7 Task 8 implements binding for these` comment so it is not mistaken for the final state.
- `ferro-backend-mysql/src/bind.rs` — same treatment.
- `ferrod/src/services/sql.rs:1090` `estimate_row_bytes` — real length-proportional estimates (hazard 4):

```rust
Value::U64(_) => 9,
Value::Decimal(s)
| Value::Date(s)
| Value::Time(s)
| Value::Timestamp(s)
| Value::TimestampTz(s)
| Value::Uuid(s)
| Value::Json(s) => s.len() + 5, // str payload + worst-case str32 header + tag
```

- `ferro-e2e/src/main.rs` and `gen_vectors.rs` `v_json` — render the new variants; `v_json` must emit a large `U64` as a decimal **string** (hazard 8).

- [ ] **Step 7: Pin the wire contract in PROTOCOL.md §3**

Replace the "registry constants only" claim for these eight tags with the payload-family table from Global Constraints, verbatim, including the fractional-second rule, the legal `DECIMAL` special values, the `"infinity"` forms, and the still-deferred list. State explicitly that `str` payloads carry canonical text and that `bin` is not used by any S7 tag (with the PHP `str`/`bin` indistinguishability as the recorded reason).

- [ ] **Step 8: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add proto/PROTOCOL.md engine/crates/ferro-proto engine/crates/ferro-backend-pg/src/bind.rs \
        engine/crates/ferro-backend-mysql/src/bind.rs engine/crates/ferrod/src/services/sql.rs \
        engine/crates/ferro-e2e/src/main.rs
git commit -m "feat(m1-s7): pin text-canonical wire payloads for 8 canonical tags + widen the Rust TypedValue codec"
```

---

## Task 2: Golden vectors per tag + the PHP codec + a completeness guard

**Files:**
- Modify: `engine/crates/ferro-proto/src/bin/gen_vectors.rs`
- Create: `proto/vectors/sql_exec_response_types_*.json` (via the generator — never hand-written, hazard 9)
- Modify: `engine/crates/ferro-proto/tests/golden_vectors.rs:35` (add the positive completeness guard)
- Modify: `php/client/src/Protocol/Value.php:23,27`; `php/client/src/Protocol/SqlValueCodec.php:28,43`
- Test: `php/client/tests/Conformance/VectorConformanceTest.php`

**Interfaces:**
- Consumes: `Value::{U64,Decimal,…}` from Task 1.
- Produces: the cross-language byte lock every later task relies on, and `Ferro\Protocol\Value`/`SqlValueCodec` support for all 14 tags (the *codec* level — the *policy* level is Task 7).

- [ ] **Step 1: Write the failing completeness guard**

In `engine/crates/ferro-proto/tests/golden_vectors.rs`, replace the vacuous `assert!(count >= 7)` with a real per-tag requirement (hazard 5):

```rust
/// Every IMPLEMENTED tag must have at least one committed golden vector exercising it.
/// The old `count >= 7` assertion was permanently satisfied and locked nothing.
#[test]
fn every_implemented_tag_has_a_vector() {
    use ferro_proto::consts::tag;
    const REQUIRED: &[(u8, &str)] = &[
        (tag::NULL, "NULL"), (tag::BOOL, "BOOL"), (tag::I64, "I64"),
        (tag::F64, "F64"), (tag::TEXT, "TEXT"), (tag::BYTES, "BYTES"),
        (tag::U64, "U64"), (tag::DECIMAL, "DECIMAL"), (tag::DATE, "DATE"),
        (tag::TIME, "TIME"), (tag::TIMESTAMP, "TIMESTAMP"),
        (tag::TIMESTAMPTZ, "TIMESTAMPTZ"), (tag::UUID, "UUID"), (tag::JSON, "JSON"),
    ];
    let seen = tags_present_in_committed_vectors(); // decode every vectors/*.json, collect ColMeta+row tags
    for (t, name) in REQUIRED {
        assert!(seen.contains(t), "no golden vector exercises tag {name} ({t})");
    }
}
```

Implement `tags_present_in_committed_vectors()` by decoding each committed vector's bytes with the real codec and walking its `ColMeta` tags and row values — do not text-scan the JSON.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-proto --test golden_vectors every_implemented_tag`
Expected: FAIL — `no golden vector exercises tag U64 (3)`.

- [ ] **Step 3: Generate the vectors**

Extend `gen_vectors.rs` with `sql_exec_response_types_scalars` and `sql_exec_response_types_edge`, plus a `stream_data_types` so the streamed path is byte-locked too. **The `sql_exec_response_*` / `stream_data_*` prefixes are mandatory** — PHP's byte-lock is keyed on them (hazard 6).

Payload choices are constrained by real hazards:
- `U64`: use `18446744073709551615` rendered as a **decimal string** in the JSON (hazard 8).
- `DECIMAL`: use `"-12345.6700"` and `"NaN"` — both contain `.`/`-`, avoiding the `hasBigUint` false-skip (hazard 7). Do **not** use a bare ≥20-digit integer string.
- `TIME`: include `"24:00:00"` (PG-legal, chrono-hostile).
- `TIMESTAMP` vs `TIMESTAMPTZ`: include both so the naive/UTC distinction is byte-locked separately.
- `JSON`: a document with nesting, a `null`, and a non-ASCII character to prove UTF-8 survives.

Then regenerate and commit the bytes:

```bash
cargo run -p ferro-proto --bin gen-vectors
```

- [ ] **Step 4: Add the PHP codec arms**

`php/client/src/Protocol/Value.php` — encode/decode all 8. **`TAG_U64` must use `packUint`, never `packInt`** (hazard 29); `packUint` accepts `int|string` so a >`PHP_INT_MAX` value round-trips as a decimal string.

`php/client/src/Protocol/SqlValueCodec.php` — the new tags ride the msgpack `str` family, so `fromWire` needs **no** new special case (the `TAG_BYTES` → `list<int>` conversion at `:43` stays the only one, hazard by design). Confirm this in a comment: adding a `bin`-family tag later would require the same treatment plus a vector-JSON workaround.

- [ ] **Step 5: Run both conformance suites**

```bash
cargo test -p ferro-proto --test golden_vectors
(cd php/client && ./vendor/bin/phpunit --filter VectorConformance)
```
Expected: PASS both. Confirm the new vectors actually reached the byte-lock test — assert the count of byte-locked vectors went up, and verify `hasBigUint` did not silently skip the DECIMAL vector (temporarily `var_dump` the skip decision if unsure, then remove).

- [ ] **Step 6: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): golden vector per canonical tag (both codecs) + a real per-tag completeness guard"
```

---

## Task 3: Hash the implemented tag set (fail version skew at the handshake)

**Files:**
- Modify: `proto/types.toml:5` (replace the dead `m0_scalar` with a lock-feeding `implemented` list)
- Modify: `engine/crates/ferro-proto/src/registry.rs:30` (parse it — serde currently DROPS `m0_scalar`)
- Modify: the Rust hash generator (`build.rs` `fnv1a_hex`) and `proto/gen-php.php:8-31` (the limb-based implementation) — both must produce identical hex
- Modify: `proto/registry.lock.json`, `php/client/src/Protocol/Generated/Constants.php:113`
- Test: `engine/crates/ferro-proto/tests/registry_sync.rs`, `php/client/tests/Conformance/RegistrySyncTest.php`

**Interfaces:**
- Produces: a `TYPE_REGISTRY_HASH` that changes whenever the implemented-tag set changes, so `ferrod`'s handshake check rejects a skewed client immediately.

**Why:** all 18 tag *numbers* were already in the lock, so without this the hash does not move and an old M0 client passes the handshake, then throws `ProtocolException` mid-query on the first `DECIMAL` row (hazard: `M0ValuePolicy.php:33`). Note `m0_scalar` is currently **dead documentation** — `registry.rs:30-31` shows serde drops it, it is not in the lock, and no test reads it. This task makes it real.

- [ ] **Step 1: Write the failing tests**

```rust
// engine/crates/ferro-proto/tests/registry_sync.rs
#[test]
fn implemented_tag_set_is_part_of_the_hashed_registry() {
    let reg = Registry::from_toml_dir("../../../proto").expect("registry parses");
    // The implemented set is REAL (parsed), not dropped like the old m0_scalar key.
    assert!(reg.implemented_tags().contains(&"DECIMAL".to_string()));
    assert!(!reg.implemented_tags().contains(&"ARRAY".to_string()), "ARRAY is deferred in S7");
    // And it is load-bearing: perturbing it changes the hash.
    let base = reg.type_registry_hash();
    let mut perturbed = reg.clone();
    perturbed.remove_implemented_tag("DECIMAL");
    assert_ne!(base, perturbed.type_registry_hash(),
        "the implemented set must feed TYPE_REGISTRY_HASH so version skew fails at the handshake");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-proto --test registry_sync implemented_tag_set`
Expected: FAIL — no `implemented_tags()` on `Registry`.

- [ ] **Step 3: Make the implemented set real in `/proto`**

In `proto/types.toml`, replace `m0_scalar` (keeping the top-level-key placement comment, which matters for TOML parsing):

```toml
# The tags implemented END-TO-END (engine both backends + PHP client). This list FEEDS
# registry.lock.json and therefore TYPE_REGISTRY_HASH: changing it changes the hash, so an
# engine/client pair with different type coverage fails FAST at the handshake instead of
# throwing mid-query on the first row of a newly-supported type (M1-S7 decision).
# MUST precede the [tags] table so it is a TOP-LEVEL key, not absorbed as tags.implemented.
implemented = ["NULL", "BOOL", "I64", "U64", "F64", "DECIMAL", "TEXT", "BYTES",
               "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "UUID", "JSON"]
# Deferred (registry constants only; a loud NonRetryable{Unsupported}): ARRAY, INTERVAL, INET, VECTOR.
```

- [ ] **Step 4: Parse it, feed the lock, update both hash generators**

Add the field to the `Registry` deserialization (`registry.rs:30` — the struct that currently drops it), include it in the locked JSON in a **stable, sorted** order, and hash it in both generators. Verify byte-identical output:

```bash
cargo run -p ferro-proto --bin gen-registry   # or whatever the existing lock generator is
php proto/gen-php.php
git diff --stat proto/registry.lock.json php/client/src/Protocol/Generated/Constants.php
```

Both hashes must be the same 16 hex chars. If they differ, the limb-based PHP FNV-1a and the Rust one disagree on input framing — fix the *framing*, never by special-casing one side.

- [ ] **Step 5: Run both sync guards**

```bash
cargo test -p ferro-proto --test registry_sync
(cd php/client && ./vendor/bin/phpunit --filter RegistrySync)
```
Expected: PASS. `RegistrySyncTest` actually re-runs `gen-php.php` and diffs, so a stale `Constants.php` fails here.

- [ ] **Step 6: Prove the skew check bites**

Add a `ferrod` test asserting a handshake with a mismatched `type_registry_hash` is rejected (extend the existing handshake test rather than inventing a new harness). Expected: the client sees a clear registry-mismatch error, not a successful handshake.

- [ ] **Step 7: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): hash the implemented tag set so type-coverage skew fails at the handshake"
```

---

## Task 4: PG read path — canonical text from the binary protocol

**Files:**
- Create: `engine/crates/ferro-backend-pg/src/pgtext.rs`
- Modify: `engine/crates/ferro-backend-pg/src/rowmap.rs` (`oid_extract_type`, `oid_to_tag`, `extract_value`, `unsupported_oid:114`, the unit test at `:154`)
- Modify: `engine/crates/ferro-backend-pg/Cargo.toml` (add `postgres-protocol` pinned to the fork's exact version)
- Test: `engine/crates/ferro-backend-pg/src/pgtext.rs` unit tests + `engine/crates/ferro-backend-pg/tests/pg_types_it.rs` (live)

**Interfaces:**
- Consumes: `Value::{U64,…}` (Task 1).
- Produces: `pgtext::{numeric_to_text, date_to_text, time_to_text, timestamp_to_text, timestamptz_to_text, uuid_to_text, json_to_text}` — each `fn(&[u8]) -> Result<String, PoolError>` over the **raw binary payload**; plus a `RawBytes` `FromSql` newtype used ONLY behind the `oid_extract_type` gate.

**Critical:** `oid_extract_type` stays the sole type authority. The `RawBytes` newtype has `accepts(_) -> true`, which defeats tokio-postgres' own check (hazard 16) — so it must never be reachable without first passing the OID gate. Enforce it with a unit test.

`U64` does **not** apply to PG (no unsigned integer type in scope).

- [ ] **Step 1: Write the failing pure decoder tests**

These are pure byte math — the densest correctness surface in the slice. In `pgtext.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // NUMERIC is base-10000 with an explicit display scale. 1.10 and 1.1 are DISTINCT.
    #[test]
    fn numeric_preserves_display_scale() {
        assert_eq!(numeric_to_text(&num_bytes("1.10")).unwrap(), "1.10");
        assert_eq!(numeric_to_text(&num_bytes("1.1")).unwrap(), "1.1");
    }

    #[test]
    fn numeric_handles_special_values_and_huge_precision() {
        assert_eq!(numeric_to_text(&num_bytes("NaN")).unwrap(), "NaN");
        assert_eq!(numeric_to_text(&num_bytes("Infinity")).unwrap(), "Infinity");
        assert_eq!(numeric_to_text(&num_bytes("-Infinity")).unwrap(), "-Infinity");
        let big = format!("{}.{}", "9".repeat(200), "1".repeat(50));
        assert_eq!(numeric_to_text(&num_bytes(&big)).unwrap(), big, "no precision loss");
    }

    // The PG epoch is 2000-01-01, NOT 1970-01-01 (hazard 12).
    #[test]
    fn date_uses_the_postgres_epoch() {
        assert_eq!(date_to_text(&0i32.to_be_bytes()).unwrap(), "2000-01-01");
        assert_eq!(date_to_text(&(-10957i32).to_be_bytes()).unwrap(), "1970-01-01");
    }

    // Infinity sentinels are values, not arithmetic (hazard 13).
    #[test]
    fn date_and_timestamp_infinities_are_explicit() {
        assert_eq!(date_to_text(&i32::MAX.to_be_bytes()).unwrap(), "infinity");
        assert_eq!(date_to_text(&i32::MIN.to_be_bytes()).unwrap(), "-infinity");
        assert_eq!(timestamp_to_text(&i64::MAX.to_be_bytes()).unwrap(), "infinity");
        assert_eq!(timestamptz_to_text(&i64::MIN.to_be_bytes()).unwrap(), "-infinity");
    }

    // PG time '24:00:00' is legal and must NOT wrap to 00:00:00 (hazard 14).
    #[test]
    fn time_does_not_wrap_at_midnight() {
        assert_eq!(time_to_text(&86_400_000_000i64.to_be_bytes()).unwrap(), "24:00:00");
        assert_eq!(time_to_text(&0i64.to_be_bytes()).unwrap(), "00:00:00");
        assert_eq!(time_to_text(&1i64.to_be_bytes()).unwrap(), "00:00:00.000001");
    }

    // TIMESTAMP is naive, TIMESTAMPTZ is UTC with a Z — same 8 bytes, different rendering.
    #[test]
    fn timestamp_and_timestamptz_render_differently_from_identical_bytes() {
        let b = 0i64.to_be_bytes();
        assert_eq!(timestamp_to_text(&b).unwrap(), "2000-01-01 00:00:00");
        assert_eq!(timestamptz_to_text(&b).unwrap(), "2000-01-01T00:00:00Z");
    }

    #[test]
    fn uuid_is_canonical_lowercase_hyphenated() {
        let raw: [u8; 16] = [0x3F,0x2B,0x8C,0x1A,0,0,0x4F,0xFF,0x80,0,0xAB,0xCD,0xEF,0xAB,0xCD,0xEF];
        assert_eq!(uuid_to_text(&raw).unwrap(), "3f2b8c1a-0000-4fff-8000-abcdefabcdef");
    }

    // JSONB's binary payload is a 1-byte version prefix + the raw JSON text; JSON has no prefix.
    #[test]
    fn json_and_jsonb_both_yield_the_raw_document() {
        assert_eq!(json_to_text(br#"{"a":1}"#, false).unwrap(), r#"{"a":1}"#);
        assert_eq!(json_to_text(b"\x01{\"a\":1}", true).unwrap(), r#"{"a":1}"#);
    }
}
```

`num_bytes(&str)` is a test helper that builds the PG base-10000 wire form; write it in the test module so the decoder is tested against real payload shapes.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-backend-pg pgtext`
Expected: FAIL — module/functions do not exist.

- [ ] **Step 3: Implement `pgtext.rs`**

Hand-decode over the raw binary payloads. Reference the wire formats in `postgres-protocol`'s `src/types/mod.rs` (`date_from_sql`, `time_from_sql`, `timestamp_from_sql`, `uuid_from_sql`) for the integer extraction, but do the **formatting** yourself so hazards 12-15 stay closed. For NUMERIC, decode the `(ndigits, weight, sign, dscale, digits[])` header and render digits base-10000 into a decimal string honouring `dscale` exactly; handle the `sign` values for NaN/±Infinity.

Add `postgres-protocol` to `Cargo.toml` **pinned to the exact version the fork resolves** (check `Cargo.lock`, hazard 21).

- [ ] **Step 4: Run the decoder tests**

Run: `cargo test -p ferro-backend-pg pgtext`
Expected: PASS.

- [ ] **Step 5: Widen `rowmap.rs` — both gates in lockstep**

Extend `ExtractType` and `oid_extract_type` with `NUMERIC`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `JSON`, `JSONB`; extend `oid_to_tag` and `extract_value` in the **same** change (hazard 18 — a HEAD promising a tag the producer cannot fill is the failure mode). Keep `TIMETZ` out (hazard 15). Update `unsupported_oid:114` so its message no longer says "M0" and enumerates the *current* supported set.

Add a unit test that the two gates agree for every OID:

```rust
/// Hazard 18: cols-build and per-cell must never disagree about a column's type.
#[test]
fn both_gates_cover_exactly_the_same_oid_set() {
    for oid in ALL_KNOWN_OIDS {
        assert_eq!(
            oid_to_tag(oid).is_ok(),
            oid_extract_type(oid).is_some(),
            "gate disagreement for oid {oid}"
        );
    }
}

/// Hazard 16: the raw getter must be unreachable without the OID gate.
#[test]
fn timetz_and_deferred_oids_stay_unsupported() {
    for oid in [Type::TIMETZ.oid(), Type::INT4_ARRAY.oid(), Type::INTERVAL.oid(), Type::INET.oid()] {
        assert!(oid_extract_type(oid).is_none(), "oid {oid} must stay Unsupported in S7");
    }
}
```

Update the pre-S7 tests that assert the old world: `rowmap.rs:154 out_of_m0_oid_is_unsupported` (TIMESTAMPTZ/UUID/NUMERIC/JSONB are now supported) and the live `pg_query_it.rs:160 query_out_of_m0_column_is_unsupported` (`SELECT now()` now succeeds — repoint it at a genuinely deferred type such as `SELECT '1 day'::interval`).

- [ ] **Step 6: Write the live per-type round-trip test**

`engine/crates/ferro-backend-pg/tests/pg_types_it.rs` — skips (never fails) without `FERRO_TEST_PG_URL`. Create a temp table per type, insert via literal SQL, read back, assert the **exact canonical text**:

```rust
// Representative assertions — the full matrix covers every new tag.
("numeric(30,10)", "'-12345.6700000000'", Value::Decimal("-12345.6700000000".into())),
("numeric",        "'NaN'",               Value::Decimal("NaN".into())),
("date",           "'2026-08-05'",        Value::Date("2026-08-05".into())),
("date",           "'infinity'",          Value::Date("infinity".into())),
("time",           "'24:00:00'",          Value::Time("24:00:00".into())),
("timestamp",      "'2026-08-05 13:45:07.25'", Value::Timestamp("2026-08-05 13:45:07.250000".into())),
("timestamptz",    "'2026-08-05 13:45:07.25+02'", Value::TimestampTz("2026-08-05T11:45:07.250000Z".into())),
("uuid",           "'3F2B8C1A-0000-4FFF-8000-ABCDEFABCDEF'", Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into())),
("jsonb",          r#"'{"a":[1,2],"b":null}'"#, /* jsonb may reorder/normalize — assert a parse-equal, not byte-equal */),
```

Note the `timestamptz` case proves UTC normalization (`+02` → `11:45:07Z`) — set the session `TimeZone` to something non-UTC first so a "server just happens to be UTC" false green is impossible. For `jsonb`, assert semantic equality (PG normalizes jsonb key order/whitespace); for `json`, assert byte-exact passthrough.

- [ ] **Step 7: Run offline + live**

```bash
cargo test -p ferro-backend-pg                                   # offline: live tests skip
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
  cargo test -p ferro-backend-pg -- --nocapture
```
Expected: every case PASS; paste the per-type actual values into the task report.

- [ ] **Step 8: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git commit -m "feat(m1-s7): PG canonical-text type coverage (numeric/date/time/timestamp(tz)/uuid/json) with both gates in lockstep"
```

---

## Task 5: MySQL read path — canonical text from the binary protocol

**Files:**
- Create: `engine/crates/ferro-backend-mysql/src/mytext.rs`
- Modify: `engine/crates/ferro-backend-mysql/src/rowmap.rs` (`MyKind:43`, `column_kind:59`, `column_to_tag:121`, `extract_value:135`, `unsupported:191`)
- Test: `mytext.rs` unit tests + `engine/crates/ferro-backend-mysql/tests/mysql_types_it.rs` (live, BOTH engines)

**Interfaces:**
- Produces: `mytext::{date_to_text, datetime_to_text, timestamptz_to_text, time_to_text}` over `mysql_async::Value`'s date/time components, plus `MyKind::{U64, Decimal, Date, Time, Timestamp, TimestampTz, Json}`.

**The mapping (hazards 22-27):**

| MySQL column | `MyKind` | Tag | Note |
|---|---|---|---|
| `BIGINT UNSIGNED` | `U64` | `U64` | Only BIGINT — narrower unsigned fits `i64` |
| unsigned `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT` | `I64` | `I64` | **Lossless**, deliberately narrows the U64 surface |
| `DECIMAL`/`NEWDECIMAL` | `Decimal` | `DECIMAL` | Arrives as `Bytes` = the server's ASCII text; **pass through**, do not re-render |
| `DATE` | `Date` | `DATE` | |
| `DATETIME` | `Timestamp` | `TIMESTAMP` | **Naive** |
| `TIMESTAMP` | `TimestampTz` | `TIMESTAMPTZ` | Server-normalized **UTC** |
| `TIME` | `Time` | `TIME` | May be negative / exceed 24 h |
| `JSON` | `Json` | `JSON` | Arrives as `Bytes` = raw JSON text |
| `YEAR`, `BIT`, `ENUM`, `SET`, `GEOMETRY`, `VECTOR` | — | — | **Still a loud `Unsupported`** |
| `BINARY(16)` / `CHAR(36)` | `Bytes` / `Text` | unchanged | MySQL has no UUID type (hazard 25) |

- [ ] **Step 1: Write the failing renderer tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value as MyValue;

    #[test]
    fn datetime_is_naive_and_timestamp_is_utc_z() {
        let v = MyValue::Date(2026, 8, 5, 13, 45, 7, 250_000);
        assert_eq!(datetime_to_text(&v).unwrap(), "2026-08-05 13:45:07.250000");
        assert_eq!(timestamptz_to_text(&v).unwrap(), "2026-08-05T13:45:07.250000Z");
    }

    #[test]
    fn zero_sub_second_omits_the_fraction() {
        let v = MyValue::Date(2026, 8, 5, 13, 45, 7, 0);
        assert_eq!(datetime_to_text(&v).unwrap(), "2026-08-05 13:45:07");
    }

    // Hazard 27: MySQL zero-dates are legal and must surface as canonical text, not an error.
    #[test]
    fn zero_dates_render_literally() {
        assert_eq!(date_to_text(&MyValue::Date(0,0,0,0,0,0,0)).unwrap(), "0000-00-00");
        assert_eq!(datetime_to_text(&MyValue::Date(0,0,0,0,0,0,0)).unwrap(), "0000-00-00 00:00:00");
    }

    // Hazard 26: TIME is (is_negative, days, hours, minutes, seconds, micros) and may exceed 24h.
    #[test]
    fn time_handles_sign_and_days_overflow() {
        assert_eq!(time_to_text(&MyValue::Time(false, 0, 13, 45, 7, 0)).unwrap(), "13:45:07");
        assert_eq!(time_to_text(&MyValue::Time(true, 34, 22, 59, 58, 1)).unwrap(), "-838:59:58.000001");
        assert_eq!(time_to_text(&MyValue::Time(false, 1, 2, 0, 0, 0)).unwrap(), "26:00:00");
    }
}
```

Verify the `MyValue::Time` field order against the vendored `mysql_common` enum definition before relying on it; correct the test if the real layout differs, and record the actual layout in a doc comment.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p ferro-backend-mysql mytext` → FAIL.

- [ ] **Step 3: Implement `mytext.rs`** — pure formatting over the driver's already-parsed components. No date library.

- [ ] **Step 4: Run the renderer tests** → PASS.

- [ ] **Step 5: Widen `rowmap.rs`**

Extend `MyKind` and `column_kind` per the table. `column_kind` remains the ONE classifier backing both `column_to_tag` and `extract_value` (the file's existing discipline — preserve it). Split the unsigned arm so only `MYSQL_TYPE_LONGLONG + UNSIGNED_FLAG` reaches `U64`. Update `unsupported:191` so its message enumerates the current supported set instead of "only NULL/BOOL/I64/F64/TEXT/BYTES are supported in M1".

Add the gate-agreement test (mirroring Task 4's) and update the existing unit tests that pin the old `Unsupported` behavior for these types.

- [ ] **Step 6: Write the live round-trip test (BOTH engines)**

`mysql_types_it.rs`, gated separately on `FERRO_TEST_MYSQL_URL` and `FERRO_TEST_MARIADB_URL` (the S6 pattern — two test fns calling one shared body). Cover:

- `DECIMAL(30,10)` — `-12345.6700000000` with the trailing zeros **preserved**.
- `BIGINT UNSIGNED` — `18446744073709551615` (> `i64::MAX`, the whole reason `U64` exists).
- `DATETIME(6)` vs `TIMESTAMP(6)` — fractional seconds preserved; and prove the naive/UTC split by setting a non-UTC session `time_zone` and asserting `DATETIME` is unshifted while `TIMESTAMP` comes back as `Z`.
- `TIME(6)` — a negative value and one exceeding 24 h.
- `JSON` — semantic equality (MySQL normalizes JSON).
- `DATE` — a zero date, if the server's `sql_mode` permits (skip that case with an explicit log if `NO_ZERO_DATE` is on; do not silently pass).
- Still-`Unsupported`: `YEAR`, `BIT(8)`, an `ENUM`, a `SET`.

- [ ] **Step 7: Run offline + live on both engines**

```bash
cargo test -p ferro-backend-mysql
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test -p ferro-backend-mysql -- --nocapture
```
Expected: PASS on both; paste per-type actuals for MySQL **and** MariaDB (record any divergence).

- [ ] **Step 8: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git commit -m "feat(m1-s7): MySQL/MariaDB canonical-text type coverage (u64/decimal/date family/json) on the column-metadata classifier"
```

---

## Task 6: §9.1 policy plumbing

**Files:**
- Modify: `engine/crates/ferro-pool/src/config.rs:11` (`PoolConfig`)
- Modify: `engine/crates/ferrod/src/config.rs` (env parsing), `engine/crates/ferrod/src/pools.rs:62`
- Create: `php/client/src/Client/Value/TypePolicyOptions.php`
- Modify: `php/client/src/Ferro.php`, `php/client/src/Client/Connection.php:63`

**Interfaces:**
- Produces: `TypePolicyOptions { decimal: 'object'|'string', naive_datetime_zone: 'utc'|'error', u64_overflow: 'object'|'string'|'error', uuid: 'object'|'string' }` with the **safe object forms as defaults** (§9.1).

**Key insight — where each policy lives.** Because the wire is text-canonical, three of the four policies are pure **client-side presentation** choices (`decimal`, `uuid`, `u64_overflow`): the engine always sends lossless canonical text and the client decides what PHP type to hand back. Only `naive_datetime_zone` has an engine component, and its `server` variant is **not implementable** without the backend session timezone on the wire (hazard 33) — so **S7 ships `utc` (default) and `error`; `server` is deferred to S8** with the `HELLO_ACK` pool metadata it needs, recorded in §22.

This means the engine-side plumbing in this task is deliberately thin: a `PoolConfig` home for the knobs so they are configured per pool (§9.1 says pool-level), even though S7's decode-time application is client-side. Do not thread policy arguments through every `rowmap` signature — that would be churn for no behavior, since the canonical text is policy-independent by design. Record that rationale in a code comment so a later reader does not "fix" it.

- [ ] **Step 1: Write the failing tests**

```php
// php/client/tests/Unit/TypePolicyOptionsTest.php
public function testDefaultsAreTheSafeObjectForms(): void
{
    $p = new TypePolicyOptions();
    self::assertSame('object', $p->decimal);
    self::assertSame('object', $p->uuid);
    self::assertSame('object', $p->u64Overflow);
    self::assertSame('utc', $p->naiveDatetimeZone);
}

public function testServerZoneIsRejectedAsDeferred(): void
{
    $this->expectException(\InvalidArgumentException::class);
    $this->expectExceptionMessageMatches('/naive_datetime_zone=server .*deferred/i');
    new TypePolicyOptions(naiveDatetimeZone: 'server');
}

public function testUnknownPolicyValueIsRejectedLoudly(): void
{
    $this->expectException(\InvalidArgumentException::class);
    new TypePolicyOptions(decimal: 'float');   // never a lossy default
}
```

- [ ] **Step 2: Run to verify it fails** — `(cd php/client && ./vendor/bin/phpunit --filter TypePolicyOptions)` → FAIL (class missing).

- [ ] **Step 3: Implement `TypePolicyOptions`** — a `final readonly` class validating each knob in the constructor and rejecting `server` with a message pointing at the S8 deferral.

- [ ] **Step 4: Plumb it to `Ferro::connect`/`connectTcp` and `Connection`**

Add an optional `?TypePolicyOptions $types = null` parameter. **Close the `codec:`/`values:` trap (hazard 34):** `Connection.php:63` currently silently discards `values:` when `codec:` is supplied. Either throw an `InvalidArgumentException` when both are passed, or drop the `codec:` escape hatch. Add a regression test asserting the silent-discard is impossible.

- [ ] **Step 5: Add the `PoolConfig` fields** — the four knobs with §9.1 defaults, parsed from env in `ferrod::config`, with the "why the engine side is thin" comment. Unit-test the env parsing including an invalid value → startup failure.

- [ ] **Step 6: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): §9.1 type-policy plumbing (safe object defaults; naive_datetime_zone=server deferred to S8)"
```

---

## Task 7: PHP read path — the M1 ValuePolicy + the §9 value objects

**Files:**
- Create: `php/client/src/Value/{Decimal,Date,Time,Uuid,Json,U64}.php`
- Create: `php/client/src/Client/Value/M1ValuePolicy.php`
- Modify: `php/client/src/Client/ExecCodec.php` (default policy), `php/client/src/Client/Connection.php:209` (streamed cols keep the tag)
- Test: `php/client/tests/Unit/{ValueObjectsTest,M1ValuePolicyTest}.php`

**Interfaces:**
- Consumes: `TypePolicyOptions` (Task 6), `Constants::TAG_*`.
- Produces: `M1ValuePolicy implements ValuePolicy` covering all 14 implemented tags; `Ferro\Decimal` (string-backed, exact), `Ferro\Date`, `Ferro\Time`, `Ferro\Uuid`, `Ferro\Json` (lazy — decodes on access), `Ferro\U64`. `TIMESTAMP`/`TIMESTAMPTZ` hydrate to `DateTimeImmutable` per §9.

**Non-negotiable (hazard 30):** every arm **throws** `ProtocolException` on a malformed payload. Do **not** reuse `M0ValuePolicy::toInt/toFloat/toStr` or the `SqlValueCodec` helpers — they return `0`/`0.0`/`''` and would turn a bad `DECIMAL` into `Decimal('')`.

- [ ] **Step 1: Write the failing policy tests**

```php
// php/client/tests/Unit/M1ValuePolicyTest.php

/** Hazard 28: a U64 arrives as int OR decimal-string depending on MAGNITUDE. */
public function testU64AcceptsBothWireForms(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'string'));
    self::assertSame('5', $p->decode(C::TAG_U64, 5));                       // small: PHP int
    self::assertSame('4294967296', $p->decode(C::TAG_U64, '4294967296'));   // >2^32: decimal string
    self::assertSame('18446744073709551615', $p->decode(C::TAG_U64, '18446744073709551615'));
}

public function testU64OverflowPolicies(): void
{
    $big = '18446744073709551615';
    self::assertInstanceOf(U64::class, (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_U64, $big));
    self::assertSame($big, (new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'string')))->decode(C::TAG_U64, $big));
    $this->expectException(ProtocolException::class);
    (new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'error')))->decode(C::TAG_U64, $big);
}

/** A value that FITS PHP_INT_MAX must come back as a plain int regardless of wire form. */
public function testU64WithinIntRangeIsAnInt(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    self::assertSame(4294967296, $p->decode(C::TAG_U64, '4294967296'));
}

/** §9: DECIMAL is string-backed and EXACT — display scale survives. */
public function testDecimalPreservesDisplayScale(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    self::assertSame('1.10', (string) $p->decode(C::TAG_DECIMAL, '1.10'));
    self::assertSame('1.1',  (string) $p->decode(C::TAG_DECIMAL, '1.1'));
    self::assertSame('NaN',  (string) $p->decode(C::TAG_DECIMAL, 'NaN'));
}

public function testTimestampTzIsAUtcInstantAndTimestampIsNaive(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    $tz = $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z');
    self::assertInstanceOf(\DateTimeImmutable::class, $tz);
    self::assertSame('UTC', $tz->getTimezone()->getName());
    self::assertSame('2026-08-05 13:45:07.250000', $tz->format('Y-m-d H:i:s.u'));

    $naive = $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000');
    self::assertSame('2026-08-05 13:45:07.250000', $naive->format('Y-m-d H:i:s.u'));
}

public function testNaiveDatetimeZoneErrorPolicyThrows(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions(naiveDatetimeZone: 'error'));
    $this->expectException(ProtocolException::class);
    $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07');
}

/** Hazard 30: malformed payloads THROW — never a silent zero/empty coercion. */
public function testMalformedPayloadsThrowAndNeverCoerce(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    foreach ([[C::TAG_DECIMAL, 'not-a-number'], [C::TAG_DATE, '2026-13-99'],
              [C::TAG_UUID, 'nope'], [C::TAG_TIMESTAMP, ''], [C::TAG_U64, 'x1']] as [$tag, $bad]) {
        try {
            $p->decode($tag, $bad);
            self::fail("tag $tag accepted a malformed payload: " . var_export($bad, true));
        } catch (ProtocolException) { /* expected */ }
    }
}

/** JSON is LAZY: no decode cost until access, and invalid JSON fails on access, not on row read. */
public function testJsonIsLazy(): void
{
    $j = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_JSON, '{"a":[1,2]}');
    self::assertInstanceOf(Json::class, $j);
    self::assertSame('{"a":[1,2]}', (string) $j);
    self::assertSame([1, 2], $j->decoded()['a']);
}

/** The deferred tags must still be a loud, named failure. */
public function testDeferredTagsStillThrowNamingTheTag(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    foreach ([C::TAG_ARRAY, C::TAG_INTERVAL, C::TAG_INET, C::TAG_VECTOR] as $tag) {
        try { $p->decode($tag, null); self::fail("tag $tag must be unsupported"); }
        catch (ProtocolException $e) { self::assertStringContainsString((string) $tag, $e->getMessage()); }
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `(cd php/client && ./vendor/bin/phpunit --filter M1ValuePolicy)` → FAIL.

- [ ] **Step 3: Implement the value objects**

Each `final readonly`, each with `__toString()` returning the **canonical wire text** (so a round-trip through bind is byte-stable), and each validating in the constructor. `Ferro\Json` stores the raw text and decodes lazily in `decoded()`, caching the result; invalid JSON throws on access, not on construction. `Ferro\U64` is string-backed for values above `PHP_INT_MAX`.

- [ ] **Step 4: Implement `M1ValuePolicy`**

All 14 tags. The U64 arm must normalize **both** wire forms and compare against `PHP_INT_MAX` itself (hazard 28) — never branch on `is_int($data)`. PHPStan L9 requires explicit `is_string`/`is_int` narrowing before use (hazard 36).

- [ ] **Step 5: Switch the default policy + keep the streamed column tags**

Make `M1ValuePolicy` the default in `ExecCodec` (keeping `M0ValuePolicy` for its existing tests). At `Connection.php:209` the streamed path currently drops the per-column tag (`array_map(fn($c) => $c['name'], ...)`); preserve the tag alongside the name so streamed rows decode identically to buffered ones. Add a test that a streamed row and a buffered row of the same data decode to equal values.

- [ ] **Step 6: Run the suites** — `(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)` → PASS/clean.

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(m1-s7): M1ValuePolicy + §9 value objects (exact Decimal, lazy Json, U64 dual-form) — throws, never coerces"
```

---

## Task 8: The bind (write) path — both codecs and both backends

**Files:**
- Modify: `php/client/src/Client/ExecCodec.php:185` (`bindOne`), `php/client/src/Protocol/SqlValueCodec.php:28`, `php/client/src/Protocol/Value.php:23`
- Modify: `engine/crates/ferro-backend-pg/src/bind.rs:42,67,80`; `engine/crates/ferro-backend-mysql/src/bind.rs`
- Test: `php/client/tests/Unit/BindTest.php`; per-backend bind unit tests; live coverage folded into Task 9

**Why this task exists (hazard 31):** the `ValuePolicy` seam is **decode-only**. Without this task the slice is read-only and a DBAL suite — which binds `DateTime`s and decimals constantly — is still broken.

**Non-negotiable (hazard 19):** `accepts` and `value_to_boxed` must stay **mirror-image, arm for arm**, in both backends. A `value_to_boxed` arm without an `accepts` arm turns valid binds into false known-fate rejections; the reverse lets `to_sql_checked` fail *post-send* — the false-`Indeterminate` path the pre-validation exists to prevent.

- [ ] **Step 1: Write the failing bind tests**

```php
// php/client/tests/Unit/BindTest.php

/** Hazard 29: a U64 above PHP_INT_MAX MUST go through packUint, never packInt. */
public function testU64BindsViaPackUintAndSurvivesTheFullRange(): void
{
    $big = '18446744073709551615';
    $wire = SqlValueCodec::encode(['tag' => C::TAG_U64, 'data' => $big]);
    self::assertSame($big, SqlValueCodec::fromWire(PurePacker::unpack($wire))['data']);
}

public function testValueObjectsBindToTheirCanonicalText(): void
{
    foreach ([
        [new Decimal('1.10'),                            C::TAG_DECIMAL,     '1.10'],
        [new Date('2026-08-05'),                         C::TAG_DATE,        '2026-08-05'],
        [new Time('24:00:00'),                           C::TAG_TIME,        '24:00:00'],
        [new Uuid('3f2b8c1a-0000-4fff-8000-abcdefabcdef'), C::TAG_UUID,      '3f2b8c1a-0000-4fff-8000-abcdefabcdef'],
        [new Json('{"a":1}'),                            C::TAG_JSON,        '{"a":1}'],
    ] as [$obj, $tag, $text]) {
        $bound = (new ExecCodec(new M1ValuePolicy(new TypePolicyOptions())))->bindOne($obj);
        self::assertSame($tag,  $bound['tag']);
        self::assertSame($text, $bound['data']);
    }
}

/** A native DateTimeImmutable is the ergonomic case DBAL will hit — UTC-normalized to TIMESTAMPTZ. */
public function testDateTimeImmutableBindsAsTimestampTz(): void
{
    $dt = new \DateTimeImmutable('2026-08-05 13:45:07.250000', new \DateTimeZone('+02:00'));
    $bound = (new ExecCodec(new M1ValuePolicy(new TypePolicyOptions())))->bindOne($dt);
    self::assertSame(C::TAG_TIMESTAMPTZ, $bound['tag']);
    self::assertSame('2026-08-05T11:45:07.250000Z', $bound['data']);
}

public function testUnbindableValueStillThrows(): void
{
    $this->expectException(ProtocolException::class);
    (new ExecCodec(new M1ValuePolicy(new TypePolicyOptions())))->bindOne(new \stdClass());
}
```

- [ ] **Step 2: Run to verify it fails** → FAIL.

- [ ] **Step 3: Implement the PHP bind arms** — extend `bindOne` to recognise the value objects plus native `DateTimeImmutable`/`DateTime`; extend `SqlValueCodec::encode` and `Protocol/Value.php` for all 14 tags, with `packUint` for `TAG_U64`.

- [ ] **Step 4: Implement the engine bind arms**

PG (`bind.rs`): each new `Value` variant needs a `ToSql`. The pragmatic, precision-safe route is a **text-format param** — `ToSql::encode_format` is per-param selectable (unlike result format, hazard 17), so canonical text can be sent as text and let PG parse it. This avoids hand-writing a base-10000 NUMERIC *encoder*. Keep `accepts` a mirror of `value_to_boxed`; add a unit test asserting the two agree for every variant:

```rust
/// Hazard 19: accepts() and value_to_boxed() must cover exactly the same variants.
#[test]
fn accepts_mirrors_value_to_boxed_for_every_variant() {
    for v in every_value_variant() {
        assert_eq!(accepts(&v), value_to_boxed(v.clone()).is_ok(),
            "accepts/value_to_boxed disagree for {v:?}");
    }
}
```

MySQL (`bind.rs`): map the canonical text to the appropriate `MyValue` (the server parses date/decimal literals from strings), with the same mirror test.

- [ ] **Step 5: Run everything** — `cargo test --workspace` + the PHP suites. PASS.

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(m1-s7): bind path for all canonical tags (both codecs, both backends) with accepts/value_to_boxed mirror tests"
```

---

## Task 9: Live end-to-end acceptance + DTO path + spec truth

**Files:**
- Create: `engine/crates/ferrod/tests/types_e2e_it.rs` (client → ferrod → DB, both backends)
- Modify: `php/client/tests/Live/` (a typed round-trip through the real client)
- Modify: `ferro-spec-v0.2.md` §9, §9.1, §22.2; `proto/PROTOCOL.md` (verify Task 1's table still matches shipped behavior)

**This is the slice acceptance gate.** It must prove the *whole* path — PHP bind → wire → engine → DB → engine → wire → PHP decode — for every new tag on every engine, and that the deferrals are still loud.

- [ ] **Step 1: Write the full-path round-trip test**

For each new tag, on PG and MySQL and MariaDB: bind a value from PHP (or from the Rust e2e client), write it, read it back, assert **exact equality with the original**. Include the precision-critical cases:

- `DECIMAL`: `-12345.6700000000` (trailing zeros survive a full round trip), a 200-digit value on PG, `NaN` on PG.
- `U64`: `18446744073709551615` on MySQL (> `i64::MAX`).
- `TIMESTAMP` vs `TIMESTAMPTZ`: with a **non-UTC session timezone** on both engines, so a UTC-by-accident false green is impossible.
- `TIME`: `24:00:00` (PG), negative and > 24 h (MySQL).
- `UUID`: mixed-case input normalizes to lowercase (PG).
- `JSON`: nested + non-ASCII, semantic equality.
- Fractional seconds: `.250000` preserved; a whole second emits no fraction.

- [ ] **Step 2: Prove the deferrals are still loud**

Assert `Unsupported` — naming the column/type — for: PG `interval`, `inet`, `int4[]`, `timetz`, an enum, a domain over numeric; MySQL `YEAR`, `BIT(8)`, `ENUM`, `SET`. And that the client raises a named `ProtocolException` for `TAG_ARRAY`/`INTERVAL`/`INET`/`VECTOR`.

- [ ] **Step 3: Cover the DTO/hydration path (hazard 35)**

Add a test for the native-API DTO path with a value-object-typed constructor param, and one proving a **type mismatch** (e.g. `Ferro\Decimal` into `readonly string`) surfaces inside the `FerroException` contract rather than as a bare `\TypeError`. Fix `hydrateDto` if it escapes the contract.

- [ ] **Step 4: Run the whole gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                                    # offline: live skips
docker compose -f testkit/docker-compose.yml up -d
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test --workspace -- --nocapture
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
```
Paste the per-type actual values for all three engines into the task report.

- [ ] **Step 5: Spec truth**

- §9: mark the canonical table's covered rows as implemented in M1-S7; state that the *wire* payload is canonical text (pointing at `PROTOCOL.md` §3) while the table's PHP column describes the native-API hydration.
- §9.1: record the shipped policy set and defaults; record that `naive_datetime_zone: server` is **deferred to S8** (needs `HELLO_ACK` pool metadata) and that `error`/`utc` ship now.
- §22.2: one entry for the slice — the text-canonical wire decision and *why* (PurePacker cannot decode maps/ext; `str`/`bin` indistinguishable in PHP; precision/display-scale preservation), the implemented-set-in-the-hash skew decision, the still-deferred tags (`ARRAY`/`INTERVAL`/`INET`/`VECTOR`, PG `TIMETZ`/enums/domains, MySQL `YEAR`/`BIT`/`ENUM`/`SET`, and MySQL having no native UUID type), and the deliberately-thin engine-side policy plumbing with its rationale.

- [ ] **Step 6: Commit**

```bash
git commit -m "test(m1-s7): live end-to-end canonical-type acceptance on PG + MySQL + MariaDB, deferrals still loud, spec truth"
```

---

## Self-Review (controller, after the plan is written)

1. **Spec coverage** — §9's table rows for the 8 tags: Tasks 4/5 (engine), 7 (PHP hydration), 8 (bind). §9.1's four policies: Task 6 + 7 (with `server` explicitly deferred). Charter rule 2 (registry + vectors + both codecs together): Tasks 1-3. The §22 deferral record: Task 9.
2. **Placeholder scan** — every code step carries real code or a named file:line target; no "add error handling" steps.
3. **Type consistency** — `Value::{U64,Decimal,Date,Time,Timestamp,TimestampTz,Uuid,Json}` is used identically in Tasks 1, 4, 5, 8; `TypePolicyOptions` field names match between Tasks 6, 7, 8; `MyKind` variants match between the Task 5 table and its steps.
4. **Ordering** — 1 (codec) → 2 (vectors/PHP codec) → 3 (hash) → 4/5 (read paths, independent of each other) → 6 (policy) → 7 (PHP read) → 8 (bind) → 9 (acceptance). Tasks 4 and 5 can run in parallel.
