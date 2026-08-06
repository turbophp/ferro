# Ferro M1-S7 — Canonical Type Coverage (§9/§9.1) Implementation Plan — **v2**

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **v2 (adversarial-verification pass applied).** 38 fixes (F1–F38) from six code-reading probes are folded in. Two structural consequences: the **registry-hash task now runs second** (v1's Task 3 → **Task 2**; v1's Task 2 → **Task 3**) so the vector-completeness guard derives from the registry instead of a parallel hardcoded list; and the three heaviest tasks are **split in half** (4a/4b, 5a/5b, 8a/8b), each half independently green and committable. Execution order is strictly **top-to-bottom**. Hazard numbers 1–36 are stable across v1→v2 (corrected ones are marked **(v2 correction)**); 37–48 are new evidence.

**Goal:** Take the eight DBAL-critical canonical tags — `U64`, `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `JSON` — from "registry constants only" to working end-to-end on both PostgreSQL and MySQL/MariaDB, in both directions (read *and* bind), with the §9.1 policies, so the S8 Doctrine tier and the S9 exit-gate suite stand on real type support instead of a loud `Unsupported`.

**Architecture:** The wire stays a `[tag, payload]` MessagePack pair and every new payload is **text-canonical** (msgpack `str`), except `U64` which rides the msgpack uint family. The engine renders each backend's native binary form into that canonical text losslessly; the PHP client's `ValuePolicy` seam turns canonical text into the §9 value objects (or, via the new `RawStringValuePolicy`, leaves it as driver-native strings for the S8 DBAL tier). Both backends keep their existing single-classifier discipline — PG's `oid_extract_type` and MySQL's `column_kind` remain the ONE authority backing both the `ColMeta` tag and the cell extraction, so `cols` and `rows` can never disagree.

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

Text-canonical. Chosen because PHP's `PurePacker` cannot decode msgpack **maps or ext types at all** (`php/client/src/Protocol/Msgpack/PurePacker.php:110` throws on every fixmap/map16/map32/ext marker, and `proto/PROTOCOL.md` §2 bans ext types outright), and because `str` and `bin` are **indistinguishable** in PHP after unpack (the tag is the only discriminator), so a `bin` payload would need a `list<int>` special case *and* could not round-trip through the golden-vector JSON `message` field.

| Tag | # | msgpack family | Canonical payload | Notes |
|---|---|---|---|---|
| `U64` | 3 | uint | unsigned 64-bit integer | The ONLY non-`str` addition. See the U64 hazards below. |
| `DECIMAL` | 5 | `str` | `"-12345.6700"` — full precision, **display scale preserved** | `"NaN"`, `"Infinity"`, `"-Infinity"` are legal payloads (PG NUMERIC allows them). `1.10` and `1.1` are **distinct** payloads. |
| `DATE` | 8 | `str` | `"YYYY-MM-DD"` | `"infinity"` / `"-infinity"` for the PG sentinels; `"0000-00-00"` for a MySQL zero date. |
| `TIME` | 9 | `str` | `"HH:MM:SS"` or `"HH:MM:SS.ffffff"` | Hours may exceed 23 (PG `time '24:00:00'`; MySQL `TIME` spans ±838h and may be negative → a leading `-`). |
| `TIMESTAMP` | 10 | `str` | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` | **Naive** — no zone suffix, ever. |
| `TIMESTAMPTZ` | 11 | `str` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` | RFC3339, **always normalized to UTC**, always the literal `Z`. |
| `UUID` | 12 | `str` | 36-char canonical **lowercase** hyphenated | Never raw bytes (see the `bin` hazard). |
| `JSON` | 13 | `str` | the raw UTF-8 JSON document text | Not re-serialized, not validated by the engine; PHP decodes lazily. |

Fractional seconds: emit **no** `.ffffff` group when the sub-second part is zero; otherwise emit exactly 6 digits. Never emit trailing-zero-trimmed variants — the payload must be byte-stable for the golden vectors.

**Still deferred in S7** (must remain a loud `Unsupported`, documented in §22.2):

*Tags with zero implementation:* `ARRAY`(14), `INTERVAL`(15), `INET`(16), `VECTOR`(17) — PG-exotic, not required for a green DBAL suite. Also out of scope and still `Unsupported`: PG `TIMETZ`, PG enums/domains/composites/ranges; MySQL/MariaDB `YEAR`/`BIT`/`ENUM`/`SET`/`GEOMETRY`/`VECTOR`; MariaDB's native `UUID` type (10.7+).

*Sized S8 carries (**F32** — named here so the S9 DBAL gate is reduced, not moved). These are NOT S7 work; Task 9 Step 5 writes each into §22.2 with this sizing:*

| S8 carry | Why a real DBAL suite hits it | Size |
|---|---|---|
| **PG catalog scalars** — `name`(19), `oid`(26), `"char"`(18), `regtype` | `information_schema` / `pg_catalog` introspection, which DBAL's `AbstractSchemaManager` runs constantly | ~6 new arms in `oid_extract_type`/`oid_to_tag`/`extract_value`, all trivially `TEXT`/`I64`. **~1 h.** |
| **PG domains** — `Type::kind() → Kind::Domain(inner)` unwrap before the OID match | Every `information_schema` column is a **domain** (`sql_identifier`, `character_data`, …), so introspection is 100 % blocked without it | ~10 lines in `oid_extract_type` plus one recursion guard; needs the `Type` (not just the `Oid`) threaded from `Row::columns()`. **~2 h.** |
| **Narrowing bind path** — bind `I64` as the statement's inferred `int2`/`int4`, `F64` as `float4`, with `accepts` widened in lockstep | `bind.rs:127-131` / `query.rs:86-95` hard-reject a PHP `int` against an `INTEGER` column today; DBAL generates those on every insert into a `serial`/`int` PK | a per-target-type `ToSql` dispatch inside `value_to_boxed` + the mirrored `accepts` table + a range check that a too-large `i64` is a **known-fate** rejection. **~4 h, and it is the single highest-frequency S8 blocker.** |
| **`Ferro\Bytes` value object / explicit binary-bind marker** | `ExecCodec::bindOne` maps *every* PHP string to `TAG_TEXT`, so `TAG_BYTES` is **unreachable** from PHP — `ParameterType::BINARY`/`LARGE_OBJECT` and DBAL's `BlobTest` cannot bind at all | one value object + one `bindOne` arm + a live blob round trip. **~2 h.** |

### Version skew (USER-DECIDED)

The implemented-tag set becomes **part of the hashed registry**, so `TYPE_REGISTRY_HASH` changes and an engine/client pair with different type coverage fails **fast at the handshake** with a clear registry-mismatch error, instead of throwing a confusing `ProtocolException` mid-query on the first `DECIMAL` row.

**(v2 correction — F21.)** Both hash implementations already FNV-1a the **raw `registry.lock.json` bytes** — Rust `build.rs:118-127` `fnv1a_hex`, PHP `proto/tools/gen-php.php:14-31` (limb-based, over `$raw`). Parity is therefore **automatic** and `fnv1a_hex` must **not** be touched. The only work is getting `implemented` *into the lock file*; the hash then moves by construction.

### Verified hazards — a naive implementation is WRONG

**Rust codec**

1. **The `Value` codec is NOT generic over the tag.** `ferro_proto::value::Value` is a closed 6-variant enum; `Value::decode` (`engine/crates/ferro-proto/src/value.rs:50`) ends in `other => Err(CodecError::Malformed(...))`. Eight new variants + arms in `tag()` (`:23`), `encode` (`:34`) and `decode` (`:50`) are **mandatory**, and they cascade into exhaustive matches at `ferro-backend-pg/src/bind.rs:42` (`value_to_boxed`), `:67` (`accepts`), `:80` (`value_kind`), `ferro-backend-mysql/src/bind.rs:48` (`value_to_my` — see hazard 37, the MySQL module's shape is **different**), `ferrod/src/services/sql.rs:1090` (`estimate_row_bytes`), `ferro-e2e/src/main.rs`, `gen_vectors.rs` (`v_json`), and `ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs:7` (hazard 42 — **not** caught by any workspace gate). `clippy -D warnings` will catch the workspace ones — that is the guard, not a reason to add a `_ =>` catch-all.
2. **Reuse `read_str`/`read_bin`, never a hand-rolled reader.** They call `bound_len(len, remaining)` (`value.rs:122`) which rejects a lying length prefix *before* allocating. Regression test: `engine/crates/ferro-proto/tests/value.rs:85 lying_length_prefix_is_rejected_before_allocating`.
3. **The tag byte must stay a bare positive fixint.** `encode` uses `enc::write_pfix` and `decode` uses `dec::read_pfix` (`value.rs:37`, `:58`). Correct for 0..=17; keep the invariant documented rather than "fixing" it to a generic int read (which would let a non-canonical tag encoding through).
4. **`estimate_row_bytes` feeds the streaming batch-size bound. (v2 correction — F7.)** It sizes a **soft** batch bound, *not* the hard frame ceiling: `Responder::send_data` enforces an independent `Oversized` hard ceiling (`ferrod/src/services/sql.rs:552-556`, estimator at `:1088-1090`). So a careless `_ => 9` does not silently emit an over-large frame — it **mis-sizes the soft bound so a perfectly valid request trips the hard `Oversized` check and aborts mid-stream**. Every new variant still needs a real length-proportional estimate; the prescribed ones below match the existing arms' shape exactly.

**Golden vectors**

5. **There is NO completeness guard that a tag has a vector.** The only positive-side assertion is `assert!(count >= 7)` (`engine/crates/ferro-proto/tests/golden_vectors.rs:35`) against 21 existing vectors — permanently satisfied. The *negative* side has an explicit required-name list (`:175-185`); the positive side does not. **Task 3 adds one**, deriving its required set from the registry's `implemented` list (**F1**) so it cannot drift from Task 2's single source of truth.
6. **PHP byte-lock coverage is keyed on vector NAME PREFIX.** `VectorConformanceTest::sqlVectors()` matches `sql_exec_`, `streamVectors()` matches `stream_head_`/`stream_data_` (`php/client/tests/Conformance/VectorConformanceTest.php:102`, `:161`). A vector named e.g. `typedvalue_decimal.json` gets only the generic header/unpack tests and **no byte lock** — silently half-covered. **Name new vectors `sql_exec_response_*` / `stream_data_*`.**
7. **`hasBigUint` gates ONLY the ext-vs-pure parity test — not the byte lock. (v2 correction — F8.)** `VectorConformanceTest::hasBigUint` (`:364-377`) is consulted at exactly one site, `testExtPackerDecodeMatchesPureWhenLoaded` (`:90`). The byte-lock tests never consult it, so there is **no false green on the byte lock**; what a big-uint literal costs is the *whole vector's* ext-vs-pure comparison. The real CI hazard is the opposite one: a golden-vector `U64` must be **≤ `0xffffffff`** or **> `PHP_INT_MAX`**, and **never in `(2^32, 2^63]`** — rmp emits marker `0xcf` from 2^32 up, `PurePacker::be()` returns a decimal **string** for every `0xcf` uint64 while ext-msgpack returns an `int`, so `assertEquals(json_encode($pure), json_encode($ext))` **fails** in CI (which provisions ext-msgpack) and `hasBigUint` does not skip it because the string is ≤ `PHP_INT_MAX`. Bare ≥20-digit DECIMAL literals are therefore **allowed** (they only forgo the ext comparison, with a comment saying so).
8. **A large `U64` must be rendered as a decimal STRING in the vector JSON** — a JSON number > 2^53 is lossy through PHP `json_decode`. The established convention is already in the generator (`gen_vectors.rs:199` hard-codes `"boot_epoch":"18446744073709551600"`).
9. **There is no regenerate-and-diff guard for vectors** (unlike the two registry guards). A hand-written vector JSON that is not also in `gen_vectors.rs` survives forever and silently diverges. Every new vector goes through the generator.

**PostgreSQL**

10. **`postgres-types` has NO `NUMERIC` `FromSql` under any feature. (v2 correction — F9.)** Neither does `postgres-protocol`: `grep -rni numeric postgres-protocol-0.6.12/src` → **zero hits**, so it is *not* a reference oracle for the layout and adding it as a dependency buys nothing for NUMERIC. `DECIMAL` must be hand-decoded from the base-10000 binary format, whose layout is stated inline in Task 4a Step 3 (source of truth: PG's `src/backend/utils/adt/numeric.c`). Do **not** route it through `rust_decimal`: a 96-bit mantissa (~28 digits) cannot hold PG's 131 072 integral digits, cannot represent `NaN`, and normalizing through any decimal type **loses the display scale** (`1.10` → `1.1`), which breaks DBAL string comparisons.
11. **`TIMESTAMP` and `TIMESTAMPTZ` share an identical 8-byte payload.** Only the column OID separates naive-local from UTC-instant. Do **not** use the featureless `SystemTime` `FromSql` — its `accepts!(TIMESTAMP, TIMESTAMPTZ)` erases the distinction by construction.
12. **The PG epoch is 2000-01-01, not 1970-01-01** — off by 946 684 800 s / 10 957 days. A Unix-epoch assumption yields a plausible-looking wrong date, not a crash.
13. **Infinity sentinels:** `DATE` ±infinity are `i32::MAX`/`i32::MIN`; `TIMESTAMP`/`TIMESTAMPTZ` ±infinity are `i64::MAX`/`i64::MIN`. Handle them explicitly (payload `"infinity"`/`"-infinity"`), never arithmetically.
14. **PG `time '24:00:00'` is legal** (86 400 000 000 µs). chrono's `NaiveTime` addition **wraps** it to `00:00:00` (`chrono_04.rs:136`). Hand-roll `i64 µs → "HH:MM:SS[.ffffff]"` so hours may exceed 23.
15. **`TIMETZ` (OID 1266) has no `FromSql` under any feature** and its payload is 12 bytes (i64 µs + i32 zone), so `time_from_sql` rejects it. Keep it explicitly `Unsupported` — never let it fall into the `TIME` arm.
16. **A raw-bytes `FromSql` with `accepts(_) -> true` DEFEATS tokio-postgres' own type check.** `oid_extract_type` must remain the sole authority; never call the raw getter without first passing the OID gate.
17. **Result format is BINARY and is NOT per-statement selectable** — `Some(1)` is hardcoded in the vendored fork (`vendor/tokio-postgres/src/query.rs:324`). Any "just request text format" approach needs a *second* fork divergence and is out of scope. (Asymmetry: *param* format IS per-param selectable via `ToSql::encode_format` — which is exactly what Task 8b's bind newtypes exploit.)
18. **Two `Unsupported` gates must move in lockstep.** cols-build runs pre-execution with the conn still clean (`ferro-backend-pg/src/query.rs:67` buffered, `:172` streaming); the per-cell gate fires **mid-stream after HEAD is already on the wire** (`:108`, `:245`). Adding a type to `oid_extract_type` but not `extract_value` yields a HEAD promising a tag the producer cannot fill.
19. **`bind::accepts` (`bind.rs:67`) is the §19.3 known-fate pre-flight. (v2 restatement — F17.)** The rule is *directional*, not symmetric: `accepts` may be **stricter** than the concrete `ToSql` it fronts (that yields a clean, diagnosable known-fate rejection), but it must **never be looser** — a looser `accepts` lets `to_sql_checked` fail **post-send**, which is exactly the false-`Indeterminate` path the pre-validation exists to prevent. Concretely: **do not** give a new bind newtype `PgNull`'s `accepts(_ty) -> true` (`bind.rs:29-31`); each of the eight tags gets its **own** newtype with a narrow `accepts`.
20. **Pure-OID matching misses domains/enums/composites** (they arrive with custom OIDs via `Kind::Domain(inner)` etc.). They stay `Unsupported` in S7 — do not attempt `kind()` unwrapping in this slice; it is a sized S8 carry (see the deferral table above) and gets a §22.2 line in Task 9.
21. **If `postgres-protocol` is added at all, pin it to the exact version the fork resolves (0.6.12, `Cargo.lock`)** and run `cargo deny check`. A mismatched minor gives two distinct crates whose traits will not unify, and `deny.toml`'s `multiple-versions = "allow"` will NOT catch it. **(v2 note — F9:** per hazard 10, hand-decoding `&[u8]` needs no new crate at all; Task 4a Step 3 makes "do we need it?" an explicit decision point, with "no" as the default answer.)

**MySQL/MariaDB**

22. `DECIMAL`/`NEWDECIMAL` arrive as `MyValue::Bytes` holding the server's own ASCII rendering — that text **is** the canonical payload (display scale already preserved). Do not parse it into a numeric type and re-render.
23. **Only `BIGINT UNSIGNED` needs `U64`.** Unsigned `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT` all fit `i64` losslessly and should map to `I64` — this deliberately narrows the `U64` surface. (§9's table lists U64 against "bigint unsigned".) **(v2 addition — F5.)** A `BIGINT UNSIGNED` cell **≤ `i64::MAX` arrives as `MyValue::Int`, not `MyValue::UInt`** — `mysql_common-0.37.3/src/value/mod.rs:320-329` only produces `UInt` when the value exceeds `i64::MAX`. A `MyValue::UInt(n)`-only extractor therefore rejects the **common** case as a decode mismatch *after* HEAD already promised tag `U64`. The extractor needs an `as_u64` helper accepting **both** forms (Task 5b Step 5).
24. **MySQL `DATETIME` is naive → `TIMESTAMP`(10); MySQL `TIMESTAMP` → `TIMESTAMPTZ`(11). (v2 correction — F4.)** The reason is *not* "the server hands back UTC". MySQL stores `TIMESTAMP` in UTC but **converts it to the session `time_zone` on retrieval**, and the driver hands back zone-**less** `MyValue::Date(y, m, d, h, mi, s, us)` components (`mysql_common-0.37.3/src/value/mod.rs:66,443-446`) — the wire carries no offset at all. Stamping a `Z` on those components is truthful **only because Ferro pins the session to `+00:00`** (Task 5a Step 0, the F3 decision). Getting this wrong is a silent zone shift in **both** directions (read *and* bind). This matches §9's table exactly (`TIMESTAMP | timestamp | datetime` / `TIMESTAMPTZ | timestamptz | timestamp`).
25. **UUID/JSON engine asymmetry. (v2 correction — F6.)** **MySQL 8 has no native UUID type** — `BINARY(16)` stays `BYTES`, `CHAR(36)` stays `TEXT`, and nothing maps to the `UUID` tag on MySQL. **MariaDB 10.7+ *does* have a native `UUID` type — it stays `Unsupported` in S7** and is recorded as such in §22.2. Separately: **MariaDB has no JSON type** — `JSON` is an alias for `LONGTEXT` plus a `json_valid()` CHECK, and the driver exposes no MariaDB extended metadata (`ColumnMeta` gives only `column_type`/`flags`/`character_set`/`column_length`, and `rowmap.rs:93-111` keys on charset alone), so a **MariaDB `JSON` column classifies as `MyKind::Text` → `tag::TEXT` by design**. "Fixing" that by promoting utf8 `LONGTEXT` to `JSON` would be a silent miscast violating charter rule 6.
26. **`MyValue::Time` carries `(is_negative, days, hours, minutes, seconds, micros)`** — a MySQL `TIME` may be negative and may exceed 24 h. Render the sign and fold `days` into hours.
27. **Zero dates (`'0000-00-00'`, `'0000-00-00 00:00:00'`) are legal in MySQL** unless `NO_ZERO_DATE` is set, and arrive with `year=0`. They are not representable as a real date — surface them as the literal canonical text (`"0000-00-00"`), not as an error. **(v2 note — F35:** MySQL 8's *default* `sql_mode` **includes** `NO_ZERO_DATE,NO_ZERO_IN_DATE` — verified live on `testkit-mysql-1`, and the compose file sets no override — while MariaDB 11's does not. So the live zero-date case lands on **MariaDB**; MySQL needs an explicit per-statement `SET SESSION sql_mode=''` or the coverage is unit-test-only. See Task 5b Step 6.)

**PHP client**

28. **`U64` arrives as an int OR a decimal string depending on MAGNITUDE, not type.** `PurePacker::be()` (**`php/client/src/Protocol/Msgpack/PurePacker.php:154-166`** — v2 path correction, F10) returns a decimal **string** for *every* `0xcf`-marked uint64, while rmp's narrowing ladder emits `0xcc/0xcd/0xce` for anything ≤ `0xffffffff`. So `5` arrives as `int 5` but `2^33` arrives as a **string**. A naive `is_int($data)` branch mishandles the whole 2^32..2^64 range — the policy must normalize **both** forms and compare against `PHP_INT_MAX` itself.
29. **Encoding a `U64` must use `packUint(int|string)`, never `packInt(int)`** — `packInt` physically cannot emit > `PHP_INT_MAX`, and `Protocol/Value.php:23` currently calls `packInt(self::toInt(...))`. A naive `TAG_U64 => $p->packInt(...)` arm is a data-corruption bug. (`PackerInterface::packUint(int|string $n)` — `php/client/src/Protocol/Msgpack/PackerInterface.php:10`; see also hazard 48 for the `ExtPacker` side.)
30. **Do NOT reuse the existing narrowing helpers — in the policy layer OR the codec layer.** `M0ValuePolicy::toInt/toFloat/toStr` return `0`/`0.0`/`''` for an unexpected payload and `bytesFromInts` returns `''` for a non-array (`M0ValuePolicy.php:40-78`); `SqlValueCodec`'s helpers behave identically (`SqlValueCodec.php:52-76`). Copying that idiom would turn a malformed `DECIMAL` into `Decimal('')` and a bad `TIMESTAMP` into epoch-zero — exactly the silent miscast §9.1 exists to prevent. **The M1 arms must throw.** **(v2 scope extension — F18:** this binds `SqlValueCodec::encode` and `Protocol/Value.php` too, not just the Task 7 policy. Those are the **bind** path — `toStr` there turns a bad payload into a silent empty-string **write**, and `toInt` saturates a `u64::MAX` decimal string to `PHP_INT_MAX`.)
31. **The `ValuePolicy` seam is DECODE-ONLY.** Binding goes through an unrelated chokepoint: `ExecCodec::bindOne` (`ExecCodec.php:185-198`) throws for anything not null/bool/int/float/string, and below it `SqlValueCodec::encode` (`:16`) and `Protocol/Value.php` (`:27`) each throw for any tag > `BYTES`. Read-only support leaves writes broken; a DBAL suite binds `DateTime`s and decimals constantly. **Tasks 8a/8b exist for this.**
32. **A decode error must never look like a §19.3 fate signal.** It surfaces client-side inside `ExecCodec::decodeRow` — i.e. *after* the statement already succeeded, and on the streamed path after earlier rows were already yielded (`Connection.php:234-239` yields inside the loop; `stream()`'s `finally` then fires `abandonStream`, sending `CANCEL` + drain). Raise it in the `FerroException` family, matching the existing rationale at `ExecCodec.php:97-98`. **(v2 refinement — F30:** split the class by *cause*. A **malformed** payload (a `DECIMAL` that is not a number, a truncated `TIMESTAMP`) stays `ProtocolException` — it is a wire fault. An **operator policy refusal** (`naive_datetime_zone=error`, `u64_overflow=error`) becomes a new `TypePolicyException extends FerroException`, because routing a configuration choice into the wire-fault class makes S8's DBAL `ExceptionConverter` misreport it as a driver protocol failure.)
33. **`naive_datetime_zone: server` is NOT implementable client-side** — nothing on the wire carries the backend's session timezone (`HelloAck.php:24-30` has only `[engine_version, boot_epoch, features, pools, type_registry_hash]`). **S7 implements `utc` (default) and `error` only**; `server` is deferred with a §22.2 note, since it needs the `HELLO_ACK` pool metadata that is already an S8 carry.
34. **Passing both `codec:` and `values:` to `Connection` silently DISCARDS `values:`** — `Connection.php:63-68` is `$this->codec = $codec ?? new ExecCodec($values ?? new M0ValuePolicy(), $plans ?? new PlanCache(), …)`. **(v2 widening — F38:** it discards **`plans:` too.** Reject `codec:` together with *either*, in one exception naming both. No in-repo caller passes `codec:`, so making them mutually exclusive is safe today.)
35. **Value objects break DTO hydration silently-loudly.** `ExecCodec::hydrateDto` calls `newInstanceArgs` with no coercion (`:167-168`), so a `Ferro\Decimal` fed to a `readonly string $amount` throws a bare `\TypeError` that escapes the `FerroException` contract. Cover the native-API DTO path explicitly.
36. **PHPStan L9 runs over `src` only** (`phpstan.neon.dist`), no baseline. `ValuePolicy::decode` returns `mixed`, so constructing a value object from `mixed $data` needs explicit `is_string`/`is_int` guards (a bare cast is the lossy anti-pattern above). Tests are unanalyzed — an L9 violation in a test helper will not be caught by the gate.

**New evidence (v2)**

37. **The two bind modules have DIFFERENT shapes — "same treatment" is wrong (F20).** *PG* (`ferro-backend-pg/src/bind.rs`): `fn value_to_boxed(v: &Value) -> Box<dyn ToSql + Sync + Send>` at `:42` is **infallible** (returns a box, not a `Result`); `pub fn accepts(v: &Value, ty: &Type) -> bool` at `:67` takes **two** arguments and is the ONLY known-fate rejection mechanism (read at `query.rs:87` buffered, `:191` streaming); `value_kind` at `:80` is a label. *MySQL* (`ferro-backend-mysql/src/bind.rs`): there is **no `accepts` and no `value_kind`** — the module has only `validate_arity` (`:26`), `to_params` (`:38`) and `value_to_my` (`:48`), all **infallible by documented invariant** (`bind.rs:3-14` explains why: `COM_STMT_PREPARE` exposes no inferred param types, so arity is the only possible pre-flight). Any plan step saying "same three fns" / "the same mirror test" for MySQL targets functions that do not exist.
38. **Registry/hash API facts (F21).** `Registry::from_toml_dir(dir: &Path) -> Registry` is **infallible** and takes a `&Path` (`registry.rs:58`) — no `.expect()`. `Registry` derives `Debug, Serialize, Deserialize, PartialEq, Eq` — **no `Clone`** — and has **no** `type_registry_hash()`, `implemented_tags()` or `remove_implemented_tag()`. Its only relevant method is `to_lock_json()` (`:81`). `TypesToml` (`registry.rs:47-49`) currently parses **only** `tags`, which is why `m0_scalar` is dead. And critically: **`build.rs:11-27` carries its OWN `#[derive(Deserialize)] #[serde(deny_unknown_fields)] struct Registry`** — a new lock field that is not added there makes `cargo build` **panic** and the whole workspace stop building. Three edit sites: `registry.rs` `TypesToml`, `registry.rs` `Registry`, `build.rs` `Registry`.
39. **Real PHP signatures (F24).** `ExecCodec::__construct(ValuePolicy $values, PlanCache $plans, PackerInterface $encodePacker, PackerInterface $decodePacker)` — **four required** args (`ExecCodec.php:37-42`). `SqlValueCodec::encode(PackerInterface $p, mixed $vj): string` — **packer first** (`SqlValueCodec.php:16`). `PackerInterface::unpack(string $buf, int &$offset): mixed` is an **instance** method with a **by-reference** offset (`Msgpack/PurePacker.php:81`). `ExecCodec::bindOne` is **`private static`** (`:185`), called at `:179`.
40. **PSR-4 autoload map (F16).** `php/client/composer.json:15` maps `"Ferro\\": "src/"` (dev: `"Ferro\\Tests\\": "tests/"`). So `Ferro\Decimal` **must** live at `src/Decimal.php`; a file at `src/Value/Decimal.php` declaring `namespace Ferro;` cannot autoload and every test referencing it fatals on class-not-found.
41. **MySQL/MariaDB reject zoned datetime literals (F13).** `INSERT … VALUES ('2026-08-05T11:45:07.250000Z')` fails on MySQL 8 with `ERROR 1292 Incorrect datetime value` under the default `STRICT_TRANS_TABLES`, and MariaDB 11 rejects offsets in datetime literals outright. The canonical `TIMESTAMPTZ` text can therefore **never** be passed through as a MySQL string param — it must be parsed into `MyValue::Date(y,m,d,h,mi,s,us)` (a typed `MYSQL_TYPE_DATETIME` param, no server-side literal parsing), which is correct **only under the F3 UTC pin**. The two are coupled.
42. **The `fuzz` package is not a workspace member (F33).** `Cargo.toml:3` is `members = ["engine/crates/*", "bench"]`, so `engine/crates/ferro-proto/fuzz` is its own workspace and **no `cargo clippy --workspace` / `cargo test --workspace` gate touches it**. CI's `fuzz-smoke` job does run it (`ci.yml:63-64`, `cargo +nightly fuzz run roundtrip_frame`), so a stale `FuzzValue` compiles fine and silently covers 6 of 14 variants forever. Verify by hand: `cd engine/crates/ferro-proto/fuzz && cargo check`.
43. *(see hazard 27's v2 note — MySQL 8 default `sql_mode` includes `NO_ZERO_DATE`.)*
44. **`MYSQL_TYPE_TINY` arm order is wrong for `TINYINT(1) UNSIGNED` (F36).** `rowmap.rs:66-74` tests `unsigned` **first**, so a `TINYINT(1) UNSIGNED` falls past the `column_length() == 1` → `Bool` branch — contradicting the §9.1 `TINYINT(1) → Bool` policy pinned in M1-S6. Test `column_length() == 1` → `Bool` **first** (signed or unsigned), then the width branches.
45. **`ALL_KNOWN_OIDS` does not exist and a gate-agreement test is a tautology (F22).** `postgres-types` exposes no enumeration of its `Type` constants, and `oid_to_tag` is *defined* as a match over `oid_extract_type` (`rowmap.rs:45-53`), so `oid_to_tag(o).is_ok() == oid_extract_type(o).is_some()` holds by construction for every input. The assertion that actually bites is **HEAD-vs-producer**: for each newly-admitted OID, drive a real cell through `extract_value` and assert its tag equals what `oid_to_tag` promised.
46. **`dec::read_u64` is marker-strict and would break the feature (F28).** `rmp-0.8.15`'s `decode/uint.rs:109` `read_u64` accepts **only** `Marker::U64` — it rejects the canonical narrowing, so `Value::U64(0)` (encoded as a positive fixint) would fail to decode. `dec::read_int(rd)` (`decode/mod.rs:337`) is the correct reader: generic over the target, infers `u64` from `Value::U64`, and handles `Marker::U64` losslessly (`num-traits` is a mandatory rmp dep). On encode, `enc::write_uint` (`encode/uint.rs:151`) is required — it is byte-identical to PHP `PurePacker::packUint` across the whole range. Forbid `enc::write_u64` and `enc::write_sint`.
47. **`Connection.php:209` does not affect row decoding (F25).** Row decoding is driven by the **per-cell** tag (`ExecCodec.php:117-122`), and the **buffered** path drops the `ColMeta` tag too (`:83-88`, which keeps only `$c['name']`). Changing `$colNames` from `list<string>` to a richer shape breaks `assocRow` (`:144`), `hydrateDto` and `PlanCache::planFor` — a PHPStan L9 failure and a `TypeError` for **zero** behavior gain.
48. **`ExtPacker::packUint` silently `(int)`-casts (F19).** `Msgpack/ExtPacker.php:13` is `packUint(int|string $n) { return \msgpack_pack(is_string($n) ? (int) $n : $n); }` — a latent corruption for any `u64` above `PHP_INT_MAX`. Harmless today because nothing passes a string; Task 8a creates the first such call path.

### Definition of done (charter DoD, every task)

- `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` (green **offline** — live tests skip, never fail, when `FERRO_TEST_PG_URL` / `FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL` are unset).
- **`cargo deny check`** (F37) — a real CI job (`ci.yml:50-54`); mandatory on any task that touches a `Cargo.toml` dependency list.
- `(cd php/client && ./vendor/bin/phpunit)` green; `./vendor/bin/phpstan analyse src --level 9` clean.
- Protocol work adds/updates golden vectors **and** both codecs in the same commit, **and** the two vector-index tables in `proto/PROTOCOL.md` (§7 at `:195`, §8.3 at `:269`).
- The relevant SPEC section still tells the truth; a forced deviation is amended in the spec text **plus** a §22.2 line in the same change.

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
- `engine/crates/ferro-backend-pg/src/pgtext.rs` — the hand-rolled PG binary → canonical-text decoders (numeric base-10000, date/time/timestamp from the 2000 epoch, uuid hex, json passthrough). Isolated in its own file because it is pure, table-driven byte math with the densest unit-test surface in the slice. The `RawBytes` `FromSql` newtype lands with it in Task 4b.
- `engine/crates/ferro-backend-mysql/src/mytext.rs` — the MySQL `MyValue` → canonical-text renderers (date/time/timestamp formatting, negative/overlong `TIME`, zero dates).
- **`php/client/src/{Decimal,Date,Time,Uuid,Json,U64,NaiveTimestamp}.php`** — the §9 value objects, namespace `Ferro` (i.e. `Ferro\Decimal`). **(F16 decision: `src/` root, NOT `src/Value/`** — `composer.json:15` maps `Ferro\ => src/`, so this is the ONLY layout under which `Ferro\Decimal` autoloads, and it keeps §9's PHP column literally true with no spec amendment. Lower churn than renaming the namespace to `Ferro\Value\*`.)
- `php/client/src/Client/Value/M1ValuePolicy.php` — the M1 policy implementing all 14 implemented tags + the §9.1 knobs.
- `php/client/src/Client/Value/RawStringValuePolicy.php` — the S8 DBAL hand-off: canonical wire text verbatim for all 14 tags (F31).
- `php/client/src/Client/Value/TypePolicyOptions.php` — the §9.1 policy value object (`decimal`, `naiveDatetimeZone`, `u64Overflow`, `uuid`).
- `php/client/src/Client/Error/TypePolicyException.php` — policy **refusals**, distinct from wire faults (F30, hazard 32).
- `engine/crates/ferro-backend-pg/tests/pg_types_it.rs`, `engine/crates/ferro-backend-mysql/tests/mysql_types_it.rs` — live per-type round-trip acceptance.
- `engine/crates/ferrod/tests/types_e2e_it.rs` — the slice acceptance gate.
- `php/client/tests/Unit/{M1ValuePolicyTest,ValueObjectsTest,TypePolicyOptionsTest,BindTest}.php`.

**Modified**
- `proto/PROTOCOL.md` §3 (the payload-family table above) + the §7 (`:195`) and §8.3 (`:269`) vector-index tables.
- `proto/types.toml` — replace the dead `m0_scalar` key with a real, lock-feeding `implemented` list.
- `engine/crates/ferro-proto/src/registry.rs` (`TypesToml` **and** `Registry`) + **`engine/crates/ferro-proto/build.rs`** (its own `deny_unknown_fields` `Registry` — omitting this breaks the build, hazard 38); `proto/registry.lock.json`; `php/client/src/Protocol/Generated/Constants.php`.
- `engine/crates/ferro-proto/src/value.rs` — 8 new `Value` variants + `tag()`/`encode()`/`decode()` arms.
- `engine/crates/ferro-proto/tests/{value.rs,golden_vectors.rs,registry_sync.rs}` + `engine/crates/ferro-proto/src/bin/gen_vectors.rs`.
- **`engine/crates/ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs`** (`FuzzValue`, `:7` — outside every workspace gate, hazard 42).
- `engine/crates/ferro-backend-pg/src/{rowmap.rs,bind.rs}`; `engine/crates/ferro-backend-pg/tests/pg_query_it.rs` (`query_out_of_m0_column_is_unsupported`).
- `engine/crates/ferro-backend-mysql/src/{rowmap.rs,bind.rs,conn.rs}` (`conn.rs:198` — the UTC pin); **`engine/crates/ferro-backend-mysql/tests/query_it.rs:110-147`** (`out_of_scope_column_is_unsupported` + the file-header contract comment at `:7`, hazard/F27).
- `engine/crates/ferrod/src/services/sql.rs` (`estimate_row_bytes:1090`); `engine/crates/ferro-e2e/src/main.rs`.
- `php/client/src/Protocol/{Value.php,SqlValueCodec.php,Msgpack/ExtPacker.php}`; `php/client/src/Client/{ExecCodec.php,Connection.php}`; `php/client/src/Ferro.php`.
- `ferro-spec-v0.2.md` §9/§9.1/§22.2.

**Explicitly NOT modified** (F25/F29): `Connection.php:209`'s `$colNames` shape (hazard 47 — zero behavior gain, three breakages); `engine/crates/ferro-pool/src/config.rs` and `ferrod`'s env parsing (F29 — the four §9.1 knobs are client-side in M1; inert `PoolConfig` fields would let a typo block boot while changing nothing).

---

## Task 1: Pin the wire contract + widen the Rust TypedValue codec

**Files:**
- Modify: `proto/PROTOCOL.md` (§3 payload table, currently lines 91-108)
- Modify: `engine/crates/ferro-proto/src/value.rs:12` (enum), `:23` (`tag`), `:34` (`encode`), `:50` (`decode`)
- Modify (compile-cascade only): `engine/crates/ferro-backend-pg/src/bind.rs:42,67,80`; `engine/crates/ferro-backend-mysql/src/bind.rs:48` (`value_to_my` — **the ONLY fn there**, hazard 37); `engine/crates/ferrod/src/services/sql.rs:1090`; `engine/crates/ferro-e2e/src/main.rs`; `engine/crates/ferro-proto/src/bin/gen_vectors.rs` (`v_json`)
- Modify (**outside every workspace gate**, hazard 42): `engine/crates/ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs:7` (`FuzzValue`)
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

/// Hazard 46: U64 uses the CANONICAL NARROWING ladder (write_uint), so a small U64 is a positive
/// fixint on the wire — byte-identical to PHP `PurePacker::packUint`. A marker-strict reader
/// (`dec::read_u64`) would reject exactly this.
#[test]
fn s7_u64_uses_the_canonical_narrowing_ladder() {
    let mut small = Vec::new();
    Value::U64(0).encode(&mut small);
    assert_eq!(small, vec![0x92, 0x03, 0x00], "U64(0) must narrow to a positive fixint");
    let mut big = Vec::new();
    Value::U64(u64::MAX).encode(&mut big);
    assert_eq!(big[2], 0xcf, "U64::MAX must ride the uint64 marker");
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

/// The still-deferred tags MUST stay rejected — this is the §22.2 deferral, enforced.
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

`encode()`:

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

**These two functions are FIXED, not a hedge (hazard 46, F28).** `enc::write_uint` (`rmp-0.8.15/src/encode/uint.rs:151`) is required on encode: it narrows canonically, byte-identically to PHP `PurePacker::packUint`, across the whole range. `dec::read_int` (`rmp-0.8.15/src/decode/mod.rs:337`) is required on decode: it is generic over the target, infers `u64` from `Value::U64`, and handles `Marker::U64` losslessly (`num-traits` is a mandatory rmp dep). **Forbidden:** `dec::read_u64` (marker-strict, `decode/uint.rs:109` — it would reject the canonically-narrowed `Value::U64(0)`), `enc::write_u64` (always emits `0xcf`, diverging from the PHP narrowing the golden vectors lock), and `enc::write_sint` (cannot represent > `i64::MAX`, which is the entire reason `U64` exists).

- [ ] **Step 5: Run the codec tests**

Run: `cargo test -p ferro-proto --test value`
Expected: PASS (all five new tests + the existing suite).

- [ ] **Step 6: Fix the compile cascade — no `_ =>` catch-alls**

`cargo build --workspace` now fails on the exhaustive matches. The two backends have **different shapes** (hazard 37) — do not apply "the same treatment":

**PG (`ferro-backend-pg/src/bind.rs`).** `value_to_boxed` is `fn(&Value) -> Box<dyn ToSql + Sync + Send>` — **infallible**, no `Result`. **Decision taken in this plan: keep it infallible.** Threading a `Result<_, PoolError>` through `to_boxed_params` (`:38`) and both `query.rs` call sites (`:87`, `:191`) is an unbudgeted signature cascade that buys nothing — `accepts` (`:67`) is already the *only* known-fate rejection mechanism the query path reads, so a rejected variant never reaches `value_to_boxed`. Therefore, for **this task**:

```rust
// in `accepts` — the §19.3 known-fate gate. Task 8b replaces these with per-tag newtypes.
Value::U64(_)
| Value::Decimal(_)
| Value::Date(_)
| Value::Time(_)
| Value::Timestamp(_)
| Value::TimestampTz(_)
| Value::Uuid(_)
| Value::Json(_) => false, // M1-S7 Task 8b implements binding for these

// in `value_to_boxed` — UNREACHABLE while `accepts` returns false for these variants (the query
// path calls `accepts` first, at query.rs:87 / :191). Documented, not silently plausible.
Value::U64(_)
| Value::Decimal(_)
| Value::Date(_)
| Value::Time(_)
| Value::Timestamp(_)
| Value::TimestampTz(_)
| Value::Uuid(_)
| Value::Json(_) => unreachable!(
    "M1-S7 Task 1: bind::accepts rejects these tags until Task 8b; value_to_boxed is only \
     reached for a Value that accepts() approved"
),
```

plus the eight `value_kind` label arms (`"U64"`, `"DECIMAL"`, …). Add a unit test that the gate holds: `assert!(!accepts(&Value::Decimal("1".into()), &Type::NUMERIC))` — so if Task 8b is skipped the rejection is provably still in force.

**MySQL (`ferro-backend-mysql/src/bind.rs`).** There is **no `accepts` and no `value_kind`** — only `validate_arity` (`:26`), `to_params` (`:38`) and `value_to_my` (`:48`), all infallible by the module's documented TOTAL invariant (`bind.rs:3-14`). The cascade fix is to add **total** arms to `value_to_my`, which is already the Task 8b target — so implement the real mapping here and let Task 8b only add tests + the date parsing:

```rust
Value::U64(n) => MyValue::UInt(*n),
Value::Decimal(s) | Value::Uuid(s) | Value::Json(s) => MyValue::Bytes(s.clone().into_bytes()),
// DATE / TIME / TIMESTAMP / TIMESTAMPTZ: Task 8b parses the canonical text into MyValue::Date /
// MyValue::Time (a TYPED param) — a Bytes passthrough of the `Z`-suffixed text is REJECTED by both
// servers (hazard 41). Until then they are Bytes of the canonical text, which is correct for DATE
// and naive TIMESTAMP and WRONG for TIMESTAMPTZ, so Task 8b is not optional.
Value::Date(s) | Value::Time(s) | Value::Timestamp(s) | Value::TimestampTz(s) =>
    MyValue::Bytes(s.clone().into_bytes()),
```

Test it as **"`value_to_my` is total over all 14 variants"** (a loop over one instance of each variant asserting no panic and a non-`NULL` result for non-`Null` inputs) — the module's invariant, not a PG-style mirror test.

**`ferrod/src/services/sql.rs:1090` `estimate_row_bytes`** — real length-proportional estimates (hazard 4):

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

**`ferro-e2e/src/main.rs`** and **`gen_vectors.rs` `v_json`** — render the new variants; `v_json` must emit a large `U64` as a decimal **string** (hazard 8).

- [ ] **Step 7: Extend the fuzz target (outside every gate)**

`engine/crates/ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs:7` — add the eight variants to `FuzzValue` and to the `match fv` mapping:

```rust
#[derive(Arbitrary, Debug)]
enum FuzzValue {
    Null, Bool(bool), I64(i64), F64(f64), Text(String), Bytes(Vec<u8>),
    U64(u64), Decimal(String), Date(String), Time(String),
    Timestamp(String), TimestampTz(String), Uuid(String), Json(String),
}
```

The `fuzz` package is **not** a workspace member (`Cargo.toml:3`), so no gate catches a stale `FuzzValue` — CI's `cargo +nightly fuzz run roundtrip_frame` (`ci.yml:64`) would happily cover 6 of 14 variants forever. Verify by hand:

```bash
cd engine/crates/ferro-proto/fuzz && cargo check
```

- [ ] **Step 8: Pin the wire contract in PROTOCOL.md §3**

Replace the "registry constants only" claim for these eight tags with the payload-family table from Global Constraints, verbatim, including the fractional-second rule, the legal `DECIMAL` special values, the `"infinity"` / `"0000-00-00"` forms, and the still-deferred list. State explicitly that `str` payloads carry canonical text and that `bin` is not used by any S7 tag (with the PHP `str`/`bin` indistinguishability as the recorded reason), and that `U64` uses the **canonical narrowing** ladder (not a fixed `0xcf`).

- [ ] **Step 9: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd engine/crates/ferro-proto/fuzz && cargo check)
git add proto/PROTOCOL.md engine/crates/ferro-proto engine/crates/ferro-backend-pg/src/bind.rs \
        engine/crates/ferro-backend-mysql/src/bind.rs engine/crates/ferrod/src/services/sql.rs \
        engine/crates/ferro-e2e/src/main.rs
git commit -m "feat(m1-s7): pin text-canonical wire payloads for 8 canonical tags + widen the Rust TypedValue codec"
```

---

## Task 2: Hash the implemented tag set (fail version skew at the handshake)

> **v2 ordering (F1):** this was Task 3 in v1 and now runs **second**, before the golden-vector task, so that task's completeness guard can derive its required set from `/proto/types.toml`'s `implemented` list instead of a parallel hardcoded array — otherwise the two drift silently and recreate exactly the `m0_scalar` dead-documentation failure this task exists to fix.

**Files:**
- Modify: `proto/types.toml:5` (replace the dead `m0_scalar` with a lock-feeding `implemented` list)
- Modify: `engine/crates/ferro-proto/src/registry.rs:47` (`TypesToml` — currently parses only `tags`, which is *why* `m0_scalar` is dropped) **and** `:8` (`Registry`)
- Modify: **`engine/crates/ferro-proto/build.rs:11-27`** — its own `#[serde(deny_unknown_fields)] struct Registry`. **Omitting this makes `cargo build` panic and the whole workspace stop building** (hazard 38).
- Modify: `proto/registry.lock.json`, `php/client/src/Protocol/Generated/Constants.php:113`
- Test: `engine/crates/ferro-proto/tests/registry_sync.rs`, `php/client/tests/Conformance/RegistrySyncTest.php`

**Interfaces:**
- Produces: `Registry.implemented: Vec<String>` (sorted), present in `to_lock_json()`, and therefore a `TYPE_REGISTRY_HASH` that changes whenever the implemented-tag set changes — so `ferrod`'s handshake check rejects a skewed client immediately.

**Why:** all 18 tag *numbers* were already in the lock, so without this the hash does not move and an old M0 client passes the handshake, then throws `ProtocolException` mid-query on the first `DECIMAL` row (`M0ValuePolicy.php:33`). `m0_scalar` is currently **dead documentation** — `registry.rs:47-49`'s `TypesToml` reads only `tags`, it is not in the lock, and no test reads it. This task makes it real.

**Do NOT touch `fnv1a_hex` (F21).** Both generators already FNV-1a the **raw lock bytes** — `build.rs:104-110` hashes `lock_bytes`, `proto/tools/gen-php.php:14-31,69` hashes `$raw`. Parity is automatic; the hash moves the moment `implemented` lands in the lock file. Any "fix the framing so both agree" work is a no-op that actively risks *creating* the divergence it claims to prevent.

- [ ] **Step 1: Write the failing tests**

Append to `engine/crates/ferro-proto/tests/registry_sync.rs` (mirroring its existing path idiom at `:9-12`):

```rust
use ferro_proto::registry::Registry;
use std::path::PathBuf;

fn proto_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

/// The implemented-tag set is REAL (parsed + locked), not dead documentation like `m0_scalar`.
#[test]
fn implemented_tag_set_is_parsed_and_locked() {
    let reg = Registry::from_toml_dir(&proto_dir()); // infallible, takes &Path — no .expect()
    assert!(reg.implemented.iter().any(|t| t == "DECIMAL"));
    assert!(!reg.implemented.iter().any(|t| t == "ARRAY"), "ARRAY is deferred in S7");
    // Every name must be a real tag, or the vector guard (Task 3) cannot resolve it.
    for name in &reg.implemented {
        assert!(reg.tags.contains_key(name), "`implemented` names unknown tag {name}");
    }
    // SORTED: a cosmetic reorder of the TOML list must not mint a spurious handshake failure.
    let mut sorted = reg.implemented.clone();
    sorted.sort();
    assert_eq!(reg.implemented, sorted, "`implemented` must be emitted sorted");
    // And it reaches the lock — which is what the hash is taken over.
    let lock = reg.to_lock_json();
    assert!(lock.contains("\"implemented\""), "`implemented` must be in registry.lock.json");
    assert!(lock.contains("DECIMAL"));
}

/// TYPE_REGISTRY_HASH is FNV-1a over the committed lock BYTES (build.rs:118-127), so ANY edit to
/// `implemented` necessarily moves it. That — not a perturbation API — is the skew mechanism.
#[test]
fn type_registry_hash_is_fnv1a_of_the_lock_bytes() {
    let bytes = std::fs::read(proto_dir().join("registry.lock.json")).unwrap();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    assert_eq!(ferro_proto::consts::TYPE_REGISTRY_HASH, format!("{h:016x}"));
}

/// CROSS-LANGUAGE GUARD (new): nothing offline asserts the PHP constant matches the Rust one today,
/// so a stale `Constants.php` would only surface as an unbootable live handshake.
#[test]
fn php_generated_constant_matches_the_rust_hash() {
    let php = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../php/client/src/Protocol/Generated/Constants.php"),
    )
    .unwrap();
    let needle = "public const TYPE_REGISTRY_HASH = '";
    let start = php.find(needle).expect("Constants.php declares TYPE_REGISTRY_HASH") + needle.len();
    let hash = &php[start..start + 16];
    assert_eq!(
        hash,
        ferro_proto::consts::TYPE_REGISTRY_HASH,
        "php/client Constants.php is stale — run `php proto/tools/gen-php.php` and commit"
    );
}
```

**Note what is deliberately absent.** `Registry` derives no `Clone`, and has no `type_registry_hash()`, `implemented_tags()` or `remove_implemented_tag()` (hazard 38) — do not invent them. `implemented` is a plain public field, consistent with `tags`/`codes`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-proto --test registry_sync`
Expected: FAIL — `no field `implemented` on type `Registry``.

- [ ] **Step 3: Make the implemented set real in `/proto`**

In `proto/types.toml`, replace `m0_scalar` (keeping the top-level-key placement comment, which matters for TOML parsing). The list is **sorted** so the lock is reorder-stable:

```toml
# The tags implemented END-TO-END (engine both backends + PHP client). This list FEEDS
# registry.lock.json and therefore TYPE_REGISTRY_HASH: changing it changes the hash, so an
# engine/client pair with different type coverage fails FAST at the handshake instead of
# throwing mid-query on the first row of a newly-supported type (M1-S7 decision).
# SORTED so a cosmetic reorder cannot mint a spurious handshake failure.
# MUST precede the [tags] table so it is a TOP-LEVEL key, not absorbed as tags.implemented.
implemented = ["BOOL", "BYTES", "DATE", "DECIMAL", "F64", "I64", "JSON", "NULL",
               "TEXT", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "U64", "UUID"]
# Deferred (registry constants only; a loud NonRetryable{Unsupported}): ARRAY, INTERVAL, INET, VECTOR.
```

- [ ] **Step 4: Parse it in all THREE structs, then regenerate**

1. `engine/crates/ferro-proto/src/registry.rs:47` — add `implemented: Vec<String>` to `TypesToml` (today it parses only `tags`, which is exactly why `m0_scalar` vanished).
2. `engine/crates/ferro-proto/src/registry.rs:8` — add `pub implemented: Vec<String>` to `Registry`, and populate it in `from_toml_dir` **sorted** (`let mut v = t.implemented; v.sort();`). Field position in the struct determines its position in `to_lock_json()`; put it directly before `tags` so the lock reads naturally.
3. **`engine/crates/ferro-proto/build.rs:11-27`** — add `implemented: Vec<String>` to *its* `Registry`. This struct is `#[serde(deny_unknown_fields)]`, so **skipping this step makes `cargo build` panic** and nothing in the workspace compiles (hazard 38).

Then regenerate both artifacts and confirm the two hashes agree:

```bash
cargo run -p ferro-proto --bin gen-registry-lock
php proto/tools/gen-php.php
git diff --stat proto/registry.lock.json php/client/src/Protocol/Generated/Constants.php
grep TYPE_REGISTRY_HASH php/client/src/Protocol/Generated/Constants.php
```

Both must show the same 16 hex chars, and both must **differ from the pre-change `fc0807a28c0d0ab4`**. They will agree automatically — both hash the same raw bytes. If they somehow do not, the cause is a stale artifact (one generator not re-run), never the hash functions.

- [ ] **Step 5: Run both sync guards**

```bash
cargo test -p ferro-proto --test registry_sync
(cd php/client && ./vendor/bin/phpunit --filter RegistrySync)
```
Expected: PASS. `RegistrySyncTest` re-runs `gen-php.php` and diffs, so a stale `Constants.php` fails there; the new `php_generated_constant_matches_the_rust_hash` catches it from the Rust side too.

- [ ] **Step 6: Confirm the existing skew check still bites (do NOT add a harness)**

`engine/crates/ferrod/tests/handshake.rs:52 wrong_type_registry_hash_is_fatal_unsupported` **already covers this** (the check itself lives at `session/handshake.rs:31-34`). Confirm it still passes after the hash moves, and that PHP's `SessionHandshakeTest` agrees with the regenerated `Constants::TYPE_REGISTRY_HASH`:

```bash
cargo test -p ferrod --test handshake
(cd php/client && ./vendor/bin/phpunit --filter SessionHandshake)
```

- [ ] **Step 7: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): hash the implemented tag set so type-coverage skew fails at the handshake"
```

---

## Task 3: Golden vectors per tag + the PHP codec + a completeness guard

> **v2 ordering (F1):** this was Task 2 in v1 and now runs **third**, after the registry task, so the completeness guard derives from `Registry::from_toml_dir(&proto).implemented` rather than a hardcoded 14-tag array.

**Files:**
- Modify: `engine/crates/ferro-proto/src/bin/gen_vectors.rs`
- Create (via the generator — never hand-written, hazard 9): `proto/vectors/sql_exec_response_types_scalars.json`, `sql_exec_response_types_edge.json`, `sql_exec_response_types_u64.json`, `stream_data_types.json`
- Modify: `engine/crates/ferro-proto/tests/golden_vectors.rs:35` (replace the vacuous `assert!(count >= 7)`)
- Modify: `php/client/src/Protocol/Value.php:23,27`; `php/client/src/Protocol/SqlValueCodec.php:16,43`
- Modify: **`proto/PROTOCOL.md` §7 vector index (`:195`) and §8.3 SQL vector index (`:269`)** — both enumerate the committed vectors by name; a new vector that is not listed makes the DoD's "the relevant SPEC section still tells the truth" false in the same commit (F37).
- Test: `php/client/tests/Conformance/VectorConformanceTest.php`

**Interfaces:**
- Consumes: `Value::{U64,Decimal,…}` from Task 1; `Registry.implemented` from Task 2.
- Produces: the cross-language byte lock every later task relies on, and `Ferro\Protocol\Value`/`SqlValueCodec` support for all 14 implemented tags (the *codec* level — the *policy* level is Task 7).

- [ ] **Step 1: Write the failing completeness guard**

In `engine/crates/ferro-proto/tests/golden_vectors.rs`, replace the vacuous `assert!(count >= 7)` with a real per-tag requirement **derived from the registry** (hazard 5, F1):

```rust
/// Every tag in the registry's IMPLEMENTED set must have at least one committed golden vector
/// exercising it — and no DEFERRED tag may have one. The required set is derived from
/// /proto/types.toml (Task 2's single source of truth) so the two cannot drift; the old
/// `count >= 7` assertion was permanently satisfied and locked nothing.
#[test]
fn every_implemented_tag_has_a_vector() {
    use ferro_proto::registry::Registry;
    use std::path::PathBuf;

    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let reg = Registry::from_toml_dir(&proto);
    let seen = tags_present_in_committed_vectors();

    for name in &reg.implemented {
        let t = reg.tags[name];
        assert!(seen.contains(&t), "no golden vector exercises implemented tag {name} ({t})");
    }
    for (name, t) in &reg.tags {
        if !reg.implemented.contains(name) {
            assert!(
                !seen.contains(t),
                "a golden vector exercises DEFERRED tag {name} ({t}) — the vectors claim coverage \
                 the codec does not have"
            );
        }
    }
}
```

Implement `tags_present_in_committed_vectors() -> std::collections::BTreeSet<u8>` by decoding each committed vector's `frame_hex` with the **real codec** and walking its `ColMeta` tags and row `Value`s (`Value::tag()`), collecting the union. **Do not text-scan the JSON** — a JSON scan would pass on a vector whose bytes and `message` disagree, which is the exact class of drift the byte lock exists to catch.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-proto --test golden_vectors every_implemented_tag`
Expected: FAIL — `no golden vector exercises implemented tag U64 (3)`.

- [ ] **Step 3: Generate the vectors**

Extend `gen_vectors.rs` with **four** new vectors. The `sql_exec_response_*` / `stream_data_*` prefixes are **mandatory** — PHP's byte-lock generators key on them (hazard 6, `VectorConformanceTest.php:102`, `:161`).

| Vector | Contents | Why separate |
|---|---|---|
| `sql_exec_response_types_scalars` | `DECIMAL "-12345.6700"`, `DATE "2026-08-05"`, `TIME "13:45:07"`, `TIMESTAMP "2026-08-05 13:45:07.250000"`, `TIMESTAMPTZ "2026-08-05T13:45:07.250000Z"`, `UUID`, `JSON` (nested + a `null` + a non-ASCII char, proving UTF-8 survives), and a **small** `U64` (`5`) | the everyday shapes; keeps full ext-vs-pure parity coverage |
| `sql_exec_response_types_edge` | `DECIMAL "NaN"`, `DECIMAL "123456789012345678901234567890"` (bare 30-digit), `DATE "infinity"`, `TIME "24:00:00"` (PG-legal, chrono-hostile), `TIME "-838:59:58.000001"`, `TIMESTAMP` with a whole second (no `.ffffff` group), `DATE "0000-00-00"` | the sentinels and the fraction-omission rule |
| `sql_exec_response_types_u64` | **only** `U64 18446744073709551615`, rendered as a decimal **string** in the JSON (hazard 8) | **isolation is the point (F8):** a `> PHP_INT_MAX` uint64 makes `hasBigUint` skip the *whole vector's* `testExtPackerDecodeMatchesPureWhenLoaded`; putting it alone means the other 13 tags keep that parity coverage |
| `stream_data_types` | the scalars set again, as a `StreamData` frame | byte-locks the streamed path independently (`Connection::stream` decodes through the same `decodeRow`) |

**Hard constraint on any golden-vector `U64` (F8, hazard 7):** it must be **≤ `0xffffffff`** or **> `PHP_INT_MAX`** — **never** in `(2^32, 2^63]`. rmp emits marker `0xcf` from 2^32 up; `PurePacker::be()` returns a decimal **string** for every `0xcf` uint64 while ext-msgpack returns an `int`; `hasBigUint` does *not* skip it (the string is ≤ `PHP_INT_MAX`), so `assertEquals(json_encode($pure), json_encode($ext))` **fails in CI**, which provisions ext-msgpack. Hence `5` in the scalars vector and `u64::MAX` alone in its own.

The bare 30-digit `DECIMAL` in the edge vector is **deliberate** — it is the DBAL-realistic big-integer-in-a-`numeric` shape, and per the hazard-7 correction it costs only that vector's ext-vs-pure comparison, never the byte lock. Add a generator comment saying exactly that so nobody "fixes" it later.

Then regenerate and commit the bytes, and add all four names to `proto/PROTOCOL.md`'s §7 and §8.3 index tables:

```bash
cargo run -p ferro-proto --bin gen-vectors
```

- [ ] **Step 4: Add the PHP codec arms — with NO narrowing helpers**

`php/client/src/Protocol/Value.php` — add the eight factories and the eight `encode()` arms. **This is the BIND path, so hazard 30 applies here too (F18):** every new arm guards its payload explicitly and throws `CodecException` naming the tag; **never** `self::toStr`/`self::toInt` (which would turn a malformed payload into a silent empty-string **write**, and saturate a `u64::MAX` decimal string to `PHP_INT_MAX`).

```php
// factories
public static function u64(int|string $n): self { return new self(C::TAG_U64, $n); }
public static function decimal(string $s): self { return new self(C::TAG_DECIMAL, $s); }
// … date/time/timestamp/timestamptz/uuid/json identically …

// encode() arms
C::TAG_U64 => $p->packUint(self::requireUint($this->data)),   // NEVER packInt (hazard 29)
C::TAG_DECIMAL,
C::TAG_DATE,
C::TAG_TIME,
C::TAG_TIMESTAMP,
C::TAG_TIMESTAMPTZ,
C::TAG_UUID,
C::TAG_JSON => $p->packStr(self::requireStr($this->data, $this->tag)),

/** @throws CodecException */
private static function requireStr(mixed $v, int $tag): string
{
    if (!is_string($v)) {
        throw new CodecException("TypedValue tag {$tag}: expected a canonical-text string payload, got " . get_debug_type($v));
    }
    return $v;
}

/** @throws CodecException */
private static function requireUint(mixed $v): int|string
{
    if (is_int($v) && $v >= 0) { return $v; }
    if (is_string($v) && preg_match('/^\d+$/', $v) === 1) { return $v; }
    throw new CodecException('TypedValue tag ' . C::TAG_U64 . ': expected a non-negative int or decimal string, got ' . get_debug_type($v));
}
```

`php/client/src/Protocol/SqlValueCodec.php` — `encode` (`:16`) gains the same eight arms, routed through the new factories (so the guards live in one place). `fromWire` (`:43`) needs **no** new special case: the new tags ride the msgpack `str` family, so the `TAG_BYTES` → `list<int>` conversion stays the only one. **Record that in a comment** — adding a `bin`-family tag later would require the same treatment *plus* a vector-JSON workaround, which is precisely why the wire contract is text-canonical.

Add one unit test per new tag that a **non-string** payload throws `CodecException`, and one that a `TAG_U64` string above `PHP_INT_MAX` round-trips exactly.

- [ ] **Step 5: Run both conformance suites**

```bash
cargo test -p ferro-proto --test golden_vectors
(cd php/client && ./vendor/bin/phpunit --filter VectorConformance)
```
Expected: PASS both. **Verify coverage actually moved** — assert the byte-locked vector count went up by four, and that each new name matches the `sql_exec_response_*` / `stream_data_*` prefix rule (a `sqlVectors()`/`streamVectors()` count assertion, not a `var_dump`).

- [ ] **Step 6: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): golden vector per canonical tag (both codecs) + a registry-derived per-tag completeness guard"
```

---

## Task 4a: PG canonical-text decoders (`pgtext.rs`, pure byte math)

> **v2 split (F2):** v1's Task 4 bundled trivially-reviewable byte math with the safety-critical OID-gate change and a new live suite in one commit. 4a touches **exactly one new file** and is green and committable alone.

**Files:**
- Create: `engine/crates/ferro-backend-pg/src/pgtext.rs` (+ its `mod pgtext;` declaration in `lib.rs`)
- Modify (only if the dependency decision in Step 3 says yes): `engine/crates/ferro-backend-pg/Cargo.toml`
- Test: `engine/crates/ferro-backend-pg/src/pgtext.rs` unit tests

**Interfaces:**
- Produces: `pgtext::{numeric_to_text, date_to_text, time_to_text, timestamp_to_text, timestamptz_to_text, uuid_to_text, json_to_text}` — each `fn(&[u8]) -> Result<String, PoolError>` over the **raw binary payload** (`json_to_text` additionally takes a `jsonb: bool`). Nothing else in the crate is touched in 4a.
- Consumes: nothing from Task 1 (these return `String`; the `Value` wrapping happens in 4b).

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

    // ZERO: ndigits == 0 is legal and a naive digit loop emits "" (F23).
    #[test]
    fn numeric_zero_renders_at_its_declared_scale() {
        assert_eq!(numeric_to_text(&num_bytes("0")).unwrap(), "0");
        // dscale 4 with no digits at all -> the scale must still be honoured.
        assert_eq!(numeric_to_text(&num_header(0, 0, 0x0000, 4, &[])).unwrap(), "0.0000");
    }

    // weight < 0: the leading base-10000 groups are SKIPPED on the wire and must be re-emitted as
    // "0000" runs, or 1e-5 renders as "0.1" instead of "0.00001" (F23).
    #[test]
    fn numeric_reemits_skipped_leading_zero_groups() {
        // 0.00001 == digits [1000] at weight -2, dscale 5.
        assert_eq!(numeric_to_text(&num_header(1, -2, 0x0000, 5, &[1000])).unwrap(), "0.00001");
        assert_eq!(numeric_to_text(&num_bytes("0.00001")).unwrap(), "0.00001");
    }

    // A numeric(30,10) holding -12345.67 must ZERO-PAD the 4 available fractional digits to 10.
    #[test]
    fn numeric_pads_out_to_the_declared_scale() {
        assert_eq!(
            numeric_to_text(&num_header(3, 0, 0x4000, 10, &[1, 2345, 6700])).unwrap(),
            "-12345.6700000000"
        );
    }

    // dscale TRUNCATES, never rounds (matches PG's own ::text rendering).
    #[test]
    fn numeric_truncates_at_dscale_never_rounds() {
        assert_eq!(numeric_to_text(&num_header(2, 0, 0x0000, 1, &[1, 9900])).unwrap(), "1.9");
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

    // Malformed input is a Backend error (SPEC §9.1 decode-mismatch rule), never a panic.
    #[test]
    fn short_payloads_are_backend_errors_not_panics() {
        assert!(matches!(date_to_text(&[0u8; 3]), Err(PoolError::Backend(_))));
        assert!(matches!(uuid_to_text(&[0u8; 15]), Err(PoolError::Backend(_))));
        assert!(matches!(json_to_text(b"\x09{}", true), Err(PoolError::Backend(_))));
    }
}
```

Two test helpers, both in the test module: `num_header(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8>` builds the wire header directly (the *layout* oracle), and `num_bytes(&str) -> Vec<u8>` builds it from a decimal literal (the *ergonomic* oracle, written on top of `num_header`).

**Oracle caveat (F23) — read this before trusting a green run.** `num_bytes` is written by the same author as the decoder, so a shared misunderstanding of the layout passes every unit test here. That is why the header-level cases above bypass `num_bytes`, and why Task 4b Step 6 **re-verifies zero / `weight < 0` / padding / the 200-digit case against PG's own `::text` in the same query**. Neither half is sufficient alone.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-backend-pg pgtext`
Expected: FAIL — module/functions do not exist.

- [ ] **Step 3: Implement `pgtext.rs`**

Hand-decode over the raw binary payloads, formatting yourself so hazards 12-15 stay closed.

**The NUMERIC binary layout is stated here inline because there is no crate to crib it from (F9, hazard 10):** `postgres-protocol-0.6.12` contains **no numeric code at all** (`grep -rni numeric postgres-protocol-0.6.12/src` → zero hits). Source of truth is PG's `src/backend/utils/adt/numeric.c`.

```
header (8 bytes, all big-endian):
  i16 ndigits   number of base-10000 digit groups that FOLLOW
  i16 weight    base-10000 exponent of the FIRST digit group (0 => that group is the 1s..9999s
                place; NEGATIVE => leading all-zero groups were SKIPPED on the wire and MUST be
                re-emitted as "0000" runs)
  u16 sign      0x0000 positive | 0x4000 negative | 0xC000 NaN
                | 0xD000 +Infinity | 0xF000 -Infinity   (±Inf are PG14+; testkit is postgres:17)
  u16 dscale    DISPLAY scale — the number of fractional decimal digits to render. TRUNCATE at it,
                never round. This is what makes 1.10 and 1.1 distinct payloads.
then: ndigits x i16, each 0..=9999
```

Rendering rules that the unit tests above pin: `ndigits == 0` still renders `"0"` (or `"0." + "0" * dscale`); groups after the first are zero-padded to 4 chars; groups implied by a negative `weight` are emitted as `"0000"`; the fractional part is padded out to `dscale` and truncated at it; NaN/±Inf ignore everything else and return the literal text.

For `date`/`time`/`timestamp`/`timestamptz`/`uuid`, do the integer extraction from big-endian bytes directly (`i32`/`i64`/16 raw bytes) — no crate needed there either. Days/µs → civil date via a plain days-from-civil algorithm anchored at **2000-01-01** (hazard 12).

**Dependency decision (explicit, F9/hazard 21):** the default answer is **do not add `postgres-protocol`** — nothing above needs it, and hand-decoding `&[u8]` pulls in no crate. If an implementer nonetheless finds a reason to add it, pin it to **0.6.12** (the exact version the vendored fork resolves in `Cargo.lock`; a mismatched minor yields two crates whose traits will not unify, and `deny.toml`'s `multiple-versions = "allow"` will not catch it) and run `cargo deny check` in Step 5.

- [ ] **Step 4: Run the decoder tests**

Run: `cargo test -p ferro-backend-pg pgtext`
Expected: PASS.

- [ ] **Step 5: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
cargo deny check          # mandatory if Cargo.toml gained a dependency in Step 3
git add engine/crates/ferro-backend-pg/src/pgtext.rs engine/crates/ferro-backend-pg/src/lib.rs
git commit -m "feat(m1-s7): pgtext canonical-text decoders (numeric base-10000, 2000-epoch date/time, uuid, json)"
```

---

## Task 4b: PG read path — the OID gates + live round trip

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/pgtext.rs` (add the `RawBytes` `FromSql` newtype)
- Modify: `engine/crates/ferro-backend-pg/src/rowmap.rs` — `ExtractType`, `oid_extract_type` (`:57`), `oid_to_tag` (`:45`), `extract_value`, `unsupported_oid` (`:114`), and the `#[cfg(test)]` module (`:145-172`)
- Modify: `engine/crates/ferro-backend-pg/tests/pg_query_it.rs:164 query_out_of_m0_column_is_unsupported`
- Create: `engine/crates/ferro-backend-pg/tests/pg_types_it.rs` (live)

**Interfaces:**
- Consumes: `pgtext::*` (Task 4a), `Value::{Decimal,Date,…}` (Task 1).
- Produces: `ExtractType::{Numeric, Date, Time, Timestamp, TimestampTz, Uuid, Json, Jsonb}` + the matching `oid_to_tag` / `extract_value` arms; `pgtext::RawBytes`.

**Critical:** `oid_extract_type` stays the sole type authority. The `RawBytes` newtype has `accepts(_) -> true`, which **defeats tokio-postgres' own check** (hazard 16) — so it must never be reachable without first passing the OID gate. It is `pub(crate)` and every call site sits inside an `extract_value` arm that the gate already selected.

- [ ] **Step 1: Write the failing gate tests**

Replace `rowmap.rs:154 out_of_m0_oid_is_unsupported` (TIMESTAMPTZ/UUID/NUMERIC/JSONB are no longer out of scope) and add the two guards that actually bite:

```rust
#[test]
fn s7_oids_are_admitted_with_the_right_tag() {
    for (ty, want) in [
        (Type::NUMERIC, tag::DECIMAL),
        (Type::DATE, tag::DATE),
        (Type::TIME, tag::TIME),
        (Type::TIMESTAMP, tag::TIMESTAMP),
        (Type::TIMESTAMPTZ, tag::TIMESTAMPTZ),
        (Type::UUID, tag::UUID),
        (Type::JSON, tag::JSON),
        (Type::JSONB, tag::JSON),
    ] {
        assert!(oid_extract_type(ty.oid()).is_some(), "{ty:?} must be admitted in S7");
        assert_eq!(oid_to_tag(ty.oid()).unwrap(), want, "{ty:?} tag");
    }
}

/// The DEFERRAL guard (hazard 15/20). Note it asserts `oid_extract_type(..).is_none()` — it is a
/// deferral lock, NOT the hazard-16 "raw getter is unreachable" guard, which is structural
/// (`RawBytes` is pub(crate) and only called inside an already-gated `extract_value` arm).
#[test]
fn timetz_and_deferred_oids_stay_unsupported() {
    for ty in [Type::TIMETZ, Type::INT4_ARRAY, Type::INTERVAL, Type::INET] {
        assert!(oid_extract_type(ty.oid()).is_none(), "{ty:?} must stay Unsupported in S7");
        assert!(matches!(oid_to_tag(ty.oid()), Err(PoolError::Unsupported(_))));
    }
}
```

**Do NOT write `both_gates_cover_exactly_the_same_oid_set` (F22, hazard 45).** `ALL_KNOWN_OIDS` does not exist — `postgres-types` exposes no enumeration — and the assertion is a **tautology**: `oid_to_tag` is *defined* as a match over `oid_extract_type` (`rowmap.rs:45-53`), so the two agree by construction for every input. The assertion that bites is **HEAD-vs-producer**, and it needs a real cell, so it lives in Step 6 (live): for each newly-admitted OID, drive a cell through `extract_value` and assert its `Value::tag()` equals what `oid_to_tag` promised — i.e. that the tag `query.rs:67`/`:172` put in HEAD is the tag `:108`/`:245` actually emits (hazard 18).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ferro-backend-pg rowmap`
Expected: FAIL — `Type::NUMERIC must be admitted in S7`.

- [ ] **Step 3: Add the `RawBytes` `FromSql` newtype**

In `pgtext.rs`:

```rust
/// A raw-payload passthrough `FromSql`. `accepts` is universally true, which DEFEATS
/// tokio-postgres' own type check (hazard 16) — so this is `pub(crate)` and is ONLY ever
/// constructed inside an `extract_value` arm that `oid_extract_type` already selected.
/// `oid_extract_type` is the sole type authority; this newtype must never widen it.
pub(crate) struct RawBytes<'a>(pub &'a [u8]);

impl<'a> tokio_postgres::types::FromSql<'a> for RawBytes<'a> {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawBytes(raw))
    }
    fn accepts(_ty: &Type) -> bool { true }
}
```

- [ ] **Step 4: Widen `rowmap.rs` — both gates in the SAME change**

Extend `ExtractType` and `oid_extract_type` with `NUMERIC`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `JSON`, `JSONB`; extend `oid_to_tag` and `extract_value` **in the same edit** (hazard 18 — a HEAD promising a tag the producer cannot fill is the failure mode, and it fires mid-stream *after* HEAD is on the wire). Keep `TIMETZ` out (hazard 15). Route every new arm through `get_opt::<RawBytes>(row, idx)` → `pgtext::*_to_text` so a decode failure lands as `PoolError::Backend`, never `ConnectionLost` (§9.1 decode-mismatch rule).

Update `unsupported_oid` (`:114`) so its message no longer says "out-of-M0 … only NULL/BOOL/I64/F64/TEXT/BYTES are supported in M0" and instead enumerates the **current** supported set plus the named deferrals.

- [ ] **Step 5: Repoint the pre-S7 live assertion**

`engine/crates/ferro-backend-pg/tests/pg_query_it.rs:164 query_out_of_m0_column_is_unsupported` asserts `SELECT now()` is `Unsupported` — `timestamptz` is now **supported**, so this test (env-gated, therefore green offline) would only redden during the Step 7 live run. Repoint it at a genuinely deferred type and rename it:

```rust
/// A still-deferred column type (`interval`) is a loud `Unsupported`, raised before the query
/// runs, and the connection stays clean. (Was `now()`/timestamptz until M1-S7 implemented it.)
#[tokio::test(flavor = "multi_thread")]
async fn query_deferred_column_type_is_unsupported() {
    // … unchanged body, with:
    let err = co.query("SELECT '1 day'::interval", &[]).await
        .expect_err("interval is deferred past M1-S7");
```

Update the file-header contract comment in the same edit.

- [ ] **Step 6: Write the live per-type round-trip test**

`engine/crates/ferro-backend-pg/tests/pg_types_it.rs` — **skips (never fails)** without `FERRO_TEST_PG_URL`. Set the session `TimeZone` to something non-UTC **first** so a "server just happens to be UTC" false green is impossible:

```rust
co.simple_query("SET TIME ZONE 'America/New_York'").await.expect("non-UTC session zone");
```

Then, per type: create a temp table, insert via literal SQL, read back, assert the **exact canonical text**:

```rust
// Representative assertions — the full matrix covers every new tag.
("numeric(30,10)", "'-12345.6700000000'", Value::Decimal("-12345.6700000000".into())),
("numeric",        "'NaN'",               Value::Decimal("NaN".into())),
("numeric",        "'0'",                 Value::Decimal("0".into())),
("numeric",        "'0.00001'",           Value::Decimal("0.00001".into())),
("date",           "'2026-08-05'",        Value::Date("2026-08-05".into())),
("date",           "'infinity'",          Value::Date("infinity".into())),
("time",           "'24:00:00'",          Value::Time("24:00:00".into())),
("timestamp",      "'2026-08-05 13:45:07.25'", Value::Timestamp("2026-08-05 13:45:07.250000".into())),
("timestamptz",    "'2026-08-05 13:45:07.25+02'", Value::TimestampTz("2026-08-05T11:45:07.250000Z".into())),
("uuid",           "'3F2B8C1A-0000-4FFF-8000-ABCDEFABCDEF'", Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into())),
```

**Break the self-referential NUMERIC oracle (F23).** For the zero / `weight < 0` / scale-padding / 200-digit cases, do **not** assert against a hand-built expectation — assert against **PG's own rendering in the same query**, so the only shared assumption is the wire format itself:

```rust
// `col` is read through Ferro's decoder; `col::text` is PG's own canonical rendering.
let sql = "SELECT v, v::text FROM ferro_num";
// … assert Value::Decimal(ours) == Value::Text(pg_text) for every seeded value, including
//    0, 0.0000 (numeric(10,4)), 0.00001, -12345.67 in numeric(30,10), and the 200+50-digit value.
```

**The HEAD-vs-producer assertion (hazard 18/45)** lands here too: for each admitted OID, assert `res.cols[i].tag == res.rows[0][i].tag()` — HEAD's promise equals the producer's emission. For `jsonb`, assert **semantic** equality (PG normalizes jsonb key order/whitespace); for `json`, assert **byte-exact** passthrough.

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
cargo deny check
git commit -m "feat(m1-s7): PG canonical-text type coverage (numeric/date/time/timestamp(tz)/uuid/json) with both gates in lockstep"
```

---

## Task 5a: Pin the MySQL session zone + the canonical-text renderers (`mytext.rs`)

> **v2 split (F2) + new blocking step (F3).** Step 0 is a **prerequisite for everything date/time on MySQL** — Task 5b's renderer wiring, Task 8b's MySQL bind, and Task 9's round trips all depend on it. Do it first, in this task, and prove it on a fresh **and** a recycled connection.

**Files:**
- Modify: `engine/crates/ferro-backend-mysql/src/conn.rs:198` (the `OptsBuilder::setup(...)` list)
- Create: `engine/crates/ferro-backend-mysql/src/mytext.rs` (+ `mod mytext;` in `lib.rs`)
- Test: `mytext.rs` unit tests; a live UTC-pin assertion in `engine/crates/ferro-backend-mysql/tests/` (fresh + recycled conn)

**Interfaces:**
- Produces: `mytext::{date_to_text, datetime_to_text, timestamptz_to_text, time_to_text}` over `mysql_async::Value`'s already-parsed date/time components; and the invariant **every pooled MySQL/MariaDB session has `@@session.time_zone = '+00:00'`**.

- [ ] **Step 0 (BLOCKING): pin the MySQL session timezone to UTC**

**This is a settled decision — implement it, do not re-open it.**

*Why it is required.* MySQL stores `TIMESTAMP` in UTC but **converts it to the session `time_zone` on retrieval**, and the driver hands back zone-**less** components (`mysql_common-0.37.3/src/value/mod.rs:66,443-446`) — hazard 24. Nothing in the backend sets `time_zone` today: `conn.rs:198-205` sets only the three `session_track_*` vars, and `conn.rs:213` explicitly notes `time_zone` is left to the driver/server. So SPEC §9's `MySQL timestamp → TIMESTAMPTZ` mapping — a **UTC instant** — is only definable against a known zone, and under pooling an unpinned session zone makes the same column read differently depending on which connection serves the request. That is a correctness defect, not a preference.

*The visible consequence, accepted:* `NOW()`, `CURDATE()` and `CURTIME()` become **UTC** on every Ferro MySQL connection. This is what Doctrine and Laravel already do. The only alternative — remapping MySQL `TIMESTAMP` → `TAG_TIMESTAMP` (naive) — contradicts §9's table and is rejected. Task 9 Step 5 records the change in **§9.1 + §22.2**.

*How (no extra round trip).* `run_setup_commands` issues one `query_drop` **per element** of the setup list (`vendor/mysql-async/src/conn/mod.rs:1013-1019`) and re-runs the whole list after `COM_RESET_CONNECTION` (`:1249`). So fold the pin into the **existing** tracker SET rather than appending a fourth statement — connect cost and every recycle cost stay exactly where they are:

```rust
// engine/crates/ferro-backend-mysql/src/conn.rs:198
.setup(vec![
    "SET SESSION session_track_state_change = ON".to_string(),
    "SET SESSION session_track_transaction_info = 'STATE'".to_string(),
    // M1-S7: `time_zone` is folded into THIS statement (not appended as a 4th) so the UTC pin
    // costs no extra round trip on connect or on any COM_RESET_CONNECTION recycle
    // (run_setup_commands issues one query_drop per element and re-runs the list after reset).
    // WHY UTC: SPEC §9 maps MySQL `timestamp` -> TIMESTAMPTZ, a UTC INSTANT, and MySQL converts
    // TIMESTAMP into the SESSION zone on retrieval (mysql_common value/mod.rs:443-446) while the
    // driver returns zone-less components. Under pooling, an unpinned session zone makes the same
    // column read differently per connection. `time_zone` is in CURATED_SESSION_TRACK_VARS, so a
    // user `SET time_zone` still TAINTS (PinCause::SessionTracker) and hygiene restores this pin.
    // Consequence (recorded in SPEC §9.1 + §22.2): NOW()/CURDATE()/CURTIME() are UTC on every
    // Ferro MySQL connection — the same choice Doctrine/Laravel make.
    format!(
        "SET SESSION session_track_system_variables = '{CURATED_SESSION_TRACK_VARS}', \
         time_zone = '+00:00'"
    ),
])
```

Note `time_zone` is **already** in `CURATED_SESSION_TRACK_VARS` (`conn.rs:39-40`), so this composes with the S6 tracker design rather than fighting it.

Add a live test (skips without the env vars, runs on **both** engines) asserting the pin holds on a **fresh** and on a **recycled** connection:

```rust
async fn utc_pin_holds_fresh_and_recycled(backend: &MysqlBackend, label: &str) {
    let mut conn = backend.connect().await.expect("connect");
    let z = scalar_text(&mut conn, "SELECT @@session.time_zone").await;
    assert_eq!(z, "+00:00", "[{label}] fresh conn must be UTC-pinned");

    // Dirty it, then force the recycle path (COM_RESET_CONNECTION re-runs the setup list).
    backend.simple_query(&mut conn, "SET SESSION time_zone = '+05:30'").await.unwrap();
    backend.reset(&mut conn).await.expect("COM_RESET_CONNECTION");
    let z = scalar_text(&mut conn, "SELECT @@session.time_zone").await;
    assert_eq!(z, "+00:00", "[{label}] recycled conn must be re-pinned to UTC");
    conn.mysql.disconnect().await.ok();
}
```

Run it live before writing a single renderer — if the pin does not hold, every date/time assertion downstream is meaningless.

- [ ] **Step 1: Write the failing renderer tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value as MyValue;

    // DATETIME is naive; TIMESTAMP renders as a UTC instant — truthful ONLY because Step 0 pins
    // the session to '+00:00' (hazard 24). Same driver components, different rendering.
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
    // This is the ONLY MySQL-8 coverage of hazard 27 — its default sql_mode blocks a live insert
    // (F35), so the live case runs on MariaDB. Do not delete it.
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

    // A wrong-variant cell is a decode mismatch -> Backend, never ConnectionLost (§9.1).
    #[test]
    fn wrong_variant_is_a_backend_error() {
        assert!(matches!(date_to_text(&MyValue::Int(1)), Err(PoolError::Backend(_))));
    }
}
```

Verify the `MyValue::Time` and `MyValue::Date` field orders against the vendored `mysql_common` enum definition (`vendor/…/mysql_common-0.37.3/src/value/mod.rs:60-70`) before relying on them; correct the tests if the real layout differs, and record the actual layout in a doc comment on each renderer.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p ferro-backend-mysql mytext` → FAIL (module missing).

- [ ] **Step 3: Implement `mytext.rs`** — pure formatting over the driver's already-parsed components. **No date library** (the components are already split; a library would only re-introduce the wrap/negative hazards).

- [ ] **Step 4: Run the renderer tests** → PASS.

- [ ] **Step 5: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test -p ferro-backend-mysql -- --nocapture utc_pin
git commit -m "feat(m1-s7): pin MySQL/MariaDB sessions to time_zone='+00:00' (no extra round trip) + mytext canonical-text renderers"
```

---

## Task 5b: MySQL read path — the column-metadata classifier + live round trip

**Files:**
- Modify: `engine/crates/ferro-backend-mysql/src/rowmap.rs` — `MyKind` (`:43`), `column_kind` (`:59`), `column_to_tag` (`:121`), `extract_value` (`:135`), `unsupported` (`:191`); **add a `#[cfg(test)] mod tests`** — the file currently ends at line 207 with **no test module** (F27)
- Modify: **`engine/crates/ferro-backend-mysql/tests/query_it.rs:110-147 out_of_scope_column_is_unsupported`** + the file-header contract comment at `:7` (F27)
- Create: `engine/crates/ferro-backend-mysql/tests/mysql_types_it.rs` (live, BOTH engines)

**Interfaces:**
- Consumes: `mytext::*` and the UTC pin (Task 5a).
- Produces: `MyKind::{U64, Decimal, Date, Time, Timestamp, TimestampTz, Json}` + the matching `column_to_tag`/`extract_value` arms; `rowmap::as_u64`.

**The mapping (hazards 22-27):**

| MySQL/MariaDB column | `MyKind` | Tag | Note |
|---|---|---|---|
| `BIGINT UNSIGNED` | `U64` | `U64` | Only BIGINT — narrower unsigned fits `i64`. **Arrives as `MyValue::Int` OR `MyValue::UInt`** (hazard 23) |
| unsigned `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT` | `I64` | `I64` | **Lossless**, deliberately narrows the U64 surface |
| `TINYINT(1)` — signed **or unsigned** | `Bool` | `BOOL` | display length 1 is tested **FIRST** (hazard 44) |
| `DECIMAL`/`NEWDECIMAL` | `Decimal` | `DECIMAL` | Arrives as `Bytes` = the server's ASCII text; **pass through**, do not re-render |
| `DATE` | `Date` | `DATE` | zero dates render literally |
| `DATETIME` | `Timestamp` | `TIMESTAMP` | **Naive** |
| `TIMESTAMP` | `TimestampTz` | `TIMESTAMPTZ` | UTC instant — truthful **only** under the Task 5a UTC pin |
| `TIME` | `Time` | `TIME` | May be negative / exceed 24 h |
| **MySQL 8** `JSON` (`MYSQL_TYPE_JSON`) | `Json` | `JSON` | Arrives as `Bytes` = raw JSON text |
| **MariaDB** `JSON` | `Text` | `TEXT` | **By design** — it is `LONGTEXT` + a `json_valid()` CHECK with no recoverable metadata (hazard 25). Promoting utf8 `LONGTEXT` to JSON would be a silent miscast (charter rule 6) |
| `YEAR`, `BIT`, `ENUM`, `SET`, `GEOMETRY`, `VECTOR`, MariaDB native `UUID` | — | — | **Still a loud `Unsupported`** |
| `BINARY(16)` / `CHAR(36)` | `Bytes` / `Text` | unchanged | MySQL 8 has no UUID type (hazard 25) |

- [ ] **Step 1: Write the failing classifier tests (a NEW unit-test module)**

`rowmap.rs` has **no** `#[cfg(test)]` module today — create one. `Column` is constructible from a raw column-definition packet in `mysql_common`; if building one in a unit test proves impractical, assert the classification decisions through a thin `classify(ct, flags, len, charset)` helper that `column_kind` delegates to, and unit-test **that** (state whichever you chose in the module doc comment).

```rust
#[test]
fn tinyint_display_length_one_is_bool_even_when_unsigned() {
    // Hazard 44: the current arm tests `unsigned` FIRST (rowmap.rs:66-74), so TINYINT(1) UNSIGNED
    // falls through to I64 — contradicting the §9.1 TINYINT(1) -> Bool policy pinned in M1-S6.
    assert_eq!(classify(MYSQL_TYPE_TINY, NO_FLAGS, 1, UTF8), MyKind::Bool);
    assert_eq!(classify(MYSQL_TYPE_TINY, UNSIGNED_FLAG, 1, UTF8), MyKind::Bool);
    assert_eq!(classify(MYSQL_TYPE_TINY, UNSIGNED_FLAG, 4, UTF8), MyKind::I64);
}

#[test]
fn only_bigint_unsigned_reaches_u64() {
    assert_eq!(classify(MYSQL_TYPE_LONGLONG, UNSIGNED_FLAG, 20, BIN), MyKind::U64);
    for ct in [MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_INT24] {
        assert_eq!(classify(ct, UNSIGNED_FLAG, 11, BIN), MyKind::I64, "{ct:?} fits i64 losslessly");
    }
}

/// Hazard 25 / F15: a utf8 LONGTEXT must NEVER be promoted to Json — that is how MariaDB's
/// `JSON` column legitimately arrives, and guessing would be a silent miscast (charter rule 6).
#[test]
fn utf8_longtext_never_classifies_as_json() {
    assert_eq!(classify(MYSQL_TYPE_BLOB, NO_FLAGS, 4_294_967_295, UTF8), MyKind::Text);
    assert_eq!(classify(MYSQL_TYPE_JSON, NO_FLAGS, 0, UTF8), MyKind::Json);
}

#[test]
fn deferred_types_stay_unsupported() {
    for ct in [MYSQL_TYPE_YEAR, MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_GEOMETRY] {
        assert!(classify_result(ct, NO_FLAGS, 0, BIN).is_err(), "{ct:?} must stay Unsupported");
    }
}

/// Hazard 23 / F5: BIGINT UNSIGNED <= i64::MAX arrives as MyValue::Int, NOT UInt
/// (mysql_common value/mod.rs:320-329). A UInt-only extractor rejects the COMMON case
/// mid-row, after HEAD already promised U64.
#[test]
fn as_u64_accepts_both_driver_forms() {
    assert_eq!(as_u64(&col(), &MyValue::UInt(u64::MAX)).unwrap(), u64::MAX);
    assert_eq!(as_u64(&col(), &MyValue::Int(0)).unwrap(), 0);
    assert_eq!(as_u64(&col(), &MyValue::Int(5)).unwrap(), 5);
    assert_eq!(as_u64(&col(), &MyValue::Int(4_294_967_296)).unwrap(), 4_294_967_296);
    assert!(matches!(as_u64(&col(), &MyValue::Int(-1)), Err(PoolError::Backend(_))));
    assert!(matches!(as_u64(&col(), &MyValue::Double(1.0)), Err(PoolError::Backend(_))));
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p ferro-backend-mysql rowmap` → FAIL.

- [ ] **Step 3: Implement the `as_u64` helper**

Mirror the existing defensive `as_i64` (`rowmap.rs:156-165`):

```rust
/// A `BIGINT UNSIGNED` cell arrives as `UInt` ONLY when it exceeds `i64::MAX`
/// (mysql_common-0.37.3/src/value/mod.rs:320-329); every smaller value arrives as `Int`.
/// Accepting only `UInt` would reject the common case as a decode mismatch AFTER HEAD already
/// promised tag `U64` — the mid-stream failure mode hazard 18 describes.
fn as_u64(col: &Column, v: &MyValue) -> Result<u64, PoolError> {
    match v {
        MyValue::UInt(n) => Ok(*n),
        MyValue::Int(n) if *n >= 0 => Ok(*n as u64),
        MyValue::Int(n) => Err(decode_err(col, format!("negative value {n} in a BIGINT UNSIGNED column"))),
        other => Err(decode_err(col, format!("expected an integer cell, got {other:?}"))),
    }
}
```

- [ ] **Step 4: Widen `column_kind` / `column_to_tag` / `extract_value`**

Extend `MyKind` and `column_kind` per the table. `column_kind` remains the **ONE** classifier backing both `column_to_tag` and `extract_value` (the file's existing discipline — preserve it, it is what makes `cols` and `rows` unable to disagree).

Specific edits:
- **Reorder the `MYSQL_TYPE_TINY` arm (hazard 44):** test `column_length() == 1` → `Bool` **first**, signed or unsigned; only then split on width/sign.
- Split the unsigned integer arm so **only** `MYSQL_TYPE_LONGLONG + UNSIGNED_FLAG` reaches `U64`; the narrower unsigned widths join `I64`.
- `MYSQL_TYPE_JSON` → `Json` (MySQL 8 only — MariaDB never emits it).
- Update `unsupported` (`:191`) so its message enumerates the current supported set instead of "only NULL/BOOL/I64/F64/TEXT/BYTES are supported in M1".

- [ ] **Step 5: Repoint the pre-S7 live `Unsupported` assertion**

`engine/crates/ferro-backend-mysql/tests/query_it.rs:110-147 out_of_scope_column_is_unsupported` creates `ferro_oos (u BIGINT UNSIGNED, m DECIMAL(10,2))` and asserts **both** are `Unsupported` — both become **supported** in this task. It is env-gated, so it passes the offline gate and would only redden during the Step 7 live run. Repoint it at genuinely deferred types and update the file-header contract comment at `:7`:

```rust
"CREATE TEMPORARY TABLE ferro_oos (y YEAR, b BIT(8), e ENUM('a','b'), s SET('x','y'))"
// … then loop over ["y", "b", "e", "s"] asserting PoolError::Unsupported, unchanged otherwise.
```

- [ ] **Step 6: Write the live round-trip test (BOTH engines)**

`mysql_types_it.rs`, gated separately on `FERRO_TEST_MYSQL_URL` and `FERRO_TEST_MARIADB_URL` (the S6 pattern — two test fns calling one shared body, with an engine probe for the divergences). Cover:

- `DECIMAL(30,10)` — `-12345.6700000000` with the trailing zeros **preserved**.
- `BIGINT UNSIGNED` — **`0`, `5`, `4294967296` AND `18446744073709551615`** (F11). The small values are the ones that exercise the `MyValue::Int` arm; a suite testing only `u64::MAX` is green over the bug that breaks every real-world row.
- `DATETIME(6)` vs `TIMESTAMP(6)` — fractional seconds preserved, **and the instant proven, not the suffix** (F12):

```rust
// Insert a KNOWN instant, then read it back through Ferro under two DIFFERENT external client
// session zones. The engine's canonical text must be BYTE-IDENTICAL both times and must be the
// correct UTC instant. Asserting only "TIMESTAMP comes back with a Z" passes while the value is
// shifted by the session offset — the old assertion blessed the bug.
for external_zone in ["+05:30", "-08:00"] {
    let mut side = raw_conn(url).await;            // a SIDE connection, not the pooled one
    side.query_drop(format!("SET SESSION time_zone = '{external_zone}'")).await.unwrap();
    side.query_drop("INSERT INTO ferro_ts (ts, dt) VALUES ('2026-08-05 11:45:07.250000', \
                     '2026-08-05 11:45:07.250000')").await.unwrap();
    let got = pooled.query("SELECT ts, dt FROM ferro_ts ORDER BY id DESC LIMIT 1", &[]).await.unwrap();
    // ts was written under `external_zone`, so its UTC instant differs per zone — assert the
    // ENGINE's rendering matches the instant that zone implies, computed independently.
    assert_eq!(got.rows[0][0], Value::TimestampTz(expected_utc_for(external_zone).into()));
    // dt is NAIVE: identical text regardless of zone, and NEVER a Z suffix.
    assert_eq!(got.rows[0][1], Value::Timestamp("2026-08-05 11:45:07.250000".into()));
}
// And the pooled conn itself is UTC-pinned (Task 5a Step 0), fresh and recycled.
assert_eq!(scalar_text(&mut pooled, "SELECT @@session.time_zone").await, "+00:00");
```

- `TIME(6)` — a negative value and one exceeding 24 h.
- **`JSON` — engine-conditional, asserted explicitly, never skipped (F15):**

```rust
let want_json_tag = if is_mariadb { tag::TEXT } else { tag::JSON };
assert_eq!(res.cols[0].tag, want_json_tag,
    "[{label}] MariaDB JSON is LONGTEXT + a json_valid() CHECK with no recoverable metadata \
     (hazard 25) — it classifies as TEXT BY DESIGN. MySQL 8 emits MYSQL_TYPE_JSON -> JSON.");
```

  On MySQL, additionally assert **semantic** equality (the server normalizes JSON).
- **`DATE` zero date — lands on MariaDB (F35, hazard 27).** MySQL 8's *default* `sql_mode` includes `NO_ZERO_DATE,NO_ZERO_IN_DATE` (verified live on `testkit-mysql-1`; the compose file sets no override); MariaDB 11's does not. So: run the zero-date insert **unconditionally on MariaDB**, and on MySQL wrap it in an explicit per-statement `SET SESSION sql_mode = ''` (restoring it after) — a `SET SESSION` **taints** the connection via the tracker, which is correct and expected here. Do **not** write a silent skip branch: as v1 had it, the "unusual" branch was the only branch and hazard 27 got zero live verification on either engine.
- Still-`Unsupported`, on both engines: `YEAR`, `BIT(8)`, an `ENUM`, a `SET` — plus **MariaDB's native `UUID`** column type on MariaDB only (hazard 25).
- The **HEAD-vs-producer** assertion (hazard 18): `res.cols[i].tag == res.rows[0][i].tag()` for every covered type on both engines.

- [ ] **Step 7: Run offline + live on both engines**

```bash
cargo test -p ferro-backend-mysql
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test -p ferro-backend-mysql -- --nocapture
```
Expected: PASS on both; paste per-type actuals for MySQL **and** MariaDB, and record the two documented divergences (JSON→TEXT on MariaDB; zero dates blocked by MySQL's default `sql_mode`).

- [ ] **Step 8: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git commit -m "feat(m1-s7): MySQL/MariaDB canonical-text type coverage (u64 dual-form/decimal/date family/json) on the column-metadata classifier"
```

---

## Task 6: §9.1 policy plumbing (client-side)

**Files:**
- Create: `php/client/src/Client/Value/TypePolicyOptions.php`
- Create: `php/client/src/Client/Error/TypePolicyException.php`
- Modify: `php/client/src/Ferro.php` (`connect`/`connectTcp`/`assemble`), `php/client/src/Client/Connection.php:55-68`

**Interfaces:**
- Produces: `TypePolicyOptions { decimal: 'object'|'string', naiveDatetimeZone: 'utc'|'error', u64Overflow: 'object'|'string'|'error', uuid: 'object'|'string' }` with the **safe object forms as defaults** (§9.1); `TypePolicyException extends FerroException`.

**Key insight — where each policy lives.** Because the wire is text-canonical, three of the four policies are pure **client-side presentation** choices (`decimal`, `uuid`, `u64_overflow`): the engine always sends lossless canonical text and the client decides what PHP type to hand back. Only `naive_datetime_zone` has an engine component, and its `server` variant is **not implementable** without the backend session timezone on the wire (hazard 33) — so **S7 ships `utc` (default) and `error`; `server` is deferred to S8** with the `HELLO_ACK` pool metadata it needs, recorded in §22.2.

**No engine-side knobs in S7 (F29).** v1 planned four `PoolConfig` fields + `ferrod` env parsing. They are **dropped**: nothing in the engine reads them (the canonical text is policy-independent by design), so an operator setting `decimal=string` on a pool would observe **nothing**, while a typo in an inert setting would **prevent `ferrod` from booting** — the worst of both. Task 9 Step 5 instead amends §9.1 to state that in M1 the four knobs are **client-side**, and that pool-level defaults advertised via `HELLO_ACK` pool metadata are an S8 carry — the same metadata `naive_datetime_zone: server` already waits on. Record that rationale in a code comment on `TypePolicyOptions` so a later reader does not "fix" it by adding dead config.

**`naive_datetime_zone = error` — pinned scope (F30).** It applies to **`TAG_TIMESTAMP` only**. `TIMESTAMPTZ`, `DATE` and `TIME` decode normally under it. Intended use: failing loudly while migrating a schema from naive `datetime`/`timestamp` columns to tz-aware ones, so no naive column is silently read as UTC. Escapes: switch the pool's policy back to `utc`, or read the column with `RawStringValuePolicy` (Task 7) to get the canonical text verbatim.

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

```php
// php/client/tests/Unit/ConnectionArgumentTrapTest.php — hazard 34, widened to `plans:` (F38)
public function testCodecCannotBeCombinedWithValuesOrPlans(): void
{
    foreach ([['values' => new M0ValuePolicy()], ['plans' => new PlanCache()]] as $extra) {
        try {
            new Connection(session: $s, pool: 'p', codec: $codec, ...$extra);
            self::fail('codec: plus ' . array_key_first($extra) . ': must be rejected');
        } catch (\InvalidArgumentException $e) {
            self::assertStringContainsString('values', $e->getMessage());
            self::assertStringContainsString('plans', $e->getMessage());
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `(cd php/client && ./vendor/bin/phpunit --filter 'TypePolicyOptions|ConnectionArgumentTrap')` → FAIL (classes missing).

- [ ] **Step 3: Implement `TypePolicyOptions` and `TypePolicyException`**

`TypePolicyOptions`: a `final readonly` class validating each knob in the constructor, rejecting `server` with a message pointing at the S8 deferral, and carrying the "why there is no engine-side knob" comment above.

`TypePolicyException extends FerroException` (`php/client/src/Client/Error/`): the **policy-refusal** class (hazard 32 / F30). Its doc comment must state the split explicitly — a **malformed** payload stays `ProtocolException` (a wire fault), an operator **policy refusal** (`naive_datetime_zone=error`, `u64_overflow=error`) is this class — because S8's DBAL `ExceptionConverter` would otherwise misreport a configuration choice as a driver protocol failure.

- [ ] **Step 4: Plumb it through `Ferro` and close the `Connection` argument trap**

Add an optional `?TypePolicyOptions $types = null` to `Ferro::connect`/`connectTcp`, threaded into `Ferro::assemble` (`Ferro.php:83-89`) which today constructs `new Connection(...)` with no `values:` at all — so pass `values: $types === null ? null : new M1ValuePolicy($types)`.

**Close the silent-discard trap in the same step (hazard 34, widened by F38).** `Connection.php:63-68` currently discards **both** `$values` **and** `$plans` when `codec:` is supplied. Throw a single `InvalidArgumentException` naming both when `codec:` arrives with either. No in-repo caller passes `codec:`, so making them mutually exclusive is safe today.

- [ ] **Step 5: Run the suites** — `(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)` → PASS/clean.

- [ ] **Step 6: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
git commit -m "feat(m1-s7): §9.1 type-policy options (safe object defaults, client-side in M1) + TypePolicyException + close the codec/values/plans discard trap"
```

---

## Task 7: PHP read path — the M1 ValuePolicy + the §9 value objects

**Files:**
- Create: `php/client/src/{Decimal,Date,Time,Uuid,Json,U64,NaiveTimestamp}.php` — namespace `Ferro`, at the **`src/` root** (F16/hazard 40: `composer.json:15` maps `Ferro\ => src/`, so `src/Value/Decimal.php` declaring `namespace Ferro;` cannot autoload and every Task 7/8/9 test would fatal on class-not-found). Chosen over renaming to `Ferro\Value\*` because it keeps §9's PHP column (`Ferro\Decimal`, `Ferro\Uuid`, …) literally true with no spec amendment.
- Create: `php/client/src/Client/Value/M1ValuePolicy.php`, `php/client/src/Client/Value/RawStringValuePolicy.php`
- Modify: `php/client/src/Client/Connection.php:63-68` (the default-policy site)
- Test: `php/client/tests/Unit/{ValueObjectsTest,M1ValuePolicyTest}.php`

**Interfaces:**
- Consumes: `TypePolicyOptions`, `TypePolicyException` (Task 6), `Constants::TAG_*`.
- Produces: `M1ValuePolicy implements ValuePolicy` covering all 14 implemented tags; `RawStringValuePolicy implements ValuePolicy` returning canonical text verbatim; `Ferro\Decimal` (string-backed, exact), `Ferro\Date`, `Ferro\Time`, `Ferro\Uuid`, `Ferro\Json` (lazy), `Ferro\U64`, `Ferro\NaiveTimestamp extends \DateTimeImmutable`.

**The naive-vs-instant decision (F14) — settled here, used by Task 8a.** `TAG_TIMESTAMPTZ` hydrates to a plain `\DateTimeImmutable` in UTC. `TAG_TIMESTAMP` hydrates to **`Ferro\NaiveTimestamp extends \DateTimeImmutable`**. *Why this option:* it is the only one that makes the round trip **read → write-back → read** byte-stable without amending §9 — §9's PHP column says naive `TIMESTAMP` hydrates to `DateTimeImmutable`, and a subclass **is** a `DateTimeImmutable` (`instanceof` holds, every `format()`/`getTimestamp()` call is unchanged), while giving `bindOne` a discriminator so a naive value binds back as `TAG_TIMESTAMP` instead of being silently re-interpreted as a UTC instant. The alternative (plain `DateTimeImmutable` always means an instant, naive needs a brand-new non-`DateTimeImmutable` object) contradicts §9's PHP column and breaks every `instanceof DateTimeInterface` in user code. **Task 8a's `bindOne` must match `NaiveTimestamp` BEFORE `DateTimeImmutable`** — subclass first, or every naive value binds as an instant. Recorded in §9 + §22.2 by Task 9 Step 5.

**Non-negotiable (hazard 30):** every arm **throws** on a bad payload — `ProtocolException` for a **malformed** payload, `TypePolicyException` for a **policy refusal** (hazard 32 / F30). Do **not** reuse `M0ValuePolicy::toInt/toFloat/toStr` or the `SqlValueCodec` helpers — they return `0`/`0.0`/`''` and would turn a bad `DECIMAL` into `Decimal('')`.

- [ ] **Step 1: Write the failing policy tests**

```php
// php/client/tests/Unit/M1ValuePolicyTest.php

protected function setUp(): void
{
    // F26: format('Y-m-d H:i:s.u') returns the same string in ANY zone, so a naive assertion made
    // under a UTC default is satisfied by a WRONGLY-zoned object — which then shifts on write-back.
    // Run the whole class in a non-UTC default so the zone assertions below can actually fail.
    $this->prevTz = date_default_timezone_get();
    date_default_timezone_set('America/New_York');
}
protected function tearDown(): void { date_default_timezone_set($this->prevTz); }

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
    // A POLICY REFUSAL, not a wire fault (F30).
    $this->expectException(TypePolicyException::class);
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

/** F31: the string policies must be exercised, not just declared. */
public function testStringPoliciesReturnCanonicalTextVerbatim(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions(decimal: 'string', uuid: 'string'));
    self::assertSame('1.10', $p->decode(C::TAG_DECIMAL, '1.10'));
    self::assertSame('3f2b8c1a-0000-4fff-8000-abcdefabcdef',
        $p->decode(C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'));
}

public function testTimestampTzIsAUtcInstantAndTimestampIsNaive(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    $tz = $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z');
    self::assertInstanceOf(\DateTimeImmutable::class, $tz);
    self::assertSame('UTC', $tz->getTimezone()->getName());
    self::assertSame('2026-08-05 13:45:07.250000', $tz->format('Y-m-d H:i:s.u'));

    $naive = $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000');
    self::assertInstanceOf(NaiveTimestamp::class, $naive);        // binds back as TAG_TIMESTAMP
    self::assertInstanceOf(\DateTimeImmutable::class, $naive);    // §9's PHP column stays true
    self::assertSame('2026-08-05 13:45:07.250000', $naive->format('Y-m-d H:i:s.u'));
    // F26: under naive_datetime_zone=utc the object is EXPLICITLY UTC — these two assertions are
    // what make the test able to fail while date_default_timezone_set('America/New_York').
    self::assertSame('UTC', $naive->getTimezone()->getName());
    self::assertSame(0, $naive->getOffset());
}

/** F30: `error` is scoped to TAG_TIMESTAMP ONLY — the other date/time tags decode normally. */
public function testNaiveDatetimeZoneErrorIsScopedToTimestampOnly(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions(naiveDatetimeZone: 'error'));
    self::assertInstanceOf(\DateTimeImmutable::class, $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z'));
    self::assertInstanceOf(Date::class, $p->decode(C::TAG_DATE, '2026-08-05'));
    self::assertInstanceOf(Time::class, $p->decode(C::TAG_TIME, '13:45:07'));
    $this->expectException(TypePolicyException::class);   // a policy refusal, not a wire fault
    $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07');
}

/** Hazard 30: MALFORMED payloads throw ProtocolException — never a silent zero/empty coercion. */
public function testMalformedPayloadsThrowAndNeverCoerce(): void
{
    $p = new M1ValuePolicy(new TypePolicyOptions());
    foreach ([[C::TAG_DECIMAL, 'not-a-number'], [C::TAG_DATE, '2026-13-99'],
              [C::TAG_UUID, 'nope'], [C::TAG_TIMESTAMP, ''], [C::TAG_U64, 'x1'],
              [C::TAG_DECIMAL, 42], [C::TAG_JSON, ['a']]] as [$tag, $bad]) {
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
    $bad = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_JSON, '{oops');
    $this->expectException(ProtocolException::class);
    $bad->decoded();
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

/** F31: the S8 DBAL hand-off — a whole row of driver-native strings, no value objects. */
public function testRawStringPolicyReturnsCanonicalTextForEveryTag(): void
{
    $p = new RawStringValuePolicy();
    self::assertSame('1.10', $p->decode(C::TAG_DECIMAL, '1.10'));
    self::assertSame('2026-08-05', $p->decode(C::TAG_DATE, '2026-08-05'));
    self::assertSame('13:45:07', $p->decode(C::TAG_TIME, '13:45:07'));
    self::assertSame('2026-08-05 13:45:07.250000', $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'));
    self::assertSame('2026-08-05T13:45:07.250000Z', $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z'));
    self::assertSame('3f2b8c1a-0000-4fff-8000-abcdefabcdef', $p->decode(C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'));
    self::assertSame('{"a":1}', $p->decode(C::TAG_JSON, '{"a":1}'));
    self::assertSame('18446744073709551615', $p->decode(C::TAG_U64, '18446744073709551615'));
    // The M0 scalars keep their natural PHP types — only the 8 new tags become strings.
    self::assertSame(7, $p->decode(C::TAG_I64, 7));
    self::assertTrue($p->decode(C::TAG_BOOL, true));
}
```

- [ ] **Step 2: Run to verify it fails** — `(cd php/client && ./vendor/bin/phpunit --filter M1ValuePolicy)` → FAIL.

- [ ] **Step 3: Implement the value objects (at `src/` root, namespace `Ferro`)**

Each `final readonly` (except `NaiveTimestamp`, which extends `\DateTimeImmutable` and therefore cannot be), each with `__toString()` returning the **canonical wire text** (so a round-trip through bind is byte-stable), and each validating in the constructor:

- `Ferro\Decimal` — string-backed and exact; accepts `NaN`/`Infinity`/`-Infinity`; **never** normalizes (`1.10` stays `1.10`).
- `Ferro\Date`, `Ferro\Time` — canonical text; `Time` accepts a leading `-` and hours > 23; `Date` accepts `infinity`/`-infinity`/`0000-00-00`.
- `Ferro\Uuid` — 36-char lowercase hyphenated; rejects anything else.
- `Ferro\Json` — stores the raw text, decodes **lazily** in `decoded()` and caches; invalid JSON throws `ProtocolException` on access, not on construction.
- `Ferro\U64` — string-backed for values above `PHP_INT_MAX`.
- `Ferro\NaiveTimestamp extends \DateTimeImmutable` — constructed with an explicit `new \DateTimeZone('UTC')` under `naive_datetime_zone=utc`; carries a doc comment stating it exists purely as the bind-back discriminator (F14) and that `bindOne` must match it **before** `DateTimeImmutable`.

- [ ] **Step 4: Implement `M1ValuePolicy` and `RawStringValuePolicy`**

`M1ValuePolicy`: all 14 implemented tags plus a named throw for the 4 deferred ones. The U64 arm must normalize **both** wire forms and compare against `PHP_INT_MAX` itself (hazard 28) — never branch on `is_int($data)`. PHPStan L9 requires explicit `is_string`/`is_int` narrowing before use (hazard 36); a bare cast is the lossy anti-pattern hazard 30 forbids.

`RawStringValuePolicy`: the **S8 DBAL hand-off** the slice definition promises (F31). Returns the canonical wire text **verbatim** for all eight new tags (and keeps the M0 scalars as their natural PHP types), which is exactly what DBAL 4's `DateTimeType::convertToPHPValue`, `DecimalType` and `JsonType` want — they parse strings themselves. No policy knobs; it is the identity policy for the new tag set.

- [ ] **Step 5: Switch the default policy**

The default-policy site is **`Connection.php:63-68`** — `$values ?? new M0ValuePolicy()` — which `Ferro::assemble` (`Ferro.php:83-89`) relies on. `ExecCodec` has **no** defaultable policy parameter (it is the first of four **required** constructor args, hazard 39), so do not try to change it there. Make `M1ValuePolicy` the default at that one site, keeping `M0ValuePolicy` available for its existing tests.

**Do NOT change `Connection.php:209`'s `$colNames` shape (F25, hazard 47).** v1 asked to "preserve the per-column tag alongside the name" on the streamed path; its premise is false. Row decoding is driven by the **per-cell** tag (`ExecCodec.php:117-122`), and the **buffered** path drops the `ColMeta` tag too (`:83-88`). Widening `list<string>` would break `assocRow` (`:144`), `hydrateDto` and `PlanCache::planFor` — a PHPStan L9 failure and a `TypeError` — for zero behavior gain. Instead:

- **Keep** the regression lock: a test that a **streamed** row and a **buffered** row of the same data decode to **equal** values.
- Add a comment at `Connection.php:209` recording that the `ColMeta` tag is intentionally unused client-side, and that the per-cell tag is the decode authority — so nobody "fixes" this again.

- [ ] **Step 6: Run the suites** — `(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)` → PASS/clean.

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(m1-s7): M1ValuePolicy + §9 value objects (exact Decimal, lazy Json, U64 dual-form, NaiveTimestamp) + RawStringValuePolicy for the S8 DBAL tier"
```

---

## Task 8a: The PHP bind path

> **v2 split (F2):** the PHP half is independently green and committable — it needs no engine change, because `SqlValueCodec`/`Value.php` already gained their arms in Task 3.

**Files:**
- Modify: `php/client/src/Client/ExecCodec.php:179,185` (`bindParams` call site + `bindOne`)
- Modify: `php/client/src/Protocol/Msgpack/ExtPacker.php:13` (`packUint` hardening)
- Test: `php/client/tests/Unit/BindTest.php`

**Interfaces:**
- Consumes: the value objects + `M1ValuePolicy` (Task 7), `Value`/`SqlValueCodec` tag support (Task 3).
- Produces: `ExecCodec::bindOne(mixed $v): array{tag:int,data:mixed}` as a **non-static public `@internal`** method covering the value objects, `NaiveTimestamp`, and native `DateTimeImmutable`/`DateTime`.

**Why this task exists (hazard 31):** the `ValuePolicy` seam is **decode-only**. Without it the slice is read-only and a DBAL suite — which binds `DateTime`s and decimals constantly — is still broken.

- [ ] **Step 1: Write the failing bind tests — against the REAL signatures**

All four v1 snippets fataled before reaching their intended red state (hazard 39). The corrected forms:

```php
// php/client/tests/Unit/BindTest.php

/** ExecCodec takes FOUR required args (ExecCodec.php:37-42) — one factory for the whole class. */
private function codec(): ExecCodec
{
    return new ExecCodec(
        new M1ValuePolicy(new TypePolicyOptions()),
        new PlanCache(),
        new PurePacker(),
        new PurePacker(),
    );
}

/** Hazard 29: a U64 above PHP_INT_MAX MUST go through packUint, never packInt. */
public function testU64BindsViaPackUintAndSurvivesTheFullRange(): void
{
    $big  = '18446744073709551615';
    $p    = new PurePacker();
    // SqlValueCodec::encode(PackerInterface $p, mixed $vj) — PACKER FIRST (SqlValueCodec.php:16).
    $wire = SqlValueCodec::encode($p, ['tag' => C::TAG_U64, 'data' => $big]);
    // unpack is an INSTANCE method with a BY-REF offset (Msgpack/PurePacker.php:81).
    $off  = 0;
    self::assertSame($big, SqlValueCodec::fromWire($p->unpack($wire, $off))['data']);
    // Byte-level: marker 0xcf. A regression to packInt cannot silently pass this.
    self::assertSame(0xcf, ord($wire[2]), 'TAG_U64 payload must carry the uint64 marker');
}

public function testValueObjectsBindToTheirCanonicalText(): void
{
    foreach ([
        [new Decimal('1.10'),                              C::TAG_DECIMAL, '1.10'],
        [new Date('2026-08-05'),                           C::TAG_DATE,    '2026-08-05'],
        [new Time('24:00:00'),                             C::TAG_TIME,    '24:00:00'],
        [new Uuid('3f2b8c1a-0000-4fff-8000-abcdefabcdef'), C::TAG_UUID,    '3f2b8c1a-0000-4fff-8000-abcdefabcdef'],
        [new Json('{"a":1}'),                              C::TAG_JSON,    '{"a":1}'],
        [new U64('18446744073709551615'),                  C::TAG_U64,     '18446744073709551615'],
    ] as [$obj, $tag, $text]) {
        $bound = $this->codec()->bindOne($obj);   // non-static, public @internal (Step 3)
        self::assertSame($tag,  $bound['tag']);
        self::assertSame($text, $bound['data']);
    }
}

/** F14: a plain DateTimeImmutable is an INSTANT -> TIMESTAMPTZ, UTC-normalized. */
public function testDateTimeImmutableBindsAsTimestampTz(): void
{
    $dt = new \DateTimeImmutable('2026-08-05 13:45:07.250000', new \DateTimeZone('+02:00'));
    $bound = $this->codec()->bindOne($dt);
    self::assertSame(C::TAG_TIMESTAMPTZ, $bound['tag']);
    self::assertSame('2026-08-05T11:45:07.250000Z', $bound['data']);
}

/** F14: NaiveTimestamp is a SUBCLASS of DateTimeImmutable — it must be matched FIRST, or every
 *  naive value silently becomes a UTC instant on write-back. This test is the guard. */
public function testNaiveTimestampBindsBackAsTimestampNotTimestampTz(): void
{
    $naive = (new M1ValuePolicy(new TypePolicyOptions()))
        ->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000');
    $bound = $this->codec()->bindOne($naive);
    self::assertSame(C::TAG_TIMESTAMP, $bound['tag'], 'subclass arm must precede DateTimeImmutable');
    self::assertSame('2026-08-05 13:45:07.250000', $bound['data'], 'byte-stable read->write round trip');
}

public function testUnbindableValueStillThrows(): void
{
    $this->expectException(ProtocolException::class);
    $this->codec()->bindOne(new \stdClass());
}

/** Hazard 48 / F19: ExtPacker::packUint must not silently (int)-cast a big decimal string. */
public function testExtPackerRejectsOrPreservesBigUintStrings(): void
{
    if (!\extension_loaded('msgpack')) { $this->markTestSkipped('ext-msgpack not loaded'); }
    $big = '18446744073709551615';
    try {
        $out = (new ExtPacker())->packUint($big);
        self::assertSame(0xcf, ord($out[0]), 'if it encodes, it must be a real uint64');
    } catch (CodecException $e) {
        self::assertStringContainsString('PHP_INT_MAX', $e->getMessage());
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `(cd php/client && ./vendor/bin/phpunit --filter BindTest)` → FAIL.

- [ ] **Step 3: Promote `bindOne` and implement the arms**

**API change, stated explicitly (F24):** `ExecCodec::bindOne` is **`private static`** today (`:185`), called via `self::bindOne($v)` at `:179`. Promote it to a **non-static `public` method marked `@internal`** and change the call site to `$this->bindOne($v)`. The reason is not testability: the `DateTimeImmutable` handling needs instance state (the policy / `TypePolicyOptions` reached through `$this->values`) to decide fraction rendering and to keep the naive/instant rule in one place.

Arm order matters — **subclass first**:

```php
public function bindOne(mixed $v): array
{
    return match (true) {
        $v === null   => ['tag' => C::TAG_NULL, 'data' => null],
        is_bool($v)   => ['tag' => C::TAG_BOOL, 'data' => $v],
        is_int($v)    => ['tag' => C::TAG_I64,  'data' => $v],
        is_float($v)  => ['tag' => C::TAG_F64,  'data' => $v],
        is_string($v) => ['tag' => C::TAG_TEXT, 'data' => $v],
        // --- M1-S7 value objects (canonical text is their __toString) ---
        $v instanceof Decimal => ['tag' => C::TAG_DECIMAL, 'data' => (string) $v],
        $v instanceof Date    => ['tag' => C::TAG_DATE,    'data' => (string) $v],
        $v instanceof Time    => ['tag' => C::TAG_TIME,    'data' => (string) $v],
        $v instanceof Uuid    => ['tag' => C::TAG_UUID,    'data' => (string) $v],
        $v instanceof Json    => ['tag' => C::TAG_JSON,    'data' => (string) $v],
        $v instanceof U64     => ['tag' => C::TAG_U64,     'data' => $v->wireValue()], // int|string
        // NaiveTimestamp EXTENDS DateTimeImmutable — it MUST be tested first (F14).
        $v instanceof NaiveTimestamp   => ['tag' => C::TAG_TIMESTAMP,   'data' => self::naiveText($v)],
        $v instanceof \DateTimeInterface => ['tag' => C::TAG_TIMESTAMPTZ, 'data' => self::utcText($v)],
        default => throw new ProtocolException(sprintf(
            'unsupported bind parameter type %s', get_debug_type($v),
        )),
    };
}
```

`utcText()` converts to UTC then renders `Y-m-d\TH:i:s` + the fraction rule + `Z`; `naiveText()` renders `Y-m-d H:i:s` + the fraction rule, **never** a suffix. Both honour the §3 rule: **no** `.ffffff` group when the sub-second part is zero, otherwise exactly 6 digits.

Also harden **`ExtPacker::packUint`** (`Msgpack/ExtPacker.php:13`, hazard 48): it currently does `is_string($n) ? (int) $n : $n`, silently corrupting any `u64` above `PHP_INT_MAX`. This task creates the first string-carrying call path. Either throw `CodecException` for an out-of-range string, or delegate to the limb encoder — **not** a silent cast.

- [ ] **Step 4: Run the suites** — `(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)` → PASS/clean.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(m1-s7): PHP bind path for all canonical tags (value objects + naive/instant DateTime rule) + harden ExtPacker::packUint"
```

---

## Task 8b: The engine bind path — both backends

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/bind.rs` — `value_to_boxed` (`:42`), `accepts` (`:67`), the test module (`:124-142 accepts_mirrors_boxed_binding`)
- Modify: `engine/crates/ferro-backend-mysql/src/bind.rs:48` (`value_to_my` — the date/time arms Task 1 left as `Bytes`)
- Test: per-backend bind unit tests; live coverage folded into Task 9

**Interfaces:**
- Produces (PG): seven text-format `ToSql` newtypes — `PgDecimalText`, `PgDateText`, `PgTimeText`, `PgTimestampText`, `PgTimestampTzText`, `PgUuidText`, `PgJsonText` — each with a **narrow** `accepts`, plus the mirrored `accepts` arms.
- Produces (MySQL): typed `MyValue::Date`/`MyValue::Time` construction from canonical text.

**The directional rule (hazard 19, restated by F17).** `accepts` may be **stricter** than the concrete `ToSql` it fronts — that yields a clean, diagnosable known-fate rejection — but it must **never be looser**, because a looser `accepts` lets `to_sql_checked` fail **post-send**, the false-`Indeterminate` path the pre-validation exists to prevent. Concretely: **one shared newtype is forbidden.** A single newtype would have to `accepts` every target type the eight tags collectively touch (`NUMERIC` ∪ `DATE` ∪ `TIME` ∪ `TIMESTAMP` ∪ `TIMESTAMPTZ` ∪ `UUID` ∪ `JSON` ∪ `JSONB`), turning the §19.3 pre-flight into a **no-op for all eight new tags**. And **never** copy `PgNull`'s `accepts(_ty) -> true` (`bind.rs:29-31`) — that is legitimate only for a typed NULL slot.

- [ ] **Step 1: Write the failing PG bind tests**

Extend the existing `accepts_mirrors_boxed_binding` (`bind.rs:124-142`) with an **exhaustive `(Value, Type, bool)` table** — one accepting `Type` per variant plus at least one **rejecting** `Type` per variant. (v1 prescribed `assert_eq!(accepts(&v), value_to_boxed(v.clone()).is_ok())`, which **does not compile**: `accepts` is `(v: &Value, ty: &Type) -> bool` and `value_to_boxed` returns a `Box`, not a `Result` — hazard 37.)

```rust
/// Hazard 19 (directional): `accepts` may be STRICTER than the boxed impl, never LOOSER. Each new
/// tag gets its OWN narrow newtype — a shared one would accept every target type the eight tags
/// touch and silently disable the §19.3 pre-flight for all of them.
#[test]
fn s7_accepts_is_narrow_per_tag() {
    let cases: &[(Value, Type, bool)] = &[
        (Value::Decimal("1.10".into()),     Type::NUMERIC,     true),
        (Value::Decimal("1.10".into()),     Type::DATE,        false),
        (Value::Decimal("1.10".into()),     Type::INT4,        false),
        (Value::Date("2026-08-05".into()),  Type::DATE,        true),
        (Value::Date("2026-08-05".into()),  Type::TIMESTAMP,   false),
        (Value::Time("24:00:00".into()),    Type::TIME,        true),
        (Value::Time("24:00:00".into()),    Type::TIMETZ,      false),  // hazard 15 stays closed
        (Value::Timestamp("2026-08-05 00:00:00".into()),   Type::TIMESTAMP,   true),
        (Value::Timestamp("2026-08-05 00:00:00".into()),   Type::TIMESTAMPTZ, false), // no zone guess
        (Value::TimestampTz("2026-08-05T00:00:00Z".into()), Type::TIMESTAMPTZ, true),
        (Value::TimestampTz("2026-08-05T00:00:00Z".into()), Type::TIMESTAMP,   false),
        (Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()), Type::UUID, true),
        (Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()), Type::TEXT, false),
        (Value::Json("{}".into()),          Type::JSON,        true),
        (Value::Json("{}".into()),          Type::JSONB,       true),
        (Value::Json("{}".into()),          Type::TEXT,        false),
        // U64 has no PG target type in S7 — it stays a known-fate rejection everywhere.
        (Value::U64(1),                     Type::INT8,        false),
        (Value::U64(1),                     Type::NUMERIC,     false),
    ];
    for (v, ty, want) in cases {
        assert_eq!(accepts(v, ty), *want, "accepts({v:?}, {ty:?})");
    }
}

/// The newtypes send TEXT format (param format IS per-param selectable, hazard 17) — so PG parses
/// the canonical text and no base-10000 NUMERIC ENCODER has to be hand-written.
#[test]
fn s7_newtypes_send_text_format() {
    assert!(matches!(PgDecimalText("1.10".into()).encode_format(&Type::NUMERIC), Format::Text));
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p ferro-backend-pg bind` → FAIL.

- [ ] **Step 3: Implement the seven PG newtypes**

Each is a one-field `struct` over the canonical `String`, with:
- `to_sql` writing the canonical text bytes verbatim,
- `encode_format(&self, _ty) -> Format::Text` — the per-param asymmetry (hazard 17) that makes this precision-safe with no encoder,
- `accepts(ty)` matching **only** its own target type(s): `PgDecimalText` → `NUMERIC`; `PgDateText` → `DATE`; `PgTimeText` → `TIME`; `PgTimestampText` → `TIMESTAMP`; `PgTimestampTzText` → `TIMESTAMPTZ`; `PgUuidText` → `UUID`; `PgJsonText` → `JSON` **and** `JSONB`,
- `to_sql_checked!()`.

Then replace Task 1's `unreachable!()` block in `value_to_boxed` with the real boxes, and Task 1's blanket `=> false` in `accepts` with the seven `<PgXText as ToSql>::accepts(ty)` arms. `Value::U64` **stays** `false` in `accepts` with a comment: PG has no unsigned integer type in scope, so a `U64` param is a legitimate known-fate rejection, not an oversight.

- [ ] **Step 4: Implement the MySQL bind arms — typed params, never the `Z` text**

Task 1 left `Date`/`Time`/`Timestamp`/`TimestampTz` as a `MyValue::Bytes` passthrough of the canonical text. That is **correct for `DATE` and naive `TIMESTAMP`** and **wrong for `TIMESTAMPTZ`**: `INSERT … '2026-08-05T11:45:07.250000Z'` fails on MySQL 8 with `ERROR 1292 Incorrect datetime value` under the default `STRICT_TRANS_TABLES`, and MariaDB 11 rejects offsets in datetime literals outright (hazard 41). Parse into typed params instead — no server-side literal parsing at all:

```rust
Value::U64(n)          => MyValue::UInt(*n),
Value::Decimal(s) | Value::Uuid(s) | Value::Json(s) => MyValue::Bytes(s.clone().into_bytes()),
// Typed MYSQL_TYPE_DATETIME / MYSQL_TYPE_DATE params built from the canonical text.
Value::Date(s)         => parse_date(s),        // "YYYY-MM-DD"  -> MyValue::Date(y,m,d,0,0,0,0)
Value::Timestamp(s)    => parse_datetime(s),    // naive         -> MyValue::Date(y,m,d,h,mi,s,us)
// TIMESTAMPTZ is a UTC instant; the session is pinned to '+00:00' (Task 5a Step 0), so the UTC
// components ARE the correct session-local components. The two are COUPLED — without the pin this
// silently shifts the instant by the session offset.
Value::TimestampTz(s)  => parse_rfc3339_utc(s), // strip the trailing 'Z' -> MyValue::Date(..)
// -838:59:58.000001 and 26:00:00 must both survive -> MyValue::Time(neg, days, h, mi, s, us)
Value::Time(s)         => parse_time(s),
```

`value_to_my` stays **infallible** (the module's documented TOTAL invariant, `bind.rs:3-14`). A canonical string the engine itself produced always parses; make the parse helpers fall back to `MyValue::Bytes(text)` on the impossible branch rather than introducing a `Result` cascade the module has no pre-flight to report through — and unit-test each helper on its edge inputs so that branch is provably unreachable.

**There is no MySQL `accepts`/`value_kind` and no mirror test** (hazard 37) — `COM_STMT_PREPARE` exposes no inferred param types, so `validate_arity` remains the only pre-flight. Test instead that **`value_to_my` is total over all 14 variants** and that each date/time helper round-trips its canonical text:

```rust
#[test]
fn value_to_my_is_total_over_every_variant() {
    for v in every_value_variant() {            // one instance of each of the 14
        let my = value_to_my(&v);
        assert!(!matches!((&v, &my), (v, MyValue::NULL) if !matches!(v, Value::Null)),
            "value_to_my must never degrade {v:?} to NULL");
    }
}

#[test]
fn time_and_datetime_helpers_survive_the_edges() {
    assert_eq!(parse_time("-838:59:58.000001"), MyValue::Time(true, 34, 22, 59, 58, 1));
    assert_eq!(parse_time("26:00:00"),          MyValue::Time(false, 1, 2, 0, 0, 0));
    assert_eq!(parse_rfc3339_utc("2026-08-05T11:45:07.250000Z"),
               MyValue::Date(2026, 8, 5, 11, 45, 7, 250_000));
    assert_eq!(parse_date("0000-00-00"),        MyValue::Date(0, 0, 0, 0, 0, 0, 0));
}
```

- [ ] **Step 5: Run everything** — `cargo test --workspace` (offline) then the live MySQL/PG suites. PASS.

- [ ] **Step 6: Gates + commit**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git commit -m "feat(m1-s7): engine bind path — per-tag PG text-format ToSql newtypes with narrow accepts + typed MySQL date/time params"
```

---

## Task 9: Live end-to-end acceptance + DTO path + spec truth

**Files:**
- Create: `engine/crates/ferrod/tests/types_e2e_it.rs` (client → ferrod → DB, all three engines)
- Modify: `php/client/tests/Live/` (a typed round trip through the real client)
- Modify: `ferro-spec-v0.2.md` §9, §9.1, §22.2; `proto/PROTOCOL.md` (verify Task 1's table still matches shipped behavior)

**This is the slice acceptance gate.** It must prove the *whole* path — PHP bind → wire → engine → DB → engine → wire → PHP decode — for every new tag on every engine, and that the deferrals are still loud.

- [ ] **Step 1: Write the full-path round-trip test**

For each new tag, on PG **and** MySQL **and** MariaDB: bind a value from PHP (or from the Rust e2e client), write it, read it back, assert **exact equality with the original**. Precision-critical cases:

- `DECIMAL`: `-12345.6700000000` (trailing zeros survive a full round trip), a 200-digit value on PG, `NaN` on PG.
- `U64` on MySQL: **`0`, `5`, `4294967296` AND `18446744073709551615`** (F11 — the small values exercise the `MyValue::Int` arm; a suite testing only `u64::MAX` is green over the bug that breaks every ordinary row).
- `TIME`: `24:00:00` (PG), negative and > 24 h (MySQL).
- `UUID`: mixed-case input normalizes to lowercase (PG).
- `JSON`: nested + non-ASCII; semantic equality on MySQL 8 and PG `jsonb`; byte-exact on PG `json`; **`tag::TEXT` expected on MariaDB**, asserted explicitly with the hazard-25 reason in the message (F15) — never skipped.
- Fractional seconds: `.250000` preserved; a whole second emits **no** fraction.

**The timezone proof (F12) — prove the INSTANT, not the suffix.** Insert a known instant, read it back through Ferro under **two different external client session zones**, and assert the engine's `TIMESTAMPTZ` canonical text is **byte-identical both times** and is the correct UTC instant; assert `@@session.time_zone = '+00:00'` on the pooled conn (fresh **and** recycled). On PG, set the session `TimeZone` to a non-UTC zone so a "server happens to be UTC" false green is impossible. The v1 assertion ("`DATETIME` unshifted while `TIMESTAMP` comes back as `Z`") passes while the value is shifted by the session offset — it blessed the bug.

**The read → re-bind → read round trip (F13) — one per column type per engine.** PG `timestamp` + `timestamptz`; MySQL/MariaDB `DATETIME` + `TIMESTAMP`:

```
read the column through Ferro  ->  bind the SAME hydrated value straight back into a second row
                               ->  read that row  ->  assert byte-identical canonical text
```

Run it with a **non-UTC PHP `date.timezone`** *and* a **non-UTC external server session zone**. This is the only test that can catch the naive/instant corruption F14 settles (a `NaiveTimestamp` re-bound as `TAG_TIMESTAMPTZ`) and the hazard-41 literal rejection, and v1 had no read→re-bind coverage at all.

- [ ] **Step 2: Prove the deferrals are still loud**

Assert `Unsupported` — naming the column/type — for: PG `interval`, `inet`, `int4[]`, `timetz`, an enum, a domain over numeric; MySQL/MariaDB `YEAR`, `BIT(8)`, `ENUM`, `SET`; MariaDB's native `UUID` type. And that the client raises a named `ProtocolException` for `TAG_ARRAY`/`INTERVAL`/`INET`/`VECTOR`.

- [ ] **Step 3: Cover the DTO/hydration path (hazard 35)**

Add a test for the native-API DTO path with a value-object-typed constructor param (`readonly Decimal $amount`, `readonly \DateTimeImmutable $at`), and one proving a **type mismatch** (e.g. `Ferro\Decimal` into `readonly string $amount`) surfaces inside the `FerroException` contract — `HydrationException` is the natural home — rather than as a bare `\TypeError` escaping from `newInstanceArgs` (`ExecCodec.php:167-168`). Fix `hydrateDto` if it escapes the contract.

- [ ] **Step 4: Run the whole gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                                    # offline: live skips
cargo deny check
(cd engine/crates/ferro-proto/fuzz && cargo check)         # outside every workspace gate
docker compose -f testkit/docker-compose.yml up -d
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test --workspace -- --nocapture
(cd php/client && ./vendor/bin/phpunit && ./vendor/bin/phpstan analyse src --level 9)
```
Paste the per-type actual values for all three engines into the task report.

- [ ] **Step 5: Spec truth**

- **§9** — mark the canonical table's covered rows as implemented in M1-S7; state that the *wire* payload is canonical text (pointing at `PROTOCOL.md` §3) while the table's PHP column describes the native-API hydration. Add the **naive/instant** rule (F14): `TIMESTAMP` hydrates to `Ferro\NaiveTimestamp extends \DateTimeImmutable` — still a `DateTimeImmutable`, so the table stays true — and that a plain `DateTimeImmutable` **binds** as `TIMESTAMPTZ` (an instant) while a `NaiveTimestamp` binds back as `TIMESTAMP`, which is what makes read→write-back byte-stable.
- **§9.1** — record the shipped policy set and defaults; that in M1 the four knobs are **client-side** (the wire is lossless canonical text) and pool-level defaults advertised via `HELLO_ACK` pool metadata are an **S8 carry** (F29); that `naive_datetime_zone: server` is **deferred to S8** (needs that same metadata) while `utc`/`error` ship now, and that `error` is scoped to `TAG_TIMESTAMP` only (F30); that policy **refusals** raise `TypePolicyException` while malformed payloads stay `ProtocolException`. **Add the MySQL UTC pin (F3):** every Ferro MySQL/MariaDB session is pinned to `time_zone = '+00:00'`, why (§9 maps MySQL `timestamp` → `TIMESTAMPTZ`, a UTC instant, which is only definable against a known zone; under pooling an unpinned session zone makes the same column read differently per connection), and the visible consequence — **`NOW()`/`CURDATE()`/`CURTIME()` are UTC on every Ferro MySQL connection**, the same choice Doctrine/Laravel make. Also update the M1-S6 paragraph, which still lists `BIGINT UNSIGNED`, `DECIMAL`, the date/time family and `JSON` as out of scope.
- **§22.2** — one entry for the slice: the text-canonical wire decision and *why* (PurePacker cannot decode maps/ext; `str`/`bin` indistinguishable in PHP; precision/display-scale preservation); the implemented-set-in-the-hash skew decision; the MySQL UTC pin and its `NOW()` consequence; the naive/instant bind rule; the **documented engine asymmetries** — MySQL 8 has no native UUID type, **MariaDB `JSON` classifies as `TEXT`** (it is `LONGTEXT` + a `json_valid()` CHECK with no recoverable metadata) so S8's DBAL `json` mapping must be designed against it, MariaDB's native `UUID` stays `Unsupported`, and MySQL 8's default `sql_mode` blocks live zero-date inserts (unit-tested instead); the still-deferred tags (`ARRAY`/`INTERVAL`/`INET`/`VECTOR`, PG `TIMETZ`/enums/domains, MySQL `YEAR`/`BIT`/`ENUM`/`SET`); the deliberately-thin (in fact **absent**) engine-side policy plumbing with its rationale; and the **four sized S8 carries** from the Global Constraints deferral table — PG catalog scalars (~1 h), PG **domains** via `Kind::Domain(inner)` (~2 h, unblocks all of `information_schema`), the **narrowing bind path** (~4 h, the highest-frequency S8 blocker), and a `Ferro\Bytes` / binary-bind marker so `TAG_BYTES` is reachable from PHP at all (~2 h).

- [ ] **Step 6: Commit**

```bash
git commit -m "test(m1-s7): live end-to-end canonical-type acceptance on PG + MySQL + MariaDB, deferrals still loud, spec truth"
```

---

## Self-Review (controller, after the plan is written)

1. **Spec coverage** — §9's table rows for the 8 tags: Tasks 4a/4b + 5a/5b (engine), 7 (PHP hydration), 8a/8b (bind). §9.1's four policies: Task 6 (options) + Task 7 (every knob exercised, incl. `decimal: 'string'` / `uuid: 'string'` / the `RawStringValuePolicy` S8 hand-off), with `server` explicitly deferred. Charter rule 2 (registry + vectors + both codecs together): Tasks 1–3. The §22.2 deferral record incl. the four **sized** S8 carries: Task 9 Step 5.
2. **Placeholder scan** — every code step carries real code or a named file:line target; no "add error handling" steps; every prescribed test compiles against the signatures verified in the hazard list (37–40).
3. **Type consistency** — `Value::{U64,Decimal,Date,Time,Timestamp,TimestampTz,Uuid,Json}` is used identically in Tasks 1, 4b, 5b, 8b; `TypePolicyOptions` field names (`decimal`, `naiveDatetimeZone`, `u64Overflow`, `uuid`) match between Tasks 6, 7, 8a; `MyKind` variants match between the Task 5b table and its steps; `NaiveTimestamp` is introduced once (Task 7) and consumed once (Task 8a).
4. **Ordering** — **1** (codec) → **2** (registry hash; before vectors so the completeness guard derives from `implemented`) → **3** (vectors + PHP codec) → **4a** (pgtext) → **4b** (PG gates + live) → **5a** (MySQL UTC pin + mytext) → **5b** (MySQL gates + live) → **6** (policy options) → **7** (PHP read) → **8a** (PHP bind) → **8b** (engine bind) → **9** (acceptance + spec truth). The 4x pair and the 5x pair are independent of each other and may run in parallel **after** Task 3; 5a Step 0 blocks all MySQL date/time work; 8b depends on 8a only for the value objects' canonical text, not for code.
5. **Split integrity** — each half of 4/5/8 is independently green (`cargo test --workspace` + PHP suites) and independently committable; no half leaves the tree with a HEAD promising a tag its producer cannot fill (4a touches one new file; 4b moves both gates in a single edit).
