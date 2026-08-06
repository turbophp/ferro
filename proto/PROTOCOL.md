# Ferro wire protocol — field-order contract

This is the human-readable arbiter for the Ferro wire format. `/proto/*.toml` is the source of
truth for **numbers** (method ids, flags, error codes, type tags — see `registry.lock.json` and
the generated `consts` module in both languages); this document is the source of truth for
**byte layout and field order**, which the golden vectors in `/proto/vectors/` (Task 6) enforce
byte-for-byte across the Rust codec, the pure-PHP codec, and the `ext-msgpack` codec. Where this
document and a golden vector disagree, the vector wins and this document is wrong — fix the doc.

Status of this document: written in S1 Task 2 alongside the registry. The header, canonical
MessagePack profile, TypedValue encoding, and the six core-service messages (`HELLO`,
`HELLO_ACK`, `PING`, `PONG`, `GOODBYE`, `WINDOW_UPDATE`) plus `ERROR`/`Outcome` are pinned now,
ahead of their Rust implementation (Tasks 4–5), so the codec is written *against* a fixed
contract rather than the contract being reverse-engineered from the code. Task 5 amends this file
only if implementation forces a deviation (per CLAUDE.md "when uncertain"); it does not
re-litigate the layout below.

## 1. Frame header (16 bytes, little-endian)

Every frame on the wire begins with a fixed 16-byte header, followed immediately by a
MessagePack-encoded payload of exactly `payload_len` bytes. There is no padding and no
alignment — the payload starts at byte 16.

| offset | field | type | notes |
|---|---|---|---|
| 0 | `magic` | `u8` | always `0xF7` (`consts::MAGIC`) |
| 1 | `version` | `u8` | protocol major version, currently `1` (`consts::PROTOCOL_VERSION`) |
| 2 | `flags` | `u16` | bitfield: `STREAM 0x01`, `END 0x02`, `CANCEL 0x04`, `OOB_FD 0x08`, `COMPRESSED 0x10` (reserved, unimplemented before post-M3) |
| 4 | `service` | `u16` | `CORE 1`, `SQL 2`, `TX 3`, `STREAM 4`, `ADMIN 5` |
| 6 | `method` | `u16` | per-service method id, registry `/proto/methods.toml` |
| 8 | `request_id` | `u32` | client-assigned multiplexing key |
| 12 | `payload_len` | `u32` | MessagePack payload length in bytes |

Total: 16 bytes. Field order above is the encode/decode order — do not reorder. The PHP codec
packs/unpacks this with `pack('CCvvvVV', magic, version, flags, service, method, request_id,
payload_len)` / the matching `unpack` format (all fields little-endian; `C`=u8, `v`=u16 LE,
`V`=u32 LE, matching PHP's `pack()` on little-endian platforms).

**Hard ceiling:** `MAX_FRAME_PAYLOAD = 16777216` (16 MiB, `consts::MAX_FRAME_PAYLOAD`). A frame
declaring a larger `payload_len` MUST be rejected with a `Protocol` error **before any allocation
sized by the declared length** — the header is fully readable with zero payload-sized allocation,
so a decoder can reject an oversize claim cheaply. This is conceptually distinct from the
`DEFAULT_CREDIT_BYTES` flow-control window (§5.2 of the spec, also `16777216` / 16 MiB as of
M1-S5): `MAX_FRAME_PAYLOAD` is a hard per-frame reject ceiling enforced by the codec, while
`DEFAULT_CREDIT_BYTES` is a replenishable per-request budget that bounds buffered stream output,
not a single frame's size. The two are numerically equal by deliberate design (a single valid
frame at the ceiling must always fit the initial credit window, or a maximally-sized row could
never be sent — see SPEC §22.2's M1-S5 deviation note) but remain distinct knobs: one is a codec
invariant, the other an operator-tunable default.

## 2. Canonical MessagePack profile

All payloads use plain MessagePack — **no ext types**. The profile below is what every
implementation (Rust `rmp`, pure-PHP packer, `ext-msgpack`) must produce byte-for-byte; it is
locked by the golden vectors, not by this prose, but this prose explains what the vectors encode:

- **Signed/unsigned integers** follow `rmp::encode::write_sint` narrowing: **non-negative**
  values use the smallest of positive-fixint / `uint8 (0xcc)` / `uint16 (0xcd)` / `uint32 (0xce)`
  / `uint64 (0xcf)`; **negative** values use the smallest of negative-fixint / `int8 (0xd0)` /
  `int16 (0xd1)` / `int32 (0xd2)` / `int64 (0xd3)`. Unsigned-typed fields (e.g. `boot_epoch: u64`)
  use the same non-negative ladder — a small `boot_epoch` is still encoded as a positive-fixint,
  not padded to a fixed width. (Confirmed against `rmp` 0.8.15: `write_sint(200)` encodes as
  `cc c8`, an unsigned uint8 — not `d1 00 c8` — because positive values ≥ 128 always prefer the
  unsigned markers.)
- **Floats** are always `float64 (0xcb)`, big-endian — never the narrower `float32 (0xca)`.
- **Strings** use the `str` family: fixstr / `str8` / `str16` / `str32`, chosen by length.
- **Byte payloads** (`BYTES` TypedValue, opaque blobs) use the `bin` family: `bin8 (0xc4)` /
  `bin16 (0xc5)` / `bin32 (0xc6)`.
- **Absence** (`None` / PHP `null`) is `nil (0xc0)`.
- **Arrays** use fixarray / `array16` / `array32` by length. Messages are positional arrays of
  their fields in declaration order (§4) — there is no map-keyed message encoding and no
  message-schema IDL; this document plus the golden vectors are the schema.
- **uint64 overflow:** a `uint64` value greater than `PHP_INT_MAX` (2^63−1) cannot be represented
  losslessly by `ext-msgpack`, which decodes it to a lossy float. The pure-PHP decoder is
  authoritative for this case and MUST decode such a value to a decimal string instead. Rust has no
  such limit (`u64` is native). Only **full-range** `u64` fields need this treatment. The wire has
  exactly one today: `HelloAck.boot_epoch` (a random per-start id, §19.1). All OTHER `u64` fields
  are contractually **bounded < 2^63** and are decoded as native PHP ints: `ExecOk.affected`, the
  four `Stats` fields (`queue_us`, `exec_us`, `rows`, `bytes`), and the TX `tx_id`
  (`ExecRequest.tx_id`, `BeginResponse.tx_id`, `TxControl.tx_id`, `SavepointRequest.tx_id` — a
  monotonic never-reused counter, §7) — rows affected, microsecond timings, a frame-bounded byte
  count, and a per-daemon transaction counter cannot approach 2^63. (The Rust encoder carries a
  debug-mode `debug_assert!` tripwire on the `Stats`/`affected` fields; a future field that outgrows
  the bound must either adopt the `boot_epoch` decimal-string treatment or be documented here.)

## 3. TypedValue (`Value`)

`TypedValue` is the canonical row/param value encoding (SPEC §9): a 2-element MessagePack array
`[tag: u8, payload]`. `NULL` is tag `0` with a `nil` payload — there is no separate "is null" bit.

As of **M1-S7**, encode/decode is implemented for **fourteen** tags (M0's six, decision T-1, plus
the eight canonical tags below). The remaining tags in `/proto/types.toml`'s `[tags]` table are
registry constants only — attempting to use one is a `NonRetryable{Unsupported}` error, not a codec
crash.

### 3.1 M0 scalars

| tag name | tag value | payload MessagePack family |
|---|---|---|
| `NULL` | 0 | `nil` |
| `BOOL` | 1 | `bool` (`0xc2`/`0xc3`) |
| `I64` | 2 | signed int family (§2 narrowing) |
| `F64` | 4 | `float64` |
| `TEXT` | 6 | `str` family |
| `BYTES` | 7 | `bin` family |

### 3.2 M1-S7 canonical tags — **text-canonical payloads**

Every tag added in M1-S7 rides the msgpack **`str`** family carrying **canonical text**, except
`U64` which rides the **uint** family. No S7 tag uses `bin`.

*Why text, and why never `bin`:* PHP's pure decoder (`PurePacker`) cannot decode msgpack **maps or
ext types at all**, and §2 bans ext types outright; separately, `str` and `bin` are
**indistinguishable in PHP after unpack** (both come back as a PHP string, so the TypedValue tag is
the only discriminator), which would force a `list<int>` special case *and* make the payload
un-round-trippable through the golden-vector JSON `message` field. Canonical text sidesteps all of
it and is directly comparable across the two codecs.

| tag name | tag value | msgpack family | canonical payload | notes |
|---|---|---|---|---|
| `U64` | 3 | uint family | unsigned 64-bit integer | The ONLY non-`str` addition. |
| `DECIMAL` | 5 | `str` | `"-12345.6700"` — full precision, **display scale preserved** | `"NaN"`, `"Infinity"`, `"-Infinity"` are legal payloads (PG `NUMERIC` allows them). `1.10` and `1.1` are **distinct** payloads and must never be normalized to each other. |
| `DATE` | 8 | `str` | `"YYYY-MM-DD"` | `"infinity"` / `"-infinity"` for the PG sentinels; `"0000-00-00"` for a MySQL zero date. |
| `TIME` | 9 | `str` | `"HH:MM:SS"` or `"HH:MM:SS.ffffff"` | Hours may exceed 23 (PG `time '24:00:00'`); a MySQL `TIME` spans ±838 h and may be negative → a leading `-`. |
| `TIMESTAMP` | 10 | `str` | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` | **Naive** — no zone suffix, ever. |
| `TIMESTAMPTZ` | 11 | `str` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` | RFC3339, **always normalized to UTC**, always the literal `Z`. |
| `UUID` | 12 | `str` | 36-char canonical **lowercase** hyphenated | Never raw bytes. |
| `JSON` | 13 | `str` | the raw UTF-8 JSON document text | Not re-serialized and not validated by the engine; the client decodes lazily. |

**Fractional seconds** (`TIME`, `TIMESTAMP`, `TIMESTAMPTZ`): emit **no** `.ffffff` group when the
sub-second part is zero; otherwise emit **exactly six** digits. Never emit a trailing-zero-trimmed
variant — the payload must be byte-stable for the golden vectors.

**`U64` uses the canonical narrowing ladder**, not a fixed `0xcf`. `Value::U64(0)` is a positive
fixint (`0x92 0x03 0x00`); the marker widens through `0xcc`/`0xcd`/`0xce` and reaches `0xcf` only
above `0xffffffff`. This is byte-identical to PHP `PurePacker::packUint`, so a decoder must accept
**any** uint marker for tag 3 — a marker-strict `uint64`-only reader is a defect. Consequence for
the PHP side: a `U64` at or below `0xffffffff` decodes to a PHP `int`, while anything above it
decodes to a **decimal string** (the §2 `uint64` overflow rule), so the value's PHP type follows its
**magnitude**, not its tag.

The `str`-family payloads are **canonical text produced by the backend**. The codecs move that text
verbatim and validate nothing beyond UTF-8 — the rendering decision lives where the source format is
known (the backend), never in the codec.

> **Rollout status (M1-S7 in progress).** This table is the *contract*, pinned first so both codecs
> are written against one text. The Rust codec (`ferro_proto::value::Value`) implements all fourteen
> tags today; the PHP codec and the per-tag golden vectors land later in the same slice, and until
> they do the PHP side still rejects tags 3/5/8–13 as `Unsupported`. Remove this note when the
> vectors land.

### 3.3 Still unimplemented

`ARRAY` (14), `INTERVAL` (15), `INET` (16) and `VECTOR` (17) remain **registry constants only** and
stay a loud `NonRetryable{Unsupported}` (SPEC §22.2). Decoding one is an error by construction, and
that is asserted (`ferro-proto/tests/value.rs::deferred_tags_are_still_rejected`).

The full tag table (`NULL` through `VECTOR`) lives in `/proto/types.toml` and `consts::tag::*`; the
numeric assignments are not duplicated beyond the tables above — see the registry lock file.

**Tag-byte encoding.** The tag is always a **bare positive fixint** (written with `write_pfix`, read
with `read_pfix`), which is exact for the whole registry (0..=17). A multi-byte encoding of the same
tag number is **not** accepted — this keeps the `[tag, payload]` pair canonical byte-for-byte.

## 4. Core-service messages

Every message below is a MessagePack **array of its fields in the declaration order listed** —
not a map. Optional fields (`Option<T>` in Rust) are present in the array as `nil` when absent,
not omitted — the array arity never changes for a given message type. These are the six
`[methods.core]` messages plus the two cross-cutting envelopes (`ERROR`, `Outcome`) that every
service's terminal frame carries.

### `HELLO` (service `CORE`, method `HELLO` = 1) — client → server

| # | field | type | notes |
|---|---|---|---|
| 1 | `client_version` | `u32` | |
| 2 | `type_registry_hash` | `str` | |
| 3 | `manifest_hash` | `str \| nil` | present only in manifest-only mode (M3+) |
| 4 | `pid` | `u32` | client OS pid, diagnostic |
| 5 | `features` | `u32` | client feature bitfield: `MEMFD_RX 0x01`, `FIBERS 0x02` |

### `HELLO_ACK` (service `CORE`, method `HELLO_ACK` = 2) — server → client

| # | field | type | notes |
|---|---|---|---|
| 1 | `engine_version` | `u32` | |
| 2 | `boot_epoch` | `u64` | unique per daemon start (§19.1); see §2 uint64-overflow note |
| 3 | `features` | `u32` | engine feature bitfield: `MEMFD 0x01`, `LISTEN_STREAMS 0x02`, `MANIFEST 0x04` |
| 4 | `pools` | `array<str>` | pool names available on this engine |
| 5 | `type_registry_hash` | `str` | echoed back; mismatch vs. the client's hash is a hard error |

### `PING` (service `CORE`, method `PING` = 3) — either direction

| # | field | type | notes |
|---|---|---|---|
| 1 | `token` | `u64` | echoed unchanged in the matching `PONG` |

### `PONG` (service `CORE`, method `PONG` = 4) — either direction

| # | field | type | notes |
|---|---|---|---|
| 1 | `token` | `u64` | copied from the `PING` it answers |

### `GOODBYE` (service `CORE`, method `GOODBYE` = 5) — client → server

Empty message — a zero-field array (`[]`). Announces graceful client close so the engine can
distinguish drain from death (§5.2).

### `WINDOW_UPDATE` (service `CORE`, method `WINDOW_UPDATE` = 6) — client → server

| # | field | type | notes |
|---|---|---|---|
| 1 | `frames` | `u32` | additional frame credit granted for the named `request_id` |
| 2 | `bytes` | `u32` | additional byte credit granted for the named `request_id` |

Note: `WINDOW_UPDATE` is itself carried in a frame whose header `request_id` names the stream
being credited (§5.2) — the message body does not repeat `request_id`.

## 5. `ERROR` payload

Carried as the `body` of an `Outcome::Error` (below), never as its own top-level frame type. One
normalized shape for every backend/engine error (SPEC §9.2), positional array:

| # | field | type | notes |
|---|---|---|---|
| 1 | `code` | `u16` | registry `errc::*`; grouped by branch nibble (`Retryable 0x1xxx`, `Indeterminate 0x2xxx`, `NonRetryable 0x3xxx`) but an unknown code is still classified correctly because... |
| 2 | `branch` | `u8` | ...`branch` (`Retryable 1`, `Indeterminate 2`, `NonRetryable 3`) is carried explicitly on the wire (decision W-3), not inferred from `code`'s range |
| 3 | `sqlstate` | `str \| nil` | raw backend SQLSTATE, when the backend provides one |
| 4 | `errno` | `i32 \| nil` | raw backend errno, when the backend provides one |
| 5 | `message` | `str` | human-readable, not for programmatic matching |
| 6 | `detail` | `str \| nil` | backend detail/hint text, if any |
| 7 | `retry_after_ms` | `u32 \| nil` | advisory backoff hint (e.g. for `PoolTimeout`) |

## 6. Terminal outcome envelope

Every in-flight request terminates in exactly one frame carrying flag `END`, whose payload is
this envelope (decision W-4) — a 2-element positional array `[status: u8, body]`:

| `status` | meaning | `body` |
|---|---|---|
| 0 | `Ok` | method-specific success payload (opaque to this envelope — e.g. `EXEC`'s result set) |
| 1 | `Error` | the `ERROR` array, §5 |
| 2 | `Cancelled` | `nil` |

This is the one place every service's happy path, error path, and cancellation path converge —
session-layer code and both client languages key their state machines off `status`, never off
inferring completion from the absence of further frames.

## 7. Vector index

Golden vectors live in `/proto/vectors/*.json` (positive cases: logical value + expected
`frame_hex`) and `/proto/vectors/negative/*.bin` (raw malformed frames the decoder must reject
with zero unbounded allocation; also used as `cargo-fuzz` seeds). They do not exist yet as of
this commit — they are generated by `gen-vectors` in Task 6 and are the actual byte-level
arbiter for everything in this document. This section will be replaced with a table of
`{name → header summary → message}` once Task 6 lands; until then, treat §1–§6 above as the
provisional contract that Tasks 3–5 implement against and Task 6 locks.

## 8. SQL service messages (`EXEC`)

The SQL service (`SERVICE_SQL = 2`) has one request-bearing method in M0, `EXEC`
(`METHOD_SQL_EXEC = 1`, registry `methods.sql`). Unlike the core messages, `ExecRequest` and the
terminal `ExecOk` body carry `TypedValue`s, so they do **not** use the rmp-serde `msg!` path (which
requires `Serialize/Deserialize/Eq` — `Value` has none, and `F64` forbids `Eq`). They use a
**bespoke positional codec** that splices `Value` (§3) bytes per element, exactly as the `Outcome`
envelope (§6) splices its opaque body. The Rust codec (`ferro-proto` `messages::sql`) and the PHP
codec (`ExecRequest`/`ExecOk`/`SqlValueCodec`) mirror these BYTES — not their internal decode
structure (PHP unpacks the whole body and walks the nested arrays). The layout below is locked by
the `sql_exec_*` golden vectors.

Three rules apply throughout §8:

- **Strict arity.** Every positional array is fixed-shape, and a conforming decoder MUST read the
  declared array length and REJECT a mismatch (it MUST NOT read a fixed number of fields and ignore
  the prefix). The required lengths: `ExecRequest` = 8, `ExecOk` = 5, `ColMeta` = 2 (`[name, tag]`),
  and each `Value` = 2 (`[tag, payload]`, §3). Both reference codecs enforce this; a lax third
  implementation that trusted a shorter/longer prefix would mis-frame every following field.
- **`Option<Value>` peek rule.** An optional `Value` slot (`ExecOk.last_insert_id`) is a bare
  `nil` (`0xc0`) when absent, else the value's own `[tag, payload]` encoding. Decoders peek the
  first byte: `0xc0` ⇒ absent; anything else ⇒ decode a `Value`. This is unambiguous because a
  present `Value::Null` encodes as the fixarray `[NULL, nil]` (first byte `0x92`), never a bare
  `0xc0`. (Optional scalars — `sql`, `query_id`, `timeout_ms`, `tx_id` — follow the same nil-vs-value rule.)
- **`array16` threshold.** Array length prefixes narrow by count: fixarray (`0x9_`) for ≤15
  elements, `array16` (`0xdc` + `u16` BE) for 16..=65535, `array32` (`0xdd` + `u32` BE) beyond.
  A result with ≥16 columns or ≥16 cells in a row therefore emits `0xdc`; both codecs must agree at
  the boundary (locked by `sql_exec_response_wide`). Every decoded array length is bounds-checked
  against the bytes remaining before any allocation (a MessagePack element is ≥1 byte), closing the
  lying-`u32::MAX`-length hole.

### 8.1 `ExecRequest` (service `SQL`, method `EXEC` = 1) — client → server

A positional fixarray of 8 fields in declaration order (payload of a non-`END` `EXEC` frame):

| # | field | type | notes |
|---|---|---|---|
| 1 | `pool` | `str` | target pool name |
| 2 | `sql` | `str \| nil` | literal SQL; `nil` iff a `query_id` is used (manifest mode, M3) |
| 3 | `query_id` | `str \| nil` | manifest query id; `nil` in M0 (rejected `Unsupported` if set) |
| 4 | `params` | `array<Value>` | positional bind params, each a `[tag, payload]` `Value` (§3) |
| 5 | `timeout_ms` | `u32 \| nil` | per-statement deadline hint |
| 6 | `readonly` | `bool` | client-declared; drives the write-loss → `Indeterminate` split (no engine inference) |
| 7 | `fetch` | `u8` | `0` = rows, `1` = none (affected only), `2` = stream (§10's windowed `HEAD`/`DATA`/`END` producer, M1-S5) — a valid, wire-accepted value as of M1-S5 Task 1 (the codec never restricted it); the ferrod EXEC handler's `Unsupported` rejection of `2` is unchanged by that task and lifts in a later S5 task |
| 8 | `tx_id` | `u64 \| nil` | S6: `nil` = autocommit; a value routes this EXEC to the actor pinning that tx's conn (§9). Bounded < 2^63 (native int, §2); the opt-u64 nil/value peek rule (below) applies |

### 8.2 `ExecOk` — terminal `Outcome::Ok` body — server → client

`EXEC`'s success result is buffered into the single `END` frame as the `Outcome::Ok` body (§6):
`[OUTCOME_OK, <ExecOk>]`. `ExecOk` itself is a positional fixarray of 5 fields:

| # | field | type | notes |
|---|---|---|---|
| 1 | `cols` | `array<ColMeta>` | one `ColMeta` per result column (empty for `fetch:none`) |
| 2 | `rows` | `array<array<Value>>` | outer = rows, inner = that row's cells (each a `Value`, §3) |
| 3 | `affected` | `u64` | rows affected (`fetch:none`) or `0` |
| 4 | `last_insert_id` | `Value \| nil` | Option<Value> peek rule above |
| 5 | `stats` | `Stats` | `[queue_us, exec_us, rows, bytes]`, all `u64` |

`ColMeta` is the 2-element array `[name: str, tag: u8]` (`tag` is a `TypedValue` tag, §3). It
encodes by concatenation into `cols`; inside `ExecOk` it is decoded positionally off the shared
cursor (a whole-slice decode would spuriously reject the trailing bytes that always follow it).
`Stats` is the final field, so decoding it whole doubles as the ExecOk-body trailing-byte check.

### 8.3 SQL vector index

`sql_exec_request_select1` (autocommit `SELECT 1`, no params, readonly), `sql_exec_request_params`
(the full M0 scalar set including the divergent-range ints `I64(200)` = `cc c8` / `I64(-200)` =
`d1 ff 38`), `sql_exec_response_select1` (a one-col/one-row terminal body),
`sql_exec_response_none` (`fetch:none` `affected` with empty rows), `sql_exec_response_lastid`
(`Some(last_insert_id)` = `Some(I64)` — locks the Option<Value> peek path), `sql_exec_response_wide`
(≥16 cols and a ≥16-cell row — locks the `array16` marker `0xdc`), `sql_exec_response_nullid`
(`Some(Value::Null)` — locks the `[NULL, nil]` = `92 00 c0` vs bare-`c0` `None` disambiguation),
`sql_exec_response_typedvalue` (a row carrying the full M0 scalar set — the S1-deferral shared
cross-language arbiter, including a `Bytes` whose first byte is the `0xc0` nil marker), and
`sql_exec_request_intx` (a tx-scoped EXEC with `tx_id = Some(7)` — locks the field-8 opt-u64
`Some` path; the regenerated `select1`/`params` request vectors lock the `None` path as a trailing
bare `nil`).

## 9. TX service messages (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`/`ROLLBACK_TO`)

The TX service (`SERVICE_TX = 3`, S6) runs a transaction on a pooled connection **pinned to a
`tx_id`, not to the client socket** (SPEC §4/§7). `BEGIN` mints a `tx_id`; tx-scoped `EXEC` (§8.1
with `tx_id = Some(..)`) and the savepoint methods run on that pinned conn; `COMMIT`/`ROLLBACK` end
it and release the conn. The methods are registry `methods.tx` (`/proto/registry.lock.json`): `BEGIN
= 1`, `COMMIT = 2`, `ROLLBACK = 3`, `SAVEPOINT = 4`, `RELEASE = 5`, `ROLLBACK_TO = 6`.

Unlike `EXEC`, the TX messages are **`Value`-free**, so they use the same plain rmp-serde positional
layout (fixarray of fields in declaration order, `Option<T>` present as bare `nil` when absent) as
the core messages (§4) — not the bespoke `Value`-splicing codec. `tx_id` is a monotonic never-reused
counter, contractually **bounded < 2^63** (§2), so it is a native PHP int, NOT the `boot_epoch`
decimal-string treatment.

### 9.1 `Isolation` (a message-field `u8`, not a registry constant)

`BeginRequest.isolation` is an optional `u8`. It is a message-field VALUE, not a `/proto` registry
constant (it is neither a method id, flag, error code, nor type tag — charter rule 2's
source-of-truth scope), so the mapping is fixed HERE and in `ferro-proto` `messages::tx::Isolation`,
never in `methods.toml`:

| value | level |
|---|---|
| 0 | `READ COMMITTED` |
| 1 | `REPEATABLE READ` |
| 2 | `SERIALIZABLE` |

There is no fourth value: PostgreSQL's `READ UNCOMMITTED` is an alias for `READ COMMITTED` and maps
to `0`. `nil` means "engine/pool default".

### 9.2 `BeginRequest` (service `TX`, method `BEGIN` = 1) — client → server

A positional fixarray of 3 fields:

| # | field | type | notes |
|---|---|---|---|
| 1 | `pool` | `str` | target pool name |
| 2 | `isolation` | `u8 \| nil` | §9.1; `nil` = default |
| 3 | `readonly` | `bool` | client-declared read-only tx (a write → PG `25006` → `NonRetryable`) |

### 9.3 `BeginResponse` — terminal `Outcome::Ok` body — server → client

`BEGIN`'s success result is the single `END` frame's `Outcome::Ok` body (§6): `[OUTCOME_OK,
<BeginResponse>]`. `BeginResponse` is a positional fixarray of 1 field, and composes into the
envelope exactly as `ExecOk` does because its encoding is one complete MessagePack value:

| # | field | type | notes |
|---|---|---|---|
| 1 | `tx_id` | `u64` | the minted transaction id; bounded < 2^63 (§2) |

### 9.4 `TxControl` (service `TX`, methods `COMMIT` = 2 / `ROLLBACK` = 3) — client → server

The method id in the frame header selects commit vs rollback; the body is a positional fixarray of
1 field:

| # | field | type | notes |
|---|---|---|---|
| 1 | `tx_id` | `u64` | the transaction to end |

### 9.5 `SavepointRequest` (service `TX`, methods `SAVEPOINT` = 4 / `RELEASE` = 5 / `ROLLBACK_TO` = 6) — client → server

The method id selects the savepoint operation; the body is a positional fixarray of 2 fields:

| # | field | type | notes |
|---|---|---|---|
| 1 | `tx_id` | `u64` | the owning transaction |
| 2 | `name` | `str \| nil` | savepoint name; `nil` ⇒ the engine names it (`sp_<n>` stack) |

### 9.6 TX vector index

`tx_begin_request` (`SERIALIZABLE`, not readonly), `tx_begin_response` (the terminal
`Outcome::Ok(BeginResponse)` envelope, locking the one-field-msg-composes-with-`Outcome::Ok` path),
`tx_commit` (a bare `TxControl`), and `tx_savepoint` (a named `SavepointRequest`).

## 10. STREAM service messages (`HEAD`/`DATA`)

The STREAM service (`SERVICE_STREAM = 4`, M1-S5) is the windowed DATA-channel producer for a
`fetch:stream` `EXEC` (§8.1 field 7 = `2`): instead of buffering the whole result into the single
terminal frame (the `fetch:rows` path, §8.2), the engine emits one `HEAD` frame (the column
metadata) followed by N `DATA` frames (row batches), then the SAME terminal `END` frame every EXEC
uses — an `Outcome::Ok(ExecOk)` body whose `cols`/`rows` are empty (the rows already went out as
`DATA`; only `affected`/`stats` are populated, exactly like the `fetch:none` shape, §8.2). `HEAD`
and `DATA` share `EXEC`'s `request_id` (§5.2) and are **not** terminal frames, so neither carries
the `END` flag and neither is wrapped in the `Outcome` envelope (§6, reserved for the one true
terminal) — each is a plain positional message payload, exactly like an `ExecRequest` frame. `DATA`
frames (and only `DATA` frames) carry the `STREAM` flag (`flags::STREAM = 0x01`) to mark them as
DATA-channel frames under the per-request credit window (§5.2's `WINDOW_UPDATE`/§7.2). The methods
are registry `methods.stream` (`/proto/registry.lock.json`): `HEAD = 1`, `DATA = 2`.

Like `EXEC`, both messages carry `TypedValue`s (`DATA.rows`) or the `Value`-free `ColMeta`
(`HEAD.cols`), so they live in `ferro-proto`'s `messages::sql` alongside `ExecOk` and use the SAME
**bespoke positional codec** (`ferro-proto`'s hand-rolled `encode`/`decode`, not `msg!`/rmp-serde) —
never a second source of truth for how a column or a row cell is framed.

**Task-1 scope note:** this section defines the `/proto`-layer wire shapes only. The producer that
actually emits `HEAD`/`DATA` frames from a real query, and the ferrod EXEC handler's lift of its
current `fetch:stream` → `Unsupported` rejection, are later M1-S5 tasks.

### 10.1 `HEAD` (service `STREAM`, method `HEAD` = 1) — server → client

A positional fixarray of 1 field, sent once, before any `DATA` frame for the same `request_id`:

| # | field | type | notes |
|---|---|---|---|
| 1 | `cols` | `array<ColMeta>` | one `ColMeta` per result column — the exact shape `ExecOk.cols` uses (§8.2), so the client hydrator is shared between the buffered and streamed paths |

### 10.2 `DATA` (service `STREAM`, method `DATA` = 2) — server → client

A positional fixarray of 1 field, carried in a frame with the `STREAM` flag set:

| # | field | type | notes |
|---|---|---|---|
| 1 | `rows` | `array<array<Value>>` | outer = rows in this batch, inner = that row's cells (each a `Value`, §3) — the SAME `[tag, payload]` scalar codec `ExecOk.rows` uses |

### 10.3 STREAM vector index

`stream_head_cols` (a `HEAD` frame with 3 cols, incl. a `TEXT` and a `BYTES`-tagged col — locks the
`ColMeta` shape shared with `ExecOk.cols`) and `stream_data_rows` (a `DATA` frame — `STREAM` flag
set — with 3 rows incl. an all-`Null` row, the divergent-range negative int `I64(-200)` = `d1 ff
38`, and a `BYTES` cell whose first byte is the `0xc0` nil marker — the same cross-language arbiter
shape as `sql_exec_response_typedvalue`, §8.3).
