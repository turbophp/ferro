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
| 1 | `version` | `u8` | protocol major version, currently `4` (`consts::PROTOCOL_VERSION`) |
| 2 | `flags` | `u16` | bitfield: `STREAM 0x01`, `END 0x02`, `CANCEL 0x04`, `OOB_FD 0x08` (engine → client only, §1.1), `COMPRESSED 0x10` (reserved, unimplemented before post-M3) |
| 4 | `service` | `u16` | `CORE 1`, `SQL 2`, `TX 3`, `STREAM 4`, `ADMIN 5`, `HTTP 6` (§12, M6-F2), `QUEUE 7` (§14, M7-G1a) |
| 6 | `method` | `u16` | per-service method id, registry `/proto/methods.toml` |
| 8 | `request_id` | `u32` | client-assigned multiplexing key |
| 12 | `payload_len` | `u32` | MessagePack payload length in bytes |

Total: 16 bytes. Field order above is the encode/decode order — do not reorder. The PHP codec
packs/unpacks this with `pack('CCvvvVV', magic, version, flags, service, method, request_id,
payload_len)` / the matching `unpack` format (all fields little-endian; `C`=u8, `v`=u16 LE,
`V`=u32 LE, matching PHP's `pack()` on little-endian platforms).

**`protocol_version` went `1` → `2` in M1-S8a**, and the reason is the only reason it ever moves: a
message SHAPE changed in a way nothing else would catch. `HELLO_ACK`'s `pools` became structured
per-pool metadata (§4), and `TYPE_REGISTRY_HASH` — the other cross-version tripwire — is FNV-1a over
`registry.lock.json`, which carries protocol version / magic / flags / services / methods / error
codes / type tags but **no message layouts**. Without the bump a skewed pair would handshake
"successfully" (the engine compares only the hash) and then fail deep inside `HelloAck::decode`.
Bumping the version *also* changes the lock file, so the hash moves too — two independent tripwires,
one of which fires first.

It has moved twice more for the same reason. **`2` → `3` at M2-C2g**, when `HELLO_ACK`'s per-pool
metadata gained `literals_are_standard` (§4). **`3` → `4` at M2-C4c-1**, when `ExecRequest` gained a
ninth field, the caller's W3C `traceparent` (§8.1). Strict arity (§8) means a version-3 client's
8-field `EXEC` would otherwise be refused deep in the SQL service as `Protocol`, one statement at a
time; the bump moves that refusal to the first frame.

**What the skew failure LOOKS like, honestly.** Byte 1 is checked in `Header::decode` on **both**
sides — Rust `header.rs` and PHP `Header.php` — **before any payload is decoded**, so a mismatch is
caught deterministically at the first byte pair of the first frame, in both directions (an old
client's `HELLO` reaching a new engine; an old engine's frame reaching a new client). But it is a
**codec-class** failure, not a typed handshake rejection, and the two look different in three ways
worth knowing before you debug one. Measured end to end against a live `ferrod` at
`protocol_version = 2`, and — since the M1-S8a review round — pinned there by a test, so this table
cannot go stale under a refactor: `ferrod`'s
`handshake.rs::a_previous_version_hello_is_answered_with_the_documented_protocol_terminal` asserts
row 1 (rid, `END`, code, branch, message, one-frame-then-EOF) and
`ferrod`'s `handshake.rs::wrong_type_registry_hash_is_fatal_unsupported` asserts row 2's code.

| what arrived | engine's terminal on `request_id=0` | message |
|---|---|---|
| a `HELLO` frame with version byte `1` | `errc::PROTOCOL` (`0x3009`) | `unsupported protocol version: expected 4, got 1` |
| a well-formed current-version `HELLO` with a stale `type_registry_hash` | `errc::UNSUPPORTED` (`0x300A`) | `type_registry_hash mismatch: client sent …, engine is …` |

So: (1) the code is `PROTOCOL`, not `UNSUPPORTED` — anything keying on `errc::UNSUPPORTED` to mean
"we disagree" will not fire on a version skew; (2) the engine's own log/terminal message *does* name
both versions, so the engine-side diagnosis is good; but (3) **the old client cannot read that
terminal**, because the reply frame itself carries version `2` and dies in the client's
`Header::decode` as `CodecException('bad version 2')`. The client-side symptom is therefore a bare
codec error with no server text at all. That asymmetry is a deliberate trade, not an oversight —
carrying a version inside the `HELLO` *payload* and rejecting it as a typed error is a strictly
larger change and was not in scope for M1-S8a. Do not file the missing typed error as a bug against
this section.

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

## 1.1 Out-of-band payloads (`OOB_FD`, M3-D3)

Engine → client only, and only to a client whose `HELLO` set `MEMFD_RX` (§4). A frame carrying the
`OOB_FD` flag has had its payload moved into a **sealed** memfd (`F_SEAL_SHRINK | F_SEAL_GROW |
F_SEAL_WRITE | F_SEAL_SEAL`, positioned at offset 0), passed beside the frame with `SCM_RIGHTS` on the
Unix socket. The frame's `payload_len`/payload are then an `OobRef`, a positional fixarray of 3:

| # | field | type | notes |
|---|---|---|---|
| 1 | `fd_index` | `u32` | which of the fds that arrived with this frame holds the payload. The engine attaches exactly ONE fd per `OOB_FD` frame, so it is always `0`; a receiver refuses any other value |
| 2 | `len` | `u64` | the payload's exact length; the memfd's size equals it. Bounded by `MAX_FRAME_PAYLOAD`, like an inline payload — a receiver refuses more |
| 3 | `encoding` | `u8` | registry `oob_encoding::*`. `FRAME_PAYLOAD` (`0`, the only value) means the memfd holds EXACTLY the bytes the frame would have carried inline, so the receiver decodes them as that payload |

In this build only a **success terminal** moves (`END | OOB_FD`; the memfd holds the whole
`Outcome::Ok` envelope, §6), and only when its inline payload is at least the engine's threshold
(`FERRO_MEMFD_THRESHOLD_BYTES`, default 1 MiB). It is still the request's ONE `END`. A stream's
`HEAD`/`DATA` frames never move. A CLIENT frame carrying `OOB_FD` remains session-fatal (§7's
`reserved_flag.bin`).

**Pairing an fd with its frame — the rule both sides follow.** On a `SOCK_STREAM` Unix socket an fd
is attached to the bytes of the `sendmsg` that carried it, but one `recvmsg` can return those bytes
together with EARLIER, fd-less bytes, so an fd cannot be paired by its position in a read. The engine
writes every earlier frame completely, then sends the `OOB_FD` frame with a `sendmsg` that begins at
the frame's first byte and carries its one fd, and attaches fds to nothing else. Fds therefore arrive
in the same order as `OOB_FD` frames, each no later than its own frame's first byte. A receiver reads
**every** byte with `recvmsg` (plain `read(2)` makes the kernel discard an attached fd), queues the
fds it receives in arrival order, and gives the n-th `OOB_FD` frame the n-th fd. An `OOB_FD` frame
with no fd queued, a memfd whose size is not `len`, or an `OOB_FD` frame on a session that never set
`MEMFD_RX` means the two ends disagree about the byte stream: a desync.

**What a receiver must not lose.** A receiver whose process has no free fd-table slot gets no fd: the
kernel closes it and reports a truncated control message (`MSG_CTRUNC`), and the result is gone. A
receiver therefore keeps a slot free for the read of an `OOB_FD` frame's FIRST byte (the only read an
fd can arrive with when every read is exact-length) — the PHP client holds one slot in reserve and
frees it for that read. A `MSG_PEEK` of the header into no control buffer reads the flags without
taking the fd. Engine side, the memfd is made immediately before its frame is sent and the engine's
copy closed right after, so a terminal waiting behind a slow reader holds no fd in the engine.

Vector: `oob_ref` (§7).

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
  **two**: `HelloAck.boot_epoch` (a random per-start id, §19.1) and the **`U64` TypedValue payload
  (tag 3, M1-S7 — §3.2)**, which carries a user column value and is therefore full-range by
  definition. Both MUST take the decimal-string path above; applying the native-PHP-int rule to
  either silently truncates every value above `PHP_INT_MAX`. All OTHER `u64` fields
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
| `DATE` | 8 | `str` | `"YYYY-MM-DD"` | `"infinity"` / `"-infinity"` for the PG sentinels; `"0000-00-00"` for a MySQL zero date, and any zero month/day component (`"2026-00-05"`) — see **Sentinels** below for the full class. |
| `TIME` | 9 | `str` | `"HH:MM:SS"` or `"HH:MM:SS.ffffff"` | Hours may exceed 23 (PG `time '24:00:00'`); a MySQL `TIME` spans ±838 h and may be negative → a leading `-`. |
| `TIMESTAMP` | 10 | `str` | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` | **Naive** — no zone suffix, ever. Sentinels: `"infinity"` / `"-infinity"` for the PG values; `"0000-00-00 00:00:00"` for a MySQL zero datetime. |
| `TIMESTAMPTZ` | 11 | `str` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` | RFC3339, **always normalized to UTC**, always the literal `Z`. Sentinels: `"infinity"` / `"-infinity"` (PG); `"0000-00-00 00:00:00"` for a MySQL zero `TIMESTAMP`. |
| `UUID` | 12 | `str` | 36-char canonical **lowercase** hyphenated | Never raw bytes. |
| `JSON` | 13 | `str` | the raw UTF-8 JSON document text | Not re-serialized and not validated by the engine; the client decodes lazily. |

**Fractional seconds** (`TIME`, `TIMESTAMP`, `TIMESTAMPTZ`): emit **no** `.ffffff` group when the
sub-second part is zero; otherwise emit **exactly six** digits. Never emit a trailing-zero-trimmed
variant — the payload must be byte-stable for the golden vectors.

**Sentinels** (`DATE`, `TIMESTAMP`, `TIMESTAMPTZ`): the two infinity forms (`"infinity"`,
`"-infinity"`) **and every value whose year, month or day component is ZERO** are **literal payloads
carried verbatim** — they are deliberately NOT parseable as a calendar value. The zero-component
class is larger than the two all-zero forms and is **not** a fixed list: it covers the zero date
`"0000-00-00"` / `"0000-00-00 00:00:00"` *and* a **zero-IN-date**, where only some components are
zero — `"2026-00-05"`, `"2026-08-00"`, `"2026-00-05 12:00:00"`. A decoder MUST therefore test the
components, never string-compare against the four named forms; a zero-in-date passed to a calendar
parser is exactly the silent-corruption class this paragraph exists to prevent. A backend renderer
emits all of them as-is rather than inventing a date, and a client decoder must branch on them
**before** attempting to construct a date/time object; feeding one to a parser yields either an
exception or a nonsense date. PG's ±infinity arrive as the `i32`/`i64` extremes; a MySQL zero date
is legal wherever `sql_mode` omits `NO_ZERO_DATE`, and a zero-in-date wherever it omits
`NO_ZERO_IN_DATE` (MariaDB 11's default; MySQL 8's default sets both, so writing one there needs an
explicit `SET SESSION sql_mode = ''`). Month and day are consequently NOT range-checked below 1 —
only above 12 / 31. Shipped on both sides: `ferro-backend-mysql`'s `mytext.rs` and the client's
`CanonicalText::dateIsSentinel` / `timestampIsInstant` / `timestamptzIsInstant`.

**Calendar range** (`DATE`, `TIMESTAMP`, `TIMESTAMPTZ`): the canonical `YYYY-MM-DD` form defines
**years `0001`–`9999` only**. A backend value outside that range — a BC date, or a year above 9999
(PG's `date`/`timestamp` reach 5874897 AD) — is a loud `NonRetryable{Unsupported}` naming the year,
never an invented `" BC"` suffix, negative year or 7-digit year, since a guessed form would differ
between the two codecs. Widening it means adding an extended-year/BC canonical form here, to both
codecs, and to the golden vectors (SPEC §22.2).

**`U64` uses the canonical narrowing ladder**, not a fixed `0xcf`. `Value::U64(0)` is a positive
fixint (`0x92 0x03 0x00`); the marker widens through `0xcc`/`0xcd`/`0xce` and reaches `0xcf` only
above `0xffffffff`. This is byte-identical to PHP `PurePacker::packUint`, so a decoder must accept
**any** uint marker for tag 3 — a marker-strict `uint64`-only reader is a defect. Consequence for
the PHP side **under the PURE decoder** (`PurePacker`, the authoritative one for this case per §2):
a `U64` at or below `0xffffffff` decodes to a PHP `int`, while anything above it decodes to a
**decimal string**, so the value's PHP type follows its **magnitude**, not its tag. `ext-msgpack`
does NOT split at the same point — it returns a native `int` for those same `0xcf` markers all the
way up to `PHP_INT_MAX` and only goes lossy (a float) above it, which is why §8.3 bars a
golden-vector `U64` from the `(2^32, 2^63]` band where the two decoders disagree.

The `str`-family payloads are **canonical text produced by the backend**. The codecs move that text
verbatim and validate nothing beyond UTF-8 — the rendering decision lives where the source format is
known (the backend), never in the codec.

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
| 3 | `manifest_hash` | `str \| nil` | the checked-SQL manifest the client was built against (§11 of the SPEC, M3-D2d), or `nil` for none. When set it must equal the engine's loaded manifest hash, or the handshake is refused session-fatal `Unsupported` (also when the engine has no manifest). `nil` makes no claim and is always admitted. `HELLO_ACK` sets the `MANIFEST` engine feature when a manifest is loaded |
| 4 | `pid` | `u32` | client OS pid, diagnostic |
| 5 | `features` | `u32` | client feature bitfield: `MEMFD_RX 0x01`, `FIBERS 0x02`. `MEMFD_RX` licenses the engine to send `OOB_FD` frames (§1.1), so a client sets it only when it reads every byte with `recvmsg`; `FIBERS` is informational |

### `HELLO_ACK` (service `CORE`, method `HELLO_ACK` = 2) — server → client

| # | field | type | notes |
|---|---|---|---|
| 1 | `engine_version` | `u32` | |
| 2 | `boot_epoch` | `u64` | unique per daemon start (§19.1); see §2 uint64-overflow note |
| 3 | `features` | `u32` | engine feature bitfield: `MEMFD 0x01` (the §1.1 path is enabled; informational — a client reacts to the `OOB_FD` flag, not to this), `LISTEN_STREAMS 0x02`, `MANIFEST 0x04`, `HTTP 0x08` (this engine SERVES the HTTP service, §12; a client must check it before sending an HTTP `REQUEST`, because an engine built without the cargo feature `http` has the same registry hash. No engine sets it yet: it is first set by the slice that serves a request, M6-F4) |
| 4 | `pools` | `array<[str, str, str \| nil, bool \| nil]>` | one nested positional entry per pool available on this engine — `[name, kind, server_version, literals_are_standard]`; see below (M1-S8a; fourth element M2-C2g) |
| 5 | `type_registry_hash` | `str` | echoed back; mismatch vs. the client's hash is a hard error |

**`pools` element (`PoolInfo`, M1-S8a; fourth element M2-C2g)** — a positional fixarray of 4, in this order:

| # | field | type | notes |
|---|---|---|---|
| 1 | `name` | `str` | what a client puts in `ExecRequest.pool` / `BeginRequest.pool` |
| 2 | `kind` | `str` | the backend FAMILY: `"postgres"` or `"mysql"` (MariaDB is `"mysql"` — it is the same family and the same wire protocol; the *product* is distinguishable only from `server_version`). Derived engine-side from the DSN SCHEME, so it is known without dialling the backend |
| 3 | `server_version` | `str \| nil` | the backend's own `version()` output, **verbatim and unnormalised**; `nil` when the engine has not learned it |
| 4 | `literals_are_standard` | `bool \| nil` | is a backslash inside a single-quoted string literal an ORDINARY CHARACTER on this backend? `nil` when the engine has not learned it. See below (M2-C2g) |

Two rules that are contract, not implementation detail. **`server_version` is never normalised on
the wire** — stripping PostgreSQL's leading product word or extracting a `major.minor.patch` is a
consuming tier's job (a Doctrine driver needs the literal substring `mariadb` to take its MariaDB
branch), and normalising here would bake one ecosystem's conventions into the protocol. And
**`nil` is a legitimate steady state, not an error**: the handshake never depends on a backend being
reachable — `ferrod` boots and serves `HELLO_ACK` with every upstream down — so a client must treat
an absent version as "unknown", never as a failure. Since M1-S8a Task 12 the engine LEARNS the
string, lazily, on the first handshake that asks: every pool is probed concurrently under one
bounded budget, a success is cached with a TTL and a failure with a short backoff, and any pool that
has not answered when the budget expires simply rides as `nil` for that handshake. So `nil` may mean
"never learned", "learned then expired", or "not learned YET" — all three are the same contract to a
client, and none of them can fail or delay the handshake.

**`literals_are_standard` answers ONE question, and it is deliberately not named for a backend's own
setting.** A client that must build a SQL string literal — `PDO::quote()`, and therefore Laravel's
`DB::escape()` and every `Builder::toRawSql()` — needs to know whether doubling `'` is the whole
rule. It is, exactly when a backslash is an ordinary character inside the literal. Both families
have that property under different names: PostgreSQL's `standard_conforming_strings`, MySQL's
`NO_BACKSLASH_ESCAPES` in `sql_mode`. Putting either NAME on the wire would bake one backend's
vocabulary into the protocol, and a client would then have to know which family's spelling to look
for. **MySQL needs this bit more than PostgreSQL does**: PostgreSQL has defaulted to the safe value
since 9.1, while MySQL defaults to the UNSAFE one, so a MySQL `quote()` cannot be written at all
without it.

It costs no round trip in any family. PostgreSQL reports `standard_conforming_strings` as a
`GUC_REPORT` parameter, so it arrives in the startup `ParameterStatus` stream and again on every
change, and the M1-S1 vendored fork already mirrors it (`Client::parameter`). MySQL and MariaDB
report it on EVERY OK packet, as the `SERVER_STATUS_NO_BACKSLASH_ESCAPES` status flag, and the
engine reads it off the last OK packet the probe's checkout already holds (M2-C1g). SQLite's answer is a constant
`true`: its literal has no backslash escape mode at all (M2-C1g). It rides the same lazy,
concurrent, TTL'd per-pool probe as `server_version`, so it inherits that probe's `nil` contract
exactly: `nil` may mean never learned, learned then expired, or not learned yet, and all three are
one thing to a client — **unknown**.

**It describes the PROBED session, and that is why it must not be a quoting rule** (M2-C1g,
SPEC §22.2 (cc)). The value is cached per pool for the probe's TTL; an operator can change a
server's global mode under it; a tenant can change its own session's mode inside a transaction; and
on MySQL an `init_connect` applies to a fresh dial but not after `COM_RESET_CONNECTION`, so a pool's
fresh and recycled sessions can genuinely differ. C1g's adversarial review turned each of those into
a live breakout against a literal built from the advertised bit. **A client that builds SQL string
literals should therefore use forms whose meaning does not depend on the mode** — the Laravel tier
doubles `'` for a string without a backslash (the backslash is the only mode-dependent character)
and uses PostgreSQL's `E'…'` or MySQL's `_utf8mb4 X'<hex>'` for one with a backslash — and treat
this field as INFORMATIONAL. A client that does use it must refuse on `nil` and on any value whose
rule it does not implement. (C2g designed the field as the quoting authority; C1g withdrew that
role after the review. No wire change: the field and its three states are as they were at
`protocol_version` 3, and before C1g every non-PostgreSQL pool advertised `nil`.)

The DSN is **never** on the wire (SPEC §12 — it is a server-side secret), and `pools` is ordered by
`name` so two connections to one engine see the identical list.

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

### `WINDOW_UPDATE` (service `CORE`, method `WINDOW_UPDATE` = 6) — either direction

| # | field | type | notes |
|---|---|---|---|
| 1 | `frames` | `u32` | additional frame credit granted for the named `request_id` |
| 2 | `bytes` | `u32` | additional byte credit granted for the named `request_id` |

Note: `WINDOW_UPDATE` is itself carried in a frame whose header `request_id` names the stream
being credited (§5.2) — the message body does not repeat `request_id`.

**Client → server** (since M1-S5) it replenishes the credit of a server → client stream. **Server →
client** (since M3-D4, and only on an open `COPY_IN`'s `request_id`) it grants the CLIENT credit to
send COPY data; the first one also means "the COPY has started" (§13). Same shape both ways — locked
by the one `window_update` vector. Neither direction is ever answered.

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
| 6 | `detail` | `str \| nil` | backend detail/hint text, if any. **On service `HTTP` it is never free text:** it is either `nil` or EXACTLY one token of the registry's `[http.causes]` vocabulary (`consts::http_cause::*` / `Constants::HTTP_CAUSE_*`, SPEC §23.5.6), and `sqlstate` and `errno` are `nil`. It is `nil` only on an HTTP terminal that is not an exchange fate — `Protocol` (a malformed frame) and `Unsupported` (not served) — and a cause token on every other HTTP error (§12.6). On service `QUEUE` the engine's own refusals carry `detail` `nil` (§14.3) |
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
with zero unbounded allocation; also used as `cargo-fuzz` seeds). They are **generated, never
hand-written** — `cargo run -p ferro-proto --bin gen-vectors` emits them from `ferro-proto`'s own
encoders and the result is committed, so the on-disk bytes ARE the canonical encoder output. They
are the byte-level arbiter for everything in this document: where a vector and this prose disagree,
the vector wins.

**Core service + the cross-cutting envelopes** (§4/§5/§6): `hello`, `hello_ack` (also locks the §2
`boot_epoch` uint64 → decimal-string rule), `ping`, `pong`, `goodbye`, `window_update`, and
`error_protocol` (a terminal `Outcome::Error(ERROR)` with `sqlstate` and `errno` both `nil`).

**M1-S8a reshaped `hello_ack`** (§4): its `pools` list carries **two** nested `PoolInfo` triples —
`["main", "postgres", "PostgreSQL 17.10"]` and `["reporting", "mysql", nil]` — covering both the
`Some` and the `nil` arm of `server_version`, with pairwise-distinct field values. Non-empty is
load-bearing: the pre-S8a fixture was `pools: []`, which byte-locks **no** element shape at all, and
that was measured — with the empty fixture a deliberately swapped `name`/`kind` order in the PHP
encoder passes the byte lock; with these two triples it fails. The same commit bumped
`protocol_version` 1 → 2 (§1), so **every** committed vector's `frame_hex` changed in byte 1 and
every `negative/*.bin` fixture was regenerated with it (`bad_version.bin` alone is unchanged — its
version byte is an explicit `0x99` that must stay wrong).

**M1-S8a:** `error_mysql_errno` — the FIRST vector locking a **non-null `errno`** (field 4, §5)
alongside a real `sqlstate`: a MySQL duplicate key, `errno 1062` / SQLSTATE `23000`, carried in a
terminal `Outcome::Error`. It exists because the two fields are independent on the wire and only the
`errno` carries vendor-level identity — MySQL reuses `23000` for both a duplicate key (`1062`) and a
NOT NULL violation (`1048`), so a consumer keyed on the SQLSTATE alone cannot tell them apart. The
`errno` rides the §2 signed/unsigned narrowing ladder like any other integer field (`1062` ⇒
`cd 04 26`, a `uint16`), NOT a fixed width.

**Per-service indexes:** SQL `EXEC` → §8.3 · TX → §9.6 · STREAM `HEAD`/`DATA` → §10.3 · ADMIN → §11.3 · HTTP → §12.5 · COPY (`COPY_IN`/`COPY_OUT`, `COPY_DATA`/`COPY_DONE`) → §13.4 · QUEUE → §14.4 (with the
`refusal/` vectors).

**M3-D3:** `oob_ref` — an `END | OOB_FD` SQL/`EXEC` terminal whose payload is the §1.1 `OobRef`
(`len = 1048578`, past u16, so the width is locked), NOT an `Outcome`: the `Outcome` is what the memfd
holds, which a vector cannot. Byte-locked by name in the PHP conformance suite.

**Negative seeds:** `bad_magic.bin`, `bad_version.bin`, `oversize_len.bin`, `reserved_flag.bin`
(the last has a structurally valid header and is rejected at the flags layer, not by
`Header::decode`). That required-name list is itself asserted, so a deleted seed cannot make the
rejection test pass vacuously.

**Per-tag completeness (M1-S7).** Every tag in `/proto/types.toml`'s `implemented` list must be
exercised by at least one committed vector, and no deferred tag may be. This is asserted by
`ferro-proto/tests/golden_vectors.rs::every_implemented_tag_has_a_vector`, which **derives** its
required set from the registry rather than a parallel hardcoded list, and which walks each vector's
**decoded** `ColMeta` tags and `Value`s — never a text scan of the JSON, which would pass on a
vector whose `message` and `frame_hex` disagree.

**Byte-lock coverage is keyed on the vector NAME.** The PHP conformance suite selects its
cross-language byte-lock cases by prefix (`sql_exec_`, `stream_head_`, `stream_data_`, `http_` — since
M6-F2 — and `queue_`, since M7-G1a); a vector
named outside those prefixes silently receives only the generic header/unpack tests. New SQL and
STREAM vectors MUST use those prefixes (asserted by
`VectorConformanceTest::testEveryCommittedVectorIsByteLocked`). A vector outside those prefixes
(`tx_begin_response`, `error_protocol`, `error_mysql_errno`) earns its lock from a NAME-keyed test
instead, and that accounting is **derived, not declared** (M1-S8a): the guard scrapes its own source
for `loadVector('<name>.json')` call sites, so a name can only count as locked when a byte-lock test
that loads it actually exists. Appending a name to a hand-written array — the shape this replaced —
would have let a registration certify a vector with no byte-lock test at all.

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
  the prefix). The required lengths: `ExecRequest` = 9 (8 before `protocol_version` 4), `ExecOk` = 5, `ColMeta` = 2 (`[name, tag]`),
  and each `Value` = 2 (`[tag, payload]`, §3). Both reference codecs enforce this; a lax third
  implementation that trusted a shorter/longer prefix would mis-frame every following field.
- **`Option<Value>` peek rule.** An optional `Value` slot (`ExecOk.last_insert_id`) is a bare
  `nil` (`0xc0`) when absent, else the value's own `[tag, payload]` encoding. Decoders peek the
  first byte: `0xc0` ⇒ absent; anything else ⇒ decode a `Value`. This is unambiguous because a
  present `Value::Null` encodes as the fixarray `[NULL, nil]` (first byte `0x92`), never a bare
  `0xc0`. (Optional scalars — `sql`, `query_id`, `timeout_ms`, `tx_id`, `traceparent` — follow the same nil-vs-value rule.)
- **`array16` threshold.** Array length prefixes narrow by count: fixarray (`0x9_`) for ≤15
  elements, `array16` (`0xdc` + `u16` BE) for 16..=65535, `array32` (`0xdd` + `u32` BE) beyond.
  A result with ≥16 columns or ≥16 cells in a row therefore emits `0xdc`; both codecs must agree at
  the boundary (locked by `sql_exec_response_wide`). Every decoded array length is bounds-checked
  against the bytes remaining before any allocation (a MessagePack element is ≥1 byte), closing the
  lying-`u32::MAX`-length hole.

### 8.1 `ExecRequest` (service `SQL`, method `EXEC` = 1) — client → server

A positional fixarray of 9 fields in declaration order (payload of a non-`END` `EXEC` frame):

| # | field | type | notes |
|---|---|---|---|
| 1 | `pool` | `str` | target pool name |
| 2 | `sql` | `str \| nil` | literal SQL; `nil` iff a `query_id` is used. Exactly one of the two must be set: both is `Protocol`, neither is `Unsupported` |
| 3 | `query_id` | `str \| nil` | manifest query id (M3-D2d): the engine runs the manifest's SQL for it. The request's `readonly`, and the pool it runs on (`pool` for autocommit, the transaction's pinned pool when `tx_id` is set), must equal the manifest's declaration, else `Unsupported`; so are an unknown id and an engine with no manifest. Setting both `sql` and `query_id` is `Protocol` |
| 4 | `params` | `array<Value>` | positional bind params, each a `[tag, payload]` `Value` (§3) |
| 5 | `timeout_ms` | `u32 \| nil` | per-statement deadline hint |
| 6 | `readonly` | `bool` | client-declared; drives the write-loss → `Indeterminate` split (no engine inference) |
| 7 | `fetch` | `u8` | `0` = rows, `1` = none (affected only), `2` = stream (§10's windowed `HEAD`/`DATA`/`END` producer, M1-S5) — a valid, wire-accepted value as of M1-S5 Task 1 (the codec never restricted it); the ferrod EXEC handler's `Unsupported` rejection of `2` is unchanged by that task and lifts in a later S5 task |
| 8 | `tx_id` | `u64 \| nil` | S6: `nil` = autocommit; a value routes this EXEC to the actor pinning that tx's conn (§9). Bounded < 2^63 (native int, §2); the opt-u64 nil/value peek rule (below) applies |
| 9 | `traceparent` | `str \| nil` | M2-C4c-1: the caller's W3C Trace Context `traceparent` header, as its own text. `ferrod` parses it against the W3C grammar and joins the statement's observability to that trace (the slow log's `trace_id`/`parent_span_id`). A value that does not parse is **ignored and counted** (`ferro_traceparent_invalid_total`), never refused: an observability field must not fail a statement. **The one `str` field a decoder MUST NOT refuse over its bytes:** the framing stays strict (a `str` or `nil`, anything else is a wire fault), but invalid UTF-8 is decoded lossily — the Rust codec substitutes U+FFFD, which the ASCII-only W3C grammar then rejects and the engine counts. Every other `str` still refuses invalid UTF-8. The PHP client fills it from `Ferro\Client\TraceContext`'s provider, read once per EXEC; it sends `nil` for a provider that throws, returns a non-string, an empty string, more than 512 bytes or anything but printable ASCII (`0x21`–`0x7e`), and drops a context that would push the frame over `MAX_FRAME_PAYLOAD` |

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
bare `nil`), and `sql_exec_request_traceparent` (field 9 `Some`: the W3C specification's own example
header — every other request vector locks its `None` path as a second trailing bare `nil`).

**M1-S7 canonical-tag response vectors** (§3.2), three of them, split deliberately:

- `sql_exec_response_types_scalars` — one cell per S7 tag in its everyday shape: `DECIMAL
  "-12345.6700"` (display scale preserved), `DATE`, `TIME`, `TIMESTAMP` (with `.ffffff`),
  `TIMESTAMPTZ` (RFC3339 `Z`), `UUID` (lowercase hyphenated), `JSON` (nested + a `null` + a
  non-ASCII char, proving UTF-8 survives), and a **small** `U64` (`5`).
- `sql_exec_response_types_edge` — the sentinels and the fraction-omission rule: `DECIMAL "NaN"`,
  a bare 30-digit `DECIMAL`, `DATE "infinity"`, `DATE "0000-00-00"`, `TIME "24:00:00"` (PG-legal),
  `TIME "-838:59:58.000001"` (MySQL: negative and beyond 24 h), a whole-second `TIMESTAMP` with
  **no** `.ffffff` group, `TIMESTAMP "0000-00-00 00:00:00"`, and `TIMESTAMPTZ "-infinity"`.
- `sql_exec_response_types_u64` — `U64 18446744073709551615` (`u64::MAX`) **alone**.

**Hard constraint on any golden-vector `U64`:** it must be `<= 0xffffffff` **or** `> PHP_INT_MAX`,
and **never** inside `(2^32, 2^63]`. The uint ladder reaches marker `0xcf` at 2^32, and PHP's pure
decoder returns a decimal **string** for every `0xcf` uint64 while `ext-msgpack` returns an int —
so a value in that band makes the ext-vs-pure parity assertion fail. `u64::MAX` therefore lives in
its own vector: a `> PHP_INT_MAX` uint makes that assertion skip the **whole** vector, so isolating
it keeps the parity coverage for every other tag. (Same reasoning puts the bare 30-digit `DECIMAL`
in `edge`: its only cost is that one vector's parity comparison, never the byte lock.)

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

**A `tx_id` the engine has no LIVE entry for is `errc::TX_NOT_FOUND` (`0x300B`, NonRetryable), never
`errc::PROTOCOL`.** It covers unknown, already-committed, already-rolled-back, aborted, and another
session's — all deliberately indistinguishable to the client (SPEC §7), and it applies to a tx-scoped
`EXEC` as much as to the TX methods. The one tx-gone case that is NOT this code is the engine's own
deadline tombstone, which stays `errc::TX_DEADLINE` (`0x1003`, **Retryable**) so its owner can tell
"the engine ended this" from "this was never yours".

The split from `errc::PROTOCOL` — which stays reserved for a genuine WIRE fault (a body that will not
decode, an unknown savepoint name) — is load-bearing for clients, not cosmetic. A client's
`rollBack()` is normally called from a `finally` already carrying the caller's real error, so it must
swallow "that transaction is gone" rather than replace that error; while the two facts shared one
code, swallowing the first meant swallowing the client's own codec defects with it.

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

`error_tx_not_found` is the TX service's terminal vector: an `Outcome::Error(ErrorPayload)` carrying
`code = 0x300B` on `TX`/`COMMIT` with the `END` flag. It is the first committed vector that is a TX
RESPONSE rather than a request, which is why a decoder keyed only on `(service, method)` must also
read the `END` flag to tell the two apart.

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

**M1-S7:** `stream_data_types` — the SAME S7 canonical-tag row as `sql_exec_response_types_scalars`
(§8.3), carried in a `DATA` frame. The streamed path decodes cells through the same per-cell
TypedValue codec as the buffered one, so this byte-locks it **independently** rather than assuming
the buffered vector covers it.

## 11. ADMIN service messages (`BACKUP`)

The ADMIN service (`SERVICE_ADMIN = 5`, M2-C3-7b; SPEC §7.6, D14, D15) carries operator verbs. Its
methods are registry `methods.admin` (`/proto/registry.lock.json`): `BACKUP = 1`. Its messages are
`Value`-free, so they ride the same `msg!`/rmp-serde positional layout as the TX messages (§9).

**Authorization happens before any handler, in the session layer (SPEC D15).** Every admin method
has a fixed class in `ferrod` — READ or OPERATE — and the session checks the peer's
KERNEL-ATTESTED uid (`SO_PEERCRED` on the session's own Unix socket, never anything on the wire)
before the request enters the request lifecycle: a READ verb needs only an admitted peer, an OPERATE
verb additionally needs the uid in `FERRO_ADMIN_UIDS`, and **an empty `FERRO_ADMIN_UIDS` disables
every OPERATE verb**. A connection whose uid the kernel cannot attest is refused every admin verb. A
refusal is one terminal `Outcome::Error` with `code = errc::FORBIDDEN` (`0x300C`, NonRetryable) on
the request's `request_id`; because the session — not a handler — sends it, its header is the generic
session terminal (`service = CORE`, `method = 0`, `END`), the same shape as `error_protocol` (§7). A
refused verb never reaches pool lookup, so a refusal is identical whether or not the named pool
exists. An `ADMIN` method id this build does not serve is `Unsupported`, never a handler.

`BACKUP` is an OPERATE verb.

### 11.1 `BackupRequest` (service `ADMIN`, method `BACKUP` = 1) — client → server

A positional fixarray of 4 fields:

| # | field | type | notes |
|---|---|---|---|
| 1 | `pool` | `str` | the pool whose database is snapshotted — a SQLite pool; any other is `Unsupported` |
| 2 | `file` | `str` | the snapshot's FILE NAME — `[A-Za-z0-9._-]`, 1..=200 bytes, not starting with `.` — placed in the pool's D14 allowed directory. **Not a path**: no directory crosses the wire in either direction (a failed snapshot's error text has the directory redacted), and anything else is `Forbidden`. A name that is any SQLite pool's live database in that directory, or one of its `-wal`/`-shm`/`-journal` sidecars, is `Forbidden` regardless of `replace` |
| 3 | `replace` | `bool` | `true` swaps a new snapshot over an existing regular file of that name atomically (`rename`); `false` refuses an existing name (`Forbidden`) |
| 4 | `timeout_ms` | `u32 \| nil` | bounds the snapshot statement; `nil` leaves it unbounded. A per-request `CANCEL` stops it either way |

### 11.2 `BackupResponse` — terminal `Outcome::Ok` body — server → client

`[OUTCOME_OK, <BackupResponse>]`, a positional fixarray of 3, composing into the envelope exactly as
`BeginResponse` does (§9.3):

| # | field | type | notes |
|---|---|---|---|
| 1 | `bytes` | `u64` | the finalised snapshot's size |
| 2 | `queue_us` | `u64` | pool wait (SPEC §13's split) |
| 3 | `exec_us` | `u64` | the snapshot statement itself |

A failed `BACKUP` never leaves a file: the snapshot is written to a temporary the engine creates
exclusively (mode `0600`, an unpredictable name) and only moved into place on success — and only if
that temporary is still the engine's own file with a single link — and the previous snapshot
survives a failed `replace`. The published snapshot is mode `0600`, owned by the engine's user. A cancelled or timed-out
backup is `Cancelled`/`QueryTimeout` — never `Indeterminate`, since the source database is not
written.

### 11.3 ADMIN vector index

`admin_backup_request` (`replace = true`, `timeout_ms = 30000` — the populated arm of the nullable),
`admin_backup_response` (the terminal `Outcome::Ok(BackupResponse)`, with `bytes = 5000000000` so the
u64 width is locked rather than a fixint every width would pass), and `error_forbidden` (the D15
refusal, carrying the generic session-terminal header the engine actually sends — `CORE`/0 — and
locking that `Forbidden` is a code distinct from `Auth`, which is the BACKEND refusing the pool's own
credentials).

**No `protocol_version` bump.** The registry hash (`TYPE_REGISTRY_HASH`, an FNV-1a over the whole
lock file) already moved with these additions, and the engine refuses a client whose hash differs at
the handshake — so a skewed pair fails at the first frame rather than mid-request. `protocol_version`
moves when an EXISTING message changes shape (§1); these are new messages on a new method, and a new
error code that an older decoder would classify correctly by its explicit `branch` (§5).

## 12. HTTP service messages (`REQUEST`/`HEAD`/`BODY`)

The HTTP service (`SERVICE_HTTP = 6`, M6-F2; SPEC §23) carries Ferro HTTP: a PHP worker hands the
engine a request addressed to an operator-declared UPSTREAM by name, and the engine sends it at most
once and streams the response back. Its methods are registry `methods.http`
(`/proto/registry.lock.json`): `REQUEST = 1` (client → engine), `HEAD = 2` and `BODY = 3` (engine →
client only). The engine routes a client frame carrying `HEAD`, `BODY` or any other HTTP method id
to `Unsupported` (SPEC §23.5, review F26).

**A response always streams.** One `HEAD`, zero or more `BODY` frames (each with the `STREAM` flag),
then the request's one `END`. A completed exchange is `Outcome::Ok(HttpDone)` WHATEVER its status —
a 500 is a completed exchange, not an error (SPEC §23.7.4). An exchange that fails is
`Outcome::Error` with a `[http.causes]` token in `detail` (§5, §12.6), or `Outcome::Cancelled`.
HTTP does **not** reuse STREAM's `HEAD`/`DATA` (§10), whose payloads are SQL `ColMeta` and `Value`
rows; all four messages share the request's `request_id` (§5.2). Credit is keyed by `request_id` and
the `STREAM` flag, not by service, and HTTP uses the session's global window (SPEC §23.5).

**The messages are `Value`-free but carry `bin`.** Header values, a reason phrase and bodies are not
guaranteed UTF-8, so they ride the msgpack **`bin`** family; header NAMES are `str`. Every message is
a positional fixarray with strict arity (§8's rules apply: the declared length must match, every
array and `str`/`bin` length is bounded by the bytes remaining before anything is allocated, and
trailing bytes are refused). The Rust codec is hand-rolled (`ferro-proto` `messages::http`), because
rmp-serde writes a `Vec<u8>` as an array of integers, not `bin`. A header is the 2-element array
`[name: str, value: bin]`, and a header block is an array of them in wire order — duplicates and order
are preserved; it is never a map.

**The codec checks the wire's types and widths, never HTTP semantics.** A `u16` that does not fit,
a `str` that is not UTF-8, a `bin` where a `str` belongs (Rust) or a value of the wrong type (PHP) is a
malformed frame. What is ALLOWED — a method, a target, a header name, a status range, a version, a
chunk size — is the engine's validator (SPEC §23.4, slice F3) and producer's (F4) business, never a
decoder's. (PHP cannot tell a msgpack `str` from a `bin` after unpack, both arrive as a PHP string,
so on the PHP side that one check is the encoder's: it always writes the right family, which the
vectors lock.)

### 12.1 `HttpRequest` (service `HTTP`, method `REQUEST` = 1) — client → server

A positional fixarray of 13:

| # | field | type | notes |
|---|---|---|---|
| 1 | `upstream` | `str` | the upstream's NAME (SPEC §23.3), never a URL |
| 2 | `method` | `str` | SPEC §23.4.1 |
| 3 | `target` | `str` | origin-form (SPEC §23.4.2); strict UTF-8, so a non-UTF-8 target is `Protocol` |
| 4 | `origin` | `str \| nil` | the origin the caller believes the upstream has — checked, never used |
| 5 | `headers` | `array<[str, bin]>` | SPEC §23.4.3 |
| 6 | `body` | `bin \| nil` | `nil` = no body; a zero-length `bin` = a present, empty body (`c4 00`) — distinct on the wire. Bounded by `MAX_FRAME_PAYLOAD` minus the rest of the frame (SPEC §23.4.4) |
| 7 | `timeout_ms` | `u32 \| nil` | the whole exchange; `nil` means the upstream's `TIMEOUT_MS`, and it is always capped by it |
| 8 | `connect_timeout_ms` | `u32 \| nil` | capped by `CONNECT_TIMEOUT_MS` |
| 9 | `read_timeout_ms` | `u32 \| nil` | the idle bound between body bytes |
| 10 | `idempotent` | `bool \| nil` | the CALLER's declaration (SPEC §23.7.2): `true` declares, `false` downgrades, `nil` defers to the operator |
| 11 | `decode` | `bool` | decode `gzip`/`deflate` (SPEC §23.9.2) |
| 12 | `route` | `str \| nil` | observability only; never sent upstream |
| 13 | `traceparent` | `str \| nil` | §8.1 field 9's rule exactly: the framing is strict, the BYTES are decoded lossily (the Rust codec substitutes U+FFFD), and a value that does not parse is ignored and counted, never refused. Never forwarded upstream |

A client refuses, before sending, what the engine's decoder would refuse: a non-UTF-8 strict `str`, a
timeout outside `u32`, a value of the wrong type (`Ferro\Protocol\HttpRequest::encode` throws
`CodecException`).

### 12.2 `HttpHead` (service `HTTP`, method `HEAD` = 2) — server → client

Sent once, before any `BODY`; NOT terminal and NOT flagged `STREAM` (its frame's flags are `0`).
A positional fixarray of 6:

| # | field | type | notes |
|---|---|---|---|
| 1 | `status` | `u16` | the producer sends `200..=599` (a 1xx is consumed; a 101 is malformed, SPEC §23.5.6) |
| 2 | `version` | `u8` | `10`, `11` or `20` |
| 3 | `reason` | `bin \| nil` | the HTTP/1.x reason phrase as received; `nil` on HTTP/2 |
| 4 | `headers` | `array<[str, bin]>` | as received, minus hop-by-hop, plus SPEC §23.9.2's changes; names lowercase |
| 5 | `decoded` | `[str, u64 \| nil] \| nil` | when the engine decoded the body: the `Content-Encoding` it removed and the `Content-Length` it removed with it. The length is bounded < 2^63 (a native PHP int, §2), STRUCTURALLY: the encoder writes `nil` for one it cannot represent so, and both decoders refuse one ≥ 2^63 on the wire |
| 6 | `idempotent` | `bool` | the engine's EFFECTIVE idempotency (SPEC §23.7.2) — the authority a client classifies against after `HEAD` (SPEC §23.7.3) |

### 12.3 `HttpBody` (service `HTTP`, method `BODY` = 3) — server → client, flag `STREAM`

`[chunk: bin]`, a positional fixarray of 1. The producer sends a NON-EMPTY chunk of at most 256 KiB,
carrying what was readable when it was built (SPEC §23.5.3); that bound is the producer's contract,
not a codec check, and it is not a registry constant because no receiver enforces it — if one ever
must, it becomes a `/proto` key first (charter rule 2).

### 12.4 `HttpDone` — terminal `Outcome::Ok` body — server → client

`[OUTCOME_OK, <HttpDone>]` on the request's `HTTP`/`REQUEST` header with `END`. `HttpDone` is a
positional fixarray of 2, `[trailers: array<[str, bin]>, stats: HttpStats]`, and
`HttpStats` a positional fixarray of 8:

| # | field | type | notes |
|---|---|---|---|
| 1 | `queue_us` | `u64` | admission to dispatch (SPEC §23.6 step 4) |
| 2 | `connect_us` | `u64` | |
| 3 | `tls_us` | `u64` | |
| 4 | `ttfb_us` | `u64` | |
| 5 | `total_us` | `u64` | |
| 6 | `bytes_sent` | `u64` | HTTP/1.1: the write tracker's PLAINTEXT count since this exchange's dispatch (SPEC §23.5.4, §22.2 (ct)) |
| 7 | `bytes_received` | `u64` | same; the count that tells `eof_empty` from `eof_partial_head` |
| 8 | `reused` | `bool` | the exchange rode a reused connection |

Every `u64` here is contractually bounded < 2^63 (§2), so PHP decodes each to a native int and
REFUSES a decimal-string one rather than inventing a number.

### 12.5 HTTP vector index

Every vector named in SPEC §23.5.5, and only those (asserted by
`ferro-proto/tests/golden_vectors.rs::the_http_vectors_are_exactly_the_spec_list`, which parses the
spec's list):

- `http_request_get` — the smallest real request: every optional field `nil`, so each nil arm is
  locked.
- `http_request_post` — every field set, each distinct from every other field of its type (the six strings, the three timeouts, the two bools — `idempotent` true, `decode` false): a body whose first byte is the
  `0xc0` nil marker (a `bin` that must not be read as `nil`), a header value carrying `0x80`, a
  `read_timeout_ms` past u16, and the W3C specification's example `traceparent`.
- `http_head` — HTTP/1.1, a reason phrase, a non-UTF-8 header value, `decoded = ["gzip", 70000]`.
- `http_head_h2` — HTTP/2, `reason` and `decoded` both `nil`, effective idempotency `true`.
- `http_body` — a `STREAM`-flagged 260-byte chunk holding every byte value (so `bin16` is locked).
- `http_done` — `Outcome::Ok(HttpDone)` with a trailer and `stats` in every uint width (fixint,
  uint8, uint16, uint32, uint64).
- `error_upstream_unavailable` (`breaker_open`, `retry_after_ms` set), `error_rate_limited`
  (`rate_limited`, `retry_after_ms` set), `error_tls_refused` (`tls_verify`),
  `error_response_incomplete` (`body_eof`), `error_forbidden_http` (`forbidden_target`) — handler-built
  terminals, so each rides the request's own `HTTP`/`REQUEST` header with `END` (unlike
  `error_forbidden`'s session-built `CORE`/0), with `sqlstate`/`errno` `nil` and one cause token in
  `detail`.

The `http_` vectors are byte-locked in PHP by the `httpVectors()` prefix provider, which picks the
codec from each vector's HEADER method, never its name; the five error vectors by a name-keyed test.
In Rust, besides the decode→encode fixpoint every vector gets, each HTTP message is decoded and
compared against its vector's NAMED fields — a fixpoint alone passes a symmetric field swap.

### 12.6 The cause vocabulary, and the codes

On service `HTTP`, `ErrorPayload.detail` is exactly one token of `[http.causes]` (`/proto/errors.toml`,
SPEC §23.5.6) — 45 tokens, generated into both codecs as `consts::http_cause::*` (+ `ALL`) and
`Constants::HTTP_CAUSE_*` (+ `HTTP_CAUSES`); the registry table maps each constant NAME to its
token, the name must be the token upper-cased, and a token is `[a-z][a-z0-9_]*` (one rule, shared
by the registry parser and `build.rs`, copied in `gen-php.php`, and held to the shared fixture
`proto/tools/http-causes-shape-cases.json` in both languages). A Guzzle handler picks its exception class by
the cause (SPEC §23.11.3), which `message` must never be used for. The registry's table is checked
against SPEC §23.5.6's own table by `registry_sync.rs::http_causes_are_exactly_the_spec_table`.
`detail` is `nil` on exactly two HTTP terminals, neither of which is an exchange fate: `Protocol`
(the frame was malformed) and `Unsupported` (nothing serves it — every HTTP `REQUEST` in this build,
since no engine serves HTTP until M6-F4).

The codes HTTP added (SPEC §23.7.1): `UpstreamUnavailable` `0x1007` and `RateLimited` `0x1008`
(Retryable — refused before any byte existed on a connection; a breaker's refusal and every
`RateLimited` carry `retry_after_ms`), `TlsRefused` `0x300D` and `ResponseIncomplete` `0x300E`
(NonRetryable). `Forbidden` `0x300C` was widened to the HTTP policy refusals (C6). The other fates
reuse §9.2's existing codes (`ConnectionLost`, `PoolTimeout`, `QueryTimeout`, `WriteUnconfirmed`).

**No `protocol_version` bump** (the §11 ADMIN precedent): no existing message changed shape. These
are new messages on a new service, the registry hash moved with them and is checked at the handshake,
so a skewed engine/client pair fails at the first frame, and an older decoder classifies a new code
correctly by its explicit `branch` (§5).

*(Ferro Queue's messages, once reserved §13 here, are §14: M3-D4's COPY took §13 first.)*

## 13. COPY (`SQL`/`COPY_IN`, `SQL`/`COPY_OUT`, `STREAM`/`COPY_DATA`, `STREAM`/`COPY_DONE`)

PostgreSQL's COPY sub-protocol (M3-D4; SPEC §6.1). Methods: `methods.sql` `COPY_IN = 2`,
`COPY_OUT = 3`; `methods.stream` `COPY_DATA = 3`, `COPY_DONE = 4`. The client's statement reaches the
server unmodified; the engine refuses, before anything reaches the server, a statement that is not
exactly one `COPY … FROM STDIN` (for `COPY_IN`) or `COPY … TO STDOUT` (for `COPY_OUT`) — a refusal of
the request's SHAPE, `Unsupported`, never an inference about what it does. It sends nothing and
involves no connection, so with a `tx_id` it leaves the transaction open, as an `EXEC` refusal does. A `COPY … STDIN/STDOUT`
sent as an `EXEC` is refused the same way. Non-PostgreSQL pools refuse both methods (`Unsupported`).
No `protocol_version` bump: no existing message changed shape (the ADMIN precedent, §11.3); the
registry hash moved.

### 13.1 `CopyRequest` — the body of `COPY_IN` and `COPY_OUT` (client → server)

A positional fixarray of 5 (Value-free, the TX layout):

| # | field | type | notes |
|---|---|---|---|
| 1 | `pool` | `str` | the pool (ignored when `tx_id` is set — the transaction's pool is used) |
| 2 | `sql` | `str` | the COPY statement, verbatim |
| 3 | `readonly` | `bool` | the client's §19.3 declaration. Meaningful for `COPY_OUT` only — `COPY (DELETE … RETURNING *) TO STDOUT` writes, and the engine does not infer otherwise. A `COPY_IN` declared `readonly` is refused (`Unsupported`): the method is the write |
| 4 | `timeout_ms` | `u32 \| nil` | bounds the WHOLE COPY — the wait for a connection, the data, and the end — as `timeout_ms` bounds a stream. The PHP client sends `nil` |
| 5 | `tx_id` | `u64 \| nil` | run inside that transaction (§9) |

### 13.2 The exchange

**`COPY_IN`.** Client sends `COPY_IN`. The engine reserves the COPY's window, checks out, starts the
COPY, and only then sends a `CORE`/`WINDOW_UPDATE {frames, bytes}` on the request's id (§4): the
GRANT, which also means "the COPY has started". If the COPY cannot start, the terminal `END` comes
instead and no grant is ever sent — so no byte is ever sent into a COPY that did not start. The client
then sends `STREAM`/`COPY_DATA` frames (`STREAM` flag set) within its credit — each frame costs one
frame and its chunk's byte length — and reads the engine's further grants as it runs low; then one
`STREAM`/`COPY_DONE`. The terminal is an ordinary `Outcome` (§6): on success an `ExecOk` (§8.2) with
empty `cols`/`rows`, `affected` = rows copied, `stats.rows = 0`, `stats.bytes` = COPY bytes received.

- COPY data is DIVISIBLE (a byte stream; a chunk need not end on a row), so a client with ANY credit
  left can always send — it splits a chunk to fit — and the engine re-grants after it has forwarded
  half its window to the server: no deadlock. The engine's default window is 32 frames / 2 MiB.
- **Session-fatal** (`request_id = 0` terminal, `Protocol`, then the in-flight requests are drained
  and the session closes): a `COPY_DATA` beyond the granted credit, `COPY_DATA`/`COPY_DONE` for an
  in-flight request that is not a started `COPY_IN`, or a malformed body. The request in question is
  in flight with its own handler, so a per-request error would be a second terminal for it.
- **Silently discarded:** `COPY_DATA`/`COPY_DONE` for a `request_id` that is not in flight, or whose
  COPY has already ended — the chunks a client sent before it read an early terminal.
- **Fate.** Until `COPY_DONE`, NOTHING of the COPY can apply (PostgreSQL completes a `COPY FROM STDIN`
  only on its end-of-data): a `CANCEL`, a deadline, the session ending, a lost backend link, or a
  violation aborts it and the terminal is a KNOWN did-not-apply — `ConnectionLost` (Retryable) in
  autocommit, `TxDeadline` (Retryable, the transaction rolled back) inside one. A server error
  (a malformed row, a constraint) is reported as itself, known-fate. After `COPY_DONE` the §19.3
  write rules apply: a cancel or timeout the engine cannot confirm, or a lost link, is
  `Indeterminate` in autocommit.

**`COPY_OUT`.** Exactly a stream (§10) without a `HEAD`: `STREAM`/`COPY_DATA` frames (`STREAM` flag)
under the request's credit window and the session cap, replenished by the client's `WINDOW_UPDATE`,
then the terminal: an `ExecOk` with `affected` = rows exported, `stats.rows = affected`,
`stats.bytes` = COPY bytes sent. The engine coalesces the per-row messages the server has ALREADY sent
into frames of up to 256 KiB — it never waits to fill one — and splits a larger row across frames. Abandon with `CANCEL` and read to the terminal.
Fate as a streamed `EXEC` of the same `readonly`.

### 13.3 `CopyData` / `CopyDone`

`CopyData` is a fixarray(1) `[data: bin]` — a MessagePack `bin` (smallest width), never a `str`; a
receiver refuses anything else, trailing bytes, or a declared length the payload does not carry. The
bytes are opaque: the COPY format (text, CSV, binary) is the statement's. `CopyDone` is the empty
fixarray `[]`. Neither is an `Outcome`, and neither carries `END`.

### 13.4 COPY vector index

`copy_in_request` (autocommit; both nullables `nil`), `copy_out_request` (`readonly = true`,
`timeout_ms = 30000`, `tx_id = 42` — the populated arms), `copy_data` (300 bytes, so `bin16` is
locked rather than a `bin8` every codec agrees on, carrying COPY text-format specials and a `0xc0`
byte), and `copy_done`. The grant is a `window_update` (§4); the terminals are `ExecOk`s (§8.3).

## 14. QUEUE service messages (`ENQUEUE`/`RESERVE`/`ACK`/`RELEASE`/`EXTEND`/`SIZE`/`CLEAR`)

The QUEUE service (`SERVICE_QUEUE = 7`, M7-G1a; SPEC §24, `docs/spec/24-queue.md`) carries Ferro Queue:
PHP producers and workers move **jobs** through an operator-declared STORE (a table in a pool `ferrod`
owns). Its methods are registry `methods.queue`: `ENQUEUE = 1`, `RESERVE = 2`, `ACK = 3`, `RELEASE = 4`,
`EXTEND = 5`, `SIZE = 6`, `CLEAR = 7`. **Every method is client → engine and request-bearing**: one
request frame (flags `0`), and the request's ONE terminal on the same `QUEUE`/method header with `END`,
whose `Outcome::Ok` body is the method's response shape below. There is no streamed delivery (SPEC
§24.4). The engine routes a QUEUE method id the registry does not allocate to `Unsupported`.

**Every shape here is frozen from M7-G1a** (SPEC §24.4): a later change to any of them bumps
`protocol_version` (§8). That is why `RESERVE` already carries `liveness`, which a v1 engine refuses
when `true`.

**Opaque handles.** Every `job_id`, `new_job_id` and `token` is a msgpack **`bin`** of
`1..=QUEUE_HANDLE_MAX_BYTES` (1 024) bytes (SPEC D22 (b), D24). A client never interprets one: it
receives it and sends it back unchanged, as `bin` — a `str` would change the wire type and could fail
the engine's UTF-8 check. Each store KIND mints its own: the v1 `sql` kind writes a `job_id` as the
canonical decimal text of the row's `bigint` id (1–20 ASCII bytes) and an 8-byte token, and an engine
that cannot decode a well-formed handle answers `InvalidHandle` (below), never `Protocol`.

**The codec enforces the wire's types, widths and the shapes' bounds**: strict arity; every array
and `str`/`bin` length bounded by the bytes remaining before anything is allocated; trailing bytes
refused; a `str` must be UTF-8, except `common.traceparent`, decoded lossily as `ExecRequest` field 9
is; a handle outside `1..=QUEUE_HANDLE_MAX_BYTES`, an `ENQUEUE` with `0` or more than
`QUEUE_ENQUEUE_MAX_JOBS` (1 000) jobs, a `RESERVE` naming `0` or more than `QUEUE_RESERVE_MAX_QUEUES`
(16) queues, an `ACK` `outcome` outside `[ack_outcome]`, and a count at or above 2^63 are malformed.
Those bounds are registry keys because both RECEIVERS enforce them (the M6-F2 rule). What a value
MEANS — a payload containing U+0000, a queue name over 255 characters, an unknown store — is the
engine's to refuse (SPEC §24.4), never a decoder's. The Rust codec is hand-rolled (`ferro-proto`
`messages::queue`); the PHP one is `Ferro\Protocol\QueueCodec`, which also refuses, BEFORE writing a
byte, everything the engine's decoder would refuse.

Two shapes are shared: `common = [tx_id: u64 | nil, timeout_ms: u32 | nil, traceparent: str | nil]` (a
fixarray(3), the last element of every request) and
`stats = [queue_us: u64, exec_us: u64]` (a fixarray(2), the last element of every response; both bounded
below 2^63).

**`common.tx_id` is decoded exactly as `ExecRequest.tx_id` is** (M7-G1a review L6, which corrected an
overstatement here): the Rust decoder accepts any `u64`, while PHP's encoder refuses a value at or above
2^63 because it cannot hold one. The engine mints `tx_id`s from a counter that never approaches 2^63, so
such a value names no transaction; it is not a wire fault — since M7-G2 it is resolved like any
`tx_id` and answered `TxNotFound` (a tx-scoped `RESERVE` is refused `Unsupported` first, for good;
SPEC §24.5). Pinned by `messages::queue`'s
`a_tx_id_at_or_above_2_63_decodes_as_exec_s_does`.

### 14.1 Requests — client → server

| method | fixarray | fields |
|---|---|---|
| `ENQUEUE` | 4 | `store: str`, `jobs: array<[queue: str, payload: str, delay_s: u32]>` (1..=1000), `dedup_key: str \| nil`, `common` |
| `RESERVE` | 6 | `store: str`, `queues: array<str>` (1..=16, priority order), `max_jobs: u16`, `wait_ms: u32`, `liveness: bool`, `common` |
| `ACK`, `EXTEND` | 4 | `store: str`, `job_id: bin`, `token: bin`, `common` |
| `RELEASE` | 5 | `store: str`, `job_id: bin`, `token: bin`, `delay_s: u32`, `common` |
| `SIZE`, `CLEAR` | 3 | `store: str`, `queue: str`, `common` |

A job is a fixarray(3). `payload` is opaque to the engine (SPEC §24.2 I2) but is a `str`, so it is
UTF-8 by construction.

### 14.2 Responses — the terminal `Outcome::Ok` body — server → client

| method | fixarray | fields |
|---|---|---|
| `ENQUEUE` | 4 | `job_id: bin \| nil` (non-nil iff exactly one job), `inserted: u32`, `deduplicated: bool`, `stats` |
| `RESERVE` | 2 | `jobs: array<ReservedJob>` (possibly empty), `stats` |
| `ACK` | 2 | `outcome: u8` — `ACK_OUTCOME_ACKED` (1) or `ACK_OUTCOME_GONE` (2), registry `[ack_outcome]` — `stats` |
| `RELEASE` | 2 | `new_job_id: bin \| nil` (`nil` = gone, autocommit only), `stats` |
| `EXTEND` | 2 | `lease_deadline: i64`, `stats` |
| `SIZE` | 5 | `pending: u64`, `delayed: u64`, `reserved: u64`, `oldest_pending_at: i64 \| nil`, `stats` |
| `CLEAR` | 2 | `deleted: u64`, `stats` |

`ReservedJob` is a fixarray(7): `[job_id: bin, token: bin, attempts: u32, queue: str, payload: str,
created_at: i64, lease_deadline: i64]`. Times are Unix seconds on the database's clock (SPEC §24.3);
they are `i64` on the wire because PostgreSQL's `integer` columns are signed, and they ride §2's signed
ladder. Every `u64` here is bounded below 2^63, so PHP decodes it to a native int and REFUSES a
decimal-string one.

`SIZE.oldest_pending_at` (added in the M7-G1a review round, before any engine served SIZE) is the
smallest `available_at` among the queue's PENDING jobs, or `nil` when it has none. It is what Laravel 12's
`DatabaseQueue::creationTimeOfOldestPendingJob()` returns (checked in `illuminate/queue` v12.69.3,
which also has `pendingSize`/`delayedSize`/`reservedSize`, matching the three counts); adding it after
the shapes froze would have needed a registry change, so it is in the frozen shape now.

### 14.3 The codes

QUEUE terminals use §5's `ErrorPayload` with `sqlstate`, `errno` and `detail` `nil` on the engine's own
refusals (a classified statement error carries the backend's, as on EXEC). The three QUEUE codes, all
NonRetryable known fates:

- **`LeaseLost` (`0x300F`)** — a fenced verb's token names no current reservation; the verb did
  nothing (SPEC §24.4, §24.6).
- **`PoolMismatch` (`0x3010`)** — a tx-scoped verb named a store whose pool is not the transaction's;
  refused before any statement (SPEC §24.5).
- **`InvalidHandle` (`0x3011`, allocated at M7-G1a)** — a `job_id` or `token` that is well-formed on the
  wire but that the store's kind cannot decode; refused before any statement (SPEC §24.3 prerequisite
  (c)). Not `Protocol` (the frame is well-formed) and not `LeaseLost` (a tier treats that as "did
  nothing, done", which would hide what is always a client defect).

An unknown store reuses `Unsupported`, as an unknown pool does. **No `protocol_version` bump** (the §11
and §12 precedent): these are new messages on a new service, and the registry hash, which moved with
them, refuses a skewed pair at the handshake.

### 14.4 QUEUE vector index

Every `queue_*` vector, each locking a handle position at its `sql`-kind size and at the registry
maximum (asserted, from the decoded vectors, by
`golden_vectors.rs::every_queue_handle_position_is_locked_at_the_sql_size_and_at_the_maximum`):
`queue_enqueue_request` (one job, a dedup key, every `common` field set — a `tx_id` past u32),
`queue_enqueue_request_batch` (two jobs, a `delay_s` past u16, a multi-byte payload, every nullable
`nil`), `queue_enqueue_response` (the largest canonical id; a dedup replay, so `inserted` 0),
`queue_enqueue_response_max`,
`queue_enqueue_response_batch` (`job_id` `nil`), `queue_enqueue_request_max_jobs` (exactly 1 000 jobs),
`queue_reserve_request_max_queues` (exactly 16 queues), `queue_reserve_request` (two queues, `max_jobs` past
u8, `wait_ms` past u16), `queue_reserve_response` (one job at the `sql` sizes and one at 1 024 bytes for
both handles, a negative `created_at`), `queue_reserve_response_empty`, `queue_ack_request`,
`queue_ack_request_max`, `queue_ack_response`, `queue_ack_response_gone`, `queue_release_request`,
`queue_release_request_max`, `queue_release_response`, `queue_release_response_max`,
`queue_release_response_gone`, `queue_extend_request`, `queue_extend_request_max`,
`queue_extend_response`, `queue_size_request`, `queue_size_response` (a count past u32 and an
`oldest_pending_at`), `queue_size_response_empty` (`oldest_pending_at` `nil`),
`queue_clear_request`, `queue_clear_response`; and the three error terminals `error_lease_lost`,
`error_pool_mismatch`, `error_invalid_handle`, each on its request's own `QUEUE`/method header. A
`1 024`-byte handle is not UTF-8 and holds bytes in the `0xc0` range, so neither a `str` read nor a
`nil` peek could pass it by accident.

**Refusal vectors** (`/proto/vectors/refusal/queue_*.json`, new at M7-G1a: `{name, header, field, len,
frame_hex}`): a frame with a valid header whose payload is well-formed EXCEPT the one `field`, which
is out of its bound — every handle position at `0` and `QUEUE_HANDLE_MAX_BYTES + 1` bytes, and the job
and queue counts at `0` and their maximum + 1. Both codecs must refuse each FOR ITS OWN REASON (the
error names the field and the length): `golden_vectors.rs::queue_refusal_vectors_are_refused_for_their_own_reason`
and PHP's `QueueRefusalVectorTest`, each also asserting the set is complete.

The `queue_` vectors are byte-locked in PHP by the `queueVectors()` prefix provider, which picks the
codec from the HEADER (method and `END`), never the name; the three error vectors by a name-keyed test.
In Rust each is decoded and compared against its vector's NAMED fields.
