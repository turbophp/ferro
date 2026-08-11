# Follow-up: an `I64` at or above 2^32 is unreadable by `php/client`, on every value policy

**Found:** first as M1-S8b Task 7 finding F2 (journalled, not fixed — the task was the bind
direction). **Independently re-measured at the M1-S8b Task 14 acceptance gate**, twice: by the
upstream `Types\BigIntTypeTest::testSelectBigInt` on **all three backends**, and by a standalone
probe through `Ferro\Client\Connection::scalar()` with no DBAL involved at all.
**Belongs to:** `php/client` — `Client/Value/CanonicalText::requireInt` and the two value policies.
**Severity:** HIGH, and higher than a type-coverage gap: it is a plain `bigint` READ.
**Blocks:** any `bigint` primary key past 4 294 967 295, every epoch-milliseconds column, every
counter past 2^32.

## Measured, today, on PG 17 through the native client (`SELECT <n>::bigint`)

```
4294967295             -> int(4294967295)
4294967296             -> Ferro\Client\Error\ProtocolException: value tag 2: expected a int payload, got string
1755000000000          -> Ferro\Client\Error\ProtocolException: value tag 2: expected a int payload, got string
9223372036854775807    -> Ferro\Client\Error\ProtocolException: value tag 2: expected a int payload, got string
```

Every one of those is comfortably inside PHP's own `int` range (`PHP_INT_MAX` is
9 223 372 036 854 775 807). Nothing about the value is unrepresentable; the decode path simply
refuses it.

## Mechanism (read from the source, not inferred)

1. The engine encodes `TAG_I64` with `rmp`'s canonical `write_sint`
   (`engine/crates/ferro-proto/src/value.rs:71`), which narrows and — for a **non-negative** value
   too large for the small-int ladder — emits the **unsigned** `0xcf` uint64 marker.
2. `PurePacker` decodes `0xcf` via `be($buf, $offset, 8, signed: false)`
   (`php/client/src/Protocol/Msgpack/PurePacker.php:95`), and that branch deliberately
   `return self::be64ToDec($slice);` — *"unsigned 64: return decimal string to preserve >
   PHP_INT_MAX"* (`:153-165`). It returns a **decimal string** for every 8-byte unsigned, including
   the ones that fit an `int` perfectly.
3. `CanonicalText::requireInt` (`:72-78`) accepts `is_int()` only and throws otherwise.
4. `ext-msgpack` cannot mask it: `PackerFactory::forDecode()` returns `PurePacker`
   **unconditionally** (`:16-19`), because `ExtPacker` ignores the caller's `$offset`.

So the boundary is exactly where the marker changes: 2^32 − 1 fits `0xce` (uint32) and decodes to an
`int`; 2^32 needs `0xcf` and becomes a string.

## The fix, and the trap in it

The obvious repair is for `be()` to return an `int` when the unsigned 64-bit value is `<= PHP_INT_MAX`
and the decimal string only above it. That is almost certainly right, but it is the **shared codec**,
so a fix has to keep two other things true and prove it:

- **`TAG_U64` must still be able to carry values above `PHP_INT_MAX`** as a string — that is the
  entire reason the string branch exists (`Ferro\U64` is a §9 value object).
- **The two packers must stay conformant.** There is still no ext-vs-pure conformance test (a
  standing S8a carry); a change to `be()` is a good reason to add one rather than a reason to skip
  it.

Coverage a fix must bring: a golden vector at the 2^32 boundary and at `PHP_INT_MAX`, a unit test per
value policy, and a live read on all three backends. The bind direction already refuses an
out-of-range integer loudly (`ParameterBinder::asInt`), so only the READ path moves.

## How to reproduce

```bash
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh --filter 'testSelectBigInt'
```

or, with no DBAL, point a `ferrod` at the shared PG container and call
`Ferro\Ferro::connect($sock, 'default')->scalar('SELECT 4294967296::bigint')`.
