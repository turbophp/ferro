<?php // /php/client/src/Client/ExecCodec.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\HydrationException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Bytes;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\SqlValueCodec;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;

/**
 * The shared EXEC encode/decode/hydration engine used by BOTH the autocommit {@see Connection} and
 * the tx-scoped {@see TxHandle}, so a `tx_id` is the ONLY difference between an autocommit and an
 * in-transaction statement on the wire (and the value-policy + memoized DTO hydration are identical).
 *
 * Bind: positional PHP values → the canonical `{tag, data}` shape. As of M1-S7 that is the M0 scalars
 * PLUS the SPEC §9 value objects and `\DateTimeInterface` — see {@see bindOne}, which is the ONE
 * place a PHP type becomes a canonical tag (the {@see ValuePolicy} seam is decode-only). Decode: an
 * `Ok` {@see ExecOk} body → column names + value-policy-decoded rows. A garbled `Ok` body
 * ({@see CodecException}) is wrapped as a {@see ProtocolException} so a codec fault never escapes
 * the {@see \Ferro\Client\Error\FerroException} contract as a raw runtime error.
 */
final class ExecCodec
{
    /**
     * ExecRequest.fetch modes (PROTOCOL.md §8.1): 0 = rows, 1 = none (affected only), 2 = stream
     * (the §7.2 windowed HEAD/DATA/END producer, M1-S5). `FETCH_STREAM` is a valid, wire-accepted
     * `fetch` value as of M1-S5 Task 1 (the codec never restricted it); the engine's `Unsupported`
     * rejection of it is unrelated client-side surface and lifts in a later S5 task.
     */
    public const FETCH_ROWS = 0;
    public const FETCH_NONE = 1;
    public const FETCH_STREAM = 2;

    public function __construct(
        private readonly ValuePolicy $values,
        private readonly PlanCache $plans,
        private readonly PackerInterface $encodePacker,
        private readonly PackerInterface $decodePacker,
    ) {}

    public function plans(): PlanCache { return $this->plans; }

    /**
     * Build one EXEC payload. `$txId = null` is the autocommit path; a non-null `$txId` scopes the
     * statement to that transaction's actor on the engine.
     *
     * @param list<mixed> $params
     */
    public function encode(string $pool, string $sql, array $params, bool $readonly, int $fetch, ?int $txId): string
    {
        return ExecRequest::encode([
            'pool' => $pool,
            'sql' => $sql,
            'query_id' => null,
            'params' => $this->bindParams($params),
            'timeout_ms' => null,
            'readonly' => $readonly,
            'fetch' => $fetch,
            'tx_id' => $txId,
        ], $this->encodePacker);
    }

    /**
     * Decode an `Ok` {@see ExecOk} body into column names + value-policy-decoded rows.
     *
     * `last_insert_id` is returned RAW (`int|string|null`), deliberately NOT through the
     * {@see \Ferro\Client\Value\ValuePolicy}: it is a scalar terminal field, not a column, and the
     * DBAL contract for `lastInsertId()` is `int|string`. A key that needs a msgpack **uint64**
     * (>= 2^32) arrives as its canonical decimal STRING, not an int — `PurePacker`, the
     * spec-authoritative decoder `PackerFactory::forDecode()` always returns, never narrows a `0xcf`
     * payload (measured: 2^32-1 -> int, 2^32 -> `'4294967296'`). That is why `int|string` is the
     * honest return type, and why the turnover is at 2^32 rather than at `PHP_INT_MAX`; the engine's
     * `I64`/`U64` tag choice (`ferrod`'s `last_insert_id_value`) is a separate, higher boundary.
     *
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    public function decode(Outcome $outcome): array
    {
        try {
            $off = 0;
            $w = $this->decodePacker->unpack($outcome->body(), $off);
            if (!is_array($w)) {
                throw new CodecException('ExecOk terminal body is not an array');
            }
            $ok = ExecOk::mapFromWire(array_values($w));

            $cols = [];
            foreach (SqlValueCodec::listOf($ok['cols']) as $c) {
                if (!is_array($c)) {
                    throw new CodecException('ExecOk: bad ColMeta');
                }
                $cols[] = SqlValueCodec::toStr($c['name'] ?? '');
            }

            $rows = [];
            foreach (SqlValueCodec::listOf($ok['rows']) as $row) {
                $rows[] = $this->decodeRow(SqlValueCodec::listOf($row));
            }

            $lastId = $ok['last_insert_id'] ?? null;

            return [
                'cols' => $cols,
                'rows' => $rows,
                'affected' => SqlValueCodec::toInt($ok['affected'] ?? 0),
                'last_insert_id' => self::rawLastInsertId(is_array($lastId) ? $lastId : null),
            ];
        } catch (CodecException $e) {
            // A terminal that read fully but failed to parse is a protocol fault, not a fate signal.
            throw new ProtocolException('failed to decode SQL terminal: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Narrow the ALREADY-DECODED `last_insert_id` cell to the raw scalar.
     *
     * **It must NOT call {@see SqlValueCodec::fromWire} again.** {@see ExecOk::mapFromWire} has
     * already run it (`ExecOk.php:57`), so what arrives here is a decoded `['tag' => int, 'data' =>
     * mixed]` cell, not a wire `[tag, payload]` pair. Re-decoding happens to be idempotent for an
     * `I64`, which is why the mistake is invisible in the common case — but it is a real fault for a
     * `TAG_BYTES` cell (whose `data` is already a `list<int>`, not the wire string), and it makes
     * this method's contract a lie about what it receives.
     *
     * `null` (no id) stays null; otherwise the payload is an `int` (anything the decoder narrowed)
     * or a canonical decimal `string` (any key in the msgpack uint64 band, >= 2^32 — see
     * {@see decode}). Deliberately no coercion: a malformed payload is a wire fault, not a
     * silently-zeroed key.
     *
     * @param array<array-key, mixed>|null $cell the decoded `{tag, data}` cell from
     *   {@see ExecOk::mapFromWire}, or null when the terminal carried a bare nil
     */
    private static function rawLastInsertId(?array $cell): int|string|null
    {
        if ($cell === null) {
            return null;
        }
        $data = $cell['data'] ?? null;
        if ($data === null || is_int($data) || is_string($data)) {
            return $data;
        }
        throw new CodecException(
            'ExecOk.last_insert_id: expected an int or decimal string, got ' . get_debug_type($data),
        );
    }

    /**
     * Value-policy-decode one raw wire row (a list of `{tag, data}` cells — the exact shape both
     * `ExecOk.rows` and the streaming `StreamData.rows` use, see {@see \Ferro\Protocol\SqlValueCodec::fromWire}).
     * Shared by the buffered {@see decode} and the streaming path ({@see \Ferro\Client\Connection::stream}) so
     * a cell is decoded identically regardless of which wire channel it arrived on.
     *
     * @param list<mixed> $rawCells
     * @return list<mixed>
     */
    public function decodeRow(array $rawCells): array
    {
        $cells = [];
        foreach ($rawCells as $cell) {
            if (!is_array($cell)) {
                throw new ProtocolException('bad TypedValue cell (not an array)');
            }
            $cells[] = $this->values->decode(
                SqlValueCodec::toInt($cell['tag'] ?? -1),
                $cell['data'] ?? null,
            );
        }
        return $cells;
    }

    /**
     * @param array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id?: int|string|null} $res
     * @return list<array<string,mixed>>
     */
    public function assocRows(array $res): array
    {
        $out = [];
        foreach ($res['rows'] as $row) {
            $out[] = $this->assocRow($res['cols'], $row);
        }
        return $out;
    }

    /**
     * @param list<string> $cols
     * @param list<mixed> $row
     * @return array<string,mixed>
     */
    public function assocRow(array $cols, array $row): array
    {
        if (count($cols) !== count($row)) {
            throw new ProtocolException(sprintf(
                'ExecOk row arity %d does not match column count %d',
                count($row),
                count($cols),
            ));
        }
        /** @var array<string,mixed> $assoc */
        $assoc = array_combine($cols, $row);
        return $assoc;
    }

    /**
     * Hydrate one row into `$class` via its constructor.
     *
     * **The value objects made this fallible (hazard 35).** Through M1-S6 every decoded cell was a
     * PHP scalar, so `newInstanceArgs` only ever failed on a genuinely broken DTO. As of M1-S7 a
     * `DECIMAL` column arrives as a {@see \Ferro\Decimal} and a `TIMESTAMP` as a
     * {@see \Ferro\NaiveTimestamp}, so a DTO whose promoted parameter is typed `string` (or `int`,
     * or a different value object) now throws a bare `\TypeError` — which would ESCAPE the
     * {@see \Ferro\Client\Error\FerroException} contract the whole client surface is caught by, and
     * land in application code as an engine-internals leak. It is translated here into
     * {@see HydrationException} (the same class a missing column raises), naming the DTO, the
     * column, the value's actual type and the SPEC §9.1 knob that would change it.
     *
     * `\ArgumentCountError` extends `\TypeError`, so the same arm covers a plan/row arity fault
     * *against that constructor*.
     *
     * **The wrap is SCOPED to the constructor's OWN argument binding** ({@see isConstructorArgError}).
     * A `\TypeError`/`\ArgumentCountError` raised from INSIDE the constructor *body* is an ordinary
     * application bug that has nothing to do with §9.1 type policy, and re-labelling it attached
     * confidently wrong advice ("type the DTO property to match") to, say, a bad internal helper call
     * — actively misleading, and it hid the real bug behind `getPrevious()`. Those rethrow UNCHANGED.
     *
     * @template T of object
     * @param class-string<T> $class
     * @param list<string> $cols
     * @param list<mixed> $row
     * @return T
     * @throws HydrationException on a constructor ARGUMENT fault (the §9.1 boundary).
     * @throws \TypeError unchanged, when the fault came from inside the constructor body.
     */
    public function hydrateDto(string $class, array $cols, array $row): object
    {
        $args = $this->plans->planFor($class, $cols)->argsFor($row);
        try {
            return (new \ReflectionClass($class))->newInstanceArgs($args);
        } catch (\TypeError $e) {
            if (!self::isConstructorArgError($class, $e)) {
                throw $e;
            }
            throw new HydrationException(sprintf(
                'cannot hydrate %s: %s. The row supplied [%s] — a SPEC §9 canonical column hydrates '
                . 'to its value object (Ferro\{Decimal, Date, Time, Uuid, Json, U64, '
                . 'NaiveTimestamp} or a \DateTimeImmutable), so type the DTO property to match, or '
                . 'decode with a §9.1 string policy (decimal/uuid/u64_overflow = "string") or the '
                . 'RawStringValuePolicy.',
                $class,
                $e->getMessage(),
                implode(', ', array_map(
                    static fn (string $c, mixed $v): string => $c . ': ' . get_debug_type($v),
                    $cols,
                    $row,
                )),
            ), 0, $e);
        }
    }

    /**
     * Did `$e` come from binding the DTO constructor's OWN arguments, or from executing its body?
     *
     * PHP names the failing callee in both message shapes, and only those two shapes are raised at
     * the argument-binding boundary (measured on PHP 8.4):
     *
     *   - `Acme\Dto::__construct(): Argument #1 ($x) must be of type string, Ferro\Decimal given`
     *   - `Too few arguments to function Acme\Dto::__construct(), 1 passed and exactly 2 expected`
     *
     * A fault raised INSIDE the body names the inner callee instead (`needsTwo(): Argument #2 …`,
     * `Too few arguments to function needsTwo(), …`), so the class-qualified prefix separates them
     * without touching stack-trace internals. The prefix is built from the constructor's DECLARING
     * class, not `$class`, because an inherited constructor is reported under the parent's name.
     *
     * @param class-string $class
     */
    private static function isConstructorArgError(string $class, \TypeError $e): bool
    {
        $owner = (new \ReflectionClass($class))->getConstructor()?->getDeclaringClass()->getName() ?? $class;
        $msg = $e->getMessage();

        return str_starts_with($msg, $owner . '::__construct(): Argument #')
            || str_starts_with($msg, 'Too few arguments to function ' . $owner . '::__construct(),');
    }

    /**
     * @param list<mixed> $params
     * @return list<array{tag:int,data:mixed}>
     */
    private function bindParams(array $params): array
    {
        $out = [];
        foreach ($params as $v) {
            $out[] = $this->bindOne($v);
        }
        return $out;
    }

    /**
     * Map ONE PHP bind parameter to its canonical `{tag, data}` cell — the single chokepoint every
     * positional parameter passes through, and the write-side counterpart of the decode-only
     * {@see ValuePolicy} seam (hazard 31: without these arms the M1-S7 type slice is read-only and a
     * DBAL suite, which binds `DateTime`s and decimals constantly, cannot write at all).
     *
     * **Arm order is load-bearing.** {@see \Ferro\NaiveTimestamp} EXTENDS `\DateTimeImmutable`, so it
     * MUST be tested BEFORE `\DateTimeInterface`. Reversed, every value read from a naive column and
     * written straight back would bind as `TIMESTAMPTZ` and shift by whatever zone the object carries
     * — silently, with no error anywhere (F14). {@see \Ferro\Tests\Unit\BindTest} fails if the arms
     * are swapped.
     *
     * **The naive/instant rule (SPEC §9):** a plain `\DateTimeInterface` is an INSTANT and binds
     * `TIMESTAMPTZ`, normalized to UTC; a `NaiveTimestamp` is a wall clock and binds `TIMESTAMP`, its
     * text emitted verbatim with no zone conversion. That pairing is exactly what makes a read →
     * write-back round trip byte-stable.
     *
     * **A `U64` rides its canonical decimal string**, never a PHP `int`: `packInt` physically cannot
     * emit a value above `PHP_INT_MAX` and an `(int)` cast SATURATES there (hazard 29). The string
     * narrows canonically in {@see \Ferro\Protocol\Msgpack\PurePacker::packUint}, so a small `U64`
     * is byte-identical to the same number bound as an int.
     *
     * **Sentinels (`infinity`, `-infinity`, `0000-00-00 00:00:00`) — the pinned C20 rule.** A `DATE`
     * sentinel round-trips with its TAG intact, because {@see \Ferro\Date} carries it. A
     * `TIMESTAMP`/`TIMESTAMPTZ` sentinel is not a wall-clock value, so the policy hands it back as
     * canonical TEXT (throwing would make a legal column unreadable) and it re-binds here as
     * `TAG_TEXT` with the bytes VERBATIM — never parsed, never rewritten. The tag widening is
     * deliberate: a bare PHP string's CONTENTS are never sniffed for a temporal tag, because
     * `'infinity'` is an ordinary value in a `varchar` column and retagging it would be the silent
     * miscast SPEC §9.1 forbids. Downstream the consequence is loud, not silent — PG's `bind::accepts`
     * refuses a `TEXT` param against a `timestamp` slot as a §19.3 known-fate, pre-send rejection.
     * A tag-preserving sentinel write shares the `TAG_BYTES` gap's root cause (every PHP string binds
     * `TEXT`) and its S8 fix shape: an explicit bind marker (SPEC §22.2).
     *
     * Non-static (it was `private static` through M1-S6) so the `\DateTimeInterface` arms can reach
     * the §9.1 policy through `$this->values` — see {@see naiveTimestampText}.
     *
     * @internal the wire cell shape is not part of the public client API.
     * @return array{tag:int,data:mixed}
     * @throws ProtocolException for a PHP type that has no canonical tag.
     * @throws TypePolicyException when a §9.1 knob refuses the value.
     */
    public function bindOne(mixed $v): array
    {
        return match (true) {
            $v === null   => ['tag' => C::TAG_NULL, 'data' => null],
            is_bool($v)   => ['tag' => C::TAG_BOOL, 'data' => $v],
            is_int($v)    => ['tag' => C::TAG_I64,  'data' => $v],
            is_float($v)  => ['tag' => C::TAG_F64,  'data' => $v],
            is_string($v) => ['tag' => C::TAG_TEXT, 'data' => $v],
            // --- the M1-S7 §9 value objects: their canonical text, byte-for-byte as it was read ---
            $v instanceof Decimal => ['tag' => C::TAG_DECIMAL, 'data' => $v->value],
            $v instanceof Date    => ['tag' => C::TAG_DATE,    'data' => $v->value],
            $v instanceof Time    => ['tag' => C::TAG_TIME,    'data' => $v->value],
            $v instanceof Uuid    => ['tag' => C::TAG_UUID,    'data' => $v->value],
            $v instanceof Json    => ['tag' => C::TAG_JSON,    'data' => $v->raw],
            $v instanceof U64     => ['tag' => C::TAG_U64,     'data' => $v->value],
            // The explicit BINARY marker (SPEC §22.2 (k)(4)). `TAG_BYTES` rides the msgpack `bin`
            // family, so a non-UTF-8 payload survives — unlike `TAG_TEXT`, whose `str` family is
            // rejected by the engine's reader as `invalid utf8` before the bind pre-flight.
            $v instanceof Bytes   => ['tag' => C::TAG_BYTES,   'data' => $v->value],
            // NaiveTimestamp EXTENDS DateTimeImmutable — it MUST stay ahead of the arm below (F14).
            $v instanceof NaiveTimestamp     => ['tag' => C::TAG_TIMESTAMP,   'data' => $this->naiveTimestampText($v)],
            $v instanceof \DateTimeInterface => ['tag' => C::TAG_TIMESTAMPTZ, 'data' => self::utcInstantText($v)],
            default => throw new ProtocolException(sprintf(
                'unsupported bind parameter type %s (bind null/bool/int/float/string, a '
                . '\DateTimeInterface, or one of the SPEC §9 value objects: Ferro\{Decimal, Date, '
                . 'Time, Uuid, Json, U64, NaiveTimestamp, Bytes}). A binary payload or a stream '
                . 'binds through Ferro\Bytes / Ferro\Bytes::fromStream($h) — there is deliberately '
                . 'no implicit resource arm, because reading a stream into memory is the caller\'s '
                . 'decision to make.',
                get_debug_type($v),
            )),
        };
    }

    /**
     * A naive wall clock's canonical `YYYY-MM-DD HH:MM:SS[.ffffff]` text — NO zone conversion and no
     * suffix, ever (`/proto/PROTOCOL.md` §3.2). The rendering itself lives on the value object, so
     * the read and write sides cannot drift apart.
     *
     * `naive_datetime_zone=error` is an operator declaring that this application does not deal in
     * naive timestamps at all; {@see \Ferro\Client\Value\M1ValuePolicy} refuses to READ one, and the
     * write is refused symmetrically here — permitting the write would let the exact value the knob
     * exists to eliminate back into the schema. The scope is the one pinned in
     * {@see TypePolicyOptions::refusesNaiveTimestamp} (`TAG_TIMESTAMP` alone), and it stays a
     * {@see TypePolicyException} — a configuration refusal, not the wire fault a `ProtocolException`
     * reports — so S8's DBAL `ExceptionConverter` never misreports it as a driver protocol failure.
     * A policy without §9.1 knobs (the `RawStringValuePolicy` DBAL hand-off, or M0's) has nothing to
     * refuse with and binds unconditionally.
     *
     * @throws TypePolicyException
     */
    private function naiveTimestampText(NaiveTimestamp $v): string
    {
        $options = $this->values instanceof M1ValuePolicy ? $this->values->options() : null;
        if ($options !== null && $options->refusesNaiveTimestamp(C::TAG_TIMESTAMP)) {
            throw new TypePolicyException(
                'naive_datetime_zone=error refuses to BIND a naive TIMESTAMP: the wire carries no '
                . 'zone for it, so the instant this value lands on depends on the column\'s server '
                . 'session zone. Use naive_datetime_zone=utc (the default), or bind a plain '
                . '\DateTimeImmutable to write an explicit UTC instant (SPEC §9.1).',
            );
        }
        return $v->toCanonicalText();
    }

    /**
     * An INSTANT's canonical RFC3339 text, always normalized to UTC and always the literal `Z`
     * (`/proto/PROTOCOL.md` §3.2). Built through `createFromInterface` so a MUTABLE `\DateTime`
     * handed in by the caller is copied rather than re-zoned in place.
     *
     * The §3.2 fraction rule: NO `.ffffff` group when the sub-second part is zero, otherwise exactly
     * six digits — never a trailing-zero-trimmed variant, so the payload stays byte-stable against
     * the golden vectors.
     */
    private static function utcInstantText(\DateTimeInterface $v): string
    {
        $utc = \DateTimeImmutable::createFromInterface($v)->setTimezone(new \DateTimeZone('UTC'));
        return $utc->format('u') === '000000'
            ? $utc->format('Y-m-d\TH:i:s') . 'Z'
            : $utc->format('Y-m-d\TH:i:s.u') . 'Z';
    }
}
