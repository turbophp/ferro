<?php // /php/client/src/Client/ExecCodec.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
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
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int}
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

            return ['cols' => $cols, 'rows' => $rows, 'affected' => SqlValueCodec::toInt($ok['affected'] ?? 0)];
        } catch (CodecException $e) {
            // A terminal that read fully but failed to parse is a protocol fault, not a fate signal.
            throw new ProtocolException('failed to decode SQL terminal: ' . $e->getMessage(), 0, $e);
        }
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
     * @param array{cols: list<string>, rows: list<list<mixed>>, affected: int} $res
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
     * @template T of object
     * @param class-string<T> $class
     * @param list<string> $cols
     * @param list<mixed> $row
     * @return T
     */
    public function hydrateDto(string $class, array $cols, array $row): object
    {
        $args = $this->plans->planFor($class, $cols)->argsFor($row);
        return (new \ReflectionClass($class))->newInstanceArgs($args);
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
            // NaiveTimestamp EXTENDS DateTimeImmutable — it MUST stay ahead of the arm below (F14).
            $v instanceof NaiveTimestamp     => ['tag' => C::TAG_TIMESTAMP,   'data' => $this->naiveTimestampText($v)],
            $v instanceof \DateTimeInterface => ['tag' => C::TAG_TIMESTAMPTZ, 'data' => self::utcInstantText($v)],
            default => throw new ProtocolException(sprintf(
                'unsupported bind parameter type %s (bind null/bool/int/float/string, a '
                . '\DateTimeInterface, or one of the SPEC §9 value objects: Ferro\{Decimal, Date, '
                . 'Time, Uuid, Json, U64, NaiveTimestamp})',
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
