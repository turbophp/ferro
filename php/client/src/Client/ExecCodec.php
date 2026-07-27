<?php // /php/client/src/Client/ExecCodec.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\SqlValueCodec;

/**
 * The shared EXEC encode/decode/hydration engine used by BOTH the autocommit {@see Connection} and
 * the tx-scoped {@see TxHandle}, so a `tx_id` is the ONLY difference between an autocommit and an
 * in-transaction statement on the wire (and the value-policy + memoized DTO hydration are identical).
 *
 * Bind: positional PHP scalars → the canonical `{tag, data}` shape (M0 binds null/bool/int/float and
 * strings as TEXT). Decode: an `Ok` {@see ExecOk} body → column names + value-policy-decoded rows.
 * A garbled `Ok` body ({@see CodecException}) is wrapped as a {@see ProtocolException} so a codec
 * fault never escapes the {@see \Ferro\Client\Error\FerroException} contract as a raw runtime error.
 */
final class ExecCodec
{
    /** ExecRequest.fetch modes (PROTOCOL.md §8.1): 0 = rows, 1 = none (affected only), 2 = stream. */
    public const FETCH_ROWS = 0;
    public const FETCH_NONE = 1;

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
                $cells = [];
                foreach (SqlValueCodec::listOf($row) as $cell) {
                    if (!is_array($cell)) {
                        throw new CodecException('ExecOk: bad cell');
                    }
                    $cells[] = $this->values->decode(
                        SqlValueCodec::toInt($cell['tag'] ?? -1),
                        $cell['data'] ?? null,
                    );
                }
                $rows[] = $cells;
            }

            return ['cols' => $cols, 'rows' => $rows, 'affected' => SqlValueCodec::toInt($ok['affected'] ?? 0)];
        } catch (CodecException $e) {
            // A terminal that read fully but failed to parse is a protocol fault, not a fate signal.
            throw new ProtocolException('failed to decode SQL terminal: ' . $e->getMessage(), 0, $e);
        }
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
            $out[] = self::bindOne($v);
        }
        return $out;
    }

    /** @return array{tag:int,data:mixed} */
    private static function bindOne(mixed $v): array
    {
        return match (true) {
            $v === null   => ['tag' => C::TAG_NULL, 'data' => null],
            is_bool($v)   => ['tag' => C::TAG_BOOL, 'data' => $v],
            is_int($v)    => ['tag' => C::TAG_I64,  'data' => $v],
            is_float($v)  => ['tag' => C::TAG_F64,  'data' => $v],
            is_string($v) => ['tag' => C::TAG_TEXT, 'data' => $v],
            default => throw new ProtocolException(sprintf(
                'unsupported bind parameter type %s (M0 binds null/bool/int/float/string)',
                get_debug_type($v),
            )),
        };
    }
}
