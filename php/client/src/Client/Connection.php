<?php // /php/client/src/Client/Connection.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\SqlValueCodec;

/**
 * The query surface a PHP app actually calls, on top of a {@see Session} (the M0 "pool" is a single
 * connection). `query`/`queryOne`/`scalar`/`rows` are READS and `exec` is the write path.
 *
 * **§19.3-CRITICAL — reads declare `readonly=true`.** The engine gates the `Indeterminate` split on
 * the client-declared `readonly` flag ALONE (no SQL inference): a statement `sent && !readonly` whose
 * connection dies mid-flight is surfaced `WriteUnconfirmed{Indeterminate}` (which the client NEVER
 * retries). So every read here sends `readonly=true` — a read has no write-fate; a lost read is
 * `ConnectionLost{Retryable}`, safely re-issuable. Only {@see exec} defaults `readonly=false`, and a
 * caller must opt a read-only `exec` into `readonly=true` explicitly.
 *
 * Each call builds an {@see ExecRequest}, sends it through {@see Session::sendRequest}, and on a
 * non-`Ok` {@see Outcome} throws the {@see ErrorMapper}-classified exception; on `Ok` it decodes the
 * {@see ExecOk} body and hydrates rows via the {@see ValuePolicy}. A mid-stream {@see CodecException}
 * (which lives OUTSIDE the {@see \Ferro\Client\Error\FerroException} tree) is wrapped as a
 * {@see ProtocolException} so a codec fault never escapes the client contract.
 */
final class Connection
{
    /** ExecRequest.fetch modes (PROTOCOL.md §8.1): 0 = rows, 1 = none (affected only), 2 = stream
     *  (reserved). Not emitted as `FETCH_*` in the generated registry, so mirrored here exactly as
     *  the S5 EXEC codec / live harness already spell them. */
    private const FETCH_ROWS = 0;
    private const FETCH_NONE = 1;

    private readonly ValuePolicy $values;
    private readonly PlanCache $plans;
    private readonly PackerInterface $encodePacker;
    private readonly PackerInterface $decodePacker;

    public function __construct(
        private readonly Session $session,
        private readonly string $pool = 'default',
        ?ValuePolicy $values = null,
        ?PlanCache $plans = null,
        ?PackerInterface $encodePacker = null,
        ?PackerInterface $decodePacker = null,
    ) {
        $this->values = $values ?? new M0ValuePolicy();
        $this->plans = $plans ?? new PlanCache();
        $this->encodePacker = $encodePacker ?? PackerFactory::forEncode();
        $this->decodePacker = $decodePacker ?? PackerFactory::forDecode();
    }

    /** The underlying session (the Task-4 reconnect loop drives it). */
    public function session(): Session { return $this->session; }

    /**
     * Execute a statement. Defaults to the WRITE fate (`readonly=false`, `fetch=none`) — a caller
     * running a read-only statement through `exec` must pass `readonly=true` so a lost connection is
     * classified Retryable, not Indeterminate. Returns the affected-row count.
     *
     * @param list<mixed> $params positional bind values (`?` → `$n`).
     */
    public function exec(string $sql, array $params = [], bool $readonly = false): int
    {
        return $this->runExec($sql, $params, $readonly, self::FETCH_NONE)['affected'];
    }

    /**
     * Run a read and return every row. Assoc `array<string,mixed>` by default; pass a `final
     * readonly` DTO `class-string` to hydrate instead (snake_case column → camelCase param).
     *
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? list<array<string,mixed>> : list<T>)
     */
    public function query(string $sql, array $params = [], ?string $dto = null): array
    {
        $res = $this->runExec($sql, $params, true, self::FETCH_ROWS);
        if ($dto === null) {
            return $this->assocRows($res['cols'], $res['rows']);
        }
        $out = [];
        foreach ($res['rows'] as $row) {
            $out[] = $this->hydrateDto($dto, $res['cols'], $row);
        }
        return $out;
    }

    /**
     * Run a read and return the FIRST row, or null if the result is empty. Assoc by default; pass a
     * DTO `class-string` to hydrate the row into it.
     *
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? array<string,mixed>|null : T|null)
     */
    public function queryOne(string $sql, array $params = [], ?string $dto = null): array|object|null
    {
        $res = $this->runExec($sql, $params, true, self::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        if ($firstRow === null) {
            return null;
        }
        if ($dto === null) {
            return $this->assocRow($res['cols'], $firstRow);
        }
        return $this->hydrateDto($dto, $res['cols'], $firstRow);
    }

    /**
     * Run a read and return the first column of the first row (a single scalar), or null if the
     * result is empty.
     *
     * @param list<mixed> $params
     */
    public function scalar(string $sql, array $params = []): mixed
    {
        $res = $this->runExec($sql, $params, true, self::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        if ($firstRow === null) {
            return null;
        }
        return $firstRow[0] ?? null;
    }

    /**
     * Alias of {@see query} without DTO hydration: all rows as assoc arrays.
     *
     * @param list<mixed> $params
     * @return list<array<string,mixed>>
     */
    public function rows(string $sql, array $params = []): array
    {
        $res = $this->runExec($sql, $params, true, self::FETCH_ROWS);
        return $this->assocRows($res['cols'], $res['rows']);
    }

    /**
     * Build + send one EXEC and return the decoded, value-policy-applied result. Non-`Ok` outcomes
     * throw the mapped exception; a mid-stream codec fault is wrapped so it stays inside the
     * FerroException contract.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int}
     */
    private function runExec(string $sql, array $params, bool $readonly, int $fetch): array
    {
        $payload = ExecRequest::encode([
            'pool' => $this->pool,
            'sql' => $sql,
            'query_id' => null,
            'params' => $this->bindParams($params),
            'timeout_ms' => null,
            'readonly' => $readonly,
            'fetch' => $fetch,
            'tx_id' => null,
        ], $this->encodePacker);

        try {
            $outcome = $this->session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
            if (!$outcome->isOk()) {
                // A FerroException (never a CodecException), so it escapes the catch below and
                // propagates with its three-branch fate intact.
                throw ErrorMapper::fromOutcome($outcome);
            }
            return $this->decodeExecOk($outcome);
        } catch (CodecException $e) {
            // CARRY-FORWARD (Task-2 review): CodecException extends \RuntimeException OUTSIDE the
            // FerroException tree the Task-4 classifier catches on. A garbled/torn terminal body that
            // read fully but failed to parse is a protocol fault, not a fate signal → ProtocolException.
            throw new ProtocolException('failed to decode SQL terminal: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Decode an `Ok` {@see ExecOk} body into column names + value-policy-decoded rows.
     *
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int}
     */
    private function decodeExecOk(Outcome $outcome): array
    {
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
    }

    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     * @return list<array<string,mixed>>
     */
    private function assocRows(array $cols, array $rows): array
    {
        $out = [];
        foreach ($rows as $row) {
            $out[] = $this->assocRow($cols, $row);
        }
        return $out;
    }

    /**
     * @param list<string> $cols
     * @param list<mixed> $row
     * @return array<string,mixed>
     */
    private function assocRow(array $cols, array $row): array
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
    private function hydrateDto(string $class, array $cols, array $row): object
    {
        $args = $this->plans->planFor($class, $cols)->argsFor($row);
        return (new \ReflectionClass($class))->newInstanceArgs($args);
    }

    /**
     * Bind positional PHP scalars to the canonical `{tag, data}` shape {@see ExecRequest::encode}
     * consumes. M0 binds strings as TEXT (BYTES binding is post-M0).
     *
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
