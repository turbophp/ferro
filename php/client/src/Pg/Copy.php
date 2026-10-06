<?php // /php/client/src/Pg/Copy.php
declare(strict_types=1);
namespace Ferro\Pg;

use Ferro\Bytes;
use Ferro\Client\Connection;
use Ferro\Client\TxHandle;

/**
 * PostgreSQL's COPY through Ferro (M3-D4; SPEC §6.1, §14) — the first-class replacement for
 * `pdo_pgsql`'s `pgsqlCopyFromArray()`/`pgsqlCopyToArray()`, which buffer the whole data set in PHP
 * memory. Here neither direction buffers.
 *
 * ```php
 * $copy = new Ferro\Pg\Copy($connection);           // or a transaction() closure's TxHandle
 * $n = $copy->in('COPY items (id, name) FROM STDIN', Copy::textRows($rowGenerator));
 * foreach ($copy->out('COPY items TO STDOUT', readonly: true) as $chunk) { fwrite($out, $chunk); }
 * ```
 *
 * Ferro moves BYTES: the data is whatever the statement's format says it is (text — the default —
 * CSV, or binary), and formatting rows is the caller's job. {@see textRow} is the one formatter
 * provided, for the default text format, because its escaping rules are short and exact; CSV and
 * binary are left to the caller rather than half-implemented here. A chunk handed to `in()` need not
 * end on a row boundary, and a chunk `out()` yields may end mid-row: COPY data is a byte stream.
 *
 * The contracts — atomicity, the §19.3 fate of a lost or stopped COPY, `readonly` on `out()` — are
 * {@see Connection::copyIn} and {@see Connection::copyOut}'s.
 */
final class Copy
{
    public function __construct(private readonly Connection|TxHandle $target) {}

    /**
     * `COPY … FROM STDIN`: send `$data` and return the number of rows copied.
     *
     * @param iterable<mixed, string> $data
     */
    public function in(string $sql, iterable $data): int
    {
        return $this->target->copyIn($sql, $data);
    }

    /**
     * `COPY … TO STDOUT`, lazily; `getReturn()` is the number of rows exported.
     *
     * @return \Generator<int, string, mixed, int>
     */
    public function out(string $sql, bool $readonly = false): \Generator
    {
        return $this->target->copyOut($sql, $readonly);
    }

    /**
     * One row in PostgreSQL's default COPY TEXT format (tab-delimited, `\N` for NULL), newline
     * included. A string is escaped exactly as the format requires — backslash, newline, carriage
     * return and tab become `\\`, `\n`, `\r`, `\t` — so the server's COPY reader hands the column
     * type's TEXT INPUT exactly the bytes given. What the column makes of them is that type's input
     * function: into `text`/`varchar` they round-trip (a NUL byte or invalid encoding is refused by
     * the server, loudly); into `bytea` they do NOT — bytea's input re-parses `\x…` and `\nnn` — so
     * pass a {@see Bytes} for a bytea column, which is rendered in bytea's own hex form and
     * round-trips any bytes. An int is its decimal form; a bool is `t`/`f`; null is `\N`. A float is
     * REFUSED: PHP's own string form of a float depends on `precision` and can drop digits, so format
     * it yourself into a string. Only for the default text format: a statement with `DELIMITER`,
     * `NULL` or `FORMAT csv` options needs rows in that format instead.
     *
     * @param array<array-key, mixed> $values string|int|bool|null|Bytes, checked at runtime
     */
    public static function textRow(array $values): string
    {
        $out = [];
        foreach ($values as $v) {
            $out[] = match (true) {
                $v === null => '\N',
                is_bool($v) => $v ? 't' : 'f',
                is_int($v) => (string) $v,
                // bytea's hex input form (`\x` + hex), its backslash escaped for the COPY reader.
                $v instanceof Bytes => '\\\\x' . bin2hex($v->value),
                is_string($v) => strtr($v, ['\\' => '\\\\', "\n" => '\n', "\r" => '\r', "\t" => '\t']),
                default => throw new \InvalidArgumentException(sprintf(
                    'Copy::textRow() takes string|int|bool|null|Ferro\\Bytes, got %s (format a float into a string yourself)',
                    get_debug_type($v),
                )),
            };
        }
        return implode("\t", $out) . "\n";
    }

    /**
     * {@see textRow} over an iterable of rows, lazily — feed it straight to {@see in}.
     *
     * @param iterable<mixed, array<array-key, mixed>> $rows
     * @return \Generator<int, string>
     */
    public static function textRows(iterable $rows): \Generator
    {
        foreach ($rows as $row) {
            yield self::textRow($row);
        }
    }
}
