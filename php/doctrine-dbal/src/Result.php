<?php // /php/doctrine-dbal/src/Result.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\InvalidColumnIndex;
use Ferro\DBAL\Exception\DriverException;

/**
 * A DBAL driver result over Ferro's `{cols, rows, affected}`.
 *
 * The nine SPI methods plus `getColumnName()`. Only `fetchNumeric()` and `fetchAssociative()` are
 * real cursors; the other four delegate to `Doctrine\DBAL\Driver\FetchUtils`, exactly as all eight
 * bundled driver results do, so the family stays consistent by construction and Task 12's streamed
 * mode inherits the whole family from ONE incremental `fetchNumeric()`.
 *
 * `rowCount()` is the TERMINAL's `affected`, never `count($this->rows)` — they are different
 * numbers, and conflating them reports 0 for an `UPDATE` that changed rows (the exact bug the
 * research spike shipped).
 *
 * `getColumnName()` is declared on `Doctrine\DBAL\Driver\Result` only as a docblock `@method`, which
 * makes it look optional. It is not: `Doctrine\DBAL\Result::getColumnName()` (dbal 4.4.4,
 * `src/Result.php:267-281`) throws a `LogicException` through `method_exists` when it is missing,
 * `Connection::executeCacheQuery()` loops it to build the cache key, and
 * `Driver\Middleware\AbstractResultMiddleware` forwards it behind the same guard — so omitting it
 * would silently disable DBAL's result cache and break any middleware wrapping our result.
 *
 * Buffered mode only; Task 12 adds the streamed mode alongside it.
 */
final class Result implements ResultInterface
{
    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    private function __construct(
        private array $cols,
        private array $rows,
        private readonly int $affected,
        private int $cursor = 0,
    ) {}

    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    public static function buffered(array $cols, array $rows, int $affected): self
    {
        return new self($cols, $rows, $affected);
    }

    /**
     * The ONE cursor. Everything else in the family is built on it (directly, or through
     * `FetchUtils`), so an exhausted result keeps answering `false` from every method for free.
     *
     * @return list<mixed>|false
     */
    public function fetchNumeric(): array|false
    {
        return $this->rows[$this->cursor++] ?? false;
    }

    /**
     * @return array<string,mixed>|false
     *
     * DUPLICATE column names collapse here (the last wins) exactly as they do under PDO. That is
     * why {@see fetchNumeric} is built on POSITIONAL rows from the wire rather than on
     * `array_values()` of this — the numeric shape must not lose a column.
     *
     * The arity check turns a framing disagreement into a `Doctrine\DBAL\Driver\Exception`. Without
     * it `array_combine()` raises a bare `ValueError`, and `Doctrine\DBAL\Connection::executeQuery()`
     * catches exactly `Driver\Exception` — so the raw error would escape DBAL's conversion entirely
     * and reach the application past every `catch (Doctrine\DBAL\Exception)` it has.
     */
    public function fetchAssociative(): array|false
    {
        $row = $this->fetchNumeric();
        if ($row === false) {
            return false;
        }
        if (count($row) !== count($this->cols)) {
            throw DriverException::local(sprintf(
                'Ferro: result row has %d cells but the header declared %d columns.',
                count($row),
                count($this->cols),
            ));
        }
        return array_combine($this->cols, $row);
    }

    /**
     * `false` at end-of-result, and NEVER for a row whose first cell is `NULL` — the two are
     * different answers and `FetchUtils::fetchFirstColumn()` is `while (($v = fetchOne()) !== false)`,
     * so a `$row[0] ?? false` shortcut would silently TRUNCATE a column containing NULLs. Delegating
     * to `FetchUtils` (as every bundled driver does) is what keeps the distinction.
     */
    public function fetchOne(): mixed
    {
        return FetchUtils::fetchOne($this);
    }

    /** @return list<list<mixed>> the rows REMAINING from the cursor, not the whole buffer */
    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    /** @return list<array<string,mixed>> the rows REMAINING from the cursor, not the whole buffer */
    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    /** @return list<mixed> */
    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    /**
     * The TERMINAL's `affected` count — never `count($this->rows)`, which is a different number
     * (`Doctrine\DBAL\Connection::executeStatement()` returns exactly this value for a
     * parameterised statement, and a `SELECT` carries rows while affecting nothing).
     *
     * **A documented cross-backend divergence:** for a `SELECT`, PostgreSQL's command tag reports
     * the row count while MySQL reports `0`. DBAL treats `rowCount()` on a SELECT as
     * driver-specific and undefined, and every stock driver has the same divergence, so this is
     * reported as-is rather than normalised — normalising it would mean counting rows, which is
     * exactly the conflation above.
     */
    public function rowCount(): int
    {
        return $this->affected;
    }

    public function columnCount(): int
    {
        return count($this->cols);
    }

    public function getColumnName(int $index): string
    {
        return $this->cols[$index] ?? throw InvalidColumnIndex::new($index);
    }

    /**
     * Idempotent, and afterwards the result holds no rows and no columns: `fetchNumeric()` is
     * `false`, `fetchAll*()` are `[]`, `columnCount()` is `0` and `getColumnName()` throws
     * `InvalidColumnIndex` — the same post-free state the stock `Driver\PgSQL\Result` reaches by
     * nulling its `PgSql\Result` handle.
     *
     * `rowCount()` deliberately SURVIVES, because `affected` is a value the terminal already
     * delivered rather than a handle into a released result. That matches the stock
     * `Driver\SQLite3\Result`, whose `rowCount()` returns a `$changes` int captured at construction
     * and is unaffected by `free()`; `PgSQL\Result` is the one bundled driver that answers `0`
     * afterwards, because it reads `pg_affected_rows()` off the very handle it just released. Both
     * shapes exist upstream; ours is the one that cannot lose a number it was already told.
     */
    public function free(): void
    {
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
}
