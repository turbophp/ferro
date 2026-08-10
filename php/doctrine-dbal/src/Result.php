<?php // /php/doctrine-dbal/src/Result.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\InvalidColumnIndex;

/**
 * A DBAL driver result over Ferro's `{cols, rows, affected}`.
 *
 * `rowCount()` is the TERMINAL's `affected`, never `count($this->rows)` — they are different
 * numbers, and conflating them reports 0 for an `UPDATE` that changed rows (the exact bug the
 * research spike shipped).
 *
 * Walking-skeleton form: Task 8 completes the contract, Task 12 adds the streamed mode.
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

    /** @return list<mixed>|false */
    public function fetchNumeric(): array|false
    {
        return $this->rows[$this->cursor++] ?? false;
    }

    /** @return array<string,mixed>|false */
    public function fetchAssociative(): array|false
    {
        $row = $this->fetchNumeric();
        if ($row === false) {
            return false;
        }
        return array_combine($this->cols, $row);
    }

    public function fetchOne(): mixed
    {
        return FetchUtils::fetchOne($this);
    }

    /** @return list<list<mixed>> */
    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    /** @return list<array<string,mixed>> */
    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    /** @return list<mixed> */
    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

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

    public function free(): void
    {
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
}
