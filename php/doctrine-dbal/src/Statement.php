<?php // /php/doctrine-dbal/src/Statement.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use Ferro\DBAL\Exception\DriverException;

/**
 * A prepared statement. Ferro has no separate PREPARE round trip at this tier — the engine owns
 * statement caching — so `prepare()` records the SQL and `execute()` sends it with the bound
 * parameters as one `EXEC`.
 *
 * **Positional parameters only.** DBAL 4 hands a named `:name` straight to the driver, and the
 * stock `Driver\Mysqli\Statement::bindValue` simply `assert(is_int($param))`; refusing them loudly
 * here is exactly as capable as the stock mysqli driver, and a silent misbind would be worse.
 *
 * Walking-skeleton form: Task 7 replaces `bindValue`'s body with the full
 * `(ParameterType, PHP type)` → canonical-tag mapping.
 */
final class Statement implements StatementInterface
{
    /** @var array<int,mixed> 1-based, exactly as DBAL numbers them */
    private array $values = [];

    public function __construct(
        private readonly Connection $conn,
        private readonly string $sql,
    ) {}

    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING): void
    {
        if (!is_int($param)) {
            throw DriverException::local(
                'Ferro: named parameters are not supported; use positional `?` placeholders '
                . '(Doctrine expands named parameters above the driver when you pass them to '
                . 'executeQuery()/executeStatement()).',
            );
        }
        $this->values[$param] = $value;
    }

    public function execute(): ResultInterface
    {
        ksort($this->values);
        return $this->conn->runPrepared($this->sql, array_values($this->values));
    }
}
