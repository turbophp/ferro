<?php // /php/doctrine-dbal/src/Dbal3/Statement.php
declare(strict_types=1);
namespace Ferro\DBAL\Dbal3;

use Doctrine\DBAL\Driver\Result as DriverResult;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Doctrine\DBAL\ParameterType;
use Ferro\DBAL\AbstractConnection;
use Ferro\DBAL\Exception\DriverException;

/**
 * The DBAL **3** prepared statement. Same model as DBAL 4's {@see \Ferro\DBAL\Statement}: `prepare()`
 * records the SQL, `execute()` sends it with the bound parameters as one `EXEC`, positional
 * parameters only, and every value passes through the shared binder at the call site that
 * supplied it.
 *
 * DBAL 3's SPI additionally has `bindParam()` (by reference, read at `execute()`) and
 * `execute($params)` (PDO-style: every value bound as `STRING`). Both are deprecated upstream and
 * both are implemented, because a deprecated method is still a method a DBAL 3 application calls.
 */
final class Statement implements DriverStatement
{
    /** @var array<int,mixed> 1-based, exactly as DBAL numbers them; already canonical */
    private array $values = [];

    /** @var array<int,array{0:mixed,1:mixed}> 1-based; [&variable, ParameterType] read at execute() */
    private array $refs = [];

    public function __construct(
        private readonly AbstractConnection $conn,
        private readonly string $sql,
    ) {}

    /**
     * @param string|int $param
     * @param mixed $value
     * @param int $type
     */
    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        $i = self::position($param);
        unset($this->refs[$i]);
        $this->values[$i] = ParameterBinder::toCanonical($value, $type);
        return true;
    }

    /**
     * Bound by REFERENCE and converted at {@see execute}, which is the whole meaning of `bindParam`:
     * a caller may change the variable between binding and executing. OUT parameters (`$length`)
     * do not exist on this wire and are not emulated.
     *
     * @param string|int $param
     * @param mixed $variable
     * @param int $type
     * @param int|null $length
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $i = self::position($param);
        // No `unset($this->values[$i])`: a reference is applied AFTER the plain values in
        // execute(), so it already wins for its position; only bindValue() must clear the other side.
        $this->refs[$i] = [&$variable, $type];
        return true;
    }

    /** @param mixed[]|null $params PDO-style: replaces every binding, each value bound as STRING. */
    public function execute($params = null): DriverResult
    {
        if ($params !== null) {
            $values = [];
            foreach ($params as $k => $v) {
                // PDO numbers an execute() array from 0; DBAL numbers bindValue() from 1.
                $values[self::position(is_int($k) ? $k + 1 : $k)] = ParameterBinder::toCanonical($v, ParameterType::STRING);
            }
        } else {
            $values = $this->values;
            foreach ($this->refs as $i => [$variable, $type]) {
                $values[$i] = ParameterBinder::toCanonical($variable, $type);
            }
        }
        ksort($values);
        return $this->conn->runPrepared($this->sql, array_values($values));
    }

    private static function position(mixed $param): int
    {
        if (!is_int($param)) {
            throw DriverException::local(
                'Ferro: named parameters are not supported; use positional `?` placeholders '
                . '(Doctrine expands named parameters above the driver when you pass them to '
                . 'executeQuery()/executeStatement()).',
            );
        }
        return $param;
    }
}
