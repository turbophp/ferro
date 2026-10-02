<?php // /php/doctrine-dbal/src/Connection.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Exception\NoIdentityValue;
use Doctrine\DBAL\Driver\Statement as StatementInterface;

/**
 * The DBAL **4** driver connection: {@see AbstractConnection} plus the methods whose SIGNATURE is
 * DBAL 4's alone. Behaviour lives in the base class; this file only adapts it to the SPI.
 *
 * Its DBAL 3 twin is {@see \Ferro\DBAL\Dbal3\Connection}. Neither can be loaded under the other
 * major — PHP rejects an incompatible method signature when the class is declared — which is why
 * each major has its own `driverClass` (SPEC §14, §22.2 (by)).
 */
final class Connection extends AbstractConnection implements DriverConnection
{
    protected function isolationWrapperClass(): string
    {
        return Wrapper\FerroConnection::class;
    }

    public function prepare(string $sql): StatementInterface
    {
        return new Statement($this, $sql);
    }

    /** @see AbstractConnection::quoteString */
    public function quote(string $value): string
    {
        return $this->quoteString($value);
    }

    /**
     * DBAL 4's SPI takes no sequence name and must THROW when there is no identity value. The
     * thrown class is `Doctrine\DBAL\Driver\Exception\NoIdentityValue` — the SPI's own signal; see
     * {@see AbstractConnection::generatedKey} for the semantics.
     */
    public function lastInsertId(): int|string
    {
        return $this->generatedKey() ?? throw new NoIdentityValue($this->noKeyMessage());
    }

    public function beginTransaction(): void
    {
        $this->doBegin();
    }

    public function commit(): void
    {
        $this->doCommit();
    }

    public function rollBack(): void
    {
        $this->doRollBack();
    }
}
