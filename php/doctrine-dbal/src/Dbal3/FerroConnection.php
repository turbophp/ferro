<?php // /php/doctrine-dbal/src/Dbal3/FerroConnection.php
declare(strict_types=1);
namespace Ferro\DBAL\Dbal3;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\AbstractConnection;
use Ferro\DBAL\Exception\DriverException;
use Ferro\Protocol\Isolation;

/**
 * The DBAL **3** `wrapperClass` that makes `setTransactionIsolation()` actually work — DBAL 4's
 * {@see \Ferro\DBAL\Wrapper\FerroConnection}, re-declared for DBAL 3's untyped
 * `setTransactionIsolation($level): int` (DBAL 4's override cannot even be declared here).
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'  => Ferro\DBAL\Dbal3\Driver::class,
 *     'wrapperClass' => Ferro\DBAL\Dbal3\FerroConnection::class,
 *     'unix_socket'  => '/run/ferro/app.sock',
 * ]],
 * ```
 *
 * Why it exists is unchanged from DBAL 4: Doctrine's own `setTransactionIsolation()` emits the
 * SESSION form, which on a transaction-mode pool reports success and changes nothing (SPEC §22.2
 * (s)). This override captures the level and hands it to the driver connection to ride
 * `BeginRequest.isolation` on the next transaction. No SQL is inspected, rewritten or generated.
 */
class FerroConnection extends DbalConnection
{
    /** @var TransactionIsolationLevel::*|null */
    private ?int $ferroLevel = null;

    /**
     * @param TransactionIsolationLevel::* $level
     * @return int|string DBAL 3 returns the SET statement's affected count; Ferro sends none, so 0.
     */
    public function setTransactionIsolation($level)
    {
        // Through the NATIVE connection: with any driver middleware configured the wrapper's own
        // driver-connection handle is a middleware's wrapper — see
        // AbstractConnection::forNativeConnection(). `getNativeConnection()` also connects from
        // INSIDE doctrine/dbal, so it raises none of the "public access to connect() is deprecated"
        // notices DBAL 3 emits for a caller in this package.
        $inner = AbstractConnection::forNativeConnection($this->getNativeConnection());
        if ($inner === null) {
            // Wrapping a non-Ferro driver: behave exactly like stock Doctrine.
            return parent::setTransactionIsolation($level);
        }
        $isolation = self::toFerroIsolation($level);
        $this->ferroLevel = $level;
        $inner->setIsolation($isolation);
        return 0;
    }

    /** @return TransactionIsolationLevel::* */
    public function getTransactionIsolation()
    {
        return $this->ferroLevel ?? parent::getTransactionIsolation();
    }

    /**
     * DBAL 3's level (an int constant) → Ferro's wire enum, with DBAL 4's mapping:
     * `READ_UNCOMMITTED` becomes `ReadCommitted` (PostgreSQL treats them as one level; on MySQL it is
     * an upgrade to a stricter level, never a weaker one — `docs/known-incompatibilities.md`).
     *
     * DBAL 3's levels are ints, so the match cannot be exhaustive by construction; its `default`
     * arm refuses, which keeps DBAL 4's property that an unknown level is never coerced to one
     * nobody asked for.
     */
    public static function toFerroIsolation(mixed $level): Isolation
    {
        return match ($level) {
            TransactionIsolationLevel::READ_UNCOMMITTED,
            TransactionIsolationLevel::READ_COMMITTED => Isolation::ReadCommitted,
            TransactionIsolationLevel::REPEATABLE_READ => Isolation::RepeatableRead,
            TransactionIsolationLevel::SERIALIZABLE => Isolation::Serializable,
            default => throw DriverException::local(sprintf(
                'Ferro: unknown DBAL 3 transaction isolation level %s.',
                is_int($level) ? (string) $level : get_debug_type($level),
            )),
        };
    }
}
