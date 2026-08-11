<?php // /php/doctrine-dbal/src/Wrapper/FerroConnection.php
declare(strict_types=1);
namespace Ferro\DBAL\Wrapper;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\Protocol\Isolation;

/**
 * The optional `wrapperClass` that makes `setTransactionIsolation()` actually work.
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'  => Ferro\DBAL\Driver::class,
 *     'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class,
 *     'unix_socket'  => '/run/ferro/app.sock',
 * ]],
 * ```
 *
 * **Why it exists.** Doctrine's own `setTransactionIsolation()` runs
 * `executeStatement($platform->getSetTransactionIsolationSQL($level))` — the SESSION form. On a
 * transaction-mode pool that statement lands on an arbitrary pooled connection, taints it, and is
 * wiped by hygiene before the next `BEGIN`: it reports success and changes nothing, while
 * `getTransactionIsolation()` keeps returning the level Doctrine cached. SPEC §22.2 (s) names both
 * spellings as the FORBIDDEN form for exactly this reason, and records that the obvious "did the
 * next tenant inherit it" test cannot fail because hygiene masks the leak either way.
 *
 * This override captures the level as a TYPED enum, above the SQL layer, and hands it to the driver
 * connection to ride `BeginRequest.isolation` on the next transaction — where the engine composes
 * the correct PER-TRANSACTION form for the dialect (`BEGIN ISOLATION LEVEL …` on PostgreSQL, the
 * batched `SET TRANSACTION …; START TRANSACTION …` on MySQL). **No SQL is inspected, rewritten or
 * generated here** — charter rule 6 is untouched; the wrapper simply never emits the statement.
 */
class FerroConnection extends DbalConnection
{
    private ?TransactionIsolationLevel $ferroLevel = null;

    public function setTransactionIsolation(TransactionIsolationLevel $level): void
    {
        $inner = $this->connect();
        if (!$inner instanceof FerroDriverConnection) {
            // Wrapping a non-Ferro driver: behave exactly like stock Doctrine.
            parent::setTransactionIsolation($level);
            return;
        }
        $this->ferroLevel = $level;
        $inner->setIsolation(self::toFerroIsolation($level));
    }

    public function getTransactionIsolation(): TransactionIsolationLevel
    {
        return $this->ferroLevel ?? parent::getTransactionIsolation();
    }

    /**
     * DBAL's level → Ferro's wire enum.
     *
     * `READ_UNCOMMITTED` becomes `ReadCommitted`, which is what `Ferro\Protocol\Isolation`'s own
     * docblock specifies: PostgreSQL treats the two as the same level and the wire enum has no
     * fourth value. On MySQL that is a genuine UPGRADE to a stricter level — never a weaker one —
     * and it is recorded in `docs/known-incompatibilities.md`.
     *
     * The `match` has **no `default` arm** on purpose: it is the nearest thing PHP offers to a
     * compile-forced mapping, so a fifth `TransactionIsolationLevel` case in a future DBAL release
     * throws `\UnhandledMatchError` here instead of being silently coerced to a level nobody asked
     * for. `IsolationRefusalTest::testEveryDbalLevelMaps` derives its table from
     * `TransactionIsolationLevel::cases()` for the same reason.
     */
    public static function toFerroIsolation(TransactionIsolationLevel $level): Isolation
    {
        return match ($level) {
            TransactionIsolationLevel::READ_UNCOMMITTED,
            TransactionIsolationLevel::READ_COMMITTED => Isolation::ReadCommitted,
            TransactionIsolationLevel::REPEATABLE_READ => Isolation::RepeatableRead,
            TransactionIsolationLevel::SERIALIZABLE => Isolation::Serializable,
        };
    }

    /**
     * Whether `$sql` is one of the two isolation statements Doctrine's platforms generate.
     *
     * A CLOSED, prefix-anchored test on the two fixed strings — not open-ended SQL parsing. It is
     * anchored so a literal appearing inside an INSERT or a comparison cannot trip it, which
     * matters: a refusal that fired on ordinary SQL would be far worse than the bug it prevents.
     */
    public static function isIsolationStatement(string $sql): bool
    {
        $t = ltrim($sql);
        foreach ([
            'SET SESSION TRANSACTION ISOLATION LEVEL',
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL',
        ] as $prefix) {
            if (strncasecmp($t, $prefix, strlen($prefix)) === 0) {
                return true;
            }
        }
        return false;
    }
}
