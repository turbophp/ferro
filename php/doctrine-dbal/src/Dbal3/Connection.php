<?php // /php/doctrine-dbal/src/Dbal3/Connection.php
declare(strict_types=1);
namespace Ferro\DBAL\Dbal3;

use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Doctrine\DBAL\ParameterType;
use Ferro\DBAL\AbstractConnection;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\ExceptionConverter;

/**
 * The DBAL **3** driver connection: {@see AbstractConnection} plus the methods whose SIGNATURE is
 * DBAL 3's alone. Every behaviour — the fate declaration, the pinned-transaction routing, the
 * streamed results, the isolation refusal, the nil-version decision — is the base class's and is
 * identical to DBAL 4's.
 *
 * It implements `ServerInfoAwareConnection` (which DBAL 4 deleted) because DBAL 3's
 * `Connection::getServerVersion()` asks for the version ONLY through that interface; without it
 * DBAL 3 would fall back to a version-less platform (SPEC §22.2 (by)). `getServerVersion()` itself
 * is the base class's.
 */
final class Connection extends AbstractConnection implements ServerInfoAwareConnection
{
    protected function isolationWrapperClass(): string
    {
        return FerroConnection::class;
    }

    public function prepare(string $sql): DriverStatement
    {
        return new Statement($this, $sql);
    }

    /**
     * DBAL 3's `quote($value, $type)`. The wrapper has ALREADY converted the value through its
     * Doctrine `Type`, so what arrives is a scalar. For every textual type `$type` changes nothing
     * about a string literal, so it is not consulted.
     *
     * **Except the binary ones, which are REFUSED.** Both stock PostgreSQL drivers consult `$type`
     * there and escape the value as `bytea` (`pg_escape_bytea`/`PQescapeByteaConn`), so
     * `quote("\\x41", LARGE_OBJECT)` becomes a literal that stores the four bytes `\x41`; quoted as
     * text, PostgreSQL's `bytea` input would read that same literal as hex and store ONE byte, 0x41 —
     * a silently different value (measured live, M2-C5 review F7). A binary literal's syntax is
     * per-family and per-column-type, so the honest answer is the one the class already gives every
     * other value it cannot represent: refuse, and point at the bound parameter, which carries the
     * bytes as `TAG_BYTES` and stores them intact.
     *
     * A value with no string form is refused rather than stringified into nonsense.
     *
     * @param mixed $value
     * @param int $type
     */
    public function quote($value, $type = ParameterType::STRING): string
    {
        if ($type === ParameterType::BINARY || $type === ParameterType::LARGE_OBJECT) {
            throw DriverException::local(
                'Ferro: refusing to quote() a binary value — a binary literal\'s syntax depends on the '
                . 'backend and the column type, and quoting it as text would store different bytes. '
                . 'Bind it as a parameter instead (ParameterType::BINARY / LARGE_OBJECT).',
            );
        }
        if ($value === null || is_scalar($value) || $value instanceof \Stringable) {
            return $this->quoteString(is_bool($value) ? ($value ? '1' : '0') : (string) $value);
        }
        throw DriverException::local(sprintf('Ferro: cannot quote a value of type %s.', get_debug_type($value)));
    }

    /**
     * **Throws when there is no key, where DBAL 3's SPI docblock allows `false`.** Returning
     * `false` would be a silently wrong key in the commonest consumer: Doctrine ORM 2's
     * `IdentityGenerator` is `(int) $conn->lastInsertId($sequenceName)`, so `false` becomes the
     * primary key `0`. A `Doctrine\DBAL\Driver\Exception` is what DBAL 3's wrapper already catches
     * and converts, so a caller sees an ordinary `Doctrine\DBAL\Exception\DriverException` naming
     * the reason (DBAL 3 has no `NoIdentityValue`; DBAL 4 added it for exactly this signal).
     *
     * **The sequence name is not used, on any family.** On MySQL and SQLite the engine reports the
     * key itself and a name means nothing (PDO ignores it there too). On PostgreSQL its only use
     * would be a follow-up `currval()`: OUTSIDE a transaction that is the cross-connection hazard
     * {@see AbstractConnection::noKeyMessage} describes, and INSIDE one — where Doctrine ORM 2's
     * identity generator runs — it would be correct, and is deferred to the ORM-suite slice for
     * both majors at once (SPEC §22.2 (by)). Until then DBAL 3 gets exactly DBAL 4's answer. DBAL 3's own upstream test of the `false` return (`WriteTest::
     * testLastInsertIdNoSequenceGiven`) is skipped on every family Ferro serves, since all three
     * support identity columns.
     *
     * @param string|null $name
     */
    public function lastInsertId($name = null): int|string
    {
        return $this->generatedKey() ?? throw DriverException::local($this->noKeyMessage());
    }

    /**
     * DBAL 3's SPI returns `true` on success (and throws on failure, as every driver does).
     *
     * **These three CONVERT their own failures, which no other method here does** — because DBAL 3's
     * wrapper never converts a driver exception from `beginTransaction()`, `commit()` or
     * `rollBack()` (DBAL 4's does, in all three), and `transactional()` rethrows them raw. Left to
     * the wrapper, a PostgreSQL `40001` serialization failure at COMMIT — which SSI defers to COMMIT
     * by design, so it is the commonest shape — reached the application as a bare driver exception
     * instead of a `DeadlockException` (`RetryableException`), and an indeterminate COMMIT never as
     * `IndeterminateWriteException` (M2-C5 review F3, measured on both majors). Converted here, the
     * shared {@see ExceptionConverter} gives DBAL 3 exactly DBAL 4's classes. Legal on DBAL 3's SPI,
     * whose `@throws` is `Driver\Exception`: DBAL 3's `Doctrine\DBAL\Exception\DriverException`
     * implements it, and the converter returns an already-converted exception unchanged when
     * `transactional()` hands it back to decide whether to roll back.
     */
    public function beginTransaction(): bool
    {
        try {
            $this->doBegin();
        } catch (DriverException $e) {
            throw $this->converted($e);
        }
        return true;
    }

    public function commit(): bool
    {
        try {
            $this->doCommit();
        } catch (DriverException $e) {
            throw $this->converted($e);
        }
        return true;
    }

    public function rollBack(): bool
    {
        try {
            $this->doRollBack();
        } catch (DriverException $e) {
            throw $this->converted($e);
        }
        return true;
    }

    private function converted(DriverException $e): \Doctrine\DBAL\Exception\DriverException
    {
        return (new ExceptionConverter($this->poolKind()))->convert($e, null);
    }
}
