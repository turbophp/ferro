<?php // /php/doctrine-dbal/src/Value/TemporalFormat.php
declare(strict_types=1);
namespace Ferro\DBAL\Value;

use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;

/**
 * The one platform format string the value policy needs, per backend family.
 *
 * It is a LITERAL here rather than a call to `$platform->getDateTimeTzFormatString()` for a
 * structural reason: resolving a platform requires a server VERSION, which may not exist yet
 * (SPEC §14's nil-version case) — but a row can arrive before it does. Holding the literal keeps
 * the decode path independent of platform resolution, and
 * {@see \Ferro\DBAL\Tests\Unit\TemporalFormatTest} locks it against the stock accessors so a DBAL
 * release that changes either string turns this into a RED test rather than a driver that emits a
 * shape DBAL can no longer parse.
 *
 * MEASURED on doctrine/dbal 4.4.4: PostgreSQL `Y-m-d H:i:sO`; MySQL and MariaDB `Y-m-d H:i:s`;
 * SQLite `Y-m-d H:i:s` — i.e. neither the MySQL family's nor SQLite's `datetimetz` carries any
 * offset at all, which is DBAL's own mapping of a type neither engine has.
 *
 * **SQLite's entry is a bind-side contract only, and that is worth stating.** No SQLite backend
 * ever EMITS a `TIMESTAMPTZ`: SQLite has no date-time storage class, so all four temporal tags are
 * unreachable on the read path by construction (SPEC §22.2 (bf)). The format still has to exist,
 * because `bindBackend()` resolves it at handshake time for every family and a missing arm would
 * refuse the connection outright.
 */
final class TemporalFormat
{
    private function __construct(public readonly string $dateTimeTz) {}

    public static function forKind(string $kind): self
    {
        return new self(match ($kind) {
            PlatformVersion::KIND_POSTGRES => 'Y-m-d H:i:sO',
            PlatformVersion::KIND_MYSQL, PlatformVersion::KIND_SQLITE => 'Y-m-d H:i:s',
            default => throw BackendFamilyUnknown::forKind($kind),
        });
    }
}
