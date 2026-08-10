<?php // /php/doctrine-dbal/tests/Unit/TemporalFormatTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDB110700Platform;
use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Value\TemporalFormat;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 9 — the driver holds two format-string literals so that its value policy does not
 * have to resolve a PLATFORM (which would need a server version, which may not exist yet). That
 * duplication is only safe if it is LOCKED against the stock accessors, which is what this test is:
 * if a DBAL release changes `getDateTimeTzFormatString()` for either family, this goes red rather
 * than the driver silently emitting a shape DBAL can no longer parse.
 *
 * MEASURED on 4.4.4: PostgreSQL `Y-m-d H:i:sO`, MySQL and MariaDB `Y-m-d H:i:s` (no offset at all).
 */
final class TemporalFormatTest extends TestCase
{
    public function testOurLiteralsEqualTheStockPlatformAccessors(): void
    {
        self::assertSame(
            (new PostgreSQL120Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_POSTGRES)->dateTimeTz,
        );
        self::assertSame(
            (new MySQL84Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
        );
        self::assertSame(
            (new MariaDB110700Platform())->getDateTimeTzFormatString(),
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
            'MariaDB and MySQL share the format, which is why one KIND covers both',
        );
    }

    public function testTheTwoFamiliesGenuinelyDiffer(): void
    {
        self::assertNotSame(
            TemporalFormat::forKind(PlatformVersion::KIND_POSTGRES)->dateTimeTz,
            TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz,
            'if these ever became equal, the per-kind branch would be dead code and this test a tautology',
        );
    }

    /**
     * ADDED beyond the plan. The `default` arm is the only thing standing between an unknown family
     * and a GUESSED datetime format, and nothing else in the task reaches it: the two rows above
     * both name a known kind, so a `default => 'Y-m-d H:i:s'` would keep the whole file green.
     */
    public function testAnUnknownFamilyIsRefusedRatherThanGivenAFormat(): void
    {
        $this->expectException(BackendFamilyUnknown::class);
        TemporalFormat::forKind('sqlite');
    }
}
