<?php // /php/doctrine-dbal/tests/Unit/DriverOptionsTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\DBAL\DriverOptions;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — configuration arrives through `driverOptions`, NOT through SPEC §14's `ferro`
 * key. That is not a style preference: `Doctrine\DBAL\Driver::connect()` is `@phpstan-param Params`,
 * `Params` is a SEALED array shape with no `ferro` key, and reading `$params['ferro']['pool']`
 * MEASURED as two `nullCoalesce.offset` errors at PHPStan level 9 — which is a charter
 * Definition-of-Done gate. `driverOptions?: array<mixed>` is the sanctioned slot. §14 is amended by
 * Task 14.
 */
final class DriverOptionsTest extends TestCase
{
    public function testItReadsTheSocketPoolAndReadonlyFlagOutOfDriverOptions(): void
    {
        $o = DriverOptions::fromParams([
            'driverOptions' => ['socket' => '/run/ferro/dev.sock', 'pool' => 'main', 'readonly' => true],
        ]);
        self::assertSame('/run/ferro/dev.sock', $o->socketPath);
        self::assertSame('main', $o->pool);
        self::assertTrue($o->readonly);
    }

    /** `unix_socket` is a first-class DBAL param and naturally carries the ferrod socket path. */
    public function testUnixSocketParamIsAccepted(): void
    {
        $o = DriverOptions::fromParams(['unix_socket' => '/run/ferro/dev.sock']);
        self::assertSame('/run/ferro/dev.sock', $o->socketPath);
        self::assertSame('default', $o->pool, 'the pool defaults to "default"');
        self::assertFalse($o->readonly, 'a connection is a WRITE connection unless declared otherwise');
    }

    /** TCP is the FERRO_ADDR fallback; host+port travel through the ordinary DBAL params. */
    public function testHostAndPortSelectTheTcpTransport(): void
    {
        $o = DriverOptions::fromParams(['host' => '127.0.0.1', 'port' => 7777]);
        self::assertNull($o->socketPath);
        self::assertSame('127.0.0.1', $o->host);
        self::assertSame(7777, $o->port);
    }

    /** Neither a socket nor a host is a configuration error worth reporting as itself. */
    public function testNoTransportAtAllThrowsWithAnActionableMessage(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/unix_socket|driverOptions/');
        DriverOptions::fromParams([]);
    }

    /** A wrongly-typed option is refused, not silently coerced (level 9 narrows, but so do we). */
    public function testAWronglyTypedOptionIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        DriverOptions::fromParams(['driverOptions' => ['socket' => 42, 'pool' => 'main']]);
    }

    /**
     * ADDED (beyond the plan) — the two timeout knobs and their defaults, which nothing else in this
     * task observes. `Ferro::connect()` takes them positionally, so a parse that dropped or swapped
     * them would silently give every driver connection the wrong io budget: with the plan's five
     * tests alone, `connect_timeout` and `io_timeout` could both be hard-coded and stay green.
     */
    public function testTheTimeoutsAreParsedAndDefaultedSeparately(): void
    {
        $d = DriverOptions::fromParams(['unix_socket' => '/s']);
        self::assertSame(2.0, $d->connectTimeout, 'the client facade default');
        self::assertSame(5.0, $d->ioTimeout, 'the client facade default');

        $o = DriverOptions::fromParams([
            'unix_socket' => '/s',
            'driverOptions' => ['connect_timeout' => 0.25, 'io_timeout' => 30],
        ]);
        self::assertSame(0.25, $o->connectTimeout);
        self::assertSame(30.0, $o->ioTimeout, 'an int is accepted and widened to float');
    }

    /**
     * ADDED (beyond the plan) — the MIRROR of `testAWronglyTypedOptionIsRefused`, which on its own
     * cannot tell "every option is type-checked" from "the `socket` key is type-checked". Each row
     * is a DIFFERENT key with a wrong type; a refusal that only covered `socket` fails here.
     *
     * @return array<string, array{0: array<string, mixed>}>
     */
    public static function wrongTypes(): array
    {
        return [
            'pool is not a string' => [['pool' => 42]],
            'readonly is not a bool' => [['readonly' => 'yes']],
            'connect_timeout is not a number' => [['connect_timeout' => 'fast']],
            'io_timeout is not a number' => [['io_timeout' => []]],
        ];
    }

    /**
     * @param array<string, mixed> $driverOptions
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('wrongTypes')]
    public function testEveryOptionIsTypeCheckedNotJustTheSocket(array $driverOptions): void
    {
        $this->expectException(\InvalidArgumentException::class);
        DriverOptions::fromParams(['unix_socket' => '/s', 'driverOptions' => $driverOptions]);
    }

    /**
     * ADDED (beyond the plan) — `driverOptions` itself being the wrong type. The plan's error
     * message for it exists but nothing reached it, and `$params['driverOptions']` is operator-typed
     * configuration: a scalar there (a DSN string pasted into the wrong key) must be a loud refusal
     * rather than an empty option set that then fails much later as "no transport configured".
     */
    public function testANonArrayDriverOptionsIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/driverOptions/');
        DriverOptions::fromParams(['unix_socket' => '/s', 'driverOptions' => 'pool=main']);
    }
}
