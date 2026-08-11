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
     * WHOLE-BRANCH REVIEW, BLOCKER (`review/wb-docs.md`): this repository's own README shipped
     * `'ferro' => ['pool' => 'main', 'read_pool' => 'main_ro']`, a shape SPEC §14 documented and
     * this slice REMOVED. Measured before the fix: `fromParams()` returned `pool = "default"` —
     * a different pool, therefore a different DSN, therefore possibly a different database — with
     * no exception, no warning and no PHPStan error (it is the operator's array, not the driver's).
     *
     * The refusal has to fire on the exact params an operator copies, i.e. WITH a working
     * `unix_socket` next to it: that is the case that would otherwise connect successfully to the
     * wrong place. It also has to fire BEFORE the "no transport configured" check, or the diagnosis
     * for a straight copy-paste (no socket added yet) names the wrong mistake.
     */
    public function testATopLevelFerroKeyIsRefusedRatherThanSilentlyRoutingToTheDefaultPool(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/`ferro`.+driverOptions/s');
        DriverOptions::fromParams([
            'unix_socket' => '/run/ferro/app.sock',
            'ferro' => ['pool' => 'main', 'read_pool' => 'main_ro'],
        ]);
    }

    /** The same block with no transport beside it still names the `ferro` key, not the transport. */
    public function testTheFerroKeyRefusalPrecedesTheTransportCheck(): void
    {
        try {
            DriverOptions::fromParams(['ferro' => ['pool' => 'main']]);
            self::fail('expected a refusal');
        } catch (\InvalidArgumentException $e) {
            self::assertStringContainsString('`ferro`', $e->getMessage());
            self::assertStringNotContainsString('no engine transport', $e->getMessage());
        }
    }

    /**
     * `read_pool` gets its own answer at BOTH levels, because the mistake arrives from both: the
     * removed README example put it at the top level, and the natural retry is to move it into
     * `driverOptions` — where a generic "unrecognised key" message would be true but would not say
     * that the feature does not exist and why (charter rule 6).
     *
     * @return array<string, array{0: array<string, mixed>}>
     */
    public static function readPoolShapes(): array
    {
        return [
            'top level' => [['unix_socket' => '/s', 'read_pool' => 'main_ro']],
            'inside driverOptions' => [['unix_socket' => '/s', 'driverOptions' => ['read_pool' => 'main_ro']]],
        ];
    }

    /** @param array<string, mixed> $params */
    #[\PHPUnit\Framework\Attributes\DataProvider('readPoolShapes')]
    public function testReadPoolIsRefusedAtEveryLevelAndNamesTheSecondConnection(array $params): void
    {
        try {
            DriverOptions::fromParams($params);
            self::fail('expected a refusal');
        } catch (\InvalidArgumentException $e) {
            self::assertStringContainsString('read_pool', $e->getMessage());
            self::assertStringContainsString("'readonly' => true", $e->getMessage());
        }
    }

    /**
     * `driverOptions` is Ferro's OWN namespace, so the check there is exhaustive — a typo and a
     * leftover PDO attribute are the same defect as `read_pool`, and each of these rows is a
     * different reachable shape rather than a restatement of one.
     *
     * @return array<string, array{0: array<string, mixed>}>
     */
    public static function unrecognisedOptions(): array
    {
        return [
            'a typo in a real key' => [['pooll' => 'main']],
            'the client facade spelling' => [['read_only' => true]],
            'a leftover PDO attribute' => [[\PDO::ATTR_EMULATE_PREPARES => false]],
        ];
    }

    /** @param array<string, mixed> $driverOptions */
    #[\PHPUnit\Framework\Attributes\DataProvider('unrecognisedOptions')]
    public function testAnUnrecognisedDriverOptionsKeyIsRefused(array $driverOptions): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/unrecognised driverOptions key/');
        DriverOptions::fromParams(['unix_socket' => '/s', 'driverOptions' => $driverOptions]);
    }

    /**
     * The counterweight, and the reason the refusal can be trusted not to be over-broad: every
     * recognised key together still parses, and the top-level params DBAL itself owns
     * (`driverClass`, `wrapperClass`, `serverVersion`, `user`, `password`, `dbname`, `charset` —
     * the last four inert under Ferro but routinely present in a real config) are NOT refused.
     * A blanket top-level unknown-key check would fail this test.
     */
    public function testEveryRecognisedKeyTogetherParsesAndDbalsOwnParamsAreNotRefused(): void
    {
        $o = DriverOptions::fromParams([
            'driverClass' => 'Ferro\DBAL\Driver',
            'wrapperClass' => 'Ferro\DBAL\Wrapper\FerroConnection',
            'serverVersion' => '17.10',
            'user' => 'app', 'password' => 'x', 'dbname' => 'app', 'charset' => 'utf8',
            'unix_socket' => '/run/ferro/app.sock',
            'driverOptions' => [
                'socket' => '/run/ferro/app.sock',
                'pool' => 'main',
                'readonly' => true,
                'connect_timeout' => 1.5,
                'io_timeout' => 9.0,
            ],
        ]);
        self::assertSame('main', $o->pool);
        self::assertTrue($o->readonly);
        self::assertSame(1.5, $o->connectTimeout);
        self::assertSame(9.0, $o->ioTimeout);
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
