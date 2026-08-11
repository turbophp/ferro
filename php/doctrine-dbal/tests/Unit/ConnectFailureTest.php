<?php // /php/doctrine-dbal/tests/Unit/ConnectFailureTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Driver\Exception as DbalDriverExceptionInterface;
use Doctrine\DBAL\Exception as DbalException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\TransportException;
use Ferro\DBAL\Driver;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\RetryableDriverException;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * The whole-branch review's MAJOR: a connect failure escaped DBAL's exception hierarchy ENTIRELY and
 * lost its §9.2 fate.
 *
 * `Doctrine\DBAL\Connection::connect()` catches exactly `Doctrine\DBAL\Driver\Exception`; a
 * `Ferro\Client\Error\TransportException` is not one, so with `ferrod` down every worker's first
 * query threw a class no framework has ever heard of — measured: `instanceof
 * Doctrine\DBAL\Driver\Exception: NO`, `instanceof Doctrine\DBAL\Exception: NO`. Nothing in the
 * package covered a failed connect at all, and `DriverOptionsTest` asserted the raw
 * `\InvalidArgumentException`, i.e. it PINNED the escape rather than catching it.
 *
 * **Two vantage points on purpose.** The driver-SPI one (`Driver::connect()` directly) proves the
 * class; the APPLICATION one (a real `Doctrine\DBAL\Connection` built by `DriverManager`, then
 * `executeQuery`) proves the thing the finding is actually about — that `catch
 * (Doctrine\DBAL\Exception)`, which is what every app and bundle writes, now fires. The first alone
 * would be species (c): correct at a vantage point nobody occupies.
 *
 * No ferrod and no socket are needed: the failure IS the absence of one. The paths below are
 * deliberately non-existent, and the TCP port is one nothing listens on.
 */
final class ConnectFailureTest extends TestCase
{
    /** A UDS path in the temp dir that nothing can be listening on. */
    private static function deadSocket(): string
    {
        return sys_get_temp_dir() . '/ferro-no-such-engine-' . getmypid() . '.sock';
    }

    /** @return array<string, array{0: array<string,mixed>}> */
    public static function deadTransports(): array
    {
        return [
            'a missing UDS (ferrod down, or the wrong socket path)' => [[
                'unix_socket' => sys_get_temp_dir() . '/ferro-no-such-engine.sock',
                'driverOptions' => ['pool' => 'main', 'connect_timeout' => 0.25],
            ]],
            'a refused TCP port (the FERRO_ADDR fallback)' => [[
                'host' => '127.0.0.1',
                'port' => 59999,
                'driverOptions' => ['pool' => 'main', 'connect_timeout' => 0.25],
            ]],
        ];
    }

    /**
     * @param array<string,mixed> $params
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('deadTransports')]
    public function testAFailedConnectLeavesTheDriverAsADbalDriverException(array $params): void
    {
        try {
            (new Driver())->connect($params);
            self::fail('connecting to nothing must fail');
        } catch (\Throwable $e) {
            self::assertInstanceOf(
                DbalDriverExceptionInterface::class,
                $e,
                'this is the interface Doctrine\DBAL\Connection::connect() catches — anything else '
                . 'escapes DBAL conversion entirely',
            );
            self::assertNotInstanceOf(
                FerroException::class,
                $e,
                'and it must not be the raw client exception (the measured escape)',
            );
            self::assertInstanceOf(FerroException::class, $e->getPrevious(), 'the cause is preserved');
            self::assertStringContainsString('could not connect to the engine', $e->getMessage());
        }
    }

    /**
     * @param array<string,mixed> $params
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('deadTransports')]
    public function testAnApplicationCatchesItAsADbalException(array $params): void
    {
        // `serverVersion` short-circuits platform resolution, so the ONLY thing that can fail here is
        // the connect — which is what is being measured.
        $conn = DriverManager::getConnection(
            ['driverClass' => Driver::class, 'serverVersion' => '17.10'] + $params,
        );

        try {
            $conn->executeQuery('SELECT 1');
            self::fail('a query against a dead engine must fail');
        } catch (DbalException $e) {
            // THE application vantage point: `catch (Doctrine\DBAL\Exception)`.
            self::assertInstanceOf(
                RetryableDriverException::class,
                $e,
                'a connect failure applied nothing, so it is §9.2 Retryable — that is what lets a '
                . 'framework backoff loop ride out a ferrod restart instead of failing the request',
            );
            self::assertInstanceOf(TransportException::class, self::rootCause($e));
        }
    }

    /**
     * A malformed `driverOptions` value is a CONFIGURATION mistake: catchable as a DBAL exception,
     * and deliberately NOT retryable — retrying cannot fix a typo.
     *
     * `DriverOptions::fromParams()` still throws its own `\InvalidArgumentException` (its unit test
     * asserts exactly that); the conversion happens at the boundary.
     */
    public function testABadDriverOptionIsCatchableAndNotRetryable(): void
    {
        $conn = DriverManager::getConnection([
            'driverClass' => Driver::class,
            'serverVersion' => '17.10',
            'unix_socket' => self::deadSocket(),
            'driverOptions' => ['pool' => 123],
        ]);

        try {
            $conn->executeQuery('SELECT 1');
            self::fail('a non-string pool name must fail');
        } catch (DbalException $e) {
            self::assertNotInstanceOf(RetryableDriverException::class, $e, 'a config error is deterministic');
            self::assertInstanceOf(\InvalidArgumentException::class, self::rootCause($e));
            self::assertStringContainsString('driverOptions.pool must be a string', $e->getMessage());
        }
    }

    /**
     * The FATE half, tested on the classifier directly because a handshake rejection cannot be
     * produced without an engine that speaks a different `/proto` registry.
     *
     * The asymmetry is load-bearing: a TRANSPORT failure is `Retryable` (nothing applied on a session
     * that never opened), while a HANDSHAKE rejection is FATAL — `HandshakeException`'s own docblock
     * says reconnecting re-enters the same rejection — so it must arrive branch-less and never be
     * upgraded to `RetryableDriverException`.
     */
    public function testTheFateIsRetryableForTransportAndFatalForAHandshakeRejection(): void
    {
        $transport = DriverException::connectFailed(new TransportException('connect failed'), 'ctx');
        self::assertSame(C::BRANCH_RETRYABLE, $transport->branch());

        $handshake = DriverException::connectFailed(
            new HandshakeException(new ErrorPayload(
                code: C::ERR_UNSUPPORTED,
                branch: C::BRANCH_NON_RETRYABLE,
                sqlstate: null,
                errno: null,
                message: 'type_registry_hash mismatch',
                detail: null,
                retryAfterMs: null,
            )),
            'ctx',
        );
        self::assertNull(
            $handshake->branch(),
            'a registry/version mismatch is not retryable — reconnecting re-enters the same rejection',
        );
    }

    private static function rootCause(\Throwable $e): \Throwable
    {
        while ($e->getPrevious() !== null) {
            $e = $e->getPrevious();
        }
        return $e;
    }
}
