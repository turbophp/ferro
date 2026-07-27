<?php // /php/client/tests/Client/ReconnectLoopTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\SessionInterface;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The epoch-aware reconnect (SPEC §19.1). The load-bearing case: `boot_epoch` is compared as an
 * OPAQUE scalar with strict `===`, NEVER `(int)`-coerced — so two DISTINCT `u64 > PHP_INT_MAX`
 * decimal-string epochs (which BOTH coerce to PHP_INT_MAX) are correctly seen as CHANGED, and a real
 * restart's dead `tx_id` is voided.
 */
final class ReconnectLoopTest extends TestCase
{
    /** A Backoff whose sleep is a no-op so tests are instant. */
    private static function instantBackoff(): Backoff
    {
        return new Backoff(0.0, 0.0, rng: static fn (): float => 0.0, sleep: static function (float $s): void {});
    }

    /** @param \Closure(): SessionInterface $factory */
    private static function loop(SessionInterface $initial, \Closure $factory, int $maxAttempts = 3): ReconnectLoop
    {
        return new ReconnectLoop($initial, $factory, self::instantBackoff(), $maxAttempts);
    }

    // ---- opaque epoch compare (§19.3-CRITICAL) --------------------------------------------------

    /**
     * TWO DISTINCT decimal-string epochs both `> PHP_INT_MAX` → CHANGED. This is the proof that the
     * compare is opaque: `(int)"18446744073709551615"` and `(int)"18446744073709551614"` are BOTH
     * PHP_INT_MAX, so any int coercion would report "unchanged" here and fail the assertion.
     */
    public function testTwoDistinctLargeStringEpochsAreDetectedAsChanged(): void
    {
        $old = '18446744073709551615'; // 2^64 - 1
        $new = '18446744073709551614'; // 2^64 - 2  (distinct, both > PHP_INT_MAX)
        $this->assertGreaterThan(PHP_INT_MAX, (float) $old);
        $this->assertGreaterThan(PHP_INT_MAX, (float) $new);
        $this->assertSame((int) $old, (int) $new, 'sanity: int-coercion WOULD collapse these two epochs');

        $loop = self::loop(new FakeSession($old), static fn (): SessionInterface => new FakeSession($new));
        $this->assertTrue($loop->reconnect(), 'distinct large-string epochs must compare as CHANGED');
        $this->assertTrue($loop->lastEpochChanged());
        $this->assertSame($new, $loop->currentEpoch());
    }

    /** The SAME large-string epoch → NOT changed (engine did not restart). */
    public function testSameLargeStringEpochIsNotChanged(): void
    {
        $epoch = '18446744073709551615';
        $loop = self::loop(new FakeSession($epoch), static fn (): SessionInterface => new FakeSession($epoch));
        $this->assertFalse($loop->reconnect(), 'the same epoch must compare as unchanged');
        $this->assertFalse($loop->lastEpochChanged());
    }

    /** A changed native-int epoch is also detected (the ordinary case). */
    public function testChangedIntEpochIsDetected(): void
    {
        $loop = self::loop(new FakeSession(100), static fn (): SessionInterface => new FakeSession(200));
        $this->assertTrue($loop->reconnect());
        $this->assertSame(200, $loop->currentEpoch());
    }

    // ---- bounded retry + backoff ----------------------------------------------------------------

    public function testReconnectRetriesUpToMaxAttemptsThenSucceeds(): void
    {
        $attempts = 0;
        $factory = static function () use (&$attempts): SessionInterface {
            $attempts++;
            if ($attempts < 3) {
                throw new ConnectionLostException("attempt {$attempts} failed");
            }
            return new FakeSession(2); // succeed on the 3rd
        };
        $loop = self::loop(new FakeSession(1), $factory, maxAttempts: 3);
        $this->assertTrue($loop->reconnect(), 'epoch 1 → 2 is a change');
        $this->assertSame(3, $attempts);
        $this->assertSame(1, $loop->reconnectCount());
    }

    public function testReconnectThrowsAfterExhaustingAttempts(): void
    {
        $factory = static function (): SessionInterface {
            throw new ConnectionLostException('still down');
        };
        $loop = self::loop(new FakeSession(1), $factory, maxAttempts: 2);
        $this->expectException(ConnectionLostException::class);
        $loop->reconnect();
    }

    /** Backoff sleep is invoked once per attempt (bounded), via the injected seam. */
    public function testBackoffSleepInvokedPerAttempt(): void
    {
        $slept = [];
        $backoff = new Backoff(0.01, 0.1, rng: static fn (): float => 0.5, sleep: static function (float $s) use (&$slept): void {
            $slept[] = $s;
        });
        $attempts = 0;
        $factory = static function () use (&$attempts): SessionInterface {
            $attempts++;
            if ($attempts < 3) {
                throw new ConnectionLostException('down');
            }
            return new FakeSession(2);
        };
        $loop = new ReconnectLoop(new FakeSession(1), $factory, $backoff, 3);
        $loop->reconnect();
        $this->assertCount(3, $slept, 'one bounded backoff per attempt');
        foreach ($slept as $s) {
            $this->assertGreaterThanOrEqual(0.0, $s);
            $this->assertLessThanOrEqual(0.1 + 1e-9, $s);
        }
    }

    /** A HandshakeException (registry/version mismatch) during reconnect is FATAL — rethrown at once. */
    public function testHandshakeRejectionIsFatalNotRetried(): void
    {
        $ep = new ErrorPayload(C::ERR_UNSUPPORTED, C::ERR_UNSUPPORTED_BRANCH, null, null, 'registry mismatch', null, null);
        $tries = 0;
        $factory = static function () use (&$tries, $ep): SessionInterface {
            $tries++;
            throw new HandshakeException($ep);
        };
        $loop = self::loop(new FakeSession(1), $factory, maxAttempts: 5);
        try {
            $loop->reconnect();
            $this->fail('expected HandshakeException');
        } catch (HandshakeException) {
            $this->assertSame(1, $tries, 'a fatal handshake rejection must not be retried');
        }
    }
}
