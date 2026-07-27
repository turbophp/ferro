<?php // /php/client/tests/Client/BackoffTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use PHPUnit\Framework\TestCase;

/**
 * Full-jitter exponential backoff: every delay stays in `[0, min(maxSeconds, base*2^n)]` (bounded),
 * varies under a seeded RNG (jittered), and is reproducible for a given seed (deterministic-in-test).
 */
final class BackoffTest extends TestCase
{
    /**
     * A seeded RNG closure yielding floats in [0,1). A self-contained LCG over a per-closure `$state`
     * (captured by reference) — independent of PHP's global `mt_rand` state, so two closures seeded
     * alike produce IDENTICAL streams (the determinism assertion needs that independence).
     */
    private static function seededRng(int $seed): \Closure
    {
        $state = $seed & 0x7FFFFFFF;
        return static function () use (&$state): float {
            $state = ($state * 1103515245 + 12345) & 0x7FFFFFFF;
            return $state / 0x80000000; // ∈ [0, 1)
        };
    }

    public function testCapIsExponentialThenClampedToMax(): void
    {
        $b = new Backoff(baseSeconds: 0.05, maxSeconds: 2.0);
        $this->assertEqualsWithDelta(0.05, $b->capFor(0), 1e-9); // base * 2^0
        $this->assertEqualsWithDelta(0.10, $b->capFor(1), 1e-9); // base * 2^1
        $this->assertEqualsWithDelta(0.20, $b->capFor(2), 1e-9); // base * 2^2
        $this->assertSame(2.0, $b->capFor(50)); // clamped to maxSeconds (INF-safe)
    }

    public function testDelayIsBoundedByTheCapForEveryAttempt(): void
    {
        $b = new Backoff(baseSeconds: 0.05, maxSeconds: 2.0, rng: self::seededRng(42));
        for ($attempt = 0; $attempt < 40; $attempt++) {
            $d = $b->delayFor($attempt);
            $this->assertGreaterThanOrEqual(0.0, $d);
            $this->assertLessThanOrEqual($b->capFor($attempt) + 1e-9, $d, "attempt {$attempt} exceeded its cap");
            $this->assertLessThanOrEqual(2.0 + 1e-9, $d, 'no delay may exceed maxSeconds');
        }
    }

    public function testDelayIsJittered(): void
    {
        // Same attempt number, successive draws from a seeded RNG → distinct (jittered, not fixed).
        $b = new Backoff(baseSeconds: 0.05, maxSeconds: 2.0, rng: self::seededRng(7));
        $draws = [];
        for ($i = 0; $i < 8; $i++) {
            $draws[] = $b->delayFor(5); // a big-enough cap that jitter has room to vary
        }
        $this->assertGreaterThan(1, count(array_unique($draws)), 'delays must vary (full jitter)');
    }

    public function testDeterministicForAGivenSeed(): void
    {
        $mk = fn (): Backoff => new Backoff(0.05, 2.0, self::seededRng(123));
        $a = $mk();
        $b = $mk();
        for ($attempt = 0; $attempt < 10; $attempt++) {
            $this->assertSame($a->delayFor($attempt), $b->delayFor($attempt));
        }
    }

    public function testFullJitterBounds(): void
    {
        // rng=1.0 → delay hits the cap (upper edge); rng=0.0 → delay is exactly 0 (lower edge).
        $atCap = new Backoff(0.05, 2.0, rng: static fn (): float => 1.0);
        $this->assertEqualsWithDelta($atCap->capFor(3), $atCap->delayFor(3), 1e-9);

        $atZero = new Backoff(0.05, 2.0, rng: static fn (): float => 0.0);
        $this->assertSame(0.0, $atZero->delayFor(3));
    }

    public function testSleepUsesInjectedSeamWithinBounds(): void
    {
        $slept = [];
        $b = new Backoff(
            baseSeconds: 0.05,
            maxSeconds: 2.0,
            rng: self::seededRng(9),
            sleep: static function (float $s) use (&$slept): void { $slept[] = $s; },
        );
        $b->sleepFor(2);
        $this->assertCount(1, $slept);
        $this->assertGreaterThanOrEqual(0.0, $slept[0]);
        $this->assertLessThanOrEqual($b->capFor(2) + 1e-9, $slept[0]);
    }
}
