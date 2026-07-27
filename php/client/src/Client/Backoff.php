<?php // /php/client/src/Client/Backoff.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * Full-jitter exponential backoff (AWS "Exponential Backoff And Jitter"): the delay before retry
 * attempt `n` is a uniform random draw in `[0, cap]` where `cap = min(maxSeconds, base * 2^n)`.
 * Full jitter (0..cap, not cap/2..cap) spreads a thundering herd of reconnecting clients the widest,
 * which is what a just-restarted `ferrod` needs.
 *
 * Both the randomness and the sleep are INJECTABLE so tests are deterministic AND instant: pass a
 * seeded `$rng` (a `(): float` in `[0,1)`) to assert the jitter distribution, and a `$sleep`
 * (`(float $seconds): void`) spy to assert bounded waits without ever really sleeping. In production
 * both default to `mt_rand()`-derived jitter and `usleep`.
 */
final class Backoff
{
    /** @var \Closure(): float returns a float in [0,1) */
    private readonly \Closure $rng;
    /** @var \Closure(float): void sleeps for the given number of seconds */
    private readonly \Closure $sleep;

    /**
     * @param \Closure(): float|null      $rng   deterministic RNG seam; defaults to `mt_rand()/mt_getrandmax()`.
     * @param \Closure(float): void|null  $sleep sleep seam; defaults to `usleep()`.
     */
    public function __construct(
        private readonly float $baseSeconds = 0.05,
        private readonly float $maxSeconds = 2.0,
        ?\Closure $rng = null,
        ?\Closure $sleep = null,
    ) {
        $this->rng = $rng ?? static fn (): float => mt_rand() / (mt_getrandmax() + 1);
        $this->sleep = $sleep ?? static function (float $seconds): void {
            if ($seconds > 0.0) {
                usleep((int) round($seconds * 1_000_000));
            }
        };
    }

    /** The exponential cap for attempt `$attempt` (0-based), clamped to `maxSeconds` (INF-safe). */
    public function capFor(int $attempt): float
    {
        $attempt = max(0, $attempt);
        $exp = $this->baseSeconds * (2.0 ** $attempt); // 2.0**large ⇒ INF; min() clamps it to the cap
        return min($this->maxSeconds, $exp);
    }

    /**
     * The jittered delay for attempt `$attempt` (0-based): a uniform draw in `[0, capFor($attempt)]`.
     * Always within `[0, maxSeconds]` however large `$attempt` grows (bounded).
     */
    public function delayFor(int $attempt): float
    {
        $r = ($this->rng)();
        // Defend against a misbehaving RNG straying outside [0,1); the delay must stay within the cap.
        $r = max(0.0, min(1.0, $r));
        return $r * $this->capFor($attempt);
    }

    /** Sleep the jittered backoff for attempt `$attempt` (via the injected sleep seam). */
    public function sleepFor(int $attempt): void
    {
        ($this->sleep)($this->delayFor($attempt));
    }
}
