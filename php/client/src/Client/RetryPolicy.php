<?php // /php/client/src/Client/RetryPolicy.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * The client's resilience budget (SPEC §19): how many total attempts a transparently-recoverable op
 * (a Retryable READ, or a whole `transaction` closure whose tx died) may take, whether reads are
 * retried at all (`retryReads`, default true), and the full-jitter backoff bounds between attempts.
 *
 * `maxAttempts` counts the FIRST try plus retries — `maxAttempts = 1` means "never retry". It never
 * licenses retrying an `Indeterminate`/lost-COMMIT/write; that ban lives in {@see FateClassifier} and
 * no policy value can override it (§19.3).
 */
final class RetryPolicy
{
    public function __construct(
        public readonly bool $retryReads = true,
        public readonly int $maxAttempts = 3,
        public readonly float $baseDelaySeconds = 0.05,
        public readonly float $maxDelaySeconds = 2.0,
    ) {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException("maxAttempts must be >= 1, got {$maxAttempts}");
        }
        if ($baseDelaySeconds < 0.0 || $maxDelaySeconds < 0.0) {
            throw new \InvalidArgumentException('backoff delays must be non-negative');
        }
    }

    /** The M0 default: retry reads, up to 3 attempts, 50 ms base / 2 s cap. */
    public static function default(): self
    {
        return new self();
    }

    /** A policy that never retries anything (first attempt only). */
    public static function none(): self
    {
        return new self(retryReads: false, maxAttempts: 1);
    }

    /** Build the {@see Backoff} this policy's bounds describe, optionally with injected RNG/sleep. */
    public function backoff(?\Closure $rng = null, ?\Closure $sleep = null): Backoff
    {
        return new Backoff($this->baseDelaySeconds, $this->maxDelaySeconds, $rng, $sleep);
    }
}
