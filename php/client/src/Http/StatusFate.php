<?php // /php/client/src/Http/StatusFate.php
declare(strict_types=1);
namespace Ferro\Http;

/**
 * The ADVISORY verdict on an HTTP status (SPEC §23.7.4): a completed exchange is a success to the
 * engine whatever its status, and what a status means for a retry is the application's — this helper
 * is the one table Ferro ships for it, fed by the engine's effective idempotency
 * ({@see HttpResponse::$idempotent}, `HttpHead.idempotent`).
 *
 * | Status                                              | Idempotent              | Non-idempotent |
 * |-----------------------------------------------------|-------------------------|----------------|
 * | 1xx–3xx                                             | not a failure           | not a failure  |
 * | 408, 425                                            | Retryable               | Retryable      |
 * | 429; 503 with `Retry-After`                         | Retryable, delay = `Retry-After` | same  |
 * | other 4xx; 501, 505                                 | NonRetryable            | NonRetryable   |
 * | 500, 502, 504, 503 without `Retry-After`, other 5xx | Retryable               | **Indeterminate** |
 *
 * A non-idempotent 5xx is Indeterminate because an intermediary may have forwarded the request, and a
 * 500 promises nothing about partial application. A `Retry-After` that is neither delay-seconds nor
 * an HTTP-date (RFC 9110 §10.2.3) is treated as absent, which can only make a 503 MORE cautious.
 */
final class StatusFate
{
    private function __construct(
        public readonly FateClass $fate,
        /** The delay `Retry-After` asks for, in ms, on a Retryable 429/503 that carried one; else null. */
        public readonly ?int $retryAfterMs,
    ) {}

    /**
     * @param int $status a final or informational status, 100..=599
     * @param bool $idempotent the request's EFFECTIVE idempotency (`HttpHead.idempotent`)
     * @param ?string $retryAfter the response's `Retry-After` value, if any
     * @param ?float $now the clock an HTTP-date is measured against (Unix seconds); defaults to now
     * @throws \InvalidArgumentException for a status outside 100..=599
     */
    public static function of(int $status, bool $idempotent, ?string $retryAfter = null, ?float $now = null): self
    {
        if ($status < 100 || $status > 599) {
            throw new \InvalidArgumentException("not an HTTP status: {$status}");
        }
        if ($status < 400) {
            return new self(FateClass::NotAFailure, null);
        }
        if ($status === 408 || $status === 425) {
            return new self(FateClass::Retryable, null);
        }
        $delay = $retryAfter === null ? null : self::retryAfterMs($retryAfter, $now ?? microtime(true));
        if ($status === 429 || ($status === 503 && $delay !== null)) {
            return new self(FateClass::Retryable, $delay);
        }
        if ($status < 500 || $status === 501 || $status === 505) {
            return new self(FateClass::NonRetryable, null);
        }
        return new self($idempotent ? FateClass::Retryable : FateClass::Indeterminate, null);
    }

    /** A `Retry-After` value in ms (0 for a date already past), or null when it is not one. */
    private static function retryAfterMs(string $value, float $now): ?int
    {
        $value = trim($value, " \t");
        if (preg_match('/^[0-9]+$/D', $value) === 1) {
            // Thirteen digits of seconds is already past any delay a caller can wait for.
            return strlen($value) > 12 ? 999_999_999_999_000 : (int) $value * 1000;
        }
        $utc = new \DateTimeZone('UTC');
        // IMF-fixdate first; RFC 850 and asctime are the obsolete forms a recipient must accept.
        foreach (['D, d M Y H:i:s \G\M\T', 'l, d-M-y H:i:s \G\M\T', 'D M j H:i:s Y'] as $format) {
            $at = \DateTimeImmutable::createFromFormat('!' . $format, preg_replace('/ +/', ' ', $value) ?? $value, $utc);
            $errors = \DateTimeImmutable::getLastErrors();
            if ($at !== false && ($errors === false || ($errors['warning_count'] === 0 && $errors['error_count'] === 0))) {
                return max(0, (int) round(((float) $at->format('U.u') - $now) * 1000));
            }
        }
        return null;
    }
}
