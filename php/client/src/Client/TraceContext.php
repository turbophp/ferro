<?php // /php/client/src/Client/TraceContext.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * The application's W3C trace context, sent with every EXEC (M2-C4c-1, SPEC §13).
 *
 * `ferrod` attaches the caller's `traceparent` to its per-statement observability, so a slow
 * statement or (C4c-2) an engine span can be opened from the application request that issued it.
 * This class is how the application's tracer hands that header to the client:
 *
 * ```php
 * use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
 *
 * \Ferro\Client\TraceContext::useProvider(static function (): ?string {
 *     $carrier = [];
 *     TraceContextPropagator::getInstance()->inject($carrier);
 *     return $carrier['traceparent'] ?? null;
 * });
 * ```
 *
 * **A provider, not a value.** The provider is called when each EXEC is ENCODED, which happens in
 * the fiber that issued the statement. A tracer whose current context is per-fiber (OpenTelemetry
 * PHP's is) therefore gives each statement its own span's context, with no plumbing through Ferro.
 *
 * **Process-wide, not per connection.** Both drop-in tiers create clients the application never
 * sees, and Illuminate replaces them on every reconnect (§22.2 (bw)). Per-connection state would be
 * lost at exactly the moments it is hardest to notice.
 *
 * **Tracing never fails a statement.** A provider that throws, returns a non-string, returns an
 * empty string or returns more than {@see MAX_LENGTH} bytes sends NO context, and the statement
 * runs. A provider that issues a Ferro statement itself sends none for that inner statement rather
 * than recursing. The client does not validate the header's grammar; the engine does, and it counts
 * every malformed value in `ferro_traceparent_invalid_total`, so a broken provider is visible to an
 * operator rather than silently corrected here.
 */
final class TraceContext
{
    /**
     * The longest header sent. A version-00 `traceparent` is 55 bytes; later W3C versions may append
     * fields, so the cap is generous, but bounded, so a runaway provider cannot inflate every frame.
     */
    public const MAX_LENGTH = 512;

    /** @var (\Closure(): mixed)|null */
    private static ?\Closure $provider = null;

    private static bool $calling = false;

    /** Install the provider (or remove it with `null`). Replaces any earlier one. */
    public static function useProvider(?\Closure $provider): void
    {
        self::$provider = $provider;
    }

    /** The header to send with the EXEC being encoded now, or `null` to send none. Never throws. */
    public static function current(): ?string
    {
        $provider = self::$provider;
        if ($provider === null || self::$calling) {
            return null;
        }
        self::$calling = true;
        try {
            $value = $provider();
        } catch (\Throwable) {
            return null;
        } finally {
            self::$calling = false;
        }
        if (!is_string($value) || $value === '' || strlen($value) > self::MAX_LENGTH) {
            return null;
        }
        return $value;
    }
}
