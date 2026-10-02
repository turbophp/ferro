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
 * the fiber that issued the statement. A tracer whose current context is per-fiber therefore gives
 * each statement its own span's context, with no plumbing through Ferro. (OpenTelemetry PHP's
 * context is per-fiber once its fiber support is initialised; without it, its storage warns in a
 * fiber whose context was never set up, and if the application turns warnings into exceptions the
 * provider throws — which sends no context, and the statement still runs.)
 *
 * **Process-wide, not per connection.** Both drop-in tiers create clients the application never
 * sees, and Illuminate replaces them on every reconnect (§22.2 (bw)). Per-connection state would be
 * lost at exactly the moments it is hardest to notice.
 *
 * **Tracing never fails a statement.** A provider that throws, returns a non-string, an empty
 * string, more than {@see MAX_LENGTH} bytes, or anything but printable ASCII sends NO context, and
 * the statement runs. ASCII matters: the W3C header is ASCII-only, and before the C4c-1 review a
 * single byte >= 0x80 — reachable by any external caller when a provider forwards an inbound HTTP
 * `traceparent` verbatim — made the engine refuse the whole request. (The engine now tolerates such
 * a byte too, but no client should rely on that.) A provider that reaches the client again from
 * inside itself gets no context for that inner statement rather than recursing; the guard is held
 * PER FIBER, so a provider that suspends its fiber does not blank anyone else's context. A context
 * that would push a statement over the 16 MiB frame cap is dropped by the encoder rather than fail
 * the statement. Beyond that the client does not validate the header's grammar; the engine does,
 * and it counts every malformed value in `ferro_traceparent_invalid_total`, so a broken provider is
 * visible to an operator rather than silently corrected here.
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

    /** Whether the provider is running on the MAIN (non-fiber) stack. */
    private static bool $callingMain = false;

    /**
     * The fibers whose provider call is in progress. Weak, so a fiber that is destroyed while
     * parked inside its provider does not leak an entry.
     *
     * @var \WeakMap<object, true>|null
     */
    private static ?\WeakMap $callingFibers = null;

    /** Install the provider (or remove it with `null`). Replaces any earlier one. */
    public static function useProvider(?\Closure $provider): void
    {
        self::$provider = $provider;
    }

    /** The header to send with the EXEC being encoded now, or `null` to send none. Never throws. */
    public static function current(): ?string
    {
        $provider = self::$provider;
        if ($provider === null) {
            return null;
        }
        $fiber = \Fiber::getCurrent();
        if ($fiber === null) {
            if (self::$callingMain) {
                return null;
            }
            self::$callingMain = true;
        } else {
            self::$callingFibers ??= new \WeakMap();
            if (isset(self::$callingFibers[$fiber])) {
                return null;
            }
            self::$callingFibers[$fiber] = true;
        }
        try {
            $value = $provider();
        } catch (\Throwable) {
            return null;
        } finally {
            if ($fiber === null) {
                self::$callingMain = false;
            } else {
                unset(self::$callingFibers[$fiber]);
            }
        }
        if (
            !is_string($value)
            || strlen($value) > self::MAX_LENGTH
            || preg_match('/^[\x21-\x7e]+$/D', $value) !== 1
        ) {
            return null;
        }
        return $value;
    }
}
