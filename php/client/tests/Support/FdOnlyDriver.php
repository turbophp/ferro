<?php // /php/client/tests/Support/FdOnlyDriver.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

/**
 * A driver that sees a stream's readiness the way ev, uv and event do (M3-D1d review): only bytes
 * waiting in the KERNEL make it readable, never bytes PHP has already read into the stream's own
 * buffer. `StreamSelectDriver` reports both, so on it alone the adapter's buffer draining cannot be
 * observed.
 *
 * Each watched stream is selected through a TWIN: a second PHP stream on a `dup` of the same
 * descriptor (`php://fd/N`), whose PHP-side buffer stays empty. The callback still receives the
 * original stream. (`socket_export_stream(socket_import_stream($s))` is NOT a twin: it hands back
 * `$s` itself, buffer included — measured.) Linux only (`/proc/self/fd`).
 *
 * The adapter cannot see into this driver, so only `{main}` suspends under it.
 */
final class FdOnlyDriver extends DelegatingDriver
{
    /** @var array<string, resource> twins by callback id, closed when the callback is cancelled */
    private array $twins = [];

    public function onReadable(mixed $stream, \Closure $closure): string
    {
        $twin = self::twin($stream);
        $id = $this->inner->onReadable($twin, static fn (string $id) => $closure($id, $stream));
        $this->twins[$id] = $twin;
        return $id;
    }

    public function cancel(string $callbackId): void
    {
        $this->inner->cancel($callbackId);
        if (isset($this->twins[$callbackId])) {
            fclose($this->twins[$callbackId]); // a dup: the client's descriptor stays open
            unset($this->twins[$callbackId]);
        }
    }

    /** @return resource */
    private static function twin(mixed $stream)
    {
        $stat = is_resource($stream) ? fstat($stream) : false;
        if ($stat === false) {
            throw new \RuntimeException('FdOnlyDriver: not a stream');
        }
        foreach (scandir('/proc/self/fd') ?: [] as $n) {
            if (!ctype_digit($n)) {
                continue;
            }
            $s = @stat("/proc/self/fd/{$n}");
            if ($s !== false && $s['ino'] === $stat['ino'] && $s['dev'] === $stat['dev']) {
                $twin = @fopen("php://fd/{$n}", 'r');
                if ($twin !== false) {
                    return $twin;
                }
            }
        }
        throw new \RuntimeException('FdOnlyDriver: cannot find the stream\'s descriptor');
    }
}
