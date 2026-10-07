<?php // /php/laravel/src/Http/HttpWiring.php
declare(strict_types=1);
namespace Ferro\Laravel\Http;

use Ferro\Client\Connection;
use Ferro\Ferro;
use Ferro\Guzzle\FerroHandler;
use Ferro\Http\Adapter\ConnectionProvider;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Http\Client\Factory;

/**
 * The Laravel `Http` facade's wiring to Ferro HTTP (SPEC §23.11.6), run by
 * {@see \Ferro\Laravel\FerroServiceProvider} when — and only when — the application configures an
 * `http.ferro` block:
 *
 *     // config/http.php
 *     return ['ferro' => [
 *         'socket'    => '/run/ferro/app.sock',
 *         'upstreams' => ['https://api.openai.com' => 'openai'],   // origin => upstream name
 *         // optional: 'pool' => 'default', 'io_timeout' => 5.0, 'connect_timeout' => 2.0
 *     ]];
 *
 * It rebinds the container's `Illuminate\Http\Client\Factory` singleton to a
 * {@see FerroHttpFactory} whose pending requests carry `Ferro\Guzzle\FerroHandler`. The connection to
 * `ferrod` is opened on the first request that actually reaches the handler.
 *
 * **Fragility, stated (§23.11.6):** the seam is a PROTECTED framework method,
 * `Factory::newPendingRequest()`. A Laravel release that renames it would leave the override
 * dead and route every request around Ferro silently; instead this refuses to wire, loudly, at
 * boot. Likewise a missing `ferro/guzzle` or `illuminate/http` is a configuration error, never a
 * silent no-op.
 */
final class HttpWiring
{
    private function __construct() {}

    public static function register(Container $app): void
    {
        if (!$app->bound('config')) {
            return;
        }
        $config = $app->make('config');
        if (!is_object($config) || !method_exists($config, 'get')) {
            return;
        }
        $ferro = $config->get('http.ferro');
        if ($ferro === null) {
            return;
        }
        $settings = self::settings($ferro);
        self::assertSeam();
        $connect = self::connectArguments($settings);

        $app->singleton(Factory::class, static function (Container $app) use ($settings, $connect): Factory {
            $events = $app->bound(Dispatcher::class) ? $app->make(Dispatcher::class) : null;
            // ONE connection for every upstream this worker calls (review F-B): memoised here, and
            // again by the handler.
            $connection = ConnectionProvider::memoise(static fn (): Connection => Ferro::connect(...$connect));
            return new FerroHttpFactory(
                static fn (): FerroHandler => new FerroHandler($connection, $settings['upstreams']),
                $events instanceof Dispatcher ? $events : null,
            );
        });
    }

    /**
     * The `Ferro::connect()` arguments an `http.ferro` block names, BY PARAMETER NAME — so a swapped
     * timeout or an ignored `pool` cannot pass for wiring (both survived the review's mutation round
     * while the call was positional).
     *
     * @param array{socket: string, pool: string, upstreams: array<string, string>, io_timeout: float, connect_timeout: float} $settings
     * @return array{socketPath: string, pool: string, connectTimeout: float, ioTimeout: float}
     */
    public static function connectArguments(array $settings): array
    {
        return [
            'socketPath' => $settings['socket'],
            'pool' => $settings['pool'],
            'connectTimeout' => $settings['connect_timeout'],
            'ioTimeout' => $settings['io_timeout'],
        ];
    }

    /**
     * Refuse to wire when the seam is not there to override. `$factory` is the class whose seam is
     * checked — `Illuminate\Http\Client\Factory` in production; a test passes a stand-in to prove
     * the refusal.
     *
     * @throws \LogicException
     */
    public static function assertSeam(string $factory = Factory::class): void
    {
        if (!class_exists($factory)) {
            throw new \LogicException('http.ferro is configured, but illuminate/http is not installed');
        }
        if (!class_exists(FerroHandler::class)) {
            throw new \LogicException('http.ferro is configured, but ferro/guzzle is not installed (composer require ferro/guzzle)');
        }
        $seam = new \ReflectionClass($factory);
        if (!$seam->hasMethod('newPendingRequest') || $seam->getMethod('newPendingRequest')->getDeclaringClass()->getName() !== $factory) {
            throw new \LogicException(
                'http.ferro is configured, but this Laravel version\'s Illuminate\\Http\\Client\\Factory has no '
                    . 'newPendingRequest() for Ferro to override (SPEC §23.11.6); refusing to wire rather than '
                    . 'route requests around Ferro',
            );
        }
    }

    /**
     * @return array{socket: string, pool: string, upstreams: array<string, string>, io_timeout: float, connect_timeout: float}
     */
    public static function settings(mixed $ferro): array
    {
        if (!is_array($ferro)) {
            throw new \InvalidArgumentException('http.ferro must be an array');
        }
        $socket = $ferro['socket'] ?? null;
        if (!is_string($socket) || $socket === '') {
            throw new \InvalidArgumentException('http.ferro.socket must name the ferrod socket');
        }
        $upstreams = $ferro['upstreams'] ?? null;
        if (!is_array($upstreams) || $upstreams === []) {
            throw new \InvalidArgumentException('http.ferro.upstreams must map at least one origin to an upstream name');
        }
        $map = [];
        foreach ($upstreams as $origin => $name) {
            if (!is_string($origin) || !is_string($name)) {
                throw new \InvalidArgumentException('http.ferro.upstreams is origin (string) => upstream name (string)');
            }
            $map[$origin] = $name;
        }
        $pool = $ferro['pool'] ?? 'default';
        $io = $ferro['io_timeout'] ?? 5.0;
        $connect = $ferro['connect_timeout'] ?? 2.0;
        if (!is_string($pool) || !is_numeric($io) || !is_numeric($connect)) {
            throw new \InvalidArgumentException('http.ferro.pool must be a string and its timeouts numbers (seconds)');
        }
        return ['socket' => $socket, 'pool' => $pool, 'upstreams' => $map, 'io_timeout' => (float) $io, 'connect_timeout' => (float) $connect];
    }
}
