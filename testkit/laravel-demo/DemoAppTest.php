<?php

namespace Illuminate\Tests\Integration\Database\FerroDemo;

use Illuminate\Auth\Notifications\ResetPassword;
use Illuminate\Bus\BatchRepository;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Notification;
use Illuminate\Support\Facades\Password;
use Illuminate\Tests\Integration\Database\DatabaseTestCase;
use Orchestra\Testbench\Attributes\WithMigration;

use function Orchestra\Testbench\load_migration_paths;

/**
 * **The SPEC §15 demo app** (M2): a stock Laravel 11 application whose framework subsystems all
 * persist through ONE database connection — auth with database-backed sessions, password reset
 * tokens, the `database` queue (jobs, failed jobs, batches) and the `database` cache store.
 *
 * **"Only the config diff" is proven by construction, not asserted in prose.** This file is the
 * same for every column. WHICH connection it runs on is decided in exactly one place — the patched
 * `DatabaseTestCase::defineEnvironment()` the framework suite already uses — and it differs between
 * the Ferro column and the stock-PDO control ONLY in the `database.connections.<name>` entry: the
 * driver name and `ferro_socket`/`pool` versus PDO's host/credentials. Every subsystem below is
 * pointed at "the default connection" and {@see testEverySubsystemIsOnTheConnectionUnderTest} fails
 * the run if any of them would quietly fall back to testbench's own SQLite.
 *
 * Horizon is not in it, and that is a finding rather than an omission: every Horizon repository is
 * Redis (`RedisJobRepository`, `RedisMetricsRepository`, …), so its "DB metrics" have no database
 * workload at all. Its one database read is the batches screen, which calls
 * {@see BatchRepository::get()}/`find()` — exercised directly in {@see testQueueBatches}.
 *
 * Requests are real HTTP requests through the `web` middleware group (cookies encrypted, session
 * started and saved by `StartSession`). Between requests {@see fresh()} drops every in-memory
 * session store and auth guard, so state can only survive by round-tripping through the database —
 * otherwise a test client sharing one application would pass this with sessions never stored.
 */
#[WithMigration('laravel', 'cache', 'queue', 'session')]
class DemoAppTest extends DatabaseTestCase
{
    protected function defineEnvironment($app)
    {
        parent::defineEnvironment($app);

        $default = $app['config']->get('database.default');
        $app['config']->set([
            'app.key' => 'base64:' . base64_encode(str_repeat('f', 32)),
            'auth.providers.users.model' => DemoUser::class,
            'session.driver' => 'database',
            'session.connection' => $default,
            'session.table' => 'sessions',
            'auth.passwords.users.table' => 'password_reset_tokens',
            'auth.passwords.users.connection' => $default,
            'queue.default' => 'database',
            'queue.connections.database.connection' => $default,
            'queue.connections.database.table' => 'jobs',
            'queue.connections.database.after_commit' => false,
            'queue.batching.database' => $default,
            'queue.batching.table' => 'job_batches',
            'queue.failed.driver' => 'database-uuids',
            'queue.failed.database' => $default,
            'queue.failed.table' => 'failed_jobs',
            'cache.default' => 'database',
            'cache.stores.database.connection' => $default,
            'cache.stores.database.lock_connection' => $default,
            'mail.default' => 'array',
        ]);

        // Registered on the migrator rather than run (`loadMigrationsFrom()` RUNS them, and rolls
        // back at teardown, racing `DatabaseMigrations`' own `migrate:fresh`).
        load_migration_paths($app, [__DIR__ . '/migrations']);
    }

    protected function defineRoutes($router)
    {
        $router->middleware('web')->group(function ($router) {
            $router->post('/register', function (Request $request) {
                $data = $request->validate([
                    'name' => 'required|string',
                    'email' => 'required|email|unique:users,email',
                    'password' => 'required|string|min:8',
                ]);
                $user = DemoUser::create(['password' => Hash::make($data['password'])] + $data);
                Auth::login($user);
                $request->session()->regenerate();

                return response()->json(['id' => $user->getKey()], 201);
            });

            $router->post('/login', function (Request $request) {
                $credentials = $request->validate(['email' => 'required|email', 'password' => 'required']);
                if (! Auth::attempt($credentials)) {
                    return response()->json(['message' => 'invalid credentials'], 422);
                }
                $request->session()->regenerate();

                return response()->json(['ok' => true]);
            })->name('login');

            $router->get('/dashboard', function (Request $request) {
                return response()->json(['email' => $request->user()->email]);
            })->middleware('auth');

            $router->post('/logout', function (Request $request) {
                Auth::guard('web')->logout();
                $request->session()->invalidate();
                $request->session()->regenerateToken();

                return response()->json(['ok' => true]);
            })->middleware('auth');

            $router->post('/forgot-password', function (Request $request) {
                $status = Password::sendResetLink($request->only('email'));

                return response()->json(['status' => $status], $status === Password::RESET_LINK_SENT ? 200 : 422);
            });

            $router->post('/reset-password', function (Request $request) {
                $status = Password::reset(
                    $request->only('email', 'password', 'password_confirmation', 'token'),
                    function (DemoUser $user, string $password) {
                        $user->forceFill(['password' => Hash::make($password)])->save();
                    },
                );

                return response()->json(['status' => $status], $status === Password::PASSWORD_RESET ? 200 : 422);
            });
        });
    }

    public function testEverySubsystemIsOnTheConnectionUnderTest(): void
    {
        $default = config('database.default');
        $this->assertSame($this->driver, DB::connection()->getConfig('driver'));

        // Every database-backed subsystem resolves THIS connection, by name — none falls through to
        // testbench's in-memory SQLite, which would make the demo pass without touching the engine.
        $this->assertSame($default, config('session.connection'));
        $this->assertSame($default, config('auth.passwords.users.connection'));
        $this->assertSame($default, config('queue.connections.database.connection'));
        $this->assertSame($default, config('queue.batching.database'));
        $this->assertSame($default, config('queue.failed.database'));
        $this->assertSame($default, config('cache.stores.database.connection'));
        $this->assertSame($default, config('cache.stores.database.lock_connection'));

        // The Ferro columns run a Ferro connection class and the control does not — the same
        // two-way check `bootstrap.php` makes before any test, repeated from inside the app.
        $isFerro = str_starts_with(DB::connection()::class, 'Ferro\\Laravel\\');
        $this->assertSame(! str_starts_with((string) getenv('FERRO_LARAVEL_DRIVER'), 'stock-'), $isFerro, DB::connection()::class);
    }

    public function testRegisterLoginLogoutWithDatabaseSessions(): void
    {
        $cookie = config('session.cookie');

        $register = $this->postJsonish('/register', [
            'name' => 'Ada', 'email' => 'ada@example.test', 'password' => 'correct horse',
        ]);
        $register->assertStatus(201);
        $sid = $register->getCookie($cookie)->getValue();
        $userId = $register->json('id');

        // The session is a ROW, owned by the user, written by `DatabaseSessionHandler` at the end
        // of the request.
        $this->assertDatabaseHas('sessions', ['id' => $sid, 'user_id' => $userId]);

        // A second request finds the user ONLY through that row.
        $this->fresh();
        $this->getJsonish('/dashboard', $sid)->assertOk()->assertJson(['email' => 'ada@example.test']);
        $this->fresh();
        $this->getJsonish('/dashboard', null)->assertUnauthorized();

        // Duplicate registration is refused by the `unique` validation rule's query.
        $this->fresh();
        $this->postJsonish('/register', [
            'name' => 'Ada 2', 'email' => 'ada@example.test', 'password' => 'correct horse',
        ])->assertStatus(422);

        // Logout invalidates: the row goes, and the old cookie no longer authenticates.
        $this->fresh();
        $this->postJsonish('/logout', [], $sid)->assertOk();
        $this->assertDatabaseMissing('sessions', ['id' => $sid]);
        $this->fresh();
        $this->getJsonish('/dashboard', $sid)->assertUnauthorized();

        // Login checks the stored hash, and a fresh session row carries the user.
        $this->fresh();
        $this->postJsonish('/login', ['email' => 'ada@example.test', 'password' => 'wrong password'])->assertStatus(422);
        $this->fresh();
        $login = $this->postJsonish('/login', ['email' => 'ada@example.test', 'password' => 'correct horse']);
        $login->assertOk();
        $sid2 = $login->getCookie($cookie)->getValue();
        $this->assertNotSame($sid, $sid2);
        $this->assertDatabaseHas('sessions', ['id' => $sid2, 'user_id' => $userId]);
        $this->fresh();
        $this->getJsonish('/dashboard', $sid2)->assertOk()->assertJson(['email' => 'ada@example.test']);
    }

    public function testPasswordResetThroughTheTokenTable(): void
    {
        DemoUser::create(['name' => 'Bob', 'email' => 'bob@example.test', 'password' => Hash::make('old password')]);
        Notification::fake();

        $this->postJsonish('/forgot-password', ['email' => 'bob@example.test'])->assertOk();
        $this->assertDatabaseHas('password_reset_tokens', ['email' => 'bob@example.test']);

        $token = null;
        Notification::assertSentTo(DemoUser::where('email', 'bob@example.test')->first(), ResetPassword::class,
            function (ResetPassword $n) use (&$token) {
                $token = $n->token;

                return true;
            });
        $this->assertIsString($token);

        // The broker throttles a second request inside a minute — by reading the token row's
        // `created_at` back through the connection.
        $this->fresh();
        $this->postJsonish('/forgot-password', ['email' => 'bob@example.test'])->assertStatus(422);

        $this->fresh();
        $this->postJsonish('/reset-password', [
            'email' => 'bob@example.test', 'token' => 'not-the-token',
            'password' => 'new password!', 'password_confirmation' => 'new password!',
        ])->assertStatus(422);
        $this->fresh();
        $this->postJsonish('/reset-password', [
            'email' => 'bob@example.test', 'token' => $token,
            'password' => 'new password!', 'password_confirmation' => 'new password!',
        ])->assertOk();
        $this->assertDatabaseMissing('password_reset_tokens', ['email' => 'bob@example.test']);

        $this->fresh();
        $this->postJsonish('/login', ['email' => 'bob@example.test', 'password' => 'old password'])->assertStatus(422);
        $this->fresh();
        $this->postJsonish('/login', ['email' => 'bob@example.test', 'password' => 'new password!'])->assertOk();
    }

    public function testDatabaseQueueRunsAndFailsJobs(): void
    {
        RecordEvent::dispatch('queued-1');
        RecordEvent::dispatch('queued-2');
        AlwaysFails::dispatch();
        $this->assertSame(3, DB::table('jobs')->count());

        $this->work();

        $this->assertSame(0, DB::table('jobs')->count());
        $this->assertSame(['queued-1', 'queued-2'], DB::table('demo_events')->orderBy('id')->pluck('name')->all());

        $failed = DB::table('failed_jobs')->get();
        $this->assertCount(1, $failed);
        $this->assertStringContainsString('this job fails on purpose', $failed[0]->exception);
        $this->assertSame(AlwaysFails::class, json_decode($failed[0]->payload, true)['displayName']);

        // `queue:retry` reads the failed job back by UUID and re-queues it.
        $this->artisan('queue:retry', ['id' => [$failed[0]->uuid]])->assertSuccessful();
        $this->assertSame(1, DB::table('jobs')->count());
        $this->assertSame(0, DB::table('failed_jobs')->count());
    }

    public function testQueueBatches(): void
    {
        $batch = Bus::batch([
            new RecordEvent('batch-a'),
            new RecordEvent('batch-b'),
            new RecordEvent('batch-c'),
        ])->name('ferro-demo')->then(static function () {
            DB::table('demo_events')->insert(['name' => 'batch-then']);
        })->dispatch();

        $this->assertDatabaseHas('job_batches', ['id' => $batch->id, 'total_jobs' => 3, 'pending_jobs' => 3]);

        $this->work();

        // The worker decremented the counters inside `DatabaseBatchRepository`'s locking
        // transactions, and the `then` callback ran once, when the last job finished.
        $fresh = $batch->fresh();
        $this->assertTrue($fresh->finished());
        $this->assertSame(0, $fresh->pendingJobs);
        $this->assertSame(0, $fresh->failedJobs);
        $this->assertSame(
            ['batch-a', 'batch-b', 'batch-c', 'batch-then'],
            DB::table('demo_events')->orderBy('id')->pluck('name')->all(),
        );

        // Horizon's batches screen: `BatchesController::index()` / `show()` call exactly these.
        $repository = $this->app->make(BatchRepository::class);
        $listed = $repository->get(50, null);
        $this->assertCount(1, $listed);
        $this->assertSame('ferro-demo', $listed[0]->name);
        $this->assertSame($batch->id, $repository->find($batch->id)->id);
    }

    public function testDatabaseCacheStoreAndLocks(): void
    {
        $cache = Cache::store('database');

        $this->assertTrue($cache->put('greeting', ['hello' => 'world'], 60));
        $this->assertSame(['hello' => 'world'], $cache->get('greeting'));
        $this->assertFalse($cache->add('greeting', 'second', 60), 'add() must not overwrite');
        $this->assertTrue($cache->add('counter', 1, 60));
        $this->assertSame(2, $cache->increment('counter'));
        $this->assertSame(['hello' => 'world'], $cache->get('greeting'));
        $this->assertTrue($cache->forget('greeting'));
        $this->assertNull($cache->get('greeting'));

        // Atomic locks: the second acquire collides on the primary key and must report "held",
        // not throw — `DatabaseLock` relies on the INSERT failing cleanly outside a transaction.
        $first = $cache->lock('report', 10);
        $this->assertTrue($first->get());
        $this->assertFalse($cache->lock('report', 10)->get());
        $first->release();
        $this->assertTrue($cache->lock('report', 10)->get());
    }

    /** Drop every in-memory session store and auth guard, so only the database carries state. */
    private function fresh(): void
    {
        $this->app['session']->forgetDrivers();
        // `session.store` is a container SINGLETON the session guard is built from; left alone it
        // keeps the first request's store (and its attributes) for the whole test.
        $this->app->forgetInstance('session.store');
        $this->app['auth']->forgetGuards();
        $this->defaultCookies = [];
    }

    private function getJsonish(string $uri, ?string $sid)
    {
        if ($sid !== null) {
            $this->withCookie(config('session.cookie'), $sid);
        }

        return $this->get($uri, ['Accept' => 'application/json']);
    }

    private function postJsonish(string $uri, array $data, ?string $sid = null)
    {
        if ($sid !== null) {
            $this->withCookie(config('session.cookie'), $sid);
        }

        return $this->post($uri, $data, ['Accept' => 'application/json']);
    }

    /** Run the `database` queue's worker in-process until the queue is empty. */
    private function work(): void
    {
        $this->artisan('queue:work', ['connection' => 'database', '--stop-when-empty' => true, '--sleep' => 0])
            ->assertSuccessful();
    }
}
