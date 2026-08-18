<?php // /php/client/tests/Live/DaemonKillFateLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Ferro;

/**
 * SPEC §20.3's kill-`ferrod` harness — the M1 exit bar's chaos part (M1-S9), and the FIRST test
 * anywhere in this repository in which the DAEMON dies mid-request. Every prior chaos suite
 * (`chaos_fate_it.rs`, `mysql_chaos_it.rs`, `in_tx_fate_it.rs`) kills the BACKEND LINK and the
 * daemon survives to classify; that is a different failure. The vantage here is the PHP client
 * through the §19.2 reconnect loop, because that is what a production caller actually observes.
 *
 * Cells (each kill is PROVEN in flight — the sidecar observes the marker in
 * `pg_stat_activity`/`information_schema.processlist` before killing; the stream cell is in flight
 * by construction, DATA frames already received):
 *   1.  autocommit non-readonly EXEC   → `IndeterminateException`, never re-issued, at most once.
 *   2.  in-transaction EXEC (plain DML) → NEVER `Indeterminate`; prefix proven unpersisted.
 *   2b. MySQL implicit-commit prefix    → MEASUREMENT pinning the client-side residual: the prefix
 *       IS durable (no COMMIT was ever sent) while the client cannot know (§22.2 (aq)).
 *   3.  readonly-declared read (pg, mysql) → NEVER `Indeterminate`; and once the daemon returns,
 *       its `boot_epoch` is asserted CHANGED (§19.1 — engine state void) from a FRESH connection.
 *       The plan asked for that epoch proof via `$conn`'s TRANSPARENT reconnect (§19.2); that path
 *       is BROKEN and is pinned below instead of being asserted here.
 *   4.  open stream (PG only — MySQL-family streaming is `Unsupported`, §22.2 (n)) → exactly one
 *       thrown terminal, never a hang, never a clean end that silently truncates.
 *
 * Plus TWO **pinned defects** this harness FOUND on its first live run — the §19.2 recovery half of
 * the plan's cells 3 and 4, which turned out to assert a property the client does not have. Both
 * are measured, both are reachable from ordinary caller code, both leave a `Connection`
 * PERMANENTLY dead after an ordinary `ferrod` restart, and both surface OUTSIDE the fate taxonomy
 * (a raw `TypeError`; a `ProtocolException`), so a caller's `catch (FerroException)` never sees
 * them. They are pinned rather than fixed because Task 3 changes no `php/client/src` — see each
 * pin's docblock, and `docs/followups/2026-08-13-client-recovery-unreachable-after-daemon-death.md`.
 *
 * **Why these cells do NOT use {@see LiveTestCase::connectConnection} (the plan's adversarial
 * verification found this, MEASURED).** That helper hardcodes a **5.0 s io timeout**;
 * `Transport::readExact` surfaces a read timeout as `TransportException`, `Connection`'s autocommit
 * path catches that IDENTICALLY to a daemon death, and `FateClassifier::classifyLoss(Write)` mints
 * the **same `IndeterminateException`**. A cell parking a 30 s sleep behind a 5 s io window would
 * race the killer (PHP CLI start + autoload + connect + 50 ms poll) against its own client timeout:
 * on a loaded box the client times out first, the assertion passes, and the daemon is still running
 * at classification time — while the killer then still finds its marker inside its budget, kills,
 * and exits 0, so nothing anywhere reports a problem. This is the acceptance harness for the
 * property the whole design exists to protect, so a cell that can pass for an unrelated reason is
 * not a weak test, it is a FALSE GATE. Two defences, both load-bearing:
 *   - {@see chaosConnect} uses an io timeout that EXCEEDS the parked sleep, so a client-side
 *     timeout is IMPOSSIBLE inside a cell and the only two exits are the kill (throw) or the
 *     statement completing ({@see self::fail}); the relation is ASSERTED in {@see setUp}, not
 *     merely commented;
 *   - {@see assertKillerObservedAndKilled} additionally proves the kill PRECEDED the
 *     classification, by comparing the sidecar's pre-signal timestamp against the instant the cell
 *     caught. The weaker form of this — "has the killer exited by now?" — was MEASURED
 *     insufficient (the killer catches up inside any usable grace), and with the ordering
 *     assertion removed a slow-killer mutation makes cell 1 report `OK` while certifying
 *     `Indeterminate` with ferrod alive for another 0.94 s.
 */
final class DaemonKillFateLiveTest extends LiveTestCase
{
    /** How long the watched statement parks server-side, in seconds. */
    private const SLEEP_SEC = 30;

    /**
     * The cells' io timeout. MUST exceed {@see SLEEP_SEC} — see the class docblock; without that
     * inequality every sidecar cell can pass on a CLIENT-side timeout with the daemon alive.
     */
    private const IO_TIMEOUT_SEC = 60.0;

    /** The sidecar's own budget for observing the marker in flight. Smaller than the sleep. */
    private const KILLER_BUDGET_SEC = '20';

    /** @var list<array{proc: resource, stamp: string}> killer handles + their kill-stamp files */
    private array $killers = [];

    protected function setUp(): void
    {
        parent::setUp();
        // The load-bearing inequality, asserted rather than commented (see the class docblock).
        self::assertGreaterThan(
            (float) self::SLEEP_SEC,
            self::IO_TIMEOUT_SEC,
            'the chaos io timeout must EXCEED the parked sleep, or a cell can classify a CLIENT-side '
                . 'read timeout as the daemon death and pass with ferrod still alive',
        );
    }

    protected function tearDown(): void
    {
        foreach ($this->killers as $killer) {
            if (is_resource($killer['proc'])) {
                @proc_terminate($killer['proc'], 9);
                @proc_close($killer['proc']);
            }
            @unlink($killer['stamp']);
        }
        $this->killers = [];
        parent::tearDown();
    }

    // ---- cell 1: autocommit write → Indeterminate, at most once --------------------------------

    public function testAutocommitWriteKilledMidFlightIsIndeterminateOnPg(): void
    {
        $this->runAutocommitWriteCell('pg', 'default');
    }

    public function testAutocommitWriteKilledMidFlightIsIndeterminateOnMysql(): void
    {
        $this->runAutocommitWriteCell('mysql', $this->requireMysqlPool());
    }

    private function runAutocommitWriteCell(string $family, string $pool): void
    {
        $conn = $this->chaosConnect($pool);
        $this->setupTable($conn, $family);
        $k = self::uniq('cell1');
        $marker = self::uniq('m1');

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        $caughtAt = 0.0;
        try {
            // readonly=false (exec's default) — OpKind::Write on the loss path.
            $conn->exec($this->sleepingInsertSql($family, $marker), [$k]);
            self::fail('the write completed — the killer never fired (see its log in sys_get_temp_dir())');
        } catch (IndeterminateException $e) {
            // Any OTHER class propagates and errors the test — that IS the guard: §19.3's
            // autocommit-write row is IndeterminateException, nothing softer.
            $caughtAt = microtime(true);
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled($caughtAt);
        self::assertInstanceOf(IndeterminateException::class, $caught);

        // The daemon returns; the classifier must not have "resolved" the fate by re-issuing
        // (charter rule 3, client half): at most one application, ever.
        $this->restartFerrod();
        $fresh = $this->connectConnection(pool: $pool);
        self::assertLessThanOrEqual(
            1,
            $this->countKey($fresh, $family, $k),
            'at-most-once violated: an Indeterminate write was re-applied',
        );
        $fresh->session()->close();
    }

    // ---- cell 2: in-tx plain DML → never Indeterminate, prefix unpersisted ---------------------

    public function testInTxStatementKilledMidFlightIsNeverIndeterminateOnPg(): void
    {
        $this->runInTxPlainDmlCell('pg', 'default');
    }

    public function testInTxStatementKilledMidFlightIsNeverIndeterminateOnMysql(): void
    {
        $this->runInTxPlainDmlCell('mysql', $this->requireMysqlPool());
    }

    private function runInTxPlainDmlCell(string $family, string $pool): void
    {
        $conn = $this->chaosConnect($pool);
        $this->setupTable($conn, $family);
        $k1 = self::uniq('cell2_prefix');
        $k2 = self::uniq('cell2_inflight');
        $marker = self::uniq('m2');

        $conn->begin();
        $conn->exec($this->plainInsertSql($family), [$k1]); // the prefix — must die with the tx

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        $caughtAt = 0.0;
        try {
            $conn->exec($this->sleepingInsertSql($family, $marker), [$k2]);
            self::fail('the in-tx statement completed — the killer never fired');
        } catch (FerroException $e) {
            $caughtAt = microtime(true);
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled($caughtAt);

        // THE §19.3 SAFETY FLOOR: the transaction died with the daemon, nothing persisted (proven
        // below) — an Indeterminate here would be a false alarm on the one branch that must never
        // cry wolf. What the class concretely IS today (a RAW ConnectionLost/Transport —
        // TxHandle::run classifies nothing, OpKind::TxStatement has zero call sites) is a
        // RECORDED fact for §22.2 (aq), not an assertion: asserting the exact class would turn a
        // future honest client improvement into a chaos-harness failure.
        self::assertNotInstanceOf(IndeterminateException::class, $caught);

        $this->restartFerrod();
        $fresh = $this->connectConnection(pool: $pool);
        self::assertSame(
            0,
            $this->countKey($fresh, $family, $k1),
            'the plain-DML prefix persisted across a daemon kill — replay would double-apply (§19.3)',
        );
        self::assertSame(0, $this->countKey($fresh, $family, $k2));
        $fresh->session()->close();
    }

    // ---- cell 2b: the PINNED RESIDUAL — MySQL implicit commit across daemon death --------------

    /**
     * NOT an assertion of desired behavior — a PIN of a measured residual (the same instrument as
     * the §22.2 (ac) cry-wolf guard). MySQL's implicit commit makes the prefix INSERT durable the
     * moment `CREATE TABLE` runs, engine latch or no engine latch; a SIGKILL then destroys the
     * engine's `tx_writes_persisted` latch, NO wire field carries it, and the client — which
     * cannot know — reports a connection-shaped, non-`Indeterminate` loss whose replay would
     * re-apply `k1`. If this test ever FAILS on the last assertion, the client grew a wire signal:
     * update §19.3's residual note, §22.2 (aq), and this pin together.
     * Follow-up: `docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md` (Task 5).
     */
    public function testImplicitCommitPrefixSurvivesDaemonKillOnMysqlPinnedResidual(): void
    {
        $pool = $this->requireMysqlPool();
        $conn = $this->chaosConnect($pool);
        $this->setupTable($conn, 'mysql');
        $k1 = self::uniq('cell2b_prefix');
        $k2 = self::uniq('cell2b_inflight');
        $marker = self::uniq('m2b');
        $ddlTable = 'chaos_ic_' . strtolower(self::uniq('t'));

        try {
            $conn->begin();
            $conn->exec($this->plainInsertSql('mysql'), [$k1]);
            $conn->exec("CREATE TABLE {$ddlTable} (id INT)"); // implicit commit: k1 is now durable

            $this->spawnKiller('mysql', $marker, $pool);
            $caught = null;
            $caughtAt = 0.0;
            try {
                $conn->exec($this->sleepingInsertSql('mysql', $marker), [$k2]);
                self::fail('the post-DDL statement completed — the killer never fired');
            } catch (FerroException $e) {
                $caughtAt = microtime(true);
                $caught = $e;
            }
            $this->assertKillerObservedAndKilled($caughtAt);

            $this->restartFerrod();
            $fresh = $this->connectConnection(pool: $pool);

            self::assertSame(
                1,
                $this->countKey($fresh, 'mysql', $k1),
                'the measured premise moved: the implicitly-committed prefix did NOT survive — '
                    . 're-measure before touching the residual documentation',
            );
            self::assertSame(0, $this->countKey($fresh, 'mysql', $k2));
            self::assertNotInstanceOf(
                IndeterminateException::class,
                $caught,
                'the client reported Indeterminate for the in-tx loss — it appears to have grown '
                    . 'a persisted-writes signal; update §19.3 / §22.2 (aq) and this pin TOGETHER',
            );
            $fresh->session()->close();
        } finally {
            try {
                $cleanup = $this->connectConnection(pool: $pool);
                $cleanup->exec("DROP TABLE IF EXISTS {$ddlTable}");
                $cleanup->session()->close();
            } catch (\Throwable) {
                // an assertion-failure path may leave the daemon mid-restart; the table is
                // uniquely named and harmless if orphaned.
            }
        }
    }

    // ---- cell 3: readonly read → never Indeterminate; §19.2 recovery with changed epoch --------

    public function testReadonlyReadKilledMidFlightNeverIndeterminateAndRecoversOnPg(): void
    {
        $this->runReadonlyCell('pg', 'default');
    }

    public function testReadonlyReadKilledMidFlightNeverIndeterminateAndRecoversOnMysql(): void
    {
        $this->runReadonlyCell('mysql', $this->requireMysqlPool());
    }

    private function runReadonlyCell(string $family, string $pool): void
    {
        $conn = $this->chaosConnect($pool);
        $marker = self::uniq('m3');
        $sleepRead = $family === 'pg'
            ? sprintf("SELECT count(*) FROM pg_sleep(%d) WHERE '%s' <> ''", self::SLEEP_SEC, $marker)
            : sprintf("SELECT SLEEP(%d) FROM DUAL WHERE '%s' <> ''", self::SLEEP_SEC, $marker);
        $epochBefore = $conn->currentEpoch();

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        $caughtAt = 0.0;
        try {
            $conn->scalar($sleepRead); // scalar() declares readonly=true on the wire
            self::fail('the read completed — the killer never fired');
        } catch (FerroException $e) {
            $caughtAt = microtime(true);
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled($caughtAt);
        // A DECLARED read must never be Indeterminate (§19.3; the §22.2 (ac) guarantee's client
        // vantage). The concrete class with the daemon still DOWN is the raw last dial error
        // (ReconnectLoop rethrows it at exhaustion) — recorded for §22.2 (aq), not asserted.
        self::assertNotInstanceOf(IndeterminateException::class, $caught);

        // §19.1: a SIGKILLed daemon relaunched on the same socket draws a NEW `boot_epoch`, so all
        // engine-side state is void. Proven here from a FRESH connection rather than by re-using
        // `$conn` — the plan asked for `$conn`'s TRANSPARENT reconnect (§19.2), and this harness
        // MEASURED that path to be broken after a reconnect exhaustion (finding A: the loop leaves
        // a CLOSED session in place and the next call raises a raw PHP `TypeError`). That defect is
        // PINNED by {@see testReconnectExhaustionLeavesTheConnectionPermanentlyDeadPinnedDefect}
        // instead of being asserted here as a guarantee the client does not have.
        $this->restartFerrod();
        $fresh = $this->connectConnection(pool: $pool);
        self::assertSame(1, (int) $fresh->scalar('SELECT 1'));
        self::assertNotSame(
            $epochBefore,
            $fresh->currentEpoch(),
            'boot_epoch must CHANGE across a SIGKILL restart (§19.1) — engine state void',
        );
        $fresh->session()->close();
        $conn->session()->close();
    }

    // ---- cell 4: open stream → exactly one terminal, never a hang, never a silent clean end ----

    public function testOpenStreamKilledMidFlightThrowsExactlyOnceAndNeverHangsOnPg(): void
    {
        // PG ONLY: MySQL-family fetch:stream is Unsupported (§22.2 (n)) — there is no stream to
        // kill there (plan correction C2).
        $conn = $this->chaosConnect();
        $total = 200000;
        $rows = 0;
        $threw = null;
        $start = microtime(true);
        try {
            foreach ($conn->stream("SELECT g, repeat('x', 64) FROM generate_series(1, {$total}) g") as $row) {
                ++$rows;
                if ($rows === 5) {
                    // In flight BY CONSTRUCTION: DATA frames are arriving. The generator hands
                    // control back between rows, so the kill needs no sidecar.
                    shell_exec('kill -9 ' . $this->ferrodPid() . ' 2>/dev/null');
                }
            }
        } catch (\Throwable $e) {
            $threw = $e;
        }
        $elapsed = microtime(true) - $start;

        self::assertNotNull($threw, sprintf(
            'the stream ended CLEANLY after %d of %d rows — a mid-stream daemon death must '
                . 'surface as exactly one thrown terminal, never a silent truncation '
                . '(charter rule 4, client half)',
            $rows,
            $total,
        ));
        self::assertLessThan($total, $rows, 'the kill landed after the stream completed — not a mid-stream test');
        self::assertNotInstanceOf(
            IndeterminateException::class,
            $threw,
            'a streamed DECLARED read must never be Indeterminate (stream() declares readonly=true)',
        );
        // The io timeout is 60 s, so a genuine HANG would land here at ~60 s, not ~0 s: this bound
        // is a real hang detector only BECAUSE the timeout exceeds it (see the class docblock).
        self::assertLessThan(20.0, $elapsed, 'the loss must surface promptly — never a hang');

        // The §19.2 recovery half the plan put here is NOT asserted: this harness measured that a
        // mid-stream wire failure leaves `Session::$streamOpen` set forever, so the next request on
        // this connection is a `ProtocolException` and no reconnect is ever attempted (finding B).
        // Pinned by {@see testMidStreamDaemonDeathPoisonsTheSessionPinnedDefect}.
        $conn->session()->close();
    }

    // ---- PINNED DEFECTS — measured breakage, NOT desired behaviour -----------------------------

    /**
     * **PINNED DEFECT (finding A), not an assertion of desired behaviour** — the same instrument as
     * cell 2b and the §22.2 (ac) cry-wolf guard: when the client cannot do the thing, pin what
     * ACTUALLY happens rather than assert a guarantee we do not have, so the gap cannot be closed
     * or widened silently.
     *
     * MEASURED: `ReconnectLoop::reconnect()` closes the live session FIRST, then on exhaustion
     * rethrows the raw last dial error WITHOUT replacing it, so the loop is left holding a CLOSED
     * session. Every later call reaches `Transport::writeAll` → `@fwrite($closedResource, …)`, and
     * `@` suppresses warnings but NOT PHP 8's `TypeError`. The `TypeError` is outside the Ferro
     * taxonomy, so the autocommit path's `catch (ConnectionLostException | TransportException)`
     * cannot act on it and the reconnect loop is never re-entered: **the `Connection` is
     * permanently dead and a caller's `catch (FerroException)` never sees the failure.**
     * The default budget exhausts in under ~0.35 s (3 attempts, ≤50/100/200 ms jitter, an instant
     * ECONNREFUSED at a dead UDS), i.e. faster than any real `ferrod` restart — so this is the
     * COMMON case, not a corner. `RestartLiveTest` never saw it because it restarts the daemon
     * BEFORE issuing the read, so its reconnect succeeds on attempt 1.
     *
     * WHEN THIS TEST GOES RED the client was fixed: delete this pin and restore the §19.2
     * transparent-recovery assertion in {@see runReadonlyCell} (and in the stream cell, for the
     * sibling pin below).
     */
    public function testReconnectExhaustionLeavesTheConnectionPermanentlyDeadPinnedDefect(): void
    {
        $conn = $this->chaosConnect();
        self::assertSame(1, (int) $conn->scalar('SELECT 1'));

        shell_exec('kill -9 ' . $this->ferrodPid() . ' 2>/dev/null');
        $this->awaitFerrodGone();

        $exhausted = null;
        try {
            $conn->scalar('SELECT 1');
        } catch (\Throwable $e) {
            $exhausted = $e;
        }
        self::assertNotNull($exhausted, 'a read succeeded against a SIGKILLed daemon');
        // The §19.2 machinery RAN and exhausted: the surfaced error is the DIAL failure, not the
        // read-side EOF. This half is what distinguishes "the loop ran to exhaustion" from "the
        // loop was never entered", and it is what plan mutation M4 falsifies.
        self::assertInstanceOf(TransportException::class, $exhausted);
        self::assertStringContainsString(
            'connect failed',
            $exhausted->getMessage(),
            'the reconnect loop was never entered, so this test is not exercising exhaustion',
        );

        $this->restartFerrod();

        $post = null;
        try {
            $conn->scalar('SELECT 1');
        } catch (\Throwable $e) {
            $post = $e;
        }
        self::assertInstanceOf(
            \TypeError::class,
            $post,
            'the post-exhaustion call no longer raises a raw TypeError — if it now RECOVERS, the '
                . 'client was fixed: delete this pin and restore the §19.2 recovery assertion in '
                . 'runReadonlyCell()',
        );
        self::assertStringContainsString('not a valid stream resource', $post->getMessage());
    }

    /**
     * **PINNED DEFECT (finding B), not an assertion of desired behaviour.**
     *
     * MEASURED: `Session::$streamOpen` is cleared only by a `request_id=0` session-fatal terminal,
     * by a normal END, or by a drain that reaches one. A `readFrame()`/`sendWindowUpdate()` that
     * THROWS clears nothing, and `Connection::stream()`'s `finally` correctly declines to run the
     * release over a wire it already knows is broken — so the session is left believing a stream is
     * open. Every later `sendRequest` then hits `assertNoOpenStream()` and gets a
     * `ProtocolException` telling the caller to "drive it to its terminal", which is IMPOSSIBLE:
     * the socket is dead. `ProtocolException` is outside the reconnect-triggering set, so §19.2
     * recovery is unreachable and the `Connection` is permanently unusable.
     *
     * WHEN THIS TEST GOES RED the client was fixed: delete this pin and restore the post-restart
     * recovery assertion in {@see testOpenStreamKilledMidFlightThrowsExactlyOnceAndNeverHangsOnPg}.
     */
    public function testMidStreamDaemonDeathPoisonsTheSessionPinnedDefect(): void
    {
        $conn = $this->chaosConnect();
        $rows = 0;
        $threw = null;
        try {
            foreach ($conn->stream("SELECT g, repeat('x', 64) FROM generate_series(1, 200000) g") as $row) {
                if (++$rows === 5) {
                    shell_exec('kill -9 ' . $this->ferrodPid() . ' 2>/dev/null');
                }
            }
        } catch (\Throwable $e) {
            $threw = $e;
        }
        self::assertNotNull($threw, 'the stream ended cleanly — the kill landed outside the stream');
        $this->restartFerrod();

        $post = null;
        try {
            $conn->scalar('SELECT 1');
        } catch (\Throwable $e) {
            $post = $e;
        }
        self::assertInstanceOf(
            ProtocolException::class,
            $post,
            'the connection is no longer poisoned by a mid-stream wire failure — if it now '
                . 'RECOVERS, the client was fixed: delete this pin and restore the post-restart '
                . 'recovery assertion in the stream cell',
        );
        self::assertStringContainsString('is open on this session', $post->getMessage());
    }

    // ---- plumbing -------------------------------------------------------------------------------

    /**
     * A {@see Connection} whose io timeout EXCEEDS the parked sleep — deliberately NOT
     * {@see LiveTestCase::connectConnection}, whose 5.0 s window makes every sidecar cell able to
     * pass on a client-side timeout with `ferrod` still alive. See the class docblock.
     */
    private function chaosConnect(string $pool = 'default'): Connection
    {
        return Ferro::connect($this->socketPath, $pool, 2.0, self::IO_TIMEOUT_SEC);
    }

    /**
     * Block (<= 5 s) until the SIGKILLed `ferrod` stops accepting on its socket. A direct
     * observable — no ext-posix, no `/proc` poking (charter rule 7) — so the pin below cannot
     * race the kill and classify a still-live daemon.
     */
    private function awaitFerrodGone(): void
    {
        $deadline = microtime(true) + 5.0;
        while (microtime(true) < $deadline) {
            $errno = 0;
            $errstr = '';
            $sock = @stream_socket_client('unix://' . $this->socketPath, $errno, $errstr, 0.5);
            if ($sock === false) {
                return;
            }
            fclose($sock);
            usleep(20_000);
        }
        self::fail('ferrod was still accepting connections 5s after SIGKILL');
    }

    private static function uniq(string $prefix): string
    {
        return $prefix . '_' . getmypid() . '_' . bin2hex(random_bytes(6));
    }

    private function setupTable(Connection $conn, string $family): void
    {
        // No PK on `k` — deliberately: a unique constraint would make the at-most-once read-back
        // (count <= 1) TRUE BY SCHEMA and the assertion vacuous. Fixed table name + unique keys is
        // the shared-database discipline the Rust chaos suites use.
        $conn->exec($family === 'pg'
            ? 'CREATE TABLE IF NOT EXISTS chaos_kill_fate (k text, pad text)'
            : 'CREATE TABLE IF NOT EXISTS chaos_kill_fate (k VARCHAR(191), pad TEXT)');
    }

    private function plainInsertSql(string $family): string
    {
        return $family === 'pg'
            ? 'INSERT INTO chaos_kill_fate (k) VALUES ($1)'
            : 'INSERT INTO chaos_kill_fate (k) VALUES (?)';
    }

    private function sleepingInsertSql(string $family, string $marker): string
    {
        // The marker is a STRING-LITERAL predicate riding the statement text (§20.3: never a
        // comment — MariaDB strips comments from processlist INFO); the sleep keeps the statement
        // provably in flight until the sidecar observes it.
        return $family === 'pg'
            ? sprintf(
                "INSERT INTO chaos_kill_fate (k, pad) SELECT \$1, pg_sleep(%d)::text WHERE '%s' <> ''",
                self::SLEEP_SEC,
                $marker,
            )
            : sprintf(
                "INSERT INTO chaos_kill_fate (k, pad) SELECT ?, CONCAT('', SLEEP(%d)) FROM DUAL WHERE '%s' <> ''",
                self::SLEEP_SEC,
                $marker,
            );
    }

    private function countKey(Connection $conn, string $family, string $k): int
    {
        return (int) $conn->scalar(
            $family === 'pg'
                ? 'SELECT count(*) FROM chaos_kill_fate WHERE k = $1'
                : 'SELECT count(*) FROM chaos_kill_fate WHERE k = ?',
            [$k],
        );
    }

    private function spawnKiller(string $family, string $marker, string $pool): void
    {
        $log = sys_get_temp_dir() . '/ferro-chaos-killer-' . getmypid() . '.log';
        $stamp = sys_get_temp_dir() . '/ferro-chaos-stamp-' . getmypid() . '-' . $marker;
        @unlink($stamp);
        $proc = proc_open(
            [
                PHP_BINARY,
                __DIR__ . '/../Support/chaos_killer.php',
                $this->socketPath,
                $pool,
                $family,
                $marker,
                (string) $this->ferrodPid(),
                self::KILLER_BUDGET_SEC,
                $stamp,
            ],
            [1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']],
            $pipes,
        );
        self::assertIsResource($proc, 'failed to spawn chaos_killer.php');
        $this->killers[] = ['proc' => $proc, 'stamp' => $stamp];
    }

    /**
     * The last spawned killer must have (a) delivered its SIGKILL **BEFORE** the client observed
     * the loss and (b) exited 0 = "observed IN FLIGHT, then killed" — never 2 (budget expired,
     * nothing killed) or 3 (usage/connect/poll failure).
     *
     * (a) is what makes this harness an acceptance test rather than a coincidence detector, and it
     * is an ORDER comparison for a measured reason. The first version of this belt asked "has the
     * killer exited by now?" with a 1 s grace — and a mutation that shrank the client's io timeout
     * to 0.2 s (reproducing the impostor the plan's verification described: a client-side read
     * timeout classified as the SAME `IndeterminateException` a daemon death mints) came back
     * **GREEN**, because the killer simply caught up inside the grace. The sidecar therefore stamps
     * `microtime(true)` immediately BEFORE signalling — a strict lower bound on the kill — and the
     * cell stamps the instant it caught. `killedAt <= caughtAt` has no race window: a genuine kill
     * always precedes the loss it causes, and a client-side timeout with the daemon still alive
     * always precedes the kill.
     *
     * @param float $caughtAt `microtime(true)` taken where the cell caught its exception.
     */
    private function assertKillerObservedAndKilled(float $caughtAt): void
    {
        $killer = array_pop($this->killers);
        self::assertIsArray($killer, 'assertKillerObservedAndKilled(): no killer was spawned');
        $proc = $killer['proc'];
        self::assertIsResource($proc);

        $deadline = microtime(true) + 5.0;
        $status = proc_get_status($proc);
        while ($status['running'] === true && microtime(true) < $deadline) {
            usleep(20_000);
            $status = proc_get_status($proc);
        }
        $stillRunning = $status['running'] === true;
        $exit = $stillRunning ? null : $status['exitcode'];
        if ($stillRunning) {
            @proc_terminate($proc, 9);
        }
        @proc_close($proc);

        self::assertSame(
            0,
            $exit,
            'chaos_killer did not observe the marker in flight before killing — the kill proves '
                . 'nothing (see ferro-chaos-killer-*.log in sys_get_temp_dir())',
        );

        $raw = @file_get_contents($killer['stamp']);
        @unlink($killer['stamp']);
        self::assertIsString($raw, 'chaos_killer exited 0 but wrote no kill stamp');
        self::assertNotSame('', $raw, 'chaos_killer wrote an empty kill stamp');
        $killedAt = (float) $raw;

        self::assertLessThanOrEqual(
            $caughtAt,
            $killedAt,
            sprintf(
                'the client classified its loss %.3f s BEFORE ferrod was signalled — whatever this '
                    . 'cell observed was NOT the §20.3 daemon death (the measured impostor is a '
                    . 'client-side io timeout, classified identically; see the class docblock)',
                $killedAt - $caughtAt,
            ),
        );
    }
}
