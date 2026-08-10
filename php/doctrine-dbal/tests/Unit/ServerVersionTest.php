<?php // /php/doctrine-dbal/tests/Unit/ServerVersionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection\StaticServerVersionProvider;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\TransportException;
use Ferro\DBAL\Connection;
use Ferro\DBAL\Driver;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\Exception\ServerVersionUnavailable;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 6 — the SPEC §14 nil-`server_version` decision: DEFER, resolve ONCE, then FAIL LOUDLY.
 *
 * `Doctrine\DBAL\ServerVersionProvider::getServerVersion(): string` is non-nullable while
 * `PoolInfo.server_version` is `str | nil`, and nil is a NORMAL recurring value on a healthy system
 * (a TTL expiry racing a re-probe, a probe failure inside its 5 s backoff, a backend down at
 * connect). So "unknown" cannot be represented and the only honest options are to resolve it or to
 * throw — never to guess a platform, because a platform IS the SQL dialect of every statement that
 * follows.
 *
 * Three properties are asserted here and each has its own reachable failing input:
 *   A. the loud failure is ACTIONABLE (the message half — "fail loudly" is only better than
 *      "guess a platform" if an operator can act on it);
 *   B. resolution actually HAPPENS and is exactly ONE readonly `SELECT version()` (the plan
 *      predicted this half would be unobservable offline; it is observable against a scripted
 *      session, so the fallback cannot be deleted silently — see the journal);
 *   C. the cache is PER CONNECTION (a shared cache would let one pool's version choose another
 *      pool's dialect — the same wrong-dialect failure by a different route).
 *
 * The behavioural half — a pool whose backend is genuinely DOWN — is live
 * ({@see \Ferro\DBAL\Tests\Live\ServerVersionLiveTest}).
 */
final class ServerVersionTest extends TestCase
{
    /** The live PG string, verbatim as `ferrod` caches it (plan hazard 43). */
    private const PG_LIVE = 'PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu';

    /**
     * Decode a recorded `ExecRequest` payload back to its field map — the established idiom
     * (`ConnectionFateFlagTest`, `RawFetchTest`). `PurePacker`, never `ExtPacker`: the latter
     * consumes the whole buffer regardless of the offset it is handed (hazard 87).
     *
     * @return array<string,mixed>
     */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        return ExecRequest::mapFromWire(array_values((array) (new PurePacker())->unpack($payload, $off)));
    }

    /**
     * A driver connection over a scripted session whose `HELLO_ACK` advertised `$advertised` as this
     * pool's `server_version` (`null` = the nil case this whole task exists for).
     */
    private static function conn(FakeSession $session, ?string $advertised, string $pool = 'main'): Connection
    {
        $session->poolInfo = [new PoolInfo($pool, PlatformVersion::KIND_POSTGRES, $advertised)];
        return new Connection(
            new FerroClientConnection($session, $pool),
            $pool,
            PlatformVersion::KIND_POSTGRES,
            false,
        );
    }

    /** A one-row, one-column TEXT `ExecOk`, i.e. what `SELECT version()` really answers. */
    private static function versionRow(string $version): \Ferro\Protocol\Outcome
    {
        return FakeSession::execOk([
            'cols' => [['name' => 'version', 'tag' => C::TAG_TEXT]],
            'rows' => [[['tag' => C::TAG_TEXT, 'data' => $version]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);
    }

    // ---- A. the loud failure ---------------------------------------------------------------------

    /**
     * Four things must be in the text, each its own assertion so a message rewrite that drops one
     * goes red:
     *   1. WHICH pool (a driver may serve several).
     *   2. That the family IS known — only the version within it is not.
     *   3. That nil is a NORMAL transient state, so "wait and retry" is a real fix.
     *   4. The `serverVersion` connection parameter, by its literal name, as the deterministic fix.
     */
    public function testTheLoudFailureIsActionable(): void
    {
        $e = ServerVersionUnavailable::forPool('main', 'postgres', null);
        $msg = $e->getMessage();

        self::assertStringContainsString('"main"', $msg, 'name the pool');
        self::assertStringContainsString('postgres', $msg, 'the family IS known');
        self::assertStringContainsString('transient', $msg, 'nil is a normal recurring state');
        self::assertStringContainsString('serverVersion', $msg, 'name the operator escape hatch');
        self::assertStringNotContainsString(
            'defaulting',
            $msg,
            'no default platform is ever guessed — a wrong platform is a wrong SQL dialect',
        );
    }

    /** It is a `Driver\Exception`, so it is well-formed if it ever reaches the converter. */
    public function testItIsADriverException(): void
    {
        self::assertInstanceOf(
            \Doctrine\DBAL\Driver\Exception::class,
            ServerVersionUnavailable::forPool('main', 'mysql', null),
        );
    }

    // ---- B. resolution happens, exactly once, as a declared READ ---------------------------------

    /**
     * The handshake already knew it: hand it back VERBATIM (normalisation is
     * {@see PlatformVersion}'s job and is asymmetric) and touch the wire not at all.
     */
    public function testAnAdvertisedVersionIsUsedVerbatimAndCostsNoRoundTrip(): void
    {
        $session = new FakeSession();
        $c = self::conn($session, self::PG_LIVE);

        self::assertSame(self::PG_LIVE, $c->getServerVersion());
        self::assertSame(0, $session->sendCount(), 'the advertised version needs no statement');
    }

    /**
     * THE DEFERRAL'S WHOLE VALUE, and the guard the plan expected not to have: when the handshake
     * advertised nil, the driver asks the backend ITSELF rather than failing or guessing. Asserted
     * on the encoded `ExecRequest`, so the statement, its fate declaration and its fetch mode are
     * all pinned — deleting the fallback, or sending something else, is RED here rather than
     * "green with a note".
     *
     * `readonly = true` is correct precisely BECAUSE this is the driver's own statement: the
     * connection-wide "declare write for everything" rule exists because the DBAL SPI hides the
     * caller's intent (hazard 22), and here there is no caller to hide.
     */
    public function testANilAdvertisedVersionIsResolvedByExactlyOneReadonlySelectVersion(): void
    {
        $session = (new FakeSession())->push(self::versionRow(self::PG_LIVE), [C::SERVICE_SQL, C::METHOD_SQL_EXEC]);
        $c = self::conn($session, null);

        self::assertSame(self::PG_LIVE, $c->getServerVersion());
        self::assertSame(1, $session->sendCount());

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame('SELECT version()', $req['sql'], "the engine's own probe statement, verbatim");
        self::assertTrue($req['readonly'], 'the driver declares its OWN statement a read');
        self::assertSame(0, $req['fetch'], 'fetch:rows (0) — a version with no rows back is useless');
        self::assertSame('main', $req['pool'], 'resolved against THIS connection\'s pool');
    }

    /**
     * An EMPTY advertised string is nil in disguise (`PoolInfo` is `str | nil`, but an engine that
     * ever advertised `""` would otherwise hand `''` to `PlatformVersion` and become
     * `InvalidPlatformVersion`, which names no pool). It resolves like nil.
     */
    public function testAnEmptyAdvertisedVersionIsTreatedAsUnknownAndResolved(): void
    {
        $session = (new FakeSession())->push(self::versionRow(self::PG_LIVE), [C::SERVICE_SQL, C::METHOD_SQL_EXEC]);
        $c = self::conn($session, '');

        self::assertSame(self::PG_LIVE, $c->getServerVersion());
        self::assertSame(1, $session->sendCount());
    }

    /** One round trip, EVER — the docblock's claim, measured rather than asserted in prose. */
    public function testTheResolvedVersionIsCachedForTheLifeOfTheConnection(): void
    {
        $session = (new FakeSession())->push(self::versionRow(self::PG_LIVE), [C::SERVICE_SQL, C::METHOD_SQL_EXEC]);
        $c = self::conn($session, null);

        self::assertSame(self::PG_LIVE, $c->getServerVersion());
        self::assertSame(self::PG_LIVE, $c->getServerVersion());
        self::assertSame(1, $session->sendCount(), 'the second call must not reach the wire');
    }

    // ---- the failure branches, each from a reachable input ---------------------------------------

    /**
     * The statement failed: the loud failure carries the cause, so the real error is not lost.
     *
     * MEASURED (and the reason this is not an identity assertion): the client does not rethrow the
     * transport failure — `FateClassifier` turns it into the §19.3 classification
     * `RetryableException("connection lost with no write-fate to be unsure about (Retryable): link
     * died …")` with a null `previous`. So the property that must hold is that the ORIGINAL
     * diagnostic text survives into our chained cause, not that the object is the same one.
     */
    public function testAFailedResolutionBecomesTheLoudFailureAndKeepsTheCause(): void
    {
        $cause = new TransportException('link died writing SELECT version()');
        $session = (new FakeSession())->push($cause, [C::SERVICE_SQL, C::METHOD_SQL_EXEC]);
        $c = self::conn($session, null);

        try {
            $c->getServerVersion();
            self::fail('an unresolvable version must not produce a string');
        } catch (ServerVersionUnavailable $e) {
            self::assertStringContainsString('"main"', $e->getMessage());
            $previous = $e->getPrevious();
            self::assertInstanceOf(FerroException::class, $previous, 'the client error must be chained, not swallowed');
            self::assertStringContainsString(
                'link died writing SELECT version()',
                $previous->getMessage(),
                'the real diagnostic must survive the wrapping',
            );
        }
    }

    /** The statement SUCCEEDED and returned nothing usable — a different input, same refusal. */
    public function testAnEmptyResultSetIsAlsoTheLoudFailure(): void
    {
        $session = (new FakeSession())->thenEmptyRows();
        $c = self::conn($session, null);

        $this->expectException(ServerVersionUnavailable::class);
        $c->getServerVersion();
    }

    /**
     * A non-TEXT cell is refused rather than coerced. `(string) 42` would be a version string that
     * parses (`InvalidPlatformVersion` never fires on `"42"`) and selects a platform — the silent
     * wrong dialect this whole task exists to refuse.
     */
    public function testANonStringCellIsRefusedRatherThanCoerced(): void
    {
        $session = (new FakeSession())->push(FakeSession::scalarRow(42), [C::SERVICE_SQL, C::METHOD_SQL_EXEC]);
        $c = self::conn($session, null);

        $this->expectException(ServerVersionUnavailable::class);
        $c->getServerVersion();
    }

    // ---- C. the cache is per connection ----------------------------------------------------------

    /**
     * Two connections, two pools, in ONE process — which is every PHP-FPM worker that talks to more
     * than one pool. A cache shared between them (a `static`, or anything hung off the Driver) would
     * hand pool `b` the version of pool `a` and therefore possibly MySQL's dialect to PostgreSQL.
     * Order-independent by construction: both connections live inside this one test.
     */
    public function testTheCacheIsPerConnectionNotShared(): void
    {
        $a = self::conn(new FakeSession(), self::PG_LIVE, 'a');
        self::assertSame(self::PG_LIVE, $a->getServerVersion());

        $bSession = (new FakeSession())->push(
            new TransportException('pool b cannot be reached'),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $b = self::conn($bSession, null, 'b');

        try {
            $b->getServerVersion();
            self::fail('pool b resolved a version it never learned — the cache is shared');
        } catch (ServerVersionUnavailable $e) {
            self::assertStringContainsString('"b"', $e->getMessage());
        }
    }

    // ---- hazard 15: the platform can be resolved with NO connection at all ------------------------

    /**
     * `Doctrine\DBAL\Connection::getDatabasePlatform()` builds a `StaticServerVersionProvider` from
     * `$params['serverVersion']` and NEVER asks the driver connection (measured in dbal 4.4.4's
     * `Connection.php:185-200`). So `Driver::getDatabasePlatform()` is reachable with no pool kind
     * ever learned, and the version string is then the only signal there is. This is the operator
     * escape hatch the loud message names, so it must actually work.
     */
    public function testThePlatformResolvesFromTheServerVersionParamAloneWithNoConnection(): void
    {
        $platform = (new Driver())->getDatabasePlatform(new StaticServerVersionProvider('PostgreSQL 17.10'));

        self::assertInstanceOf(PostgreSQL120Platform::class, $platform);
    }

    /**
     * The MIRROR, and the reason this fork may not "just pick one": `8.4.11` is the LIVE MySQL
     * string, and `11.8.8` is MariaDB's with its family suffix stripped — neither names a family, so
     * a driver that defaulted would be right half the time and silently wrong the other half. It
     * fails loudly instead, naming the parameter that caused it.
     */
    public function testABeforeConnectVersionThatNamesNoFamilyFailsLoudly(): void
    {
        try {
            (new Driver())->getDatabasePlatform(new StaticServerVersionProvider('8.4.11'));
            self::fail('a family-less serverVersion must not silently select a platform');
        } catch (BackendFamilyUnknown $e) {
            self::assertStringContainsString('8.4.11', $e->getMessage());
            self::assertStringContainsString('serverVersion', $e->getMessage());
        }
    }
}
