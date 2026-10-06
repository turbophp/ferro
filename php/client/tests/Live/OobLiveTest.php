<?php // /php/client/tests/Live/OobLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Ferro;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\OobRef;
use Ferro\Protocol\Outcome;
use PHPUnit\Framework\Attributes\Group;
use function Ferro\await;

/**
 * M3-D3 (SPEC §5.1) against a real `ferrod` and PostgreSQL: a result above the engine's 1 MiB
 * default threshold reaches a `MEMFD_RX` client through a sealed memfd passed with `SCM_RIGHTS`,
 * identical to what the same statement returns inline, interleaved correctly with other requests on
 * the same session, and never sent to a client that did not advertise `MEMFD_RX`.
 *
 * Every test asserts that the out-of-band path was TAKEN (the transport's fd count and the session's
 * OOB count), not only that the data is right — the inline fallback would produce the same rows,
 * which is precisely what SPEC §5.1 requires and precisely why the data alone proves nothing.
 *
 * Needs `ext-sockets` on Linux; CI's PHP job provides it, so a skip here fails that lane. The
 * `oob` group is what CI's `fread`-path re-run excludes (it runs with `socket_recvmsg` disabled).
 */
#[Group('oob')]
final class OobLiveTest extends LiveTestCase
{
    /** 2 MiB of payload: well past the 1 MiB default threshold. */
    private const BIG = 2 * 1024 * 1024;

    protected function setUp(): void
    {
        parent::setUp();
        if (!Transport::canReceiveFds()) {
            $this->markTestSkipped('the memfd path needs Linux and ext-sockets');
        }
    }

    private static function bigSql(): string
    {
        return 'SELECT ?::text || repeat(\'x\', ' . self::BIG . ') AS big, 7 AS seven';
    }

    /**
     * Review F6: the per-connection switch an application can reach. `Ferro::connect()` receives
     * fds by default (auto) and takes a large result by memfd; `receiveFds: false` keeps that
     * connection on the `fread` path, where the same result arrives inline.
     */
    public function testFerroConnectReceivesFdsByDefaultAndCanOptOut(): void
    {
        foreach ([[null, 1], [false, 0]] as [$receiveFds, $expectedOob]) {
            $c = Ferro::connect($this->socketPath, receiveFds: $receiveFds);
            $session = $c->session();
            try {
                $this->assertInstanceOf(Session::class, $session);
                $this->assertSame($receiveFds === null, $session->receivesFds());
                $rows = $c->rows(self::bigSql(), ['sw-']);
                $this->assertSame(strlen('sw-') + self::BIG, strlen((string) $rows[0]['big']));
                $this->assertSame($expectedOob, $session->oobPayloadsReceived());
            } finally {
                $session->close();
            }
        }
    }

    public function testALargeResultArrivesThroughAMemfdAndEqualsTheInlineResult(): void
    {
        $rxTransport = Transport::connectUnix($this->socketPath, 2.0, 5.0);
        $this->assertTrue($rxTransport->receivesFds(), 'auto-detected: Linux + ext-sockets + UDS');
        $rx = new Session($rxTransport);
        $ack = $rx->hello();
        $this->assertNotSame(0, $ack->features & C::FEATURE_ENGINE_MEMFD, 'the engine advertises MEMFD');

        $plainTransport = Transport::connectUnix($this->socketPath, 2.0, 5.0, receiveFds: false);
        $this->assertFalse($plainTransport->receivesFds());
        $plain = new Session($plainTransport);
        $plain->hello();

        try {
            $viaMemfd = (new Connection($rx, 'default'))->rows(self::bigSql(), ['oob-']);
            $this->assertSame(1, $rx->oobPayloadsReceived(), 'the result came through a memfd');
            $this->assertSame(1, $rxTransport->fdsReceived(), 'exactly one fd was received');

            $inline = (new Connection($plain, 'default'))->rows(self::bigSql(), ['oob-']);
            $this->assertSame(0, $plain->oobPayloadsReceived(), 'no MEMFD_RX, no memfd');
            $this->assertSame(0, $plainTransport->fdsReceived());

            $this->assertSame($inline, $viaMemfd, 'identical rows either way');
            $this->assertSame(strlen('oob-') + self::BIG, strlen((string) $viaMemfd[0]['big']));
            $this->assertStringStartsWith('oob-xxx', (string) $viaMemfd[0]['big']);

            // A small result on the SAME memfd-capable session stays inline.
            $this->assertSame(1, (new Connection($rx, 'default'))->scalar('SELECT 1'));
            $this->assertSame(1, $rx->oobPayloadsReceived(), 'a small result does not use a memfd');
        } finally {
            $rx->close();
            $plain->close();
        }
    }

    /**
     * D1a multiplexing: three large requests and twelve small ones in flight on ONE session, written
     * before anything is read. Each large result must come back to ITS OWN Future — the content is
     * unique per request, so an fd paired with the wrong frame is caught by value — and exactly
     * three fds arrive.
     */
    public function testMemfdsPairWithTheirOwnRequestsAmongConcurrentSmallOnes(): void
    {
        $t = Transport::connectUnix($this->socketPath, 2.0, 10.0);
        $session = new Session($t);
        $session->hello();
        $conn = new Connection($session, 'default');
        try {
            $futures = [];
            for ($b = 0; $b < 3; ++$b) {
                $futures["big{$b}"] = $conn->scalarAsync(self::bigSql(), ["tag{$b}-"]);
                for ($s = 0; $s < 4; ++$s) {
                    $n = $b * 10 + $s;
                    $futures["small{$n}"] = $conn->scalarAsync('SELECT ?::int AS n', [$n]);
                }
            }
            $results = await($futures);
            for ($b = 0; $b < 3; ++$b) {
                $v = (string) $results["big{$b}"];
                $this->assertStringStartsWith("tag{$b}-xxx", $v, "big{$b} got its own result");
                $this->assertSame(strlen("tag{$b}-") + self::BIG, strlen($v));
                for ($s = 0; $s < 4; ++$s) {
                    $n = $b * 10 + $s;
                    $this->assertSame($n, $results["small{$n}"]);
                }
            }
            $this->assertSame(3, $session->oobPayloadsReceived());
            $this->assertSame(3, $t->fdsReceived());
        } finally {
            $session->close();
        }
    }

    /**
     * The fd a client receives is SEALED: it cannot be written, grown or shrunk, so the `len` the
     * frame names is the memfd's size for as long as anyone holds it. Read at the transport level, so
     * the test holds the fd itself rather than the session (which closes it once read).
     */
    public function testTheReceivedMemfdCannotBeWrittenOrResized(): void
    {
        $t = Transport::connectUnix($this->socketPath, 2.0, 5.0);
        $session = new Session($t);
        $session->hello();
        $p = PackerFactory::forEncode();
        $req = ExecRequest::encode([
            'pool' => 'default',
            'sql' => self::bigSql(),
            'params' => [['tag' => C::TAG_TEXT, 'data' => 'sealed-']],
            'readonly' => true,
            'fetch' => 0,
        ], $p);
        $rid = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $req);
        try {
            $header = Header::decode($t->readExact(16));
            $this->assertSame($rid, $header->requestId);
            $this->assertSame(C::FLAG_END | C::FLAG_OOB_FD, $header->flags);
            $ref = OobRef::decode($t->readExact($header->payloadLen), PackerFactory::forDecode());
            $fd = $t->takeFd();
            $this->assertIsResource($fd, 'the fd arrived with its frame');

            $stat = fstat($fd);
            $this->assertIsArray($stat);
            $this->assertSame($ref['len'], $stat['size']);
            $this->assertFalse(@ftruncate($fd, $ref['len'] + 1), 'F_SEAL_GROW');
            $this->assertFalse(@ftruncate($fd, 1), 'F_SEAL_SHRINK');
            $wrote = @fwrite($fd, 'tamper');
            $this->assertTrue($wrote === false || $wrote === 0, 'F_SEAL_WRITE');
            $this->assertSame($ref['len'], (int) (fstat($fd)['size'] ?? -1), 'unchanged');

            $bytes = stream_get_contents($fd, $ref['len'], 0);
            $this->assertIsString($bytes);
            $outcome = Outcome::decode($bytes, PackerFactory::forDecode());
            $this->assertTrue($outcome->isOk(), 'the memfd holds the terminal Outcome');
            fclose($fd);
        } finally {
            $t->close();
        }
    }
}
