<?php // /php/client/tests/Live/BytesLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Bytes;
use Ferro\Client\Error\FerroException;

/**
 * **`Ferro\Bytes` end to end (SPEC §22.2 (k)(4)).** A genuine binary payload — a NUL byte and two
 * bytes that are not valid UTF-8 in any position — written through `TAG_BYTES` and read back
 * byte-identical, on BOTH engine families, plus the pre-S8a failure pinned so it cannot come back.
 */
final class BytesLiveTest extends LiveTestCase
{
    /**
     * NUL (the classic C-string truncator), `\xfe`/`\xff` (bytes that can NEVER appear in valid
     * UTF-8), and `\x80` (a bare continuation byte). If any part of the path treats this as text,
     * the round trip either truncates or the request is refused as `invalid utf8`.
     */
    private const BLOB = "\x00\x01\xfe\xff\x7f\x80";

    public function testPostgresByteaRoundTrip(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_bytes');
        $c->exec('CREATE TABLE s8a_bytes (b bytea)');
        $c->exec('INSERT INTO s8a_bytes (b) VALUES (?)', [new Bytes(self::BLOB)]);

        $rows = $c->query('SELECT b FROM s8a_bytes');
        $this->assertSame(bin2hex(self::BLOB), bin2hex(self::str($rows[0]['b'])));
        // Not just equal — the full length survived, so nothing truncated at the NUL.
        $this->assertSame(strlen(self::BLOB), strlen(self::str($rows[0]['b'])));
    }

    /**
     * VARBINARY **and** BLOB: they are different `ColumnType`s on the wire
     * (`MYSQL_TYPE_VAR_STRING` vs `MYSQL_TYPE_BLOB`) and reach `Value::Bytes` through different
     * arms of `rowmap`, so one column would not cover the other.
     */
    public function testMysqlVarbinaryAndBlobRoundTrip(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_bytes');
        $c->exec('CREATE TABLE s8a_bytes (b VARBINARY(64), bl BLOB)');
        $c->exec('INSERT INTO s8a_bytes (b, bl) VALUES (?, ?)', [new Bytes(self::BLOB), new Bytes(self::BLOB)]);

        $rows = $c->query('SELECT b, bl FROM s8a_bytes');
        $this->assertSame(bin2hex(self::BLOB), bin2hex(self::str($rows[0]['b'])));
        $this->assertSame(bin2hex(self::BLOB), bin2hex(self::str($rows[0]['bl'])));
        $this->assertSame(strlen(self::BLOB), strlen(self::str($rows[0]['bl'])));
    }

    /**
     * The pre-M1-S8a failure, pinned so it cannot silently come back: a BARE non-UTF-8 string is
     * still rejected — and by the CODEC, not the bind pre-flight — because a string's contents are
     * never sniffed for a tag.
     */
    public function testABareNonUtf8StringIsStillRefused(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_bytes_bare');
        $c->exec('CREATE TABLE s8a_bytes_bare (b bytea)');

        // NOT `expectException(FerroException::class)`: that is the ROOT of the tree (hazard 68), so
        // it would also pass if one of the two setup statements above threw — i.e. the test would be
        // green having never reached the assertion's subject. Assert on the MESSAGE instead, which
        // pins the actual mechanism: `TAG_TEXT` rides the msgpack `str` family and the engine's
        // reader ends in `String::from_utf8`, so the failure is a CODEC fault, not a bind error.
        try {
            $c->exec('INSERT INTO s8a_bytes_bare (b) VALUES (?)', [self::BLOB]);
            $this->fail('a bare non-UTF-8 string must not bind');
        } catch (FerroException $e) {
            $this->assertStringContainsStringIgnoringCase(
                'utf8',
                $e->getMessage(),
                'the refusal must be the UTF-8 codec fault, not some other error: ' . $e->getMessage(),
            );
        }

        // Charter rule 4: the refusal is a per-request error END, not a dead session — the very next
        // statement on the SAME connection must still work.
        $rows = $c->query('SELECT count(*) AS n FROM s8a_bytes_bare');
        $this->assertSame(1, count($rows));
        $this->assertSame(0, $rows[0]['n'], 'the refused INSERT must not have landed');
    }

    public function testLargeObjectStreamBindsThroughFromStream(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_blob');
        $c->exec('CREATE TABLE s8a_blob (b bytea)');
        $h = fopen('php://memory', 'r+');
        $this->assertIsResource($h);
        fwrite($h, self::BLOB);
        rewind($h);
        $c->exec('INSERT INTO s8a_blob (b) VALUES (?)', [Bytes::fromStream($h)]);

        $rows = $c->query('SELECT b FROM s8a_blob');
        $this->assertSame(bin2hex(self::BLOB), bin2hex(self::str($rows[0]['b'])));
    }

    /** PHPStan level 9: a hydrated cell is `mixed`; narrow it without coercing a wrong shape. */
    private static function str(mixed $v): string
    {
        self::assertIsString($v, 'a BYTES column must hydrate to a plain PHP string');
        return $v;
    }
}
