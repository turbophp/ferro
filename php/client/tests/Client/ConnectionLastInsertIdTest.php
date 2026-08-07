<?php // /php/client/tests/Client/ConnectionLastInsertIdTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8a Task 2 — the OFFLINE half of the `last_insert_id` contract, over an in-memory transport
 * (no ferrod, no database), so the codec rules are pinned independently of any backend:
 *
 *  * the field is carried RAW, NOT through the §9.1 {@see \Ferro\Client\Value\ValuePolicy} — a big
 *    key must arrive as its canonical decimal STRING, never as a `Ferro\U64` value object (which is
 *    what a column cell of the same tag decodes to). Routing it through the policy is the natural
 *    "consistency" refactor, and it would break the DBAL `int|string` contract;
 *  * `0` is a real key and must not collapse to `null`;
 *  * every statement OVERWRITES it, so a read that reports none CLEARS an earlier INSERT's key —
 *    the offline mirror of the live "a SELECT reports no generated key" assertion.
 *
 * The table below carries BOTH a present and an absent key, so the assertion is a mirror property
 * (output equals the encoded input) rather than a one-sided negative that cannot fail.
 */
final class ConnectionLastInsertIdTest extends TestCase
{
    private PackerInterface $packer;
    private Codec $codec;

    protected function setUp(): void
    {
        $this->packer = PackerFactory::forEncode();
        $this->codec = new Codec();
    }

    /**
     * Every `last_insert_id` shape the engine's `last_insert_id_value` can emit, plus the absent
     * case, each with the exact scalar `lastInsertId()` must report back.
     *
     * @return array<string, array{0: array{tag:int,data:mixed}|null, 1: int|string|null}>
     */
    public static function keys(): array
    {
        return [
            'absent (PG, or any non-INSERT)' => [null, null],
            'zero is a real key, not absent' => [['tag' => C::TAG_I64, 'data' => 0], 0],
            'the golden vector shape'        => [['tag' => C::TAG_I64, 'data' => 200], 200],
            // 2^32-1 is the last value msgpack packs as a `uint32` (0xce), which PurePacker narrows
            // to a PHP int; 2^32 needs a `uint64` (0xcf), which it hands back as the canonical
            // decimal string. THAT — not PHP_INT_MAX — is where `int|string` actually turns over.
            'uint32 ceiling stays an int'    => [['tag' => C::TAG_I64, 'data' => 4294967295], 4294967295],
            'uint64 band -> decimal string'  => [['tag' => C::TAG_I64, 'data' => 4294967296], '4294967296'],
            'i64::MAX -> decimal string'     => [
                ['tag' => C::TAG_I64, 'data' => PHP_INT_MAX],
                '9223372036854775807',
            ],
            'above i64::MAX (the U64 tag)'   => [
                ['tag' => C::TAG_U64, 'data' => '18446744073709551615'],
                '18446744073709551615',
            ],
        ];
    }

    /**
     * @param array{tag:int,data:mixed}|null $cell
     * @param int|string|null $expected
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('keys')]
    public function testLastInsertIdMirrorsTheTerminalField(?array $cell, int|string|null $expected): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, 1, $cell);
        $c = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');

        $this->assertNull($c->lastInsertId(), 'no statement has run yet');
        $c->exec('INSERT INTO t (v) VALUES (1)');
        $this->assertSame($expected, $c->lastInsertId());
    }

    /** A later statement that reports no key CLEARS the earlier one — never a stale carry-over. */
    public function testASubsequentKeylessStatementClearsIt(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, 1, ['tag' => C::TAG_I64, 'data' => 42]);
        $this->feedOk($t, 2, null);
        $c = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');

        $c->exec('INSERT INTO t (v) VALUES (1)');
        $this->assertSame(42, $c->lastInsertId());

        $c->query('SELECT v FROM t');
        $this->assertNull($c->lastInsertId(), 'a read must not leave the previous key behind');
    }

    /** Frame an `Ok` ExecOk terminal carrying `$cell` as `last_insert_id`. */
    private function feedOk(FakeTransport $t, int $requestId, ?array $cell): void
    {
        $body = ExecOk::encode([
            'cols' => [],
            'rows' => [],
            'affected' => 1,
            'last_insert_id' => $cell,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ], $this->packer);
        $payload = Outcome::ok($body)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $requestId, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }
}
