<?php // /php/client/tests/Client/ConnectionTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\PersonDto;
use PHPUnit\Framework\TestCase;

/**
 * Connection query API over an in-memory transport (no ferrod): the READ methods send
 * `readonly=true` (only `exec` defaults false, §19.3-CRITICAL); an `Ok` terminal hydrates; a
 * non-`Ok` terminal throws the mapped exception; `queryOne` is null on an empty result; and a
 * garbled terminal body surfaces as a ProtocolException (the CodecException wrap), never as a raw
 * codec fault escaping the FerroException tree.
 */
final class ConnectionTest extends TestCase
{
    private PackerInterface $packer;
    private Codec $codec;

    protected function setUp(): void
    {
        $this->packer = PackerFactory::forEncode();
        $this->codec = new Codec();
    }

    /** A Connection whose session reads exactly the fed terminal (first allocated request_id = 1). */
    private function connectionWith(FakeTransport $t): Connection
    {
        return new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
    }

    /** Frame an `Ok` ExecOk terminal for request_id 1. @param array<string,mixed> $execOk */
    private function feedOk(FakeTransport $t, array $execOk): void
    {
        $body = ExecOk::encode($execOk, $this->packer);
        $payload = Outcome::ok($body)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    private function feedError(FakeTransport $t, ErrorPayload $ep): void
    {
        $payload = Outcome::error($ep)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    /** Decode the ExecRequest the connection actually wrote. @return array<string,mixed> */
    private function sentRequest(FakeTransport $t): array
    {
        $header = Header::decode(substr($t->written, 0, 16));
        $payload = substr($t->written, 16, $header->payloadLen);
        $off = 0;
        $w = PackerFactory::forDecode()->unpack($payload, $off);
        $this->assertIsArray($w);
        return ExecRequest::mapFromWire($w);
    }

    private static function oneRow(): array
    {
        return [
            'cols' => [['name' => 'n', 'tag' => C::TAG_I64], ['name' => 'm', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => 1], ['tag' => C::TAG_I64, 'data' => 2]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ];
    }

    public function testScalarReturnsFirstColumnOfFirstRow(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, self::oneRow());
        $this->assertSame(1, $this->connectionWith($t)->scalar('SELECT 1 AS n, 2 AS m'));
    }

    public function testQueryReturnsAssocRows(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, self::oneRow());
        $this->assertSame([['n' => 1, 'm' => 2]], $this->connectionWith($t)->query('SELECT 1 AS n, 2 AS m'));
    }

    public function testQueryOneReturnsNullOnEmptyResult(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, [
            'cols' => [['name' => 'n', 'tag' => C::TAG_I64]],
            'rows' => [],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ]);
        $this->assertNull($this->connectionWith($t)->queryOne('SELECT 1 AS n WHERE false'));
    }

    public function testQueryOneHydratesDto(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, [
            'cols' => [
                ['name' => 'id', 'tag' => C::TAG_I64],
                ['name' => 'first_name', 'tag' => C::TAG_TEXT],
                ['name' => 'is_active', 'tag' => C::TAG_BOOL],
            ],
            'rows' => [[
                ['tag' => C::TAG_I64, 'data' => 7],
                ['tag' => C::TAG_TEXT, 'data' => 'Ada'],
                ['tag' => C::TAG_BOOL, 'data' => true],
            ]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);

        $dto = $this->connectionWith($t)->queryOne('SELECT ...', [], PersonDto::class);
        $this->assertInstanceOf(PersonDto::class, $dto);
        $this->assertSame(7, $dto->id);
        $this->assertSame('Ada', $dto->firstName);
        $this->assertTrue($dto->isActive);
    }

    /** §19.3-CRITICAL: every read declares readonly=true (and fetch=rows). */
    public function testReadsSendReadonlyTrue(): void
    {
        foreach (['query', 'queryOne', 'scalar', 'rows'] as $method) {
            $t = new FakeTransport();
            $this->feedOk($t, self::oneRow());
            $this->connectionWith($t)->{$method}('SELECT 1 AS n, 2 AS m');
            $req = $this->sentRequest($t);
            $this->assertTrue($req['readonly'], "{$method}() must send readonly=true");
            $this->assertSame(0, $req['fetch'], "{$method}() fetch=rows");
        }
    }

    /** exec() defaults to the write fate: readonly=false, fetch=none. */
    public function testExecDefaultsReadonlyFalseFetchNone(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, [
            'cols' => [],
            'rows' => [],
            'affected' => 3,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ]);
        $affected = $this->connectionWith($t)->exec('UPDATE t SET x = 1');
        $this->assertSame(3, $affected);

        $req = $this->sentRequest($t);
        $this->assertFalse($req['readonly'], 'exec() defaults readonly=false');
        $this->assertSame(1, $req['fetch'], 'exec() fetch=none');
    }

    /** exec(readonly: true) opts a read-only statement into the safe (Retryable) fate. */
    public function testExecCanOptIntoReadonly(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, [
            'cols' => [],
            'rows' => [],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ]);
        $this->connectionWith($t)->exec('SELECT set_config(...)', [], true);
        $this->assertTrue($this->sentRequest($t)['readonly']);
    }

    public function testErrorTerminalThrowsMappedException(): void
    {
        $t = new FakeTransport();
        $this->feedError($t, new ErrorPayload(
            C::ERR_SYNTAX,
            C::ERR_SYNTAX_BRANCH, // 3 → NonRetryable
            '42601',
            null,
            'syntax error at or near "SELCT"',
            null,
            null,
        ));

        try {
            $this->connectionWith($t)->query('SELCT 1');
            $this->fail('expected NonRetryableException');
        } catch (NonRetryableException $e) {
            $this->assertSame('42601', $e->sqlstate());
            $this->assertSame(C::ERR_SYNTAX, $e->errorCode());
        }
    }

    /** A garbled `Ok` body (not a valid ExecOk) is wrapped as a ProtocolException, not a raw CodecException. */
    public function testGarbledOkBodyIsWrappedAsProtocolException(): void
    {
        $t = new FakeTransport();
        // Ok terminal whose body is a bare nil — not the fixarray(5) ExecOk shape.
        $payload = Outcome::ok($this->packer->packNil())->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));

        $this->expectException(ProtocolException::class);
        $this->connectionWith($t)->query('SELECT 1');
    }
}
