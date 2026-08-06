<?php // /php/client/tests/Unit/TypePolicyExceptionTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\Error\CancelledException;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A §9.1 policy REFUSAL (`naive_datetime_zone=error`, `u64_overflow=error`) is an operator
 * configuration choice — NOT a wire fault and NOT a §19.3 fate signal.
 *
 *  - It is its own class ({@see TypePolicyException}), so S8's DBAL `ExceptionConverter` cannot
 *    misreport a configuration choice as a driver protocol failure. A MALFORMED payload keeps
 *    raising {@see ProtocolException}; the split is by CAUSE.
 *  - It fires mid-row, inside {@see ExecCodec::decodeRow} — i.e. AFTER the statement succeeded, and
 *    on the streamed path after earlier rows were already yielded. It must survive that trip
 *    unrewritten (the buffered `decode()` wraps only `CodecException`) and must never license a
 *    retry or a transaction re-run.
 */
final class TypePolicyExceptionTest extends TestCase
{
    /** A stand-in for the M1 policy (Task 7): refuses TAG_TIMESTAMP the way `zone=error` will. */
    private static function refusingPolicy(): ValuePolicy
    {
        return new class implements ValuePolicy {
            public function decode(int $tag, mixed $data): mixed
            {
                if ($tag === C::TAG_TIMESTAMP) {
                    throw new TypePolicyException('naive_datetime_zone=error refuses a naive TIMESTAMP column');
                }
                return $data;
            }
        };
    }

    private static function codecWith(ValuePolicy $p): ExecCodec
    {
        return new ExecCodec($p, new PlanCache(), PackerFactory::forEncode(), PackerFactory::forDecode());
    }

    // ---- taxonomy placement ---------------------------------------------------------------------

    public function testItLivesInTheFerroExceptionTree(): void
    {
        self::assertInstanceOf(FerroException::class, new TypePolicyException('x'));
    }

    /** @return list<array{class-string<\Throwable>}> */
    public static function otherFamilyProvider(): array
    {
        return [
            [ProtocolException::class],      // a WIRE fault — a policy refusal is not one
            [RetryableException::class],     // §19.3 fate branches: never
            [IndeterminateException::class],
            [NonRetryableException::class],
            [ConnectionLostException::class],
            [TransportException::class],
            [CancelledException::class],
        ];
    }

    #[DataProvider('otherFamilyProvider')]
    public function testItIsNotMistakenForAWireFaultOrAFateSignal(string $other): void
    {
        self::assertNotInstanceOf($other, new TypePolicyException('x'));
    }

    // ---- it survives the decode path unrewritten -------------------------------------------------

    public function testARefusalEscapesDecodeRowUnrewritten(): void
    {
        $codec = self::codecWith(self::refusingPolicy());
        $this->expectException(TypePolicyException::class);
        $codec->decodeRow([['tag' => C::TAG_TIMESTAMP, 'data' => '2026-08-06 11:45:07']]);
    }

    public function testARefusalEscapesTheBufferedTerminalDecodeUnrewritten(): void
    {
        $codec = self::codecWith(self::refusingPolicy());
        $outcome = FakeSession::execOk([
            'cols' => [['name' => 'created_at', 'tag' => C::TAG_TIMESTAMP]],
            'rows' => [[['tag' => C::TAG_TIMESTAMP, 'data' => '2026-08-06 11:45:07']]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);

        // `decode()` catches CodecException and rewrites it to ProtocolException; a policy refusal
        // must pass straight through that arm.
        $this->expectException(TypePolicyException::class);
        $codec->decode($outcome);
    }

    // ---- it never licenses a re-run --------------------------------------------------------------

    public function testAPolicyRefusalInsideATransactionRollsBackAndNeverReRunsTheClosure(): void
    {
        $s = (new FakeSession())
            ->push(FakeSession::beginOk(42))
            ->push(FakeSession::controlOk());   // the best-effort ROLLBACK ack

        $conn = new Connection(session: $s, pool: 'p');
        $ran = 0;

        try {
            $conn->transaction(function () use (&$ran): void {
                ++$ran;
                throw new TypePolicyException('u64_overflow=error refuses a value above PHP_INT_MAX');
            });
            self::fail('the policy refusal must propagate');
        } catch (TypePolicyException) {
            // expected
        }

        self::assertSame(1, $ran, 'a policy refusal is deterministic — re-running it is pointless');
        self::assertSame([C::SERVICE_TX, C::METHOD_TX_ROLLBACK], $s->lastInFlight());
    }

    // ---- the M1 seam is inert TODAY, but safely so ------------------------------------------------

    /**
     * Until Task 7 lands the M1 policy, a `types:`-configured Connection still decodes with
     * {@see M0ValuePolicy} — which raises a LOUD ProtocolException for every tag the four §9.1 knobs
     * govern. So the seam cannot silently miscast anything in the meantime.
     *
     * @return list<array{int}>
     */
    public static function governedTagProvider(): array
    {
        return [[C::TAG_DECIMAL], [C::TAG_TIMESTAMP], [C::TAG_U64], [C::TAG_UUID]];
    }

    #[DataProvider('governedTagProvider')]
    public function testEveryPolicyGovernedTagIsStillALoudRefusalUnderM0(int $tag): void
    {
        $this->expectException(ProtocolException::class);
        (new M0ValuePolicy())->decode($tag, 'whatever');
    }
}
