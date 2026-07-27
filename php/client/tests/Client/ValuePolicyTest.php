<?php // /php/client/tests/Client/ValuePolicyTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The M0 value policy table: each canonical scalar tag → its PHP type/value, and every reserved M1
 * tag is a loud Unsupported (never a silent miscast).
 */
final class ValuePolicyTest extends TestCase
{
    public function testScalarTagsDecodeToPhpTypes(): void
    {
        $p = new M0ValuePolicy();

        $this->assertNull($p->decode(C::TAG_NULL, null));

        $this->assertSame(true, $p->decode(C::TAG_BOOL, true));
        $this->assertSame(false, $p->decode(C::TAG_BOOL, false));

        $i = $p->decode(C::TAG_I64, 42);
        $this->assertIsInt($i);
        $this->assertSame(42, $i);

        $f = $p->decode(C::TAG_F64, 1.5);
        $this->assertIsFloat($f);
        $this->assertSame(1.5, $f);

        $t = $p->decode(C::TAG_TEXT, 'hello');
        $this->assertIsString($t);
        $this->assertSame('hello', $t);

        // BYTES arrives as a list<int> (SqlValueCodec::fromWire) → re-assembled to a byte string.
        $b = $p->decode(C::TAG_BYTES, [0x01, 0x02, 0xff]);
        $this->assertIsString($b);
        $this->assertSame("\x01\x02\xff", $b);
    }

    /** @return list<array{int}> */
    public static function unsupportedTagProvider(): array
    {
        return [
            [C::TAG_DECIMAL], [C::TAG_DATE], [C::TAG_TIME], [C::TAG_TIMESTAMP],
            [C::TAG_TIMESTAMPTZ], [C::TAG_UUID], [C::TAG_JSON], [C::TAG_ARRAY],
            [C::TAG_INTERVAL], [C::TAG_INET], [C::TAG_VECTOR], [C::TAG_U64],
        ];
    }

    #[DataProvider('unsupportedTagProvider')]
    public function testReservedM1TagsAreUnsupported(int $tag): void
    {
        $this->expectException(ProtocolException::class);
        (new M0ValuePolicy())->decode($tag, null);
    }
}
