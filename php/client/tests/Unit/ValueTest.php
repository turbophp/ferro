<?php // /php/client/tests/Unit/ValueTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Value;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

final class ValueTest extends TestCase
{
    public function testCanonicalBytesMatchRust(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x92\x00\xc0", Value::null()->encode($p));
        $this->assertSame("\x92\x01\xc3", Value::bool(true)->encode($p));
        $this->assertSame("\x92\x02\x01", Value::i64(1)->encode($p));
        $this->assertSame("\x92\x02\xcc\xc8", Value::i64(200)->encode($p)); // uint8, matches Rust
        $this->assertSame("\x92\x02\xd1\xff\x38", Value::i64(-200)->encode($p)); // int16
        $this->assertSame("\x92\x06\xa2hi", Value::text('hi')->encode($p));
        $this->assertSame("\x92\x07\xc4\x03\x01\x02\x03", Value::bytes("\x01\x02\x03")->encode($p));
    }
}
