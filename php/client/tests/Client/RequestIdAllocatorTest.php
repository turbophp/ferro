<?php // /php/client/tests/Client/RequestIdAllocatorTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\RequestIdAllocator;
use PHPUnit\Framework\TestCase;

final class RequestIdAllocatorTest extends TestCase
{
    public function testMonotonicFromZeroSkipsZero(): void
    {
        $ids = new RequestIdAllocator(0);
        $this->assertSame(1, $ids->next());
        $this->assertSame(2, $ids->next());
        $this->assertSame(3, $ids->next());
    }

    public function testWrapsAtU32AndNeverYieldsZero(): void
    {
        // Seed just below the u32 ceiling so the wrap boundary is reachable in three calls.
        $ids = new RequestIdAllocator(0xFFFFFFFE);
        $this->assertSame(0xFFFFFFFF, $ids->next(), 'last u32 value');
        // 0xFFFFFFFF + 1 wraps to 0, which is reserved -> must skip to 1.
        $this->assertSame(1, $ids->next(), 'wraps past 2^32-1 and skips the reserved 0');
        $this->assertSame(2, $ids->next());
    }

    public function testNeverZeroAcrossTheWrap(): void
    {
        $ids = new RequestIdAllocator(0xFFFFFFFD);
        for ($i = 0; $i < 6; $i++) {
            $this->assertNotSame(0, $ids->next(), 'request_id 0 is reserved and must never be allocated');
        }
    }
}
