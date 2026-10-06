<?php // /php/laravel/tests/Live/ReceiveFdsLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Client\Session;
use Ferro\Client\Transport;

/**
 * M3-D3 review F6: the connection config's `ferro_receive_fds` reaches the client. Unset, a
 * connection receives large results by memfd whenever this process can (Linux, `ext-sockets`);
 * `false` opts that connection out.
 */
final class ReceiveFdsLiveTest extends LaravelLiveTestCase
{
    public function testReceiveFdsIsAutoByDefaultAndCanBeTurnedOff(): void
    {
        $auto = $this->connection()->getFerroConnection()->session();
        self::assertInstanceOf(Session::class, $auto);
        self::assertSame(Transport::canReceiveFds(), $auto->receivesFds(), 'auto');

        $off = $this->connection(extra: ['ferro_receive_fds' => false])->getFerroConnection()->session();
        self::assertInstanceOf(Session::class, $off);
        self::assertFalse($off->receivesFds(), 'opted out');
    }
}
