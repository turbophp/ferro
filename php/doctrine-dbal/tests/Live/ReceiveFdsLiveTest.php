<?php // /php/doctrine-dbal/tests/Live/ReceiveFdsLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\Session;
use Ferro\Client\Transport;

/**
 * M3-D3 review F6: `driverOptions.receive_fds` reaches the client. Unset, a connection receives
 * large results by memfd whenever this process can (Linux, `ext-sockets`); `false` opts it out
 * — the per-connection switch an application can configure, rather than unloading `ext-sockets`
 * process-wide or turning the path off for every tenant at the engine.
 */
final class ReceiveFdsLiveTest extends DbalLiveTestCase
{
    public function testReceiveFdsIsAutoByDefaultAndCanBeTurnedOff(): void
    {
        self::assertSame(Transport::canReceiveFds(), self::sessionOf($this->dbal())->receivesFds(), 'auto');
        self::assertFalse(self::sessionOf($this->dbal(extraOptions: ['receive_fds' => false]))->receivesFds(), 'opted out');
    }

    private static function sessionOf(\Doctrine\DBAL\Connection $c): Session
    {
        $native = $c->getNativeConnection();
        self::assertInstanceOf(FerroClientConnection::class, $native);
        $session = $native->session();
        self::assertInstanceOf(Session::class, $session);
        return $session;
    }
}
