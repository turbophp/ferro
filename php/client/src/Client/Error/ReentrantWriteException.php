<?php // /php/client/src/Client/Error/ReentrantWriteException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A frame was to be written on a session while another frame was ALREADY being written on it
 * (M6-F8 review round 2, R2-3). This happens only from code that runs in the middle of a write:
 * a destructor run by PHP's cycle collector while the session reads during a blocked write
 * ({@see \Ferro\Client\DuplexTransportInterface}). Writing would splice a whole frame into the
 * middle of the half-written one — garbage to the engine, which closes the session. So it is
 * refused, and NOTHING was written: the request did not run.
 *
 * A client-usage error, deliberately not a {@see TransportException}: the link is healthy, and a
 * lost-link classification would make a read path reconnect. Issue the request after the
 * destructor returns (e.g. from the code that dropped the object), or on another connection.
 */
final class ReentrantWriteException extends FerroException {}
