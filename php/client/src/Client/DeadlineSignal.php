<?php // /php/client/src/Client/DeadlineSignal.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * Internal control flow (M3-D1c): a read reached a request deadline. Raised by
 * {@see Session}'s frame reader and caught inside the session, which then CANCELs the request that
 * is due. Nothing was lost — the stream is still in step — and it never reaches a caller, which is
 * why it is not a {@see Error\FerroException}.
 *
 * @internal
 */
final class DeadlineSignal extends \RuntimeException {}
