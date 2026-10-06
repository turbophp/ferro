<?php // /php/client/src/Client/SelectableTransportInterface.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * A transport whose underlying stream can be watched with `stream_select` (M3-D1b), so a scheduler
 * can wait on several sessions at once and read only from the ones that have data.
 */
interface SelectableTransportInterface extends TransportInterface
{
    /** @return resource|null the stream to select on, or null once closed. */
    public function stream(): mixed;

    /**
     * Seconds a blocking read waits before failing. A scheduler that selects instead of reading
     * uses it as the session's deadline, so a silent peer fails as soon as it would synchronously.
     */
    public function readTimeout(): float;
}
