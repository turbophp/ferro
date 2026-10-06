<?php // /php/client/src/Client/FdReceivingTransportInterface.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * A transport that can receive file descriptors passed with `SCM_RIGHTS` (M3-D3, SPEC §5.1) — the
 * receive half of the `MEMFD_RX` out-of-band path. Only {@see Transport} implements it, and only
 * over a Unix domain socket on Linux with `ext-sockets` loaded; {@see receivesFds} says whether THIS
 * instance does, and the {@see Session} advertises `MEMFD_RX` exactly when it is true.
 *
 * **The pairing contract.** The engine attaches exactly one fd to the FIRST byte of each `OOB_FD`
 * frame and to nothing else, and a receiving transport reads every byte with `recvmsg`, queueing the
 * fds in the order they arrive. A single read may return an fd together with EARLIER bytes that are
 * not its frame's, so an fd is never paired by position in a read: the n-th `OOB_FD` frame takes the
 * n-th fd ({@see takeFd}), and its fd has always arrived by the time that frame's header has been
 * read.
 */
interface FdReceivingTransportInterface extends TransportInterface
{
    /** Whether this transport reads with `recvmsg` and so can be sent fds. */
    public function receivesFds(): bool;

    /**
     * The oldest received fd not yet taken, as the stream resource `ext-sockets` wraps it in, or
     * null when none is queued. The caller owns it and must close it.
     *
     * @return resource|null
     */
    public function takeFd(): mixed;

    /** How many fds this transport has received so far. Diagnostic: proves the path was taken. */
    public function fdsReceived(): int;
}
