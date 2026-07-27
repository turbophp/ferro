<?php // /php/client/src/Client/TransportInterface.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * The byte-level duplex the {@see Session} drives: exact-length reads, whole-buffer writes, and a
 * clean close. Abstracted so the session can be exercised over an in-memory fake in unit tests
 * (feed reply bytes, capture writes) as well as the real {@see Transport} socket.
 */
interface TransportInterface
{
    /**
     * Read EXACTLY `$n` bytes, blocking until they arrive. Throws
     * {@see \Ferro\Client\Error\TransportException} on EOF/timeout/error before `$n` bytes.
     */
    public function readExact(int $n): string;

    /** Write the whole buffer, looping over partial writes. Throws on error/short write. */
    public function writeAll(string $bytes): void;

    /** Close the underlying transport. Idempotent. */
    public function close(): void;
}
