<?php // /php/client/src/Client/Error/TransportException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A raw transport-layer failure: connect refused/timed out, a read that hit EOF or a read/write
 * timeout, a short write. Distinct from a protocol-level fault (a well-formed connection that
 * carried an unexpected frame) — this is the socket itself failing.
 */
final class TransportException extends FerroException {}
