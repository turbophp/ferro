<?php // /php/client/src/Client/Error/ProtocolException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * The peer spoke the wire incorrectly for the current exchange: a non-terminal frame where a
 * terminal was required, an unexpected service/method, a terminal whose echoed `request_id` does
 * not match the one sent, an END flag on a control frame that must not carry it. NOT used for the
 * session-fatal `request_id=0` terminal (that routes to {@see ConnectionLostException}, carrying
 * the decoded error) — masking a real session-fatal as a generic id-mismatch would hide its fate.
 */
final class ProtocolException extends FerroException {}
