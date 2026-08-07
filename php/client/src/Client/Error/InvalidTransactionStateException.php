<?php // /php/client/src/Client/Error/InvalidTransactionStateException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A transaction-lifecycle API was called in a state that cannot support it: `commit()`/`rollBack()`
 * with nothing open, a nested `begin()`, or `transaction()` while an imperative transaction is open
 * (M1-S8a Task 9).
 *
 * A distinct LEAF class, not a bare {@see FerroException}, because these are the only misuses this
 * client can detect purely from its own state — and because a test asserting on the ROOT of the
 * exception tree passes for ANY Ferro error, including one thrown by the test's own setup SQL. Every
 * misuse test in this slice names this class.
 *
 * Never a taxonomy error: no request was sent, so there is no `ErrorPayload` and no fate. Nothing is
 * dangling engine-side either — a misuse is refused BEFORE any frame is written.
 */
final class InvalidTransactionStateException extends FerroException {}
