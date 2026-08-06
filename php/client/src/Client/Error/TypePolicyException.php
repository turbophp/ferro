<?php // /php/client/src/Client/Error/TypePolicyException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * An operator's SPEC §9.1 type policy REFUSED to decode a value that arrived intact: a naive
 * `TIMESTAMP` under `naive_datetime_zone=error`, a `U64` above `PHP_INT_MAX` under
 * `u64_overflow=error`. The wire was correct and the statement succeeded — the configuration says
 * "do not guess here", so the client reports instead of silently miscasting.
 *
 * **The split is by CAUSE, and it is load-bearing.** A MALFORMED payload — a `DECIMAL` that is not a
 * number, a truncated `TIMESTAMP`, a `UUID` of the wrong length — stays a {@see ProtocolException}:
 * the peer spoke the wire incorrectly. A POLICY refusal is this class. Routing a configuration
 * choice into the wire-fault class would make the Doctrine tier's `ExceptionConverter` (§14) report
 * an operator setting as a driver protocol failure, and would tell an operator to debug the engine
 * when the fix is one policy value.
 *
 * **It is not a §19.3 fate signal.** It is raised client-side inside
 * {@see \Ferro\Client\ExecCodec::decodeRow} — i.e. AFTER the statement already succeeded, and on the
 * streamed path after earlier rows were already yielded — so it sits in the {@see FerroException}
 * tree but deliberately OUTSIDE the Retryable/Indeterminate/NonRetryable branches mapped from the
 * wire `branch` byte. Nothing retries it and no transaction re-runs on it (it is deterministic:
 * replaying the statement would refuse the same cell again).
 */
final class TypePolicyException extends FerroException {}
