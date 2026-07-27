<?php // /php/client/src/Client/Error/IndeterminateException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * Branch 2 ({@see \Ferro\Protocol\Generated\Constants::BRANCH_INDETERMINATE}) — the spec's defining
 * safety property (SPEC §9.2 / §19.3): a write that was TRANSMITTED but whose fate is UNKNOWN
 * (`WriteUnconfirmed`; a connection lost mid-flight on a non-readonly statement). The engine NEVER
 * transparently retries it and NEITHER DOES THE CLIENT — this exception propagates to the caller,
 * whose policy decides. Auto-retry is licensed ONLY by a manifest `idempotent: true` (M3), never by
 * a default. Carries the decoded `ErrorPayload`.
 */
final class IndeterminateException extends FerroException
{
    use CarriesErrorPayload;
}
