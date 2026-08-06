<?php // /php/client/src/Client/Value/TypePolicyOptions.php
declare(strict_types=1);
namespace Ferro\Client\Value;

use Ferro\Protocol\Generated\Constants as C;

/**
 * The SPEC §9.1 "policies over guesses" knobs, as one validated value object:
 * `decimal: object|string`, `naive_datetime_zone: utc|server|error`, `u64_overflow:
 * object|string|error`, `uuid: object|string`. **Defaults are the safe object forms** — Ferro never
 * silently narrows a value the way PDO's per-driver casting does.
 *
 * **Why these are CLIENT-SIDE in M1 — do not "fix" this by adding engine config.** The wire is
 * lossless CANONICAL TEXT (`/proto/PROTOCOL.md` §3.2): the engine renders each backend's native
 * binary form into a policy-INDEPENDENT payload, and the client alone decides what PHP type comes
 * back. So nothing in `ferrod` could read a `decimal=string` pool setting — an operator who set one
 * would observe exactly nothing, while a typo in that inert setting would stop `ferrod` from
 * booting: the worst of both. Pool-level defaults ADVERTISED to the client via `HELLO_ACK` pool
 * metadata are an M1-S8 carry (they are also what `naive_datetime_zone: server` waits on, below).
 *
 * **`naive_datetime_zone: server` is NOT implementable in M1** and is rejected loudly rather than
 * silently downgraded to `utc`: nothing on the wire carries the backend's session timezone
 * (`HelloAck` is `[engine_version, boot_epoch, features, pools, type_registry_hash]`). It lands with
 * the S8 `HELLO_ACK` pool metadata (SPEC §22.2).
 *
 * **`naive_datetime_zone: error` has a PINNED SCOPE: `TAG_TIMESTAMP` alone** (see
 * {@see refusesNaiveTimestamp}). `TIMESTAMPTZ`, `DATE` and `TIME` decode normally under it. Its
 * intended use is migrating a schema off naive `datetime`/`timestamp` columns: reads of a naive
 * column fail loudly instead of being silently assumed UTC. The escape hatches are switching back to
 * `utc` or reading the column with a raw-string policy (canonical text verbatim).
 *
 * A refusal by any of these knobs is a {@see \Ferro\Client\Error\TypePolicyException} — an operator
 * configuration choice, NOT the wire fault a {@see \Ferro\Client\Error\ProtocolException} reports.
 */
final class TypePolicyOptions
{
    /** @var list<string> */
    public const DECIMAL_FORMS = ['object', 'string'];
    /** @var list<string> `server` is deliberately absent — deferred to S8, see the class doc. */
    public const NAIVE_DATETIME_ZONES = ['utc', 'error'];
    /** @var list<string> */
    public const U64_OVERFLOW_FORMS = ['object', 'string', 'error'];
    /** @var list<string> */
    public const UUID_FORMS = ['object', 'string'];

    /**
     * @param string $decimal one of {@see DECIMAL_FORMS}.
     * @param string $naiveDatetimeZone one of {@see NAIVE_DATETIME_ZONES}.
     * @param string $u64Overflow one of {@see U64_OVERFLOW_FORMS}.
     * @param string $uuid one of {@see UUID_FORMS}.
     */
    public function __construct(
        public readonly string $decimal = 'object',
        public readonly string $naiveDatetimeZone = 'utc',
        public readonly string $u64Overflow = 'object',
        public readonly string $uuid = 'object',
    ) {
        if ($naiveDatetimeZone === 'server') {
            throw new \InvalidArgumentException(
                'naive_datetime_zone=server is deferred to M1-S8: nothing on the wire carries the '
                . "backend's session timezone (HELLO_ACK advertises no pool metadata yet), so the "
                . 'client cannot honour it. Use "utc" (the default) or "error" (SPEC §9.1, §22.2).',
            );
        }
        self::check('decimal', $decimal, self::DECIMAL_FORMS);
        self::check('naive_datetime_zone', $naiveDatetimeZone, self::NAIVE_DATETIME_ZONES);
        self::check('u64_overflow', $u64Overflow, self::U64_OVERFLOW_FORMS);
        self::check('uuid', $uuid, self::UUID_FORMS);
    }

    /** The §9.1 defaults: the safe object forms, `naive_datetime_zone=utc`. */
    public static function defaults(): self
    {
        return new self();
    }

    /**
     * Whether `naive_datetime_zone=error` refuses this tag. **Scoped to `TAG_TIMESTAMP` alone** — a
     * naive wall-clock value with no zone on the wire is the only thing the policy is about, so
     * `TIMESTAMPTZ` (an explicit UTC instant), `DATE` and `TIME` are unaffected. Leaving the scope
     * undefined would make whole columns unreadable with no escape hatch, so it is pinned here and
     * every decode path asks this one question rather than re-deriving it.
     */
    public function refusesNaiveTimestamp(int $tag): bool
    {
        return $this->naiveDatetimeZone === 'error' && $tag === C::TAG_TIMESTAMP;
    }

    /**
     * Whether `u64_overflow=error` refuses a `U64` above `PHP_INT_MAX` (the `object`/`string` forms
     * hand back a value object / a decimal string instead). The magnitude test itself belongs to the
     * decoding policy — this only reports the operator's choice.
     */
    public function refusesU64Overflow(): bool
    {
        return $this->u64Overflow === 'error';
    }

    /** @param list<string> $allowed */
    private static function check(string $knob, string $value, array $allowed): void
    {
        if (!in_array($value, $allowed, true)) {
            throw new \InvalidArgumentException(sprintf(
                "%s='%s' is not a known §9.1 policy value; expected one of: %s",
                $knob,
                $value,
                implode(', ', $allowed),
            ));
        }
    }
}
