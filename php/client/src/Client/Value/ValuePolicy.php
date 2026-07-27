<?php // /php/client/src/Client/Value/ValuePolicy.php
declare(strict_types=1);
namespace Ferro\Client\Value;

/**
 * Maps a canonical wire `TypedValue` `[tag, payload]` to a PHP value — the "policies over guesses"
 * seam (SPEC §9.1). The engine has already resolved every column to a canonical tag; this policy
 * only decides the PHP REPRESENTATION, deterministically, never inferring from column names or SQL.
 *
 * This is the documented EXTENSION POINT for M1: the DECIMAL / TIMESTAMP / TIMESTAMPTZ / UUID / JSON
 * policies (`naive_datetime_zone=utc`, safe-object defaults, etc.) land as ALTERNATE implementations
 * of this same interface, and a {@see \Ferro\Client\Connection} is constructed with whichever policy
 * the app configures. The M0 default is {@see M0ValuePolicy} (scalar tags only; every reserved M1
 * tag is `Unsupported`).
 */
interface ValuePolicy
{
    /**
     * @param int $tag one of the `Constants::TAG_*` values.
     * @param mixed $data the decoded wire payload for `$tag`, as produced by
     *   {@see \Ferro\Protocol\SqlValueCodec::fromWire} — note BYTES arrives as a `list<int>`.
     * @return mixed the PHP representation of the value.
     */
    public function decode(int $tag, mixed $data): mixed;
}
