<?php // /php/client/src/Client/Value/ValuePolicy.php
declare(strict_types=1);
namespace Ferro\Client\Value;

/**
 * Maps a canonical wire `TypedValue` `[tag, payload]` to a PHP value — the "policies over guesses"
 * seam (SPEC §9.1). The engine has already resolved every column to a canonical tag; this policy
 * only decides the PHP REPRESENTATION, deterministically, never inferring from column names or SQL.
 *
 * This is the documented EXTENSION POINT, and as of M1-S7 it has three implementations:
 *
 *  - {@see M1ValuePolicy} — the DEFAULT ({@see \Ferro\Client\Connection} builds one from its
 *    `types:` {@see TypePolicyOptions}). All fourteen implemented tags → the SPEC §9 PHP types under
 *    the four §9.1 knobs.
 *  - {@see RawStringValuePolicy} — the canonical wire text verbatim for the eight M1-S7 tags; the
 *    M1-S8 Doctrine DBAL hand-off, where the type layer wants driver-native strings.
 *  - {@see M0ValuePolicy} — the historical scalar-only policy (every M1 tag `Unsupported`), kept for
 *    the M0 conformance tests.
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
