<?php // /php/doctrine-dbal/src/IsolationStatement.php
declare(strict_types=1);
namespace Ferro\DBAL;

/**
 * Recognises the two SESSION-level isolation statements Doctrine's platforms generate, which
 * {@see AbstractConnection} refuses on every statement entry point (SPEC §22.2 (s), (ad)).
 *
 * It lived on the DBAL 4 wrapper class until M2-C5. It moved because the CONNECTION needs it on
 * both majors, and that wrapper cannot even be loaded under DBAL 3 (it overrides
 * `setTransactionIsolation()` with DBAL 4's typed signature) — so leaving it there would have made
 * the refusal fatal on DBAL 3 instead of loud.
 */
final class IsolationStatement
{
    /**
     * A CLOSED, prefix-anchored test on the two fixed strings — not open-ended SQL parsing. It is
     * anchored so a literal appearing inside an INSERT or a comparison cannot trip it, which
     * matters: a refusal that fired on ordinary SQL would be far worse than the bug it prevents.
     * DBAL 3 and DBAL 4 generate the same two strings (verified against 3.10.6 and 4.4.4).
     */
    public static function matches(string $sql): bool
    {
        $t = ltrim($sql);
        foreach ([
            'SET SESSION TRANSACTION ISOLATION LEVEL',
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL',
        ] as $prefix) {
            if (strncasecmp($t, $prefix, strlen($prefix)) === 0) {
                return true;
            }
        }
        return false;
    }
}
