<?php // /php/doctrine-dbal/src/BindKind.php
declare(strict_types=1);
namespace Ferro\DBAL;

/**
 * What a bound DBAL parameter is AS FAR AS BINDING IS CONCERNED — the major-independent meaning of
 * a `ParameterType`, which DBAL 4 spells as an enum and DBAL 3 as int constants (M2-C5).
 *
 * `Natural` is `STRING`/`ASCII`: under those the PHP TYPE decides, because DBAL routes floats, ints,
 * numeric strings and even bools through `STRING` (see {@see CanonicalBinder}).
 */
enum BindKind
{
    case Null;
    case Boolean;
    case Integer;
    case Binary;
    case Natural;
}
