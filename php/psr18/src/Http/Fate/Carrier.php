<?php // /php/psr18/src/Http/Fate/Carrier.php
declare(strict_types=1);
namespace Ferro\Http\Fate;

use Ferro\Http\HttpFate;

/**
 * Anything that knows its Ferro HTTP fate (SPEC §23.11.2, §23.11.4): a response built by a Ferro
 * adapter (`Ferro\Http\FerroResponse` from `ferro/guzzle`, {@see \Ferro\Psr18\FatedResponse}) and
 * every Ferro adapter exception (through {@see Retryable}, {@see NonRetryable} or
 * {@see Indeterminate}). {@see \Ferro\Http\Fate::of()} is the one reader.
 */
interface Carrier
{
    public function ferroFate(): HttpFate;
}
