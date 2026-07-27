<?php // /php/client/src/Client/Hydration/PlanCache.php
declare(strict_types=1);
namespace Ferro\Client\Hydration;

/**
 * Memoizes {@see HydrationPlan}s keyed by (DTO class, column-name tuple), so the reflection build
 * happens EXACTLY ONCE per shape across the life of a {@see \Ferro\Client\Connection} — every
 * subsequent query of the same DTO against the same column shape reuses the cached plan.
 *
 * {@see builds} exposes the number of reflection builds performed so the memoization is directly
 * testable (N `planFor` calls of one shape → `builds() === 1`).
 */
final class PlanCache
{
    /** @var array<string, HydrationPlan> */
    private array $plans = [];

    private int $builds = 0;

    /**
     * @param class-string $class
     * @param list<string> $columnNames
     */
    public function planFor(string $class, array $columnNames): HydrationPlan
    {
        // NUL is not a legal class-name or SQL identifier char, so it is an unambiguous key joiner.
        $key = $class . "\0" . implode("\0", $columnNames);
        if (!array_key_exists($key, $this->plans)) {
            $this->plans[$key] = HydrationPlan::build($class, $columnNames);
            ++$this->builds;
        }
        return $this->plans[$key];
    }

    /** Number of distinct (class, column-shape) plans cached. */
    public function size(): int { return count($this->plans); }

    /** Number of reflection builds performed — a spy for the memoization test. */
    public function builds(): int { return $this->builds; }
}
