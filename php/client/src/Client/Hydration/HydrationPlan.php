<?php // /php/client/src/Client/Hydration/HydrationPlan.php
declare(strict_types=1);
namespace Ferro\Client\Hydration;

use Ferro\Client\Error\HydrationException;

/**
 * The memoized reflection artifact for hydrating a result set into a `final readonly` DTO with
 * constructor-promoted properties. Built ONCE per (DTO class, column-name tuple) by {@see build}
 * and cached in {@see PlanCache} — reflection never runs again for the same shape, no matter how
 * many rows are hydrated.
 *
 * The plan itself is pure data: for each constructor parameter (in declaration order) it records the
 * INDEX of the result column that feeds it, resolved by mapping the camelCase parameter name to a
 * snake_case column (`firstName` → `first_name`), with an exact-name fallback. Applying the plan
 * ({@see argsFor}) is a cheap array reorder; the actual `newInstanceArgs` instantiation is done by
 * the caller with a `class-string<T>`-typed `ReflectionClass` so the DTO type is preserved for L9.
 */
final class HydrationPlan
{
    /** @param list<int> $paramColumnIndex constructor-param position → result-column index */
    private function __construct(private readonly array $paramColumnIndex) {}

    /**
     * Reflect `$class`'s constructor ONCE against the result `$columnNames` and produce the plan.
     *
     * @param class-string $class
     * @param list<string> $columnNames the result columns, in wire order.
     * @throws HydrationException if the DTO has no constructor, or a parameter has no matching column.
     */
    public static function build(string $class, array $columnNames): self
    {
        $ctor = (new \ReflectionClass($class))->getConstructor();
        if ($ctor === null) {
            throw new HydrationException(sprintf('DTO %s has no constructor to hydrate into', $class));
        }

        /** @var array<string,int> $indexByColumn */
        $indexByColumn = [];
        foreach ($columnNames as $i => $name) {
            $indexByColumn[$name] = $i; // last-wins on duplicate column names (harmless for M0)
        }

        $paramColumnIndex = [];
        foreach ($ctor->getParameters() as $param) {
            $paramName = $param->getName();
            $snake = self::camelToSnake($paramName);
            if (array_key_exists($snake, $indexByColumn)) {
                $paramColumnIndex[] = $indexByColumn[$snake];
            } elseif (array_key_exists($paramName, $indexByColumn)) {
                $paramColumnIndex[] = $indexByColumn[$paramName];
            } else {
                throw new HydrationException(sprintf(
                    'no result column for DTO %s parameter $%s (looked for column "%s")',
                    $class,
                    $paramName,
                    $snake,
                ));
            }
        }

        return new self($paramColumnIndex);
    }

    /**
     * Reorder one row's already-policy-decoded cell values into constructor-argument order.
     *
     * @param list<mixed> $rowValues the row's cells, in result-column order.
     * @return list<mixed> the constructor arguments, in parameter order.
     */
    public function argsFor(array $rowValues): array
    {
        $args = [];
        foreach ($this->paramColumnIndex as $idx) {
            $args[] = $rowValues[$idx] ?? null;
        }
        return $args;
    }

    /** `firstName` → `first_name`, `userId` → `user_id`, `id` → `id`. */
    private static function camelToSnake(string $s): string
    {
        $out = preg_replace('/([a-z0-9])([A-Z])/', '$1_$2', $s);
        return strtolower($out ?? $s);
    }
}
