<?php // /php/client/tests/Client/HydrationPlanTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\HydrationException;
use Ferro\Client\Hydration\HydrationPlan;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Tests\Support\PersonDto;
use PHPUnit\Framework\TestCase;

/**
 * The reflection plan is built ONCE per (DTO, column-shape) and memoized — proven by the
 * {@see PlanCache::builds} spy — and the snake_case→camelCase mapping reorders columns correctly,
 * even when the result columns arrive in a different order than the constructor parameters.
 */
final class HydrationPlanTest extends TestCase
{
    public function testMemoizationBuildsPlanOncePerShape(): void
    {
        $cache = new PlanCache();
        $cols = ['id', 'first_name', 'is_active'];

        // Ten calls with the same (DTO, column-shape) → exactly ONE reflection build.
        for ($i = 0; $i < 10; $i++) {
            $cache->planFor(PersonDto::class, $cols);
        }
        $this->assertSame(1, $cache->builds(), 'same shape hydrated N times → one reflection build');
        $this->assertSame(1, $cache->size());

        // A DIFFERENT column shape (same columns, different order) is a distinct plan → a 2nd build.
        $cache->planFor(PersonDto::class, ['is_active', 'id', 'first_name']);
        $this->assertSame(2, $cache->builds());
        $this->assertSame(2, $cache->size());
    }

    public function testPlanReordersColumnsIntoConstructorArgOrder(): void
    {
        // Result columns in a DIFFERENT order than the constructor (id, firstName, isActive).
        $plan = HydrationPlan::build(PersonDto::class, ['is_active', 'id', 'first_name']);
        $args = $plan->argsFor([true, 7, 'Ada']); // values in the result-column order above

        // argsFor must reorder to (id, first_name, is_active).
        $this->assertSame([7, 'Ada', true], $args);
    }

    public function testHydratesFinalReadonlyDto(): void
    {
        $plan = HydrationPlan::build(PersonDto::class, ['id', 'first_name', 'is_active']);
        $args = $plan->argsFor([7, 'Ada', true]);
        $dto = (new \ReflectionClass(PersonDto::class))->newInstanceArgs($args);

        $this->assertSame(7, $dto->id);
        $this->assertSame('Ada', $dto->firstName);
        $this->assertTrue($dto->isActive);
    }

    public function testMissingColumnForParamThrows(): void
    {
        $this->expectException(HydrationException::class);
        HydrationPlan::build(PersonDto::class, ['id', 'first_name']); // no is_active column
    }
}
