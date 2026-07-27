<?php // /php/client/tests/Support/PersonDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

/**
 * A `final readonly` DTO with constructor-promoted properties, used by the hydration tests. The
 * camelCase parameters map to snake_case result columns (`firstName` → `first_name`,
 * `isActive` → `is_active`).
 */
final readonly class PersonDto
{
    public function __construct(
        public int $id,
        public string $firstName,
        public bool $isActive,
    ) {}
}
