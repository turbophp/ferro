<?php // /php/client/src/Attribute/FerroQuery.php
declare(strict_types=1);
namespace Ferro\Attribute;

/**
 * Declares a checked query (SPEC §11, M3-D2a): the `ferro` CLI collects it into the manifest, the
 * engine runs it by `id`, and `idempotent: true` is the only licence to retry an `Indeterminate`
 * write (§9.2).
 *
 *     #[FerroQuery(id: 'users.find', sql: 'SELECT id, email FROM users WHERE id = ?', readonly: true)]
 *     final class FindUser {}
 *
 * The arguments must be literals — `vendor/bin/ferro-queries` reads them with PHP's tokenizer,
 * without running the code, so a constant or an expression cannot be resolved and is refused.
 */
#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD | \Attribute::TARGET_CLASS_CONSTANT | \Attribute::IS_REPEATABLE)]
final class FerroQuery
{
    public function __construct(
        public readonly string $id,
        public readonly string $sql,
        public readonly string $pool = 'default',
        public readonly bool $readonly = false,
        public readonly bool $idempotent = false,
        public readonly ?string $dto = null,
    ) {}
}
