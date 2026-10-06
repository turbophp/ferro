<?php // /php/client/src/ManifestQuery.php
declare(strict_types=1);
namespace Ferro;

/** One declared query of a {@see Manifest} (SPEC §11). */
final class ManifestQuery
{
    public function __construct(
        public readonly string $id,
        public readonly string $sql,
        public readonly string $pool,
        public readonly bool $readonly,
        public readonly bool $idempotent,
        public readonly ?string $dto,
    ) {}
}
