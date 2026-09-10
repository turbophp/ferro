<?php // /php/laravel/src/ServerVersion.php
declare(strict_types=1);
namespace Ferro\Laravel;

/**
 * Turns the engine's RAW backend version string into one Illuminate's `version_compare` calls read
 * correctly.
 *
 * **The wire value is deliberately raw, and normalising it is this tier's job.** `ferrod` caches the
 * backend's own `version()` output VERBATIM, and {@see \Ferro\Protocol\PoolInfo}'s docblock says so
 * in as many words. On PostgreSQL that output leads with the product name:
 *
 *     PostgreSQL 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc …
 *
 * **and PHP's `version_compare` reads a leading non-numeric part as OLDER THAN ANY NUMBER.** MEASURED
 * against that exact string: `version_compare($raw, '12.0', '<')` and `version_compare($raw, '14.0',
 * '<')` are both `true` on a PostgreSQL 16 server. It does not throw, it does not warn — it just
 * answers wrong, which is why this survived a green tier suite and was only caught by upstream
 * `laravel/framework`'s own `SchemaBuilderTest::testGetAndDropTypes`, whose assertion count branches
 * on `version_compare(…, '14.0', '<')`.
 *
 * **The harm is silently wrong introspection SQL, not a failed query.** Stock
 * `PostgresGrammar::compileColumns()` selects `'' as generated` instead of `a.attgenerated` when it
 * believes the server is pre-12, so `Schema::getColumns()` reports EVERY column as non-generated on
 * a modern PostgreSQL — and a `GENERATED ALWAYS AS … STORED` column then looks like an ordinary
 * writable one to anything diffing or dumping the schema. That is exactly the failure
 * {@see FerroPdoShim::getAttribute}'s own docblock said it existed to prevent; its nil-version guard
 * was right and incomplete, because a version that is present but unparseable is just as wrong and
 * much quieter.
 *
 * **The rule is the sibling Doctrine tier's, deliberately unchanged** (`Ferro\DBAL\PlatformVersion
 * ::normalise`, which measured the same string breaking DBAL's anchored parser): strip PostgreSQL's
 * leading product name and NOTHING else. Minimal on purpose — every extra rule is another chance to
 * discard a suffix that turns out to be load-bearing, and the MySQL family is precisely that case,
 * where MariaDB is detected ONLY by the `-MariaDB-` substring, so its string must pass through
 * byte-identical. The two packages cannot share code (`ferro/laravel` does not depend on
 * `ferro/doctrine-dbal-driver`), so they share the rule and this comment instead.
 */
final class ServerVersion
{
    /** The `PoolInfo.kind` wire values (`PoolKind::wire_name()` in `ferrod`). Never nil. */
    public const KIND_POSTGRES = 'postgres';
    public const KIND_MYSQL = 'mysql';

    public static function normalise(string $kind, string $raw): string
    {
        if ($kind !== self::KIND_POSTGRES) {
            return $raw;
        }
        return preg_replace('/^\s*PostgreSQL\s+/i', '', $raw) ?? $raw;
    }
}
