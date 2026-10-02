<?php // /php/doctrine-dbal/tests/Live/SearchPathLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;

/**
 * M2-C5b review F1 — DBAL 3's PostgreSQL schema manager decides the CURRENT schema from the
 * connection's `user` PARAMETER, and a Ferro connection has none.
 *
 * DBAL 3.10.6's `getSchemaSearchPaths()` reads `SHOW search_path` and substitutes `"$user"` with
 * `$params['user']`; `getCurrentSchema()` takes the first EXISTING schema of the result, and
 * `_getPortableTableDefinition()` names every table relative to it. With no `user` param the literal
 * `"$user"` matches no schema, so when the role HAS a schema of its own name — PostgreSQL's
 * documented secure-schema-usage pattern — DBAL 3 believes the current schema is the next one in
 * the path. A table that is unqualified to PostgreSQL then comes back schema-qualified, and the
 * comparator plans a CREATE of the unqualified name plus a DROP of the qualified one. DBAL 4 asks
 * the server (`SELECT current_schema()`) and is not affected.
 *
 * The test pins all three cells, so a change in either major's behaviour, or in whether Ferro reads
 * the `user` param, turns it red:
 *  - DBAL 3, no `user` param: the WRONG current schema (the incompatibility, asserted on purpose);
 *  - DBAL 3, `user` = the backend role: the right one (the documented workaround — the param is
 *    inert for Ferro's own connection, which takes no credentials, SPEC §12 / D8);
 *  - DBAL 4, no `user` param: the right one.
 * PostgreSQL's own `current_schema()` is the oracle in every cell.
 */
final class SearchPathLiveTest extends DbalLiveTestCase
{
    /** @param array<string,mixed> $extra */
    private function dbalWith(array $extra = []): DbalConnection
    {
        $c = DriverManager::getConnection([
            'driverClass' => self::driverClass(),
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ] + $extra);
        self::assertInstanceOf(\Ferro\Client\Connection::class, $c->getNativeConnection());
        return $c;
    }

    /** @return list<string> */
    private static function ours(DbalConnection $c): array
    {
        $names = array_values(array_filter(
            $c->createSchemaManager()->listTableNames(),
            static fn (string $n): bool => str_contains($n, 'c5b_sp_'),
        ));
        sort($names);
        return $names;
    }

    public function testTheCurrentSchemaFollowsTheUserParamOnDbal3AndTheServerOnDbal4(): void
    {
        $admin = $this->dbal();
        $role = $admin->fetchOne('SELECT current_user');
        self::assertIsString($role);
        // CREATE without IF NOT EXISTS: a schema of this name that already exists belongs to
        // someone else, and the `finally` below would drop it.
        $admin->executeStatement(sprintf('CREATE SCHEMA %s', $admin->quoteIdentifier($role)));
        try {
            $admin->executeStatement(sprintf('CREATE TABLE %s.c5b_sp_mine (id int)', $admin->quoteIdentifier($role)));
            $admin->executeStatement('CREATE TABLE public.c5b_sp_shared (id int)');

            self::assertSame($role, $admin->fetchOne('SELECT current_schema()'), 'the oracle: "$user" resolves first');
            $right = ['c5b_sp_mine', 'public.c5b_sp_shared'];

            if (self::isDbal3()) {
                self::assertSame(
                    ['c5b_sp_shared', $role . '.c5b_sp_mine'],
                    self::ours($this->dbalWith()),
                    'DBAL 3 with no `user` param must still pick the WRONG current schema — if this '
                    . 'turns red, re-check SPEC §22.2 (bz) and the incompatibilities page',
                );
                self::assertSame($right, self::ours($this->dbalWith(['user' => $role])), 'the documented workaround');
            } else {
                self::assertSame($right, self::ours($this->dbalWith()), 'DBAL 4 asks the server');
            }
        } finally {
            $admin->executeStatement(sprintf('DROP SCHEMA %s CASCADE', $admin->quoteIdentifier($role)));
            $admin->executeStatement('DROP TABLE IF EXISTS public.c5b_sp_shared');
        }
    }
}
