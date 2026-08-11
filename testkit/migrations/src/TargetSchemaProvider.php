<?php // /testkit/migrations/src/TargetSchemaProvider.php
declare(strict_types=1);

namespace Ferro\MigrationsAcceptance;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\Provider\SchemaProvider;

/**
 * The TARGET schema `doctrine-migrations diff` compares the live database against.
 *
 * Without an ORM, `diff` has no target, so a provider is the sanctioned way to drive it — this is
 * the same seam `doctrine/migrations`' own docs describe for a DBAL-only project. It is deliberately
 * ORDINARY Doctrine schema code: two tables, a primary key on each, a multi-column secondary index
 * and a foreign key. Every one of those is read back through the STOCK `PostgreSQLSchemaManager`
 * on the next `diff`, which is what makes the second diff a real statement about introspection
 * rather than about this file.
 *
 * The multi-column index is the load-bearing fixture. `PostgreSQLSchemaManager::selectIndexColumns()`
 * selects `i.indkey` — an `int2vector`, OID 22 — and orders the columns by its unnested ordinality.
 * Before the M1-S8c TEXT FALLBACK (D-S8b-6) that column was a loud `Unsupported` and every
 * introspection died on it, which is precisely why `doctrine/migrations` did not work on Ferro.
 */
final class TargetSchemaProvider implements SchemaProvider
{
    public function __construct(private readonly Connection $connection)
    {
    }

    public function createSchema(): Schema
    {
        // The schema config carries the CURRENT schema name and the max identifier length. Without
        // it the target's tables are unqualified while the introspected ones are `public.…`, and
        // every diff reports a spurious drop-and-create.
        $config = $this->connection->createSchemaManager()->createSchemaConfig();
        $schema = new Schema([], [], $config);

        $author = $schema->createTable('s8c_author');
        $author->addColumn('id', 'integer');
        $author->addColumn('name', 'string', ['length' => 64]);
        $author->setPrimaryKey(['id']);

        $book = $schema->createTable('s8c_book');
        $book->addColumn('id', 'integer');
        $book->addColumn('author_id', 'integer');
        $book->addColumn('title', 'string', ['length' => 128]);
        $book->addColumn('published_on', 'date_immutable', ['notnull' => false]);
        $book->setPrimaryKey(['id']);
        // TWO columns, in this order. `indkey` is what carries that order.
        $book->addIndex(['author_id', 'title'], 's8c_book_author_title_idx');
        $book->addForeignKeyConstraint(
            's8c_author',
            ['author_id'],
            ['id'],
            [],
            's8c_book_author_fk',
        );

        return $schema;
    }
}
