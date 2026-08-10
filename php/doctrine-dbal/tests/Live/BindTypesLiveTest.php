<?php // /php/doctrine-dbal/tests/Live/BindTypesLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Types;

/**
 * M1-S8b Task 7, live — a stock Doctrine type-layer round trip on PostgreSQL, where the bind
 * pre-flight is NARROW and therefore where the mapping is actually tested. MySQL has no such
 * pre-flight at all (its `COM_STMT_PREPARE` exposes no inferred parameter types), so a driver
 * developed against MySQL alone would look correct and fail on every typed PG column — which is why
 * the PG half comes first here and is the more detailed one.
 */
final class BindTypesLiveTest extends DbalLiveTestCase
{
    public function testTheStockTypeLayerRoundTripsOnPostgres(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_bind');
        $c->executeStatement(
            'CREATE TABLE s8b_bind (id int primary key, flag boolean, n bigint, s text, '
            . 'b bytea, d date, ts timestamp, num numeric(12,4), j jsonb, u uuid)',
        );

        // Bound through DBAL's own $types map, i.e. through convertToDatabaseValue() +
        // getBindingType() — the path a real application takes.
        $c->executeStatement(
            'INSERT INTO s8b_bind (id, flag, n, s, b, d, ts, num, j, u) '
            . 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                1,
                true,
                // 2^31 (needs an int8) — deliberately BELOW 2^32. A bigint at or above 2^32 is
                // currently UNREADABLE by the PHP client on every value policy: rmp's `write_sint`
                // delegates non-negative values to `write_uint`, PurePacker hands any `0xcf` back as
                // a decimal STRING, and `CanonicalText::requireInt` refuses a string for `TAG_I64`.
                // Measured live on the NATIVE client with no DBAL in the picture (see the Task 7
                // journal). It is a READ-path defect that predates this task; using 2^31 here keeps
                // this test measuring the BIND direction it exists to measure instead of failing on
                // an unrelated bug.
                2147483648,
                'hello',
                "\x00\x01\xff",
                // DBAL 4.4's *_MUTABLE and *_IMMUTABLE Types are DIFFERENT classes with an
                // `instanceof` gate each (`DateType` refuses a `DateTimeImmutable` outright — the
                // plan's snippet paired them the wrong way round and errored before reaching the
                // driver). Both classes are exercised here, one per column, because both produce
                // the same canonical text and either could regress alone.
                new \DateTimeImmutable('2026-08-05'),
                new \DateTime('2026-08-05 13:45:07'),
                '1.2500',
                ['a' => 1],
                '0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60',
            ],
            [
                Types::INTEGER,
                Types::BOOLEAN,
                Types::BIGINT,
                Types::STRING,
                ParameterType::BINARY,
                Types::DATE_IMMUTABLE,
                Types::DATETIME_MUTABLE,
                Types::DECIMAL,
                Types::JSON,
                Types::GUID,
            ],
        );

        $row = $c->fetchAssociative('SELECT * FROM s8b_bind WHERE id = ?', [1]);
        self::assertIsArray($row);
        self::assertTrue($row['flag']);
        self::assertSame('hello', $row['s']);
        self::assertSame('1.2500', $row['num'], 'a decimal keeps its display scale end to end');
        self::assertSame('0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60', $row['u']);

        // STRENGTHENED beyond the plan: read every remaining column back through PG's OWN renderer.
        // The insert merely succeeding proves the pre-flight ACCEPTED each parameter; these
        // assertions prove PG PARSED it into the column's type rather than storing something else.
        // `j` is the sharpest of them — `{"a": 1}` (with the space) is jsonb's canonical re-render,
        // which only exists if the payload went through jsonb's input parser, and the `::text` casts
        // keep the whole check independent of the driver's own read path (Task 9's territory).
        $stored = $c->fetchAssociative(
            'SELECT n, d::text AS d_t, ts::text AS ts_t, num::text AS num_t, j::text AS j_t, '
            . "u::text AS u_t, octet_length(b) AS blen, encode(b, 'hex') AS bhex "
            . 'FROM s8b_bind WHERE id = ?',
            [1],
        );
        self::assertIsArray($stored);
        self::assertSame(2147483648, $stored['n'], 'a value past int4 range lands in the bigint column');
        self::assertSame('2026-08-05', $stored['d_t']);
        self::assertSame('2026-08-05 13:45:07', $stored['ts_t']);
        self::assertSame('1.2500', $stored['num_t']);
        self::assertSame('{"a": 1}', $stored['j_t'], "jsonb's canonical re-render — it really was parsed as JSON");
        self::assertSame('0b7f0a5e-1f4a-4b7d-8f4e-2a9c1d3e5f60', $stored['u_t']);
        self::assertSame(3, $stored['blen']);
        self::assertSame('0001ff', $stored['bhex'], 'the blob is 3 bytes of binary, not its UTF-8 mangling');

        $c->executeStatement('DROP TABLE s8b_bind');
    }

    /**
     * ADDED beyond the plan — the mapping asserted as a MIRROR against a live PostgreSQL, in the
     * direction the plan's rows do not cover.
     *
     * Task 4 widened PG's canonical-TEXT bind into `numeric`, `date`, `time`, `timestamp`,
     * `timestamptz`, `uuid`, `json` and `jsonb` — deliberately NOT into the integer family. So a
     * numeric string is bindable into an `int` column ONLY because `ParameterType::INTEGER` narrows
     * it to a PHP int (`TAG_I64`) first; the SAME string under `ParameterType::STRING` reaches the
     * engine as `TAG_TEXT` and is refused PRE-SEND. One value, two `ParameterType`s, two outcomes —
     * a property no single-sided assertion can express, and the live proof that the `INTEGER` arm
     * is not decoration.
     *
     * The refusal is also the §19.3 shape this project cares about: it happens before the statement
     * leaves the process, so it is a KNOWN fate (`NonRetryable`), never an `Indeterminate` write.
     */
    public function testTheSameNumericStringIsAnIntUnderIntegerAndIsRefusedUnderString(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_bind_int');
        $c->executeStatement('CREATE TABLE s8b_bind_int (id int primary key)');

        $c->executeStatement('INSERT INTO s8b_bind_int (id) VALUES (?)', ['42'], [ParameterType::INTEGER]);
        self::assertSame(42, $c->fetchOne('SELECT id FROM s8b_bind_int'));

        try {
            $c->executeStatement('INSERT INTO s8b_bind_int (id) VALUES (?)', ['43'], [ParameterType::STRING]);
            self::fail('a canonical TEXT parameter must not bind to a PG integer column');
        } catch (DbalDriverException $e) {
            self::assertStringContainsString('cannot bind to PG type int4', $e->getMessage());
        }

        // The refusal stored nothing and left the connection usable — a pre-send known fate.
        self::assertSame(1, $c->fetchOne('SELECT count(*) FROM s8b_bind_int'));

        $c->executeStatement('DROP TABLE s8b_bind_int');
    }

    /** The binary round trip, on both families — the only route to TAG_BYTES from PHP. */
    public function testBinaryRoundTripsOnBothFamilies(): void
    {
        $blob = "\x00\x01\x02\xfe\xff";

        $pg = $this->dbal();
        $pg->executeStatement('DROP TABLE IF EXISTS s8b_blob');
        $pg->executeStatement('CREATE TABLE s8b_blob (id int primary key, b bytea)');
        $pg->executeStatement('INSERT INTO s8b_blob (id, b) VALUES (?, ?)', [1, $blob], [ParameterType::INTEGER, ParameterType::BINARY]);
        self::assertSame($blob, $pg->fetchOne('SELECT b FROM s8b_blob WHERE id = ?', [1]));
        $pg->executeStatement('DROP TABLE s8b_blob');

        $my = $this->dbal($this->requireMysqlPool());
        $my->executeStatement('DROP TABLE IF EXISTS s8b_blob');
        $my->executeStatement('CREATE TABLE s8b_blob (id INT PRIMARY KEY, b VARBINARY(64))');
        $my->executeStatement('INSERT INTO s8b_blob (id, b) VALUES (?, ?)', [1, $blob], [ParameterType::INTEGER, ParameterType::BINARY]);
        self::assertSame($blob, $my->fetchOne('SELECT b FROM s8b_blob WHERE id = ?', [1]));
        $my->executeStatement('DROP TABLE s8b_blob');
    }
}
