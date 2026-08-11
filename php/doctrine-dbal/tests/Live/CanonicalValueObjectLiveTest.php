<?php // /php/doctrine-dbal/tests/Live/CanonicalValueObjectLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Time;

/**
 * The cross-slice MAJOR, measured where it did its damage: through a REAL engine, on both families.
 *
 * The unit guard (`Unit\CanonicalValueObjectBindTest`) pins the TAG that leaves the process. This
 * pins what that tag BUYS — the two halves the review measured:
 *
 *  * **MySQL/MariaDB: a pre-send REFUSAL instead of a silent coercion.** The S7 guarantee is
 *    tag-keyed (`ferro-backend-mysql`'s `value_to_my`: `Value::Decimal(s) => reject(...)`,
 *    `Value::Time(s) => parse_time(s).ok_or(reject)`), while `Value::Text` has no pre-flight at all.
 *    With the value objects stringified, `Ferro\Decimal('NaN')` stored `0.00` and
 *    `Ferro\Time('839:00:00')` stored `838:59:59` under a permissive `sql_mode`. The assertion is on
 *    the engine's own words — *"refused before sending"* — and not merely on "an exception", because
 *    under the testkit's STRICT `sql_mode` the pre-fix path ALSO threw: a server-side `1366`, after
 *    the value was on the wire. Those two are the whole difference and a bare `expectException`
 *    cannot tell them apart.
 *  * **PostgreSQL: the engine's own advice becomes followable.** The S8b sentinel gate refuses a bare
 *    `'infinity'` string into a `date` slot and tells the caller to *"send it with its own canonical
 *    tag instead (Ferro\Date / …), which binds it deliberately"*. Before the fix that advice was
 *    impossible to follow THROUGH THIS TIER, because passing exactly that object produced the bare
 *    string it refuses. Here the sentinel lands and reads back.
 */
final class CanonicalValueObjectLiveTest extends DbalLiveTestCase
{
    /**
     * PostgreSQL: `Ferro\Date('infinity')` binds the sentinel deliberately and round-trips.
     *
     * The mirror in the same test — the bare STRING `'infinity'` still refused, with the message that
     * names the value objects — is what keeps this from reading as "the gate was loosened".
     */
    public function testTheDateSentinelBindsDeliberatelyOnPostgres(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_canon_pg');
        $c->executeStatement('CREATE TABLE s8b_canon_pg (id int primary key, d date)');

        $c->executeStatement('INSERT INTO s8b_canon_pg (id, d) VALUES (?, ?)', [1, new Date('infinity')]);
        // `d::text`, not `d`: reading a DATE SENTINEL back as a typed value is refused by this
        // driver's own `DbalValuePolicy` (Task 9 — Doctrine's DateType would convert 'infinity' to a
        // DIFFERENT calendar date without complaining). That refusal is on the READ side and is
        // deliberate; the cast is the escape hatch its own message names. What is being measured here
        // is the WRITE, and PostgreSQL's own rendering is the strongest possible witness of it.
        self::assertSame(
            'infinity',
            $c->fetchOne('SELECT d::text FROM s8b_canon_pg WHERE id = 1'),
            'the DATE sentinel round-trips with its tag intact',
        );

        try {
            $c->executeStatement('INSERT INTO s8b_canon_pg (id, d) VALUES (?, ?)', [2, 'infinity']);
            self::fail('a BARE string sentinel must still be refused — the gate is not loosened');
        } catch (DbalDriverException $e) {
            self::assertStringContainsString(
                'Ferro\Date',
                $e->getMessage(),
                'and the refusal still names the value object this test just proved works',
            );
        }
        self::assertSame(
            1,
            (int) $c->fetchOne('SELECT count(*) FROM s8b_canon_pg'),
            'the refused bare string wrote nothing',
        );
        $c->executeStatement('DROP TABLE s8b_canon_pg');
    }

    /**
     * MySQL/MariaDB: the two values the review measured being silently corrupted are REFUSED, by the
     * engine, BEFORE anything is sent — and nothing is written.
     */
    public function testTheNonRepresentableValuesAreRefusedBeforeSendingOnMysql(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_canon_my');
        $c->executeStatement('CREATE TABLE s8b_canon_my (id int primary key, amount decimal(10,2), t time)');

        foreach ([
            'DECIMAL' => [new Decimal('NaN'), 'INSERT INTO s8b_canon_my (id, amount) VALUES (1, ?)'],
            'TIME' => [new Time('839:00:00'), 'INSERT INTO s8b_canon_my (id, t) VALUES (2, ?)'],
        ] as $kind => [$value, $sql]) {
            try {
                $c->executeStatement($sql, [$value]);
                self::fail("$kind: a value with no MySQL representation must not be sent");
            } catch (DbalDriverException $e) {
                self::assertStringContainsString(
                    'refused before sending',
                    $e->getMessage(),
                    "$kind: this must be the engine's PRE-SEND, tag-keyed refusal — a server-side "
                    . 'error here means the value was stringified and travelled as TAG_TEXT',
                );
                self::assertStringContainsString("no MySQL/MariaDB $kind representation", $e->getMessage());
            }
        }

        self::assertSame(
            0,
            (int) $c->fetchOne('SELECT count(*) FROM s8b_canon_my'),
            'nothing was written — no coerced 0.00, no clamped 838:59:59',
        );
        $c->executeStatement('DROP TABLE s8b_canon_my');
    }
}
