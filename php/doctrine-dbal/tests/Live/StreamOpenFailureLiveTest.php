<?php // /php/doctrine-dbal/tests/Live/StreamOpenFailureLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Result as DbalResult;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\Error\FerroException;

/**
 * **The stream-OPEN error terminal** — the one vantage point every other error-mapping guard in this
 * package misses, and the reason the whole-branch review filed it as a BLOCKER
 * (`review/wb-guards.md`).
 *
 * On PostgreSQL a parameterless `executeQuery()` is the ONE statement shape this driver streams
 * ({@see \Ferro\DBAL\Connection::query}), so it never reaches `runPrepared()`/`exec()` and never
 * touches the buffered error path those guards drive. When the failure happens BEFORE the first
 * `HEAD` frame — a missing table, a parse error, a constant-folded `1/0`, all of which PostgreSQL
 * rejects at PREPARE — the engine's terminal arrives as the answer to the stream OPEN itself, and
 * exactly one line in `Ferro\Client\Connection::streamRaw()` turns it into an exception:
 *
 * ```php
 * if ($opened['type'] === 'end') {
 *     $this->throwIfError($opened['outcome']);      // <- this one
 *     return new RawStream([], …);                  // <- what a caller gets without it
 * }
 * ```
 *
 * Delete it and every failed parameterless PG `executeQuery()` becomes a **silently empty result
 * set**: no exception, zero rows, a healthy connection, and an application that believes the table
 * was empty. Measured by the reviewer at HEAD (`NO THROW, rows=[]` for all three statements below)
 * with the entire slice green over the deletion.
 *
 * ## What makes this test able to fail
 *
 * The assertion is made at the ONLY vantage point that can see the defect: the exception must come
 * out of `executeQuery()` ITSELF, before anything consumes the result. A test that iterated the
 * result would pass either way — a mid-stream terminal (which `StreamingLiveTest` already covers)
 * arrives from `Result::advance()`, a different line entirely. So each case here fails LOUDLY when
 * a `Doctrine\DBAL\Result` is returned, and reports the rows it would have handed the application.
 *
 * Both layers are pinned from here, because the line under test lives in the client:
 *  - the DBAL surface (`executeQuery` → converted stock DBAL exception classes), and
 *  - the raw client surface (`streamRaw()` → a `FerroException`), reached through
 *    `getNativeConnection()` so no test in `php/client` has to exist for this to be guarded.
 */
final class StreamOpenFailureLiveTest extends DbalLiveTestCase
{
    /**
     * The three failure shapes PostgreSQL decides at PREPARE, i.e. before any row could exist:
     * a missing relation, a parse error, and a division by zero PG constant-folds during planning.
     * Each must arrive as its stock DBAL class from `executeQuery()` itself.
     *
     * @return array<string, array{string, class-string<\Throwable>, ?string}>
     */
    public static function preHeadFailures(): array
    {
        return [
            'missing table' => ['SELECT * FROM zz_stream_open_no_such_table', TableNotFoundException::class, '42P01'],
            'parse error' => ['SELECT FROM WHERE', SyntaxErrorException::class, '42601'],
            'constant-folded division by zero' => ['SELECT 1/0', DbalDriverException::class, '22012'],
        ];
    }

    /**
     * @param class-string<\Throwable> $expected
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('preHeadFailures')]
    public function testAStatementThatFailsBeforeTheFirstHeadThrowsFromExecuteQueryItself(
        string $sql,
        string $expected,
        ?string $sqlstate,
    ): void {
        // The `default` pool is the PostgreSQL one throughout this suite, and PostgreSQL is the
        // family whose parameterless reads stream — the only route that reaches the open arm.
        $c = $this->dbal();

        $returned = null;
        $caught = null;
        try {
            // NOT consumed, on purpose: the throw under test happens during the OPEN, so any
            // fetch here would move the vantage point to `Result::advance()` and the guard would
            // pass for the wrong reason.
            $returned = $c->executeQuery($sql);
        } catch (\Throwable $e) {
            $caught = $e;
        }

        if ($caught === null) {
            self::fail(sprintf(
                'executeQuery(%s) returned %s instead of throwing — the stream-OPEN error terminal '
                . 'was swallowed and the application sees rows=%s',
                $sql,
                $returned instanceof DbalResult ? DbalResult::class : get_debug_type($returned),
                $returned instanceof DbalResult ? json_encode($returned->fetchAllNumeric()) : 'n/a',
            ));
        }

        self::assertInstanceOf($expected, $caught, sprintf('[%s] wrong converted class', $sql));
        if ($sqlstate !== null) {
            self::assertInstanceOf(DbalDriverException::class, $caught);
            self::assertSame($sqlstate, $caught->getSQLState(), sprintf('[%s] wrong SQLSTATE', $sql));
        }

        // The session survives it: the failure was the statement's, not the connection's, and the
        // next parameterless query — the same streaming path — still works.
        self::assertSame(1, (int) $c->fetchOne('SELECT 1'));
    }

    /**
     * The same terminal one layer down, at the method that owns the line: `streamRaw()` must THROW
     * rather than hand back an empty `RawStream`. Driven through `getNativeConnection()` so this
     * package's suite guards the client contract it depends on.
     */
    public function testStreamRawOnTheRawClientThrowsInsteadOfReturningAnEmptyStream(): void
    {
        $c = $this->dbal();
        $native = $c->getNativeConnection();
        self::assertInstanceOf(FerroClientConnection::class, $native);

        $stream = null;
        $caught = null;
        try {
            $stream = $native->streamRaw('SELECT * FROM zz_stream_open_no_such_table', [], true);
        } catch (FerroException $e) {
            $caught = $e;
        }

        if ($caught === null) {
            $rows = [];
            if ($stream !== null) {
                foreach ($stream->rows() as $row) {
                    $rows[] = $row;
                }
            }
            self::fail(sprintf(
                'streamRaw() on a statement that fails before the first HEAD returned a RawStream '
                . 'with cols=%s and rows=%s instead of throwing',
                json_encode($stream?->columns() ?? []),
                json_encode($rows),
            ));
        }

        self::assertStringContainsString(
            'zz_stream_open_no_such_table',
            $caught->getMessage(),
            'the engine error must name the relation — otherwise it is not the statement failure',
        );
        self::assertSame(1, (int) $c->fetchOne('SELECT 1'));
    }
}
