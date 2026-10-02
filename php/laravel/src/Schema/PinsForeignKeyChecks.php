<?php // /php/laravel/src/Schema/PinsForeignKeyChecks.php
declare(strict_types=1);
namespace Ferro\Laravel\Schema;

use Closure;

/**
 * Foreign-key toggling on a TRANSACTION-MODE pool (M2-C1f, review F14), for the MySQL family's two
 * schema builders.
 *
 * `SET FOREIGN_KEY_CHECKS=0` is SESSION state, and on a transaction-mode pool every statement
 * outside a transaction is its own checkout: the `SET` taints its connection, hygiene
 * (`COM_RESET_CONNECTION`) restores the default, and the next statement — the `DROP`, the
 * `TRUNCATE` — runs with checks ON (SPEC §7.4). Measured through `ferro-mysql`: on MariaDB stock
 * `dropAllTables()` failed `1451 Cannot delete or update a parent row` on an ordinary parent/child
 * schema (it drops in name order, so any parent named before its child), which is `migrate:fresh`,
 * `db:wipe` and `RefreshDatabase`; `pdo_mysql` drops the same schema cleanly. MySQL 8.4 accepts that
 * one multi-table `DROP` with checks on (measured in CI), but refuses the shapes stock users write
 * inside the toggles — truncating a referenced parent, dropping it alone (`3730`) — on both.
 *
 * **A Ferro transaction pins ONE connection to a `tx_id`, and that pin survives MySQL's implicit
 * commit** — measured: after `DROP TABLE` inside a transaction, `@@foreign_key_checks` still reads 0
 * on the same `connection_id()`, the COMMIT succeeds, and the next statement gets a clean
 * connection. So the toggling methods run INSIDE a transaction — the existing one if the caller is
 * already in one, a new one otherwise — and a bare `disableForeignKeyConstraints()` outside any
 * transaction, which can have no effect at all, REFUSES rather than silently doing nothing.
 *
 * The cost, stated: inside `withoutForeignKeyConstraints($callback)` a thrown exception now ROLLS
 * BACK whatever the callback did that a DDL statement had not already implicitly committed, where
 * stock autocommits statement by statement.
 */
trait PinsForeignKeyChecks
{
    public function dropAllTables()
    {
        $this->onOnePinnedConnection(fn () => parent::dropAllTables());
    }

    public function withoutForeignKeyConstraints(Closure $callback)
    {
        return $this->onOnePinnedConnection(fn () => parent::withoutForeignKeyConstraints($callback));
    }

    public function disableForeignKeyConstraints()
    {
        if ($this->connection->transactionLevel() === 0) {
            throw new \LogicException(
                'Ferro: Schema::disableForeignKeyConstraints() outside a transaction would have NO '
                . 'effect on a pooled connection — FOREIGN_KEY_CHECKS is session state, the next '
                . 'statement runs on a freshly reset connection with checks ON (SPEC §7.4). Use '
                . 'Schema::withoutForeignKeyConstraints(fn () => …), or call it inside '
                . 'DB::transaction(), which pins one connection for every statement in it.',
            );
        }
        return parent::disableForeignKeyConstraints();
    }

    /**
     * @template T
     * @param Closure(): T $fn
     * @return T
     */
    private function onOnePinnedConnection(Closure $fn): mixed
    {
        return $this->connection->transactionLevel() > 0 ? $fn() : $this->connection->transaction($fn);
    }
}
