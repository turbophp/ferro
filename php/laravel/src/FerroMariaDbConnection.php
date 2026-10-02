<?php // /php/laravel/src/FerroMariaDbConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Laravel\Schema\FerroMariaDbBuilder;
use Illuminate\Database\MariaDbConnection;

/**
 * An Illuminate MariaDB connection whose EXECUTION layer talks to `ferrod` — driver
 * `ferro-mariadb`, adopted from Laravel 11's `mariadb` driver (M2-C1f, review F4).
 *
 * It exists because `mariadb` is not `mysql` in Laravel 11: `MariaDbConnection` brings its own
 * `MariaDbGrammar` (query and schema), `MariaDbProcessor` and `MariaDbBuilder` — measured through
 * stock `pdo_mysql` on MariaDB, `threadCount()` answers and JSON casts compile under `mariadb`
 * where the `mysql` grammar answers null and emits `cast(… as json)`, a MariaDB syntax error. An
 * app on the `mariadb` driver that adopted `ferro-mysql` would silently change dialect; this keeps
 * the one-word change one word. Same execution layer, same {@see FerroMySqlFamily}.
 */
class FerroMariaDbConnection extends MariaDbConnection
{
    use FerroConnectionBody;
    use FerroMySqlFamily;

    protected function newFerroSchemaBuilder(): FerroMariaDbBuilder
    {
        return new FerroMariaDbBuilder($this);
    }
}
