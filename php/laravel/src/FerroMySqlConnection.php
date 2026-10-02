<?php // /php/laravel/src/FerroMySqlConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Laravel\Schema\FerroMySqlBuilder;
use Illuminate\Database\MySqlConnection;

/**
 * An Illuminate MySQL connection whose EXECUTION layer talks to `ferrod` (§15, M2-C1f) — driver
 * `ferro-mysql`, adopted from Illuminate's `mysql` driver.
 *
 * Like its siblings it extends the stock family connection, so `MySqlGrammar`, `MySqlProcessor`
 * and the schema grammar are inherited untouched (charter rule 6). The execution layer is the
 * shared {@see FerroConnectionBody}; what the MySQL family needs beyond it is
 * {@see FerroMySqlFamily}, shared with {@see FerroMariaDbConnection}. A MariaDB server reached
 * through THIS driver gets Illuminate's MySQL grammar, exactly as stock `mysql` gives it; an app
 * that used Laravel 11's `mariadb` driver adopts `ferro-mariadb` instead.
 */
class FerroMySqlConnection extends MySqlConnection
{
    use FerroConnectionBody;
    use FerroMySqlFamily;

    protected function newFerroSchemaBuilder(): FerroMySqlBuilder
    {
        return new FerroMySqlBuilder($this);
    }
}
