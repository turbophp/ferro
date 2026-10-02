<?php // /php/laravel/src/Schema/FerroMySqlBuilder.php
declare(strict_types=1);
namespace Ferro\Laravel\Schema;

use Illuminate\Database\Schema\MySqlBuilder;

/** The stock MySQL schema builder, with foreign-key toggling pinned to one connection — see {@see PinsForeignKeyChecks}. */
final class FerroMySqlBuilder extends MySqlBuilder
{
    use PinsForeignKeyChecks;
}
