<?php // /php/laravel/src/Schema/FerroMariaDbBuilder.php
declare(strict_types=1);
namespace Ferro\Laravel\Schema;

use Illuminate\Database\Schema\MariaDbBuilder;

/** The stock MariaDB schema builder, with foreign-key toggling pinned to one connection — see {@see PinsForeignKeyChecks}. */
final class FerroMariaDbBuilder extends MariaDbBuilder
{
    use PinsForeignKeyChecks;
}
