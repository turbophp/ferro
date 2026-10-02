<?php // /php/laravel/src/FerroServiceProvider.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Illuminate\Support\ServiceProvider;

/**
 * **What makes §15's "change `driver` and nothing else" literally true for a Laravel application.**
 *
 * `composer.json` lists this class under `extra.laravel.providers`, so Laravel's package discovery
 * (`Illuminate\Foundation\PackageManifest`, run by `composer dump-autoload`'s
 * `package:discover`) registers it without the application writing a line of PHP. Before it
 * existed, every application had to call {@see FerroConnections::register()} from a provider of its
 * own — a code change, which the §15 demo app's review measured: a config entry naming
 * `ferro-pgsql` with nothing registered fails `Unsupported driver [ferro-pgsql]` (§22.2 (ch)).
 *
 * It registers the four `ferro-*` drivers and NOTHING under a stock name: the alias that registers
 * Ferro as `pgsql`/`mysql`/… replaces every connection in the application that uses that name, so
 * it stays an explicit {@see FerroConnections::register()} call. An application that disables
 * discovery (`extra.laravel.dont-discover`) calls `register()` itself, as before.
 *
 * `illuminate/support` (where `ServiceProvider` lives) is already required by `illuminate/database`,
 * so this adds no dependency. Registration happens in `register()`, not `boot()`, because a service
 * provider's `boot()` may run after another provider has already resolved a database connection.
 */
final class FerroServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        FerroConnections::register();
    }
}
