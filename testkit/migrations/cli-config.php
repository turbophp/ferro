<?php // /testkit/migrations/cli-config.php
declare(strict_types=1);

/**
 * `doctrine-migrations`' own configuration seam (`ConsoleRunner::findDependencyFactory()` looks for
 * exactly this filename in the CWD and requires it to return a `DependencyFactory`).
 *
 * Nothing here is Ferro-specific except `driverClass` + `driverOptions` — which IS the claim under
 * test: M1-S8b's promise is CONFIG-ONLY adoption for DBAL, so a real `doctrine/migrations` run must
 * need no patched migrations code, no custom platform and no SQL rewriting (charter rule 6).
 */

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\Migrations\Configuration\Connection\ExistingConnection;
use Doctrine\Migrations\Configuration\Migration\ConfigurationArray;
use Doctrine\Migrations\DependencyFactory;
use Doctrine\Migrations\Provider\SchemaProvider;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Wrapper\FerroConnection;
use Ferro\MigrationsAcceptance\TargetSchemaProvider;

require_once __DIR__ . '/vendor/autoload.php';

$sock = getenv('FERRO_SOCK');
if (!is_string($sock) || $sock === '') {
    fwrite(STDERR, "FERRO_SOCK is not set — testkit/migrations-e2e.sh sets it.\n");
    exit(1);
}

$connection = DriverManager::getConnection([
    'driverClass' => Ferro\DBAL\Driver::class,
    // REQUIRED, not a refinement (SPEC §22.2 (ah)). Without it `Doctrine\DBAL\Connection::
    // transactional()` runs a rollBack() at nesting level 0 after a failed COMMIT and raises
    // `NoActiveTransaction` FROM A `finally`, replacing the exception in flight — so an
    // `IndeterminateWriteException` survives only as getPrevious(), where no catch block keys on
    // it. This template configures `all_or_nothing` + `transactional` migrations, i.e. exactly the
    // shape whose every DDL step runs inside that transactional() call.
    'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class,
    'unix_socket' => $sock,
    'driverOptions' => ['pool' => 'default'],
]);
assert($connection instanceof DbalConnection);

// THE CONTACT ASSERTION, the same one testkit/dbal-suite.sh's bootstrap makes and for the same
// measured reason: a harness that silently connects to something else reports a perfect green.
// S8b reproduced that deliberately — upstream's TestUtil fell back to in-memory SQLite and the
// subset reported `OK (105 tests, 211 assertions)` with ZERO engine contact.
$native = $connection->getNativeConnection();
if (!$native instanceof FerroClientConnection) {
    fwrite(STDERR, "the DBAL connection is not a Ferro one: " . get_debug_type($native) . "\n");
    exit(1);
}
// The WRAPPER assertion, and it is not decoration: `wrapperClass` is a silent parameter — omit it
// and DriverManager hands back a stock Doctrine\DBAL\Connection that works perfectly until the
// first failed COMMIT, at which point transactional() destroys the write's fate (§22.2 (ah)). This
// template is what an operator copies, so it fails LOUDLY rather than shipping the omission onward.
if (!$connection instanceof FerroConnection) {
    fwrite(STDERR, sprintf(
        "the DBAL connection is a %s, not a %s — 'wrapperClass' is REQUIRED (SPEC §22.2 (ah)); "
        . "without it transactional() masks IndeterminateWriteException.\n",
        get_debug_type($connection),
        FerroConnection::class,
    ));
    exit(1);
}
$probe = $connection->fetchOne('SELECT 1');
if ((int) $probe !== 1) {
    fwrite(STDERR, "the round-trip probe did not reach a real backend\n");
    exit(1);
}
fwrite(
    STDOUT,
    sprintf(
        "[ferro] migrations: driver=%s wrapper=%s platform=%s server=%s\n",
        $connection->getDriver()::class,
        $connection::class,
        $connection->getDatabasePlatform()::class,
        (string) $connection->fetchOne('SELECT version()'),
    ),
);

$configuration = new ConfigurationArray([
    'table_storage' => ['table_name' => 'ferro_migration_versions'],
    'migrations_paths' => [
        'Ferro\\MigrationsAcceptance\\Generated' => __DIR__ . '/generated',
    ],
    'all_or_nothing' => true,
    'transactional' => true,
    'check_database_platform' => true,
    'organize_migrations' => 'none',
]);

$factory = DependencyFactory::fromConnection($configuration, new ExistingConnection($connection));
$factory->setService(SchemaProvider::class, new TargetSchemaProvider($connection));

return $factory;
