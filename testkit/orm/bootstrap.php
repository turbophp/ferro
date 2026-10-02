<?php // testkit/orm/bootstrap.php — autoload, then the CONTACT ASSERTION, before any test runs.
//
// The column must reach what it claims: a Ferro column's native connection is a
// `Ferro\Client\Connection` and the control's is a PDO (INVERTED, so a control that quietly routed
// through Ferro — or a Ferro column that fell through to PDO — refuses to run rather than records a
// number). Both must round-trip `SELECT 1` and sit in the database the runner reset. The one line it
// prints is what the CI lane greps.
declare(strict_types=1);

$orm = getenv('FERRO_ORM_SRC') ?: '';
if ($orm === '') {
    fwrite(STDERR, "FERRO_ORM_SRC is unset\n");
    exit(1);
}
require $orm . '/vendor/autoload.php';

$control = getenv('FERRO_ORM_CONTROL') === '1';
$conn = Doctrine\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();
if ($control) {
    if ($native instanceof Ferro\Client\Connection || ! $native instanceof PDO) {
        fwrite(STDERR, 'CONTROL CONTACT ASSERTION FAILED: native connection is ' . get_debug_type($native) . "\n");
        exit(1);
    }
} elseif (! $native instanceof Ferro\Client\Connection) {
    fwrite(STDERR, 'FERRO CONTACT ASSERTION FAILED: native connection is ' . get_debug_type($native) . "\n");
    exit(1);
}
if ((int) $conn->fetchOne('SELECT 1') !== 1) {
    fwrite(STDERR, "CONTACT ASSERTION FAILED: SELECT 1 did not round-trip\n");
    exit(1);
}
$platform = $conn->getDatabasePlatform();
$db = $conn->fetchOne($platform instanceof Doctrine\DBAL\Platforms\PostgreSQLPlatform ? 'SELECT current_database()' : 'SELECT database()');
$server = $conn->fetchOne($platform instanceof Doctrine\DBAL\Platforms\PostgreSQLPlatform ? 'SHOW server_version' : 'SELECT version()');
$expect = getenv('FERRO_ORM_DB') ?: '';
if ($expect !== '' && $db !== $expect) {
    fwrite(STDERR, "CONTACT ASSERTION FAILED: connected to database '$db', expected '$expect'\n");
    exit(1);
}
fwrite(STDOUT, sprintf(
    "[%s] orm=%s dbal=%s native=%s platform=%s server=%s database=%s sequence-preference=%s\n",
    $control ? 'control' : 'ferro',
    Composer\InstalledVersions::getPrettyVersion('doctrine/orm'),
    Composer\InstalledVersions::getPrettyVersion('doctrine/dbal'),
    // The NATIVE connection, not getDriver(): the suite wraps every driver in a logging middleware,
    // so the driver slot names the middleware in both columns and distinguishes nothing.
    get_debug_type($native),
    get_class($platform),
    $server,
    $db,
    getenv('FERRO_ORM_SEQUENCE') === '1' ? 'on' : 'off',
));
$conn->close();
