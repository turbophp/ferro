<?php // testkit/dbal/bootstrap.php

declare(strict_types=1);

// ONE autoloader: the driver package's, which carries `Ferro\DBAL\`, `ferro/client` (through its
// composer path repository), `doctrine/dbal` at exactly the pinned tag, `doctrine/deprecations`
// (whose PHPUnit\VerifyDeprecations trait several allowlisted tests use) and PHPUnit itself.
//
// The pinned CLONE contributes its `tests/` tree and nothing else, registered here as the
// `autoload-dev` PSR-4 root a CONSUMER install never sets up. Requiring the clone's OWN
// vendor/autoload.php as well is what the first attempt did, and it fails before the first test:
// two Composer autoloaders answer for two different PHPUnit builds (11.5.56 vs 11.5.50) and the
// runner dies on `Call to undefined method PHPUnit\TextUI\Configuration\Source::identifyIssueTrigger()`.
// testkit/dbal-suite.sh asserts the two doctrine/dbal versions match, so "tests from the clone,
// source from vendor" cannot silently drift.
//
// WHICH vendor tree is the runner's choice (M2-C5b): the driver package's `vendor/` for DBAL 4, its
// `vendor-dbal3/` lane for DBAL 3. Defaulting keeps a bare `phpunit -c` against the DBAL 4 tree
// working as before.
$vendor = getenv('FERRO_DBAL_VENDOR');
if ($vendor === false || $vendor === '') {
    $vendor = __DIR__ . '/../../php/doctrine-dbal/vendor';
}

$dbal = getenv('FERRO_DBAL_SRC');
if ($dbal === false || $dbal === '') {
    fwrite(STDERR, "FERRO_DBAL_SRC is unset\n");
    exit(1);
}

/** @var Composer\Autoload\ClassLoader $loader */
$loader = require $vendor . '/autoload.php';
$loader->addPsr4('Doctrine\\DBAL\\Tests\\', $dbal . '/tests');

// -------------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION. It runs BEFORE the first test, and it is the whole reason this file exists.
// The upstream TestUtil silently falls back to in-memory SQLite when it cannot find a driver, and the
// functional suite then passes — genuinely, with nothing skipped — against the wrong engine.
// `--fail-on-skipped` cannot catch that; only asking the connection what it IS can.
// -------------------------------------------------------------------------------------------------
$control = getenv('FERRO_DBAL_CONTROL') === '1';

$conn   = Doctrine\DBAL\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();

if ($control) {
    // THE CONTROL INVERTS IT. This run is SUPPOSED to reach upstream's own driver, so "is this a
    // Ferro connection" is the failure, not the pass. Without the inversion a mis-set variable
    // would quietly produce a second Ferro column labelled "control", and every attribution drawn
    // from the pair would be wrong in the same direction — which is worse than having no control,
    // because it reads as corroboration.
    if ($native instanceof Ferro\Client\Connection) {
        fwrite(STDERR,
            "CONTROL CONTACT ASSERTION FAILED: the control's connection IS a Ferro one.\n"
            . "Refusing to run: this column exists to measure the suite WITHOUT Ferro.\n");
        exit(1);
    }
    if (! $native instanceof PDO) {
        fwrite(STDERR, sprintf(
            "CONTROL CONTACT ASSERTION FAILED: expected a PDO, got %s.\n",
            get_debug_type($native),
        ));
        exit(1);
    }
} elseif (! $native instanceof Ferro\Client\Connection) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: the suite's connection is a %s, not a Ferro one.\n"
        . "Refusing to run: a green result here would mean nothing.\n",
        get_debug_type($native),
    ));
    exit(1);
}

// DBAL 4's wrapper exposes `getServerVersion()`; DBAL 3's keeps it PRIVATE, and there the version
// is the driver connection's — the Ferro one found through the native client, or the control's PDO.
if (is_callable([$conn, 'getServerVersion'])) {
    $version = $conn->getServerVersion();
} elseif ($native instanceof Ferro\Client\Connection) {
    $version = Ferro\DBAL\AbstractConnection::forNativeConnection($native)?->getServerVersion() ?? '?';
} else {
    $version = $native instanceof PDO ? (string) $native->getAttribute(PDO::ATTR_SERVER_VERSION) : '?';
}
$platform = get_class($conn->getDatabasePlatform());

// A real round trip, so "connected" cannot mean "constructed an object".
if ((int) $conn->fetchOne('SELECT 1') !== 1) {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: SELECT 1 did not return 1\n");
    exit(1);
}

fwrite(STDOUT, sprintf(
    ($control ? "[control] " : "[ferro] ") . "driver=%s platform=%s server=%s vendor_gates=%s\n",
    get_class($conn->getDriver()),
    $platform,
    $version,
    // Which PDO driver name TestUtil::isDriverOneOf() answers for this column (E9).
    (string) ($GLOBALS['db_driver'] ?? $GLOBALS['db_vendor_driver'] ?? 'none'),
));

$conn->close();
