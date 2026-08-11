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
require __DIR__ . '/../../php/doctrine-dbal/vendor/autoload.php';

$dbal = getenv('FERRO_DBAL_SRC');
if ($dbal === false || $dbal === '') {
    fwrite(STDERR, "FERRO_DBAL_SRC is unset\n");
    exit(1);
}

/** @var Composer\Autoload\ClassLoader $loader */
$loader = require __DIR__ . '/../../php/doctrine-dbal/vendor/autoload.php';
$loader->addPsr4('Doctrine\\DBAL\\Tests\\', $dbal . '/tests');

// -------------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION. It runs BEFORE the first test, and it is the whole reason this file exists.
// The upstream TestUtil silently falls back to in-memory SQLite when it cannot find a driver, and the
// functional suite then passes — genuinely, with nothing skipped — against the wrong engine.
// `--fail-on-skipped` cannot catch that; only asking the connection what it IS can.
// -------------------------------------------------------------------------------------------------
$conn   = Doctrine\DBAL\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();

if (! $native instanceof Ferro\Client\Connection) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: the suite's connection is a %s, not a Ferro one.\n"
        . "Refusing to run: a green result here would mean nothing.\n",
        get_debug_type($native),
    ));
    exit(1);
}

$version  = $conn->getServerVersion();
$platform = get_class($conn->getDatabasePlatform());

// A real round trip, so "connected" cannot mean "constructed an object".
if ((int) $conn->fetchOne('SELECT 1') !== 1) {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: SELECT 1 did not return 1\n");
    exit(1);
}

fwrite(STDOUT, sprintf(
    "[ferro] driver=%s platform=%s server=%s\n",
    get_class($conn->getDriver()),
    $platform,
    $version,
));

$conn->close();
