<?php // testkit/orm/bootstrap.php — M1-S9

declare(strict_types=1);

// ONE autoloader: the ORM CLONE's vendor tree (the inverse of testkit/dbal/bootstrap.php, and for
// the same measured reason — two Composer autoloaders answer for two PHPUnit builds and the
// runner dies before the first test). The clone's vendor carries doctrine/orm's dev deps,
// doctrine/dbal at the asserted pin, AND ferro/client + ferro/doctrine-dbal-driver through the
// path repositories testkit/orm-suite.sh configures.
$src = getenv('FERRO_ORM_SRC');
if ($src === false || $src === '') {
    fwrite(STDERR, "FERRO_ORM_SRC is unset\n");
    exit(1);
}

// Upstream's own bootstrap: clone-vendor autoload + proxy dir setup + error_reporting(E_ALL).
require $src . '/tests/Tests/TestInit.php';

$mode = getenv('FERRO_ORM_MODE') ?: 'ferro';

// -------------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION — the whole reason this file exists. The S8b gate measured a green run
// (`OK (105 tests, 211 assertions)`) against in-memory SQLite with ZERO Ferro contact; ORM's
// TestUtil throws instead of falling back, but a db_driver var smuggled into any config runs stock
// PDO under a Ferro-labeled banner just as silently. Only asking the connection what it IS closes
// the class.
// -------------------------------------------------------------------------------------------------
$conn   = Doctrine\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();

if ($mode === 'ferro') {
    if (! $native instanceof Ferro\Client\Connection) {
        fwrite(STDERR, sprintf(
            "FERRO CONTACT ASSERTION FAILED: the suite's connection is a %s, not a Ferro one.\n"
            . "Refusing to run: a green result here would mean nothing.\n",
            get_debug_type($native),
        ));
        exit(1);
    }

    // The WRAPPER assertion, transitive form: the suite's own QueryLog wrapper
    // (Doctrine\Tests\DbalExtensions\Connection — hardcoded by TestUtil, and OrmFunctionalTestCase
    // asserts that exact class) must EXTEND Ferro's REQUIRED FerroConnection (§22.2 (ah)). This
    // single check also proves the runner's one-line parent patch actually applied — an unpatched
    // clone fails HERE, before the first test, not 3485 tests later in a masked fate.
    if (! is_subclass_of(Doctrine\Tests\DbalExtensions\Connection::class, Ferro\DBAL\Wrapper\FerroConnection::class)) {
        fwrite(STDERR,
            "FERRO WRAPPER ASSERTION FAILED: Doctrine\\Tests\\DbalExtensions\\Connection does not "
            . "extend Ferro\\DBAL\\Wrapper\\FerroConnection — the DbalExtensions parent patch did "
            . "not apply. Without it transactional() masks IndeterminateWriteException (§22.2 (ah)).\n");
        exit(1);
    }
} else {
    // STOCK comparator: assert the DUAL, so a mislabeled run can never publish under the wrong
    // banner (both directions of the same lie).
    if (! $native instanceof PDO || $native instanceof Ferro\Client\Connection) {
        fwrite(STDERR, sprintf(
            "STOCK COMPARATOR ASSERTION FAILED: expected a PDO native connection, got %s.\n",
            get_debug_type($native),
        ));
        exit(1);
    }

    fwrite(STDOUT, "[ferro-orm] MODE: STOCK COMPARATOR — this run measures the PDO baseline, not Ferro\n");
}

// -------------------------------------------------------------------------------------------------
// THE PREFERENCE ASSERTION (M1-S9 Task 2; the plan's own adversarial pass caught this as a
// BLOCKER). When the runner sets FERRO_ORM_PG_SEQUENCE=1 it is asserting a CONFIGURATION — the
// documented D-S8b-5 adoption path — and the number the suite then produces is only meaningful if
// that configuration actually reached the ORM Configuration object.
//
// It cannot be taken on trust. Upstream `configureProxies()` opens with
// `if (PHP_VERSION_ID >= 80400 && $enableNativeLazyObjects) { …; return; }`, and this environment
// is PHP 8.4 with native lazy objects on: a preference written anywhere after that line is DEAD
// CODE. The failure is not loud — it is 1229 errors (~35% of the suite) carrying the exact
// D-S8b-5 error text, i.e. a broken harness that reads as a genuine finding about PostgreSQL
// identity strategy. So the harness proves the effect, on a Configuration built exactly the way
// OrmFunctionalTestCase::getEntityManager() builds it (tests/Tests/OrmFunctionalTestCase.php:909).
// -------------------------------------------------------------------------------------------------
if (getenv('FERRO_ORM_PG_SEQUENCE') === '1') {
    $probeConfig = new Doctrine\ORM\Configuration();
    Doctrine\Tests\TestUtil::configureProxies($probeConfig);

    $prefs  = $probeConfig->getIdentityGenerationPreferences();
    $wanted = Doctrine\ORM\Mapping\ClassMetadata::GENERATOR_TYPE_SEQUENCE;
    $got    = $prefs[Doctrine\DBAL\Platforms\PostgreSQLPlatform::class] ?? null;

    if ($got !== $wanted) {
        fwrite(STDERR, sprintf(
            "FERRO PREFERENCE ASSERTION FAILED: FERRO_ORM_PG_SEQUENCE=1 is set, but after\n"
            . "TestUtil::configureProxies() the identity-generation preference for %s is %s,\n"
            . "expected GENERATOR_TYPE_SEQUENCE (%d). The D-S8b-5 SEQUENCE preference did NOT take\n"
            . "effect — the usual cause is that it sits AFTER configureProxies()'s PHP>=8.4\n"
            . "native-lazy-objects early return, where it is dead code (PHP here: %s).\n"
            . "Refusing to run: the suite would report ~1229 errors in the exact D-S8b-5 wording,\n"
            . "and that reads as a finding about PostgreSQL rather than a broken harness.\n",
            Doctrine\DBAL\Platforms\PostgreSQLPlatform::class,
            var_export($got, true),
            $wanted,
            PHP_VERSION,
        ));
        exit(1);
    }

    fwrite(STDOUT, sprintf(
        "[ferro-orm] identity preference: %s => GENERATOR_TYPE_SEQUENCE (D-S8b-5, verified in effect)\n",
        Doctrine\DBAL\Platforms\PostgreSQLPlatform::class,
    ));
}

// The round trip: a connection object proves wiring; a row proves an engine.
$one = $conn->fetchOne('SELECT 1');
if ((int) $one !== 1) {
    fwrite(STDERR, "SELECT 1 round trip failed (got " . var_export($one, true) . ")\n");
    exit(1);
}

fwrite(STDOUT, sprintf(
    "[ferro-orm] contact: native=%s wrapper=%s platform=%s server=%s\n",
    get_debug_type($native),
    get_debug_type($conn),
    get_debug_type($conn->getDatabasePlatform()),
    $conn->getServerVersion(),
));

$conn->close();
