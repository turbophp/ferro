<?php // testkit/laravel/bootstrap.php
declare(strict_types=1);

// ONE autoloader: the harness's own vendor tree, which carries laravel/framework at the pinned
// version, orchestra/testbench-core, PHPUnit, and `ferro/laravel` + `ferro/client` through path
// repositories. The pinned CLONE contributes its `tests/` tree and nothing else.
//
// Requiring the clone's own vendor/autoload.php as well is what the SIBLING suite tried first, and
// it fails before the first test: two Composer autoloaders answer for two different PHPUnit builds
// and the runner dies inside its own error handler. One vendor tree removes that class of failure,
// and the runner asserts the framework version matches the test tag so "tests from the clone,
// source from vendor" cannot silently drift.
/** @var Composer\Autoload\ClassLoader $loader */
$loader = require __DIR__ . '/vendor/autoload.php';

$src = getenv('FERRO_LARAVEL_SRC');
if ($src === false || $src === '') {
    fwrite(STDERR, "FERRO_LARAVEL_SRC is unset\n");
    exit(1);
}
$loader->addPsr4('Illuminate\\Tests\\', $src . '/tests');

// -------------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION. It runs BEFORE the first test, and it is the whole reason this file exists.
//
// Testbench silently injects an in-memory SQLite connection when one named `testing` is not
// configured, and upstream's phpunit.xml.dist points DB_CONNECTION straight at it — which is how the
// sibling Doctrine suite once reported OK (105 tests, 211 assertions) against SQLite with nothing
// skipped. The patched base class avoids that trap by construction (it never uses the name
// `testing`), but "by construction" is an argument, and this is evidence.
//
// THE PROBE IS UNGUESSABLE, per FB-7. A `SELECT 1` gate was MEASURED fakeable in this very project:
// with the connection stubbed to return a fixed row of the right shape, a constant probe PASSED. A
// per-call random nonce cannot be guessed, so its return is evidence that something RECEIVED it;
// version() rides along because a nonce alone would be satisfied by any SQL database.
// -------------------------------------------------------------------------------------------------
$sock = getenv('FERRO_LARAVEL_SOCK');
if ($sock === false || $sock === '') {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: FERRO_LARAVEL_SOCK is unset\n");
    exit(1);
}

// The probe uses the SAME driver NAME the suite is about to run under, so it also proves that
// Illuminate's resolver map answers for that name — under the `pgsql` alias, that Ferro really did
// take over the stock name rather than the stock PostgresConnection quietly winning.
$driver = getenv('FERRO_LARAVEL_DRIVER') ?: 'ferro-pgsql';

// -------------------------------------------------------------------------------------------------
// THE CONTROL COLUMN INVERTS THIS ASSERTION, and that inversion is the whole safety of having one.
// Under `stock-pgsql` the run is SUPPOSED to reach upstream's own pdo_pgsql, so "is this a Ferro
// connection?" flips from the thing we require to the thing we refuse: a control that quietly ran
// through Ferro would report Ferro's behaviour as upstream's baseline, which is worse than having no
// baseline. The nonce + version() round trip stays identical — a control still has to prove it
// reached a real PostgreSQL, for exactly the reason the Ferro column does.
//
// It is checked STRUCTURALLY (the RESOLVED connection's class) rather than by trusting the env var,
// because the env var is the input being verified. MUTATION-PROVEN twice, and the two outcomes are
// worth distinguishing: registering Ferro's resolver for `pgsql` alone kills the run one step
// EARLIER (Ferro's own config refusal — the control array carries no `ferro_socket`), and adding
// that socket so a real Ferro connection IS built is what makes the check below fire. Both refuse
// to run; neither reports a number.
// -------------------------------------------------------------------------------------------------
if ($driver === 'stock-pgsql') {
    $conn = (new Illuminate\Database\Connectors\ConnectionFactory(new Illuminate\Container\Container()))
        ->make(Illuminate\Tests\Integration\Database\DatabaseTestCase::controlConfigForBootstrap(), 'control');

    if ($conn instanceof Ferro\Laravel\FerroPostgresConnection) {
        fwrite(STDERR, "CONTROL ASSERTION FAILED: the control column resolved a FERRO connection.\n"
            . "Refusing to run: it would report Ferro's behaviour as upstream's baseline.\n");
        exit(1);
    }
    $nonce = bin2hex(random_bytes(8));
    $probe = $conn->select("select '{$nonce}' as nonce, version() as v");
    if (count($probe) !== 1 || $probe[0]->nonce !== $nonce) {
        fwrite(STDERR, "CONTROL ASSERTION FAILED: the nonce did not round-trip\n");
        exit(1);
    }
    if (! str_contains((string) $probe[0]->v, 'PostgreSQL')) {
        fwrite(STDERR, sprintf("CONTROL ASSERTION FAILED: not PostgreSQL (%s)\n", $probe[0]->v));
        exit(1);
    }
    fwrite(STDOUT, sprintf(
        "[ferro] connection=%s driver=%s server=%s\n",
        get_class($conn),
        $driver,
        $probe[0]->v,
    ));
    return;
}
Ferro\Laravel\FerroConnections::register(['pgsql' => 'ferro-pgsql']);
$resolver = Illuminate\Database\Connection::getResolver($driver);
if ($resolver === null) {
    fwrite(STDERR, sprintf("FERRO CONTACT ASSERTION FAILED: the %s resolver is not registered\n", $driver));
    exit(1);
}

$conn = $resolver(null, 'laravel_tests', '', [
    'driver' => $driver,
    'ferro_socket' => $sock,
    'pool' => getenv('FERRO_LARAVEL_POOL') ?: 'default',
]);

if (! $conn instanceof Ferro\Laravel\FerroPostgresConnection) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: the connection is a %s, not a Ferro one.\n"
        . "Refusing to run: a green result here would mean nothing.\n",
        get_debug_type($conn),
    ));
    exit(1);
}

$nonce = bin2hex(random_bytes(8));
$probe = $conn->select("select '{$nonce}' as nonce, version() as v");
if (count($probe) !== 1 || $probe[0]->nonce !== $nonce) {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: the nonce did not round-trip — whatever answered did not receive it\n");
    exit(1);
}
if (! str_contains((string) $probe[0]->v, 'PostgreSQL')) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: something answered, but it was not PostgreSQL (%s).\n"
        . "That is the SQLite-fallback trap this assertion exists for.\n",
        $probe[0]->v,
    ));
    exit(1);
}

fwrite(STDOUT, sprintf("[ferro] connection=%s driver=%s server=%s\n", get_class($conn), $driver, $probe[0]->v));
