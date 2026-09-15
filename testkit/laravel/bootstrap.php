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
// The probe uses the SAME driver NAME the suite is about to run under, so it also proves that
// Illuminate's resolver map answers for that name — under the `pgsql` alias, that Ferro really did
// take over the stock name rather than the stock PostgresConnection quietly winning.
$driver = getenv('FERRO_LARAVEL_DRIVER') ?: 'ferro-pgsql';
$family = Illuminate\Tests\Integration\Database\DatabaseTestCase::familyOf($driver);

// -------------------------------------------------------------------------------------------------
// THE PROBE IS PER-FAMILY, because SQLite has no `version()` at all — the same premise that measured
// FALSE for the engine's own version probe at C3-3e. `sqlite_version()` is its equivalent.
//
// **And on SQLite the "is it really the engine?" question cannot be answered by the server product,
// because the trap IS SQLite.** Testbench's injected fallback is an in-memory SQLite, so a probe
// that only proved "something SQLite answered" would pass against exactly the thing it exists to
// catch. `PRAGMA database_list` names the FILE the main database is open on, so it distinguishes the
// intended file from `:memory:` (which reports an empty path) and from any other file. For the Ferro
// column that is end-to-end evidence the ENGINE opened the expected path — which the connection
// itself cannot know, since §12/D8 keeps the path out of PHP.
// -------------------------------------------------------------------------------------------------
$expectFile = null;
if ($family === 'sqlite') {
    // From the RUNNER, not parsed here: the engine's DSN rule is `strip_prefix("sqlite://")` and
    // PHP's `parse_url()` disagrees with it outright — it returns FALSE for `sqlite:///abs/path`.
    $expectFile = getenv('FERRO_LARAVEL_SQLITE_DB') ?: '';
    if ($expectFile === '') {
        fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: FERRO_LARAVEL_SQLITE_DB is unset\n");
        exit(1);
    }
}

/** One probe, used by both the Ferro column and its control. Returns the server version string. */
$probeServer = static function (Illuminate\Database\Connection $conn) use ($family, $expectFile): string {
    $nonce = bin2hex(random_bytes(8));
    $vsql = $family === 'sqlite' ? 'sqlite_version()' : 'version()';
    $probe = $conn->select("select '{$nonce}' as nonce, {$vsql} as v");
    if (count($probe) !== 1 || $probe[0]->nonce !== $nonce) {
        fwrite(STDERR, "CONTACT ASSERTION FAILED: the nonce did not round-trip — whatever answered did not receive it\n");
        exit(1);
    }
    $v = (string) $probe[0]->v;

    if ($family === 'sqlite') {
        $rows = $conn->select('PRAGMA database_list');
        $main = null;
        foreach ($rows as $r) {
            if (($r->name ?? null) === 'main') {
                $main = (string) ($r->file ?? '');
            }
        }
        if ($main === null) {
            fwrite(STDERR, "CONTACT ASSERTION FAILED: PRAGMA database_list named no `main` database\n");
            exit(1);
        }
        if ($main === '') {
            fwrite(STDERR, "CONTACT ASSERTION FAILED: the main database is IN-MEMORY, not a file.\n"
                . "That is the testbench SQLite-fallback trap this assertion exists for.\n");
            exit(1);
        }
        // Both sides must RESOLVE. `realpath()` returns false for a missing path, and
        // `false === false` would make this comparison pass for two files that do not exist —
        // a check that succeeds for the wrong reason is worse than no check.
        $got = realpath($main);
        $want = realpath((string) $expectFile);
        if ($got === false || $want === false || $got !== $want) {
            fwrite(STDERR, sprintf(
                "CONTACT ASSERTION FAILED: open on %s (resolved %s), expected %s (resolved %s)\n",
                $main,
                var_export($got, true),
                (string) $expectFile,
                var_export($want, true),
            ));
            exit(1);
        }
        return 'SQLite ' . $v;
    }

    if (! str_contains($v, 'PostgreSQL')) {
        fwrite(STDERR, sprintf(
            "CONTACT ASSERTION FAILED: something answered, but it was not PostgreSQL (%s).\n"
            . "That is the SQLite-fallback trap this assertion exists for.\n",
            $v,
        ));
        exit(1);
    }
    return $v;
};

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
if (str_starts_with($driver, 'stock-')) {
    $conn = (new Illuminate\Database\Connectors\ConnectionFactory(new Illuminate\Container\Container()))
        ->make(Illuminate\Tests\Integration\Database\DatabaseTestCase::controlConfigForBootstrap(), 'control');

    // Checked against the Ferro BASE, not one family's class, so adding a family cannot silently
    // leave the inversion unenforced for it.
    if ($conn instanceof Ferro\Laravel\FerroPostgresConnection || $conn instanceof Ferro\Laravel\FerroSQLiteConnection) {
        fwrite(STDERR, "CONTROL ASSERTION FAILED: the control column resolved a FERRO connection.\n"
            . "Refusing to run: it would report Ferro's behaviour as upstream's baseline.\n");
        exit(1);
    }
    $v = $probeServer($conn);
    fwrite(STDOUT, sprintf(
        "[ferro] connection=%s driver=%s server=%s\n",
        get_class($conn),
        $driver,
        $v,
    ));
    return;
}
// Same rule as the test case: only the column that RUNS under the stock name registers the alias.
if ($driver === $family) {
    Ferro\Laravel\FerroConnections::register([$family => "ferro-$family"]);
} else {
    Ferro\Laravel\FerroConnections::register();
}
// Required by the FERRO columns only — the SQLite control starts no daemon, so demanding a socket
// before the control branch above would refuse the one configuration that must not have one.
$sock = getenv('FERRO_LARAVEL_SOCK');
if ($sock === false || $sock === '') {
    fwrite(STDERR, "FERRO CONTACT ASSERTION FAILED: FERRO_LARAVEL_SOCK is unset\n");
    exit(1);
}

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

$want = $family === 'sqlite'
    ? Ferro\Laravel\FerroSQLiteConnection::class
    : Ferro\Laravel\FerroPostgresConnection::class;
if (! $conn instanceof $want) {
    fwrite(STDERR, sprintf(
        "FERRO CONTACT ASSERTION FAILED: the connection is a %s, not a %s.\n"
        . "Refusing to run: a green result here would mean nothing.\n",
        get_debug_type($conn),
        $want,
    ));
    exit(1);
}

$v = $probeServer($conn);

fwrite(STDOUT, sprintf("[ferro] connection=%s driver=%s server=%s\n", get_class($conn), $driver, $v));
