<?php // testkit/orm/prepare-composer.php <orm-clone> <repo-root> <dbal-version>
//
// Rewrites the ORM clone's composer.json so its OWN vendor tree carries the code under test. The
// ORM cannot reuse php/doctrine-dbal's vendor the way testkit/dbal-suite.sh does: it needs
// doctrine/persistence, symfony/console, var-exporter and cache, which the driver does not.
//  - doctrine/dbal is PINNED to the version the driver's own lock tests against, so the ORM suite
//    and the driver's live lane measure the same DBAL;
//  - ferro/client + ferro/doctrine-dbal-driver come from THIS checkout through symlinked path
//    repositories, and testkit/orm-suite.sh then verifies the symlinks resolve here — a vendor
//    tree pointing at another checkout would measure that checkout's code;
//  - the dev tools the suite does not run (coding standard, phpbench, phpstan) are dropped.
declare(strict_types=1);

[$_, $orm, $root, $dbal] = $argv + [null, null, null, null];
if ($orm === null || $root === null || $dbal === null) {
    fwrite(STDERR, "usage: prepare-composer.php <orm-clone> <repo-root> <dbal-version>\n");
    exit(2);
}
$file = $orm . '/composer.json';
$json = json_decode((string) file_get_contents($file), true, 512, JSON_THROW_ON_ERROR);

$json['require']['doctrine/dbal'] = $dbal;
$json['require-dev'] = [
    'ferro/client' => '@dev',
    'ferro/doctrine-dbal-driver' => '@dev',
    'phpunit/phpunit' => '^11.5',
    'psr/log' => '^1 || ^2 || ^3',
    // The range the measured tree resolved (7.x); upstream's own also admits 8.x.
    'symfony/cache' => '^7.0',
];
$json['repositories'] = [
    ['type' => 'path', 'url' => $root . '/php/client', 'options' => ['symlink' => true]],
    ['type' => 'path', 'url' => $root . '/php/doctrine-dbal', 'options' => ['symlink' => true]],
];
$json['config']['allow-plugins'] = false;

file_put_contents($file, json_encode($json, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n");
