<?php // /php/laravel/tests/Live/RollbackSignal.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * Thrown from inside a `transaction()` closure to make Illuminate roll it back, and caught by the
 * test — and by NOTHING else.
 *
 * Not `\RuntimeException`, on purpose and measured: PHPUnit's `AssertionFailedError` extends
 * `PHPUnit\Framework\Exception extends \RuntimeException`, so a test that throws and catches
 * `\RuntimeException` around `transaction()` silently swallows every assertion that FAILS inside
 * the closure. C1e-2's mutation round found exactly that — a `cursor()` assertion failed under a
 * mutation and the test still passed — so a test that asserts inside a transaction closure throws
 * this instead.
 */
final class RollbackSignal extends \Exception
{
}
