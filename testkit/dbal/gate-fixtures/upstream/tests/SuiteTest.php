<?php
// A stand-in for a pinned upstream clone, for ci/test-suite-gate.sh: the lines triage-good.txt cites.
namespace Fx;

final class SuiteTest
{
    #[RequiresDatabase('stock')]
    public function testNameGated(): void
    {
    }

    public function testControlSkips(): void
    {
        if ($driver instanceof StockDriver) {
            self::markTestSkipped('the stock driver does not report this');
        }
    }
}

#[RequiresDatabase('stock')]
final class GatedTest
{
    public function testOne(): void
    {
    }

    public function testTwo(): void
    {
    }
}
