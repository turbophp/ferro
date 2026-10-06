<?php // /php/client/tests/Support/OpaqueDriver.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

/** A driver the adapter cannot see into, and nothing else (M3-D1d review F4). */
final class OpaqueDriver extends DelegatingDriver
{
}
