<?php // /php/client/src/functions.php
declare(strict_types=1);
namespace Ferro;

if (\function_exists('Ferro\\await')) {
    return;
}

/**
 * Await every Future and return their values, keyed as given (SPEC §10.1):
 *
 *     [$profile, $orders] = Ferro\await([$db->queryOneAsync(…), $db->queryAsync(…)]);
 *
 * EVERY Future is awaited even when an earlier one fails, so no request is left unread on the
 * socket; the FIRST failure, in the order given, is then thrown.
 *
 * @template T
 * @param array<array-key, Future<T>> $futures
 * @return array<array-key, T>
 */
function await(array $futures): array
{
    $values = [];
    $first = null;
    foreach ($futures as $key => $future) {
        try {
            $values[$key] = $future->await();
        } catch (\Throwable $e) {
            $first ??= $e;
        }
    }
    if ($first !== null) {
        throw $first;
    }
    return $values;
}
