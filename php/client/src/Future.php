<?php // /php/client/src/Future.php
declare(strict_types=1);
namespace Ferro;

/**
 * The result of an asynchronous call, such as {@see \Ferro\Client\Connection::queryAsync} (SPEC §10.1).
 *
 * The request behind a pending Future has already been WRITTEN to the engine; {@see await} reads
 * its terminal and turns it into a value or an exception. Under plain FPM `await` blocks on the
 * socket. Several Futures created before any is awaited run concurrently in the engine, so awaiting
 * all of them costs about the slowest one, not the sum.
 *
 * A Future settles exactly once. A second `await` returns the same value, or rethrows the same
 * exception, without touching the wire. Every error a synchronous call would throw is thrown by
 * `await` instead, with the same type and the same fate.
 *
 * @template T
 */
final class Future
{
    private bool $settled = false;
    private mixed $value = null;
    private ?\Throwable $error = null;

    /** @var (\Closure(): T)|null */
    private ?\Closure $resolver;

    /** @var (\Closure(): void)|null */
    private ?\Closure $onDrop;

    /**
     * @param \Closure(): T          $resolver reads the terminal and produces the value (or throws).
     * @param (\Closure(): void)|null $onDrop  runs if this Future is destroyed before it settled, so
     *                                         the session can throw its terminal away on arrival
     *                                         instead of keeping it forever (M3-D1a review F6).
     */
    public function __construct(\Closure $resolver, ?\Closure $onDrop = null)
    {
        $this->resolver = $resolver;
        $this->onDrop = $onDrop;
    }

    public function __destruct()
    {
        if (!$this->settled && $this->onDrop !== null) {
            try {
                ($this->onDrop)();
            } catch (\Throwable) {
                // A destructor must not throw; an un-discarded terminal only costs memory.
            }
        }
    }

    /**
     * A Future that ran `$work` now and holds its outcome: the value, or the exception it threw,
     * rethrown at {@see await}. Used where a request cannot be left in flight, such as inside an
     * open transaction, so its error still surfaces at `await` like any other.
     *
     * @template U
     * @param \Closure(): U $work
     * @return self<U>
     */
    public static function settleNow(\Closure $work): self
    {
        $future = new self($work);
        $future->settle();
        return $future;
    }

    /**
     * Wait for the value. Throws whatever the operation threw.
     *
     * @return T
     */
    public function await(): mixed
    {
        $this->settle();
        if ($this->error !== null) {
            throw $this->error;
        }
        /** @var T */
        return $this->value;
    }

    /** Whether the outcome is already known (awaiting will not touch the wire). */
    public function isSettled(): bool
    {
        return $this->settled;
    }

    private function settle(): void
    {
        if ($this->settled) {
            return;
        }
        $resolver = $this->resolver;
        $this->resolver = null;
        $this->onDrop = null;
        try {
            $this->value = $resolver !== null ? $resolver() : null;
        } catch (\Throwable $e) {
            $this->error = $e;
        }
        $this->settled = true;
    }
}
