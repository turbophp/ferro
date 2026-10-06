<?php // /php/client/src/Client/CopyRunner.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Outcome;

/**
 * The client half of the COPY sub-protocol (M3-D4; SPEC §6.1, `/proto/PROTOCOL.md` §13), shared by
 * {@see Connection} and {@see TxHandle} — and through them by {@see \Ferro\Pg\Copy}. Internal: the
 * public surface is `copyIn()`/`copyOut()` on those classes.
 *
 * **Never buffers.** `in()` pulls the caller's iterable one piece at a time, coalesces small pieces
 * up to {@see CHUNK_BYTES} and splits large ones, and sends nothing beyond the credit the ENGINE has
 * granted — when it has none it reads the engine's next grant (or its terminal) before pulling more.
 * `out()` is a Generator that yields each chunk as it arrives and replenishes the engine's credit only
 * after the caller took it.
 *
 * **Fate (§19.3).** A COPY_IN lost before its `COPY_DONE` was completely written cannot have applied
 * — PostgreSQL completes a COPY only on its end-of-data — so it is `Retryable`
 * ({@see FateClassifier::copyStoppedBeforeDone}). Lost after it, it is a lost write: `Indeterminate` in
 * autocommit. A COPY_OUT is classified like a streamed statement, by the caller's `readonly`
 * declaration. The client never re-sends a COPY.
 *
 * **Abandonment.** If the caller stops — its iterable throws, or it stops iterating `out()` — the
 * COPY is CANCELled and drained to its terminal (a COPY_IN then applies nothing), so the session's
 * next request reads its own reply. Not after a wire failure: the session is poisoned then, and a
 * second wire operation would only mask the real error.
 */
final class CopyRunner
{
    /** The most COPY bytes one client `COPY_DATA` frame carries (always also within the grant). */
    public const CHUNK_BYTES = 256 * 1024;

    /** @param \Closure(): bool $epochChanged whether the last reconnect changed the engine epoch */
    public function __construct(
        private readonly FateClassifier $fate,
        private readonly ExecCodec $codec,
        private readonly bool $inTx,
        private readonly \Closure $epochChanged,
    ) {}

    /**
     * Run a COPY_IN: send `$data` (raw COPY bytes, as many strings as the caller likes) and return
     * the number of rows the server copied. Anything but a string in `$data` is refused at runtime
     * (an `\InvalidArgumentException`, and the COPY is abandoned with nothing applied), so the type
     * below is what is CHECKED, not what is trusted.
     *
     * @param iterable<mixed, mixed> $data
     */
    public function in(CopySessionInterface $session, string $payload, iterable $data): int
    {
        try {
            $opened = $session->openCopyIn($payload);
        } catch (ConnectionLostException | TransportException $e) {
            throw $this->fate->copyStoppedBeforeDone('the COPY_IN request was lost: ' . $e->getMessage());
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode the COPY_IN answer: ' . $e->getMessage(), 0, $e);
        }
        if ($opened['type'] === 'end') {
            return $this->affected($opened['outcome']); // the COPY never started: throws its error
        }
        $rid = $opened['requestId'];
        $credit = [$opened['frames'], $opened['bytes']];
        $doneSent = false;
        $reachedTerminal = false;
        $wireFailed = false;
        try {
            try {
                $pending = '';
                foreach ($data as $piece) {
                    if (!is_string($piece)) {
                        throw new \InvalidArgumentException(sprintf(
                            'COPY data must be strings of raw COPY bytes, got %s',
                            get_debug_type($piece),
                        ));
                    }
                    if ($pending === '' && strlen($piece) >= self::CHUNK_BYTES) {
                        $this->send($session, $rid, $piece, $credit, $reachedTerminal);
                        continue;
                    }
                    $pending .= $piece;
                    if (strlen($pending) >= self::CHUNK_BYTES) {
                        $this->send($session, $rid, $pending, $credit, $reachedTerminal);
                        $pending = '';
                    }
                }
                if ($pending !== '') {
                    $this->send($session, $rid, $pending, $credit, $reachedTerminal);
                }
                $session->sendCopyDone($rid);
                $doneSent = true;
                while (true) {
                    $event = $session->readCopyEvent($rid);
                    if ($event['type'] === 'end') {
                        $reachedTerminal = true;
                        return $this->affected($event['outcome']);
                    }
                    if ($event['type'] !== 'grant') {
                        throw new ProtocolException('unexpected COPY_DATA on a COPY_IN');
                    }
                }
            } catch (ConnectionLostException | TransportException $e) {
                $wireFailed = true;
                if (!$doneSent) {
                    throw $this->fate->copyStoppedBeforeDone($e->getMessage());
                }
                throw $this->fate->classifyLoss(
                    $this->inTx ? OpKind::TxStatement : OpKind::Write,
                    false,
                    'COPY_IN lost after its end-of-data was sent — whether it applied is unconfirmed: '
                        . $e->getMessage(),
                    $e instanceof ConnectionLostException ? $e->errorPayload() : null,
                    ($this->epochChanged)(),
                    sent: true,
                );
            } catch (CodecException $e) {
                $wireFailed = true;
                throw new ProtocolException('failed to decode a COPY_IN frame: ' . $e->getMessage(), 0, $e);
            }
        } finally {
            if (!$reachedTerminal && !$wireFailed) {
                $session->abandonCopy($rid);
            }
        }
    }

    /**
     * Send all of `$s` within the engine's credit, reading grants as it runs out. The engine's
     * terminal can arrive instead of a grant — a COPY that failed early — and is thrown here.
     *
     * @param array{0:int,1:int} $credit [frames, bytes], updated in place
     */
    private function send(CopySessionInterface $session, int $rid, string $s, array &$credit, bool &$reachedTerminal): void
    {
        $len = strlen($s);
        $off = 0;
        while ($off < $len) {
            while ($credit[0] <= 0 || $credit[1] <= 0) {
                $event = $session->readCopyEvent($rid);
                if ($event['type'] === 'end') {
                    $reachedTerminal = true;
                    $this->affected($event['outcome']); // an error terminal throws here
                    throw new ProtocolException('the engine ended a COPY_IN successfully before its end-of-data');
                }
                if ($event['type'] !== 'grant') {
                    throw new ProtocolException('unexpected COPY_DATA on a COPY_IN');
                }
                $credit[0] += $event['frames'];
                $credit[1] += $event['bytes'];
            }
            $n = min($len - $off, $credit[1], self::CHUNK_BYTES);
            $session->sendCopyData($rid, $off === 0 && $n === $len ? $s : substr($s, $off, $n));
            $off += $n;
            $credit[0] -= 1;
            $credit[1] -= $n;
        }
    }

    /**
     * Run a COPY_OUT lazily: yield each chunk of raw COPY bytes as it arrives; the Generator's return
     * value is the number of rows exported.
     *
     * @return \Generator<int, string, mixed, int>
     */
    public function out(CopySessionInterface $session, string $payload, bool $readonly): \Generator
    {
        $kind = $this->inTx ? OpKind::TxStatement : ($readonly ? OpKind::Read : OpKind::Write);
        try {
            $rid = $session->openCopyOut($payload);
        } catch (ConnectionLostException | TransportException $e) {
            throw $this->lost($kind, $readonly, 'COPY_OUT request lost: ', $e);
        }
        $reachedTerminal = false;
        $wireFailed = false;
        try {
            while (true) {
                try {
                    $event = $session->readCopyEvent($rid);
                } catch (ConnectionLostException | TransportException $e) {
                    $wireFailed = true;
                    throw $this->lost($kind, $readonly, 'COPY_OUT lost mid-stream: ', $e);
                } catch (CodecException $e) {
                    $wireFailed = true;
                    throw new ProtocolException('failed to decode a COPY_OUT frame: ' . $e->getMessage(), 0, $e);
                }
                if ($event['type'] === 'end') {
                    $reachedTerminal = true;
                    return $this->affected($event['outcome']);
                }
                if ($event['type'] !== 'data') {
                    throw new ProtocolException('unexpected grant on a COPY_OUT');
                }
                yield $event['data'];
                try {
                    $session->sendWindowUpdate($rid, 1, $event['bytes']);
                } catch (TransportException $e) {
                    $wireFailed = true;
                    throw $this->lost($kind, $readonly, 'COPY_OUT lost replenishing its window: ', $e);
                }
            }
        } finally {
            if (!$reachedTerminal && !$wireFailed) {
                $session->abandonCopy($rid);
            }
        }
    }

    private function lost(OpKind $kind, bool $readonly, string $why, ConnectionLostException|TransportException $e): FerroException
    {
        return $this->fate->classifyLoss(
            $kind,
            $readonly,
            $why . $e->getMessage(),
            $e instanceof ConnectionLostException ? $e->errorPayload() : null,
            ($this->epochChanged)(),
            sent: !($e instanceof TransportException && $e->requestUnsent()),
        );
    }

    /** The `affected` of an Ok terminal; an error terminal throws its mapped exception. */
    private function affected(Outcome $outcome): int
    {
        if (!$outcome->isOk()) {
            throw ErrorMapper::fromOutcome($outcome);
        }
        return $this->codec->decode($outcome)['affected'];
    }
}
