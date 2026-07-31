<?php // /php/client/src/Client/StreamingSessionInterface.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Protocol\Outcome;

/**
 * The streamed-read half of the session surface (M1-S5 Task 6): a `fetch:FETCH_STREAM` EXEC is
 * MANY frames per request (one `STREAM/HEAD`, N `STREAM/DATA`, one terminal `END`) rather than
 * {@see SessionInterface::sendRequest}'s one-frame-in/one-frame-out shape, so it gets its own
 * primitives. Implemented by the concrete {@see Session} (over a real/fake {@see TransportInterface});
 * {@see Connection::stream} checks `instanceof` and refuses cleanly on a session that lacks it
 * (e.g. a scripted {@see \Ferro\Tests\Support\FakeSession} used only for the buffered-path tests).
 *
 * Sequencing contract: {@see openStream} then repeated {@see readStreamFrame} until it returns the
 * `end` shape (which clears the session's "stream open" guard); {@see sendWindowUpdate} replenishes
 * the credit window as each `data` batch is consumed. On abandonment (the caller stops pulling
 * before `end`), {@see abandonStream} sends the outbound `CANCEL` and drains to the terminal so the
 * socket is never left mid-stream for the next {@see SessionInterface::sendRequest} to misread.
 */
interface StreamingSessionInterface
{
    /**
     * Write the streamed request frame and synchronously read its first reply: either the
     * `STREAM/HEAD` (column metadata — the common case) or an immediate terminal `END` (a known
     * fate decided before any frame went out, e.g. a checkout failure — no `HEAD`/`DATA` ever
     * sent). Sets the "stream open" guard in the `head` case; a `sendRequest` (or another
     * `openStream`) while it is set throws {@see \Ferro\Client\Error\ProtocolException}.
     *
     * @return array{type:'head', requestId:int, cols:list<array{name:string,tag:int}>}
     *       | array{type:'end', requestId:int, outcome:Outcome}
     */
    public function openStream(int $service, int $method, string $payload): array;

    /**
     * Read the next frame of the stream opened by {@see openStream}: either one `STREAM/DATA`
     * batch (raw wire cells, NOT yet value-policy-decoded — the caller hydrates) or the terminal
     * `Outcome`. Reading the terminal clears the "stream open" guard. Sends no `WINDOW_UPDATE`
     * itself — the caller replenishes once it has actually consumed the batch.
     *
     * @return array{type:'data', rows:list<list<array{tag:int,data:mixed}>>, bytes:int}
     *       | array{type:'end', outcome:Outcome}
     */
    public function readStreamFrame(int $requestId): array;

    /** Send a `CORE/WINDOW_UPDATE {request_id, frames, bytes}` replenishing the credit window. */
    public function sendWindowUpdate(int $requestId, int $frames, int $bytes): void;

    /** Send an outbound `CANCEL` targeting `$requestId` (empty payload; service/method are ignored). */
    public function sendCancel(int $requestId): void;

    /**
     * Abandonment safety: if a stream is still open for `$requestId`, send a `CANCEL` then drain
     * every remaining frame until (and including) the terminal `END`, discarding them — leaving the
     * socket cleanly framed for the next request. A no-op if the stream already reached its
     * terminal (or was never this request) — safe to call unconditionally from a `finally`.
     */
    public function abandonStream(int $requestId): void;
}
