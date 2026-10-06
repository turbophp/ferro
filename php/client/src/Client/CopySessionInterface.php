<?php // /php/client/src/Client/CopySessionInterface.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Protocol\Outcome;

/**
 * The COPY wire operations a session offers (M3-D4; `/proto/PROTOCOL.md` §13) — what
 * {@see CopyRunner} drives. Implemented by the concrete {@see Session}.
 *
 * A COPY_IN's flow control is the reverse of a stream's: the ENGINE grants the client credit with
 * `CORE/WINDOW_UPDATE` frames on the request's id, the first of which says the COPY has started,
 * and the client sends STREAM/`COPY_DATA` chunks only within it, then STREAM/`COPY_DONE`. A COPY_OUT
 * is a stream of STREAM/`COPY_DATA` chunks the client replenishes with its own `WINDOW_UPDATE`s.
 */
interface CopySessionInterface extends StreamingSessionInterface
{
    /** @return array{type:'grant', requestId:int, frames:int, bytes:int}|array{type:'end', requestId:int, outcome:Outcome} */
    public function openCopyIn(string $payload): array;

    public function openCopyOut(string $payload): int;

    /** @return array{type:'grant', frames:int, bytes:int}|array{type:'data', data:string, bytes:int}|array{type:'end', outcome:Outcome} */
    public function readCopyEvent(int $requestId): array;

    public function sendCopyData(int $requestId, string $data): void;

    public function sendCopyDone(int $requestId): void;

    public function abandonCopy(int $requestId): void;
}
