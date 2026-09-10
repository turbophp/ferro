<?php // /php/client/src/Protocol/Codec.php
declare(strict_types=1);
namespace Ferro\Protocol;

use Ferro\Protocol\Generated\Constants as C;

final class Codec
{
    /**
     * Build one wire frame: the 16-byte header followed by its payload.
     *
     * **The size guard mirrors the Rust codec's, and its absence here was a real defect.**
     * `ferrod`'s `Encoder<OutFrame>` (`session/codec.rs`) refuses to build a frame whose payload
     * exceeds `MAX_FRAME_PAYLOAD`; this side did not, so the client would happily put on the wire a
     * frame **its own {@see Header::decode} would reject**. That is not a symmetric-looking wart:
     * the engine classifies an oversize `payload_len` as FATAL, not as a per-request error — it must,
     * since the framing is desynchronised and a payload you refused to read cannot be skipped — so
     * an oversize bind (a `Ferro\Bytes` over 16 MiB, reached through `ParameterType::LARGE_OBJECT`)
     * **killed the session** instead of failing locally. Proven engine-side by `ferrod`'s
     * `session_rules::oversize_payload_len_is_fatal`, whose own comment notes the Rust encoder
     * "would refuse to build such a frame" — the refusal this side was missing.
     *
     * Checked BEFORE a single byte reaches the transport, which is the property that matters: the
     * caller gets a local exception naming the limit and the actual size, and the session is
     * untouched and still usable.
     *
     * This bounds a frame, NOT the total size of a bind. Carrying one payload across several frames
     * (chunked `LARGE_OBJECT`) is a `/proto` design change and is deliberately not this.
     *
     * @throws CodecException if `$payload` exceeds `MAX_FRAME_PAYLOAD`
     */
    public function encodeFrame(Header $header, string $payload): string
    {
        $len = strlen($payload);
        if ($len > C::MAX_FRAME_PAYLOAD) {
            throw new CodecException(sprintf(
                'frame payload of %d bytes exceeds MAX_FRAME_PAYLOAD (%d); the engine would treat '
                . 'an oversize frame as a FATAL protocol fault and close the session, so it is '
                . 'refused here instead',
                $len,
                C::MAX_FRAME_PAYLOAD,
            ));
        }
        return $header->encode() . $payload;
    }

    /** @return array{0:Header,1:string} */
    public function decodeFrame(string $frame): array
    {
        $h = Header::decode($frame);
        $payload = substr($frame, 16, $h->payloadLen);
        if (strlen($payload) !== $h->payloadLen) { throw new CodecException('truncated payload'); }
        return [$h, $payload];
    }
}
