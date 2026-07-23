<?php // /php/client/src/Protocol/Codec.php
declare(strict_types=1);
namespace Ferro\Protocol;

final class Codec
{
    public function encodeFrame(Header $header, string $payload): string
    {
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
