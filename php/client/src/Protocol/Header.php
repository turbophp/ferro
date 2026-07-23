<?php // /php/client/src/Protocol/Header.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;

final class Header
{
    public function __construct(
        public readonly int $flags, public readonly int $service, public readonly int $method,
        public readonly int $requestId, public readonly int $payloadLen,
    ) {}

    public function encode(): string
    {
        // C C v v v V V  => u8 u8 u16le u16le u16le u32le u32le
        return pack('CCvvvVV', C::MAGIC, C::PROTOCOL_VERSION,
            $this->flags, $this->service, $this->method, $this->requestId, $this->payloadLen);
    }

    public static function decode(string $buf): self
    {
        if (strlen($buf) < 16) { throw new CodecException('short header'); }
        $u = unpack('Cmagic/Cver/vflags/vservice/vmethod/Vreq/Vlen', substr($buf, 0, 16));
        if ($u === false) { throw new CodecException('unpack failed'); }
        if ($u['magic'] !== C::MAGIC) { throw new CodecException(sprintf('bad magic 0x%02x', $u['magic'])); }
        if ($u['ver'] !== C::PROTOCOL_VERSION) { throw new CodecException('bad version ' . $u['ver']); }
        if ($u['len'] > C::MAX_FRAME_PAYLOAD) { throw new CodecException('frame too large ' . $u['len']); }
        return new self($u['flags'], $u['service'], $u['method'], $u['req'], $u['len']);
    }
}
