<?php // /php/client/src/Protocol/ErrorPayload.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * The normalized engine/backend error, carried as the `body` of an `Outcome::Error` (never as its
 * own top-level frame). Mirrors the Rust `messages::ErrorPayload` BYTES: a positional fixarray of 7
 * fields in declaration order (PROTOCOL.md §5) —
 *   [code:u16, branch:u8, sqlstate:str|nil, errno:i32|nil, message:str, detail:str|nil, retry_after_ms:u32|nil].
 *
 * This is a decode-mostly VALUE OBJECT: the client receives it (server → client), and the S7
 * `ErrorMapper` (Task 3) classifies the three-branch taxonomy on the WIRE `branch` byte alone
 * ({@see $branch} = `ErrorPayload[1]`), never on `code`'s range — so an unknown `code` still
 * classifies correctly (decision W-3). `errno` is the one SIGNED field (i32); the rest are unsigned.
 */
final class ErrorPayload
{
    public function __construct(
        public readonly int $code,
        public readonly int $branch,
        public readonly ?string $sqlstate,
        public readonly ?int $errno,
        public readonly string $message,
        public readonly ?string $detail,
        public readonly ?int $retryAfterMs,
    ) {}

    /** @return string the encoded fixarray(7) ErrorPayload body (no Outcome wrapper) */
    public function encode(PackerInterface $p): string
    {
        return $p->packArrayLen(7)
            . $p->packUint($this->code)
            . $p->packUint($this->branch)
            . ($this->sqlstate === null ? $p->packNil() : $p->packStr($this->sqlstate))
            . ($this->errno === null ? $p->packNil() : $p->packInt($this->errno))
            . $p->packStr($this->message)
            . ($this->detail === null ? $p->packNil() : $p->packStr($this->detail))
            . ($this->retryAfterMs === null ? $p->packNil() : $p->packUint($this->retryAfterMs));
    }

    /** Decode a standalone 7-element ErrorPayload body from its raw MessagePack bytes. */
    public static function decode(string $bytes, PackerInterface $p): self
    {
        $off = 0;
        $w = $p->unpack($bytes, $off);
        if ($off !== strlen($bytes)) { throw new CodecException('ErrorPayload: trailing bytes'); }
        if (!is_array($w)) { throw new CodecException('ErrorPayload: body is not an array'); }
        return self::mapFromWire($w);
    }

    /**
     * Build from an already-unpacked 7-element wire array.
     * @param array<array-key,mixed> $w
     */
    public static function mapFromWire(array $w): self
    {
        $w = array_values($w);
        if (count($w) !== 7) { throw new CodecException('ErrorPayload arity != 7'); }
        return new self(
            SqlValueCodec::toInt($w[0]),
            SqlValueCodec::toInt($w[1]),
            SqlValueCodec::nullableStr($w[2]),
            SqlValueCodec::nullableInt($w[3]),
            SqlValueCodec::toStr($w[4]),
            SqlValueCodec::nullableStr($w[5]),
            SqlValueCodec::nullableInt($w[6]),
        );
    }

    /**
     * Build from the golden-vector "error" JSON shape (positional field names).
     * @param array<string,mixed> $m
     */
    public static function fromArray(array $m): self
    {
        return new self(
            SqlValueCodec::toInt($m['code'] ?? 0),
            SqlValueCodec::toInt($m['branch'] ?? 0),
            SqlValueCodec::nullableStr($m['sqlstate'] ?? null),
            SqlValueCodec::nullableInt($m['errno'] ?? null),
            SqlValueCodec::toStr($m['message'] ?? ''),
            SqlValueCodec::nullableStr($m['detail'] ?? null),
            SqlValueCodec::nullableInt($m['retry_after_ms'] ?? null),
        );
    }

    /**
     * Render back to the golden-vector "error" JSON shape (for round-trip assertions / diagnostics).
     * @return array{code:int,branch:int,sqlstate:?string,errno:?int,message:string,detail:?string,retry_after_ms:?int}
     */
    public function toArray(): array
    {
        return [
            'code' => $this->code,
            'branch' => $this->branch,
            'sqlstate' => $this->sqlstate,
            'errno' => $this->errno,
            'message' => $this->message,
            'detail' => $this->detail,
            'retry_after_ms' => $this->retryAfterMs,
        ];
    }
}
