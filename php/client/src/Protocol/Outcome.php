<?php // /php/client/src/Protocol/Outcome.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * The terminal outcome envelope `[status: u8, body]` (decision W-4, PROTOCOL.md §6) that every
 * in-flight request ends in (exactly one `END` frame). Mirrors the Rust `messages::Outcome` BYTES:
 *
 *   - `Ok`   (status {@see Constants::OUTCOME_OK} = 0): `body` is the method-specific success payload,
 *     OPAQUE to this envelope — kept as RAW MessagePack bytes exactly like the Rust `Outcome::Ok(Vec<u8>)`
 *     (the caller decodes it via {@see ExecOk::mapFromWire} / {@see BeginResponse::mapFromWire}). Raw
 *     bytes, not a decoded array, so re-encoding is byte-exact (a generic re-pack cannot recover the
 *     canonical int-width / str-vs-bin choices).
 *   - `Error` (status {@see Constants::OUTCOME_ERROR} = 1): `body` is the 7-field {@see ErrorPayload};
 *     the S7 `ErrorMapper` (Task 3) classifies on `$this->error->branch` (the wire byte).
 *   - `Cancelled` (status {@see Constants::OUTCOME_CANCELLED} = 2): `body` is `nil`.
 *
 * Decodes from / encodes to the terminal-frame PAYLOAD bytes (the caller strips the 16-byte header).
 */
final class Outcome
{
    private function __construct(
        public readonly int $status,
        /** RAW opaque MessagePack body bytes; non-null only for `Ok`. */
        public readonly ?string $okBody,
        /** Non-null only for `Error`. */
        public readonly ?ErrorPayload $error,
    ) {}

    public static function ok(string $body): self { return new self(C::OUTCOME_OK, $body, null); }
    public static function error(ErrorPayload $e): self { return new self(C::OUTCOME_ERROR, null, $e); }
    public static function cancelled(): self { return new self(C::OUTCOME_CANCELLED, null, null); }

    public function isOk(): bool { return $this->status === C::OUTCOME_OK; }
    public function isError(): bool { return $this->status === C::OUTCOME_ERROR; }
    public function isCancelled(): bool { return $this->status === C::OUTCOME_CANCELLED; }

    /** The RAW opaque `Ok` body bytes; the caller decodes them into the method-specific result. */
    public function body(): string
    {
        if ($this->okBody === null) { throw new CodecException('Outcome::body() on a non-Ok outcome'); }
        return $this->okBody;
    }

    /** The decoded `Error` payload; the S7 taxonomy classifies on `->branch`. */
    public function errorPayload(): ErrorPayload
    {
        if ($this->error === null) { throw new CodecException('Outcome::errorPayload() on a non-Error outcome'); }
        return $this->error;
    }

    /**
     * Decode a terminal payload `[status, body]`. The status is the positional-fixint (`write_pfix`)
     * head of a 2-element array; the raw remainder is the opaque body (exactly the Rust `rd.to_vec()`).
     */
    public static function decode(string $payload, PackerInterface $p): self
    {
        if ($payload === '') { throw new CodecException('Outcome: empty payload'); }
        // The envelope is always a 2-element array; canonically encoded as fixarray(2) = 0x92.
        if (ord($payload[0]) !== 0x92) {
            throw new CodecException(sprintf('Outcome: expected fixarray(2), got marker 0x%02x', ord($payload[0])));
        }
        $off = 1;
        $status = $p->unpack($payload, $off); // the status scalar (write_pfix ⇒ positive fixint)
        if (!is_int($status)) { throw new CodecException('Outcome: status is not an int'); }
        $body = substr($payload, $off); // raw opaque body bytes

        return match ($status) {
            C::OUTCOME_OK => self::ok($body),
            C::OUTCOME_ERROR => self::error(ErrorPayload::decode($body, $p)),
            C::OUTCOME_CANCELLED => self::decodeCancelled($body, $p),
            default => throw new CodecException("Outcome: unknown status {$status}"),
        };
    }

    private static function decodeCancelled(string $body, PackerInterface $p): self
    {
        $off = 0;
        $v = $p->unpack($body, $off);
        if ($v !== null || $off !== strlen($body)) {
            throw new CodecException('Outcome::Cancelled body expected exactly nil');
        }
        return self::cancelled();
    }

    /** Encode back to the terminal payload bytes (mirrors the Rust `Outcome::encode`). */
    public function encode(PackerInterface $p): string
    {
        $head = $p->packArrayLen(2);
        return match ($this->status) {
            C::OUTCOME_OK => $head . $p->packUint(C::OUTCOME_OK) . $this->body(),
            C::OUTCOME_ERROR => $head . $p->packUint(C::OUTCOME_ERROR) . $this->errorPayload()->encode($p),
            C::OUTCOME_CANCELLED => $head . $p->packUint(C::OUTCOME_CANCELLED) . $p->packNil(),
            default => throw new CodecException("Outcome: cannot encode status {$this->status}"),
        };
    }
}
