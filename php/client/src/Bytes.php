<?php // /php/client/src/Bytes.php
declare(strict_types=1);
namespace Ferro;

/**
 * An explicit BINARY bind marker (SPEC §9, §22.2 (k)(4)).
 *
 * Every bare PHP string binds `TAG_TEXT`, and that is deliberate: a string's CONTENTS are never
 * inspected to pick a tag (the same rule that stops `'infinity'` in a `varchar` column from being
 * retagged as a temporal). So a byte string needs a marker, exactly as `Ferro\Decimal` and
 * `Ferro\Uuid` do — otherwise `TAG_BYTES` is unreachable from PHP and Doctrine's
 * `ParameterType::BINARY` / `ParameterType::LARGE_OBJECT` cannot bind at all.
 *
 * Without it a non-UTF-8 string does not merely mis-bind, it fails at the CODEC: `TAG_TEXT` rides
 * the msgpack `str` family and the engine's reader ends in `String::from_utf8`, so the request is
 * rejected as a malformed payload (`invalid utf8`) — a generic protocol fault rather than a
 * diagnosable bind error.
 *
 * READS are asymmetric and that is intentional: a `bytea`/`VARBINARY`/`BLOB` column hydrates to a
 * plain PHP string (a binary-safe type), so a round trip is `Bytes` out, `string` back.
 */
final class Bytes
{
    public function __construct(public readonly string $value)
    {
    }

    /**
     * Materialise a stream into a `Bytes`. Doctrine's `BlobType::convertToPHPValue` hands the driver
     * a PHP **resource** for `LARGE_OBJECT`; the client deliberately has no implicit `is_resource`
     * bind arm — deciding to read a stream into memory is the CALLER's, made explicitly here.
     *
     * @param mixed $stream an open, readable stream resource
     */
    public static function fromStream(mixed $stream): self
    {
        if (!\is_resource($stream)) {
            throw new \InvalidArgumentException(
                'Ferro\Bytes::fromStream expects an open stream resource, got ' . \get_debug_type($stream),
            );
        }
        $data = \stream_get_contents($stream);
        if ($data === false) {
            throw new \RuntimeException('Ferro\Bytes::fromStream could not read the stream');
        }
        return new self($data);
    }
}
