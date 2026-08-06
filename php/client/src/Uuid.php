<?php // /php/client/src/Uuid.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `UUID` → the 36-char canonical LOWERCASE hyphenated text (`/proto/PROTOCOL.md` §3.2).
 *
 * The wire never carries raw bytes for a UUID (a msgpack `bin` payload is indistinguishable from a
 * `str` after PHP unpacks it, so the text form is the only one both codecs can agree on), and the
 * form is pinned to lowercase-hyphenated so the same value is one payload, not five. An uppercase,
 * unhyphenated or braced rendering is malformed and throws rather than being normalized — silently
 * rewriting a payload is how a byte-stable round trip stops being byte-stable.
 *
 * Not a `readonly class`: the package targets PHP >= 8.2, where `readonly` is a property modifier.
 */
final class Uuid implements \Stringable
{
    public readonly string $value;

    /** @throws \Ferro\Client\Error\ProtocolException when `$value` is not a canonical UUID. */
    public function __construct(string $value)
    {
        $this->value = CanonicalText::uuid($value);
    }

    public function __toString(): string
    {
        return $this->value;
    }
}
