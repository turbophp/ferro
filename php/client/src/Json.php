<?php // /php/client/src/Json.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Error\ProtocolException;

/**
 * SPEC §9 `JSON` → the raw UTF-8 document text, decoded LAZILY on first access.
 *
 * **Laziness is the contract, not an optimization.** The engine neither re-serializes nor validates
 * the document (`/proto/PROTOCOL.md` §3.2), and a 200-row result set of JSON columns must not pay
 * `json_decode` for documents the caller never opens. It also keeps the failure where it belongs: a
 * document PHP cannot parse fails on {@see decoded()}, not on the row read — so one unparseable
 * cell never costs the caller the rest of the result set.
 *
 * Not a `readonly class` (and not just because the package targets PHP >= 8.2): the decode cache is
 * mutable by design, so only {@see raw} is `readonly`.
 */
final class Json implements \Stringable
{
    private bool $isDecoded = false;
    private mixed $cache = null;

    public function __construct(public readonly string $raw) {}

    /** The raw document text — what the bind path re-emits, byte-for-byte. */
    public function __toString(): string
    {
        return $this->raw;
    }

    /**
     * The decoded document (objects as associative arrays), decoded ONCE and cached.
     *
     * The cache is keyed on a FLAG, not on `$cache !== null`, because `null` is both a valid JSON
     * document and `json_decode`'s failure return — a null-keyed cache would re-decode `"null"`
     * forever and, worse, could not tell the two apart.
     *
     * @throws ProtocolException when the document is not valid JSON (a wire fault: the backend
     *   handed over a `JSON` column's text and it did not parse).
     */
    public function decoded(): mixed
    {
        if ($this->isDecoded) {
            return $this->cache;
        }
        try {
            $this->cache = json_decode($this->raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            // The message names the parse failure but never the document — a cell's contents are
            // user data and must not land in an exception message (SPEC §12).
            throw new ProtocolException('JSON payload is not a valid document: ' . $e->getMessage(), 0, $e);
        }
        $this->isDecoded = true;
        return $this->cache;
    }
}
