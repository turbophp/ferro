<?php // /php/client/src/Manifest.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Error\ManifestException;

/**
 * The checked-SQL manifest, as the client holds it (SPEC §11, M3-D2e): the `manifest.json` that
 * `ferro manifest` writes and `ferrod` loads from `FERRO_MANIFEST`.
 *
 * The client needs it for two things. It sends the manifest's HASH at HELLO, which the engine
 * compares with its own — a client built against another manifest is refused at connect, and only a
 * session that agreed may run a query by id. And it reads each query's declarations: `pool` and
 * `readonly` travel with every request (the engine checks both), while `idempotent` — the licence to
 * re-send an `Indeterminate` write (§9.2) — is acted on HERE, so it must be the engine's value, which
 * the hash agreement is what proves.
 *
 * **The hash is recomputed, never trusted.** It is SHA-256 over the same canonical bytes the Rust
 * side digests (`ferro_manifest::Manifest::canonical_bytes`): `{"v":1,"q":{<id>:{"sql":…,"pool":…,
 * "readonly":…,"idempotent":…},…}}`, ids in byte order, no whitespace, strings escaped as serde_json
 * escapes them (raw UTF-8, `/` unescaped, U+2028/9 unescaped, control characters as `\u00xx`). A
 * file whose recorded `hash` differs from the recomputed one was edited after `ferro manifest` wrote
 * it — perhaps an `idempotent` flipped — and is refused rather than believed.
 *
 * **Known difference from the engine's loader:** PHP's JSON decoder keeps the LAST of two equal
 * keys silently, where the engine refuses such a file. It cannot make the client act on a different
 * declaration than the engine's: the hash is computed from what PHP kept, and the engine admits a
 * client only if that hash equals its own, which no file with a repeated key can produce on the
 * engine side.
 */
final class Manifest
{
    private const JSON_FLAGS = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
        | JSON_UNESCAPED_LINE_TERMINATORS | JSON_THROW_ON_ERROR;

    private const QUERY_FIELDS = ['sql', 'pool', 'readonly', 'idempotent', 'dto', 'source'];

    /** @param array<string, ManifestQuery> $queries */
    private function __construct(
        private readonly array $queries,
        private readonly string $hash,
    ) {}

    public static function fromFile(string $path): self
    {
        $json = @file_get_contents($path);
        if ($json === false) {
            throw new ManifestException("cannot read the manifest {$path}");
        }
        return self::fromJson($json);
    }

    public static function fromJson(string $json): self
    {
        try {
            $doc = json_decode($json, true, 16, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new ManifestException('the manifest is not JSON: ' . $e->getMessage(), 0, $e);
        }
        if (!is_array($doc) || array_is_list($doc) && $doc !== []) {
            throw new ManifestException('the manifest is not a JSON object');
        }
        $unknown = array_diff(array_keys($doc), ['version', 'hash', 'queries']);
        if ($unknown !== []) {
            throw new ManifestException('unknown manifest field `' . implode('`, `', $unknown) . '`');
        }
        if (($doc['version'] ?? null) !== 1) {
            throw new ManifestException('unsupported manifest version (this client reads version 1)');
        }
        $raw = $doc['queries'] ?? null;
        if (!is_array($raw) || $raw === [] || array_is_list($raw)) {
            throw new ManifestException('the manifest declares no queries');
        }
        $queries = [];
        foreach ($raw as $id => $q) {
            $id = (string) $id;
            // `\z`, not `$`: `$` also matches before a final newline, so `"abc\n"` passed (review F1).
            if (preg_match('/\A[a-z][a-z0-9_.-]{0,127}\z/', $id) !== 1) {
                throw new ManifestException("invalid query id `{$id}`");
            }
            $queries[$id] = self::parseQuery($id, $q);
        }
        ksort($queries, SORT_STRING);
        $hash = hash('sha256', self::canonical($queries));
        // `"hash": null` is accepted, as the engine accepts it: the field is informational.
        if (($doc['hash'] ?? null) !== null && $doc['hash'] !== $hash) {
            throw new ManifestException(
                'the manifest\'s recorded hash is not the hash of its queries: it was edited after '
                . '`ferro manifest` wrote it. Regenerate it rather than editing it.',
            );
        }
        return new self($queries, $hash);
    }

    private static function parseQuery(string $id, mixed $q): ManifestQuery
    {
        if (!is_array($q) || array_is_list($q) && $q !== []) {
            throw new ManifestException("query `{$id}` is not an object");
        }
        $unknown = array_diff(array_keys($q), self::QUERY_FIELDS);
        if ($unknown !== []) {
            throw new ManifestException("query `{$id}` has unknown field `" . implode('`, `', $unknown) . '`');
        }
        $sql = $q['sql'] ?? null;
        $pool = $q['pool'] ?? null;
        $readonly = $q['readonly'] ?? null;
        $idempotent = $q['idempotent'] ?? null;
        $dto = $q['dto'] ?? null;
        $source = $q['source'] ?? null;
        if (!is_string($sql) || $sql === '' || !is_string($pool) || $pool === ''
            || !is_bool($readonly) || !is_bool($idempotent) || !($dto === null || is_string($dto))
            || !($source === null || is_string($source))
        ) {
            throw new ManifestException(
                "query `{$id}` needs a non-empty `sql` and `pool`, boolean `readonly` and `idempotent`",
            );
        }
        // The engine's own rules (`ferro_manifest::Manifest::validate`), so the client never accepts
        // a file the engine would refuse (M3-D2e review F1).
        if (preg_match('/\A[A-Za-z0-9_-]+\z/', $pool) !== 1) {
            throw new ManifestException("query `{$id}` has an invalid pool name");
        }
        if (preg_match('/\A[\s\p{Z}]|[\s\p{Z}]\z/u', $sql) === 1) {
            throw new ManifestException("query `{$id}`'s SQL has surrounding whitespace (it must be stored trimmed)");
        }
        return new ManifestQuery($id, $sql, $pool, $readonly, $idempotent, $dto);
    }

    /**
     * The exact bytes the hash covers (public for the cross-language known-answer tests).
     *
     * @param array<string, ManifestQuery> $queries already sorted by id
     */
    public static function canonical(array $queries): string
    {
        $parts = [];
        foreach ($queries as $id => $q) {
            $parts[] = json_encode((string) $id, self::JSON_FLAGS) . ':{"sql":' . json_encode($q->sql, self::JSON_FLAGS)
                . ',"pool":' . json_encode($q->pool, self::JSON_FLAGS)
                . ',"readonly":' . ($q->readonly ? 'true' : 'false')
                . ',"idempotent":' . ($q->idempotent ? 'true' : 'false') . '}';
        }
        return '{"v":1,"q":{' . implode(',', $parts) . '}}';
    }

    /** The manifest hash, as `ferro manifest` prints it and the engine compares it at HELLO. */
    public function hash(): string
    {
        return $this->hash;
    }

    /** The declared query `$id`, or a {@see ManifestException} naming it. */
    public function query(string $id): ManifestQuery
    {
        return $this->queries[$id] ?? throw new ManifestException("query id `{$id}` is not in the manifest");
    }

    public function has(string $id): bool
    {
        return isset($this->queries[$id]);
    }
}
