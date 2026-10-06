<?php // /php/client/src/Tooling/QueryExtractor.php
declare(strict_types=1);
namespace Ferro\Tooling;

/**
 * Reads `#[FerroQuery(...)]` declarations out of PHP source with PHP's own tokenizer, without running
 * the code (SPEC §11, M3-D2a). `bin/ferro-queries` prints what this returns as the JSON list the
 * `ferro manifest --php-queries` command reads.
 *
 * Only what can be read EXACTLY is accepted, because a query the manifest gets wrong runs the wrong
 * SQL under a trusted id:
 *  - arguments must be NAMED (`id: …`), and only the attribute's own parameters;
 *  - a string must be one literal: a single-quoted string, a double-quoted string or heredoc with no
 *    backslash and no interpolation, or a nowdoc. A concatenation, constant or expression is refused;
 *  - `readonly`/`idempotent` must be the literal `true` or `false`.
 *
 * The attribute is recognised by its short name `FerroQuery` or the fully-qualified
 * `\Ferro\Attribute\FerroQuery`. An alias (`use … as Q`) is not resolved and is therefore not seen —
 * a known limit, stated here rather than guessed around.
 *
 * Kept free of `ferro/client`'s other classes so the CLI script can `require` it alone.
 */
final class QueryExtractor
{
    private const ALLOWED = ['id', 'sql', 'pool', 'readonly', 'idempotent', 'dto'];

    /**
     * @param list<string> $paths files or directories (directories are searched for `*.php`)
     * @return array{0: list<array<string, mixed>>, 1: list<string>} the queries, and every problem
     */
    public static function extractPaths(array $paths): array
    {
        $files = [];
        foreach ($paths as $path) {
            if (is_dir($path)) {
                $it = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($path, \FilesystemIterator::SKIP_DOTS));
                foreach ($it as $file) {
                    if ($file instanceof \SplFileInfo && $file->isFile() && $file->getExtension() === 'php') {
                        $files[] = $file->getPathname();
                    }
                }
            } elseif (is_file($path)) {
                $files[] = $path;
            } else {
                return [[], ["{$path}: no such file or directory"]];
            }
        }
        sort($files);
        $queries = [];
        $problems = [];
        foreach ($files as $file) {
            $code = file_get_contents($file);
            if ($code === false) {
                $problems[] = "{$file}: cannot read";
                continue;
            }
            [$q, $p] = self::extractSource($file, $code);
            array_push($queries, ...$q);
            array_push($problems, ...$p);
        }
        return [$queries, $problems];
    }

    /**
     * @return array{0: list<array<string, mixed>>, 1: list<string>}
     */
    public static function extractSource(string $file, string $code): array
    {
        $tokens = array_values(\PhpToken::tokenize($code));
        $queries = [];
        $problems = [];
        $n = count($tokens);
        for ($i = 0; $i < $n; ++$i) {
            if (!$tokens[$i]->is(T_ATTRIBUTE)) {
                continue;
            }
            // Inside `#[ … ]`: a comma-separated list of attributes. Walk to the matching `]`.
            $depth = 1;
            $j = $i + 1;
            for (; $j < $n && $depth > 0; ++$j) {
                $t = $tokens[$j];
                if ($t->text === '[' || $t->is(T_ATTRIBUTE)) {
                    ++$depth;
                } elseif ($t->text === ']') {
                    --$depth;
                } elseif ($depth === 1 && $t->is([T_STRING, T_NAME_QUALIFIED, T_NAME_FULLY_QUALIFIED]) && self::isFerroQuery($t->text)) {
                    [$args, $next, $err] = self::parseArgs($tokens, $j + 1);
                    $at = "{$file}:{$t->line}";
                    if ($err !== null) {
                        $problems[] = "{$at}: {$err}";
                    } else {
                        $q = self::toQuery($args, $at, $problems);
                        if ($q !== null) {
                            $queries[] = $q;
                        }
                    }
                    $j = $next - 1;
                }
            }
            $i = $j - 1;
        }
        return [$queries, $problems];
    }

    private static function isFerroQuery(string $name): bool
    {
        return $name === 'FerroQuery' || ltrim($name, '\\') === 'Ferro\\Attribute\\FerroQuery';
    }

    /**
     * Parse `( name: literal, … )` starting at `$i` (the token after the attribute name).
     *
     * @param list<\PhpToken> $tokens
     * @return array{0: array<string, string|bool|null>, 1: int, 2: ?string}
     */
    private static function parseArgs(array $tokens, int $i): array
    {
        $n = count($tokens);
        $i = self::skipTrivia($tokens, $i);
        if ($i >= $n || $tokens[$i]->text !== '(') {
            return [[], $i, 'FerroQuery needs arguments (at least `id:` and `sql:`)'];
        }
        ++$i;
        $args = [];
        while (true) {
            $i = self::skipTrivia($tokens, $i);
            if ($i >= $n) {
                return [[], $i, 'unterminated FerroQuery arguments'];
            }
            if ($tokens[$i]->text === ')') {
                return [$args, $i + 1, null];
            }
            // A name is any identifier-shaped token: `readonly` is the T_READONLY keyword in PHP 8.1+.
            $name = $tokens[$i]->text;
            if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $name) !== 1) {
                return [[], $i, 'FerroQuery arguments must be NAMED (`id: …`, `sql: …`)'];
            }
            $i = self::skipTrivia($tokens, $i + 1);
            if (($tokens[$i] ?? null)?->text !== ':') {
                return [[], $i, 'FerroQuery arguments must be NAMED (`id: …`, `sql: …`)'];
            }
            if (!in_array($name, self::ALLOWED, true)) {
                return [[], $i, "unknown FerroQuery argument `{$name}`"];
            }
            if (array_key_exists($name, $args)) {
                return [[], $i, "FerroQuery argument `{$name}` is repeated"];
            }
            $i = self::skipTrivia($tokens, $i + 1);
            [$value, $i, $err] = self::parseLiteral($tokens, $i);
            if ($err !== null) {
                return [[], $i, "`{$name}`: {$err}"];
            }
            $args[$name] = $value;
            $i = self::skipTrivia($tokens, $i);
            $sep = ($tokens[$i] ?? null)?->text;
            if ($sep === ',') {
                ++$i;
            } elseif ($sep !== ')') {
                return [[], $i, "`{$name}` must be a single literal (no concatenation, constant or expression)"];
            }
        }
    }

    /**
     * @param list<\PhpToken> $tokens
     * @return array{0: string|bool|null, 1: int, 2: ?string}
     */
    private static function parseLiteral(array $tokens, int $i): array
    {
        $t = $tokens[$i] ?? null;
        if ($t === null) {
            return [null, $i, 'missing value'];
        }
        if ($t->is(T_CONSTANT_ENCAPSED_STRING)) {
            $raw = $t->text;
            if ($raw[0] === "'") {
                return [str_replace(['\\\\', "\\'"], ['\\', "'"], substr($raw, 1, -1)), $i + 1, null];
            }
            $inner = substr($raw, 1, -1);
            if (str_contains($inner, '\\')) {
                return [null, $i, 'a double-quoted string with a backslash escape is refused; use single quotes or a nowdoc'];
            }
            return [$inner, $i + 1, null];
        }
        if ($t->is(T_START_HEREDOC)) {
            $isNowdoc = str_contains($t->text, "'");
            $body = '';
            for ($j = $i + 1; isset($tokens[$j]); ++$j) {
                $u = $tokens[$j];
                if ($u->is(T_END_HEREDOC)) {
                    if (!$isNowdoc && str_contains($body, '\\')) {
                        return [null, $j, 'a heredoc with a backslash escape is refused; use a nowdoc (<<<\'SQL\')'];
                    }
                    return [self::dedent($body, $u->text), $j + 1, null];
                }
                if (!$u->is(T_ENCAPSED_AND_WHITESPACE)) {
                    return [null, $j, 'a heredoc with interpolation is refused; use a nowdoc (<<<\'SQL\')'];
                }
                $body .= $u->text;
            }
            return [null, $i, 'unterminated heredoc'];
        }
        if ($t->is(T_STRING)) {
            $word = strtolower($t->text);
            if ($word === 'true' || $word === 'false') {
                return [$word === 'true', $i + 1, null];
            }
            if ($word === 'null') {
                return [null, $i + 1, null];
            }
        }
        return [null, $i, 'must be a literal string, true, false or null (no constant or expression)'];
    }

    /** PHP's flexible heredoc: strip the closing marker's indentation from every line, and the final newline. */
    private static function dedent(string $body, string $endToken): string
    {
        $indent = strlen($endToken) - strlen(ltrim($endToken, " \t"));
        $body = preg_replace('/\R\z/', '', $body) ?? $body;
        if ($indent === 0) {
            return $body;
        }
        $lines = preg_split('/\R/', $body) ?: [$body];
        return implode("\n", array_map(
            static fn (string $l): string => substr($l, min($indent, strlen($l) - strlen(ltrim($l, " \t")))),
            $lines,
        ));
    }

    /** @param list<\PhpToken> $tokens */
    private static function skipTrivia(array $tokens, int $i): int
    {
        while (isset($tokens[$i]) && $tokens[$i]->is([T_WHITESPACE, T_COMMENT, T_DOC_COMMENT])) {
            ++$i;
        }
        return $i;
    }

    /**
     * @param array<string, string|bool|null> $args
     * @param list<string> $problems
     * @return array<string, mixed>|null
     */
    private static function toQuery(array $args, string $at, array &$problems): ?array
    {
        $ok = true;
        foreach (['id', 'sql'] as $required) {
            if (!is_string($args[$required] ?? null) || $args[$required] === '') {
                $problems[] = "{$at}: `{$required}:` is required and must be a non-empty string";
                $ok = false;
            }
        }
        foreach (['readonly', 'idempotent'] as $flag) {
            if (array_key_exists($flag, $args) && !is_bool($args[$flag])) {
                $problems[] = "{$at}: `{$flag}:` must be the literal true or false";
                $ok = false;
            }
        }
        foreach (['pool', 'dto'] as $str) {
            if (array_key_exists($str, $args) && $args[$str] !== null && !is_string($args[$str])) {
                $problems[] = "{$at}: `{$str}:` must be a string";
                $ok = false;
            }
        }
        if (!$ok) {
            return null;
        }
        $q = ['id' => $args['id'], 'sql' => $args['sql']];
        foreach (['pool', 'readonly', 'idempotent', 'dto'] as $k) {
            if (array_key_exists($k, $args) && $args[$k] !== null) {
                $q[$k] = $args[$k];
            }
        }
        $q['source'] = $at;
        return $q;
    }
}
