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
 * The attribute is recognised by RESOLVING its name exactly as PHP does — against the file's
 * `namespace` and its `use` imports (aliases and group imports included), case-insensitively — so
 * `#[Q(...)]` after `use Ferro\Attribute\FerroQuery as Q` is seen, and a `FerroQuery` that PHP would
 * resolve to some other class is not (M3-D2a review F3/F4). Only a top-level attribute of an
 * attribute group counts: `#[Other(new FerroQuery(...))]` or `#[CoversClass(FerroQuery::class)]`
 * declares nothing.
 *
 * Every string must be valid UTF-8 (the manifest is JSON), and a heredoc/nowdoc body keeps its
 * line terminators byte for byte.
 *
 * Known leniency: the attribute's TARGET is not checked, so a declaration on a function or a
 * parameter, which PHP refuses when the attribute is instantiated, is still extracted.
 *
 * Kept free of `ferro/client`'s other classes so the CLI script can `require` it alone.
 */
final class QueryExtractor
{
    private const ALLOWED = ['id', 'sql', 'pool', 'readonly', 'idempotent', 'dto'];

    /**
     * @param list<string> $paths files or directories (directories are searched for `*.php`,
     *                            case-insensitively, following symlinked directories once each)
     * @return array{0: list<array<string, mixed>>, 1: list<string>} the queries, and every problem
     */
    public static function extractPaths(array $paths): array
    {
        $files = [];
        $problems = [];
        $seen = [];
        foreach ($paths as $path) {
            if (is_dir($path)) {
                self::walk($path, $files, $seen, $problems);
            } elseif (is_file($path)) {
                $files[] = $path;
            } else {
                $problems[] = "{$path}: no such file or directory";
            }
        }
        if ($problems !== []) {
            return [[], $problems];
        }
        $files = array_values(array_unique($files));
        sort($files);
        $queries = [];
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
     * The same walk as the Rust side's `collect_sql_dir`: symlinked directories are followed, each
     * real directory at most once, so a link cycle cannot hang it (M3-D2a review F5).
     *
     * @param list<string> $files
     * @param array<string, true> $seen
     * @param list<string> $problems
     */
    private static function walk(string $dir, array &$files, array &$seen, array &$problems): void
    {
        $real = realpath($dir);
        if ($real === false) {
            $problems[] = "{$dir}: cannot read directory";
            return;
        }
        if (isset($seen[$real])) {
            return;
        }
        $seen[$real] = true;
        $entries = scandir($dir);
        if ($entries === false) {
            $problems[] = "{$dir}: cannot read directory";
            return;
        }
        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $path = rtrim($dir, '/') . '/' . $entry;
            if (is_dir($path)) {
                self::walk($path, $files, $seen, $problems);
            } elseif (is_file($path) && strcasecmp(pathinfo($path, PATHINFO_EXTENSION), 'php') === 0) {
                $files[] = $path;
            }
        }
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

        $namespace = '';
        /** @var array<string, string> $imports lower-cased alias => fully-qualified class name */
        $imports = [];
        $depth = 0;          // `{` nesting
        $nsDepth = 0;        // the depth namespace-level code lives at (1 inside `namespace X { }`)

        for ($i = 0; $i < $n; ++$i) {
            $t = $tokens[$i];
            if ($t->text === '{' || $t->is([T_CURLY_OPEN, T_DOLLAR_OPEN_CURLY_BRACES])) {
                ++$depth;
                continue;
            }
            if ($t->text === '}') {
                --$depth;
                if ($depth < $nsDepth) {
                    // The end of a braced `namespace X { }`.
                    $nsDepth = 0;
                    $namespace = '';
                    $imports = [];
                }
                continue;
            }
            if ($t->is(T_NAMESPACE)) {
                $j = self::skipTrivia($tokens, $i + 1);
                $next = $tokens[$j] ?? null;
                if ($next !== null && $next->is([T_STRING, T_NAME_QUALIFIED])) {
                    $namespace = $next->text;
                    $j = self::skipTrivia($tokens, $j + 1);
                } elseif ($next === null || $next->text !== '{') {
                    continue; // not a declaration
                } else {
                    $namespace = '';
                }
                $imports = [];
                if (($tokens[$j] ?? null)?->text === '{') {
                    ++$depth;
                    $nsDepth = $depth;
                }
                $i = $j;
                continue;
            }
            if ($t->is(T_USE) && $depth === $nsDepth) {
                $i = self::parseUse($tokens, $i + 1, $imports);
                continue;
            }
            if (!$t->is(T_ATTRIBUTE)) {
                continue;
            }

            // Inside `#[ … ]`: a comma-separated list of attributes. Walk to the matching `]`,
            // considering only names that START an attribute (after `#[` or a top-level `,`).
            $brackets = 1;
            $parens = 0;
            $atStart = true;
            $j = $i + 1;
            for (; $j < $n && $brackets > 0; ++$j) {
                $u = $tokens[$j];
                if ($u->is([T_WHITESPACE, T_COMMENT, T_DOC_COMMENT])) {
                    continue;
                }
                $starts = $atStart;
                $atStart = false;
                if ($u->text === '[' || $u->is(T_ATTRIBUTE)) {
                    ++$brackets;
                } elseif ($u->text === ']') {
                    --$brackets;
                } elseif ($u->text === '(') {
                    ++$parens;
                } elseif ($u->text === ')') {
                    --$parens;
                } elseif ($u->text === ',' && $brackets === 1 && $parens === 0) {
                    $atStart = true;
                } elseif (
                    $starts && $brackets === 1 && $parens === 0
                    && $u->is([T_STRING, T_NAME_QUALIFIED, T_NAME_FULLY_QUALIFIED, T_NAME_RELATIVE])
                    && strcasecmp(self::resolve($u, $namespace, $imports), 'Ferro\\Attribute\\FerroQuery') === 0
                ) {
                    [$args, $next, $err] = self::parseArgs($tokens, $j + 1);
                    $at = "{$file}:{$u->line}";
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

    /**
     * The fully-qualified class name PHP gives `$name` in `$namespace` with `$imports` (the rules of
     * https://www.php.net/manual/en/language.namespaces.rules.php for class names).
     *
     * @param array<string, string> $imports
     */
    private static function resolve(\PhpToken $name, string $namespace, array $imports): string
    {
        $text = $name->text;
        if ($name->is(T_NAME_FULLY_QUALIFIED)) {
            return substr($text, 1);
        }
        if ($name->is(T_NAME_RELATIVE)) {
            // `namespace\X`
            $rest = substr($text, strlen('namespace\\'));
            return $namespace === '' ? $rest : $namespace . '\\' . $rest;
        }
        $first = explode('\\', $text, 2);
        $alias = strtolower($first[0]);
        if (isset($imports[$alias])) {
            return $imports[$alias] . (isset($first[1]) ? '\\' . $first[1] : '');
        }
        return $namespace === '' ? $text : $namespace . '\\' . $text;
    }

    /**
     * Parse a namespace-level `use` statement starting after the `use` keyword, recording class
     * imports in `$imports`, and return the index of its `;`. `use function`/`use const` import no
     * class and are skipped, as is a closure's `use (…)`.
     *
     * @param list<\PhpToken> $tokens
     * @param array<string, string> $imports
     */
    private static function parseUse(array $tokens, int $i, array &$imports): int
    {
        $n = count($tokens);
        $i = self::skipTrivia($tokens, $i);
        if (($tokens[$i] ?? null)?->text === '(') {
            return $i - 1; // a closure's `use (…)`
        }
        $kind = 'class';
        if (($tokens[$i] ?? null)?->is([T_FUNCTION, T_CONST]) === true) {
            $kind = 'other';
            ++$i;
        }
        $prefix = '';
        $name = '';
        $alias = null;
        $inGroup = false;
        $entryKind = $kind;
        for (; $i < $n; ++$i) {
            $t = $tokens[$i];
            if ($t->is([T_WHITESPACE, T_COMMENT, T_DOC_COMMENT])) {
                continue;
            }
            if ($t->text === ';') {
                self::addImport($imports, $entryKind, $prefix, $name, $alias);
                return $i;
            }
            if ($t->text === '{') {
                $prefix = rtrim($name, '\\') . '\\';
                $name = '';
                $inGroup = true;
                continue;
            }
            if ($t->text === '}') {
                self::addImport($imports, $entryKind, $prefix, $name, $alias);
                [$name, $alias, $inGroup] = ['', null, false];
                continue;
            }
            if ($t->text === ',') {
                self::addImport($imports, $entryKind, $prefix, $name, $alias);
                [$name, $alias] = ['', null];
                $entryKind = $kind;
                if (!$inGroup) {
                    $prefix = '';
                }
                continue;
            }
            if ($t->is(T_AS)) {
                $i = self::skipTrivia($tokens, $i + 1);
                $alias = $tokens[$i]->text ?? null;
                continue;
            }
            if ($inGroup && $name === '' && $t->is([T_FUNCTION, T_CONST])) {
                $entryKind = 'other';
                continue;
            }
            $name .= $t->text;
        }
        return $n;
    }

    /** @param array<string, string> $imports */
    private static function addImport(array &$imports, string $kind, string $prefix, string $name, ?string $alias): void
    {
        if ($name === '' || $kind !== 'class') {
            return;
        }
        $fq = ltrim($prefix . $name, '\\');
        $short = $alias ?? substr((string) strrchr('\\' . $fq, '\\'), 1);
        $imports[strtolower($short)] = $fq;
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
            // A binary-string prefix (`b'…'`, `B"…"`) changes nothing in PHP 8; strip it before
            // reading the quote (M3-D2a review F2: it was read as part of the string).
            if ($raw[0] === 'b' || $raw[0] === 'B') {
                $raw = substr($raw, 1);
            }
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
            // The binary-prefixed form `b<<<…` arrives as the same token with a leading `b`.
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

    /**
     * PHP's flexible heredoc: strip the closing marker's indentation from every line, and the final
     * line terminator. Line terminators are KEPT byte for byte (`\r\n`, `\n`, `\r`): splitting on
     * PCRE's `\R` turned CRLF into LF, `\f`/`\v` into a newline, and — without `/u` — byte 0x85
     * inside a UTF-8 character into a line break (M3-D2a review F1).
     */
    private static function dedent(string $body, string $endToken): string
    {
        $indent = strlen($endToken) - strlen(ltrim($endToken, " \t"));
        $body = preg_replace('/(?:\r\n|\n|\r)\z/', '', $body) ?? $body;
        if ($indent === 0) {
            return $body;
        }
        $parts = preg_split('/(\r\n|\n|\r)/', $body, -1, PREG_SPLIT_DELIM_CAPTURE) ?: [$body];
        $out = '';
        foreach ($parts as $k => $part) {
            if ($k % 2 === 1) {
                $out .= $part; // a terminator, as written
                continue;
            }
            $lead = strlen($part) - strlen(ltrim($part, " \t"));
            $out .= substr($part, min($indent, $lead));
        }
        return $out;
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
        if (array_key_exists('pool', $args) && !is_string($args['pool'])) {
            $problems[] = "{$at}: `pool:` must be a string";
            $ok = false;
        }
        if (array_key_exists('dto', $args) && $args['dto'] !== null && !is_string($args['dto'])) {
            $problems[] = "{$at}: `dto:` must be a string or null";
            $ok = false;
        }
        foreach ($args as $k => $v) {
            if (is_string($v) && preg_match('//u', $v) !== 1) {
                $problems[] = "{$at}: `{$k}:` is not valid UTF-8";
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
