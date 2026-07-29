//! The region-tracking scanner: literal/comment/dollar-quote-aware, panic-safe, multi-statement.
//!
//! This is the CORRECTNESS CRUX of `ferro-classify` (M1-S2 task T1a): every keyword/identifier
//! match the classifier (`rules.rs`, T1b) makes MUST go through this module's region awareness, or
//! a false-positive/false-negative here becomes a cross-tenant session-state leak (SPEC §7.1).
//!
//! **Ported from `engine/crates/ferro-backend-pg/src/placeholder.rs`** (that crate's `scan()`
//! rewrites `?` → `$n` for the same set of regions; we cannot depend on it — `ferro-classify` is a
//! leaf crate and `ferro-backend-pg → ferro-pool → ferro-classify` would cycle — so the proven
//! region-scanning technique is reimplemented here, adapted from placeholder-substitution to
//! region-tagging):
//! - the nested block-comment **depth counter** (`placeholder.rs:96-109`) → [`Region::BlockComment`]
//! - `''`/PG `E'...'` `\'` string escapes (`placeholder.rs:47-65`) → [`Region::SingleQuote`]
//! - `"..."` quoted idents with `""` escape (`placeholder.rs:66-83`) → [`Region::DoubleQuote`]
//!   (unlike `placeholder.rs`, its content is kept **visible** — see the safety note below)
//! - `$tag$...$tag$` dollar-quotes + `$1` positional-param disambiguation
//!   (`placeholder.rs:111-131,170-189`) → [`Region::DollarQuote`] / [`dollar_quote_tag`]
//! - the char-boundary-safe single-char advance (`placeholder.rs:157-168`) → [`char_end`]
//!
//! **The load-bearing safety direction:** `'...'`/`E'...'` strings, `--`/`/* */` (nested) comments,
//! and `$tag$...$tag$` bodies are NON-code (hidden from identifier/keyword matching and from
//! top-level `;` splitting). `"..."` quoted identifiers ARE code — hidden them and a real call
//! like `SELECT "pg_advisory_lock"(1)` would be MISSED, which is a leak, not a false positive.
//! Every function here is TOTAL (never panics): scanning walks `sql.as_bytes()` under an index
//! that only ever advances to a value produced by [`char_end`] (or by consuming a full multi-byte
//! ASCII marker like `--`/`/*`/`$tag$`, always boundary-aligned since ASCII bytes are always a
//! complete char by themselves in UTF-8) — so every `&sql[..]` slice taken is on a char boundary.
//! Unterminated strings/comments/dollar-quotes consume cleanly to EOF instead of panicking.
//!
//! **Design note (untested edge case, not specified either way):** a literal `;` inside a
//! `"..."` quoted identifier (e.g. `SELECT "a;b"`) is NOT treated as a top-level statement
//! separator by [`split_top_level_statements`], even though quoted-identifier content is
//! otherwise "code". Splitting there would produce a nonsensical fragment for no safety benefit
//! (the whole statement stays intact and still gets scanned in full — no trigger can be hidden by
//! *not* splitting); this is the conservative reading of "prefer a false taint to a missed one"
//! applied to a case the spec's exact wording didn't anticipate.
//!
//! These helpers are `pub(crate)`, consumed by `rules.rs` in task T1b (not yet implemented — until
//! then, `#[allow(dead_code)]` below suppresses the expected "never used outside `#[cfg(test)]`"
//! warning for this standalone task).

#![allow(dead_code)]

/// The current lexical region while scanning left-to-right through `sql`.
///
/// `Code` and `DoubleQuote` are both "visible" (their bytes participate in identifier/keyword
/// matching); `SingleQuote`/`LineComment`/`BlockComment`/`DollarQuote` are "hidden" (masked out).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Region {
    Code,
    /// `"..."` quoted identifier. Content is visible/code (see module doc); `""` is an escaped
    /// literal `"` that does not close the region.
    DoubleQuote,
    /// `'...'` string literal. `e_string` is true iff this open quote was immediately preceded
    /// (token-adjacently) by `E`/`e`, which additionally enables `\'` as an escaped quote.
    SingleQuote {
        e_string: bool,
    },
    /// `-- ...` through end of line (or EOF).
    LineComment,
    /// `/* ... */`, nestable — `depth` counts unclosed opens.
    BlockComment {
        depth: u32,
    },
    /// `$tag$...$tag$` dollar-quoted body. The matching close delimiter (identical text to the
    /// open, e.g. `"$foo$"` or `"$$"`) is tracked in the caller's `dollar_tag` variable, not here
    /// (keeps this enum `Copy`, no lifetime parameter).
    DollarQuote,
}

/// Output of [`scan`]: a same-byte-length masked copy of the input (hidden-region bytes replaced
/// by ASCII spaces, one-for-one, so byte offsets are preserved) plus the byte offsets of every
/// top-level (Code-region) `;`.
struct ScanResult {
    masked: String,
    semicolons: Vec<usize>,
}

/// The single region-tracking pass. Total (never panics) on any input, including empty,
/// multibyte, and unterminated strings/comments/dollar-quotes (which simply consume to EOF).
fn scan(sql: &str) -> ScanResult {
    let bytes = sql.as_bytes();
    let mut masked = String::with_capacity(sql.len());
    let mut semicolons = Vec::new();
    let mut region = Region::Code;
    let mut dollar_tag: &str = "";
    let mut i = 0usize;

    while i < bytes.len() {
        match region {
            Region::Code => match bytes[i] {
                b'\'' => {
                    let e_string = is_e_string_prefix(sql, i);
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::SingleQuote { e_string };
                }
                b'"' => {
                    push_visible_char(&mut masked, sql, &mut i);
                    region = Region::DoubleQuote;
                }
                b'-' if bytes.get(i + 1) == Some(&b'-') => {
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::LineComment;
                }
                b'/' if bytes.get(i + 1) == Some(&b'*') => {
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::BlockComment { depth: 1 };
                }
                b'$' => {
                    if let Some((tag, tag_end)) = dollar_quote_tag(sql, i) {
                        while i < tag_end {
                            push_hidden_char(&mut masked, sql, &mut i);
                        }
                        dollar_tag = tag;
                        region = Region::DollarQuote;
                    } else {
                        push_visible_char(&mut masked, sql, &mut i);
                    }
                }
                b';' => {
                    semicolons.push(i);
                    push_visible_char(&mut masked, sql, &mut i);
                }
                _ => push_visible_char(&mut masked, sql, &mut i),
            },
            Region::DoubleQuote => match bytes[i] {
                b'"' if bytes.get(i + 1) == Some(&b'"') => {
                    // `""` escape: an escaped literal `"`, stays inside the quoted identifier.
                    push_visible_char(&mut masked, sql, &mut i);
                    push_visible_char(&mut masked, sql, &mut i);
                }
                b'"' => {
                    push_visible_char(&mut masked, sql, &mut i);
                    region = Region::Code;
                }
                _ => push_visible_char(&mut masked, sql, &mut i),
            },
            Region::SingleQuote { e_string } => match bytes[i] {
                b'\\' if e_string && bytes.get(i + 1) == Some(&b'\'') => {
                    // E-string backslash escape: `\'` does not close the string.
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                }
                b'\'' if bytes.get(i + 1) == Some(&b'\'') => {
                    // `''` escape: applies to every single-quoted string (E or not).
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                }
                b'\'' => {
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::Code;
                }
                _ => push_hidden_char(&mut masked, sql, &mut i),
            },
            Region::LineComment => match bytes[i] {
                b'\n' => {
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::Code;
                }
                _ => push_hidden_char(&mut masked, sql, &mut i),
            },
            Region::BlockComment { depth } => {
                if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = Region::BlockComment { depth: depth + 1 };
                } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    push_hidden_char(&mut masked, sql, &mut i);
                    push_hidden_char(&mut masked, sql, &mut i);
                    region = if depth <= 1 {
                        Region::Code
                    } else {
                        Region::BlockComment { depth: depth - 1 }
                    };
                } else {
                    push_hidden_char(&mut masked, sql, &mut i);
                }
            }
            Region::DollarQuote => {
                if bytes[i] == b'$' && sql[i..].starts_with(dollar_tag) {
                    let end = i + dollar_tag.len();
                    while i < end {
                        push_hidden_char(&mut masked, sql, &mut i);
                    }
                    region = Region::Code;
                } else {
                    push_hidden_char(&mut masked, sql, &mut i);
                }
            }
        }
    }

    ScanResult { masked, semicolons }
}

/// Copies the char at `sql[*i..]` into `masked` unchanged (it is CODE) and advances `*i` past it.
fn push_visible_char(masked: &mut String, sql: &str, i: &mut usize) {
    let end = char_end(sql, *i);
    masked.push_str(&sql[*i..end]);
    *i = end;
}

/// Replaces the char at `sql[*i..]` with ASCII space filler of the same BYTE length (it is
/// non-code) and advances `*i` past it. Same-byte-length filler is what keeps every later byte
/// offset in `masked` aligned with the original `sql`.
fn push_hidden_char(masked: &mut String, sql: &str, i: &mut usize) {
    let end = char_end(sql, *i);
    for _ in 0..(end - *i) {
        masked.push(' ');
    }
    *i = end;
}

/// The byte index just past the char starting at `sql[start]`. Ported from `placeholder.rs`'s
/// `push_byte` boundary walk (`placeholder.rs:160-168`): never panics, total for any `start <
/// sql.len()`.
fn char_end(sql: &str, start: usize) -> usize {
    let mut end = start + 1;
    while end < sql.len() && !sql.is_char_boundary(end) {
        end += 1;
    }
    end
}

/// True iff the `'` at `sql.as_bytes()[quote_pos]` is immediately preceded by a standalone `E`/`e`
/// token (PG's `E'...'` escape-string prefix) — i.e. the previous byte is `E`/`e` AND the byte
/// before *that* is not an identifier-continuation char (so `TABLE'`'s trailing `E` doesn't
/// count). Never panics: only reads raw bytes at in-bounds indices, never slices a `&str`.
fn is_e_string_prefix(sql: &str, quote_pos: usize) -> bool {
    let bytes = sql.as_bytes();
    if quote_pos == 0 {
        return false;
    }
    let e_pos = quote_pos - 1;
    if !matches!(bytes[e_pos], b'E' | b'e') {
        return false;
    }
    if e_pos == 0 {
        return true;
    }
    !is_ident_char_byte(bytes[e_pos - 1])
}

/// If a `$tag$` dollar-quote OPENS at `sql.as_bytes()[start]` (`== b'$'`), returns `(delimiter,
/// end)`: `delimiter` is the full opening text (e.g. `"$foo$"`, or `"$$"` for an empty tag) —
/// textually identical to its matching close — and `end` is the byte index just past it. A tag is
/// `[A-Za-z_][A-Za-z0-9_]*` or empty; a digit immediately after `$` (e.g. `$1`) is a positional
/// param, NOT a dollar-quote, so this returns `None` and the `$` is ordinary Code. Ported from
/// `placeholder.rs`'s `dollar_quote_tag_end` (`placeholder.rs:173-189`).
fn dollar_quote_tag(sql: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = sql.as_bytes();
    debug_assert_eq!(bytes[start], b'$');
    let mut j = start + 1;
    if j < bytes.len() && bytes[j].is_ascii_digit() {
        return None;
    }
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j < bytes.len() && bytes[j] == b'$' {
        Some((&sql[start..=j], j + 1))
    } else {
        None
    }
}

fn is_ident_char_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Skips leading whitespace + `--` line comments + nested `/* */` block comments (looped: any mix
/// of these repeated any number of times). An unterminated block comment consumes everything to
/// EOF, so nothing is left to classify — returns `""`. Total: uses only `str::trim_start` /
/// `str::find` / `strip_prefix`, all of which return char-boundary-safe offsets.
pub(crate) fn strip_leading_noise(sql: &str) -> &str {
    let mut rest = sql;
    loop {
        let trimmed = rest.trim_start();
        if let Some(after) = trimmed.strip_prefix("--") {
            let nl = after.find('\n').map(|i| i + 1).unwrap_or(after.len());
            rest = &after[nl..];
            continue;
        }
        if let Some(after) = trimmed.strip_prefix("/*") {
            match skip_block_comment(after) {
                Some(remaining) => {
                    rest = remaining;
                    continue;
                }
                None => return "",
            }
        }
        return trimmed;
    }
}

/// Skips a (possibly nested) block comment whose opening `/*` has ALREADY been consumed (`s` is
/// the text right after it). Returns the text after the matching `*/`, or `None` if it (or a
/// nested one) never closes.
fn skip_block_comment(mut s: &str) -> Option<&str> {
    let mut depth = 1u32;
    loop {
        let next_open = s.find("/*");
        let next_close = s.find("*/");
        match (next_open, next_close) {
            (Some(o), Some(c)) if o < c => {
                depth += 1;
                s = &s[o + 2..];
            }
            (_, Some(c)) => {
                s = &s[c + 2..];
                depth -= 1;
                if depth == 0 {
                    return Some(s);
                }
            }
            _ => return None,
        }
    }
}

/// The first maximal ASCII-alphabetic run after [`strip_leading_noise`], uppercased. `None` if
/// nothing (or non-alphabetic content) remains.
pub(crate) fn leading_keyword(sql: &str) -> Option<String> {
    let rest = strip_leading_noise(sql);
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    if end == 0 {
        None
    } else {
        Some(rest[..end].to_ascii_uppercase())
    }
}

/// The token immediately after the leading keyword, skipping whitespace/comments — but ONLY if it
/// is a COMPLETE token (the char right after its alphabetic run is neither an identifier-
/// continuation char `[A-Za-z0-9_]` nor `.`). Otherwise `None` (a dotted/underscored/ident-
/// continued next word, e.g. `local.foo`/`local_x`, is deliberately not read as a bare keyword —
/// this is what makes `SET LOCAL x` exact-token detection safe against `SET local.foo`).
pub(crate) fn next_token_after_keyword(sql: &str) -> Option<String> {
    let rest = strip_leading_noise(sql);
    let kw_end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    if kw_end == 0 {
        return None;
    }
    let after_noise = strip_leading_noise(&rest[kw_end..]);
    let tok_end = after_noise
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(after_noise.len());
    if tok_end == 0 {
        return None;
    }
    match after_noise[tok_end..].chars().next() {
        Some(c) if c == '.' || c.is_ascii_alphanumeric() || c == '_' => None,
        _ => Some(after_noise[..tok_end].to_ascii_uppercase()),
    }
}

/// True iff `ident` (ASCII, case-insensitive) appears as a WHOLE identifier — neighboring bytes
/// (if any) are not `[A-Za-z0-9_]` — inside a CODE region (everything except `'...'`/`E'...'`
/// strings, `--`/`/* */` (nested) comments, and `$tag$...$tag$` bodies; `"..."` quoted identifiers
/// count as code). A leading schema qualifier is fine — `pg_catalog.pg_advisory_lock` still
/// matches the bare `pg_advisory_lock` (`.` is not an identifier-continuation byte).
pub(crate) fn contains_identifier_ci(sql: &str, ident: &str) -> bool {
    if ident.is_empty() {
        return false;
    }
    let masked = scan(sql).masked;
    let hay = masked.to_ascii_lowercase();
    let needle = ident.to_ascii_lowercase();
    let hay = hay.as_bytes();
    let needle = needle.as_bytes();
    let n = needle.len();
    if n > hay.len() {
        return false;
    }
    hay.windows(n).enumerate().any(|(start, w)| {
        w == needle
            && (start == 0 || !is_ident_char_byte(hay[start - 1]))
            && (start + n == hay.len() || !is_ident_char_byte(hay[start + n]))
    })
}

/// Splits `sql` on `;` that lies in a CODE region (not inside any string/comment/dollar-quote —
/// see the module doc for the one deliberately-untested exception, `;` inside a quoted
/// identifier). Trims each piece and drops empties. Used for the `exec` multi-statement batch
/// path, so a trailing/leading/doubled `;` never produces a spurious empty statement.
pub(crate) fn split_top_level_statements(sql: &str) -> Vec<&str> {
    let ScanResult { semicolons, .. } = scan(sql);
    let mut out = Vec::with_capacity(semicolons.len() + 1);
    let mut start = 0usize;
    for pos in semicolons {
        let piece = sql[start..pos].trim();
        if !piece.is_empty() {
            out.push(piece);
        }
        start = pos + 1; // `;` is one ASCII byte, so `pos + 1` is always a valid char boundary.
    }
    let tail = sql[start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- contains_identifier_ci: TRUE cases ---------------------------------------------------

    #[test]
    fn ci_true_plain_call() {
        assert!(contains_identifier_ci(
            "SELECT pg_advisory_lock(1)",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_case_insensitive() {
        assert!(contains_identifier_ci(
            "SELECT PG_ADVISORY_LOCK(1)",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_schema_qualified() {
        assert!(contains_identifier_ci(
            "SELECT pg_catalog.pg_advisory_lock(1)",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_quoted_identifier_is_code() {
        // Load-bearing: a quoted identifier IS a real, executable call. Hiding it would MISS a
        // trigger -- that is the leak this scanner exists to prevent.
        assert!(contains_identifier_ci(
            r#"SELECT "pg_advisory_lock"(1)"#,
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_across_block_comment() {
        assert!(contains_identifier_ci(
            "SELECT/* c */pg_advisory_lock(1)",
            "pg_advisory_lock"
        ));
    }

    // ---- contains_identifier_ci: FALSE cases --------------------------------------------------

    #[test]
    fn ci_false_inside_string() {
        assert!(!contains_identifier_ci(
            "SELECT 'pg_advisory_lock'",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_inside_line_comment() {
        assert!(!contains_identifier_ci(
            "-- pg_advisory_lock",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_inside_block_comment() {
        assert!(!contains_identifier_ci(
            "/* pg_advisory_lock */ SELECT 1",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_inside_dollar_quote() {
        assert!(!contains_identifier_ci(
            "$$ pg_advisory_lock $$",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_not_whole_identifier_prefixed() {
        assert!(!contains_identifier_ci(
            "SELECT my_pg_advisory_lock",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_not_whole_identifier_suffixed() {
        assert!(!contains_identifier_ci(
            "SELECT pg_advisory_lockx",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_doubled_quote_escape_still_a_string() {
        assert!(!contains_identifier_ci(
            "SELECT 'it''s pg_advisory_lock'",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_e_string_backslash_escape_does_not_close() {
        // Positive direction of the `is_e_string_prefix` boundary: a standalone `E'...'` IS an
        // E-string, so `\'` does NOT close it -- "pg_advisory_lock" stays swallowed inside the
        // string (FALSE). If E-string recognition were removed (e_string always false), the
        // backslash would have no special meaning, the string would close right after `\`, and
        // "pg_advisory_lock" would spill into CODE (this assertion would flip to TRUE).
        assert!(!contains_identifier_ci(
            r"SELECT E'a\' pg_advisory_lock'",
            "pg_advisory_lock"
        ));
    }

    // ---- is_e_string_prefix rejection boundary: an identifier ENDING in e/E immediately before a
    // quote is NOT a standalone E-prefix, so `\'` must NOT be treated as an escape there -- this is
    // the leak-relevant direction (a wrongly-accepted E-prefix would swallow a trailing trigger
    // into the "hidden" string region instead of leaving it in CODE). Each input below would FAIL
    // (flip to `false`) if `is_e_string_prefix` incorrectly returned `true` for it.

    #[test]
    fn ci_true_identifier_ending_in_e_does_not_enable_backslash_escape() {
        // "TABLE" ends in `E`, directly adjacent to the quote -- but `L` (not an identifier
        // boundary) precedes that `E`, so it is NOT a standalone E-prefix. The backslash has no
        // special meaning, the string closes right after it, and the trailing trigger is CODE.
        assert!(contains_identifier_ci(
            r"TABLE'a\' pg_advisory_lock(1)'",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_lowercase_identifier_ending_in_e_does_not_enable_backslash_escape() {
        // Same boundary, lowercase and a longer/underscored identifier ("some_table" ends in `e`).
        assert!(contains_identifier_ci(
            r"some_table'a\' pg_advisory_lock(1)'",
            "pg_advisory_lock"
        ));
    }

    // ---- nested / adjacent regions ------------------------------------------------------------

    #[test]
    fn ci_false_nested_block_comment() {
        assert!(!contains_identifier_ci(
            "/* /* */ pg_advisory_lock */ SELECT 1",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_after_line_comment_and_newline() {
        assert!(contains_identifier_ci(
            "SELECT 1 -- x\npg_advisory_lock(1)",
            "pg_advisory_lock"
        ));
    }

    // ---- dollar-quote edge cases ---------------------------------------------------------------

    #[test]
    fn ci_false_digit_in_tag_body() {
        assert!(!contains_identifier_ci(
            "$a1$ pg_advisory_lock $a1$",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_true_positional_param_is_not_a_dollar_quote() {
        assert!(contains_identifier_ci(
            "$1 + pg_advisory_lock(1)",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn ci_false_mismatched_tag_all_inside_outer_body() {
        assert!(!contains_identifier_ci(
            "$a$ x $b$ pg_advisory_lock $a$",
            "pg_advisory_lock"
        ));
    }

    // ---- leading_keyword ------------------------------------------------------------------------

    #[test]
    fn leading_keyword_plain() {
        assert_eq!(leading_keyword("SELECT 1"), Some("SELECT".to_string()));
    }

    #[test]
    fn leading_keyword_skips_leading_comments_and_whitespace() {
        assert_eq!(
            leading_keyword("  -- hi\n/* x */  SELECT 1"),
            Some("SELECT".to_string())
        );
    }

    #[test]
    fn leading_keyword_empty_and_whitespace_are_none() {
        assert_eq!(leading_keyword(""), None);
        assert_eq!(leading_keyword("   "), None);
    }

    // ---- next_token_after_keyword ----------------------------------------------------------------

    #[test]
    fn next_token_set_local_is_exact() {
        assert_eq!(
            next_token_after_keyword("SET LOCAL x"),
            Some("LOCAL".to_string())
        );
    }

    #[test]
    fn next_token_dotted_guc_is_none() {
        // `local.foo` is NOT the bare token `LOCAL` -- must not be excluded as SET LOCAL.
        assert_eq!(next_token_after_keyword("SET local.foo"), None);
    }

    #[test]
    fn next_token_underscored_guc_is_none() {
        assert_eq!(next_token_after_keyword("SET local_x"), None);
    }

    #[test]
    fn next_token_after_comment_is_the_real_next_token() {
        // The comment is transparent; the actual next token is `x`, not `LOCAL` -- so the
        // *classifier* (T1b) must NOT treat this as SET LOCAL (Some("X") != Some("LOCAL")).
        assert_eq!(
            next_token_after_keyword("SET/* LOCAL */x"),
            Some("X".to_string())
        );
    }

    #[test]
    fn next_token_extra_whitespace() {
        assert_eq!(
            next_token_after_keyword("SET  LOCAL  y"),
            Some("LOCAL".to_string())
        );
    }

    // ---- split_top_level_statements ----------------------------------------------------------------

    #[test]
    fn split_two_statements() {
        assert_eq!(
            split_top_level_statements("SELECT 1; LISTEN c"),
            vec!["SELECT 1", "LISTEN c"]
        );
    }

    #[test]
    fn split_semicolon_in_string_is_not_a_split() {
        assert_eq!(split_top_level_statements("SELECT ';'"), vec!["SELECT ';'"]);
    }

    #[test]
    fn split_semicolon_in_comment_is_not_a_split() {
        assert_eq!(
            split_top_level_statements("SELECT 1 /* ; */ ; SELECT 2"),
            vec!["SELECT 1 /* ; */", "SELECT 2"]
        );
    }

    #[test]
    fn split_trims_and_drops_empties() {
        assert_eq!(
            split_top_level_statements(" SELECT 1 ; ; SELECT 2 ; "),
            vec!["SELECT 1", "SELECT 2"]
        );
    }

    // ---- design decision: `;` inside a `"..."` quoted identifier is NOT a split point ---------
    //
    // Locks in the judgment call documented in the module doc and confirmed correct by review: a
    // literal `;` that is part of a quoted identifier's NAME (not a real statement terminator) does
    // not split the statement, even though quoted-identifier content otherwise counts as CODE.

    #[test]
    fn split_semicolon_inside_quoted_identifier_is_not_a_split() {
        assert_eq!(
            split_top_level_statements(r#"CREATE TABLE "a;b" (x int)"#),
            vec![r#"CREATE TABLE "a;b" (x int)"#]
        );
    }

    #[test]
    fn split_real_semicolon_after_quoted_identifier_still_splits() {
        assert_eq!(
            split_top_level_statements(r#"CREATE TABLE "a;b"(x int); LISTEN c"#),
            vec![r#"CREATE TABLE "a;b"(x int)"#, "LISTEN c"]
        );
    }

    #[test]
    fn ci_true_trigger_inside_quoted_identifier_with_embedded_semicolon() {
        // Ties the two directions together: the `;` inside the quoted name does not split the
        // statement (previous test), AND the quoted-identifier content is still CODE, so a trigger
        // substring embedded in it is still found -- consistent with "quoted idents are code, not
        // hidden" (the opposite bug -- hiding quoted-ident content -- is the actual leak surface).
        assert!(contains_identifier_ci(
            r#"CREATE TABLE "a;pg_advisory_lock" (x int)"#,
            "pg_advisory_lock"
        ));
    }

    // ---- panic-safety: every helper, on a hostile corpus, must never panic --------------------

    #[test]
    fn panic_safety_corpus() {
        let inputs = [
            "",
            "   ",
            "SELECT 'café'",
            "SELECT '", // unterminated string
            "/* x",     // unterminated block comment
            "$$ x",     // unterminated dollar-quote
        ];
        for sql in inputs {
            let _ = strip_leading_noise(sql);
            let _ = leading_keyword(sql);
            let _ = next_token_after_keyword(sql);
            let _ = contains_identifier_ci(sql, "pg_advisory_lock");
            let _ = split_top_level_statements(sql);
        }
    }

    #[test]
    fn panic_safety_empty_and_whitespace_produce_no_statements() {
        assert_eq!(split_top_level_statements(""), Vec::<&str>::new());
        assert_eq!(split_top_level_statements("   "), Vec::<&str>::new());
    }

    #[test]
    fn panic_safety_multibyte_leading_keyword_still_found() {
        assert_eq!(leading_keyword("SELECT 'café'"), Some("SELECT".to_string()));
    }

    #[test]
    fn panic_safety_multibyte_ident_inside_string_is_false() {
        assert!(!contains_identifier_ci("SELECT 'café'", "café"));
    }

    #[test]
    fn panic_safety_unterminated_string_consumes_to_eof() {
        // No closing quote anywhere: the identifier after it (if any) is unreachable/hidden, and
        // splitting sees no top-level `;` -- the whole thing is one (trimmed) statement.
        assert!(!contains_identifier_ci(
            "SELECT ' pg_advisory_lock",
            "pg_advisory_lock"
        ));
        assert_eq!(
            split_top_level_statements("SELECT ' pg_advisory_lock"),
            vec!["SELECT ' pg_advisory_lock"]
        );
    }

    #[test]
    fn panic_safety_unterminated_block_comment() {
        assert_eq!(strip_leading_noise("/* x"), "");
        assert_eq!(leading_keyword("/* x"), None);
        assert!(!contains_identifier_ci(
            "/* x pg_advisory_lock",
            "pg_advisory_lock"
        ));
    }

    #[test]
    fn panic_safety_unterminated_dollar_quote() {
        assert!(!contains_identifier_ci(
            "$$ pg_advisory_lock",
            "pg_advisory_lock"
        ));
        assert_eq!(split_top_level_statements("$$ x ; y"), vec!["$$ x ; y"]);
    }
}
