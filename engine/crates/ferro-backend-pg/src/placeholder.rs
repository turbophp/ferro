//! The `?`→`$n` placeholder scanner (decision M-1, MAJOR-11).
//!
//! Mechanical parameter-syntax normalization that is literal-, comment-, and dollar-quote-aware.
//! A single unescaped `?` outside any of those contexts becomes `$1`, `$2`, … in order. Two rules
//! resolve the jsonb-operator ambiguity a naive scanner can't (a bare `?` is BOTH a placeholder
//! and the jsonb existence operator):
//!
//! - **`??` is an escaped literal `?`** (PDO/Doctrine convention) — emitted as a single `?`, NOT
//!   counted as a placeholder. This is how a real jsonb `?` operator is written.
//! - **`?|` and `?&` are left untouched** via one-char lookahead — the jsonb "any/all keys exist"
//!   operators.
//!
//! Skipped contexts (their `?`s are never placeholders): `'...'` string literals (with `''`
//! escape), `"..."` quoted identifiers (with `""` escape), `--` line comments, `/* */` block
//! comments (PG nests them), and `$tag$...$tag$` dollar-quoted bodies. `$1`/`$2` (positional
//! params) and a bare `$` are NOT dollar-quote opens — a dollar-quote tag is an identifier that
//! cannot start with a digit.
//!
//! Results are cached keyed by the raw SQL in a **bounded** cache (fixed cap, FIFO eviction —
//! MINOR-13): unbounded growth in a long-lived per-host daemon is the bug this guards against.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

/// Default cap for the process-wide normalization cache. Distinct query strings past this are
/// evicted oldest-first; the scan is cheap and paid again on a future miss.
const DEFAULT_CACHE_CAP: usize = 1024;

/// Normalize `sql` (`?`→`$n`), memoized in the process-wide bounded cache. Returns an `Arc<str>`
/// so a cache hit is a cheap refcount bump, not a re-scan or a full copy.
pub fn normalize(sql: &str) -> Arc<str> {
    static CACHE: LazyLock<Mutex<PlaceholderCache>> =
        LazyLock::new(|| Mutex::new(PlaceholderCache::new(DEFAULT_CACHE_CAP)));
    CACHE.lock().unwrap().get_or_insert(sql)
}

/// The pure scanner (no cache). `$n` counting starts at 1. Exposed for the hazard-case corpus.
pub fn scan(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 8);
    let mut i = 0;
    let mut next_param = 1u32;

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            // --- single-quoted string literal (with '' escape) ---
            b'\'' => {
                out.push('\'');
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        // '' is an escaped quote: stay in the string.
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            out.push_str("''");
                            i += 2;
                            continue;
                        }
                        out.push('\'');
                        i += 1;
                        break;
                    }
                    push_byte(&mut out, sql, &mut i);
                }
            }
            // --- double-quoted identifier (with "" escape) ---
            b'"' => {
                out.push('"');
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            out.push_str("\"\"");
                            i += 2;
                            continue;
                        }
                        out.push('"');
                        i += 1;
                        break;
                    }
                    push_byte(&mut out, sql, &mut i);
                }
            }
            // --- line comment ---
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                out.push_str("--");
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    push_byte(&mut out, sql, &mut i);
                }
            }
            // --- block comment (PG nests) ---
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                out.push_str("/*");
                i += 2;
                let mut depth = 1usize;
                while i < bytes.len() && depth > 0 {
                    if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                        out.push_str("/*");
                        i += 2;
                        depth += 1;
                    } else if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        out.push_str("*/");
                        i += 2;
                        depth -= 1;
                    } else {
                        push_byte(&mut out, sql, &mut i);
                    }
                }
            }
            // --- dollar: possible $tag$ dollar-quote open, or a bare `$`/$n param ---
            b'$' => {
                if let Some(tag_end) = dollar_quote_tag_end(bytes, i) {
                    // bytes[i..tag_end] is the opening `$tag$`; copy it and skip to the matching close.
                    let open = &sql[i..tag_end];
                    out.push_str(open);
                    i = tag_end;
                    if let Some(close_at) = find_subslice(bytes, open.as_bytes(), i) {
                        let body_end = close_at + open.len();
                        out.push_str(&sql[i..body_end]);
                        i = body_end;
                    } else {
                        // Unterminated dollar-quote: copy the rest verbatim.
                        out.push_str(&sql[i..]);
                        i = bytes.len();
                    }
                } else {
                    // A bare `$` (e.g. `$1` positional param, or an operator): copy verbatim.
                    push_byte(&mut out, sql, &mut i);
                }
            }
            // --- the placeholder itself ---
            b'?' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'?' {
                    // `??` → literal `?` (jsonb existence operator), NOT a placeholder.
                    out.push('?');
                    i += 2;
                } else if i + 1 < bytes.len() && (bytes[i + 1] == b'|' || bytes[i + 1] == b'&') {
                    // `?|` / `?&` jsonb operators: emit the `?` literally, leave the operator char
                    // for the normal loop to copy next.
                    out.push('?');
                    i += 1;
                } else {
                    // A lone `?` → the next positional `$n`.
                    out.push('$');
                    out.push_str(&next_param.to_string());
                    next_param += 1;
                    i += 1;
                }
            }
            _ => push_byte(&mut out, sql, &mut i),
        }
    }
    out
}

/// Copies the (possibly multibyte-UTF-8) character starting at `bytes[*i]` into `out`, advancing
/// `*i` past it. `sql`/`bytes` are the same buffer; indexing on a UTF-8 char boundary is safe
/// because the scanner only special-cases ASCII bytes, which are always their own boundary.
fn push_byte(out: &mut String, sql: &str, i: &mut usize) {
    let start = *i;
    let mut end = start + 1;
    while end < sql.len() && !sql.is_char_boundary(end) {
        end += 1;
    }
    out.push_str(&sql[start..end]);
    *i = end;
}

/// If a `$tag$` dollar-quote *opens* at `bytes[start]` (`bytes[start] == b'$'`), returns the index
/// just past the closing `$` of the opening delimiter. The tag is `[A-Za-z_][A-Za-z0-9_]*` or
/// empty; a digit right after `$` (e.g. `$1`) is a positional param, NOT a dollar-quote.
fn dollar_quote_tag_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'$');
    let mut j = start + 1;
    // Optional tag: first char (if any) must not be a digit.
    if j < bytes.len() && bytes[j].is_ascii_digit() {
        return None;
    }
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    // Must be terminated by a second `$` to be a dollar-quote open.
    if j < bytes.len() && bytes[j] == b'$' {
        Some(j + 1)
    } else {
        None
    }
}

/// First index at or after `from` where `needle` occurs in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= haystack.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Bounded fingerprint cache: fixed cap, FIFO eviction. Not LRU (insertion-order eviction is
/// enough to keep the cache bounded — MINOR-13); the scan is cheap on a miss.
struct PlaceholderCache {
    cap: usize,
    map: HashMap<String, Arc<str>>,
    order: VecDeque<String>,
}

impl PlaceholderCache {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get_or_insert(&mut self, sql: &str) -> Arc<str> {
        if let Some(hit) = self.map.get(sql) {
            return Arc::clone(hit);
        }
        let normalized: Arc<str> = Arc::from(scan(sql));
        self.map.insert(sql.to_string(), Arc::clone(&normalized));
        self.order.push_back(sql.to_string());
        while self.order.len() > self.cap {
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
            }
        }
        normalized
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_placeholders_number_in_order() {
        assert_eq!(scan("SELECT ?, ?, ?"), "SELECT $1, $2, $3");
        assert_eq!(
            scan("INSERT INTO t (a, b) VALUES (?, ?)"),
            "INSERT INTO t (a, b) VALUES ($1, $2)"
        );
    }

    #[test]
    fn double_question_is_escaped_literal() {
        // `??` → a single literal `?` (jsonb existence operator), NOT a placeholder.
        assert_eq!(scan("data ?? 'key'"), "data ? 'key'");
        // Mixed: the escaped `??` stays literal, the lone `?` becomes $1.
        assert_eq!(scan("a ?? 'k' AND b = ?"), "a ? 'k' AND b = $1");
    }

    #[test]
    fn jsonb_or_and_operators_untouched() {
        assert_eq!(scan("data ?| array['a','b']"), "data ?| array['a','b']");
        assert_eq!(scan("data ?& array['a','b']"), "data ?& array['a','b']");
        // A real placeholder later in the same statement still numbers from 1.
        assert_eq!(scan("data ?| x AND y = ?"), "data ?| x AND y = $1");
    }

    #[test]
    fn question_marks_inside_string_literals_are_ignored() {
        assert_eq!(
            scan("SELECT '? not a param' , ?"),
            "SELECT '? not a param' , $1"
        );
        // '' escaped quote inside the string does not end it early.
        assert_eq!(scan("SELECT 'a '' ? b', ?"), "SELECT 'a '' ? b', $1");
    }

    #[test]
    fn question_marks_inside_quoted_identifiers_are_ignored() {
        assert_eq!(scan(r#"SELECT "we?rd" , ?"#), r#"SELECT "we?rd" , $1"#);
    }

    #[test]
    fn question_marks_inside_comments_are_ignored() {
        assert_eq!(
            scan("SELECT ? -- ? in a comment\n, ?"),
            "SELECT $1 -- ? in a comment\n, $2"
        );
        assert_eq!(scan("SELECT /* ? here */ ?"), "SELECT /* ? here */ $1");
        // PG nests block comments.
        assert_eq!(
            scan("SELECT /* a /* ? */ b */ ?"),
            "SELECT /* a /* ? */ b */ $1"
        );
    }

    #[test]
    fn dollar_quoted_bodies_are_ignored() {
        // A `?` inside a tagged dollar-quoted body is literal.
        assert_eq!(
            scan("SELECT $func$ a ? b $func$, ?"),
            "SELECT $func$ a ? b $func$, $1"
        );
        // Empty tag $$...$$.
        assert_eq!(scan("SELECT $$ ? $$, ?"), "SELECT $$ ? $$, $1");
    }

    #[test]
    fn positional_dollar_params_are_not_dollar_quotes() {
        // `$1` is a positional param, not a dollar-quote open: a following `?` still becomes $1
        // (the scanner counts `?`s independently; native `$n` is passed through verbatim).
        assert_eq!(scan("SELECT $1, ?"), "SELECT $1, $1");
    }

    #[test]
    fn casts_and_double_colons_untouched() {
        assert_eq!(scan("SELECT ?::int, ?::text"), "SELECT $1::int, $2::text");
    }

    #[test]
    fn bounded_cache_evicts_past_cap() {
        let mut cache = PlaceholderCache::new(2);
        cache.get_or_insert("SELECT ?");
        cache.get_or_insert("SELECT ?, ?");
        assert_eq!(cache.len(), 2);
        // Third distinct query evicts the oldest ("SELECT ?"); size stays at the cap.
        cache.get_or_insert("SELECT ?, ?, ?");
        assert_eq!(cache.len(), 2, "cache must stay bounded at its cap");
        assert!(
            !cache.map.contains_key("SELECT ?"),
            "the oldest entry must have been evicted"
        );
        assert!(cache.map.contains_key("SELECT ?, ?, ?"));
    }

    #[test]
    fn cache_hit_returns_same_normalization() {
        let mut cache = PlaceholderCache::new(8);
        let a = cache.get_or_insert("SELECT ?, ?");
        let b = cache.get_or_insert("SELECT ?, ?");
        assert_eq!(&*a, "SELECT $1, $2");
        assert_eq!(&*a, &*b);
        assert_eq!(cache.len(), 1, "a repeated query must not grow the cache");
    }
}
