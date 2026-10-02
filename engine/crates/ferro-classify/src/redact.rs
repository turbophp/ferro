//! The REDACTION region pass: which bytes of a statement are VALUES, per dialect, failing CLOSED.
//!
//! [`crate::scan`] answers a different question for a different consumer, and the two have
//! OPPOSITE safe directions. The pin lexer must never MISS a session-mutating keyword, so when a
//! dialect is ambiguous it keeps text VISIBLE (an over-pin costs a reset, a missed pin leaks
//! session state across tenants). A fingerprint must never SHOW a value, so when a dialect is
//! ambiguous this pass HIDES (an over-hidden fingerprint is less readable; an under-hidden one puts
//! a password in a log file or an exported span). C4a reused the pin lexer for fingerprints and the
//! C4c-2 review measured the cost: on MySQL `"…"` is a string and `\'` does not end one, and on
//! SQLite `"…"` falls back to a string — both reached a collector verbatim.
//!
//! **Where the server's own reading is unknowable, the rest of the statement is hidden.** Two
//! session settings change where a literal ENDS, and neither is visible to the engine:
//! PostgreSQL's `standard_conforming_strings` and MySQL's `NO_BACKSLASH_ESCAPES` (C1g measured
//! that an app can flip either inside its own transaction). They disagree only on a run of an ODD
//! number of backslashes immediately before the closing quote; there, this pass hides everything to
//! the end of the statement rather than pick a mode. Every other backslash means the same in both
//! modes, so a Windows path or a JSON escape keeps the statement's shape.
//!
//! Per dialect:
//!
//! | | `'…'` | `"…"` | `` `…` `` | `[…]` | comments | `$tag$…$tag$` |
//! |---|---|---|---|---|---|---|
//! | PostgreSQL | literal; `E'…'` escapes; else mode-ambiguous | identifier | code | code | `--`, `/* */` | literal |
//! | MySQL/MariaDB | literal, mode-ambiguous | literal (string unless `ANSI_QUOTES`), mode-ambiguous | identifier | code | `--`, `#`, `/* */` | code |
//! | SQLite | literal | literal (the `DQS` fallback can make it a string) | identifier | identifier | `--`, `/* */` | code |
//!
//! Identifier regions are kept VISIBLE — they are the statement's shape — but they are still
//! REGIONS, because a `'` inside `` `it's` `` must not open a literal that then closes on the next
//! real literal's opening quote and exposes its content. An UNTERMINATED region of any kind,
//! identifiers included, hides everything to the end.
//!
//! Total on any input, like `scan`: the index only advances to char boundaries.

use crate::Dialect;
use crate::scan::Hidden;

/// The hidden spans of `sql` under `dialect`'s rules, in ascending order, each a VALUE
/// (`Literal`) or a `Comment`.
pub(crate) fn hidden_spans(sql: &str, dialect: Dialect) -> Vec<(usize, usize, Hidden)> {
    let b = sql.as_bytes();
    let len = b.len();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < len {
        match b[i] {
            b'\'' => {
                let start = prefix_start(sql, i);
                let escapes = match dialect {
                    Dialect::Postgres if is_prefixed_by(sql, i, b"Ee") => Backslash::Escapes,
                    Dialect::Postgres | Dialect::MySql => Backslash::Ambiguous,
                    Dialect::Sqlite => Backslash::Ordinary,
                };
                match close_quoted(sql, i, b'\'', escapes) {
                    Some(end) => {
                        out.push((start, end, Hidden::Literal));
                        i = end;
                    }
                    None => return hide_rest(out, start, len),
                }
            }
            b'"' => match dialect {
                Dialect::Postgres => match close_quoted(sql, i, b'"', Backslash::Ordinary) {
                    Some(end) => i = end,
                    None => return hide_rest(out, i, len),
                },
                Dialect::MySql | Dialect::Sqlite => {
                    let escapes = if dialect == Dialect::MySql {
                        Backslash::Ambiguous
                    } else {
                        Backslash::Ordinary
                    };
                    match close_quoted(sql, i, b'"', escapes) {
                        Some(end) => {
                            out.push((i, end, Hidden::Literal));
                            i = end;
                        }
                        None => return hide_rest(out, i, len),
                    }
                }
            },
            b'`' if dialect != Dialect::Postgres => {
                match close_quoted(sql, i, b'`', Backslash::Ordinary) {
                    Some(end) => i = end,
                    None => return hide_rest(out, i, len),
                }
            }
            b'[' if dialect == Dialect::Sqlite => {
                match b[i + 1..].iter().position(|&c| c == b']') {
                    Some(off) => i = i + 1 + off + 1,
                    None => return hide_rest(out, i, len),
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => i = line_comment(b, i, &mut out),
            b'#' if dialect == Dialect::MySql => i = line_comment(b, i, &mut out),
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // Nested, as PostgreSQL nests. MySQL and SQLite do not, and for them nesting can
                // only hide MORE (a comment runs past where the server would end it), never less.
                let mut depth = 0u32;
                let mut j = i;
                while j < len {
                    if b[j] == b'/' && b.get(j + 1) == Some(&b'*') {
                        depth += 1;
                        j += 2;
                    } else if b[j] == b'*' && b.get(j + 1) == Some(&b'/') {
                        depth -= 1;
                        j += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        j += 1;
                    }
                }
                if depth > 0 {
                    return hide_rest_as(out, i, len, Hidden::Comment);
                }
                out.push((i, j, Hidden::Comment));
                i = j;
            }
            b'$' if dialect == Dialect::Postgres => match crate::scan::dollar_quote_tag(sql, i) {
                Some((tag, body)) => match sql[body..].find(tag) {
                    Some(off) => {
                        let end = body + off + tag.len();
                        out.push((i, end, Hidden::Literal));
                        i = end;
                    }
                    None => return hide_rest(out, i, len),
                },
                None => i = char_end(sql, i),
            },
            _ => i = char_end(sql, i),
        }
    }
    out
}

/// How a backslash behaves inside a quoted region.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Backslash {
    /// An ordinary character (SQLite; identifiers; PostgreSQL's quoted identifiers).
    Ordinary,
    /// Always an escape for the next character (PostgreSQL's `E'…'`).
    Escapes,
    /// An escape or not depending on a session setting the engine cannot see.
    Ambiguous,
}

/// The index just past the quote that closes the region opened at `open`, or `None` when the region
/// does not close — or, for [`Backslash::Ambiguous`], when where it closes depends on the mode.
fn close_quoted(sql: &str, open: usize, quote: u8, backslash: Backslash) -> Option<usize> {
    let b = sql.as_bytes();
    let mut j = open + 1;
    while j < b.len() {
        let c = b[j];
        if c == b'\\' && backslash != Backslash::Ordinary {
            if backslash == Backslash::Escapes {
                j += 1;
                if j < b.len() {
                    j = char_end(sql, j);
                }
                continue;
            }
            // Ambiguous: only an ODD run of backslashes right before the quote reads differently
            // in the two modes (escaped quote vs closing quote). Anything else agrees in both.
            let run = b[j..].iter().take_while(|&&x| x == b'\\').count();
            if run % 2 == 1 && b.get(j + run) == Some(&quote) {
                return None;
            }
            j += run;
            continue;
        }
        if c == quote {
            if b.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return Some(j + 1);
        }
        j = char_end(sql, j);
    }
    None
}

fn line_comment(b: &[u8], start: usize, out: &mut Vec<(usize, usize, Hidden)>) -> usize {
    let end = b[start..]
        .iter()
        .position(|&c| c == b'\n')
        .map_or(b.len(), |off| start + off);
    out.push((start, end, Hidden::Comment));
    end
}

/// Everything from `start` on is a value: the scan stops here.
fn hide_rest(
    out: Vec<(usize, usize, Hidden)>,
    start: usize,
    len: usize,
) -> Vec<(usize, usize, Hidden)> {
    hide_rest_as(out, start, len, Hidden::Literal)
}

fn hide_rest_as(
    mut out: Vec<(usize, usize, Hidden)>,
    start: usize,
    len: usize,
    kind: Hidden,
) -> Vec<(usize, usize, Hidden)> {
    out.push((start, len, kind));
    out
}

/// A one-letter literal prefix (`E'…'`, `X'…'`, `B'…'`, `N'…'`) belongs to the literal, so it is
/// hidden with it — `X'deadbeef'` must not render as `X?` leaving a reader to wonder, and on
/// PostgreSQL the `E` decides how the literal is read.
fn prefix_start(sql: &str, quote: usize) -> usize {
    if is_prefixed_by(sql, quote, b"EeXxBbNn") {
        quote - 1
    } else {
        quote
    }
}

/// True when the byte before `quote` is one of `letters` AND stands alone (`TABLE'` is not an
/// `E'…'` string: its `E` ends an identifier).
fn is_prefixed_by(sql: &str, quote: usize, letters: &[u8]) -> bool {
    let b = sql.as_bytes();
    if quote == 0 || !letters.contains(&b[quote - 1]) {
        return false;
    }
    quote == 1 || !(b[quote - 2].is_ascii_alphanumeric() || b[quote - 2] == b'_')
}

fn char_end(sql: &str, start: usize) -> usize {
    let mut end = start + 1;
    while end < sql.len() && !sql.is_char_boundary(end) {
        end += 1;
    }
    end
}
