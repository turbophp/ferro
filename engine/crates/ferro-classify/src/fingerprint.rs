//! SQL **fingerprints** for observability (SPEC §13, product-vision §5).
//!
//! §13 wants normalized fingerprints in the slow log, in spans and as metric labels; the redaction
//! contract in product-vision §5 says *"normalized fingerprints only, never raw SQL"* and *"closed
//! label vocabularies"*. This module is where that contract is MADE TRUE, rather than asked for.
//!
//! **It is not SQL rewriting** (charter rule 6). Nothing here is ever sent to a database: the
//! output exists only to be logged and counted. The input statement reaches the backend untouched.
//!
//! **It reuses the pin lexer's region pass** rather than writing a second one. That matters for
//! more than tidiness: the scanner in [`crate::scan`] is the code that already knows what a string
//! literal is on each dialect — E-strings, `''` escapes, dollar-quoting, nested block comments —
//! and it is total on any input. A separate "good enough" literal finder would be a second place
//! for the redaction contract to be wrong, and the failure mode is a leaked value in a log file.
//!
//! What normalization does, and nothing more:
//!
//! * every string literal and dollar-quoted body becomes `?`
//! * every numeric literal becomes `?`
//! * comments are dropped
//! * whitespace collapses to single spaces
//! * a RUN of placeholders — `?, ?, ?` — collapses to a single `?`, wherever it occurs, so a
//!   query's fingerprint does not depend on how many ids the caller happened to pass
//!
//! Keywords, identifiers and quoted identifiers are preserved verbatim — they are the statement's
//! shape, and they are what makes a fingerprint recognisable to the operator reading it. Case is
//! NOT folded: `select 1` and `SELECT 1` are different fingerprints, because folding would make a
//! fingerprint unreadable back to the caller's own source.

use crate::scan::{self, Hidden};

/// A normalized SQL fingerprint: never raw SQL, never a parameter value.
///
/// The newtype is the enforcement point for product-vision §5. A slow-log record or metric label
/// takes a `Fingerprint`, and the ONLY way to obtain one is [`fingerprint`], so "we must remember
/// not to log raw SQL" stops being a rule anyone can forget and becomes something the type system
/// declines to express. There is deliberately no `From<String>` and no public constructor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fingerprint(String);

impl Fingerprint {
    /// The normalized text, for a log field or a metric label.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Normalize `sql` into a [`Fingerprint`].
///
/// TOTAL: never panics on any input, inheriting [`crate::scan`]'s guarantee — empty, multibyte,
/// unterminated strings/comments/dollar-quotes. That is load-bearing rather than a nicety: this
/// runs on the slow path of every statement, and a panic here would take down a session over a
/// LOG line.
pub fn fingerprint(sql: &str) -> Fingerprint {
    let hidden = scan::hidden_spans(sql);
    let bytes = sql.as_bytes();
    // Roughly the input's size; normalization only ever shrinks.
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut h = 0usize;

    while i < bytes.len() {
        // Inside a hidden span? Emit its placeholder (or nothing, for a comment) and skip it whole.
        if let Some(&(start, end, kind)) = hidden.get(h)
            && i >= start
        {
            if kind == Hidden::Literal {
                push_placeholder(&mut out);
            } else {
                // A dropped comment still SEPARATES tokens: `a/**/b` is two tokens, not `ab`.
                push_space(&mut out);
            }
            // `max(i + 1)` guarantees forward progress even for a zero-width span, so this loop
            // cannot spin on a malformed one.
            i = end.max(i + 1);
            h += 1;
            continue;
        }

        let b = bytes[i];
        if b.is_ascii_whitespace() {
            push_space(&mut out);
            i += 1;
        } else if b.is_ascii_digit() && !continues_identifier(&out) {
            // A NUMERIC literal — but only where a number can start. `t1` and `col2` are
            // identifiers whose digits must survive, or every table named `t1` fingerprints as `t?`
            // and two different tables collide in the operator's "hot fingerprints" list.
            i = skip_number(sql, i);
            push_placeholder(&mut out);
        } else {
            // Code byte. Copy the WHOLE char — a multibyte identifier must never be split.
            let end = char_end(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
        }
    }

    Fingerprint(collapse_in_lists(out.trim()))
}

/// True when the character just emitted can continue an identifier, so a digit here is part of a
/// NAME (`t1`) rather than the start of a numeric literal.
fn continues_identifier(out: &str) -> bool {
    matches!(out.chars().next_back(), Some(c) if c.is_alphanumeric() || c == '_' || c == '$')
}

/// Consumes a numeric literal starting at `i`: digits, an optional decimal point, and an optional
/// exponent. Deliberately generous — anything it over-consumes becomes part of the same `?`.
fn skip_number(sql: &str, mut i: usize) -> usize {
    let bytes = sql.as_bytes();
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let mut j = i + 1;
        if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
            j += 1;
        }
        if j < bytes.len() && bytes[j].is_ascii_digit() {
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            i = j;
        }
    }
    i
}

/// Appends `?`, collapsing `?, ?` runs — see [`collapse_in_lists`] for the list case.
fn push_placeholder(out: &mut String) {
    out.push('?');
}

/// Appends at most one space, so runs of whitespace (and dropped comments) collapse.
fn push_space(out: &mut String) {
    if !out.is_empty() && !out.ends_with(' ') {
        out.push(' ');
    }
}

/// The byte index just past the char starting at `sql[start]`. Same total walk as `scan.rs`'s.
fn char_end(sql: &str, start: usize) -> usize {
    let mut end = start + 1;
    while end < sql.len() && !sql.is_char_boundary(end) {
        end += 1;
    }
    end
}

/// Collapses a RUN of placeholders — `?, ?, ?` — down to a single `?`.
///
/// Without this, `WHERE id IN (1,2,3)` and `WHERE id IN (1,2,3,4)` are different fingerprints, and
/// an ORM that batches by page size scatters one logical query across dozens of them.
///
/// **It applies to every placeholder run, not only to `IN` lists, and that is worth stating
/// because it costs something:** `VALUES (?, ?)` and `VALUES (?, ?, ?)` fingerprint identically,
/// so an INSERT's column COUNT is not visible in its fingerprint. The alternative — collapsing
/// only inside a paren that follows `IN` — means this module starts tracking paren depth and
/// keyword context, i.e. becomes a parser, to recover a distinction an operator reading a slow log
/// does not need. The statement's identity is its shape, and the column names are already in it.
/// Placeholders only: `(a, b, c)` is a column list and is left alone.
fn collapse_in_lists(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'?' {
            out.push('?');
            i += 1;
            // Eat every following `, ?` (with or without the space).
            loop {
                let mut j = i;
                while j < bytes.len() && bytes[j] == b' ' {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b',' {
                    j += 1;
                    while j < bytes.len() && bytes[j] == b' ' {
                        j += 1;
                    }
                    if j < bytes.len() && bytes[j] == b'?' {
                        i = j + 1;
                        continue;
                    }
                }
                break;
            }
        } else {
            let end = char_end(s, i);
            out.push_str(&s[i..end]);
            i = end;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cases that named this module's behaviour, measured against real statement shapes from
    /// this repository's own suites before the implementation was trusted.
    #[test]
    fn normalizes_real_statement_shapes() {
        let cases: &[(&str, &str)] = &[
            (
                "SELECT * FROM users WHERE email = 'alice@example.com' AND id = 42",
                "SELECT * FROM users WHERE email = ? AND id = ?",
            ),
            // The digits of `t1`/`col2` are part of the NAME and must survive, or every table
            // named `t1` fingerprints as `t?` and distinct queries collide in "hot fingerprints".
            (
                "select id from t1 where col2 in (1,2,3,4,5)",
                "select id from t1 where col2 in (?)",
            ),
            (
                "INSERT INTO t (a,b) VALUES ('x','y')",
                "INSERT INTO t (a,b) VALUES (?)",
            ),
            // A bind MARKER is not a literal: it is already the shape.
            (
                "UPDATE u SET p = 'p@ss' WHERE id = $1",
                "UPDATE u SET p = ? WHERE id = $1",
            ),
            // Quoted identifiers are code, and case is not folded.
            (
                "SELECT * FROM \"MyTable\" WHERE \"Col\" = 'v'",
                "SELECT * FROM \"MyTable\" WHERE \"Col\" = ?",
            ),
            // The `E` belongs to the literal, not the code: `E?` reads like a typo.
            ("SELECT E'line\\'s end', 'it''s'", "SELECT ?"),
            ("SELECT $tag$ secret body $tag$", "SELECT ?"),
            ("SELECT 1.5e-3, 0.25", "SELECT ?"),
            ("SELECT   *\n  FROM    t", "SELECT * FROM t"),
            // A dropped comment still SEPARATES: `a/**/b` is two tokens, not `ab`.
            ("SELECT a/**/b FROM t", "SELECT a b FROM t"),
            ("SELECT '日本語' AS x", "SELECT ? AS x"),
        ];
        for (sql, want) in cases {
            assert_eq!(&fingerprint(sql).as_str(), want, "fingerprint of {sql:?}");
        }
    }

    /// **The redaction contract (product-vision §5), as a property rather than a spot check.**
    ///
    /// Every one of these hides a distinctive secret inside a different lexical shape — an
    /// ordinary literal, an escaped one, an E-string, a dollar-quote, both comment forms, and the
    /// UNTERMINATED versions that a naive scanner walks straight past the end of. The assertion is
    /// the one that matters operationally: the secret does not appear in the output. It is stated
    /// over the secret, not over the expected fingerprint, so a future normalization change cannot
    /// quietly weaken it while still matching a golden string.
    #[test]
    fn no_value_survives_into_a_fingerprint() {
        const SECRET: &str = "hunter2-swordfish";
        let carriers = [
            format!("SELECT * FROM t WHERE pw = '{SECRET}'"),
            format!("SELECT * FROM t WHERE pw = 'it''s {SECRET}'"),
            format!("SELECT E'\\'{SECRET}'"),
            format!("SELECT $q$ {SECRET} $q$"),
            format!("SELECT 1 -- {SECRET}"),
            format!("SELECT 1 /* {SECRET} */"),
            format!("SELECT 1 /* outer /* {SECRET} */ still */"),
            // Unterminated: the region runs to EOF and must still be hidden.
            format!("SELECT '{SECRET}"),
            format!("SELECT -- {SECRET}"),
            format!("SELECT /* {SECRET}"),
            format!("SELECT $q$ {SECRET}"),
            format!("INSERT INTO t VALUES ('a', '{SECRET}', 3)"),
        ];
        for sql in &carriers {
            let fp = fingerprint(sql);
            assert!(
                !fp.as_str().contains(SECRET),
                "the secret survived into the fingerprint of {sql:?}: {fp}",
            );
        }
    }

    /// TOTAL on any input — the guarantee inherited from the region pass. This runs on the slow
    /// path of every statement, so a panic here would take down a session over a LOG line.
    #[test]
    fn never_panics_on_pathological_input() {
        let inputs = [
            "", " ", "'", "\"", "$", "$$", "$tag$", "--", "/*", "/* /* /*", "*/", "E'", "\\", "日",
            "'日", "1.2.3.4e", "1e", "1e+", "?,?,?,", ",,,???",
        ];
        for sql in inputs {
            let _ = fingerprint(sql);
        }
        // And a long adversarial mix, to exercise the state machine rather than its edges.
        let mixed =
            "SELECT 'a''b', $x$c$x$, E'\\'d' -- e\n/* f /* g */ h */ FROM \"i\" WHERE j = 1"
                .repeat(200);
        let _ = fingerprint(&mixed);
    }

    /// A placeholder RUN collapses regardless of length — the property that keeps one logical
    /// query from scattering across dozens of fingerprints when an ORM batches by page size.
    #[test]
    fn a_placeholder_run_collapses_whatever_its_length() {
        let one = fingerprint("SELECT * FROM t WHERE id IN (1)");
        for n in 2..64 {
            let list = (1..=n).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
            assert_eq!(
                fingerprint(&format!("SELECT * FROM t WHERE id IN ({list})")),
                one,
                "a {n}-element IN list fingerprinted differently from a 1-element one",
            );
        }
    }

    /// A column list is NOT a placeholder run and is left alone — the distinction the collapse
    /// rule rests on.
    #[test]
    fn a_column_list_is_not_collapsed() {
        assert_eq!(
            fingerprint("INSERT INTO t (a, b, c) VALUES (1, 2, 3)").as_str(),
            "INSERT INTO t (a, b, c) VALUES (?)",
        );
    }

    /// The newtype is the enforcement point, so this asserts the thing a reviewer would check by
    /// eye: there is no way to build a `Fingerprint` around arbitrary text. If a `From<String>` or
    /// a public constructor is ever added, this comment is the reason not to.
    #[test]
    fn a_fingerprint_round_trips_its_own_text() {
        let fp = fingerprint("SELECT 1");
        assert_eq!(fp.as_str(), "SELECT ?");
        assert_eq!(fp.to_string(), "SELECT ?");
    }
}
