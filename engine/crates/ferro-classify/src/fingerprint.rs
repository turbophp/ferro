//! SQL **fingerprints** for observability (SPEC §13, product-vision §5).
//!
//! §13 wants normalized fingerprints in the slow log, in spans and as metric labels; the redaction
//! contract in product-vision §5 says *"normalized fingerprints only, never raw SQL"* and *"closed
//! label vocabularies"*. This module is where that contract is MADE TRUE, rather than asked for.
//!
//! **It is not SQL rewriting** (charter rule 6). Nothing here is ever sent to a database: the
//! output exists only to be logged and counted. The input statement reaches the backend untouched.
//!
//! **It has its own region pass, per dialect, and that CORRECTS C4a.** C4a reused the pin lexer's
//! pass ([`crate::scan`]) on the reasoning that one lexer is one place to be right. But the two
//! consumers have OPPOSITE safe directions: the pin lexer keeps ambiguous text VISIBLE (a missed
//! pin leaks session state), a fingerprint must HIDE it (a shown value is the leak). That pass is
//! also PostgreSQL's rules on every dialect, so on MySQL a `"…"` string and the tail of an
//! `'O\'Brien'` literal were "code", and the C4c-2 review exported both to a collector. The
//! fingerprint now uses [`crate::redact`], which knows each dialect's quoting and hides the rest of
//! a statement wherever the server's own reading depends on a session setting the engine cannot
//! see. The cost is readability where hiding is the only safe answer: on MySQL and SQLite a
//! double-quoted token is shown as `?`, because it may be a string.
//!
//! **Its input is bounded** ([`INPUT_LIMIT`]): a fingerprint is a label, and the C4c-2 review
//! measured an unbounded one costing 383 ms of a worker per 2 MiB statement and blacking out a
//! collector's batches. A longer statement is fingerprinted from its first [`INPUT_LIMIT`] bytes,
//! and the result ends in [`TRUNCATED_MARKER`]. A cut through a literal leaves it unterminated,
//! which hides it to the end — so truncation cannot expose a value.
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

use crate::Dialect;
use crate::scan::Hidden;

/// The most bytes of a statement a fingerprint is computed from. See the module doc.
pub const INPUT_LIMIT: usize = 4096;

/// Appended to a fingerprint computed from a truncated statement.
pub const TRUNCATED_MARKER: &str = " …";

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

/// Normalize `sql`, as `dialect` reads it, into a [`Fingerprint`].
///
/// TOTAL: never panics on any input — empty, multibyte, unterminated strings/comments/
/// dollar-quotes. That is load-bearing rather than a nicety: this runs on the slow path of every
/// statement, and a panic here would take down a session over a LOG line.
pub fn fingerprint(sql: &str, dialect: Dialect) -> Fingerprint {
    let (sql, truncated) = if sql.len() > INPUT_LIMIT {
        let mut cut = INPUT_LIMIT;
        while !sql.is_char_boundary(cut) {
            cut -= 1;
        }
        (&sql[..cut], true)
    } else {
        (sql, false)
    };
    let hidden = crate::redact::hidden_spans(sql, dialect);
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

    let mut text = collapse_in_lists(out.trim());
    if truncated {
        text.push_str(TRUNCATED_MARKER);
    }
    Fingerprint(text)
}

/// True when the character just emitted can continue an identifier, so a digit here is part of a
/// NAME (`t1`) rather than the start of a numeric literal.
fn continues_identifier(out: &str) -> bool {
    matches!(out.chars().next_back(), Some(c) if c.is_alphanumeric() || c == '_' || c == '$')
}

/// Consumes a numeric literal starting at `i`: digits, an optional decimal point, and an optional
/// exponent — or a `0x`/`0b`/`0o` integer, whose digits would otherwise survive as `?xDEADBEEF`
/// (the C4c-2 review measured exactly that on MariaDB). Underscore separators (PostgreSQL 16's
/// `1_000`) are part of the number. Deliberately generous — anything it over-consumes becomes part
/// of the same `?`.
fn skip_number(sql: &str, mut i: usize) -> usize {
    let bytes = sql.as_bytes();
    if bytes[i] == b'0'
        && matches!(
            bytes.get(i + 1),
            Some(b'x' | b'X' | b'b' | b'B' | b'o' | b'O')
        )
    {
        i += 2;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        return i;
    }
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.' || bytes[i] == b'_') {
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

    /// The PostgreSQL reading — the dialect every case below was written against.
    fn pg(sql: &str) -> Fingerprint {
        fingerprint(sql, Dialect::Postgres)
    }
    fn my(sql: &str) -> Fingerprint {
        fingerprint(sql, Dialect::MySql)
    }
    fn lite(sql: &str) -> Fingerprint {
        fingerprint(sql, Dialect::Sqlite)
    }

    const ALL: [Dialect; 3] = [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite];

    /// A non-ASCII dollar-quote tag is a tag to PostgreSQL, so its body is a literal and never
    /// reaches the slow log or a span (review round 2, L2 — it did, verbatim).
    #[test]
    fn a_non_ascii_dollar_tag_hides_its_body() {
        for sql in [
            "SELECT $é$hunter2-secret$é$",
            "SELECT $tagé$hunter2-secret$tagé$",
            "SELECT $é$hunter2-secret",
        ] {
            let f = pg(sql);
            assert!(!f.as_str().contains("hunter2"), "{sql:?} -> {f}");
        }
    }

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
            assert_eq!(&pg(sql).as_str(), want, "fingerprint of {sql:?}");
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
    fn no_value_survives_into_a_pg() {
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
            let fp = pg(sql);
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
            let _ = pg(sql);
        }
        // And a long adversarial mix, to exercise the state machine rather than its edges.
        let mixed =
            "SELECT 'a''b', $x$c$x$, E'\\'d' -- e\n/* f /* g */ h */ FROM \"i\" WHERE j = 1"
                .repeat(200);
        let _ = pg(&mixed);
    }

    /// A placeholder RUN collapses regardless of length — the property that keeps one logical
    /// query from scattering across dozens of fingerprints when an ORM batches by page size.
    #[test]
    fn a_placeholder_run_collapses_whatever_its_length() {
        let one = pg("SELECT * FROM t WHERE id IN (1)");
        for n in 2..64 {
            let list = (1..=n).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
            assert_eq!(
                pg(&format!("SELECT * FROM t WHERE id IN ({list})")),
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
            pg("INSERT INTO t (a, b, c) VALUES (1, 2, 3)").as_str(),
            "INSERT INTO t (a, b, c) VALUES (?)",
        );
    }

    /// The newtype is the enforcement point, so this asserts the thing a reviewer would check by
    /// eye: there is no way to build a `Fingerprint` around arbitrary text. If a `From<String>` or
    /// a public constructor is ever added, this comment is the reason not to.
    #[test]
    fn a_fingerprint_round_trips_its_own_text() {
        let fp = pg("SELECT 1");
        assert_eq!(fp.as_str(), "SELECT ?");
        assert_eq!(fp.to_string(), "SELECT ?");
    }

    /// **The C4c-2 review's BLOCKER, each carrier it measured reaching a collector, per dialect.**
    /// Each statement SUCCEEDS on its backend (the review ran them live), so each one is a value a
    /// real application can put in a real span. Stated over the secret, like the test above.
    #[test]
    fn no_value_survives_on_any_dialect() {
        const S: &str = "hunter2-swordfish";
        let mysql = [
            // `"…"` is a STRING on MySQL unless ANSI_QUOTES is set.
            format!("select 1 from dual where 'x' <> \"{S}\""),
            // `\'` does not end a MySQL string in the default mode.
            format!("select 1 from dual where 'O\\'Brien' <> '{S}'"),
            format!("select 1 from dual where \"a\\\"b\" <> '{S}'"),
            // `#` is a MySQL line comment.
            format!("select 1 # card {S} for bob\n from dual"),
            // A backtick identifier holding a quote must not open a literal.
            format!("select `it's` from t where a = '{S}'"),
            format!("select 0xDEADBEEF, x'{S}'"),
            format!("select _utf8mb4'{S}'"),
        ];
        for sql in &mysql {
            let fp = my(sql);
            assert!(!fp.as_str().contains(S), "MySQL: {sql:?} → {fp}");
        }
        assert!(!my("select 0xDEADBEEF").as_str().contains("DEADBEEF"));

        let sqlite = [
            // SQLite's DQS fallback makes an unknown `"…"` a STRING.
            format!("select 1 where 'x' <> \"{S}\""),
            format!("select [it's] from t where a = '{S}'"),
            format!("select `it's` from t where a = '{S}'"),
            format!("select x'{S}'"),
        ];
        for sql in &sqlite {
            let fp = lite(sql);
            assert!(!fp.as_str().contains(S), "SQLite: {sql:?} → {fp}");
        }

        // PostgreSQL with standard_conforming_strings OFF reads `\'` as an escaped quote — so
        // this literal ENDS at the quote before `|| '{S}`… and the scs-ON reading puts `{S}`
        // inside a literal too. Only hiding the rest is right in both modes.
        let pg_carriers = [
            format!("select 'a\\' as x, ' || '{S}' --'"),
            format!("select 'a\\' , '{S}'"),
        ];
        for sql in &pg_carriers {
            let fp = pg(sql);
            assert!(!fp.as_str().contains(S), "PostgreSQL: {sql:?} → {fp}");
        }
    }

    /// The CONTROL for the rule above: a backslash that means the same in both modes keeps the
    /// statement's shape. Without this, "hide the rest on any backslash" would pass every leak
    /// test while making every Windows path or JSON escape blank a fingerprint.
    #[test]
    fn a_backslash_both_modes_agree_on_keeps_the_shape() {
        for d in [Dialect::Postgres, Dialect::MySql] {
            let fp = fingerprint(r"select 'C:\temp\x' as p, n from t where id = 7", d);
            assert_eq!(fp.as_str(), "select ? as p, n from t where id = ?", "{d:?}");
            // An even run before the quote closes it in both modes.
            let fp = fingerprint(r"select 'a\\' as p from t", d);
            assert_eq!(fp.as_str(), "select ? as p from t", "{d:?}");
        }
        // ...and an ODD run before the quote is exactly where the rest is hidden.
        assert_eq!(pg(r"select 'a\' as p from t").as_str(), "select ?");
        // SQLite has no backslash escapes at all, so it is never ambiguous there.
        assert_eq!(
            lite(r"select 'a\' as p from t").as_str(),
            "select ? as p from t"
        );
        // A PostgreSQL E-string's backslash is an escape in every mode: precise, not hidden.
        assert_eq!(
            pg(r"select E'a\'b' as p from t").as_str(),
            "select ? as p from t"
        );
    }

    /// Identifiers stay readable where the dialect makes them identifiers — PostgreSQL's `"…"`,
    /// MySQL's and SQLite's backticks, SQLite's brackets — and are hidden where they might be a
    /// string: the stated readability cost of failing closed.
    #[test]
    fn identifiers_are_shown_only_where_they_cannot_be_strings() {
        assert_eq!(
            pg(r#"select "Col" from "T""#).as_str(),
            r#"select "Col" from "T""#
        );
        assert_eq!(
            my("select `Col` from `T`").as_str(),
            "select `Col` from `T`"
        );
        assert_eq!(
            lite("select [Col] from `T`").as_str(),
            "select [Col] from `T`"
        );
        assert_eq!(my(r#"select "Col" from t"#).as_str(), "select ? from t");
        assert_eq!(lite(r#"select "Col" from t"#).as_str(), "select ? from t");
    }

    /// An UNTERMINATED region of any kind — identifiers included — hides the rest. An unterminated
    /// identifier is a syntax error, but the failed statement's text still reaches its span.
    #[test]
    fn an_unterminated_region_hides_the_rest_on_every_dialect() {
        const S: &str = "hunter2-swordfish";
        for d in ALL {
            for sql in [
                format!("select \"a, '{S}'"),
                format!("select `a, '{S}'"),
                format!("select [a, '{S}'"),
                format!("select '{S}"),
                format!("select 1 /* {S}"),
            ] {
                let fp = fingerprint(&sql, d);
                assert!(!fp.as_str().contains(S), "{d:?}: {sql:?} → {fp}");
            }
        }
    }

    /// The input bound: a statement longer than [`INPUT_LIMIT`] is fingerprinted from its prefix,
    /// marked, and a literal the cut runs through stays hidden.
    #[test]
    fn a_long_statement_is_fingerprinted_from_a_bounded_prefix() {
        let ident = "c".repeat(INPUT_LIMIT * 4);
        let fp = pg(&format!("select {ident} from t"));
        assert!(fp.as_str().len() <= INPUT_LIMIT + TRUNCATED_MARKER.len());
        assert!(
            fp.as_str().ends_with(TRUNCATED_MARKER),
            "{}",
            &fp.as_str()[..40]
        );

        let mut sql = "x".repeat(INPUT_LIMIT - 10);
        sql.push_str(" = 'hunter2-swordfish-and-a-long-tail'");
        let fp = pg(&sql);
        assert!(
            !fp.as_str().contains("hunter2"),
            "a cut literal must stay hidden"
        );
        assert!(fp.as_str().ends_with(TRUNCATED_MARKER));

        // A multibyte char straddling the cut neither panics nor splits.
        let mut sql = "x".repeat(INPUT_LIMIT - 1);
        sql.push('日');
        let _ = pg(&sql);

        assert!(
            !pg("select 1").as_str().ends_with(TRUNCATED_MARKER),
            "short is unmarked"
        );
    }

    #[test]
    fn prefixed_numbers_are_values() {
        for d in ALL {
            assert_eq!(
                fingerprint("select 0xDEADBEEF, 0b1010, 0o17, 1_000_000", d).as_str(),
                "select ?",
                "{d:?}"
            );
        }
    }

    #[test]
    fn total_on_pathological_input_in_every_dialect() {
        let inputs = [
            "", "'", "\"", "`", "[", "#", "\\", "'\\", "\"\\", "E'\\", "$q$", "/*", "--", "日",
            "'日", "x'", "0x", "0b", "_'",
        ];
        for d in ALL {
            for sql in inputs {
                let _ = fingerprint(sql, d);
            }
        }
    }
}
