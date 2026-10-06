//! The COPY shape check (M3-D4, SPEC §6.1): is this statement ONE PostgreSQL `COPY … FROM STDIN`
//! (or `COPY … TO STDOUT`)?
//!
//! **Why the engine looks at all — protocol shape, not inference.** SQL/`COPY_IN` and SQL/`COPY_OUT`
//! drive PostgreSQL's COPY sub-protocol, and the backend's own error does NOT suffice when the
//! statement is something else, because the driver has already sent `Bind`/`Execute`/`Sync` by the
//! time it learns what the server is doing:
//!
//! - a non-COPY statement on the COPY_IN path EXECUTES and its implicit transaction COMMITS at the
//!   `Sync`, and only THEN does the driver report "unexpected message" — a write applied while the
//!   request reports failure;
//! - a `COPY … FROM STDIN` on the COPY_OUT path puts the server into copy-in mode, where it ignores
//!   `Sync` and waits for data that never comes — a wedged pooled connection;
//! - a `COPY … FROM 'file'` / `FROM PROGRAM` reads a server-side file or runs a program instead of
//!   the client's bytes.
//!
//! So a method whose wire contract IS the COPY sub-protocol in one direction refuses a statement
//! that cannot speak it. This is the same kind of check as `ferro-pool`'s bare-tx-control guard: it
//! reads only the statement's leading keyword and the top-level `FROM`/`TO` target, never decides
//! read-vs-write (charter rule 6), and never rewrites anything — the statement goes to the server
//! byte for byte. Everything else about the COPY (table, columns, options, `WHERE`) is the server's
//! to accept or refuse.
//!
//! **Fails closed.** Anything this does not positively recognise — an unterminated literal, a
//! second statement, a target that is not exactly `STDIN`/`STDOUT` — is "not a COPY of that
//! direction", and the request is refused before anything reaches the backend.

use crate::scan;

/// Which way a COPY moves data, as the statement itself says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyDirection {
    /// `COPY … FROM STDIN`: the client sends rows.
    In,
    /// `COPY … TO STDOUT`: the server sends rows.
    Out,
}

#[derive(Debug, PartialEq, Eq)]
enum Tok {
    /// An unquoted word, upper-cased.
    Word(String),
    /// A `"quoted identifier"`: never a keyword, whatever it spells.
    Quoted,
    Open,
    Close,
    Semicolon,
    Other,
}

/// The direction of `sql` if it is exactly one PostgreSQL `COPY … FROM STDIN` or
/// `COPY … TO STDOUT` statement (a trailing `;` allowed), else `None`.
pub fn copy_direction(sql: &str) -> Option<CopyDirection> {
    let toks = tokenize(&scan::masked_code(sql))?;
    let mut it = toks.iter().enumerate();
    match it.next() {
        Some((_, Tok::Word(w))) if w == "COPY" => {}
        _ => return None,
    }
    // The first TOP-LEVEL `FROM`/`TO`: a `(query)` or a `(column, …)` list sits at depth > 0, and an
    // unquoted `from`/`to` cannot be a table name (both are reserved words in PostgreSQL).
    let mut depth = 0i32;
    let mut found: Option<(usize, CopyDirection)> = None;
    for (i, t) in it {
        match t {
            Tok::Open => depth += 1,
            Tok::Close => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            Tok::Semicolon => return None,
            Tok::Word(w) if depth == 0 && w == "FROM" => {
                found = Some((i, CopyDirection::In));
                break;
            }
            Tok::Word(w) if depth == 0 && w == "TO" => {
                found = Some((i, CopyDirection::Out));
                break;
            }
            _ => {}
        }
    }
    let (at, dir) = found?;
    let want = match dir {
        CopyDirection::In => "STDIN",
        CopyDirection::Out => "STDOUT",
    };
    match toks.get(at + 1) {
        Some(Tok::Word(w)) if w == want => {}
        _ => return None,
    }
    // One statement: nothing but a trailing `;` after the first top-level `;`.
    let rest = &toks[at + 2..];
    if let Some(semi) = rest.iter().position(|t| *t == Tok::Semicolon)
        && rest[semi + 1..].iter().any(|t| *t != Tok::Semicolon)
    {
        return None;
    }
    Some(dir)
}

/// Does any top-level statement of `sql` speak the COPY STDIN/STDOUT sub-protocol? For the
/// statement paths that CANNOT carry it (SQL/`EXEC`): such a statement, sent there, leaves the
/// pooled connection in copy mode with a driver that expected rows — measured before M3-D4 as a
/// killed connection and an `Indeterminate` for a statement that could not have applied.
pub fn speaks_copy_stdio(sql: &str) -> bool {
    scan::split_top_level_statements(sql)
        .into_iter()
        .any(|s| copy_direction(s).is_some())
}

/// Tokens of the masked code. `None` for an unterminated quoted identifier (fail closed).
fn tokenize(masked: &str) -> Option<Vec<Tok>> {
    let b = masked.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'"' {
            // `""` inside is an escaped quote; the content is an identifier, never a keyword.
            i += 1;
            loop {
                match b.get(i) {
                    None => return None,
                    Some(b'"') if b.get(i + 1) == Some(&b'"') => i += 2,
                    Some(b'"') => {
                        i += 1;
                        break;
                    }
                    Some(_) => i += 1,
                }
            }
            out.push(Tok::Quoted);
        } else if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Tok::Word(masked[start..i].to_ascii_uppercase()));
        } else {
            out.push(match c {
                b'(' => Tok::Open,
                b')' => Tok::Close,
                b';' => Tok::Semicolon,
                _ => Tok::Other,
            });
            i += 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use CopyDirection::{In, Out};

    #[test]
    fn recognises_both_directions_in_their_ordinary_shapes() {
        for (sql, want) in [
            ("COPY t FROM STDIN", In),
            ("copy t from stdin", In),
            (
                "COPY public.items (id, name) FROM STDIN WITH (FORMAT csv, HEADER true)",
                In,
            ),
            ("COPY t FROM STDIN;", In),
            ("  -- load\n COPY t FROM STDIN", In),
            ("COPY BINARY t FROM STDIN", In),
            ("COPY t (a) FROM STDIN WHERE a > 0", In),
            ("COPY \"from\" FROM STDIN", In),
            ("COPY \"weird\"\"name\" (\"to\") FROM STDIN", In),
            ("COPY t TO STDOUT", Out),
            (
                "COPY (SELECT a FROM t WHERE b = 'x TO STDOUT') TO STDOUT",
                Out,
            ),
            (
                "COPY (DELETE FROM t RETURNING *) TO STDOUT WITH (FORMAT binary)",
                Out,
            ),
            ("COPY t TO STDOUT WITH (DELIMITER E'\\t')", Out),
            ("COPY t TO STDOUT /* trailing */ ;", Out),
        ] {
            assert_eq!(copy_direction(sql), Some(want), "{sql}");
        }
    }

    #[test]
    fn refuses_everything_that_cannot_speak_the_client_sub_protocol() {
        for sql in [
            "",
            "SELECT 1",
            "DELETE FROM t",
            "INSERT INTO t SELECT * FROM s",
            "COPY t FROM '/etc/passwd'",
            "COPY t FROM PROGRAM 'id'",
            "COPY t TO '/tmp/x'",
            "COPY t TO PROGRAM 'cat'",
            "COPY t FROM STDOUT",
            "COPY t TO STDIN",
            "COPY t FROM STDIN; DELETE FROM t",
            "COPY t FROM STDIN; COPY t FROM STDIN",
            "COPY t",
            "COPY t FROM",
            "COPY (SELECT 1 FROM STDIN",
            "COPY t FROM \"STDIN\"",
            "COPY t FROM 'STDIN'",
            "/* COPY t FROM STDIN */ SELECT 1",
            "WITH x AS (SELECT 1) COPY t FROM STDIN",
            "COPY t) FROM STDIN",
            "COPY \"t FROM STDIN",
            "COPY t FROMSTDIN",
            "EXPLAIN COPY t FROM STDIN",
        ] {
            assert_eq!(copy_direction(sql), None, "{sql:?}");
        }
    }

    #[test]
    fn a_from_inside_a_query_or_a_literal_is_not_the_target() {
        // The query's own FROM is at depth 1; the literal is masked; the target decides.
        assert_eq!(
            copy_direction("COPY (SELECT * FROM STDIN) TO STDOUT"),
            Some(Out)
        );
        assert_eq!(copy_direction("COPY (SELECT 'FROM STDIN') TO '/x'"), None);
    }

    #[test]
    fn speaks_copy_stdio_finds_a_copy_in_any_top_level_statement_and_nowhere_else() {
        assert!(speaks_copy_stdio("COPY t FROM STDIN"));
        assert!(speaks_copy_stdio("SELECT 1; COPY t TO STDOUT;"));
        assert!(!speaks_copy_stdio("SELECT 'COPY t FROM STDIN'"));
        assert!(!speaks_copy_stdio("-- COPY t FROM STDIN\nSELECT 1"));
        assert!(!speaks_copy_stdio("COPY t FROM '/x.csv'"));
        assert!(!speaks_copy_stdio("SELECT 1"));
    }

    #[test]
    fn total_on_arbitrary_bytes() {
        for sql in [
            "\"",
            "((((",
            "))))",
            "COPY \u{1F600} FROM STDIN",
            "COPY é FROM STDIN",
            "$$",
            "COPY t FROM STDIN $$ x",
        ] {
            let _ = copy_direction(sql);
        }
        assert_eq!(copy_direction("COPY é FROM STDIN"), Some(In));
    }
}
