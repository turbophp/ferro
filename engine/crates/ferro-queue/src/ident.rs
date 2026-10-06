//! A store's table identifier (SPEC §24.3): `[schema.]identifier`, each part
//! `[A-Za-z_][A-Za-z0-9_]*`, validated at configuration load.
//!
//! Two decisions of M7-G1a, recorded in SPEC §24.3:
//!
//! - **Each part is at most 63 bytes.** PostgreSQL silently TRUNCATES a longer identifier to
//!   `NAMEDATALEN - 1` (63) bytes, so `TABLE=<64 bytes>` would address a different table than the
//!   one named, and shape verification would read the truncated name's columns. MySQL's limit is 64;
//!   the stricter bound is taken for every dialect so a store's configuration means the same table on
//!   either family.
//! - **The identifier is used VERBATIM and QUOTED** — `"schema"."table"` on PostgreSQL, `` `t` `` on
//!   MySQL — exactly as Laravel's schema builder quotes the tables it creates, so `TABLE=Jobs` means
//!   the case-sensitive table `Jobs` a migration would have created, never PostgreSQL's case-folded
//!   `jobs`. The character rule is what makes quoting injection-free: no part can contain a quote.

use crate::Dialect;

/// The longest identifier part accepted (PostgreSQL's `NAMEDATALEN - 1`).
pub const MAX_PART_LEN: usize = 63;

/// A validated `[schema.]table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub schema: Option<String>,
    pub table: String,
}

/// Why a table identifier was refused. Log-safe: it names the rule, never the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentError {
    /// A part is empty or contains a character outside `[A-Za-z0-9_]`, or starts with a digit.
    Characters,
    /// A part is longer than [`MAX_PART_LEN`] bytes.
    TooLong,
    /// More than one `.`.
    TooManyParts,
}

impl std::fmt::Display for IdentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IdentError::Characters => {
                "is not [schema.]identifier with each part [A-Za-z_][A-Za-z0-9_]*"
            }
            IdentError::TooLong => "has a part longer than 63 bytes (PostgreSQL would truncate it)",
            IdentError::TooManyParts => "has more than one `.` (at most schema.table)",
        })
    }
}

fn part_ok(p: &str) -> Result<(), IdentError> {
    let mut bytes = p.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return Err(IdentError::Characters),
    }
    if !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(IdentError::Characters);
    }
    if p.len() > MAX_PART_LEN {
        return Err(IdentError::TooLong);
    }
    Ok(())
}

impl TableName {
    pub fn parse(raw: &str) -> Result<TableName, IdentError> {
        let mut parts = raw.split('.');
        let first = parts.next().unwrap_or_default();
        let second = parts.next();
        if parts.next().is_some() {
            return Err(IdentError::TooManyParts);
        }
        part_ok(first)?;
        match second {
            None => Ok(TableName {
                schema: None,
                table: first.to_string(),
            }),
            Some(t) => {
                part_ok(t)?;
                Ok(TableName {
                    schema: Some(first.to_string()),
                    table: t.to_string(),
                })
            }
        }
    }

    /// The identifier quoted for `dialect`. Injection-free because [`TableName::parse`] admits no
    /// quote character into any part.
    pub fn quoted(&self, dialect: Dialect) -> String {
        let q = |p: &str| match dialect {
            Dialect::Postgres => format!("\"{p}\""),
            Dialect::Mysql => format!("`{p}`"),
        };
        match &self.schema {
            None => q(&self.table),
            Some(s) => format!("{}.{}", q(s), q(&self.table)),
        }
    }
}

impl std::fmt::Display for TableName {
    /// The unquoted `[schema.]table` — safe to log: an operator-declared identifier of `[A-Za-z0-9_]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.schema {
            None => f.write_str(&self.table),
            Some(s) => write!(f, "{s}.{}", self.table),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_identifiers_parse_and_quote_per_dialect() {
        let t = TableName::parse("ferro_jobs").unwrap();
        assert_eq!(t.schema, None);
        assert_eq!(t.quoted(Dialect::Postgres), "\"ferro_jobs\"");
        assert_eq!(t.quoted(Dialect::Mysql), "`ferro_jobs`");
        let t = TableName::parse("App_1.Jobs").unwrap();
        assert_eq!(t.schema.as_deref(), Some("App_1"));
        assert_eq!(t.quoted(Dialect::Postgres), "\"App_1\".\"Jobs\"");
        assert_eq!(t.to_string(), "App_1.Jobs");
        assert!(TableName::parse("_x").is_ok());
        assert!(TableName::parse(&"a".repeat(MAX_PART_LEN)).is_ok());
    }

    #[test]
    fn every_refused_identifier_names_its_rule() {
        for (raw, why) in [
            ("", IdentError::Characters),
            ("1jobs", IdentError::Characters),
            ("jobs-x", IdentError::Characters),
            ("jo bs", IdentError::Characters),
            ("jobs\"", IdentError::Characters),
            ("jobs`", IdentError::Characters),
            ("jobs;drop", IdentError::Characters),
            ("é", IdentError::Characters),
            (".jobs", IdentError::Characters),
            ("app.", IdentError::Characters),
            ("a.b.c", IdentError::TooManyParts),
        ] {
            assert_eq!(TableName::parse(raw), Err(why), "{raw:?}");
        }
        let long = "a".repeat(MAX_PART_LEN + 1);
        assert_eq!(TableName::parse(&long), Err(IdentError::TooLong));
        assert_eq!(
            TableName::parse(&format!("s.{long}")),
            Err(IdentError::TooLong)
        );
    }
}
