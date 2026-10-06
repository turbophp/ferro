//! `ferro gen` (M3-D2c, SPEC §11): PHP code from a CHECKED manifest — one readonly DTO class per
//! declared `dto`, a class of query-id constants, and the manifest itself.
//!
//! The DTOs are built for `ferro/client`'s hydrator (`Ferro\Client\Hydration\HydrationPlan`): one
//! promoted constructor parameter per result column, named as the column in camelCase, which is how
//! the hydrator matches them (`camelToSnake(param) == column`). Each parameter is typed as the
//! client's DEFAULT §9.1 policy decodes that column's §9 tag (`M1ValuePolicy`), and is NULLABLE:
//! preparing a statement does not report whether a column can be NULL, and a non-nullable guess
//! that is wrong is a `TypeError` on the first NULL row. A column with no tag (SQLite types values,
//! not columns) is `mixed`.

use std::collections::BTreeMap;
use std::path::Path;

use ferro_manifest::{Column, Manifest, Problem};
use ferro_proto::consts::tag;

/// One generated file: its path relative to the output directory, and its contents.
pub struct File {
    pub name: String,
    pub contents: String,
}

/// Generate every file, or every problem. Nothing is written by this function.
pub fn generate(manifest: &Manifest, queries_class: &str) -> Result<Vec<File>, Vec<Problem>> {
    let mut problems = Vec::new();
    let mut files = Vec::new();

    if let Err(m) = check_fqcn(queries_class) {
        problems.push(Problem {
            at: "--queries-class".into(),
            message: m,
        });
    }

    // DTOs: one per class; every query naming it must have the same columns.
    let mut dtos: BTreeMap<String, (&str, &[Column])> = BTreeMap::new();
    for (id, q) in &manifest.queries {
        let Some(dto) = q.dto.as_deref() else {
            continue;
        };
        let at = q.source.clone().unwrap_or_else(|| id.clone());
        let fq = dto.trim_start_matches('\\');
        if let Err(m) = check_fqcn(fq) {
            problems.push(Problem { at, message: m });
            continue;
        }
        let Some(cols) = q.columns.as_deref() else {
            problems.push(Problem {
                at,
                message: format!(
                    "query `{id}` declares dto `{fq}` but has no recorded columns: run \
                     `ferro check --write` on the manifest first"
                ),
            });
            continue;
        };
        match dtos.get(fq) {
            Some((other, existing)) if *existing != cols => problems.push(Problem {
                at,
                message: format!(
                    "dto `{fq}` is declared by `{other}` and `{id}` with different columns"
                ),
            }),
            Some(_) => {}
            None => {
                dtos.insert(fq.to_string(), (id.as_str(), cols));
            }
        }
    }
    for (fq, (id, cols)) in &dtos {
        match dto_file(fq, id, cols) {
            Ok(f) => files.push(f),
            Err(m) => problems.push(Problem {
                at: format!("dto `{fq}`"),
                message: m,
            }),
        }
    }

    match queries_file(manifest, queries_class) {
        Ok(f) => files.push(f),
        Err(ps) => problems.extend(ps),
    }
    files.push(File {
        name: "manifest.json".into(),
        contents: manifest.to_json_with_hash(),
    });

    if problems.is_empty() {
        Ok(files)
    } else {
        Err(problems)
    }
}

/// Write `files` under `dir` (each atomically, via `write`).
pub fn write_all(
    dir: &Path,
    files: &[File],
    write: impl Fn(&Path, &[u8]) -> std::io::Result<()>,
) -> Result<(), Problem> {
    std::fs::create_dir_all(dir).map_err(|e| Problem {
        at: dir.display().to_string(),
        message: format!("cannot create: {e}"),
    })?;
    for f in files {
        let path = dir.join(&f.name);
        write(&path, f.contents.as_bytes()).map_err(|e| Problem {
            at: path.display().to_string(),
            message: format!("cannot write: {e}"),
        })?;
    }
    Ok(())
}

fn split_fqcn(fq: &str) -> (Option<&str>, &str) {
    match fq.rsplit_once('\\') {
        Some((ns, short)) => (Some(ns), short),
        None => (None, fq),
    }
}

/// A PHP class name: identifier segments separated by `\`, none of them a reserved word.
fn check_fqcn(fq: &str) -> Result<(), String> {
    let ok = !fq.is_empty()
        && fq.split('\\').all(|seg| {
            is_identifier(seg) && !RESERVED.contains(&seg.to_ascii_lowercase().as_str())
        });
    if ok {
        Ok(())
    } else {
        Err(format!("`{fq}` is not a usable PHP class name"))
    }
}

fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// PHP's reserved words that cannot name a class (a subset that matters for generated names).
const RESERVED: &[&str] = &[
    "abstract",
    "and",
    "array",
    "as",
    "bool",
    "break",
    "callable",
    "case",
    "catch",
    "class",
    "clone",
    "const",
    "continue",
    "declare",
    "default",
    "do",
    "echo",
    "else",
    "elseif",
    "empty",
    "enum",
    "extends",
    "false",
    "final",
    "float",
    "fn",
    "for",
    "foreach",
    "function",
    "global",
    "goto",
    "if",
    "implements",
    "include",
    "instanceof",
    "int",
    "interface",
    "isset",
    "iterable",
    "list",
    "match",
    "mixed",
    "namespace",
    "never",
    "new",
    "null",
    "object",
    "or",
    "print",
    "private",
    "protected",
    "public",
    "readonly",
    "require",
    "return",
    "self",
    "static",
    "string",
    "switch",
    "throw",
    "trait",
    "true",
    "try",
    "unset",
    "use",
    "var",
    "void",
    "while",
    "xor",
    "yield",
];

/// The column's constructor parameter name: its camelCase form, which the client's hydrator maps
/// back with `camelToSnake`. A name that does not survive that round trip cannot be hydrated by
/// name, so it is refused rather than generated into a DTO that throws on every row.
fn param_name(column: &str) -> Result<String, String> {
    if !is_identifier(column) {
        return Err(format!(
            "column `{column}` is not a PHP identifier: alias it in the SQL (`… AS some_name`)"
        ));
    }
    let mut out = String::new();
    let mut upper = false;
    for c in column.chars() {
        if c == '_' && !out.is_empty() {
            upper = true;
        } else if upper {
            out.push(c.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    if camel_to_snake(&out) != column {
        return Err(format!(
            "column `{column}` cannot be matched by name by the hydrator (it maps camelCase \
             parameters to snake_case columns): alias it in lower snake_case"
        ));
    }
    Ok(out)
}

/// `HydrationPlan::camelToSnake`: `strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $s))`.
fn camel_to_snake(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && c.is_ascii_uppercase() {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The PHP type the client's DEFAULT §9.1 policy decodes a tag to (`M1ValuePolicy`), nullable.
fn php_type(t: Option<u8>) -> &'static str {
    match t {
        Some(tag::BOOL) => "?bool",
        Some(tag::I64) => "?int",
        Some(tag::F64) => "?float",
        Some(tag::TEXT) | Some(tag::BYTES) => "?string",
        Some(tag::U64) => "int|\\Ferro\\U64|null",
        Some(tag::DECIMAL) => "?\\Ferro\\Decimal",
        Some(tag::DATE) => "?\\Ferro\\Date",
        Some(tag::TIME) => "?\\Ferro\\Time",
        Some(tag::TIMESTAMP) => "?\\Ferro\\NaiveTimestamp",
        Some(tag::TIMESTAMPTZ) => "?\\DateTimeImmutable",
        Some(tag::UUID) => "?\\Ferro\\Uuid",
        Some(tag::JSON) => "?\\Ferro\\Json",
        _ => "mixed",
    }
}

fn dto_file(fq: &str, id: &str, cols: &[Column]) -> Result<File, String> {
    let (ns, short) = split_fqcn(fq);
    let mut params = Vec::with_capacity(cols.len());
    let mut seen = std::collections::BTreeSet::new();
    for c in cols {
        let name = param_name(&c.name)?;
        if !seen.insert(name.clone()) {
            return Err(format!(
                "column `{}` appears twice: alias one of them",
                c.name
            ));
        }
        params.push(format!(
            "        /** `{}` ({}) */\n        public {} ${},",
            c.name.replace("*/", "*\\/"),
            if c.type_name.is_empty() {
                "untyped"
            } else {
                c.type_name.as_str()
            }
            .replace("*/", "*\\/"),
            php_type(c.tag),
            name
        ));
    }
    let mut s = String::from(
        "<?php\n// Generated by `ferro gen` (SPEC §11). Do not edit: regenerate.\ndeclare(strict_types=1);\n\n",
    );
    if let Some(ns) = ns {
        s.push_str(&format!("namespace {ns};\n\n"));
    }
    s.push_str(&format!(
        "/**\n * The row of query `{id}`. Every property is nullable: preparing a statement does not report\n * whether a column can be NULL.\n */\nfinal readonly class {short}\n{{\n    public function __construct(\n{}\n    ) {{}}\n}}\n",
        params.join("\n")
    ));
    Ok(File {
        name: format!("{short}.php"),
        contents: s,
    })
}

fn queries_file(manifest: &Manifest, fq: &str) -> Result<File, Vec<Problem>> {
    let (ns, short) = split_fqcn(fq);
    let mut problems = Vec::new();
    let mut consts: BTreeMap<String, &str> = BTreeMap::new();
    for id in manifest.queries.keys() {
        let name: String = id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect();
        if let Some(other) = consts.insert(name.clone(), id) {
            problems.push(Problem {
                at: format!("query `{id}`"),
                message: format!("its constant `{name}` collides with query `{other}`'s"),
            });
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let mut s = String::from(
        "<?php\n// Generated by `ferro gen` (SPEC §11). Do not edit: regenerate.\ndeclare(strict_types=1);\n\n",
    );
    if let Some(ns) = ns {
        s.push_str(&format!("namespace {ns};\n\n"));
    }
    s.push_str(&format!(
        "/** The query ids of the manifest {}, for `Connection::…ById`. */\nfinal class {short}\n{{\n    public const MANIFEST_HASH = '{}';\n\n",
        manifest.hash(),
        manifest.hash()
    ));
    for (name, id) in &consts {
        s.push_str(&format!("    public const {name} = '{id}';\n"));
    }
    s.push_str("}\n");
    Ok(File {
        name: format!("{short}.php"),
        contents: s,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn param_names_round_trip_through_the_hydrators_rule() {
        assert_eq!(param_name("created_at").unwrap(), "createdAt");
        assert_eq!(param_name("id").unwrap(), "id");
        assert_eq!(param_name("user_id2").unwrap(), "userId2");
        // `emailAddress` comes back as `email_address`, not `emailAddress`: unmatched by name.
        assert!(param_name("emailAddress").is_err());
        assert!(param_name("count(*)").is_err());
        assert!(param_name("a__b").is_err());
    }

    #[test]
    fn class_names_are_checked() {
        assert!(check_fqcn("App\\Dto\\User").is_ok());
        assert!(check_fqcn("App\\List").is_err());
        assert!(check_fqcn("App\\9x").is_err());
        assert!(check_fqcn("").is_err());
    }
}
