//! `ferro gen` (M3-D2c, SPEC §11): PHP code from a CHECKED manifest — one readonly DTO class per
//! declared `dto`, a class of query-id constants, and the manifest itself.
//!
//! The DTOs are built for `ferro/client`'s hydrator (`Ferro\Client\Hydration\HydrationPlan`): one
//! promoted constructor parameter per result column, named so that the hydrator maps it back to
//! THAT column. The hydrator resolves a parameter `$p` to the column `camelToSnake($p)` if one
//! exists, else to the column named exactly `$p`; [`hydrator_camel_to_snake`] and [`resolve`] are
//! ports of those two rules, and every generated name is checked against them over the query's
//! whole column list (a refused column gets an "alias it" message, never a class that throws).
//!
//! Each parameter is typed as EXACTLY what the client's DEFAULT §9.1 policy (`M1ValuePolicy` with
//! default `TypePolicyOptions`) can return for that column's §9 tag — sentinels included, so a
//! `TIMESTAMP`'s `infinity` (handed back as the canonical TEXT) is a `string` the type admits — and
//! is NULLABLE: preparing a statement does not report whether a column can be NULL, and a
//! non-nullable guess that is wrong is a `TypeError` on the first NULL row. A column with no tag
//! (SQLite types values, not columns) is `mixed`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use ferro_manifest::{Column, Manifest, Problem};
use ferro_proto::consts::tag;

/// One generated file: its name inside the output directory, and its contents.
pub struct File {
    pub name: String,
    pub contents: String,
}

/// The file in `--out` listing what the previous run generated, so the next run can remove what it
/// no longer generates — and NOTHING else.
pub const MARKER: &str = ".ferro-gen";

/// Generate every file, or every problem. Nothing is written by this function.
pub fn generate(manifest: &Manifest, queries_class: &str) -> Result<Vec<File>, Vec<Problem>> {
    let mut problems = Vec::new();
    let mut files = Vec::new();

    let queries_ok = match check_fqcn(queries_class) {
        Ok(()) => true,
        Err(m) => {
            problems.push(Problem {
                at: "--queries-class".into(),
                message: m,
            });
            false
        }
    };

    // DTOs, keyed by the class's IDENTITY — PHP class names are case-insensitive, so `App\user`
    // and `App\User` are one class.
    struct Dto<'a> {
        spelling: &'a str,
        id: &'a str,
        cols: &'a [Column],
    }
    let mut dtos: BTreeMap<String, Dto<'_>> = BTreeMap::new();
    for (id, q) in &manifest.queries {
        let Some(dto) = q.dto.as_deref() else {
            continue;
        };
        let at = q.source.clone().unwrap_or_else(|| id.clone());
        // One leading `\` (a fully-qualified spelling) is accepted; anything else is checked.
        let fq = dto.strip_prefix('\\').unwrap_or(dto);
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
        let key = fq.to_ascii_lowercase();
        match dtos.get(&key) {
            None => {
                dtos.insert(
                    key,
                    Dto {
                        spelling: fq,
                        id: id.as_str(),
                        cols,
                    },
                );
            }
            Some(first) => {
                if first.spelling != fq {
                    problems.push(Problem {
                        at,
                        message: format!(
                            "dto `{fq}` (query `{id}`) and `{}` (query `{}`) are ONE PHP class — \
                             class names are case-insensitive — spelled two ways: spell it one way",
                            first.spelling, first.id
                        ),
                    });
                } else if let Some(m) = column_conflict(first.cols, cols) {
                    problems.push(Problem {
                        at,
                        message: format!(
                            "dto `{fq}` is declared by `{}` and `{id}` with {m}",
                            first.id
                        ),
                    });
                }
            }
        }
    }

    // Every class is one flat `<Short>.php`; two classes on one file name would overwrite each
    // other (and on a case-insensitive filesystem, so would two names differing in case).
    let mut by_file: BTreeMap<String, String> = BTreeMap::new();
    if queries_ok {
        if dtos.contains_key(&queries_class.to_ascii_lowercase()) {
            problems.push(Problem {
                at: "--queries-class".into(),
                message: format!(
                    "`{queries_class}` is also a declared dto: choose another --queries-class"
                ),
            });
        }
        by_file.insert(
            file_name(queries_class).to_ascii_lowercase(),
            queries_class.to_string(),
        );
    }
    for d in dtos.values() {
        let name = file_name(d.spelling);
        match by_file.get(&name.to_ascii_lowercase()) {
            Some(other) if !other.eq_ignore_ascii_case(d.spelling) => problems.push(Problem {
                at: format!("dto `{}`", d.spelling),
                message: format!(
                    "it and `{other}` would both be written to `{name}` (output files are \
                     flat, compared case-insensitively): rename one of them"
                ),
            }),
            Some(_) => {}
            None => {
                by_file.insert(name.to_ascii_lowercase(), d.spelling.to_string());
            }
        }
    }

    for d in dtos.values() {
        match dto_file(d.spelling, d.id, d.cols) {
            Ok(f) => files.push(f),
            Err(ms) => problems.extend(ms.into_iter().map(|m| Problem {
                at: format!("dto `{}`", d.spelling),
                message: m,
            })),
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

/// Why two queries' columns cannot share one DTO class, or `None` if they can. The identity is the
/// column NAMES and §9 TAGS in order: the server's type name does not decide the PHP type, and the
/// order decides the constructor's.
fn column_conflict(a: &[Column], b: &[Column]) -> Option<&'static str> {
    let key = |cols: &[Column]| -> Vec<(String, Option<u8>)> {
        cols.iter().map(|c| (c.name.clone(), c.tag)).collect()
    };
    let (ka, kb) = (key(a), key(b));
    if ka == kb {
        return None;
    }
    let (mut sa, mut sb) = (ka, kb);
    sa.sort();
    sb.sort();
    Some(if sa == sb {
        "the same columns in a different order (the constructor follows the column order): \
         select them in one order"
    } else {
        "different columns (names or §9 types)"
    })
}

/// Where a generated or listed file is written: plain, flat names only.
fn is_generated_name(name: &str) -> bool {
    name == "manifest.json" || name.strip_suffix(".php").is_some_and(is_ascii_identifier)
}

/// What happened when the output directory could not be fully written.
#[derive(Debug)]
pub enum WriteError {
    /// Nothing generated in the output directory was replaced or removed.
    Untouched(Problem),
    /// Some files were replaced before the failure.
    Partial {
        problem: Problem,
        written: Vec<String>,
        not_written: Vec<String>,
    },
}

/// What a successful write did.
#[derive(Debug)]
pub struct Written {
    pub written: Vec<String>,
    /// Files the PREVIOUS run generated (per its marker) that this run no longer does.
    pub removed: Vec<String>,
}

/// Write `files` into `dir`.
///
/// Every file is staged first (written and synced beside its target); if ANY staging fails, the
/// temporaries are removed and nothing generated is touched. Only then are they renamed into place;
/// a failure there is reported as [`WriteError::Partial`], naming exactly what was replaced. The
/// marker is rewritten BEFORE the renames as the union of the old and new lists, so an interrupted
/// run never loses track of a file it generated, and AFTER them as exactly the new list. Stale files
/// are removed only when the previous marker lists them and they are regular files.
pub fn write_out(
    dir: &Path,
    files: &[File],
    rename: impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> Result<Written, WriteError> {
    let untouched = |at: &Path, message: String| {
        WriteError::Untouched(Problem {
            at: at.display().to_string(),
            message,
        })
    };
    let existed = dir.exists();
    std::fs::create_dir_all(dir).map_err(|e| untouched(dir, format!("cannot create: {e}")))?;
    let previous = read_marker(dir).map_err(|e| untouched(&dir.join(MARKER), e))?;

    // Refuse a target that is a directory or anything else not a plain file BEFORE writing.
    for name in files
        .iter()
        .map(|f| f.name.as_str())
        .chain(std::iter::once(MARKER))
    {
        let p = dir.join(name);
        if let Ok(md) = std::fs::symlink_metadata(&p)
            && !md.file_type().is_file()
        {
            return Err(untouched(
                &p,
                "exists and is not a regular file: remove it and re-run".into(),
            ));
        }
    }

    let mut staged: Vec<PathBuf> = Vec::new();
    let discard = |staged: &[PathBuf]| {
        for t in staged {
            let _ = std::fs::remove_file(t);
        }
        if !existed {
            let _ = std::fs::remove_dir(dir);
        }
    };
    for f in files {
        match stage(dir, &f.name, f.contents.as_bytes()) {
            Ok(t) => staged.push(t),
            Err(e) => {
                discard(&staged);
                return Err(untouched(&dir.join(&f.name), format!("cannot write: {e}")));
            }
        }
    }

    let new_names: Vec<String> = files.iter().map(|f| f.name.clone()).collect();
    let union: BTreeSet<String> = previous.iter().cloned().chain(new_names.clone()).collect();
    if let Err(e) = write_marker(dir, union.iter(), &rename) {
        discard(&staged);
        return Err(untouched(&dir.join(MARKER), format!("cannot write: {e}")));
    }

    for (i, (tmp, name)) in staged.iter().zip(&new_names).enumerate() {
        if let Err(e) = rename(tmp, &dir.join(name)) {
            for t in &staged[i..] {
                let _ = std::fs::remove_file(t);
            }
            if i == 0 {
                // Only the marker changed, and it lists a superset of what is on disk.
                return Err(untouched(&dir.join(name), format!("cannot write: {e}")));
            }
            return Err(WriteError::Partial {
                problem: Problem {
                    at: dir.join(name).display().to_string(),
                    message: format!("cannot write: {e}"),
                },
                written: new_names[..i].to_vec(),
                not_written: new_names[i..].to_vec(),
            });
        }
    }

    let mut removed = Vec::new();
    for old in previous.iter().filter(|n| !new_names.contains(n)) {
        let p = dir.join(old);
        match std::fs::symlink_metadata(&p) {
            Ok(md) if md.file_type().is_file() => {
                if let Err(e) = std::fs::remove_file(&p) {
                    return Err(WriteError::Partial {
                        problem: Problem {
                            at: p.display().to_string(),
                            message: format!(
                                "a previous run generated it and this one does not, but it \
                                 cannot be removed: {e}"
                            ),
                        },
                        written: new_names,
                        not_written: Vec::new(),
                    });
                }
                removed.push(old.clone());
            }
            // Gone already, or no longer a plain file: not ours to remove.
            _ => {}
        }
    }
    if let Err(e) = write_marker(dir, new_names.iter(), &rename) {
        return Err(WriteError::Partial {
            problem: Problem {
                at: dir.join(MARKER).display().to_string(),
                message: format!("cannot update the list of generated files: {e}"),
            },
            written: new_names,
            not_written: Vec::new(),
        });
    }
    Ok(Written {
        written: new_names,
        removed,
    })
}

/// The previous run's file list; entries that are not plain generated names are ignored, so a
/// hand-edited marker can never make `gen` delete outside what it could have generated.
fn read_marker(dir: &Path) -> Result<Vec<String>, String> {
    match std::fs::read_to_string(dir.join(MARKER)) {
        Ok(s) => Ok(s
            .lines()
            .map(str::trim)
            .filter(|l| !l.starts_with('#') && is_generated_name(l))
            .map(str::to_string)
            .collect()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("cannot read the list of generated files: {e}")),
    }
}

fn write_marker<'a>(
    dir: &Path,
    names: impl Iterator<Item = &'a String>,
    rename: &impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut s = String::from(
        "# Files generated by `ferro gen` into this directory. The next run removes the ones it no\n\
         # longer generates, and nothing else.\n",
    );
    for n in names {
        s.push_str(n);
        s.push('\n');
    }
    let tmp = stage(dir, MARKER, s.as_bytes())?;
    rename(&tmp, &dir.join(MARKER)).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

/// Write `bytes` to a fresh temporary beside `dir/name` and sync it.
fn stage(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<PathBuf> {
    let mut n = 0u32;
    loop {
        let tmp = dir.join(format!(".{name}.ferro-tmp-{}-{n}", std::process::id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(mut f) => {
                let r = f.write_all(bytes).and_then(|()| f.sync_all());
                if let Err(e) = r {
                    let _ = std::fs::remove_file(&tmp);
                    return Err(e);
                }
                return Ok(tmp);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && n < 64 => n += 1,
            Err(e) => return Err(e),
        }
    }
}

fn split_fqcn(fq: &str) -> (Option<&str>, &str) {
    match fq.rsplit_once('\\') {
        Some((ns, short)) => (Some(ns), short),
        None => (None, fq),
    }
}

fn file_name(fq: &str) -> String {
    format!("{}.php", split_fqcn(fq).1)
}

/// A PHP class name usable by generated code (verified with `php -l`, PHP 8.4): ASCII identifier
/// segments separated by `\`; the CLASS segment not a reserved word; the namespace may use reserved
/// words (PHP 8 lexes a qualified name as one token) except that it must not START with
/// `namespace` (that spells a relative name) and a one-segment namespace must not be
/// `__halt_compiler`.
fn check_fqcn(fq: &str) -> Result<(), String> {
    let bad = || Err(format!("`{fq}` is not a usable PHP class name"));
    if fq.is_empty() || !fq.split('\\').all(is_ascii_identifier) {
        return bad();
    }
    let (ns, short) = split_fqcn(fq);
    if RESERVED_CLASS.contains(&short.to_ascii_lowercase().as_str()) {
        return Err(format!(
            "`{fq}` is not a usable PHP class name: `{short}` is reserved in PHP"
        ));
    }
    if let Some(ns) = ns {
        let first = ns.split('\\').next().unwrap_or(ns);
        if first.eq_ignore_ascii_case("namespace")
            || (!ns.contains('\\') && ns.eq_ignore_ascii_case("__halt_compiler"))
        {
            return Err(format!(
                "`{fq}` is not a usable PHP class name: namespace `{ns}` is not declarable"
            ));
        }
    }
    Ok(())
}

fn is_ascii_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// PHP's variable-name rule, `[a-zA-Z_\x80-\xff][a-zA-Z0-9_\x80-\xff]*` over BYTES: every non-ASCII
/// character encodes as bytes >= 0x80.
fn is_php_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || !c.is_ascii())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || !c.is_ascii())
}

/// Words PHP refuses as a CLASS name (PHP 8.4, each verified with `php -l`; `enum`, `from`,
/// `resource`, `numeric` and `__COMPILER_HALT_OFFSET__` are accepted and are not listed). Compared
/// lowercase: keywords are case-insensitive.
const RESERVED_CLASS: &[&str] = &[
    "__class__",
    "__dir__",
    "__file__",
    "__function__",
    "__halt_compiler",
    "__line__",
    "__method__",
    "__namespace__",
    "__property__",
    "__trait__",
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
    "die",
    "do",
    "echo",
    "else",
    "elseif",
    "empty",
    "enddeclare",
    "endfor",
    "endforeach",
    "endif",
    "endswitch",
    "endwhile",
    "eval",
    "exit",
    "extends",
    "false",
    "final",
    "finally",
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
    "include_once",
    "instanceof",
    "insteadof",
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
    "parent",
    "print",
    "private",
    "protected",
    "public",
    "readonly",
    "require",
    "require_once",
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

/// Variable names PHP refuses as a constructor parameter ("Cannot use $this as parameter", "Cannot
/// re-assign auto-global variable"). Case-sensitive, as variable names are.
const FORBIDDEN_PARAMS: &[&str] = &[
    "this", "GLOBALS", "_GET", "_POST", "_SERVER", "_COOKIE", "_FILES", "_ENV", "_REQUEST",
    "_SESSION",
];

/// `HydrationPlan::camelToSnake`, ported exactly:
/// `strtolower(preg_replace('/([a-z0-9])([A-Z])/', '$1_$2', $s))`. The regex has no `/u`, so it
/// runs over BYTES (non-ASCII bytes match neither class), matches do not overlap (`aBC` → `a_BC`),
/// and PHP >= 8.2's `strtolower` folds ASCII only.
fn hydrator_camel_to_snake(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() + 4);
    let mut i = 0;
    while i < b.len() {
        if i + 1 < b.len()
            && (b[i].is_ascii_lowercase() || b[i].is_ascii_digit())
            && b[i + 1].is_ascii_uppercase()
        {
            out.extend_from_slice(&[b[i], b'_', b[i + 1]]);
            i += 2;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out.make_ascii_lowercase();
    String::from_utf8(out).expect("only ASCII bytes were inserted or folded")
}

/// `HydrationPlan::build`'s lookup for one parameter: the column named `camelToSnake($p)`, else the
/// column named exactly `$p`; last-wins on duplicate column names, as its index map is.
fn resolve(param: &str, columns: &[&str]) -> Option<usize> {
    let snake = hydrator_camel_to_snake(param);
    let last = |want: &str| columns.iter().rposition(|c| *c == want);
    last(&snake).or_else(|| last(param))
}

/// The camelCase spelling the hydrator maps back: an `_` followed by a lowercase letter becomes
/// that letter in uppercase, but only where the regex will put the `_` back — after `[a-z0-9]`.
/// Anything else is kept as is (`x_y_z` → `xY_z`, `__id` → `__id`).
fn camel_candidate(column: &str) -> String {
    let mut out = String::with_capacity(column.len());
    let mut chars = column.chars().peekable();
    while let Some(c) = chars.next() {
        let prev_ok = out
            .chars()
            .last()
            .is_some_and(|p: char| p.is_ascii_lowercase() || p.is_ascii_digit());
        match chars.peek() {
            Some(&n) if c == '_' && prev_ok && n.is_ascii_lowercase() => {
                out.push(n.to_ascii_uppercase());
                chars.next();
            }
            _ => out.push(c),
        }
    }
    out
}

/// Each column's constructor parameter name, checked against the hydrator's rules over the WHOLE
/// column list: the camelCase spelling if it maps back to this column, else the column's own name
/// if that does. One problem per column that neither reaches.
fn param_names(columns: &[&str]) -> Result<Vec<String>, Vec<String>> {
    let mut names = Vec::with_capacity(columns.len());
    let mut problems = Vec::new();
    let mut reported = BTreeSet::new();
    for (i, &c) in columns.iter().enumerate() {
        // The hydrator's index map is last-wins, so the first of two equal names is unreachable.
        if columns.iter().filter(|o| **o == c).count() > 1 {
            if reported.insert(c) {
                problems.push(format!("column `{c}` appears twice: alias one of them"));
            }
            continue;
        }
        if !is_php_identifier(c) {
            problems.push(format!(
                "column `{c}` is not a PHP identifier: alias it in the SQL (`… AS some_name`)"
            ));
            continue;
        }
        let camel = camel_candidate(c);
        let found = [camel.as_str(), c]
            .into_iter()
            .find(|p| !FORBIDDEN_PARAMS.contains(p) && resolve(p, columns) == Some(i));
        match found {
            Some(p) => names.push(p.to_string()),
            None if FORBIDDEN_PARAMS.contains(&c) => problems.push(format!(
                "column `{c}` would be the parameter `${c}`, which PHP forbids: alias it in the SQL"
            )),
            None => problems.push(format!(
                "column `{c}` cannot be matched back by the hydrator (it reads a parameter `$p` \
                 from the column `camelToSnake($p)` — here `{}`, another column — before the \
                 column named exactly `$p`): alias it in the SQL",
                hydrator_camel_to_snake(c)
            )),
        }
    }
    if problems.is_empty() {
        // Resolution is a function of the parameter, so distinct columns cannot share one; this
        // holds by construction and is asserted so a future change cannot silently break it.
        let distinct: BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            distinct.len(),
            names.len(),
            "two columns resolved to one parameter"
        );
        Ok(names)
    } else {
        Err(problems)
    }
}

/// The PHP type of a column: EXACTLY what `M1ValuePolicy` under default `TypePolicyOptions` can
/// return for the tag, plus `null`. `None` for a tag the client cannot decode.
fn php_type(t: Option<u8>) -> Option<&'static str> {
    Some(match t {
        // No column type (SQLite), or a column the prepare typed as NULL: no narrower honest type.
        None | Some(tag::NULL) => "mixed",
        Some(tag::BOOL) => "?bool",
        Some(tag::I64) => "?int",
        // NaN and ±Infinity are PHP floats.
        Some(tag::F64) => "?float",
        // BYTES decodes to a binary PHP string (a `Ferro\Bytes` is a BIND-side wrapper only).
        Some(tag::TEXT) | Some(tag::BYTES) => "?string",
        // u64_overflow=object: an int when it fits, a `Ferro\U64` above PHP_INT_MAX.
        Some(tag::U64) => "int|\\Ferro\\U64|null",
        // decimal=object; `NaN`/`±Infinity` live inside the value object.
        Some(tag::DECIMAL) => "?\\Ferro\\Decimal",
        // Date sentinels (`infinity`, MySQL's zero date) live inside the value object; so does
        // PG's `24:00:00` and MySQL's ±838 h for TIME.
        Some(tag::DATE) => "?\\Ferro\\Date",
        Some(tag::TIME) => "?\\Ferro\\Time",
        // Sentinels (`infinity`, `-infinity`, `0000-00-00 00:00:00`) are not instants: the policy
        // hands back the canonical TEXT, a string.
        Some(tag::TIMESTAMP) => "\\Ferro\\NaiveTimestamp|string|null",
        Some(tag::TIMESTAMPTZ) => "\\DateTimeImmutable|string|null",
        Some(tag::UUID) => "?\\Ferro\\Uuid",
        Some(tag::JSON) => "?\\Ferro\\Json",
        Some(_) => return None,
    })
}

fn dto_file(fq: &str, id: &str, cols: &[Column]) -> Result<File, Vec<String>> {
    let (ns, short) = split_fqcn(fq);
    let col_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    let mut problems = Vec::new();
    let names = param_names(&col_names).unwrap_or_else(|ps| {
        problems.extend(ps);
        Vec::new()
    });
    let mut types = Vec::with_capacity(cols.len());
    for c in cols {
        match php_type(c.tag) {
            Some(t) => types.push(t),
            None => problems.push(format!(
                "column `{}` has §9 tag {:?}, which the client cannot decode",
                c.name, c.tag
            )),
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let params: Vec<String> = cols
        .iter()
        .zip(&names)
        .zip(&types)
        .map(|((c, name), ty)| {
            format!(
                "        /** `{}` ({}) */\n        public {ty} ${name},",
                c.name,
                if c.type_name.is_empty() {
                    "untyped"
                } else {
                    c.type_name.as_str()
                }
                .replace("*/", "*\\/"),
            )
        })
        .collect();
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

/// The constant naming a query id: its ASCII letters and digits uppercased, everything else `_`.
fn const_name(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn queries_file(manifest: &Manifest, fq: &str) -> Result<File, Vec<Problem>> {
    let (ns, short) = split_fqcn(fq);
    let mut problems = Vec::new();
    let mut consts: BTreeMap<String, &str> = BTreeMap::new();
    for id in manifest.queries.keys() {
        let name = const_name(id);
        let at = format!("query `{id}`");
        if name.eq_ignore_ascii_case("class") {
            problems.push(Problem {
                at,
                message: format!(
                    "its constant would be `{name}`, which PHP forbids as a class constant name: \
                     rename the query"
                ),
            });
        } else if name == "MANIFEST_HASH" {
            problems.push(Problem {
                at,
                message: "its constant would be `MANIFEST_HASH`, which the generated class \
                          already defines: rename the query"
                    .into(),
            });
        } else if let Some(other) = consts.insert(name.clone(), id) {
            problems.push(Problem {
                at,
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

    /// The PHP rule's outputs, RECORDED by running `HydrationPlan::camelToSnake` itself (PHP 8.4.19,
    /// via `ReflectionMethod`) — so this table holds even where no `php` is installed.
    const CAMEL_TO_SNAKE_RECORDED: &[(&str, &str)] = &[
        ("x_y_z", "x_y_z"),
        ("a_b_c", "a_b_c"),
        ("is_a_b", "is_a_b"),
        ("__id", "__id"),
        ("userId", "user_id"),
        ("ID", "id"),
        ("a__b", "a__b"),
        ("id_", "id_"),
        ("a_1", "a_1"),
        ("v1_a_b", "v1_a_b"),
        ("created_at", "created_at"),
        ("é_x", "é_x"),
        ("xYZ", "x_yz"),
        ("xY_z", "x_y_z"),
        ("aBC", "a_bc"),
        ("a1B2C", "a1_b2_c"),
        ("_Id", "_id"),
        ("isAB", "is_ab"),
        ("isA_b", "is_a_b"),
        ("emailAddress", "email_address"),
        ("ABc", "abc"),
        ("aB", "a_b"),
        ("v1AB", "v1_ab"),
        ("éX", "éx"),
        ("aÉb", "aÉb"),
    ];

    #[test]
    fn camel_to_snake_matches_the_recorded_php_outputs() {
        for (input, want) in CAMEL_TO_SNAKE_RECORDED {
            assert_eq!(hydrator_camel_to_snake(input), *want, "input `{input}`");
        }
    }

    fn php() -> Option<std::process::Command> {
        let ok = std::process::Command::new("php")
            .arg("-v")
            .output()
            .is_ok_and(|o| o.status.success());
        if ok {
            Some(std::process::Command::new("php"))
        } else {
            // "skip:" so CI's no-skip gate (ci/assert-no-skips.sh) fails the lane if PHP is missing.
            eprintln!("skip: no `php` on PATH; the live PHP comparison did not run");
            None
        }
    }

    fn hydration_plan_php() -> String {
        format!(
            "{}/../../../php/client/src/Client/Hydration/HydrationPlan.php",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    /// The port against the REAL rule: `HydrationPlan::camelToSnake`, called through reflection on
    /// the client's own source — so a change to the PHP rule fails this test.
    #[test]
    fn camel_to_snake_matches_the_php_hydrator_when_php_is_available() {
        let Some(mut cmd) = php() else { return };
        let mut inputs: Vec<String> = CAMEL_TO_SNAKE_RECORDED
            .iter()
            .map(|(i, _)| i.to_string())
            .collect();
        for (i, _) in CAMEL_TO_SNAKE_RECORDED {
            inputs.push(camel_candidate(i));
        }
        inputs.extend(["plan_a_price", "ab_c_d", "col_A", "Name", "a1b_2"].map(String::from));
        let script = format!(
            "require {:?}; $m = new ReflectionMethod(Ferro\\Client\\Hydration\\HydrationPlan::class, 'camelToSnake'); \
             foreach (array_slice($argv, 1) as $s) {{ echo $m->invoke(null, $s), \"\\n\"; }}",
            hydration_plan_php()
        );
        let out = cmd
            .arg("-r")
            .arg(&script)
            .arg("--")
            .args(&inputs)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let got: Vec<String> = String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        assert_eq!(got.len(), inputs.len());
        for (input, php) in inputs.iter().zip(&got) {
            assert_eq!(&hydrator_camel_to_snake(input), php, "input `{input}`");
        }
    }

    /// The generated names against the REAL `HydrationPlan::build`: a class with those parameters,
    /// hydrated from the column list, must read column `i` into parameter `i`.
    #[test]
    fn generated_names_resolve_through_the_php_hydration_plan_when_php_is_available() {
        let Some(mut cmd) = php() else { return };
        let cols = [
            "x_y_z",
            "a_b_c",
            "is_a_b",
            "__id",
            "userId",
            "ID",
            "a__b",
            "id_",
            "a_1",
            "v1_a_b",
            "created_at",
            "é_x",
            "plan_a_price",
            "col_A",
            "emailAddress",
            "Name",
            "_",
        ];
        let names = param_names(&cols).unwrap();
        let params: Vec<String> = names.iter().map(|n| format!("public ${n}")).collect();
        let script = format!(
            "spl_autoload_register(function ($c) {{ $p = {root:?} . '/' . str_replace('\\\\', '/', substr($c, 6)) . '.php'; if (str_starts_with($c, 'Ferro\\\\') && is_file($p)) require $p; }}); \
             final class G {{ public function __construct({}) {{}} }} \
             $cols = array_slice($argv, 1); \
             $plan = Ferro\\Client\\Hydration\\HydrationPlan::build(G::class, $cols); \
             echo json_encode($plan->argsFor(array_keys($cols)));",
            params.join(", "),
            root = format!("{}/../../../php/client/src", env!("CARGO_MANIFEST_DIR")),
        );
        let out = cmd
            .arg("-r")
            .arg(&script)
            .arg("--")
            .args(cols)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let want: Vec<usize> = (0..cols.len()).collect();
        let got: Vec<usize> = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(got, want, "names {names:?}");
    }

    #[test]
    fn param_names_follow_the_hydrators_rule_and_its_exact_name_fallback() {
        let cols = [
            "created_at",
            "id",
            "user_id2",
            "x_y_z",
            "__id",
            "is_a_b",
            "a__b",
            "emailAddress",
            "NAME",
            "é_x",
        ];
        assert_eq!(
            param_names(&cols).unwrap(),
            [
                "createdAt",
                "id",
                "userId2",
                "xY_z",
                "__id",
                "isA_b",
                "a__b",
                "emailAddress",
                "NAME",
                "é_x"
            ]
        );
        // `ID` alone falls back to its exact name; beside an `id` column, `camelToSnake('ID')` is
        // `id`, so the hydrator would read the WRONG column: refused.
        let e = param_names(&["id", "ID"]).unwrap_err();
        assert_eq!(e.len(), 1);
        assert!(e[0].contains("`ID` cannot be matched back"), "{e:?}");
        // Likewise `userId` beside `user_id`.
        assert!(param_names(&["user_id", "userId"]).is_err());
        assert!(param_names(&["count(*)"]).unwrap_err()[0].contains("alias it in the SQL"));
        assert!(param_names(&["this"]).unwrap_err()[0].contains("PHP forbids"));
        assert!(param_names(&["GLOBALS"]).is_err());
        // A duplicate column: the hydrator's index map is last-wins, so the first is unreachable.
        let e = param_names(&["id", "name", "id"]).unwrap_err();
        assert_eq!(e.len(), 1, "{e:?}");
        assert!(e[0].contains("appears twice"), "{e:?}");
    }

    #[test]
    fn every_tag_has_exactly_the_default_policys_php_type() {
        let want: &[(Option<u8>, &str)] = &[
            (None, "mixed"),
            (Some(tag::NULL), "mixed"),
            (Some(tag::BOOL), "?bool"),
            (Some(tag::I64), "?int"),
            (Some(tag::U64), "int|\\Ferro\\U64|null"),
            (Some(tag::F64), "?float"),
            (Some(tag::DECIMAL), "?\\Ferro\\Decimal"),
            (Some(tag::TEXT), "?string"),
            (Some(tag::BYTES), "?string"),
            (Some(tag::DATE), "?\\Ferro\\Date"),
            (Some(tag::TIME), "?\\Ferro\\Time"),
            (Some(tag::TIMESTAMP), "\\Ferro\\NaiveTimestamp|string|null"),
            (Some(tag::TIMESTAMPTZ), "\\DateTimeImmutable|string|null"),
            (Some(tag::UUID), "?\\Ferro\\Uuid"),
            (Some(tag::JSON), "?\\Ferro\\Json"),
        ];
        // All fourteen implemented tags (0..=13) plus "no tag".
        assert_eq!(want.len(), 15);
        for (t, ty) in want {
            assert_eq!(php_type(*t), Some(*ty), "tag {t:?}");
        }
        for t in [tag::ARRAY, tag::INTERVAL, tag::INET, tag::VECTOR, 200] {
            assert_eq!(php_type(Some(t)), None, "tag {t} is not decodable");
        }
    }

    #[test]
    fn class_names_are_checked_as_php_does() {
        assert!(check_fqcn("App\\Dto\\User").is_ok());
        assert!(check_fqcn("App\\9x").is_err());
        assert!(check_fqcn("").is_err());
        assert!(check_fqcn("App\\\\User").is_err());
        for reserved in [
            "List",
            "Parent",
            "exit",
            "DIE",
            "eval",
            "include_once",
            "require_once",
            "insteadof",
            "endif",
            "endfor",
            "endforeach",
            "endwhile",
            "endswitch",
            "enddeclare",
            "__halt_compiler",
            "__CLASS__",
            "__LINE__",
            "Readonly",
            "finally",
        ] {
            assert!(
                check_fqcn(&format!("App\\{reserved}")).is_err(),
                "{reserved}"
            );
        }
        // Accepted by PHP 8.4 as class names.
        for fine in ["Enum", "From", "Resource", "Numeric"] {
            assert!(check_fqcn(&format!("App\\{fine}")).is_ok(), "{fine}");
        }
        // Reserved words ARE legal namespace segments in PHP 8.
        assert!(check_fqcn("App\\List\\Class\\Row").is_ok());
        assert!(check_fqcn("List\\Row").is_ok());
        assert!(check_fqcn("Namespace\\Row").is_err());
        assert!(check_fqcn("__halt_compiler\\Row").is_err());
        assert!(check_fqcn("__halt_compiler\\X\\Row").is_ok());
    }

    /// The class-name rule against `php -l` itself.
    #[test]
    fn class_name_check_agrees_with_php_lint_when_php_is_available() {
        if php().is_none() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ferro-gen-lint-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cases: Vec<String> = RESERVED_CLASS.iter().map(|w| format!("App\\{w}")).collect();
        cases.extend(
            [
                "App\\Enum",
                "App\\From",
                "App\\List\\Row",
                "List\\Row",
                "Namespace\\Row",
                "__halt_compiler\\Row",
                "__halt_compiler\\X\\Row",
                "App\\Row",
            ]
            .map(String::from),
        );
        for fq in &cases {
            let (ns, short) = split_fqcn(fq);
            let src = format!(
                "<?php namespace {}; final readonly class {short} {{}}\n",
                ns.unwrap()
            );
            let f = dir.join("t.php");
            std::fs::write(&f, src).unwrap();
            let lint = std::process::Command::new("php")
                .arg("-l")
                .arg(&f)
                .output()
                .unwrap();
            assert_eq!(
                lint.status.success(),
                check_fqcn(fq).is_ok(),
                "`{fq}`: php -l and check_fqcn disagree"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn files(names: &[&str]) -> Vec<File> {
        names
            .iter()
            .map(|n| File {
                name: n.to_string(),
                contents: format!("<{n}>"),
            })
            .collect()
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ferro-gen-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn listing(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn a_failed_rename_is_reported_as_partial_naming_what_was_written() {
        let dir = scratch("partial");
        let calls = std::cell::Cell::new(0);
        let r = write_out(
            &dir,
            &files(&["A.php", "B.php", "manifest.json"]),
            |a, b| {
                calls.set(calls.get() + 1);
                // Call 1 is the marker; call 2 places A.php; call 3 (B.php) fails.
                if calls.get() == 3 {
                    Err(std::io::Error::other("injected"))
                } else {
                    std::fs::rename(a, b)
                }
            },
        );
        match r {
            Err(WriteError::Partial {
                written,
                not_written,
                ..
            }) => {
                assert_eq!(written, ["A.php"]);
                assert_eq!(not_written, ["B.php", "manifest.json"]);
            }
            other => panic!("{other:?}"),
        }
        // No temporary is left behind, and the marker already lists every file it may have placed.
        assert_eq!(listing(&dir), [MARKER, "A.php"]);
        let marker = std::fs::read_to_string(dir.join(MARKER)).unwrap();
        for n in ["A.php", "B.php", "manifest.json"] {
            assert!(marker.lines().any(|l| l == n), "{marker}");
        }
        let _ = std::fs::remove_dir_all(&dir);

        // Failing on the FIRST target replaces nothing: that is not a partial update.
        let calls = std::cell::Cell::new(0);
        let r = write_out(&dir, &files(&["A.php"]), |a, b| {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(std::io::Error::other("injected"))
            } else {
                std::fs::rename(a, b)
            }
        });
        assert!(matches!(r, Err(WriteError::Untouched(_))), "{r:?}");
        assert_eq!(listing(&dir), [MARKER]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stale_files_listed_by_the_previous_run_are_removed_and_nothing_else() {
        let dir = scratch("stale");
        write_out(&dir, &files(&["Old.php", "Keep.php"]), |a, b| {
            std::fs::rename(a, b)
        })
        .unwrap();
        std::fs::write(dir.join("Mine.php"), "hand-written").unwrap();
        // A hand-edited marker naming something gen could never have generated is ignored.
        let mut m = std::fs::read_to_string(dir.join(MARKER)).unwrap();
        m.push_str("../escape.php\nMine.txt\n");
        std::fs::write(dir.join(MARKER), m).unwrap();
        std::fs::write(dir.join("Mine.txt"), "hand-written").unwrap();
        let w = write_out(&dir, &files(&["Keep.php", "New.php"]), |a, b| {
            std::fs::rename(a, b)
        })
        .unwrap();
        assert_eq!(w.removed, ["Old.php"]);
        assert_eq!(
            listing(&dir),
            [MARKER, "Keep.php", "Mine.php", "Mine.txt", "New.php"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
