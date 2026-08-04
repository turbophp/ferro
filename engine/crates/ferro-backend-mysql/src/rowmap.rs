//! MySQL/MariaDB column-type → canonical `Value` mapping for the M1-S6 scoped scalar set, plus the
//! binary-protocol cell extraction. The MySQL counterpart of `ferro-backend-pg`'s `rowmap` (which
//! keys off PG OIDs); here we key off the wire **column metadata** (`ColumnType` + `ColumnFlags` +
//! charset + display length), because a MySQL binary-protocol `Value` alone is ambiguous:
//!
//! * TEXT, VARCHAR, CHAR, BLOB, VARBINARY, BINARY and DECIMAL all arrive as `Value::Bytes` — only
//!   the column's charset (binary collation `63` = a byte string) and type tell them apart, and
//!   DECIMAL is deliberately out of scope (a loud `Unsupported`, never a silent `Text` miscast);
//! * a signed and an unsigned integer both arrive as `Value::Int`/`Value::UInt` — the `UNSIGNED_FLAG`
//!   is what makes `BIGINT UNSIGNED` an out-of-scope `Unsupported` (the unsigned-64 policy is
//!   deferred, SPEC §9) rather than a lossy cast into the signed `I64` domain;
//! * `TINYINT(1)` is MySQL's `BOOLEAN` convention — the prepared-statement column metadata keeps its
//!   display length `1` (integer display widths are otherwise deprecated in MySQL 8, but `TINYINT(1)`
//!   is retained precisely for the boolean idiom), so `TINYINT` with display length `1` maps to
//!   `Bool` and any wider signed `TINYINT` folds into `I64`.
//!
//! [`column_kind`] is the ONE classifier both [`column_to_tag`] (for `ColMeta`, built off the
//! prepared statement BEFORE the query runs — an out-of-scope column errors loudly while the conn is
//! still clean) and [`extract_value`] share, exactly as PG's `oid_extract_type` backs both
//! `oid_to_tag` and `extract_value`. Out-of-scope types (unsigned integers, `DECIMAL`, the
//! date/time family, `JSON`, `BIT`, `ENUM`/`SET`, `GEOMETRY`, `VECTOR`, …) are a LOUD
//! `PoolError::Unsupported` NAMING the type — never a silent miscast. A cell whose driver `Value`
//! variant does not match its column kind is a client-side decode mismatch surfaced as
//! `PoolError::Backend` (NonRetryable) — NEVER `ConnectionLost`, so it can never mint a false
//! §19.3 `Indeterminate`.

use ferro_pool::error::PoolError;
use ferro_proto::consts::tag;
use mysql_async::Column;
use mysql_async::Value as MyValue;
use mysql_async::consts::{ColumnFlags, ColumnType};

use crate::Value;

/// The MySQL "binary" collation id. A string/blob column reporting this collation is a byte string
/// (`BINARY`/`VARBINARY`/`BLOB`); any other collation on the same column type is character data
/// (`CHAR`/`VARCHAR`/`TEXT`). Stable across MySQL and MariaDB.
const BINARY_COLLATION_ID: u16 = 63;

/// The canonical scalar a MySQL column maps to (the scoped M1-S6 set). Split out from `Value` so the
/// classification is unit-testable on its own (mirrors PG's `ExtractType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyKind {
    /// `TINYINT(1)` — MySQL's `BOOLEAN`.
    Bool,
    /// Signed `TINYINT`(≠1)/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT` → `Value::I64`.
    I64,
    /// `FLOAT`/`DOUBLE` → `Value::F64`.
    F64,
    /// `CHAR`/`VARCHAR`/`TEXT` (a non-binary charset) → `Value::Text`.
    Text,
    /// `BINARY`/`VARBINARY`/`BLOB` (the binary collation) → `Value::Bytes`.
    Bytes,
}

/// Classify a MySQL column into its scoped canonical [`MyKind`], or a LOUD `Unsupported` naming the
/// out-of-scope type. This is the single source of truth for BOTH the `ColMeta` tag and the cell
/// extraction, so `cols` and `rows` can never disagree on a column's type.
pub fn column_kind(col: &Column) -> Result<MyKind, PoolError> {
    let ct = col.column_type();
    let flags = col.flags();
    let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    match ct {
        // TINYINT: the BOOLEAN idiom (display length 1) is `Bool`; a wider signed TINYINT is I64;
        // an unsigned TINYINT is out of scope (the unsigned-integer policy is deferred wholesale).
        ColumnType::MYSQL_TYPE_TINY => {
            if unsigned {
                Err(unsupported(col, "unsigned TINYINT"))
            } else if col.column_length() == 1 {
                Ok(MyKind::Bool)
            } else {
                Ok(MyKind::I64)
            }
        }
        // The remaining signed integer widths → I64. Unsigned → out of scope (no lossy cast into
        // the signed I64 domain — a `BIGINT UNSIGNED` value can exceed `i64::MAX`).
        ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_LONGLONG => {
            if unsigned {
                Err(unsupported(
                    col,
                    "unsigned integer (the unsigned-64 policy is deferred, SPEC §9)",
                ))
            } else {
                Ok(MyKind::I64)
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => Ok(MyKind::F64),
        // The string/blob family: charset decides Text vs Bytes. ENUM/SET arrive here (as STRING
        // with a flag) but are out of scope — reject them loudly rather than treat as Text.
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            if flags.contains(ColumnFlags::ENUM_FLAG) {
                return Err(unsupported(col, "ENUM"));
            }
            if flags.contains(ColumnFlags::SET_FLAG) {
                return Err(unsupported(col, "SET"));
            }
            if col.character_set() == BINARY_COLLATION_ID {
                Ok(MyKind::Bytes)
            } else {
                Ok(MyKind::Text)
            }
        }
        // Everything else is out of scope in M1: DECIMAL/NEWDECIMAL, DATE/DATETIME/TIMESTAMP/TIME/
        // YEAR, JSON, BIT, GEOMETRY, VECTOR, the NULL column type, … A loud, diagnosable error.
        _ => Err(unsupported(col, "out-of-scope column type")),
    }
}

/// Map a MySQL column to its canonical `Value` tag (for `ColMeta`). `Unsupported` for any
/// out-of-scope type — a loud typed error, never a silent miscast (the MySQL analog of PG's
/// `oid_to_tag`).
pub fn column_to_tag(col: &Column) -> Result<u8, PoolError> {
    Ok(match column_kind(col)? {
        MyKind::Bool => tag::BOOL,
        MyKind::I64 => tag::I64,
        MyKind::F64 => tag::F64,
        MyKind::Text => tag::TEXT,
        MyKind::Bytes => tag::BYTES,
    })
}

/// Extract a single binary-protocol cell (`value` for column `col`) into a canonical `Value`. A SQL
/// `NULL` is `Value::Null` regardless of column type. An out-of-scope column is `Unsupported`; a
/// driver `Value` variant that does not match the column kind is a client-side decode mismatch
/// surfaced as `Backend` (NonRetryable) — NEVER `ConnectionLost` (no false §19.3 Indeterminate).
pub fn extract_value(value: &MyValue, col: &Column) -> Result<Value, PoolError> {
    if matches!(value, MyValue::NULL) {
        return Ok(Value::Null);
    }
    match column_kind(col)? {
        MyKind::Bool => Ok(Value::Bool(as_i64(value, col)? != 0)),
        MyKind::I64 => Ok(Value::I64(as_i64(value, col)?)),
        MyKind::F64 => Ok(Value::F64(as_f64(value, col)?)),
        MyKind::Text => {
            let bytes = as_bytes(value, col)?;
            String::from_utf8(bytes)
                .map(Value::Text)
                .map_err(|e| decode_err(col, format!("invalid UTF-8 in a TEXT column: {e}")))
        }
        MyKind::Bytes => Ok(Value::Bytes(as_bytes(value, col)?)),
    }
}

/// A signed-integer cell: the binary protocol yields `Int` for a signed column. `UInt` in the
/// i64 range is accepted defensively; anything else (or an out-of-range `UInt`) is a decode
/// mismatch.
fn as_i64(value: &MyValue, col: &Column) -> Result<i64, PoolError> {
    match value {
        MyValue::Int(n) => Ok(*n),
        MyValue::UInt(n) if *n <= i64::MAX as u64 => Ok(*n as i64),
        other => Err(decode_err(
            col,
            format!("expected a signed integer cell, got {other:?}"),
        )),
    }
}

/// A floating cell: `Double` for `DOUBLE`, `Float` widened for `FLOAT`.
fn as_f64(value: &MyValue, col: &Column) -> Result<f64, PoolError> {
    match value {
        MyValue::Double(f) => Ok(*f),
        MyValue::Float(f) => Ok(*f as f64),
        other => Err(decode_err(
            col,
            format!("expected a floating cell, got {other:?}"),
        )),
    }
}

/// A byte-string cell: the binary protocol yields `Bytes` for every string/blob column.
fn as_bytes(value: &MyValue, col: &Column) -> Result<Vec<u8>, PoolError> {
    match value {
        MyValue::Bytes(b) => Ok(b.clone()),
        other => Err(decode_err(
            col,
            format!("expected a byte-string cell, got {other:?}"),
        )),
    }
}

/// A loud out-of-scope-type error naming the column + its MySQL type (never a silent miscast).
fn unsupported(col: &Column, what: &str) -> PoolError {
    PoolError::Unsupported(format!(
        "out-of-scope MySQL column type for '{}' ({what}; ColumnType={:?}, flags={:?}, charset={}); \
         only NULL/BOOL/I64/F64/TEXT/BYTES are supported in M1",
        col.name_str(),
        col.column_type(),
        col.flags(),
        col.character_set(),
    ))
}

/// A client-side decode mismatch (the driver `Value` did not match the column kind). Surfaced as
/// `Backend` — NOT `ConnectionLost` — so a decode bug can never mint a false §19.3 `Indeterminate`
/// (parity with PG's `rowmap::get_opt`, which routes `try_get` failures to `Backend`).
fn decode_err(col: &Column, detail: String) -> PoolError {
    PoolError::Backend(format!("column '{}' decode: {detail}", col.name_str()))
}
