//! PG OID → canonical `Value` mapping for the M0 scalar set (T-1) and the OID-strict row
//! extraction (MAJOR-8).
//!
//! tokio-postgres `FromSql` is **OID-strict**: `try_get::<_, i64>` accepts ONLY `int8`. `SELECT 1`
//! returns `int4`, so extraction MUST be driven off each column's ACTUAL OID — read it into the
//! Rust type that matches the OID, then widen into the canonical [`Value`]. Getting this wrong is
//! exactly the headline bug this module exists to prevent.
//!
//! Two tables, both keyed on the raw OID:
//! - [`oid_to_tag`] → the canonical `Value` tag for `ColMeta` (loud `Unsupported` outside the M0
//!   set — never a silent miscast);
//! - [`oid_extract_type`] → which Rust `FromSql` type to read the column as.
//!
//! Both are unit-tested (no Docker) against the `Type` OID constants, incl. an out-of-M0 OID.

use ferro_pool::error::PoolError;
use ferro_proto::consts::tag;
use tokio_postgres::Row;
use tokio_postgres::types::{Oid, Type};

/// The Rust `FromSql` type a given PG OID must be read as (before widening into a canonical
/// `Value`). Split out from `Value` so the OID→extraction-type table is unit-testable on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractType {
    Bool,
    /// `int2` — read as `i16`, widened to `Value::I64`.
    I16,
    /// `int4` — read as `i32`, widened to `Value::I64` (this is the `SELECT 1` case).
    I32,
    /// `int8` — read as `i64`.
    I64,
    /// `float4` — read as `f32`, widened to `Value::F64`.
    F32,
    /// `float8` — read as `f64`.
    F64,
    /// `text`/`varchar`/`bpchar` — read as `String`.
    Text,
    /// `bytea` — read as `Vec<u8>`.
    Bytes,
}

/// Maps a PG column OID to the canonical `Value` tag (for `ColMeta`). Returns
/// `PoolError::Unsupported` for any OID outside the M0 scalar set — a loud typed error, never a
/// silent miscast (T-1). The message names the OID so an out-of-M0 column is diagnosable.
pub fn oid_to_tag(oid: Oid) -> Result<u8, PoolError> {
    match oid_extract_type(oid) {
        Some(ExtractType::Bool) => Ok(tag::BOOL),
        Some(ExtractType::I16 | ExtractType::I32 | ExtractType::I64) => Ok(tag::I64),
        Some(ExtractType::F32 | ExtractType::F64) => Ok(tag::F64),
        Some(ExtractType::Text) => Ok(tag::TEXT),
        Some(ExtractType::Bytes) => Ok(tag::BYTES),
        None => Err(unsupported_oid(oid)),
    }
}

/// Maps a PG column OID to the Rust type extraction must use. `None` ⇒ out-of-M0 (reserved types
/// like `timestamptz`/`uuid`/`numeric`/`json` — implemented in M1+).
pub fn oid_extract_type(oid: Oid) -> Option<ExtractType> {
    match oid {
        o if o == Type::BOOL.oid() => Some(ExtractType::Bool),
        o if o == Type::INT2.oid() => Some(ExtractType::I16),
        o if o == Type::INT4.oid() => Some(ExtractType::I32),
        o if o == Type::INT8.oid() => Some(ExtractType::I64),
        o if o == Type::FLOAT4.oid() => Some(ExtractType::F32),
        o if o == Type::FLOAT8.oid() => Some(ExtractType::F64),
        o if o == Type::TEXT.oid() || o == Type::VARCHAR.oid() || o == Type::BPCHAR.oid() => {
            Some(ExtractType::Text)
        }
        o if o == Type::BYTEA.oid() => Some(ExtractType::Bytes),
        _ => None,
    }
}

/// Extracts column `idx` of `row` (whose OID is `oid`) into a canonical `Value`, OID-strict. A
/// SQL `NULL` in any column becomes `Value::Null` (read as `Option<T>` — `None`, never `WasNull`).
/// An out-of-M0 OID is `Unsupported`; a `try_get` failure on an in-set OID is a client-side
/// decode mismatch (NOT a connection loss), surfaced as `Backend` (NonRetryable).
pub fn extract_value(row: &Row, idx: usize, oid: Oid) -> Result<crate::Value, PoolError> {
    use crate::Value;
    match oid_extract_type(oid) {
        Some(ExtractType::Bool) => Ok(get_opt::<bool>(row, idx)?.map_or(Value::Null, Value::Bool)),
        Some(ExtractType::I16) => {
            Ok(get_opt::<i16>(row, idx)?.map_or(Value::Null, |n| Value::I64(n as i64)))
        }
        Some(ExtractType::I32) => {
            Ok(get_opt::<i32>(row, idx)?.map_or(Value::Null, |n| Value::I64(n as i64)))
        }
        Some(ExtractType::I64) => Ok(get_opt::<i64>(row, idx)?.map_or(Value::Null, Value::I64)),
        Some(ExtractType::F32) => {
            Ok(get_opt::<f32>(row, idx)?.map_or(Value::Null, |f| Value::F64(f as f64)))
        }
        Some(ExtractType::F64) => Ok(get_opt::<f64>(row, idx)?.map_or(Value::Null, Value::F64)),
        Some(ExtractType::Text) => {
            Ok(get_opt::<String>(row, idx)?.map_or(Value::Null, Value::Text))
        }
        Some(ExtractType::Bytes) => {
            Ok(get_opt::<Vec<u8>>(row, idx)?.map_or(Value::Null, Value::Bytes))
        }
        None => Err(unsupported_oid(oid)),
    }
}

/// `row.try_get::<usize, Option<T>>(idx)` with a NON-connection error mapping. A `try_get` error
/// here is a client-side type-conversion mismatch (`as_db_error()` is `None` for it), so routing
/// it through `error_map` would MISCLASSIFY it as `ConnectionLost`; it is a `Backend` error.
fn get_opt<'a, T>(row: &'a Row, idx: usize) -> Result<Option<T>, PoolError>
where
    T: tokio_postgres::types::FromSql<'a>,
{
    row.try_get::<usize, Option<T>>(idx)
        .map_err(|e| PoolError::Backend(format!("column {idx} decode: {e}")))
}

fn unsupported_oid(oid: Oid) -> PoolError {
    PoolError::Unsupported(format!(
        "out-of-M0 column type (PG OID {oid}); only NULL/BOOL/I64/F64/TEXT/BYTES are supported in M0"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_to_tag_covers_m0_scalar_set() {
        assert_eq!(oid_to_tag(Type::BOOL.oid()).unwrap(), tag::BOOL);
        assert_eq!(oid_to_tag(Type::INT2.oid()).unwrap(), tag::I64);
        assert_eq!(oid_to_tag(Type::INT4.oid()).unwrap(), tag::I64);
        assert_eq!(oid_to_tag(Type::INT8.oid()).unwrap(), tag::I64);
        assert_eq!(oid_to_tag(Type::FLOAT4.oid()).unwrap(), tag::F64);
        assert_eq!(oid_to_tag(Type::FLOAT8.oid()).unwrap(), tag::F64);
        assert_eq!(oid_to_tag(Type::TEXT.oid()).unwrap(), tag::TEXT);
        assert_eq!(oid_to_tag(Type::VARCHAR.oid()).unwrap(), tag::TEXT);
        assert_eq!(oid_to_tag(Type::BPCHAR.oid()).unwrap(), tag::TEXT);
        assert_eq!(oid_to_tag(Type::BYTEA.oid()).unwrap(), tag::BYTES);
    }

    #[test]
    fn oid_extract_type_matches_tag_widening() {
        assert_eq!(oid_extract_type(Type::INT2.oid()), Some(ExtractType::I16));
        assert_eq!(oid_extract_type(Type::INT4.oid()), Some(ExtractType::I32));
        assert_eq!(oid_extract_type(Type::INT8.oid()), Some(ExtractType::I64));
        assert_eq!(oid_extract_type(Type::FLOAT4.oid()), Some(ExtractType::F32));
        assert_eq!(oid_extract_type(Type::FLOAT8.oid()), Some(ExtractType::F64));
        assert_eq!(oid_extract_type(Type::BOOL.oid()), Some(ExtractType::Bool));
        assert_eq!(oid_extract_type(Type::TEXT.oid()), Some(ExtractType::Text));
        assert_eq!(
            oid_extract_type(Type::BYTEA.oid()),
            Some(ExtractType::Bytes)
        );
    }

    #[test]
    fn out_of_m0_oid_is_unsupported() {
        // timestamptz / uuid / numeric / json are reserved but unimplemented in M0.
        for ty in [Type::TIMESTAMPTZ, Type::UUID, Type::NUMERIC, Type::JSONB] {
            assert_eq!(oid_extract_type(ty.oid()), None, "{ty:?} must be out-of-M0");
            assert!(
                matches!(oid_to_tag(ty.oid()), Err(PoolError::Unsupported(_))),
                "{ty:?} tag must be Unsupported"
            );
        }
    }
}
