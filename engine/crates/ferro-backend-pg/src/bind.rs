//! Canonical [`Value`] params → tokio-postgres `ToSql`, for every M0 scalar incl. `Null` and
//! `Bytes`.
//!
//! Each param becomes a `Box<dyn ToSql + Sync + Send>` (which `BorrowToSql` accepts), so the boxed Vec
//! can be handed straight to `Client::query_raw` as an `ExactSizeIterator`. `Value::Null` is the
//! subtle one: a NULL has no canonical Rust type, so it is bound via [`PgNull`], a `ToSql` that
//! `accepts` EVERY type and always writes `IsNull::Yes`. That sidesteps the usual "which
//! `Option::<T>::None`?" problem — with a prepared statement PG has already fixed each param's
//! type, and `PgNull` writes a typed NULL slot for whatever that type is.

use ferro_proto::value::Value;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

/// A type-agnostic SQL `NULL`. `accepts` returns `true` for any `Type`, and `to_sql` writes no
/// bytes and reports `IsNull::Yes`, so it binds a NULL for whatever type the prepared statement
/// assigned the parameter — no need to know the concrete type at bind time.
#[derive(Debug)]
struct PgNull;

impl ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &Type,
        _out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Converts canonical params into boxed `ToSql` values ready for `query_raw`. Owned (cloned
/// `String`/`Vec<u8>`) so the boxes outlive the query without borrowing the caller's slice.
pub fn to_boxed_params(params: &[Value]) -> Vec<Box<dyn ToSql + Sync + Send>> {
    params.iter().map(value_to_boxed).collect()
}

fn value_to_boxed(v: &Value) -> Box<dyn ToSql + Sync + Send> {
    match v {
        Value::Null => Box::new(PgNull),
        Value::Bool(b) => Box::new(*b),
        Value::I64(n) => Box::new(*n),
        Value::F64(f) => Box::new(*f),
        Value::Text(s) => Box::new(s.clone()),
        Value::Bytes(b) => Box::new(b.clone()),
    }
}

/// Whether the concrete `ToSql` impl `value_to_boxed` would box this `Value` as `accepts` the
/// prepared statement's inferred `Type` for this parameter slot.
///
/// This is the **pre-flight of the exact bind `query_raw` will perform** — it MUST mirror
/// `value_to_boxed` arm-for-arm, because `query_raw`'s own `to_sql_checked` calls `accepts` on
/// precisely these concrete types. `query.rs` runs this BEFORE sending the statement so a bind
/// error (an uncastable param — the canonical `Value::I64`→`i64`→`int8` bound against an
/// `int4`/serial column being the common one) surfaces as a KNOWN-FATE error (the statement
/// provably never executed), never the fate-unknown `ConnectionLost` a post-send transport failure
/// yields. Surfacing it as `ConnectionLost` is exactly what would let the SQL service mint a FALSE
/// `WriteUnconfirmed{Indeterminate}` for a write that never happened (§19.3).
///
/// `Value::Null` accepts every type: it is bound via [`PgNull`], whose `accepts` is `true` for any
/// `Type`, so a NULL never mis-binds.
pub fn accepts(v: &Value, ty: &Type) -> bool {
    match v {
        Value::Null => true,
        Value::Bool(_) => <bool as ToSql>::accepts(ty),
        Value::I64(_) => <i64 as ToSql>::accepts(ty),
        Value::F64(_) => <f64 as ToSql>::accepts(ty),
        Value::Text(_) => <String as ToSql>::accepts(ty),
        Value::Bytes(_) => <Vec<u8> as ToSql>::accepts(ty),
    }
}

/// The canonical-type label for a `Value`, used only to build a clear diagnostic bind-error
/// message ("parameter N: canonical I64 cannot bind to PG type int4 …").
pub fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOL",
        Value::I64(_) => "I64",
        Value::F64(_) => "F64",
        Value::Text(_) => "TEXT",
        Value::Bytes(_) => "BYTES",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_all_m0_scalars() {
        let params = [
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("x".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
        ];
        let boxed = to_boxed_params(&params);
        assert_eq!(boxed.len(), 6, "one boxed ToSql per param");
    }

    #[test]
    fn pgnull_accepts_any_type_and_is_null() {
        assert!(<PgNull as ToSql>::accepts(&Type::TEXT));
        assert!(<PgNull as ToSql>::accepts(&Type::INT4));
        let mut buf = tokio_postgres::types::private::BytesMut::new();
        let is_null = PgNull.to_sql(&Type::TEXT, &mut buf).unwrap();
        assert!(matches!(is_null, IsNull::Yes));
        assert!(buf.is_empty(), "a NULL writes no value bytes");
    }

    /// `accepts` mirrors `value_to_boxed`: it is the pre-flight of the exact bind `query_raw`
    /// performs. The load-bearing case is `I64` vs `int4` — the common false-Indeterminate trigger
    /// (a canonical `I64` bound against an `int4`/serial PK): `i64` boxes as `int8`, which does NOT
    /// accept `int4`. Offline (no Docker) proof of the COMMIT-1 fix's core predicate.
    #[test]
    fn accepts_mirrors_boxed_binding() {
        // The trigger: I64 -> i64 -> int8 does NOT accept int4/int2 (narrower column).
        assert!(accepts(&Value::I64(1), &Type::INT8));
        assert!(!accepts(&Value::I64(1), &Type::INT4));
        assert!(!accepts(&Value::I64(1), &Type::INT2));
        // F64 -> f64 -> float8 does NOT accept float4.
        assert!(accepts(&Value::F64(1.0), &Type::FLOAT8));
        assert!(!accepts(&Value::F64(1.0), &Type::FLOAT4));
        // The straightforward same-type binds accept.
        assert!(accepts(&Value::Bool(true), &Type::BOOL));
        assert!(accepts(&Value::Text("x".to_string()), &Type::TEXT));
        assert!(accepts(&Value::Text("x".to_string()), &Type::VARCHAR));
        assert!(accepts(&Value::Bytes(vec![0xde]), &Type::BYTEA));
        // NULL binds against anything (PgNull::accepts is universally true).
        assert!(accepts(&Value::Null, &Type::INT4));
        assert!(accepts(&Value::Null, &Type::TEXT));
        // A canonical mismatch is caught (Text cannot bind int4).
        assert!(!accepts(&Value::Text("x".to_string()), &Type::INT4));
    }

    #[test]
    fn value_kind_labels_each_variant() {
        assert_eq!(value_kind(&Value::Null), "NULL");
        assert_eq!(value_kind(&Value::I64(1)), "I64");
        assert_eq!(value_kind(&Value::F64(1.0)), "F64");
        assert_eq!(value_kind(&Value::Text(String::new())), "TEXT");
        assert_eq!(value_kind(&Value::Bytes(vec![])), "BYTES");
        assert_eq!(value_kind(&Value::Bool(true)), "BOOL");
    }
}
