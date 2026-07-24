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
}
