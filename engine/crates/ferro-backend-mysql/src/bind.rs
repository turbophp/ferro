//! Canonical [`Value`] params → `mysql_async::Params` (positional), plus the ARITY pre-check.
//!
//! Unlike Postgres (whose prepared statement carries a server-inferred type per `$n`, so PG's
//! `bind` can pre-flight each param's `ToSql::accepts`), a MySQL `COM_STMT_PREPARE` exposes NO
//! inferred parameter types — every `?` is reported as an opaque placeholder. So the ONLY client-side
//! bind validation possible here is the parameter COUNT. A mismatch is a KNOWN-FATE
//! [`PoolError::Unsupported`] (the statement provably never executed) — deliberately NOT
//! `PoolError::ConnectionLost`, whose fate is unknown and would let the SQL service mint a false
//! `WriteUnconfirmed{Indeterminate}` for a write that never happened (§19.3, the same no-false-
//! Indeterminate safety PG's `bind`/`query` pre-validation enforces).
//!
//! The canonical scalar → `mysql_common::Value` conversion itself is TOTAL — every `Value` variant
//! has an exact MySQL representation — so it never fails; there is no lossy or fallible arm that
//! could produce a fate-unknown error.

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use mysql_async::{Params, Value as MyValue};

use crate::Value;

/// Validate that the supplied param count matches the prepared statement's placeholder count. A
/// mismatch is a KNOWN-FATE bind error (`Sql{Unsupported}`) — never the fate-unknown
/// `ConnectionLost` a post-send transport failure produces (§19.3). Raised BEFORE anything is sent,
/// so the connection stays clean and usable.
pub fn validate_arity(params: &[Value], num_params: usize) -> Result<(), PoolError> {
    if params.len() != num_params {
        return Err(bind_error(format!(
            "parameter count mismatch: got {}, statement expects {num_params}",
            params.len()
        )));
    }
    Ok(())
}

/// Convert canonical params into `mysql_async::Params`. Positional (MySQL's native `?` binding);
/// an empty slice is `Params::Empty`. The conversion is total (never fails) — see the module docs.
pub fn to_params(params: &[Value]) -> Params {
    if params.is_empty() {
        return Params::Empty;
    }
    Params::Positional(params.iter().map(value_to_my).collect())
}

/// The canonical → driver `Value` mapping. `Bool` binds as the MySQL boolean idiom (`0`/`1` integer);
/// `Text` and `Bytes` both bind as `Bytes` (MySQL has one byte-string param form — the target
/// column's type decides the interpretation server-side).
fn value_to_my(v: &Value) -> MyValue {
    match v {
        Value::Null => MyValue::NULL,
        Value::Bool(b) => MyValue::Int(*b as i64),
        Value::I64(n) => MyValue::Int(*n),
        Value::F64(f) => MyValue::Double(*f),
        Value::Text(s) => MyValue::Bytes(s.clone().into_bytes()),
        Value::Bytes(b) => MyValue::Bytes(b.clone()),
    }
}

/// A KNOWN-FATE bind error: `PoolError::Sql` carrying the `Unsupported` code/branch. Deliberately
/// NOT `ConnectionLost` — a bind fault is caught before the statement is sent, so its fate is known
/// (it never executed) and the SQL service applies NO `readonly`→`Indeterminate` override to it.
/// `sqlstate` is `None`: the server never saw the statement. (Mirrors PG's `query::bind_error`.)
fn bind_error(message: String) -> PoolError {
    PoolError::Sql {
        code: errc::UNSUPPORTED,
        branch: errc::UNSUPPORTED_BRANCH,
        sqlstate: None,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arity_match_ok_mismatch_is_known_fate_unsupported() {
        assert!(validate_arity(&[Value::I64(1)], 1).is_ok());
        assert!(validate_arity(&[], 0).is_ok());

        // Too few and too many both fail — and as a KNOWN-FATE Sql{Unsupported}, never ConnectionLost.
        for (params, expected) in [
            (vec![], 1usize),
            (vec![Value::I64(1)], 0usize),
            (vec![Value::I64(1), Value::Null], 1usize),
        ] {
            match validate_arity(&params, expected) {
                Err(PoolError::Sql {
                    code, branch: b, ..
                }) => {
                    assert_eq!(code, errc::UNSUPPORTED);
                    assert_eq!(b, errc::UNSUPPORTED_BRANCH);
                }
                Err(PoolError::ConnectionLost) => {
                    panic!("REGRESSION: an arity mismatch must NEVER be ConnectionLost (§19.3)")
                }
                other => panic!("expected Sql{{Unsupported}}, got {other:?}"),
            }
        }
    }

    #[test]
    fn to_params_maps_every_scalar_positionally() {
        // Empty → Params::Empty (MySQL's no-param form).
        assert!(matches!(to_params(&[]), Params::Empty));

        let params = [
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("hi".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
        ];
        match to_params(&params) {
            Params::Positional(vs) => {
                assert_eq!(vs.len(), 6, "one driver Value per canonical param");
                assert!(matches!(vs[0], MyValue::NULL));
                assert!(
                    matches!(vs[1], MyValue::Int(1)),
                    "Bool(true) binds as Int(1)"
                );
                assert!(matches!(vs[2], MyValue::Int(-200)));
                assert!(matches!(vs[3], MyValue::Double(_)));
                assert!(
                    matches!(vs[4], MyValue::Bytes(_)),
                    "Text binds as a byte string"
                );
                assert!(matches!(vs[5], MyValue::Bytes(_)));
            }
            other => panic!("expected Params::Positional, got {other:?}"),
        }
    }
}
