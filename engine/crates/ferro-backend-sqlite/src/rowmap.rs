//! SQLite's five storage classes ↔ the §9 type tags (C3-3c).
//!
//! # The decision: the VALUE's storage class, not the column's declared type
//!
//! SQLite is dynamically typed. A declared column type is only an AFFINITY and is not enforced, so
//! `CREATE TABLE t(v INTEGER)` will store the string `'abc'` in `v` quite happily. That makes the
//! usual "map the column type once, apply it to every cell" shape — which both the PG and MySQL
//! backends use — unavailable here. **Measured** on a table with one row per shape:
//!
//! ```text
//! DECL: i:INTEGER | r:REAL | tx:TEXT | b:BLOB | n:NUMERIC | none_col:<NONE>
//! ROW1: INT       | REAL   | TEXT    | BLOB   | INT       | INT
//! ROW2: TEXT      | TEXT   | TEXT    | TEXT   | TEXT      | TEXT
//! ```
//!
//! Two facts fall out, and between them they rule out keying on the declared type at all:
//!
//! 1. **A single column's storage class CHANGES between rows** (`i` is INTEGER then TEXT), so no
//!    one per-column tag can describe the cells.
//! 2. **Every EXPRESSION column reports no declared type.** `SELECT 1+1`, `SELECT 'lit'`,
//!    `count(*)` all report `<NONE>`, so for a large and ordinary class of queries the declared
//!    type carries no information whatsoever. (It also requires rusqlite's opt-in
//!    `column_decltype` feature, which is a hint about how central SQLite considers it.)
//!
//! So each cell is tagged from its own storage class, which is always present and always truthful.
//!
//! # Why that costs nothing, which corrects the obvious worry
//!
//! The worry is that per-cell tagging makes the `cols` header a lie when a column is
//! heterogeneous. It would — except **`ColMeta.tag` has no consumer**. The PHP client drops it
//! deliberately in BOTH the buffered and streaming paths, saying so in its own comment
//! (`Connection.php`, F25/hazard 47): *"The decode authority is the PER-CELL tag … not the column
//! metadata."* Nothing in `ferrod` reads it either. Every cell is self-describing on the wire, so
//! per-cell tagging is not merely acceptable, it is what the client already relies on.
//!
//! `ColMeta.tag` is therefore filled from the FIRST row's storage class (and `NULL` for an empty
//! result), which describes the data actually returned rather than a declaration SQLite does not
//! enforce. It is advisory; the per-cell tags are authoritative.
//!
//! # Nine of the fourteen §9 tags are UNREACHABLE from a SQLite read, by construction
//!
//! Not unimplemented — unreachable. SQLite has five storage classes and none of them carries the
//! information the other nine tags require. Named here rather than papered over, following S7's
//! precedent (no MySQL-family backend ever emits `UUID`; MariaDB `JSON` is a `LONGTEXT` alias).
//!
//! | tag | why unreachable |
//! | --- | --- |
//! | `BOOL` | SQLite has no boolean type; `0`/`1` are INTEGER and read back as `I64`. Recovering `Bool` would need the declared type, which SQLite does not enforce. |
//! | `U64` | SQLite's INTEGER is a SIGNED 64-bit value. There is no unsigned storage class. |
//! | `DECIMAL` | No decimal type. `NUMERIC` affinity stores INTEGER or REAL — the probe shows `42` in a NUMERIC column arriving as INTEGER. |
//! | `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ` | SQLite has NO date/time storage class. Dates are TEXT, INTEGER or REAL by convention only. |
//! | `UUID` | No UUID type. |
//! | `JSON` | SQLite's JSON functions operate on TEXT; there is no JSON storage class. |
//!
//! **Drop-in consequence, stated rather than discovered later:** a SQLite column holding an ISO
//! date reads back as `TEXT`, not as a §9 `Date`. That is also what PDO's SQLite driver does, so
//! the tiers already cope — but it is a real asymmetry against the PG and MySQL columns and the
//! acceptance slice should expect it.
//!
//! The BIND direction is not symmetric: a client may bind any of the fourteen, and the canonical
//! text tags land in TEXT, which is how every SQLite application stores them.

use ferro_pool::error::PoolError;
use ferro_proto::value::Value;
use rusqlite::types::{Value as SqlValue, ValueRef};

/// One cell, tagged from its own storage class.
pub fn value_from_ref(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::I64(i),
        ValueRef::Real(f) => Value::F64(f),
        // Lossy only for non-UTF-8 bytes stored in a TEXT cell, which SQLite permits. Rather than
        // refuse the row, such bytes are surfaced as BYTES below — see the fallthrough.
        ValueRef::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::Text(s.to_string()),
            // A TEXT cell whose bytes are not valid UTF-8. The wire's TEXT payload is UTF-8 by
            // contract, so handing it over as BYTES is the only lossless option; refusing the whole
            // row would make one malformed legacy cell poison an otherwise readable table.
            Err(_) => Value::Bytes(b.to_vec()),
        },
        ValueRef::Blob(b) => Value::Bytes(b.to_vec()),
    }
}

/// A bound parameter. The canonical-text tags land in TEXT, which is how SQLite applications store
/// them; `U64` is the one refusal.
pub fn param_from_value(v: &Value) -> Result<SqlValue, PoolError> {
    Ok(match v {
        Value::Null => SqlValue::Null,
        // SQLite has no boolean type and stores 0/1 as INTEGER — the same shape every SQLite
        // application uses. The read direction cannot recover it (see the table above).
        Value::Bool(b) => SqlValue::Integer(i64::from(*b)),
        Value::I64(i) => SqlValue::Integer(*i),
        Value::F64(f) => SqlValue::Real(*f),
        Value::Text(s) => SqlValue::Text(s.clone()),
        Value::Bytes(b) => SqlValue::Blob(b.clone()),
        Value::U64(u) => {
            // SQLite's INTEGER is signed 64-bit, so a u64 above i64::MAX has NO representation.
            // Refused pre-send rather than wrapped to a negative — the same rule PG applies for the
            // same reason (§9.1: non-representable values are loud, never coerced).
            let n = i64::try_from(*u).map_err(|_| {
                PoolError::Unsupported(format!(
                    "U64 {u} exceeds SQLite's signed 64-bit INTEGER range; SQLite has no unsigned \
                     integer storage class"
                ))
            })?;
            SqlValue::Integer(n)
        }
        // The S7 canonical-text tags. SQLite stores dates, decimals, UUIDs and JSON as TEXT by
        // convention, and the canonical rendering is exactly what a round trip should preserve.
        Value::Decimal(s)
        | Value::Date(s)
        | Value::Time(s)
        | Value::Timestamp(s)
        | Value::TimestampTz(s)
        | Value::Uuid(s)
        | Value::Json(s) => SqlValue::Text(s.clone()),
    })
}
