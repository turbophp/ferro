//! Shape verification (SPEC §24.3): at a store's first use in each `boot_epoch` the engine reads
//! `information_schema.columns` once and checks the table against the one layout v1 serves —
//! Laravel's stock `jobs.stub` (§24.17 Q1). A mismatch makes every verb on the store answer
//! `Unsupported`, naming the column. The engine never repairs a table and never creates one.
//!
//! **The accepted types are the stock layout's, exactly** (PostgreSQL, measured against
//! laravel/framework v11's `PostgresGrammar`: `bigIncrements` → `bigserial`, `string` →
//! `varchar(255)`, `longText` → `text`, `unsignedTinyInteger` → `smallint`, `unsignedInteger` →
//! `integer`), with ONE widening: `queue` may also be `text`. Each narrower accepted type is
//! load-bearing rather than fussy — the `sql` kind's token carries `created_at` in 32 bits and
//! `attempts` in 16 ([`crate::sql::Token`]), so an `integer`/`smallint` layout is exactly what makes
//! the token lossless, and a `bigint` `created_at` would make two rows' tokens collide. `reserved_at`
//! must be nullable (an unreserved row's is `NULL`); the other columns' nullability is not checked.
//! Extra columns are allowed — an operator may add one — though one that is `NOT NULL` without a
//! default makes ENQUEUE fail, classified like any statement.
//!
//! The MySQL-family statement and type table land with MySQL stores, at G6 (SPEC §24.14).

use crate::ident::TableName;
use ferro_proto::value::Value;

/// One engine-authored statement and its bound parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub sql: &'static str,
    pub params: Vec<Value>,
}

/// The `information_schema` read on PostgreSQL. Every column it compares and returns is cast to
/// `text`, so neither the bind (`$1`, `$2` are plain text) nor the decode depends on
/// `information_schema`'s domain types (`sql_identifier`, `character_data`, `yes_or_no`). An
/// unqualified table is looked up in `current_schema()` — the schema an unqualified name in the
/// verbs' own statements would resolve to first.
pub const PG_COLUMNS_SQL: &str = "SELECT column_name::text, data_type::text, is_nullable::text \
     FROM information_schema.columns \
     WHERE table_schema::text = COALESCE($1::text, current_schema()::text) \
       AND table_name::text = $2::text \
     ORDER BY ordinal_position";

pub fn pg_columns_statement(table: &TableName) -> Statement {
    Statement {
        sql: PG_COLUMNS_SQL,
        params: vec![
            table.schema.clone().map_or(Value::Null, Value::Text),
            Value::Text(table.table.clone()),
        ],
    }
}

/// SPEC §24.3: the identity default is resolved for DIAGNOSTICS only — a missing one is logged, never
/// refused (`ENQUEUE` would then fail with `NotNull`, classified like any statement).
pub const PG_SERIAL_SQL: &str = "SELECT pg_get_serial_sequence($1::text, 'id')";

pub fn pg_serial_statement(table: &TableName) -> Statement {
    Statement {
        sql: PG_SERIAL_SQL,
        params: vec![Value::Text(table.quoted(crate::Dialect::Postgres))],
    }
}

/// One `information_schema.columns` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRow {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl ColumnRow {
    /// Read one row of [`PG_COLUMNS_SQL`]'s result. `None` for a row that is not three `TEXT` cells
    /// — which the statement cannot produce, so the caller treats it as a verification failure.
    pub fn from_values(row: &[Value]) -> Option<ColumnRow> {
        match row {
            [
                Value::Text(name),
                Value::Text(data_type),
                Value::Text(nullable),
            ] => Some(ColumnRow {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable: nullable == "YES",
            }),
            _ => None,
        }
    }
}

/// What the table must have, in the order the check walks it (so the column a mismatch names is
/// deterministic): `(column, accepted data_type values, must be nullable)`.
pub const PG_EXPECTED: &[(&str, &[&str], bool)] = &[
    ("id", &["bigint"], false),
    ("queue", &["character varying", "text"], false),
    ("payload", &["text"], false),
    ("attempts", &["smallint"], false),
    ("reserved_at", &["integer"], true),
    ("available_at", &["integer"], false),
    ("created_at", &["integer"], false),
];

/// Why a table does not have the served shape. Every variant names the table, and all but
/// [`ShapeError::TableAbsent`] the column; its `Display` is the `Unsupported` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeError {
    /// No column rows at all: the table does not exist (or is not visible to the pool's role).
    TableAbsent {
        table: String,
    },
    MissingColumn {
        table: String,
        column: &'static str,
    },
    WrongType {
        table: String,
        column: &'static str,
        found: String,
        accepted: &'static [&'static str],
    },
    NotNullable {
        table: String,
        column: &'static str,
    },
}

impl ShapeError {
    /// Whether this verdict may be CACHED for the rest of the `boot_epoch`. An absent table may not:
    /// G1a's decision (SPEC §24.3 amendment) — the ordinary deploy order is "start `ferrod`, then
    /// run migrations", and caching "no table" would refuse the store until the next restart. A table
    /// that EXISTS with the wrong shape is cached, as §24.3 says.
    pub fn is_definitive(&self) -> bool {
        !matches!(self, ShapeError::TableAbsent { .. })
    }
}

impl std::fmt::Display for ShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShapeError::TableAbsent { table } => write!(
                f,
                "queue table {table} does not exist or is not visible to the pool's role \
                 (run the ferro_jobs migration, or set the store's TABLE)"
            ),
            ShapeError::MissingColumn { table, column } => {
                write!(f, "queue table {table} has no column {column}")
            }
            ShapeError::WrongType {
                table,
                column,
                found,
                accepted,
            } => write!(
                f,
                "queue table {table} column {column} is {found}, expected {}",
                accepted.join(" or ")
            ),
            ShapeError::NotNullable { table, column } => {
                write!(f, "queue table {table} column {column} must be nullable")
            }
        }
    }
}

/// The verdict on [`PG_COLUMNS_SQL`]'s rows.
pub fn verify_pg(table: &TableName, rows: &[ColumnRow]) -> Result<(), ShapeError> {
    let name = table.to_string();
    if rows.is_empty() {
        return Err(ShapeError::TableAbsent { table: name });
    }
    for &(column, accepted, must_be_nullable) in PG_EXPECTED {
        let Some(row) = rows.iter().find(|r| r.name == column) else {
            return Err(ShapeError::MissingColumn {
                table: name,
                column,
            });
        };
        if !accepted.contains(&row.data_type.as_str()) {
            return Err(ShapeError::WrongType {
                table: name,
                column,
                found: row.data_type.clone(),
                accepted,
            });
        }
        if must_be_nullable && !row.nullable {
            return Err(ShapeError::NotNullable {
                table: name,
                column,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stock() -> Vec<ColumnRow> {
        [
            ("id", "bigint", false),
            ("queue", "character varying", false),
            ("payload", "text", false),
            ("attempts", "smallint", false),
            ("reserved_at", "integer", true),
            ("available_at", "integer", false),
            ("created_at", "integer", false),
        ]
        .into_iter()
        .map(|(n, t, null)| ColumnRow {
            name: n.into(),
            data_type: t.into(),
            nullable: null,
        })
        .collect()
    }

    fn t() -> TableName {
        TableName::parse("ferro_jobs").unwrap()
    }

    /// The expectation table, pinned against a LITERAL copy of `jobs.stub`'s columns — the
    /// per-column tests below iterate `PG_EXPECTED` itself, so deleting an entry from it would delete
    /// that entry's test too; this one would fail.
    #[test]
    fn the_expected_columns_are_exactly_the_stock_stub() {
        let got: Vec<(&str, Vec<&str>, bool)> = PG_EXPECTED
            .iter()
            .map(|&(c, t, n)| (c, t.to_vec(), n))
            .collect();
        assert_eq!(
            got,
            vec![
                ("id", vec!["bigint"], false),
                ("queue", vec!["character varying", "text"], false),
                ("payload", vec!["text"], false),
                ("attempts", vec!["smallint"], false),
                ("reserved_at", vec!["integer"], true),
                ("available_at", vec!["integer"], false),
                ("created_at", vec!["integer"], false),
            ]
        );
    }

    #[test]
    fn the_stock_layout_passes_with_a_text_queue_and_an_extra_column() {
        assert_eq!(verify_pg(&t(), &stock()), Ok(()));
        let mut rows = stock();
        rows[1].data_type = "text".into();
        rows.push(ColumnRow {
            name: "tenant".into(),
            data_type: "uuid".into(),
            nullable: true,
        });
        rows.reverse(); // order in the result does not matter
        assert_eq!(verify_pg(&t(), &rows), Ok(()));
    }

    #[test]
    fn each_required_column_is_checked_and_named() {
        for (i, &(column, _, _)) in PG_EXPECTED.iter().enumerate() {
            let mut rows = stock();
            rows.remove(i);
            assert_eq!(
                verify_pg(&t(), &rows),
                Err(ShapeError::MissingColumn {
                    table: "ferro_jobs".into(),
                    column
                })
            );
            let mut rows = stock();
            rows[i].data_type = "numeric".into();
            let err = verify_pg(&t(), &rows).unwrap_err();
            assert!(
                matches!(&err, ShapeError::WrongType { column: c, .. } if *c == column),
                "{err}"
            );
            assert!(err.to_string().contains(column), "{err}");
        }
    }

    #[test]
    fn the_token_widths_are_enforced() {
        // A bigint created_at would not fit the token's 32 bits; an integer attempts its 16.
        for (i, wide) in [(6, "bigint"), (3, "integer"), (4, "bigint"), (0, "integer")] {
            let mut rows = stock();
            rows[i].data_type = wide.into();
            assert!(verify_pg(&t(), &rows).is_err(), "{i} {wide}");
        }
    }

    #[test]
    fn reserved_at_must_be_nullable_and_an_empty_result_is_an_absent_table() {
        let mut rows = stock();
        rows[4].nullable = false;
        assert_eq!(
            verify_pg(&t(), &rows),
            Err(ShapeError::NotNullable {
                table: "ferro_jobs".into(),
                column: "reserved_at"
            })
        );
        let absent = verify_pg(&t(), &[]).unwrap_err();
        assert_eq!(
            absent,
            ShapeError::TableAbsent {
                table: "ferro_jobs".into()
            }
        );
        assert!(!absent.is_definitive(), "an absent table is re-checked");
        assert!(
            verify_pg(&t(), &rows).unwrap_err().is_definitive(),
            "a wrong shape is cached"
        );
    }

    #[test]
    fn the_statements_bind_the_schema_and_table_as_text() {
        let s = pg_columns_statement(&t());
        assert_eq!(
            s.params,
            vec![Value::Null, Value::Text("ferro_jobs".into())]
        );
        let q = TableName::parse("app.jobs").unwrap();
        let s = pg_columns_statement(&q);
        assert_eq!(
            s.params,
            vec![Value::Text("app".into()), Value::Text("jobs".into())]
        );
        assert!(s.sql.contains("information_schema.columns"));
        assert!(s.sql.contains("current_schema()"));
        assert_eq!(
            pg_serial_statement(&q).params,
            vec![Value::Text("\"app\".\"jobs\"".into())]
        );
    }

    #[test]
    fn a_row_of_the_wrong_shape_is_not_a_column_row() {
        assert_eq!(
            ColumnRow::from_values(&[
                Value::Text("id".into()),
                Value::Text("bigint".into()),
                Value::Text("NO".into())
            ]),
            Some(ColumnRow {
                name: "id".into(),
                data_type: "bigint".into(),
                nullable: false
            })
        );
        assert_eq!(ColumnRow::from_values(&[Value::Text("id".into())]), None);
        assert_eq!(
            ColumnRow::from_values(&[Value::Null, Value::Null, Value::Null]),
            None
        );
    }
}
