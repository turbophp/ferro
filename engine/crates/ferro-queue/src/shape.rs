//! Shape verification (SPEC §24.3): at a store's first use in each `boot_epoch` the engine reads the
//! store table's catalog entry once and checks it against the one layout v1 serves — Laravel's stock
//! `jobs.stub` (§24.17 Q1). A mismatch makes every verb on the store answer `Unsupported`, naming the
//! column. The engine never repairs a table and never creates one.
//!
//! **The table verified is the table the statements will touch** (M7-G1a review M1). The relation is
//! resolved with `to_regclass` on the SAME quoted identifier the verbs' statements use, so an
//! unqualified `TABLE` follows the pool's `search_path` exactly as they do — `pg_temp` and `"$user"`
//! included. The first version looked the name up in `current_schema()` only, and measured wrong: with
//! `search_path = app, public` and the table in `public`, the statements reach `public.ferro_jobs`
//! while verification reported the table absent on every request. The columns are then read by the
//! resolved OID from `pg_attribute`, through `pg_class_oid_index` and
//! `pg_attribute_relid_attnum_index` (measured with `EXPLAIN`): the old `information_schema` read
//! cast `relname` to `text` and scanned the whole of `pg_class` (review M2).
//!
//! **Only an ordinary or partitioned table passes** (`relkind` `r` or `p`): a view, a materialised
//! view or a foreign table named `ferro_jobs` is refused, because a fenced `DELETE` through one is not
//! the row operation the fence reasons about.
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
//! default makes ENQUEUE fail, classified like any statement. Types are compared as
//! `format_type(atttypid, NULL)` renders them — the same strings `information_schema.data_type` uses
//! for these types (`character varying`, `integer`, …).
//!
//! The MySQL-family statement and type table land with MySQL stores, at G6 (SPEC §24.14).

use crate::ident::TableName;
use ferro_proto::value::Value;

/// One engine-authored statement and its bound parameters. The SQL is owned because every verb's
/// statement interpolates the store's QUOTED table identifier (validated at configuration, never a
/// client value) and an ENQUEUE's row count; nothing a client sends is ever spliced into it.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

/// The catalog read on PostgreSQL: one row per live column of the relation `to_regclass($1)` names —
/// or one row with NULL column cells for a relation without columns, or NO row when nothing by that
/// name is visible. `$1` is the store's QUOTED identifier, bound as text. Every returned cell is
/// `text`, so the decode does not depend on catalog types (`name`, `"char"`).
///
/// The third cell (M7-G1b, carried from the G1a review) says whether `id` is UNIQUE: a fence
/// `WHERE id = $1 AND attempts = $2 AND created_at = $3` over duplicate ids would match several rows,
/// so one ACK could delete two jobs. It is `true` iff some index on the relation is unique, VALID (a
/// `CREATE UNIQUE INDEX CONCURRENTLY` that failed leaves an invalid one that enforces nothing),
/// IMMEDIATE (a `DEFERRABLE` constraint admits duplicates until commit), not partial, not on an
/// expression, and has exactly one key column, `id` (`INCLUDE` columns do not weaken uniqueness; a
/// composite key such as `(id, queue)` does). Measured against PostgreSQL 16 for each of those cases
/// and for a partitioned table's primary key.
///
/// **A unique index does not reach an inheritance child** (M7-G1b review F1), so the fourth cell says
/// whether an ORDINARY table (`relkind` `r`) has any: `CREATE TABLE child () INHERITS (jobs)` leaves
/// the parent's primary key in place, every statement without `ONLY` reaches the child's rows too, and
/// measured on PostgreSQL 16 one ACK then deleted the same `(id, attempts, created_at)` from parent
/// AND child. A PARTITIONED table (`p`) is exempt — its partitions are `pg_inherits` children, but
/// PostgreSQL requires a unique index on a partitioned table to include every partition-key column,
/// so a unique index on `id` ALONE means the table is partitioned by `id` and uniqueness holds across
/// every partition (measured: a duplicate id across hash sub-partitions is refused, `PRIMARY KEY (id)`
/// on a table partitioned by `queue` is refused, an index created `ON ONLY` the parent is invalid, and
/// neither a partitioned table nor a partition can be an inheritance parent).
pub const PG_RELATION_SQL: &str = "SELECT c.relkind::text, c.relnamespace::regnamespace::text, \
            (EXISTS (SELECT 1 FROM pg_index i \
                     JOIN pg_attribute ia ON ia.attrelid = i.indrelid AND ia.attnum = i.indkey[0] \
                     WHERE i.indrelid = c.oid AND i.indisunique AND i.indisvalid AND i.indimmediate \
                       AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1 \
                       AND ia.attname = 'id'))::text, \
            (c.relkind = 'r' AND EXISTS (SELECT 1 FROM pg_inherits h WHERE h.inhparent = c.oid))::text, \
            a.attname::text, format_type(a.atttypid, NULL), \
            CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END \
     FROM pg_class c \
     LEFT JOIN pg_attribute a \
            ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
     WHERE c.oid = to_regclass($1::text) \
     ORDER BY a.attnum";

pub fn pg_relation_statement(table: &TableName) -> Statement {
    Statement {
        sql: PG_RELATION_SQL.to_string(),
        params: vec![Value::Text(table.quoted(crate::Dialect::Postgres))],
    }
}

/// SPEC §24.3: the identity default is resolved for DIAGNOSTICS only — a missing one is logged, never
/// refused (`ENQUEUE` would then fail with `NotNull`, classified like any statement).
pub const PG_SERIAL_SQL: &str = "SELECT pg_get_serial_sequence($1::text, 'id')";

pub fn pg_serial_statement(table: &TableName) -> Statement {
    Statement {
        sql: PG_SERIAL_SQL.to_string(),
        params: vec![Value::Text(table.quoted(crate::Dialect::Postgres))],
    }
}

/// One column of the resolved relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRow {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// The relation [`PG_RELATION_SQL`] resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRelation {
    /// `pg_class.relkind`: `r` table, `p` partitioned table, `v` view, `m` materialised view, `f`
    /// foreign table, …
    pub relkind: String,
    /// The schema it resolved in (for the log line saying which table was verified).
    pub schema: String,
    /// Whether `id` carries a single-column, valid, immediate, non-partial unique index (see
    /// [`PG_RELATION_SQL`]).
    pub id_unique: bool,
    /// Whether an ordinary table has inheritance children (see [`PG_RELATION_SQL`]).
    pub inheritance_children: bool,
    pub columns: Vec<ColumnRow>,
}

impl PgRelation {
    /// Read [`PG_RELATION_SQL`]'s rows. `Ok(None)`: no such relation. `Err(())`: a row the statement
    /// cannot produce, which the caller treats as a verification failure.
    #[allow(clippy::result_unit_err)]
    pub fn from_rows(rows: &[Vec<Value>]) -> Result<Option<PgRelation>, ()> {
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        let flag = |v: &str| match v {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(()),
        };
        let (relkind, schema, id_unique, inheritance_children) = match first.as_slice() {
            [
                Value::Text(k),
                Value::Text(s),
                Value::Text(u),
                Value::Text(i),
                ..,
            ] => (k.clone(), s.clone(), flag(u)?, flag(i)?),
            _ => return Err(()),
        };
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            match row.as_slice() {
                [
                    Value::Text(_),
                    Value::Text(_),
                    Value::Text(_),
                    Value::Text(_),
                    Value::Text(name),
                    Value::Text(data_type),
                    Value::Text(nullable),
                ] => columns.push(ColumnRow {
                    name: name.clone(),
                    data_type: data_type.clone(),
                    nullable: nullable == "YES",
                }),
                // The LEFT JOIN's no-column row.
                [
                    Value::Text(_),
                    Value::Text(_),
                    Value::Text(_),
                    Value::Text(_),
                    Value::Null,
                    Value::Null,
                    Value::Text(_),
                ] if rows.len() == 1 => {}
                _ => return Err(()),
            }
        }
        Ok(Some(PgRelation {
            relkind,
            schema,
            id_unique,
            inheritance_children,
            columns,
        }))
    }
}

/// The relation kinds a store may be: an ordinary table and a partitioned table.
pub const PG_TABLE_RELKINDS: &[&str] = &["r", "p"];

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

/// Why a table does not have the served shape. Every variant names the table, and the column ones
/// the column; its `Display` is the `Unsupported` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeError {
    /// No relation by that name is visible on the pool's `search_path` (or to its role).
    TableAbsent {
        table: String,
    },
    /// The name resolves to something that is not a table (a view, a foreign table, …).
    NotATable {
        table: String,
        relkind: String,
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
    /// `id` has no single-column unique index (M7-G1b): the fence would not name one row.
    IdNotUnique {
        table: String,
    },
    /// An ordinary table with inheritance children (M7-G1b review F1): its unique index does not
    /// reach the children's rows, which every statement also touches.
    InheritanceChildren {
        table: String,
    },
}

impl ShapeError {
    /// Whether this verdict may be CACHED for the rest of the `boot_epoch`. An absent table may not
    /// (G1a's decision, SPEC §24.3 amendment): the ordinary deploy order is "start `ferrod`, then run
    /// migrations". `ferrod` re-checks it only after a short negative TTL, so a missing table costs
    /// one catalog read per TTL, not one per request (review M2). Everything else is a table that
    /// EXISTS in the wrong form, cached as §24.3 says.
    pub fn is_definitive(&self) -> bool {
        !matches!(self, ShapeError::TableAbsent { .. })
    }
}

impl std::fmt::Display for ShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShapeError::TableAbsent { table } => write!(
                f,
                "queue table {table} does not exist on the pool's search_path or is not visible to \
                 its role (run the ferro_jobs migration, or set the store's TABLE)"
            ),
            ShapeError::NotATable { table, relkind } => write!(
                f,
                "queue table {table} is not a table (pg_class.relkind {relkind:?}; a view or a \
                 foreign table cannot be a store)"
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
            ShapeError::IdNotUnique { table } => write!(
                f,
                "queue table {table} column id is not unique (it needs a primary key or a valid, \
                 immediate, non-partial unique index on id alone; a fence over duplicate ids would \
                 match several rows)"
            ),
            ShapeError::InheritanceChildren { table } => write!(
                f,
                "queue table {table} has inheritance children (CREATE TABLE … INHERITS): its unique \
                 index on id does not cover their rows, which every queue statement also reaches, so \
                 a fence could match several rows (a partitioned table is accepted)"
            ),
        }
    }
}

/// The verdict on the resolved relation (`None` = nothing by that name).
pub fn verify_pg(table: &TableName, relation: Option<&PgRelation>) -> Result<(), ShapeError> {
    let name = table.to_string();
    let Some(rel) = relation else {
        return Err(ShapeError::TableAbsent { table: name });
    };
    if !PG_TABLE_RELKINDS.contains(&rel.relkind.as_str()) {
        return Err(ShapeError::NotATable {
            table: name,
            relkind: rel.relkind.clone(),
        });
    }
    if rel.inheritance_children {
        return Err(ShapeError::InheritanceChildren { table: name });
    }
    for &(column, accepted, must_be_nullable) in PG_EXPECTED {
        let Some(row) = rel.columns.iter().find(|r| r.name == column) else {
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
    // After the columns, so a table missing `id` is named for that first.
    if !rel.id_unique {
        return Err(ShapeError::IdNotUnique { table: name });
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

    fn rel(relkind: &str, columns: Vec<ColumnRow>) -> PgRelation {
        PgRelation {
            relkind: relkind.into(),
            schema: "public".into(),
            id_unique: true,
            inheritance_children: false,
            columns,
        }
    }

    fn table(columns: Vec<ColumnRow>) -> Option<PgRelation> {
        Some(rel("r", columns))
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
        assert_eq!(PG_TABLE_RELKINDS, ["r", "p"]);
    }

    #[test]
    fn the_stock_layout_passes_with_a_text_queue_and_an_extra_column() {
        assert_eq!(verify_pg(&t(), table(stock()).as_ref()), Ok(()));
        let mut rows = stock();
        rows[1].data_type = "text".into();
        rows.push(ColumnRow {
            name: "tenant".into(),
            data_type: "uuid".into(),
            nullable: true,
        });
        rows.reverse(); // order in the result does not matter
        assert_eq!(verify_pg(&t(), table(rows).as_ref()), Ok(()));
        // A partitioned table passes too.
        assert_eq!(verify_pg(&t(), Some(&rel("p", stock()))), Ok(()));
    }

    /// Review M1: a view (or anything that is not a table) with the right columns is refused, and
    /// the refusal is cached.
    #[test]
    fn a_view_or_foreign_table_with_the_right_columns_is_not_a_table() {
        for kind in ["v", "m", "f", "S", "c"] {
            let err = verify_pg(&t(), Some(&rel(kind, stock()))).unwrap_err();
            assert_eq!(
                err,
                ShapeError::NotATable {
                    table: "ferro_jobs".into(),
                    relkind: kind.into()
                }
            );
            assert!(err.is_definitive());
        }
    }

    #[test]
    fn each_required_column_is_checked_and_named() {
        for (i, &(column, _, _)) in PG_EXPECTED.iter().enumerate() {
            let mut rows = stock();
            rows.remove(i);
            assert_eq!(
                verify_pg(&t(), table(rows).as_ref()),
                Err(ShapeError::MissingColumn {
                    table: "ferro_jobs".into(),
                    column
                })
            );
            let mut rows = stock();
            rows[i].data_type = "numeric".into();
            let err = verify_pg(&t(), table(rows).as_ref()).unwrap_err();
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
            assert!(verify_pg(&t(), table(rows).as_ref()).is_err(), "{i} {wide}");
        }
    }

    /// M7-G1b: a table whose `id` is not unique is refused, definitively, naming `id`; and the check
    /// runs after the columns, so a table missing `id` is named for THAT.
    #[test]
    fn an_id_without_a_unique_index_is_refused() {
        let mut r = rel("r", stock());
        r.id_unique = false;
        let err = verify_pg(&t(), Some(&r)).unwrap_err();
        assert_eq!(
            err,
            ShapeError::IdNotUnique {
                table: "ferro_jobs".into()
            }
        );
        assert!(err.is_definitive());
        assert!(err.to_string().contains("column id is not unique"), "{err}");
        let mut cols = stock();
        cols.remove(0);
        let mut r = rel("r", cols);
        r.id_unique = false;
        assert!(matches!(
            verify_pg(&t(), Some(&r)),
            Err(ShapeError::MissingColumn { column: "id", .. })
        ));
    }

    /// The uniqueness predicate's every clause is load-bearing (each was measured against a table
    /// that only it refuses: an invalid index, a DEFERRABLE constraint, a partial index, an
    /// expression index, a composite key, a key on another column).
    #[test]
    fn the_uniqueness_predicate_has_every_clause() {
        for clause in [
            "i.indisunique",
            "i.indisvalid",
            "i.indimmediate",
            "i.indpred IS NULL",
            "i.indexprs IS NULL",
            "i.indnkeyatts = 1",
            "ia.attnum = i.indkey[0]",
            "ia.attname = 'id'",
            "i.indrelid = c.oid",
        ] {
            assert!(PG_RELATION_SQL.contains(clause), "{clause}");
        }
    }

    /// Review F1: an ordinary table with inheritance children is refused, definitively, by name —
    /// before its columns are read (the parent's columns are fine; the CHILDREN are the defect).
    #[test]
    fn an_ordinary_table_with_inheritance_children_is_refused() {
        let mut r = rel("r", stock());
        r.inheritance_children = true;
        let err = verify_pg(&t(), Some(&r)).unwrap_err();
        assert_eq!(
            err,
            ShapeError::InheritanceChildren {
                table: "ferro_jobs".into()
            }
        );
        assert!(err.is_definitive());
        assert!(err.to_string().contains("inheritance children"), "{err}");
        // The catalog cell is gated on relkind 'r' (a partitioned table's partitions are
        // pg_inherits children too) and the flag parses from the fourth cell.
        assert!(PG_RELATION_SQL.contains(
            "(c.relkind = 'r' AND EXISTS (SELECT 1 FROM pg_inherits h WHERE h.inhparent = c.oid))"
        ));
        let rows = vec![vec![
            txt("r"),
            txt("app"),
            txt("true"),
            txt("true"),
            txt("id"),
            txt("bigint"),
            txt("NO"),
        ]];
        assert!(
            PgRelation::from_rows(&rows)
                .unwrap()
                .unwrap()
                .inheritance_children
        );
    }

    #[test]
    fn reserved_at_must_be_nullable_and_no_relation_is_an_absent_table() {
        let mut rows = stock();
        rows[4].nullable = false;
        let not_null = verify_pg(&t(), table(rows).as_ref()).unwrap_err();
        assert_eq!(
            not_null,
            ShapeError::NotNullable {
                table: "ferro_jobs".into(),
                column: "reserved_at"
            }
        );
        let absent = verify_pg(&t(), None).unwrap_err();
        assert_eq!(
            absent,
            ShapeError::TableAbsent {
                table: "ferro_jobs".into()
            }
        );
        assert!(!absent.is_definitive(), "an absent table is re-checked");
        assert!(not_null.is_definitive(), "a wrong shape is cached");
        // A table with no columns at all is a table missing `id`, not an absent one.
        assert_eq!(
            verify_pg(&t(), table(vec![]).as_ref()),
            Err(ShapeError::MissingColumn {
                table: "ferro_jobs".into(),
                column: "id"
            })
        );
    }

    /// Review M1: the relation is resolved by `to_regclass` on the QUOTED identifier — the statements'
    /// own resolution — never by `current_schema()`; and the columns are read by the resolved OID.
    #[test]
    fn the_statement_resolves_the_quoted_identifier_through_the_search_path() {
        let s = pg_relation_statement(&t());
        assert_eq!(s.params, vec![Value::Text("\"ferro_jobs\"".into())]);
        let q = TableName::parse("App.Jobs").unwrap();
        assert_eq!(
            pg_relation_statement(&q).params,
            vec![Value::Text("\"App\".\"Jobs\"".into())]
        );
        assert!(s.sql.contains("to_regclass($1::text)"));
        assert!(
            s.sql.contains("c.oid = to_regclass"),
            "the OID index predicate"
        );
        assert!(!s.sql.contains("current_schema"));
        assert!(!s.sql.contains("information_schema"));
        assert!(s.sql.contains("NOT a.attisdropped"));
        assert_eq!(
            pg_serial_statement(&q).params,
            vec![Value::Text("\"App\".\"Jobs\"".into())]
        );
    }

    fn txt(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn rows_parse_into_a_relation_and_malformed_rows_are_refused() {
        assert_eq!(PgRelation::from_rows(&[]), Ok(None));
        let rows = vec![
            vec![
                txt("r"),
                txt("app"),
                txt("true"),
                txt("false"),
                txt("id"),
                txt("bigint"),
                txt("NO"),
            ],
            vec![
                txt("r"),
                txt("app"),
                txt("true"),
                txt("false"),
                txt("reserved_at"),
                txt("integer"),
                txt("YES"),
            ],
        ];
        let rel = PgRelation::from_rows(&rows).unwrap().unwrap();
        assert_eq!((rel.relkind.as_str(), rel.schema.as_str()), ("r", "app"));
        assert!(rel.id_unique);
        assert_eq!(
            rel.columns,
            vec![
                ColumnRow {
                    name: "id".into(),
                    data_type: "bigint".into(),
                    nullable: false
                },
                ColumnRow {
                    name: "reserved_at".into(),
                    data_type: "integer".into(),
                    nullable: true
                },
            ]
        );
        // A relation without columns: one row of NULL column cells.
        let empty = vec![vec![
            txt("v"),
            txt("app"),
            txt("false"),
            txt("false"),
            Value::Null,
            Value::Null,
            txt("YES"),
        ]];
        assert_eq!(
            PgRelation::from_rows(&empty),
            Ok(Some(PgRelation {
                relkind: "v".into(),
                schema: "app".into(),
                id_unique: false,
                inheritance_children: false,
                columns: vec![]
            }))
        );
        // The uniqueness cell is a closed vocabulary.
        assert_eq!(
            PgRelation::from_rows(&[vec![
                txt("r"),
                txt("a"),
                txt("t"),
                txt("false"),
                txt("id"),
                txt("bigint"),
                txt("NO")
            ]]),
            Err(())
        );
        assert_eq!(PgRelation::from_rows(&[vec![txt("r")]]), Err(()));
        assert_eq!(
            PgRelation::from_rows(&[vec![
                Value::Null,
                txt("a"),
                txt("true"),
                txt("false"),
                txt("id"),
                txt("bigint"),
                txt("NO")
            ]]),
            Err(())
        );
    }
}
