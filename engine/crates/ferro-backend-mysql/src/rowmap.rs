//! MySQL/MariaDB column-type → canonical `Value` mapping (M1-S6's scoped scalar set, widened to the
//! M1-S7 canonical tags), plus the binary-protocol cell extraction. The MySQL counterpart of
//! `ferro-backend-pg`'s `rowmap` (which keys off PG OIDs); here we key off the wire **column
//! metadata** (`ColumnType` + `ColumnFlags` + charset + display length), because a MySQL
//! binary-protocol `Value` alone is ambiguous:
//!
//! * TEXT, VARCHAR, CHAR, BLOB, VARBINARY, BINARY, DECIMAL and MySQL 8's JSON all arrive as
//!   `Value::Bytes` — only the column's type and charset (binary collation `63` = a byte string)
//!   tell them apart;
//! * `DATETIME` and `TIMESTAMP` arrive as **byte-identical** `Value::Date(y, m, d, h, mi, s, us)`
//!   components with no zone at all, so only the column type separates a naive wall clock from a
//!   UTC instant (SPEC §9: `datetime` → `TIMESTAMP`, `timestamp` → `TIMESTAMPTZ`);
//! * a signed and an unsigned integer both arrive as `Value::Int`/`Value::UInt`, and the choice
//!   between those two variants follows the *magnitude*, not the column — the `UNSIGNED_FLAG` on a
//!   `BIGINT` is the only thing that makes a cell `U64` rather than `I64`;
//! * `TINYINT(1)` is MySQL's `BOOLEAN` convention — the prepared-statement column metadata keeps its
//!   display length `1` (integer display widths are otherwise deprecated in MySQL 8, but `TINYINT(1)`
//!   is retained precisely for the boolean idiom), so `TINYINT` with display length `1` maps to
//!   `Bool` **whether or not it is UNSIGNED** and any wider `TINYINT` folds into `I64`.
//!
//! [`column_kind`] is the ONE classifier both [`column_to_tag`] (for `ColMeta`, built off the
//! prepared statement BEFORE the query runs — an out-of-scope column errors loudly while the conn is
//! still clean) and [`extract_value`] share, exactly as PG's `oid_extract_type` backs both
//! `oid_to_tag` and `extract_value`.
//!
//! **The two gates fire at DIFFERENT times and must move in LOCKSTEP (hazard 18).**
//! [`column_to_tag`] runs at cols-build, pre-execution, and decides what `HEAD` promises;
//! [`extract_value`] runs **per cell** and is the producer that has to fill that promise. Admitting
//! a type in one but not the other yields a `HEAD` promising a tag the producer cannot fill. Both
//! are matches over the single [`column_kind`] classifier precisely so they cannot drift, and the
//! agreement is asserted on real cells offline
//! (`head_tag_equals_emitted_tag_for_every_admitted_column`) and live on BOTH engines
//! (`tests/mysql_types_it.rs`).
//!
//! *When the per-cell gate fires, on MySQL, today:* this backend's read path is **buffered** —
//! [`crate::query`] drains the whole result set and maps the rows only after the drain, and
//! `fetch:stream` on a MySQL pool is still a clean `Unsupported` (§22.2) — so nothing is on the wire
//! yet when an `extract_value` refusal is raised, and it surfaces as an ordinary request error. The
//! invariant being defended is nonetheless the streaming one — *HEAD promises exactly what the
//! producer emits* — because that is what the buffered path would silently violate the moment it is
//! made incremental. PG streams today, so there the same disagreement genuinely lands after `HEAD`
//! is on the wire; MySQL streaming (a later slice) will need its own gate pair proved against the
//! incremental path.
//!
//! **M1-S7 canonical text.** The canonical tags added in this slice (`DECIMAL`/`DATE`/`TIME`/
//! `TIMESTAMP`/`TIMESTAMPTZ`/`JSON`) are carried as canonical **text** (`PROTOCOL.md` §3.2),
//! rendered from the driver's components by [`crate::mytext`]. The `Z` on a `TIMESTAMPTZ` is
//! truthful only because `conn::connect` pins every session to `time_zone = '+00:00'`.
//!
//! **Two documented MySQL-family divergences, both MEASURED (MySQL 8.4.11 / MariaDB 11.8.8):**
//! * **`UUID` is never emitted by this backend.** MySQL 8 has no UUID type (a `BINARY(16)` stays
//!   `Bytes`, a `CHAR(36)` stays `Text`), and MariaDB's native `UUID` reaches the wire as
//!   `MYSQL_TYPE_STRING` in a **utf8mb4** charset carrying the 36-char hyphenated text — indistinguishable
//!   from a `CHAR(36)` — so it classifies as `Text` (the value is correct; only the tag is the
//!   generic one). MariaDB's `INET4`/`INET6` arrive the same way.
//! * **MariaDB `JSON` classifies as `Text` BY DESIGN** — it is an alias for `LONGTEXT` plus a
//!   `json_valid()` CHECK, byte-identical on the wire to a plain `LONGTEXT`. Promoting a utf8
//!   `LONGTEXT` to `JSON` would be the silent miscast charter rule 6 forbids.
//!
//! **M1-S8a: `ENUM` is admitted as a string.** An `ENUM` cell's binary-protocol value IS its label
//! string in the column's charset, so it takes the SAME charset branch as every other string type
//! (`Text`, or `Bytes` on a binary collation) — lossless, not a promotion. Measured on both
//! engines, an `ENUM` column arrives as `MYSQL_TYPE_STRING` carrying `ENUM_FLAG` (the dedicated
//! `MYSQL_TYPE_ENUM` code never reaches the client), which is what unblocks DBAL's schema manager:
//! `information_schema.COLUMNS.COLUMN_KEY` and `referential_constraints.UPDATE_RULE` are ENUMs on
//! MySQL 8. **`SET` stays out of scope** — its wire form is a comma-joined MULTI-value string, a
//! different type rather than a longer one.
//!
//! Out-of-scope types (`YEAR`, `BIT`, `SET`, `GEOMETRY`, `VECTOR`, …) are a LOUD
//! `PoolError::Unsupported` NAMING the column and its native type — never a silent miscast. A cell
//! whose driver `Value` variant does not match its column kind is a client-side decode mismatch
//! surfaced as `PoolError::Backend` (NonRetryable) — NEVER `ConnectionLost`, so it can never mint a
//! false §19.3 `Indeterminate`.

use ferro_pool::error::PoolError;
use ferro_proto::consts::tag;
use mysql_async::Column;
use mysql_async::Value as MyValue;
use mysql_async::consts::{ColumnFlags, ColumnType};

use crate::Value;
use crate::mytext;

/// The MySQL "binary" collation id. A string/blob column reporting this collation is a byte string
/// (`BINARY`/`VARBINARY`/`BLOB`); any other collation on the same column type is character data
/// (`CHAR`/`VARCHAR`/`TEXT`). Stable across MySQL and MariaDB.
const BINARY_COLLATION_ID: u16 = 63;

/// The canonical scalar a MySQL column maps to. Split out from `Value` so the classification is
/// unit-testable on its own (mirrors PG's `ExtractType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyKind {
    /// `TINYINT(1)` — MySQL's `BOOLEAN` — signed **or** unsigned (§9.1, pinned in M1-S6).
    Bool,
    /// `TINYINT`(≠1)/`SMALLINT`/`MEDIUMINT`/`INT` (either signedness — all fit `i64` losslessly)
    /// and a **signed** `BIGINT` → `Value::I64`.
    I64,
    /// `BIGINT UNSIGNED` → `Value::U64`. The ONLY column type that reaches this kind: every
    /// narrower unsigned width is lossless in `i64` and stays [`MyKind::I64`], which deliberately
    /// keeps the `U64` surface as small as §9's table specifies.
    U64,
    /// `FLOAT`/`DOUBLE` → `Value::F64`.
    F64,
    /// `CHAR`/`VARCHAR`/`TEXT` (a non-binary charset) → `Value::Text`. Also where MariaDB's `JSON`,
    /// native `UUID` and `INET4`/`INET6` land — see the module docs.
    Text,
    /// `BINARY`/`VARBINARY`/`BLOB` (the binary collation) → `Value::Bytes`.
    Bytes,
    // ---- M1-S7 canonical-text kinds. Each renders through `crate::mytext`.
    /// `DECIMAL`/`NEWDECIMAL` → `Value::Decimal`. The cell is the server's own ASCII rendering and
    /// is carried **verbatim**, so the display scale survives (`1.10` ≠ `1.1`).
    Decimal,
    /// `DATE` → `Value::Date` (`"YYYY-MM-DD"`, or the verbatim `"0000-00-00"` zero-date sentinel).
    Date,
    /// `TIME` → `Value::Time`. A signed duration spanning ±838:59:59, NOT a time of day.
    Time,
    /// `DATETIME` → **naive** `Value::Timestamp`, no zone suffix ever.
    ///
    /// The driver components are byte-identical to [`MyKind::TimestampTz`]'s; only the column type
    /// separates the two, so they must never collapse onto one arm (a swap is a silent zone shift
    /// with no error anywhere).
    Timestamp,
    /// `TIMESTAMP` → `Value::TimestampTz`, a UTC instant with a literal `Z` — truthful only under
    /// the `time_zone = '+00:00'` session pin. See [`MyKind::Timestamp`].
    TimestampTz,
    /// **MySQL 8's** `MYSQL_TYPE_JSON` → `Value::Json` (the raw document text). MariaDB never emits
    /// this type code — its `JSON` is a `LONGTEXT` alias and classifies as [`MyKind::Text`].
    Json,
}

/// The string/blob family's Text-vs-Bytes decision: the binary collation (63) means a byte string,
/// anything else is character data. Shared by the `STRING`-family arm and the standalone
/// `MYSQL_TYPE_ENUM` arm so the two can never diverge — the ENUM rejection used to live in BOTH
/// places, and the string-family one is the only one live traffic ever reaches.
fn string_family_kind(col: &Column) -> MyKind {
    if col.character_set() == BINARY_COLLATION_ID {
        MyKind::Bytes
    } else {
        MyKind::Text
    }
}

/// Classify a MySQL column into its canonical [`MyKind`], or a LOUD `Unsupported` naming the column
/// and its out-of-scope native type. This is the single source of truth for BOTH the `ColMeta` tag
/// and the cell extraction, so `cols` and `rows` can never disagree on a column's type.
pub fn column_kind(col: &Column) -> Result<MyKind, PoolError> {
    let ct = col.column_type();
    let flags = col.flags();
    let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    match ct {
        // TINYINT: the BOOLEAN idiom (display length 1) is `Bool`, tested FIRST and independently
        // of signedness — the pre-S7 arm tested `unsigned` first, so a `TINYINT(1) UNSIGNED` fell
        // past this branch and contradicted the §9.1 policy pinned in M1-S6 (hazard 44). A wider
        // TINYINT is an integer either way (0..=255 and -128..=127 both fit i64).
        ColumnType::MYSQL_TYPE_TINY => {
            if col.column_length() == 1 {
                Ok(MyKind::Bool)
            } else {
                Ok(MyKind::I64)
            }
        }
        // The narrower integer widths fit `i64` LOSSLESSLY in both signednesses (an INT UNSIGNED
        // maxes at 4 294 967 295), so they all fold into I64 — this is what keeps the U64 surface
        // down to the one column type §9's table names.
        ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_INT24 => Ok(MyKind::I64),
        // BIGINT is the split: UNSIGNED can exceed `i64::MAX`, so it — and only it — is U64.
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if unsigned {
                Ok(MyKind::U64)
            } else {
                Ok(MyKind::I64)
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT | ColumnType::MYSQL_TYPE_DOUBLE => Ok(MyKind::F64),
        // DECIMAL is exact: the cell IS the server's ASCII text, passed through byte-for-byte.
        // (`MYSQL_TYPE_DECIMAL` is the pre-5.0 form; every live server sends `NEWDECIMAL`.)
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => Ok(MyKind::Decimal),
        // The date/time family. Signedness is IGNORED here on purpose: MariaDB 11 reports
        // `TIMESTAMP` with `UNSIGNED_FLAG | BINARY_FLAG` (measured) where MySQL 8 sends
        // `BINARY_FLAG` alone, and neither says anything about the value.
        ColumnType::MYSQL_TYPE_DATE => Ok(MyKind::Date),
        ColumnType::MYSQL_TYPE_TIME => Ok(MyKind::Time),
        // NAIVE vs UTC INSTANT — identical driver components, different canonical forms (SPEC §9).
        ColumnType::MYSQL_TYPE_DATETIME => Ok(MyKind::Timestamp),
        ColumnType::MYSQL_TYPE_TIMESTAMP => Ok(MyKind::TimestampTz),
        // MySQL 8 only. MariaDB has no JSON type code at all, so this arm simply never fires there
        // and its `JSON` column falls into the string family below as `Text` (by design, hazard 25).
        ColumnType::MYSQL_TYPE_JSON => Ok(MyKind::Json),
        // The string/blob family: charset decides Text vs Bytes. ENUM and SET arrive here (as
        // STRING carrying a flag); ENUM is admitted as of M1-S8a, SET stays out of scope.
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            // M1-S8a: `ENUM_FLAG` is NO LONGER a rejection. An ENUM cell's binary-protocol value IS
            // the label string in the column's charset — carrying it as TEXT is lossless, not a
            // promotion (contrast MariaDB's JSON-as-LONGTEXT, which WOULD be a promotion because
            // the wire cannot distinguish it from a plain LONGTEXT).
            //
            // THIS is the fix that unblocks DBAL's schema manager, and it is sufficient on its own:
            // measured on MySQL 8.4 and MariaDB 11.8, an ENUM column — whether it is
            // `information_schema.COLUMNS.COLUMN_KEY`, `referential_constraints.UPDATE_RULE`, or a
            // user-declared `ENUM('a','b')` — arrives as MYSQL_TYPE_STRING + ENUM_FLAG and lands
            // here. The dedicated MYSQL_TYPE_ENUM code never reaches the client.
            if flags.contains(ColumnFlags::SET_FLAG) {
                // SET stays out of scope: its wire form is a COMMA-JOINED multi-value string, a
                // different type rather than a longer one.
                return Err(unsupported(col, "SET"));
            }
            Ok(string_family_kind(col))
        }
        // Still deferred in S7 (SPEC §22.2) — each a loud, diagnosable refusal, named individually
        // so the message tells an operator WHICH type they hit rather than "something".
        ColumnType::MYSQL_TYPE_YEAR => Err(unsupported(col, "YEAR")),
        ColumnType::MYSQL_TYPE_BIT => Err(unsupported(col, "BIT")),
        // Unreachable from any server this project tests against: both engines send an ENUM column
        // as MYSQL_TYPE_STRING + ENUM_FLAG (handled above). Fixed anyway so a server or driver
        // version that DOES send the dedicated code cannot hit a stale refusal — but deliberately
        // NOT the subject of a live test, because no live traffic can reach it and such a test
        // would be a guard that cannot fail. The offline `column_kind` unit tests, which build the
        // `Column` fixture directly, are its only coverage and that is the honest amount.
        ColumnType::MYSQL_TYPE_ENUM => Ok(string_family_kind(col)),
        ColumnType::MYSQL_TYPE_SET => Err(unsupported(col, "SET")),
        ColumnType::MYSQL_TYPE_GEOMETRY => Err(unsupported(col, "GEOMETRY")),
        ColumnType::MYSQL_TYPE_VECTOR => Err(unsupported(col, "VECTOR")),
        // The NULL column type plus the server-internal codes (NEWDATE, DATETIME2, TIMESTAMP2,
        // TIME2, TYPED_ARRAY, UNKNOWN) which no live server sends on the client protocol.
        _ => Err(unsupported(col, "out-of-scope column type")),
    }
}

/// Map a MySQL column to its canonical `Value` tag (for `ColMeta`). `Unsupported` for any
/// out-of-scope type — a loud typed error, never a silent miscast (the MySQL analog of PG's
/// `oid_to_tag`).
///
/// This is the **cols-build (pre-execution)** half of the two-gate pair; [`extract_value`] is the
/// **per-cell (mid-stream)** half. Both are matches over [`column_kind`], so the tag `HEAD` promises
/// is by construction the tag the producer emits (hazard 18).
pub fn column_to_tag(col: &Column) -> Result<u8, PoolError> {
    Ok(match column_kind(col)? {
        MyKind::Bool => tag::BOOL,
        MyKind::I64 => tag::I64,
        MyKind::U64 => tag::U64,
        MyKind::F64 => tag::F64,
        MyKind::Text => tag::TEXT,
        MyKind::Bytes => tag::BYTES,
        MyKind::Decimal => tag::DECIMAL,
        MyKind::Date => tag::DATE,
        MyKind::Time => tag::TIME,
        MyKind::Timestamp => tag::TIMESTAMP,
        MyKind::TimestampTz => tag::TIMESTAMPTZ,
        MyKind::Json => tag::JSON,
    })
}

/// Extract a single binary-protocol cell (`value` for column `col`) into a canonical `Value`. A SQL
/// `NULL` is `Value::Null` regardless of column type. An out-of-scope column is `Unsupported`; a
/// driver `Value` variant that does not match the column kind is a client-side decode mismatch
/// surfaced as `Backend` (NonRetryable) — NEVER `ConnectionLost` (no false §19.3 Indeterminate).
///
/// This is the **per-cell** gate — the producer half of the promise [`column_to_tag`] already made
/// in `HEAD` for the same column — which is why the two are both matches over the single
/// [`column_kind`] classifier (hazard 18). On MySQL today the read path is buffered
/// ([`crate::query`] maps rows after the full drain; `fetch:stream` is `Unsupported`), so nothing is
/// on the wire when this fires; the invariant it defends is the streaming one, and MySQL streaming
/// will need its own gate pair. See the module docs.
pub fn extract_value(value: &MyValue, col: &Column) -> Result<Value, PoolError> {
    if matches!(value, MyValue::NULL) {
        return Ok(Value::Null);
    }
    match column_kind(col)? {
        MyKind::Bool => Ok(Value::Bool(as_i64(value, col)? != 0)),
        MyKind::I64 => Ok(Value::I64(as_i64(value, col)?)),
        MyKind::U64 => Ok(Value::U64(as_u64(value, col)?)),
        MyKind::F64 => Ok(Value::F64(as_f64(value, col)?)),
        MyKind::Text => {
            let bytes = as_bytes(value, col)?;
            String::from_utf8(bytes)
                .map(Value::Text)
                .map_err(|e| decode_err(col, format!("invalid UTF-8 in a TEXT column: {e}")))
        }
        MyKind::Bytes => Ok(Value::Bytes(as_bytes(value, col)?)),
        // ---- M1-S7 canonical text. Each arm is the producer half of the tag `column_to_tag`
        // already promised in HEAD for this same column; the pairs are asserted on real cells by
        // `tests/mysql_types_it.rs` on BOTH engines (`cols[i].tag == rows[0][i].tag()`).
        MyKind::Decimal => Ok(Value::Decimal(render(col, value, mytext::decimal_to_text)?)),
        MyKind::Date => Ok(Value::Date(render(col, value, mytext::date_to_text)?)),
        MyKind::Time => Ok(Value::Time(render(col, value, mytext::time_to_text)?)),
        // The next two read the SAME driver components and differ ONLY in the renderer — naive vs
        // UTC instant. Swapping them is a silent zone shift with no error anywhere, which is why
        // the column type is the discriminator and the two kinds never share an arm.
        MyKind::Timestamp => Ok(Value::Timestamp(render(
            col,
            value,
            mytext::datetime_to_text,
        )?)),
        MyKind::TimestampTz => Ok(Value::TimestampTz(render(
            col,
            value,
            mytext::timestamptz_to_text,
        )?)),
        MyKind::Json => Ok(Value::Json(render(col, value, mytext::json_to_text)?)),
    }
}

/// Run one [`crate::mytext`] renderer and re-stamp the column identity onto its error.
///
/// `mytext` is deliberately column-agnostic (it names the driver variant, never the cell contents —
/// SPEC §12), so this is where "which column" is added, matching the `decode_err` shape every other
/// arm uses. A non-`Backend` error is passed through untouched: only a decode mismatch belongs to a
/// column.
fn render<F>(col: &Column, value: &MyValue, renderer: F) -> Result<String, PoolError>
where
    F: FnOnce(&MyValue) -> Result<String, PoolError>,
{
    renderer(value).map_err(|e| match e {
        PoolError::Backend(detail) => decode_err(col, detail),
        other => other,
    })
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
            format!(
                "expected a signed integer cell, got a driver `{}` value",
                variant_name(other)
            ),
        )),
    }
}

/// An unsigned-integer cell for a `BIGINT UNSIGNED` column, accepting **both** driver forms.
///
/// **Hazard 23 / F5.** A `BIGINT UNSIGNED` value arrives as `MyValue::UInt` ONLY when it exceeds
/// `i64::MAX` (`mysql_common-0.37.3/src/value/mod.rs:320-329`); every smaller value — i.e. every
/// row a real application stores — arrives as `MyValue::Int`. Measured live on MySQL 8.4 and
/// MariaDB 11.8: `0`, `5` and `4294967296` all came back as `Int`, only `18446744073709551615` as
/// `UInt`. A `UInt`-only extractor would therefore reject the COMMON case as a decode mismatch
/// AFTER `HEAD` had already promised tag `U64` — the mid-stream failure mode hazard 18 describes.
fn as_u64(value: &MyValue, col: &Column) -> Result<u64, PoolError> {
    u64_of(value).map_err(|detail| decode_err(col, detail))
}

/// The pure half of [`as_u64`] — returns the decode-mismatch DETAIL so the caller can stamp the
/// column identity on it (and so the dual-form rule is unit-testable without a live `Column`).
fn u64_of(value: &MyValue) -> Result<u64, String> {
    match value {
        MyValue::UInt(n) => Ok(*n),
        MyValue::Int(n) if *n >= 0 => Ok(*n as u64),
        // A negative value in an UNSIGNED column is corrupt input; wrapping it would silently
        // produce a ~1.8e19 "value" that never existed.
        MyValue::Int(n) => Err(format!(
            "negative value {n} in a BIGINT UNSIGNED column — the column classifier and the cell \
             disagree"
        )),
        other => Err(format!(
            "expected an unsigned integer cell, got a driver `{}` value",
            variant_name(other)
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
            format!(
                "expected a floating cell, got a driver `{}` value",
                variant_name(other)
            ),
        )),
    }
}

/// A byte-string cell: the binary protocol yields `Bytes` for every string/blob column.
fn as_bytes(value: &MyValue, col: &Column) -> Result<Vec<u8>, PoolError> {
    match value {
        MyValue::Bytes(b) => Ok(b.clone()),
        other => Err(decode_err(
            col,
            format!(
                "expected a byte-string cell, got a driver `{}` value",
                variant_name(other)
            ),
        )),
    }
}

/// The NAME of a driver value's variant — never its contents. A cell can hold user data, so an
/// error message may identify the shape it got but must not echo the payload (SPEC §12 secret
/// hygiene). This is the same discipline `mytext::wrong_variant` keeps.
fn variant_name(v: &MyValue) -> &'static str {
    match v {
        MyValue::NULL => "NULL",
        MyValue::Bytes(_) => "Bytes",
        MyValue::Int(_) => "Int",
        MyValue::UInt(_) => "UInt",
        MyValue::Float(_) => "Float",
        MyValue::Double(_) => "Double",
        MyValue::Date(..) => "Date",
        MyValue::Time(..) => "Time",
    }
}

/// The loud, diagnosable refusal for a column type Ferro does not support (charter rule 6 — never a
/// silent miscast). It LEADS with the two human-readable identifiers — the column name and the
/// native type — then the wire metadata it actually matched on, then the CURRENT supported set and
/// the deferrals by name, so an operator can tell "not implemented yet" from "you hit a bug" AND
/// know which column to change.
///
/// The column name is not a nicety: an `ENUM`/`SET`'s member list is defined per table, and a
/// MariaDB extended type reaches the wire as a plain `MYSQL_TYPE_STRING`, so the type code alone is
/// **not** something an operator can look up — it identifies a shape, not a column. The type code,
/// flags and charset are still included because they are the exact key [`column_kind`] matched on.
///
/// Kept in sync with [`column_kind`] by
/// `unsupported_message_names_the_column_and_the_current_supported_set`.
fn unsupported(col: &Column, what: &str) -> PoolError {
    PoolError::Unsupported(format!(
        "unsupported type for column '{}': MySQL {what} \
         (ColumnType={:?}, flags={:?}, charset={}). \
         Supported: NULL/BOOL/I64/U64/F64/TEXT/BYTES \
         (TINYINT(1), the integer widths, BIGINT UNSIGNED, FLOAT/DOUBLE, CHAR/VARCHAR/TEXT, \
         BINARY/VARBINARY/BLOB) plus the M1-S7 canonical tags DECIMAL, DATE, TIME, \
         TIMESTAMP (datetime), TIMESTAMPTZ (timestamp) and JSON (MySQL 8's JSON type; MariaDB's \
         JSON is a LONGTEXT alias and reads as TEXT). \
         Deferred: YEAR, BIT, SET, GEOMETRY and VECTOR.",
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

#[cfg(test)]
mod tests {
    use super::*;

    /// MySQL 8's `utf8mb4_0900_ai_ci` (45) and MariaDB 11's `utf8mb4_uca1400_ai_ci` (224) — both
    /// MEASURED off the live servers. Any non-63 collation is character data; the two ids are kept
    /// distinct here so a classifier that accidentally hard-coded one engine's number is caught.
    const UTF8MB4_MYSQL: u16 = 45;
    const UTF8MB4_MARIA: u16 = 224;
    const BIN: u16 = BINARY_COLLATION_ID;
    const NO_FLAGS: ColumnFlags = ColumnFlags::empty();
    const UNSIGNED: ColumnFlags = ColumnFlags::UNSIGNED_FLAG;

    /// A synthetic column carrying exactly the four wire-metadata fields the classifier reads.
    /// `Column`'s public builder (`mysql_common::packets::Column::new(..).with_*`) means the REAL
    /// `column_kind`/`column_to_tag`/`extract_value` are under test here — not a parallel helper
    /// that could drift from them.
    fn col(ct: ColumnType, flags: ColumnFlags, len: u32, charset: u16) -> Column {
        Column::new(ct)
            .with_name(b"c")
            .with_flags(flags)
            .with_column_length(len)
            .with_character_set(charset)
    }

    fn kind(ct: ColumnType, flags: ColumnFlags, len: u32, charset: u16) -> MyKind {
        column_kind(&col(ct, flags, len, charset))
            .unwrap_or_else(|e| panic!("{ct:?} must classify, got {e:?}"))
    }

    /// **Hazard 44.** The pre-S7 arm tested `unsigned` FIRST, so a `TINYINT(1) UNSIGNED` fell past
    /// the display-length branch — contradicting the §9.1 `TINYINT(1) → Bool` policy pinned in
    /// M1-S6. Display length 1 is now tested first, signed or unsigned.
    ///
    /// Both shapes are MEASURED: MySQL 8.4/MariaDB 11.8 report `TINYINT(1)` as
    /// `MYSQL_TYPE_TINY len=1 flags=0x0` and `TINYINT(1) UNSIGNED` as the same with `UNSIGNED_FLAG`.
    #[test]
    fn tinyint_display_length_one_is_bool_even_when_unsigned() {
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_TINY, NO_FLAGS, 1, BIN),
            MyKind::Bool
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_TINY, UNSIGNED, 1, BIN),
            MyKind::Bool
        );
        // A wider TINYINT is an integer in both signednesses (0..=255 and -128..=127 both fit i64).
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_TINY, UNSIGNED, 4, BIN),
            MyKind::I64
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_TINY, NO_FLAGS, 4, BIN),
            MyKind::I64
        );
    }

    /// **Hazard 23.** ONLY `BIGINT UNSIGNED` reaches `U64`; every narrower unsigned width fits `i64`
    /// losslessly and stays `I64`, which deliberately narrows the `U64` surface (§9's table lists
    /// U64 against "bigint unsigned" alone).
    #[test]
    fn only_bigint_unsigned_reaches_u64() {
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_LONGLONG, UNSIGNED, 20, BIN),
            MyKind::U64
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_LONGLONG, NO_FLAGS, 20, BIN),
            MyKind::I64,
            "a SIGNED bigint is still I64"
        );
        for ct in [
            ColumnType::MYSQL_TYPE_SHORT,
            ColumnType::MYSQL_TYPE_LONG,
            ColumnType::MYSQL_TYPE_INT24,
        ] {
            assert_eq!(
                kind(ct, UNSIGNED, 11, BIN),
                MyKind::I64,
                "{ct:?} UNSIGNED fits i64 losslessly"
            );
        }
    }

    /// The date/time family takes FOUR distinct kinds. `DATETIME` is naive (`TIMESTAMP` tag) and
    /// MySQL `TIMESTAMP` is a UTC instant (`TIMESTAMPTZ` tag) — SPEC §9, hazard 24. They must never
    /// collapse onto one arm: the driver components are byte-identical, so a swap is a silent zone
    /// shift with no error anywhere.
    ///
    /// MariaDB 11.8 reports `TIMESTAMP` with `UNSIGNED_FLAG | BINARY_FLAG` (MEASURED; MySQL 8.4
    /// sends `BINARY_FLAG` alone), so the date/time arms must ignore signedness entirely.
    #[test]
    fn date_family_takes_four_distinct_kinds_regardless_of_signedness() {
        for flags in [
            NO_FLAGS,
            UNSIGNED,
            ColumnFlags::BINARY_FLAG,
            UNSIGNED | ColumnFlags::BINARY_FLAG,
        ] {
            assert_eq!(
                kind(ColumnType::MYSQL_TYPE_DATE, flags, 10, BIN),
                MyKind::Date
            );
            assert_eq!(
                kind(ColumnType::MYSQL_TYPE_DATETIME, flags, 26, BIN),
                MyKind::Timestamp,
                "DATETIME is NAIVE"
            );
            assert_eq!(
                kind(ColumnType::MYSQL_TYPE_TIMESTAMP, flags, 26, BIN),
                MyKind::TimestampTz,
                "MySQL TIMESTAMP is a UTC instant (MariaDB tags it UNSIGNED — must not matter)"
            );
            assert_eq!(
                kind(ColumnType::MYSQL_TYPE_TIME, flags, 17, BIN),
                MyKind::Time
            );
        }
        assert_ne!(
            kind(ColumnType::MYSQL_TYPE_DATETIME, NO_FLAGS, 26, BIN),
            kind(ColumnType::MYSQL_TYPE_TIMESTAMP, NO_FLAGS, 26, BIN),
            "naive-vs-instant must never collapse onto one arm"
        );
    }

    /// Both decimal column types (`DECIMAL` is the pre-5.0 form, `NEWDECIMAL` what every live server
    /// actually sends) classify as `Decimal`, whatever their charset/flags.
    #[test]
    fn both_decimal_column_types_classify_as_decimal() {
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_NEWDECIMAL, NO_FLAGS, 32, BIN),
            MyKind::Decimal
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_DECIMAL, UNSIGNED, 32, BIN),
            MyKind::Decimal
        );
    }

    /// **Hazard 25 / F15.** A utf8 `LONGTEXT` must NEVER be promoted to `Json`: that is exactly how
    /// MariaDB's `JSON` column legitimately arrives (an alias for `LONGTEXT` + a `json_valid()`
    /// CHECK), and the driver exposes no distinguishing metadata — MEASURED on MariaDB 11.8, a
    /// `JSON` column reports `MYSQL_TYPE_BLOB, BLOB|BINARY flags, charset 224`, byte-identical to a
    /// plain `LONGTEXT`. Guessing would be the silent miscast charter rule 6 forbids.
    #[test]
    fn utf8_longtext_never_classifies_as_json() {
        for charset in [UTF8MB4_MYSQL, UTF8MB4_MARIA] {
            assert_eq!(
                kind(
                    ColumnType::MYSQL_TYPE_BLOB,
                    ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG,
                    4_294_967_295,
                    charset
                ),
                MyKind::Text,
                "charset {charset}: a utf8 LONGTEXT (== MariaDB's JSON) stays TEXT"
            );
        }
        // MySQL 8 emits a REAL type code for JSON — that, and only that, admits the JSON tag.
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_JSON,
                ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG,
                4_294_967_295,
                BIN
            ),
            MyKind::Json
        );
    }

    /// The string/blob family still splits Text vs Bytes on the binary collation alone — unchanged
    /// by S7, and asserted here because the JSON arm now sits next to it.
    #[test]
    fn string_family_still_splits_on_the_binary_collation() {
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_STRING,
                ColumnFlags::BINARY_FLAG,
                16,
                BIN
            ),
            MyKind::Bytes,
            "BINARY(16) — binary collation"
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_STRING, NO_FLAGS, 144, UTF8MB4_MYSQL),
            MyKind::Text,
            "CHAR(36) — MySQL 8 has no UUID type, so this stays TEXT (hazard 25)"
        );
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_VAR_STRING,
                NO_FLAGS,
                1020,
                UTF8MB4_MARIA
            ),
            MyKind::Text
        );
    }

    /// M1-S8a: an ENUM column classifies as its charset's string kind, in BOTH places the type can
    /// be spelled. The FIRST case is the one live traffic actually takes (measured: both engines
    /// send an ENUM as `MYSQL_TYPE_STRING | ENUM_FLAG`); the SECOND can only be reached from a
    /// hand-built `Column`, which is exactly why it is tested here and NOT in the live gate — a
    /// live test aimed at it could never fail.
    #[test]
    fn s8a_enum_classifies_as_a_string_in_both_spellings() {
        // The reachable spelling.
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_STRING,
                ColumnFlags::ENUM_FLAG,
                4,
                UTF8MB4_MYSQL
            ),
            MyKind::Text
        );
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_STRING,
                ColumnFlags::ENUM_FLAG,
                4,
                UTF8MB4_MARIA
            ),
            MyKind::Text
        );
        // A binary-collation ENUM takes the SAME charset branch as every other string type.
        assert_eq!(
            kind(
                ColumnType::MYSQL_TYPE_STRING,
                ColumnFlags::ENUM_FLAG,
                4,
                BIN
            ),
            MyKind::Bytes
        );
        // The unreachable-but-fixed spelling.
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_ENUM, NO_FLAGS, 4, UTF8MB4_MYSQL),
            MyKind::Text
        );
    }

    /// SET stays out of scope in BOTH spellings, and the refusal names the type in a way that
    /// CANNOT be satisfied by the message's trailing "Deferred: …" list (hazard 65).
    #[test]
    fn s8a_set_stays_unsupported_in_both_spellings() {
        for (ct, flags) in [
            (ColumnType::MYSQL_TYPE_STRING, ColumnFlags::SET_FLAG),
            (ColumnType::MYSQL_TYPE_SET, NO_FLAGS),
        ] {
            let c = col(ct, flags, 12, UTF8MB4_MARIA);
            let msg = match column_kind(&c) {
                Err(PoolError::Unsupported(m)) => m,
                other => panic!("{ct:?} must stay Unsupported, got {other:?}"),
            };
            assert!(
                msg.contains("MySQL SET ("),
                "the refusal must name SET as the OFFENDING type, not merely list it among the \
                 deferred ones: {msg}"
            );
        }
    }

    /// The DEFERRAL lock. These stay a loud `Unsupported` in S7 (§22.2) — repointed from the
    /// pre-S7 set, which claimed `BIGINT UNSIGNED`/`DECIMAL`/the date family were out of scope.
    ///
    /// **M1-S8a shrinks it by exactly one case.** `MYSQL_TYPE_STRING | ENUM_FLAG` — the spelling
    /// every live ENUM column actually arrives as — is now ADMITTED as a string, so it moved to
    /// `s8a_enum_classifies_as_a_string_in_both_spellings`. Everything else here is untouched.
    #[test]
    fn deferred_types_stay_unsupported() {
        let cases = [
            (
                ColumnType::MYSQL_TYPE_YEAR,
                UNSIGNED | ColumnFlags::ZEROFILL_FLAG,
                4u32,
                BIN,
            ),
            (ColumnType::MYSQL_TYPE_BIT, UNSIGNED, 8, BIN),
            (
                ColumnType::MYSQL_TYPE_STRING,
                ColumnFlags::SET_FLAG,
                12,
                UTF8MB4_MARIA,
            ),
            (ColumnType::MYSQL_TYPE_GEOMETRY, NO_FLAGS, 0, BIN),
            (ColumnType::MYSQL_TYPE_VECTOR, NO_FLAGS, 0, BIN),
            (ColumnType::MYSQL_TYPE_NULL, NO_FLAGS, 0, BIN),
        ];
        for (ct, flags, len, charset) in cases {
            let c = col(ct, flags, len, charset);
            assert!(
                matches!(column_kind(&c), Err(PoolError::Unsupported(_))),
                "{ct:?} ({flags:?}) must stay Unsupported in S7"
            );
            assert!(matches!(column_to_tag(&c), Err(PoolError::Unsupported(_))));
        }
    }

    /// **Hazard 23 / F5.** A `BIGINT UNSIGNED` cell ≤ `i64::MAX` arrives as `MyValue::Int`, NOT
    /// `MyValue::UInt` (`mysql_common-0.37.3/src/value/mod.rs:320-329` only produces `UInt` above
    /// `i64::MAX`) — MEASURED live: `0`, `5` and `4294967296` all came back as `Int`, only
    /// `18446744073709551615` as `UInt`. A `UInt`-only extractor would reject the COMMON case as a
    /// decode mismatch AFTER `HEAD` already promised tag `U64`.
    #[test]
    fn as_u64_accepts_both_driver_forms() {
        assert_eq!(u64_of(&MyValue::UInt(u64::MAX)).unwrap(), u64::MAX);
        assert_eq!(u64_of(&MyValue::UInt(0)).unwrap(), 0);
        // The small magnitudes are the ones a UInt-only extractor breaks on — every real-world row.
        assert_eq!(u64_of(&MyValue::Int(0)).unwrap(), 0);
        assert_eq!(u64_of(&MyValue::Int(5)).unwrap(), 5);
        assert_eq!(u64_of(&MyValue::Int(4_294_967_296)).unwrap(), 4_294_967_296);
        assert_eq!(u64_of(&MyValue::Int(i64::MAX)).unwrap(), i64::MAX as u64);
        // A negative Int in an UNSIGNED column is corrupt input, not a value to wrap around.
        assert!(u64_of(&MyValue::Int(-1)).is_err());
        assert!(u64_of(&MyValue::Double(1.0)).is_err());
        assert!(u64_of(&MyValue::Bytes(b"5".to_vec())).is_err());

        // And through the column-naming wrapper: a decode mismatch is Backend, never ConnectionLost.
        let c = col(ColumnType::MYSQL_TYPE_LONGLONG, UNSIGNED, 20, BIN);
        assert_eq!(as_u64(&MyValue::Int(5), &c).unwrap(), 5);
        assert!(matches!(
            as_u64(&MyValue::Int(-1), &c),
            Err(PoolError::Backend(_))
        ));
    }

    /// **THE LOCKSTEP GATE (hazard 18), offline.** `column_to_tag` runs at cols-build, BEFORE the
    /// query; `extract_value` runs per cell, MID-STREAM, after `HEAD` is already on the wire. For
    /// every newly-admitted column type, drive a REAL cell through the extractor and assert the
    /// emitted tag equals what the classifier promised — plus the exact canonical text, so a
    /// mis-routed renderer (DATE vs DATETIME) is caught here and not only live.
    ///
    /// **Completeness is enforced by the COMPILER, not by a count.** The matrix is *generated* from
    /// two exhaustive matches over [`MyKind`] ([`cases_for_kind`] and [`next_kind`]), so adding a
    /// variant to `MyKind` fails to compile until it has both a case and a place in the walk. The
    /// earlier `assert!(cases.len() >= 13)` was the silent-drift shape this slice has been deleting
    /// everywhere else: a 13th kind would have satisfied it while never being exercised.
    #[test]
    fn head_tag_equals_emitted_tag_for_every_admitted_column() {
        /// The case(s) for ONE kind: `(column, driver cell, canonical value)`. EXHAUSTIVE over
        /// `MyKind` — a new variant is a non-exhaustive-match compile error here.
        fn cases_for_kind(kind: MyKind) -> Vec<(Column, MyValue, Value)> {
            match kind {
                // ---- M0 set, unchanged.
                MyKind::Bool => vec![(
                    col(ColumnType::MYSQL_TYPE_TINY, NO_FLAGS, 1, BIN),
                    MyValue::Int(1),
                    Value::Bool(true),
                )],
                MyKind::I64 => vec![(
                    col(ColumnType::MYSQL_TYPE_LONGLONG, NO_FLAGS, 20, BIN),
                    MyValue::Int(-42),
                    Value::I64(-42),
                )],
                // Hazard 23: a `BIGINT UNSIGNED` ≤ `i64::MAX` arrives as `Int`, above it as `UInt` —
                // both driver forms belong to this ONE kind, hence two cases.
                MyKind::U64 => vec![
                    (
                        col(ColumnType::MYSQL_TYPE_LONGLONG, UNSIGNED, 20, BIN),
                        MyValue::Int(5),
                        Value::U64(5),
                    ),
                    (
                        col(ColumnType::MYSQL_TYPE_LONGLONG, UNSIGNED, 20, BIN),
                        MyValue::UInt(u64::MAX),
                        Value::U64(u64::MAX),
                    ),
                ],
                // The second case is f32-LOSSY on purpose (M1-S8a review): `2.5` alone is exactly
                // representable in `f32`, so it cannot witness a width loss anywhere on the F64
                // path — the blindness that let a narrowing BIND ship green. `0.1 + 0.2` narrows
                // to `0.30000001192092896`, so a truncating extractor is RED here.
                MyKind::F64 => vec![
                    (
                        col(ColumnType::MYSQL_TYPE_DOUBLE, NO_FLAGS, 22, BIN),
                        MyValue::Double(2.5),
                        Value::F64(2.5),
                    ),
                    (
                        col(ColumnType::MYSQL_TYPE_DOUBLE, NO_FLAGS, 22, BIN),
                        MyValue::Double(0.1 + 0.2),
                        Value::F64(0.1 + 0.2),
                    ),
                ],
                MyKind::Text => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_VAR_STRING,
                        NO_FLAGS,
                        1020,
                        UTF8MB4_MYSQL,
                    ),
                    MyValue::Bytes("héllo".as_bytes().to_vec()),
                    Value::Text("héllo".into()),
                )],
                MyKind::Bytes => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_BLOB,
                        ColumnFlags::BINARY_FLAG,
                        65535,
                        BIN,
                    ),
                    MyValue::Bytes(vec![0xde, 0xad]),
                    Value::Bytes(vec![0xde, 0xad]),
                )],
                // ---- M1-S7 admissions.
                MyKind::Decimal => vec![(
                    col(ColumnType::MYSQL_TYPE_NEWDECIMAL, NO_FLAGS, 32, BIN),
                    MyValue::Bytes(b"-12345.6700000000".to_vec()),
                    Value::Decimal("-12345.6700000000".into()),
                )],
                MyKind::Date => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_DATE,
                        ColumnFlags::BINARY_FLAG,
                        10,
                        BIN,
                    ),
                    MyValue::Date(2026, 8, 5, 0, 0, 0, 0),
                    Value::Date("2026-08-05".into()),
                )],
                MyKind::Time => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_TIME,
                        ColumnFlags::BINARY_FLAG,
                        17,
                        BIN,
                    ),
                    MyValue::Time(true, 34, 22, 59, 58, 1),
                    Value::Time("-838:59:58.000001".into()),
                )],
                MyKind::Timestamp => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_DATETIME,
                        ColumnFlags::BINARY_FLAG,
                        26,
                        BIN,
                    ),
                    MyValue::Date(2026, 8, 5, 11, 45, 7, 250_000),
                    Value::Timestamp("2026-08-05 11:45:07.250000".into()),
                )],
                MyKind::TimestampTz => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_TIMESTAMP,
                        UNSIGNED | ColumnFlags::BINARY_FLAG,
                        26,
                        BIN,
                    ),
                    MyValue::Date(2026, 8, 5, 11, 45, 7, 250_000),
                    Value::TimestampTz("2026-08-05T11:45:07.250000Z".into()),
                )],
                MyKind::Json => vec![(
                    col(
                        ColumnType::MYSQL_TYPE_JSON,
                        ColumnFlags::BLOB_FLAG | ColumnFlags::BINARY_FLAG,
                        4_294_967_295,
                        BIN,
                    ),
                    MyValue::Bytes(br#"{"a": 1}"#.to_vec()),
                    Value::Json(r#"{"a": 1}"#.into()),
                )],
            }
        }

        /// The variant chain that DRIVES the matrix, also EXHAUSTIVE over `MyKind`: a new variant
        /// cannot be walked without naming its predecessor's successor, so it cannot slip in
        /// uncovered. `None` ends the walk.
        fn next_kind(kind: MyKind) -> Option<MyKind> {
            Some(match kind {
                MyKind::Bool => MyKind::I64,
                MyKind::I64 => MyKind::U64,
                MyKind::U64 => MyKind::F64,
                MyKind::F64 => MyKind::Text,
                MyKind::Text => MyKind::Bytes,
                MyKind::Bytes => MyKind::Decimal,
                MyKind::Decimal => MyKind::Date,
                MyKind::Date => MyKind::Time,
                MyKind::Time => MyKind::Timestamp,
                MyKind::Timestamp => MyKind::TimestampTz,
                MyKind::TimestampTz => MyKind::Json,
                MyKind::Json => return None,
            })
        }

        let mut walked: Vec<MyKind> = Vec::new();
        let mut cursor = Some(MyKind::Bool);
        while let Some(kind) = cursor {
            assert!(
                !walked.contains(&kind),
                "the {kind:?} arm of `next_kind` re-enters the chain — the walk must visit each \
                 kind exactly once"
            );
            walked.push(kind);
            cursor = next_kind(kind);
        }

        for kind in &walked {
            let cases = cases_for_kind(*kind);
            assert!(!cases.is_empty(), "{kind:?} has no matrix case");
            for (c, cell, want) in &cases {
                // The case must actually be filed under the kind it claims — a case parked in the
                // wrong arm would otherwise leave its own kind untested.
                assert_eq!(
                    column_kind(c).expect("classifier admits this column"),
                    *kind,
                    "a {:?} column is filed under the {kind:?} arm",
                    c.column_type()
                );
                let head = column_to_tag(c).expect("classifier admits this column");
                let got = extract_value(cell, c).expect("producer fills it");
                assert_eq!(got, *want, "value for {:?}", c.column_type());
                assert_eq!(
                    head,
                    got.tag(),
                    "HEAD promised tag {head} for {:?} but the producer emitted {} — the \
                     cols-build gate and the per-cell gate disagree",
                    c.column_type(),
                    got.tag()
                );
                // A SQL NULL in the same column is `Value::Null`, never a decode error — and HEAD
                // still promises the column's own tag.
                assert_eq!(extract_value(&MyValue::NULL, c).unwrap(), Value::Null);
            }
        }
    }

    /// **Carry C16.** `mytext::date_to_text` rejects a cell carrying a time part, which is the
    /// classifier-mismatch guard: if DATE-vs-DATETIME routing is ever swapped, this fires. The
    /// refusal must arrive as `Backend` (a decode mismatch, §9.1) NAMING the column — never a panic,
    /// never `ConnectionLost`.
    #[test]
    fn a_misrouted_cell_is_a_named_backend_decode_error() {
        let c = col(
            ColumnType::MYSQL_TYPE_DATE,
            ColumnFlags::BINARY_FLAG,
            10,
            BIN,
        )
        .with_name(b"created_on");
        let err = extract_value(&MyValue::Date(2026, 8, 5, 13, 0, 0, 0), &c).unwrap_err();
        let PoolError::Backend(msg) = err else {
            panic!("a decode mismatch must be Backend (never ConnectionLost)");
        };
        assert!(msg.contains("created_on"), "must name the column: {msg}");
    }

    /// The `Unsupported` refusal must NAME the column and its native type — a bare type code is not
    /// actionable, and an `ENUM`/`SET` member list (or a MariaDB extended type) is install-local, so
    /// the column name is the only stable identifier an operator can act on. It must also describe
    /// the CURRENT supported set: the pre-S7 message claimed only NULL/BOOL/I64/F64/TEXT/BYTES.
    #[test]
    fn unsupported_message_names_the_column_and_the_current_supported_set() {
        let c = col(ColumnType::MYSQL_TYPE_YEAR, UNSIGNED, 4, BIN).with_name(b"hired_year");
        let msg = match column_to_tag(&c) {
            Err(PoolError::Unsupported(m)) => m,
            other => panic!("expected Unsupported, got {other:?}"),
        };
        assert!(msg.contains("hired_year"), "names the COLUMN: {msg}");
        assert!(msg.contains("YEAR"), "names the NATIVE type: {msg}");
        assert!(
            msg.contains("MYSQL_TYPE_YEAR"),
            "names the wire type code it matched on: {msg}"
        );
        for named in ["U64", "DECIMAL", "TIMESTAMPTZ", "JSON"] {
            assert!(
                msg.contains(named),
                "the supported set must mention {named}: {msg}"
            );
        }
        assert!(
            !msg.contains("only NULL/BOOL/I64/F64/TEXT/BYTES"),
            "the pre-S7 supported-set claim is now false: {msg}"
        );
    }
}
