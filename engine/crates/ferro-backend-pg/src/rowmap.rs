//! PG OID → canonical `Value` mapping (M0's scalar set, T-1; widened to the M1-S7 canonical tags)
//! and the OID-strict row extraction (MAJOR-8).
//!
//! tokio-postgres `FromSql` is **OID-strict**: `try_get::<_, i64>` accepts ONLY `int8`. `SELECT 1`
//! returns `int4`, so extraction MUST be driven off each column's ACTUAL OID — read it into the
//! Rust type that matches the OID, then widen into the canonical [`Value`]. Getting this wrong is
//! exactly the headline bug this module exists to prevent.
//!
//! Two tables, both keyed on the raw OID:
//! - [`oid_to_tag`] → the canonical `Value` tag for `ColMeta` (loud `Unsupported` outside the
//!   supported set — never a silent miscast);
//! - [`oid_extract_type`] → which Rust `FromSql` type to read the column as.
//!
//! **The two gates fire at DIFFERENT times and must move in LOCKSTEP (hazard 18).**
//! [`oid_to_tag`] runs at cols-build, BEFORE execution, with the connection still clean
//! (`query.rs` `run`/`stream`); [`extract_value`] runs **per cell, mid-stream, after `HEAD` is
//! already on the wire**. Admitting a type in one but not the other yields either a `HEAD`
//! promising a tag the producer cannot fill (an abort after the client has been told the shape) or
//! a column rejected pre-flight that could have been read. Both are driven off the single
//! [`oid_extract_type`] table precisely so they cannot drift; the live `pg_types_it.rs` proves the
//! agreement on real cells (`cols[i].tag == rows[0][i].tag()`).
//!
//! **M1-S7 canonical text.** The eight tags added in this slice (`DECIMAL`/`DATE`/`TIME`/
//! `TIMESTAMP`/`TIMESTAMPTZ`/`UUID`/`JSON`) are carried as canonical **text** (`PROTOCOL.md` §3.2),
//! rendered from the raw binary payload by [`crate::pgtext`]. Those arms read the column through
//! `pgtext::RawBytes`, whose `accepts` is universally true — see its docs: [`oid_extract_type`] is
//! the sole type authority and the raw getter is never reachable without passing it first
//! (hazard 16, locked by `raw_getter_is_only_named_behind_the_oid_gate`).
//!
//! All of the above is unit-tested (no Docker) against the `Type` OID constants, including the
//! still-deferred ones (`timetz`, arrays, `interval`, `inet`).

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
    // ---- M1-S7 canonical-text tags. Each reads the RAW binary payload (`pgtext::RawBytes`) and
    // renders it with the matching `pgtext` decoder; `postgres-types` either has no `FromSql` for
    // these at all (`numeric`) or only a lossy/ambiguous one (`SystemTime`'s
    // `accepts!(TIMESTAMP, TIMESTAMPTZ)` erases the naive-vs-UTC distinction by construction).
    /// `numeric` — raw base-10000 payload → `Value::Decimal` (display scale preserved).
    Numeric,
    /// `date` — raw i32 days from the 2000 epoch → `Value::Date`.
    Date,
    /// `time` — raw i64 µs since midnight → `Value::Time`. **Never `timetz`** (12-byte payload,
    /// no `FromSql` under any feature): that OID stays unadmitted.
    Time,
    /// `timestamp` — raw i64 µs from the 2000 epoch → **naive** `Value::Timestamp`.
    ///
    /// Byte-identical on the wire to [`ExtractType::TimestampTz`]; only the column OID separates
    /// naive-local from UTC-instant, so these two must never collapse onto one arm.
    Timestamp,
    /// `timestamptz` — the same raw i64 → `Value::TimestampTz`, rendered as a UTC instant with a
    /// literal `Z`. See [`ExtractType::Timestamp`].
    TimestampTz,
    /// `uuid` — 16 raw bytes → canonical lowercase hyphenated `Value::Uuid`.
    Uuid,
    /// `json` — raw document text → `Value::Json`, byte-exact passthrough.
    Json,
    /// `jsonb` — one version byte + the document text → `Value::Json` (PG normalizes jsonb, so the
    /// document text is PG's normalized form, not the client's input bytes).
    Jsonb,
}

/// Maps a PG column's type to the canonical `Value` tag (for `ColMeta`). Returns
/// `PoolError::Unsupported` for any OID outside the supported set — a loud typed error, never a
/// silent miscast (T-1).
///
/// Takes the column NAME and the resolved [`Type`] (not a bare `Oid`) so the refusal can name both:
/// a custom OID is **database-local** (the same `citext` is a different number in every install),
/// so an operator needs `"amount" (numeric)`, not just an integer, to act on the error.
///
/// This is the **cols-build (pre-execution)** half of the two-gate pair; [`extract_value`] is the
/// **per-cell (mid-stream)** half. Both are matches over [`oid_extract_type`] so the tag `HEAD`
/// promises is, by construction, the tag the producer emits — see the module docs (hazard 18).
pub fn oid_to_tag(col_name: &str, ty: &Type) -> Result<u8, PoolError> {
    match oid_extract_type(ty.oid()) {
        Some(ExtractType::Bool) => Ok(tag::BOOL),
        Some(ExtractType::I16 | ExtractType::I32 | ExtractType::I64) => Ok(tag::I64),
        Some(ExtractType::F32 | ExtractType::F64) => Ok(tag::F64),
        Some(ExtractType::Text) => Ok(tag::TEXT),
        Some(ExtractType::Bytes) => Ok(tag::BYTES),
        Some(ExtractType::Numeric) => Ok(tag::DECIMAL),
        Some(ExtractType::Date) => Ok(tag::DATE),
        Some(ExtractType::Time) => Ok(tag::TIME),
        Some(ExtractType::Timestamp) => Ok(tag::TIMESTAMP),
        Some(ExtractType::TimestampTz) => Ok(tag::TIMESTAMPTZ),
        Some(ExtractType::Uuid) => Ok(tag::UUID),
        Some(ExtractType::Json | ExtractType::Jsonb) => Ok(tag::JSON),
        None => Err(unsupported_column(col_name, ty)),
    }
}

/// Maps a PG column OID to the Rust type extraction must use. `None` ⇒ a type Ferro deliberately
/// does not support yet (`timetz`, arrays, `interval`, `inet`, and every enum/composite/range,
/// which arrive with a CUSTOM oid and are an S8 carry) — those stay a loud `Unsupported`.
///
/// **DOMAINs are NOT in that list and need no `Kind::Domain` unwrap.** PG resolves a domain to its
/// BASE type when it builds the `RowDescription` (`printtup.c` → `getBaseTypeAndTypmod`), so the
/// domain's own OID never reaches the wire: a domain over a supported base is admitted by that base
/// OID (`numeric` ⇒ 1700 ⇒ `DECIMAL`) and a domain over an unsupported base is refused by it
/// (`timetz` ⇒ 1266 ⇒ `Unsupported`). Proven live in `pg_types_it.rs`.
///
/// **This is the SOLE type authority.** `pgtext::RawBytes` accepts every `Type` by design, so a
/// raw read that skipped this table would decode an unsupported column as garbage (hazard 16).
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
        // ---- M1-S7. `TIMETZ` is deliberately ABSENT and must never be folded into the `TIME`
        // arm: its payload is 12 bytes (i64 µs + i32 zone), so it would be rejected mid-decode.
        o if o == Type::NUMERIC.oid() => Some(ExtractType::Numeric),
        o if o == Type::DATE.oid() => Some(ExtractType::Date),
        o if o == Type::TIME.oid() => Some(ExtractType::Time),
        o if o == Type::TIMESTAMP.oid() => Some(ExtractType::Timestamp),
        o if o == Type::TIMESTAMPTZ.oid() => Some(ExtractType::TimestampTz),
        o if o == Type::UUID.oid() => Some(ExtractType::Uuid),
        o if o == Type::JSON.oid() => Some(ExtractType::Json),
        o if o == Type::JSONB.oid() => Some(ExtractType::Jsonb),
        _ => None,
    }
}

/// Extracts column `idx` of `row` (whose OID is `oid`) into a canonical `Value`, OID-strict. A
/// SQL `NULL` in any column becomes `Value::Null` (read as `Option<T>` — `None`, never `WasNull`).
/// A deferred OID is `Unsupported`; a `try_get` or render failure on an in-set OID is a
/// client-side decode mismatch (NOT a connection loss), surfaced as `Backend` (NonRetryable) —
/// SPEC §9.1, so a decode bug can never mint a false §19.3 `Indeterminate`.
///
/// This is the **per-cell, mid-stream** gate: it fires AFTER `HEAD` is already on the wire, which
/// is why it and [`oid_to_tag`] are both matches over the single [`oid_extract_type`] table
/// (hazard 18). The M1-S7 arms read the raw binary payload through `pgtext::RawBytes` — every one
/// of those call sites is inside a match arm `oid_extract_type` has ALREADY selected, which is the
/// containment that keeps `RawBytes`'s universally-true `accepts` harmless (hazard 16).
pub fn extract_value(row: &Row, idx: usize, oid: Oid) -> Result<crate::Value, PoolError> {
    use crate::Value;
    use crate::pgtext::{self, RawBytes};

    /// Reads the raw binary payload of an ALREADY-GATED column and renders it to canonical text.
    /// SQL `NULL` short-circuits to `None` before any rendering. Returns `Option<String>` (not a
    /// `Value`) so each arm keeps the same `map_or(Value::Null, Value::X)` shape as the M0 arms —
    /// the canonical variant is named by the caller, right next to the OID it was gated on.
    fn raw_text<F>(row: &Row, idx: usize, render: F) -> Result<Option<String>, PoolError>
    where
        F: FnOnce(&[u8]) -> Result<String, PoolError>,
    {
        match get_opt::<RawBytes>(row, idx)? {
            Some(b) => render(b.0).map(Some),
            None => Ok(None),
        }
    }

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
        // ---- M1-S7 canonical text. Each arm is the producer half of the tag `oid_to_tag`
        // already promised in HEAD for this same OID; the pairs are asserted on real cells by the
        // live `pg_types_it.rs` (`cols[i].tag == rows[0][i].tag()`).
        Some(ExtractType::Numeric) => {
            Ok(raw_text(row, idx, pgtext::numeric_to_text)?.map_or(Value::Null, Value::Decimal))
        }
        Some(ExtractType::Date) => {
            Ok(raw_text(row, idx, pgtext::date_to_text)?.map_or(Value::Null, Value::Date))
        }
        Some(ExtractType::Time) => {
            Ok(raw_text(row, idx, pgtext::time_to_text)?.map_or(Value::Null, Value::Time))
        }
        // The next two read the SAME 8 raw bytes and differ ONLY in the renderer — naive vs UTC
        // instant. Swapping them is a silent zone shift with no error anywhere, which is why the
        // OID is the discriminator and `SystemTime`'s `accepts!(TIMESTAMP, TIMESTAMPTZ)` (which
        // erases the distinction) is deliberately not used.
        Some(ExtractType::Timestamp) => Ok(
            raw_text(row, idx, pgtext::timestamp_to_text)?.map_or(Value::Null, Value::Timestamp)
        ),
        Some(ExtractType::TimestampTz) => Ok(raw_text(row, idx, pgtext::timestamptz_to_text)?
            .map_or(Value::Null, Value::TimestampTz)),
        Some(ExtractType::Uuid) => {
            Ok(raw_text(row, idx, pgtext::uuid_to_text)?.map_or(Value::Null, Value::Uuid))
        }
        Some(ExtractType::Json) => Ok(raw_text(row, idx, |b| pgtext::json_to_text(b, false))?
            .map_or(Value::Null, Value::Json)),
        Some(ExtractType::Jsonb) => {
            Ok(raw_text(row, idx, |b| pgtext::json_to_text(b, true))?
                .map_or(Value::Null, Value::Json))
        }
        // Unreachable in practice — `oid_to_tag` refused this OID at cols-build, before the query
        // ran. Kept as the belt-and-braces half of the lockstep pair (hazard 18), and it names the
        // column off the ROW's own descriptor, which is where the identity lives mid-stream.
        None => Err(match row.columns().get(idx) {
            Some(col) => unsupported_column(col.name(), col.type_()),
            None => PoolError::Unsupported(format!(
                "unsupported column at index {idx} (PG OID {oid}), which is past the end of the \
                 row descriptor"
            )),
        }),
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

/// The loud, diagnosable refusal for a column type Ferro does not support (charter rule 6 — never
/// a silent miscast). The message LEADS with the two human-readable identifiers — the column name
/// and PG's own name for its type — then the OID, the CURRENT supported set, and the deferrals by
/// name, so an operator can tell "not implemented yet" from "you hit a bug" AND know which column
/// to change.
///
/// The name/type pair is not a nicety: a custom OID (enum, composite, `citext`) is assigned per
/// database, so the bare number is **not reproducible across installs** and identifies nothing on
/// its own. It is still included because it is the exact key `oid_extract_type` matched on.
///
/// Kept in sync with [`oid_extract_type`] by `unsupported_message_describes_the_current_supported_set`.
fn unsupported_column(col_name: &str, ty: &Type) -> PoolError {
    PoolError::Unsupported(format!(
        "unsupported type for column \"{col_name}\": PG type {} (OID {}). \
         Supported: NULL/BOOL/I64/F64/TEXT/BYTES \
         (bool, int2/4/8, float4/8, text/varchar/bpchar, bytea) plus the M1-S7 canonical tags \
         DECIMAL (numeric), DATE, TIME, TIMESTAMP, TIMESTAMPTZ, UUID and JSON (json/jsonb). \
         Deferred: timetz, arrays, interval, inet, and every enum/composite/range type. \
         (A DOMAIN is reported by PG as its BASE type, so it is supported iff that base is.)",
        ty.name(),
        ty.oid()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `oid_to_tag` under test always gets a placeholder column name — the name only ever reaches
    /// the `Unsupported` message, never the tag decision.
    fn tag_of(ty: &Type) -> Result<u8, PoolError> {
        oid_to_tag("c", ty)
    }

    #[test]
    fn oid_to_tag_covers_m0_scalar_set() {
        assert_eq!(tag_of(&Type::BOOL).unwrap(), tag::BOOL);
        assert_eq!(tag_of(&Type::INT2).unwrap(), tag::I64);
        assert_eq!(tag_of(&Type::INT4).unwrap(), tag::I64);
        assert_eq!(tag_of(&Type::INT8).unwrap(), tag::I64);
        assert_eq!(tag_of(&Type::FLOAT4).unwrap(), tag::F64);
        assert_eq!(tag_of(&Type::FLOAT8).unwrap(), tag::F64);
        assert_eq!(tag_of(&Type::TEXT).unwrap(), tag::TEXT);
        assert_eq!(tag_of(&Type::VARCHAR).unwrap(), tag::TEXT);
        assert_eq!(tag_of(&Type::BPCHAR).unwrap(), tag::TEXT);
        assert_eq!(tag_of(&Type::BYTEA).unwrap(), tag::BYTES);
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

    /// The M1-S7 admissions. Was `out_of_m0_oid_is_unsupported`, which asserted the exact opposite
    /// for TIMESTAMPTZ/UUID/NUMERIC/JSONB — those are supported as of this slice, so the assertion
    /// is REPOINTED (see `timetz_and_deferred_oids_stay_unsupported`), not deleted.
    #[test]
    fn s7_oids_are_admitted_with_the_right_tag() {
        for (ty, want) in [
            (Type::NUMERIC, tag::DECIMAL),
            (Type::DATE, tag::DATE),
            (Type::TIME, tag::TIME),
            (Type::TIMESTAMP, tag::TIMESTAMP),
            (Type::TIMESTAMPTZ, tag::TIMESTAMPTZ),
            (Type::UUID, tag::UUID),
            (Type::JSON, tag::JSON),
            (Type::JSONB, tag::JSON),
        ] {
            assert!(
                oid_extract_type(ty.oid()).is_some(),
                "{ty:?} must be admitted in S7"
            );
            assert_eq!(tag_of(&ty).unwrap(), want, "{ty:?} tag");
        }
    }

    /// TIMESTAMP and TIMESTAMPTZ share an IDENTICAL 8-byte payload — only the column OID separates
    /// naive-local from UTC-instant, so a swapped pair is a silent zone shift with no error. Pin
    /// the two OIDs to DISTINCT extraction arms here (the live `pg_types_it.rs` proves the
    /// rendering itself against PG's own oracle under a non-UTC session zone).
    #[test]
    fn timestamp_and_timestamptz_take_distinct_arms() {
        assert_eq!(
            oid_extract_type(Type::TIMESTAMP.oid()),
            Some(ExtractType::Timestamp)
        );
        assert_eq!(
            oid_extract_type(Type::TIMESTAMPTZ.oid()),
            Some(ExtractType::TimestampTz)
        );
        assert_ne!(
            oid_extract_type(Type::TIMESTAMP.oid()),
            oid_extract_type(Type::TIMESTAMPTZ.oid()),
            "the two must never collapse onto one arm"
        );
    }

    /// The DEFERRAL guard (hazards 15/20). Note it asserts `oid_extract_type(..).is_none()` — it is
    /// a deferral lock, NOT the hazard-16 "raw getter is unreachable" guard (that one is
    /// `raw_getter_is_only_named_behind_the_oid_gate`).
    ///
    /// `TIMETZ` in particular must never fall into the `TIME` arm: its payload is 12 bytes
    /// (i64 µs + i32 zone), so it has no `FromSql` under any feature and would be rejected
    /// mid-decode — after HEAD is already on the wire.
    #[test]
    fn timetz_and_deferred_oids_stay_unsupported() {
        for ty in [Type::TIMETZ, Type::INT4_ARRAY, Type::INTERVAL, Type::INET] {
            assert!(
                oid_extract_type(ty.oid()).is_none(),
                "{ty:?} must stay Unsupported in S7"
            );
            assert!(
                matches!(tag_of(&ty), Err(PoolError::Unsupported(_))),
                "{ty:?} tag must be Unsupported"
            );
        }
    }

    /// The `Unsupported` message must describe the CURRENT contract. It said "out-of-M0 … only
    /// NULL/BOOL/I64/F64/TEXT/BYTES are supported in M0" until M1-S7, which is now false in both
    /// directions (the set is wider, and "M0" is the wrong milestone).
    ///
    /// It must also NAME THE COLUMN and its native type. A custom OID is database-local (the same
    /// enum is a different number in every install), so a bare number identifies nothing an
    /// operator can act on; the column name plus PG's own type name do.
    #[test]
    fn unsupported_message_describes_the_current_supported_set() {
        let msg = match oid_to_tag("elapsed", &Type::INTERVAL) {
            Err(PoolError::Unsupported(m)) => m,
            other => panic!("expected Unsupported, got {other:?}"),
        };
        assert!(
            !msg.contains("M0"),
            "the message still names the M0 milestone: {msg}"
        );
        assert!(
            msg.contains("elapsed"),
            "message must name the offending COLUMN: {msg}"
        );
        assert!(
            msg.contains(Type::INTERVAL.name()),
            "message must name the column's NATIVE type ({}): {msg}",
            Type::INTERVAL.name()
        );
        for named in ["DECIMAL", "TIMESTAMPTZ", "UUID", "JSON", "interval"] {
            assert!(msg.contains(named), "message must mention {named}: {msg}");
        }
        assert!(
            msg.contains(&Type::INTERVAL.oid().to_string()),
            "message must name the offending OID: {msg}"
        );
        // F1: PG resolves a DOMAIN to its base type in the RowDescription, so domains are NOT a
        // deferral — the message must not claim they are.
        assert!(
            !msg.contains("domain/enum") && !msg.contains("domains"),
            "domains are NOT deferred (PG reports the base type); the message must not say so: \
             {msg}"
        );
    }

    /// **Hazard 16, mechanically enforced.** `pgtext::RawBytes` is a `FromSql` whose
    /// `accepts(_) -> true` DEFEATS tokio-postgres' own type check; `oid_extract_type` must remain
    /// the SOLE type authority. If any path calls the raw getter without first passing the OID
    /// gate, an unsupported/unknown type decodes as garbage instead of raising the loud
    /// `Unsupported` this module exists to guarantee.
    ///
    /// The containment is structural, and this test locks the structure: `RawBytes` is
    /// `pub(crate)` (so it cannot leak out of the crate at all), and inside the crate it is NAMED
    /// only in `pgtext.rs` (its definition) and inside `extract_value`'s LINE SPAN in `rowmap.rs`,
    /// which the OID gate has already run for. A `get_opt::<RawBytes>` added anywhere else reddens
    /// this test.
    ///
    /// **The span check is deliberately not a `fn`-chunk split** (T4b review F3). Splitting the
    /// source on `"\nfn "`/`"\npub fn "` made every other qualifier a non-boundary, so a
    /// `pub(crate) fn` / `async fn` / `const fn` / `pub(super) fn` inserted right after
    /// `extract_value` rode INSIDE its chunk and escaped the guard — demonstrated live with a
    /// `pub(crate) fn evil_raw_read` that called `get_opt::<RawBytes>` ungated and left the test
    /// GREEN. Line containment has no qualifier vocabulary to keep up with: `extract_value` runs
    /// from its `pub fn` line to its column-0 `}`, and every line naming `RawBytes` must be inside
    /// that range, whatever surrounds it.
    #[test]
    fn raw_getter_is_only_named_behind_the_oid_gate() {
        let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        collect_rs(&src_dir, &mut files);
        assert!(
            files.len() >= 8,
            "source walk found too few files: {files:?}"
        );

        let mut checked_definition = false;
        let mut checked_call_sites = false;
        for f in &files {
            let name = f.file_name().unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(f).expect("read source");
            // Comments are documentation, not reachability — the guard is about CODE, and every
            // containment note in this crate necessarily names the type it is describing. The
            // `#[cfg(test)]` tail is cut for the same reason: this very test names `RawBytes`,
            // and a test binary is not a production read path.
            let code = strip_comments(text.split("\n#[cfg(test)]").next().unwrap_or(&text));
            match name.as_str() {
                "pgtext.rs" => {
                    assert!(
                        code.contains("pub(crate) struct RawBytes"),
                        "RawBytes must be declared `pub(crate)` in pgtext.rs — a `pub` one escapes \
                         the crate entirely and the OID gate with it"
                    );
                    checked_definition = true;
                }
                "rowmap.rs" => {
                    // `extract_value`'s LINE SPAN: its top-level `pub fn` line through the first
                    // column-0 `}`. Every line naming RawBytes must fall inside it — no `fn`
                    // qualifier can widen the span, because the span is not made of `fn`s.
                    let lines: Vec<&str> = code.lines().collect();
                    let start = lines
                        .iter()
                        .position(|l| l.starts_with("pub fn extract_value"))
                        .expect("`extract_value` must be a top-level `pub fn` in rowmap.rs");
                    let end = start
                        + 1
                        + lines[start + 1..]
                            .iter()
                            .position(|l| *l == "}")
                            .expect("`extract_value` must close with a column-0 `}`");
                    for (i, l) in lines.iter().enumerate() {
                        if !l.contains("RawBytes") {
                            continue;
                        }
                        assert!(
                            (start..=end).contains(&i),
                            "rowmap.rs line {} names RawBytes OUTSIDE `extract_value` (lines \
                             {}..={}), which would bypass the OID gate (hazard 16):\n{}",
                            i + 1,
                            start + 1,
                            end + 1,
                            l.trim()
                        );
                        checked_call_sites = true;
                    }
                }
                _ => assert!(
                    !code.contains("RawBytes"),
                    "{name} names RawBytes in code; only pgtext.rs (the definition) and \
                     rowmap.rs's `extract_value` (behind the OID gate) may"
                ),
            }
        }
        assert!(
            checked_definition,
            "pgtext.rs was not visited — the source walk is broken, not the invariant"
        );
        assert!(
            checked_call_sites,
            "no RawBytes call site was found in rowmap.rs — either the M1-S7 arms regressed to a \
             typed FromSql, or this guard has stopped looking where the code is"
        );
    }

    /// Drops WHOLE-LINE `//` comments (which is every doc comment in this crate) and nothing else.
    ///
    /// Deliberately does NOT strip a trailing `// …` from a code line: that would need to
    /// distinguish a real comment from a `//` inside a string literal, and getting it wrong strips
    /// real code — making the guard silently LOOSER, the one direction a safety guard must never
    /// fail in. Leaving trailing comments in place can only ever make it stricter (a trailing
    /// comment that names `RawBytes` outside `extract_value` fails the assertion, and the fix is to
    /// reword the comment).
    fn strip_comments(text: &str) -> String {
        text.lines()
            .map(|l| {
                if l.trim_start().starts_with("//") {
                    ""
                } else {
                    l
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn collect_rs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).expect("read_dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                collect_rs(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
}
