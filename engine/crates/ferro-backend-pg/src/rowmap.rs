//! PG OID → canonical `Value` mapping (M0's scalar set, T-1; widened to the M1-S7 canonical tags)
//! and the OID-strict row extraction (MAJOR-8).
//!
//! tokio-postgres `FromSql` is **OID-strict**: `try_get::<_, i64>` accepts ONLY `int8`. `SELECT 1`
//! returns `int4`, so extraction MUST be driven off each column's ACTUAL OID — read it into the
//! Rust type that matches the OID, then widen into the canonical [`Value`]. Getting this wrong is
//! exactly the headline bug this module exists to prevent.
//!
//! Two tables, both keyed on the raw OID:
//! - [`oid_to_tag`] → the canonical `Value` tag for `ColMeta`;
//! - [`oid_extract_type`] → which Rust `FromSql` type to read the column as, or `None` for the
//!   M1-S8c TEXT FALLBACK.
//!
//! **M1-S8c — the TEXT FALLBACK (D-S8b-6).** An OID OUTSIDE the canonical set is no longer a loud
//! `Unsupported`. It is read as [`tag::TEXT`] carrying **PostgreSQL's own text rendering** — the
//! exact bytes `pdo_pgsql`/libpq hand back, because libpq asks for the same TEXT result format.
//! The 14 canonical tags keep their typed, policy-aware, BINARY path untouched.
//!
//! That is not a §9.1 "guess": §9.1 forbids inferring SEMANTICS from a type we do not know, and the
//! old `Unsupported` existed to stop us decoding GARBAGE. PG's `typoutput` is neither — it is
//! exact, lossless, and already what every Doctrine application is written against. It unblocks the
//! stock `PostgreSQLSchemaManager` (`pg_index.indkey` is an `int2vector`, and the catalog spans
//! `oidvector`, `_text`, `_oid`, `_aclitem`, `pg_node_tree`, `anyarray`, `xid`, …), and with it
//! `doctrine/migrations`, plus PostGIS, `hstore`, `ltree`, `citext` and every native enum — with no
//! `/proto` change, since `TAG_TEXT` already exists (charter rule 2).
//!
//! **MECHANISM, and the one hazard it creates.** `tokio-postgres` asks the server for BINARY for
//! every result column and has no `FromSql` for any of these types, so their *binary* payload is
//! undecodable in principle (an extension's `typsend` is arbitrary C). The vendored fork therefore
//! gained a per-`Client` **result-format policy** (`Client::set_result_format_policy`,
//! `/UPSTREAM_PR.md`): at PREPARE time each `Column` records the `Bind` result-format code it was
//! described with — `1` binary for a canonical OID, `0` TEXT for a fallback one — and the `Bind`
//! encoder projects those same fields onto the wire. `PgBackend::connect` installs the policy,
//! whose predicate IS [`oid_extract_type`].
//!
//! The hazard is decoding a BINARY payload as if it were text (silent garbage — hazard 16's
//! sibling). It is closed by construction, not by a call-site convention: the fallback arm of
//! [`extract_value`] refuses unless `Column::result_format()` — *the very field that chose the wire
//! format* — says `RESULT_FORMAT_TEXT`. A connection with no policy installed (a raw
//! `tokio_postgres::connect` in a test, say) reports binary, so the fallback raises a loud
//! `Backend` error instead of rendering nonsense.
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
//! rendered from the raw binary payload by [`crate::pgtext`]. Those arms read the column through a
//! raw-passthrough `FromSql` whose `accepts` is universally true, declared as a FUNCTION-LOCAL item
//! inside [`extract_value`] itself: [`oid_extract_type`] is the sole type authority, and the raw
//! getter is unnameable — hence unreachable — anywhere the gate has not already run (hazard 16,
//! locked by `the_only_raw_from_sql_is_inside_the_oid_gate`).
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
    // ---- M1-S7 canonical-text tags. Each reads the RAW binary payload (through the gate-local
    // raw `FromSql` declared inside `extract_value`) and renders it with the matching `pgtext`
    // decoder; `postgres-types` either has no `FromSql` for
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
    // ---- M1-S8a catalog scalars (the types DBAL's `AbstractSchemaManager` selects). `name`
    // (OID 19) is NOT here: `Type::NAME` is already in `String`'s `FromSql::accepts` list, so it
    // folds straight into the existing `Text` arm with no new machinery.
    /// `"char"` (OID 18) — ONE byte read as `i8`, rendered by [`crate::pgtext::char_byte_to_text`].
    /// Not `Text`: the payload is a bare byte, so `String`'s `FromSql` would reject it outright.
    CharByte,
    /// `oid` (OID 26) — read as `u32`, widened losslessly to `Value::I64`.
    OidU32,
    /// The `reg*` alias family (`regtype` 2206, `regclass` 2205) — their BINARY payload is a bare
    /// 4-byte OID (`regtypesend` IS `oidsend`), so the only truthful report is the numeric oid.
    /// A caller wanting the NAME casts in SQL (`::text` / `format_type(...)`); resolving it here
    /// would mean a catalog round trip the engine must not make (charter rule 6).
    RegOid,
}

/// Maps a PG column OID to the canonical `Value` tag (for `ColMeta`).
///
/// **Infallible since M1-S8c** (D-S8b-6): an OID outside the canonical set is [`tag::TEXT`], the
/// TEXT FALLBACK, not a refusal. It used to return `Result<u8, PoolError>` and take the column name
/// so the `Unsupported` could name it; there is no refusal left to name.
///
/// This is the **cols-build (pre-execution)** half of the two-gate pair; [`extract_value`] is the
/// **per-cell (mid-stream)** half. Both are matches over [`oid_extract_type`] so the tag `HEAD`
/// promises is, by construction, the tag the producer emits — see the module docs (hazard 18).
pub fn oid_to_tag(oid: Oid) -> u8 {
    match oid_extract_type(oid) {
        Some(ExtractType::Bool) => tag::BOOL,
        Some(
            ExtractType::I16
            | ExtractType::I32
            | ExtractType::I64
            | ExtractType::OidU32
            | ExtractType::RegOid,
        ) => tag::I64,
        Some(ExtractType::F32 | ExtractType::F64) => tag::F64,
        Some(ExtractType::Text | ExtractType::CharByte) => tag::TEXT,
        Some(ExtractType::Bytes) => tag::BYTES,
        Some(ExtractType::Numeric) => tag::DECIMAL,
        Some(ExtractType::Date) => tag::DATE,
        Some(ExtractType::Time) => tag::TIME,
        Some(ExtractType::Timestamp) => tag::TIMESTAMP,
        Some(ExtractType::TimestampTz) => tag::TIMESTAMPTZ,
        Some(ExtractType::Uuid) => tag::UUID,
        Some(ExtractType::Json | ExtractType::Jsonb) => tag::JSON,
        // M1-S8c TEXT FALLBACK (D-S8b-6) — PG's own `typoutput`, carried as TEXT. `extract_value`
        // fills exactly this tag from the TEXT-format payload, and refuses if the payload is not
        // actually text (see there).
        None => tag::TEXT,
    }
}

/// **The `Bind` result-format policy this crate installs on every pooled connection** (M1-S8c,
/// `PgBackend::connect`): `true` ⇒ ask PostgreSQL for this OID in BINARY, `false` ⇒ ask for
/// PostgreSQL's own TEXT output.
///
/// It is [`oid_extract_type`] and nothing else — the canonical set is exactly the set with a binary
/// decoder, and everything else is the TEXT FALLBACK. Keeping the predicate here, rather than
/// inlining `is_some()` at the install site, is what makes "the format on the wire and the decoder
/// read ONE table" a property of this file instead of a convention.
pub fn wants_binary_result(oid: Oid) -> bool {
    oid_extract_type(oid).is_some()
}

/// Maps a PG column OID to the Rust type extraction must use, i.e. the CANONICAL set.
///
/// `None` ⇒ the M1-S8c **TEXT FALLBACK** (D-S8b-6): PostgreSQL renders the column with its own
/// `typoutput` and it reaches the client as [`tag::TEXT`], verbatim. That covers `timetz`, every
/// array (`_int2`, `_text`, `_oid`, `_aclitem`, …), `int2vector`/`oidvector`, `interval`, `inet`,
/// `pg_node_tree`, `anyarray`, `xid`, and every enum/composite/range/extension type, which arrive
/// with a CUSTOM (database-local) oid.
///
/// This function is ALSO the connection's `Bind` result-format policy — see
/// [`wants_binary_result`]. A `None` here is what puts the column in TEXT format on the wire, so
/// this table decides the wire format and the decoder in one place.
///
/// **DOMAINs are NOT in that list and need no `Kind::Domain` unwrap.** PG resolves a domain to its
/// BASE type when it builds the `RowDescription` (`printtup.c` → `getBaseTypeAndTypmod`), so the
/// domain's own OID never reaches the wire: a domain over a supported base is admitted by that base
/// OID (`numeric` ⇒ 1700 ⇒ `DECIMAL`) and a domain over an unsupported base is refused by it
/// (`timetz` ⇒ 1266 ⇒ `Unsupported`). Proven live in `pg_types_it.rs`.
///
/// **This is the SOLE type authority.** The raw-passthrough `FromSql` the M1-S7 arms read through
/// accepts every `Type` by design, so a raw read that skipped this table would decode an
/// unsupported column as garbage (hazard 16). That is why it is declared INSIDE [`extract_value`].
pub fn oid_extract_type(oid: Oid) -> Option<ExtractType> {
    match oid {
        o if o == Type::BOOL.oid() => Some(ExtractType::Bool),
        o if o == Type::INT2.oid() => Some(ExtractType::I16),
        o if o == Type::INT4.oid() => Some(ExtractType::I32),
        o if o == Type::INT8.oid() => Some(ExtractType::I64),
        o if o == Type::FLOAT4.oid() => Some(ExtractType::F32),
        o if o == Type::FLOAT8.oid() => Some(ExtractType::F64),
        // `name` (OID 19) folds into the EXISTING Text arm: `Type::NAME` is already in `String`'s
        // `FromSql::accepts` list, so no raw read and no new extraction kind is involved.
        o if o == Type::TEXT.oid()
            || o == Type::VARCHAR.oid()
            || o == Type::BPCHAR.oid()
            || o == Type::NAME.oid() =>
        {
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
        // ---- M1-S8a catalog scalars.
        o if o == Type::CHAR.oid() => Some(ExtractType::CharByte),
        o if o == Type::OID.oid() => Some(ExtractType::OidU32),
        o if o == Type::REGTYPE.oid() || o == Type::REGCLASS.oid() => Some(ExtractType::RegOid),
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
/// (hazard 18). The M1-S7 arms read the raw binary payload through a raw-passthrough `FromSql`
/// declared as a function-local item BELOW — unnameable outside this function, so every one of its
/// call sites is necessarily inside a match arm `oid_extract_type` has ALREADY selected. That is
/// the containment which keeps its universally-true `accepts` harmless (hazard 16).
pub fn extract_value(row: &Row, idx: usize, oid: Oid) -> Result<crate::Value, PoolError> {
    use crate::Value;
    use crate::pgtext;

    /// A raw-payload passthrough `FromSql`: hands back the column's binary bytes untouched, so the
    /// `pgtext` decoders can render them (result format is BINARY and is not per-statement
    /// selectable — see that module's docs).
    ///
    /// **DANGER (hazard 16) — this is why it is declared HERE, inside the gate.** Its `accepts` is
    /// universally `true`, which **DEFEATS tokio-postgres' own type check**: `try_get::<RawBytes>`
    /// will happily hand back the bytes of *any* column, including a type Ferro does not support
    /// and cannot render, which would decode as garbage instead of raising the loud `Unsupported`
    /// this module exists to guarantee (charter rule 6, "no silent miscasts").
    /// [`oid_extract_type`] is the SOLE type authority, and the containment is now the COMPILER's,
    /// not a grep's: a function-local item is unnameable outside `extract_value`, so no other
    /// function — in this file, in `pgtext.rs`, or anywhere in the crate — can reach the raw
    /// getter, directly or through an indirection. It previously lived in `pgtext.rs` as a
    /// `pub(crate) struct`, where a `pub(crate) fn raw_slice(row, idx)` wrapper called ungated from
    /// `query.rs` compiled cleanly and left the name-based guard GREEN.
    struct RawBytes<'a>(&'a [u8]);

    impl<'a> tokio_postgres::types::FromSql<'a> for RawBytes<'a> {
        fn from_sql(
            _ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            Ok(RawBytes(raw))
        }

        /// Universally true BY DESIGN — see the type docs. The OID gate, not this predicate,
        /// decides what may be read.
        fn accepts(_ty: &Type) -> bool {
            true
        }
    }

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
        // ---- M1-S8a catalog scalars.
        Some(ExtractType::CharByte) => Ok(get_opt::<i8>(row, idx)?
            .map_or(Ok(Value::Null), |b| {
                pgtext::char_byte_to_text(b as u8).map(Value::Text)
            })?),
        Some(ExtractType::OidU32) => {
            Ok(get_opt::<u32>(row, idx)?.map_or(Value::Null, |n| Value::I64(i64::from(n))))
        }
        // `u32`'s `FromSql::accepts` is `Type::OID` ONLY, so a `regtype`/`regclass` cannot use it.
        // The payload is nevertheless a 4-byte big-endian oid, read here through the gate-local raw
        // getter and decoded explicitly — never guessed.
        Some(ExtractType::RegOid) => Ok(match get_opt::<RawBytes>(row, idx)? {
            None => Value::Null,
            Some(b) => {
                let arr: [u8; 4] = b.0.try_into().map_err(|_| {
                    PoolError::Backend(format!(
                        "PG reg* payload must be 4 bytes, got {}",
                        b.0.len()
                    ))
                })?;
                Value::I64(i64::from(u32::from_be_bytes(arr)))
            }
        }),
        // ---- M1-S8c: THE TEXT FALLBACK (D-S8b-6). PostgreSQL rendered this column with its own
        // `typoutput` because the `Bind` asked for the TEXT result format for it — the same bytes
        // libpq/`pdo_pgsql` return. `oid_to_tag` promised `TAG_TEXT` for this OID at cols-build.
        None => {
            let col = row.columns().get(idx).ok_or_else(|| {
                PoolError::Backend(format!(
                    "column at index {idx} (PG OID {oid}) is past the end of the row descriptor"
                ))
            })?;
            // THE GATE, and the reason this cannot silently miscast (module docs). The raw
            // `FromSql` below hands back whatever bytes the server sent; interpreting BINARY bytes
            // as text would be exactly the garbage read the loud `Unsupported` used to prevent. So
            // we consult `Column::result_format()` — the SAME field the fork's `Bind` encoder put
            // on the wire, resolved at prepare time from this connection's result-format policy.
            // A connection with NO policy installed reports binary and lands here, loudly.
            if col.result_format() != tokio_postgres::RESULT_FORMAT_TEXT {
                return Err(PoolError::Backend(format!(
                    "column \"{}\": PG type {} (OID {}) has no canonical Ferro tag, so it must be \
                     read through the M1-S8c TEXT fallback — but this connection returned it in \
                     the BINARY result format (code {}). The `Bind` result-format policy \
                     (`Client::set_result_format_policy`, installed by `PgBackend::connect`) is \
                     missing on this connection; decoding the binary payload as text would be a \
                     silent miscast, so this is refused instead.",
                    col.name(),
                    col.type_().name(),
                    oid,
                    col.result_format(),
                )));
            }
            Ok(raw_text(row, idx, |b| fallback_text(col, b))?.map_or(Value::Null, Value::Text))
        }
    }
}

/// Renders a TEXT-format cell payload — PostgreSQL's own `typoutput` bytes — as the canonical
/// `TAG_TEXT` string (M1-S8c, D-S8b-6). A pure passthrough: PG emits text in the connection's
/// `client_encoding`, which `tokio-postgres` fixes at `UTF8` on every connect, so the only
/// transformation is the UTF-8 validation the wire contract requires.
///
/// Invalid UTF-8 is a LOUD `Backend` decode mismatch, never a lossy replacement character and never
/// `ConnectionLost` — the same rule `char_byte_to_text` follows, so a decode bug can never mint a
/// false §19.3 `Indeterminate` (SPEC §9.1).
fn fallback_text(col: &tokio_postgres::Column, raw: &[u8]) -> Result<String, PoolError> {
    String::from_utf8(raw.to_vec()).map_err(|e| {
        PoolError::Backend(format!(
            "column \"{}\": PG type {} (OID {}) text output is not valid UTF-8 at byte {} \
             (client_encoding must be UTF8)",
            col.name(),
            col.type_().name(),
            col.type_().oid(),
            e.utf8_error().valid_up_to(),
        ))
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `oid_to_tag` keys on the bare OID since M1-S8c; this keeps the tests reading in `Type`s.
    fn tag_of(ty: &Type) -> u8 {
        oid_to_tag(ty.oid())
    }

    #[test]
    fn oid_to_tag_covers_m0_scalar_set() {
        assert_eq!(tag_of(&Type::BOOL), tag::BOOL);
        assert_eq!(tag_of(&Type::INT2), tag::I64);
        assert_eq!(tag_of(&Type::INT4), tag::I64);
        assert_eq!(tag_of(&Type::INT8), tag::I64);
        assert_eq!(tag_of(&Type::FLOAT4), tag::F64);
        assert_eq!(tag_of(&Type::FLOAT8), tag::F64);
        assert_eq!(tag_of(&Type::TEXT), tag::TEXT);
        assert_eq!(tag_of(&Type::VARCHAR), tag::TEXT);
        assert_eq!(tag_of(&Type::BPCHAR), tag::TEXT);
        assert_eq!(tag_of(&Type::BYTEA), tag::BYTES);
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
            assert_eq!(tag_of(&ty), want, "{ty:?} tag");
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

    /// The NON-CANONICAL guard (hazards 15/20), REPOINTED at M1-S8c. Was
    /// `timetz_and_deferred_oids_stay_unsupported`, which asserted the tag was a loud
    /// `PoolError::Unsupported`; D-S8b-6 replaced that refusal with the TEXT FALLBACK, so the
    /// assertion moves rather than disappears — the property that still matters, and the one a
    /// silent miscast would violate, is that these OIDs must never acquire a BINARY extraction arm.
    ///
    /// `TIMETZ` in particular must never fall into the `TIME` arm: its BINARY payload is 12 bytes
    /// (i64 µs + i32 zone) against `TIME`'s 8, so it would render a wrong time-of-day out of the
    /// first 8 bytes — a silent miscast, not an error. Under the fallback it is PG's own
    /// `12:34:56+02` text, which is exactly right.
    ///
    /// It is a fallback lock, NOT the hazard-16 "raw getter is unreachable" guard (that one is
    /// `the_only_raw_from_sql_is_inside_the_oid_gate`).
    #[test]
    fn timetz_and_non_canonical_oids_take_the_text_fallback() {
        for ty in [Type::TIMETZ, Type::INT4_ARRAY, Type::INTERVAL, Type::INET] {
            assert!(
                oid_extract_type(ty.oid()).is_none(),
                "{ty:?} must have NO binary extraction arm"
            );
            assert_eq!(
                tag_of(&ty),
                tag::TEXT,
                "{ty:?} must reach the client as TAG_TEXT (the M1-S8c fallback)"
            );
            assert!(
                !wants_binary_result(ty.oid()),
                "{ty:?} must be requested in the TEXT result format, or its bytes would be binary"
            );
        }
    }

    /// **D-S8b-6, the tag half.** Every OID the S8b acceptance run measured as blocking the stock
    /// `PostgreSQLSchemaManager` — plus the extension shapes the decision names — reaches the client
    /// as `TAG_TEXT` in the TEXT result format, and NONE of them acquires a binary arm.
    ///
    /// `9_999_999` stands in for an EXTENSION-ASSIGNED oid (PostGIS `geometry`, `hstore`, `ltree`,
    /// `citext`, a native enum): those are database-local numbers, so the only faithful unit-level
    /// statement about them is "an oid nothing in the canonical table claims". The live
    /// `pg_text_fallback_it.rs` exercises real ones.
    #[test]
    fn s8c_catalog_and_extension_oids_take_the_text_fallback() {
        // (oid, what it is) — the list D-S8b-6 enumerates, by raw oid so the ones postgres-types
        // has no constant for are still covered.
        for (oid, what) in [
            (
                22u32,
                "int2vector (pg_index.indkey — 50 of PG's 78 S8b failures)",
            ),
            (30, "oidvector (pg_proc.proargtypes)"),
            (1005, "_int2"),
            (1009, "_text"),
            (1028, "_oid"),
            (1034, "_aclitem (pg_class.relacl)"),
            (194, "pg_node_tree (pg_attrdef.adbin)"),
            (2277, "anyarray"),
            (24, "regproc"),
            (28, "xid"),
            (9_999_999, "an extension-assigned custom oid"),
        ] {
            assert!(
                oid_extract_type(oid).is_none(),
                "oid {oid} ({what}) must have no binary extraction arm"
            );
            assert_eq!(
                oid_to_tag(oid),
                tag::TEXT,
                "oid {oid} ({what}) must reach the client as TAG_TEXT"
            );
            assert!(
                !wants_binary_result(oid),
                "oid {oid} ({what}) must be requested in the TEXT result format"
            );
        }
    }

    /// **The fallback must not swallow the typed path.** Every canonical OID keeps its own tag AND
    /// stays BINARY on the wire — i.e. the S8c change is additive, not a wholesale rerouting.
    ///
    /// This is the unit-level half of the mutation the task names: routing, say, `TIMESTAMPTZ`
    /// through the fallback (drop its arm from `oid_extract_type`) turns its tag into `TAG_TEXT`
    /// and its format into text, and BOTH assertions here go red. The live half — that the VALUE
    /// then changes from the canonical `…T…Z` form to PG's `… +00` form — is in
    /// `pg_text_fallback_it.rs`.
    #[test]
    fn every_canonical_oid_stays_binary_and_keeps_its_own_tag() {
        for (ty, want) in [
            (Type::BOOL, tag::BOOL),
            (Type::INT2, tag::I64),
            (Type::INT4, tag::I64),
            (Type::INT8, tag::I64),
            (Type::FLOAT4, tag::F64),
            (Type::FLOAT8, tag::F64),
            (Type::TEXT, tag::TEXT),
            (Type::VARCHAR, tag::TEXT),
            (Type::BPCHAR, tag::TEXT),
            (Type::NAME, tag::TEXT),
            (Type::CHAR, tag::TEXT),
            (Type::BYTEA, tag::BYTES),
            (Type::NUMERIC, tag::DECIMAL),
            (Type::DATE, tag::DATE),
            (Type::TIME, tag::TIME),
            (Type::TIMESTAMP, tag::TIMESTAMP),
            (Type::TIMESTAMPTZ, tag::TIMESTAMPTZ),
            (Type::UUID, tag::UUID),
            (Type::JSON, tag::JSON),
            (Type::JSONB, tag::JSON),
            (Type::OID, tag::I64),
            (Type::REGTYPE, tag::I64),
            (Type::REGCLASS, tag::I64),
        ] {
            assert!(
                wants_binary_result(ty.oid()),
                "{ty:?} is canonical and MUST stay in the BINARY result format — routing it \
                 through the text fallback would silently change its wire value"
            );
            assert_eq!(tag_of(&ty), want, "{ty:?} keeps its own canonical tag");
        }
    }

    /// The `Bind` result-format policy and the extraction table are ONE authority (M1-S8c). If they
    /// ever diverged, a column would be requested BINARY and decoded as text (silent garbage) or
    /// requested TEXT and decoded by a binary `FromSql` (a mid-stream decode error after HEAD is
    /// already on the wire).
    ///
    /// Swept across a wide oid range rather than a curated list, so a policy that grew its own
    /// hand-maintained table — the exact refactor this locks against — cannot pass by matching the
    /// list. It is deliberately a WEAK guard on its own (the production predicate is one line); the
    /// load-bearing proof is the live `pg_text_fallback_it.rs`.
    #[test]
    fn the_bind_format_policy_is_the_extraction_table() {
        for oid in 0u32..3000 {
            assert_eq!(
                wants_binary_result(oid),
                oid_extract_type(oid).is_some(),
                "oid {oid}: the Bind result-format policy disagrees with the extraction table"
            );
        }
    }

    /// M1-S8a: the catalog scalars DBAL's `AbstractSchemaManager` selects on every introspection.
    /// Both gates must admit them — `oid_to_tag` (cols-build) and `oid_extract_type` (per-cell) are
    /// matches over ONE table precisely so they cannot drift (hazard 7).
    #[test]
    fn s8a_catalog_scalars_are_admitted_by_both_gates() {
        for (ty, want) in [
            (Type::NAME, tag::TEXT),
            (Type::CHAR, tag::TEXT),
            (Type::OID, tag::I64),
            (Type::REGTYPE, tag::I64),
            (Type::REGCLASS, tag::I64),
        ] {
            assert!(
                oid_extract_type(ty.oid()).is_some(),
                "{ty:?} must have an extraction type"
            );
            assert_eq!(
                tag_of(&ty),
                want,
                "{ty:?} must map to the canonical tag {want}"
            );
        }
    }

    /// The non-canonical neighbours must not acquire a BINARY arm — admitting the S8a catalog
    /// family must not quietly widen anything else.
    ///
    /// REPOINTED at M1-S8c: it used to assert `oid_to_tag(..).is_err()`, which D-S8b-6 abolished.
    /// The tag is now `TAG_TEXT` for all of these, so asserting on the tag would no longer
    /// distinguish them from `Type::TEXT` itself — `oid_extract_type` is what still does.
    #[test]
    fn s8a_catalog_admission_does_not_widen_the_binary_set() {
        for ty in [
            Type::TIMETZ,
            Type::INTERVAL,
            Type::INET,
            Type::INT4_ARRAY,
            Type::OID_VECTOR,
        ] {
            assert!(
                oid_extract_type(ty.oid()).is_none(),
                "{ty:?} must stay OUT of the binary/canonical set"
            );
        }
    }

    /// PG's `"char"` is one BYTE, and `'\0'` — what `attidentity` holds on a non-identity column —
    /// renders as the EMPTY string, exactly as PG's own text output does. A non-ASCII byte has no
    /// canonical text form and is a loud decode mismatch, never a lossy replacement character.
    #[test]
    fn s8a_char_byte_rendering_matches_pg_text_output() {
        assert_eq!(crate::pgtext::char_byte_to_text(0).unwrap(), "");
        assert_eq!(crate::pgtext::char_byte_to_text(b'a').unwrap(), "a");
        assert_eq!(crate::pgtext::char_byte_to_text(b'd').unwrap(), "d");
        let e =
            crate::pgtext::char_byte_to_text(0xff).expect_err("non-ASCII has no canonical form");
        assert!(
            matches!(e, PoolError::Backend(_)),
            "a decode mismatch is Backend, never ConnectionLost"
        );
    }

    /// **Hazard 16, mechanically enforced — on the CAPABILITY, not on a name.**
    ///
    /// A `FromSql` whose `accepts(_) -> true` DEFEATS tokio-postgres' own type check: it hands
    /// back the bytes of *any* column, so a read that reached it without first passing
    /// [`oid_extract_type`] would decode an unsupported type as garbage instead of raising the
    /// loud `Unsupported` this module exists to guarantee. `oid_extract_type` must remain the SOLE
    /// type authority.
    ///
    /// **Why this guard was rewritten.** It used to assert that the IDENTIFIER `RawBytes` appeared
    /// only in `pgtext.rs` and inside `extract_value`'s line span — which said nothing about what
    /// `pgtext.rs` could EXPORT. Surviving mutation: add `pub(crate) fn raw_slice(row, idx) ->
    /// Option<Vec<u8>>` to `pgtext.rs` (its body does the `try_get::<Option<RawBytes>>`) and call
    /// it ungated from `query.rs` — guard GREEN, 57 lib tests green, clippy clean, OID gate gone.
    /// One indirection defeated a name check, exactly as a `pub(crate) fn` escape had once before.
    ///
    /// The fix is structural first and a test second: the raw `FromSql` now lives INSIDE
    /// `extract_value` as a function-local item, so it is unnameable anywhere else and that
    /// `raw_slice` no longer COMPILES. This test locks the property the compiler cannot state —
    /// that nobody declares a SECOND one somewhere else under a different name:
    ///
    /// 1. every `impl … FromSql …` in the crate's production source must sit inside
    ///    `extract_value`'s line span (headers are read to their opening `{`, so a rustfmt-split
    ///    or `where`-claused header cannot slip past a line-shaped pattern); and
    /// 2. the identifier `RawBytes` must likewise appear only there — belt to the compiler's
    ///    braces, and the assertion that keeps naming the hazard where a reader will meet it.
    ///
    /// **The span check is deliberately not a `fn`-chunk split** (T4b review F3). Splitting the
    /// source on `"\nfn "`/`"\npub fn "` made every other qualifier a non-boundary, so a
    /// `pub(crate) fn` / `async fn` / `const fn` / `pub(super) fn` inserted right after
    /// `extract_value` rode INSIDE its chunk and escaped the guard — demonstrated live with a
    /// `pub(crate) fn evil_raw_read` that read raw bytes ungated and left the test GREEN. Line
    /// containment has no qualifier vocabulary to keep up with: `extract_value` runs from its
    /// `pub fn` line to its column-0 `}`, whatever surrounds it.
    #[test]
    fn the_only_raw_from_sql_is_inside_the_oid_gate() {
        let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        collect_rs(&src_dir, &mut files);

        // The span is computed ONCE, from rowmap.rs, and every file is judged against it (only
        // rowmap.rs can be inside it). A file count would be a hardcoded number that says nothing;
        // the `visited_*` flags below prove the walk actually reached the code that matters.
        let rowmap = files
            .iter()
            .find(|f| f.file_name().unwrap() == "rowmap.rs")
            .expect("the source walk must reach rowmap.rs");
        let rowmap_code = production_code(rowmap);
        let (gate_start, gate_end) = extract_value_span(&rowmap_code);

        let mut visited_pgtext = false;
        let mut gated_from_sql_impls = 0usize;
        let mut gated_raw_uses = 0usize;
        for f in &files {
            let name = f.file_name().unwrap().to_string_lossy().to_string();
            let code = production_code(f);
            let is_rowmap = name == "rowmap.rs";
            if name == "pgtext.rs" {
                visited_pgtext = true;
            }

            // (1) THE CAPABILITY: a custom `FromSql` is the only way to get unchecked raw bytes
            // out of a `Row` — tokio-postgres' own impls all have a real `accepts`. Renaming the
            // type defeats a name check; it cannot defeat this one.
            for (line, header) in from_sql_impls(&code) {
                assert!(
                    is_rowmap && (gate_start..=gate_end).contains(&line),
                    "{name}:{} declares a FromSql OUTSIDE `extract_value` (rowmap.rs lines \
                     {}..={}). A FromSql with a permissive `accepts` bypasses the OID gate \
                     (hazard 16); a gated one belongs inside the gate:\n{}",
                    line + 1,
                    gate_start + 1,
                    gate_end + 1,
                    header.trim()
                );
                gated_from_sql_impls += 1;
            }

            // (2) THE NAME, still: a call site added elsewhere in rowmap.rs would not compile, but
            // this keeps the failure legible if the containment is ever loosened again.
            for (i, l) in code.lines().enumerate() {
                if !l.contains("RawBytes") {
                    continue;
                }
                assert!(
                    is_rowmap && (gate_start..=gate_end).contains(&i),
                    "{name}:{} names RawBytes OUTSIDE `extract_value` (rowmap.rs lines {}..={}), \
                     which would bypass the OID gate (hazard 16):\n{}",
                    i + 1,
                    gate_start + 1,
                    gate_end + 1,
                    l.trim()
                );
                gated_raw_uses += 1;
            }
        }

        assert!(
            visited_pgtext,
            "pgtext.rs was not visited — the source walk is broken, not the invariant"
        );
        assert_eq!(
            gated_from_sql_impls, 1,
            "expected EXACTLY the one raw FromSql inside `extract_value`; found \
             {gated_from_sql_impls}. Zero means the M1-S7 arms regressed to a typed FromSql (or \
             this guard stopped looking where the code is); more than one means a second raw \
             reader was added and each needs its own review."
        );
        assert!(
            gated_raw_uses >= 2,
            "RawBytes must be declared AND read inside the gate; found {gated_raw_uses} mentions"
        );
    }

    /// A file's production source: whole-line comments blanked and the `#[cfg(test)]` tail cut.
    fn production_code(f: &std::path::Path) -> String {
        let text = std::fs::read_to_string(f).expect("read source");
        strip_comments(text.split("\n#[cfg(test)]").next().unwrap_or(&text))
    }

    /// `extract_value`'s LINE SPAN: its top-level `pub fn` line through the first column-0 `}`.
    fn extract_value_span(code: &str) -> (usize, usize) {
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
        (start, end)
    }

    /// Every `impl … FromSql …` block in `code`, as (line index of `impl`, full header text).
    ///
    /// The header is read forward to the `{` that opens the block, so a header rustfmt split
    /// across lines — or one carrying a `where` clause — is still matched. A line-shaped
    /// `impl.*FromSql` regex would miss exactly those, which is the loosening a safety guard must
    /// never take. Generic BOUNDS (`T: FromSql<'a>`) are not impls and are correctly ignored:
    /// they read through whatever impls already exist and mint no new one.
    fn from_sql_impls(code: &str) -> Vec<(usize, String)> {
        let lines: Vec<&str> = code.lines().collect();
        let mut out = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            let t = l.trim_start();
            if !t.starts_with("impl")
                || t[4..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
            {
                continue;
            }
            let mut header = String::new();
            for l2 in &lines[i..] {
                header.push_str(l2.trim());
                header.push(' ');
                if l2.contains('{') {
                    break;
                }
            }
            if header.contains("FromSql") {
                out.push((i, header));
            }
        }
        out
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
