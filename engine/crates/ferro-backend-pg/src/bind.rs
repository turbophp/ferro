//! Canonical [`Value`] params → tokio-postgres `ToSql`, for every M0 scalar incl. `Null` and
//! `Bytes`, plus the eight M1-S7 canonical tags.
//!
//! Each param becomes a `Box<dyn ToSql + Sync + Send>` (which `BorrowToSql` accepts), so the boxed Vec
//! can be handed straight to `Client::query_raw` as an `ExactSizeIterator`. `Value::Null` is the
//! subtle one: a NULL has no canonical Rust type, so it is bound via [`PgNull`], a `ToSql` that
//! `accepts` EVERY type and always writes `IsNull::Yes`. That sidesteps the usual "which
//! `Option::<T>::None`?" problem — with a prepared statement PG has already fixed each param's
//! type, and `PgNull` writes a typed NULL slot for whatever that type is.
//!
//! ## M1-S7: the canonical tags bind as TEXT-format params
//!
//! The eight tags added in M1-S7 carry **canonical text** (`proto/PROTOCOL.md` §3.2), and each one
//! binds through its **own** newtype ([`PgDecimalText`] … [`PgJsonText`]) that writes that text
//! verbatim in PG's **text** wire format. Two properties make this the right shape:
//!
//! - **Text format is per-param selectable** (hazard 17). The vendored fork builds a per-param
//!   format array from `ToSql::encode_format` (`vendor/tokio-postgres/src/query.rs:305-308`) even
//!   though the RESULT format is hardcoded binary (`:324`). Sending text lets PG's own input parser
//!   do the work — no hand-written base-10000 NUMERIC encoder, no 2000-epoch date arithmetic — and
//!   it is exactly why a display scale (`1.10` ≠ `1.1`), a 131 072-digit numeric, `NaN` and the
//!   ±`infinity` sentinels all survive a bind untouched: nothing re-renders them.
//! - **One newtype PER TAG, each with a NARROW `accepts`** (hazard 19 / F17). See
//!   [`pg_canonical_text_param`] for why a single shared newtype would silently disable the §19.3
//!   pre-flight for all eight tags at once.
//!
//! There is **no `unreachable!()` in this module**: every `Value` variant has a real box, so a
//! caller that skipped the `accepts` pre-flight would get a typed `WrongType` error from
//! `to_sql_checked`, never a daemon panic.

use ferro_proto::value::Value;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};

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

/// Declares one **canonical-text** `ToSql` newtype (`proto/PROTOCOL.md` §3.2): a `String` wrapper
/// that writes its canonical text VERBATIM in PG's **text** wire format and `accepts` ONLY the PG
/// types named at the call site.
///
/// **Why one newtype per tag (hazard 19 / F17).** [`accepts`] is the §19.3 known-fate pre-flight —
/// `query.rs` runs it BEFORE the statement is sent. The rule is DIRECTIONAL: it may be STRICTER
/// than the concrete `ToSql` it fronts (a clean, diagnosable pre-send rejection), but it must NEVER
/// be looser, because a looser `accepts` lets `to_sql_checked` fail POST-send — precisely the
/// false-`Indeterminate` path the pre-flight exists to prevent. One SHARED newtype would have to
/// accept the union of every target type the eight tags touch (`numeric ∪ date ∪ time ∪ timestamp ∪
/// timestamptz ∪ uuid ∪ json ∪ jsonb`), disabling the pre-flight for all eight at once: a
/// `Value::Decimal` would sail into a `date` column and fail on the wire. And never copy
/// [`PgNull`]'s `accepts(_ty) -> true` — that is legitimate ONLY for a typed NULL slot, which
/// writes no bytes at all.
macro_rules! pg_canonical_text_param {
    ($(#[$meta:meta])* $name:ident accepts [$($ty:ident),+ $(,)?]) => {
        $(#[$meta])*
        #[derive(Debug)]
        struct $name(String);

        impl ToSql for $name {
            /// The canonical text, byte-for-byte. It is NOT re-rendered, re-parsed or validated
            /// here: the reader already produced the exact form PG's input parser accepts, and any
            /// round trip through a numeric/date type would lose the display scale or a sentinel.
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                out.extend_from_slice(self.0.as_bytes());
                Ok(IsNull::No)
            }

            /// NARROW by construction — only the types listed at the declaration site.
            fn accepts(ty: &Type) -> bool {
                [$(Type::$ty),+].contains(ty)
            }

            /// Text format for THIS param only; the result format stays binary (hazard 17).
            fn encode_format(&self, _ty: &Type) -> Format {
                Format::Text
            }

            to_sql_checked!();
        }
    };
}

pg_canonical_text_param! {
    /// `DECIMAL` → `numeric` only. PG parses the canonical text itself, so full precision, the
    /// display scale (`1.10` and `1.1` stay distinct) and the `NaN` / `Infinity` / `-Infinity`
    /// payloads all survive — none of which a binary encoder through a fixed-width decimal type
    /// could preserve (hazard 10).
    PgDecimalText accepts [NUMERIC]
}

pg_canonical_text_param! {
    /// `DATE` → `date` only. Never `timestamp`: promoting a date to a timestamp is a guess, and the
    /// `infinity` / `-infinity` sentinels bind as the literals PG itself accepts.
    PgDateText accepts [DATE]
}

pg_canonical_text_param! {
    /// `TIME` → `time` only. Deliberately NOT `timetz` (hazard 15): `timetz` has a 12-byte payload
    /// and no `FromSql`, so it is `Unsupported` on the read side — admitting it here would create a
    /// column Ferro can write but not read back.
    PgTimeText accepts [TIME]
}

pg_canonical_text_param! {
    /// `TIMESTAMP` (NAIVE) → `timestamp` only. Never `timestamptz`: the canonical payload carries no
    /// zone, so binding it to `timestamptz` would make PG apply the session `TimeZone` — a silent
    /// shift. A naive value that genuinely means an instant must arrive as `TIMESTAMPTZ`.
    PgTimestampText accepts [TIMESTAMP]
}

pg_canonical_text_param! {
    /// `TIMESTAMPTZ` (a UTC INSTANT) → `timestamptz` only. The canonical text ends in a literal `Z`,
    /// which PG's parser reads as UTC regardless of the session `TimeZone`. Never `timestamp`:
    /// that would silently drop the zone and store the UTC wall clock as a local one.
    PgTimestampTzText accepts [TIMESTAMPTZ]
}

pg_canonical_text_param! {
    /// `UUID` → `uuid` only. Never `text`: the canonical 36-char lowercase form would then bind to
    /// any string column, defeating the pre-flight for the whole tag.
    PgUuidText accepts [UUID]
}

pg_canonical_text_param! {
    /// `JSON` → `json` AND `jsonb` (the one tag with two legitimate targets — the canonical payload
    /// is the raw document text, which is the text input form of both). Never `text`.
    PgJsonText accepts [JSON, JSONB]
}

/// `U64` has **no** PG target type in S7 — PostgreSQL has no unsigned integer type, so there is
/// nothing a `U64` param could bind to without a widening guess (`int8` cannot hold the top half of
/// the range; `numeric` would silently change the column's type semantics). Its `accepts` is
/// therefore `false` for EVERY type: a legitimate, diagnosable known-fate rejection, not an
/// oversight.
///
/// It still exists as a real newtype rather than an `unreachable!()` arm so that `value_to_boxed`
/// stays TOTAL: a caller that somehow skipped the `accepts` pre-flight gets a typed `WrongType`
/// error out of `to_sql_checked`, never a panic reachable from a user-supplied param.
#[derive(Debug)]
struct PgU64Text(String);

impl ToSql for PgU64Text {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    /// Accepts NOTHING — see the type docs. This is the strict end of the directional rule
    /// (hazard 19): stricter than any impl can be, so it can never be looser.
    fn accepts(_ty: &Type) -> bool {
        false
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
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
        // ---- M1-S7 (Task 8b): one text-format newtype PER TAG, each with a NARROW `accepts`
        // mirrored arm-for-arm in `accepts` below — the two MUST move together (a widened
        // `accepts` over a missing box, or vice versa, is the defect this pairing prevents). No
        // `unreachable!()`: every variant boxes, so no user param can reach a panic here.
        Value::U64(n) => Box::new(PgU64Text(n.to_string())),
        Value::Decimal(s) => Box::new(PgDecimalText(s.clone())),
        Value::Date(s) => Box::new(PgDateText(s.clone())),
        Value::Time(s) => Box::new(PgTimeText(s.clone())),
        Value::Timestamp(s) => Box::new(PgTimestampText(s.clone())),
        Value::TimestampTz(s) => Box::new(PgTimestampTzText(s.clone())),
        Value::Uuid(s) => Box::new(PgUuidText(s.clone())),
        Value::Json(s) => Box::new(PgJsonText(s.clone())),
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
        // ---- M1-S7 (Task 8b): each tag delegates to the SAME newtype `value_to_boxed` boxes it
        // as, so the pre-flight is by construction the exact predicate `query_raw`'s own
        // `to_sql_checked` will apply. Every one of these is NARROW (never `PgNull`'s universally
        // true `accepts`), and `U64` is narrow to the point of empty — PG has no unsigned type.
        Value::U64(_) => <PgU64Text as ToSql>::accepts(ty),
        Value::Decimal(_) => <PgDecimalText as ToSql>::accepts(ty),
        Value::Date(_) => <PgDateText as ToSql>::accepts(ty),
        Value::Time(_) => <PgTimeText as ToSql>::accepts(ty),
        Value::Timestamp(_) => <PgTimestampText as ToSql>::accepts(ty),
        Value::TimestampTz(_) => <PgTimestampTzText as ToSql>::accepts(ty),
        Value::Uuid(_) => <PgUuidText as ToSql>::accepts(ty),
        Value::Json(_) => <PgJsonText as ToSql>::accepts(ty),
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
        Value::U64(_) => "U64",
        Value::Decimal(_) => "DECIMAL",
        Value::Date(_) => "DATE",
        Value::Time(_) => "TIME",
        Value::Timestamp(_) => "TIMESTAMP",
        Value::TimestampTz(_) => "TIMESTAMPTZ",
        Value::Uuid(_) => "UUID",
        Value::Json(_) => "JSON",
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
        // M1-S7 canonical tags: a label per tag so a bind rejection names the real canonical type.
        assert_eq!(value_kind(&Value::U64(1)), "U64");
        assert_eq!(value_kind(&Value::Decimal("1".into())), "DECIMAL");
        assert_eq!(value_kind(&Value::Date("2026-01-01".into())), "DATE");
        assert_eq!(value_kind(&Value::Time("00:00:00".into())), "TIME");
        assert_eq!(
            value_kind(&Value::Timestamp("2026-01-01 00:00:00".into())),
            "TIMESTAMP"
        );
        assert_eq!(
            value_kind(&Value::TimestampTz("2026-01-01T00:00:00Z".into())),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            value_kind(&Value::Uuid("00000000-0000-0000-0000-000000000000".into())),
            "UUID"
        );
        assert_eq!(value_kind(&Value::Json("null".into())), "JSON");
    }

    /// One instance of every canonical `Value` variant — the totality fixture.
    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("x".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
            Value::U64(u64::MAX),
            Value::Decimal("-12345.6700".to_string()),
            Value::Date("2026-08-05".to_string()),
            Value::Time("24:00:00".to_string()),
            Value::Timestamp("2026-08-05 11:45:07.250000".to_string()),
            Value::TimestampTz("2026-08-05T11:45:07.250000Z".to_string()),
            Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".to_string()),
            Value::Json(r#"{"a":[1,2]}"#.to_string()),
        ]
    }

    /// Every PG `Type` any of the arms above could plausibly be aimed at, plus a few that must
    /// never be accepted. Used for the cross-product directional proof below.
    fn every_target_type() -> Vec<Type> {
        vec![
            Type::BOOL,
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::TEXT,
            Type::VARCHAR,
            Type::BPCHAR,
            Type::BYTEA,
            Type::NUMERIC,
            Type::DATE,
            Type::TIME,
            Type::TIMETZ,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
            Type::INTERVAL,
            Type::INET,
            Type::INT4_ARRAY,
        ]
    }

    /// Hazard 19 (DIRECTIONAL): `accepts` may be STRICTER than the boxed impl, never LOOSER. Each
    /// new tag gets its OWN narrow newtype — a shared one would accept every target type the eight
    /// tags collectively touch and silently disable the §19.3 pre-flight for all of them.
    #[test]
    fn s7_accepts_is_narrow_per_tag() {
        let cases: &[(Value, Type, bool)] = &[
            (Value::Decimal("1.10".into()), Type::NUMERIC, true),
            (Value::Decimal("1.10".into()), Type::DATE, false),
            (Value::Decimal("1.10".into()), Type::INT4, false),
            (Value::Date("2026-08-05".into()), Type::DATE, true),
            (Value::Date("2026-08-05".into()), Type::TIMESTAMP, false),
            (Value::Time("24:00:00".into()), Type::TIME, true),
            // Hazard 15 stays closed: `timetz` has a 12-byte payload and no `FromSql`; it is
            // Unsupported on the read side, so it must not be bindable either.
            (Value::Time("24:00:00".into()), Type::TIMETZ, false),
            (
                Value::Timestamp("2026-08-05 00:00:00".into()),
                Type::TIMESTAMP,
                true,
            ),
            // A naive value never guesses a zone.
            (
                Value::Timestamp("2026-08-05 00:00:00".into()),
                Type::TIMESTAMPTZ,
                false,
            ),
            (
                Value::TimestampTz("2026-08-05T00:00:00Z".into()),
                Type::TIMESTAMPTZ,
                true,
            ),
            (
                Value::TimestampTz("2026-08-05T00:00:00Z".into()),
                Type::TIMESTAMP,
                false,
            ),
            (
                Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
                Type::UUID,
                true,
            ),
            (
                Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
                Type::TEXT,
                false,
            ),
            (Value::Json("{}".into()), Type::JSON, true),
            (Value::Json("{}".into()), Type::JSONB, true),
            (Value::Json("{}".into()), Type::TEXT, false),
            // U64 has no PG target type in S7 — PG has no unsigned integer type, so it stays a
            // known-fate rejection everywhere (never a silent widening to int8/numeric).
            (Value::U64(1), Type::INT8, false),
            (Value::U64(1), Type::NUMERIC, false),
        ];
        for (v, ty, want) in cases {
            assert_eq!(accepts(v, ty), *want, "accepts({v:?}, {ty:?})");
        }
    }

    /// `U64` is refused against EVERY type, not just the two spot-checked above — PG has no
    /// unsigned integer type in scope for S7, so there is no target it could bind to.
    #[test]
    fn s7_u64_is_a_known_fate_rejection_against_every_type() {
        for ty in every_target_type() {
            assert!(
                !accepts(&Value::U64(1), &ty),
                "U64 must stay a known-fate rejection against {ty:?}"
            );
        }
    }

    /// The newtypes send **TEXT** format. Param format IS per-param selectable (hazard 17 — the
    /// vendored fork builds a per-param format array at `query.rs:305-308`), even though the RESULT
    /// format is hardcoded binary at `:324`. That asymmetry is what lets PG's own input parser
    /// consume the canonical text, so no base-10000 NUMERIC encoder and no 2000-epoch date
    /// arithmetic has to be hand-written on the write side.
    #[test]
    fn s7_newtypes_send_text_format() {
        assert!(matches!(
            PgDecimalText("1.10".into()).encode_format(&Type::NUMERIC),
            Format::Text
        ));
        assert!(matches!(
            PgDateText("2026-08-05".into()).encode_format(&Type::DATE),
            Format::Text
        ));
        assert!(matches!(
            PgTimeText("24:00:00".into()).encode_format(&Type::TIME),
            Format::Text
        ));
        assert!(matches!(
            PgTimestampText("2026-08-05 00:00:00".into()).encode_format(&Type::TIMESTAMP),
            Format::Text
        ));
        assert!(matches!(
            PgTimestampTzText("2026-08-05T00:00:00Z".into()).encode_format(&Type::TIMESTAMPTZ),
            Format::Text
        ));
        assert!(matches!(
            PgUuidText("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()).encode_format(&Type::UUID),
            Format::Text
        ));
        assert!(matches!(
            PgJsonText("{}".into()).encode_format(&Type::JSONB),
            Format::Text
        ));
    }

    /// The canonical text is written VERBATIM — no re-rendering, so the display scale (`1.10` ≠
    /// `1.1`) and the `NaN`/`infinity` sentinels reach PG's parser exactly as the reader produced
    /// them.
    #[test]
    fn s7_newtypes_write_the_canonical_text_verbatim() {
        for (text, ty) in [
            ("1.10", Type::NUMERIC),
            ("NaN", Type::NUMERIC),
            ("-Infinity", Type::NUMERIC),
        ] {
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            let is_null = PgDecimalText(text.into()).to_sql(&ty, &mut buf).unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(
                &buf[..],
                text.as_bytes(),
                "canonical text must go out as-is"
            );
        }
    }

    /// **The lockstep proof (carry C2/C3/C12).** Over the FULL cross product of every canonical
    /// variant × every plausible target type: whenever `accepts` says yes, the concrete boxed impl
    /// must actually bind. That is the directional rule mechanically — `accepts` can be stricter
    /// (a clean pre-send rejection), never looser (a POST-send `to_sql_checked` failure, which is
    /// the false-`Indeterminate` path §19.3 forbids). It also proves `accepts` and `value_to_boxed`
    /// were flipped together: widening one without the other fails here.
    #[test]
    fn s7_accepts_is_never_looser_than_the_boxed_impl() {
        for v in every_variant() {
            let boxed = value_to_boxed(&v);
            for ty in every_target_type() {
                if !accepts(&v, &ty) {
                    continue;
                }
                let mut buf = tokio_postgres::types::private::BytesMut::new();
                assert!(
                    boxed.to_sql_checked(&ty, &mut buf).is_ok(),
                    "accepts({v:?}, {ty:?}) said yes but the boxed impl refuses it — a LOOSER \
                     accepts lets to_sql_checked fail POST-send (false Indeterminate, §19.3)"
                );
            }
        }
    }

    /// **No panic is reachable from a user param (carry C2/C3/C12).** `value_to_boxed` used to be
    /// an `unreachable!()` for the eight canonical tags, sound only while `accepts` gated every
    /// path. It now has a real box per variant, so even a caller that skipped the pre-flight
    /// entirely gets a typed `WrongType` error rather than a daemon panic. Exercised against the
    /// full cross product, including the pairs `accepts` rejects.
    #[test]
    fn s7_value_to_boxed_is_total_and_never_panics() {
        for v in every_variant() {
            let boxed = value_to_boxed(&v);
            for ty in every_target_type() {
                let mut buf = tokio_postgres::types::private::BytesMut::new();
                // The only contract here is "does not panic"; a rejected pair returns Err.
                let _ = boxed.to_sql_checked(&ty, &mut buf);
            }
        }
        assert_eq!(to_boxed_params(&every_variant()).len(), 14);
    }

    /// **Sentinel discipline, preserved from Task 8a.** A `TIMESTAMP`/`TIMESTAMPTZ` sentinel
    /// (`infinity`, a MySQL zero datetime) reaches the engine as `TAG_TEXT` byte-verbatim — a bare
    /// PHP string's contents are never sniffed for a temporal tag. So it lands on `Value::Text`,
    /// whose `accepts` is `String`'s, which refuses every temporal type: a §19.3 known-fate
    /// rejection instead of a silent miscast. Widening `Value::Text`'s accepts would break that.
    #[test]
    fn s7_a_bare_text_never_binds_to_a_temporal_or_numeric_column() {
        for s in ["infinity", "-infinity", "0000-00-00 00:00:00", "2026-08-05"] {
            for ty in [
                Type::DATE,
                Type::TIME,
                Type::TIMESTAMP,
                Type::TIMESTAMPTZ,
                Type::NUMERIC,
                Type::UUID,
                Type::JSONB,
            ] {
                assert!(
                    !accepts(&Value::Text(s.to_string()), &ty),
                    "a bare TEXT param must not bind to {ty:?} — the sentinel would be miscast"
                );
            }
        }
        // ...while a DATE sentinel that arrived tag-intact binds to a `date` column, which is
        // exactly what PG's own parser accepts (`'infinity'::date`).
        assert!(accepts(&Value::Date("infinity".into()), &Type::DATE));
    }
}
