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
//!
//! ## M1-S7: the temporal tags bind as TYPED params, never as text (hazard 41)
//!
//! `DATE` / `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` carry canonical TEXT on the wire
//! (`proto/PROTOCOL.md` §3.2) but must NOT reach the server as a string literal:
//! `'2026-08-05T11:45:07.250000Z'` fails on MySQL 8 with `1292 Incorrect datetime value` under the
//! default `STRICT_TRANS_TABLES`, and MariaDB 11 rejects offsets in datetime literals outright. The
//! canonical text is therefore parsed here into the driver's component form
//! ([`MyValue::Date`] / [`MyValue::Time`]) — a typed `MYSQL_TYPE_DATETIME` / `MYSQL_TYPE_TIME`
//! param with no server-side literal parsing at all.
//!
//! **`TIMESTAMPTZ` is correct ONLY under the UTC session pin.** A `TIMESTAMPTZ` payload is a UTC
//! instant, and it is bound as bare wall-clock components — which the server interprets in the
//! SESSION zone. `conn::connect` pins every Ferro MySQL/MariaDB session to `time_zone = '+00:00'`
//! (M1-S7 Task 5a, re-applied after every `COM_RESET_CONNECTION`), so those components ARE the
//! correct session-local ones. The two are **coupled**: remove the pin and every `TIMESTAMPTZ`
//! write silently shifts by the session offset — the exact mirror of the read-side coupling
//! documented in [`crate::mytext::timestamptz_to_text`].
//!
//! The parse helpers stay **infallible**: canonical text the engine itself produced always parses,
//! and anything else falls back to a byte-string param (so the server produces its own clean error)
//! rather than introducing a `Result` cascade this module has no pre-flight to report through.

use ferro_pool::error::PoolError;
use ferro_proto::consts::errc;
use mysql_async::{Params, Value as MyValue};

use crate::Value;
use crate::mytext::{MAX_TIME_US, ZERO_DATETIME};

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
        // ---- M1-S7 canonical tags. The mapping stays TOTAL (the module invariant above): every
        // variant has a driver representation, so no arm can fail and mint a fate-unknown error.
        Value::U64(n) => MyValue::UInt(*n),
        // DECIMAL/UUID/JSON: the canonical text IS what the server wants as a string param.
        Value::Decimal(s) | Value::Uuid(s) | Value::Json(s) => {
            MyValue::Bytes(s.clone().into_bytes())
        }
        // DATE / TIME / TIMESTAMP / TIMESTAMPTZ: TYPED params built from the canonical text (see
        // the module docs, hazard 41). Never a `Z`-suffixed literal — both servers reject it.
        Value::Date(s) => parse_date(s),
        Value::Time(s) => parse_time(s),
        Value::Timestamp(s) => parse_datetime(s),
        // Correct ONLY under the `time_zone = '+00:00'` session pin — the components are bound
        // bare and the server reads them in the session zone. The two are COUPLED (module docs).
        Value::TimestampTz(s) => parse_rfc3339_utc(s),
    }
}

/// Canonical `DATE` text (`"YYYY-MM-DD"`, incl. the `"0000-00-00"` zero sentinel) → a typed
/// date-only param. See [`fallback`] for the non-canonical branch.
fn parse_date(s: &str) -> MyValue {
    match split_date(s) {
        Some((y, mo, d)) => MyValue::Date(y, mo, d, 0, 0, 0, 0),
        None => fallback(s),
    }
}

/// Canonical **naive** `TIMESTAMP` text (`"YYYY-MM-DD HH:MM:SS[.ffffff]"`, incl. the
/// `"0000-00-00 00:00:00"` zero sentinel) → a typed `MYSQL_TYPE_DATETIME` param.
fn parse_datetime(s: &str) -> MyValue {
    match split_naive_datetime(s) {
        Some(v) => v,
        None => fallback(s),
    }
}

/// Canonical `TIMESTAMPTZ` text (`"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"`) → a typed
/// `MYSQL_TYPE_DATETIME` param carrying the **UTC** components.
///
/// Truthful only under the session UTC pin (module docs). The zero-`TIMESTAMP` sentinel is the one
/// canonical `TIMESTAMPTZ` payload that is NOT RFC3339 — `mytext` renders it as the verbatim
/// `"0000-00-00 00:00:00"`, deliberately with neither `T` nor `Z` because it is not an instant — so
/// it is matched exactly, not by loosening the RFC3339 shape.
fn parse_rfc3339_utc(s: &str) -> MyValue {
    if s == ZERO_DATETIME {
        return MyValue::Date(0, 0, 0, 0, 0, 0, 0);
    }
    let Some(body) = s.strip_suffix('Z') else {
        return fallback(s);
    };
    // The RFC3339 `T` separator, and ONLY it: a space here would mean a naive payload arrived on
    // the instant tag.
    if body.as_bytes().get(10) != Some(&b'T') {
        return fallback(s);
    }
    match split_date_and_time(body, b'T') {
        Some(v) => v,
        None => fallback(s),
    }
}

/// Canonical `TIME` text (`"[-]HH:MM:SS[.ffffff]"`, hours up to 838) → a typed `MYSQL_TYPE_TIME`
/// param. The driver's component form carries the day overflow in its own field, so the hour count
/// is split back out: `"26:00:00"` → `Time(false, 1, 2, 0, 0, 0)`, mirroring
/// [`crate::mytext::time_to_text`], which folds it the other way.
fn parse_time(s: &str) -> MyValue {
    match split_time(s) {
        Some(v) => v,
        None => fallback(s),
    }
}

/// The impossible branch, made total. A canonical string the engine produced always parses, so this
/// is reached only by a payload no renderer emits — a PG-origin `infinity` bound at a MySQL pool,
/// or malformed client text. Passing the bytes through keeps `value_to_my` infallible (the module's
/// TOTAL invariant: there is no MySQL bind pre-flight to report a `Result` through) and lets the
/// SERVER produce its own clean, known-fate error instead of the engine fabricating a component
/// tuple — which would be the silent miscast charter rule 6 forbids.
fn fallback(s: &str) -> MyValue {
    MyValue::Bytes(s.as_bytes().to_vec())
}

/// `YYYY-MM-DD` → `(year, month, day)`. Strict: exactly ten bytes, ASCII digits only (so a
/// sign-prefixed `"+026-08-05"` cannot slip through `str::parse`'s leading-`+` tolerance). Month
/// and day may be **zero** — that is the legal zero-date / zero-in-date form `PROTOCOL.md` §3.2
/// pins — but never out of range.
fn split_date(s: &str) -> Option<(u16, u8, u8)> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y = digits(&s[0..4])? as u16;
    let mo = digits(&s[5..7])? as u8;
    let d = digits(&s[8..10])? as u8;
    if mo > 12 || d > 31 {
        return None;
    }
    Some((y, mo, d))
}

/// `HH:MM:SS[.ffffff]` as a **wall clock** → `(hour, minute, second, micros)`. Hours are bounded at
/// 23 here; the >24 h case belongs to `TIME` alone (see [`split_time`]). The fraction group is
/// absent or **exactly six** digits — the canonical rule, so a lenient `.25` is rejected rather
/// than silently scaled.
fn split_time_of_day(s: &str) -> Option<(u8, u8, u8, u32)> {
    let (hms, frac) = match s.split_once('.') {
        Some((hms, f)) => (hms, Some(f)),
        None => (s, None),
    };
    let b = hms.as_bytes();
    if b.len() != 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let h = digits(&hms[0..2])? as u8;
    let mi = digits(&hms[3..5])? as u8;
    let sec = digits(&hms[6..8])? as u8;
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let us = match frac {
        None => 0,
        Some(f) if f.len() == 6 => digits(f)?,
        Some(_) => return None,
    };
    Some((h, mi, sec, us))
}

/// `YYYY-MM-DD<sep>HH:MM:SS[.ffffff]` → the driver's component tuple.
fn split_date_and_time(s: &str, sep: u8) -> Option<MyValue> {
    if s.as_bytes().get(10) != Some(&sep) {
        return None;
    }
    let (y, mo, d) = split_date(&s[0..10])?;
    let (h, mi, sec, us) = split_time_of_day(&s[11..])?;
    Some(MyValue::Date(y, mo, d, h, mi, sec, us))
}

/// The naive `TIMESTAMP` form: a SPACE separator, no zone suffix, ever.
fn split_naive_datetime(s: &str) -> Option<MyValue> {
    split_date_and_time(s, b' ')
}

/// `[-]HH:MM:SS[.ffffff]` (hours unbounded by the wall clock) → `Time(neg, days, h, mi, s, us)`.
///
/// The magnitude is range-checked against the same [`MAX_TIME_US`] bound the READER enforces, so
/// the two directions agree by construction: a value `mytext::time_to_text` refuses to render is a
/// value this refuses to type. A zero magnitude is always bound unsigned — `-00:00:00` and
/// `00:00:00` must not be two driver params for one value.
fn split_time(s: &str) -> Option<MyValue> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    // The hour field is the only variable-width component (up to `838`).
    let (hh, tail) = rest.split_once(':')?;
    if hh.is_empty() || hh.len() > 3 {
        return None;
    }
    let hours = u64::from(digits(hh)?);
    let (mi, sec, us) = {
        // Reuse the wall-clock parser for `MM:SS[.ffffff]` by prefixing a placeholder hour: it
        // enforces the two-digit widths, the ranges and the six-digit fraction rule in one place.
        let (_, mi, sec, us) = split_time_of_day(&format!("00:{tail}"))?;
        (mi, sec, us)
    };

    const US_PER_SECOND: u64 = 1_000_000;
    let total_us = hours * 3_600 * US_PER_SECOND
        + u64::from(mi) * 60 * US_PER_SECOND
        + u64::from(sec) * US_PER_SECOND
        + u64::from(us);
    if total_us > MAX_TIME_US {
        return None;
    }

    let days = u32::try_from(hours / 24).ok()?;
    let h = u8::try_from(hours % 24).ok()?;
    Some(MyValue::Time(neg && total_us != 0, days, h, mi, sec, us))
}

/// An all-ASCII-digit run → its numeric value. Deliberately NOT `str::parse`, which accepts a
/// leading `+`/`-` and would let `"+026-08-05"` or `"-9"` masquerade as a canonical component.
/// Bounded to nine digits, so the `u32` cannot overflow (every caller's field is far shorter).
fn digits(s: &str) -> Option<u32> {
    if s.is_empty() || s.len() > 9 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u32>().ok()
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

    /// The module's documented invariant, restated for M1-S7: `value_to_my` is TOTAL over every
    /// canonical `Value` variant — no panic, no fallible arm, and only `Value::Null` produces a
    /// driver `NULL`. (MySQL has no `accepts`-style pre-flight — see the module docs — so totality
    /// is what keeps a bind from ever becoming a fate-unknown error.)
    #[test]
    fn value_to_my_is_total_over_every_canonical_variant() {
        let all = [
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("hi".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
            Value::U64(u64::MAX),
            Value::Decimal("-12345.6700".to_string()),
            Value::Date("2026-08-05".to_string()),
            Value::Time("-838:59:58.000001".to_string()),
            Value::Timestamp("2026-08-05 13:45:07.250000".to_string()),
            Value::TimestampTz("2026-08-05T13:45:07.250000Z".to_string()),
            Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".to_string()),
            Value::Json(r#"{"a":[1,2]}"#.to_string()),
        ];
        assert_eq!(all.len(), 14, "one instance of every canonical tag");

        match to_params(&all) {
            Params::Positional(vs) => {
                assert_eq!(vs.len(), 14);
                for (i, (v, my)) in all.iter().zip(vs.iter()).enumerate() {
                    let is_null = matches!(my, MyValue::NULL);
                    assert_eq!(
                        is_null,
                        matches!(v, Value::Null),
                        "param {i} ({v:?}) NULL-ness must mirror the canonical value"
                    );
                }
                // U64 above i64::MAX must survive as an UNSIGNED driver value, not wrap to Int.
                assert!(
                    matches!(vs[6], MyValue::UInt(u64::MAX)),
                    "U64(u64::MAX) must bind as MyValue::UInt, got {:?}",
                    vs[6]
                );
                // The four temporal tags must be TYPED driver params, never a text passthrough
                // (hazard 41) — see `temporal_tags_bind_as_typed_params_never_a_literal`.
                assert_eq!(vs[8], MyValue::Date(2026, 8, 5, 0, 0, 0, 0), "DATE");
                assert_eq!(vs[9], MyValue::Time(true, 34, 22, 59, 58, 1), "TIME");
                assert_eq!(
                    vs[10],
                    MyValue::Date(2026, 8, 5, 13, 45, 7, 250_000),
                    "TIMESTAMP"
                );
                assert_eq!(
                    vs[11],
                    MyValue::Date(2026, 8, 5, 13, 45, 7, 250_000),
                    "TIMESTAMPTZ"
                );
            }
            other => panic!("expected Params::Positional, got {other:?}"),
        }
    }

    /// **Hazard 41 — the C2 fix.** A `Z`-suffixed canonical `TIMESTAMPTZ` string can NEVER be
    /// passed through as a text param: MySQL 8 rejects it with `1292 Incorrect datetime value`
    /// under the default `STRICT_TRANS_TABLES`, and MariaDB 11 rejects offsets in datetime literals
    /// outright. The four temporal tags therefore become TYPED params (`MYSQL_TYPE_DATETIME` /
    /// `MYSQL_TYPE_TIME`) built from the canonical components — no server-side literal parsing at
    /// all.
    #[test]
    fn temporal_tags_bind_as_typed_params_never_a_literal() {
        for v in [
            Value::Date("2026-08-05".into()),
            Value::Time("13:45:07".into()),
            Value::Timestamp("2026-08-05 13:45:07".into()),
            Value::TimestampTz("2026-08-05T13:45:07Z".into()),
        ] {
            let my = value_to_my(&v);
            assert!(
                matches!(my, MyValue::Date(..) | MyValue::Time(..)),
                "{v:?} must bind as a TYPED temporal param, got {my:?} — a text literal is \
                 rejected by both servers (hazard 41)"
            );
        }
    }

    /// The canonical → component parse, at every edge the renderers can produce (the plan's
    /// prescribed table). These are the inverse of `mytext`'s renderers, so a value that survives
    /// a read must survive the bind back.
    #[test]
    fn time_and_datetime_helpers_survive_the_edges() {
        assert_eq!(
            parse_time("-838:59:58.000001"),
            MyValue::Time(true, 34, 22, 59, 58, 1)
        );
        assert_eq!(parse_time("26:00:00"), MyValue::Time(false, 1, 2, 0, 0, 0));
        assert_eq!(
            parse_rfc3339_utc("2026-08-05T11:45:07.250000Z"),
            MyValue::Date(2026, 8, 5, 11, 45, 7, 250_000)
        );
        assert_eq!(parse_date("0000-00-00"), MyValue::Date(0, 0, 0, 0, 0, 0, 0));
    }

    /// Whole-second and fractional forms both parse — the canonical text omits the fraction group
    /// entirely when it is zero (`PROTOCOL.md` §3.2), so "no `.ffffff`" is the COMMON case, not an
    /// edge one.
    #[test]
    fn the_fraction_group_is_optional_on_every_temporal_helper() {
        assert_eq!(
            parse_datetime("2026-08-05 11:45:07"),
            MyValue::Date(2026, 8, 5, 11, 45, 7, 0)
        );
        assert_eq!(
            parse_datetime("2026-08-05 11:45:07.000001"),
            MyValue::Date(2026, 8, 5, 11, 45, 7, 1)
        );
        assert_eq!(
            parse_rfc3339_utc("2026-08-05T11:45:07Z"),
            MyValue::Date(2026, 8, 5, 11, 45, 7, 0)
        );
        assert_eq!(parse_time("00:00:00"), MyValue::Time(false, 0, 0, 0, 0, 0));
        assert_eq!(
            parse_time("00:00:00.900000"),
            MyValue::Time(false, 0, 0, 0, 0, 900_000)
        );
        // The full documented TIME range, both signs.
        assert_eq!(
            parse_time("838:59:59.999999"),
            MyValue::Time(false, 34, 22, 59, 59, 999_999)
        );
        assert_eq!(
            parse_time("-838:59:59"),
            MyValue::Time(true, 34, 22, 59, 59, 0)
        );
        // `-00:00:00` and `00:00:00` are ONE value: the sign is dropped at zero magnitude, exactly
        // as `mytext::time_to_text` renders it.
        assert_eq!(parse_time("-00:00:00"), MyValue::Time(false, 0, 0, 0, 0, 0));
    }

    /// The zero sentinels (`PROTOCOL.md` §3.2) bind as the all-zero driver components, which
    /// `mysql_common` serializes as a zero-length datetime — the wire form of `'0000-00-00'`. They
    /// are deliberately NOT parsed as a calendar value on the way in either.
    #[test]
    fn zero_sentinels_bind_as_the_all_zero_components() {
        let zero = MyValue::Date(0, 0, 0, 0, 0, 0, 0);
        assert_eq!(parse_date("0000-00-00"), zero);
        assert_eq!(parse_datetime("0000-00-00 00:00:00"), zero);
        // A zero `TIMESTAMP` renders WITHOUT the `T`/`Z` (it is not an instant), so the
        // TIMESTAMPTZ parser must recognise that exact sentinel too.
        assert_eq!(parse_rfc3339_utc("0000-00-00 00:00:00"), zero);
        // A zero-in-date is legal without NO_ZERO_IN_DATE and carries through component-wise.
        assert_eq!(
            parse_date("2026-00-05"),
            MyValue::Date(2026, 0, 5, 0, 0, 0, 0)
        );
    }

    /// **The impossible branch is provably total.** Every helper falls back to a byte-string param
    /// rather than panicking or introducing a `Result` this module has no pre-flight to report
    /// through (`value_to_my` is TOTAL by the documented module invariant). The inputs below are
    /// the ones a canonical renderer never emits — a PG-origin `infinity` bound at a MySQL pool,
    /// and outright malformed text — and each stays a driver value the server can reject cleanly.
    #[test]
    fn non_canonical_text_falls_back_to_a_byte_string_never_a_panic() {
        /// One canonical-text parse helper, so the case table below can name which one each input
        /// is aimed at.
        type Parser = fn(&str) -> MyValue;

        let cases: &[(&str, Parser)] = &[
            // PG sentinels: legal DATE/TIMESTAMP payloads that MySQL has no representation for.
            ("infinity", parse_date),
            ("-infinity", parse_date),
            ("infinity", parse_datetime),
            ("-infinity", parse_rfc3339_utc),
            // Malformed / out-of-domain / wrong-shape.
            ("", parse_date),
            ("2026-8-5", parse_date),
            ("2026-08-05T00:00:00Z", parse_date),
            ("+026-08-05", parse_date),
            ("2026-13-05", parse_date),
            ("2026-08-32", parse_date),
            ("2026-08-05 11:45", parse_datetime),
            ("2026-08-05 11:45:60", parse_datetime),
            ("2026-08-05 24:00:00", parse_datetime),
            ("2026-08-05 11:45:07.25", parse_datetime),
            ("2026-08-05 11:45:07Z", parse_datetime),
            ("2026-08-05 11:45:07", parse_rfc3339_utc),
            ("2026-08-05T11:45:07", parse_rfc3339_utc),
            ("2026-08-05T11:45:07.250000+01:00", parse_rfc3339_utc),
            ("", parse_time),
            ("-", parse_time),
            ("1:2:3:4", parse_time),
            ("00:60:00", parse_time),
            ("00:00:60", parse_time),
            ("00:00:00.25", parse_time),
            // One µs past the documented MySQL TIME range: the reader refuses to render it, so
            // the writer refuses to type it.
            ("839:00:00", parse_time),
            ("-839:00:00", parse_time),
        ];
        for (text, f) in cases {
            let my = f(text);
            assert_eq!(
                my,
                MyValue::Bytes(text.as_bytes().to_vec()),
                "{text:?} is not canonical: it must fall back to a byte-string param, not a \
                 fabricated component tuple"
            );
        }
    }
}
