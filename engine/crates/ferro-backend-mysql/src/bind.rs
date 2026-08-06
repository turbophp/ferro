//! Canonical [`Value`] params → `mysql_async::Params` (positional), plus the ARITY pre-check.
//!
//! Unlike Postgres (whose prepared statement carries a server-inferred type per `$n`, so PG's
//! `bind` can pre-flight each param's `ToSql::accepts`), a MySQL `COM_STMT_PREPARE` exposes NO
//! inferred parameter types — every `?` is reported as an opaque placeholder. So the client-side
//! bind validation possible here is the parameter COUNT ([`validate_arity`]) plus the CANONICAL
//! SHAPE of each payload ([`to_params`]). Both are KNOWN-FATE [`bind_error`]s (`Sql{Unsupported}`,
//! branch `NonRetryable`) raised BEFORE anything is sent, so the statement provably never executed
//! — deliberately NOT `PoolError::ConnectionLost`, whose fate is unknown and would let the SQL
//! service mint a false `WriteUnconfirmed{Indeterminate}` for a write that never happened (§19.3,
//! the same no-false-Indeterminate safety PG's `bind`/`query` pre-validation enforces).
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
//! ## A non-canonical payload is REJECTED here, never passed through (Task 8b fix round 1)
//!
//! Canonical text the engine itself produced always parses. Anything else — a PG-origin
//! `date 'infinity'` bound at a MySQL pool, an over-range `TIME`, a PG `NUMERIC` `NaN` — has NO
//! MySQL representation, and the parse helpers below return `None` for it. That becomes a
//! [`bind_error`] at [`to_params`].
//!
//! It deliberately does **not** fall back to a byte-string param for the server to reject. That was
//! the original Task 8b design and it is only safe under `STRICT_TRANS_TABLES`. Measured on both
//! engines under `SET SESSION sql_mode = ''` — the mode a legacy stack routinely runs in, since
//! Doctrine sets no `sql_mode` — the server does not error, it **silently coerces**:
//! `Date("infinity")` → `0000-00-00`, `Time("839:00:00")` → `838:59:59`, `Decimal("NaN")` → `0.0000`.
//! That is the silent miscast charter rule 6 forbids, so the decision cannot be left to a server
//! session variable: the engine refuses the payload itself, pre-send, with a known fate.
//!
//! A legal MySQL SENTINEL is not a non-canonical payload: `"0000-00-00"`, `"0000-00-00 00:00:00"`
//! and a zero-in-date such as `"2026-00-05"` are values a permissive `sql_mode` legitimately
//! accepts, and they bind verbatim as the all-zero components (`PROTOCOL.md` §3.2).

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
/// an empty slice is `Params::Empty`.
///
/// This is the **per-param canonical pre-flight**, the companion to [`validate_arity`]: a payload
/// with no MySQL representation is a KNOWN-FATE [`bind_error`] raised before anything is sent (see
/// the module docs — passing it through for the server to reject is a SILENT COERCION under a
/// permissive `sql_mode`). Both pre-checks run back to back at the one call site,
/// `query::run`.
pub fn to_params(params: &[Value]) -> Result<Params, PoolError> {
    if params.is_empty() {
        return Ok(Params::Empty);
    }
    let mut out = Vec::with_capacity(params.len());
    for (idx, v) in params.iter().enumerate() {
        out.push(value_to_my(v, idx)?);
    }
    Ok(Params::Positional(out))
}

/// The canonical → driver `Value` mapping. `Bool` binds as the MySQL boolean idiom (`0`/`1` integer);
/// `Text` and `Bytes` both bind as `Bytes` (MySQL has one byte-string param form — the target
/// column's type decides the interpretation server-side).
///
/// `idx` is the zero-based param position, carried only so a rejection can name the offending `?`.
fn value_to_my(v: &Value, idx: usize) -> Result<MyValue, PoolError> {
    Ok(match v {
        Value::Null => MyValue::NULL,
        Value::Bool(b) => MyValue::Int(*b as i64),
        Value::I64(n) => MyValue::Int(*n),
        Value::F64(f) => MyValue::Double(*f),
        Value::Text(s) => MyValue::Bytes(s.clone().into_bytes()),
        Value::Bytes(b) => MyValue::Bytes(b.clone()),
        // ---- M1-S7 canonical tags. Every arm below either has an exact MySQL representation or
        // is REFUSED here — never coerced, never passed through (module docs, fix round 1).
        Value::U64(n) => MyValue::UInt(*n),
        // UUID/JSON: the canonical text IS what the server wants as a string param.
        Value::Uuid(s) | Value::Json(s) => MyValue::Bytes(s.clone().into_bytes()),
        // DECIMAL likewise — but PG's `NaN`/`Infinity`/`-Infinity` are legal canonical payloads with
        // no MySQL representation, and a permissive `sql_mode` stores them as `0`.
        Value::Decimal(s) if is_decimal_text(s) => MyValue::Bytes(s.clone().into_bytes()),
        Value::Decimal(s) => return Err(reject(idx, "DECIMAL", s)),
        // DATE / TIME / TIMESTAMP / TIMESTAMPTZ: TYPED params built from the canonical text (see
        // the module docs, hazard 41). Never a `Z`-suffixed literal — both servers reject it.
        Value::Date(s) => parse_date(s).ok_or_else(|| reject(idx, "DATE", s))?,
        Value::Time(s) => parse_time(s).ok_or_else(|| reject(idx, "TIME", s))?,
        Value::Timestamp(s) => parse_datetime(s).ok_or_else(|| reject(idx, "TIMESTAMP", s))?,
        // Correct ONLY under the `time_zone = '+00:00'` session pin — the components are bound
        // bare and the server reads them in the session zone. The two are COUPLED (module docs).
        Value::TimestampTz(s) => {
            parse_rfc3339_utc(s).ok_or_else(|| reject(idx, "TIMESTAMPTZ", s))?
        }
    })
}

/// Canonical `DATE` text (`"YYYY-MM-DD"`, incl. the `"0000-00-00"` zero sentinel) → a typed
/// date-only param. `None` is the non-canonical branch — see [`reject`].
fn parse_date(s: &str) -> Option<MyValue> {
    let (y, mo, d) = split_date(s)?;
    Some(MyValue::Date(y, mo, d, 0, 0, 0, 0))
}

/// Canonical **naive** `TIMESTAMP` text (`"YYYY-MM-DD HH:MM:SS[.ffffff]"`, incl. the
/// `"0000-00-00 00:00:00"` zero sentinel) → a typed `MYSQL_TYPE_DATETIME` param.
fn parse_datetime(s: &str) -> Option<MyValue> {
    split_naive_datetime(s)
}

/// Canonical `TIMESTAMPTZ` text (`"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"`) → a typed
/// `MYSQL_TYPE_DATETIME` param carrying the **UTC** components.
///
/// Truthful only under the session UTC pin (module docs). The zero-`TIMESTAMP` sentinel is the one
/// canonical `TIMESTAMPTZ` payload that is NOT RFC3339 — `mytext` renders it as the verbatim
/// `"0000-00-00 00:00:00"`, deliberately with neither `T` nor `Z` because it is not an instant — so
/// it is matched exactly, not by loosening the RFC3339 shape.
fn parse_rfc3339_utc(s: &str) -> Option<MyValue> {
    if s == ZERO_DATETIME {
        return Some(MyValue::Date(0, 0, 0, 0, 0, 0, 0));
    }
    let body = s.strip_suffix('Z')?;
    // The RFC3339 `T` separator, and ONLY it: a space here would mean a naive payload arrived on
    // the instant tag.
    if body.as_bytes().get(10) != Some(&b'T') {
        return None;
    }
    split_date_and_time(body, b'T')
}

/// Canonical `TIME` text (`"[-]HH:MM:SS[.ffffff]"`, hours up to 838) → a typed `MYSQL_TYPE_TIME`
/// param. The driver's component form carries the day overflow in its own field, so the hour count
/// is split back out: `"26:00:00"` → `Time(false, 1, 2, 0, 0, 0)`, mirroring
/// [`crate::mytext::time_to_text`], which folds it the other way.
fn parse_time(s: &str) -> Option<MyValue> {
    split_time(s)
}

/// A canonical `DECIMAL` payload MySQL can actually store: `[+-]digits[.digits]`, the exact grammar
/// [`crate::mytext::decimal_to_text`] validates on the way OUT, so the two directions agree by
/// construction. It deliberately EXCLUDES PG `NUMERIC`'s `NaN` / `Infinity` / `-Infinity`, which are
/// legal canonical payloads (`PROTOCOL.md` §3.2) that MySQL has no representation for — and which a
/// permissive `sql_mode` stores as `0` rather than rejecting.
fn is_decimal_text(s: &str) -> bool {
    let body = s.strip_prefix(['-', '+']).unwrap_or(s);
    let (int, frac) = match body.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (body, None),
    };
    let digits_ok = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    digits_ok(int) && frac.is_none_or(digits_ok)
}

/// The non-canonical branch: a payload with NO MySQL representation → a KNOWN-FATE, PRE-SEND
/// rejection. Reached only by text no canonical renderer emits — a PG-origin `infinity` /
/// `NaN` bound at a MySQL pool, an over-range `TIME`, or malformed client text.
///
/// It is deliberately NOT passed through as a byte string for the server to reject: under a
/// permissive `sql_mode` the server silently COERCES it instead of erroring (module docs), so the
/// engine would be shipping a corrupt write. Refusing here also cannot fabricate a component tuple,
/// the other half of the charter rule 6 miscast ban.
fn reject(idx: usize, kind: &str, text: &str) -> PoolError {
    bind_error(format!(
        "parameter {idx}: {text:?} has no MySQL/MariaDB {kind} representation — refused before \
         sending (binding it as text would be silently coerced under a permissive sql_mode)"
    ))
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

    /// `to_params`, unwrapped — every case below is a CANONICAL param set, which must always bind.
    fn bound(params: &[Value]) -> Params {
        to_params(params).unwrap_or_else(|e| panic!("canonical params must bind, got {e:?}"))
    }

    #[test]
    fn to_params_maps_every_scalar_positionally() {
        // Empty → Params::Empty (MySQL's no-param form).
        assert!(matches!(bound(&[]), Params::Empty));

        let params = [
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("hi".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
        ];
        match bound(&params) {
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

    /// `value_to_my` binds every canonical `Value` variant — no panic, no rejection of a legitimate
    /// payload, and only `Value::Null` produces a driver `NULL`. (MySQL has no `accepts`-style
    /// server-side pre-flight — see the module docs — so this canonical-shape check IS the bind
    /// pre-flight, and it must accept everything a canonical renderer can emit for MySQL.)
    #[test]
    fn value_to_my_binds_every_canonical_variant() {
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

        match bound(&all) {
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
            let my = value_to_my(&v, 0).expect("a canonical temporal payload must bind");
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
            Some(MyValue::Time(true, 34, 22, 59, 58, 1))
        );
        assert_eq!(
            parse_time("26:00:00"),
            Some(MyValue::Time(false, 1, 2, 0, 0, 0))
        );
        // `24:00:00` is the top of a day as a DURATION — legal, and never confused with the
        // wall-clock hour bound the naive-datetime parser enforces.
        assert_eq!(
            parse_time("24:00:00"),
            Some(MyValue::Time(false, 1, 0, 0, 0, 0))
        );
        assert_eq!(
            parse_rfc3339_utc("2026-08-05T11:45:07.250000Z"),
            Some(MyValue::Date(2026, 8, 5, 11, 45, 7, 250_000))
        );
        assert_eq!(
            parse_date("0000-00-00"),
            Some(MyValue::Date(0, 0, 0, 0, 0, 0, 0))
        );
    }

    /// Whole-second and fractional forms both parse — the canonical text omits the fraction group
    /// entirely when it is zero (`PROTOCOL.md` §3.2), so "no `.ffffff`" is the COMMON case, not an
    /// edge one.
    #[test]
    fn the_fraction_group_is_optional_on_every_temporal_helper() {
        assert_eq!(
            parse_datetime("2026-08-05 11:45:07"),
            Some(MyValue::Date(2026, 8, 5, 11, 45, 7, 0))
        );
        assert_eq!(
            parse_datetime("2026-08-05 11:45:07.000001"),
            Some(MyValue::Date(2026, 8, 5, 11, 45, 7, 1))
        );
        assert_eq!(
            parse_rfc3339_utc("2026-08-05T11:45:07Z"),
            Some(MyValue::Date(2026, 8, 5, 11, 45, 7, 0))
        );
        assert_eq!(
            parse_time("00:00:00"),
            Some(MyValue::Time(false, 0, 0, 0, 0, 0))
        );
        assert_eq!(
            parse_time("00:00:00.900000"),
            Some(MyValue::Time(false, 0, 0, 0, 0, 900_000))
        );
        // The full documented TIME range, both signs.
        assert_eq!(
            parse_time("838:59:59.999999"),
            Some(MyValue::Time(false, 34, 22, 59, 59, 999_999))
        );
        assert_eq!(
            parse_time("-838:59:59"),
            Some(MyValue::Time(true, 34, 22, 59, 59, 0))
        );
        // `-00:00:00` and `00:00:00` are ONE value: the sign is dropped at zero magnitude, exactly
        // as `mytext::time_to_text` renders it.
        assert_eq!(
            parse_time("-00:00:00"),
            Some(MyValue::Time(false, 0, 0, 0, 0, 0))
        );
    }

    /// The zero sentinels (`PROTOCOL.md` §3.2) bind as the all-zero driver components, which
    /// `mysql_common` serializes as a zero-length datetime — the wire form of `'0000-00-00'`. They
    /// are deliberately NOT parsed as a calendar value on the way in either.
    #[test]
    fn zero_sentinels_bind_as_the_all_zero_components() {
        let zero = Some(MyValue::Date(0, 0, 0, 0, 0, 0, 0));
        assert_eq!(parse_date("0000-00-00"), zero);
        assert_eq!(parse_datetime("0000-00-00 00:00:00"), zero);
        // A zero `TIMESTAMP` renders WITHOUT the `T`/`Z` (it is not an instant), so the
        // TIMESTAMPTZ parser must recognise that exact sentinel too.
        assert_eq!(parse_rfc3339_utc("0000-00-00 00:00:00"), zero);
        // A zero-in-date is legal without NO_ZERO_IN_DATE and carries through component-wise.
        assert_eq!(
            parse_date("2026-00-05"),
            Some(MyValue::Date(2026, 0, 5, 0, 0, 0, 0))
        );
        // A legal SENTINEL is NOT a non-canonical payload: the fix must never reject one, at any
        // level — these are exactly the values a permissive `sql_mode` exists to allow.
        for v in [
            Value::Date("0000-00-00".into()),
            Value::Timestamp("0000-00-00 00:00:00".into()),
            Value::TimestampTz("0000-00-00 00:00:00".into()),
            Value::Date("2026-00-05".into()),
        ] {
            assert!(
                to_params(std::slice::from_ref(&v)).is_ok(),
                "{v:?} is a LEGAL MySQL sentinel and must still bind"
            );
        }
    }

    /// **The non-canonical branch is a PRE-SEND REJECTION, never a passthrough and never a panic
    /// (fix round 1).** The inputs below are the ones a canonical renderer never emits for MySQL —
    /// a PG-origin `infinity` bound at a MySQL pool, an over-range `TIME`, outright malformed text
    /// — and each must produce `None` here rather than a byte-string param the server would
    /// *silently coerce* under `sql_mode = ''` (module docs), or a fabricated component tuple.
    #[test]
    fn non_canonical_text_is_refused_by_every_helper() {
        /// One canonical-text parse helper, so the case table below can name which one each input
        /// is aimed at.
        type Parser = fn(&str) -> Option<MyValue>;

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
            assert_eq!(
                f(text),
                None,
                "{text:?} is not canonical: it must be REFUSED, not bound as a byte string (which \
                 a permissive sql_mode silently coerces) and not as a fabricated component tuple"
            );
        }
    }

    /// **The rejection reaches the caller as a KNOWN-FATE, PRE-SEND `Sql{Unsupported}`** — the same
    /// shape `validate_arity` produces, and deliberately never `ConnectionLost`, which would let the
    /// SQL service mint a false §19.3 `Indeterminate` for a write that never left the engine.
    ///
    /// The `Decimal` cases are the ones no temporal helper covers: PG `NUMERIC`'s `NaN` /
    /// `Infinity` / `-Infinity` are LEGAL canonical payloads (`PROTOCOL.md` §3.2) with no MySQL
    /// representation, which a permissive `sql_mode` stores as `0`.
    #[test]
    fn a_non_canonical_param_is_a_known_fate_pre_send_bind_error() {
        for v in [
            Value::Date("infinity".into()),
            Value::Date("-infinity".into()),
            Value::Timestamp("infinity".into()),
            Value::TimestampTz("-infinity".into()),
            Value::Time("839:00:00".into()),
            Value::Time("-839:00:00".into()),
            Value::Decimal("NaN".into()),
            Value::Decimal("Infinity".into()),
            Value::Decimal("-Infinity".into()),
            Value::Decimal("1e5".into()),
            Value::Decimal("".into()),
        ] {
            // Bound at position 1 so the message's param index is actually exercised.
            match to_params(&[Value::I64(1), v.clone()]) {
                Err(PoolError::Sql {
                    code,
                    branch: b,
                    sqlstate,
                    message,
                }) => {
                    assert_eq!(code, errc::UNSUPPORTED, "{v:?}");
                    assert_eq!(b, errc::UNSUPPORTED_BRANCH, "{v:?} must be NonRetryable");
                    assert_eq!(
                        sqlstate, None,
                        "{v:?}: the server never saw the statement, so there is no SQLSTATE"
                    );
                    assert!(
                        message.starts_with("parameter 1:"),
                        "{v:?}: the message must name the offending placeholder, got {message:?}"
                    );
                }
                Err(PoolError::ConnectionLost) => panic!(
                    "REGRESSION: {v:?} must NEVER be ConnectionLost — a pre-send bind rejection \
                     has a KNOWN fate (§19.3)"
                ),
                other => panic!("{v:?}: expected Sql{{Unsupported}}, got {other:?}"),
            }
        }

        // The finite DECIMAL grammar — including the display scale and an explicit sign — is
        // untouched by the check (the over-rejection guard for `is_decimal_text`).
        for text in [
            "0",
            "1.10",
            "-12345.6700000000",
            "+1.5",
            "18446744073709551615",
            "0.0000000001",
        ] {
            assert!(
                is_decimal_text(text),
                "{text:?} is a canonical DECIMAL and must still bind"
            );
        }
        for text in ["NaN", "Infinity", "-Infinity", "1e5", "1.", ".5", "-", ""] {
            assert!(
                !is_decimal_text(text),
                "{text:?} has no MySQL DECIMAL representation"
            );
        }
    }
}
