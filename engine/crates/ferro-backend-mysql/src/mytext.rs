//! MySQL/MariaDB driver value → **canonical text** renderers (M1-S7, `proto/PROTOCOL.md` §3.2).
//!
//! Every function here is pure formatting over ONE `mysql_async::Value` — the driver's
//! **already-parsed** representation of a single column cell — and yields the canonical wire text
//! that `ferro-proto`'s `Value::{Decimal,Date,Time,Timestamp,TimestampTz,Json}` carries as a msgpack
//! `str`. The rendering decision lives where the source format is known (the backend), never in the
//! codec (PROTOCOL.md §3.2).
//!
//! **This is NOT the PG story.** `ferro-backend-pg`'s `pgtext` decodes PG's *raw binary payloads*
//! (`fn(&[u8])`); MySQL's driver hands back split components, so nothing is shared and nothing may
//! be reused across the two — different inputs entirely.
//!
//! **No date library.** The components are already split, and every library abstraction here is
//! actively harmful: a time-of-day type **wraps** (MySQL `TIME` legally reaches ±838:59:59, more
//! than a day, and may be **negative**), and a decimal/float type **normalizes away the display
//! scale** (`1.10` and `1.1` are distinct canonical payloads).
//!
//! ## The driver's component layouts (verified against `mysql_common-0.37.3/src/value/mod.rs:66-69`)
//!
//! ```text
//! Value::Date(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8, micros: u32)
//! Value::Time(is_negative: bool, days: u32, hours: u8, minutes: u8, seconds: u8, micros: u32)
//! ```
//!
//! Note `Time` carries **`days` separately** — the hour field alone never exceeds 23, so a caller
//! that ignores `days` silently renders `26:00:00` as `02:00:00`.
//!
//! ## Rules this module exists to keep (each has a unit test)
//!
//! - **`TIME` is not a time-of-day.** Fold `days` into the hour field (hours may far exceed 23) and
//!   emit a leading `-` for a negative value.
//! - **Fractional seconds** are emitted as **no group at all** when zero, otherwise **exactly six**
//!   digits — never trailing-zero-trimmed, so the payload is byte-stable for the golden vectors.
//! - **Zero dates** (`'0000-00-00'`, `'0000-00-00 00:00:00'`, `year = 0`) AND **zero-in-dates**
//!   (only some components zero: `'2026-00-05'`, `'2026-08-00'`) are legal MySQL values under a
//!   permissive `sql_mode`. Both render as the **verbatim, deliberately non-parseable sentinel
//!   text** PROTOCOL.md §3.2 pins — never as an error, and never as an invented calendar date. §3.2
//!   defines the sentinel class by a **zero-component test**, not by the four named literal forms,
//!   precisely so the partial case cannot be parsed as a calendar day by a conforming decoder.
//! - **`DECIMAL` is passed through byte-for-byte.** The server's own ASCII rendering already carries
//!   the display scale; parsing and re-rendering it (through any numeric type, and *especially*
//!   through a float) is the precision loss §9.1 exists to prevent.
//! - **`DATETIME` is naive, `TIMESTAMP` is a UTC instant.** Identical driver components, different
//!   canonical forms (SPEC §9). The `Z` on the `TIMESTAMPTZ` form is truthful ONLY because
//!   `conn::connect` pins every session to `time_zone = '+00:00'` — MySQL converts `TIMESTAMP` into
//!   the session zone on retrieval and the driver returns zone-LESS components, so the two are
//!   coupled (see `tests/utc_pin_it.rs`).
//! - A wrong-variant or out-of-domain cell is a client-side **decode mismatch** →
//!   `PoolError::Backend` (NonRetryable, SPEC §9.1), **never** a panic and never a `ConnectionLost`
//!   that could mint a false §19.3 `Indeterminate`.
//!
//! Which renderer a column gets is decided by the single column-metadata authority in
//! [`crate::rowmap`] — this module never inspects metadata and never guesses.

use ferro_pool::error::PoolError;
use mysql_async::Value as MyValue;

/// The largest magnitude a MySQL/MariaDB `TIME` can hold: `838:59:59.999999`. The server clamps
/// out-of-range results (`SEC_TO_TIME(999999999)` → `838:59:59` + a warning), so anything beyond
/// this is a corrupt payload, not a value — and rendering it would emit a plausible-looking lie.
///
/// **The trailing `.999999` is REQUIRED, not slack — do not "tighten" this to match MySQL.** The
/// two engines diverge at the fraction: MySQL rejects `'838:59:59.999999'` under strict mode and
/// truncates it to `838:59:59.000000` under a permissive `sql_mode`, but **MariaDB stores and
/// returns it exactly** (measured live on MariaDB 11.8: `CAST('838:59:59.999999' AS TIME(6))` →
/// `838:59:59.999999`). A bound narrowed to MySQL's clamp would reject a legal MariaDB value.
///
/// Shared with [`crate::bind`] (M1-S7 Task 8b) so the read and write directions agree by
/// construction: a magnitude this renderer refuses to emit is one the binder refuses to type.
pub(crate) const MAX_TIME_US: u64 = ((838 * 3600) + (59 * 60) + 59) * 1_000_000 + 999_999;

const US_PER_SECOND: u64 = 1_000_000;

/// MySQL `DATE` → `"YYYY-MM-DD"`, or the verbatim `"0000-00-00"` zero-date sentinel.
///
/// The driver delivers a `DATE` column as a [`MyValue::Date`] whose time components are all zero
/// (the binary protocol sends a 4-byte, date-only payload). A **non-zero** time part therefore means
/// this renderer was handed a `DATETIME`/`TIMESTAMP` cell, and silently dropping the time would be
/// exactly the miscast charter rule 6 forbids — so it is rejected loudly instead.
pub fn date_to_text(v: &MyValue) -> Result<String, PoolError> {
    let (y, mo, d, h, mi, s, us) = date_parts(v, "date")?;
    if (h, mi, s, us) != (0, 0, 0, 0) {
        return Err(backend(format!(
            "date: cell carries a non-zero time part ({h:02}:{mi:02}:{s:02}.{us:06}); a DATE column \
             never does — the column classifier and the renderer disagree"
        )));
    }
    Ok(canonical_date(y, mo, d))
}

/// MySQL `DATETIME` → **naive** `"YYYY-MM-DD HH:MM:SS[.ffffff]"`, no zone suffix ever, or the
/// verbatim `"0000-00-00 00:00:00"` zero-datetime sentinel.
pub fn datetime_to_text(v: &MyValue) -> Result<String, PoolError> {
    let (y, mo, d, h, mi, s, us) = date_parts(v, "datetime")?;
    check_time_of_day(h, mi, s, us, "datetime")?;
    Ok(format!(
        "{} {}",
        canonical_date(y, mo, d),
        fmt_time_of_day(h, mi, s, us)
    ))
}

/// MySQL `TIMESTAMP` → RFC3339 `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"`.
///
/// The driver components are **byte-identical** to `DATETIME`'s — only the column type separates
/// naive-local from UTC-instant, which is why this is a separate function rather than a flag on one
/// (SPEC §9: MySQL `datetime` → `TIMESTAMP`, MySQL `timestamp` → `TIMESTAMPTZ`).
///
/// **The `Z` is truthful only under the session pin.** MySQL stores `TIMESTAMP` in UTC and converts
/// it into the session `time_zone` on retrieval; `conn::connect` folds `time_zone = '+00:00'` into
/// the setup list (re-applied after every `COM_RESET_CONNECTION`), so the components ARE UTC. Remove
/// that pin and this function starts lying.
///
/// The all-zero `TIMESTAMP` renders as the verbatim `"0000-00-00 00:00:00"` sentinel — the literal
/// form PROTOCOL.md §3.2 pins, deliberately with neither the `T` separator nor the `Z` suffix, since
/// it is not an instant and must not parse as one.
pub fn timestamptz_to_text(v: &MyValue) -> Result<String, PoolError> {
    let (y, mo, d, h, mi, s, us) = date_parts(v, "timestamp")?;
    check_time_of_day(h, mi, s, us, "timestamp")?;
    if (y, mo, d, h, mi, s, us) == (0, 0, 0, 0, 0, 0, 0) {
        return Ok(ZERO_DATETIME.to_string());
    }
    Ok(format!(
        "{}T{}Z",
        canonical_date(y, mo, d),
        fmt_time_of_day(h, mi, s, us)
    ))
}

/// MySQL `TIME` → `"[-]HH:MM:SS[.ffffff]"`.
///
/// **Not a time-of-day.** A MySQL `TIME` is a signed *duration* spanning `-838:59:59` ..
/// `838:59:59`, delivered as `(is_negative, days, hours, minutes, seconds, micros)` with the
/// day-overflow in its own field. `days` is folded into the hour field (so `Time(false, 1, 2, …)`
/// renders `26:00:00`, never `02:00:00`) and a negative value gets a leading `-`.
///
/// A zero magnitude renders unsigned (`"00:00:00"`), matching the server's own text form: `-0` and
/// `0` must not be two distinct canonical payloads for one value.
pub fn time_to_text(v: &MyValue) -> Result<String, PoolError> {
    let MyValue::Time(neg, days, h, mi, s, us) = v else {
        return Err(wrong_variant("time", v));
    };
    check_time_of_day(*h, *mi, *s, *us, "time")?;

    // u64 AND saturating: `days` is a u32, so `days * 86_400 * 1e6` overflows u32 — and at
    // `u32::MAX` it overflows **u64** too (~3.7e20 µs), which in a debug build is a panic and in a
    // release build a wrapped, plausible-looking value that would sail past the range check. A
    // corrupt payload must always REACH the check below, so every step saturates instead.
    let total_us = u64::from(*days)
        .saturating_mul(86_400)
        .saturating_mul(US_PER_SECOND)
        .saturating_add(u64::from(*h) * 3_600 * US_PER_SECOND)
        .saturating_add(u64::from(*mi) * 60 * US_PER_SECOND)
        .saturating_add(u64::from(*s) * US_PER_SECOND)
        .saturating_add(u64::from(*us));
    if total_us > MAX_TIME_US {
        return Err(backend(format!(
            "time: magnitude ({total_us} µs) exceeds the MySQL TIME range of ±838:59:59.999999 \
             ({MAX_TIME_US} µs)"
        )));
    }

    let hours = total_us / (3_600 * US_PER_SECOND);
    let mins = (total_us % (3_600 * US_PER_SECOND)) / (60 * US_PER_SECOND);
    let secs = (total_us % (60 * US_PER_SECOND)) / US_PER_SECOND;
    let frac = total_us % US_PER_SECOND;
    let sign = if *neg && total_us != 0 { "-" } else { "" };
    if frac == 0 {
        Ok(format!("{sign}{hours:02}:{mins:02}:{secs:02}"))
    } else {
        Ok(format!("{sign}{hours:02}:{mins:02}:{secs:02}.{frac:06}"))
    }
}

/// MySQL `DECIMAL`/`NEWDECIMAL` → the server's own ASCII rendering, **verbatim**.
///
/// The driver delivers a decimal column as raw bytes holding the text the server produced, and that
/// text already carries the **display scale** — so `1.10` and `1.1` arrive (and stay) distinct, as
/// PROTOCOL.md §3.2 requires. This function therefore validates the *shape* and copies the bytes; it
/// deliberately does **not** parse. Routing through `f64` would lose precision outright, and routing
/// through any decimal type would normalize the scale away and break DBAL's string comparisons.
pub fn decimal_to_text(v: &MyValue) -> Result<String, PoolError> {
    let text = bytes_utf8(v, "decimal")?;
    // Shape-only: `[-|+]digits[.digits]`, which is the entire grammar the server emits for a
    // DECIMAL (no exponent form, ever). A cell that does not match means a non-decimal column was
    // routed here — a loud decode mismatch beats a silently miscast payload.
    let body = text.strip_prefix(['-', '+']).unwrap_or(&text);
    let (int, frac) = match body.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (body, None),
    };
    let digits_ok = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    if !digits_ok(int) || frac.is_some_and(|f| !digits_ok(f)) {
        return Err(backend(format!(
            "decimal: {text:?} is not a MySQL DECIMAL rendering ([-|+]digits[.digits])"
        )));
    }
    Ok(text)
}

/// MySQL `JSON` → the raw UTF-8 document text, **verbatim**.
///
/// The engine does not re-serialize and does not validate the document beyond UTF-8 (which the
/// msgpack `str` family requires) — the client decodes lazily (PROTOCOL.md §3.2).
///
/// MySQL 8 only: **MariaDB has no JSON type** (its `JSON` is an alias for `LONGTEXT` plus a
/// `json_valid()` CHECK, indistinguishable on the wire), so a MariaDB `JSON` column is classified as
/// text by design — promoting it here would be a silent miscast (charter rule 6).
pub fn json_to_text(v: &MyValue) -> Result<String, PoolError> {
    bytes_utf8(v, "json")
}

/// The verbatim zero-datetime sentinel (PROTOCOL.md §3.2) — carried as-is for both the `TIMESTAMP`
/// and `TIMESTAMPTZ` tags, deliberately not parseable as a calendar value.
///
/// Shared with [`crate::bind`] (M1-S7 Task 8b), which must recognise the exact same literal on the
/// way back in — it is the one canonical `TIMESTAMPTZ` payload that is not RFC3339.
pub(crate) const ZERO_DATETIME: &str = "0000-00-00 00:00:00";

/// Destructure a [`MyValue::Date`] or fail with the §9.1 decode-mismatch error.
#[allow(clippy::type_complexity)]
fn date_parts(v: &MyValue, what: &str) -> Result<(u16, u8, u8, u8, u8, u8, u32), PoolError> {
    match v {
        MyValue::Date(y, mo, d, h, mi, s, us) => Ok((*y, *mo, *d, *h, *mi, *s, *us)),
        other => Err(wrong_variant(what, other)),
    }
}

/// `YYYY-MM-DD`. Month/day are NOT range-checked below 1: `0` is legal in both (a zero date, or a
/// zero-in-date such as `2026-00-05` wherever `sql_mode` omits `NO_ZERO_IN_DATE` — MariaDB 11's
/// default), and PROTOCOL.md §3.2's **Sentinels** paragraph pins any zero year/month/day component
/// as verbatim sentinel text. The year is a `u16` the server bounds to 9999, so `{:04}` is exact.
fn canonical_date(y: u16, mo: u8, d: u8) -> String {
    format!("{y:04}-{mo:02}-{d:02}")
}

/// `HH:MM:SS` or `HH:MM:SS.ffffff` — **no** fraction group when the sub-second part is zero,
/// otherwise exactly six digits (never trailing-zero-trimmed: the payload must be byte-stable for
/// the golden vectors).
fn fmt_time_of_day(h: u8, mi: u8, s: u8, us: u32) -> String {
    if us == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{us:06}")
    }
}

/// Range-check the wall-clock components. `hours` is bounded at 23 here — the >24 h case belongs to
/// `TIME` alone, where it lives in the separate `days` field. An out-of-domain component is corrupt
/// input; rendering it would emit a well-formed lie such as `2026-08-05 99:61:61`.
fn check_time_of_day(h: u8, mi: u8, s: u8, us: u32, what: &str) -> Result<(), PoolError> {
    // MySQL has no leap seconds (`SECOND` is 0..=59), so 60 is out of domain.
    if h > 23 || mi > 59 || s > 59 || us >= 1_000_000 {
        return Err(backend(format!(
            "{what}: out-of-domain time components {h:02}:{mi:02}:{s:02}.{us:06}"
        )));
    }
    Ok(())
}

/// Raw driver bytes → an owned UTF-8 `String`, verbatim.
fn bytes_utf8(v: &MyValue, what: &str) -> Result<String, PoolError> {
    let MyValue::Bytes(raw) = v else {
        return Err(wrong_variant(what, v));
    };
    String::from_utf8(raw.clone())
        .map_err(|e| backend(format!("{what}: payload is not valid UTF-8: {e}")))
}

/// A cell that does not match its column's classified kind. Names the variant it *did* get, but
/// NEVER its contents — a value here can be user data (SPEC §12 secret hygiene).
fn wrong_variant(what: &str, got: &MyValue) -> PoolError {
    let variant = match got {
        MyValue::NULL => "NULL",
        MyValue::Bytes(_) => "Bytes",
        MyValue::Int(_) => "Int",
        MyValue::UInt(_) => "UInt",
        MyValue::Float(_) => "Float",
        MyValue::Double(_) => "Double",
        MyValue::Date(..) => "Date",
        MyValue::Time(..) => "Time",
    };
    backend(format!(
        "{what}: cell arrived as a driver `{variant}` value, which cannot be rendered as {what} \
         canonical text"
    ))
}

/// A render failure is a client-side **decode mismatch** (SPEC §9.1), i.e. `Backend`
/// (NonRetryable) — never `ConnectionLost`, which would mint a false §19.3 `Indeterminate`.
fn backend(msg: String) -> PoolError {
    PoolError::Backend(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value as MyValue;

    // DATETIME is naive; TIMESTAMP renders as a UTC instant — truthful ONLY because the session is
    // pinned to '+00:00' (hazard 24). Same driver components, different rendering.
    #[test]
    fn datetime_is_naive_and_timestamp_is_utc_z() {
        let v = MyValue::Date(2026, 8, 5, 13, 45, 7, 250_000);
        assert_eq!(datetime_to_text(&v).unwrap(), "2026-08-05 13:45:07.250000");
        assert_eq!(
            timestamptz_to_text(&v).unwrap(),
            "2026-08-05T13:45:07.250000Z"
        );
    }

    #[test]
    fn zero_sub_second_omits_the_fraction() {
        let v = MyValue::Date(2026, 8, 5, 13, 45, 7, 0);
        assert_eq!(datetime_to_text(&v).unwrap(), "2026-08-05 13:45:07");
        assert_eq!(timestamptz_to_text(&v).unwrap(), "2026-08-05T13:45:07Z");
    }

    /// The fraction group is all-or-exactly-six, never trailing-zero-trimmed — the payload has to be
    /// byte-stable for the golden vectors.
    #[test]
    fn fractional_seconds_are_all_or_exactly_six_digits() {
        assert_eq!(
            datetime_to_text(&MyValue::Date(2026, 8, 5, 0, 0, 0, 1)).unwrap(),
            "2026-08-05 00:00:00.000001"
        );
        assert_eq!(
            datetime_to_text(&MyValue::Date(2026, 8, 5, 0, 0, 0, 100_000)).unwrap(),
            "2026-08-05 00:00:00.100000"
        );
        assert_eq!(
            timestamptz_to_text(&MyValue::Date(2026, 8, 5, 0, 0, 0, 999_999)).unwrap(),
            "2026-08-05T00:00:00.999999Z"
        );
        assert_eq!(
            time_to_text(&MyValue::Time(false, 0, 0, 0, 0, 900_000)).unwrap(),
            "00:00:00.900000"
        );
    }

    // Hazard 27: MySQL zero-dates are legal and must surface as canonical text, not an error.
    // This is the ONLY MySQL-8 coverage of hazard 27 — its default sql_mode blocks a live insert
    // (F35), so the live case runs on MariaDB. Do not delete it.
    #[test]
    fn zero_dates_render_literally() {
        assert_eq!(
            date_to_text(&MyValue::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(),
            "0000-00-00"
        );
        assert_eq!(
            datetime_to_text(&MyValue::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(),
            "0000-00-00 00:00:00"
        );
    }

    /// A zero `TIMESTAMP` carries the SAME verbatim sentinel as a zero `DATETIME` — no `T`, no `Z`
    /// (PROTOCOL.md §3.2): it is not an instant and must not be parseable as one.
    #[test]
    fn zero_timestamp_is_the_verbatim_sentinel_not_an_instant() {
        assert_eq!(
            timestamptz_to_text(&MyValue::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(),
            "0000-00-00 00:00:00"
        );
    }

    /// A zero-in-date (`2026-00-05`, legal without `NO_ZERO_IN_DATE`) is carried verbatim too — the
    /// renderer never invents a calendar day.
    #[test]
    fn zero_in_date_components_are_carried_verbatim() {
        assert_eq!(
            date_to_text(&MyValue::Date(2026, 0, 5, 0, 0, 0, 0)).unwrap(),
            "2026-00-05"
        );
        assert_eq!(
            datetime_to_text(&MyValue::Date(2026, 8, 0, 1, 2, 3, 0)).unwrap(),
            "2026-08-00 01:02:03"
        );
    }

    /// Single-digit components are zero-padded and the year is always four digits.
    #[test]
    fn date_components_are_zero_padded() {
        assert_eq!(
            date_to_text(&MyValue::Date(7, 1, 2, 0, 0, 0, 0)).unwrap(),
            "0007-01-02"
        );
        assert_eq!(
            datetime_to_text(&MyValue::Date(9999, 12, 31, 23, 59, 59, 0)).unwrap(),
            "9999-12-31 23:59:59"
        );
    }

    // Hazard 26: TIME is (is_negative, days, hours, minutes, seconds, micros) and may exceed 24h.
    #[test]
    fn time_handles_sign_and_days_overflow() {
        assert_eq!(
            time_to_text(&MyValue::Time(false, 0, 13, 45, 7, 0)).unwrap(),
            "13:45:07"
        );
        assert_eq!(
            time_to_text(&MyValue::Time(true, 34, 22, 59, 58, 1)).unwrap(),
            "-838:59:58.000001"
        );
        assert_eq!(
            time_to_text(&MyValue::Time(false, 1, 2, 0, 0, 0)).unwrap(),
            "26:00:00"
        );
    }

    /// The documented MySQL `TIME` extremes both render; one µs past the maximum is a loud decode
    /// mismatch rather than a plausible-looking lie.
    #[test]
    fn time_renders_the_range_extremes_and_rejects_beyond() {
        assert_eq!(
            time_to_text(&MyValue::Time(false, 34, 22, 59, 59, 0)).unwrap(),
            "838:59:59"
        );
        assert_eq!(
            time_to_text(&MyValue::Time(true, 34, 22, 59, 59, 999_999)).unwrap(),
            "-838:59:59.999999"
        );
        assert!(matches!(
            time_to_text(&MyValue::Time(false, 35, 0, 0, 0, 0)),
            Err(PoolError::Backend(_))
        ));
        // A u32 `days` must reach the range check, never wrap through the µs multiplication.
        assert!(matches!(
            time_to_text(&MyValue::Time(false, u32::MAX, 23, 59, 59, 999_999)),
            Err(PoolError::Backend(_))
        ));
    }

    /// `-00:00:00` and `00:00:00` must not be two canonical payloads for one value (the server's own
    /// text form is unsigned), and the driver itself drops the sign on an all-zero `TIME`.
    #[test]
    fn negative_zero_time_renders_unsigned() {
        assert_eq!(
            time_to_text(&MyValue::Time(true, 0, 0, 0, 0, 0)).unwrap(),
            "00:00:00"
        );
    }

    /// A DATE cell never carries a time part; being handed one means the classifier and the renderer
    /// disagree, and silently dropping it would be the miscast charter rule 6 forbids.
    #[test]
    fn date_rejects_a_cell_that_carries_a_time_part() {
        assert!(matches!(
            date_to_text(&MyValue::Date(2026, 8, 5, 13, 45, 7, 0)),
            Err(PoolError::Backend(_))
        ));
    }

    /// Out-of-domain wall-clock components are corrupt input, not a value to render.
    #[test]
    fn out_of_domain_time_components_are_rejected() {
        for v in [
            MyValue::Date(2026, 8, 5, 24, 0, 0, 0),
            MyValue::Date(2026, 8, 5, 0, 60, 0, 0),
            MyValue::Date(2026, 8, 5, 0, 0, 60, 0),
            MyValue::Date(2026, 8, 5, 0, 0, 0, 1_000_000),
        ] {
            assert!(
                matches!(datetime_to_text(&v), Err(PoolError::Backend(_))),
                "{v:?} must be rejected"
            );
            assert!(matches!(
                timestamptz_to_text(&v),
                Err(PoolError::Backend(_))
            ));
        }
        assert!(matches!(
            time_to_text(&MyValue::Time(false, 0, 0, 60, 0, 0)),
            Err(PoolError::Backend(_))
        ));
    }

    /// Hazard 22: the server's ASCII rendering IS the canonical payload — the display scale survives
    /// byte-for-byte, so `1.10` and `1.1` stay distinct and nothing routes through a float.
    #[test]
    fn decimal_preserves_the_display_scale_verbatim() {
        for lit in [
            "1.10",
            "1.1",
            "-12345.6700",
            "0.00",
            "0",
            "+7",
            "12345678901234567890123456789012345.123456789012345678901234567890",
            "-0.000000000000000000000000000001",
        ] {
            assert_eq!(
                decimal_to_text(&MyValue::Bytes(lit.as_bytes().to_vec())).unwrap(),
                lit,
                "decimal must be carried verbatim"
            );
        }
    }

    /// A non-decimal payload routed here is a loud decode mismatch, never a coerced number.
    #[test]
    fn decimal_rejects_a_non_decimal_rendering() {
        for bad in ["", "-", ".", "1.2.3", "1e5", "NaN", "12 ", "1,5"] {
            assert!(
                matches!(
                    decimal_to_text(&MyValue::Bytes(bad.as_bytes().to_vec())),
                    Err(PoolError::Backend(_))
                ),
                "{bad:?} must be rejected"
            );
        }
        // A DECIMAL never arrives as a driver numeric — that would be the lossy path.
        assert!(matches!(
            decimal_to_text(&MyValue::Double(1.1)),
            Err(PoolError::Backend(_))
        ));
    }

    /// JSON is the raw document text, moved verbatim: not re-serialized, not re-indented, not
    /// validated beyond UTF-8.
    #[test]
    fn json_is_the_raw_document_text() {
        let doc = r#"{"b": 1, "a": [1, 2, {"n": null}], "u": "héllo"}"#;
        assert_eq!(
            json_to_text(&MyValue::Bytes(doc.as_bytes().to_vec())).unwrap(),
            doc
        );
        // Invalid UTF-8 cannot ride the msgpack `str` family — a loud decode mismatch, not a panic.
        assert!(matches!(
            json_to_text(&MyValue::Bytes(vec![0xff, 0xfe])),
            Err(PoolError::Backend(_))
        ));
    }

    // A wrong-variant cell is a decode mismatch -> Backend, never ConnectionLost (§9.1).
    #[test]
    fn wrong_variant_is_a_backend_error() {
        assert!(matches!(
            date_to_text(&MyValue::Int(1)),
            Err(PoolError::Backend(_))
        ));
    }

    /// Every renderer rejects every variant it does not own — and no renderer panics on any of them.
    #[test]
    fn every_renderer_rejects_every_foreign_variant() {
        let foreign = [
            MyValue::NULL,
            MyValue::Int(1),
            MyValue::UInt(1),
            MyValue::Float(1.0),
            MyValue::Double(1.0),
            MyValue::Time(false, 0, 1, 0, 0, 0),
            MyValue::Bytes(b"2026-08-05".to_vec()),
        ];
        for v in &foreign {
            assert!(matches!(date_to_text(v), Err(PoolError::Backend(_))));
            assert!(matches!(datetime_to_text(v), Err(PoolError::Backend(_))));
            assert!(matches!(timestamptz_to_text(v), Err(PoolError::Backend(_))));
        }
        for v in [
            MyValue::NULL,
            MyValue::Int(1),
            MyValue::Date(2026, 8, 5, 0, 0, 0, 0),
        ] {
            assert!(matches!(time_to_text(&v), Err(PoolError::Backend(_))));
            assert!(matches!(json_to_text(&v), Err(PoolError::Backend(_))));
            assert!(matches!(decimal_to_text(&v), Err(PoolError::Backend(_))));
        }
    }

    /// SPEC §12: an error message names the driver variant, never the cell's contents.
    #[test]
    fn error_messages_never_leak_cell_contents() {
        let secret = "s3cr3t-value";
        let err = json_to_text(&MyValue::Time(false, 0, 1, 0, 0, 0)).unwrap_err();
        let PoolError::Backend(msg) = err else {
            panic!("expected Backend");
        };
        assert!(msg.contains("Time"), "names the variant: {msg}");
        let err = date_to_text(&MyValue::Bytes(secret.as_bytes().to_vec())).unwrap_err();
        let PoolError::Backend(msg) = err else {
            panic!("expected Backend");
        };
        assert!(!msg.contains(secret), "must not echo cell contents: {msg}");
    }
}
