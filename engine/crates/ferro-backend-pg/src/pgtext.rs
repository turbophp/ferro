//! PG binary payload → **canonical text** decoders (M1-S7, `proto/PROTOCOL.md` §3.2).
//!
//! Every function here is pure byte math over the **raw binary payload** of one PG column value:
//! `fn(&[u8]) -> Result<String, PoolError>`. The result is the canonical wire text that
//! `ferro-proto`'s `Value::{Decimal,Date,Time,Timestamp,TimestampTz,Uuid,Json}` carries as a
//! msgpack `str` — so the rendering decision lives where the source format is known (the backend),
//! never in the codec (PROTOCOL.md §3.2).
//!
//! **Result format is BINARY** and is not per-statement selectable (the vendored fork hardcodes
//! `Some(1)`), so "just ask PG for text" is not an option — these decoders are the only way to get
//! canonical text out of the wire.
//!
//! **No new dependency.** `postgres-types` has no `NUMERIC` `FromSql` under any feature and
//! `postgres-protocol` contains no NUMERIC code at all, so there is nothing to crib; date/time/uuid
//! need only big-endian integer reads. Deliberately hand-rolled instead of routed through
//! `rust_decimal`/`chrono`, both of which are *lossy* here:
//! - `rust_decimal`'s 96-bit mantissa cannot hold PG's 131 072 integral digits, cannot represent
//!   `NaN`, and normalizes away the **display scale** (`1.10` → `1.1`), which are distinct
//!   canonical payloads;
//! - `chrono`'s `NaiveTime` arithmetic **wraps** `24:00:00` (a legal PG `time`) to `00:00:00`.
//!
//! Rules this module exists to keep (each has a unit test):
//! - the PG epoch is **2000-01-01**, not 1970-01-01 (a Unix-epoch assumption yields a plausible
//!   wrong date, not a crash);
//! - ±infinity are **sentinel values** (`i32::MIN`/`MAX`, `i64::MIN`/`MAX`), never arithmetic, and
//!   render as the verbatim non-parseable payloads `"infinity"` / `"-infinity"`;
//! - NUMERIC is base-10000 with an explicit **display scale**: truncate at `dscale`, never round,
//!   and never normalize (`1.10` and `1.1` are distinct payloads);
//! - fractional seconds are emitted as **no group at all** when zero, otherwise **exactly six**
//!   digits — never trailing-zero-trimmed, so the payload is byte-stable for the golden vectors;
//! - a malformed payload is a `PoolError::Backend` (SPEC §9.1 decode-mismatch rule → NonRetryable),
//!   **never** a panic and never a `ConnectionLost` that could mint a false §19.3 `Indeterminate`.
//!
//! **Oracle caveat (F23).** The `num_bytes` test helper is written by this module's author, so a
//! shared misunderstanding of the NUMERIC layout would pass every test here. That is why the
//! zero / `weight < 0` / scale-padding cases go through the header-level `num_header` helper
//! instead, and why Task 4b re-verifies those same cases against **PG's own `::text`** in the same
//! query. Neither half is sufficient alone.
//!
//! **Where the raw bytes come from (hazard 16).** This module renders payloads; it never READS
//! them. Every renderer below takes a plain `&[u8]` and has no way to obtain one from a `Row`.
//! The `FromSql` that turns a column into raw bytes lives INSIDE `rowmap::extract_value`, on the
//! far side of the OID gate, as a function-local item — so nothing in this file (or anywhere else
//! in the crate) can name it, and adding a `pub(crate) fn raw_slice(row, idx)` here does not
//! compile. See `rowmap`'s `the_only_raw_from_sql_is_inside_the_oid_gate` for the mechanical lock.

use ferro_pool::error::PoolError;

/// `numeric_send` sign words (PG `src/backend/utils/adt/numeric.c`; ±Inf are PG14+, testkit is
/// `postgres:17`). Anything else in the sign field is a malformed payload.
const SIGN_POS: u16 = 0x0000;
const SIGN_NEG: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000;
const SIGN_PINF: u16 = 0xD000;
const SIGN_NINF: u16 = 0xF000;

/// PG masks the wire display scale with `NUMERIC_DSCALE_MASK`, so a larger value is malformed
/// (`numeric_recv` rejects it with "invalid scale in external \"numeric\" value").
const MAX_DSCALE: u16 = 0x3FFF;

/// The only `JB_HEADER` version PG has ever sent for a `jsonb` binary payload.
const JSONB_VERSION: u8 = 1;

/// Days from 1970-01-01 (the civil algorithm's anchor) to 2000-01-01 (the PG epoch).
const PG_EPOCH_UNIX_DAYS: i64 = 10_957;

const US_PER_DAY: i64 = 86_400_000_000;
const US_PER_HOUR: i64 = 3_600_000_000;
const US_PER_MINUTE: i64 = 60_000_000;
const US_PER_SECOND: i64 = 1_000_000;

/// PG `numeric` → canonical decimal text, full precision, **display scale preserved**.
///
/// Wire layout (all big-endian, source of truth `numeric_send`):
/// ```text
/// i16 ndigits   number of base-10000 groups that FOLLOW
/// i16 weight    base-10000 exponent of the FIRST group (0 => the 1s..9999s place; NEGATIVE =>
///               leading all-zero groups were SKIPPED and must be re-emitted as "0000" runs)
/// u16 sign      0x0000 pos | 0x4000 neg | 0xC000 NaN | 0xD000 +Inf | 0xF000 -Inf
/// u16 dscale    DISPLAY scale: how many fractional digits to render. TRUNCATE at it, never round.
/// then ndigits x i16, each 0..=9999
/// ```
/// The group at index `i` has base-10000 exponent `weight - i`, which is why the fractional part
/// starts at index `weight + 1` and why a negative `weight` starts it at a *negative* index whose
/// (absent) groups render as `"0000"`.
pub fn numeric_to_text(raw: &[u8]) -> Result<String, PoolError> {
    if raw.len() < 8 {
        return Err(backend(format!(
            "numeric: payload is {} bytes, shorter than the 8-byte header",
            raw.len()
        )));
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
    let weight = i16::from_be_bytes([raw[2], raw[3]]);
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]);

    // The specials ignore every other header field and render verbatim.
    match sign {
        SIGN_NAN => return Ok("NaN".to_string()),
        SIGN_PINF => return Ok("Infinity".to_string()),
        SIGN_NINF => return Ok("-Infinity".to_string()),
        SIGN_POS | SIGN_NEG => {}
        other => return Err(backend(format!("numeric: unknown sign word {other:#06x}"))),
    }

    if ndigits < 0 {
        return Err(backend(format!("numeric: negative ndigits {ndigits}")));
    }
    let n = usize::from(ndigits.unsigned_abs());
    let want = 8 + 2 * n;
    if raw.len() != want {
        return Err(backend(format!(
            "numeric: ndigits {ndigits} implies a {want}-byte payload, got {}",
            raw.len()
        )));
    }
    if dscale > MAX_DSCALE {
        return Err(backend(format!(
            "numeric: display scale {dscale} exceeds PG's maximum {MAX_DSCALE}"
        )));
    }

    let mut digits: Vec<i16> = Vec::with_capacity(n);
    for (i, c) in raw[8..].chunks_exact(2).enumerate() {
        let d = i16::from_be_bytes([c[0], c[1]]);
        if !(0..=9999).contains(&d) {
            return Err(backend(format!(
                "numeric: base-10000 digit {d} at index {i} is outside 0..=9999"
            )));
        }
        digits.push(d);
    }

    let dscale = usize::from(dscale);
    let mut out = String::with_capacity(4 * (n + 1) + dscale + 2);
    if sign == SIGN_NEG {
        out.push('-');
    }

    // Integer part. A negative weight means every integral group was skipped => a bare "0".
    if weight < 0 {
        out.push('0');
    } else {
        let groups = usize::from(weight.unsigned_abs()) + 1;
        for i in 0..groups {
            // Groups past the wire's last digit are implied zeros ("10000" from digits [1]).
            let dig = digits.get(i).copied().unwrap_or(0);
            push_group(&mut out, dig, 4, i == 0);
        }
    }

    // Fractional part: `dscale` digits exactly — zero-padded when the wire ran out of groups,
    // TRUNCATED (never rounded) when a group carries more digits than the scale allows.
    if dscale > 0 {
        out.push('.');
        let mut idx = i64::from(weight) + 1;
        let mut emitted = 0usize;
        while emitted < dscale {
            let dig = usize::try_from(idx)
                .ok()
                .and_then(|k| digits.get(k))
                .copied()
                .unwrap_or(0);
            push_group(&mut out, dig, (dscale - emitted).min(4), false);
            emitted += 4;
            idx += 1;
        }
    }
    Ok(out)
}

/// PG `date` (i32 days from the 2000-01-01 epoch) → `"YYYY-MM-DD"`, or the verbatim
/// `"infinity"` / `"-infinity"` sentinels.
pub fn date_to_text(raw: &[u8]) -> Result<String, PoolError> {
    let days = read_i32(raw, "date")?;
    if days == i32::MAX {
        return Ok("infinity".to_string());
    }
    if days == i32::MIN {
        return Ok("-infinity".to_string());
    }
    let (y, m, d) = civil_from_pg_days(i64::from(days));
    canonical_date(y, m, d, "date")
}

/// PG `time` (i64 µs since midnight) → `"HH:MM:SS"` / `"HH:MM:SS.ffffff"`.
///
/// **Does not wrap at midnight:** PG's `time '24:00:00'` is legal (86 400 000 000 µs) and renders
/// as `"24:00:00"`, so the hour field may exceed 23.
pub fn time_to_text(raw: &[u8]) -> Result<String, PoolError> {
    let us = read_i64(raw, "time")?;
    if us < 0 {
        return Err(backend(format!(
            "time: negative time-of-day ({us} µs) is not a legal PG `time` payload"
        )));
    }
    // Upper bound mirrors the negative guard above: PG's `time` domain is 0..=86_400_000_000 µs
    // inclusive (24:00:00 IS legal). Only a corrupt payload can exceed it, and without this an
    // `i64::MAX` would render the implausible-but-silent "2562047788:00:54.775807" instead of
    // failing — the loud-rejection posture this module exists to hold.
    if us > US_PER_DAY {
        return Err(backend(format!(
            "time: time-of-day ({us} µs) exceeds the legal PG `time` maximum of {US_PER_DAY} µs"
        )));
    }
    Ok(fmt_time_of_day(us))
}

/// PG `timestamp` (i64 µs from the 2000-01-01 epoch) → **naive** `"YYYY-MM-DD HH:MM:SS[.ffffff]"`,
/// no zone suffix ever, or the verbatim `"infinity"` / `"-infinity"` sentinels.
pub fn timestamp_to_text(raw: &[u8]) -> Result<String, PoolError> {
    let us = read_i64(raw, "timestamp")?;
    if let Some(sentinel) = timestamp_sentinel(us) {
        return Ok(sentinel.to_string());
    }
    let (date, time) = split_timestamp(us, "timestamp")?;
    Ok(format!("{date} {time}"))
}

/// PG `timestamptz` → RFC3339 `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"`.
///
/// The payload is **byte-identical** to `timestamp`'s; only the column OID separates naive-local
/// from UTC-instant, which is why this is a separate function rather than a flag on one. PG stores
/// and sends `timestamptz` as a UTC instant, so no zone math is needed — the `Z` is truthful and
/// the session `TimeZone` GUC is irrelevant to the binary payload.
pub fn timestamptz_to_text(raw: &[u8]) -> Result<String, PoolError> {
    let us = read_i64(raw, "timestamptz")?;
    if let Some(sentinel) = timestamp_sentinel(us) {
        return Ok(sentinel.to_string());
    }
    let (date, time) = split_timestamp(us, "timestamptz")?;
    Ok(format!("{date}T{time}Z"))
}

/// PG `uuid` (16 raw bytes) → the canonical 36-char **lowercase** hyphenated form.
pub fn uuid_to_text(raw: &[u8]) -> Result<String, PoolError> {
    if raw.len() != 16 {
        return Err(backend(format!(
            "uuid: expected a 16-byte payload, got {}",
            raw.len()
        )));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(36);
    for (i, b) in raw.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    Ok(out)
}

/// PG `json` / `jsonb` → the raw UTF-8 document text, **verbatim**.
///
/// A `jsonb` binary payload is one version byte followed by the document text; `json` has no
/// prefix. The engine does not re-serialize and does not validate the document beyond UTF-8 (which
/// the msgpack `str` family requires) — the client decodes lazily.
pub fn json_to_text(raw: &[u8], jsonb: bool) -> Result<String, PoolError> {
    let body = if jsonb {
        match raw.split_first() {
            Some((&JSONB_VERSION, rest)) => rest,
            Some((&v, _)) => {
                return Err(backend(format!(
                    "jsonb: unknown binary format version {v} (expected {JSONB_VERSION})"
                )));
            }
            None => {
                return Err(backend(
                    "jsonb: empty payload (no version byte)".to_string(),
                ));
            }
        }
    } else {
        raw
    };
    String::from_utf8(body.to_vec())
        .map_err(|e| backend(format!("json: payload is not valid UTF-8: {e}")))
}

/// PG's `"char"` (OID 18) is a SINGLE BYTE, not a string — `postgres-types` reads it as `i8`. Render
/// it the way PG's own `charout` does: `'\0'` is the EMPTY string (which is what `pg_attribute
/// .attidentity` holds for a non-identity column, and what DBAL's schema manager compares against),
/// any ASCII byte is that one character.
///
/// A non-ASCII byte has no canonical-text form (`PROTOCOL.md` §3.2 defines none) and inventing one
/// would differ between the two codecs — so it is a client-side decode mismatch:
/// `PoolError::Backend` (NonRetryable), **never** `ConnectionLost`, so it can never mint a false
/// §19.3 `Indeterminate` (SPEC §9.1).
pub(crate) fn char_byte_to_text(b: u8) -> Result<String, PoolError> {
    match b {
        0 => Ok(String::new()),
        0x01..=0x7f => Ok((b as char).to_string()),
        _ => Err(backend(format!(
            "PG \"char\" byte 0x{b:02x} is not ASCII and has no canonical text form"
        ))),
    }
}

/// Emits the first `take` (1..=4) decimal characters of one base-10000 group. With
/// `strip_leading`, leading zeros are suppressed but the **last** character is always emitted —
/// PG's own first-group rule, which is what makes a zero `numeric` render `"0"` and not `""`.
fn push_group(out: &mut String, dig: i16, take: usize, strip_leading: bool) {
    // Both invariants are established by `numeric_to_text` before any call: every wire digit is
    // range-checked to 0..=9999 (a wider one would emit 5 chars and desync the group alignment),
    // and `take` is either 4 or the `dscale` remainder.
    debug_assert!((0..=9999).contains(&dig), "base-10000 digit out of range");
    debug_assert!((1..=4).contains(&take), "group width out of range");
    let chars = [
        b'0' + (dig / 1000).unsigned_abs() as u8,
        b'0' + ((dig / 100) % 10).unsigned_abs() as u8,
        b'0' + ((dig / 10) % 10).unsigned_abs() as u8,
        b'0' + (dig % 10).unsigned_abs() as u8,
    ];
    let mut started = !strip_leading;
    for (i, c) in chars.iter().take(take).enumerate() {
        let last = i + 1 == take;
        if !started && *c == b'0' && !last {
            continue;
        }
        started = true;
        out.push(char::from(*c));
    }
}

/// `i64::MAX` / `i64::MIN` are the `timestamp`/`timestamptz` ±infinity sentinels — values, not
/// arithmetic. They must be caught BEFORE any division.
fn timestamp_sentinel(us: i64) -> Option<&'static str> {
    match us {
        i64::MAX => Some("infinity"),
        i64::MIN => Some("-infinity"),
        _ => None,
    }
}

/// Splits µs-from-the-PG-epoch into a canonical date and time-of-day. **Floor** division, so a
/// pre-2000 (negative) timestamp lands on the previous day rather than truncating toward zero.
fn split_timestamp(us: i64, what: &str) -> Result<(String, String), PoolError> {
    let days = us.div_euclid(US_PER_DAY);
    let tod = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_pg_days(days);
    Ok((canonical_date(y, m, d, what)?, fmt_time_of_day(tod)))
}

/// µs → `"HH:MM:SS"` or `"HH:MM:SS.ffffff"`. `us` must be non-negative; the hour field is NOT
/// reduced mod 24 (PG `time '24:00:00'`).
fn fmt_time_of_day(us: i64) -> String {
    let h = us / US_PER_HOUR;
    let mi = (us % US_PER_HOUR) / US_PER_MINUTE;
    let s = (us % US_PER_MINUTE) / US_PER_SECOND;
    let frac = us % US_PER_SECOND;
    if frac == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{frac:06}")
    }
}

/// Days from the PG epoch → `(year, month, day)` in the proleptic Gregorian calendar.
///
/// Howard Hinnant's `civil_from_days`, re-anchored from 1970-01-01 onto PG's **2000-01-01**. All
/// intermediate values are non-negative after the `rem_euclid`, so every division floors.
fn civil_from_pg_days(pg_days: i64) -> (i64, i64, i64) {
    let z = pg_days + PG_EPOCH_UNIX_DAYS + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Renders `YYYY-MM-DD`, or refuses **loudly**.
///
/// PG's `date`/`timestamp` range (4713 BC .. 5874897 AD) is wider than the canonical text form,
/// which PROTOCOL.md §3.2 pins as exactly `YYYY-MM-DD`. Rather than invent a payload the contract
/// does not define (a `" BC"` suffix, an ambiguous negative year, a 7-digit year), a value outside
/// `0001-01-01..9999-12-31` is a loud `Unsupported` naming the year — charter rule 6, no silent
/// miscast. Nothing in the DBAL/S9 surface produces one.
fn canonical_date(y: i64, m: i64, d: i64, what: &str) -> Result<String, PoolError> {
    if !(1..=9999).contains(&y) {
        return Err(PoolError::Unsupported(format!(
            "{what}: proleptic year {y} has no canonical YYYY-MM-DD form (PROTOCOL.md §3.2); \
             values outside 0001-01-01..9999-12-31 (incl. BC dates) are unsupported in M1-S7"
        )));
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

fn read_i32(raw: &[u8], what: &str) -> Result<i32, PoolError> {
    let b: [u8; 4] = raw.try_into().map_err(|_| {
        backend(format!(
            "{what}: expected a 4-byte payload, got {}",
            raw.len()
        ))
    })?;
    Ok(i32::from_be_bytes(b))
}

fn read_i64(raw: &[u8], what: &str) -> Result<i64, PoolError> {
    let b: [u8; 8] = raw.try_into().map_err(|_| {
        backend(format!(
            "{what}: expected an 8-byte payload, got {}",
            raw.len()
        ))
    })?;
    Ok(i64::from_be_bytes(b))
}

/// A decode failure here is a client-side **decode mismatch** (SPEC §9.1), i.e. `Backend`
/// (NonRetryable) — never `ConnectionLost`, which would mint a false §19.3 `Indeterminate`.
fn backend(msg: String) -> PoolError {
    PoolError::Backend(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The *layout* oracle: builds a NUMERIC wire payload from the header fields directly, so a
    /// case can pin a wire shape (`ndigits == 0`, `weight < 0`) that no decimal literal spells out.
    fn num_header(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut v = Vec::with_capacity(8 + digits.len() * 2);
        v.extend_from_slice(&ndigits.to_be_bytes());
        v.extend_from_slice(&weight.to_be_bytes());
        v.extend_from_slice(&sign.to_be_bytes());
        v.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// The *ergonomic* oracle: builds the same payload from a decimal literal, mirroring PG's
    /// `set_var_from_str` + `strip_var` (group on 4-digit boundaries **around the decimal point**,
    /// then drop leading and trailing all-zero groups). Deliberately written on top of
    /// `num_header`, and deliberately NOT the only oracle — see the F23 note in the module docs.
    fn num_bytes(lit: &str) -> Vec<u8> {
        match lit {
            "NaN" => return num_header(0, 0, 0xC000, 0, &[]),
            "Infinity" => return num_header(0, 0, 0xD000, 0, &[]),
            "-Infinity" => return num_header(0, 0, 0xF000, 0, &[]),
            _ => {}
        }
        let (sign, body) = match lit.strip_prefix('-') {
            Some(rest) => (0x4000u16, rest),
            None => (0x0000u16, lit),
        };
        let (int_part, frac_part) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        let dscale = u16::try_from(frac_part.len()).expect("test dscale fits u16");
        // Integer digits pad on the LEFT, fractional digits on the RIGHT — the grouping is
        // anchored at the decimal point, which is what makes `weight` meaningful.
        let ip = format!("{}{int_part}", "0".repeat((4 - int_part.len() % 4) % 4));
        let fp = format!("{frac_part}{}", "0".repeat((4 - frac_part.len() % 4) % 4));
        let mut digits: Vec<i16> = ip
            .as_bytes()
            .chunks(4)
            .chain(fp.as_bytes().chunks(4))
            .map(|c| {
                std::str::from_utf8(c)
                    .expect("ascii")
                    .parse::<i16>()
                    .expect("4-digit group")
            })
            .collect();
        let mut weight = i32::try_from(ip.len() / 4).expect("test weight fits i32") - 1;
        let mut first = 0;
        while first < digits.len() && digits[first] == 0 {
            first += 1;
            weight -= 1;
        }
        digits.drain(..first);
        while digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() {
            weight = 0;
        }
        let ndigits = i16::try_from(digits.len()).expect("test ndigits fits i16");
        num_header(
            ndigits,
            i16::try_from(weight).expect("test weight fits i16"),
            sign,
            dscale,
            &digits,
        )
    }

    fn hex(s: &str) -> Vec<u8> {
        assert!(
            s.len().is_multiple_of(2),
            "hex capture must be byte-aligned"
        );
        s.as_bytes()
            .chunks(2)
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).expect("ascii"), 16).expect("hex"))
            .collect()
    }

    /// **The independent oracle (F23).** Every payload below was captured from a live
    /// `postgres:17` (the testkit container) with `encode(<type>_send(<literal>), 'hex')`, so the
    /// wire layout comes from PG itself, not from this file's author. Expectations are PG's own
    /// `::text` rendering, except where PROTOCOL.md §3.2 deliberately differs (fractional seconds
    /// are always **exactly six** digits, where PG trims: PG says `13:45:07.25`).
    ///
    /// The capture session ran `SET TIME ZONE 'America/New_York'`, which proves the
    /// `timestamptz` payload is the UTC instant and zone-independent: `13:45:07.25+02` came back
    /// as a *different* payload from the naive `2026-08-05 13:45:07.25` and equals PG's own
    /// `AT TIME ZONE 'UTC'` rendering, `2026-08-05 11:45:07.25`.
    #[test]
    fn live_pg_send_captures_decode_to_pg_canonical_text() {
        // numeric: 1.10 / 1.1 (distinct payloads, distinct dscale), zero at scale 4,
        // 1e-5 (weight -2), numeric(30,10) of -12345.67 (weight 1), and the specials.
        assert_eq!(
            numeric_to_text(&hex("0002000000000002000103e8")).unwrap(),
            "1.10"
        );
        assert_eq!(
            numeric_to_text(&hex("0002000000000001000103e8")).unwrap(),
            "1.1"
        );
        assert_eq!(numeric_to_text(&hex("0000000000000000")).unwrap(), "0");
        assert_eq!(
            numeric_to_text(&hex("0000000000000004")).unwrap(),
            "0.0000",
            "0::numeric(10,4)"
        );
        assert_eq!(
            numeric_to_text(&hex("0001fffe0000000503e8")).unwrap(),
            "0.00001"
        );
        assert_eq!(
            numeric_to_text(&hex("000300014000000a000109291a2c")).unwrap(),
            "-12345.6700000000",
            "-12345.67::numeric(30,10) — PG sends weight=1, ndigits=3, digits [1,2345,6700]"
        );
        assert_eq!(
            numeric_to_text(&hex("00010001000000000001")).unwrap(),
            "10000"
        );
        assert_eq!(numeric_to_text(&hex("0001000000000000000c")).unwrap(), "12");
        assert_eq!(numeric_to_text(&hex("00000000c0000000")).unwrap(), "NaN");
        // NOTE the real ±Infinity payloads carry dscale = 0x0020 (32). What that actually makes
        // load-bearing is that the special-sign match precedes *rendering*: reach the digit loop
        // with this header and `Infinity` renders as "0." followed by 32 zeros. (It need NOT
        // precede dscale *validation* — 32 is well under MAX_DSCALE, so reordering against the
        // validation alone leaves the suite green. Verified by mutation in the T4a review.)
        assert_eq!(
            numeric_to_text(&hex("00000000d0000020")).unwrap(),
            "Infinity"
        );
        assert_eq!(
            numeric_to_text(&hex("00000000f0000020")).unwrap(),
            "-Infinity"
        );

        // date / time / timestamp / timestamptz
        assert_eq!(date_to_text(&hex("000025f1")).unwrap(), "2026-08-05");
        assert_eq!(date_to_text(&hex("0000003b")).unwrap(), "2000-02-29");
        assert_eq!(date_to_text(&hex("00008ee8")).unwrap(), "2100-03-01");
        assert_eq!(date_to_text(&hex("ffffd533")).unwrap(), "1970-01-01");
        assert_eq!(date_to_text(&hex("7fffffff")).unwrap(), "infinity");
        assert_eq!(date_to_text(&hex("80000000")).unwrap(), "-infinity");
        assert_eq!(time_to_text(&hex("000000141dd76000")).unwrap(), "24:00:00");
        assert_eq!(
            time_to_text(&hex("0000000b86dcaf50")).unwrap(),
            "13:45:07.250000",
            "PG's own text trims to .25; the canonical form is exactly six digits"
        );
        assert_eq!(
            time_to_text(&hex("000000141dd75fff")).unwrap(),
            "23:59:59.999999"
        );
        assert_eq!(
            timestamp_to_text(&hex("ffffffffffffffff")).unwrap(),
            "1999-12-31 23:59:59.999999",
            "-1 µs is the day BEFORE the epoch — floor division, not truncation"
        );
        assert_eq!(
            timestamp_to_text(&hex("0002fb4bbf7e0f50")).unwrap(),
            "2026-08-05 13:45:07.250000"
        );
        assert_eq!(
            timestamptz_to_text(&hex("0002fb4a1256c750")).unwrap(),
            "2026-08-05T11:45:07.250000Z",
            "'2026-08-05 13:45:07.25+02' as a UTC instant"
        );

        // uuid / json / jsonb
        assert_eq!(
            uuid_to_text(&hex("3f2b8c1a00004fff8000abcdefabcdef")).unwrap(),
            "3f2b8c1a-0000-4fff-8000-abcdefabcdef"
        );
        assert_eq!(
            json_to_text(&hex("7b2261223a317d"), false).unwrap(),
            r#"{"a":1}"#
        );
        assert_eq!(
            json_to_text(&hex("017b2261223a20317d"), true).unwrap(),
            r#"{"a": 1}"#,
            "jsonb is PG-normalized (a space after the colon) and passes through verbatim"
        );
    }

    /// The ergonomic oracle is only trustworthy if it agrees with PG's real encoder. These are the
    /// same captures as above, asserted against `num_bytes` — so a shared misunderstanding of the
    /// layout between helper and decoder cannot hide (F23).
    #[test]
    fn the_num_bytes_helper_matches_real_pg_encoded_payloads() {
        for (lit, capture) in [
            ("1.10", "0002000000000002000103e8"),
            ("1.1", "0002000000000001000103e8"),
            ("0", "0000000000000000"),
            ("0.00001", "0001fffe0000000503e8"),
            ("-12345.6700000000", "000300014000000a000109291a2c"),
            ("10000", "00010001000000000001"),
            ("12", "0001000000000000000c"),
            ("NaN", "00000000c0000000"),
        ] {
            assert_eq!(num_bytes(lit), hex(capture), "num_bytes({lit})");
        }
    }

    // NUMERIC is base-10000 with an explicit display scale. 1.10 and 1.1 are DISTINCT.
    #[test]
    fn numeric_preserves_display_scale() {
        assert_eq!(numeric_to_text(&num_bytes("1.10")).unwrap(), "1.10");
        assert_eq!(numeric_to_text(&num_bytes("1.1")).unwrap(), "1.1");
    }

    #[test]
    fn numeric_handles_special_values_and_huge_precision() {
        assert_eq!(numeric_to_text(&num_bytes("NaN")).unwrap(), "NaN");
        assert_eq!(numeric_to_text(&num_bytes("Infinity")).unwrap(), "Infinity");
        assert_eq!(
            numeric_to_text(&num_bytes("-Infinity")).unwrap(),
            "-Infinity"
        );
        let big = format!("{}.{}", "9".repeat(200), "1".repeat(50));
        assert_eq!(
            numeric_to_text(&num_bytes(&big)).unwrap(),
            big,
            "no precision loss"
        );
    }

    // ZERO: ndigits == 0 is legal and a naive digit loop emits "" (F23).
    #[test]
    fn numeric_zero_renders_at_its_declared_scale() {
        assert_eq!(numeric_to_text(&num_bytes("0")).unwrap(), "0");
        // dscale 4 with no digits at all -> the scale must still be honoured.
        assert_eq!(
            numeric_to_text(&num_header(0, 0, 0x0000, 4, &[])).unwrap(),
            "0.0000"
        );
    }

    // weight < 0: the leading base-10000 groups are SKIPPED on the wire and must be re-emitted as
    // "0000" runs, or 1e-5 renders as "0.1" instead of "0.00001" (F23).
    #[test]
    fn numeric_reemits_skipped_leading_zero_groups() {
        // 0.00001 == digits [1000] at weight -2, dscale 5.
        assert_eq!(
            numeric_to_text(&num_header(1, -2, 0x0000, 5, &[1000])).unwrap(),
            "0.00001"
        );
        assert_eq!(numeric_to_text(&num_bytes("0.00001")).unwrap(), "0.00001");
    }

    // A numeric(30,10) holding -12345.67 must ZERO-PAD the 4 available fractional digits to 10.
    //
    // PLAN DEVIATION (documented in the task report): the plan's literal was
    // `num_header(3, 0, …, &[1, 2345, 6700])`, i.e. weight 0. Under the layout the plan itself
    // states ("weight = the base-10000 exponent of the FIRST group; 0 => the 1s..9999s place")
    // that payload is **1**.2345670000, not -12345.67 — the first group `1` would sit in the ones
    // place. -12345.67 is `1 * 10000^1 + 2345 * 10000^0 + 6700 * 10000^-1`, so weight is **1**.
    // The expected TEXT is the plan's, unchanged; only the (self-inconsistent) input is corrected.
    #[test]
    fn numeric_pads_out_to_the_declared_scale() {
        assert_eq!(
            numeric_to_text(&num_header(3, 1, 0x4000, 10, &[1, 2345, 6700])).unwrap(),
            "-12345.6700000000"
        );
        // ... and the literal oracle agrees on the same value, independently of the header math.
        assert_eq!(
            numeric_to_text(&num_bytes("-12345.6700000000")).unwrap(),
            "-12345.6700000000"
        );
    }

    // dscale TRUNCATES, never rounds (matches PG's own ::text rendering).
    #[test]
    fn numeric_truncates_at_dscale_never_rounds() {
        assert_eq!(
            numeric_to_text(&num_header(2, 0, 0x0000, 1, &[1, 9900])).unwrap(),
            "1.9"
        );
    }

    // A group ABOVE the wire's last digit is an implied zero group ("10000" from one digit).
    #[test]
    fn numeric_emits_implied_trailing_integer_groups() {
        assert_eq!(
            numeric_to_text(&num_header(1, 1, 0x0000, 0, &[1])).unwrap(),
            "10000"
        );
        // The FIRST group suppresses its own leading zeros ("12", not "0012").
        assert_eq!(
            numeric_to_text(&num_header(1, 0, 0x0000, 0, &[12])).unwrap(),
            "12"
        );
    }

    // A malformed NUMERIC header is a Backend error, never a panic or a corrupt rendering.
    #[test]
    fn numeric_rejects_malformed_headers() {
        assert!(matches!(
            numeric_to_text(&[0u8; 7]),
            Err(PoolError::Backend(_))
        ));
        // sign word outside the five legal values
        assert!(matches!(
            numeric_to_text(&num_header(0, 0, 0x1234, 0, &[])),
            Err(PoolError::Backend(_))
        ));
        // ndigits disagrees with the payload length
        assert!(matches!(
            numeric_to_text(&num_header(3, 0, 0x0000, 0, &[1])),
            Err(PoolError::Backend(_))
        ));
        // a digit outside 0..=9999 would corrupt the 4-char group alignment
        assert!(matches!(
            numeric_to_text(&num_header(1, 0, 0x0000, 0, &[10_000])),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            numeric_to_text(&num_header(1, 0, 0x0000, 0, &[-1])),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            numeric_to_text(&num_header(-1, 0, 0x0000, 0, &[])),
            Err(PoolError::Backend(_))
        ));
    }

    // The PG epoch is 2000-01-01, NOT 1970-01-01 (hazard 12).
    #[test]
    fn date_uses_the_postgres_epoch() {
        assert_eq!(date_to_text(&0i32.to_be_bytes()).unwrap(), "2000-01-01");
        assert_eq!(
            date_to_text(&(-10957i32).to_be_bytes()).unwrap(),
            "1970-01-01"
        );
    }

    // Civil-calendar arithmetic: the 400-year rule (2000 IS a leap year, 2100 is NOT).
    // Expectations produced with an INDEPENDENT oracle (`date -u -d "2000-01-01 + N days"`).
    #[test]
    fn date_honours_the_leap_year_rules() {
        assert_eq!(date_to_text(&59i32.to_be_bytes()).unwrap(), "2000-02-29");
        assert_eq!(date_to_text(&(-1i32).to_be_bytes()).unwrap(), "1999-12-31");
        assert_eq!(date_to_text(&36525i32.to_be_bytes()).unwrap(), "2100-01-01");
        assert_eq!(date_to_text(&36584i32.to_be_bytes()).unwrap(), "2100-03-01");
    }

    // Infinity sentinels are values, not arithmetic (hazard 13).
    #[test]
    fn date_and_timestamp_infinities_are_explicit() {
        assert_eq!(date_to_text(&i32::MAX.to_be_bytes()).unwrap(), "infinity");
        assert_eq!(date_to_text(&i32::MIN.to_be_bytes()).unwrap(), "-infinity");
        assert_eq!(
            timestamp_to_text(&i64::MAX.to_be_bytes()).unwrap(),
            "infinity"
        );
        assert_eq!(
            timestamptz_to_text(&i64::MIN.to_be_bytes()).unwrap(),
            "-infinity"
        );
        // both renderings, both signs — the sentinel check is per-function, not per-sign.
        assert_eq!(
            timestamp_to_text(&i64::MIN.to_be_bytes()).unwrap(),
            "-infinity"
        );
        assert_eq!(
            timestamptz_to_text(&i64::MAX.to_be_bytes()).unwrap(),
            "infinity"
        );
    }

    // PG time '24:00:00' is legal and must NOT wrap to 00:00:00 (hazard 14).
    #[test]
    fn time_does_not_wrap_at_midnight() {
        assert_eq!(
            time_to_text(&86_400_000_000i64.to_be_bytes()).unwrap(),
            "24:00:00"
        );
        assert_eq!(time_to_text(&0i64.to_be_bytes()).unwrap(), "00:00:00");
        assert_eq!(
            time_to_text(&1i64.to_be_bytes()).unwrap(),
            "00:00:00.000001"
        );
    }

    // Sub-second rule: absent when zero, EXACTLY six digits otherwise — never trimmed.
    #[test]
    fn time_fractional_seconds_are_all_or_exactly_six() {
        assert_eq!(
            time_to_text(&49_507_250_000i64.to_be_bytes()).unwrap(),
            "13:45:07.250000"
        );
        assert_eq!(
            time_to_text(&86_399_999_999i64.to_be_bytes()).unwrap(),
            "23:59:59.999999"
        );
        // a negative time-of-day is not a legal PG `time` payload
        assert!(matches!(
            time_to_text(&(-1i64).to_be_bytes()),
            Err(PoolError::Backend(_))
        ));
        // 24:00:00 exactly IS legal and must survive (the no-wrap rule above).
        assert_eq!(time_to_text(&US_PER_DAY.to_be_bytes()).unwrap(), "24:00:00");
        // ...but one microsecond past it is not. Without the upper guard this rendered the
        // implausible-but-silent "2562047788:00:54.775807" for i64::MAX (T4a review nit).
        assert!(matches!(
            time_to_text(&(US_PER_DAY + 1).to_be_bytes()),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            time_to_text(&i64::MAX.to_be_bytes()),
            Err(PoolError::Backend(_))
        ));
    }

    // TIMESTAMP is naive, TIMESTAMPTZ is UTC with a Z — same 8 bytes, different rendering.
    #[test]
    fn timestamp_and_timestamptz_render_differently_from_identical_bytes() {
        let b = 0i64.to_be_bytes();
        assert_eq!(timestamp_to_text(&b).unwrap(), "2000-01-01 00:00:00");
        assert_eq!(timestamptz_to_text(&b).unwrap(), "2000-01-01T00:00:00Z");
    }

    // Pre-epoch values need FLOOR division, not truncation, or -1 µs lands on 2000-01-01.
    #[test]
    fn timestamps_before_the_epoch_floor_correctly() {
        assert_eq!(
            timestamp_to_text(&(-1i64).to_be_bytes()).unwrap(),
            "1999-12-31 23:59:59.999999"
        );
        assert_eq!(
            timestamptz_to_text(&(-1i64).to_be_bytes()).unwrap(),
            "1999-12-31T23:59:59.999999Z"
        );
        assert_eq!(
            timestamp_to_text(&(86_400_000_000i64 + 250_000).to_be_bytes()).unwrap(),
            "2000-01-02 00:00:00.250000"
        );
    }

    // A year outside 0001..=9999 has no canonical `YYYY-MM-DD` form: loud Unsupported (charter
    // rule 6), never an invented date or an ambiguous 5-digit/negative year.
    #[test]
    fn dates_outside_the_canonical_year_range_are_loudly_unsupported() {
        // 4713-11-24 BC (PG's minimum date) is ~2 440 588 days before the 2000 epoch.
        assert!(matches!(
            date_to_text(&(-2_451_545i32).to_be_bytes()),
            Err(PoolError::Unsupported(_))
        ));
        // 5874897-12-31 (PG's maximum date) is well past year 9999.
        assert!(matches!(
            date_to_text(&2_145_000_000i32.to_be_bytes()),
            Err(PoolError::Unsupported(_))
        ));
        assert!(matches!(
            timestamp_to_text(&(-100_000_000_000_000_000i64).to_be_bytes()),
            Err(PoolError::Unsupported(_))
        ));
    }

    #[test]
    fn uuid_is_canonical_lowercase_hyphenated() {
        let raw: [u8; 16] = [
            0x3F, 0x2B, 0x8C, 0x1A, 0, 0, 0x4F, 0xFF, 0x80, 0, 0xAB, 0xCD, 0xEF, 0xAB, 0xCD, 0xEF,
        ];
        assert_eq!(
            uuid_to_text(&raw).unwrap(),
            "3f2b8c1a-0000-4fff-8000-abcdefabcdef"
        );
        assert_eq!(
            uuid_to_text(&[0u8; 16]).unwrap(),
            "00000000-0000-0000-0000-000000000000"
        );
        // exactly 16 bytes — a 17-byte payload is as wrong as a 15-byte one
        assert!(matches!(
            uuid_to_text(&[0u8; 17]),
            Err(PoolError::Backend(_))
        ));
    }

    // JSONB's binary payload is a 1-byte version prefix + the raw JSON text; JSON has no prefix.
    #[test]
    fn json_and_jsonb_both_yield_the_raw_document() {
        assert_eq!(json_to_text(br#"{"a":1}"#, false).unwrap(), r#"{"a":1}"#);
        assert_eq!(json_to_text(b"\x01{\"a\":1}", true).unwrap(), r#"{"a":1}"#);
        // the document is passed through VERBATIM (not re-serialized, not validated)
        assert_eq!(
            json_to_text(b"  [ 1,  2 ]\n", false).unwrap(),
            "  [ 1,  2 ]\n"
        );
        // non-UTF-8 cannot be a msgpack `str` payload
        assert!(matches!(
            json_to_text(&[0xffu8, 0xfe], false),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            json_to_text(b"", true),
            Err(PoolError::Backend(_))
        ));
    }

    // Malformed input is a Backend error (SPEC §9.1 decode-mismatch rule), never a panic.
    #[test]
    fn short_payloads_are_backend_errors_not_panics() {
        assert!(matches!(
            date_to_text(&[0u8; 3]),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            uuid_to_text(&[0u8; 15]),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            json_to_text(b"\x09{}", true),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            timestamp_to_text(&[0u8; 7]),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            timestamptz_to_text(&[0u8; 9]),
            Err(PoolError::Backend(_))
        ));
        assert!(matches!(
            time_to_text(&[0u8; 4]),
            Err(PoolError::Backend(_))
        ));
    }
}
