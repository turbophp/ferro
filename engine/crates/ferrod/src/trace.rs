//! **The caller's W3C trace context (M2-C4c-1, SPEC §13).**
//!
//! An EXEC may carry the application's `traceparent` header text (`ExecRequest::traceparent`,
//! `/proto/PROTOCOL.md` §8.1) so the engine's per-statement observability joins the trace the
//! application is already in. This module is the ONE place that text is interpreted. The codec
//! moves it as bytes and validates nothing — not even UTF-8: it is the one `str` field decoded
//! lossily, so a non-UTF-8 header arrives here carrying U+FFFD and is refused by the grammar below.
//!
//! # Malformed context is IGNORED, never refused
//!
//! W3C Trace Context says a vendor that cannot parse a `traceparent` does not use it and starts a
//! new trace (§3.2.2.3 for an invalid trace id, §3.2.4 for a header it cannot parse, §4.3's
//! processing model) — and an observability field must never be the reason a statement fails. A
//! bad value is therefore dropped and COUNTED (`ferro_traceparent_invalid_total`), so a broken
//! provider is visible to an operator without being visible to the statement.
//!
//! # What is accepted
//!
//! Exactly the W3C grammar (Trace Context Level 1, §3.2), with no leniency added:
//!
//! * `version-traceid-parentid-flags`, all LOWERCASE hex: 2, 32, 16 and 2 digits.
//! * Version `ff` is invalid.
//! * Version `00` must be EXACTLY 55 characters.
//! * A HIGHER version may be longer, provided the character after the first 55 is `-`. Only the
//!   first 55 are parsed, and of its flags only `sampled`: §3.2.4 says to parse the trace id, the
//!   parent id and "the sampled bit of flags", and that vendors "MUST NOT parse or assume anything
//!   about unknown fields for this version".
//! * An all-zero trace id or parent id is invalid.
//!
//! Uppercase hex is refused rather than normalised. The grammar says lowercase, and a producer
//! emitting uppercase is broken in a way its other consumers will also reject; accepting it here
//! would make Ferro the one place that hides the bug.

use std::sync::atomic::{AtomicU64, Ordering};

/// Requests whose `traceparent` was present but did not parse — dropped, never refused.
pub static INVALID: AtomicU64 = AtomicU64::new(0);

/// The length of a version-`00` header, and of the known prefix of any later version.
const V00_LEN: usize = 55;

/// A parsed W3C `traceparent`. Ids are kept as bytes, so nothing downstream can mistake the
/// header's raw text for a validated value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParent {
    /// The 16-byte trace id: the whole distributed trace.
    pub trace_id: [u8; 16],
    /// The 8-byte id of the caller's span, which becomes the PARENT of the engine's span.
    pub parent_id: [u8; 8],
    /// The trace flags byte. Bit 0 is `sampled`. A version-`00` header's other bits are kept as
    /// received; a higher version's are cleared, since §3.2.4 parses only its sampled bit.
    pub flags: u8,
}

impl TraceParent {
    /// Parse a `traceparent` header value. `None` for anything the W3C grammar does not accept.
    pub fn parse(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        if b.len() < V00_LEN {
            return None;
        }
        let version = hex_byte(&b[0..2])?;
        if version == 0xff {
            return None;
        }
        // Version 00 is exactly 55 characters. A later version may append fields, but only after
        // a `-` — anything else means the known prefix is not what it appears to be.
        if b.len() > V00_LEN && (version == 0 || b[V00_LEN] != b'-') {
            return None;
        }
        if b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        let mut trace_id = [0u8; 16];
        for (i, out) in trace_id.iter_mut().enumerate() {
            *out = hex_byte(&b[3 + 2 * i..5 + 2 * i])?;
        }
        let mut parent_id = [0u8; 8];
        for (i, out) in parent_id.iter_mut().enumerate() {
            *out = hex_byte(&b[36 + 2 * i..38 + 2 * i])?;
        }
        let flags = hex_byte(&b[53..55])?;
        let flags = if version == 0 { flags } else { flags & 0x01 };
        if trace_id == [0; 16] || parent_id == [0; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            parent_id,
            flags,
        })
    }

    /// The trace id as the 32 lowercase hex digits every tracing backend displays and searches by.
    pub fn trace_id_hex(&self) -> String {
        hex(&self.trace_id)
    }

    /// The caller's span id as 16 lowercase hex digits.
    pub fn parent_id_hex(&self) -> String {
        hex(&self.parent_id)
    }

    /// Whether the caller sampled this trace (flags bit 0).
    pub fn sampled(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Interpret one request's optional `traceparent`. An ABSENT header is not counted, because
/// sending none is the normal case; a PRESENT one that does not parse is.
pub fn from_request(raw: Option<&str>) -> Option<TraceParent> {
    parse_counted(raw, &INVALID)
}

/// [`from_request`] against an explicit counter, so the counting rule is testable without racing
/// every other test in the process on the shared [`INVALID`].
fn parse_counted(raw: Option<&str>, invalid: &AtomicU64) -> Option<TraceParent> {
    let raw = raw?;
    let parsed = TraceParent::parse(raw);
    if parsed.is_none() {
        invalid.fetch_add(1, Ordering::Relaxed);
    }
    parsed
}

/// Requests seen with an unparseable `traceparent`.
pub fn invalid_total() -> u64 {
    INVALID.load(Ordering::Relaxed)
}

/// Two LOWERCASE hex digits as one byte.
fn hex_byte(pair: &[u8]) -> Option<u8> {
    Some(nibble(pair[0])? << 4 | nibble(pair[1])?)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The W3C specification's own example (Trace Context Level 1, §3.2.3).
    const EXAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn the_w3c_example_parses_and_round_trips_its_ids() {
        let tp = TraceParent::parse(EXAMPLE).expect("the spec's own example");
        assert_eq!(tp.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tp.parent_id_hex(), "00f067aa0ba902b7");
        assert_eq!(tp.flags, 0x01);
        assert!(tp.sampled());

        let unsampled = TraceParent::parse(&EXAMPLE.replace("-01", "-00")).unwrap();
        assert!(!unsampled.sampled());
    }

    /// Every way the grammar can be broken is refused. Each case changes ONE thing from the valid
    /// example, so a case that wrongly parses names the rule that is missing.
    #[test]
    fn each_grammar_violation_is_refused() {
        let cases: &[(&str, String)] = &[
            ("empty", String::new()),
            ("too short", EXAMPLE[..54].to_string()),
            ("v00 too long", format!("{EXAMPLE}-")),
            ("v00 with trailing data", format!("{EXAMPLE}-extra")),
            ("version ff", EXAMPLE.replacen("00", "ff", 1)),
            ("uppercase trace id", EXAMPLE.replace("4bf92f", "4BF92F")),
            ("uppercase version", EXAMPLE.replacen("00", "0A", 1)),
            ("non-hex parent id", EXAMPLE.replace("00f067aa", "00f067zz")),
            ("non-hex flags", EXAMPLE.replace("-01", "-0g")),
            (
                "all-zero trace id",
                format!("00-{}-00f067aa0ba902b7-01", "0".repeat(32)),
            ),
            (
                "all-zero parent id",
                format!("00-4bf92f3577b34da6a3ce929d0e0e4736-{}-01", "0".repeat(16)),
            ),
            ("wrong separator", EXAMPLE.replacen('-', "_", 1)),
            // The second and third separators, each replaced by a HEX digit: anything else is
            // caught by the hex parse of a neighbouring field, so only a hex digit proves the
            // separator check itself (the C4c-1 review deleted each check and both survived).
            ("hex digit for the second separator", sep_to_hex(35)),
            ("hex digit for the third separator", sep_to_hex(52)),
            (
                "short trace id, long parent",
                "00-4bf92f3577b34da6a3ce929d0e0e473-600f067aa0ba902b7-01".to_string(),
            ),
            ("whitespace padded", format!(" {}", &EXAMPLE[..54])),
        ];
        for (name, s) in cases {
            assert_eq!(TraceParent::parse(s), None, "{name}: {s:?} must not parse");
        }
    }

    fn sep_to_hex(at: usize) -> String {
        let mut b = EXAMPLE.as_bytes().to_vec();
        assert_eq!(b[at], b'-');
        b[at] = b'a';
        String::from_utf8(b).unwrap()
    }

    /// A non-UTF-8 header reaches the parser as U+FFFD (the codec decodes this field lossily) and
    /// must be refused there. The review's probe sent 55 bytes ending in `0xff`; decoded, that is 57
    /// bytes — refused by version 00's exact length — and under a LATER version, where length alone
    /// does not refuse it, the replacement character's first byte fails the flags' hex parse.
    #[test]
    fn a_replacement_character_is_refused() {
        let lossy = format!("{}\u{FFFD}", &EXAMPLE[..54]);
        assert_eq!(TraceParent::parse(&lossy), None);
        let later = lossy.replacen("00", "01", 1);
        assert_eq!(TraceParent::parse(&later), None);
    }

    /// A higher version's flags are reduced to `sampled` (§3.2.4); version 00 keeps its byte.
    #[test]
    fn only_the_sampled_bit_of_a_later_versions_flags_is_kept() {
        let v01 = EXAMPLE.replacen("00", "01", 1).replace("-01", "-ff");
        assert_eq!(TraceParent::parse(&v01).unwrap().flags, 0x01);
        let v00 = EXAMPLE.replace("-01", "-ff");
        assert_eq!(TraceParent::parse(&v00).unwrap().flags, 0xff);
    }

    /// A LATER version may append fields after a `-` (§3.2.4); its known prefix is parsed.
    /// Without this, a W3C version bump upstream would silently drop every Ferro span's parent.
    #[test]
    fn a_future_version_parses_its_known_prefix() {
        let v01 = EXAMPLE.replacen("00", "01", 1);
        assert!(TraceParent::parse(&v01).is_some(), "exactly 55 is fine");
        let extended = format!("{v01}-future-field");
        let tp = TraceParent::parse(&extended).expect("a later version with an extra field");
        assert_eq!(tp.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        // ...but only after a `-`.
        assert_eq!(TraceParent::parse(&format!("{v01}x")), None);
    }

    /// Absent is the normal case and is not counted; a valid header is not counted; a present
    /// and invalid one is counted and dropped.
    #[test]
    fn only_a_present_invalid_header_is_counted() {
        let counter = AtomicU64::new(0);
        assert_eq!(parse_counted(None, &counter), None);
        assert!(parse_counted(Some(EXAMPLE), &counter).is_some());
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "absent and valid are not counted"
        );
        assert_eq!(parse_counted(Some("not-a-traceparent"), &counter), None);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "a present invalid header is counted"
        );
    }
}
