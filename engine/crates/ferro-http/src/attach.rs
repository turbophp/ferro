//! Attached headers (SPEC §23.3.1 `ATTACH_HEADERS_FILE`, §23.3.2 credential custody).
//!
//! The file holds `Name: value` lines. A value is credential material: it is validated as a
//! field-value at load and then held in [`SecretValue`], which has no `Debug`, `Display`, `Clone`
//! or comparison, so the only way to read it is [`SecretValue::expose_for_wire`] — the one call
//! the request writer (slice F4) makes. Errors name a line number and a rule, never a name or a
//! value from the file.

use std::fmt;

use crate::syntax::{fold_name, is_field_value, is_token};

/// One attached value. Deliberately not `Debug`/`Display`/`Clone`.
pub struct SecretValue(Box<[u8]>);

impl SecretValue {
    /// The bytes, for rendering into the outbound request only (§23.3.2).
    pub fn expose_for_wire(&self) -> &[u8] {
        &self.0
    }
}

/// One `Name: value` line. The name is kept as written (it is sent); comparisons use
/// [`lower`](AttachedHeader::lower), the FOLDED name (`crate::fold_name`).
pub struct AttachedHeader {
    name: String,
    lower: String,
    value: SecretValue,
}

impl AttachedHeader {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn lower(&self) -> &str {
        &self.lower
    }

    pub fn value(&self) -> &SecretValue {
        &self.value
    }
}

/// An upstream's attached set. `Debug` prints the names only.
#[derive(Default)]
pub struct AttachedHeaders(Vec<AttachedHeader>);

impl fmt::Debug for AttachedHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|h| format!("{}: <redacted>", h.name)))
            .finish()
    }
}

impl AttachedHeaders {
    pub fn iter(&self) -> impl Iterator<Item = &AttachedHeader> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether `lower_name` (already folded with `crate::fold_name`) is attached.
    pub fn names(&self, lower_name: &str) -> bool {
        self.0.iter().any(|h| h.lower == lower_name)
    }
}

/// The file's size cap: the request-header limit of §23.4.3 (64 KiB), plus line overhead.
pub const MAX_FILE_BYTES: usize = 64 * 1024 + 4096;
/// The line cap: §23.4.3's 100.
pub const MAX_LINES: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachError {
    TooLarge,
    TooManyLines,
    /// 1-based line number.
    NoColon(usize),
    BadName(usize),
    BadValue(usize),
    /// A name the engine itself owns or refuses (`host`, `content-length`, hop-by-hop …).
    ReservedName(usize),
}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachError::TooLarge => write!(f, "file exceeds {MAX_FILE_BYTES} bytes"),
            AttachError::TooManyLines => write!(f, "file has more than {MAX_LINES} header lines"),
            AttachError::NoColon(n) => write!(f, "line {n}: not a `Name: value` line"),
            AttachError::BadName(n) => {
                write!(
                    f,
                    "line {n}: header name is not an RFC 9110 token of 1-256 bytes"
                )
            }
            AttachError::BadValue(n) => write!(f, "line {n}: header value is not a field-value"),
            AttachError::ReservedName(n) => write!(
                f,
                "line {n}: header name is one the engine sets, drops or refuses and cannot be attached"
            ),
        }
    }
}

/// Header names an attached line may not use: every name §23.4.3 has the engine set, drop or
/// refuse unconditionally, because attaching one would either be overwritten or put a hop-by-hop
/// or framing header on the wire behind the engine's back.
pub(crate) fn is_reserved_attach_name(lower: &str) -> bool {
    matches!(
        lower,
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "te"
            | "http2-settings"
            | "expect"
            | "upgrade"
            | "proxy-authorization"
    )
}

/// Parse the file's bytes. Lines end in LF or CRLF; blank lines are skipped; optional `SP`/`HTAB`
/// around the value is trimmed (RFC 9110's OWS). There is no comment syntax.
pub fn parse(bytes: &[u8]) -> Result<AttachedHeaders, AttachError> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err(AttachError::TooLarge);
    }
    let mut out = Vec::new();
    for (i, raw) in bytes.split(|&b| b == b'\n').enumerate() {
        let n = i + 1;
        let line = raw.strip_suffix(b"\r").unwrap_or(raw);
        if line.is_empty() {
            continue;
        }
        if out.len() == MAX_LINES {
            return Err(AttachError::TooManyLines);
        }
        let colon = line
            .iter()
            .position(|&b| b == b':')
            .ok_or(AttachError::NoColon(n))?;
        let (name, value) = (&line[..colon], &line[colon + 1..]);
        if !is_token(name, 256) {
            return Err(AttachError::BadName(n));
        }
        let ows = |b: &u8| *b == b' ' || *b == b'\t';
        let start = value.iter().position(|b| !ows(b)).unwrap_or(value.len());
        let end = value.iter().rposition(|b| !ows(b)).map_or(start, |p| p + 1);
        let value = &value[start..end];
        if !is_field_value(value) {
            return Err(AttachError::BadValue(n));
        }
        // `is_token` admitted only ASCII, so this is valid UTF-8.
        let name = String::from_utf8_lossy(name).into_owned();
        let lower = fold_name(&name);
        if is_reserved_attach_name(&lower) {
            return Err(AttachError::ReservedName(n));
        }
        out.push(AttachedHeader {
            name,
            lower,
            value: SecretValue(value.into()),
        });
    }
    Ok(AttachedHeaders(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lines_and_never_prints_a_value() {
        let h = parse(b"Authorization: Bearer CANARY-1\r\n\nX-Api-Key:\tCANARY-2 \n").unwrap();
        assert_eq!(h.len(), 2);
        let v: Vec<&[u8]> = h.iter().map(|x| x.value().expose_for_wire()).collect();
        assert_eq!(v, vec![&b"Bearer CANARY-1"[..], b"CANARY-2"]);
        assert!(h.names("authorization"));
        let dbg = format!("{h:?}");
        assert!(!dbg.contains("CANARY"), "{dbg}");
        assert!(dbg.contains("Authorization"));
    }

    #[test]
    fn refusals_name_the_line_only() {
        let cases: &[(&[u8], AttachError)] = &[
            (b"Authorization Bearer x", AttachError::NoColon(1)),
            (b"ok: 1\nbad name: x", AttachError::BadName(2)),
            (b": x", AttachError::BadName(1)),
            (b"a: CANARY\x00", AttachError::BadValue(1)),
            (b"a: x\rInjected: y", AttachError::BadValue(1)),
            (b"Host: evil", AttachError::ReservedName(1)),
            (b"Transfer-Encoding: chunked", AttachError::ReservedName(1)),
            (b"TE: trailers", AttachError::ReservedName(1)),
            (b"Connection: close", AttachError::ReservedName(1)),
            (
                b"Proxy-Authorization: Basic x",
                AttachError::ReservedName(1),
            ),
            // Folded names (review F-1): CGI-style servers read these as the reserved ones.
            (b"Transfer_Encoding: chunked", AttachError::ReservedName(1)),
            (b"Content.Length: 1", AttachError::ReservedName(1)),
        ];
        for (input, want) in cases {
            let e = parse(input).err();
            assert_eq!(e, Some(*want), "{:?}", String::from_utf8_lossy(input));
            assert!(!e.unwrap().to_string().contains("CANARY"));
        }
        let many: Vec<u8> = (0..101)
            .flat_map(|i| format!("h{i}: v\n").into_bytes())
            .collect();
        assert_eq!(parse(&many).err(), Some(AttachError::TooManyLines));
        assert_eq!(
            parse(&vec![b'a'; MAX_FILE_BYTES + 1]).err(),
            Some(AttachError::TooLarge)
        );
    }
}
