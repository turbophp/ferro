//! An upstream's `ORIGIN` (SPEC §23.3.1): `http(s)://host[:port]`, parsed strictly.
//!
//! The operator writes it, so it is refused rather than repaired wherever a repair could be
//! ambiguous. The only normalisations are the two §23.4.2 names for the *normalised* origin:
//! the default port is removed, and an IPv6 literal is rewritten to its RFC 5952 form (in
//! brackets). Everything else must already be canonical:
//!
//! - lowercase ASCII throughout (punycode for an IDN — a non-ASCII or fullwidth host is refused,
//!   never IDNA-mapped, so `ｅxample.com` and `example。com` cannot alias `example.com`);
//! - no userinfo, path (a lone trailing `/` included), query, fragment, backslash or `%`;
//! - a host name is LDH labels (plus `_`), 1–63 bytes each, at most 253 bytes, with no trailing dot;
//! - **a host whose last label is numeric must be a canonical dotted-quad IPv4 address.** This
//!   refuses every other form `inet_aton` would read as an address — `2130706433`, `0x7f.1`,
//!   `0177.0.0.1`, `127.1` — so a name the address guard never classifies cannot reach a
//!   literal address through the resolver;
//! - a port is decimal, 1–65535, with no leading zero.
//!
//! The choices that §23 left open are recorded in SPEC §22.2 (cw).

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::Http => "http",
            Scheme::Https => "https",
        }
    }

    pub fn default_port(self) -> u16 {
        match self {
            Scheme::Http => 80,
            Scheme::Https => 443,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Host {
    Name(String),
    V4(Ipv4Addr),
    V6(Ipv6Addr),
}

/// A parsed, normalised origin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    scheme: Scheme,
    host: Host,
    port: u16,
    authority: String,
    normalised: String,
}

/// Why an `ORIGIN` was refused. The [`Display`](fmt::Display) form is a fixed sentence: it never
/// quotes the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginError {
    NotAscii,
    Control,
    Uppercase,
    Scheme,
    Userinfo,
    PathQueryOrFragment,
    Percent,
    EmptyHost,
    TrailingDot,
    HostSyntax,
    NumericHost,
    Ipv6Literal,
    Port,
}

impl OriginError {
    /// Every variant. The refusal corpus asserts it exercises each one.
    pub const ALL: [OriginError; 13] = [
        OriginError::NotAscii,
        OriginError::Control,
        OriginError::Uppercase,
        OriginError::Scheme,
        OriginError::Userinfo,
        OriginError::PathQueryOrFragment,
        OriginError::Percent,
        OriginError::EmptyHost,
        OriginError::TrailingDot,
        OriginError::HostSyntax,
        OriginError::NumericHost,
        OriginError::Ipv6Literal,
        OriginError::Port,
    ];
}

impl fmt::Display for OriginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OriginError::NotAscii => "must be ASCII (use punycode for an IDN host)",
            OriginError::Control => "contains whitespace or a control byte",
            OriginError::Uppercase => "must be lowercase",
            OriginError::Scheme => "scheme must be http or https",
            OriginError::Userinfo => "must not carry userinfo",
            OriginError::PathQueryOrFragment => {
                "must not carry a path, query, fragment or backslash (no trailing slash)"
            }
            OriginError::Percent => "must not carry percent-encoding",
            OriginError::EmptyHost => "host is empty",
            OriginError::TrailingDot => "host must not end with a dot",
            OriginError::HostSyntax => {
                "host must be LDH labels of 1-63 bytes ([a-z0-9_-], no edge hyphen), at most 253 bytes"
            }
            OriginError::NumericHost => {
                "a host ending in a numeric label must be a canonical dotted-quad IPv4 address"
            }
            OriginError::Ipv6Literal => "IPv6 literal must be a bracketed address with no zone id",
            OriginError::Port => "port must be decimal 1-65535 with no leading zero",
        })
    }
}

impl Origin {
    pub fn parse(s: &str) -> Result<Origin, OriginError> {
        let b = s.as_bytes();
        if b.iter().any(|&c| c >= 0x80) {
            return Err(OriginError::NotAscii);
        }
        if b.iter().any(|&c| c <= 0x20 || c == 0x7f) {
            return Err(OriginError::Control);
        }
        if b.iter().any(u8::is_ascii_uppercase) {
            return Err(OriginError::Uppercase);
        }
        let (scheme, rest) = if let Some(r) = s.strip_prefix("https://") {
            (Scheme::Https, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (Scheme::Http, r)
        } else {
            return Err(OriginError::Scheme);
        };
        if rest.contains('@') {
            return Err(OriginError::Userinfo);
        }
        if rest.contains(['/', '?', '#', '\\']) {
            return Err(OriginError::PathQueryOrFragment);
        }
        if rest.contains('%') {
            return Err(OriginError::Percent);
        }
        let (host, port_text) = if let Some(after) = rest.strip_prefix('[') {
            let (inside, tail) = after.split_once(']').ok_or(OriginError::Ipv6Literal)?;
            let a: Ipv6Addr = inside.parse().map_err(|_| OriginError::Ipv6Literal)?;
            let port = match tail {
                "" => None,
                t => Some(t.strip_prefix(':').ok_or(OriginError::Ipv6Literal)?),
            };
            (Host::V6(a), port)
        } else {
            if rest.contains(['[', ']']) {
                return Err(OriginError::Ipv6Literal);
            }
            let (h, p) = match rest.split_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (rest, None),
            };
            (parse_host_name(h)?, p)
        };
        let port = match port_text {
            None => scheme.default_port(),
            Some(p) => parse_port(p)?,
        };
        Ok(Origin::build(scheme, host, port))
    }

    fn build(scheme: Scheme, host: Host, port: u16) -> Origin {
        let mut authority = match &host {
            Host::Name(n) => n.clone(),
            Host::V4(a) => a.to_string(),
            Host::V6(a) => format!("[{a}]"),
        };
        if port != scheme.default_port() {
            authority.push(':');
            authority.push_str(&port.to_string());
        }
        let normalised = format!("{}://{}", scheme.as_str(), authority);
        Origin {
            scheme,
            host,
            port,
            authority,
            normalised,
        }
    }

    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    /// The effective port (the default one when none was written).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// `host[:port]`, default port removed, IPv6 in brackets: the exact `Host` the engine sends
    /// and the only value a PHP `host` header may carry (§23.4.3).
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// `scheme://authority`: the value the request's `origin` field must equal byte for byte
    /// (§23.4.2).
    pub fn normalised(&self) -> &str {
        &self.normalised
    }

    pub fn ip_literal(&self) -> Option<IpAddr> {
        match self.host {
            Host::Name(_) => None,
            Host::V4(a) => Some(IpAddr::V4(a)),
            Host::V6(a) => Some(IpAddr::V6(a)),
        }
    }
}

fn parse_port(p: &str) -> Result<u16, OriginError> {
    let ok = !p.is_empty()
        && p.len() <= 5
        && p.bytes().all(|c| c.is_ascii_digit())
        && !p.starts_with('0');
    if !ok {
        return Err(OriginError::Port);
    }
    match p.parse::<u32>() {
        Ok(n) if (1..=65535).contains(&n) => Ok(n as u16),
        _ => Err(OriginError::Port),
    }
}

fn is_numeric_label(l: &str) -> bool {
    (!l.is_empty() && l.bytes().all(|c| c.is_ascii_digit()))
        || l.strip_prefix("0x")
            .is_some_and(|h| h.bytes().all(|c| c.is_ascii_hexdigit()))
}

fn canonical_v4(h: &str) -> Option<Ipv4Addr> {
    let parts: Vec<&str> = h.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut o = [0u8; 4];
    for (i, p) in parts.iter().enumerate() {
        let canonical = !p.is_empty()
            && p.len() <= 3
            && p.bytes().all(|c| c.is_ascii_digit())
            && (*p == "0" || !p.starts_with('0'));
        if !canonical {
            return None;
        }
        o[i] = p.parse::<u8>().ok()?;
    }
    Some(Ipv4Addr::from(o))
}

fn parse_host_name(h: &str) -> Result<Host, OriginError> {
    if h.is_empty() {
        return Err(OriginError::EmptyHost);
    }
    if h.ends_with('.') {
        return Err(OriginError::TrailingDot);
    }
    if h.len() > 253 {
        return Err(OriginError::HostSyntax);
    }
    let labels: Vec<&str> = h.split('.').collect();
    let last = labels.last().copied().unwrap_or_default();
    if is_numeric_label(last) {
        return canonical_v4(h)
            .map(Host::V4)
            .ok_or(OriginError::NumericHost);
    }
    for l in &labels {
        let ok = !l.is_empty()
            && l.len() <= 63
            && l.bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'_')
            && !l.starts_with('-')
            && !l.ends_with('-');
        if !ok {
            return Err(OriginError::HostSyntax);
        }
    }
    Ok(Host::Name(h.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalisation_is_only_the_default_port_and_ipv6_form() {
        let cases = [
            (
                "https://api.example.com",
                "https://api.example.com",
                "api.example.com",
            ),
            (
                "https://api.example.com:443",
                "https://api.example.com",
                "api.example.com",
            ),
            (
                "http://api.example.com:80",
                "http://api.example.com",
                "api.example.com",
            ),
            (
                "http://api.example.com:443",
                "http://api.example.com:443",
                "api.example.com:443",
            ),
            (
                "https://api.example.com:80",
                "https://api.example.com:80",
                "api.example.com:80",
            ),
            (
                "http://127.0.0.1:9200",
                "http://127.0.0.1:9200",
                "127.0.0.1:9200",
            ),
            ("http://[::1]:8080", "http://[::1]:8080", "[::1]:8080"),
            ("http://[0:0:0:0:0:0:0:1]", "http://[::1]", "[::1]"),
            (
                "http://[::ffff:10.0.0.1]",
                "http://[::ffff:10.0.0.1]",
                "[::ffff:10.0.0.1]",
            ),
            (
                "https://xn--bcher-kva.example",
                "https://xn--bcher-kva.example",
                "xn--bcher-kva.example",
            ),
            (
                "http://es_node.internal",
                "http://es_node.internal",
                "es_node.internal",
            ),
            ("http://localhost", "http://localhost", "localhost"),
        ];
        for (input, norm, auth) in cases {
            let o = Origin::parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(o.normalised(), norm, "{input}");
            assert_eq!(o.authority(), auth, "{input}");
            assert_eq!(
                Origin::parse(o.normalised()).as_ref(),
                Ok(&o),
                "round trip {input}"
            );
        }
    }

    #[test]
    fn the_numeric_host_rule() {
        assert_eq!(
            Origin::parse("http://10.0.0.1").unwrap().ip_literal(),
            Some("10.0.0.1".parse().unwrap())
        );
        for bad in [
            "http://2130706433",
            "http://0x7f000001",
            "http://0x7f.0.0.1",
            "http://0177.0.0.1",
            "http://127.1",
            "http://127.0.1",
            "http://127.0.0.01",
            "http://256.0.0.1",
            "http://1.2.3.4.5",
            "http://evil.0x1",
            "http://evil.123",
        ] {
            assert_eq!(Origin::parse(bad), Err(OriginError::NumericHost), "{bad}");
        }
        // Controls: a numeric label that is NOT last is an ordinary DNS label.
        assert!(Origin::parse("http://123.example.com").is_ok());
        assert!(Origin::parse("http://0x7f.example.com").is_ok());
    }
}
