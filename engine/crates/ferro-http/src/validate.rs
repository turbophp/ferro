//! Request validation: the SSRF rule (SPEC §23.4).
//!
//! *PHP can reach only declared upstreams, at their declared origin, on permitted path prefixes,
//! with permitted methods.* Every check here refuses; none rewrites. The engine sends exactly the
//! method and target bytes it accepted, and the headers it keeps are byte-identical to PHP's.
//!
//! A refusal is a [`Refusal`]: the [`Rule`] that fired, its §23.5.6 cause group ([`PolicyCause`]),
//! and at most a byte offset or a header *name* — never a PHP-supplied value (§23.4). The wire
//! token for a cause (`forbidden_target`, …) is NOT spelled here: it belongs to `/proto`'s
//! `[http.causes]` (slice F2), and charter rule 2 makes a hand-written copy a defect. The engine
//! maps [`PolicyCause`] to the generated constant when it builds the terminal (slice F4).

use std::fmt;

use unicode_normalization::UnicodeNormalization;

use crate::config::{AttachPolicy, HttpConfig, PathEncoding, PathParams, Upstream};
use crate::syntax::{hex_val, is_field_value, is_token, parse_decimal_u64};

/// The `forbidden_*` cause group of §23.5.6 a refusal belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PolicyCause {
    Upstream,
    Origin,
    Target,
    Method,
    Header,
    Body,
    Address,
}

/// The individual rule that refused. [`Rule::name`] is a stable test/diagnostic name (the refusal
/// corpus keys on it); it is not a wire value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rule {
    /// Unknown upstream, a disabled one, or one this peer's uid may not use — one identical
    /// refusal (D15 indistinguishability, §23.4).
    UpstreamUnavailable,
    /// The `origin` field is not byte-equal to the normalised `ORIGIN`.
    OriginMismatch,
    // §23.4.1
    MethodToken,
    MethodAlwaysRefused,
    MethodNotAllowed,
    // §23.4.2
    TargetLength,
    TargetForm,
    TargetByte,
    TargetPercent,
    TargetNul,
    PathPct25,
    PathControl,
    PathNonAscii,
    PathParam,
    PathDotSegment,
    PathUtf8,
    PathNfkcDotSegment,
    PathNfkcPercent,
    PathPrefix,
    // §23.4.3
    HeaderCount,
    HeaderSize,
    HeaderPseudo,
    HeaderName,
    HeaderValue,
    HeaderHost,
    HeaderContentLength,
    HeaderTransferEncoding,
    HeaderUpgradeOrProxyAuth,
    HeaderMethodOverride,
    HeaderForwarding,
    HeaderAttached,
    // §23.4.4
    BodyTooLarge,
}

impl Rule {
    /// Every rule. The refusal corpus asserts it exercises each one.
    pub const ALL: [Rule; 32] = [
        Rule::UpstreamUnavailable,
        Rule::OriginMismatch,
        Rule::MethodToken,
        Rule::MethodAlwaysRefused,
        Rule::MethodNotAllowed,
        Rule::TargetLength,
        Rule::TargetForm,
        Rule::TargetByte,
        Rule::TargetPercent,
        Rule::TargetNul,
        Rule::PathPct25,
        Rule::PathControl,
        Rule::PathNonAscii,
        Rule::PathParam,
        Rule::PathDotSegment,
        Rule::PathUtf8,
        Rule::PathNfkcDotSegment,
        Rule::PathNfkcPercent,
        Rule::PathPrefix,
        Rule::HeaderCount,
        Rule::HeaderSize,
        Rule::HeaderPseudo,
        Rule::HeaderName,
        Rule::HeaderValue,
        Rule::HeaderHost,
        Rule::HeaderContentLength,
        Rule::HeaderTransferEncoding,
        Rule::HeaderUpgradeOrProxyAuth,
        Rule::HeaderMethodOverride,
        Rule::HeaderForwarding,
        Rule::HeaderAttached,
        Rule::BodyTooLarge,
    ];

    pub fn cause(self) -> PolicyCause {
        use Rule::*;
        match self {
            UpstreamUnavailable => PolicyCause::Upstream,
            OriginMismatch => PolicyCause::Origin,
            MethodToken | MethodAlwaysRefused | MethodNotAllowed => PolicyCause::Method,
            TargetLength | TargetForm | TargetByte | TargetPercent | TargetNul | PathPct25
            | PathControl | PathNonAscii | PathParam | PathDotSegment | PathUtf8
            | PathNfkcDotSegment | PathNfkcPercent | PathPrefix => PolicyCause::Target,
            HeaderCount
            | HeaderSize
            | HeaderPseudo
            | HeaderName
            | HeaderValue
            | HeaderHost
            | HeaderContentLength
            | HeaderTransferEncoding
            | HeaderUpgradeOrProxyAuth
            | HeaderMethodOverride
            | HeaderForwarding
            | HeaderAttached => PolicyCause::Header,
            BodyTooLarge => PolicyCause::Body,
        }
    }

    pub fn name(self) -> &'static str {
        use Rule::*;
        match self {
            UpstreamUnavailable => "upstream.unavailable",
            OriginMismatch => "origin.mismatch",
            MethodToken => "method.token",
            MethodAlwaysRefused => "method.always_refused",
            MethodNotAllowed => "method.not_allowed",
            TargetLength => "target.length",
            TargetForm => "target.form",
            TargetByte => "target.byte",
            TargetPercent => "target.percent",
            TargetNul => "target.nul",
            PathPct25 => "path.pct25",
            PathControl => "path.control",
            PathNonAscii => "path.non_ascii",
            PathParam => "path.param",
            PathDotSegment => "path.dot_segment",
            PathUtf8 => "path.utf8",
            PathNfkcDotSegment => "path.nfkc_dot_segment",
            PathNfkcPercent => "path.nfkc_percent",
            PathPrefix => "path.prefix",
            HeaderCount => "header.count",
            HeaderSize => "header.size",
            HeaderPseudo => "header.pseudo",
            HeaderName => "header.name",
            HeaderValue => "header.value",
            HeaderHost => "header.host",
            HeaderContentLength => "header.content_length",
            HeaderTransferEncoding => "header.transfer_encoding",
            HeaderUpgradeOrProxyAuth => "header.upgrade_or_proxy_auth",
            HeaderMethodOverride => "header.method_override",
            HeaderForwarding => "header.forwarding",
            HeaderAttached => "header.attached",
            BodyTooLarge => "body.too_large",
        }
    }

    fn sentence(self) -> &'static str {
        use Rule::*;
        match self {
            UpstreamUnavailable => "upstream not available to this peer",
            OriginMismatch => "origin does not match the upstream's declared origin",
            MethodToken => "method is not an RFC 9110 token of 1-32 bytes",
            MethodAlwaysRefused => "method CONNECT, TRACE and TRACK are never allowed",
            MethodNotAllowed => "method is not in the upstream's ALLOW_METHODS",
            TargetLength => "target must be 1-8192 bytes",
            TargetForm => "target must be origin-form (start with one '/')",
            TargetByte => "target byte is outside the allowed set",
            TargetPercent => "'%' must be followed by two hex digits",
            TargetNul => "%00 is refused",
            PathPct25 => "%25 is refused in the path",
            PathControl => "a percent-encoded control byte is refused in the path",
            PathNonAscii => "a percent-encoded byte >= 0x80 is refused in the path",
            PathParam => "a raw ';' is refused in the path",
            PathDotSegment => "the path contains a dot segment",
            PathUtf8 => "the decoded path is not valid UTF-8",
            PathNfkcDotSegment => "the path contains a dot segment under NFKC",
            PathNfkcPercent => "the path's NFKC form contains '%'",
            PathPrefix => "the path is outside the upstream's ALLOW_PATHS",
            HeaderCount => "more than 100 header lines",
            HeaderSize => "header names and values exceed 64 KiB",
            HeaderPseudo => "a pseudo-header is refused",
            HeaderName => "a header name is not an RFC 9110 token of 1-256 bytes",
            HeaderValue => "a header value is not a field-value",
            HeaderHost => "host does not match the upstream authority",
            HeaderContentLength => "content-length does not match the body",
            HeaderTransferEncoding => "transfer-encoding other than chunked is refused",
            HeaderUpgradeOrProxyAuth => "upgrade and proxy-authorization are refused",
            HeaderMethodOverride => "a method or URL override header is refused",
            HeaderForwarding => "a forwarding header is refused",
            HeaderAttached => "the header is attached by the engine for this upstream",
            BodyTooLarge => "the body exceeds the upstream's body budget",
        }
    }
}

/// A refusal. Its `Display` names the rule, and at most a byte offset or a header name: never a
/// PHP-supplied value. For [`Rule::UpstreamUnavailable`] it is one fixed sentence, whatever the
/// reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub rule: Rule,
    /// A byte offset into the target, or a header's index in the request.
    pub offset: Option<usize>,
    /// The offending header's name — only once it has been validated as a token.
    pub header: Option<String>,
}

impl Refusal {
    fn new(rule: Rule) -> Self {
        Refusal {
            rule,
            offset: None,
            header: None,
        }
    }

    fn at(rule: Rule, offset: usize) -> Self {
        Refusal {
            rule,
            offset: Some(offset),
            header: None,
        }
    }

    fn header(rule: Rule, index: usize, name: Option<&str>) -> Self {
        Refusal {
            rule,
            offset: Some(index),
            header: name.map(str::to_string),
        }
    }

    pub fn cause(&self) -> PolicyCause {
        self.rule.cause()
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.rule.sentence())?;
        if self.rule == Rule::UpstreamUnavailable {
            return Ok(());
        }
        match (&self.header, self.offset, self.rule.cause()) {
            (Some(name), _, _) => write!(f, " (header `{name}`)"),
            (None, Some(i), PolicyCause::Header) => write!(f, " (header #{i})"),
            (None, Some(off), _) => write!(f, " (byte {off})"),
            _ => Ok(()),
        }
    }
}

/// One request as `HttpRequest` (§23.5.1) carries it, minus the fields validation does not read.
#[derive(Clone, Debug)]
pub struct Request<'a> {
    pub upstream: &'a str,
    pub method: &'a str,
    pub target: &'a str,
    pub origin: Option<&'a str>,
    pub headers: &'a [(String, Vec<u8>)],
    pub body: Option<&'a [u8]>,
    pub idempotent: Option<bool>,
    pub decode: bool,
}

/// An accepted request: what the engine sends, by reference to what PHP sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Validated<'c> {
    pub upstream: &'c Upstream,
    /// The exact `Host` (`:authority`): the upstream's normalised authority.
    pub host: &'c str,
    /// Indices of the PHP headers sent verbatim, in order.
    pub send_headers: Vec<usize>,
    /// PHP values discarded under `ATTACH_POLICY=override` (counted, §23.10.2).
    pub overridden: u32,
    /// Whether the engine adds `Accept-Encoding: gzip, deflate` (§23.9.2).
    pub add_accept_encoding: bool,
    /// The recomputed `Content-Length`.
    pub content_length: u64,
    /// Effective idempotency (§23.7.2).
    pub idempotent: bool,
}

pub const MAX_TARGET: usize = 8192;
pub const MAX_METHOD: usize = 32;
pub const MAX_HEADER_NAME: usize = 256;
pub const MAX_HEADER_LINES: usize = 100;
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Validate one request for a peer. `peer_uid` is the kernel-attested uid (`None` when the
/// transport attests none, which an upstream with `ALLOW_UIDS` refuses).
pub fn validate<'c>(
    cfg: &'c HttpConfig,
    peer_uid: Option<u32>,
    req: &Request<'_>,
) -> Result<Validated<'c>, Refusal> {
    let up = cfg
        .upstream_for(req.upstream, peer_uid)
        .ok_or(Refusal::new(Rule::UpstreamUnavailable))?;
    check_method(up, req.method)?;
    check_target(up, req.target.as_bytes())?;
    if let Some(o) = req.origin
        && o != up.origin.normalised()
    {
        return Err(Refusal::new(Rule::OriginMismatch));
    }
    let body_len = req.body.map_or(0, <[u8]>::len) as u64;
    let plan = check_headers(up, req.headers, body_len)?;
    let limit = up.limits.max_body_bytes.min(cfg.daemon.max_body_bytes);
    if body_len > limit {
        return Err(Refusal::new(Rule::BodyTooLarge));
    }
    let idempotent = effective_idempotency(up, req.method, req.headers, req.idempotent);
    Ok(Validated {
        upstream: up,
        host: up.origin.authority(),
        send_headers: plan.keep,
        overridden: plan.overridden,
        add_accept_encoding: req.decode && !plan.saw_accept_encoding,
        content_length: body_len,
        idempotent,
    })
}

/// §23.7.2: a declaration (the caller's, or the operator's per upstream), never the method alone.
pub fn effective_idempotency(
    up: &Upstream,
    method: &str,
    headers: &[(String, Vec<u8>)],
    declared: Option<bool>,
) -> bool {
    match declared {
        Some(d) => d,
        None => {
            up.idempotent_methods.iter().any(|m| m == method)
                || up.idempotency_key_header.as_deref().is_some_and(|key| {
                    headers
                        .iter()
                        .any(|(n, v)| n.eq_ignore_ascii_case(key) && !v.is_empty())
                })
        }
    }
}

fn check_method(up: &Upstream, m: &str) -> Result<(), Refusal> {
    if !is_token(m.as_bytes(), MAX_METHOD) {
        return Err(Refusal::new(Rule::MethodToken));
    }
    if is_always_refused_method(m) {
        return Err(Refusal::new(Rule::MethodAlwaysRefused));
    }
    if let Some(allowed) = &up.allow_methods
        && !allowed.iter().any(|a| a == m)
    {
        return Err(Refusal::new(Rule::MethodNotAllowed));
    }
    Ok(())
}

/// `CONNECT`, `TRACE`, `TRACK`, compared ASCII-case-insensitively (§23.4.1).
pub fn is_always_refused_method(m: &str) -> bool {
    ["CONNECT", "TRACE", "TRACK"]
        .iter()
        .any(|x| x.eq_ignore_ascii_case(m))
}

/// The step-3 byte allow-list.
pub fn is_target_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/?%".contains(&b)
}

fn check_target(up: &Upstream, t: &[u8]) -> Result<(), Refusal> {
    check_target_syntax(t)?;
    // 5. Split.
    let path = match t.iter().position(|&b| b == b'?') {
        Some(q) => &t[..q],
        None => t,
    };
    // 6–8.
    check_path(path, up.path_params, up.path_encoding)?;
    // 9. Prefix confinement, on the raw path.
    if !up
        .allow_paths
        .iter()
        .any(|p| prefix_matches(p.as_bytes(), path))
    {
        return Err(Refusal::new(Rule::PathPrefix));
    }
    Ok(())
}

/// Steps 1–4.
fn check_target_syntax(t: &[u8]) -> Result<(), Refusal> {
    // 1. Length.
    if t.is_empty() || t.len() > MAX_TARGET {
        return Err(Refusal::new(Rule::TargetLength));
    }
    // 2. Form.
    if t[0] != b'/' {
        return Err(Refusal::at(Rule::TargetForm, 0));
    }
    if t.get(1) == Some(&b'/') {
        return Err(Refusal::at(Rule::TargetForm, 1));
    }
    // 3. Byte allow-list.
    if let Some(i) = t.iter().position(|&b| !is_target_byte(b)) {
        return Err(Refusal::at(Rule::TargetByte, i));
    }
    // 4. Percent syntax, and %00 anywhere.
    let mut i = 0;
    while i < t.len() {
        if t[i] == b'%' {
            match (
                t.get(i + 1).copied().and_then(hex_val),
                t.get(i + 2).copied().and_then(hex_val),
            ) {
                (Some(0), Some(0)) => return Err(Refusal::at(Rule::TargetNul, i)),
                (Some(_), Some(_)) => i += 3,
                _ => return Err(Refusal::at(Rule::TargetPercent, i)),
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// §23.4.2 step 9.
pub fn prefix_matches(p: &[u8], t: &[u8]) -> bool {
    t == p || (t.starts_with(p) && (p.ends_with(b"/") || t.get(p.len()) == Some(&b'/')))
}

fn is_dot_only(seg: &[u8]) -> bool {
    !seg.is_empty() && seg.iter().all(|&b| b == b'.' || b == b' ') && seg.contains(&b'.')
}

/// Split on `/` and `\`, cut each segment at its first `;`, and report whether any segment is a
/// dot segment (§23.4.2 step 7).
///
/// The cut is applied whatever `PATH_PARAMS` says. With `PATH_PARAMS=refuse` a raw `;` is already
/// refused, so a `;` in the decoded copy came from `%3B`; a server that decodes BEFORE stripping
/// `;params` reads `/api/..%3B/admin` as `/admin`, and cutting can only refuse more
/// (SPEC §22.2 (cw)).
fn has_dot_segment(decoded: &[u8]) -> bool {
    decoded
        .split(|&b| b == b'/' || b == b'\\')
        .map(|seg| match seg.iter().position(|&b| b == b';') {
            Some(c) => &seg[..c],
            None => seg,
        })
        .any(is_dot_only)
}

/// An `ALLOW_PATHS` prefix must itself pass steps 1–8 as a target with no query, under the
/// upstream's own `PATH_PARAMS`/`PATH_ENCODING` — a prefix the validator would refuse as a target
/// could never match one it accepts, and one carrying a dot segment is a configuration mistake.
pub(crate) fn check_prefix(
    p: &[u8],
    params: PathParams,
    encoding: PathEncoding,
) -> Result<(), Refusal> {
    check_target_syntax(p)?;
    if p.contains(&b'?') {
        return Err(Refusal::at(
            Rule::TargetByte,
            p.iter().position(|&b| b == b'?').unwrap_or(0),
        ));
    }
    check_path(p, params, encoding)
}

/// Steps 6–8 over a path (no query). Requires steps 1–4 to have passed.
fn check_path(path: &[u8], params: PathParams, encoding: PathEncoding) -> Result<(), Refusal> {
    let mut decoded = Vec::with_capacity(path.len());
    let mut i = 0;
    while i < path.len() {
        let b = path[i];
        if b == b'%' {
            // Step 4 has run: two hex digits follow.
            let v = (hex_val(path[i + 1]).unwrap_or(0) << 4) | hex_val(path[i + 2]).unwrap_or(0);
            if v == 0x25 {
                return Err(Refusal::at(Rule::PathPct25, i));
            }
            if v < 0x20 || v == 0x7f {
                return Err(Refusal::at(Rule::PathControl, i));
            }
            if v >= 0x80 && encoding == PathEncoding::Ascii {
                return Err(Refusal::at(Rule::PathNonAscii, i));
            }
            decoded.push(v);
            i += 3;
        } else {
            if b == b';' && params == PathParams::Refuse {
                return Err(Refusal::at(Rule::PathParam, i));
            }
            decoded.push(b);
            i += 1;
        }
    }
    if has_dot_segment(&decoded) {
        return Err(Refusal::new(Rule::PathDotSegment));
    }
    if encoding == PathEncoding::Utf8 {
        let s = std::str::from_utf8(&decoded).map_err(|_| Refusal::new(Rule::PathUtf8))?;
        // NFKC of the WHOLE decoded path, then split: per-segment NFKC (the spec's wording) is
        // contained in it, and it also catches a compatibility solidus (U+FF0F → '/') or
        // semicolon creating a segment boundary (SPEC §22.2 (cw)).
        let nfkc: String = s.nfkc().collect();
        if nfkc.contains('%') {
            // The decoded copy holds no '%' (step 6a refused %25), so this one came from a
            // compatibility character such as U+FF05: a server that NFKC-normalises and then
            // percent-decodes would decode a second time.
            return Err(Refusal::new(Rule::PathNfkcPercent));
        }
        if has_dot_segment(nfkc.as_bytes()) {
            return Err(Refusal::new(Rule::PathNfkcDotSegment));
        }
    }
    Ok(())
}

pub(crate) fn is_override_header(lower: &str) -> bool {
    matches!(
        lower,
        "x-http-method-override"
            | "x-http-method"
            | "x-method-override"
            | "x-original-url"
            | "x-rewrite-url"
    )
}

pub(crate) fn is_forwarding_header(lower: &str) -> bool {
    lower == "forwarded"
        || lower == "x-real-ip"
        || lower
            .strip_prefix("x-forwarded-")
            .is_some_and(|rest| !rest.is_empty())
}

struct HeaderPlan {
    keep: Vec<usize>,
    overridden: u32,
    saw_accept_encoding: bool,
}

fn check_headers(
    up: &Upstream,
    headers: &[(String, Vec<u8>)],
    body_len: u64,
) -> Result<HeaderPlan, Refusal> {
    if headers.len() > MAX_HEADER_LINES {
        return Err(Refusal::new(Rule::HeaderCount));
    }
    let total = headers.iter().fold(0usize, |acc, (n, v)| {
        acc.saturating_add(n.len()).saturating_add(v.len())
    });
    if total > MAX_HEADER_BYTES {
        return Err(Refusal::new(Rule::HeaderSize));
    }
    let mut plan = HeaderPlan {
        keep: Vec::with_capacity(headers.len()),
        overridden: 0,
        saw_accept_encoding: false,
    };
    for (i, (name, value)) in headers.iter().enumerate() {
        if name.starts_with(':') {
            return Err(Refusal::header(Rule::HeaderPseudo, i, None));
        }
        if !is_token(name.as_bytes(), MAX_HEADER_NAME) {
            return Err(Refusal::header(Rule::HeaderName, i, None));
        }
        let named = Some(name.as_str());
        if !is_field_value(value) {
            return Err(Refusal::header(Rule::HeaderValue, i, named));
        }
        let lower = name.to_ascii_lowercase();
        let keep = match lower.as_str() {
            "host" => {
                if value.as_slice() != up.origin.authority().as_bytes() {
                    return Err(Refusal::header(Rule::HeaderHost, i, named));
                }
                false
            }
            "content-length" => {
                if parse_decimal_u64(value) != Some(body_len) {
                    return Err(Refusal::header(Rule::HeaderContentLength, i, named));
                }
                false
            }
            "transfer-encoding" => {
                if !value.eq_ignore_ascii_case(b"chunked") {
                    return Err(Refusal::header(Rule::HeaderTransferEncoding, i, named));
                }
                false
            }
            "connection" | "keep-alive" | "proxy-connection" | "http2-settings" | "expect" => false,
            "te" => value.eq_ignore_ascii_case(b"trailers"),
            "upgrade" | "proxy-authorization" => {
                return Err(Refusal::header(Rule::HeaderUpgradeOrProxyAuth, i, named));
            }
            l if is_override_header(l) && !up.pass_headers.iter().any(|p| p == l) => {
                return Err(Refusal::header(Rule::HeaderMethodOverride, i, named));
            }
            l if is_forwarding_header(l) && !up.pass_headers.iter().any(|p| p == l) => {
                return Err(Refusal::header(Rule::HeaderForwarding, i, named));
            }
            l if up.attached.names(l) => match up.attach_policy {
                AttachPolicy::Refuse => {
                    return Err(Refusal::header(Rule::HeaderAttached, i, named));
                }
                AttachPolicy::Override => {
                    plan.overridden += 1;
                    false
                }
            },
            "accept-encoding" => {
                plan.saw_accept_encoding = true;
                true
            }
            _ => true,
        };
        if keep {
            plan.keep.push(i);
        }
    }
    if up.attached.names("accept-encoding") {
        plan.saw_accept_encoding = true;
    }
    Ok(plan)
}
