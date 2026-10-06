//! Byte-level grammar shared by the validator and the configuration parser (RFC 9110).
//!
//! Every function here is a pure predicate over bytes. Nothing normalises.

/// RFC 9110 §5.6.2 `tchar`.
pub(crate) fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// An RFC 9110 `token` of `1..=max` bytes.
pub(crate) fn is_token(s: &[u8], max: usize) -> bool {
    !s.is_empty() && s.len() <= max && s.iter().all(|&b| is_tchar(b))
}

/// A field-value as §23.4.3 restricts it: bytes `0x21–0x7E` and `0x80–0xFF`, with `SP`/`HTAB`
/// allowed only in the interior. An empty value is a field-value (RFC 9110 §5.5). Every other
/// control byte — CR, LF and NUL included — is refused, so header injection is refused, never
/// stripped.
pub(crate) fn is_field_value(v: &[u8]) -> bool {
    let visible = |b: u8| (0x21..=0x7e).contains(&b) || b >= 0x80;
    let (Some(&first), Some(&last)) = (v.first(), v.last()) else {
        return true; // empty
    };
    visible(first) && visible(last) && v.iter().all(|&b| visible(b) || b == b' ' || b == b'\t')
}

/// A header NAME as every rule table looks it up: ASCII-lowercased, with every non-alphanumeric
/// byte folded to `-` (SPEC §23.4.3, amended M6-F3).
///
/// CGI-style servers do not see header names, they see `HTTP_*` variables: `php -S`, WSGI
/// (`wsgiref`), gunicorn before 22 and Puma before CVE-2024-45614's fix map `X_Forwarded_For`,
/// `X.Forwarded.For` and `X-Forwarded-For` onto the same `HTTP_X_FORWARDED_FOR`. A table keyed on
/// the exact name would let PHP smuggle a refused forwarding/override header, or a second value for
/// an attached credential, past the engine under one of those spellings. Folding makes every
/// spelling such a server could merge hit the same rule. The name SENT is still PHP's own bytes.
pub fn fold_name(name: &str) -> String {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                char::from(b.to_ascii_lowercase())
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Strict unsigned decimal: one or more ASCII digits, nothing else (no sign, no whitespace, no
/// `_`, no exponent). Leading zeros are accepted; overflow is an error.
pub(crate) fn parse_decimal_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    s.iter().try_fold(0u64, |acc, &d| {
        acc.checked_mul(10)?.checked_add(u64::from(d - b'0'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tchar_is_exactly_rfc9110() {
        let set: Vec<u8> = (0u8..=255).filter(|&b| is_tchar(b)).collect();
        let mut want: Vec<u8> = b"!#$%&'*+-.^_`|~".to_vec();
        want.extend(b'0'..=b'9');
        want.extend(b'A'..=b'Z');
        want.extend(b'a'..=b'z');
        want.sort_unstable();
        assert_eq!(set, want);
    }

    #[test]
    fn field_value_refuses_controls_and_edge_whitespace() {
        assert!(is_field_value(b""));
        assert!(is_field_value(b"a b\tc"));
        assert!(is_field_value(b"\x80\xff"));
        for bad in [
            &b" a"[..],
            b"a ",
            b"\ta",
            b"a\t",
            b"a\rb",
            b"a\nb",
            b"a\0b",
            b"a\x7fb",
            b" ",
        ] {
            assert!(!is_field_value(bad), "{bad:?}");
        }
    }

    #[test]
    fn decimal_is_strict() {
        assert_eq!(parse_decimal_u64(b"0"), Some(0));
        assert_eq!(parse_decimal_u64(b"007"), Some(7));
        assert_eq!(parse_decimal_u64(b"18446744073709551615"), Some(u64::MAX));
        for bad in [
            &b""[..],
            b"+1",
            b"-1",
            b" 1",
            b"1 ",
            b"1_0",
            b"18446744073709551616",
            b"0x1",
        ] {
            assert_eq!(parse_decimal_u64(bad), None, "{bad:?}");
        }
    }
}
