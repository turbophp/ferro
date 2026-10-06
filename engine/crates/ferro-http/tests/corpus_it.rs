//! Runs `tests/corpus/refuse.txt` and `tests/corpus/accept.txt` (SPEC §23.4; chaos case 13's
//! offline half). See the header of `refuse.txt` for the format.
//!
//! Beyond each line's own verdict, this asserts COVERAGE: every validator [`Rule`], every
//! [`OriginError`] and every address-guard label is exercised by at least one refusal line, so
//! deleting a rule's check cannot leave the corpus silently green.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::net::IpAddr;
use std::path::Path;

use ferro_http::address::{AddressPolicy, AddressRefusal, ClassSet, RefusedRange};
use ferro_http::config::{ConfigError, HttpConfig, UpstreamEntry};
use ferro_http::fuzzing::{FIXTURE_UID, fixture_config, fixture_read};
use ferro_http::origin::{Origin, OriginError};
use ferro_http::{Refusal, Request, Rule, Validated, validate};

/// Decode `\xHH`, `\s`, `\\` and `\e` (nothing: an empty field or part of one).
fn unescape(s: &str) -> Vec<u8> {
    if s == "\\e" {
        return Vec::new();
    }
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            match b.get(i + 1) {
                Some(b'x') => {
                    let h = std::str::from_utf8(&b[i + 2..i + 4]).unwrap();
                    out.push(u8::from_str_radix(h, 16).unwrap());
                    i += 4;
                }
                Some(b's') => {
                    out.push(b' ');
                    i += 2;
                }
                Some(b'\\') => {
                    out.push(b'\\');
                    i += 2;
                }
                Some(b'e') => i += 2,
                other => panic!("bad escape {other:?} in {s}"),
            }
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

fn text(s: &str) -> String {
    String::from_utf8(unescape(s)).expect("corpus input is valid UTF-8 for a `str` field")
}

struct Line {
    no: usize,
    kind: String,
    ctx: String,
    input: String,
    expect: String,
}

fn load(file: &str) -> Vec<Line> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus")
        .join(file);
    let body = std::fs::read_to_string(&path).unwrap();
    body.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|(i, l)| {
            let f: Vec<&str> = l.split_whitespace().collect();
            assert_eq!(f.len(), 4, "{file}:{}: want 4 fields: {l}", i + 1);
            Line {
                no: i + 1,
                kind: f[0].into(),
                ctx: f[1].into(),
                input: f[2].into(),
                expect: f[3].into(),
            }
        })
        .collect()
}

fn default_target(up: &str) -> &'static str {
    match up {
        "api" => "/api/x",
        "params" => "/x",
        "utf8" => "/w/x",
        _ => "/",
    }
}

enum Outcome<'c> {
    Validated(Result<Validated<'c>, Refusal>, Option<(String, Vec<u8>)>),
    Origin(Result<Origin, OriginError>),
    Address(Result<(), AddressRefusal>),
    Config(HttpConfig, String, String),
}

fn run(l: &Line) -> Outcome<'static> {
    let cfg = fixture_config();
    let req = |up: &str,
               method: &str,
               target: &str,
               origin: Option<&str>,
               headers: &[(String, Vec<u8>)],
               body: Option<&[u8]>,
               uid: Option<u32>| {
        validate(
            cfg,
            uid,
            &Request {
                upstream: up,
                method,
                target,
                origin,
                headers,
                body,
                idempotent: None,
                decode: false,
            },
        )
    };
    let up = l.ctx.as_str();
    let uid = Some(FIXTURE_UID);
    match l.kind.as_str() {
        "target" => Outcome::Validated(req(up, "GET", &text(&l.input), None, &[], None, uid), None),
        "method" => Outcome::Validated(
            req(
                up,
                &text(&l.input),
                default_target(up),
                None,
                &[],
                None,
                uid,
            ),
            None,
        ),
        "header" => {
            let raw = unescape(&l.input);
            let eq = raw.iter().position(|&b| b == b'=').expect("name=value");
            let h = (
                String::from_utf8(raw[..eq].to_vec()).unwrap(),
                raw[eq + 1..].to_vec(),
            );
            let headers = vec![h.clone()];
            Outcome::Validated(
                req(up, "GET", default_target(up), None, &headers, None, uid),
                Some(h),
            )
        }
        "origin-field" => Outcome::Validated(
            req(
                up,
                "GET",
                default_target(up),
                Some(&text(&l.input)),
                &[],
                None,
                uid,
            ),
            None,
        ),
        "upstream" => {
            let uid = match l.input.as_str() {
                "none" => None,
                n => Some(n.parse().unwrap()),
            };
            let name = text(&l.ctx);
            Outcome::Validated(req(&name, "GET", "/", None, &[], None, uid), None)
        }
        "body" => {
            let body = vec![b'b'; l.input.parse().unwrap()];
            Outcome::Validated(
                req(up, "POST", default_target(up), None, &[], Some(&body), uid),
                None,
            )
        }
        "origin" => Outcome::Origin(Origin::parse(&text(&l.input))),
        "address" => {
            let policy = if up == "meta" {
                AddressPolicy {
                    classes: ClassSet::PUBLIC_ONLY,
                    allow_metadata: true,
                    literal: None,
                }
            } else {
                let mut c = ClassSet {
                    public: false,
                    private: false,
                    loopback: false,
                };
                for x in up.split(',') {
                    match x {
                        "public" => c.public = true,
                        "private" => c.private = true,
                        "loopback" => c.loopback = true,
                        _ => panic!("bad class {x}"),
                    }
                }
                AddressPolicy {
                    classes: c,
                    allow_metadata: false,
                    literal: None,
                }
            };
            let a: IpAddr = l.input.parse().unwrap();
            Outcome::Address(policy.check(a, &cfg.daemon.nat64))
        }
        "config" => {
            let kv = text(&l.input);
            let (k, v) = kv.split_once('=').expect("KEY=VALUE");
            let base = match up {
                "https" => "https://cfg.example.com",
                "http" => "http://cfg.example.com",
                _ => panic!("config context is https or http"),
            };
            let cfg = config_with(base, k, v);
            Outcome::Config(cfg, k.to_string(), v.to_string())
        }
        k => panic!("unknown kind {k}"),
    }
}

fn config_with(origin: &str, key: &str, value: &str) -> HttpConfig {
    let mut env = vec![
        ("FERRO_UPSTREAMS".to_string(), "cfg".to_string()),
        ("FERRO_UPSTREAM_CFG_ORIGIN".to_string(), origin.to_string()),
    ];
    let full = format!("FERRO_UPSTREAM_CFG_{key}");
    env.retain(|(k, _)| *k != full);
    env.push((full, value.to_string()));
    let read = |p: &Path| {
        if p == Path::new("/fixture/bad-attach") {
            Ok(b"Host: evil.example\n".to_vec())
        } else {
            fixture_read(p)
        }
    };
    HttpConfig::load(
        env.into_iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
        &read,
    )
}

/// A refusal's message may name the rule, a byte offset and a header NAME — never a value.
fn assert_no_echo(r: &Refusal, value: &[u8], l: &Line) {
    let shown = r.to_string();
    let bare = Refusal {
        rule: r.rule,
        offset: None,
        header: None,
    }
    .to_string();
    if value.len() >= 6 && !bare.as_bytes().windows(value.len()).any(|w| w == value) {
        assert!(
            !shown.as_bytes().windows(value.len()).any(|w| w == value),
            "line {}: the refusal message quotes the input: {shown}",
            l.no
        );
    }
}

#[test]
fn every_refusal_line_is_refused_by_its_rule() {
    let lines = load("refuse.txt");
    assert!(lines.len() > 300, "the corpus lost lines: {}", lines.len());
    let mut rules: BTreeSet<&'static str> = BTreeSet::new();
    let mut origin_errors: BTreeSet<String> = BTreeSet::new();
    let mut labels: BTreeSet<&'static str> = BTreeSet::new();
    for l in &lines {
        match run(l) {
            Outcome::Validated(r, header) => {
                let refusal = match r {
                    Err(e) => e,
                    Ok(v) => panic!("refuse.txt:{}: ACCEPTED {} {}: {v:?}", l.no, l.ctx, l.input),
                };
                assert_eq!(
                    refusal.rule.name(),
                    l.expect,
                    "refuse.txt:{}: {} {} refused by the wrong rule ({refusal})",
                    l.no,
                    l.ctx,
                    l.input
                );
                if refusal.rule == Rule::UpstreamUnavailable {
                    assert_eq!(refusal.to_string(), "upstream not available to this peer");
                }
                match &header {
                    Some((_, v)) => assert_no_echo(&refusal, v, l),
                    None => assert_no_echo(&refusal, &unescape(&l.input), l),
                }
                rules.insert(refusal.rule.name());
            }
            Outcome::Origin(r) => {
                let e = r.expect_err(&format!("refuse.txt:{}: ORIGIN {} accepted", l.no, l.input));
                assert_eq!(
                    format!("{e:?}"),
                    l.expect,
                    "refuse.txt:{}: {}",
                    l.no,
                    l.input
                );
                let input = text(&l.input);
                if input.len() >= 6 {
                    assert!(!e.to_string().contains(&input));
                }
                origin_errors.insert(format!("{e:?}"));
            }
            Outcome::Address(r) => {
                let e = r.expect_err(&format!("refuse.txt:{}: {} admitted", l.no, l.input));
                assert_eq!(e.label(), l.expect, "refuse.txt:{}: {}", l.no, l.input);
                labels.insert(e.label());
            }
            Outcome::Config(cfg, key, value) => {
                let errs: Vec<&ConfigError> = cfg.errors();
                let disabled =
                    matches!(cfg.entries().next(), Some((_, UpstreamEntry::Disabled(_))));
                assert!(
                    disabled,
                    "refuse.txt:{}: {key}={value} left the upstream enabled",
                    l.no
                );
                assert!(
                    errs.iter().any(
                        |e| matches!(e, ConfigError::Upstream { key: k, .. } if *k == l.expect)
                    ),
                    "refuse.txt:{}: no error names {}: {:?}",
                    l.no,
                    l.expect,
                    errs.iter().map(ToString::to_string).collect::<Vec<_>>()
                );
                assert!(
                    cfg.upstream_for("cfg", Some(FIXTURE_UID)).is_none(),
                    "a disabled upstream must be refused"
                );
                for e in &errs {
                    let shown = e.to_string();
                    // A value that is a word of the fixed sentence ("upgrade") is not an echo.
                    let fixed = match e {
                        ConfigError::Upstream { reason, .. } => reason.to_string(),
                        _ => String::new(),
                    };
                    if value.len() >= 5 && !fixed.contains(&value) {
                        assert!(
                            !shown.contains(&value),
                            "refuse.txt:{}: the error quotes the value: {shown}",
                            l.no
                        );
                    }
                }
            }
        }
    }
    // Rules no single short corpus line can reach.
    rules.extend(synthesized_refusals());
    let missing: Vec<&str> = Rule::ALL
        .iter()
        .map(|r| r.name())
        .filter(|n| !rules.contains(n))
        .collect();
    assert!(
        missing.is_empty(),
        "rules the corpus never exercises: {missing:?}"
    );
    let missing: Vec<String> = OriginError::ALL
        .iter()
        .map(|e| format!("{e:?}"))
        .filter(|e| !origin_errors.contains(e))
        .collect();
    assert!(
        missing.is_empty(),
        "ORIGIN refusals never exercised: {missing:?}"
    );
    let mut all_labels: Vec<&str> = RefusedRange::ALL.iter().map(|r| r.label()).collect();
    all_labels.extend(["public", "private", "loopback"]);
    let missing: Vec<&&str> = all_labels
        .iter()
        .filter(|l| !labels.contains(**l))
        .collect();
    assert!(
        missing.is_empty(),
        "address labels never exercised: {missing:?}"
    );
}

/// Refusals whose inputs are too long for a corpus line.
fn synthesized_refusals() -> Vec<&'static str> {
    let cfg = fixture_config();
    let go = |target: &str, headers: &[(String, Vec<u8>)]| {
        validate(
            cfg,
            Some(FIXTURE_UID),
            &Request {
                upstream: "root",
                method: "GET",
                target,
                origin: None,
                headers,
                body: None,
                idempotent: None,
                decode: false,
            },
        )
    };
    let long = format!("/{}", "a".repeat(8192));
    let at_limit = format!("/{}", "a".repeat(8191));
    let r1 = go(&long, &[]).unwrap_err().rule;
    assert_eq!(r1, Rule::TargetLength);
    go(&at_limit, &[]).expect("control: 8192 bytes is accepted");

    let many: Vec<(String, Vec<u8>)> = (0..101)
        .map(|i| (format!("x-{i}"), b"v".to_vec()))
        .collect();
    assert_eq!(go("/", &many).unwrap_err().rule, Rule::HeaderCount);
    go("/", &many[..100]).expect("control: 100 lines are accepted");

    let big = vec![("x-big".to_string(), vec![b'v'; 64 * 1024 - 5 + 1])];
    assert_eq!(go("/", &big).unwrap_err().rule, Rule::HeaderSize);
    let fits = vec![("x-big".to_string(), vec![b'v'; 64 * 1024 - 5])];
    go("/", &fits).expect("control: exactly 64 KiB of names plus values is accepted");

    let long_name = vec![("x".repeat(257), b"v".to_vec())];
    assert_eq!(go("/", &long_name).unwrap_err().rule, Rule::HeaderName);
    go("/", &[("x".repeat(256), b"v".to_vec())]).expect("control: a 256-byte name");
    vec![
        Rule::TargetLength.name(),
        Rule::HeaderCount.name(),
        Rule::HeaderSize.name(),
    ]
}

#[test]
fn every_accept_line_is_accepted() {
    let lines = load("accept.txt");
    assert!(lines.len() > 150, "the corpus lost lines: {}", lines.len());
    for l in &lines {
        match run(l) {
            Outcome::Validated(r, header) => {
                let v = r.unwrap_or_else(|e| {
                    panic!(
                        "accept.txt:{}: REFUSED {} {}: {} ({e})",
                        l.no,
                        l.ctx,
                        l.input,
                        e.rule.name()
                    )
                });
                if let Some(_h) = header {
                    let got = match (v.send_headers.as_slice(), v.overridden) {
                        ([0], 0) => "kept",
                        ([], 0) => "dropped",
                        ([], 1) => "overridden",
                        other => panic!("accept.txt:{}: unexpected plan {other:?}", l.no),
                    };
                    assert_eq!(got, l.expect, "accept.txt:{}: {}", l.no, l.input);
                } else {
                    assert_eq!(l.expect, "accept", "accept.txt:{}", l.no);
                }
                assert_eq!(v.host, v.upstream.origin.authority());
            }
            Outcome::Origin(r) => {
                let o =
                    r.unwrap_or_else(|e| panic!("accept.txt:{}: {} refused: {e}", l.no, l.input));
                assert_eq!(o.normalised(), l.expect, "accept.txt:{}", l.no);
            }
            Outcome::Address(r) => {
                assert_eq!(r, Ok(()), "accept.txt:{}: {}", l.no, l.input);
                assert_eq!(l.expect, "accept");
            }
            Outcome::Config(cfg, key, value) => {
                assert!(
                    cfg.errors().is_empty(),
                    "accept.txt:{}: {key}={value}: {:?}",
                    l.no,
                    cfg.errors()
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                );
                assert!(cfg.upstream_for("cfg", Some(FIXTURE_UID)).is_some());
                assert_eq!(l.expect, "enabled");
            }
        }
    }
}

#[test]
fn the_config_base_is_enabled() {
    // The control every `config` line is measured against.
    for base in ["https://cfg.example.com", "http://cfg.example.com"] {
        let cfg = config_with(base, "LOG_ROUTE", "1");
        assert!(cfg.errors().is_empty());
        assert!(cfg.upstream_for("cfg", None).is_some());
    }
}
