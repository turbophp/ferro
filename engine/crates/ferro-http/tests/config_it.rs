//! Configuration loading (SPEC §23.3.1, §23.3.2, §23.3.3) beyond single-key refusals, which
//! `tests/corpus/refuse.txt` carries.

use std::ffi::OsString;
use std::path::Path;

use ferro_http::config::{
    AttachPolicy, BreakerCounts, ConfigError, HttpConfig, HttpVersions, Reason, UpstreamEntry,
};
use ferro_http::fuzzing::fixture_read;

fn load(pairs: &[(&str, &str)]) -> HttpConfig {
    HttpConfig::load(
        pairs
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
        &fixture_read,
    )
}

fn enabled(cfg: &HttpConfig, name: &str) -> bool {
    cfg.entries()
        .any(|(n, e)| n == name && matches!(e, UpstreamEntry::Enabled(_)))
}

fn shown(cfg: &HttpConfig) -> Vec<String> {
    cfg.errors().iter().map(ToString::to_string).collect()
}

#[test]
fn defaults_are_section_23_3_1s() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "openai"),
        ("FERRO_UPSTREAM_OPENAI_ORIGIN", "https://api.openai.com"),
    ]);
    assert!(cfg.errors().is_empty(), "{:?}", shown(&cfg));
    let up = cfg.upstream_for("openai", Some(33)).unwrap();
    assert_eq!(up.allow_paths, vec!["/"]);
    assert!(up.allow_methods.is_none());
    assert!(up.allow_uids.is_none());
    assert_eq!(up.attach_policy, AttachPolicy::Refuse);
    assert!(
        up.idempotent_methods.is_empty(),
        "§23.18 Q8: empty by default"
    );
    assert_eq!(up.http, HttpVersions::H1Only);
    assert!(up.address.classes.public && !up.address.classes.private);
    assert!(!up.address.classes.loopback && !up.address.allow_metadata);
    assert_eq!(up.limits.connect_timeout_ms, 5_000);
    assert_eq!(up.limits.timeout_ms, 600_000);
    assert_eq!(up.limits.max_body_bytes, 64 * 1024 * 1024);
    assert_eq!(up.limits.h1_unsafe_reuse_max_idle_ms, 2_000);
    assert_eq!(up.limits.max_response_bytes, None);
    assert_eq!(up.breaker.counts, BreakerCounts::Connect);
    assert!(up.rate.is_none());
    assert!(up.log_route);
    assert_eq!(cfg.daemon.drain_ms, 30_000);
    assert_eq!(cfg.daemon.max_body_bytes, 256 * 1024 * 1024);
    assert!(cfg.daemon.nat64.is_empty());
    assert_eq!(cfg.daemon.slow_log_ms, None);
}

#[test]
fn no_upstreams_declared_is_an_inert_service() {
    let cfg = load(&[]);
    assert!(cfg.errors().is_empty());
    assert_eq!(cfg.entries().count(), 0);
    assert!(cfg.upstream_for("anything", Some(1)).is_none());
}

#[test]
fn rate_defaults_burst_to_the_rate() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "a,b"),
        ("FERRO_UPSTREAM_A_ORIGIN", "https://a.example"),
        ("FERRO_UPSTREAM_A_RATE_PER_SEC", "0.333"),
        ("FERRO_UPSTREAM_B_ORIGIN", "https://b.example"),
        ("FERRO_UPSTREAM_B_RATE_PER_SEC", "12.5"),
        ("FERRO_UPSTREAM_B_RATE_MAX_WAIT_MS", "250"),
    ]);
    assert!(cfg.errors().is_empty(), "{:?}", shown(&cfg));
    let a = cfg.upstream_for("a", None).unwrap().rate.clone().unwrap();
    assert_eq!((a.per_sec_milli, a.burst, a.max_wait_ms), (333, 1, 0));
    let b = cfg.upstream_for("b", None).unwrap().rate.clone().unwrap();
    assert_eq!((b.per_sec_milli, b.burst, b.max_wait_ms), (12_500, 13, 250));
}

#[test]
fn names_sharing_an_env_prefix_resolve_by_exact_key() {
    // `api` and `api_http`: FERRO_UPSTREAM_API_HTTP is api's `HTTP` key, and
    // FERRO_UPSTREAM_API_HTTP_ORIGIN is api_http's ORIGIN. Neither is ambiguous.
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "api,api_http"),
        ("FERRO_UPSTREAM_API_ORIGIN", "https://a.example"),
        ("FERRO_UPSTREAM_API_HTTP", "auto"),
        ("FERRO_UPSTREAM_API_HTTP_ORIGIN", "https://b.example"),
    ]);
    assert!(cfg.errors().is_empty(), "{:?}", shown(&cfg));
    assert_eq!(
        cfg.upstream_for("api", None).unwrap().http,
        HttpVersions::Auto
    );
    let b = cfg.upstream_for("api_http", None).unwrap();
    assert_eq!(b.origin.normalised(), "https://b.example");
    assert_eq!(b.http, HttpVersions::H1Only);
}

#[test]
fn a_variable_two_upstreams_could_own_disables_both() {
    // `x` + READ_TIMEOUT_MS and `x_read` + TIMEOUT_MS are the same variable.
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "x,x_read"),
        ("FERRO_UPSTREAM_X_ORIGIN", "https://a.example"),
        ("FERRO_UPSTREAM_X_READ_ORIGIN", "https://b.example"),
        ("FERRO_UPSTREAM_X_READ_TIMEOUT_MS", "1000"),
    ]);
    assert!(
        !enabled(&cfg, "x") && !enabled(&cfg, "x_read"),
        "{:?}",
        shown(&cfg)
    );
    assert!(cfg.errors().iter().all(|e| matches!(
        e,
        ConfigError::Upstream {
            reason: Reason::Ambiguous,
            ..
        }
    )));
    // Control: without the ambiguous variable both are enabled.
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "x,x_read"),
        ("FERRO_UPSTREAM_X_ORIGIN", "https://a.example"),
        ("FERRO_UPSTREAM_X_READ_ORIGIN", "https://b.example"),
    ]);
    assert!(enabled(&cfg, "x") && enabled(&cfg, "x_read"));
}

#[test]
fn a_typo_in_a_key_disables_rather_than_widens() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "pay"),
        ("FERRO_UPSTREAM_PAY_ORIGIN", "https://pay.example"),
        ("FERRO_UPSTREAM_PAY_ALLOW_PATH", "/v1/"),
    ]);
    assert!(!enabled(&cfg, "pay"));
    assert!(cfg.upstream_for("pay", None).is_none());
    assert_eq!(
        shown(&cfg),
        vec![
            "upstream pay: FERRO_UPSTREAM_PAY_ALLOW_PATH is not a known key; the upstream is disabled"
        ]
    );
}

#[test]
fn a_typo_in_the_name_part_is_reported_as_an_orphan() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "openai"),
        ("FERRO_UPSTREAM_OPENAI_ORIGIN", "https://api.openai.com"),
        ("FERRO_UPSTREAM_OPENIA_ALLOW_PATHS", "/v1/"),
    ]);
    assert!(enabled(&cfg, "openai"));
    assert_eq!(cfg.orphan_keys(), ["FERRO_UPSTREAM_OPENIA_ALLOW_PATHS"]);
}

#[test]
fn colliding_and_invalid_names() {
    let cfg = load(&[
        (
            "FERRO_UPSTREAMS",
            "read-replica, read.replica ,ok,_unknown,bad name,,x/y,ok",
        ),
        ("FERRO_UPSTREAM_READ_REPLICA_ORIGIN", "https://a.example"),
        ("FERRO_UPSTREAM_OK_ORIGIN", "https://ok.example"),
    ]);
    assert!(!enabled(&cfg, "read-replica") && !enabled(&cfg, "read.replica"));
    assert!(enabled(&cfg, "ok"), "a duplicate of one name is that name");
    let errs = shown(&cfg);
    assert!(
        errs.iter()
            .any(|e| e.contains("read-replica, read.replica")),
        "{errs:?}"
    );
    // Positions 4, 5 and 6 (1-based, counting non-blank entries): `_unknown`, `bad name`, `x/y`.
    for p in [4, 5, 6] {
        assert!(
            errs.iter()
                .any(|e| e.contains(&format!("entry {p} is not a valid upstream name"))),
            "{p}: {errs:?}"
        );
    }
    assert!(
        errs.iter()
            .all(|e| !e.contains("bad name") && !e.contains("x/y"))
    );
}

#[test]
fn a_daemon_key_error_disables_the_service_never_the_daemon() {
    let base = [
        ("FERRO_UPSTREAMS", "a"),
        ("FERRO_UPSTREAM_A_ORIGIN", "https://a.example"),
    ];
    for (k, v) in [
        ("FERRO_HTTP_NAT64_PREFIXES", "2001:db8::/64"),
        ("FERRO_HTTP_NAT64_PREFIXES", ","),
        ("FERRO_HTTP_DRAIN_MS", "30s"),
        ("FERRO_HTTP_MAX_BODY_BYTES", "-1"),
        ("FERRO_HTTP_SLOW_LOG_MS", "x"),
        ("FERRO_HTTP_NAT64_PREFIX", "2001:db8::/96"),
    ] {
        let mut env = base.to_vec();
        env.push((k, v));
        let cfg = load(&env);
        assert!(cfg.service_disabled(), "{k}={v}");
        assert!(cfg.upstream_for("a", None).is_none(), "{k}={v}");
        assert!(
            shown(&cfg).iter().all(|e| !e.contains(v) || v.len() < 3),
            "{k}={v}"
        );
    }
    let mut env = base.to_vec();
    env.push((
        "FERRO_HTTP_NAT64_PREFIXES",
        "2001:db8:64::/96, 2001:db8:65::/96",
    ));
    env.push(("FERRO_HTTP_DRAIN_MS", " "));
    let cfg = load(&env);
    assert!(!cfg.service_disabled(), "{:?}", shown(&cfg));
    assert_eq!(cfg.daemon.nat64.len(), 2);
    assert_eq!(cfg.daemon.drain_ms, 30_000, "a blank value reads as unset");
    assert!(cfg.upstream_for("a", None).is_some());
}

#[test]
fn a_declared_nat64_prefix_reaches_the_origin_literal_check() {
    let cfg = load(&[
        ("FERRO_HTTP_NAT64_PREFIXES", "2001:db8:64::/96"),
        ("FERRO_UPSTREAMS", "a"),
        ("FERRO_UPSTREAM_A_ORIGIN", "http://[2001:db8:64::a9fe:a9fe]"),
    ]);
    assert!(
        !enabled(&cfg, "a"),
        "169.254.169.254 behind a declared NAT64 prefix"
    );
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "a"),
        ("FERRO_UPSTREAM_A_ORIGIN", "http://[2001:db8:64::a9fe:a9fe]"),
    ]);
    assert!(
        enabled(&cfg, "a"),
        "control: undeclared, it is plain public IPv6"
    );
}

#[test]
fn uid_gating_is_indistinguishable_from_an_unknown_upstream() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "internal"),
        ("FERRO_UPSTREAM_INTERNAL_ORIGIN", "https://i.example"),
        ("FERRO_UPSTREAM_INTERNAL_ALLOW_UIDS", "2000"),
    ]);
    assert!(cfg.upstream_for("internal", Some(2000)).is_some());
    assert!(cfg.upstream_for("internal", Some(1000)).is_none());
    assert!(cfg.upstream_for("internal", None).is_none());
    assert!(cfg.upstream_for("nope", Some(2000)).is_none());
}

#[test]
fn attached_values_never_reach_debug_or_errors() {
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "pay,broken"),
        ("FERRO_UPSTREAM_PAY_ORIGIN", "https://pay.example"),
        ("FERRO_UPSTREAM_PAY_ATTACH_HEADERS_FILE", "/fixture/attach"),
        (
            "FERRO_UPSTREAM_BROKEN_ORIGIN",
            "https://user:FIXTURE-CANARY-SECRET@pay.example",
        ),
    ]);
    let all = format!("{cfg:?} {:?}", shown(&cfg));
    assert!(!all.contains("FIXTURE-CANARY-SECRET"), "{all}");
    let up = cfg.upstream_for("pay", None).unwrap();
    let names: Vec<&str> = up.attached.iter().map(|h| h.name()).collect();
    assert_eq!(names, ["Authorization", "X-Api-Key"]);
    assert_eq!(
        up.attached.iter().next().unwrap().value().expose_for_wire(),
        b"Bearer FIXTURE-CANARY-SECRET"
    );
}

#[test]
fn an_unreadable_or_oversize_attach_file_disables_the_upstream() {
    let big = vec![b'a'; ferro_http::attach::MAX_FILE_BYTES + 1];
    let read = move |p: &Path| {
        if p == Path::new("/big") {
            Ok(big.clone())
        } else {
            Err(std::io::ErrorKind::PermissionDenied.into())
        }
    };
    for path in ["/big", "/denied"] {
        let cfg = HttpConfig::load(
            [
                ("FERRO_UPSTREAMS", "a"),
                ("FERRO_UPSTREAM_A_ORIGIN", "https://a.example"),
                ("FERRO_UPSTREAM_A_ATTACH_HEADERS_FILE", path),
            ]
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
            &read,
        );
        assert!(!enabled(&cfg, "a"), "{path}");
        assert!(
            shown(&cfg).iter().all(|e| !e.contains(path)),
            "the path is not echoed"
        );
    }
}

#[test]
fn non_utf8_values_disable() {
    use std::os::unix::ffi::OsStringExt;
    let cfg = HttpConfig::load(
        [
            (OsString::from("FERRO_UPSTREAMS"), OsString::from("a")),
            (
                OsString::from("FERRO_UPSTREAM_A_ORIGIN"),
                OsString::from_vec(b"https://a.example\xff".to_vec()),
            ),
        ],
        &fixture_read,
    );
    assert!(!enabled(&cfg, "a"));
    assert!(shown(&cfg)[0].contains("is not valid UTF-8"));
}

#[test]
fn isolation_void_names_upstreams_with_credential_material_only() {
    // Credential material: attached headers, or (M6-F5c) a client key. A CA_FILE is not.
    let cfg = load(&[
        ("FERRO_UPSTREAMS", "pay,plain,narrow,mtls,ca"),
        ("FERRO_UPSTREAM_MTLS_ORIGIN", "https://m.example"),
        ("FERRO_UPSTREAM_MTLS_CLIENT_CERT_FILE", "/fixture/cert"),
        ("FERRO_UPSTREAM_MTLS_CLIENT_KEY_FILE", "/fixture/key"),
        ("FERRO_UPSTREAM_CA_ORIGIN", "https://ca.example"),
        ("FERRO_UPSTREAM_CA_CA_FILE", "/fixture/ca"),
        ("FERRO_UPSTREAM_PAY_ORIGIN", "https://pay.example"),
        ("FERRO_UPSTREAM_PAY_ATTACH_HEADERS_FILE", "/fixture/attach"),
        ("FERRO_UPSTREAM_PLAIN_ORIGIN", "https://plain.example"),
        ("FERRO_UPSTREAM_NARROW_ORIGIN", "https://n.example"),
        (
            "FERRO_UPSTREAM_NARROW_ATTACH_HEADERS_FILE",
            "/fixture/attach",
        ),
        ("FERRO_UPSTREAM_NARROW_ALLOW_UIDS", "33,999"),
    ]);
    let own = 999;
    // FERRO_ALLOW_UIDS empty → only the daemon's own uid connects: void for every attached set.
    assert_eq!(cfg.isolation_void(&[], own), ["mtls", "narrow", "pay"]);
    // ferrod's own uid listed daemon-wide.
    assert_eq!(
        cfg.isolation_void(&[33, own], own),
        ["mtls", "narrow", "pay"]
    );
    // A proper split, but one upstream lists ferrod's uid.
    assert_eq!(cfg.isolation_void(&[33], own), ["narrow"]);
}

/// §23.7.2: effective idempotency is a declaration — the caller's, the operator's methods, or the
/// operator's key header carried non-empty — and never the method alone.
#[test]
fn effective_idempotency_is_a_declaration_never_the_method() {
    use ferro_http::fuzzing::{FIXTURE_UID, fixture_config};
    use ferro_http::{Request, validate};
    let cfg = fixture_config();
    let go = |up: &str, method: &str, headers: &[(String, Vec<u8>)], declared: Option<bool>| {
        let target = if up == "api" { "/api/x" } else { "/" };
        validate(
            cfg,
            Some(FIXTURE_UID),
            &Request {
                upstream: up,
                method,
                target,
                origin: None,
                headers,
                body: None,
                idempotent: declared,
                decode: false,
            },
        )
        .unwrap()
        .idempotent
    };
    let key = |v: &[u8]| vec![("idempotency-key".to_string(), v.to_vec())];
    // `root` declares nothing: a GET is NOT idempotent by its method.
    assert!(!go("root", "GET", &[], None));
    assert!(!go("root", "HEAD", &[], None));
    assert!(
        go("root", "POST", &[], Some(true)),
        "(a) the caller's declaration"
    );
    // `api`: IDEMPOTENT_METHODS=GET, IDEMPOTENCY_KEY_HEADER=Idempotency-Key.
    assert!(go("api", "GET", &[], None), "(b) the operator's methods");
    assert!(!go("api", "POST", &[], None));
    assert!(
        go("api", "POST", &key(b"k-1"), None),
        "(c) the operator's key, non-empty"
    );
    assert!(
        !go("api", "POST", &key(b""), None),
        "an empty key is not a key"
    );
    assert!(
        !go("root", "POST", &key(b"k-1"), None),
        "the key licenses only where declared"
    );
    assert!(!go("api", "GET", &[], Some(false)), "false downgrades (b)");
    assert!(
        !go("api", "POST", &key(b"k-1"), Some(false)),
        "false downgrades (c)"
    );
    // Methods are case-sensitive (RFC 9110): IDEMPOTENT_METHODS=GET does not cover `patch`.
    assert!(!go("api", "patch", &[], None));
}

/// An attached header may not be the idempotency key: with one constant, operator-supplied key on
/// every request, every request would be declared idempotent (found by mutation C9, which no
/// single-key corpus line could reach).
#[test]
fn an_attached_header_cannot_be_the_idempotency_key() {
    let with = |key: &str| {
        load(&[
            ("FERRO_UPSTREAMS", "pay"),
            ("FERRO_UPSTREAM_PAY_ORIGIN", "https://pay.example"),
            ("FERRO_UPSTREAM_PAY_ATTACH_HEADERS_FILE", "/fixture/attach"),
            ("FERRO_UPSTREAM_PAY_IDEMPOTENCY_KEY_HEADER", key),
        ])
    };
    for attached in ["Authorization", "x-api-key"] {
        let cfg = with(attached);
        assert!(!enabled(&cfg, "pay"), "{attached}");
        assert!(cfg.errors().iter().any(|e| matches!(
            e,
            ConfigError::Upstream { key, reason: Reason::KeyHeaderReserved, .. }
                if key == "IDEMPOTENCY_KEY_HEADER"
        )));
    }
    assert!(
        enabled(&with("Idempotency-Key"), "pay"),
        "control: an unattached key"
    );
}

/// The remedy §23.8.5's amendment names for a local-use NAT64 `/96` deployment: undeclared, every
/// address is refused (the /48 reading is 0.0.0.0); declared, the prefix decides alone.
#[test]
fn a_declared_local_use_nat64_prefix_decides_alone() {
    use ferro_http::address::{AddressPolicy, ClassSet, Nat64Prefixes};
    let policy = AddressPolicy {
        classes: ClassSet::PUBLIC_ONLY,
        allow_metadata: false,
        literal: None,
    };
    let a = "64:ff9b:1::808:808".parse().unwrap();
    assert!(policy.check(a, &Nat64Prefixes::default()).is_err());
    let declared = Nat64Prefixes::new(vec![Nat64Prefixes::parse_entry("64:ff9b:1::/96").unwrap()]);
    assert_eq!(policy.check(a, &declared), Ok(()));
    let meta = "64:ff9b:1::a9fe:a9fe".parse().unwrap();
    assert!(
        policy.check(meta, &declared).is_err(),
        "and still refuses what it carries"
    );
}

/// `decode` asks for `Accept-Encoding` only when PHP set none (§23.4.3, §23.9.2).
#[test]
fn accept_encoding_is_added_only_when_absent() {
    use ferro_http::fuzzing::{FIXTURE_UID, fixture_config};
    use ferro_http::{Request, validate};
    let cfg = fixture_config();
    let go = |headers: &[(String, Vec<u8>)], decode: bool| {
        validate(
            cfg,
            Some(FIXTURE_UID),
            &Request {
                upstream: "root",
                method: "GET",
                target: "/",
                origin: None,
                headers,
                body: None,
                idempotent: None,
                decode,
            },
        )
        .unwrap()
        .add_accept_encoding
    };
    let ae = vec![("Accept-Encoding".to_string(), b"br".to_vec())];
    assert!(go(&[], true));
    assert!(!go(&[], false));
    assert!(!go(&ae, true));
}

fn one_upstream(
    extra: &[(&str, &str)],
    read: &dyn Fn(&Path) -> std::io::Result<Vec<u8>>,
) -> HttpConfig {
    let mut env = vec![
        ("FERRO_UPSTREAMS", "u"),
        ("FERRO_UPSTREAM_U_ORIGIN", "https://u.example"),
    ];
    env.extend_from_slice(extra);
    HttpConfig::load(
        env.iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v))),
        read,
    )
}

fn validate_u<'c>(
    cfg: &'c HttpConfig,
    headers: &[(String, Vec<u8>)],
    body: Option<&[u8]>,
    decode: bool,
) -> Result<ferro_http::Validated<'c>, ferro_http::Refusal> {
    ferro_http::validate(
        cfg,
        None,
        &ferro_http::Request {
            upstream: "u",
            method: "POST",
            target: "/",
            origin: None,
            headers,
            body,
            idempotent: None,
            decode,
        },
    )
}

/// Review mutation B: an attached `Accept-Encoding` suppresses the decode-only one (§23.4.4
/// amendment), otherwise the request would carry two.
#[test]
fn an_attached_accept_encoding_suppresses_the_decode_only_one() {
    let read = |_: &Path| Ok(b"Accept-Encoding: identity\n".to_vec());
    let cfg = one_upstream(&[("FERRO_UPSTREAM_U_ATTACH_HEADERS_FILE", "/f")], &read);
    assert!(cfg.errors().is_empty(), "{:?}", shown(&cfg));
    assert!(
        !validate_u(&cfg, &[], None, true)
            .unwrap()
            .add_accept_encoding
    );
    // Control: the same upstream without the attached header asks for decoding.
    let cfg = one_upstream(&[], &fixture_read);
    assert!(
        validate_u(&cfg, &[], None, true)
            .unwrap()
            .add_accept_encoding
    );
}

/// Review mutation V: the daemon-wide FERRO_HTTP_MAX_BODY_BYTES half of "the smaller of the two".
#[test]
fn the_daemon_body_budget_bounds_a_body_too() {
    let cfg = one_upstream(&[("FERRO_HTTP_MAX_BODY_BYTES", "10")], &fixture_read);
    assert!(cfg.errors().is_empty());
    assert!(validate_u(&cfg, &[], Some(&[0u8; 10]), false).is_ok());
    assert_eq!(
        validate_u(&cfg, &[], Some(&[0u8; 11]), false)
            .map(|_| ())
            .unwrap_err()
            .rule,
        ferro_http::Rule::BodyTooLarge
    );
}

/// Review mutation U: a BLANK unknown daemon key is still an unknown key (a typo is a typo).
#[test]
fn a_blank_unknown_daemon_key_still_disables() {
    let cfg = one_upstream(&[("FERRO_HTTP_BOGUS", "")], &fixture_read);
    assert!(cfg.service_disabled());
    let cfg = one_upstream(&[("FERRO_HTTP_DRAIN_MS", "")], &fixture_read);
    assert!(
        !cfg.service_disabled(),
        "control: a blank KNOWN key reads as unset"
    );
}

/// Review mutation A: the real reader caps one byte PAST the limit, so an oversize file is refused
/// rather than truncated to a file that parses.
#[test]
fn read_capped_refuses_an_oversize_file_instead_of_truncating_it() {
    let dir = std::env::temp_dir().join(format!("ferro-http-f3-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let max = ferro_http::attach::MAX_FILE_BYTES;
    let line = |len: usize| {
        let mut v = b"X-A: ".to_vec();
        v.resize(len, b'v');
        v
    };
    let over = dir.join("over");
    std::fs::write(&over, line(max + 1)).unwrap();
    let exact = dir.join("exact");
    std::fs::write(&exact, line(max)).unwrap();
    let path = |p: &Path| p.to_str().unwrap().to_string();
    let cfg = one_upstream(
        &[("FERRO_UPSTREAM_U_ATTACH_HEADERS_FILE", &path(&over))],
        &ferro_http::config::read_capped,
    );
    assert!(
        !enabled(&cfg, "u"),
        "an oversize file must not be truncated into acceptance"
    );
    let cfg = one_upstream(
        &[("FERRO_UPSTREAM_U_ATTACH_HEADERS_FILE", &path(&exact))],
        &ferro_http::config::read_capped,
    );
    assert!(
        enabled(&cfg, "u"),
        "control: a file of exactly the cap: {:?}",
        shown(&cfg)
    );
    std::fs::remove_dir_all(&dir).ok();
    // from_env reads the real environment, which declares no upstream under `cargo test`.
    let env_cfg = HttpConfig::from_env();
    assert!(env_cfg.errors().is_empty() || std::env::var_os("FERRO_UPSTREAMS").is_some());
}

/// Review F-6: a request's Debug never shows a header value, the target or the body.
#[test]
fn a_request_debug_shows_names_never_values() {
    let headers = vec![(
        "Authorization".to_string(),
        b"Bearer END-USER-SECRET".to_vec(),
    )];
    let req = ferro_http::Request {
        upstream: "u",
        method: "POST",
        target: "/users/TARGET-SECRET?token=QUERY-SECRET",
        origin: Some("https://u.example"),
        headers: &headers,
        body: Some(b"BODY-SECRET"),
        idempotent: None,
        decode: false,
    };
    let shown = format!("{req:?} {req:#?}");
    for secret in [
        "END-USER-SECRET",
        "TARGET-SECRET",
        "QUERY-SECRET",
        "BODY-SECRET",
    ] {
        assert!(!shown.contains(secret), "{secret} in {shown}");
    }
    assert!(shown.contains("Authorization"), "names are shown: {shown}");
}

/// Review F-1, at the plan level: under ATTACH_POLICY=override a folded spelling of an attached
/// name is REPLACED (counted), never sent beside the daemon's value.
#[test]
fn a_folded_spelling_of_an_attached_name_is_overridden() {
    let cfg = ferro_http::fuzzing::fixture_config();
    for name in ["X_Api_Key", "x.api.key", "X-API_KEY"] {
        let headers = vec![(name.to_string(), b"PHP".to_vec())];
        let v = ferro_http::validate(
            cfg,
            Some(ferro_http::fuzzing::FIXTURE_UID),
            &ferro_http::Request {
                upstream: "override",
                method: "GET",
                target: "/",
                origin: None,
                headers: &headers,
                body: None,
                idempotent: None,
                decode: false,
            },
        )
        .unwrap();
        assert_eq!((v.send_headers.len(), v.overridden), (0, 1), "{name}");
    }
}

/// The idempotency key is matched EXACTLY (case-insensitively), never folded: folding it would
/// license a request whose `Idempotency_Key` a non-folding upstream never sees.
#[test]
fn the_idempotency_key_is_not_folded() {
    let cfg = ferro_http::fuzzing::fixture_config();
    let go = |name: &str| {
        let headers = vec![(name.to_string(), b"k-1".to_vec())];
        ferro_http::validate(
            cfg,
            Some(ferro_http::fuzzing::FIXTURE_UID),
            &ferro_http::Request {
                upstream: "api",
                method: "POST",
                target: "/api/x",
                origin: None,
                headers: &headers,
                body: None,
                idempotent: None,
                decode: false,
            },
        )
        .unwrap()
        .idempotent
    };
    assert!(go("Idempotency-Key"));
    assert!(!go("Idempotency_Key"));
    assert!(!go("idempotency.key"));
}
