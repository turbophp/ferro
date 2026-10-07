//! Ferro HTTP configuration (SPEC §23.3.1): `FERRO_UPSTREAMS=<n1>,<n2>` plus
//! `FERRO_UPSTREAM_<NAME>_<KEY>`, and the daemon-wide `FERRO_HTTP_*` keys.
//!
//! The rules, from §23.3.1, with this slice's decisions (SPEC §22.2 (cw)):
//!
//! - **A blank value reads as unset** (D14's `ALLOW_DIR` rule).
//! - **An unparseable value disables that upstream, never the daemon.** A disabled upstream is
//!   still listed, so requests to it are refused exactly as an unknown name is (D15). An
//!   unparseable *daemon-wide* key disables the whole HTTP service the same way.
//! - **An unknown key disables its upstream.** A typo is not cosmetic here:
//!   `…_ALLOW_PATH=/api` (no `S`) would otherwise leave `ALLOW_PATHS` at its default `/` and
//!   silently widen what PHP can reach. Keys are matched exactly against the known key list, so
//!   two upstreams whose env names share a prefix (`api`, `api_http`) still resolve; a variable
//!   that two upstreams could both own is ambiguous and disables both.
//! - **Configuration is read once, at start.** There is no reload in v1.
//!
//! Errors ([`ConfigError`]) name the upstream and the key and say what rule failed. They never
//! quote a value — a value can be a credential path, a URL with userinfo, or a header from the
//! attached file (§12, §23.3.2).

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fmt;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use crate::address::{AddressPolicy, AddressRefusal, ClassSet, Nat64Prefixes};
use crate::attach::{self, AttachError, AttachedHeaders, is_reserved_attach_name};
use crate::origin::{Origin, OriginError, Scheme};
use crate::syntax::{fold_name, is_token, parse_decimal_u64};
use crate::validate::{
    Refusal, check_prefix, is_always_refused_method, is_forwarding_header, is_override_header,
};

pub const UPSTREAMS_VAR: &str = "FERRO_UPSTREAMS";
pub const UPSTREAM_PREFIX: &str = "FERRO_UPSTREAM_";
pub const HTTP_PREFIX: &str = "FERRO_HTTP_";

/// Every per-upstream key of §23.3.1's table.
pub const UPSTREAM_KEYS: &[&str] = &[
    "ORIGIN",
    "ALLOW_PATHS",
    "PATH_PARAMS",
    "PATH_ENCODING",
    "ALLOW_METHODS",
    "ALLOW_UIDS",
    "ATTACH_HEADERS_FILE",
    "ATTACH_POLICY",
    "PASS_HEADERS",
    "IDEMPOTENT_METHODS",
    "IDEMPOTENCY_KEY_HEADER",
    "HTTP",
    "CA_FILE",
    "CLIENT_CERT_FILE",
    "CLIENT_KEY_FILE",
    "MIN_TLS",
    "ADDRESS_CLASSES",
    "ALLOW_METADATA",
    "PARTITION",
    "CONNECT_TIMEOUT_MS",
    "TIMEOUT_MS",
    "READ_TIMEOUT_MS",
    "MAX_CONNECTIONS",
    "MAX_REQUESTS",
    "MAX_QUEUED",
    "MAX_BODY_BYTES",
    "QUEUE_TIMEOUT_MS",
    "MAX_DIALS",
    "IDLE_TIMEOUT_MS",
    "H1_UNSAFE_REUSE_MAX_IDLE_MS",
    "MAX_LIFETIME_MS",
    "DNS_TTL_MS",
    "MAX_RESPONSE_BYTES",
    "BREAKER_FAILURES",
    "BREAKER_COUNTS",
    "BREAKER_OPEN_MS",
    "RATE_PER_SEC",
    "RATE_BURST",
    "RATE_MAX_WAIT_MS",
    "HONOR_RETRY_AFTER",
    "RETRY_AFTER_MAX_MS",
    "LOG_ROUTE",
];

/// Every daemon-wide key (the part after `FERRO_HTTP_`).
pub const DAEMON_KEYS: &[&str] = &[
    "DRAIN_MS",
    "MAX_BODY_BYTES",
    "NAT64_PREFIXES",
    "SLOW_LOG_MS",
];

/// The metrics label every `forbidden_upstream` refusal counts under (§23.10.2), so no upstream
/// may be named it: a real upstream called `_unknown` would turn that label back into an
/// existence oracle.
pub const RESERVED_UPSTREAM_NAME: &str = "_unknown";

const MIB: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathParams {
    Refuse,
    Allow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathEncoding {
    Ascii,
    Utf8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachPolicy {
    Refuse,
    Override,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpVersions {
    /// `HTTP=1.1` (the default).
    H1Only,
    /// `HTTP=auto`.
    Auto,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinTls {
    V1_2,
    V1_3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Partition {
    None,
    Uid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerCounts {
    Connect,
    ConnectTimeout,
    ConnectTimeout5xx,
}

/// TLS file locations. They are only *named* here; `engine::tls` reads them once, at start (`CA_FILE`
/// since M6-F5a; `CLIENT_CERT_FILE`/`CLIENT_KEY_FILE`, mTLS, since M6-F5c).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsFiles {
    pub ca_file: Option<PathBuf>,
    pub client_cert_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub min_tls: MinTls,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub connect_timeout_ms: u32,
    pub timeout_ms: u32,
    pub read_timeout_ms: Option<u32>,
    pub max_connections: u32,
    pub max_requests: u32,
    pub max_queued: u32,
    pub max_body_bytes: u64,
    pub queue_timeout_ms: u32,
    pub max_dials: u32,
    pub idle_timeout_ms: u32,
    pub h1_unsafe_reuse_max_idle_ms: u32,
    pub max_lifetime_ms: u32,
    pub dns_ttl_ms: u32,
    pub max_response_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Breaker {
    pub failures: u32,
    pub counts: BreakerCounts,
    pub open_ms: u32,
}

/// `RATE_PER_SEC` is a positive decimal with at most three fractional digits, held exactly in
/// milli-requests per second (a provider limit of 20 per minute is `0.333`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rate {
    pub per_sec_milli: u64,
    pub burst: u32,
    pub max_wait_ms: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryAfter {
    pub honor: bool,
    pub max_ms: u32,
}

/// One enabled upstream.
#[derive(Debug)]
pub struct Upstream {
    pub name: String,
    pub origin: Origin,
    pub allow_paths: Vec<String>,
    pub path_params: PathParams,
    pub path_encoding: PathEncoding,
    /// `None`: every method except the always-refused ones. Case-sensitive.
    pub allow_methods: Option<Vec<String>>,
    /// `None`: any admitted peer.
    pub allow_uids: Option<Vec<u32>>,
    pub attached: AttachedHeaders,
    pub attach_policy: AttachPolicy,
    /// Lowercase.
    pub pass_headers: Vec<String>,
    /// Case-sensitive; default empty (§23.18 Q8).
    pub idempotent_methods: Vec<String>,
    /// Lowercase.
    pub idempotency_key_header: Option<String>,
    pub http: HttpVersions,
    pub tls: TlsFiles,
    pub address: AddressPolicy,
    pub partition: Partition,
    pub limits: Limits,
    pub breaker: Breaker,
    pub rate: Option<Rate>,
    pub retry_after: RetryAfter,
    pub log_route: bool,
}

impl PartialEq for Upstream {
    /// Identity: one configuration holds one upstream per name.
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for Upstream {}

/// Daemon-wide settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonSettings {
    pub drain_ms: u32,
    pub max_body_bytes: u64,
    pub nat64: Nat64Prefixes,
    pub slow_log_ms: Option<u32>,
}

impl Default for DaemonSettings {
    fn default() -> Self {
        DaemonSettings {
            drain_ms: 30_000,
            max_body_bytes: 256 * MIB,
            nat64: Nat64Prefixes::default(),
            slow_log_ms: None,
        }
    }
}

/// What failed about one key. A fixed sentence; never the value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason {
    Required,
    NotUtf8,
    NotANumber,
    OutOfRange {
        min: u64,
        max: u64,
    },
    OneOf(&'static [&'static str]),
    EmptyList,
    Origin(OriginError),
    /// An IP-literal `ORIGIN` in the always-refused table.
    OriginAddress(AddressRefusal),
    Attach(AttachError),
    FileUnreadable(std::io::ErrorKind),
    /// A FIFO, a device or a directory where a file was expected (M6-F5c review F6).
    FileNotRegular,
    /// A list entry (1-based) that is not a method token, or is `CONNECT`/`TRACE`/`TRACK`.
    BadMethod(usize),
    AlwaysRefusedMethod(usize),
    BadUid(usize),
    /// An `ALLOW_PATHS` entry (1-based) refused as a target by the given rule.
    BadPath(usize, &'static str),
    BadHeaderName(usize),
    /// A `PASS_HEADERS` entry (1-based) that names nothing §23.4.3 refuses by default, or one
    /// that can never be exempted.
    NotExemptible(usize),
    /// An `IDEMPOTENCY_KEY_HEADER` the engine sets, drops, refuses or attaches.
    KeyHeaderReserved,
    BadNat64Prefix(usize),
    BadRate,
    /// A key that only makes sense with another.
    Conflict(&'static str),
    Unknown,
    /// Two declared upstreams could both own this variable.
    Ambiguous,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reason::Required => f.write_str("is required"),
            Reason::NotUtf8 => f.write_str("is not valid UTF-8"),
            Reason::NotANumber => f.write_str("is not a decimal number"),
            Reason::OutOfRange { min, max } => write!(f, "must be in {min}..={max}"),
            Reason::OneOf(opts) => write!(f, "must be one of: {}", opts.join(", ")),
            Reason::EmptyList => f.write_str("lists nothing"),
            Reason::Origin(e) => write!(f, "{e}"),
            Reason::OriginAddress(r) => write!(
                f,
                "IP literal is in the always-refused address table ({})",
                r.label()
            ),
            Reason::Attach(e) => write!(f, "{e}"),
            Reason::FileUnreadable(k) => write!(f, "file cannot be read ({k})"),
            Reason::FileNotRegular => write!(f, "is not a regular file"),
            Reason::BadMethod(n) => write!(f, "entry {n} is not an RFC 9110 token of 1-32 bytes"),
            Reason::AlwaysRefusedMethod(n) => {
                write!(
                    f,
                    "entry {n} is CONNECT, TRACE or TRACK, which can never be allowed"
                )
            }
            Reason::BadUid(n) => write!(f, "entry {n} is not a uid"),
            Reason::BadPath(n, rule) => write!(f, "entry {n} is refused as a path ({rule})"),
            Reason::BadHeaderName(n) => write!(f, "entry {n} is not a header-name token"),
            Reason::NotExemptible(n) => write!(
                f,
                "entry {n} is not an override or forwarding header (pseudo-headers, host, \
                 upgrade and proxy-authorization can never be exempted)"
            ),
            Reason::KeyHeaderReserved => f.write_str(
                "names a header the engine sets, drops, refuses or attaches for this upstream",
            ),
            Reason::BadNat64Prefix(n) => {
                write!(
                    f,
                    "entry {n} is not an IPv6 /96 prefix with zero low 32 bits"
                )
            }
            Reason::BadRate => {
                f.write_str("must be a positive decimal with at most three fractional digits")
            }
            Reason::Conflict(why) => f.write_str(why),
            Reason::Unknown => f.write_str("is not a known key"),
            Reason::Ambiguous => f.write_str("could belong to more than one declared upstream"),
        }
    }
}

/// One configuration error. Its `Display` is safe to log: names and keys are operator-declared
/// identifiers checked against a safe character set, and no value is ever quoted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// A `FERRO_UPSTREAMS` entry (1-based position) that is not a valid upstream name.
    BadName {
        position: usize,
    },
    /// Upstream names that map to the same `FERRO_UPSTREAM_<NAME>_` prefix.
    NameCollision {
        names: Vec<String>,
    },
    Daemon {
        key: String,
        reason: Reason,
    },
    Upstream {
        upstream: String,
        key: String,
        reason: Reason,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::BadName { position } => write!(
                f,
                "{UPSTREAMS_VAR}: entry {position} is not a valid upstream name \
                 (1-64 bytes of [A-Za-z0-9._-], not `{RESERVED_UPSTREAM_NAME}`)"
            ),
            ConfigError::NameCollision { names } => write!(
                f,
                "{UPSTREAMS_VAR}: upstreams {} share one {UPSTREAM_PREFIX}<NAME>_ prefix; all are disabled",
                names.join(", ")
            ),
            ConfigError::Daemon { key, reason } => {
                write!(
                    f,
                    "{HTTP_PREFIX}{key} {reason}; the HTTP service is disabled"
                )
            }
            ConfigError::Upstream {
                upstream,
                key,
                reason,
            } => write!(
                f,
                "upstream {upstream}: {UPSTREAM_PREFIX}{}_{key} {reason}; the upstream is disabled",
                env_name(upstream)
            ),
        }
    }
}

/// A declared upstream: enabled, or disabled with the errors that disabled it.
#[derive(Debug)]
pub enum UpstreamEntry {
    Enabled(Box<Upstream>),
    Disabled(Vec<ConfigError>),
}

/// The loaded configuration.
#[derive(Debug, Default)]
pub struct HttpConfig {
    pub daemon: DaemonSettings,
    /// Non-empty: the HTTP service refuses every request (`forbidden_upstream`).
    daemon_errors: Vec<ConfigError>,
    list_errors: Vec<ConfigError>,
    upstreams: BTreeMap<String, UpstreamEntry>,
    orphan_keys: Vec<String>,
}

impl HttpConfig {
    /// Read the process environment. Attached-header files are read with a size cap.
    pub fn from_env() -> HttpConfig {
        HttpConfig::load(std::env::vars_os(), &read_capped)
    }

    /// Load from explicit variables and a file reader (the test seam: `std::env::set_var` is
    /// `unsafe` under edition 2024, which `unsafe_code = "forbid"` refuses).
    pub fn load(
        vars: impl IntoIterator<Item = (OsString, OsString)>,
        read_file: &dyn Fn(&Path) -> std::io::Result<Vec<u8>>,
    ) -> HttpConfig {
        let mut env: HashMap<String, Result<String, ()>> = HashMap::new();
        for (k, v) in vars {
            let Some(k) = k.to_str() else { continue };
            if k == UPSTREAMS_VAR || k.starts_with(UPSTREAM_PREFIX) || k.starts_with(HTTP_PREFIX) {
                env.insert(k.to_string(), v.into_string().map_err(|_| ()));
            }
        }
        let mut cfg = HttpConfig::default();
        cfg.load_daemon(&env);
        cfg.load_upstreams(&env, read_file);
        cfg
    }

    fn load_daemon(&mut self, env: &HashMap<String, Result<String, ()>>) {
        let mut errs = Vec::new();
        let mut d = DaemonSettings::default();
        let mut keys: Vec<&String> = env.keys().filter(|k| k.starts_with(HTTP_PREFIX)).collect();
        keys.sort();
        for full in keys {
            let key = &full[HTTP_PREFIX.len()..];
            let err = |reason| ConfigError::Daemon {
                key: safe_key(key),
                reason,
            };
            let raw = match &env[full] {
                Ok(v) => v.trim(),
                Err(()) => {
                    errs.push(err(Reason::NotUtf8));
                    continue;
                }
            };
            if !DAEMON_KEYS.contains(&key) {
                errs.push(err(Reason::Unknown));
                continue;
            }
            if raw.is_empty() {
                continue;
            }
            let r = match key {
                "DRAIN_MS" => num_u32(raw, 0).map(|v| d.drain_ms = v),
                "MAX_BODY_BYTES" => num_u64(raw, 0).map(|v| d.max_body_bytes = v),
                "SLOW_LOG_MS" => num_u32(raw, 0).map(|v| d.slow_log_ms = Some(v)),
                "NAT64_PREFIXES" => list(raw)
                    .enumerate()
                    .map(|(i, e)| {
                        Nat64Prefixes::parse_entry(e).ok_or(Reason::BadNat64Prefix(i + 1))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(|v| {
                        if v.is_empty() {
                            Err(Reason::EmptyList)
                        } else {
                            Ok(v)
                        }
                    })
                    .map(|v| d.nat64 = Nat64Prefixes::new(v)),
                _ => Ok(()),
            };
            if let Err(reason) = r {
                errs.push(err(reason));
            }
        }
        self.daemon = d;
        self.daemon_errors = errs;
    }

    fn load_upstreams(
        &mut self,
        env: &HashMap<String, Result<String, ()>>,
        read_file: &dyn Fn(&Path) -> std::io::Result<Vec<u8>>,
    ) {
        let names_raw = match env.get(UPSTREAMS_VAR) {
            None => return,
            Some(Err(())) => {
                self.list_errors.push(ConfigError::BadName { position: 0 });
                return;
            }
            Some(Ok(v)) => v.clone(),
        };
        // Declared names, grouped by env name.
        let mut by_env: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (i, name) in list(&names_raw).enumerate() {
            if !is_valid_name(name) {
                self.list_errors
                    .push(ConfigError::BadName { position: i + 1 });
                continue;
            }
            let group = by_env.entry(env_name(name)).or_default();
            if !group.iter().any(|n| n == name) {
                group.push(name.to_string());
            }
        }
        // Assign every FERRO_UPSTREAM_<rest> variable to (upstream env name, key).
        let mut assigned: BTreeMap<String, BTreeMap<&'static str, Result<String, ()>>> =
            BTreeMap::new();
        let mut key_errors: BTreeMap<String, Vec<(String, Reason)>> = BTreeMap::new();
        let mut vars: Vec<&String> = env
            .keys()
            .filter(|k| k.starts_with(UPSTREAM_PREFIX) && *k != UPSTREAMS_VAR)
            .collect();
        vars.sort();
        for full in vars {
            let rest = &full[UPSTREAM_PREFIX.len()..];
            let readings: Vec<(&String, &'static str)> = by_env
                .keys()
                .filter_map(|e| {
                    let k = rest.strip_prefix(e.as_str())?.strip_prefix('_')?;
                    UPSTREAM_KEYS.iter().find(|x| **x == k).map(|x| (e, *x))
                })
                .collect();
            match readings.as_slice() {
                [(e, k)] => {
                    assigned
                        .entry((*e).clone())
                        .or_default()
                        .insert(k, env[full].clone());
                }
                [] => {
                    let owner = by_env
                        .keys()
                        .filter(|e| {
                            rest.strip_prefix(e.as_str())
                                .is_some_and(|r| r.starts_with('_'))
                        })
                        .max_by_key(|e| e.len());
                    match owner {
                        Some(e) => {
                            let key = &rest[e.len() + 1..];
                            key_errors
                                .entry(e.clone())
                                .or_default()
                                .push((safe_key(key), Reason::Unknown));
                        }
                        None => self.orphan_keys.push(safe_key(full)),
                    }
                }
                many => {
                    for (e, k) in many {
                        key_errors
                            .entry((*e).clone())
                            .or_default()
                            .push(((*k).to_string(), Reason::Ambiguous));
                    }
                }
            }
        }
        for (e, names) in by_env {
            if names.len() > 1 {
                let err = ConfigError::NameCollision {
                    names: names.clone(),
                };
                for n in names {
                    self.upstreams
                        .insert(n, UpstreamEntry::Disabled(vec![err.clone()]));
                }
                continue;
            }
            let name = names.into_iter().next().unwrap_or_default();
            let mut errors: Vec<ConfigError> = key_errors
                .remove(&e)
                .unwrap_or_default()
                .into_iter()
                .map(|(key, reason)| ConfigError::Upstream {
                    upstream: name.clone(),
                    key,
                    reason,
                })
                .collect();
            let keys = assigned.remove(&e).unwrap_or_default();
            let entry = match parse_upstream(&name, &keys, &self.daemon, read_file) {
                Ok(up) if errors.is_empty() => UpstreamEntry::Enabled(Box::new(up)),
                Ok(_) => UpstreamEntry::Disabled(errors),
                Err(mut more) => {
                    errors.append(&mut more);
                    UpstreamEntry::Disabled(errors)
                }
            };
            self.upstreams.insert(name, entry);
        }
    }

    /// The upstream a peer may use, or `None` — one answer for "unknown", "disabled", "HTTP
    /// disabled" and "not for your uid" (D15, §23.4).
    pub fn upstream_for(&self, name: &str, peer_uid: Option<u32>) -> Option<&Upstream> {
        if !self.daemon_errors.is_empty() {
            return None;
        }
        let UpstreamEntry::Enabled(up) = self.upstreams.get(name)? else {
            return None;
        };
        match (&up.allow_uids, peer_uid) {
            (None, _) => Some(up),
            (Some(uids), Some(uid)) if uids.contains(&uid) => Some(up),
            _ => None,
        }
    }

    /// Every declared upstream and its state, for startup logging.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &UpstreamEntry)> {
        self.upstreams.iter().map(|(n, e)| (n.as_str(), e))
    }

    /// Every error, for startup logging at `error` (each one's `Display` is log-safe).
    pub fn errors(&self) -> Vec<&ConfigError> {
        let mut out: Vec<&ConfigError> = self.daemon_errors.iter().collect();
        out.extend(self.list_errors.iter());
        for e in self.upstreams.values() {
            if let UpstreamEntry::Disabled(errs) = e {
                out.extend(errs.iter());
            }
        }
        out
    }

    /// Whether a daemon-wide error disabled the whole service.
    pub fn service_disabled(&self) -> bool {
        !self.daemon_errors.is_empty()
    }

    /// `FERRO_UPSTREAM_*` variables no declared upstream owns — a typo in the NAME part
    /// (`FERRO_UPSTREAM_OPENIA_ALLOW_PATHS`) lands here, and the startup log should say so.
    pub fn orphan_keys(&self) -> &[String] {
        &self.orphan_keys
    }

    /// §23.3.3: the upstreams carrying credential material — attached headers, or (since M6-F5c)
    /// a `CLIENT_KEY_FILE`, which is credential material too (§23.3.1) — whose credential isolation
    /// is void, given `FERRO_ALLOW_UIDS` and `ferrod`'s own uid. Names only — never values.
    pub fn isolation_void(&self, ferro_allow_uids: &[u32], own_uid: u32) -> Vec<&str> {
        let daemon_void = ferro_allow_uids.is_empty() || ferro_allow_uids.contains(&own_uid);
        self.upstreams
            .iter()
            .filter_map(|(n, e)| match e {
                UpstreamEntry::Enabled(up)
                    if !up.attached.is_empty() || up.tls.client_key_file.is_some() =>
                {
                    let up_void = up.allow_uids.as_ref().is_some_and(|u| u.contains(&own_uid));
                    (daemon_void || up_void).then_some(n.as_str())
                }
                _ => None,
            })
            .collect()
    }
}

/// The reader [`HttpConfig::from_env`] uses: at most one byte past the attached-header file cap,
/// so an oversize file is REFUSED by the parser instead of being silently truncated to fit.
pub fn read_capped(p: &Path) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    open_regular(p)?
        .0
        .take(attach::MAX_FILE_BYTES as u64 + 1)
        .read_to_end(&mut buf)?;
    Ok(buf)
}

/// A file the daemon reads at start that is not a regular file — a FIFO, a device, a directory.
/// It is refused BEFORE it is opened, because opening a FIFO for reading blocks until a writer
/// appears, which at start is forever (M6-F5c review F6).
#[derive(Debug)]
pub struct NotRegularFile;

impl fmt::Display for NotRegularFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not a regular file")
    }
}

impl std::error::Error for NotRegularFile {}

/// Open `p` for reading only if it is a regular file, and return it with its length. The check is
/// made on the path (`stat`, following symlinks) before the open, and again on the opened handle,
/// so a FIFO is never opened and a swap between the two checks is still caught once opened (a
/// FIFO swapped in after the first check would block the open itself: a race only whoever can
/// write the configured path can run).
pub fn open_regular(p: &Path) -> std::io::Result<(std::fs::File, u64)> {
    let refuse = || std::io::Error::new(std::io::ErrorKind::InvalidInput, NotRegularFile);
    if !std::fs::metadata(p)?.is_file() {
        return Err(refuse());
    }
    let f = std::fs::File::open(p)?;
    let meta = f.metadata()?;
    if !meta.is_file() {
        return Err(refuse());
    }
    Ok((f, meta.len()))
}

/// The env-var form of a name: ASCII-uppercased, every other byte `_` — `ferrod`'s `env_name`
/// for pools (`config.rs`), reproduced because this crate does not depend on `ferrod`.
pub fn env_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// 1–64 bytes of `[A-Za-z0-9._-]`, and not [`RESERVED_UPSTREAM_NAME`]. Names reach logs and the
/// metrics `upstream` label, so they are held to a set that needs no escaping to be safe.
pub fn is_valid_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 64
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
        && n != RESERVED_UPSTREAM_NAME
}

/// A key name as it may be logged: `[A-Z0-9_]` only, otherwise a placeholder.
fn safe_key(k: &str) -> String {
    if !k.is_empty()
        && k.len() <= 128
        && k.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        k.to_string()
    } else {
        "<key with unsafe characters>".to_string()
    }
}

fn list(raw: &str) -> impl Iterator<Item = &str> {
    raw.split(',').map(str::trim).filter(|s| !s.is_empty())
}

fn num_u64(raw: &str, min: u64) -> Result<u64, Reason> {
    let v = parse_decimal_u64(raw.as_bytes()).ok_or(Reason::NotANumber)?;
    if v < min {
        return Err(Reason::OutOfRange { min, max: u64::MAX });
    }
    Ok(v)
}

fn num_u32(raw: &str, min: u32) -> Result<u32, Reason> {
    let v = parse_decimal_u64(raw.as_bytes()).ok_or(Reason::NotANumber)?;
    match u32::try_from(v) {
        Ok(v) if v >= min => Ok(v),
        _ => Err(Reason::OutOfRange {
            min: u64::from(min),
            max: u64::from(u32::MAX),
        }),
    }
}

fn one_of<T: Copy>(raw: &str, opts: &'static [&'static str], vals: &[T]) -> Result<T, Reason> {
    opts.iter()
        .position(|o| *o == raw)
        .map(|i| vals[i])
        .ok_or(Reason::OneOf(opts))
}

/// `RATE_PER_SEC`: `\d+(\.\d{1,3})?`, positive, in milli-units.
fn parse_rate(raw: &str) -> Result<u64, Reason> {
    let (int, frac) = match raw.split_once('.') {
        Some((i, f)) if !f.is_empty() && f.len() <= 3 => (i, f),
        Some(_) => return Err(Reason::BadRate),
        None => (raw, ""),
    };
    let i = parse_decimal_u64(int.as_bytes()).ok_or(Reason::BadRate)?;
    let mut f = if frac.is_empty() {
        0
    } else {
        parse_decimal_u64(frac.as_bytes()).ok_or(Reason::BadRate)?
    };
    for _ in frac.len()..3 {
        f *= 10;
    }
    let milli = i
        .checked_mul(1000)
        .and_then(|v| v.checked_add(f))
        .ok_or(Reason::BadRate)?;
    if milli == 0 {
        return Err(Reason::BadRate);
    }
    Ok(milli)
}

fn method_list(raw: &str) -> Result<Vec<String>, Reason> {
    let mut out = Vec::new();
    for (i, m) in list(raw).enumerate() {
        if !is_token(m.as_bytes(), 32) {
            return Err(Reason::BadMethod(i + 1));
        }
        if is_always_refused_method(m) {
            return Err(Reason::AlwaysRefusedMethod(i + 1));
        }
        out.push(m.to_string());
    }
    if out.is_empty() {
        return Err(Reason::EmptyList);
    }
    Ok(out)
}

fn is_exemptible(lower: &str) -> bool {
    is_override_header(lower) || is_forwarding_header(lower)
}

/// The per-upstream key map an upstream was assigned.
type Keys = BTreeMap<&'static str, Result<String, ()>>;

fn parse_upstream(
    name: &str,
    keys: &Keys,
    daemon: &DaemonSettings,
    read_file: &dyn Fn(&Path) -> std::io::Result<Vec<u8>>,
) -> Result<Upstream, Vec<ConfigError>> {
    let mut errs: Vec<ConfigError> = Vec::new();
    let mut fail = |key: &str, reason: Reason| {
        errs.push(ConfigError::Upstream {
            upstream: name.to_string(),
            key: key.to_string(),
            reason,
        })
    };
    // A key's trimmed value; `None` when unset or blank; a non-UTF-8 value is an error.
    let mut nonutf8 = Vec::new();
    let get = |key: &'static str| -> Option<&str> {
        match keys.get(key)? {
            Ok(v) if v.trim().is_empty() => None,
            Ok(v) => Some(v.trim()),
            Err(()) => None,
        }
    };
    for (k, v) in keys {
        if v.is_err() {
            nonutf8.push(*k);
        }
    }
    for k in nonutf8 {
        fail(k, Reason::NotUtf8);
    }

    macro_rules! parse {
        ($key:literal, $default:expr, $f:expr) => {
            match get($key) {
                None => $default,
                Some(raw) => match $f(raw) {
                    Ok(v) => v,
                    Err(reason) => {
                        fail($key, reason);
                        $default
                    }
                },
            }
        };
    }

    let origin = match get("ORIGIN") {
        None => {
            fail("ORIGIN", Reason::Required);
            None
        }
        Some(raw) => match Origin::parse(raw) {
            Ok(o) => Some(o),
            Err(e) => {
                fail("ORIGIN", Reason::Origin(e));
                None
            }
        },
    };
    let path_params = parse!("PATH_PARAMS", PathParams::Refuse, |r| one_of(
        r,
        &["refuse", "allow"],
        &[PathParams::Refuse, PathParams::Allow]
    ));
    let path_encoding = parse!("PATH_ENCODING", PathEncoding::Ascii, |r| one_of(
        r,
        &["ascii", "utf8"],
        &[PathEncoding::Ascii, PathEncoding::Utf8]
    ));
    let allow_paths = parse!("ALLOW_PATHS", vec!["/".to_string()], |r: &str| {
        let mut out = Vec::new();
        for (i, p) in list(r).enumerate() {
            check_prefix(p.as_bytes(), path_params, path_encoding)
                .map_err(|e: Refusal| Reason::BadPath(i + 1, e.rule.name()))?;
            out.push(p.to_string());
        }
        if out.is_empty() {
            return Err(Reason::EmptyList);
        }
        Ok(out)
    });
    let allow_methods = parse!("ALLOW_METHODS", None, |r| method_list(r).map(Some));
    let idempotent_methods = parse!("IDEMPOTENT_METHODS", Vec::new(), method_list);
    let allow_uids = parse!("ALLOW_UIDS", None, |r: &str| {
        let mut out = Vec::new();
        for (i, u) in list(r).enumerate() {
            let v = parse_decimal_u64(u.as_bytes())
                .and_then(|v| u32::try_from(v).ok())
                .ok_or(Reason::BadUid(i + 1))?;
            out.push(v);
        }
        if out.is_empty() {
            return Err(Reason::EmptyList);
        }
        Ok(Some(out))
    });
    let attach_policy = parse!("ATTACH_POLICY", AttachPolicy::Refuse, |r| one_of(
        r,
        &["refuse", "override"],
        &[AttachPolicy::Refuse, AttachPolicy::Override]
    ));
    let attached = parse!(
        "ATTACH_HEADERS_FILE",
        AttachedHeaders::default(),
        |r: &str| {
            let bytes = read_file(Path::new(r)).map_err(|e| {
                if e.get_ref().is_some_and(|x| x.is::<NotRegularFile>()) {
                    Reason::FileNotRegular
                } else {
                    Reason::FileUnreadable(e.kind())
                }
            })?;
            attach::parse(&bytes).map_err(Reason::Attach)
        }
    );
    let pass_headers = parse!("PASS_HEADERS", Vec::new(), |r: &str| {
        let mut out = Vec::new();
        for (i, h) in list(r).enumerate() {
            if !is_token(h.as_bytes(), 256) {
                return Err(Reason::BadHeaderName(i + 1));
            }
            let lower = fold_name(h);
            if !is_exemptible(&lower) {
                return Err(Reason::NotExemptible(i + 1));
            }
            out.push(lower);
        }
        if out.is_empty() {
            return Err(Reason::EmptyList);
        }
        Ok(out)
    });
    let idempotency_key_header = parse!("IDEMPOTENCY_KEY_HEADER", None, |r: &str| {
        if !is_token(r.as_bytes(), 256) {
            return Err(Reason::BadHeaderName(1));
        }
        // Checked FOLDED (any spelling a CGI-style server would merge), matched EXACTLY at
        // request time: folding the match would license a key a non-folding upstream never sees.
        let folded = fold_name(r);
        // A key the engine sets/drops/refuses could never arrive as PHP sent it; an ATTACHED key
        // would make every request "idempotent" with one constant key; and `accept-encoding`,
        // which the engine itself may set (review F-4), would make every Guzzle request so.
        if is_reserved_attach_name(&folded)
            || is_exemptible(&folded)
            || attached.names(&folded)
            || folded == "accept-encoding"
        {
            return Err(Reason::KeyHeaderReserved);
        }
        Ok(Some(r.to_ascii_lowercase()))
    });
    let http = parse!("HTTP", HttpVersions::H1Only, |r| one_of(
        r,
        &["1.1", "auto"],
        &[HttpVersions::H1Only, HttpVersions::Auto]
    ));
    let path = |r: &str| -> Result<Option<PathBuf>, Reason> { Ok(Some(PathBuf::from(r))) };
    let tls = TlsFiles {
        ca_file: parse!("CA_FILE", None, path),
        client_cert_file: parse!("CLIENT_CERT_FILE", None, path),
        client_key_file: parse!("CLIENT_KEY_FILE", None, path),
        min_tls: parse!("MIN_TLS", MinTls::V1_2, |r| one_of(
            r,
            &["1.2", "1.3"],
            &[MinTls::V1_2, MinTls::V1_3]
        )),
    };
    let classes = parse!("ADDRESS_CLASSES", ClassSet::PUBLIC_ONLY, |r: &str| {
        let mut set = ClassSet {
            public: false,
            private: false,
            loopback: false,
        };
        let mut any = false;
        for c in list(r) {
            match c {
                "public" => set.public = true,
                "private" => set.private = true,
                "loopback" => set.loopback = true,
                _ => return Err(Reason::OneOf(&["public", "private", "loopback"])),
            }
            any = true;
        }
        if !any {
            return Err(Reason::EmptyList);
        }
        Ok(set)
    });
    let bool01 = |r: &str| one_of(r, &["0", "1"], &[false, true]);
    let allow_metadata = parse!("ALLOW_METADATA", false, bool01);
    let partition = parse!("PARTITION", Partition::None, |r| one_of(
        r,
        &["none", "uid"],
        &[Partition::None, Partition::Uid]
    ));
    let limits = Limits {
        connect_timeout_ms: parse!("CONNECT_TIMEOUT_MS", 5_000, |r| num_u32(r, 1)),
        timeout_ms: parse!("TIMEOUT_MS", 600_000, |r| num_u32(r, 1)),
        read_timeout_ms: parse!("READ_TIMEOUT_MS", None, |r| num_u32(r, 1).map(Some)),
        max_connections: parse!("MAX_CONNECTIONS", 32, |r| num_u32(r, 1)),
        max_requests: parse!("MAX_REQUESTS", 128, |r| num_u32(r, 1)),
        max_queued: parse!("MAX_QUEUED", 256, |r| num_u32(r, 0)),
        max_body_bytes: parse!("MAX_BODY_BYTES", 64 * MIB, |r| num_u64(r, 0)),
        queue_timeout_ms: parse!("QUEUE_TIMEOUT_MS", 5_000, |r| num_u32(r, 0)),
        max_dials: parse!("MAX_DIALS", 4, |r| num_u32(r, 1)),
        idle_timeout_ms: parse!("IDLE_TIMEOUT_MS", 15_000, |r| num_u32(r, 0)),
        h1_unsafe_reuse_max_idle_ms: parse!("H1_UNSAFE_REUSE_MAX_IDLE_MS", 2_000, |r| num_u32(
            r, 0
        )),
        max_lifetime_ms: parse!("MAX_LIFETIME_MS", 300_000, |r| num_u32(r, 1)),
        dns_ttl_ms: parse!("DNS_TTL_MS", 60_000, |r| num_u32(r, 0)),
        max_response_bytes: parse!("MAX_RESPONSE_BYTES", None, |r| num_u64(r, 1).map(Some)),
    };
    let breaker = Breaker {
        failures: parse!("BREAKER_FAILURES", 5, |r| num_u32(r, 1)),
        counts: parse!("BREAKER_COUNTS", BreakerCounts::Connect, |r| one_of(
            r,
            &["connect", "connect+timeout", "connect+timeout+5xx"],
            &[
                BreakerCounts::Connect,
                BreakerCounts::ConnectTimeout,
                BreakerCounts::ConnectTimeout5xx
            ]
        )),
        open_ms: parse!("BREAKER_OPEN_MS", 5_000, |r| num_u32(r, 1)),
    };
    let per_sec = parse!("RATE_PER_SEC", None, |r| parse_rate(r).map(Some));
    let burst = parse!("RATE_BURST", None, |r| num_u32(r, 1).map(Some));
    let max_wait = parse!("RATE_MAX_WAIT_MS", None, |r| num_u32(r, 0).map(Some));
    let rate = match per_sec {
        Some(milli) => Some(Rate {
            per_sec_milli: milli,
            // "= rate": the bucket holds one second's worth, and at least one token.
            burst: burst.unwrap_or_else(|| {
                u32::try_from(milli.div_ceil(1000))
                    .unwrap_or(u32::MAX)
                    .max(1)
            }),
            max_wait_ms: max_wait.unwrap_or(0),
        }),
        None => {
            if burst.is_some() {
                fail(
                    "RATE_BURST",
                    Reason::Conflict("is set but RATE_PER_SEC is not"),
                );
            }
            if max_wait.is_some() {
                fail(
                    "RATE_MAX_WAIT_MS",
                    Reason::Conflict("is set but RATE_PER_SEC is not"),
                );
            }
            None
        }
    };
    let retry_after = RetryAfter {
        honor: parse!("HONOR_RETRY_AFTER", false, bool01),
        max_ms: parse!("RETRY_AFTER_MAX_MS", 60_000, |r| num_u32(r, 0)),
    };
    let log_route = parse!("LOG_ROUTE", true, bool01);

    if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
        fail(
            if tls.client_cert_file.is_some() {
                "CLIENT_KEY_FILE"
            } else {
                "CLIENT_CERT_FILE"
            },
            Reason::Conflict("CLIENT_CERT_FILE and CLIENT_KEY_FILE must be set together"),
        );
    }
    let mut address = None;
    if let Some(o) = &origin {
        if o.scheme() == Scheme::Http {
            for k in ["CA_FILE", "CLIENT_CERT_FILE", "CLIENT_KEY_FILE", "MIN_TLS"] {
                if get(k).is_some() {
                    fail(
                        k,
                        Reason::Conflict(
                            "is a TLS setting, and ORIGIN is http:// (it would protect nothing)",
                        ),
                    );
                }
            }
        }
        let policy = AddressPolicy {
            classes,
            allow_metadata,
            literal: o.ip_literal(),
        };
        if let Some(lit) = o.ip_literal()
            && let Err(r @ AddressRefusal::Range(_)) = policy.check(lit, &daemon.nat64)
        {
            fail("ORIGIN", Reason::OriginAddress(r));
        }
        address = Some(policy);
    }

    match (origin, address) {
        (Some(origin), Some(address)) if errs.is_empty() => Ok(Upstream {
            name: name.to_string(),
            origin,
            allow_paths,
            path_params,
            path_encoding,
            allow_methods,
            allow_uids,
            attached,
            attach_policy,
            pass_headers,
            idempotent_methods,
            idempotency_key_header,
            http,
            tls,
            address,
            partition,
            limits,
            breaker,
            rate,
            retry_after,
            log_route,
        }),
        _ => Err(errs),
    }
}
