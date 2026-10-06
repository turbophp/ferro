# SPEC §23 — Ferro HTTP: the outbound HTTP transport engine

**Status:** normative, adopted 2026-10-06 (M6-F0, SPEC §22.2 (cn)). This file is §23 of `ferro-spec-v0.2.md`, which carries a stub pointing here; it has the same authority as the rest of the spec, and the charter's definition of done applies to it unchanged. Its decisions are recorded in SPEC §21 as **D19** (the HTTP contract), **D20** (the dependency set) and **D21** (retry licences per service, shared with §24). The choices the draft left open were decided under the owner's full-freedom grant as applied 2026-10-06 (ledger, "Owner directives and grants"), and are listed in §23.18.

**Allocations.** This section allocates service id `HTTP = 6`, the engine feature bit `HTTP = 0x08`, the error codes `UpstreamUnavailable = 0x1007`, `RateLimited = 0x1008` (Retryable), `TlsRefused = 0x300D` and `ResponseIncomplete = 0x300E` (NonRetryable), and the `[http.causes]` vocabulary (§23.5). They are allocated here and **reserved in `/proto` as comments only** — beside `[services]` in `methods.toml` and in a reserved-codes block at the end of `errors.toml` — with no keys, so no generated constant and no registry-hash change follows, and no other change may take these numbers. The `/proto` keys, golden vectors and both codecs land with slice F2, the engine's first code slice (§23.15). §24 (Ferro Queue) allocates from the numbers after these. *[Amended M6-F2 (SPEC §22.2 (cy)): **keyed.** Every allocation above is now a real `/proto` key — the reservation comments are gone, Queue's excepted — with golden vectors and both codecs; `/proto/PROTOCOL.md` §12 is the layout. The registry hash moved; no `protocol_version` bump.]*

**Read first:** §23.16 (how each conflict with existing text was resolved), §23.18 (the decisions) and §23.19 (the premises not yet measured, owed by spikes F1a and F1b). A premise that turns out false changes the plan, not the evidence (the C3-1 precedent). Review-finding ids such as "review F13" are provenance; the appendix lists them.

## 23. Ferro HTTP: the outbound HTTP transport engine

A PHP worker hands `ferrod` a **request addressed to a declared upstream**: never a URL, never a TCP stream. `ferrod` then:

- resolves and dials the upstream, and originates TLS itself;
- pools connections across every worker on the host;
- attaches the upstream's credentials;
- sends the request **at most once**;
- streams the response back over the chassis's credit path (§5.2).

Every request ends in exactly one `END` (charter rule 4). A failed request's *transport* fate is classified on the §9.2 tree. **The engine never re-sends a request** (charter rule 3, §23.7). It does not follow redirects, keep cookies or interpret status codes (§23.2, §23.7.4). Those stay in PHP, above a Guzzle `HandlerStack` handler or a PSR-18 client (§23.11).

### 23.0 Decisions at a glance

1. **The service id is `HTTP = 6`, with three methods.** `REQUEST` goes client → engine. `HEAD` and `BODY` go engine → client, and `BODY` carries the `STREAM` flag. A response always streams: one `HEAD`, zero or more `BODY` frames, one `END`. Buffering is the client's choice (§23.5).
2. **PHP addresses an upstream by name and sends only an origin-form target.** The authority comes from operator configuration. Targets and headers are validated byte by byte and refused, never normalised (§23.4).
3. **A completed exchange is `Outcome::Ok`, whatever its status.** The engine reports its *effective idempotency* so that PHP can classify a status against one authority (§23.7.4).
4. **Fate rests on two observed facts and one declaration, never on inference.** The facts are whether any byte of the request reached the upstream connection (`sent`) and whether a response head arrived. The declaration is whether the request is *effectively idempotent*: the caller said so, or the operator declared it for the upstream. **The HTTP method alone licenses nothing** (§23.7.2, D21).
5. **HTTP/1.1 is the default. In v1 a non-idempotent request never rides HTTP/2** (§23.8.3). This is drop-in parity, because Guzzle's default request version is `1.1` and curl honours it. It also removes every HTTP/2 not-processed licence from v1's correctness path.
6. **Credentials live in operator-owned files and are attached by the engine.** A PHP value for an attached header name is **refused** by default (§23.3). The guarantee holds only if PHP and `ferrod` run as different uids, and the engine says so at startup (§23.3.3).
7. **The engine adds `hyper` 1.x (client side only), `rustls` with `ring`, and `rustls-native-certs`.** They live in a new crate `ferro-http` behind a default-on cargo feature `http` (D20, §23.13).
8. **Request bodies are inline, up to the 16 MiB frame cap,** and are held under per-upstream and daemon-wide byte budgets (§23.8.6, §23.9.3). A larger body is refused before dispatch; v1 has no large-request-body path.
9. **Breakers, rate limits, budgets and drain refusals all act before sending,** so each refusal is `Retryable`. Rate limits and holds carry a `retry_after_ms`.
10. **Prerequisite: per-request client deadlines (ledger D1c, M3).** Until they ship, a slow upstream would kill the PHP session (§23.11.0).

---

### 23.1 The admission test (product-vision §3), against the current tree

D16 requires the five points in writing before any code exists.

**A. Shared-nothing really hurts.** This **holds as argued, but is unmeasured.** Under PHP-FPM every worker owns its own curl handles, DNS cache and TLS session cache. N workers calling one API pay up to N TLS handshakes and hold up to N connections.

- PHP 8.5's persistent curl share handles (§23.19 P8, UNVERIFIED) narrow the cost to one per worker. They share nothing across workers, and Guzzle does not use them by default.
- **v1 delivers A through a host-wide HTTP/1.1 keep-alive pool and shared TLS session resumption,** not through HTTP/2 multiplexing. Product-vision §4.2's "no HTTP/2 connection sharing" therefore gets half an answer in v1: HTTP/2 is opt-in and carries only effectively-idempotent requests (§23.8.3, C22).
- Slice F11 measures handshake and connection counts for N workers × M requests through Ferro and through stock curl, on the D17 runner.

**B. The slot is empty.**

- Transparent egress proxies either tunnel `CONNECT`, which gives no pooling, or terminate TLS, which needs a CA on every host.
- Remote AI gateways add a WAN hop and need their own token in PHP.
- PHP 8.5's share handles are per process.

Ferro hands over the *request*, not a stream. Its limit: credential isolation does not survive an upstream that reflects request headers (§23.3.4).

**C. A drop-in seam exists.**

- Guzzle's `handler` option and `HandlerStack`.
- Laravel's `Http` facade, reached through a Factory rebinding inside the package's auto-discovered provider (§23.11.6).
- PSR-18 for `openai-php/client` and its peers.

Three gaps are stated now rather than discovered later:

1. **Stripe's PHP SDK ships its own curl client.** It needs an `ApiRequestor::setHttpClient()` adapter, which v1 does not ship.
2. **Guzzle has no process-global handler.** A `new GuzzleHttp\Client()` built by application code without the `handler` option uses curl. Adopting Ferro there is a one-line *code* change per client construction, not configuration (C9).
3. **The Laravel seam rebinds a framework class through a protected method** (`Factory::newPendingRequest()`). It is version-fragile and pinned by tests (§23.11.6).

**D. The correctness moat transfers.** **This holds, and it is the strongest point.**

- A `POST` that times out after dispatch is §9.2's `WriteUnconfirmed`.
- curl reports it as an untyped error, and Laravel's `retry()` with no `when` re-sends it.
- The engine already has the machinery: the pure classifier in `services/fate.rs`, the §19.3 client rules (`FateClassifier`), and the `Indeterminate` classes in both drop-in tiers.

HTTP reuses the branches. It adds one fact SQL lacks: an `Idempotency-Key` licence, real only when the *operator* declares that the server honours it.

**E. The engine/PHP boundary stays clean.**

- **The engine owns:** no storage at all (breaker, rate-limit and DNS state is in memory and dies with the epoch); transport, pools, TLS, limits and fate classification. It runs no PHP.
- **PHP owns:** request construction, middleware, redirects, cookies, auth flows, retries and response semantics.

Product-vision §4.2 E lists "retries" as engine-owned. Charter rule 3 binds, so the engine classifies and *licenses* a retry and PHP performs it (C1).

**New failure modes the engine introduces, stated against curl (review F5, F20).** These are the honest debits of the admission test:

1. **A restart is host-wide.** Every in-flight exchange on the host shares one daemon. A deploy or credential rotation cancels exchanges that outlive the HTTP drain (§23.6.1), where curl in each worker would not have noticed.
2. **The host shares one client identity per upstream.** All tenants on the host present one TLS client and one source IP to the provider, so abuse detection, IP bans and per-connection limits see one client. `PARTITION=uid` splits connection pools, but not the source IP.
3. **Breaker, rate-limit and Retry-After state is shared per upstream by design.** One tenant's failures can open the breaker for all. That is the feature, and the cost.

What v1 deliberately does **not** introduce: one tenant's connection-level failure turning another tenant's *write* Indeterminate. Non-idempotent requests never share an HTTP/2 connection (§23.8.3), and an HTTP/1.1 connection carries one exchange at a time.

**The latency-inversion corollary.** The hop is noise against a WAN call. For a plaintext loopback upstream (Elasticsearch on `127.0.0.1`), the hop costs about what the request does. Such an upstream gains credential custody and SSRF confinement, not speed, and the documentation says so.

---

### 23.2 Scope of v1

**In scope:**

- request/response exchange;
- streamed response bodies (SSE, chunked);
- named upstreams with daemon-held credentials;
- the fate taxonomy;
- per-upstream HTTP/1.1 pools with TLS, and HTTP/2 for effectively-idempotent requests (opt-in);
- concurrency limits, breakers, rate limits and body budgets;
- `gzip`/`deflate` decoding;
- OTLP spans, Prometheus metrics and the slow log;
- a native PHP API, a Guzzle handler, a PSR-18 client and the Laravel `Http` wiring.

**Not in scope for v1:**

| Not built | Where it lives instead / why |
|---|---|
| Redirect following | Guzzle's `RedirectMiddleware`. A redirect to an origin with no upstream is refused by the handler (§23.11.2), which is the SSRF rule working. |
| Cookie jars | Guzzle's `CookieMiddleware`. `Set-Cookie` and `Cookie` pass through. |
| Retries of any kind | PHP middleware. Ferro ships a decider, never an engine loop (charter rule 3). |
| A non-idempotent request over HTTP/2 | A post-v1 slice gated on P2 (§23.8.3). |
| `h2c` prior knowledge, gRPC | Later, as an extension of this engine. |
| HTTP/3, WebSocket, `Upgrade`, `CONNECT` | `Upgrade` and `CONNECT` are refused. |
| Outbound proxies (`HTTP_PROXY`, the `proxy` option) | Not honoured. A drop-in difference (C9). |
| Request signing (SigV4, HMAC), query-string keys | Both need the request rewritten (charter rule 6). They stay in PHP, and their keys cannot be daemon-held in v1. |
| Failover across upstreams | It chooses which account is billed, so it is an application decision. |
| Response caching | Charter rule 6. |
| Token-aware (LLM) rate limits | Limits count requests only. |
| Configuration reload | Rotation needs a restart (§23.6.1, C18; decided, §23.18 Q9). |
| Symfony `HttpClientInterface`, HTTPlug async | Post-v1. v1's client surface is the native API, Guzzle, synchronous PSR-18 and Laravel (§23.18 Q6). Lane S is the first post-v1 lane (§23.12). |
| Request bodies above the frame cap | Refused before dispatch; no large-request-body path in v1 (§23.9.3; §23.18 Q3). |
| Per-pool uid allow-lists | Upstreams have `ALLOW_UIDS`; database pools still have none. A recorded gap, not in v1 (§23.18 Q4). |

---

### 23.3 Named upstreams

An **upstream** is an operator-declared name bound to exactly one origin (`scheme://host[:port]`). Upstream names share no namespace with pool names.

#### 23.3.1 Configuration

Configuration uses the environment convention the tree already has (`config.rs`): `FERRO_UPSTREAMS=<n1>,<n2>` plus `FERRO_UPSTREAM_<NAME>_<KEY>`, with `<NAME>` mapped by the existing `env_name`. §18's `ferro.toml` is not built first: HTTP keeps the environment convention (decided, §23.18 Q1), and a later `ferro.toml` maps onto these keys rather than replacing them.

- **A blank value reads as unset.** That is D14's `ALLOW_DIR` rule.
- **An unparseable value disables that upstream, never the daemon.** It is logged at `error`, and requests to that upstream are refused.

  *[Amended M6-F3 (SPEC §22.2 (cw)): an **unknown key** also disables its upstream, because a typo widens rather than narrows (`…_ALLOW_PATH=/api` would leave `ALLOW_PATHS` at `/`). Keys are matched exactly against the table below, so `api` and `api_http` both resolve; a variable two declared upstreams could both own disables both, and a `FERRO_UPSTREAM_*` variable no declared name owns is reported for the startup log. An unparseable or unknown **daemon-wide** `FERRO_HTTP_*` key disables the whole HTTP service, never the daemon (a `NAT64_PREFIXES` the engine half-applied would silently classify the wrong addresses). Upstream names are 1–64 bytes of `[A-Za-z0-9._-]` and never `_unknown`, the metrics label every `forbidden_upstream` counts under (§23.10.2); names that map to one `<NAME>` all disable. A refused configuration's error names the upstream and the key and never quotes the value. Booleans are exactly `0` or `1`. `ORIGIN` is parsed strictly: lowercase is required, not applied; no trailing `/` or `.`; no IDNA mapping (a non-ASCII host is refused); and a host whose last label is numeric must be a canonical dotted-quad, so no `inet_aton` spelling (`2130706433`, `0x7f.1`, `0177.0.0.1`, `127.1`) reaches the resolver unclassified. An IP-literal `ORIGIN` in the always-refused table (§23.8.5) disables the upstream at start. TLS keys on an `http://` origin are refused; `CLIENT_CERT_FILE` and `CLIENT_KEY_FILE` come as a pair; `RATE_BURST`/`RATE_MAX_WAIT_MS` without `RATE_PER_SEC` are refused, and `RATE_PER_SEC` takes up to three fractional digits.]*
- **Configuration is read once, at start.** There is no reload in v1 (C18).

**Daemon-wide keys:**

- `FERRO_HTTP_DRAIN_MS` (default 30000, §23.6.1);
- `FERRO_HTTP_MAX_BODY_BYTES` (default 256 MiB, §23.8.6);
- `FERRO_HTTP_NAT64_PREFIXES` (none; extra `/96` prefixes decoded like `64:ff9b::/96`, §23.8.5);
- `FERRO_HTTP_SLOW_LOG_MS` (off, §23.10.3).

**Per-upstream keys:**

| Key | Default | Meaning |
|---|---|---|
| `ORIGIN` | — (required) | `https://host[:port]` or `http://host[:port]`. Lowercase ASCII (punycode for IDN), no path, userinfo or query. An IP literal is allowed. |
| `ALLOW_PATHS` | `/` | Comma-separated path prefixes (§23.4.2 step 9). |
| `PATH_PARAMS` | `refuse` | `allow` permits a raw `;` in the path. The dot-segment check then cuts each segment at its first `;` (§23.4.2 steps 6d and 7). |
| `PATH_ENCODING` | `ascii` | `utf8` permits percent-encoded bytes ≥ 0x80 in the path, which must form valid UTF-8, with an NFKC dot check (§23.4.2 step 8). |
| `ALLOW_METHODS` | every method except the always-refused ones | Narrows the methods. `CONNECT`, `TRACE` and `TRACK` can never be allowed. |
| `ALLOW_UIDS` | any admitted peer | Peer uids allowed to use this upstream (the D15 mechanism). |
| `ATTACH_HEADERS_FILE` | none | `Name: value` lines attached to every request. These headers are daemon-owned: never logged, never exported (§23.3.2). |
| `ATTACH_POLICY` | **`refuse`** | What happens to a PHP-supplied header that an attached header names. `refuse`: refuse the request as `Forbidden` (`forbidden_header`). `override`: discard PHP's value and count it. `override` is for upstreams where **PHP must never send its own value for that header.** Under per-end-user auth it silently substitutes the daemon's identity for the user's (a confused deputy), which is why it is not the default. |
| `PASS_HEADERS` | none | Names exempted from §23.4.3's default refusal list (the override and forwarding headers). Pseudo-headers, `host`, `upgrade` and `proxy-authorization` cannot be exempted. |
| `IDEMPOTENT_METHODS` | **empty** | The operator's declaration that these methods are idempotent *on this upstream* (§23.7.2). Any methods except the always-refused ones. For a REST API whose `GET`s have no side effects the documentation gives `GET,HEAD,OPTIONS` as the usual value; the engine never assumes it (decided, §23.18 Q8). |
| `IDEMPOTENCY_KEY_HEADER` | none | The name of a header whose non-empty presence makes a request idempotent. Set it only for an upstream that implements the IETF `Idempotency-Key` draft (Stripe, Adyen …). |
| `HTTP` | **`1.1`** | `1.1`: HTTP/1.1 only. `auto`: also offer ALPN `h2` on `https`, and send *effectively-idempotent* requests over HTTP/2 when it is negotiated (§23.8.3). |
| `CA_FILE` | the OS store | PEM roots that **replace** the OS store for this upstream. |
| `CLIENT_CERT_FILE`, `CLIENT_KEY_FILE` | none | mTLS. The key file is credential material. |
| `MIN_TLS` | `1.2` | `1.2` or `1.3`. |
| `ADDRESS_CLASSES` | `public` | The address classes a resolved address may belong to: any of `public`, `private`, `loopback` (§23.8.5). An IP-literal `ORIGIN` implicitly permits its own literal. |
| `ALLOW_METADATA` | `0` | Permits the link-local and cloud-metadata ranges for this upstream (§23.8.5). |
| `PARTITION` | `none` | `uid`: one connection pool per peer uid, instead of one per upstream (§23.8.1). |
| `CONNECT_TIMEOUT_MS` | 5000 | Default and ceiling for DNS + TCP + TLS. |
| `TIMEOUT_MS` | 600000 | The total-exchange ceiling, and the default when a request sends none. |
| `READ_TIMEOUT_MS` | none | Default idle bound between received body bytes. |
| `MAX_CONNECTIONS` | 32 | |
| `MAX_REQUESTS` | 128 | Requests in flight at once. |
| `MAX_QUEUED` | 256 | Requests waiting for a slot. |
| `MAX_BODY_BYTES` | 64 MiB | Request-body bytes held for this upstream, from admission until fully written (§23.8.6). |
| `QUEUE_TIMEOUT_MS` | 5000 | |
| `MAX_DIALS` | 4 | |
| `IDLE_TIMEOUT_MS` | 15000 | Lowered to the server's `Keep-Alive: timeout` minus 1 s. |
| `H1_UNSAFE_REUSE_MAX_IDLE_MS` | 2000 | §23.8.2. |
| `MAX_LIFETIME_MS` | 300000 | |
| `DNS_TTL_MS` | 60000 | |
| `MAX_RESPONSE_BYTES` | unlimited | Wire bytes per response. Exceeding it is `ResponseIncomplete`. |
| `BREAKER_FAILURES` / `BREAKER_COUNTS` / `BREAKER_OPEN_MS` | 5 / `connect` / 5000 | §23.8.6. |
| `RATE_PER_SEC`, `RATE_BURST`, `RATE_MAX_WAIT_MS` | off / = rate / 0 | §23.8.7. |
| `HONOR_RETRY_AFTER`, `RETRY_AFTER_MAX_MS` | `0` / 60000 | §23.8.7. |
| `LOG_ROUTE` | `1` | §23.10. |

#### 23.3.2 Credential custody

- **Attached headers come from files, never from environment values.** `/proc/<pid>/environ` is readable by the daemon's uid and is inherited by children. A file works with systemd's `LoadCredential=` at mode `0400`. The DSN-in-environment debt is not extended (C10).
- **Values are validated as field-values at load.** They are held in a type with no `Debug` or `Display`, and rendered only into the outbound request. The canary gate proves it (§23.10.4).
- **Rotation needs a restart in v1** (C18).
- **`Host` comes from `ORIGIN` alone.**

#### 23.3.3 The isolation guarantee is only as real as the uid split

§12's promise ("PHP never sees the credential") needs PHP-FPM's uid to differ from `ferrod`'s. Under the same uid, PHP can read the attached-header file, read `/proc/<ferrod>/environ`, or ptrace the daemon.

`ferrod` checks this cheaply at startup. It logs at `warn`, naming each upstream that carries attached headers (never their values), that credential isolation is **void** when any of these holds:

- `FERRO_ALLOW_UIDS` is empty, so only the daemon's own uid can connect (D15's `uid_allowed`);
- `FERRO_ALLOW_UIDS` contains `ferrod`'s own uid;
- an upstream's `ALLOW_UIDS` contains `ferrod`'s own uid.

It does not refuse to start, because the first case is the normal developer setup.

#### 23.3.4 What custody does not protect

An upstream endpoint that **reflects request headers** returns the attached credential to PHP. httpbin's `/headers` and `/anything`, and some debug routes, do exactly that. The engine cannot prevent this without reading response semantics (rule E).

- `TRACE`/`TRACK` are refused for this reason.
- Operators narrow `ALLOW_PATHS`. That narrowing is itself bounded: query-string routing is not confined (§23.4.2), and neither is body-level method override (§23.4.3).

The incompatibilities page states all three in D14's *Files and paths* style.

---

### 23.4 Request validation: the SSRF rule

**PHP can reach only declared upstreams, at their declared origin, on permitted path prefixes, with permitted methods.** That is what the engine enforces on the bytes it sends. Whether the *upstream* re-routes on content the engine does not interpret (the query string, a body field) is outside the guarantee, and the text below says where.

Every refusal here happens before any dial and before any byte is sent. It is **NonRetryable `Forbidden` (`0x300C`)**, whose meaning widens to "the engine's own access policy" (C6).

- **`detail` carries the reason token** from the registry vocabulary (§23.5.6): `forbidden_upstream`, `forbidden_origin`, `forbidden_target`, `forbidden_method`, `forbidden_header`, `forbidden_body`, `forbidden_address`.
- **The message** may name a byte offset or a header *name*, and never a PHP-supplied value.
- **An unknown upstream and an upstream this peer's uid may not use give one identical refusal** (`forbidden_upstream`, "upstream not available to this peer"). That is the D15 indistinguishability rule (C7). Metrics preserve it (§23.10.2).

#### 23.4.1 Method

The method must be an RFC 9110 `token`, 1–32 bytes, and is sent as given.

- **Always refused:** a method whose ASCII-uppercase form is `CONNECT`, `TRACE` or `TRACK`. Case-insensitive, because some servers are (review F16 d).
- **Also refused:** a method outside `ALLOW_METHODS`. That comparison is case-sensitive, because RFC 9110 methods are.

#### 23.4.2 Target

The `target` is checked by the following algorithm over its raw bytes. Every step refuses; no step rewrites. The engine sends exactly the bytes it accepted.

1. **Length.** 1..=8192 bytes.
2. **Form.** The first byte is `/` and the second is not `/`. This refuses absolute-, authority- and asterisk-form, and network-path references.
3. **Byte allow-list.** Every byte is one of `A–Z a–z 0–9 - . _ ~ ! $ & ' ( ) * + , ; = : @ / ? %`. That refuses every control byte, space, DEL, `#`, `\`, `"`, `<`, `>`, `[`, `]`, `^`, `` ` ``, `{`, `|`, `}` and every byte ≥ 0x80 (§23.19 P10).
4. **Percent syntax.** Every `%` is followed by two hex digits, and `%00` is refused anywhere.
5. **Split.** The *path* is everything before the first `?`, and the *query* is the rest. Steps 6–9 apply to the path only.
6. **Path escapes.** In the path, refuse:
   - (a) `%25`, in any case. No double-encoding can then exist in the path, so one decode is the fixpoint.
   - (b) a percent-encoded control byte (`%01`–`%1F`, `%7F`).
   - (c) a percent-encoded byte ≥ `0x80`, unless the upstream sets `PATH_ENCODING=utf8` (step 8).
   - (d) a raw `;`, unless the upstream sets `PATH_PARAMS=allow`. Tomcat, Jetty and Spring strip `;params` from each segment before normalising, so `/api/..;/admin` is `/admin` to them.
7. **Dot segments.** Take a copy of the path and decode every `%XX` once; by step 6a, this is the fixpoint. Split the copy on `/` and `\`. When `PATH_PARAMS=allow`, cut each segment at its first `;`. Refuse any segment that is non-empty, consists only of `.` and space characters, and contains at least one `.`. That covers `.`, `..`, `...` and `.. `, the last two being Windows/IIS trailing-dot-and-space forms. The copy is used only for this check. *Encoded slashes are otherwise allowed* (GitLab addresses projects as `group%2Fproject`).
8. **UTF-8 opt-in** (`PATH_ENCODING=utf8`). The decoded copy must be valid UTF-8, which refuses overlong forms such as `%c0%ae` by construction. Step 7's check then also runs on each segment's NFKC normalisation, which catches fullwidth `．` (U+FF0E), one-dot and two-dot leaders, and similar characters. The default refuses non-ASCII escapes outright. Wikipedia-style APIs need the opt-in.
9. **Prefix confinement.** Some prefix `P` in `ALLOW_PATHS` must match the raw path `T`, case-sensitively. `P` matches when `T == P`, or when `T` starts with `P` and either `P` ends with `/` or the byte after `P` in `T` is `/`. An encoded form of the prefix (`/%61pi`) does not match, which is conservative.

*[Amended M6-F3 (SPEC §22.2 (cw)), all stricter than the text above, never looser:* **step 7 cuts each decoded segment at its first `;`, `?` or `#`, whatever `PATH_PARAMS` says.** With `refuse`, a raw `;` is already refused, so a `;` in the decoded copy came from `%3B`, and a server that decodes *before* stripping `;params` reads `/api/..%3B/admin` as `/admin`. A decoded `?` or `#` (`%3F`, `%23`, or through step 8 their compatibility forms U+FF1F, U+FE56, U+FF03) is cut for the same reason: code that percent-decodes the path and then parses it as a URL reads `/api/..%3F` as `/api/..` plus a query (mainstream servers do not — nginx does not re-feed a decoded `?`, `#` or `%` — so the reach is application code, one level up). Cutting can only refuse more. **`+` is not a path decoding:** it means space only in `application/x-www-form-urlencoded`, which the query may be and the path is not, so the dot check does not model it (a server that read `..+` as `.. ` would see a segment step 7 refuses in its `%20` form; no change). **Step 8 normalises the whole decoded path, then splits** (which contains the per-segment check and also catches a compatibility solidus, backslash or semicolon, `／` U+FF0F, `＼` U+FF3C, `；` U+FF1B, making a segment boundary), and **refuses a path whose NFKC form contains `%`** (the decoded copy holds none after step 6a, so it came from a character such as U+FF05, which a server that normalises and then percent-decodes would decode twice). **Every `ALLOW_PATHS` entry must itself pass steps 1–8** as a target with no query, under the upstream's own `PATH_PARAMS`/`PATH_ENCODING`; `PATH_PARAMS=allow` does not relax step 9, so `/x;v=1` does not match the prefix `/x`.]*

**What the guarantee does not cover, stated:**

- **The query string is not confined.** An upstream that routes on it (`index.php?r=admin`, `?action=delete`) is reachable at any route the query can name. Operators who must confine such an upstream cannot do it with `ALLOW_PATHS`.
- **A case-insensitive server** treats `/API` as `/api`. Prefix matching is case-sensitive, so this can only *refuse* more, never admit more.

**The `origin` field.** PHP may state the origin it believes it addresses. When present, it must be byte-equal to the upstream's normalised `ORIGIN`: lowercase, default port removed, IPv6 in brackets. A mismatch is `forbidden_origin`. The engine never *uses* the field. The check turns a drifted PHP-side map into a loud refusal.

**Property gate (slice F3).** A `cargo-fuzz` target and a property test assert, for every accepted input, that the request line a test upstream *received* carries exactly that target and that `Host` is exactly the upstream authority.

*[Amended M6-F3 (SPEC §22.2 (cw)): F3 has no network, so it owns the **offline half**: for every accepted generated request the HTTP/1.1 head the plan describes is rendered and parsed back as an upstream reads it (the request-target is byte-identical, there is exactly one `Host` and it is the authority, the line count is the plan's — no injection); every accepted request passes an oracle written independently of the validator; and it **re-validates identically after a round trip** (the request the engine would send is itself accepted, with the same target, kept headers and effective idempotency). Three `cargo-fuzz` targets (`validate_request`, `parse_origin`, `classify_address`) share those bodies with `cargo test`. The **wire half** — the bytes a test upstream received through `hyper` — needs the engine, and is owed by slice F4.]*

**The refusal corpus (chaos case 13)** includes:

- `/api/..;/admin` and `/api/.%2e;/admin`;
- `/api/%25%32%65%25%32%65/admin`;
- `%c0%ae`, `%e0%80%ae` and `%ef%bc%8e` dots;
- `%0d%0a` in the path;
- `/api/...`;
- the absolute, authority and `//` forms;
- CR/LF/NUL in header values.

#### 23.4.3 Headers

*[Amended M6-F3 (SPEC §22.2 (cw)): **every rule in the table below looks the name up FOLDED** — ASCII-lowercased, with every non-alphanumeric byte mapped to `-` — so `X_Forwarded_For`, `X.Forwarded.For` and `x-forwarded_for` all hit the forwarding row, and `X_Api_Key` is an attached name. CGI-style servers never see header names, only `HTTP_*` variables: `php -S`, WSGI (`wsgiref`), gunicorn before 22 and Puma before its CVE-2024-45614 fix merge those spellings into one variable, so an exact-name table let PHP smuggle a refused header — or a second value for an attached credential, which under `php -S` REPLACED the daemon's — past the engine (reproduced by the review). The name sent is still PHP's own bytes. The one exception is `IDEMPOTENCY_KEY_HEADER`, matched exactly (case-insensitively): folding there would widen a licence, declaring a request idempotent on a spelling a non-folding upstream never deduplicates.]*

- **Names** are RFC 9110 tokens, 1–256 bytes.
- **Values** are field-values (`0x21–0x7E`, `0x80–0xFF`, interior `SP`/`HTAB`). Every other control byte is refused, so header injection is refused, never stripped.
- **At most 100 lines and 64 KiB** of names plus values. Order and duplicates are preserved.

| Header (case-insensitive) | Rule |
|---|---|
| `:`-prefixed | Refused. |
| `host` | Must equal the normalised upstream authority, otherwise refused. Then dropped, because the engine sets `Host`/`:authority`. |
| `content-length` | Must equal the body length (an absent body is 0), otherwise refused. Then dropped and recomputed. |
| `transfer-encoding` | Exactly `chunked` is dropped. Anything else is refused. |
| `connection`, `keep-alive`, `proxy-connection`, `te` (except `te: trailers`), `http2-settings` | Dropped (hop-by-hop on a hop that is not HTTP). |
| `expect` | Dropped. The body is already complete. |
| `upgrade`, `proxy-authorization` | Refused. |
| **Method and URL overrides:** `x-http-method-override`, `x-http-method`, `x-method-override`, `x-original-url`, `x-rewrite-url`, `destination` | **Refused** unless named in `PASS_HEADERS`. They re-target the method or path server-side after the engine's checks (review F19). *[Amended M6-F3: `destination` added — WebDAV `MOVE`/`COPY` name a second target the server writes to. Cost: WebDAV clients cannot `MOVE`/`COPY` through Ferro in v1 unless the operator lists `Destination` in `PASS_HEADERS`.]* |
| **Forwarding:** `forwarded`, any `x-forwarded-*`, `x-real-ip`, `true-client-ip`, `cf-connecting-ip`, `x-client-ip`, `client-ip`, `x-host`, `x-original-host` | **Refused** unless named in `PASS_HEADERS`. Fronts that trust them re-route or re-authorise on them. *[Amended M6-F3: the last six added, the client-identity and host headers CDNs and frameworks trust.]* |
| A name in the upstream's attached set | Refused (the default), or overridden and counted, per `ATTACH_POLICY`. |
| `accept-encoding` | Passed as given. When the request asks for decoding and PHP set none, the engine sets `gzip, deflate`. |
| Everything else | Sent verbatim. **The engine adds no header except `Host`, `Content-Length`, the attached headers and the decode-only `Accept-Encoding`.** |

**Not refusable, and stated:** a body-level override (Symfony/Laravel `_method` in a form body, JSON-RPC method fields). Method and path confinement is only as real as the upstream's refusal to honour overrides the engine cannot see.

**Header-name case.** On HTTP/1.1, request header names are sent title-cased (`hyper`'s `title_case_headers`; exact case is not preservable, P6). HTTP/2 lowercases them, as RFC 9113 requires.

*[Amended M6-F4a (SPEC §22.2 (cz)): "order and duplicates are preserved" holds per NAME, not across names. The request goes through `http::HeaderMap`, which keeps every value of a name in order but groups a name's values together at its first occurrence, so PHP's `a: 1`, `b: 2`, `a: 3` is sent as `a: 1`, `a: 3`, `b: 2`; `hyper`'s header-order extension is crate-private (P6's finding, again). RFC 9110 §5.3 makes the relative order of DIFFERENT field names insignificant, so a conforming upstream sees the same request. `Host` is sent first, then PHP's kept headers, then the attached ones, then `Content-Length`.]*

#### 23.4.4 Body

The body is a `bin` of at most `MAX_FRAME_PAYLOAD` minus the encoded size of the rest of the frame. The client computes that size exactly and refuses a larger body before sending (§23.9.3). A `GET` or `HEAD` with a body is passed through, as curl does.

*[Amended M6-F3 (SPEC §22.2 (cw)): a body larger than the smaller of the upstream's `MAX_BODY_BYTES` and `FERRO_HTTP_MAX_BODY_BYTES` can never be admitted, so it is `forbidden_body` (NonRetryable) at validation rather than a Retryable `body_budget` that no retry could satisfy; `body_budget` keeps its meaning of *current* occupancy (§23.8.6). Headers, as built: `transfer-encoding: chunked` and `te: trailers` compare their value ASCII-case-insensitively; `content-length` compares numerically (leading zeros allowed, nothing else); an `Accept-Encoding` in the attached set suppresses the decode-only one. `PASS_HEADERS` may name only the override and forwarding headers (anything else is a configuration error, not a no-op). `IDEMPOTENCY_KEY_HEADER` may not name, in any folded spelling, a header the engine sets, drops, refuses or attaches — `accept-encoding` included, which the engine may set and Guzzle sends on most requests — since an attached key would declare every request idempotent with one constant key. The attached-header file is `Name: value` lines (LF or CRLF, blank lines skipped, no comment syntax, surrounding `SP`/`HTAB` trimmed), at most 100 lines and 68 KiB, and may not name `host`, `content-length`, `transfer-encoding`, a hop-by-hop header, `expect`, `upgrade` or `proxy-authorization`. An upstream with `ALLOW_UIDS` refuses a peer whose uid the transport did not attest. A refusal is reported as a `Rule` grouped into the seven `forbidden_*` causes; the wire tokens themselves are not spelled in `ferro-http` (charter rule 2) — F4 maps the group to F2's generated `[http.causes]` constants.]*

---

### 23.5 Wire contract (`/proto`)

**Registry additions** (allocated here; entered in `/proto` at slice F2):

- `methods.toml`:
  - `[services] HTTP = 6`;
  - `[methods.http] REQUEST = 1, HEAD = 2, BODY = 3`;
  - `[features.engine] HTTP = 0x08`, set when the build includes the cargo feature. The client must check it, because a `--no-default-features` engine has the same registry hash.
- `errors.toml`, codes allocated explicitly, with nothing else taken:

  | Code | Name | Branch |
  |---|---|---|
  | `0x1007` | `UpstreamUnavailable` | Retryable |
  | `0x1008` | `RateLimited` | Retryable |
  | `0x300D` | `TlsRefused` | NonRetryable |
  | `0x300E` | `ResponseIncomplete` | NonRetryable |

  `Forbidden`'s comment is widened (C6).
- `errors.toml` `[http.causes]`: the closed cause vocabulary (§23.5.6), generated into both codecs.

**Dispatch routes per method (review F26).** `dispatch::route(HTTP, REQUEST)` returns `Route::Request`. `route(HTTP, HEAD | BODY | anything else)` returns `Route::Unsupported`, because `HEAD` and `BODY` are engine → client only. Slice F2 pins both with a test and keeps `route` total.

**HTTP does not reuse `STREAM`'s `HEAD`/`DATA`,** whose payloads are SQL `ColMeta` and `Value` rows. Credit is keyed by `request_id` and the `STREAM` flag, not by service. The one chassis change is that `Responder::send_stream_frame` takes the service id. **There is no per-request credit window:** HTTP uses the session's global window (`credit_frames` 64 / `credit_bytes` 16 MiB), so §5.2's large-frame rule and `Config::validate` hold unchanged (review F4).

**No `protocol_version` bump.** These are new messages on a new service, and the registry hash refuses skew at the handshake (the §11 ADMIN precedent). `HELLO_ACK` is not extended (C8).

All messages are `Value`-free positional arrays with strict arity (PROTOCOL.md §2, §8). Header values and bodies are `bin`. A header is `[name: str, value: bin]`.

*[Amended M6-F2 (SPEC §22.2 (cy)): **built**, as `ferro-proto` `messages::http` and PHP `Ferro\Protocol\Http*`, layouts in `/proto/PROTOCOL.md` §12. Decided where this section was silent: (1) **the codecs check the wire's types and widths, never HTTP semantics** — a status outside 200..=599, a version outside {10, 11, 20}, an empty or over-256 KiB `BODY` chunk is the producer's contract (F4), not a decode error, and the 256 KiB bound is therefore not a registry constant (no receiver enforces it); (2) **`body: nil` and a zero-length `body` are distinct** ("no body" vs an empty one); (3) **`HttpHead.decoded`'s `Content-Length` is bounded < 2^63** like every u64 here — structurally: the encoder writes `nil` for one it cannot represent so, and both decoders refuse one on the wire; (4) PHP decoders are strict and the PHP encoder refuses, before sending, what the engine's decoder would refuse. `ferrod` routes `REQUEST` into the request lifecycle, DECODES it (a malformed one is `Protocol`, §23.6 step 1) and answers `Unsupported`; `HEAD`, `BODY` and every other method id are `Route::Unsupported`. `HELLO_ACK` does not set `HTTP = 0x08` until a build serves HTTP (F4).]* *[Amended M6-F4a (SPEC §22.2 (cz)): **served.** `REQUEST` runs the §23.6 lifecycle when `ferrod` is built with the `http` feature (the default) and `FERRO_UPSTREAMS` is set; otherwise it is still `Unsupported`. `HELLO_ACK` sets `HTTP = 0x08` exactly when the engine serves — configured, and not disabled by a daemon-wide configuration error (§23.3.1), in which case every request is refused `forbidden_upstream` and the bit is clear. Until slice F5, an `https` upstream is `Unsupported` (no cause token): configured but not served.]*

#### 23.5.1 `HttpRequest` (service `HTTP`, method `REQUEST` = 1): client → server, arity 13

| # | field | type | notes |
|---|---|---|---|
| 1 | `upstream` | `str` | |
| 2 | `method` | `str` | §23.4.1 |
| 3 | `target` | `str` | §23.4.2. ASCII by construction, so non-UTF-8 is `Protocol`. |
| 4 | `origin` | `str \| nil` | checked, never used |
| 5 | `headers` | `array<[str, bin]>` | §23.4.3 |
| 6 | `body` | `bin \| nil` | §23.4.4 |
| 7 | `timeout_ms` | `u32 \| nil` | total exchange. `nil` means `TIMEOUT_MS`. Always capped by it. |
| 8 | `connect_timeout_ms` | `u32 \| nil` | capped by `CONNECT_TIMEOUT_MS` |
| 9 | `read_timeout_ms` | `u32 \| nil` | idle bound between body bytes |
| 10 | `idempotent` | `bool \| nil` | the caller's declaration (§23.7.2) |
| 11 | `decode` | `bool` | §23.9.2 |
| 12 | `route` | `str \| nil` | observability only. ≤ 256 bytes, the step-3 byte set plus `{` `}`. Never sent upstream. |
| 13 | `traceparent` | `str \| nil` | `ExecRequest` field 9's rule (PROTOCOL.md §8.1). Not forwarded upstream. |

#### 23.5.2 `HttpHead` (method `HEAD` = 2): server → client, arity 6, sent once

Not terminal and not flagged `STREAM`. It debits the request's credit like `STREAM`'s `HEAD`. Its size is bounded by §23.9.1's head limits, far below the window floor.

| # | field | type | notes |
|---|---|---|---|
| 1 | `status` | `u16` | 200..=599. A 1xx is consumed by the engine; 101 is malformed. |
| 2 | `version` | `u8` | `10`, `11` or `20` |
| 3 | `reason` | `bin \| nil` | HTTP/1.x reason phrase; `nil` on HTTP/2 |
| 4 | `headers` | `array<[str, bin]>` | as received, minus hop-by-hop, plus §23.9.2's changes. **Names are lowercase** (P6). |
| 5 | `decoded` | `[str, u64 \| nil] \| nil` | the removed `Content-Encoding`/`Content-Length` |
| 6 | `idempotent` | `bool` | the engine's **effective** idempotency (§23.7.2) |

#### 23.5.3 `HttpBody` (method `BODY` = 3): server → client, flag `STREAM`, arity 1

`[chunk: bin]`. The chunk is non-empty and at most **256 KiB**. It carries what was readable when the chunk was built, and the engine never waits to fill a chunk (SSE latency). Every chunk passes the existing `Responder` gauntlet: debit, then reserve, then enqueue.

#### 23.5.4 `HttpDone`: the terminal `Outcome::Ok` body, arity 2

`[trailers: array<[str, bin]>, stats: HttpStats]`.

`HttpStats = [queue_us, connect_us, tls_us, ttfb_us, total_us, bytes_sent, bytes_received, reused: bool]`. Each `u64` is bounded below 2^63.

*[Amended 2026-10-06 (M6-F1a review F-4, §22.2 (ct)). This sentence said "`bytes_received` counts wire bytes", which contradicted the plaintext tracker it is read from. On HTTP/1.1, `bytes_sent` and `bytes_received` are the write tracker's **plaintext** counts **since this exchange's dispatch** (§23.7.1): the HTTP message bytes, head and body framing included, without TLS record overhead and without any earlier exchange on a reused connection. That is the same count §23.5.6 uses to tell `eof_empty` from `eof_partial_head`, so the stat and the cause cannot disagree. On HTTP/2 a connection's bytes are shared by its streams, so per-stream counts are slice F5b's to define and measure.]*

#### 23.5.5 Golden vectors

- `http_request_get`;
- `http_request_post` (every field set, a `0xc0` body byte, a `0x80` header byte);
- `http_head`;
- `http_head_h2`;
- `http_body`;
- `http_done` (a trailer, plus a non-fixint `stats`);
- `error_upstream_unavailable` and `error_rate_limited` (both carrying `retry_after_ms`);
- `error_tls_refused`;
- `error_response_incomplete`;
- `error_forbidden_http` (a `detail` cause token).

`http_` joins the derived prefix list (PROTOCOL.md §7).

#### 23.5.6 The HTTP cause vocabulary (`detail` on HTTP terminals)

`ErrorPayload.message` is "not for programmatic matching" (PROTOCOL.md §5), but Guzzle's exception class depends on the cause (§23.11.3). So **on service `HTTP`, `ErrorPayload.detail` is exactly one token from `[http.causes]`**, generated into both codecs (charter rule 2). It is never free text and never a PHP-supplied value. `sqlstate` and `errno` are `nil` (C11).

*[Amended M6-F2 (SPEC §22.2 (cy)): **`detail` is `nil` on exactly two HTTP terminals, neither an exchange fate:** `Protocol` (a malformed frame, §23.6 step 1, and every session-built protocol fault) and `Unsupported` (an HTTP method with no route, and — until F4 — every `REQUEST`). Every other HTTP error carries one token. The table below is the registry's `[http.causes]` (45 distinct tokens, generated as `consts::http_cause::*` and `Constants::HTTP_CAUSE_*`), and a test parses it from this file and requires set equality, so the two cannot drift.]*

| Group | Tokens |
|---|---|
| Not sent: policy | `forbidden_upstream`, `forbidden_origin`, `forbidden_target`, `forbidden_method`, `forbidden_header`, `forbidden_body`, `forbidden_address` |
| Not sent: admission | `breaker_open`, `breaker_probe_busy`, `rate_limited`, `retry_after_hold`, `queue_full`, `queue_timeout`, `body_budget`, `deadline`, `draining` |
| Not sent: dial | `dns`, `connect_refused`, `connect_unreachable`, `connect_timeout`, `tls_handshake`, `tls_version`, `tls_alpn`, `tls_verify` |
| Dispatched, not sent *(added by §22.2 (ct), review F-3)* | `unsent_write`, `unsent_closed`; also `deadline` and `cancelled` |
| Sent, no head | `write`, `reset`, `eof_empty`, `eof_partial_head`, `malformed_head`, `oversize_head`, `informational_101`, `timeout`, `cancelled` |
| HTTP/2 (idempotent only in v1) | `h2_refused_stream`, `h2_goaway_above_last`, `h2_stream_error`, `h2_connection_error` |
| After head | `body_reset`, `body_eof`, `body_framing`, `decode`, `max_response_bytes`, `read_idle`, `timeout`, `cancelled` |

*[Amended M6-F4b (SPEC §22.2 (db)): **`draining` is also an after-dispatch token.** §23.6.1 stops an exchange that outlives `FERRO_HTTP_DRAIN_MS`, and that stop names the cause `draining` in every phase: "dispatched, not sent" (Retryable `UpstreamUnavailable`, as at admission), "sent, no head" and "after head" (the link-level rows, §23.7.1). The token set is unchanged — no `/proto` change — and a token may already sit in several groups (`deadline`, `cancelled`, `timeout`).]*

`eof_empty` means the connection closed after the request was sent with zero response bytes received, which is curl's `GOT_NOTHING`. `reset`, `write` and `eof_partial_head` are curl's `RECV_ERROR`/`SEND_ERROR`/`WEIRD_SERVER_REPLY` class. Distinguishing them through `hyper` is premise P14.

*[Amended 2026-10-06 (M6-F1a, P14 measured, §22.2 (ct)): **how each token is derived, because `hyper`'s error alone does not separate them.** P14 holds with three caveats, none of which changes a fate or a Guzzle class.*

- *`eof_empty` and `eof_partial_head` are the **same** `hyper` error (`IncompleteMessage`). The write tracker's **read count** separates them: zero response bytes means `eof_empty`, more than zero means `eof_partial_head`. They map to different Guzzle classes (§23.11.3), so the tracker counts reads as well as writes. That count is `HttpStats.bytes_received`, and it **starts at this exchange's dispatch** (review F-4): F4 reuses keep-alive connections, and a count over the connection's life would turn every `eof_empty` on a reused connection into `eof_partial_head` (a `ConnectException` into a `RequestException`). Measured on a reused connection: the first exchange read 40 bytes, the second closed with none, and only the dispatch-relative count classifies it `eof_empty`.*
- *`oversize_head` and `malformed_head` are both `is_parse()`. `hyper::Error::is_parse_too_large()` is compiled only with `hyper`'s `server` feature, which D20 does not enable. The separating signal is the error's `Display` text, `"message head is too large"`. Slice F4 pins it with a test, so an upgrade that changes the text fails loudly. If it ever drifts unnoticed, the result degrades to `malformed_head`, which has the same class and the same fate.*
- *`write` is named only when the tracker saw the **write** side fail first. `hyper`'s h1 client reads for an early head while it writes the body, so a reset during the body write is normally observed on the read side and classified `reset` (5 of 5 runs). Both tokens are `RequestException` and both are "sent, no head", so P14's "stricter class for both" rule is already met and §23.11.3 is unchanged.*

*The **dispatched, not sent** group (review F-3) names an I/O failure after dispatch with `sent = false` (§23.7.1): `unsent_write` when the first failure was on the write side, `unsent_closed` when the connection was found closed or reset first. P1's controls and P19's case C produce exactly this state.*

*Measured besides: `hyper` returns a **101** as an ordinary response head, so the engine itself must turn it into `informational_101`. Other 1xx responses are consumed by `hyper` and never surface. On HTTP/2, an over-limit head is a library-initiated `RST_STREAM(PROTOCOL_ERROR)`, identical to a malformed header block, so it is `h2_stream_error` (§23.9.1). Evidence: `engine/crates/ferro-http-spike/`.]*

---

### 23.6 Request lifecycle

1. **Decode.** A malformed frame is `Protocol`.
2. **Upstream.** Resolve the upstream and check the uid. Failure is `forbidden_upstream`.
3. **Validate** (§23.4), and compute effective idempotency (§23.7.2). That selects the HTTP/1.1 or HTTP/2 sub-pool (§23.8.3).
4. **Admit,** in this order:
   - (a) drain state;
   - (b) breaker and Retry-After hold;
   - (c) rate limit;
   - (d) the body budget;
   - (e) the queue, under `MAX_QUEUED`.

   Time from (e) to step 6 is `queue_us`. The request's total deadline runs from step 1. A deadline that elapses before dispatch, or after dispatch while nothing has been sent (§23.7.1, "dispatched, not sent"), is Retryable `PoolTimeout` (`deadline`).

   *[Amended M6-F4b (SPEC §22.2 (db)): (a) and (d) are built; (b), (c) and (e) are slice F6's.]*
5. **Acquire a connection** under `MAX_REQUESTS`, `MAX_CONNECTIONS` and `MAX_DIALS`. Reuse an idle one (§23.8.2), or dial: DNS, the address guard, TCP to the checked `SocketAddr`, TLS. Dialling another address after a failed dial is not a retry, because no byte of the request exists anywhere yet.
6. **Dispatch.** Arm the write tracker (§23.7.1) and hand the request to `hyper`.
7. **Head.** On a final response head, send `HEAD`. **The transport's part of the fate is now settled:** the upstream answered, and no transport outcome after this point can be "maybe not received". What the status means (applied? retry?) is §23.7.4's, and lives in PHP.
8. **Body.** Stream `BODY` under credit. While credit is exhausted the engine stops reading, so HTTP/1.1 sees TCP backpressure and HTTP/2 sees stream flow control.
9. **Terminal.** Exactly one `END`: `Ok(HttpDone)`, `Error` (§23.7), or `Cancelled`. The request's body is released from the budget once written, or at the terminal.

**Session death** cancels the session's in-flight exchanges, as curl's process death does. The client applies §23.7.3.

#### 23.6.1 Drain and restart (review F5, F12)

On `SIGTERM`:

- **New HTTP requests** are refused at once with Retryable `UpstreamUnavailable` (`draining`), not sent.
- **In-flight exchanges** keep running until their own terminal or `FERRO_HTTP_DRAIN_MS` (default 30 s), whichever comes first. Then they are cancelled and classified by §23.7.1. A sent non-idempotent request with no head is Indeterminate; a delivery in progress is `ResponseIncomplete`.

**Chassis changes, listed in §23.17 and recorded in §5** (review of the adoption, R13):

1. **A drain signal reaches the sessions.** Today `serve` stops accepting and then hard-aborts every session task when its wait ends (`serve.rs`); sessions are never told the daemon is draining. The refusal of new HTTP requests above, and §24.8's immediate answer to parked `RESERVE`s, both need that signal plumbed into the session layer, the HTTP service and the queue waker. P17 covers only that frames are still read.
2. **`serve` outlasts the HTTP cap.** While HTTP exchanges are in flight, `serve`'s wait becomes `FERRO_HTTP_DRAIN_MS + drain_deadline`, not their maximum, so an exchange cancelled at the cap still has `drain_deadline` to emit its engine-classified `END` before the hard abort. Otherwise the client classifies by §23.7.3, which cannot see an operator's `IDEMPOTENT_METHODS` and reports a declared-idempotent request without a head as `Indeterminate` where the engine would have said `Retryable`.

SQL draining is unchanged.

*[Amended M6-F4b (SPEC §22.2 (db)), **as built**:*

- *Chassis change 1. `shutdown::Drain` records the instant it was triggered (set before its token is cancelled, so every observer agrees on it). `serve` spawns every session with that `Drain` (`Session::run_draining`; `run_with_handler` keeps its signature and passes one that never fires), and the session hands it to its handler factory as `SessionInfo::drain`. The session layer itself does nothing different on a drain; the HTTP service builds the engine's `DrainView` from it. A request that reaches admission step 4(a) while the daemon drains is refused `draining`, unsent — after validation, so a request that would be refused anyway still gets its `forbidden_*` (§23.6's order).*
- *The cap. An exchange admitted before the drain runs until its own terminal or `FERRO_HTTP_DRAIN_MS` after the drain began. At the cap the engine stops it through the same cancellation path a `CANCEL` takes — so the connection is discarded (aborted and awaited) before `sent` is read — and it classifies with the new `Drain` events (§23.7.1 as amended): **the link-level rows, cause `draining`**. Not the `CANCEL` rows (a declared-idempotent request would be `Cancelled`, which the client never asked for) and not the timeout rows (`QueryTimeout`); only the link rows give what this section states — Indeterminate for a sent non-idempotent request without a head, `ResponseIncomplete` for a delivery in progress, and Retryable for a declared-idempotent request, the answer chassis change 2 exists to deliver. Before any byte was sent the stop is Retryable `UpstreamUnavailable` (`draining`), the admission answer.*
- *Chassis change 2. `serve` waits `drain_deadline` from the drain's start as before; if Ferro HTTP exchanges are still in flight then, it keeps waiting until `FERRO_HTTP_DRAIN_MS + drain_deadline` from the start (the sum, pinned by a unit test), and ends earlier — `drain_deadline` after the in-flight count reaches zero — once every exchange has returned its terminal. With no HTTP in flight at `drain_deadline` the drain is exactly what it was. **Cost, stated:** sessions are shared, so a session that carries an HTTP exchange keeps serving SQL during the extension too; "SQL draining is unchanged" holds for the decision, not for how long a busy session lives.*
- *Review round (§22.2 (db)). Pinned by tests, each failing without its line: `sent` read only after the teardown, for the drain cap AND a `CANCEL`, while `hyper`'s first write is still in progress on another worker; the cap reaching a `BODY` parked on credit is the drain row (`ResponseIncomplete`, `draining`), not `Cancelled`; validation before the drain check; no extension with nothing in flight; and the extension's hard end measured from the drain's START (an exchange the cap cannot stop on time — a connector that blocks its thread — ends `serve` at `FERRO_HTTP_DRAIN_MS + drain_deadline` from the start, not from the extension's entry). **`FERRO_HTTP_DRAIN_MS` ≤ `drain_deadline` is not extended, by decision:** an exchange the cap stops before `drain_deadline` has the rest of `drain_deadline` to deliver its `END`, the slack an SQL `END` produced at that instant has; at exact equality the two timers fire together and the extension is entered only if the stopped exchange has not yet returned — which it almost always has not, since it must first abort and await its connection (16 of 16 in the review's probe) — so the case that can lose an `END` is one produced at the hard deadline itself, as for SQL. Operators set the cap clearly above or below `drain_deadline`.*]*

**§18 (amended):** the systemd unit's `TimeoutStopSec` must exceed `FERRO_HTTP_DRAIN_MS + drain_deadline`.

**Cost, stated (C17; accepted, §23.18 Q9):**

- Every `ferrod` restart is a host-wide event for HTTP. An LLM stream or a slow POST that outlives the drain dies with it, where curl in each worker would have finished.
- With no reload (C18), every credential rotation is such a restart.

Operators bound the damage by scheduling restarts and by keeping the drain above their p99 exchange time. The engine bounds it by refusing new work first. A crash (`SIGKILL`) has no drain at all, and the client applies §23.7.3.

---

### 23.7 The fate taxonomy

#### 23.7.1 Engine-side fate

**`sent` is a measured fact.**

- **HTTP/1.1** (every non-idempotent request in v1). The connection carries one exchange at a time. A tracker wraps the connection's *plaintext* I/O, below `hyper` and above TLS, and, on `https`, a second counter wraps the *transport* below TLS. Both are armed at dispatch, and every count starts at zero there. **`sent = true` once the plaintext layer has accepted one or more bytes since dispatch, or the socket has accepted one or more ciphertext bytes since dispatch.** TLS handshake bytes precede arming. The ciphertext half is required, not belt-and-braces: `tokio-rustls` can fail a plaintext write after earlier encrypted records of that same write reached the socket, so the plaintext count can read zero while the upstream has decrypted the request head (review F-1, measured). It is conservative: a record that carries no request byte (an alert, say) also counts. `hyper`'s `try_send_request`, which returns an unserialised message, is a cross-check (P19). *(Text corrected 2026-10-06, §22.2 (ct): this bullet said "bytes buffered into rustls count as sent, which is conservative". That is false on the error path, where a buffered write returns `Err` and is never counted.)*

  *[Amended 2026-10-06 (M6-F1a, §22.2 (ct)): **P1 holds; P19 holds in one direction only.** Measured:*
  - *Through `hyper::client::conn::http1`, no byte reaches the I/O between handshake and dispatch.*
  - *On a successful exchange, the plaintext count equals the bytes the upstream received (over TCP) or decrypted (through TLS).*
  - *On a failed TLS write it does not (review F-1). With the first post-dispatch socket write passing and the second failing, the upstream decrypted the request head and 16 KiB of body while the plaintext layer had accepted **nothing**. The ciphertext count saw the record, which is why `sent` is the OR of the two.*
  - *A connection dead at its first write, or reset before dispatch, reports `sent = false`.*
  - *`hyper` writes **vectored** on both transports, so the tracker must count `poll_write_vectored` as well as `poll_write`.*

  *`try_send_request` returns the message only when the dispatcher never dequeued it. A request that `hyper` has already serialised into its own buffer is lost to the caller even when the first I/O write then fails with no byte sent. So the only valid cross-check is **message returned ⇒ not sent**. "No message" never implies "sent", and the tracker stays the sole authority for `sent = true`.]*
- **HTTP/2** (effectively-idempotent requests only in v1). A request is `sent` once it is handed to a ready `SendRequest`. No post-send failure of an idempotent request is `Indeterminate` — a link-level one is `Retryable`, and a timeout or cancel follows §9.2's read rule (the table below) — so v1 needs no HTTP/2 not-processed licence (§23.8.3).
- **No partial-write refinement.** The first byte is the line.

**"Head received"** means `hyper` returned a complete final head (2xx–5xx).

The table below is total over (phase × event). The classifier `ferro_http::fate::classify` carries a totality test, as `fate_57014_total_over_all_axes` does. *Idem.* means effective idempotency (§23.7.2). *[Amended M6-F4b (SPEC §22.2 (db)): three drain-cap rows added (§23.6.1); the table is 54 situations × 2 columns = 108 cells, and `Indeterminate` ⇔ non-idempotent ∧ sent ∧ no head still holds as an equivalence over all of them.]*

| Phase | Event (cause token) | Idempotent | Non-idempotent |
|---|---|---|---|
| before dispatch | policy refusal (`forbidden_*`) | NonRetryable `Forbidden` | same |
| before dispatch | `rate_limited`, `retry_after_hold` | Retryable `RateLimited` + `retry_after_ms` | same |
| before dispatch | `breaker_open`, `breaker_probe_busy`, `draining` | Retryable `UpstreamUnavailable` (+ `retry_after_ms` for the breaker) | same |
| before dispatch | `queue_full`, `queue_timeout`, `body_budget`, `deadline` | Retryable `PoolTimeout` | same |
| before dispatch | `dns`, `connect_*`, and TLS transport failure (`tls_handshake`) | Retryable `UpstreamUnavailable` | same |
| before dispatch | `tls_verify`, `tls_version`, `tls_alpn` | NonRetryable `TlsRefused` | same |
| before dispatch | `CANCEL` | `Outcome::Cancelled` | same |
| dispatched, not sent | I/O failure with `sent = false` (`unsent_write`, `unsent_closed`) | Retryable `ConnectionLost` | same |
| dispatched, not sent | total deadline (`deadline`) | Retryable `PoolTimeout` | same |
| dispatched, not sent | `CANCEL` (`cancelled`) | `Outcome::Cancelled` | same |
| sent, no head | `write`, `reset`, `eof_empty`, `eof_partial_head`, `malformed_head`, `oversize_head`, `informational_101`; HTTP/2 `h2_*` | Retryable `ConnectionLost` | **Indeterminate `WriteUnconfirmed`** (§9.2 cause `link_lost`). No `h2_*` cause can arise for a non-idempotent request in v1. |
| sent, no head | total deadline (`timeout`) | NonRetryable `QueryTimeout` | **Indeterminate `WriteUnconfirmed`** (cause `timeout`) |
| sent, no head | `CANCEL` (`cancelled`) | `Outcome::Cancelled` | **Indeterminate `WriteUnconfirmed`** (cause `timeout`, as a cancelled SQL autocommit write; review F25) |
| head received | `body_reset`, `body_eof`, `body_framing`, `h2_stream_error` | Retryable `ConnectionLost` | NonRetryable `ResponseIncomplete` |
| head received | `decode`, `max_response_bytes` | NonRetryable `ResponseIncomplete` | same |
| head received | `read_idle`, `timeout` | NonRetryable `QueryTimeout` | same |
| head received | `CANCEL` | `Outcome::Cancelled` | same |
| *(M6-F4b)* dispatched, not sent | drain cap (`draining`) | Retryable `UpstreamUnavailable` | same |
| *(M6-F4b)* sent, no head | drain cap (`draining`) | Retryable `ConnectionLost` | **Indeterminate `WriteUnconfirmed`** |
| *(M6-F4b)* head received | drain cap (`draining`) | Retryable `ConnectionLost` | NonRetryable `ResponseIncomplete` |
| complete | any final status | `Outcome::Ok(HttpDone)` | same |

How to read the table:

- **`Indeterminate` arises in exactly one region:** non-idempotent, sent, no head. That is §9.2's definition, transcribed.
- **"Dispatched, not sent"** *(added 2026-10-06, §22.2 (ct), review F-3)* is the state between dispatch (step 6, the tracker armed) and the first byte: an I/O failure there, P19's case C (the request already serialised into `hyper`'s buffer), or a deadline or `CANCEL` that arrives first. Nothing of the request reached the socket, in plaintext or in ciphertext, so it is never `Indeterminate`, and a non-idempotent request is as safe to retry as before dispatch. **The engine reads `sent` only after it has torn the connection down.** `hyper`'s connection task is spawned and writes independently of the response future, so the engine first aborts that task and drops the I/O; only then is `sent = false` final, since no later byte can follow the reading. The connection is never reused. The deadline is `PoolTimeout` (`deadline`) as before dispatch, not `QueryTimeout`, because nothing was sent. *[Amended at M6-F4a's review round (§22.2 (cz)): **a head that arrives while no byte of the request has been sent is not an answer to it** — an upstream that speaks first (a 101 or a 200 on accept, read by `hyper` once the request is queued behind a slow write) would otherwise turn an unsent POST into `Indeterminate` (101, an oversize head) or even `Ok`. The engine checks `sent` when a head arrives; if it is false, it tears the connection down, reads `sent` again, and classifies `unsent_closed` (Retryable) — or, if bytes slipped out meanwhile, `malformed_head` (sent, no head), since the head preceded the request.]*
- **After the head, a non-idempotent failure is a *transport* NonRetryable `ResponseIncomplete`.** The transport fate is known: the upstream received the request and answered. It does **not** mean "nothing happened". After a 2xx head the request was **applied**. The PHP exception therefore exposes the head's `status()` and `wasApplied()`, and its fate marker combines the transport fate with `StatusFate`. A non-idempotent request with a 500/502/504 head and a truncated body is Indeterminate *in PHP's combined fate*, agreeing with §23.7.4 (§23.11.3, review F13).
- **Timeouts and cancels of idempotent requests follow §9.2's rule for reads** (NonRetryable `QueryTimeout`/`Cancelled`). The `QueryTimeout` name is historical (C5).
- **`retry_after_ms`** is set for `RateLimited` and for a breaker's `UpstreamUnavailable`.

**The engine never re-sends.** It never moves a dispatched request to another connection. libcurl re-sends a request once when a *reused* connection dies before any response byte, POST included (P4, to be measured). Ferro reports `Indeterminate` instead, and narrows the race (§23.8.2) (C9).

#### 23.7.2 Effective idempotency: declarations only (D19, D21; review F9)

A request is **idempotent** if and only if one of the following holds:

- **(a) The caller declared it.** The request carries `idempotent = true`. This is the HTTP analogue of the SQL tier's *declared* `readonly`, which `fate.rs` already honours to suppress `Indeterminate` (§22.2 (ac)). A declaration, never an inference.
- **(b) The operator declared it.** `idempotent` is `nil` and the method is in the upstream's `IDEMPOTENT_METHODS` (default **empty**). The operator's per-upstream configuration is the reviewed, deployment-owned artifact here, the role the manifest plays for SQL (§11). M3's manifest may later carry HTTP entries; until then, this key is the operator-side declaration.
- **(c) The operator declared the key and the request carries it.** `idempotent` is `nil`, the upstream declares `IDEMPOTENCY_KEY_HEADER`, and the request carries that header non-empty. The operator declares that the server deduplicates on it; the caller supplies the key.

**`idempotent = false` downgrades** regardless of (b) and (c).

The engine computes the result once, uses it for fate and sub-pool selection, and reports it in `HttpHead.idempotent`.

**The method alone licenses nothing.** RFC 9110 calls `GET` safe, and a `GET` with side effects is as real as a `SELECT` calling a volatile function. The SQL tiers were forbidden that inference (§22.2 (ac)), and HTTP takes the same rule.

**Cost, stated (C16; accepted, §23.18 Q8):** with the defaults, a `GET` whose connection dies after sending is reported Indeterminate. What it costs:

- **Guzzle's exception class does not change** (§23.11.3), so stock and naive deciders behave as they do under curl.
- **Ferro's own decider will not retry it** (§23.11.4).
- **`ferro_http_indeterminate_total` counts it.**
- **HTTP/2 is not used for it** (§23.8.3).

The remedy is one operator line, `IDEMPOTENT_METHODS=GET,HEAD,OPTIONS`, for an upstream whose `GET`s the operator knows to be side-effect-free, or a per-request declaration. It is a declaration either way; the engine never supplies it.

#### 23.7.3 Client-side fate when `ferrod` dies (§19.3, extended)

The client does not know the upstream's configuration. Before `HEAD`, it counts a request as idempotent **only** when the request itself declared `idempotent = true`. After `HEAD`, it uses `HttpHead.idempotent`.

| In flight when the UDS link died | Client-idempotent | Otherwise |
|---|---|---|
| `REQUEST` frame not completely written | Retryable `ConnectionLost` | same (§19.3, the poisoned-session rule) |
| written, no `HEAD` | Retryable `ConnectionLost` (epoch changed) | **Indeterminate `WriteUnconfirmed`** (`engine_restart`) |
| `HEAD` received | Retryable `ConnectionLost` | NonRetryable `ResponseIncomplete` (client-synthesised; combined fate per §23.7.1) |

**The client never re-issues an HTTP request** (§19.3's `retry_reads` covers SQL reads only).

**Client per-request deadline expiry** is not a link death and does not poison the session (§23.11.0). It is classified by the same table, with the §9.2 cause `timeout`, after a `CANCEL` is sent.

#### 23.7.4 Status codes are not transport errors (decided)

**Any final status is `Outcome::Ok`.**

- PSR-18 forbids treating a well-formed response as an error.
- Guzzle maps status to exception in `http_errors` middleware, above the handler.
- A status's meaning is application semantics (rule E).

PHP gets an advisory helper, `Ferro\Http\StatusFate::of(int $status, bool $idempotent, ?string $retryAfter)`, fed by `HttpHead.idempotent`:

| Status | Idempotent | Non-idempotent |
|---|---|---|
| 1xx–3xx | not a failure | not a failure |
| 408, 425 | Retryable | Retryable |
| 429; 503 with `Retry-After` | Retryable, delay = `Retry-After` | same |
| other 4xx; 501, 505 | NonRetryable | NonRetryable |
| 500, 502, 504, 503 without `Retry-After`, other 5xx | Retryable | **Indeterminate**: an intermediary may have forwarded it, and a 500 promises nothing about partial application |

This refines product-vision §4.2 D and the ledger's F0 row, both amended in the same change (C2; acknowledged, §23.18 Q5).

---

### 23.8 Pools, versions, TLS, DNS and limits

#### 23.8.1 The pool is hand-rolled (D9, extended)

There is one pool per upstream, or per (upstream, peer uid) under `PARTITION=uid`. Each pool has an HTTP/1.1 sub-pool and, under `HTTP=auto`, an HTTP/2 sub-pool. It is built on `hyper::client::conn::{http1,http2}`.

- **`hyper-util`'s `legacy::Client` is not used.** Its pool sits where `sent` must be observed, and it re-dispatches internally (`retry_canceled_requests`).
- **`hyper-util` contributes only the tokio I/O adapter.**
- **Nothing from `ferro-pool` is reused,** because HTTP has no pins or hygiene.

`PARTITION=uid` partitions *connections* only. The breaker, rate bucket, Retry-After hold and body budget stay per upstream, because the provider's limits are per key, not per tenant. The cost is up to N× connections and handshakes.

#### 23.8.2 Connection reuse

Idle connections are chosen LIFO. A candidate must be:

- not closed;
- younger than `MAX_LIFETIME_MS`;
- idle for less than `IDLE_TIMEOUT_MS`, clamped to the server's `Keep-Alive: timeout` minus 1 s.

**A non-idempotent request additionally refuses an HTTP/1.1 connection idle longer than `H1_UNSAFE_REUSE_MAX_IDLE_MS` (2 s) and dials fresh.** The keep-alive race (the server closes just as a POST is written) is the stock cause of a stale-reuse `Indeterminate`. curl hides it with a re-send; Ferro cannot re-send, so it narrows the race. The cost is a handshake per non-idempotent request on a quiet upstream, softened by TLS resumption. Chaos case 4 measures the residual.

After a cancel, a timeout, or an abandoned body, an HTTP/1.1 connection is **discarded, never drained.** An HTTP/2 stream is reset with `CANCEL`.

*[Amended M6-F4b (SPEC §22.2 (db)): **dropping a connection now closes it.** A tokio `JoinHandle` does not stop its task when dropped, so a connection object dropped without the discard path — an exchange's future dropped mid-flight (a panic unwinding through it, an aborted task, a runtime shutting down), a stale or surplus pooled connection — used to leave `hyper`'s connection task, and the socket with it, running. Every drop now aborts it; the discard path still also awaits it, which reading `sent` needs. **Chaos 4, measured** (loopback, 2026-10-06; `http_f4b_it`'s `chaos4_…`): against a server whose idle close CROSSES the next request (it reads a request that arrives on a connection idle ≥ 100 ms and closes without answering — the race, made deterministic), 20 POSTs 150 ms apart are 10 `Ok` / **10 Indeterminate** at the default 2 s bound (every POST that reuses the idle connection meets the close) and 20 `Ok` / 0 Indeterminate at `H1_UNSAFE_REUSE_MAX_IDLE_MS=0` (a dial per POST); against a server with a real 100 ms idle timer (a FIN), 40 POSTs at 80–120 ms gaps were 39 `Ok` / 0 Indeterminate / 1 Retryable `unsent_closed` at the default and 40 `Ok` at 0. Every POST was received at most once in every run. So the residual is what this section says it is: a server whose keep-alive timeout is shorter than `H1_UNSAFE_REUSE_MAX_IDLE_MS` makes every such reuse a candidate, and 0 removes it at the cost of a handshake per non-idempotent request.]*

*[Amended M6-F4a (SPEC §22.2 (cz)): under `PARTITION=uid` a peer the transport did not attest is REFUSED (`forbidden_upstream`), as an `ALLOW_UIDS` upstream refuses it — pooling every unattested peer together is the cross-tenant sharing the key exists to prevent. A connection checked out and handed back UNUSED (a `CANCEL` or deadline caught just before dispatch) keeps its idle time. Also: a connection whose exchange COMPLETED is returned to the pool before the request's terminal is declared, and only once `hyper` reports it ready for its next request (bounded at 250 ms; otherwise it is discarded), so a request the client sends after the `END` finds it. A response carrying `Connection: close` is discarded. Until slice F6, `MAX_CONNECTIONS` bounds only how many IDLE connections a sub-pool retains; `MAX_REQUESTS`, `MAX_DIALS` and the queue are F6's.]*

#### 23.8.3 HTTP versions (review F1, F16 a)

- **`HTTP=1.1` (the default).** Every request uses HTTP/1.1, one exchange per connection, with no pipelining. This matches stock Guzzle over curl: Guzzle's default request version is `1.1`, and `CurlFactory` maps it to `CURL_HTTP_VERSION_1_1`. So switching to Ferro does not silently move traffic to HTTP/2.
- **`HTTP=auto` (opt-in, slice F5b).** On `https`, *effectively-idempotent* requests use an HTTP/2 sub-pool (ALPN `h2`) when the server negotiates it. Non-idempotent requests **always** use the HTTP/1.1 sub-pool, whose dials offer ALPN `http/1.1` only. Each HTTP/2 connection carries up to `min(SETTINGS_MAX_CONCURRENT_STREAMS, 100)` streams, and sends a keep-alive `PING` after 30 s idle.
- **Why non-idempotent requests stay on HTTP/1.1 in v1.** The HTTP/2 "not processed" licence is real (RFC 9113 §8.7) but narrow in `h2`. Only a **remote** `RST_STREAM(REFUSED_STREAM)` and a **remote** GOAWAY delivered to a stream *above* its `last_stream_id` (h2 `recv_go_away`, `Error::remote_go_away`) prove non-processing. A *library*-initiated GOAWAY resets every active stream, processed or not (h2 `handle_go_away` → `streams.handle_error`). Streams at or below `last_stream_id` die with `BrokenPipe`. And `hyper` may wrap any of these as its own `Canceled`. Building v1's write fate on that distinction, through two crates' error wrapping, is the risk this design declines.
- **What it costs:** no HTTP/2 multiplexing for writes, and with the default `IDEMPOTENT_METHODS` empty, no HTTP/2 at all until the operator declares methods. This is the admission-A debit stated in §23.1.
- **The post-v1 gate.** A slice may let non-idempotent requests ride HTTP/2 only after spike F1b proves both licence predicates (`is_remote()` included) **and** the negative control: a library-initiated GOAWAY on a processed stream must *not* be licensed (P2).
- **What HTTP/2 does for idempotent requests in v1.** A shared connection's GOAWAY or reset turns them Retryable, never Indeterminate. Cross-tenant coupling on HTTP/2 is therefore a retry, not a fate loss (§23.1).
- **The PSR-7 `version`** does not select the version, because the upstream's `HTTP` does. A request at `1.0` or `2.0` is sent as the upstream's version, and the real version is reported in `HttpHead.version` (C9).
- **Not in v1:** `h2c` (prior knowledge or upgrade).
- **A server that supports *only* `h2` over TLS** fails the HTTP/1.1 sub-pool's ALPN as `TlsRefused` (`tls_alpn`), and is documented.

#### 23.8.4 Timeouts

The effective value of each bound is `min(request field ?? upstream default, upstream ceiling)`.

- **`timeout_ms`** bounds the whole exchange, body included, and runs from decode (§23.6). With no value, the request gets the 600 s ceiling, where curl would wait forever (C9).
- **`connect_timeout_ms`** bounds DNS + TCP + TLS.
- **`read_timeout_ms`** bounds idle time between body bytes, which is Guzzle's `read_timeout`.

The client's own per-request deadline is `timeout_ms` plus a margin (§23.11.0). It is a backstop, and never the primary bound.

#### 23.8.5 DNS and the address guard (review F6)

**Resolution** uses `getaddrinfo` (`tokio::net::lookup_host`), so `/etc/hosts` and nsswitch behave as they do for curl. Positive answers are cached for `DNS_TTL_MS`; negative answers are not cached. Addresses are tried in resolver order. Happy Eyeballs is deferred, at a cost of one `connect_timeout` share per dead address family.

**Classification.** Every resolved address, and an IP-literal `ORIGIN`, is classified after unwrapping these embeddings to the IPv4 address they carry:

- IPv4-mapped `::ffff:0:0/96`;
- SIIT IPv4-translated `::ffff:0:0:0/96` (RFC 2765, deprecated) — *[Amended M6-F3: added; it carries an IPv4 address the same way and was otherwise `public`, so a translator that still honours it would reach a metadata address under the default class]*;
- NAT64 `64:ff9b::/96` and `64:ff9b:1::/48`;
- every `FERRO_HTTP_NAT64_PREFIXES` entry.

**Always refused** (`forbidden_address`). No key widens this, except `ALLOW_METADATA=1` for the rows marked *M*:

| Range | Why |
|---|---|
| `0.0.0.0/8`, `::` | unspecified |
| `224.0.0.0/4`, `ff00::/8`, `255.255.255.255`, `240.0.0.0/4` | multicast, broadcast, reserved |
| `169.254.0.0/16`, `fe80::/10` *M* | link-local: AWS/GCP/Azure/OCI/DigitalOcean IMDS `169.254.169.254`, ECS `169.254.170.2`, Tencent `169.254.0.23` |
| `fd00:ec2::/32` *M* | AWS IPv6 IMDS `fd00:ec2::254`, EKS Pod Identity `fd00:ec2::23` (inside ULA, which would otherwise be `private`) |
| `fd20:ce::254/128` *M* | GCP's IPv6 metadata server (inside ULA). *[Amended M6-F3: added; documented by GCP, not verified here.]* |
| `100.100.100.200/32` *M* | Alibaba Cloud metadata (inside CGNAT) |
| `168.63.129.16/32` *M* | Azure WireServer (a *public* address) |
| `192.0.0.0/24` *M* | IETF special-purpose block, which includes metadata uses such as `192.0.0.192` |
| `::/96` (other than `::` and `::1`), `2002::/16`, `2001::/32`, `fec0::/10` | deprecated IPv4-compatible, 6to4 and Teredo forms (each can carry a metadata IPv4) and site-local |

**Classes**, admitted per upstream by `ADDRESS_CLASSES` (default `public`):

- **`loopback`:** `127.0.0.0/8`, `::1`.
- **`private`:** `10/8`, `172.16/12`, `192.168/16`, `100.64/10`, `fc00::/7`, and `198.18/15`, minus the always-refused rows.
- **`public`:** everything else.

*[Amended M6-F3 (SPEC §22.2 (cw)): the IPv6 rows of the always-refused table are checked **before** any embedding is unwrapped, so a `FERRO_HTTP_NAT64_PREFIXES` entry declared inside one (`fe80::/96`) cannot widen it. `64:ff9b:1::/48` may carry its IPv4 address in any RFC 6052 layout of length 48 or more, which the engine cannot see, so every candidate (/48, /56, /64, /96) is classified and **all** must be admitted. The cost: an undeclared local-use deployment is refused wherever another layout reads as refused — which is every `64:ff9b:1::/96` address, whose /48 reading is `0.0.0.0` — and the remedy is to declare the exact prefix in `FERRO_HTTP_NAT64_PREFIXES`, which then decides alone. `ALLOW_METADATA=1` admits the *M* rows whatever `ADDRESS_CLASSES` says (they sit across classes: Azure's WireServer is public, Alibaba's metadata address is inside CGNAT) and never admits another row. The classification lives in `ferro-http` (`address.rs`) from F3; F4 owns resolution and pinning.]*

*[Amended M6-F4a (SPEC §22.2 (cz)), as built: resolution that fails, answers nothing, or does not answer within the request's connect bound is `dns`. `CONNECT_TIMEOUT_MS` bounds DNS and every connect attempt together; each admitted address gets an equal share of what is left (one share per dead address, as Happy Eyeballs' absence costs). A failed dial drops the cached answer, so the next dial re-resolves; an answer the guard refuses outright stays cached for its TTL like any positive answer. The connect goes to the checked `SocketAddr` through one connector seam (TCP in production; a fault-injecting stream in tests, the only way to reach "dispatched, not sent" deterministically — §23.7.1).]*

An IP-literal `ORIGIN` implicitly admits its own literal, unless that literal is in the always-refused table. An internal upstream on a DNS name (`http://es.internal:9200` → `10.x`) needs `ADDRESS_CLASSES=private`. That is the cost, and it is what stops a **DNS rebinding** of any upstream's name to `127.0.0.1` or to an internal service. On `https`, certificate verification also defeats rebinding; on `http`, this guard is the only defence.

**Pinning.** Each address is checked, and the TCP connect goes to **exactly** that checked `SocketAddr`. Nothing re-resolves between check and connect. A connection keeps its address for life. New resolution happens only on a new dial (at `MAX_LIFETIME_MS` or after a failure), and is checked again. Refused addresses in a mixed answer are skipped and counted. If none remain, the request is `forbidden_address`.

**Residual risks, stated:**

- A network-specific NAT64 prefix is undetectable unless declared in `FERRO_HTTP_NAT64_PREFIXES`.
- A metadata service at an address not on this list is reachable if its class is admitted.

#### 23.8.6 Concurrency, queueing, budgets and the breaker

**Admission order:** drain, then breaker and hold, then rate, then body budget, then queue. Everything in it happens before sending.

**Body budget (review F18).** A request's body is charged at admission and released when it has been fully written upstream, or at its terminal. It is charged to the per-upstream `MAX_BODY_BYTES` (64 MiB) and to the daemon-wide `FERRO_HTTP_MAX_BODY_BYTES` (256 MiB). Over either budget, the request is Retryable `PoolTimeout` (`body_budget`), not sent. So worst-case retained body memory is the daemon budget, not `(MAX_REQUESTS + MAX_QUEUED) × 16 MiB` per upstream. The transient decode buffer is one frame per session reader, as for SQL.

*[Amended M6-F4b (SPEC §22.2 (db)), **as built**: "fully written" is MEASURED as the moment `hyper` drops the body. The body is handed over as one `Bytes` whose owner holds the charge (no copy of PHP's body); the exchange keeps a second holder, and whichever drops first releases the charge exactly once. `hyper` 1.x's h1 write path under its `Queue` strategy keeps that buffer, uncopied, until its last byte has been accepted by the socket, so the drop IS "fully written"; the engine **pins** that strategy (`http1::Builder::writev(true)`), because under `Flatten` — `hyper`'s choice for a transport that is not write-vectored — the body is copied into `hyper`'s own buffer and ours dropped at once, which would release the charge while the copy was still held. The decoded frame's copy of the body is released before the exchange starts, so the charge counts the one copy that is held. Accounting is atomic across both accounts (a refused charge takes nothing). The gauges are F7's; the engine exposes the counts.]*

**The breaker** is per upstream: closed → open → half-open.

- **Opening.** It opens after `BREAKER_FAILURES` consecutive *counted* failures. The default counted class is `connect`: `dns`, `connect_*` and `tls_handshake`, all not sent. `connect+timeout` and `connect+timeout+5xx` (502/503/504) are opt-in.
- **Open:** every request is refused with `UpstreamUnavailable` (`breaker_open`) and `retry_after_ms` set to the time left.
- **Half-open** admits exactly one user request as the probe, holding a **probe slot that is an RAII guard** (review F24). Any other request is refused (`breaker_probe_busy`). The probe's outcome decides the next state:

  | Probe outcome | Next state |
  |---|---|
  | A final head (any status, unless 5xx is counted) | closed |
  | A counted failure | open again for `BREAKER_OPEN_MS` |
  | **Any other terminal:** Indeterminate, non-counted timeout, `Cancelled`, a refusal, or the session dying or the request being cancelled before dispatch | **half-open**, with the slot released, so the next request probes |

  The slot can never leak.
- **Why only connect failures count by default:** that is where the stampede happens (§19.4), and they are never Indeterminate.

#### 23.8.7 Host-level rate limits

- **The bucket.** One token bucket per upstream (`RATE_PER_SEC`, `RATE_BURST`), shared host-wide. This is the AI-gateway feature. It approximates a per-key provider limit, and the documentation says so.
- **No token.** `RATE_MAX_WAIT_MS=0` (the default) fails fast with `RateLimited` and the time to the next token. Otherwise the request waits up to `min(RATE_MAX_WAIT_MS, remaining deadline)`.
- **Retry-After hold** (`HONOR_RETRY_AFTER=1`, opt-in). A completed 429, or a 503 with `Retry-After` ≤ `RETRY_AFTER_MAX_MS`, holds the upstream. Later admissions fail fast (`retry_after_hold`). The hold is off by default, because one tenant's 429 would throttle every app on the upstream.

#### 23.8.8 TLS

- **The stack.** `rustls` + `ring`, TLS 1.2 and 1.3.
- **Roots.** The OS root store is loaded **at start** (`rustls-native-certs`). `rustls-native-certs` honours `SSL_CERT_FILE` and `SSL_CERT_DIR` from `ferrod`'s environment, so the "OS store" can be redirected by whoever controls that environment (P3/F21). `CA_FILE` replaces it per upstream. There is no reload, so a root-store update needs a restart.
- **SNI** is the origin host. An IP literal sends no SNI and needs an IP SAN.
- **Resumption.** One client session cache per upstream. `ferro_http_tls_handshakes_total{resumed}` makes it measurable (P5).
- **No option to disable verification.** Development uses `CA_FILE`.
- **mTLS** uses `CLIENT_CERT_FILE`/`CLIENT_KEY_FILE`.
- **This is the first TLS stack in the tree.** The database backends run `NoTls`.

---

### 23.9 Bodies and streaming

#### 23.9.1 Responses always stream; head and chunk limits (review F4)

There is one shape: `HEAD`, `BODY`*, `END`. A buffered shape would need a size failure that, for an applied POST, could only be NonRetryable, and it would break `$client->get($bigFile)`. The cost of three frames for a small response is noise against a WAN call. Coalescing into the terminal is a bench-justified slice, never speculative (charter rule 5).

**Response-head limits**, set explicitly in `hyper` (P15):

- **HTTP/1.1:** `max_buf_size` = 256 KiB and `max_headers` = 256.
- **HTTP/2:** `max_header_list_size` = 256 KiB.

A head over a limit is `oversize_head`, which is the "malformed or oversize head" row (sent, no head). *[Amended M6-F4a (SPEC §22.2 (cz)): the engine's measure reconstructs the status line as `HTTP/1.1 NNN <reason as received>` + CRLF (`hyper` exposes a non-canonical reason, and implies the canonical one); every field is measured as the `http` crate hands it over, so optional whitespace the upstream wrote around a value is not counted — the measure can only under-count by that whitespace, never refuse a head that is within the limit *[Review round (§22.2 (cz)): the SP after the status code is counted only when the reason is non-empty — `HTTP/1.1 200\r\n` is legal, and counting a space never sent refused a head at exactly the limit. `HTTP/1.1 200 \r\n` is therefore under-counted by one byte, the permitted direction.]*]* A maximal permitted head encodes to well under 1 MiB, far below the global window's floor (`credit_bytes` ≥ `MAX_FRAME_PAYLOAD` = 16 MiB, enforced by `Config::validate`). A `HEAD` frame therefore always fits, which is §5.2's rule, held by construction.

**Test (slice F4):** with `credit_bytes` at its validated floor, a response carrying the maximal permitted head is delivered, and a head one byte over is refused as `oversize_head`. Curl's own response-head limits are measured for the incompatibility entry (P20).

*[Amended 2026-10-06 (M6-F1a, P15 measured, §22.2 (ct)): **all three setters exist, and an oversize head is always an error, never a truncation. But `max_buf_size` is not an exact head limit,** so the one-byte-over test cannot rest on `hyper`.*

- *`max_headers` = 256 is exact at every read chunking.*
- *`max_buf_size` = 256 KiB is exact only when the head arrives in small reads. `hyper` checks the buffer's length after a failed parse, and one read can fill the buffer's whole spare capacity. Delivered in a single read, heads up to **507 904 B** passed.*
- *The ceiling is hard. `hyper` (`proto/h1/io.rs`) reads only while the buffer holds fewer than `max_buf_size` bytes, asks for at most `max_buf_size − len` more, and reserves only when the spare capacity is smaller than that. A reservation therefore starts from a capacity below `max_buf_size` and grows it to `max(2 × capacity, required)` (`Vec::reserve`'s amortised rule, which `BytesMut` follows), with `required ≤ max_buf_size`. So the capacity stays below 2 × `max_buf_size`, and so does what one read can fill: a head `hyper` delivers is always **< 2 × `max_buf_size`** (< 512 KiB). A 512 KiB head was refused under every delivery shape tried, and the review found the same 507 904 B maximum after a preceding 1xx. §5.2's "a `HEAD` frame always fits" therefore still holds by a factor of more than 30.*

*The exact 256 KiB limit is the **engine's** check, applied to the parsed head before `HEAD` is sent: the status line, plus each field's name, value and 4 bytes (`": "` and CRLF), plus the final CRLF. A head over it is `oversize_head` (sent, no head), and `max_buf_size` is the memory backstop. F4's test is written against that measure.*

*On HTTP/2:*
- *`hyper`'s own default `max_header_list_size` is **16 KiB** (not `h2`'s 16 MiB), so setting it is load-bearing.*
- *An over-limit head is a library `RST_STREAM(PROTOCOL_ERROR)`, indistinguishable from a malformed block, so its cause is `h2_stream_error` (§23.5.6).*
- *Far over the limit, `h2`'s CONTINUATION cap answers with a **connection-level** `GOAWAY(ENHANCE_YOUR_CALM)`, failing every stream on that connection. That is §23.1's cross-tenant coupling, which in v1 reaches only effectively-idempotent requests (Retryable).]*

**`BODY` chunks are ≤ 256 KiB,** so the default 64-frame window holds at most 16 MiB of unread body per stream. That is also **the client-side memory bound per open HTTP stream** whose frames the application has not consumed. It is stated as a cost of lifting (cj)'s stream exclusivity (§23.11.1): N open, unread streams can hold N × 16 MiB in PHP.

#### 23.9.2 Content decoding

When `decode = true`:

- **Asking.** If PHP set no `accept-encoding`, the engine sends `gzip, deflate`.
- **Decoding.** `gzip`, `x-gzip` and `deflate` (zlib, with a raw-deflate fallback) are decoded incrementally under credit. `content-encoding` and `content-length` are removed from the head and reported in `decoded`.
- **Other encodings** (`br`, `zstd`, stacked encodings) pass through with `decoded = nil`.

*[Amended M6-F4a (SPEC §22.2 (cz)): **decoding is slice F4b's.** Until it lands, `decode = true` has no effect: the engine adds no `Accept-Encoding` and removes nothing, so `HttpHead.decoded` is always `nil` and a body PHP asked to be encoded arrives encoded, with its `content-encoding` header intact. That is §23.9.2's own "pass through" rule applied to every encoding, so a client cannot mistake an undecoded body for a decoded one.]* *[Amended M6-F4b (SPEC §22.2 (db)): **built**; the note above is superseded. Decided where this section was silent, each stricter or equal, never looser:*
- *A body is decoded only when the response carries **exactly one** `Content-Encoding` field whose whole value is, ASCII case-insensitively, `gzip`, `x-gzip` or `deflate`. A list (`gzip, br`), two fields, `identity`, `br`, `zstd` or anything else passes through with `decoded = nil`.*
- *`decoded` carries the `Content-Encoding` value **as received** and the `Content-Length` as received (`nil` when there was none — chunked — or it is not a number the wire can carry).*
- *`deflate` is zlib when its first two bytes are a valid RFC 1950 header without a preset dictionary, and raw RFC 1951 otherwise. A gzip body may be several members (RFC 1952 §2.2). The gzip header is bounded at 64 KiB (its optional name/comment fields are otherwise unbounded on the wire); CRC32 and length are checked.*
- *`decode` (`ResponseIncomplete`, NonRetryable, after the `HEAD`) for: a bad header, an inflate error, a checksum or length mismatch, a stream truncated when the body ends, and bytes after the end of the stream that do not start another gzip member. An **empty** body is never an error (a `HEAD` response, a 204 or a 304 routinely carries `Content-Encoding: gzip` and nothing else).*
- *The bounds this section names, held explicitly: each decode step emits at most one `BODY` chunk (256 KiB) into a fixed buffer, and is sent — through the credit gauntlet — before the next is decoded, so a reader that stops replenishing stops decompression (memory bounded by the window: chaos 6's RSS half measured +0.6 MiB for an endless bomb against a stalled reader). The stop token and the total deadline are checked between steps, so a bomb that never lacks credit ends at the deadline (measured: 1.0 s, ~180 MB decoded, terminal on time). **A step is bounded by its INPUT and its TIME as well as its output** (review rounds, §22.2 (db)): it consumes at most 64 KiB, starts at most 64 gzip members, and runs at most ~1 ms (checked between inflate calls, each handed at most 256 B, and between members, once the step has made progress), then returns — with a short chunk, or with nothing, in which case the engine checks the stop and the deadline and yields exactly as after a frame. A byte bound is not a CPU bound: bounding by output alone let a stream of EMPTY gzip members (20 bytes each, inflating to nothing) be consumed a whole network read per step (93–97 ms executor stalls, a terminal 62–79 ms past its deadline); and with the 64 KiB input bound, EMPTY fixed-Huffman blocks (about 10 bits each, ~2.5 µs of inflate per input byte) still made a step of ~160 ms (release; 294–303 ms stalls through the engine in debug) — no frame, so no credit could park it, a core burned until the deadline. Measured after, release, longest step: empty fixed-Huffman blocks 1.5–2.5 ms, 1-byte full-flush blocks 1.2 ms (was 25–28), empty gzip members 0.5 ms, the zeros bomb 0.1 ms. The inflater is reset per member, never reallocated, and a gzip member header is parsed incrementally — each byte examined once and consumed, however the header is split across reads (re-joining and re-scanning a partial header on every read was quadratic: four ~60 KiB `FNAME`s in 1-byte chunks cost 3.24 s). Cost: an incompressible encoded body arrives in `BODY` chunks of at most about 64 KiB (the read size when smaller), and a step that reaches its time bound sends a shorter chunk. That is why the engine uses `flate2`'s low-level `Decompress` (already in the lock, D20's "`flate2` is already locked"); `flate2::write::GzDecoder` would emit all of one input chunk's output at once — a 256 KiB gzip chunk of zeros is about 256 MiB.*
- *`MAX_RESPONSE_BYTES` and `HttpStats.bytes_received` keep counting **wire** bytes (encoded).]*
- **Safety.** A decompression bomb costs CPU bounded by the deadline and memory bounded by the window. A corrupt stream is `ResponseIncomplete` (`decode`).
- **Brotli** is not decoded, and that is an incompatibility (C9).

#### 23.9.3 Request bodies above the frame cap: refused in v1

The body rides inside the `REQUEST` frame. That is what makes `sent` well defined and lets the engine drop `Expect`.

- **A body that would push the frame past 16 MiB is refused by the client before sending** (§22.2 (ak)), loudly: the Guzzle handler rejects with a `RequestException` subclass and PSR-18 throws a `RequestExceptionInterface` (§23.11.3), naming the limit. Nothing reaches the engine, so the fate is "not sent".
- **Cost:** a multipart upload above about 16 MiB fails where curl streams it (C9). The routes around it are presigned URLs, or a stock handler for that one upstream.
- **Decided: v1 has no client → engine large-request-body path** (§23.18 Q3). This is a recorded v1 limit. A post-v1 path is `/proto` design work beside M3-D3 (memfd) and the deferred chunked `LARGE_OBJECT` (B6b), and must keep `sent` measurable at the upstream connection's first plaintext byte.

---

### 23.10 Observability and redaction

The redaction contract is §13's and product-vision §5's. **Nothing PHP sends as data is exported:** no target, no query, no header value, no body. Attached credentials are never exported.

What may appear:

- configured names (upstream, origin host);
- closed vocabularies (method, status class, code name, cause token);
- measurements;
- a route template the caller declared;
- trace ids.

#### 23.10.1 Spans (OTLP)

There is one CLIENT span per `REQUEST`, a child of field 13's `traceparent`, under §13's sampling rule.

- **Name:** the method (`HTTP` with `http.request.method=_OTHER` outside the RFC set).
- **Attributes:**
  - `http.request.method`;
  - `server.address`, `server.port` and `url.scheme`, all from `ORIGIN`;
  - `ferro.upstream`;
  - `http.response.status_code`;
  - `network.protocol.version`;
  - `url.template`, only when `route` is declared and `LOG_ROUTE=1`;
  - the `ferro.*_us` timings;
  - the body sizes;
  - `ferro.reused`, `ferro.idempotent`;
  - on failure, `error.type` = the code name and `ferro.cause` = the cause token.
- **Never exported:** `url.full`, `url.path`, `url.query`, or any header.
- **No path fingerprinting.** It is an inference heuristic that leaks tokens.
- **The engine does not inject `traceparent` upstream.** PHP instrumentation does that when wanted.

#### 23.10.2 Prometheus

Labels come from closed vocabularies:

- **`upstream`** is operator-declared and escaped like `pool`. **Every refusal with reason `forbidden_upstream` counts under `upstream="_unknown"`, whether the name is unknown or merely forbidden to the peer** (review F22). Otherwise a counter delta would be an existence oracle.
- **`method`** is the RFC set or `_OTHER`.
- **`status_class`** is `1xx` through `5xx`.
- **`code`/`branch`** are generated from the registry.
- **`cause`** comes from `[http.causes]`.

**Stated cost (C21):** the metrics endpoint is unauthenticated on loopback (§22.2 (bs)). Enabling `FERRO_METRICS_ADDR` therefore **publishes every configured upstream name to every local uid**, which D15's indistinguishability rule otherwise hides. Operators on multi-tenant hosts who treat upstream names as confidential leave metrics off or choose neutral names.

| Metric | Type | Labels / notes |
|---|---|---|
| `ferro_http_requests_total` | counter | `upstream`, `method`, `status_class` (completed exchanges) |
| `ferro_http_errors_total` | counter | `upstream`, `code`, `branch`, `cause` (terminals produced, §22.2 (bu)) |
| `ferro_http_indeterminate_total` | counter | `upstream` |
| `ferro_http_refused_total` | counter | `upstream`, `cause` (the `forbidden_*` and admission causes) |
| `ferro_http_queue_duration_seconds`, `ferro_http_ttfb_seconds`, `ferro_http_duration_seconds` | histogram | `upstream` |
| `ferro_http_connections` | gauge | `upstream`, `version`, `state="idle"\|"active"` |
| `ferro_http_waiting` | gauge | `upstream` |
| `ferro_http_body_budget_bytes` | gauge | `upstream` (plus `_daemon`) |
| `ferro_http_dials_total` | counter | `upstream`, `result` (`ok`/`dns`/`connect`/`tls`/`address`) |
| `ferro_http_address_skipped_total` | counter | `upstream`, `range` (closed vocabulary of §23.8.5's rows and classes) |
| `ferro_http_tls_handshakes_total` | counter | `upstream`, `resumed` |
| `ferro_http_breaker_state`, `ferro_http_breaker_transitions_total` | gauge / counter | `upstream` (`to`) |
| `ferro_http_rate_limited_total` | counter | `upstream` |
| `ferro_http_attached_header_conflicts_total` | counter | `upstream`, `policy="refuse"\|"override"` |
| `ferro_http_draining` | gauge | 0 or 1 |
| `ferro_traceparent_invalid_total` | counter | existing; HTTP feeds it |

`ferro_errors_total{code,branch}` keeps counting every service's error terminals at the one terminal builder, as it does today for SQL, TX, STREAM and ADMIN (§22.2 (bu)); `ferro_http_errors_total` adds the `upstream` and `cause` labels for HTTP (C12).

#### 23.10.3 Slow log and error text

- **Threshold.** `FERRO_HTTP_SLOW_LOG_MS`, off by default.
- **A record carries:** upstream, method, status, `route` (under `LOG_ROUTE`), the timings, byte counts, `reused`, `idempotent`, code and cause, `trace_id` and `parent_span_id`.
- **A record never carries:** the target, the query, any header, or a body. `FERRO_LOG_PARAMS` does not apply. There is no "log the path" option, because `route` exists for that.
- **Error payloads** never quote a PHP-supplied value or an attached header. PHP's own exception messages may contain the URI, as curl's do.

#### 23.10.4 The canary gate (slice F7)

One end-to-end test plants distinct canaries in seven places:

1. an attached credential;
2. a PHP header value;
3. the path;
4. the query;
5. the body;
6. a response header;
7. the response body.

It asserts:

- **Canaries 1–5** appear in none of: the slow log, any span a real `otelcol` exported (§22.2 (ce)), the Prometheus exposition, or any `ErrorPayload` (including `detail`) for refused or failed variants.
- **Canary 1** reaches PHP *only* through a reflecting upstream (§23.3.4). That case is asserted too, as the documented hole.

Every assertion is mutation-proven (§22.2 (br)).

---

### 23.11 The PHP side

#### 23.11.0 Prerequisite: per-request client deadlines (ledger D1c, M3; review F2)

**The defect.** Today `Ferro::connect(..., float $ioTimeout = 5.0)` arms `stream_set_timeout` on the socket. A read that waits longer throws `TransportException`, and the session **poisons itself**, failing every pending request as sent (§22.2 (bx), (cj)). So any request slower than 5 s, whether a query or an API call with 5 s TTFB, kills the session. A concurrent `INSERT` on that session then becomes `Indeterminate` because an unrelated call was slow.

**HTTP's slice F8 does not start until this is fixed,** as ledger D1c already scopes it. Product-vision capability 2 ("a DB query and two API calls over one socket") is false until then.

**The change, which applies to SQL and HTTP alike:**

1. **A per-request client deadline.** A request whose bound the client knows records `deadline = now + bound + margin`, with a default margin of 2 s. The bound is (review of the adoption, R9):
   - a SQL request or a queue verb other than a parked `RESERVE`: its `timeout_ms`;
   - an HTTP request: its `timeout_ms` when it carries one;
   - a queue `RESERVE` that waits: `wait_ms + queue_wait_grace_ms`, the engine's bound on its terminal (§24.8), plus its own `timeout_ms` if any.

   **A request that carries no bound gets no client deadline** and is bounded on the client side by liveness only (item 2). That is the rule for an HTTP request with `timeout_ms = nil`: the engine bounds it by the upstream's `TIMEOUT_MS`, which the client cannot see (§23.7.3), and a client-chosen default shorter than that would `CANCEL` a sent POST early and make it `Indeterminate` by the client's own hand.
   - The engine enforces `timeout_ms` itself, so in normal operation its terminal arrives first.
   - **When the client deadline expires,** the client sends `CANCEL` for that `request_id` and fails **only that request**, using §23.7.3's table (HTTP) or §19.3's (SQL) with the §9.2 cause `timeout`.
   - **The session stays alive.** A late `END` for that id is drained and discarded.

   *[Amended M3-D1c (SPEC §22.2 (cp)), for SQL: on expiry the client sends the `CANCEL` but does not fail the request itself. It waits one liveness interval for the engine's terminal, which carries the exact fate (a statement that finished as the `CANCEL` arrived is the success it was), and only if none arrives does it close the session. A deadline is armed only where the engine bounds the request and acts on a `CANCEL`: a buffered SQL EXEC. Transaction control and admin requests carry no `timeout_ms`, and a stream is deliberately sent none (the engine would bound the stream's whole consumption with it), so all three get no client deadline and are bounded by liveness, as item 1 says of a request that carries no bound. The engine's `timeout_ms` also bounds the wait for a pooled connection. Whether F8 keeps "fails only that request" for HTTP or adopts this wait is F8's decision.]*
2. **The socket read timeout becomes a liveness bound, not a deadline.** Reads wait in slices. Silence on the socket while requests are pending triggers a `PING`, which the engine answers from the session reader independently of busy handlers (P18).
   - Only an unanswered `PING`, after a further liveness interval, is a transport failure. Only that poisons the session, under (bx).
   - An SSE stream with a 15 s gap, or an LLM call with 60 s TTFB, never trips it.
3. **Connect and `HELLO`** keep their existing short timeouts.

**Tests (D1c's acceptance):**

- A 30 s-TTFB upstream and a concurrent DB write on the same session **both complete** under the default configuration.
- A 30 s `pg_sleep` with `timeout_ms = 60000` completes.
- A request whose client deadline expires fails alone, and the next request on the session succeeds.
- A silent, dead engine is detected within two liveness intervals.

#### 23.11.1 Native API (`ferro/client`, dependency-free)

```php
$http = $conn->upstream('openai');          // shares $conn's multiplexed session
$res  = $http->request('POST', '/v1/chat/completions', headers: [...], body: $json,
                       timeoutMs: 30_000, idempotent: null, route: '/v1/chat/completions');
$res->status; $res->headers; $res->body; $res->idempotent;   // headers keyed lowercase
$f    = $http->requestAsync(...);           // Ferro\Future<HttpResponse>
[$user, $reply] = Ferro\await([$db->queryOneAsync(...), $f]);

$s = $http->stream('POST', '/v1/chat/completions', ...);  // HEAD awaited, then:
foreach ($s as $chunk) { ... }               // WINDOW_UPDATE as consumed; break → close()
$s->close();                                 // CANCEL + drain: the RawStream contract
```

- **`upstream()` hangs off `Connection`,** which owns the session.
- **Exceptions** are mapped by `ErrorMapper`:

  | Code | PHP exception base |
  |---|---|
  | `UpstreamUnavailable`, `RateLimited` | `RetryableException` (carrying `retryAfterMs`) |
  | `TlsRefused` | `NonRetryableException` |
  | `ResponseIncomplete` | `NonRetryableException`; **`ResponseIncompleteException`** carries `status(): int`, `headers()` and `wasApplied(): ?bool` (`true` after a 2xx head, `null` otherwise), taken from the `HEAD` the client already holds. Its `fate()` is the combined fate (§23.7.1). |
  | `WriteUnconfirmed` | `IndeterminateException` |

  Every HTTP exception exposes `cause(): string`, the registry token from `detail`.
- **The client re-issues nothing** (§23.7.3).
- **(cj)'s "an open stream stays exclusive on its session" is lifted for HTTP streams.** It existed because a statement on the same `tx_id` could queue behind a stalled stream, and an HTTP stream has no transaction relation. The router files frames by `request_id`. Each stream is bounded by the global window (16 MiB / 64 frames), which is its client-side memory bound (§23.9.1). Slice F8's guard asserts the *next* query after an abandoned stream (the C1d lesson).

#### 23.11.2 The Guzzle handler (`ferro/guzzle`)

```php
$stack  = HandlerStack::create(new FerroHandler($conn, upstreams: [
    'https://api.openai.com' => 'openai',
    'https://api.stripe.com' => 'stripe',
]));
$client = new GuzzleHttp\Client(['handler' => $stack]);
```

**Routing.** The handler maps the URI's normalised origin (lowercase ASCII host, default port removed) to an upstream name.

- **An unmapped origin is refused** with `Ferro\Guzzle\UnmappedOriginException extends RequestException` (no response). It is a configuration error and is loud by design. In Laravel it escapes `Http::get()` unwrapped, because Laravel wraps only `ConnectException` (C9).
- **There is no curl fallback by default.** An explicit `fallback:` handler can be configured, and it gives up the SSRF guarantee for unmapped origins (documented).
- **A non-ASCII host is unmapped.** Use punycode in both maps.
- **What the handler sends:** `origin`; a `target` built from the path (an empty path becomes `/`) plus `?query`; never the fragment.

**Promises.** `__invoke` writes the `REQUEST` at once and returns a `GuzzleHttp\Promise\Promise`. Its wait function runs **the handler's wait loop**; its cancel function sends `CANCEL`.

- **The wait loop** pumps responses for every in-flight request and submits due delayed requests (below).
- **Every request in a `Pool`/`Utils::all()` batch is written at `__invoke` time,** so they run concurrently in the engine (P11).

**`delay` (review F16 c).** A request carrying `delay` is not submitted in `__invoke`. It joins the handler's delay queue and is submitted by the wait loop once due, while other requests progress. That is `CurlMultiHandler`'s model. Under D1b's `Ferro\Loop`, the wait suspends the fiber instead. **Test:** a `Pool` of N requests, each with delay d, completes in about d, not N × d.

**Request options:**

| Option | Behaviour |
|---|---|
| `timeout`, `connect_timeout`, `read_timeout` | → fields 7–9 (ms) |
| `stream` | `true`: a lazy PSR-7 stream over `BODY`, with `WINDOW_UPDATE` as read; `close()` or destruction sends `CANCEL` and drains. `false`: buffered into `php://temp`. |
| `sink`, `on_headers`, `progress`, `on_stats` | As stock. `on_headers` throwing sends `CANCEL` and rejects with `RequestException`. Upload progress is reported once, at dispatch. `handlerStats` keys are curl's names (a subset). |
| `decode_content` | → field 11. Headers are renamed `x-encoded-content-*`, as `CurlFactory` does. |
| `delay` | scheduled in the wait loop (above) |
| `version` | does not select the version (§23.8.3) |
| `multiplex` (Guzzle ≥ 7.15) | ignored, except that the `REQUIRE_*` values are refused, naming `HTTP=auto` |
| `verify` | `true` or a CA path: ignored (trust is daemon-side). `false`: **refused**, naming `CA_FILE`. |
| `cert`, `ssl_key`, `proxy`, `force_ip_resolve` | refused, naming the daemon setting or the non-goal |
| `curl`, `stream_context`, `debug` | ignored |
| `ferro` | `['idempotent' => bool, 'route' => string]` |
| `allow_redirects`, `cookies`, `http_errors`, `auth`, `query`, `json`, `form_params`, `multipart` | handled above the handler, unchanged |

**Response.** A `Ferro\Http\FerroResponse extends GuzzleHttp\Psr7\Response`, built from the status, headers (lowercase names), reason, version and body. It carries `ferroFate(): HttpFate`: the idempotent bit, the status class, and `StatusFate`'s verdict. Because PSR-7's `with*()` methods clone, the fate survives header-modifying middleware. A middleware that *rebuilds* the response loses it, and the decider then treats the response as non-idempotent (§23.11.4).

#### 23.11.3 How errors map: by cause, mirroring curl (review F10)

Guzzle's `CurlFactory` (7.15.5, verified) makes `ConnectException` for exactly `OPERATION_TIMEOUTED` (28), `COULDNT_RESOLVE_HOST` (6), `COULDNT_CONNECT` (7), `SSL_CONNECT_ERROR` (35) and `GOT_NOTHING` (52). It makes `RequestException` for everything else, including a send or receive error after sending (55/56) and a peer verification failure (60).

**Ferro maps each cause token to the class curl would have produced for the same physical event.** Every Ferro exception also implements a fate marker (`Ferro\Http\Fate\Retryable`, `…\Indeterminate`, `…\NonRetryable`) and chains the `ferro/client` exception as `getPrevious()`.

| Cause (§23.5.6) | Guzzle rejection | curl analogue | PSR-18 (`ferro/psr18`) |
|---|---|---|---|
| `dns` | `Ferro\Guzzle\ConnectException` | 6 | `NetworkExceptionInterface` |
| `connect_*` | `ConnectException` subclass | 7 / 28 | Network |
| `tls_handshake`, `tls_version`, `tls_alpn` | `ConnectException` subclass | 35 | Network |
| `tls_verify` | `Ferro\Guzzle\RequestException` (no response) | 60 | Network |
| admission (`breaker_*`, `rate_limited`, `retry_after_hold`, `queue_*`, `body_budget`, `deadline`, `draining`) | `ConnectException` subclass (never reached the server; no curl analogue) | — | Network |
| sent, no head: `timeout` | `ConnectException` subclass, marker by fate (Indeterminate if non-idempotent) | 28 | Network |
| sent, no head: `eof_empty` | `ConnectException` subclass, marker by fate | 52 | Network |
| sent, no head: `write`, `reset`, `eof_partial_head`, `malformed_head`, `oversize_head`, `informational_101`, `h2_*` | **`RequestException` subclass** (no response), marker by fate | 55 / 56 / 8 | Network |
| dispatched, not sent: `unsent_write`, `unsent_closed` *(§22.2 (ct))* | `RequestException` subclass (no response), marker Retryable | 55 / 56 | Network |
| `cancelled` | `CancellationException` (the promise was cancelled) | — | n/a |
| after `HEAD`, buffered body failed | `RequestException` carrying the response; the previous exception is `ResponseIncompleteException` | 18 / 56 | Network |
| after `HEAD`, streamed body failed | `\RuntimeException` from `StreamInterface::read()`, carrying the fate | — | same |
| `forbidden_*`, origin unmapped, body over the cap, `verify=false` | `RequestException` subclass (no response) | — | `RequestExceptionInterface` |
| any status | **fulfilled**; `http_errors` throws as stock | — | returned (PSR-18) |

**Consequence.**

- A naive `$e instanceof ConnectException` decider re-sends exactly the events it re-sends under curl (timeout, got-nothing), and **not** a reset after send.
- Laravel wraps `ConnectException` in `Illuminate\Http\Client\ConnectionException` (PendingRequest.php:940), with Ferro's exception as `getPrevious()`. A `RequestException` escapes `Http::send()` raw, as curl's 56 does.

So the *class* behaviour is curl's in every cell. Only the marker is new.

#### 23.11.4 Left to middleware, with deciders shipped (review F15)

Redirects, cookies, auth flows, retries, `Http::fake()` and Telescope's watcher all stay above the handler.

**The one reader of fate is `Ferro\Http\Fate::of(ResponseInterface|\Throwable $x): ?HttpFate`.** It reads:

- a `FerroResponse`'s fate;
- a fate-marked exception;
- a `ConnectionException` or `RequestException` whose `getPrevious()` chain contains one;
- an Illuminate `RequestException`'s `->response->toPsrResponse()`.

It returns `null` when it cannot vouch for the object, and callers **treat `null` as non-idempotent.**

**`Ferro\Guzzle\Retry::decider(int $maxRetries)`** for `Middleware::retry()`:

- retries `Fate\Retryable`;
- retries a response `StatusFate` calls Retryable, honouring `Retry-After`, with backoff otherwise;
- **never** retries `Fate\Indeterminate`, an Indeterminate status, or a `null` fate.

**`Ferro\Laravel\Http\Retry::when()`** is the same rules for `Http::retry(n, when: …)`. It unwraps `ConnectionException::getPrevious()` and reads the Illuminate response.

**Tests** (both through real middleware stacks):

- Guzzle `Middleware::retry` with the decider re-sends a `breaker_open` refusal and does **not** re-send an Indeterminate POST (upstream receive count = 1).
- `Http::retry(3, 0, when: Retry::when())` refuses an Indeterminate POST through Laravel's wrapping, and does retry a `RateLimited`.

#### 23.11.5 PSR-18 (`ferro/psr18`)

`Ferro\Psr18\Client implements Psr\Http\Client\ClientInterface`. It takes a `Connection`, PSR-17 factories and the origin map, and depends only on the PSR interfaces.

- It is synchronous. v1 ships no asynchronous PSR client and no Symfony `HttpClientInterface` (decided, §23.18 Q6).
- Its responses are `FerroResponse`-equivalent wrappers when the factory is Guzzle's. Otherwise `Fate::of()` returns `null`.
- Exceptions follow §23.11.3's PSR-18 column.
- Streaming bodies are lazy, which serves `openai-php/client`.

#### 23.11.6 Laravel (review F11)

**The wiring** lives in `ferro/laravel`'s auto-discovered `FerroServiceProvider`. When an `http.ferro` configuration block is present (the socket and the origin-to-upstream map), it rebinds the container's `Illuminate\Http\Client\Factory` singleton to `Ferro\Laravel\Http\FerroHttpFactory extends Factory`, which overrides only:

```php
protected function newPendingRequest() {
    return parent::newPendingRequest()->setHandler($this->ferroHandler);
}
```

`buildHandlerStack()` then puts Laravel's own before-sending, recorder and **stub** handlers *above* `FerroHandler`, so **`Http::fake()` still wins**. A faked request never reaches the engine, and recording and events behave as stock.

**What was rejected:**

- **`globalOptions(['handler' => …])`.** A request-level handler replaces the whole client stack, bypassing the stub and recorder, and Guzzle 7.12+ deprecates it (ignored in 8.0).
- **A global middleware.** It is pushed outermost, so a short-circuit would skip `Http::fake()`.

**Cost and fragility, stated:**

- `newPendingRequest()` is `protected`. A Laravel release that renames it breaks the wiring loudly at boot: the provider asserts the method exists and refuses to rebind with a clear error rather than routing silently around Ferro.
- CI runs the wiring against the pinned 11.51 and the newest 12.x.
- **Adoption stays config-only** (package plus configuration). "One line" is withdrawn.

**Tests:**

- `Http::fake()` with Ferro wired: zero engine requests.
- `Http::get()` reaches the engine (the contact counter moves).
- Events and recording fire as stock.
- An Indeterminate surfaces as `ConnectionException` or the raw `RequestException` per §23.11.3.

The §15 demo application gains one outbound call through the facade to a local test upstream, with its stock control.

---

### 23.12 The drop-in acceptance bar (D18)

**The stock handler is the control** in every lane. A column is green per D18:

- every control pass also passes through Ferro;
- the skip sets match;
- every Ferro-only non-pass is a documented incompatibility in `docs/known-incompatibilities.md`, or a pre-registered difference that cites §23.16 C9;
- the contact assertions hold.

**Guzzle has no upstream handler-conformance suite that runs unedited with a substituted handler** (P13, measured false: the stock handler is constructed inline in the upstream tests). **M6's acceptance bar is therefore Lane B plus Lane C, below** (decided, §23.18 Q7).

**Lane C (primary): Ferro's transport conformance file.**

- **Shape.** One file, two columns that differ only in the handler: `FerroHandler` versus stock `CurlHandler`/`CurlMultiHandler`.
- **Server.** A Rust test upstream (`testkit`, dev-only `hyper`) serving h1 and h2, with and without TLS. The h2 cases run only where the upstream is `HTTP=auto` and the request is declared idempotent.
- **Porting.** Each case that ports behaviour from Guzzle's `CurlHandlerTest`/`CurlMultiHandlerTest`/`StreamHandlerTest` cites the upstream method it ports. That is where the intent of the cut Lane A survives (review F8).
- **Coverage:**
  - methods, duplicate headers and binary bodies;
  - gzip;
  - 4xx/5xx with `http_errors` on and off;
  - redirects and cookies through middleware;
  - `Pool` concurrency and `delay` scheduling;
  - `on_headers`, `progress`, `on_stats` and `sink`;
  - `RetryMiddleware` with the decider;
  - **SSE incremental delivery** (a stall probe calibrated so that a buffering mutation *fails*, §22.2 (bj));
  - the response-header-name case difference (pre-registered).

**Lane B (upstream, PSR-18): `php-http/client-integration-tests` `HttpClientTest` only.** It uses its own local PHP `TEST_SERVER`. The upstream is declared `ORIGIN=http://localhost:<port>` with `ADDRESS_CLASSES=loopback`, which itself exercises §23.8.5.

- **Two Ferro columns:** Ferro's PSR-18 client, and a `GuzzleHttp\Client` over `HandlerStack::create(new FerroHandler(...))`, which makes Lane B an upstream lane for the Guzzle handler too.
- **One control:** `GuzzleHttp\Client` over curl.
- **Pre-registered differences:** `TRACE` refused (§23.4.1); a `1.0` request sent as 1.1 (§23.8.3); `http://invalid.php-http.org` unmapped, so `RequestExceptionInterface` instead of `NetworkExceptionInterface` (§23.11.2).
- **`HttpFeatureTest` is not run.** It hard-codes `https://httpbin.org/...`, which is public internet, a third party, and (via `/headers` and `/anything`) the very reflection hole of §23.3.4.

**Lane S — not run in v1; the first post-v1 lane: symfony/http-client-contracts `Test\HttpClientTestCase`.** v1 ships no Symfony `HttpClientInterface` (§23.18 Q6), so there is nothing for this lane to test. It is recorded here because it is the one upstream suite built for swapping implementations, and it is the first lane to add after v1:

- 61 tests, an abstract `getHttpClient()`;
- its own local server (`TestHttpServer`, `php -S` via `symfony/process`, on 127.0.0.1:8057/8067/8077 and [::1]:8087);
- control: Symfony's `CurlHttpClient`.

It runs once a Symfony `HttpClientInterface` ships. Expected Ferro-only non-passes, to be pre-registered after a dry run (P21):

- `resolve` (`testResolve`, `testIdnResolve`, `testIPv6Resolve`);
- `proxy`/`no_proxy`;
- `bindto` (`testBindToPort*`);
- inline userinfo auth;
- undeclared ports;
- `http_version` selection.

Until then the documentation says the Symfony seam does not exist in v1, rather than that it is untested.

**Laravel.** The demo app's outbound call (Ferro and control columns), plus §23.11.6's `Http::fake()` proof. Laravel's own `HttpClientTest` runs on `Http::fake()`, never reaches a handler, and is not a lane.

**Contact assertions, in both directions (the C2e shape):**

- **Ferro column:** it refuses to run unless the handler or client under test is Ferro's *and* `ferro_http_requests_total{upstream="<lane>"}` advanced across one probe request. Guzzle's `Utils::chooseHandler()` falls back to curl silently.
- **Control column:** it refuses to run if the counter moves.
- Both guards are mutation-proven.

**What D18 can and cannot claim here:**

- **Lane B** is an upstream suite, green per D18 with three pre-registered differences.
- **Lane C** is green per D18 against a stock control, but it is Ferro-authored. The documentation says so rather than call it "upstream".
- **Lane S** is not run in v1 and is claimed by nothing in v1.

---

### 23.13 The dependency decision (D20) and its security argument

**Adopted:**

- `hyper` 1.x (`default-features = false`; `client`, `http1`, `http2`, which pulls in `h2` and `httparse`);
- `hyper-util` (`tokio` only);
- `http`, `http-body`;
- `tokio-rustls`;
- `rustls` (`default-features = false`; `ring`, `std`, `tls12`);
- `rustls-native-certs`;
- `rustls-pki-types`;
- `unicode-normalization`, used only by `PATH_ENCODING=utf8` (P16 checks its licence).

`flate2` is already locked.

*[Amended 2026-10-06 (M6-F1a, P3 measured, §22.2 (ct)): **`tokio-rustls` must be declared with `default-features = false, features = ["ring", "tls12"]`.** Its default features select `aws_lc_rs`, the backend argument 3 rejects, so a plain `tokio-rustls = "0.26"` would quietly bring back CMake and the `OpenSSL` licence. With that declaration, the measured tree is as follows:*
- *It holds no `aws-lc-*`, `cmake`, `webpki-roots` or OpenSSL crate.*
- *It holds no `httpdate`, so `hyper` is client-only.*
- *`ring` builds through `cc` with a failing `cmake` shim on `PATH` and as `$CMAKE`, which it never invoked.*
- *Every licence in it is on `deny.toml`'s allow-list, unchanged.*

*The full `cargo deny check` (cargo-deny 0.20.2) passes: advisories, bans, licences and sources. Its review round found that the licence check had been **skipping crates reachable only through dev-dependencies**, which `[licenses]` does unless `include-dev = true`. With `ISC` deleted from the allow-list, the check still passed, because `ring`, `rustls-webpki` and `untrusted` are dev-only in the spike. `deny.toml` now sets `include-dev = true`, and with that the same deletion fails on exactly those three. The allow-list is unchanged.]*

All of it sits in `engine/crates/ferro-http`, behind the default-on cargo feature `http`. `--no-default-features` builds a database-only daemon, and CI builds and `cargo deny`s both configurations.

**The argument:**

1. **TLS cannot be hand-rolled.**
2. **This is the opposite of the case that justified hand-rolling.** OTLP and metrics are one fixed request shape to a trusted local peer. Here the input is adversarial: response heads, chunked framing, HPACK and HTTP/2 state from the internet. `hyper`, `h2` and `httparse` are among the most deployed and fuzzed parsers there are. **The credential-holding daemon is the reason to use them, not the reason to avoid them.**
3. **`ring` over `aws-lc-rs`.** `aws-lc-sys` needs CMake and carries the `OpenSSL` licence, which `deny.toml` does not allow. `ring` is `Apache-2.0 AND ISC`. It needs a C compiler (via `cc`), which the tree already requires for `libsqlite3-sys` (P3, review F21).
4. **OS roots over `webpki-roots`.** `webpki-roots` is a stale snapshot under `CDLA-Permissive-2.0`, which is not allowed.
5. **Containment.** `unsafe_code = "forbid"` governs our crates, and `cargo deny check advisories` covers the new tree. `ferro-http` has no path to the pool registry or the DSNs. **Decided: in-process** (§23.18 Q2). Ferro HTTP runs inside `ferrod`, isolated by the crate boundary and the cargo feature, and gated by the full `cargo deny check` (advisories included) that CI already runs, extended to both feature configurations. A separate process behind the socket was declined: it is a chassis change with no measured need. An unpatchable advisory in this tree is D20's revisit trigger.

OTLP and metrics stay hand-rolled.

---

### 23.14 The chaos suite (§20.3, extended to HTTP)

All cases run in `ferrod`'s tests against a Rust fault-injecting upstream that **records every request it received**, so at-most-once is a read-back assertion. Each case asserts branch, code, cause token and receive count.

| # | Fault | Asserted |
|---|---|---|
| 1 | Upstream closes after reading the full request, with no response | `POST`: Indeterminate (`eof_empty`), received exactly 1. Declared-idempotent `GET`: Retryable. Undeclared `GET`: Indeterminate (the §23.7.2 cost, asserted). Never 2. |
| 2 | Reset after reading N bytes of a large body | `POST`: Indeterminate (`reset`); no second request |
| 3 | Refused; `.invalid` NXDOMAIN; wrong-host certificate; expired certificate; reset during the handshake | `UpstreamUnavailable` (`connect_refused`/`dns`/`tls_handshake`); `TlsRefused` (`tls_verify`); received 0 |
| 4 | Stale keep-alive race | POST: success or Indeterminate, never re-sent (≤ 1 each). Residual rate recorded at `H1_UNSAFE_REUSE_MAX_IDLE_MS` default and at 0. |
| 5 | Slow loris head | the deadline fires; POST Indeterminate (`timeout`), declared GET `QueryTimeout`; the connection is evicted |
| 6 | PHP stops sending `WINDOW_UPDATE` | the engine stops reading and the upstream sees backpressure; RSS bounded; a buffering mutation **fails** |
| 7 | HTTP/2 (`HTTP=auto`, declared-idempotent requests): remote `REFUSED_STREAM`; remote GOAWAY below our id; `RST_STREAM(INTERNAL_ERROR)` | all Retryable; a **non-idempotent request on an `auto` upstream is observed on an HTTP/1.1 connection** (asserted at the upstream) |
| 8 | `SIGKILL ferrod` with requests in flight | §23.7.3, cell by cell |
| 9 | Breaker: K connect failures, then traffic; a half-open probe that ends Indeterminate | opens after K; fast refusals, zero dials; the probe closes it; an Indeterminate probe leaves half-open with the slot released (the next request probes) |
| 10 | Rate limit across two sessions | burst shared host-wide; correct `retry_after_ms` |
| 11 | `CANCEL` before dispatch / after dispatch / after `HEAD` | `Cancelled`, received 0 / POST Indeterminate (cause `timeout`), declared GET `Cancelled` / `Cancelled` |
| 12 | Session death with exchanges in flight | aborted; no leaked connection; budget returned to 0 |
| 13 | §23.4.2's refusal corpus and §23.4.3's override/forwarding headers | refused before any dial; the fuzz target runs in the fuzz smoke job |
| 14 | Address guard: a name resolving to `127.0.0.1`, to `fd00:ec2::254`, to `64:ff9b::a9fe:a9fe`; a DNS answer changing between dials | `forbidden_address` with no dial (default class); allowed with `ADDRESS_CLASSES=loopback` for the first only; the connect goes to the checked address |
| 15 | `SIGTERM` with a 10 s POST and a 60 s POST in flight, and a new request during the drain | 10 s: `Ok`; 60 s: Indeterminate at the drain cap, received exactly 1; new request: Retryable `draining`, received 0 |
| 16 | Body budget: concurrent large bodies past `MAX_BODY_BYTES` | Retryable `body_budget`, received 0; the gauge returns to 0 |
| 17 | Maximal response head at the credit floor; head one byte over | delivered / `oversize_head` |

*[Amended M6-F4b (SPEC §22.2 (db)): cases 4, 5, 12, 15 and 16 are `ferrod`'s `http_f4b_it`, and 6's RSS half is `http_rss_it` (its own binary, since a resident-set reading is process-wide). Case 15 runs **scaled** — `FERRO_HTTP_DRAIN_MS` 1.5 s, `drain_deadline` 300 ms, an 800 ms POST for the "10 s" one and a never-answered POST for the "60 s" one — through the real `serve`, and also asserts a declared-idempotent GET without a head (Retryable `ConnectionLost`, `draining`) and two GETs mid-body. Case 12 adds a dropped-exchange variant (the future dropped, not cancelled). Case 4's residual is recorded in §23.8.2.]*

---

### 23.15 Slice plan

Every slice runs the adversarial review before push (D15's process note). Every `/proto` change carries vectors and both codecs.

| Slice | Content | What it proves |
|---|---|---|
| **F0** *(DONE, §22.2 (cn))* | This section; D19–D21 in §21 (D21 merged with Queue's licence decision); the §23.17 amendments; §17 M6 aligned. | The admission test passes in writing; every open choice is decided (§23.18). |
| **D1c** *(M3, prerequisite)* | Per-request client deadlines and PING liveness (§23.11.0). | A slow request no longer poisons a session (SQL and HTTP). Must land before F8. |
| **F1a** *(built and review-fixed, §22.2 (ct); DONE when merged)* | Spike `engine/crates/ferro-http-spike` (ships no library code), **v1 critical path:** P1, P14, P15, P19, P7 (h1), P3, P16, P17 (P18 is measured with D1c). | `sent` is exact on HTTP/1.1; causes are distinguishable; head limits are settable; backpressure is bounded; deps pass cargo-deny. **A false premise here changes §23.7 before F2.** |
| **F1b** | Spike, off the critical path: P2 (with the library-GOAWAY negative control), P5, P7 (h2), P4, P6, P20. | Evidence for F5b and for the post-v1 HTTP/2-writes slice; incompatibility entries measured. |
| **F2** *(BUILT, §22.2 (cy); DONE when merged)* | `/proto`: the registry entries for what F0 allocated in the spec — `HTTP = 6`, three methods, the four codes, `[http.causes]`, the feature bit — plus vectors, both codecs, and PROTOCOL.md §1, §5 (the `detail` sentence) and a new §12 (Queue's messages take §13). `ferrod` routes `HTTP/REQUEST` and answers `Unsupported`; the other HTTP methods route `Unsupported` (tested). | The wire is pinned before behaviour. |
| **F3** | `ferro-http` configuration and the validator (§23.4), with the fuzz target, property gate and refusal corpus. No network. | The SSRF rule in isolation. |
| **F4** | The HTTP/1.1 plaintext engine: DNS, the address guard with pinning, the pool, the write tracker, `ferro_http::fate`, head limits, `HEAD`/`BODY`/`END` through the generalised `Responder`, `CANCEL`, deadlines, budgets, the HTTP drain. Chaos 1, 2, 4, 5, 6, 11, 12, 14, 15, 16, 17. *[Split at M6-F4a (SPEC §22.2 (cz)) into the two rows below.]* | Rules 3 and 4 hold; memory is bounded; restarts behave as stated. |
| **F4a** *(BUILT, §22.2 (cz); DONE when merged)* | The vertical: `ferrod` serves `HTTP/REQUEST` over `http://` behind the default-on `http` feature — §23.4 validation, DNS + the §23.8.5 guard + pinning, the HTTP/1.1 keep-alive pool with §23.8.2's reuse rules, the write tracker armed at dispatch, the exact head limit, `HEAD`/`BODY`/`END` under credit, the total `ferro_http::fate` table including "dispatched, not sent", `CANCEL` and the three deadlines, the `HTTP` feature bit; the wire half of F3's property gate. Chaos 1, 2, 11, 14, 17, and 6's backpressure half. | Rules 3 and 4 hold for HTTP. *[Corrected at F4a's review round, §22.2 (cz).]* Every cell F4a can reach is asserted end to end: the seven `forbidden_*`, `deadline`, `CANCEL`, `dns`, `connect_refused`, `connect_unreachable` (through an injected connect error), `connect_timeout` and the connect bound before dispatch; `unsent_write`, `unsent_closed`, `deadline`, `CANCEL` dispatched-not-sent; `write`, `reset`, `eof_empty`, `eof_partial_head`, `malformed_head`, `oversize_head`, `informational_101`, `timeout`, `cancelled` sent-no-head; `body_reset`, `body_eof`, `body_framing`, `max_response_bytes`, `read_idle`, `timeout`, `CANCEL` after the head; and `Ok`. Not reachable in F4a, so asserted only by the table's unit test: the admission causes (F4b/F6), the TLS causes (F5), the `h2_*` causes (F5b), `decode` (F4b). |
| **F4b** *(BUILT, §22.2 (db); DONE when merged)* | Body budgets (chaos 16), the HTTP drain with both §23.6.1 chassis changes (chaos 15), content decoding (§23.9.2), session death with no leaked connection and the budget back at 0 (chaos 12), the stale keep-alive residual measured at `H1_UNSAFE_REUSE_MAX_IDLE_MS` default and 0 (chaos 4), slow-loris heads (chaos 5), 6's RSS half. | Memory is bounded; restarts behave as stated. *[As built, §22.2 (db):* every case asserted end to end through real sessions, the drain through the real `serve`; the after-dispatch drain-cap events added to §23.7.1's table (108 cells); `body_budget` and `draining` (admission and all three later phases) and `decode` now reachable and asserted; the RSS half in its own test binary. Still asserted only by the table's unit test: the F6 admission causes, the TLS causes, `h2_*`.*]* |
| **F5** | TLS: rustls, roots, `CA_FILE`, mTLS, resumption. Chaos 3. | Real `https`. |
| **F5b** | `HTTP=auto`: the HTTP/2 sub-pool for effectively-idempotent requests. Chaos 7. **Cuttable** without touching anything else. | Multiplexing for declared reads; writes provably stay on HTTP/1.1. |
| **F6** | Limits: concurrency, queue, breaker (with the RAII probe), rate limit, hold. Chaos 9, 10. | Host-level coordination, every refusal unsent. |
| **F7** | Observability: spans from `otelcol`, metrics, slow log, the canary gate. | The redaction contract, mutation-proven. |
| **F8** | The native PHP API: `upstream()`, Futures, streams, §23.7.3, (cj) lifted for HTTP. Chaos 8. **Requires D1c.** | DB and HTTP fan-out on one socket, including a 30 s-TTFB call beside a DB write. |
| **F9** | `ferro/guzzle` (cause mapping, `delay` scheduling, `FerroResponse`, decider), `ferro/psr18`, the Laravel Factory rebinding with the `Http::fake()` proof. | The seams, config-only where the ecosystem allows it. |
| **F10** | Lanes C and B (Lane S is post-v1), with controls and contact assertions; incompatibility entries; the demo app's HTTP call. | D18 parity, or the gap stated. |
| **F11** | Bench on D17: added latency against curl keep-alive on loopback; handshake and connection counts for N workers × M calls; §16 gains a *recorded* HTTP row. | Admission A, measured. |

**The ledger's Phase F rows follow this plan** (replaced at F0). The pre-F0 rows map as: F1 → F4+F5 (+F5b); F2 → F4's fate and §23.14; F3 → F4's streaming; F4 → F6; F5 → F8+F9; F6 → F10. D1c is the M3 row the ledger already holds, now marked as F8's prerequisite.

---

### 23.16 Conflicts with existing text, and how each was resolved

Every row is resolved in this change; the amendments it names are applied in `ferro-spec-v0.2.md`, `docs/product-vision.md` and the ledger, except the `/proto` and `PROTOCOL.md` ones, which land with slice F2.

| # | Existing text | §23 | Resolution |
|---|---|---|---|
| C1 | product-vision §4.2 E: the engine owns "**retries**" | The engine never retries. | Charter rule 3 binds. Read as "retry classification and licensing"; product-vision §4.2 E amended. |
| C2 | product-vision §4.2 D and ledger F0: status → branch as engine taxonomy | Statuses are `Ok`; an advisory PHP helper classifies them. | PSR-18 and `http_errors` force it. **Acknowledged** (§23.18 Q5); product-vision §4.2 D amended. |
| C3 | §22.2 (ce): no HTTP stack in the daemon | `hyper` + `rustls` (D20). | A different case (§23.13). OTLP and metrics stay hand-rolled. |
| C4 | §5 service table `01`–`05` | adds `06 http` | §5 amended; PROTOCOL.md §1 at F2. |
| C5 | §9.2 codes; `QueryTimeout` names a query | four new codes; `QueryTimeout` reused | §9.2 amended. One timeout code. |
| C6 | `Forbidden`: "admin verb … destination" | also HTTP policy | The registry comment widens at F2; §9.2 amended. |
| C7 | an unknown pool is `Unsupported` | an unknown upstream is `Forbidden`, identical to a uid refusal | D15 indistinguishability. Pools keep their precedent. Per-pool uid allow-lists are a recorded gap, not built in v1 (§23.18 Q4). |
| C8 | `HELLO_ACK` carries `pools[]` | no upstream list | No version bump. PHP holds an explicit map. |
| C9 | D18 drop-in parity | **Drop-in differences, each a cited incompatibility entry:** (1) proxy env and `proxy` not honoured; (2) no re-send on a stale reused connection (P4); (3) 600 s default ceiling instead of none; (4) title-cased HTTP/1.1 request names; (5) **response header names lowercase** (`getHeaders()` keys; P6); (6) `verify=false`/`cert`/`ssl_key`/`force_ip_resolve` refused; (7) bodies > 16 MiB refused; (8) no brotli; (9) unmapped origins refused with `UnmappedOriginException` (a `RequestException`, so it escapes Laravel unwrapped); (10) `TRACE`/`TRACK`/`CONNECT` refused case-insensitively; (11) override and forwarding headers refused by default; (12) request-header limits (100 lines / 64 KiB) and response-head limits (256 KiB / 256 fields; P20); (13) PSR-7 `version` does not select the version, and a `1.0` request is sent as 1.1; (14) query-string routing not confined; (15) **`new GuzzleHttp\Client()` without the handler uses curl** (no global seam); (16) restarts cancel exchanges that outlive the HTTP drain; (17) `curl` options ignored, so no `CURLOPT_RESOLVE` equivalent; (18) a body-level `_method` override is not confined. | `docs/known-incompatibilities.md`, checked by `ci/check-incompatibilities-doc.sh`. |
| C10 | §12: credentials in the secret store; DSNs in the environment | attached HTTP credentials are file-only | Not extending the debt. |
| C11 | `ErrorPayload` `sqlstate`/`errno`/`detail` are backend fields | on HTTP: `sqlstate`/`errno` are `nil`, and **`detail` is exactly one `[http.causes]` registry token** | PROTOCOL.md §5 gains one sentence at F2. No shape change. Charter rule 2 holds because the tokens are generated. |
| C12 | §13 `ferro_errors_total`, one span per EXEC | the `ferro_http_*` family, one span per `REQUEST`; `ferro_errors_total` still counts HTTP's error terminals at the one terminal builder | §13 amended. |
| C13 | (cj): an open stream is exclusive on its session | lifted for HTTP streams | Slice F8, with the guard; the per-stream memory bound is stated (§23.9.1). |
| C14 | §18 `ferro.toml`; the tree is env-only | `FERRO_UPSTREAM_<NAME>_*` | The environment convention is kept; `ferro.toml` is not built first (§23.18 Q1). |
| C15 | §17 M6 | matches; PSR-18 is synchronous; the Symfony client is not built | §17 M6 amended; no Symfony client in v1 (§23.18 Q6). |
| C16 | §9.2/§11/§19.3: manifest `idempotent: true` is "the sole licence"; §22.2 (ac): no read/write inference | HTTP licenses a request by the caller's declaration, the operator's per-upstream `IDEMPOTENT_METHODS`, or an operator-declared key header. **Never by method alone.** | **D21** (wording in §23.17) makes the manifest flag the sole licence **for a SQL statement**; every other service defines its licences in its own section, each a declaration or a protocol-defined property of an engine-composed operation, never an inference. It aligns with the SQL tier's honoured `readonly` declaration. The Queue's §24.6 licences are cited under the same entry. The default-empty cost is accepted (§23.18 Q8). |
| C17 | §19 (restart resilience); `drain_deadline` 5 s | HTTP drain 30 s; exchanges beyond it die | Stated cost (§23.6.1), chaos 15, §18 `TimeoutStopSec`. **Accepted** (§23.18 Q9). |
| C18 | the first F0 draft said "on reload"; §18 names a `SIGHUP` reload that `ferrod` does not implement | **no reload in v1** | Credential and root rotation need a restart. A SIGHUP reload is a named post-v1 follow-up (§23.18 Q9). |
| C19 | product-vision §4.2's per-worker failure isolation is implicit | shared client identity, breaker and rate state | Stated in §23.1. `PARTITION=uid` for connections. |
| C20 | ledger D1c (M3) | a hard prerequisite of slice F8 | §23.11.0, in the build order. |
| C21 | D15 indistinguishability of resource names | the metrics endpoint exports upstream names | Stated (§23.10.2). |
| C22 | product-vision §4.2 A and capability text: HTTP/2 sharing across workers | v1 defaults to HTTP/1.1, and writes never ride HTTP/2 | §23.8.3. **Accepted** (§23.18 Q10); product-vision §4.2 A amended. |

### 23.17 §21 entries and the amendments made with them

**D19: Ferro HTTP's contract.**

> The HTTP transport engine (service `HTTP = 6`) sends a request only to an operator-declared upstream, at its declared origin and checked address class, with an origin-form target validated byte by byte and never rewritten (§23.4). It attaches daemon-held credentials from files, refusing a PHP value for an attached name unless the operator opts into override, and never sends a request twice (charter rule 3). A completed exchange is `Outcome::Ok` whatever its status; status classification is an advisory client helper. Fate follows §23.7: `sent` is measured at the HTTP/1.1 connection's plaintext write, and `Indeterminate` is exactly *non-idempotent ∧ sent ∧ no response head*. In v1 a non-idempotent request never rides HTTP/2. Effective idempotency is a declaration (the caller's, or the operator's per upstream), never the method alone.

Revisit triggers:

- spike F1b proves the HTTP/2 licences with their negative control (which unlocks writes over HTTP/2 in a post-v1 slice);
- an upstream class whose fate a response head does not settle.

**D20: The HTTP dependency set.**

> `hyper` 1.x (connection-level client only), `rustls` with `ring`, OS roots and `unicode-normalization`, isolated in the crate `ferro-http` behind the default-on cargo feature `http` and run in-process, gated by the full `cargo deny check` (advisories included) on both feature configurations. TLS and HTTP parsing are never hand-rolled. OTLP and metrics stay hand-rolled.

Revisit triggers: an unpatchable advisory in this tree, or a measured reason to move the engine into its own process (a chassis change; §23.18 Q2).

**D21: Retry licences are defined per service, and none is an inference.**

> Manifest `idempotent: true` (§11) is the sole licence to auto-retry an `Indeterminate` **SQL statement**. Every other service defines its licences in its own section — §23.7.2 for Ferro HTTP, §24.6 for Ferro Queue — and each licence is either a **declaration** (by the caller or by the operator) or a property the **protocol** defines for an operation whose statements the engine itself composes. None is ever inferred from an HTTP method or from user statement text. A licence permits only the caller's policy layer to retry; the engine never re-sends anything (charter rule 3).

For HTTP the licence works through classification, and nothing licenses re-sending an `Indeterminate` HTTP request: a request the caller or the operator declared idempotent is never `Indeterminate` — a link-level failure after sending is `Retryable`, and a timeout or cancel follows §9.2's read rule (`QueryTimeout`, `Cancelled`) (§23.7.1, §23.7.2). For Queue the licences are the autocommit fenced verbs re-sent with the same token, a dedup-keyed single-job `ENQUEUE`, and `RESERVE` (§24.6). Rationale: §22.2 (ac), §23.7.2, §24.6. Both drafts had numbered their licence decision D21; it is one §21 entry, not two.

**Amendments made in the same change** (each an *[Amended …]* note in the existing text):

- **§5:** the service table (`06 http`, `07 queue`), the `HTTP` engine feature bit, and the drain note (a drain signal plumbed into the sessions; `serve` waits `FERRO_HTTP_DRAIN_MS + drain_deadline` while HTTP exchanges are in flight; §23.6.1).
- **§9.2:** the four HTTP codes (and §24's two), `Forbidden` widened, and D21's licence sentence.
- **§11, §19.3:** D21's wording; §19.3 gains §23.7.3's table by reference.
- **§12:** the uid-split statement, the reflection limit, the address guard, file-only attached credentials, and the per-pool allow-list gap.
- **§13:** HTTP spans, metrics, slow log, the `cause` label, and the metrics name-exposure note.
- **§17 M6:** points to §23.15, with D1c named as the prerequisite of slice F8.
- **§18:** HTTP drain vs `TimeoutStopSec`; no reload in v1, so rotation is a restart.
- **§20.1:** `ferro-http`, `php/guzzle`, `php/psr18`. **§20.3:** HTTP chaos (§23.14) and lanes B and C.
- **product-vision §4.2:** C1, C2, C22 and the §23.18 Q8 rewording of D.
- **ledger:** Phase F follows §23.15; D1c is marked as the prerequisite of M6-F8.
- **`/proto`, comments only:** `HTTP = 6` and the four codes are reserved as comments in `methods.toml` and `errors.toml` (no keys, no registry-hash change).
- **Deferred to slice F2, because they change `/proto`'s keys:** `methods.toml`, `errors.toml`, golden vectors, both codecs, and PROTOCOL.md §1, §5 and a new §12.

### 23.18 Decisions taken at adoption

The F0 draft left ten choices open. Each was **decided under the owner's full-freedom grant as applied 2026-10-06 (ledger, "Owner directives and grants")**; the labels Q1–Q10 are kept because the text above cites them.

1. **Q1: configuration format — the environment convention.** `FERRO_UPSTREAMS` plus `FERRO_UPSTREAM_<NAME>_<KEY>`, as `config.rs` already does for pools (§23.3.1). §18's `ferro.toml` is not built first as a chassis slice; when it is built, it maps onto these keys.
2. **Q2: process isolation — in-process.** Ferro HTTP lives in the crate `ferro-http`, behind the default-on cargo feature `http`, inside `ferrod`, with the full `cargo deny check` (advisories included) as its gate on both feature configurations (§23.13, D20). A separate process behind the socket was declined as a chassis change with no measured need.
3. **Q3: large request bodies — no large-request-body path in v1.** A body that would push the `REQUEST` frame past the 16 MiB cap is refused loudly by the client before dispatch (§23.9.3). This is a recorded v1 limit (C9 item 7).
4. **Q4: per-pool uid allow-lists — not in v1.** Upstreams have `ALLOW_UIDS`; database pools (and §24's stores) still admit any admitted peer. The gap is recorded in §12 and §23.2.
5. **Q5: status codes — acknowledged.** An HTTP status code is never an engine error: any completed exchange is `Outcome::Ok`, and status classification is the advisory `StatusFate` helper (§23.7.4, C2).
6. **Q6: client surface — no Symfony `HttpClientInterface` in v1.** v1 ships the native API, the Guzzle handler, a synchronous PSR-18 client and the Laravel wiring. Lane S is therefore not run in v1; it is the first post-v1 lane (§23.12).
7. **Q7: the acceptance bar — Lane B plus Lane C** (§23.12): `php-http/client-integration-tests`' `HttpClientTest` through two Ferro columns against a curl control, and the Ferro-authored, stock-controlled transport conformance file. Guzzle has no upstream handler suite that runs unedited with a substituted handler (P13, measured false).
8. **Q8: idempotency default — `IDEMPOTENT_METHODS` is empty by default.** The method alone licenses nothing; idempotency is a declaration, per request by the caller or per upstream by the operator (§23.7.2, D21). **Cost:** with the defaults an undeclared `GET` that dies after sending is `Indeterminate` to Ferro-aware deciders (the §22.2 (ac) cry-wolf trade), Ferro's decider will not retry it, and it does not ride HTTP/2. Stock and naive deciders are unaffected, because the Guzzle exception class does not change (§23.11.3). Product-vision §4.2 D, which said the licence "maps onto RFC-idempotent methods", is amended to "a declaration — per request, or per upstream by the operator — never the method alone".
9. **Q9: restart semantics — no reload in v1.** Credential and root-store rotation is a restart, and a restart drains HTTP for up to `FERRO_HTTP_DRAIN_MS` (30 s by default); exchanges that outlive it are cancelled and classified (§23.6.1, C17, C18). A SIGHUP reload is a post-v1 follow-up.
10. **Q10: HTTP versions — HTTP/1.1 by default, and a non-idempotent request never rides HTTP/2 in v1** (§23.8.3). Product-vision §4.2 A's "HTTP/2 connection sharing" is delivered in v1 only for declared-idempotent traffic on an upstream set to `HTTP=auto`, until a post-v1 slice proves the HTTP/2 not-processed licence (P2).

### 23.19 Premises not yet measured

Every premise below is owed by the slice that names it, and none may be relied on before it is measured. **F1a's are the v1 critical path:** a false one changes §23.7 (and this section) before F2 starts, not after. F1b's gate only F5b, the post-v1 HTTP/2-writes slice and the incompatibility page.

**F1a: the v1 critical path. These run first, before F2.**

- **P1.** Through `hyper::client::conn::http1`, no byte of a request reaches the I/O before dispatch, and a plaintext-layer tracker armed at dispatch decides `sent` exactly. Required control: a connection that dies before the first write reports `sent = false`.
- **P14.** `hyper`'s h1 client errors let the engine distinguish: EOF with zero response bytes after sending; a reset or write error; a partial or malformed head; and a head over the configured limits. If two are indistinguishable, the Guzzle mapping uses the stricter class (`RequestException`) for both, and §23.11.3 is amended.
- **P15.** `hyper` 1.x's http1 client builder exposes `max_buf_size` and `max_headers`, and the http2 builder `max_header_list_size`. Oversize heads are reported as errors, not truncated.
- **P19.** `http1::SendRequest::try_send_request` returns the message when it was not serialised, and agrees with the tracker.
- **P7 (h1).** Credit-gated reading of a `hyper` body yields TCP backpressure, with no unbounded internal buffering, under a calibrated stall probe (§22.2 (bj)).
- **P3.** The dependency set passes `deny.toml` unchanged and builds with no CMake and no toolchain beyond the existing C compiler. `ring` is `Apache-2.0 AND ISC`; `rustls-native-certs` is `Apache-2.0 OR ISC OR MIT`.
- **P16.** `unicode-normalization`'s licence is on the allow-list. If it is not, `PATH_ENCODING=utf8` ships with a fixed code-point refusal table instead, generated from Unicode data at build time.
- **P17.** During `ferrod`'s drain, an existing session keeps reading frames, so a new HTTP `REQUEST` can be refused with `draining` rather than lost. Measured against `serve.rs`/`session`.

*[Amended 2026-10-06 (M6-F1a, §22.2 (ct)): **F1a's premises are MEASURED and none is refuted. Its adversarial review then amended §23.7.1:** `sent` gained a ciphertext half (F-1), and the fate table gained a "dispatched, not sent" phase (F-3), which P1's own controls exhibit. Evidence: `engine/crates/ferro-http-spike/`, 25 tests. Every premise has a negative control, and 21 of 22 mutations were killed; the 22nd is equivalent, and the README says why. Verdicts:*

| *Premise* | *Verdict* | *Where the text changed* |
|---|---|---|
| *P1* | *HOLDS, with the tracker amended: on `https`, `sent` is plaintext accepted OR ciphertext written since dispatch (a failed TLS write can carry records the upstream decrypts)* | *§23.7.1 (vectored writes; the ciphertext half; the "dispatched, not sent" phase)* |
| *P19* | *HOLDS in one direction only:* message returned ⇒ not sent *(a request serialised into `hyper`'s buffer whose first I/O write fails returns no message)* | *§23.7.1* |
| *P14* | *HOLDS WITH CAVEAT: `eof_empty`/`eof_partial_head` need the tracker's read count, counted from dispatch; `oversize_head`/`malformed_head` need `hyper`'s `Display` text; `write` vs `reset` is not reliably separable (same class, same fate); a 101 arrives as a head* | *§23.5.4, §23.5.6* |
| *P15* | *HOLDS WITH CAVEAT: `max_buf_size` is not exact (heads up to 507 904 B delivered at 256 KiB; hard ceiling < 2×); the exact limit is the engine's own check; `hyper`'s HTTP/2 default is 16 KiB* | *§23.9.1* |
| *P7 (h1)* | *HOLDS: ~0.6 MB past the credit with pinned 128 KiB socket buffers, against 511 MiB for the read-ahead control. After the stall, the upstream must not move for a 3 s hold, so a read-ahead slower than the 300 ms stall window is caught too when its reads reach the socket inside the hold — guaranteed at one frame per 350 ms; slower readers only sometimes (caught at 2–3.2 s locally, missed at 2 s on a GitHub runner)* | *—* |
| *P3* | *HOLDS WITH CAVEAT: `tokio-rustls` defaults to `aws_lc_rs`; no CMake (measured with a failing shim); every licence allowed; the full `cargo deny check` passes, once `deny.toml` stops its licence check skipping dev-only crates (`include-dev = true`, allow-list unchanged)* | *§23.13* |
| *P16* | *HOLDS: `MIT OR Apache-2.0` (`tinyvec`: `Zlib OR Apache-2.0 OR MIT`); NFKC folds U+FF0E/U+2024/U+2025, NFC does not* | *—* |
| *P17* | *HOLDS, bounded by `drain_deadline`: busy and idle sessions answer new frames after the drain starts; after the hard close a request is lost, which is why §23.6.1's chassis change 2 is needed* | *—* |

*Observation for §23.6.1's chassis change 1: the HTTP service can refuse with `draining` by capturing the `Drain` handle in the `HandlerFactory` `main` builds, with no session-layer change. Today `main` builds the factory before the `Drain`, so this is a reorder. The queue waker is separate and was not measured.]*

**Owned by D1c (M3), not by F1a:**

- **P18.** The session reader answers `PING` while request handlers are busy or parked on credit. §23.11.0's liveness design rests on it, so D1c measures it before it relies on it.

**F1b: off the critical path, before F5b and for the incompatibility page.**

- **P2.** Through `hyper`'s http2 client, a stream error can be downcast to `h2::Error`, and the engine can distinguish:
  - (i) `is_reset() && is_remote() && reason() == REFUSED_STREAM`;
  - (ii) `is_go_away() && is_remote()` delivered by `recv_go_away` to a stream above `last_stream_id`;
  - (iii) a request failed by `ensure_no_conn_error` after a remote GOAWAY.

  It must be distinguished from:
  - (iv) **the negative control:** a library-initiated GOAWAY (`handle_go_away`, `Initiator::Library`) on a stream the server **processed**, which must *not* be licensed;
  - (v) `BrokenPipe` on streams ≤ `last_stream_id`;
  - (vi) `hyper`'s own `Canceled`.

  v1 does not depend on P2.
- **P5.** rustls resumes across connections to one upstream, observably per handshake.
- **P7 (h2).** Credit gating withholds HTTP/2 `WINDOW_UPDATE`.
- **P4.** libcurl, through `CurlHandler`, re-sends a `POST` once when a reused connection dies before any response byte.
- **P6.** `hyper` 1.x can title-case request names on HTTP/1.1 but cannot preserve arbitrary case. The original case of *response* names is unavailable through the public API, because `HeaderCaseMap` is crate-private.
- **P20.** curl's response-head limits (believed 300 KiB total, 100 KiB per line), for the incompatibility entry.

**PHP side, before F9 and F10.**

- **P8.** PHP 8.5's `curl_share_init_persistent()` shares within one process only.
- **P9.** Rebinding `Factory` with an overridden `newPendingRequest()` routes `Http::*` through `FerroHandler` on Laravel 11.51 and 12.x, while `Http::fake()`, recording and events still win.
- **P10.** guzzlehttp/psr7 `Uri` percent-encodes every byte §23.4.2 step 3 refuses. Raw `[`/`]` in queries is the case to check.
- **P11.** `EachPromise`/`Pool`/`Utils::all()` work with wait-resolved promises that are all submitted, and with deferred `delay` submission from the wait loop.
- **P21.** Lane S dry run: `HttpClientTestCase` runs against `CurlHttpClient` in `testkit`, with `symfony/process` as a dev dependency, and its Ferro-only non-passes are enumerable. Post-v1, with Lane S (§23.18 Q6); not owed by any v1 slice.

**Verified while this section was drafted, no longer premises** (against downloaded upstream sources; a slice that depends on one re-checks it in-tree):

- **P12.** Guzzle 7.15.5's `CurlFactory::createRejection` maps exactly 28/6/7/35/52 to `ConnectException` and everything else to `RequestException`.
- **P13.** "Guzzle's handler tests run with a substituted handler unedited" is **measured false**: the stock handler is constructed inline. Retired, and Lane A cut.
- Laravel 11.51 wraps only `ConnectException`.
- Guzzle maps request version `1.1` to `CURL_HTTP_VERSION_1_1`.
- `h2`'s `recv_go_away` only errors streams above `last_stream_id`, with `Initiator::Remote`, while `handle_go_away` resets every active stream.
- symfony/http-client-contracts v3.6.0's `HttpClientTestCase` has 61 tests and a bundled local server.

---

### Appendix: review history

This section was drafted as M6-F0, attacked by an adversarial review before adoption, and revised once. The review journalled **26 findings** (3 HIGH, 8 MAJOR including one security finding, 9 MEDIUM, 6 LOW); each was re-checked against its cited source before the revision relied on it, and none was found to overstate. The ids are the "review F*n*" references in the text above.

| # | Sev. | Finding | Disposition |
|---|---|---|---|
| F1 | HIGH | The HTTP/2 GOAWAY licence keyed on `REFUSED_STREAM` never fires, and a library-initiated GOAWAY resets processed streams | Redesigned: non-idempotent requests never ride HTTP/2 in v1; the licence is narrowed and spiked with a negative control (P2), post-v1 only (§23.8.3) |
| F2 | HIGH | The client's 5 s `ioTimeout` is every request's deadline and poisons the session | D1c (per-request client deadlines, PING liveness) made a hard prerequisite of F8 (§23.11.0) |
| F3 | HIGH | Path confinement bypassable via `..;`, `%25` double-encoding, overlong and fullwidth dots | Validator rewritten; corpus extended; query string stated as unconfined (§23.4.2) |
| F4 | MAJOR | `window_bytes` broke §5.2's large-frame rule; response head unbounded | Per-request window cut; head limits set in `hyper` (§23.5, §23.9.1) |
| F5 | MAJOR | A 5 s drain makes in-flight POSTs Indeterminate on every restart | 30 s HTTP drain, new requests refused first, cost stated (§23.6.1) |
| F6 | MAJOR | "Link-local covers metadata" false; IP embeddings; DNS rebinding | Metadata list, NAT64 decoding, `ADDRESS_CLASSES`, address pinning (§23.8.5) |
| F7 | MAJOR | Lane B as drafted needed the public internet and untestable cases | `HttpFeatureTest` dropped; three pre-registered differences; two Ferro columns (§23.12) |
| F8 | MAJOR | Lane A's premise (P13) false | Lane A cut; its intent moved into Lane C (§23.12) |
| F9 | MAJOR | Method-based idempotency is read/write inference | Declarations only; `IDEMPOTENT_METHODS` empty by default (§23.7.2, D21) |
| F10 | MAJOR | `IndeterminateException extends ConnectException` widened naive retries | Exceptions mapped by cause, mirroring `CurlFactory` (§23.11.3) |
| F11 | MEDIUM | No clean Laravel global-handler hook | Factory rebinding, pinned on 11.x and 12.x, `Http::fake()` proven (§23.11.6) |
| F12 | MEDIUM | "Reload" does not exist | No reload in v1; rotation is a restart (§23.6.1) |
| F13 | MEDIUM | "The head settles fate" contradicted the 5xx rule | The head settles the transport's part; combined fate in PHP (§23.7.1) |
| F14 | MEDIUM | An unsent timed-out request reported NonRetryable | Retryable `PoolTimeout` (`deadline`) (§23.6) |
| F15 | MEDIUM | No path from `HttpHead.idempotent` to the deciders | `FerroResponse` and `Fate::of()` (§23.11.4) |
| F16 | MEDIUM | Drop-in differences list incomplete | HTTP/1.1 default; full list in C9 (§23.16) |
| F17 | MAJOR (security) | `ATTACH_POLICY=override` default is a confused deputy | Default `refuse` (§23.3.1) |
| F18 | MEDIUM | ~6 GiB of body memory per upstream by default | Per-upstream and daemon-wide body budgets (§23.8.6) |
| F19 | MEDIUM | Override and forwarding headers defeat confinement | Refused by default, `PASS_HEADERS` exemptions (§23.4.3) |
| F20 | MEDIUM | Cross-tenant fate coupling through shared connections | Removed for writes by F1's design; the rest stated (§23.1) |
| F21 | LOW | P3's "no C toolchain" false for `ring` | Restated; `SSL_CERT_FILE`/`SSL_CERT_DIR` noted (§23.8.8, §23.13) |
| F22 | LOW | Metrics defeat D15 indistinguishability | Refusals count under `_unknown`; exposure stated (§23.10.2) |
| F23 | LOW | Startup check missed an explicit same-uid listing | Warns on own uid in either allow-list (§23.3.3) |
| F24 | LOW | Breaker half-open outcome undefined for non-counted terminals | RAII probe slot (§23.8.6) |
| F25 | LOW | `WriteUnconfirmed (cancelled)` invented a §9.2 cause | Maps to `timeout`; the finer token rides `detail` (§23.7.1) |
| F26 | LOW | Dispatch must route per method | Pinned by a test in F2 (§23.5) |
