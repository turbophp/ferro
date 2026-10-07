//! Ferro HTTP, the outbound HTTP transport engine (SPEC §23, `docs/spec/23-http.md`).
//!
//! **Slice M6-F3: configuration and the request validator only — no network.** This is the SSRF
//! rule in isolation:
//!
//! - [`config`]: named upstreams from `FERRO_UPSTREAMS` / `FERRO_UPSTREAM_<NAME>_<KEY>` and the
//!   daemon-wide `FERRO_HTTP_*` keys (§23.3.1), with credential custody for attached headers
//!   (§23.3.2, [`attach`]);
//! - [`origin`]: the strict `ORIGIN` parser and its normalised form (§23.3.1, §23.4.2);
//! - [`validate`]: method, target, `origin` field, headers and body (§23.4), and effective
//!   idempotency (§23.7.2), which validation computes in the same step (§23.6 step 3);
//! - [`address`]: the address guard's classification (§23.8.5). Slice F4 resolves and pins; the
//!   decision lives here so it can be tested without a resolver.
//!
//! **Slice M6-F4a adds** [`fate`] (§23.7.1's table, pure and total) and, behind the `engine`
//! feature, [`engine`]: the HTTP/1.1 plaintext request lifecycle `ferrod` serves service `HTTP`
//! with (DNS, the address guard with pinning, the keep-alive pool, the write tracker, `HEAD`/`BODY`
//! under credit, `CANCEL` and deadlines). Choices §23 left open are recorded in SPEC §22.2 (cw) and
//! (cz). Later slices extend the engine: budgets and the drain (F4b), TLS (F5a, F5c) and the
//! host-level limits — breaker, Retry-After hold, rate limit, queue, connection caps (F6, (dk)).

pub mod address;
pub mod attach;
pub mod config;
pub mod fate;
pub mod origin;
pub mod validate;

/// The HTTP/1.1 plaintext engine (slice M6-F4a), behind this crate's `engine` feature, which
/// `ferrod`'s default-on `http` feature enables. Off by default so the fuzz targets stay tokio-free.
#[cfg(feature = "engine")]
pub mod engine;

#[doc(hidden)]
pub mod fuzzing;

/// Test scaffolding: certificates generated in-test (M6-F5a). Feature `test-certs` only — never in a
/// daemon build.
#[cfg(any(feature = "test-certs", all(test, feature = "engine")))]
#[doc(hidden)]
pub mod testcert;

mod syntax;

pub use config::{HttpConfig, Upstream};
pub use syntax::fold_name;
pub use validate::{PolicyCause, Refusal, Request, Rule, Validated, validate};
