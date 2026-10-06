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
//! Nothing here is wired into `ferrod` yet (slice F4 does that, behind the default-on `http`
//! feature, §23.13). Choices §23 left open are recorded in SPEC §22.2 (cw).

pub mod address;
pub mod attach;
pub mod config;
pub mod origin;
pub mod validate;

#[doc(hidden)]
pub mod fuzzing;

mod syntax;

pub use config::{HttpConfig, Upstream};
pub use validate::{PolicyCause, Refusal, Request, Rule, Validated, validate};
