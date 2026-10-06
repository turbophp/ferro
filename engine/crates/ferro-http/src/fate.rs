//! The engine-side fate table (SPEC §23.7.1): one pure function from *(phase × event × effective
//! idempotency)* to the request's terminal.
//!
//! The table is §23.7.1's, transcribed, and **total**: [`Situation`] makes every phase carry only
//! the events §23.7.1 lists for it, so an impossible pair (a policy refusal after the head, say)
//! cannot be expressed, and every expressible one has a cell. `fate_is_total_and_matches_the_table`
//! walks every cell, the way `fate_57014_total_over_all_axes` does for SQL.
//!
//! Two facts and one declaration decide every cell, never an inference (D19): whether any byte of
//! the request reached the upstream connection (`sent`, which picks the PHASE), whether a final head
//! arrived (also the phase), and the request's *effective* idempotency (§23.7.2), which the caller
//! computes once at validation. **`Indeterminate` arises in exactly one region: non-idempotent,
//! sent, no head** — the test asserts that as an equivalence over the whole table.
//!
//! Every error carries exactly one `[http.causes]` token in `detail` and `nil` `sqlstate`/`errno`
//! (§23.5.6, C11). Codes, branches and tokens are named ONLY through `ferro-proto`'s generated
//! constants (charter rule 2). The `message` is a fixed sentence: it never quotes anything PHP sent.

use ferro_proto::consts::{errc, http_cause};
use ferro_proto::messages::ErrorPayload;

use crate::validate::PolicyCause;

/// Before dispatch (§23.6 steps 1–5): nothing of the request exists on any connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BeforeDispatch {
    /// A §23.4 validator refusal or the §23.8.5 address guard (`forbidden_*`).
    Policy(PolicyCause),
    RateLimited {
        retry_after_ms: u32,
    },
    RetryAfterHold {
        retry_after_ms: u32,
    },
    BreakerOpen {
        retry_after_ms: u32,
    },
    BreakerProbeBusy,
    Draining,
    QueueFull,
    QueueTimeout,
    BodyBudget,
    /// The request's total deadline elapsed before dispatch.
    Deadline,
    Cancel,
    Dns,
    ConnectRefused,
    ConnectUnreachable,
    ConnectTimeout,
    /// A TLS TRANSPORT failure (Retryable).
    TlsHandshake,
    TlsVerify,
    TlsVersion,
    TlsAlpn,
}

/// Dispatched, not sent (§23.7.1, review F-3): the tracker was armed and the request handed to
/// `hyper`, and — read only after `hyper`'s connection task was aborted — no byte of it reached the
/// socket, in plaintext or in ciphertext.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchedNotSent {
    /// The first I/O failure was on the write side.
    UnsentWrite,
    /// The connection was found closed or reset first.
    UnsentClosed,
    Deadline,
    Cancel,
}

/// Sent, no head: at least one request byte reached the upstream connection, and no final head
/// (2xx–5xx) arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SentNoHead {
    Write,
    Reset,
    EofEmpty,
    EofPartialHead,
    MalformedHead,
    OversizeHead,
    Informational101,
    H2RefusedStream,
    H2GoawayAboveLast,
    H2StreamError,
    H2ConnectionError,
    /// The request's total deadline.
    Timeout,
    Cancel,
}

/// After a final head: the upstream received the request and answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadReceived {
    BodyReset,
    BodyEof,
    BodyFraming,
    H2StreamError,
    Decode,
    MaxResponseBytes,
    ReadIdle,
    /// The request's total deadline.
    Timeout,
    Cancel,
}

/// One cell's row: the phase, carrying the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Situation {
    BeforeDispatch(BeforeDispatch),
    DispatchedNotSent(DispatchedNotSent),
    SentNoHead(SentNoHead),
    HeadReceived(HeadReceived),
}

/// A failed request's terminal: `Outcome::Cancelled`, or an `Outcome::Error`. (A completed exchange
/// is `Outcome::Ok(HttpDone)` whatever its status, §23.7.4, and never reaches this table.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fate {
    Cancelled,
    Error(ErrorPayload),
}

impl Fate {
    /// The error payload, if this fate is an error.
    pub fn error(&self) -> Option<&ErrorPayload> {
        match self {
            Fate::Error(ep) => Some(ep),
            Fate::Cancelled => None,
        }
    }
}

fn err(code: u16, branch: u8, cause: &'static str, message: &str) -> Fate {
    Fate::Error(ErrorPayload {
        code,
        branch,
        sqlstate: None,
        errno: None,
        message: message.to_string(),
        detail: Some(cause.to_string()),
        retry_after_ms: None,
    })
}

fn with_retry_after(f: Fate, ms: u32) -> Fate {
    match f {
        Fate::Error(mut ep) => {
            ep.retry_after_ms = Some(ms);
            Fate::Error(ep)
        }
        other => other,
    }
}

/// The `forbidden_*` token for a policy cause group.
pub fn policy_cause_token(c: PolicyCause) -> &'static str {
    match c {
        PolicyCause::Upstream => http_cause::FORBIDDEN_UPSTREAM,
        PolicyCause::Origin => http_cause::FORBIDDEN_ORIGIN,
        PolicyCause::Target => http_cause::FORBIDDEN_TARGET,
        PolicyCause::Method => http_cause::FORBIDDEN_METHOD,
        PolicyCause::Header => http_cause::FORBIDDEN_HEADER,
        PolicyCause::Body => http_cause::FORBIDDEN_BODY,
        PolicyCause::Address => http_cause::FORBIDDEN_ADDRESS,
    }
}

const NOT_SENT: &str = "the request was not sent upstream (retryable — the engine never re-sends)";
const UNSENT: &str = "the upstream connection failed after dispatch, before any byte of the \
                      request was sent (retryable — the engine never re-sends)";
const LINK_LOST_IDEM: &str = "the upstream connection failed after the request was sent and \
                              before a response head; the request was declared idempotent \
                              (retryable — the engine never re-sends)";
const LINK_LOST_WRITE: &str = "the upstream connection failed after the request was sent and \
                               before a response head; it may or may not have been applied \
                               (§9.2 indeterminate, cause link_lost — the engine never re-sends)";
const TIMEOUT_WRITE: &str = "the request was sent and timed out or was cancelled before a response \
                             head; it may or may not have been applied (§9.2 indeterminate, cause \
                             timeout — the engine never re-sends)";
const BODY_IDEM: &str = "the upstream connection failed while the response body was delivered; \
                         the request was declared idempotent (retryable)";
const BODY_WRITE: &str = "the upstream connection failed while the response body was delivered; \
                          the upstream received and answered the request";

/// SPEC §23.7.1's table. `idempotent` is the request's EFFECTIVE idempotency (§23.7.2); it changes
/// only the "sent, no head" and the link-level "head received" rows.
pub fn classify(s: Situation, idempotent: bool) -> Fate {
    use errc::*;
    match s {
        Situation::BeforeDispatch(e) => match e {
            BeforeDispatch::Policy(c) => err(
                FORBIDDEN,
                FORBIDDEN_BRANCH,
                policy_cause_token(c),
                "refused by the engine's access policy before sending",
            ),
            BeforeDispatch::RateLimited { retry_after_ms } => with_retry_after(
                err(
                    RATE_LIMITED,
                    RATE_LIMITED_BRANCH,
                    http_cause::RATE_LIMITED,
                    NOT_SENT,
                ),
                retry_after_ms,
            ),
            BeforeDispatch::RetryAfterHold { retry_after_ms } => with_retry_after(
                err(
                    RATE_LIMITED,
                    RATE_LIMITED_BRANCH,
                    http_cause::RETRY_AFTER_HOLD,
                    NOT_SENT,
                ),
                retry_after_ms,
            ),
            BeforeDispatch::BreakerOpen { retry_after_ms } => with_retry_after(
                err(
                    UPSTREAM_UNAVAILABLE,
                    UPSTREAM_UNAVAILABLE_BRANCH,
                    http_cause::BREAKER_OPEN,
                    NOT_SENT,
                ),
                retry_after_ms,
            ),
            BeforeDispatch::BreakerProbeBusy => err(
                UPSTREAM_UNAVAILABLE,
                UPSTREAM_UNAVAILABLE_BRANCH,
                http_cause::BREAKER_PROBE_BUSY,
                NOT_SENT,
            ),
            BeforeDispatch::Draining => err(
                UPSTREAM_UNAVAILABLE,
                UPSTREAM_UNAVAILABLE_BRANCH,
                http_cause::DRAINING,
                NOT_SENT,
            ),
            BeforeDispatch::QueueFull => err(
                POOL_TIMEOUT,
                POOL_TIMEOUT_BRANCH,
                http_cause::QUEUE_FULL,
                NOT_SENT,
            ),
            BeforeDispatch::QueueTimeout => err(
                POOL_TIMEOUT,
                POOL_TIMEOUT_BRANCH,
                http_cause::QUEUE_TIMEOUT,
                NOT_SENT,
            ),
            BeforeDispatch::BodyBudget => err(
                POOL_TIMEOUT,
                POOL_TIMEOUT_BRANCH,
                http_cause::BODY_BUDGET,
                NOT_SENT,
            ),
            BeforeDispatch::Deadline => err(
                POOL_TIMEOUT,
                POOL_TIMEOUT_BRANCH,
                http_cause::DEADLINE,
                "the request's deadline elapsed before it was sent (retryable — the engine never \
                 re-sends)",
            ),
            BeforeDispatch::Cancel => Fate::Cancelled,
            BeforeDispatch::Dns => dial(http_cause::DNS),
            BeforeDispatch::ConnectRefused => dial(http_cause::CONNECT_REFUSED),
            BeforeDispatch::ConnectUnreachable => dial(http_cause::CONNECT_UNREACHABLE),
            BeforeDispatch::ConnectTimeout => dial(http_cause::CONNECT_TIMEOUT),
            BeforeDispatch::TlsHandshake => dial(http_cause::TLS_HANDSHAKE),
            BeforeDispatch::TlsVerify => tls_refused(http_cause::TLS_VERIFY),
            BeforeDispatch::TlsVersion => tls_refused(http_cause::TLS_VERSION),
            BeforeDispatch::TlsAlpn => tls_refused(http_cause::TLS_ALPN),
        },
        Situation::DispatchedNotSent(e) => match e {
            DispatchedNotSent::UnsentWrite => err(
                CONNECTION_LOST,
                CONNECTION_LOST_BRANCH,
                http_cause::UNSENT_WRITE,
                UNSENT,
            ),
            DispatchedNotSent::UnsentClosed => err(
                CONNECTION_LOST,
                CONNECTION_LOST_BRANCH,
                http_cause::UNSENT_CLOSED,
                UNSENT,
            ),
            // `PoolTimeout`, not `QueryTimeout`: nothing was sent (§23.7.1).
            DispatchedNotSent::Deadline => err(
                POOL_TIMEOUT,
                POOL_TIMEOUT_BRANCH,
                http_cause::DEADLINE,
                "the request's deadline elapsed after dispatch, before any byte was sent \
                 (retryable — the engine never re-sends)",
            ),
            DispatchedNotSent::Cancel => Fate::Cancelled,
        },
        Situation::SentNoHead(e) => {
            let link = |cause| {
                if idempotent {
                    err(
                        CONNECTION_LOST,
                        CONNECTION_LOST_BRANCH,
                        cause,
                        LINK_LOST_IDEM,
                    )
                } else {
                    err(
                        WRITE_UNCONFIRMED,
                        WRITE_UNCONFIRMED_BRANCH,
                        cause,
                        LINK_LOST_WRITE,
                    )
                }
            };
            match e {
                SentNoHead::Write => link(http_cause::WRITE),
                SentNoHead::Reset => link(http_cause::RESET),
                SentNoHead::EofEmpty => link(http_cause::EOF_EMPTY),
                SentNoHead::EofPartialHead => link(http_cause::EOF_PARTIAL_HEAD),
                SentNoHead::MalformedHead => link(http_cause::MALFORMED_HEAD),
                SentNoHead::OversizeHead => link(http_cause::OVERSIZE_HEAD),
                SentNoHead::Informational101 => link(http_cause::INFORMATIONAL_101),
                SentNoHead::H2RefusedStream => link(http_cause::H2_REFUSED_STREAM),
                SentNoHead::H2GoawayAboveLast => link(http_cause::H2_GOAWAY_ABOVE_LAST),
                SentNoHead::H2StreamError => link(http_cause::H2_STREAM_ERROR),
                SentNoHead::H2ConnectionError => link(http_cause::H2_CONNECTION_ERROR),
                SentNoHead::Timeout if idempotent => err(
                    QUERY_TIMEOUT,
                    QUERY_TIMEOUT_BRANCH,
                    http_cause::TIMEOUT,
                    "the request timed out before a response head (declared idempotent)",
                ),
                SentNoHead::Timeout => err(
                    WRITE_UNCONFIRMED,
                    WRITE_UNCONFIRMED_BRANCH,
                    http_cause::TIMEOUT,
                    TIMEOUT_WRITE,
                ),
                SentNoHead::Cancel if idempotent => Fate::Cancelled,
                // Review F25: the §9.2 cause is `timeout`, as a cancelled SQL autocommit write; the
                // finer token rides `detail`.
                SentNoHead::Cancel => err(
                    WRITE_UNCONFIRMED,
                    WRITE_UNCONFIRMED_BRANCH,
                    http_cause::CANCELLED,
                    TIMEOUT_WRITE,
                ),
            }
        }
        Situation::HeadReceived(e) => {
            let link = |cause| {
                if idempotent {
                    err(CONNECTION_LOST, CONNECTION_LOST_BRANCH, cause, BODY_IDEM)
                } else {
                    err(
                        RESPONSE_INCOMPLETE,
                        RESPONSE_INCOMPLETE_BRANCH,
                        cause,
                        BODY_WRITE,
                    )
                }
            };
            match e {
                HeadReceived::BodyReset => link(http_cause::BODY_RESET),
                HeadReceived::BodyEof => link(http_cause::BODY_EOF),
                HeadReceived::BodyFraming => link(http_cause::BODY_FRAMING),
                HeadReceived::H2StreamError => link(http_cause::H2_STREAM_ERROR),
                HeadReceived::Decode => err(
                    RESPONSE_INCOMPLETE,
                    RESPONSE_INCOMPLETE_BRANCH,
                    http_cause::DECODE,
                    "the response body could not be decoded",
                ),
                HeadReceived::MaxResponseBytes => err(
                    RESPONSE_INCOMPLETE,
                    RESPONSE_INCOMPLETE_BRANCH,
                    http_cause::MAX_RESPONSE_BYTES,
                    "the response exceeded the upstream's MAX_RESPONSE_BYTES",
                ),
                HeadReceived::ReadIdle => err(
                    QUERY_TIMEOUT,
                    QUERY_TIMEOUT_BRANCH,
                    http_cause::READ_IDLE,
                    "the response body was idle longer than the read timeout",
                ),
                HeadReceived::Timeout => err(
                    QUERY_TIMEOUT,
                    QUERY_TIMEOUT_BRANCH,
                    http_cause::TIMEOUT,
                    "the request timed out while the response was delivered",
                ),
                HeadReceived::Cancel => Fate::Cancelled,
            }
        }
    }
}

fn dial(cause: &'static str) -> Fate {
    err(
        errc::UPSTREAM_UNAVAILABLE,
        errc::UPSTREAM_UNAVAILABLE_BRANCH,
        cause,
        "the upstream could not be reached; nothing was sent (retryable — the engine never \
         re-sends)",
    )
}

fn tls_refused(cause: &'static str) -> Fate {
    err(
        errc::TLS_REFUSED,
        errc::TLS_REFUSED_BRANCH,
        cause,
        "the upstream's TLS identity or parameters were refused; nothing was sent",
    )
}

/// Every [`Situation`], for totality tests (the enums are closed, so a new variant must be added
/// here or `every_situation_is_listed` fails).
pub fn all_situations() -> Vec<Situation> {
    use BeforeDispatch as B;
    use DispatchedNotSent as N;
    use HeadReceived as A;
    use SentNoHead as S;
    let mut v = Vec::new();
    for c in [
        PolicyCause::Upstream,
        PolicyCause::Origin,
        PolicyCause::Target,
        PolicyCause::Method,
        PolicyCause::Header,
        PolicyCause::Body,
        PolicyCause::Address,
    ] {
        v.push(Situation::BeforeDispatch(B::Policy(c)));
    }
    for b in [
        B::RateLimited { retry_after_ms: 7 },
        B::RetryAfterHold { retry_after_ms: 7 },
        B::BreakerOpen { retry_after_ms: 7 },
        B::BreakerProbeBusy,
        B::Draining,
        B::QueueFull,
        B::QueueTimeout,
        B::BodyBudget,
        B::Deadline,
        B::Cancel,
        B::Dns,
        B::ConnectRefused,
        B::ConnectUnreachable,
        B::ConnectTimeout,
        B::TlsHandshake,
        B::TlsVerify,
        B::TlsVersion,
        B::TlsAlpn,
    ] {
        v.push(Situation::BeforeDispatch(b));
    }
    for n in [N::UnsentWrite, N::UnsentClosed, N::Deadline, N::Cancel] {
        v.push(Situation::DispatchedNotSent(n));
    }
    for s in [
        S::Write,
        S::Reset,
        S::EofEmpty,
        S::EofPartialHead,
        S::MalformedHead,
        S::OversizeHead,
        S::Informational101,
        S::H2RefusedStream,
        S::H2GoawayAboveLast,
        S::H2StreamError,
        S::H2ConnectionError,
        S::Timeout,
        S::Cancel,
    ] {
        v.push(Situation::SentNoHead(s));
    }
    for a in [
        A::BodyReset,
        A::BodyEof,
        A::BodyFraming,
        A::H2StreamError,
        A::Decode,
        A::MaxResponseBytes,
        A::ReadIdle,
        A::Timeout,
        A::Cancel,
    ] {
        v.push(Situation::HeadReceived(a));
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_proto::consts::branch;

    /// The exhaustive-match guard for [`all_situations`]: each arm names a variant, so adding one to
    /// an enum without listing it in `all_situations` fails to compile here (no `_` arm), and a
    /// listed-but-missing one fails the count.
    #[test]
    fn every_situation_is_listed() {
        let all = all_situations();
        let mut counts = [0usize; 4];
        for s in &all {
            let i = match s {
                Situation::BeforeDispatch(b) => {
                    match b {
                        BeforeDispatch::Policy(_)
                        | BeforeDispatch::RateLimited { .. }
                        | BeforeDispatch::RetryAfterHold { .. }
                        | BeforeDispatch::BreakerOpen { .. }
                        | BeforeDispatch::BreakerProbeBusy
                        | BeforeDispatch::Draining
                        | BeforeDispatch::QueueFull
                        | BeforeDispatch::QueueTimeout
                        | BeforeDispatch::BodyBudget
                        | BeforeDispatch::Deadline
                        | BeforeDispatch::Cancel
                        | BeforeDispatch::Dns
                        | BeforeDispatch::ConnectRefused
                        | BeforeDispatch::ConnectUnreachable
                        | BeforeDispatch::ConnectTimeout
                        | BeforeDispatch::TlsHandshake
                        | BeforeDispatch::TlsVerify
                        | BeforeDispatch::TlsVersion
                        | BeforeDispatch::TlsAlpn => {}
                    }
                    0
                }
                Situation::DispatchedNotSent(n) => {
                    match n {
                        DispatchedNotSent::UnsentWrite
                        | DispatchedNotSent::UnsentClosed
                        | DispatchedNotSent::Deadline
                        | DispatchedNotSent::Cancel => {}
                    }
                    1
                }
                Situation::SentNoHead(s) => {
                    match s {
                        SentNoHead::Write
                        | SentNoHead::Reset
                        | SentNoHead::EofEmpty
                        | SentNoHead::EofPartialHead
                        | SentNoHead::MalformedHead
                        | SentNoHead::OversizeHead
                        | SentNoHead::Informational101
                        | SentNoHead::H2RefusedStream
                        | SentNoHead::H2GoawayAboveLast
                        | SentNoHead::H2StreamError
                        | SentNoHead::H2ConnectionError
                        | SentNoHead::Timeout
                        | SentNoHead::Cancel => {}
                    }
                    2
                }
                Situation::HeadReceived(a) => {
                    match a {
                        HeadReceived::BodyReset
                        | HeadReceived::BodyEof
                        | HeadReceived::BodyFraming
                        | HeadReceived::H2StreamError
                        | HeadReceived::Decode
                        | HeadReceived::MaxResponseBytes
                        | HeadReceived::ReadIdle
                        | HeadReceived::Timeout
                        | HeadReceived::Cancel => {}
                    }
                    3
                }
            };
            counts[i] += 1;
        }
        // 7 policy groups + 18 other pre-dispatch events; 4; 13; 9.
        assert_eq!(counts, [25, 4, 13, 9]);
        let mut dedup = all.clone();
        dedup.dedup();
        assert_eq!(dedup.len(), all.len());
    }

    /// The expected `(code, branch, cause)` of every cell, written from §23.7.1's table rather than
    /// from `classify`: `None` means `Outcome::Cancelled`.
    fn expected(s: Situation, idem: bool) -> Option<(u16, u8, &'static str)> {
        use BeforeDispatch as B;
        use errc::*;
        let pool = |c| Some((POOL_TIMEOUT, branch::RETRYABLE, c));
        let up = |c| Some((UPSTREAM_UNAVAILABLE, branch::RETRYABLE, c));
        let tls = |c| Some((TLS_REFUSED, branch::NON_RETRYABLE, c));
        match s {
            Situation::BeforeDispatch(b) => match b {
                B::Policy(c) => Some((FORBIDDEN, branch::NON_RETRYABLE, policy_cause_token(c))),
                B::RateLimited { .. } => {
                    Some((RATE_LIMITED, branch::RETRYABLE, http_cause::RATE_LIMITED))
                }
                B::RetryAfterHold { .. } => Some((
                    RATE_LIMITED,
                    branch::RETRYABLE,
                    http_cause::RETRY_AFTER_HOLD,
                )),
                B::BreakerOpen { .. } => up(http_cause::BREAKER_OPEN),
                B::BreakerProbeBusy => up(http_cause::BREAKER_PROBE_BUSY),
                B::Draining => up(http_cause::DRAINING),
                B::QueueFull => pool(http_cause::QUEUE_FULL),
                B::QueueTimeout => pool(http_cause::QUEUE_TIMEOUT),
                B::BodyBudget => pool(http_cause::BODY_BUDGET),
                B::Deadline => pool(http_cause::DEADLINE),
                B::Cancel => None,
                B::Dns => up(http_cause::DNS),
                B::ConnectRefused => up(http_cause::CONNECT_REFUSED),
                B::ConnectUnreachable => up(http_cause::CONNECT_UNREACHABLE),
                B::ConnectTimeout => up(http_cause::CONNECT_TIMEOUT),
                B::TlsHandshake => up(http_cause::TLS_HANDSHAKE),
                B::TlsVerify => tls(http_cause::TLS_VERIFY),
                B::TlsVersion => tls(http_cause::TLS_VERSION),
                B::TlsAlpn => tls(http_cause::TLS_ALPN),
            },
            Situation::DispatchedNotSent(n) => match n {
                DispatchedNotSent::UnsentWrite => {
                    Some((CONNECTION_LOST, branch::RETRYABLE, http_cause::UNSENT_WRITE))
                }
                DispatchedNotSent::UnsentClosed => Some((
                    CONNECTION_LOST,
                    branch::RETRYABLE,
                    http_cause::UNSENT_CLOSED,
                )),
                DispatchedNotSent::Deadline => pool(http_cause::DEADLINE),
                DispatchedNotSent::Cancel => None,
            },
            Situation::SentNoHead(e) => {
                let token = match e {
                    SentNoHead::Write => http_cause::WRITE,
                    SentNoHead::Reset => http_cause::RESET,
                    SentNoHead::EofEmpty => http_cause::EOF_EMPTY,
                    SentNoHead::EofPartialHead => http_cause::EOF_PARTIAL_HEAD,
                    SentNoHead::MalformedHead => http_cause::MALFORMED_HEAD,
                    SentNoHead::OversizeHead => http_cause::OVERSIZE_HEAD,
                    SentNoHead::Informational101 => http_cause::INFORMATIONAL_101,
                    SentNoHead::H2RefusedStream => http_cause::H2_REFUSED_STREAM,
                    SentNoHead::H2GoawayAboveLast => http_cause::H2_GOAWAY_ABOVE_LAST,
                    SentNoHead::H2StreamError => http_cause::H2_STREAM_ERROR,
                    SentNoHead::H2ConnectionError => http_cause::H2_CONNECTION_ERROR,
                    SentNoHead::Timeout => http_cause::TIMEOUT,
                    SentNoHead::Cancel => http_cause::CANCELLED,
                };
                match (e, idem) {
                    (SentNoHead::Cancel, true) => None,
                    (SentNoHead::Timeout, true) => {
                        Some((QUERY_TIMEOUT, branch::NON_RETRYABLE, token))
                    }
                    (_, true) => Some((CONNECTION_LOST, branch::RETRYABLE, token)),
                    (_, false) => Some((WRITE_UNCONFIRMED, branch::INDETERMINATE, token)),
                }
            }
            Situation::HeadReceived(a) => match a {
                HeadReceived::BodyReset
                | HeadReceived::BodyEof
                | HeadReceived::BodyFraming
                | HeadReceived::H2StreamError => {
                    let token = match a {
                        HeadReceived::BodyReset => http_cause::BODY_RESET,
                        HeadReceived::BodyEof => http_cause::BODY_EOF,
                        HeadReceived::BodyFraming => http_cause::BODY_FRAMING,
                        _ => http_cause::H2_STREAM_ERROR,
                    };
                    if idem {
                        Some((CONNECTION_LOST, branch::RETRYABLE, token))
                    } else {
                        Some((RESPONSE_INCOMPLETE, branch::NON_RETRYABLE, token))
                    }
                }
                HeadReceived::Decode => Some((
                    RESPONSE_INCOMPLETE,
                    branch::NON_RETRYABLE,
                    http_cause::DECODE,
                )),
                HeadReceived::MaxResponseBytes => Some((
                    RESPONSE_INCOMPLETE,
                    branch::NON_RETRYABLE,
                    http_cause::MAX_RESPONSE_BYTES,
                )),
                HeadReceived::ReadIdle => {
                    Some((QUERY_TIMEOUT, branch::NON_RETRYABLE, http_cause::READ_IDLE))
                }
                HeadReceived::Timeout => {
                    Some((QUERY_TIMEOUT, branch::NON_RETRYABLE, http_cause::TIMEOUT))
                }
                HeadReceived::Cancel => None,
            },
        }
    }

    /// **Totality** (the `fate_57014_total_over_all_axes` precedent): every cell of §23.7.1's table,
    /// for both idempotency columns, resolves to the table's answer; every error carries exactly one
    /// registry cause token, nil `sqlstate`/`errno`, a code whose registry branch is the wire branch,
    /// and `retry_after_ms` exactly where §23.7.1 says; and **`Indeterminate` arises in exactly the
    /// non-idempotent ∧ sent ∧ no-head region**.
    #[test]
    fn fate_is_total_and_matches_the_table() {
        let mut cells = 0;
        for s in all_situations() {
            for idem in [false, true] {
                cells += 1;
                let got = classify(s, idem);
                match (expected(s, idem), &got) {
                    (None, Fate::Cancelled) => {}
                    (Some((code, br, cause)), Fate::Error(ep)) => {
                        assert_eq!(
                            (ep.code, ep.branch, ep.detail.as_deref()),
                            (code, br, Some(cause)),
                            "{s:?} idempotent={idem}"
                        );
                        assert!(
                            http_cause::ALL.contains(&cause),
                            "{s:?}: {cause} is a registry token"
                        );
                        assert_eq!((ep.sqlstate.as_deref(), ep.errno), (None, None), "{s:?}");
                        let registry_branch = errc::ALL
                            .iter()
                            .find(|(_, c, _)| *c == ep.code)
                            .map(|(_, _, b)| *b);
                        assert_eq!(registry_branch, Some(ep.branch), "{s:?}: branch");
                        let wants_retry_after = matches!(
                            s,
                            Situation::BeforeDispatch(
                                BeforeDispatch::RateLimited { .. }
                                    | BeforeDispatch::RetryAfterHold { .. }
                                    | BeforeDispatch::BreakerOpen { .. }
                            )
                        );
                        assert_eq!(
                            ep.retry_after_ms.is_some(),
                            wants_retry_after,
                            "{s:?}: retry_after_ms"
                        );
                    }
                    (want, got) => panic!("{s:?} idempotent={idem}: want {want:?}, got {got:?}"),
                }
                let indeterminate = got
                    .error()
                    .is_some_and(|ep| ep.branch == branch::INDETERMINATE);
                let region = !idem && matches!(s, Situation::SentNoHead(_));
                assert_eq!(
                    indeterminate, region,
                    "{s:?} idempotent={idem}: Indeterminate must be exactly non-idempotent ∧ sent ∧ no head"
                );
            }
        }
        assert_eq!(cells, 102);
    }

    /// The table and the registry vocabulary are the same set: every `[http.causes]` token is the
    /// `detail` of at least one cell, and no cell names anything else. A token the registry gains
    /// without a fate cell — or a cell that names an unregistered token — fails here.
    #[test]
    fn every_registry_cause_has_a_cell() {
        let mut produced = std::collections::BTreeSet::new();
        for s in all_situations() {
            for idem in [false, true] {
                if let Fate::Error(ep) = classify(s, idem) {
                    produced.insert(ep.detail.expect("a cause"));
                }
            }
        }
        let registry: std::collections::BTreeSet<String> =
            http_cause::ALL.iter().map(|t| t.to_string()).collect();
        assert_eq!(produced, registry);
    }
}
