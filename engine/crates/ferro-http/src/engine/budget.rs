//! Request-body budgets (SPEC §23.8.6, review F18; slice M6-F4b).
//!
//! A request's body is **charged at admission** (§23.6 step 4d) to two accounts — its upstream's
//! `MAX_BODY_BYTES` (64 MiB) and the daemon-wide `FERRO_HTTP_MAX_BODY_BYTES` (256 MiB) — and
//! **released when it has been fully written upstream, or at the request's terminal**, whichever
//! comes first. Over either account the request is Retryable `PoolTimeout` (`body_budget`), not
//! sent. So the request-body memory the engine retains is bounded by the daemon budget, not by
//! `(MAX_REQUESTS + MAX_QUEUED) × 16 MiB` per upstream.
//!
//! **"Fully written" is measured, not assumed.** The engine hands `hyper` the body as ONE `Bytes`
//! whose owner carries a [`Charge`] holder ([`Charge::wrap_body`]); the request's own exchange
//! keeps a second holder. Whichever holder drops first releases the charge exactly once:
//!
//! - `hyper` 1.x's h1 write path, under the `Queue` strategy the engine forces
//!   (`http1::Builder::writev(true)`), PUSHES the body buffer into its write queue without copying
//!   it and drops it only when its last byte has been accepted by the socket (`proto/h1/io.rs`
//!   `WriteBuf::buffer`, `common/buf.rs` `BufList::advance`). So the owner's drop IS "fully
//!   written" — the moment the memory is actually gone. (Under `Flatten` `hyper` would COPY the body
//!   into its own buffer and drop ours at once, releasing the charge while the copy is still held;
//!   that is why the engine pins the strategy rather than inheriting it from `is_write_vectored`.)
//! - the exchange's holder drops at the terminal, so a body never fully written (a cancel, a
//!   failure, a connection discarded with the body still queued) is released then at the latest.
//!
//! Under `PARTITION=uid` the budget stays per upstream (§23.8.1): the provider's limits are per
//! key, not per tenant.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;

use crate::config::{HttpConfig, UpstreamEntry};

/// One budget account: a limit and what is currently charged against it.
#[derive(Debug)]
struct Account {
    limit: u64,
    used: AtomicU64,
}

impl Account {
    fn new(limit: u64) -> Arc<Self> {
        Arc::new(Account {
            limit,
            used: AtomicU64::new(0),
        })
    }

    /// Take `n` if it fits under the limit; never over-commits, even under contention.
    fn try_take(&self, n: u64) -> bool {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                cur.checked_add(n).filter(|&next| next <= self.limit)
            })
            .is_ok()
    }

    fn give(&self, n: u64) {
        self.used.fetch_sub(n, Ordering::SeqCst);
    }

    fn used(&self) -> u64 {
        self.used.load(Ordering::SeqCst)
    }
}

/// The engine's body budgets: the daemon account and one account per enabled upstream.
#[derive(Debug)]
pub struct Budgets {
    daemon: Arc<Account>,
    upstreams: HashMap<String, Arc<Account>>,
}

impl Budgets {
    pub fn new(config: &HttpConfig) -> Self {
        let upstreams = config
            .entries()
            .filter_map(|(name, e)| match e {
                UpstreamEntry::Enabled(up) => {
                    Some((name.to_string(), Account::new(up.limits.max_body_bytes)))
                }
                UpstreamEntry::Disabled(_) => None,
            })
            .collect();
        Budgets {
            daemon: Account::new(config.daemon.max_body_bytes),
            upstreams,
        }
    }

    /// Charge `n` body bytes to `upstream` and to the daemon (§23.6 step 4d). `Ok(None)`: nothing to
    /// charge (no body, or an empty one). `Err(())`: over either budget — `body_budget`.
    #[allow(clippy::result_unit_err)]
    pub fn charge(&self, upstream: &str, n: u64) -> Result<Option<Charge>, ()> {
        if n == 0 {
            return Ok(None);
        }
        // Every request that reaches admission was validated against an ENABLED upstream, which has
        // an account; a missing one would be an engine defect, and refusing is the safe answer.
        let Some(up) = self.upstreams.get(upstream) else {
            return Err(());
        };
        if !up.try_take(n) {
            return Err(());
        }
        if !self.daemon.try_take(n) {
            up.give(n);
            return Err(());
        }
        Ok(Some(Charge(Arc::new(ChargeInner {
            n,
            released: AtomicBool::new(false),
            upstream: Arc::clone(up),
            daemon: Arc::clone(&self.daemon),
        }))))
    }

    /// Body bytes currently charged to `upstream` (the `ferro_http_body_budget_bytes` gauge, F7).
    pub fn in_use(&self, upstream: &str) -> u64 {
        self.upstreams.get(upstream).map_or(0, |a| a.used())
    }

    /// Body bytes currently charged daemon-wide (the gauge's `_daemon` series, F7).
    pub fn daemon_in_use(&self) -> u64 {
        self.daemon.used()
    }
}

#[derive(Debug)]
struct ChargeInner {
    n: u64,
    released: AtomicBool,
    upstream: Arc<Account>,
    daemon: Arc<Account>,
}

impl ChargeInner {
    fn release(&self) {
        if !self.released.swap(true, Ordering::SeqCst) {
            self.upstream.give(self.n);
            self.daemon.give(self.n);
        }
    }
}

/// A HOLDER of one body's charge. Unlike a reference count, dropping ANY holder releases the whole
/// charge (exactly once): the charge ends at the first of "fully written" and "terminal".
#[derive(Debug)]
pub struct Charge(Arc<ChargeInner>);

impl Charge {
    /// The body as `hyper` will send it: one `Bytes` (no copy of `body`) whose owner holds a second
    /// holder of this charge, so the charge is released when `hyper` drops the last reference to the
    /// body's bytes — after the last byte was written (see the module docs).
    pub fn wrap_body(&self, body: Vec<u8>) -> Bytes {
        Bytes::from_owner(ChargedBody {
            bytes: body,
            _charge: Charge(Arc::clone(&self.0)),
        })
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A body's bytes plus a holder of its charge: the `Bytes::from_owner` owner.
struct ChargedBody {
    bytes: Vec<u8>,
    _charge: Charge,
}

impl AsRef<[u8]> for ChargedBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budgets(up: u64, daemon: u64) -> Budgets {
        let mut upstreams = HashMap::new();
        upstreams.insert("a".to_string(), Account::new(up));
        upstreams.insert("b".to_string(), Account::new(up));
        Budgets {
            daemon: Account::new(daemon),
            upstreams,
        }
    }

    #[test]
    fn a_charge_fits_both_accounts_or_takes_nothing() {
        let b = budgets(10, 15);
        let c1 = b.charge("a", 6).unwrap().unwrap();
        assert!(b.charge("a", 5).is_err(), "over the upstream account");
        assert_eq!(b.in_use("a"), 6, "a refused charge takes nothing");
        let c2 = b.charge("b", 9).unwrap().unwrap();
        assert!(
            b.charge("b", 1).is_err(),
            "over the DAEMON account (6 + 9 = 15)"
        );
        assert_eq!(
            (b.in_use("b"), b.daemon_in_use()),
            (9, 15),
            "the upstream half of a daemon refusal is rolled back"
        );
        drop((c1, c2));
        assert_eq!((b.in_use("a"), b.in_use("b"), b.daemon_in_use()), (0, 0, 0));
        assert!(
            b.charge("a", 0).unwrap().is_none(),
            "an empty body charges nothing"
        );
        assert!(
            b.charge("nope", 1).is_err(),
            "an unknown upstream is refused"
        );
    }

    /// The body holder and the exchange holder each release the whole charge, exactly once, at the
    /// first drop — whichever it is.
    #[test]
    fn the_first_holder_dropped_releases_exactly_once() {
        let b = budgets(100, 100);
        // The body's bytes dropped first ("fully written").
        let c = b.charge("a", 40).unwrap().unwrap();
        let body = c.wrap_body(vec![7; 40]);
        let slice = body.slice(10..20);
        drop(body);
        assert_eq!(b.in_use("a"), 40, "a live slice still holds the bytes");
        drop(slice);
        assert_eq!(
            b.in_use("a"),
            0,
            "released when the last reference to the bytes drops"
        );
        drop(c);
        assert_eq!(b.daemon_in_use(), 0, "and never twice");

        // The terminal first: the exchange's holder.
        let c = b.charge("a", 40).unwrap().unwrap();
        let body = c.wrap_body(vec![7; 40]);
        drop(c);
        assert_eq!(b.in_use("a"), 0);
        assert_eq!(&body[..3], &[7, 7, 7], "the bytes themselves are untouched");
        drop(body);
        assert_eq!((b.in_use("a"), b.daemon_in_use()), (0, 0));
    }

    #[test]
    fn concurrent_charges_never_over_commit() {
        let b = Arc::new(budgets(1_000, 1_000));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&b);
                std::thread::spawn(move || {
                    let mut held = Vec::new();
                    for _ in 0..1_000 {
                        if let Ok(Some(c)) = b.charge("a", 7) {
                            assert!(b.daemon_in_use() <= 1_000);
                            held.push(c);
                        }
                        if held.len() > 20 {
                            held.clear();
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!((b.in_use("a"), b.daemon_in_use()), (0, 0));
    }
}
