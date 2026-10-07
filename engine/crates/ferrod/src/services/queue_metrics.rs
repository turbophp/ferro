//! Ferro Queue's SPEC §24.9 metrics (M7-G3): one [`QueueMetrics`] per enabled store, rendered into the
//! §13 Prometheus exposition by [`render`].
//!
//! **Redaction carries over (§24.9).** No series carries a payload, a dedup key, a token or a job id.
//! A queue NAME is a label only when the operator listed it in `LABELLED_QUEUES`; every other queue is
//! `queue="_other"`. The label set is therefore CLOSED and fixed at configuration, so every series is
//! pre-allocated and exported from the first scrape — zeroes included, for the reason the rest of
//! §13 does it: "no data" and "nothing happened" must not be the same observation to an alert.
//!
//! **What G3 exports, and what it does not (SPEC §22.2 (dl)).** Exported: `ops_total`,
//! `enqueued_total`, `unreserved_total`, `unreserve_failed_total`, `reserve_unconfirmed_total`,
//! `polls_total`, the `waiters` gauge, the enqueue/ack/wait-duration histograms, and the wake hints by
//! source. NOT exported yet, each named rather than exported as a fake zero: `redeliveries_total` and
//! `pickup_latency_seconds` (both need the RESERVE statement to report the row's previous
//! `reserved_at`/`available_at`, a change to the statement §24.4 writes verbatim), and the sampled
//! depth gauges (`depth`, `oldest_pending_age_seconds`, `stuck_jobs`), which need the per-store
//! sampler statement. All three are carried to the ledger as G3's follow-up.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use ferro_pool::histogram::{HistogramSnapshot, fmt_seconds};

/// The seven verbs, in `[methods.queue]` order — `ops_total`'s `op` label.
pub const OPS: [&str; 7] = [
    "enqueue", "reserve", "ack", "release", "extend", "size", "clear",
];

/// `ops_total`'s `outcome` label (§24.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok = 0,
    Empty = 1,
    LeaseLost = 2,
    Gone = 3,
    Deduplicated = 4,
    Error = 5,
}

pub const OUTCOMES: [&str; 6] = ["ok", "empty", "lease_lost", "gone", "deduplicated", "error"];

/// Why an undelivered reservation was unreserved (§24.9 `unreserved_total{cause}`). `teardown` and
/// `deadline` are §24.9's; `cancel` is M7-G3's (SPEC §22.2 (dl)): a client CANCEL of a waiting RESERVE
/// whose sweep was already in flight is answered `Cancelled` at once (§24.4: "no reservation statement
/// has completed for the request"), and the jobs that sweep then brings back for it are unreserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnreserveCause {
    Teardown = 0,
    Deadline = 1,
    Cancel = 2,
}

pub const UNRESERVE_CAUSES: [&str; 3] = ["teardown", "deadline", "cancel"];

/// What triggered a waker sweep (§24.9 `polls_total{trigger}`). `hint` and `interval` are §24.9's;
/// `arrival` (a new waiter's first sweep of each of its queues, §24.8 "register, then sweep") and
/// `refill` (a sweep that came back FULL is followed by one for the waiters it could not cover) are
/// M7-G3's (SPEC §22.2 (dl)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Hint = 0,
    Interval = 1,
    Arrival = 2,
    Refill = 3,
}

pub const TRIGGERS: [&str; 4] = ["hint", "interval", "arrival", "refill"];

/// Where a wake hint came from (§24.8 trigger 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintSource {
    /// An autocommit ENQUEUE, or an autocommit RELEASE with `delay_s = 0`.
    Autocommit = 0,
    /// A tx-scoped verb's hint, fired at its transaction's successful COMMIT (§24.5 step 4).
    AfterCommit = 1,
    /// An unreserve that restored a job on this engine.
    Unreserve = 2,
}

pub const HINT_SOURCES: [&str; 3] = ["autocommit", "after_commit", "unreserve"];

/// Bounds for the queue verbs' DURATIONS (enqueue, ack): queue + exec time, as a statement's.
pub const VERB_BOUNDS_US: [u64; 14] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000,
    5_000_000, 30_000_000,
];

/// Bounds for `wait_duration_seconds`: how long a RESERVE stayed parked, up to the 30 s default
/// `MAX_WAIT_MS` and its grace.
pub const WAIT_BOUNDS_US: [u64; 12] = [
    1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000,
    31_000_000, 120_000_000,
];

type Hist14 = ferro_pool::histogram::Histogram<14>;
type Hist12 = ferro_pool::histogram::Histogram<12>;

/// One store's counters.
#[derive(Debug)]
pub struct QueueMetrics {
    store: String,
    /// The queue label vocabulary: the operator's `LABELLED_QUEUES`, then `_other`.
    labels: Vec<String>,
    /// `[label][op][outcome]`.
    ops: Vec<AtomicU64>,
    /// `[label][mode]`, mode 0 = autocommit, 1 = tx.
    enqueued: Vec<AtomicU64>,
    unreserved: [AtomicU64; 3],
    unreserve_failed: AtomicU64,
    reserve_unconfirmed: AtomicU64,
    polls: [AtomicU64; 4],
    hints: [AtomicU64; 3],
    waiters: AtomicU64,
    enqueue_duration: Hist14,
    ack_duration: Hist14,
    wait_duration: Hist12,
}

impl QueueMetrics {
    pub fn new(store: &str, labelled_queues: &[String]) -> QueueMetrics {
        let mut labels: Vec<String> = labelled_queues.to_vec();
        labels.push("_other".to_string());
        let n = labels.len();
        QueueMetrics {
            store: store.to_string(),
            ops: (0..n * OPS.len() * OUTCOMES.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            enqueued: (0..n * 2).map(|_| AtomicU64::new(0)).collect(),
            labels,
            unreserved: [const { AtomicU64::new(0) }; 3],
            unreserve_failed: AtomicU64::new(0),
            reserve_unconfirmed: AtomicU64::new(0),
            polls: [const { AtomicU64::new(0) }; 4],
            hints: [const { AtomicU64::new(0) }; 3],
            waiters: AtomicU64::new(0),
            enqueue_duration: Hist14::new(VERB_BOUNDS_US),
            ack_duration: Hist14::new(VERB_BOUNDS_US),
            wait_duration: Hist12::new(WAIT_BOUNDS_US),
        }
    }

    /// The label index of `queue`: its own when the operator listed it, `_other` otherwise.
    pub fn label_of(&self, queue: Option<&str>) -> usize {
        let other = self.labels.len() - 1;
        match queue {
            Some(q) => self.labels[..other]
                .iter()
                .position(|l| l == q)
                .unwrap_or(other),
            None => other,
        }
    }

    /// The label TEXT for index `i` (for spans and the slow log, which use the same vocabulary).
    pub fn label(&self, i: usize) -> &str {
        &self.labels[i.min(self.labels.len() - 1)]
    }

    pub fn op(&self, label: usize, op: usize, outcome: Outcome) {
        let i = (label * OPS.len() + op) * OUTCOMES.len() + outcome as usize;
        if let Some(c) = self.ops.get(i) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn ops_count(&self, label: usize, op: usize, outcome: Outcome) -> u64 {
        let i = (label * OPS.len() + op) * OUTCOMES.len() + outcome as usize;
        self.ops.get(i).map_or(0, |c| c.load(Ordering::Relaxed))
    }

    pub fn enqueued(&self, label: usize, in_tx: bool, jobs: u64) {
        if let Some(c) = self.enqueued.get(label * 2 + usize::from(in_tx)) {
            c.fetch_add(jobs, Ordering::Relaxed);
        }
    }

    pub fn unreserved(&self, cause: UnreserveCause, n: u64) {
        self.unreserved[cause as usize].fetch_add(n, Ordering::Relaxed);
    }

    pub fn unreserved_count(&self, cause: UnreserveCause) -> u64 {
        self.unreserved[cause as usize].load(Ordering::Relaxed)
    }

    pub fn unreserve_failed(&self, n: u64) {
        self.unreserve_failed.fetch_add(n, Ordering::Relaxed);
    }

    pub fn unreserve_failed_count(&self) -> u64 {
        self.unreserve_failed.load(Ordering::Relaxed)
    }

    pub fn reserve_unconfirmed(&self) {
        self.reserve_unconfirmed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reserve_unconfirmed_count(&self) -> u64 {
        self.reserve_unconfirmed.load(Ordering::Relaxed)
    }

    pub fn poll(&self, trigger: Trigger) {
        self.polls[trigger as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn polls(&self, trigger: Trigger) -> u64 {
        self.polls[trigger as usize].load(Ordering::Relaxed)
    }

    /// Every sweep, whatever triggered it — the checkouts parked waiters cost (§24.8's cost bound).
    pub fn polls_total(&self) -> u64 {
        self.polls.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn hint(&self, source: HintSource) {
        self.hints[source as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn hints(&self, source: HintSource) -> u64 {
        self.hints[source as usize].load(Ordering::Relaxed)
    }

    pub fn set_waiters(&self, n: usize) {
        self.waiters.store(n as u64, Ordering::Relaxed);
    }

    pub fn waiters(&self) -> u64 {
        self.waiters.load(Ordering::Relaxed)
    }

    pub fn observe_enqueue_us(&self, us: u64) {
        self.enqueue_duration.observe_us(us);
    }

    pub fn observe_ack_us(&self, us: u64) {
        self.ack_duration.observe_us(us);
    }

    pub fn observe_wait_us(&self, us: u64) {
        self.wait_duration.observe_us(us);
    }

    pub fn wait_histogram(&self) -> HistogramSnapshot {
        self.wait_duration.snapshot()
    }
}

/// Escape a label value (backslash, quote, newline), as `metrics.rs` does for a pool name: a store
/// name is operator-supplied and closed, a queue label likewise.
fn esc(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// Append every Ferro Queue family for `stores` (sorted by name) to `out`. Every family is written
/// even with no stores, so its HELP/TYPE lines are stable.
pub fn render(out: &mut String, stores: &[&QueueMetrics]) {
    let fam = |out: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    };
    fam(
        out,
        "ferro_queue_ops_total",
        "counter",
        "Ferro Queue verbs answered, by store, labelled queue, verb and outcome (SPEC §24.9).",
    );
    for m in stores {
        let s = esc(&m.store);
        for (li, label) in m.labels.iter().enumerate() {
            let q = esc(label);
            for (oi, op) in OPS.iter().enumerate() {
                for (ui, outcome) in OUTCOMES.iter().enumerate() {
                    let v = m.ops[(li * OPS.len() + oi) * OUTCOMES.len() + ui].load(Ordering::Relaxed);
                    let _ = writeln!(
                        out,
                        "ferro_queue_ops_total{{store=\"{s}\",queue=\"{q}\",op=\"{op}\",outcome=\"{outcome}\"}} {v}"
                    );
                }
            }
        }
    }
    fam(
        out,
        "ferro_queue_enqueued_total",
        "counter",
        "Jobs inserted by ENQUEUE (SPEC §24.9). mode=\"tx\" counts jobs its STATEMENTS inserted inside a client transaction, not committed jobs.",
    );
    for m in stores {
        let s = esc(&m.store);
        for (li, label) in m.labels.iter().enumerate() {
            for (mi, mode) in ["autocommit", "tx"].iter().enumerate() {
                let v = m.enqueued[li * 2 + mi].load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "ferro_queue_enqueued_total{{store=\"{s}\",queue=\"{}\",mode=\"{mode}\"}} {v}",
                    esc(label)
                );
            }
        }
    }
    fam(
        out,
        "ferro_queue_unreserved_total",
        "counter",
        "Undelivered reservations restored by the engine's fenced unreserve, by cause (SPEC §24.8).",
    );
    for m in stores {
        for (ci, cause) in UNRESERVE_CAUSES.iter().enumerate() {
            let _ = writeln!(
                out,
                "ferro_queue_unreserved_total{{store=\"{}\",cause=\"{cause}\"}} {}",
                esc(&m.store),
                m.unreserved[ci].load(Ordering::Relaxed)
            );
        }
    }
    for (name, help, read) in [
        (
            "ferro_queue_unreserve_failed_total",
            "Jobs whose unreserve statement failed; each waits for its lease_deadline, one attempt counted (SPEC §24.8).",
            (|m: &QueueMetrics| m.unreserve_failed_count()) as fn(&QueueMetrics) -> u64,
        ),
        (
            "ferro_queue_reserve_unconfirmed_total",
            "RESERVE statements whose fate was Indeterminate; any lease taken is a stock-equivalent phantom attempt (SPEC §24.8).",
            |m: &QueueMetrics| m.reserve_unconfirmed_count(),
        ),
    ] {
        fam(out, name, "counter", help);
        for m in stores {
            let _ = writeln!(out, "{name}{{store=\"{}\"}} {}", esc(&m.store), read(m));
        }
    }
    fam(
        out,
        "ferro_queue_polls_total",
        "counter",
        "Waker sweeps — each one checkout and one RESERVE statement — by trigger (SPEC §24.8, §24.9).",
    );
    for m in stores {
        for (ti, t) in TRIGGERS.iter().enumerate() {
            let _ = writeln!(
                out,
                "ferro_queue_polls_total{{store=\"{}\",trigger=\"{t}\"}} {}",
                esc(&m.store),
                m.polls[ti].load(Ordering::Relaxed)
            );
        }
    }
    fam(
        out,
        "ferro_queue_wake_hints_total",
        "counter",
        "Local wake hints fired, by source (SPEC §24.8 trigger 1).",
    );
    for m in stores {
        for (hi, src) in HINT_SOURCES.iter().enumerate() {
            let _ = writeln!(
                out,
                "ferro_queue_wake_hints_total{{store=\"{}\",source=\"{src}\"}} {}",
                esc(&m.store),
                m.hints[hi].load(Ordering::Relaxed)
            );
        }
    }
    fam(
        out,
        "ferro_queue_waiters",
        "gauge",
        "RESERVEs currently waiting on the store (SPEC §24.9). A waiter holds no connection, pin or permit (§24.8).",
    );
    for m in stores {
        let _ = writeln!(
            out,
            "ferro_queue_waiters{{store=\"{}\"}} {}",
            esc(&m.store),
            m.waiters()
        );
    }
    for (name, help, read) in [
        (
            "ferro_queue_enqueue_duration_seconds",
            "ENQUEUE queue + exec time (SPEC §24.9).",
            (|m: &QueueMetrics| m.enqueue_duration.snapshot()) as fn(&QueueMetrics) -> HistogramSnapshot,
        ),
        (
            "ferro_queue_ack_duration_seconds",
            "ACK queue + exec time (SPEC §24.9).",
            |m: &QueueMetrics| m.ack_duration.snapshot(),
        ),
        (
            "ferro_queue_wait_duration_seconds",
            "How long a waiting RESERVE stayed parked, receipt to terminal (SPEC §24.9).",
            |m: &QueueMetrics| m.wait_duration.snapshot(),
        ),
    ] {
        fam(out, name, "histogram", help);
        for m in stores {
            let s = esc(&m.store);
            let snap = read(m);
            for &(le, cum) in &snap.buckets {
                let le = le.map_or_else(|| "+Inf".to_string(), fmt_seconds);
                let _ = writeln!(out, "{name}_bucket{{store=\"{s}\",le=\"{le}\"}} {cum}");
            }
            let _ = writeln!(out, "{name}_sum{{store=\"{s}\"}} {}", fmt_seconds(snap.sum_us));
            let _ = writeln!(out, "{name}_count{{store=\"{s}\"}} {}", snap.count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unlisted_queue_is_other_and_a_listed_one_is_its_own_label() {
        let m = QueueMetrics::new("jobs", &["default".into(), "emails".into()]);
        assert_eq!(m.label_of(Some("default")), 0);
        assert_eq!(m.label_of(Some("emails")), 1);
        assert_eq!(m.label_of(Some("secret-tenant-42")), 2);
        assert_eq!(m.label(2), "_other");
        assert_eq!(m.label_of(None), 2);
        // `_other` is a label, not a queue the operator can list into a different series.
        let m = QueueMetrics::new("jobs", &[]);
        assert_eq!(m.label_of(Some("_other")), 0);
    }

    /// Every series is present from the first scrape — zeroes included — and an unlisted queue's
    /// name never appears in the exposition.
    #[test]
    fn the_exposition_is_closed_and_complete() {
        let m = QueueMetrics::new("jobs", &["default".into()]);
        let l = m.label_of(Some("tenant-secret"));
        m.op(l, 1, Outcome::Empty);
        m.enqueued(l, false, 3);
        m.unreserved(UnreserveCause::Cancel, 2);
        m.poll(Trigger::Arrival);
        m.hint(HintSource::Unreserve);
        m.set_waiters(7);
        m.observe_wait_us(1_500_000);
        let mut out = String::new();
        render(&mut out, &[&m]);
        assert!(!out.contains("tenant-secret"), "{out}");
        assert!(out.contains(
            "ferro_queue_ops_total{store=\"jobs\",queue=\"_other\",op=\"reserve\",outcome=\"empty\"} 1"
        ));
        assert!(out.contains(
            "ferro_queue_ops_total{store=\"jobs\",queue=\"default\",op=\"ack\",outcome=\"lease_lost\"} 0"
        ));
        assert!(out.contains(
            "ferro_queue_enqueued_total{store=\"jobs\",queue=\"_other\",mode=\"autocommit\"} 3"
        ));
        assert!(out.contains("ferro_queue_unreserved_total{store=\"jobs\",cause=\"cancel\"} 2"));
        assert!(out.contains("ferro_queue_unreserved_total{store=\"jobs\",cause=\"teardown\"} 0"));
        assert!(out.contains("ferro_queue_polls_total{store=\"jobs\",trigger=\"arrival\"} 1"));
        assert!(out.contains("ferro_queue_wake_hints_total{store=\"jobs\",source=\"unreserve\"} 1"));
        assert!(out.contains("ferro_queue_waiters{store=\"jobs\"} 7"));
        assert!(out.contains("ferro_queue_wait_duration_seconds_bucket{store=\"jobs\",le=\"2.5\"} 1"));
        assert!(out.contains("ferro_queue_wait_duration_seconds_bucket{store=\"jobs\",le=\"1\"} 0"));
        assert!(out.contains("ferro_queue_wait_duration_seconds_sum{store=\"jobs\"} 1.5"));
        assert!(out.contains("ferro_queue_reserve_unconfirmed_total{store=\"jobs\"} 0"));
        assert_eq!(
            out.matches("ferro_queue_ops_total{").count(),
            2 * OPS.len() * OUTCOMES.len(),
            "every (label, op, outcome), zeroes included"
        );
        // A store name that could close its quotes is escaped.
        let m = QueueMetrics::new("a\"b", &[]);
        let mut out = String::new();
        render(&mut out, &[&m]);
        assert!(out.contains("store=\"a\\\"b\""), "{out}");
    }
}
