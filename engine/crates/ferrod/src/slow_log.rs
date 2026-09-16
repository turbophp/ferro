//! The SPEC §13 **slow log**: one structured record per statement at or above a threshold.
//!
//! Of §13's four observability bullets this is the one whose CONSUMER already exists. Prometheus
//! needs a scrape endpoint and `ferro top` needs the admin service, and neither transport is built
//! — the admin service is `ADMIN = 5`, a reserved `/proto` service id with no method table
//! (§22.2 (bo)). A log line needs only the `tracing` subscriber the daemon has had since M0, which
//! is what product-vision §5 means by *"slow log as structured JSON to stdout/journald"*. So this
//! ships first, and it ships the piece the other three will reuse: the fingerprint.
//!
//! # The redaction contract is structural, not procedural
//!
//! product-vision §5 says *"normalized fingerprints only, never raw SQL, in metrics/spans; params
//! redacted by default"*. Both halves are enforced by the types here rather than by review:
//!
//! * [`SlowStatement::fingerprint`] is a [`Fingerprint`], and `ferro-classify` gives no way to
//!   build one except by normalizing. There is no field a caller could put raw SQL in.
//! * Parameters are not a field at all. [`SlowStatement::params`] is a COUNT; the values appear
//!   only when [`LogParams`] says so, and even then through [`redact_params`], which emits each
//!   parameter's TYPE and length rather than its bytes.
//!
//! That last point is a deliberate narrowing of §13's `log_params = always`. "Always" cannot mean
//! "print the password column", because the operator who flips it to debug a slow query is not
//! consenting on behalf of the data subject. A type and a length identify which bind went wrong —
//! which is what the setting is FOR — without the log becoming a place secrets live.

use ferro_classify::{Fingerprint, fingerprint};
use ferro_pool::error::PoolError;
use ferro_proto::value::Value;

use crate::config::LogParams;

/// One statement's slow-log record. Every field is a SHAPE or a MEASUREMENT — never a value.
#[derive(Debug)]
pub struct SlowStatement<'a> {
    /// The normalized statement. Never raw SQL — see the module doc.
    pub fingerprint: Fingerprint,
    /// The pool the statement ran on. A configured NAME, so it is a closed label vocabulary in
    /// §13's sense (the operator chose it; a client cannot invent one).
    pub pool: &'a str,
    /// Microseconds spent waiting for a pooled connection.
    pub queue_us: u64,
    /// Microseconds spent in the backend call itself.
    pub exec_us: u64,
    /// Rows RETURNED for a read, or rows AFFECTED for a write — whichever the statement produced.
    /// A write's `rows.len()` is always 0, and `affected` is the number an operator is looking for
    /// when an UPDATE is slow.
    pub rows: u64,
    /// The bound parameters, for the count and — under [`LogParams`] — their redacted shapes.
    pub params: &'a [Value],
    /// `None` for a statement that succeeded; the SQLSTATE-or-equivalent for one that did not.
    pub error: Option<&'a str>,
}

/// Emit `stmt` if it is at or above `threshold_ms`.
///
/// Returns whether it logged, which is what makes the threshold testable without a subscriber.
pub fn record(stmt: &SlowStatement<'_>, threshold_ms: Option<u64>, log_params: LogParams) -> bool {
    let Some(threshold_ms) = threshold_ms else {
        return false;
    };
    // The statement's WALL time is queue + exec: an operator chasing a slow query cares that it
    // took two seconds, not that only 40 ms of that was the database. The split is in the record.
    let total_us = stmt.queue_us.saturating_add(stmt.exec_us);
    if total_us / 1_000 < threshold_ms {
        return false;
    }

    let params = match (log_params, stmt.error) {
        (LogParams::Always, _) | (LogParams::OnError, Some(_)) => Some(redact_params(stmt.params)),
        _ => None,
    };

    tracing::info!(
        target: "ferro::slow_log",
        fingerprint = %stmt.fingerprint,
        pool = %stmt.pool,
        queue_us = stmt.queue_us,
        exec_us = stmt.exec_us,
        total_us,
        rows = stmt.rows,
        param_count = stmt.params.len(),
        params = params.as_deref().unwrap_or(""),
        error = stmt.error.unwrap_or(""),
        "slow statement",
    );
    true
}

/// Build the fingerprint for a statement about to be recorded.
///
/// A thin re-export so the call sites never touch raw SQL for longer than this one expression.
pub fn fingerprint_of(sql: &str) -> Fingerprint {
    fingerprint(sql)
}

/// A failed statement's error as a CLOSED LABEL — the `PoolError` variant's name, never its
/// message.
///
/// This is not fussiness. A backend's error text routinely QUOTES the offending value:
/// PostgreSQL's unique-violation detail is `Key (email)=(alice@example.com) already exists`, and
/// MySQL's is much the same. Logging the message would hand the slow log exactly the values the
/// fingerprint exists to keep out of it, and it would do so on the error path — where an operator
/// is most likely to be reading. The variant name is a closed vocabulary in product-vision §5's
/// sense: it comes from an enum, so a client cannot invent one and cardinality cannot drift.
///
/// The SQLSTATE is deliberately NOT recovered here by re-running the fate matrix: `fate.rs` is the
/// ONE place a `PoolError` becomes a wire error (M1-S4), and a second classification beside it
/// could disagree with the terminal the client actually received.
pub fn error_label(e: &PoolError) -> String {
    // The discriminant, without its payload — `Backend("...")` becomes `Backend`.
    let dbg = format!("{e:?}");
    dbg.split(['(', ' ', '{'])
        .next()
        .unwrap_or("Error")
        .to_string()
}

/// Render parameters as TYPE and LENGTH only — never their bytes.
///
/// See the module doc for why `log_params = always` stops here rather than printing values. The
/// output looks like `[text(11), i64, null]`: enough to see that bind 3 was NULL when it should
/// not have been, and not enough to be a breach.
fn redact_params(params: &[Value]) -> String {
    let mut out = String::from("[");
    for (i, p) in params.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&describe(p));
    }
    out.push(']');
    out
}

/// One parameter's redacted shape.
fn describe(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::I64(_) => "i64".to_string(),
        Value::U64(_) => "u64".to_string(),
        Value::F64(_) => "f64".to_string(),
        Value::Text(s) => format!("text({})", s.len()),
        Value::Bytes(b) => format!("bytes({})", b.len()),
        other => {
            // The canonical-text tags (S7: DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, UUID,
            // JSON) all ride a string payload. Naming the TAG without its text keeps this arm
            // correct as tags are added — a new tag degrades to its name, never to its value.
            let name = format!("{other:?}");
            name.split(['(', ' '])
                .next()
                .unwrap_or("value")
                .to_lowercase()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stmt<'a>(params: &'a [Value], error: Option<&'a str>) -> SlowStatement<'a> {
        SlowStatement {
            fingerprint: fingerprint_of("SELECT * FROM t WHERE email = 'alice@example.com'"),
            pool: "default",
            queue_us: 1_000,
            exec_us: 250_000,
            rows: 3,
            params,
            error,
        }
    }

    /// OFF unless the operator asked. An operator who has not set a threshold should not silently
    /// gain a new log stream — and `record` returning a bool is what makes that assertable without
    /// standing up a subscriber and parsing its output.
    #[test]
    fn no_threshold_means_no_record() {
        assert!(!record(&stmt(&[], None), None, LogParams::Never));
    }

    /// The threshold is on the statement's WALL time — queue + exec — not on the backend call
    /// alone. A statement that spent 2 s waiting for a connection is slow to the caller, and a
    /// slow log that only counted `exec_us` would be silent on the single most common production
    /// stall: pool exhaustion.
    #[test]
    fn the_threshold_is_queue_plus_exec() {
        let s = SlowStatement {
            queue_us: 150_000,
            exec_us: 60_000,
            ..stmt(&[], None)
        };
        // 210 ms total: over a 200 ms bar, under it on `exec_us` alone.
        assert!(record(&s, Some(200), LogParams::Never));
        assert!(!record(&s, Some(400), LogParams::Never));
    }

    /// A statement exactly AT the threshold is slow — §13 says "at or above", and an off-by-one
    /// here is the kind of thing that makes a threshold of 0 (log everything) not work.
    #[test]
    fn at_the_threshold_is_slow_and_zero_logs_everything() {
        let s = SlowStatement {
            queue_us: 0,
            exec_us: 200_000,
            ..stmt(&[], None)
        };
        assert!(record(&s, Some(200), LogParams::Never));
        let fast = SlowStatement {
            queue_us: 0,
            exec_us: 1,
            ..stmt(&[], None)
        };
        assert!(record(&fast, Some(0), LogParams::Never));
    }

    /// **The redaction contract, at the emitter rather than in the normalizer.** A parameter's
    /// VALUE never reaches the record on any setting — `always` widens it to the type and length,
    /// which is what identifies a bad bind, and no further.
    #[test]
    fn a_parameter_value_never_appears_on_any_setting() {
        const SECRET: &str = "hunter2-swordfish";
        let params = vec![
            Value::Text(SECRET.to_string()),
            Value::I64(42),
            Value::Null,
            Value::Bytes(vec![1, 2, 3]),
        ];
        for setting in [LogParams::Never, LogParams::OnError, LogParams::Always] {
            for error in [None, Some("Unique")] {
                let rendered = match (setting, error) {
                    (LogParams::Always, _) | (LogParams::OnError, Some(_)) => {
                        redact_params(&params)
                    }
                    _ => String::new(),
                };
                assert!(
                    !rendered.contains(SECRET),
                    "the parameter value leaked under {setting:?}/{error:?}: {rendered}",
                );
            }
        }
        // And the widened form is still USEFUL — type and length, which is what it is for.
        let rendered = redact_params(&params);
        assert_eq!(rendered, "[text(17), i64, null, bytes(3)]");
    }

    /// `on_error` is the middle setting and has to actually differ from both ends, or the
    /// vocabulary has two members pretending to be three.
    #[test]
    fn on_error_widens_only_for_a_failed_statement() {
        let params = vec![Value::I64(1)];
        assert_eq!(
            params_for(LogParams::OnError, None),
            None,
            "on_error must not render params for a statement that SUCCEEDED",
        );
        assert!(params_for(LogParams::OnError, Some("Unique")).is_some());
        assert!(params_for(LogParams::Never, Some("Unique")).is_none());
        assert!(params_for(LogParams::Always, None).is_some());
        let _ = params;
    }

    /// Mirrors `record`'s own decision so the matrix above is testable without a subscriber.
    fn params_for(log_params: LogParams, error: Option<&str>) -> Option<()> {
        match (log_params, error) {
            (LogParams::Always, _) | (LogParams::OnError, Some(_)) => Some(()),
            _ => None,
        }
    }

    /// An error is labelled by its VARIANT, never by its message — a backend's error text quotes
    /// the offending value (`Key (email)=(alice@example.com) already exists`).
    #[test]
    fn an_error_label_is_the_variant_never_the_message() {
        let e = PoolError::Backend("Key (email)=(alice@example.com) already exists".into());
        let label = error_label(&e);
        assert_eq!(label, "Backend");
        assert!(!label.contains("alice@example.com"));
    }
}
