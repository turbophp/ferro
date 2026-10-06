//! The per-request refusals a QUEUE verb makes BEFORE any checkout (SPEC §24.4 "Refusals … are
//! declared before any checkout"), and the `sql` kind's frame arithmetic.
//!
//! Each refusal here is a known non-execution: nothing was sent, so an open transaction is unaffected
//! and the caller fixes the call (§24.6). `ferrod` answers a [`Refusal`] as `Unsupported`, except an
//! undecodable handle, which is `InvalidHandle` ([`crate::sql::Undecodable`], §24.3 prerequisite (c)).
//! The codec has already enforced every SHAPE bound (handle lengths, job and queue counts); what is
//! checked here is what a value MEANS to a store.
//!
//! **A queue name is 1 to 255 characters and contains no U+0000** (G1a's decision, SPEC §24.3
//! amendment): 255 is the stock `string('queue')` column's width, so a longer name could not be
//! stored, and the bound is what lets the RESERVE reply envelope be computed ([`sql_reserve_envelope`]).
//! Characters, not bytes, because the column counts characters on both families.

use crate::config::StoreConfig;
use ferro_proto::messages::{EnqueueRequest, ReserveRequest};

/// The longest queue name, in characters (the stock `varchar(255)`).
pub const QUEUE_NAME_MAX_CHARS: usize = 255;

/// A pre-checkout refusal. Its `Display` is the terminal's message: it names the rule and the field,
/// never a payload, a dedup key or a token (§24.9's redaction carries over to error text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// A queue name that is empty, longer than [`QUEUE_NAME_MAX_CHARS`], or contains U+0000.
    QueueName,
    /// A payload containing U+0000 — refused on EVERY dialect, because PostgreSQL's `text` refuses it
    /// (22021) and refusing everywhere keeps a table portable across families (§24.4).
    PayloadNul,
    /// A payload larger than the store's `MAX_PAYLOAD_BYTES`.
    PayloadTooLarge { max: u32 },
    /// A `dedup_key` on a batch of more than one job (§24.4).
    DedupOnBatch,
    /// A `dedup_key` on a store without a dedup table (§24.6), or — until slice G4 builds dedup — on
    /// any store.
    DedupUnavailable { configured: bool },
    /// `liveness: true`: liveness release is not in v1 (§24.15); every v1 client sends `false`.
    Liveness,
    /// `max_jobs = 0`.
    MaxJobsZero,
    /// A non-zero `wait_ms`: waiting lands with the waker, at slice G3 (§24.14).
    WaitNotYet,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::QueueName => write!(
                f,
                "a queue name must be 1 to {QUEUE_NAME_MAX_CHARS} characters without U+0000"
            ),
            Refusal::PayloadNul => f.write_str("a job payload must not contain U+0000"),
            Refusal::PayloadTooLarge { max } => write!(
                f,
                "a job payload is larger than this store's MAX_PAYLOAD_BYTES ({max})"
            ),
            Refusal::DedupOnBatch => {
                f.write_str("a dedup_key is accepted only on a single-job ENQUEUE")
            }
            Refusal::DedupUnavailable { configured: false } => {
                f.write_str("this store has no DEDUP_TABLE, so a dedup_key is refused")
            }
            Refusal::DedupUnavailable { configured: true } => f.write_str(
                "dedup-keyed ENQUEUE is not served before slice G4 (SPEC §24.14), so a dedup_key is \
                 refused",
            ),
            Refusal::Liveness => f.write_str(
                "liveness release is not in v1 (SPEC §24.15); RESERVE must send liveness = false",
            ),
            Refusal::MaxJobsZero => f.write_str("RESERVE max_jobs must be at least 1"),
            Refusal::WaitNotYet => f.write_str(
                "a waiting RESERVE (wait_ms > 0) is not served before slice G3 (SPEC §24.14); send \
                 wait_ms = 0",
            ),
        }
    }
}

/// SPEC §24.3/§24.4: a queue name is 1..=255 characters, without U+0000.
pub fn queue_name(q: &str) -> Result<(), Refusal> {
    let chars = q.chars().count();
    if chars == 0 || chars > QUEUE_NAME_MAX_CHARS || q.contains('\0') {
        return Err(Refusal::QueueName);
    }
    Ok(())
}

/// SPEC §24.4: U+0000 is refused on every dialect, and so is a payload over the store's bound. (A
/// non-UTF-8 payload never reaches here: the payload is a `str` on the wire, which the codec refuses
/// unless it is UTF-8.)
pub fn payload(p: &str, max_bytes: u32) -> Result<(), Refusal> {
    if p.len() > max_bytes as usize {
        return Err(Refusal::PayloadTooLarge { max: max_bytes });
    }
    if p.contains('\0') {
        return Err(Refusal::PayloadNul);
    }
    Ok(())
}

/// Every ENQUEUE refusal, in the order a caller would want to fix them.
pub fn enqueue(req: &EnqueueRequest, store: &StoreConfig) -> Result<(), Refusal> {
    for job in &req.jobs {
        queue_name(&job.queue)?;
        payload(&job.payload, store.max_payload_bytes)?;
    }
    if req.dedup_key.is_some() {
        if req.jobs.len() > 1 {
            return Err(Refusal::DedupOnBatch);
        }
        // Dedup is G4's (§24.14). A configured DEDUP_TABLE is accepted at load and validated, but a
        // dedup-keyed ENQUEUE is refused until the statements that honour it exist — never silently
        // enqueued without the guarantee it asked for.
        return Err(Refusal::DedupUnavailable {
            configured: store.dedup_table.is_some(),
        });
    }
    Ok(())
}

/// Every RESERVE refusal that does not need the transaction registry (a tx-scoped RESERVE is refused
/// by `ferrod`, which owns `tx_id`).
pub fn reserve(req: &ReserveRequest) -> Result<(), Refusal> {
    for q in &req.queues {
        queue_name(q)?;
    }
    if req.liveness {
        return Err(Refusal::Liveness);
    }
    if req.max_jobs == 0 {
        return Err(Refusal::MaxJobsZero);
    }
    if req.wait_ms > 0 {
        return Err(Refusal::WaitNotYet);
    }
    Ok(())
}

/// An upper bound on the bytes a ONE-job `RESERVE` terminal payload needs besides the job's payload
/// bytes, for the `sql` kind. `MAX_PAYLOAD_BYTES` may not exceed `max_frame_payload` minus this
/// (§24.3), so a reserved job of the largest admitted payload always fits one frame. Itemised so a
/// reviewer can check each term against the codec (`messages::queue`), and pinned against the REAL
/// encoder by the test below rather than trusted:
pub const fn sql_reserve_envelope() -> usize {
    let outcome = 2; // fixarray(2) + status
    let response = 1 + 5; // fixarray(2) + the jobs array header (array32 at worst)
    let job = 1; // fixarray(7)
    let job_id = 2 + crate::sql::JobId::MAX_ENCODED_LEN; // bin8 + 20 bytes
    let token = 2 + crate::sql::Token::ENCODED_LEN; // bin8 + 8 bytes
    let attempts = 5; // uint32 at worst
    let queue = 3 + QUEUE_NAME_MAX_CHARS * 4; // str16 + 255 four-byte characters
    let payload_header = 5; // str32 at worst
    let times = 9 + 9; // two int64 at worst
    let stats = 1 + 9 + 9; // fixarray(2) + two uint64 at worst
    outcome + response + job + job_id + token + attempts + queue + payload_header + times + stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::tests::store;
    use ferro_proto::consts::MAX_FRAME_PAYLOAD;
    use ferro_proto::messages::{
        EnqueueJob, Outcome, QueueCommon, QueueStats, ReserveResponse, ReservedJob,
    };

    #[test]
    fn queue_names_are_bounded_in_characters_and_refuse_nul() {
        assert_eq!(queue_name("default"), Ok(()));
        assert_eq!(
            queue_name(&"é".repeat(255)),
            Ok(()),
            "255 characters, 510 bytes"
        );
        assert_eq!(queue_name(""), Err(Refusal::QueueName));
        assert_eq!(queue_name(&"q".repeat(256)), Err(Refusal::QueueName));
        assert_eq!(queue_name("a\0b"), Err(Refusal::QueueName));
    }

    #[test]
    fn a_payload_is_refused_for_nul_and_for_size_and_accepted_at_the_bound() {
        assert_eq!(payload("{}", 2), Ok(()), "exactly the bound");
        assert_eq!(payload("{} ", 2), Err(Refusal::PayloadTooLarge { max: 2 }));
        assert_eq!(payload("a\0", 10), Err(Refusal::PayloadNul));
        assert_eq!(payload("", 1), Ok(()));
        // U+0000 is refused anywhere, not only at the start.
        assert_eq!(payload("\0", 10), Err(Refusal::PayloadNul));
    }

    fn enq(jobs: usize, dedup: Option<&str>, payload: &str) -> EnqueueRequest {
        EnqueueRequest {
            store: "jobs".into(),
            jobs: (0..jobs)
                .map(|_| EnqueueJob {
                    queue: "default".into(),
                    payload: payload.into(),
                    delay_s: 0,
                })
                .collect(),
            dedup_key: dedup.map(str::to_string),
            common: QueueCommon::default(),
        }
    }

    #[test]
    fn enqueue_refusals() {
        let s = store();
        assert_eq!(enqueue(&enq(2, None, "{}"), &s), Ok(()));
        assert_eq!(
            enqueue(&enq(2, Some("k"), "{}"), &s),
            Err(Refusal::DedupOnBatch)
        );
        assert_eq!(
            enqueue(&enq(1, Some("k"), "{}"), &s),
            Err(Refusal::DedupUnavailable { configured: false })
        );
        let mut with_table = store();
        with_table.dedup_table = Some(crate::ident::TableName::parse("ferro_dedup").unwrap());
        assert_eq!(
            enqueue(&enq(1, Some("k"), "{}"), &with_table),
            Err(Refusal::DedupUnavailable { configured: true })
        );
        assert_eq!(enqueue(&enq(1, None, "\0"), &s), Err(Refusal::PayloadNul));
        let mut bad_queue = enq(1, None, "{}");
        bad_queue.jobs[0].queue = String::new();
        assert_eq!(enqueue(&bad_queue, &s), Err(Refusal::QueueName));
        let mut small = store();
        small.max_payload_bytes = 1;
        assert_eq!(
            enqueue(&enq(1, None, "{}"), &small),
            Err(Refusal::PayloadTooLarge { max: 1 })
        );
    }

    #[test]
    fn reserve_refusals() {
        let ok = ReserveRequest {
            store: "jobs".into(),
            queues: vec!["high".into(), "default".into()],
            max_jobs: 1,
            wait_ms: 0,
            liveness: false,
            common: QueueCommon::default(),
        };
        assert_eq!(reserve(&ok), Ok(()));
        let mut r = ok.clone();
        r.liveness = true;
        assert_eq!(reserve(&r), Err(Refusal::Liveness));
        let mut r = ok.clone();
        r.max_jobs = 0;
        assert_eq!(reserve(&r), Err(Refusal::MaxJobsZero));
        let mut r = ok.clone();
        r.wait_ms = 1;
        assert_eq!(reserve(&r), Err(Refusal::WaitNotYet));
        let mut r = ok;
        r.queues[1] = "\0".into();
        assert_eq!(reserve(&r), Err(Refusal::QueueName));
    }

    /// The envelope is a real bound: a one-job reply carrying the worst case of every other field and
    /// a payload of `MAX_FRAME_PAYLOAD - envelope` bytes encodes within one frame — and the overhead
    /// the encoder actually spent is no more than the envelope says.
    #[test]
    fn the_reserve_envelope_bounds_the_real_encoder() {
        let envelope = sql_reserve_envelope();
        let payload_len = MAX_FRAME_PAYLOAD as usize - envelope;
        let reply = ReserveResponse {
            jobs: vec![ReservedJob {
                job_id: crate::sql::JobId(i64::MIN).encode(),
                token: crate::sql::Token::from_pg(-1, -1).encode().to_vec(),
                attempts: u32::MAX,
                queue: "😀".repeat(QUEUE_NAME_MAX_CHARS),
                payload: "p".repeat(payload_len),
                created_at: i64::MIN,
                lease_deadline: i64::MIN,
            }],
            stats: QueueStats {
                queue_us: (1 << 63) - 1,
                exec_us: (1 << 63) - 1,
            },
        };
        let frame_payload = Outcome::Ok(reply.encode()).encode();
        assert!(
            frame_payload.len() <= MAX_FRAME_PAYLOAD as usize,
            "{} > {MAX_FRAME_PAYLOAD}",
            frame_payload.len()
        );
        assert!(frame_payload.len() - payload_len <= envelope);
    }
}
