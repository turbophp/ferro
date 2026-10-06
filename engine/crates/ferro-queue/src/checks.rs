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
use crate::{Dialect, PoolFamily};
use ferro_proto::messages::{EnqueueRequest, ReleaseRequest, ReserveRequest};

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
    /// A `delay_s` whose `available_at = now + 1 + delay_s` would not fit the store's time column
    /// (M7-G1b, carried from the G1a review). Refused BEFORE sending: PostgreSQL would otherwise
    /// answer `22003` after the statement was sent. `max` is the largest `delay_s` the engine's clock
    /// admits now.
    DelayTooLarge { max: i64 },
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
            Refusal::DelayTooLarge { max } => write!(
                f,
                "delay_s is too large: available_at = now + 1 + delay_s would overflow the store's \
                 time column (the largest delay_s accepted now is {max}); nothing was sent"
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

/// How far the DATABASE's clock may be ahead of the engine's before [`delay`]'s pre-check stops
/// covering it: one day. The check has to run before anything is sent, so it cannot read the
/// database's `now`; it reads the engine's wall clock and keeps this margin below the column's
/// ceiling. **Residual, stated:** a database clock more than a day ahead of `ferrod`'s lets a delay
/// within a day of the ceiling through, and PostgreSQL then refuses the INSERT with `22003` — a known
/// non-execution (the statement is atomic; nothing was inserted), never a wrong or partial write.
/// **Cost, stated:** a delay landing within one day of the ceiling (2038-01-19 on PostgreSQL) is
/// refused although it would have fit.
pub const DELAY_CLOCK_MARGIN_S: i64 = 86_400;

/// The largest value the dialect's time columns hold: PostgreSQL's signed `integer` (the stock layout's
/// `unsignedInteger` becomes `integer` on PG) and MySQL's `INT UNSIGNED`.
pub fn time_column_max(dialect: Dialect) -> i64 {
    match dialect {
        Dialect::Postgres => i64::from(i32::MAX),
        Dialect::Mysql => i64::from(u32::MAX),
    }
}

/// The dialect a store's statements are composed in.
pub fn dialect_of(store: &StoreConfig) -> Dialect {
    match store.family {
        PoolFamily::Mysql => Dialect::Mysql,
        // SQLite stores are refused at configuration; Postgres is the only other family.
        PoolFamily::Postgres | PoolFamily::Sqlite => Dialect::Postgres,
    }
}

/// SPEC §24.3's delay rule writes `available_at = now + 1 + delay_s` for `delay_s > 0`; refuse a
/// `delay_s` for which that would not fit the time column, before anything is sent, judged on the
/// engine's clock `engine_now` (Unix seconds) with [`DELAY_CLOCK_MARGIN_S`] to spare. `delay_s = 0`
/// stores `now` itself and is never refused here.
pub fn delay(delay_s: u32, engine_now: i64, dialect: Dialect) -> Result<(), Refusal> {
    if delay_s == 0 {
        return Ok(());
    }
    let max = time_column_max(dialect) - engine_now - 1 - DELAY_CLOCK_MARGIN_S;
    if i64::from(delay_s) > max {
        return Err(Refusal::DelayTooLarge { max: max.max(0) });
    }
    Ok(())
}

/// Every ENQUEUE refusal, in the order a caller would want to fix them. `engine_now` is the engine's
/// wall clock (Unix seconds), for [`delay`].
pub fn enqueue(req: &EnqueueRequest, store: &StoreConfig, engine_now: i64) -> Result<(), Refusal> {
    for job in &req.jobs {
        queue_name(&job.queue)?;
        payload(&job.payload, store.max_payload_bytes)?;
        delay(job.delay_s, engine_now, dialect_of(store))?;
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

/// RELEASE's refusal: its `delay_s` (the handles are the store kind's to decode).
pub fn release(req: &ReleaseRequest, store: &StoreConfig, engine_now: i64) -> Result<(), Refusal> {
    delay(req.delay_s, engine_now, dialect_of(store))
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
    SQL_RESERVE_FIXED + SQL_RESERVE_PER_JOB
}

/// The part of a RESERVE terminal that does not repeat per job: the `Outcome` envelope, the response
/// array and the jobs array's header, and `stats`.
pub const SQL_RESERVE_FIXED: usize = {
    let outcome = 2; // fixarray(2) + status
    let response = 1 + 5; // fixarray(2) + the jobs array header (array32 at worst)
    let stats = 1 + 9 + 9; // fixarray(2) + two uint64 at worst
    outcome + response + stats
};

/// What one reserved job costs besides its payload bytes, for the `sql` kind: the largest `job_id`
/// PLUS the largest token (§24.4's per-job overhead), and every other per-job field at its worst.
pub const SQL_RESERVE_PER_JOB: usize = {
    let job = 1; // fixarray(7)
    let job_id = 2 + crate::sql::JobId::MAX_ENCODED_LEN; // bin8 + 20 bytes
    let token = 2 + crate::sql::Token::ENCODED_LEN; // bin8 + 8 bytes
    let attempts = 5; // uint32 at worst
    let queue = 3 + QUEUE_NAME_MAX_CHARS * 4; // str16 + 255 four-byte characters
    let payload_header = 5; // str32 at worst
    let times = 9 + 9; // two int64 at worst
    job + job_id + token + attempts + queue + payload_header + times
};

/// SPEC §24.4's frame clamp: the most jobs one RESERVE may lease so its reply fits one frame even when
/// every job carries a payload of the store's `MAX_PAYLOAD_BYTES`. At least 1 — configuration keeps
/// `MAX_PAYLOAD_BYTES` at or below `max_frame_payload − sql_reserve_envelope()` — and never more than
/// the client asked for. At the 4 MiB default it is 3. The clamp is applied BEFORE the statement, as
/// its `LIMIT`: leasing more than fits would leave reservations no reply can carry.
pub fn reserve_limit(max_jobs: u16, max_payload_bytes: u32) -> u16 {
    let room = ferro_proto::consts::MAX_FRAME_PAYLOAD as usize - SQL_RESERVE_FIXED;
    let fit = room / (max_payload_bytes as usize + SQL_RESERVE_PER_JOB);
    let fit = u16::try_from(fit).unwrap_or(u16::MAX).max(1);
    max_jobs.min(fit)
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
        let now = 1_790_000_000;
        assert_eq!(enqueue(&enq(2, None, "{}"), &s, now), Ok(()));
        assert_eq!(
            enqueue(&enq(2, Some("k"), "{}"), &s, now),
            Err(Refusal::DedupOnBatch)
        );
        assert_eq!(
            enqueue(&enq(1, Some("k"), "{}"), &s, now),
            Err(Refusal::DedupUnavailable { configured: false })
        );
        let mut with_table = store();
        with_table.dedup_table = Some(crate::ident::TableName::parse("ferro_dedup").unwrap());
        assert_eq!(
            enqueue(&enq(1, Some("k"), "{}"), &with_table, now),
            Err(Refusal::DedupUnavailable { configured: true })
        );
        assert_eq!(
            enqueue(&enq(1, None, "\0"), &s, now),
            Err(Refusal::PayloadNul)
        );
        let mut bad_queue = enq(1, None, "{}");
        bad_queue.jobs[0].queue = String::new();
        assert_eq!(enqueue(&bad_queue, &s, now), Err(Refusal::QueueName));
        let mut small = store();
        small.max_payload_bytes = 1;
        assert_eq!(
            enqueue(&enq(1, None, "{}"), &small, now),
            Err(Refusal::PayloadTooLarge { max: 1 })
        );
    }

    /// M7-G1b (carried from the G1a review): `now + 1 + delay_s` must fit PostgreSQL's `integer`,
    /// judged before sending on the engine's clock with a one-day margin; `delay_s = 0` is never
    /// refused. The boundary is exact.
    #[test]
    fn a_delay_that_would_overflow_the_time_column_is_refused_before_sending() {
        let now = 1_790_000_000i64;
        let max = i64::from(i32::MAX) - now - 1 - DELAY_CLOCK_MARGIN_S;
        let pg = Dialect::Postgres;
        assert_eq!(delay(0, now, pg), Ok(()));
        assert_eq!(
            delay(0, i64::from(i32::MAX), pg),
            Ok(()),
            "0 is never refused"
        );
        assert_eq!(delay(u32::try_from(max).unwrap(), now, pg), Ok(()));
        assert_eq!(
            delay(u32::try_from(max + 1).unwrap(), now, pg),
            Err(Refusal::DelayTooLarge { max })
        );
        assert_eq!(
            delay(u32::MAX, now, pg),
            Err(Refusal::DelayTooLarge { max })
        );
        // MySQL's INT UNSIGNED holds more (G6 serves it; the rule is the dialect's).
        assert_eq!(
            delay(u32::try_from(max + 1).unwrap(), now, Dialect::Mysql),
            Ok(())
        );
        // Past the ceiling the bound is reported as 0, never negative.
        assert_eq!(
            delay(1, i64::from(i32::MAX), pg),
            Err(Refusal::DelayTooLarge { max: 0 })
        );
        assert!(
            Refusal::DelayTooLarge { max: 7 }
                .to_string()
                .contains("nothing was sent")
        );
        // ENQUEUE checks every job's delay, RELEASE its one.
        let s = store();
        let mut big = enq(2, None, "{}");
        big.jobs[1].delay_s = u32::MAX;
        assert_eq!(enqueue(&big, &s, now), Err(Refusal::DelayTooLarge { max }));
        let rel = |d| ReleaseRequest {
            store: "jobs".into(),
            job_id: b"1".to_vec(),
            token: vec![0; 8],
            delay_s: d,
            common: QueueCommon::default(),
        };
        assert_eq!(release(&rel(5), &s, now), Ok(()));
        assert_eq!(
            release(&rel(u32::MAX), &s, now),
            Err(Refusal::DelayTooLarge { max })
        );
    }

    /// SPEC §24.4's clamp: 3 at the 4 MiB default; never above the request; at least 1 at the largest
    /// payload configuration admits; and the clamped reply of max-size jobs fits one frame.
    #[test]
    fn the_reserve_clamp_fits_one_frame() {
        use crate::config::{DEFAULT_MAX_PAYLOAD_BYTES, max_payload_bytes_limit};
        assert_eq!(reserve_limit(100, DEFAULT_MAX_PAYLOAD_BYTES), 3);
        assert_eq!(reserve_limit(1, DEFAULT_MAX_PAYLOAD_BYTES), 1);
        assert_eq!(reserve_limit(2, DEFAULT_MAX_PAYLOAD_BYTES), 2);
        assert_eq!(reserve_limit(u16::MAX, max_payload_bytes_limit()), 1);
        assert!(reserve_limit(u16::MAX, 1024) > 3);
        for max_payload in [1024u32, 65_536, DEFAULT_MAX_PAYLOAD_BYTES, 5_000_000] {
            let k = reserve_limit(u16::MAX, max_payload);
            let job = ReservedJob {
                job_id: crate::sql::JobId(i64::MIN).encode(),
                token: crate::sql::Token::from_pg(-1, -1).encode().to_vec(),
                attempts: u32::MAX,
                queue: "😀".repeat(QUEUE_NAME_MAX_CHARS),
                payload: "p".repeat(max_payload as usize),
                created_at: i64::MIN,
                lease_deadline: i64::MIN,
            };
            let reply = |n: u16| ReserveResponse {
                jobs: vec![job.clone(); usize::from(n)],
                stats: QueueStats {
                    queue_us: (1 << 63) - 1,
                    exec_us: (1 << 63) - 1,
                },
            };
            let fits = Outcome::Ok(reply(k).encode()).encode().len();
            assert!(
                fits <= MAX_FRAME_PAYLOAD as usize,
                "{max_payload}: {k} jobs = {fits}"
            );
            if max_payload >= 65_536 {
                // Tight: one more max-size job would not fit.
                let over = Outcome::Ok(reply(k + 1).encode()).encode().len();
                assert!(
                    over > MAX_FRAME_PAYLOAD as usize,
                    "{max_payload}: {k}+1 still fits"
                );
            }
        }
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
