//! QUEUE-service wire messages (service `QUEUE = 7`, M7-G1a; SPEC §24.4) — a **bespoke positional
//! codec**, like `messages::http`.
//!
//! They are `Value`-free but carry msgpack **`bin`**: every `job_id`, `new_job_id` and `token` is an
//! OPAQUE byte string (SPEC D22 amendment (b) for the token, D24 for the id), which rmp-serde would
//! write as an array of integers. So each message is written field by field with `rmp` and decoded
//! with `messages::http`'s discipline: strict arity, every array and `str`/`bin` length bounded by
//! the bytes remaining before it allocates, trailing bytes refused, and `common.traceparent`
//! decoded LOSSILY (`ExecRequest` field 9's rule).
//!
//! **What the codec enforces, and what it leaves to the engine.** The codec checks the wire's TYPES
//! and WIDTHS and the shape BOUNDS SPEC §24.4 writes into the shapes — each `job_id`/`token` is
//! `1..=QUEUE_HANDLE_MAX_BYTES` bytes, an ENQUEUE carries `1..=QUEUE_ENQUEUE_MAX_JOBS` jobs, a RESERVE
//! names `1..=QUEUE_RESERVE_MAX_QUEUES` queues, and an ACK `outcome` is one of `[ack_outcome]` — all
//! registry keys, because a bound a RECEIVER enforces is a `/proto` key (the M6-F2 rule). Breaking one
//! is a malformed frame (`Protocol` on the engine, `CodecException` in PHP). What a value MEANS — a
//! payload containing U+0000, a queue name the store refuses, a `job_id` the store's KIND cannot
//! decode — is the engine's business (`ferro-queue`), never a decoder's.
//!
//! Layouts are pinned in `/proto/PROTOCOL.md` §14 and locked by the `queue_*` golden vectors and the
//! `refusal/queue_*` vectors.

use crate::CodecError;
use crate::consts::{
    QUEUE_ENQUEUE_MAX_JOBS, QUEUE_HANDLE_MAX_BYTES, QUEUE_RESERVE_MAX_QUEUES, ack_outcome,
};
use crate::messages::http::{expect_arity, expect_end, write_opt_bin};
use crate::messages::sql::{
    peek_nil, read_opt_str, read_opt_u32, read_opt_u64, write_opt_str, write_opt_u32, write_opt_u64,
};
use crate::value::{bound_len, read_bin, read_bool, read_str, read_str_lossy};
use rmp::decode as dec;
use rmp::encode as enc;

/// `u64` counts on this service (`SIZE`'s three counts, `CLEAR`'s `deleted`, both `stats` fields) are
/// contractually bounded below 2^63, so the PHP client reads them as native ints (`PROTOCOL.md` §2).
const U64_WIRE_BOUND: u64 = 1 << 63;

/// Whether `len` bytes is a legal opaque handle (`job_id`, `new_job_id`, `token`): SPEC §24.4's
/// `bin (1..=1024)`. The one rule both directions of every handle position share.
pub fn handle_len_ok(len: usize) -> bool {
    (1..=QUEUE_HANDLE_MAX_BYTES as usize).contains(&len)
}

/// `common`: `[tx_id: u64 | nil, timeout_ms: u32 | nil, traceparent: str | nil]` (SPEC §24.4), the
/// trailing element of every QUEUE request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueCommon {
    /// The transaction to run in, `nil` for autocommit. Bounded below 2^63 (the TX counter, §2).
    pub tx_id: Option<u64>,
    pub timeout_ms: Option<u32>,
    /// Decoded LOSSILY, exactly as `ExecRequest::traceparent` is (§22.2 (cd)).
    pub traceparent: Option<String>,
}

impl QueueCommon {
    pub const ARITY: u32 = 3;

    fn write(&self, out: &mut Vec<u8>) {
        enc::write_array_len(out, Self::ARITY).unwrap();
        write_opt_u64(out, &self.tx_id);
        write_opt_u32(out, &self.timeout_ms);
        write_opt_str(out, &self.traceparent);
    }

    fn read(rd: &mut &[u8]) -> Result<QueueCommon, CodecError> {
        expect_arity(rd, "QueueCommon", Self::ARITY)?;
        let tx_id = read_opt_u64(rd)?;
        let timeout_ms = read_opt_u32(rd)?;
        let traceparent = if peek_nil(rd)? {
            None
        } else {
            Some(read_str_lossy(rd)?)
        };
        Ok(QueueCommon {
            tx_id,
            timeout_ms,
            traceparent,
        })
    }
}

/// `stats`: `[queue_us: u64, exec_us: u64]`, carried by every success terminal (SPEC §24.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueStats {
    pub queue_us: u64,
    pub exec_us: u64,
}

impl QueueStats {
    pub const ARITY: u32 = 2;

    fn write(&self, out: &mut Vec<u8>) {
        debug_assert!(
            self.queue_us < U64_WIRE_BOUND && self.exec_us < U64_WIRE_BOUND,
            "QueueStats are contractually bounded < 2^63 (PHP int limit); got {self:?}"
        );
        enc::write_array_len(out, Self::ARITY).unwrap();
        enc::write_uint(out, self.queue_us).unwrap();
        enc::write_uint(out, self.exec_us).unwrap();
    }

    fn read(rd: &mut &[u8]) -> Result<QueueStats, CodecError> {
        expect_arity(rd, "QueueStats", Self::ARITY)?;
        Ok(QueueStats {
            queue_us: read_count(rd, "queue_us")?,
            exec_us: read_count(rd, "exec_us")?,
        })
    }
}

/// One job of an `ENQUEUE`: `[queue: str, payload: str, delay_s: u32]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueJob {
    pub queue: String,
    /// Opaque to the engine (SPEC §24.2 I2). A `str`, so strict UTF-8 on the wire.
    pub payload: String,
    pub delay_s: u32,
}

/// `ENQUEUE` (method 1) — client → engine. `[store, jobs (1..=1000), dedup_key: str | nil, common]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueRequest {
    pub store: String,
    pub jobs: Vec<EnqueueJob>,
    pub dedup_key: Option<String>,
    pub common: QueueCommon,
}

impl EnqueueRequest {
    pub const ARITY: u32 = 4;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(count_ok(self.jobs.len(), QUEUE_ENQUEUE_MAX_JOBS));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.store).unwrap();
        enc::write_array_len(&mut out, self.jobs.len() as u32).unwrap();
        for j in &self.jobs {
            enc::write_array_len(&mut out, 3).unwrap();
            enc::write_str(&mut out, &j.queue).unwrap();
            enc::write_str(&mut out, &j.payload).unwrap();
            enc::write_uint(&mut out, u64::from(j.delay_s)).unwrap();
        }
        write_opt_str(&mut out, &self.dedup_key);
        self.common.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<EnqueueRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "EnqueueRequest", Self::ARITY)?;
        let store = read_str(&mut rd)?;
        let n = read_bounded_array_len(&mut rd, "EnqueueRequest jobs", QUEUE_ENQUEUE_MAX_JOBS)?;
        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            expect_arity(&mut rd, "EnqueueJob", 3)?;
            let queue = read_str(&mut rd)?;
            let payload = read_str(&mut rd)?;
            let delay_s = read_u32(&mut rd, "delay_s")?;
            jobs.push(EnqueueJob {
                queue,
                payload,
                delay_s,
            });
        }
        let dedup_key = read_opt_str(&mut rd)?;
        let common = QueueCommon::read(&mut rd)?;
        expect_end(rd)?;
        Ok(EnqueueRequest {
            store,
            jobs,
            dedup_key,
            common,
        })
    }
}

/// `ENQUEUE`'s success terminal body: `[job_id: bin | nil, inserted: u32, deduplicated: bool,
/// stats]`. `job_id` is non-nil iff exactly one job was enqueued (SPEC §24.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueResponse {
    pub job_id: Option<Vec<u8>>,
    pub inserted: u32,
    pub deduplicated: bool,
    pub stats: QueueStats,
}

impl EnqueueResponse {
    pub const ARITY: u32 = 4;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(self.job_id.as_ref().is_none_or(|h| handle_len_ok(h.len())));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        write_opt_bin(&mut out, &self.job_id);
        enc::write_uint(&mut out, u64::from(self.inserted)).unwrap();
        enc::write_bool(&mut out, self.deduplicated).unwrap();
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<EnqueueResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "EnqueueResponse", Self::ARITY)?;
        let job_id = read_opt_handle(&mut rd, "job_id")?;
        let inserted = read_u32(&mut rd, "inserted")?;
        let deduplicated = read_bool(&mut rd)?;
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(EnqueueResponse {
            job_id,
            inserted,
            deduplicated,
            stats,
        })
    }
}

/// `RESERVE` (method 2) — client → engine. `[store, queues: [str] (1..=16, priority order),
/// max_jobs: u16, wait_ms: u32, liveness: bool, common]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveRequest {
    pub store: String,
    pub queues: Vec<String>,
    pub max_jobs: u16,
    pub wait_ms: u32,
    /// In the frozen shape since G1 for a post-v1 liveness release (SPEC §24.15); a v1 engine refuses
    /// `true` and every v1 client sends `false`.
    pub liveness: bool,
    pub common: QueueCommon,
}

impl ReserveRequest {
    pub const ARITY: u32 = 6;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(count_ok(self.queues.len(), QUEUE_RESERVE_MAX_QUEUES));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.store).unwrap();
        enc::write_array_len(&mut out, self.queues.len() as u32).unwrap();
        for q in &self.queues {
            enc::write_str(&mut out, q).unwrap();
        }
        enc::write_uint(&mut out, u64::from(self.max_jobs)).unwrap();
        enc::write_uint(&mut out, u64::from(self.wait_ms)).unwrap();
        enc::write_bool(&mut out, self.liveness).unwrap();
        self.common.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ReserveRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ReserveRequest", Self::ARITY)?;
        let store = read_str(&mut rd)?;
        let n = read_bounded_array_len(&mut rd, "ReserveRequest queues", QUEUE_RESERVE_MAX_QUEUES)?;
        let mut queues = Vec::with_capacity(n);
        for _ in 0..n {
            queues.push(read_str(&mut rd)?);
        }
        let max_jobs: u16 = dec::read_int(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("ReserveRequest max_jobs: {e:?}")))?;
        let wait_ms = read_u32(&mut rd, "wait_ms")?;
        let liveness = read_bool(&mut rd)?;
        let common = QueueCommon::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ReserveRequest {
            store,
            queues,
            max_jobs,
            wait_ms,
            liveness,
            common,
        })
    }
}

/// One reserved job: `[job_id: bin, token: bin, attempts: u32, queue: str, payload: str,
/// created_at: i64, lease_deadline: i64]`. Times are Unix seconds on the database's clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedJob {
    pub job_id: Vec<u8>,
    pub token: Vec<u8>,
    pub attempts: u32,
    pub queue: String,
    pub payload: String,
    pub created_at: i64,
    pub lease_deadline: i64,
}

impl ReservedJob {
    pub const ARITY: u32 = 7;
}

/// `RESERVE`'s success terminal body: `[jobs: [ReservedJob], stats]`, possibly empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveResponse {
    pub jobs: Vec<ReservedJob>,
    pub stats: QueueStats,
}

impl ReserveResponse {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_array_len(&mut out, self.jobs.len() as u32).unwrap();
        for j in &self.jobs {
            debug_assert!(handle_len_ok(j.job_id.len()) && handle_len_ok(j.token.len()));
            enc::write_array_len(&mut out, ReservedJob::ARITY).unwrap();
            enc::write_bin(&mut out, &j.job_id).unwrap();
            enc::write_bin(&mut out, &j.token).unwrap();
            enc::write_uint(&mut out, u64::from(j.attempts)).unwrap();
            enc::write_str(&mut out, &j.queue).unwrap();
            enc::write_str(&mut out, &j.payload).unwrap();
            enc::write_sint(&mut out, j.created_at).unwrap();
            enc::write_sint(&mut out, j.lease_deadline).unwrap();
        }
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ReserveResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ReserveResponse", Self::ARITY)?;
        let n = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("ReserveResponse jobs: {e:?}")))?
            as usize;
        bound_len(n, rd.len())?; // bound BEFORE with_capacity (a lying u32 length)
        let mut jobs = Vec::with_capacity(n);
        for _ in 0..n {
            expect_arity(&mut rd, "ReservedJob", ReservedJob::ARITY)?;
            let job_id = read_handle(&mut rd, "job_id")?;
            let token = read_handle(&mut rd, "token")?;
            let attempts = read_u32(&mut rd, "attempts")?;
            let queue = read_str(&mut rd)?;
            let payload = read_str(&mut rd)?;
            let created_at = read_i64(&mut rd, "created_at")?;
            let lease_deadline = read_i64(&mut rd, "lease_deadline")?;
            jobs.push(ReservedJob {
                job_id,
                token,
                attempts,
                queue,
                payload,
                created_at,
                lease_deadline,
            });
        }
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ReserveResponse { jobs, stats })
    }
}

/// A FENCED verb's request: `ACK` (method 3) and `EXTEND` (method 5) share it —
/// `[store, job_id: bin, token: bin, common]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedRequest {
    pub store: String,
    pub job_id: Vec<u8>,
    pub token: Vec<u8>,
    pub common: QueueCommon,
}

impl FencedRequest {
    pub const ARITY: u32 = 4;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(handle_len_ok(self.job_id.len()) && handle_len_ok(self.token.len()));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.store).unwrap();
        enc::write_bin(&mut out, &self.job_id).unwrap();
        enc::write_bin(&mut out, &self.token).unwrap();
        self.common.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<FencedRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "FencedRequest", Self::ARITY)?;
        let store = read_str(&mut rd)?;
        let job_id = read_handle(&mut rd, "job_id")?;
        let token = read_handle(&mut rd, "token")?;
        let common = QueueCommon::read(&mut rd)?;
        expect_end(rd)?;
        Ok(FencedRequest {
            store,
            job_id,
            token,
            common,
        })
    }
}

/// `ACK`'s success terminal body: `[outcome: u8, stats]`, `outcome` one of `[ack_outcome]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckResponse {
    pub outcome: u8,
    pub stats: QueueStats,
}

impl AckResponse {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(ack_outcome_ok(self.outcome));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_uint(&mut out, u64::from(self.outcome)).unwrap();
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<AckResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "AckResponse", Self::ARITY)?;
        let outcome: u8 = dec::read_int(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("AckResponse outcome: {e:?}")))?;
        if !ack_outcome_ok(outcome) {
            return Err(CodecError::Malformed(format!(
                "AckResponse outcome {outcome} is not an [ack_outcome] value"
            )));
        }
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(AckResponse { outcome, stats })
    }
}

/// `RELEASE` (method 4) — client → engine. `[store, job_id: bin, token: bin, delay_s: u32, common]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseRequest {
    pub store: String,
    pub job_id: Vec<u8>,
    pub token: Vec<u8>,
    pub delay_s: u32,
    pub common: QueueCommon,
}

impl ReleaseRequest {
    pub const ARITY: u32 = 5;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(handle_len_ok(self.job_id.len()) && handle_len_ok(self.token.len()));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.store).unwrap();
        enc::write_bin(&mut out, &self.job_id).unwrap();
        enc::write_bin(&mut out, &self.token).unwrap();
        enc::write_uint(&mut out, u64::from(self.delay_s)).unwrap();
        self.common.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ReleaseRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ReleaseRequest", Self::ARITY)?;
        let store = read_str(&mut rd)?;
        let job_id = read_handle(&mut rd, "job_id")?;
        let token = read_handle(&mut rd, "token")?;
        let delay_s = read_u32(&mut rd, "delay_s")?;
        let common = QueueCommon::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ReleaseRequest {
            store,
            job_id,
            token,
            delay_s,
            common,
        })
    }
}

/// `RELEASE`'s success terminal body: `[new_job_id: bin | nil, stats]`; `nil` is `gone`
/// (autocommit only, SPEC §24.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseResponse {
    pub new_job_id: Option<Vec<u8>>,
    pub stats: QueueStats,
}

impl ReleaseResponse {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.new_job_id
                .as_ref()
                .is_none_or(|h| handle_len_ok(h.len()))
        );
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        write_opt_bin(&mut out, &self.new_job_id);
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ReleaseResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ReleaseResponse", Self::ARITY)?;
        let new_job_id = read_opt_handle(&mut rd, "new_job_id")?;
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ReleaseResponse { new_job_id, stats })
    }
}

/// `EXTEND`'s success terminal body: `[lease_deadline: i64, stats]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtendResponse {
    pub lease_deadline: i64,
    pub stats: QueueStats,
}

impl ExtendResponse {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_sint(&mut out, self.lease_deadline).unwrap();
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ExtendResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ExtendResponse", Self::ARITY)?;
        let lease_deadline = read_i64(&mut rd, "lease_deadline")?;
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ExtendResponse {
            lease_deadline,
            stats,
        })
    }
}

/// A per-queue request: `SIZE` (method 6) and `CLEAR` (method 7) share it — `[store, queue, common]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueScopeRequest {
    pub store: String,
    pub queue: String,
    pub common: QueueCommon,
}

impl QueueScopeRequest {
    pub const ARITY: u32 = 3;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_str(&mut out, &self.store).unwrap();
        enc::write_str(&mut out, &self.queue).unwrap();
        self.common.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<QueueScopeRequest, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "QueueScopeRequest", Self::ARITY)?;
        let store = read_str(&mut rd)?;
        let queue = read_str(&mut rd)?;
        let common = QueueCommon::read(&mut rd)?;
        expect_end(rd)?;
        Ok(QueueScopeRequest {
            store,
            queue,
            common,
        })
    }
}

/// `SIZE`'s success terminal body: `[pending: u64, delayed: u64, reserved: u64, stats]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeResponse {
    pub pending: u64,
    pub delayed: u64,
    pub reserved: u64,
    pub stats: QueueStats,
}

impl SizeResponse {
    pub const ARITY: u32 = 4;

    pub fn encode(&self) -> Vec<u8> {
        let counts = [self.pending, self.delayed, self.reserved];
        debug_assert!(counts.iter().all(|&n| n < U64_WIRE_BOUND));
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        for n in counts {
            enc::write_uint(&mut out, n).unwrap();
        }
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<SizeResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "SizeResponse", Self::ARITY)?;
        let pending = read_count(&mut rd, "pending")?;
        let delayed = read_count(&mut rd, "delayed")?;
        let reserved = read_count(&mut rd, "reserved")?;
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(SizeResponse {
            pending,
            delayed,
            reserved,
            stats,
        })
    }
}

/// `CLEAR`'s success terminal body: `[deleted: u64, stats]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClearResponse {
    pub deleted: u64,
    pub stats: QueueStats,
}

impl ClearResponse {
    pub const ARITY: u32 = 2;

    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(self.deleted < U64_WIRE_BOUND);
        let mut out = Vec::new();
        enc::write_array_len(&mut out, Self::ARITY).unwrap();
        enc::write_uint(&mut out, self.deleted).unwrap();
        self.stats.write(&mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Result<ClearResponse, CodecError> {
        let mut rd: &[u8] = b;
        expect_arity(&mut rd, "ClearResponse", Self::ARITY)?;
        let deleted = read_count(&mut rd, "deleted")?;
        let stats = QueueStats::read(&mut rd)?;
        expect_end(rd)?;
        Ok(ClearResponse { deleted, stats })
    }
}

// --- field rules (one each, mirrored in the PHP codec `Ferro\Protocol\QueueCodec`) ---

fn ack_outcome_ok(v: u8) -> bool {
    v == ack_outcome::ACKED || v == ack_outcome::GONE
}

fn count_ok(n: usize, max: u32) -> bool {
    (1..=max as usize).contains(&n)
}

/// An opaque handle: a `bin` of `1..=QUEUE_HANDLE_MAX_BYTES` bytes. The length is checked BEFORE the
/// body is copied, so a 1 025-byte handle is refused without allocating it.
fn read_handle(rd: &mut &[u8], what: &str) -> Result<Vec<u8>, CodecError> {
    let mut peek: &[u8] = rd;
    let len = dec::read_bin_len(&mut peek)
        .map_err(|e| CodecError::Malformed(format!("QUEUE {what}: {e:?}")))? as usize;
    if !handle_len_ok(len) {
        return Err(CodecError::Malformed(format!(
            "QUEUE {what}: {len} bytes, outside 1..={QUEUE_HANDLE_MAX_BYTES}"
        )));
    }
    read_bin(rd)
}

fn read_opt_handle(rd: &mut &[u8], what: &str) -> Result<Option<Vec<u8>>, CodecError> {
    if peek_nil(rd)? {
        return Ok(None);
    }
    Ok(Some(read_handle(rd, what)?))
}

/// An array length in `1..=max`, checked against the bytes remaining before anything is allocated.
fn read_bounded_array_len(rd: &mut &[u8], what: &str, max: u32) -> Result<usize, CodecError> {
    let n = dec::read_array_len(rd).map_err(|e| CodecError::Malformed(format!("{what}: {e:?}")))?
        as usize;
    if !count_ok(n, max) {
        return Err(CodecError::Malformed(format!(
            "{what}: {n} entries, outside 1..={max}"
        )));
    }
    bound_len(n, rd.len())?;
    Ok(n)
}

fn read_u32(rd: &mut &[u8], what: &str) -> Result<u32, CodecError> {
    dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("QUEUE {what}: {e:?}")))
}

fn read_i64(rd: &mut &[u8], what: &str) -> Result<i64, CodecError> {
    dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("QUEUE {what}: {e:?}")))
}

/// A `u64` count, refused at or above 2^63 — the encoder never writes one, and PHP refuses one, so
/// the two decoders agree on what a well-formed body is.
fn read_count(rd: &mut &[u8], what: &str) -> Result<u64, CodecError> {
    let n: u64 =
        dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("QUEUE {what}: {e:?}")))?;
    if n >= U64_WIRE_BOUND {
        return Err(CodecError::Malformed(format!(
            "QUEUE {what}: {n} is not below 2^63"
        )));
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Outcome;

    const MAX: usize = QUEUE_HANDLE_MAX_BYTES as usize;

    fn common() -> QueueCommon {
        QueueCommon {
            tx_id: Some(5_000_000_000),
            timeout_ms: Some(30_000),
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        }
    }
    fn stats() -> QueueStats {
        QueueStats {
            queue_us: 12,
            exec_us: 70_000,
        }
    }
    fn fenced(job_id: Vec<u8>, token: Vec<u8>) -> FencedRequest {
        FencedRequest {
            store: "jobs".into(),
            job_id,
            token,
            common: QueueCommon::default(),
        }
    }
    fn release(job_id: Vec<u8>, token: Vec<u8>) -> ReleaseRequest {
        ReleaseRequest {
            store: "jobs".into(),
            job_id,
            token,
            delay_s: 30,
            common: common(),
        }
    }
    fn reserved(job_id: Vec<u8>, token: Vec<u8>) -> ReserveResponse {
        ReserveResponse {
            jobs: vec![ReservedJob {
                job_id,
                token,
                attempts: 3,
                queue: "emails".into(),
                payload: "{}".into(),
                created_at: 1_790_000_000,
                lease_deadline: 1_790_000_091,
            }],
            stats: stats(),
        }
    }

    /// The bytes a broken peer would send: built field by field, because the encoders' debug
    /// asserts guard the ENGINE's own output and would refuse to build them.
    fn raw_fenced(job_id: &[u8], token: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, FencedRequest::ARITY).unwrap();
        enc::write_str(&mut out, "jobs").unwrap();
        enc::write_bin(&mut out, job_id).unwrap();
        enc::write_bin(&mut out, token).unwrap();
        QueueCommon::default().write(&mut out);
        out
    }

    #[test]
    fn every_message_roundtrips() {
        let enq = EnqueueRequest {
            store: "jobs".into(),
            jobs: vec![
                EnqueueJob {
                    queue: "default".into(),
                    payload: "{\"a\":1}".into(),
                    delay_s: 0,
                },
                EnqueueJob {
                    queue: "emails".into(),
                    payload: "x".repeat(300),
                    delay_s: 70_000,
                },
            ],
            dedup_key: Some("k-1".into()),
            common: common(),
        };
        assert_eq!(EnqueueRequest::decode(&enq.encode()).unwrap(), enq);
        for r in [
            EnqueueResponse {
                job_id: Some(b"42".to_vec()),
                inserted: 1,
                deduplicated: true,
                stats: stats(),
            },
            EnqueueResponse {
                job_id: None,
                inserted: 1000,
                deduplicated: false,
                stats: stats(),
            },
        ] {
            assert_eq!(EnqueueResponse::decode(&r.encode()).unwrap(), r);
        }
        let res = ReserveRequest {
            store: "jobs".into(),
            queues: vec!["high".into(), "default".into()],
            max_jobs: 65_535,
            wait_ms: 3_000,
            liveness: false,
            common: QueueCommon::default(),
        };
        assert_eq!(ReserveRequest::decode(&res.encode()).unwrap(), res);
        let rr = reserved(vec![0xc0; MAX], vec![0x80; 8]);
        assert_eq!(ReserveResponse::decode(&rr.encode()).unwrap(), rr);
        let empty = ReserveResponse {
            jobs: vec![],
            stats: stats(),
        };
        assert_eq!(ReserveResponse::decode(&empty.encode()).unwrap(), empty);
        let f = fenced(b"1".to_vec(), vec![1; MAX]);
        assert_eq!(FencedRequest::decode(&f.encode()).unwrap(), f);
        for outcome in [ack_outcome::ACKED, ack_outcome::GONE] {
            let a = AckResponse {
                outcome,
                stats: stats(),
            };
            assert_eq!(AckResponse::decode(&a.encode()).unwrap(), a);
        }
        let rel = release(b"9223372036854775807".to_vec(), vec![0; 8]);
        assert_eq!(ReleaseRequest::decode(&rel.encode()).unwrap(), rel);
        for new_job_id in [None, Some(b"43".to_vec())] {
            let r = ReleaseResponse {
                new_job_id,
                stats: stats(),
            };
            assert_eq!(ReleaseResponse::decode(&r.encode()).unwrap(), r);
        }
        // Negative times survive (an i64 on the wire; PG's `integer` is signed).
        let e = ExtendResponse {
            lease_deadline: -5,
            stats: stats(),
        };
        assert_eq!(ExtendResponse::decode(&e.encode()).unwrap(), e);
        let s = QueueScopeRequest {
            store: "jobs".into(),
            queue: "default".into(),
            common: common(),
        };
        assert_eq!(QueueScopeRequest::decode(&s.encode()).unwrap(), s);
        let z = SizeResponse {
            pending: 1,
            delayed: 70_000,
            reserved: 5_000_000_000,
            stats: stats(),
        };
        assert_eq!(SizeResponse::decode(&z.encode()).unwrap(), z);
        let c = ClearResponse {
            deleted: 7,
            stats: stats(),
        };
        assert_eq!(ClearResponse::decode(&c.encode()).unwrap(), c);
        // Every response body composes with `Outcome::Ok` (one top-level msgpack value).
        match Outcome::decode(&Outcome::Ok(c.encode()).encode()).unwrap() {
            Outcome::Ok(body) => assert_eq!(ClearResponse::decode(&body).unwrap(), c),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_handle_is_refused_at_zero_and_past_the_registry_bound_in_both_request_positions() {
        // The control: both bounds INCLUSIVE are accepted.
        for len in [1, MAX] {
            assert!(FencedRequest::decode(&raw_fenced(&vec![7; len], &[1; 8])).is_ok());
            assert!(FencedRequest::decode(&raw_fenced(b"1", &vec![7; len])).is_ok());
        }
        for len in [0, MAX + 1] {
            for (job_id, token, field) in [
                (vec![7; len], vec![1; 8], "job_id"),
                (b"1".to_vec(), vec![7; len], "token"),
            ] {
                match FencedRequest::decode(&raw_fenced(&job_id, &token)) {
                    Err(CodecError::Malformed(m)) => {
                        assert!(m.contains(field) && m.contains(&len.to_string()), "{m}")
                    }
                    other => panic!("{field} of {len} bytes: {other:?}"),
                }
            }
        }
    }

    /// Encode a valid body around a unique 7-byte marker, then splice a `bin` of `len` bytes in its
    /// place — the bytes a broken engine (or peer) would send.
    fn splice(valid: Vec<u8>, marker: &[u8], len: usize) -> Vec<u8> {
        let mut needle = vec![0xc4, marker.len() as u8];
        needle.extend_from_slice(marker);
        let at = valid
            .windows(needle.len())
            .position(|w| w == needle.as_slice())
            .expect("marker present");
        let mut bad = Vec::new();
        enc::write_bin(&mut bad, &vec![9; len]).unwrap();
        let mut out = valid[..at].to_vec();
        out.extend_from_slice(&bad);
        out.extend_from_slice(&valid[at + needle.len()..]);
        out
    }

    #[test]
    fn a_handle_is_refused_out_of_bounds_in_every_other_position() {
        let m = b"\x01MARKER".to_vec();
        for len in [0, MAX + 1] {
            let enq = EnqueueResponse {
                job_id: Some(m.clone()),
                inserted: 1,
                deduplicated: false,
                stats: stats(),
            };
            assert!(EnqueueResponse::decode(&splice(enq.encode(), &m, len)).is_err());
            let rel = ReleaseResponse {
                new_job_id: Some(m.clone()),
                stats: stats(),
            };
            assert!(ReleaseResponse::decode(&splice(rel.encode(), &m, len)).is_err());
            let rr = reserved(m.clone(), vec![1; 8]);
            assert!(ReserveResponse::decode(&splice(rr.encode(), &m, len)).is_err());
            let rr = reserved(b"1".to_vec(), m.clone());
            assert!(ReserveResponse::decode(&splice(rr.encode(), &m, len)).is_err());
            let rq = release(m.clone(), vec![1; 8]);
            assert!(ReleaseRequest::decode(&splice(rq.encode(), &m, len)).is_err());
            let rq = release(b"1".to_vec(), m.clone());
            assert!(ReleaseRequest::decode(&splice(rq.encode(), &m, len)).is_err());
        }
        // The control: the splice itself is sound — an in-bounds splice decodes, in every position.
        for len in [1, 20, MAX] {
            let rq = release(m.clone(), vec![1; 8]);
            assert_eq!(
                ReleaseRequest::decode(&splice(rq.encode(), &m, len))
                    .unwrap()
                    .job_id,
                vec![9; len]
            );
            let enq = EnqueueResponse {
                job_id: Some(m.clone()),
                inserted: 1,
                deduplicated: false,
                stats: stats(),
            };
            assert!(EnqueueResponse::decode(&splice(enq.encode(), &m, len)).is_ok());
            let rr = reserved(b"1".to_vec(), m.clone());
            assert!(ReserveResponse::decode(&splice(rr.encode(), &m, len)).is_ok());
        }
    }

    #[test]
    fn a_handle_must_be_bin_not_str() {
        let mut b = Vec::new();
        enc::write_array_len(&mut b, FencedRequest::ARITY).unwrap();
        enc::write_str(&mut b, "jobs").unwrap();
        enc::write_str(&mut b, "42").unwrap(); // str where a bin belongs
        enc::write_bin(&mut b, &[1; 8]).unwrap();
        QueueCommon::default().write(&mut b);
        assert!(matches!(
            FencedRequest::decode(&b),
            Err(CodecError::Malformed(_))
        ));
    }

    fn raw_enqueue(n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, EnqueueRequest::ARITY).unwrap();
        enc::write_str(&mut out, "jobs").unwrap();
        enc::write_array_len(&mut out, n as u32).unwrap();
        for _ in 0..n {
            enc::write_array_len(&mut out, 3).unwrap();
            enc::write_str(&mut out, "q").unwrap();
            enc::write_str(&mut out, "p").unwrap();
            enc::write_uint(&mut out, 0).unwrap();
        }
        enc::write_nil(&mut out).unwrap();
        QueueCommon::default().write(&mut out);
        out
    }

    fn raw_reserve(n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, ReserveRequest::ARITY).unwrap();
        enc::write_str(&mut out, "jobs").unwrap();
        enc::write_array_len(&mut out, n as u32).unwrap();
        for _ in 0..n {
            enc::write_str(&mut out, "q").unwrap();
        }
        enc::write_uint(&mut out, 1).unwrap();
        enc::write_uint(&mut out, 0).unwrap();
        enc::write_bool(&mut out, false).unwrap();
        QueueCommon::default().write(&mut out);
        out
    }

    #[test]
    fn job_and_queue_counts_are_bounded_by_the_registry() {
        let max_jobs = QUEUE_ENQUEUE_MAX_JOBS as usize;
        assert_eq!(
            EnqueueRequest::decode(&raw_enqueue(1)).unwrap().jobs.len(),
            1
        );
        assert_eq!(
            EnqueueRequest::decode(&raw_enqueue(max_jobs))
                .unwrap()
                .jobs
                .len(),
            max_jobs
        );
        assert!(EnqueueRequest::decode(&raw_enqueue(0)).is_err());
        assert!(EnqueueRequest::decode(&raw_enqueue(max_jobs + 1)).is_err());

        let max_q = QUEUE_RESERVE_MAX_QUEUES as usize;
        assert_eq!(
            ReserveRequest::decode(&raw_reserve(1))
                .unwrap()
                .queues
                .len(),
            1
        );
        assert_eq!(
            ReserveRequest::decode(&raw_reserve(max_q))
                .unwrap()
                .queues
                .len(),
            max_q
        );
        assert!(ReserveRequest::decode(&raw_reserve(0)).is_err());
        assert!(ReserveRequest::decode(&raw_reserve(max_q + 1)).is_err());
    }

    #[test]
    fn an_ack_outcome_outside_the_registry_is_refused() {
        for bad in [0u8, 3, 255] {
            let mut b = Vec::new();
            enc::write_array_len(&mut b, 2).unwrap();
            enc::write_uint(&mut b, u64::from(bad)).unwrap();
            stats().write(&mut b);
            assert!(AckResponse::decode(&b).is_err(), "outcome {bad}");
        }
    }

    #[test]
    fn a_count_at_2_63_is_refused() {
        let body = |n: u64| {
            let mut b = Vec::new();
            enc::write_array_len(&mut b, 2).unwrap();
            enc::write_uint(&mut b, n).unwrap();
            stats().write(&mut b);
            b
        };
        assert!(ClearResponse::decode(&body(U64_WIRE_BOUND)).is_err());
        assert_eq!(
            ClearResponse::decode(&body(U64_WIRE_BOUND - 1))
                .unwrap()
                .deleted,
            U64_WIRE_BOUND - 1,
            "the control: 2^63 - 1 is a count"
        );
    }

    #[test]
    fn a_non_utf8_queue_is_refused_but_a_non_utf8_traceparent_is_not() {
        let mut req = QueueScopeRequest {
            store: "jobs".into(),
            queue: "q".into(),
            common: QueueCommon {
                tx_id: None,
                timeout_ms: None,
                traceparent: Some("00-x".into()),
            },
        };
        let mut b = req.encode();
        let n = b.len();
        b[n - 1] = 0xff;
        let back = QueueScopeRequest::decode(&b).expect("a lossy traceparent never fails");
        assert_eq!(back.common.traceparent.as_deref(), Some("00-\u{fffd}"));

        req.common.traceparent = None;
        let mut b = req.encode();
        let at = b.iter().rposition(|&x| x == b'q').unwrap();
        b[at] = 0xff; // the queue name is a strict str
        assert!(matches!(
            QueueScopeRequest::decode(&b),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn arity_trailing_bytes_and_lying_lengths_are_refused() {
        let mut b = fenced(b"1".to_vec(), vec![1; 8]).encode();
        b.push(0xc0);
        assert!(matches!(
            FencedRequest::decode(&b),
            Err(CodecError::TrailingBytes(1))
        ));
        let mut b = fenced(b"1".to_vec(), vec![1; 8]).encode();
        b[0] = 0x93; // claim 3 fields
        assert!(FencedRequest::decode(&b).is_err());
        let mut b = Vec::new();
        enc::write_array_len(&mut b, 2).unwrap();
        enc::write_array_len(&mut b, u32::MAX).unwrap();
        assert!(matches!(
            ReserveResponse::decode(&b),
            Err(CodecError::Truncated { .. })
        ));
        // `common` is a fixarray of exactly 3.
        let mut b = Vec::new();
        enc::write_array_len(&mut b, 3).unwrap();
        enc::write_str(&mut b, "jobs").unwrap();
        enc::write_str(&mut b, "q").unwrap();
        enc::write_array_len(&mut b, 2).unwrap();
        enc::write_nil(&mut b).unwrap();
        enc::write_nil(&mut b).unwrap();
        assert!(QueueScopeRequest::decode(&b).is_err());
    }
}
