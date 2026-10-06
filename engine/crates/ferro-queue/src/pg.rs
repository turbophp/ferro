//! The `sql` kind's PostgreSQL statements (SPEC §24.3, §24.4; M7-G1b): the CLOSED set of statements
//! the engine authors for the seven verbs in autocommit, and the decoders that turn each statement's
//! rows into the verb's outcome.
//!
//! D22 grants the engine a scope exception — it composes statements that write application rows —
//! and states its mitigation: those statements live HERE, in a crate with no I/O, one builder per verb
//! and nothing else, so each is pinned by a unit test and mutation-tested. Nothing in this module
//! reads, changes or infers from a user statement; the only identifier spliced into the SQL is the
//! store's table, validated at configuration and quoted ([`TableName::quoted`]); every client value
//! is a bound parameter.
//!
//! # The clock (§24.3)
//!
//! Every verb computes `now` ONCE, as integer seconds on the DATABASE's clock, in its first statement:
//! [`NOW_CTE`], `floor(extract(epoch FROM statement_timestamp()))::bigint` — statement time, not the
//! wall-clock `clock_timestamp()`. On PostgreSQL every autocommit verb is ONE statement, so "the first
//! statement" is the statement, and every rule below reads `n.s`. `MATERIALIZED` makes the CTE an
//! optimisation fence: `n` is evaluated once and every reference reads the same row.
//!
//! # The rounding rules (§24.3, normative)
//!
//! - **Availability:** `reserved_at IS NULL AND available_at <= now`.
//! - **Delay:** `available_at = now` when `delay_s = 0`, else `now + 1 + delay_s` — never early.
//! - **Lease expiry:** `reserved_at < now - lease_s` — the effective lease is in (L, L+1] s.
//! - **`lease_deadline`:** `reserved_at + lease_s + 1`, the first DB second another RESERVE may take
//!   the job.
//!
//! # The fence (§24.3)
//!
//! A token is valid iff a row exists with that `id`, `attempts` and `created_at`. ACK, RELEASE and
//! EXTEND each match `WHERE id = $1 AND attempts = $2 AND created_at = $3` and nothing else — in
//! particular NOT `reserved_at`, so a token survives its lease's expiry until someone else reserves the
//! job (a late ACK is honoured when nobody took the job), and an unreserve (§24.8, slice G3), which
//! restores `attempts` only for a token nobody received, never invalidates a delivered one. Shape
//! verification has proven `id` unique, so the fence names at most one row.
//!
//! When a fenced ACK or RELEASE matches nothing, the same statement PROBES the id ([`probe_verdict`]):
//! absent → `gone`; present with another token → `LeaseLost`; present with the SAME token → `gone`
//! (a concurrent delete committed after the statement's snapshot: the probe reads the snapshot, the
//! fenced DELETE the latest committed version). A race can misreport `LeaseLost` as `gone` — a
//! concurrent RESERVE that re-leased the row after our snapshot makes the probe read the old token —
//! and both outcomes mean "did nothing", so the race affects only a counter (§24.4).

use crate::Dialect;
use crate::ident::TableName;
use crate::shape::Statement;
use crate::sql::{JobId, Token};
use ferro_proto::messages::EnqueueJob;
use ferro_proto::value::Value;

/// The verb's `now`, computed once (SPEC §24.3). Every statement in this module starts `WITH` it.
pub const NOW_CTE: &str =
    "n AS MATERIALIZED (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint AS s)";

/// The stock layout's `attempts` is `smallint`. A row is reserved only while `attempts` is BELOW
/// this, so the reservation's `attempts + 1` always fits; a row AT it is never reserved again (the
/// column ceiling, §24.7's only engine poison rule).
pub const PG_ATTEMPTS_CEILING: i64 = i16::MAX as i64;

/// The largest value an `available_at`/`reserved_at`/`created_at` column holds (`integer`).
pub const PG_TIME_MAX: i64 = i32::MAX as i64;

fn table(t: &TableName) -> String {
    t.quoted(Dialect::Postgres)
}

/// The `available_at` expression for a delay held in `delay` (a `bigint` expression), per §24.3.
fn available_at(delay: &str) -> String {
    format!("CASE WHEN {delay} = 0 THEN n.s ELSE n.s + 1 + {delay} END")
}

/// The fence predicate over `$1` (id), `$2` (attempts) and `$3` (created_at).
const FENCE: &str = "id = $1::bigint AND attempts = $2::smallint AND created_at = $3::integer";

fn fence_params(id: JobId, token: Token) -> Vec<Value> {
    vec![
        Value::I64(id.0),
        Value::I64(i64::from(token.pg_attempts())),
        Value::I64(i64::from(token.pg_created_at())),
    ]
}

/// The probe of `$1`'s row, as the statement's snapshot sees it: one row, always, whose two cells
/// are the row's `attempts` and `created_at`, or both NULL when no row has that id.
fn probe_from(t: &str) -> String {
    format!("FROM (SELECT 1) AS one LEFT JOIN {t} AS p ON p.id = $1::bigint")
}

/// ENQUEUE (§24.4): ONE multi-row INSERT, atomic. `attempts = 0`, `reserved_at = NULL`,
/// `created_at = now`, `available_at` per the delay rule; rows are inserted in the request's order, so
/// ids ascend in it. A single job's statement returns its id; a batch's returns nothing, and
/// `inserted` is the command tag's count (§24.4: no batch ids).
pub fn enqueue(t: &TableName, jobs: Vec<EnqueueJob>) -> Statement {
    let single = jobs.len() == 1;
    let mut rows = Vec::with_capacity(jobs.len());
    let mut params = Vec::with_capacity(jobs.len() * 3);
    for (i, job) in jobs.into_iter().enumerate() {
        let b = i * 3;
        rows.push(format!(
            "(${}::text, ${}::text, ${}::bigint, {i})",
            b + 1,
            b + 2,
            b + 3
        ));
        params.push(Value::Text(job.queue));
        params.push(Value::Text(job.payload));
        params.push(Value::I64(i64::from(job.delay_s)));
    }
    let sql = format!(
        "WITH {NOW_CTE} \
         INSERT INTO {t} (queue, payload, attempts, reserved_at, available_at, created_at) \
         SELECT v.q, v.p, 0, NULL::integer, {avail}, n.s \
         FROM n, (VALUES {rows}) AS v(q, p, d, o) \
         ORDER BY v.o{returning}",
        t = table(t),
        avail = available_at("v.d"),
        rows = rows.join(", "),
        returning = if single { " RETURNING id" } else { "" },
    );
    Statement { sql, params }
}

/// RESERVE for ONE queue (§24.4), exactly the statement §24.4 writes: the `MATERIALIZED` locking CTE
/// is evaluated once, so the statement never locks or updates more than `limit` rows (the rescan
/// premise, asserted under concurrency by `ferrod`'s live suite). Rows at the attempts ceiling and
/// rows whose payload exceeds `max_payload_bytes` (from stock producers) are never reserved.
pub fn reserve(
    t: &TableName,
    queue: &str,
    lease_s: u32,
    max_payload_bytes: u32,
    limit: u16,
) -> Statement {
    let sql = format!(
        "WITH {NOW_CTE}, \
         c AS MATERIALIZED (\
           SELECT t.id FROM {t} AS t, n \
           WHERE t.queue = $1 AND t.attempts < $2::smallint \
             AND octet_length(t.payload) <= $3::integer \
             AND ((t.reserved_at IS NULL AND t.available_at <= n.s) \
                  OR t.reserved_at < n.s - $4::bigint) \
           ORDER BY t.id LIMIT $5::bigint FOR UPDATE OF t SKIP LOCKED) \
         UPDATE {t} AS t SET reserved_at = n.s, attempts = t.attempts + 1 \
         FROM c, n WHERE t.id = c.id \
         RETURNING t.id, t.attempts, t.created_at, t.queue, t.payload, n.s + $4::bigint + 1",
        t = table(t),
    );
    Statement {
        sql,
        params: vec![
            Value::Text(queue.to_string()),
            Value::I64(PG_ATTEMPTS_CEILING),
            Value::I64(i64::from(max_payload_bytes)),
            Value::I64(i64::from(lease_s)),
            Value::I64(i64::from(limit)),
        ],
    }
}

/// ACK (§24.4): the fenced DELETE and, in the same statement, the probe of the id.
pub fn ack(t: &TableName, id: JobId, token: Token) -> Statement {
    let t = table(t);
    let sql = format!(
        "WITH d AS (DELETE FROM {t} WHERE {FENCE} RETURNING 1) \
         SELECT (SELECT count(*) FROM d), p.attempts, p.created_at {probe}",
        probe = probe_from(&t),
    );
    Statement {
        sql,
        params: fence_params(id, token),
    }
}

/// RELEASE (§24.4): one statement — the fenced DELETE feeds an INSERT of the same job under a NEW id,
/// `attempts` kept, `reserved_at = NULL`, `created_at = now` (as stock's `pushToDatabase`),
/// `available_at` per the delay rule — plus the probe of the old id, for the no-match answer.
pub fn release(t: &TableName, id: JobId, token: Token, delay_s: u32) -> Statement {
    let t = table(t);
    let sql = format!(
        "WITH {NOW_CTE}, \
         d AS (DELETE FROM {t} WHERE {FENCE} RETURNING queue, payload, attempts), \
         i AS (INSERT INTO {t} (queue, payload, attempts, reserved_at, available_at, created_at) \
               SELECT d.queue, d.payload, d.attempts, NULL::integer, {avail}, n.s \
               FROM d, n RETURNING id) \
         SELECT (SELECT i.id FROM i), p.attempts, p.created_at {probe}",
        avail = available_at("$4::bigint"),
        probe = probe_from(&t),
    );
    let mut params = fence_params(id, token);
    params.push(Value::I64(i64::from(delay_s)));
    Statement { sql, params }
}

/// EXTEND (§24.4): the fenced `reserved_at = now`, renewing by one full lease; it also retakes a job
/// whose lease expired when nobody else took it. Returns the new `lease_deadline`.
pub fn extend(t: &TableName, id: JobId, token: Token, lease_s: u32) -> Statement {
    let sql = format!(
        "WITH {NOW_CTE} \
         UPDATE {t} AS t SET reserved_at = n.s FROM n \
         WHERE {FENCE} \
         RETURNING n.s + $4::bigint + 1",
        t = table(t),
    );
    let mut params = fence_params(id, token);
    params.push(Value::I64(i64::from(lease_s)));
    Statement { sql, params }
}

/// SIZE (§24.4): the three counts with Laravel 12's `pendingSize`/`delayedSize`/`reservedSize`
/// definitions (illuminate/queue v12.69.3) — `reserved` counts every reserved row, expired leases
/// included — and `oldest_pending_at`, the smallest pending `available_at`
/// (`creationTimeOfOldestPendingJob`). One row, always, even for an empty queue.
pub fn size(t: &TableName, queue: &str) -> Statement {
    let sql = format!(
        "WITH {NOW_CTE} \
         SELECT count(*) FILTER (WHERE t.reserved_at IS NULL AND t.available_at <= n.s), \
                count(*) FILTER (WHERE t.reserved_at IS NULL AND t.available_at > n.s), \
                count(*) FILTER (WHERE t.reserved_at IS NOT NULL), \
                min(t.available_at) FILTER (WHERE t.reserved_at IS NULL AND t.available_at <= n.s) \
         FROM n LEFT JOIN {t} AS t ON t.queue = $1",
        t = table(t),
    );
    Statement {
        sql,
        params: vec![Value::Text(queue.to_string())],
    }
}

/// CLEAR (§24.4): delete the queue's rows, reserved ones included (stock's `clear()`).
pub fn clear(t: &TableName, queue: &str) -> Statement {
    Statement {
        sql: format!("DELETE FROM {} WHERE queue = $1", table(t)),
        params: vec![Value::Text(queue.to_string())],
    }
}

/// A statement's result did not have the shape its builder produces. Only a table changed after
/// verification (no reload in v1, §24.3) or a defect produces one; `ferrod` reports it without
/// claiming the verb did nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

/// What a fenced verb that matched no row answers (§24.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoMatch {
    /// The job is gone: success-equivalent ("did nothing", and nobody holds it under this token).
    Gone,
    /// Another holder's token is current: "did nothing", known fate.
    LeaseLost,
}

/// The probe's verdict on an unmatched fenced verb: absent → `Gone`; present with the SAME token →
/// `Gone` (a concurrent delete committed after the statement snapshot); present with another token →
/// `LeaseLost`.
pub fn probe_verdict(probe: Option<Token>, token: Token) -> NoMatch {
    match probe {
        None => NoMatch::Gone,
        Some(current) if current == token => NoMatch::Gone,
        Some(_) => NoMatch::LeaseLost,
    }
}

fn int(v: &Value) -> Result<i64, Malformed> {
    match v {
        Value::I64(n) => Ok(*n),
        _ => Err(Malformed),
    }
}

/// The probe's two cells: both NULL = no row; otherwise the row's token.
fn probe_cells(attempts: &Value, created_at: &Value) -> Result<Option<Token>, Malformed> {
    match (attempts, created_at) {
        (Value::Null, Value::Null) => Ok(None),
        (a, c) => {
            let a = i16::try_from(int(a)?).map_err(|_| Malformed)?;
            let c = i32::try_from(int(c)?).map_err(|_| Malformed)?;
            Ok(Some(Token::from_pg(c, a)))
        }
    }
}

/// ENQUEUE's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enqueued {
    /// Set iff exactly one job was enqueued.
    pub job_id: Option<JobId>,
    pub inserted: u32,
}

/// Decode [`enqueue`]'s result for a request of `jobs` jobs: one job → its one `RETURNING id` row; a
/// batch → the command tag's count, which must be the batch size (the INSERT is atomic).
pub fn decode_enqueue(
    rows: &[Vec<Value>],
    affected: u64,
    jobs: usize,
) -> Result<Enqueued, Malformed> {
    if jobs == 1 {
        return match rows {
            [row] => match row.as_slice() {
                [id] => Ok(Enqueued {
                    job_id: Some(JobId(int(id)?)),
                    inserted: 1,
                }),
                _ => Err(Malformed),
            },
            _ => Err(Malformed),
        };
    }
    if !rows.is_empty() || affected != jobs as u64 {
        return Err(Malformed);
    }
    Ok(Enqueued {
        job_id: None,
        inserted: u32::try_from(jobs).map_err(|_| Malformed)?,
    })
}

/// One reserved job, as [`reserve`]'s `RETURNING` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reserved {
    pub id: JobId,
    /// The token minted by THIS reservation: the incremented `attempts` and the row's `created_at`.
    pub token: Token,
    pub attempts: u32,
    pub queue: String,
    pub payload: String,
    pub created_at: i64,
    pub lease_deadline: i64,
}

/// Decode [`reserve`]'s rows, sorted by id: `UPDATE … RETURNING` reports rows in no promised order,
/// and the queue is served best-effort FIFO by id (§24.7). More rows than `limit` is malformed — the
/// rescan premise says it cannot happen, and this refuses to deliver it if it ever does.
pub fn decode_reserve(rows: &[Vec<Value>], limit: u16) -> Result<Vec<Reserved>, Malformed> {
    if rows.len() > usize::from(limit) {
        return Err(Malformed);
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let [
            id,
            attempts,
            created_at,
            Value::Text(queue),
            Value::Text(payload),
            deadline,
        ] = row.as_slice()
        else {
            return Err(Malformed);
        };
        let a = i16::try_from(int(attempts)?).map_err(|_| Malformed)?;
        let c = i32::try_from(int(created_at)?).map_err(|_| Malformed)?;
        if a < 1 {
            return Err(Malformed);
        }
        out.push(Reserved {
            id: JobId(int(id)?),
            token: Token::from_pg(c, a),
            attempts: u32::try_from(a).map_err(|_| Malformed)?,
            queue: queue.clone(),
            payload: payload.clone(),
            created_at: i64::from(c),
            lease_deadline: int(deadline)?,
        });
    }
    out.sort_by_key(|r| r.id.0);
    if out.windows(2).any(|w| w[0].id == w[1].id) {
        return Err(Malformed);
    }
    Ok(out)
}

/// ACK's outcome: `Ok(())` acked, `Err(NoMatch)` otherwise.
pub fn decode_ack(rows: &[Vec<Value>], token: Token) -> Result<Result<(), NoMatch>, Malformed> {
    let [row] = rows else { return Err(Malformed) };
    let [deleted, attempts, created_at] = row.as_slice() else {
        return Err(Malformed);
    };
    match int(deleted)? {
        1 => Ok(Ok(())),
        0 => Ok(Err(probe_verdict(
            probe_cells(attempts, created_at)?,
            token,
        ))),
        // `id` is verified unique, so the fence deletes at most one row.
        _ => Err(Malformed),
    }
}

/// RELEASE's outcome: `Ok(new id)` released, `Err(NoMatch)` otherwise.
pub fn decode_release(
    rows: &[Vec<Value>],
    token: Token,
) -> Result<Result<JobId, NoMatch>, Malformed> {
    let [row] = rows else { return Err(Malformed) };
    let [new_id, attempts, created_at] = row.as_slice() else {
        return Err(Malformed);
    };
    match new_id {
        Value::Null => Ok(Err(probe_verdict(
            probe_cells(attempts, created_at)?,
            token,
        ))),
        v => Ok(Ok(JobId(int(v)?))),
    }
}

/// EXTEND's outcome: `Some(lease_deadline)`, or `None` when the fence matched nothing (`LeaseLost`:
/// EXTEND has no `gone`, §24.4).
pub fn decode_extend(rows: &[Vec<Value>]) -> Result<Option<i64>, Malformed> {
    match rows {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [deadline] => Ok(Some(int(deadline)?)),
            _ => Err(Malformed),
        },
        _ => Err(Malformed),
    }
}

/// SIZE's counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sizes {
    pub pending: u64,
    pub delayed: u64,
    pub reserved: u64,
    pub oldest_pending_at: Option<i64>,
}

pub fn decode_size(rows: &[Vec<Value>]) -> Result<Sizes, Malformed> {
    let [row] = rows else { return Err(Malformed) };
    let [pending, delayed, reserved, oldest] = row.as_slice() else {
        return Err(Malformed);
    };
    let count = |v: &Value| u64::try_from(int(v)?).map_err(|_| Malformed);
    Ok(Sizes {
        pending: count(pending)?,
        delayed: count(delayed)?,
        reserved: count(reserved)?,
        oldest_pending_at: match oldest {
            Value::Null => None,
            v => Some(int(v)?),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> TableName {
        TableName::parse("app.ferro_jobs").unwrap()
    }

    fn job(q: &str, p: &str, d: u32) -> EnqueueJob {
        EnqueueJob {
            queue: q.into(),
            payload: p.into(),
            delay_s: d,
        }
    }

    fn i(n: i64) -> Value {
        Value::I64(n)
    }

    fn txt(s: &str) -> Value {
        Value::Text(s.into())
    }

    /// Every statement computes `now` from statement time, once, and quotes the table.
    #[test]
    fn every_statement_reads_the_database_clock_once_and_quotes_the_table() {
        assert_eq!(
            NOW_CTE,
            "n AS MATERIALIZED (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint AS s)"
        );
        let id = JobId(7);
        let tok = Token::from_pg(100, 2);
        let with_now = [
            enqueue(&t(), vec![job("q", "p", 0)]),
            reserve(&t(), "q", 90, 1024, 1),
            release(&t(), id, tok, 0),
            extend(&t(), id, tok, 90),
            size(&t(), "q"),
        ];
        for s in &with_now {
            assert!(s.sql.starts_with(&format!("WITH {NOW_CTE}")), "{}", s.sql);
            assert_eq!(s.sql.matches("statement_timestamp()").count(), 1);
            assert!(!s.sql.contains("clock_timestamp"));
            assert!(!s.sql.contains("now()"));
        }
        for s in with_now
            .iter()
            .chain([&ack(&t(), id, tok), &clear(&t(), "q")])
        {
            assert!(s.sql.contains("\"app\".\"ferro_jobs\""), "{}", s.sql);
            assert!(!s.sql.contains(" app.ferro_jobs"), "{}", s.sql);
        }
    }

    #[test]
    fn enqueue_is_one_ordered_insert_with_the_delay_rule() {
        let s = enqueue(&t(), vec![job("a", "{1}", 0)]);
        assert_eq!(
            s.sql,
            "WITH n AS MATERIALIZED (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint \
             AS s) INSERT INTO \"app\".\"ferro_jobs\" (queue, payload, attempts, reserved_at, \
             available_at, created_at) SELECT v.q, v.p, 0, NULL::integer, CASE WHEN v.d = 0 THEN n.s \
             ELSE n.s + 1 + v.d END, n.s FROM n, (VALUES ($1::text, $2::text, $3::bigint, 0)) AS \
             v(q, p, d, o) ORDER BY v.o RETURNING id"
        );
        assert_eq!(s.params, vec![txt("a"), txt("{1}"), i(0)]);
        let s = enqueue(
            &t(),
            vec![job("a", "x", 0), job("b", "y", 5), job("c", "z", u32::MAX)],
        );
        assert!(
            s.sql.contains(
                "(VALUES ($1::text, $2::text, $3::bigint, 0), ($4::text, $5::text, $6::bigint, 1), \
                 ($7::text, $8::text, $9::bigint, 2))"
            ),
            "{}",
            s.sql
        );
        assert!(s.sql.ends_with("ORDER BY v.o"), "a batch returns no ids");
        assert_eq!(
            s.params,
            vec![
                txt("a"),
                txt("x"),
                i(0),
                txt("b"),
                txt("y"),
                i(5),
                txt("c"),
                txt("z"),
                i(i64::from(u32::MAX))
            ]
        );
    }

    #[test]
    fn reserve_is_the_spec_statement_with_bound_rules() {
        let s = reserve(&t(), "emails", 90, 4096, 3);
        assert_eq!(
            s.sql,
            "WITH n AS MATERIALIZED (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint \
             AS s), c AS MATERIALIZED (SELECT t.id FROM \"app\".\"ferro_jobs\" AS t, n WHERE t.queue \
             = $1 AND t.attempts < $2::smallint AND octet_length(t.payload) <= $3::integer AND \
             ((t.reserved_at IS NULL AND t.available_at <= n.s) OR t.reserved_at < n.s - \
             $4::bigint) ORDER BY t.id LIMIT $5::bigint FOR UPDATE OF t SKIP LOCKED) UPDATE \
             \"app\".\"ferro_jobs\" AS t SET reserved_at = n.s, attempts = t.attempts + 1 FROM c, n \
             WHERE t.id = c.id RETURNING t.id, t.attempts, t.created_at, t.queue, t.payload, n.s + \
             $4::bigint + 1"
        );
        assert_eq!(
            s.params,
            vec![txt("emails"), i(32_767), i(4096), i(90), i(3)]
        );
    }

    #[test]
    fn the_fenced_statements_match_exactly_id_attempts_and_created_at() {
        let tok = Token::from_pg(-5, -2);
        let fence = "id = $1::bigint AND attempts = $2::smallint AND created_at = $3::integer";
        let a = ack(&t(), JobId(9), tok);
        assert_eq!(
            a.sql,
            format!(
                "WITH d AS (DELETE FROM \"app\".\"ferro_jobs\" WHERE {fence} RETURNING 1) SELECT \
                 (SELECT count(*) FROM d), p.attempts, p.created_at FROM (SELECT 1) AS one LEFT \
                 JOIN \"app\".\"ferro_jobs\" AS p ON p.id = $1::bigint"
            )
        );
        assert_eq!(a.params, vec![i(9), i(-2), i(-5)]);
        let r = release(&t(), JobId(9), tok, 30);
        assert_eq!(
            r.sql,
            format!(
                "WITH {NOW_CTE}, d AS (DELETE FROM \"app\".\"ferro_jobs\" WHERE {fence} RETURNING \
                 queue, payload, attempts), i AS (INSERT INTO \"app\".\"ferro_jobs\" (queue, \
                 payload, attempts, reserved_at, available_at, created_at) SELECT d.queue, \
                 d.payload, d.attempts, NULL::integer, CASE WHEN $4::bigint = 0 THEN n.s ELSE n.s + \
                 1 + $4::bigint END, n.s FROM d, n RETURNING id) SELECT (SELECT i.id FROM i), \
                 p.attempts, p.created_at FROM (SELECT 1) AS one LEFT JOIN \"app\".\"ferro_jobs\" AS \
                 p ON p.id = $1::bigint"
            )
        );
        assert_eq!(r.params, vec![i(9), i(-2), i(-5), i(30)]);
        let e = extend(&t(), JobId(9), tok, 45);
        assert_eq!(
            e.sql,
            format!(
                "WITH {NOW_CTE} UPDATE \"app\".\"ferro_jobs\" AS t SET reserved_at = n.s FROM n \
                 WHERE {fence} RETURNING n.s + $4::bigint + 1"
            )
        );
        assert_eq!(e.params, vec![i(9), i(-2), i(-5), i(45)]);
        // The fence never reads `reserved_at`: a token outlives its lease until someone re-leases.
        for s in [&a, &r, &e] {
            let after = s.sql.split(" WHERE ").nth(1).unwrap();
            let where_ = after.split(" RETURNING ").next().unwrap();
            assert_eq!(where_, fence, "{}", s.sql);
        }
    }

    #[test]
    fn size_and_clear_follow_stock_definitions() {
        let s = size(&t(), "q");
        assert_eq!(
            s.sql,
            format!(
                "WITH {NOW_CTE} SELECT count(*) FILTER (WHERE t.reserved_at IS NULL AND \
                 t.available_at <= n.s), count(*) FILTER (WHERE t.reserved_at IS NULL AND \
                 t.available_at > n.s), count(*) FILTER (WHERE t.reserved_at IS NOT NULL), \
                 min(t.available_at) FILTER (WHERE t.reserved_at IS NULL AND t.available_at <= n.s) \
                 FROM n LEFT JOIN \"app\".\"ferro_jobs\" AS t ON t.queue = $1"
            )
        );
        assert_eq!(s.params, vec![txt("q")]);
        let c = clear(&t(), "q");
        assert_eq!(c.sql, "DELETE FROM \"app\".\"ferro_jobs\" WHERE queue = $1");
        assert_eq!(c.params, vec![txt("q")]);
    }

    #[test]
    fn the_probe_answers_gone_or_lease_lost() {
        let tok = Token::from_pg(100, 2);
        assert_eq!(probe_verdict(None, tok), NoMatch::Gone);
        assert_eq!(probe_verdict(Some(tok), tok), NoMatch::Gone);
        assert_eq!(
            probe_verdict(Some(Token::from_pg(100, 3)), tok),
            NoMatch::LeaseLost
        );
        assert_eq!(
            probe_verdict(Some(Token::from_pg(101, 2)), tok),
            NoMatch::LeaseLost
        );
    }

    #[test]
    fn ack_and_release_results_decode_through_the_probe() {
        let tok = Token::from_pg(100, 2);
        assert_eq!(decode_ack(&[vec![i(1), i(2), i(100)]], tok), Ok(Ok(())));
        assert_eq!(
            decode_ack(&[vec![i(0), Value::Null, Value::Null]], tok),
            Ok(Err(NoMatch::Gone))
        );
        assert_eq!(
            decode_ack(&[vec![i(0), i(2), i(100)]], tok),
            Ok(Err(NoMatch::Gone))
        );
        assert_eq!(
            decode_ack(&[vec![i(0), i(3), i(100)]], tok),
            Ok(Err(NoMatch::LeaseLost))
        );
        assert_eq!(decode_ack(&[vec![i(2), i(2), i(100)]], tok), Err(Malformed));
        assert_eq!(decode_ack(&[], tok), Err(Malformed));
        assert_eq!(
            decode_ack(&[vec![i(0), i(2), Value::Null]], tok),
            Err(Malformed)
        );
        assert_eq!(
            decode_ack(&[vec![i(0), i(70_000), i(100)]], tok),
            Err(Malformed)
        );

        assert_eq!(
            decode_release(&[vec![i(55), i(2), i(100)]], tok),
            Ok(Ok(JobId(55)))
        );
        assert_eq!(
            decode_release(&[vec![Value::Null, Value::Null, Value::Null]], tok),
            Ok(Err(NoMatch::Gone))
        );
        assert_eq!(
            decode_release(&[vec![Value::Null, i(2), i(100)]], tok),
            Ok(Err(NoMatch::Gone))
        );
        assert_eq!(
            decode_release(&[vec![Value::Null, i(2), i(99)]], tok),
            Ok(Err(NoMatch::LeaseLost))
        );
        assert_eq!(decode_release(&[], tok), Err(Malformed));
    }

    #[test]
    fn reserve_rows_decode_sorted_with_their_minted_tokens() {
        let row = |id: i64, a: i64| vec![i(id), i(a), i(500), txt("q"), txt("p"), i(1091)];
        let got = decode_reserve(&[row(9, 1), row(3, 4)], 2).unwrap();
        assert_eq!(
            got.iter().map(|r| r.id.0).collect::<Vec<_>>(),
            vec![3, 9],
            "FIFO by id"
        );
        assert_eq!(got[0].token, Token::from_pg(500, 4));
        assert_eq!(got[0].attempts, 4);
        assert_eq!(got[0].created_at, 500);
        assert_eq!(got[0].lease_deadline, 1091);
        assert_eq!(
            decode_reserve(&[row(1, 1), row(2, 1)], 1),
            Err(Malformed),
            "more than the LIMIT"
        );
        assert_eq!(
            decode_reserve(&[row(1, 1), row(1, 1)], 2),
            Err(Malformed),
            "a row twice"
        );
        assert_eq!(decode_reserve(&[row(1, 0)], 1), Err(Malformed));
        assert_eq!(decode_reserve(&[], 1), Ok(vec![]));
    }

    #[test]
    fn enqueue_extend_and_size_results_decode() {
        assert_eq!(
            decode_enqueue(&[vec![i(41)]], 1, 1),
            Ok(Enqueued {
                job_id: Some(JobId(41)),
                inserted: 1
            })
        );
        assert_eq!(
            decode_enqueue(&[], 3, 3),
            Ok(Enqueued {
                job_id: None,
                inserted: 3
            })
        );
        assert_eq!(decode_enqueue(&[], 2, 3), Err(Malformed));
        assert_eq!(decode_enqueue(&[], 1, 1), Err(Malformed));
        assert_eq!(decode_extend(&[]), Ok(None));
        assert_eq!(decode_extend(&[vec![i(10)]]), Ok(Some(10)));
        assert_eq!(decode_extend(&[vec![i(1)], vec![i(2)]]), Err(Malformed));
        assert_eq!(
            decode_size(&[vec![i(1), i(2), i(3), i(77)]]),
            Ok(Sizes {
                pending: 1,
                delayed: 2,
                reserved: 3,
                oldest_pending_at: Some(77)
            })
        );
        assert_eq!(
            decode_size(&[vec![i(0), i(0), i(0), Value::Null]]).map(|s| s.oldest_pending_at),
            Ok(None)
        );
        assert_eq!(
            decode_size(&[vec![i(-1), i(0), i(0), Value::Null]]),
            Err(Malformed)
        );
    }
}
