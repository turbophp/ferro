//! SQL-service wire messages (service `SQL`, method `EXEC`) — a **bespoke positional codec**.
//!
//! `ExecRequest` and the terminal `ExecOk` body carry [`Value`]s (`params`, `rows`,
//! `last_insert_id`). `Value` derives only `Debug/Clone/PartialEq` (no `Serialize/Deserialize`, and
//! `F64` forbids `Eq`), and the `msg!` macro forces `Eq/Serialize/Deserialize` — so these messages
//! CANNOT ride the `msg!`/rmp-serde path. They use a hand-written codec that splices
//! `Value::encode`/`Value::decode` per element, exactly as `Outcome::Ok` splices raw body bytes.
//! `ColMeta`/`Stats` are `Value`-free and use the same rmp-serde compact helpers (`to_vec`/
//! `from_slice`) that `msg!` expands to.
//!
//! Decode safety (mirrors `value.rs`):
//! - every variable-length array calls [`bound_len`] before `Vec::with_capacity` (a MessagePack
//!   element is ≥1 byte, so a declared length > remaining bytes is provably a lie — this closes the
//!   `u32::MAX`-array unbounded-alloc hole);
//! - `ExecOk::decode` reads each `ColMeta` positionally off an advancing cursor via
//!   [`ColMeta::decode_at`], NEVER `ColMeta::decode` (whose whole-slice `from_slice` rejects the
//!   trailing bytes that always follow a col mid-buffer);
//! - `Option<Value>` (`last_insert_id`) is discriminated by peeking the first byte: bare `nil`
//!   (`0xc0`) ⇒ `None`, else `Value::decode` — unambiguous because `Value::Null` encodes as the
//!   fixarray `[NULL, nil]` (first byte `0x92`), never bare `0xc0`.
//!
//! The exact byte layout is pinned in `/proto/PROTOCOL.md` §8 and locked by the `sql_exec_*` golden
//! vectors plus the PHP byte-match. The two languages mirror BYTES, not decode structure.

use super::{from_slice, to_vec};
use crate::CodecError;
use crate::value::{Value, bound_len, read_bool, read_str};
use rmp::decode as dec;
use rmp::encode as enc;
use serde::{Deserialize, Serialize};

/// `EXEC` request (service `SQL`, method `EXEC` = 1) — client → server. A positional fixarray of 7
/// fields in declaration order (`/proto/PROTOCOL.md` §8.1).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecRequest {
    pub pool: String,
    pub sql: Option<String>,
    pub query_id: Option<String>,
    pub params: Vec<Value>,
    pub timeout_ms: Option<u32>,
    pub readonly: bool,
    /// 0 = rows, 1 = none (affected only), 2 = stream (reserved; rejected `Unsupported` in M0).
    pub fetch: u8,
}

impl ExecRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc::write_array_len(&mut out, 7).unwrap();
        enc::write_str(&mut out, &self.pool).unwrap();
        write_opt_str(&mut out, &self.sql);
        write_opt_str(&mut out, &self.query_id);
        enc::write_array_len(&mut out, self.params.len() as u32).unwrap();
        for v in &self.params {
            v.encode(&mut out);
        }
        write_opt_u32(&mut out, &self.timeout_ms);
        enc::write_bool(&mut out, self.readonly).unwrap();
        // fetch is unsigned; write_uint narrows 0/1/2 to a positive fixint (mirrors PHP packUint).
        enc::write_uint(&mut out, self.fetch as u64).unwrap();
        out
    }

    pub fn decode(b: &[u8]) -> Result<ExecRequest, CodecError> {
        let mut rd: &[u8] = b;
        let n = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("ExecRequest array: {e:?}")))?;
        if n != 7 {
            return Err(CodecError::Malformed(format!("ExecRequest len {n} != 7")));
        }
        let pool = read_str(&mut rd)?;
        let sql = read_opt_str(&mut rd)?;
        let query_id = read_opt_str(&mut rd)?;
        let params_len = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("params array: {e:?}")))?
            as usize;
        bound_len(params_len, rd.len())?; // MAJOR-v2a: bound BEFORE with_capacity
        let mut params = Vec::with_capacity(params_len);
        for _ in 0..params_len {
            params.push(Value::decode(&mut rd)?);
        }
        let timeout_ms = read_opt_u32(&mut rd)?;
        let readonly = read_bool(&mut rd)?;
        let fetch: u8 =
            dec::read_int(&mut rd).map_err(|e| CodecError::Malformed(format!("fetch: {e:?}")))?;
        if !rd.is_empty() {
            return Err(CodecError::TrailingBytes(rd.len()));
        }
        Ok(ExecRequest {
            pool,
            sql,
            query_id,
            params,
            timeout_ms,
            readonly,
            fetch,
        })
    }
}

/// A result column's name + canonical TypedValue tag. `Value`-free, so it uses the same rmp-serde
/// compact layout (`[name, tag]`) as `msg!`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColMeta {
    pub name: String,
    pub tag: u8,
}

impl ColMeta {
    pub fn encode(&self) -> Vec<u8> {
        to_vec(self)
    }
    /// Whole-slice decode (rejects trailing bytes). Usable only when a `ColMeta` is the ENTIRE
    /// buffer — NOT inside `ExecOk`, where a col is always followed by more bytes. Use
    /// [`ColMeta::decode_at`] there.
    pub fn decode(b: &[u8]) -> Result<ColMeta, CodecError> {
        from_slice(b)
    }
    /// Cursor-positional decode for use INSIDE a larger buffer (`ExecOk`). Reads `[name, tag]` off
    /// an advancing cursor, leaving trailing bytes for the caller to keep decoding.
    pub fn decode_at(rd: &mut &[u8]) -> Result<ColMeta, CodecError> {
        let n = dec::read_array_len(rd)
            .map_err(|e| CodecError::Malformed(format!("ColMeta array: {e:?}")))?;
        if n != 2 {
            return Err(CodecError::Malformed(format!("ColMeta len {n} != 2")));
        }
        let name = read_str(rd)?;
        let tag: u8 =
            dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("ColMeta tag: {e:?}")))?;
        Ok(ColMeta { name, tag })
    }
}

/// Per-request timing/size accounting (SPEC §6/§16). `Value`-free → rmp-serde compact `[queue_us,
/// exec_us, rows, bytes]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub queue_us: u64,
    pub exec_us: u64,
    pub rows: u64,
    pub bytes: u64,
}

/// `Stats`/`affected`/`ExecOk` u64 fields are contractually bounded < 2^63 in the M0 domain (rows
/// affected, µs timings, and frame-bounded byte counts cannot approach 2^63) — unlike
/// `HelloAck.boot_epoch`, which is a random full-range u64 preserved as a decimal string in PHP.
/// This tripwire catches a future field that outgrows the bound before the PHP `(int)` cast would
/// silently truncate it (see PROTOCOL.md §2). Debug-only: never a release-mode panic on the wire.
const U64_WIRE_BOUND: u64 = 1 << 63;

impl Stats {
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.queue_us < U64_WIRE_BOUND
                && self.exec_us < U64_WIRE_BOUND
                && self.rows < U64_WIRE_BOUND
                && self.bytes < U64_WIRE_BOUND,
            "Stats u64 fields are contractually bounded < 2^63 (PHP int limit); got {self:?}"
        );
        to_vec(self)
    }
    pub fn decode(b: &[u8]) -> Result<Stats, CodecError> {
        from_slice(b)
    }
}

/// The terminal `Outcome::Ok` body for `EXEC`. A positional fixarray of 5 fields; `cols`/`rows`
/// splice `ColMeta::encode()`/`Value::encode()` cells (concatenation is well-formed). It composes
/// with `Outcome::Ok` because `ExecOk::encode()` is exactly one top-level MessagePack value.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecOk {
    pub cols: Vec<ColMeta>,
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
    pub last_insert_id: Option<Value>,
    pub stats: Stats,
}

impl ExecOk {
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.affected < U64_WIRE_BOUND,
            "ExecOk.affected is contractually bounded < 2^63 (PHP int limit); got {}",
            self.affected
        );
        let mut out = Vec::new();
        enc::write_array_len(&mut out, 5).unwrap();
        // cols: array of ColMeta [name, tag]
        enc::write_array_len(&mut out, self.cols.len() as u32).unwrap();
        for c in &self.cols {
            out.extend_from_slice(&c.encode());
        }
        // rows: array of (array of Value cells)
        enc::write_array_len(&mut out, self.rows.len() as u32).unwrap();
        for row in &self.rows {
            enc::write_array_len(&mut out, row.len() as u32).unwrap();
            for v in row {
                v.encode(&mut out);
            }
        }
        enc::write_uint(&mut out, self.affected).unwrap();
        write_opt_value(&mut out, &self.last_insert_id);
        out.extend_from_slice(&self.stats.encode());
        out
    }

    pub fn decode(b: &[u8]) -> Result<ExecOk, CodecError> {
        let mut rd: &[u8] = b;
        let top = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("ExecOk array: {e:?}")))?;
        if top != 5 {
            return Err(CodecError::Malformed(format!("ExecOk len {top} != 5")));
        }
        // cols
        let ncols = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("cols array: {e:?}")))?
            as usize;
        bound_len(ncols, rd.len())?; // MAJOR-v2a
        let mut cols = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            cols.push(ColMeta::decode_at(&mut rd)?); // NEVER ColMeta::decode (trailing bytes follow)
        }
        // rows (outer)
        let nrows = dec::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("rows array: {e:?}")))?
            as usize;
        bound_len(nrows, rd.len())?; // MAJOR-v2a
        let mut rows = Vec::with_capacity(nrows);
        for _ in 0..nrows {
            let ncell = dec::read_array_len(&mut rd)
                .map_err(|e| CodecError::Malformed(format!("row array: {e:?}")))?
                as usize;
            bound_len(ncell, rd.len())?; // MAJOR-v2a: bound each inner row too
            let mut row = Vec::with_capacity(ncell);
            for _ in 0..ncell {
                row.push(Value::decode(&mut rd)?);
            }
            rows.push(row);
        }
        let affected: u64 = dec::read_int(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("affected: {e:?}")))?;
        let last_insert_id = read_opt_value(&mut rd)?;
        // Stats is the final field, so the remaining slice is exactly its bytes. `from_slice`
        // decodes it AND rejects any trailing bytes — which IS the ExecOk-body trailing-byte check.
        let stats = Stats::decode(rd)?;
        Ok(ExecOk {
            cols,
            rows,
            affected,
            last_insert_id,
            stats,
        })
    }
}

// --- shared Option/peek helpers (one rule, mirrored byte-for-byte in the PHP codec) ---

fn write_opt_str(out: &mut Vec<u8>, v: &Option<String>) {
    match v {
        None => enc::write_nil(out).unwrap(),
        Some(s) => {
            enc::write_str(out, s).unwrap();
        }
    }
}

fn write_opt_u32(out: &mut Vec<u8>, v: &Option<u32>) {
    match v {
        None => enc::write_nil(out).unwrap(),
        Some(n) => {
            enc::write_uint(out, *n as u64).unwrap();
        }
    }
}

fn write_opt_value(out: &mut Vec<u8>, v: &Option<Value>) {
    match v {
        None => enc::write_nil(out).unwrap(),
        Some(val) => val.encode(out),
    }
}

fn read_opt_str(rd: &mut &[u8]) -> Result<Option<String>, CodecError> {
    if peek_nil(rd)? {
        return Ok(None);
    }
    Ok(Some(read_str(rd)?))
}

fn read_opt_u32(rd: &mut &[u8]) -> Result<Option<u32>, CodecError> {
    if peek_nil(rd)? {
        return Ok(None);
    }
    let n: u32 = dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("opt u32: {e:?}")))?;
    Ok(Some(n))
}

fn read_opt_value(rd: &mut &[u8]) -> Result<Option<Value>, CodecError> {
    // Option<Value> peek rule: bare nil (0xc0) ⇒ None; anything else ⇒ Value::decode. Unambiguous
    // because Value::Null encodes as the fixarray [NULL, nil] (first byte 0x92), never bare 0xc0.
    if peek_nil(rd)? {
        return Ok(None);
    }
    Ok(Some(Value::decode(rd)?))
}

/// Peek the next marker; if it is bare `nil` (`0xc0`) consume it and return `true`, else leave the
/// cursor untouched. An empty cursor is truncation, not `None`.
fn peek_nil(rd: &mut &[u8]) -> Result<bool, CodecError> {
    match rd.first() {
        None => Err(CodecError::Truncated { need: 1, have: 0 }),
        Some(&0xc0) => {
            *rd = &rd[1..];
            Ok(true)
        }
        Some(_) => Ok(false),
    }
}
