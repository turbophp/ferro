use crate::CodecError;
use serde::{Deserialize, Serialize};

/// SQL-service messages carry `Value`s, which cannot ride the `msg!`/rmp-serde path, so they live
/// in a submodule with a bespoke positional codec. Declared after `to_vec`/`from_slice` and the
/// `msg!` macro so the Value-free `ColMeta`/`Stats` there can reuse the same rmp-serde helpers.
pub mod sql;
pub use sql::{ColMeta, ExecOk, ExecRequest, Stats, StreamData, StreamHead};

/// rmp-serde in default (compact) mode encodes a struct as a fixarray of its fields in
/// declaration order — exactly the positional layout PROTOCOL.md pins.
pub(crate) fn to_vec<T: Serialize>(v: &T) -> Vec<u8> {
    rmp_serde::to_vec(v).expect("infallible in-memory encode")
}
pub(crate) fn from_slice<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T, CodecError> {
    // `Deserializer::new` over a `&[u8]` reader (rather than `from_slice`/`from_read_ref`)
    // consumes the slice as it decodes, so `get_ref()` afterward yields exactly the
    // unconsumed remainder — letting us reject a payload that smuggles extra bytes past a
    // valid message instead of silently ignoring them.
    let mut de = rmp_serde::Deserializer::new(b);
    let v = T::deserialize(&mut de).map_err(|e| CodecError::Malformed(e.to_string()))?;
    let rest: &[u8] = de.get_ref();
    if !rest.is_empty() {
        return Err(CodecError::TrailingBytes(rest.len()));
    }
    Ok(v)
}

/// Declares one positional wire message. The leading `$(#[$meta:meta])*` capture is what lets a
/// `///` doc comment ride the invocation: without it rustc emits `unused_doc_comments` (a
/// `-D warnings` build failure), because a doc comment written in front of a macro CALL documents
/// nothing — the expansion has to carry it onto the generated struct.
macro_rules! msg {
    ($(#[$meta:meta])* $name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name { $(pub $field: $ty),* }
        impl $name {
            pub fn encode(&self) -> Vec<u8> { to_vec(self) }
            pub fn decode(b: &[u8]) -> Result<Self, CodecError> { from_slice(b) }
        }
    };
}

msg!(Hello { client_version: u32, type_registry_hash: String, manifest_hash: Option<String>, pid: u32, features: u32 });
msg!(
    /// One pool's advertised metadata (M1-S8a). A positional fixarray of 3, nested inside
    /// `HelloAck.pools`.
    ///
    /// The doc comment lives INSIDE the `msg!` invocation on purpose: a `///` written in FRONT of a
    /// macro call is attached to the invocation item and never enters the macro's token stream, so
    /// it documents nothing and rustc raises `unused_doc_comments` (a `-D warnings` failure).
    ///
    /// `kind` is the backend FAMILY (`"postgres"` / `"mysql"`), which the engine has known since
    /// `PoolRegistry::build` (from the DSN scheme) but never put on the wire. `server_version` is
    /// the backend's own `version()` string, **verbatim and unnormalised** — parsing it into a
    /// platform decision is a client-tier concern (a Doctrine driver needs `mariadb` to appear in
    /// the string for the MariaDB branch, and PG's leading word stripped), and normalising it here
    /// would bake one ecosystem's conventions into the protocol.
    ///
    /// `server_version` is `nil` when the engine has not learned it — never learned (an unreachable
    /// backend), learned and since expired, or not learned YET (a probe that outran the handshake's
    /// bounded budget). All three are one contract to a client: "unknown", never a failure. The
    /// handshake never depends on backend availability. Filled since M1-S8a Task 12, off a lazy,
    /// concurrent, TTL'd per-pool probe in `ferrod`'s `PoolRegistry::pool_info`.
    ///
    /// `literals_are_standard` (M2-C2g) answers ONE question — is a backslash inside a single-quoted
    /// string literal an ORDINARY CHARACTER on this backend? — and is deliberately NOT named for
    /// either family's own setting. A client that must build a SQL literal (`PDO::quote()`, and so
    /// Laravel's `DB::escape()` and every `Builder::toRawSql()`) needs to know whether doubling `'`
    /// is the whole rule; it is, exactly when that property holds. PostgreSQL spells it
    /// `standard_conforming_strings`, MySQL spells it `NO_BACKSLASH_ESCAPES` in `sql_mode`, and
    /// putting either NAME on the wire would bake one backend's vocabulary into the protocol.
    /// **MySQL needs the bit MORE than PostgreSQL does**: PG has defaulted to the safe value since
    /// 9.1, MySQL defaults to the unsafe one.
    ///
    /// It costs no round trip: PG reports the GUC via `ParameterStatus` (the M1-S1 fork mirrors it
    /// in `Client::parameter`), MySQL reports it as an OK-packet status flag, and SQLite's answer is
    /// a constant (M2-C1g); it rides the SAME per-pool probe as `server_version`, inheriting that
    /// probe's `nil` contract exactly. **It describes the PROBED session, so it is informational,
    /// not a quoting rule** (M2-C1g review: a cached pool-level bit can describe a different
    /// session, and a literal built from it broke out) — see `/proto/PROTOCOL.md` §4.
    ///
    /// Still NEVER exposed: the DSN (§12 server secret).
    PoolInfo {
        name: String,
        kind: String,
        server_version: Option<String>,
        literals_are_standard: Option<bool>
    }
);

msg!(HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<PoolInfo>, type_registry_hash: String });
msg!(Ping { token: u64 });
msg!(Pong { token: u64 });
msg!(Goodbye {});
msg!(WindowUpdate {
    frames: u32,
    bytes: u32
});
msg!(
    /// The payload of a frame carrying the `OOB_FD` flag (M3-D3, SPEC §5.1, `/proto/PROTOCOL.md`
    /// §1.1): the frame's real payload was moved into a SEALED memfd passed beside the frame with
    /// `SCM_RIGHTS`, and this positional fixarray of 3 says how to read it back.
    ///
    /// * `fd_index` — which of the fds that arrived with this frame's FIRST BYTE holds the payload.
    ///   The engine attaches exactly one fd per `OOB_FD` frame, so it is always `0`, and a receiver
    ///   refuses any other value rather than guessing.
    /// * `len` — the payload's exact byte length; the memfd's size equals it.
    /// * `encoding` — registry `oob_encoding::*`. `FRAME_PAYLOAD` (the only value) means the memfd
    ///   holds exactly the bytes the frame would otherwise have carried inline.
    OobRef {
        fd_index: u32,
        len: u64,
        encoding: u8
    }
);
msg!(ErrorPayload {
    code: u16, branch: u8, sqlstate: Option<String>, errno: Option<i32>,
    message: String, detail: Option<String>, retry_after_ms: Option<u32>
});

/// TX-service messages (`Value`-free) ride the `msg!`/rmp-serde path, so `tx` is declared AFTER the
/// `msg!` definition above — a `macro_rules!` macro is in textual scope only for modules that follow
/// it. (`sql` sits at the top of this file and cannot use `msg!`, which is why its codec is hand-rolled.)
pub mod tx;
pub use tx::{BeginRequest, BeginResponse, Isolation, SavepointRequest, TxControl};

pub mod admin;
pub use admin::{BackupRequest, BackupResponse};

pub mod copy;
pub use copy::{CopyData, CopyDone, CopyRequest};

/// HTTP-service messages (M6-F2, SPEC §23.5): `Value`-free but `bin`-bearing, so — like `sql` — a
/// hand-rolled positional codec rather than `msg!`.
pub mod http;
pub use http::{
    HttpBody, HttpDecoded, HttpDone, HttpHead, HttpHeaderField, HttpRequest, HttpStats,
};

/// Terminal outcome envelope `[status, body]` (decision W-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `body` MUST be exactly one complete MessagePack value (the method-specific opaque
    /// result) — not zero values, not more than one, and not a partial encoding. `encode`
    /// splices these bytes directly into the outcome array, so a body that is anything else
    /// corrupts the frame for every downstream reader.
    Ok(Vec<u8>), // opaque method-specific body bytes
    Error(ErrorPayload),
    Cancelled,
}

impl Outcome {
    pub fn encode(&self) -> Vec<u8> {
        use crate::consts::outcome;
        use rmp::encode as e;
        let mut o = Vec::new();
        e::write_array_len(&mut o, 2).unwrap();
        match self {
            Outcome::Ok(body) => {
                e::write_pfix(&mut o, outcome::OK).unwrap();
                // body is raw msgpack already; splice it in
                debug_assert!(
                    body.is_empty() || rmp::decode::read_marker(&mut &body[..]).is_ok(),
                    "Outcome::Ok body must be a single complete MessagePack value"
                );
                o.extend_from_slice(body);
            }
            Outcome::Error(ep) => {
                e::write_pfix(&mut o, outcome::ERROR).unwrap();
                o.extend_from_slice(&ep.encode());
            }
            Outcome::Cancelled => {
                e::write_pfix(&mut o, outcome::CANCELLED).unwrap();
                e::write_nil(&mut o).unwrap();
            }
        }
        o
    }
    pub fn decode(b: &[u8]) -> Result<Outcome, CodecError> {
        use crate::consts::outcome;
        use rmp::decode as d;
        let mut rd: &[u8] = b;
        let len = d::read_array_len(&mut rd)
            .map_err(|e| CodecError::Malformed(format!("outcome: {e:?}")))?;
        if len != 2 {
            return Err(CodecError::Malformed(format!("outcome len {len} != 2")));
        }
        let status: u8 =
            d::read_pfix(&mut rd).map_err(|e| CodecError::Malformed(format!("status: {e:?}")))?;
        match status {
            s if s == outcome::OK => Ok(Outcome::Ok(rd.to_vec())),
            s if s == outcome::ERROR => Ok(Outcome::Error(ErrorPayload::decode(rd)?)),
            s if s == outcome::CANCELLED => {
                // Validate the body slot is `nil` rather than silently discarding trailing bytes.
                match d::read_marker(&mut rd)
                    .map_err(|e| CodecError::Malformed(format!("cancelled body: {e:?}")))?
                {
                    rmp::Marker::Null => Ok(Outcome::Cancelled),
                    m => Err(CodecError::Malformed(format!(
                        "cancelled body expected nil, got {m:?}"
                    ))),
                }
            }
            s => Err(CodecError::Malformed(format!("unknown outcome status {s}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oob_ref_is_a_positional_fixarray_of_three_and_roundtrips() {
        let r = OobRef {
            fd_index: 0,
            len: 16_777_218,
            encoding: crate::consts::oob_encoding::FRAME_PAYLOAD,
        };
        let b = r.encode();
        assert_eq!(b[0], 0x93, "OobRef is a fixarray(3)");
        assert_eq!(OobRef::decode(&b).unwrap(), r);
        // A map or a wrong arity is not an OobRef.
        assert!(OobRef::decode(&[0x92, 0x00, 0x01]).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut b = Ping { token: 7 }.encode();
        b.push(0xff);
        match Ping::decode(&b) {
            Err(CodecError::TrailingBytes(1)) => {}
            other => panic!("expected TrailingBytes(1), got {other:?}"),
        }
    }
}
