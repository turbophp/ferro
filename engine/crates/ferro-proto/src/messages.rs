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
    /// `server_version` is `nil` when the engine has not learned it — a pool whose backend was
    /// unreachable at handshake time. The handshake never depends on backend availability. M1-S8a
    /// Task 11 emits `None` unconditionally; Task 12 is what fills it.
    ///
    /// Still NEVER exposed: the DSN (§12 server secret).
    PoolInfo { name: String, kind: String, server_version: Option<String> }
);

msg!(HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<PoolInfo>, type_registry_hash: String });
msg!(Ping { token: u64 });
msg!(Pong { token: u64 });
msg!(Goodbye {});
msg!(WindowUpdate {
    frames: u32,
    bytes: u32
});
msg!(ErrorPayload {
    code: u16, branch: u8, sqlstate: Option<String>, errno: Option<i32>,
    message: String, detail: Option<String>, retry_after_ms: Option<u32>
});

/// TX-service messages (`Value`-free) ride the `msg!`/rmp-serde path, so `tx` is declared AFTER the
/// `msg!` definition above — a `macro_rules!` macro is in textual scope only for modules that follow
/// it. (`sql` sits at the top of this file and cannot use `msg!`, which is why its codec is hand-rolled.)
pub mod tx;
pub use tx::{BeginRequest, BeginResponse, Isolation, SavepointRequest, TxControl};

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
    fn trailing_bytes_rejected() {
        let mut b = Ping { token: 7 }.encode();
        b.push(0xff);
        match Ping::decode(&b) {
            Err(CodecError::TrailingBytes(1)) => {}
            other => panic!("expected TrailingBytes(1), got {other:?}"),
        }
    }
}
