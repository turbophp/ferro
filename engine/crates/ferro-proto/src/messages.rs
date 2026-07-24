use crate::CodecError;
use serde::{Deserialize, Serialize};

/// SQL-service messages carry `Value`s, which cannot ride the `msg!`/rmp-serde path, so they live
/// in a submodule with a bespoke positional codec. Declared after `to_vec`/`from_slice` and the
/// `msg!` macro so the Value-free `ColMeta`/`Stats` there can reuse the same rmp-serde helpers.
pub mod sql;
pub use sql::{ColMeta, ExecOk, ExecRequest, Stats};

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

macro_rules! msg {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name { $(pub $field: $ty),* }
        impl $name {
            pub fn encode(&self) -> Vec<u8> { to_vec(self) }
            pub fn decode(b: &[u8]) -> Result<Self, CodecError> { from_slice(b) }
        }
    };
}

msg!(Hello { client_version: u32, type_registry_hash: String, manifest_hash: Option<String>, pid: u32, features: u32 });
msg!(HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<String>, type_registry_hash: String });
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
