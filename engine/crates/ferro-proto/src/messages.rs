use crate::CodecError;
use serde::{Deserialize, Serialize};

/// rmp-serde in default (compact) mode encodes a struct as a fixarray of its fields in
/// declaration order — exactly the positional layout PROTOCOL.md pins.
fn to_vec<T: Serialize>(v: &T) -> Vec<u8> {
    rmp_serde::to_vec(v).expect("infallible in-memory encode")
}
fn from_slice<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T, CodecError> {
    rmp_serde::from_slice(b).map_err(|e| CodecError::Malformed(e.to_string()))
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
    Ok(Vec<u8>), // opaque method-specific body bytes
    Error(ErrorPayload),
    Cancelled,
}

impl Outcome {
    pub fn encode(&self) -> Vec<u8> {
        use rmp::encode as e;
        let mut o = Vec::new();
        e::write_array_len(&mut o, 2).unwrap();
        match self {
            Outcome::Ok(body) => {
                e::write_pfix(&mut o, 0).unwrap();
                // body is raw msgpack already; splice it in
                o.extend_from_slice(body);
            }
            Outcome::Error(ep) => {
                e::write_pfix(&mut o, 1).unwrap();
                o.extend_from_slice(&ep.encode());
            }
            Outcome::Cancelled => {
                e::write_pfix(&mut o, 2).unwrap();
                e::write_nil(&mut o).unwrap();
            }
        }
        o
    }
    pub fn decode(b: &[u8]) -> Result<Outcome, CodecError> {
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
            0 => Ok(Outcome::Ok(rd.to_vec())),
            1 => Ok(Outcome::Error(ErrorPayload::decode(rd)?)),
            2 => Ok(Outcome::Cancelled),
            s => Err(CodecError::Malformed(format!("unknown outcome status {s}"))),
        }
    }
}
