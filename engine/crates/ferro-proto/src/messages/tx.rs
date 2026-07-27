//! TX-service wire messages (service `TX`, methods `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/
//! `RELEASE`/`ROLLBACK_TO`). All are `Value`-free, so they ride the same `msg!`/rmp-serde compact
//! positional layout as the core messages — a fixarray of their fields in declaration order, with
//! `Option<T>` present as bare `nil` when absent (never omitted). `BeginResponse` is the terminal
//! `Outcome::Ok` body for `BEGIN` and composes via `Outcome::Ok(BeginResponse.encode())` exactly as
//! `ExecOk` does, because `encode()` is one complete top-level MessagePack value.
//!
//! `tx_id` is a monotonic, never-reused counter, contractually **bounded < 2^63** (SPEC §7) — NOT a
//! full-range/`boot_epoch`-style u64 — so it is a native PHP int on the client side (no decimal-string
//! treatment; see `/proto/PROTOCOL.md` §2). Layouts are pinned in `/proto/PROTOCOL.md` §9 and locked
//! by the `tx_*` golden vectors.

use super::{from_slice, to_vec};
use crate::CodecError;
use serde::{Deserialize, Serialize};

/// Transaction isolation level. Carried on the wire as a `u8` in [`BeginRequest::isolation`] — it is
/// a message-field VALUE, not a `/proto` registry constant (isolation is neither a method id, flag,
/// error code, nor type tag — charter rule 2's source-of-truth scope), so it is defined here in code
/// and documented in `/proto/PROTOCOL.md` §9, never in `methods.toml`/`registry.lock.json`.
///
/// The `u8` mapping is fixed: `ReadCommitted = 0`, `RepeatableRead = 1`, `Serializable = 2`. There is
/// no fourth value: PostgreSQL's `READ UNCOMMITTED` is an alias for `READ COMMITTED` (SPEC §7), so it
/// maps to `ReadCommitted`; the engine never emits a distinct `READ UNCOMMITTED` level.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    ReadCommitted = 0,
    RepeatableRead = 1,
    Serializable = 2,
}

impl From<Isolation> for u8 {
    fn from(v: Isolation) -> u8 {
        v as u8
    }
}

impl TryFrom<u8> for Isolation {
    type Error = CodecError;
    fn try_from(v: u8) -> Result<Isolation, CodecError> {
        match v {
            0 => Ok(Isolation::ReadCommitted),
            1 => Ok(Isolation::RepeatableRead),
            2 => Ok(Isolation::Serializable),
            other => Err(CodecError::Malformed(format!(
                "unknown isolation u8 {other} (0=ReadCommitted, 1=RepeatableRead, 2=Serializable)"
            ))),
        }
    }
}

msg!(BeginRequest {
    pool: String,
    isolation: Option<u8>,
    readonly: bool
});
msg!(BeginResponse { tx_id: u64 });
msg!(TxControl { tx_id: u64 });
msg!(SavepointRequest {
    tx_id: u64,
    name: Option<String>
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodecError;

    #[test]
    fn isolation_u8_enum_roundtrip() {
        for iso in [
            Isolation::ReadCommitted,
            Isolation::RepeatableRead,
            Isolation::Serializable,
        ] {
            let n: u8 = iso.into();
            assert_eq!(Isolation::try_from(n).unwrap(), iso);
        }
        assert_eq!(u8::from(Isolation::ReadCommitted), 0);
        assert_eq!(u8::from(Isolation::RepeatableRead), 1);
        assert_eq!(u8::from(Isolation::Serializable), 2);
        // No 4th value; anything >= 3 is rejected (not silently coerced).
        assert!(matches!(
            Isolation::try_from(3),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn begin_request_roundtrip_some_and_none_isolation() {
        let with = BeginRequest {
            pool: "main".into(),
            isolation: Some(Isolation::Serializable.into()),
            readonly: true,
        };
        assert_eq!(BeginRequest::decode(&with.encode()).unwrap(), with);

        let without = BeginRequest {
            pool: "ro".into(),
            isolation: None,
            readonly: false,
        };
        assert_eq!(BeginRequest::decode(&without.encode()).unwrap(), without);
    }

    #[test]
    fn begin_response_is_one_value_and_composes_with_outcome_ok() {
        use crate::messages::Outcome;
        let resp = BeginResponse { tx_id: 42 };
        let body = resp.encode();
        // A one-field msg encodes to a single well-formed top-level value (fixarray(1) + the tx_id).
        assert_eq!(body[0], 0x91, "BeginResponse is a fixarray(1)");
        assert_eq!(BeginResponse::decode(&body).unwrap(), resp);
        // Composes as the terminal Outcome::Ok body, mirroring ExecOk.
        let outcome = Outcome::Ok(body.clone());
        match Outcome::decode(&outcome.encode()).unwrap() {
            Outcome::Ok(recovered) => {
                assert_eq!(recovered, body);
                assert_eq!(BeginResponse::decode(&recovered).unwrap(), resp);
            }
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }
    }

    #[test]
    fn tx_control_and_savepoint_roundtrip() {
        let ctl = TxControl { tx_id: 7 };
        assert_eq!(TxControl::decode(&ctl.encode()).unwrap(), ctl);

        let named = SavepointRequest {
            tx_id: 7,
            name: Some("sp_1".into()),
        };
        assert_eq!(SavepointRequest::decode(&named.encode()).unwrap(), named);

        // Engine names it when `None`.
        let unnamed = SavepointRequest {
            tx_id: 7,
            name: None,
        };
        assert_eq!(
            SavepointRequest::decode(&unnamed.encode()).unwrap(),
            unnamed
        );
    }

    #[test]
    fn tx_messages_reject_trailing_bytes() {
        let mut b = TxControl { tx_id: 7 }.encode();
        b.push(0xff);
        match TxControl::decode(&b) {
            Err(CodecError::TrailingBytes(1)) => {}
            other => panic!("expected TrailingBytes(1), got {other:?}"),
        }
    }
}
