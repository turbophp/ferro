//! HELLO / HELLO_ACK: the mandatory first exchange on every connection. `Session::run` reads the
//! first frame itself (so it can special-case "not HELLO" as session-fatal before ever touching
//! this module); this module holds the decode/validate/reply logic once that first frame is
//! known to be `core/HELLO`.

use ferro_proto::consts::{TYPE_REGISTRY_HASH, feature_engine, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Hello, HelloAck, PoolInfo};

use crate::epoch::BootEpoch;

use super::codec::{InFrame, OutFrame};
use super::error::SessionError;

/// The engine_version advertised in `HELLO_ACK`. SPEC has not yet defined a real versioning
/// scheme for M0; `1` is a placeholder until it does (not a protocol constant, so it does not
/// belong in the registry).
pub const ENGINE_VERSION: u32 = 1;

/// Whether `frame` is the mandatory first frame: `service=CORE, method=HELLO`.
pub fn is_hello(frame: &InFrame) -> bool {
    frame.header.service == service::CORE && frame.header.method == method_core::HELLO
}

/// Decode the `HELLO` payload and hard-check its `type_registry_hash` against this build's
/// `ferro_proto::consts::TYPE_REGISTRY_HASH`. A decode failure is a protocol fault; a hash
/// mismatch is the dedicated `errc::UNSUPPORTED` session-fatal case (SPEC §5).
///
/// M3-D2d: a `manifest_hash` the client sends must equal `engine_manifest` (this engine's loaded
/// manifest hash) — the same session-fatal refusal, because a client built against another
/// manifest would read different SQL and different `idempotent` declarations under the same ids.
/// A client that sends none makes no claim and is admitted either way.
pub fn validate_hello(
    frame: &InFrame,
    engine_manifest: Option<&str>,
) -> Result<Hello, SessionError> {
    let hello = Hello::decode(&frame.payload)
        .map_err(|e| SessionError::protocol_fatal(format!("malformed HELLO payload: {e}")))?;
    if hello.type_registry_hash != TYPE_REGISTRY_HASH {
        return Err(SessionError::type_registry_mismatch(format!(
            "type_registry_hash mismatch: client sent {:?}, engine is {:?}",
            hello.type_registry_hash, TYPE_REGISTRY_HASH
        )));
    }
    if let Some(client) = hello.manifest_hash.as_deref() {
        match engine_manifest {
            Some(engine) if engine == client => {}
            Some(engine) => {
                return Err(SessionError::type_registry_mismatch(format!(
                    "manifest_hash mismatch: the client was built against manifest {client:?}, \
                     the engine runs {engine:?} (redeploy one side so both carry the same \
                     manifest)"
                )));
            }
            None => {
                return Err(SessionError::type_registry_mismatch(format!(
                    "manifest_hash {client:?} sent, but this engine has no manifest loaded \
                     (FERRO_MANIFEST)"
                )));
            }
        }
    }
    Ok(hello)
}

/// Build the `HELLO_ACK` `OutFrame` replying to `request_id` (the `HELLO` frame's own id, per
/// the wire convention that `HELLO_ACK` echoes it), with `flags=0` — `HELLO_ACK` is a
/// non-terminal core control frame, never a request-bearing terminal (see `session::mod`'s
/// concurrency-model doc comment).
///
/// `pools` is the per-pool metadata (`PoolInfo { name, kind, server_version }`) the client may
/// reference in `ExecRequest.pool`; it is advertised in `HelloAck.pools` so a client discovers both
/// the names and the backend FAMILY from the handshake (PROTOCOL.md §4) instead of probing with a
/// dialect-specific query. Only the name/family/version are exposed — never the DSNs (§12 server
/// secret). Build it with [`crate::pools::PoolRegistry::pool_info`] — the ONE derivation of this
/// list, so the names/kinds a session advertises are always the registry's own (M1-S8a Task 12).
///
/// `manifest_loaded` sets `feature_engine::MANIFEST` (M3-D2d).
pub fn hello_ack_frame(
    request_id: u32,
    epoch: BootEpoch,
    pools: Vec<PoolInfo>,
    manifest_loaded: bool,
) -> OutFrame {
    let ack = HelloAck {
        engine_version: ENGINE_VERSION,
        boot_epoch: epoch.0,
        features: if manifest_loaded {
            u32::from(feature_engine::MANIFEST)
        } else {
            0
        },
        pools,
        type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
    };
    let payload = ack.encode();
    OutFrame {
        header: Header {
            flags: 0,
            service: service::CORE,
            method: method_core::HELLO_ACK,
            request_id,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}
