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
            "type_registry_hash mismatch: client sent {}, engine is {:?}",
            shown(&hello.type_registry_hash),
            TYPE_REGISTRY_HASH
        )));
    }
    if let Some(client) = hello.manifest_hash.as_deref() {
        // A manifest hash is 64 lowercase hex characters (`Manifest::hash`); anything else cannot
        // match and is refused without being echoed (M3-D2d review F6: a multi-megabyte value was
        // echoed into a terminal frame larger than the frame cap, so the refusal never arrived).
        if client.len() != 64
            || !client
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(SessionError::type_registry_mismatch(format!(
                "manifest_hash is not a manifest hash (64 lowercase hex characters; got {})",
                shown(client)
            )));
        }
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

/// A client-supplied hash for an error message: itself when it is short lowercase hex (it can then
/// carry nothing but hex), else only its length — never arbitrary client bytes, and never a value
/// large enough to push the refusal past the frame cap.
fn shown(value: &str) -> String {
    if value.len() <= 128
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        format!("{value:?}")
    } else {
        format!("a malformed value of {} bytes", value.len())
    }
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
/// `manifest_loaded` sets `feature_engine::MANIFEST` (M3-D2d); `memfd_enabled` sets
/// `feature_engine::MEMFD` (M3-D3); `http_served` sets `feature_engine::HTTP` (M6-F4a) — "this
/// engine SERVES Ferro HTTP" (§23.5): built with the `http` feature, configured, and not disabled
/// (`PoolRegistry::http_served`). A client must check it, because a `--no-default-features` engine
/// carries the same registry hash.
pub fn hello_ack_frame(
    request_id: u32,
    epoch: BootEpoch,
    pools: Vec<PoolInfo>,
    manifest_loaded: bool,
    memfd_enabled: bool,
    http_served: bool,
) -> OutFrame {
    let mut features = 0u32;
    if http_served {
        features |= u32::from(feature_engine::HTTP);
    }
    if manifest_loaded {
        features |= u32::from(feature_engine::MANIFEST);
    }
    // M3-D3: the engine CAN send a sealed memfd (SPEC §5.1). It still sends one only to a client
    // that advertised `MEMFD_RX`; the bit is informational for the client, which reacts to the
    // `OOB_FD` flag on a frame rather than to this.
    if memfd_enabled {
        features |= u32::from(feature_engine::MEMFD);
    }
    let ack = HelloAck {
        engine_version: ENGINE_VERSION,
        boot_epoch: epoch.0,
        features,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// SPEC §23.5 / §22.2 (cy), (cz): `feature_engine::HTTP` means "this engine SERVES Ferro HTTP",
    /// and is set by `http_served` alone. Asserted over every input `hello_ack_frame` takes — not
    /// only the default one an e2e test happens to run — and as "nothing but the bits those inputs
    /// control", so a coupling of ANY bit to another input fails here.
    #[test]
    fn hello_ack_advertises_only_the_bits_its_inputs_control() {
        let controlled = u32::from(feature_engine::MANIFEST)
            | u32::from(feature_engine::MEMFD)
            | u32::from(feature_engine::HTTP);
        for manifest_loaded in [false, true] {
            for memfd_enabled in [false, true] {
                for http_served in [false, true] {
                    let frame = hello_ack_frame(
                        7,
                        BootEpoch(1),
                        Vec::new(),
                        manifest_loaded,
                        memfd_enabled,
                        http_served,
                    );
                    let ack = HelloAck::decode(&frame.payload).expect("HELLO_ACK decodes");
                    let case = format!(
                        "manifest_loaded={manifest_loaded} memfd_enabled={memfd_enabled} \
                     http_served={http_served}"
                    );
                    assert_eq!(
                        ack.features & u32::from(feature_engine::HTTP) != 0,
                        http_served,
                        "{case}: HTTP"
                    );
                    assert_eq!(
                        ack.features & !controlled,
                        0,
                        "{case}: an uncontrolled bit is set"
                    );
                    assert_eq!(
                        ack.features & u32::from(feature_engine::MANIFEST) != 0,
                        manifest_loaded,
                        "{case}"
                    );
                    assert_eq!(
                        ack.features & u32::from(feature_engine::MEMFD) != 0,
                        memfd_enabled,
                        "{case}"
                    );
                }
            }
        }
    }
}
