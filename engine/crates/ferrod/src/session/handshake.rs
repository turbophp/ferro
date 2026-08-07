//! HELLO / HELLO_ACK: the mandatory first exchange on every connection. `Session::run` reads the
//! first frame itself (so it can special-case "not HELLO" as session-fatal before ever touching
//! this module); this module holds the decode/validate/reply logic once that first frame is
//! known to be `core/HELLO`.

use ferro_proto::consts::{TYPE_REGISTRY_HASH, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::{Hello, HelloAck, PoolInfo};

use crate::config::{Config, PoolKind};
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
pub fn validate_hello(frame: &InFrame) -> Result<Hello, SessionError> {
    let hello = Hello::decode(&frame.payload)
        .map_err(|e| SessionError::protocol_fatal(format!("malformed HELLO payload: {e}")))?;
    if hello.type_registry_hash != TYPE_REGISTRY_HASH {
        return Err(SessionError::type_registry_mismatch(format!(
            "type_registry_hash mismatch: client sent {:?}, engine is {:?}",
            hello.type_registry_hash, TYPE_REGISTRY_HASH
        )));
    }
    Ok(hello)
}

/// The `HELLO_ACK` metadata for every configured pool, derived purely from [`Config`]
/// (PROTOCOL.md §4).
///
/// No `PoolRegistry` is involved and none is needed: `PoolSpec.kind` is already the backend FAMILY,
/// inferred from the DSN SCHEME by `config::infer_pool_kind` at config-parse time. That also keeps
/// the handshake independent of whether any pool has ever been dialled — `ferrod` boots with
/// unreachable backends today and must keep doing so.
///
/// `server_version` is `None` here; M1-S8a Task 12 fills it (and is where the registry finally has
/// to be threaded in). Only the NAME and the family are exposed — never the DSN (§12 server secret).
pub fn pool_info_from_config(config: &Config) -> Vec<PoolInfo> {
    let mut out: Vec<PoolInfo> = config
        .pools
        .iter()
        .map(|spec| PoolInfo {
            name: spec.name.clone(),
            // Exhaustive over `PoolKind` on purpose: a third backend family must break the build
            // here rather than silently advertise the wrong string to a driver that picks a
            // platform from it.
            kind: match spec.kind {
                PoolKind::Postgres => "postgres".to_string(),
                PoolKind::Mysql => "mysql".to_string(),
            },
            server_version: None,
        })
        .collect();
    // Deterministic order. `config.pools` is a Vec so it is already stable, but sorting makes the
    // contract explicit and survives a future map-backed representation — a handshake that reports
    // pools in a different order per connection is needlessly untestable.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
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
/// secret). Build it with [`pool_info_from_config`].
pub fn hello_ack_frame(request_id: u32, epoch: BootEpoch, pools: Vec<PoolInfo>) -> OutFrame {
    let ack = HelloAck {
        engine_version: ENGINE_VERSION,
        boot_epoch: epoch.0,
        features: 0,
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
    use crate::config::{PoolSpec, infer_pool_kind};

    /// Two pools whose kinds are INFERRED from their DSN schemes (never hard-set), so the guard
    /// below is driven through the same `infer_pool_kind` the daemon uses rather than asserting
    /// against a value the test itself chose.
    fn config_with_two_pools() -> Config {
        let spec = |name: &str, dsn: &str| PoolSpec {
            name: name.to_string(),
            dsn: dsn.to_string(),
            kind: infer_pool_kind(dsn),
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        };
        Config {
            pools: vec![
                // Declared MySQL-first so the deterministic-order assertion below can fail.
                spec("reporting", "mysql://u:secret@127.0.0.1:3306/rep"),
                spec("default", "postgres://u:secret@127.0.0.1:5432/app"),
            ],
            ..Config::default()
        }
    }

    /// The M1-S8a `HELLO_ACK` metadata: each pool's backend FAMILY rides the wire beside its name,
    /// `server_version` is `None` until Task 12 fills it, and the order is by name.
    #[test]
    fn pool_info_carries_the_backend_family_and_no_server_version() {
        let info = pool_info_from_config(&config_with_two_pools());

        let seen: Vec<(&str, &str, Option<&str>)> = info
            .iter()
            .map(|p| {
                (
                    p.name.as_str(),
                    p.kind.as_str(),
                    p.server_version.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![("default", "postgres", None), ("reporting", "mysql", None),],
            "HELLO_ACK advertises name + backend family per pool, sorted by name, with \
             server_version still unlearned (M1-S8a Task 12 fills it)"
        );
    }

    /// §12: the DSN is a SERVER-side secret. `PoolInfo` has no DSN field, so the only way one could
    /// reach the wire is via `name`/`kind`/`server_version` — assert on the ENCODED ack bytes, which
    /// is what actually leaves the process, not on the struct.
    #[test]
    fn the_encoded_ack_never_carries_a_dsn_or_a_credential() {
        let config = config_with_two_pools();
        let frame = hello_ack_frame(1, BootEpoch(7), pool_info_from_config(&config));
        let bytes = frame.payload.to_vec();
        let as_text = String::from_utf8_lossy(&bytes).into_owned();
        for spec in &config.pools {
            assert!(
                !as_text.contains(&spec.dsn),
                "the HELLO_ACK payload must never carry a DSN"
            );
        }
        assert!(
            !as_text.contains("secret"),
            "the HELLO_ACK payload must never carry a DSN credential"
        );
        // ...and the metadata it IS supposed to carry is really there, so the negative above
        // cannot pass merely because the encoder emitted nothing.
        assert!(as_text.contains("default") && as_text.contains("postgres"));
    }
}
