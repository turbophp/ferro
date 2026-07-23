//! Emit deterministic golden vectors: for each case, build the full frame (header+payload),
//! and write {name, header, message(json), frame_hex}. Also emit malformed negative .bin seeds.
use ferro_proto::consts::{self, flags, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::*;
use std::path::PathBuf;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors")
}

fn frame(flags_: u16, svc: u16, method: u16, req: u32, payload: Vec<u8>) -> Vec<u8> {
    let h = Header {
        flags: flags_,
        service: svc,
        method,
        request_id: req,
        payload_len: payload.len() as u32,
    };
    let mut f = h.encode().to_vec();
    f.extend_from_slice(&payload);
    f
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn write_case(
    name: &str,
    flags_: u16,
    svc: u16,
    method: u16,
    req: u32,
    payload: Vec<u8>,
    msg_json: serde_json::Value,
) {
    let frame = frame(flags_, svc, method, req, payload);
    let v = serde_json::json!({
        "name": name,
        "header": { "flags": flags_, "service": svc, "method": method, "request_id": req },
        "message": msg_json,
        "frame_hex": hex(&frame),
    });
    let out = dir().join(format!("{name}.json"));
    std::fs::write(out, serde_json::to_string_pretty(&v).unwrap() + "\n").unwrap();
}

fn main() {
    std::fs::create_dir_all(dir().join("negative")).unwrap();

    let hello = Hello {
        client_version: 1,
        type_registry_hash: "deadbeef".into(),
        manifest_hash: None,
        pid: 4242,
        features: 0,
    };
    write_case(
        "hello",
        0,
        service::CORE,
        method_core::HELLO,
        1,
        hello.encode(),
        serde_json::json!({ "client_version":1, "type_registry_hash":"deadbeef",
                            "manifest_hash":null, "pid":4242, "features":0 }),
    );

    let ack = HelloAck {
        engine_version: 1,
        boot_epoch: 0xFFFF_FFFF_FFFF_FFF0,
        features: 0,
        pools: vec![],
        type_registry_hash: "deadbeef".into(),
    };
    write_case(
        "hello_ack",
        0,
        service::CORE,
        method_core::HELLO_ACK,
        1,
        ack.encode(),
        serde_json::json!({ "engine_version":1, "boot_epoch":"18446744073709551600",
                            "features":0, "pools":[], "type_registry_hash":"deadbeef" }),
    );

    write_case(
        "ping",
        0,
        service::CORE,
        method_core::PING,
        9,
        Ping { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }),
    );
    write_case(
        "pong",
        0,
        service::CORE,
        method_core::PONG,
        9,
        Pong { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }),
    );
    write_case(
        "goodbye",
        0,
        service::CORE,
        method_core::GOODBYE,
        0,
        Goodbye {}.encode(),
        serde_json::json!({}),
    );
    write_case(
        "window_update",
        0,
        service::CORE,
        method_core::WINDOW_UPDATE,
        5,
        WindowUpdate {
            frames: 64,
            bytes: 4_194_304,
        }
        .encode(),
        serde_json::json!({ "frames":64, "bytes":4194304 }),
    );

    let err = ErrorPayload {
        code: consts::errc::PROTOCOL,
        branch: consts::errc::PROTOCOL_BRANCH,
        sqlstate: None,
        errno: None,
        message: "reused_request_id".into(),
        detail: None,
        retry_after_ms: None,
    };
    let outcome = Outcome::Error(err);
    write_case(
        "error_protocol",
        flags::END,
        service::CORE,
        0,
        0,
        outcome.encode(),
        serde_json::json!({ "status":1, "error": { "code":0x3009, "branch":3,
            "sqlstate":null, "errno":null, "message":"reused_request_id",
            "detail":null, "retry_after_ms":null } }),
    );

    // Negative seeds (decoder must reject; also fuzz corpus).
    let mut bad_magic = frame(
        0,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    bad_magic[0] = 0x00;
    std::fs::write(dir().join("negative/bad_magic.bin"), &bad_magic).unwrap();

    let mut bad_ver = frame(
        0,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    bad_ver[1] = 0x99;
    std::fs::write(dir().join("negative/bad_version.bin"), &bad_ver).unwrap();

    // Oversize payload_len with no payload body present.
    let mut oversize = Header {
        flags: 0,
        service: service::SQL,
        method: 1,
        request_id: 1,
        payload_len: consts::MAX_FRAME_PAYLOAD + 1,
    }
    .encode()
    .to_vec();
    // (intentionally no payload appended)
    oversize.truncate(16);
    std::fs::write(dir().join("negative/oversize_len.bin"), &oversize).unwrap();

    // Reserved flag set.
    let reserved = frame(
        flags::OOB_FD,
        service::CORE,
        method_core::PING,
        1,
        Ping { token: 1 }.encode(),
    );
    std::fs::write(dir().join("negative/reserved_flag.bin"), &reserved).unwrap();

    eprintln!("vectors written to {}", dir().display());
}
