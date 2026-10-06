//! **M6-F2 — service `HTTP` is routed, end to end through a real `ferrod` session over a real Unix
//! socket, and served by nothing yet (SPEC §23.5, §23.15).**
//!
//! The handler is the REAL per-connection factory `main` builds (`services::sql::make_handler`), so
//! what these tests see is what a client of this build sees:
//!
//! * a well-formed `HTTP`/`REQUEST` enters the request lifecycle and ends in exactly ONE `END` —
//!   `Unsupported`, built by the handler on the request's own `HTTP`/`REQUEST` header (a frame with
//!   no route would be answered by the SESSION, on the generic `CORE`/0 header, which is how the two
//!   paths are told apart from outside);
//! * a malformed `REQUEST` body is `Protocol` (§23.6 step 1), proving the engine decodes it;
//! * `HEAD`, `BODY` and every other HTTP method id have NO route (engine → client only, review F26):
//!   one session-built `Unsupported` each;
//! * the session survives every one of them, and `HELLO_ACK` does not advertise the `HTTP` feature.

mod common;

use std::sync::Arc;

use common::{TestClient, TestServer, assert_session_alive};
use ferro_proto::consts::{errc, feature_engine, flags, method_http, service};
use ferro_proto::messages::{HttpHeaderField, HttpRequest, Outcome};
use ferrod::config::Config;
use ferrod::epoch::BootEpoch;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::tx::TxRegistry;

/// A daemon with no pools and the real handler factory.
fn server() -> TestServer {
    let config = Config::default();
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory_and_config(BootEpoch(1), config, registry, tx_registry, factory)
}

fn request() -> HttpRequest {
    HttpRequest {
        upstream: "billing".into(),
        method: "POST".into(),
        target: "/v1/charges".into(),
        origin: None,
        headers: vec![HttpHeaderField {
            name: "content-type".into(),
            value: b"application/json".to_vec(),
        }],
        body: Some(b"{}".to_vec()),
        timeout_ms: Some(1_000),
        connect_timeout_ms: None,
        read_timeout_ms: None,
        idempotent: None,
        decode: true,
        route: None,
        traceparent: None,
    }
}

/// Receive the request's ONE frame and return its error, asserting the header it arrived on.
async fn terminal_error(
    client: &mut TestClient,
    rid: u32,
    header: (u16, u16),
) -> ferro_proto::messages::ErrorPayload {
    let frame = client.recv().await;
    assert_eq!(frame.header.request_id, rid);
    assert_eq!(
        frame.header.flags,
        flags::END,
        "the first frame back is the terminal"
    );
    assert_eq!(
        (frame.header.service, frame.header.method),
        header,
        "rid {rid}: which path built the terminal"
    );
    let ep = match Outcome::decode(&frame.payload).expect("decode Outcome") {
        Outcome::Error(ep) => ep,
        other => panic!("rid {rid}: expected Outcome::Error, got {other:?}"),
    };
    // SPEC §23.5.6 as amended by §22.2 (cy), and C11: on service HTTP, `detail` is never free
    // text — it is `nil` on exactly the two non-fate terminals (`Protocol`, `Unsupported`), which
    // are the only HTTP terminals this build produces — and `sqlstate`/`errno` are always `nil`.
    // Held on BOTH paths: the handler-built `HTTP`/`REQUEST` terminal and the session-built CORE/0.
    assert_eq!(
        (ep.detail.as_deref(), ep.sqlstate.as_deref(), ep.errno),
        (None, None, None),
        "rid {rid}: an HTTP Protocol/Unsupported terminal carries no detail, sqlstate or errno"
    );
    ep
}

#[tokio::test]
async fn an_http_request_is_routed_decoded_and_answered_unsupported_exactly_once() {
    let server = server();
    let mut client = server.connect().await;
    let hello = client.hello(1).await;
    assert_eq!(
        hello.ack.features & u32::from(feature_engine::HTTP),
        0,
        "nothing serves HTTP in this build, so the engine must not advertise it (SPEC §23.5)"
    );

    client
        .send_request(30, service::HTTP, method_http::REQUEST, request().encode())
        .await;
    let ep = terminal_error(&mut client, 30, (service::HTTP, method_http::REQUEST)).await;
    assert_eq!(
        (ep.code, ep.branch),
        (errc::UNSUPPORTED, errc::UNSUPPORTED_BRANCH)
    );
    assert!(
        ep.message.contains("not served"),
        "the handler's own refusal, not a generic stub: {:?}",
        ep.message
    );
    // Exactly one END: the next frame on the wire is the PONG, not a second terminal.
    assert_session_alive(&mut client, 31).await;

    // A malformed body (a fixarray(1), not the fixarray(13) an HttpRequest is) is a wire fault.
    client
        .send_request(32, service::HTTP, method_http::REQUEST, vec![0x91, 0xc0])
        .await;
    let ep = terminal_error(&mut client, 32, (service::HTTP, method_http::REQUEST)).await;
    assert_eq!(
        ep.code,
        errc::PROTOCOL,
        "§23.6 step 1: a malformed REQUEST is Protocol"
    );
    assert_session_alive(&mut client, 33).await;
}

#[tokio::test]
async fn http_head_body_and_unknown_methods_have_no_route() {
    let server = server();
    let mut client = server.connect().await;
    client.hello(1).await;

    for (rid, method) in [
        (40, method_http::HEAD),
        (41, method_http::BODY),
        (42, 0),
        (43, 4),
        (44, 0xFFFF),
    ] {
        // A well-formed REQUEST body, so a mis-route into the handler would be ACCEPTED as one and
        // answered on HTTP/<method> — the header below is what tells the two paths apart.
        client
            .send_request(rid, service::HTTP, method, request().encode())
            .await;
        // `Route::Unsupported` is answered by the session itself, on the generic CORE/0 header.
        let ep = terminal_error(&mut client, rid, (service::CORE, 0)).await;
        assert_eq!(ep.code, errc::UNSUPPORTED, "HTTP method {method}");
        assert_session_alive(&mut client, u64::from(rid)).await;
    }
}
