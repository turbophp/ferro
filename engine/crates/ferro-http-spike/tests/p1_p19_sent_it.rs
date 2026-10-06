//! P1 and P19 (SPEC §23.19): is `sent` exact on HTTP/1.1, and does `try_send_request` agree?
//!
//! P1: "Through `hyper::client::conn::http1`, no byte of a request reaches the I/O before dispatch,
//! and a plaintext-layer tracker armed at dispatch decides `sent` exactly. Required control: a
//! connection that dies before the first write reports `sent = false`."
//!
//! P19: "`http1::SendRequest::try_send_request` returns the message when it was not serialised, and
//! agrees with the tracker."

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use tokio::io::AsyncWriteExt;

const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

/// **P1, the positive half.** Nothing reaches the I/O between the handshake and dispatch — even with
/// the connection task running and idling — and once armed the tracker's count of accepted bytes is
/// EXACTLY what the upstream received.
#[tokio::test]
async fn p1_no_byte_before_dispatch_and_tracker_count_equals_wire_count() {
    let body = vec![0x5a_u8; 300 * 1024]; // larger than one write, so hyper writes more than once
    let body_len = body.len();
    let up = fake_upstream(move |mut s, received| async move {
        read_request(&mut s, &received, body_len).await;
        s.write_all(OK_RESPONSE).await.unwrap();
        // Hold the connection open until the client is done with it.
        let mut sink = [0u8; 1];
        let _ = tokio::io::AsyncReadExt::read(&mut s, &mut sink).await;
    })
    .await;

    let (mut send, track, _conn) = h1_over(dial(up.addr).await, &h1_builder()).await;

    // Let the connection task run idle for a while: it polls the socket for an early close
    // (`require_empty_read`), which must not write anything.
    tokio::time::sleep(Duration::from_millis(150)).await;
    within("ready", send.ready())
        .await
        .expect("connection ready");
    assert_eq!(
        track.written_unarmed(),
        0,
        "P1: hyper wrote {} byte(s) to the I/O BEFORE dispatch",
        track.written_unarmed()
    );
    assert!(!track.sent());

    // Dispatch.
    track.arm();
    let resp = within("exchange", send.send_request(post(body)))
        .await
        .expect("exchange succeeds");
    assert_eq!(resp.status(), 200);

    assert!(
        track.sent(),
        "P1: an exchange that completed was not marked sent"
    );
    let on_wire = up.received.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        track.written_armed(),
        on_wire,
        "P1 exactness: the tracker counted {} plaintext bytes, the upstream received {on_wire}",
        track.written_armed()
    );
    assert!(
        on_wire > body_len as u64,
        "the whole request (head + body) arrived"
    );
    assert_eq!(track.written_unarmed(), 0);
}

/// **P1, the control for the "no byte before dispatch" half.** The same probe, run on `hyper`'s
/// HTTP/2 client, DOES see bytes reach the I/O before any request exists (the connection preface
/// and SETTINGS). So the probe is not blind, and the h1 result above is a property of h1 — which is
/// exactly why §23.7.1 defines `sent` differently for HTTP/2 (handed to a ready `SendRequest`)
/// instead of reusing the plaintext tracker.
#[tokio::test]
async fn p1_control_the_probe_sees_h2s_pre_dispatch_preface() {
    let up = fake_upstream(|mut s, received| async move {
        let mut buf = [0u8; 4096];
        while let Ok(n) = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await {
            if n == 0 {
                break;
            }
            received.fetch_add(n as u64, std::sync::atomic::Ordering::SeqCst);
        }
    })
    .await;
    let (tracked, track) = Tracker::new(dial(up.addr).await);
    let (_send, conn) =
        hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .handshake::<_, OneChunk>(hyper_util::rt::TokioIo::new(tracked))
            .await
            .unwrap();
    tokio::spawn(conn);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        track.written_unarmed() >= 24,
        "h2 writes its 24-byte preface (+ SETTINGS) before any request; the probe saw {}",
        track.written_unarmed()
    );
    assert!(!track.sent(), "nothing was dispatched, so nothing is sent");
}

/// **P1, the required control (deterministic form).** A connection whose first write FAILS — no
/// byte accepted anywhere — must report `sent = false`. Without this control, a tracker that set
/// `sent` on every `poll_write` *call* would pass the positive test above.
#[tokio::test]
async fn p1_control_connection_dead_at_first_write_reports_not_sent() {
    let (mut send, track, conn) = h1_over(DeadOnWrite, &h1_builder()).await;
    track.arm();
    let err = within("send", send.send_request(post(vec![1u8; 1024])))
        .await
        .expect_err("a dead connection cannot complete an exchange");
    let _ = within("conn", conn).await;
    assert!(
        !track.sent(),
        "P1 control: no byte was accepted by the I/O, yet the tracker says sent ({err:?})"
    );
    assert_eq!(track.written_armed(), 0);
    assert_eq!(
        track.first_error().map(|(d, _)| d),
        Some(Dir::Write),
        "the control really did fail at a write, not somewhere else"
    );
}

/// **P1, the required control (real TCP form).** The upstream resets the connection BEFORE dispatch.
/// `hyper`'s idle connection task notices (it reads while idle), the request is never written, and
/// the tracker reports `sent = false`; the upstream received nothing.
#[tokio::test]
async fn p1_control_connection_reset_before_dispatch_reports_not_sent() {
    let up = fake_upstream(|s, _| async move { reset(s) }).await;
    let (mut send, track, conn) = h1_over(dial(up.addr).await, &h1_builder()).await;

    // Wait until hyper's connection task has observed the reset and ended.
    let ended = within("connection task ends", conn).await.unwrap();
    // An idle client connection that is reset ends with an I/O error or cleanly (EOF); either way
    // it is over before any request exists.
    let _ = ended;

    track.arm();
    let err = within("send", send.send_request(post(vec![1u8; 16])))
        .await
        .expect_err("the connection is gone");
    assert!(
        !track.sent(),
        "P1 control: reset-before-dispatch reported sent ({err:?})"
    );
    assert_eq!(up.received.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// **P1 through TLS.** The tracker wraps the `TlsStream` (plaintext above TLS, below `hyper`). The
/// handshake completes before the tracker exists, so handshake bytes are never counted; the
/// plaintext count after dispatch equals the bytes the upstream DECRYPTED; and a TLS connection the
/// upstream closes before dispatch reports `sent = false`. This is also the test that makes `ring`
/// actually compile and link (P3's "builds with no CMake" is a build, not a lock-file reading).
#[tokio::test]
async fn p1_through_tls_tracker_counts_plaintext_and_control_holds() {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let ca = CertificateDer::from_pem_slice(include_bytes!("fixtures/ca.pem")).unwrap();
    let leaf = CertificateDer::from_pem_slice(include_bytes!("fixtures/leaf.pem")).unwrap();
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/leaf.key")).unwrap();

    let server_cfg = Arc::new(
        rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![leaf, ca.clone()], key)
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca).unwrap();
    let client_cfg = Arc::new(
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    for close_before_dispatch in [false, true] {
        let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg.clone());
        let body = vec![7u8; 100 * 1024];
        let body_len = body.len();
        let received = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let r2 = received.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.expect("server handshake");
            if close_before_dispatch {
                tls.shutdown().await.ok(); // close_notify + FIN, before any request
                return;
            }
            read_tls_request(&mut tls, &r2, body_len).await;
            tls.write_all(OK_RESPONSE).await.unwrap();
            tls.flush().await.unwrap();
            let mut sink = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut tls, &mut sink).await;
        });

        let connector = tokio_rustls::TlsConnector::from(client_cfg.clone());
        let tcp = dial(addr).await;
        let tls = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .expect("client handshake");
        let (mut send, track, conn) = h1_over(tls, &h1_builder()).await;

        if close_before_dispatch {
            let _ = within("tls conn ends", conn).await;
            track.arm();
            let r = within("send", send.send_request(post(body))).await;
            assert!(r.is_err());
            assert!(
                !track.sent(),
                "P1/TLS control: closed-before-dispatch reported sent"
            );
            assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), 0);
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(track.written_unarmed(), 0, "P1/TLS: bytes before dispatch");
            track.arm();
            let resp = within("tls exchange", send.send_request(post(body)))
                .await
                .expect("tls exchange");
            assert_eq!(resp.status(), 200);
            assert!(track.sent());
            assert_eq!(
                track.written_armed(),
                received.load(std::sync::atomic::Ordering::SeqCst),
                "P1/TLS exactness: plaintext counted == plaintext the upstream decrypted"
            );
        }
        server.abort();
    }
}

async fn read_tls_request<S: tokio::io::AsyncRead + Unpin>(
    s: &mut S,
    received: &std::sync::atomic::AtomicU64,
    body_len: usize,
) {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 64 * 1024];
    let mut seen = Vec::new();
    loop {
        if let Some(p) = seen.windows(4).position(|w: &[u8]| w == b"\r\n\r\n")
            && seen.len() >= p + 4 + body_len
        {
            return;
        }
        let n = match s.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        received.fetch_add(n as u64, std::sync::atomic::Ordering::SeqCst);
        seen.extend_from_slice(&buf[..n]);
    }
}

// ---------------------------------------------------------------------------------------------
// P19 — `try_send_request` as a cross-check of the tracker.
// ---------------------------------------------------------------------------------------------

/// The three ways an exchange can fail before a head, each run through `try_send_request` with the
/// tracker armed. Returns (message returned?, tracker sent?).
async fn try_send_case(case: &str) -> (bool, bool) {
    match case {
        // (A) The connection is known closed before dispatch: hyper never dequeues the request.
        "closed_before_dispatch" => {
            let up = fake_upstream(|s, _| async move { reset(s) }).await;
            let (mut send, track, conn) = h1_over(dial(up.addr).await, &h1_builder()).await;
            let _ = within("conn ends", conn).await;
            track.arm();
            let mut e = within("try_send", send.try_send_request(post(vec![1u8; 64])))
                .await
                .expect_err("closed");
            (e.take_message().is_some(), track.sent())
        }
        // (B) The request is written and the upstream closes with no response.
        "written_then_closed" => {
            let up = fake_upstream(|mut s, received| async move {
                read_request(&mut s, &received, 64).await;
                drop(s);
            })
            .await;
            let (mut send, track, _conn) = h1_over(dial(up.addr).await, &h1_builder()).await;
            within("ready", send.ready()).await.unwrap();
            track.arm();
            let mut e = within("try_send", send.try_send_request(post(vec![1u8; 64])))
                .await
                .expect_err("no response");
            assert!(up.received.load(std::sync::atomic::Ordering::SeqCst) > 0);
            (e.take_message().is_some(), track.sent())
        }
        // (C) hyper dequeues and serialises the request into its own write buffer, and the very
        // first write to the I/O fails: nothing reached the wire.
        "serialised_but_first_write_failed" => {
            let (mut send, track, _conn) = h1_over(DeadOnWrite, &h1_builder()).await;
            track.arm();
            let mut e = within("try_send", send.try_send_request(post(vec![1u8; 64])))
                .await
                .expect_err("dead");
            (e.take_message().is_some(), track.sent())
        }
        other => unreachable!("{other}"),
    }
}

/// **P19.** `try_send_request` hands the message back exactly when hyper never dequeued it, and in
/// that case the tracker agrees (not sent). When the request reached the wire, no message comes
/// back and the tracker says sent.
///
/// **The caveat this test pins (case C):** the message is NOT returned when hyper serialised it into
/// its OWN buffer and the first write to the I/O then failed — so "no message" does not imply
/// "sent". Agreement is one-directional: `message returned ⇒ ¬sent`. The tracker is the authority
/// (§23.7.1 already says so); `try_send_request` can only ever confirm a NOT-sent, never a sent.
#[tokio::test]
async fn p19_try_send_request_message_implies_not_sent_but_not_conversely() {
    let a = try_send_case("closed_before_dispatch").await;
    let b = try_send_case("written_then_closed").await;
    let c = try_send_case("serialised_but_first_write_failed").await;

    // The invariant the engine's cross-check may rely on: a returned message means not sent.
    for (name, (msg, sent)) in [("A", a), ("B", b), ("C", c)] {
        assert!(
            !(msg && sent),
            "P19 cross-check violated in case {name}: message returned AND tracker says sent"
        );
    }
    assert_eq!(
        a,
        (true, false),
        "A: not dequeued → message back, not sent (agree)"
    );
    assert_eq!(
        b,
        (false, true),
        "B: on the wire → no message, sent (agree)"
    );
    assert_eq!(
        c,
        (false, false),
        "C: serialised into hyper's buffer, first I/O write failed → NO message, yet NOT sent. \
         P19's 'agrees with the tracker' holds only in the direction message ⇒ ¬sent."
    );
}
