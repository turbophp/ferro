//! M3-D3 (SPEC §5.1, `/proto/PROTOCOL.md` §1.1): a large buffered result reaches a `MEMFD_RX`
//! client through a SEALED memfd passed with `SCM_RIGHTS`, and nowhere else.
//!
//! Runs against a SQLite pool in a temp file, so it needs no server and runs in every lane.
//!
//! The client here is NOT `common::TestClient`: that one reads through `Framed`, i.e. plain
//! `read(2)`, and the kernel DISCARDS an fd attached to bytes read that way. This one reads every
//! byte with `recvmsg` — deliberately in tiny chunks, so a single read routinely spans the tail of
//! one frame and the head of the next — and queues fds FIFO, which is the receiver half of the
//! association rule `session::oob` documents.

mod common;

use std::collections::VecDeque;
use std::io::{self, IoSliceMut, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::time::Duration;

use ferro_proto::consts::{
    TYPE_REGISTRY_HASH, feature_client, feature_engine, flags, method_core, method_sql, method_tx,
    oob_encoding, service,
};
use ferro_proto::header::Header;
use ferro_proto::messages::sql::{ExecOk, ExecRequest, Stats};
use ferro_proto::messages::{
    BeginRequest, BeginResponse, Hello, HelloAck, OobRef, Outcome, TxControl,
};
use ferro_proto::value::Value;
use nix::fcntl::{FcntlArg, SealFlag, fcntl};
use nix::sys::socket::{ControlMessageOwned, MsgFlags, recvmsg};
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::UnixStream;

use common::{exec_server_with_session_config, req};

const READ_DEADLINE: Duration = Duration::from_secs(20);

/// One frame as the fd-receiving client saw it: the header, the payload, and — for an `OOB_FD`
/// frame — the fd paired with it.
struct Got {
    header: Header,
    payload: Vec<u8>,
    fd: Option<RawFd>,
}

struct FdClient {
    stream: UnixStream,
    buf: Vec<u8>,
    fds: VecDeque<RawFd>,
    chunk: usize,
}

impl FdClient {
    async fn connect(path: &Path, chunk: usize) -> Self {
        FdClient {
            stream: UnixStream::connect(path).await.expect("connect"),
            buf: Vec::new(),
            fds: VecDeque::new(),
            chunk,
        }
    }

    async fn write_frame(&mut self, flags_: u16, svc: u16, method: u16, rid: u32, payload: &[u8]) {
        let h = Header {
            flags: flags_,
            service: svc,
            method,
            request_id: rid,
            payload_len: payload.len() as u32,
        };
        let mut bytes = h.encode().to_vec();
        bytes.extend_from_slice(payload);
        self.stream.write_all(&bytes).await.expect("write");
    }

    async fn hello(&mut self, features: u32) -> HelloAck {
        let hello = Hello {
            client_version: 1,
            type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
            manifest_hash: None,
            pid: std::process::id(),
            features,
        };
        self.write_frame(0, service::CORE, method_core::HELLO, 1, &hello.encode())
            .await;
        let f = self.next_frame().await.expect("HELLO_ACK");
        assert_eq!(f.header.method, method_core::HELLO_ACK);
        HelloAck::decode(&f.payload).expect("decode HELLO_ACK")
    }

    async fn exec(&mut self, rid: u32, r: &ExecRequest) {
        self.write_frame(0, service::SQL, method_sql::EXEC, rid, &r.encode())
            .await;
    }

    /// One `recvmsg` of at most `chunk` bytes; any fds it carried join the FIFO. `false` at EOF.
    async fn fill(&mut self) -> bool {
        let chunk = self.chunk;
        loop {
            tokio::time::timeout(READ_DEADLINE, self.stream.readable())
                .await
                .expect("a frame within the read deadline")
                .expect("readable");
            let mut tmp = vec![0u8; chunk];
            let fd = self.stream.as_raw_fd();
            let got = self.stream.try_io(Interest::READABLE, || {
                let mut cmsg = nix::cmsg_space!([RawFd; 4]);
                let mut iov = [IoSliceMut::new(&mut tmp)];
                let msg = recvmsg::<()>(fd, &mut iov, Some(&mut cmsg), MsgFlags::MSG_CMSG_CLOEXEC)
                    .map_err(io::Error::from)?;
                let mut fds = Vec::new();
                // `cmsgs()` is an Err when the control buffer was truncated (MSG_CTRUNC) — an fd
                // the kernel closed on us, which the test must not shrug off.
                for c in msg.cmsgs().map_err(io::Error::from)? {
                    if let ControlMessageOwned::ScmRights(f) = c {
                        fds.extend(f);
                    }
                }
                Ok((msg.bytes, fds))
            });
            match got {
                Ok((0, fds)) => {
                    assert!(fds.is_empty());
                    return false;
                }
                Ok((n, fds)) => {
                    self.buf.extend_from_slice(&tmp[..n]);
                    self.fds.extend(fds);
                    return true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("recvmsg: {e}"),
            }
        }
    }

    async fn next_frame(&mut self) -> Option<Got> {
        while self.buf.len() < 16 {
            if !self.fill().await {
                return None;
            }
        }
        let header = Header::decode(&self.buf[..16]).expect("header");
        // THE association rule, receiver side: the fd travels with the frame's FIRST byte, so by the
        // time the header is in hand this frame's fd is already queued — and every earlier OOB
        // frame has already taken its own, so the front of the FIFO is this frame's.
        let fd = if header.flags & flags::OOB_FD != 0 {
            Some(
                self.fds
                    .pop_front()
                    .expect("an OOB_FD frame's fd must arrive with the frame's first byte"),
            )
        } else {
            None
        };
        let need = 16 + header.payload_len as usize;
        while self.buf.len() < need {
            assert!(self.fill().await, "EOF mid-frame");
        }
        let payload = self.buf[16..need].to_vec();
        self.buf.drain(..need);
        Some(Got {
            header,
            payload,
            fd,
        })
    }
}

/// Read a received memfd from offset 0 (through a fresh open, so whatever offset the shared
/// description has is irrelevant), then close the received fd.
fn read_and_close(fd: RawFd) -> Vec<u8> {
    let bytes = std::fs::read(format!("/proc/self/fd/{fd}")).expect("read the memfd");
    nix::unistd::close(fd).expect("close the received fd");
    bytes
}

fn sqlite_url(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "ferro-oob-{tag}-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let _ = std::fs::remove_file(&path);
    format!("sqlite://{}", path.display())
}

/// A result of about `n` bytes whose content is unique to `tag`, so a memfd paired with the wrong
/// request would be caught by its CONTENT, not just its size.
fn big(tag: &str, n: usize) -> ExecRequest {
    ExecRequest {
        params: vec![Value::Text(tag.to_string())],
        ..req(&format!(
            "select ?1 || printf('%.*c', {n}, 'z') as big, 7 as seven"
        ))
    }
}

/// An `ExecOk` with its timing stats zeroed: two runs of one statement differ ONLY there, so this
/// is what "the same result, byte for byte" can honestly compare.
fn without_timing(ok: &ExecOk) -> Vec<u8> {
    ExecOk {
        stats: Stats {
            queue_us: 0,
            exec_us: 0,
            bytes: 0,
            ..ok.stats.clone()
        },
        ..ok.clone()
    }
    .encode()
}

fn ok_of(payload: &[u8]) -> ExecOk {
    match Outcome::decode(payload).expect("an Outcome") {
        Outcome::Ok(body) => ExecOk::decode(&body).expect("an ExecOk"),
        other => panic!("expected Outcome::Ok, got {other:?}"),
    }
}

const MEMFD_RX: u32 = feature_client::MEMFD_RX as u32;

/// The headline: a >1 MiB result reaches a `MEMFD_RX` client as an `END | OOB_FD` terminal whose
/// fd holds the inline payload; the same statement on a session that did NOT advertise `MEMFD_RX`
/// arrives inline; the two results are identical apart from timing; and the received memfd is
/// sealed against every change.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_result_arrives_through_a_sealed_memfd_and_equals_the_inline_result() {
    let server = exec_server_with_session_config(sqlite_url("headline"), |_| {});
    let before = ferrod::session::oob::COUNTERS.sent();

    let mut rx = FdClient::connect(server.socket_path(), 64 * 1024).await;
    let ack = rx.hello(MEMFD_RX).await;
    assert_ne!(
        ack.features & u32::from(feature_engine::MEMFD),
        0,
        "an engine with the OOB path enabled advertises MEMFD"
    );

    let q = big("headline", 2 * 1024 * 1024);
    rx.exec(10, &q).await;
    let t = rx.next_frame().await.expect("the terminal");
    assert_eq!(t.header.request_id, 10);
    assert_eq!(
        t.header.flags,
        flags::END | flags::OOB_FD,
        "exactly one END, and it is OOB"
    );
    assert_eq!(
        (t.header.service, t.header.method),
        (service::SQL, method_sql::EXEC)
    );
    let r = OobRef::decode(&t.payload).expect("the OOB_FD payload is an OobRef");
    assert_eq!(r.fd_index, 0);
    assert_eq!(r.encoding, oob_encoding::FRAME_PAYLOAD);
    let fd = t.fd.expect("the fd arrived with the frame");

    // Sealed: the four seals are set, and they hold through a FRESH writable open of the same
    // memfd (seals belong to the inode, so no fd to it can change it).
    let seals =
        SealFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GET_SEALS).expect("F_GET_SEALS"));
    assert!(
        seals.contains(
            SealFlag::F_SEAL_SHRINK
                | SealFlag::F_SEAL_GROW
                | SealFlag::F_SEAL_WRITE
                | SealFlag::F_SEAL_SEAL
        ),
        "every seal is set: {seals:?}"
    );
    let size = std::fs::metadata(format!("/proc/self/fd/{fd}"))
        .unwrap()
        .len();
    assert_eq!(size, r.len, "the memfd's size is exactly the OobRef's len");
    match std::fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/self/fd/{fd}"))
    {
        Ok(mut w) => {
            assert!(w.write_all(b"tamper").is_err(), "a write is refused");
            assert!(w.set_len(r.len + 1).is_err(), "a grow is refused");
            assert!(w.set_len(1).is_err(), "a shrink is refused");
        }
        // Refusing the writable open outright is the same guarantee, one step earlier.
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied, "{e}"),
    }
    let oob_payload = read_and_close(fd);
    assert_eq!(oob_payload.len() as u64, r.len);
    let via_memfd = ok_of(&oob_payload);
    assert_eq!(via_memfd.rows.len(), 1);
    match &via_memfd.rows[0][0] {
        Value::Text(s) => {
            assert!(s.starts_with("headline"));
            assert_eq!(s.len(), "headline".len() + 2 * 1024 * 1024);
        }
        other => panic!("expected TEXT, got {other:?}"),
    }

    // The same statement on a session that did not advertise MEMFD_RX: inline, no fd.
    let mut plain = FdClient::connect(server.socket_path(), 64 * 1024).await;
    plain.hello(0).await;
    plain.exec(10, &q).await;
    let t = plain.next_frame().await.expect("the inline terminal");
    assert_eq!(t.header.flags, flags::END, "no OOB without MEMFD_RX");
    assert!(t.fd.is_none() && plain.fds.is_empty(), "no fd was sent");
    let inline = ok_of(&t.payload);
    assert_eq!(
        without_timing(&via_memfd),
        without_timing(&inline),
        "the memfd carries the same result as the inline frame"
    );
    assert!(
        ferrod::session::oob::COUNTERS.sent() > before,
        "the engine counted the OOB send"
    );
}

/// D1a multiplexing: several large requests and many small ones in flight on ONE session, read
/// back in 7-byte `recvmsg`s so reads routinely straddle frames. Every large result must arrive
/// through ITS OWN memfd — checked by content, which is unique per request — every small one
/// inline, each request exactly one END, and no fd left over.
#[tokio::test(flavor = "multi_thread")]
async fn memfds_pair_with_their_own_frames_when_interleaved_with_small_requests() {
    let server = exec_server_with_session_config(sqlite_url("interleave"), |_| {});
    let mut c = FdClient::connect(server.socket_path(), 7).await;
    c.hello(MEMFD_RX).await;

    // Write everything before reading anything (D1a): big ones at 100, 200, 300 and small ones
    // around and between them.
    let mut expected_big = std::collections::BTreeMap::new();
    let mut rid = 100;
    for b in 0..3u32 {
        let tag = format!("big-{b}-");
        let size = 1_200_000 + (b as usize) * 333_333;
        c.exec(rid, &big(&tag, size)).await;
        expected_big.insert(rid, (tag, size));
        for s in 1..=5 {
            c.exec(rid + s, &req(&format!("select {s} as n"))).await;
        }
        rid += 100;
    }

    let mut ends = std::collections::BTreeMap::new();
    while ends.len() < 3 + 15 {
        let f = c.next_frame().await.expect("a frame");
        let id = f.header.request_id;
        assert_ne!(
            f.header.flags & flags::END,
            0,
            "every EXEC here is buffered: one frame, END"
        );
        assert!(
            ends.insert(id, ()).is_none(),
            "request {id} got a second END"
        );
        match expected_big.get(&id) {
            Some((tag, size)) => {
                assert_eq!(
                    f.header.flags,
                    flags::END | flags::OOB_FD,
                    "big request {id} is OOB"
                );
                let r = OobRef::decode(&f.payload).unwrap();
                let bytes = read_and_close(f.fd.expect("fd"));
                assert_eq!(bytes.len() as u64, r.len);
                let ok = ok_of(&bytes);
                match &ok.rows[0][0] {
                    Value::Text(s) => {
                        assert!(
                            s.starts_with(tag.as_str()),
                            "request {id}'s memfd holds another request's result"
                        );
                        assert_eq!(s.len(), tag.len() + size);
                    }
                    other => panic!("expected TEXT, got {other:?}"),
                }
            }
            None => {
                assert_eq!(f.header.flags, flags::END, "small request {id} is inline");
                assert!(f.fd.is_none());
                let ok = ok_of(&f.payload);
                assert_eq!(ok.rows[0][0], Value::I64(i64::from(id % 100)));
            }
        }
    }
    assert!(c.fds.is_empty(), "no fd arrived without its frame");
}

/// The threshold is the inline PAYLOAD size, the off switch works, a transaction-scoped statement
/// takes the same path, and a stream's DATA frames never do (row streaming stays inline, §5.1).
#[tokio::test(flavor = "multi_thread")]
async fn the_threshold_the_off_switch_a_transaction_and_a_stream() {
    // A small result under the default 1 MiB threshold stays inline even on a MEMFD_RX session.
    let server = exec_server_with_session_config(sqlite_url("small"), |_| {});
    let mut c = FdClient::connect(server.socket_path(), 4096).await;
    c.hello(MEMFD_RX).await;
    c.exec(2, &big("small", 1000)).await;
    let t = c.next_frame().await.unwrap();
    assert_eq!(t.header.flags, flags::END);

    // `memfd_threshold: None` (FERRO_MEMFD_THRESHOLD_BYTES=off): never, and not advertised.
    let off = exec_server_with_session_config(sqlite_url("off"), |cfg| cfg.memfd_threshold = None);
    let mut c = FdClient::connect(off.socket_path(), 4096).await;
    let ack = c.hello(MEMFD_RX).await;
    assert_eq!(ack.features & u32::from(feature_engine::MEMFD), 0);
    c.exec(2, &big("off", 2 * 1024 * 1024)).await;
    let t = c.next_frame().await.unwrap();
    assert_eq!(
        t.header.flags,
        flags::END,
        "the off switch keeps a large result inline"
    );
    assert!(c.fds.is_empty());

    // Threshold 0: every success terminal goes out of band — the transaction's BEGIN response and
    // its statement included (they share the supervisor), the stream's DATA frames never.
    let all =
        exec_server_with_session_config(sqlite_url("all"), |cfg| cfg.memfd_threshold = Some(0));
    let mut c = FdClient::connect(all.socket_path(), 4096).await;
    c.hello(MEMFD_RX).await;
    let begin = BeginRequest {
        pool: "default".into(),
        isolation: None,
        readonly: true,
    };
    c.write_frame(0, service::TX, method_tx::BEGIN, 3, &begin.encode())
        .await;
    let t = c.next_frame().await.unwrap();
    assert_eq!(t.header.flags, flags::END | flags::OOB_FD);
    let tx_id = match Outcome::decode(&read_and_close(t.fd.unwrap())).unwrap() {
        Outcome::Ok(body) => BeginResponse::decode(&body).unwrap().tx_id,
        other => panic!("BEGIN failed: {other:?}"),
    };
    c.exec(
        4,
        &ExecRequest {
            tx_id: Some(tx_id),
            ..req("select 41 + 1 as n")
        },
    )
    .await;
    let t = c.next_frame().await.unwrap();
    assert_eq!(
        t.header.flags,
        flags::END | flags::OOB_FD,
        "a tx-scoped result too"
    );
    assert_eq!(
        ok_of(&read_and_close(t.fd.unwrap())).rows[0][0],
        Value::I64(42)
    );
    c.write_frame(
        0,
        service::TX,
        method_tx::COMMIT,
        5,
        &TxControl { tx_id }.encode(),
    )
    .await;
    let t = c.next_frame().await.unwrap();
    if let Some(fd) = t.fd {
        let _ = read_and_close(fd);
    }

    // A stream: HEAD and DATA inline (never an fd), then its terminal.
    c.exec(
        6,
        &ExecRequest {
            fetch: ferrod::services::sql::FETCH_STREAM,
            ..req("with recursive n(i) as (select 1 union all select i + 1 from n where i < 5000) select i from n")
        },
    )
    .await;
    let mut data_frames = 0;
    loop {
        let f = c.next_frame().await.unwrap();
        assert_eq!(f.header.request_id, 6);
        if f.header.flags & flags::END != 0 {
            if let Some(fd) = f.fd {
                let _ = read_and_close(fd);
            }
            break;
        }
        assert_eq!(
            f.header.flags & flags::OOB_FD,
            0,
            "a stream frame is never OOB"
        );
        assert!(f.fd.is_none());
        if f.header.flags & flags::STREAM != 0 {
            data_frames += 1;
        }
    }
    assert!(data_frames >= 1);
    assert!(c.fds.is_empty());
}

/// `recvmsg` exactly `n` bytes (each call's iov is exactly what is still missing, so a read never
/// reaches past the `n`-th byte), returning the bytes and every fd that came with them.
async fn recv_exact(stream: &UnixStream, n: usize) -> (Vec<u8>, Vec<RawFd>) {
    let mut out = Vec::with_capacity(n);
    let mut fds = Vec::new();
    while out.len() < n {
        tokio::time::timeout(READ_DEADLINE, stream.readable())
            .await
            .expect("bytes within the read deadline")
            .expect("readable");
        let mut tmp = vec![0u8; n - out.len()];
        let raw = stream.as_raw_fd();
        let got = stream.try_io(Interest::READABLE, || {
            let mut cmsg = nix::cmsg_space!([RawFd; 4]);
            let mut iov = [IoSliceMut::new(&mut tmp)];
            let msg = recvmsg::<()>(raw, &mut iov, Some(&mut cmsg), MsgFlags::MSG_CMSG_CLOEXEC)
                .map_err(io::Error::from)?;
            let mut got_fds = Vec::new();
            for c in msg.cmsgs().map_err(io::Error::from)? {
                if let ControlMessageOwned::ScmRights(f) = c {
                    got_fds.extend(f);
                }
            }
            Ok((msg.bytes, got_fds))
        });
        match got {
            Ok((0, _)) => panic!("EOF after {} of {n} bytes", out.len()),
            Ok((k, f)) => {
                out.extend_from_slice(&tmp[..k]);
                fds.extend(f);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("recvmsg: {e}"),
        }
    }
    (out, fds)
}

/// The association rule, deterministically (review F5): the fd is attached to an `OOB_FD` frame's
/// FIRST byte, so a `recvmsg` of EXACTLY the 16 header bytes — and nothing past them — already
/// carries it, while the exact-length reads of the frame before it carry none. A writer that sent
/// the header plainly and attached the fd to the payload (byte 16) passes the chunked tests above
/// only when a read happens to straddle the boundary; this read never does.
#[tokio::test(flavor = "multi_thread")]
async fn the_fd_arrives_with_exactly_the_frame_header() {
    let server = exec_server_with_session_config(sqlite_url("first-byte"), |cfg| {
        cfg.memfd_threshold = Some(4096)
    });
    let mut c = FdClient::connect(server.socket_path(), 16).await;
    let hello = Hello {
        client_version: 1,
        type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
        manifest_hash: None,
        pid: std::process::id(),
        features: MEMFD_RX,
    };
    c.write_frame(0, service::CORE, method_core::HELLO, 1, &hello.encode())
        .await;
    let (head, fds) = recv_exact(&c.stream, 16).await;
    assert!(fds.is_empty(), "no fd with HELLO_ACK's header");
    let h = Header::decode(&head).unwrap();
    let (_, fds) = recv_exact(&c.stream, h.payload_len as usize).await;
    assert!(fds.is_empty(), "no fd with HELLO_ACK's payload");

    // A small inline result first, read in exact lengths: no fd anywhere in it.
    c.exec(2, &req("select 1 as n")).await;
    let (head, fds) = recv_exact(&c.stream, 16).await;
    assert!(fds.is_empty());
    let h = Header::decode(&head).unwrap();
    assert_eq!((h.request_id, h.flags), (2, flags::END));
    let (_, fds) = recv_exact(&c.stream, h.payload_len as usize).await;
    assert!(fds.is_empty());

    // Then a large one: its fd arrives with its 16 header bytes.
    c.exec(3, &big("first-byte", 64 * 1024)).await;
    let (head, fds) = recv_exact(&c.stream, 16).await;
    let h = Header::decode(&head).unwrap();
    assert_eq!((h.request_id, h.flags), (3, flags::END | flags::OOB_FD));
    assert_eq!(
        fds.len(),
        1,
        "the fd must ride the OOB_FD frame's first byte, so a 16-byte read carries it"
    );
    let (payload, more) = recv_exact(&c.stream, h.payload_len as usize).await;
    assert!(more.is_empty(), "exactly one fd per OOB_FD frame");
    let r = OobRef::decode(&payload).unwrap();
    let bytes = read_and_close(fds[0]);
    assert_eq!(bytes.len() as u64, r.len);
    match &ok_of(&bytes).rows[0][0] {
        Value::Text(s) => assert!(s.starts_with("first-byte")),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

/// How many OOB memfds this process holds open — the engine under test runs in-process.
fn open_oob_memfds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|p| p.to_string_lossy().contains("memfd:ferro-oob"))
        .count()
}

/// Review F2: the engine holds at most one OOB memfd per session, however far the client is
/// behind. A client submits hundreds of large requests and reads NOTHING: the first few hundred
/// OOB frames fit in its socket's receive queue (their fds are in flight, in no process's table),
/// and the rest wait in the session's control channel. When the memfd was made as the HANDLER
/// finished, every one of those waiting terminals held an open fd in `ferrod` — one session could
/// exhaust `RLIMIT_NOFILE` and lock every other tenant out of `accept`. Made by the writer just
/// before the send, a waiting terminal is heap, as an inline one is. Then every result is read
/// back, each from its own memfd, so the test cannot pass by the path simply not being taken.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_does_not_read_costs_the_engine_at_most_one_memfd() {
    const N: u32 = 600;
    let server = exec_server_with_session_config(sqlite_url("noread"), |cfg| {
        cfg.memfd_threshold = Some(4096)
    });
    let mut c = FdClient::connect(server.socket_path(), 64 * 1024).await;
    c.hello(MEMFD_RX).await;
    for i in 0..N {
        c.exec(100 + i, &big(&format!("nr-{i}-"), 8 * 1024)).await;
    }
    // Other tests in this binary receive (and promptly close) memfds of their own, so allow a
    // little slack; the defect this pins held hundreds.
    let mut worst = 0;
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < until {
        worst = worst.max(open_oob_memfds());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        worst <= 8,
        "a non-reading client made the engine hold {worst} memfds open"
    );

    let mut oob = 0;
    for _ in 0..N {
        let f = c.next_frame().await.expect("a terminal");
        let i = f.header.request_id - 100;
        let bytes = match f.fd {
            Some(fd) => {
                oob += 1;
                read_and_close(fd)
            }
            None => f.payload,
        };
        match &ok_of(&bytes).rows[0][0] {
            Value::Text(s) => assert!(s.starts_with(&format!("nr-{i}-")), "request {i}"),
            other => panic!("expected TEXT, got {other:?}"),
        }
    }
    assert_eq!(oob, N, "every result travelled by memfd");
    assert!(c.fds.is_empty());
}

/// Kills its child on drop, so a failing assertion never leaves a daemon behind.
struct KillOnDrop(std::process::Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The `ETOOMANYREFS` inline fallback (review RV3), against a REAL `ferrod` process. The kernel
/// refuses an `SCM_RIGHTS` send once the sending user has more fds in flight than the sender's
/// `RLIMIT_NOFILE` — unless the sender is privileged, which is why a root daemon never reaches
/// this path. So `ferrod` runs here under a lowered `RLIMIT_NOFILE` and, when the test runs as root,
/// as uid 65534 (no capabilities). A client that does not read keeps every fd in flight until the
/// limit refuses the next one; the writer must then send that terminal INLINE. Every result must
/// arrive, each its own, some by memfd and — the point — some inline.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_fd_sends_the_same_terminal_inline() {
    use std::os::unix::process::CommandExt;
    const N: u32 = 400;
    const NOFILE: u64 = 128;

    let dir = std::path::PathBuf::from(format!(
        "/tmp/ferro-oob-refs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let as_root = nix::unistd::geteuid().is_root();
    if as_root {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        println!(
            "note: running as root — ferrod drops to uid 65534 so the kernel enforces the \
             in-flight fd limit (a privileged sender is exempt)"
        );
    }
    let sock = dir.join("ferrod.sock");
    // `sh -c 'ulimit -n'` lowers the limit for ferrod alone (this crate forbids the `unsafe` a
    // `pre_exec` would need); std's `uid`/`gid` drop root, clearing supplementary groups first.
    let mut cmd = std::process::Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("ulimit -n {NOFILE} && exec \"$0\""))
        .arg(env!("CARGO_BIN_EXE_ferrod"))
        .env("FERRO_SOCK", &sock)
        .env("FERRO_POOLS", "default")
        .env(
            "FERRO_POOL_DEFAULT_DSN",
            format!("sqlite://{}", dir.join("db.sqlite").display()),
        )
        .env("FERRO_MEMFD_THRESHOLD_BYTES", "4096")
        .env("FERRO_ALLOW_UIDS", nix::unistd::geteuid().to_string())
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    if as_root {
        cmd.uid(65534).gid(65534);
    }
    let _ferrod = KillOnDrop(cmd.spawn().expect("spawn ferrod"));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut c = loop {
        if let Ok(s) = UnixStream::connect(&sock).await {
            break FdClient {
                stream: s,
                buf: Vec::new(),
                fds: VecDeque::new(),
                chunk: 64 * 1024,
            };
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ferrod never listened"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // The precondition, asserted rather than assumed: the daemon really is unprivileged.
    let ack = c.hello(MEMFD_RX).await;
    assert_ne!(ack.features & u32::from(feature_engine::MEMFD), 0);

    for i in 0..N {
        c.exec(100 + i, &big(&format!("rf-{i}-"), 32 * 1024)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (mut oob, mut inline) = (0, 0);
    for _ in 0..N {
        let f = c
            .next_frame()
            .await
            .expect("a terminal — a missing one means the session died on the refusal");
        assert_ne!(f.header.flags & flags::END, 0);
        let i = f.header.request_id - 100;
        let bytes = match f.fd {
            Some(fd) => {
                oob += 1;
                read_and_close(fd)
            }
            None => {
                inline += 1;
                f.payload
            }
        };
        match &ok_of(&bytes).rows[0][0] {
            Value::Text(s) => assert!(s.starts_with(&format!("rf-{i}-")), "request {i}"),
            other => panic!("expected TEXT, got {other:?}"),
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        oob > 0,
        "some results went by memfd ({oob} OOB, {inline} inline)"
    );
    assert!(
        inline > 0,
        "the in-flight limit refused no fd, so the fallback was never exercised \
         ({oob} OOB, {inline} inline)"
    );
}
