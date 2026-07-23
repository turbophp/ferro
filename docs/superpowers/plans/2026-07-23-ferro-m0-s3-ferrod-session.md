# Ferro M0 · Slice S3 — `ferrod` Session Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stand up the `ferrod` daemon's session layer — a tokio UDS server that speaks the S1 wire protocol: peer-cred-gated connections, the `HELLO`/`HELLO_ACK` handshake with a per-boot epoch, request multiplexing with the **exactly-one-END** invariant, `PING`/`PONG`/`GOODBYE` liveness, `CANCEL`/`WINDOW_UPDATE` routing, credit-based flow-control primitives, a dispatch table (SQL/TX stubbed to `Unsupported` until S4/S5), session-fatal-vs-per-request error classification, and `SIGTERM` drain — with a `session_reader` fuzz target and the `ferrod` container image (deferred from S2). **No database yet** (that is S4).

**Architecture:** One tokio task per connection. A `tokio_util::codec::Framed` adapts the runtime-free `ferro-proto` codec into a `Stream`/`Sink` of `(Header, payload)` frames. The connection task owns a single writer half; request handlers are spawned tasks that push frames through an mpsc to that writer, each wrapped in a **Drop-guarded `Responder`** that guarantees exactly one terminal `END` per in-flight `request_id` even under panic. An in-flight registry rejects reused ids. Handshake assigns a process-lifetime random `boot_epoch` and hard-fails on a `type_registry_hash` mismatch. The dispatch table answers core-service methods; SQL/TX return `Unsupported` (real handlers land in S4/S5); stream/admin return `Unsupported`.

**Tech Stack:** Rust 1.95 (edition 2024), `tokio` (rt-multi-thread, net, sync, signal, macros), `tokio-util` (codec), `futures`, `tracing`, `thiserror` (lib) + `anyhow` (binary edge), `getrandom` (epoch), `rustix` (SO_PEERCRED via `getsockopt`/`SO_PEERCRED`), `ferro-proto` (path). Dev: `tokio` test macros. Docker for the `ferrod` container.

## Global Constraints

- **Exactly one END per in-flight request** (SPEC §5.2, charter rule 4). Every request_id, from first client frame to terminal, ends in one frame carrying the `END` flag — success, error, or cancelled. Enforced structurally by a Drop-guard, unforgeable under handler panic.
- **`boot_epoch`**: random `u64` from `getrandom` at startup, constant for process life, injectable for tests (decision G-1). A changed epoch on reconnect voids all session state.
- **`type_registry_hash` mismatch is a hard error** — close the connection (versioning story, SPEC §5).
- **Session-fatal vs per-request** (decision G-4): session-fatal (bad magic, unsupported version, HELLO-not-first, type-registry mismatch, oversize frame > `MAX_FRAME_PAYLOAD`, undecodable header, peercred denial) → send one Protocol/Auth error frame then CLOSE. Per-request (unknown service/method, reused id, malformed payload, max_inflight) → error `END` on that id, keep the session.
- **Protocol constants come only from `ferro_proto::consts`** — no hand-written method/flag/service/error/outcome numbers (charter rule 2).
- **`MAX_FRAME_PAYLOAD` (16 MiB) is the codec's hard ceiling**; the `Framed` decoder rejects an oversize declared length with zero payload allocation (reuses the S1 header guard).
- **Flow control** (SPEC §5.2, decision F-1): per-request credit window default 64 frames / 4 MiB (`consts::DEFAULT_CREDIT_*`); per-session aggregate cap 16 MiB. S3 builds the primitives + `WINDOW_UPDATE` routing; streaming consumers arrive in S5.
- **No panics reachable from client bytes** — the reader/decoder returns typed errors; the `session_reader` fuzz target asserts arbitrary input always ends in a Protocol error frame or clean close, never a panic.
- **UDS only in M0** (decision G-2): socket path from `FERRO_SOCK` env / default `/run/ferro/dev.sock`; `SO_PEERCRED` uid allow-list; stale socket unlinked on bind. TCP/bearer-token deferred.
- **Charter gates**: `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`, and (unchanged) the PHP gates.

## File Structure

```
/engine/crates/ferrod/
  Cargo.toml
  src/main.rs               binary edge: config, tracing init, runtime, listener, signal wiring (anyhow)
  src/lib.rs                pub surface for integration tests
  src/config.rs             Config { socket_path, peer_allow_uids, credit_frames/bytes, session_cap_bytes, drain_deadline }
  src/epoch.rs              BootEpoch(u64) + EpochSource (real getrandom / injectable)
  src/listener.rs           UDS bind (stale-unlink) + accept loop; TCP behind a Listener enum stub
  src/peercred.rs           SO_PEERCRED read + uid allow-list check
  src/shutdown.rs           SIGTERM -> drain token; drain deadline
  src/dispatch.rs           method dispatch table -> handler or Unsupported
  src/session/
    mod.rs                  Session::run(conn) orchestrator
    codec.rs                tokio_util Encoder/Decoder over ferro-proto (Frame = (Header, Bytes))
    reader.rs               frame read loop; session-fatal vs per-request classification
    writer.rs               single-writer task fed by an mpsc<OutFrame>
    registry.rs             in-flight request_id set; reuse rejection; max_inflight
    responder.rs            Drop-guarded Responder (exactly-one-END)
    handshake.rs            HELLO/HELLO_ACK, epoch, type-registry hard check
    liveness.rs             PING/PONG, GOODBYE
    flow.rs                 per-request credit + per-session cap + WINDOW_UPDATE apply
    error.rs                SessionError (fatal vs per-request), -> ERROR frame mapping
  tests/common/mod.rs       in-process client: connect a UDS pair, send frames, read frames
  tests/handshake.rs  tests/session_rules.rs  tests/peercred.rs
  fuzz/fuzz_targets/session_reader.rs
/testkit/Dockerfile.ferrod  multi-stage build of the ferrod binary (deferred-from-S2)
/testkit/docker-compose.yml (extended) ferrod sidecar sharing a socket volume with pg
```

---

### Task 1: `ferrod` crate bootstrap + `Framed` codec adapter

**Files:**
- Create: `engine/crates/ferrod/Cargo.toml`, `src/lib.rs`, `src/main.rs` (minimal), `src/session/codec.rs`, `src/session/mod.rs` (stub)
- Create: `engine/crates/ferrod/tests/codec.rs`

**Interfaces:**
- Produces: `session::codec::FrameCodec` implementing `tokio_util::codec::{Encoder<OutFrame>, Decoder}` where a decoded item is `InFrame { header: ferro_proto::header::Header, payload: bytes::Bytes }` and `OutFrame { header, payload }`. `Decoder::decode` rejects oversize/`bad-header` via the S1 `Header::decode` and buffers until the full payload is present. Consumed by every later task.

- [ ] **Step 1: Crate manifest**

```toml
# engine/crates/ferrod/Cargo.toml
[package]
name = "ferrod"
version = "0.0.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
ferro-proto = { path = "../ferro-proto" }
tokio = { version = "1", features = ["rt-multi-thread", "net", "sync", "signal", "macros", "io-util", "time"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1"
futures = "0.3"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
anyhow = "1"
getrandom = "0.2"
rustix = { version = "0.38", features = ["net"] }

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "net", "sync", "macros", "io-util", "time", "test-util"] }
```

- [ ] **Step 2: Write the failing codec test**

```rust
// engine/crates/ferrod/tests/codec.rs
use bytes::BytesMut;
use ferro_proto::consts::{method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::Ping;
use ferrod::session::codec::{FrameCodec, InFrame, OutFrame};
use tokio_util::codec::{Decoder, Encoder};

fn ping_frame() -> OutFrame {
    let payload = Ping { token: 7 }.encode();
    OutFrame {
        header: Header { flags: 0, service: service::CORE, method: method_core::PING,
                         request_id: 1, payload_len: payload.len() as u32 },
        payload: payload.into(),
    }
}

#[test]
fn encode_then_decode_roundtrips_a_frame() {
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();

    let decoded: InFrame = codec.decode(&mut buf).unwrap().expect("a full frame");
    assert_eq!(decoded.header.service, service::CORE);
    assert_eq!(decoded.header.method, method_core::PING);
    assert_eq!(Ping::decode(&decoded.payload).unwrap(), Ping { token: 7 });
    assert!(codec.decode(&mut buf).unwrap().is_none(), "buffer fully consumed");
}

#[test]
fn decode_waits_for_full_payload() {
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();
    // Truncate to header + 1 byte: decoder must return Ok(None) (need more), not error/panic.
    let full = buf.split();
    let mut partial = BytesMut::from(&full[..17]);
    assert!(codec.decode(&mut partial).unwrap().is_none());
}

#[test]
fn decode_rejects_bad_magic() {
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();
    buf[0] = 0x00; // corrupt magic
    assert!(codec.decode(&mut buf).is_err());
}
```

- [ ] **Step 3: Run to verify FAIL**

Run: `cargo test -p ferrod --test codec`
Expected: FAIL (module `session::codec` missing).

- [ ] **Step 4: Implement the codec**

```rust
// engine/crates/ferrod/src/session/codec.rs
use bytes::{Buf, Bytes, BytesMut};
use ferro_proto::consts::MAX_FRAME_PAYLOAD;
use ferro_proto::header::{Header, HEADER_LEN};
use ferro_proto::CodecError;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone)]
pub struct InFrame {
    pub header: Header,
    pub payload: Bytes,
}
#[derive(Debug, Clone)]
pub struct OutFrame {
    pub header: Header,
    pub payload: Bytes,
}

#[derive(Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = InFrame;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<InFrame>, CodecError> {
        if src.len() < HEADER_LEN {
            return Ok(None); // need the header
        }
        // Validate header (magic/version/oversize) WITHOUT consuming yet.
        let header = Header::decode(&src[..HEADER_LEN])?; // FrameTooLarge etc. bubble up = fatal
        let need = HEADER_LEN + header.payload_len as usize;
        if src.len() < need {
            src.reserve(need - src.len());
            return Ok(None); // need the payload
        }
        src.advance(HEADER_LEN);
        let payload = src.split_to(header.payload_len as usize).freeze();
        Ok(Some(InFrame { header, payload }))
    }
}

impl Encoder<OutFrame> for FrameCodec {
    type Error = CodecError;
    fn encode(&mut self, item: OutFrame, dst: &mut BytesMut) -> Result<(), CodecError> {
        debug_assert_eq!(item.header.payload_len as usize, item.payload.len());
        if item.payload.len() > MAX_FRAME_PAYLOAD as usize {
            return Err(CodecError::FrameTooLarge { len: item.payload.len() as u32, max: MAX_FRAME_PAYLOAD });
        }
        dst.extend_from_slice(&item.header.encode());
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}
```

```rust
// engine/crates/ferrod/src/session/mod.rs  (stub — filled by later tasks)
pub mod codec;
```
```rust
// engine/crates/ferrod/src/lib.rs
pub mod session;
```
```rust
// engine/crates/ferrod/src/main.rs  (minimal — real wiring in Task 2/7)
fn main() {
    eprintln!("ferrod skeleton — session server wired in later S3 tasks");
}
```

- [ ] **Step 5: Run to verify PASS + gates**

Run: `cargo test -p ferrod --test codec` → PASS.
Run: `cargo fmt --check && cargo clippy -p ferrod -- -D warnings` → clean.

- [ ] **Step 6: Commit** — `git commit -m "feat(s3): ferrod crate + tokio-util Framed codec over ferro-proto"`

---

### Task 2: UDS listener + SO_PEERCRED allow-list + stale-socket unlink + config

**Files:**
- Create: `src/config.rs`, `src/peercred.rs`, `src/listener.rs`, `src/epoch.rs`
- Create: `engine/crates/ferrod/tests/peercred.rs`

**Interfaces:**
- Produces: `Config` (from env + defaults); `peercred::peer_uid(&UnixStream) -> io::Result<u32>` and `Config::uid_allowed(uid) -> bool`; `listener::bind_uds(&Config) -> io::Result<UnixListener>` (unlinks a stale socket first); `epoch::BootEpoch` with a real and an injectable source.

- [ ] **Step 1..N** (TDD): tests assert `peer_uid` returns the current uid for a connected `UnixStream` pair; `uid_allowed` honors an allow-list (empty list = allow the daemon's own uid only, or all — pick per config default and TEST it); `bind_uds` unlinks a pre-existing socket file and succeeds; binding twice from one process errors cleanly. Implement `peercred` via `rustix::net::sockopt::socket_peercred` (or `getsockopt(SO_PEERCRED)`), `listener` via `tokio::net::UnixListener` with `std::fs::remove_file` on `AddrInUse`/existing path, `epoch` via `getrandom` with an injectable `EpochSource` trait for deterministic tests.

*(Reviewer/implementer note: keep peercred Linux-only behind `#[cfg(target_os = "linux")]`; the acceptance suite runs on Linux/WSL2. Provide the exact `rustix` call the installed version exposes; the tests assert behavior, not the API name.)*

- [ ] **Final Step: Commit** — `git commit -m "feat(s3): UDS listener (stale-unlink) + SO_PEERCRED allow-list + config + boot epoch"`

---

### Task 3: Session task + handshake (HELLO/HELLO_ACK, epoch, type-registry hard check)

**Files:**
- Create: `src/session/writer.rs`, `src/session/handshake.rs`, `src/session/error.rs`; extend `src/session/mod.rs`
- Create: `engine/crates/ferrod/tests/common/mod.rs`, `engine/crates/ferrod/tests/handshake.rs`

**Interfaces:**
- Produces: `Session::run(stream, config, epoch)` that (a) reads the first frame, requires it to be `core/HELLO` (else session-fatal), (b) verifies `type_registry_hash` against `ferro_proto` (mismatch → hard error frame + close), (c) replies `HELLO_ACK { engine_version, boot_epoch, features: 0, pools: [], type_registry_hash }`, then enters the frame loop (stubbed to answer PING in this task, extended later). `tests/common` exposes an in-process harness: `connect() -> (client_io, server JoinHandle)` over a `UnixStream::pair()`, plus `send(frame)` / `recv() -> InFrame` helpers.

- [ ] **TDD tests (handshake.rs):** HELLO→HELLO_ACK round-trips and the ACK carries the injected `boot_epoch`; a first frame that is not HELLO closes the connection (server task ends, client sees EOF after an error frame); a HELLO with a wrong `type_registry_hash` yields a Protocol/Unsupported error frame then close; the same server started twice yields the same epoch only if the same injected source is used (proves epoch is injected, not re-randomized per connection).

- [ ] **Implement** the writer task (owns the write half, drains an `mpsc::Receiver<OutFrame>`), the handshake, and `SessionError` (fatal vs per-request) with `-> ferro_proto::messages::ErrorPayload` mapping using `consts::errc`. The type-registry hash the daemon compares against is `ferro_proto`'s own registry hash — expose it from `ferro-proto` if not already (a `pub const TYPE_REGISTRY_HASH: &str` generated from the lock, or hash the lock at build time); if adding it, that is a `/proto`+build.rs change (charter rule 2) — do it in this task and note it.

- [ ] **Commit** — `git commit -m "feat(s3): session task + HELLO/HELLO_ACK handshake with epoch + type-registry hard check"`

---

### Task 4: Drop-guarded Responder + in-flight registry + exactly-one-END + request_id reuse

**Files:**
- Create: `src/session/responder.rs`, `src/session/registry.rs`; extend `mod.rs`
- Create/extend: `engine/crates/ferrod/tests/session_rules.rs`

**Interfaces:**
- Produces: `Registry` (tracks in-flight `request_id`s, rejects reuse with a per-request Protocol error, enforces `max_inflight`); `Responder { id, tx }` with `stream_frame(...)`, `end_ok(payload)`, `end_error(ErrorPayload)`, `end_cancelled()`, and a `Drop` impl that, if no terminal was sent, pushes exactly one synthetic terminal error `END` and removes the registry entry.

- [ ] **TDD tests:** a handler that returns without ending yields exactly one terminal `END` (Drop synthesizes it); a handler that panics yields exactly one terminal error `END` (catch via the spawned task's `JoinHandle` being aborted/dropped — the Responder Drop still fires); reusing an in-flight `request_id` yields a Protocol error on that id and does NOT disturb the original; exceeding `max_inflight` yields a per-request error, session stays up. Assert "exactly one END" by counting `END`-flagged frames per id at the client.

- [ ] **Implement** the Responder with an `ended: bool` (or a consumed-on-terminal typestate) and a `Drop` that emits the synthetic END through the writer mpsc (best-effort; ignore send error if the session is already gone). The registry is a `HashMap<u32, ()>`/`HashSet<u32>` guarded per-session (single-threaded per connection task, so a `RefCell`/plain field suffices if handlers report back via the mpsc; if handlers are spawned tasks, use `Arc<Mutex<..>>` or an actor channel — pick one and document the concurrency model).

- [ ] **Commit** — `git commit -m "feat(s3): Drop-guarded Responder (exactly-one-END) + in-flight registry + reuse rejection"`

---

### Task 5: Liveness (PING/PONG) + GOODBYE drain + CANCEL + WINDOW_UPDATE routing

**Files:**
- Create: `src/session/liveness.rs`, `src/session/flow.rs`; extend `dispatch.rs`, `mod.rs`
- Extend: `tests/session_rules.rs`

**Interfaces:**
- Produces: PING→PONG (echoing token) answered even while another request is in flight (multiplexing); `GOODBYE` initiates graceful drain (stop accepting new requests, finish in-flight, then close); `CANCEL` (flag on a frame with `request_id`=target, empty payload) marks the target cancelled — advisory + idempotent (no-op if already done); `WINDOW_UPDATE` (core method, `request_id`=target, `{frames, bytes}`) applies credit to that request's flow window (primitive only; no streaming producer yet).

- [ ] **TDD tests:** a PING sent while a long (stubbed-slow) request is in flight gets a PONG before the slow request's END (multiplexing works); `CANCEL` on an in-flight id is idempotent and the request still terminates in exactly one END (`Cancelled` or raced result); `CANCEL` on an unknown/completed id is a no-op; `GOODBYE` lets in-flight finish then closes (client sees the in-flight END then EOF); `WINDOW_UPDATE` on a request replenishes its credit counters (unit-test the `flow` primitive directly).

- [ ] **Commit** — `git commit -m "feat(s3): PING/PONG liveness, GOODBYE drain, CANCEL, WINDOW_UPDATE routing"`

---

### Task 6: Flow-control primitives + dispatch table + error classification

**Files:**
- Extend: `src/session/flow.rs`, `src/dispatch.rs`, `src/session/reader.rs`, `src/session/error.rs`
- Extend: `tests/session_rules.rs`

**Interfaces:**
- Produces: `flow::Credit` (per-request window: 64 frames / 4 MiB default from `consts`, debited per stream frame, replenished by WINDOW_UPDATE) + a per-session 16 MiB aggregate cap; `dispatch::dispatch(service, method) -> Route` where core methods route to their handlers, `sql`/`tx` return `NonRetryable{Unsupported}` (real handlers in S4/S5), `stream`/`admin` return `Unsupported`; the reader's session-fatal-vs-per-request classification (decision G-4) with the exact fatal set from Global Constraints.

- [ ] **TDD tests:** unknown service/method → per-request `Unsupported` error END, session survives; an unsupported reserved flag set on a frame → session-fatal close; a frame with `payload_len > MAX_FRAME_PAYLOAD` → session-fatal close (Protocol) with zero payload read (the codec guard); credit debit/replenish + the session cap are unit-tested; a `sql`/`tx` method returns `Unsupported` (stub) not a panic.

- [ ] **Commit** — `git commit -m "feat(s3): flow-control primitives + dispatch table (sql/tx stubbed) + fatal/per-request classification"`

---

### Task 7: SIGTERM drain + main wiring

**Files:**
- Create: `src/shutdown.rs`; finalize `src/main.rs`
- Extend: `tests/session_rules.rs` (or a `tests/shutdown.rs`)

**Interfaces:**
- Produces: `shutdown::drain_on_sigterm() -> CancellationToken`-like signal; `main` builds the runtime, binds the listener, spawns a session task per accept, and on `SIGTERM` stops accepting, lets in-flight sessions drain up to `drain_deadline`, then hard-closes. Binary runs: `ferrod` listens on the configured socket.

- [ ] **TDD/integration test:** start the server in-process, connect, begin an in-flight request, send the drain signal, assert new connections are refused while the in-flight request still finishes with its one END, then the server exits within the deadline. (Simulate SIGTERM via the injectable drain token in tests rather than a real signal.)

- [ ] **Manual run:** `cargo run -p ferrod` binds `/run/ferro/dev.sock` (or `$FERRO_SOCK`); a scratch client (or `tests/common`) completes a HELLO/PING/GOODBYE cycle. Capture it.

- [ ] **Commit** — `git commit -m "feat(s3): SIGTERM drain + main wiring (bind, accept loop, graceful shutdown)"`

---

### Task 8: `session_reader` fuzz target

**Files:**
- Create: `engine/crates/ferrod/fuzz/Cargo.toml`, `fuzz/fuzz_targets/session_reader.rs`

**Interfaces:**
- Consumes: the codec + reader classification. The target feeds arbitrary bytes to `FrameCodec::decode` in a loop (simulating the read side) and asserts it never panics and always either yields frames or returns a typed error (which the reader maps to a Protocol error + close). Optionally drive a full in-memory session read loop over the arbitrary bytes.

- [ ] **Implement** the fuzz crate (own `[workspace]` table, `libfuzzer-sys`, `ferrod`/`ferro-proto` path deps) and the target. **Run it in nightly Docker** (host has no nightly): `docker run --rm -v "$PWD":/w -w /w rustlang/rust:nightly bash -c 'cargo install cargo-fuzz --locked && cd engine/crates/ferrod && cargo +nightly fuzz run session_reader -- -runs=50000 -max_total_time=90'` — no crash. Fall back to CI-deferred (documented) only if the container path is infeasible. Add the target to CI's fuzz-smoke lane.

- [ ] **Commit** — `git commit -m "test(s3): session_reader cargo-fuzz target (no-panic on arbitrary bytes)"`

---

### Task 9: `ferrod` container image + sidecar compose (deferred from S2)

**Files:**
- Create: `testkit/Dockerfile.ferrod`
- Extend: `testkit/docker-compose.yml` (add a `ferrod` service sharing a socket volume; note pg is present for later slices)

**Interfaces:**
- Produces: a multi-stage Docker build of the `ferrod` binary (rust:1.95 builder → slim runtime), and a compose `ferrod` service mounting a shared `emptyDir`-style volume for `/run/ferro` (the UDS), started alongside `pg`. Validates the daemon runs in a container.

- [ ] **Build + run:** `docker build -f testkit/Dockerfile.ferrod -t ferrod:dev .` succeeds; `docker compose -f testkit/docker-compose.yml up -d ferrod` starts the daemon; a client (or an exec'd smoke) completes a HELLO/PING over the shared socket volume. Capture it. (No DB interaction yet — that is S4/S5.)

- [ ] **Commit** — `git commit -m "feat(s3): ferrod container image + sidecar compose (shared socket volume)"`

---

## Self-Review

- **Spec coverage (design S3 gate):** handshake+epoch+registry-hard-check → T3; request_id reuse rejected → T4; exactly-one-END incl. Responder Drop → T4; multiplexed PING → T5; CANCEL idempotent / GOODBYE drain → T5; peercred allow/deny → T2; SIGTERM drain → T7; core golden vectors byte-match (reuses S1 vectors via the codec) → T1; `session_reader` fuzz never panics → T8; flow-control primitives + dispatch stubs + fatal/per-request → T5/T6; ferrod container → T9.
- **Deferred (noted):** real SQL/TX handlers (S4/S5 replace the `Unsupported` stubs); streaming producers that consume the credit windows (S5); the whole-branch review's S3 items — Rust rejects map-encoded messages (harden the reader here or at S5) and PHP flags::validate parity (PHP client slice).
- **Concurrency model decision (call it in T4 and keep it consistent):** either (a) all frame handling on the single connection task with cooperative async (no per-request spawns; simplest, and PING-while-busy needs `select!` over reader + a work queue), or (b) spawn a task per request with an mpsc back to the single writer (true multiplexing, needs `Arc<Mutex>`/actor for the registry). Pick based on what makes multiplexed-PING (T5) pass cleanly; document it in `session/mod.rs`.
- **Execution-time confirmations:** exact `rustix`/`tokio-util 0.7`/`getrandom` API names (tests assert behavior); nightly Docker availability for the fuzz run (fallback documented); `ferro-proto` type-registry-hash constant may need adding (a `/proto`+build.rs change in T3).

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), review after each (this slice is concurrency-critical — reviewers should probe the one-END invariant, panic-safety, and the fatal/per-request split hard), then a whole-branch adversarial review before S4.
