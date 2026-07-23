# Ferro M0 · Slice S3 — `ferrod` Session Skeleton Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **v2** — rewritten after an adversarial plan-verification pass (wf_c18dc460) that found 3 blockers + 13 majors in v1. The concurrency model, terminal-delivery, error-envelope, and one-END scoping are now decided up front so no task is built against an undecided design.

**Goal:** Stand up the `ferrod` daemon's session layer — a tokio UDS server speaking the S1 wire protocol: peer-cred-gated connections, the `HELLO`/`HELLO_ACK` handshake with a per-boot epoch, request multiplexing with the **exactly-one-END** invariant (a supervisor guarantees it even under handler panic), `PING`/`PONG`/`GOODBYE` liveness, `CANCEL`/`WINDOW_UPDATE` routing, credit flow-control primitives, a dispatch table (SQL/TX stubbed to `Unsupported` until S4/S5), session-fatal-vs-per-request error classification, and `SIGTERM` drain — with a classification-level fuzz target and the `ferrod` container image (deferred from S2). **No database yet** (S4).

## Decided architecture (read before any task)

One tokio task per accepted connection ("the **session task**"). It owns:
- a **reader loop** over `Framed<UnixStream, FrameCodec>`;
- a long-lived **writer task** fed by TWO channels: a small **control channel** (`mpsc`, capacity ~= max_inflight+8) for HELLO_ACK / PONG / GOODBYE / WINDOW_UPDATE-ack / **every terminal END** / session-fatal errors, and a **data channel** (bounded, credit-limited) for streamed result frames (produced starting S5). The writer drains control first, then data. **Terminals and control are never blocked by stream backpressure** — this is how Blocker-3 (silently-dropped mandatory terminal) is prevented.
- an **in-flight registry** (`std::sync::Mutex<HashMap<u32, InFlight>>`) keyed by `request_id`, holding ONLY request-bearing requests (services SQL/TX/STREAM). Core control/liveness frames never enter it.

**Request handling = spawn-per-request + supervisor (the exactly-one-END mechanism).**
- A request-bearing frame spawns a handler task that owns a `Responder` (holds a control-channel `Sender` clone + the request id + a shared `terminated: Arc<AtomicBool>`). The handler calls `responder.end_ok/end_error/end_cancelled(...)` exactly once; each `end_*` consumes `self`, enqueues the terminal on the control channel (which always has room — see below), then sets `terminated`.
- The session task holds each handler's `JoinHandle`. When the handle resolves, the **supervisor** checks: if the task panicked (`JoinError::is_panic()`) OR completed with `terminated == false`, it synthesizes exactly one terminal `END` with a distinct `errc::Protocol` "handler produced no terminal" code and removes the registry entry. This does not rely on `Drop`-during-unwind timing; it works whether the handler returned early or panicked, and the writer task is always still alive (it is owned by the session task, not the handler).
- **Control-channel deliverability:** at registry-insert the session task reserves a control-channel permit (`Sender::reserve_owned`) and hands it to the `InFlight`/`Responder`, so the terminal always has a slot even if other control traffic is queued. (Equivalently: size the control channel to `max_inflight + slack` and treat control as never-full; the reservation is the belt-and-suspenders.)
- **`panic = "unwind"` is pinned** for the ferrod profile (documented as load-bearing); the supervisor's `JoinError::is_panic()` path also covers it. (`panic = "abort"` would kill the process, so it is forbidden for ferrod.)

**Terminal envelope:** every terminal END encodes `ferro_proto::messages::Outcome` — `Outcome::Ok(body)` / `Outcome::Error(ErrorPayload)` / `Outcome::Cancelled` — via `Outcome::encode()`, on a frame with the `END` flag. There is no bare-`ErrorPayload` terminal anywhere.

**Session-fatal errors** send ONE frame: `service=CORE, request_id=0, flags=END, payload=Outcome::Error(ErrorPayload{code, ...})`, then close.

## Global Constraints

- **Exactly one END per request-bearing request** (SPEC §5.2, charter rule 4), scoped to services SQL/TX/STREAM. Core control/liveness responses (HELLO_ACK, PONG, WINDOW_UPDATE-ack, GOODBYE) are **non-terminal `flags=0` frames**, not in the registry, not subject to one-END. Guaranteed by the supervisor (panic + no-terminal both covered); the diagnostic rejection of a reused id is a separate frame, not part of the original's lifecycle.
- **Every terminal END = `Outcome::encode()`**; session-fatal frame = `CORE / rid=0 / END / Outcome::Error`.
- **CANCEL and drain are flag-based**, never abort-based: the handler observes a cancel/drain flag and itself calls `end_cancelled()` (or the raced result). The supervisor's synthetic terminal is reserved strictly for the panic / no-terminal bug path and uses a distinct code.
- **`boot_epoch`**: random `u64` from `getrandom` at startup, **constant across all connections of one running instance**, injectable for tests (G-1). Changes only on real restart.
- **`type_registry_hash` mismatch is a hard error** → session-fatal close with `errc::UNSUPPORTED` (§5: forces regen/redeploy). The daemon's hash is `ferro_proto::TYPE_REGISTRY_HASH` (added in Task 3 by hashing `registry.lock.json` in build.rs).
- **Session-fatal set (G-4):** bad magic, unsupported version, HELLO-not-first, type-registry mismatch, oversize frame (`FrameTooLarge`), undecodable/truncated header, **a set RESERVED flag (`OOB_FD`/`COMPRESSED` → `UnsupportedFlag`)**, peercred denial. **Per-request:** unknown service/method, reused in-flight id, malformed payload, `max_inflight` exceeded, **unknown non-reserved flag bits (`UnknownFlags`)** (payload_len is known so the frame is skippable). Session-fatal → one `rid=0` error frame + close; per-request → error `END` on that id, session survives.
- **Protocol constants only from `ferro_proto::consts`** (charter rule 2). Terminal status uses `consts::outcome::*`; error codes `consts::errc::*`.
- **`MAX_FRAME_PAYLOAD` (16 MiB)** is the codec's hard ceiling; the `Framed` decoder rejects an oversize declared length with zero payload allocation (S1 `Header::decode` guard, surfaced as a fatal `FrameError::Codec`).
- **Flow control** (F-1): per-request credit default 64 frames / 4 MiB (`consts::DEFAULT_CREDIT_*`); **per-session aggregate cap `session_cap_bytes` defaults to its OWN literal `16*1024*1024`** (a distinct concept from `MAX_FRAME_PAYLOAD`, not coupled to it). S3 builds the primitives + WINDOW_UPDATE routing; stream producers arrive in S5.
- **No panics reachable from client bytes** — the reader's byte→classification step is a pure function returning typed outcomes; the fuzz target asserts arbitrary input always terminates in a typed Protocol error or clean close, never a panic.
- **UDS only** (G-2): socket path from `FERRO_SOCK` / default `/run/ferro/dev.sock`; `SO_PEERCRED` uid allow-list; stale socket unlinked on bind. `unsafe_code = "forbid"` is inherited from the workspace — peercred MUST use a safe wrapper (`rustix`/`nix`), never raw `libc::getsockopt`.
- **request_id conventions** (from the committed vectors): HELLO_ACK echoes HELLO's request_id; PONG echoes PING's request_id (and token); GOODBYE + session-fatal control use request_id=0.
- **Charter gates**: `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`, plus the unchanged PHP gates.

## File Structure

```
/engine/crates/ferrod/
  Cargo.toml                (profile note: ferrod requires panic = "unwind")
  src/main.rs               binary edge: config, tracing, runtime, listener, signal (anyhow)
  src/lib.rs                pub surface for integration tests
  src/config.rs             Config { socket_path, peer_allow_uids, credit_frames/bytes, session_cap_bytes, max_inflight, drain_deadline }
  src/epoch.rs              BootEpoch(u64) + EpochSource (real getrandom / injectable)
  src/listener.rs           UDS bind (stale-unlink) + accept loop (peercred gate here)
  src/peercred.rs           SO_PEERCRED via safe wrapper + uid allow-list
  src/shutdown.rs           injectable drain signal + deadline
  src/dispatch.rs           (service, method) -> Route { CoreControl | Request(handler) | Unsupported }
  src/session/
    mod.rs                  Session::run — reader loop + writer task + supervisor
    codec.rs                FrameCodec (Encoder/Decoder) with FrameError { Codec, Io }
    writer.rs               writer task draining control (priority) + data channels
    registry.rs             in-flight registry (std::sync::Mutex), reuse rejection, max_inflight
    responder.rs            consuming-typestate Responder (control permit) + terminated flag
    supervisor.rs           awaits handler JoinHandles, synthesizes terminal on panic/no-terminal
    handshake.rs            HELLO/HELLO_ACK, epoch, type-registry hard check
    liveness.rs             PING/PONG, GOODBYE
    flow.rs                 per-request Credit + per-session cap + WINDOW_UPDATE apply
    classify.rs             pure bytes/decode-result -> Classification { Frame | PerRequestErr | FatalErr | NeedMore }
    error.rs                SessionError (fatal vs per-request) -> Outcome::Error mapping
  tests/common/mod.rs       in-process harness: real bound UnixListener + connect; recv() with real-time timeout
  tests/handshake.rs  tests/session_rules.rs  tests/peercred.rs  tests/shutdown.rs
  fuzz/fuzz_targets/session_classify.rs
/proto (build.rs adds TYPE_REGISTRY_HASH by hashing registry.lock.json)
/testkit/Dockerfile.ferrod  multi-stage ferrod build
/testkit/docker-compose.yml (extended) ferrod sidecar sharing a socket volume with pg
```

---

### Task 1: crate bootstrap + `Framed` codec adapter (with `FrameError`)

**Files:** Create `engine/crates/ferrod/{Cargo.toml, src/lib.rs, src/main.rs (minimal), src/session/mod.rs (stub: `pub mod codec;`), src/session/codec.rs}`, `tests/codec.rs`.

**Interfaces:** Produces `session::codec::{FrameCodec, InFrame{header,payload:Bytes}, OutFrame{header,payload:Bytes}, FrameError}`. `FrameCodec` impls `tokio_util::codec::{Decoder, Encoder<OutFrame>}` with `type Error = FrameError`. `FrameError { Codec(ferro_proto::CodecError), Io(std::io::Error) }` with `From<io::Error>` (satisfies the tokio-util bound) and `From<ferro_proto::CodecError>` (so `Header::decode(..)?` works inside `decode`).

- [ ] **Step 1: Cargo.toml** (as v1) — deps: `ferro-proto` (path), `tokio` (rt-multi-thread,net,sync,signal,macros,io-util,time), `tokio-util` (codec), `bytes`, `futures`, `tracing`, `tracing-subscriber`(env-filter), `thiserror`, `anyhow`, `getrandom`="0.2", `rustix`(net). dev: `tokio`(...,test-util). **Add a comment**: `# ferrod REQUIRES panic = "unwind" (see the workspace profile); the exactly-one-END supervisor depends on it.`

- [ ] **Step 2: failing codec test** — same three tests as v1 (`encode_then_decode_roundtrips_a_frame`, `decode_waits_for_full_payload`, `decode_rejects_bad_magic`) BUT the error type in assertions is `FrameError` (e.g. `assert!(matches!(codec.decode(&mut buf), Err(FrameError::Codec(_))))`).

- [ ] **Step 3: run → FAIL.**

- [ ] **Step 4: implement `codec.rs`** — the `InFrame`/`OutFrame`/`FrameCodec` from v1, plus:

```rust
// src/session/codec.rs (error type)
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("codec: {0}")]
    Codec(#[from] ferro_proto::CodecError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
```
and `impl Decoder for FrameCodec { type Error = FrameError; ... Header::decode(&src[..HEADER_LEN])? ... }` (the `?` converts `CodecError -> FrameError` via the `#[from]`), `impl Encoder<OutFrame> for FrameCodec { type Error = FrameError; ... }`. Body identical to v1's codec logic (header decode/bounds, `split_to`/`advance`/`freeze`, oversize guard on encode).

- [ ] **Step 5: run → PASS; fmt/clippy clean.**  **Step 6: commit** `feat(s3): ferrod crate + Framed codec (FrameError over ferro-proto CodecError + io::Error)`.

---

### Task 2: UDS listener + SO_PEERCRED (safe) + stale-unlink + config + epoch

**Files:** Create `src/config.rs`, `src/peercred.rs`, `src/listener.rs`, `src/epoch.rs`; `tests/peercred.rs`.

**Interfaces:** `Config` (env + defaults, incl. `session_cap_bytes = 16*1024*1024` literal, `max_inflight`, `peer_allow_uids: Vec<u32>` where empty = allow only the daemon's own uid); `peercred::peer_uid(&impl AsFd) -> io::Result<u32>` via a **safe** rustix/nix wrapper (NO raw libc — `unsafe_code` is forbidden); `Config::uid_allowed(uid)`; `listener::bind_uds(&Config)` (unlink stale path first); `epoch::{BootEpoch, EpochSource}` (real `getrandom` + injectable).

- [ ] **TDD tests:** `peer_uid` on a `UnixStream::pair()` returns the current uid; `uid_allowed` honors the allow-list AND the empty-list=self-only default (test both); `bind_uds` unlinks a pre-existing socket file and succeeds; the **deny path** is tested in Task 3's peercred integration test (needs the accept loop), not here — this task unit-tests `peer_uid`/`uid_allowed`/`bind_uds` only.
- [ ] **Implement** peercred with the exact safe rustix 0.38 symbol (confirm at impl against `AsFd`; if 0.38 lacks a safe peercred getter, use `nix::sys::socket::getsockopt(.., PeerCredentials)` — add `nix` and keep it safe). listener via `tokio::net::UnixListener` + `std::fs::remove_file` guard. epoch via `getrandom` behind an `EpochSource` trait.
- [ ] **Commit** `feat(s3): UDS listener (stale-unlink) + safe SO_PEERCRED + config + boot epoch`.

---

### Task 3: session task + handshake + `TYPE_REGISTRY_HASH` + writer task

**Files:** Create `src/session/{writer.rs, handshake.rs, error.rs}`, extend `mod.rs`; `tests/common/mod.rs`, `tests/handshake.rs`. **Also** `engine/crates/ferro-proto/build.rs` (+ maybe a tiny helper): add `pub const TYPE_REGISTRY_HASH: &str` = a hex hash (inline FNV-1a is fine) of the committed `registry.lock.json` bytes (build.rs already has `rerun-if-changed` on it — no registry-schema change, no gen-php/registry_sync churn; PHP-side parity deferred to the PHP client slice).

**Interfaces:** `Session::run(stream, config, epoch)`: spawn the writer task (owns the write half, drains control-then-data); read the first frame — require `core/HELLO` else session-fatal (`rid=0` Outcome::Error close); verify `hello.type_registry_hash == ferro_proto::TYPE_REGISTRY_HASH` else session-fatal `Unsupported` close; reply `HELLO_ACK{engine_version, boot_epoch, features:0, pools:[], type_registry_hash}` echoing the HELLO request_id (control channel, flags=0); then enter the reader loop (answers PING in this task; extended in T4-T6). `tests/common` = a **real bound `UnixListener` + client `UnixStream::connect`** harness (so the accept-loop/peercred path is exercisable), with `send(OutFrame)` and `recv() -> InFrame` where **recv wraps `Framed::next()` in a real-time `tokio::time::timeout`** (a missing/mis-ordered frame or a writer deadlock fails fast, never hangs CI).

- [ ] **TDD tests:** HELLO→HELLO_ACK round-trips, ACK echoes the HELLO request_id and carries the injected `boot_epoch`; a non-HELLO first frame → one `rid=0` Outcome::Error frame then EOF; a wrong `type_registry_hash` → `Unsupported` Outcome::Error then EOF; **two separate connections to the SAME running instance receive identical `boot_epoch`** (the §19.1 property), and a real `getrandom` source yields some value (injected-equality kept only as a determinism aid).
- [ ] **Implement** writer task (select over control + data receivers, control prioritized), handshake, and `error.rs` mapping `SessionError -> Outcome::Error(ErrorPayload)` using `consts::errc` (type-registry mismatch → `UNSUPPORTED`; header faults → `PROTOCOL`; peercred → `AUTH`).
- [ ] **Commit** `feat(s3): session/writer/handshake + ferro-proto TYPE_REGISTRY_HASH + timeout harness`.

---

### Task 4: in-flight registry + consuming-typestate Responder + supervisor (exactly-one-END)

**Files:** Create `src/session/{registry.rs, responder.rs, supervisor.rs}`, extend `mod.rs`; extend `tests/session_rules.rs`.

**Interfaces:**
- `Registry` (`std::sync::Mutex<HashMap<u32, InFlight>>`): `insert(id) -> Result<Guard, ReuseOrFull>` (rejects a reused in-flight id and `max_inflight`); entry removed by the **supervisor** when the handler resolves (NOT from Drop).
- `Responder` (consuming typestate): `end_ok(self, body: Bytes)`, `end_error(self, ErrorPayload)`, `end_cancelled(self)` — each enqueues the terminal (`Outcome::encode`) on the reserved control permit, **then** sets `terminated: Arc<AtomicBool>`; returns a `Terminated` token. Holds the request id + control permit + `terminated`.
- `supervisor`: for each request, `tokio::spawn` the handler returning its `JoinHandle`; the session task awaits handles; on resolve, if `join.is_err() && is_panic()` OR `terminated == false`, emit exactly one terminal `Outcome::Error(ErrorPayload{code: errc::PROTOCOL})` with a **distinct "no terminal from handler" detail string** on that id, then remove the registry entry.

- [ ] **TDD tests (deterministic, barrier-gated — no sleeps):**
  - `end_ok yields exactly one END, supervisor adds none`: a handler that calls `end_ok` → client reads one END for the id; then a `tokio::time::timeout` read asserts **no second frame** (at-most-one).
  - `panic without end_* → exactly one supervisor terminal with the distinct code`: the handler panics WITHOUT calling any `end_*` (so the ONLY route to an END is the supervisor); assert one END whose ERROR payload carries the distinct "no terminal" detail; then assert the **session still answers a subsequent PING** (panic isolation — the connection survives).
  - `reused in-flight id → diagnostic Protocol error on that (nonzero) id, original undisturbed`: use a barrier (`tokio::sync::Notify`) to hold request A in-flight; send a second frame reusing A's id → client gets a Protocol error frame for the reuse (counted separately, NOT via a Responder), then release A → A still emits its own single terminal END.
  - `max_inflight exceeded → per-request error, session survives`.
- [ ] **Implement** per the decided architecture; document the concurrency model at the top of `mod.rs`.
- [ ] **Commit** `feat(s3): registry + consuming-typestate Responder + supervisor (exactly-one-END incl. panic)`.

---

### Task 5: liveness (PING/PONG) + GOODBYE drain + CANCEL (flag-based) + WINDOW_UPDATE

**Files:** `src/session/liveness.rs`, `src/session/flow.rs` (primitive), extend `dispatch.rs`, `mod.rs`; extend `tests/session_rules.rs`.

**Interfaces:** PING→PONG (echo request_id + token, control channel, flags=0) answered while a request is in flight (multiplexing); `GOODBYE` → stop accepting new requests, let in-flight finish, then close; `CANCEL` (flag on a frame, request_id=target, empty payload) sets a per-request cancel flag — the handler observes it and calls `end_cancelled()` (advisory + idempotent; no-op if already terminated/unknown); `WINDOW_UPDATE` (core method, request_id=target, `{frames,bytes}`) applies credit to that request's `flow::Credit`.

- [ ] **TDD tests (barrier-gated):** a PING sent while request A is held in-flight (via Notify) gets its PONG **before** A's terminal (multiplexing); `CANCEL` on the held A is idempotent and A terminates in exactly one END with **`Outcome::Cancelled`** (assert the status is Cancelled, not Error); `CANCEL` on an unknown/completed id is a no-op (no frame); `GOODBYE` with A in-flight → A's terminal is delivered, then EOF; `WINDOW_UPDATE` unit-tests `flow::Credit` debit/replenish directly.
- [ ] **Commit** `feat(s3): PING/PONG, GOODBYE drain, flag-based CANCEL (Cancelled terminal), WINDOW_UPDATE`.

---

### Task 6: reader classification + flag validation + dispatch table + flow cap

**Files:** Create `src/session/classify.rs`, extend `src/session/flow.rs`, `src/dispatch.rs`, `mod.rs`, `error.rs`; extend `tests/session_rules.rs`.

**Interfaces:**
- `classify.rs`: a **pure** step mapping a decode result → `Classification { Frame(InFrame) | PerRequestErr{rid, ErrorPayload} | FatalErr(ErrorPayload) | NeedMore }`, applying `flags::validate` (RESERVED set → fatal `UnsupportedFlag`; `UnknownFlags` → per-request skip) and the G-4 fatal/per-request split. This function is what the fuzz target (Task 8) exercises.
- `dispatch.rs`: `(service, method) -> Route`: core control methods → their handlers (non-registry); SQL/TX → per-request `NonRetryable{Unsupported}` stub (real handlers S4/S5); STREAM/ADMIN → `Unsupported`.
- `flow.rs`: per-session `session_cap_bytes` aggregate cap alongside the per-request `Credit`.

- [ ] **TDD tests:** unknown service/method → per-request `Unsupported` END, session survives; a set reserved flag (`OOB_FD`) → session-fatal close; an unknown non-reserved flag bit → per-request skip + session survives; `payload_len > MAX_FRAME_PAYLOAD` → session-fatal close, zero payload read; `sql`/`tx` method → `Unsupported` stub (no panic); credit + session-cap unit-tested.
- [ ] **Commit** `feat(s3): pure reader classification + flag validation + dispatch stubs + session flow cap`.

---

### Task 7: SIGTERM drain + main wiring

**Files:** `src/shutdown.rs`, finalize `src/main.rs`; `tests/shutdown.rs`.

**Interfaces:** injectable drain token; `main` builds the multi-thread runtime, binds the listener, peercred-gates + spawns a session task per accept, and on drain stops accepting, lets in-flight sessions drain up to `drain_deadline`, then hard-closes. Drain sets the per-session drain flag so handlers can `end_cancelled()` cooperatively (never abort).

- [ ] **TDD/integration test (injected token, not a real signal):** with request A in-flight (barrier), trigger drain → new connections refused while A finishes with its one END → server exits within the deadline.
- [ ] **Manual run:** `cargo run -p ferrod` binds `$FERRO_SOCK`/default; the harness completes HELLO/PING/GOODBYE. Capture it.
- [ ] **Commit** `feat(s3): SIGTERM drain + main wiring (bind, peercred accept loop, graceful shutdown)`.

---

### Task 8: `session_classify` fuzz target

**Files:** `engine/crates/ferrod/fuzz/{Cargo.toml, fuzz_targets/session_classify.rs}`.

**Interfaces:** fuzz the **pure `classify` step** (Task 6) — arbitrary bytes → drive `FrameCodec::decode` + `classify` in a loop that **breaks on `NeedMore`/empty** — asserting it never panics and every input terminates in a typed `FatalErr`/`PerRequestErr` or clean end (not just re-fuzzing S1's `Header::decode`). Own `[workspace]` table; deps `libfuzzer-sys`, `ferrod` + `ferro-proto` paths.

- [ ] **Run in nightly Docker** (host has no nightly): `docker run --rm -v "$PWD":/w -w /w rustlang/rust:nightly bash -c 'cargo install cargo-fuzz --locked && cd engine/crates/ferrod && cargo +nightly fuzz run session_classify -- -runs=50000 -max_total_time=90'` — no crash. Add to CI's fuzz-smoke lane. Fallback: CI-deferred (documented) if the container path is infeasible.
- [ ] **Commit** `test(s3): session_classify fuzz (reader classification never panics)`.

---

### Task 9: `ferrod` container image + sidecar compose (deferred from S2)

**Files:** `testkit/Dockerfile.ferrod`; extend `testkit/docker-compose.yml` (add `ferrod` sidecar sharing a `/run/ferro` socket volume with `pg`).

- [ ] **Build + run:** `docker build -f testkit/Dockerfile.ferrod -t ferrod:dev .` (multi-stage rust:1.95 builder → slim runtime); `docker compose up -d ferrod`; an exec'd or mounted client completes HELLO/PING over the shared socket volume. Capture it. (No DB yet.)
- [ ] **Commit** `feat(s3): ferrod container image + sidecar compose (shared socket volume)`.

---

## Self-Review (v2)

- **Blockers fixed:** FrameError codec Error type (B1); spawn-per-request + supervisor committed up front, no undecided model (B2); two-channel writer + reserved control permit so terminals are never backpressure-dropped, Full-vs-Closed moot (B3).
- **Majors fixed:** every terminal is `Outcome::encode()` incl. session-fatal + synthetic (M1); one-END/registry scoped to request-bearing services, control/liveness excluded (M2); synthetic terminal reserved for panic/no-terminal with a distinct code, CANCEL/drain flag-based → Cancelled status (M3, M6); reused-id diagnostic frame separate, original undisturbed, oracle refined (M4); `std::sync::Mutex` registry, removal off the Drop path (into the supervisor) (M5); flag-validation added to reader + G-4 set, agrees with Task 6 (M7); session-fatal frame pinned to CORE/rid=0/END/Outcome::Error (M8); peercred safe-wrapper only, libc fallback deleted (M9); deny tested via allow-list-excludes-self + real accept loop (M10); barrier-gated in-flight tests, no sleeps (M11); fuzz targets the classify step (M12); one-END test asserts at-most-one via timeout + distinct synthetic code + panic-without-end (M13).
- **Minors fixed:** panic=unwind pinned + supervisor JoinError path (m1); rid conventions stated (m2); type-registry mismatch code pinned to Unsupported (m3); session_cap_bytes own literal (m4); TYPE_REGISTRY_HASH by hashing the lock, no schema churn, PHP parity deferred (m5); harness recv() real-time timeout (m6); epoch test = two connections same instance (m7).
- **Deferred (noted):** real SQL/TX handlers + stream producers consuming credit (S4/S5); Rust rejecting map-encoded messages (harden at S5 decode of untrusted client EXEC); PHP flags::validate + TYPE_REGISTRY_HASH parity (PHP client slice).
- **Execution-time confirmations:** exact `rustix`/`nix` peercred symbol, `tokio-util 0.7` codec signatures, `getrandom 0.2` API, `Sender::reserve_owned` availability — tests assert behavior; nightly Docker for fuzz (fallback documented).

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), review after each (probe one-END at-most-one, panic isolation + session survival, the fatal/per-request split, terminal envelope), then a whole-branch adversarial review before S4.
