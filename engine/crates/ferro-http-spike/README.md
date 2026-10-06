# ferro-http-spike — M6-F1a, the Ferro HTTP premise spike

**This is not `ferro-http`.** The crate ships no library code. Its tests prove or refute, against
the real `hyper` 1.11.1 / `h2` 0.4.19 / `rustls` 0.23.45 (`ring` 0.17.14) stack and against
`ferrod`'s real `serve`, the premises SPEC §23.19 owes to slice F1a, the v1 critical path. The
precedent is `ferro-sqlite-spike` (SPEC §22.2 (ba)). The SPEC record is §22.2 (ct).

Every premise has a **negative control**, so its tests cannot pass vacuously, and the tests were
**mutation-checked** (table below). The slice's adversarial review found defects in the first
draft and in the spec (review F-1 to F-6, recorded in SPEC §22.2 (ct)). Each fix below has a test that failed
before it. Everything runs on loopback or in memory; nothing needs a server
or the network.

```
cargo test -p ferro-http-spike          # 25 tests, ~10 s (P7's two 3 s holds dominate)
```

## Verdicts

| Premise | Verdict | Test file |
|---|---|---|
| P1 — `sent` is exact on HTTP/1.1 | **HOLDS**, with the tracker amended: through TLS it needs a ciphertext half (review F-1) | `p1_p19_sent_it.rs` |
| P19 — `try_send_request` agrees with the tracker | **HOLDS WITH CAVEAT** (one direction only) | `p1_p19_sent_it.rs` |
| P14 — "sent, no head" causes are distinguishable | **HOLDS WITH CAVEAT** (three caveats; the read count starts at dispatch, review F-4) | `p14_causes_it.rs` |
| P15 — head limits are settable; oversize is an error | **HOLDS WITH CAVEAT** (`max_buf_size` is not exact) | `p15_head_limits_it.rs` |
| P7 (h1) — credit-gated reading backpressures, bounded | **HOLDS** (the probe now holds 3 s after the stall, review F-2) | `p7_backpressure_it.rs` |
| P3 — deps pass `deny.toml`, no CMake | **HOLDS WITH CAVEAT** (`tokio-rustls` default features; the licence gate skipped dev-only crates until `include-dev = true`) | `p3_p16_deps_it.rs` + manual |
| P16 — `unicode-normalization`'s licence is allowed | **HOLDS** | `p3_p16_deps_it.rs` |
| P17 — sessions keep reading frames during the drain | **HOLDS** (bounded by `drain_deadline`) | `p17_drain_it.rs` |

**No premise is refuted, but §23.7.1 is amended.**
- `sent` is the OR of a plaintext and a ciphertext count (F-1).
- The fate table gains a "dispatched, not sent" phase, the state P1's own controls produce (F-3).
- The caveats change the text around the table: the P19 cross-check, the cause-token derivations
  in §23.5.4/§23.5.6, the head limit in §23.9.1, the unsent tokens' class in §23.11.3 and the
  feature note in §23.13. All are amended with §22.2 (ct).

## Evidence

### P1 — HOLDS

A tracker wraps the plaintext I/O below `hyper`. It counts the bytes each `poll_write` and
`poll_write_vectored` *accepts*, separately before and after it is armed, and the bytes each read
delivers.

- **Before dispatch, zero bytes.** After the handshake, with the connection task running idle for
  150 ms, `written_unarmed == 0`.
- **After dispatch, exact.** A 300 KiB POST completes and `written_armed` **equals** the byte count
  the upstream received.
- **Through TLS, on success.** The plaintext tracker wraps the `TlsStream` and a `CipherTap`
  wraps the TCP stream below TLS. The two share one state. The plaintext count equals what the
  upstream *decrypted*, and the ciphertext count is larger, since every byte travels inside a
  record. Handshake bytes precede arming, and `arm()` zeroes every per-exchange count.
- **Through TLS, on a failed write (review F-1, the control the first draft lacked).**
  `tokio-rustls` accepts a plaintext write into rustls and then writes the records in a loop. If a
  later record's write fails, the call returns `Err`, although earlier records already reached the
  socket. Below the tap, the test lets the first post-dispatch socket write through and fails the
  second. Measured:
  - The upstream decrypted the request head plus 16 384 body bytes.
  - The plaintext layer accepted **0** bytes, and the test asserts that, so it provably exercises
    the hole.
  - The ciphertext count is non-zero, so `sent == true`.

  The pre-review tracker (plaintext only, mutation M-P1c) fails this test. The rule is now: `sent`
  is plaintext accepted since dispatch > 0 **or** ciphertext written since dispatch > 0.
  It is conservative: a TLS alert after dispatch also counts.
- **Control 1 (required): dead at the first write.** An I/O whose every write fails gives
  `sent == false`, and the first error is on the write side, so the control did exercise a write.
- **Control 2 (required): reset before dispatch, over real TCP.** `hyper`'s idle connection task sees
  the reset and ends. The request then fails with `sent == false`, and the upstream received 0
  bytes. The TLS form (`close_notify` before dispatch) also has 0 ciphertext bytes after arming.
- **Control 3: the probe can see pre-dispatch bytes.** Run on `hyper`'s *HTTP/2* client, the same
  probe sees at least 24 bytes before any request exists (the preface and SETTINGS). The h1 zero is
  therefore a property of h1, not a blind probe. This is also why §23.7.1 defines `sent` differently
  for HTTP/2.

`hyper` writes **vectored** on both TCP and TLS. A tracker that counted only `poll_write` would see
nothing; mutation M-P1b proves it.

### P19 — HOLDS WITH CAVEAT

| Case | Message returned | Tracker `sent` | Agree? |
|---|---|---|---|
| A. Connection closed before dispatch (never dequeued) | yes | false | yes |
| B. Request written, upstream closes with no response | no | true | yes |
| C. `hyper` serialises the request into **its own** buffer; the first I/O write fails | **no** | **false** | **no** |

`try_send_request` returns the message only when the dispatcher never dequeued it. Once dequeued,
the request is serialised into `hyper`'s write buffer, and a failure before any byte reaches the I/O
still loses the message. The cross-check is sound in **one direction only**: *message returned ⇒ not
sent*. "No message" does **not** mean "sent". The tracker remains the authority, as §23.7.1 already
said. `try_send_request` can confirm a not-sent; it can never confirm a sent.

### P14 — HOLDS WITH CAVEAT

Each fault is injected after the request was sent, and is classified only from the `hyper::Error`
and the tracker. Every case is `sent`.

| Fault | `hyper` error | Distinguished by | Token |
|---|---|---|---|
| FIN after the request, 0 response bytes | `IncompleteMessage` | tracker read count = 0 | `eof_empty` |
| FIN mid-head | `IncompleteMessage` (identical) | tracker read count > 0 | `eof_partial_head` |
| RST after the request | `Io(ConnectionReset)` | — | `reset` |
| RST while the client still writes a 128 MiB body | `Io(ConnectionReset)`, **read side** | (not separable) | `reset` (5/5 runs) |
| Partial head, then RST | `Io(ConnectionReset)` | — | `reset` |
| `XTTP/1.1 …` | `Parse` | `Display` ≠ too-large | `malformed_head` |
| 257 fields; a 512 KiB head | `Parse(TooLarge)` | `Display` = `"message head is too large"` | `oversize_head` |

The caveats:

1. **`eof_empty` and `eof_partial_head` are the same `hyper` error.** Only the tracker's read count
   separates them. The control proves this: with the tracker withheld, the classifier collapses
   them. It matters, because §23.11.3 maps them to *different* Guzzle classes (`ConnectException` for
   52, `RequestException` for 56/8). The tracker's read count is the mechanism, and `HttpStats`
   already carries it as `bytes_received`.
2. **`oversize_head` and `malformed_head` separate only by `Display` text.**
   `hyper::Error::is_parse_too_large()` is `#[cfg(all(feature = "http1", feature = "server"))]` in
   1.11.1, so D20's client-only feature set does not compile it. This spike's first draft called it
   and failed to build. The separating signal is the `Display` string, pinned by
   `p14_oversize_and_malformed_differ_only_by_display_text`, so a `hyper` upgrade that changes it
   fails loudly. If it ever drifts, the result degrades to `malformed_head`: same Guzzle class, same
   fate. The alternative is enabling hyper's `server` feature, which adds only `httpdate`, but that
   would change D20's "client side only". Not taken.
3. **`write` and `reset` are not reliably separable.** `hyper`'s h1 client reads for an early
   response head *while* writing the body, and polls the read side first. A reset during the body
   write was observed as a read-side `ConnectionReset` in 5/5 runs. The token `write` is reachable
   only when the write side fails first. Both are `RequestException` and both are the same fate
   (sent, no head), so **§23.11.3 needs no amendment**. P14's "stricter class for both" rule is
   already satisfied.

**The read count starts at dispatch (review F-4).** F4 reuses keep-alive connections, so the count
that separates `eof_empty` from `eof_partial_head` must be this exchange's.
`p14_eof_empty_on_a_reused_connection_counts_reads_from_dispatch` runs two exchanges on one
connection:
- The first reads a 40-byte response.
- The second is closed with no response bytes and classifies `eof_empty`.

Its control asserts that the connection's lifetime count is ≥ 40, so a lifetime count would have
said `eof_partial_head`. Mutation M-P14c (no reset at `arm()`) fails it.

**`write` can be named.** `p14_control_a_write_side_failure_is_named_write` uses an I/O that accepts
4 096 bytes and then fails its writes, while its reads never complete. The cause is `write`. So the
"usually `reset`" finding above comes from `hyper` polling the read side first, not from a
classifier that cannot see writes.

Also measured: **`hyper` returns a 101 as an ordinary response head** (`Ok`, status 101). The engine
must therefore intercept it as `informational_101`. Other 1xx responses (a 103 was tested) are
consumed by `hyper` and never surface. The 103 arm is this observation's control: what surfaces is
specific to 101.

### P15 — HOLDS WITH CAVEAT

These tests run over an in-memory scripted I/O, so the read chunking `hyper` sees is fixed by the
test and not by TCP. Every result here is deterministic.

- **`max_headers(256)` is exact.** 256 fields are delivered whole (all 256 present, not truncated);
  257 are refused, at every chunking (one read, 4 KiB, 1 byte). Controls: `max_headers(300)` accepts
  257, and hyper's default of 100 refuses 101.
- **`max_buf_size(256 KiB)` is an error, never a truncation, but it is NOT an exact head limit.**
  - With reads ≤ 4 KiB the boundary is exact: 262 144 B delivered, 262 145 B refused.
  - Delivered in **one read**, heads up to **507 904 B** were delivered (found by bisection).
    `hyper` checks the buffer length only after a failed parse attempt, and one read fills the
    buffer's whole spare capacity, which the `bytes`/`Vec` amortised growth has made larger than the
    limit.
  - The ceiling is hard. `hyper` (`proto/h1/io.rs`) reads only while the buffer holds fewer than
    `max_buf_size` bytes. It asks for at most `max_buf_size − len` more, and reserves only when the
    spare capacity is smaller than that. A reservation therefore starts from a capacity below
    `max_buf_size` and grows it to `max(2 × capacity, required)` (`Vec::reserve`'s rule, which
    `BytesMut` follows), with `required ≤ max_buf_size`. The capacity, and with it what one read
    can deliver, stays **< 2 × `max_buf_size`** (< 512 KiB). A head of exactly 2× was refused under
    all five delivery shapes tried, and the review found the same 507 904 B maximum after a
    preceding 1xx.
  - Control: `max_buf_size(1 MiB)` delivers that 2× head.
  - Consequence: §5.2's large-frame rule still holds by a wide margin. But §23.9.1's "a head one byte
    over is refused" (slice F4's test) cannot rest on `hyper`. The engine must apply the exact limit
    to the parsed head itself, with `max_buf_size` as the memory backstop (§23.9.1 amended).
- **HTTP/2 `max_header_list_size(256 KiB)` is settable, and an oversize head is an error.**
  - Under the limit, the head is delivered whole.
  - **Hyper's own default is 16 KiB** (`proto/h2/client.rs`), not h2's 16 MiB. The control shows a
    20 KiB head refused without the setter and delivered with it.
  - An over-limit head fails as a **library-initiated `RST_STREAM(PROTOCOL_ERROR)`**, `is_remote()
    == false`. That is the same error `h2` raises for a *malformed* header block, so on HTTP/2
    `oversize_head` cannot be told from malformed. Both are `h2_stream_error`, and on idempotent-only
    HTTP/2 both have the same fate. (`h2`'s `recv.rs` comment promising `REFUSED_STREAM` is stale.)
  - Far over the limit (2 MiB against 256 KiB), `h2`'s CONTINUATION cap turns the refusal into a
    **connection-level `GOAWAY(ENHANCE_YOUR_CALM)`**, which fails every stream on that connection.
    That is §23.1's cross-tenant coupling: a retry for the other idempotent streams, never a fate
    loss.
  - For F1b's P2: here the server *processed* the request and the reset is library-initiated. That
    is the shape P2's negative controls must refuse to license.

### P7 (h1) — HOLDS

Both sockets pin `SO_RCVBUF`/`SO_SNDBUF` to 128 KiB, which turns autotuning off, so the honest
excess is small and the same on any host. The "engine" polls the body while 1 MiB of credit lasts,
then stops. The upstream offers 512 MiB.

| Run | Upstream wrote past what was delivered | Stalled | Resumed on new credit |
|---|---|---|---|
| Content-Length, credit-gated | ~0.65 MB | yes | yes |
| chunked, credit-gated | ~0.59 MB | yes | yes |
| **Control:** read-ahead client, Content-Length | 535 822 336 B | — | — |
| **Control:** read-ahead client, chunked | 535 896 064 B | — | — |

The bound is 4 MiB. The control must exceed 64 MiB, and it overshoots by 511 MiB.

Calibration follows the §22.2 (bj) rule: a stall probe proves something only if the mutated code
being merely *slow* cannot also pass it.

- **Fast read-ahead.** Mutation M-P7b throttles the read-ahead to ~25 MB/s (4 ms per 64 KiB frame).
  The probe still fails it: it never stalls, and the excess reaches 118 MiB in 5 s.
- **Slow read-ahead (review F-2).** The first draft declared a stall after one 300 ms quiet window.
  So a reader that honours the credit and then keeps reading past it **one frame per 350 ms** passed
  the positive test: excess 3.7 MB, under the bound, "stalled". Its claim to have applied (bj) rested
  only on the 4 ms throttle.
  - **The fix:** after the stall, the probe **holds for 3 s** and requires the upstream's written
    count not to move at all.
  - **The slowest read-ahead it catches** is one frame per ~3.3 s, the 300 ms window plus the hold.
    Measured: caught at 2.9 s and 3.2 s per frame, missed at 4 s.
  - **A slower reader** still leaks one 64 KiB frame per period past the credit. It is not
    detectable in a bounded test, and its harm is bounded by its rate.
  - `p7_control_a_slow_read_ahead_client_fails_the_probe` keeps the review's mutation as a permanent
    control at 350 ms (measured excess 1.6 MB, `held = false`) and at 2 s.
  - The review's mutation applied inside the positive test (M-P7d) and a probe without the hold
    (M-P7c) are both killed.
  - The positive runs hold for the full 3 s, with the excess unchanged (~0.65 / ~0.59 MB).
- **Resumption.** Replenished credit resumes the upstream, so the stall is backpressure and not a
  dead connection.

### P3 — HOLDS WITH CAVEAT

- **The resolved tree.** The workspace `Cargo.lock` holds the whole D20 set: `hyper` 1.11.1,
  `hyper-util` 0.1.21, `http` 1.5.0, `http-body` 1.1.0, `tokio-rustls` 0.26.6, `rustls` 0.23.45,
  `rustls-native-certs` 0.8.4, `rustls-pki-types` 1.15.1, `unicode-normalization` 0.1.25, `ring`
  0.17.14 and `rustls-webpki` 0.103.15.
  - It holds none of `aws-lc-rs`, `aws-lc-sys`, `cmake`, `webpki-roots`, `openssl` or `openssl-sys`.
  - It holds no `httpdate`, so the build is client-only (the condition behind P14 caveat 2).
  - Control (table-driven since review F-5): each of those seven names is injected into the real
    lock **one at a time**, and the scanner must report exactly it. The table is a literal separate
    from the scanner's list, so dropping a name from the list fails the control (M-P3b, M-P3c).
- **No CMake, measured by building.** `ring`, `rustls`, `rustls-webpki` and `tokio-rustls` were
  rebuilt from clean with a `cmake` shim first on `PATH` and as `$CMAKE`. The shim records any
  invocation and exits 1. The build succeeded, the TLS test passed, and the shim was never invoked.
  Control: the shim does record a direct `cmake --version`. `ring` builds with the existing C
  compiler via `cc`.
- **Licences**, read from each crate's manifest:

  | Crate | Licence |
  |---|---|
  | `ring` | `Apache-2.0 AND ISC` |
  | `rustls-native-certs` | `Apache-2.0 OR ISC OR MIT` |
  | `rustls` | `Apache-2.0 OR ISC OR MIT` |
  | `rustls-webpki`, `untrusted` | `ISC` |
  | `hyper`, `hyper-util`, `h2`, `want` | `MIT` |
  | `tokio-rustls`, `rustls-pki-types`, `httparse`, `openssl-probe` | `MIT OR Apache-2.0` |
  | `tinyvec` | `Zlib OR Apache-2.0 OR MIT` |

  Every one is on `deny.toml`'s allow-list, so `deny.toml` is unchanged.
- **Caveat 1, a trap the spec did not name.** `tokio-rustls`'s **default** features select
  `aws_lc_rs`, the backend D20 rejects (CMake plus the `OpenSSL` licence). `default-features = false,
  features = ["ring", "tls12"]` is load-bearing; this crate's `Cargo.toml` says so. §23.13 is amended
  to list it.
- **Caveat 2, the gate itself.**
  - **The first draft never ran `cargo deny check`.** `cargo-deny` was not installed, the GitHub
    release download was refused by the egress proxy, and the disk was full. It deferred the check
    to CI's `deny` job, claiming that job covers "this crate's dev-dependencies included".
  - **That claim was false.** cargo-deny's licence check skips crates reachable only through
    dev-dependencies unless `[licenses] include-dev = true`.
  - **Measured at the review round,** with cargo-deny 0.20.2 built from crates.io: with `ISC`
    deleted from the allow-list, `cargo deny check licenses` still said `licenses ok`. Every D20
    crate is a dev-dependency here, so CI would have passed a D20 licence violation.
  - **The fix:** `deny.toml` now sets `include-dev = true`. The allow-list is unchanged.
  - **Control:** with `include-dev = true`, the same `ISC` deletion fails with `rejected` on exactly
    `ring` (`Apache-2.0 AND ISC`), `rustls-webpki` and `untrusted` (`ISC`).
  - **With the real allow-list,** the full `cargo deny check` passes: `advisories ok, bans ok,
    licenses ok, sources ok`. A yanked `chacha20` (through `tokio-postgres`) is a pre-existing
    warning.

### P16 — HOLDS

`unicode-normalization` 0.1.25 is `MIT OR Apache-2.0`, and its dependency `tinyvec` is
`Zlib OR Apache-2.0 OR MIT`. Both are allowed, so the fixed-table fallback is not needed.

Beyond the licence, the test pins the property §23.4.2 step 8 relies on. NFKC folds U+FF0E to `.`,
U+2024 to `.` and U+2025 to `..`, so two fullwidth dots become a `..` segment. Control: NFC leaves
all three unchanged, so step 8 is right to require NFKC specifically.

### P17 — HOLDS (bounded by `drain_deadline`)

Measured through the real `ferrod::serve::serve` and `Session`, with the drain triggered by
`ferrod::shutdown::Drain`, the handle `main`'s SIGTERM watcher drives.

- After the drain starts, a session **with a request in flight** and an **idle** session both read
  new frames.
- Each new request is answered with exactly one `END` (`draining`, from a handler that captured the
  `Drain`).
- A raw frame on service 6 is answered too (`Unsupported` today, one `END`).
- The parked in-flight request still completes.

Control: once `drain_deadline` passes, `serve` aborts every session task. A request sent after that
is **lost**: no `END`, the handler never runs, and the client sees EOF. The window therefore exists
but is bounded by the SQL `drain_deadline` (5 s by default). That is exactly why §23.6.1's chassis
change 2 (`serve` waits `FERRO_HTTP_DRAIN_MS + drain_deadline`) is needed.

Observation for chassis change 1: the HTTP service's refusal needs no session-layer plumbing. The
`Drain` reaches a handler by closure capture through the `HandlerFactory` that `main` builds. Today
`main` builds the factory before it creates the `Drain`, which is a two-line reorder. The queue
waker's need is separate and is not measured here.

## Mutation table

Of 22 mutations, 21 were **killed**, and one is equivalent (M-P1d, below). Each was applied alone,
its target tests were run, and the original was restored. The `MUTATION SITE` comments in the tests
mark the sites. M-P1a and M-P1b were re-run after the review's tracker change and are still killed.

| # | Mutation | Killed by |
|---|---|---|
| M-P1a | Tracker sets `sent` when a write FAILS (on the call, not on an accepted byte) | `p1_control_connection_dead_at_first_write…`, `p19_…` |
| M-P1b | Tracker does not count `poll_write_vectored` | `p1_no_byte_before_dispatch…`, `p1_through_tls…`, `p19_…` (and P14's) |
| M-P1c | `sent` ignores the ciphertext count (the pre-review tracker) | `p1_control_tls_records_written_before_a_write_error_count_as_sent` |
| M-P1d | `CipherTap` counts while unarmed | **equivalent, not killable**: `arm()` zeroes the ciphertext count, so handshake bytes cannot survive into an exchange either way |
| M-P14c | `arm()` does not reset the read count (a connection-lifetime count) | `p14_eof_empty_on_a_reused_connection…` (read 40, want 0) |
| M-P14d | Classifier files a write-side failure as `reset` | `p14_control_a_write_side_failure_is_named_write` only |
| M-P14a | Classifier ignores the tracker's read count | `p14_sent_no_head_causes…`, `p14_control_hyper_alone_conflates…` |
| M-P14b | Classifier drops the oversize (`Display`) arm | `p14_sent_no_head_causes…` |
| M-P15a | h1 builder without `max_headers` | `p15_h1_max_headers…` |
| M-P15b | h1 builder without `max_buf_size` | `p15_h1_max_buf_size…` |
| M-P15c | h2 builder without `max_header_list_size` | `p15_h2_max_header_list_size…` |
| M-P7a | Credit-gated reader replaced by a read-ahead drain | `p7_h1_credit_gated…` (excess 511 MiB) |
| M-P7b | Read-ahead throttled to ~25 MB/s | `p7_h1_credit_gated…` (never stalls; 118 MiB in 5 s) |
| M-P7c | The probe does not hold after the stall (the first draft) | `p7_control_a_slow_read_ahead…` (350 ms reader: `held = true`) |
| M-P7d | Review F-2's mutation inside the positive test: after credit, read one frame per 350 ms | `p7_h1_credit_gated…` (`held = false`, excess 1.36 MB) |
| M-P3 | Lock scanner blind | `p3_control_the_scan_sees_every_forbidden_crate` |
| M-P3b | `webpki-roots` dropped from `FORBIDDEN` | `p3_control_the_scan_sees_every_forbidden_crate` |
| M-P3c | `httpdate` dropped from `FORBIDDEN` | `p3_control_the_scan_sees_every_forbidden_crate` |
| M-P16 | NFC instead of NFKC | `p16_nfkc_folds…` |
| M-P17a | The service ignores the drain | `p17_existing_sessions_keep_reading…` |
| M-P17b | **`ferrod`'s `serve.rs`** hard-closes sessions at drain START | `p17_existing_sessions_keep_reading…` |
| M-P17c | **`ferrod`'s `serve.rs`** never hard-closes past `drain_deadline` | `p17_control_after_the_drain_deadline…` |

## Fixtures

`tests/fixtures/` holds a throwaway ECDSA P-256 test CA and a `localhost`/`127.0.0.1` leaf, valid for
100 years, generated with `openssl` for the TLS form of P1. The CA's private key was deleted after
signing. `leaf.key` is a test-only key that nothing trusts outside these tests.

To regenerate them (OpenSSL 3), in an empty directory:

```
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout ca.key -out ca.pem \
  -days 36500 -subj "/CN=ferro-http-spike test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout leaf.key -out leaf.csr \
  -subj "/CN=localhost"
printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n' > leaf.ext
openssl x509 -req -in leaf.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 36500 -sha256 \
  -extfile leaf.ext -out leaf.pem
openssl verify -CAfile ca.pem leaf.pem      # leaf.pem: OK
```

Then copy `ca.pem`, `leaf.pem` and `leaf.key` into `tests/fixtures/` and delete the rest, `ca.key`
above all.
