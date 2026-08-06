# Golden protocol vectors

This directory is the single cross-language conformance artifact for the Ferro wire protocol.
Both the Rust engine codec (`ferro-proto`) and the PHP client codec assert against these exact
bytes — neither language's test suite is the source of truth; these files are.

## Layout

- `*.json` — positive vectors: one complete, valid frame per case, covering the CORE messages, the
  `ERROR`/`Outcome` envelopes, and the SQL / TX / STREAM services. The authoritative index (what
  each vector locks and why) is `/proto/PROTOCOL.md` §7, with the per-service tables at §8.3, §9.6
  and §10.3 — deliberately not duplicated here, and deliberately not a file count, which drifts.
- `negative/*.bin` — malformed frame seeds that a conformant decoder MUST reject (4 files:
  `bad_magic`, `bad_version`, `oversize_len`, `reserved_flag`). These are raw bytes only (no
  JSON sidecar) since there is no canonical decoded form to assert — the point is rejection.
  They also double as a fuzz corpus seed.

## Positive vector JSON schema

```json
{
  "name": "ping",
  "header": { "flags": 0, "service": 1, "method": 3, "request_id": 9 },
  "message": { "token": 7 },
  "frame_hex": "f7010000...."
}
```

- `name` — the vector's case name, matching the filename stem.
- `header` — the logical header fields (`flags`, `service`, `method`, `request_id`) as decoded
  from the frame, for quick human/CI cross-checking without hex-decoding `frame_hex` first.
  `payload_len` is intentionally omitted here since it is fully implied by `frame_hex`'s length
  (see below) and duplicating it invites drift.
- `message` — the logical, language-neutral JSON rendering of the decoded payload. This is
  documentation/fixture context for humans and other implementations; conformance tests assert
  on `frame_hex`, not on this field. Values that don't fit JSON's number range losslessly
  (e.g. a `u64` `boot_epoch` above `2^53`) are rendered as decimal **strings**.
- `frame_hex` — the **full wire frame**, lowercase hex, no separators: the 16-byte header
  (magic, version, flags, service, method, request_id, payload_len — all little-endian) followed
  immediately by the MessagePack-encoded payload. `frame_hex.len() / 2 == 16 + payload_len`.

## Regenerating

```
cargo run -p ferro-proto --bin gen-vectors
```

This overwrites every file in this directory deterministically from `ferro-proto`'s own
encoders (`Header::encode` + each message type's `.encode()`), so the on-disk vectors are by
construction the canonical encoder output. Regenerated vectors **must be committed** — this
directory is not build output; it is checked-in fixture data that both codecs' test suites read
at test time (see `ferro-proto/tests/golden_vectors.rs` and the PHP conformance test, Task 9).

Do not hand-edit the generated files. If a case needs to change, change
`ferro-proto/src/bin/gen_vectors.rs` and regenerate.
