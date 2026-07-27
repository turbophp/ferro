# Ferro M0 · Slice S1 — Cross-Language Wire Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish `/proto` as the single source of truth and prove **one** wire contract across Rust and PHP — a 16-byte frame header + canonical MessagePack payloads whose bytes are identical from the Rust codec, the pure-PHP codec, and the ext-msgpack codec, locked by golden vectors and a fuzzed decoder.

**Architecture:** A single TOML registry (`/proto/*.toml`) is compiled once by a Rust generator into `registry.lock.json`; both languages generate their constants from that lock file (Rust `build.rs` → consts; committed `gen-php.php` → `Constants.php`), so no protocol number is ever hand-written (charter rule 2). `ferro-proto` is **runtime-free** (encode/decode on byte slices) so the exact codec is shared by the PHP-parity vector tests. Golden vectors in `/proto/vectors/` are the sole cross-language arbiter; `cargo-fuzz` guards the decoder against panics and unbounded allocation.

**Tech Stack:** Rust 1.95 (edition 2024), crates `rmp` (low-level MessagePack, for `Value`), `rmp-serde` + `serde` (message structs), `serde_json` + `toml` (registry tooling), `getrandom`; `cargo-fuzz`. PHP 8.4, PSR-4 `Ferro\`, pure-PHP MessagePack primitives + optional `ext-msgpack`, PHPUnit 11, PHPStan level 9. Docker not required for S1.

## Global Constraints

Copied verbatim from the design doc / spec; every task implicitly includes these.

- **Single source of truth:** `/proto` is the only place protocol numbers exist. Hand-written method/flag/service/error/type constants in Rust or PHP are a review reject. *(charter rule 2, §20.2)*
- **Registry → both codecs in one change:** any protocol change updates `/proto` TOML, `registry.lock.json`, the golden vectors, and both codecs in the same commit. *(§0, §20.2)*
- **Frame header:** exactly 16 bytes, little-endian, field order `magic:u8, version:u8, flags:u16, service:u16, method:u16, request_id:u32, payload_len:u32`. PHP packs it with `pack('CCvvvVV', …)`. *(§5)*
- **magic = `0xF7`, protocol version = `1`.** Both are registry constants, never literals. *(§5)*
- **`MAX_FRAME_PAYLOAD = 16 MiB` (16777216)** — hard codec ceiling; any larger declared `payload_len` → Protocol error with **zero allocation**. Distinct from the `4 MiB` default credit window (flow control, not S1). *(§5.2, decision W-5)*
- **TypedValue** is a 2-element MessagePack array `[tag:int, value]`; `NULL` tag `0` with a `nil` value. No MessagePack ext types. *(§9, decision W-1)*
- **M0 scalar TypedValue set:** `NULL, BOOL, I64, F64, TEXT, BYTES` are encoded/decoded in S1. All other tags are defined as constants but unimplemented. *(decision T-1)*
- **Messages are positional** MessagePack arrays; field order pinned in `/proto/PROTOCOL.md` and enforced by golden vectors. No message-schema IDL. *(decision W-2)*
- **Canonical MessagePack profile (locked by vectors, verified against rmp 0.8.15):** integer fields follow `rmp::encode::write_sint` semantics — **non-negative** values narrow to positive-fixint / uint8 (`0xcc`) / uint16 (`0xcd`) / uint32 (`0xce`) / uint64 (`0xcf`); **negative** values narrow to negative-fixint / int8 (`0xd0`) / int16 (`0xd1`) / int32 (`0xd2`) / int64 (`0xd3`). Unsigned fields (`boot_epoch`) use the same non-negative ladder. Floats always float64 (`0xcb`, big-endian); strings use the `str` family (fixstr/str8/16/32); byte payloads use the `bin` family (`0xc4`+); `None`/`null` → `nil` (`0xc0`); arrays use fixarray/array16/array32. **A uint64 > PHP_INT_MAX decodes in pure PHP to a decimal string** — ext-msgpack cannot represent it losslessly, so pure PHP is authoritative there. *(W-1/W-2, R3; CONFIRMED: `write_sint(200)` = `cc c8` uint8, NOT `d1 00 c8` — positive ≥128 uses unsigned markers)*
- **Error codes** grouped by branch range (`Retryable 0x1xxx / Indeterminate 0x2xxx / NonRetryable 0x3xxx`) **and** carry an explicit `branch:u8` on the wire so an unknown code is still classified. *(decision W-3)*
- **Terminal outcome envelope:** `[status:u8, body]` with `0=Ok, 1=Error, 2=Cancelled`; on Error, `body` is the canonical ERROR array `[code:u16, branch:u8, sqlstate?, errno?, message, detail?, retry_after_ms?]`. *(decision W-4)*
- **Rust: `thiserror` in libs, `tracing` where relevant, clippy warnings denied, `cargo fmt`.** PHP: PSR-12, PHPStan level 9 on `php/client`, dependency-free at runtime (`ext-msgpack` runtime-detected, never required). *(§20.2, charter rule 7)*
- **Workspace:** single Cargo workspace at repo root, `members = ["engine/crates/*"]` in S1 (the `bench` member is added in slice S8 when that crate exists — decision B-1).
- **TDD ordering (every test-bearing task):** observe a RED state before GREEN. Tasks 3–5 do this explicitly. For the generate-then-verify tasks (6, 8, 9), run the new conformance/vector test **once before** generating its artifact (vectors / `Constants.php`) so the failure is actually seen, then generate and re-run to green — a test never seen red risks being silently vacuous.

## File Structure

```
/Cargo.toml                                     workspace root (members, shared lints/profiles)
/rust-toolchain.toml                            pin stable 1.95, edition 2024, components
/proto/
  methods.toml  errors.toml  types.toml         registry (human source of truth)
  registry.lock.json                            machine intermediate (committed; both langs read it)
  PROTOCOL.md                                   field-order + canonical-profile prose, vector index
  vectors/*.json                                positive golden vectors (logical value + expected hex)
  vectors/negative/*.bin                        raw malformed frames (decoder must reject; fuzz seeds)
  vectors/README.md
  tools/gen-php.php                             lock.json -> php/client/src/Protocol/Generated/*.php
/engine/crates/ferro-proto/
  Cargo.toml
  build.rs                                       reads registry.lock.json -> $OUT_DIR consts
  src/lib.rs                                     re-exports; #![forbid(unsafe_code)] except fuzz
  src/consts.rs                                  include!(concat!(env!("OUT_DIR"),"/consts.rs"))
  src/registry.rs                                TOML->lock model (shared by gen bin + sync test)
  src/header.rs                                  Header struct, encode/decode, bounds
  src/flags.rs                                   Flags bitfield helpers
  src/value.rs                                   TypedValue scalar codec (rmp low-level)
  src/messages.rs                                Hello/HelloAck/Ping/Pong/Goodbye/WindowUpdate/Error/Outcome
  src/error.rs                                   CodecError (thiserror)
  src/bin/gen_registry_lock.rs                   TOML -> /proto/registry.lock.json
  src/bin/gen_vectors.rs                         emit /proto/vectors/*.json + negative/*.bin
  tests/header.rs  tests/golden_vectors.rs  tests/registry_sync.rs
  fuzz/Cargo.toml  fuzz/fuzz_targets/{decode_frame,roundtrip_frame}.rs
/php/client/
  composer.json  phpunit.xml.dist  phpstan.neon.dist
  src/Protocol/Generated/Constants.php           generated from lock.json
  src/Protocol/Msgpack/{PackerInterface,PurePacker,ExtPacker,PackerFactory}.php
  src/Protocol/{CodecException,Header,Value,Codec,Message}.php
  tests/Conformance/VectorConformanceTest.php  tests/Conformance/RegistrySyncTest.php
  tests/Unit/{HeaderTest,ValueTest,PurePackerTest}.php
```

---

### Task 1: Workspace + crate + PHP package bootstrap

**Files:**
- Create: `/Cargo.toml`, `/rust-toolchain.toml`, `/engine/crates/ferro-proto/Cargo.toml`, `/engine/crates/ferro-proto/src/lib.rs`, `/engine/crates/ferro-proto/src/error.rs`
- Create: `/php/client/composer.json`, `/php/client/phpunit.xml.dist`, `/php/client/phpstan.neon.dist`

**Interfaces:**
- Produces: workspace that `cargo build` compiles; `ferro_proto::error::CodecError` enum used by every later Rust task; `Ferro\` PSR-4 autoload root used by every later PHP task.

- [ ] **Step 1: Write the workspace root `Cargo.toml`**

```toml
# /Cargo.toml
[workspace]
resolver = "3"
members = ["engine/crates/*"]
# NOTE: `bench` (decision B-1) is added to `members` in slice S8 when the crate is created.
# Listing a non-existent path here fails workspace resolution, so it is added just-in-time.

[workspace.package]
edition = "2024"
rust-version = "1.95"
license = "Apache-2.0"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }

[profile.release]
lto = "thin"
```

- [ ] **Step 2: Pin the toolchain**

```toml
# /rust-toolchain.toml
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

- [ ] **Step 3: Write the `ferro-proto` crate manifest**

```toml
# /engine/crates/ferro-proto/Cargo.toml
[package]
name = "ferro-proto"
version = "0.0.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lints]
workspace = true

# serde_json + toml are in [dependencies] (NOT dev-only): the `registry` module and the two
# gen binaries use them, and binaries/lib can only see [dependencies], not [dev-dependencies].
[dependencies]
rmp = "0.8"
rmp-serde = "1"
serde = { version = "1", features = ["derive"] }
serde_bytes = "0.11"
serde_json = "1"
thiserror = "2"
toml = "0.8"

[build-dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[[bin]]
name = "gen-registry-lock"
path = "src/bin/gen_registry_lock.rs"

[[bin]]
name = "gen-vectors"
path = "src/bin/gen_vectors.rs"
```

- [ ] **Step 4: Write the error type and lib root**

```rust
// /engine/crates/ferro-proto/src/error.rs
use thiserror::Error;

/// Every decode failure is a protocol violation the caller maps to `NonRetryable{Protocol}`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("bad magic: expected 0x{expected:02X}, got 0x{got:02X}")]
    BadMagic { expected: u8, got: u8 },
    #[error("unsupported protocol version: expected {expected}, got {got}")]
    BadVersion { expected: u8, got: u8 },
    #[error("frame payload_len {len} exceeds MAX_FRAME_PAYLOAD {max}")]
    FrameTooLarge { len: u32, max: u32 },
    #[error("unknown flag bits set: 0x{bits:04X}")]
    UnknownFlags { bits: u16 },
    #[error("frame sets a reserved flag (OOB_FD/COMPRESSED) unsupported in M0")]
    UnsupportedFlag,
    #[error("buffer too short: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("malformed messagepack payload: {0}")]
    Malformed(String),
    #[error("trailing bytes after payload: {0} extra")]
    TrailingBytes(usize),
}
```

```rust
// /engine/crates/ferro-proto/src/lib.rs
pub mod error;
pub mod consts;
pub mod flags;
pub mod header;
pub mod value;
pub mod messages;
pub mod registry;

pub use error::CodecError;
```

- [ ] **Step 5: Stub the modules so the crate compiles**

Create empty-but-valid module files (each replaced in a later task):

```rust
// src/consts.rs
include!(concat!(env!("OUT_DIR"), "/consts.rs"));
```
```rust
// src/flags.rs
// (filled in Task 3)
```
```rust
// src/header.rs
// (filled in Task 3)
```
```rust
// src/value.rs
// (filled in Task 4)
```
```rust
// src/messages.rs
// (filled in Task 5)
```
```rust
// src/registry.rs
// (filled in Task 2)
```

Because `consts.rs` needs `$OUT_DIR/consts.rs`, add a placeholder `build.rs` that writes an empty consts file (replaced in Task 2):

```rust
// /engine/crates/ferro-proto/build.rs
use std::{env, fs, path::Path};
fn main() {
    let out = env::var("OUT_DIR").unwrap();
    fs::write(Path::new(&out).join("consts.rs"), "// generated in Task 2\n").unwrap();
}
```

The manifest declares two `[[bin]]` targets whose real sources are created later (`gen_registry_lock.rs` in Task 2, `gen_vectors.rs` in Task 6). `cargo build` HARD-ERRORS on a declared `[[bin]]` whose source file is absent, so create placeholder stubs now (both overwritten later):

```rust
// /engine/crates/ferro-proto/src/bin/gen_registry_lock.rs
fn main() {}
```
```rust
// /engine/crates/ferro-proto/src/bin/gen_vectors.rs
fn main() {}
```

- [ ] **Step 6: Write the PHP package manifests**

```json
// /php/client/composer.json
{
    "name": "ferro/client",
    "description": "Ferro native PHP client",
    "type": "library",
    "license": "Apache-2.0",
    "require": { "php": ">=8.2" },
    "require-dev": {
        "phpunit/phpunit": "^11.0",
        "phpstan/phpstan": "^2.0"
    },
    "suggest": {
        "ext-msgpack": "Faster MessagePack codec hot path",
        "ext-sockets": "Enables the MEMFD_RX out-of-band receive path (M3)"
    },
    "autoload": { "psr-4": { "Ferro\\": "src/" } },
    "autoload-dev": { "psr-4": { "Ferro\\Tests\\": "tests/" } },
    "scripts": {
        "test": "phpunit",
        "stan": "phpstan analyse src --level 9"
    },
    "config": { "sort-packages": true }
}
```
```xml
<!-- /php/client/phpunit.xml.dist -->
<?xml version="1.0" encoding="UTF-8"?>
<phpunit xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         bootstrap="vendor/autoload.php"
         colors="true"
         cacheDirectory=".phpunit.cache">
    <testsuites>
        <testsuite name="ferro-client">
            <directory>tests</directory>
        </testsuite>
    </testsuites>
</phpunit>
```
```neon
# /php/client/phpstan.neon.dist
parameters:
    level: 9
    paths:
        - src
```

- [ ] **Step 7: Verify both toolchains build the skeleton**

Run: `cargo build -p ferro-proto`
Expected: compiles (warnings-denied clean), empty consts included.
Run: `(cd php/client && composer install)`
Expected: installs PHPUnit 11 + PHPStan 2, autoloader generated.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml rust-toolchain.toml engine/crates/ferro-proto php/client/composer.json php/client/phpunit.xml.dist php/client/phpstan.neon.dist php/client/composer.lock
git commit -m "chore(s1): bootstrap workspace, ferro-proto crate, ferro/client package"
```

---

### Task 2: `/proto` registry + lock generation + Rust const generation

**Files:**
- Create: `/proto/methods.toml`, `/proto/errors.toml`, `/proto/types.toml`, `/proto/PROTOCOL.md`
- Create: `/engine/crates/ferro-proto/src/registry.rs`, `/engine/crates/ferro-proto/src/bin/gen_registry_lock.rs`
- Modify: `/engine/crates/ferro-proto/build.rs` (replace placeholder)
- Create: `/proto/registry.lock.json` (generated), `/engine/crates/ferro-proto/tests/registry_sync.rs`

**Interfaces:**
- Produces: `ferro_proto::consts` — `PROTOCOL_VERSION: u8`, `MAGIC: u8`, `MAX_FRAME_PAYLOAD: u32`, `DEFAULT_CREDIT_FRAMES: u32`, `DEFAULT_CREDIT_BYTES: u32`; modules `flags`, `service`, `method_core` (per-service methods are emitted as `method_<service>`), `tag`, `errc` (each `pub const NAME: u16` plus `NAME_BRANCH: u8`), `branch`, `feature_engine`, `feature_client`. Consumed by Tasks 3–7 and mirrored into PHP `Constants.php` in Task 8.
- Produces: `ferro_proto::registry::Registry` with `to_lock_json()`, used by the gen bin and the sync test. (The gen bin parses the three TOML files into a `Registry` via local deserialize structs — there is no `Registry::from_toml_dir` method.)

- [ ] **Step 1: Write the registry TOML (single source of truth)**

```toml
# /proto/methods.toml
protocol_version    = 1
magic               = 0xF7
max_frame_payload   = 16777216   # 16 MiB hard codec ceiling (W-5)
default_credit_frames = 64
default_credit_bytes  = 4194304  # 4 MiB per-request credit window (W-5)

[flags]
STREAM     = 0x01
END        = 0x02
CANCEL     = 0x04
OOB_FD     = 0x08
COMPRESSED = 0x10

[services]
CORE   = 1
SQL    = 2
TX     = 3
STREAM = 4
ADMIN  = 5

[methods.core]
HELLO         = 1
HELLO_ACK     = 2
PING          = 3
PONG          = 4
GOODBYE       = 5
WINDOW_UPDATE = 6

# Terminal outcome-envelope status discriminant (decision W-4). In /proto so both codecs
# generate it rather than hand-writing 0/1/2 (charter rule 2).
[outcome]
OK        = 0
ERROR     = 1
CANCELLED = 2

[features.engine]
MEMFD          = 0x01
LISTEN_STREAMS = 0x02
MANIFEST       = 0x04

[features.client]
MEMFD_RX = 0x01
FIBERS   = 0x02
```
```toml
# /proto/types.toml
# Canonical TypedValue tags (SPEC §9). [tag, value] 2-element msgpack array (W-1).

# M0 EXEC/PG path implements only these (T-1); others => NonRetryable{Unsupported}.
# MUST precede the [tags] table so it is a TOP-LEVEL key, not absorbed as tags.m0_scalar.
m0_scalar = ["NULL", "BOOL", "I64", "F64", "TEXT", "BYTES"]

[tags]
NULL = 0
BOOL = 1
I64 = 2
U64 = 3
F64 = 4
DECIMAL = 5
TEXT = 6
BYTES = 7
DATE = 8
TIME = 9
TIMESTAMP = 10
TIMESTAMPTZ = 11
UUID = 12
JSON = 13
ARRAY = 14
INTERVAL = 15
INET = 16
VECTOR = 17
```
```toml
# /proto/errors.toml
# Branch carried explicitly on the wire (W-3): unknown code still classified by branch.
[branches]
Retryable = 1
Indeterminate = 2
NonRetryable = 3

# Each code is its own [codes.<Name>] table (NOT an inline-table on the header line — that is
# invalid TOML). code = 0xBxxx where B is the branch nibble.
[codes.ConnectionLost]
code = 0x1001
branch = 1
[codes.PoolTimeout]
code = 0x1002
branch = 1
[codes.TxDeadline]
code = 0x1003
branch = 1
[codes.Deadlock]
code = 0x1004
branch = 1
[codes.SerializationFailure]
code = 0x1005
branch = 1
[codes.ReplicaUnavailable]
code = 0x1006
branch = 1
[codes.WriteUnconfirmed]
code = 0x2001
branch = 2
[codes.Syntax]
code = 0x3001
branch = 3
[codes.Unique]
code = 0x3002
branch = 3
[codes.ForeignKey]
code = 0x3003
branch = 3
[codes.NotNull]
code = 0x3004
branch = 3
[codes.Check]
code = 0x3005
branch = 3
[codes.Auth]
code = 0x3006
branch = 3
[codes.QueryTimeout]
code = 0x3007
branch = 3
[codes.Cancelled]
code = 0x3008
branch = 3
[codes.Protocol]
code = 0x3009
branch = 3
[codes.Unsupported]
code = 0x300A
branch = 3
```

> These three TOML files MUST parse before anything downstream works — verify with a quick `cargo run -p ferro-proto --bin gen-registry-lock` immediately after writing them (Step 4), before trusting the sync test.

- [ ] **Step 2: Write the shared registry model**

```rust
// /engine/crates/ferro-proto/src/registry.rs
//! TOML registry -> lock model. Used by the gen bin and the sync test only (not the hot path).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registry {
    pub protocol_version: u8,
    pub magic: u8,
    pub max_frame_payload: u32,
    pub default_credit_frames: u32,
    pub default_credit_bytes: u32,
    pub flags: BTreeMap<String, u16>,
    pub services: BTreeMap<String, u16>,
    pub methods: BTreeMap<String, BTreeMap<String, u16>>,
    pub features: BTreeMap<String, BTreeMap<String, u16>>,
    pub outcome: BTreeMap<String, u8>,
    pub tags: BTreeMap<String, u8>,
    pub branches: BTreeMap<String, u8>,
    pub codes: BTreeMap<String, ErrCode>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrCode {
    pub code: u16,
    pub branch: u8,
}

// Deserialize shapes for the three TOML files. serde ignores unknown keys, so `m0_scalar` in
// types.toml is simply not read here.
#[derive(Deserialize)]
struct MethodsToml {
    protocol_version: u8,
    magic: u8,
    max_frame_payload: u32,
    default_credit_frames: u32,
    default_credit_bytes: u32,
    flags: BTreeMap<String, u16>,
    services: BTreeMap<String, u16>,
    methods: BTreeMap<String, BTreeMap<String, u16>>,
    features: BTreeMap<String, BTreeMap<String, u16>>,
    outcome: BTreeMap<String, u8>,
}
#[derive(Deserialize)]
struct TypesToml {
    tags: BTreeMap<String, u8>,
}
#[derive(Deserialize)]
struct ErrorsToml {
    branches: BTreeMap<String, u8>,
    codes: BTreeMap<String, ErrCode>,
}

impl Registry {
    /// Parse the three `/proto/*.toml` files in-process into a `Registry`. Shared by the gen bin
    /// (which serializes the result) and the sync test (which compares it) so both parse identically.
    pub fn from_toml_dir(dir: &Path) -> Registry {
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
        let m: MethodsToml = toml::from_str(&read("methods.toml")).unwrap();
        let t: TypesToml = toml::from_str(&read("types.toml")).unwrap();
        let e: ErrorsToml = toml::from_str(&read("errors.toml")).unwrap();
        Registry {
            protocol_version: m.protocol_version,
            magic: m.magic,
            max_frame_payload: m.max_frame_payload,
            default_credit_frames: m.default_credit_frames,
            default_credit_bytes: m.default_credit_bytes,
            flags: m.flags,
            services: m.services,
            methods: m.methods,
            features: m.features,
            outcome: m.outcome,
            tags: t.tags,
            branches: e.branches,
            codes: e.codes,
        }
    }

    /// Produce the canonical lock JSON (stable key order via BTreeMap, 2-space indent).
    pub fn to_lock_json(&self) -> String {
        let mut s = serde_json::to_string_pretty(self).expect("serialize registry");
        s.push('\n');
        s
    }
}
```

- [ ] **Step 3: Write the `gen-registry-lock` binary**

```rust
// /engine/crates/ferro-proto/src/bin/gen_registry_lock.rs
//! Parse /proto/*.toml -> /proto/registry.lock.json. Run after editing any TOML.
use ferro_proto::registry::Registry;
use std::path::PathBuf;

fn proto_dir() -> PathBuf {
    // bin runs from the crate dir under `cargo run`; repo root is three up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto")
}

fn main() {
    let dir = proto_dir();
    let reg = Registry::from_toml_dir(&dir);
    std::fs::write(dir.join("registry.lock.json"), reg.to_lock_json()).unwrap();
    eprintln!("wrote {}", dir.join("registry.lock.json").display());
}
```

`toml` and `serde_json` are already in `[dependencies]` (placed there in Task 1) because binaries and the library can only see `[dependencies]`, not `[dev-dependencies]`. No manifest change is needed here — just confirm they are present before building the bin.

- [ ] **Step 4: Generate the lock file**

Run: `cargo run -p ferro-proto --bin gen-registry-lock`
Expected: writes `/proto/registry.lock.json` with sorted keys.

- [ ] **Step 5: Replace `build.rs` to emit consts from the lock file**

```rust
// /engine/crates/ferro-proto/build.rs
use serde::Deserialize;
use std::collections::BTreeMap;
use std::{env, fmt::Write as _, fs, path::PathBuf};

#[derive(Deserialize)]
struct ErrCode { code: u16, branch: u8 }
#[derive(Deserialize)]
struct Registry {
    protocol_version: u8, magic: u8, max_frame_payload: u32,
    default_credit_frames: u32, default_credit_bytes: u32,
    flags: BTreeMap<String, u16>, services: BTreeMap<String, u16>,
    methods: BTreeMap<String, BTreeMap<String, u16>>,
    features: BTreeMap<String, BTreeMap<String, u16>>,
    outcome: BTreeMap<String, u8>,
    tags: BTreeMap<String, u8>, branches: BTreeMap<String, u8>,
    codes: BTreeMap<String, ErrCode>,
}

fn lock_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/registry.lock.json")
}

fn main() {
    let lock = lock_path();
    println!("cargo:rerun-if-changed={}", lock.display());
    let reg: Registry = serde_json::from_str(&fs::read_to_string(&lock).unwrap()).unwrap();

    let mut o = String::new();
    writeln!(o, "// @generated from /proto/registry.lock.json — do not edit.").unwrap();
    writeln!(o, "pub const PROTOCOL_VERSION: u8 = {};", reg.protocol_version).unwrap();
    writeln!(o, "pub const MAGIC: u8 = 0x{:02X};", reg.magic).unwrap();
    writeln!(o, "pub const MAX_FRAME_PAYLOAD: u32 = {};", reg.max_frame_payload).unwrap();
    writeln!(o, "pub const DEFAULT_CREDIT_FRAMES: u32 = {};", reg.default_credit_frames).unwrap();
    writeln!(o, "pub const DEFAULT_CREDIT_BYTES: u32 = {};", reg.default_credit_bytes).unwrap();

    emit_mod_u16(&mut o, "flags", &reg.flags);
    emit_mod_u16(&mut o, "service", &reg.services);
    for (svc, m) in &reg.methods {
        emit_mod_u16(&mut o, &format!("method_{svc}"), m);
    }
    for (side, f) in &reg.features {
        emit_mod_u16(&mut o, &format!("feature_{side}"), f);
    }
    emit_mod_u8(&mut o, "outcome", &reg.outcome);
    emit_mod_u8(&mut o, "tag", &reg.tags);
    emit_mod_u8(&mut o, "branch", &reg.branches);

    writeln!(o, "pub mod errc {{").unwrap();
    for (name, ec) in &reg.codes {
        writeln!(o, "    pub const {}: u16 = 0x{:04X};", screaming(name), ec.code).unwrap();
        writeln!(o, "    pub const {}_BRANCH: u8 = {};", screaming(name), ec.branch).unwrap();
    }
    writeln!(o, "}}").unwrap();

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("consts.rs");
    fs::write(out, o).unwrap();
}

fn emit_mod_u16(o: &mut String, name: &str, m: &BTreeMap<String, u16>) {
    writeln!(o, "pub mod {name} {{").unwrap();
    for (k, v) in m { writeln!(o, "    pub const {}: u16 = {};", screaming(k), v).unwrap(); }
    writeln!(o, "}}").unwrap();
}
fn emit_mod_u8(o: &mut String, name: &str, m: &BTreeMap<String, u8>) {
    writeln!(o, "pub mod {name} {{").unwrap();
    for (k, v) in m { writeln!(o, "    pub const {}: u8 = {};", screaming(k), v).unwrap(); }
    writeln!(o, "}}").unwrap();
}
fn screaming(s: &str) -> String {
    // ConnectionLost -> CONNECTION_LOST ; HELLO -> HELLO ; MEMFD_RX -> MEMFD_RX
    let mut out = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if c.is_uppercase() && prev_lower { out.push('_'); }
        out.push(c.to_ascii_uppercase());
        prev_lower = c.is_lowercase() || c.is_ascii_digit();
    }
    out
}
```

Add `serde` + `serde_json` are already `[build-dependencies]`; keep them.

- [ ] **Step 6: Write the registry-sync test (drift guard)**

```rust
// /engine/crates/ferro-proto/tests/registry_sync.rs
//! Fails if /proto/*.toml was edited without regenerating registry.lock.json.
//! PURE and side-effect-free: parses the TOML in-process via `Registry::from_toml_dir` and compares
//! to the committed lock file. Does NOT run the gen binary and does NOT write to disk.
use ferro_proto::registry::Registry;
use std::path::PathBuf;

#[test]
fn lock_matches_toml() {
    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let committed = std::fs::read_to_string(proto.join("registry.lock.json")).unwrap();
    let regenerated = Registry::from_toml_dir(&proto).to_lock_json();
    assert_eq!(
        committed, regenerated,
        "registry.lock.json is stale — run `cargo run -p ferro-proto --bin gen-registry-lock` and commit"
    );
}
```

- [ ] **Step 7: Run the test to verify it passes on synced state, fails on drift**

Run: `cargo test -p ferro-proto --test registry_sync`
Expected: PASS.
Manually verify drift detection: change `TxDeadline` code in `errors.toml` to `0x1099`, run the test → FAIL with the "stale" message; revert.

- [ ] **Step 8: Write `/proto/PROTOCOL.md` (field-order contract)**

Document, in prose: the 16-byte header layout; the canonical MessagePack profile (from Global Constraints); the positional field order of every message (filled as Tasks 4–5 land — for now the header + Value + the six core messages + ERROR + Outcome envelope); and the vector index. This file is the human-readable arbiter that accompanies the vectors.

- [ ] **Step 9: Commit**

```bash
git add proto engine/crates/ferro-proto/build.rs engine/crates/ferro-proto/src/registry.rs engine/crates/ferro-proto/src/bin/gen_registry_lock.rs engine/crates/ferro-proto/tests/registry_sync.rs engine/crates/ferro-proto/Cargo.toml
git commit -m "feat(s1): proto registry + lock generation + rust const codegen with drift guard"
```

---

### Task 3: Frame header codec + flags

**Files:**
- Create/replace: `/engine/crates/ferro-proto/src/flags.rs`, `/engine/crates/ferro-proto/src/header.rs`
- Create: `/engine/crates/ferro-proto/tests/header.rs`

**Interfaces:**
- Produces: `Header { flags: u16, service: u16, method: u16, request_id: u32, payload_len: u32 }` with `fn encode(&self) -> [u8; 16]` and `fn decode(buf: &[u8]) -> Result<Header, CodecError>`. `magic`/`version` are validated on decode and written on encode from `consts`, never stored. Consumed by Task 6 (vectors), Task 7 (fuzz), the daemon's `Framed` adapter in S3, and mirrored by PHP `Header` in Task 8. **S1 has no separate `Frame` type** — a frame is `Header::encode()` concatenated with the payload bytes (Rust tests build it inline; PHP has `Ferro\Protocol\Codec::encodeFrame/decodeFrame`).
- Produces: `flags::has(bits, mask) -> bool`, `flags::validate(bits) -> Result<(), CodecError>` (rejects unknown + reserved bits).

- [ ] **Step 1: Write the failing header tests**

```rust
// /engine/crates/ferro-proto/tests/header.rs
use ferro_proto::consts::{self, flags};
use ferro_proto::header::Header;
use ferro_proto::CodecError;

#[test]
fn encode_is_16_bytes_little_endian() {
    let h = Header { flags: flags::END, service: consts::service::CORE, method: consts::method_core::PING,
                     request_id: 0x0A0B0C0D, payload_len: 1 };
    let b = h.encode();
    assert_eq!(b.len(), 16);
    assert_eq!(b[0], consts::MAGIC);          // 0xF7
    assert_eq!(b[1], consts::PROTOCOL_VERSION); // 1
    assert_eq!(u16::from_le_bytes([b[2], b[3]]), flags::END);
    assert_eq!(u16::from_le_bytes([b[4], b[5]]), consts::service::CORE);
    assert_eq!(u16::from_le_bytes([b[6], b[7]]), consts::method_core::PING);
    assert_eq!(u32::from_le_bytes([b[8], b[9], b[10], b[11]]), 0x0A0B0C0D);
    assert_eq!(u32::from_le_bytes([b[12], b[13], b[14], b[15]]), 1);
}

#[test]
fn roundtrip() {
    let h = Header { flags: flags::STREAM | flags::END, service: 2, method: 1, request_id: 42, payload_len: 7 };
    assert_eq!(Header::decode(&h.encode()).unwrap(), h);
}

#[test]
fn rejects_bad_magic() {
    let mut b = Header { flags: 0, service: 1, method: 3, request_id: 1, payload_len: 0 }.encode();
    b[0] = 0x00;
    assert_eq!(Header::decode(&b), Err(CodecError::BadMagic { expected: consts::MAGIC, got: 0x00 }));
}

#[test]
fn rejects_bad_version() {
    let mut b = Header { flags: 0, service: 1, method: 3, request_id: 1, payload_len: 0 }.encode();
    b[1] = 99;
    assert_eq!(Header::decode(&b), Err(CodecError::BadVersion { expected: consts::PROTOCOL_VERSION, got: 99 }));
}

#[test]
fn rejects_oversize_payload_len_without_reading_payload() {
    let mut b = Header { flags: 0, service: 2, method: 1, request_id: 1, payload_len: 0 }.encode();
    let too_big = consts::MAX_FRAME_PAYLOAD + 1;
    b[12..16].copy_from_slice(&too_big.to_le_bytes());
    assert_eq!(Header::decode(&b), Err(CodecError::FrameTooLarge { len: too_big, max: consts::MAX_FRAME_PAYLOAD }));
}

#[test]
fn rejects_short_buffer() {
    assert_eq!(Header::decode(&[0u8; 15]), Err(CodecError::Truncated { need: 16, have: 15 }));
}

#[test]
fn rejects_reserved_and_unknown_flags() {
    use ferro_proto::flags as F;
    assert_eq!(F::validate(flags::OOB_FD), Err(CodecError::UnsupportedFlag));
    assert_eq!(F::validate(flags::COMPRESSED), Err(CodecError::UnsupportedFlag));
    assert_eq!(F::validate(0x8000), Err(CodecError::UnknownFlags { bits: 0x8000 }));
    assert!(F::validate(flags::STREAM | flags::END | flags::CANCEL).is_ok());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-proto --test header`
Expected: FAIL (module `header`/`flags` unimplemented).

- [ ] **Step 3: Implement flags**

```rust
// /engine/crates/ferro-proto/src/flags.rs
use crate::consts::flags::{CANCEL, COMPRESSED, END, OOB_FD, STREAM};
use crate::CodecError;

/// All bits defined in M0 (known set). OOB_FD/COMPRESSED are known-but-reserved.
pub const KNOWN: u16 = STREAM | END | CANCEL | OOB_FD | COMPRESSED;
/// Reserved bits that are illegal to *set* in an M0 frame.
pub const RESERVED: u16 = OOB_FD | COMPRESSED;

#[inline]
pub fn has(bits: u16, mask: u16) -> bool { bits & mask != 0 }

/// Unknown bits -> Protocol; reserved-but-known bits actually set -> Unsupported.
pub fn validate(bits: u16) -> Result<(), CodecError> {
    if bits & !KNOWN != 0 { return Err(CodecError::UnknownFlags { bits: bits & !KNOWN }); }
    if bits & RESERVED != 0 { return Err(CodecError::UnsupportedFlag); }
    Ok(())
}
```

- [ ] **Step 4: Implement the header**

```rust
// /engine/crates/ferro-proto/src/header.rs
use crate::consts::{MAGIC, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION};
use crate::CodecError;

pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub flags: u16,
    pub service: u16,
    pub method: u16,
    pub request_id: u32,
    pub payload_len: u32,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0] = MAGIC;
        b[1] = PROTOCOL_VERSION;
        b[2..4].copy_from_slice(&self.flags.to_le_bytes());
        b[4..6].copy_from_slice(&self.service.to_le_bytes());
        b[6..8].copy_from_slice(&self.method.to_le_bytes());
        b[8..12].copy_from_slice(&self.request_id.to_le_bytes());
        b[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        b
    }

    /// Decode + validate the header ONLY. Rejects an oversize `payload_len` before any payload
    /// is read (the zero-allocation DoS guard). Does not validate flags (caller decides when).
    pub fn decode(buf: &[u8]) -> Result<Header, CodecError> {
        if buf.len() < HEADER_LEN {
            return Err(CodecError::Truncated { need: HEADER_LEN, have: buf.len() });
        }
        if buf[0] != MAGIC { return Err(CodecError::BadMagic { expected: MAGIC, got: buf[0] }); }
        if buf[1] != PROTOCOL_VERSION {
            return Err(CodecError::BadVersion { expected: PROTOCOL_VERSION, got: buf[1] });
        }
        let payload_len = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        if payload_len > MAX_FRAME_PAYLOAD {
            return Err(CodecError::FrameTooLarge { len: payload_len, max: MAX_FRAME_PAYLOAD });
        }
        Ok(Header {
            flags: u16::from_le_bytes([buf[2], buf[3]]),
            service: u16::from_le_bytes([buf[4], buf[5]]),
            method: u16::from_le_bytes([buf[6], buf[7]]),
            request_id: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            payload_len,
        })
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p ferro-proto --test header`
Expected: PASS (all 7 tests).

- [ ] **Step 6: Commit**

```bash
git add engine/crates/ferro-proto/src/flags.rs engine/crates/ferro-proto/src/header.rs engine/crates/ferro-proto/tests/header.rs
git commit -m "feat(s1): 16-byte frame header codec + flag validation"
```

---

### Task 4: TypedValue scalar codec

**Files:**
- Create/replace: `/engine/crates/ferro-proto/src/value.rs`
- Create: `/engine/crates/ferro-proto/tests/value.rs`

**Interfaces:**
- Produces: `enum Value { Null, Bool(bool), I64(i64), F64(f64), Text(String), Bytes(Vec<u8>) }` with `fn encode(&self, out: &mut Vec<u8>)` and `fn decode(rd: &mut &[u8]) -> Result<Value, CodecError>`. Each value is the 2-element msgpack array `[tag, payload]` (decision W-1). Consumed by Task 5 (ERROR/Outcome bodies carry no Value in S1, but S5 EXEC rows will) and mirrored by PHP `Value` in Task 8.

- [ ] **Step 1: Write the failing value tests**

```rust
// /engine/crates/ferro-proto/tests/value.rs
use ferro_proto::value::Value;

fn enc(v: &Value) -> Vec<u8> { let mut o = Vec::new(); v.encode(&mut o); o }
fn dec(b: &[u8]) -> Value { let mut r = b; Value::decode(&mut r).unwrap() }

#[test]
fn null_is_tag0_nil() {
    // fixarray(2) = 0x92 ; tag 0 = positive fixint 0x00 ; nil = 0xc0
    assert_eq!(enc(&Value::Null), vec![0x92, 0x00, 0xc0]);
}

#[test]
fn bool_true() {
    // 0x92, tag 1 (BOOL), 0xc3 (true)
    assert_eq!(enc(&Value::Bool(true)), vec![0x92, 0x01, 0xc3]);
}

#[test]
fn i64_small_positive_is_fixint() {
    // 0x92, tag 2 (I64), positive fixint 1 (0x01)
    assert_eq!(enc(&Value::I64(1)), vec![0x92, 0x02, 0x01]);
}

#[test]
fn i64_200_is_uint8() {
    // Canonical rule = rmp write_sint: non-negative 200 fits uint8 -> 0xcc 0xc8 (NOT int16).
    // This is the load-bearing cross-language byte: PHP PurePacker::packInt MUST match it.
    assert_eq!(enc(&Value::I64(200)), vec![0x92, 0x02, 0xcc, 0xc8]);
}

#[test]
fn i64_negative_uses_signed_marker() {
    // -200 does not fit i8; narrows to int16 0xd1. Negatives keep the signed ladder.
    assert_eq!(enc(&Value::I64(-200)), vec![0x92, 0x02, 0xd1, 0xff, 0x38]);
}

#[test]
fn f64_is_always_float64() {
    let b = enc(&Value::F64(1.5));
    assert_eq!(b[0], 0x92);
    assert_eq!(b[1], 0x04);       // tag F64
    assert_eq!(b[2], 0xcb);       // float64 marker
}

#[test]
fn text_uses_str_family() {
    // "hi" -> fixstr len2 = 0xa2 'h' 'i'
    assert_eq!(enc(&Value::Text("hi".into())), vec![0x92, 0x06, 0xa2, b'h', b'i']);
}

#[test]
fn bytes_uses_bin_family() {
    // 3 bytes -> bin8 0xc4 0x03 <data>
    assert_eq!(enc(&Value::Bytes(vec![1, 2, 3])), vec![0x92, 0x07, 0xc4, 0x03, 1, 2, 3]);
}

#[test]
fn roundtrip_all_scalars() {
    for v in [Value::Null, Value::Bool(false), Value::I64(-40000), Value::F64(-0.0),
              Value::Text(String::new()), Value::Bytes(vec![])] {
        assert_eq!(dec(&enc(&v)), v);
    }
}

#[test]
fn lying_length_prefix_is_rejected_before_allocating() {
    // str32 (0xdb) claiming ~4 GiB with no body must error via the bound check, NOT pre-allocate.
    let s = [0x92u8, 0x06, 0xdb, 0xff, 0xff, 0xff, 0xff];
    let mut r = &s[..];
    assert!(Value::decode(&mut r).is_err());
    // bin32 (0xc6) claiming ~4 GiB with no body: same.
    let b = [0x92u8, 0x07, 0xc6, 0xff, 0xff, 0xff, 0xff];
    let mut r = &b[..];
    assert!(Value::decode(&mut r).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-proto --test value`
Expected: FAIL (module `value` empty).

- [ ] **Step 3: Implement the value codec with `rmp` low-level**

```rust
// /engine/crates/ferro-proto/src/value.rs
use crate::consts::tag;
use crate::CodecError;
use rmp::decode::{self as dec, RmpRead};
use rmp::encode as enc;
use rmp::Marker;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn tag(&self) -> u8 {
        match self {
            Value::Null => tag::NULL,
            Value::Bool(_) => tag::BOOL,
            Value::I64(_) => tag::I64,
            Value::F64(_) => tag::F64,
            Value::Text(_) => tag::TEXT,
            Value::Bytes(_) => tag::BYTES,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        // [tag, payload] — fixarray(2)
        enc::write_array_len(out, 2).unwrap();
        enc::write_pfix(out, self.tag()).unwrap(); // tags 0..=17 fit positive fixint
        match self {
            Value::Null => enc::write_nil(out).unwrap(),
            Value::Bool(b) => enc::write_bool(out, *b).unwrap(),
            Value::I64(n) => { enc::write_sint(out, *n).unwrap(); }
            Value::F64(f) => enc::write_f64(out, *f).unwrap(),
            Value::Text(s) => enc::write_str(out, s).unwrap(),
            Value::Bytes(b) => enc::write_bin(out, b).unwrap(),
        }
    }

    pub fn decode(rd: &mut &[u8]) -> Result<Value, CodecError> {
        let len = dec::read_array_len(rd).map_err(|e| CodecError::Malformed(format!("array: {e:?}")))?;
        if len != 2 { return Err(CodecError::Malformed(format!("TypedValue array len {len} != 2"))); }
        let value_tag: u8 = dec::read_pfix(rd).map_err(|e| CodecError::Malformed(format!("tag: {e:?}")))?;
        match value_tag {
            t if t == tag::NULL => { read_nil(rd)?; Ok(Value::Null) }
            t if t == tag::BOOL => Ok(Value::Bool(read_bool(rd)?)),
            t if t == tag::I64 => Ok(Value::I64(
                dec::read_int(rd).map_err(|e| CodecError::Malformed(format!("i64: {e:?}")))?)),
            t if t == tag::F64 => Ok(Value::F64(
                dec::read_f64(rd).map_err(|e| CodecError::Malformed(format!("f64: {e:?}")))?)),
            t if t == tag::TEXT => Ok(Value::Text(read_str(rd)?)),
            t if t == tag::BYTES => Ok(Value::Bytes(read_bin(rd)?)),
            other => Err(CodecError::Malformed(format!("unsupported TypedValue tag {other} in M0"))),
        }
    }
}

fn read_nil(rd: &mut &[u8]) -> Result<(), CodecError> {
    match dec::read_marker(rd).map_err(|e| CodecError::Malformed(format!("nil: {e:?}")))? {
        Marker::Null => Ok(()),
        m => Err(CodecError::Malformed(format!("expected nil, got {m:?}"))),
    }
}
fn read_bool(rd: &mut &[u8]) -> Result<bool, CodecError> {
    match dec::read_marker(rd).map_err(|e| CodecError::Malformed(format!("bool: {e:?}")))? {
        Marker::True => Ok(true),
        Marker::False => Ok(false),
        m => Err(CodecError::Malformed(format!("expected bool, got {m:?}"))),
    }
}
fn read_str(rd: &mut &[u8]) -> Result<String, CodecError> {
    let len = dec::read_str_len(rd).map_err(|e| CodecError::Malformed(format!("str len: {e:?}")))? as usize;
    bound_len(len, rd.len())?;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf).map_err(|e| CodecError::Malformed(format!("str body: {e:?}")))?;
    String::from_utf8(buf).map_err(|_| CodecError::Malformed("invalid utf8".into()))
}
fn read_bin(rd: &mut &[u8]) -> Result<Vec<u8>, CodecError> {
    let len = dec::read_bin_len(rd).map_err(|e| CodecError::Malformed(format!("bin len: {e:?}")))? as usize;
    bound_len(len, rd.len())?;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf).map_err(|e| CodecError::Malformed(format!("bin body: {e:?}")))?;
    Ok(buf)
}

/// Reject a length prefix that exceeds the bytes actually remaining BEFORE allocating, so a lying
/// str/bin length (up to u32::MAX) cannot force a huge pre-allocation. The frame payload is already
/// capped at MAX_FRAME_PAYLOAD, so `remaining` is bounded; this bounds the allocation to it.
fn bound_len(len: usize, remaining: usize) -> Result<(), CodecError> {
    if len > remaining {
        return Err(CodecError::Truncated { need: len, have: remaining });
    }
    Ok(())
}
```

> Implementation note: exact `rmp` function names (`write_pfix`, `read_pfix`, `read_int`, `read_str_len`, `RmpRead::read_exact_buf`) must be confirmed against `rmp 0.8` during execution; if a name differs, the *tests* (which assert exact bytes) are the contract — adjust the calls until the byte assertions pass. Do not change the asserted bytes.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-proto --test value`
Expected: PASS (all tests). The `i64(200)` = `cc c8` (uint8) and `i64(-200)` = `d1 ff 38` (int16) bytes are confirmed against rmp 0.8.15 and are the canonical truth PHP mirrors in Task 8. Do NOT change these assertions to make a mismatched PHP codec pass — fix the PHP side instead.

- [ ] **Step 5: Commit**

```bash
git add engine/crates/ferro-proto/src/value.rs engine/crates/ferro-proto/tests/value.rs
git commit -m "feat(s1): TypedValue scalar codec ([tag,value], canonical msgpack)"
```

---

### Task 5: Core message structs + ERROR + Outcome envelope

**Files:**
- Create/replace: `/engine/crates/ferro-proto/src/messages.rs`
- Create: `/engine/crates/ferro-proto/tests/messages.rs`

**Interfaces:**
- Produces: structs `Hello`, `HelloAck`, `Ping`, `Pong`, `Goodbye`, `WindowUpdate`, `ErrorPayload`, and enum `Outcome`. Each has `fn encode(&self) -> Vec<u8>` (the frame payload) and `fn decode(buf: &[u8]) -> Result<Self, CodecError>`. Positional arrays via `rmp-serde` compact. Consumed by Task 6 (vectors), Task 7 (fuzz), Task 8 (PHP mirror), and by S3 (`ferrod` session layer).

- [ ] **Step 1: Write the failing message tests**

```rust
// /engine/crates/ferro-proto/tests/messages.rs
use ferro_proto::messages::*;

#[test]
fn hello_roundtrip() {
    let h = Hello { client_version: 1, type_registry_hash: "abc".into(), manifest_hash: None,
                    pid: 4242, features: 0 };
    assert_eq!(Hello::decode(&h.encode()).unwrap(), h);
}

#[test]
fn hello_ack_roundtrip_with_large_epoch() {
    // boot_epoch above i64::MAX exercises unsigned-smallest (uint64) — must round-trip.
    let a = HelloAck { engine_version: 1, boot_epoch: u64::MAX - 3, features: 0,
                       pools: vec![], type_registry_hash: "abc".into() };
    assert_eq!(HelloAck::decode(&a.encode()).unwrap(), a);
}

#[test]
fn ping_pong_goodbye_roundtrip() {
    assert_eq!(Ping::decode(&Ping { token: 7 }.encode()).unwrap(), Ping { token: 7 });
    assert_eq!(Pong::decode(&Pong { token: 7 }.encode()).unwrap(), Pong { token: 7 });
    assert_eq!(Goodbye::decode(&Goodbye {}.encode()).unwrap(), Goodbye {});
}

#[test]
fn window_update_roundtrip() {
    let w = WindowUpdate { frames: 64, bytes: 4_194_304 };
    assert_eq!(WindowUpdate::decode(&w.encode()).unwrap(), w);
}

#[test]
fn error_payload_roundtrip() {
    use ferro_proto::consts::errc;
    let e = ErrorPayload { code: errc::PROTOCOL, branch: errc::PROTOCOL_BRANCH,
                           sqlstate: None, errno: None, message: "reused_request_id".into(),
                           detail: None, retry_after_ms: None };
    assert_eq!(ErrorPayload::decode(&e.encode()).unwrap(), e);
}

#[test]
fn outcome_ok_and_error() {
    let ok = Outcome::Ok(vec![0x01]); // opaque body bytes
    assert_eq!(Outcome::decode(&ok.encode()).unwrap(), ok);
    let err = Outcome::Error(ErrorPayload {
        code: 0x3001, branch: 3, sqlstate: Some("42601".into()), errno: None,
        message: "syntax error".into(), detail: None, retry_after_ms: None });
    assert_eq!(Outcome::decode(&err.encode()).unwrap(), err);
    let c = Outcome::Cancelled;
    assert_eq!(Outcome::decode(&c.encode()).unwrap(), c);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-proto --test messages`
Expected: FAIL (module empty).

- [ ] **Step 3: Implement the messages via `rmp-serde` (compact = positional arrays)**

```rust
// /engine/crates/ferro-proto/src/messages.rs
use crate::CodecError;
use serde::{Deserialize, Serialize};

/// rmp-serde in default (compact) mode encodes a struct as a fixarray of its fields in
/// declaration order — exactly the positional layout PROTOCOL.md pins.
fn to_vec<T: Serialize>(v: &T) -> Vec<u8> {
    rmp_serde::to_vec(v).expect("infallible in-memory encode")
}
fn from_slice<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T, CodecError> {
    rmp_serde::from_slice(b).map_err(|e| CodecError::Malformed(e.to_string()))
}

macro_rules! msg {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name { $(pub $field: $ty),* }
        impl $name {
            pub fn encode(&self) -> Vec<u8> { to_vec(self) }
            pub fn decode(b: &[u8]) -> Result<Self, CodecError> { from_slice(b) }
        }
    };
}

msg!(Hello { client_version: u32, type_registry_hash: String, manifest_hash: Option<String>, pid: u32, features: u32 });
msg!(HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<String>, type_registry_hash: String });
msg!(Ping { token: u64 });
msg!(Pong { token: u64 });
msg!(Goodbye {});
msg!(WindowUpdate { frames: u32, bytes: u32 });
msg!(ErrorPayload {
    code: u16, branch: u8, sqlstate: Option<String>, errno: Option<i32>,
    message: String, detail: Option<String>, retry_after_ms: Option<u32>
});

/// Terminal outcome envelope `[status, body]` (decision W-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok(Vec<u8>),        // opaque method-specific body bytes
    Error(ErrorPayload),
    Cancelled,
}

impl Outcome {
    pub fn encode(&self) -> Vec<u8> {
        use crate::consts::outcome;
        use rmp::encode as e;
        let mut o = Vec::new();
        e::write_array_len(&mut o, 2).unwrap();
        match self {
            Outcome::Ok(body) => {
                e::write_pfix(&mut o, outcome::OK).unwrap();
                // body is raw msgpack already; splice it in
                o.extend_from_slice(body);
            }
            Outcome::Error(ep) => {
                e::write_pfix(&mut o, outcome::ERROR).unwrap();
                o.extend_from_slice(&ep.encode());
            }
            Outcome::Cancelled => {
                e::write_pfix(&mut o, outcome::CANCELLED).unwrap();
                e::write_nil(&mut o).unwrap();
            }
        }
        o
    }
    pub fn decode(b: &[u8]) -> Result<Outcome, CodecError> {
        use crate::consts::outcome;
        use rmp::decode as d;
        let mut rd: &[u8] = b;
        let len = d::read_array_len(&mut rd).map_err(|e| CodecError::Malformed(format!("outcome: {e:?}")))?;
        if len != 2 {
            return Err(CodecError::Malformed(format!("outcome len {len} != 2")));
        }
        let status: u8 = d::read_pfix(&mut rd).map_err(|e| CodecError::Malformed(format!("status: {e:?}")))?;
        match status {
            s if s == outcome::OK => Ok(Outcome::Ok(rd.to_vec())),
            s if s == outcome::ERROR => Ok(Outcome::Error(ErrorPayload::decode(rd)?)),
            s if s == outcome::CANCELLED => {
                // Validate the body slot is `nil` rather than silently discarding trailing bytes.
                match d::read_marker(&mut rd)
                    .map_err(|e| CodecError::Malformed(format!("cancelled body: {e:?}")))?
                {
                    rmp::Marker::Null => Ok(Outcome::Cancelled),
                    m => Err(CodecError::Malformed(format!("cancelled body expected nil, got {m:?}"))),
                }
            }
            s => Err(CodecError::Malformed(format!("unknown outcome status {s}"))),
        }
    }
}
```

> Implementation note: confirm `rmp_serde::to_vec` uses compact (array) struct encoding by default in `rmp-serde 1.x`. If it defaults to maps, switch to `rmp_serde::encode::to_vec` with a `Serializer` configured via `.with_struct_map()`'s inverse (compact is the default; `to_vec` = compact). The `messages` roundtrip tests plus the Task-6 golden vectors are the arbiter.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-proto --test messages`
Expected: PASS (all 6 tests).

- [ ] **Step 5: Update `PROTOCOL.md` with the exact field order of every message**

List each struct's fields in declaration order with type + msgpack family, and the Outcome/ERROR layout. This is what the PHP mirror in Task 8 codes against.

- [ ] **Step 6: Commit**

```bash
git add engine/crates/ferro-proto/src/messages.rs engine/crates/ferro-proto/tests/messages.rs proto/PROTOCOL.md
git commit -m "feat(s1): core message structs + ERROR + Outcome envelope"
```

---

### Task 6: Golden vectors — format, generator, and Rust conformance test

**Files:**
- Create: `/engine/crates/ferro-proto/src/bin/gen_vectors.rs`
- Create (generated): `/proto/vectors/*.json`, `/proto/vectors/negative/*.bin`, `/proto/vectors/README.md`
- Create: `/engine/crates/ferro-proto/tests/golden_vectors.rs`

**Interfaces:**
- Produces: the vector JSON schema `{ "name", "header": {flags,service,method,request_id}, "message": <logical>, "frame_hex": "<full frame incl. header>" }` — the single artifact both languages assert against. Consumed by Task 9 (PHP conformance).

- [ ] **Step 1: Write the vector generator**

```rust
// /engine/crates/ferro-proto/src/bin/gen_vectors.rs
//! Emit deterministic golden vectors: for each case, build the full frame (header+payload),
//! and write {name, header, message(json), frame_hex}. Also emit malformed negative .bin seeds.
use ferro_proto::consts::{self, flags, method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::*;
use std::path::PathBuf;

fn dir() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors") }

fn frame(flags_: u16, svc: u16, method: u16, req: u32, payload: Vec<u8>) -> Vec<u8> {
    let h = Header { flags: flags_, service: svc, method, request_id: req, payload_len: payload.len() as u32 };
    let mut f = h.encode().to_vec();
    f.extend_from_slice(&payload);
    f
}
fn hex(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }

fn write_case(name: &str, flags_: u16, svc: u16, method: u16, req: u32, payload: Vec<u8>, msg_json: serde_json::Value) {
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

    let hello = Hello { client_version: 1, type_registry_hash: "deadbeef".into(),
                        manifest_hash: None, pid: 4242, features: 0 };
    write_case("hello", 0, service::CORE, method_core::HELLO, 1, hello.encode(),
        serde_json::json!({ "client_version":1, "type_registry_hash":"deadbeef",
                            "manifest_hash":null, "pid":4242, "features":0 }));

    let ack = HelloAck { engine_version: 1, boot_epoch: 0xFFFF_FFFF_FFFF_FFF0, features: 0,
                         pools: vec![], type_registry_hash: "deadbeef".into() };
    write_case("hello_ack", 0, service::CORE, method_core::HELLO_ACK, 1, ack.encode(),
        serde_json::json!({ "engine_version":1, "boot_epoch":"18446744073709551600",
                            "features":0, "pools":[], "type_registry_hash":"deadbeef" }));

    write_case("ping", 0, service::CORE, method_core::PING, 9, Ping { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }));
    write_case("pong", 0, service::CORE, method_core::PONG, 9, Pong { token: 7 }.encode(),
        serde_json::json!({ "token": 7 }));
    write_case("goodbye", 0, service::CORE, method_core::GOODBYE, 0, Goodbye {}.encode(),
        serde_json::json!({}));
    write_case("window_update", 0, service::CORE, method_core::WINDOW_UPDATE, 5,
        WindowUpdate { frames: 64, bytes: 4_194_304 }.encode(),
        serde_json::json!({ "frames":64, "bytes":4194304 }));

    let err = ErrorPayload { code: consts::errc::PROTOCOL, branch: consts::errc::PROTOCOL_BRANCH,
        sqlstate: None, errno: None, message: "reused_request_id".into(), detail: None, retry_after_ms: None };
    let outcome = Outcome::Error(err);
    write_case("error_protocol", flags::END, service::CORE, 0, 0, outcome.encode(),
        serde_json::json!({ "status":1, "error": { "code":0x3009, "branch":3,
            "sqlstate":null, "errno":null, "message":"reused_request_id",
            "detail":null, "retry_after_ms":null } }));

    // Negative seeds (decoder must reject; also fuzz corpus).
    let mut bad_magic = frame(0, service::CORE, method_core::PING, 1, Ping { token: 1 }.encode());
    bad_magic[0] = 0x00;
    std::fs::write(dir().join("negative/bad_magic.bin"), &bad_magic).unwrap();

    let mut bad_ver = frame(0, service::CORE, method_core::PING, 1, Ping { token: 1 }.encode());
    bad_ver[1] = 0x99;
    std::fs::write(dir().join("negative/bad_version.bin"), &bad_ver).unwrap();

    // Oversize payload_len with no payload body present.
    let mut oversize = Header { flags: 0, service: service::SQL, method: 1, request_id: 1,
        payload_len: consts::MAX_FRAME_PAYLOAD + 1 }.encode().to_vec();
    // (intentionally no payload appended)
    oversize.truncate(16);
    std::fs::write(dir().join("negative/oversize_len.bin"), &oversize).unwrap();

    // Reserved flag set.
    let reserved = frame(flags::OOB_FD, service::CORE, method_core::PING, 1, Ping { token: 1 }.encode());
    std::fs::write(dir().join("negative/reserved_flag.bin"), &reserved).unwrap();

    eprintln!("vectors written to {}", dir().display());
}
```

- [ ] **Step 2: Generate the vectors**

Run: `cargo run -p ferro-proto --bin gen-vectors`
Expected: `/proto/vectors/*.json` (7 files) + `/proto/vectors/negative/*.bin` (4 files).

- [ ] **Step 3: Write the Rust conformance test**

```rust
// /engine/crates/ferro-proto/tests/golden_vectors.rs
use ferro_proto::header::Header;
use std::fs;
use std::path::PathBuf;

fn vectors_dir() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors") }
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

#[test]
fn positive_vectors_header_decodes_and_frame_len_is_consistent() {
    let mut count = 0;
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") { continue; }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).expect("header decodes");
        assert_eq!(h.payload_len as usize, frame.len() - 16, "vector {p:?} payload_len mismatch");
        assert_eq!(h.service as u64, v["header"]["service"].as_u64().unwrap());
        assert_eq!(h.method as u64, v["header"]["method"].as_u64().unwrap());
        count += 1;
    }
    assert!(count >= 7, "expected >=7 positive vectors, found {count}");
}

#[test]
fn message_payloads_are_canonical_and_byte_stable() {
    // For every positive vector, decode the payload into its typed message and re-encode it;
    // the bytes MUST be identical. Since gen-vectors produced each vector via `.encode()`, this
    // proves the on-disk bytes ARE the canonical encoder output (encode==bytes at the message
    // level), and that decode->encode is a fixpoint. This is the Rust half of the cross-language
    // byte lock; the PHP half asserts PurePacker re-encodes to these same bytes (Task 9).
    use ferro_proto::consts::{method_core as mc, service};
    use ferro_proto::messages::*;
    for entry in fs::read_dir(vectors_dir()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") { continue; }
        let v: serde_json::Value = serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap();
        let frame = unhex(v["frame_hex"].as_str().unwrap());
        let h = Header::decode(&frame).unwrap();
        let payload = &frame[16..];
        let reencoded: Vec<u8> = match (h.service, h.method) {
            (s, m) if s == service::CORE && m == mc::HELLO => Hello::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::HELLO_ACK => HelloAck::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::PING => Ping::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::PONG => Pong::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::GOODBYE => Goodbye::decode(payload).unwrap().encode(),
            (s, m) if s == service::CORE && m == mc::WINDOW_UPDATE => WindowUpdate::decode(payload).unwrap().encode(),
            // error_protocol vector: an Outcome::Error terminal payload (method 0, END flag).
            _ => Outcome::decode(payload).unwrap().encode(),
        };
        assert_eq!(reencoded, payload.to_vec(),
                   "payload for {:?} is not canonical / byte-stable", p.file_name().unwrap());
    }
}

#[test]
fn negative_vectors_are_rejected() {
    let neg = vectors_dir().join("negative");
    let mut seen = std::collections::HashSet::new();
    for entry in fs::read_dir(&neg).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) != Some("bin") { continue; }
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        let bytes = fs::read(&p).unwrap();
        if name == "reserved_flag.bin" {
            // This one has a VALID header (good magic/version/len) but sets the reserved OOB_FD
            // flag — it is rejected at the flags layer, not by Header::decode. Assert both facts.
            let h = Header::decode(&bytes).expect("reserved_flag.bin has a structurally valid header");
            assert_eq!(
                ferro_proto::flags::validate(h.flags),
                Err(ferro_proto::CodecError::UnsupportedFlag),
                "reserved_flag.bin flags must be rejected by flags::validate"
            );
        } else {
            assert!(Header::decode(&bytes).is_err(), "negative vector {name} was NOT rejected by header decode");
        }
        seen.insert(name);
    }
    // Completeness guard: every required negative must be present, so a deleted/renamed .bin cannot
    // make this test (especially the reserved_flag branch) pass vacuously.
    for required in ["bad_magic.bin", "bad_version.bin", "oversize_len.bin", "reserved_flag.bin"] {
        assert!(seen.contains(required), "missing required negative vector: {required}");
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-proto --test golden_vectors`
Expected: PASS.

- [ ] **Step 5: Write `/proto/vectors/README.md`** documenting the JSON schema, the `frame_hex` convention (full frame incl. 16-byte header), and that vectors are regenerated with `cargo run -p ferro-proto --bin gen-vectors` and must be committed.

- [ ] **Step 6: Commit**

```bash
git add engine/crates/ferro-proto/src/bin/gen_vectors.rs engine/crates/ferro-proto/tests/golden_vectors.rs proto/vectors
git commit -m "feat(s1): golden vector format + generator + rust conformance test"
```

---

### Task 7: cargo-fuzz decoder targets

**Files:**
- Create: `/engine/crates/ferro-proto/fuzz/Cargo.toml`, `/engine/crates/ferro-proto/fuzz/fuzz_targets/decode_frame.rs`, `/engine/crates/ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs`

**Interfaces:**
- Consumes: `Header::decode`, `Value::decode`, `messages::*::decode`. No new public API.

- [ ] **Step 1: Scaffold the fuzz crate**

Run: `cargo install cargo-fuzz` (if absent), then `cd engine/crates/ferro-proto && cargo fuzz init` (creates the `fuzz/` skeleton). Replace the generated target files with the two below.

- [ ] **Step 2: Write the decode_frame target**

```rust
// /engine/crates/ferro-proto/fuzz/fuzz_targets/decode_frame.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use ferro_proto::header::Header;

// Arbitrary bytes in: header decode MUST NOT panic and MUST NOT allocate on an oversize length.
fuzz_target!(|data: &[u8]| {
    if let Ok(h) = Header::decode(data) {
        // If the header decodes, payload_len is already bounded by MAX_FRAME_PAYLOAD.
        // Attempt to slice the claimed payload; never trust it beyond available bytes.
        let body = &data[16.min(data.len())..];
        let take = (h.payload_len as usize).min(body.len());
        let _ = &body[..take];
        // Try message decode on the core methods; must not panic.
        let _ = ferro_proto::messages::Ping::decode(&body[..take]);
        let _ = ferro_proto::messages::Outcome::decode(&body[..take]);
        let mut rd = &body[..take];
        let _ = ferro_proto::value::Value::decode(&mut rd);
    }
});
```

- [ ] **Step 3: Write the roundtrip_frame target**

```rust
// /engine/crates/ferro-proto/fuzz/fuzz_targets/roundtrip_frame.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;
use ferro_proto::value::Value;

#[derive(Arbitrary, Debug)]
enum FuzzValue { Null, Bool(bool), I64(i64), F64(f64), Text(String), Bytes(Vec<u8>) }

// Any valid Value encodes, decodes to an equal Value, and re-encodes to identical bytes.
fuzz_target!(|fv: FuzzValue| {
    let v = match fv {
        FuzzValue::Null => Value::Null,
        FuzzValue::Bool(b) => Value::Bool(b),
        FuzzValue::I64(n) => Value::I64(n),
        FuzzValue::F64(f) => Value::F64(f),
        FuzzValue::Text(s) => Value::Text(s),
        FuzzValue::Bytes(b) => Value::Bytes(b),
    };
    let mut a = Vec::new(); v.encode(&mut a);
    let mut rd: &[u8] = &a;
    let back = Value::decode(&mut rd).expect("valid value decodes");
    // NaN != NaN, so compare bytes not values for floats.
    let mut b = Vec::new(); back.encode(&mut b);
    assert_eq!(a, b, "re-encode not byte-stable");
});
```

Add `arbitrary = { version = "1", features = ["derive"] }` to `fuzz/Cargo.toml` deps.

- [ ] **Step 4: Smoke-run both targets**

Run: `cargo +nightly fuzz run decode_frame -- -runs=100000`
Expected: completes, no crash, no OOM.
Run: `cargo +nightly fuzz run roundtrip_frame -- -runs=50000`
Expected: completes, no crash.

- [ ] **Step 5: Commit**

```bash
git add engine/crates/ferro-proto/fuzz
git commit -m "test(s1): cargo-fuzz decode + roundtrip targets for the frame codec"
```

---

### Task 8: PHP codec — pure-PHP MessagePack + Frame/Value/Codec + Constants generation

**Files:**
- Create: `/proto/tools/gen-php.php`
- Create (generated): `/php/client/src/Protocol/Generated/Constants.php`
- Create: `/php/client/src/Protocol/Msgpack/{PackerInterface,PurePacker,ExtPacker,PackerFactory}.php`
- Create: `/php/client/src/Protocol/{CodecException,Header,Value,Codec,Message}.php`
- Create: `/php/client/tests/Unit/{HeaderTest,ValueTest,PurePackerTest}.php`

**Interfaces:**
- Consumes: `/proto/registry.lock.json`, `/proto/vectors/*`.
- Produces: `Ferro\Protocol\Codec` with `encodeFrame(Header, string $payload): string` and `decodeFrame(string): array{0:Header,1:string}`; `Ferro\Protocol\Value` with `encode(PackerInterface): string` / static `decode(PackerInterface, string, int &$offset): Value`; `Ferro\Protocol\Message::encode(string $name, array $fields, PackerInterface): string` (client-sent core messages); `Ferro\Protocol\Msgpack\PackerInterface`. Consumed by Task 9 (conformance) and all later PHP client slices.

- [ ] **Step 1: Write the PHP constants generator**

```php
<?php // /proto/tools/gen-php.php — reads registry.lock.json, emits Generated/Constants.php
declare(strict_types=1);
$root = dirname(__DIR__, 2);
$lock = json_decode(file_get_contents("$root/proto/registry.lock.json"), true, 512, JSON_THROW_ON_ERROR);
$out = "<?php\n\ndeclare(strict_types=1);\n\n// @generated from /proto/registry.lock.json — do not edit.\n\nnamespace Ferro\\Protocol\\Generated;\n\nfinal class Constants\n{\n";
$out .= "    public const PROTOCOL_VERSION = {$lock['protocol_version']};\n";
$out .= "    public const MAGIC = {$lock['magic']};\n";
$out .= "    public const MAX_FRAME_PAYLOAD = {$lock['max_frame_payload']};\n";
$out .= "    public const DEFAULT_CREDIT_FRAMES = {$lock['default_credit_frames']};\n";
$out .= "    public const DEFAULT_CREDIT_BYTES = {$lock['default_credit_bytes']};\n\n";
$emit = function (string $prefix, array $kv) {
    $s = '';
    foreach ($kv as $k => $v) { $s .= "    public const {$prefix}_{$k} = {$v};\n"; }
    return $s;
};
foreach ($lock['flags'] as $k => $v) { $out .= "    public const FLAG_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['services'] as $k => $v) { $out .= "    public const SERVICE_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['methods'] as $svc => $kv) {
    foreach ($kv as $k => $v) { $out .= "    public const METHOD_" . strtoupper($svc) . "_{$k} = {$v};\n"; }
}
$out .= "\n";
foreach ($lock['outcome'] as $k => $v) { $out .= "    public const OUTCOME_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['tags'] as $k => $v) { $out .= "    public const TAG_{$k} = {$v};\n"; }
$out .= "\n";
foreach ($lock['branches'] as $k => $v) {
    // camel-split to match Rust screaming(): NonRetryable -> NON_RETRYABLE
    $u = strtoupper(preg_replace('/(?<=[a-z0-9])(?=[A-Z])/', '_', $k));
    $out .= "    public const BRANCH_{$u} = {$v};\n";
}
$out .= "\n";
foreach ($lock['features'] as $side => $kv) {
    foreach ($kv as $k => $v) { $out .= "    public const FEATURE_" . strtoupper($side) . "_{$k} = {$v};\n"; }
}
$out .= "\n";
foreach ($lock['codes'] as $name => $ec) {
    $u = strtoupper(preg_replace('/(?<=[a-z0-9])(?=[A-Z])/', '_', $name));
    $out .= "    public const ERR_{$u} = {$ec['code']};\n";
    $out .= "    public const ERR_{$u}_BRANCH = {$ec['branch']};\n";
}
$out .= "}\n";
$dir = "$root/php/client/src/Protocol/Generated";
@mkdir($dir, 0777, true);
file_put_contents("$dir/Constants.php", $out);
fwrite(STDERR, "wrote $dir/Constants.php\n");
```

Run: `php proto/tools/gen-php.php`
Expected: writes `Constants.php`.

- [ ] **Step 2: Write the failing PurePacker unit test (canonical byte assertions)**

```php
<?php // /php/client/tests/Unit/PurePackerTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

final class PurePackerTest extends TestCase
{
    public function testCanonicalScalars(): void
    {
        $p = new PurePacker();
        $this->assertSame("\xc0", $p->packNil());
        $this->assertSame("\xc3", $p->packBool(true));
        $this->assertSame("\x01", $p->packInt(1));            // positive fixint
        $this->assertSame("\xcc\xc8", $p->packInt(200));      // uint8 (matches rmp write_sint)
        $this->assertSame("\xd1\xff\x38", $p->packInt(-200)); // int16 (negatives keep the signed marker)
        $this->assertSame("\xcb" . pack('E', 1.5), $p->packFloat64(1.5)); // 'E' = big-endian double
        $this->assertSame("\xa2hi", $p->packStr('hi'));      // fixstr
        $this->assertSame("\xc4\x03\x01\x02\x03", $p->packBin("\x01\x02\x03"));
        $this->assertSame("\x92", $p->packArrayLen(2));      // fixarray(2)
    }

    public function testUint64BeyondPhpIntDecodesToString(): void
    {
        $p = new PurePacker();
        $bytes = "\xcf\xff\xff\xff\xff\xff\xff\xff\xf0"; // uint64 0xFFFFFFFFFFFFFFF0
        $off = 0;
        $this->assertSame('18446744073709551600', $p->unpack($bytes, $off));
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `(cd php/client && ./vendor/bin/phpunit --filter PurePacker)`
Expected: FAIL (class missing).

- [ ] **Step 4: Implement the packer interface + pure packer**

```php
<?php // /php/client/src/Protocol/Msgpack/PackerInterface.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

interface PackerInterface
{
    public function packNil(): string;
    public function packBool(bool $b): string;
    public function packInt(int $n): string;         // signed-smallest canonical
    public function packUint(int|string $n): string; // unsigned-smallest; string allows > PHP_INT_MAX
    public function packFloat64(float $f): string;
    public function packStr(string $s): string;
    public function packBin(string $s): string;
    public function packArrayLen(int $n): string;
    /** @return mixed decoded scalar/array; advances $offset */
    public function unpack(string $buf, int &$offset): mixed;
}
```

```php
<?php // /php/client/src/Protocol/Msgpack/PurePacker.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

use Ferro\Protocol\CodecException;

/**
 * Dependency-free MessagePack encoder/decoder pinned to Ferro's canonical profile
 * (signed-smallest ints, big-endian float64, str/bin families, fixarray). Mirrors `rmp`.
 */
final class PurePacker implements PackerInterface
{
    public function packNil(): string { return "\xc0"; }
    public function packBool(bool $b): string { return $b ? "\xc3" : "\xc2"; }

    public function packInt(int $n): string
    {
        // Canonical = rmp write_sint: NON-NEGATIVE narrows to unsigned markers (cc/cd/ce/cf),
        // NEGATIVE to signed markers (d0/d1/d2/d3). This is the load-bearing cross-language rule.
        if ($n >= 0) {
            if ($n <= 0x7f) { return chr($n); }                      // positive fixint
            if ($n <= 0xff) { return "\xcc" . chr($n); }             // uint8
            if ($n <= 0xffff) { return "\xcd" . pack('n', $n); }     // uint16 BE
            if ($n <= 0xffffffff) { return "\xce" . pack('N', $n); } // uint32 BE
            return "\xcf" . pack('J', $n);                           // uint64 BE
        }
        if ($n >= -32) { return chr(0xe0 | ($n & 0x1f)); }           // negative fixint
        if ($n >= -128) { return "\xd0" . pack('c', $n); }           // int8
        if ($n >= -32768) { return "\xd1" . pack('n', $n & 0xffff); } // int16 BE
        if ($n >= -2147483648) { return "\xd2" . pack('N', $n & 0xffffffff); } // int32 BE
        return "\xd3" . pack('J', $n);                               // int64 BE
    }

    public function packUint(int|string $n): string
    {
        // Only used for fields known unsigned (e.g. boot_epoch). Accept string for > PHP_INT_MAX.
        if (is_string($n)) {
            // value > PHP_INT_MAX: emit uint64 big-endian from decimal string.
            return "\xcf" . self::decToBe64($n);
        }
        if ($n < 0) { throw new CodecException('packUint got negative'); }
        if ($n <= 0x7f) { return chr($n); }
        if ($n <= 0xff) { return "\xcc" . chr($n); }
        if ($n <= 0xffff) { return "\xcd" . pack('n', $n); }
        if ($n <= 0xffffffff) { return "\xce" . pack('N', $n); }
        return "\xcf" . pack('J', $n);
    }

    public function packFloat64(float $f): string { return "\xcb" . pack('E', $f); } // 'E' = double BE

    public function packStr(string $s): string
    {
        $len = strlen($s);
        if ($len <= 31) { return chr(0xa0 | $len) . $s; }
        if ($len <= 0xff) { return "\xd9" . chr($len) . $s; }
        if ($len <= 0xffff) { return "\xda" . pack('n', $len) . $s; }
        return "\xdb" . pack('N', $len) . $s;
    }

    public function packBin(string $s): string
    {
        $len = strlen($s);
        if ($len <= 0xff) { return "\xc4" . chr($len) . $s; }
        if ($len <= 0xffff) { return "\xc5" . pack('n', $len) . $s; }
        return "\xc6" . pack('N', $len) . $s;
    }

    public function packArrayLen(int $n): string
    {
        if ($n <= 15) { return chr(0x90 | $n); }
        if ($n <= 0xffff) { return "\xdc" . pack('n', $n); }
        return "\xdd" . pack('N', $n);
    }

    public function unpack(string $buf, int &$offset): mixed
    {
        $c = ord($buf[$offset++]);
        if ($c <= 0x7f) { return $c; }                       // positive fixint
        if ($c >= 0xe0) { return $c - 0x100; }               // negative fixint
        if ($c >= 0x90 && $c <= 0x9f) { return $this->unpackArray($buf, $offset, $c & 0x0f); }
        if ($c >= 0xa0 && $c <= 0xbf) { return $this->take($buf, $offset, $c & 0x1f); } // fixstr
        return match ($c) {
            0xc0 => null,
            0xc2 => false, 0xc3 => true,
            0xcc => ord($buf[$offset++]),
            0xcd => $this->be($buf, $offset, 2, false),
            0xce => $this->be($buf, $offset, 4, false),
            0xcf => $this->be($buf, $offset, 8, false),
            0xd0 => $this->signed8($buf, $offset),
            0xd1 => $this->be($buf, $offset, 2, true),
            0xd2 => $this->be($buf, $offset, 4, true),
            0xd3 => $this->be($buf, $offset, 8, true),
            0xca => $this->unpackF32($buf, $offset),
            0xcb => $this->unpackF64($buf, $offset),
            0xd9 => $this->take($buf, $offset, ord($buf[$offset++])),
            0xda => $this->take($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xdb => $this->take($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            0xc4 => $this->take($buf, $offset, ord($buf[$offset++])),
            0xc5 => $this->take($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xc6 => $this->take($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            0xdc => $this->unpackArray($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xdd => $this->unpackArray($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            default => throw new CodecException(sprintf('unknown msgpack marker 0x%02x', $c)),
        };
    }

    /** @return list<mixed> */
    private function unpackArray(string $buf, int &$offset, int $n): array
    {
        $a = [];
        for ($i = 0; $i < $n; $i++) { $a[] = $this->unpack($buf, $offset); }
        return $a;
    }
    private function take(string $buf, int &$offset, int $len): string
    {
        $s = substr($buf, $offset, $len); $offset += $len; return $s;
    }
    private function signed8(string $buf, int &$offset): int
    {
        $v = ord($buf[$offset++]); return $v < 128 ? $v : $v - 256;
    }
    /** Big-endian integer of $bytes width; returns int, or decimal string for uint64 > PHP_INT_MAX. */
    private function be(string $buf, int &$offset, int $bytes, bool $signed): int|string
    {
        $slice = substr($buf, $offset, $bytes); $offset += $bytes;
        if ($bytes < 8) {
            $v = 0; foreach (str_split($slice) as $b) { $v = ($v << 8) | ord($b); }
            if ($signed) { $bits = $bytes * 8; if ($v >= (1 << ($bits - 1))) { $v -= (1 << $bits); } }
            return $v;
        }
        // 8 bytes
        if ($signed) { return unpack('J', $slice)[1]; } // PHP int is 64-bit signed
        return self::be64ToDec($slice); // unsigned 64: return decimal string to preserve > PHP_INT_MAX
    }
    private function unpackF32(string $buf, int &$offset): float
    { $s = substr($buf, $offset, 4); $offset += 4; return (float) unpack('G', $s)[1]; }
    private function unpackF64(string $buf, int &$offset): float
    { $s = substr($buf, $offset, 8); $offset += 8; return (float) unpack('E', $s)[1]; }

    private static function be64ToDec(string $be): string
    {
        // Convert 8 big-endian bytes to an unsigned decimal string without bcmath/gmp.
        $dec = '0';
        foreach (str_split($be) as $byte) {
            $carry = ord($byte);
            $res = ''; $i = strlen($dec) - 1; $mul = '';
            // dec = dec*256 + carry, done via simple string math
            $dec = self::mulAdd($dec, 256, $carry);
        }
        return $dec;
    }
    private static function mulAdd(string $dec, int $mul, int $add): string
    {
        $carry = $add; $out = '';
        for ($i = strlen($dec) - 1; $i >= 0; $i--) {
            $prod = ((int) $dec[$i]) * $mul + $carry;
            $out = ((string) ($prod % 10)) . $out;
            $carry = intdiv($prod, 10);
        }
        while ($carry > 0) { $out = ((string) ($carry % 10)) . $out; $carry = intdiv($carry, 10); }
        return ltrim($out, '0') ?: '0';
    }
    private static function decToBe64(string $dec): string
    {
        // Convert an unsigned decimal string to 8 big-endian bytes.
        $bytes = array_fill(0, 8, 0); $n = $dec;
        for ($pos = 7; $pos >= 0 && $n !== '0'; $pos--) {
            [$n, $rem] = self::divmod($n, 256); $bytes[$pos] = $rem;
        }
        return implode('', array_map('chr', $bytes));
    }
    /** @return array{0:string,1:int} */
    private static function divmod(string $dec, int $div): array
    {
        $q = ''; $rem = 0;
        for ($i = 0; $i < strlen($dec); $i++) {
            $cur = $rem * 10 + (int) $dec[$i];
            $q .= (string) intdiv($cur, $div); $rem = $cur % $div;
        }
        return [ltrim($q, '0') ?: '0', $rem];
    }
}
```

- [ ] **Step 5: Implement `CodecException`, `Header`, `Value`, `Codec`, `ExtPacker`, `PackerFactory`**

```php
<?php // /php/client/src/Protocol/CodecException.php
declare(strict_types=1);
namespace Ferro\Protocol;
final class CodecException extends \RuntimeException {}
```
```php
<?php // /php/client/src/Protocol/Header.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;

final class Header
{
    public function __construct(
        public readonly int $flags, public readonly int $service, public readonly int $method,
        public readonly int $requestId, public readonly int $payloadLen,
    ) {}

    public function encode(): string
    {
        // C C v v v V V  => u8 u8 u16le u16le u16le u32le u32le
        return pack('CCvvvVV', C::MAGIC, C::PROTOCOL_VERSION,
            $this->flags, $this->service, $this->method, $this->requestId, $this->payloadLen);
    }

    public static function decode(string $buf): self
    {
        if (strlen($buf) < 16) { throw new CodecException('short header'); }
        $u = unpack('Cmagic/Cver/vflags/vservice/vmethod/Vreq/Vlen', substr($buf, 0, 16));
        if ($u['magic'] !== C::MAGIC) { throw new CodecException(sprintf('bad magic 0x%02x', $u['magic'])); }
        if ($u['ver'] !== C::PROTOCOL_VERSION) { throw new CodecException('bad version ' . $u['ver']); }
        if ($u['len'] > C::MAX_FRAME_PAYLOAD) { throw new CodecException('frame too large ' . $u['len']); }
        return new self($u['flags'], $u['service'], $u['method'], $u['req'], $u['len']);
    }
}
```
```php
<?php // /php/client/src/Protocol/Value.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

final class Value
{
    private function __construct(public readonly int $tag, public readonly mixed $data) {}

    public static function null(): self { return new self(C::TAG_NULL, null); }
    public static function bool(bool $b): self { return new self(C::TAG_BOOL, $b); }
    public static function i64(int $n): self { return new self(C::TAG_I64, $n); }
    public static function f64(float $f): self { return new self(C::TAG_F64, $f); }
    public static function text(string $s): self { return new self(C::TAG_TEXT, $s); }
    public static function bytes(string $s): self { return new self(C::TAG_BYTES, $s); }

    public function encode(PackerInterface $p): string
    {
        $payload = match ($this->tag) {
            C::TAG_NULL => $p->packNil(),
            C::TAG_BOOL => $p->packBool((bool) $this->data),
            C::TAG_I64 => $p->packInt((int) $this->data),
            C::TAG_F64 => $p->packFloat64((float) $this->data),
            C::TAG_TEXT => $p->packStr((string) $this->data),
            C::TAG_BYTES => $p->packBin((string) $this->data),
            default => throw new CodecException('unsupported TypedValue tag ' . $this->tag),
        };
        return $p->packArrayLen(2) . $p->packInt($this->tag) . $payload;
    }

    public static function decode(PackerInterface $p, string $buf, int &$offset): self
    {
        $arr = $p->unpack($buf, $offset);
        if (!is_array($arr) || count($arr) !== 2) { throw new CodecException('bad TypedValue array'); }
        return new self((int) $arr[0], $arr[1]);
    }
}
```
```php
<?php // /php/client/src/Protocol/Codec.php
declare(strict_types=1);
namespace Ferro\Protocol;

final class Codec
{
    public function encodeFrame(Header $header, string $payload): string
    {
        return $header->encode() . $payload;
    }

    /** @return array{0:Header,1:string} */
    public function decodeFrame(string $frame): array
    {
        $h = Header::decode($frame);
        $payload = substr($frame, 16, $h->payloadLen);
        if (strlen($payload) !== $h->payloadLen) { throw new CodecException('truncated payload'); }
        return [$h, $payload];
    }
}
```
```php
<?php // /php/client/src/Protocol/Message.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Encodes core-service messages as positional MessagePack arrays whose field order matches
 * PROTOCOL.md and the Rust structs (compact rmp-serde layout). Byte-identity vs the Rust codec is
 * locked by golden vectors (Task 9). Server->client-only payloads (error Outcomes) are decode-only.
 * All integer fields here are unsigned in the Rust structs, so packUint is correct throughout.
 */
final class Message
{
    /** @param array<string,mixed> $f logical fields (e.g. a golden vector's "message" object) */
    public static function encode(string $name, array $f, PackerInterface $p): string
    {
        return match ($name) {
            'hello' => self::arr($p, [
                $p->packUint(self::i($f, 'client_version')),
                $p->packStr(self::s($f, 'type_registry_hash')),
                ($f['manifest_hash'] ?? null) === null ? $p->packNil() : $p->packStr(self::s($f, 'manifest_hash')),
                $p->packUint(self::i($f, 'pid')),
                $p->packUint(self::i($f, 'features')),
            ]),
            'hello_ack' => self::arr($p, [
                $p->packUint(self::i($f, 'engine_version')),
                $p->packUint(self::u($f, 'boot_epoch')),   // string-safe for > PHP_INT_MAX
                $p->packUint(self::i($f, 'features')),
                self::strArray($p, $f['pools'] ?? []),
                $p->packStr(self::s($f, 'type_registry_hash')),
            ]),
            'ping', 'pong' => self::arr($p, [$p->packUint(self::u($f, 'token'))]),
            'goodbye' => self::arr($p, []),
            'window_update' => self::arr($p, [$p->packUint(self::i($f, 'frames')), $p->packUint(self::i($f, 'bytes'))]),
            default => throw new CodecException("no client encoder for message '$name'"),
        };
    }

    /** @param list<string> $items already-encoded field byte-strings */
    private static function arr(PackerInterface $p, array $items): string
    {
        return $p->packArrayLen(count($items)) . implode('', $items);
    }
    private static function strArray(PackerInterface $p, mixed $pools): string
    {
        $list = is_array($pools) ? $pools : [];
        $out = $p->packArrayLen(count($list));
        foreach ($list as $s) { $out .= $p->packStr((string) $s); }
        return $out;
    }
    /** @param array<string,mixed> $f */ private static function i(array $f, string $k): int { return (int) ($f[$k] ?? 0); }
    /** @param array<string,mixed> $f */ private static function s(array $f, string $k): string { return (string) ($f[$k] ?? ''); }
    /** @param array<string,mixed> $f @return int|string */ private static function u(array $f, string $k): int|string
    { $v = $f[$k] ?? 0; return is_string($v) ? $v : (int) $v; }
}
```
```php
<?php // /php/client/src/Protocol/Msgpack/ExtPacker.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;
use Ferro\Protocol\CodecException;

/** ext-msgpack fast path. Only used where it byte-matches the canonical profile (conformance-gated). */
final class ExtPacker implements PackerInterface
{
    public function __construct() { if (!\extension_loaded('msgpack')) { throw new CodecException('ext-msgpack absent'); } }
    public function packNil(): string { return \msgpack_pack(null); }
    public function packBool(bool $b): string { return \msgpack_pack($b); }
    public function packInt(int $n): string { return \msgpack_pack($n); }
    public function packUint(int|string $n): string { return \msgpack_pack(is_string($n) ? (int) $n : $n); }
    public function packFloat64(float $f): string { return \msgpack_pack($f); }
    public function packStr(string $s): string { return \msgpack_pack($s); }
    public function packBin(string $s): string { return \msgpack_pack($s); }
    public function packArrayLen(int $n): string { throw new CodecException('ExtPacker packs whole values, not array headers'); }
    public function unpack(string $buf, int &$offset): mixed { $v = \msgpack_unpack($buf); $offset = strlen($buf); return $v; }
}
```

> ext-msgpack packs *whole values*, not incremental array headers, so `ExtPacker` cannot drive the incremental `Value::encode` path. Per R3's mitigation, the ext path is used for **decode conformance** and whole-message encode where it byte-matches; the incremental encode path stays on `PurePacker`. `PackerFactory::forEncode()` returns `PurePacker`; `PackerFactory::forDecode()` returns `ExtPacker` when loaded else `PurePacker`. Document this split in the class docblocks.

```php
<?php // /php/client/src/Protocol/Msgpack/PackerFactory.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

final class PackerFactory
{
    public static function forEncode(): PackerInterface { return new PurePacker(); }
    public static function forDecode(): PackerInterface
    {
        return \extension_loaded('msgpack') ? new ExtPacker() : new PurePacker();
    }
}
```

- [ ] **Step 6: Write the Header + Value unit tests**

```php
<?php // /php/client/tests/Unit/HeaderTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Header;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

final class HeaderTest extends TestCase
{
    public function testRoundtrip(): void
    {
        $h = new Header(C::FLAG_END, C::SERVICE_CORE, C::METHOD_CORE_PING, 0x0A0B0C0D, 1);
        $b = $h->encode();
        $this->assertSame(16, strlen($b));
        $this->assertSame(C::MAGIC, ord($b[0]));
        $d = Header::decode($b);
        $this->assertSame($h->requestId, $d->requestId);
        $this->assertSame($h->payloadLen, $d->payloadLen);
    }
    public function testRejectsBadMagic(): void
    {
        $b = (new Header(0, 1, 3, 1, 0))->encode(); $b[0] = "\x00";
        $this->expectException(CodecException::class);
        Header::decode($b);
    }
    public function testRejectsOversizeLen(): void
    {
        $b = (new Header(0, 2, 1, 1, 0))->encode();
        $big = pack('V', C::MAX_FRAME_PAYLOAD + 1);
        $b = substr($b, 0, 12) . $big;
        $this->expectException(CodecException::class);
        Header::decode($b);
    }
}
```
```php
<?php // /php/client/tests/Unit/ValueTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Value;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

final class ValueTest extends TestCase
{
    public function testCanonicalBytesMatchRust(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x92\x00\xc0", Value::null()->encode($p));
        $this->assertSame("\x92\x01\xc3", Value::bool(true)->encode($p));
        $this->assertSame("\x92\x02\x01", Value::i64(1)->encode($p));
        $this->assertSame("\x92\x02\xcc\xc8", Value::i64(200)->encode($p)); // uint8, matches Rust
        $this->assertSame("\x92\x02\xd1\xff\x38", Value::i64(-200)->encode($p)); // int16
        $this->assertSame("\x92\x06\xa2hi", Value::text('hi')->encode($p));
        $this->assertSame("\x92\x07\xc4\x03\x01\x02\x03", Value::bytes("\x01\x02\x03")->encode($p));
    }
}
```

- [ ] **Step 7: Run to verify pass**

Run: `(cd php/client && ./vendor/bin/phpunit --testsuite ferro-client --filter 'Header|Value|PurePacker')`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add proto/tools/gen-php.php php/client/src php/client/tests/Unit
git commit -m "feat(s1): pure-PHP msgpack + Header/Value/Codec + Constants generation"
```

---

### Task 9: PHP vector conformance + Constants sync + PHPStan gate

**Files:**
- Create: `/php/client/tests/Conformance/VectorConformanceTest.php`, `/php/client/tests/Conformance/RegistrySyncTest.php`

**Interfaces:**
- Consumes: `/proto/vectors/*.json`, `/proto/registry.lock.json`, the Task-8 PHP codec. No new production API.

- [ ] **Step 1: Write the vector conformance test (byte-match both paths)**

```php
<?php // /php/client/tests/Conformance/VectorConformanceTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\{PurePacker, ExtPacker};
use PHPUnit\Framework\TestCase;

final class VectorConformanceTest extends TestCase
{
    private const DIR = __DIR__ . '/../../../../proto/vectors';

    /** @return iterable<string, array{0:array<string,mixed>}> */
    public static function vectors(): iterable
    {
        foreach (glob(self::DIR . '/*.json') ?: [] as $f) {
            /** @var array<string,mixed> $v */
            $v = json_decode((string) file_get_contents($f), true, 512, JSON_THROW_ON_ERROR);
            yield basename($f) => [$v];
        }
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testHeaderDecodesToVectorFields(array $v): void
    {
        $frame = (string) hex2bin((string) $v['frame_hex']);
        $h = Header::decode($frame);
        $this->assertSame($v['header']['service'], $h->service, "service for {$v['name']}");
        $this->assertSame($v['header']['method'], $h->method, "method for {$v['name']}");
        $this->assertSame(strlen($frame) - 16, $h->payloadLen, "payload_len for {$v['name']}");
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testPurePackerDecodesPayloadToLogicalMessage(array $v): void
    {
        $frame = (string) hex2bin((string) $v['frame_hex']);
        $payload = substr($frame, 16);
        $p = new PurePacker();
        $off = 0;
        $decoded = $p->unpack($payload, $off);
        $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$v['name']}");
        $this->assertIsArray($decoded, "every S1 message payload is a positional array for {$v['name']}");
    }

    /**
     * THE cross-language byte lock: PurePacker must re-encode each client-sent message to the EXACT
     * bytes the Rust codec produced. hello_ack is included (encoding boot_epoch from its decimal
     * string yields the exact uint64 bytes); error_protocol is an Outcome the client never sends,
     * so it is decode-only and skipped here. A rmp-serde map-vs-array default mismatch, a field-order
     * bug, or an integer-width divergence all fail HERE rather than silently in S5.
     * @param array<string,mixed> $v
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testPurePackerEncodesMessageToExactVectorBytes(array $v): void
    {
        $name = (string) $v['name'];
        if (!in_array($name, ['hello', 'hello_ack', 'ping', 'pong', 'goodbye', 'window_update'], true)) {
            $this->markTestSkipped("{$name} is decode-only for the client in S1 (no message encoder)");
        }
        $fields = is_array($v['message']) ? $v['message'] : [];
        $payload = Message::encode($name, $fields, new PurePacker());
        $expected = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $this->assertSame(bin2hex($expected), bin2hex($payload),
            "PHP-encoded {$name} payload must byte-match the Rust-generated vector");
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testExtPackerDecodeMatchesPureWhenLoaded(array $v): void
    {
        if (!\extension_loaded('msgpack')) { $this->markTestSkipped('ext-msgpack not loaded (CI provisions it)'); }
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $off = 0;
        $pure = (new PurePacker())->unpack($payload, $off);
        if (self::hasBigUint($pure)) {
            // ext-msgpack decodes a uint64 > PHP_INT_MAX to a LOSSY float; PurePacker returns the
            // exact decimal string and is authoritative. The two are not comparable here — pure-only
            // coverage lives in PurePackerTest::testUint64BeyondPhpIntDecodesToString.
            $this->markTestSkipped("vector {$v['name']} carries a uint64 > PHP_INT_MAX (ext-msgpack lossy)");
        }
        $off = 0;
        $ext = (new ExtPacker())->unpack($payload, $off);
        $this->assertEquals(json_encode($pure), json_encode($ext), "ext vs pure decode for {$v['name']}");
    }

    /** True if $v (recursively) contains a decimal string that exceeds PHP_INT_MAX — PurePacker's
     *  representation of a uint64 the msgpack extension cannot decode losslessly. */
    private static function hasBigUint(mixed $v): bool
    {
        if (is_array($v)) {
            foreach ($v as $x) { if (self::hasBigUint($x)) { return true; } }
            return false;
        }
        if (!is_string($v) || !preg_match('/^\d+$/', $v)) { return false; }
        $s = ltrim($v, '0');
        if ($s === '') { $s = '0'; }
        $max = '9223372036854775807';
        return strlen($s) > strlen($max) || (strlen($s) === strlen($max) && strcmp($s, $max) > 0);
    }
}
```

- [ ] **Step 2: Write the Constants sync test**

```php
<?php // /php/client/tests/Conformance/RegistrySyncTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use PHPUnit\Framework\TestCase;

final class RegistrySyncTest extends TestCase
{
    public function testGeneratedConstantsMatchLock(): void
    {
        $root = dirname(__DIR__, 4);
        $constants = "$root/php/client/src/Protocol/Generated/Constants.php";
        $before = (string) file_get_contents($constants);
        // Regenerate into a temp location by running the generator, then diff.
        $tmpHome = sys_get_temp_dir() . '/ferro_gen_' . getmypid();
        exec(sprintf('php %s 2>&1', escapeshellarg("$root/proto/tools/gen-php.php")), $out, $rc);
        $this->assertSame(0, $rc, 'gen-php.php failed: ' . implode("\n", $out));
        $after = (string) file_get_contents($constants);
        $this->assertSame($before, $after,
            'Constants.php is stale — run `php proto/tools/gen-php.php` and commit');
    }
}
```

- [ ] **Step 3: Run conformance + full suite + PHPStan**

Run: `(cd php/client && ./vendor/bin/phpunit)`
Expected: PASS (unit + conformance; ext-msgpack test skips locally, runs in CI).
Run: `(cd php/client && ./vendor/bin/phpstan analyse src --level 9)`
Expected: `[OK] No errors`.

- [ ] **Step 4: Cross-language sanity — regenerate everything, expect zero diff**

Run:
```bash
cargo run -p ferro-proto --bin gen-registry-lock
cargo run -p ferro-proto --bin gen-vectors
php proto/tools/gen-php.php
git status --porcelain
```
Expected: no changes (all generated artifacts already committed and in sync).

- [ ] **Step 5: Full Rust gate**

Run: `cargo fmt --check && cargo clippy -p ferro-proto -- -D warnings && cargo test -p ferro-proto`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add php/client/tests/Conformance
git commit -m "test(s1): cross-language vector conformance + constants sync gate"
```

---

## Self-Review

> **Plan v2** — revised after an adversarial verification pass (workflow `wf_9022d315`) that empirically checked byte layouts against rmp 0.8.15 and PHP 8.4. Applied: 4 blockers, 3 majors, 4 minors. See the changelog at the end.

**Spec coverage (design doc S1 acceptance gate):**
- `cargo test -p ferro-proto` (golden vectors, **message byte-stability**, header bounds, registry_lock_sync) → Tasks 3, 6, 2. ✓
- Fuzz decode no-crash/bounded-alloc → Task 7. ✓
- Regen → zero git diff → Task 9 Step 4. ✓
- **PHP encodes each core message to byte-identical vector bytes** (the real cross-language lock, was missing) → Task 9 `testPurePackerEncodesMessageToExactVectorBytes`, backed by the Rust `message_payloads_are_canonical_and_byte_stable` test. ✓
- ext-msgpack decode conformance where representable; uint64 > PHP_INT_MAX is pure-only (ext lossy) → Task 9. ✓
- `/proto` single source of truth, both codecs generated (branches + features now mirrored) → Tasks 2, 8. ✓
- ERROR payload + Outcome envelope + branch-on-wire → Task 5. ✓

**Integer canonical rule (the fixed landmine):** both codecs follow rmp `write_sint` — non-negative → uint markers (`200`=`cc c8`), negative → int markers (`-200`=`d1 ff 38`). Locked by a TypedValue in the Rust value tests **and** by the PHP `packInt`/`ValueTest` assertions; a divergence now fails a unit test, not silently in S5.

**Placeholder scan:** No `TODO`/`TBD`/"handle edge cases". Two *implementation notes* flag `rmp`/`rmp-serde` exact-name/compact-default confirmation with the byte-asserting tests + vectors as the arbiter — verification instructions, not deferred work.

**Type consistency:** `Header` fields identical in Rust (Task 3) and PHP (Task 8). `Value` variants/tags consistent across Tasks 4/8. Constants naming (`service::CORE`↔`SERVICE_CORE`, `method_core::PING`↔`METHOD_CORE_PING`, `errc::PROTOCOL`↔`ERR_PROTOCOL`, `branch::NON_RETRYABLE`↔`BRANCH_NON_RETRYABLE`) consistent between `build.rs` (Task 2) and `gen-php.php` (Task 8). PHP `Value`/`Message` signatures in the interface blocks match their implementations.

**Known execution-time confirmations (not gaps):**
1. `rmp 0.8` exact low-level function names in Task 4 — tests assert exact bytes; adjust calls to satisfy, never the asserted bytes.
2. `rmp-serde 1.x` compact-struct-as-array default in Task 5 — the message roundtrip tests, the Rust byte-stability test, AND the PHP encode-byte-match test are the arbiter; if rmp-serde defaults to maps, switch it to compact so PHP array-encoding matches.
3. All three `/proto/*.toml` files are now explicit one-key-per-line / per-code tables — verified to parse; `gen-registry-lock` in Task 2 Step 4 is the parse gate.

## Changelog (plan v1 → v2, from verification `wf_9022d315`)
- **B1 integer rule:** rmp `write_sint` narrows non-negative ≥128 to unsigned markers (`200`=`cc c8`, not `d1 00 c8`). Aligned Rust test, PHP `packInt`, PHP tests, prose; added a TypedValue-carrying assertion so the ladder is locked.
- **B2 deps:** `serde_json` + `toml` moved to `[dependencies]` (bins/lib can't see dev-deps).
- **B3 bins:** Task 1 creates `gen_registry_lock.rs`/`gen_vectors.rs` stub `main()`s (declared `[[bin]]` sources must exist).
- **B4 TOML:** `types.toml` one-key-per-line with `m0_scalar` hoisted above `[tags]`; `errors.toml` per-code `[codes.X]` tables.
- **M1 ext/uint64:** ext-vs-pure decode test skips uint64 > PHP_INT_MAX (ext lossy); pure-only coverage retained.
- **M2 byte-identity:** added Rust message byte-stability test, a PHP `Message` encoder, and a PHP encode-equals-vector test — S1 now actually proves what it claims.
- **M3 negative vector:** `reserved_flag.bin` asserted via `flags::validate`, excluded from the header `is_err` loop.
- **Minors:** PHP floor kept at spec's `>=8.2` (untyped constants, parenthesized `new`); `gen-php.php` branch camel-split + features loop; TDD fail-first note for generate-then-verify tasks; prose/interface fixes (`method_core`, no `Frame` type, PHP `Value`/`Message` signatures).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-23-ferro-m0-s1-wire-foundation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
