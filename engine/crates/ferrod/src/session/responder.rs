//! Consuming-typestate `Responder`: a handler DECLARES its terminal outcome by consuming `self`
//! in exactly one of `end_ok`/`end_error`/`end_cancelled` — since each takes `self` by value, a
//! second call is a compile error, not a runtime assertion ("at most one declaration" is enforced
//! by the type system, not by a check).
//!
//! Declaring does NOT send anything to the wire: it only stores the outcome into a `cell` that
//! the supervisor holds a clone of (see `session::supervisor`), which reads it back — exactly
//! once — after the handler's spawned task joins. The supervisor, not the handler, is the sole
//! terminal-sender (SPEC's "Terminal-delivery refinement (v2.1)"). A handler that returns (or
//! panics) without calling any `end_*` leaves the cell `None`; the supervisor treats that
//! identically to a panic — a synthesized, distinctly-marked error — so exactly-one-`END` holds
//! even when a handler has a bug.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ferro_proto::messages::ErrorPayload;

/// A handler's declared outcome, as read back by the supervisor. Distinct from
/// `ferro_proto::messages::Outcome` because `Ok` here still holds the handler's owned `Bytes` —
/// the supervisor converts it to the wire `Outcome::Ok(Vec<u8>)` at send time.
#[derive(Debug, Clone)]
pub enum Terminal {
    Ok(Bytes),
    Error(ErrorPayload),
    Cancelled,
}

/// A handler's one-shot terminal declaration.
pub struct Responder {
    cell: Arc<Mutex<Option<Terminal>>>,
}

impl Responder {
    /// Construct a linked `(Responder, cell)` pair. The caller (`session::mod`'s request
    /// dispatch, or a test constructing the pieces directly) hands the `Responder` to the
    /// handler task and keeps `cell` — cloning the `Arc` first if it also needs to hand a copy to
    /// a supervisor task — to read back whatever the handler declares.
    pub fn new_pair() -> (Responder, Arc<Mutex<Option<Terminal>>>) {
        let cell = Arc::new(Mutex::new(None));
        (Responder { cell: cell.clone() }, cell)
    }

    /// Declare success with `body` — the method-specific opaque result bytes (must already be a
    /// single complete MessagePack value, or empty; see `Outcome::encode`'s contract).
    pub fn end_ok(self, body: Bytes) {
        *self.cell.lock().unwrap() = Some(Terminal::Ok(body));
    }

    /// Declare failure.
    pub fn end_error(self, ep: ErrorPayload) {
        *self.cell.lock().unwrap() = Some(Terminal::Error(ep));
    }

    /// Declare cancellation. SPEC: flag-based `CANCEL` is advisory — the handler observes the
    /// cancel flag and calls this itself (or races it against its own natural completion); the
    /// supervisor never synthesizes `Cancelled` on its own (its synthetic path is reserved for
    /// the panic/no-terminal bug case and always uses `Terminal::Error`).
    pub fn end_cancelled(self) {
        *self.cell.lock().unwrap() = Some(Terminal::Cancelled);
    }
}
