//! The dispatch table: `(service, method) -> Route`, deciding whether a decoded frame is core
//! control traffic answered synchronously by the reader loop, a request-bearing frame that goes
//! through the registry + spawned-handler + supervisor mechanism (SQL/TX/STREAM — the
//! exactly-one-`END` path, `session::mod`'s `handle_request_frame`), or something this build has
//! no route for at all.
//!
//! `Admin` covers an `ADMIN` method this build serves (M2-C3-7b): it goes through the SAME request
//! lifecycle as SQL/TX/STREAM, but only after the session's D15 gate (`crate::admin::authorize`)
//! admits the peer — so the routing decision carries the verb, and the verb carries its class.
//!
//! `Unsupported` covers an `ADMIN` method this build does not serve, any other unrecognized service,
//! and any CORE method that isn't one of the three control methods this build understands
//! (`HELLO`/`HELLO_ACK` are handshake-only — settled before the reader loop ever starts, and
//! deliberately not a `Route` here at all). An `Unsupported` route never touches the in-flight
//! registry: nothing is spawned, so there is no request lifecycle to guard — the caller sends a
//! per-request `Unsupported` error `END` directly on that frame's `request_id` and moves on; the
//! session survives (SPEC's per-request set, not the session-fatal one).

use ferro_proto::consts::{method_core, method_stream, service};

use crate::admin::AdminVerb;

/// The core control/liveness methods the reader loop answers synchronously: no registry entry,
/// no terminal `END` (these are non-terminal `flags=0` replies, or — for `GOODBYE` — a drain
/// signal with no reply frame of its own at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreMethod {
    Ping,
    Goodbye,
    WindowUpdate,
}

/// Where a decoded frame's `(service, method)` sends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Core control/liveness traffic, answered synchronously by the reader loop.
    CoreControl(CoreMethod),
    /// A request-bearing frame (SQL/TX/STREAM): goes through the registry + spawned-handler +
    /// supervisor mechanism regardless of the specific method id — no SQL/TX/STREAM method is
    /// registered yet (real handlers land in S4/S5), so today's `default_handler` declares
    /// `Unsupported` for all of them. That is a HANDLER decision, not a dispatch one: dispatch's
    /// job here is only "this belongs to the request lifecycle", not "this method exists".
    Request,
    /// An admin verb this build serves (M2-C3-7b). The session authorizes it under SPEC D15 BEFORE it
    /// enters the request lifecycle; an authorized one is then handled exactly like `Request`.
    Admin(AdminVerb),
    /// M3-D4: a client COPY frame (STREAM/`COPY_DATA` or STREAM/`COPY_DONE`) for an ALREADY
    /// in-flight `COPY_IN`. It is not a request: it never enters the registry as one and gets no
    /// terminal of its own — the reader loop routes it to that request's handler, or discards it.
    CopyData { done: bool },
    /// No route at all in this build: an ADMIN method it does not serve, an unrecognized service, or a CORE method this
    /// build doesn't recognize. Produces a per-request `Unsupported` error `END` directly —
    /// there is no request lifecycle to guard because nothing is ever spawned for it.
    Unsupported,
}

/// Decide the route for a decoded frame's `(service, method)`. Pure and total: every `u16` pair
/// maps to exactly one `Route`, never a panic.
pub fn route(service: u16, method: u16) -> Route {
    if service == service::CORE {
        return match method {
            method_core::PING => Route::CoreControl(CoreMethod::Ping),
            method_core::GOODBYE => Route::CoreControl(CoreMethod::Goodbye),
            method_core::WINDOW_UPDATE => Route::CoreControl(CoreMethod::WindowUpdate),
            _ => Route::Unsupported,
        };
    }
    if service == service::STREAM && method == method_stream::COPY_DATA {
        return Route::CopyData { done: false };
    }
    if service == service::STREAM && method == method_stream::COPY_DONE {
        return Route::CopyData { done: true };
    }
    if service == service::SQL || service == service::TX || service == service::STREAM {
        return Route::Request;
    }
    if service == service::ADMIN {
        return AdminVerb::from_method(method).map_or(Route::Unsupported, Route::Admin);
    }
    Route::Unsupported
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_control_methods_route_to_their_variant() {
        assert_eq!(
            route(service::CORE, method_core::PING),
            Route::CoreControl(CoreMethod::Ping)
        );
        assert_eq!(
            route(service::CORE, method_core::GOODBYE),
            Route::CoreControl(CoreMethod::Goodbye)
        );
        assert_eq!(
            route(service::CORE, method_core::WINDOW_UPDATE),
            Route::CoreControl(CoreMethod::WindowUpdate)
        );
    }

    #[test]
    fn unrecognized_core_method_is_unsupported() {
        assert_eq!(route(service::CORE, method_core::HELLO), Route::Unsupported);
        assert_eq!(route(service::CORE, 0xFFFF), Route::Unsupported);
    }

    #[test]
    fn client_copy_frames_route_to_their_copy_in_and_nothing_else_does() {
        assert_eq!(
            route(service::STREAM, method_stream::COPY_DATA),
            Route::CopyData { done: false }
        );
        assert_eq!(
            route(service::STREAM, method_stream::COPY_DONE),
            Route::CopyData { done: true }
        );
        // The engine-to-client STREAM methods are not client data.
        assert_eq!(route(service::STREAM, method_stream::DATA), Route::Request);
        assert_eq!(route(service::STREAM, method_stream::HEAD), Route::Request);
        // A SQL method with the same number is a request.
        assert_eq!(route(service::SQL, method_stream::COPY_DATA), Route::Request);
    }

    #[test]
    fn request_bearing_services_always_route_to_request() {
        for svc in [service::SQL, service::TX, service::STREAM] {
            assert_eq!(route(svc, 0), Route::Request);
            assert_eq!(
                route(svc, 0xFFFF),
                Route::Request,
                "method id doesn't matter yet"
            );
        }
    }

    #[test]
    fn a_served_admin_method_routes_to_its_verb_and_any_other_is_unsupported() {
        use ferro_proto::consts::method_admin;
        assert_eq!(
            route(service::ADMIN, method_admin::BACKUP),
            Route::Admin(AdminVerb::Backup)
        );
        // An ADMIN method id this build does not serve never reaches a handler (or the D15 gate).
        assert_eq!(route(service::ADMIN, 0), Route::Unsupported);
        assert_eq!(route(service::ADMIN, 0xFFFF), Route::Unsupported);
    }

    #[test]
    fn unknown_services_are_unsupported() {
        assert_eq!(route(0xBEEF, 1), Route::Unsupported);
    }
}
