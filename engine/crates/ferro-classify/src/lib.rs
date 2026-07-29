//! `ferro-classify` — the assist lexer (SPEC §7.1).
//!
//! A dialect-aware keyword *classifier* (NOT a SQL parser) that flags statements mutating
//! protocol-invisible session state, so `ferro-pool` can taint + reset the connection before the
//! next tenant. The RFQ protocol byte (M1-S1) remains the transaction-pin AUTHORITY; this crate is
//! ASSIST-only (see the M1-S2 plan). This is a leaf crate: std-only, no `ferro-pool` dependency.
//!
//! This module is a stub for task T1a — it exists only so the crate compiles and the scanner
//! (`scan.rs`) can be built and tested in isolation. The public `Dialect`/`PinTrigger`/`classify`
//! API lands in T1b.

mod scan;
