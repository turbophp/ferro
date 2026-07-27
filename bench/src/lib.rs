//! `ferro-bench` — the D12 boundary-latency benchmark (the M0 exit gate, SPEC §16.1 / §21 D12).
//!
//! The orchestrator (`main.rs`) launches a RELEASE `ferrod` and the PHP bench scripts as child
//! processes, times the trivial-call path (PHP client -> ferrod -> live `SELECT 1` -> response)
//! against a local PDO baseline in the same environment, computes a stable latency distribution,
//! and writes ONE self-contained, schema-validated JSON with a complete environment manifest to
//! `bench/results/`.
//!
//! The library half is deliberately small and pure so it is unit-testable without a live stack:
//! - [`stats`] — nearest-rank percentiles over a sorted ns sample.
//! - [`result`] — the serde result types + `validate()` (the structural honesty self-check).
//! - [`manifest`] — the host-side environment manifest collector.
//! - [`ferrod_proc`] — a `Child` + socket wrapper whose `Drop` guarantees teardown even on a panic.

pub mod ferrod_proc;
pub mod manifest;
pub mod result;
pub mod stats;
