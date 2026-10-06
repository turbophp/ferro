//! M6-F1a — the Ferro HTTP premise spike. **This is NOT `ferro-http`.**
//!
//! The crate ships no library code. Everything lives in `tests/`, whose only job is to PROVE or
//! REFUTE, against the real `hyper` 1.x / `rustls` stack and against `ferrod`'s real `serve`, the
//! premises SPEC §23.19 owes to slice F1a (the v1 critical path): P1, P14, P15, P19, P7 (h1), P3,
//! P16 and P17. A false premise changes §23.7 before slice F2 starts, which is why they are measured
//! before `ferro-http` exists (the C3-1 / `ferro-sqlite-spike` precedent, SPEC §22.2 (ba)).
//!
//! Each premise test carries a NEGATIVE CONTROL so it cannot pass vacuously, and each was
//! mutation-checked; the verdicts, the evidence and the mutation table are in this crate's
//! `README.md`.
