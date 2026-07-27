//! `ferro-bench` binary entry point.
//!
//! Task 1 (this commit) ships the crate scaffolding: the pure, unit-tested library
//! (`stats`/`result`/`manifest`/`ferrod_proc`). The live orchestration — resolve a release ferrod,
//! launch it under the Drop guard, drive the PHP bench scripts under JIT off/on + the PDO baseline,
//! compute + assemble + validate + write `bench/results/<UTC>-wsl2.json` — lands in Task 2.

fn main() {
    eprintln!(
        "ferro-bench {} (schema v{}): live orchestration is wired in S8 Task 2.\n\
         The library (stats/result/manifest/ferrod_proc) is complete and unit-tested.",
        env!("CARGO_PKG_VERSION"),
        ferro_bench::result::SCHEMA_VERSION,
    );
    std::process::exit(2);
}
