#![no_main]
// Never panics; anything accepted satisfies the independent oracle and re-validates identically
// after a round trip (see `ferro_http::fuzzing::request`).
libfuzzer_sys::fuzz_target!(|data: &[u8]| ferro_http::fuzzing::request(data));
