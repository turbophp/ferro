#![no_main]
// Never panics; an accepted ORIGIN re-parses from its normalised form to the same value.
libfuzzer_sys::fuzz_target!(|data: &[u8]| ferro_http::fuzzing::origin(data));
