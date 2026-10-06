#![no_main]
// Never panics; an IPv4 address classifies exactly as every embedding of it does.
libfuzzer_sys::fuzz_target!(|data: &[u8]| ferro_http::fuzzing::address(data));
