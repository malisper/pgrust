#![no_main]
//! Differential: regex_core shipped Rust vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C — see decoder_fuzz::regex_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::regex_diff(data);
});
