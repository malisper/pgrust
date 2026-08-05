#![no_main]
//! Differential: backend/access/tablesample shipped Rust vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C — see decoder_fuzz::tablesample_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::tablesample_diff(data);
});
