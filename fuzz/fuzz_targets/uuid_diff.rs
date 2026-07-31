#![no_main]
//! Differential: adt/uuid shipped Rust vs vendored PostgreSQL 18.3 C
//! (Stamp-18.3, upstream sha 62d6c7d3df) — see decoder_fuzz::uuid_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::uuid_diff(data);
});
