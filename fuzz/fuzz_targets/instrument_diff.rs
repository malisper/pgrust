#![no_main]
//! Differential: backend/executor/instrument shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! — see decoder_fuzz::instrument_diff (lane p1-wavea).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::instrument_diff(data);
});
