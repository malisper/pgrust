#![no_main]
//! Differential: contrib/hstore shipped Rust vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C — see
//! decoder_fuzz::hstorefam_diff (lane p1-mb-contribc).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::hstorefam_diff(data);
});
