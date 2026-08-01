#![no_main]
//! Differential: contrib/seg + contrib/cube shipped Rust vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C incl. the
//! generated flex/bison parsers — see decoder_fuzz::contribb_diff
//! (lane p1-mb-contribb).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::contribb_diff(data);
});
