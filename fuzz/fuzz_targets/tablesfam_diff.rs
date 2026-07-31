#![no_main]
//! Differential: p1-lanef tables batch (common/keywords,
//! common/unicode_category) shipped Rust vs vendored PostgreSQL 18.3 C —
//! see decoder_fuzz::tablesfam::tablesfam_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::tablesfam_diff(data);
});
