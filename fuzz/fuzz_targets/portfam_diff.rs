#![no_main]
//! Differential: the p1-microbatch PORTFAM bucket (pg_bitutils, crc32c,
//! pgstrcasecmp, pg_path, bufmask) shipped Rust vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C — see decoder_fuzz::portfam_diff
//! for the arm map, comparison planes, and domain carves.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::portfam_diff(data);
});
