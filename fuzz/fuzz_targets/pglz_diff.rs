#![no_main]
//! Differential: shipped Rust `pglz::pglz_decompress` vs vendored PostgreSQL
//! 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C `pglz_decompress` — the TOAST
//! decompression OOB / decompression-bomb surface. See decoder_fuzz::pglz_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::pglz_diff(data);
});
