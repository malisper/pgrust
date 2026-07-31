#![no_main]
//! Differential: adt/numeric shipped Rust vs vendored PostgreSQL 18.3 C
//! (Stamp-18.3, upstream sha 62d6c7d3df, whole numeric.c) — value-domain
//! ops family. See decoder_fuzz::numericfam.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::numeric_ops_diff(data);
});
