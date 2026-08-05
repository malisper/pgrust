#![no_main]
//! Differential: adt/pseudotypes shipped Rust (cores + fc_ wrappers) vs
//! vendored PostgreSQL C (Stamp 18.3) — see decoder_fuzz::pseudotypes_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::pseudotypes_diff(data);
});
