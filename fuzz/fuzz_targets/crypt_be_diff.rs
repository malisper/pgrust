#![no_main]
//! Differential: backend/libpq/crypt shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! — see decoder_fuzz::cryptbe_diff. Mirrors the tsm_system_* scaffold shape.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::crypt_be_diff(data);
});
