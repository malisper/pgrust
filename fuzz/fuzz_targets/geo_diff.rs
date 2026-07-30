#![no_main]
//! Differential: point_out image + on_ppath predicate, shipped Rust vs
//! vendored PostgreSQL C — see decoder_fuzz::diff::geo_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::geo_diff(data);
});
