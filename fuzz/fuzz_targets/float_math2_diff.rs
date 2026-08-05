#![no_main]
//! Differential: two-argument float8 math family (datan2/datan2d/dpow)
//! shipped Rust vs vendored PostgreSQL C — see
//! decoder_fuzz::diff::float_math2_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::float_math2_diff(data);
});
