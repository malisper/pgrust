#![no_main]
//! Differential: float4in/float8in shipped Rust vs vendored PostgreSQL C
//! (strtod parse cascade) — see decoder_fuzz::diff::float_in_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::float_in_diff(data);
});
