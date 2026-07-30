#![no_main]
//! Differential: float4out/float8out shipped Rust vs vendored PostgreSQL C
//! (Ryu shortest-decimal image) — see decoder_fuzz::diff::float_out_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::float_out_diff(data);
});
