#![no_main]
//! Differential: float.c remainder — rounding/sqrt/degrees unary family,
//! extra_float_digits <= 0 output arm (pg_strfromd %.*g), float4/8
//! recv/send wire images — shipped Rust vs vendored PostgreSQL C. See
//! decoder_fuzz::diff::float_misc_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::float_misc_diff(data);
});
