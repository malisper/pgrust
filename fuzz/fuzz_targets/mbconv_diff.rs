#![no_main]
//! Differential: mb/conv encoding conversions (all 84 pg_proc directions,
//! oids 4302..=4387) shipped Rust vs the vendored PostgreSQL 18.3 C the
//! proofs/mbconv Kani family solves against — see decoder_fuzz::mbconv_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::mbconv_diff(data);
});
