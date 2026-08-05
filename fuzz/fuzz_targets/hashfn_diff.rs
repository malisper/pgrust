#![no_main]
//! Differential: hashfn shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! — see decoder_fuzz::hashfn_diff. Scaffolded by fuzz/scaffold.py.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::hashfn_diff(data);
});
