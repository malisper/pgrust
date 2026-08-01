#![no_main]
//! Differential: backend/lib/radixtree shipped Rust vs vendored PostgreSQL
//! 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C lib/radixtree.h template —
//! see decoder_fuzz::radixtree_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::radixtree_diff(data);
});
