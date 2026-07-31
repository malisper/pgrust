#![no_main]
//! Differential: p1-lanec string-family batch (common/string, common/archive,
//! common/percentrepl, common/relpath, common/wait_error) shipped Rust vs
//! vendored PostgreSQL 18.3 C — see decoder_fuzz::strfam::strfam_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::strfam_diff(data);
});
