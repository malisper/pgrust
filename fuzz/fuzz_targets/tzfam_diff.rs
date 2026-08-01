#![no_main]
//! Differential: strftime/tzparser/ts_locale shipped Rust vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C — see
//! decoder_fuzz::tzfam_diff (lane p1-mb-tzfam).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::tzfam_diff(data);
});
