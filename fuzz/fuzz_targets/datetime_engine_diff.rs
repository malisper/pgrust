#![no_main]
//! Differential: adt_datetime timestamp-image encoder + ISO week/year
//! calendar helpers (shipped Rust) vs vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C — see decoder_fuzz::datetime_engine_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::datetime_engine_diff::datetime_engine_diff(data);
});
