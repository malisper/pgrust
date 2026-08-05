#![no_main]
//! Differential: adt_date extract_date / time_part_common(retnumeric) /
//! timetz_part_common / date skip-support (shipped Rust) vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C, plus the owed
//! adt_date builtins.rs fc-wrapper plane (wrapper vs C-verified core) — see
//! decoder_fuzz::datetime_closeout_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::datetime_closeout_diff::datetime_closeout_diff(data);
});
