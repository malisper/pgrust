#![no_main]
//! Differential: adt_date timestamp<->date/time/timetz conversions and
//! time/timetz +- interval arithmetic (shipped Rust) vs vendored PostgreSQL
//! 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C — see
//! decoder_fuzz::datetime_convert_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::datetime_convert_diff::datetime_convert_diff(data);
});
