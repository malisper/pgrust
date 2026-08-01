#![no_main]
//! Differential: shipped heaptuple/tupdesc (+ types_tuple deform) vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C — see
//! decoder_fuzz::tupaccess_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::tupaccess_diff(data);
});
