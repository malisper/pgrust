#![no_main]
//! Differential: adt_geo text I/O (point/box/lseg/line/path/poly/circle
//! in+out) shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream
//! sha 62d6c7d3df) C — see decoder_fuzz::geo_io_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::geo_io_diff(data);
});
