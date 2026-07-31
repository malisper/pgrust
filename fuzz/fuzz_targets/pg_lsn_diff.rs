#![no_main]
//! Differential: pg_lsn family (in/out/cmp/recv/send/mi/pli/mii/
//! numeric_pg_lsn) shipped Rust vs vendored PostgreSQL 18.3 C — see
//! decoder_fuzz::lsn_diff::pg_lsn_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::pg_lsn_diff(data);
});
