#![no_main]
//! Differential: adt/bool shipped Rust — cores AND fc_* fmgr wrappers,
//! incl. the aggregate state machine — vs vendored PostgreSQL 18.3 C
//! (csrc/pg_bool.c) — see decoder_fuzz::diff_charbool::bool_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::bool_diff(data);
});
