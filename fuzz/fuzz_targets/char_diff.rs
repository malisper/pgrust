#![no_main]
//! Differential: adt/char ("char" type) shipped Rust — cores AND fc_* fmgr
//! wrappers — vs vendored PostgreSQL 18.3 C (csrc/pg_char.c) — see
//! decoder_fuzz::diff_charbool::char_diff.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::char_diff(data);
});
