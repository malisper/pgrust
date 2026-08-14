#![no_main]
//! Differential: shipped Rust `adt_varbit::bits_in` (bit_in / varbit_in) vs
//! verbatim vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! — see decoder_fuzz::varbit_io_diff. The hand-rolled `B`/`X`-prefix text
//! parser on an attacker-controlled cstring (the tid/ltree ST3/Q8 bug class).
//! Any accept/reject, value-image, or errcode-class mismatch panics, so a
//! libFuzzer crash artifact here is a C/Rust divergence reproducer; a pgrust
//! panic/OOB where C cleanly rejects is a HIGH-severity crash finding.
//!
//! Lane PARSER-INVENTORY: the module has shipped a full comparator +
//! adversarial bank + detection-power control since the VENDOR lane; this
//! shell wires it into the continuous libFuzzer rig (it was compiled and
//! cargo-tested but never exposed as a runnable target).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::varbit_io_diff(data);
});
