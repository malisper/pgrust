#![no_main]
//! Differential: shipped Rust COPY binary per-field framing
//! (copy_cmd::bench_internals, CopyReadBinaryData/CopyGetInt/
//! CopyReadBinaryAttribute) vs verbatim vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C — see decoder_fuzz::copyframe_diff. The binary
//! length-word surface: the `-1` NULL sentinel vs illegal negatives / oversize
//! length words (the COPY_FIELD_LEN bank). Any accept/reject, field-image, or
//! errcode-class mismatch panics; a pgrust panic/OOB where C cleanly rejects a
//! malformed length word is HIGH.
//!
//! Lane PARSER-INVENTORY: wires the ready-but-unexposed copyframe binary arm.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::copyframe_diff::copyframe_binary_diff(data);
});
