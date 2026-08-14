#![no_main]
//! Differential: shipped Rust COPY text field parse (copy_cmd::bench_internals,
//! CopyReadAttributesText) vs verbatim vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C — see decoder_fuzz::copyframe_diff. The COPY
//! text de-escape + delimiter split (the Q8-F1 memory-safety surface). Any
//! accept/reject, field-image, or errcode-class mismatch panics; a pgrust
//! panic/OOB where C cleanly rejects a malformed field is HIGH.
//!
//! Lane PARSER-INVENTORY: wires the ready-but-unexposed copyframe text arm.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::copyframe_diff::copyframe_text_diff(data);
});
