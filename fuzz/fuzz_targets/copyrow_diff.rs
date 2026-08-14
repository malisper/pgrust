#![no_main]
//! Differential: shipped Rust COPY line/row framing (copy_cmd::bench_internals
//! copy_read_line / copy_read_line_text) vs verbatim vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C CopyReadLine / CopyReadLineText —
//! see decoder_fuzz::copyrow_diff. The hand-rolled COPY row splitter: a state
//! machine over an attacker-controlled stream (CSV quote/escape, CR/LF/CRLF
//! EOL-style latching, the `\.` end-of-copy marker, embedded-NUL rejection).
//! Any accept/reject, line-image, EOF-verdict, or errcode-class mismatch
//! panics; a pgrust panic/OOB where C handles the stream cleanly is HIGH.
//!
//! Lane PARSER-INVENTORY: wires the ready-but-unexposed copyrow_diff module.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::copyrow_diff::copyrow_diff(data);
});
