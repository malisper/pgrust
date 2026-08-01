#![no_main]
//! Differential: nodes/readfuncs + nodes/outfuncs + nodes/copyfuncs shipped
//! Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C —
//! see decoder_fuzz::nodesfam_diff (lane p1-nodes). ONE node-universe fixture
//! drives all three crates: read -> out -> copy -> out -> re-read, with the
//! out->read round trip as a self-checking oracle alongside the C differential.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::nodesfam_diff::fuzz_entry(data);
});
