#![no_main]
//! Differential: nodes/readfuncs + nodes/outfuncs + nodes/copyfuncs shipped
//! Rust vs vendored PostgreSQL 18.6 (REL_18_6, upstream sha 724edf9bde) C —
//! see decoder_fuzz::nodesfam_diff (lane p1-nodes). ONE node-universe fixture
//! drives all three crates: read -> out -> copy -> out -> re-read, with the
//! out->read round trip as a self-checking oracle alongside the C differential.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    decoder_fuzz::nodesfam_diff::fuzz_entry(data);
});
