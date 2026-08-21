//! The M3-A exit-slice suite (`lanev3-m3-chunks.md` §5 M3-A):
//! - [`layout`] — `size_of`/layout pins on every on-disk struct (issue-#69
//!   template) + geometry + rowid bit-budget pins.
//! - [`golden`] — golden encode/decode vectors (manifest, commit pointer,
//!   identity hashes, names, varlena headers) — independent hand-assembled
//!   bytes vs the encoders.
//! - [`roundtrip`] — the reference Verbatim/Const codec through the FULL
//!   six-face ABI, every storage class, nulls, selections, overflow.
//! - [`refusal`] — born-RED typed refusals: seeded unknown encoding ID, the
//!   reserved FSST ID, face refusals, corrupt/truncated structures.

mod golden;
mod layout;
mod norm;
mod refusal;
mod roundtrip;

pub(crate) use golden::sample_manifest as golden_sample_manifest;
