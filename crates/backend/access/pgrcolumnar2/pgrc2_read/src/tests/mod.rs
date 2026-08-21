//! M3-F reader gates (chunk table §5 M3-F, verbatim):
//!
//! - [`open_faults`] — O(streams-touched) born-RED: opening one column of a
//!   wide shredded part faults only that column's streams (fault-counter
//!   witness, both teeth), + the per-face fault-policy pins.
//! - [`refusal`] — per-section CRC refusal on EVERY section kind, structural
//!   truncation/corruption battery, typed encoding/wrapper/expect refusals,
//!   and the seeded byte-flip no-panic battery (the M3-K fuzzer's PR-local
//!   tooth).
//! - [`cache`] — part cache: equal identity ⇒ identical bytes, pin + LRU
//!   janitor under budget pressure, no eviction of pinned parts.
//! - [`dict`] — lazy ensure-frame/ensure-code, generation-stable payload
//!   region pin (the StrView zero-copy dependency), varlena-shaped entry
//!   presentation pin (StrView §7b — this crate's chartered pinning test).
//! - [`decode_props`] — decode_sel ≡ decode_full ∘ select THROUGH the
//!   dispatch layer, full-decode correctness across classes/encodings/nulls/
//!   overflow/multi-extent/child streams, validity + meta faces.
//! - [`manifest`] — effective-generation walk: clog fence, absent-CURRENT
//!   emptiness, typed chain refusals, expectation facts.
//! - [`pins`] — `size_of` pins on the structs that price engagement.
//!
//! Loom models for the shared segment-map/registry/dict states live in
//! `tests/loom.rs` (separate build cone, `--cfg loom`).

mod cache;
mod decode_props;
mod dict;
mod manifest;
mod open_faults;
mod pins;
mod refusal;

use std::sync::Arc;

use crate::openpart::{OpenPart, PartExpect};
use crate::testpart::BuiltPart;

/// Open a built part over MemPartIo with the given synthetic identity.
pub(crate) fn open_built(b: &BuiltPart, dev: u64, ino: u64) -> Arc<OpenPart> {
    Arc::new(
        OpenPart::open(Box::new(b.mem_io(dev, ino)), &PartExpect::none()).expect("open built part"),
    )
}

/// An 8-aligned decode arena backing buffer.
pub(crate) struct ArenaBuf {
    words: Vec<u64>,
}

impl ArenaBuf {
    pub(crate) fn new(bytes: usize) -> ArenaBuf {
        ArenaBuf {
            words: vec![0u64; bytes.div_ceil(8)],
        }
    }

    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: u64 → u8 reinterpret of an exclusively borrowed buffer.
        unsafe {
            core::slice::from_raw_parts_mut(
                self.words.as_mut_ptr() as *mut u8,
                self.words.len() * 8,
            )
        }
    }
}

/// Deterministic xorshift64* (seeded batteries — no `rand`, no clocks).
pub(crate) struct XorShift(pub u64);

impl XorShift {
    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}
