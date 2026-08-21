//! The ONE hash family: bloom bit selection, NDV register hashing, and
//! predicate fingerprints all route through here — one definition, one
//! drift surface, golden-pinned in the test suite.
//!
//! Extracted verbatim from `pgrc2_meta::hash` (ruling 2026-08-08; see the
//! crate provenance header). Bloom/NDV bytes are part bytes, but the family
//! is NOT frozen format vocabulary (spec §8.3 freezes the header, and the
//! sections are self-describing) — a change to the family is a builder
//! change, caught by the golden pins, priced as a bank-recipe version bump
//! (O-10), not a format break.
//!
//! Construction: splitmix64-finalizer chains (the same primitive family the
//! format crate uses for identity, with DIFFERENT seeds and a local
//! definition — `format::ident` documents itself as not a general-purpose
//! hash surface, so nothing here borrows it). Both lanes fold the length
//! first (so zero-padding of the tail word cannot alias lengths), then each
//! 8-byte LE word.

/// splitmix64 finalizer (local copy — see module doc).
#[inline]
pub const fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One chain step.
#[inline]
pub const fn fold64(acc: u64, word: u64) -> u64 {
    mix64(acc ^ word.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Lane seeds (ASCII "pgrc2mh1" / "pgrc2mh2").
const LANE1_SEED: u64 = u64::from_le_bytes(*b"pgrc2mh1");
const LANE2_SEED: u64 = u64::from_le_bytes(*b"pgrc2mh2");

fn lane(seed: u64, bytes: &[u8]) -> u64 {
    let mut acc = fold64(seed, bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for c in &mut chunks {
        acc = fold64(acc, u64::from_le_bytes(c.try_into().expect("len 8")));
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut tail = [0u8; 8];
        tail[..rem.len()].copy_from_slice(rem);
        acc = fold64(acc, u64::from_le_bytes(tail));
    }
    mix64(acc)
}

/// The two-lane 128-bit hash of a canonical value image (spec §18.1 bytes).
/// Lane 1 seeds NDV register hashing; (lane 1, lane 2) drive bloom double
/// hashing (Kirsch–Mitzenmacher).
pub fn meta_hash128(bytes: &[u8]) -> (u64, u64) {
    (lane(LANE1_SEED, bytes), lane(LANE2_SEED, bytes))
}

/// The NDV lane by itself.
#[inline]
pub fn ndv_hash(bytes: &[u8]) -> u64 {
    lane(LANE1_SEED, bytes)
}

/// An incremental fold chain for structured fingerprints (the predicate
/// fingerprint in `pgrc2_meta::pcache`): absorb tagged words; finish with
/// [`mix64`]. Golden-pinned through `predicate_fingerprint`'s pins.
#[derive(Debug, Clone, Copy)]
pub struct FoldChain(u64);

impl FoldChain {
    pub fn new(seed: u64) -> FoldChain {
        FoldChain(mix64(seed))
    }
    pub fn word(&mut self, w: u64) {
        self.0 = fold64(self.0, w);
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.word(lane(LANE2_SEED, b));
    }
    pub fn finish(self) -> u64 {
        mix64(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{meta_hash128, FoldChain};

    /// Golden vectors carried verbatim from `pgrc2_meta`'s pin suite (they
    /// travel with the functions): a drift here is a part-byte drift.
    #[test]
    fn hash_family_golden_vectors() {
        assert_eq!(
            meta_hash128(b""),
            (0x22af_6b82_c16f_c5d8, 0xd32b_d8b3_fdcd_e7be)
        );
        assert_eq!(
            meta_hash128(b"pgrc2"),
            (0x8491_3bbc_e706_7257, 0xc828_bbc2_9cb2_3b94)
        );
        assert_eq!(
            meta_hash128(b"hello meta plane"),
            (0x25fb_f673_0c93_65f5, 0x4903_0cd2_b4aa_3441)
        );
        // Length is folded before content: a single zero byte is not the
        // empty string.
        assert_eq!(
            meta_hash128(&[0u8]),
            (0xb69c_9127_5573_441b, 0x86c7_899e_136f_0bae)
        );
    }

    #[test]
    fn fold_chain_golden_vectors() {
        // FoldChain over ("pgrc2pfp" seed): the fingerprint substrate.
        let mut c = FoldChain::new(u64::from_le_bytes(*b"pgrc2pfp"));
        c.word(1);
        c.word(8);
        assert_eq!(c.finish(), 0x15b5_df2e_7d16_b100);
    }
}
