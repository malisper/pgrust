//! `SimEntropy` — the deterministic sim-harness entropy stream (DST P2
//! contract §2.1; compiled ONLY under the non-default `--cfg pgrust_sim`,
//! never into product builds — law 0.1).
//!
//! Master seed: `PGRUST_SIM_SEED` (R-KNOBS row, owner dst/p2-rng) — u64, hex
//! (`0x…`) or decimal; default 0; unparsable falls back to the default (the
//! sim-knob convention, mirroring PGRUST_SIM_CLOCK_MODE's unparsable→frozen).
//! Read once through a OnceLock; the parse fn is pure with a unit corpus.
//!
//! Each fill draws from an independent splitmix64 stream keyed by
//! `(master_seed, fill_counter)`. The counter is a process-wide AtomicU64,
//! so a serial corpus observes one deterministic fill order; the per-backend
//! prng seed (miscinit/process.rs) is one such fill, which makes every
//! `pg_prng` consumer downstream same-seed deterministic for free.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use crate::EntropySource;

/// ZST; sim builds monomorphize `ActiveEntropy = SimEntropy`.
pub(crate) struct SimEntropy;

impl SimEntropy {
    pub(crate) const fn new() -> Self {
        SimEntropy
    }
}

/// Process-wide fill counter: fill N draws the stream keyed by
/// `(master_seed, N)`. Relaxed suffices — the counter carries no ordering
/// obligation beyond uniqueness; determinism of the SEQUENCE comes from the
/// serial-corpus discipline (gate §3.2 runs a single connection).
static FILL_COUNTER: AtomicU64 = AtomicU64::new(0);

static MASTER_SEED: OnceLock<u64> = OnceLock::new();

fn master_seed() -> u64 {
    *MASTER_SEED.get_or_init(|| {
        // The ONLY reader of PGRUST_SIM_SEED, cfg'd out of product builds
        // (R-KNOBS: no product reader exists).
        std::env::var("PGRUST_SIM_SEED")
            .ok()
            .and_then(|v| parse_sim_seed(&v))
            .unwrap_or(0)
    })
}

/// Pure parse fn (R-KNOBS idiom): `0x…`/`0X…` hex or decimal u64, tolerant
/// of surrounding whitespace. `None` on empty/malformed/overflow — the
/// reader falls back to the default seed 0.
fn parse_sim_seed(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16).ok()
    } else {
        s.parse::<u64>().ok()
    }
}

/// splitmix64 (Steele/Lea/Flood, JPDC 2014) — the stream generator. Pure;
/// designed to be seeded with arbitrary (including sequential) values, so
/// the raw `(seed, fill_no)` key needs no extra pre-mixing round.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministically fill `buf` from the stream keyed by `(seed, fill_no)`.
/// Pure — the entire sim entropy behavior, testable without env or process
/// state. Key collisions across distinct `(seed, fill_no)` pairs are
/// possible in principle (XOR fold) and irrelevant: the skeleton's contract
/// is same-seed determinism, not stream independence.
fn fill_stream(seed: u64, fill_no: u64, buf: &mut [u8]) {
    let mut state = seed ^ fill_no.wrapping_mul(0xA076_1D64_78BD_642F);
    for chunk in buf.chunks_mut(8) {
        let v = splitmix64(&mut state).to_le_bytes();
        chunk.copy_from_slice(&v[..chunk.len()]);
    }
}

impl EntropySource for SimEntropy {
    #[inline]
    fn fill(&self, buf: &mut [u8]) -> bool {
        let n = FILL_COUNTER.fetch_add(1, Ordering::Relaxed);
        fill_stream(master_seed(), n, buf);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse-fn unit corpus (R-KNOBS registration law) ---

    #[test]
    fn parse_corpus() {
        // good
        assert_eq!(parse_sim_seed("0x5EED"), Some(0x5EED));
        assert_eq!(parse_sim_seed("0X10"), Some(16));
        assert_eq!(parse_sim_seed("123"), Some(123));
        assert_eq!(parse_sim_seed("0"), Some(0));
        assert_eq!(parse_sim_seed(" 42 "), Some(42));
        assert_eq!(parse_sim_seed("18446744073709551615"), Some(u64::MAX));
        assert_eq!(
            parse_sim_seed("0xFFFFFFFFFFFFFFFF"),
            Some(u64::MAX)
        );
        // empty
        assert_eq!(parse_sim_seed(""), None);
        assert_eq!(parse_sim_seed("   "), None);
        // bad hex / bad decimal
        assert_eq!(parse_sim_seed("0x"), None);
        assert_eq!(parse_sim_seed("0xZZ"), None);
        assert_eq!(parse_sim_seed("zz"), None);
        assert_eq!(parse_sim_seed("-1"), None);
        assert_eq!(parse_sim_seed("12 34"), None);
        // overflow
        assert_eq!(parse_sim_seed("18446744073709551616"), None);
        assert_eq!(parse_sim_seed("0x10000000000000000"), None);
    }

    // --- determinism battery (contract §2.1 conformance) ---

    #[test]
    fn same_seed_identical_streams() {
        // Two process-simulated sequences: same seed, same fill order =>
        // identical byte streams, fill by fill.
        let seed = 0x5EED;
        for fill_no in 0..8u64 {
            let mut a = [0u8; 37];
            let mut b = [0u8; 37];
            fill_stream(seed, fill_no, &mut a);
            fill_stream(seed, fill_no, &mut b);
            assert_eq!(a, b, "fill {fill_no} diverged under one seed");
        }
    }

    #[test]
    fn distinct_seeds_distinct_streams() {
        let mut a = [0u8; 64];
        let mut b = [0u8; 64];
        fill_stream(1, 0, &mut a);
        fill_stream(2, 0, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn distinct_fills_distinct_streams() {
        let mut a = [0u8; 64];
        let mut b = [0u8; 64];
        fill_stream(0x5EED, 0, &mut a);
        fill_stream(0x5EED, 1, &mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn odd_tail_covered() {
        // Non-multiple-of-8 buffers: the tail chunk is filled, and a
        // same-key refill reproduces it exactly.
        let mut a = [0u8; 13];
        let mut b = [0u8; 13];
        fill_stream(7, 3, &mut a);
        fill_stream(7, 3, &mut b);
        assert_eq!(a, b);
        assert_ne!(a, [0u8; 13]);
    }

    #[test]
    fn trait_fill_advances_counter() {
        // Global-state assertions kept minimal (the counter is shared with
        // any concurrently running test): two fills through the trait must
        // succeed and differ (distinct fill numbers => distinct streams).
        let src = SimEntropy::new();
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        assert!(src.fill(&mut a));
        assert!(src.fill(&mut b));
        assert_ne!(a, b);
    }
}
