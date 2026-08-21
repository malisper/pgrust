//! XC-5: the parallel-safe meta-engagement census (PC-6.1/PC-6.2).
//!
//! A plain per-worker FOLD struct — never a shared hot-path atomic
//! (PC-6.1); workers fold at drain and the leader sums. The BLOOM counter
//! ships in the SAME landing that populates bloom sections under the
//! OD-11 arming (PC-6.2: "a bloom section with no census counter is the
//! §4.2 built-not-engaging RED state") — so the M3 A/B (q19 clustered
//! needle + QA uncorrelated needle) has a witness to read, and FT-10's
//! decline trigger has numbers instead of darkness. PSMA rides the same
//! vocabulary (the v3 `psma-windows-skipped` census line's v4 home).
//!
//! Serial==parallel census IDENTITY (PC-6.3) holds by construction: the
//! counters are pure functions of (probe, sealed metadata), independent of
//! which worker ran the probe or in what order folds happen (u64 sums).

use crate::format::meta::{StatsRecord, Verdict};
use crate::profile::MetaProfile;
use crate::verdict::{evaluate, BloomEvidence, GrainFacts, ZonePredicate};

/// The per-worker engagement counters (fold currency).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetaEngagement {
    /// Equality probes that CONSULTED a bloom block.
    pub bloom_probes: u64,
    /// Probes where the bloom alone proved definite absence (the verdict
    /// without bloom evidence would NOT have been AllFail) — the
    /// engagement number the OD-11 A/B switches.
    pub bloom_definite_absent: u64,
    /// Equality probes that consulted a PSMA block.
    pub psma_probes: u64,
    /// PSMA probes whose candidate row window was NARROWER than the whole
    /// granule (the v3 "psma windows skipped" class).
    pub psma_windows_narrowed: u64,
}

impl MetaEngagement {
    /// Fold another worker's counters in (drain-time; PC-6.1).
    pub fn fold(&mut self, other: &MetaEngagement) {
        self.bloom_probes += other.bloom_probes;
        self.bloom_definite_absent += other.bloom_definite_absent;
        self.psma_probes += other.psma_probes;
        self.psma_windows_narrowed += other.psma_windows_narrowed;
    }

    /// One census line (the machine-readable form census consumers grep).
    pub fn line(&self) -> String {
        format!(
            "pgrc2_meta_census bloom_probes={} bloom_definite_absent={} \
             psma_probes={} psma_windows_narrowed={}",
            self.bloom_probes,
            self.bloom_definite_absent,
            self.psma_probes,
            self.psma_windows_narrowed
        )
    }
}

/// The CENSUSED verdict face — [`evaluate`] with engagement attribution.
/// Attribution is exact: when bloom evidence is supplied the probe counts,
/// and `bloom_definite_absent` increments only when the bloom CHANGED the
/// verdict to AllFail (the no-bloom verdict is re-derived on that arm —
/// probe-side cost, never scan-side; zone-key AllFails never inflate the
/// bloom's number).
pub fn evaluate_censused(
    profile: &MetaProfile,
    facts: GrainFacts,
    rec: &StatsRecord,
    probe: &ZonePredicate<'_>,
    bloom: Option<BloomEvidence<'_>>,
    census: &mut MetaEngagement,
) -> Verdict {
    let consulted = bloom.is_some();
    let v = evaluate(profile, facts, rec, probe, bloom);
    if consulted {
        census.bloom_probes += 1;
        if v == Verdict::AllFail {
            let without = evaluate(profile, facts, rec, probe, None);
            if without != Verdict::AllFail {
                census.bloom_definite_absent += 1;
            }
        }
    }
    v
}

/// The CENSUSED PSMA equality face: [`crate::psma::psma_candidates_eq`]
/// with engagement attribution (`rows_in_granule` bounds the "whole
/// granule" comparison for the narrowing witness).
pub fn psma_candidates_eq_censused(
    block: &[u8],
    min_key: i64,
    max_key: i64,
    probe_key: i64,
    rows_in_granule: u32,
    census: &mut MetaEngagement,
) -> Option<(u16, u16)> {
    let r = crate::psma::psma_candidates_eq(block, min_key, max_key, probe_key);
    if let Some((lo, hi)) = r {
        census.psma_probes += 1;
        // `max_row` is EXCLUSIVE (psma.rs module doc; `PsmaAcc::observe`
        // stores `row16 + 1`), so the window is `hi - lo` — the previous
        // `+ 1` inclusive reading over-counted every window by one row: a
        // window covering rows_in_granule-1 rows read as the full granule
        // (never counted narrowed), and the (0,0) proven-absent sentinel
        // read as a 1-row window through the wrong arithmetic. Fixed at
        // M3-L3 (found wiring the scan consult); the born-RED boundary
        // case is pinned below.
        let window = (hi as u32) - (lo as u32);
        if window < rows_in_granule {
            census.psma_windows_narrowed += 1;
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_sums_and_line_shape() {
        let mut a = MetaEngagement {
            bloom_probes: 2,
            bloom_definite_absent: 1,
            psma_probes: 3,
            psma_windows_narrowed: 2,
        };
        let b = MetaEngagement {
            bloom_probes: 5,
            bloom_definite_absent: 4,
            psma_probes: 1,
            psma_windows_narrowed: 0,
        };
        a.fold(&b);
        assert_eq!(a.bloom_probes, 7);
        assert_eq!(a.bloom_definite_absent, 5);
        assert!(a.line().contains("bloom_definite_absent=5"));
    }

    /// The exclusive-bound pin (the M3-L3 fix): a window of
    /// `rows_in_granule - 1` rows IS narrowed (the inclusive misreading
    /// counted it as full-granule), a full-granule window is NOT, and the
    /// proven-absent sentinel counts as narrowed through the same
    /// arithmetic.
    #[test]
    fn psma_narrowing_witness_uses_exclusive_bounds() {
        use crate::format::meta::PSMA_BLOCK_LEN;
        let rows = 100u32;
        // min 0, max 255 → shift 0 → entry index = key value.
        let mut block = vec![0u8; PSMA_BLOCK_LEN];
        let set = |b: &mut [u8], idx: usize, lo: u16, hi: u16| {
            b[idx * 4..idx * 4 + 2].copy_from_slice(&lo.to_le_bytes());
            b[idx * 4 + 2..idx * 4 + 4].copy_from_slice(&hi.to_le_bytes());
        };
        set(&mut block, 5, 0, 99); // 99 rows (max_row EXCLUSIVE) of 100
        set(&mut block, 6, 0, 100); // the full granule
        let mut c = MetaEngagement::default();
        let w = psma_candidates_eq_censused(&block, 0, 255, 5, rows, &mut c);
        assert_eq!(w, Some((0, 99)));
        assert_eq!(c.psma_windows_narrowed, 1, "99-of-100 IS narrowed");
        let w = psma_candidates_eq_censused(&block, 0, 255, 6, rows, &mut c);
        assert_eq!(w, Some((0, 100)));
        assert_eq!(c.psma_windows_narrowed, 1, "full granule is NOT narrowed");
        // Proven absent (probe outside [min,max]) → the (0,0) sentinel.
        let w = psma_candidates_eq_censused(&block, 0, 255, 300, rows, &mut c);
        assert_eq!(w, Some((0, 0)));
        assert_eq!(c.psma_windows_narrowed, 2, "empty window counts narrowed");
        assert_eq!(c.psma_probes, 3);
    }
}
