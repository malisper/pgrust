//! The stand-in `ColumnMetaBuilder` (spec §19.7).
//!
//! M3-E owns the real metadata plane (zone keys, SMA/PSMA, blooms, NDV,
//! byte+char text stats). This lane DRIVES the frozen builder ABI at seal —
//! per the M3-D charter, with a stand-in where the slice allows — so the
//! stand-in emits records that are CORRECT but minimal:
//!
//! - `nonnull` is exact (it is one leg of the two-witness null law, spec
//!   §6.6 — the seal path cross-checks it against the emitted validity
//!   bitmap's popcount, and the born-RED test seeds a skew through this same
//!   ABI);
//! - every zone key is `KeyKind::Absent` (never a wrong key — Absent means
//!   "validity + counts only", spec §8.1);
//! - sortedness `Unknown`, all other aggregates zero, no aux sections.
//!
//! When M3-E's builders land they replace this through the identical trait;
//! the seal driver does not change.

use pgrc2_format::abi::{ColumnMetaBuilder, EncodeInput};
use pgrc2_format::geom::GRANULES_PER_BAND;
use pgrc2_format::meta::StatsRecord;
use pgrc2_format::part::SectionKind;

/// Minimal, correct stand-in: exact nonnull at all three grains.
#[derive(Debug, Default)]
pub struct StandinMetaBuilder {
    /// Per-granule nonnull, indexed by part granule ordinal.
    granule_nonnull: Vec<u32>,
}

impl StandinMetaBuilder {
    pub fn new() -> StandinMetaBuilder {
        StandinMetaBuilder::default()
    }

    fn record(nonnull: u32) -> StatsRecord {
        StatsRecord {
            nonnull,
            ..StatsRecord::absent()
        }
    }
}

impl ColumnMetaBuilder for StandinMetaBuilder {
    fn observe_granule(&mut self, input: &EncodeInput<'_>, granule: u32) {
        let mut nonnull = 0u32;
        for r in 0..input.rows {
            if input.valid(r) {
                nonnull += 1;
            }
        }
        let g = granule as usize;
        if self.granule_nonnull.len() <= g {
            self.granule_nonnull.resize(g + 1, 0);
        }
        self.granule_nonnull[g] = nonnull;
    }

    fn seal_granule(&mut self, granule: u32) -> StatsRecord {
        let n = self
            .granule_nonnull
            .get(granule as usize)
            .copied()
            .unwrap_or(0);
        StandinMetaBuilder::record(n)
    }

    fn seal_band(&mut self, band: u32) -> StatsRecord {
        let start = (band * GRANULES_PER_BAND) as usize;
        let end = (start + GRANULES_PER_BAND as usize).min(self.granule_nonnull.len());
        let n: u32 = self.granule_nonnull[start.min(end)..end].iter().sum();
        StandinMetaBuilder::record(n)
    }

    fn seal_part(&mut self) -> StatsRecord {
        let n: u32 = self.granule_nonnull.iter().sum();
        StandinMetaBuilder::record(n)
    }

    fn aux_sections(&mut self) -> Vec<(SectionKind, Vec<u8>)> {
        Vec::new()
    }
}
