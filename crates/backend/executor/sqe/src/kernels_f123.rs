//! Extraction from the harness kernels_f123.rs (port-study/port-map.md
//! §3.11): part_stats. The render lawlets that lived here (`date_str`,
//! `avg_exact`) moved to `render` — the ONE render seam.

use crate::bank::Bank;
use pgrc2_format::meta::{StatsRecord, STATSF_COMPUTED};

/// The §8.1 Stats section PART-grain record for (attno, path 0) of a part —
/// the record after granule_count + band_count granule/band records.
pub fn part_stats(bank: &Bank, pi: usize, attno: u32) -> Option<StatsRecord> {
    let body = crate::scan::stats_body(bank, pi, attno)?;
    let m = &bank.manifest.parts[pi];
    let idx = (m.granule_count + m.band_count) as usize;
    crate::scan::stats_record(&body, idx).filter(|r| r.flags & STATSF_COMPUTED != 0)
}
