//! ST-2: the columnar ANALYZE fold — pg_statistic-shaped output folded
//! from seal-built facts (footer StatsRecords + the Stats sidecar
//! sketches), NO sampling reads (OD-2: the fold covers the pg_statistic
//! families; the sampling path stays designed-but-idle with no consumer).
//!
//! Exactness classes, stated per family (the ST-1 law):
//! - `stanullfrac` / `stawidth`: EXACT (footer part records).
//! - MCV: per-part counts EXACT; the cross-part fold sums counts by value
//!   — a value absent from one part's top-k contributes its OTHER parts'
//!   counts only, so folded frequencies are LOWER BOUNDS (exact for
//!   single-part tables and for values in every part's top-k).
//! - histogram bounds: per-part EXACT equi-depth; the fold merges bound
//!   multisets in byte order and re-thins — approximate across parts,
//!   exact for one part.
//! - `stadistinct`: the caller supplies the NDV estimate (the footer HLL
//!   max-union — 0.6%-off class); this fold never re-derives it.
//! - `stacorrelation` (P-8): cluster-key columns fold to ±1.0 from the
//!   FT-6 witness; otherwise derived from the P-1 sortedness bytes
//!   (all-Ascending ⇒ 1.0, all-Descending ⇒ -1.0, else 0.0). Never
//!   default-0 on data the format KNOWS is clustered.
//!
//! The never-analyzed refusal law (ST-2): banked data always has these
//! inputs (stats are a seal byproduct), so a consumer holding a sealed
//! part can ALWAYS fold — the refusal path is unreachable on banked data
//! by construction, which is exactly what `M3.estimate-probe.<rung>`
//! witnesses (with a born-RED stats-stripped seed).

use crate::format::meta::{Sortedness, StatsRecord};
use crate::format::sidecar::{ColDistribution, STATS_HIST_BOUNDS};

/// One part's fold inputs for one column.
pub struct PartColInput<'a> {
    /// The part-grain StatsRecord (footer Stats section, part row).
    pub part_record: &'a StatsRecord,
    /// Rows in the part (footer fact).
    pub rows: u64,
    /// The column's Stats-sidecar sketch, if the profile computed one.
    pub sketch: Option<&'a ColDistribution>,
}

/// pg_statistic-shaped output for one column (the R1-class seam currency;
/// values are canonical bytes — the AM layer renders them into datums).
#[derive(Debug, Clone, PartialEq)]
pub struct PgStatColumn {
    pub stanullfrac: f64,
    /// Average value width in bytes (pg_statistic stawidth).
    pub stawidth: f64,
    /// Distinct estimate — POSITIVE absolute count (callers may convert to
    /// the pg_statistic negative-fraction form).
    pub stadistinct: f64,
    /// (canonical value bytes, frequency), MCV form.
    pub sta_mcv: Vec<(Vec<u8>, f64)>,
    /// Histogram bound values (canonical bytes, ascending).
    pub sta_histogram: Vec<Vec<u8>>,
    pub stacorrelation: f64,
}

/// Fold one column across its parts. `ndv_estimate` is the footer-HLL
/// union (never re-derived here); `cluster_declared` is the FT-6 witness.
pub fn fold_pg_statistic(
    parts: &[PartColInput<'_>],
    ndv_estimate: f64,
    cluster_declared: bool,
) -> Option<PgStatColumn> {
    if parts.is_empty() {
        return None;
    }
    let total_rows: u64 = parts.iter().map(|p| p.rows).sum();
    let total_nonnull: u64 = parts.iter().map(|p| p.part_record.nonnull as u64).sum();
    if total_rows == 0 {
        return None;
    }
    let stanullfrac = 1.0 - (total_nonnull as f64 / total_rows as f64);
    let byte_sum: u64 = parts.iter().map(|p| p.part_record.byte_len_sum).sum();
    let stawidth = if total_nonnull > 0 && byte_sum > 0 {
        byte_sum as f64 / total_nonnull as f64
    } else {
        // Word classes carry no byte_len stats; the record's key width
        // class is the caller's to render. 0.0 = "not supplied here".
        0.0
    };

    // MCV: sum exact counts by value across parts (lower-bound law above).
    let mut merged: std::collections::BTreeMap<&[u8], u64> = std::collections::BTreeMap::new();
    let mut any_sketch = false;
    for p in parts {
        if let Some(s) = p.sketch {
            any_sketch = true;
            for (v, c) in &s.mcv {
                *merged.entry(v.as_slice()).or_insert(0) += c;
            }
        }
    }
    let mut by_count: Vec<(&[u8], u64)> = merged.into_iter().collect();
    by_count.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let sta_mcv: Vec<(Vec<u8>, f64)> = by_count
        .iter()
        .take(crate::format::sidecar::STATS_MCV_K)
        .map(|(v, c)| (v.to_vec(), *c as f64 / total_rows as f64))
        .collect();

    // Histogram: merge per-part bound multisets in byte order, re-thin to
    // the bound budget.
    let mut bounds: Vec<&[u8]> = parts
        .iter()
        .filter_map(|p| p.sketch)
        .flat_map(|s| s.hist_bounds.iter().map(|b| b.as_slice()))
        .collect();
    bounds.sort();
    bounds.dedup();
    let sta_histogram: Vec<Vec<u8>> = if bounds.len() <= STATS_HIST_BOUNDS {
        bounds.iter().map(|b| b.to_vec()).collect()
    } else {
        let n = bounds.len();
        (0..STATS_HIST_BOUNDS)
            .map(|i| bounds[(i * (n - 1)) / (STATS_HIST_BOUNDS - 1)].to_vec())
            .collect()
    };

    // A column with no sketch anywhere (opaque/nondeterministic profiles)
    // still folds the exact families — value lists stay empty, honestly.
    let _ = any_sketch;

    // P-8 correlation.
    let stacorrelation = if cluster_declared {
        let desc = parts
            .iter()
            .all(|p| p.part_record.sortedness == Sortedness::Descending.as_u8());
        if desc {
            -1.0
        } else {
            1.0
        }
    } else {
        let all = |s: Sortedness| {
            parts
                .iter()
                .all(|p| p.part_record.sortedness == s.as_u8())
        };
        if all(Sortedness::Ascending) || all(Sortedness::Constant) {
            1.0
        } else if all(Sortedness::Descending) {
            -1.0
        } else {
            0.0
        }
    };

    Some(PgStatColumn {
        stanullfrac,
        stawidth,
        stadistinct: ndv_estimate,
        sta_mcv,
        sta_histogram,
        stacorrelation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::sidecar::ColDistribution;

    fn rec(nonnull: u32, sortedness: Sortedness, byte_len_sum: u64) -> StatsRecord {
        let mut r = StatsRecord::absent();
        r.nonnull = nonnull;
        r.sortedness = sortedness.as_u8();
        r.byte_len_sum = byte_len_sum;
        r
    }

    #[test]
    fn single_part_fold_is_exact() {
        let r = rec(90, Sortedness::Ascending, 450);
        let sketch = ColDistribution {
            nonnull: 90,
            ndv_eligible: 3,
            long_values: 0,
            mcv: vec![(b"a".to_vec(), 60), (b"b".to_vec(), 20), (b"c".to_vec(), 10)],
            hist_bounds: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        };
        let out = fold_pg_statistic(
            &[PartColInput {
                part_record: &r,
                rows: 100,
                sketch: Some(&sketch),
            }],
            3.0,
            false,
        )
        .expect("fold");
        assert!((out.stanullfrac - 0.1).abs() < 1e-9);
        assert!((out.stawidth - 5.0).abs() < 1e-9);
        assert_eq!(out.stadistinct, 3.0);
        assert_eq!(out.sta_mcv[0], (b"a".to_vec(), 0.6));
        assert_eq!(out.sta_histogram.len(), 3);
        assert_eq!(out.stacorrelation, 1.0, "P-8: ascending folds to 1.0");
    }

    #[test]
    fn cluster_witness_forces_unit_correlation() {
        let r = rec(10, Sortedness::Unknown, 0);
        let out = fold_pg_statistic(
            &[PartColInput {
                part_record: &r,
                rows: 10,
                sketch: None,
            }],
            10.0,
            true,
        )
        .expect("fold");
        assert_eq!(out.stacorrelation, 1.0, "FT-6 witness dominates");
        assert!(out.sta_mcv.is_empty(), "no sketch = empty lists, honest");
    }

    #[test]
    fn multi_part_mcv_counts_sum() {
        let r1 = rec(50, Sortedness::Unknown, 100);
        let r2 = rec(50, Sortedness::Unknown, 100);
        let s1 = ColDistribution {
            nonnull: 50,
            ndv_eligible: 2,
            long_values: 0,
            mcv: vec![(b"x".to_vec(), 30), (b"y".to_vec(), 20)],
            hist_bounds: vec![b"x".to_vec(), b"y".to_vec()],
        };
        let s2 = ColDistribution {
            nonnull: 50,
            ndv_eligible: 2,
            long_values: 0,
            mcv: vec![(b"x".to_vec(), 40), (b"z".to_vec(), 10)],
            hist_bounds: vec![b"x".to_vec(), b"z".to_vec()],
        };
        let out = fold_pg_statistic(
            &[
                PartColInput {
                    part_record: &r1,
                    rows: 50,
                    sketch: Some(&s1),
                },
                PartColInput {
                    part_record: &r2,
                    rows: 50,
                    sketch: Some(&s2),
                },
            ],
            3.0,
            false,
        )
        .expect("fold");
        assert_eq!(out.sta_mcv[0], (b"x".to_vec(), 0.7), "counts sum by value");
        assert_eq!(out.stacorrelation, 0.0);
    }
}
