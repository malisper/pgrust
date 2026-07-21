//! Step-1 runtime cost model — cost-derived engagement floors
//! (scratchpad/night/runtime-cost-model-design.md §5 step 1).
//!
//! Replaces the M5-5 FloorGuard RECTANGLES (m5_suppress::class_guard) with
//! per-class crossover CURVES fit by least squares from the EXISTING m5-5
//! ladder cells (notes/m5-5-floors.md: jobs -6b53/-0591/-4e08/-5020/-4082/
//! -6632 @ 2159563ff, dop4 x 100k..5M, + -0831/-7237/-3aa5 @ 37decba75,
//! dop8/16 @ 5M + dop16 @ 2.5M; fast-profile, medians of 5). The fit script
//! of record is scripts/runtime-cost-fit.py (deterministic, seeded); the
//! constants table of record is crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv,
//! pinned against this module by `constants_match_tsv` below (the
//! bootstrap_matrix_matches_tsv precedent). No naked numbers: every value
//! here carries measurement sha + date + jobs in the TSV.
//!
//! MODEL (ratio-normalized — legacy per-row work == 1 unit; verdicts are
//! ratio comparisons, so the absolute anchor cancels; step 2 rebases both
//! sides to cost units via the t34 anchor without moving any verdict):
//!
//! ```text
//!   t_rt(N, D)  = c_engage + w_row * N / D     runtime: measured
//!                                              near-linear for D <= 16
//!   t_leg(N, D) = l_setup + N / min(D, l_cap)  legacy Gather saturates
//!                                              at l_cap workers
//!   predicted_ratio(N, D) = t_rt / t_leg       (rt/legacy, the ladder's
//!                                              own unit)
//!   suppress Gather  iff  ratio <= 1.0  &&  N >= n_min_fit
//! ```
//!
//! `n_min_fit` is the smallest ENGAGED ladder row count for the class:
//! below the measured support the curve is extrapolation, and the verdict
//! fails toward the incumbent (Gather stands) — the same fail-closed
//! posture the rectangles had, now with provenance.
//!
//! What this module does NOT model (each with a named owner):
//! - CbGroupedAggTextKey: rectangle retained (non-monotonic N profile —
//!   the legacy partial-agg step at 5M is a G-axis effect; GL-COST-1 owes
//!   the G-annotated ladder). m5_suppress keeps its FloorGuard.
//! - HeapPlainCountStar `min_pages 8192`: an ADMISSION MIRROR of the
//!   rowdrive 64MB block floor (m5-5 reading #3), not economics — the
//!   caller must keep applying it in every mode.
//! - groupby_high HOLD (4e6): classify-time input, retired at step 2.
//! - CbMetaFooterAgg: footer answers are O(1) — never floored, no curve.
//!
//! Routing mode — `PGRUST_M5_COST_ROUTE` (design §migration):
//!   unset / "shadow"  SHADOW (default): curve computed + traced next to
//!                     the floor verdict; FLOORS DECIDE. Zero behavior
//!                     change; the fleet verdict-diff report reads the
//!                     `m5-cost-route:` trace lines.
//!   "0" / "off"       fully off: no curve evaluation, no trace.
//!   "1" / "all"       curves decide every curve-modeled class (rectangle
//!                     classes keep floors).
//!   "ClassA,ClassB"   curves decide the named classes only (the per-class
//!                     flip vehicle; others stay shadow).

/// The curve-modeled classes. Mirrors the m5_suppress::CoverClass names
/// (the planner maps CoverClass -> RuntimeClass, with the documented
/// reuse: CbHashJoinMultiBuild/CbHashJoinGroupedAgg ride
/// CbHashJoinPlainAgg's curve, AggPolyHeapPlain rides HeapCmpFoldPrefix's
/// — both PROVISIONAL reuses matching the shipped guard reuse).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuntimeClass {
    CbPlainAggFold,
    CbGroupedAggIntKeys,
    CbGroupedAggTopN,
    CbDistinctIntKeys,
    CbTopnBoundedIntKeys,
    CbHashJoinPlainAgg,
    HeapPlainCountStar,
    HeapCmpFoldPrefix,
}

impl RuntimeClass {
    pub fn name(self) -> &'static str {
        match self {
            RuntimeClass::CbPlainAggFold => "CbPlainAggFold",
            RuntimeClass::CbGroupedAggIntKeys => "CbGroupedAggIntKeys",
            RuntimeClass::CbGroupedAggTopN => "CbGroupedAggTopN",
            RuntimeClass::CbDistinctIntKeys => "CbDistinctIntKeys",
            RuntimeClass::CbTopnBoundedIntKeys => "CbTopnBoundedIntKeys",
            RuntimeClass::CbHashJoinPlainAgg => "CbHashJoinPlainAgg",
            RuntimeClass::HeapPlainCountStar => "HeapPlainCountStar",
            RuntimeClass::HeapCmpFoldPrefix => "HeapCmpFoldPrefix",
        }
    }

    pub const ALL: [RuntimeClass; 8] = [
        RuntimeClass::CbPlainAggFold,
        RuntimeClass::CbGroupedAggIntKeys,
        RuntimeClass::CbGroupedAggTopN,
        RuntimeClass::CbDistinctIntKeys,
        RuntimeClass::CbTopnBoundedIntKeys,
        RuntimeClass::CbHashJoinPlainAgg,
        RuntimeClass::HeapPlainCountStar,
        RuntimeClass::HeapCmpFoldPrefix,
    ];
}

/// Fitted per-class constants (units: legacy-row-equivalents; see module
/// doc). Values of record live in crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv.
#[derive(Clone, Copy, Debug)]
pub struct ClassModel {
    pub c_engage: f64,
    pub w_row: f64,
    pub l_setup: f64,
    pub l_cap: f64,
    pub n_min_fit: f64,
}

pub fn class_model(class: RuntimeClass) -> ClassModel {
    // GENERATED by scripts/runtime-cost-fit.py — edit the ladder cells /
    // rerun the fit, never hand-tune (the TSV drift test pins this block).
    match class {
        RuntimeClass::CbDistinctIntKeys => ClassModel { c_engage: 0.0, w_row: 1.1915, l_setup: 5930.2, l_cap: 11.56, n_min_fit: 1000000.0 },
        RuntimeClass::CbGroupedAggIntKeys => ClassModel { c_engage: 1619590.2, w_row: 0.0000, l_setup: 1971350.9, l_cap: 3.73, n_min_fit: 1000000.0 },
        RuntimeClass::CbGroupedAggTopN => ClassModel { c_engage: 552019.0, w_row: 0.0000, l_setup: 384767.4, l_cap: 4.55, n_min_fit: 1000000.0 },
        RuntimeClass::CbHashJoinPlainAgg => ClassModel { c_engage: 137140.0, w_row: 2.1783, l_setup: 481535.5, l_cap: 16.00, n_min_fit: 1000000.0 },
        RuntimeClass::CbPlainAggFold => ClassModel { c_engage: 664933.1, w_row: 2.3909, l_setup: 1471291.5, l_cap: 16.00, n_min_fit: 1000000.0 },
        RuntimeClass::CbTopnBoundedIntKeys => ClassModel { c_engage: 0.0, w_row: 8.1956, l_setup: 90700.6, l_cap: 12.51, n_min_fit: 1000000.0 },
        RuntimeClass::HeapCmpFoldPrefix => ClassModel { c_engage: 12948.8, w_row: 1.2270, l_setup: 8510.4, l_cap: 9.47, n_min_fit: 100000.0 },
        RuntimeClass::HeapPlainCountStar => ClassModel { c_engage: 272535.1, w_row: 0.2965, l_setup: 600901.3, l_cap: 6.71, n_min_fit: 2500000.0 },
    }
}

/// HeapPlainCountStar's rowdrive 64MB block-floor ADMISSION MIRROR
/// (m5-5 reading #3): applied by the caller in EVERY cost-route mode —
/// suppressing below the block geometry lands on a refusing arm and a
/// losing serial fallback. Never retired by the curve.
pub const HEAP_COUNT_ADMISSION_MIN_PAGES: f64 = 8192.0;

/// Predicted rt/legacy ratio for a class at (est rows, engaged dop).
pub fn predicted_ratio(class: RuntimeClass, rows: f64, dop: i32) -> f64 {
    let m = class_model(class);
    let d = (dop.max(1)) as f64;
    let t_rt = m.c_engage + m.w_row * rows / d;
    let t_leg = m.l_setup + rows / d.min(m.l_cap);
    t_rt / t_leg
}

/// The curve verdict at (rows, dop).
#[derive(Clone, Copy, Debug)]
pub struct CostVerdict {
    pub ratio: f64,
    pub suppress: bool,
}

pub fn cost_route_verdict(class: RuntimeClass, rows: f64, dop: i32) -> CostVerdict {
    let ratio = predicted_ratio(class, rows, dop);
    let m = class_model(class);
    CostVerdict {
        ratio,
        // Fail toward the incumbent: predicted win AND inside measured
        // support. (Ties keep Gather; parity suppression remains the
        // floors'/flip-gates' call, never the curve's.)
        suppress: ratio <= 1.0 && rows >= m.n_min_fit,
    }
}

/// `PGRUST_M5_COST_ROUTE` (memoized; see module doc).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CostRouteMode {
    Off,
    Shadow,
    DecideAll,
    DecideClasses(Vec<&'static str>),
}

pub fn cost_route_mode() -> &'static CostRouteMode {
    static MODE: std::sync::OnceLock<CostRouteMode> = std::sync::OnceLock::new();
    MODE.get_or_init(|| match std::env::var("PGRUST_M5_COST_ROUTE").as_deref() {
        Err(_) | Ok("") | Ok("shadow") => CostRouteMode::Shadow,
        Ok("0") | Ok("off") => CostRouteMode::Off,
        Ok("1") | Ok("all") => CostRouteMode::DecideAll,
        Ok(list) => {
            let mut classes = Vec::new();
            for name in list.split(',') {
                let name = name.trim();
                if let Some(c) = RuntimeClass::ALL.iter().find(|c| c.name() == name) {
                    classes.push(c.name());
                }
                // Unknown names are ignored (fail toward shadow), so a typo
                // can never widen routing.
            }
            if classes.is_empty() { CostRouteMode::Shadow } else { CostRouteMode::DecideClasses(classes) }
        }
    })
}

/// Does the curve DECIDE for this class (vs shadow-trace only)?
pub fn cost_route_decides(class: RuntimeClass) -> bool {
    match cost_route_mode() {
        CostRouteMode::Off | CostRouteMode::Shadow => false,
        CostRouteMode::DecideAll => true,
        CostRouteMode::DecideClasses(v) => v.contains(&class.name()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The engaged ladder cells (class, rows, dop, measured rt/legacy) —
    // the SAME table scripts/runtime-cost-fit.py fits from (provenance in
    // the module doc). Refused (sub-granule) cells are excluded: they
    // measure fallback parity, not arm economics.
    const CELLS: &[(RuntimeClass, f64, i32, f64)] = &[
        (RuntimeClass::CbPlainAggFold, 1e6, 4, 0.34),
        (RuntimeClass::CbPlainAggFold, 2.5e6, 4, 1.26),
        (RuntimeClass::CbPlainAggFold, 5e6, 4, 1.21),
        (RuntimeClass::CbPlainAggFold, 5e6, 8, 1.10),
        (RuntimeClass::CbPlainAggFold, 5e6, 16, 1.04),
        (RuntimeClass::CbPlainAggFold, 2.5e6, 16, 0.89),
        (RuntimeClass::HeapPlainCountStar, 2.5e6, 4, 0.37),
        (RuntimeClass::HeapPlainCountStar, 5e6, 4, 0.35),
        (RuntimeClass::HeapPlainCountStar, 5e6, 8, 0.34),
        (RuntimeClass::HeapPlainCountStar, 5e6, 16, 0.27),
        (RuntimeClass::HeapPlainCountStar, 2.5e6, 16, 0.33),
        (RuntimeClass::HeapCmpFoldPrefix, 1e5, 4, 1.25),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e5, 4, 1.39),
        (RuntimeClass::HeapCmpFoldPrefix, 5e5, 4, 1.25),
        (RuntimeClass::HeapCmpFoldPrefix, 1e6, 4, 1.23),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e6, 4, 1.25),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 4, 1.25),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 8, 1.13),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 16, 0.76),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e6, 16, 0.73),
        (RuntimeClass::CbGroupedAggIntKeys, 1e6, 4, 0.76),
        (RuntimeClass::CbGroupedAggIntKeys, 2.5e6, 4, 0.60),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 4, 0.49),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 8, 0.47),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 16, 0.53),
        (RuntimeClass::CbGroupedAggIntKeys, 2.5e6, 16, 0.57),
        (RuntimeClass::CbGroupedAggTopN, 1e6, 4, 0.85),
        (RuntimeClass::CbGroupedAggTopN, 2.5e6, 4, 0.55),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 4, 0.34),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 8, 0.36),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 16, 0.37),
        (RuntimeClass::CbGroupedAggTopN, 2.5e6, 16, 0.62),
        (RuntimeClass::CbDistinctIntKeys, 1e6, 4, 1.24),
        (RuntimeClass::CbDistinctIntKeys, 2.5e6, 4, 1.21),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 4, 1.21),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 8, 1.06),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 16, 0.90),
        (RuntimeClass::CbDistinctIntKeys, 2.5e6, 16, 0.79),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 4, 6.28),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 4, 7.18),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 4, 8.00),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 8, 6.55),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 16, 5.33),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 16, 4.30),
        (RuntimeClass::CbHashJoinPlainAgg, 1e6, 4, 0.41),
        (RuntimeClass::CbHashJoinPlainAgg, 2.5e6, 4, 1.48),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 4, 1.50),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 8, 1.39),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 16, 1.14),
        (RuntimeClass::CbHashJoinPlainAgg, 2.5e6, 16, 0.92),
    ];

    /// The SHIPPED FloorGuard rectangles for the curve classes, replicated
    /// here as a deliberate cross-crate pin of m5_suppress::class_guard
    /// (planner depends on costsize, so this module cannot import it; the
    /// planner-side test `cost_route_map_is_total` pins the mapping).
    fn floor_suppresses(class: RuntimeClass, rows: f64, dop: i32) -> bool {
        match class {
            RuntimeClass::CbPlainAggFold => dop >= 12 || rows <= 1_500_000.0,
            RuntimeClass::CbGroupedAggIntKeys => true,
            RuntimeClass::CbGroupedAggTopN => true,
            RuntimeClass::CbDistinctIntKeys => dop >= 12,
            RuntimeClass::CbTopnBoundedIntKeys => false, // max_rows: 0
            RuntimeClass::CbHashJoinPlainAgg => rows <= 2_000_000.0,
            RuntimeClass::HeapPlainCountStar => true, // pages mirror aside
            RuntimeClass::HeapCmpFoldPrefix => rows >= 1_000_000.0 && dop >= 12,
        }
    }

    /// EQUIVALENCE ASSERTION (design §migration-2): at every measured
    /// ladder cell outside the M5-5 ±5% parity band, the curve verdict
    /// must match the MEASUREMENT — suppress where the runtime clearly won
    /// (<0.95), keep Gather where it clearly lost (>1.05).
    #[test]
    fn curve_verdicts_match_measurements_at_ladder_cells() {
        for &(class, rows, dop, meas) in CELLS {
            let v = cost_route_verdict(class, rows, dop);
            if meas < 0.95 {
                assert!(
                    v.suppress,
                    "{class:?} N={rows} D={dop}: measured win {meas} but curve keeps \
                     Gather (r_pred={:.3})",
                    v.ratio
                );
            } else if meas > 1.05 {
                assert!(
                    !v.suppress,
                    "{class:?} N={rows} D={dop}: measured loss {meas} but curve \
                     suppresses (r_pred={:.3})",
                    v.ratio
                );
            }
        }
    }

    /// The curve-vs-floor disagreement set at measured cells is EXACTLY the
    /// named forgone win the design expects the curves to recover
    /// (hashjoin dop16@2.5M, measured 0.92 — the rectangle's clean-2M-bound
    /// sacrifice) plus nothing. Any new disagreement is a red test — it
    /// must arrive with a ladder cell + a flip-gate letter, not by drift.
    /// (Parity-band cells are exempt: suppressed-at-parity vs
    /// kept-at-parity are both inside the shipped acceptance bar.)
    #[test]
    fn curve_vs_floor_disagreements_are_exactly_the_named_forgone_wins() {
        let mut disagreements = Vec::new();
        for &(class, rows, dop, meas) in CELLS {
            if (0.95..=1.05).contains(&meas) {
                continue;
            }
            let cost = cost_route_verdict(class, rows, dop).suppress;
            let floor = floor_suppresses(class, rows, dop);
            if cost != floor {
                disagreements.push((class, rows as i64, dop));
            }
        }
        assert_eq!(
            disagreements,
            vec![(RuntimeClass::CbHashJoinPlainAgg, 2_500_000, 16)],
            "unexpected curve-vs-floor disagreement set"
        );
    }

    /// Crossover roots reproduce the shipped floor boundaries within their
    /// measurement brackets (design §2 acceptance: each FloorGuard value is
    /// a root of the crossover equation, interpolated between ladder
    /// points).
    #[test]
    fn crossover_roots_land_in_the_floor_brackets() {
        let root_n = |class, dop| {
            let mut n = 1e4f64;
            let mut prev = predicted_ratio(class, n, dop) - 1.0;
            while n < 2e7 {
                n *= 1.05;
                let cur = predicted_ratio(class, n, dop) - 1.0;
                if prev * cur <= 0.0 {
                    return Some(n);
                }
                prev = cur;
            }
            None
        };
        let root_d = |class, rows: f64| {
            (2..=16).find(|&d| predicted_ratio(class, rows, d) <= 1.0)
        };
        // low_dop_max_rows 1.5M was interpolated in (1M, 2.5M):
        let n = root_n(RuntimeClass::CbPlainAggFold, 4).unwrap();
        assert!((1e6..2.5e6).contains(&n), "CbPlainAggFold N*(dop4)={n}");
        // max_rows 2M was interpolated in (1M, 2.5M):
        let n = root_n(RuntimeClass::CbHashJoinPlainAgg, 4).unwrap();
        assert!((1e6..2.5e6).contains(&n), "CbHashJoinPlainAgg N*(dop4)={n}");
        // min_dop 12 was interpolated in (8, 16]:
        let d = root_d(RuntimeClass::HeapCmpFoldPrefix, 5e6).unwrap();
        assert!((9..=16).contains(&d), "HeapCmpFoldPrefix D*(5M)={d}");
        let d = root_d(RuntimeClass::CbDistinctIntKeys, 5e6).unwrap();
        assert!((9..=16).contains(&d), "CbDistinctIntKeys D*(5M)={d}");
        // topn: honestly never wins anywhere in the measured range — the
        // curve IS the retired max_rows=0 hack.
        for &(class, rows, dop, _) in CELLS {
            if class == RuntimeClass::CbTopnBoundedIntKeys {
                assert!(!cost_route_verdict(class, rows, dop).suppress);
            }
        }
    }

    /// Below measured support the verdict fails toward the incumbent
    /// (Gather stands), whatever the curve extrapolates to. The structural
    /// tiny-query invariant (design §4.1) rides this: no curve class can
    /// suppress a fixture-sized shape.
    #[test]
    fn below_fit_support_keeps_gather() {
        for &class in RuntimeClass::ALL.iter() {
            for rows in [100.0, 10_000.0, 90_000.0] {
                for dop in [2, 4, 16] {
                    assert!(
                        !cost_route_verdict(class, rows, dop).suppress,
                        "{class:?} suppressed below n_min_fit at rows={rows} dop={dop}"
                    );
                }
            }
        }
    }

    /// Constants of record: the TSV and this module must not drift apart
    /// (the bootstrap_matrix_matches_tsv precedent).
    #[test]
    fn constants_match_tsv() {
        let tsv = include_str!("../../../../../../crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv");
        let mut seen = std::collections::BTreeSet::new();
        for line in tsv.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            if cols[0] == "class" {
                continue;
            }
            assert_eq!(cols.len(), 10, "malformed TSV row: {line}");
            let (class_name, term, value) = (cols[0], cols[1], cols[2]);
            let Some(&class) = RuntimeClass::ALL.iter().find(|c| c.name() == class_name)
            else {
                continue; // structural rows (rectangle/admission/reuse/hold)
            };
            let m = class_model(class);
            let expect = match term {
                "c_engage" => m.c_engage,
                "w_row" => m.w_row,
                "l_setup" => m.l_setup,
                "l_cap" => m.l_cap,
                "n_min_fit" => m.n_min_fit,
                "admission_min_pages" => HEAP_COUNT_ADMISSION_MIN_PAGES,
                _ => continue, // structural terms owned by other pins
            };
            let got: f64 = value.parse().unwrap();
            assert_eq!(got, expect, "{class_name}.{term}: TSV {got} != code {expect}");
            if term != "admission_min_pages" {
                seen.insert((class_name.to_string(), term.to_string()));
            }
        }
        // Every curve class carries all five terms in the TSV.
        assert_eq!(
            seen.len(),
            RuntimeClass::ALL.len() * 5,
            "TSV curve rows incomplete: {seen:?}"
        );
    }

    /// Mode knob: unset is SHADOW (floors decide), and no class decides
    /// unless explicitly flipped — the default-inert guarantee.
    #[test]
    fn cost_route_default_is_shadow() {
        assert_eq!(*cost_route_mode(), CostRouteMode::Shadow);
        for &class in RuntimeClass::ALL.iter() {
            assert!(!cost_route_decides(class));
        }
    }
}
