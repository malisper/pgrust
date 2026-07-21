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
//! CALIBRATION STATUS — WITNESSED v2 GRID (GL-COST-3, 2026-07-21): fit
//! from the M5ROWFLIP2 rows of scripts/m5-rowflip-measure-v2.sh (per-leg
//! engine pins, serial/legacy NEGATIVE witnesses, legacy Workers-Launched
//! witness, runtime dop-pinned engagement, fast-profile server) — jobs
//! pgrust-fast-tests-c517fec04e-…{-66c9,-0f09,-3ad6,-2686,-47ad,-58be,
//! -17f3,-11d5,-683a} @ night/cost-step1 c517fec04, replacing the
//! contaminated v1 record (hj-ladder-v2-and-seat-letter.md) wholesale.
//! The v2 record contradicts three shipped rectangles at witnessed cells
//! (see the disagreement pin below): those cells are the flip-gate
//! candidates, each needing its GL-COST-<class> letter before any
//! PGRUST_M5_COST_ROUTE flip. Heap-scan classes engaged at dop<DOP in
//! some cells (runtime witness dopN-MISMATCH — a scan-program item);
//! their ratios are honest "what suppression delivers" wall ratios.
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
//! - HeapPlainCountStar `min_pages 8192`: an ADMISSION MIRROR of the
//!   rowdrive 64MB block floor (m5-5 reading #3), not economics — the
//!   caller must keep applying it in every mode.
//! - groupby_high HOLD (4e6): classify-time input, retired at step 2.
//! - CbMetaFooterAgg: footer answers are O(1) — never floored, no curve.
//!
//! Routing mode — `PGRUST_M5_COST_ROUTE` (design §migration):
//!   unset             DEFAULT since t36 flips2 = the THREE WITNESSED rot
//!                     cells decide by curve:
//!                     `CbPlainAggFold,CbGroupedAggTextKey,CbHashJoinPlainAgg`
//!                     (GL-COST-SCANFOLD-1 / -TEXTKEY-1 / -HASHJOIN-1, all
//!                     FLIP-RECOMMENDED — notes/gl-cost-class-flip-letters.md,
//!                     2026-07-21: flip A/B jobs -4ff1/-296e/-0325 @
//!                     3f50bea20, every cell in the letters' expected
//!                     direction, hashjoin 1M@dop4 57->43ms loss avoided +
//!                     2.5M@dop16 forgone win recovered, agree-controls
//!                     flat; economics = the witnessed v2 ladder). The
//!                     multibuild/groupsink hashjoin riders follow the
//!                     PlainAgg curve exactly as they rode the rectangle.
//!                     Every other curve-modeled class stays SHADOW.
//!   "shadow"          the KILL for the default flip: curve computed +
//!                     traced next to the floor verdict; FLOORS DECIDE
//!                     everywhere (the pre-t36 posture).
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
    CbGroupedAggTextKey,
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
            RuntimeClass::CbGroupedAggTextKey => "CbGroupedAggTextKey",
            RuntimeClass::CbGroupedAggTopN => "CbGroupedAggTopN",
            RuntimeClass::CbDistinctIntKeys => "CbDistinctIntKeys",
            RuntimeClass::CbTopnBoundedIntKeys => "CbTopnBoundedIntKeys",
            RuntimeClass::CbHashJoinPlainAgg => "CbHashJoinPlainAgg",
            RuntimeClass::HeapPlainCountStar => "HeapPlainCountStar",
            RuntimeClass::HeapCmpFoldPrefix => "HeapCmpFoldPrefix",
        }
    }

    pub const ALL: [RuntimeClass; 9] = [
        RuntimeClass::CbPlainAggFold,
        RuntimeClass::CbGroupedAggIntKeys,
        RuntimeClass::CbGroupedAggTextKey,
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
        RuntimeClass::CbDistinctIntKeys => ClassModel { c_engage: 0.0, w_row: 1.1159, l_setup: 6127.2, l_cap: 11.52, n_min_fit: 1000000.0 },
        RuntimeClass::CbGroupedAggIntKeys => ClassModel { c_engage: 1292041.9, w_row: 1.8392, l_setup: 2689172.8, l_cap: 6.04, n_min_fit: 1000000.0 },
        RuntimeClass::CbGroupedAggTextKey => ClassModel { c_engage: 628935.8, w_row: 1.6932, l_setup: 647011.4, l_cap: 5.76, n_min_fit: 1000000.0 },
        RuntimeClass::CbGroupedAggTopN => ClassModel { c_engage: 461286.3, w_row: 1.6395, l_setup: 1903979.4, l_cap: 6.06, n_min_fit: 1000000.0 },
        RuntimeClass::CbHashJoinPlainAgg => ClassModel { c_engage: 1747.4, w_row: 1.5379, l_setup: 48931.8, l_cap: 11.79, n_min_fit: 1000000.0 },
        RuntimeClass::CbPlainAggFold => ClassModel { c_engage: 0.0, w_row: 1.2712, l_setup: 18206.0, l_cap: 12.66, n_min_fit: 1000000.0 },
        RuntimeClass::CbTopnBoundedIntKeys => ClassModel { c_engage: 439075.0, w_row: 10.4623, l_setup: 236241.6, l_cap: 11.81, n_min_fit: 1000000.0 },
        RuntimeClass::HeapCmpFoldPrefix => ClassModel { c_engage: 199319.8, w_row: 1.1983, l_setup: 164041.4, l_cap: 7.32, n_min_fit: 100000.0 },
        RuntimeClass::HeapPlainCountStar => ClassModel { c_engage: 257515.3, w_row: 0.3037, l_setup: 552913.9, l_cap: 6.88, n_min_fit: 2500000.0 },
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

/// The t36 flips2 default decide-list: the three witnessed rot cells
/// (notes/gl-cost-class-flip-letters.md — all FLIP-RECOMMENDED). Kept as a
/// named constant so the default and the letters stay reviewably tied.
const DEFAULT_DECIDE_CLASSES: [&str; 3] =
    ["CbPlainAggFold", "CbGroupedAggTextKey", "CbHashJoinPlainAgg"];

pub fn cost_route_mode() -> &'static CostRouteMode {
    static MODE: std::sync::OnceLock<CostRouteMode> = std::sync::OnceLock::new();
    MODE.get_or_init(|| match std::env::var("PGRUST_M5_COST_ROUTE").as_deref() {
        // DEFAULT since t36 flips2 (GL-COST class flip letters): the three
        // witnessed rot cells decide by curve; "shadow" is the kill.
        Err(_) | Ok("") => CostRouteMode::DecideClasses(DEFAULT_DECIDE_CLASSES.to_vec()),
        Ok("shadow") => CostRouteMode::Shadow,
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

    // The WITNESSED engaged ladder cells (class, rows, dop, measured
    // rt/legacy) — the SAME table scripts/runtime-cost-fit.py fits from
    // (M5ROWFLIP2 rows; provenance in the module doc CALIBRATION STATUS).
    // Cells whose runtime witness is `absent` (arm refused; fallback
    // parity) are excluded — refusal is coverage vocabulary, not
    // economics.
    const CELLS: &[(RuntimeClass, f64, i32, f64)] = &[
        (RuntimeClass::CbPlainAggFold, 1e6, 4, 1.258),
        (RuntimeClass::CbPlainAggFold, 2.5e6, 4, 1.279),
        (RuntimeClass::CbPlainAggFold, 5e6, 4, 1.235),
        (RuntimeClass::CbPlainAggFold, 5e6, 8, 1.143),
        (RuntimeClass::CbPlainAggFold, 5e6, 16, 1.021),
        (RuntimeClass::CbPlainAggFold, 2.5e6, 16, 0.865),
        (RuntimeClass::HeapPlainCountStar, 2.5e6, 4, 0.368),
        (RuntimeClass::HeapPlainCountStar, 5e6, 4, 0.362),
        (RuntimeClass::HeapPlainCountStar, 5e6, 8, 0.349),
        (RuntimeClass::HeapPlainCountStar, 5e6, 16, 0.271),
        (RuntimeClass::HeapPlainCountStar, 2.5e6, 16, 0.341),
        (RuntimeClass::HeapCmpFoldPrefix, 1e5, 4, 1.143),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e5, 4, 1.333),
        (RuntimeClass::HeapCmpFoldPrefix, 5e5, 4, 1.207),
        (RuntimeClass::HeapCmpFoldPrefix, 1e6, 4, 1.184),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e6, 4, 1.189),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 4, 1.201),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 8, 1.093),
        (RuntimeClass::HeapCmpFoldPrefix, 5e6, 16, 0.727),
        (RuntimeClass::HeapCmpFoldPrefix, 2.5e6, 16, 0.724),
        (RuntimeClass::CbGroupedAggIntKeys, 1e6, 4, 0.570),
        (RuntimeClass::CbGroupedAggIntKeys, 2.5e6, 4, 0.771),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 4, 0.898),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 8, 0.693),
        (RuntimeClass::CbGroupedAggIntKeys, 5e6, 16, 0.522),
        (RuntimeClass::CbGroupedAggIntKeys, 2.5e6, 16, 0.526),
        (RuntimeClass::CbGroupedAggTextKey, 1e6, 4, 1.130),
        (RuntimeClass::CbGroupedAggTextKey, 2.5e6, 4, 1.339),
        (RuntimeClass::CbGroupedAggTextKey, 5e6, 4, 1.460),
        (RuntimeClass::CbGroupedAggTextKey, 5e6, 8, 1.139),
        (RuntimeClass::CbGroupedAggTextKey, 5e6, 16, 0.714),
        (RuntimeClass::CbGroupedAggTextKey, 2.5e6, 16, 0.881),
        (RuntimeClass::CbGroupedAggTopN, 1e6, 4, 0.388),
        (RuntimeClass::CbGroupedAggTopN, 2.5e6, 4, 0.628),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 4, 0.773),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 8, 0.535),
        (RuntimeClass::CbGroupedAggTopN, 5e6, 16, 0.361),
        (RuntimeClass::CbGroupedAggTopN, 2.5e6, 16, 0.313),
        (RuntimeClass::CbDistinctIntKeys, 1e6, 4, 1.087),
        (RuntimeClass::CbDistinctIntKeys, 2.5e6, 4, 1.220),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 4, 1.055),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 8, 1.056),
        (RuntimeClass::CbDistinctIntKeys, 5e6, 16, 0.805),
        (RuntimeClass::CbDistinctIntKeys, 2.5e6, 16, 0.769),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 4, 6.350),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 4, 8.677),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 4, 9.204),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 8, 7.405),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 16, 5.643),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 16, 4.609),
        (RuntimeClass::CbHashJoinPlainAgg, 1e6, 4, 1.319),
        (RuntimeClass::CbHashJoinPlainAgg, 2.5e6, 4, 1.494),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 4, 1.545),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 8, 1.284),
        (RuntimeClass::CbHashJoinPlainAgg, 5e6, 16, 1.024),
        (RuntimeClass::CbHashJoinPlainAgg, 2.5e6, 16, 0.923),
    ];

    /// The SHIPPED FloorGuard rectangles for the curve classes, replicated
    /// here as a deliberate cross-crate pin of m5_suppress::class_guard
    /// (planner depends on costsize, so this module cannot import it; the
    /// planner-side test `cost_route_map_is_total` pins the mapping).
    fn floor_suppresses(class: RuntimeClass, rows: f64, dop: i32) -> bool {
        match class {
            RuntimeClass::CbPlainAggFold => dop >= 12 || rows <= 1_500_000.0,
            RuntimeClass::CbGroupedAggIntKeys => true,
            RuntimeClass::CbGroupedAggTextKey => dop >= 12 || rows <= 3_000_000.0,
            RuntimeClass::CbGroupedAggTopN => true,
            RuntimeClass::CbDistinctIntKeys => dop >= 12,
            // GL-SORTECON-3 re-flip (min_dop 4; the max_rows=0 hack retired
            // by the COLSTAGE+GCUT sort-arm rework, not by this model).
            RuntimeClass::CbTopnBoundedIntKeys => dop >= 4,
            RuntimeClass::CbHashJoinPlainAgg => rows <= 2_000_000.0,
            RuntimeClass::HeapPlainCountStar => true, // pages mirror aside
            RuntimeClass::HeapCmpFoldPrefix => rows >= 1_000_000.0 && dop >= 12,
        }
    }

    /// Classes whose WITNESSED v2 cells were measured on an arm that has
    /// since CHANGED ECONOMICS (the design §3 arm-change trigger, fired but
    /// not yet honored with a refit ladder):
    ///   CbTopnBoundedIntKeys — the v2 grid measured the PRE-COLSTAGE sort
    ///   arm (4.6-9.2x rt/legacy); GL-SORTECON-3 (COLSTAGE+GCUT) retired
    ///   that loss and re-flipped the floor to min_dop=4. The fitted curve
    ///   is therefore SUPERSEDED: it still reflects its cells, but the
    ///   cells no longer reflect the shipped arm. Shadow-only exposure (the
    ///   class is not in the decide list); refit owed — ladder spec L6,
    ///   TSV witness=witnessed-v2-superseded.
    const SUPERSEDED_CLASSES: &[RuntimeClass] = &[RuntimeClass::CbTopnBoundedIntKeys];

    fn superseded(class: RuntimeClass) -> bool {
        SUPERSEDED_CLASSES.contains(&class)
    }

    /// EQUIVALENCE ASSERTION (design §migration-2): at every measured
    /// ladder cell outside the M5-5 ±5% parity band, the curve verdict
    /// must match the MEASUREMENT — suppress where the runtime clearly won
    /// (<0.95), keep Gather where it clearly lost (>1.05).
    #[test]
    fn curve_verdicts_match_measurements_at_ladder_cells() {
        for &(class, rows, dop, meas) in CELLS {
            if superseded(class) {
                continue; // cells no longer describe the shipped arm (L6)
            }
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

    /// The curve-vs-floor disagreement set at WITNESSED cells is exactly
    /// the five cells where the v2 record contradicts the shipped
    /// rectangles — four v1-carved floor-rot cells that suppress at a
    /// measured loss (scan-fold 1M@dop4 1.258; text 1M/2.5M@dop4
    /// 1.130/1.339; hashjoin 1M@dop4 1.319) and the one forgone win the
    /// rectangle's clean-2M bound sacrificed (hashjoin 2.5M@dop16 0.923,
    /// now witnessed). These are the flip-gate candidates (GL-COST-<class>
    /// letters). Any new disagreement is a red test — it must arrive with
    /// a witnessed cell, not by drift. (Parity-band cells exempt.)
    #[test]
    fn curve_vs_floor_disagreements_are_exactly_the_witnessed_rot_cells() {
        let mut disagreements = Vec::new();
        for &(class, rows, dop, meas) in CELLS {
            if (0.95..=1.05).contains(&meas) || superseded(class) {
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
            vec![
                (RuntimeClass::CbPlainAggFold, 1_000_000, 4),
                (RuntimeClass::CbGroupedAggTextKey, 1_000_000, 4),
                (RuntimeClass::CbGroupedAggTextKey, 2_500_000, 4),
                (RuntimeClass::CbHashJoinPlainAgg, 1_000_000, 4),
                (RuntimeClass::CbHashJoinPlainAgg, 2_500_000, 16),
            ],
            "unexpected curve-vs-floor disagreement set"
        );
    }

    /// Crossover roots land inside the WITNESSED brackets (design §2
    /// acceptance): the dop-axis crossovers the min_dop-12 floors
    /// interpolated sit in (8, 16] for every class the v2 record shows
    /// flipping there; the dop4 axis has NO win region >= n_min_fit for
    /// the classes v2 shows losing at dop4 (the v1-carved size windows
    /// are gone — see the disagreement pin).
    #[test]
    fn crossover_roots_land_in_the_witnessed_brackets() {
        let root_d = |class, rows: f64| {
            (2..=16).find(|&d| predicted_ratio(class, rows, d) <= 1.0)
        };
        // min_dop 12 was interpolated in (8, 16] — v2 vindicates:
        let d = root_d(RuntimeClass::HeapCmpFoldPrefix, 5e6).unwrap();
        assert!((9..=16).contains(&d), "HeapCmpFoldPrefix D*(5M)={d}");
        let d = root_d(RuntimeClass::CbDistinctIntKeys, 5e6).unwrap();
        assert!((9..=16).contains(&d), "CbDistinctIntKeys D*(5M)={d}");
        let d = root_d(RuntimeClass::CbGroupedAggTextKey, 5e6).unwrap();
        assert!((9..=16).contains(&d), "CbGroupedAggTextKey D*(5M)={d}");
        let d = root_d(RuntimeClass::CbPlainAggFold, 5e6).unwrap();
        assert!((9..=16).contains(&d), "CbPlainAggFold D*(5M)={d}");
        // hashjoin: wins only at high dop and below ~5M (2.5M@16 witnessed
        // 0.923); at 5M@16 it is parity (1.024) — the curve must not
        // suppress there.
        let d = root_d(RuntimeClass::CbHashJoinPlainAgg, 2.5e6).unwrap();
        assert!((9..=16).contains(&d), "CbHashJoinPlainAgg D*(2.5M)={d}");
        assert!(!cost_route_verdict(RuntimeClass::CbHashJoinPlainAgg, 5e6, 16).suppress);
        // dop4: no win region at or above n_min_fit for the v2-losing
        // classes (the v1 size windows were contamination).
        for class in [
            RuntimeClass::CbPlainAggFold,
            RuntimeClass::CbGroupedAggTextKey,
            RuntimeClass::CbHashJoinPlainAgg,
            RuntimeClass::CbDistinctIntKeys,
        ] {
            for rows in [1e6, 1.5e6, 2e6, 2.5e6, 5e6, 1e7] {
                assert!(
                    !cost_route_verdict(class, rows, 4).suppress,
                    "{class:?} suppressed at dop4 rows={rows} against the witnessed record"
                );
            }
        }
        // topn: the SUPERSEDED curve (pre-COLSTAGE cells) never suppresses
        // anywhere in its measured range. Kept as a shadow-BEHAVIOR pin —
        // this is the census direction the class emits until the L6 refit
        // ladder lands post-GL-SORTECON-3 constants (the shipped floor now
        // suppresses at dop>=4, so every engaged cell censuses as
        // wl_suppress_model_gather by construction).
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
            assert_eq!(cols.len(), 11, "malformed TSV row: {line}");
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

    /// WITNESS CENSUS of record (charter: every cell carries witness
    /// status; UNWITNESSED cells are marked and unusable). Pins:
    /// (1) the witness vocabulary; (2) every fitted curve term is
    /// witnessed-v2 — an unwitnessed constant can never enter the fitted
    /// block by drift; (3) the unwitnessed-reuse set is EXACTLY the three
    /// named reuse rows (their own ladder cells are owed — specs in
    /// notes/runtime-cost-ladder-specs.md, owners GL-COST-2/GL-AGGPOLY-1);
    /// (4) no unwitnessed-reuse class name is in the default decide list
    /// (the riders decide only THROUGH their host curve's flip letters,
    /// matching the shipped guard reuse — an own-curve flip needs its own
    /// witnessed cells first).
    #[test]
    fn witness_census_is_pinned() {
        let tsv = include_str!("../../../../../../crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv");
        let mut unwitnessed = Vec::new();
        let mut debt = Vec::new();
        let mut refuted = Vec::new();
        for line in tsv.lines() {
            if line.starts_with('#') || line.trim().is_empty() || line.starts_with("class\t") {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 11, "malformed TSV row: {line}");
            let (class, term, witness) = (cols[0], cols[1], cols[10]);
            assert!(
                matches!(
                    witness,
                    "witnessed-v2" | "witnessed-v2-superseded" | "witnessed-ab"
                        | "unwitnessed-reuse" | "witnessed-refutes-reuse" | "structural"
                        | "unwitnessed-debt"
                ),
                "unknown witness status {witness:?} in row: {line}"
            );
            if witness == "unwitnessed-debt" {
                debt.push((class.to_string(), term.to_string()));
            }
            if witness == "witnessed-refutes-reuse" {
                refuted.push((class.to_string(), term.to_string()));
            }
            if let Some(&rc) = RuntimeClass::ALL.iter().find(|c| c.name() == class) {
                if matches!(term, "c_engage" | "w_row" | "l_setup" | "l_cap" | "n_min_fit") {
                    let want = if superseded(rc) {
                        // The arm changed under the fit (GL-SORTECON-3);
                        // cells still witnessed, no longer current — L6
                        // owns the refit.
                        "witnessed-v2-superseded"
                    } else {
                        "witnessed-v2"
                    };
                    assert_eq!(
                        witness, want,
                        "fitted curve term {class}.{term} witness status"
                    );
                }
            }
            if witness == "unwitnessed-reuse" {
                unwitnessed.push((class.to_string(), term.to_string()));
            }
        }
        assert_eq!(
            unwitnessed,
            vec![("AggPolyHeapPlain".to_string(), "curve_reuse".to_string())],
            "the unwitnessed-reuse census changed; a new unwitnessed cell must \
             arrive with its ladder spec (notes/runtime-cost-ladder-specs.md), \
             and a witnessed one must flip this pin + the TSV row together"
        );
        // The witnessed-REFUTED reuses (L1/L2 grids @ d10db8ef5e): both
        // hashjoin riders measured 3.0-6.4x rt/legacy against the host
        // curve's 0.73-1.43 predictions. The reuse stays WIRED because
        // unwiring changes routing (the riders are cost-decided through the
        // host at default) — that flip is GL-COST-2's letter, and this pin
        // flips with it, never by drift.
        assert_eq!(
            refuted,
            vec![
                ("CbHashJoinMultiBuild".to_string(), "curve_reuse".to_string()),
                ("CbHashJoinGroupedAgg".to_string(), "curve_reuse".to_string()),
            ],
            "the witnessed-refutes-reuse census changed"
        );
        for (class, _) in &unwitnessed {
            assert!(
                !DEFAULT_DECIDE_CLASSES.contains(&class.as_str()),
                "unwitnessed-reuse class {class} must not be in the default decide list"
            );
        }
        // The pinned-debt census: exactly the t36 seat-vs-curve overlap
        // cell (hashjoin 1M@dop4 — seat band witnessed >= 2.5M only, curve
        // witnessed a 1.32x loss). Retiring it requires the witnessed cell
        // (ladder spec L5) or a curve gate on the seat path — either way
        // this pin flips WITH the TSV row, never by drift.
        assert_eq!(
            debt,
            vec![("CbHashJoinPlainAgg".to_string(), "seat_overlap_cell".to_string())],
            "the unwitnessed-debt census changed"
        );
    }

    /// Model-shape sanity (charter: monotonicity unit tests): both cost
    /// curves strictly increase in N; the runtime side never gets more
    /// expensive with more workers; and widening dop 4 -> 16 never makes the
    /// predicted rt/legacy ratio WORSE anywhere in or above measured support
    /// (the fitted l_cap < 16 everywhere, so legacy saturates first).
    #[test]
    fn model_terms_are_monotone() {
        let grid = [1e5, 2.5e5, 5e5, 1e6, 2.5e6, 5e6, 1e7];
        for &class in RuntimeClass::ALL.iter() {
            let m = class_model(class);
            assert!(m.w_row > 0.0 && m.l_setup > 0.0 && m.c_engage >= 0.0, "{class:?}");
            assert!(m.l_cap < 16.0, "{class:?}: l_cap must saturate below dop16");
            for w in grid.windows(2) {
                for d in [1, 4, 8, 16] {
                    let t_rt = |n: f64| m.c_engage + m.w_row * n / d as f64;
                    let t_leg = |n: f64| m.l_setup + n / (d as f64).min(m.l_cap);
                    assert!(t_rt(w[1]) > t_rt(w[0]), "{class:?} t_rt not increasing in N");
                    assert!(t_leg(w[1]) > t_leg(w[0]), "{class:?} t_leg not increasing in N");
                }
            }
            for &n in &grid {
                // Runtime cost non-increasing in D.
                let rt = |d: i32| m.c_engage + m.w_row * n / d as f64;
                assert!(rt(16) <= rt(8) && rt(8) <= rt(4), "{class:?} t_rt increases in D");
                // Ratio no worse at dop16 than dop4.
                assert!(
                    predicted_ratio(class, n, 16) <= predicted_ratio(class, n, 4) + 1e-9,
                    "{class:?} N={n}: ratio worsens from dop4 to dop16"
                );
            }
        }
    }

    /// Mode knob posture of record at t36 flips2: unset = the THREE
    /// witnessed rot cells decide by curve (the GL-COST class flip
    /// letters' exact list — pinned name-for-name against RuntimeClass so
    /// a rename can never silently un-flip a cell); every other
    /// curve-modeled class stays shadow; `shadow` is the kill.
    #[test]
    fn cost_route_default_is_the_witnessed_rot_cells() {
        match cost_route_mode() {
            CostRouteMode::DecideClasses(v) => {
                assert_eq!(v.as_slice(), DEFAULT_DECIDE_CLASSES.as_slice());
            }
            m => panic!("t36 default must be the rot-cell decide list, got {m:?}"),
        }
        for name in DEFAULT_DECIDE_CLASSES {
            assert!(
                RuntimeClass::ALL.iter().any(|c| c.name() == name),
                "default decide-list names a real curve class: {name}"
            );
        }
        for &class in RuntimeClass::ALL.iter() {
            assert_eq!(
                cost_route_decides(class),
                DEFAULT_DECIDE_CLASSES.contains(&class.name()),
                "exactly the letters' three cells decide at default: {class:?}"
            );
        }
    }
}
