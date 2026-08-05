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
//!                     multibuild/groupsink hashjoin riders were UNWIRED
//!                     from the PlainAgg curve at GL-COST-2: their own
//!                     witnessed grids refuted the reuse (3.0-6.4x
//!                     rt/legacy) — guarded off at every size until an own
//!                     witnessed curve (m5_suppress::class_guard, TSV
//!                     rectangle_max_rows rows, kill
//!                     PGRUST_M5_HJRIDER_CURVE=1 one train).
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
    /// GL-MBSEAT-1: the grouped-join rider's OWN curve, fitted on the
    /// SEATED arm (MBSHARED + MBSEAT default-ON world) — the planner
    /// wiring mirrors both kills and falls back to the guarded-off
    /// rectangle when either is dark.
    CbHashJoinGroupedAgg,
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
            RuntimeClass::CbHashJoinGroupedAgg => "CbHashJoinGroupedAgg",
            RuntimeClass::HeapPlainCountStar => "HeapPlainCountStar",
            RuntimeClass::HeapCmpFoldPrefix => "HeapCmpFoldPrefix",
        }
    }

    pub const ALL: [RuntimeClass; 10] = [
        RuntimeClass::CbPlainAggFold,
        RuntimeClass::CbGroupedAggIntKeys,
        RuntimeClass::CbGroupedAggTextKey,
        RuntimeClass::CbGroupedAggTopN,
        RuntimeClass::CbDistinctIntKeys,
        RuntimeClass::CbTopnBoundedIntKeys,
        RuntimeClass::CbHashJoinPlainAgg,
        RuntimeClass::CbHashJoinGroupedAgg,
        RuntimeClass::HeapPlainCountStar,
        RuntimeClass::HeapCmpFoldPrefix,
    ];
}

/// Fitted per-class constants (units: legacy-row-equivalents; see module
/// doc). Values of record live in crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv.
/// `ser_setup`/`ser_row` — the R1 SERIAL-REGIME curve (t_ser = ser_setup
/// + ser_row * N, dop-independent): the same nine witnessed jobs' SERIAL
/// legs (serial:neg-ok on every row; valid where the runtime witness is
/// `absent` too — an engine-pinned serial wall needs no arm), fit with
/// the class's legacy params FROZEN (strictly additive: no engaged-regime
/// verdict can move). Consumed by `cost_route_verdict_regime` when the
/// arm-admission mirror says the arm will NOT own the suppressed plan.
#[derive(Clone, Copy, Debug)]
pub struct ClassModel {
    pub c_engage: f64,
    pub w_row: f64,
    pub l_setup: f64,
    pub l_cap: f64,
    pub n_min_fit: f64,
    pub ser_setup: f64,
    pub ser_row: f64,
    /// R2 emit terms (soak-adj round-2 §R2.4, the (class,rows,dop)
    /// aliasing breaker): per-OUTPUT-row cost added to each side —
    /// `t_rt += wout_rt*OUT`, `t_leg += wout_leg*OUT`. Zero for every
    /// class except CbHashJoinPlainAgg, whose both-geometry grid
    /// witnessed the split (one-clause OUT~N loses at dop4 while the
    /// two-clause OUT~0 member wins; the dense-seat control proved pages
    /// INERT on the join family). Callers without an output estimate
    /// price OUT := rows (the one-clause posture — the old implicit
    /// assumption, conservative for selective joins).
    pub wout_rt: f64,
    pub wout_leg: f64,
}

pub fn class_model(class: RuntimeClass) -> ClassModel {
    // GENERATED by scripts/runtime-cost-fit.py — edit the ladder cells /
    // rerun the fit, never hand-tune (the TSV drift test pins this block).
    match class {
        RuntimeClass::CbDistinctIntKeys => ClassModel { c_engage: 0.0, w_row: 1.1159, l_setup: 6127.2, l_cap: 11.52, n_min_fit: 1000000.0, ser_setup: 0.0, ser_row: 0.2701, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbGroupedAggIntKeys => ClassModel { c_engage: 1292041.9, w_row: 1.8392, l_setup: 2689172.8, l_cap: 6.04, n_min_fit: 1000000.0, ser_setup: 1521808.2, ser_row: 0.5155, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbGroupedAggTextKey => ClassModel { c_engage: 628935.8, w_row: 1.6932, l_setup: 647011.4, l_cap: 5.76, n_min_fit: 1000000.0, ser_setup: 496422.6, ser_row: 0.9510, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbGroupedAggTopN => ClassModel { c_engage: 461286.3, w_row: 1.6395, l_setup: 1903979.4, l_cap: 6.06, n_min_fit: 1000000.0, ser_setup: 534384.6, ser_row: 0.3162, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbHashJoinGroupedAgg => ClassModel { c_engage: 5695.0, w_row: 1.3218, l_setup: 64597.1, l_cap: 16.00, n_min_fit: 1000000.0, ser_setup: 0.0, ser_row: 0.0, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbHashJoinPlainAgg => ClassModel { c_engage: 35092.4, w_row: 0.9898, l_setup: 48931.8, l_cap: 11.79, n_min_fit: 1000000.0, ser_setup: 0.0, ser_row: 0.9928, wout_rt: 0.0254, wout_leg: 0.0 },
        RuntimeClass::CbPlainAggFold => ClassModel { c_engage: 0.0, w_row: 1.2712, l_setup: 18206.0, l_cap: 12.66, n_min_fit: 1000000.0, ser_setup: 29327.6, ser_row: 0.0120, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::CbTopnBoundedIntKeys => ClassModel { c_engage: 305844.2, w_row: 0.8711, l_setup: 322122.7, l_cap: 16.00, n_min_fit: 1000000.0, ser_setup: 592578.0, ser_row: 0.4290, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::HeapCmpFoldPrefix => ClassModel { c_engage: 199319.8, w_row: 1.1983, l_setup: 164041.4, l_cap: 7.32, n_min_fit: 100000.0, ser_setup: 228223.5, ser_row: 1.3933, wout_rt: 0.0, wout_leg: 0.0 },
        RuntimeClass::HeapPlainCountStar => ClassModel { c_engage: 257515.3, w_row: 0.3037, l_setup: 552913.9, l_cap: 6.88, n_min_fit: 2500000.0, ser_setup: 454725.6, ser_row: 0.4871, wout_rt: 0.0, wout_leg: 0.0 },
    }
}

/// Serial-regime support floor: the smallest serial-witnessed cell in the
/// nine-job grid. Below it the regime verdict fails toward the incumbent
/// (the n_min_fit posture applied to the serial curve). The R1 small-N
/// wave (30k..600k cells) extends this floor downward when it lands.
pub const N_MIN_SERIAL: f64 = 100_000.0;

/// GL-TOPNHEAP-1: the bounded top-N car's routed-admission k band (the
/// LIMIT bound axis the (N, dop) vocabulary never carried — the fit
/// vehicle's K_BAND block, scripts/runtime-cost-fit.py). The class's curve
/// is fit on the k=1000 SKIP-DEAD plane, which upper-bounds the car for
/// every smaller in-band k (zone skips only help), so the verdict is valid
/// for k in [K_MIN, K_MAX] and the router keeps Gather outside it: below
/// K_MIN the serial zone walk owns the shape (five-posture record:
/// car/serial 1.00-1.16 at k=10; legacy elects serial under the guard-off
/// posture); above K_MAX is unmeasured (fail toward the incumbent).
/// Provenance: SORTECON5 grid @ 8c11541a17, k in {10, 100, 1000}; the
/// k=100 cells beat serial 1.4-3.1x everywhere measured.
pub const TOPN_CAR_K_MIN: f64 = 100.0;
pub const TOPN_CAR_K_MAX: f64 = 1000.0;

/// Is the plan's LIMIT bound inside the car's routed-admission band?
/// (`k <= 0` = unknown/non-const — out of band, keep Gather.)
pub fn topn_car_k_band(k: f64) -> bool {
    (TOPN_CAR_K_MIN..=TOPN_CAR_K_MAX).contains(&k)
}

/// Classes whose serial fit FAILED the quality bar (rms > 40% on the
/// witnessed grid — a linear-in-N serial curve cannot follow the class's
/// measured serial/legacy surface):
///   CbDistinctIntKeys  rms ~1152% (sort+distinct serial cost is not
///                      linear against this legacy curve's tiny setup);
///   CbGroupedAggTopN   rms ~69%.
/// Their TSV rows carry the fitted values for provenance, but the regime
/// verdict ABSTAINS (fails toward the incumbent) — the F1 floor keeps
/// owning grouped-topn's small-N routing. Neither class is in the decide
/// list, so this exclusion changes no live route.
/// CbHashJoinGroupedAgg boarded with GL-MBSEAT-1 AFTER the R1 serial wave
/// (no witnessed serial cell — its ClassModel carries ser placeholders);
/// the regime verdict abstains (fails toward the incumbent) pending its
/// R1B refit row.
pub const SERIAL_FIT_UNUSABLE: [RuntimeClass; 3] = [
    RuntimeClass::CbDistinctIntKeys,
    RuntimeClass::CbGroupedAggTopN,
    RuntimeClass::CbHashJoinGroupedAgg,
];

/// Predicted serial/legacy ratio (the serial-regime curve). `None` when
/// the class's serial fit is below the quality bar.
pub fn predicted_serial_ratio(class: RuntimeClass, rows: f64, dop: i32) -> Option<f64> {
    if SERIAL_FIT_UNUSABLE.contains(&class) {
        return None;
    }
    let m = class_model(class);
    let d = (dop.max(1)) as f64;
    let t_ser = m.ser_setup + m.ser_row * rows;
    let t_leg = m.l_setup + rows / d.min(m.l_cap);
    Some(t_ser / t_leg)
}

/// The R1 regime-split verdict (soak-adjudication round-2 spec §R2.4):
/// when the plan-computable ARM-ADMISSION MIRROR says the arm will not
/// own the suppressed plan (`arm_admits == false`), suppression delivers
/// the SERIAL lane — so the verdict compares t_ser/t_leg on the serial
/// support floor instead of the engaged curve on n_min_fit. Both regimes
/// fail toward the incumbent outside their measured support; an
/// unusable serial fit abstains (Gather stands).
pub fn cost_route_verdict_regime(
    class: RuntimeClass,
    rows: f64,
    dop: i32,
    arm_admits: bool,
) -> CostVerdict {
    if arm_admits {
        return cost_route_verdict(class, rows, dop);
    }
    match predicted_serial_ratio(class, rows, dop) {
        Some(ratio) => CostVerdict {
            ratio,
            suppress: ratio <= 1.0 && rows >= N_MIN_SERIAL,
        },
        None => CostVerdict { ratio: f64::NAN, suppress: false },
    }
}

/// `PGRUST_M5_THREEWAY` — the GL-ELECTION-22 finding-2 gating fix's kill
/// (t35 flipped-kill idiom): DEFAULT ON = the decide-list consults the
/// THREE-WAY argmin (`cost_route_verdict_threeway`); `0|off` restores
/// the regime-gated two-way (`cost_route_verdict_regime`) for one train.
pub fn threeway_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_M5_THREEWAY").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// THE GL-ELECTION-22 FINDING-2 FIX: the three-way verdict. The regime
/// split only consulted the serial curve when the arm did NOT admit — so
/// at the unqualed plain-fold census cells (1e8 x dop16) the two-way
/// priced Gather at rt/legacy ~1.004 while the SAME model's serial fit
/// knew serial/legacy ~0.16, and nobody asked it. Here suppression is
/// priced as WHAT SUPPRESSION DELIVERS — the better of the two engines
/// the suppressed plan can land on (the runtime arm where it admits, the
/// serial lane always; at exec the arm's own bands pick between them,
/// e.g. the META band hands provable folds to the serial answer):
///
///   suppress iff min(t_ser*, t_rt*) <= t_leg
///
/// each side inside its own measured support (t_rt needs arm admission
/// AND rows >= n_min_fit; t_ser needs a usable serial fit AND rows >=
/// N_MIN_SERIAL AND `serial_applies` — the caller's POSTURE gate: the
/// class's serial cells must describe this shape's serial delivery.
/// CbPlainAggFold's serial fit IS the footer-META wall (fit from
/// provable-qual cells, ~flat 0.6-1.0ms), so its caller passes the
/// plan-time mirror of the executor META band: unqualed, or estimated
/// survival ~1 — a Mixed-qual fold must NOT ride the META-priced curve.
/// Other classes' serial cells are ordinary serial walls
/// (serial_applies=true). A side without support does not compete; with
/// NO side in support the verdict fails toward Gather, exactly as
/// before.
pub fn cost_route_verdict_threeway(
    class: RuntimeClass,
    rows: f64,
    dop: i32,
    arm_admits: bool,
    serial_applies: bool,
) -> CostVerdict {
    cost_route_verdict_threeway_out(class, rows, dop, arm_admits, serial_applies, None)
}

/// The three-way with the R2 output-cardinality input.
pub fn cost_route_verdict_threeway_out(
    class: RuntimeClass,
    rows: f64,
    dop: i32,
    arm_admits: bool,
    serial_applies: bool,
    out_rows: Option<f64>,
) -> CostVerdict {
    let m = class_model(class);
    let mut best: Option<f64> = None;
    if arm_admits && rows >= m.n_min_fit {
        best = Some(predicted_ratio_out(class, rows, dop, out_rows));
    }
    if serial_applies && rows >= N_MIN_SERIAL {
        if let Some(sr) = predicted_serial_ratio(class, rows, dop) {
            best = Some(match best {
                Some(r) => r.min(sr),
                None => sr,
            });
        }
    }
    match best {
        Some(ratio) => CostVerdict { ratio, suppress: ratio <= 1.0 },
        None => CostVerdict { ratio: f64::NAN, suppress: false },
    }
}

/// HeapPlainCountStar's rowdrive 64MB block-floor ADMISSION MIRROR
/// (m5-5 reading #3): applied by the caller in EVERY cost-route mode —
/// suppressing below the block geometry lands on a refusing arm and a
/// losing serial fallback. Never retired by the curve.
pub const HEAP_COUNT_ADMISSION_MIN_PAGES: f64 = 8192.0;

/// Predicted rt/legacy ratio for a class at (est rows, engaged dop),
/// with the R2 OUTPUT-cardinality input: `None` prices OUT := rows (the
/// one-clause posture — identical to the pre-R2 behavior for classes
/// with zero emit terms, and the conservative default for joins).
pub fn predicted_ratio_out(
    class: RuntimeClass,
    rows: f64,
    dop: i32,
    out_rows: Option<f64>,
) -> f64 {
    let m = class_model(class);
    let d = (dop.max(1)) as f64;
    let out = out_rows.unwrap_or(rows).max(0.0);
    let t_rt = m.c_engage + m.w_row * rows / d + m.wout_rt * out;
    let t_leg = m.l_setup + rows / d.min(m.l_cap) + m.wout_leg * out;
    t_rt / t_leg
}

/// Predicted rt/legacy ratio at the one-clause posture (OUT = rows).
pub fn predicted_ratio(class: RuntimeClass, rows: f64, dop: i32) -> f64 {
    predicted_ratio_out(class, rows, dop, None)
}

/// The curve verdict at (rows, dop).
#[derive(Clone, Copy, Debug)]
pub struct CostVerdict {
    pub ratio: f64,
    pub suppress: bool,
}

pub fn cost_route_verdict(class: RuntimeClass, rows: f64, dop: i32) -> CostVerdict {
    cost_route_verdict_out(class, rows, dop, None)
}

/// `cost_route_verdict` with the R2 output-cardinality input.
pub fn cost_route_verdict_out(
    class: RuntimeClass,
    rows: f64,
    dop: i32,
    out_rows: Option<f64>,
) -> CostVerdict {
    let ratio = predicted_ratio_out(class, rows, dop, out_rows);
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

/// The default decide-list: the t36 flips2 witnessed rot cells
/// (notes/gl-cost-class-flip-letters.md — all FLIP-RECOMMENDED) plus the
/// GL-MBSEAT-1 seated grouped-join lift (notes/gl-mbseat-1-letter.md:
/// its own witnessed curve on the MBSHARED+MBSEAT arm; the planner
/// wiring un-curves the class when either knob is killed, so the decide
/// entry is inert in any kill posture). Kept as a named constant so the
/// default and the letters stay reviewably tied.
/// GL-TOPNHEAP-1 (2026-07-21): CbTopnBoundedIntKeys joins on the car's
/// five-posture record — the planner wiring gates its decide entry on the
/// PGRUST_RUNTIME_TOPN_HEAP spelling AND the TOPN_CAR_K_* band, so the
/// entry is inert when the car is killed or the shape is out of band
/// (keep Gather = the GL-COST-TOPN-1 guard-off posture, byte-exactly).
const DEFAULT_DECIDE_CLASSES: [&str; 5] = [
    "CbPlainAggFold",
    "CbGroupedAggTextKey",
    "CbHashJoinPlainAgg",
    "CbHashJoinGroupedAgg",
    "CbTopnBoundedIntKeys",
];

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
        // GL-TOPNHEAP-1 re-refit (supersedes the L6/GL-SORTECON-3 cells):
        // under the flipped PGRUST_RUNTIME_TOPN_HEAP the routed arm is the
        // direct morsel-native CAR — five-posture vehicle
        // scripts/sortecon-topn-ladder.sh (SORTECON5 heap legs) @
        // 8c11541a17, fleet fast-profile jobs -214f/-6a7c/-7ea6/-3383
        // (dist confirms -17ce/-7912; dup band -743c/-0282). Ratios are
        // car / forced-Gather-Merge on the k=1000 SKIP-DEAD plane (gcut
        // zone-skips witnessed 0 at every cell), which upper-bounds the
        // car for smaller in-band k — the k axis is the TOPN_CAR_K_*
        // routed-admission band, not a curve term. k=10 is OUT OF BAND
        // (the serial zone walk owns it; L4). Legacy params FROZEN at the
        // L6 values (the GM legs are the same posture; the ser_* rows
        // were fit against them).
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 2, 0.889),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 4, 0.935),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 8, 0.871),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e6, 16, 0.903),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 2, 0.893),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 4, 0.884),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 8, 0.971),
        (RuntimeClass::CbTopnBoundedIntKeys, 2.5e6, 16, 0.939),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 2, 0.879),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 4, 0.879),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 8, 0.917),
        (RuntimeClass::CbTopnBoundedIntKeys, 5e6, 16, 0.947),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e7, 2, 0.854),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e7, 4, 0.857),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e7, 8, 0.884),
        (RuntimeClass::CbTopnBoundedIntKeys, 1e7, 16, 0.922),
        // GL-MBSEAT-1 seated grid (jobs pgrust-fast-tests-39d74f1439-*,
        // @ night/mbseat 39d74f143; every leg dop-pinned CLEAN; the arm
        // of record is MBSHARED+MBSEAT — see the class's enum doc).
        (RuntimeClass::CbHashJoinGroupedAgg, 1e6, 4, 1.200),
        (RuntimeClass::CbHashJoinGroupedAgg, 1e6, 8, 0.832),
        (RuntimeClass::CbHashJoinGroupedAgg, 1e6, 16, 0.705),
        (RuntimeClass::CbHashJoinGroupedAgg, 2.5e6, 4, 1.197),
        (RuntimeClass::CbHashJoinGroupedAgg, 2.5e6, 8, 1.112),
        (RuntimeClass::CbHashJoinGroupedAgg, 2.5e6, 16, 0.972),
        (RuntimeClass::CbHashJoinGroupedAgg, 5e6, 4, 1.289),
        (RuntimeClass::CbHashJoinGroupedAgg, 5e6, 8, 1.125),
        (RuntimeClass::CbHashJoinGroupedAgg, 5e6, 16, 1.102),
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
            // F1 (soak adjudication round 1): post-qual min_rows floor —
            // tiny-selective shapes land on the sorted serial election the
            // arm refuses (suppress-then-refuse).
            RuntimeClass::CbGroupedAggTopN => rows >= 500_000.0,
            RuntimeClass::CbDistinctIntKeys => dop >= 12,
            // GL-COST-TOPN-1 guard-off (the GL-SORTECON-3 min_dop=4
            // re-flip retired): zero best-of-four wins on the four-posture
            // grid — the rectangle never suppresses.
            RuntimeClass::CbTopnBoundedIntKeys => false,
            // S1 band collapse (soak adjudication round 1): the arm-floor
            // min, the low-dop 2M ceiling, and the dop>=12 extension to the
            // fitted dop16 crossover (~4.18M) floored to 4M.
            RuntimeClass::CbHashJoinPlainAgg => {
                rows >= 524_288.0
                    && rows <= 4_000_000.0
                    && (dop >= 12 || rows <= 2_000_000.0)
            }
            // GL-MBSEAT-1: the shipped FloorGuard for the rider is
            // max_rows=0 (never suppress) — the lift lives ONLY in the
            // decide-listed curve; shadow/kill postures keep Gather.
            RuntimeClass::CbHashJoinGroupedAgg => false,
            RuntimeClass::HeapPlainCountStar => true, // pages mirror aside
            RuntimeClass::HeapCmpFoldPrefix => rows >= 1_000_000.0 && dop >= 12,
        }
    }

    /// Classes whose WITNESSED v2 cells were measured on an arm that has
    /// since CHANGED ECONOMICS (the design §3 arm-change trigger, fired but
    /// not yet honored with a refit ladder). EMPTY since the L6 refit
    /// (GL-SORTECON-3 follow-through, 2026-07-21): CbTopnBoundedIntKeys was
    /// the only member — its post-COLSTAGE four-posture grid landed
    /// (scripts/sortecon-topn-ladder.sh @ 27db94812) and the curve refit
    /// from the k=1000 rt/GM cells. The mechanism stays: an arm rework that
    /// invalidates a class's fitted cells re-adds the class here WITH its
    /// TSV rows flipped to witnessed-v2-superseded, never by drift.
    const SUPERSEDED_CLASSES: &[RuntimeClass] = &[];

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
    /// the four v1-carved floor-rot cells that suppress at a measured loss
    /// (scan-fold 1M@dop4 1.258; text 1M/2.5M@dop4 1.130/1.339; hashjoin
    /// 1M@dop4 1.319). The fifth disagreement of the original pin — the
    /// forgone win the clean-2M hashjoin bound sacrificed (2.5M@dop16
    /// 0.923) — was RETIRED by the S1 band collapse (soak adjudication
    /// round 1, 2026-07-21: re-confirmed in vivo 2.06x, whereupon the
    /// rectangle was re-derived to mirror the witnessed curve verdicts).
    /// These are the flip-gate candidates (GL-COST-<class> letters). Any
    /// new disagreement is a red test — it must arrive with a witnessed
    /// cell, not by drift. (Parity-band cells exempt.)
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
                // GL-TOPNHEAP-1: the CAR's witnessed win cells (the
                // re-refit curve suppresses at every out-of-parity k=1000
                // cell; the 2.5M@8 cell at 0.971 is band-exempt) vs the
                // GL-COST-TOPN-1 guard-off rectangle (never suppresses).
                // RESOLVED BY THE DECIDE LIST: the class decides by curve
                // at default, gated in the planner on the
                // PGRUST_RUNTIME_TOPN_HEAP spelling + TOPN_CAR_K_* band —
                // killed or out-of-band, the rectangle's keep-Gather
                // stands and these cells are inert.
                (RuntimeClass::CbTopnBoundedIntKeys, 1_000_000, 2),
                (RuntimeClass::CbTopnBoundedIntKeys, 1_000_000, 4),
                (RuntimeClass::CbTopnBoundedIntKeys, 1_000_000, 8),
                (RuntimeClass::CbTopnBoundedIntKeys, 1_000_000, 16),
                (RuntimeClass::CbTopnBoundedIntKeys, 2_500_000, 2),
                (RuntimeClass::CbTopnBoundedIntKeys, 2_500_000, 4),
                (RuntimeClass::CbTopnBoundedIntKeys, 2_500_000, 16),
                (RuntimeClass::CbTopnBoundedIntKeys, 5_000_000, 2),
                (RuntimeClass::CbTopnBoundedIntKeys, 5_000_000, 4),
                (RuntimeClass::CbTopnBoundedIntKeys, 5_000_000, 8),
                (RuntimeClass::CbTopnBoundedIntKeys, 5_000_000, 16),
                (RuntimeClass::CbTopnBoundedIntKeys, 10_000_000, 2),
                (RuntimeClass::CbTopnBoundedIntKeys, 10_000_000, 4),
                (RuntimeClass::CbTopnBoundedIntKeys, 10_000_000, 8),
                (RuntimeClass::CbTopnBoundedIntKeys, 10_000_000, 16),
                // GL-MBSEAT-1: the lift's clear-win cells (outside the
                // ±5% parity band; the 2.5M@16 win at 0.972 is band-exempt)
                // — the rider's shipped floor is max_rows=0 (never
                // suppress), so every curve-suppressed win cell disagrees
                // by design; the decide-list entry IS the flip that
                // resolves them. CELLS order (grouped precedes plain).
                (RuntimeClass::CbHashJoinGroupedAgg, 1_000_000, 8),
                (RuntimeClass::CbHashJoinGroupedAgg, 1_000_000, 16),
                (RuntimeClass::CbHashJoinPlainAgg, 1_000_000, 4),
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
        // 0.923). The R2 refit (emit-term split; legacy frozen) keeps the
        // dop-axis crossover in the witnessed bracket; the 5M@16 cell —
        // measured 1.024, inside the ±5% parity band — moved to
        // SUPPRESSED-AT-PARITY (the M5-5 acceptance bar's allowed
        // outcome; GL-SERIALTERM-R2-HJ-1 letter).
        let d = root_d(RuntimeClass::CbHashJoinPlainAgg, 2.5e6).unwrap();
        assert!((9..=16).contains(&d), "CbHashJoinPlainAgg D*(2.5M)={d}");
        let v = cost_route_verdict(RuntimeClass::CbHashJoinPlainAgg, 5e6, 16);
        assert!(v.suppress && v.ratio > 0.95, "5M@16 suppressed-at-parity: {v:?}");
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
        // topn (GL-TOPNHEAP-1): the CAR's re-refit curve SUPPRESSES at
        // every witnessed k=1000 cell (measured car/GM 0.854-0.971, all
        // 16 cells; predicted 0.876-0.937) — suppression IS the witnessed
        // direction under the flipped arm. The k band and the kill are the
        // planner's gates (out-of-band/killed = the guard-off keep-Gather,
        // asserted by the suppression e2e's topn legs).
        for &(class, rows, dop, _) in CELLS {
            if class == RuntimeClass::CbTopnBoundedIntKeys {
                assert!(
                    cost_route_verdict(class, rows, dop).suppress,
                    "car curve must suppress at witnessed cell rows={rows} dop={dop}"
                );
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
                "ser_setup" => m.ser_setup,
                "ser_row" => m.ser_row,
                "wout_rt" => m.wout_rt,
                "wout_leg" => m.wout_leg,
                "admission_min_pages" => HEAP_COUNT_ADMISSION_MIN_PAGES,
                _ => continue, // structural terms owned by other pins
            };
            let got: f64 = value.parse().unwrap();
            assert_eq!(got, expect, "{class_name}.{term}: TSV {got} != code {expect}");
            if term != "admission_min_pages" {
                seen.insert((class_name.to_string(), term.to_string()));
            }
        }
        // Every curve class carries all seven terms in the TSV (five
        // engaged-regime + the two R1 serial-regime terms); the R2 emit
        // terms are carried ONLY by the class whose both-geometry grid
        // fit them (CbHashJoinPlainAgg) — a wout row for any other class
        // must arrive with its own geometry cells.
        assert_eq!(
            seen.len(),
            RuntimeClass::ALL.len() * 7 + 2,
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
                // The R1 serial-regime terms are witnessed-v2 for EVERY
                // class the R1 wave fitted — the L6 supersession reworked
                // the ARM, not the serial lane the ser legs measured.
                // CARVE (GL-MBSEAT-1 boarding): CbHashJoinGroupedAgg
                // boarded AFTER the R1 wave with no witnessed serial cell;
                // its ser rows are abstain placeholders (SERIAL_FIT_UNUSABLE
                // fails toward the incumbent) carried as unwitnessed-debt
                // until the R1B refit row lands — the debt census below
                // pins the pair so it cannot rot silently.
                if matches!(term, "ser_setup" | "ser_row" | "wout_rt" | "wout_leg") {
                    let want = if rc == RuntimeClass::CbHashJoinGroupedAgg {
                        "unwitnessed-debt"
                    } else {
                        "witnessed-v2"
                    };
                    assert_eq!(
                        witness, want,
                        "serial-regime term {class}.{term} witness status"
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
        // witnessed-refutes-reuse is ACTED-ON state, not a resting place:
        // the L1/L2 refutation (3.0-6.4x rt/legacy grids @ d10db8ef5e) was
        // resolved by the GL-COST-2 unwire (the riders' rectangle_max_rows=0
        // rows) — so the refuted census must be EMPTY. A new refuted row is
        // legitimate only while its unwire letter is in flight.
        assert_eq!(
            refuted,
            Vec::<(String, String)>::new(),
            "a witnessed-refutes-reuse row exists — its unwire letter is owed"
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
            vec![
                ("CbHashJoinPlainAgg".to_string(), "seat_overlap_cell".to_string()),
                // GL-MBSEAT-1 boarding: the seated class's ser placeholders
                // (abstain via SERIAL_FIT_UNUSABLE) — retired by the R1B
                // refit row, flipping WITH the TSV rows, never by drift.
                ("CbHashJoinGroupedAgg".to_string(), "ser_setup".to_string()),
                ("CbHashJoinGroupedAgg".to_string(), "ser_row".to_string()),
            ],
            "the unwitnessed-debt census changed"
        );
    }

    /// Classes whose witnessed grid shows the LEGACY engine out-scaling the
    /// runtime arm in D — the arm's wall time is engage-dominated (flat to
    /// RISING in dop: 20.4->30.6ms across dop 4->16 at 2.5M) while forced
    /// Gather Merge keeps gaining through dop16 (w16 witnessed at 5M/10M,
    /// gm 7.0->5.3ms). For these the fitted l_cap legitimately reaches the
    /// 16.0 bound (legacy never saturates below the measured dop ceiling)
    /// and the predicted ratio legitimately WORSENS as dop widens.
    /// Witnessed: the L6 four-posture grid @ 27db94812 (rt/GM rising
    /// 6.80->9.77 at 1M across dop 4->16).
    const LEGACY_OUTSCALES_ARM: &[RuntimeClass] = &[RuntimeClass::CbTopnBoundedIntKeys];
    // GL-MBSEAT-1: the seated grouped-join class's legacy reference
    // genuinely scaled to w16 in its witnessed grid (gather+w16 at 5M),
    // so its fit sits AT the l_cap bound like the topn class — but its
    // RATIO shape is the normal one (improves dop4 -> dop16), so it is
    // NOT in the outscales set.
    const L_CAP_AT_FIT_BOUND: &[RuntimeClass] =
        &[RuntimeClass::CbTopnBoundedIntKeys, RuntimeClass::CbHashJoinGroupedAgg];

    /// Model-shape sanity (charter: monotonicity unit tests): both cost
    /// curves strictly increase in N; the runtime side never gets more
    /// expensive with more workers; and widening dop 4 -> 16 never makes the
    /// predicted rt/legacy ratio WORSE anywhere in or above measured support
    /// (the fitted l_cap saturates below dop16 for every class EXCEPT the
    /// LEGACY_OUTSCALES_ARM set, where the witnessed record shows the
    /// opposite shape).
    #[test]
    fn model_terms_are_monotone() {
        let grid = [1e5, 2.5e5, 5e5, 1e6, 2.5e6, 5e6, 1e7];
        for &class in RuntimeClass::ALL.iter() {
            let m = class_model(class);
            assert!(m.w_row > 0.0 && m.l_setup > 0.0 && m.c_engage >= 0.0, "{class:?}");
            if L_CAP_AT_FIT_BOUND.contains(&class) {
                assert!(m.l_cap <= 16.0, "{class:?}: l_cap capped at the fit bound");
            } else {
                assert!(m.l_cap < 16.0, "{class:?}: l_cap must saturate below dop16");
            }
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
                // Ratio no worse at dop16 than dop4 — except where the
                // witnessed record shows legacy out-scaling the arm.
                if LEGACY_OUTSCALES_ARM.contains(&class) {
                    assert!(
                        predicted_ratio(class, n, 16) >= predicted_ratio(class, n, 4) - 1e-9,
                        "{class:?} N={n}: the legacy-outscales shape inverted"
                    );
                } else {
                    assert!(
                        predicted_ratio(class, n, 16) <= predicted_ratio(class, n, 4) + 1e-9,
                        "{class:?} N={n}: ratio worsens from dop4 to dop16"
                    );
                }
            }
        }
    }

    /// R1 REGIME PINS (soak-adj round-2 §R2.4 acceptance):
    /// (1) with arm_admits=true the regime verdict is BYTE-IDENTICAL to
    ///     the shipped verdict at every witnessed cell — no agree-cell
    ///     can flip;
    /// (2) the q2-class cells (scan-fold below the arm floor, where the
    ///     nine-job grid witnessed serial 0.6-0.8ms vs forced Gather
    ///     2.1-2.8ms) route to SUPPRESSION under the fallback regime;
    /// (3) the hashjoin sub-floor LOSS cells (serial/legacy 1.68-2.35 at
    ///     250k-500k) keep Gather by ECONOMICS, not by any clamp;
    /// (4) below the serial support floor the regime abstains (Gather
    ///     stands), and unusable-fit classes always abstain.
    #[test]
    fn regime_split_matches_the_witnessed_serial_record() {
        // (1) engaged regime is the shipped verdict, cell for cell.
        for &(class, rows, dop, _) in CELLS {
            let a = cost_route_verdict(class, rows, dop);
            let b = cost_route_verdict_regime(class, rows, dop, true);
            assert_eq!(a.suppress, b.suppress, "{class:?} N={rows} D={dop}");
            assert_eq!(a.ratio, b.ratio, "{class:?} N={rows} D={dop}");
        }
        // (2) q2-class small-N scan-fold wins route to suppression.
        for rows in [1e5, 2.5e5, 5e5] {
            let v = cost_route_verdict_regime(RuntimeClass::CbPlainAggFold, rows, 4, false);
            assert!(
                v.suppress && v.ratio < 1.0,
                "scan-fold fallback regime must suppress at N={rows}: {v:?}"
            );
        }
        // (3) hashjoin sub-floor losses keep Gather by economics.
        for rows in [2.5e5, 5e5] {
            let v = cost_route_verdict_regime(RuntimeClass::CbHashJoinPlainAgg, rows, 4, false);
            assert!(
                !v.suppress && v.ratio > 1.0,
                "hashjoin fallback regime must keep Gather by ratio at N={rows}: {v:?}"
            );
        }
        // (4) support floor + unusable-fit abstention.
        assert!(!cost_route_verdict_regime(RuntimeClass::CbPlainAggFold, 5e4, 4, false).suppress);
        for class in SERIAL_FIT_UNUSABLE {
            for rows in [1e5, 5e5, 1e6] {
                let v = cost_route_verdict_regime(class, rows, 4, false);
                assert!(!v.suppress, "{class:?} unusable fit must abstain at N={rows}");
            }
            assert!(predicted_serial_ratio(class, 1e6, 4).is_none());
        }
        // Serial-curve shape sanity: nondecreasing in N, dop-independent
        // numerator (the ratio still moves with dop through t_leg only).
        for &class in RuntimeClass::ALL.iter() {
            let m = class_model(class);
            assert!(m.ser_setup >= 0.0 && m.ser_row >= 0.0, "{class:?}");
            if SERIAL_FIT_UNUSABLE.contains(&class) {
                continue;
            }
            let grid = [1e5, 2.5e5, 5e5, 1e6, 5e6];
            for w in grid.windows(2) {
                let t = |n: f64| m.ser_setup + m.ser_row * n;
                assert!(t(w[1]) >= t(w[0]), "{class:?} t_ser decreasing in N");
            }
        }
    }

    /// R2 BOTH-GEOMETRY EQUIVALENCE (soak-adj §R2.4): one hashjoin curve
    /// predicts both aliasing members' witnessed verdicts once OUT_ROWS
    /// is an input — (rows, dop, OUT, measured rt/legacy) from the wide
    /// one-clause grid (OUT ~ N), the dense-seat control (OUT ~ N; pages
    /// proven INERT — it tracks the wide member at every config), and the
    /// dense two-clause member (OUT ~ 0; the witnessed WIN the
    /// (class,rows,dop) vocabulary aliased away). Parity-band cells
    /// (±5%) exempt per the M5-5 bar.
    #[test]
    fn r2_out_rows_input_breaks_the_geometry_aliasing() {
        const R2_CELLS: &[(f64, i32, f64, f64)] = &[
            (1e6, 4, 1e6, 1.319),
            (2.5e6, 4, 2.5e6, 1.494),
            (5e6, 4, 5e6, 1.545),
            (5e6, 8, 5e6, 1.284),
            (5e6, 16, 5e6, 1.024),
            (2.5e6, 16, 2.5e6, 0.923),
            (1e6, 4, 1e6, 1.413),      // dense seat
            (2.5e6, 4, 2.5e6, 1.524),  // dense seat
            (2.5e6, 16, 2.5e6, 0.914), // dense seat
            (1e6, 4, 40.0, 0.745),     // dense two-clause
            (2.5e6, 4, 100.0, 0.649),  // dense two-clause
            (2.5e6, 16, 100.0, 0.485), // dense two-clause
        ];
        for &(rows, dop, out, meas) in R2_CELLS {
            if (0.95..=1.05).contains(&meas) {
                continue;
            }
            let v = cost_route_verdict_out(
                RuntimeClass::CbHashJoinPlainAgg,
                rows,
                dop,
                Some(out),
            );
            assert_eq!(
                v.suppress,
                meas < 1.0,
                "N={rows} D={dop} OUT={out}: measured {meas}, predicted {v:?}"
            );
        }
        // The out-less wrapper prices OUT := rows — identical to the
        // one-clause posture (back-compat for every caller without an
        // estimate).
        for &(rows, dop, ..) in R2_CELLS {
            assert_eq!(
                cost_route_verdict(RuntimeClass::CbHashJoinPlainAgg, rows, dop).suppress,
                cost_route_verdict_out(RuntimeClass::CbHashJoinPlainAgg, rows, dop, Some(rows))
                    .suppress
            );
        }
        // Emit terms are zero for every class without geometry cells.
        for &class in RuntimeClass::ALL.iter() {
            if class != RuntimeClass::CbHashJoinPlainAgg {
                let m = class_model(class);
                assert_eq!((m.wout_rt, m.wout_leg), (0.0, 0.0), "{class:?}");
            }
        }
    }

    /// GL-ELECTION-22 FINDING-2 PINS: the three-way argmin consults the
    /// serial curve even when the arm admits.
    /// (1) THE MOTIVATING CENSUS CELL — the unqualed plain-fold family at
    ///     1e8 x dop16 (six census queries, one mechanism): the two-way
    ///     prices the arm at rt/legacy ~1.004 and keeps Gather; the
    ///     three-way sees the serial/META curve at ~0.16 and suppresses.
    ///     Witnessed anchors: the L4 cell (serial/legacy 1.1/8.1 = 0.136
    ///     at 10M) and the META-2 both-ways ladder (band-delivered 1.1ms
    ///     vs Gather 20.9/8.1 at 10M) — the executor META band (flipped
    ///     on this lineage) is what suppression DELIVERS on this posture.
    /// (2) The change set at WITNESSED cells is exactly the plain-fold
    ///     family: textkey/hashjoin serial curves lose everywhere in
    ///     measured range, so their three-way verdicts equal the two-way
    ///     cell for cell.
    /// (3) The posture gate: serial_applies=false (a Mixed-selective
    ///     fold, whose delivery is the ARM, not the META answer) restores
    ///     the two-way verdict exactly.
    /// (4) With neither side in support the verdict still fails toward
    ///     Gather.
    #[test]
    fn threeway_consults_the_serial_curve_when_the_arm_admits() {
        // (1) the census cell.
        let two = cost_route_verdict_regime(RuntimeClass::CbPlainAggFold, 1e8, 16, true);
        let three =
            cost_route_verdict_threeway(RuntimeClass::CbPlainAggFold, 1e8, 16, true, true);
        assert!(!two.suppress && two.ratio > 1.0, "two-way keeps Gather: {two:?}");
        assert!(
            three.suppress && three.ratio < 0.2,
            "three-way must price the serial META answer: {three:?}"
        );
        // (2) witnessed change set = plain-fold only.
        let mut changed = Vec::new();
        for &(class, rows, dop, _) in CELLS {
            if superseded(class) {
                continue;
            }
            let two = cost_route_verdict_regime(class, rows, dop, true);
            let three = cost_route_verdict_threeway(class, rows, dop, true, true);
            if two.suppress != three.suppress {
                changed.push((class, rows as i64, dop));
            }
        }
        assert_eq!(
            changed,
            vec![
                // The LIVE set (decide-listed): the four dop4/8 fold cells
                // (dop16 already suppressed by the arm curve). Each flipped
                // cell's WITNESSED serial leg beats legacy 4-11x (the v2
                // grid serial walls, 0.09-0.23 serial/legacy) — the flip is
                // toward the measured-best engine everywhere it fires.
                (RuntimeClass::CbPlainAggFold, 1_000_000, 4),
                (RuntimeClass::CbPlainAggFold, 2_500_000, 4),
                (RuntimeClass::CbPlainAggFold, 5_000_000, 4),
                (RuntimeClass::CbPlainAggFold, 5_000_000, 8),
                // SHADOW-ONLY (the class is guarded off and NOT in the
                // decide list — nothing routes off these): the low-dop
                // bounded-topn cells, where the R1 serial curve (fit on the
                // dup-heavy k=100 fixture) beats the GM-legged legacy
                // curve. That serial curve is K-BLIND; the k-dependence is
                // owned by serial_model::SerialTopnIntBounded (two
                // witnessed k-planes), and any decide-listing of this
                // class rides THAT vocabulary, never this curve.
                // (Set re-derived TWICE: at the ser-term RE-EMIT the
                // 2.5M@2 cell left — sr 1.059 > 1 keeps Gather; at the
                // GL-TOPNHEAP-1 k-banded refit (t43 composition) the six
                // remaining low-dop topn cells left — the refit engaged
                // curve (c_engage 305844.2, w_row 0.8711) already
                // suppresses them TWO-WAY, so the serial term no longer
                // changes any topn verdict. The shadow set is now exactly
                // the four decide-listed fold cells.)
            ],
            "the three-way witnessed change set"
        );
        for (class, _, _) in &changed {
            if *class != RuntimeClass::CbPlainAggFold {
                assert!(
                    !DEFAULT_DECIDE_CLASSES.contains(&class.name()),
                    "{class:?}: only decide-listed flips are live; the rest must be shadow"
                );
            }
        }
        // Each flipped cell's WITNESSED serial leg beats legacy (the v2
        // grid's serial walls: 0.09-0.23 serial/legacy) — the flip is
        // toward the measured-best engine at every changed cell.
        // (3) posture gate restores the two-way.
        for &(class, rows, dop, _) in CELLS {
            if superseded(class) {
                continue;
            }
            let two = cost_route_verdict_regime(class, rows, dop, true);
            let gated = cost_route_verdict_threeway(class, rows, dop, true, false);
            assert_eq!(two.suppress, gated.suppress, "{class:?} N={rows} D={dop}");
        }
        // (4) no side in support -> Gather stands.
        let v = cost_route_verdict_threeway(RuntimeClass::CbPlainAggFold, 5e4, 4, false, false);
        assert!(!v.suppress);
        // Kill spelling: default ON unless 0|off (read once; only assert
        // when the runner env leaves it unset).
        if std::env::var("PGRUST_M5_THREEWAY").is_err() {
            assert!(threeway_enabled());
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
