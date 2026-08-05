//! The SERIAL-SIDE cost term — step-2 three-way shadow arbitration
//! (runtime-cost-model design §2 "serial-side term" / §5 step 2).
//!
//! `runtime_model` prices runtime-vs-Gather; it carries NO term for the
//! serial lane's advantages — the early-exit/zone-skip machinery (zone
//! maps, best-first bound tightening, shared-cutoff granule skip, footer
//! META answers) that makes the serial lane the measured BEST engine on
//! three witnessed shape families, each hand-fenced today instead of
//! priced:
//!   * the selective-qual datetime-lead bounded top-N carve
//!     (m5_suppress `TOPN_NONINT_MIN_QUAL_SURVIVAL`),
//!   * the truncated-timestamp grouped-fold page fence
//!     (m5_suppress `tstrunc_max_pages`),
//!   * the unqualed wide-tlist bounded top-N residual (the serial
//!     zone-adaptive walk beats the engaged runtime arm at the full
//!     scale; named in the topn-nonint letter, un-fenced).
//!
//! WHAT MAKES SERIAL FAST, as a model: the walk's cost is granules READ,
//! not rows scanned. A bounded shape's best-first walk reads a ~constant
//! granule count (`g_bf`) at survival 1 and pays `1/q` more as a qual
//! starves the bound (survivors must be found before the bound can prune);
//! zone-friendly data (sorted-bank axis) shrinks `g_bf` an order of
//! magnitude because bands are disjoint and skips are provable. Unbounded
//! folds read every granule, but a zone-PROVABLE qual downgrades a
//! granule read to a footer META answer.
//!
//! ```text
//!   g_total = N / GRANULE_ROWS
//!   t_ser   = s_setup + s_gran * g_read
//!     bounded, numeric lead:  g_read = min(g_total, g_bf(z) * (k/10) / q)
//!     bounded, text lead:     g_read = g_total   (no zone vocabulary)
//!     unbounded fold:         g_read = g_total   (META: s_meta per granule)
//!   t_par   = setup + rate * N / min(D, cap)      (per family + engine)
//!   pick    = argmin over the family's REACHABLE engines
//! ```
//!
//! Reachability is structural: on the zone-friendly posture the runtime
//! sort arm BAND-REFUSES to the serial walk (the executor's own predicate),
//! so the friendly three-way is {serial, Gather} — pricing an engine the
//! router will not run would be a fiction.
//!
//! UNITS: witnessed-wall-ms on the reference fleet rig (dist profile,
//! warm medians). Verdicts are argmin comparisons WITHIN one family, so
//! the absolute anchor cancels — the same normalization argument as
//! runtime_model's legacy-row-equivalents. The two modules' numbers are
//! NOT commensurable; nothing here feeds the two-way rt/legacy curves.
//!
//! CALIBRATION (witnessed-ab; constants of record in
//! crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv, pinned by
//! `serial_constants_match_tsv`; fit script scripts/serial-term-fit.py):
//! * topn-nonint family: the GL-TOPNNI-1 three-way grid (10M dist job
//!   pgrust-fast-tests-8cf38a8c7c-1784631271-6af8 + the wave-3 100M dist
//!   leg @ 8cf38a8c7; per-cell serial/legacy/runtime witnesses), plus the
//!   real-sorted-bank friendly anchors (flip-sanity jobs -628/-632/-634272
//!   @ 34b23fdf2).
//! * tstrunc family: the GL-OPENROWS car-3 two-scale record (jobs
//!   -0c51/-2e14/-70ed/-66f5 @ 0ead86dcd/cd271d726).
//! * topn-selqual family (the carve flip's ENFORCING model): the
//!   GL-RESIDUAL-2 born-RED red legs' star-wide selective cells (jobs
//!   pgrust-resid-bornred-red-s{10m,100m}-… -31c8/-0156, 3 draws each)
//!   plus the green legs' stock-posture cells (-2f77/-671f).
//! * scanfold-meta family: the ladder-spec L4 named serial-dominance cell
//!   (job …-1784634348-6239 @ 45f07dd71). Single-cell anchor — the L7
//!   ladder spec owns the grid.
//!
//! SHADOW ONLY: this module decides NOTHING. The planner traces/censuses
//! the three-way pick next to the shipped carves (m5_suppress
//! serial_shadow); the carves stay until their GL flip letters ride the
//! agreement record this shadow banks.

/// Footer granule geometry of the banked fixtures (rows per granule;
/// 1221 granules at the 10M banks — witnessed in the topn-nonint letter's
/// band diagnosis).
pub const GRANULE_ROWS: f64 = 8192.0;

/// Engage/parse floor shared by every serial leg (ms).
pub const SER_SETUP_MS: f64 = 1.0;

/// Best-first granules read at survival 1, k=10, zone-HOSTILE posture
/// (fit: the unqualed wide-tlist serial walls, flat across 10M/100M).
pub const G_BF_HOSTILE: f64 = 97.6;

/// Same, zone-FRIENDLY posture (anchored on the real-sorted-bank pair:
/// the walk loses at survival 0.0101, wins at 0.75).
pub const G_BF_FRIENDLY: f64 = 12.5;

/// The bound the g_bf constants were witnessed at; larger bounds scale
/// g_bf linearly (fail-conservative: prices the serial walk higher).
pub const K_REF: f64 = 10.0;

/// Survival floor: below this the estimate classes are unwitnessed and
/// the term clamps (the carve's own losing anchor sits at 0.001).
pub const MIN_SURVIVAL: f64 = 1e-4;

/// Bottom of measured support (rows) for every family: below it the
/// term abstains (`None`) and the shipped carve/floor verdict stands —
/// the n_min_fit fail-toward-the-incumbent posture.
pub const SERIAL_N_MIN: f64 = 1e7;

/// Two engines within this ratio are parity: the census skips them and
/// the pick is not evidence (mirrors the M5-5 ±5% acceptance band).
pub const PARITY_BAND: f64 = 1.05;

/// Bounded top-N shape classes over non-int sort keys — the probe's own
/// vocabulary (key kind x tlist arity), one row per witnessed ladder
/// shape.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TopnKeyClass {
    /// Wide all-column tlist, datetime lead.
    StarWide,
    /// Single-key narrow tlist, datetime lead.
    NarrowTs,
    /// Multi-key (datetime lead + text), narrow tlist.
    MultiKey,
    /// Text (dict-code) leading key, non-datum tlist.
    TextLead,
    /// Text leading key, single-entry (datum) tlist.
    TextDatum,
}

/// One engine's fitted wall curve: `t = setup + rate * N / min(D, cap)`.
#[derive(Clone, Copy, Debug)]
pub struct EngineCurve {
    pub setup: f64,
    pub rate: f64,
    pub cap: f64,
}

impl EngineCurve {
    pub fn wall_ms(&self, rows: f64, dop: i32) -> f64 {
        let d = (dop.max(1)) as f64;
        self.setup + self.rate * rows / d.min(self.cap)
    }
}

/// Fitted constants for one topn-nonint shape class: serial per-granule
/// cost + the parallel competitors' curves (qualed/unqualed variants
/// where the ladder witnessed both).
#[derive(Clone, Copy, Debug)]
pub struct TopnClassModel {
    pub s_gran: f64,
    pub legacy: EngineCurve,
    pub runtime: EngineCurve,
}

/// GENERATED by scripts/serial-term-fit.py — edit the witnessed cells /
/// rerun the fit, never hand-tune (`serial_constants_match_tsv` pins this
/// block against the TSV).
pub fn topn_class_model(class: TopnKeyClass, qualed: bool) -> TopnClassModel {
    match (class, qualed) {
        (TopnKeyClass::StarWide, true) => TopnClassModel {
            s_gran: 0.0449,
            legacy: EngineCurve { setup: 3.51, rate: 1.1235e-5, cap: 13.34 },
            runtime: EngineCurve { setup: 5.85, rate: 5.4943e-6, cap: 14.25 },
        },
        (TopnKeyClass::StarWide, false) => TopnClassModel {
            s_gran: 0.0449,
            legacy: EngineCurve { setup: 3.13, rate: 8.0746e-6, cap: 13.61 },
            runtime: EngineCurve { setup: 4.76, rate: 8.6386e-8, cap: 6.09 },
        },
        (TopnKeyClass::NarrowTs, _) => TopnClassModel {
            s_gran: 0.1943,
            legacy: EngineCurve { setup: 7.10, rate: 5.1631e-5, cap: 13.67 },
            runtime: EngineCurve { setup: 4.65, rate: 1.4015e-7, cap: 6.83 },
        },
        (TopnKeyClass::MultiKey, _) => TopnClassModel {
            s_gran: 0.4639,
            legacy: EngineCurve { setup: 7.38, rate: 5.9300e-5, cap: 13.60 },
            runtime: EngineCurve { setup: 5.56, rate: 1.9453e-7, cap: 8.03 },
        },
        (TopnKeyClass::TextLead, _) => TopnClassModel {
            s_gran: 0.3445,
            legacy: EngineCurve { setup: 4.53, rate: 3.3829e-5, cap: 12.90 },
            runtime: EngineCurve { setup: 1.76, rate: 5.4498e-6, cap: 14.96 },
        },
        (TopnKeyClass::TextDatum, _) => TopnClassModel {
            s_gran: 0.1665,
            legacy: EngineCurve { setup: 3.39, rate: 1.6108e-5, cap: 12.95 },
            runtime: EngineCurve { setup: 1.79, rate: 4.6133e-6, cap: 15.54 },
        },
    }
}

/// Truncated-timestamp grouped fold (the page-fence family): serial fold
/// per-row cost pinned at the LOSING scale point (conservative at
/// mid-scale — overprices the serial fold where it wins anyway) vs the
/// ordered partial-agg exchange (dop-flat over the witnessed cells).
pub const TSTRUNC_S_ROW: f64 = 1.0100e-6;
pub const TSTRUNC_LEG_SETUP: f64 = 37.33;
pub const TSTRUNC_LEG_ROW: f64 = 6.6667e-8;

/// Zone-provable-qual plain fold (the L4 named-cell family): footer META
/// per-granule answer vs either parallel engine's per-row wall.
pub const SCANFOLD_S_META: f64 = 7.3728e-4;
pub const SCANFOLD_SER_SETUP: f64 = 0.2;
pub const SCANFOLD_PAR_ROW: f64 = 9.7200e-6;
pub const SCANFOLD_PAR_CAP: f64 = 12.0;
/// META answerability posture: the qual must be zone-provable per granule
/// — plan-time proxy is an estimated survival at (or mirrored below) 1.
pub const SCANFOLD_META_MIN_SURVIVAL: f64 = 0.999;

/// Bounded top-N over INT keys — the GL-COST-TOPN-1 four-posture grid
/// folded into the term's support (coordinator routing: "serial beats
/// both engines at k=10 — the L4 serial-term gap on this class's own
/// fixture; a 2-engine ratio cannot price a 3-way verdict"). Two
/// WITNESSED k-PLANES (uniform pseudo-random int keys, 2-col tlist,
/// N 250k-10M x dop 1-16, jobs pgrust-fast-tests-27db94812f-17846695
/// {68-376a,77-3d75,84-7436,92-7d0c,97-4b35} @ 27db94812, per-leg
/// witnesses): the ENTIRE k=10 plane is serial-dominant (the zone
/// walk's cutoff skips ~99% of granules — flat 1.2-2.4ms across 40x N);
/// the ENTIRE k=1000 plane is Gather-Merge-dominant. For this family
/// `t_gather` prices the forced GATHER MERGE plan (the class's legacy
/// engine of record post the GL-COST-TOPN-1 guard-off). The k-band
/// BETWEEN the planes is unwitnessed — the verdict abstains there
/// (fail toward the incumbent), and the k-crossover sweep is an L7 cell.
#[derive(Clone, Copy, Debug)]
pub struct TopnPlaneModel {
    pub ser_setup: f64,
    pub ser_row: f64,
    pub gm: EngineCurve,
    pub arm: EngineCurve,
}

/// GENERATED by scripts/serial-term-fit.py (the four-posture cells).
pub fn topn_int_plane(k1000: bool) -> TopnPlaneModel {
    if k1000 {
        TopnPlaneModel {
            ser_setup: 17.9200,
            ser_row: 2.9518e-6,
            gm: EngineCurve { setup: 2.4454, rate: 2.4384e-6, cap: 6.33 },
            arm: EngineCurve { setup: 19.5314, rate: 4.8864e-6, cap: 3.25 },
        }
    } else {
        TopnPlaneModel {
            ser_setup: 1.5474,
            ser_row: 1.0213e-7,
            gm: EngineCurve { setup: 1.3707, rate: 2.2784e-6, cap: 6.37 },
            arm: EngineCurve { setup: 1.7908, rate: 3.4752e-7, cap: 1.59 },
        }
    }
}

/// Witnessed bound planes: k <= 10 rides the k=10 plane, k >= 1000 the
/// k=1000 plane; between them the family abstains.
pub const TOPN_INT_K_LO: f64 = 10.0;
pub const TOPN_INT_K_HI: f64 = 1000.0;
/// Bottom of measured support for this family (rows).
pub const TOPN_INT_N_MIN: f64 = 2.5e5;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EnginePick {
    Serial,
    Runtime,
    Gather,
}

impl EnginePick {
    pub fn name(self) -> &'static str {
        match self {
            EnginePick::Serial => "serial",
            EnginePick::Runtime => "runtime",
            EnginePick::Gather => "gather",
        }
    }
}

/// The three-way shadow verdict: per-engine predicted walls (ms on the
/// reference rig; `t_runtime` None where the arm is unreachable) + the
/// argmin pick + the parity flag (top two within PARITY_BAND — not
/// census evidence).
#[derive(Clone, Copy, Debug)]
pub struct ThreeWay {
    pub t_serial: f64,
    pub t_runtime: Option<f64>,
    pub t_gather: f64,
    pub pick: EnginePick,
    pub parity: bool,
}

fn arbitrate(t_serial: f64, t_runtime: Option<f64>, t_gather: f64) -> ThreeWay {
    let mut best = (EnginePick::Serial, t_serial);
    let mut second = f64::INFINITY;
    let mut consider = |pick, t: f64| {
        if t < best.1 {
            second = best.1;
            best = (pick, t);
        } else if t < second {
            second = t;
        }
    };
    consider(EnginePick::Gather, t_gather);
    if let Some(t) = t_runtime {
        consider(EnginePick::Runtime, t);
    }
    ThreeWay {
        t_serial,
        t_runtime,
        t_gather,
        pick: best.0,
        parity: second <= best.1 * PARITY_BAND,
    }
}

/// Inputs the planner already has at the topn-nonint probe site.
#[derive(Clone, Copy, Debug)]
pub struct TopnShape {
    pub class: TopnKeyClass,
    pub rows: f64,
    pub dop: i32,
    /// Const LIMIT bound (the probe admits 1..=65536 only).
    pub limit: f64,
    /// Estimated qual-survivor fraction (1.0 unqualed).
    pub survival: f64,
    /// Plan-time zone posture PROXY: band-eligible (datetime) lead — the
    /// carve's own conservative proxy; actual friendliness is a data
    /// property only the executor's zone walk sees.
    pub zone_friendly: bool,
}

/// Serial walk granules read for a bounded top-N shape.
pub fn topn_granules_read(shape: &TopnShape) -> f64 {
    let g_total = (shape.rows / GRANULE_ROWS).max(1.0);
    match shape.class {
        // Text leads have no zone vocabulary: the walk is scan-linear.
        TopnKeyClass::TextLead | TopnKeyClass::TextDatum => g_total,
        _ => {
            let g_bf = if shape.zone_friendly { G_BF_FRIENDLY } else { G_BF_HOSTILE };
            let q = shape.survival.clamp(MIN_SURVIVAL, 1.0);
            let k_scale = (shape.limit / K_REF).max(1.0);
            (g_bf * k_scale / q).min(g_total)
        }
    }
}

/// The topn-nonint family three-way. `None` below measured support (the
/// shipped carve/floor verdict stands unchallenged there).
pub fn topn_nonint_three_way(shape: &TopnShape) -> Option<ThreeWay> {
    if shape.rows < SERIAL_N_MIN {
        return None;
    }
    let qualed = shape.survival < 1.0;
    let m = topn_class_model(shape.class, qualed);
    let t_serial = SER_SETUP_MS + m.s_gran * topn_granules_read(shape);
    let t_gather = m.legacy.wall_ms(shape.rows, shape.dop);
    // Band-refusal reachability: on the friendly posture the runtime sort
    // arm refuses to the serial walk — it is not a candidate engine.
    let t_runtime = (!shape.zone_friendly).then(|| m.runtime.wall_ms(shape.rows, shape.dop));
    Some(arbitrate(t_serial, t_runtime, t_gather))
}

/// The int-bounded top-N family three-way on its witnessed k-planes:
/// serial zone walk vs forced Gather Merge (the `Gather` pick) vs the
/// runtime sort arm. Abstains off the planes (k in (10, 1000)), below
/// row support, and for non-positive bounds.
pub fn topn_int_bounded_three_way(rows: f64, dop: i32, limit: f64) -> Option<ThreeWay> {
    if rows < TOPN_INT_N_MIN || limit < 1.0 {
        return None;
    }
    let plane = if limit <= TOPN_INT_K_LO {
        topn_int_plane(false)
    } else if limit >= TOPN_INT_K_HI {
        topn_int_plane(true)
    } else {
        return None; // the unwitnessed k-band — incumbent stands (L7)
    };
    let t_serial = plane.ser_setup + plane.ser_row * rows;
    let t_gather = plane.gm.wall_ms(rows, dop);
    let t_runtime = plane.arm.wall_ms(rows, dop);
    Some(arbitrate(t_serial, Some(t_runtime), t_gather))
}

/// The truncated-timestamp fold family two-way (serial fold vs ordered
/// partial-agg exchange; no runtime arm exists for the shape).
pub fn tstrunc_two_way(rows: f64) -> Option<ThreeWay> {
    if rows < SERIAL_N_MIN {
        return None;
    }
    let t_serial = TSTRUNC_S_ROW * rows;
    let t_gather = TSTRUNC_LEG_SETUP + TSTRUNC_LEG_ROW * rows;
    Some(arbitrate(t_serial, None, t_gather))
}

/// The selective-qual band-eligible-lead carve family (SE-TOPNNI,
/// GL-RESIDUAL-2 re-adjudication): the STARVED serial zone walk vs the
/// kept Gather (Merge) on the REAL sorted banks, wide-tlist (star) shape
/// — the ONLY class with witnessed sub-threshold-survival cells; every
/// other shape in the carve region abstains (incumbent stands). The
/// original unconditional keep-Gather was minted on a cell the current
/// tree INVERTS at BOTH scales (per-draw-stable at the mid-scale bank,
/// growing ~6x by the census bank: the kept plan's parallel scan is
/// width-blind on an all-columns projection while the serial lane reads
/// the filter+sort columns and late-materializes). Serial is priced FLAT
/// at its WORST witnessed wall (it improves toward census scale —
/// conservative); the kept plan is priced through its witnessed cells at
/// the LOW end of the census band.
pub const TOPN_SELQUAL_SER_FLAT: f64 = 126.2;
pub const TOPN_SELQUAL_GM_SETUP: f64 = 24.7721;
pub const TOPN_SELQUAL_GM_RATE: f64 = 1.2306e-5;

/// The carve's priced two-way (star-wide class only; the runtime sort arm
/// band-refuses on the friendly posture, so it is not a candidate).
/// `None` below measured support — the shipped keep-Gather stands there.
pub fn topn_selqual_starwide_two_way(rows: f64) -> Option<ThreeWay> {
    if rows < SERIAL_N_MIN {
        return None;
    }
    let t_serial = TOPN_SELQUAL_SER_FLAT;
    let t_gather = TOPN_SELQUAL_GM_SETUP + TOPN_SELQUAL_GM_RATE * rows;
    Some(arbitrate(t_serial, None, t_gather))
}

/// The zone-provable-qual plain-fold family (single-anchor support: the
/// term abstains unless the META posture holds and N is in support).
pub fn scanfold_meta_three_way(rows: f64, dop: i32, survival: f64) -> Option<ThreeWay> {
    if rows < SERIAL_N_MIN || survival < SCANFOLD_META_MIN_SURVIVAL {
        return None;
    }
    let g_total = (rows / GRANULE_ROWS).max(1.0);
    let t_serial = SCANFOLD_SER_SETUP + SCANFOLD_S_META * g_total;
    let d = (dop.max(1) as f64).min(SCANFOLD_PAR_CAP);
    // Both parallel engines measured at parity on the anchor cell; one
    // curve prices them until the L7 grid splits them.
    let t_par = SCANFOLD_PAR_ROW * rows / d;
    Some(arbitrate(t_serial, Some(t_par), t_par))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The WITNESSED three-way ladder cells (class, qualed-survival,
    // zone_friendly, rows, dop, serial_ms, legacy_ms, runtime_ms) — the
    // SAME table scripts/serial-term-fit.py fits from (provenance in the
    // module doc CALIBRATION block). The synthetic grid's datetime lead
    // is a permutation => zone-HOSTILE; survivals are fixture facts
    // (1/97 for the wide-tlist qual, 0.75 for the text qual, 1.0
    // unqualed).
    const Q_STAR: f64 = 1.0 / 97.0;
    const CELLS: &[(TopnKeyClass, f64, bool, f64, i32, f64, f64, f64)] = &[
        (TopnKeyClass::StarWide, Q_STAR, false, 1e7, 4, 85.1, 31.8, 23.0),
        (TopnKeyClass::StarWide, Q_STAR, false, 1e7, 8, 84.3, 18.2, 13.2),
        (TopnKeyClass::StarWide, Q_STAR, false, 1e7, 16, 84.4, 11.6, 8.7),
        (TopnKeyClass::StarWide, Q_STAR, false, 1e8, 4, 291.1, 274.6, 124.7),
        (TopnKeyClass::StarWide, Q_STAR, false, 1e8, 16, 291.4, 89.6, 46.7),
        (TopnKeyClass::StarWide, 1.0, false, 1e7, 4, 5.1, 23.5, 5.1),
        (TopnKeyClass::StarWide, 1.0, false, 1e7, 8, 5.2, 13.7, 5.0),
        (TopnKeyClass::StarWide, 1.0, false, 1e7, 16, 5.2, 8.8, 4.7),
        (TopnKeyClass::StarWide, 1.0, false, 1e8, 4, 5.6, 198.0, 6.9),
        (TopnKeyClass::StarWide, 1.0, false, 1e8, 16, 5.8, 63.7, 6.2),
        (TopnKeyClass::NarrowTs, 0.75, false, 1e7, 4, 23.6, 132.9, 5.6),
        (TopnKeyClass::NarrowTs, 0.75, false, 1e7, 8, 23.4, 75.7, 4.9),
        (TopnKeyClass::NarrowTs, 0.75, false, 1e7, 16, 23.5, 43.6, 4.3),
        (TopnKeyClass::NarrowTs, 0.75, false, 1e8, 4, 31.5, 1263.6, 8.0),
        (TopnKeyClass::NarrowTs, 0.75, false, 1e8, 16, 30.8, 394.2, 6.8),
        (TopnKeyClass::MultiKey, 0.75, false, 1e7, 4, 53.7, 151.4, 6.9),
        (TopnKeyClass::MultiKey, 0.75, false, 1e7, 8, 53.9, 85.9, 5.7),
        (TopnKeyClass::MultiKey, 0.75, false, 1e7, 16, 55.6, 49.7, 5.2),
        (TopnKeyClass::MultiKey, 0.75, false, 1e8, 4, 74.3, 1458.0, 10.2),
        (TopnKeyClass::MultiKey, 0.75, false, 1e8, 16, 73.0, 453.5, 8.1),
        (TopnKeyClass::TextLead, 0.75, false, 1e7, 4, 420.0, 86.5, 15.3),
        (TopnKeyClass::TextLead, 0.75, false, 1e7, 8, 423.6, 49.0, 8.6),
        (TopnKeyClass::TextLead, 0.75, false, 1e7, 16, 423.1, 30.1, 5.4),
        (TopnKeyClass::TextLead, 0.75, false, 1e8, 4, 4199.9, 839.1, 138.3),
        (TopnKeyClass::TextLead, 0.75, false, 1e8, 16, 4191.4, 271.8, 38.2),
        (TopnKeyClass::TextDatum, 1.0, false, 1e7, 4, 205.7, 42.7, 13.2),
        (TopnKeyClass::TextDatum, 1.0, false, 1e7, 8, 201.9, 24.5, 7.5),
        (TopnKeyClass::TextDatum, 1.0, false, 1e7, 16, 211.4, 15.5, 4.8),
        (TopnKeyClass::TextDatum, 1.0, false, 1e8, 4, 2003.6, 400.3, 118.8),
        (TopnKeyClass::TextDatum, 1.0, false, 1e8, 16, 2004.0, 129.9, 31.3),
    ];

    fn measured_pick(ser: f64, leg: f64, rt: f64) -> (EnginePick, bool) {
        let mut v = [
            (EnginePick::Serial, ser),
            (EnginePick::Gather, leg),
            (EnginePick::Runtime, rt),
        ];
        v.sort_by(|a, b| a.1.total_cmp(&b.1));
        (v[0].0, v[1].1 <= v[0].1 * PARITY_BAND)
    }

    /// EQUIVALENCE ASSERTION (the design §migration-2 discipline applied
    /// to the three-way): at every witnessed ladder cell OUTSIDE the
    /// parity band, the model's argmin must be the measured argmin.
    #[test]
    fn three_way_matches_measured_best_at_witnessed_cells() {
        for &(class, q, z, rows, dop, ser, leg, rt) in CELLS {
            let (meas, meas_parity) = measured_pick(ser, leg, rt);
            if meas_parity {
                continue; // measured tie — not verdict evidence
            }
            let shape = TopnShape { class, rows, dop, limit: 10.0, survival: q, zone_friendly: z };
            let v = topn_nonint_three_way(&shape).expect("in support");
            assert_eq!(
                v.pick, meas,
                "{class:?} q={q} N={rows} D={dop}: measured best {meas:?} \
                 (s={ser} l={leg} r={rt}) but model picks {:?} \
                 (s={:.1} l={:.1} r={:?})",
                v.pick, v.t_serial, v.t_gather, v.t_runtime
            );
        }
    }

    /// CARVE-AGREEMENT PIN — the letter table as a test. At the three
    /// hand-fenced shapes' witnessed anchor cells the three-way shadow
    /// reproduces what the carve enforces; the disagreement set is
    /// exactly the two NAMED residuals the term newly prices (the
    /// unqualed wide-tlist walk at full scale, and the L4 fold cell) —
    /// both are shapes where the shipped route demonstrably leaves the
    /// measured-best serial engine on the table, i.e. the carves are
    /// conservative, never wrong-way.
    #[test]
    fn carve_agreement_at_the_fenced_shapes() {
        // (1) Selective-qual datetime-lead carve, RE-FIT (GL-RESIDUAL-2):
        // below the survival threshold the ENFORCING verdict is now the
        // star-wide selqual two-way — the serial walk wins at BOTH
        // witnessed scales at the current tree (the minting-era losing
        // cell is refuted; the observation shadow below keeps the
        // minting-era record for the census trail).
        let v = topn_selqual_starwide_two_way(1e7).unwrap();
        assert_eq!(v.pick, EnginePick::Serial, "mid-scale selqual cell: {v:?}");
        assert!(!v.parity, "the mid-scale margin is outside the parity band");
        let v = topn_selqual_starwide_two_way(1e8).unwrap();
        assert_eq!(v.pick, EnginePick::Serial, "census-scale selqual cell: {v:?}");
        assert!(!v.parity, "the census margin is outside the parity band");
        assert!(v.t_gather / v.t_serial > 3.0, "census margin must be decisive: {v:?}");
        assert!(topn_selqual_starwide_two_way(9e6).is_none(), "below support abstains");

        // The minting-era OBSERVATION record (shadow constants, unchanged):
        // real-bank anchors (friendly proxy, narrow shape, 10M): survival
        // 0.0101 keeps Gather, survival 0.75 suppresses into the serial
        // walk (band refusal — runtime unreachable).
        let losing = TopnShape {
            class: TopnKeyClass::NarrowTs,
            rows: 1e7,
            dop: 16,
            limit: 10.0,
            survival: 0.0101,
            zone_friendly: true,
        };
        let v = topn_nonint_three_way(&losing).unwrap();
        assert_eq!(v.pick, EnginePick::Gather, "selective friendly anchor: {v:?}");
        assert!(v.t_runtime.is_none(), "band refusal removes the runtime candidate");

        let winning = TopnShape { survival: 0.75, ..losing };
        let v = topn_nonint_three_way(&winning).unwrap();
        assert_eq!(v.pick, EnginePick::Serial, "survival-0.75 friendly anchor: {v:?}");

        // The carve threshold (0.10) sits ABOVE the model's own crossover
        // (the conservative direction): just under the threshold the
        // model already prefers the serial walk.
        let near = TopnShape { survival: 0.09, ..losing };
        assert_eq!(topn_nonint_three_way(&near).unwrap().pick, EnginePick::Serial);

        // (2) The page fence: serial fold wins at the mid-scale bank,
        // the ordered exchange wins at the full-scale bank — and the
        // model's crossover lands INSIDE the witnessed bracket, at the
        // same order as the shipped provisional bound.
        let v = tstrunc_two_way(1e7).unwrap();
        assert_eq!(v.pick, EnginePick::Serial, "mid-scale fold: {v:?}");
        let v = tstrunc_two_way(1e8).unwrap();
        assert_eq!(v.pick, EnginePick::Gather, "full-scale fold: {v:?}");
        let rows_star = TSTRUNC_LEG_SETUP / (TSTRUNC_S_ROW - TSTRUNC_LEG_ROW);
        let pages_star = rows_star * (216_000.0 / 1e7);
        assert!(
            (216_000.0..=2_160_000.0).contains(&pages_star),
            "fold crossover {pages_star:.0} pages outside the witnessed bracket"
        );
        assert!(
            (5e5..=1.5e6).contains(&pages_star),
            "fold crossover {pages_star:.0} pages far from the shipped 1e6 fence"
        );

        // (3) NAMED RESIDUAL 1 — unqualed wide-tlist top-N at full scale,
        // hostile posture: enforcement suppresses onto the runtime arm;
        // the model (and the measured record: 5.6/5.8 serial vs 6.9/6.2
        // runtime) picks the serial walk. The disagreement is the term's
        // reason to exist, not a miscalibration.
        for dop in [4, 16] {
            let shape = TopnShape {
                class: TopnKeyClass::StarWide,
                rows: 1e8,
                dop,
                limit: 10.0,
                survival: 1.0,
                zone_friendly: false,
            };
            let v = topn_nonint_three_way(&shape).unwrap();
            assert_eq!(v.pick, EnginePick::Serial, "unqualed star residual @dop{dop}: {v:?}");
        }

        // (4) NAMED RESIDUAL 2 — the L4 serial-dominance fold cell:
        // enforcement (the two-way curve) suppresses onto the runtime
        // arm; the priced three-way picks the serial META walk, matching
        // the witnessed 1.1ms vs ~8ms record.
        let v = scanfold_meta_three_way(1e7, 16, 1.0).unwrap();
        assert_eq!(v.pick, EnginePick::Serial, "L4 fold cell: {v:?}");
        assert!(
            (0.5..=2.0).contains(&v.t_serial) && (6.0..=10.0).contains(&v.t_gather),
            "L4 anchor drifted: {v:?}"
        );
    }

    /// Everywhere the carve ADMITS (hostile qualed cells at both scales),
    /// the model routes to the runtime arm — suppression's delivered
    /// engine — so retiring the carve flips no admitted cell.
    #[test]
    fn admitted_cells_stay_on_the_runtime_arm() {
        for &(class, q, _, rows, dop, ser, leg, rt) in CELLS {
            if q >= 1.0 || measured_pick(ser, leg, rt).1 {
                continue;
            }
            let shape = TopnShape { class, rows, dop, limit: 10.0, survival: q, zone_friendly: false };
            let v = topn_nonint_three_way(&shape).unwrap();
            assert_eq!(v.pick, EnginePick::Runtime, "{class:?} N={rows} D={dop}: {v:?}");
        }
    }

    /// Below measured support the term abstains — the shipped carves and
    /// floors stand unchallenged (fail toward the incumbent).
    #[test]
    fn below_support_abstains() {
        let shape = TopnShape {
            class: TopnKeyClass::NarrowTs,
            rows: 2e6,
            dop: 4,
            limit: 10.0,
            survival: 0.5,
            zone_friendly: true,
        };
        assert!(topn_nonint_three_way(&shape).is_none());
        assert!(tstrunc_two_way(9e6).is_none());
        assert!(scanfold_meta_three_way(9e6, 16, 1.0).is_none());
        // Non-META posture abstains too (single-anchor support).
        assert!(scanfold_meta_three_way(1e7, 16, 0.5).is_none());
    }

    /// Model-shape sanity: granules read are monotone the right way —
    /// nonincreasing in survival, nondecreasing in the bound and in N;
    /// the friendly posture never reads MORE than the hostile one; and
    /// every serial wall is finite and positive across the grid.
    #[test]
    fn skip_model_is_monotone() {
        let base = TopnShape {
            class: TopnKeyClass::NarrowTs,
            rows: 1e8,
            dop: 4,
            limit: 10.0,
            survival: 0.5,
            zone_friendly: false,
        };
        let g = |s: &TopnShape| topn_granules_read(s);
        for w in [0.001, 0.01, 0.1, 0.5, 0.9].windows(2) {
            let lo = TopnShape { survival: w[0], ..base };
            let hi = TopnShape { survival: w[1], ..base };
            assert!(g(&lo) >= g(&hi), "granules not nonincreasing in survival");
        }
        for w in [10.0, 100.0, 1000.0, 65536.0].windows(2) {
            let lo = TopnShape { limit: w[0], ..base };
            let hi = TopnShape { limit: w[1], ..base };
            assert!(g(&lo) <= g(&hi), "granules not nondecreasing in the bound");
        }
        for w in [1e7, 2.5e7, 1e8, 1e9].windows(2) {
            let lo = TopnShape { rows: w[0], ..base };
            let hi = TopnShape { rows: w[1], ..base };
            assert!(g(&lo) <= g(&hi), "granules not nondecreasing in N");
        }
        let friendly = TopnShape { zone_friendly: true, ..base };
        assert!(g(&friendly) <= g(&base), "friendly posture reads more than hostile");
        for &(class, q, z, rows, dop, ..) in CELLS {
            let shape = TopnShape { class, rows, dop, limit: 10.0, survival: q, zone_friendly: z };
            let v = topn_nonint_three_way(&shape).unwrap();
            assert!(v.t_serial.is_finite() && v.t_serial > 0.0);
            let f_skip = 1.0 - topn_granules_read(&shape) / (rows / GRANULE_ROWS);
            assert!((0.0..=1.0).contains(&f_skip), "skip fraction out of range");
        }
    }

    /// The GL-COST-TOPN-1 four-posture grid (50 witnessed cells; the
    /// int-bounded family's fit table — provenance in the family doc).
    /// (N, dop, limit, serial_ms, gm_ms, runtime_ms).
    const TOPN4P: &[(f64, i32, f64, f64, f64, f64)] = &[
        (2.5e5, 1, 10.0, 1.3, 1.4, 1.3), (2.5e5, 2, 10.0, 1.3, 1.5, 1.3),
        (2.5e5, 4, 10.0, 1.3, 1.5, 1.3), (2.5e5, 8, 10.0, 1.2, 1.5, 1.3),
        (2.5e5, 16, 10.0, 1.3, 1.5, 1.3),
        (1e6, 1, 10.0, 1.9, 3.1, 2.9), (1e6, 2, 10.0, 1.9, 2.4, 2.9),
        (1e6, 4, 10.0, 1.9, 1.9, 2.6), (1e6, 8, 10.0, 1.9, 2.0, 2.5),
        (1e6, 16, 10.0, 1.9, 2.0, 2.5),
        (2.5e6, 1, 10.0, 2.0, 6.0, 3.4), (2.5e6, 2, 10.0, 2.0, 4.4, 2.9),
        (2.5e6, 4, 10.0, 2.0, 3.1, 2.7), (2.5e6, 8, 10.0, 1.9, 2.3, 2.6),
        (2.5e6, 16, 10.0, 1.9, 2.2, 2.6),
        (5e6, 1, 10.0, 2.2, 11.1, 3.8), (5e6, 2, 10.0, 2.2, 7.9, 3.4),
        (5e6, 4, 10.0, 2.2, 5.3, 3.2), (5e6, 8, 10.0, 2.2, 3.6, 3.0),
        (5e6, 16, 10.0, 2.2, 2.8, 3.1),
        (1e7, 1, 10.0, 2.4, 21.0, 4.2), (1e7, 2, 10.0, 2.4, 14.3, 3.7),
        (1e7, 4, 10.0, 2.3, 9.1, 3.4), (1e7, 8, 10.0, 2.3, 5.6, 3.3),
        (1e7, 16, 10.0, 2.4, 4.0, 3.2),
        (2.5e5, 1, 1000.0, 18.2, 2.5, 18.3), (2.5e5, 2, 1000.0, 18.2, 2.5, 18.3),
        (2.5e5, 4, 1000.0, 18.2, 2.5, 18.1), (2.5e5, 8, 1000.0, 18.2, 2.5, 18.3),
        (2.5e5, 16, 1000.0, 18.3, 2.5, 18.3),
        (1e6, 1, 1000.0, 21.0, 4.4, 23.2), (1e6, 2, 1000.0, 21.0, 3.7, 20.7),
        (1e6, 4, 1000.0, 21.0, 3.0, 20.4), (1e6, 8, 1000.0, 21.0, 3.1, 23.5),
        (1e6, 16, 1000.0, 21.0, 3.1, 30.3),
        (2.5e6, 1, 1000.0, 25.7, 7.4, 31.4), (2.5e6, 2, 1000.0, 25.7, 5.8, 24.7),
        (2.5e6, 4, 1000.0, 25.7, 4.4, 22.3), (2.5e6, 8, 1000.0, 25.8, 3.6, 23.7),
        (2.5e6, 16, 1000.0, 25.7, 3.4, 30.6),
        (5e6, 1, 1000.0, 33.7, 12.7, 45.6), (5e6, 2, 1000.0, 33.7, 9.4, 32.1),
        (5e6, 4, 1000.0, 33.8, 6.7, 25.6), (5e6, 8, 1000.0, 33.7, 4.8, 25.2),
        (5e6, 16, 1000.0, 33.8, 4.0, 32.9),
        (1e7, 1, 1000.0, 45.9, 22.7, 67.5), (1e7, 2, 1000.0, 45.9, 15.9, 46.0),
        (1e7, 4, 1000.0, 46.1, 10.7, 32.9), (1e7, 8, 1000.0, 46.0, 7.0, 29.8),
        (1e7, 16, 1000.0, 46.1, 5.3, 34.5),
    ];

    /// EQUIVALENCE at the four-posture grid: at every non-parity cell the
    /// int-bounded family's argmin is the measured argmin — the k=10
    /// plane routes SERIAL everywhere, the k=1000 plane routes GATHER
    /// (Gather Merge) everywhere, and the runtime arm wins zero cells
    /// (the GL-COST-TOPN-1 verdict, now priced three-way).
    #[test]
    fn topn_int_planes_match_the_four_posture_grid() {
        for &(rows, dop, limit, ser, gm, rt) in TOPN4P {
            let (meas, parity) = measured_pick(ser, gm, rt);
            let v = topn_int_bounded_three_way(rows, dop, limit).expect("in support");
            assert_ne!(v.pick, EnginePick::Runtime, "arm must win zero cells");
            if parity || v.parity {
                continue;
            }
            // The source letter's own noise discipline: sub-3ms cells
            // (k=10 at <= 2.5M) carry ~±20% psql overhead — sign-consistent
            // in the record but below the argmin pin's resolution.
            if ser.max(gm).max(rt) < 3.0 {
                continue;
            }
            assert_eq!(
                v.pick, meas,
                "N={rows} D={dop} k={limit}: measured {meas:?} (s={ser} gm={gm} rt={rt}) \
                 model {v:?}"
            );
        }
        // Plane-level pins.
        for &(rows, dop) in &[(1e6, 4), (1e7, 16)] {
            assert_eq!(
                topn_int_bounded_three_way(rows, dop, 10.0).unwrap().pick,
                EnginePick::Serial
            );
            assert_eq!(
                topn_int_bounded_three_way(rows, dop, 1000.0).unwrap().pick,
                EnginePick::Gather
            );
        }
        // Abstention: the unwitnessed k-band, below support, degenerate bound.
        assert!(topn_int_bounded_three_way(1e6, 4, 100.0).is_none());
        assert!(topn_int_bounded_three_way(1e5, 4, 10.0).is_none());
        assert!(topn_int_bounded_three_way(1e6, 4, 0.0).is_none());
    }

    /// Constants of record: the TSV serial-term rows and this module must
    /// not drift apart, every row is witnessed-ab, and the row census is
    /// exactly the fitted set (the witness-census-pin extension for the
    /// serial families).
    #[test]
    fn serial_constants_match_tsv() {
        let tsv = include_str!("../../../../../../crates/backend/optimizer/path/costsize/src/runtime-cost-constants.tsv");
        let mut seen = std::collections::BTreeSet::new();
        for line in tsv.lines() {
            if line.starts_with('#') || line.trim().is_empty() || line.starts_with("class\t") {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            assert_eq!(cols.len(), 11, "malformed TSV row: {line}");
            let (class, term, value, witness) = (cols[0], cols[1], cols[2], cols[10]);
            if !class.starts_with("Serial") {
                continue;
            }
            assert_eq!(
                witness, "witnessed-ab",
                "serial-term row {class}.{term} must be witnessed-ab"
            );
            let got: f64 = value.parse().unwrap_or_else(|_| panic!("bad value {value}"));
            let expect = expected_tsv_value(class, term)
                .unwrap_or_else(|| panic!("unpinned serial-term row {class}.{term}"));
            assert_eq!(got, expect, "{class}.{term}: TSV {got} != code {expect}");
            seen.insert(format!("{class}.{term}"));
        }
        // The full fitted census: no row may vanish or appear by drift.
        let want: Vec<String> = expected_rows();
        let want: std::collections::BTreeSet<String> = want.into_iter().collect();
        assert_eq!(
            seen, want,
            "the serial-term TSV row census changed without this pin"
        );
    }

    fn class_rows(class: TopnKeyClass, qualed: bool, tag: &str) -> Vec<(String, f64)> {
        let m = topn_class_model(class, qualed);
        vec![
            (format!("s_gran_{tag}"), m.s_gran),
            (format!("leg_setup_{tag}"), m.legacy.setup),
            (format!("leg_rate_{tag}"), m.legacy.rate),
            (format!("leg_cap_{tag}"), m.legacy.cap),
            (format!("rt_setup_{tag}"), m.runtime.setup),
            (format!("rt_rate_{tag}"), m.runtime.rate),
            (format!("rt_cap_{tag}"), m.runtime.cap),
        ]
    }

    fn all_pinned() -> Vec<(String, String, f64)> {
        let mut rows: Vec<(String, String, f64)> = vec![
            ("SerialTopnNonInt".into(), "granule_rows".into(), GRANULE_ROWS),
            ("SerialTopnNonInt".into(), "ser_setup_ms".into(), SER_SETUP_MS),
            ("SerialTopnNonInt".into(), "g_bf_hostile".into(), G_BF_HOSTILE),
            ("SerialTopnNonInt".into(), "g_bf_friendly".into(), G_BF_FRIENDLY),
            ("SerialTopnNonInt".into(), "k_ref".into(), K_REF),
            ("SerialTopnNonInt".into(), "n_min_fit".into(), SERIAL_N_MIN),
            ("SerialTsTruncFold".into(), "s_row_serial".into(), TSTRUNC_S_ROW),
            ("SerialTsTruncFold".into(), "leg_setup".into(), TSTRUNC_LEG_SETUP),
            ("SerialTsTruncFold".into(), "leg_rate".into(), TSTRUNC_LEG_ROW),
            ("SerialTsTruncFold".into(), "n_min_fit".into(), SERIAL_N_MIN),
            ("SerialTopnSelQual".into(), "ser_flat_ms".into(), TOPN_SELQUAL_SER_FLAT),
            ("SerialTopnSelQual".into(), "gm_setup".into(), TOPN_SELQUAL_GM_SETUP),
            ("SerialTopnSelQual".into(), "gm_rate".into(), TOPN_SELQUAL_GM_RATE),
            ("SerialTopnSelQual".into(), "n_min_fit".into(), SERIAL_N_MIN),
            ("SerialScanFoldMeta".into(), "s_meta".into(), SCANFOLD_S_META),
            ("SerialScanFoldMeta".into(), "ser_setup_ms".into(), SCANFOLD_SER_SETUP),
            ("SerialScanFoldMeta".into(), "par_rate".into(), SCANFOLD_PAR_ROW),
            ("SerialScanFoldMeta".into(), "par_cap".into(), SCANFOLD_PAR_CAP),
            ("SerialScanFoldMeta".into(), "n_min_fit".into(), SERIAL_N_MIN),
        ];
        for (class, qualed, tag) in [
            (TopnKeyClass::StarWide, true, "star_wide"),
            (TopnKeyClass::StarWide, false, "star_wide_noqual"),
            (TopnKeyClass::NarrowTs, true, "narrow_ts"),
            (TopnKeyClass::MultiKey, true, "multi_key"),
            (TopnKeyClass::TextLead, true, "text_lead"),
            (TopnKeyClass::TextDatum, false, "text_datum"),
        ] {
            for (term, v) in class_rows(class, qualed, tag) {
                rows.push(("SerialTopnNonInt".into(), term, v));
            }
        }
        for (k1000, tag) in [(false, "k10"), (true, "k1000")] {
            let p = topn_int_plane(k1000);
            for (term, v) in [
                (format!("ser_setup_{tag}"), p.ser_setup),
                (format!("ser_row_{tag}"), p.ser_row),
                (format!("gm_setup_{tag}"), p.gm.setup),
                (format!("gm_rate_{tag}"), p.gm.rate),
                (format!("gm_cap_{tag}"), p.gm.cap),
                (format!("arm_setup_{tag}"), p.arm.setup),
                (format!("arm_rate_{tag}"), p.arm.rate),
                (format!("arm_cap_{tag}"), p.arm.cap),
            ] {
                rows.push(("SerialTopnIntBounded".into(), term, v));
            }
        }
        rows.push(("SerialTopnIntBounded".into(), "k_lo".into(), TOPN_INT_K_LO));
        rows.push(("SerialTopnIntBounded".into(), "k_hi".into(), TOPN_INT_K_HI));
        rows.push(("SerialTopnIntBounded".into(), "n_min_fit".into(), TOPN_INT_N_MIN));
        rows
    }

    fn expected_tsv_value(class: &str, term: &str) -> Option<f64> {
        all_pinned()
            .into_iter()
            .find(|(c, t, _)| c == class && t == term)
            .map(|(_, _, v)| v)
    }

    fn expected_rows() -> Vec<String> {
        all_pinned().into_iter().map(|(c, t, _)| format!("{c}.{t}")).collect()
    }
}
