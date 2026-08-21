//! # The prune chokepoint + the runtime install face (M4-S5)
//!
//! The SINGLE prune plane of the S5 landing (plan §S5: "single-chokepoint
//! prune plane, leader-derives-once, Ne arm from birth" — v3-2, planned-5;
//! the runtime-install face IN the chokepoint contract makes the #728
//! class impossible):
//!
//! - **Leader-derives-once:** [`PrunePlane::derive`] evaluates the
//!   installed predicate set over every (part, granule) zone/PSMA/bloom
//!   plane EXACTLY ONCE, before any claim is issued, producing
//!   [`PruneVerdicts`] — the survivor unit list that BECOMES the claim
//!   space ([`crate::SurvivorSpans`]). A verdict-eliminated granule never
//!   claims, never pins, never costs a claim-CAS (the S3 posture — claim
//!   first, consult after — is retired for consumers of this plane).
//! - **PSMA windows narrow claims:** the derive intersects per-predicate
//!   candidate windows per granule; the scan stages only windows that
//!   intersect the candidate range (the v2-19 subtractive compose,
//!   supplied at claim grain instead of re-derived per worker).
//! - **THE ONE CHOKEPOINT:** every granule a worker is about to stage
//!   passes [`PrunePlane::admit`] exactly once. Predicates present at
//!   derive time were already consumed by the claim space; predicates
//!   INSTALLED MID-EXECUTION (the runtime face — bloom/Ne producers land
//!   M5e/M6; the synthetic probe proves the face now) are consulted here,
//!   so an install between two claims prunes every not-yet-staged granule
//!   on every worker, structurally. There is no second consult site to
//!   forget (#728).
//! - **Census law:** derive-time eliminations are schedule-INDEPENDENT
//!   and fold into [`crate::ScanCensus::granules_zone_pruned`] (PC-6.3
//!   identity holds). Post-claim eliminations by late installs are
//!   schedule-DEPENDENT by nature and are witnessed on the plane's own
//!   [`PrunePlane::late_pruned`] counter (the PinWitness class: reported,
//!   never identity-compared).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use pgsync::Mutex;

use pgrc2_format::geom::rows_in_granule_at;
use pgrc2_format::meta::Verdict;
use pgrc2_meta::census::{evaluate_censused, psma_candidates_eq_censused, MetaEngagement};
use pgrc2_meta::key::{TypedKey, TypedKeys};
use pgrc2_meta::lower::{lower_const, ConstInput, LoweredConst};
use pgrc2_meta::profile::MetaProfile;
use pgrc2_meta::psma;
use pgrc2_meta::verdict::{grain_keys, BloomEvidence, GrainFacts, ZonePredicate};
use pgrc2_read::{OpenPart, ReadResult};

use crate::meta::{ColumnMeta, ScanConst};

/// The prune-plane operator vocabulary (the zone-verdict shapes the S5
/// lowering supplies: the general conjunct class + the P-9 SAOP route +
/// planned-5's Ne).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// SAOP/IN-list (P-9): consts are the member set.
    InSet,
}

/// One installed prune predicate: a single-column zone probe with its
/// owned constants and the column's meta profile (the caller's catalog
/// fact — the same derivation the builder used).
#[derive(Debug, Clone)]
pub struct PrunePred {
    pub attno: u32,
    pub op: PruneOp,
    /// One constant for the cmp ops; the member set for `InSet`.
    pub consts: Vec<ScanConst>,
    pub profile: MetaProfile,
}

/// The leader's derive output: the claim space + its witnesses.
#[derive(Debug, Default)]
pub struct PruneVerdicts {
    /// Ascending global granule units that survived every derived
    /// predicate — THE claim space input of [`crate::SurvivorSpans`].
    pub survivors: Vec<u64>,
    /// Granules eliminated at derive (never claimed) — folds into
    /// `ScanCensus::granules_zone_pruned` (schedule-independent).
    pub eliminated: u64,
    /// Intersected PSMA candidate window per surviving unit, granule-
    /// relative `[lo, hi)`; absent = stage every window.
    pub windows: HashMap<u64, (u32, u32)>,
    /// Zone/PSMA/bloom engagement attribution (XC-5), leader-derived.
    pub meta: MetaEngagement,
    /// How many installed predicates the derive consumed (the admit
    /// chokepoint consults only the suffix installed after this).
    pub preds_consumed: usize,
}

/// The statement-grain prune plane. Constructed at lowering, shared by
/// every worker; the ONE install face and the ONE admit chokepoint.
pub struct PrunePlane {
    parts: Vec<Arc<OpenPart>>,
    preds: Mutex<Vec<PrunePred>>,
    /// Total installed predicates (fast-path read: == derived ⇒ nothing
    /// late to consult).
    installed: AtomicUsize,
    /// Predicates consumed by the derive.
    derived: AtomicUsize,
    /// Schedule-dependent witness: granules eliminated at the chokepoint
    /// by predicates installed after derive (the runtime-install probe's
    /// evidence surface).
    late_pruned: AtomicU64,
    /// Lazily loaded meta planes for late-pred consult, keyed by
    /// (part index, attno).
    late_meta: Mutex<HashMap<(usize, u32), Arc<ColumnMeta>>>,
}

impl PrunePlane {
    pub fn new(parts: Vec<Arc<OpenPart>>) -> PrunePlane {
        PrunePlane {
            parts,
            preds: Mutex::new(Vec::new()),
            installed: AtomicUsize::new(0),
            derived: AtomicUsize::new(0),
            late_pruned: AtomicU64::new(0),
            late_meta: Mutex::new(HashMap::new()),
        }
    }

    /// THE INSTALL FACE. Legal at any time: before [`derive`](Self::derive)
    /// (the lowering-grain path — the predicate joins the claim space) or
    /// mid-execution (the runtime face — the predicate takes effect at the
    /// admit chokepoint for every granule not yet staged).
    pub fn install(&self, pred: PrunePred) {
        let mut preds = self.preds.lock().unwrap();
        preds.push(pred);
        self.installed.store(preds.len(), Ordering::Release);
    }

    /// Leader-derives-once: consume the currently installed predicates
    /// against every (part, granule) meta plane and produce the claim
    /// space. Callers run this exactly once, before issuing claims.
    pub fn derive(&self) -> ReadResult<PruneVerdicts> {
        let preds: Vec<PrunePred> = self.preds.lock().unwrap().clone();
        let mut out = PruneVerdicts {
            preds_consumed: preds.len(),
            ..PruneVerdicts::default()
        };
        self.derived.store(preds.len(), Ordering::Release);
        let mut unit = 0u64;
        for part in &self.parts {
            let granules = part.footer().granule_count;
            // Load each predicate column's meta plane once per part.
            let mut metas: Vec<ColumnMeta> = Vec::with_capacity(preds.len());
            for p in &preds {
                metas.push(ColumnMeta::load(part, p.profile, p.attno)?);
            }
            for g in 0..granules {
                let rows_g = rows_in_granule_at(part.rows(), part.grain(), g);
                let mut window: Option<(u32, u32)> = None;
                let mut eliminated = false;
                for (p, meta) in preds.iter().zip(metas.iter()) {
                    match consult_pred(p, meta, g, rows_g, granules, &mut out.meta) {
                        PredConsult::Eliminate => {
                            eliminated = true;
                            break;
                        }
                        PredConsult::Window(w) => {
                            window = Some(match window {
                                None => w,
                                Some((alo, ahi)) => (alo.max(w.0), ahi.min(w.1)),
                            });
                            if let Some((lo, hi)) = window {
                                if lo >= hi {
                                    // Empty candidate intersection: no row
                                    // can match — the granule never claims.
                                    eliminated = true;
                                    break;
                                }
                            }
                        }
                        PredConsult::Scan => {}
                    }
                }
                if eliminated {
                    out.eliminated += 1;
                } else {
                    if let Some(w) = window {
                        out.windows.insert(unit, w);
                    }
                    out.survivors.push(unit);
                }
                unit += 1;
            }
        }
        Ok(out)
    }

    /// THE CHOKEPOINT: every about-to-stage granule passes here exactly
    /// once. `false` = a predicate installed AFTER the derive eliminated
    /// it (claimed, never staged; counted on [`Self::late_pruned`]).
    /// With no late installs this is one relaxed load.
    pub fn admit(&self, pidx: usize, g: u32, rows_g: u32) -> ReadResult<bool> {
        let derived = self.derived.load(Ordering::Acquire);
        if self.installed.load(Ordering::Acquire) == derived {
            return Ok(true);
        }
        let late: Vec<PrunePred> = {
            let preds = self.preds.lock().unwrap();
            preds[derived.min(preds.len())..].to_vec()
        };
        let granules = self.parts[pidx].footer().granule_count;
        let mut census = MetaEngagement::default();
        for p in &late {
            let meta = self.late_meta_for(pidx, p)?;
            if matches!(
                consult_pred(p, &meta, g, rows_g, granules, &mut census),
                PredConsult::Eliminate
            ) {
                self.late_pruned.fetch_add(1, Ordering::Relaxed);
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Schedule-dependent late-prune witness (reported, never
    /// identity-compared — the PinWitness class).
    pub fn late_pruned(&self) -> u64 {
        self.late_pruned.load(Ordering::Relaxed)
    }

    fn late_meta_for(&self, pidx: usize, pred: &PrunePred) -> ReadResult<Arc<ColumnMeta>> {
        let key = (pidx, pred.attno);
        if let Some(m) = self.late_meta.lock().unwrap().get(&key) {
            return Ok(m.clone());
        }
        let loaded = Arc::new(ColumnMeta::load(&self.parts[pidx], pred.profile, pred.attno)?);
        self.late_meta.lock().unwrap().entry(key).or_insert_with(|| loaded.clone());
        Ok(loaded)
    }
}

/// One predicate's per-granule consult outcome.
enum PredConsult {
    /// Zone/bloom verdict (or an empty PSMA candidate set) erased the
    /// granule.
    Eliminate,
    /// Scan, restricted to the candidate window `[lo, hi)`.
    Window((u32, u32)),
    /// Scan; this predicate contributes no narrowing.
    Scan,
}

fn const_input(c: &ScanConst) -> ConstInput<'_> {
    match c {
        ScanConst::Word(w) => ConstInput::Word(*w),
        ScanConst::VarlenaImage(img) => ConstInput::VarlenaImage(img),
    }
}

/// Consult one predicate over one granule: zone/bloom verdict first, then
/// the PSMA candidate window (subtractive). Abstained constants consult
/// nothing (advisory-only — abstention is always sound).
fn consult_pred(
    pred: &PrunePred,
    meta: &ColumnMeta,
    g: u32,
    rows_g: u32,
    granule_count: u32,
    census: &mut MetaEngagement,
) -> PredConsult {
    let Some(rec) = meta.granule_record(g) else {
        return PredConsult::Scan;
    };
    // Lower every constant; any abstention ⇒ no verdict from this pred.
    let mut lowered: Vec<LoweredConst<'_>> = Vec::with_capacity(pred.consts.len());
    for c in &pred.consts {
        match lower_const(&pred.profile, const_input(c)).lowered() {
            Some(lc) => lowered.push(lc),
            None => return PredConsult::Scan,
        }
    }
    let probe = match (pred.op, lowered.as_slice()) {
        (PruneOp::Eq, [c]) => ZonePredicate::Eq(*c),
        (PruneOp::Ne, [c]) => ZonePredicate::Ne(*c),
        (PruneOp::Lt, [c]) => ZonePredicate::Lt(*c),
        (PruneOp::Le, [c]) => ZonePredicate::Le(*c),
        (PruneOp::Gt, [c]) => ZonePredicate::Gt(*c),
        (PruneOp::Ge, [c]) => ZonePredicate::Ge(*c),
        (PruneOp::InSet, members) => ZonePredicate::InSet(members),
        // A cmp op without exactly one constant is a caller defect;
        // consult nothing (advisory-only).
        _ => return PredConsult::Scan,
    };
    let bloom_ev = meta.bloom_body.as_deref().and_then(|body| {
        pgrc2_meta::bloom::bloom_block_for(body, granule_count, g)
            .ok()
            .flatten()
            .map(|(k, block)| BloomEvidence { k, block })
    });
    let v = evaluate_censused(
        &pred.profile,
        GrainFacts { rows: rows_g as u64 },
        &rec,
        &probe,
        bloom_ev,
        census,
    );
    if matches!(v, Verdict::AllFail) {
        return PredConsult::Eliminate;
    }
    // PSMA windows: exact keys only (the measured-only law), Eq and the
    // ordered comparisons + the InSet hull; Ne has no candidate shape.
    let window = (|| {
        let body = meta.psma_body.as_deref()?;
        let keys = grain_keys(&pred.profile, &rec);
        let (min_key, max_key) = match keys {
            TypedKeys::Exact { min, max } => (min.raw(), max.raw()),
            _ => return None,
        };
        let block = psma::psma_block_for(body, granule_count, g).ok().flatten()?;
        let exact = |lc: &LoweredConst<'_>| match lc.key {
            Some(TypedKey::Exact(k)) => Some(k.raw()),
            _ => None,
        };
        match pred.op {
            PruneOp::Eq => {
                let k = exact(&lowered[0])?;
                psma_candidates_eq_censused(block, min_key, max_key, k, rows_g, census)
                    .map(|(lo, hi)| (lo as u32, hi as u32))
            }
            PruneOp::Lt | PruneOp::Le => {
                let k = exact(&lowered[0])?;
                // Le/Lt share the bucket hull (bucket index is monotone;
                // over-approximation is sound — advisory-only).
                psma::psma_candidates_range(block, min_key, max_key, min_key, k)
                    .map(|(lo, hi)| (lo as u32, hi as u32))
            }
            PruneOp::Gt | PruneOp::Ge => {
                let k = exact(&lowered[0])?;
                psma::psma_candidates_range(block, min_key, max_key, k, max_key)
                    .map(|(lo, hi)| (lo as u32, hi as u32))
            }
            PruneOp::InSet => {
                // Member-window hull: every member must resolve.
                let mut hull: Option<(u32, u32)> = None;
                for lc in &lowered {
                    let k = exact(lc)?;
                    let (lo, hi) =
                        psma_candidates_eq_censused(block, min_key, max_key, k, rows_g, census)?;
                    if lo < hi {
                        hull = Some(match hull {
                            None => (lo as u32, hi as u32),
                            Some((alo, ahi)) => (alo.min(lo as u32), ahi.max(hi as u32)),
                        });
                    }
                }
                // All members proven absent by PSMA ⇒ empty window.
                Some(hull.unwrap_or((0, 0)))
            }
            PruneOp::Ne => None,
        }
    })();
    match window {
        Some(w) => PredConsult::Window(w),
        None => PredConsult::Scan,
    }
}

#[cfg(test)]
mod prune_tests {
    use super::*;

    /// The install face is append-only and the fast path stays lock-free
    /// shaped: derive consumes the installed set, admit with no late
    /// installs answers true without consulting anything.
    #[test]
    fn admit_fast_path_without_late_installs() {
        let plane = PrunePlane::new(Vec::new());
        let v = plane.derive().expect("empty derive");
        assert_eq!(v.preds_consumed, 0);
        assert_eq!(v.eliminated, 0);
        assert!(v.survivors.is_empty());
        assert_eq!(plane.late_pruned(), 0);
    }
}
