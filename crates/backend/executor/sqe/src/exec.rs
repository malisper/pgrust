//! sqe executor — the Stencil trait + registry wrapping the free-function
//! stencil ABI (`pub fn run_<family>(ctx, node) -> AnswerSet`), plus the
//! tier-1 condition-cache wiring shared by every predicate-bearing
//! stencil.
//!
//! P1-1 reshapes vs the PoC exec.rs: the ABI is the typed AnswerSet (the
//! Vec<String> contract is dead — abi-kill-list.md §5); replay/publish
//! take the engine ctx (faces are per-relation, not process-global); the
//! populate recurrence witness lives on Faces next to the cond store
//! (was a process-global static); populate policy + density gate are
//! SqeConfig fields, not env reads.

use crate::answer::AnswerSet;
use crate::engine::{CacheSidecar, CVerdict, PopulatePolicy, PredCache, SqeCtx, Unit};
use crate::ir::*;
use std::sync::Arc;

/// The stencil ABI: one free function per family (interface contract).
pub type RunFn = fn(&SqeCtx, &PlanNode) -> AnswerSet;

/// The empty-bank answer path (P2-1, R1's new obligation): a zero-part
/// never-ingested relation CANNOT refuse — there is no incumbent to fall
/// to — so the engine answers it directly with the SQL identities:
/// COUNT = 0; SUM/MIN/MAX/AVG = NULL (the validity leg, never a
/// sentinel); grouped/scan shapes = zero rows. Consumed by the dispatch
/// slot before any stencil runs (stencils assume >= 1 part).
pub fn empty_bank_answer(node: &PlanNode) -> AnswerSet {
    use crate::answer::{AnswerCol, Validity};
    // [winserve v1] a window answer over an empty bank is zero rows with
    // the declared column classes (emit columns then function columns).
    if let Some(w) = &node.params.window {
        let mut tys: Vec<crate::typmeta::TypMeta> =
            w.emit.iter().map(|&c| node.ty_of(c)).collect();
        tys.extend(w.funcs.iter().map(|f| f.out));
        return AnswerSet::empty(tys);
    }
    if node.params.group_cols.is_empty() && !node.agg.is_empty() {
        let cols: Vec<AnswerCol> = node
            .agg
            .iter()
            .map(|a| match a.op {
                AggOp::CountStar | AggOp::CountDistinct => {
                    AnswerCol::i64s(a.out, vec![0])
                }
                AggOp::EmitMatches => AnswerCol::i64s(a.out, Vec::new()),
                AggOp::Sum | AggOp::SumShifted | AggOp::SumDistinct => {
                    let mut c = AnswerCol::i128s(a.out, vec![0]);
                    c.validity = Validity::Mask(vec![false]);
                    c
                }
                AggOp::Avg | AggOp::AvgLen | AggOp::AvgCharLen | AggOp::AvgDistinct => {
                    AnswerCol::ratios(a.out, vec![(0, 0)], false)
                }
                AggOp::Min | AggOp::Max | AggOp::BitAnd | AggOp::BitOr => {
                    AnswerCol::i64s_opt(a.out, vec![None])
                }
                AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                    let kind = crate::answer::MomentKind::of_op(a.op).expect("moment op");
                    AnswerCol::moments(a.out, kind, vec![(0, 0, 0)])
                }
                AggOp::MinBytes => {
                    let mut c = crate::answer::BytesBuild::new();
                    c.push(b"");
                    let mut c = c.finish(a.out);
                    c.validity = Validity::Mask(vec![false]);
                    c
                }
                // [sortgrp v1] ungrouped tier-2 aggs over an empty bank
                // answer one NULL row (PG: no non-null inputs => NULL).
                AggOp::StringAgg | AggOp::Mode | AggOp::PercentileDisc
                    if a.in_ty.map(|t| t.is_varlena()).unwrap_or(false) =>
                {
                    let mut c = crate::answer::BytesBuild::new();
                    c.push(b"");
                    let mut c = c.finish(a.out);
                    c.validity = Validity::Mask(vec![false]);
                    c
                }
                AggOp::Mode | AggOp::PercentileDisc => AnswerCol::i64s_opt(a.out, vec![None]),
                AggOp::PercentileCont => {
                    let mut c = AnswerCol::f64s(a.out, vec![0.0]);
                    c.validity = Validity::Mask(vec![false]);
                    c
                }
                AggOp::StringAgg => unreachable!("string_agg input is varlena"),
                AggOp::ArrayAgg | AggOp::ArrayAggDistinct => {
                    let elems = if a.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
                        crate::answer::BytesBuild::new().finish(a.out)
                    } else {
                        AnswerCol::i64s(a.out, Vec::new())
                    };
                    AnswerCol {
                        ty: a.out,
                        data: crate::answer::ColData::List {
                            elems: Box::new(elems),
                            offs: vec![0, 0],
                        },
                        validity: Validity::Mask(vec![false]),
                    }
                }
            })
            .collect();
        // EmitMatches is a zero-row scan shape even in agg clothing.
        if node.agg.iter().any(|a| matches!(a.op, AggOp::EmitMatches)) {
            return AnswerSet::empty(node.agg.iter().map(|a| a.out).collect());
        }
        return AnswerSet::from_cols(cols);
    }
    // Grouped / projection shapes: zero rows with the declared column
    // types (group keys then aggregates, the render law's order).
    let mut tys: Vec<crate::typmeta::TypMeta> = node
        .params
        .group_cols
        .iter()
        .map(|&c| node.ty_of(c))
        .collect();
    tys.extend(node.agg.iter().map(|a| a.out));
    if tys.is_empty() {
        tys = node.col_tys.clone();
    }
    AnswerSet::empty(tys)
}

/// Object-safe wrapper for family lanes that prefer a trait object.
pub trait Stencil: Sync {
    fn family(&self) -> Family;
    fn run(&self, ctx: &SqeCtx, node: &PlanNode) -> AnswerSet;
}

struct FnStencil {
    family: Family,
    f: RunFn,
}

impl Stencil for FnStencil {
    fn family(&self) -> Family {
        self.family
    }
    fn run(&self, ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
        (self.f)(ctx, node)
    }
}

/// The registry. Family lanes: add your `run_<family>` free function
/// here — one line, nothing else changes.
static REGISTRY: &[(Family, RunFn)] = &[
    // Families land in registry order as their stencils port (P1-1
    // incremental landings); the full ten close the port.
    (Family::MetadataAnswer, crate::stencils::metadata::run_metadata_answer),
    (Family::FusedFilterAgg, crate::stencils::fused_filter_agg::run_fused_filter_agg),
    (Family::HashPlaneOwnedGroup, crate::stencils::hash_plane::run_hash_plane_owned_group),
    (Family::TwoLevelCodeAgg, crate::stencils::two_level::run_two_level_code_agg),
    (Family::DenseDomainGroup, crate::stencils::dense_domain::run_dense_domain_group),
    (Family::DistinctPipeline, crate::stencils::distinct::run_distinct_pipeline), // [famA]
    // [famB M1] scan/predicate/window families:
    (Family::WindowReplay, crate::stencils::window_replay::run_window_replay),
    (Family::ZoneOrderWalk, crate::stencils::zone_order::run_zone_order_walk),
    (Family::SurvivorGather, crate::stencils::survivor_gather::run_survivor_gather),
    (Family::DerivedKeyFold, crate::stencils::derived_key::run_derived_key_fold),
    (Family::ScanServe, crate::stencils::scan_serve::run_scan_serve),
    // [sortgrp v1] tier-2 order-sensitive/holistic aggregate family:
    (Family::SortGrouped, crate::stencils::sort_grouped::run_sort_grouped),
    // [winserve v1] SQL window functions over a served scan child:
    (Family::WindowServe, crate::stencils::window_serve::run_window_serve),
];

pub fn registry() -> Vec<Box<dyn Stencil>> {
    REGISTRY
        .iter()
        .map(|&(family, f)| Box::new(FnStencil { family, f }) as Box<dyn Stencil>)
        .collect()
}

pub fn lookup(family: Family) -> Option<RunFn> {
    REGISTRY.iter().find(|&&(f, _)| f == family).map(|&(_, f)| f)
}

/// Execute one node through the registry.
pub fn run_node(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    // Publish the resident pool: every fan-out under this run reuses its
    // workers instead of spawning (the per-execution clone churn).
    let _pool = crate::pool::PoolScope::enter(ctx.pool);
    // [ruling] per-query-run derivation: nothing but condcache survives.
    crate::engine::reset_per_query(ctx.faces);
    let f = lookup(node.family)
        .unwrap_or_else(|| panic!("no stencil registered for family {:?}", node.family));
    let mut a = f(ctx, node);
    // Pushed-down bounded answer: the stencil folded every group; the
    // answer keeps only the spec's top n (native arms already bounded
    // themselves — this trim is then a no-op or a tie-window cut).
    if let Some(t) = &node.params.topk {
        crate::answer::apply_topk(&mut a, t);
    }
    a
}

// ---------------------------------------------------------------------------
// condition-cache wiring (tier-1 rewrite: replay vs recompute)
// ---------------------------------------------------------------------------

/// Replay path election: Some(cache) iff the node's GOAL vector carries
/// the frame fingerprint (an empty goal means the value class was
/// NEGATIVE and no cache legs exist), the engine cache holds verdicts for
/// it (re-checked at execution), and the node is not the honest arm.
pub fn replay_cache(ctx: &SqeCtx, node: &PlanNode) -> Option<Arc<PredCache>> {
    let pred = node.pred.as_ref()?;
    replay_cache_at(ctx, node, &pred.frame_fingerprint())
}

/// [sqe-m2] Replay under an EXPLICIT fingerprint (the final-survivor
/// plane's identity vs the shared frame's).
pub fn replay_cache_at(ctx: &SqeCtx, node: &PlanNode, fp: &ConjFp) -> Option<Arc<PredCache>> {
    if node.params.flags & F_HONEST != 0 {
        return None;
    }
    if !node.params.goal.fingerprints.iter().any(|f| f == fp) {
        return None;
    }
    let pc = ctx.faces.cond_get(fp)?;
    // [oracle, ruling Q4 2026-08-18] hash-as-identity in production;
    // oracle/CI builds structurally verify every hit.
    #[cfg(feature = "oracle")]
    if let Some(pred) = node.pred.as_ref() {
        oracle_verify_cond(pred, fp, &pc);
    }
    Some(pc)
}

/// [oracle, ruling Q4] Structural verification of a condition-cache hit:
/// the cached plane's structural key must equal the structural key the
/// current predicate mints for this fingerprint. Mismatch = the fp
/// function mapped two semantically different predicates to one key —
/// a fingerprint collision or an encoder omission — and replaying the
/// plane would be a wrong answer, so fail LOUD.
#[cfg(feature = "oracle")]
pub(crate) fn oracle_verify_cond(
    pred: &crate::ir::PredSpec,
    fp: &ConjFp,
    cached: &PredCache,
) {
    let current = pred.structural_for(fp).unwrap_or_else(|| {
        panic!(
            "sqe oracle: condition-cache fp {fp} is neither this node's frame nor \
             full predicate identity — identity plumbing bug (predicate: {pred:?})"
        )
    });
    assert!(
        cached.skey == current,
        "sqe oracle: fingerprint collision or encoder omission on the condition \
         cache (fp {fp}):\n  cached structure:  {:?}\n  current structure: {current:?}",
        cached.skey
    );
}

/// Cold-path publication: the stencil computed `verdicts` (aligned with
/// `units`) as a side effect of its fused loop — store them under the
/// canonical frame fingerprint so the NEXT execution (any query sharing
/// the predicate — the hot-shape frame law) replays. No-op when the goal
/// carries no fingerprints (negative value class — MonetDB-recycler
/// admission).
pub fn publish_cache(
    ctx: &SqeCtx,
    node: &PlanNode,
    units: Arc<Vec<Unit>>,
    verdicts: Vec<CVerdict>,
    survivors: u64,
) {
    publish_cache_with(ctx, node, units, verdicts, survivors, None)
}

/// [famB M1] Publication with a typed sidecar (per-granule code lists —
/// the hot-shape cache-payload contract extension).
pub fn publish_cache_with(
    ctx: &SqeCtx,
    node: &PlanNode,
    units: Arc<Vec<Unit>>,
    verdicts: Vec<CVerdict>,
    survivors: u64,
    sidecar: Option<CacheSidecar>,
) {
    let Some(pred) = node.pred.as_ref() else { return };
    let fp = pred.frame_fingerprint();
    publish_cache_at(ctx, node, &fp, units, verdicts, survivors, sidecar);
}

/// [sqe-m2] Publication under an EXPLICIT fingerprint (final-survivor
/// planes). Goal-membership admission as always; first publish wins
/// (cond_put's or_insert — planes are deterministic per fingerprint).
#[allow(clippy::too_many_arguments)]
pub fn publish_cache_at(
    ctx: &SqeCtx,
    node: &PlanNode,
    fp: &ConjFp,
    units: Arc<Vec<Unit>>,
    verdicts: Vec<CVerdict>,
    survivors: u64,
    sidecar: Option<CacheSidecar>,
) {
    if !node.params.goal.fingerprints.iter().any(|f| f == fp) {
        return;
    }
    // A canceled statement publishes nothing (its planes may be partial)
    // and must not bump the recurrence witness.
    if crate::cancel::stop_requested() {
        return;
    }
    // [coldstart] populate admission (Michael's rung, 2026-08-17): the
    // FIRST execution of a fingerprint computes and does NOT populate (no
    // recurrence witness yet); the second execution populates, the third
    // replays. Also refuse ~non-selective planes outright (survivors/rows
    // above the density cap: decode_sel-per-verdict replay is SLOWER than
    // the fused recompute — the M5 u6 finding).
    let rows: u64 = units.iter().map(|u| u.2 as u64).sum();
    let admit = populate_admitted(ctx, fp, survivors, rows);
    crate::coldledger::note(
        if admit { "cond_populate" } else { "cond_skip" },
        format!("{fp}|survivors={survivors}|rows={rows}"),
        std::time::Instant::now(),
        0,
        if admit { crate::coldledger::Reason::SecondTouch } else { crate::coldledger::Reason::CheaperThanPlain },
    );
    if !admit {
        return;
    }
    #[cfg(feature = "oracle")]
    let skey = {
        let pred = node.pred.as_ref().expect("publish with a goal fp requires a predicate");
        pred.structural_for(fp).unwrap_or_else(|| {
            panic!(
                "sqe oracle: publishing under fp {fp} which is neither this node's \
                 frame nor full predicate identity (predicate: {pred:?})"
            )
        })
    };
    ctx.faces.cond_put(
        fp,
        PredCache {
            units,
            v: verdicts,
            survivors,
            sidecar,
            #[cfg(feature = "oracle")]
            skey,
        },
    );
}

/// [oracle, ruling Q4] Unit gate on the verify helper: a forged encoder
/// omission (two semantically different predicates carrying the same
/// fingerprint) must panic; the genuine predicate must verify clean.
#[cfg(all(test, feature = "oracle"))]
mod oracle_tests {
    use super::*;
    use crate::ir::{CmpOp, PredSpec, PredTerm};
    use crate::typmeta::TypMeta;

    fn plane(skey: crate::ir::StructuralPred) -> PredCache {
        PredCache {
            units: Arc::new(Vec::new()),
            v: Vec::new(),
            survivors: 0,
            sidecar: None,
            skey,
        }
    }

    #[test]
    fn forged_omission_panics_and_genuine_hit_passes() {
        let ty = TypMeta::INT8;
        let a = PredSpec::all(vec![PredTerm::new(1, CmpOp::Eq, 5, 0, ty)]);
        // The forge: same column, DIFFERENT constant, fp deliberately
        // stale — what an encoder omitting the constant would mint.
        let mut b_term = PredTerm::new(1, CmpOp::Eq, 7, 0, ty);
        b_term.fp = a.terms[0].fp.clone();
        let b = PredSpec::all(vec![b_term]);
        let fp = a.frame_fingerprint();
        assert_eq!(fp, b.frame_fingerprint(), "forge premise: identical fingerprint");

        let cached = plane(a.frame_structural());
        // Genuine hit: clean.
        oracle_verify_cond(&a, &fp, &cached);
        // Forged hit: loud.
        let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            oracle_verify_cond(&b, &fp, &cached)
        }));
        let err = got.expect_err("verify must panic on the forged omission");
        let msg = err
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            msg.contains("fingerprint collision or encoder omission"),
            "panic must name the failure class, got: {msg}"
        );
    }

    #[test]
    fn structural_key_is_order_canonical() {
        let ty = TypMeta::INT8;
        let t1 = PredTerm::new(1, CmpOp::Eq, 5, 0, ty);
        let t2 = PredTerm::new(2, CmpOp::Between, 10, 20, ty);
        let p12 = PredSpec::all(vec![t1.clone(), t2.clone()]);
        let p21 = PredSpec::all(vec![t2, t1]);
        assert_eq!(p12.frame_fingerprint(), p21.frame_fingerprint());
        assert_eq!(p12.frame_structural(), p21.frame_structural());
        // The wired path accepts either ordering against one plane.
        let cached = plane(p12.frame_structural());
        oracle_verify_cond(&p21, &p21.frame_fingerprint(), &cached);
    }
}

/// Recurrence witness: per-fingerprint execution count on this relation's
/// faces (bumped per publish attempt = per execution of a plan that would
/// populate it).
fn populate_admitted(ctx: &SqeCtx, fp: &ConjFp, survivors: u64, rows: u64) -> bool {
    let n = ctx.faces.touch_bump(fp);
    match ctx.faces.cfg.populate {
        PopulatePolicy::First => true,
        PopulatePolicy::Never => false,
        PopulatePolicy::Second => {
            let dense =
                rows > 0 && (survivors as f64) > ctx.faces.cfg.condcache_max_density * rows as f64;
            n >= 2 && !dense
        }
    }
}
