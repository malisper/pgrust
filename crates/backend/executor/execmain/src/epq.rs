// EvalPlanQual (execMain.c). Divergence: no child EState — the recheck tree
// runs against the parent estate (shared tuple table, shared subplan cells).
// Each EPQ owner (ModifyTable/LockRows) holds its own relsubs arrays
// (C EPQState.relsubs_*), swapped into estate.es_epq only while its recheck
// runs, so nested EPQ (a LockRows inside a recheck) never clobbers the outer
// run's per-rel state.
//
// WS-U wave-5 inc-1 (docs/design/lane-epq.md): the module is factored into
// named seam functions mapped 1:1 onto C's EvalPlanQual* entry points —
// `eval_plan_qual` (EvalPlanQual), `eval_plan_qual_begin` (EvalPlanQualBegin),
// `eval_plan_qual_start` (EvalPlanQualStart), `eval_plan_qual_slot`
// (EvalPlanQualSlot), `eval_plan_qual_next` (EvalPlanQualNext),
// `eval_plan_qual_end` (EvalPlanQualEnd). Pure code moves; the rowmark arm
// (EvalPlanQualFetchRowMark) lives with the scan dispatch in execscan
// (`epq_fetch`/`epq_fetch_row_mark`), where C's ExecScanFetch calls it.
// Inc-5 (EPQ capture) hangs its admission chokepoint here without moving
// these seams again.

use crate::procnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
use ::executils::{EStateData, EpqSubs, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::{Node, NodeTag};

pub struct EpqState<'mcx> {
    pub plan: Option<Node<'mcx>>,
    pub recheck: Option<PlanStateNode<'mcx>>,
    pub result_rti: u32,
    /// WS-Y wave-7 (Y1): the memoized per-node lane-admission verdicts for
    /// `plan` — classified ONCE per recheck plan by
    /// `lanev2::epq::epq_recheck_admission` (knob-ON only; stays None
    /// forever on the OFF arm). The plan is fixed at plan init (procnode.rs
    /// builds EpqState once), so this cache never goes stale; a future
    /// dynamic SetPlan owner must reset it alongside `recheck`.
    pub(crate) lane_verdicts: Option<crate::lanev2::epq::EpqPlanVerdicts<'mcx>>,
}

/// `EvalPlanQual`: `Some` = new candidate tuple, `None` = skip the row.
/// `subs` is the owner's relsubs (test slots parked by EvalPlanQualSlot).
pub fn eval_plan_qual<'mcx>(
    epq: &mut EpqState<'mcx>,
    subs: &mut Option<EpqSubs<'mcx>>,
    estate: &mut EStateData<'mcx>,
    inputslot: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    // Inc-5's admission chokepoint (WS-U wave-5 structure, WS-Y wave-7
    // widening): `PGRUST_LANE_V2_EPQ` default-OFF; ON classifies the
    // recheck plan into per-node verdicts ONCE (memoized in
    // `epq.lane_verdicts` — one classification per recheck plan, never per
    // recheck row) and, while the es_epq_active HARD LAW stands (the Y3
    // lift is census-gated and did not land at wave-7), still refuses
    // every mappable shape through the existing `epq` carrier per
    // initiation — the drive below stays Volcano either way. The OFF arm
    // is one relaxed byte load at recheck INITIATION only (never
    // per-row/per-batch; §0.6 idiom).
    if crate::lanev2::epq_lane_enabled() {
        crate::lanev2::epq::epq_recheck_admission(
            epq.plan,
            &mut epq.lane_verdicts,
            estate.es_query_cxt,
        );
    }
    ::executils::ensure_epq_subs(subs, estate.es_query_cxt, estate.epq_rtsize(), epq.result_rti);
    let saved_subs = core::mem::replace(&mut estate.es_epq, subs.take());
    let saved_active = estate.es_epq_active;
    estate.es_epq_active = true;
    let r = eval_plan_qual_guts(epq, estate, inputslot);
    estate.es_epq_active = saved_active;
    *subs = core::mem::replace(&mut estate.es_epq, saved_subs);
    r
}

fn eval_plan_qual_guts<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
    inputslot: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    eval_plan_qual_begin(epq, estate)?;

    let idx = (epq.result_rti - 1) as usize;
    let testslot = eval_plan_qual_slot(epq, estate)?;
    if testslot != inputslot {
        let (dst, src) = slot_pair_mut(estate, testslot, inputslot);
        exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    }

    // C EvalPlanQual: mark that an EPQ tuple is available for this relation
    // (other result relations remain marked as having no tuple available).
    {
        let subs = estate.es_epq.as_mut().expect("EPQ state installed");
        subs.relsubs_done[idx] = false;
        subs.relsubs_blocked[idx] = false;
    }

    let slot = eval_plan_qual_next(epq, estate)?;

    if let Some(s) = slot {
        // A projection-less recheck would hand back the test slot, which the
        // clear below destroys (real subplans project: junk ctid).
        assert_ne!(s, testslot, "EvalPlanQual (execMain.c): recheck returned the test slot");
        exectuples::exec_materialize_slot(estate.slot_mut(s), mcx)?;
    }

    exectuples::exec_clear_tuple(estate.slot_mut(testslot), mcx);
    estate.es_epq.as_mut().expect("EPQ state installed").relsubs_blocked[idx] = true;

    Ok(slot)
}

/// `EvalPlanQualSlot` (execMain.c): the rel's per-rti test slot, made on
/// first use. The trigger path reaches here without a parked slot; DML
/// paths park the input via the owner-held relsubs before the recheck.
pub(crate) fn eval_plan_qual_slot<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    let mcx = estate.es_query_cxt;
    let idx = (epq.result_rti - 1) as usize;
    if estate.es_epq.as_ref().expect("EPQ state installed").relsubs_slot[idx].is_none() {
        let (kind, desc) = {
            let rel = estate.es_relations[idx].as_ref().expect("EPQ relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
        estate.es_tupleTable.push(slot);
        estate.es_epq.as_mut().expect("EPQ state installed").relsubs_slot[idx] = Some(id);
    }
    Ok(estate.es_epq.as_ref().expect("EPQ state installed").relsubs_slot[idx]
        .expect("just ensured"))
}

/// `EvalPlanQualNext` (execMain.c): one pull of the recheck tree — the EPQ
/// query returns at most one tuple (every scan under it substitutes its
/// test slot exactly once via the relsubs_done latch).
pub(crate) fn eval_plan_qual_next<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    exec_proc_node(epq.recheck.as_mut().expect("begun"), estate)
}

/// `EvalPlanQualBegin` (execMain.c): reset+rescan an already-built recheck
/// tree (relsubs_done reloaded from relsubs_blocked, so result rels with no
/// parked tuple stay blocked), or fall to the first-run Start arm.
pub(crate) fn eval_plan_qual_begin<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(recheck) = epq.recheck.as_mut() {
        let subs = estate.es_epq.as_mut().expect("EPQ state installed");
        for i in 0..subs.relsubs_done.len() {
            subs.relsubs_done[i] = subs.relsubs_blocked[i];
        }
        return crate::execami::exec_re_scan(recheck, estate);
    }
    eval_plan_qual_start(epq, estate)
}

/// `EvalPlanQualStart` (execMain.c): first-run initialization of the
/// recheck plan tree. Divergence from C is deliberate and pinned: C builds
/// a child EState here (parentestate/recheckestate split); pgrust inits the
/// tree against the parent estate under the swapped-in `EpqSubs`
/// (docs/design/lane-epq.md §3 — the capture-model decision).
pub(crate) fn eval_plan_qual_start<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let plan = epq.plan.expect("ModifyTable has a subplan");
    check_epq_plan(plan);
    debug_assert!(estate.es_epq.is_some(), "EvalPlanQualSlot precedes EvalPlanQual");
    // Recheck planstates are never reported: init uninstrumented so EPQ
    // reruns don't double-count into the main tree's es_instrumentation
    // (C gives the child estate throwaway per-planstate Instrumentation).
    let saved_instrument = core::mem::replace(&mut estate.es_instrument, 0);
    let inited = exec_init_node(Some(plan), estate, 0);
    estate.es_instrument = saved_instrument;
    epq.recheck = Some(inited?.expect("recheck subplan"));
    Ok(())
}

// The recheck tree re-runs against the parent estate; every node in it must
// have exercised EPQ rescan semantics. Scans substitute the test tuple via
// ExecScanFetch; joins/sorts/materials rescan their children.
//
// WS-U wave-5 (contract §6.2c) / WS-Y wave-7 (rung Y2): this whitelist is
// THE LOUD ADMISSION LIST for inc-5 (EPQ capture, LAST in the program).
// Before the `es_epq_active` refusal is lifted (rung Y3, census-gated —
// CARRIED at wave-7), every shape admitted here must be census-green on
// WS-P's read-side coverage census — a recheck plan can contain any read
// shape, and a mid-recheck refusal would mean a mixed-engine recheck. The
// TAG list admits nothing new at wave-7; additions are the documented
// loud-admission-list deliverable, one reviewed act per shape, each tied to
// census evidence (docs/design/lane-epq.md §5/§6). Wave-7 tightenings, both
// loud-refusal-only (nothing newly admitted):
//   * scanrelid == 0 pushed-down-join scans refuse LOUDLY until a spec
//     exercises them (lane-epq.md §2's recorded FDW gap; the refused-tag
//     arm already catches ForeignScan/CustomScan, this pins the invariant
//     for every ADMITTED scan tag too);
//   * SubqueryScan recurses into its subplan (the tag whitelist previously
//     stopped at the SubqueryScan node, silently admitting any shape
//     underneath — an honesty gap in the loud list, not a new admission).
pub(crate) fn check_epq_plan(plan: Node<'_>) {
    let ok = matches!(
        plan.node_tag(),
        NodeTag::T_Append
            | NodeTag::T_SeqScan
            | NodeTag::T_TidScan
            | NodeTag::T_TidRangeScan
            | NodeTag::T_IndexScan
            | NodeTag::T_IndexOnlyScan
            | NodeTag::T_BitmapHeapScan
            | NodeTag::T_BitmapIndexScan
            | NodeTag::T_NestLoop
            | NodeTag::T_MergeJoin
            | NodeTag::T_HashJoin
            | NodeTag::T_Hash
            | NodeTag::T_Sort
            | NodeTag::T_Material
            | NodeTag::T_Result
            | NodeTag::T_ValuesScan
            | NodeTag::T_CteScan
            | NodeTag::T_SubqueryScan
            | NodeTag::T_FunctionScan
            | NodeTag::T_LockRows
            | NodeTag::T_Limit
    );
    if !ok {
        panic!(
            "EvalPlanQualStart (execMain.c): {:?} recheck plan \
             (subquery/aggregate EPQ) not exercised",
            plan.node_tag()
        );
    }
    if let Some(scanrelid) = table_scan_scanrelid(plan) {
        if scanrelid == 0 {
            panic!(
                "ExecScanFetch (execScan.h): scanrelid == 0 pushed-down-join \
                 {:?} recheck not exercised (lane-epq.md §2 FDW gap)",
                plan.node_tag()
            );
        }
    }
    if let Some(ap) = plan.as_append() {
        for child in ap.appendplans.iter() {
            check_epq_plan(child);
        }
    }
    if let Some(sq) = plan.as_subquery_scan() {
        if let Some(sub) = sq.subplan {
            check_epq_plan(sub);
        }
    }
    if let Some(p) = plan.as_plan() {
        if let Some(l) = p.lefttree {
            check_epq_plan(l);
        }
        if let Some(r) = p.righttree {
            check_epq_plan(r);
        }
    }
}

/// `scanrelid` of the ADMITTED table-scan tags (the shapes whose EPQ fetch
/// goes through ExecScanFetch's rti-indexed relsubs arrays). Non-scan tags
/// and the scan-shaped glue whose rti semantics differ (SubqueryScan /
/// ValuesScan / CteScan / FunctionScan scan virtual rels; BitmapIndexScan
/// rides its BitmapHeapScan parent) return None.
fn table_scan_scanrelid(plan: Node<'_>) -> Option<u32> {
    if let Some(s) = plan.as_seq_scan() {
        return Some(s.scan.scanrelid);
    }
    if let Some(s) = plan.as_tid_scan() {
        return Some(s.scan.scanrelid);
    }
    if let Some(s) = plan.as_tid_range_scan() {
        return Some(s.scan.scanrelid);
    }
    if let Some(s) = plan.as_index_scan() {
        return Some(s.scan.scanrelid);
    }
    if let Some(s) = plan.as_index_only_scan() {
        return Some(s.scan.scanrelid);
    }
    if let Some(s) = plan.as_bitmap_heap_scan() {
        return Some(s.scan.scanrelid);
    }
    None
}

fn slot_pair_mut<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    a: ExecSlotId,
    b: ExecSlotId,
) -> (&'a mut types_slot::SlotData<'mcx>, &'a mut types_slot::SlotData<'mcx>) {
    let (i, j) = (a.0 as usize, b.0 as usize);
    debug_assert_ne!(i, j);
    let slots = &mut estate.es_tupleTable[..];
    if i < j {
        let (lo, hi) = slots.split_at_mut(j);
        (&mut lo[i], &mut hi[0])
    } else {
        let (lo, hi) = slots.split_at_mut(i);
        (&mut hi[0], &mut lo[j])
    }
}

/// `EvalPlanQualEnd`, at `ExecEndModifyTable`/`ExecEndLockRows`.
pub fn eval_plan_qual_end<'mcx>(
    epq: &mut EpqState<'mcx>,
    subs: &mut Option<EpqSubs<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(mut recheck) = epq.recheck.take() {
        exec_end_node(&mut recheck, estate)?;
    }
    let mcx = estate.es_query_cxt;
    let n = subs.as_ref().map_or(0, |s| s.relsubs_slot.len());
    for i in 0..n {
        if let Some(id) = subs.as_ref().expect("checked").relsubs_slot[i] {
            exectuples::exec_clear_tuple(estate.slot_mut(id), mcx);
        }
    }
    Ok(())
}

::mcx::forget_safe_struct!(EpqState<'_> { plan, recheck, result_rti, lane_verdicts });
