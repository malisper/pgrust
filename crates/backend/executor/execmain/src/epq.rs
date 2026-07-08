// EvalPlanQual (execMain.c). Divergence: no child EState — the recheck tree
// runs against the parent estate (shared tuple table, shared subplan cells).
// Each EPQ owner (ModifyTable/LockRows) holds its own relsubs arrays
// (C EPQState.relsubs_*), swapped into estate.es_epq only while its recheck
// runs, so nested EPQ (a LockRows inside a recheck) never clobbers the outer
// run's per-rel state.

use crate::procnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
use ::executils::{EStateData, EpqSubs, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::{Node, NodeTag};

pub struct EpqState<'mcx> {
    pub plan: Option<Node<'mcx>>,
    pub recheck: Option<PlanStateNode<'mcx>>,
    pub result_rti: u32,
}

/// `EvalPlanQual`: `Some` = new candidate tuple, `None` = skip the row.
/// `subs` is the owner's relsubs (test slots parked by EvalPlanQualSlot).
pub fn eval_plan_qual<'mcx>(
    epq: &mut EpqState<'mcx>,
    subs: &mut Option<EpqSubs<'mcx>>,
    estate: &mut EStateData<'mcx>,
    inputslot: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
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
    // C EvalPlanQualSlot: make the rel's test slot on first use (the trigger
    // path reaches here without a parked slot; DML paths park the input).
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
    let testslot = estate.es_epq.as_ref().expect("EPQ state installed").relsubs_slot[idx]
        .expect("just ensured");
    if testslot != inputslot {
        let (dst, src) = slot_pair_mut(estate, testslot, inputslot);
        exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    }

    {
        let subs = estate.es_epq.as_mut().expect("EPQ state installed");
        subs.relsubs_done[idx] = false;
        subs.relsubs_blocked[idx] = false;
    }

    let slot = exec_proc_node(epq.recheck.as_mut().expect("begun"), estate)?;

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

fn eval_plan_qual_begin<'mcx>(
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
fn check_epq_plan(plan: Node<'_>) {
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
    if let Some(ap) = plan.as_append() {
        for child in ap.appendplans.iter() {
            check_epq_plan(child);
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

::mcx::forget_safe_struct!(EpqState<'_> { plan, recheck, result_rti });
