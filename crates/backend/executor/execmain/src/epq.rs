// EvalPlanQual (execMain.c). Divergence: no child EState — the recheck tree
// runs against the parent estate (relsubs on EStateData, shared tuple table),
// and Begin's reset rescans directly instead of C's deferred chgParam; safe
// because anything beyond a plain SeqScan recheck is a loud panic below.

use crate::procnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::{Node, NodeTag};

pub struct EpqState<'mcx> {
    pub plan: Option<Node<'mcx>>,
    pub recheck: Option<PlanStateNode<'mcx>>,
    pub result_rti: u32,
}

/// `EvalPlanQual`: `Some` = new candidate tuple, `None` = skip the row.
pub fn eval_plan_qual<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
    inputslot: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    estate.es_epq_active = true;
    let r = eval_plan_qual_guts(epq, estate, inputslot);
    estate.es_epq_active = false;
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
    let testslot = estate.es_epq.as_ref().expect("EPQ state installed").relsubs_slot[idx]
        .expect("EvalPlanQualSlot created the test slot");
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

    let pstmt = estate.es_plannedstmt.expect("EPQ under a planned statement");
    if !pstmt.subplans.is_nil() {
        panic!("EvalPlanQualStart (execMain.c): subplan/initplan recheck not ported");
    }
    if estate.es_instrument != 0 {
        panic!("EvalPlanQualStart (execMain.c): instrumented recheck not ported");
    }
    let plan = epq.plan.expect("ModifyTable has a subplan");
    if plan.node_tag() != NodeTag::T_SeqScan {
        panic!(
            "EvalPlanQualStart (execMain.c): {:?} recheck plan \
             (join/subquery/index-scan EPQ) not exercised",
            plan.node_tag()
        );
    }
    debug_assert!(estate.es_epq.is_some(), "EvalPlanQualSlot precedes EvalPlanQual");
    epq.recheck = Some(exec_init_node(Some(plan), estate, 0)?.expect("recheck subplan"));
    Ok(())
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

/// `EvalPlanQualEnd`, at `ExecEndModifyTable`.
pub fn eval_plan_qual_end<'mcx>(
    epq: &mut EpqState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(mut recheck) = epq.recheck.take() {
        exec_end_node(&mut recheck, estate)?;
    }
    let mcx = estate.es_query_cxt;
    let n = estate.es_epq.as_ref().map_or(0, |s| s.relsubs_slot.len());
    for i in 0..n {
        if let Some(id) = estate.es_epq.as_ref().expect("checked").relsubs_slot[i] {
            exectuples::exec_clear_tuple(estate.slot_mut(id), mcx);
        }
    }
    estate.es_epq_active = false;
    Ok(())
}
