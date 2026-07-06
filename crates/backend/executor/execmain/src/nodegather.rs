// nodeGather.c. Lives in execmain (SubqueryScan precedent: the node drives
// exec_proc_node on its child and execparallel walks the leader estate).

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::plannodes::Gather;
use ::types_slot::TupleSlotKind;

use crate::execparallel::{
    self, exec_init_parallel_plan, exec_parallel_cleanup, exec_parallel_create_readers,
    exec_parallel_finish, exec_parallel_reinitialize, ParallelExecutorInfo,
};
use crate::procnode::{exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode};

const WL_LATCH_SET: u32 = types_storage::waiteventset::WL_LATCH_SET;
const WL_EXIT_ON_PM_DEATH: u32 = types_storage::waiteventset::WL_EXIT_ON_PM_DEATH;
pub(crate) const WAIT_EVENT_EXECUTE_GATHER: u32 = 0x0800_0000 + 13;

pub struct GatherState<'mcx> {
    pub plan: &'mcx Gather<'mcx>,
    pub ps: PlanStateBase<'mcx>,
    pub initialized: bool,
    pub need_to_scan_locally: bool,
    pub tuples_needed: i64,
    pub funnel_slot: ExecSlotId,
    pub pei: Option<ParallelExecutorInfo>,
    pub nworkers_launched: i32,
    pub nreaders: usize,
    pub nextreader: usize,
    pub reader: Vec<tqueue::TupleQueueReader>,
    // C outerPlan->chgParam: the deferred-rescan set ExecReScanGather leaves
    // for the child; consumed at the leader's next local pull, after
    // ExecParallelReinitialize.
    pub outer_chg: Bitmapset<'mcx>,
}

pub(crate) fn leader_participation() -> bool {
    guc_tables::vars::parallel_leader_participation.read()
}

pub(crate) fn bms_members(bms: &Bitmapset<'_>) -> Vec<u32> {
    let mut v = Vec::with_capacity(bms.num_members() as usize);
    let mut i = bms.next_member(-1);
    while i >= 0 {
        v.push(i as u32);
        i = bms.next_member(i);
    }
    v
}

/// `ExecInitGather` (nodeGather.c). The caller (procnode) owns the child.
pub fn exec_init_gather<'mcx>(
    node: &'mcx Gather<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer: &PlanStateNode<'mcx>,
) -> PgResult<GatherState<'mcx>> {
    debug_assert!(node.plan.righttree.is_none());
    debug_assert!(node.plan.qual.is_nil());
    let mcx = estate.es_query_cxt;
    let ecxt = estate.exec_assign_expr_context();

    let outer_plan =
        node.plan.lefttree.expect("Gather without an outer plan").as_plan().unwrap();
    let tup_desc = outer.exec_get_result_type(outer_plan)?;

    let proj = ::execscan::exec_conditional_assign_projection_info(
        mcx,
        estate,
        &node.plan.targetlist,
        ::types_nodes::primnodes::OUTER_VAR as u32,
        &tup_desc,
    )?;
    let (result_desc, result_slot, proj_state) = match proj {
        Some(p) => {
            let desc = crate::exec_type_from_tl(&node.plan.targetlist)?;
            (desc, Some(p.pi_result_slot), Some(p.pi_state))
        }
        None => (tup_desc.clone(), None, None),
    };

    let funnel_slot =
        estate.exec_init_extra_tuple_slot(Some(tup_desc), TupleSlotKind::MinimalTuple);

    Ok(GatherState {
        plan: node,
        ps: PlanStateBase {
            plan: &node.plan,
            ps_ExprContext: Some(ecxt),
            ps_ResultTupleDesc: Some(result_desc),
            ps_ResultTupleSlot: result_slot,
            ps_ProjInfo: proj_state,
            qual: None,
        },
        initialized: false,
        need_to_scan_locally: !node.single_copy && leader_participation(),
        tuples_needed: -1,
        funnel_slot,
        pei: None,
        nworkers_launched: 0,
        nreaders: 0,
        nextreader: 0,
        reader: Vec::new(),
        outer_chg: Bitmapset::empty(),
    })
}

fn gather_startup<'mcx>(
    node: &mut GatherState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let gather = node.plan;
    if gather.num_workers > 0 && estate.es_use_parallel_mode {
        match node.pei.as_mut() {
            None => {
                node.pei = Some(exec_init_parallel_plan(
                    gather.plan.lefttree.expect("Gather without an outer plan"),
                    outer,
                    estate,
                    &gather.initParam,
                    gather.num_workers,
                    node.tuples_needed,
                )?)
            }
            Some(pei) => exec_parallel_reinitialize(outer, estate, pei, &gather.initParam)?,
        }
        let pei = node.pei.as_mut().expect("just initialized");
        parallel::LaunchParallelWorkers(pei.pcxt)?;
        node.nworkers_launched = parallel::nworkers_launched(pei.pcxt);
        execparallel::account_workers(estate, pei.pcxt);

        if node.nworkers_launched > 0 {
            exec_parallel_create_readers(pei);
            // C copies pei->reader into a working array; ownership moves here
            // and detach happens in ExecParallelFinish via drop.
            node.reader = core::mem::take(&mut pei.reader);
        } else {
            node.reader = Vec::new();
        }
        node.nreaders = node.reader.len();
        node.nextreader = 0;
    }
    node.need_to_scan_locally =
        node.nreaders == 0 || (!gather.single_copy && leader_participation());
    node.initialized = true;
    Ok(())
}

/// `ExecGather` (nodeGather.c).
pub fn exec_gather<'mcx>(
    node: &mut GatherState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;

    if !node.initialized {
        gather_startup(node, outer, estate)?;
    }

    let ecxt = node.ps.ps_ExprContext.expect("GatherState without ExprContext");
    estate.reset_expr_context(ecxt);

    let Some(slot) = gather_getnext(node, outer, estate)? else {
        return Ok(None);
    };
    if node.ps.ps_ProjInfo.is_none() {
        return Ok(Some(slot));
    }
    estate.ecxt_mut(ecxt).ecxt_outertuple = Some(slot);
    let result_slot = node.ps.ps_ResultTupleSlot.expect("projection without result slot");
    let proj = node.ps.ps_ProjInfo.as_deref_mut().unwrap();
    with_eval_slots(estate, ecxt, Some(result_slot), |slots, result, mcx| {
        ::execexpr::exec_project(proj, slots, result.unwrap(), mcx)
    })?;
    Ok(Some(result_slot))
}

/// `gather_getnext` (nodeGather.c).
fn gather_getnext<'mcx>(
    node: &mut GatherState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    while node.nreaders > 0 || node.need_to_scan_locally {
        crate::cfi()?;

        if node.nreaders > 0 {
            if let Some(ptr) = gather_readnext(node)? {
                let mcx = estate.es_query_cxt;
                let slot = estate.slot_mut(node.funnel_slot);
                // SAFETY: queue memory (8-aligned ring/reassembly buffer),
                // live until the next receive on this reader — consumed
                // before the next gather_readnext, as C.
                unsafe { ::exectuples::exec_store_minimal_tuple_ptr(slot, mcx, ptr) };
                return Ok(Some(node.funnel_slot));
            }
        }

        if node.need_to_scan_locally {
            apply_pending_outer_chg(
                &mut node.outer_chg,
                outer,
                node.plan.plan.lefttree.expect("Gather without an outer plan"),
                estate,
            )?;
            if let Some(id) = exec_proc_node(outer, estate)? {
                return Ok(Some(id));
            }
            node.need_to_scan_locally = false;
        }
    }
    Ok(None)
}

/// `gather_readnext` (nodeGather.c): round-robin nowait reads; keep draining
/// one queue until it would block.
fn gather_readnext(
    node: &mut GatherState<'_>,
) -> PgResult<Option<core::ptr::NonNull<::types_tuple::MinimalTupleData>>> {
    let mut nvisited = 0;
    loop {
        crate::cfi()?;

        debug_assert!(node.nextreader < node.nreaders);
        let mut done = false;
        let tup = node.reader[node.nextreader].next(true, &mut done)?.map(|bytes| {
            core::ptr::NonNull::new(bytes.as_ptr().cast_mut())
                .expect("queue payload is non-null")
                .cast::<::types_tuple::MinimalTupleData>()
        });

        if done {
            debug_assert!(tup.is_none());
            node.nreaders -= 1;
            if node.nreaders == 0 {
                exec_shutdown_gather_workers(node)?;
                return Ok(None);
            }
            node.reader.remove(node.nextreader);
            if node.nextreader >= node.nreaders {
                node.nextreader = 0;
            }
            continue;
        }

        if tup.is_some() {
            return Ok(tup);
        }

        node.nextreader += 1;
        if node.nextreader >= node.nreaders {
            node.nextreader = 0;
        }

        nvisited += 1;
        if nvisited >= node.nreaders {
            if node.need_to_scan_locally {
                return Ok(None);
            }
            wait_on_my_latch(WAIT_EVENT_EXECUTE_GATHER)?;
            nvisited = 0;
        }
    }
}

pub(crate) fn wait_on_my_latch(wait_event: u32) -> PgResult<()> {
    let latch = init_small::globals::MyLatch().expect("gather leader without MyLatch");
    latch::WaitLatch(Some(latch), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event)?;
    latch::ResetLatch(latch);
    Ok(())
}

/// `ExecShutdownGatherWorkers` (nodeGather.c).
pub fn exec_shutdown_gather_workers(node: &mut GatherState<'_>) -> PgResult<()> {
    node.reader = Vec::new();
    node.nreaders = 0;
    node.nextreader = 0;
    if let Some(pei) = node.pei.as_mut() {
        exec_parallel_finish(pei)?;
    }
    Ok(())
}

/// `ExecShutdownGather` (nodeGather.c).
pub fn exec_shutdown_gather(
    node: &mut GatherState<'_>,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    exec_shutdown_gather_workers(node)?;
    if let Some(mut pei) = node.pei.take() {
        exec_parallel_cleanup(estate, &mut pei)?;
    }
    Ok(())
}

/// `ExecReScanGather` (nodeGather.c): shut workers down; relaunch on the next
/// ExecProcNode. With a rescan_param the child rescan is deferred (chgParam):
/// parallel-aware children must see ReInitializeDSM before their ReScan.
pub fn exec_rescan_gather<'mcx>(
    node: &mut GatherState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_shutdown_gather_workers(node)?;
    node.initialized = false;
    if node.plan.rescan_param >= 0 {
        let mcx = estate.es_query_cxt;
        node.outer_chg.add_member(mcx, node.plan.rescan_param)?;
    }
    if node.outer_chg.is_empty() {
        return crate::execami::exec_re_scan(outer, estate);
    }
    Ok(())
}

/// C's ExecProcNode chgParam check on the leader's local child: consume the
/// deferred set before the pull.
pub(crate) fn apply_pending_outer_chg<'mcx>(
    outer_chg: &mut Bitmapset<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    outer_plan: ::types_nodes::node_tree::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if outer_chg.is_empty() {
        return Ok(());
    }
    let chg = core::mem::replace(outer_chg, Bitmapset::empty());
    crate::execami::exec_re_scan_chg_forced(outer, outer_plan, estate, &chg)
}

// pei/reader are droppy owners (Arc/Mutex/queue handles), released by
// ExecShutdownGather and release_owned.
::mcx::forget_safe_struct!(
    GatherState<'_> { plan, ps, initialized, need_to_scan_locally, tuples_needed,
        funnel_slot, nworkers_launched, nreaders, nextreader, outer_chg; pei, reader },
);
