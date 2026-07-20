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
    // Read-fairness stride (guc_tables::gather_fair): after `fair_stride`
    // consecutive tuples from one queue the read cursor rotates. 0 = C's
    // ratified stick-until-would-block policy (the default).
    fair_stride: i64,
    stride_rem: i64,
    // C outerPlan->chgParam: the deferred-rescan set ExecReScanGather leaves
    // for the child; consumed at the leader's next local pull, after
    // ExecParallelReinitialize.
    pub outer_chg: Bitmapset<'mcx>,
    // WS-O (wave 2)/WS-WIDTH: the gang's admission-ledger width lease
    // (PGRUST_RUNTIME_WIDTH_UNIFIED; None on every fail-open path — knob
    // off, no runtime, ledger off, gang capacity). Held launch →
    // workers-done; dropped (retired) in exec_shutdown_gather_workers.
    width_lease: Option<runtime::ParallelWidthLease>,
}

// The gang-width knob consult (PHASE3-CLOSE WS-WIDTH §2.5, post-W4): ONE
// `runtime::gang_width_face()` read resolves `PGRUST_RUNTIME_WIDTH_UNIFIED`
// (unified pool-face gang entries — the ONLY leased path; the WS-O
// external-face knob died with its body in the W4 audited removal).
// REQUIRES the ledger (`PGRUST_RUNTIME_LEDGER_V2`) —
// with the ledger off (or no runtime, or the 64-entry gang capacity) the
// seam FAILS OPEN to today's launch exactly. The knob-OFF budget pin
// (§2.6): knob off costs exactly the ONE OnceLock read the WS-O seam
// already paid — zero reads beyond it.

/// The launch-width seam (WS-O inc-1b, both gather nodes; unified face by
/// PHASE3-CLOSE WS-WIDTH): lease gang width from the admission ledger's
/// pool face and
/// clamp the context's launch count to the grant through C's own
/// lower-the-launch mechanism
/// (`ReinitializeParallelWorkers` / `nworkers_to_launch`) — DSM and queue
/// sizing stay at plan width, only fewer workers launch, exactly the
/// C-parity "launched < planned" shape every reader already handles.
/// Grant 0 launches NOTHING: the leader's local scan (`need_to_scan_locally`)
/// IS the serial path the lease API demands. None = fail-open: the launch
/// count is untouched (no configuration can have clamped it earlier — the
/// knob and runtime presence are process-static, and a cap refusal leaves
/// the count at the previous startup's value only if THAT startup leased,
/// so the fail-open arm restores the plan width defensively).
///
/// STATED DIVERGENCE — PINNED (PHASE3-CLOSE FM-1; do NOT "fix"): C
/// degrades to fewer/zero launched workers on bgworker-slot exhaustion;
/// under these knobs we ADDITIONALLY degrade on ledger width saturation.
/// Advisory policy only — plan bytes and result bytes are identical,
/// the divergence is visible ONLY in launch counts. The degrade shape is
/// C's own (fewer/zero via this clamp, leader-local scan serves); it must
/// NEVER become a blocking wait, an error, or a re-plan. FM-3 rider:
/// sustained saturation serializes every relaunch (per-startup cadence) —
/// accepted, and VISIBLE via `gang_zero_grants` + the width mirror line;
/// a measured real-workload regression escalates to a BOARD decision
/// (gang minimum-grant floor), never silent code.
///
/// Returns the lease AND the consulted face (the §2.4 mirror's
/// engine-of-record input — carrying it avoids a second OnceLock read on
/// the knob-OFF path).
pub(crate) fn lease_gather_width(
    pcxt: parallel::ParallelContextId,
    num_workers: i32,
) -> (Option<runtime::ParallelWidthLease>, runtime::GangWidthFace) {
    let face = runtime::gang_width_face();
    if face == runtime::GangWidthFace::Off {
        return (None, face);
    }
    let Some(rt) = runtime::global() else {
        return (None, face);
    };
    if !rt.ledger_enabled() {
        return (None, face);
    }
    let lease = rt.lease_parallel_width(num_workers.max(0) as u32);
    let clamp = lease
        .as_ref()
        .map_or(num_workers, |l| l.granted().min(i32::MAX as u32) as i32);
    parallel::ReinitializeParallelWorkers(pcxt, clamp);
    (lease, face)
}

/// Post-launch settle: charge only the gang's ACTIVE width (launched may
/// be below the grant — registration slots, bgworker limits).
pub(crate) fn settle_gather_width(
    lease: &mut Option<runtime::ParallelWidthLease>,
    launched: i32,
) {
    if let Some(l) = lease.as_mut() {
        l.settle(launched.max(0) as u32);
    }
}

/// The WIDTH MIRROR (PHASE3-CLOSE §2.4 = WS-O TODO item 5): one
/// trace-channel detail line per gather startup — requested / granted /
/// launched / engine-of-record (unified vs fail-open) — per
/// the WS-O adjudication ("a detail line to the trace channel rather than
/// EXPLAIN"; the lease is not a lane verdict, EXPLAIN stays untouched —
/// Workers Planned/Launched keeps coming from the existing machinery).
/// This is the observability contract for FM-1/FM-3 (grant-0 serialization
/// must be VISIBLE) and WS-COVER's census cross-checks. Gated on the
/// face being armed FIRST (knob-OFF never reaches the trace OnceLock —
/// the §2.6 budget pin) and on `PGRUST_LANE_V2_TRACE` (the standing
/// engagement-trace env; stderr → server log, e2e-greppable).
pub(crate) fn mirror_gather_width(
    face: runtime::GangWidthFace,
    requested: i32,
    lease: &Option<runtime::ParallelWidthLease>,
    launched: i32,
) {
    if face == runtime::GangWidthFace::Off || !width_mirror_enabled() {
        return;
    }
    let engine = lease.as_ref().map_or("fail-open", |l| l.engine());
    let granted = lease.as_ref().map_or(-1, |l| i64::from(l.granted()));
    eprintln!(
        "[gather-width] engine={engine} requested={requested} granted={granted} \
         launched={launched}"
    );
}

/// Trace arm for the width mirror (`PGRUST_LANE_V2_TRACE`, the lanev2
/// engagement-trace env). Read once; consulted ONLY when a gang-width
/// face is armed — the knob-OFF gather startup never reaches this.
fn width_mirror_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    })
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
    // C ExecInitGather builds the result type from the Gather tlist
    // (ExecInitResultTypeTL) even without a projection: only the top plan's
    // tlist carries the labeled resnames RowDescription needs; the outer
    // scan's descriptor is nameless.
    let result_desc = crate::exec_type_from_tl(&node.plan.targetlist)?;
    let (result_slot, proj_state) = match proj {
        Some(p) => (Some(p.pi_result_slot), Some(p.pi_state)),
        None => (None, None),
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
        fair_stride: 0,
        stride_rem: 0,
        outer_chg: Bitmapset::empty(),
        width_lease: None,
    })
}

fn gather_startup<'mcx>(
    node: &mut GatherState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let gather = node.plan;
    ::parallel::gtrace("l.gather.begin");
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
        // WS-O width lease (default-OFF knob; every fail-open path leaves
        // the launch untouched). Acquired per startup — a rescan relaunch
        // re-leases against the headroom of ITS moment.
        debug_assert!(node.width_lease.is_none(), "lease survived a shutdown");
        let (lease, face) = lease_gather_width(pei.pcxt, gather.num_workers);
        node.width_lease = lease;
        parallel::LaunchParallelWorkers(pei.pcxt)?;
        node.nworkers_launched = parallel::nworkers_launched(pei.pcxt);
        settle_gather_width(&mut node.width_lease, node.nworkers_launched);
        mirror_gather_width(face, gather.num_workers, &node.width_lease, node.nworkers_launched);
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
        node.fair_stride = guc_tables::gather_fair::gather_fair_stride();
        node.stride_rem = node.fair_stride;
    }
    node.need_to_scan_locally =
        node.nreaders == 0 || (!gather.single_copy && leader_participation());
    node.initialized = true;
    ::parallel::gtrace("l.gather.launched");
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
                // SAFETY: transport memory (8-aligned batch chunk or ring),
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
    // Stall self-report clock for the leader's all-queues-idle wait; a tuple
    // or a reader finishing returns from this call (= progress). Latch wakes
    // without progress keep the clock running.
    let mut stall = ::shm_mq::stall::StallDetector::new();
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
            stall.reset();
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
            // Fairness stride (opt-in; see gather_fair): rotate the cursor
            // after `fair_stride` consecutive tuples so no producer's queue
            // is drained exclusively. C never rotates on a successful read.
            if node.fair_stride > 0 && node.nreaders > 1 {
                node.stride_rem -= 1;
                if node.stride_rem <= 0 {
                    node.stride_rem = node.fair_stride;
                    node.nextreader += 1;
                    if node.nextreader >= node.nreaders {
                        node.nextreader = 0;
                    }
                }
            }
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
            leader_wait_reporting(WAIT_EVENT_EXECUTE_GATHER, &mut stall, &node.reader)?;
            nvisited = 0;
        }
    }
}

pub(crate) fn wait_on_my_latch(wait_event: u32) -> PgResult<()> {
    let latch = init_small::globals::MyLatch().expect("gather leader without MyLatch");
    ::parallel::gtrace("l.wait.begin");
    latch::WaitLatch(Some(latch), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event)?;
    ::parallel::gtrace("l.wait.end");
    latch::ResetLatch(latch);
    Ok(())
}

/// `wait_on_my_latch` with the MQ stall self-report armed
/// (notes/parallel-repeat-wedge-2026-07-12.md): if the leader's wait crosses
/// the threshold with every reader idle, elog one LOG line describing every
/// worker queue — counters plus each endpoint's latch and wakeup-registry
/// state, the deaf-worker evidence — then keep waiting unchanged.
fn leader_wait_reporting(
    wait_event: u32,
    stall: &mut ::shm_mq::stall::StallDetector,
    readers: &[tqueue::TupleQueueReader],
) -> PgResult<()> {
    let latch = init_small::globals::MyLatch().expect("gather leader without MyLatch");
    ::parallel::gtrace("l.wait.begin");
    ::shm_mq::stall::wait_latch_reporting(latch, wait_event, stall, &mut |waited_ms| {
        let mut msg = format!(
            "gather leader stall self-report: waited_ms={waited_ms} my_pid={} my_procno={} nreaders={}",
            init_small::globals::MyProcPid(),
            init_small::globals::MyProcNumber(),
            readers.len(),
        );
        for (i, reader) in readers.iter().enumerate() {
            msg.push_str(&format!(
                " reader[{i}]={{{}}}",
                ::shm_mq::stall::describe_queue(reader.mq())
            ));
        }
        ::shm_mq::stall::log_stall_report(msg);
    })?;
    ::parallel::gtrace("l.wait.end");
    latch::ResetLatch(latch);
    Ok(())
}

/// `ExecShutdownGatherWorkers` (nodeGather.c).
pub fn exec_shutdown_gather_workers(node: &mut GatherState<'_>) -> PgResult<()> {
    ::parallel::gtrace("l.gather.workers_done");
    node.reader = Vec::new();
    node.nreaders = 0;
    node.nextreader = 0;
    if let Some(pei) = node.pei.as_mut() {
        exec_parallel_finish(pei)?;
    }
    // WS-O: gang done — retire the width lease (drop returns the width).
    node.width_lease = None;
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

// pei/reader/width_lease are droppy owners (Arc/Mutex/queue/lease handles),
// released by ExecShutdownGather and release_owned.
::mcx::forget_safe_struct!(
    GatherState<'_> { plan, ps, initialized, need_to_scan_locally, tuples_needed,
        funnel_slot, nworkers_launched, nreaders, nextreader, fair_stride,
        stride_rem, outer_chg; pei, reader, width_lease },
);
