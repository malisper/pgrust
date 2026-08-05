// execParallel.c, thread-native (docs/parallel-query-design.md): plan tree,
// rtable, and extern params cross by shared reference (read-only; C copies
// only because processes force it); PARAM_EXEC datum words and per-worker
// output slots stay copied per C. Not carried (phase-3 handoff): JIT
// instrumentation (no JIT), per-worker memoize/tuplestore/bitmap display,
// parallel-aware per-node arms (loud below).

use std::any::Any;
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use ::datum::Datum;
use ::executils::{EStateData, WorkerInstr};
use ::mcx::PgVec;
use ::tcop_dest::DestReceiver;
use ::types_core::instrument::{
    AggregateInstrumentation, BufferUsage, HashInstrumentation, IncrementalSortInfo,
    Instrumentation, TuplesortInstrumentation, WalUsage,
};
use ::types_dest::CommandDest;
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::nodes_enums::CmdType;
use ::types_portal::params::{ParamExecData, ParamExternData};
use ::types_portal::{ParamListHandle, QueryEnvHandle};
use ::types_scan::sdir::ForwardScanDirection;

use crate::querydesc;

struct SendConst<T>(*const T);
// SAFETY: read-only erased reference; the leader keeps the pointee alive and
// unmodified until DestroyParallelContext has joined every worker.
unsafe impl<T> Send for SendConst<T> {}
// SAFETY: as above; workers only read.
unsafe impl<T> Sync for SendConst<T> {}

// One worker's end-of-run estate side tables (ExecParallelReportInstrumentation
// + the Exec*RetrieveInstrumentation family, collapsed to one copy).
pub(crate) struct WorkerInstrReport {
    instrument: Vec<Instrumentation>,
    sort: Vec<(i32, TuplesortInstrumentation)>,
    incsort: Vec<(i32, IncrementalSortInfo)>,
    agg: Vec<(i32, AggregateInstrumentation)>,
    hash: Vec<(i32, HashInstrumentation)>,
    index: Vec<(i32, u64)>,
    bitmap: Vec<(i32, types_core::instrument::BitmapHeapScanInstrumentation)>,
}

struct SharedInstrumentation {
    instrument_options: i32,
    workers: Mutex<Vec<Option<WorkerInstrReport>>>,
}

// C's per-node shm_toc entries, typed and keyed by plan_node_id. IndexScan
// covers IndexOnlyScan too (same shared descriptor shape).
pub(crate) enum ParallelNodeShared {
    SeqScan(Arc<::tableam::ParallelTableScanDescShared>),
    IndexScan(Arc<::indexam::ParallelIndexScanDescShared>),
    BitmapHeapScan(Arc<::nodebitmapheapscan::ParallelBitmapHeapState>),
    HashJoin(Arc<::nodehash::parallel::ParallelHashJoinState>),
    Append(Arc<::nodeappend::ParallelAppendState>),
}

pub(crate) struct ParallelExecShared {
    pstmt: SendConst<PlannedStmt<'static>>,
    query_text: String,
    param_extern: Option<(SendConst<ParamExternData>, usize)>,
    // (paramid, datum word, isnull); by-ref datum words point into leader
    // memory that outlives the run (SerializeParamExecParams' dsa analog).
    param_exec: Mutex<Vec<(i32, Datum, bool)>>,
    tuples_needed: i64,
    eflags: i32,
    // Tuple queue pair per worker: shm_mq ring (chunk indices + detach
    // discipline) and the chunk ledger the payloads cross through.
    queues: Mutex<Vec<(Arc<shm_mq::ShmMq>, Arc<tqueue::ChunkLedger>)>>,
    // Written once by the leader's InitializeDSM walk, before workers launch.
    nodes: Mutex<Vec<(i32, ParallelNodeShared)>>,
    // Thread-native agg table handoff registry snapshot (nodeagg::merge);
    // worker threads adopt it before their run (leader-registered at
    // ExecInitAgg, keyed by partial Agg plan-node address). (The lane-v2
    // pardistinct handoff snapshot that rode next to it was DELETED at
    // Phase-5 D1 with the GM-hybrid drives.)
    agg_handoff: ::nodeagg::merge::AggHandoffExport,
    instrumentation: Option<SharedInstrumentation>,
    usage: Mutex<Vec<(BufferUsage, WalUsage)>>,
}

pub struct ParallelExecutorInfo {
    pub pcxt: parallel::ParallelContextId,
    shared: Arc<ParallelExecShared>,
    tqueue: Vec<Option<(shm_mq::ShmMqHandle, Arc<tqueue::ChunkLedger>)>>,
    pub reader: Vec<tqueue::TupleQueueReader>,
    pub finished: bool,
    instrumented: bool,
    // Gather subtree plan_node_ids (ExecParallelRetrieveInstrumentation's
    // walk scope): worker_instrument attaches only to these nodes.
    subtree_ids: Vec<i32>,
}

fn collect_plan_node_ids(node: Option<Node<'_>>, out: &mut Vec<i32>) {
    let Some(node) = node else { return };
    let plan = plan_of_node(node);
    out.push(plan.plan_node_id);
    collect_plan_node_ids(plan.lefttree, out);
    collect_plan_node_ids(plan.righttree, out);
    for child in node_child_lists(node) {
        for sub in child.iter() {
            collect_plan_node_ids(Some(sub), out);
        }
    }
}

fn walk_parallel_aware(node: Option<Node<'_>>) {
    let Some(node) = node else { return };
    let plan = plan_of_node(node);
    if plan.parallel_aware
        && !matches!(
            node.node_tag(),
            ::types_nodes::NodeTag::T_SeqScan
                | ::types_nodes::NodeTag::T_IndexScan
                | ::types_nodes::NodeTag::T_IndexOnlyScan
                | ::types_nodes::NodeTag::T_BitmapHeapScan
                | ::types_nodes::NodeTag::T_HashJoin
                | ::types_nodes::NodeTag::T_Hash
                | ::types_nodes::NodeTag::T_Append
        )
    {
        panic!(
            "ExecParallelInitializeDSM (execParallel.c): parallel-aware {:?} — \
             per-node DSM/worker arms land with the parallel append \
             scan lanes",
            node.node_tag()
        );
    }
    walk_parallel_aware(plan.lefttree);
    walk_parallel_aware(plan.righttree);
    for child in node_child_lists(node) {
        for sub in child.iter() {
            walk_parallel_aware(Some(sub));
        }
    }
}

// The parallel-aware scan states the ExecParallel* walks act on.
pub(crate) enum ParallelScanMut<'x, 'mcx> {
    SeqScan(&'x mut ::nodeseqscan::SeqScanState<'mcx>),
    IndexScan(&'x mut ::nodeindexscan::IndexScanState<'mcx>),
    IndexOnlyScan(&'x mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>),
    BitmapHeapScan(&'x mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>),
    // A parallel-aware HashJoin (Parallel Hash): the whole node, because the
    // shared state hangs off the inner Hash sub-node.
    HashJoin(&'x mut crate::procnode::HashJoinNode<'mcx>),
    // Parallel Append: the subplan claim table lives on the AppendState.
    Append(&'x mut ::nodeappend::AppendState<'mcx>),
}

impl ParallelScanMut<'_, '_> {
    fn plan_node_id(&self) -> i32 {
        match self {
            ParallelScanMut::SeqScan(ss) => ss.plan_node_id(),
            ParallelScanMut::IndexScan(is) => is.iss_PlanNodeId,
            ParallelScanMut::IndexOnlyScan(ios) => ios.ioss_PlanNodeId,
            ParallelScanMut::BitmapHeapScan(bhs) => bhs.plan_node_id,
            ParallelScanMut::HashJoin(hj) => hj.state.plan.join.plan.plan_node_id,
            ParallelScanMut::Append(ap) => ap.plan.plan.plan_node_id,
        }
    }
}

// planstate_tree_walker for the ExecParallel* walks (execParallel.c); other
// parallel-aware nodes are rejected loud by walk_parallel_aware.
// Divergence: initPlan/subPlan planstates are not walked — subplans are
// parallel_safe, never parallel_aware, so those arms are no-ops in C too.
fn for_each_parallel_scan<'mcx>(
    ps: &mut crate::procnode::PlanStateNode<'mcx>,
    f: &mut dyn FnMut(ParallelScanMut<'_, 'mcx>) -> PgResult<()>,
) -> PgResult<()> {
    use crate::procnode::PlanStateNode as N;
    match ps {
        N::Instrumented(w) => for_each_parallel_scan(&mut w.inner, f)?,
        N::SeqScan(ss) => {
            if ss.parallel_aware() {
                f(ParallelScanMut::SeqScan(ss))?;
            }
        }
        N::IndexScan(is) => {
            if is.iss_ParallelAware {
                f(ParallelScanMut::IndexScan(is))?;
            }
        }
        N::IndexOnlyScan(ios) => {
            if ios.ioss_ParallelAware {
                f(ParallelScanMut::IndexOnlyScan(&mut **ios))?;
            }
        }
        N::Result(rs) => {
            if let Some(outer) = rs.outer.as_deref_mut() {
                for_each_parallel_scan(outer, f)?;
            }
        }
        N::ProjectSet(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Agg(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Group(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Sort(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::IncrementalSort(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Material(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Memoize(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Unique(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Limit(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::LockRows(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::WindowAgg(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::Gather(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::GatherMerge(x) => for_each_parallel_scan(&mut x.outer, f)?,
        N::BitmapHeapScan(x) => {
            let x = &mut **x;
            if x.scan.parallel_aware {
                f(ParallelScanMut::BitmapHeapScan(&mut x.scan))?;
            }
            for_each_parallel_scan(&mut x.bitmapqual, f)?;
        }
        N::ModifyTable(x) => for_each_parallel_scan(&mut x.subplan, f)?,
        N::SubqueryScan(x) => for_each_parallel_scan(&mut x.subplan, f)?,
        N::NestLoop(x) => {
            for_each_parallel_scan(&mut x.outer, f)?;
            for_each_parallel_scan(&mut x.inner, f)?;
        }
        N::MergeJoin(x) => {
            for_each_parallel_scan(&mut x.outer, f)?;
            for_each_parallel_scan(&mut x.inner, f)?;
        }
        N::HashJoin(x) => {
            let x = &mut **x;
            if x.state.plan.join.plan.parallel_aware {
                f(ParallelScanMut::HashJoin(x))?;
            }
            for_each_parallel_scan(&mut x.outer, f)?;
            for_each_parallel_scan(&mut x.hash.child, f)?;
        }
        N::SetOp(x) => {
            for_each_parallel_scan(&mut x.outer, f)?;
            for_each_parallel_scan(&mut x.inner, f)?;
        }
        N::RecursiveUnion(x) => {
            for_each_parallel_scan(&mut x.outer, f)?;
            for_each_parallel_scan(&mut x.inner, f)?;
        }
        N::Append(x) => {
            let x = &mut **x;
            if x.state.plan.plan.parallel_aware {
                f(ParallelScanMut::Append(&mut x.state))?;
            }
            for sub in x.substates.iter_mut() {
                for_each_parallel_scan(sub, f)?;
            }
        }
        N::MergeAppend(x) => {
            for sub in x.substates.iter_mut() {
                for_each_parallel_scan(sub, f)?;
            }
        }
        N::BitmapAnd(x) | N::BitmapOr(x) => {
            for sub in x.substates.iter_mut() {
                for_each_parallel_scan(sub, f)?;
            }
        }
        // ForeignScan is parallel-unsafe by construction — the parallel-FDW
        // ABI was deleted (2026-07-20 FDW-ABI ruling): create_foreignscan_path
        // pins parallel_aware=false and set_rel_consider_parallel refuses
        // foreign rels outright. walk_parallel_aware panics loud if a
        // parallel-aware ForeignScan ever materializes anyway.
        N::ForeignScan(_)
        | N::SampleScan(_)
        | N::FunctionScan(_)
        | N::ValuesScan(_)
        | N::TableFuncScan(_)
        | N::CteScan(_)
        | N::TidScan(_)
        | N::TidRangeScan(_)
        | N::BitmapIndexScan(_)
        | N::WorkTableScan(_)
        | N::NamedTuplestoreScan(_) => {}
    }
    Ok(())
}

fn plan_of_node<'mcx>(node: Node<'mcx>) -> &'mcx ::types_nodes::plannodes::Plan<'mcx> {
    node.as_plan().expect("plan-tree node")
}

fn node_child_lists<'mcx>(node: Node<'mcx>) -> Vec<&'mcx NodeList<'mcx>> {
    use ::types_nodes::NodeTag;
    match node.node_tag() {
        NodeTag::T_Append => vec![&node.as_append().unwrap().appendplans],
        NodeTag::T_MergeAppend => vec![&node.as_merge_append().unwrap().mergeplans],
        NodeTag::T_BitmapAnd => vec![&node.as_bitmap_and().unwrap().bitmapplans],
        NodeTag::T_BitmapOr => vec![&node.as_bitmap_or().unwrap().bitmapplans],
        NodeTag::T_SubqueryScan => {
            walk_parallel_aware(node.as_subquery_scan().unwrap().subplan);
            vec![]
        }
        _ => vec![],
    }
}

// ExecSerializePlan, share-not-serialize: the dummy PlannedStmt is built in
// the leader's executor arena and crosses by reference. The resjunk-clearing
// copy is replaced by the worker-side junk-filter suppression in InitPlan
// (same observable: junk columns reach the leader).
pub(crate) fn build_worker_pstmt<'mcx>(
    estate: &EStateData<'mcx>,
    plan_node: Node<'mcx>,
) -> PgResult<&'mcx PlannedStmt<'mcx>> {
    let leader = estate.es_plannedstmt.expect("parallel plan without es_plannedstmt");
    let mcx = estate.es_query_cxt;
    // Transfer only parallel-safe subplans, leaving a NULL hole for unsafe
    // ones so the plan_id indexes of the safe ones are preserved; workers
    // must never ExecInitNode an unsafe subplan.
    let mut subplans = ::types_nodes::list::OptNodeList::nil();
    for subplan in leader.subplans.iter() {
        let cell = subplan
            .filter(|sp| sp.as_plan().expect("subplans cell is a plan tree").parallel_safe);
        subplans.lappend(mcx, cell)?;
    }
    let pstmt = PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        queryId: ::types_nodes::SyncCell::new(leader.queryId.get()),
        planId: leader.planId,
        hasReturning: false,
        hasModifyingCTE: false,
        canSetTag: true,
        transientPlan: false,
        dependsOnRole: false,
        parallelModeNeeded: false,
        jitFlags: 0,
        planTree: Some(plan_node),
        partPruneInfos: leader.partPruneInfos.clone_in(mcx)?,
        rtable: leader.rtable.clone_in(mcx)?,
        unprunableRelids: estate.es_unpruned_relids.clone_in(mcx)?,
        permInfos: leader.permInfos.clone_in(mcx)?,
        resultRelations: ::types_nodes::list::IntList::nil(),
        appendRelations: NodeList::nil(),
        subplans,
        rewindPlanIDs: Bitmapset::empty(),
        rowMarks: NodeList::nil(),
        relationOids: ::types_nodes::list::OidList::nil(),
        invalItems: NodeList::nil(),
        paramExecTypes: leader.paramExecTypes.clone_in(mcx)?,
        utilityStmt: None,
        stmt_location: -1,
        stmt_len: -1,
    };
    Ok(Node::mk(mcx, pstmt)?.as_planned_stmt().expect("PlannedStmt"))
}

fn serialize_param_exec(
    estate: &EStateData<'_>,
    send_params: &Bitmapset<'_>,
) -> Vec<(i32, Datum, bool)> {
    let mut out = Vec::new();
    let mut paramid = send_params.next_member(-1);
    while paramid >= 0 {
        let prm = &estate.es_param_exec_vals[paramid as usize];
        out.push((paramid, prm.value, prm.isnull));
        paramid = send_params.next_member(paramid);
    }
    out
}

fn setup_tuple_queues(
    shared: &ParallelExecShared,
    nworkers: i32,
) -> Vec<Option<(shm_mq::ShmMqHandle, Arc<tqueue::ChunkLedger>)>> {
    let me = init_small::globals::MyProcNumber();
    let mut queues = Vec::with_capacity(nworkers.max(0) as usize);
    let mut handles = Vec::with_capacity(nworkers.max(0) as usize);
    for _ in 0..nworkers {
        let mq = shm_mq::shm_mq_create(tqueue::PARALLEL_TUPLE_QUEUE_SIZE);
        mq.set_receiver(me);
        let ledger = Arc::new(tqueue::ChunkLedger::new());
        handles.push(Some((shm_mq::shm_mq_attach(Arc::clone(&mq)), Arc::clone(&ledger))));
        queues.push((mq, ledger));
    }
    *shared.queues.lock().unwrap_or_else(|e| e.into_inner()) = queues;
    handles
}

/// `ExecInitParallelPlan` (execParallel.c). `planstate` is the leader's tree
/// for `child_plan` (C receives the PlanState and reads plan through it).
pub fn exec_init_parallel_plan<'mcx>(
    child_plan: Node<'mcx>,
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    send_params: &Bitmapset<'mcx>,
    nworkers: i32,
    tuples_needed: i64,
) -> PgResult<ParallelExecutorInfo> {
    parallel::gtrace("l.initplan.begin");
    // ExecSetParamPlanMulti: force initplan outputs before workers read them.
    ::executils::exec_eval_param_exec_params(estate, &crate::nodegather::bms_members(send_params))?;
    walk_parallel_aware(Some(child_plan));

    let pstmt = build_worker_pstmt(estate, child_plan)?;
    let pcxt = parallel::CreateParallelContext("postgres", "ParallelQueryMain", nworkers)?;
    // Leader-panic containment injection: forced fault during parallel init,
    // env-gated so the surface is inert in production (crash-restart-design
    // pattern). The panic must unwind to an ERROR, abort the transaction, and
    // leave the session usable — never wedge it.
    if std::env::var_os("PGRUST_CRASH_TEST").is_some()
        && estate.es_sourceText.unwrap_or("").contains("pgrust:leader-panic-parallel-init")
    {
        panic!("injected leader panic (parallel init)");
    }
    parallel::InitializeParallelDSM(pcxt)?;
    let nworkers = parallel::nworkers(pcxt);

    debug_assert!(estate.es_snapshot.is_some());

    // ExecParallelEstimate + ExecParallelInitializeDSM: before workers launch
    // and before the leader executes (its parallel scan descs install here).
    let mut nodes = Vec::new();
    for_each_parallel_scan(planstate, &mut |node| {
        let id = node.plan_node_id();
        let shared = match node {
            ParallelScanMut::SeqScan(ss) => {
                ::nodeseqscan::exec_seq_scan_estimate(ss);
                ParallelNodeShared::SeqScan(::nodeseqscan::exec_seq_scan_initialize_dsm(
                    ss, estate,
                )?)
            }
            ParallelScanMut::IndexScan(is) => {
                ::nodeindexscan::exec_index_scan_estimate(is);
                ParallelNodeShared::IndexScan(::nodeindexscan::exec_index_scan_initialize_dsm(
                    is, estate,
                )?)
            }
            ParallelScanMut::IndexOnlyScan(ios) => {
                ::nodeindexonlyscan::exec_index_only_scan_estimate(ios);
                ParallelNodeShared::IndexScan(
                    ::nodeindexonlyscan::exec_index_only_scan_initialize_dsm(ios, estate)?,
                )
            }
            ParallelScanMut::BitmapHeapScan(bhs) => ParallelNodeShared::BitmapHeapScan(
                ::nodebitmapheapscan::exec_bitmap_heap_initialize_dsm(bhs),
            ),
            // ExecHashJoinInitializeDSM: nparticipants = nworkers + leader.
            ParallelScanMut::HashJoin(hj) => {
                let ps = Arc::new(::nodehash::parallel::ParallelHashJoinState::new(
                    nworkers + 1,
                )?);
                hj.hash.state.set_parallel_state(Arc::clone(&ps));
                ParallelNodeShared::HashJoin(ps)
            }
            ParallelScanMut::Append(ap) => {
                ParallelNodeShared::Append(::nodeappend::exec_append_initialize_dsm(ap))
            }
        };
        nodes.push((id, shared));
        Ok(())
    })?;

    let mut subtree_ids = Vec::new();
    collect_plan_node_ids(Some(child_plan), &mut subtree_ids);

    let instrumented = estate.es_instrument != 0;
    let shared = Arc::new(ParallelExecShared {
        // SAFETY: the pstmt lives in the leader's executor arena; workers are
        // joined by DestroyParallelContext before ExecutorEnd frees it.
        pstmt: SendConst(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(pstmt)
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        // Portal-lifetime array, outlives the workers (registered params
        // contract in standard_executor_start).
        param_extern: estate.es_param_list_info.map(|p| (SendConst(p.as_ptr()), p.len())),
        param_exec: Mutex::new(serialize_param_exec(estate, send_params)),
        tuples_needed,
        eflags: estate.es_top_eflags,
        queues: Mutex::new(Vec::new()),
        nodes: Mutex::new(nodes),
        agg_handoff: ::nodeagg::merge::export_registry(),
        instrumentation: instrumented.then(|| SharedInstrumentation {
            instrument_options: estate.es_instrument,
            workers: Mutex::new((0..nworkers).map(|_| None).collect()),
        }),
        usage: Mutex::new(vec![(BufferUsage::default(), WalUsage::default()); nworkers.max(0) as usize]),
    });
    let tqueue = setup_tuple_queues(&shared, nworkers);
    parallel::set_private(pcxt, Arc::clone(&shared) as Arc<dyn Any + Send + Sync>);

    parallel::gtrace("l.initplan.end");
    Ok(ParallelExecutorInfo {
        pcxt,
        shared,
        tqueue,
        reader: Vec::new(),
        finished: false,
        instrumented,
        subtree_ids,
    })
}

/// `ExecParallelCreateReaders` (execParallel.c).
pub fn exec_parallel_create_readers(pei: &mut ParallelExecutorInfo) {
    debug_assert!(pei.reader.is_empty());
    let launched = parallel::nworkers_launched(pei.pcxt);
    for i in 0..launched as usize {
        let (mut handle, ledger) =
            pei.tqueue[i].take().expect("tuple queue handle already taken");
        if let Some(bgwh) = parallel::worker_bgwhandle(pei.pcxt, i) {
            handle.set_handle(bgwh.slot, bgwh.generation);
        }
        pei.reader.push(tqueue::TupleQueueReader::new_batched(handle, ledger));
    }
}

/// `ExecParallelReinitialize` (execParallel.c).
pub fn exec_parallel_reinitialize<'mcx>(
    planstate: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    pei: &mut ParallelExecutorInfo,
    send_params: &Bitmapset<'mcx>,
) -> PgResult<()> {
    debug_assert!(pei.finished);
    ::executils::exec_eval_param_exec_params(estate, &crate::nodegather::bms_members(send_params))?;
    parallel::ReinitializeParallelDSM(pei.pcxt)?;
    pei.tqueue = setup_tuple_queues(&pei.shared, parallel::nworkers(pei.pcxt));
    pei.reader.clear();
    pei.finished = false;
    *pei.shared.param_exec.lock().unwrap_or_else(|e| e.into_inner()) =
        serialize_param_exec(estate, send_params);
    // Worker instrumentation slots survive reinitialize: relaunched workers
    // aggregate into them (execParallel.c:1314-1322), retrieved once at cleanup.
    // ExecParallelReInitializeDSM walk.
    for_each_parallel_scan(planstate, &mut |node| match node {
        ParallelScanMut::SeqScan(ss) => {
            ::nodeseqscan::exec_seq_scan_reinitialize_dsm(ss);
            Ok(())
        }
        ParallelScanMut::IndexScan(is) => ::nodeindexscan::exec_index_scan_reinitialize_dsm(is),
        ParallelScanMut::IndexOnlyScan(ios) => {
            ::nodeindexonlyscan::exec_index_only_scan_reinitialize_dsm(ios)
        }
        ParallelScanMut::BitmapHeapScan(bhs) => {
            ::nodebitmapheapscan::exec_bitmap_heap_reinitialize_dsm(bhs);
            Ok(())
        }
        // ExecHashJoinReInitializeDSM: detach, clear shared files, reset the
        // build barrier for the next scan.
        ParallelScanMut::HashJoin(hj) => {
            if let Some(t) = hj.hash.state.ptable.as_mut() {
                ::nodehash::parallel::exec_hash_table_detach_batch(t)?;
                ::nodehash::parallel::exec_hash_table_detach(t)?;
            }
            hj.hash.state.ptable = None;
            hj.hash
                .state
                .parallel_state()
                .expect("parallel hash join was initialized")
                .reinitialize()
        }
        ParallelScanMut::Append(ap) => {
            ::nodeappend::exec_append_reinitialize_dsm(ap);
            Ok(())
        }
    })
}

/// `ExecParallelFinish` (execParallel.c).
pub fn exec_parallel_finish(pei: &mut ParallelExecutorInfo) -> PgResult<()> {
    if pei.finished {
        return Ok(());
    }
    // Detach ASAP so still-active workers see no further results are wanted;
    // dropping a reader/handle detaches its queue.
    pei.reader.clear();
    pei.tqueue.clear();

    parallel::WaitForParallelWorkersToFinish(pei.pcxt)?;

    let launched = parallel::nworkers_launched(pei.pcxt);
    let usage = pei.shared.usage.lock().unwrap_or_else(|e| e.into_inner());
    for (buf, _wal) in usage.iter().take(launched.max(0) as usize) {
        ::instrument::instr_accum_parallel_query(buf);
    }
    pei.finished = true;
    Ok(())
}

/// `ExecParallelCleanup` + `ExecParallelRetrieveInstrumentation`: aggregate
/// worker slots into the leader's per-node instrumentation, keep per-worker
/// detail for EXPLAIN, destroy the context.
pub fn exec_parallel_cleanup(
    estate: &mut EStateData<'_>,
    pei: &mut ParallelExecutorInfo,
) -> PgResult<()> {
    if pei.instrumented {
        retrieve_instrumentation(estate, pei)?;
    }
    parallel::DestroyParallelContext(pei.pcxt)?;
    pei.reader.clear();
    pei.tqueue.clear();
    Ok(())
}

fn retrieve_instrumentation(
    estate: &mut EStateData<'_>,
    pei: &ParallelExecutorInfo,
) -> PgResult<()> {
    let Some(si) = &pei.shared.instrumentation else { return Ok(()) };
    let mcx = estate.es_query_cxt;
    let mut workers = si.workers.lock().unwrap_or_else(|e| e.into_inner());
    for slot in workers.iter_mut() {
        let Some(w) = slot.take() else { continue };
        if estate.es_instrumentation.len() < w.instrument.len() {
            let grow = w.instrument.len() - estate.es_instrumentation.len();
            estate.es_instrumentation.try_reserve(grow).map_err(|_| mcx.oom(grow))?;
            estate.es_instrumentation.resize(w.instrument.len(), Instrumentation::default());
        }
        for (id, wi) in w.instrument.iter().enumerate() {
            ::instrument::instr_agg_node(&mut estate.es_instrumentation[id], wi);
        }
        let mut instrument = PgVec::new_in(mcx);
        instrument.try_reserve_exact(w.instrument.len()).map_err(|_| mcx.oom(1))?;
        instrument.extend(w.instrument.iter().copied());
        let mut node_ids = PgVec::new_in(mcx);
        node_ids.try_reserve_exact(pei.subtree_ids.len()).map_err(|_| mcx.oom(1))?;
        node_ids.extend(pei.subtree_ids.iter().copied());
        let mut wi = WorkerInstr {
            node_ids,
            instrument,
            sort: PgVec::new_in(mcx),
            incsort: PgVec::new_in(mcx),
            agg: PgVec::new_in(mcx),
            hash: PgVec::new_in(mcx),
            index: PgVec::new_in(mcx),
            bitmap: PgVec::new_in(mcx),
        };
        wi.sort.try_reserve_exact(w.sort.len()).map_err(|_| mcx.oom(1))?;
        wi.sort.extend(w.sort.iter().copied());
        wi.incsort.try_reserve_exact(w.incsort.len()).map_err(|_| mcx.oom(1))?;
        wi.incsort.extend(w.incsort.iter().copied());
        wi.agg.try_reserve_exact(w.agg.len()).map_err(|_| mcx.oom(1))?;
        wi.agg.extend(w.agg.iter().copied());
        wi.hash.try_reserve_exact(w.hash.len()).map_err(|_| mcx.oom(1))?;
        wi.hash.extend(w.hash.iter().copied());
        wi.index.try_reserve_exact(w.index.len()).map_err(|_| mcx.oom(1))?;
        wi.index.extend(w.index.iter().copied());
        wi.bitmap.try_reserve_exact(w.bitmap.len()).map_err(|_| mcx.oom(1))?;
        wi.bitmap.extend(w.bitmap.iter().copied());
        estate.es_worker_instrument.push(wi);
    }
    Ok(())
}

/// `ParallelQueryMain` (execParallel.c) — runs on the worker thread, inside
/// the transaction/snapshot/GUC environment ParallelWorkerMain restored.
pub fn parallel_query_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let private = shared.private().expect("ParallelQueryMain without executor shared state");
    let exec: &ParallelExecShared =
        private.downcast_ref().expect("ParallelQueryMain private is ParallelExecShared");
    let me = parallel::ParallelWorkerNumber();
    debug_assert!(me >= 0);

    let (mq, ledger) = {
        let queues = exec.queues.lock().unwrap_or_else(|e| e.into_inner());
        let (mq, ledger) = &queues[me as usize];
        (Arc::clone(mq), Arc::clone(ledger))
    };
    mq.set_sender(init_small::globals::MyProcNumber());
    let mut receiver = DestReceiver::TupleQueue(tqueue::tqueue_create_DR_batched(
        shm_mq::shm_mq_attach(mq),
        ledger,
    ));

    // Stall-injection test hook (env-gated, inert in production): worker 0
    // sleeps latch-deaf after attaching its queue, so the leader's blocking
    // receive on this queue crosses the stall-report threshold and the
    // self-report fires, then the query completes normally
    // (scripts/parallel-repeat-wedge-e2e.sh).
    if me == 0 {
        if let Some(ms) = std::env::var("PGRUST_TEST_MQ_STALL_INJECT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
        {
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }
    }

    // SAFETY: leader-arena pstmt, alive until the leader joins this thread.
    let pstmt: &PlannedStmt<'_> = unsafe { &*exec.pstmt.0 };
    let params = match &exec.param_extern {
        // SAFETY: portal-lifetime array (see ParallelExecShared).
        Some((p, len)) => unsafe {
            types_portal::params::register(core::slice::from_raw_parts(p.0, *len))
        },
        None => ParamListHandle::NULL,
    };
    let instrument_options =
        exec.instrumentation.as_ref().map_or(0, |si| si.instrument_options);

    let qd = querydesc::create_query_desc_seam(
        pstmt,
        &exec.query_text,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::TupleQueue,
        params,
        QueryEnvHandle::NULL,
        instrument_options,
    )?;
    parallel::gtrace("w.qd.created");

    // Worker-panic containment injection: forced fault on demand, env-gated
    // so the surface is inert in production (crash-restart-design pattern).
    let inject = std::env::var_os("PGRUST_CRASH_TEST")
        .is_some()
        .then(|| exec.query_text.clone())
        .unwrap_or_default();

    ::nodeagg::merge::adopt_registry(&exec.agg_handoff);

    let mut run = || -> PgResult<()> {
        crate::execmain::executor_start_seam(qd, exec.eflags)?;
        parallel::gtrace("w.execstart.done");
        if inject.contains("pgrust:worker-panic-run") {
            panic!("injected parallel worker panic (run)");
        }
        // FATAL flavor, via the SIGTERM/ProcDiePending path the register's
        // numeric-suite kill took: proc_exit unwinds first — this frame's
        // tqueue handle drops with the proc identity still live — and the
        // deferred exit callbacks (incl. ProcKill) run at the thread top.
        if inject.contains("pgrust:worker-panic-fatal") {
            init_small::globals::SetProcDiePending(true);
            init_small::globals::SetInterruptPending(true);
            postgres_seams::check_for_interrupts::call()?;
        }

        querydesc::with_qd(qd, |q| {
            let x = q.exec.as_mut().expect("worker ExecutorStart left no exec");
            x.with_mut(|d| -> PgResult<()> {
                let pe = exec.param_exec.lock().unwrap_or_else(|e| e.into_inner());
                for (paramid, value, isnull) in pe.iter() {
                    d.estate.es_param_exec_vals[*paramid as usize] =
                        ParamExecData { value: *value, isnull: *isnull, exec_plan: false };
                }
                drop(pe);
                // ExecParallelInitializeWorker, then the tuple bound (C order).
                let estate = &mut d.estate;
                if let Some(ps) = d.planstate.as_mut() {
                    for_each_parallel_scan(ps, &mut |node| {
                        let id = node.plan_node_id();
                        let nodes = exec.nodes.lock().unwrap_or_else(|e| e.into_inner());
                        let entry =
                            nodes.iter().find(|(nid, _)| *nid == id).unwrap_or_else(|| {
                                panic!(
                                    "ExecParallelInitializeWorker (execParallel.c): no shared \
                                     entry for plan node {id}"
                                )
                            });
                        match (node, &entry.1) {
                            (ParallelScanMut::SeqScan(ss), ParallelNodeShared::SeqScan(p)) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                ::nodeseqscan::exec_seq_scan_initialize_worker(ss, estate, p)
                            }
                            (
                                ParallelScanMut::IndexScan(is),
                                ParallelNodeShared::IndexScan(p),
                            ) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                ::nodeindexscan::exec_index_scan_initialize_worker(is, estate, p)
                            }
                            (
                                ParallelScanMut::IndexOnlyScan(ios),
                                ParallelNodeShared::IndexScan(p),
                            ) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                ::nodeindexonlyscan::exec_index_only_scan_initialize_worker(
                                    ios, estate, p,
                                )
                            }
                            (
                                ParallelScanMut::BitmapHeapScan(bhs),
                                ParallelNodeShared::BitmapHeapScan(p),
                            ) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                ::nodebitmapheapscan::exec_bitmap_heap_initialize_worker(bhs, p);
                                Ok(())
                            }
                            (
                                ParallelScanMut::HashJoin(hj),
                                ParallelNodeShared::HashJoin(p),
                            ) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                hj.hash.state.set_parallel_state(p);
                                Ok(())
                            }
                            (
                                ParallelScanMut::Append(ap),
                                ParallelNodeShared::Append(p),
                            ) => {
                                let p = Arc::clone(p);
                                drop(nodes);
                                ::nodeappend::exec_append_initialize_worker(ap, p);
                                Ok(())
                            }
                            _ => panic!(
                                "ExecParallelInitializeWorker (execParallel.c): shared entry \
                                 kind mismatch for plan node {id}"
                            ),
                        }
                    })?;
                    crate::procnode::exec_set_tuple_bound(exec.tuples_needed, ps);
                }
                Ok(())
            })
        })?;

        let save = ::instrument::instr_start_parallel_query();

        let count = if exec.tuples_needed < 0 { 0 } else { exec.tuples_needed as u64 };
        parallel::gtrace("w.run.begin");
        crate::execmain::executor_run_seam(qd, ForwardScanDirection, count, &mut receiver)?;
        parallel::gtrace("w.run.end");
        crate::execmain::executor_finish_seam(qd)?;

        {
            let mut usage = exec.usage.lock().unwrap_or_else(|e| e.into_inner());
            usage[me as usize] = (::instrument::instr_end_parallel_query(&save), WalUsage::default());
        }

        if let Some(si) = &exec.instrumentation {
            let report = querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("worker executor state");
                x.with_mut(|d| {
                    let es = &mut d.estate;
                    for i in es.es_instrumentation.iter_mut() {
                        ::instrument::instr_end_loop(i);
                    }
                    WorkerInstrReport {
                        instrument: es.es_instrumentation.iter().copied().collect(),
                        sort: es.es_sort_instrumentation.iter().copied().collect(),
                        incsort: es.es_incsort_instrumentation.iter().copied().collect(),
                        agg: es.es_agg_instrumentation.iter().copied().collect(),
                        hash: es.es_hash_instrumentation.iter().copied().collect(),
                        index: es.es_index_instrumentation.iter().copied().collect(),
                        bitmap: es.es_bitmap_instrumentation.iter().copied().collect(),
                    }
                })
            });
            // A relaunched worker (rescan) aggregates into its slot; the
            // per-node aux details are C's in-place DSM structs — latest
            // write wins (execParallel.c:1314-1322).
            let mut workers = si.workers.lock().unwrap_or_else(|e| e.into_inner());
            let slot = &mut workers[me as usize];
            match slot {
                None => *slot = Some(report),
                Some(prev) => {
                    if prev.instrument.len() < report.instrument.len() {
                        prev.instrument
                            .resize(report.instrument.len(), Instrumentation::default());
                    }
                    for (id, wi) in report.instrument.iter().enumerate() {
                        ::instrument::instr_agg_node(&mut prev.instrument[id], wi);
                    }
                    prev.sort = report.sort;
                    prev.incsort = report.incsort;
                    prev.agg = report.agg;
                    prev.hash = report.hash;
                    prev.index = report.index;
                    prev.bitmap = report.bitmap;
                }
            }
        }

        crate::execmain::executor_end_seam(qd)?;
        Ok(())
    };
    // A panic must not skip this cleanup: the qd/params reference the LEADER's
    // arena, and anything left registered would be torn down at thread-exit
    // time, racing the leader freeing that arena after it reaps the worker.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut run));
    let result = match result {
        Ok(r) => r,
        Err(payload) => {
            ::nodeagg::merge::clear_thread_registry();
            querydesc::release_query_desc_seam(qd);
            types_portal::params::free(params);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = receiver.shutdown();
            }));
            std::panic::resume_unwind(payload);
        }
    };
    ::nodeagg::merge::clear_thread_registry();
    if result.is_err() {
        querydesc::release_query_desc_seam(qd);
    } else {
        querydesc::free_query_desc_seam(qd);
    }
    types_portal::params::free(params);
    let _ = receiver.shutdown();
    if inject.contains("pgrust:worker-panic-teardown") {
        panic!("injected parallel worker panic (teardown)");
    }
    result
}

// es_parallel_workers_to_launch/_launched accounting shared by both Gather
// nodes (nodeGather.c:189-190 / nodeGatherMerge.c:230-231).
pub(crate) fn account_workers(estate: &mut EStateData<'_>, pcxt: parallel::ParallelContextId) {
    estate.es_parallel_workers_to_launch += parallel::nworkers_to_launch(pcxt);
    estate.es_parallel_workers_launched += parallel::nworkers_launched(pcxt);
}

static REGISTERED: AtomicI32 = AtomicI32::new(0);

pub fn register_parallel_query_main() {
    if REGISTERED.swap(1, Relaxed) == 0 {
        parallel::register_parallel_worker_entrypoint("ParallelQueryMain", parallel_query_main);
    }
}
