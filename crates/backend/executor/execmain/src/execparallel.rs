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
}

struct SharedInstrumentation {
    instrument_options: i32,
    workers: Mutex<Vec<Option<WorkerInstrReport>>>,
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
    queues: Mutex<Vec<Arc<shm_mq::ShmMq>>>,
    instrumentation: Option<SharedInstrumentation>,
    usage: Mutex<Vec<(BufferUsage, WalUsage)>>,
}

pub struct ParallelExecutorInfo {
    pub pcxt: parallel::ParallelContextId,
    shared: Arc<ParallelExecShared>,
    tqueue: Vec<Option<shm_mq::ShmMqHandle>>,
    pub reader: Vec<tqueue::TupleQueueReader>,
    pub finished: bool,
    instrumented: bool,
}

fn walk_parallel_aware(node: Option<Node<'_>>) {
    let Some(node) = node else { return };
    let plan = plan_of_node(node);
    if plan.parallel_aware {
        panic!(
            "ExecParallelInitializeDSM (execParallel.c): parallel-aware {:?} — \
             per-node DSM/worker arms land with the parallel scan lanes",
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
        _ => vec![],
    }
}

// ExecSerializePlan, share-not-serialize: the dummy PlannedStmt is built in
// the leader's executor arena and crosses by reference. The resjunk-clearing
// copy is replaced by the worker-side junk-filter suppression in InitPlan
// (same observable: junk columns reach the leader).
fn build_worker_pstmt<'mcx>(
    estate: &EStateData<'mcx>,
    plan_node: Node<'mcx>,
) -> PgResult<&'mcx PlannedStmt<'mcx>> {
    let leader = estate.es_plannedstmt.expect("parallel plan without es_plannedstmt");
    for (i, subplan) in leader.subplans.iter().enumerate() {
        let sp = subplan.as_plan().expect("subplans cell is a plan tree");
        if !sp.parallel_safe {
            // C leaves a NULL hole; NodeList cells cannot be NULL (planner
            // lane owns the hole representation).
            panic!(
                "ExecSerializePlan (execParallel.c): parallel-unsafe subplan {} — \
                 NULL-hole subplan transfer unported",
                i + 1
            );
        }
    }
    let mcx = estate.es_query_cxt;
    let pstmt = PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        queryId: leader.queryId,
        planId: leader.planId,
        hasReturning: false,
        hasModifyingCTE: false,
        canSetTag: true,
        transientPlan: false,
        dependsOnRole: false,
        parallelModeNeeded: false,
        jitFlags: 0,
        planTree: Some(plan_node),
        partPruneInfos: leader.partPruneInfos.clone(),
        rtable: leader.rtable.clone(),
        unprunableRelids: estate.es_unpruned_relids.clone_in(mcx)?,
        permInfos: leader.permInfos.clone(),
        resultRelations: ::types_nodes::list::IntList::nil(),
        appendRelations: NodeList::nil(),
        subplans: leader.subplans.clone(),
        rewindPlanIDs: Bitmapset::empty(),
        rowMarks: NodeList::nil(),
        relationOids: ::types_nodes::list::OidList::nil(),
        invalItems: NodeList::nil(),
        paramExecTypes: leader.paramExecTypes.clone(),
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
) -> Vec<Option<shm_mq::ShmMqHandle>> {
    let me = init_small::globals::MyProcNumber();
    let mut queues = Vec::with_capacity(nworkers.max(0) as usize);
    let mut handles = Vec::with_capacity(nworkers.max(0) as usize);
    for _ in 0..nworkers {
        let mq = shm_mq::shm_mq_create(tqueue::PARALLEL_TUPLE_QUEUE_SIZE);
        mq.set_receiver(me);
        handles.push(Some(shm_mq::shm_mq_attach(Arc::clone(&mq))));
        queues.push(mq);
    }
    *shared.queues.lock().unwrap_or_else(|e| e.into_inner()) = queues;
    handles
}

/// `ExecInitParallelPlan` (execParallel.c).
pub fn exec_init_parallel_plan<'mcx>(
    child_plan: Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    send_params: &Bitmapset<'mcx>,
    nworkers: i32,
    tuples_needed: i64,
) -> PgResult<ParallelExecutorInfo> {
    // ExecSetParamPlanMulti: force initplan outputs before workers read them.
    ::executils::exec_eval_param_exec_params(estate, &crate::nodegather::bms_members(send_params))?;
    walk_parallel_aware(Some(child_plan));

    let pstmt = build_worker_pstmt(estate, child_plan)?;
    let pcxt = parallel::CreateParallelContext("postgres", "ParallelQueryMain", nworkers)?;
    parallel::InitializeParallelDSM(pcxt)?;
    let nworkers = parallel::nworkers(pcxt);

    debug_assert!(estate.es_snapshot.is_some());

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
        instrumentation: instrumented.then(|| SharedInstrumentation {
            instrument_options: estate.es_instrument,
            workers: Mutex::new((0..nworkers).map(|_| None).collect()),
        }),
        usage: Mutex::new(vec![(BufferUsage::default(), WalUsage::default()); nworkers.max(0) as usize]),
    });
    let tqueue = setup_tuple_queues(&shared, nworkers);
    parallel::set_private(pcxt, Arc::clone(&shared) as Arc<dyn Any + Send + Sync>);

    Ok(ParallelExecutorInfo {
        pcxt,
        shared,
        tqueue,
        reader: Vec::new(),
        finished: false,
        instrumented,
    })
}

/// `ExecParallelCreateReaders` (execParallel.c).
pub fn exec_parallel_create_readers(pei: &mut ParallelExecutorInfo) {
    debug_assert!(pei.reader.is_empty());
    let launched = parallel::nworkers_launched(pei.pcxt);
    for i in 0..launched as usize {
        let mut handle = pei.tqueue[i].take().expect("tuple queue handle already taken");
        if let Some(bgwh) = parallel::worker_bgwhandle(pei.pcxt, i) {
            handle.set_handle(bgwh.slot, bgwh.generation);
        }
        pei.reader.push(tqueue::TupleQueueReader::new(handle));
    }
}

/// `ExecParallelReinitialize` (execParallel.c).
pub fn exec_parallel_reinitialize<'mcx>(
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
    if let Some(si) = &pei.shared.instrumentation {
        for w in si.workers.lock().unwrap_or_else(|e| e.into_inner()).iter_mut() {
            *w = None;
        }
    }
    Ok(())
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
        let mut wi = WorkerInstr {
            instrument,
            sort: PgVec::new_in(mcx),
            incsort: PgVec::new_in(mcx),
            agg: PgVec::new_in(mcx),
            hash: PgVec::new_in(mcx),
            index: PgVec::new_in(mcx),
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

    let mq = {
        let queues = exec.queues.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&queues[me as usize])
    };
    mq.set_sender(init_small::globals::MyProcNumber());
    let mut receiver =
        DestReceiver::TupleQueue(tqueue::tqueue_create_DR(shm_mq::shm_mq_attach(mq)));

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

    let run = || -> PgResult<()> {
        crate::execmain::executor_start_seam(qd, exec.eflags)?;

        querydesc::with_qd(qd, |q| {
            let x = q.exec.as_mut().expect("worker ExecutorStart left no exec");
            x.with_mut(|d| {
                let pe = exec.param_exec.lock().unwrap_or_else(|e| e.into_inner());
                for (paramid, value, isnull) in pe.iter() {
                    d.estate.es_param_exec_vals[*paramid as usize] =
                        ParamExecData { value: *value, isnull: *isnull, exec_plan: false };
                }
                if let Some(ps) = d.planstate.as_mut() {
                    crate::procnode::exec_set_tuple_bound(exec.tuples_needed, ps);
                }
            });
        });

        let save = ::instrument::instr_start_parallel_query();

        let count = if exec.tuples_needed < 0 { 0 } else { exec.tuples_needed as u64 };
        crate::execmain::executor_run_seam(qd, ForwardScanDirection, count, &mut receiver)?;
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
                    }
                })
            });
            si.workers.lock().unwrap_or_else(|e| e.into_inner())[me as usize] = Some(report);
        }

        crate::execmain::executor_end_seam(qd)?;
        Ok(())
    };
    let result = run();
    if result.is_err() {
        querydesc::release_query_desc_seam(qd);
    } else {
        querydesc::free_query_desc_seam(qd);
    }
    types_portal::params::free(params);
    let _ = receiver.shutdown();
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
