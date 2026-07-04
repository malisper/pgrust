// Substrate e2e: real worker threads through bgworker::BackgroundWorkerMain,
// tuples back through shm_mq/tqueue, C-shaped error rethrow. Harness deltas
// from a live server, each the narrowest available: no postmaster thread (the
// test drives the registered-worker launch/reap the way serverloop would),
// database_id InvalidOid (no catalog connect), hand-built MVCC active
// snapshot (Serialize/Restore is unit-proven in snapmgr).
use std::any::Any;
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Once};

use init_small::globals as g;
use types_core::InvalidOid;
use types_error::{PgError, PgResult, ERROR};
use types_startup::StartupData;

const N_TUPLES: usize = 500;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static NEXT_PID: AtomicI32 = AtomicI32::new(9000);

// IsUnderPostmaster is thread-local; scope it to registration so the latch
// wait paths never arm a postmaster-death watch (no postmaster pipe here).
fn launch_as_if_under_postmaster(
    pcxt: parallel::ParallelContextId,
) -> PgResult<i32> {
    g::SetIsUnderPostmaster(true);
    let r = parallel::LaunchParallelWorkers(pcxt);
    g::SetIsUnderPostmaster(false);
    r
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        // Production seam wiring, then boot: test_boot's stubs are
        // install-if-absent, so the real implementations win.
        seams_init::init_all();
        test_boot::boot_wal("parallel_substrate_e2e");
        let _ = waiteventset::InitializeWaitEventSupport();
        let _ = latch::InitializeLatchWaitSet();
        leader_latch();
        pmsignal::PMSignalShmemInit(64);
        bgworker::BackgroundWorkerShmemInit();
        procsignal::ProcSignalShmemInit();
        procsignal::ProcSignalInit(&[]).unwrap();
        miscinit::SetAuthenticatedUserId(10);
        miscinit::SetSessionAuthorization(10, true).unwrap();
        parallel::register_parallel_worker_entrypoint("substrate_e2e_main", e2e_worker_main);
        parallel::register_parallel_worker_entrypoint("substrate_e2e_error", e2e_error_main);
    });
}

fn leader_latch() {
    let procno = g::MyProcNumber();
    let h = types_storage::latch::LatchHandle::proc(procno);
    lmgr_proc::GetPGProcByNumber(procno)
        .procLatch
        .owner_pid
        .store(0, std::sync::atomic::Ordering::SeqCst);
    latch::OwnLatch(h).unwrap();
    g::SetMyLatch(Some(h));
}

struct E2eShared {
    queues: Vec<Arc<shm_mq::ShmMq>>,
    instr: Mutex<Vec<types_core::instrument::Instrumentation>>,
}

fn tuple_image(worker: usize, i: usize) -> Vec<u8> {
    (0..(16 + (i * 7) % 96))
        .map(|j| (worker * 131 + i * 31 + j) as u8)
        .collect()
}

fn e2e_worker_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let me = parallel::ParallelWorkerNumber() as usize;
    let private = shared.private().expect("e2e private missing");
    let e2e = private.downcast_ref::<E2eShared>().expect("e2e private type");
    let mq = Arc::clone(&e2e.queues[me]);
    mq.set_sender(g::MyProcNumber());
    let mut tx = shm_mq::shm_mq_attach(mq);
    let mut instr = types_core::instrument::Instrumentation::default();
    instrument::instr_init(&mut instr, 0);
    instrument::instr_start_node(&mut instr);
    let mut sent = 0f64;
    for i in 0..N_TUPLES {
        let img = tuple_image(me, i);
        if !tqueue::tqueue_send_bytes(&mut tx, &img)? {
            break;
        }
        sent += 1.0;
    }
    instrument::instr_stop_node(&mut instr, sent);
    instrument::instr_end_loop(&mut instr);
    e2e.instr.lock().unwrap_or_else(|e| e.into_inner())[me] = instr;
    Ok(())
}

fn e2e_error_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Err(Box::new(
        PgError::new(types_error::FATAL, "worker exploded on purpose")
            .with_sqlstate(types_error::ERRCODE_DIVISION_BY_ZERO)
            .with_context("inner worker frame"),
    ))
}

// The postmaster stand-in: what serverloop's maybe_start_bgworkers + the
// reaper do, driven synchronously by the test.
fn launch_registered_workers() -> Vec<std::thread::JoinHandle<i32>> {
    bgworker::BackgroundWorkerStateChange(true);
    let mut joins = Vec::new();
    for idx in bgworker::registered_indexes() {
        if bgworker::rw_pid(idx) != 0 || bgworker::rw_terminate(idx) {
            continue;
        }
        let pid = NEXT_PID.fetch_add(1, Relaxed);
        let slot = bgworker::rw_shmem_slot(idx);
        let generation = bgworker::slot_generation(slot);
        bgworker::set_rw_pid(idx, pid);
        bgworker::ReportBackgroundWorkerPID(idx);
        let data_dir: &'static str =
            Box::leak(test_boot::data_dir().to_str().unwrap().to_string().into_boxed_str());
        // launch_backend's per-thread GUC boot (the postmaster snapshot).
        let guc_snapshot = guc::store::capture_nondefault_variables();
        let handle = std::thread::Builder::new()
            .name(format!("pg:parallel-e2e-worker:{pid}"))
            .spawn(move || {
                g::SetDataDir(data_dir);
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                    .unwrap();
                g::SetMaxConnections(64);
                g::set_max_worker_processes(2);
                g::SetMaxBackends(64 + 3 + 2 + 2 + 2);
                g::SetMyProcPid(pid);
                g::SetMyDatabaseId(InvalidOid);
                g::set_transaction_buffers(64);
                g::set_subtransaction_buffers(64);
                g::set_enableFsync(false);
                fd::InitFileAccess();
                waiteventset::InitializeWaitEventSupport().unwrap();
                latch::InitializeLatchWaitSet().unwrap();
                let sd = StartupData::BgWorker(types_startup::BgWorkerStartupData {
                    slot,
                    generation,
                });
                let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    bgworker::BackgroundWorkerMain(&sd)
                }))
                .unwrap_err();
                let code = payload
                    .downcast_ref::<ipc::ProcExitThread>()
                    .map(|p| p.code)
                    .unwrap_or_else(|| panic!("worker died without proc_exit"));
                bgworker::ReportBackgroundWorkerExit(idx);
                code
            })
            .unwrap();
        joins.push(handle);
    }
    joins
}

fn push_test_active_snapshot() {
    let mcx = Box::leak(Box::new(mcx::MemoryContext::new("e2e-snap"))).mcx();
    let mut d =
        types_snapshot::SnapshotData::sentinel(mcx, types_snapshot::SnapshotType::SNAPSHOT_MVCC);
    d.xmin = 3;
    d.xmax = 3;
    d.curcid.set(0);
    d.vistest = types_core::GlobalVisStateHandle::new(0);
    let snap: snapmgr::Snapshot = std::rc::Rc::new(d);
    snapmgr::PushActiveSnapshot(&snap).unwrap();
}

#[test]
fn substrate_happy_path_with_launch_fewer() {
    let _s = serial();
    setup();

    g::SetMyDatabaseId(InvalidOid);
    push_test_active_snapshot();
    xact::EnterParallelMode();

    // Ask for 3; only 2 bgworker slots exist (max_worker_processes=2): the
    // third registration fails and C's contract is to run with fewer.
    let pcxt = parallel::CreateParallelContext("postgres", "substrate_e2e_main", 3).unwrap();
    parallel::InitializeParallelDSM(pcxt).unwrap();

    let leader_procno = g::MyProcNumber();
    let queues: Vec<Arc<shm_mq::ShmMq>> = (0..3)
        .map(|_| {
            let mq = shm_mq::shm_mq_create(tqueue::PARALLEL_TUPLE_QUEUE_SIZE);
            mq.set_receiver(leader_procno);
            mq
        })
        .collect();
    let mut readers: Vec<tqueue::TupleQueueReader> = queues
        .iter()
        .map(|mq| tqueue::TupleQueueReader::new(shm_mq::shm_mq_attach(Arc::clone(mq))))
        .collect();
    let e2e_shared = Arc::new(E2eShared {
        queues,
        instr: Mutex::new(vec![types_core::instrument::Instrumentation::default(); 3]),
    });
    parallel::set_private(pcxt, Arc::clone(&e2e_shared) as Arc<dyn Any + Send + Sync>);

    let launched = launch_as_if_under_postmaster(pcxt).unwrap();
    assert_eq!(launched, 2);
    assert_eq!(parallel::nworkers_launched(pcxt), 2);

    let joins = launch_registered_workers();
    assert_eq!(joins.len(), 2);

    parallel::WaitForParallelWorkersToAttach(pcxt).unwrap();

    let mut got: Vec<Vec<Vec<u8>>> = vec![Vec::new(), Vec::new()];
    let mut done = [false, false];
    while !(done[0] && done[1]) {
        let mut progressed = false;
        for w in 0..2 {
            if done[w] {
                continue;
            }
            let mut d = false;
            if let Some(bytes) = readers[w].next(true, &mut d).unwrap() {
                got[w].push(bytes.to_vec());
                progressed = true;
            }
            if d {
                done[w] = true;
                progressed = true;
            }
        }
        if !progressed {
            std::thread::yield_now();
        }
    }
    for (w, tuples) in got.iter().enumerate() {
        assert_eq!(tuples.len(), N_TUPLES, "worker {w} tuple count");
        for (i, t) in tuples.iter().enumerate() {
            assert_eq!(t, &tuple_image(w, i), "worker {w} tuple {i}");
        }
    }

    parallel::WaitForParallelWorkersToFinish(pcxt).unwrap();

    // ExecParallelRetrieveInstrumentation's shape: InstrAggNode of every
    // worker slot into the leader's node instrumentation.
    let mut leader_instr = types_core::instrument::Instrumentation::default();
    instrument::instr_init(&mut leader_instr, 0);
    for wi in e2e_shared.instr.lock().unwrap_or_else(|e| e.into_inner()).iter().take(2) {
        instrument::instr_agg_node(&mut leader_instr, wi);
    }
    assert_eq!(leader_instr.ntuples, (2 * N_TUPLES) as f64);
    assert_eq!(leader_instr.nloops, 2.0);

    parallel::DestroyParallelContext(pcxt).unwrap();
    xact::ExitParallelMode();
    snapmgr::PopActiveSnapshot().unwrap();

    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
    assert!(!parallel::ParallelContextActive());
}

#[test]
fn worker_error_rethrows_with_c_shape() {
    let _s = serial();
    setup();

    g::SetMyDatabaseId(InvalidOid);
    push_test_active_snapshot();
    xact::EnterParallelMode();

    let pcxt = parallel::CreateParallelContext("postgres", "substrate_e2e_error", 1).unwrap();
    parallel::InitializeParallelDSM(pcxt).unwrap();
    let launched = launch_as_if_under_postmaster(pcxt).unwrap();
    assert_eq!(launched, 1);
    let joins = launch_registered_workers();

    let err = parallel::WaitForParallelWorkersToFinish(pcxt).unwrap_err();
    assert_eq!(err.message(), "worker exploded on purpose");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.level, ERROR); // clamped from FATAL per C
    let ctx = err.context().unwrap();
    assert!(ctx.ends_with("parallel worker"), "context was: {ctx}");
    assert!(ctx.contains("inner worker frame"));

    parallel::DestroyParallelContext(pcxt).unwrap();
    xact::ExitParallelMode();
    snapmgr::PopActiveSnapshot().unwrap();
    for j in joins {
        assert_eq!(j.join().unwrap(), 1);
    }
}
