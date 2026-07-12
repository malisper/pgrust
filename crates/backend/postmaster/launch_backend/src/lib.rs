//! launch_backend.c under the thread model: postmaster_child_launch spawns a
//! backend THREAD. fork's implicit inheritance becomes an explicit snapshot
//! (captured in the launcher, applied as the child's first act) followed by
//! C's child-init sequence in order — EXEC_BACKEND-shaped (`Inherited` is
//! save/restore_backend_variables). One-address-space divergences:
//! ClosePostmasterPorts/dsm-detach are no-ops, the returned "pid" is a
//! reserved synthetic MyProcPid, and session identity is MyProcPid/
//! MyProcNumber, never the thread id (docs/strategy.md M5).

use std::sync::atomic::{AtomicI32, Ordering};

use types_core::{init::BackendType, init::BACKEND_NUM_TYPES, pid_t};
use types_startup::{ClientSocket, StartupData};

#[cfg(test)]
mod tests;

fn is_external_connection_backend(backend_type: BackendType) -> bool {
    backend_type == BackendType::Backend || backend_type == BackendType::WalSender
}

fn default_sigquit_handler() {
    interrupt::SignalHandlerForCrashExit()
}

type ChildMainFn = fn(&StartupData) -> !;

enum Main {
    Ported(ChildMainFn),
    Unported(&'static str), // real C main_fn, owning unit not yet ported
    None,                   // NULL in the C table
}

struct ChildProcessKind {
    name: &'static str,
    main_fn: Main,
    shmem_attach: bool,
}

/// C `child_process_kinds[]`, in BackendType order (asserted in tests).
static CHILD_PROCESS_KINDS: [ChildProcessKind; BACKEND_NUM_TYPES] = [
    ChildProcessKind { name: "invalid", main_fn: Main::None, shmem_attach: false },
    ChildProcessKind {
        name: "backend",
        main_fn: Main::Ported(backend_startup::backend_main),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "dead-end backend",
        main_fn: Main::Ported(backend_startup::backend_main),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "autovacuum launcher",
        main_fn: Main::Ported(autovacuum::AutoVacLauncherMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "autovacuum worker",
        main_fn: Main::Ported(autovacuum::AutoVacWorkerMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "bgworker",
        main_fn: Main::Ported(bgworker::BackgroundWorkerMain),
        shmem_attach: true,
    },
    ChildProcessKind { name: "wal sender", main_fn: Main::None, shmem_attach: true },
    ChildProcessKind {
        name: "slot sync worker",
        main_fn: Main::Unported("ReplSlotSyncWorkerMain (backend-replication-slotsync)"),
        shmem_attach: true,
    },
    ChildProcessKind { name: "standalone backend", main_fn: Main::None, shmem_attach: false },
    ChildProcessKind {
        name: "archiver",
        main_fn: Main::Ported(pgarch::PgArchiverMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "bgwriter",
        main_fn: Main::Ported(bgwriter::BackgroundWriterMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "checkpointer",
        main_fn: Main::Ported(checkpointer::CheckpointerMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "io_worker",
        main_fn: Main::Unported("IoWorkerMain (backend-storage-aio-method-worker)"),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "startup",
        main_fn: Main::Ported(postmaster_startup::StartupProcessMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "wal_receiver",
        main_fn: Main::Ported(walreceiver::WalReceiverMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "wal_summarizer",
        main_fn: Main::Ported(walsummarizer::WalSummarizerMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "wal_writer",
        main_fn: Main::Ported(walwriter::WalWriterMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "syslogger",
        main_fn: Main::Ported(sys_logger_main),
        shmem_attach: false,
    },
];

// Seam-shaped: a direct syslogger dep would cycle (syslogger calls
// postmaster_child_launch).
fn sys_logger_main(startup_data: &StartupData) -> ! {
    syslogger_seams::sys_logger_main::call(startup_data)
}

/// PostmasterChildName (launch_backend.c).
pub fn postmaster_child_name(child_type: BackendType) -> &'static str {
    CHILD_PROCESS_KINDS[child_type as usize].name
}

static NEXT_CHILD_PID: AtomicI32 = AtomicI32::new(1000);

fn reserve_child_pid() -> pid_t {
    NEXT_CHILD_PID.fetch_add(1, Ordering::Relaxed)
}

// C waitpid reports a child only after the process is fully dead; announce
// fires before this thread's TLS destructors run, so the reaper must join
// here or a parallel leader can free leader-owned state (execparallel's
// pstmt/param_extern contract) while the worker thread is still tearing down.
static CHILD_THREADS: std::sync::Mutex<Vec<(pid_t, std::thread::JoinHandle<()>)>> =
    std::sync::Mutex::new(Vec::new());

/// Joins the announced child's thread (TLS destructors included). Announce is
/// the closure's last act, so this blocks only for teardown, as waitpid does.
/// A retention park's announce (wretain) is a task end, not a thread end:
/// the marker set before the announce makes this a no-op and the thread's
/// CHILD_THREADS entry stays (re-keyed to the next task's pid at claim).
pub fn join_announced_child(pid: pid_t) {
    if wpool::take_parked_announce(pid) {
        return;
    }
    let handle = {
        let mut t = CHILD_THREADS.lock().unwrap_or_else(|e| e.into_inner());
        let Some(idx) = t.iter().position(|(p, _)| *p == pid) else { return };
        t.swap_remove(idx).1
    };
    let _ = handle.join();
}

// Fork-inherited postmaster globals, applied to the fresh thread's TLS first;
// per-child state is deliberately absent (the child-init sequence owns it).
macro_rules! inherited {
    ($($field:ident : $ty:ty = $get:ident / $set:ident;)+) => {
        struct Inherited {
            data_dir: Option<&'static str>,
            $($field: $ty,)+
        }
        impl Inherited {
            fn capture() -> Self {
                Self {
                    data_dir: init_small::globals::DataDir(),
                    $($field: init_small::globals::$get(),)+
                }
            }
            fn apply(&self) {
                if let Some(dd) = self.data_dir {
                    init_small::globals::SetDataDir(dd);
                }
                $(init_small::globals::$set(self.$field);)+
            }
        }
    };
}

inherited! {
    is_postmaster_environment: bool = IsPostmasterEnvironment / SetIsPostmasterEnvironment;
    is_binary_upgrade: bool = IsBinaryUpgrade / SetIsBinaryUpgrade;
    postmaster_pid: pid_t = PostmasterPid / SetPostmasterPid;
    data_directory_mode: i32 = data_directory_mode / set_data_directory_mode;
    output_file_name: [u8; types_core::MAXPGPATH] = OutputFileName / SetOutputFileName;
    my_exec_path: [u8; types_core::MAXPGPATH] = my_exec_path / set_my_exec_path;
    pkglib_path: [u8; types_core::MAXPGPATH] = pkglib_path / set_pkglib_path;
    date_style: i32 = DateStyle / SetDateStyle;
    date_order: i32 = DateOrder / SetDateOrder;
    interval_style: i32 = IntervalStyle / SetIntervalStyle;
    enable_fsync: bool = enableFsync / set_enableFsync;
    allow_system_table_mods: bool = allowSystemTableMods / set_allowSystemTableMods;
    work_mem: i32 = work_mem / set_work_mem;
    hash_mem_multiplier: f64 = hash_mem_multiplier / set_hash_mem_multiplier;
    maintenance_work_mem: i32 = maintenance_work_mem / set_maintenance_work_mem;
    max_parallel_maintenance_workers: i32 =
        max_parallel_maintenance_workers / set_max_parallel_maintenance_workers;
    n_buffers: i32 = NBuffers / SetNBuffers;
    max_connections: i32 = MaxConnections / SetMaxConnections;
    max_worker_processes: i32 = max_worker_processes / set_max_worker_processes;
    max_parallel_workers: i32 = max_parallel_workers / set_max_parallel_workers;
    max_backends: i32 = MaxBackends / SetMaxBackends;
    vacuum_buffer_usage_limit: i32 = VacuumBufferUsageLimit / SetVacuumBufferUsageLimit;
    vacuum_cost_page_hit: i32 = VacuumCostPageHit / SetVacuumCostPageHit;
    vacuum_cost_page_miss: i32 = VacuumCostPageMiss / SetVacuumCostPageMiss;
    vacuum_cost_page_dirty: i32 = VacuumCostPageDirty / SetVacuumCostPageDirty;
    vacuum_cost_limit: i32 = VacuumCostLimit / SetVacuumCostLimit;
    vacuum_cost_delay: f64 = VacuumCostDelay / SetVacuumCostDelay;
    commit_timestamp_buffers: i32 = commit_timestamp_buffers / set_commit_timestamp_buffers;
    multixact_member_buffers: i32 = multixact_member_buffers / set_multixact_member_buffers;
    multixact_offset_buffers: i32 = multixact_offset_buffers / set_multixact_offset_buffers;
    notify_buffers: i32 = notify_buffers / set_notify_buffers;
    serializable_buffers: i32 = serializable_buffers / set_serializable_buffers;
    subtransaction_buffers: i32 = subtransaction_buffers / set_subtransaction_buffers;
    transaction_buffers: i32 = transaction_buffers / set_transaction_buffers;
}

// The per-task half of the child thread body (InitPostmasterChild through the
// exit announce): the spawn closure runs it after the thread prelude; a wpool
// standby runs it on claim with the prelude already paid.
fn run_child_task(
    child_type: BackendType,
    child_pid: pid_t,
    child_slot: i32,
    startup_data: StartupData,
    client_sock: Option<ClientSocket>,
) {
    let main_fn: ChildMainFn = match CHILD_PROCESS_KINDS[child_type as usize].main_fn {
        Main::Ported(f) => f,
        Main::Unported(what) => panic!(
            "run_child_task: {} unported (child kind \"{}\")",
            what,
            CHILD_PROCESS_KINDS[child_type as usize].name
        ),
        Main::None => panic!(
            "run_child_task: no main_fn for child kind \"{}\"",
            CHILD_PROCESS_KINDS[child_type as usize].name
        ),
    };

    if is_external_connection_backend(child_type) {
        let StartupData::Backend(bsdata) = &startup_data else { unreachable!() };
        backend_startup::conn_timing::set_socket_create(bsdata.socket_created);
        backend_startup::conn_timing::set_fork_start(bsdata.fork_started);
        backend_startup::conn_timing::set_fork_end(
            timestamp_seams::get_current_timestamp::call(),
        );
    }

    // ClosePostmasterPorts: no-op, shared fd table (module doc).
    if init_small::wretain::warm_claim() {
        // Retained thread (wretain): the once-per-thread half (wait-event
        // pipe, latch wait set, sigmask, SIGQUIT disposition) survived the
        // park; only the per-task pid/start-time identity refreshes. The
        // wakeup registry is keyed by task pid (WakeupOtherProc is SetLatch's
        // cross-thread wake), so it must follow the fresh synthetic pid —
        // a stale key silently drops every wake to this thread (P1 wedge:
        // repeated parallel queries, workers asleep in shm_mq/CV waits).
        miscinit::InitProcessGlobals(child_pid);
        waiteventset_seams::rekey_wakeup_registry::call();
    } else {
        miscinit::InitPostmasterChild(child_pid)
            .unwrap_or_else(|e| panic!("InitPostmasterChild failed: {e:?}"));
        // InitPostmasterChild's SIGQUIT default; miscinit can't reach interrupt.
        procsignal::pqsignal_thread(
            libc::SIGQUIT,
            procsignal::ThreadSignalHandler::Simple(default_sigquit_handler),
        );
    }

    // !shmem_attach detach + context switch: no-ops (module doc).
    init_small::globals::SetMyPMChildSlot(child_slot);
    if let Some(cs) = client_sock {
        init_small::globals::SetMyClientSocket(cs);
    }

    let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        main_fn(&startup_data)
    })) else {
        unreachable!("child main_fn returns !")
    };
    // proc_exit's deferred half: the unwind above ran the stack's Drop glue
    // with the session state still alive; the exit-callback stacks run here
    // at the thread top, in C's order. Crash payloads (exit_thread_raw,
    // PanicExitThread, raw panics) never defer and skip the drain like C's
    // _exit. A panic escaping the drain is announced SIGABRT below.
    let payload = match payload.downcast_ref::<ipc::ProcExitThread>() {
        Some(p) => {
            let code = p.code;
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ipc::run_deferred_exit_callbacks(code)
            })) {
                Ok(final_code) => Box::new(ipc::ProcExitThread { code: final_code })
                    as Box<dyn std::any::Any + Send>,
                Err(crash) => crash,
            }
        }
        None => payload,
    };
    // C's process death closes fds; without this the peer never sees EOF.
    if let Some(cs) = client_sock {
        unsafe { libc::close(cs.sock) };
    }
    // C wait status: ProcExitThread == WIFEXITED; KilledBySignal ==
    // WTERMSIG(signo); other payloads == WTERMSIG(SIGABRT).
    let exitstatus = payload
        .downcast_ref::<ipc::ProcExitThread>()
        .map(|p| p.code << 8)
        .or_else(|| payload.downcast_ref::<ipc::KilledBySignal>().map(|k| k.signo))
        .unwrap_or(libc::SIGABRT);
    // Park in flight (wretain): the reaper must treat this announce as a
    // task end, not a thread end. Marked before the announce so the reaper
    // can never observe the announce without the marker.
    if exitstatus == 0
        && init_small::wretain::parking()
        && init_small::wretain::proc_retained()
        && init_small::wretain::sinval_retained()
    {
        wpool::mark_parked_announce(child_pid);
    }
    if postmaster_seams::announce_child_exit::is_installed() {
        postmaster_seams::announce_child_exit::call(child_pid, exitstatus);
    } else {
        std::panic::resume_unwind(payload);
    }
}

/// postmaster_child_launch (launch_backend.c). Returns the child's reserved
/// MyProcPid, or -1 if the thread could not be spawned.
pub fn postmaster_child_launch(
    child_type: BackendType,
    child_slot: i32,
    mut startup_data: StartupData,
    client_sock: Option<ClientSocket>,
) -> pid_t {
    debug_assert!(
        init_small::globals::IsPostmasterEnvironment()
            && !init_small::globals::IsUnderPostmaster()
    );

    if is_external_connection_backend(child_type) {
        let StartupData::Backend(bsdata) = &mut startup_data else {
            panic!("postmaster_child_launch: {child_type:?} launched without BackendStartupData")
        };
        bsdata.fork_started = timestamp_seams::get_current_timestamp::call();
    }

    let kind = &CHILD_PROCESS_KINDS[child_type as usize];
    let main_fn: ChildMainFn = match kind.main_fn {
        Main::Ported(f) => f,
        Main::Unported(what) => {
            panic!("postmaster_child_launch: {} unported (child kind \"{}\")", what, kind.name)
        }
        Main::None => {
            panic!("postmaster_child_launch: no main_fn for child kind \"{}\"", kind.name)
        }
    };

    let child_pid = reserve_child_pid();
    let inherited = Inherited::capture();
    let guc_snapshot = guc::store::capture_nondefault_variables();

    let spawned = std::thread::Builder::new()
        .name(format!("pg:{}:{}", kind.name, child_pid))
        .stack_size(child_thread_stack_size())
        .spawn(move || {
            // Thread-scoped (a retained thread reuses its slot across tasks):
            // the local latch slab slot returns on every thread exit —
            // announce fallthrough and panic unwind alike.
            let _local_latch_release = miscinit::LocalLatchReleaseGuard::new();

            inherited.apply();

            // C records the stack base once in main(); each thread owns its own.
            let _ = stack_depth::set_stack_base();

            guc::store::initialize_guc_options_for_child(&guc_snapshot)
                .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                .unwrap_or_else(|e| panic!("child GUC restore failed: {e:?}"));

            run_child_task(child_type, child_pid, child_slot, startup_data, client_sock);
        });

    match spawned {
        Ok(handle) => {
            CHILD_THREADS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((child_pid, handle));
            child_pid
        }
        Err(_) => -1,
    }
}

// C backends run on the process stack (RLIMIT_STACK); a raised-from-rlimit
// max_stack_depth needs the same real budget here, so child threads reserve
// the finite rlimit (env RUST_MIN_STACK still wins when larger; std ignores
// it once stack_size() is explicit). Unlimited/unknown rlimit reserves 16MiB
// (or the max_stack_depth budget + slop when the GUC was raised above that):
// reserve is address space only, but 64MiB x max_connections=500 was 32 GB
// of VSZ for zero benefit under `ulimit -s unlimited`.
fn child_thread_stack_size() -> usize {
    let rlim = stack_depth::get_stack_depth_rlimit();
    let unlimited_reserve =
        (16usize << 20).max(stack_depth::max_stack_depth_bytes().max(0) as usize + (2 << 20));
    let rlim = if rlim > 0 && rlim < isize::MAX { rlim as usize } else { unlimited_reserve };
    let min_stack = std::env::var("RUST_MIN_STACK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    rlim.max(min_stack).max(2 << 20)
}

pub fn init_seams() {
    postmaster_seams::parallel_pool_dispatch::set(wpool::dispatch);
    postmaster_seams::parallel_pool_retire_db::set(wpool::retire_db);
}

pub mod wpool {
    //! §3.1 P-pool + the phase-4 retention increment: a process-lifetime pool
    //! of parked standby threads for BGWORKER_CLASS_PARALLEL launches. A
    //! standby pre-pays the spawn-path fixed costs (thread create,
    //! inherited-globals apply, GUC store build + postmaster-snapshot
    //! restore); a claim hands it the same StartupData the postmaster spawn
    //! path would build and it runs the unchanged per-task child body.
    //!
    //! Retention (wretain, PGRUST_NO_RELCACHE_RETAIN kill switch): after a
    //! clean task a standby parks holding its PGPROC + sinval slot + warm
    //! relcache/catcache, and later claims skip the cold init (postinit warm
    //! arm) and drain sinval instead of nuking caches. Parked standbys are
    //! pinned to the database their caches were built against; dispatch only
    //! hands them same-db tasks (a miss falls back to the postmaster spawn
    //! path, which is always correct). Each task runs under a fresh synthetic
    //! pid so the reaper's async processing of task N's exit announce can
    //! never touch task N+1's bookkeeping. With retention off, threads rotate
    //! — one task per standby — exactly as phase 1 shipped.

    use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
    use std::sync::mpsc::{Receiver, SyncSender};
    use std::sync::Mutex;

    use types_core::{init::BackendType, pid_t, InvalidOid, Oid};
    use types_startup::{BgWorkerStartupData, StartupData};

    struct StandbyTask {
        child_pid: pid_t,
        child_slot: i32,
        startup_data: StartupData,
    }

    struct Standby {
        pid: pid_t,
        tx: SyncSender<StandbyTask>,
        // Database the retained caches are pinned to; InvalidOid = fresh
        // standby, matches any task.
        db: Oid,
        // Signaled by the standby thread after a retire has pushed its
        // retained PGPROC back on the freelist; a cross-db miss waits on this
        // so the deferred postmaster spawn cannot race the release
        // (InitProcess FATALs on an empty bgworker freelist).
        released: Receiver<()>,
    }

    static AVAILABLE: Mutex<Vec<Standby>> = Mutex::new(Vec::new());
    // Live standby THREADS (prelude, parked, or running a task). Incremented
    // at spawn, decremented by the thread itself on any exit (rotation or
    // retire). Claims do NOT decrement: a retention claim comes back, and
    // counting it as gone made maintain() over-replenish, overshoot the
    // target at park, and shrink-retire live retained standbys.
    static POPULATION: AtomicI32 = AtomicI32::new(0);
    // Task-pids whose exit announce parked the thread; consumed by
    // join_announced_child. Tiny (<= pool size).
    static PARKED_ANNOUNCES: Mutex<Vec<pid_t>> = Mutex::new(Vec::new());
    // Bumped by flush_for_crash: shared memory is about to be reset, so a
    // woken standby must abandon (not tear down) its retained identity. A
    // standby compares against the value it captured before parking.
    static CRASH_EPOCH: AtomicU64 = AtomicU64::new(0);
    // Bumped by every flush (reload + crash): a standby finishing a task
    // after a flush must not re-park itself into the drained pool (its GUC
    // prelude snapshot predates the reload; on crash the pool is dead).
    static FLUSH_EPOCH: AtomicU64 = AtomicU64::new(0);

    fn available() -> std::sync::MutexGuard<'static, Vec<Standby>> {
        AVAILABLE.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn target() -> i32 {
        if std::env::var_os("PGRUST_NO_WORKER_POOL").is_some() {
            return 0;
        }
        init_small::globals::max_parallel_workers()
    }

    pub(super) fn mark_parked_announce(pid: pid_t) {
        PARKED_ANNOUNCES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(pid);
    }

    pub(super) fn take_parked_announce(pid: pid_t) -> bool {
        let mut v = PARKED_ANNOUNCES.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = v.iter().position(|p| *p == pid) {
            v.swap_remove(i);
            true
        } else {
            false
        }
    }

    /// Postmaster thread only: Inherited/GUC snapshot capture must match
    /// postmaster_child_launch's launcher-side capture.
    pub fn maintain() {
        while POPULATION.load(Relaxed) < target() {
            if !spawn_standby() {
                return;
            }
        }
        // POPULATION only falls when the woken threads exit; bound the pops
        // locally or this loop would drain the whole pool.
        let mut excess = POPULATION.load(Relaxed) - target();
        while excess > 0 {
            let Some(sb) = available().pop() else { return };
            excess -= 1;
            drop(sb); // closed channel retires the standby
        }
    }

    /// DROP DATABASE rider (parallel_pool_retire_db seam): parked standbys
    /// pinned to the dropped database can never be claimed again (dispatch is
    /// same-db-only) but each holds a bgworker PGPROC and a POPULATION charge;
    /// left parked they exhaust the InitProcess freelist for the postmaster
    /// fallback spawn ("parallel worker failed to initialize" where C
    /// launches fine) and block maintain() from replenishing fresh standbys.
    /// Runs on the dropping backend's thread; pool-lock only, no registry
    /// lock (claim path untouched).
    pub fn retire_db(dboid: Oid) {
        available().retain(|s| s.db != dboid); // dropped channels retire the standbys
    }

    /// Retire every parked standby (config reload): the next maintain()
    /// respawns with a fresh postmaster GUC snapshot. Woken standbys with a
    /// retained identity release it against live shared memory.
    pub fn flush() {
        FLUSH_EPOCH.fetch_add(1, Relaxed);
        available().clear(); // dropped channels retire the standbys
    }

    /// Crash reinit: shared memory is about to be reset wholesale, so woken
    /// standbys must NOT touch it — bump the epoch before dropping their
    /// channels.
    pub fn flush_for_crash() {
        CRASH_EPOCH.fetch_add(1, Relaxed);
        flush();
    }

    fn spawn_standby() -> bool {
        let spawn_pid = super::reserve_child_pid();
        let inherited = super::Inherited::capture();
        let guc_snapshot = guc::store::capture_nondefault_variables();
        let (tx, rx) = std::sync::mpsc::sync_channel::<StandbyTask>(1);
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let spawned = std::thread::Builder::new()
            .name(format!("pg:standby:{spawn_pid}"))
            .stack_size(super::child_thread_stack_size())
            .spawn(move || {
                // Any exit — rotation, retire, or a prelude panic — drops the
                // population charge exactly once.
                struct PopulationCharge;
                impl Drop for PopulationCharge {
                    fn drop(&mut self) {
                        POPULATION.fetch_sub(1, Relaxed);
                    }
                }
                let _charge = PopulationCharge;
                // Thread-scoped, not per-task: a parked standby keeps its
                // local latch slot warm; only thread exit returns it.
                let _local_latch_release = miscinit::LocalLatchReleaseGuard::new();
                inherited.apply();
                let _ = stack_depth::set_stack_base();
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                    .unwrap_or_else(|e| panic!("standby GUC restore failed: {e:?}"));
                standby_loop(spawn_pid, rx, ack_tx);
            });
        match spawned {
            Ok(handle) => {
                super::CHILD_THREADS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((spawn_pid, handle));
                POPULATION.fetch_add(1, Relaxed);
                available().push(Standby {
                    pid: spawn_pid,
                    tx,
                    db: InvalidOid,
                    released: ack_rx,
                });
                true
            }
            Err(_) => false,
        }
    }

    fn standby_loop(spawn_pid: pid_t, mut rx: Receiver<StandbyTask>, mut ack_tx: SyncSender<()>) {
        // CHILD_THREADS key for our JoinHandle; re-keyed to each task's pid
        // so a rotation exit's announce joins this thread, while a park's
        // announce (marked) does not.
        let mut thread_key = spawn_pid;
        // Crash epoch our retained identity was parked under; a bump between
        // park and wake means the shared slots were reset out from under it.
        let mut parked_crash_epoch = CRASH_EPOCH.load(Relaxed);
        loop {
            match rx.recv() {
                Ok(task) => {
                    bgworker::gtrace("w.pool.task");
                    rekey_child_thread(thread_key, task.child_pid);
                    thread_key = task.child_pid;
                    let pre_flush = FLUSH_EPOCH.load(Relaxed);
                    let pre_crash = CRASH_EPOCH.load(Relaxed);
                    // §5 leak guard: a claimed standby must not carry a
                    // previous session's GUC bind.
                    debug_assert!(!guc::store::session_bound());
                    init_small::wretain::begin_task(
                        init_small::wretain::retention_enabled(),
                    );
                    super::run_child_task(
                        BackendType::BgWorker,
                        task.child_pid,
                        task.child_slot,
                        task.startup_data,
                        None,
                    );
                    let parked = init_small::wretain::confirm_parked();
                    if parked && FLUSH_EPOCH.load(Relaxed) != pre_flush {
                        // The pool was flushed while we ran: do not re-park a
                        // pre-flush prelude into the drained pool.
                        release_retained_identity(pre_crash);
                        break;
                    }
                    if parked {
                        // Zero-leak: a parked standby carries no session
                        // identity (GUC bind already dropped by its guard).
                        miscinit::ResetSessionIdentityForRetainedPark();
                        // Back to the fresh-thread mode so the next claim's
                        // init-processing asserts hold.
                        miscinit::SetProcessingMode(
                            types_core::init::ProcessingMode::InitProcessing,
                        );
                        ipc::reset_exit_state_for_retained_park();
                        let (tx2, rx2) = std::sync::mpsc::sync_channel::<StandbyTask>(1);
                        let (ack_tx2, ack_rx2) = std::sync::mpsc::sync_channel::<()>(1);
                        parked_crash_epoch = pre_crash;
                        available().push(Standby {
                            pid: task.child_pid,
                            tx: tx2,
                            db: init_small::wretain::retained_db(),
                            released: ack_rx2,
                        });
                        rx = rx2;
                        ack_tx = ack_tx2;
                        continue;
                    }
                    // Rotation (retention off, task error, or partial park):
                    // release whatever the park arms retained, then exit; the
                    // reaper joins us through the announce.
                    release_retained_identity(pre_crash);
                    break;
                }
                Err(_) => {
                    // Retired while parked (maintain shrink / reload flush /
                    // cross-db miss) or never claimed: release any retained
                    // identity, drop our reaper entry (nothing announced this
                    // wake), exit. The ack must follow the identity release —
                    // a cross-db miss blocks on it before deferring to the
                    // postmaster spawn, which needs our PGPROC on the
                    // freelist.
                    release_retained_identity(parked_crash_epoch);
                    let _ = ack_tx.try_send(());
                    let mut t =
                        super::CHILD_THREADS.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(i) = t.iter().position(|(p, _)| *p == thread_key) {
                        t.swap_remove(i);
                    }
                    break;
                }
            }
        }
    }

    // The identity survived (parked shape: latch already local + disowned,
    // locks/lock-group released) exactly when MyProc is still set — parking
    // is latched before the teardown and constant across it, so proc and
    // sinval always take the same branch. Sinval cleanup keys off
    // MyProcNumber, which KillRetainedProc clears, so it runs first.
    fn release_retained_identity(park_epoch: u64) {
        if CRASH_EPOCH.load(Relaxed) == park_epoch && lmgr_proc::MyProc().is_some() {
            sinval::CleanupInvalidationState()
                .expect("CleanupInvalidationState failed releasing retained identity");
            lmgr_proc::KillRetainedProc();
        }
        init_small::wretain::clear_identity();
    }

    /// parallel_pool_dispatch seam impl. Runs on the registering backend's
    /// thread, under the bgworker registry lock. A panic here must not unwind
    /// into the registry critical section (it would leak the slot's
    /// parallel_register_count admission charge): contain it and report a
    /// miss so the caller takes the postmaster spawn path.
    pub fn dispatch(slot: i32, generation: u64, dboid: Oid) -> i32 {
        match std::panic::catch_unwind(|| dispatch_inner(slot, generation, dboid)) {
            Ok(pid) => pid,
            Err(_) => {
                eprintln!(
                    "wpool: parallel worker pool dispatch panicked (slot {slot}); \
                     falling back to postmaster launch"
                );
                0
            }
        }
    }

    fn dispatch_inner(slot: i32, generation: u64, dboid: Oid) -> i32 {
        loop {
            // Prefer a standby whose retained caches match the task's
            // database, then a fresh one; a mismatched standby stays parked
            // (its warmth is only legal for its own db).
            let sb = {
                let mut avail = available();
                let idx = avail
                    .iter()
                    .position(|s| s.db == dboid)
                    .or_else(|| avail.iter().position(|s| s.db == InvalidOid));
                match idx {
                    Some(i) => avail.swap_remove(i),
                    None => {
                        // Cross-db miss: retained parks pinned to other live
                        // databases each hold a bgworker-class PGPROC, and at
                        // population target they exhaust the InitProcess
                        // freelist — the deferred postmaster spawn FATALs
                        // where C (fresh spawn per query) succeeds. Retire
                        // one mismatched park per missed dispatch and block
                        // on its release ack so the freed PGPROC is on the
                        // freelist before the deferral; maintain() then
                        // replenishes a fresh any-db standby. Warm-claim hit
                        // paths are untouched.
                        let Some(v) = avail.iter().position(|s| s.db != InvalidOid) else {
                            return 0;
                        };
                        let Standby { tx, released, .. } = avail.swap_remove(v);
                        drop(avail);
                        drop(tx); // closed channel retires the standby
                        bgworker::gtrace("w.pool.retire_mismatch");
                        // Timeout = fail open: worst case is today's behavior
                        // (deferral races the release), never a hang under
                        // the registry lock.
                        let _ = released.recv_timeout(std::time::Duration::from_secs(2));
                        return 0;
                    }
                }
            };
            let Some(child_slot) =
                pmchild_seams::assign_postmaster_child_slot::call(BackendType::BgWorker)
            else {
                available().push(sb);
                return 0;
            };
            // Fresh per-task pid: the previous task's exit announce may still
            // be queued at the postmaster; reusing its pid would let the
            // reaper's cleanup land on the new task.
            let task_pid = super::reserve_child_pid();
            pmchild_seams::set_child_pid::call(child_slot, task_pid);
            let task = StandbyTask {
                child_pid: task_pid,
                child_slot,
                startup_data: StartupData::BgWorker(BgWorkerStartupData { slot, generation }),
            };
            match sb.tx.send(task) {
                Ok(()) => {
                    bgworker::gtrace("w.pool.dispatch");
                    return task_pid;
                }
                Err(_) => {
                    pmchild_seams::release_postmaster_child_slot::call(child_slot);
                }
            }
        }
    }

    fn rekey_child_thread(old_pid: pid_t, new_pid: pid_t) {
        if old_pid == new_pid {
            return;
        }
        let mut t = super::CHILD_THREADS.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = t.iter_mut().find(|(p, _)| *p == old_pid) {
            entry.0 = new_pid;
        }
    }
}
