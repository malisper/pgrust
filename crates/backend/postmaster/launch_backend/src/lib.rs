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
        main_fn: Main::Unported("WalReceiverMain (backend-replication-walreceiver)"),
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
pub fn join_announced_child(pid: pid_t) {
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
    miscinit::InitPostmasterChild(child_pid)
        .unwrap_or_else(|e| panic!("InitPostmasterChild failed: {e:?}"));
    // InitPostmasterChild's SIGQUIT default; miscinit can't reach interrupt.
    procsignal::pqsignal_thread(
        libc::SIGQUIT,
        procsignal::ThreadSignalHandler::Simple(default_sigquit_handler),
    );

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
    // C's process death closes fds; without this the peer never sees EOF.
    if let Some(cs) = client_sock {
        unsafe { libc::close(cs.sock) };
    }
    // C wait status: ProcExitThread == WIFEXITED; other payloads == WTERMSIG(SIGABRT).
    let exitstatus = payload
        .downcast_ref::<ipc::ProcExitThread>()
        .map(|p| p.code << 8)
        .unwrap_or(libc::SIGABRT);
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
        .spawn(move || {
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

pub fn init_seams() {
    postmaster_seams::parallel_pool_dispatch::set(wpool::dispatch);
}

pub mod wpool {
    //! §3.1 P-pool, phase 1: a process-lifetime pool of parked standby threads
    //! for BGWORKER_CLASS_PARALLEL launches. A standby pre-pays the spawn-path
    //! fixed costs (thread create, inherited-globals apply, GUC store build +
    //! postmaster-snapshot restore); a claim hands it the same StartupData the
    //! postmaster spawn path would build and it runs the unchanged per-task
    //! child body. Threads rotate — one task per standby, retirement through
    //! the normal child exit path — so per-task TLS state is byte-identical to
    //! a postmaster-spawned worker; the postmaster replenishes the pool off
    //! the query's critical path (ServerLoop maintain()).

    use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
    use std::sync::mpsc::SyncSender;
    use std::sync::Mutex;

    use types_core::{init::BackendType, pid_t};
    use types_startup::{BgWorkerStartupData, StartupData};

    struct StandbyTask {
        child_slot: i32,
        startup_data: StartupData,
    }

    struct Standby {
        pid: pid_t,
        tx: SyncSender<StandbyTask>,
    }

    static AVAILABLE: Mutex<Vec<Standby>> = Mutex::new(Vec::new());
    // Standbys alive (parked or still in their prelude); dispatch and retire
    // decrement, maintain() tops back up to target.
    static POPULATION: AtomicI32 = AtomicI32::new(0);

    fn available() -> std::sync::MutexGuard<'static, Vec<Standby>> {
        AVAILABLE.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn target() -> i32 {
        if std::env::var_os("PGRUST_NO_WORKER_POOL").is_some() {
            return 0;
        }
        init_small::globals::max_parallel_workers()
    }

    /// Postmaster thread only: Inherited/GUC snapshot capture must match
    /// postmaster_child_launch's launcher-side capture.
    pub fn maintain() {
        while POPULATION.load(Relaxed) < target() {
            if !spawn_standby() {
                return;
            }
        }
        while POPULATION.load(Relaxed) > target() {
            let Some(sb) = available().pop() else { return };
            POPULATION.fetch_sub(1, Relaxed);
            drop(sb); // closed channel retires the standby
        }
    }

    /// Retire every parked standby (config reload / crash reinit): the next
    /// maintain() respawns with a fresh postmaster GUC snapshot.
    pub fn flush() {
        let drained: Vec<Standby> = available().drain(..).collect();
        POPULATION.fetch_sub(drained.len() as i32, Relaxed);
    }

    fn spawn_standby() -> bool {
        let child_pid = super::reserve_child_pid();
        let inherited = super::Inherited::capture();
        let guc_snapshot = guc::store::capture_nondefault_variables();
        let (tx, rx) = std::sync::mpsc::sync_channel::<StandbyTask>(1);
        let spawned = std::thread::Builder::new()
            .name(format!("pg:standby:{child_pid}"))
            .spawn(move || {
                inherited.apply();
                let _ = stack_depth::set_stack_base();
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                    .unwrap_or_else(|e| panic!("standby GUC restore failed: {e:?}"));
                match rx.recv() {
                    Ok(task) => {
                        bgworker::gtrace("w.pool.task");
                        // §5 leak guard: a claimed standby must not carry a
                        // previous session's GUC bind (rotation makes this
                        // structurally true today; the assert keeps it true
                        // when threads start being retained across tasks).
                        debug_assert!(!guc::store::session_bound());
                        super::run_child_task(
                            BackendType::BgWorker,
                            child_pid,
                            task.child_slot,
                            task.startup_data,
                            None,
                        );
                    }
                    Err(_) => {
                        // Retired unused: nothing announced, so drop our own
                        // reaper entry.
                        POPULATION.fetch_sub(1, Relaxed);
                        let mut t =
                            super::CHILD_THREADS.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(i) = t.iter().position(|(p, _)| *p == child_pid) {
                            t.swap_remove(i);
                        }
                    }
                }
            });
        match spawned {
            Ok(handle) => {
                super::CHILD_THREADS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((child_pid, handle));
                POPULATION.fetch_add(1, Relaxed);
                available().push(Standby { pid: child_pid, tx });
                true
            }
            Err(_) => false,
        }
    }

    /// parallel_pool_dispatch seam impl. Runs on the registering backend's
    /// thread, under the bgworker registry lock. A panic here must not unwind
    /// into the registry critical section (it would leak the slot's
    /// parallel_register_count admission charge): contain it and report a
    /// miss so the caller takes the postmaster spawn path.
    pub fn dispatch(slot: i32, generation: u64) -> i32 {
        match std::panic::catch_unwind(|| dispatch_inner(slot, generation)) {
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

    fn dispatch_inner(slot: i32, generation: u64) -> i32 {
        loop {
            let Some(sb) = available().pop() else { return 0 };
            POPULATION.fetch_sub(1, Relaxed);
            let Some(child_slot) =
                pmchild_seams::assign_postmaster_child_slot::call(BackendType::BgWorker)
            else {
                POPULATION.fetch_add(1, Relaxed);
                available().push(sb);
                return 0;
            };
            pmchild_seams::set_child_pid::call(child_slot, sb.pid);
            let task = StandbyTask {
                child_slot,
                startup_data: StartupData::BgWorker(BgWorkerStartupData { slot, generation }),
            };
            match sb.tx.send(task) {
                Ok(()) => {
                    bgworker::gtrace("w.pool.dispatch");
                    return sb.pid;
                }
                Err(_) => {
                    pmchild_seams::release_postmaster_child_slot::call(child_slot);
                }
            }
        }
    }
}
