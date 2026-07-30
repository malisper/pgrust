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

// CPU affinity for pool threads (cpuaff increment A; default OFF). Hosted
// here because this crate owns every pool spawn site the policy binds.
pub mod cpuaff;

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
        main_fn: Main::Ported(slotsync::ReplSlotSyncWorkerMain),
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
    // Thread-population liveness (GL-POOLDB-HELPERDEATH-1 class law): io
    // workers are pmchild-tracked announced children; the postmaster carries
    // their term as pm.io_worker_count (reaper arm) + PM_WAIT_IO_WORKERS
    // (statemachine), sequenced AFTER the pool/gang quiescence gates.
    ChildProcessKind {
        name: "io_worker",
        main_fn: Main::Ported(io_worker::IoWorkerMain),
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

/// Reserve a synthetic task pid. The synthetic pid namespace SHARES the
/// latch owner-pid / wakeup-registry / thread-signal routing namespace with
/// exactly one REAL OS pid: the postmaster's. A child whose synthetic pid
/// equals PostmasterPid() hijacks the postmaster's pid->wakeup-pipe registry
/// entry at InitializeWaitEventSupport (find-by-pid overwrite) — from that
/// moment every SetLatch/pmsignal wake aimed at the postmaster is silently
/// misrouted, and each child-exit/launch request the postmaster sleeps
/// through costs one DetermineSleepTime period (60 s). Root-caused from
/// a narrow-sort grouped exact-DISTINCT rep (the pdstall job 1783929932: postmaster pid 1094,
/// worker synthetic pid 1094 in the wedged rep, 12 exit announces processed
/// exactly 60.03 s late; notes/pardistinct-contention-fix.md). The counter
/// must therefore never emit the postmaster's pid.
///
/// PGRUST_TEST_CHILD_PID_BELOW_PM=N (test-only): first reservation restarts
/// the counter N below PostmasterPid(), so an e2e crosses the collision
/// point within a few worker launches instead of never/late.
/// SIMVFS-SHARED (s2 §6 item 1): parent-side capture of the spawning
/// thread's shared-universe id — taken next to every spawn-door
/// registration so the simulated process's filesystem follows its threads
/// (a fork inherits the fd table). `None` when sharing is off (scheduler-off
/// sim runs, the sessions=2 corpus): the child keeps its private universe,
/// byte-identical to the pre-lane behavior.
#[cfg(pgrust_sim)]
fn sim_universe_capture() -> Option<u64> {
    vfs::sim::SimVfs::current_universe_id()
}

/// SIMVFS-SHARED: child-side adoption, called as the FIRST act after the
/// spawn door's `enter_child` (before any prelude that could touch files).
#[cfg(pgrust_sim)]
fn sim_universe_adopt(id: Option<u64>) {
    if let Some(id) = id {
        vfs::sim::SimVfs::adopt_universe(id);
    }
}

fn reserve_child_pid() -> pid_t {
    // pgsync by crate law (permit-s5; test-only env-knob memo, hygiene —
    // the walreceiverfuncs cfg(test) Once precedent).
    static TEST_INIT: pgsync::OnceLock<()> = pgsync::OnceLock::new();
    TEST_INIT.get_or_init(|| {
        if let Some(n) = std::env::var("PGRUST_TEST_CHILD_PID_BELOW_PM")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
        {
            let start = init_small::globals::PostmasterPid() - n;
            if start > 0 {
                NEXT_CHILD_PID.store(start, Ordering::Relaxed);
            }
        }
    });
    loop {
        let pid = NEXT_CHILD_PID.fetch_add(1, Ordering::Relaxed);
        if pid != init_small::globals::PostmasterPid() {
            return pid;
        }
    }
}

// C waitpid reports a child only after the process is fully dead; announce
// fires before this thread's TLS destructors run, so the reaper must join
// here or a parallel leader can free leader-owned state (execparallel's
// pstmt/param_extern contract) while the worker thread is still tearing down.
// pgsync (permit-s5, the s2 §6 item-3 row): the reaper registry is locked by
// the postmaster AND by standby threads (standby_loop retire arm, rekey), all
// registered under scheduler-on corpora — a raw std lock here was safe only
// while no pgsync op sat inside its guards. Native arm = the identical std
// re-export (zero cost).
static CHILD_THREADS: pgsync::Mutex<Vec<(pid_t, std::thread::JoinHandle<()>)>> =
    pgsync::Mutex::new(Vec::new());

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
    // NB-2 (permit-s2 review, closed at permit-s5): under the permit
    // scheduler a registered reaper must not RAW-join a registered child
    // that still needs the permit to reach exit (announce fires before the
    // child's final quantum runs its guards' drops). Hooked Join parks
    // until the child's exit hook wakes joiners; the residual raw join
    // below is then bounded OS teardown — the pgsync::thread wrapper's
    // exact join shape, applied to the door's std handle.
    #[cfg(pgrust_sim)]
    if let Some(h) = pgsync::sim::hooks::installed() {
        while !handle.is_finished() {
            h.block_on(
                core::panic::Location::caller(),
                pgsync::sim::hooks::OpClass::Join,
            );
        }
    }
    let _ = handle.join();
}

// Fork-inherited postmaster globals, applied to the fresh thread's TLS first;
// per-child state is deliberately absent (the child-init sequence owns it).
macro_rules! inherited {
    ($($field:ident : $ty:ty = $get:ident / $set:ident;)+) => {
        struct Inherited {
            data_dir: Option<&'static str>,
            // fd.c's max_safe_fds: a process static in C, computed once by
            // PostmasterMain's set_max_safe_fds() pre-fork and inherited by
            // every child via fork. Without this snapshot a backend thread
            // boots at the FD_MINFREE (48) default, freezing
            // maxAllocatedDescs at FD_MINFREE/3 = 16 ("exceeded
            // maxAllocatedDescs (16)" on pg_subtrans SLRU opens under
            // high-VU OLTP) and LRU-thrashing the VFD cache at 48 fds
            // instead of ~max_files_per_process.
            max_safe_fds: i32,
            $($field: $ty,)+
        }
        impl Inherited {
            fn capture() -> Self {
                Self {
                    data_dir: init_small::globals::DataDir(),
                    max_safe_fds: fd::max_safe_fds(),
                    $($field: init_small::globals::$get(),)+
                }
            }
            fn apply(&self) {
                if let Some(dd) = self.data_dir {
                    init_small::globals::SetDataDir(dd);
                }
                fd::vfd::set_max_safe_fds_value(self.max_safe_fds);
                $(init_small::globals::$set(self.$field);)+
            }
        }
    };
}

inherited! {
    is_postmaster_environment: bool = IsPostmasterEnvironment / SetIsPostmasterEnvironment;
    is_binary_upgrade: bool = IsBinaryUpgrade / SetIsBinaryUpgrade;
    postmaster_pid: pid_t = PostmasterPid / SetPostmasterPid;
    // data_directory_mode is process-global (set once by checkDataDir); it is
    // deliberately NOT in this list — a stale captured copy applied by a
    // pooled standby thread would reset the fixed value.
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

/// Connection-setup fault injection, for tests that must reach the
/// out-of-descriptors path without exhausting the machine's descriptor table.
/// `PGRUST_TEST_FAIL_CONN_SETUP=N` fails every Nth external connection's
/// child init exactly as an EMFILE wake pipe would (N=2: every other one), so
/// one server proves both the clean refusal AND that it kept serving.
/// Unset or 0 (the default) costs one relaxed load per connection.
fn fault_or_init_postmaster_child(
    child_type: BackendType,
    child_pid: pid_t,
) -> types_error::PgResult<()> {
    static PERIOD: AtomicI32 = AtomicI32::new(-1);
    static SEEN: AtomicI32 = AtomicI32::new(0);
    if is_external_connection_backend(child_type) {
        let mut period = PERIOD.load(Ordering::Relaxed);
        if period < 0 {
            period = std::env::var("PGRUST_TEST_FAIL_CONN_SETUP")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(0);
            PERIOD.store(period, Ordering::Relaxed);
        }
        if period > 0 && (SEEN.fetch_add(1, Ordering::Relaxed) + 1) % period == 0 {
            return Err(Box::new(types_error::PgError::new(
                types_error::FATAL,
                "waiter wake pipe creation failed: errno 24".to_string(),
            )));
        }
    }
    miscinit::InitPostmasterChild(child_pid)
}

/// A child that could not finish its own bring-up: report to the client,
/// close the socket, hand the slot back. Never panics and never leaves the
/// peer waiting — the two halves of the GL-FDLIMIT-1 defect.
fn fail_child_startup(
    child_type: BackendType,
    child_pid: pid_t,
    client_sock: Option<ClientSocket>,
    e: &types_error::PgError,
) {
    // The launcher-prepared pre-identity signal entry will never be adopted
    // or consumed by a ProcSignalInit: drop it here.
    procsignal::PreIdentitySignalDiscard(child_pid);
    // The child's own error reporting is not up yet (that is what failed),
    // so log the cause from here.
    let _ = elog::elog(
        types_error::LOG,
        format!(
            "could not start {}: {}",
            miscinit::GetBackendTypeDesc(child_type),
            e.message
        ),
    );
    if let Some(cs) = client_sock {
        report_startup_failure_to_client(cs.sock, &e.message);
    }
    // Exit status 1 = C's FATAL exit: the postmaster releases the child slot
    // and keeps running (a crash status would restart the whole server).
    if postmaster_seams::announce_child_exit::is_installed() {
        postmaster_seams::announce_child_exit::call(child_pid, 1 << 8);
    }
}

/// Send one error message to a client whose session will never exist, then
/// close the socket.
///
/// Message form is C's `report_fork_failure_to_client` (postmaster.c): the
/// bare pre-3.0 `'E'` + text + NUL, which libpq accepts whatever protocol
/// version it asked for (it sniffs the length word). Send errors are ignored
/// by design — the socket is going away either way, and the close is what
/// guarantees the client stops waiting.
pub fn report_startup_failure_to_client(sock: i32, msg: &str) {
    if sock < 0 {
        return;
    }
    let mut buf = Vec::with_capacity(msg.len() + 3);
    buf.push(b'E');
    buf.extend_from_slice(msg.as_bytes());
    buf.push(b'\n');
    buf.push(0);
    // SAFETY: fcntl/send/close on the accepted fd we own; the nonblocking
    // flip keeps a wedged client from blocking this thread's exit.
    unsafe {
        let flags = libc::fcntl(sock, libc::F_GETFL);
        if flags >= 0 && libc::fcntl(sock, libc::F_SETFL, flags | libc::O_NONBLOCK) >= 0 {
            loop {
                let rc = libc::send(sock, buf.as_ptr() as *const libc::c_void, buf.len(), 0);
                if rc >= 0
                    || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
                {
                    break;
                }
            }
        }
        libc::close(sock);
    }
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
        // Retained thread (wretain): the once-per-thread half (waiter wake
        // pipe, latch wait set, sigmask, SIGQUIT disposition) survived the
        // park; only the per-task pid/start-time identity refreshes. Wake
        // routing is NOT pid-keyed anymore (SetLatch unparks the waker
        // handle the owner publishes at every wait — the old registry's
        // stale-key wedge class is structurally gone); the seam now
        // reissues this thread's waiter token so handles published for the
        // PREVIOUS task go stale instead of delivering stray wakes into the
        // new one.
        miscinit::InitProcessGlobals(child_pid);
        waiteventset_seams::rekey_wakeup_registry::call();
        // Pre-identity signal window (wpool claim): the task pid became
        // broadcast-visible at dispatch's set_child_pid, but this thread's
        // ProcSignal slot is stamped only at the task's ProcSignalInit —
        // bind the dispatch-prepared entry so a fast-shutdown SIGTERM in
        // that window pends + wakes instead of vanishing (ESRCH).
        procsignal::PreIdentitySignalAdopt(child_pid);
    } else {
        // Everything InitPostmasterChild acquires is a kernel resource that
        // can legitimately run out: the wake pipe and the latch wait set are
        // descriptors, and this whole server is ONE process with ONE
        // descriptor table. Running out must fail THIS connection, not the
        // server: a panic here left the accepted socket open with nobody
        // owning it and no exit announce, so the client waited forever and
        // the child slot never came back (GL-FDLIMIT-1).
        if let Err(e) = fault_or_init_postmaster_child(child_type, child_pid) {
            fail_child_startup(child_type, child_pid, client_sock, &e);
            return;
        }
        // InitPostmasterChild's SIGQUIT default; miscinit can't reach interrupt.
        procsignal::pqsignal_thread(
            procsignal::signums::SIGQUIT,
            procsignal::ThreadSignalHandler::Simple(default_sigquit_handler),
        );
        // Pre-identity signal window (fresh spawn): this pid is already in
        // its pmchild slot (a TerminateChildren broadcast targets it) but
        // its ProcSignal slot exists only from ProcSignalInit, deep into
        // InitPostgres. Bind the launcher-prepared entry so a signal in the
        // window pends + wakes instead of vanishing — the fast-shutdown
        // startup-packet wedge (a backend parked in secure_read waiting for
        // a startup packet never saw SIGTERM; PM_WAIT_BACKENDS never
        // drained; the GL-GANGWEDGE-1 watchdog escalated to immediate
        // shutdown and the next start ran crash recovery).
        procsignal::PreIdentitySignalAdopt(child_pid);
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
    // Clean-exit marker (FPBUDGET-1 v2): TRUE only for a real proc_exit()
    // (callback drain owed). quickdie's exit_thread_raw also unwinds
    // ProcExitThread but never defers callbacks — the payload alone cannot
    // tell the two apart, which is exactly how the t29 bounce happened (the
    // estate drain ran after a quickdie that skipped the abort ceremony and
    // dropped a FAILED portal into freed memory). Read BEFORE the drain
    // consumes the flag.
    let clean_proc_exit =
        payload.is::<ipc::ProcExitThread>() && ipc::exit_callbacks_pending();
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
        .unwrap_or(procsignal::signums::SIGABRT);
    // Park in flight (wretain): the reaper must treat this announce as a
    // task end, not a thread end. Marked before the announce so the reaper
    // can never observe the announce without the marker.
    let parks = exitstatus == 0
        && init_small::wretain::parking()
        && init_small::wretain::proc_retained()
        && init_small::wretain::sinval_retained();
    // Session-memory teardown (FPBUDGET-1): C's process model frees the
    // backend's whole TopMemoryContext estate at process exit; the thread
    // model must do it explicitly or every session leaks its cache estate
    // into the shared process (~2.2 MiB/seed under DDL/session churn).
    // GATE (v2): clean proc_exit exits ONLY — the callback ceremony
    // (ShutdownPostgres -> AbortOutOfAnyTransaction -> portal cleanup) must
    // have run first, with every context alive, or leftover portals reach
    // the drain holding Rc's into estates the drain frees (the t29 SIGABRT:
    // FAILED-portal drop -> dealloc into a dead arena -> panic in Drop ->
    // process abort). quickdie/kill-class exits skip the drain entirely and
    // leak their estate, exactly as C's _exit(2) leaves cleanup to process
    // death; the postmaster's crash cycle follows anyway. Parked standbys
    // keep their retained caches by design. The payload re-check drops the
    // clean claim if a deferred callback crashed mid-drain.
    if !parks && clean_proc_exit && payload.downcast_ref::<ipc::ProcExitThread>().is_some()
    {
        mcxt_stats::run_session_teardown();
        // Hand freed-but-retained segments back before the thread dies
        // (mi_collect(force) via the installed hook): without it the dead
        // thread's heap pages sit abandoned-but-committed in RSS.
        mcx::release_retained();
    }
    // FPBUDGET-1 debug instrument (PGRUST_MCXT_CENSUS): process-global live
    // context census at task end. Growth across successive dumps = context
    // nodes leaked by already-dead session threads.
    if mcx::debug_census::on() {
        let rows = mcx::debug_census::snapshot();
        let mut line = String::from("MCXT-CENSUS");
        for (name, n) in rows.iter().take(48) {
            line.push_str(&format!(" [{}]={}", name, n));
        }
        eprintln!("{} (pid {})", line, child_pid);
        // This thread's own live roots with sizes: anything still here after
        // teardown is the ledgered tail (bytes attribution for the census).
        let mut forest = String::from("MCXT-FOREST");
        for t in mcxt_stats::backend_context_forest() {
            forest.push_str(&format!(
                " [{}]=used{}/fp{}",
                t.name, t.subtree_used, t.arena_footprint
            ));
        }
        eprintln!("{} (pid {})", forest, child_pid);
    }
    // Pre-identity signal cleanup: normally consumed at the task's
    // ProcSignalInit; a child that died before reaching it (startup-packet
    // die, bring-up failure) still owns its entry — drop both halves so the
    // registry cannot grow. No-ops when already consumed.
    procsignal::PreIdentitySignalRelease();
    procsignal::PreIdentitySignalDiscard(child_pid);
    if parks {
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

    // M4 bgjobs virtual child (docs/design/m4-bgjobs.md §3.6): a migrated
    // periodic daemon becomes a dispatcher job on the runtime pool instead
    // of a thread. Default OFF (PGRUST_RUNTIME_BGJOBS); everything the
    // caller observes (pid, PmChild bookkeeping, signal fanout, exit
    // announces) is shape-identical.
    if let Some(pid) = rtpool::try_launch_job(child_type, child_slot) {
        return pid;
    }

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
    // Pre-identity signal window: make the pid deliverable BEFORE the caller
    // publishes it (set_child_pid) — a fast-shutdown SIGTERM broadcast can
    // land before the child thread has run a single instruction. The child
    // binds its latch at InitPostmasterChild; ProcSignalInit consumes the
    // entry; failure paths discard it.
    procsignal::PreIdentitySignalPrepare(child_pid);
    let inherited = Inherited::capture();
    // Keep the process-wide BASE snapshot current with this (postmaster)
    // thread's store: first launch publishes the boot base, later launches
    // republish only if the postmaster's config changed (guc::layers). The
    // child's GUC bring-up shares that Arc — one typed capture per config
    // change instead of a string render per launch, and the child applies
    // postmaster-validated values (no re-parse, no check-hook rerun; assign
    // hooks still fire on the child thread). PGRUST_NO_GUC_BASE reverts to
    // the per-launch string capture/restore path for A/B.
    let guc_base = guc::layers::ensure_base_current();
    let guc_snapshot = if guc::layers::base_share_enabled() {
        Vec::new()
    } else {
        guc::store::capture_nondefault_variables()
    };

    // PERMIT-S1 (WS-CORE, contract §3.1): the spawn door registers the
    // child's permit-scheduler slot BEFORE the OS spawn, keyed by the
    // reserved vpid (reserve_child_pid pids are positive: the u32 cast is
    // lossless and stays below the synthetic ranges). No-op unless
    // PGRUST_SIM_SCHED=1 under `pgrust_sim`.
    #[cfg(pgrust_sim)]
    let sim_sched_slot =
        pgsync::sim::spawn_door::register_child(child_pid as u32, kind.name);
    // SIMVFS-SHARED: parent-side universe capture — the simulated process's
    // filesystem follows its threads (a fork inherits the fd table). None
    // when sharing is off: the child keeps its private universe, exactly
    // the pre-lane behavior.
    #[cfg(pgrust_sim)]
    let sim_universe = sim_universe_capture();

    let spawned = std::thread::Builder::new()
        .name(format!("pg:{}:{}", kind.name, child_pid))
        .stack_size(child_thread_stack_size())
        .spawn(move || {
            // PERMIT-S1: bind this thread to its slot and park until the
            // first permit grant. Declared FIRST so its Drop — the teardown
            // epilogue (teardown hooks inside the final quantum, deregister,
            // join-wake, handoff) — runs LAST, after every TLS-owning guard
            // below has dropped inside the final quantum (TLS-teardown
            // rule 1, design §3).
            #[cfg(pgrust_sim)]
            let _sim_sched_permit = pgsync::sim::spawn_door::enter_child(sim_sched_slot);
            // SIMVFS-SHARED: adopt the parent's universe FIRST — before any
            // guard or prelude that could touch the filesystem.
            #[cfg(pgrust_sim)]
            sim_universe_adopt(sim_universe);
            // Thread-scoped (a retained thread reuses its slot across tasks):
            // the local latch slab slot returns on every thread exit —
            // announce fallthrough and panic unwind alike.
            let _local_latch_release = miscinit::LocalLatchReleaseGuard::new();
            // C's process death closes the "static, never freed" wait event
            // sets (LatchWaitSet, FeBeWaitSet); a session thread's death
            // closes nothing (chaos F2: 2 leaked epoll fds per connection).
            // Declared after the latch guard so it drops first — after
            // run_child_task's deferred proc_exit drain, before the latch
            // slot the sets reference is recycled.
            let _wait_event_sets_release = waiteventset::WaitEventSetReleaseGuard::new();

            inherited.apply();

            // C records the stack base once in main(); each thread owns its own.
            let _ = stack_depth::set_stack_base();

            let guc_result = if guc::layers::base_share_enabled() {
                guc::store::initialize_guc_options_for_child_base(&guc_base)
                    .and_then(|()| guc::layers::bind_base(&guc_base))
            } else {
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
            };
            // Same rule as the child-init failure below: a connection whose
            // bring-up fails is refused, not panicked (a panic here left the
            // accepted socket open forever — GL-FDLIMIT-1).
            if let Err(e) = guc_result {
                fail_child_startup(child_type, child_pid, client_sock, &e);
                return;
            }

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
        Err(_) => {
            // F3 ledger row (fixed at PERMIT-S2, taken while dooring the
            // wpool site): retire the never-entered slot so the failed
            // spawn cannot leak a Runnable ghost into the schedule.
            #[cfg(pgrust_sim)]
            pgsync::sim::spawn_door::cancel_child(sim_sched_slot);
            procsignal::PreIdentitySignalDiscard(child_pid);
            -1
        }
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
    postmaster_seams::parallel_pool_retire_db::set(retire_db_all_pools);
    // PERMIT-S2 F2: the sim wpool demo drives the real pool through seams
    // (tcop cannot depend on this crate — package cycle via autovacuum).
    postmaster_seams::wpool_maintain::set(wpool::maintain);
    postmaster_seams::wpool_flush::set(wpool::flush);
    postmaster_seams::wpool_population::set(wpool::population);
    // PERMIT-S5: the sim rtpool demo drives the REAL runtime pool through
    // its (now-doored) spawn sites — same package-cycle reason as wpool.
    postmaster_seams::rtpool_start::set(rtpool_start_for_demo);
    postmaster_seams::rtpool_stop::set(rtpool::demo_stop);
    postmaster_seams::rtpool_population::set(rtpool::population);
    // SIMVFS-SHARED: the sim rtgang corpus needs postmaster parity in the
    // sim-net harness (sizing before InitializeMaxBackends, install after
    // shared memory) — same package-cycle reason as the trios above.
    postmaster_seams::rtgang_procs_wanted::set(rtgang::runtime_reserved_procs);
    postmaster_seams::rtgang_install::set(rtgang::install_if_enabled);
    // Shutdown sequencing (mode-W class fix): the state machine fences the
    // gang at PM_STOP_BACKENDS and gates PM_WAIT_BACKENDS on quiescence.
    postmaster_seams::rtgang_retire::set(rtgang::retire_for_shutdown);
    postmaster_seams::rtgang_live::set(runtime_shm_busy_threads);
    // The pool busy guard's crash/shutdown state-machine poke (parallel
    // cannot name pmsignal in production — the POOL_GATE fn-pointer
    // precedent).
    parallel::standing::install_pool_busy_poke(|| {
        pmsignal::SendPostmasterSignal(
            pmsignal::PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE,
        )
    });
}

/// GL-GANGWEDGE-1 shutdown-backstop fault injector, re-exported for the
/// postmaster (which cannot name `parallel` — the seam package-cycle rule).
/// Inert unless `PGRUST_TEST_WEDGE_SHM_BUSY_MS` is set; see
/// `parallel::standing::install_wedge_shm_busy_injection`.
pub fn install_wedge_shm_busy_injection() {
    parallel::standing::install_wedge_shm_busy_injection();
}

/// The PM_WAIT_BACKENDS quiescence term for registry-invisible runtime
/// threads (rtgang_live seam): live gang threads PLUS pool-db threads
/// inside a shared-memory-touching span (deferred identity bring-up /
/// exit-callback drains). Pool threads carry no pmchild slot, no exit
/// announce, and no gang LIVE charge, so without the second term the
/// crash-restart arm reset shared memory UNDERNEATH an in-flight pool
/// exit drain — the drain's re-find assert then fired while holding a
/// lock-table partition LWLock, and the swallowed panic leaked the
/// partition forever (no recovery, every later acquisition wedged).
/// Same class as the mode-W shutdown fix that installed these seams:
/// a thread population the postmaster state machine cannot see.
fn runtime_shm_busy_threads() -> i32 {
    rtgang::live_gang_threads() + parallel::standing::pool_shm_busy()
}

/// rtpool_start seam impl: start the pool if `PGRUST_RUNTIME` enables it;
/// returns the live pool-thread count (-1 = runtime disabled).
fn rtpool_start_for_demo() -> i32 {
    if rtpool::start_if_enabled().is_none() {
        return -1;
    }
    rtpool::population()
}

/// DROP DATABASE rider for BOTH db-pinned pools: wpool's parked standbys
/// and the M2 standing runtime executors (parallel::standing).
fn retire_db_all_pools(dboid: types_core::Oid) {
    wpool::retire_db(dboid);
    parallel::standing::retire_db(dboid);
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

    // PERMIT-S2 (F2, the SECOND spawn door): the standby handoff channels
    // and the pool registries are pgsync types. Native arm = the identical
    // std re-exports (zero cost, no semantics change); under the sim permit
    // scheduler the standby's recv() park, the claim's send, and the
    // retire-drop wakes are hooked ops — pooled workers are schedulable.
    // (AVAILABLE must be pgsync too: retire/flush drop Standby channels
    // INSIDE its guard, and a preemption at that drop-wake touch would let
    // another registered thread block raw on the registry mutex while
    // holding the permit — the watchdog wedge shape.)
    use pgsync::mpsc::{Receiver, SyncSender};
    use pgsync::Mutex;

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

    fn available() -> pgsync::MutexGuard<'static, Vec<Standby>> {
        AVAILABLE.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Live standby-thread count (POPULATION discipline above). Public for
    /// the sim wpool demo's drain probe; harmless elsewhere.
    pub fn population() -> i32 {
        POPULATION.load(Relaxed)
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
        // M2 pool-binding: the standing runtime executors park on plain
        // process-local primitives under the same discipline — fence them
        // before shared memory resets (no-op if the gang never started).
        parallel::standing::flush_for_crash();
    }

    fn spawn_standby() -> bool {
        let spawn_pid = super::reserve_child_pid();
        let inherited = super::Inherited::capture();
        // Base share, same contract as postmaster_child_launch; wpool::flush
        // on reload still retires parked standbys so respawns pick up the
        // NEW base on the next maintain().
        let guc_base = guc::layers::ensure_base_current();
        let guc_snapshot = if guc::layers::base_share_enabled() {
            Vec::new()
        } else {
            guc::store::capture_nondefault_variables()
        };
        let (tx, rx) = pgsync::mpsc::sync_channel::<StandbyTask>(1);
        let (ack_tx, ack_rx) = pgsync::mpsc::sync_channel::<()>(1);
        // PERMIT-S2 (F2): the wpool spawn door — pooled standbys register
        // parent-side BEFORE the OS spawn, exactly like the
        // postmaster_child_launch door above. No-op unless PGRUST_SIM_SCHED=1.
        #[cfg(pgrust_sim)]
        let sim_sched_slot = pgsync::sim::spawn_door::register_child(
            spawn_pid as u32,
            &format!("wpool-standby:{spawn_pid}"),
        );
        // SIMVFS-SHARED: parent-side universe capture (fd-table inheritance).
        #[cfg(pgrust_sim)]
        let sim_universe = super::sim_universe_capture();
        let spawned = std::thread::Builder::new()
            .name(format!("pg:standby:{spawn_pid}"))
            .stack_size(super::child_thread_stack_size())
            .spawn(move || {
                // PERMIT-S1 door discipline: bind + gate FIRST, so this
                // guard's Drop (the teardown epilogue) runs LAST — after
                // the population charge and every TLS-owning guard below
                // has dropped inside the final quantum.
                #[cfg(pgrust_sim)]
                let _sim_sched_permit = pgsync::sim::spawn_door::enter_child(sim_sched_slot);
                // SIMVFS-SHARED: adopt the parent's universe FIRST.
                #[cfg(pgrust_sim)]
                super::sim_universe_adopt(sim_universe);
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
                // As on the spawn path: a rotating standby's exit must close
                // its wait event sets (parked standbys keep them warm).
                let _wait_event_sets_release =
                    waiteventset::WaitEventSetReleaseGuard::new();
                inherited.apply();
                let _ = stack_depth::set_stack_base();
                // cpuaff increment A (default OFF): standbys ride the full
                // pool-set mask (no dedicated core). Fail-open, loud-once.
                super::cpuaff::apply_wpool_standby();
                if guc::layers::base_share_enabled() {
                    guc::store::initialize_guc_options_for_child_base(&guc_base)
                        .and_then(|()| guc::layers::bind_base(&guc_base))
                        .unwrap_or_else(|e| panic!("standby GUC base bind failed: {e:?}"));
                } else {
                    guc::store::initialize_guc_options_for_child(&guc_snapshot)
                        .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                        .unwrap_or_else(|e| panic!("standby GUC restore failed: {e:?}"));
                }
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
            Err(_) => {
                // F3 shape: retire the never-entered slot so the failed
                // spawn cannot leak a Runnable ghost into the schedule.
                #[cfg(pgrust_sim)]
                pgsync::sim::spawn_door::cancel_child(sim_sched_slot);
                false
            }
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
                    // PERMIT-S2 (F2): the model identity follows the task
                    // pid — sim waiter slots key off current_vpid (rule 2),
                    // so a retained standby's slot renames at every claim.
                    // No-op with the scheduler off.
                    #[cfg(pgrust_sim)]
                    pgsync::sim::spawn_door::rekey_self(task.child_pid as u32);
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
                        let (tx2, rx2) = pgsync::mpsc::sync_channel::<StandbyTask>(1);
                        let (ack_tx2, ack_rx2) = pgsync::mpsc::sync_channel::<()>(1);
                        parked_crash_epoch = pre_crash;
                        // Fence re-check UNDER the pool lock: flush/
                        // flush_for_crash bump their epochs BEFORE draining
                        // under this same lock, so an unchanged epoch read
                        // here guarantees a concurrent drain still sees this
                        // entry; a changed one means the drain already ran —
                        // re-pooling would park a PRE-FENCE retained identity
                        // across the shared-memory reset (the post-recovery
                        // "ReattachRetainedProc: retained PGPROC was
                        // released" abort loop).
                        let repooled = {
                            let mut avail = available();
                            if FLUSH_EPOCH.load(Relaxed) == pre_flush
                                && CRASH_EPOCH.load(Relaxed) == pre_crash
                            {
                                avail.push(Standby {
                                    pid: task.child_pid,
                                    tx: tx2,
                                    db: init_small::wretain::retained_db(),
                                    released: ack_rx2,
                                });
                                true
                            } else {
                                false
                            }
                        };
                        if !repooled {
                            release_retained_identity(pre_crash);
                            break;
                        }
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
                    // DST-PMCHILD shutdown-drain RED fixture (sim-only,
                    // PGRUST_SIM_STUCKCHILD=1): the FIRST standby to be
                    // retired refuses to exit — it parks forever on hooked
                    // sleeps (the scheduler keeps advancing virtual time, so
                    // the boot thread's drain must produce the NAMED
                    // SHUTDOWNDRAIN verdict, never a hang; the SCHEDCEILING
                    // bound is the net that would name the parked site).
                    // POPULATION stays charged — exactly the observable a
                    // real never-exiting child would leave.
                    #[cfg(pgrust_sim)]
                    if stuck_child_take_once() {
                        loop {
                            pgsync::thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
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
            // Fence snapshot for the push-back arm below: a standby popped
            // here holds a PRE-pop retained identity; it may only re-enter
            // the pool if no flush/crash fence ran in between (see the
            // repark re-check in standby_loop for the ordering argument).
            let pop_flush = FLUSH_EPOCH.load(Relaxed);
            let pop_crash = CRASH_EPOCH.load(Relaxed);
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
                // Fence re-check UNDER the pool lock (see the repark
                // re-check in standby_loop): a fence between the pop and
                // this push already drained the pool — re-pooling `sb`
                // would carry its pre-fence retained identity across the
                // shared-memory reset. Dropping it (closed channel)
                // retires the standby; its own wake skips the shared-
                // memory release through the parked-epoch check.
                let mut avail = available();
                if FLUSH_EPOCH.load(Relaxed) == pop_flush
                    && CRASH_EPOCH.load(Relaxed) == pop_crash
                {
                    avail.push(sb);
                }
                return 0;
            };
            // Fresh per-task pid: the previous task's exit announce may still
            // be queued at the postmaster; reusing its pid would let the
            // reaper's cleanup land on the new task.
            let task_pid = super::reserve_child_pid();
            // Pre-identity signal window (claim): between set_child_pid below
            // and the standby's per-task ProcSignalInit, a shutdown SIGTERM
            // targets task_pid — make it deliverable first (the standby
            // adopts at claim wake; run_child_task's tail discards).
            procsignal::PreIdentitySignalPrepare(task_pid);
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
                    procsignal::PreIdentitySignalDiscard(task_pid);
                }
            }
        }
    }

    /// DST-PMCHILD (sim-only): one-shot arming of the shutdown-drain red —
    /// returns true exactly once process-wide, and only when
    /// PGRUST_SIM_STUCKCHILD=1. Plain atomics for the armed memo (the
    /// schedule-invisible pattern — the multibackend §3 pgsync-OnceLock
    /// lesson: a hooked memo on this path would perturb every corpus); the
    /// env read itself happens at most once per thread on the cold retire
    /// arm, never on a task path.
    #[cfg(pgrust_sim)]
    fn stuck_child_take_once() -> bool {
        use std::sync::atomic::{AtomicBool, AtomicU8};
        static ARMED: AtomicU8 = AtomicU8::new(0); // 0 unknown, 1 armed, 2 off
        static TAKEN: AtomicBool = AtomicBool::new(false);
        let mut armed = ARMED.load(Relaxed);
        if armed == 0 {
            armed = if std::env::var("PGRUST_SIM_STUCKCHILD").as_deref() == Ok("1") {
                1
            } else {
                2
            };
            // Racing initializers compute the same value: benign.
            ARMED.store(armed, Relaxed);
        }
        armed == 1 && !TAKEN.swap(true, Relaxed)
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

pub mod rtpool {
    //! M0 runtime worker-pool spawn glue (docs/design/parallelism-redesign-
    //! 2026-07.md §2.1/§2.8, §5 M0). INERT IN M0: nothing in production
    //! calls [`start_if_enabled`] yet (M1 wires it into postmaster startup);
    //! `PGRUST_RUNTIME=1` is the only way in and defaults OFF, so this
    //! module is dead code in every production path — the M0 gate is zero
    //! behavior change.
    //!
    //! Spawn discipline (wpool-inherited):
    //! - synthetic pids come from the SAME guarded counter as every child
    //!   (`reserve_child_pid` — inherits the pid-collision fix: never emits
    //!   `PostmasterPid()`; notes/pardistinct-contention-fix.md §2b);
    //! - standard child stack size + fork-inherited globals prelude +
    //!   per-thread stack base, exactly the warm costs wpool standbys
    //!   pre-pay;
    //! - runtime workers are EXECUTORS, not sessions (redesign §2.1): no
    //!   PGPROC, no bgworker slot, no GUC session bind, no reaper entry
    //!   (process-lifetime threads, never announced), and NO latch /
    //!   wakeup-registry entry — they park on the runtime's eventcount, so
    //!   the pid-keyed registry-hijack wedge class has no surface. If pool
    //!   threads ever take latch waits (M1+), they must follow the
    //!   `waiteventset_seams::rekey_wakeup_registry` discipline wpool uses.

    use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
    use std::sync::Arc;

    use pgsync::OnceLock;
    use types_core::pid_t;

    static RUNTIME: OnceLock<Arc<runtime::Runtime>> = OnceLock::new();

    // Live pool-thread count (wpool POPULATION discipline): bumped at spawn
    // success, dropped by the thread itself on any exit (the pool is
    // process-lifetime in production, so this only ever falls under an
    // explicit request_stop — the sim rtpool demo's drain probe).
    static POPULATION: AtomicI32 = AtomicI32::new(0);

    /// Live pool-thread count. Public for the sim rtpool demo's drain
    /// probe (postmaster_seams::rtpool_population); harmless elsewhere.
    pub fn population() -> i32 {
        POPULATION.load(Relaxed)
    }

    /// Sim rtpool demo (permit-s5): ask the started pool's workers to exit
    /// their loops (stop flag + wake_all). No-op if the pool never started.
    /// Production never calls this — the pool is process-lifetime.
    pub fn demo_stop() {
        if let Some(rt) = RUNTIME.get() {
            rt.request_stop();
        }
    }

    /// The process runtime, if the kill switch enabled it. M0: called by
    /// nothing in production; tests and (later) M1 postmaster startup.
    pub fn start_if_enabled() -> Option<&'static Arc<runtime::Runtime>> {
        // ONE authority (t34-config review, defect 3): the registered
        // `pgrust.runtime` GUC backing cell — which `runtime::runtime_enabled`
        // itself now reads — gates the spawn. PGRUST_RUNTIME only SEEDS the
        // cell at boot (initialize_guc_options_from_environment,
        // PGC_S_ENV_VAR); postgresql.conf/-c can override the seed and every
        // reader (this spawn gate, the executor arms, the M5-3 planner probe
        // via the pool-live flag start() publishes) agrees. The old dual gate
        // (env read AND GUC cell) could disagree — e.g. PGRUST_RUNTIME=0 plus
        // an explicit conf `pgrust.runtime=on` spawned no pool while the
        // planner probe suppressed Gather: silent serial.
        if !runtime::runtime_enabled() {
            return None;
        }
        Some(start())
    }

    // ---- M2 inc-2: PGPROC-LEASING POOL WORKERS (PGRUST_RUNTIME_POOLDB) ----
    //
    // With the kill switch armed, every rtpool thread takes the PROCESS-
    // LOCAL rtgang prelude at spawn (GUC child prelude, InitPostmasterChild,
    // synthetic bgworker entry, timeouts); the SHARED-MEMORY identity
    // (InitProcess from the boot-reserved segment, BaseInit) is deferred to
    // the first serve gate — an engaging leader proves live shared memory,
    // so a respawn can never bring up identity inside a crash-restart
    // window (the round-32 helperdeath wedge class). Exits keep the rtgang
    // discipline (ProcExitThread → deferred-callback drain so ProcKill
    // releases the PGPROC; PoolRetireRaw OR a stale crash-fence epoch →
    // raw exit, no shmem touch) PLUS slot respawn — the pool must never
    // shrink. Identity-touching spans charge SHM_BUSY, which the
    // postmaster's PM_WAIT_BACKENDS gate reads through the rtgang_live
    // seam sum (the crash reset waits out in-flight drains/bring-ups).
    // The per-serve identity/fence gate is installed into
    // parallel::standing (POOL_GATE); serve dispatch itself rides the
    // runtime's bound descriptor. OFF (the default posture of this
    // module's inc-2 layer): byte-identical rtpool.

    struct PoolBoot {
        guc_base: Option<std::sync::Arc<guc::layers::GucBaseSnapshot>>,
        guc_snapshot: Vec<guc::store::NondefaultGuc>,
    }

    static POOL_BOOT: OnceLock<PoolBoot> = OnceLock::new();

    /// All three switch layers (read once): the runtime exists, the
    /// standing module is alive, and the inc-2 keystone is armed.
    pub fn pooldb_armed() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| {
            runtime::runtime_enabled()
                && parallel::standing::pool_binding_enabled()
                && parallel::standing::pooldb_enabled()
        })
    }

    /// PGPROCs to boot-reserve for pool-db threads: ONE PER POOL THREAD
    /// (workers + standbys — the scope doc §5 decided default; idle
    /// entries are park-invisible so the procarray never scans them).
    /// 0 unless armed — byte-identical sizing under the kill switch.
    /// Consumed by rtgang::runtime_reserved_procs (the one budget
    /// authority feeding SetRuntimeGangProcs).
    pub fn pool_procs_wanted() -> i32 {
        if !pooldb_armed() {
            return 0;
        }
        let cfg = runtime::RuntimeConfig::from_env();
        ((cfg.workers + cfg.standbys) as i64).clamp(0, 2048) as i32
    }

    #[derive(Clone, Copy, PartialEq)]
    enum PoolIdent {
        /// No identity on this thread yet: bring-up is DEFERRED to the
        /// first serve gate (pool_gate completes it — a serve entry proves
        /// an engaging leader, which proves live shared memory).
        None,
        /// Identity live; the crash-fence epoch captured BEFORE the
        /// PGPROC claim (a bump landing anywhere after the capture retires
        /// this identity at the next gate).
        Ready(usize),
        /// Bring-up failed: this thread never serves (fail-open to inc-1 —
        /// it keeps executing ordinary runtime work).
        Poisoned,
    }

    thread_local! {
        static POOL_IDENT: std::cell::Cell<PoolIdent> =
            const { std::cell::Cell::new(PoolIdent::None) };
    }

    // The pool's shared-memory busy term + crash-window flag live in
    // parallel::standing (POOL_SHM_BUSY / POOL_CRASH_PENDING — the
    // warm-connect span charges the same counter and only that crate can
    // see it); this glue charges it around identity bring-up and the exit
    // drains, and super::runtime_shm_busy_threads feeds the sum to the
    // postmaster's PM_WAIT_BACKENDS gate through the rtgang_live seam.

    /// The per-serve gate installed into parallel::standing: verify — or
    /// COMPLETE — this thread's leased identity, and check the crash fence.
    /// Unwinds PoolRetireRaw when the fence moved (shared memory was reset
    /// under our identity — the thread must die RAW and respawn cold).
    fn pool_gate() -> bool {
        match POOL_IDENT.with(std::cell::Cell::get) {
            PoolIdent::Ready(epoch) => {
                if epoch != parallel::standing::pool_fence_epoch() {
                    std::panic::panic_any(parallel::standing::PoolRetireRaw);
                }
                true
            }
            PoolIdent::None => pool_identity_complete(),
            PoolIdent::Poisoned => false,
        }
    }

    /// Deferred identity bring-up (the crash-window fix): runs at the FIRST
    /// serve gate on this thread — a serve entry proves an engaging leader,
    /// which proves reinit completed (the gang's try_engage respawn
    /// discipline: backends only run against live shared memory). Spawn-time
    /// bring-up ran InitProcess inside crash-restart windows on the respawn
    /// path (against pre/mid-reset shared memory, an unthrottled
    /// panic/respawn storm) and captured the fence epoch AFTER the claim,
    /// so a bump landing mid-bring-up minted a dangling identity the fence
    /// could never retire. Capturing BEFORE InitProcess closes that race.
    fn pool_identity_complete() -> bool {
        // Busy charge spans the whole claim: a ticketless completing thread
        // is not awaited by any leader's close_and_await, so the leader's
        // exit (and a crash reset behind it) can race the claim without it.
        let _busy = parallel::standing::pool_shm_busy_guard();
        // Crash-window re-check under the charge (retire_all/clear-at-
        // engage pattern): the window may have opened after the engaging
        // leader published — never MINT identity against memory the reset
        // is about to reclaim; a later, post-recovery serve completes it.
        if parallel::standing::pool_crash_pending() {
            return false;
        }
        let epoch0 = parallel::standing::pool_fence_epoch();
        if let Err(e) = lmgr_proc::InitProcess(types_core::init::BackendType::BgWorker) {
            let _ = elog::elog(
                types_error::WARNING,
                format!("pool executor: InitProcess failed: {}", e.message()),
            );
            POOL_IDENT.with(|c| c.set(PoolIdent::Poisoned));
            return false;
        }
        // Identity exists from here: the exit discipline (fence-checked
        // drain in pooldb_thread_main) owns its release on every path.
        POOL_IDENT.with(|c| c.set(PoolIdent::Ready(epoch0)));
        if let Err(e) = postinit::BaseInit() {
            let _ = elog::elog(
                types_error::WARNING,
                format!("pool executor: BaseInit failed: {}", e.message()),
            );
            // PGPROC is claimed: release identity via the exit path (the
            // glue drains against live shared memory and respawns cold).
            ipc::proc_exit(1, init_small::globals::MyProcPid());
        }
        true
    }

    /// Pool-db thread body: the process-local prelude (GUC bring-up,
    /// InitPostmasterChild, signal dispositions, synthetic bgworker entry,
    /// timeout machinery), then the ordinary worker loop under the rtgang
    /// exit discipline + respawn. The SHARED-MEMORY identity (InitProcess +
    /// BaseInit) is NOT taken here — it is deferred to the first serve gate
    /// (pool_identity_complete), where an engaging leader proves live
    /// shared memory; spawn-time bring-up ran during crash-restart windows
    /// on the respawn path. Prelude failures poison the thread and fall
    /// open to the plain executor loop (inc-1 behavior; the gate refuses
    /// serves); identity-holding exits release through the fence-checked
    /// drain below and respawn.
    fn pooldb_thread_main(ordinal: usize, child_pid: pid_t, body: Box<dyn FnOnce() + Send>) {
        // Thread-scoped local latch slot (returned on thread exit).
        let _local_latch_release = miscinit::LocalLatchReleaseGuard::new();
        let Some(boot) = POOL_BOOT.get() else {
            POOL_IDENT.with(|c| c.set(PoolIdent::Poisoned));
            body();
            return;
        };
        let guc_ok = if let Some(base) = &boot.guc_base {
            guc::store::initialize_guc_options_for_child_base(base)
                .and_then(|()| guc::layers::bind_base(base))
        } else {
            guc::store::initialize_guc_options_for_child(&boot.guc_snapshot)
                .and_then(|()| guc::store::restore_nondefault_variables(&boot.guc_snapshot))
        };
        if let Err(e) = guc_ok {
            let _ = elog::elog(
                types_error::WARNING,
                format!("pool executor {ordinal}: GUC bring-up failed: {}", e.message()),
            );
            POOL_IDENT.with(|c| c.set(PoolIdent::Poisoned));
            body();
            return;
        }
        if let Err(e) = miscinit::InitPostmasterChild(child_pid) {
            let _ = elog::elog(
                types_error::WARNING,
                format!(
                    "pool executor {ordinal}: InitPostmasterChild failed: {}",
                    e.message()
                ),
            );
            POOL_IDENT.with(|c| c.set(PoolIdent::Poisoned));
            body();
            return;
        }
        procsignal::pqsignal_thread(
            procsignal::signums::SIGQUIT,
            procsignal::ThreadSignalHandler::Simple(super::default_sigquit_handler),
        );
        miscinit::SetMyBackendType(types_core::init::BackendType::BgWorker);
        bgworker::adopt_worker_entry(bgworker::BackgroundWorker {
            bgw_name: format!("runtime pool executor {ordinal}"),
            bgw_type: "runtime pool executor".to_string(),
            bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS
                | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
            bgw_start_time: bgworker::BgWorkerStartTime::ConsistentState,
            bgw_restart_time: bgworker::BGW_NEVER_RESTART,
            bgw_main: pool_entry_never,
            bgw_main_arg: 0,
            bgw_extra: [0u8; bgworker::BGW_EXTRALEN],
            bgw_notify_pid: 0,
        });
        // Per-thread timeout machinery BEFORE any connect (the rtgang
        // train-12 finding: the serve-time InitPostgres registers session
        // timeouts, which debug_assert InitializeTimeouts ran here).
        timeout_seams::initialize_timeouts::call();

        // Shared-memory identity is deferred to the first serve gate
        // (pool_identity_complete) — see the fn doc.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
        match outcome {
            // Clean loop exit (request_stop — tests/shutdown only): the
            // thread ends without respawn; identity dies with the process.
            Ok(()) => {}
            Err(payload) => {
                // Sticky hygiene (rung 3): drop any parked session
                // retention with a plain drop — heap-only, disarmed guard,
                // no shared-memory touch (safe on the raw arm too; the
                // gang exit discipline).
                parallel::standing::sticky_clear_on_pool_exit();
                if payload.is::<parallel::standing::PoolRetireRaw>() {
                    // Crash fence: NO shared-memory interaction — the
                    // PGPROC was reset wholesale with shared memory.
                } else {
                    // Busy charge BEFORE the fence check (PoolShmBusyGuard
                    // doc): either this drain sees a fence bump and exits
                    // raw, or the postmaster's quiescence gate sees the
                    // charge and the crash reset waits the drain out.
                    let _busy = parallel::standing::pool_shm_busy_guard();
                    let stale = matches!(
                        POOL_IDENT.with(std::cell::Cell::get),
                        PoolIdent::Ready(epoch)
                            if epoch != parallel::standing::pool_fence_epoch()
                    );
                    if stale {
                        // The crash fence moved since this identity's
                        // bring-up: shared memory was (or is being) reset
                        // wholesale and the identity died with it. Draining
                        // now would run ProcKill/LockReleaseAll against
                        // REINITIALIZED structures — the drain's re-find
                        // assert fires while holding a lock-table partition
                        // LWLock, the guarded-callback drain swallows the
                        // panic, and the leaked partition wedges recovery
                        // forever. Exit RAW, exactly the PoolRetireRaw
                        // discipline; the reset already reclaimed the PGPROC.
                        let _ = elog::elog(
                            types_error::WARNING,
                            format!(
                                "pool executor {ordinal} died across a crash fence; exiting raw"
                            ),
                        );
                    } else if let Some(p) = payload.downcast_ref::<ipc::ProcExitThread>() {
                        // FATAL / retired-db / connect-failure exit: the
                        // deferred drain (ProcKill, RemoveProcFromArray,
                        // sinval cleanup) releases identity against LIVE
                        // shared memory.
                        let code = p.code;
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            ipc::run_deferred_exit_callbacks(code)
                        }));
                    } else {
                        // Generic panic with identity possibly held: best-
                        // effort drain (the rtgang leak argument — a leaked
                        // procarray/sinval entry blocks DROP DATABASE forever
                        // and a leaked PGPROC drains the freelist).
                        let _ = elog::elog(
                            types_error::WARNING,
                            format!("pool executor {ordinal} died on a panic; releasing identity"),
                        );
                        // A PARKED pool thread is procarray-invisible (the
                        // serve bracket), but the exit callbacks expect
                        // membership (RemoveProcFromArray) — re-add first, the
                        // gang's Wake::Retire discipline (best-effort; a
                        // mid-serve panic dies VISIBLE and the double-add's
                        // own failure is swallowed so the drain still runs).
                        parallel::standing::pool_exit_rejoin_procarray();
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            ipc::proc_exit(2, init_small::globals::MyProcPid())
                        }));
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            ipc::run_deferred_exit_callbacks(2)
                        }));
                    }
                }
                // The pool must never shrink: respawn the slot cold (fresh
                // pid; identity bring-up happens at its first serve gate,
                // under an engaging leader — never inside a crash window).
                respawn_pool_slot(ordinal);
            }
        }
    }

    fn pool_entry_never(_arg: u64) -> types_error::PgResult<()> {
        unreachable!("pool executor entry is never dispatched")
    }

    fn respawn_pool_slot(ordinal: usize) {
        let Some(rt) = RUNTIME.get() else { return };
        let rt2 = Arc::clone(rt);
        if spawn_worker(ordinal, Box::new(move || runtime::worker_loop(&rt2, ordinal)))
            .is_err()
        {
            let _ = elog::elog(
                types_error::WARNING,
                format!("pool executor {ordinal}: respawn failed (pool shrinks)"),
            );
        }
    }

    /// Start (or get) the process runtime pool: `workers + standbys`
    /// executor threads per the env-derived config (workers = cores).
    /// Postmaster thread only (the Inherited/GUC capture rule wpool's
    /// maintain() documents applies to the prelude capture here too).
    pub fn start() -> &'static Arc<runtime::Runtime> {
        RUNTIME.get_or_init(|| {
            // M2 inc-2 boot wiring (postmaster thread; BEFORE any worker
            // spawns — the threads' bring-up reads these): GUC captures +
            // the per-serve identity gate.
            if pooldb_armed() {
                let _ = POOL_BOOT.set(PoolBoot {
                    guc_base: guc::layers::base_share_enabled()
                        .then(guc::layers::ensure_base_current),
                    guc_snapshot: if guc::layers::base_share_enabled() {
                        Vec::new()
                    } else {
                        guc::store::capture_nondefault_variables()
                    },
                });
                parallel::standing::install_pool_gate(pool_gate);
                // M2 inc-3 rung 3: sticky session retention on pool serves
                // needs the scheduler's unbound-work eviction gate armed —
                // the runtime evicts a parked session view (through this
                // installed fn) before any ordinary task work runs on a
                // pool thread. Installed whenever pooldb is armed (the
                // sticky knob itself is consulted at the binder layer;
                // installing the evictor unconditionally is a no-op when
                // no retention ever parks).
                runtime::install_session_residue_evictor(
                    parallel::standing::sticky_evict_for_unbound_work,
                );
            }
            let cfg = runtime::RuntimeConfig::from_env();
            // cpuaff increment A (default OFF): compute the affinity policy
            // + boards once, BEFORE any pool thread spawns — the worker
            // closures below read it, and the standing gang (installed
            // after this boot point, spawning lazily) mirrors the map.
            super::cpuaff::install_from_env(
                cfg.workers,
                cfg.standbys,
                super::rtgang::gang_procs_wanted().max(0) as usize,
            );
            let rt = runtime::Runtime::new(cfg);
            let pool = runtime::WorkerPool::spawn_with(Arc::clone(&rt), spawn_worker)
                .expect("runtime worker pool spawn failed");
            // Process-lifetime pool: the handles are never joined; leak the
            // pool handle so its Drop (if any ever grows one) can't fire.
            std::mem::forget(pool);
            // Publish the process-global handle (M1: the executor's runtime
            // scan arm reaches the runtime through `runtime::global()`,
            // avoiding an execmain -> launch_backend dependency).
            runtime::install_global(Arc::clone(&rt));
            // Publish pool liveness where plan-time code can see it: the
            // M5-3 suppression probe (guc_tables::parallel_engine) requires
            // a LIVE pool before suppressing any Gather — never suppress
            // what the runtime cannot pick up (t34-config review, defect 3).
            guc_tables::runtime_pool::set_runtime_pool_live();
            // M4 background-job dispatcher (docs/design/m4-bgjobs.md §3.2):
            // its own kill switch on top of the pool's; with
            // PGRUST_RUNTIME_BGJOBS unset this is a no-op and no dispatcher
            // thread exists. The spawner applies the child prelude — the
            // dispatcher hosts job identity init and config reloads.
            let _ = bgjobs::start_if_enabled(&rt, spawn_dispatcher);
            rt
        })
    }

    fn spawn_worker(
        ordinal: usize,
        body: Box<dyn FnOnce() + Send>,
    ) -> std::io::Result<std::thread::JoinHandle<()>> {
        let pid: pid_t = super::reserve_child_pid();
        let inherited = super::Inherited::capture();
        // PERMIT-S5 (s2 §6 item 3 / review NB-1): the rtpool worker spawn
        // door — parent-side registration BEFORE the OS spawn, exactly the
        // postmaster_child_launch/wpool pattern. Symbolic watchdog naming:
        // dumps show "rtworker<ordinal>:<vpid>". No-op unless
        // PGRUST_SIM_SCHED=1 under `pgrust_sim`.
        #[cfg(pgrust_sim)]
        let sim_sched_slot = pgsync::sim::spawn_door::register_child(
            pid as u32,
            &format!("rtworker{ordinal}:{pid}"),
        );
        // SIMVFS-SHARED: parent-side universe capture (fd-table inheritance).
        #[cfg(pgrust_sim)]
        let sim_universe = super::sim_universe_capture();
        let spawned = std::thread::Builder::new()
            .name(format!("pg:rtworker{ordinal}:{pid}"))
            .stack_size(super::child_thread_stack_size())
            .spawn(move || {
                // Door discipline: bind + gate FIRST, so this guard's Drop
                // (the teardown epilogue) runs LAST — after the population
                // charge below has dropped inside the final quantum.
                #[cfg(pgrust_sim)]
                let _sim_sched_permit = pgsync::sim::spawn_door::enter_child(sim_sched_slot);
                // SIMVFS-SHARED: adopt the parent's universe FIRST.
                #[cfg(pgrust_sim)]
                super::sim_universe_adopt(sim_universe);
                // Any exit (only a request_stop can cause one — the pool is
                // process-lifetime) drops the population charge exactly once.
                struct PopulationCharge;
                impl Drop for PopulationCharge {
                    fn drop(&mut self) {
                        POPULATION.fetch_sub(1, Relaxed);
                    }
                }
                let _charge = PopulationCharge;
                inherited.apply();
                let _ = stack_depth::set_stack_base();
                // cpuaff increment A (default OFF): bind this executor to
                // its core (standby ordinals take the set mask) before any
                // work runs. Fail-open — a refused set degrades loud-once.
                super::cpuaff::apply_rtworker(ordinal);
                // M2 inc-2: PGPROC-leasing identity + exit/respawn
                // discipline around the loop (armed only; OFF = the plain
                // executor body, byte-identical).
                if pooldb_armed() {
                    pooldb_thread_main(ordinal, pid, body);
                } else {
                    body();
                }
            });
        match spawned {
            Ok(h) => {
                POPULATION.fetch_add(1, Relaxed);
                Ok(h)
            }
            Err(e) => {
                // F3 shape: retire the never-entered slot so the failed
                // spawn cannot leak a Runnable ghost into the schedule.
                #[cfg(pgrust_sim)]
                pgsync::sim::spawn_door::cancel_child(sim_sched_slot);
                Err(e)
            }
        }
    }

    fn spawn_dispatcher(
        body: Box<dyn FnOnce() + Send>,
    ) -> std::io::Result<std::thread::JoinHandle<()>> {
        let inherited = super::Inherited::capture();
        // The dispatcher hosts job GUC state (overlay capture + SIGHUP
        // ProcessConfigFile), so it gets the full child GUC prelude — the
        // fork-inherited nondefault values (command-line -c and config
        // file), exactly as postmaster_child_launch's thread body.
        // PERMIT-S5: that now includes the BASE-SHARE arm postmaster_child_
        // launch uses (one typed capture per config change; the child
        // adopts postmaster-validated values without re-running check
        // hooks). The dispatcher previously always took the string
        // capture/restore path, whose re-run check hooks re-read e.g. the
        // timezone database from THIS thread — under sim that is a read in
        // the dispatcher's empty thread-local SimVfs universe (the L1
        // wedge-ledger class) and the prelude dies. PGRUST_NO_GUC_BASE
        // reverts to the string path for A/B, exactly as on the spawn path.
        let guc_base = guc::layers::ensure_base_current();
        let guc_snapshot = if guc::layers::base_share_enabled() {
            Vec::new()
        } else {
            guc::store::capture_nondefault_variables()
        };
        // PERMIT-S5: the dispatcher spawn door. The dispatcher has no
        // product pid identity; its model vpid is minted from the SAME
        // guarded counter as every child (sim-only — the native pid stream
        // is untouched).
        #[cfg(pgrust_sim)]
        let sim_sched_slot = {
            let vpid = super::reserve_child_pid();
            pgsync::sim::spawn_door::register_child(
                vpid as u32,
                &format!("bgjobs-dispatcher:{vpid}"),
            )
        };
        // SIMVFS-SHARED: parent-side universe capture (fd-table inheritance).
        #[cfg(pgrust_sim)]
        let sim_universe = super::sim_universe_capture();
        let spawned = std::thread::Builder::new()
            .name("pg-bgjobs-dispatcher".into())
            .stack_size(super::child_thread_stack_size())
            .spawn(move || {
                // Door discipline: bind + gate FIRST (drop = epilogue LAST).
                #[cfg(pgrust_sim)]
                let _sim_sched_permit = pgsync::sim::spawn_door::enter_child(sim_sched_slot);
                // SIMVFS-SHARED: adopt the parent's universe FIRST — with a
                // shared universe the dispatcher's GUC prelude can read the
                // timezone db even on the string-restore arm (the s5 §6.2
                // kill class dies here, not at the base-share workaround).
                #[cfg(pgrust_sim)]
                super::sim_universe_adopt(sim_universe);
                inherited.apply();
                let _ = stack_depth::set_stack_base();
                if guc::layers::base_share_enabled() {
                    guc::store::initialize_guc_options_for_child_base(&guc_base)
                        .and_then(|()| guc::layers::bind_base(&guc_base))
                        .unwrap_or_else(|e| {
                            panic!("bgjobs dispatcher GUC base bind failed: {e:?}")
                        });
                } else {
                    guc::store::initialize_guc_options_for_child(&guc_snapshot)
                        .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                        .unwrap_or_else(|e| {
                            panic!("bgjobs dispatcher GUC prelude failed: {e:?}")
                        });
                }
                body();
            });
        match spawned {
            Ok(h) => Ok(h),
            Err(e) => {
                #[cfg(pgrust_sim)]
                pgsync::sim::spawn_door::cancel_child(sim_sched_slot);
                Err(e)
            }
        }
    }

    /// M4 bgjobs increment 4 (docs/design/m4-bgjobs.md §3.6, the virtual
    /// child): with `PGRUST_RUNTIME_BGJOBS=1`, StartChildProcess's launch
    /// of a MIGRATED daemon creates a dispatcher job instead of a thread
    /// and returns the reserved pid — the PmChild, count_children masks,
    /// SignalChildren fanout (thread signals land in the job's procsignal
    /// slot and wake the dispatcher through the latch redirect), and the
    /// LaunchMissingBackgroundProcesses restart logic are all untouched.
    /// The exit announce comes from the job's teardown; the reaper's join
    /// is a CHILD_THREADS lookup miss. Postmaster thread only.
    pub fn try_launch_job(child_type: types_core::BackendType, child_slot: i32) -> Option<pid_t> {
        let migrated = matches!(
            child_type,
            types_core::BackendType::BgWriter | types_core::BackendType::WalWriter
        );
        if !migrated || !bgjobs::bgjobs_enabled() {
            return None;
        }
        // The pool + dispatcher start lazily here when the daemon launch
        // precedes ServerLoop's rtpool start (boot starts bgwriter from
        // main_entry). Both are postmaster-thread idempotent.
        let rt = start_if_enabled()?;
        let dispatcher = bgjobs::start_if_enabled(rt, spawn_dispatcher)?;
        let pid: pid_t = super::reserve_child_pid();
        match child_type {
            types_core::BackendType::BgWriter => {
                dispatcher.register(std::sync::Arc::new(bgwriter::job::new_bgwriter_job(
                    pid, child_slot,
                )));
            }
            types_core::BackendType::WalWriter => {
                dispatcher.register(std::sync::Arc::new(walwriter::job::new_walwriter_job(
                    pid, child_slot,
                )));
            }
            _ => unreachable!("migrated set checked above"),
        }
        Some(pid)
    }
}

pub mod rtgang {
    //! M2 pool-binding spawn glue: STANDING runtime executor threads
    //! (parallel::standing — see that module's doc for the architecture).
    //! This module owns everything postmaster/thread-substrate-shaped:
    //! boot capture (Inherited globals + GUC base), thread spawn, the
    //! bgworker-equivalent identity bring-up (InitPostmasterChild +
    //! synthetic bgworker entry + InitProcess from the boot-reserved
    //! segment + BaseInit), and the run_child_task-shaped exit discipline
    //! (ProcExitThread -> deferred-callback drain, so ProcKill releases
    //! the PGPROC; GangExit::Raw exits with zero shmem interaction for
    //! the crash fence).
    //!
    //! Gang threads are process-lifetime and REGISTRY-INVISIBLE: no
    //! postmaster child slot, no CHILD_THREADS entry, no bgworker
    //! registry slot, no exit announce — the shutdown state machine
    //! counts pmchild `active` only, so they neither wedge shutdown nor
    //! receive SIGTERM (they die with the process; DROP DATABASE and
    //! crash reinit ride parallel::standing's retire/epoch paths).

    use pgsync::OnceLock;
    use types_core::pid_t;

    struct Boot {
        inherited: super::Inherited,
        guc_base: Option<std::sync::Arc<guc::layers::GucBaseSnapshot>>,
        guc_snapshot: Vec<guc::store::NondefaultGuc>,
    }

    static BOOT: OnceLock<Boot> = OnceLock::new();

    /// Live gang threads, parent-charged: incremented BEFORE the OS spawn
    /// (so a spawn in flight at shutdown is never invisible), decremented at
    /// the true end of gang_thread (after the deferred-callback drain — the
    /// last shared-memory interaction). The postmaster's PM_WAIT_BACKENDS
    /// gate reads it through the rtgang_live seam: the shutdown checkpoint
    /// must never run behind a live, registry-invisible gang thread
    /// (C-parity — every C child is counted before ShutdownXLOG).
    static LIVE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

    pub fn live_gang_threads() -> i32 {
        LIVE.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Postmaster shutdown: fence the standing gang (parked workers exit
    /// clean; new engagements refuse). The exiting threads poke
    /// PMSIGNAL_ADVANCE_STATE_MACHINE so the PM_WAIT_BACKENDS gate re-runs
    /// as the count drains.
    pub fn retire_for_shutdown() {
        parallel::standing::retire_for_shutdown();
    }

    /// Decrement + state-machine poke at gang-thread end (RAII so every
    /// exit path of gang_thread — clean, raw, drained panic — reports).
    struct LiveGuard;
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            // Wake the postmaster's shutdown gate. Cheap and harmless
            // outside shutdown (one extra state-machine pass); guarded so
            // unit substrates without a postmaster stay inert.
            if init_small::globals::IsUnderPostmaster() {
                pmsignal::SendPostmasterSignal(
                    pmsignal::PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE,
                );
            }
        }
    }

    /// SIMCORPUS RED (sim builds only, env-armed by the sim harness):
    /// spawn gang workers WITHOUT their spawn door — the "new spawn site
    /// forgot the door" wiring bug (the simvfs-shared worklog's named
    /// census trap), deliberately resurrected. The un-doored thread is
    /// invisible to the permit scheduler, so its FIRST shared-universe
    /// touch (the adoption right after spawn) dies loudly at the strict
    /// access probe — the deterministic catch the P10 red asserts.
    /// Env-armed (`PGRUST_SIM_DOORSKIP=rtgang`) because the sim harness
    /// (tcop) cannot call this crate directly (package cycle — the
    /// wpool/rtpool seam reason); cfg(pgrust_sim) keeps it native-inert.
    #[cfg(pgrust_sim)]
    fn doorskip_red_armed() -> bool {
        static ARMED: OnceLock<bool> = OnceLock::new();
        *ARMED
            .get_or_init(|| std::env::var("PGRUST_SIM_DOORSKIP").as_deref() == Ok("rtgang"))
    }

    /// PGPROCs to boot-reserve for the gang: PGRUST_RUNTIME=1 (+ the
    /// PGRUST_RUNTIME_POOLBIND=0 kill) gates it; PGRUST_RUNTIME_GANG
    /// overrides the size (default = the runtime's worker count = cores).
    /// Called by the postmaster BEFORE InitializeMaxBackends (sizing) and
    /// again at install (wiring) — one pure function, values agree.
    pub fn gang_procs_wanted() -> i32 {
        if !runtime::runtime_enabled() || !parallel::standing::pool_binding_enabled() {
            return 0;
        }
        let n = std::env::var("PGRUST_RUNTIME_GANG")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or_else(|| runtime::RuntimeConfig::from_env().workers as i64);
        n.clamp(0, 1024) as i32
    }

    /// The ONE boot PGPROC-budget authority (SetRuntimeGangProcs — both the
    /// postmaster's main_entry and the sim seam consume this): the standing
    /// gang's reservation PLUS, under PGRUST_RUNTIME_POOLDB=1 (M2 inc-2),
    /// one PGPROC per pool thread (workers + standbys). The gang keeps its
    /// own segment while it remains the fallback channel; both terms are 0
    /// under their kill switches — byte-identical sizing.
    pub fn runtime_reserved_procs() -> i32 {
        gang_procs_wanted() + super::rtpool::pool_procs_wanted()
    }

    /// Postmaster boot wiring (serverloop, next to rtpool::start_if_enabled;
    /// postmaster thread only — the Inherited/GUC captures below are only
    /// valid there). Lazy thereafter: threads spawn at first engagement.
    ///
    /// GUC staleness note: the boot-captured base/snapshot seeds a gang
    /// thread's store; per engagement the query-task binder applies the
    /// leader's query PIN, which adopts the leader's CURRENT base — a
    /// post-SIGHUP engagement never sees stale boot values.
    pub fn install_if_enabled() {
        let n = gang_procs_wanted();
        if n <= 0 {
            return;
        }
        let _ = BOOT.set(Boot {
            inherited: super::Inherited::capture(),
            guc_base: guc::layers::base_share_enabled()
                .then(guc::layers::ensure_base_current),
            guc_snapshot: if guc::layers::base_share_enabled() {
                Vec::new()
            } else {
                guc::store::capture_nondefault_variables()
            },
        });
        parallel::standing::install_spawner(n as usize, spawn_gang_worker);
    }

    /// parallel::standing spawner: may run on any backend thread (first
    /// engagement / respawn) — everything postmaster-scoped was captured
    /// at install.
    fn spawn_gang_worker(ordinal: usize) -> bool {
        let Some(boot) = BOOT.get() else { return false };
        let child_pid: pid_t = super::reserve_child_pid();
        // PERMIT-S5 (s2 §6 item 3 / review NB-1): the rtgang spawn door —
        // parent-side registration keyed by the reserved pid, symbolic
        // watchdog naming ("rtgang<ordinal>:<vpid>"). The spawner may run on
        // any backend thread (first engagement / respawn); parent-side
        // registration needs no spawn fence (spawn_door module doc).
        #[cfg(pgrust_sim)]
        let sim_sched_slot = if doorskip_red_armed() {
            // RED: the forgotten door. enter_child(None) below is a no-op,
            // so the child runs unregistered — and dies at the adoption's
            // access probe (the catch).
            None
        } else {
            pgsync::sim::spawn_door::register_child(
                child_pid as u32,
                &format!("rtgang{ordinal}:{child_pid}"),
            )
        };
        // SIMVFS-SHARED: parent-side universe capture (fd-table
        // inheritance). The gang spawner may run on any BACKEND thread at
        // first engagement — that thread is bound to the process universe,
        // so the capture inherits correctly from wherever the gang is born.
        #[cfg(pgrust_sim)]
        let sim_universe = super::sim_universe_capture();
        // Parent-side live charge (see LIVE): visible to the shutdown gate
        // before the thread exists.
        LIVE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let spawned = std::thread::Builder::new()
            .name(format!("pg:rtgang{ordinal}:{child_pid}"))
            .stack_size(super::child_thread_stack_size())
            .spawn(move || {
                // Door discipline: the guard is declared BEFORE gang_thread
                // runs, so every guard inside it drops first and this drop —
                // the teardown epilogue — runs LAST.
                #[cfg(pgrust_sim)]
                let _sim_sched_permit = pgsync::sim::spawn_door::enter_child(sim_sched_slot);
                // SIMVFS-SHARED: adopt the engagement's universe FIRST —
                // the gang's bring-up (database bind, catalog reads) and its
                // scan reads all go through vfs.
                #[cfg(pgrust_sim)]
                super::sim_universe_adopt(sim_universe);
                // cpuaff increment A (default OFF): the gang mirrors the
                // rtworker ordinal->core map. Fail-open, loud-once.
                super::cpuaff::apply_rtgang(ordinal);
                gang_thread(ordinal, child_pid, boot)
            });
        match spawned {
            Ok(_) => true,
            Err(_) => {
                // F3 shape: retire the never-entered slot (and refund the
                // parent-side live charge — no LiveGuard ever runs).
                LIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                #[cfg(pgrust_sim)]
                pgsync::sim::spawn_door::cancel_child(sim_sched_slot);
                false
            }
        }
    }

    fn gang_thread(ordinal: usize, child_pid: pid_t, boot: &'static Boot) {
        // Live-count refund + shutdown-gate poke, dropped LAST among this
        // frame's locals (declared first) — after the deferred-callback
        // drain below, i.e. after the thread's final shared-memory touch.
        let _live = LiveGuard;
        // Thread-scoped local latch slot (returned on thread exit).
        let _local_latch_release = miscinit::LocalLatchReleaseGuard::new();
        boot.inherited.apply();
        let _ = stack_depth::set_stack_base();
        let guc_ok = if let Some(base) = &boot.guc_base {
            guc::store::initialize_guc_options_for_child_base(base)
                .and_then(|()| guc::layers::bind_base(base))
        } else {
            guc::store::initialize_guc_options_for_child(&boot.guc_snapshot)
                .and_then(|()| guc::store::restore_nondefault_variables(&boot.guc_snapshot))
        };
        if let Err(e) = guc_ok {
            let _ = elog::elog(
                types_error::WARNING,
                format!("standing executor {ordinal}: GUC bring-up failed: {}", e.message()),
            );
            parallel::standing::note_worker_exit(ordinal);
            return;
        }

        // bgworker-equivalent identity (BackgroundWorkerMain +
        // run_worker_body, minus registry/dispatch).
        if let Err(e) = miscinit::InitPostmasterChild(child_pid) {
            let _ = elog::elog(
                types_error::WARNING,
                format!("standing executor {ordinal}: InitPostmasterChild failed: {}", e.message()),
            );
            parallel::standing::note_worker_exit(ordinal);
            return;
        }
        procsignal::pqsignal_thread(
            procsignal::signums::SIGQUIT,
            procsignal::ThreadSignalHandler::Simple(super::default_sigquit_handler),
        );
        miscinit::SetMyBackendType(types_core::init::BackendType::BgWorker);
        bgworker::adopt_worker_entry(bgworker::BackgroundWorker {
            bgw_name: format!("runtime standing executor {ordinal}"),
            bgw_type: "runtime standing executor".to_string(),
            bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS
                | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
            bgw_start_time: bgworker::BgWorkerStartTime::ConsistentState,
            bgw_restart_time: bgworker::BGW_NEVER_RESTART,
            bgw_main: gang_entry_never,
            bgw_main_arg: 0,
            bgw_extra: [0u8; bgworker::BGW_EXTRALEN],
            bgw_notify_pid: 0,
        });
        // Per-thread timeout machinery, exactly where the bgworker glue
        // does it (after signal dispositions, before any connect): the
        // gang's warm/cold InitPostgres path registers the session
        // timeouts, and RegisterTimeout debug_asserts InitializeTimeouts
        // ran on THIS thread. Latent in m2-pool-binding -- its tranche rode
        // a fast-profile sweep (assert compiled out); the train-12 split
        // dev-profile tranche jobs exposed it (13 warm-connect panics,
        // gang dead, standing refusals on every engagement).
        timeout_seams::initialize_timeouts::call();

        // The loop + exit discipline (run_child_task-shaped): proc_exit's
        // unwind drains the deferred callbacks (ProcKill releases the
        // boot-reserved PGPROC against live shmem); GangExit::Raw and
        // pre-InitProcess failures exit with no shmem interaction; other
        // panics are logged and the slot is left respawnable.
        let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Err(e) = lmgr_proc::InitProcess(types_core::init::BackendType::BgWorker) {
                let _ = elog::elog(
                    types_error::WARNING,
                    format!("standing executor {ordinal}: InitProcess failed: {}", e.message()),
                );
                return;
            }
            if let Err(e) = postinit::BaseInit() {
                let _ = elog::elog(
                    types_error::WARNING,
                    format!("standing executor {ordinal}: BaseInit failed: {}", e.message()),
                );
                // PGPROC is claimed: release identity via the exit path.
                ipc::proc_exit(1, init_small::globals::MyProcPid());
            }
            match parallel::standing::gang_worker_loop(ordinal) {
                parallel::standing::GangExit::Clean => {
                    ipc::proc_exit(0, init_small::globals::MyProcPid());
                }
                parallel::standing::GangExit::Raw => {}
            }
        }));
        parallel::standing::note_worker_exit(ordinal);
        match body {
            Ok(()) => {} // Raw exit (crash fence) or pre-PGPROC failure.
            Err(payload) => {
                if let Some(p) = payload.downcast_ref::<ipc::ProcExitThread>() {
                    let code = p.code;
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        ipc::run_deferred_exit_callbacks(code)
                    }));
                } else {
                    // Generic panic: unlike run_child_task there is no exit
                    // announce here (registry-invisible thread), so no
                    // crash-reinit backstop reclaims this identity — a
                    // leaked procarray/sinval entry would block DROP
                    // DATABASE forever and a leaked PGPROC drains the
                    // freelist. Run the drain (best effort, ProcKill
                    // included); a mid-drain panic leaves us no worse.
                    let _ = elog::elog(
                        types_error::WARNING,
                        format!(
                            "standing executor {ordinal} died on a panic; releasing identity"
                        ),
                    );
                    // proc_exit arms the deferred-callback flag (and
                    // unwinds ProcExitThread, caught here); the drain then
                    // actually runs the stack.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        ipc::proc_exit(2, init_small::globals::MyProcPid())
                    }));
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        ipc::run_deferred_exit_callbacks(2)
                    }));
                }
            }
        }
    }

    fn gang_entry_never(_arg: u64) -> types_error::PgResult<()> {
        unreachable!("standing executor entry is never dispatched")
    }
}
