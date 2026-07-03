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
        main_fn: Main::Unported("AutoVacWorkerMain (backend-postmaster-autovacuum)"),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "bgworker",
        main_fn: Main::Unported("BackgroundWorkerMain (backend-postmaster-bgworker)"),
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
        main_fn: Main::Unported("PgArchiverMain (backend-postmaster-pgarch)"),
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
        main_fn: Main::Unported("WalSummarizerMain (backend-postmaster-walsummarizer)"),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "wal_writer",
        main_fn: Main::Ported(walwriter::WalWriterMain),
        shmem_attach: true,
    },
    ChildProcessKind {
        name: "syslogger",
        main_fn: Main::Unported("SysLoggerMain (backend-postmaster-syslogger)"),
        shmem_attach: false,
    },
];

/// PostmasterChildName (launch_backend.c).
pub fn postmaster_child_name(child_type: BackendType) -> &'static str {
    CHILD_PROCESS_KINDS[child_type as usize].name
}

static NEXT_CHILD_PID: AtomicI32 = AtomicI32::new(1000);

fn reserve_child_pid() -> pid_t {
    NEXT_CHILD_PID.fetch_add(1, Ordering::Relaxed)
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
        });

    match spawned {
        Ok(_handle) => child_pid, // detached; reaping is pmchild design

        Err(_) => -1,
    }
}

pub fn init_seams() {}
