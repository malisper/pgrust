//! Port of `src/backend/postmaster/launch_backend.c` — launching backends and
//! other postmaster child processes.
//!
//! On Unix a new child process is launched with `fork()`. It inherits all the
//! global variables and data structures that had been initialized in the
//! postmaster. After forking, the child closes the file descriptors that are
//! not needed in the child, sets up the mechanism to detect death of the
//! parent postmaster, etc. After that, it calls the right Main function
//! depending on the kind of child process.
//!
//! The `EXEC_BACKEND` half of the C file (fork+exec, used on Windows:
//! `BackendParameters`, `internal_forkexec`, `SubPostmasterMain`, the
//! save/restore/read backend-variables machinery, and the inheritable-socket
//! helpers) is not compiled on a Unix build and is not ported.

use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::pid_t;
use types_net::ClientSocket;
use types_startup::StartupData;

/// `IsExternalConnectionBackend(backend_type)` (`miscadmin.h`):
/// `backend_type == B_BACKEND || backend_type == B_WAL_SENDER`.
#[inline]
fn is_external_connection_backend(backend_type: BackendType) -> bool {
    backend_type == BackendType::Backend || backend_type == BackendType::WalSender
}

/// C: `void (*main_fn)(const void *startup_data, size_t startup_data_len)` —
/// a child process's entry point; never returns. [`StartupData`] is the typed
/// stand-in for the pointer/length pair.
type ChildMainFn = fn(&StartupData) -> !;

/// C: `child_process_kind` — information needed to launch one kind of child
/// process.
struct ChildProcessKind {
    name: &'static str,
    main_fn: Option<ChildMainFn>,
    shmem_attach: bool,
}

/// C: `static child_process_kind child_process_kinds[]` — a
/// designated-initializer array indexed by `BackendType`. Entries here are in
/// enum order, so `CHILD_PROCESS_KINDS[child_type as usize]` indexes
/// identically. The `*Main` entry points live in other units and are reached
/// through their owners' seam crates; the C table's NULL `main_fn` slots
/// (`B_INVALID`, `B_WAL_SENDER`, `B_STANDALONE_BACKEND`) are `None`.
static CHILD_PROCESS_KINDS: [ChildProcessKind; BACKEND_NUM_TYPES] = [
    // [B_INVALID] = {"invalid", NULL, false},
    ChildProcessKind {
        name: "invalid",
        main_fn: None,
        shmem_attach: false,
    },
    // [B_BACKEND] = {"backend", BackendMain, true},
    ChildProcessKind {
        name: "backend",
        main_fn: Some(backend_tcop_backend_startup_seams::backend_main::call),
        shmem_attach: true,
    },
    // [B_DEAD_END_BACKEND] = {"dead-end backend", BackendMain, true},
    ChildProcessKind {
        name: "dead-end backend",
        main_fn: Some(backend_tcop_backend_startup_seams::backend_main::call),
        shmem_attach: true,
    },
    // [B_AUTOVAC_LAUNCHER] = {"autovacuum launcher", AutoVacLauncherMain, true},
    ChildProcessKind {
        name: "autovacuum launcher",
        main_fn: Some(backend_postmaster_autovacuum_seams::auto_vac_launcher_main::call),
        shmem_attach: true,
    },
    // [B_AUTOVAC_WORKER] = {"autovacuum worker", AutoVacWorkerMain, true},
    ChildProcessKind {
        name: "autovacuum worker",
        main_fn: Some(backend_postmaster_autovacuum_seams::auto_vac_worker_main::call),
        shmem_attach: true,
    },
    // [B_BG_WORKER] = {"bgworker", BackgroundWorkerMain, true},
    ChildProcessKind {
        name: "bgworker",
        main_fn: Some(backend_postmaster_bgworker_seams::background_worker_main::call),
        shmem_attach: true,
    },
    // [B_WAL_SENDER] = {"wal sender", NULL, true},
    //
    // WAL senders start their life as regular backend processes, and change
    // their type after authenticating the client for replication. We list it
    // here for PostmasterChildName() but cannot launch them directly.
    ChildProcessKind {
        name: "wal sender",
        main_fn: None,
        shmem_attach: true,
    },
    // [B_SLOTSYNC_WORKER] = {"slot sync worker", ReplSlotSyncWorkerMain, true},
    ChildProcessKind {
        name: "slot sync worker",
        main_fn: Some(backend_replication_logical_slotsync_seams::repl_slot_sync_worker_main::call),
        shmem_attach: true,
    },
    // [B_STANDALONE_BACKEND] = {"standalone backend", NULL, false},
    ChildProcessKind {
        name: "standalone backend",
        main_fn: None,
        shmem_attach: false,
    },
    // [B_ARCHIVER] = {"archiver", PgArchiverMain, true},
    ChildProcessKind {
        name: "archiver",
        main_fn: Some(backend_postmaster_pgarch_seams::pg_archiver_main::call),
        shmem_attach: true,
    },
    // [B_BG_WRITER] = {"bgwriter", BackgroundWriterMain, true},
    ChildProcessKind {
        name: "bgwriter",
        main_fn: Some(backend_postmaster_bgwriter_seams::background_writer_main::call),
        shmem_attach: true,
    },
    // [B_CHECKPOINTER] = {"checkpointer", CheckpointerMain, true},
    ChildProcessKind {
        name: "checkpointer",
        main_fn: Some(backend_postmaster_checkpointer_seams::checkpointer_main::call),
        shmem_attach: true,
    },
    // [B_IO_WORKER] = {"io_worker", IoWorkerMain, true},
    ChildProcessKind {
        name: "io_worker",
        main_fn: Some(backend_storage_aio_methods_seams::io_worker_main::call),
        shmem_attach: true,
    },
    // [B_STARTUP] = {"startup", StartupProcessMain, true},
    ChildProcessKind {
        name: "startup",
        main_fn: Some(backend_postmaster_startup_seams::startup_process_main::call),
        shmem_attach: true,
    },
    // [B_WAL_RECEIVER] = {"wal_receiver", WalReceiverMain, true},
    ChildProcessKind {
        name: "wal_receiver",
        main_fn: Some(backend_replication_walreceiver_seams::wal_receiver_main::call),
        shmem_attach: true,
    },
    // [B_WAL_SUMMARIZER] = {"wal_summarizer", WalSummarizerMain, true},
    ChildProcessKind {
        name: "wal_summarizer",
        main_fn: Some(backend_postmaster_walsummarizer_seams::wal_summarizer_main::call),
        shmem_attach: true,
    },
    // [B_WAL_WRITER] = {"wal_writer", WalWriterMain, true},
    ChildProcessKind {
        name: "wal_writer",
        main_fn: Some(backend_postmaster_walwriter_seams::wal_writer_main::call),
        shmem_attach: true,
    },
    // [B_LOGGER] = {"syslogger", SysLoggerMain, false},
    ChildProcessKind {
        name: "syslogger",
        main_fn: Some(backend_postmaster_syslogger_seams::sys_logger_main::call),
        shmem_attach: false,
    },
];

/// C: `const char *PostmasterChildName(BackendType child_type)`.
pub fn postmaster_child_name(child_type: BackendType) -> &'static str {
    CHILD_PROCESS_KINDS[child_type as usize].name
}

/// C: `pid_t postmaster_child_launch(BackendType child_type, int child_slot,
/// void *startup_data, size_t startup_data_len, ClientSocket *client_sock)`.
///
/// Start a new postmaster child process.
///
/// `child_slot` is the `PMChildFlags` array index reserved for the child
/// process. `startup_data` is the typed stand-in for the C `void
/// *`/`startup_data_len` pair passed to the child process
/// ([`StartupData::None`] is the C NULL). `client_sock`, when `Some`, is the
/// inherited client socket.
///
/// The child closes inherited resources, (optionally) detaches shared memory,
/// switches to `TopMemoryContext`, records `MyPMChildSlot` / `MyClientSocket`,
/// and dispatches to the per-type Main function, which never returns. The
/// parent returns the pid from `fork_process()` (`-1` on fork failure).
pub fn postmaster_child_launch(
    child_type: BackendType,
    child_slot: i32,
    startup_data: &mut StartupData,
    client_sock: Option<&ClientSocket>,
) -> pid_t {
    // Assert(IsPostmasterEnvironment && !IsUnderPostmaster);
    debug_assert!(
        backend_utils_init_small_seams::is_postmaster_environment::call()
            && !backend_utils_init_small_seams::is_under_postmaster::call()
    );

    // Capture time Postmaster initiates process creation for logging.
    if is_external_connection_backend(child_type) {
        // ((BackendStartupData *) startup_data)->fork_started = GetCurrentTimestamp();
        // The C cast's type confusion would be UB; panic is the loud equivalent.
        let StartupData::Backend(backend_startup_data) = &mut *startup_data else {
            panic!("postmaster_child_launch: {child_type:?} launched without BackendStartupData")
        };
        backend_startup_data.fork_started =
            backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    }

    let pid = backend_postmaster_fork_process_seams::fork_process::call();
    if pid == 0 {
        // child

        // Capture and transfer timings that may be needed for logging.
        if is_external_connection_backend(child_type) {
            let StartupData::Backend(backend_startup_data) = &*startup_data else {
                unreachable!()
            };
            let (socket_created, fork_started) = (
                backend_startup_data.socket_created,
                backend_startup_data.fork_started,
            );
            let fork_end = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
            backend_tcop_backend_startup_seams::set_conn_timing_child::call(
                socket_created,
                fork_started,
                fork_end,
            );
        }

        // Close the postmaster's sockets.
        backend_postmaster_postmaster_seams::close_postmaster_ports::call(
            child_type == BackendType::Logger,
        );

        // Detangle from postmaster.
        backend_utils_init_miscinit_seams::init_postmaster_child::call();

        // Detach shared memory if not needed.
        if !CHILD_PROCESS_KINDS[child_type as usize].shmem_attach {
            backend_storage_ipc_dsm_core_seams::dsm_detach_all::call();
            backend_port_sysv_shmem_seams::pg_shared_memory_detach::call();
        }

        // Enter the Main function with TopMemoryContext. The startup data is
        // allocated in PostmasterContext, so we cannot release it here yet.
        // The Main function will do it after it's done handling the startup
        // data.
        backend_utils_mmgr_mcxt_seams::switch_to_top_memory_context::call();

        // MyPMChildSlot = child_slot;
        backend_utils_init_small_seams::set_my_pm_child_slot::call(child_slot);
        if let Some(client_sock) = client_sock {
            // MyClientSocket = palloc(sizeof(ClientSocket)); memcpy(...);
            backend_utils_init_small_seams::set_my_client_socket::call(*client_sock);
        }

        // Run the appropriate Main function; it never returns. The C table's
        // NULL main_fn slots cannot be launched (a NULL call in C); panic is
        // the loud equivalent.
        let main_fn = CHILD_PROCESS_KINDS[child_type as usize]
            .main_fn
            .unwrap_or_else(|| {
                panic!("postmaster_child_launch: no main_fn for child type {child_type:?}")
            });
        main_fn(startup_data);
    }
    pid
}

/// Adapter from the `&[u8]` seam interface to the typed `&mut StartupData`
/// implementation. The seam was declared with a simplified signature covering
/// non-backend children (syslogger and similar auxiliary processes) for which
/// C always passes `startup_data == NULL, startup_data_len == 0`; those map to
/// [`types_startup::StartupData::None`]. A non-empty slice cannot be decoded
/// through this seam (the seam lacks the `BackendStartupData` fields) and
/// panics loudly so that the caller upgrades the seam declaration instead of
/// silently receiving wrong data.
fn postmaster_child_launch_seam_adapter(
    child_type: types_core::init::BackendType,
    child_slot: i32,
    startup_data: &[u8],
) -> i32 {
    assert!(
        startup_data.is_empty(),
        "postmaster_child_launch seam: non-empty &[u8] startup_data cannot \
         be decoded; extend the seam declaration to carry BackendStartupData"
    );
    let mut sd = types_startup::StartupData::None;
    postmaster_child_launch(child_type, child_slot, &mut sd, None)
}

/// Install this crate's seam implementations.
pub fn init_seams() {
    backend_postmaster_launch_backend_seams::postmaster_child_launch::set(
        postmaster_child_launch_seam_adapter,
    );
}

#[cfg(test)]
mod tests;
