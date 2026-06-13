//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `CreateSocketLockFile(socketfile, amPostmaster, socketDir)` — create
    /// the interlock file for a Unix socket path and arrange for it to be
    /// removed at exit. Failure paths `ereport(FATAL)` inside
    /// `CreateLockFile`.
    pub fn create_socket_lock_file(
        socketfile: &str,
        am_postmaster: bool,
        socket_dir: &str,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `process_shmem_requests_in_progress` (miscinit.c) — true only while the
    /// postmaster is running registered `shmem_request_hook`s.
    pub fn process_shmem_requests_in_progress() -> bool
);

seam_core::seam!(
    /// `process_shared_preload_libraries_in_progress` (miscinit.c) — whether
    /// the backend is currently inside the `shared_preload_libraries`
    /// initialization window. A backend-local global read.
    pub fn process_shared_preload_libraries_in_progress() -> bool
);

seam_core::seam!(
    /// `IsBootstrapProcessingMode()` (miscadmin.h): `Mode ==
    /// BootstrapProcessing`. A plain global read — infallible.
    pub fn is_bootstrap_processing_mode() -> bool
);

// ---------------------------------------------------------------------------
// Per-backend process identity + the slot-sync worker's bootstrap sequence.
//
// These cross several C owners (globals.c `MyProcPid`, miscadmin
// `MyBackendType`/`SetProcessingMode`, ps_status.c, proc.c `InitProcess`,
// postinit.c `BaseInit`/`InitPostgres`, timeout.c, and the worker's signal
// setup). They are consolidated under the process-init owner because each is a
// single leaf step of one backend's startup; each is annotated with its true
// C source.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `MyProcPid` (globals.c) — this backend's PID.
    pub fn my_proc_pid() -> i32
);

seam_core::seam!(
    /// `AmLogicalSlotSyncWorkerProcess()` (miscadmin.h): `MyBackendType ==
    /// B_SLOTSYNC_WORKER`. A per-backend global read — infallible.
    pub fn am_logical_slot_sync_worker_process() -> bool
);

seam_core::seam!(
    /// `MyBackendType = B_SLOTSYNC_WORKER` (miscadmin.h) — declare this backend
    /// as the slot-sync worker.
    pub fn set_my_backend_type_slotsync() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SetProcessingMode(NormalProcessing)` (miscadmin.h).
    pub fn set_processing_mode_normal() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `init_ps_display("")` (utils/misc/ps_status.c) — initialize the process
    /// title display.
    pub fn init_ps_display() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitProcess()` (storage/lmgr/proc.c) — create this backend's PGPROC in
    /// shared memory.
    pub fn init_process() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BaseInit()` (utils/init/postinit.c) — early per-backend initialization.
    pub fn base_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitPostgres(dbname, InvalidOid, NULL, InvalidOid, 0, NULL)`
    /// (utils/init/postinit.c) — connect to and initialize the named database.
    pub fn init_postgres(dbname: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeTimeouts()` (utils/misc/timeout.c) — establish the SIGALRM
    /// handler and initialize the timeout module.
    pub fn initialize_timeouts() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The slot-sync worker's signal-handler installation block (the
    /// `pqsignal(...)` sequence in `ReplSlotSyncWorkerMain`). Signals stay
    /// blocked until [`unblock_signals`]; matching C ordering.
    pub fn setup_signal_handlers() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)` — unblock signals after
    /// the postmaster forked the worker with them blocked.
    pub fn unblock_signals() -> types_error::PgResult<()>
);

