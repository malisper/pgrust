//! Seam declarations for the `backend-utils-init-small` unit
//! (`utils/init/globals.c`, `utils/init/usercontext.c`): backend-global
//! variable reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.


seam_core::seam!(
    /// `work_mem` (globals.c): the `work_mem` GUC — per-operation memory
    /// budget in kilobytes.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `MyProcPid` (globals.c): this backend's PID, set at process start.
    pub fn my_proc_pid() -> i32
);

seam_core::seam!(
    /// `MyProcNumber` (globals.c) — the pgprocno of the current backend, or
    /// `INVALID_PROC_NUMBER` when no `PGPROC` is attached (`MyProc == NULL`,
    /// i.e. during bootstrap / shared-memory initialization).
    pub fn my_proc_number() -> types_core::ProcNumber
);

seam_core::seam!(
    /// `IsUnderPostmaster` (globals.c) — false in the postmaster itself, true
    /// in a forked backend.
    pub fn is_under_postmaster() -> bool
);

seam_core::seam!(
    /// `MaxBackends` (globals.c): the computed backend-slot count, fixed at
    /// postmaster startup.
    pub fn max_backends() -> i32
);

seam_core::seam!(
    /// `MaxConnections` (globals.c): the `max_connections` GUC — number of
    /// regular-backend slots.
    pub fn max_connections() -> i32
);

seam_core::seam!(
    /// `max_worker_processes` (globals.c): the GUC capping the number of
    /// background-worker slots.
    pub fn max_worker_processes() -> i32
);

seam_core::seam!(
    /// `max_prepared_xacts` (globals.c): the GUC bounding the number of
    /// concurrently-prepared transactions (dummy PGPROC slots).
    pub fn max_prepared_xacts() -> i32
);

seam_core::seam!(
    /// `autovacuum_worker_slots` (globals.c): the GUC bounding the number of
    /// autovacuum-worker slots.
    pub fn autovacuum_worker_slots() -> i32
);

seam_core::seam!(
    /// `FastPathLockGroupsPerBackend` (globals.c): the number of fast-path
    /// lock groups per backend, computed at startup from `max_locks_per_xact`.
    pub fn fast_path_lock_groups_per_backend() -> i32
);

seam_core::seam!(
    /// `max_parallel_workers` (globals.c): the GUC capping concurrently
    /// active parallel workers.
    pub fn max_parallel_workers() -> i32
);

seam_core::seam!(
    /// `PostAuthDelay` (globals.c): seconds to sleep after authentication, to
    /// allow attaching a debugger. Read by `BackgroundWorkerMain`.
    pub fn post_auth_delay() -> i32
);

seam_core::seam!(
    /// `MyProcPort` (globals.c): run `f` with mutable access to this
    /// backend's connection `Port`, or `None` when there is no client
    /// connection (`MyProcPort == NULL`). Callback shape per the seam rules:
    /// a seam must not hand out `&'static mut`.
    pub fn with_my_proc_port(f: &mut dyn FnMut(Option<&mut types_net::Port>))
);

seam_core::seam!(
    /// `ClientConnectionLost = value` (globals.c / miscadmin.h).
    pub fn set_client_connection_lost(value: bool)
);

seam_core::seam!(
    /// `InterruptPending = value` (globals.c).
    pub fn set_interrupt_pending(value: bool)
);

seam_core::seam!(
    /// `ProcDiePending = value` (globals.c).
    pub fn set_proc_die_pending(value: bool)
);

seam_core::seam!(
    /// `QueryCancelPending = value` (globals.c).
    pub fn set_query_cancel_pending(value: bool)
);

seam_core::seam!(
    /// `InterruptHoldoffCount = value` (globals.c).
    pub fn set_interrupt_holdoff_count(value: u32)
);

seam_core::seam!(
    /// `HOLD_INTERRUPTS()` (miscadmin.h): `InterruptHoldoffCount++`.
    pub fn hold_interrupts()
);

seam_core::seam!(
    /// `RESUME_INTERRUPTS()` (miscadmin.h): `InterruptHoldoffCount--` (with
    /// the underflow Assert).
    pub fn resume_interrupts()
);

seam_core::seam!(
    /// Write `MyBackendType` (globals.c, declared in miscadmin.h): processes
    /// assign their own type at startup (e.g. `MyBackendType = B_LOGGER` in
    /// SysLoggerMain). Per-crate mirrors of this global (e.g. elog's
    /// `am_syslogger`) are updated by the assigning unit itself.
    ///
    /// Decision (recorded per AGENTS.md): this write-side seam to a foreign
    /// ambient global is accepted because process-identity assignment at
    /// bootstrap is the C semantics; when `launch_backend` lands, prefer
    /// folding backend-type assignment into the
    /// `postmaster_child_launch`/child-main contract and retiring this seam.
    pub fn set_my_backend_type(backend_type: types_core::init::BackendType)
);

seam_core::seam!(
    /// `NBuffers` (globals.c): the `shared_buffers` GUC — number of shared
    /// buffers. Pure read of backend-local state.
    pub fn nbuffers() -> i32
);

seam_core::seam!(
    /// `MyDatabaseId` (globals.c).
    pub fn my_database_id() -> types_core::Oid
);

seam_core::seam!(
    /// `MyDatabaseTableSpace` (globals.c).
    pub fn my_database_table_space() -> types_core::Oid
);

seam_core::seam!(
    /// `DatabasePath` (globals.c): the path to the current database's data
    /// directory, set up at backend startup. Returns an owned copy of the
    /// backend-global string (the caller uses it transiently). `Err` carries
    /// the OOM surface of copying the global.
    pub fn database_path() -> types_error::PgResult<String>
);

seam_core::seam!(
    /// Read `IsPostmasterEnvironment` (`globals.c`).
    pub fn is_postmaster_environment() -> bool
);

seam_core::seam!(
    /// `MyPMChildSlot = child_slot` (`globals.c`): record the `PMChildFlags`
    /// array index reserved for this child process.
    pub fn set_my_pm_child_slot(child_slot: i32)
);

seam_core::seam!(
    /// `MyClientSocket = palloc(...); memcpy(...)` (`globals.c` global): store
    /// this child's inherited client socket.
    pub fn set_my_client_socket(client_sock: types_net::ClientSocket)
);

seam_core::seam!(
    /// `*MyClientSocket` (`globals.c` global): the inherited accepted client
    /// socket, copied out. `None` when `MyClientSocket == NULL`. Pure read of
    /// process-identity state.
    pub fn my_client_socket() -> Option<types_net::ClientSocket>
);

seam_core::seam!(
    /// `START_CRIT_SECTION()` — increment `CritSectionCount` (globals.c);
    /// while non-zero any ERROR escalates to PANIC.
    pub fn start_critical_section()
);

seam_core::seam!(
    /// `END_CRIT_SECTION()` — decrement `CritSectionCount`.
    pub fn end_critical_section()
);

seam_core::seam!(
    /// Read `ExitOnAnyError` (globals.c).
    pub fn exit_on_any_error() -> bool
);

seam_core::seam!(
    /// Write `ExitOnAnyError` (BeginInternalSubTransaction forces FATAL exit
    /// on error around its body).
    pub fn set_exit_on_any_error(value: bool)
);

seam_core::seam!(
    /// `MyBackendType` (globals.c, declared in miscadmin.h) — this process's
    /// identity, assigned once at startup (the `AmStartupProcess()` /
    /// `AmWalReceiverProcess()` macros are `MyBackendType == B_*` tests).
    /// Process-identity read, same class as `my_proc_pid`.
    pub fn my_backend_type() -> types_core::init::BackendType
);

seam_core::seam!(
    /// `IsBinaryUpgrade` (globals.c / miscadmin.h): true during a
    /// `pg_upgrade`-driven binary upgrade. The launcher refuses to register the
    /// logical-replication launcher in this mode. Pure read of backend-local
    /// state.
    pub fn is_binary_upgrade() -> bool
);

// --- backend-utils-init-postinit consumers (globals.c per-backend state) ---

seam_core::seam!(
    /// `MaxBackends = value` (globals.c): set the computed total backend count.
    pub fn set_max_backends(value: i32)
);

seam_core::seam!(
    /// `MyProcPort != NULL` (globals.c): does this backend have a client Port?
    pub fn has_my_proc_port() -> bool
);

seam_core::seam!(
    /// `SuperuserReservedConnections` (globals.c GUC).
    pub fn superuser_reserved_connections() -> i32
);

seam_core::seam!(
    /// `ReservedConnections` (globals.c GUC).
    pub fn reserved_connections() -> i32
);

seam_core::seam!(
    /// `MyDatabaseId = dboid` (globals.c): set the backend's database OID.
    pub fn set_my_database_id(dboid: types_core::primitive::Oid)
);

seam_core::seam!(
    /// `MyDatabaseTableSpace = spcoid` (globals.c).
    pub fn set_my_database_table_space(spcoid: types_core::primitive::Oid)
);

seam_core::seam!(
    /// `MyDatabaseHasLoginEventTriggers = value` (globals.c).
    pub fn set_my_database_has_login_event_triggers(value: bool)
);

seam_core::seam!(
    /// `MyProcPort->cmdline_options` (globals.c Port): the `-c`-style command
    /// line options string from the startup packet, copied into `mcx`, or
    /// `None` if absent.
    pub fn my_proc_port_cmdline_options<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `MyProcPort->guc_options` (globals.c Port): the alternating
    /// name/value GUC settings from the startup packet, copied into `mcx`.
    pub fn my_proc_port_guc_options<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `MyProcPort->user_name` (globals.c Port), copied into `mcx`.
    pub fn my_proc_port_user_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `MyProcPort->database_name` (globals.c Port), copied into `mcx`.
    pub fn my_proc_port_database_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `MyProcPort->application_name` (globals.c Port), copied into `mcx`, or
    /// `None` if not set.
    pub fn my_proc_port_application_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);
