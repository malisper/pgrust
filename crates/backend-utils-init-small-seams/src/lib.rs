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
    /// `MaxConnections` (globals.c): the `max_connections` GUC.
    pub fn max_connections() -> i32
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
    /// `MyDatabaseId` (globals.c): the OID of the database this backend is
    /// connected to (`InvalidOid` before `InitPostgres` selects one).
    pub fn my_database_id() -> types_core::primitive::Oid
);

seam_core::seam!(
    /// `MyDatabaseTableSpace` (globals.c): the default tablespace of the
    /// connected database.
    pub fn my_database_tablespace() -> types_core::primitive::Oid
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
