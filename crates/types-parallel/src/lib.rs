//! Vocabulary owned by `access/transam/parallel.c` plus the carrier types its
//! runtime seams (`backend-access-transam-parallel-rt-seams`) name.
//!
//! `access/parallel.h` defines `FixedParallelState` (the DSM-resident fixed
//! state the leader writes and each worker reads) and the per-worker bookkeeping
//! the leader keeps in the live `ParallelContext`. The live `ParallelContext`
//! itself is named by `types_execparallel::ParallelContextHandle` (execParallel
//! consumes it across the seam); this crate carries the *contents* the parallel
//! subsystem maintains behind that handle.

#![no_std]
#![allow(non_camel_case_types)]

use types_core::{pid_t, Oid, ProcNumber, TimestampTz, XLogRecPtr, INVALID_PROC_NUMBER};

// ===========================================================================
// Opaque handles for the raw `*` objects parallel.c threads through other
// subsystems (dsm_segment *, shm_mq *, shm_mq_handle *, PGPROC *,
// BackgroundWorkerHandle *). Each is a Copy newtype over usize; value 0 == NULL.
// ===========================================================================

macro_rules! opaque_handle {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
        pub struct $name(pub usize);

        impl $name {
            /// The C `NULL` sentinel (handle value 0).
            pub const NULL: Self = Self(0);
            /// `ptr == NULL`.
            #[inline]
            pub fn is_null(self) -> bool {
                self.0 == 0
            }
        }
    };
}

opaque_handle!(
    /// `dsm_segment *` (`storage/dsm.h`).
    DsmSegmentHandle
);
opaque_handle!(
    /// `shm_mq *` (`storage/shm_mq.h`).
    ShmMqHandle
);
opaque_handle!(
    /// `shm_mq_handle *` (`storage/shm_mq.h`).
    ShmMqHandleHandle
);
opaque_handle!(
    /// `BackgroundWorkerHandle *` (`postmaster/bgworker.h`).
    BgwHandle
);
opaque_handle!(
    /// `PGPROC *` (`storage/proc.h`).
    PgProcHandle
);

/// `dsm_handle` (`storage/dsm_impl.h`) — the integer name of a DSM segment.
pub type dsm_handle = u32;

/// `parallel_worker_main_type` (`access/parallel.h`) — function pointer carried
/// opaquely; the crate never calls it directly (it crosses into the entry-point
/// subsystem via `invoke_entrypoint`).
pub type ParallelWorkerMainFn = usize;

/// `BgwHandleStatus` (`postmaster/bgworker.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BgwHandleStatus {
    /// `BGWH_STARTED` — worker is running.
    Started,
    /// `BGWH_NOT_YET_STARTED` — worker hasn't been started yet.
    NotYetStarted,
    /// `BGWH_STOPPED` — worker has exited.
    Stopped,
    /// `BGWH_POSTMASTER_DIED` — postmaster died; worker status unclear.
    PostmasterDied,
}

/// `shm_mq_result` (`storage/shm_mq.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmMqResult {
    /// Sent or received a message.
    Success,
    /// Not completed; retry later.
    WouldBlock,
    /// Other process has detached queue.
    Detached,
}

/// The `ErrorData` subset `ProcessParallelMessage` needs out of
/// `pq_parse_errornotice` to rethrow the worker's error/notice in the leader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedErrorNotice {
    /// `edata.elevel` (raw; the in-crate code applies the `Min(elevel, ERROR)`).
    pub elevel: i32,
    /// `edata.context` (may be absent).
    pub context: Option<alloc::string::String>,
}

extern crate alloc;

/// `FixedParallelState` (`access/transam/parallel.c`) — the fixed-size state the
/// leader writes into DSM and the worker restores. The cross-process mutex
/// (`slock_t`) and `last_xlog_end` are handled by the in-crate DSM driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedParallelState {
    pub database_id: Oid,
    pub authenticated_user_id: Oid,
    pub session_user_id: Oid,
    pub outer_user_id: Oid,
    pub current_user_id: Oid,
    pub temp_namespace_id: Oid,
    pub temp_toast_namespace_id: Oid,
    pub sec_context: i32,
    pub session_user_is_superuser: bool,
    pub role_is_superuser: bool,
    pub parallel_leader_pgproc: PgProcHandle,
    pub parallel_leader_pid: pid_t,
    pub parallel_leader_proc_number: ProcNumber,
    pub xact_ts: TimestampTz,
    pub stmt_ts: TimestampTz,
    /// `SerializableXactHandle` (an opaque pointer carried as a handle).
    pub serializable_xact_handle: usize,
    /// Maximum `XactLastRecEnd` of any worker (leader sets 0; mutex-protected).
    pub last_xlog_end: XLogRecPtr,
}

impl Default for FixedParallelState {
    fn default() -> Self {
        Self {
            database_id: 0,
            authenticated_user_id: 0,
            session_user_id: 0,
            outer_user_id: 0,
            current_user_id: 0,
            temp_namespace_id: 0,
            temp_toast_namespace_id: 0,
            sec_context: 0,
            session_user_is_superuser: false,
            role_is_superuser: false,
            parallel_leader_pgproc: PgProcHandle::NULL,
            parallel_leader_pid: 0,
            parallel_leader_proc_number: INVALID_PROC_NUMBER,
            xact_ts: 0,
            stmt_ts: 0,
            serializable_xact_handle: 0,
            last_xlog_end: 0,
        }
    }
}
