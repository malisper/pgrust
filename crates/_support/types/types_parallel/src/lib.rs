//! Vocabulary owned by `access/transam/parallel.c` plus the carrier types its
//! runtime seams (`backend-access-transam-parallel-rt-seams`) name.
//!
//! `access/parallel.h` defines `FixedParallelState` (the DSM-resident fixed
//! state the leader writes and each worker reads) and the per-worker bookkeeping
//! the leader keeps in the live `ParallelContext`. The live `ParallelContext`
//! itself is named by `execparallel::ParallelContextHandle` (execParallel
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
// NOTE: the former `PgProcHandle` (`PGPROC *`) opaque handle is retired. This
// repo identifies a proc by its slot index (`ProcNumber`), not a process-local
// `PGPROC *` (which is meaningless across the leader→DSM→worker hand-off). The
// leader's identity travels in `FixedParallelState::parallel_leader_proc_number`
// and the lock-group / transaction-snapshot seams take a `ProcNumber`.

/// Marker trait for a `#[repr(C)]` struct that is sound to map as a shared
/// `&T` across OS processes after being placed in a DSM segment by the
/// typed-shared-DSM-object primitive (`shared_dsm_object` in
/// `backend-access-transam-parallel`, which re-exports this trait).
///
/// The trait is defined here, in the lowest crate that owns
/// [`DsmSegmentHandle`], so the audited per-node `repr(C)` structs that live in
/// `types-nodes` / `types-execparallel` (`ParallelHashJoinState`,
/// `ParallelTableScanDescData`, `ParallelBitmapHeapState`, …) can implement it
/// directly next to their definition (the orphan rule otherwise forbids the
/// per-node *node* crates from implementing it for a foreign struct).
///
/// # Safety
///
/// Implementing this trait is an assertion, audited per-struct, that:
/// 1. `Self` is `#[repr(C)]` and its layout matches the C struct field-for-field;
/// 2. every field the C mutates concurrently after the launch barrier is
///    interior-mutable (a `pg_atomic_*`, an in-segment `Spinlock`, a `Barrier`,
///    or a `ConditionVariable`);
/// 3. the leader's placement initializer fully initializes every field (no
///    padding-relied-on-zero, no uninit field read);
/// 4. it is sound to form a shared `&Self` over bytes that another process may
///    also hold a shared `&Self` to (no plain `&mut`-style interior mutation and
///    no non-shared `UnsafeCell` write path).
///
/// It is implemented ONLY on the audited per-node objects; per-node *node*
/// crates do NOT get to implement it for arbitrary types.
pub unsafe trait SharedDsmObject {}

/// The typed-shared-DSM-object primitive (`SharedRef` / `SharedSlice` /
/// `SharedView` + the `estimate`/`place_*`/`attach*`/`with_mut` helpers). It
/// lives here — below `types-nodes` — so the per-node `repr(C)` node structs
/// that carry a `SharedRef` field can name it without an upward dependency on
/// `backend-access-transam-parallel`. It is re-exported from
/// `transam_parallel::shared_dsm_object` so every historical
/// call site keeps compiling unchanged.
pub mod shared_dsm_object;

/// `dsm_handle` (`storage/dsm_impl.h`) — the integer name of a DSM segment.
pub type dsm_handle = u32;

/// `parallel_worker_main_type` (`access/parallel.h`) — function pointer carried
/// opaquely; the crate never calls it directly (it crosses into the entry-point
/// subsystem via `invoke_entrypoint`).
pub type ParallelWorkerMainFn = usize;

/// `BgwHandleStatus` (`postmaster/bgworker.h`) — canonically defined in
/// `types_bgworker` (the `postmaster/bgworker.h` home); re-exported here so
/// existing `types_parallel::BgwHandleStatus` paths keep working.
pub use ::types_bgworker::BgwHandleStatus;

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
    /// C `PGPROC *parallel_leader_pgproc`. A process-local pointer is
    /// meaningless across the leader→DSM→worker hand-off in this repo, so this
    /// word is never populated (stays 0); the leader's identity is carried in
    /// [`Self::parallel_leader_proc_number`], which every consumer reads. Kept
    /// as a raw machine word only for `FixedParallelState` byte-layout fidelity
    /// with the C struct.
    pub parallel_leader_pgproc: usize,
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
            parallel_leader_pgproc: 0,
            parallel_leader_pid: 0,
            parallel_leader_proc_number: INVALID_PROC_NUMBER,
            xact_ts: 0,
            stmt_ts: 0,
            serializable_xact_handle: 0,
            last_xlog_end: 0,
        }
    }
}
