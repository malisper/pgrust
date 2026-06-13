//! Seam declarations for the logical-replication apply-worker subsystem
//! (`replication/logical/worker.c` and `launcher.c`), as consumed by the
//! parallel-apply coordinator (`applyparallelworker.c`).
//!
//! These are the worker/launcher-owned externals the parallel-apply coordinator
//! reaches across a dependency cycle: the apply-worker / subscription identity
//! and state (`MyLogicalRepWorker` / `MySubscription` fields, GUCs read through
//! the worker), the change/spool dispatch, the apply error-context stack, the
//! stream-file machinery, the
//! parallel-apply DSM segment + `shm_mq` glue, and the `ParallelApplyWorkerShared`
//! header (which lives in the DSM segment, shared between leader and worker, and
//! is reached through opaque `u64` handles the runtime resolves against the
//! segment it owns).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Signatures mirror the C failure surface
//! (`PgResult` where the C path can `ereport(ERROR)`; bare values otherwise).
#![allow(non_snake_case)]

use types_applyparallel::{DsmSetupResult, FileSet, ShmMqReceived, ShmMqResult};
use types_core::{Oid, TransactionId, XLogRecPtr};
use types_error::PgResult;

// --- apply-worker / subscription identity + state -------------------------
// `am_leader_apply_worker()` (worker.c): leader apply worker?
// (`MyLogicalRepWorker->type == WORKERTYPE_APPLY`). Modeled fallible to mirror
// the failure surface of the inline accessors it uses (the owner's authoritative
// contract on main).
seam_core::seam!(pub fn am_leader_apply_worker() -> PgResult<bool>);
seam_core::seam!(pub fn am_parallel_apply_worker() -> bool);
seam_core::seam!(pub fn my_worker_parallel_apply() -> bool);
seam_core::seam!(pub fn my_subscription_skiplsn() -> XLogRecPtr);
seam_core::seam!(pub fn my_subscription_oid() -> Oid);
seam_core::seam!(pub fn my_subscription_name() -> alloc::string::String);
seam_core::seam!(pub fn my_worker_dbid() -> Oid);
seam_core::seam!(pub fn my_worker_userid() -> Oid);
seam_core::seam!(pub fn my_worker_subid() -> Oid);
seam_core::seam!(pub fn my_worker_leader_pid() -> i32);
seam_core::seam!(pub fn my_worker_generation() -> u16);
seam_core::seam!(pub fn maybe_reread_subscription() -> PgResult<()>);
seam_core::seam!(pub fn max_parallel_apply_workers_per_subscription() -> i32);
seam_core::seam!(pub fn debug_streaming_is_immediate() -> bool);

extern crate alloc;

// --- change / spool dispatch + apply error context ------------------------
seam_core::seam!(pub fn apply_dispatch(msg: &[u8]) -> PgResult<()>);
seam_core::seam!(pub fn apply_spooled_messages(lsn: XLogRecPtr) -> PgResult<()>);
seam_core::seam!(pub fn push_apply_error_callback() -> PgResult<()>);
seam_core::seam!(pub fn pop_apply_error_callback());
seam_core::seam!(pub fn restore_apply_error_context_stack());
seam_core::seam!(pub fn stream_start_internal(xid: TransactionId, first_segment: bool) -> PgResult<()>);
seam_core::seam!(pub fn stream_cleanup_files(subid: Oid, xid: TransactionId) -> PgResult<()>);
seam_core::seam!(pub fn store_flush_position(remote_lsn: XLogRecPtr, local_lsn: XLogRecPtr) -> PgResult<()>);

// --- launcher: parallel-apply worker launch / stop ------------------------
seam_core::seam!(pub fn logicalrep_worker_launch_parallel_apply(
    dbid: Oid,
    subid: Oid,
    subname: &str,
    userid: Oid,
    subworker_dsm: u32,
) -> PgResult<bool>);
// The leader reads `winfo->shared->{generation,slot_no}` (now an in-crate
// header) and passes them in; the launcher-owned stop sequence (LWLock +
// generation/proc check + SIGUSR2) lives in launcher.c.
seam_core::seam!(pub fn logicalrep_pa_worker_stop(generation: u16, slot_no: i32) -> PgResult<()>);

// --- parallel-apply DSM segment + shm_mq glue -----------------------------
// The DSM segment, `shm_toc`, and the two `shm_mq`s are owned by the DSM/shm_mq
// machinery and the worker runtime; the coordinator carries them as opaque
// `u64`/`u32` handles the runtime resolves. (The `ParallelApplyWorkerShared`
// header is *not* among them — it is owned and created in-crate by the
// coordinator, which owns `MyParallelShared`.)
//
// `setup_dsm` builds the segment + queues and registers the in-crate shared
// header (created by the coordinator and handed in as `shared_token`) into the
// segment's TOC so the worker can re-discover it on attach.
seam_core::seam!(pub fn setup_dsm(winfo_handle: u64, shared_token: u64) -> PgResult<core::option::Option<DsmSetupResult>>);
seam_core::seam!(pub fn dsm_segment_handle(dsm_seg: u64) -> PgResult<u32>);
seam_core::seam!(pub fn dsm_detach_winfo(dsm_seg: u64) -> PgResult<()>);
seam_core::seam!(pub fn dsm_detach_handle(handle: u32) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_detach_data(mq_handle: u64) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_detach_error(error_mq_handle: u64) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_send_data(mq_handle: u64, data: &[u8]) -> PgResult<ShmMqResult>);
seam_core::seam!(pub fn shm_mq_receive_main() -> PgResult<ShmMqReceived>);
seam_core::seam!(pub fn shm_mq_receive_error(error_mq_handle: u64) -> PgResult<ShmMqReceived>);
// Performs the worker-side attach phases (signal setup, dsm_attach, toc
// lookups, queue attach, slot attach, before_shmem_exit, error-queue redirect,
// InitializeLogRepWorker, replication-origin setup, syscache callback) and
// returns the in-crate `shared_token` it recovered from the segment's TOC so
// the coordinator can bind `MyParallelShared`.
seam_core::seam!(pub fn worker_attach_dsm(worker_slot: i32) -> PgResult<u64>);

// --- worker-owned source the coordinator's shared header needs ------------
// `MyLogicalRepWorker->stream_fileset` — the leader's serialized-changes
// fileset, copied into `ParallelApplyWorkerShared.fileset` by the in-crate
// `pa_set_fileset_state` FS_SERIALIZE_DONE branch. `None` mirrors the C
// `Assert(MyLogicalRepWorker->stream_fileset)` failing.
seam_core::seam!(pub fn my_worker_stream_fileset() -> core::option::Option<FileSet>);

// --- per-message hold-pending-messages memory context (HandleParallelApply
//     Messages' transient context) ---------------------------------------
seam_core::seam!(pub fn enter_hpam_context() -> PgResult<()>);
seam_core::seam!(pub fn leave_hpam_context());

// --- additional worker.c-owned externals (from current main's owner contract)
seam_core::seam!(
    /// `AtEOXact_LogicalRepWorkers(isCommit)`.
    pub fn at_eoxact_logical_rep_workers(is_commit: bool)
);

seam_core::seam!(
    /// `LogRepWorkerWalRcvConn != NULL` (worker.c global): does this worker
    /// currently hold a walreceiver connection to the remote side?
    pub fn have_walrcv_conn() -> bool
);

seam_core::seam!(
    /// `walrcv_disconnect(LogRepWorkerWalRcvConn)` (walreceiver dispatch via the
    /// worker's connection global): gracefully disconnect from the remote side.
    /// Can `ereport(ERROR)` on a protocol/libpq failure, carried on `Err`.
    pub fn walrcv_disconnect() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MyLogicalRepWorker->stream_fileset != NULL` (worker.c): is a streaming
    /// transaction fileset currently allocated for this worker?
    pub fn have_stream_fileset() -> bool
);

seam_core::seam!(
    /// `FileSetDeleteAll(MyLogicalRepWorker->stream_fileset)`: delete the
    /// streaming-transaction fileset and all its buffiles. Can `ereport` on a
    /// filesystem error, carried on `Err`.
    pub fn fileset_delete_all() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializingApplyWorker` (worker.c global): true while an apply worker
    /// is still initializing; gates the session-level `LockReleaseAll` on exit.
    pub fn initializing_apply_worker() -> bool
);
