//! Seam declarations for the logical-replication apply-worker subsystem
//! (`replication/logical/worker.c` and `launcher.c`), as consumed by the
//! parallel-apply coordinator (`applyparallelworker.c`).
//!
//! These are the worker/launcher-owned externals the parallel-apply coordinator
//! reaches across a dependency cycle: the apply-worker / subscription identity
//! and state (`MyLogicalRepWorker` / `MySubscription` fields, GUCs read through
//! the worker), the change/spool dispatch, the apply error-context stack, the
//! stream-file machinery, the `TopTransactionContext` subxact list, the
//! parallel-apply DSM segment + `shm_mq` glue, and the `ParallelApplyWorkerShared`
//! header (which lives in the DSM segment, shared between leader and worker, and
//! is reached through opaque `u64` handles the runtime resolves against the
//! segment it owns).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Signatures mirror the C failure surface
//! (`PgResult` where the C path can `ereport(ERROR)`; bare values otherwise).
#![allow(non_snake_case)]

use types_applyparallel::{
    DsmSetupResult, ParallelTransState, PartialFileSetState, ShmMqReceived, ShmMqResult,
};
use types_core::{Oid, TransactionId, XLogRecPtr};
use types_error::PgResult;

// --- apply-worker / subscription identity + state -------------------------
seam_core::seam!(pub fn am_leader_apply_worker() -> bool);
seam_core::seam!(pub fn am_parallel_apply_worker() -> bool);
seam_core::seam!(pub fn my_worker_parallel_apply() -> bool);
seam_core::seam!(pub fn my_subscription_skiplsn() -> XLogRecPtr);
seam_core::seam!(pub fn my_subscription_oid() -> Oid);
seam_core::seam!(pub fn my_subscription_name() -> alloc::string::String);
seam_core::seam!(pub fn my_worker_dbid() -> Oid);
seam_core::seam!(pub fn my_worker_userid() -> Oid);
seam_core::seam!(pub fn my_worker_subid() -> Oid);
seam_core::seam!(pub fn my_worker_leader_pid() -> i32);
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
seam_core::seam!(pub fn logicalrep_pa_worker_stop(shared: u64) -> PgResult<()>);

// --- subxact list (TopTransactionContext-allocated) -----------------------
seam_core::seam!(pub fn subxact_member(xid: TransactionId) -> bool);
seam_core::seam!(pub fn subxact_append(xid: TransactionId));
seam_core::seam!(pub fn subxact_reset());
seam_core::seam!(pub fn subxact_length() -> i32);
seam_core::seam!(pub fn subxact_nth(i: i32) -> TransactionId);
seam_core::seam!(pub fn subxact_truncate(n: i32));

// --- parallel-apply DSM segment + shm_mq glue -----------------------------
// The DSM segment, `shm_toc`, the two `shm_mq`s, and the in-segment
// `ParallelApplyWorkerShared` header are owned by the DSM/shm_mq machinery and
// the worker runtime; the coordinator carries them as opaque `u64`/`u32`
// handles the runtime resolves.
seam_core::seam!(pub fn setup_dsm(winfo_handle: u64) -> PgResult<core::option::Option<DsmSetupResult>>);
seam_core::seam!(pub fn dsm_segment_handle(dsm_seg: u64) -> PgResult<u32>);
seam_core::seam!(pub fn dsm_detach_winfo(dsm_seg: u64) -> PgResult<()>);
seam_core::seam!(pub fn dsm_detach_handle(handle: u32) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_detach_data(mq_handle: u64) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_detach_error(error_mq_handle: u64) -> PgResult<()>);
seam_core::seam!(pub fn shm_mq_send_data(mq_handle: u64, data: &[u8]) -> PgResult<ShmMqResult>);
seam_core::seam!(pub fn shm_mq_receive_main() -> PgResult<ShmMqReceived>);
seam_core::seam!(pub fn shm_mq_receive_error(error_mq_handle: u64) -> PgResult<ShmMqReceived>);
seam_core::seam!(pub fn worker_attach_dsm(worker_slot: i32) -> PgResult<()>);

// --- ParallelApplyWorkerShared header (in-DSM, leader<->worker shared) -----
seam_core::seam!(pub fn init_worker_shared_for_xid(shared: u64, xid: TransactionId));
seam_core::seam!(pub fn winfo_shared_xid(shared: u64) -> TransactionId);
seam_core::seam!(pub fn winfo_shared_last_commit_end(shared: u64) -> XLogRecPtr);
seam_core::seam!(pub fn my_parallel_shared_xid() -> TransactionId);
seam_core::seam!(pub fn pending_stream_count_read() -> u32);
seam_core::seam!(pub fn pending_stream_count_sub_fetch_1() -> u32);
seam_core::seam!(pub fn set_winfo_xact_state(shared: u64, xact_state: ParallelTransState));
seam_core::seam!(pub fn get_winfo_xact_state(shared: u64) -> ParallelTransState);
seam_core::seam!(pub fn set_my_xact_state(xact_state: ParallelTransState));
seam_core::seam!(pub fn set_winfo_fileset_state(shared: u64, fileset_state: PartialFileSetState) -> PgResult<()>);
seam_core::seam!(pub fn set_my_fileset_state(fileset_state: PartialFileSetState) -> PgResult<()>);
seam_core::seam!(pub fn get_my_fileset_state() -> PartialFileSetState);

// --- per-message hold-pending-messages memory context (HandleParallelApply
//     Messages' transient context) ---------------------------------------
seam_core::seam!(pub fn enter_hpam_context() -> PgResult<()>);
seam_core::seam!(pub fn leave_hpam_context());
