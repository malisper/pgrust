//! Runtime-service seams that `access/transam/parallel.c` calls outward into the
//! many subsystems it orchestrates (DSM, shm_mq, bgworker lock-group/state,
//! latch/interrupt, the transaction/snapshot/GUC/namespace/relmapper/combocid/
//! reindex/enum/clientconninfo serializers, pgstat, libpq message parsing, the
//! memory-context machinery, and the misc backend accessors).
//!
//! NOTE (design debt, recorded in `audits/backend-access-transam-parallel.md`):
//! these declarations span ~15 distinct not-yet-ported owners. The per-owner
//! seam discipline wants each declared in its owner's `-seams` crate; until
//! those owners land they are collected here so the parallel orchestration can
//! be ported with 100% of its logic, every call panicking loudly until the owner
//! installs the real implementation. When an owner lands it reclaims its decls.

#![allow(non_snake_case)]
#![allow(unused_doc_comments)]

extern crate alloc;
use alloc::string::String;

use types_core::{pid_t, Oid, ProcNumber, Size, TimestampTz, XLogRecPtr};
use types_tuple::Datum;
use types_snapshot::SnapshotData;
use types_error::PgResult;
use types_bgworker::BackgroundWorkerHandle;
use types_parallel::{
    dsm_handle, BgwHandleStatus, DsmSegmentHandle, FixedParallelState,
    ParallelWorkerMainFn, ParsedErrorNotice, ShmMqHandleHandle,
};

// --- memory contexts (utils/mmgr) ------------------------------------------
seam_core::seam!(pub fn switch_to_top_transaction_context() -> PgResult<usize>);
seam_core::seam!(pub fn memory_context_switch_back(saved: usize) -> PgResult<()>);
// NOTE: `top_memory_context()` is no longer declared here — its real owner
// (`utils/mmgr/mcxt.c`, installed by `backend-utils-mmgr-portalmem` into
// `backend-utils-mmgr-mcxt-seams`) supplies the long-lived `Mcx<'static>` the
// DSM descriptors need; the parallel crate calls that seam directly. The
// retired `top_memory_context_alloc(size) -> usize` seam is also gone; the
// no-worker fallback allocates its private buffer directly in that context
// (family `dsm-substrate-convert`).
seam_core::seam!(pub fn pfree(ptr: usize) -> PgResult<()>);
seam_core::seam!(pub fn enter_hpm_context() -> PgResult<usize>);
seam_core::seam!(pub fn leave_hpm_context(saved: usize) -> PgResult<()>);

// --- interrupts / misc backend state ---------------------------------------
seam_core::seam!(pub fn interrupts_can_be_processed() -> bool);
seam_core::seam!(pub fn isolation_uses_xact_snapshot() -> bool);
seam_core::seam!(pub fn debug_parallel_query() -> i32);
seam_core::seam!(pub fn parallel_leader_proc_number() -> ProcNumber);
seam_core::seam!(pub fn error_context_stack() -> PgResult<usize>);
seam_core::seam!(pub fn get_current_subtransaction_id() -> PgResult<u32>);
seam_core::seam!(pub fn check_for_interrupts() -> PgResult<()>);
seam_core::seam!(pub fn hold_interrupts() -> PgResult<()>);
seam_core::seam!(pub fn resume_interrupts() -> PgResult<()>);
seam_core::seam!(pub fn set_interrupt_pending() -> PgResult<()>);

// --- DSM (storage/ipc/dsm.c) ------------------------------------------------
// NOTE: `GetSessionDsmHandle` is no longer declared here — its owner
// (`access/common/session.c`, the `backend-access-common-session` crate) has
// landed and reclaimed the decl into `backend-access-common-session-seams`,
// where it installs the real body (family `session-dsm`).
// NOTE: `dsm_create` for the leader's segment is no longer a seam — the merged
// `dsm-core` `dsm_create` is called directly (family `dsm-substrate-convert`).
// `top_memory_context()` (above) supplies the `Mcx<'static>` its descriptor
// needs. The retired `dsm_create_null_if_maxsegments` seam is gone.
// NOTE (family `worker-attach`): the worker-side `dsm_attach` and
// `dsm_segment_address` seams are retired — `ParallelWorkerMain` now calls the
// merged `dsm-core` `dsm_attach` / `dsm_segment_address` directly (mirroring the
// leader's direct `dsm_create`), holding the real `DsmSegment` guard in the
// crate's worker registry. The retired `dsm_attach -> DsmSegmentHandle` and
// `dsm_segment_address(seg) -> usize` seams are gone.
seam_core::seam!(pub fn dsm_detach(seg: DsmSegmentHandle) -> PgResult<()>);
seam_core::seam!(pub fn dsm_segment_handle(seg: DsmSegmentHandle) -> PgResult<dsm_handle>);
seam_core::seam!(pub fn dsm_detach_handle(seg: DsmSegmentHandle) -> PgResult<()>);
// `arg` is the canonical unified `Datum` (Datum-unification): the DSM-handle
// machine word the C `Datum arg` carries (`UInt32GetDatum`), in the `'static`
// `ByVal` arm — matching the `dsm-core-seams` `on_dsm_detach` convention.
seam_core::seam!(pub fn dsm_segment_from_datum(arg: Datum<'static>) -> PgResult<DsmSegmentHandle>);

// --- shm_mq (storage/ipc/shm_mq.c) -----------------------------------------
// NOTE (family `shm-mq-leader`): the leader (and worker) error-queue `shm_mq_*`
// seams are retired here — parallel.c now drives the merged `shm-mq` over real
// chunk addresses through the owner's `backend-storage-ipc-shm-mq-seams` (OPTION
// (i): the backend-private `shm_mq_handle` is an owned `PgBox<ShmMqHandle>` in
// the owner's registry, named across the seam by `ShmMqAttachHandle`). The
// retired `shm_mq_create`/`shm_mq_set_receiver_to_myproc`/`set_sender_to_myproc`/
// `get_sender`/`attach`/`detach`/`set_handle`/`get_queue`/`receive` seams are
// gone (`ShmMqHandle`/`ShmMqHandleHandle` carriers retired with them).

// --- background workers (postmaster/bgworker.c) ----------------------------
// These carry the REAL value handle `types_bgworker::BackgroundWorkerHandle`
// (`{slot, generation}`), not an opaque token: parallel.c stores the real
// `BackgroundWorkerHandle` in `pcxt->worker[i].bgwhandle` (modeled as an
// `Option`, the C `NULL`-pointer-or-handle).
//
// `register_dynamic_background_worker` assembles the `BackgroundWorker`
// registration record (memset/snprintf, `bgw_extra` = worker index,
// `bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg))`) and calls the
// owner's `RegisterDynamicBackgroundWorker`. The leader passes the resolved
// DSM-segment handle (`dsm_segment_handle(pcxt->seg)`, the `dsm_handle` machine
// name) so the bgworker owner — which cannot resolve the parallel subsystem's
// private `DsmSegmentHandle` registry — can stamp `bgw_main_arg` directly.
// Returns `None` on C `false` (out of bgworker slots), mirroring the owner's
// `RegisterDynamicBackgroundWorker`.
seam_core::seam!(pub fn register_dynamic_background_worker(seg_handle: dsm_handle, worker_index: i32) -> PgResult<Option<BackgroundWorkerHandle>>);
seam_core::seam!(pub fn get_background_worker_pid(handle: BackgroundWorkerHandle) -> PgResult<(BgwHandleStatus, pid_t)>);
seam_core::seam!(pub fn wait_for_background_worker_shutdown(handle: BackgroundWorkerHandle) -> PgResult<BgwHandleStatus>);
seam_core::seam!(pub fn terminate_background_worker(handle: BackgroundWorkerHandle) -> PgResult<()>);
// `pfree(bgwhandle)` in C; with a value handle there is nothing to free, so the
// owner's install is a no-op acknowledging the consumer dropped its copy.
seam_core::seam!(pub fn terminate_background_worker_handle_free(handle: BackgroundWorkerHandle) -> PgResult<()>);
seam_core::seam!(pub fn register_parallel_worker_shutdown(seg: DsmSegmentHandle) -> PgResult<()>);

// --- lock groups / latches (storage/lmgr/proc.c, storage/ipc/latch.c) ------
seam_core::seam!(pub fn become_lock_group_leader() -> PgResult<()>);
/// Join the lock group led by the proc in slot `leader_procno` (the leader's
/// `ProcNumber`, carried in `FixedParallelState::parallel_leader_proc_number`),
/// verifying the leader's pid as the recycle interlock. This repo identifies a
/// proc by its slot index rather than a process-local `PGPROC *`, which is
/// meaningless across the leader→DSM→worker hand-off; the real owner
/// (`BecomeLockGroupMemberByNumber`, proc.c) resolves the slot.
seam_core::seam!(pub fn become_lock_group_member(leader_procno: ProcNumber, pid: pid_t) -> PgResult<bool>);
seam_core::seam!(pub fn wait_latch(wait_event: u32) -> PgResult<i32>);
seam_core::seam!(pub fn reset_latch() -> PgResult<()>);
seam_core::seam!(pub fn set_my_latch() -> PgResult<()>);
seam_core::seam!(pub fn send_parallel_message_signal(pid: pid_t, procno: ProcNumber) -> PgResult<()>);

// --- DSM byte helpers (the parallel subsystem owns the segment buffer) ------
seam_core::seam!(pub fn write_dsm_handle(base: usize, value: dsm_handle) -> PgResult<()>);
seam_core::seam!(pub fn read_dsm_handle(base: usize) -> PgResult<dsm_handle>);
// `write_entrypoint`/`read_entrypoint` retired in family `shm-toc-address`: the
// entrypoint "library\0function\0" bytes are now written/read in place at the
// real chunk address by parallel.c's own `write_entrypoint`/`read_entrypoint`
// (strcpy/strlen), not through a seam.

// --- FixedParallelState DSM driver (cross-process spinlock is the hard core)-
seam_core::seam!(pub fn fps_init(base: usize, state: FixedParallelState) -> PgResult<()>);
seam_core::seam!(pub fn fps_read(base: usize) -> PgResult<FixedParallelState>);
seam_core::seam!(pub fn fps_reset_last_xlog_end(base: usize) -> PgResult<()>);
seam_core::seam!(pub fn fps_get_last_xlog_end(base: usize) -> PgResult<XLogRecPtr>);
seam_core::seam!(pub fn fps_report_last_rec_end(base: usize, last_xlog_end: XLogRecPtr) -> PgResult<()>);
// NOTE: `collect_fixed_parallel_state` is no longer a seam — it is the leader
// side of `InitializeParallelDSM` (parallel.c), owned by the parallel crate
// itself, which reads each session global through its owning subsystem's
// accessor seam directly. A crate does not seam-call its own logic.

// --- state estimate/serialize/restore (the dozen serializers) --------------
seam_core::seam!(pub fn estimate_library_state_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_library_state(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_library_state(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_guc_state_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_guc_state(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_guc_state(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_combocid_state_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_combocid_state(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_combocid_state(space: usize) -> PgResult<()>);
// The snapshot seams below are snapmgr.c functions. The snapshot OBJECT crosses
// as snapmgr's real owned `SnapshotData` value (Datum-unification: no opaque
// token); only the DSM chunk address (`space: usize`) — a genuine in-segment
// pointer the leader's `shm_toc_allocate` handed out, dereferenced in place like
// every other serializer here — stays a raw address. snapmgr installs these
// slots (bridging `EstimateSnapshotSpace`/`SerializeSnapshot`/`RestoreSnapshot`/
// `GetTransactionSnapshot`/`GetActiveSnapshot`/`RestoreTransactionSnapshot`/
// `PushActiveSnapshot`/`PopActiveSnapshot`), reading/writing the chunk bytes with
// its own ownership of the `SerializedSnapshotData` byte layout.
seam_core::seam!(pub fn get_transaction_snapshot() -> PgResult<SnapshotData>);
seam_core::seam!(pub fn get_active_snapshot() -> PgResult<SnapshotData>);
seam_core::seam!(pub fn estimate_snapshot_space(snapshot: &SnapshotData) -> PgResult<Size>);
seam_core::seam!(pub fn serialize_snapshot(snapshot: &SnapshotData, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_snapshot(space: usize) -> PgResult<SnapshotData>);
/// `RestoreTransactionSnapshot(snapshot, source_pgproc)` — the `source_proc`
/// argument identifies the leader whose serializable snapshot the worker
/// inherits. Carried as the leader's `ProcNumber`
/// (`FixedParallelState::parallel_leader_proc_number`); the snapmgr owner
/// resolves the slot. (Was a never-populated `PgProcHandle`.)
seam_core::seam!(pub fn restore_transaction_snapshot(snapshot: SnapshotData, source_proc: ProcNumber) -> PgResult<()>);
seam_core::seam!(pub fn push_active_snapshot(snapshot: SnapshotData) -> PgResult<()>);
seam_core::seam!(pub fn pop_active_snapshot() -> PgResult<()>);
seam_core::seam!(pub fn estimate_transaction_state_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_transaction_state(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn start_parallel_worker_transaction(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn end_parallel_worker_transaction() -> PgResult<()>);
seam_core::seam!(pub fn estimate_pending_syncs_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_pending_syncs(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_pending_syncs(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_reindex_state_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_reindex_state(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_reindex_state(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_relation_map_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_relation_map(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_relation_map(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_uncommitted_enums_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_uncommitted_enums(space: usize, len: Size) -> PgResult<()>);
seam_core::seam!(pub fn restore_uncommitted_enums(space: usize) -> PgResult<()>);
seam_core::seam!(pub fn estimate_client_connection_info_space() -> PgResult<Size>);
seam_core::seam!(pub fn serialize_client_connection_info(len: Size, space: usize) -> PgResult<()>);
seam_core::seam!(pub fn restore_client_connection_info(space: usize) -> PgResult<()>);

// --- xact WAL bookkeeping ---------------------------------------------------
seam_core::seam!(pub fn xact_last_rec_end() -> PgResult<XLogRecPtr>);
seam_core::seam!(pub fn set_xact_last_rec_end(value: XLogRecPtr) -> PgResult<()>);

// --- libpq message parsing / pgstat / notify -------------------------------
seam_core::seam!(pub fn pq_parse_errornotice(msg: &[u8]) -> PgResult<ParsedErrorNotice>);
seam_core::seam!(pub fn throw_parallel_error_data(elevel: i32, context: Option<&str>, pcxt_error_context_stack: usize) -> PgResult<()>);
seam_core::seam!(pub fn notify_my_front_end(channel: &str, payload: &str, pid: i32) -> PgResult<()>);
seam_core::seam!(pub fn pgstat_progress_incr_param(index: i32, incr: i64) -> PgResult<()>);
seam_core::seam!(pub fn parse_notification_response(msg: &[u8]) -> PgResult<(i32, String, String)>);
seam_core::seam!(pub fn parse_progress(msg: &[u8]) -> PgResult<(i32, i64)>);

// --- ParallelWorkerMain restore sequence -----------------------------------
seam_core::seam!(pub fn set_initializing_parallel_worker(value: bool) -> PgResult<()>);
seam_core::seam!(pub fn worker_install_signal_handlers() -> PgResult<()>);
seam_core::seam!(pub fn worker_number_from_bgw_extra() -> PgResult<i32>);
seam_core::seam!(pub fn worker_create_memory_context() -> PgResult<()>);
// NOTE (family `worker-attach`): the worker-side `shm_toc_attach(address)` and
// `worker_toc_lookup(toc, key, no_error)` seams are retired — `ParallelWorkerMain`
// now builds the real `ShmToc` via `ShmToc::attach(PARALLEL_MAGIC, base)` over the
// `dsm_segment_address` of the attached segment and does real in-segment chunk
// lookups (mirroring the leader's real `shm_toc` over the real base), parking the
// real toc in the crate's worker registry. Both retired seams are gone.
seam_core::seam!(pub fn set_my_fixed_parallel_state(base: usize) -> PgResult<()>);
seam_core::seam!(pub fn set_parallel_leader_proc_number(procno: ProcNumber) -> PgResult<()>);
seam_core::seam!(pub fn pq_redirect_to_shm_mq(seg: DsmSegmentHandle, mqh: ShmMqHandleHandle) -> PgResult<()>);
seam_core::seam!(pub fn pq_set_parallel_leader(pid: pid_t, procno: ProcNumber) -> PgResult<()>);
seam_core::seam!(pub fn set_parallel_start_timestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) -> PgResult<()>);
seam_core::seam!(pub fn set_authenticated_user_id(id: Oid) -> PgResult<()>);
seam_core::seam!(pub fn set_session_authorization(id: Oid, is_superuser: bool) -> PgResult<()>);
seam_core::seam!(pub fn set_current_role_id(id: Oid, is_superuser: bool) -> PgResult<()>);
seam_core::seam!(pub fn set_user_id_and_sec_context(id: Oid, sec_context: i32) -> PgResult<()>);
seam_core::seam!(pub fn set_temp_namespace_state(ns: Oid, toast_ns: Oid) -> PgResult<()>);
seam_core::seam!(pub fn background_worker_initialize_connection_by_oid(dbid: Oid, userid: Oid, flags: u32) -> PgResult<()>);
seam_core::seam!(pub fn get_database_encoding() -> PgResult<i32>);
seam_core::seam!(pub fn set_client_encoding(enc: i32) -> PgResult<i32>);
seam_core::seam!(pub fn start_transaction_command() -> PgResult<()>);
seam_core::seam!(pub fn commit_transaction_command() -> PgResult<()>);
seam_core::seam!(pub fn attach_session(handle: dsm_handle) -> PgResult<()>);
seam_core::seam!(pub fn detach_session() -> PgResult<()>);
seam_core::seam!(pub fn invalidate_system_caches() -> PgResult<()>);
seam_core::seam!(pub fn maybe_initialize_system_user() -> PgResult<()>);
seam_core::seam!(pub fn attach_serializable_xact(handle: usize) -> PgResult<()>);
seam_core::seam!(pub fn enter_parallel_mode() -> PgResult<()>);
seam_core::seam!(pub fn exit_parallel_mode() -> PgResult<()>);
seam_core::seam!(pub fn invoke_entrypoint(entrypt: ParallelWorkerMainFn, seg: DsmSegmentHandle, toc: usize) -> PgResult<()>);
seam_core::seam!(pub fn pq_put_terminate() -> PgResult<()>);
seam_core::seam!(pub fn load_external_function(libraryname: &str, funcname: &str) -> PgResult<ParallelWorkerMainFn>);
seam_core::seam!(pub fn resolve_internal_parallel_worker(funcname: &str) -> PgResult<ParallelWorkerMainFn>);
