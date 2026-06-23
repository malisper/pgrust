//! Seam declarations for the heap AM's *physical-tuple-modification* table-AM
//! callbacks (`access/heap/heapam_handler.c`):
//! `heapam_tuple_insert` / `heapam_tuple_delete` / `heapam_tuple_update` /
//! `heapam_tuple_lock`. These are the vtable fields the
//! `backend-access-heap-heapam-handler-core` crate populates by `::call`ing
//! through here, so the heapam-handler **core** stage (scan + fetch + toast +
//! filelocator) can assemble and install the complete `TableAmRoutine` vtable
//! without yet porting the DML marshalling.
//!
//! Each callback in C wraps the heap modify core (`heap_insert` /
//! `heap_delete` / `heap_update` / `heap_lock_tuple`, all already ported) plus
//! the slot↔tuple bridge (`ExecFetchSlotHeapTuple`) and, for `tuple_lock`, the
//! `FIND_LAST_VERSION` update-chain follow loop. That marshalling layer is the
//! `backend-access-heap-heapam-handler-dml` owner; until it lands a call panics
//! loudly. The signatures mirror the `TableAmRoutine` vtable fields exactly so
//! the core's vtable assignment is a direct `::call` thunk.

use mcx::Mcx;
use types_core::xact::CommandId;
use types_error::PgResult;
use ::nodes::tuptable::SlotData;
use rel::Relation;
use types_tableam::tableam::{
    BulkInsertStateData, LockTupleMode, LockWaitPolicy, Snapshot, TM_FailureData, TM_Result,
    TU_UpdateIndexes,
};
use types_storage::RelFileLocator;
use snapshot::SnapshotData;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `heapam_relation_set_new_filelocator(rel, newrlocator, persistence,
    /// &freezeXid, &minmulti)` (heapam_handler.c): `*freezeXid = RecentXmin`,
    /// `*minmulti = GetOldestMultiXactId()`, `RelationCreateStorage(*newrlocator,
    /// persistence, true)`, and (for an unlogged rel) create + WAL-log the INIT
    /// fork, then `smgrclose`. The storage-creation leg (RelationCreateStorage
    /// returning a transient `SMgrRelation` plus the INIT-fork `smgrcreate` /
    /// `log_smgrcreate`) is the DDL/storage owner's; this seam returns the
    /// AM-chosen `(relfrozenxid, relminmxid)` the relcache stores in pg_class.
    /// Owned by `backend-access-heap-heapam-handler-dml`.
    pub fn heapam_relation_set_new_filelocator<'mcx>(
        rel: &Relation<'mcx>,
        newrlocator: &RelFileLocator,
        persistence: i8,
    ) -> PgResult<(u32, u32)>
);

seam_core::seam!(
    /// `heapam_tuple_insert(rel, slot, cid, options, bistate)`
    /// (heapam_handler.c): `ExecFetchSlotHeapTuple(slot, true, &shouldFree)`,
    /// stamp `tts_tableOid`/`t_tableOid`, `heap_insert(...)`, copy `t_self`
    /// back into `slot->tts_tid`. Owned by `backend-access-heap-heapam-handler-
    /// dml`.
    pub fn heapam_tuple_insert<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heapam_tuple_insert_speculative(rel, slot, cid, options, bistate,
    /// specToken)` (heapam_handler.c): `ExecFetchSlotHeapTuple(slot, true,
    /// &shouldFree)`, `options |= HEAP_INSERT_SPECULATIVE`, stamp
    /// `tts_tableOid`/`t_tableOid`, `HeapTupleHeaderSetSpeculativeToken(
    /// tuple->t_data, specToken)`, `heap_insert(...)`, copy `t_self` back into
    /// `slot->tts_tid`. Owned by `backend-access-heap-heapam-handler-dml`.
    #[allow(clippy::too_many_arguments)]
    pub fn heapam_tuple_insert_speculative<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
        spec_token: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heapam_tuple_complete_speculative(rel, slot, specToken, succeeded)`
    /// (heapam_handler.c): `ExecFetchSlotHeapTuple(slot, true, &shouldFree)`,
    /// then `heap_finish_speculative(rel, &slot->tts_tid)` when `succeeded`
    /// else `heap_abort_speculative(rel, &slot->tts_tid)`. Owned by
    /// `backend-access-heap-heapam-handler-dml`.
    pub fn heapam_tuple_complete_speculative<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        spec_token: u32,
        succeeded: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heapam_multi_insert(rel, slots, nslots, cid, options, bistate)`
    /// (heapam_handler.c): fetch each slot's heap tuple
    /// (`ExecFetchSlotHeapTuple`), stamp `tts_tableOid`/`t_tableOid`,
    /// `heap_multi_insert(...)`, copy each `t_self` back into the originating
    /// slot's `tts_tid`. Owned by `backend-access-heap-heapam-handler-dml`.
    pub fn heapam_multi_insert<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        slots: &mut [&mut SlotData<'mcx>],
        cid: CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heapam_tuple_delete(rel, tid, cid, snapshot, crosscheck, wait, tmfd,
    /// changingPart)` (heapam_handler.c): forwards to `heap_delete`. Owned by
    /// `backend-access-heap-heapam-handler-dml`.
    #[allow(clippy::too_many_arguments)]
    pub fn heapam_tuple_delete<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        cid: CommandId,
        snapshot: &Snapshot,
        crosscheck: &Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `heapam_tuple_update(rel, otid, slot, cid, snapshot, crosscheck, wait,
    /// tmfd, lockmode, update_indexes)` (heapam_handler.c):
    /// `ExecFetchSlotHeapTuple`, `heap_update`, copy `t_self` back, the
    /// HOT/index-update assertions. Owned by `backend-access-heap-heapam-handler-
    /// dml`.
    #[allow(clippy::too_many_arguments)]
    pub fn heapam_tuple_update<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        otid: &ItemPointerData,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        snapshot: &Snapshot,
        crosscheck: &Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut LockTupleMode,
        update_indexes: &mut TU_UpdateIndexes,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `heapam_tuple_lock(rel, tid, snapshot, slot, cid, mode, wait_policy,
    /// flags, tmfd)` (heapam_handler.c): `heap_lock_tuple` plus the
    /// `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` update-chain follow loop
    /// (`heap_fetch` under a dirty snapshot, `XactLockTableWait`), and the
    /// `ExecStorePinnedBufferHeapTuple` of the locked tuple into `slot`. Owned
    /// by `backend-access-heap-heapam-handler-dml`.
    #[allow(clippy::too_many_arguments)]
    pub fn heapam_tuple_lock<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        snapshot: &Snapshot,
        slot: &mut SlotData<'mcx>,
        cid: CommandId,
        mode: LockTupleMode,
        wait_policy: LockWaitPolicy,
        flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `heapam_tuple_satisfies_snapshot(rel, slot, snapshot)`
    /// (heapam_handler.c): the heap AM's `tuple_satisfies_snapshot` table-AM
    /// callback. `slot` is a `BufferHeapTupleTableSlot` holding a pinned heap
    /// tuple; the provider takes a SHARE lock on the slot's buffer, calls
    /// `HeapTupleSatisfiesVisibility` against `snapshot`, then drops the lock.
    /// Used by `systable_recheck_tuple` (genam) under a fresh catalog snapshot.
    /// Owned by `backend-access-heap-heapam-handler-dml`.
    pub fn heapam_tuple_satisfies_snapshot<'mcx>(
        rel: &Relation<'mcx>,
        slot: &mut SlotData<'mcx>,
        snapshot: &mut SnapshotData,
    ) -> PgResult<bool>
);
