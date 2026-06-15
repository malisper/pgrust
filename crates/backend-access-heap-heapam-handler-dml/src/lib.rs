//! `access/heap/heapam_handler.c` — the physical-tuple-modification half of the
//! heap AM's `TableAmRoutine` vtable: `heapam_tuple_insert` /
//! `heapam_tuple_delete` / `heapam_tuple_update` and the storage-creation
//! callback `heapam_relation_set_new_filelocator`.
//!
//! These are the DML marshalling wrappers around the already-ported heap modify
//! core (`heap_insert` / `heap_delete` / `heap_update`,
//! `backend-access-heap-heapam`, a direct dep — this crate sits one layer above
//! heapam, so the edge is acyclic) plus the slot→tuple bridge
//! (`ExecFetchSlotHeapTuple`). The heapam-handler **core** crate populates the
//! DML vtable fields by `::call`ing through
//! `backend-access-heap-heapam-handler-dml-seams`; this crate installs those
//! seams from `init_seams()`, retiring their latent panics.
//!
//! `heapam_tuple_lock` is NOT installed here: its success path must store the
//! locked tuple into a `BufferHeapTupleTableSlot` via
//! `ExecStorePinnedBufferHeapTuple`, which consumes a `FormedTuple`, but
//! `heap_lock_tuple` returns a by-reference on-page `HeapTupleData` (header +
//! pin, no separate user-data carrier). Bridging the two requires widening
//! `HeapLockResult` to carry a `FormedTuple` — a heapam.c change (the
//! FormedTuple-carrier keystone) outside this stage. The seam stays declared
//! and allowlisted until that keystone lands.
//!
//! The storage-creation leg of `heapam_relation_set_new_filelocator`
//! (`RelationCreateStorage` returning a transient `SMgrRelation`, the unlogged
//! INIT-fork `smgrcreate` / `log_smgrcreate`, and `smgrclose`) lives entirely in
//! the unported `storage.c` owner; this crate calls it through one storage seam
//! (`relation_set_new_filelocator_storage`) that panics until that owner lands.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::xact::CommandId;
use types_error::PgResult;
use types_rel::Relation;
use types_slot::SlotData;
use types_storage::RelFileLocator;
use types_tableam::tableam::{
    BulkInsertStateData, LockTupleMode, Snapshot, TM_FailureData, TM_Result, TU_UpdateIndexes,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::{HeapTupleHeaderData, ItemPointerData};

use backend_access_heap_heapam as heapam;
use backend_access_heap_heapam::BulkInsertState as HeapBulkInsertState;
use backend_access_heap_heapam_seams::HeapUpdateResult;
use backend_executor_execTuples_seams as slot_seam;

/// `td.t_data` as `&HeapTupleHeaderData` (the header is always present on a
/// formed heap tuple).
fn header<'a, 'mcx>(td: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    td.tuple.t_data.as_ref().expect("heap tuple has no header")
}

/// `HEAP_ONLY_TUPLE` (htup_details.h) — `t_infomask2` bit marking a heap-only
/// tuple.
const HEAP_ONLY_TUPLE: u16 = types_tuple::heaptuple::HEAP_ONLY_TUPLE;

/// Bridge the tableam-vocabulary `BulkInsertStateData` to heapam's own
/// `BulkInsertState` (both are the same `access/hio.h` struct; C never copies it
/// by value, it threads a pointer). The DML vtable callers all pass `None`; the
/// bulk path proper goes through `heap_multi_insert`, not these wrappers.
fn translate_bistate(b: &mut BulkInsertStateData) -> HeapBulkInsertState {
    HeapBulkInsertState {
        strategy: b.strategy.clone(),
        current_buf: b.current_buf,
        next_free: b.next_free,
        last_free: b.last_free,
        already_extended_by: b.already_extended_by,
    }
}

// ===========================================================================
// tuple_insert
// ===========================================================================

/// `heapam_tuple_insert(relation, slot, cid, options, bistate)`
/// (heapam_handler.c): `ExecFetchSlotHeapTuple(slot, true, &shouldFree)`, stamp
/// the table OID, `heap_insert`, copy the resulting `t_self` into `tts_tid`.
fn heapam_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    let (mut tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    slot.base_mut().tts_tableOid = relation.rd_id;
    tuple.tuple.t_tableOid = relation.rd_id;

    let mut heap_bistate = bistate.map(translate_bistate);
    heapam::insert::heap_insert(mcx, relation, &mut tuple, cid, options, heap_bistate.as_mut())?;

    slot.base_mut().tts_tid = tuple.tuple.t_self;
    Ok(())
}

// ===========================================================================
// tuple_delete
// ===========================================================================

/// `heapam_tuple_delete(relation, tid, cid, snapshot, crosscheck, wait, tmfd,
/// changingPart)` (heapam_handler.c): forwards to `heap_delete`. `snapshot` is
/// unused by the heap implementation.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: CommandId,
    _snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changing_part: bool,
) -> PgResult<TM_Result> {
    heapam::delete::heap_delete(
        mcx,
        relation,
        *tid,
        cid,
        crosscheck.as_ref(),
        wait,
        tmfd,
        changing_part,
    )
}

// ===========================================================================
// tuple_update
// ===========================================================================

/// `heapam_tuple_update(relation, otid, slot, cid, snapshot, crosscheck, wait,
/// tmfd, lockmode, update_indexes)` (heapam_handler.c): `ExecFetchSlotHeapTuple`,
/// stamp the table OID, `heap_update`, copy `t_self` back, then the HOT /
/// index-update bookkeeping asserts. `snapshot` is unused by the heap
/// implementation.
#[allow(clippy::too_many_arguments)]
fn heapam_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    _snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    let (mut tuple, _should_free) = slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;

    slot.base_mut().tts_tableOid = relation.rd_id;
    tuple.tuple.t_tableOid = relation.rd_id;

    let HeapUpdateResult {
        result,
        lockmode: hu_lockmode,
        update_indexes: hu_update_indexes,
    } = heapam::update::heap_update(
        mcx,
        relation,
        *otid,
        &mut tuple,
        cid,
        crosscheck.as_ref(),
        wait,
        tmfd,
    )?;
    *lockmode = hu_lockmode;
    *update_indexes = hu_update_indexes;

    slot.base_mut().tts_tid = tuple.tuple.t_self;

    // Mirror C's output asserts on heap_update's *update_indexes.
    if result != TM_Result::TM_Ok {
        debug_assert_eq!(*update_indexes, TU_UpdateIndexes::TU_None);
        *update_indexes = TU_UpdateIndexes::TU_None;
    } else if (header(&tuple).t_infomask2 & HEAP_ONLY_TUPLE) == 0 {
        debug_assert_eq!(*update_indexes, TU_UpdateIndexes::TU_All);
    } else {
        debug_assert!(
            *update_indexes == TU_UpdateIndexes::TU_Summarizing
                || *update_indexes == TU_UpdateIndexes::TU_None
        );
    }

    Ok(result)
}

// ===========================================================================
// relation_set_new_filelocator
// ===========================================================================

/// `heapam_relation_set_new_filelocator(rel, newrlocator, persistence,
/// &freezeXid, &minmulti)` (heapam_handler.c): `*freezeXid = RecentXmin`,
/// `*minmulti = GetOldestMultiXactId()`, then the storage-creation leg
/// (`RelationCreateStorage` + unlogged INIT fork + `smgrclose`), which lives in
/// the `storage.c` owner. Returns the AM-chosen `(relfrozenxid, relminmxid)`.
fn heapam_relation_set_new_filelocator(
    _rel: &Relation<'_>,
    newrlocator: &RelFileLocator,
    persistence: i8,
) -> PgResult<(u32, u32)> {
    let freeze_xid = backend_utils_time_snapmgr::RecentXmin();
    let minmulti = backend_access_transam_multixact_seams::get_oldest_multi_xact_id::call()?;

    backend_catalog_storage_seams::relation_set_new_filelocator_storage::call(
        *newrlocator,
        persistence,
    )?;

    Ok((freeze_xid, minmulti))
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install the DML tuple-modification seams the heapam-handler core `::call`s.
/// `heapam_tuple_lock` is intentionally not installed (FormedTuple-carrier
/// keystone, see the module note); it stays allowlisted.
pub fn init_seams() {
    use backend_access_heap_heapam_handler_dml_seams as sx;
    sx::heapam_tuple_insert::set(heapam_tuple_insert);
    sx::heapam_tuple_delete::set(heapam_tuple_delete);
    sx::heapam_tuple_update::set(heapam_tuple_update);
    sx::heapam_relation_set_new_filelocator::set(heapam_relation_set_new_filelocator);
}
