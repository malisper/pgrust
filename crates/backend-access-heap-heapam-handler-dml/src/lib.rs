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

    heapam::insert::heap_insert(mcx, relation, &mut tuple, cid, options, bistate)?;

    slot.base_mut().tts_tid = tuple.tuple.t_self;
    Ok(())
}

/// `heapam_multi_insert(relation, slots, ntuples, cid, options, bistate)`
/// (heapam_handler.c): `ExecFetchSlotHeapTuple` each slot, stamp the table OID,
/// `heap_multi_insert`, then copy each stored `t_self` back into the matching
/// slot's `tts_tid`.
fn heapam_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    // heaptuples = palloc(ntuples * sizeof(HeapTuple));
    // for (i = 0; i < ntuples; i++) {
    //     tuple = ExecFetchSlotHeapTuple(slots[i], true, NULL);
    //     slots[i]->tts_tableOid = RelationGetRelid(relation);
    //     tuple->t_tableOid = slots[i]->tts_tableOid;
    // }
    let mut tuples: mcx::PgVec<'mcx, FormedTuple<'mcx>> =
        mcx::vec_with_capacity_in(mcx, slots.len())?;
    for slot in slots.iter_mut() {
        let (mut tuple, _should_free) =
            slot_seam::exec_fetch_slot_heap_tuple::call(mcx, slot, true)?;
        slot.base_mut().tts_tableOid = relation.rd_id;
        tuple.tuple.t_tableOid = relation.rd_id;
        tuples.push(tuple);
    }

    let stored =
        heapam::insert::heap_multi_insert(mcx, relation, tuples, cid, options, bistate)?;

    // for (i = 0; i < ntuples; i++) slots[i]->tts_tid = heaptuples[i]->t_self;
    for (slot, tuple) in slots.iter_mut().zip(stored.iter()) {
        slot.base_mut().tts_tid = tuple.tuple.t_self;
    }
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
    sx::heapam_multi_insert::set(heapam_multi_insert);
    sx::heapam_tuple_delete::set(heapam_tuple_delete);
    sx::heapam_tuple_update::set(heapam_tuple_update);
    sx::heapam_relation_set_new_filelocator::set(heapam_relation_set_new_filelocator);
}
