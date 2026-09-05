// indexing.c, insert/update/delete lanes.
#![allow(non_snake_case)]

use heaptuple::HeapTuple;
use mcx::Mcx;
use types_error::PgResult;
use types_rel::Relation;
use types_tuple::{HeapTupleData, ItemPointerData};

pub type CatalogIndexState<'mcx> = execindexing::ResultRelIndexState<'mcx>;

// MAX_CATALOG_MULTI_INSERT_BYTES (indexing.h).
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

pub fn CatalogOpenIndexes<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
) -> PgResult<CatalogIndexState<'mcx>> {
    execindexing::ExecOpenIndices(mcx, heap_rel, false)
}

pub fn CatalogCloseIndexes(state: CatalogIndexState<'_>) -> PgResult<()> {
    execindexing::ExecCloseIndices(state)
}

fn CatalogIndexInsert<'mcx>(
    mcx: Mcx<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &HeapTupleData<'mcx>,
) -> PgResult<()> {
    // Fresh inserts are never heap-only (HOT arm rides with CatalogTupleUpdate).
    debug_assert!(!tup.is_heap_only());
    if indstate.num_indices() == 0 {
        return Ok(());
    }
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        types_slot::TupleSlotKind::HeapTuple,
        Some(heap_rel.rd_att.clone()),
    );
    // SAFETY: aliases tup's image, which outlives the slot (dropped below).
    let view = unsafe {
        HeapTupleData::from_raw_parts(tup.header_ptr(), tup.t_len, tup.t_self, tup.t_tableOid)
    };
    exectuples::exec_store_heap_tuple(&mut slot, mcx, view);
    // System catalogs never carry expression/partial indexes (CatalogIndexInsert
    // asserts likewise), so eval never allocates — mcx stands in for the
    // per-tuple context.
    for ii in indstate.infos.iter() {
        assert!(
            ii.ii_Expressions.is_nil() && ii.ii_Predicate.is_nil(),
            "system catalog with expression/partial index"
        );
    }
    execindexing::ExecInsertIndexTuples(mcx, mcx, indstate, heap_rel, &mut slot, false, None, &[], false)?;
    exectuples::exec_clear_tuple(&mut slot, mcx);
    Ok(())
}

/// Inserts one formed catalog tuple, wherever this relation's rows live.
///
/// The catalog path calls heap directly rather than dispatching on the access
/// method, which is correct while catalogs are heap files and silently wrong
/// once they are objkv rows: heap would reach the storage manager for a
/// relation that has no file.
/// Refuses a catalog write from a process that flipped while running.
///
/// It would go to the local file, which the bucket has already replaced: the
/// catalogs would live half in each and the next machine would be quietly
/// missing whatever was written here.
fn refuse_if_flip_pending(heap_rel: &Relation<'_>) -> PgResult<()> {
    if catalog::IsCatalogRelation(heap_rel) && objkv_marker::flipped_needs_restart() {
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
                "the catalogs moved to the bucket while this server was running".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail(
                "This process still has its pre-flip view, so the row would be \
                 written to a local file the bucket has replaced."
                    .to_string(),
            )
            .with_hint("Restart the server.".to_string()),
        ));
    }
    Ok(())
}

fn is_objkv_catalog(heap_rel: &Relation<'_>) -> bool {
    tableam_vocab::is_objkv_relam(heap_rel.rd_rel.relam)
        && tableam_seams::objkv_catalog_tuple_insert::is_installed()
}

/// The row as it stands, before the write that is about to replace it.
///
/// Invalidation is named after the keys that stop being true, and those belong
/// to the old image: invalidating the new one leaves a catcache entry under
/// the old key answering with a row that no longer has it. `simple_heap_update`
/// and `simple_heap_delete` read it from the buffer they hold; here it is a
/// fetch, into a context of its own because the callers do not all have one.
fn objkv_old_tuple<'a, 'mcx>(
    mcx: Mcx<'a>,
    heap_rel: &Relation<'mcx>,
    tid: &ItemPointerData,
) -> PgResult<Option<HeapTuple<'a>>> {
    let Some(image) = tableam_seams::objkv_catalog_fetch_tuple::call(heap_rel.alias(), *tid)?
    else {
        return Ok(None);
    };
    let mut tup = HeapTuple::alloc_zeroed(mcx, image.len())?;
    tup.image_mut().copy_from_slice(&image);
    tup.as_tuple_mut().t_self = *tid;
    tup.as_tuple_mut().t_tableOid = heap_rel.rd_id;
    Ok(Some(tup))
}

fn insert_formed_tuple<'mcx>(
    heap_rel: &Relation<'mcx>,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    if is_objkv_catalog(heap_rel) {
        tableam_seams::objkv_catalog_tuple_insert::call(heap_rel.alias(), tup.as_tuple_mut())?;
        // heap_insert registers this on the way past, and bypassing heap
        // bypassed it: without the message the catcache keeps the answer it
        // had before the row existed, so the transaction that just created a
        // relation cannot find it by name.
        return inval::invalidate::CacheInvalidateHeapTuple(heap_rel, tup.as_tuple(), None);
    }
    heapam::simple_heap_insert(heap_rel.data_rc(), tup.as_tuple_mut())
}

pub fn CatalogTupleInsert<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;
    insert_formed_tuple(heap_rel, tup)?;
    CatalogIndexInsert(mcx, &mut indstate, heap_rel, tup.as_tuple())?;
    CatalogCloseIndexes(indstate)
}

/// The objkv half of an in-place catalog update: write the row, then say what
/// the old image stopped being. Shared so `CatalogTupleUpdateWithInfo` cannot
/// drift from it -- `pg_largeobject` is an objkv relation and reaches this
/// through that entry point.
fn objkv_update_formed_tuple<'mcx>(
    heap_rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    // Its own context: the old image is read, handed to invalidation, and
    // done with. The callers do not all have a per-tuple context to lend.
    let cx = mcx::MemoryContext::new("objkv catalog update: old image");
    let old = objkv_old_tuple(cx.mcx(), heap_rel, otid)?;
    tableam_seams::objkv_catalog_tuple_update::call(heap_rel.alias(), *otid, tup.as_tuple_mut())?;
    match &old {
        Some(o) => {
            inval::invalidate::CacheInvalidateHeapTuple(heap_rel, o.as_tuple(), Some(tup.as_tuple()))
        }
        // Nothing there to stop being true; the new row still needs announcing.
        None => inval::invalidate::CacheInvalidateHeapTuple(heap_rel, tup.as_tuple(), None),
    }
}

pub fn CatalogTupleUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_All;
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;
    if is_objkv_catalog(heap_rel) {
        objkv_update_formed_tuple(heap_rel, otid, tup)?;
    } else {
        heapam::simple_heap_update(
            heap_rel.data_rc(),
            otid,
            tup.as_tuple_mut(),
            &mut update_indexes,
        )?;
    }
    match update_indexes {
        tableam_vocab::TU_UpdateIndexes::TU_All => {
            CatalogIndexInsert(mcx, &mut indstate, heap_rel, tup.as_tuple())?
        }
        tableam_vocab::TU_UpdateIndexes::TU_None => {}
        tableam_vocab::TU_UpdateIndexes::TU_Summarizing => panic!(
            "CatalogIndexInsert (indexing.c): TU_Summarizing on a catalog index"
        ),
    }
    CatalogCloseIndexes(indstate)
}

pub fn CatalogTupleDelete(heap_rel: &Relation<'_>, tid: &ItemPointerData) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    if is_objkv_catalog(heap_rel) {
        // simple_heap_delete sends this on the way past; bypassing heap
        // bypassed it, and the catcache went on answering from the row.
        let cx = mcx::MemoryContext::new("objkv catalog delete: old image");
        let old = objkv_old_tuple(cx.mcx(), heap_rel, tid)?;
        tableam_seams::objkv_catalog_tuple_delete::call(heap_rel.alias(), *tid)?;
        if let Some(o) = &old {
            inval::invalidate::CacheInvalidateHeapTuple(heap_rel, o.as_tuple(), None)?;
        }
        return Ok(());
    }
    heapam::simple_heap_delete(heap_rel.data_rc(), tid)
}

pub fn CatalogTuplesMultiInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tuples: std::vec::Vec<HeapTuple<'mcx>>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    if tuples.is_empty() {
        return Ok(());
    }
    // std Vec: SlotData owns droppy state (no-drop arena rule).
    let mut slots: std::vec::Vec<types_slot::SlotData<'mcx>> =
        std::vec::Vec::with_capacity(tuples.len());
    for tup in tuples {
        let mut slot = exectuples::make_tuple_table_slot(
            mcx,
            types_slot::TupleSlotKind::HeapTuple,
            Some(heap_rel.rd_att.clone()),
        );
        exectuples::exec_store_heap_tuple_owned(&mut slot, mcx, tup);
        slots.push(slot);
    }
    // objkv has no batching to gain from: a transaction's writes are already
    // one object, so a row at a time reaches the bucket in exactly one PUT.
    if is_objkv_catalog(heap_rel) {
        for slot in slots.iter_mut() {
            let types_slot::SlotData::Heap(h) = slot else { unreachable!() };
            let tup = h.tuple.as_mut().expect("multi-insert slot holds a tuple");
            tableam_seams::objkv_catalog_tuple_insert::call(heap_rel.alias(), tup)?;
            inval::invalidate::CacheInvalidateHeapTuple(heap_rel, tup, None)?;
        }
    } else {
        let cid = xact_seams::get_current_command_id::call(true)?;
        let mut refs: std::vec::Vec<&mut types_slot::SlotData<'mcx>> = slots.iter_mut().collect();
        heapam::heap_multi_insert(mcx, heap_rel, &mut refs, cid, 0, None)?;
    }
    for slot in slots.iter() {
        let types_slot::SlotData::Heap(h) = slot else {
            unreachable!()
        };
        let tup = h.tuple.as_ref().expect("multi-insert slot holds a tuple");
        CatalogIndexInsert(mcx, indstate, heap_rel, tup)?;
    }
    Ok(())
}

pub fn CatalogTupleInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut HeapTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    insert_formed_tuple(heap_rel, tup)?;
    CatalogIndexInsert(mcx, indstate, heap_rel, tup.as_tuple())
}

pub fn CatalogTupleUpdateWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    tup: &mut HeapTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    refuse_if_flip_pending(heap_rel)?;
    let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_All;
    if is_objkv_catalog(heap_rel) {
        // `pg_largeobject` is lifted, and `inv_write`/`inv_truncate` update it
        // through here. simple_heap_update would read a buffer for a relation
        // with no file.
        objkv_update_formed_tuple(heap_rel, otid, tup)?;
    } else {
        heapam::simple_heap_update(
            heap_rel.data_rc(),
            otid,
            tup.as_tuple_mut(),
            &mut update_indexes,
        )?;
    }
    match update_indexes {
        tableam_vocab::TU_UpdateIndexes::TU_All => {
            CatalogIndexInsert(mcx, indstate, heap_rel, tup.as_tuple())
        }
        tableam_vocab::TU_UpdateIndexes::TU_None => Ok(()),
        tableam_vocab::TU_UpdateIndexes::TU_Summarizing => panic!(
            "CatalogIndexInsert (indexing.c): TU_Summarizing on a catalog index"
        ),
    }
}
