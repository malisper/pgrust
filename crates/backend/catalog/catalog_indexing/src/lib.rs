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

pub fn CatalogTupleInsert<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;
    heapam::simple_heap_insert(heap_rel.data_rc(), tup.as_tuple_mut())?;
    CatalogIndexInsert(mcx, &mut indstate, heap_rel, tup.as_tuple())?;
    CatalogCloseIndexes(indstate)
}

pub fn CatalogTupleUpdate<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    tup: &mut HeapTuple<'mcx>,
) -> PgResult<()> {
    let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_All;
    let mut indstate = CatalogOpenIndexes(mcx, heap_rel)?;
    heapam::simple_heap_update(heap_rel.data_rc(), otid, tup.as_tuple_mut(), &mut update_indexes)?;
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
    heapam::simple_heap_delete(heap_rel.data_rc(), tid)
}

pub fn CatalogTuplesMultiInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tuples: std::vec::Vec<HeapTuple<'mcx>>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
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
    let cid = xact_seams::get_current_command_id::call(true)?;
    let mut refs: std::vec::Vec<&mut types_slot::SlotData<'mcx>> = slots.iter_mut().collect();
    heapam::heap_multi_insert(mcx, heap_rel, &mut refs, cid, 0, None)?;
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
    heapam::simple_heap_insert(heap_rel.data_rc(), tup.as_tuple_mut())?;
    CatalogIndexInsert(mcx, indstate, heap_rel, tup.as_tuple())
}

pub fn CatalogTupleUpdateWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    tup: &mut HeapTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_All;
    heapam::simple_heap_update(heap_rel.data_rc(), otid, tup.as_tuple_mut(), &mut update_indexes)?;
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
