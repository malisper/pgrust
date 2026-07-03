// indexing.c, insert/update lanes. CatalogTupleDelete lands with the DDL that
// needs it.
#![allow(non_snake_case)]

use heaptuple::HeapTuple;
use mcx::Mcx;
use types_error::PgResult;
use types_rel::Relation;
use types_tuple::{HeapTupleData, ItemPointerData};

pub type CatalogIndexState<'mcx> = execindexing::ResultRelIndexState<'mcx>;

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
    execindexing::ExecInsertIndexTuples(mcx, indstate, heap_rel, &mut slot)?;
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

pub fn CatalogTupleInsertWithInfo<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    tup: &mut HeapTuple<'mcx>,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    heapam::simple_heap_insert(heap_rel.data_rc(), tup.as_tuple_mut())?;
    CatalogIndexInsert(mcx, indstate, heap_rel, tup.as_tuple())
}
