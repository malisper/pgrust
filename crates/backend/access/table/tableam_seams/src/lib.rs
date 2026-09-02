use std::rc::Rc;

use mcx::Mcx;
use tableam_vocab::VacuumParams;
use types_error::PgResult;
use types_rel::{Relation, RelationData};
use types_snapshot::SnapshotData;
use types_storage::buf::BufferAccessStrategy;
use types_tuple::{HeapTupleData, ItemPointerData};

seam_core::seam!(
    // The key columns an index holds for the row in `slot`, expressions
    // evaluated and the predicate applied: Ok(false) means a partial index
    // has no entry for this row. objkv re-derives a row's entry to retire it,
    // and the executor, which evaluates both, sits above the table AM.
    pub fn objkv_index_row_datum<'a, 'b, 'mcx>(
        mcx: Mcx<'mcx>,
        index: &'a Relation<'b>,
        slot: &'a mut types_slot::SlotData<'mcx>,
        values: &'a mut [datum::Datum],
        isnull: &'a mut [bool],
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn table_relation_vacuum<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        rel: &'a RelationData<'mcx>,
        params: &'a VacuumParams,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<()>
);

seam_core::seam!(
    // currtid_internal (tid.c): scan-open + fetch + scan-close, bundled.
    pub fn table_tid_get_latest<'mcx>(
        mcx: Mcx<'mcx>,
        rel: Relation<'mcx>,
        snapshot: Rc<SnapshotData<'static>>,
        tid: ItemPointerData,
    ) -> PgResult<ItemPointerData>
);

seam_core::seam!(
    // get_table_am_oid (amcmds.c): guc <- tableam <- amcmds is not a crate edge.
    pub fn get_table_am_oid(amname: &str, missing_ok: bool) -> PgResult<types_core::Oid>
);

seam_core::seam!(
    // The catalog write path inserts a formed tuple without going through the
    // table AM, which is wrong once catalogs are objkv rows in a bucket.
    pub fn objkv_catalog_tuple_insert<'a, 'mcx>(
        rel: Relation<'mcx>,
        tup: &'a mut HeapTupleData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // Object-id allocation in bucket mode: Postgres keeps this boundary in the
    // WAL and control file, neither of which reaches a blank machine.
    pub fn objkv_claim_oid_block(want: u32, prefetch: u32) -> PgResult<u32>
);

seam_core::seam!(
    // Update and delete, for the same reason. The update keeps its row id:
    // entries name it, and a catalog row that moved would strand them all.
    pub fn objkv_catalog_tuple_update<'a, 'mcx>(
        rel: Relation<'mcx>,
        otid: ItemPointerData,
        tup: &'a mut HeapTupleData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn objkv_catalog_tuple_delete<'mcx>(
        rel: Relation<'mcx>,
        tid: ItemPointerData,
    ) -> PgResult<()>
);

seam_core::seam!(
    // The row image as it stands before an update or delete overwrites it.
    // Cache invalidation names the keys that are about to stop being true, and
    // those are the old row's. heap reads them from the buffer it is already
    // holding; objkv has to ask the store.
    pub fn objkv_catalog_fetch_tuple<'mcx>(
        rel: Relation<'mcx>,
        tid: ItemPointerData,
    ) -> PgResult<Option<std::vec::Vec<u8>>>
);
