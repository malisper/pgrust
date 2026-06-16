//! Seam declarations for the BRIN AM insert/vacuum callbacks
//! (`brininsert` / `brininsertcleanup` / `brinbulkdelete` /
//! `brinvacuumcleanup`), the F3 slice of `src/backend/access/brin/brin.c`.
//!
//! These are the vtable slots the BRIN handler (in the F2 scan crate,
//! `backend-access-brin-scan`) populates. The handler's adapters dispatch
//! through these seams so the scan crate need not depend on the F3
//! insert/vacuum crate (which in turn depends on the scan crate for
//! `brin_build_desc`/`BrinScan`) — breaking the dependency cycle, exactly the
//! way other per-AM towers split scan from insert/vacuum.
//!
//! The owning unit (`backend-access-brin-insert-vacuum`) installs these from
//! its `init_seams()`; until then a call panics loudly. The serial scan path
//! never invokes them.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_rel::Relation;
use types_tableam::amapi::IndexUniqueCheck;
use types_tableam::index_info_carrier::IndexInfoCarrier;
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `brininsert(idxRel, values, nulls, heaptid, heapRel, checkUnique,
    /// indexUnchanged, indexInfo)` (brin.c): add a heap tuple's values to the
    /// summary of the page range that contains `heap_tid`, or leave the range
    /// un-summarized. Always returns `false` (BRIN never reports a unique
    /// conflict). The running `BrinInsertState` is cached in
    /// the C `indexInfo->ii_AmCache` (the `Opaque` field of the carried
    /// `IndexInfo`). `Err` carries its `ereport(ERROR)` surface.
    #[allow(clippy::too_many_arguments)]
    pub fn brininsert<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap_relation: &Relation<'mcx>,
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut IndexInfoCarrier<'a, 'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `brininsertcleanup(index, indexInfo)` (brin.c): release the cached
    /// `BrinInsertState` (`indexInfo->ii_AmCache`) once all inserts in the
    /// command are done. `Err` carries its `ereport(ERROR)` surface.
    pub fn brininsertcleanup<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        index_info: &mut IndexInfoCarrier<'a, 'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `brinbulkdelete(info, stats, callback, callback_state)` (brin.c): BRIN
    /// does nothing during the bulk-delete phase except allocate the stats
    /// struct on first call. `Err` carries its `ereport(ERROR)` surface.
    pub fn brinbulkdelete<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
        callback_state: Option<u64>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `brinvacuumcleanup(info, stats)` (brin.c): "vacuum" a BRIN index by
    /// summarizing ranges that are currently un-summarized, after a full
    /// physical scan that repairs any lost pages. `Err` carries its
    /// `ereport(ERROR)` surface.
    pub fn brinvacuumcleanup<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `brin_summarize_range(indexoid, heapBlk64)` (brin.c): SQL-callable —
    /// summarize the indicated page range (or all unsummarized ranges if
    /// `heap_blk64 == BRIN_ALL_BLOCKRANGES`). Returns the number of ranges
    /// summarized. `Err` carries its `ereport(ERROR)` surface.
    pub fn brin_summarize_range<'mcx>(
        mcx: Mcx<'mcx>,
        indexoid: Oid,
        heap_blk64: i64,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `brin_desummarize_range(indexoid, heapBlk64)` (brin.c): SQL-callable —
    /// mark the range containing `heap_blk64` as no longer summarized. `Err`
    /// carries its `ereport(ERROR)` surface.
    pub fn brin_desummarize_range<'mcx>(
        mcx: Mcx<'mcx>,
        indexoid: Oid,
        heap_blk64: i64,
    ) -> PgResult<()>
);
