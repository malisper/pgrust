//! Seam declarations for the GiST AM insert / vacuum vtable callbacks
//! (`gistinsert` / `gistbulkdelete` / `gistvacuumcleanup`).
//!
//! These are the non-scan, required `IndexAmRoutine` fn-pointer slots the GiST
//! handler (in this F2-scan crate, `backend-access-gist-core`) must populate.
//! The handler's adapters dispatch through these seams so the scan crate need
//! not depend on the unported GiST insert-entry (`gist.c` `gistinsert`) and
//! vacuum (`gistvacuum.c`) units — exactly how `backend-access-brin-scan`
//! splits scan from insert/vacuum via `backend-access-brin-insert-vacuum-seams`.
//!
//! The owning units install these from their `init_seams()` when they land
//! (the F3+ GiST insert/vacuum lanes); until then a call panics loudly
//! (mirror-PG-and-panic). The serial *scan* path never invokes them.

use mcx::Mcx;
use types_error::PgResult;
use types_rel::Relation;
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `gistinsert(r, values, isnull, ht_ctid, heapRel, checkUnique,
    /// indexUnchanged, indexInfo)` (gist.c): the AM `aminsert` entry — form a
    /// GiST index tuple for the heap row and descend the tree to place it
    /// (`gistdoinsert`). GiST never reports a unique conflict (returns `false`).
    /// `Err` carries its `ereport(ERROR)` surface.
    #[allow(clippy::too_many_arguments)]
    pub fn gistinsert<'mcx>(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap_relation: &Relation<'mcx>,
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut IndexInfo,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `gistbulkdelete(info, stats, callback, callback_state)` (gistvacuum.c):
    /// delete index entries pointing at heap tuples the vacuum callback reports
    /// as dead, during a bulk-delete pass. `callback_state` keys the
    /// `vacuum_tid_is_dead` callback. `Err` carries its `ereport(ERROR)`
    /// surface.
    pub fn gistbulkdelete<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
        callback_state: Option<u64>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `gistvacuumcleanup(info, stats)` (gistvacuum.c): final vacuum pass over a
    /// GiST index — physically remove empty pages and update statistics. `Err`
    /// carries its `ereport(ERROR)` surface.
    pub fn gistvacuumcleanup<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);
