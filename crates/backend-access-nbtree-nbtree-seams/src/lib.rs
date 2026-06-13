//! Inward seam declarations for the `backend-access-nbtree-nbtree` unit
//! (`access/nbtree/nbtree.c`): the parallel-scan coordination entry points
//! the sibling nbtree modules (`nbtsearch.c`) call across the module cycle.
//!
//! `nbtree` installs these from its `init_seams()`; until a caller (the
//! nbtree-core unit) lands they are simply unused. Each mirrors the C
//! `_bt_parallel_*` function, with the `IndexScanDesc` projected to the
//! relation handle plus the btree-private scan workspace and the parallel
//! descriptor handle the parallel infrastructure hands out.

#![allow(non_snake_case)]

use types_core::primitive::BlockNumber;
use types_nbtree::BTScanOpaqueData;
use types_rel::Relation;

seam_core::seam!(
    /// `_bt_parallel_seize(scan, &next_scan_page, &last_curr_page, first)`
    /// (nbtree.c): begin advancing the parallel scan to a new page. Returns
    /// `(status, next_scan_page, last_curr_page)`.
    pub fn bt_parallel_seize<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        first: bool,
    ) -> (bool, BlockNumber, BlockNumber)
);

seam_core::seam!(
    /// `_bt_parallel_release(scan, next_scan_page, curr_page)` (nbtree.c):
    /// publish the new `btps_nextScanPage`.
    pub fn bt_parallel_release<'mcx>(
        so: &mut BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        next_scan_page: BlockNumber,
        curr_page: BlockNumber,
    )
);

seam_core::seam!(
    /// `_bt_parallel_done(scan)` (nbtree.c): mark the parallel scan complete.
    pub fn bt_parallel_done<'mcx>(
        so: &mut BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
    )
);

seam_core::seam!(
    /// `_bt_parallel_primscan_schedule(scan, curr_page)` (nbtree.c): schedule
    /// another primitive index scan.
    pub fn bt_parallel_primscan_schedule<'mcx>(
        rel: &Relation<'mcx>,
        so: &mut BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        curr_page: BlockNumber,
    )
);
