//! Seam declarations for the `backend-storage-freespace` unit
//! (`storage/freespace/indexfsm.c`): the free-space map for indexes.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_core::primitive::BlockNumber;
use ::types_error::PgResult;
use ::rel::Relation;

seam_core::seam!(
    /// `RecordFreeIndexPage(rel, blkno)` (indexfsm.c): mark an index page as
    /// free in the FSM. `Err` carries the FSM write ereports.
    pub fn record_free_index_page<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `GetFreeIndexPage(rel)` (indexfsm.c): return the block number of a free
    /// page reclaimed from the FSM (and mark it no-longer-free), or
    /// `InvalidBlockNumber` if none is available. Used by SP-GiST/nbtree page
    /// allocation. `Err` carries the FSM read/write ereports.
    pub fn get_free_index_page<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `IndexFreeSpaceMapVacuum(rel)` (indexfsm.c): force the upper FSM levels
    /// up to date so searchers find the freed pages. `Err` carries the FSM
    /// write ereports.
    pub fn index_free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `GetPageWithFreeSpace(rel, spaceNeeded)` (freespace.c): search the FSM
    /// for a page with at least `space_needed` bytes free, returning its block
    /// number (or `InvalidBlockNumber` if none). `Err` carries the FSM read
    /// ereports.
    pub fn get_page_with_free_space<'mcx>(
        rel: &Relation<'mcx>,
        space_needed: usize,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `RecordPageWithFreeSpace(rel, heapBlk, spaceAvail)` (freespace.c):
    /// record the amount of free space on a page in the FSM. `Err` carries the
    /// FSM write ereports.
    pub fn record_page_with_free_space<'mcx>(
        rel: &Relation<'mcx>,
        heap_blk: BlockNumber,
        space_avail: usize,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RecordAndGetPageWithFreeSpace(rel, oldPage, oldSpaceAvail,
    /// spaceNeeded)` (freespace.c): record the free space of `old_page` and in
    /// the same descent find a (different) page with `space_needed` free. `Err`
    /// carries the FSM ereports.
    pub fn record_and_get_page_with_free_space<'mcx>(
        rel: &Relation<'mcx>,
        old_page: BlockNumber,
        old_space_avail: usize,
        space_needed: usize,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `FreeSpaceMapVacuumRange(rel, start, end)` (freespace.c): update the
    /// upper-level FSM pages covering the `[start, end)` block range so that
    /// searchers see the leaf updates. `Err` carries the FSM write ereports.
    pub fn free_space_map_vacuum_range<'mcx>(
        rel: &Relation<'mcx>,
        start: BlockNumber,
        end: BlockNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FreeSpaceMapVacuum(rel)` (freespace.c): update ALL upper-level FSM
    /// pages of `rel` so that searchers see every leaf update, repairing any
    /// pre-existing damage or out-of-dateness. The whole-relation form of
    /// [`free_space_map_vacuum_range`], used by BRIN's `brin_vacuum_scan` after
    /// it cleans up every index page. `Err` carries the FSM write ereports.
    pub fn free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()>
);
