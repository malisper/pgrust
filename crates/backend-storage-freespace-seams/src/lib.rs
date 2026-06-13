//! Seam declarations for the `backend-storage-freespace` unit
//! (`storage/freespace/indexfsm.c`): the free-space map for indexes.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::BlockNumber;
use types_error::PgResult;
use types_rel::Relation;

seam_core::seam!(
    /// `RecordFreeIndexPage(rel, blkno)` (indexfsm.c): mark an index page as
    /// free in the FSM. `Err` carries the FSM write ereports.
    pub fn record_free_index_page<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `IndexFreeSpaceMapVacuum(rel)` (indexfsm.c): force the upper FSM levels
    /// up to date so searchers find the freed pages. `Err` carries the FSM
    /// write ereports.
    pub fn index_free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()>
);
