//! Seam declarations for the `backend-commands-vacuum` unit
//! (`commands/vacuum.c`): the cross-cutting VACUUM helpers other AMs call.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// The btbulkdelete `IndexBulkDeleteCallback(htup, callback_state)`: does
    /// this heap TID belong to a tuple being deleted by VACUUM? The callback
    /// and its state live in the VACUUM driver; `callback_state_handle`
    /// identifies the state. Infallible (a pure membership test).
    pub fn vacuum_tid_is_dead(
        tid: types_tuple::heaptuple::ItemPointerData,
        callback_state_handle: u64,
    ) -> bool
);

seam_core::seam!(
    /// `vacuum_delay_point(is_analyze = false)` (vacuum.c): cost-based VACUUM
    /// delay / interrupt check, called while holding no buffer lock. `Err`
    /// carries a pending `ProcessInterrupts` `ereport(ERROR)` (query cancel).
    pub fn vacuum_delay_point() -> PgResult<()>
);

seam_core::seam!(
    /// `memset(&params, 0, sizeof(VacuumParams)); vacuum_get_cutoffs(OldHeap,
    /// &params, &cutoffs)` (vacuum.c): freeze/cutoff computation for CLUSTER.
    pub fn vacuum_get_cutoffs(
        old_heap: &types_rel::Relation<'_>,
    ) -> PgResult<types_cluster::VacuumCutoffs>
);
