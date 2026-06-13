//! Seam declarations for the `backend-access-index-indexam` unit
//! (`access/index/indexam.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `index_open(relationId, lockmode)` (indexam.c): open an index relation
    /// by OID — `relation_open` plus the not-an-index `ereport(ERROR)` check.
    /// The consumed slice of the relcache entry is copied into `mcx`. The
    /// owner installs the handle's closer, so `index_close(rel, lockmode)` is
    /// the returned handle's [`types_rel::Relation::close`] and drop is the
    /// abort-path `index_close(rel, NoLock)`.
    pub fn index_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_rel::Relation<'mcx>>
);

// === parallel btree scan DSM coordination ==================================
//
// The `BTParallelScanDescData` lives in the DSM region the parallel
// index-scan infrastructure (indexam.c `ParallelIndexScanDesc` +
// `OffsetToPointer(parallel_scan, ps_offset_am)`) sets up; nbtree's
// `_bt_parallel_*` logic operates on it. Until that infrastructure lands the
// whole coordination is reached through these seams, which panic loudly —
// a serial scan never touches them.

seam_core::seam!(
    /// `btestimateparallelscan(rel, nkeys, norderbys)` DSA sizing
    /// (`BTParallelScanDescData` + per-key SAOP/skip-array space).
    pub fn bt_estimate_parallel_scan<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
    ) -> types_core::primitive::Size
);

seam_core::seam!(
    /// `btinitparallelscan(target)` — initialize the `BTParallelScanDescData`
    /// in the DSM area `target_handle` (LWLock + ConditionVariable + state).
    pub fn bt_init_parallel_scan(target_handle: u64)
);

seam_core::seam!(
    /// `btparallelrescan` body — reset the shared parallel state under the
    /// scan's parallel descriptor.
    pub fn bt_parallel_rescan<'mcx>(
        so: &mut types_nbtree::BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
    )
);

seam_core::seam!(
    /// `_bt_parallel_seize` DSM core: the LWLock-protected page-status state
    /// machine over `BTParallelScanDescData`. Returns
    /// `(status, next_scan_page, last_curr_page)`.
    pub fn bt_parallel_seize_dsm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        so: &mut types_nbtree::BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        first: bool,
    ) -> (bool, types_core::primitive::BlockNumber, types_core::primitive::BlockNumber)
);

seam_core::seam!(
    /// `_bt_parallel_release` DSM core: publish `btps_nextScanPage` and signal.
    pub fn bt_parallel_release_dsm<'mcx>(
        so: &mut types_nbtree::BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        next_scan_page: types_core::primitive::BlockNumber,
        curr_page: types_core::primitive::BlockNumber,
    )
);

seam_core::seam!(
    /// `_bt_parallel_done` DSM core: mark the parallel scan complete and
    /// broadcast.
    pub fn bt_parallel_done_dsm<'mcx>(
        so: &mut types_nbtree::BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
    )
);

seam_core::seam!(
    /// `_bt_parallel_primscan_schedule` DSM core: schedule another primitive
    /// scan and serialize the array keys into the shared descriptor.
    pub fn bt_parallel_primscan_schedule_dsm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        so: &mut types_nbtree::BTScanOpaqueData<'mcx>,
        parallel_handle: u64,
        curr_page: types_core::primitive::BlockNumber,
    )
);
