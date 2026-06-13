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

// === parallel btree scan DSM pointer resolution ============================
//
// The `BTParallelScanDescData` lives in the DSM region the parallel
// index-scan infrastructure (indexam.c `ParallelIndexScanDesc` +
// `OffsetToPointer(parallel_scan, ps_offset_am)`) sets up. The
// `_bt_parallel_*` *logic* (the LWLock-protected page-status state machine,
// the array serialize/restore, the init/rescan field writes) lives in the
// owning `backend-access-nbtree-nbtree` crate; only the DSM-pointer
// resolution itself is foreign and reached through this seam. Until the
// parallel index-scan infrastructure lands, the resolver panics loudly — a
// serial scan never reaches it.

seam_core::seam!(
    /// `(BTParallelScanDesc) OffsetToPointer(parallel_scan, parallel_scan->ps_offset_am)`
    /// — resolve the DSM handle for a parallel index scan to the AM-specific
    /// `BTParallelScanDescData` that lives within it. Returns the raw DSM
    /// pointer exactly as the C macro does; the nbtree state machine
    /// dereferences it under the descriptor's embedded LWLock.
    pub fn bt_resolve_parallel_scan(
        parallel_handle: u64,
    ) -> *mut types_nbtree::BTParallelScanDescData
);
