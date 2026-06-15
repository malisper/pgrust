//! Seam declarations for the xlogprefetcher owner
//! (`src/backend/access/transam/xlogprefetcher.c`). ipci.c sizes
//! (`XLogPrefetchShmemSize`) and initializes (`XLogPrefetchShmemInit`) the
//! prefetch stats shared state. The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `XLogPrefetchShmemSize()` — shared-memory bytes for prefetch stats;
    /// summed by ipci.c `CalculateShmemSize`. Infallible (mirrors C `size_t`
    /// `sizeof(XLogPrefetchStats)`); the overflow `ereport` lives on the
    /// caller's `add_size`, not here.
    pub fn xlog_prefetch_shmem_size() -> types_core::Size
);

seam_core::seam!(
    /// `XLogPrefetchShmemInit()` — allocate-or-attach the prefetch stats
    /// shared state. `Err` carries the out-of-shmem `ereport(ERROR)`.
    /// Scaffolded slot.
    pub fn xlog_prefetch_shmem_init() -> types_error::PgResult<()>
);

// ===========================================================================
// The prefetcher read-record entry points consumed by
// `access/transam/xlogrecovery.c`'s `ReadRecord` retry loop. Declared here (the
// xlogprefetcher owns the `XLogPrefetcher` + its `XLogReaderState`) but NOT
// installed: the recovery crate stays `needs-decomp` and the page-read driver is
// not yet ported, so a call panics loudly until the owner lands.
// ===========================================================================

seam_core::seam!(
    /// `XLogPrefetcherBeginRead(prefetcher, RecPtr)` (xlogprefetcher.c) —
    /// position the prefetcher/reader to begin reading at `rec_ptr`.
    pub fn prefetcher_begin_read(rec_ptr: types_core::XLogRecPtr)
);

seam_core::seam!(
    /// `XLogPrefetcherReadRecord(prefetcher, &errormsg)` (xlogprefetcher.c) —
    /// read and decode the next record, returning the decoded-record handle plus
    /// the reader-state fields the `ReadRecord` loop inspects (or an error
    /// message). `record == RecordRef(0)` is end-of-WAL / no record decoded.
    pub fn prefetcher_read_record() -> types_wal::xlogrecovery_carriers::ReadRecordResult
);

seam_core::seam!(
    /// `XLogPrefetcherComputeStats(prefetcher)` (xlogprefetcher.c) — publish the
    /// prefetcher's distance/depth gauges to shared memory before the recovery
    /// driver sleeps waiting for streamed WAL.
    pub fn prefetcher_compute_stats()
);
