//! Seam declarations for the xlogprefetcher owner
//! (`src/backend/access/transam/xlogprefetcher.c`). ipci.c sizes
//! (`XLogPrefetchShmemSize`) and initializes (`XLogPrefetchShmemInit`) the
//! prefetch stats shared state. The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `XLogPrefetchShmemSize()` — shared-memory bytes for prefetch stats;
    /// summed by ipci.c `CalculateShmemSize`. `Err` carries the
    /// `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn xlog_prefetch_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `XLogPrefetchShmemInit()` — allocate-or-attach the prefetch stats
    /// shared state. `Err` carries the out-of-shmem `ereport(ERROR)`.
    /// Scaffolded slot.
    pub fn xlog_prefetch_shmem_init() -> types_error::PgResult<()>
);
