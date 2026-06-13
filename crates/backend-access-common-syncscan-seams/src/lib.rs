//! Seam declarations for the `backend-access-common-syncscan` unit
//! (`access/common/syncscan.c`): the synchronized-seqscan start-location
//! machinery (shared-memory scan-location hints).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ss_get_location(rel, relnblocks)` (syncscan.c): the current scan
    /// location for the relation (0 if no valid hint), for starting a
    /// synchronized scan. Takes the shared-memory LWLock, whose acquisition
    /// can `elog(ERROR)`, carried on `Err`.
    pub fn ss_get_location(
        rel: types_core::primitive::Oid,
        relnblocks: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);

seam_core::seam!(
    /// `ss_report_location(rel, location)` (syncscan.c): report the current
    /// scan location of the relation so other scans can synchronize. Lock
    /// acquisition is conditional in C but its lock bookkeeping can still
    /// `elog(ERROR)`, carried on `Err`.
    pub fn ss_report_location(
        rel: types_core::primitive::Oid,
        location: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SyncScanShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn sync_scan_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `SyncScanShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn sync_scan_shmem_init() -> types_error::PgResult<()>
);
