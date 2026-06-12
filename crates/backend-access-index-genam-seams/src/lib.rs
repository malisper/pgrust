//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c`), ordered-systable-scan slice.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Relations cross as their `Oid`; the live scan
//! state is owned by the genam runtime and crosses as a
//! [`types_scan::genam::SysScanHandle`] ticket, exactly as C threads its
//! `SysScanDesc` pointer.

seam_core::seam!(
    /// `systable_beginscan_ordered(heapRelation, indexRelation, snapshot,
    /// nkeys, key)` (genam.c): begin an index scan on a system(-like) table,
    /// ordered by the index. The `keys` slice carries `nkeys`. `Err` carries
    /// the index-scan-setup error surface (fmgr lookup of the key procedures,
    /// AM begin-scan).
    pub fn systable_beginscan_ordered(
        heap_relation: types_core::Oid,
        index_relation: types_core::Oid,
        snapshot: types_scan::snapshot::SnapshotHandle,
        keys: &[types_scan::scankey::ScanKeyData],
    ) -> types_error::PgResult<types_scan::genam::SysScanHandle>
);

seam_core::seam!(
    /// `systable_getnext_ordered(sysscan, direction)` (genam.c): the next
    /// tuple of the ordered scan, or `None` at the end. C returns a
    /// `HeapTuple` owned by the scan (valid until the next call); the owned
    /// model copies it out into `mcx`. `Err` carries the index/heap fetch
    /// error surface.
    pub fn systable_getnext_ordered<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sysscan: types_scan::genam::SysScanHandle,
        direction: types_scan::sdir::ScanDirection,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
    >
);

seam_core::seam!(
    /// `systable_endscan_ordered(sysscan)` (genam.c): finish the ordered scan
    /// and release the handle. `Err` carries the AM end-scan error surface.
    pub fn systable_endscan_ordered(
        sysscan: types_scan::genam::SysScanHandle,
    ) -> types_error::PgResult<()>
);
