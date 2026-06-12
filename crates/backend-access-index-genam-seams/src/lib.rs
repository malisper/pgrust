//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c`), the system-table scan facility.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C exposes an iterator (`systable_beginscan` / `systable_getnext` /
//! `systable_endscan` over an open `Relation`); a cross-cycle iterator with
//! owner-held scan state cannot be expressed as a seam slot, so each scan
//! crosses as one batched call: the owner opens the catalog
//! (`table_open(rel, AccessShareLock)`), runs the full scan, deforms each
//! result tuple against the relation's descriptor, closes the relation
//! (the acquired lock persisting to end of transaction as in C), and returns
//! the deformed rows in scan order. Row values are copies in `mcx`
//! (`row[attnum - 1]` is the column's `(value, isnull)`); the consuming
//! crate's per-tuple loop logic stays in the consuming crate.

use mcx::{Mcx, PgVec};
use types_cache::ScanKeyInit;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::DeformedColumn;

/// One scanned tuple, deformed: `natts` columns in attribute-number order.
pub type DeformedRow<'mcx> = PgVec<'mcx, DeformedColumn<'mcx>>;

seam_core::seam!(
    /// `table_open(rel_oid, AccessShareLock)` + `systable_beginscan(rel,
    /// index_oid, index_ok, NULL, keys.len(), keys)` + `systable_getnext`
    /// loop + `systable_endscan` + `table_close(rel, AccessShareLock)`
    /// (genam.c), batched. `Err` carries the C `ereport(ERROR)` surface of
    /// opening/scanning the catalog plus OOM from the copies.
    pub fn systable_scan<'mcx>(
        mcx: Mcx<'mcx>,
        rel_oid: Oid,
        index_oid: Oid,
        index_ok: bool,
        keys: &[ScanKeyInit],
    ) -> PgResult<PgVec<'mcx, DeformedRow<'mcx>>>
);

seam_core::seam!(
    /// As [`systable_scan`], but the ordered variant: `index_open(index_oid,
    /// AccessShareLock)` + `systable_beginscan_ordered` +
    /// `systable_getnext_ordered(ForwardScanDirection)` loop +
    /// `systable_endscan_ordered` + `index_close` + `table_close`
    /// (genam.c), so rows come back in index order.
    pub fn systable_scan_ordered<'mcx>(
        mcx: Mcx<'mcx>,
        rel_oid: Oid,
        index_oid: Oid,
        keys: &[ScanKeyInit],
    ) -> PgResult<PgVec<'mcx, DeformedRow<'mcx>>>
);

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
