//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c`), the system-table scan facility.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The API mirrors C's iterator (`systable_beginscan*` /
//! `systable_getnext*` / `systable_endscan*`): the caller opens the catalog
//! (and, for the ordered variant, the index) itself, exactly as in C.
//! Relations cross as borrows of the caller's open
//! `types_rel::RelationData` carriers; snapshots as trimmed
//! `types_snapshot::SnapshotData`; the live scan state is the trimmed
//! `types_scan::genam::SysScanDescData`, held by a [`SysScanGuard`] so the
//! scan is closed on every early return (AGENTS.md "Locks and held
//! resources"). C returns a `HeapTuple` owned by the scan (valid until the
//! next call); the owned model copies each result tuple out into `mcx`.

use types_error::PgResult;
use types_scan::genam::SysScanDescData;

seam_core::seam!(
    /// `systable_beginscan(heapRelation, indexId, indexOK, snapshot, nkeys,
    /// key)` (genam.c): begin a scan of a system(-like) table. `index_ok`
    /// false forces a heap scan; `snapshot` `None` is the C NULL (use the
    /// catalog snapshot, registered by the owner and recorded in the
    /// descriptor for unregistration at end of scan). The `keys` slice
    /// carries `nkeys`. `Err` carries the scan-setup error surface (fmgr
    /// lookup of the key procedures, AM begin-scan).
    pub fn systable_beginscan(
        heap_relation: &types_rel::RelationData<'_>,
        index_id: types_core::primitive::Oid,
        index_ok: bool,
        snapshot: Option<&types_snapshot::SnapshotData>,
        keys: &[types_scan::scankey::ScanKeyData],
    ) -> types_error::PgResult<SysScanGuard>
);

seam_core::seam!(
    /// `systable_getnext(sysscan)` (genam.c): the next tuple of the scan,
    /// copied into `mcx`, or `None` at the end. `Err` carries the index/heap
    /// fetch error surface.
    pub fn systable_getnext<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sysscan: &mut types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
    >
);

seam_core::seam!(
    /// `systable_endscan(sysscan)` (genam.c): finish the scan, releasing
    /// the AM scan state and unregistering the descriptor's snapshot.
    /// Reached only through [`SysScanGuard`] (`end()` or `Drop`); consumers
    /// never call it directly. `Err` carries the AM end-scan error surface.
    pub fn systable_endscan(
        sysscan: types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `systable_recheck_tuple(sysscan, tup)` (genam.c): recheck visibility of
    /// the most-recently-fetched tuple under a fresh catalog snapshot,
    /// returning whether it is still live. The C `tup` argument only asserts
    /// it matches `sysscan->slot`; the recheck itself reads the scan's live
    /// slot, so the owned model passes only the scan descriptor (the caller
    /// invokes this immediately after the `systable_getnext` that produced the
    /// current row). `Err` carries the snapshot-acquisition / heap-fetch error
    /// surface as well as any concurrent-abort handling.
    pub fn systable_recheck_tuple(
        sysscan: &mut types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `systable_beginscan_ordered(heapRelation, indexRelation, snapshot,
    /// nkeys, key)` (genam.c): begin an index scan on a system(-like) table,
    /// ordered by the index. The caller has the index open (`index_open`),
    /// as in C. `snapshot` `None` is the C NULL (use the catalog snapshot).
    /// The `keys` slice carries `nkeys`. `Err` carries the index-scan-setup
    /// error surface.
    pub fn systable_beginscan_ordered(
        heap_relation: &types_rel::RelationData<'_>,
        index_relation: &types_rel::RelationData<'_>,
        snapshot: Option<&types_snapshot::SnapshotData>,
        keys: &[types_scan::scankey::ScanKeyData],
    ) -> types_error::PgResult<SysScanGuard>
);

seam_core::seam!(
    /// `systable_getnext_ordered(sysscan, direction)` (genam.c): the next
    /// tuple of the ordered scan, copied into `mcx`, or `None` at the end.
    /// `Err` carries the index/heap fetch error surface.
    pub fn systable_getnext_ordered<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sysscan: &mut types_scan::genam::SysScanDescData,
        direction: types_scan::sdir::ScanDirection,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
    >
);

seam_core::seam!(
    /// `systable_endscan_ordered(sysscan)` (genam.c): finish the ordered
    /// scan. Reached only through [`SysScanGuard`] (`end()` or `Drop`);
    /// consumers never call it directly. `Err` carries the AM end-scan
    /// error surface.
    pub fn systable_endscan_ordered(
        sysscan: types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BuildIndexValueDescription(indexRelation, values, isnull)` (genam.c):
    /// build a "(key_names) = (key_values)" description of an index entry,
    /// or `Ok(None)` when the current user lacks rights to see the key values
    /// (the C NULL). `values`/`isnull` are `FormIndexDatum` outputs (the raw
    /// index-AM input). The string is allocated in `mcx`; key out-functions
    /// can `ereport(ERROR)`, carried on `Err`.
    pub fn build_index_value_description<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_relation: &types_rel::Relation<'_>,
        values: &[types_datum::Datum],
        isnull: &[bool],
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

/// The live-scan token returned by [`systable_beginscan`] /
/// [`systable_beginscan_ordered`]: owns the `SysScanDescData`. `Drop` ends
/// the scan silently (the abort path); [`Self::end`] is the explicit
/// `systable_endscan(_ordered)` at the C call site, surfacing its error.
#[derive(Debug)]
pub struct SysScanGuard {
    desc: Option<SysScanDescData>,
    ordered: bool,
}

impl SysScanGuard {
    /// Wrap a just-begun scan (`ordered` records which begin-scan flavor
    /// created it, so release dispatches to the matching end-scan). Called
    /// by the owner's installed implementation (and test fixtures);
    /// consumers only ever receive one.
    pub fn new(desc: SysScanDescData, ordered: bool) -> Self {
        SysScanGuard {
            desc: Some(desc),
            ordered,
        }
    }

    /// The scan descriptor, as `systable_getnext*` consumes it.
    pub fn desc_mut(&mut self) -> &mut SysScanDescData {
        self.desc.as_mut().expect("SysScanGuard already ended")
    }

    /// `systable_endscan(sysscan)` / `systable_endscan_ordered(sysscan)` at
    /// the C call site, consuming the guard.
    pub fn end(mut self) -> PgResult<()> {
        let desc = self.desc.take().expect("SysScanGuard ended twice");
        if self.ordered {
            systable_endscan_ordered::call(desc)
        } else {
            systable_endscan::call(desc)
        }
    }
}

impl Drop for SysScanGuard {
    fn drop(&mut self) {
        if let Some(desc) = self.desc.take() {
            // The abort path: end silently (C reaches the equivalent
            // releases through error-recovery resource cleanup).
            let _ = if self.ordered {
                systable_endscan_ordered::call(desc)
            } else {
                systable_endscan::call(desc)
            };
        }
    }
}
