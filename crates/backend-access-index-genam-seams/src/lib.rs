//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c` systable scans).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as
//! `&types_rel::rel::RelationData`. Scan keys cross as the trimmed
//! `ScanKeyData` and rows as the deformed [`SysScanRow`] shape, so key
//! construction (`ScanKeyInit`) and row interpretation (`GETSTRUCT`) stay in
//! the calling unit, where they live in C.

use types_core::primitive::Oid;
use types_error::PgResult;
use types_rel::rel::RelationData;
use types_scan::backend_access_index_genam::SysScanRow;
use types_scan::scankey::ScanKeyData;

seam_core::seam!(
    /// `systable_beginscan(rel, indexId, true, NULL, nkeys, key)` + the
    /// `systable_getnext` loop + `systable_endscan` over an already-open
    /// catalog relation: invoke `body` once per matching row (the deformed
    /// columns + `t_self`), in scan order. `body` returning `Ok(false)` stops
    /// the scan early (the C `break`); an `Err` from `body` propagates after
    /// the owner ends the scan. The scan uses the catalog snapshot taken at
    /// beginscan, so `body` may delete/update the current row through the
    /// indexing seams without affecting which rows the scan visits — exactly
    /// the C pattern. `Err` carries the scan machinery's own
    /// `ereport(ERROR)`s as well.
    pub fn systable_scan(
        rel: &RelationData,
        index_id: Oid,
        keys: &[ScanKeyData],
        body: &mut dyn FnMut(&SysScanRow<'_>) -> PgResult<bool>,
    ) -> PgResult<()>
);
