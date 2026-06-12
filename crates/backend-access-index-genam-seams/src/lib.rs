//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c` systable scans), expressed as caller-shaped
//! projected catalog rows (the syscache-seams precedent).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid` (the
//! table-seams convention).

use types_catalog::backend_catalog_pg_depend::{DependIndex, DependScanKeys, DependTuple};
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `systable_beginscan(rel, DependDependerIndexId|DependReferenceIndexId,
    /// true, NULL, nkeys, key)` + the `systable_getnext` loop +
    /// `systable_endscan` over an already-open pg_depend relation: invoke
    /// `body` once per matching row (`GETSTRUCT` form + `t_self`), in scan
    /// order. `body` returning `Ok(false)` stops the scan early (the C
    /// `break`); an `Err` from `body` propagates after the owner ends the
    /// scan. The scan uses the catalog snapshot taken at beginscan, so
    /// `body` may delete/update the current row through the indexing seams
    /// without affecting which rows the scan visits — exactly the C pattern.
    /// `Err` carries the scan machinery's own `ereport(ERROR)`s as well.
    pub fn systable_scan_pg_depend(
        rel: Oid,
        index: DependIndex,
        keys: &DependScanKeys,
        body: &mut dyn FnMut(&DependTuple) -> PgResult<bool>,
    ) -> PgResult<()>
);
