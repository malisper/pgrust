//! Seam declarations for the `backend-catalog-pg-enum` unit
//! (`catalog/pg_enum.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AtEOXact_Enum()` — discard the uncommitted-enum-value bookkeeping.
    pub fn at_eoxact_enum()
);

seam_core::seam!(
    /// Scan `pg_enum` for the members of `enum_type_id` (the C
    /// `load_enum_cache_data` `EnumTypIdSortOrderIndexId` scan), emitting
    /// `(enum_oid, enumsortorder)` for each in catalog order (the typcache
    /// sorts). `Err` carries the scan `ereport(ERROR)` surface.
    pub fn scan_enum_members(
        enum_type_id: Oid,
        emit: &mut dyn FnMut(Oid, f32),
    ) -> PgResult<()>
);
