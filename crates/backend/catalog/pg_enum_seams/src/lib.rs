//! Seam declarations for the `backend-catalog-pg-enum` unit
//! (`catalog/pg_enum.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use ::mcx::{Mcx, PgVec};
use ::types_catalog::pg_enum::EnumTupleData;
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `AtEOXact_Enum()` — discard the uncommitted-enum-value bookkeeping.
    pub fn at_eoxact_enum()
);

seam_core::seam!(
    /// `EnumUncommitted(enum_id)` (pg_enum.c) — is the given enum value OID in
    /// the backend-local uncommitted-enum-values set? Consumed by
    /// `check_safe_enum_use` (utils/adt/enum.c). Infallible (a hashtable
    /// membership test).
    pub fn enum_uncommitted(enum_id: Oid) -> bool
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

seam_core::seam!(
    /// Scan `pg_enum` for the members of `enum_type_id` in sort order, the
    /// `enum.c` `enum_endpoint` / `enum_range_internal` ordered scan over
    /// `EnumTypIdSortOrderIndexId` (`table_open(EnumRelationId)` +
    /// `index_open(EnumTypIdSortOrderIndexId)` +
    /// `systable_beginscan_ordered`, `ForwardScanDirection`). Each member is
    /// projected to an [`EnumTupleData`] (the `Form_pg_enum` columns enum.c
    /// reads plus the header `xmin`/`xmin_committed` `check_safe_enum_use`
    /// needs). The list is allocated in `mcx`; the scan is closed before
    /// returning. `Err` carries the scan `ereport(ERROR)` surface.
    pub fn scan_enum_typid_sorted<'mcx>(
        mcx: Mcx<'mcx>,
        enum_type_id: Oid,
    ) -> PgResult<PgVec<'mcx, EnumTupleData>>
);
