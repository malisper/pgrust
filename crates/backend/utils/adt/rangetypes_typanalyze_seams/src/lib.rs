//! Inward seam declarations for the `backend-utils-adt-rangetypes-typanalyze`
//! unit (`utils/adt/rangetypes_typanalyze.c`).
//!
//! `range_typanalyze` / `multirange_typanalyze` are the `typanalyze` support
//! functions the VACUUM ANALYZE driver (`commands/analyze.c`, unported) invokes
//! by OID through the type's `pg_type.typanalyze` proc. They are declared here
//! so the (currently unported) analyze driver can reach them across the
//! dependency boundary; the owning crate installs them from its `init_seams()`.
//! Until then a call panics loudly (no silent fallback).

use types_error::PgResult;
use statistics::VacAttrStats;

seam_core::seam!(
    /// `range_typanalyze(PG_FUNCTION_ARGS)` (rangetypes_typanalyze.c): the
    /// `typanalyze` function for range columns. As a side effect on `stats` it
    /// sets `compute_stats`, `extra_data` (the range typcache, threaded through
    /// the owning crate's `void *`-faithful side table) and `minrows`, and
    /// updates `attstattarget`. Returns `Ok(true)` (the C
    /// `PG_RETURN_BOOL(true)`). `Err` carries the type-cache / `getBaseType`
    /// `ereport(ERROR)`s.
    pub fn range_typanalyze<'mcx>(stats: &mut VacAttrStats<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `multirange_typanalyze(PG_FUNCTION_ARGS)` (rangetypes_typanalyze.c): the
    /// `typanalyze` function for multirange columns. Same shape as
    /// [`range_typanalyze`] but resolves the multirange typcache. Returns
    /// `Ok(true)`. `Err` carries the type-cache / `getBaseType`
    /// `ereport(ERROR)`s.
    pub fn multirange_typanalyze<'mcx>(stats: &mut VacAttrStats<'mcx>) -> PgResult<bool>
);
