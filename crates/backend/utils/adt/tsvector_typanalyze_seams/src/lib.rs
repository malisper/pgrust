//! Inward seam declaration for the `backend-utils-adt-tsvector-typanalyze` unit
//! (`tsearch/ts_typanalyze.c`).
//!
//! `ts_typanalyze` is the `typanalyze` support function for `tsvector` columns:
//! the VACUUM ANALYZE driver (`commands/analyze.c`) invokes it by OID through
//! the column type's `pg_type.typanalyze`. The live `VacAttrStats*` it receives
//! is an `internal`-typed fmgr arg that cannot cross the owned by-word Datum
//! lane, so the analyze driver reaches it through this typed seam (which takes
//! the real `&mut VacAttrStats`). The owning crate installs it from its
//! `init_seams()`; until then a call panics loudly (no silent fallback).

use types_error::PgResult;
use statistics::VacAttrStats;

seam_core::seam!(
    /// `ts_typanalyze(PG_FUNCTION_ARGS)` (ts_typanalyze.c:58): the `typanalyze`
    /// function for `tsvector` columns. As a side effect on `stats` it sets
    /// `compute_stats` (to `compute_tsvector_stats`), `minrows`, and (when
    /// negative) `attstattarget` to `default_statistics_target`. Returns
    /// `Ok(true)` (the C `PG_RETURN_BOOL(true)`).
    pub fn ts_typanalyze<'mcx>(stats: &mut VacAttrStats<'mcx>) -> PgResult<bool>
);
