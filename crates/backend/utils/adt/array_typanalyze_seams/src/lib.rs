//! Seam declarations for the `backend-utils-adt-array-typanalyze` unit
//! (`utils/adt/array_typanalyze.c`).
//!
//! `array_typanalyze` is the `typanalyze` support function for array columns:
//! the ANALYZE driver (`commands/analyze.c`, not yet ported) calls it via the
//! column type's `pg_type.typanalyze` to install the array `compute_stats`
//! callback. That dispatch crosses this seam. The owning unit installs it from
//! its `init_seams()`; until then a call panics loudly. (Nobody installs a
//! caller yet — analyze.c is unported — but the seam is declared and installed
//! per repo convention.)

#![allow(non_snake_case)]

seam_core::seam!(
    /// `array_typanalyze(stats)` (utils/adt/array_typanalyze.c:97): the
    /// `typanalyze` function for array columns. It calls `std_typanalyze` and,
    /// when the element type has the needed eq/cmp/hash operators, installs
    /// `compute_array_stats` as `stats->compute_stats`. Returns the C
    /// `PG_RETURN_BOOL(...)` value (`false` only when `std_typanalyze` failed).
    /// `Err` carries the catalog-lookup `ereport(ERROR)` surface.
    pub fn array_typanalyze<'mcx>(
        stats: &mut statistics::VacAttrStats<'mcx>,
    ) -> types_error::PgResult<bool>
);
