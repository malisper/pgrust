//! Seam declaration for `analyze.c`'s `analyze_rel` entry point.
//!
//! `vacuum()` (commands/vacuum.c) calls `analyze_rel()` for the ANALYZE leg of
//! a VACUUM/ANALYZE command. `analyze.c` is not yet ported into this workspace,
//! so the call crosses this seam. There is only ONE seam here and it is
//! **declared, not installed** — until the owning `analyze` unit lands, a call
//! panics loudly (there is no fabricated analyze result).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::rawnodes::RangeVar;
use types_statistics::{AnalyzeAttrFetchFunc, VacAttrStats};
use types_storage::buf::BufferAccessStrategy;
use types_vacuum::vacuum::VacuumParams;

seam_core::seam!(
    /// `analyze_rel(relid, relation, params, va_cols, in_outer_xact, bstrategy)`
    /// (commands/analyze.c): collect statistics for one relation. `relation`
    /// is the parse-tree `RangeVar` (or `None` for an OID-only target);
    /// `va_cols` is the optional column-name list. The `mcx` is threaded so the
    /// owner can allocate its own working node values.
    pub fn analyze_rel<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        relation: Option<RangeVar<'mcx>>,
        params: VacuumParams,
        va_cols: Vec<String>,
        in_outer_xact: bool,
        bstrategy: BufferAccessStrategy,
    ) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// Extensions for the `backend-utils-adt-array-typanalyze` unit
// (`utils/adt/array_typanalyze.c`).
//
// `array_typanalyze` first calls `std_typanalyze(stats)` and, on the array
// path, saves the standard `compute_stats` routine to re-invoke later from
// `compute_array_stats`. Both `std_typanalyze` and the standard
// `compute_scalar_stats` callback live in the (unported) `analyze.c`, so the
// array typanalyze leaf reaches them through these two seams. The owning unit
// installs them from its `init_seams()` when it lands; until then a call panics
// loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `std_typanalyze(stats)` (commands/analyze.c): the standard typanalyze
    /// function. It looks up the column type's needed operators and, on success,
    /// fills `stats->compute_stats` (with `compute_scalar_stats` /
    /// `compute_distinct_stats` / `compute_trivial_stats`), `stats->minrows`,
    /// `stats->extra_data`, and `stats->attstattarget`, returning `true`; it
    /// returns `false` when the required operators are unavailable. The owned
    /// model mutates `stats` in place and returns the C `bool`. `Err` carries
    /// the catalog-lookup `ereport(ERROR)` surface.
    pub fn std_typanalyze<'mcx>(stats: &mut VacAttrStats<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// The standard `compute_stats` callback `std_typanalyze` installed
    /// (`compute_scalar_stats`, commands/analyze.c), re-invoked by
    /// `compute_array_stats` via `extra_data->std_compute_stats(stats,
    /// fetchfunc, samplerows, totalrows)` to produce the scalar-style stats
    /// alongside the array stats. C temporarily swaps `stats->extra_data` to the
    /// std payload around the call; here the owner resolves its own payload (the
    /// `StdAnalyzeData` it stashed during `std_typanalyze`). Fills the output
    /// fields of `stats`. `Err` carries the `ereport(ERROR)` surface.
    pub fn std_compute_stats<'mcx>(
        stats: &mut VacAttrStats<'mcx>,
        fetchfunc: AnalyzeAttrFetchFunc,
        samplerows: i32,
        totalrows: f64,
    ) -> PgResult<()>
);
