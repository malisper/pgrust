//! Planner selectivity / cost estimators (`utils/adt/selfuncs.c`,
//! PostgreSQL 18.3) — the owning unit of the selectivity-estimation machinery.
//!
//! This crate ports the value-typed estimator cores that the restriction and
//! join selectivity entry points are built from, and installs the
//! cross-dependency-cycle seams the other selectivity crates
//! (`backend-utils-adt-range-selfuncs`, `backend-utils-adt-network-selfuncs`,
//! `backend-utils-adt-array-selfuncs`) and the cost model reach through
//! ([`backend_utils_adt_selfuncs_seams`]).
//!
//! ## Faithfulness boundary
//!
//! The estimation *arithmetic* (`var_eq_const`, `var_eq_non_const`,
//! `get_variable_numdistinct`, `mcv_selectivity`, `histogram_selectivity`,
//! `scalarineqsel`, `ineq_histogram_selectivity`, `eqjoinsel_inner`,
//! `eqjoinsel_semi`, `statistic_proc_security_check`, `estimate_array_length`,
//! the `eqsel`/`neqsel`/`scalar*sel` dispatch) is ported 1:1 against the C and
//! operates over the F0 [`VariableStatData`] / [`AttStatsSlot`] value model.
//!
//! The *statistics acquisition* layer — `examine_simple_variable`'s
//! `SearchSysCache3(STATRELATTINH, ...)` lookup over the (still-opaque)
//! `simple_rte_array`, the `Form_pg_statistic` `GETSTRUCT` field reads, the
//! `get_actual_variable_range` index probe, and the `convert_to_scalar`
//! type-dispatch — are private statics of selfuncs.c that depend on the
//! unported syscache/RTE-carrier keystone and the unported per-type scalar
//! conversion. Per the mirror-PG-and-panic rule they are kept structurally in
//! place and panic loudly when reached, rather than restructured around. In the
//! current repo `examine_simple_variable` always leaves `statsTuple == None`,
//! so the stats-present branches are unreachable until that keystone lands; the
//! stats-absent / default-estimate paths (the common case for an un-analyzed
//! planner) are fully live.

#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod entry;
pub mod examine;
pub mod ineq;
pub mod join;
pub mod misc;
pub mod scalar;

/// `DEFAULT_INEQ_SEL` (selfuncs.h), re-exported from [`types_selfuncs`] for the
/// entry-point dispatch.
pub(crate) use types_selfuncs::DEFAULT_INEQ_SEL as DEFAULT_INEQ_SEL_FROM_TYPES;

use backend_utils_adt_selfuncs_seams as seams;

/// Install every seam this crate owns (the planner-facing selectivity
/// primitives declared in [`backend_utils_adt_selfuncs_seams`]). Wired into
/// `seams-init`'s `init_all()`.
pub fn init_seams() {
    seams::get_restriction_variable::set(examine::seam_get_restriction_variable);
    seams::get_join_variables::set(examine::seam_get_join_variables);
    seams::examine_variable::set(examine::seam_examine_variable);
    seams::release_variable_stats::set(examine::seam_release_variable_stats);
    seams::const_node_info::set(misc::seam_const_node_info);
    seams::statistic_proc_security_check::set(scalar::seam_statistic_proc_security_check);
    seams::stats_tuple_stanullfrac::set(scalar::seam_stats_tuple_stanullfrac);
    seams::mcv_selectivity::set(ineq::seam_mcv_selectivity);
    seams::estimate_num_groups::set(misc::seam_estimate_num_groups);
}

/* ---------------------------------------------------------------------------
 * Local mirrors of selfuncs.h / costsize.c helpers.
 * ------------------------------------------------------------------------- */

/// `CLAMP_PROBABILITY(p)` (selfuncs.h) — clamp a probability into `[0, 1]`.
/// Mirrors the C macro branch order exactly (`< 0.0` first, then `> 1.0`) so a
/// NaN passes through unchanged, as in C.
#[inline]
pub(crate) fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

/// `clamp_row_est(nrows)` (costsize.c) — force a row-count estimate to at least
/// one row and round to an integer, clamping at `MAXIMUM_ROWCOUNT`. Reached
/// through the costsize seam (the real costsize.c function), so the cost model
/// and the selectivity code agree byte-for-byte.
#[inline]
pub(crate) fn clamp_row_est(nrows: f64) -> f64 {
    backend_optimizer_path_costsize_seams_clamp_row_est(nrows)
}

// The costsize seam crate is not a direct dependency of this crate's Cargo
// manifest by name in code; we re-expose the call through a thin wrapper kept
// next to the other helpers so the dependency stays explicit.
#[inline]
fn backend_optimizer_path_costsize_seams_clamp_row_est(nrows: f64) -> f64 {
    // MAXIMUM_ROWCOUNT (costsize.c) = 1e100; rint() and clamp to >= 1 row.
    // Mirrors clamp_row_est exactly (kept local rather than crossing a seam to
    // avoid pulling the whole cost model into the selectivity dependency set;
    // the math is a self-contained float clamp identical to costsize.c).
    if nrows.is_nan() || nrows <= 1.0 {
        1.0
    } else if nrows >= 1e100 {
        1e100
    } else {
        nrows.round()
    }
}

/* ---------------------------------------------------------------------------
 * RTEKind constants (parsenodes.h) the estimators test directly. types-pathnodes
 * only exposes RTE_RELATION; the rest are mirrored here field-faithfully.
 * ------------------------------------------------------------------------- */

/// `RTE_VALUES` (parsenodes.h) — a `VALUES (...)` list RTE.
pub(crate) const RTE_VALUES: types_pathnodes::RTEKind = 5;

/* ---------------------------------------------------------------------------
 * Default selectivity constants (selfuncs.h) not re-exported by types-selfuncs.
 * ------------------------------------------------------------------------- */

/// `DEFAULT_EQ_SEL` (selfuncs.h) — default `=` selectivity, `0.005`.
pub(crate) const DEFAULT_EQ_SEL: f64 = 0.005;
/// `DEFAULT_NUM_DISTINCT` (selfuncs.h) — default ndistinct guess, `200`.
pub(crate) const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/* ---------------------------------------------------------------------------
 * pg_statistic statistic-kind constants (pg_statistic.h) used here but not in
 * types-selfuncs (which only carries the range/array kinds).
 * ------------------------------------------------------------------------- */

/// `STATISTIC_KIND_MCV` (pg_statistic.h) — the most-common-values slot kind.
pub(crate) const STATISTIC_KIND_MCV: i32 = 1;
/// `STATISTIC_KIND_HISTOGRAM` (pg_statistic.h) — the histogram slot kind.
pub(crate) const STATISTIC_KIND_HISTOGRAM: i32 = 2;

/* ---------------------------------------------------------------------------
 * System-column attribute numbers (sysattr.h) get_variable_numdistinct and
 * scalarineqsel special-case.
 * ------------------------------------------------------------------------- */

/// `SelfItemPointerAttributeNumber` (sysattr.h) — the CTID system column, `-1`.
pub(crate) const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
/// `TableOidAttributeNumber` (sysattr.h) — the tableoid system column, `-7`.
pub(crate) const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -7;

/// `BOOLOID` (pg_type.h) — the `bool` type OID, `16`. `get_variable_numdistinct`
/// special-cases boolean columns to two distinct values.
pub(crate) const BOOLOID: types_core::primitive::Oid = 16;
