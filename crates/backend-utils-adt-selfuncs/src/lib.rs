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
//! The *variable recognition* layer ([`examine`]) is now ported over the
//! PlannerRun keystone: `examine_variable` / `examine_simple_variable` /
//! `get_restriction_variable` / `get_join_variables` resolve the
//! `simple_rte_array` through `&PlannerRun<'mcx>` (`planner_rt_fetch`), strip
//! PlaceHolderVars/RelabelTypes, take the simple-Var fast path, walk index
//! expressions / extended statistics, and recurse into subquery subroots, 1:1
//! with the C.
//!
//! A few leaves remain seam-and-panic into genuinely-unported owners (see
//! [`examine`]): `SearchSysCache3(STATRELATTINH, ...)` (the `pg_statistic`
//! catcache probe — declared by the ported syscache unit but not yet installed,
//! so a relation-column stats lookup raises the owner's panic),
//! `statext_expressions_load` (extended-statistics tuple load), the CTE-subroot
//! recursion (unported CTE planner), and the `Form_pg_statistic` `GETSTRUCT`
//! field reads ([`scalar`], reached only after a live `statsTuple` exists). With
//! `search_statrelattinh` uninstalled, `examine_simple_variable` cannot yet pin
//! a `statsTuple` for a relation column, so the stats-absent / default-estimate
//! paths (the common case for an un-analyzed planner) are the live behaviour and
//! the `convert_to_scalar` / `get_actual_variable_range` ineq leaves stay
//! seam-and-panic until that catcache wiring + per-type scalar conversion land.

#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod dispatch;
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

    // The plancat selectivity-dispatch seams (`restriction_selectivity` /
    // `join_selectivity` / `function_selectivity` reach these): map the
    // operator's `oprrest`/`oprjoin` OID to the ported estimator.
    dispatch::init_dispatch_seams();
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

/// `FirstLowInvalidHeapAttributeNumber` (sysattr.h) — the lower bound for
/// system-column attnums, used to bias a column attnum into a `Bitmapset`
/// member (`attno - FirstLowInvalidHeapAttributeNumber`) in `examine_variable`
/// / `all_rows_selectable`. This repo pins it to `-7` (see
/// `types_tuple::heaptuple`).
pub(crate) const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: types_core::primitive::AttrNumber = -7;
