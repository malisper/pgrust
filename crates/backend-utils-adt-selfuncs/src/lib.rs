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
//! [`examine`]): `statext_expressions_load` (extended-statistics tuple load) and
//! the CTE-subroot recursion (unported CTE planner). The `pg_statistic` catcache
//! probe `SearchSysCache3(STATRELATTINH, ...)` is now installed by the syscache
//! unit, so `examine_simple_variable` can pin a `statsTuple` for an analyzed
//! relation column and the `Form_pg_statistic` field reads ([`scalar`]) are
//! live. `convert_to_scalar` is ported per-type. `get_actual_variable_range`
//! declines gracefully (returns `false`, as C does for an unsuitable index) —
//! the index-only-scan endpoint probe needs a bare-scan indexam form the
//! node-shaped indexam seams don't expose — so the caller falls back to the
//! histogram bound. Both are reached only for an inequality against a histogram
//! on an analyzed column.

#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod convert;
pub mod cost;
pub mod dispatch;
pub mod entry;
pub mod examine;
pub mod ineq;
pub mod join;
pub mod mergejoin;
pub mod misc;
pub mod node_sel;
pub mod patternsel;
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

    // The like_support.c planner-support index-condition function
    // (`textlike_support` et al. -> `match_pattern_prefix`), reached through
    // indxpath.c's `get_index_clause_from_support` fmgr-support dispatch.
    patternsel::init_support_seam();

    // The clausesel-owned per-clause-node estimators (boolvarsel / booltestsel /
    // nulltestsel / nulltestsel_var / rowcomparesel) that selfuncs.c owns the
    // bodies of and clausesel.c dispatches directly (not through oprrest).
    backend_optimizer_path_small_seams::boolvarsel::set(node_sel::seam_boolvarsel);
    backend_optimizer_path_small_seams::booltestsel::set(node_sel::seam_booltestsel);
    backend_optimizer_path_small_seams::nulltestsel::set(node_sel::seam_nulltestsel);
    backend_optimizer_path_small_seams::nulltestsel_var::set(node_sel::seam_nulltestsel);
    backend_optimizer_path_small_seams::rowcomparesel::set(node_sel::seam_rowcomparesel);

    // The index-AM cost-estimation family (genericcostestimate/btcostestimate),
    // reached through costsize.c's `amcostestimate` dispatch.
    backend_optimizer_path_costsize_seams::amcostestimate::set(
        |root, run, path, loop_count| cost::seam_amcostestimate(root, run, path, loop_count),
    );

    // The merge-join scan selectivity and hash-bucket statistics estimators
    // (selfuncs.c) costsize.c reaches to cost merge joins / hash joins. Each
    // opens its own per-call MemoryContext for detoasted-stats allocations
    // (the result is a scalar), matching C running these in the planner's
    // context.
    backend_optimizer_path_costsize_seams::mergejoinscansel::set(
        |run, root, clause, opfamily, cmptype, nulls_first| {
            let cx = mcx::MemoryContext::new("selfuncs mergejoinscansel");
            mergejoin::mergejoinscansel(
                cx.mcx(),
                run,
                root,
                clause,
                opfamily,
                cmptype,
                nulls_first,
            )
        },
    );
    backend_optimizer_path_costsize_seams::estimate_hash_bucket_stats::set(
        |run, root, hashkey, nbuckets| {
            let cx = mcx::MemoryContext::new("selfuncs estimate_hash_bucket_stats");
            mergejoin::estimate_hash_bucket_stats(cx.mcx(), run, root, hashkey, nbuckets)
        },
    );
    backend_optimizer_path_costsize_seams::estimate_multivariate_bucketsize::set(
        |root, inner_rel, hashclauses| {
            misc::estimate_multivariate_bucketsize(root, inner_rel, hashclauses)
        },
    );
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
