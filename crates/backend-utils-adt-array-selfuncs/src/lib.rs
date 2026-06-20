//! Selectivity estimation of the array operators `@>` / `&&` / `<@`
//! (`utils/adt/array_selfuncs.c`, PostgreSQL 18.3): the `arraycontsel` /
//! `arraycontjoinsel` restriction/join estimators and the
//! `scalararraysel_containment` shortcut `selfuncs.c` uses for
//! `const =/<> ANY/ALL (array_var)`.
//!
//! The estimation *decision* algorithms — the most-common-elements
//! containment/overlap math, the "contained-by" estimate with its
//! distinct-element-count histogram correction, the histogram interpolation
//! (`calc_hist`), the distinct-count / Poisson distribution convolution
//! (`calc_distr`), the binary-search merge (`find_next_mcelem`), `floor_log2`,
//! and the per-operator dispatch — are this crate's own logic and run on safe
//! slices.
//!
//! Genuinely-external work crosses per-owner seams (mirror-and-panic until the
//! owner lands):
//!   * planner statistics access — `examine_variable`,
//!     `get_restriction_variable`, `statistic_proc_security_check`,
//!     `stats_tuple_stanullfrac`, `ReleaseVariableStats`, and the `IsA(node,
//!     Const)` decode (`backend-utils-adt-selfuncs-seams`, owner selfuncs.c);
//!   * `lsyscache` — `get_base_element_type` / `get_attstatsslot` /
//!     `get_typlenbyvalalign` / `get_typcollation`
//!     (`backend-utils-cache-lsyscache-seams`);
//!   * the typcache — `lookup_element_cmp_proc`
//!     (`backend-utils-cache-typcache-seams`), the OID of the element type's
//!     cached btree `cmp_proc_finfo`;
//!   * the array value layer — `deconstruct_array`
//!     (`backend-utils-adt-arrayfuncs-seams`); and
//!   * the fmgr operator-call layer — `function_call2_coll` for
//!     `element_compare` (`backend-utils-fmgr-fmgr-seams`).
//!
//! `root` / `args` / `leftop` / `rightop` are the raw fmgr/planner words
//! (`PG_GETARG_POINTER` / `Node *`), carried as `Datum` because the
//! planner-node model is owned by the not-yet-ported planner; the seam
//! providers retype them — the established convention from the range/multirange
//! selectivity ports.
//!
//! `array_selfuncs.c`'s `element_compare` invokes `FunctionCall2Coll(&typentry->
//! cmp_proc_finfo, typentry->typcollation, d1, d2)`. The repo `lookup_type_cache`
//! view does not surface `cmp_proc_finfo` / `typcollation`, so the C
//! `TypeCacheEntry *` comparator context is reproduced as the in-crate
//! [`ElemCmpInfo`] — the same bundle of fields the cache entry holds
//! (`type_id` / `typlen` / `typbyval` / `typalign` plus the resolved `cmp_proc`
//! OID and `typcollation`) — populated through the typcache / lsyscache seams.

#![allow(non_upper_case_globals)]
#![allow(clippy::manual_clamp)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::{InvalidOid, Oid, OidIsValid, Selectivity};
use types_datum::datum::Datum;
use types_error::{PgError, PgResult, ERROR};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo};
use types_selfuncs::{
    VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES, STATISTIC_KIND_DECHIST,
    STATISTIC_KIND_MCELEM,
};

use backend_utils_adt_arrayfuncs_seams::deconstruct_array;
use backend_utils_adt_selfuncs_seams::{
    examine_variable, get_restriction_variable, release_variable_stats,
    statistic_proc_security_check, stats_tuple_stanullfrac,
};
use backend_utils_cache_lsyscache_seams::{
    get_attstatsslot, get_base_element_type, get_typcollation, get_typlenbyvalalign,
};
use backend_utils_cache_typcache_seams::lookup_element_cmp_proc;
use backend_utils_fmgr_fmgr_seams::function_call2_coll;

/// Install every seam this crate owns. The fmgr entry points (`arraycontsel` /
/// `arraycontjoinsel`) and the planner-internal `scalararraysel_containment`
/// are reached through fmgr / direct dispatch, not a cross-cycle seam, so this
/// crate owns no inward seams and there is nothing to install (mirrors
/// `backend-utils-adt-range-selfuncs`).
pub fn init_seams() {}

/* ---------------------------------------------------------------------------
 * Selectivity-default constants (array_selfuncs.c:30-38).
 * ------------------------------------------------------------------------- */

/// Default selectivity constant for "@>" and "<@" operators.
const DEFAULT_CONTAIN_SEL: f64 = 0.005;
/// Default selectivity constant for "&&" operator.
const DEFAULT_OVERLAP_SEL: f64 = 0.01;

/* ---------------------------------------------------------------------------
 * Array operator OIDs (catalog/pg_operator.dat -> pg_operator_d.h, 18.3).
 * ------------------------------------------------------------------------- */

/// `OID_ARRAY_OVERLAP_OP` — `anyarray && anyarray` (overlaps).
const OID_ARRAY_OVERLAP_OP: Oid = 2750;
/// `OID_ARRAY_CONTAINS_OP` — `anyarray @> anyarray` (contains).
const OID_ARRAY_CONTAINS_OP: Oid = 2751;
/// `OID_ARRAY_CONTAINED_OP` — `anyarray <@ anyarray` (is contained by).
const OID_ARRAY_CONTAINED_OP: Oid = 2752;

/// `DEFAULT_SEL(operator)` (array_selfuncs.c:36-38) — default selectivity for
/// the given operator.
#[inline]
fn default_sel(operator: Oid) -> Selectivity {
    if operator == OID_ARRAY_OVERLAP_OP {
        DEFAULT_OVERLAP_SEL
    } else {
        DEFAULT_CONTAIN_SEL
    }
}

/* ---------------------------------------------------------------------------
 * Local mirrors of C macros / helpers.
 * ------------------------------------------------------------------------- */

/// `CLAMP_PROBABILITY(p)` (selfuncs.h) — clamp to `[0, 1]`. Mirrors the C macro
/// branch order exactly (`< 0.0` first, then `> 1.0`) so a NaN passes through.
#[inline]
fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

/// `Min(a, b)` (c.h) for `float8`.
#[inline]
fn min_f64(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `Min(a, b)` (c.h) for `float4`.
#[inline]
fn min_f32(a: f32, b: f32) -> f32 {
    if a < b {
        a
    } else {
        b
    }
}

/// `elog(ERROR, msg)` — raise an internal error as a recoverable value.
#[inline]
fn elog_error<T>(msg: impl Into<alloc::string::String>) -> PgResult<T> {
    Err(PgError::new(ERROR, msg))
}

/// Allocate an `n`-entry `f32` buffer zero-filled in `mcx`, mirroring
/// `palloc(n * sizeof(float))`. The C arrays are zeroed implicitly only where
/// the algorithm writes every cell; the callers here either fill every cell or
/// rely on the explicit zero (`calc_distr`'s reset loop, `calc_hist`'s
/// not-a-bound arm), so we zero-fill to match.
fn alloc_f32_zeroed<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, f32>> {
    let mut v = vec_with_capacity_in(mcx, n)?;
    v.resize(n, 0.0f32);
    Ok(v)
}

/* ---------------------------------------------------------------------------
 * element_compare comparator context (C: the `TypeCacheEntry *` passed as
 * qsort_arg's `arg` and to the merge helpers).
 *
 * `lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO)` returns a cache entry
 * holding exactly these fields; `element_compare` reads `cmp_proc_finfo.fn_oid`
 * and `typcollation`, and `mcelem_array_selec` reads `type_id` / `typlen` /
 * `typbyval` / `typalign` for `deconstruct_array`. The repo's `lookup_type_cache`
 * view does not surface the comparison finfo / collation, so this bundle is
 * resolved through the established per-field seams (typcache `cmp_proc`,
 * lsyscache `get_typlenbyvalalign` / `get_typcollation`).
 * ------------------------------------------------------------------------- */

/// The element type's comparison context, mirroring the fields of the C
/// `TypeCacheEntry *` that `array_selfuncs.c` threads through the estimators.
struct ElemCmpInfo {
    /// `typentry->type_id`.
    type_id: Oid,
    /// `typentry->typlen`.
    typlen: i16,
    /// `typentry->typbyval`.
    typbyval: bool,
    /// `typentry->typalign`.
    typalign: i8,
    /// `typentry->cmp_proc_finfo.fn_oid` (the cached btree comparison proc OID).
    cmp_proc: Oid,
    /// `typentry->typcollation`.
    typcollation: Oid,
}

impl ElemCmpInfo {
    /// `lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO)` — resolve the
    /// comparison proc, storage attributes, and collation for `elemtype`.
    fn lookup(elemtype: Oid) -> PgResult<Self> {
        let cmp_proc = lookup_element_cmp_proc::call(elemtype)?;
        let s = get_typlenbyvalalign::call(elemtype)?;
        let typcollation = get_typcollation::call(elemtype)?;
        Ok(ElemCmpInfo {
            type_id: elemtype,
            typlen: s.typlen,
            typbyval: s.typbyval,
            typalign: s.typalign,
            cmp_proc,
            typcollation,
        })
    }
}

/// `element_compare(key1, key2, typentry)` (array_selfuncs.c:1164): the in-crate
/// call site delegates `FunctionCall2Coll(&typentry->cmp_proc_finfo,
/// typentry->typcollation, d1, d2)` then `DatumGetInt32` to the fmgr seam.
#[inline]
fn element_compare(d1: Datum, d2: Datum, typentry: &ElemCmpInfo) -> PgResult<i32> {
    let c = function_call2_coll::call(typentry.cmp_proc, typentry.typcollation, d1, d2)?;
    Ok(c.as_i32())
}

/* ===========================================================================
 * scalararraysel_containment
 *		Estimate selectivity of ScalarArrayOpExpr via array containment.
 *
 * If we have const =/<> ANY/ALL (array_var) then we can estimate the
 * selectivity as though this were an array containment operator,
 * array_var op ARRAY[const].
 *
 * scalararraysel() has already verified that the ScalarArrayOpExpr's operator
 * is the array element type's default equality or inequality operator, and
 * has aggressively simplified both inputs to constants.
 *
 * Returns selectivity (0..1), or -1 if we fail to estimate selectivity.
 *
 * `root` is the planner state; `leftop` / `rightop` are the planner node
 * handles for the operator's operands (C `Node *`).
 * =========================================================================== */
pub fn scalararraysel_containment<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    leftop: NodeId,
    rightop: NodeId,
    elemtype: Oid,
    is_equality: bool,
    mut use_or: bool,
    var_relid: i32,
) -> PgResult<Selectivity> {
    let mut selec: Selectivity;

    /*
     * rightop must be a variable, else punt.
     */
    let vardata = examine_variable::call(mcx, run, root, rightop, var_relid)?;
    /* RAII: ReleaseVariableStats on every exit path below. */
    let vardata = VarStatsGuard::new(vardata);
    if vardata.data().rel.is_none() {
        return Ok(-1.0);
    }

    /*
     * leftop must be a constant, else punt. The caller (scalararraysel) already
     * reduced leftop with estimate_expression_value and interned it into the
     * planner node arena, so resolve it directly here (C: `if (!IsA(leftop,
     * Const)) return -1`).
     */
    let leftconst = match root.node(leftop).as_const() {
        Some(c) => c.clone(),
        None => return Ok(-1.0),
    };
    if leftconst.constisnull {
        /* qual can't succeed if null on left */
        return Ok(0.0);
    }
    /* C: `((Const *) leftop)->constvalue` — the by-value/by-ref Datum word.
     * Use as_byref_word(): the scalar element type can itself be pass-by-
     * reference (e.g. text), where as_usize() would panic. as_byref_word
     * returns the scalar word for by-val and the pointer for by-ref, i.e.
     * the DatumGetPointer view the downstream codecs consume. */
    let constval = Datum::from_usize(leftconst.constvalue.as_byref_word());

    /* Get element type's default comparison function */
    let typentry = ElemCmpInfo::lookup(elemtype)?;
    if !OidIsValid(typentry.cmp_proc) {
        return Ok(-1.0);
    }

    /*
     * If the operator is <>, swap ANY/ALL, then invert the result later.
     */
    if !is_equality {
        use_or = !use_or;
    }

    /* The constant's single element; one element is trivially presorted. */
    let constval_data = [constval];

    /* Get array element stats for var, if available */
    if vardata.data().stats_tuple.is_some()
        && statistic_proc_security_check::call(vardata.data(), typentry.cmp_proc)?
    {
        let stats_tuple = vardata.data().stats_tuple.unwrap();

        /* MCELEM will be an array of same type as element */
        if let Some(sslot) = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCELEM,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )? {
            /* For ALL case, also get histogram of distinct-element counts */
            let hslot = if use_or {
                None
            } else {
                get_attstatsslot::call(
                    mcx,
                    stats_tuple,
                    STATISTIC_KIND_DECHIST,
                    InvalidOid,
                    ATTSTATSSLOT_NUMBERS,
                )?
            };

            /*
             * For = ANY, estimate as var @> ARRAY[const].
             *
             * For = ALL, estimate as var <@ ARRAY[const].
             */
            if use_or {
                selec = mcelem_array_contain_overlap_selec(
                    sslot.values.as_slice(),
                    sslot.values.len() as i32,
                    slot_numbers(sslot.numbers.as_slice()),
                    sslot.numbers.len() as i32,
                    &constval_data,
                    1,
                    OID_ARRAY_CONTAINS_OP,
                    &typentry,
                )?;
            } else {
                let (hist, nhist) = match hslot.as_ref() {
                    Some(h) => (slot_numbers(h.numbers.as_slice()), h.numbers.len() as i32),
                    None => (None, 0),
                };
                selec = mcelem_array_contained_selec(
                    mcx,
                    sslot.values.as_slice(),
                    sslot.values.len() as i32,
                    slot_numbers(sslot.numbers.as_slice()),
                    sslot.numbers.len() as i32,
                    &constval_data,
                    1,
                    hist,
                    nhist,
                    OID_ARRAY_CONTAINED_OP,
                    &typentry,
                )?;
            }
            /* hslot / sslot free on drop (C: free_attstatsslot). */
        } else {
            /* No most-common-elements info, so do without */
            if use_or {
                selec = mcelem_array_contain_overlap_selec(
                    &[],
                    0,
                    None,
                    0,
                    &constval_data,
                    1,
                    OID_ARRAY_CONTAINS_OP,
                    &typentry,
                )?;
            } else {
                selec = mcelem_array_contained_selec(
                    mcx,
                    &[],
                    0,
                    None,
                    0,
                    &constval_data,
                    1,
                    None,
                    0,
                    OID_ARRAY_CONTAINED_OP,
                    &typentry,
                )?;
            }
        }

        /*
         * MCE stats count only non-null rows, so adjust for null rows.
         */
        let stanullfrac = stats_tuple_stanullfrac::call(stats_tuple);
        selec *= 1.0 - stanullfrac as f64;
    } else {
        /* No stats at all, so do without */
        if use_or {
            selec = mcelem_array_contain_overlap_selec(
                &[],
                0,
                None,
                0,
                &constval_data,
                1,
                OID_ARRAY_CONTAINS_OP,
                &typentry,
            )?;
        } else {
            selec = mcelem_array_contained_selec(
                mcx,
                &[],
                0,
                None,
                0,
                &constval_data,
                1,
                None,
                0,
                OID_ARRAY_CONTAINED_OP,
                &typentry,
            )?;
        }
        /* we assume no nulls here, so no stanullfrac correction */
    }

    /*
     * If the operator is <>, invert the results.
     */
    if !is_equality {
        selec = 1.0 - selec;
    }

    Ok(clamp_probability(selec))
}

/* ===========================================================================
 * Variable-stats RAII guard (C: ReleaseVariableStats on every exit path).
 * =========================================================================== */

/// Holds a `VariableStatData` acquired by `examine_variable` /
/// `get_restriction_variable`, running `ReleaseVariableStats` (the
/// `release_variable_stats` seam) on drop — covering every early return / `?`
/// exit, the C cleanup AGENTS.md requires be RAII.
struct VarStatsGuard {
    vardata: VariableStatData,
}

impl VarStatsGuard {
    fn new(vardata: VariableStatData) -> Self {
        Self { vardata }
    }

    fn data(&self) -> &VariableStatData {
        &self.vardata
    }
}

impl Drop for VarStatsGuard {
    fn drop(&mut self) {
        release_variable_stats::call(self.vardata);
    }
}

/// Borrow an `AttStatsSlot`'s `numbers` array as `Some(slice)`, or `None` if it
/// is empty — the idiomatic stand-in for the C `numbers == NULL` test.
#[inline]
fn slot_numbers(numbers: &[f32]) -> Option<&[f32]> {
    if numbers.is_empty() {
        None
    } else {
        Some(numbers)
    }
}

/* ===========================================================================
 * arraycontsel -- restriction selectivity for array @>, &&, <@ operators
 *
 * The C entry point reads its four arguments out of `FunctionCallInfo`
 * (root, operator, args, varRelid); here they are explicit parameters.
 * `root` is the planner state; `args` is the operator's argument `List *` as a
 * borrowed slice of planner node handles.
 * =========================================================================== */
pub fn arraycontsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut operator: Oid,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<Selectivity> {
    let selec: Selectivity;

    /*
     * If expression is not (variable op something) or (something op
     * variable), then punt and return a default estimate.
     */
    let (vardata, other, varonleft) =
        match get_restriction_variable::call(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(default_sel(operator)),
        };
    /* RAII: ReleaseVariableStats on every exit path below. */
    let vardata = VarStatsGuard::new(vardata);

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    let other = match other {
        types_nodes::primnodes::Expr::Const(c) => c,
        _ => return Ok(default_sel(operator)),
    };

    /*
     * The "&&", "@>" and "<@" operators are strict, so we can cope with a
     * NULL constant right away.
     */
    if other.constisnull {
        return Ok(0.0);
    }

    /*
     * If var is on the right, commute the operator, so that we can assume the
     * var is on the left in what follows.
     */
    if !varonleft {
        if operator == OID_ARRAY_CONTAINS_OP {
            operator = OID_ARRAY_CONTAINED_OP;
        } else if operator == OID_ARRAY_CONTAINED_OP {
            operator = OID_ARRAY_CONTAINS_OP;
        }
    }

    /*
     * OK, there's a Var and a Const we're dealing with here.  We need the
     * Const to be an array with same element type as column, else we can't do
     * anything useful.  (Such cases will likely fail at runtime, but here
     * we'd rather just return a default estimate.)
     */
    let element_typeid = get_base_element_type::call(other.consttype)?;
    if element_typeid != InvalidOid
        && element_typeid == get_base_element_type::call(vardata.data().vartype)?
    {
        // C: `((Const *) other)->constvalue` — the raw array varlena Datum.
        // Arrays are pass-by-reference, so as_usize() would panic; use
        // as_byref_word() to get the DatumGetPointer view calc_arraycontsel
        // (DatumGetArrayTypeP / deconstruct_array) needs.
        let constval = Datum::from_usize(other.constvalue.as_byref_word());
        selec = calc_arraycontsel(mcx, vardata.data(), constval, element_typeid, operator)?;
    } else {
        selec = default_sel(operator);
    }

    Ok(clamp_probability(selec))
}

/* ===========================================================================
 * arraycontjoinsel -- join selectivity for array @>, &&, <@ operators
 * =========================================================================== */
pub fn arraycontjoinsel(operator: Oid) -> PgResult<Selectivity> {
    /* For the moment this is just a stub */
    Ok(default_sel(operator))
}

/*
 * Calculate selectivity for "arraycolumn @> const", "arraycolumn && const"
 * or "arraycolumn <@ const" based on the statistics
 *
 * This function is mainly responsible for extracting the pg_statistic data
 * to be used; we then pass the problem on to mcelem_array_selec().
 */
fn calc_arraycontsel(
    mcx: Mcx<'_>,
    vardata: &VariableStatData,
    constval: Datum,
    elemtype: Oid,
    operator: Oid,
) -> PgResult<Selectivity> {
    let mut selec: Selectivity;

    /* Get element type's default comparison function */
    let typentry = ElemCmpInfo::lookup(elemtype)?;
    if !OidIsValid(typentry.cmp_proc) {
        return Ok(default_sel(operator));
    }

    /*
     * The caller made sure the const is an array with same element type, so
     * get it now.  In C this is `DatumGetArrayTypeP(constval)`, possibly a
     * detoasted copy that is `pfree`d at the end iff it differs from `constval`.
     * The repo's `deconstruct_array` seam detoasts the array `Datum` internally
     * and owns / frees any toast copy, so the bookkeeping does not surface here;
     * the array word is threaded straight to `mcelem_array_selec`.
     */
    let array = constval;

    if vardata.stats_tuple.is_some()
        && statistic_proc_security_check::call(vardata, typentry.cmp_proc)?
    {
        let stats_tuple = vardata.stats_tuple.unwrap();

        /* MCELEM will be an array of same type as column */
        if let Some(sslot) = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCELEM,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )? {
            /*
             * For "array <@ const" case we also need histogram of distinct
             * element counts.
             */
            let hslot = if operator != OID_ARRAY_CONTAINED_OP {
                None
            } else {
                get_attstatsslot::call(
                    mcx,
                    stats_tuple,
                    STATISTIC_KIND_DECHIST,
                    InvalidOid,
                    ATTSTATSSLOT_NUMBERS,
                )?
            };

            let (hist, nhist) = match hslot.as_ref() {
                Some(h) => (slot_numbers(h.numbers.as_slice()), h.numbers.len() as i32),
                None => (None, 0),
            };

            /* Use the most-common-elements slot for the array Var. */
            selec = mcelem_array_selec(
                mcx,
                array,
                &typentry,
                sslot.values.as_slice(),
                sslot.values.len() as i32,
                slot_numbers(sslot.numbers.as_slice()),
                sslot.numbers.len() as i32,
                hist,
                nhist,
                operator,
            )?;
            /* hslot / sslot free on drop (C: free_attstatsslot). */
        } else {
            /* No most-common-elements info, so do without */
            selec =
                mcelem_array_selec(mcx, array, &typentry, &[], 0, None, 0, None, 0, operator)?;
        }

        /*
         * MCE stats count only non-null rows, so adjust for null rows.
         */
        let stanullfrac = stats_tuple_stanullfrac::call(stats_tuple);
        selec *= 1.0 - stanullfrac as f64;
    } else {
        /* No stats at all, so do without */
        selec = mcelem_array_selec(mcx, array, &typentry, &[], 0, None, 0, None, 0, operator)?;
        /* we assume no nulls here, so no stanullfrac correction */
    }

    /* C: pfree the toast copy iff distinct from constval — internal to the seam. */
    Ok(selec)
}

/*
 * Array selectivity estimation based on most common elements statistics
 *
 * This function just deconstructs and sorts the array constant's contents,
 * and then passes the problem on to mcelem_array_contain_overlap_selec or
 * mcelem_array_contained_selec depending on the operator.
 */
fn mcelem_array_selec(
    mcx: Mcx<'_>,
    array: Datum,
    typentry: &ElemCmpInfo,
    mcelem: &[Datum],
    nmcelem: i32,
    numbers: Option<&[f32]>,
    nnumbers: i32,
    hist: Option<&[f32]>,
    nhist: i32,
    operator: Oid,
) -> PgResult<Selectivity> {
    let selec: Selectivity;

    /*
     * Prepare constant array data for sorting.  Sorting lets us find unique
     * elements and efficiently merge with the MCELEM array.
     */
    let deconstructed = deconstruct_array::call(
        mcx,
        array,
        typentry.type_id,
        typentry.typlen,
        typentry.typbyval,
        typentry.typalign as core::ffi::c_char,
    )?;
    let num_elems = deconstructed.len();
    // C out-params elem_values / elem_nulls, split into parallel buffers so the
    // null-collapse can compact `elem_values` in place exactly as C does.
    let mut elem_values = vec_with_capacity_in(mcx, num_elems)?;
    let mut elem_nulls = vec_with_capacity_in(mcx, num_elems)?;
    for (d, isnull) in deconstructed.iter() {
        elem_values.push(*d);
        elem_nulls.push(*isnull);
    }

    /* Collapse out any null elements */
    let mut nonnull_nitems: usize = 0;
    let mut null_present = false;
    for i in 0..num_elems {
        if elem_nulls[i] {
            null_present = true;
        } else {
            elem_values[nonnull_nitems] = elem_values[i];
            nonnull_nitems += 1;
        }
    }

    /*
     * Query "column @> '{anything, null}'" matches nothing.  For the other
     * two operators, presence of a null in the constant can be ignored.
     */
    if null_present && operator == OID_ARRAY_CONTAINS_OP {
        return Ok(0.0);
    }

    /* Sort extracted elements using their default comparison function. */
    qsort_arg_datum(&mut elem_values[..nonnull_nitems], typentry)?;

    /* Separate cases according to operator */
    if operator == OID_ARRAY_CONTAINS_OP || operator == OID_ARRAY_OVERLAP_OP {
        selec = mcelem_array_contain_overlap_selec(
            mcelem,
            nmcelem,
            numbers,
            nnumbers,
            &elem_values[..nonnull_nitems],
            nonnull_nitems as i32,
            operator,
            typentry,
        )?;
    } else if operator == OID_ARRAY_CONTAINED_OP {
        selec = mcelem_array_contained_selec(
            mcx,
            mcelem,
            nmcelem,
            numbers,
            nnumbers,
            &elem_values[..nonnull_nitems],
            nonnull_nitems as i32,
            hist,
            nhist,
            operator,
            typentry,
        )?;
    } else {
        return elog_error(alloc::format!(
            "arraycontsel called for unrecognized operator {operator}"
        ));
    }

    Ok(selec)
}

/*
 * Estimate selectivity of "column @> const" and "column && const" based on
 * most common element statistics.  This estimation assumes element
 * occurrences are independent.
 *
 * mcelem (of length nmcelem) and numbers (of length nnumbers) are from
 * the array column's MCELEM statistics slot, or are NULL/0 if stats are
 * not available.  array_data (of length nitems) is the constant's elements.
 *
 * Both the mcelem and array_data arrays are assumed presorted according
 * to the element type's cmpfunc.  Null elements are not present.
 */
fn mcelem_array_contain_overlap_selec(
    mcelem: &[Datum],
    nmcelem: i32,
    mut numbers: Option<&[f32]>,
    nnumbers: i32,
    array_data: &[Datum],
    nitems: i32,
    operator: Oid,
    typentry: &ElemCmpInfo,
) -> PgResult<Selectivity> {
    let mut selec: Selectivity;
    let mut elem_selec: Selectivity;
    let mut mcelem_index: i32;
    let use_bsearch: bool;
    let minfreq: f32;

    /*
     * There should be three more Numbers than Values, because the last three
     * cells should hold minimal and maximal frequency among the non-null
     * elements, and then the frequency of null elements.  Ignore the Numbers
     * if not right.
     */
    if nnumbers != nmcelem + 3 {
        numbers = None;
    }

    if let Some(numbers) = numbers {
        /* Grab the lowest observed frequency */
        minfreq = numbers[nmcelem as usize];
    } else {
        /* Without statistics make some default assumptions */
        minfreq = 2.0 * (DEFAULT_CONTAIN_SEL as f32);
    }

    /* Decide whether it is faster to use binary search or not. */
    if (nitems as i64) * (floor_log2(nmcelem as u32) as i64) < (nmcelem as i64 + nitems as i64) {
        use_bsearch = true;
    } else {
        use_bsearch = false;
    }

    if operator == OID_ARRAY_CONTAINS_OP {
        /*
         * Initial selectivity for "column @> const" query is 1.0, and it will
         * be decreased with each element of constant array.
         */
        selec = 1.0;
    } else {
        /*
         * Initial selectivity for "column && const" query is 0.0, and it will
         * be increased with each element of constant array.
         */
        selec = 0.0;
    }

    /* Scan mcelem and array in parallel. */
    mcelem_index = 0;
    let mut i: i32 = 0;
    while i < nitems {
        let mut is_match = false;

        /* Ignore any duplicates in the array data. */
        if i > 0
            && element_compare(array_data[(i - 1) as usize], array_data[i as usize], typentry)? == 0
        {
            i += 1;
            continue;
        }

        /* Find the smallest MCELEM >= this array item. */
        if use_bsearch {
            is_match = find_next_mcelem(
                mcelem,
                nmcelem,
                array_data[i as usize],
                &mut mcelem_index,
                typentry,
            )?;
        } else {
            while mcelem_index < nmcelem {
                let cmp = element_compare(
                    mcelem[mcelem_index as usize],
                    array_data[i as usize],
                    typentry,
                )?;

                if cmp < 0 {
                    mcelem_index += 1;
                } else {
                    if cmp == 0 {
                        is_match = true; /* mcelem is found */
                    }
                    break;
                }
            }
        }

        if is_match && numbers.is_some() {
            /* MCELEM matches the array item; use its frequency. */
            elem_selec = numbers.unwrap()[mcelem_index as usize] as Selectivity;
            mcelem_index += 1;
        } else {
            /*
             * The element is not in MCELEM.  Punt, but assume that the
             * selectivity cannot be more than minfreq / 2.
             */
            elem_selec = min_f64(DEFAULT_CONTAIN_SEL, (minfreq / 2.0) as f64);
        }

        /*
         * Update overall selectivity using the current element's selectivity
         * and an assumption of element occurrence independence.
         */
        if operator == OID_ARRAY_CONTAINS_OP {
            selec *= elem_selec;
        } else {
            selec = selec + elem_selec - selec * elem_selec;
        }

        /* Clamp intermediate results to stay sane despite roundoff error */
        selec = clamp_probability(selec);

        i += 1;
    }

    Ok(selec)
}

/*
 * Estimate selectivity of "column <@ const" based on most common element
 * statistics.
 *
 * mcelem (of length nmcelem) and numbers (of length nnumbers) are from
 * the array column's MCELEM statistics slot, or are NULL/0 if stats are
 * not available.  array_data (of length nitems) is the constant's elements.
 * hist (of length nhist) is from the array column's DECHIST statistics slot,
 * or is NULL/0 if those stats are not available.
 *
 * Both the mcelem and array_data arrays are assumed presorted according
 * to the element type's cmpfunc.  Null elements are not present.
 *
 * (See array_selfuncs.c for the full distribution-law derivation.)
 */
fn mcelem_array_contained_selec(
    mcx: Mcx<'_>,
    mcelem: &[Datum],
    nmcelem: i32,
    numbers: Option<&[f32]>,
    nnumbers: i32,
    array_data: &[Datum],
    nitems: i32,
    hist: Option<&[f32]>,
    nhist: i32,
    _operator: Oid,
    typentry: &ElemCmpInfo,
) -> PgResult<Selectivity> {
    let mut mcelem_index: i32;
    let mut unique_nitems: i32 = 0;
    let mut selec: f32;
    let minfreq: f32;
    let nullelem_freq: f32;
    let avg_count: f32;
    let mut mult: f32;
    let mut rest: f32;

    /*
     * There should be three more Numbers than Values in the MCELEM slot,
     * because the last three cells should hold minimal and maximal frequency
     * among the non-null elements, and then the frequency of null elements.
     * Punt if not right, because we can't do much without the element freqs.
     */
    let numbers = match numbers {
        Some(n) if nnumbers == nmcelem + 3 => n,
        _ => return Ok(DEFAULT_CONTAIN_SEL),
    };

    /* Can't do much without a count histogram, either */
    let hist = match hist {
        Some(h) if nhist >= 3 => h,
        _ => return Ok(DEFAULT_CONTAIN_SEL),
    };

    /*
     * Grab some of the summary statistics that compute_array_stats() stores:
     * lowest frequency, frequency of null elements, and average distinct
     * element count.
     */
    minfreq = numbers[nmcelem as usize];
    nullelem_freq = numbers[(nmcelem + 2) as usize];
    avg_count = hist[(nhist - 1) as usize];

    /*
     * "rest" will be the sum of the frequencies of all elements not
     * represented in MCELEM.  The average distinct element count is the sum
     * of the frequencies of *all* elements.  Begin with that; we will proceed
     * to subtract the MCELEM frequencies.
     */
    rest = avg_count;

    /*
     * mult is a multiplier representing estimate of probability that each
     * mcelem that is not present in constant doesn't occur.
     */
    mult = 1.0f32;

    /*
     * elem_selec is array of estimated frequencies for elements in the
     * constant.
     */
    let mut elem_selec = alloc_f32_zeroed(mcx, nitems.max(0) as usize)?;

    /* Scan mcelem and array in parallel. */
    mcelem_index = 0;
    let mut i: i32 = 0;
    while i < nitems {
        let mut is_match = false;

        /* Ignore any duplicates in the array data. */
        if i > 0
            && element_compare(array_data[(i - 1) as usize], array_data[i as usize], typentry)? == 0
        {
            i += 1;
            continue;
        }

        /*
         * Iterate over MCELEM until we find an entry greater than or equal to
         * this element of the constant.  Update "rest" and "mult" for mcelem
         * entries skipped over.
         */
        while mcelem_index < nmcelem {
            let cmp = element_compare(
                mcelem[mcelem_index as usize],
                array_data[i as usize],
                typentry,
            )?;

            if cmp < 0 {
                mult *= 1.0f32 - numbers[mcelem_index as usize];
                rest -= numbers[mcelem_index as usize];
                mcelem_index += 1;
            } else {
                if cmp == 0 {
                    is_match = true; /* mcelem is found */
                }
                break;
            }
        }

        if is_match {
            /* MCELEM matches the array item. */
            elem_selec[unique_nitems as usize] = numbers[mcelem_index as usize];
            /* "rest" is decremented for all mcelems, matched or not */
            rest -= numbers[mcelem_index as usize];
            mcelem_index += 1;
        } else {
            /*
             * The element is not in MCELEM.  Punt, but assume that the
             * selectivity cannot be more than minfreq / 2.
             */
            elem_selec[unique_nitems as usize] = min_f32(DEFAULT_CONTAIN_SEL as f32, minfreq / 2.0);
        }

        unique_nitems += 1;
        i += 1;
    }

    /*
     * If we handled all constant elements without exhausting the MCELEM
     * array, finish walking it to complete calculation of "rest" and "mult".
     */
    while mcelem_index < nmcelem {
        mult *= 1.0f32 - numbers[mcelem_index as usize];
        rest -= numbers[mcelem_index as usize];
        mcelem_index += 1;
    }

    /*
     * The presence of many distinct rare elements materially decreases
     * selectivity.  Use the Poisson distribution to estimate the probability
     * of a column value having zero occurrences of such elements.  See above
     * for the definition of "rest".
     */
    mult *= (-rest as f64).exp() as f32;

    /*----------
     * Using the distinct element count histogram requires
     *		O(unique_nitems * (nmcelem + unique_nitems))
     * operations.  Beyond a certain computational cost threshold, it's
     * reasonable to sacrifice accuracy for decreased planning time.  We limit
     * the number of operations to EFFORT * nmcelem; since nmcelem is limited
     * by the column's statistics target, the work done is user-controllable.
     *----------
     */
    const EFFORT: i32 = 100;

    if (nmcelem + unique_nitems) > 0 && unique_nitems > EFFORT * nmcelem / (nmcelem + unique_nitems)
    {
        /*
         * Use the quadratic formula to solve for largest allowable N.  We
         * have A = 1, B = nmcelem, C = - EFFORT * nmcelem.
         */
        let b = nmcelem as f64;
        let n: i32;

        n = (((b * b + 4.0 * EFFORT as f64 * b).sqrt() - b) / 2.0) as i32;

        /* Sort, then take just the first n elements */
        qsort_float_desc(&mut elem_selec[..unique_nitems as usize]);
        unique_nitems = n;
    }

    /*
     * Calculate probabilities of each distinct element count for both mcelems
     * and constant elements.  At this point, assume independent element
     * occurrence.
     */
    let dist = calc_distr(mcx, &elem_selec, unique_nitems, unique_nitems, 0.0f32)?;
    let mcelem_dist = calc_distr(mcx, numbers, nmcelem, unique_nitems, rest)?;

    /* ignore hist[nhist-1], which is the average not a histogram member */
    let hist_part = calc_hist(mcx, hist, nhist - 1, unique_nitems)?;

    selec = 0.0f32;
    let mut i: i32 = 0;
    while i <= unique_nitems {
        /*
         * mult * dist[i] / mcelem_dist[i] gives us probability of qual
         * matching from assumption of independent element occurrence with the
         * condition that distinct element count = i.
         */
        if mcelem_dist[i as usize] > 0.0 {
            selec += hist_part[i as usize] * mult * dist[i as usize] / mcelem_dist[i as usize];
        }
        i += 1;
    }

    /* dist / mcelem_dist / hist_part / elem_selec free on drop (C: pfree). */

    /* Take into account occurrence of NULL element. */
    selec *= 1.0f32 - nullelem_freq;

    Ok(clamp_probability(selec as f64))
}

/*
 * Calculate the first n distinct element count probabilities from a
 * histogram of distinct element counts.
 *
 * Returns an array of n+1 entries, with array[k] being the probability of
 * element count k, k in [0..n].
 *
 * We assume that a histogram box with bounds a and b gives 1 / ((b - a + 1) *
 * (nhist - 1)) probability to each value in (a,b) and an additional half of
 * that to a and b themselves.
 */
fn calc_hist<'mcx>(
    mcx: Mcx<'mcx>,
    hist: &[f32],
    nhist: i32,
    n: i32,
) -> PgResult<PgVec<'mcx, f32>> {
    let mut hist_part: PgVec<'mcx, f32>;
    let mut i: i32 = 0;
    let mut prev_interval: f32 = 0.0;
    let mut next_interval: f32;
    let frac: f32;

    hist_part = alloc_f32_zeroed(mcx, (n + 1).max(0) as usize)?;

    /*
     * frac is a probability contribution for each interval between histogram
     * values.  We have nhist - 1 intervals, so contribution of each one will
     * be 1 / (nhist - 1).
     */
    frac = 1.0f32 / ((nhist - 1) as f32);

    let mut k: i32 = 0;
    while k <= n {
        let mut count: i32 = 0;

        /*
         * Count the histogram boundaries equal to k.  (Although the histogram
         * should theoretically contain only exact integers, entries are
         * floats so there could be roundoff error in large values.  Treat any
         * fractional value as equal to the next larger k.)
         */
        while i < nhist && hist[i as usize] <= k as f32 {
            count += 1;
            i += 1;
        }

        if count > 0 {
            /* k is an exact bound for at least one histogram box. */
            let mut val: f32;

            /* Find length between current histogram value and the next one */
            if i < nhist {
                next_interval = hist[i as usize] - hist[(i - 1) as usize];
            } else {
                next_interval = 0.0;
            }

            /*
             * count - 1 histogram boxes contain k exclusively.  They
             * contribute a total of (count - 1) * frac probability.  Also
             * factor in the partial histogram boxes on either side.
             */
            val = (count - 1) as f32;
            if next_interval > 0.0 {
                val += 0.5f32 / next_interval;
            }
            if prev_interval > 0.0 {
                val += 0.5f32 / prev_interval;
            }
            hist_part[k as usize] = frac * val;

            prev_interval = next_interval;
        } else {
            /* k does not appear as an exact histogram bound. */
            if prev_interval > 0.0 {
                hist_part[k as usize] = frac / prev_interval;
            } else {
                hist_part[k as usize] = 0.0f32;
            }
        }

        k += 1;
    }

    Ok(hist_part)
}

/*
 * Consider n independent events with probabilities p[].  This function
 * calculates probabilities of exact k of events occurrence for k in [0..m].
 * Returns an array of size m+1.
 *
 * "rest" is the sum of the probabilities of all low-probability events not
 * included in p.
 *
 * Imagine matrix M of size (n + 1) x (m + 1).  Element M[i,j] denotes the
 * probability that exactly j of first i events occur.  Obviously M[0,0] = 1.
 * For any constant j, each increment of i increases the probability iff the
 * event occurs.  So, by the law of total probability:
 *	M[i,j] = M[i - 1, j] * (1 - p[i]) + M[i - 1, j - 1] * p[i]
 *		for i > 0, j > 0.
 *	M[i,0] = M[i - 1, 0] * (1 - p[i]) for i > 0.
 */
fn calc_distr<'mcx>(
    mcx: Mcx<'mcx>,
    p: &[f32],
    n: i32,
    m: i32,
    rest: f32,
) -> PgResult<PgVec<'mcx, f32>> {
    let mut row: PgVec<'mcx, f32>;
    let mut prev_row: PgVec<'mcx, f32>;
    let mut i: i32;
    let mut j: i32;

    /*
     * Since we return only the last row of the matrix and need only the
     * current and previous row for calculations, allocate two rows.
     */
    row = alloc_f32_zeroed(mcx, (m + 1).max(0) as usize)?;
    prev_row = alloc_f32_zeroed(mcx, (m + 1).max(0) as usize)?;

    /* M[0,0] = 1 */
    row[0] = 1.0f32;
    i = 1;
    while i <= n {
        let t = p[(i - 1) as usize];

        /* Swap rows */
        core::mem::swap(&mut row, &mut prev_row);

        /* Calculate next row */
        j = 0;
        while j <= i && j <= m {
            let mut val = 0.0f32;

            if j < i {
                val += prev_row[j as usize] * (1.0f32 - t);
            }
            if j > 0 {
                val += prev_row[(j - 1) as usize] * t;
            }
            row[j as usize] = val;
            j += 1;
        }
        i += 1;
    }

    /*
     * The presence of many distinct rare (not in "p") elements materially
     * decreases selectivity.  Model their collective occurrence with the
     * Poisson distribution.
     */
    if rest > DEFAULT_CONTAIN_SEL as f32 {
        let mut t: f32;

        /* Swap rows */
        core::mem::swap(&mut row, &mut prev_row);

        i = 0;
        while i <= m {
            row[i as usize] = 0.0f32;
            i += 1;
        }

        /* Value of Poisson distribution for 0 occurrences */
        t = (-rest as f64).exp() as f32;

        /*
         * Calculate convolution of previously computed distribution and the
         * Poisson distribution.
         */
        i = 0;
        while i <= m {
            j = 0;
            while j <= m - i {
                row[(j + i) as usize] += prev_row[j as usize] * t;
                j += 1;
            }

            /* Get Poisson distribution value for (i + 1) occurrences */
            t *= rest / ((i + 1) as f32);
            i += 1;
        }
    }

    /* prev_row frees on drop (C: pfree(prev_row)); the returned `row` escapes. */
    Ok(row)
}

/* Fast function for floor value of 2 based logarithm calculation. */
fn floor_log2(mut n: u32) -> i32 {
    let mut logval: i32 = 0;

    if n == 0 {
        return -1;
    }
    if n >= (1 << 16) {
        n >>= 16;
        logval += 16;
    }
    if n >= (1 << 8) {
        n >>= 8;
        logval += 8;
    }
    if n >= (1 << 4) {
        n >>= 4;
        logval += 4;
    }
    if n >= (1 << 2) {
        n >>= 2;
        logval += 2;
    }
    if n >= (1 << 1) {
        logval += 1;
    }
    logval
}

/*
 * find_next_mcelem binary-searches a most common elements array, starting
 * from *index, for the first member >= value.  It saves the position of the
 * match into *index and returns true if it's an exact match.  (Note: we
 * assume the mcelem elements are distinct so there can't be more than one
 * exact match.)
 */
fn find_next_mcelem(
    mcelem: &[Datum],
    nmcelem: i32,
    value: Datum,
    index: &mut i32,
    typentry: &ElemCmpInfo,
) -> PgResult<bool> {
    let mut l: i32 = *index;
    let mut r: i32 = nmcelem - 1;
    let mut i: i32;
    let mut res: i32;

    while l <= r {
        i = (l + r) / 2;
        res = element_compare(mcelem[i as usize], value, typentry)?;
        if res == 0 {
            *index = i;
            return Ok(true);
        } else if res < 0 {
            l = i + 1;
        } else {
            r = i - 1;
        }
    }
    *index = l;
    Ok(false)
}

/*
 * Comparison function for sorting floats into descending order.
 */
fn float_compare_desc(d1: f32, d2: f32) -> i32 {
    if d1 > d2 {
        -1
    } else if d1 < d2 {
        1
    } else {
        0
    }
}

/* ---- sort helpers ------------------------------------------------------- */

/// `qsort_arg(elem_values, n, sizeof(Datum), element_compare, typentry)`
/// (array_selfuncs.c:476). Sorts the Datums using the element type's default
/// comparison proc (via the fmgr seam). A comparator error cannot be raised
/// from a Rust `sort_by` closure, so it is captured and surfaced after the sort.
fn qsort_arg_datum(slice: &mut [Datum], typentry: &ElemCmpInfo) -> PgResult<()> {
    if slice.len() <= 1 {
        return Ok(());
    }

    let mut err: Option<PgError> = None;
    slice.sort_by(|&a, &b| {
        if err.is_some() {
            return core::cmp::Ordering::Equal;
        }
        match element_compare(a, b, typentry) {
            Ok(c) => c.cmp(&0),
            Err(e) => {
                err = Some(e);
                core::cmp::Ordering::Equal
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `qsort(elem_selec, n, sizeof(float), float_compare_desc)` — sort floats into
/// descending order.
fn qsort_float_desc(elem_selec: &mut [f32]) {
    elem_selec.sort_by(|&a, &b| match float_compare_desc(a, b) {
        x if x < 0 => core::cmp::Ordering::Less,
        x if x > 0 => core::cmp::Ordering::Greater,
        _ => core::cmp::Ordering::Equal,
    });
}

#[cfg(test)]
mod tests;
