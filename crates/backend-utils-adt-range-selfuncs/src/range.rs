//! Range operator vocabulary and entry point: `rangetypes_selfuncs.c`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::{Oid, OidIsValid, Selectivity};
use types_datum::datum::Datum;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_rangetypes::{RangeBound, RangeTypeP};
use types_selfuncs::{VariableStatData, DEFAULT_INEQ_SEL, DEFAULT_RANGE_INEQ_SEL};

use backend_utils_adt_rangetypes_seams::{range_deserialize, range_get_typcache, range_serialize};
use backend_utils_adt_selfuncs_seams::get_restriction_variable;
use backend_utils_cache_lsyscache_seams::get_commutator;

use crate::{
    calc_hist_prologue, calc_hist_selectivity_contained, calc_hist_selectivity_contains,
    calc_hist_selectivity_scalar, calc_sel, clamp_probability, elog_error, VarStatsGuard,
};

/* ---------------------------------------------------------------------------
 * Range operator OIDs (pg_operator.dat -> pg_operator_d.h, PostgreSQL 18.3).
 * ------------------------------------------------------------------------- */

pub const OID_RANGE_LESS_OP: Oid = 3884;
pub const OID_RANGE_LESS_EQUAL_OP: Oid = 3885;
pub const OID_RANGE_GREATER_EQUAL_OP: Oid = 3886;
pub const OID_RANGE_GREATER_OP: Oid = 3887;
pub const OID_RANGE_OVERLAP_OP: Oid = 3888;
pub const OID_RANGE_CONTAINS_ELEM_OP: Oid = 3889;
pub const OID_RANGE_CONTAINS_OP: Oid = 3890;
pub const OID_RANGE_ELEM_CONTAINED_OP: Oid = 3891;
pub const OID_RANGE_CONTAINED_OP: Oid = 3892;
pub const OID_RANGE_LEFT_OP: Oid = 3893;
pub const OID_RANGE_RIGHT_OP: Oid = 3894;
pub const OID_RANGE_OVERLAPS_LEFT_OP: Oid = 3895;
pub const OID_RANGE_OVERLAPS_RIGHT_OP: Oid = 3896;

/// `default_range_selectivity` — default selectivity estimate for the given
/// operator, when we don't have statistics or cannot use them.
pub fn default_range_selectivity(operator: Oid) -> f64 {
    match operator {
        OID_RANGE_OVERLAP_OP => 0.01,

        OID_RANGE_CONTAINS_OP | OID_RANGE_CONTAINED_OP => 0.005,

        OID_RANGE_CONTAINS_ELEM_OP | OID_RANGE_ELEM_CONTAINED_OP => {
            /*
             * "range @> elem" is more or less identical to a scalar inequality
             * "A >= b AND A <= c".
             */
            DEFAULT_RANGE_INEQ_SEL
        }

        OID_RANGE_LESS_OP
        | OID_RANGE_LESS_EQUAL_OP
        | OID_RANGE_GREATER_OP
        | OID_RANGE_GREATER_EQUAL_OP
        | OID_RANGE_LEFT_OP
        | OID_RANGE_RIGHT_OP
        | OID_RANGE_OVERLAPS_LEFT_OP
        | OID_RANGE_OVERLAPS_RIGHT_OP => {
            /* these are similar to regular scalar inequalities */
            DEFAULT_INEQ_SEL
        }

        _ => {
            /* all range operators should be handled above, but just in case */
            0.01
        }
    }
}

/// `rangesel(PG_FUNCTION_ARGS)` — restriction selectivity for range operators.
///
/// `root` / `args` are the raw fmgr argument words (`PG_GETARG_POINTER(0)` /
/// `PG_GETARG_POINTER(2)`): the planner `PlannerInfo *` and operator `List *`.
pub fn rangesel(
    mcx: Mcx<'_>,
    root: Datum,
    mut operator: Oid,
    args: Datum,
    var_relid: i32,
) -> PgResult<Selectivity> {
    /*
     * If expression is not (variable op something) or (something op variable),
     * then punt and return a default estimate.
     */
    let (vardata, other, varonleft) =
        match get_restriction_variable::call(mcx, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(default_range_selectivity(operator)),
        };
    /* RAII: ReleaseVariableStats on every exit path below. */
    let vardata = VarStatsGuard::new(vardata);

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    let other = match other {
        Expr::Const(c) => c,
        _ => return Ok(default_range_selectivity(operator)),
    };

    /*
     * All the range operators are strict, so we can cope with a NULL constant
     * right away.
     */
    if other.constisnull {
        return Ok(0.0);
    }

    /*
     * If var is on the right, commute the operator, so that we can assume the
     * var is on the left in what follows.
     */
    if !varonleft {
        /* we have other Op var, commute to make var Op other */
        operator = get_commutator::call(operator)?;
        if !OidIsValid(operator) {
            /* Use default selectivity (should we raise an error instead?) */
            return Ok(default_range_selectivity(operator));
        }
    }

    /*
     * OK, there's a Var and a Const. We need the Const to be of the same range
     * type as the column, else we can't do anything useful.
     *
     * If the operator is "range @> element", convert the constant element to a
     * single-point range so we don't need special handling for it below.
     */
    let mut typcache: Option<TypeCacheEntry> = None;
    let mut constrange: Option<RangeTypeP> = None;

    if operator == OID_RANGE_CONTAINS_ELEM_OP {
        let tc = range_get_typcache::call(vardata.data().vartype)?;
        /* C unconditionally derefs typcache->rngelemtype for a range type. */
        let elem_type_id = tc
            .rngelemtype
            .as_ref()
            .expect("range typcache has rngelemtype")
            .type_id;
        if other.consttype == elem_type_id {
            let lower = RangeBound {
                val: other.constvalue,
                infinite: false,
                inclusive: true,
                lower: true,
            };
            let upper = RangeBound {
                val: other.constvalue,
                infinite: false,
                inclusive: true,
                lower: false,
            };
            constrange = Some(range_serialize::call(mcx, &tc, &lower, &upper, false)?);
        }
        typcache = Some(tc);
    } else if operator == OID_RANGE_ELEM_CONTAINED_OP {
        /*
         * Here the Var is the elem, not the range. In typical cases
         * elem_contained_by_range_support will have simplified this case so we
         * won't get here; if we do, fall back on a default estimate.
         */
    } else if other.consttype == vardata.data().vartype {
        /* Both sides are the same range type */
        let tc = range_get_typcache::call(vardata.data().vartype)?;
        constrange = Some(
            backend_utils_adt_rangetypes_seams::datum_get_range_type_p::call(
                mcx,
                other.constvalue,
            )?,
        );
        typcache = Some(tc);
    }

    /*
     * If we got a valid constant on one side of the operator, estimate using
     * statistics; otherwise punt to the default. calc_rangesel need not handle
     * OID_RANGE_ELEM_CONTAINED_OP.
     */
    let selec = match (constrange, typcache) {
        (Some(constrange), Some(typcache)) => {
            calc_rangesel(mcx, &typcache, vardata.data(), constrange, operator)?
        }
        _ => default_range_selectivity(operator),
    };

    Ok(clamp_probability(selec))
}

/// `calc_rangesel` — empty/non-empty + null/empty-fraction merge.
fn calc_rangesel(
    mcx: Mcx<'_>,
    typcache: &TypeCacheEntry,
    vardata: &VariableStatData,
    constval: RangeTypeP,
    operator: Oid,
) -> PgResult<f64> {
    /* Extract the bounds of the constant value to learn if it's empty. */
    let (const_lower, const_upper, const_is_empty) = range_deserialize::call(typcache, constval)?;

    calc_sel(
        mcx,
        vardata,
        operator,
        const_is_empty,
        empty_const_selec,
        |op| op == OID_RANGE_CONTAINED_OP,
        default_range_selectivity,
        |mcx, vardata| calc_hist_selectivity(mcx, typcache, vardata, const_lower, const_upper, operator),
    )
}

/// The empty-constant switch of `calc_rangesel`.
fn empty_const_selec(operator: Oid, empty_frac: f32) -> PgResult<f64> {
    match operator {
        /* these return false if either argument is empty */
        OID_RANGE_OVERLAP_OP
        | OID_RANGE_OVERLAPS_LEFT_OP
        | OID_RANGE_OVERLAPS_RIGHT_OP
        | OID_RANGE_LEFT_OP
        | OID_RANGE_RIGHT_OP
        /* nothing is less than an empty range */
        | OID_RANGE_LESS_OP => Ok(0.0),

        /* only empty ranges can be contained by an empty range */
        OID_RANGE_CONTAINED_OP
        /* only empty ranges are <= an empty range */
        | OID_RANGE_LESS_EQUAL_OP => Ok(empty_frac as f64),

        /* everything contains an empty range */
        OID_RANGE_CONTAINS_OP
        /* everything is >= an empty range */
        | OID_RANGE_GREATER_EQUAL_OP => Ok(1.0),

        /* all non-empty ranges are > an empty range */
        OID_RANGE_GREATER_OP => Ok(1.0 - empty_frac as f64),

        /* an element cannot be empty */
        OID_RANGE_CONTAINS_ELEM_OP => elog_error(alloc::format!("unexpected operator {operator}")),

        _ => elog_error(alloc::format!("unexpected operator {operator}")),
    }
}

/// `calc_hist_selectivity` — range operator selectivity from bound histograms.
/// Returns `-1.0` if no usable statistics are available.
fn calc_hist_selectivity(
    mcx: Mcx<'_>,
    typcache: &TypeCacheEntry,
    vardata: &VariableStatData,
    const_lower: RangeBound,
    mut const_upper: RangeBound,
    operator: Oid,
) -> PgResult<f64> {
    let needs_length_hist =
        operator == OID_RANGE_CONTAINS_OP || operator == OID_RANGE_CONTAINED_OP;

    let hist = match calc_hist_prologue(mcx, typcache, vardata, needs_length_hist)? {
        Some(h) => h,
        None => return Ok(-1.0),
    };
    let hist_lower = hist.hist_lower.as_slice();
    let hist_upper = hist.hist_upper.as_slice();
    let length_hist = hist.length_hist();

    /*
     * Calculate selectivity comparing the lower or upper bound of the constant
     * with the histogram of lower or upper bounds.
     */
    let hist_selec: f64 = match operator {
        OID_RANGE_LESS_OP => {
            /*
             * The b-tree comparison operators (<, <=, >, >=) compare lower
             * bounds first, and upper bounds for equal lower bounds. Estimate
             * by comparing lower bounds only.
             */
            calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, false)?
        }

        OID_RANGE_LESS_EQUAL_OP => {
            calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, true)?
        }

        OID_RANGE_GREATER_OP => {
            1.0 - calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, false)?
        }

        OID_RANGE_GREATER_EQUAL_OP => {
            1.0 - calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, true)?
        }

        OID_RANGE_LEFT_OP => {
            /* var << const when upper(var) < lower(const) */
            calc_hist_selectivity_scalar(typcache, &const_lower, hist_upper, false)?
        }

        OID_RANGE_RIGHT_OP => {
            /* var >> const when lower(var) > upper(const) */
            1.0 - calc_hist_selectivity_scalar(typcache, &const_upper, hist_lower, true)?
        }

        OID_RANGE_OVERLAPS_RIGHT_OP => {
            /* compare lower bounds */
            1.0 - calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, false)?
        }

        OID_RANGE_OVERLAPS_LEFT_OP => {
            /* compare upper bounds */
            calc_hist_selectivity_scalar(typcache, &const_upper, hist_upper, true)?
        }

        OID_RANGE_OVERLAP_OP | OID_RANGE_CONTAINS_ELEM_OP => {
            /*
             * A && B <=> NOT (A << B OR A >> B). Since A << B and A >> B are
             * mutually exclusive we sum their probabilities. "range @> elem" is
             * equivalent to "range && [elem,elem]"; the caller already built
             * the singular range, so treat it the same as &&.
             */
            let mut hs = calc_hist_selectivity_scalar(typcache, &const_lower, hist_upper, false)?;
            hs += 1.0 - calc_hist_selectivity_scalar(typcache, &const_upper, hist_lower, true)?;
            1.0 - hs
        }

        OID_RANGE_CONTAINS_OP => calc_hist_selectivity_contains(
            typcache,
            &const_lower,
            &const_upper,
            hist_lower,
            length_hist,
        )?,

        OID_RANGE_CONTAINED_OP => {
            if const_lower.infinite {
                /*
                 * Lower bound no longer matters. Just estimate the fraction
                 * with an upper bound <= const upper bound.
                 */
                calc_hist_selectivity_scalar(typcache, &const_upper, hist_upper, true)?
            } else if const_upper.infinite {
                1.0 - calc_hist_selectivity_scalar(typcache, &const_lower, hist_lower, false)?
            } else {
                calc_hist_selectivity_contained(
                    typcache,
                    &const_lower,
                    &mut const_upper,
                    hist_lower,
                    length_hist,
                )?
            }
        }

        _ => {
            return elog_error(alloc::format!("unknown range operator {operator}"));
        }
    };

    Ok(hist_selec)
}
