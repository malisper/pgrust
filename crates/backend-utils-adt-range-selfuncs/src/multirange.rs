//! Multirange operator vocabulary and entry point: `multirangetypes_selfuncs.c`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::{Oid, OidIsValid, Selectivity};
// The bare-word newtype: the still-shim-typed sinks (`RangeBound.val` and the
// `DatumGetRangeTypeP` / `DatumGetMultirangeTypeP` / `range_serialize` seams
// owned by the not-yet-migrated rangetypes/multirangetypes crates) carry the
// raw `Datum` machine word.
use types_datum::datum::Datum;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_rangetypes::{MultirangeTypeP, RangeBound};
use types_selfuncs::{VariableStatData, DEFAULT_INEQ_SEL, DEFAULT_MULTIRANGE_INEQ_SEL};

use backend_utils_adt_multirangetypes_seams::{
    datum_get_multirange_type_p, make_multirange, multirange_get_bounds, multirange_get_typcache,
};
use backend_utils_adt_rangetypes_seams::range_serialize;
use backend_utils_adt_selfuncs_seams::get_restriction_variable;
use backend_utils_cache_lsyscache_seams::get_commutator;

use crate::{
    calc_hist_prologue, calc_hist_selectivity_contained, calc_hist_selectivity_contains,
    calc_hist_selectivity_scalar, calc_sel, clamp_probability, elog_error, VarStatsGuard,
};

/* ---------------------------------------------------------------------------
 * Multirange operator OIDs (pg_operator.dat -> pg_operator_d.h, 18.3).
 * ------------------------------------------------------------------------- */

pub const OID_MULTIRANGE_LESS_OP: Oid = 2862;
pub const OID_MULTIRANGE_LESS_EQUAL_OP: Oid = 2863;
pub const OID_MULTIRANGE_GREATER_EQUAL_OP: Oid = 2864;
pub const OID_MULTIRANGE_GREATER_OP: Oid = 2865;
pub const OID_RANGE_OVERLAPS_MULTIRANGE_OP: Oid = 2866;
pub const OID_MULTIRANGE_OVERLAPS_RANGE_OP: Oid = 2867;
pub const OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP: Oid = 2868;
pub const OID_MULTIRANGE_CONTAINS_ELEM_OP: Oid = 2869;
pub const OID_MULTIRANGE_CONTAINS_RANGE_OP: Oid = 2870;
pub const OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP: Oid = 2871;
pub const OID_MULTIRANGE_ELEM_CONTAINED_OP: Oid = 2872;
pub const OID_MULTIRANGE_RANGE_CONTAINED_OP: Oid = 2873;
pub const OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP: Oid = 2874;
pub const OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP: Oid = 2875;
pub const OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP: Oid = 2876;
pub const OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP: Oid = 2877;
pub const OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP: Oid = 3585;
pub const OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP: Oid = 4035;
pub const OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP: Oid = 4142;
pub const OID_RANGE_CONTAINS_MULTIRANGE_OP: Oid = 4539;
pub const OID_RANGE_MULTIRANGE_CONTAINED_OP: Oid = 4540;
pub const OID_RANGE_LEFT_MULTIRANGE_OP: Oid = 4395;
pub const OID_MULTIRANGE_LEFT_RANGE_OP: Oid = 4396;
pub const OID_MULTIRANGE_LEFT_MULTIRANGE_OP: Oid = 4397;
pub const OID_RANGE_RIGHT_MULTIRANGE_OP: Oid = 4398;
pub const OID_MULTIRANGE_RIGHT_RANGE_OP: Oid = 4399;
pub const OID_MULTIRANGE_RIGHT_MULTIRANGE_OP: Oid = 4400;

/// `default_multirange_selectivity` — default selectivity estimate.
pub fn default_multirange_selectivity(operator: Oid) -> f64 {
    match operator {
        OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_RANGE_OVERLAPS_MULTIRANGE_OP => 0.01,

        OID_RANGE_CONTAINS_MULTIRANGE_OP
        | OID_RANGE_MULTIRANGE_CONTAINED_OP
        | OID_MULTIRANGE_CONTAINS_RANGE_OP
        | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
        | OID_MULTIRANGE_RANGE_CONTAINED_OP
        | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP => 0.005,

        OID_MULTIRANGE_CONTAINS_ELEM_OP | OID_MULTIRANGE_ELEM_CONTAINED_OP => {
            /*
             * "multirange @> elem" is more or less identical to a scalar
             * inequality "A >= b AND A <= c".
             */
            DEFAULT_MULTIRANGE_INEQ_SEL
        }

        OID_MULTIRANGE_LESS_OP
        | OID_MULTIRANGE_LESS_EQUAL_OP
        | OID_MULTIRANGE_GREATER_OP
        | OID_MULTIRANGE_GREATER_EQUAL_OP
        | OID_MULTIRANGE_LEFT_RANGE_OP
        | OID_MULTIRANGE_LEFT_MULTIRANGE_OP
        | OID_RANGE_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_RIGHT_RANGE_OP
        | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP
        | OID_RANGE_RIGHT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP => {
            /* these are similar to regular scalar inequalities */
            DEFAULT_INEQ_SEL
        }

        _ => {
            /* all multirange operators should be handled above, just in case */
            0.01
        }
    }
}

/// `multirangesel(PG_FUNCTION_ARGS)` — restriction selectivity for multirange
/// operators.
pub fn multirangesel(
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
            None => return Ok(default_multirange_selectivity(operator)),
        };
    /* RAII: ReleaseVariableStats on every exit path below. */
    let vardata = VarStatsGuard::new(vardata);

    /* Can't do anything useful if the something is not a constant, either. */
    let other = match other {
        Expr::Const(c) => c,
        _ => return Ok(default_multirange_selectivity(operator)),
    };

    /*
     * All the multirange operators are strict, so we can cope with a NULL
     * constant right away.
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
            return Ok(default_multirange_selectivity(operator));
        }
    }

    /*
     * OK, there's a Var and a Const. We need the Const to be of the same
     * multirange type as the column, else we can't do anything useful.
     *
     * If the operator is "multirange @> element", convert the constant element
     * to a single-point multirange so we don't need special handling below.
     */
    let mut typcache: Option<TypeCacheEntry> = None;
    let mut constmultirange: Option<MultirangeTypeP> = None;

    if operator == OID_MULTIRANGE_CONTAINS_ELEM_OP {
        let tc = multirange_get_typcache::call(vardata.data().vartype)?;
        let rngtype = tc
            .rngtype
            .as_ref()
            .expect("multirange typcache has rngtype");
        /* C unconditionally derefs rngtype->rngelemtype for a range type. */
        let elem_type_id = rngtype
            .rngelemtype
            .as_ref()
            .expect("range typcache has rngelemtype")
            .type_id;
        if other.consttype == elem_type_id {
            // C: `lower.val = upper.val = other->constvalue;` — a verbatim
            // Datum-word copy. Pull the machine word out of the canonical
            // value's by-value arm for the still-shim `RangeBound.val`.
            let constword = Datum::from_usize(other.constvalue.as_usize());
            let lower = RangeBound {
                val: constword,
                infinite: false,
                inclusive: true,
                lower: true,
            };
            let upper = RangeBound {
                val: constword,
                infinite: false,
                inclusive: true,
                lower: false,
            };
            let constrange = range_serialize::call(mcx, rngtype, &lower, &upper, false)?;
            constmultirange =
                Some(make_multirange::call(mcx, tc.type_id, rngtype, &[constrange])?);
        }
        typcache = Some(tc);
    } else if operator == OID_RANGE_MULTIRANGE_CONTAINED_OP
        || operator == OID_MULTIRANGE_CONTAINS_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        || operator == OID_MULTIRANGE_LEFT_RANGE_OP
        || operator == OID_MULTIRANGE_RIGHT_RANGE_OP
    {
        /*
         * Promote a range in "multirange OP range" just like we do an element
         * in "multirange OP element".
         */
        let tc = multirange_get_typcache::call(vardata.data().vartype)?;
        let rngtype = tc
            .rngtype
            .as_ref()
            .expect("multirange typcache has rngtype");
        if other.consttype == rngtype.type_id {
            // C: `DatumGetRangeTypeP(other->constvalue)` — the word is a range
            // varlena pointer; the seam (still shim-typed) detoasts it.
            let constrange =
                backend_utils_adt_rangetypes_seams::datum_get_range_type_p::call(
                    mcx,
                    Datum::from_usize(other.constvalue.as_usize()),
                )?;
            constmultirange =
                Some(make_multirange::call(mcx, tc.type_id, rngtype, &[constrange])?);
        }
        typcache = Some(tc);
    } else if operator == OID_RANGE_OVERLAPS_MULTIRANGE_OP
        || operator == OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        || operator == OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        || operator == OID_RANGE_LEFT_MULTIRANGE_OP
        || operator == OID_RANGE_RIGHT_MULTIRANGE_OP
        || operator == OID_RANGE_CONTAINS_MULTIRANGE_OP
        || operator == OID_MULTIRANGE_ELEM_CONTAINED_OP
        || operator == OID_MULTIRANGE_RANGE_CONTAINED_OP
    {
        /*
         * Here the Var is the elem/range, not the multirange. For now we punt
         * and return the default estimate.
         */
    } else if other.consttype == vardata.data().vartype {
        /* Both sides are the same multirange type */
        let tc = multirange_get_typcache::call(vardata.data().vartype)?;
        // C: `DatumGetMultirangeTypeP(other->constvalue)` — the word is a
        // multirange varlena pointer; the seam (still shim-typed) detoasts it.
        constmultirange = Some(datum_get_multirange_type_p::call(
            mcx,
            Datum::from_usize(other.constvalue.as_usize()),
        )?);
        typcache = Some(tc);
    }

    /*
     * If we got a valid constant on one side of the operator, estimate using
     * statistics; otherwise punt to the default. calc_multirangesel need not
     * handle OID_MULTIRANGE_*_CONTAINED_OP.
     */
    let selec = match (constmultirange, typcache) {
        (Some(constmultirange), Some(typcache)) => {
            calc_multirangesel(mcx, &typcache, vardata.data(), constmultirange, operator)?
        }
        _ => default_multirange_selectivity(operator),
    };

    Ok(clamp_probability(selec))
}

/// `calc_multirangesel` — empty/non-empty + null/empty-fraction merge. The
/// resolved `typcache` is the *multirange* type-cache entry; the math-time
/// kernels run against its `rngtype` sub-entry.
fn calc_multirangesel(
    mcx: Mcx<'_>,
    typcache: &TypeCacheEntry,
    vardata: &VariableStatData,
    constval: MultirangeTypeP,
    operator: Oid,
) -> PgResult<f64> {
    let rng_typcache = typcache
        .rngtype
        .as_ref()
        .expect("multirange typcache has rngtype");

    /*
     * Extract the overall bounds of the constant value, and learn if it's
     * empty (a multirange is empty iff it has zero ranges).
     */
    let range_count = unsafe { (*constval.ptr).rangeCount };
    let const_is_empty = range_count == 0;
    let (const_lower, const_upper) = if const_is_empty {
        (RangeBound::default(), RangeBound::default())
    } else {
        let (lower, _tmp) = multirange_get_bounds::call(rng_typcache, constval, 0)?;
        let (_tmp2, upper) = multirange_get_bounds::call(rng_typcache, constval, range_count - 1)?;
        (lower, upper)
    };

    calc_sel(
        mcx,
        vardata,
        operator,
        const_is_empty,
        empty_const_selec,
        |op| {
            op == OID_RANGE_MULTIRANGE_CONTAINED_OP
                || op == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
        },
        default_multirange_selectivity,
        |mcx, vardata| {
            calc_hist_selectivity(mcx, rng_typcache, vardata, const_lower, const_upper, operator)
        },
    )
}

/// The empty-constant switch of `calc_multirangesel`.
fn empty_const_selec(operator: Oid, empty_frac: f32) -> PgResult<f64> {
    match operator {
        /* these return false if either argument is empty */
        OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_MULTIRANGE_LEFT_RANGE_OP
        | OID_MULTIRANGE_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_RIGHT_RANGE_OP
        | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP
        /* nothing is less than an empty multirange */
        | OID_MULTIRANGE_LESS_OP => Ok(0.0),

        /* only empty multiranges can be contained by an empty multirange */
        OID_RANGE_MULTIRANGE_CONTAINED_OP
        | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
        /* only empty ranges are <= an empty multirange */
        | OID_MULTIRANGE_LESS_EQUAL_OP => Ok(empty_frac as f64),

        /* everything contains an empty multirange */
        OID_MULTIRANGE_CONTAINS_RANGE_OP
        | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
        /* everything is >= an empty multirange */
        | OID_MULTIRANGE_GREATER_EQUAL_OP => Ok(1.0),

        /* all non-empty multiranges are > an empty multirange */
        OID_MULTIRANGE_GREATER_OP => Ok(1.0 - empty_frac as f64),

        /* an element cannot be empty; the rest are filtered out by multirangesel() */
        OID_MULTIRANGE_CONTAINS_ELEM_OP
        | OID_RANGE_OVERLAPS_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_RANGE_LEFT_MULTIRANGE_OP
        | OID_RANGE_RIGHT_MULTIRANGE_OP
        | OID_RANGE_CONTAINS_MULTIRANGE_OP
        | OID_MULTIRANGE_ELEM_CONTAINED_OP
        | OID_MULTIRANGE_RANGE_CONTAINED_OP => {
            elog_error(alloc::format!("unexpected operator {operator}"))
        }

        _ => elog_error(alloc::format!("unexpected operator {operator}")),
    }
}

/// `calc_hist_selectivity` — multirange operator selectivity from bound
/// histograms (`rng_typcache` is the range type-cache entry). Returns `-1.0` if
/// no usable statistics are available.
fn calc_hist_selectivity(
    mcx: Mcx<'_>,
    rng_typcache: &TypeCacheEntry,
    vardata: &VariableStatData,
    const_lower: RangeBound,
    mut const_upper: RangeBound,
    operator: Oid,
) -> PgResult<f64> {
    let needs_length_hist = operator == OID_MULTIRANGE_CONTAINS_RANGE_OP
        || operator == OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
        || operator == OID_MULTIRANGE_RANGE_CONTAINED_OP
        || operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP;

    let hist = match calc_hist_prologue(mcx, rng_typcache, vardata, needs_length_hist)? {
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
        OID_MULTIRANGE_LESS_OP => {
            /*
             * The b-tree comparison operators compare lower bounds first, and
             * upper bounds for equal lower bounds. Estimate by comparing lower
             * bounds only.
             */
            calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, false)?
        }

        OID_MULTIRANGE_LESS_EQUAL_OP => {
            calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, true)?
        }

        OID_MULTIRANGE_GREATER_OP => {
            1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, false)?
        }

        OID_MULTIRANGE_GREATER_EQUAL_OP => {
            1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, true)?
        }

        OID_MULTIRANGE_LEFT_RANGE_OP | OID_MULTIRANGE_LEFT_MULTIRANGE_OP => {
            /* var << const when upper(var) < lower(const) */
            calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_upper, false)?
        }

        OID_MULTIRANGE_RIGHT_RANGE_OP | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP => {
            /* var >> const when lower(var) > upper(const) */
            1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_upper, hist_lower, true)?
        }

        OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP => {
            /* compare lower bounds */
            1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, false)?
        }

        OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP => {
            /* compare upper bounds */
            calc_hist_selectivity_scalar(rng_typcache, &const_upper, hist_upper, true)?
        }

        OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_CONTAINS_ELEM_OP => {
            /*
             * A && B <=> NOT (A << B OR A >> B). Since A << B and A >> B are
             * mutually exclusive we sum their probabilities. "multirange @>
             * elem" is equivalent to "multirange && {[elem,elem]}"; the caller
             * already built the singular range, so treat it the same as &&.
             */
            let mut hs =
                calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_upper, false)?;
            hs += 1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_upper, hist_lower, true)?;
            1.0 - hs
        }

        OID_MULTIRANGE_CONTAINS_RANGE_OP | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP => {
            calc_hist_selectivity_contains(
                rng_typcache,
                &const_lower,
                &const_upper,
                hist_lower,
                length_hist,
            )?
        }

        OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP | OID_RANGE_MULTIRANGE_CONTAINED_OP => {
            if const_lower.infinite {
                /*
                 * Lower bound no longer matters. Just estimate the fraction
                 * with an upper bound <= const upper bound.
                 */
                calc_hist_selectivity_scalar(rng_typcache, &const_upper, hist_upper, true)?
            } else if const_upper.infinite {
                1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_lower, hist_lower, false)?
            } else {
                calc_hist_selectivity_contained(
                    rng_typcache,
                    &const_lower,
                    &mut const_upper,
                    hist_lower,
                    length_hist,
                )?
            }
        }

        /* filtered out by multirangesel() */
        OID_RANGE_OVERLAPS_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_RANGE_LEFT_MULTIRANGE_OP
        | OID_RANGE_RIGHT_MULTIRANGE_OP
        | OID_RANGE_CONTAINS_MULTIRANGE_OP
        | OID_MULTIRANGE_ELEM_CONTAINED_OP
        | OID_MULTIRANGE_RANGE_CONTAINED_OP => {
            return elog_error(alloc::format!("unknown multirange operator {operator}"));
        }

        _ => {
            return elog_error(alloc::format!("unknown multirange operator {operator}"));
        }
    };

    Ok(hist_selec)
}
