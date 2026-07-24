//! multirangetypes_selfuncs.c: multirangesel. The histogram machinery is
//! line-identical to rangetypes_selfuncs.c and shared from that module.

use adt_multirangetypes::{multirange_get_bounds, multirange_is_empty};
use adt_rangetypes::RangeBound;
use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_pathnodes::NodeId;

use crate::rangetypes_selfuncs::{
    calc_hist_selectivity_contained, calc_hist_selectivity_contains,
    calc_hist_selectivity_scalar, varlena_image, RangeSelCtx,
    STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
};
use crate::run::PlannerRun;
use crate::selfuncs::{
    clamp_probability, get_restriction_variable, VariableStatData, DEFAULT_INEQ_SEL,
};

const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

const OID_MULTIRANGE_LESS_OP: Oid = 2862;
const OID_MULTIRANGE_LESS_EQUAL_OP: Oid = 2863;
const OID_MULTIRANGE_GREATER_EQUAL_OP: Oid = 2864;
const OID_MULTIRANGE_GREATER_OP: Oid = 2865;
const OID_RANGE_OVERLAPS_MULTIRANGE_OP: Oid = 2866;
const OID_MULTIRANGE_OVERLAPS_RANGE_OP: Oid = 2867;
const OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP: Oid = 2868;
const OID_MULTIRANGE_CONTAINS_ELEM_OP: Oid = 2869;
const OID_MULTIRANGE_CONTAINS_RANGE_OP: Oid = 2870;
const OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP: Oid = 2871;
const OID_MULTIRANGE_ELEM_CONTAINED_OP: Oid = 2872;
const OID_RANGE_MULTIRANGE_CONTAINED_OP: Oid = 4540;
const OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP: Oid = 2874;
const OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP: Oid = 2875;
const OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP: Oid = 2876;
const OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP: Oid = 2877;
const OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP: Oid = 3585;
const OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP: Oid = 4035;
const OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP: Oid = 4142;
const OID_RANGE_LEFT_MULTIRANGE_OP: Oid = 4395;
const OID_MULTIRANGE_LEFT_RANGE_OP: Oid = 4396;
const OID_MULTIRANGE_LEFT_MULTIRANGE_OP: Oid = 4397;
const OID_RANGE_RIGHT_MULTIRANGE_OP: Oid = 4398;
const OID_MULTIRANGE_RIGHT_RANGE_OP: Oid = 4399;
const OID_MULTIRANGE_RIGHT_MULTIRANGE_OP: Oid = 4400;
const OID_RANGE_CONTAINS_MULTIRANGE_OP: Oid = 4539;
const OID_MULTIRANGE_RANGE_CONTAINED_OP: Oid = 2873;

struct MultirangeSelCtx {
    mltrngtypid: Oid,
    rngtypid: Oid,
    rng: RangeSelCtx,
}

fn multirange_sel_ctx(mltrngtypid: Oid) -> PgResult<MultirangeSelCtx> {
    let e = typcache::lookup_type_cache(mltrngtypid, typcache::TYPECACHE_MULTIRANGE_INFO)?;
    let Some(rt) = e.rngtype() else {
        return Err(Box::new(types_error::PgError::error(format!(
            "type {mltrngtypid} is not a multirange type"
        ))));
    };
    let rngtypid = rt.type_id;
    Ok(MultirangeSelCtx { mltrngtypid, rngtypid, rng: RangeSelCtx::from_entry(rt)? })
}

fn default_multirange_selectivity(operator: Oid) -> f64 {
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
        | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP => DEFAULT_INEQ_SEL,
        _ => 0.01,
    }
}

pub fn multirangesel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut operator: Oid,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(default_multirange_selectivity(operator));
    };
    let Some(c) = other.as_const() else {
        return Ok(default_multirange_selectivity(operator));
    };
    if c.constisnull {
        return Ok(0.0);
    }
    if !varonleft {
        operator = lsyscache::get_commutator(operator)?;
        if operator == 0 {
            return Ok(default_multirange_selectivity(operator));
        }
    }

    let mcx = run.mcx;
    let mut ctx: Option<MultirangeSelCtx> = None;
    let mut constmultirange: Option<&[u8]> = None;
    if operator == OID_MULTIRANGE_CONTAINS_ELEM_OP {
        let mut c2 = multirange_sel_ctx(vardata.vartype)?;
        if c.consttype == c2.rng.ri.elem_typid {
            let mut lower = RangeBound {
                val: c.constvalue,
                infinite: false,
                inclusive: true,
                lower: true,
            };
            let mut upper = RangeBound {
                val: c.constvalue,
                infinite: false,
                inclusive: true,
                lower: false,
            };
            let img = adt_rangetypes::range_serialize(
                mcx, &mut c2.rng.ri, &mut lower, &mut upper, false, None,
            )?
            .expect("point range never soft-fails");
            let range: &[u8] = adt_multirangetypes::leak_image(img);
            let mut ranges: mcx::PgVec<'_, &[u8]> = mcx::vec_with_capacity_in(mcx, 1)?;
            ranges.push(range);
            let mr = adt_multirangetypes::make_multirange(
                mcx, c2.mltrngtypid, &mut c2.rng.ri, &mut ranges,
            )?;
            constmultirange = Some(adt_multirangetypes::leak_image(mr));
        }
        ctx = Some(c2);
    } else if matches!(
        operator,
        OID_RANGE_MULTIRANGE_CONTAINED_OP
            | OID_MULTIRANGE_CONTAINS_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
            | OID_MULTIRANGE_LEFT_RANGE_OP
            | OID_MULTIRANGE_RIGHT_RANGE_OP
    ) {
        let mut c2 = multirange_sel_ctx(vardata.vartype)?;
        if c.consttype == c2.rngtypid {
            let range: &[u8] = crate::selfuncs::varlena_image_any(mcx, c.constvalue)?;
            let mut ranges: mcx::PgVec<'_, &[u8]> = mcx::vec_with_capacity_in(mcx, 1)?;
            ranges.push(range);
            let mr = adt_multirangetypes::make_multirange(
                mcx, c2.mltrngtypid, &mut c2.rng.ri, &mut ranges,
            )?;
            constmultirange = Some(adt_multirangetypes::leak_image(mr));
        }
        ctx = Some(c2);
    } else if matches!(
        operator,
        OID_RANGE_OVERLAPS_MULTIRANGE_OP
            | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
            | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
            | OID_RANGE_LEFT_MULTIRANGE_OP
            | OID_RANGE_RIGHT_MULTIRANGE_OP
            | OID_RANGE_CONTAINS_MULTIRANGE_OP
            | OID_MULTIRANGE_ELEM_CONTAINED_OP
            | OID_MULTIRANGE_RANGE_CONTAINED_OP
    ) {
        // Var is the elem/range: punt to the default estimate (C does too).
    } else if c.consttype == vardata.vartype {
        ctx = Some(multirange_sel_ctx(vardata.vartype)?);
        constmultirange = Some(crate::selfuncs::varlena_image_any(mcx, c.constvalue)?);
    }

    let selec = match constmultirange {
        Some(cmr) => calc_multirangesel(
            run,
            ctx.as_mut().expect("typcache set with constmultirange"),
            &vardata,
            cmr,
            operator,
        )?,
        None => default_multirange_selectivity(operator),
    };
    Ok(clamp_probability(selec))
}

fn calc_multirangesel<'mcx>(
    run: &PlannerRun<'mcx>,
    ctx: &mut MultirangeSelCtx,
    vardata: &VariableStatData<'mcx>,
    constval: &[u8],
    operator: Oid,
) -> PgResult<f64> {
    let (null_frac, empty_frac): (f32, f32) = if vardata.stats.is_some() {
        let null_frac = vardata.nullfrac() as f32;
        let empty_frac = match vardata.slot(STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, 0) {
            Some(sslot) => {
                let numbers = sslot.numbers()?;
                if numbers.len() != 1 {
                    // C: elog(ERROR) — a degenerate/torn slot aborts the
                    // statement, never the backend.
                    return Err(Box::new(types_error::PgError::error(
                        "invalid empty fraction statistic".to_string(),
                    )));
                }
                numbers[0]
            }
            None => 0.0,
        };
        (null_frac, empty_frac)
    } else {
        (0.0, 0.0)
    };

    let mut selec: f64;
    if multirange_is_empty(constval) {
        selec = match operator {
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
            | OID_MULTIRANGE_LESS_OP => 0.0,
            OID_RANGE_MULTIRANGE_CONTAINED_OP
            | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
            | OID_MULTIRANGE_LESS_EQUAL_OP => empty_frac as f64,
            OID_MULTIRANGE_CONTAINS_RANGE_OP
            | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
            | OID_MULTIRANGE_GREATER_EQUAL_OP => 1.0,
            OID_MULTIRANGE_GREATER_OP => 1.0 - empty_frac as f64,
            _ => panic!("unexpected operator {operator}"),
        };
    } else {
        let mut hist_selec = calc_hist_selectivity(run, ctx, vardata, constval, operator)?;
        if hist_selec < 0.0 {
            hist_selec = default_multirange_selectivity(operator);
        }
        if operator == OID_RANGE_MULTIRANGE_CONTAINED_OP
            || operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
        {
            selec = (1.0 - empty_frac as f64) * hist_selec + empty_frac as f64;
        } else {
            selec = (1.0 - empty_frac as f64) * hist_selec;
        }
    }

    selec *= 1.0 - null_frac as f64;
    Ok(clamp_probability(selec))
}

fn calc_hist_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    ctx: &mut MultirangeSelCtx,
    vardata: &VariableStatData<'mcx>,
    constval: &[u8],
    operator: Oid,
) -> PgResult<f64> {
    let rng = &mut ctx.rng;
    // Can't use the histogram with insecure range support functions.
    if !crate::selfuncs::statistic_proc_security_check(vardata, rng.ri.cmp.fn_oid)? {
        return Ok(-1.0);
    }
    if let Some(sd) = &rng.subdiff {
        if !crate::selfuncs::statistic_proc_security_check(vardata, sd.fn_oid)? {
            return Ok(-1.0);
        }
    }
    let Some(hslot) = vardata.slot(STATISTIC_KIND_BOUNDS_HISTOGRAM, 0) else {
        return Ok(-1.0);
    };
    let hvalues = hslot.values()?;
    if hvalues.len() < 2 {
        return Ok(-1.0);
    }
    let nhist = hvalues.len();
    let mut hist_lower: mcx::PgVec<'mcx, RangeBound> =
        mcx::vec_with_capacity_in(run.mcx, nhist)?;
    let mut hist_upper: mcx::PgVec<'mcx, RangeBound> =
        mcx::vec_with_capacity_in(run.mcx, nhist)?;
    for &v in hvalues {
        let (lo, up, empty) =
            adt_rangetypes::range_deserialize(&rng.ri.elem, varlena_image(v));
        if empty {
            // C: elog(ERROR) — degenerate stats content aborts the
            // statement, never the backend.
            return Err(Box::new(types_error::PgError::error(
                "bounds histogram contains an empty range".to_string(),
            )));
        }
        hist_lower.push(lo);
        hist_upper.push(up);
    }

    let length_hist: Option<&[Datum]> = if matches!(
        operator,
        OID_MULTIRANGE_CONTAINS_RANGE_OP
            | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
            | OID_MULTIRANGE_RANGE_CONTAINED_OP
            | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
    ) {
        let Some(lslot) = vardata.slot(STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, 0) else {
            return Ok(-1.0);
        };
        let lvalues = lslot.values()?;
        if lvalues.len() < 2 {
            return Ok(-1.0);
        }
        Some(lvalues)
    } else {
        None
    };

    let count = adt_multirangetypes::multirange_count(constval) as usize;
    debug_assert!(count > 0);
    let (const_lower, _) = multirange_get_bounds(&rng.ri, constval, 0);
    let (_, const_upper) = multirange_get_bounds(&rng.ri, constval, count - 1);

    let hist_selec = match operator {
        OID_MULTIRANGE_LESS_OP => {
            calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_lower, false)?
        }
        OID_MULTIRANGE_LESS_EQUAL_OP => {
            calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_lower, true)?
        }
        OID_MULTIRANGE_GREATER_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_lower, false)?
        }
        OID_MULTIRANGE_GREATER_EQUAL_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_lower, true)?
        }
        OID_MULTIRANGE_LEFT_RANGE_OP | OID_MULTIRANGE_LEFT_MULTIRANGE_OP => {
            calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_upper, false)?
        }
        OID_MULTIRANGE_RIGHT_RANGE_OP | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, rng, &const_upper, &hist_lower, true)?
        }
        OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_lower, false)?
        }
        OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP => {
            calc_hist_selectivity_scalar(run.mcx, rng, &const_upper, &hist_upper, true)?
        }
        OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_CONTAINS_ELEM_OP => {
            let mut s =
                calc_hist_selectivity_scalar(run.mcx, rng, &const_lower, &hist_upper, false)?;
            s += 1.0
                - calc_hist_selectivity_scalar(run.mcx, rng, &const_upper, &hist_lower, true)?;
            1.0 - s
        }
        OID_MULTIRANGE_CONTAINS_RANGE_OP | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP => {
            calc_hist_selectivity_contains(
                run.mcx,
                rng,
                &const_lower,
                &const_upper,
                &hist_lower,
                length_hist.expect("length histogram fetched"),
            )?
        }
        OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP | OID_RANGE_MULTIRANGE_CONTAINED_OP => {
            if const_lower.infinite {
                calc_hist_selectivity_scalar(run.mcx, rng, &const_upper, &hist_upper, true)?
            } else if const_upper.infinite {
                1.0 - calc_hist_selectivity_scalar(
                    run.mcx, rng, &const_lower, &hist_lower, false,
                )?
            } else {
                calc_hist_selectivity_contained(
                    run.mcx,
                    rng,
                    &const_lower,
                    const_upper,
                    &hist_lower,
                    // 4540 is outside C's length-hist fetch list; C zero-fills
                    // lslot there (empty hist reads as frac 1.0).
                    length_hist.unwrap_or(&[]),
                )?
            }
        }
        _ => panic!("unknown multirange operator {operator}"),
    };

    Ok(hist_selec)
}
