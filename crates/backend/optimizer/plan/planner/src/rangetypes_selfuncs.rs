//! rangetypes_selfuncs.c: rangesel + the bound/length-histogram machinery
//! (shared with multirangetypes_selfuncs.rs, whose C copy is line-identical).

use adt_rangetypes::{range_cmp_bounds, range_deserialize, RangeBound, RangeInfo};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_pathnodes::NodeId;

use crate::run::PlannerRun;
use crate::selfuncs::{
    clamp_probability, get_restriction_variable, VariableStatData, DEFAULT_INEQ_SEL,
};

pub const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
pub const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

const OID_RANGE_LESS_OP: Oid = 3884;
const OID_RANGE_LESS_EQUAL_OP: Oid = 3885;
const OID_RANGE_GREATER_EQUAL_OP: Oid = 3886;
const OID_RANGE_GREATER_OP: Oid = 3887;
const OID_RANGE_OVERLAP_OP: Oid = 3888;
const OID_RANGE_CONTAINS_ELEM_OP: Oid = 3889;
const OID_RANGE_CONTAINS_OP: Oid = 3890;
const OID_RANGE_ELEM_CONTAINED_OP: Oid = 3891;
const OID_RANGE_CONTAINED_OP: Oid = 3892;
const OID_RANGE_LEFT_OP: Oid = 3893;
const OID_RANGE_RIGHT_OP: Oid = 3894;
const OID_RANGE_OVERLAPS_LEFT_OP: Oid = 3895;
const OID_RANGE_OVERLAPS_RIGHT_OP: Oid = 3896;

// Stats-side view of one range type: the cmp/subdiff pair calc_* needs
// (C reads them straight off the TypeCacheEntry).
pub(crate) struct RangeSelCtx {
    pub ri: RangeInfo,
    pub subdiff: Option<FmgrInfo>,
}

impl RangeSelCtx {
    pub(crate) fn for_range_type(rngtypid: Oid) -> PgResult<RangeSelCtx> {
        let e = typcache::lookup_type_cache(rngtypid, typcache::TYPECACHE_RANGE_INFO)?;
        Self::from_entry(e)
    }

    pub(crate) fn from_entry(
        e: std::rc::Rc<typcache::TypeCacheEntry>,
    ) -> PgResult<RangeSelCtx> {
        let subdiff = {
            let f = e.rng_subdiff_finfo();
            if f.fn_oid != 0 { Some(f.clone()) } else { None }
        };
        Ok(RangeSelCtx { ri: RangeInfo::from_entry(e)?, subdiff })
    }
}

// Stats lane only: decoded stats arrays hold 4-byte-header images
// (construct_array canonicalizes and the slot decode detoasts). Consts go
// through selfuncs::varlena_image_any — bound params can be short-form.
pub(crate) fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: live varlena datum out of a decoded stats array or a Const.
    unsafe {
        assert_eq!(*p & 0x03, 0, "packed varlena in range selectivity; detoast lane");
        core::slice::from_raw_parts(p, adt_rangetypes::varsize_4b(p))
    }
}

fn default_range_selectivity(operator: Oid) -> f64 {
    match operator {
        OID_RANGE_OVERLAP_OP => 0.01,
        OID_RANGE_CONTAINS_OP | OID_RANGE_CONTAINED_OP => 0.005,
        OID_RANGE_CONTAINS_ELEM_OP | OID_RANGE_ELEM_CONTAINED_OP => DEFAULT_RANGE_INEQ_SEL,
        OID_RANGE_LESS_OP
        | OID_RANGE_LESS_EQUAL_OP
        | OID_RANGE_GREATER_OP
        | OID_RANGE_GREATER_EQUAL_OP
        | OID_RANGE_LEFT_OP
        | OID_RANGE_RIGHT_OP
        | OID_RANGE_OVERLAPS_LEFT_OP
        | OID_RANGE_OVERLAPS_RIGHT_OP => DEFAULT_INEQ_SEL,
        _ => 0.01,
    }
}

pub fn rangesel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut operator: Oid,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(default_range_selectivity(operator));
    };
    let Some(c) = other.as_const() else {
        return Ok(default_range_selectivity(operator));
    };
    if c.constisnull {
        return Ok(0.0);
    }
    if !varonleft {
        operator = lsyscache::get_commutator(operator)?;
        if operator == 0 {
            return Ok(default_range_selectivity(operator));
        }
    }

    let mcx = run.mcx;
    let mut ctx: Option<RangeSelCtx> = None;
    let mut constrange: Option<&[u8]> = None;
    if operator == OID_RANGE_CONTAINS_ELEM_OP {
        let c2 = RangeSelCtx::for_range_type(vardata.vartype)?;
        if c.consttype == c2.ri.elem_typid {
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
            let mut ctx2 = c2;
            let img = adt_rangetypes::range_serialize(
                mcx, &mut ctx2.ri, &mut lower, &mut upper, false, None,
            )?
            .expect("point range never soft-fails");
            constrange = Some(adt_multirangetypes::leak_image(img));
            ctx = Some(ctx2);
        } else {
            ctx = Some(c2);
        }
    } else if operator == OID_RANGE_ELEM_CONTAINED_OP {
        // Var is the elem; elem_contained_by_range_support usually simplified
        // this away. Fall back on the default estimate (C does too).
    } else if c.consttype == vardata.vartype {
        ctx = Some(RangeSelCtx::for_range_type(vardata.vartype)?);
        constrange = Some(crate::selfuncs::varlena_image_any(mcx, c.constvalue)?);
    }

    let selec = match constrange {
        Some(cr) => {
            calc_rangesel(run, ctx.as_mut().expect("typcache set with constrange"), &vardata, cr, operator)?
        }
        None => default_range_selectivity(operator),
    };
    Ok(clamp_probability(selec))
}

fn calc_rangesel<'mcx>(
    run: &PlannerRun<'mcx>,
    ctx: &mut RangeSelCtx,
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
    if adt_rangetypes::range_is_empty(constval) {
        selec = match operator {
            OID_RANGE_OVERLAP_OP
            | OID_RANGE_OVERLAPS_LEFT_OP
            | OID_RANGE_OVERLAPS_RIGHT_OP
            | OID_RANGE_LEFT_OP
            | OID_RANGE_RIGHT_OP
            | OID_RANGE_LESS_OP => 0.0,
            OID_RANGE_CONTAINED_OP | OID_RANGE_LESS_EQUAL_OP => empty_frac as f64,
            OID_RANGE_CONTAINS_OP | OID_RANGE_GREATER_EQUAL_OP => 1.0,
            OID_RANGE_GREATER_OP => 1.0 - empty_frac as f64,
            _ => panic!("unexpected operator {operator}"),
        };
    } else {
        let mut hist_selec = calc_hist_selectivity(run, ctx, vardata, constval, operator)?;
        if hist_selec < 0.0 {
            hist_selec = default_range_selectivity(operator);
        }
        if operator == OID_RANGE_CONTAINED_OP {
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
    ctx: &mut RangeSelCtx,
    vardata: &VariableStatData<'mcx>,
    constval: &[u8],
    operator: Oid,
) -> PgResult<f64> {
    // Can't use the histogram with insecure range support functions.
    if !crate::selfuncs::statistic_proc_security_check(vardata, ctx.ri.cmp.fn_oid)? {
        return Ok(-1.0);
    }
    if let Some(sd) = &ctx.subdiff {
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
    let mut hist_lower: PgVec<'mcx, RangeBound> = mcx::vec_with_capacity_in(run.mcx, nhist)?;
    let mut hist_upper: PgVec<'mcx, RangeBound> = mcx::vec_with_capacity_in(run.mcx, nhist)?;
    for &v in hvalues {
        let (lo, up, empty) = range_deserialize(&ctx.ri.elem, varlena_image(v));
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

    let length_hist: Option<&[Datum]> =
        if operator == OID_RANGE_CONTAINS_OP || operator == OID_RANGE_CONTAINED_OP {
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

    let (const_lower, const_upper, empty) = range_deserialize(&ctx.ri.elem, constval);
    debug_assert!(!empty);

    let hist_selec = match operator {
        OID_RANGE_LESS_OP => {
            calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_lower, false)?
        }
        OID_RANGE_LESS_EQUAL_OP => {
            calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_lower, true)?
        }
        OID_RANGE_GREATER_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_lower, false)?
        }
        OID_RANGE_GREATER_EQUAL_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_lower, true)?
        }
        OID_RANGE_LEFT_OP => {
            calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_upper, false)?
        }
        OID_RANGE_RIGHT_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, ctx, &const_upper, &hist_lower, true)?
        }
        OID_RANGE_OVERLAPS_RIGHT_OP => {
            1.0 - calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_lower, false)?
        }
        OID_RANGE_OVERLAPS_LEFT_OP => {
            calc_hist_selectivity_scalar(run.mcx, ctx, &const_upper, &hist_upper, true)?
        }
        OID_RANGE_OVERLAP_OP | OID_RANGE_CONTAINS_ELEM_OP => {
            let mut s =
                calc_hist_selectivity_scalar(run.mcx, ctx, &const_lower, &hist_upper, false)?;
            s += 1.0
                - calc_hist_selectivity_scalar(run.mcx, ctx, &const_upper, &hist_lower, true)?;
            1.0 - s
        }
        OID_RANGE_CONTAINS_OP => calc_hist_selectivity_contains(
            run.mcx,
            ctx,
            &const_lower,
            &const_upper,
            &hist_lower,
            length_hist.expect("length histogram fetched"),
        )?,
        OID_RANGE_CONTAINED_OP => {
            if const_lower.infinite {
                calc_hist_selectivity_scalar(run.mcx, ctx, &const_upper, &hist_upper, true)?
            } else if const_upper.infinite {
                1.0 - calc_hist_selectivity_scalar(
                    run.mcx, ctx, &const_lower, &hist_lower, false,
                )?
            } else {
                calc_hist_selectivity_contained(
                    run.mcx,
                    ctx,
                    &const_lower,
                    const_upper,
                    &hist_lower,
                    length_hist.expect("length histogram fetched"),
                )?
            }
        }
        _ => panic!("unknown range operator {operator}"),
    };

    Ok(hist_selec)
}

pub(crate) fn calc_hist_selectivity_scalar(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    constbound: &RangeBound,
    hist: &[RangeBound],
    equal: bool,
) -> PgResult<f64> {
    let hist_nvalues = hist.len() as i32;
    let index = rbound_bsearch(mcx, ctx, constbound, hist, equal)?;
    let mut selec = index.max(0) as f64 / (hist_nvalues - 1) as f64;
    if index >= 0 && index < hist_nvalues - 1 {
        selec += get_position(
            mcx,
            ctx,
            constbound,
            &hist[index as usize],
            &hist[index as usize + 1],
        )? / (hist_nvalues - 1) as f64;
    }
    Ok(selec)
}

fn rbound_bsearch(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    value: &RangeBound,
    hist: &[RangeBound],
    equal: bool,
) -> PgResult<i32> {
    let mut lower: i32 = -1;
    let mut upper: i32 = hist.len() as i32 - 1;
    while lower < upper {
        let middle = (lower + upper + 1) / 2;
        let cmp = range_cmp_bounds(mcx, &mut ctx.ri, &hist[middle as usize], value)?;
        if cmp < 0 || (equal && cmp == 0) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    Ok(lower)
}

fn length_hist_bsearch(length_hist_values: &[Datum], value: f64, equal: bool) -> i32 {
    let mut lower: i32 = -1;
    let mut upper: i32 = length_hist_values.len() as i32 - 1;
    while lower < upper {
        let middle = (lower + upper + 1) / 2;
        let middleval = length_hist_values[middle as usize].as_f64();
        if middleval < value || (equal && middleval <= value) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    lower
}

fn subdiff(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    val2: Datum,
    val1: Datum,
) -> PgResult<f64> {
    let f = ctx.subdiff.as_mut().expect("caller checked has_subdiff");
    Ok(types_fmgr::function_call2_coll_in(f, ctx.ri.collation, mcx, val2, val1)?.as_f64())
}

fn get_position(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    value: &RangeBound,
    hist1: &RangeBound,
    hist2: &RangeBound,
) -> PgResult<f64> {
    let has_subdiff = ctx.subdiff.is_some();
    if !hist1.infinite && !hist2.infinite {
        if value.infinite {
            return Ok(0.5);
        }
        if !has_subdiff {
            return Ok(0.5);
        }
        let bin_width = subdiff(mcx, ctx, hist2.val, hist1.val)?;
        if bin_width.is_nan() || bin_width <= 0.0 {
            return Ok(0.5);
        }
        let position = subdiff(mcx, ctx, value.val, hist1.val)? / bin_width;
        if position.is_nan() {
            return Ok(0.5);
        }
        Ok(position.max(0.0).min(1.0))
    } else if hist1.infinite && !hist2.infinite {
        Ok(if value.infinite && value.lower { 0.0 } else { 1.0 })
    } else if !hist1.infinite && hist2.infinite {
        Ok(if value.infinite && !value.lower { 1.0 } else { 0.0 })
    } else {
        Ok(0.5)
    }
}

fn get_len_position(value: f64, hist1: f64, hist2: f64) -> f64 {
    if !hist1.is_infinite() && !hist2.is_infinite() {
        if value.is_infinite() {
            return 0.5;
        }
        1.0 - (hist2 - value) / (hist2 - hist1)
    } else if hist1.is_infinite() && !hist2.is_infinite() {
        1.0
    } else if hist1.is_infinite() && hist2.is_infinite() {
        // C's literal branch shape (the "reverse" case is unreachable there
        // too when hist1 <= hist2).
        0.0
    } else {
        0.5
    }
}

fn get_distance(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    bound1: &RangeBound,
    bound2: &RangeBound,
) -> PgResult<f64> {
    let has_subdiff = ctx.subdiff.is_some();
    if !bound1.infinite && !bound2.infinite {
        if has_subdiff {
            let res = subdiff(mcx, ctx, bound2.val, bound1.val)?;
            Ok(if res.is_nan() || res < 0.0 { 1.0 } else { res })
        } else {
            Ok(1.0)
        }
    } else if bound1.infinite && bound2.infinite {
        Ok(if bound1.lower == bound2.lower { 0.0 } else { f64::INFINITY })
    } else {
        Ok(f64::INFINITY)
    }
}

fn calc_length_hist_frac(
    length_hist_values: &[Datum],
    length1: f64,
    length2: f64,
    equal: bool,
) -> f64 {
    let length_hist_nvalues = length_hist_values.len() as i32;
    debug_assert!(length2 >= length1);
    if length2 < 0.0 {
        return 0.0;
    }
    if length2.is_infinite() && equal {
        return 1.0;
    }

    let mut i = length_hist_bsearch(length_hist_values, length1, equal);
    if i >= length_hist_nvalues - 1 {
        return 1.0;
    }
    let mut pos;
    if i < 0 {
        i = 0;
        pos = 0.0;
    } else {
        pos = get_len_position(
            length1,
            length_hist_values[i as usize].as_f64(),
            length_hist_values[i as usize + 1].as_f64(),
        );
    }
    let mut pb = (i as f64 + pos) / (length_hist_nvalues - 1) as f64;
    let mut b = length1;

    if length2 == length1 {
        return pb;
    }

    let mut area = 0.0;
    let mut pa;
    let mut a;
    while i < length_hist_nvalues - 1 {
        let bin_upper = length_hist_values[i as usize + 1].as_f64();
        if !(bin_upper < length2 || (equal && bin_upper <= length2)) {
            break;
        }
        a = b;
        pa = pb;
        b = bin_upper;
        pb = i as f64 / (length_hist_nvalues - 1) as f64;
        if pa > 0.0 || pb > 0.0 {
            area += 0.5 * (pb + pa) * (b - a);
        }
        i += 1;
    }

    a = b;
    pa = pb;
    b = length2;
    if i >= length_hist_nvalues - 1 {
        pos = 0.0;
    } else if length_hist_values[i as usize].as_f64()
        == length_hist_values[i as usize + 1].as_f64()
    {
        pos = 0.0;
    } else {
        pos = get_len_position(
            length2,
            length_hist_values[i as usize].as_f64(),
            length_hist_values[i as usize + 1].as_f64(),
        );
    }
    pb = (i as f64 + pos) / (length_hist_nvalues - 1) as f64;
    if pa > 0.0 || pb > 0.0 {
        area += 0.5 * (pb + pa) * (b - a);
    }

    if area.is_infinite() && length2.is_infinite() {
        0.5
    } else {
        area / (length2 - length1)
    }
}

pub(crate) fn calc_hist_selectivity_contained(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    lower: &RangeBound,
    mut upper: RangeBound,
    hist_lower: &[RangeBound],
    length_hist_values: &[Datum],
) -> PgResult<f64> {
    let hist_nvalues = hist_lower.len() as i32;
    upper.inclusive = !upper.inclusive;
    upper.lower = true;
    let mut upper_index = rbound_bsearch(mcx, ctx, &upper, hist_lower, false)?;
    if upper_index < 0 {
        return Ok(0.0);
    }
    upper_index = upper_index.min(hist_nvalues - 2);

    let upper_bin_width = get_position(
        mcx,
        ctx,
        &upper,
        &hist_lower[upper_index as usize],
        &hist_lower[upper_index as usize + 1],
    )?;

    let mut prev_dist = 0.0f64;
    let mut bin_width = upper_bin_width;
    let mut sum_frac = 0.0f64;
    let mut i = upper_index;
    while i >= 0 {
        let dist;
        let mut final_bin = false;
        if range_cmp_bounds(mcx, &mut ctx.ri, &hist_lower[i as usize], lower)? < 0 {
            dist = get_distance(mcx, ctx, lower, &upper)?;
            bin_width -= get_position(
                mcx,
                ctx,
                lower,
                &hist_lower[i as usize],
                &hist_lower[i as usize + 1],
            )?;
            if bin_width < 0.0 {
                bin_width = 0.0;
            }
            final_bin = true;
        } else {
            dist = get_distance(mcx, ctx, &hist_lower[i as usize], &upper)?;
        }

        let length_hist_frac =
            calc_length_hist_frac(length_hist_values, prev_dist, dist, true);
        sum_frac += length_hist_frac * bin_width / (hist_nvalues - 1) as f64;

        if final_bin {
            break;
        }
        bin_width = 1.0;
        prev_dist = dist;
        i -= 1;
    }

    Ok(sum_frac)
}

pub(crate) fn calc_hist_selectivity_contains(
    mcx: Mcx<'_>,
    ctx: &mut RangeSelCtx,
    lower: &RangeBound,
    upper: &RangeBound,
    hist_lower: &[RangeBound],
    length_hist_values: &[Datum],
) -> PgResult<f64> {
    let hist_nvalues = hist_lower.len() as i32;
    let mut lower_index = rbound_bsearch(mcx, ctx, lower, hist_lower, true)?;
    if lower_index < 0 {
        return Ok(0.0);
    }
    lower_index = lower_index.min(hist_nvalues - 2);

    let lower_bin_width = get_position(
        mcx,
        ctx,
        lower,
        &hist_lower[lower_index as usize],
        &hist_lower[lower_index as usize + 1],
    )?;

    let mut prev_dist = get_distance(mcx, ctx, lower, upper)?;
    let mut sum_frac = 0.0f64;
    let mut bin_width = lower_bin_width;
    let mut i = lower_index;
    while i >= 0 {
        let dist = get_distance(mcx, ctx, &hist_lower[i as usize], upper)?;
        let length_hist_frac =
            1.0 - calc_length_hist_frac(length_hist_values, prev_dist, dist, false);
        sum_frac += length_hist_frac * bin_width / (hist_nvalues - 1) as f64;
        bin_width = 1.0;
        prev_dist = dist;
        i -= 1;
    }

    Ok(sum_frac)
}
