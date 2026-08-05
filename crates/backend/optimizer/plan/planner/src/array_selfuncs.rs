//! array_selfuncs.c: arraycontsel/arraycontjoinsel + scalararraysel_containment
//! — MCELEM/DECHIST-based selectivity for array @>, &&, <@.

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_nodes::Node;
use types_pathnodes::NodeId;

use crate::run::PlannerRun;
use crate::selfuncs::{
    clamp_probability, examine_variable, get_restriction_variable,
    statistic_proc_security_check, varlena_image_any, VariableStatData,
};

const DEFAULT_CONTAIN_SEL: f64 = 0.005;
const DEFAULT_OVERLAP_SEL: f64 = 0.01;

const OID_ARRAY_OVERLAP_OP: Oid = 2750;
const OID_ARRAY_CONTAINS_OP: Oid = 2751;
const OID_ARRAY_CONTAINED_OP: Oid = 2752;

const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;

fn default_sel(operator: Oid) -> f64 {
    if operator == OID_ARRAY_OVERLAP_OP { DEFAULT_OVERLAP_SEL } else { DEFAULT_CONTAIN_SEL }
}

// Element comparisons run the type's default btree cmp proc under its default
// collation (C reads both off the TypeCacheEntry).
struct ElemCmp<'mcx> {
    mcx: Mcx<'mcx>,
    finfo: FmgrInfo,
    collation: Oid,
}

impl<'mcx> ElemCmp<'mcx> {
    fn for_type(mcx: Mcx<'mcx>, elemtype: Oid) -> PgResult<Option<ElemCmp<'mcx>>> {
        let entry = typcache::lookup_type_cache(elemtype, typcache::TYPECACHE_CMP_PROC_FINFO)?;
        let finfo = entry.cmp_proc_finfo().clone();
        if finfo.fn_oid == 0 {
            return Ok(None);
        }
        Ok(Some(ElemCmp { mcx, finfo, collation: entry.typcollation() }))
    }

    fn cmp(&mut self, a: Datum, b: Datum) -> PgResult<i32> {
        Ok(types_fmgr::function_call2_coll_in(&mut self.finfo, self.collation, self.mcx, a, b)?
            .as_i32())
    }
}

pub fn arraycontsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut operator: Oid,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(default_sel(operator));
    };
    let Some(c) = other.as_const() else {
        return Ok(default_sel(operator));
    };
    // "&&", "@>" and "<@" are strict.
    if c.constisnull {
        return Ok(0.0);
    }
    if !varonleft {
        if operator == OID_ARRAY_CONTAINS_OP {
            operator = OID_ARRAY_CONTAINED_OP;
        } else if operator == OID_ARRAY_CONTAINED_OP {
            operator = OID_ARRAY_CONTAINS_OP;
        }
    }
    let element_typeid = lsyscache::get_base_element_type(c.consttype)?;
    let selec = if element_typeid != 0
        && element_typeid == lsyscache::get_base_element_type(vardata.vartype)?
    {
        calc_arraycontsel(run.mcx, &vardata, c.constvalue, element_typeid, operator)?
    } else {
        default_sel(operator)
    };
    Ok(clamp_probability(selec))
}

// arraycontjoinsel (array_selfuncs.c): C is a stub returning DEFAULT_SEL.
pub fn arraycontjoinsel(operator: Oid) -> f64 {
    default_sel(operator)
}

// scalararraysel_containment (array_selfuncs.c): estimate "const =/<> ANY/ALL
// (array_var)" as array containment. Returns -1.0 when it cannot estimate.
pub(crate) fn scalararraysel_containment<'mcx>(
    run: &mut PlannerRun<'mcx>,
    leftop: Node<'mcx>,
    rightop: Node<'mcx>,
    elemtype: Oid,
    is_equality: bool,
    mut use_or: bool,
    varrelid: i32,
) -> PgResult<f64> {
    let rightop_id = run.intern_expr(rightop);
    let vardata = examine_variable(run, rightop_id, rightop, varrelid)?;
    if vardata.rel.is_none() {
        return Ok(-1.0);
    }
    let Some(lc) = leftop.as_const() else {
        return Ok(-1.0);
    };
    if lc.constisnull {
        return Ok(0.0);
    }
    let constval = lc.constvalue;
    let Some(mut cmp) = ElemCmp::for_type(run.mcx, elemtype)? else {
        return Ok(-1.0);
    };
    // If the operator is <>, swap ANY/ALL, then invert the result later.
    if !is_equality {
        use_or = !use_or;
    }
    let mut selec = if vardata.stats.is_some()
        && statistic_proc_security_check(&vardata, cmp.finfo.fn_oid)?
    {
        let s = match vardata.slot(STATISTIC_KIND_MCELEM, 0) {
            Some(sslot) => {
                if use_or {
                    // For = ANY, estimate as var @> ARRAY[const].
                    mcelem_array_contain_overlap_selec(
                        &mut cmp,
                        sslot.values()?,
                        Some(sslot.numbers()?),
                        &[constval],
                        OID_ARRAY_CONTAINS_OP,
                    )?
                } else {
                    // For = ALL, estimate as var <@ ARRAY[const].
                    let hist = match vardata.slot(STATISTIC_KIND_DECHIST, 0) {
                        Some(h) => Some(h.numbers()?),
                        None => None,
                    };
                    mcelem_array_contained_selec(
                        &mut cmp,
                        sslot.values()?,
                        Some(sslot.numbers()?),
                        &[constval],
                        hist,
                    )?
                }
            }
            None => {
                if use_or {
                    mcelem_array_contain_overlap_selec(
                        &mut cmp,
                        &[],
                        None,
                        &[constval],
                        OID_ARRAY_CONTAINS_OP,
                    )?
                } else {
                    mcelem_array_contained_selec(&mut cmp, &[], None, &[constval], None)?
                }
            }
        };
        // MCE stats count only non-null rows.
        s * (1.0 - vardata.stats.as_ref().expect("checked above").stanullfrac as f64)
    } else if use_or {
        mcelem_array_contain_overlap_selec(&mut cmp, &[], None, &[constval], OID_ARRAY_CONTAINS_OP)?
    } else {
        mcelem_array_contained_selec(&mut cmp, &[], None, &[constval], None)?
    };
    if !is_equality {
        selec = 1.0 - selec;
    }
    Ok(clamp_probability(selec))
}

fn calc_arraycontsel<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData<'mcx>,
    constval: Datum,
    elemtype: Oid,
    operator: Oid,
) -> PgResult<f64> {
    let entry = typcache::lookup_type_cache(elemtype, typcache::TYPECACHE_CMP_PROC_FINFO)?;
    let finfo = entry.cmp_proc_finfo().clone();
    if finfo.fn_oid == 0 {
        return Ok(default_sel(operator));
    }
    let mut cmp = ElemCmp { mcx, finfo, collation: entry.typcollation() };
    let array_img = varlena_image_any(mcx, constval)?;
    let (typlen, typbyval, typalign) = (entry.typlen(), entry.typbyval(), entry.typalign());

    if vardata.stats.is_some() && statistic_proc_security_check(vardata, cmp.finfo.fn_oid)? {
        let selec = match vardata.slot(STATISTIC_KIND_MCELEM, 0) {
            Some(sslot) => {
                // "array <@ const" also needs the distinct-element-count
                // histogram.
                let hist = if operator == OID_ARRAY_CONTAINED_OP {
                    match vardata.slot(STATISTIC_KIND_DECHIST, 0) {
                        Some(h) => Some(h.numbers()?),
                        None => None,
                    }
                } else {
                    None
                };
                mcelem_array_selec(
                    mcx,
                    array_img,
                    typlen,
                    typbyval,
                    typalign,
                    &mut cmp,
                    sslot.values()?,
                    Some(sslot.numbers()?),
                    hist,
                    operator,
                )?
            }
            None => mcelem_array_selec(
                mcx, array_img, typlen, typbyval, typalign, &mut cmp, &[], None, None, operator,
            )?,
        };
        // MCE stats count only non-null rows.
        Ok(selec * (1.0 - vardata.stats.as_ref().expect("checked above").stanullfrac as f64))
    } else {
        mcelem_array_selec(
            mcx, array_img, typlen, typbyval, typalign, &mut cmp, &[], None, None, operator,
        )
    }
}

// Deconstruct and sort the constant, then dispatch per operator.
#[allow(clippy::too_many_arguments)]
fn mcelem_array_selec<'mcx>(
    mcx: Mcx<'mcx>,
    array_img: &[u8],
    typlen: i16,
    typbyval: bool,
    typalign: i8,
    cmp: &mut ElemCmp<'mcx>,
    mcelem: &[Datum],
    numbers: Option<&[f32]>,
    hist: Option<&[f32]>,
    operator: Oid,
) -> PgResult<f64> {
    let (mut elem_values, elem_nulls) = arrayfuncs::deconstruct_array(
        mcx, array_img, typlen as i32, typbyval, typalign as u8, true,
    )?;

    // Collapse out null elements.
    let mut nonnull_nitems = 0usize;
    let mut null_present = false;
    for i in 0..elem_values.len() {
        if elem_nulls[i] {
            null_present = true;
        } else {
            elem_values[nonnull_nitems] = elem_values[i];
            nonnull_nitems += 1;
        }
    }

    // "column @> '{anything, null}'" matches nothing.
    if null_present && operator == OID_ARRAY_CONTAINS_OP {
        return Ok(0.0);
    }

    sort_datums(cmp, &mut elem_values[..nonnull_nitems])?;
    let array_data = &elem_values[..nonnull_nitems];

    if operator == OID_ARRAY_CONTAINS_OP || operator == OID_ARRAY_OVERLAP_OP {
        mcelem_array_contain_overlap_selec(cmp, mcelem, numbers, array_data, operator)
    } else if operator == OID_ARRAY_CONTAINED_OP {
        mcelem_array_contained_selec(cmp, mcelem, numbers, array_data, hist)
    } else {
        Err(types_error::PgError::error(format!(
            "arraycontsel called for unrecognized operator {operator}"
        ))
        .into())
    }
}

fn sort_datums(cmp: &mut ElemCmp<'_>, v: &mut [Datum]) -> PgResult<()> {
    let mut err = None;
    v.sort_unstable_by(|&a, &b| {
        if err.is_some() {
            return core::cmp::Ordering::Equal;
        }
        match cmp.cmp(a, b) {
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

// "column @> const" / "column && const" via MCELEM, assuming independent
// element occurrence. Both mcelem and array_data are presorted, null-free.
fn mcelem_array_contain_overlap_selec(
    cmp: &mut ElemCmp<'_>,
    mcelem: &[Datum],
    mut numbers: Option<&[f32]>,
    array_data: &[Datum],
    operator: Oid,
) -> PgResult<f64> {
    let nmcelem = mcelem.len() as i32;
    let nitems = array_data.len() as i32;

    // The last three Numbers cells hold min freq, max freq, null-element
    // freq; ignore Numbers of any other shape.
    if numbers.is_some_and(|n| n.len() as i32 != nmcelem + 3) {
        numbers = None;
    }
    let minfreq: f32 = match numbers {
        Some(n) => n[nmcelem as usize],
        None => 2.0 * DEFAULT_CONTAIN_SEL as f32,
    };

    let use_bsearch = nitems * floor_log2(nmcelem as u32) < nmcelem + nitems;

    // "@>" starts at 1.0 and decreases per element; "&&" starts at 0.0 and
    // increases.
    let mut selec: f64 = if operator == OID_ARRAY_CONTAINS_OP { 1.0 } else { 0.0 };

    let mut mcelem_index = 0usize;
    for i in 0..array_data.len() {
        let mut matched = false;

        // Ignore duplicates in the array data.
        if i > 0 && cmp.cmp(array_data[i - 1], array_data[i])? == 0 {
            continue;
        }

        // Find the smallest MCELEM >= this array item.
        if use_bsearch {
            matched = find_next_mcelem(cmp, mcelem, array_data[i], &mut mcelem_index)?;
        } else {
            while mcelem_index < mcelem.len() {
                let c = cmp.cmp(mcelem[mcelem_index], array_data[i])?;
                if c < 0 {
                    mcelem_index += 1;
                } else {
                    if c == 0 {
                        matched = true;
                    }
                    break;
                }
            }
        }

        let elem_selec: f64;
        if matched && numbers.is_some() {
            elem_selec = numbers.expect("checked")[mcelem_index] as f64;
            mcelem_index += 1;
        } else {
            // Not in MCELEM: assume selectivity is at most minfreq / 2.
            elem_selec = f64::min(DEFAULT_CONTAIN_SEL, (minfreq / 2.0) as f64);
        }

        if operator == OID_ARRAY_CONTAINS_OP {
            selec *= elem_selec;
        } else {
            selec = selec + elem_selec - selec * elem_selec;
        }
        selec = clamp_probability(selec);
    }

    Ok(selec)
}

// "column <@ const" via MCELEM, corrected by the distinct-element-count
// histogram (see C for the distribution-law derivation). All intermediate
// arithmetic is f32, matching C's float variables.
fn mcelem_array_contained_selec(
    cmp: &mut ElemCmp<'_>,
    mcelem: &[Datum],
    numbers: Option<&[f32]>,
    array_data: &[Datum],
    hist: Option<&[f32]>,
) -> PgResult<f64> {
    let nmcelem = mcelem.len();

    // The element frequencies are required, as is the count histogram.
    let Some(numbers) = numbers.filter(|n| n.len() == nmcelem + 3) else {
        return Ok(DEFAULT_CONTAIN_SEL);
    };
    let Some(hist) = hist.filter(|h| h.len() >= 3) else {
        return Ok(DEFAULT_CONTAIN_SEL);
    };
    let nhist = hist.len();

    let minfreq: f32 = numbers[nmcelem];
    let nullelem_freq: f32 = numbers[nmcelem + 2];
    let avg_count: f32 = hist[nhist - 1];

    // "rest" = sum of frequencies of all elements not in MCELEM; start from
    // the average distinct count (the sum over all elements) and subtract.
    let mut rest: f32 = avg_count;
    // "mult" = probability that each MCELEM absent from the constant does
    // not occur.
    let mut mult: f32 = 1.0;

    let mut elem_selec: Vec<f32> = Vec::with_capacity(array_data.len());

    let mut mcelem_index = 0usize;
    for i in 0..array_data.len() {
        let mut matched = false;

        // Ignore duplicates in the array data.
        if i > 0 && cmp.cmp(array_data[i - 1], array_data[i])? == 0 {
            continue;
        }

        while mcelem_index < nmcelem {
            let c = cmp.cmp(mcelem[mcelem_index], array_data[i])?;
            if c < 0 {
                mult *= 1.0 - numbers[mcelem_index];
                rest -= numbers[mcelem_index];
                mcelem_index += 1;
            } else {
                if c == 0 {
                    matched = true;
                }
                break;
            }
        }

        if matched {
            elem_selec.push(numbers[mcelem_index]);
            // "rest" is decremented for all mcelems, matched or not.
            rest -= numbers[mcelem_index];
            mcelem_index += 1;
        } else {
            elem_selec.push(f64::min(DEFAULT_CONTAIN_SEL, (minfreq / 2.0) as f64) as f32);
        }
    }

    while mcelem_index < nmcelem {
        mult *= 1.0 - numbers[mcelem_index];
        rest -= numbers[mcelem_index];
        mcelem_index += 1;
    }

    // Poisson estimate of zero occurrences of the non-MCELEM rare elements.
    mult = (mult as f64 * (-rest as f64).exp()) as f32;

    // Cap the O(unique_nitems * (nmcelem + unique_nitems)) histogram work at
    // EFFORT * nmcelem by keeping only the most-common constant elements.
    const EFFORT: i32 = 100;
    let mut unique_nitems = elem_selec.len();
    let nm = nmcelem as i32;
    if nm + unique_nitems as i32 > 0
        && unique_nitems as i32 > EFFORT * nm / (nm + unique_nitems as i32)
    {
        // Quadratic formula for the largest allowable N (A=1, B=nmcelem,
        // C=-EFFORT*nmcelem).
        let b = nmcelem as f64;
        let n = ((b * b + 4.0 * EFFORT as f64 * b).sqrt() - b) as i32 / 2;
        elem_selec.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap_or(core::cmp::Ordering::Equal));
        unique_nitems = n as usize;
    }
    let elem_selec = &elem_selec[..unique_nitems];

    // Distinct-count probabilities assuming independent occurrence, for the
    // constant's elements and for the MCELEMs.
    let dist = calc_distr(elem_selec, unique_nitems, unique_nitems, 0.0);
    let mcelem_dist = calc_distr(&numbers[..nmcelem], nmcelem, unique_nitems, rest);

    // hist[nhist-1] is the average, not a histogram member.
    let hist_part = calc_hist(&hist[..nhist - 1], unique_nitems);

    let mut selec: f32 = 0.0;
    for i in 0..=unique_nitems {
        // mult * dist[i] / mcelem_dist[i]: probability of qual match under
        // independence, conditioned on distinct element count = i.
        if mcelem_dist[i] > 0.0 {
            selec += hist_part[i] * mult * dist[i] / mcelem_dist[i];
        }
    }

    // Account for the null-element frequency.
    selec *= 1.0 - nullelem_freq;

    Ok(clamp_probability(selec as f64))
}

// First n distinct-element-count probabilities from the count histogram;
// returns n+1 entries. A box (a,b) spreads 1/((b-a+1)*(nhist-1)) to interior
// values and half that to each bound.
fn calc_hist(hist: &[f32], n: usize) -> Vec<f32> {
    let nhist = hist.len();
    let mut hist_part: Vec<f32> = vec![0.0; n + 1];
    let mut i = 0usize;
    let mut prev_interval: f32 = 0.0;
    let mut next_interval: f32;
    let frac: f32 = 1.0 / (nhist as f32 - 1.0);

    for (k, out) in hist_part.iter_mut().enumerate() {
        // Count histogram boundaries equal to k; fractional entries (roundoff
        // in large values) count toward the next larger k.
        let mut count = 0i32;
        while i < nhist && hist[i] <= k as f32 {
            count += 1;
            i += 1;
        }

        if count > 0 {
            // k is an exact bound for at least one box.
            next_interval = if i < nhist { hist[i] - hist[i - 1] } else { 0.0 };
            // count-1 boxes contain k exclusively; add the partial boxes on
            // either side.
            let mut val = (count - 1) as f32;
            if next_interval > 0.0 {
                val += 0.5 / next_interval;
            }
            if prev_interval > 0.0 {
                val += 0.5 / prev_interval;
            }
            *out = frac * val;
            prev_interval = next_interval;
        } else if prev_interval > 0.0 {
            *out = frac / prev_interval;
        } else {
            *out = 0.0;
        }
    }

    hist_part
}

// Probabilities of exactly k of n independent events for k in [0..m], with
// "rest" the summed probability of unlisted rare events (Poisson-convolved).
fn calc_distr(p: &[f32], n: usize, m: usize, rest: f32) -> Vec<f32> {
    let mut row: Vec<f32> = vec![0.0; m + 1];
    let mut prev_row: Vec<f32> = vec![0.0; m + 1];

    // M[0,0] = 1; M[i,j] = M[i-1,j]*(1-p[i]) + M[i-1,j-1]*p[i].
    row[0] = 1.0;
    for i in 1..=n {
        let t = p[i - 1];
        core::mem::swap(&mut row, &mut prev_row);
        for j in 0..=usize::min(i, m) {
            let mut val: f32 = 0.0;
            if j < i {
                val += prev_row[j] * (1.0 - t);
            }
            if j > 0 {
                val += prev_row[j - 1] * t;
            }
            row[j] = val;
        }
    }

    if rest > DEFAULT_CONTAIN_SEL as f32 {
        core::mem::swap(&mut row, &mut prev_row);
        for v in row.iter_mut() {
            *v = 0.0;
        }
        // Poisson value for 0 occurrences, then convolve.
        let mut t: f32 = (-rest as f64).exp() as f32;
        for i in 0..=m {
            for j in 0..=(m - i) {
                row[j + i] += prev_row[j] * t;
            }
            t *= rest / (i as f32 + 1.0);
        }
    }

    row
}

fn floor_log2(n: u32) -> i32 {
    let mut n = n;
    let mut logval = 0;
    if n == 0 {
        return -1;
    }
    if n >= 1 << 16 {
        n >>= 16;
        logval += 16;
    }
    if n >= 1 << 8 {
        n >>= 8;
        logval += 8;
    }
    if n >= 1 << 4 {
        n >>= 4;
        logval += 4;
    }
    if n >= 1 << 2 {
        n >>= 2;
        logval += 2;
    }
    if n >= 1 << 1 {
        logval += 1;
    }
    logval
}

// Binary search from *index for the first mcelem >= value; true on exact
// match (mcelems are distinct).
fn find_next_mcelem(
    cmp: &mut ElemCmp<'_>,
    mcelem: &[Datum],
    value: Datum,
    index: &mut usize,
) -> PgResult<bool> {
    let mut l = *index as i32;
    let mut r = mcelem.len() as i32 - 1;
    while l <= r {
        let i = (l + r) / 2;
        let res = cmp.cmp(mcelem[i as usize], value)?;
        if res == 0 {
            *index = i as usize;
            return Ok(true);
        } else if res < 0 {
            l = i + 1;
        } else {
            r = i - 1;
        }
    }
    *index = l as usize;
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_log2_matches_c() {
        assert_eq!(floor_log2(0), -1);
        assert_eq!(floor_log2(1), 0);
        assert_eq!(floor_log2(2), 1);
        assert_eq!(floor_log2(3), 1);
        assert_eq!(floor_log2(4), 2);
        assert_eq!(floor_log2(1 << 16), 16);
        assert_eq!(floor_log2(u32::MAX), 31);
    }

    #[test]
    fn calc_distr_binomial() {
        // Two independent p=0.5 events: [1/4, 1/2, 1/4].
        let d = calc_distr(&[0.5, 0.5], 2, 2, 0.0);
        assert_eq!(d, vec![0.25, 0.5, 0.25]);
    }

    #[test]
    fn calc_distr_poisson_rest() {
        // No listed events; rest=1.0 Poisson: P(0)=P(1)=e^-1.
        let d = calc_distr(&[], 0, 1, 1.0);
        let e1 = ((-1.0f64).exp()) as f32;
        assert!((d[0] - e1).abs() < 1e-7);
        assert!((d[1] - e1).abs() < 1e-7);
    }

    #[test]
    fn calc_hist_unit_boxes() {
        // Bounds {1,2,3}: each of the 2 boxes spreads frac=0.5, half to each
        // exact bound.
        let h = calc_hist(&[1.0, 2.0, 3.0], 3);
        assert_eq!(h, vec![0.0, 0.25, 0.5, 0.25]);
    }
}
