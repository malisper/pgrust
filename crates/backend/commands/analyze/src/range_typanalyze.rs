//! rangetypes_typanalyze.c: compute_range_stats for range and multirange
//! columns (bounds histogram, length histogram, empty fraction).

use adt_rangetypes::{range_cmp_bounds, RangeBound, RangeInfo};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use crate::{varlena_stored_size, VacAttrStats};

pub(crate) const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
pub(crate) const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;
const FLOAT8OID: Oid = 701;
const FLOAT8_LESS_OPERATOR: Oid = 672;

// range_typanalyze / multirange_typanalyze (setup halves): target, minrows.
pub(crate) fn setup(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    if stats.attstattarget < 0 {
        stats.attstattarget = guc_tables::vars::default_statistics_target.read();
    }
    stats.minrows = 300 * stats.attstattarget;
    Ok(true)
}

struct RangeCtx {
    ri: RangeInfo,
    subdiff: Option<FmgrInfo>,
}

fn range_ctx(base_typid: Oid, is_multirange: bool) -> PgResult<RangeCtx> {
    let e = if is_multirange {
        let e =
            typcache::lookup_type_cache(base_typid, typcache::TYPECACHE_MULTIRANGE_INFO)?;
        e.rngtype().expect("multirange typcache carries its range type")
    } else {
        typcache::lookup_type_cache(base_typid, typcache::TYPECACHE_RANGE_INFO)?
    };
    let subdiff = {
        let f = e.rng_subdiff_finfo();
        if f.fn_oid != 0 { Some(f.clone()) } else { None }
    };
    Ok(RangeCtx { ri: RangeInfo::from_entry(e)?, subdiff })
}

// DatumGetRangeTypeP/DatumGetMultirangeTypeP: 4-byte-header image.
fn detoasted_image<'m>(mcx: Mcx<'m>, value: Datum) -> PgResult<&'m [u8]> {
    let p = value.as_usize() as *const u8;
    // SAFETY: live varlena datum; varsize_any covers 1B/4B headers, toast
    // pointers are rejected below by detoast_attr's caller contract.
    let raw = unsafe {
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    };
    if raw[0] & 0x03 == 0 {
        let copy = mcx::slice_borrow_in(mcx, raw)?;
        Ok(copy)
    } else {
        let v = detoast_seams::detoast_attr::call(mcx, raw)?;
        Ok(adt_multirangetypes::leak_image(v))
    }
}

pub(crate) fn compute_range_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    stats: &mut VacAttrStats<'mcx>,
    is_multirange: bool,
    src: &crate::FetchSource<'_, '_>,
    samplerows: i32,
    _totalrows: f64,
) -> PgResult<()> {
    let base_typid = lsyscache::getBaseType(stats.attrtypid)?;
    let mut ctx = range_ctx(base_typid, is_multirange)?;
    // Bump scratch: detoasted images and subdiff results leak by design and
    // an exact-accounting context would assert at reset.
    let scratch = MemoryContext::new_bump("compute_range_stats scratch");
    let col_mcx = scratch.mcx();
    let has_subdiff = ctx.subdiff.is_some();

    let mut null_cnt = 0i32;
    let mut non_null_cnt = 0i32;
    let mut non_empty_cnt = 0i32;
    let mut empty_cnt = 0i32;
    let num_bins = stats.attstattarget;
    let mut total_width = 0.0f64;

    let mut lowers: PgVec<'_, RangeBound> =
        mcx::vec_with_capacity_in(col_mcx, samplerows as usize)?;
    let mut uppers: PgVec<'_, RangeBound> =
        mcx::vec_with_capacity_in(col_mcx, samplerows as usize)?;
    let mut lengths: PgVec<'_, f64> =
        mcx::vec_with_capacity_in(col_mcx, samplerows as usize)?;

    for rowno in 0..samplerows as usize {
        let (value, isnull) = src.fetch(rowno, stats.tupattnum);
        if isnull {
            null_cnt += 1;
            continue;
        }
        total_width += varlena_stored_size(value) as f64;

        let img = detoasted_image(col_mcx, value)?;
        let (lower, upper, empty) = if is_multirange {
            let count = adt_multirangetypes::multirange_count(img) as usize;
            if count > 0 {
                let (lower, _) =
                    adt_multirangetypes::multirange_get_bounds(&ctx.ri, img, 0);
                let (_, upper) =
                    adt_multirangetypes::multirange_get_bounds(&ctx.ri, img, count - 1);
                (lower, upper, false)
            } else {
                let dummy = RangeBound {
                    val: Datum::from_usize(0),
                    infinite: false,
                    inclusive: false,
                    lower: true,
                };
                (dummy, dummy, true)
            }
        } else {
            adt_rangetypes::range_deserialize(&ctx.ri.elem, img)
        };

        if !empty {
            let length = if lower.infinite || upper.infinite {
                f64::INFINITY
            } else if has_subdiff {
                types_fmgr::function_call2_coll_in(
                    ctx.subdiff.as_mut().unwrap(),
                    ctx.ri.collation,
                    col_mcx,
                    upper.val,
                    lower.val,
                )?
                .as_f64()
            } else {
                1.0
            };
            lowers.push(lower);
            uppers.push(upper);
            lengths.push(length);
            non_empty_cnt += 1;
        } else {
            empty_cnt += 1;
        }
        non_null_cnt += 1;
    }

    let mut slot_idx = 0usize;
    if non_null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = (total_width / non_null_cnt as f64) as i32;
        stats.stadistinct = (-1.0 * (1.0 - stats.stanullfrac as f64)) as f32;

        if non_empty_cnt >= 2 {
            let mut cmp_bound = |a: &RangeBound, b: &RangeBound| {
                range_cmp_bounds(col_mcx, &mut ctx.ri, a, b)
                    .unwrap_or_else(|e| panic!("compute_range_stats: bound cmp failed: {e:?}"))
                    .cmp(&0)
            };
            lowers.sort_unstable_by(|a, b| cmp_bound(a, b));
            uppers.sort_unstable_by(|a, b| cmp_bound(a, b));

            let mut num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }
            let mut bound_hist_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_hist as usize)?;
            let delta = (non_empty_cnt - 1) / (num_hist - 1);
            let deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;
            for _ in 0..num_hist {
                let mut lo = lowers[pos as usize];
                let mut up = uppers[pos as usize];
                let img = adt_rangetypes::range_serialize(
                    anl_mcx, &mut ctx.ri, &mut lo, &mut up, false, None,
                )?
                .expect("histogram range never soft-fails");
                bound_hist_values
                    .push(Datum::from_usize(adt_multirangetypes::leak_image(img).as_ptr()
                        as usize));
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= num_hist - 1 {
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_BOUNDS_HISTOGRAM;
            stats.stavalues[slot_idx] = bound_hist_values;
            stats.stavalues_set[slot_idx] = true;
            stats.statypid[slot_idx] = ctx.ri.rngtypid;
            stats.statyplen[slot_idx] = ctx.ri.own_typlen;
            stats.statypbyval[slot_idx] = ctx.ri.own_typbyval;
            stats.statypalign[slot_idx] = ctx.ri.own_typalign;
            slot_idx += 1;
        }

        let mut length_hist_values: PgVec<'mcx, Datum> = PgVec::new_in(anl_mcx);
        if non_empty_cnt >= 2 {
            // float8_qsort_cmp's exact shape (NaN falls to the > arm, as C).
            lengths.sort_unstable_by(|a, b| {
                if a < b {
                    core::cmp::Ordering::Less
                } else if a == b {
                    core::cmp::Ordering::Equal
                } else {
                    core::cmp::Ordering::Greater
                }
            });
            let mut num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }
            length_hist_values = mcx::vec_with_capacity_in(anl_mcx, num_hist as usize)?;
            let delta = (non_empty_cnt - 1) / (num_hist - 1);
            let deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;
            for _ in 0..num_hist {
                length_hist_values.push(Datum::from_f64(lengths[pos as usize]));
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= num_hist - 1 {
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
            }
        }
        stats.staop[slot_idx] = FLOAT8_LESS_OPERATOR;
        stats.stacoll[slot_idx] = 0;
        stats.stavalues[slot_idx] = length_hist_values;
        stats.stavalues_set[slot_idx] = true;
        stats.statypid[slot_idx] = FLOAT8OID;
        stats.statyplen[slot_idx] = 8;
        stats.statypbyval[slot_idx] = true;
        stats.statypalign[slot_idx] = b'd';

        let mut emptyfrac: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, 1)?;
        emptyfrac.push((empty_cnt as f64 / non_null_cnt as f64) as f32);
        stats.stanumbers[slot_idx] = emptyfrac;
        stats.stakind[slot_idx] = STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM;
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = 0;
        stats.stadistinct = 0.0;
    }
    Ok(())
}
