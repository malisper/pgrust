//! array_typanalyze.c: compute_array_stats — MCELEM most-common-elements via
//! Lossy Counting plus DECHIST distinct-element-count histogram.

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgFxHashMap, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};

use crate::{ComputeStats, FetchSource, StdCompute, VacAttrStats, STATISTIC_NUM_SLOTS};

pub(crate) const STATISTIC_KIND_MCELEM: i16 = 4;
pub(crate) const STATISTIC_KIND_DECHIST: i16 = 5;
const ARRAY_WIDTH_THRESHOLD: usize = 0x10000;

const ELEM_TYPECACHE_FLAGS: i32 = typcache::TYPECACHE_EQ_OPR
    | typcache::TYPECACHE_CMP_PROC_FINFO
    | typcache::TYPECACHE_HASH_PROC_FINFO;

pub(crate) fn setup(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    if !crate::std_typanalyze(stats)? {
        return Ok(false);
    }
    let element_typeid = lsyscache::get_base_element_type(stats.attrtypid)?;
    if element_typeid == InvalidOid {
        return Err(PgError::error(format!(
            "array_typanalyze was invoked for non-array type {}",
            stats.attrtypid
        ))
        .into());
    }
    let entry = typcache::lookup_type_cache(element_typeid, ELEM_TYPECACHE_FLAGS)?;
    if entry.eq_opr() == InvalidOid
        || entry.cmp_proc_finfo().fn_oid == InvalidOid
        || entry.hash_proc_finfo().fn_oid == InvalidOid
    {
        return Ok(true);
    }
    let std = match stats.compute {
        ComputeStats::Scalar => StdCompute::Scalar,
        ComputeStats::Distinct => StdCompute::Distinct,
        _ => StdCompute::Trivial,
    };
    stats.compute = ComputeStats::Array { std, elem_typeid: element_typeid };
    Ok(true)
}

#[derive(Clone, Copy)]
struct TrackItem {
    key: Datum,
    hash: u32,
    frequency: i32,
    delta: i32,
    last_container: i32,
}

type Buckets<'col> = PgFxHashMap<'col, u32, PgVec<'col, i32>>;

#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_array_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    col_mcx: Mcx<'_>,
    stats: &mut VacAttrStats<'mcx>,
    std: StdCompute,
    elem_typeid: Oid,
    src: &FetchSource<'_, '_>,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    match std {
        StdCompute::Scalar => {
            crate::compute_scalar_stats(anl_mcx, col_mcx, stats, src, samplerows, totalrows)?
        }
        StdCompute::Distinct => {
            crate::compute_distinct_stats(anl_mcx, col_mcx, stats, src, samplerows, totalrows)?
        }
        StdCompute::Trivial => crate::compute_trivial_stats(stats, src, samplerows)?,
    }

    let entry = typcache::lookup_type_cache(elem_typeid, ELEM_TYPECACHE_FLAGS)?;
    let eq_opr = entry.eq_opr();
    let coll_id = stats.attrcollid;
    let typbyval = entry.typbyval();
    let typlen = entry.typlen();
    let typalign = entry.typalign() as u8;

    // Finfo copies: element functions may re-enter typcache (range_cmp/
    // record_cmp fn_extra fills), so the entry RefCells must stay unborrowed
    // across the calls.
    let hash_finfo = core::cell::RefCell::new(entry.hash_proc_finfo().clone());
    let cmp_finfo = core::cell::RefCell::new(entry.cmp_proc_finfo().clone());
    let hash_elem = |d: Datum| -> u32 {
        let mut finfo = hash_finfo.borrow_mut();
        types_fmgr::function_call1_coll_in(&mut finfo, coll_id, col_mcx, d)
            .unwrap_or_else(|e| panic!("compute_array_stats: element hash failed: {e:?}"))
            .as_u32()
    };
    let cmp_elems = |a: Datum, b: Datum| -> i32 {
        let mut finfo = cmp_finfo.borrow_mut();
        types_fmgr::function_call2_coll_in(&mut finfo, coll_id, col_mcx, a, b)
            .unwrap_or_else(|e| panic!("compute_array_stats: element cmp failed: {e:?}"))
            .as_i32()
    };

    let num_mcelem_target = stats.attstattarget * 10;
    let bucket_width = num_mcelem_target * 1000 / 7;

    let mut items: PgVec<'_, TrackItem> = PgVec::new_in(col_mcx);
    let mut buckets: Buckets<'_> = PgFxHashMap::with_hasher_in(Default::default(), col_mcx);
    let mut count_tab: PgFxHashMap<'_, i32, i32> =
        PgFxHashMap::with_hasher_in(Default::default(), col_mcx);

    let mut b_current = 1i32;
    let mut element_no = 0i64;
    let mut null_elem_cnt = 0i32;
    let mut analyzed_rows = 0i32;

    let mut row_scratch = MemoryContext::new_bump("compute_array_stats row scratch");
    for array_no in 0..samplerows {
        let (value, isnull) = src.fetch(array_no as usize, stats.tupattnum);
        if isnull {
            continue;
        }
        {
            let row_mcx = row_scratch.mcx();
            let p = value.as_usize() as *const u8;
            // SAFETY: non-null varlena datum readable through its header.
            let raw =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            if detoast::toast_raw_datum_size(raw) > ARRAY_WIDTH_THRESHOLD {
                continue;
            }
            analyzed_rows += 1;

            let img: &[u8] = if raw[0] & 0x03 == 0 {
                raw
            } else {
                let v = detoast_seams::detoast_attr::call(row_mcx, raw)?;
                adt_multirangetypes::leak_image(v)
            };
            debug_assert_eq!(arrayfuncs::arr_elemtype(img), elem_typeid);
            let (elem_values, elem_nulls) = arrayfuncs::deconstruct_array(
                row_mcx,
                img,
                typlen as i32,
                typbyval,
                typalign,
                true,
            )?;

            let prev_element_no = element_no;
            let mut null_present = false;
            for (j, &elem_value) in elem_values.iter().enumerate() {
                if elem_nulls[j] {
                    null_present = true;
                    continue;
                }
                let h = hash_elem(elem_value);
                let bucket = buckets.entry(h).or_insert_with(|| PgVec::new_in(col_mcx));
                let found = bucket
                    .iter()
                    .copied()
                    .find(|&idx| cmp_elems(items[idx as usize].key, elem_value) == 0);
                match found {
                    Some(idx) => {
                        let it = &mut items[idx as usize];
                        if it.last_container == array_no {
                            continue;
                        }
                        it.frequency += 1;
                        it.last_container = array_no;
                    }
                    None => {
                        let key = crate::datum_copy_in(col_mcx, elem_value, typbyval, typlen)?;
                        bucket.push(items.len() as i32);
                        items.push(TrackItem {
                            key,
                            hash: h,
                            frequency: 1,
                            delta: b_current - 1,
                            last_container: array_no,
                        });
                    }
                }
                element_no += 1;
                if element_no % bucket_width as i64 == 0 {
                    prune_element_table(&mut items, &mut buckets, b_current, col_mcx);
                    b_current += 1;
                }
            }

            if null_present {
                null_elem_cnt += 1;
            }
            let distinct_count = (element_no - prev_element_no) as i32;
            *count_tab.entry(distinct_count).or_insert(0) += 1;
        }
        row_scratch.reset();
    }

    let mut slot_idx = 0usize;
    while slot_idx < STATISTIC_NUM_SLOTS && stats.stakind[slot_idx] != 0 {
        slot_idx += 1;
    }
    if slot_idx > STATISTIC_NUM_SLOTS - 2 {
        return Err(
            PgError::error("insufficient pg_statistic slots for array stats").into(),
        );
    }

    if analyzed_rows > 0 {
        let nonnull_cnt = analyzed_rows;
        let cutoff_freq = 9 * element_no / bucket_width as i64;

        let mut sort_idx: PgVec<'_, i32> = mcx::vec_with_capacity_in(col_mcx, items.len())?;
        let mut minfreq = element_no;
        let mut maxfreq = 0i64;
        for (i, it) in items.iter().enumerate() {
            if it.frequency as i64 > cutoff_freq {
                sort_idx.push(i as i32);
                minfreq = minfreq.min(it.frequency as i64);
                maxfreq = maxfreq.max(it.frequency as i64);
            }
        }
        let track_len = sort_idx.len() as i32;

        let mut num_mcelem = num_mcelem_target;
        if num_mcelem < track_len {
            // C's qsort tie order is hash-iteration-dependent; ties at the
            // truncation boundary can keep a different (equal-frequency) set.
            sort_idx.sort_unstable_by(|&a, &b| {
                items[b as usize].frequency.cmp(&items[a as usize].frequency)
            });
            minfreq = items[sort_idx[num_mcelem as usize - 1] as usize].frequency as i64;
        } else {
            num_mcelem = track_len;
        }

        if num_mcelem > 0 {
            let prefix = &mut sort_idx[..num_mcelem as usize];
            prefix.sort_unstable_by(|&a, &b| {
                cmp_elems(items[a as usize].key, items[b as usize].key).cmp(&0)
            });

            let mut mcelem_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcelem as usize)?;
            let mut mcelem_freqs: PgVec<'mcx, f32> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcelem as usize + 3)?;
            for &idx in prefix.iter() {
                let it = &items[idx as usize];
                mcelem_values.push(crate::datum_copy_in(anl_mcx, it.key, typbyval, typlen)?);
                mcelem_freqs.push((it.frequency as f64 / nonnull_cnt as f64) as f32);
            }
            mcelem_freqs.push((minfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((maxfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((null_elem_cnt as f64 / nonnull_cnt as f64) as f32);

            stats.stakind[slot_idx] = STATISTIC_KIND_MCELEM;
            stats.staop[slot_idx] = eq_opr;
            stats.stacoll[slot_idx] = coll_id;
            stats.stanumbers[slot_idx] = mcelem_freqs;
            stats.stavalues[slot_idx] = mcelem_values;
            stats.stavalues_set[slot_idx] = true;
            stats.statypid[slot_idx] = elem_typeid;
            stats.statyplen[slot_idx] = typlen;
            stats.statypbyval[slot_idx] = typbyval;
            stats.statypalign[slot_idx] = typalign;
            slot_idx += 1;
        }

        if !count_tab.is_empty() {
            let num_hist = stats.attstattarget.max(2);
            let mut sorted_counts: PgVec<'_, (i32, i32)> =
                mcx::vec_with_capacity_in(col_mcx, count_tab.len())?;
            for (&count, &freq) in count_tab.iter() {
                sorted_counts.push((count, freq));
            }
            sorted_counts.sort_unstable_by_key(|&(c, _)| c);

            let mut hist: PgVec<'mcx, f32> =
                mcx::vec_with_capacity_in(anl_mcx, num_hist as usize + 1)?;
            let delta = (analyzed_rows - 1) as i64;
            let mut j = 0usize;
            let mut frac = sorted_counts[0].1 as i64 * (num_hist as i64 - 1);
            for _ in 0..num_hist {
                while frac <= 0 {
                    j += 1;
                    frac += sorted_counts[j].1 as i64 * (num_hist as i64 - 1);
                }
                hist.push(sorted_counts[j].0 as f32);
                frac -= delta;
            }
            debug_assert_eq!(j, sorted_counts.len() - 1);
            hist.push((element_no as f64 / nonnull_cnt as f64) as f32);

            stats.stakind[slot_idx] = STATISTIC_KIND_DECHIST;
            stats.staop[slot_idx] = eq_opr;
            stats.stacoll[slot_idx] = coll_id;
            stats.stanumbers[slot_idx] = hist;
        }
    }
    Ok(())
}

// Pruned keys stay in the column bump until its reset (C pfrees per prune);
// the retained-set bound is C's, the leak is bounded by total sampled bytes.
fn prune_element_table<'col>(
    items: &mut PgVec<'col, TrackItem>,
    buckets: &mut Buckets<'col>,
    b_current: i32,
    col_mcx: Mcx<'col>,
) {
    items.retain(|it| it.frequency + it.delta > b_current);
    buckets.clear();
    for (i, it) in items.iter().enumerate() {
        buckets
            .entry(it.hash)
            .or_insert_with(|| PgVec::new_in(col_mcx))
            .push(i as i32);
    }
}
