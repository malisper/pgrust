//! Port of the multi-sort statistics-build framework of
//! `backend/statistics/extended_stats.c` (the multi-dimensional sort support
//! `multi_sort_*` / `build_sorted_items` / `build_attnums_array` /
//! `compare_*`), plus the two per-combination build kernels
//! `ndistinct_for_combination` (mvdistinct.c) and `dependency_degree`
//! (dependencies.c).
//!
//! This crate is the owner of `backend-statistics-core-seams`' build kernels:
//! it installs `ndistinct_for_combination`, `dependency_degree` and
//! `mcv_lookup_lt_opr` from [`init_seams`]. Those seams are consumed by the
//! per-kind slices (`backend-statistics-mvdistinct` /
//! `backend-statistics-dependencies` / `backend-statistics-mcv`), whose
//! `statext_*_build` control flow drives them, and ultimately by the ANALYZE
//! build leg in `backend-statistics-extended-stats`.
//!
//! The multi-sort helpers ([`MultiSortSupport`], [`build_sorted_items`],
//! [`build_attnums_array`]) are `pub` so the extended-stats build leg can reuse
//! them, exactly as `extended_stats_internal.h` exports them in C.

#![allow(non_snake_case)]
#![allow(clippy::needless_range_loop)]

use ::mcx::Mcx;
use ::types_core::{AttrNumber, Oid};
use ::types_error::{PgError, PgResult};
use ::types_sortsupport::SortSupportData;
use ::statistics::{SortItem, StatsBuildData};
use ::types_tuple::Datum;

use ::parse_oper::get_sort_group_operators;
use ::sortsupport_seams::{apply_sort_comparator, prepare_sort_support_from_ordering_op};
use ::lsyscache_seams::get_typlen;
use ::detoast_seams::{pg_detoast_datum_packed, toast_raw_datum_size};

use statistics_core_seams as seams;

/// `WIDTH_THRESHOLD` (analyze.c:91) — values wider than this are not detoasted
/// for the multi-sort build (mirrored by `build_sorted_items`).
const WIDTH_THRESHOLD: i64 = 1024;

/// `MultiSortSupport` (extended_stats_internal.h) — per-dimension sort support
/// for a multi-column comparison. The flexible `ssup[]` array becomes an owned
/// `Vec<SortSupportData>` of length `ndims`.
pub struct MultiSortSupport<'mcx> {
    pub ndims: i32,
    pub ssup: Vec<SortSupportData<'mcx>>,
}

impl<'mcx> MultiSortSupport<'mcx> {
    /// `multi_sort_init(int ndims)` (extended_stats.c:829).
    pub fn init(mcx: Mcx<'mcx>, ndims: i32) -> Self {
        debug_assert!(ndims >= 2);
        let mut ssup = Vec::with_capacity(ndims as usize);
        for _ in 0..ndims {
            ssup.push(SortSupportData::new(mcx));
        }
        MultiSortSupport { ndims, ssup }
    }

    /// `multi_sort_add_dimension(mss, sortdim, oper, collation)`
    /// (extended_stats.c:847).
    pub fn add_dimension(&mut self, sortdim: i32, oper: Oid, collation: Oid) -> PgResult<()> {
        let ssup = &mut self.ssup[sortdim as usize];
        ssup.ssup_collation = collation;
        ssup.ssup_nulls_first = false;
        prepare_sort_support_from_ordering_op::call(oper, ssup)
    }

    /// `multi_sort_compare(a, b, mss)` (extended_stats.c:862).
    pub fn compare(&self, a: &SortItem<'mcx>, b: &SortItem<'mcx>) -> PgResult<i32> {
        multi_cmp_items(a, b, self)
    }

    /// `multi_sort_compare_dim(dim, a, b, mss)` (extended_stats.c:887).
    pub fn compare_dim(&self, dim: i32, a: &SortItem<'mcx>, b: &SortItem<'mcx>) -> PgResult<i32> {
        cmp_dim_items(dim, a, b, self)
    }

    /// `multi_sort_compare_dims(start, end, a, b, mss)` (extended_stats.c:896).
    pub fn compare_dims(
        &self,
        start: i32,
        end: i32,
        a: &SortItem<'mcx>,
        b: &SortItem<'mcx>,
    ) -> PgResult<i32> {
        cmp_dims_items(start, end, a, b, self)
    }
}

/// `ApplySortComparator(a, isnull_a, b, isnull_b, ssup)` for one dimension,
/// folding the explicit per-item null flag into the comparison. With
/// `ssup_nulls_first == false` (the only mode the extended-stats build uses),
/// NULLs sort last.
fn cmp_with_nulls<'mcx>(
    ssup: &SortSupportData<'mcx>,
    av: &Datum<'mcx>,
    anull: bool,
    bv: &Datum<'mcx>,
    bnull: bool,
) -> PgResult<i32> {
    if anull {
        if bnull {
            return Ok(0);
        }
        return Ok(1);
    } else if bnull {
        return Ok(-1);
    }
    apply_sort_comparator::call(av.clone(), bv.clone(), ssup)
}

/// `build_attnums_array(attrs, nexprs, numattrs)` (extended_stats.c:938) —
/// transform a sorted attnum list into an array of `AttrNumber`. `attrs` is the
/// already-decoded sorted attnum list (the bitmapset members, ascending).
pub fn build_attnums_array(attrs: &[i32], nexprs: i32) -> Vec<AttrNumber> {
    let mut attnums: Vec<AttrNumber> = Vec::with_capacity(attrs.len());
    for &j in attrs {
        let attnum = j - nexprs;
        debug_assert!(attnum != 0);
        attnums.push(attnum as AttrNumber);
    }
    attnums
}

/// `build_sorted_items(data, nitems, mss, numattrs, attnums)`
/// (extended_stats.c:983) — build a sorted array of `SortItem` with the values
/// of the requested `attnums` from the sampled rows. Returns `None` (C `NULL`)
/// when every item was too wide.
pub fn build_sorted_items<'mcx>(
    mcx: Mcx<'mcx>,
    data: &StatsBuildData<'mcx>,
    mss: &MultiSortSupport<'mcx>,
    numattrs: i32,
    attnums: &[AttrNumber],
) -> PgResult<Option<Vec<SortItem<'mcx>>>> {
    let numattrs = numattrs as usize;

    let mut typlen: Vec<i32> = Vec::with_capacity(data.nattnums as usize);
    for i in 0..data.nattnums as usize {
        typlen.push(get_typlen::call(data.stats[i].attrtypid)? as i32);
    }

    let mut items: Vec<SortItem<'mcx>> = Vec::with_capacity(data.numrows as usize);

    let mut nrows = 0usize;
    for i in 0..data.numrows as usize {
        let mut toowide = false;
        let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(numattrs);
        let mut isnull: Vec<bool> = Vec::with_capacity(numattrs);

        for j in 0..numattrs {
            let attnum = attnums[j];

            let mut idx = 0usize;
            while idx < data.nattnums as usize {
                if attnum == data.attnums[idx] {
                    break;
                }
                idx += 1;
            }
            debug_assert!(idx < data.nattnums as usize);

            let mut value = data.values[idx][i].clone();
            let isn = data.nulls[idx][i];
            let attlen = typlen[idx];

            if !isn && attlen == -1 {
                if toast_raw_datum_size::call(mcx, value.clone())? > WIDTH_THRESHOLD {
                    toowide = true;
                    break;
                }
                // value = PointerGetDatum(PG_DETOAST_DATUM(value)): detoast the
                // varlena bytes and re-wrap as a by-reference Datum.
                let detoasted =
                    pg_detoast_datum_packed::call(mcx, value.as_varlena_bytes().as_ref())?;
                value = Datum::from_byref_bytes_in(mcx, detoasted.as_slice())?;
            }

            values.push(value);
            isnull.push(isn);
        }

        if toowide {
            continue;
        }

        items.push(SortItem {
            values,
            isnull,
            count: 0,
        });
        nrows += 1;
    }

    if nrows == 0 {
        return Ok(None);
    }

    items.truncate(nrows);
    sort_items(mcx, &mut items, mss)?;
    Ok(Some(items))
}

/// `qsort_interruptible(items, ..., multi_sort_compare, mss)` — a fallible sort
/// of `SortItem`s, implemented as a bottom-up merge sort so the comparator's
/// `Err` surface propagates.
fn sort_items<'mcx>(
    _mcx: Mcx<'mcx>,
    items: &mut Vec<SortItem<'mcx>>,
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<()> {
    let n = items.len();
    let mut idx: Vec<usize> = (0..n).collect();
    merge_sort(items, mss, &mut idx)?;
    let sorted: Vec<SortItem<'mcx>> = idx.iter().map(|&i| items[i].clone()).collect();
    *items = sorted;
    Ok(())
}

fn merge_sort<'mcx>(
    items: &[SortItem<'mcx>],
    mss: &MultiSortSupport<'mcx>,
    idx: &mut Vec<usize>,
) -> PgResult<()> {
    let n = idx.len();
    if n <= 1 {
        return Ok(());
    }
    let mid = n / 2;
    let mut left: Vec<usize> = idx[..mid].to_vec();
    let mut right: Vec<usize> = idx[mid..].to_vec();
    merge_sort(items, mss, &mut left)?;
    merge_sort(items, mss, &mut right)?;
    let (mut i, mut j, mut k) = (0usize, 0usize, 0usize);
    while i < left.len() && j < right.len() {
        let cmp = multi_cmp_items(&items[left[i]], &items[right[j]], mss)?;
        if cmp <= 0 {
            idx[k] = left[i];
            i += 1;
        } else {
            idx[k] = right[j];
            j += 1;
        }
        k += 1;
    }
    while i < left.len() {
        idx[k] = left[i];
        i += 1;
        k += 1;
    }
    while j < right.len() {
        idx[k] = right[j];
        j += 1;
        k += 1;
    }
    Ok(())
}

fn multi_cmp_items<'mcx>(
    a: &SortItem<'mcx>,
    b: &SortItem<'mcx>,
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<i32> {
    for i in 0..mss.ndims as usize {
        let compare = cmp_with_nulls(
            &mss.ssup[i],
            &a.values[i],
            a.isnull[i],
            &b.values[i],
            b.isnull[i],
        )?;
        if compare != 0 {
            return Ok(compare);
        }
    }
    Ok(0)
}

fn cmp_dim_items<'mcx>(
    dim: i32,
    a: &SortItem<'mcx>,
    b: &SortItem<'mcx>,
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<i32> {
    let d = dim as usize;
    cmp_with_nulls(&mss.ssup[d], &a.values[d], a.isnull[d], &b.values[d], b.isnull[d])
}

fn cmp_dims_items<'mcx>(
    start: i32,
    end: i32,
    a: &SortItem<'mcx>,
    b: &SortItem<'mcx>,
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<i32> {
    for dim in start..=end {
        let r = cmp_dim_items(dim, a, b, mss)?;
        if r != 0 {
            return Ok(r);
        }
    }
    Ok(0)
}

/// `lookup_type_cache(typid, TYPECACHE_LT_OPR)->lt_opr` (mcv.c / mvdistinct.c /
/// dependencies.c). Resolves a type's default btree "<" operator OID; errors
/// when the type has no "<" operator, exactly as the kernels do.
fn lookup_lt_opr(typid: Oid) -> PgResult<Oid> {
    let ops = get_sort_group_operators(typid, false, false, false, false)?;
    if ops.lt_opr == 0 {
        return Err(PgError::error(format!(
            "cache lookup failed for ordering operator for type {typid}"
        )));
    }
    Ok(ops.lt_opr)
}

/// `ndistinct_for_combination(totalrows, data, k, combination)`
/// (mvdistinct.c:424). Build the per-row `values[]`/`isnull[]` sort buffer, set
/// up the multi-sort support per dimension, sort, and count distinct
/// combinations (Duj1 estimator). `combination` is the `k`-tuple of zero-based
/// column indexes into the statistics object.
fn ndistinct_for_combination<'mcx>(
    totalrows: f64,
    data: &StatsBuildData<'mcx>,
    k: i32,
    combination: &[i32],
) -> PgResult<f64> {
    let numrows = data.numrows;
    let mcx = data_mcx(data);
    let mut mss = MultiSortSupport::init(mcx, k);

    let mut items: Vec<SortItem<'mcx>> = Vec::with_capacity(numrows as usize);
    for _ in 0..numrows {
        items.push(SortItem {
            values: Vec::with_capacity(k as usize),
            isnull: Vec::with_capacity(k as usize),
            count: 0,
        });
    }

    for i in 0..k as usize {
        let colstat = &data.stats[combination[i] as usize];
        let typid = colstat.attrtypid;
        let collid = colstat.attrcollid;

        let lt_opr = lookup_lt_opr(typid)?;
        mss.add_dimension(i as i32, lt_opr, collid)?;

        for j in 0..numrows as usize {
            items[j]
                .values
                .push(data.values[combination[i] as usize][j].clone());
            items[j].isnull.push(data.nulls[combination[i] as usize][j]);
        }
    }

    sort_items(mcx, &mut items, &mss)?;

    let mut f1 = 0i32;
    let mut cnt = 1i32;
    let mut d = 1i32;
    for i in 1..numrows as usize {
        if multi_cmp_items(&items[i], &items[i - 1], &mss)? != 0 {
            if cnt == 1 {
                f1 += 1;
            }
            d += 1;
            cnt = 0;
        }
        cnt += 1;
    }
    if cnt == 1 {
        f1 += 1;
    }

    Ok(mvdistinct::estimate_ndistinct(
        totalrows, numrows, d, f1,
    ))
}

/// `dependency_degree(data, k, dependency)` (dependencies.c:220). Validate one
/// candidate functional dependency `(a,b,...)->z` over the sampled data, by
/// sorting lexicographically, splitting into groups by the first (k-1) columns,
/// and counting supporting rows. `dependency` is the `k`-tuple of zero-based
/// column indexes into the statistics object.
fn dependency_degree<'mcx>(
    data: &StatsBuildData<'mcx>,
    k: i32,
    dependency: &[AttrNumber],
) -> PgResult<f64> {
    debug_assert!(k >= 2);

    let mcx = data_mcx(data);
    let mut mss = MultiSortSupport::init(mcx, k);

    let mut attnums_dep: Vec<AttrNumber> = Vec::with_capacity(k as usize);
    for i in 0..k as usize {
        attnums_dep.push(data.attnums[dependency[i] as usize]);
    }

    for i in 0..k as usize {
        let colstat = &data.stats[dependency[i] as usize];
        let lt_opr = lookup_lt_opr(colstat.attrtypid)?;
        mss.add_dimension(i as i32, lt_opr, colstat.attrcollid)?;
    }

    let items = match build_sorted_items(mcx, data, &mss, k, &attnums_dep)? {
        Some(items) => items,
        None => return Ok(0.0),
    };
    let nitems = items.len();

    let mut group_size = 1i32;
    let mut n_violations = 0i32;
    let mut n_supporting_rows = 0i32;

    for i in 1..=nitems {
        if i == nitems
            || cmp_dims_items(0, k - 2, &items[i - 1], &items[i], &mss)? != 0
        {
            if n_violations == 0 {
                n_supporting_rows += group_size;
            }
            n_violations = 0;
            group_size = 1;
            continue;
        } else if cmp_dim_items(k - 1, &items[i - 1], &items[i], &mss)? != 0 {
            n_violations += 1;
        }
        group_size += 1;
    }

    Ok((n_supporting_rows as f64) * 1.0 / (data.numrows as f64))
}

/// `build_mss(StatsBuildData *data)` (mcv.c:346) — build a `MultiSortSupport`
/// over every attribute of the statistics object (one dimension per attr, using
/// each column's `lt_opr`/collation).
fn build_mss<'mcx>(mcx: Mcx<'mcx>, data: &StatsBuildData<'mcx>) -> PgResult<MultiSortSupport<'mcx>> {
    let numattrs = data.nattnums;
    let mut mss = MultiSortSupport::init(mcx, numattrs);
    for i in 0..numattrs as usize {
        let colstat = &data.stats[i];
        let lt_opr = lookup_lt_opr(colstat.attrtypid)?;
        mss.add_dimension(i as i32, lt_opr, colstat.attrcollid)?;
    }
    Ok(mss)
}

/// `count_distinct_groups(numrows, items, mss)` (mcv.c:378) — count distinct
/// combinations of `SortItem`s in the sorted array.
fn count_distinct_groups<'mcx>(
    items: &[SortItem<'mcx>],
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<i32> {
    let mut ndistinct = 1i32;
    for i in 1..items.len() {
        if multi_cmp_items(&items[i], &items[i - 1], mss)? != 0 {
            ndistinct += 1;
        }
    }
    Ok(ndistinct)
}

/// `build_distinct_groups(numrows, items, mss, ndistinct)` (mcv.c:423) — collapse
/// the sorted `items` into distinct groups carrying multiplicity counts, then
/// sort the groups by count in descending order (`compare_sort_item_count`).
fn build_distinct_groups<'mcx>(
    items: &[SortItem<'mcx>],
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<Vec<SortItem<'mcx>>> {
    let numrows = items.len();
    let ngroups = count_distinct_groups(items, mss)? as usize;

    let mut groups: Vec<SortItem<'mcx>> = Vec::with_capacity(ngroups);

    let mut g = items[0].clone();
    g.count = 1;
    groups.push(g);

    let mut j = 0usize;
    for i in 1..numrows {
        if multi_cmp_items(&items[i], &items[i - 1], mss)? != 0 {
            let mut ng = items[i].clone();
            ng.count = 0;
            groups.push(ng);
            j += 1;
        }
        groups[j].count += 1;
    }

    debug_assert!(j + 1 == ngroups);

    // qsort_interruptible(groups, ..., compare_sort_item_count, NULL): sort by
    // count descending (stable-irrelevant; ties compare equal in C).
    groups.sort_by(|a, b| b.count.cmp(&a.count));

    Ok(groups)
}

/// `build_column_frequencies(groups, ngroups, mss, ncounts)` (mcv.c:489) —
/// compute, for each dimension, the deduplicated per-value occurrence counts
/// (summed across groups). Returns one `Vec<SortItem>` per dimension; the
/// returned dimension vectors are truncated to the deduplicated length, and
/// `ncounts[dim]` records that length.
fn build_column_frequencies<'mcx>(
    groups: &[SortItem<'mcx>],
    mss: &MultiSortSupport<'mcx>,
) -> PgResult<(Vec<Vec<SortItem<'mcx>>>, Vec<i32>)> {
    let ngroups = groups.len();
    let ndims = mss.ndims as usize;

    let mut result: Vec<Vec<SortItem<'mcx>>> = Vec::with_capacity(ndims);
    let mut ncounts: Vec<i32> = vec![0; ndims];

    for dim in 0..ndims {
        // Build the per-dimension single-column SortItems pointing at this
        // dimension's value/isnull (we copy the value, keeping a 1-element
        // values/isnull vector to match C's single-dimension search items).
        let mut col: Vec<SortItem<'mcx>> = Vec::with_capacity(ngroups);
        for i in 0..ngroups {
            col.push(SortItem {
                values: vec![groups[i].values[dim].clone()],
                isnull: vec![groups[i].isnull[dim]],
                count: groups[i].count,
            });
        }

        // qsort_interruptible(result[dim], ..., sort_item_compare, ssup):
        // single-dimension ApplySortComparator order. Use a fallible merge sort
        // because the comparator can ereport.
        sort_single_dim(&mut col, &mss.ssup[dim])?;

        // Deduplicate, summing counts for equal values.
        let mut nc = 1usize;
        for i in 1..ngroups {
            if cmp_single_dim(&col[nc - 1], &col[i], &mss.ssup[dim])? == 0 {
                col[nc - 1].count += col[i].count;
                continue;
            }
            col[nc] = col[i].clone();
            nc += 1;
        }
        col.truncate(nc);
        ncounts[dim] = nc as i32;
        result.push(col);
    }

    Ok((result, ncounts))
}

/// `sort_item_compare(a, b, ssup)` (mcv.c:464) — single-dimension
/// `ApplySortComparator(a->values[0], a->isnull[0], b->values[0], b->isnull[0])`.
fn cmp_single_dim<'mcx>(
    a: &SortItem<'mcx>,
    b: &SortItem<'mcx>,
    ssup: &SortSupportData<'mcx>,
) -> PgResult<i32> {
    cmp_with_nulls(ssup, &a.values[0], a.isnull[0], &b.values[0], b.isnull[0])
}

/// Fallible single-dimension sort (bottom-up merge sort) of `SortItem`s using
/// `cmp_single_dim`, so the comparator's `Err` surface propagates (qsort_interruptible).
fn sort_single_dim<'mcx>(
    items: &mut Vec<SortItem<'mcx>>,
    ssup: &SortSupportData<'mcx>,
) -> PgResult<()> {
    let n = items.len();
    let mut idx: Vec<usize> = (0..n).collect();
    merge_sort_single(items, ssup, &mut idx)?;
    let sorted: Vec<SortItem<'mcx>> = idx.iter().map(|&i| items[i].clone()).collect();
    *items = sorted;
    Ok(())
}

fn merge_sort_single<'mcx>(
    items: &[SortItem<'mcx>],
    ssup: &SortSupportData<'mcx>,
    idx: &mut Vec<usize>,
) -> PgResult<()> {
    let n = idx.len();
    if n <= 1 {
        return Ok(());
    }
    let mid = n / 2;
    let mut left: Vec<usize> = idx[..mid].to_vec();
    let mut right: Vec<usize> = idx[mid..].to_vec();
    merge_sort_single(items, ssup, &mut left)?;
    merge_sort_single(items, ssup, &mut right)?;
    let (mut i, mut j, mut k) = (0usize, 0usize, 0usize);
    while i < left.len() && j < right.len() {
        let cmp = cmp_single_dim(&items[left[i]], &items[right[j]], ssup)?;
        if cmp <= 0 {
            idx[k] = left[i];
            i += 1;
        } else {
            idx[k] = right[j];
            j += 1;
        }
        k += 1;
    }
    while i < left.len() {
        idx[k] = left[i];
        i += 1;
        k += 1;
    }
    while j < right.len() {
        idx[k] = right[j];
        j += 1;
        k += 1;
    }
    Ok(())
}

/// `get_mincount_for_mcv_list(samplerows, totalrows)` (mcv.c:147) — minimum
/// occurrence count for a value to be kept in the MCV list. Duplicated here (a
/// pure formula) because the mcv crate depends on this crate's seams, so this
/// crate cannot depend back on it.
fn get_mincount_for_mcv_list(samplerows: i32, totalrows: f64) -> f64 {
    let n: f64 = samplerows as f64;
    let big_n: f64 = totalrows;

    let numer = n * (big_n - n);
    let denom = big_n - n + 0.04 * n * (big_n - 1.0);

    if denom == 0.0 {
        return 0.0;
    }
    numer / denom
}

/// `statext_mcv_build(data, totalrows, stattarget)` (mcv.c:179) — build an MCV
/// list from the sampled rows: sort, group, threshold by `get_mincount_for_mcv_list`,
/// and compute per-item base frequencies. Returns `None` (C `NULL`) when no item
/// clears the threshold.
fn statext_mcv_build<'mcx>(
    data: &StatsBuildData<'mcx>,
    totalrows: f64,
    stattarget: i32,
) -> PgResult<Option<::statistics::MCVList<'mcx>>> {
    use ::statistics::{MCVItem, MCVList, STATS_MCV_MAGIC, STATS_MCV_TYPE_BASIC};

    let mcx = data_mcx(data);

    // comparator for all the columns
    let mss = build_mss(mcx, data)?;

    // sort the rows
    let items = match build_sorted_items(mcx, data, &mss, data.nattnums, &data.attnums)? {
        Some(items) => items,
        None => return Ok(None),
    };

    let numattrs = data.nattnums as usize;
    let numrows = data.numrows;

    // transform the sorted rows into groups (sorted by frequency)
    let groups = build_distinct_groups(&items, &mss)?;
    let ngroups = groups.len();

    // The maximum number of MCV items to store; can't keep more than available.
    let mut nitems = stattarget as usize;
    if nitems > ngroups {
        nitems = ngroups;
    }

    let mincount = get_mincount_for_mcv_list(numrows, totalrows);

    // Walk the groups until the first below mincount.
    for i in 0..nitems {
        if (groups[i].count as f64) < mincount {
            nitems = i;
            break;
        }
    }

    if nitems == 0 {
        return Ok(None);
    }

    // compute frequencies for values in each column
    let (freqs, nfreqs) = build_column_frequencies(&groups, &mss)?;

    let mut mcvlist = MCVList {
        magic: STATS_MCV_MAGIC,
        r#type: STATS_MCV_TYPE_BASIC,
        nitems: nitems as u32,
        ndimensions: numattrs as AttrNumber,
        types: [Oid::from(0u32); ::statistics::STATS_MAX_DIMENSIONS],
        items: Vec::with_capacity(nitems),
    };

    // store info about data type OIDs
    for i in 0..numattrs {
        mcvlist.types[i] = data.stats[i].attrtypid;
    }

    for i in 0..nitems {
        debug_assert!(i == 0 || groups[i - 1].count >= groups[i].count);

        let mut item = MCVItem {
            frequency: (groups[i].count as f64) / (numrows as f64),
            base_frequency: 1.0,
            isnull: groups[i].isnull.clone(),
            values: groups[i].values.clone(),
        };

        // base frequency, if the attributes were independent
        for j in 0..numattrs {
            // search this dimension's frequency table for the group's value
            let key = SortItem {
                values: vec![groups[i].values[j].clone()],
                isnull: vec![groups[i].isnull[j]],
                count: 0,
            };
            let pos = bsearch_single_dim(&key, &freqs[j], nfreqs[j] as usize, &mss.ssup[j])?
                .expect("base-frequency value must be present in the column frequency table");
            item.base_frequency *= (freqs[j][pos].count as f64) / (numrows as f64);
        }

        mcvlist.items.push(item);
    }

    Ok(Some(mcvlist))
}

/// `bsearch_arg(&key, freqs[j], nfreqs[j], ..., multi_sort_compare, tmp)` over a
/// single sorted dimension (mcv.c:324). Returns the index of the matching value.
fn bsearch_single_dim<'mcx>(
    key: &SortItem<'mcx>,
    base: &[SortItem<'mcx>],
    n: usize,
    ssup: &SortSupportData<'mcx>,
) -> PgResult<Option<usize>> {
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let c = cmp_single_dim(key, &base[mid], ssup)?;
        match c.cmp(&0) {
            core::cmp::Ordering::Equal => return Ok(Some(mid)),
            core::cmp::Ordering::Less => hi = mid,
            core::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    Ok(None)
}

/// `compare_scalars_simple(a, b, ssup)` (extended_stats.c:917) for one
/// dimension's `(lt_opr, collation)` — three-way `< 0 / 0 / > 0`. C prepares the
/// SortSupport once and reuses it; the seam re-prepares per call (the `lt_opr`
/// was resolved by a successful type-cache lookup, but the prepare and the fmgr
/// comparison can ereport, so the seam is fallible and takes the build context).
fn mcv_compare_scalars_simple<'mcx>(
    mcx: Mcx<'mcx>,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
    lt_opr: Oid,
    collation: Oid,
) -> PgResult<i32> {
    let mut ssup = SortSupportData::new(mcx);
    ssup.ssup_collation = collation;
    ssup.ssup_nulls_first = false;
    prepare_sort_support_from_ordering_op::call(lt_opr, &mut ssup)?;
    apply_sort_comparator::call(a.clone(), b.clone(), &ssup)
}

/// `statext_mcv_serialize` per-value codec (mcv.c:868-919): emit one MCV value's
/// on-wire payload bytes (data-only; the caller prepends the uint32 length for
/// the var-width categories).
fn mcv_value_to_serialized_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    typlen: i16,
    typbyval: bool,
) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let out: Vec<u8> = if typbyval {
        // store_att_byval into a local word, then copy the typlen significant
        // bytes (native-endian, matching store_att_byval/fetch_att).
        let word = value.as_usize() as u64;
        match typlen {
            1 => vec![word as u8],
            2 => (word as u16).to_ne_bytes().to_vec(),
            4 => (word as u32).to_ne_bytes().to_vec(),
            8 => word.to_ne_bytes().to_vec(),
            _ => return Err(PgError::error(format!("unsupported byval length: {typlen}"))),
        }
    } else if typlen > 0 {
        // fixed-length by-reference: the typlen bytes at the pointer.
        value.as_ref_bytes()[..typlen as usize].to_vec()
    } else if typlen == -1 {
        // varlena: detoast, emit VARSIZE_ANY_EXHDR body (no header).
        let detoasted = pg_detoast_datum_packed::call(mcx, value.as_varlena_bytes().as_ref())?;
        varlena_body(detoasted.as_slice())?.to_vec()
    } else if typlen == -2 {
        // cstring: the NUL-terminated bytes including the terminator.
        let s = value
            .as_cstring()
            .ok_or_else(|| PgError::error("MCV cstring value is not a Cstring datum".to_string()))?;
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    } else {
        return Err(PgError::error(format!("unexpected typlen {typlen} in MCV value codec")));
    };

    let mut pv = ::mcx::PgVec::new_in(mcx);
    pv.try_reserve(out.len()).map_err(|_| mcx.oom(out.len()))?;
    for b in out {
        pv.push(b);
    }
    Ok(pv)
}

/// `statext_mcv_deserialize` per-value codec (mcv.c:1186-1259): reconstruct one
/// MCV value `Datum` from its on-wire payload bytes (data-only for var-width).
fn mcv_serialized_bytes_to_value<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: &[u8],
    typlen: i16,
    typbyval: bool,
) -> PgResult<Datum<'mcx>> {
    if typbyval {
        // fetch_att(&v, true, typlen): read the typlen significant bytes into a
        // machine word.
        let mut word = [0u8; 8];
        word[..typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
        let v = match typlen {
            1 => bytes[0] as i8 as i64 as usize,
            2 => i16::from_ne_bytes([bytes[0], bytes[1]]) as i64 as usize,
            4 => i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64 as usize,
            8 => usize::from_ne_bytes(word),
            _ => return Err(PgError::error(format!("unsupported byval length: {typlen}"))),
        };
        Ok(Datum::from_usize(v))
    } else if typlen > 0 {
        // fixed by-ref: copy the typlen bytes, point a ByRef at them.
        Datum::from_byref_bytes_in(mcx, &bytes[..typlen as usize])
    } else if typlen == -1 {
        // varlena: build a full 4-byte-header varlena from the data-only body.
        let len = bytes.len();
        let mut img: Vec<u8> = Vec::with_capacity(len + 4);
        img.extend_from_slice(&[0u8; 4]);
        img.extend_from_slice(bytes);
        set_varsize_4b(&mut img, (len + 4) as u32);
        Datum::from_byref_bytes_in(mcx, &img)
    } else if typlen == -2 {
        // cstring: the NUL-terminated bytes (incl terminator) -> Cstring datum.
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        let s = core::str::from_utf8(&bytes[..end])
            .map_err(|_| PgError::error("MCV cstring value is not valid UTF-8".to_string()))?;
        Ok(Datum::from_cstring(s.to_string()))
    } else {
        Err(PgError::error(format!("unexpected typlen {typlen} in MCV value codec")))
    }
}

/// `VARDATA_ANY(p)` / `VARSIZE_ANY_EXHDR(p)`: the data-only body of a (possibly
/// short-header) varlena image. The detoast step already de-compressed/expanded
/// the datum, so only the 1B/4B inline-header distinction remains.
fn varlena_body(data: &[u8]) -> PgResult<&[u8]> {
    if data.is_empty() {
        return Err(PgError::error("empty varlena image in MCV value codec".to_string()));
    }
    // VARATT_IS_1B: low bit set in the first byte (short header, 1 byte).
    if data[0] & 0x01 != 0 {
        // VARSIZE_1B = (header >> 1) & 0x7F ; body = total - 1 header byte.
        let total = ((data[0] >> 1) & 0x7F) as usize;
        Ok(&data[1..total])
    } else {
        // 4-byte header; VARSIZE = (len >> 2) over a 4-byte big? No — the inline
        // 4B header stores the total size in the low 30 bits, little-endian on
        // this platform's layout used elsewhere in the port (set_varsize_4b).
        let total = (u32::from_ne_bytes([data[0], data[1], data[2], data[3]]) >> 2) as usize;
        Ok(&data[4..total])
    }
}

/// `SET_VARSIZE(p, size)`: write a 4-byte varlena header storing `size` (total,
/// incl header) in the standard `(size << 2)` layout the port uses.
fn set_varsize_4b(buf: &mut [u8], size: u32) {
    let hdr = (size << 2).to_ne_bytes();
    buf[0..4].copy_from_slice(&hdr);
}

/// The `MemoryContext` the kernels build their transient sort buffers in. C uses
/// `CurrentMemoryContext`; the owned `StatsBuildData` carries no context, so we
/// borrow the per-column `anl_context` (the long-lived ANALYZE context).
fn data_mcx<'mcx>(data: &StatsBuildData<'mcx>) -> Mcx<'mcx> {
    data.stats[0]
        .anl_context
        .expect("StatsBuildData stats[0].anl_context must be set for the build")
}

/// Install the build-kernel seams that the per-kind slices consume.
pub fn init_seams() {
    seams::ndistinct_for_combination::set(ndistinct_for_combination);
    seams::dependency_degree::set(dependency_degree);
    seams::mcv_lookup_lt_opr::set(lookup_lt_opr);
    seams::statext_mcv_build::set(statext_mcv_build);
    seams::mcv_compare_scalars_simple::set(mcv_compare_scalars_simple);
    seams::mcv_value_to_serialized_bytes::set(mcv_value_to_serialized_bytes);
    seams::mcv_serialized_bytes_to_value::set(mcv_serialized_bytes_to_value);
}
