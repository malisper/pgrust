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

use mcx::Mcx;
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_sortsupport::SortSupportData;
use types_statistics::{SortItem, StatsBuildData};
use types_tuple::Datum;

use backend_parser_parse_oper::get_sort_group_operators;
use backend_utils_sort_sortsupport_seams::{apply_sort_comparator, prepare_sort_support_from_ordering_op};
use backend_utils_cache_lsyscache_seams::get_typlen;
use backend_access_common_detoast_seams::{pg_detoast_datum_packed, toast_raw_datum_size};

use backend_statistics_core_seams as seams;

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

    Ok(backend_statistics_mvdistinct::estimate_ndistinct(
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
}
