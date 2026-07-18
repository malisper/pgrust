use crate::merge::*;
use crate::{KIND_MAXVALUE, KIND_MINVALUE, KIND_VALUE};
use mcx::{Mcx, MemoryContext, PgVec};
use types_error::PgResult;
use types_pathnodes::{
    DatumImage, JoinType, PartitionBoundInfoData, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT,
    JOIN_SEMI,
};

fn static_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("partbounds merge test"))).mcx()
}

fn img(v: i64) -> DatumImage<'static> {
    DatumImage::ByVal(v as u64)
}

fn img_val(d: &DatumImage<'static>) -> i64 {
    match d {
        DatumImage::ByVal(v) => *v as i64,
        DatumImage::Bytes(_) => panic!("byref image in test"),
    }
}

fn cmp_i64(_col: usize, a: &DatumImage<'static>, b: &DatumImage<'static>) -> PgResult<i32> {
    Ok(img_val(a).cmp(&img_val(b)) as i32)
}

fn list_bi(
    mcx: Mcx<'static>,
    vals: &[(i64, i32)],
    null_index: i32,
    default_index: i32,
) -> PartitionBoundInfoData<'static> {
    let mut bi = PartitionBoundInfoData::new(mcx);
    bi.strategy = b'l' as i8;
    bi.ndatums = vals.len() as i32;
    bi.nindexes = vals.len() as i32;
    bi.null_index = null_index;
    bi.default_index = default_index;
    for &(v, idx) in vals {
        let mut row: PgVec<'static, DatumImage<'static>> = PgVec::new_in(mcx);
        row.push(img(v));
        bi.datums.push(row);
        bi.indexes.push(idx);
    }
    bi
}

fn range_bi1(
    mcx: Mcx<'static>,
    bounds: &[(i64, i8)],
    indexes: &[i32],
    default_index: i32,
) -> PartitionBoundInfoData<'static> {
    assert_eq!(indexes.len(), bounds.len() + 1);
    let mut bi = PartitionBoundInfoData::new(mcx);
    bi.strategy = b'r' as i8;
    bi.ndatums = bounds.len() as i32;
    bi.nindexes = indexes.len() as i32;
    bi.default_index = default_index;
    let mut kinds: PgVec<'static, PgVec<'static, i8>> = PgVec::new_in(mcx);
    for &(v, k) in bounds {
        let mut row: PgVec<'static, DatumImage<'static>> = PgVec::new_in(mcx);
        row.push(if k == KIND_VALUE { img(v) } else { img(0) });
        bi.datums.push(row);
        let mut krow: PgVec<'static, i8> = PgVec::new_in(mcx);
        krow.push(k);
        kinds.push(krow);
    }
    bi.kind = Some(kinds);
    for &ix in indexes {
        bi.indexes.push(ix);
    }
    bi
}

fn hash_bi(
    mcx: Mcx<'static>,
    rows: &[(i64, i64)],
    indexes: &[i32],
) -> PartitionBoundInfoData<'static> {
    let mut bi = PartitionBoundInfoData::new(mcx);
    bi.strategy = b'h' as i8;
    bi.ndatums = rows.len() as i32;
    bi.nindexes = indexes.len() as i32;
    for &(m, r) in rows {
        let mut row: PgVec<'static, DatumImage<'static>> = PgVec::new_in(mcx);
        row.push(img(m));
        row.push(img(r));
        bi.datums.push(row);
    }
    for &ix in indexes {
        bi.indexes.push(ix);
    }
    bi
}

fn do_merge(
    mcx: Mcx<'static>,
    partnatts: i32,
    outer_bi: &PartitionBoundInfoData<'static>,
    outer_dummy: &[bool],
    inner_bi: &PartitionBoundInfoData<'static>,
    inner_dummy: &[bool],
    jointype: JoinType,
) -> Option<PartitionBoundsMergeResult<'static>> {
    let outer = MergeRel {
        nparts: outer_dummy.len() as i32,
        boundinfo: outer_bi,
        is_dummy: outer_dummy,
    };
    let inner = MergeRel {
        nparts: inner_dummy.len() as i32,
        boundinfo: inner_bi,
        is_dummy: inner_dummy,
    };
    let mut cmp = cmp_i64;
    partition_bounds_merge(mcx, partnatts, &mut cmp, &outer, &inner, jointype).unwrap()
}

fn row_vals(bi: &PartitionBoundInfoData<'static>) -> Vec<i64> {
    bi.datums.iter().map(|r| img_val(&r[0])).collect()
}

#[test]
fn eq_list_equal_and_datum_differs() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    assert!(partition_bounds_equal(1, &a, &b));
    let c = list_bi(mcx, &[(10, 0), (21, 1)], -1, -1);
    assert!(!partition_bounds_equal(1, &a, &c));
}

#[test]
fn eq_counts_and_special_indexes_differ() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0)], -1, -1);
    assert!(!partition_bounds_equal(1, &a, &b));
    let c = list_bi(mcx, &[(10, 0), (20, 1)], 2, -1);
    assert!(!partition_bounds_equal(1, &a, &c));
    let d = list_bi(mcx, &[(10, 0), (20, 1)], -1, 2);
    assert!(!partition_bounds_equal(1, &a, &d));
    let e = list_bi(mcx, &[(10, 1), (20, 0)], -1, -1);
    assert!(!partition_bounds_equal(1, &a, &e));
}

#[test]
fn eq_range_nonfinite_skips_datums() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_MINVALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let mut b = range_bi1(mcx, &[(0, KIND_MINVALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    b.datums[0][0] = img(99);
    assert!(partition_bounds_equal(1, &a, &b));
    let c = range_bi1(mcx, &[(0, KIND_MAXVALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    assert!(!partition_bounds_equal(1, &a, &c));
}

#[test]
fn eq_hash_compares_indexes() {
    let mcx = static_mcx();
    let a = hash_bi(mcx, &[(4, 0), (4, 1), (4, 2), (4, 3)], &[0, 1, 2, 3]);
    let b = hash_bi(mcx, &[(4, 0), (4, 1), (4, 2), (4, 3)], &[0, 1, 2, 3]);
    assert!(partition_bounds_equal(1, &a, &b));
    let c = hash_bi(mcx, &[(4, 0), (4, 1), (4, 2), (4, 3)], &[0, 1, 3, 2]);
    assert!(!partition_bounds_equal(1, &a, &c));
}

#[test]
fn merge_hash_returns_none() {
    let mcx = static_mcx();
    let a = hash_bi(mcx, &[(2, 0), (2, 1)], &[0, 1]);
    let b = hash_bi(mcx, &[(2, 0), (2, 1)], &[0, 1]);
    assert!(do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).is_none());
}

#[test]
fn list_inner_exact_match() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10, 20]);
    assert_eq!(&r.merged_bounds.indexes[..], [0, 1]);
    assert_eq!(r.merged_bounds.null_index, -1);
    assert_eq!(r.merged_bounds.default_index, -1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn list_inner_missing_value_drops_partition() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 1], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10]);
    assert_eq!(&r.outer_parts[..], [0]);
    assert_eq!(&r.inner_parts[..], [0]);
}

#[test]
fn list_left_missing_value_pairs_with_dummy() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 1], JOIN_LEFT).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10, 20]);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, -1]);
}

#[test]
fn list_full_missing_outer_value_pairs_with_dummy() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0)], -1, -1);
    let b = list_bi(mcx, &[(10, 0), (30, 1)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 2], JOIN_FULL).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10, 30]);
    assert_eq!(&r.outer_parts[..], [0, -1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn list_null_partitions_inner_eliminated_left_kept() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0)], 1, -1);
    let b = list_bi(mcx, &[(10, 0)], 1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(r.merged_bounds.null_index, -1);
    assert_eq!(r.outer_parts.len(), 1);

    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_LEFT).unwrap();
    assert_eq!(r.merged_bounds.null_index, 1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn list_outer_null_only_full_join() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0)], 1, -1);
    let b = list_bi(mcx, &[(10, 0)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 1], JOIN_FULL).unwrap();
    assert_eq!(r.merged_bounds.null_index, 1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, -1]);
}

#[test]
fn list_defaults_merge_with_each_other() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0)], -1, 1);
    let b = list_bi(mcx, &[(10, 0)], -1, 1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10]);
    assert_eq!(r.merged_bounds.default_index, 1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn list_both_defaults_with_unmatched_value_returns_none() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, 2);
    let b = list_bi(mcx, &[(10, 0)], -1, 1);
    assert!(do_merge(mcx, 1, &a, &[false; 3], &b, &[false; 2], JOIN_INNER).is_none());
}

#[test]
fn list_multi_match_returns_none() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 0)], -1, -1);
    let b = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    assert!(do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 2], JOIN_INNER).is_none());
}

#[test]
fn list_dummy_partition_skipped() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let b = list_bi(mcx, &[(10, 0), (20, 1)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false, true], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10]);
    assert_eq!(&r.outer_parts[..], [0]);
    assert_eq!(&r.inner_parts[..], [0]);
}

#[test]
fn list_dummy_default_treated_as_absent() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(10, 0), (20, 1)], -1, 2);
    let b = list_bi(mcx, &[(10, 0)], -1, 1);
    // Outer's default is dummy, so 20 pairs with the inner default instead
    // of the both-defaults rejection.
    let r = do_merge(mcx, 1, &a, &[false, false, true], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [10, 20]);
    assert_eq!(r.merged_bounds.default_index, -1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn list_full_remap_merges_dummy_assignments() {
    let mcx = static_mcx();
    let a = list_bi(mcx, &[(1, 0), (4, 0)], -1, -1);
    let b = list_bi(mcx, &[(2, 0), (4, 0)], -1, -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_FULL).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [1, 2, 4]);
    assert_eq!(&r.merged_bounds.indexes[..], [0, 0, 0]);
    assert_eq!(&r.outer_parts[..], [0]);
    assert_eq!(&r.inner_parts[..], [0]);
}

#[test]
fn range_inner_identical() {
    let mcx = static_mcx();
    let bounds = [(0, KIND_VALUE), (10, KIND_VALUE), (20, KIND_VALUE)];
    let a = range_bi1(mcx, &bounds, &[-1, 0, 1, -1], -1);
    let b = range_bi1(mcx, &bounds, &[-1, 0, 1, -1], -1);
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [0, 10, 20]);
    assert_eq!(&r.merged_bounds.indexes[..], [-1, 0, 1, -1]);
    assert_eq!(r.merged_bounds.ndatums, 3);
    assert_eq!(r.merged_bounds.nindexes, 4);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}

#[test]
fn range_inner_partial_overlap_intersects() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(5, KIND_VALUE), (15, KIND_VALUE)], &[-1, 0, -1], -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [5, 10]);
    assert_eq!(&r.merged_bounds.indexes[..], [-1, 0, -1]);
    assert_eq!(&r.outer_parts[..], [0]);
    assert_eq!(&r.inner_parts[..], [0]);
}

#[test]
fn range_full_partial_overlap_unions() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(5, KIND_VALUE), (15, KIND_VALUE)], &[-1, 0, -1], -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_FULL).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [0, 15]);
}

#[test]
fn range_left_and_anti_take_outer_bounds() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(5, KIND_VALUE), (15, KIND_VALUE)], &[-1, 0, -1], -1);
    for jt in [JOIN_LEFT, JOIN_ANTI] {
        let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], jt).unwrap();
        assert_eq!(row_vals(&r.merged_bounds), [0, 10]);
    }
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_SEMI).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [5, 10]);
}

#[test]
fn range_inner_disjoint_returns_none() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(10, KIND_VALUE), (20, KIND_VALUE)], &[-1, 0, -1], -1);
    assert!(do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_INNER).is_none());
}

#[test]
fn range_full_disjoint_keeps_both() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(10, KIND_VALUE), (20, KIND_VALUE)], &[-1, 0, -1], -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_FULL).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [0, 10, 20]);
    assert_eq!(&r.merged_bounds.indexes[..], [-1, 0, 1, -1]);
    assert_eq!(&r.outer_parts[..], [0, -1]);
    assert_eq!(&r.inner_parts[..], [-1, 0]);
}

#[test]
fn range_overlapping_next_partition_returns_none() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (20, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(
        mcx,
        &[(0, KIND_VALUE), (5, KIND_VALUE), (6, KIND_VALUE), (20, KIND_VALUE)],
        &[-1, 0, -1, 1, -1],
        -1,
    );
    assert!(do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 2], JOIN_INNER).is_none());
}

#[test]
fn range_default_beside_nonoverlap_returns_none() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], 1);
    let b = range_bi1(mcx, &[(5, KIND_VALUE), (15, KIND_VALUE)], &[-1, 0, -1], -1);
    assert!(do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 1], JOIN_INNER).is_none());
}

#[test]
fn range_minvalue_bounds_merge() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_MINVALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let b = range_bi1(mcx, &[(0, KIND_MINVALUE), (10, KIND_VALUE)], &[-1, 0, -1], -1);
    let r = do_merge(mcx, 1, &a, &[false; 1], &b, &[false; 1], JOIN_INNER).unwrap();
    let k = r.merged_bounds.kind.as_ref().unwrap();
    assert_eq!(k[0][0], KIND_MINVALUE);
    assert_eq!(k[1][0], KIND_VALUE);
    assert_eq!(img_val(&r.merged_bounds.datums[1][0]), 10);
    assert_eq!(&r.merged_bounds.indexes[..], [-1, 0, -1]);
}

#[test]
fn range_outer_default_pairs_missing_inner() {
    let mcx = static_mcx();
    let a = range_bi1(mcx, &[(0, KIND_VALUE), (10, KIND_VALUE)], &[-1, 0, -1], 1);
    let b = range_bi1(
        mcx,
        &[(0, KIND_VALUE), (10, KIND_VALUE), (20, KIND_VALUE)],
        &[-1, 0, 1, -1],
        -1,
    );
    let r = do_merge(mcx, 1, &a, &[false; 2], &b, &[false; 2], JOIN_INNER).unwrap();
    assert_eq!(row_vals(&r.merged_bounds), [0, 10, 20]);
    assert_eq!(&r.merged_bounds.indexes[..], [-1, 0, 1, -1]);
    assert_eq!(r.merged_bounds.default_index, -1);
    assert_eq!(&r.outer_parts[..], [0, 1]);
    assert_eq!(&r.inner_parts[..], [0, 1]);
}
