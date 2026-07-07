use super::*;
use mcx::MemoryContext;
use types_nodes::Bitmapset;

fn static_mcx() -> Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("partprune test"))).mcx()
}

fn i32_cmp(bound: Datum, probe: i32) -> i32 {
    (bound.as_i32() - probe).signum()
}

// LIST bounds: datums [10, 20, 30], indexes [0, 1, 2], null -> 3, default -> 4.
fn list_bounds(mcx: Mcx<'static>) -> partbounds::PartitionBoundInfoData<'static> {
    let mut datums = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    for v in [10, 20, 30] {
        datums.push(Datum::from_i32(v));
    }
    let mut indexes = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    indexes.extend([0, 1, 2]);
    partbounds::PartitionBoundInfoData {
        strategy: b'l' as i8,
        ndatums: 3,
        width: 1,
        datums,
        kind: mcx::PgVec::new_in(mcx),
        indexes,
        null_index: 3,
        default_index: 4,
    }
}

fn members(b: &Bitmapset<'_>) -> Vec<i32> {
    let mut v = Vec::new();
    let mut m = b.next_member(-1);
    while m >= 0 {
        v.push(m);
        m = b.next_member(m);
    }
    v
}

#[test]
fn list_eq_matches_single_bound() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_list_bounds(mcx, &bi, BTEqualStrategyNumber, 1, &empty, |b| {
        i32_cmp(b, 20)
    })
    .unwrap();
    assert_eq!(members(&r.bound_offsets), [1]);
    assert!(!r.scan_default && !r.scan_null);
}

#[test]
fn list_eq_miss_scans_default() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_list_bounds(mcx, &bi, BTEqualStrategyNumber, 1, &empty, |b| {
        i32_cmp(b, 25)
    })
    .unwrap();
    assert!(members(&r.bound_offsets).is_empty());
    assert!(r.scan_default);
}

#[test]
fn list_ne_drops_matched_bound() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_list_bounds(mcx, &bi, InvalidStrategy, 1, &empty, |b| i32_cmp(b, 10))
        .unwrap();
    assert_eq!(members(&r.bound_offsets), [1, 2]);
    assert!(r.scan_default);
}

#[test]
fn list_gt_range_and_default() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_list_bounds(mcx, &bi, BTGreaterStrategyNumber, 1, &empty, |b| {
        i32_cmp(b, 10)
    })
    .unwrap();
    assert_eq!(members(&r.bound_offsets), [1, 2]);
    assert!(r.scan_default);
    let r = get_matching_list_bounds(mcx, &bi, BTLessEqualStrategyNumber, 1, &empty, |b| {
        i32_cmp(b, 20)
    })
    .unwrap();
    assert_eq!(members(&r.bound_offsets), [0, 1]);
}

#[test]
fn list_nullkeys_scan_null_partition() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let nk = Bitmapset::make_singleton(mcx, 0).unwrap();
    let r = get_matching_list_bounds(mcx, &bi, BTEqualStrategyNumber, 0, &nk, |_| 0).unwrap();
    assert!(r.scan_null && !r.scan_default);
    let sel = matching_bounds_to_partitions(mcx, &bi, &r, b'l').unwrap();
    assert_eq!(members(&sel), [3]);
}

// RANGE bounds (1 key): [MIN, 10, 20] with indexes [-1?]... layout:
// datums rows: {MINVALUE}, {10}, {20}; indexes: [-1, 0, 1, -1] (upper -1 tail);
// partition 0 = [MIN,10), partition 1 = [10,20).
fn range_bounds(mcx: Mcx<'static>) -> partbounds::PartitionBoundInfoData<'static> {
    let mut datums = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    datums.extend([Datum::null(), Datum::from_i32(10), Datum::from_i32(20)]);
    let mut kind = mcx::vec_with_capacity_in(mcx, 3).unwrap();
    kind.extend([KIND_MINVALUE, KIND_VALUE, KIND_VALUE]);
    let mut indexes = mcx::vec_with_capacity_in(mcx, 4).unwrap();
    indexes.extend([-1, 0, 1, -1]);
    partbounds::PartitionBoundInfoData {
        strategy: b'r' as i8,
        ndatums: 3,
        width: 1,
        datums,
        kind,
        indexes,
        null_index: -1,
        default_index: -1,
    }
}

#[test]
fn range_eq_picks_covering_partition() {
    let mcx = static_mcx();
    let bi = range_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_range_bounds(mcx, &bi, 1, BTEqualStrategyNumber, 1, &empty, &mut |_,
             b| {
        i32_cmp(b, 15)
    })
    .unwrap();
    let sel = matching_bounds_to_partitions(mcx, &bi, &r, b'r').unwrap();
    assert_eq!(members(&sel), [1]);
}

#[test]
fn range_lt_min_prunes_to_first() {
    let mcx = static_mcx();
    let bi = range_bounds(mcx);
    let empty = Bitmapset::empty();
    let r =
        get_matching_range_bounds(mcx, &bi, 1, BTLessStrategyNumber, 1, &empty, &mut |_, b| {
            i32_cmp(b, 5)
        })
        .unwrap();
    let sel = matching_bounds_to_partitions(mcx, &bi, &r, b'r').unwrap();
    assert_eq!(members(&sel), [0]);
}

#[test]
fn range_ge_upper_prunes_all() {
    let mcx = static_mcx();
    let bi = range_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_range_bounds(
        mcx,
        &bi,
        1,
        BTGreaterEqualStrategyNumber,
        1,
        &empty,
        &mut |_, b| i32_cmp(b, 20),
    )
    .unwrap();
    let sel = matching_bounds_to_partitions(mcx, &bi, &r, b'r').unwrap();
    assert!(members(&sel).is_empty());
}

// HASH: modulus 4, remainders 0..3 -> indexes [0,1,2,3].
fn hash_bounds(mcx: Mcx<'static>) -> partbounds::PartitionBoundInfoData<'static> {
    let mut datums = mcx::vec_with_capacity_in(mcx, 8).unwrap();
    for r in 0..4 {
        datums.extend([Datum::from_i32(4), Datum::from_i32(r)]);
    }
    let mut indexes = mcx::vec_with_capacity_in(mcx, 4).unwrap();
    indexes.extend([0, 1, 2, 3]);
    partbounds::PartitionBoundInfoData {
        strategy: b'h' as i8,
        ndatums: 4,
        width: 2,
        datums,
        kind: mcx::PgVec::new_in(mcx),
        indexes,
        null_index: -1,
        default_index: -1,
    }
}

#[test]
fn hash_full_key_prunes_to_remainder() {
    let mcx = static_mcx();
    let bi = hash_bounds(mcx);
    let empty = Bitmapset::empty();
    let r = get_matching_hash_bounds(mcx, &bi, 1, HTEqualStrategyNumber, 1, &empty, || 7u64)
        .unwrap();
    assert_eq!(members(&r.bound_offsets), [3]);
    let r = get_matching_hash_bounds(mcx, &bi, 2, HTEqualStrategyNumber, 1, &empty, || 0u64)
        .unwrap();
    assert_eq!(members(&r.bound_offsets), [0, 1, 2, 3]);
}

#[test]
fn combine_union_and_intersect() {
    let mcx = static_mcx();
    let bi = list_bounds(mcx);
    let mk = |xs: &[i32]| {
        let mut b = Bitmapset::empty();
        for &x in xs {
            b.add_member(mcx, x).unwrap();
        }
        PruneStepResult { bound_offsets: b, scan_default: false, scan_null: false }
    };
    let results = vec![Some(mk(&[0, 1])), Some(mk(&[1, 2]))];
    let u = perform_pruning_combine_step(
        mcx,
        &bi,
        PARTPRUNE_COMBINE_UNION,
        2,
        [0i32, 1].into_iter(),
        &results,
    )
    .unwrap();
    assert_eq!(members(&u.bound_offsets), [0, 1, 2]);
    let i = perform_pruning_combine_step(
        mcx,
        &bi,
        PARTPRUNE_COMBINE_INTERSECT,
        2,
        [0i32, 1].into_iter(),
        &results,
    )
    .unwrap();
    assert_eq!(members(&i.bound_offsets), [1]);
    let all = perform_pruning_combine_step(
        mcx,
        &bi,
        PARTPRUNE_COMBINE_UNION,
        2,
        core::iter::empty(),
        &results,
    )
    .unwrap();
    assert_eq!(members(&all.bound_offsets), [0, 1, 2]);
    assert!(all.scan_default && all.scan_null);
}

// LIST bounds with no datums: NULL partition (-> 0) + DEFAULT (-> 1) only,
// as C builds for `FOR VALUES IN (NULL)` + DEFAULT (exists_tbl, subselect
// suite). The empty-source combine step then calls bms_add_range(0, -1),
// which C's bitmapset.c makes a no-op.
#[test]
fn combine_empty_sources_zero_datums_is_noop_range() {
    let mcx = static_mcx();
    let bi = partbounds::PartitionBoundInfoData {
        strategy: b'l' as i8,
        ndatums: 0,
        width: 1,
        datums: mcx::PgVec::new_in(mcx),
        kind: mcx::PgVec::new_in(mcx),
        indexes: mcx::PgVec::new_in(mcx),
        null_index: 0,
        default_index: 1,
    };
    let all = perform_pruning_combine_step(
        mcx,
        &bi,
        PARTPRUNE_COMBINE_UNION,
        0,
        core::iter::empty(),
        &[],
    )
    .unwrap();
    assert_eq!(members(&all.bound_offsets), [] as [i32; 0]);
    assert!(all.scan_default && all.scan_null);
}

#[test]
fn bms_add_range_inverted_is_noop() {
    let mcx = static_mcx();
    let mut b = Bitmapset::empty();
    bms_add_range(mcx, &mut b, 0, -1).unwrap();
    assert!(b.is_empty());
    bms_add_range(mcx, &mut b, 5, 3).unwrap();
    assert!(b.is_empty());
}
