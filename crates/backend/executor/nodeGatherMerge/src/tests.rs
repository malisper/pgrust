//! Seam-free unit tests for the crate's owned, non-seamed logic. The full
//! `Exec*GatherMerge` paths drive a live executor through many sibling seams
//! whose owners are not yet ported (they panic loudly until then), so they are
//! exercised by the integration suite once those owners land; here we cover the
//! comparator's NULL ordering, the compare-result inversion, and the in-crate
//! binary-heap structure.
use super::*;
use ::mcx::MemoryContext;

#[test]
fn invert_compare_result_matches_c() {
    assert_eq!(INVERT_COMPARE_RESULT(-7), 1);
    assert_eq!(INVERT_COMPARE_RESULT(7), -7);
    assert_eq!(INVERT_COMPARE_RESULT(0), 0);
    // -INT_MIN corner case avoided: INT_MIN < 0 so result is 1.
    assert_eq!(INVERT_COMPARE_RESULT(i32::MIN), 1);
}

#[test]
fn apply_sort_comparator_null_handling() {
    let ctx = MemoryContext::new("gm-test");
    let mut key = SortSupportData::new(ctx.mcx());
    // both null -> 0 (the seam is never consulted on a null branch).
    assert_eq!(
        ApplySortComparator(Datum::null(), true, Datum::null(), true, &key).unwrap(),
        0
    );
    // null1 only, nulls_first -> -1 ; nulls_last -> 1
    key.ssup_nulls_first = true;
    assert_eq!(
        ApplySortComparator(Datum::null(), true, Datum::from_i32(5), false, &key).unwrap(),
        -1
    );
    key.ssup_nulls_first = false;
    assert_eq!(
        ApplySortComparator(Datum::null(), true, Datum::from_i32(5), false, &key).unwrap(),
        1
    );
    // null2 only, nulls_first -> 1 ; nulls_last -> -1
    key.ssup_nulls_first = true;
    assert_eq!(
        ApplySortComparator(Datum::from_i32(5), false, Datum::null(), true, &key).unwrap(),
        1
    );
    key.ssup_nulls_first = false;
    assert_eq!(
        ApplySortComparator(Datum::from_i32(5), false, Datum::null(), true, &key).unwrap(),
        -1
    );
}

#[test]
fn binary_heap_allocate_starts_empty_then_add_and_reset() {
    let ctx = MemoryContext::new("gm-heap");
    let mut heap = BinaryHeap::allocate(ctx.mcx(), 4).unwrap();
    assert_eq!(heap.bh_space, 4);
    assert_eq!(heap.bh_size, 0);
    assert!(binaryheap_empty(&heap));

    binaryheap_add_unordered(&mut heap, Datum::from_i32(0)).unwrap();
    binaryheap_add_unordered(&mut heap, Datum::from_i32(1)).unwrap();
    assert_eq!(heap.bh_size, 2);
    assert!(!heap.bh_has_heap_property);
    assert_eq!(binaryheap_first(&heap).unwrap().as_i32(), 0);

    binaryheap_reset(&mut heap);
    assert_eq!(heap.bh_size, 0);
    assert!(heap.bh_has_heap_property);
    assert!(heap.bh_nodes.is_empty());
}

#[test]
fn binary_heap_add_unordered_overflows_at_capacity() {
    let ctx = MemoryContext::new("gm-heap-of");
    let mut heap = BinaryHeap::allocate(ctx.mcx(), 1).unwrap();
    binaryheap_add_unordered(&mut heap, Datum::from_i32(0)).unwrap();
    // C: elog(ERROR, "out of binary heap slots").
    assert!(binaryheap_add_unordered(&mut heap, Datum::from_i32(1)).is_err());
}

#[test]
fn heap_offsets_match_c() {
    assert_eq!(parent_offset(1), 0);
    assert_eq!(parent_offset(2), 0);
    assert_eq!(parent_offset(3), 1);
    assert_eq!(left_offset(0), 1);
    assert_eq!(right_offset(0), 2);
    assert_eq!(left_offset(1), 3);
    assert_eq!(right_offset(1), 4);
}
