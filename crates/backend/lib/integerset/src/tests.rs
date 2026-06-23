use super::*;
use std::vec::Vec;
use ::types_error::ERRCODE_INTERNAL_ERROR;

/// Build an integer set inside a dedicated context and exercise the closure with
/// it.  Mirrors PG's recommended usage of holding an integer set in a dedicated
/// memory context.
fn with_set<R>(f: impl FnOnce(&mut IntegerSet) -> R) -> R {
    let mut set = intset_create().unwrap();
    f(&mut set)
}

fn collect(set: &mut IntegerSet) -> Vec<u64> {
    intset_begin_iterate(set);
    let mut values = Vec::new();
    let mut next = 0u64;
    while intset_iterate_next(set, &mut next) {
        values.push(next);
    }
    values
}

#[test]
fn membership_and_iteration_are_ordered() {
    with_set(|set| {
        for value in [1u64, 2, 5, 9] {
            intset_add_member(set, value).unwrap();
        }
        assert!(intset_is_member(set, 5));
        assert!(!intset_is_member(set, 6));
        assert_eq!(intset_num_entries(set), 4);
        assert_eq!(collect(set), std::vec![1, 2, 5, 9]);
    });
}

#[test]
fn rejects_out_of_order_and_insert_during_iteration() {
    with_set(|set| {
        intset_add_member(set, 10).unwrap();
        assert_eq!(
            intset_add_member(set, 10).unwrap_err().message(),
            "cannot add value to integer set out of order"
        );
        assert_eq!(
            intset_add_member(set, 9).unwrap_err().message(),
            "cannot add value to integer set out of order"
        );
        intset_begin_iterate(set);
        assert_eq!(
            intset_add_member(set, 11).unwrap_err().message(),
            "cannot add new values to integer set while iteration is in progress"
        );
    });
}

#[test]
fn out_of_order_error_uses_internal_sqlstate() {
    with_set(|set| {
        intset_add_member(set, 10).unwrap();
        let err = intset_add_member(set, 5).unwrap_err();
        // elog(ERROR, ...) => ERRCODE_INTERNAL_ERROR (XX000)
        assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    });
}

#[test]
fn simple8b_packed_values_round_trip_and_support_membership() {
    with_set(|set| {
        let n = MAX_BUFFERED_VALUES as u64 + 20;
        for value in 1..=n {
            intset_add_member(set, value).unwrap();
        }
        // The B-tree should have been populated, leaving a partial buffer.
        assert!((set.num_buffered_values as usize) < MAX_VALUES_PER_LEAF_ITEM);
        assert!(set.root.is_some());

        assert!(intset_is_member(set, 1));
        assert!(intset_is_member(set, 240));
        assert!(intset_is_member(set, n));
        assert!(!intset_is_member(set, n + 1));

        let values = collect(set);
        assert_eq!(values.len() as u64, n);
        assert_eq!(values[0], 1);
        assert_eq!(*values.last().unwrap(), n);
        // Strictly increasing, contiguous run.
        assert!(values.windows(2).all(|w| w[1] == w[0] + 1));
    });
}

#[test]
fn simple8b_handles_large_gaps_as_empty_codewords() {
    with_set(|set| {
        intset_add_member(set, 1).unwrap();
        let distant = 1 + (1_u64 << 61);
        for offset in 0..(MAX_BUFFERED_VALUES as u64 + 10) {
            intset_add_member(set, distant + offset).unwrap();
        }
        assert!(intset_is_member(set, 1));
        assert!(intset_is_member(set, distant));
        assert!(intset_is_member(set, distant + MAX_BUFFERED_VALUES as u64 + 9));
        assert!(!intset_is_member(set, 2));
    });
}

#[test]
fn dense_set_spanning_multiple_btree_levels() {
    // Enough values to force multiple leaf nodes and at least one internal
    // node, exercising intset_update_upper and the descent in is_member.
    with_set(|set| {
        let n: u64 = 200_000;
        for value in 0..n {
            intset_add_member(set, value).unwrap();
        }
        assert_eq!(intset_num_entries(set), n);
        assert!(set.num_levels >= 1);
        assert!(intset_memory_usage(set) > 0);

        assert!(intset_is_member(set, 0));
        assert!(intset_is_member(set, n / 2));
        assert!(intset_is_member(set, n - 1));
        assert!(!intset_is_member(set, n));

        let values = collect(set);
        assert_eq!(values.len() as u64, n);
        assert_eq!(values[0], 0);
        assert_eq!(*values.last().unwrap(), n - 1);
    });
}

#[test]
fn sparse_clustered_values_round_trip() {
    // Clusters of nearby values separated by large gaps, the typical workload
    // Simple-8b targets.
    with_set(|set| {
        let mut expected = Vec::new();
        let mut base = 0u64;
        for _cluster in 0..500 {
            for k in 0..300u64 {
                let v = base + k;
                intset_add_member(set, v).unwrap();
                expected.push(v);
            }
            base += 1_000_000;
        }
        for &v in &expected {
            assert!(intset_is_member(set, v), "missing {v}");
        }
        assert!(!intset_is_member(set, 500));
        assert_eq!(collect(set), expected);
    });
}

#[test]
fn empty_set_has_no_members() {
    with_set(|set| {
        assert_eq!(intset_num_entries(set), 0);
        assert!(!intset_is_member(set, 0));
        assert!(!intset_is_member(set, 42));
        assert_eq!(collect(set), Vec::<u64>::new());
    });
}

#[test]
fn multi_level_tree_grows_root() {
    // Push enough distinct, widely-spaced clusters to force the root to split
    // and the tree to grow at least one internal level, then verify the tree
    // height and that everything is still found and iterated in order.
    with_set(|set| {
        let mut expected = Vec::new();
        // 5000 clusters of 100 values, each cluster 1e6 apart, is ~500k values
        // and many leaf nodes, forcing internal nodes.
        let mut base = 0u64;
        for _ in 0..5000 {
            for k in 0..100u64 {
                let v = base + k;
                intset_add_member(set, v).unwrap();
                expected.push(v);
            }
            base += 1_000_000;
        }
        assert!(set.num_levels >= 2, "expected an internal level, got {}", set.num_levels);
        assert_eq!(intset_num_entries(set), expected.len() as u64);

        // Spot-check membership across the whole range.
        for &v in expected.iter().step_by(777) {
            assert!(intset_is_member(set, v), "missing {v}");
        }
        assert_eq!(collect(set), expected);
    });
}

#[test]
fn memory_usage_grows_with_nodes() {
    with_set(|set| {
        let empty = intset_memory_usage(set);
        assert!(empty > 0, "control object itself is accounted for");
        for value in 0..(MAX_BUFFERED_VALUES as u64 * 4) {
            intset_add_member(set, value).unwrap();
        }
        assert!(
            intset_memory_usage(set) > empty,
            "allocating B-tree nodes must increase mem_used"
        );
    });
}
