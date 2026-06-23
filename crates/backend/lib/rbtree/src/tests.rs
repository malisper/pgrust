//! Tests for the safe red-black tree, mirroring `src/test/modules/test_rbtree`
//! (insert/find/find_great/find_less/leftmost/iterate coverage) plus
//! payload-ownership and stress tests proving the arena port is memory-safe.

use super::*;
use std::vec;
use std::vec::Vec;

#[test]
fn insert_find_delete_and_bounds_work() {
    let mut tree = rbt_create();
    assert!(rbt_insert(&mut tree, 2, |_, _| {}).unwrap());
    assert!(rbt_insert(&mut tree, 1, |_, _| {}).unwrap());
    assert!(rbt_insert(&mut tree, 3, |_, _| {}).unwrap());
    assert!(!rbt_insert(&mut tree, 2, |_, _| {}).unwrap());

    assert_eq!(rbt_find(&tree, &2), Some(&2));
    assert_eq!(rbt_find_less(&tree, &2, false), Some(&1));
    assert_eq!(rbt_find_less(&tree, &2, true), Some(&2));
    assert_eq!(rbt_find_great(&tree, &2, false), Some(&3));
    assert_eq!(rbt_find_great(&tree, &2, true), Some(&2));
    assert_eq!(rbt_leftmost(&tree), Some(&1));

    assert_eq!(rbt_delete(&mut tree, &2).unwrap(), Some(2));
    assert_eq!(rbt_find(&tree, &2), None);
}

#[test]
fn find_on_empty_tree_returns_none() {
    let tree: RBTree<i32, _> = rbt_create();
    assert_eq!(rbt_find(&tree, &1), None);
    assert_eq!(rbt_find_great(&tree, &1, true), None);
    assert_eq!(rbt_find_less(&tree, &1, true), None);
    assert_eq!(rbt_leftmost(&tree), None);

    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    assert_eq!(rbt_iterate(&mut iter), None);
}

#[test]
fn delete_missing_key_returns_none() {
    let mut tree = rbt_create();
    rbt_insert(&mut tree, 5, |_, _| {}).unwrap();
    assert_eq!(rbt_delete(&mut tree, &99).unwrap(), None);
    assert_eq!(rbt_find(&tree, &5), Some(&5));
}

#[test]
fn iteration_supports_both_orders() {
    let mut tree = rbt_create();
    for value in [2, 1, 3] {
        rbt_insert(&mut tree, value, |_, _| {}).unwrap();
    }

    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    assert_eq!(rbt_iterate(&mut iter), Some(&1));
    assert_eq!(rbt_iterate(&mut iter), Some(&2));
    assert_eq!(rbt_iterate(&mut iter), Some(&3));
    assert_eq!(rbt_iterate(&mut iter), None);

    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::RightLeftWalk);
    assert_eq!(rbt_iterate(&mut iter), Some(&3));
    assert_eq!(rbt_iterate(&mut iter), Some(&2));
    assert_eq!(rbt_iterate(&mut iter), Some(&1));
    assert_eq!(rbt_iterate(&mut iter), None);
}

#[test]
fn comparator_can_ignore_non_key_payload() {
    #[derive(Debug, Eq, PartialEq)]
    struct Entry {
        key: i32,
        payload: i32,
    }

    let mut tree = rbt_create_with(|left: &Entry, right: &Entry| left.key.cmp(&right.key));
    assert!(rbt_insert(
        &mut tree,
        Entry {
            key: 2,
            payload: 10
        },
        |_, _| {}
    )
    .unwrap());
    assert!(rbt_insert(
        &mut tree,
        Entry {
            key: 1,
            payload: 20
        },
        |_, _| {}
    )
    .unwrap());
    assert!(
        !rbt_insert(&mut tree, Entry { key: 2, payload: 5 }, |existing, new| {
            existing.payload += new.payload
        })
        .unwrap()
    );

    assert_eq!(
        rbt_find(&tree, &Entry { key: 2, payload: 0 }),
        Some(&Entry {
            key: 2,
            payload: 15
        })
    );
}

#[test]
fn stress_insert_delete_keeps_order() {
    let mut tree = rbt_create();
    for value in [8, 3, 10, 1, 6, 14, 4, 7, 13, 2, 5, 9, 11, 12] {
        assert!(rbt_insert(&mut tree, value, |_, _| {}).unwrap());
    }
    for value in [1, 14, 6, 8, 11] {
        assert_eq!(rbt_delete(&mut tree, &value).unwrap(), Some(value));
    }

    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    let mut values = Vec::new();
    while let Some(value) = rbt_iterate(&mut iter) {
        values.push(*value);
    }
    assert_eq!(values, vec![2, 3, 4, 5, 7, 9, 10, 12, 13]);
}

/// A larger randomized-shape stress, checking ordered traversal stays exact and
/// every key is found, then drained, against a sorted reference. This exercises
/// many insert/delete rebalancing paths (slot recycling included).
#[test]
fn large_stress_matches_sorted_reference() {
    let mut tree = rbt_create();
    // Linear-congruential pseudo-random permutation-ish sequence (deterministic).
    let mut x: u64 = 12345;
    let mut keys: Vec<i64> = Vec::new();
    for _ in 0..2000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let k = (x >> 33) as i64;
        // rbt_insert returns false for duplicates; only keep keys we actually add.
        if rbt_insert(&mut tree, k, |_, _| {}).unwrap() {
            keys.push(k);
        }
    }

    keys.sort_unstable();
    // Every inserted key is found.
    for k in &keys {
        assert_eq!(rbt_find(&tree, k), Some(k));
    }
    // In-order traversal equals the sorted reference.
    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    let mut seen = Vec::new();
    while let Some(v) = rbt_iterate(&mut iter) {
        seen.push(*v);
    }
    assert_eq!(seen, keys);

    // Reverse traversal equals the reversed reference.
    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::RightLeftWalk);
    let mut rseen = Vec::new();
    while let Some(v) = rbt_iterate(&mut iter) {
        rseen.push(*v);
    }
    let mut rev = keys.clone();
    rev.reverse();
    assert_eq!(rseen, rev);

    // Drain half, then re-check ordering of the remainder (recycles slots).
    let (drop_half, keep_half) = keys.split_at(keys.len() / 2);
    for k in drop_half {
        assert_eq!(rbt_delete(&mut tree, k).unwrap(), Some(*k));
    }
    for k in drop_half {
        assert_eq!(rbt_find(&tree, k), None);
    }
    let mut kept: Vec<i64> = keep_half.to_vec();
    kept.sort_unstable();
    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    let mut seen = Vec::new();
    while let Some(v) = rbt_iterate(&mut iter) {
        seen.push(*v);
    }
    assert_eq!(seen, kept);
}

#[test]
fn find_great_less_with_gaps() {
    let mut tree = rbt_create();
    for v in [10, 20, 30, 40, 50] {
        rbt_insert(&mut tree, v, |_, _| {}).unwrap();
    }
    // Strictly-greater searches with non-member probes.
    assert_eq!(rbt_find_great(&tree, &25, false), Some(&30));
    assert_eq!(rbt_find_great(&tree, &25, true), Some(&30));
    assert_eq!(rbt_find_great(&tree, &30, false), Some(&40));
    assert_eq!(rbt_find_great(&tree, &50, false), None);
    // Strictly-less searches.
    assert_eq!(rbt_find_less(&tree, &25, false), Some(&20));
    assert_eq!(rbt_find_less(&tree, &30, false), Some(&20));
    assert_eq!(rbt_find_less(&tree, &10, false), None);
}

// The C `rbt_copy_data` is a raw memcpy that bit-moves the successor's payload,
// and `freefunc` (== pfree) never runs a destructor. The arena port mirrors
// that with a value move between slots (no clone) plus a non-dropping free, so
// deleting a node with two children (the successor-removal path) must MOVE the
// payload -- never clone it, never double-drop it, never leak it. We prove this
// with a payload that bumps a shared live-counter on construction and decrements
// it on `Drop`: the counter must never go negative (no double-drop / use-after-
// free) and must return to zero once all values are accounted for (no leak),
// even though the payload is neither `Copy` nor `Clone`.
#[test]
fn delete_moves_payload_without_clone_or_double_drop() {
    use std::cell::Cell;
    use std::rc::Rc;

    struct Tracked {
        key: i32,
        live: Rc<Cell<i64>>,
    }
    impl Tracked {
        fn new(key: i32, live: &Rc<Cell<i64>>) -> Self {
            live.set(live.get() + 1);
            Tracked {
                key,
                live: Rc::clone(live),
            }
        }
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            let n = self.live.get() - 1;
            assert!(n >= 0, "double-drop / use-after-free detected");
            self.live.set(n);
        }
    }

    let live = Rc::new(Cell::new(0i64));
    let mut tree = rbt_create_with(|a: &Tracked, b: &Tracked| a.key.cmp(&b.key));

    // Root (10) ends up with two children, so deleting it takes the y != z
    // successor-removal branch (successor 11 is moved into 10).
    for key in [10, 5, 15, 3, 7, 11, 20] {
        assert!(rbt_insert(&mut tree, Tracked::new(key, &live), |_, _| {}).unwrap());
    }
    // 7 stored values are alive.
    assert_eq!(live.get(), 7);

    // Delete the two-child node 10: returns the value logically at 10; 11's
    // payload is moved into that slot (no clone, no extra drop).
    let removed = rbt_delete(&mut tree, &Tracked::new(10, &live))
        .unwrap()
        .unwrap();
    assert_eq!(removed.key, 10);
    drop(removed);

    // Tree is still ordered and intact, with 11 now occupying 10's slot.
    let mut iter = rbt_begin_iterate(&tree, RBTOrderControl::LeftRightWalk);
    let mut keys = Vec::new();
    while let Some(t) = rbt_iterate(&mut iter) {
        keys.push(t.key);
    }
    assert_eq!(keys, vec![3, 5, 7, 11, 15, 20]);
    // 6 stored values remain alive (the probe + returned 10 already dropped).
    assert_eq!(live.get(), 6);

    // Drain the rest; each delete returns one live value that we drop.
    for key in [3, 5, 7, 11, 15, 20] {
        let v = rbt_delete(&mut tree, &Tracked::new(key, &live))
            .unwrap()
            .unwrap();
        assert_eq!(v.key, key);
    }

    // Nothing leaked and nothing was double-dropped: the counter is back to zero
    // with every constructed Tracked having been dropped exactly once.
    assert_eq!(live.get(), 0, "payload leaked or was double-dropped");
}

/// Dropping the whole tree drops every still-stored payload exactly once (no
/// leak): the arena `Vec`s own the nodes and reclaim them on `Drop`.
#[test]
fn dropping_tree_drops_all_payloads() {
    use std::cell::Cell;
    use std::rc::Rc;

    struct Tracked {
        live: Rc<Cell<i64>>,
        key: i32,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.live.set(self.live.get() - 1);
        }
    }

    let live = Rc::new(Cell::new(0i64));
    {
        let mut tree = rbt_create_with(|a: &Tracked, b: &Tracked| a.key.cmp(&b.key));
        for key in 0..100 {
            live.set(live.get() + 1);
            rbt_insert(
                &mut tree,
                Tracked {
                    live: Rc::clone(&live),
                    key,
                },
                |_, _| {},
            )
            .unwrap();
        }
        assert_eq!(live.get(), 100);
        // tree drops here.
    }
    assert_eq!(live.get(), 0, "dropping the tree must drop every payload once");
}
