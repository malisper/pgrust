//! Tests for the idiomatic pairing heap, mirroring the faithful crate's suite.

use super::*;
use std::vec;
use std::vec::Vec;

#[test]
fn removes_values_in_max_heap_order() {
    let mut heap = pairingheap_allocate_ord();
    for value in [4, 1, 7, 3, 9, 2] {
        heap.add(value).unwrap();
    }

    let mut values = Vec::new();
    while let Some(value) = heap.remove_first() {
        values.push(value);
    }
    assert_eq!(values, vec![9, 7, 4, 3, 2, 1]);
    assert!(heap.is_empty());
}

#[test]
fn first_does_not_remove() {
    let mut heap = pairingheap_allocate_ord();
    heap.add(2).unwrap();
    heap.add(5).unwrap();

    assert_eq!(heap.first(), Some(&5));
    assert_eq!(heap.first(), Some(&5));
    assert_eq!(heap.remove_first(), Some(5));
    assert_eq!(heap.remove_first(), Some(2));
}

#[test]
fn first_on_empty_returns_none() {
    let heap: PairingHeap<i32, _> = pairingheap_allocate_ord();
    assert_eq!(heap.first(), None);
}

#[test]
fn remove_first_on_empty_returns_none() {
    let mut heap: PairingHeap<i32, _> = pairingheap_allocate_ord();
    assert_eq!(heap.remove_first(), None);
    assert!(heap.is_empty());
}

#[test]
fn arbitrary_remove_rebalances_heap() {
    let mut heap = pairingheap_allocate_ord();
    let _one = heap.add(1).unwrap();
    let seven = heap.add(7).unwrap();
    let _four = heap.add(4).unwrap();
    let nine = heap.add(9).unwrap();
    let _two = heap.add(2).unwrap();

    assert_eq!(heap.remove(seven), Some(7));
    assert_eq!(heap.remove_first(), Some(9));
    // 'nine' was already removed via remove_first; a stale handle yields None.
    assert_eq!(heap.remove(nine), None);
    assert_eq!(heap.remove_first(), Some(4));
    assert_eq!(heap.remove_first(), Some(2));
    assert_eq!(heap.remove_first(), Some(1));
    assert_eq!(heap.remove_first(), None);
}

#[test]
fn remove_root_via_handle_matches_remove_first() {
    let mut heap = pairingheap_allocate_ord();
    heap.add(1).unwrap();
    let nine = heap.add(9).unwrap(); // becomes the root (max)
    heap.add(4).unwrap();

    // Removing the root through the arbitrary-remove path delegates to
    // remove_first internally; it must return the root's value and rebalance.
    assert_eq!(heap.remove(nine), Some(9));
    assert_eq!(heap.remove_first(), Some(4));
    assert_eq!(heap.remove_first(), Some(1));
    assert!(heap.is_empty());
}

#[test]
fn singular_and_reset_match_postgres_macros() {
    let mut heap = pairingheap_allocate_ord();
    assert!(heap.is_empty());
    assert!(!heap.is_singular());

    heap.add(10).unwrap();
    assert!(heap.is_singular());
    let handle = heap.add(11).unwrap();
    assert!(!heap.is_singular());

    heap.reset();
    assert!(heap.is_empty());
    // A handle into a reset (and thus recycled) heap is stale.
    assert_eq!(heap.remove(handle), None);
}

#[test]
fn min_heap_via_reversed_comparator() {
    // Reversing the comparator turns the max-heap into a min-heap, exactly as the
    // C header documents.
    let mut heap = pairingheap_allocate(|a: &i32, b: &i32| b.cmp(a));
    for value in [4, 1, 7, 3, 9, 2] {
        heap.add(value).unwrap();
    }
    let mut values = Vec::new();
    while let Some(value) = heap.remove_first() {
        values.push(value);
    }
    assert_eq!(values, vec![1, 2, 3, 4, 7, 9]);
}

#[test]
fn comparator_can_ignore_non_key_payload() {
    #[derive(Debug, Eq, PartialEq)]
    struct Entry {
        key: i32,
        payload: i32,
    }

    let mut heap = pairingheap_allocate(|a: &Entry, b: &Entry| a.key.cmp(&b.key));
    heap.add(Entry { key: 2, payload: 10 }).unwrap();
    heap.add(Entry { key: 1, payload: 20 }).unwrap();
    heap.add(Entry { key: 3, payload: 30 }).unwrap();

    assert_eq!(heap.first().map(|e| e.key), Some(3));
    assert_eq!(
        heap.remove_first(),
        Some(Entry { key: 3, payload: 30 })
    );
}

/// A larger randomized-shape stress: heap-sort a deterministic pseudo-random
/// sequence and compare against a sorted reference. Exercises many merge /
/// merge_children rebalancing paths (slot recycling included).
#[test]
fn large_stress_matches_sorted_reference() {
    let mut heap = pairingheap_allocate_ord();
    let mut x: u64 = 12345;
    let mut keys: Vec<i64> = Vec::new();
    for _ in 0..2000 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let k = (x >> 33) as i64;
        heap.add(k).unwrap();
        keys.push(k);
    }

    // Descending heap-sort equals the reverse-sorted reference (max-heap).
    keys.sort_unstable();
    keys.reverse();
    let mut drained = Vec::new();
    while let Some(v) = heap.remove_first() {
        drained.push(v);
    }
    assert_eq!(drained, keys);
    assert!(heap.is_empty());
}

/// Interleaved adds, arbitrary removes, and remove_firsts against a reference
/// max-set, checking the reported max always matches and slots recycle cleanly.
#[test]
fn interleaved_add_remove_keeps_max() {
    let mut heap = pairingheap_allocate_ord();
    let mut handles: Vec<(PairingHeapHandle, i64)> = Vec::new();
    let mut x: u64 = 99;

    for _ in 0..500 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let k = ((x >> 40) % 1000) as i64;
        let h = heap.add(k).unwrap();
        handles.push((h, k));
    }

    // Arbitrarily remove every other inserted node by handle.
    let mut live: Vec<i64> = Vec::new();
    for (i, (h, k)) in handles.into_iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(heap.remove(h), Some(k));
        } else {
            live.push(k);
        }
    }

    // The first() must equal the live maximum at each step as we drain.
    live.sort_unstable();
    while let Some(&expected_max) = live.last() {
        assert_eq!(heap.first().copied(), Some(expected_max));
        assert_eq!(heap.remove_first(), Some(expected_max));
        live.pop();
    }
    assert!(heap.is_empty());
}

/// The arena moves payloads (never clones) and recycles slots without running a
/// destructor on a moved-from slot. Prove no leak / no double-drop with a payload
/// that bumps a shared live-counter on construction and decrements on `Drop`,
/// across add / remove / remove_first / reset / final drop.
#[test]
fn payloads_move_without_clone_or_double_drop() {
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
    {
        let mut heap = pairingheap_allocate(|a: &Tracked, b: &Tracked| a.key.cmp(&b.key));
        let mut handles = Vec::new();
        for key in [10, 5, 15, 3, 7, 11, 20] {
            handles.push((key, heap.add(Tracked::new(key, &live)).unwrap()));
        }
        assert_eq!(live.get(), 7);

        // Remove one by handle: the returned value is dropped here.
        let (_, h15) = handles.iter().find(|(k, _)| *k == 15).copied().unwrap();
        let v = heap.remove(h15).unwrap();
        assert_eq!(v.key, 15);
        drop(v);
        assert_eq!(live.get(), 6);

        // Pop the max (20), drop it.
        let top = heap.remove_first().unwrap();
        assert_eq!(top.key, 20);
        drop(top);
        assert_eq!(live.get(), 5);

        // Reset frees the remaining 5 stored payloads.
        heap.reset();
        assert_eq!(live.get(), 0);
        assert!(heap.is_empty());

        // Add a couple more, then let the heap drop with them still inside.
        heap.add(Tracked::new(1, &live)).unwrap();
        heap.add(Tracked::new(2, &live)).unwrap();
        assert_eq!(live.get(), 2);
        // heap drops here.
    }
    assert_eq!(live.get(), 0, "payload leaked or was double-dropped");
}
