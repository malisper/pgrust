//! Tests for the C-faithful red-black tree, mirroring
//! `src/test/modules/test_rbtree/test_rbtree.c`: an `IntRBTreeNode` embeds an
//! `RBTNode` as its first field plus an `int key`, with `irbt_cmp`/`irbt_combine`/
//! `irbt_alloc`/`irbt_free` support functions and the same insert/find/iterate
//! coverage.

use super::*;
use core::ffi::c_void;
use core::ptr;
use std::boxed::Box;
use std::vec;
use std::vec::Vec;

/// Our test trees store an integer key, and nothing else. `RBTNode` must be the
/// first field (the C intrusive contract).
#[repr(C)]
#[derive(Copy, Clone)]
struct IntRBTreeNode {
    rbtnode: RBTNode,
    key: core::ffi::c_int,
}

fn zeroed_int_node() -> IntRBTreeNode {
    IntRBTreeNode {
        rbtnode: RBTNode {
            color: 0,
            left: ptr::null_mut(),
            right: ptr::null_mut(),
            parent: ptr::null_mut(),
        },
        key: 0,
    }
}

/// Node comparator (`irbt_cmp`): keys are non-negative so the subtraction is fine.
unsafe extern "C" fn irbt_cmp(a: *const RBTNode, b: *const RBTNode, _arg: *mut c_void) -> core::ffi::c_int {
    let ea = a as *const IntRBTreeNode;
    let eb = b as *const IntRBTreeNode;
    (*ea).key - (*eb).key
}

/// Node combiner (`irbt_combine`): for testing, just check the library doesn't
/// try to combine unequal keys.
unsafe extern "C" fn irbt_combine(existing: *mut RBTNode, newdata: *const RBTNode, _arg: *mut c_void) {
    let eexist = existing as *const IntRBTreeNode;
    let enew = newdata as *const IntRBTreeNode;
    assert_eq!(
        (*eexist).key,
        (*enew).key,
        "red-black tree combines {} into {}",
        (*enew).key,
        (*eexist).key
    );
}

/// Node allocator (`irbt_alloc`): `palloc(sizeof(IntRBTreeNode))`.
unsafe extern "C" fn irbt_alloc(_arg: *mut c_void) -> *mut RBTNode {
    let node: *mut IntRBTreeNode = Box::into_raw(Box::new(zeroed_int_node()));
    node as *mut RBTNode
}

/// Node freer (`irbt_free`): `pfree(node)`.
unsafe extern "C" fn irbt_free(node: *mut RBTNode, _arg: *mut c_void) {
    drop(Box::from_raw(node as *mut IntRBTreeNode));
}

/// Create a red-black tree using our support functions.
unsafe fn create_int_rbtree() -> *mut RBTree {
    rbt_create(
        core::mem::size_of::<IntRBTreeNode>(),
        Some(irbt_cmp),
        Some(irbt_combine),
        Some(irbt_alloc),
        Some(irbt_free),
        ptr::null_mut(),
    )
}

/// Build a probe node carrying `key` (RBTNode fields need not be valid).
fn probe(key: core::ffi::c_int) -> IntRBTreeNode {
    let mut n = zeroed_int_node();
    n.key = key;
    n
}

unsafe fn insert(tree: *mut RBTree, key: core::ffi::c_int) -> bool {
    let n = probe(key);
    let mut is_new = false;
    rbt_insert(tree, &n as *const IntRBTreeNode as *const RBTNode, &mut is_new);
    is_new
}

unsafe fn find_key(tree: *mut RBTree, key: core::ffi::c_int) -> Option<core::ffi::c_int> {
    let n = probe(key);
    let r = rbt_find(tree, &n as *const IntRBTreeNode as *const RBTNode);
    if r.is_null() {
        None
    } else {
        Some((*(r as *const IntRBTreeNode)).key)
    }
}

unsafe fn drain_free(tree: *mut RBTree) {
    // Delete every remaining node, freeing storage (mirrors C resetting the
    // memory context). Repeatedly remove leftmost until empty.
    loop {
        let lm = rbt_leftmost(tree);
        if lm.is_null() {
            break;
        }
        rbt_delete(tree, lm);
    }
    drop(Box::from_raw(tree));
}

#[test]
fn insert_find_delete_and_bounds_work() {
    unsafe {
        let tree = create_int_rbtree();
        assert!(insert(tree, 2));
        assert!(insert(tree, 1));
        assert!(insert(tree, 3));
        assert!(!insert(tree, 2)); // duplicate -> combiner, not new

        assert_eq!(find_key(tree, 2), Some(2));

        let p1 = probe(2);
        let less = rbt_find_less(tree, &p1 as *const _ as *const RBTNode, false);
        assert_eq!((*(less as *const IntRBTreeNode)).key, 1);
        let less_eq = rbt_find_less(tree, &p1 as *const _ as *const RBTNode, true);
        assert_eq!((*(less_eq as *const IntRBTreeNode)).key, 2);
        let great = rbt_find_great(tree, &p1 as *const _ as *const RBTNode, false);
        assert_eq!((*(great as *const IntRBTreeNode)).key, 3);
        let great_eq = rbt_find_great(tree, &p1 as *const _ as *const RBTNode, true);
        assert_eq!((*(great_eq as *const IntRBTreeNode)).key, 2);

        let lm = rbt_leftmost(tree);
        assert_eq!((*(lm as *const IntRBTreeNode)).key, 1);

        let p2 = probe(2);
        let node2 = rbt_find(tree, &p2 as *const _ as *const RBTNode);
        rbt_delete(tree, node2);
        assert_eq!(find_key(tree, 2), None);

        drain_free(tree);
    }
}

#[test]
fn find_on_empty_tree_returns_none() {
    unsafe {
        let tree = create_int_rbtree();
        let p = probe(1);
        assert!(rbt_find(tree, &p as *const _ as *const RBTNode).is_null());
        assert!(rbt_find_great(tree, &p as *const _ as *const RBTNode, true).is_null());
        assert!(rbt_find_less(tree, &p as *const _ as *const RBTNode, true).is_null());
        assert!(rbt_leftmost(tree).is_null());

        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, LeftRightWalk, &mut iter).unwrap();
        assert!(rbt_iterate(&mut iter).is_null());

        drop(Box::from_raw(tree));
    }
}

#[test]
fn iteration_supports_both_orders() {
    unsafe {
        let tree = create_int_rbtree();
        for v in [2, 1, 3] {
            insert(tree, v);
        }

        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, LeftRightWalk, &mut iter).unwrap();
        let mut seen = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            seen.push((*(n as *const IntRBTreeNode)).key);
        }
        assert_eq!(seen, vec![1, 2, 3]);

        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, RightLeftWalk, &mut iter).unwrap();
        let mut rseen = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            rseen.push((*(n as *const IntRBTreeNode)).key);
        }
        assert_eq!(rseen, vec![3, 2, 1]);

        drain_free(tree);
    }
}

#[test]
fn combiner_keeps_existing_node() {
    unsafe {
        let tree = create_int_rbtree();
        assert!(insert(tree, 2));
        assert!(insert(tree, 1));
        // Re-inserting key 2 hits the combiner (which only asserts equal keys)
        // and reports not-new.
        assert!(!insert(tree, 2));
        assert_eq!(find_key(tree, 2), Some(2));
        drain_free(tree);
    }
}

#[test]
fn bad_iteration_order_errors() {
    unsafe {
        let tree = create_int_rbtree();
        insert(tree, 1);
        let mut iter: RBTreeIterator = core::mem::zeroed();
        let r = rbt_begin_iterate(tree, 99, &mut iter);
        assert!(r.is_err());
        drain_free(tree);
    }
}

#[test]
fn stress_insert_delete_keeps_order() {
    unsafe {
        let tree = create_int_rbtree();
        for v in [8, 3, 10, 1, 6, 14, 4, 7, 13, 2, 5, 9, 11, 12] {
            assert!(insert(tree, v));
        }
        for v in [1, 14, 6, 8, 11] {
            let p = probe(v);
            let node = rbt_find(tree, &p as *const _ as *const RBTNode);
            assert!(!node.is_null());
            rbt_delete(tree, node);
        }

        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, LeftRightWalk, &mut iter).unwrap();
        let mut values = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            values.push((*(n as *const IntRBTreeNode)).key);
        }
        assert_eq!(values, vec![2, 3, 4, 5, 7, 9, 10, 12, 13]);

        drain_free(tree);
    }
}

#[test]
fn large_stress_matches_sorted_reference() {
    unsafe {
        let tree = create_int_rbtree();
        // Deterministic LCG-driven keys; keep only newly-inserted ones.
        let mut x: u64 = 12345;
        let mut keys: Vec<core::ffi::c_int> = Vec::new();
        for _ in 0..3000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = ((x >> 40) & 0x7fff) as core::ffi::c_int; // non-negative
            if insert(tree, k) {
                keys.push(k);
            }
        }

        keys.sort_unstable();
        for k in &keys {
            assert_eq!(find_key(tree, *k), Some(*k));
        }

        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, LeftRightWalk, &mut iter).unwrap();
        let mut seen = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            seen.push((*(n as *const IntRBTreeNode)).key);
        }
        assert_eq!(seen, keys);

        // Reverse traversal.
        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, RightLeftWalk, &mut iter).unwrap();
        let mut rseen = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            rseen.push((*(n as *const IntRBTreeNode)).key);
        }
        let mut rev = keys.clone();
        rev.reverse();
        assert_eq!(rseen, rev);

        // Drain half, recheck ordering of the remainder.
        let (drop_half, keep_half) = keys.split_at(keys.len() / 2);
        for k in drop_half {
            let p = probe(*k);
            let node = rbt_find(tree, &p as *const _ as *const RBTNode);
            assert!(!node.is_null());
            rbt_delete(tree, node);
        }
        for k in drop_half {
            assert_eq!(find_key(tree, *k), None);
        }
        let mut kept: Vec<core::ffi::c_int> = keep_half.to_vec();
        kept.sort_unstable();
        let mut iter: RBTreeIterator = core::mem::zeroed();
        rbt_begin_iterate(tree, LeftRightWalk, &mut iter).unwrap();
        let mut seen = Vec::new();
        loop {
            let n = rbt_iterate(&mut iter);
            if n.is_null() {
                break;
            }
            seen.push((*(n as *const IntRBTreeNode)).key);
        }
        assert_eq!(seen, kept);

        drain_free(tree);
    }
}

#[test]
fn find_great_less_with_gaps() {
    unsafe {
        let tree = create_int_rbtree();
        for v in [10, 20, 30, 40, 50] {
            insert(tree, v);
        }
        let g = |k: core::ffi::c_int, eq: bool| {
            let p = probe(k);
            let r = rbt_find_great(tree, &p as *const _ as *const RBTNode, eq);
            if r.is_null() {
                None
            } else {
                Some((*(r as *const IntRBTreeNode)).key)
            }
        };
        let l = |k: core::ffi::c_int, eq: bool| {
            let p = probe(k);
            let r = rbt_find_less(tree, &p as *const _ as *const RBTNode, eq);
            if r.is_null() {
                None
            } else {
                Some((*(r as *const IntRBTreeNode)).key)
            }
        };
        assert_eq!(g(25, false), Some(30));
        assert_eq!(g(25, true), Some(30));
        assert_eq!(g(30, false), Some(40));
        assert_eq!(g(50, false), None);
        assert_eq!(l(25, false), Some(20));
        assert_eq!(l(30, false), Some(20));
        assert_eq!(l(10, false), None);

        drain_free(tree);
    }
}
