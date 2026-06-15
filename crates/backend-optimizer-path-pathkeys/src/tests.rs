//! Unit tests for the pure (no-seam) pathkey comparison helpers.

use super::*;
use backend_optimizer_util_pathnode_seams::PathKeysComparison;

fn pk(ec: u32) -> PathKey {
    PathKey {
        pk_eclass: Some(EcId(ec)),
        pk_opfamily: 0,
        pk_cmptype: COMPARE_LT,
        pk_nulls_first: false,
    }
}

#[test]
fn compare_pathkeys_equal_and_containment() {
    let a = alloc::vec![pk(0), pk(1)];
    let b = alloc::vec![pk(0), pk(1)];
    assert_eq!(compare_pathkeys(&a, &b), PathKeysComparison::Equal);

    let shorter = alloc::vec![pk(0)];
    // keys1 longer => BETTER1; keys2 longer => BETTER2.
    assert_eq!(
        compare_pathkeys(&a, &shorter),
        PathKeysComparison::Better1
    );
    assert_eq!(
        compare_pathkeys(&shorter, &a),
        PathKeysComparison::Better2
    );

    // keys2 at least as well sorted as keys1.
    assert!(pathkeys_contained_in(&shorter, &a));
    assert!(!pathkeys_contained_in(&a, &shorter));
}

#[test]
fn compare_pathkeys_different() {
    let a = alloc::vec![pk(0), pk(1)];
    let c = alloc::vec![pk(0), pk(2)];
    assert_eq!(
        compare_pathkeys(&a, &c),
        PathKeysComparison::Different
    );
}

#[test]
fn count_contained_in_common_prefix() {
    let a = alloc::vec![pk(0), pk(1)];
    let b = alloc::vec![pk(0), pk(1), pk(2)];
    let (contained, n) = pathkeys_count_contained_in(&a, &b);
    assert!(contained);
    assert_eq!(n, 2);

    let (contained2, n2) = pathkeys_count_contained_in(&b, &a);
    assert!(!contained2);
    assert_eq!(n2, 2);

    let empty: alloc::vec::Vec<PathKey> = alloc::vec![];
    assert_eq!(pathkeys_count_contained_in(&empty, &a), (true, 0));
    assert_eq!(pathkeys_count_contained_in(&a, &empty), (false, 0));
}
