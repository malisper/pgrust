//! Unit tests for the in-crate pure helpers (`itemptr_comparator`, `qunique`,
//! `tid_list_contains`) — the parts of `nodeTidscan.c` that depend on no
//! unported subsystem.

use super::*;
use types_tuple::heaptuple::ItemPointerData;

fn tid(blk: u32, off: u16) -> ItemPointerData {
    ItemPointerData::new(blk, off)
}

#[test]
fn comparator_orders_by_block_then_offset() {
    use core::cmp::Ordering;
    assert_eq!(itemptr_comparator(&tid(1, 5), &tid(2, 1)), Ordering::Less);
    assert_eq!(itemptr_comparator(&tid(2, 1), &tid(1, 5)), Ordering::Greater);
    assert_eq!(itemptr_comparator(&tid(3, 2), &tid(3, 7)), Ordering::Less);
    assert_eq!(itemptr_comparator(&tid(3, 7), &tid(3, 2)), Ordering::Greater);
    assert_eq!(itemptr_comparator(&tid(3, 7), &tid(3, 7)), Ordering::Equal);
}

#[test]
fn qunique_removes_adjacent_duplicates() {
    let mut a = [tid(1, 1), tid(1, 1), tid(1, 2), tid(2, 1), tid(2, 1)];
    let n = qunique(&mut a);
    assert_eq!(n, 3);
    assert_eq!(&a[..n], &[tid(1, 1), tid(1, 2), tid(2, 1)]);
}

#[test]
fn qunique_handles_short_slices() {
    let mut empty: [ItemPointerData; 0] = [];
    assert_eq!(qunique(&mut empty), 0);
    let mut one = [tid(5, 5)];
    assert_eq!(qunique(&mut one), 1);
}

#[test]
fn bsearch_finds_present_and_misses_absent() {
    let list = [tid(1, 1), tid(1, 2), tid(2, 1), tid(5, 9)];
    assert!(tid_list_contains(&list, &tid(2, 1)));
    assert!(tid_list_contains(&list, &tid(5, 9)));
    assert!(!tid_list_contains(&list, &tid(2, 2)));
    assert!(!tid_list_contains(&list, &tid(9, 9)));
}

#[test]
fn is_ctid_var_recognizes_self_item_pointer() {
    use types_nodes::primnodes::{Expr, Var};
    let ctid = Expr::Var(Var {
        varattno: SelfItemPointerAttributeNumber,
        ..Var::default()
    });
    let other = Expr::Var(Var {
        varattno: 3,
        ..Var::default()
    });
    assert!(is_ctid_var(Some(&ctid)));
    assert!(!is_ctid_var(Some(&other)));
    assert!(!is_ctid_var(None));
}
