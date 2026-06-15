//! Unit tests for the lifetime/seam-free pure helpers of parse_clause.c.

extern crate std;

use super::*;
use mcx::MemoryContext;
use types_nodes::primnodes::{Const, Expr};

/// A `TargetEntry` carrying a trivial non-NULL `Const` expr leaf.
fn tle<'mcx>(mcx: Mcx<'mcx>, ressortgroupref: Index) -> TargetEntry<'mcx> {
    let c = Const {
        consttype: INT8OID,
        consttypmod: -1,
        ..Default::default()
    };
    TargetEntry {
        expr: Some(alloc_in(mcx, Expr::Const(c)).unwrap()),
        resno: 1,
        resname: None,
        ressortgroupref,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk: false,
    }
}

#[test]
fn assign_sort_group_ref_reuses_existing() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut tlist = std::vec![tle(mcx, 7), tle(mcx, 0)];
    // entry 0 already has a ref -> returned unchanged
    assert_eq!(assignSortGroupRef(0, &mut tlist), 7);
    // entry 1 gets max+1 = 8
    assert_eq!(assignSortGroupRef(1, &mut tlist), 8);
    assert_eq!(tlist[1].ressortgroupref, 8);
    // idempotent
    assert_eq!(assignSortGroupRef(1, &mut tlist), 8);
}

#[test]
fn target_is_in_sort_list_marker_zero_is_false() {
    let ctx = MemoryContext::new("t");
    let t = tle(ctx.mcx(), 0);
    assert!(!targetIsInSortList(&t, InvalidOid, &[]));
}

#[test]
fn target_is_in_sort_list_matches_ref() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t = tle(mcx, 3);
    let scl = SortGroupClause {
        tleSortGroupRef: 3,
        eqop: 0,
        sortop: 0,
        reverse_sort: false,
        nulls_first: false,
        hashable: false,
    };
    // InvalidOid sortop ignores the sortop comparison
    assert!(targetIsInSortList(&t, InvalidOid, &[scl]));
    // mismatched ref
    let t2 = tle(mcx, 4);
    assert!(!targetIsInSortList(&t2, InvalidOid, &[scl]));
}

#[test]
fn leftmost_loc_semantics() {
    assert_eq!(leftmost_loc(-1, 5), 5);
    assert_eq!(leftmost_loc(5, -1), 5);
    assert_eq!(leftmost_loc(3, 7), 3);
    assert_eq!(leftmost_loc(-1, -1), -1);
}
