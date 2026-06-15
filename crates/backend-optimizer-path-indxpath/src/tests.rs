//! Smoke tests for the indxpath port. Full functional tests require a populated
//! `PlannerInfo` arena (rel/index/rinfo) plus the cross-subsystem seam providers
//! (pathnode/costsize/pathkeys/equivclass/predtest/restrictinfo/lsyscache),
//! none of which is installed in this leaf crate's test environment, so here we
//! only assert the scalar/seam-free helpers.

use crate::matchers::{IndexClauseSet, IsBooleanOpfamily};
use crate::util::{BOOL_BTREE_FAM_OID, BOOL_HASH_FAM_OID};

#[test]
fn is_boolean_opfamily_builtin() {
    // Built-in bool opfamilies are recognized without a catalog lookup.
    assert!(IsBooleanOpfamily(BOOL_BTREE_FAM_OID));
    assert!(IsBooleanOpfamily(BOOL_HASH_FAM_OID));
    // A different built-in opfamily is not a boolean opfamily.
    assert!(!IsBooleanOpfamily(1));
}

#[test]
fn index_clause_set_new_is_empty() {
    let cs = IndexClauseSet::new(3);
    assert_eq!(cs.indexclauses.len(), 3);
    assert!(!cs.nonempty);
    assert!(cs.indexclauses.iter().all(|c| c.is_empty()));
}

#[test]
fn relids_next_member_iterates_bits() {
    use crate::util::relids_next_member;
    use types_pathnodes::Bitmapset;
    // bits 0, 3, 65 set.
    let relids = Some(alloc::boxed::Box::new(Bitmapset {
        words: alloc::vec![0b1001u64, 0b10u64],
    }));
    let mut got = alloc::vec::Vec::new();
    let mut m = -1;
    loop {
        m = relids_next_member(&relids, m);
        if m < 0 {
            break;
        }
        got.push(m);
    }
    assert_eq!(got, alloc::vec![0, 3, 65]);
}
