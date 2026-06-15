//! Smoke tests for the join-relation enumerator port. These exercise the pure
//! field-only helpers and the `init_dummy_sjinfo` initializer (no seam providers
//! required); the enumeration core itself drives the relnode/pathnode/joinpath
//! seams whose owners are not yet ported.

use super::*;

#[test]
fn init_dummy_sjinfo_is_inner_with_aliased_sides() {
    let mut sj = make_dummy_sjinfo();
    sj.jointype = JOIN_LEFT; // perturb to prove init resets it
    init_dummy_sjinfo(&mut sj, None, None);
    assert_eq!(sj.jointype, JOIN_INNER);
    assert_eq!(sj.ojrelid, 0);
    assert!(!sj.lhs_strict);
    assert!(sj.commute_above_l.is_none());
    assert!(sj.semi_operators.is_empty());
}

#[test]
fn is_simple_rel_predicate() {
    let mut rel = types_pathnodes::RelOptInfo::default();
    rel.reloptkind = RELOPT_BASEREL;
    assert!(is_simple_rel(&rel));
    rel.reloptkind = RELOPT_OTHER_MEMBER_REL;
    assert!(is_simple_rel(&rel));
}

#[test]
fn is_partitioned_rel_requires_all_fields() {
    let rel = types_pathnodes::RelOptInfo::default();
    // default: no part_scheme, no boundinfo, nparts 0
    assert!(!is_partitioned_rel(&rel));
    assert!(!rel_has_all_part_props(&rel));
}
