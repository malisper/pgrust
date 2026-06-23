//! Unit tests for the pure (seam-free) parts of the nbtree AM handler.

use super::*;

// bthandler: flag/strategy/support fields exactly as in C.
#[test]
fn bthandler_sets_flags_strategies_support() {
    let r = bthandler();
    assert_eq!(r.type_, T_IndexAmRoutine);
    assert_eq!(r.amstrategies, 5);
    assert_eq!(r.amsupport, 6);
    assert_eq!(r.amoptsprocnum, 5);
    assert!(r.amcanorder);
    assert!(!r.amcanorderbyop);
    assert!(!r.amcanhash);
    assert!(r.amconsistentequality);
    assert!(r.amconsistentordering);
    assert!(r.amcanbackward);
    assert!(r.amcanunique);
    assert!(r.amcanmulticol);
    assert!(r.amoptionalkey);
    assert!(r.amsearcharray);
    assert!(r.amsearchnulls);
    assert!(!r.amstorage);
    assert!(r.amclusterable);
    assert!(r.ampredlocks);
    assert!(r.amcanparallel);
    assert!(r.amcanbuildparallel);
    assert!(r.amcaninclude);
    assert!(!r.amusemaintenanceworkmem);
    assert!(!r.amsummarizing);
    assert_eq!(
        r.amparallelvacuumoptions,
        VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP
    );
    assert_eq!(r.amkeytype, InvalidOid);
    assert!(r.amtranslatestrategy.is_some());
    assert!(r.amtranslatecmptype.is_some());
    assert!(r.amvalidate.is_none());
}

// bttranslatestrategy / bttranslatecmptype: 1:1 with the C switch.
#[test]
fn bttranslatestrategy_maps_all_btree_strategies() {
    assert_eq!(bttranslatestrategy(BTLessStrategyNumber, 0), COMPARE_LT);
    assert_eq!(bttranslatestrategy(BTLessEqualStrategyNumber, 0), COMPARE_LE);
    assert_eq!(bttranslatestrategy(BTEqualStrategyNumber, 0), COMPARE_EQ);
    assert_eq!(bttranslatestrategy(BTGreaterEqualStrategyNumber, 0), COMPARE_GE);
    assert_eq!(bttranslatestrategy(BTGreaterStrategyNumber, 0), COMPARE_GT);
    assert_eq!(bttranslatestrategy(99, 0), COMPARE_INVALID);
}

#[test]
fn bttranslatecmptype_maps_all_compare_types() {
    assert_eq!(bttranslatecmptype(COMPARE_LT, 0), BTLessStrategyNumber);
    assert_eq!(bttranslatecmptype(COMPARE_LE, 0), BTLessEqualStrategyNumber);
    assert_eq!(bttranslatecmptype(COMPARE_EQ, 0), BTEqualStrategyNumber);
    assert_eq!(bttranslatecmptype(COMPARE_GE, 0), BTGreaterEqualStrategyNumber);
    assert_eq!(bttranslatecmptype(COMPARE_GT, 0), BTGreaterStrategyNumber);
    assert_eq!(bttranslatecmptype(COMPARE_INVALID, 0), InvalidStrategy);
}

#[test]
fn bttranslate_round_trips() {
    for cmp in [COMPARE_LT, COMPARE_LE, COMPARE_EQ, COMPARE_GE, COMPARE_GT] {
        let strat = bttranslatecmptype(cmp, 0);
        assert_eq!(bttranslatestrategy(strat, 0), cmp);
    }
}

// P_FIRSTDATAKEY / page-opaque predicates.
#[test]
fn p_firstdatakey_depends_on_rightmost() {
    // rightmost (btpo_next == P_NONE): data keys start at P_HIKEY (1)
    assert_eq!(P_FIRSTDATAKEY(P_NONE), 1);
    // non-rightmost: high key at P_HIKEY, data starts at P_FIRSTKEY (2)
    assert_eq!(P_FIRSTDATAKEY(5), 2);
}

#[test]
fn page_flag_predicates_match_bits() {
    assert!(P_ISLEAF(BTP_LEAF));
    assert!(!P_ISLEAF(BTP_DELETED));
    assert!(P_ISDELETED(BTP_DELETED));
    assert!(P_ISHALFDEAD(BTP_HALF_DEAD));
    assert!(P_SPLIT_END(BTP_SPLIT_END));
    let combo = BTP_LEAF | BTP_SPLIT_END;
    assert!(P_ISLEAF(combo));
    assert!(P_SPLIT_END(combo));
    assert!(!P_ISDELETED(combo));
}

#[test]
fn p_rightmost_is_btpo_next_eq_p_none() {
    assert!(P_RIGHTMOST(P_NONE));
    assert!(!P_RIGHTMOST(1));
}

// MaxIndexTuplesPerPage matches the access/itup.h formula for BLCKSZ=8192.
#[test]
fn max_index_tuples_per_page_value() {
    assert_eq!(MaxIndexTuplesPerPage, 408);
    assert_eq!(MaxTIDsPerBTreePage, 1358);
}
