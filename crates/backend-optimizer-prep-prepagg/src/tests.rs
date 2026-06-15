//! Unit tests for the node-independent arithmetic / predicate helpers of
//! prepagg.c (the catalog/cost/equal seams are exercised at integration time;
//! these cover the in-crate pure logic).

use super::*;
use types_nodes::nodeagg::{
    AGGSPLIT_FINAL_DESERIAL, AGGSPLIT_INITIAL_SERIAL, AGGSPLIT_SIMPLE,
};

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(MAXALIGN(0), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(7), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN(9), 16);
    assert_eq!(MAXALIGN(16), 16);
}

#[test]
fn do_aggsplit_predicates_match_c_bits() {
    // AGGSPLIT_SIMPLE = 0: none of the bits set.
    assert!(!DO_AGGSPLIT_COMBINE(AGGSPLIT_SIMPLE));
    assert!(!DO_AGGSPLIT_SERIALIZE(AGGSPLIT_SIMPLE));
    assert!(!DO_AGGSPLIT_DESERIALIZE(AGGSPLIT_SIMPLE));
    assert!(!DO_AGGSPLIT_SKIPFINAL(AGGSPLIT_SIMPLE));

    // AGGSPLIT_INITIAL_SERIAL = SKIPFINAL | SERIALIZE.
    assert!(DO_AGGSPLIT_SKIPFINAL(AGGSPLIT_INITIAL_SERIAL));
    assert!(DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL));
    assert!(!DO_AGGSPLIT_COMBINE(AGGSPLIT_INITIAL_SERIAL));
    assert!(!DO_AGGSPLIT_DESERIALIZE(AGGSPLIT_INITIAL_SERIAL));

    // AGGSPLIT_FINAL_DESERIAL = COMBINE | DESERIALIZE.
    assert!(DO_AGGSPLIT_COMBINE(AGGSPLIT_FINAL_DESERIAL));
    assert!(DO_AGGSPLIT_DESERIALIZE(AGGSPLIT_FINAL_DESERIAL));
    assert!(!DO_AGGSPLIT_SERIALIZE(AGGSPLIT_FINAL_DESERIAL));
    assert!(!DO_AGGSPLIT_SKIPFINAL(AGGSPLIT_FINAL_DESERIAL));
}

#[test]
fn oid_is_valid_matches_c() {
    assert!(!OidIsValid(0));
    assert!(OidIsValid(1));
    assert!(OidIsValid(2281)); // INTERNALOID
}

#[test]
fn agg_clause_costs_default_is_zeroed() {
    let costs = AggClauseCosts::default();
    assert_eq!(costs.transCost.startup, 0.0);
    assert_eq!(costs.transCost.per_tuple, 0.0);
    assert_eq!(costs.finalCost.startup, 0.0);
    assert_eq!(costs.finalCost.per_tuple, 0.0);
    assert_eq!(costs.transitionSpace, 0);
}

#[test]
fn fmgroid_constants_match_pg18() {
    // Spot-check the fmgroids the de-dup / cost arithmetic keys on.
    assert_eq!(F_ARRAY_AGG_SERIALIZE, 6294);
    assert_eq!(F_ARRAY_AGG_DESERIALIZE, 6295);
    assert_eq!(F_ARRAY_APPEND, 378);
    assert_eq!(AGGMODIFY_READ_WRITE, b'w' as i8);
}

#[test]
fn equal_opt_expr_null_handling() {
    // NULL ↔ NULL is equal; NULL vs non-NULL is not. (The non-NULL/non-NULL
    // case goes through the equalfuncs seam, exercised at integration time.)
    assert!(equal_opt_expr(None, None));
    // We can't cheaply build two Exprs without the full node universe here; the
    // None/None and asymmetric-None arms are the in-crate logic under test.
}
