//! Smoke tests for `execIndexing`. The functions here drive deep
//! relcache/index-AM/heap machinery (most reached only via installed seams), so
//! these tests cover the pure leaf logic that does not require a live backend.

use super::*;

#[test]
fn first_low_invalid_heap_attribute_number_is_pg18() {
    // PG 18 access/sysattr.h: FirstLowInvalidHeapAttributeNumber = (-7).
    assert_eq!(FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER, -7);
}

#[test]
fn typtype_codes_match_pg_type_h() {
    assert_eq!(TYPTYPE_RANGE, b'r' as i8);
    assert_eq!(TYPTYPE_MULTIRANGE, b'm' as i8);
}

#[test]
fn scan_key_null_flags_match_skey_h() {
    assert_eq!(SK_ISNULL, 0x0002);
    assert_eq!(SK_SEARCHNULL, 0x0010);
}

#[test]
fn index_expression_changed_walker_null_is_false() {
    assert!(!index_expression_changed_walker(None, None));
}

#[test]
fn index_expression_changed_walker_matches_updated_var() {
    use backend_nodes_core::bitmapset::bms_add_member;
    use types_nodes::primnodes::{Expr, Var};

    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();

    // updated-cols set holds the bit for attno 2: (2 - (-7)) = 9.
    let bms = bms_add_member(mcx, None, 2 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER).unwrap();

    // A Var on attno 2 is in the updated set -> hint must be suppressed.
    let node = Expr::Var(Var {
        varattno: 2,
        ..Default::default()
    });
    assert!(index_expression_changed_walker(Some(&node), Some(&*bms)));

    // A different, non-updated column does not trigger hint suppression.
    let node2 = Expr::Var(Var {
        varattno: 3,
        ..Default::default()
    });
    assert!(!index_expression_changed_walker(Some(&node2), Some(&*bms)));
}
