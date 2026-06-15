//! Unit tests over the pure (non-catalog) coercion logic.

use super::*;
use types_nodes::primnodes::Const;

#[test]
fn polymorphic_predicates() {
    assert!(is_polymorphic_type(ANYELEMENTOID));
    assert!(is_polymorphic_type(ANYARRAYOID));
    assert!(is_polymorphic_type(ANYCOMPATIBLEOID));
    assert!(is_polymorphic_type_family1(ANYRANGEOID));
    assert!(is_polymorphic_type_family2(ANYCOMPATIBLERANGEOID));
    assert!(!is_polymorphic_type(INT4OID));
    assert!(!is_polymorphic_type_family1(ANYCOMPATIBLEOID));
    assert!(!is_polymorphic_type_family2(ANYELEMENTOID));
}

#[test]
fn ccontext_ordering() {
    // The C `ccontext >= castcontext` integer compare relies on this order.
    assert!(ccontext_rank(CoercionContext::COERCION_ASSIGNMENT) >= ccontext_rank(CoercionContext::COERCION_IMPLICIT));
    assert!(ccontext_rank(CoercionContext::COERCION_EXPLICIT) >= ccontext_rank(CoercionContext::COERCION_ASSIGNMENT));
}

#[test]
fn select_common_typmod_all_match() {
    // All-Const exprs of the same type and typmod collapse to that typmod.
    let mk = |typmod: i32| {
        Expr::Const(Const {
            consttype: INT4OID,
            consttypmod: typmod,
            constcollid: InvalidOid,
            constvalue: TupleDatum::ByVal(0),
            constisnull: false,
            location: -1,
        })
    };
    let exprs = [mk(5), mk(5)];
    assert_eq!(select_common_typmod(&exprs, INT4OID).unwrap(), 5);
    let exprs2 = [mk(5), mk(7)];
    assert_eq!(select_common_typmod(&exprs2, INT4OID).unwrap(), -1);
}

#[test]
fn check_valid_internal_signature_rules() {
    use types_tuple::heaptuple::INTERNALOID;
    assert!(check_valid_internal_signature(INT4OID, &[INT4OID], 1).is_none());
    assert!(check_valid_internal_signature(INTERNALOID, &[INTERNALOID], 1).is_none());
    assert!(check_valid_internal_signature(INTERNALOID, &[INT4OID], 1).is_some());
}
