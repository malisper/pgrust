//! Tests for the `enum.c` port. These exercise the catalog-free logic paths:
//! the OID-equality comparators (`enum_eq`/`enum_ne`) and the equal-OID /
//! even-numbered-OID fast paths of `enum_cmp_internal` (enum.c:264), which C
//! resolves without touching the typcache. The catalog-touching paths
//! (`enum_in`/`out`/`recv`/`send`, the odd-OID comparison, the ordered scans)
//! go through the installed owner seams and are covered by the cross-crate
//! integration once `seams-init` wires every owner; their parity is enforced by
//! the function-by-function audit against enum.c.

use super::*;

#[test]
fn enum_eq_ne_are_oid_identity() {
    assert!(enum_eq(100, 100));
    assert!(!enum_eq(100, 101));
    assert!(!enum_ne(100, 100));
    assert!(enum_ne(100, 101));
}

#[test]
fn enum_cmp_equal_oids_compare_equal() {
    // enum.c:271 — "Equal OIDs are equal no matter what" (even odd OIDs,
    // without consulting the typcache).
    assert_eq!(enum_cmp_internal(99, 99).unwrap(), 0);
}

#[test]
fn enum_cmp_even_oid_fast_path() {
    // enum.c:277 — even-numbered OIDs compare by raw value (the renumbering
    // fast path), no catalog access.
    assert_eq!(enum_cmp_internal(2, 4).unwrap(), -1);
    assert_eq!(enum_cmp_internal(8, 6).unwrap(), 1);
    assert!(enum_lt(2, 4).unwrap());
    assert!(enum_gt(8, 6).unwrap());
    assert!(enum_le(2, 4).unwrap());
    assert!(enum_ge(8, 6).unwrap());
    assert_eq!(enum_smaller(2, 4).unwrap(), 2);
    assert_eq!(enum_larger(2, 4).unwrap(), 4);
}

#[test]
fn invalid_internal_value_sqlstate() {
    let err = invalid_internal_value(42);
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_BINARY_REPRESENTATION);
    assert_eq!(err.message(), "invalid internal value for enum: 42");
}

#[test]
fn could_not_determine_enum_type_sqlstate() {
    let err = could_not_determine_enum_type();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "could not determine actual enum type");
}

#[test]
fn name_str_bytes_stops_at_nul() {
    assert_eq!(name_str_bytes(b"foo\0\0\0bar"), b"foo");
    assert_eq!(name_str_bytes(b"baz"), b"baz");
    assert_eq!(label_to_string(b"label\0pad"), "label");
}

#[test]
fn cstring_str_stops_at_nul() {
    assert_eq!(cstring_str(b"abc\0def"), "abc");
    assert_eq!(cstring_str(b"abc"), "abc");
}
