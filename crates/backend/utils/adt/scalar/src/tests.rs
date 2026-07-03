use super::*;

#[test]
fn oid_comparisons() {
    assert!(oideq(10, 10) && !oideq(10, 11));
    assert!(oidne(10, 11) && !oidne(10, 10));
    assert!(oidlt(1, 2) && !oidlt(2, 2));
    assert!(oidle(2, 2) && !oidle(3, 2));
    assert!(oidgt(3, 2) && !oidgt(2, 2));
    assert!(oidge(2, 2) && !oidge(1, 2));
    // Oid is unsigned: 4294967295 > 1 (C comparison on unsigned OIDs).
    assert!(oidgt(u32::MAX, 1));
}
