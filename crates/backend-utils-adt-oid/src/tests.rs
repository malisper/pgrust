use super::*;

#[test]
fn oidin_decimal() {
    assert_eq!(oidin("0", None).unwrap(), 0);
    assert_eq!(oidin("12345", None).unwrap(), 12345);
    assert_eq!(oidin("4294967295", None).unwrap(), 4_294_967_295);
}

#[test]
fn oidin_trailing_whitespace_ok() {
    assert_eq!(oidin("  42  ", None).unwrap(), 42);
}

#[test]
fn oidin_garbage_hard_throws() {
    assert!(oidin("notanumber", None).is_err());
    assert!(oidin("12x", None).is_err());
}

#[test]
fn oidout_decimal() {
    assert_eq!(oidout(0), "0");
    assert_eq!(oidout(4_294_967_295), "4294967295");
}

#[test]
fn comparisons() {
    assert!(oideq(7, 7));
    assert!(!oideq(7, 8));
    assert!(oidne(7, 8));
    assert!(oidlt(1, 2));
    assert!(oidle(2, 2));
    assert!(oidgt(3, 2));
    assert!(oidge(2, 2));
    assert_eq!(oidlarger(3, 7), 7);
    assert_eq!(oidsmaller(3, 7), 3);
}

#[test]
fn oid_cmp_orders() {
    assert_eq!(oid_cmp(1, 2), -1);
    assert_eq!(oid_cmp(2, 2), 0);
    assert_eq!(oid_cmp(3, 2), 1);
}

#[test]
fn check_valid_oidvector_ok_and_err() {
    assert!(check_valid_oidvector(1, 0, OIDOID).is_ok());
    assert!(check_valid_oidvector(2, 0, OIDOID).is_err());
    assert!(check_valid_oidvector(1, 8, OIDOID).is_err());
    assert!(check_valid_oidvector(1, 0, 23).is_err());
}
