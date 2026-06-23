use super::*;
use ::mcx::MemoryContext;

fn name_of(s: &str) -> NameData {
    let mut n = NameData::default();
    let b = s.as_bytes();
    n.data[..b.len()].copy_from_slice(b);
    n
}

#[test]
fn namein_zero_pads_to_namedatalen() {
    let n = namein("foo").unwrap();
    assert_eq!(&n.data[..3], b"foo");
    assert!(n.data[3..].iter().all(|&c| c == 0));
}

#[test]
fn namein_empty_is_all_zero() {
    let n = namein("").unwrap();
    assert!(n.data.iter().all(|&c| c == 0));
}

#[test]
fn nameout_stops_at_first_nul() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let n = name_of("bar");
    assert_eq!(nameout(mcx, &n).unwrap().as_str(), "bar");
}

#[test]
fn roundtrip() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let n = namein("public").unwrap();
    assert_eq!(nameout(mcx, &n).unwrap().as_str(), "public");
}

#[test]
fn namecmp_c_collation_orders_and_equals() {
    let a = name_of("abc");
    let b = name_of("abd");
    let c = name_of("abc");
    assert!(nameeq(&a, &c, C_COLLATION_OID).unwrap());
    assert!(namene(&a, &b, C_COLLATION_OID).unwrap());
    assert!(namelt(&a, &b, C_COLLATION_OID).unwrap());
    assert!(namele(&a, &c, C_COLLATION_OID).unwrap());
    assert!(namegt(&b, &a, C_COLLATION_OID).unwrap());
    assert!(namege(&a, &c, C_COLLATION_OID).unwrap());
    assert_eq!(btnamecmp(&a, &c, C_COLLATION_OID).unwrap(), 0);
    assert!(btnamecmp(&a, &b, C_COLLATION_OID).unwrap() < 0);
}

#[test]
fn namestrcmp_null_handling() {
    let a = name_of("abc");
    assert_eq!(namestrcmp(None, None), 0);
    assert_eq!(namestrcmp(None, Some("x")), -1);
    assert_eq!(namestrcmp(Some(&a), None), 1);
    assert_eq!(namestrcmp(Some(&a), Some("abc")), 0);
    assert!(namestrcmp(Some(&a), Some("abd")) < 0);
}

#[test]
fn namestrcpy_truncates_and_pads() {
    let mut n = name_of("xxxxx");
    namestrcpy(&mut n, "hi");
    assert_eq!(n.name_str(), b"hi");
    assert!(n.data[2..].iter().all(|&c| c == 0));
}
