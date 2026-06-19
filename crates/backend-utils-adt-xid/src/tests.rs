use super::*;

#[test]
fn xid_text_io_roundtrips() {
    assert_eq!(xidin("0", None).unwrap(), 0);
    assert_eq!(xidin("3", None).unwrap(), 3);
    assert_eq!(xidin("42", None).unwrap(), 42);
    assert_eq!(xidin("4294967295", None).unwrap(), u32::MAX);
    // base-0 parsing: octal, hex, negative-wraps.
    assert_eq!(xidin("010", None).unwrap(), 8);
    assert_eq!(xidin("0xffffffff", None).unwrap(), 4294967295);
    assert_eq!(xidin("-1", None).unwrap(), 4294967295);

    assert_eq!(xidout(0), "0");
    assert_eq!(xidout(42), "42");
    assert_eq!(xidout(u32::MAX), "4294967295");
}

#[test]
fn xidin_rejects_garbage_and_out_of_range() {
    assert!(xidin("", None).is_err());
    assert!(xidin("asdf", None).is_err());
    assert!(xidin("4294967296", None).is_err());
}

#[test]
fn xid_eq_neq() {
    assert!(xideq(5, 5));
    assert!(!xideq(5, 6));
    assert!(xidneq(5, 6));
    assert!(!xidneq(5, 5));
}

#[test]
fn xid_comparator_is_plain_unsigned() {
    assert_eq!(xidComparator(1, 2), -1);
    assert_eq!(xidComparator(2, 1), 1);
    assert_eq!(xidComparator(2, 2), 0);
    assert_eq!(xidComparator(u32::MAX, 1), 1);
}

#[test]
fn xid_logical_comparator_uses_wraparound() {
    assert_eq!(xidLogicalComparator(3, 4), -1);
    assert_eq!(xidLogicalComparator(4, 3), 1);
    assert_eq!(xidLogicalComparator(4, 4), 0);
}

#[test]
fn xid8_text_io() {
    assert_eq!(xid8in("0", None).unwrap(), FullTransactionId { value: 0 });
    assert_eq!(xid8in("100", None).unwrap(), FullTransactionId { value: 100 });
    assert_eq!(
        xid8in("18446744073709551615", None).unwrap(),
        FullTransactionId { value: u64::MAX }
    );
    assert_eq!(xid8out(FullTransactionId { value: 0 }), "0");
    assert_eq!(
        xid8out(FullTransactionId { value: u64::MAX }),
        "18446744073709551615"
    );
}

#[test]
fn xid8_comparisons_are_unsigned_64bit() {
    let a = FullTransactionId { value: 10 };
    let b = FullTransactionId { value: 20 };
    let big = FullTransactionId { value: u64::MAX };

    assert!(xid8eq(a, a));
    assert!(xid8ne(a, b));
    assert!(xid8lt(a, b));
    assert!(xid8gt(b, a));
    assert!(xid8le(a, a));
    assert!(xid8ge(a, a));
    assert!(xid8gt(big, b));

    assert_eq!(xid8cmp(a, b), -1);
    assert_eq!(xid8cmp(b, a), 1);
    assert_eq!(xid8cmp(a, a), 0);

    assert_eq!(xid8_larger(a, b), b);
    assert_eq!(xid8_smaller(a, b), a);

    assert_eq!(
        xid8toxid(FullTransactionId {
            value: 0x0000_0001_0000_002a
        }),
        42
    );
}

#[test]
fn cid_text_io_and_eq() {
    assert_eq!(cidin("0", None).unwrap(), 0);
    assert_eq!(cidin("4294967295", None).unwrap(), u32::MAX);
    assert_eq!(cidout(7), "7");
    assert!(cideq(3, 3));
    assert!(!cideq(3, 4));
}

#[test]
fn hashes_match_known_folds() {
    assert_eq!(hashxid(42), hash_uint32(42));
    assert_eq!(hashcid(42), hash_uint32(42));
    assert_eq!(hashxidextended(42, 7), hash_uint32_extended(42, 7));
    assert_eq!(hashcidextended(42, 7), hash_uint32_extended(42, 7));

    let fxid = FullTransactionId {
        value: 0x1234_5678_9abc_def0,
    };
    assert_eq!(hashxid8(fxid), hashint8(0x1234_5678_9abc_def0_u64 as i64));
    assert_eq!(
        hashxid8extended(fxid, 99),
        hashint8extended(0x1234_5678_9abc_def0_u64 as i64, 99)
    );
}
