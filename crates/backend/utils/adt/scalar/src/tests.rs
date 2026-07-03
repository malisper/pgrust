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

// tid rows diffed vs live C 18.3 (psql, 2026-07-03).
#[test]
fn tid_in_out() {
    let t = |s: &str| tidin(s.as_bytes()).unwrap();
    assert_eq!(t("(1,2)"), Tid { block: 1, offset: 2 });
    assert_eq!(t("(4294967295,65535)"), Tid { block: u32::MAX, offset: u16::MAX });
    // strtoul wrap: C accepts (-1,0) as block 4294967295
    assert_eq!(t("(-1,0)"), Tid { block: u32::MAX, offset: 0 });
    assert_eq!(t("( 42,7)"), Tid { block: 42, offset: 7 });
    for bad in ["", "1,2", "(1,2", "(1 ,2)", "( 42 , 7 )", "(1,65536)", "(1,2)x"] {
        // trailing garbage after ')' is accepted by C (scan stops at RDELIM)
        if bad == "(1,2)x" {
            assert!(tidin(bad.as_bytes()).is_some());
        } else {
            assert!(tidin(bad.as_bytes()).is_none(), "{bad:?}");
        }
    }
    let mut buf = [0u8; 32];
    let n = tidout(t("(-1,0)"), &mut buf);
    assert_eq!(&buf[..n], b"(4294967295,0)");
    assert_eq!(tid_cmp(t("(1,2)"), t("(1,3)")), -1);
    assert_eq!(tid_cmp(t("(2,1)"), t("(1,9)")), 1);
    assert_eq!(tid_cmp(t("(1,2)"), t("(1,2)")), 0);
}

#[test]
fn tid_hash_live_c() {
    // hashtid('(1,2)') / hashtidextended('(1,2)',7) from live C
    let img: [u8; 6] = {
        let hi = 0u16.to_ne_bytes();
        let lo = 1u16.to_ne_bytes();
        let off = 2u16.to_ne_bytes();
        [hi[0], hi[1], lo[0], lo[1], off[0], off[1]]
    };
    assert_eq!(hashfn::hash_bytes(&img) as i32, -1827449972);
    assert_eq!(hashfn::hash_bytes_extended(&img, 7) as i64, 4917257717648883525);
}

#[test]
fn xid_hash_live_c() {
    // hashxid('42'), hashxidextended('42',3), hashoid(12345),
    // hashoidextended(12345,3), hashxid8(42),hashxid8extended(42,3)
    assert_eq!(hashfn::hash_bytes_uint32(42) as i32, 1509752520);
    assert_eq!(hashfn::hash_bytes_uint32_extended(42, 3) as i64, -1610262496784391990);
    assert_eq!(hashfn::hash_bytes_uint32(12345) as i32, -78097827);
    assert_eq!(hashfn::hash_bytes_uint32_extended(12345, 3) as i64, -2672860095681695817);
    let val = 42i64;
    let lohalf = (val as u32) ^ ((val >> 32) as u32);
    assert_eq!(hashfn::hash_bytes_uint32(lohalf) as i32, 1509752520);
}

#[test]
fn xid8_ops() {
    assert_eq!(xid8cmp(42, 43), -1);
    assert_eq!(xid8cmp(43, 42), 1);
    assert_eq!(xid8cmp(7, 7), 0);
}
