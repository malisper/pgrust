use super::*;

#[test]
fn in_out_roundtrip() {
    let cases: &[(&str, u64, &str)] = &[
        ("0/0", 0, "0/0"),
        ("0/12345678", 0x12345678, "0/12345678"),
        ("ABCD1234/beef0001", 0xABCD_1234_BEEF_0001, "ABCD1234/BEEF0001"),
        ("FFFFFFFF/FFFFFFFF", u64::MAX, "FFFFFFFF/FFFFFFFF"),
        ("00000001/00000002", 0x0000_0001_0000_0002, "1/2"),
    ];
    for (input, lsn, out) in cases {
        assert_eq!(pg_lsn_in(input, None).unwrap(), *lsn, "{input}");
        let mut buf = [0u8; MAXPG_LSNLEN + 1];
        let n = pg_lsn_out_into(*lsn, &mut buf);
        assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), *out);
    }
}

#[test]
fn in_rejects() {
    for bad in ["", "/", "0", "0/", "/0", "123456789/0", "0/123456789", "0/0 ", " 0/0", "xyz/0", "0//0"] {
        assert!(pg_lsn_in(bad, None).is_err(), "{bad:?}");
    }
}

fn n(s: &str) -> adt_numeric::NumericImage {
    adt_numeric::numeric_in(s, -1, None).unwrap().unwrap()
}

fn num_out(img: &NumericImage) -> String {
    let mut buf = Vec::new();
    adt_numeric::numeric_out_into(img.num(), &mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

// Differential rows vs live C 18.3 (psql, 2026-07-03).
#[test]
fn mi_pli_mii_numeric() {
    let l = |s: &str| pg_lsn_in(s, None).unwrap();
    assert_eq!(num_out(&pg_lsn_mi(l("1/2"), l("0/FF")).unwrap()), "4294967043");
    assert_eq!(num_out(&pg_lsn_mi(l("0/FF"), l("1/2")).unwrap()), "-4294967043");
    assert_eq!(pg_lsn_pli(l("0/FF"), n("10").num()).unwrap(), 0x109);
    assert_eq!(pg_lsn_mii(l("1/0"), n("1").num()).unwrap(), 0xFFFFFFFF);
    assert_eq!(numeric_pg_lsn(n("42").num()).unwrap(), 0x2A);
    assert_eq!(numeric_pg_lsn(n("18446744073709551615").num()).unwrap(), u64::MAX);
    assert_eq!(
        numeric_pg_lsn(n("-1").num()).unwrap_err().to_string(),
        "pg_lsn out of range"
    );
    assert_eq!(
        numeric_pg_lsn(n("18446744073709551616").num()).unwrap_err().to_string(),
        "pg_lsn out of range"
    );
    assert_eq!(
        numeric_pg_lsn(n("nan").num()).unwrap_err().to_string(),
        "cannot convert NaN to pg_lsn"
    );
    assert_eq!(
        pg_lsn_pli(l("0/FF"), n("nan").num()).unwrap_err().to_string(),
        "cannot add NaN to pg_lsn"
    );
    assert_eq!(
        pg_lsn_mii(l("0/FF"), n("nan").num()).unwrap_err().to_string(),
        "cannot subtract NaN from pg_lsn"
    );
}

#[test]
fn cmp_and_hash() {
    let l = |s: &str| pg_lsn_in(s, None).unwrap();
    assert_eq!(pg_lsn_cmp_internal(l("1/2"), l("2/1")), -1);
    assert_eq!(pg_lsn_cmp_internal(l("2/1"), l("2/1")), 0);
    // C hashint8 fold, live value for '0/16B3748'
    let val = l("0/16B3748") as i64;
    let lohalf = (val as u32) ^ if val >= 0 { (val >> 32) as u32 } else { !((val >> 32) as u32) };
    assert_eq!(hashfn::hash_bytes_uint32(lohalf) as i32, -486117246);
}
