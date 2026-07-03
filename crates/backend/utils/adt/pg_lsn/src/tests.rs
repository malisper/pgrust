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
