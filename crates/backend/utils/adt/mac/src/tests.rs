use mcx::MemoryContext;
use types_error::{
    SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::LocalFcinfo;

use super::builtins::*;
use super::*;

use ::datum::Datum;

const CANON: MacAddr = MacAddr {
    a: 0x08,
    b: 0x00,
    c: 0x2b,
    d: 0x01,
    e: 0x02,
    f: 0x03,
};

fn out_str(addr: &MacAddr) -> String {
    let mut buf = [0u8; MACADDR_OUT_LEN];
    let len = macaddr_out_into(addr, &mut buf);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

#[test]
fn in_accepts_all_seven_notations() {
    for s in [
        "08:00:2b:01:02:03",
        "08-00-2b-01-02-03",
        "08002b:010203",
        "08002b-010203",
        "0800.2b01.0203",
        "0800-2b01-0203",
        "08002b010203",
        "08:00:2B:01:02:03",
        "  08:00:2b:01:02:03  ",
        "08002B010203",
    ] {
        assert_eq!(macaddr_in(s, None).unwrap(), CANON, "{s}");
    }
}

#[test]
fn in_sscanf_quirks_match_c() {
    // %x is unbounded per octet in the ':'/'-' forms; 0x prefixes parse too.
    assert_eq!(
        macaddr_in("8:0:2b:1:2:3", None).unwrap(),
        CANON,
        "unpadded octets"
    );
    assert_eq!(
        macaddr_in("0x08:0x00:0x2b:0x01:0x02:0x03", None).unwrap(),
        CANON
    );
    let err = macaddr_in("1ff:00:00:00:00:00", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "invalid octet value in \"macaddr\" value: \"1ff:00:00:00:00:00\""
    );
}

#[test]
fn in_rejects_garbage() {
    for s in [
        "",
        "08:00:2b:01:02",
        "08:00:2b:01:02:03:04",
        "08:00:2b:01:02:03 x",
        "08002b0102033",
        "0800020000ff01",
        "08:00-2b:01:02:03junk",
        "not a mac",
    ] {
        let err = macaddr_in(s, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION, "{s}");
        assert_eq!(
            err.message(),
            format!("invalid input syntax for type macaddr: \"{s}\""),
            "{s}"
        );
    }

    let mut soft = SoftErrorContext::new(true);
    assert_eq!(
        macaddr_in("bogus", Some(&mut soft)).unwrap(),
        MacAddr::default()
    );
    assert!(soft.error_occurred());
}

#[test]
fn out_fixed_format() {
    assert_eq!(out_str(&CANON), "08:00:2b:01:02:03");
    assert_eq!(
        out_str(&MacAddr::from_bytes([0xff, 0xfe, 0xab, 0, 1, 0x7f])),
        "ff:fe:ab:00:01:7f"
    );
}

#[test]
fn wire_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sent = macaddr_send(mcx, &CANON).unwrap();
    assert_eq!(sent.data(), &CANON.to_bytes());

    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&CANON.to_bytes()).unwrap();
    assert_eq!(macaddr_recv(&mut buf).unwrap(), CANON);
}

#[test]
fn cmp_ordering_and_hash() {
    let lo = MacAddr::from_bytes([0, 0, 0, 0xff, 0xff, 0xff]);
    let hi = MacAddr::from_bytes([0x80, 0, 0, 0, 0, 0]);
    assert_eq!(macaddr_cmp(&lo, &hi), -1);
    assert_eq!(macaddr_cmp(&hi, &lo), 1);
    assert_eq!(macaddr_cmp(&lo, &lo), 0);
    assert!(macaddr_lt(&lo, &hi));
    assert!(macaddr_le(&lo, &hi));
    assert!(macaddr_gt(&hi, &lo));
    assert!(macaddr_ge(&hi, &lo));
    assert!(macaddr_ne(&lo, &hi));
    assert!(macaddr_eq(&lo, &lo));

    // Lobits tiebreak.
    let a = MacAddr::from_bytes([1, 2, 3, 0, 0, 1]);
    let b = MacAddr::from_bytes([1, 2, 3, 0, 0, 2]);
    assert_eq!(macaddr_cmp(&a, &b), -1);

    assert_eq!(hashmacaddr(&CANON), hashfn::hash_bytes(&CANON.to_bytes()));
    assert_eq!(
        hashmacaddrextended(&CANON, 42),
        hashfn::hash_bytes_extended(&CANON.to_bytes(), 42)
    );
}

#[test]
fn bitwise_and_trunc() {
    let x = MacAddr::from_bytes([0xf0, 0x0f, 0xaa, 0x55, 0x00, 0xff]);
    let y = MacAddr::from_bytes([0xff, 0x00, 0x0f, 0xf0, 0x12, 0x34]);
    assert_eq!(
        macaddr_not(&x).to_bytes(),
        [0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00]
    );
    assert_eq!(
        macaddr_and(&x, &y).to_bytes(),
        [0xf0, 0x00, 0x0a, 0x50, 0x00, 0x34]
    );
    assert_eq!(
        macaddr_or(&x, &y).to_bytes(),
        [0xff, 0x0f, 0xaf, 0xf5, 0x12, 0xff]
    );
    assert_eq!(macaddr_trunc(&x).to_bytes(), [0xf0, 0x0f, 0xaa, 0, 0, 0]);
}

fn mac_datum(addr: &MacAddr) -> Datum {
    Datum::from_usize(addr as *const MacAddr as usize)
}

#[test]
fn fc_wrappers() {
    let (a, b) = (CANON, macaddr_trunc(&CANON));

    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, mac_datum(&a));
    fcinfo.set_arg(1, mac_datum(&b));
    assert!(!fc_macaddr_eq(None, &mut fcinfo).unwrap().as_bool());
    assert!(fc_macaddr_gt(None, &mut fcinfo).unwrap().as_bool());
    assert_eq!(fc_macaddr_cmp(None, &mut fcinfo).unwrap().as_i32(), 1);

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, mac_datum(&a));
    let d = fc_macaddr_out(None, &mut fcinfo).unwrap();
    let cstr = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(cstr.to_bytes(), b"08:00:2b:01:02:03");

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, mac_datum(&a));
    assert_eq!(
        fc_hashmacaddr(None, &mut fcinfo).unwrap().as_u32(),
        hashmacaddr(&a)
    );
}

#[test]
fn builtins_table_oid_ascending() {
    for w in MAC_BUILTINS.windows(2) {
        assert!(w[0].foid < w[1].foid);
    }
}
