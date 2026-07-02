use adt_mac::MacAddr;
use mcx::MemoryContext;
use types_error::{
    SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::LocalFcinfo;

use super::builtins::*;
use super::*;

use ::datum::Datum;

const CANON8: MacAddr8 = MacAddr8 {
    a: 0x08,
    b: 0x00,
    c: 0x2b,
    d: 0x01,
    e: 0x02,
    f: 0x03,
    g: 0x04,
    h: 0x05,
};

const CANON6AS8: MacAddr8 = MacAddr8 {
    a: 0x08,
    b: 0x00,
    c: 0x2b,
    d: 0xff,
    e: 0xfe,
    f: 0x01,
    g: 0x02,
    h: 0x03,
};

fn out_str(addr: &MacAddr8) -> String {
    let mut buf = [0u8; MACADDR8_OUT_LEN];
    let len = macaddr8_out_into(addr, &mut buf);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

#[test]
fn in_eui64_notations() {
    for s in [
        "08:00:2b:01:02:03:04:05",
        "08-00-2b-01-02-03-04-05",
        "08002b:0102030405",
        "08002b-0102030405",
        "0800.2b01.0203.0405",
        "08002b0102030405",
        "  08:00:2B:01:02:03:04:05  ",
    ] {
        assert_eq!(macaddr8_in(s, None).unwrap(), CANON8, "{s}");
    }
}

#[test]
fn in_eui48_expands_with_fffe() {
    for s in [
        "08:00:2b:01:02:03",
        "08-00-2b-01-02-03",
        "08002b010203",
        "0800.2b01.0203",
    ] {
        assert_eq!(macaddr8_in(s, None).unwrap(), CANON6AS8, "{s}");
    }
}

#[test]
fn in_c_state_machine_quirks() {
    // Trailing lone digit after 6 bytes falls out of the pair loop unread.
    assert_eq!(macaddr8_in("08002b0102031", None).unwrap(), CANON6AS8);
    // Trailing spacer after the final byte is consumed and accepted.
    assert_eq!(macaddr8_in("08:00:2b:01:02:03:", None).unwrap(), CANON6AS8);
}

#[test]
fn in_rejects_garbage() {
    for s in [
        "",
        "   ",
        "08:00:2b:01:02",
        "08:00-2b:01:02:03",
        "08:00:2b:01:02:03:04",
        "08:00:2b:01:02:03:04:05:06",
        "08:00:2b:01:02:03 x",
        "0g:00:2b:01:02:03",
        "08\u{80}:00:2b:01:02:03",
        "not a mac",
    ] {
        let err = macaddr8_in(s, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION, "{s}");
        assert_eq!(
            err.message(),
            format!("invalid input syntax for type macaddr8: \"{s}\""),
            "{s}"
        );
    }

    let mut soft = SoftErrorContext::new(true);
    assert_eq!(
        macaddr8_in("bogus", Some(&mut soft)).unwrap(),
        MacAddr8::default()
    );
    assert!(soft.error_occurred());
}

#[test]
fn out_fixed_format() {
    assert_eq!(out_str(&CANON8), "08:00:2b:01:02:03:04:05");
    assert_eq!(out_str(&CANON6AS8), "08:00:2b:ff:fe:01:02:03");
}

#[test]
fn wire_roundtrip_and_short_form() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sent = macaddr8_send(mcx, &CANON8).unwrap();
    assert_eq!(sent.data(), &CANON8.to_bytes());

    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&CANON8.to_bytes()).unwrap();
    assert_eq!(macaddr8_recv(&mut buf).unwrap(), CANON8);

    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]).unwrap();
    assert_eq!(macaddr8_recv(&mut buf).unwrap(), CANON6AS8);
}

#[test]
fn cmp_ordering_and_hash() {
    let lo = MacAddr8::from_bytes([0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]);
    let hi = MacAddr8::from_bytes([0x80, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(macaddr8_cmp(&lo, &hi), -1);
    assert_eq!(macaddr8_cmp(&hi, &lo), 1);
    assert_eq!(macaddr8_cmp(&lo, &lo), 0);
    assert!(macaddr8_lt(&lo, &hi));
    assert!(macaddr8_le(&lo, &hi));
    assert!(macaddr8_gt(&hi, &lo));
    assert!(macaddr8_ge(&hi, &lo));
    assert!(macaddr8_ne(&lo, &hi));
    assert!(macaddr8_eq(&lo, &lo));

    let a = MacAddr8::from_bytes([1, 2, 3, 4, 0, 0, 0, 1]);
    let b = MacAddr8::from_bytes([1, 2, 3, 4, 0, 0, 0, 2]);
    assert_eq!(macaddr8_cmp(&a, &b), -1);

    assert_eq!(hashmacaddr8(&CANON8), hashfn::hash_bytes(&CANON8.to_bytes()));
    assert_eq!(
        hashmacaddr8extended(&CANON8, 42),
        hashfn::hash_bytes_extended(&CANON8.to_bytes(), 42)
    );
}

#[test]
fn bitwise_trunc_set7bit() {
    let x = MacAddr8::from_bytes([0xf0, 0x0f, 0xaa, 0x55, 0x00, 0xff, 0x12, 0x34]);
    assert_eq!(
        macaddr8_not(&x).to_bytes(),
        [0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00, 0xed, 0xcb]
    );
    let y = MacAddr8::from_bytes([0xff; 8]);
    assert_eq!(macaddr8_and(&x, &y), x);
    assert_eq!(macaddr8_or(&x, &MacAddr8::default()), x);
    assert_eq!(
        macaddr8_trunc(&x).to_bytes(),
        [0xf0, 0x0f, 0xaa, 0, 0, 0, 0, 0]
    );
    assert_eq!(macaddr8_set7bit(&x).a, 0xf2);
}

#[test]
fn conversions() {
    let m6 = MacAddr::from_bytes([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
    assert_eq!(macaddrtomacaddr8(&m6), CANON6AS8);
    assert_eq!(macaddr8tomacaddr(&CANON6AS8).unwrap(), m6);

    let err = macaddr8tomacaddr(&CANON8).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "macaddr8 data out of range to convert to macaddr"
    );
    assert!(err.hint().unwrap().starts_with("Only addresses that have FF and FE"));
}

fn mac8_datum(addr: &MacAddr8) -> Datum {
    Datum::from_usize(addr as *const MacAddr8 as usize)
}

#[test]
fn fc_wrappers() {
    let (a, b) = (CANON8, macaddr8_trunc(&CANON8));

    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, mac8_datum(&a));
    fcinfo.set_arg(1, mac8_datum(&b));
    assert!(!fc_macaddr8_eq(None, &mut fcinfo).unwrap().as_bool());
    assert!(fc_macaddr8_gt(None, &mut fcinfo).unwrap().as_bool());
    assert_eq!(fc_macaddr8_cmp(None, &mut fcinfo).unwrap().as_i32(), 1);

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, mac8_datum(&a));
    let d = fc_macaddr8_out(None, &mut fcinfo).unwrap();
    let cstr = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(cstr.to_bytes(), b"08:00:2b:01:02:03:04:05");

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, mac8_datum(&a));
    assert_eq!(
        fc_hashmacaddr8(None, &mut fcinfo).unwrap().as_u32(),
        hashmacaddr8(&a)
    );
}

#[test]
fn builtins_table_oid_ascending() {
    for w in MAC8_BUILTINS.windows(2) {
        assert!(w[0].foid < w[1].foid);
    }
}
