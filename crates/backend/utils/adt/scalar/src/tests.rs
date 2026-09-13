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

mod oidvector_tests {
    use crate::builtins::*;
    use datum::Datum;
    use mcx::MemoryContext;
    use types_fmgr::LocalFcinfo;

    fn build(values: &[u32]) -> Vec<u8> {
        let total = 24 + values.len() * 4;
        let mut v = Vec::from(::datum::set_varsize_4b(total));
        v.extend_from_slice(&1i32.to_ne_bytes());
        v.extend_from_slice(&0i32.to_ne_bytes());
        v.extend_from_slice(&26u32.to_ne_bytes());
        v.extend_from_slice(&(values.len() as i32).to_ne_bytes());
        v.extend_from_slice(&0i32.to_ne_bytes());
        for x in values {
            v.extend_from_slice(&x.to_ne_bytes());
        }
        v
    }

    fn call1(f: types_fmgr::PGFunction, d: Datum, ctx: &MemoryContext) -> Datum {
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, d);
        // SAFETY: ctx outlives the call.
        unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
        f(None, &mut fcinfo).unwrap()
    }

    fn call2(f: types_fmgr::PGFunction, a: Datum, b: Datum) -> bool {
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        fcinfo.set_arg(0, a);
        fcinfo.set_arg(1, b);
        f(None, &mut fcinfo).unwrap().as_bool()
    }

    #[test]
    fn in_out_roundtrip() {
        let ctx = MemoryContext::new("t");
        let s = b"1 2  40010\0";
        let d = call1(fc_oidvectorin, Datum::from_usize(s.as_ptr() as usize), &ctx);
        assert_eq!(
            unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 36) },
            &build(&[1, 2, 40010])[..]
        );
        let out = call1(fc_oidvectorout, d, &ctx);
        let bytes = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
        assert_eq!(bytes.to_bytes(), b"1 2 40010");

        let empty = call1(fc_oidvectorin, Datum::from_usize(b" \0".as_ptr() as usize), &ctx);
        let out = call1(fc_oidvectorout, empty, &ctx);
        let bytes = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
        assert_eq!(bytes.to_bytes(), b"");
    }

    #[test]
    fn dim1_out_of_bounds_rejected() {
        let ctx = MemoryContext::new("t");

        // A valid 3-element oidvector still passes and prints.
        let valid = build(&[1, 2, 3]);
        let out = call1(fc_oidvectorout, Datum::from_usize(valid.as_ptr() as usize), &ctx);
        let bytes =
            unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
        assert_eq!(bytes.to_bytes(), b"1 2 3");

        let call_err = |img: &[u8]| {
            let mut fcinfo = LocalFcinfo::<1>::new(0);
            fcinfo.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
            // SAFETY: ctx outlives the call.
            unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
            fc_oidvectorout(None, &mut fcinfo).err().unwrap()
        };

        // Empty-array cast (ndim==0) must still yield the EXISTING structural
        // error, unchanged in text and SQLSTATE (regression guard). Backed by a
        // full 24-byte header so the header read stays in-bounds for the test.
        let mut empty = Vec::from(::datum::set_varsize_4b(16));
        empty.extend_from_slice(&0i32.to_ne_bytes()); // ndim = 0
        empty.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset
        empty.extend_from_slice(&26u32.to_ne_bytes()); // elemtype OID
        empty.extend_from_slice(&0i32.to_ne_bytes()); // dim1
        empty.extend_from_slice(&0i32.to_ne_bytes()); // lbound1
        let err = call_err(&empty);
        assert_eq!(err.message(), "array is not a valid oidvector");

        // Crafted image: structurally valid header (ndim==1) but dim1 claims far
        // more Oids than VARSIZE can hold -> rejected before the values slice is
        // formed, routed to the SAME error (no new "corrupt ..." message).
        let mut crafted = Vec::from(::datum::set_varsize_4b(24)); // header only
        crafted.extend_from_slice(&1i32.to_ne_bytes()); // ndim = 1
        crafted.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset
        crafted.extend_from_slice(&26u32.to_ne_bytes()); // elemtype OID
        crafted.extend_from_slice(&1000i32.to_ne_bytes()); // dim1 huge
        crafted.extend_from_slice(&0i32.to_ne_bytes()); // lbound1
        let err = call_err(&crafted);
        assert_eq!(err.message(), "array is not a valid oidvector");
    }

    #[test]
    fn comparators() {
        let a = build(&[1, 2, 3]);
        let b = build(&[1, 2, 4]);
        let short = build(&[9]);
        let d = |v: &Vec<u8>| Datum::from_usize(v.as_ptr() as usize);
        assert!(call2(fc_oidvectorlt, d(&a), d(&b)));
        assert!(call2(fc_oidvectorle, d(&a), d(&a)));
        assert!(call2(fc_oidvectorge, d(&b), d(&a)));
        assert!(call2(fc_oidvectorgt, d(&b), d(&a)));
        assert!(call2(fc_oidvectorne, d(&a), d(&b)));
        assert!(!call2(fc_oidvectorne, d(&a), d(&a)));
        // Length sorts first (btoidvectorcmp).
        assert!(call2(fc_oidvectorlt, d(&short), d(&a)));
    }
}

mod datum_ops_tests {
    use crate::datum_ops::*;
    use datum::{set_varsize_4b, Datum, VARHDRSZ};
    use mcx::MemoryContext;

    fn varlena(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::from(set_varsize_4b(VARHDRSZ + payload.len()));
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn get_size_arms() {
        assert_eq!(datum_get_size(Datum::from_i32(7), true, 4).unwrap(), 4);
        assert_eq!(datum_get_size(Datum::from_i32(7), true, 8).unwrap(), 8);
        let img = varlena(b"hello");
        let d = Datum::from_usize(img.as_ptr() as usize);
        assert_eq!(datum_get_size(d, false, -1).unwrap(), VARHDRSZ + 5);
        let cs = b"abc\0";
        let d = Datum::from_usize(cs.as_ptr() as usize);
        assert_eq!(datum_get_size(d, false, -2).unwrap(), 4);
        let d16 = [0u8; 16];
        let d = Datum::from_usize(d16.as_ptr() as usize);
        assert_eq!(datum_get_size(d, false, 16).unwrap(), 16);
        assert!(datum_get_size(Datum::null(), false, -1).is_err());
        assert!(datum_get_size(Datum::from_i32(1), false, -3).is_err());
    }

    #[test]
    fn copy_is_deep() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = varlena(b"payload");
        let src = Datum::from_usize(img.as_ptr() as usize);
        let cp = datum_copy(mcx, src, false, -1).unwrap();
        assert_ne!(cp.as_usize(), src.as_usize());
        let out = unsafe {
            core::slice::from_raw_parts(cp.as_usize() as *const u8, img.len())
        };
        assert_eq!(out, &img[..]);
        assert_eq!(datum_copy(mcx, Datum::from_i32(-5), true, 4).unwrap().as_i32(), -5);
    }

    #[test]
    fn serialize_layout_and_roundtrip() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut out = mcx::PgVec::new_in(mcx);

        datum_serialize(Datum::from_usize(42), false, true, 4, &mut out).unwrap();
        let mut expect = Vec::new();
        expect.extend_from_slice(&(-1i32).to_ne_bytes());
        expect.extend_from_slice(&42u64.to_ne_bytes());
        assert_eq!(&out[..], &expect[..]);
        assert_eq!(out.len(), datum_estimate_space(Datum::from_usize(42), false, true, 4).unwrap());

        out.clear();
        datum_serialize(Datum::null(), true, false, -1, &mut out).unwrap();
        assert_eq!(&out[..], &(-2i32).to_ne_bytes());
        assert_eq!(out.len(), datum_estimate_space(Datum::null(), true, false, -1).unwrap());

        out.clear();
        let img = varlena(b"xyz");
        let d = Datum::from_usize(img.as_ptr() as usize);
        datum_serialize(d, false, false, -1, &mut out).unwrap();
        let mut expect = Vec::new();
        expect.extend_from_slice(&(img.len() as i32).to_ne_bytes());
        expect.extend_from_slice(&img);
        assert_eq!(&out[..], &expect[..]);
        assert_eq!(out.len(), datum_estimate_space(d, false, false, -1).unwrap());

        let mut cur: &[u8] = &out;
        let (rv, rn) = datum_restore(mcx, &mut cur).unwrap();
        assert!(!rn && cur.is_empty());
        let rimg = unsafe {
            core::slice::from_raw_parts(rv.as_usize() as *const u8, img.len())
        };
        assert_eq!(rimg, &img[..]);
    }

    #[test]
    fn restore_null_and_byval() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(-2i32).to_ne_bytes());
        buf.extend_from_slice(&(-1i32).to_ne_bytes());
        buf.extend_from_slice(&7u64.to_ne_bytes());
        let mut cur: &[u8] = &buf;
        let (v, isnull) = datum_restore(mcx, &mut cur).unwrap();
        assert!(isnull && v.as_usize() == 0);
        let (v, isnull) = datum_restore(mcx, &mut cur).unwrap();
        assert!(!isnull && v.as_usize() == 7 && cur.is_empty());
    }

    #[test]
    fn is_equal_image_compare() {
        assert!(datum_is_equal(Datum::from_i64(9), Datum::from_i64(9), true, 8).unwrap());
        assert!(!datum_is_equal(Datum::from_i64(9), Datum::from_i64(8), true, 8).unwrap());
        let a = varlena(b"abc");
        let b = varlena(b"abc");
        let c = varlena(b"abd");
        let short = varlena(b"ab");
        let d = |v: &Vec<u8>| Datum::from_usize(v.as_ptr() as usize);
        assert!(datum_is_equal(d(&a), d(&b), false, -1).unwrap());
        assert!(!datum_is_equal(d(&a), d(&c), false, -1).unwrap());
        assert!(!datum_is_equal(d(&a), d(&short), false, -1).unwrap());
        let cs1 = b"xy\0";
        let cs2 = b"xy\0";
        assert!(datum_is_equal(
            Datum::from_usize(cs1.as_ptr() as usize),
            Datum::from_usize(cs2.as_ptr() as usize),
            false,
            -2
        )
        .unwrap());
    }

    #[test]
    fn transfer_copies_non_expanded() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = varlena(b"move me");
        let src = Datum::from_usize(img.as_ptr() as usize);
        let t = datum_transfer(mcx, src, false, -1).unwrap();
        assert_ne!(t.as_usize(), src.as_usize());
        let out = unsafe { core::slice::from_raw_parts(t.as_usize() as *const u8, img.len()) };
        assert_eq!(out, &img[..]);
        assert_eq!(datum_transfer(mcx, Datum::from_i32(3), true, 4).unwrap().as_i32(), 3);
    }
}

#[test]
fn oidlarger_oidsmaller_match_c() {
    // oid.c: PG_RETURN_OID((arg1 > arg2) ? arg1 : arg2) / (arg1 < arg2).
    assert_eq!(oidlarger(10, 20), 20);
    assert_eq!(oidlarger(20, 10), 20);
    assert_eq!(oidlarger(7, 7), 7);
    assert_eq!(oidsmaller(10, 20), 10);
    assert_eq!(oidsmaller(20, 10), 10);
    assert_eq!(oidsmaller(7, 7), 7);
}

#[test]
fn out_functions_do_not_alias_across_fmgrinfos() {
    use ::datum::Datum;
    use types_fmgr::{FmgrInfo, LocalFcinfo};
    for (f, oid, a, b) in [
        (crate::builtins::fc_oidout as types_fmgr::PGFunction, 1799u32, 11u32, 22u32),
        (crate::builtins::fc_xidout, 51, 5, 6),
        (crate::builtins::fc_cidout, 31, 7, 8),
    ] {
        let mut f1 = FmgrInfo::new(f, oid, 1, true, false);
        let mut f2 = FmgrInfo::new(f, oid, 1, true, false);
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, Datum::from_u32(a));
        let d1 = f1.invoke(&mut fcinfo).unwrap();
        fcinfo.set_arg(0, Datum::from_u32(b));
        let d2 = f2.invoke(&mut fcinfo).unwrap();
        let cs = |d: Datum| {
            unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) }
                .to_bytes()
                .to_vec()
        };
        assert_eq!(cs(d1), a.to_string().as_bytes());
        assert_eq!(cs(d2), b.to_string().as_bytes());
    }
}

#[test]
fn tidout_and_xid8out_do_not_alias_across_fmgrinfos() {
    use ::datum::Datum;
    use types_fmgr::{FmgrInfo, LocalFcinfo};
    let cs = |d: Datum| {
        unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) }
            .to_bytes()
            .to_vec()
    };

    let mut f1 = FmgrInfo::new(crate::builtins::fc_xid8out, 5081, 1, true, false);
    let mut f2 = FmgrInfo::new(crate::builtins::fc_xid8out, 5081, 1, true, false);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_u64(33));
    let d1 = f1.invoke(&mut fcinfo).unwrap();
    fcinfo.set_arg(0, Datum::from_u64(44));
    let d2 = f2.invoke(&mut fcinfo).unwrap();
    assert_eq!(cs(d1), b"33");
    assert_eq!(cs(d2), b"44");

    let a = crate::builtins::tid_image(Tid { block: 1, offset: 2 });
    let b = crate::builtins::tid_image(Tid { block: 3, offset: 4 });
    let mut f1 = FmgrInfo::new(crate::builtins::fc_tidout, 49, 1, true, false);
    let mut f2 = FmgrInfo::new(crate::builtins::fc_tidout, 49, 1, true, false);
    fcinfo.set_arg(0, Datum::from_usize(a.as_ptr() as usize));
    let d1 = f1.invoke(&mut fcinfo).unwrap();
    fcinfo.set_arg(0, Datum::from_usize(b.as_ptr() as usize));
    let d2 = f2.invoke(&mut fcinfo).unwrap();
    assert_eq!(cs(d1), b"(1,2)");
    assert_eq!(cs(d2), b"(3,4)");
}
