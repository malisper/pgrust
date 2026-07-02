use alloc::boxed::Box;

use ::datum::{Datum, NullableDatum};
use ::types_error::{PgError, PgResult};

use crate::fcinfo::*;

fn int4pl(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = fcinfo.arg_i32(0);
    let b = fcinfo.arg_i32(1);
    match a.checked_add(b) {
        Some(r) => Ok(Datum::from_i32(r)),
        None => Err(Box::new(PgError::error("integer out of range"))),
    }
}

fn always_null(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(fcinfo.return_null())
}

#[test]
fn frame_layout_matches_c_budget() {
    assert_eq!(core::mem::size_of::<NullableDatum>(), 16);
    assert_eq!(core::mem::offset_of!(LocalFcinfo<0>, args), 24);
    assert_eq!(core::mem::size_of::<LocalFcinfo<2>>(), 24 + 2 * 16);
    assert_eq!(core::mem::size_of::<FmgrInfo>(), 56);
    assert!(core::mem::size_of::<FmgrInfo>() <= 128);
}

#[test]
fn arg_write_read_lanes() {
    let mut fci = LocalFcinfo::<3>::new(100);
    fci.set_arg(0, Datum::from_i32(-7));
    fci.set_arg(1, Datum::from_i64(1 << 40));
    fci.set_arg(2, Datum::from_bool(true));
    assert_eq!(fci.arg_i32(0), -7);
    assert_eq!(fci.arg_i64(1), 1 << 40);
    assert!(fci.arg_bool(2));
    assert_eq!(fci.get_collation(), 100);
    assert_eq!(fci.nargs(), 3);
    assert!(!fci.has_null_args());

    fci.set_arg(0, Datum::from_f64(-2.25));
    assert_eq!(fci.arg_f64(0), -2.25);
    fci.set_arg(0, Datum::from_oid(2202));
    assert_eq!(fci.arg_oid(0), 2202);
}

#[test]
fn args_n_view_and_arity_guard() {
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(8));
    fci.set_arg(1, Datum::from_i32(9));
    let [a, b] = fci.args_n::<2>();
    assert_eq!(a.value.as_i32(), 8);
    assert_eq!(b.value.as_i32(), 9);
    assert!(!a.isnull && !b.isnull);
    let one = fci.args_n::<1>();
    assert_eq!(one[0].value.as_i32(), 8);
}

#[test]
#[should_panic(expected = "expects 3 args")]
fn args_n_over_arity_panics() {
    let fci = LocalFcinfo::<2>::new(0);
    let _ = fci.args_n::<3>();
}

#[test]
fn null_slots() {
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(1));
    fci.set_arg_null(1);
    assert!(!fci.argisnull(0));
    assert!(fci.argisnull(1));
    assert!(fci.has_null_args());
    assert_eq!(fci.arg(1), Datum::null());

    assert!(!fci.isnull);
    let d = fci.return_null();
    assert!(fci.isnull);
    assert_eq!(d, Datum::null());
}

#[test]
fn invoke_through_resolved_carrier() {
    let mut flinfo = FmgrInfo::new(int4pl, 177, 2, true, false);
    let mut fci = LocalFcinfo::<2>::new(0);
    for (a, b) in [(3i32, 4i32), (-1, 1), (i32::MAX, -1)] {
        fci.set_arg(0, Datum::from_i32(a));
        fci.set_arg(1, Datum::from_i32(b));
        fci.isnull = false;
        let r = flinfo.invoke(&mut fci).expect("int4pl ok");
        assert!(!fci.isnull);
        assert_eq!(r.as_i32(), a.wrapping_add(b));
    }
}

#[test]
fn pg_result_error_surface() {
    let mut flinfo = FmgrInfo::new(int4pl, 177, 2, true, false);
    let err = function_call2_coll(
        &mut flinfo,
        0,
        Datum::from_i32(i32::MAX),
        Datum::from_i32(1),
    )
    .unwrap_err();
    assert_eq!(err.message(), "integer out of range");
}

#[test]
fn function_call_rejects_null_result() {
    let mut flinfo = FmgrInfo::new(always_null, 42, 1, false, false);
    let err = function_call1_coll(&mut flinfo, 0, Datum::from_i32(0)).unwrap_err();
    assert_eq!(err.message(), "function 42 returned NULL");
}

#[test]
fn direct_function_call() {
    let r = direct_function_call2_coll(int4pl, 0, Datum::from_i32(20), Datum::from_i32(22))
        .expect("direct call ok");
    assert_eq!(r.as_i32(), 42);
    let err =
        direct_function_call1_coll(always_null, 0, Datum::from_i32(0)).unwrap_err();
    assert!(err.message().ends_with("returned NULL"));
}

#[test]
fn local_fcinfo_coerces_to_flexible_frame() {
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(5));
    let erased: &mut FunctionCallInfoBaseData = &mut fci;
    assert_eq!(erased.args.len(), 2);
    assert_eq!(erased.arg_i32(0), 5);
    erased.init(2, 900, None, None);
    assert_eq!(erased.get_collation(), 900);
}

#[test]
fn fn_extra_cache_roundtrip_and_clone_reset() {
    #[derive(Debug, PartialEq)]
    struct Cache {
        compiled: u64,
    }

    let mut flinfo = FmgrInfo::new(int4pl, 177, 2, true, false);
    assert!(!flinfo.has_fn_extra());
    assert!(flinfo.fn_extra_ref::<Cache>().is_none());
    flinfo.set_fn_extra(Cache { compiled: 9 });
    assert!(flinfo.has_fn_extra());
    assert_eq!(flinfo.fn_extra_ref::<Cache>().unwrap().compiled, 9);
    flinfo.fn_extra_mut::<Cache>().unwrap().compiled = 10;
    assert_eq!(flinfo.fn_extra_ref::<Cache>().unwrap().compiled, 10);

    let copy = flinfo.clone();
    assert!(!copy.has_fn_extra(), "fmgr_info_copy sets fn_extra = NULL");
    assert!(flinfo.has_fn_extra());
}

#[test]
#[should_panic(expected = "downcast_ref")]
fn fn_extra_wrong_type_panics() {
    let mut flinfo = FmgrInfo::new(int4pl, 177, 2, true, false);
    flinfo.set_fn_extra(3u32);
    let _ = flinfo.fn_extra_ref::<u64>();
}

#[test]
#[should_panic(expected = "never resolved")]
fn unresolved_carrier_panics_loudly() {
    let mut flinfo = FmgrInfo::unresolved();
    let mut fci = LocalFcinfo::<0>::new(0);
    let _ = flinfo.invoke(&mut fci);
}

mod byref {
    use super::*;
    use crate::getarg::*;

    #[test]
    fn varlena_4b_arg_borrows_source() {
        let payload = b"hello fmgr";
        let mut image = alloc::vec::Vec::new();
        image.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + payload.len()));
        image.extend_from_slice(payload);

        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let v = unsafe { fci.arg_varlena_packed(0) };
        assert_eq!(v.size(), 4 + payload.len());
        assert_eq!(v.data(), payload);
        assert_eq!(v.data().as_ptr(), image[4..].as_ptr());
    }

    #[test]
    fn varlena_short_header_arg() {
        // 1B header (LE): total_len << 1 | 1.
        let image: [u8; 4] = [(4u8 << 1) | 1, b'a', b'b', b'c'];
        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let v = unsafe { fci.arg_varlena_packed(0) };
        assert_eq!(v.size(), 4);
        assert_eq!(v.data(), b"abc");
    }

    #[test]
    #[should_panic(expected = "detoast unit")]
    fn external_varlena_panics_loudly() {
        let image: [u8; 18] = [0x01, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let _ = unsafe { fci.arg_varlena_packed(0) };
    }

    #[test]
    #[should_panic(expected = "detoast unit")]
    fn compressed_varlena_panics_loudly() {
        // 4B-C header (LE): low two bits 0b10.
        let image: [u8; 8] = [0x02, 0, 0, 0, 0, 0, 0, 0];
        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let _ = unsafe { fci.arg_varlena_packed(0) };
    }

    #[test]
    fn cstring_and_fixed_args() {
        let cs = b"12345\0";
        let uuid = [0xABu8; UUID_LEN];
        let mut fci = LocalFcinfo::<2>::new(0);
        fci.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
        fci.set_arg(1, Datum::from_usize(uuid.as_ptr() as usize));
        unsafe {
            assert_eq!(fci.arg_cstring(0).to_bytes(), b"12345");
            assert_eq!(fci.arg_uuid(1), &[0xAB; UUID_LEN]);
            assert_eq!(fci.arg_fixed(1, UUID_LEN), &[0xAB; UUID_LEN]);
            assert_eq!(fci.arg_uuid(1).as_ptr(), uuid.as_ptr());
        }
    }
}
