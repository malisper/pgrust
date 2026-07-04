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
    assert_eq!(core::mem::offset_of!(LocalFcinfo<0>, args), 32);
    assert_eq!(core::mem::size_of::<LocalFcinfo<2>>(), 32 + 2 * 16);
    assert_eq!(core::mem::size_of::<FmgrInfo>(), 48);
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
#[should_panic(expected = "downcast to u64 failed")]
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
        let v = unsafe { fci.arg_varlena_packed(0) }.unwrap();
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
        let v = unsafe { fci.arg_varlena_packed(0) }.unwrap();
        assert_eq!(v.size(), 4);
        assert_eq!(v.data(), b"abc");
    }

    #[test]
    #[should_panic(expected = "never armed")]
    fn external_varlena_needs_armed_mcx() {
        let image: [u8; 18] = [0x01, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let _ = unsafe { fci.arg_varlena_packed(0) };
    }

    #[test]
    #[should_panic(expected = "never armed")]
    fn compressed_varlena_needs_armed_mcx() {
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

mod result_mcx {
    use ::datum::{Datum, Varlena, VarlenaRef};
    use ::mcx::MemoryContext;
    use ::types_error::PgResult;

    use crate::fcinfo::*;
    use crate::result::*;

    fn text_datum_of_arg0(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        // SAFETY: test arg 0 is a live NUL-terminated cstring.
        let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
        let mcx = fcinfo.result_mcx();
        let mut image = ::mcx::vec_with_capacity_in(mcx, 4 + s.len())?;
        image.resize(4, 0);
        image.extend_from_slice(s);
        Ok(varlena_result(Varlena::from_image(image)))
    }

    fn armed_fci(ctx: &MemoryContext, arg: &'static [u8]) -> LocalFcinfo<1> {
        let mut fci = LocalFcinfo::<1>::fresh(0);
        // SAFETY: every test's ctx outlives its calls through the frame.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, Datum::from_usize(arg.as_ptr() as usize));
        fci
    }

    fn text_of(d: Datum) -> &'static [u8] {
        // SAFETY: test results are live 4B-header varlenas kept in the ctx.
        unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data()
    }

    #[test]
    fn armed_frame_allocates_in_context_and_reset_frees() {
        let mut ctx = MemoryContext::new_bump("fc-result");
        let before = ctx.used();
        {
            let mut fci = armed_fci(&ctx, b"hi\0");
            let d = text_datum_of_arg0(None, &mut fci).unwrap();
            assert_eq!(text_of(d), b"hi");
            assert!(ctx.used() > before);
        }
        ctx.reset();
        let keeper = ctx.used();
        for _ in 0..64 {
            let mut fci = armed_fci(&ctx, b"hi\0");
            let _ = text_datum_of_arg0(None, &mut fci).unwrap();
            ctx.reset();
        }
        assert_eq!(ctx.used(), keeper);
    }

    #[test]
    fn direct_call_in_arms_the_frame() {
        let ctx = MemoryContext::new_bump("fc-result");
        let d = direct_function_call1_coll_in(
            text_datum_of_arg0,
            0,
            ctx.mcx(),
            Datum::from_usize(b"abc\0".as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"abc");
    }

    #[test]
    #[should_panic(expected = "never armed")]
    fn unarmed_frame_panics_loudly() {
        let mut fci = LocalFcinfo::<1>::fresh(0);
        fci.set_arg(0, Datum::from_usize(b"x\0".as_ptr() as usize));
        let _ = text_datum_of_arg0(None, &mut fci);
    }

    #[test]
    fn byref_result_copies_at_palloc_alignment() {
        let ctx = MemoryContext::new_bump("fc-result");
        let image = [1u8, 2, 3, 4, 5, 6, 7];
        let d = byref_result(ctx.mcx(), &image).unwrap();
        let p = d.as_usize() as *const u8;
        assert_eq!(p as usize % 8, 0);
        // SAFETY: 7 bytes just copied into ctx at p.
        assert_eq!(unsafe { core::slice::from_raw_parts(p, 7) }, &image);
    }

    #[test]
    fn cstring_result_leaks_the_buffer_in_place() {
        let ctx = MemoryContext::new_bump("fc-result");
        let mut v = ::mcx::vec_with_capacity_in(ctx.mcx(), 3).unwrap();
        v.extend_from_slice(b"ab\0");
        let want = v.as_ptr() as usize;
        let d = cstring_result(v);
        assert_eq!(d.as_usize(), want);
    }

    #[test]
    fn rearm_preserves_the_result_mcx() {
        let ctx = MemoryContext::new_bump("fc-result");
        let mut fci = armed_fci(&ctx, b"kept\0");
        fci.rearm(0);
        fci.set_arg(0, Datum::from_usize(b"kept\0".as_ptr() as usize));
        let d = text_datum_of_arg0(None, &mut fci).unwrap();
        assert_eq!(text_of(d), b"kept");
    }
}

mod soft {
    use ::datum::Datum;
    use ::mcx::MemoryContext;
    use ::types_error::{ereturn, PgError, PgResult};

    use crate::fcinfo::*;
    use crate::soft::*;

    fn parse_i32(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        // SAFETY: test arg 0 is a live NUL-terminated cstring.
        let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
        match core::str::from_utf8(s)
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
        {
            Some(v) => Ok(Datum::from_i32(v)),
            None => {
                // SAFETY: context, if set, was armed by input_function_call_safe.
                let esc = unsafe { fcinfo.soft_error_context() };
                ereturn(esc, Datum::null(), PgError::error("bad int"))
            }
        }
    }

    fn null_returning(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        Ok(fcinfo.return_null())
    }

    fn strict_flinfo(f: PGFunction) -> FmgrInfo {
        FmgrInfo::new(f, 42, 3, true, false)
    }

    fn call(
        flinfo: &mut FmgrInfo,
        s: Option<&core::ffi::CStr>,
        esc: Option<&mut ErrorSaveNode>,
    ) -> (PgResult<bool>, Datum) {
        let ctx = MemoryContext::new_bump("soft-test");
        let mut result = Datum::null();
        let ok = input_function_call_safe(flinfo, s, 0, -1, ctx.mcx(), esc, &mut result);
        (ok, result)
    }

    #[test]
    fn success_returns_true_and_the_datum() {
        let mut fl = strict_flinfo(parse_i32);
        let (ok, result) = call(&mut fl, Some(c"123"), None);
        assert!(ok.unwrap());
        assert_eq!(result.as_i32(), 123);
    }

    #[test]
    fn soft_error_with_details_saves_and_returns_false() {
        let mut fl = strict_flinfo(parse_i32);
        let mut esc = ErrorSaveNode::new(true);
        let (ok, _) = call(&mut fl, Some(c"nope"), Some(&mut esc));
        assert!(!ok.unwrap());
        assert!(esc.ctx.error_occurred());
        assert_eq!(esc.ctx.error().unwrap().message(), "bad int");
    }

    #[test]
    fn soft_error_without_details_only_marks() {
        let mut fl = strict_flinfo(parse_i32);
        let mut esc = ErrorSaveNode::new(false);
        let (ok, _) = call(&mut fl, Some(c"nope"), Some(&mut esc));
        assert!(!ok.unwrap());
        assert!(esc.ctx.error_occurred());
        assert!(esc.ctx.error().is_none());
    }

    #[test]
    fn no_context_is_a_hard_error() {
        let mut fl = strict_flinfo(parse_i32);
        let (ok, _) = call(&mut fl, Some(c"nope"), None);
        assert!(ok.is_err());
    }

    #[test]
    fn strict_null_input_short_circuits() {
        let mut fl = strict_flinfo(parse_i32);
        let mut esc = ErrorSaveNode::new(true);
        let (ok, result) = call(&mut fl, None, Some(&mut esc));
        assert!(ok.unwrap());
        assert_eq!(result.as_usize(), 0);
        assert!(!esc.ctx.error_occurred());
    }

    fn const_seven(
        _flinfo: Option<&mut FmgrInfo>,
        _fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        Ok(Datum::from_i32(7))
    }

    #[test]
    fn non_strict_null_input_runs_and_must_return_null() {
        let mut fl = FmgrInfo::new(null_returning, 42, 3, false, false);
        let (ok, _) = call(&mut fl, None, None);
        assert!(ok.unwrap());

        let mut fl = FmgrInfo::new(const_seven, 42, 3, false, false);
        let (ok, _) = call(&mut fl, None, None);
        let err = ok.unwrap_err();
        assert!(err.message().contains("returned non-NULL"), "{}", err.message());
    }

    #[test]
    fn null_result_for_present_input_is_a_hard_error() {
        let mut fl = strict_flinfo(null_returning);
        let (ok, _) = call(&mut fl, Some(c"1"), None);
        let err = ok.unwrap_err();
        assert!(err.message().contains("returned NULL"), "{}", err.message());
    }

    #[test]
    fn direct_soft_and_success() {
        let ctx = MemoryContext::new_bump("soft-test");
        let mut result = Datum::null();
        assert!(direct_input_function_call_safe(
            parse_i32, Some(c"7"), 0, -1, ctx.mcx(), None, &mut result
        )
        .unwrap());
        assert_eq!(result.as_i32(), 7);

        let mut esc = ErrorSaveNode::new(true);
        assert!(!direct_input_function_call_safe(
            parse_i32, Some(c"x"), 0, -1, ctx.mcx(), Some(&mut esc), &mut result
        )
        .unwrap());
        assert!(esc.ctx.error_occurred());

        assert!(direct_input_function_call_safe(
            parse_i32, None, 0, -1, ctx.mcx(), None, &mut result
        )
        .unwrap());
        assert_eq!(result.as_usize(), 0);
    }

    #[test]
    fn input_function_call_hard_success() {
        let mut fl = strict_flinfo(parse_i32);
        let ctx = MemoryContext::new_bump("soft-test");
        let d = input_function_call(&mut fl, Some(c"55"), 0, -1, ctx.mcx()).unwrap();
        assert_eq!(d.as_i32(), 55);
    }

    #[test]
    fn foreign_context_tag_demuxes_to_none() {
        let mut node = FmNode { tag: 383 };
        let mut fci = LocalFcinfo::<1>::fresh(0);
        fci.context = Some(core::ptr::NonNull::from(&mut node));
        // SAFETY: node outlives the call; tag != T_ErrorSaveContext.
        assert!(unsafe { fci.soft_error_context() }.is_none());
    }
}

#[test]
fn fn_extra_take_restore_and_drop() {
    use core::sync::atomic::{AtomicU32, Ordering};
    static DROPS: AtomicU32 = AtomicU32::new(0);
    struct Memo(#[allow(dead_code)] alloc::vec::Vec<u64>);
    impl Drop for Memo {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut a = FmgrInfo::new(int4pl, 177, 2, true, false);
    a.set_fn_extra(Memo(alloc::vec![1, 2, 3]));
    let taken = a.fn_extra.take();
    assert!(!a.has_fn_extra());
    let mut b = FmgrInfo::new(int4pl, 177, 2, true, false);
    b.fn_extra = taken;
    assert_eq!(b.fn_extra_ref::<Memo>().unwrap().0, alloc::vec![1, 2, 3]);
    assert_eq!(DROPS.load(Ordering::Relaxed), 0);
    b.set_fn_extra(Memo(alloc::vec![9]));
    assert_eq!(DROPS.load(Ordering::Relaxed), 1, "replacement drops the old memo");
    drop(b);
    assert_eq!(DROPS.load(Ordering::Relaxed), 2, "flinfo death drops the memo");
}
