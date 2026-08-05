//! Native rig validation for the json-text family: same vendored C, same
//! shipped Rust entry points, randomized payload sweep. Census-grade.
use proof_json_text as _;
use std::os::raw::c_int;

extern "C" {
    fn pg_json_out(vardata: *const u8, len: c_int, result: *mut u8) -> c_int;
    fn pg_json_send(vardata: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg_json_build_object_noargs(out: *mut u8) -> c_int;
    fn pg_json_build_array_noargs(out: *mut u8) -> c_int;
}

fn install_seam_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils_seams::pg_server_to_client::set(|_mcx, _s| Ok(None));
    });
}

fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn native_json_text_sweep() {
    install_seam_once();
    let ctx = mcx::MemoryContext::new_bump("json-native");
    let mut st = 0x243f6a8885a308d3u64;
    for iter in 0..500_000u64 {
        let len = (xs(&mut st) % 64) as usize;
        let mut buf = vec![0u8; len];
        for b in buf.iter_mut() {
            *b = xs(&mut st) as u8;
        }
        // json_out
        let mut c_out = vec![0u8; len + 1];
        let c_len = unsafe { pg_json_out(buf.as_ptr(), len as c_int, c_out.as_mut_ptr()) } as usize;
        let r = adt_json::json_out(ctx.mcx(), &buf).expect("json_out");
        assert_eq!(r.len(), c_len + 1, "out len iter={iter}");
        assert_eq!(&r[..], &c_out[..c_len + 1], "out bytes iter={iter}");
        // json_send
        let mut c_send = vec![0u8; len + 4];
        let c_total =
            unsafe { pg_json_send(buf.as_ptr(), len as c_int, c_send.as_mut_ptr()) } as usize;
        let b = adt_json::json_send(ctx.mcx(), &buf).expect("json_send");
        assert_eq!(b.varsize(), c_total, "send size iter={iter}");
        assert_eq!(b.as_bytes(), &c_send[..c_total], "send image iter={iter}");
    }
    // noargs builders via the shipped wrappers
    for (cfn, rfn, tag) in [
        (
            pg_json_build_object_noargs as unsafe extern "C" fn(*mut u8) -> c_int,
            adt_json::builtins::fc_json_build_object_noargs
                as fn(
                    Option<&mut types_fmgr::FmgrInfo>,
                    &mut types_fmgr::FunctionCallInfoBaseData,
                ) -> types_error::PgResult<datum::Datum>,
            "object",
        ),
        (
            pg_json_build_array_noargs,
            adt_json::builtins::fc_json_build_array_noargs,
            "array",
        ),
    ] {
        let mut c_out = [0u8; 8];
        let c_total = unsafe { cfn(c_out.as_mut_ptr()) } as usize;
        let mut f = types_fmgr::LocalFcinfo::<0>::new(0);
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let d = rfn(None, &mut f).expect(tag);
        assert!(!f.isnull);
        let p = d.as_usize() as *const u8;
        let img = unsafe { std::slice::from_raw_parts(p, c_total) };
        assert_eq!(img, &c_out[..c_total], "{tag} image");
    }
}
