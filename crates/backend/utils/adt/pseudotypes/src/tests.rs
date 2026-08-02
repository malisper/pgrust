use mcx::MemoryContext;
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;

use crate::*;

#[test]
fn dummy_stubs_carry_0a000_and_exact_messages() {
    let err = any_in().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot accept a value of type any");

    let err = trigger_out().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot display a value of type trigger");

    let err = anyarray_in().unwrap_err();
    assert_eq!(err.message(), "cannot accept a value of type anyarray");
    let err = anyarray_recv().unwrap_err();
    assert_eq!(err.message(), "cannot accept a value of type anyarray");

    let err = pg_ddl_command_send().unwrap_err();
    assert_eq!(err.message(), "cannot display a value of type pg_ddl_command");

    let err = shell_in().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot accept a value of a shell type");
    let err = shell_out().unwrap_err();
    assert_eq!(err.message(), "cannot display a value of a shell type");
}

#[test]
fn cstring_io_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(&cstring_in(mcx, b"blah").unwrap()[..], b"blah\0");
    assert_eq!(&cstring_out(mcx, b"blah\0trail").unwrap()[..], b"blah\0");
    assert_eq!(&cstring_in(mcx, b"").unwrap()[..], b"\0");
}

#[test]
fn void_io() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(void_in().as_usize(), 0);
    assert_eq!(void_recv().as_usize(), 0);
    assert_eq!(&void_out(mcx).unwrap()[..], b"\0");
    assert_eq!(void_send(mcx).unwrap().data(), b"");
}

#[test]
fn pg_node_tree_out_passes_text_through() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(&pg_node_tree_out(mcx, b"{QUERY :junk 1}").unwrap()[..], b"{QUERY :junk 1}\0");
}

#[test]
fn void_send_fmgr_wrapper_sends_empty_payload() {
    use types_fmgr::LocalFcinfo;

    let ctx = MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, datum::Datum::null());
    let d = builtins::fc_void_send(None, &mut fcinfo).unwrap();
    // SAFETY: fc_void_send returns a live 4B-header varlena in ctx.
    let v = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert_eq!(v.data(), b"", "C void_send: zero payload bytes on the wire");
}

#[test]
fn builtins_table_is_oid_ascending() {
    let t = builtins::PSEUDOTYPES_BUILTINS;
    assert_eq!(t.len(), 53);
    for w in t.windows(2) {
        assert!(w[0].foid < w[1].foid, "{} !< {}", w[0].foid, w[1].foid);
    }
}

#[test]
fn cstring_send_fmgr_wrapper_sends_raw_bytes() {
    use types_fmgr::LocalFcinfo;

    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Identity client<->server conversion (single-encoding test setup).
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
    });
    let ctx = MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    let input = b"abc\0";
    fcinfo.set_arg(0, datum::Datum::from_usize(input.as_ptr() as usize));
    let d = builtins::fc_cstring_send(None, &mut fcinfo).unwrap();
    // SAFETY: fc_cstring_send returns a live 4B-header varlena in ctx.
    let v = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert_eq!(v.data(), b"abc", "C cstring_send: raw text bytes, no NUL");
}

#[test]
fn cstring_recv_fmgr_wrapper_reads_remaining_bytes() {
    use types_fmgr::LocalFcinfo;

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(b"abc").unwrap();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(
        0,
        datum::Datum::from_usize(&mut buf as *mut stringinfo::StringInfo as usize),
    );
    let d = builtins::fc_cstring_recv(None, &mut fcinfo).unwrap();
    // SAFETY: fc_cstring_recv returns a fresh NUL-terminated cstring.
    let out = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(out.to_bytes(), b"abc");
}

#[test]
fn cstring_in_fmgr_wrapper_pstrdups() {
    use types_fmgr::LocalFcinfo;

    let ctx = MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    let input = b"blah\0";
    fcinfo.set_arg(0, datum::Datum::from_usize(input.as_ptr() as usize));
    let d = builtins::fc_cstring_in(None, &mut fcinfo).unwrap();
    // SAFETY: fc_cstring_in returns a fresh NUL-terminated cstring.
    let out = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(out.to_bytes(), b"blah");
}
