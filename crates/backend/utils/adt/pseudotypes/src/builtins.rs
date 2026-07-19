//! fmgr wrappers (`fc_*`) + `PSEUDOTYPES_BUILTINS` for fmgr-core. Registered:
//! every ereport-only stub, void_in/void_out/void_recv/void_send,
//! cstring_in/cstring_out (fn_extra scratch / cstring_result frame, the
//! varlena unknownin precedent), pg_node_tree_out (varlena fc_textout
//! delegate), pg_node_tree_send (varlena textsend delegate). Not registrable:
//! cstring_recv/cstring_send (wire, no pg_proc callers) and the *_out/*_send
//! delegates whose target unit is unported
//! (anyarray/anycompatiblearray/anyenum/anyrange/anycompatiblerange/
//! anymultirange/anycompatiblemultirange).

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

macro_rules! fc_stub {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            crate::$core()
        }
    )*};
}

fc_stub! {
    fc_anyarray_in: anyarray_in;
    fc_anyarray_recv: anyarray_recv;
    fc_anycompatiblearray_in: anycompatiblearray_in;
    fc_anycompatiblearray_recv: anycompatiblearray_recv;
    fc_anyenum_in: anyenum_in;
    fc_anyrange_in: anyrange_in;
    fc_anycompatiblerange_in: anycompatiblerange_in;
    fc_anymultirange_in: anymultirange_in;
    fc_anycompatiblemultirange_in: anycompatiblemultirange_in;
    fc_pg_node_tree_in: pg_node_tree_in;
    fc_pg_node_tree_recv: pg_node_tree_recv;
    fc_pg_ddl_command_in: pg_ddl_command_in;
    fc_pg_ddl_command_out: pg_ddl_command_out;
    fc_pg_ddl_command_recv: pg_ddl_command_recv;
    fc_pg_ddl_command_send: pg_ddl_command_send;
    fc_any_in: any_in;
    fc_any_out: any_out;
    fc_trigger_in: trigger_in;
    fc_trigger_out: trigger_out;
    fc_event_trigger_in: event_trigger_in;
    fc_event_trigger_out: event_trigger_out;
    fc_language_handler_in: language_handler_in;
    fc_language_handler_out: language_handler_out;
    fc_fdw_handler_in: fdw_handler_in;
    fc_fdw_handler_out: fdw_handler_out;
    fc_table_am_handler_in: table_am_handler_in;
    fc_table_am_handler_out: table_am_handler_out;
    fc_index_am_handler_in: index_am_handler_in;
    fc_index_am_handler_out: index_am_handler_out;
    fc_tsm_handler_in: tsm_handler_in;
    fc_tsm_handler_out: tsm_handler_out;
    fc_internal_in: internal_in;
    fc_internal_out: internal_out;
    fc_anyelement_in: anyelement_in;
    fc_anyelement_out: anyelement_out;
    fc_anynonarray_in: anynonarray_in;
    fc_anynonarray_out: anynonarray_out;
    fc_anycompatible_in: anycompatible_in;
    fc_anycompatible_out: anycompatible_out;
    fc_anycompatiblenonarray_in: anycompatiblenonarray_in;
    fc_anycompatiblenonarray_out: anycompatiblenonarray_out;
    fc_shell_in: shell_in;
    fc_shell_out: shell_out;
}

pub fn fc_void_in(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(crate::void_in())
}

pub fn fc_void_recv(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(crate::void_recv())
}

// C void_send: an empty bytea payload on the binary wire (pseudotypes.c).
pub fn fc_void_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::void_send(mcx)?))
}

// C pstrdup("")s per call; the empty cstring is immutable, so one static.
static EMPTY_CSTR: [u8; 1] = [0];

pub fn fc_void_out(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_usize(EMPTY_CSTR.as_ptr() as usize))
}

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: cstring result needs a resolved FmgrInfo's scratch; direct callers use the value core")
}

// The varlena fc_unknownout retained-scratch shape (rule 7).
struct OutBuf(Vec<u8>);

fn out_scratch<'a>(flinfo: Option<&'a mut FmgrInfo>, name: &'static str) -> &'a mut Vec<u8> {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0
}

pub fn fc_cstring_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of cstring_in is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::cstring_in(mcx, s)?))
}

pub fn fc_cstring_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of cstring_out is a non-null cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let buf = out_scratch(flinfo, "cstring_out");
    buf.clear();
    buf.extend_from_slice(s.to_bytes_with_nul());
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_pg_node_tree_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    varlena::builtins::fc_textout(flinfo, fcinfo)
}

// C pg_node_tree_send is `return textsend(fcinfo)` (pseudotypes.c).
pub fn fc_pg_node_tree_send(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    varlena::builtins::fc_textsend(flinfo, fcinfo)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (none retset), OID-ascending; strict flags per pg_proc.dat
// (the handler/trigger/internal/shell in-functions are proisstrict=f).
pub const PSEUDOTYPES_BUILTINS: &[FmgrBuiltin] = &[
    b(86, "pg_ddl_command_in", 1, true, fc_pg_ddl_command_in),
    b(87, "pg_ddl_command_out", 1, true, fc_pg_ddl_command_out),
    b(88, "pg_ddl_command_recv", 1, true, fc_pg_ddl_command_recv),
    b(90, "pg_ddl_command_send", 1, true, fc_pg_ddl_command_send),
    b(195, "pg_node_tree_in", 1, true, fc_pg_node_tree_in),
    b(196, "pg_node_tree_out", 1, true, fc_pg_node_tree_out),
    b(197, "pg_node_tree_recv", 1, true, fc_pg_node_tree_recv),
    b(198, "pg_node_tree_send", 1, true, fc_pg_node_tree_send),
    b(267, "table_am_handler_in", 1, false, fc_table_am_handler_in),
    b(268, "table_am_handler_out", 1, true, fc_table_am_handler_out),
    b(326, "index_am_handler_in", 1, false, fc_index_am_handler_in),
    b(327, "index_am_handler_out", 1, true, fc_index_am_handler_out),
    b(2292, "cstring_in", 1, true, fc_cstring_in),
    b(2293, "cstring_out", 1, true, fc_cstring_out),
    b(2294, "any_in", 1, true, fc_any_in),
    b(2295, "any_out", 1, true, fc_any_out),
    b(2296, "anyarray_in", 1, true, fc_anyarray_in),
    b(2298, "void_in", 1, true, fc_void_in),
    b(2299, "void_out", 1, true, fc_void_out),
    b(2300, "trigger_in", 1, false, fc_trigger_in),
    b(2301, "trigger_out", 1, true, fc_trigger_out),
    b(2302, "language_handler_in", 1, false, fc_language_handler_in),
    b(2303, "language_handler_out", 1, true, fc_language_handler_out),
    b(2304, "internal_in", 1, false, fc_internal_in),
    b(2305, "internal_out", 1, true, fc_internal_out),
    b(2312, "anyelement_in", 1, true, fc_anyelement_in),
    b(2313, "anyelement_out", 1, true, fc_anyelement_out),
    b(2398, "shell_in", 1, false, fc_shell_in),
    b(2399, "shell_out", 1, true, fc_shell_out),
    b(2502, "anyarray_recv", 1, true, fc_anyarray_recv),
    b(2777, "anynonarray_in", 1, true, fc_anynonarray_in),
    b(2778, "anynonarray_out", 1, true, fc_anynonarray_out),
    b(3116, "fdw_handler_in", 1, false, fc_fdw_handler_in),
    b(3117, "fdw_handler_out", 1, true, fc_fdw_handler_out),
    b(3120, "void_recv", 1, true, fc_void_recv),
    b(3121, "void_send", 1, true, fc_void_send),
    b(3311, "tsm_handler_in", 1, false, fc_tsm_handler_in),
    b(3312, "tsm_handler_out", 1, true, fc_tsm_handler_out),
    b(3504, "anyenum_in", 1, true, fc_anyenum_in),
    b(3594, "event_trigger_in", 1, false, fc_event_trigger_in),
    b(3595, "event_trigger_out", 1, true, fc_event_trigger_out),
    b(3832, "anyrange_in", 3, true, fc_anyrange_in),
    b(4226, "anycompatiblemultirange_in", 3, true, fc_anycompatiblemultirange_in),
    b(4229, "anymultirange_in", 3, true, fc_anymultirange_in),
    b(5086, "anycompatible_in", 1, true, fc_anycompatible_in),
    b(5087, "anycompatible_out", 1, true, fc_anycompatible_out),
    b(5088, "anycompatiblearray_in", 1, true, fc_anycompatiblearray_in),
    b(5090, "anycompatiblearray_recv", 1, true, fc_anycompatiblearray_recv),
    b(5092, "anycompatiblenonarray_in", 1, true, fc_anycompatiblenonarray_in),
    b(5093, "anycompatiblenonarray_out", 1, true, fc_anycompatiblenonarray_out),
    b(5094, "anycompatiblerange_in", 3, true, fc_anycompatiblerange_in),
];
