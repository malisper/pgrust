//! fmgr registration for the polymorphic varlena-array I/O functions
//! (`array_in`/`array_out`/`array_recv`/`array_send`). These are the entry
//! points the fmgr registry dispatches by OID: the input function `array_in`
//! (oid 750) is what `getTypeInputInfo` resolves for every `_T` array type, so
//! e.g. nodeAgg's `GetAggInitVal` reaches it to materialize an aggregate's
//! `agginitval` text (`{0,0}`) into a transition array.
//!
//! The element-type I/O is resolved inside the ported bodies (`io::array_in`
//! etc.) through `get_array_element_io_data` + the fmgr owner's
//! `input_function_call_safe` / `array_output_function_call` seams. Here we
//! only marshal the array value across the fmgr boundary: a `cstring`/array
//! arg on the by-reference side channel, the by-value `typioparam`/`typmod`
//! words, and the array/cstring result back on the by-reference lane.

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("arrayfuncs fmgr scratch")
}

fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("array fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_*ARRAYTYPE_P(i)` / `PG_GETARG_BYTEA_PP(i)`: the by-ref varlena
/// (array image / binary message buffer) on the by-ref lane.
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("array fn: by-ref varlena arg missing from by-ref lane")
}

fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo.arg(i).expect("array fn: missing arg").value.as_oid()
}

fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("array fn: missing arg").value.as_i32()
}

fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// `array_in(cstring, oid, int4) -> anyarray` (oid 750). arg0 is the input
/// text, arg1 the element type (`typioparam`), arg2 the typmod.
fn fc_array_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = arg_cstring(fcinfo, 0).to_string();
    let element_type = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::io::array_in(m.mcx(), &string, element_type, typmod));
    ret_varlena(fcinfo, image.as_slice().to_vec())
}

/// `array_out(anyarray) -> cstring` (oid 751).
fn fc_array_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let array = arg_varlena(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let bytes = ok(crate::io::array_out(m.mcx(), &array));
    // PG_RETURN_CSTRING produces a NUL-terminated cstring; strip the terminator
    // for the cstring lane.
    let raw = bytes.as_slice();
    let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
    ret_cstring(fcinfo, String::from_utf8_lossy(body).into_owned())
}

/// `array_recv(internal, oid, int4) -> anyarray` (oid 2400). arg0 is the binary
/// message buffer (StringInfo), arg1 the element type, arg2 the typmod.
fn fc_array_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_varlena(fcinfo, 0).to_vec();
    let spec_element_type = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::io::array_recv(m.mcx(), &buf, spec_element_type, typmod));
    ret_varlena(fcinfo, image.as_slice().to_vec())
}

/// `array_send(anyarray) -> bytea` (oid 2401).
fn fc_array_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let array = arg_varlena(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let bytes = ok(crate::io::array_send(m.mcx(), &array));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the polymorphic array I/O builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`.
pub fn register_arrayfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(750, "array_in", 3, true, false, fc_array_in),
        builtin(751, "array_out", 1, true, false, fc_array_out),
        builtin(2400, "array_recv", 3, true, false, fc_array_recv),
        builtin(2401, "array_send", 1, true, false, fc_array_send),
    ]);
}
