//! fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `pg_lsn.c` functions. `pg_lsn` is a pass-by-value type (`XLogRecPtr` ==
//! `uint64`), so its values ride the by-value word; the arithmetic operators
//! take/return a `numeric` on the by-reference lane (the full header-ful varlena
//! image, carried verbatim — same convention as `backend-utils-adt-numeric`).
//!
//! Each `fc_<name>` adapter marshals the fmgr call frame onto the value core in
//! [`crate::pg_lsn`] and back. [`register_pg_lsn_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`). OIDs / nargs /
//! strict / retset are transcribed from `pg_proc.dat` (every row here is
//! `proisstrict => 't'`, none `proretset`).

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use crate::pg_lsn;

fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("pg_lsn fmgr scratch")
}

// --- argument readers -------------------------------------------------------

/// `PG_GETARG_LSN(i)`: the `pg_lsn`/`XLogRecPtr` word.
#[inline]
fn arg_lsn(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::XLogRecPtr {
    fcinfo.arg(i).expect("pg_lsn fn: missing arg").value.as_u64()
}
/// `PG_GETARG_INT64(i)` reinterpreted for the seeded hash's seed (`int8` word).
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("pg_lsn fn: missing arg").value.as_i64()
}
/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("pg_lsn fn: cstring arg missing from by-ref lane")
}
/// `PG_GETARG_NUMERIC(i)` / `PG_GETARG_POINTER(i)`: the full varlena byte image
/// (numeric arg / recv message buffer) on the by-ref lane, header included.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_lsn fn: by-ref varlena arg missing from by-ref lane")
}

// --- result writers ---------------------------------------------------------

#[inline]
fn ret_lsn(v: types_core::XLogRecPtr) -> Datum {
    Datum::from_u64(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_u32(v: u32) -> Datum {
    Datum::from_u64(v as u64)
}
#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `numeric` varlena image result on the by-ref lane (carried verbatim).
#[inline]
fn ret_numeric(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}
/// Set a `bytea` (`_send`) result on the by-ref lane: the bare wire payload
/// wrapped in a 4-byte varlena header (header-ful everywhere).
#[inline]
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    let total = payload.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

// --- adapters ---------------------------------------------------------------

fn fc_pg_lsn_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).to_string();
    ret_lsn(ok(pg_lsn::pg_lsn_in(&s, None)))
}
fn fc_pg_lsn_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_cstring(fcinfo, pg_lsn::pg_lsn_out(arg_lsn(fcinfo, 0)))
}
fn fc_pg_lsn_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_varlena(fcinfo, 0).to_vec();
    ret_lsn(ok(pg_lsn::pg_lsn_recv(&buf)))
}
fn fc_pg_lsn_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let v = arg_lsn(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(pg_lsn::pg_lsn_send(m.mcx(), v));
    ret_bytea(fcinfo, bytes.as_slice())
}

fn fc_pg_lsn_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_eq(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_ne(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_lt(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_gt(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_le(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(pg_lsn::pg_lsn_ge(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_lsn(pg_lsn::pg_lsn_larger(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_lsn(pg_lsn::pg_lsn_smaller(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(pg_lsn::pg_lsn_cmp(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1)))
}
fn fc_pg_lsn_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(pg_lsn::pg_lsn_hash(arg_lsn(fcinfo, 0)))
}
fn fc_pg_lsn_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(pg_lsn::pg_lsn_hash_extended(arg_lsn(fcinfo, 0), arg_i64(fcinfo, 1) as u64) as i64)
}

fn fc_pg_lsn_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_lsn(fcinfo, 0);
    let b = arg_lsn(fcinfo, 1);
    let m = scratch_mcx();
    let image = ok(pg_lsn::pg_lsn_mi(m.mcx(), a, b));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}
fn fc_pg_lsn_pli(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lsn = arg_lsn(fcinfo, 0);
    let nbytes = arg_varlena(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    ret_lsn(ok(pg_lsn::pg_lsn_pli(m.mcx(), lsn, &nbytes)))
}
fn fc_pg_lsn_mii(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lsn = arg_lsn(fcinfo, 0);
    let nbytes = arg_varlena(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    ret_lsn(ok(pg_lsn::pg_lsn_mii(m.mcx(), lsn, &nbytes)))
}
fn fc_numeric_pg_lsn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_varlena(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    ret_lsn(ok(pg_lsn::numeric_pg_lsn(m.mcx(), &num)))
}

// --- registration -----------------------------------------------------------

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

/// Register every `pg_lsn.c` builtin into the fmgr-core builtin table (C:
/// `fmgr_builtins[]`), so by-OID dispatch resolves them. Called from this
/// crate's `init_seams()`.
pub fn register_pg_lsn_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3229, "pg_lsn_in", 1, true, false, fc_pg_lsn_in),
        builtin(3230, "pg_lsn_out", 1, true, false, fc_pg_lsn_out),
        builtin(3238, "pg_lsn_recv", 1, true, false, fc_pg_lsn_recv),
        builtin(3239, "pg_lsn_send", 1, true, false, fc_pg_lsn_send),
        builtin(3231, "pg_lsn_lt", 2, true, false, fc_pg_lsn_lt),
        builtin(3232, "pg_lsn_le", 2, true, false, fc_pg_lsn_le),
        builtin(3233, "pg_lsn_eq", 2, true, false, fc_pg_lsn_eq),
        builtin(3234, "pg_lsn_ge", 2, true, false, fc_pg_lsn_ge),
        builtin(3235, "pg_lsn_gt", 2, true, false, fc_pg_lsn_gt),
        builtin(3236, "pg_lsn_ne", 2, true, false, fc_pg_lsn_ne),
        builtin(3251, "pg_lsn_cmp", 2, true, false, fc_pg_lsn_cmp),
        builtin(3252, "pg_lsn_hash", 1, true, false, fc_pg_lsn_hash),
        builtin(3413, "pg_lsn_hash_extended", 2, true, false, fc_pg_lsn_hash_extended),
        builtin(4187, "pg_lsn_larger", 2, true, false, fc_pg_lsn_larger),
        builtin(4188, "pg_lsn_smaller", 2, true, false, fc_pg_lsn_smaller),
        builtin(3237, "pg_lsn_mi", 2, true, false, fc_pg_lsn_mi),
        builtin(5022, "pg_lsn_pli", 2, true, false, fc_pg_lsn_pli),
        builtin(5024, "pg_lsn_mii", 2, true, false, fc_pg_lsn_mii),
        builtin(6103, "numeric_pg_lsn", 1, true, false, fc_numeric_pg_lsn),
    ]);
}
