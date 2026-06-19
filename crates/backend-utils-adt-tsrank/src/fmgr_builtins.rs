//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! ranking functions of `tsrank.c`: the `ts_rank_*` and `ts_rankcd_*` families.
//!
//! The `tsvector`/`tsquery` operands and the optional `float4[]` weight array
//! are all **header-ful** varlena images (the value cores walk them with the
//! `ts_type.h` header-macros / hand them straight to `deconstruct_float4_array`),
//! so each by-ref arg crosses VERBATIM on the by-ref lane — no header strip. The
//! `method` (`normalization`) arg and the `float4` result cross by value.

use std::string::ToString;

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A header-ful varlena arg (`tsvector` / `tsquery` / `float4[]`) on the by-ref
/// lane, read verbatim.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_rank fn: by-ref varlena arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: the `normalization` method.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("ts_rank fn: missing int4 arg").value.as_i32()
}

#[inline]
fn ret_f32(v: f32) -> Datum {
    Datum::from_f32(v)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("ts_rank fmgr scratch")
}

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

// ---------------------------------------------------------------------------
// fc_ adapters — ts_rank.
// ---------------------------------------------------------------------------

fn fc_ts_rank_wttf(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let method = arg_i32(fcinfo, 3);
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rank_wttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
        method,
    )))
}
fn fc_ts_rank_wtt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rank_wtt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
    )))
}
fn fc_ts_rank_ttf(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let method = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rank_ttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        method,
    )))
}
fn fc_ts_rank_tt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rank_tt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
    )))
}

// ---------------------------------------------------------------------------
// fc_ adapters — ts_rank_cd (cover density).
// ---------------------------------------------------------------------------

fn fc_ts_rankcd_wttf(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let method = arg_i32(fcinfo, 3);
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rankcd_wttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
        method,
    )))
}
fn fc_ts_rankcd_wtt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rankcd_wtt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
    )))
}
fn fc_ts_rankcd_ttf(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let method = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rankcd_ttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        method,
    )))
}
fn fc_ts_rankcd_tt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_f32(ok(crate::ts_rankcd_tt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
    )))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register the `ts_rank` / `ts_rank_cd` fmgr builtins. OIDs/nargs from
/// `pg_proc.dat`; every row is `proisstrict => 't'` and not retset.
pub fn register_tsrank_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3703, "ts_rank_wttf", 4, fc_ts_rank_wttf),
        builtin(3704, "ts_rank_wtt", 3, fc_ts_rank_wtt),
        builtin(3705, "ts_rank_ttf", 3, fc_ts_rank_ttf),
        builtin(3706, "ts_rank_tt", 2, fc_ts_rank_tt),
        builtin(3707, "ts_rankcd_wttf", 4, fc_ts_rankcd_wttf),
        builtin(3708, "ts_rankcd_wtt", 3, fc_ts_rankcd_wtt),
        builtin(3709, "ts_rankcd_ttf", 3, fc_ts_rankcd_ttf),
        builtin(3710, "ts_rankcd_tt", 2, fc_ts_rankcd_tt),
    ]);
}
