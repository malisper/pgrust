//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! ranking functions of `tsrank.c`: the `ts_rank_*` and `ts_rankcd_*` families.
//!
//! The `tsvector`/`tsquery` operands and the optional `float4[]` weight array
//! are all **header-ful** varlena images (the value cores walk them with the
//! `ts_type.h` header-macros / hand them straight to `deconstruct_float4_array`),
//! so each by-ref arg crosses VERBATIM on the by-ref lane — no header strip. The
//! `method` (`normalization`) arg and the `float4` result cross by value.

use std::string::ToString;

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A header-ful varlena arg (`tsvector` / `tsquery` / `float4[]`) on the by-ref
/// lane. The value cores walk it with the `ts_type.h` header-macros, reading the
/// size word at the FIXED offset 4 and the `WordEntry`/`QueryItem` array at offset
/// 8 (and `deconstruct_array` reads the array header at offset 0), so a
/// 4-byte-header base is required. C's `PG_GETARG_TSVECTOR` / `PG_GETARG_TSQUERY`
/// / `PG_GETARG_ARRAYTYPE_P` is `PG_DETOAST_DATUM`, which un-packs a short (1-byte
/// header) stored value to 4-byte form; these operands are toastable, so under
/// `SHORT_VARLENA_PACKING` a small stored value can arrive short — un-pack before
/// the fixed-offset decode. With the flag OFF no stored value is short, so the
/// un-pack branch is never taken (behavior-preserving).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_rank fn: by-ref varlena arg missing from by-ref lane");
    unpack_short_varlena(image)
}

/// Un-pack a short (1-byte header) varlena image to the canonical 4-byte-header
/// form (`SET_VARSIZE` + payload), mirroring `detoast_attr`'s short arm. A 4-byte
/// / external / compressed image passes through verbatim. The per-fn fmgr arg
/// adapter keeps a borrow tied to `fcinfo`, so the (only-under-the-flip,
/// never-while-OFF) short case leaks a `'static` un-packed buffer — the C analogue
/// `PG_DETOAST_DATUM` palloc's into the fn context (reclaimed at reset); here
/// reclaimed at process exit. Zero leak with the flag OFF, bounded to one small
/// alloc per short arg under the flip.
#[inline]
fn unpack_short_varlena(image: &[u8]) -> &[u8] {
    const VARHDRSZ: usize = 4;
    // VARATT_IS_1B && !VARATT_IS_1B_E (a genuine short inline header).
    if image.first().is_some_and(|&b| b != 0x01 && (b & 0x01) == 0x01) {
        const VARHDRSZ_SHORT: usize = 1;
        let data_size = ((image[0] >> 1) & 0x7f) as usize - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut out = std::vec::Vec::with_capacity(new_size);
        out.extend_from_slice(&((new_size as u32) << 2).to_ne_bytes());
        out.extend_from_slice(&image[VARHDRSZ_SHORT..VARHDRSZ_SHORT + data_size]);
        std::vec::Vec::leak(out)
    } else {
        image
    }
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

// ---------------------------------------------------------------------------
// fc_ adapters — ts_rank.
// ---------------------------------------------------------------------------

fn fc_ts_rank_wttf(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let method = arg_i32(fcinfo, 3);
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rank_wttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
        method,
    )?))
}
fn fc_ts_rank_wtt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rank_wtt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
    )?))
}
fn fc_ts_rank_ttf(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let method = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rank_ttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        method,
    )?))
}
fn fc_ts_rank_tt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rank_tt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
    )?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — ts_rank_cd (cover density).
// ---------------------------------------------------------------------------

fn fc_ts_rankcd_wttf(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let method = arg_i32(fcinfo, 3);
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rankcd_wttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
        method,
    )?))
}
fn fc_ts_rankcd_wtt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rankcd_wtt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        arg_varlena(fcinfo, 2),
    )?))
}
fn fc_ts_rankcd_ttf(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let method = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rankcd_ttf(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
        method,
    )?))
}
fn fc_ts_rankcd_tt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_f32(crate::ts_rankcd_tt(
        m.mcx(),
        arg_varlena(fcinfo, 0),
        arg_varlena(fcinfo, 1),
    )?))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `ts_rank` / `ts_rank_cd` fmgr builtins. OIDs/nargs from
/// `pg_proc.dat`; every row is `proisstrict => 't'` and not retset.
pub fn register_tsrank_builtins() {
    fmgr_core::register_builtins_native([
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
