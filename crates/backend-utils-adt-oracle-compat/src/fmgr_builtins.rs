//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! Oracle-compatible string functions from `oracle_compat.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in this crate, and writes back the
//! result. A `text` / `bytea` arg arrives as its detoasted `VARDATA_ANY`
//! payload on the by-ref lane (the boundary strips the 4-byte varlena header),
//! exactly matching this crate's cores, which take `&[u8]` content bytes and
//! return an owned `PgVec<'mcx, u8>`. The `int4` args (`lpad`/`rpad` length,
//! `chr` codepoint, `repeat` count) arrive by value on the word lane. The
//! collation for the case-folding wrappers is read from `fcinfo.fncollation`
//! (C: `PG_GET_COLLATION()`).
//!
//! [`register_oracle_compat_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs
//! / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (all are `proisstrict => 't'`, none retset).

use types_core::Oid;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text`/`bytea` arg's by-ref payload bytes (the boundary strips the varlena
/// header).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("oracle_compat fn: by-ref arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("oracle_compat fn: missing int4 arg").value.as_i32()
}

/// `PG_GET_COLLATION()`: the collation the function was invoked under.
#[inline]
fn collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}

/// Set a `text`/`bytea` (`PG_RETURN_TEXT_P`/`PG_RETURN_BYTEA_P`) result on the
/// by-ref lane and return the dummy word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("oracle_compat fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — case folding (text, collation-aware).
// ---------------------------------------------------------------------------

fn fc_lower(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = ok(crate::lower(m.mcx(), arg_bytes(fcinfo, 0), collid)).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_upper(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = ok(crate::upper(m.mcx(), arg_bytes(fcinfo, 0), collid)).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_initcap(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = ok(crate::initcap(m.mcx(), arg_bytes(fcinfo, 0), collid)).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_casefold(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = ok(crate::casefold(m.mcx(), arg_bytes(fcinfo, 0), collid)).to_vec();
    ret_varlena(fcinfo, out)
}

// ---------------------------------------------------------------------------
// fc_ adapters — padding (text, int4, text).
// ---------------------------------------------------------------------------

fn fc_lpad(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let len = arg_i32(fcinfo, 1);
    let out = ok(crate::lpad(m.mcx(), arg_bytes(fcinfo, 0), len, arg_bytes(fcinfo, 2))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_rpad(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let len = arg_i32(fcinfo, 1);
    let out = ok(crate::rpad(m.mcx(), arg_bytes(fcinfo, 0), len, arg_bytes(fcinfo, 2))).to_vec();
    ret_varlena(fcinfo, out)
}

// ---------------------------------------------------------------------------
// fc_ adapters — trimming (text/bytea).
// ---------------------------------------------------------------------------

fn fc_ltrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::ltrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_rtrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::rtrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_btrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::btrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_ltrim1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::ltrim1(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_rtrim1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::rtrim1(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_btrim1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::btrim1(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

/// C: `text(bpchar)` — SQL `text(character)`, OID 401, `prosrc => rtrim1`. The
/// `bpchar` argument arrives as its detoasted `VARDATA_ANY` payload on the
/// by-ref lane (same content-bytes carrier as `text`); the value core is the
/// shared `rtrim1` (strip trailing spaces).
fn fc_text_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::rtrim1(m.mcx(), arg_bytes(fcinfo, 0))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_byteatrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::byteatrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_bytealtrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::bytealtrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_byteartrim(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::byteartrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))).to_vec();
    ret_varlena(fcinfo, out)
}

// ---------------------------------------------------------------------------
// fc_ adapters — translate / ascii / chr / repeat.
// ---------------------------------------------------------------------------

fn fc_translate(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::translate(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        arg_bytes(fcinfo, 2),
    ))
    .to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_ascii(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok(crate::ascii(arg_bytes(fcinfo, 0))))
}

fn fc_chr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let arg = arg_i32(fcinfo, 0);
    let out = ok(crate::chr(m.mcx(), arg)).to_vec();
    ret_varlena(fcinfo, out)
}

fn fc_repeat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let count = arg_i32(fcinfo, 1);
    let out = ok(crate::repeat(m.mcx(), arg_bytes(fcinfo, 0), count)).to_vec();
    ret_varlena(fcinfo, out)
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

/// Register every `oracle_compat.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; all
/// are `proisstrict => 't'` and not retset.
pub fn register_oracle_compat_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- case folding ----
        builtin(870, "lower", 1, fc_lower),
        builtin(871, "upper", 1, fc_upper),
        builtin(872, "initcap", 1, fc_initcap),
        builtin(6412, "casefold", 1, fc_casefold),
        // ---- padding ----
        builtin(873, "lpad", 3, fc_lpad),
        builtin(874, "rpad", 3, fc_rpad),
        // ---- trimming (text) ----
        builtin(875, "ltrim", 2, fc_ltrim),
        builtin(876, "rtrim", 2, fc_rtrim),
        builtin(884, "btrim", 2, fc_btrim),
        builtin(881, "ltrim1", 1, fc_ltrim1),
        builtin(882, "rtrim1", 1, fc_rtrim1),
        builtin(885, "btrim1", 1, fc_btrim1),
        // ---- text(bpchar) cast (prosrc => rtrim1) ----
        builtin(401, "rtrim1", 1, fc_text_bpchar),
        // ---- trimming (bytea) ----
        builtin(2015, "byteatrim", 2, fc_byteatrim),
        builtin(6195, "bytealtrim", 2, fc_bytealtrim),
        builtin(6196, "byteartrim", 2, fc_byteartrim),
        // ---- translate / ascii / chr / repeat ----
        builtin(878, "translate", 3, fc_translate),
        builtin(1620, "ascii", 1, fc_ascii),
        builtin(1621, "chr", 1, fc_chr),
        builtin(1622, "repeat", 2, fc_repeat),
    ]);
}
