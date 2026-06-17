//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `char.c` functions — the single-byte `"char"` type's I/O, comparison
//! operators, and `int4`/`text` casts.
//!
//! `chareq` (oid 61) is the catalog-scankey equality operator's `oprcode` for
//! any `char`-keyed catalog column; like the other comparison `oprcode`s it must
//! be in the fmgr builtin fast-path table so `fmgr_isbuiltin` resolves it during
//! early catalog scans without recursing into the not-yet-built syscache. The
//! rest of the `char.c` family is registered alongside it for completeness (C:
//! their `fmgr_builtins[]` rows).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr call
//! frame and calls the matching value core (ported in this crate). A `"char"`
//! pass-by-value arg is the low signed byte of the arg word; a `cstring` arrives
//! on the by-ref lane; a `text` arg arrives as its detoasted `VARDATA_ANY`
//! payload (the boundary strips the varlena header). OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat` (all strict, none retset).

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_CHAR(i)`: the `"char"` value is the low signed byte of the word.
#[inline]
fn arg_char(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i8 {
    fcinfo.arg(i).expect("char fn: missing arg").value.as_i8()
}
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("char fn: missing arg").value.as_i32()
}
/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("char fn: cstring arg missing from by-ref lane")
}
/// A `text`/`bytea` arg's detoasted `VARDATA_ANY` payload (header already
/// stripped by the boundary).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("char fn: by-ref arg missing from by-ref lane")
}

#[inline]
fn ret_char(v: i8) -> Datum {
    Datum::from_i8(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
/// Set a `cstring` (`charout`) result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a varlena (`charsend`/`char_text`) result on the by-ref lane. The bytes
/// are the header-less payload (the boundary owns the `VARHDRSZ` framing).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("char fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_charin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_char(crate::charin(arg_cstring(fcinfo, 0)))
}
fn fc_charout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let ch = arg_char(fcinfo, 0);
    let out = match crate::charout(m.mcx(), ch) {
        Ok(s) => s.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, out)
}
fn fc_charrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    match crate::charrecv(&mut buf) {
        Ok(c) => ret_char(c),
        Err(e) => raise(e),
    }
}
fn fc_charsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let arg1 = arg_char(fcinfo, 0);
    let bytes = match crate::charsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}
fn fc_chareq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::chareq(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_charne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::charne(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_charlt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::charlt(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_charle(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::charle(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_chargt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::chargt(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_charge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::charge(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_chartoi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::chartoi4(arg_char(fcinfo, 0)))
}
fn fc_i4tochar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::i4tochar(arg_i32(fcinfo, 0)) {
        Ok(c) => ret_char(c),
        Err(e) => raise(e),
    }
}
fn fc_text_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_char(crate::text_char(arg_varlena(fcinfo, 0)))
}
fn fc_char_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: char_text wraps charout's cstring into a `text` varlena. The boundary
    // owns the VARHDRSZ framing, so the result payload is exactly charout's
    // bytes (byte-identical to cstring_to_text's payload, minus the header).
    let m = scratch_mcx();
    let arg1 = arg_char(fcinfo, 0);
    let bytes = match crate::charout(m.mcx(), arg1) {
        Ok(s) => s.as_str().as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
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

/// Register every `char.c` builtin (C: their `fmgr_builtins[]` rows). Called
/// from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; all are
/// `proisstrict => 't'` and not retset.
pub fn register_char_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(1245, "charin", 1, fc_charin),
        builtin(33, "charout", 1, fc_charout),
        builtin(2434, "charrecv", 1, fc_charrecv),
        builtin(2435, "charsend", 1, fc_charsend),
        // ---- comparison operators ----
        builtin(61, "chareq", 2, fc_chareq),
        builtin(70, "charne", 2, fc_charne),
        builtin(1246, "charlt", 2, fc_charlt),
        builtin(72, "charle", 2, fc_charle),
        builtin(73, "chargt", 2, fc_chargt),
        builtin(74, "charge", 2, fc_charge),
        // ---- int4 / text casts (funcName = prosrc, NOT proname) ----
        builtin(77, "chartoi4", 1, fc_chartoi4),
        builtin(78, "i4tochar", 1, fc_i4tochar),
        builtin(944, "text_char", 1, fc_text_char),
        builtin(946, "char_text", 1, fc_char_text),
    ]);
}
