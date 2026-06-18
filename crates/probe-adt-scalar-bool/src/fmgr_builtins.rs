//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `bool.c` whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `boolean` I/O, comparison operators, hash
//! functions, the `bool => text` cast, and the `bool_and`/`bool_or` aggregate
//! transition functions).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_probe_adt_scalar_bool_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (all are `proisstrict => 't'`, none retset).
//!
//! Not registered here: the moving-aggregate inverse / final functions
//! (`bool_accum`, `bool_accum_inv`, `bool_alltrue`, `bool_anytrue`) take/return
//! the `internal` `BoolAggState` pointer, which is not expressible at the fmgr
//! boundary here.

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: the low bit of arg `i`'s word.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("bool fn: missing arg").value.as_bool()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: the full word as a signed 64-bit int
/// (the `hashboolextended` seed).
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("bool fn: missing arg").value.as_i64()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("bool fn: cstring arg missing from by-ref lane")
}

/// A `bytea` / serialized arg's `VARDATA_ANY` payload (header already stripped
/// by the boundary): the wire bytes a `recv` function reads off the
/// `StringInfo`.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("bool fn: by-ref arg missing from by-ref lane");
    // `VARDATA_ANY`: skip the 4-byte header on the header-ful image.
    if image.len() >= 4 {
        &image[4..]
    } else {
        &[]
    }
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `cstring` (`boolout`) result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: alloc::string::String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set a varlena (`boolsend`/`text`) result on the by-ref lane. The bytes are
/// the header-less payload (the boundary owns the `VARHDRSZ` framing).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: alloc::vec::Vec<u8>) -> Datum {
    // `palloc(VARHDRSZ + len)` + `SET_VARSIZE`: build the header-ful image.
    let total = bytes.len() + 4;
    let mut img = alloc::vec::Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("bool fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_boolin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    // C: boolin(in_str, fcinfo->context). fcinfo->context carries the soft
    // ErrorSaveContext; at this boundary a hard parse is used (a soft context is
    // not modeled on the fmgr frame), matching every other adt _in.
    match crate::boolin(s, None) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

fn fc_boolout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_bool(fcinfo, 0);
    // C: boolout palloc's a 2-byte cstring ("t"/"f"). The owned core returns the
    // static spelling; ret_cstring copies it onto the by-ref lane.
    ret_cstring(fcinfo, crate::boolout(b).into())
}

fn fc_boolrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: boolrecv reads one byte off the StringInfo and returns `ext != 0`. The
    // wire payload arrives on the by-ref lane (header already stripped); copy it
    // into a scratch StringInfo so pq_getmsgbyte can consume it, mirroring
    // charrecv.
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    match crate::boolrecv(&mut buf) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

fn fc_boolsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_bool(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::boolsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

fn fc_booltext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: booltext returns the SQL-spec spelling "true"/"false" wrapped in a
    // `text` varlena (`cstring_to_text`). The boundary owns the VARHDRSZ framing,
    // so the result payload is exactly those bytes (byte-identical to
    // cstring_to_text's payload, minus the header) — same pattern as char_text.
    let arg1 = arg_bool(fcinfo, 0);
    let s = if arg1 { "true" } else { "false" };
    ret_varlena(fcinfo, s.as_bytes().to_vec())
}

fn fc_booleq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::booleq(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}
fn fc_boolne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boolne(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}
fn fc_boollt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boollt(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}
fn fc_boolgt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boolgt(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}
fn fc_boolle(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boolle(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}
fn fc_boolge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boolge(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}

fn fc_booland_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::booland_statefunc(
        arg_bool(fcinfo, 0),
        arg_bool(fcinfo, 1),
    ))
}
fn fc_boolor_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::boolor_statefunc(
        arg_bool(fcinfo, 0),
        arg_bool(fcinfo, 1),
    ))
}

fn fc_hashbool(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: hashbool returns int4; the core already returns the result `Datum`
    // (UInt32GetDatum(hash_bytes_uint32(...))).
    crate::hashbool(arg_bool(fcinfo, 0))
}
fn fc_hashboolextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: hashboolextended returns int8; the core already returns the result
    // `Datum` (UInt64GetDatum(hash_bytes_uint32_extended(..., seed))).
    crate::hashboolextended(arg_bool(fcinfo, 0), arg_i64(fcinfo, 1))
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
        name: name.into(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every `bool.c` builtin expressible at the fmgr boundary (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs/nargs
/// from `pg_proc.dat`; all are `proisstrict => 't'` and not retset.
pub fn register_probe_adt_scalar_bool_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O + cast ----
        builtin(1242, "boolin", 1, fc_boolin),
        builtin(1243, "boolout", 1, fc_boolout),
        builtin(2436, "boolrecv", 1, fc_boolrecv),
        builtin(2437, "boolsend", 1, fc_boolsend),
        builtin(2971, "booltext", 1, fc_booltext),
        // ---- comparison operators ----
        builtin(60, "booleq", 2, fc_booleq),
        builtin(84, "boolne", 2, fc_boolne),
        builtin(56, "boollt", 2, fc_boollt),
        builtin(57, "boolgt", 2, fc_boolgt),
        builtin(1691, "boolle", 2, fc_boolle),
        builtin(1692, "boolge", 2, fc_boolge),
        // ---- aggregate transition functions ----
        builtin(2515, "booland_statefunc", 2, fc_booland_statefunc),
        builtin(2516, "boolor_statefunc", 2, fc_boolor_statefunc),
        // ---- hash functions ----
        builtin(6417, "hashbool", 1, fc_hashbool),
        builtin(6418, "hashboolextended", 2, fc_hashboolextended),
    ]);
}
