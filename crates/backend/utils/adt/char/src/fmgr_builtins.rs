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
use datum::Datum;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use stringinfo::StringInfo;

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
/// A by-ref `bytea`/wire arg's verbatim image (used by `charrecv`, which builds
/// a `StringInfo` over the raw wire bytes — there is no varlena header to skip).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("char fn: by-ref arg missing from by-ref lane")
}
/// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
const VARHDRSZ: usize = 4;
/// `VARDATA_ANY` of a header-ful `text`/`bytea` arg: the payload bytes after the
/// (4-byte uncompressed) length header.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    vardata_any(arg_varlena(fcinfo, i))
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte) header, else `VARHDRSZ`. A small
/// stored value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a fixed
/// `VARHDRSZ` strip would drop three payload bytes. No-op while packing is off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
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
/// Set a varlena (`charsend`) result on the by-ref lane. The bytes are the
/// already-header-ful wire `bytea` image (`pq_endtypsend` stamps the length
/// word), carried verbatim.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}
/// Set a `text` (`char_text`) result: prepend the 4-byte varlena length header
/// to the header-less payload (`cstring_to_text`'s `SET_VARSIZE` + `memcpy`).
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("char fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind).
// ---------------------------------------------------------------------------

fn fc_charin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_char(crate::charin(arg_cstring(fcinfo, 0))))
}
fn fc_charout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let ch = arg_char(fcinfo, 0);
    let out = crate::charout(m.mcx(), ch)?.as_str().to_string();
    Ok(ret_cstring(fcinfo, out))
}
fn fc_charrecv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        return Err(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    Ok(ret_char(crate::charrecv(&mut buf)?))
}
fn fc_charsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let arg1 = arg_char(fcinfo, 0);
    let bytes = crate::charsend(m.mcx(), arg1)?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}
fn fc_chareq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::chareq(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_charne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::charne(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_charlt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::charlt(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_charle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::charle(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_chargt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::chargt(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_charge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::charge(arg_char(fcinfo, 0), arg_char(fcinfo, 1))))
}
fn fc_chartoi4(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::chartoi4(arg_char(fcinfo, 0))))
}
fn fc_i4tochar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_char(crate::i4tochar(arg_i32(fcinfo, 0))?))
}
fn fc_text_char(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_char(crate::text_char(arg_text(fcinfo, 0))))
}
fn fc_char_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: char_text wraps charout's cstring into a `text` varlena. `ret_text`
    // prepends the 4-byte VARHDRSZ length word (cstring_to_text's SET_VARSIZE +
    // memcpy), so the image crosses header-ful like every other text value.
    let m = scratch_mcx();
    let arg1 = arg_char(fcinfo, 0);
    let bytes = crate::charout(m.mcx(), arg1)?.as_str().as_bytes().to_vec();
    Ok(ret_text(fcinfo, &bytes))
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

/// Register every `char.c` builtin (C: their `fmgr_builtins[]` rows) as
/// **Result-native** (the panic→Result migration; see
/// `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// `init_seams()`. OIDs/nargs from `pg_proc.dat`; all are `proisstrict => 't'`
/// and not retset.
pub fn register_char_builtins() {
    fmgr_core::register_builtins_native([
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
