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
use datum::Datum;
use types_error::PgResult;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text`/`bytea`/`bpchar` arg's by-ref payload bytes (the boundary hands back
/// the verbatim on-disk varlena image; this strips its length header).
///
/// Header-form-agnostic (C `VARDATA_ANY`): a small stored value arrives with a
/// 1-byte ("short") header once `SHORT_VARLENA_PACKING` is on (the by-ref fmgr
/// boundary `detoast_ref_arg_if_toasted` normalizes only EXTERNAL/COMPRESSED, NOT
/// short), so stripping a fixed `VARHDRSZ` would drop three payload bytes from the
/// front (the `text(bpchar)` / `CAST(... AS text)` front-truncation). Skip ONE
/// byte for a short (low-bit-set, non-external) header, else the 4-byte header.
/// No-op while packing is off (every stored value is 4-byte) and correct once on.
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("oracle_compat fn: by-ref arg missing from by-ref lane");
    vardata_any(image)
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// the payload bytes past the length header, handling both the 4-byte
/// (`VARATT_IS_4B_U`) and 1-byte short (`VARATT_IS_1B`) header forms.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= datum::varlena::VARHDRSZ => {
            &image[datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
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
    // PG_RETURN_TEXT_P: prepend the 4-byte varlena header (header-ful image).
    let mut img = Vec::with_capacity(datum::varlena::VARHDRSZ + bytes.len());
    img.extend_from_slice(&datum::varlena::set_varsize_4b(
        datum::varlena::VARHDRSZ + bytes.len(),
    ));
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
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

// ---------------------------------------------------------------------------
// fc_ adapters — case folding (text, collation-aware).
// ---------------------------------------------------------------------------

fn fc_lower(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = crate::lower(m.mcx(), arg_bytes(fcinfo, 0), collid)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_upper(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = crate::upper(m.mcx(), arg_bytes(fcinfo, 0), collid)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_initcap(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = crate::initcap(m.mcx(), arg_bytes(fcinfo, 0), collid)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_casefold(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let collid = collation(fcinfo);
    let out = crate::casefold(m.mcx(), arg_bytes(fcinfo, 0), collid)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — padding (text, int4, text).
// ---------------------------------------------------------------------------

fn fc_lpad(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let len = arg_i32(fcinfo, 1);
    let out = crate::lpad(m.mcx(), arg_bytes(fcinfo, 0), len, arg_bytes(fcinfo, 2))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_rpad(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let len = arg_i32(fcinfo, 1);
    let out = crate::rpad(m.mcx(), arg_bytes(fcinfo, 0), len, arg_bytes(fcinfo, 2))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — trimming (text/bytea).
// ---------------------------------------------------------------------------

fn fc_ltrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::ltrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_rtrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::rtrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_btrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::btrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_ltrim1(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::ltrim1(m.mcx(), arg_bytes(fcinfo, 0))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_rtrim1(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::rtrim1(m.mcx(), arg_bytes(fcinfo, 0))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_btrim1(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::btrim1(m.mcx(), arg_bytes(fcinfo, 0))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

/// C: `text(bpchar)` — SQL `text(character)`, OID 401, `prosrc => rtrim1`. The
/// `bpchar` argument arrives as its detoasted `VARDATA_ANY` payload on the
/// by-ref lane (same content-bytes carrier as `text`); the value core is the
/// shared `rtrim1` (strip trailing spaces).
fn fc_text_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::rtrim1(m.mcx(), arg_bytes(fcinfo, 0))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_byteatrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::byteatrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_bytealtrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::bytealtrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_byteartrim(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::byteartrim(m.mcx(), arg_bytes(fcinfo, 0), arg_bytes(fcinfo, 1))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — translate / ascii / chr / repeat.
// ---------------------------------------------------------------------------

fn fc_translate(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::translate(
        m.mcx(),
        arg_bytes(fcinfo, 0),
        arg_bytes(fcinfo, 1),
        arg_bytes(fcinfo, 2),
    )?
    .to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_ascii(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i32(crate::ascii(arg_bytes(fcinfo, 0))?))
}

fn fc_chr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let arg = arg_i32(fcinfo, 0);
    let out = crate::chr(m.mcx(), arg)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_repeat(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let count = arg_i32(fcinfo, 1);
    let out = crate::repeat(m.mcx(), arg_bytes(fcinfo, 0), count)?.to_vec();
    Ok(ret_varlena(fcinfo, out))
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

/// Register every `oracle_compat.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; all
/// are `proisstrict => 't'` and not retset.
pub fn register_oracle_compat_builtins() {
    fmgr_core::register_builtins_native([
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
