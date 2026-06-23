//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for `varchar.c`'s
//! SQL-callable `bpchar` / `varchar` functions: I/O (`*in`/`*out`/`*send`),
//! typmod-out, the `bpchar()`/`varchar()` length-coercion casts, the
//! `name`/`char`/`bpchar` conversions, the character/octet length helpers, the
//! collation-aware comparison / hashing operators, and the `bpchar`
//! `pattern_ops` ordering family.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result. A `bpchar`/`varchar` arg arrives as a header-ful varlena image on the
//! by-ref lane; `arg_text` reads its `VARDATA_ANY` payload (skips the 4-byte
//! header), exactly the carrier the value cores consume. A `name` arg arrives as
//! its fixed `NAMEDATALEN` buffer bytes (verbatim — `name` is not a varlena). A
//! `bpchar`/`varchar` result is re-framed header-ful by `ret_text` (prepend the
//! 4-byte length word); a `name` result crosses verbatim; `*send` results carry
//! the wire (already header-bearing) `bytea` image; `*out`/`typmodout` results
//! cross as a `cstring`. The collation is read from `fcinfo.fncollation`
//! (C: `PG_GET_COLLATION()`). Scalars (`int4` typmod, `bool` is_explicit, the
//! `"char"` byte, the `int8` hash seed) cross by value.
//!
//! [`register_varchar_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch (and the `fmgr_isbuiltin`
//! fast path early catalog scankeys rely on) resolves them. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat` (every row here is
//! `proisstrict => 't'` — the pg_proc default — and not retset).

use ::types_core::Oid;
use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use crate::CoerceResult;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A by-ref arg's verbatim image. Used for `name` (the fixed `NAMEDATALEN`
/// buffer, which the cores NUL-trim — `name` is not a varlena, so there is no
/// header) and for `typmodin`'s `cstring[]` array image (header-ful: the array
/// reader consumes the whole `ArrayType` header).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varchar fn: by-ref arg missing from by-ref lane")
}

/// `VARDATA_ANY` of a header-ful `bpchar`/`varchar` arg: the payload bytes after
/// the varlena length header. Header-form-agnostic — a small stored value
/// arrives with a 1-byte ("short") header (`SHORT_VARLENA_PACKING`), and
/// stripping a fixed `VARHDRSZ` off it would drop three payload bytes from the
/// front. Mirrors C `VARDATA_ANY`: skip ONE byte for a short (low-bit-set,
/// non-external) header, else the 4-byte header. (No-op today while packing is
/// off — every stored value is 4-byte — and correct once it is on.)
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    vardata_any(arg_bytes(fcinfo, i))
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("varchar fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("varchar fn: missing arg").value.as_i32()
}
/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("varchar fn: missing arg").value.as_i64()
}
/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("varchar fn: missing arg").value.as_bool()
}
/// `PG_GETARG_CHAR(i)`: the 1-byte `"char"` scalar.
#[inline]
fn arg_char(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i8 {
    fcinfo.arg(i).expect("varchar fn: missing arg").value.as_char()
}

/// `PG_GET_COLLATION()`: the collation the operator was invoked under.
#[inline]
fn collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
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
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// Set a verbatim by-ref result on the by-ref lane. Used for `name` results
/// (the fixed `NAMEDATALEN` buffer — not a varlena, no header) and `*send`
/// results (the already-header-ful wire `bytea` image). Returns the dummy word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `bpchar`/`varchar` (`text`-family) result: prepend the 4-byte varlena
/// length header to the header-less core payload (`SET_VARSIZE` + `memcpy`), so
/// the image crosses header-ful like every other varlena value.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}
/// Set a `cstring` (`*out`/`typmodout`) result on the by-ref lane. The cores
/// build a NUL-terminated cstring payload; the trailing NUL is dropped here
/// (the boundary `Cstring` carries the text without the terminator).
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: &[u8]) -> Datum {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    fcinfo.set_ref_result(RefPayload::Cstring(
        String::from_utf8_lossy(&bytes[..end]).into_owned(),
    ));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("varchar fmgr scratch")
}

/// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// fc_ adapters — bpchar I/O.
// ---------------------------------------------------------------------------

fn fc_bpcharin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: bpcharin(cstring, oid typelem, int4 atttypmod) threads `fcinfo->context`
    // so a recoverable "value too long" failure `ereturn`s into the soft sink
    // installed by `InputFunctionCallSafe` (then `PG_RETURN_NULL`). Own-copy the
    // args to release the immutable arg borrow before the mutable escontext borrow.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let typelem = fcinfo.arg(1).expect("bpcharin: missing typelem").value.as_oid();
    let atttypmod = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    let escontext = fcinfo.escontext_mut();
    // Materialize the payload out of the escontext borrow before re-borrowing
    // `fcinfo` for the result write.
    let out = crate::bpcharin(m.mcx(), &s, typelem, atttypmod, escontext)?
        .map(|img| img.to_vec());
    Ok(match out {
        Some(bytes) => ret_text(fcinfo, bytes),
        None => {
            // Soft error recorded into the frame's escontext; C `PG_RETURN_NULL`.
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

fn fc_bpcharout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::bpcharout(m.mcx(), arg_text(fcinfo, 0))?;
    Ok(ret_cstring(fcinfo, &out))
}

fn fc_bpcharsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let bytes = crate::bpcharsend(m.mcx(), arg_text(fcinfo, 0))?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_bpchartypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::bpchartypmodout(m.mcx(), arg_i32(fcinfo, 0))?;
    Ok(ret_cstring(fcinfo, &out))
}

/// `bpchartypmodin(cstring[])` — arg 0 is the typmod array's varlena image on
/// the by-reference lane.
fn fc_bpchartypmodin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::bpchartypmodin(m.mcx(), arg_bytes(fcinfo, 0))?;
    Ok(ret_i32(out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — varchar I/O.
// ---------------------------------------------------------------------------

fn fc_varcharin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: varchar_input threads `fcinfo->context` so a recoverable "value too long"
    // failure `ereturn`s into the soft sink installed by `InputFunctionCallSafe`
    // (then `PG_RETURN_NULL`). Own-copy the args before the mutable escontext borrow.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let typelem = fcinfo.arg(1).expect("varcharin: missing typelem").value.as_oid();
    let atttypmod = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    let escontext = fcinfo.escontext_mut();
    let out = crate::varcharin(m.mcx(), &s, typelem, atttypmod, escontext)?
        .map(|img| img.to_vec());
    Ok(match out {
        Some(bytes) => ret_text(fcinfo, bytes),
        None => {
            // Soft error recorded into the frame's escontext; C `PG_RETURN_NULL`.
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

fn fc_varcharout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::varcharout(m.mcx(), arg_text(fcinfo, 0))?;
    Ok(ret_cstring(fcinfo, &out))
}

fn fc_varcharsend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let bytes = crate::varcharsend(m.mcx(), arg_text(fcinfo, 0))?.as_bytes().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

/// Decode a `recv` builtin: build a `StringInfo` over a copy of the wire bytes
/// (charged to a scratch context that outlives the read) and run `decode`. The
/// internal `StringInfo` arg arrives as its raw payload image on the by-ref
/// lane. A decoder `ereport(ERROR)` travels as `Err(PgError)` back to fmgr
/// dispatch — no panic / `catch_unwind`. C: `pq_getmsgtext` consumes the buffer
/// from `cursor`.
fn fc_recv(
    fcinfo: &mut FunctionCallInfoBaseData,
    decode: impl for<'m> FnOnce(
        mcx::Mcx<'m>,
        &mut stringinfo::StringInfo<'_>,
        Oid,
        i32,
    ) -> types_error::PgResult<mcx::PgVec<'m, u8>>,
) -> types_error::PgResult<Datum> {
    // arg0 = internal (StringInfo wire bytes); arg1 = typelem oid; arg2 = atttypmod.
    let src = arg_bytes(fcinfo, 0).to_vec();
    let typelem = fcinfo.arg(1).expect("recv: missing typelem").value.as_oid();
    let atttypmod = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        return Err(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(&src);
    let mut buf = stringinfo::StringInfo::from_vec(data);
    let payload = decode(m.mcx(), &mut buf, typelem, atttypmod)?.to_vec();
    Ok(ret_text(fcinfo, payload))
}

fn fc_bpcharrecv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    fc_recv(fcinfo, crate::bpcharrecv)
}

fn fc_varcharrecv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    fc_recv(fcinfo, crate::varcharrecv)
}

fn fc_varchartypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::varchartypmodout(m.mcx(), arg_i32(fcinfo, 0))?;
    Ok(ret_cstring(fcinfo, &out))
}

/// `varchartypmodin(cstring[])` — arg 0 is the typmod array's varlena image on
/// the by-reference lane.
fn fc_varchartypmodin(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let out = crate::varchartypmodin(m.mcx(), arg_bytes(fcinfo, 0))?;
    Ok(ret_i32(out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — length-coercion casts + cross-type conversions.
// ---------------------------------------------------------------------------

fn fc_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: bpchar(bpchar source, int4 maxlen, bool isExplicit). On the "no work"
    // fast path the source value is returned unchanged.
    let maxlen = arg_i32(fcinfo, 1);
    let is_explicit = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let out = match crate::bpchar(m.mcx(), arg_text(fcinfo, 0), maxlen, is_explicit)? {
        CoerceResult::Source => arg_text(fcinfo, 0).to_vec(),
        CoerceResult::New(v) => v.to_vec(),
    };
    Ok(ret_text(fcinfo, out))
}

fn fc_varchar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let typmod = arg_i32(fcinfo, 1);
    let is_explicit = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let out = match crate::varchar(m.mcx(), arg_text(fcinfo, 0), typmod, is_explicit)? {
        CoerceResult::Source => arg_text(fcinfo, 0).to_vec(),
        CoerceResult::New(v) => v.to_vec(),
    };
    Ok(ret_text(fcinfo, out))
}

fn fc_char_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: char_bpchar("char" c) -> bpchar(1). arg0 is the 1-byte `"char"` scalar.
    let m = scratch_mcx();
    let out = crate::char_bpchar(m.mcx(), arg_char(fcinfo, 0))?.to_vec();
    Ok(ret_text(fcinfo, out))
}

fn fc_bpchar_name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: bpchar_name(bpchar) -> name. The result is the fixed NAMEDATALEN buffer,
    // crossed as its raw bytes on the by-ref lane.
    let m = scratch_mcx();
    let out = crate::bpchar_name(m.mcx(), arg_text(fcinfo, 0))?.to_vec();
    Ok(ret_varlena(fcinfo, out))
}

fn fc_name_bpchar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: name_bpchar(name) -> bpchar. arg0 is the fixed NAMEDATALEN buffer.
    let m = scratch_mcx();
    let out = crate::name_bpchar(m.mcx(), arg_bytes(fcinfo, 0))?.to_vec();
    Ok(ret_text(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — length helpers.
// ---------------------------------------------------------------------------

fn fc_bpcharlen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::bpcharlen(arg_text(fcinfo, 0))?))
}

fn fc_bpcharoctetlen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: bpcharoctetlen = toast_raw_datum_size(arg) - VARHDRSZ. `arg_text` is the
    // detoasted VARDATA payload, so the raw datum size is the payload length plus
    // the 4-byte header — yielding the payload length, exactly C's result.
    let raw_total = arg_text(fcinfo, 0).len() + VARHDRSZ;
    Ok(ret_i32(crate::bpcharoctetlen(raw_total)))
}

// ---------------------------------------------------------------------------
// fc_ adapters — collation-aware comparison + hashing.
// ---------------------------------------------------------------------------

fn fc_bpchareq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpchareq(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpcharne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpcharne(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpcharlt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpcharlt(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpcharle(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpcharle(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpchargt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpchargt(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpcharge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_bool(crate::bpcharge(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpcharcmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    Ok(ret_i32(crate::bpcharcmp(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?))
}
fn fc_bpchar_larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: PG_RETURN_BPCHAR_P((cmp >= 0) ? arg1 : arg2). The core returns true to
    // pick arg1, false to pick arg2; this returns the chosen value's bytes.
    let c = collation(fcinfo);
    let pick_first = crate::bpchar_larger(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?;
    // C returns the chosen input datum unchanged: carry its full header-ful image
    // verbatim (`arg_bytes`), not a re-framed payload.
    let out = if pick_first { arg_bytes(fcinfo, 0) } else { arg_bytes(fcinfo, 1) }.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_bpchar_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    let pick_first = crate::bpchar_smaller(arg_text(fcinfo, 0), arg_text(fcinfo, 1), c)?;
    let out = if pick_first { arg_bytes(fcinfo, 0) } else { arg_bytes(fcinfo, 1) }.to_vec();
    Ok(ret_varlena(fcinfo, out))
}
fn fc_hashbpchar(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    let m = scratch_mcx();
    // C: hashbpchar -> Datum (uint32 reinterpreted). PG_RETURN_INT32 of the hash.
    Ok(ret_i32(crate::hashbpchar(m.mcx(), arg_text(fcinfo, 0), c)? as i32))
}
fn fc_hashbpcharextended(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let c = collation(fcinfo);
    // C: PG_GETARG_INT64(1) is the seed.
    let seed = arg_i64(fcinfo, 1) as u64;
    let m = scratch_mcx();
    Ok(ret_i64(crate::hashbpcharextended(m.mcx(), arg_text(fcinfo, 0), c, seed)? as i64))
}

// ---------------------------------------------------------------------------
// fc_ adapters — bpchar pattern-ops ordering family.
// ---------------------------------------------------------------------------

fn fc_bpchar_pattern_lt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::bpchar_pattern_lt(arg_text(fcinfo, 0), arg_text(fcinfo, 1))))
}
fn fc_bpchar_pattern_le(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::bpchar_pattern_le(arg_text(fcinfo, 0), arg_text(fcinfo, 1))))
}
fn fc_bpchar_pattern_ge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::bpchar_pattern_ge(arg_text(fcinfo, 0), arg_text(fcinfo, 1))))
}
fn fc_bpchar_pattern_gt(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::bpchar_pattern_gt(arg_text(fcinfo, 0), arg_text(fcinfo, 1))))
}
fn fc_btbpchar_pattern_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_i32(crate::btbpchar_pattern_cmp(arg_text(fcinfo, 0), arg_text(fcinfo, 1))))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// Build one Result-native builtin row (`func: None`; dispatch goes through the
/// native overlay) paired with its [`PgFnNative`] body.
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

/// Register every `varchar.c` fmgr builtin whose value core is ported and whose
/// arg/result types are expressible at the fmgr boundary (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs/nargs
/// from `pg_proc.dat`; every row here is `proisstrict => 't'` (the default) and
/// not retset.
pub fn register_varchar_builtins() {
    fmgr_core::register_builtins_native([
        // ---- conversions ----
        builtin(408, "name_bpchar", 1, fc_name_bpchar),
        builtin(409, "bpchar_name", 1, fc_bpchar_name),
        builtin(860, "char_bpchar", 1, fc_char_bpchar),
        // ---- length-coercion casts ----
        builtin(668, "bpchar", 3, fc_bpchar),
        builtin(669, "varchar", 3, fc_varchar),
        // ---- I/O ----
        builtin(1044, "bpcharin", 3, fc_bpcharin),
        builtin(1045, "bpcharout", 1, fc_bpcharout),
        builtin(1046, "varcharin", 3, fc_varcharin),
        builtin(1047, "varcharout", 1, fc_varcharout),
        builtin(2430, "bpcharrecv", 3, fc_bpcharrecv),
        builtin(2431, "bpcharsend", 1, fc_bpcharsend),
        builtin(2432, "varcharrecv", 3, fc_varcharrecv),
        builtin(2433, "varcharsend", 1, fc_varcharsend),
        builtin(2913, "bpchartypmodin", 1, fc_bpchartypmodin),
        builtin(2914, "bpchartypmodout", 1, fc_bpchartypmodout),
        builtin(2915, "varchartypmodin", 1, fc_varchartypmodin),
        builtin(2916, "varchartypmodout", 1, fc_varchartypmodout),
        // ---- comparison operators ----
        builtin(1048, "bpchareq", 2, fc_bpchareq),
        builtin(1049, "bpcharlt", 2, fc_bpcharlt),
        builtin(1050, "bpcharle", 2, fc_bpcharle),
        builtin(1051, "bpchargt", 2, fc_bpchargt),
        builtin(1052, "bpcharge", 2, fc_bpcharge),
        builtin(1053, "bpcharne", 2, fc_bpcharne),
        builtin(1063, "bpchar_larger", 2, fc_bpchar_larger),
        builtin(1064, "bpchar_smaller", 2, fc_bpchar_smaller),
        builtin(1078, "bpcharcmp", 2, fc_bpcharcmp),
        // ---- hashing ----
        builtin(1080, "hashbpchar", 1, fc_hashbpchar),
        builtin(972, "hashbpcharextended", 2, fc_hashbpcharextended),
        // ---- length helpers ----
        builtin(1318, "bpcharlen", 1, fc_bpcharlen),
        builtin(1367, "bpcharlen", 1, fc_bpcharlen),
        builtin(1372, "bpcharlen", 1, fc_bpcharlen),
        builtin(1375, "bpcharoctetlen", 1, fc_bpcharoctetlen),
        // ---- bpchar pattern_ops ordering ----
        builtin(2174, "bpchar_pattern_lt", 2, fc_bpchar_pattern_lt),
        builtin(2175, "bpchar_pattern_le", 2, fc_bpchar_pattern_le),
        builtin(2177, "bpchar_pattern_ge", 2, fc_bpchar_pattern_ge),
        builtin(2178, "bpchar_pattern_gt", 2, fc_bpchar_pattern_gt),
        builtin(2180, "btbpchar_pattern_cmp", 2, fc_btbpchar_pattern_cmp),
    ]);
}
