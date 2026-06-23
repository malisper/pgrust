//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `varbit.c`
//! functions whose argument/result types are expressible at the current fmgr
//! boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_varbit_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and
//! the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict / retset
//! are transcribed exactly from `pg_proc.dat`.
//!
//! # The `varbit` varlena image on the by-ref lane
//!
//! A `bit`/`varbit` value is a varlena struct (`utils/varbit.h`):
//! `[ 4-byte varlena length word | int32 bit_len | bit_dat[] ]`, where the data
//! section is exactly `ceil(bit_len / 8)` bytes (last byte zero-padded). The
//! crate's value cores ([`crate::VarBit`] / [`crate::VarBitRef`]) work on the
//! header-STRIPPED `{ bit_len, data }` form (see the crate-level carrier doc).
//!
//! Per the by-ref fmgr convention the canonical `ByRef` image for a disk-stored
//! type is the FULL varlena. So these wrappers carry, on the by-ref Varlena
//! lane, the full varlena image `[varsize_le | bit_len_le | data]`: [`decode_varbit`]
//! parses an argument back into a [`crate::VarBitRef`], and [`encode_varbit`]
//! serialises a result [`crate::VarBit`] symmetrically. The length word and
//! `bit_len` are little-endian (the in-memory layout the C struct exposes on a
//! little-endian host; the image never touches disk here, only the in-process
//! by-ref lane, so any self-consistent encoding round-trips).

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_stringinfo::StringInfo;

use crate::{VarBit, VarBitRef};

/// `VARHDRSZ` + `VARBITHDRSZ` (varbit.h): the 4-byte varlena length word plus
/// the 4-byte `int32 bit_len`, before the `bit_dat[]` payload.
const VARBIT_PREFIX: usize = 8;

/// `VARHDRSZ` (varatt.h): the 4-byte varlena length word.
const VARHDRSZ_4B: usize = 4;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, as a signed int4.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("varbit fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`: arg `i`'s word as a signed int8.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("varbit fn: missing arg").value.as_i64()
}

/// `PG_GETARG_BOOL(i)`: arg `i`'s word as a bool.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("varbit fn: missing arg").value.as_bool()
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("varbit fn: cstring arg missing from by-ref lane")
}

/// The full varlena image of arg `i` off the by-ref lane, normalized to a
/// 4-byte-header (`VARBIT_PREFIX`-based) form that [`decode_varbit`] can read.
///
/// This lane carries images from two source conventions:
///  - a `heap_deform_tuple` stored value — a GENUINE varlena (4-byte header when
///    `SHORT_VARLENA_PACKING` is OFF; a 1-byte SHORT header when ON, since `bit`
///    /`varbit` columns are packable). The C `DatumGetVarBitP` macro is
///    `PG_DETOAST_DATUM`, which un-packs a short header to 4-byte form before any
///    fixed-offset struct read.
///  - a parser/coercion-built image (e.g. `bit_in_to_varlena`) whose leading
///    4-byte word is the RAW image length written UNSHIFTED (not `SET_VARSIZE`),
///    and which is NEVER short-packed.
///
/// The earlier bare `(image[0] & 0x01) == 1` probe mis-fired on the second source
/// (an odd raw length sets the low bit) and truncated the value. The fix is the
/// strict `VARSIZE_1B == len` self-consistency test (the same disambiguation
/// `ensure_headerful_varlena` uses): a genuine short varlena has
/// `(image[0] >> 1) == image.len()`, which an unshifted-length word never
/// satisfies (its low byte is `len & 0xFF`, so `>>1` ≠ `len`). Only a genuine
/// short value is un-packed to the 4-byte `[VARSIZE_4B | bit_len | data]` form;
/// every other image passes through verbatim. With `SHORT_VARLENA_PACKING` OFF no
/// stored value is short-packed, so the un-pack branch is dead and this is a
/// behavior-preserving copy.
fn arg_varbit_bytes(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varbit fn: varbit arg missing from by-ref lane");
    // VARATT_IS_SHORT && VARSIZE_1B == len: a genuine 1-byte-header short varlena
    // (low bit set, and the encoded short length matches the total image length).
    if !image.is_empty() && (image[0] & 0x01) == 0x01 && ((image[0] >> 1) as usize) == image.len() {
        // Un-pack short -> 4-byte header (mirror detoast_attr's short arm): the
        // short payload is the on-disk struct body `[bit_len | data]`; prepend a
        // 4-byte length word so the fixed-offset readers (bit_len at [4..8], data
        // at VARBIT_PREFIX) land correctly.
        let payload = &image[1..];
        let total = VARHDRSZ_4B + payload.len();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
        out.extend_from_slice(payload);
        out
    } else {
        image.to_vec()
    }
}

/// Parse a full header-ful `varbit` varlena image
/// `[VARSIZE_4B | bit_len_ne | bit_dat]` into a borrowed [`VarBitRef`]. The
/// 4-byte varlena length word (`SET_VARSIZE_4B`) is skipped; `bit_len` is the
/// native-order `int32` field of the C `VarBit` struct. Mirror of `encode_varbit`.
#[inline]
fn decode_varbit(image: &[u8]) -> VarBitRef<'_> {
    assert!(
        image.len() >= VARBIT_PREFIX,
        "varbit fn: by-ref image shorter than the varlena prefix"
    );
    let bit_len = i32::from_ne_bytes([image[4], image[5], image[6], image[7]]);
    VarBitRef::new(bit_len, &image[VARBIT_PREFIX..])
}

/// Serialise an owned [`VarBit`] result into a full header-ful varlena image
/// `[VARSIZE_4B | bit_len_ne | bit_dat]`: the leading length word is the real
/// `SET_VARSIZE_4B(total)` encoding (so cross-tree VARSIZE readers interpret it
/// correctly). Mirror of `decode_varbit`.
#[inline]
fn encode_varbit(v: &VarBit<'_>) -> Vec<u8> {
    let total = VARBIT_PREFIX + v.data.len();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    out.extend_from_slice(&v.bit_len.to_ne_bytes());
    out.extend_from_slice(&v.data);
    out
}

/// Set a `bit`/`varbit` (by-reference) result on the by-ref lane as the full
/// varlena image, and return the dummy by-value word.
#[inline]
fn ret_varbit(fcinfo: &mut FunctionCallInfoBaseData, v: &VarBit<'_>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(encode_varbit(v)));
    Datum::from_usize(0)
}

/// Set a `bytea` (`_send`) result on the by-ref lane (full image) and return the
/// dummy word.
#[inline]
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
/// The core returns a NUL-terminated `cstring` image (`PgVec<u8>`); the by-ref
/// `Cstring` lane carries owned text without the trailing NUL.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: &[u8]) -> Datum {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = String::from_utf8_lossy(&bytes[..end]).into_owned();
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("varbit fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters — I/O. (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind.)
// ---------------------------------------------------------------------------

fn fc_bit_in(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // args: cstring, oid (typioparam, unused), int4 typmod.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    // Forward the soft ErrorSaveContext installed on the frame by
    // InputFunctionCallSafe so a recoverable parse failure `ereturn`s into the
    // sink (returning `Ok(None)`) instead of throwing past `invoke?`.
    let escontext = fcinfo.escontext_mut();
    let parsed = crate::bit_in(m.mcx(), &s, typmod, escontext)?;
    match parsed {
        Some(v) => Ok(ret_varbit(fcinfo, &v)),
        // Soft-error path: escontext recorded the failure; the caller discards
        // this placeholder after `soft_error_occurred()`.
        None => Ok(Datum::null()),
    }
}

fn fc_varbit_in(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    // Forward the soft ErrorSaveContext (see fc_bit_in).
    let escontext = fcinfo.escontext_mut();
    let parsed = crate::varbit_in(m.mcx(), &s, typmod, escontext)?;
    match parsed {
        Some(v) => Ok(ret_varbit(fcinfo, &v)),
        None => Ok(Datum::null()),
    }
}

fn fc_bit_out(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let arg = decode_varbit(&image);
    let m = scratch_mcx();
    let bytes = crate::bit_out(m.mcx(), arg)?.to_vec();
    Ok(ret_cstring(fcinfo, &bytes))
}

fn fc_varbit_out(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let arg = decode_varbit(&image);
    let m = scratch_mcx();
    let bytes = crate::varbit_out(m.mcx(), arg)?.to_vec();
    Ok(ret_cstring(fcinfo, &bytes))
}

fn fc_bit_send(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let arg = decode_varbit(&image);
    let m = scratch_mcx();
    let bytes = crate::bit_send(m.mcx(), arg)?.as_bytes().to_vec();
    Ok(ret_bytea(fcinfo, bytes))
}

fn fc_varbit_send(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let arg = decode_varbit(&image);
    let m = scratch_mcx();
    let bytes = crate::varbit_send(m.mcx(), arg)?.as_bytes().to_vec();
    Ok(ret_bytea(fcinfo, bytes))
}

/// The full `cstring[]` `ArrayType` varlena image of arg `i` off the by-ref
/// lane (the typmodin seam puts the array there as `RefPayload::Varlena`).
#[inline]
fn arg_array_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("varbit fn: cstring[] arg missing from by-ref lane")
}

fn fc_bittypmodin(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_array_bytes(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    Ok(Datum::from_i32(crate::bittypmodin(m.mcx(), &image)?))
}

fn fc_varbittypmodin(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_array_bytes(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    Ok(Datum::from_i32(crate::varbittypmodin(m.mcx(), &image)?))
}

fn fc_bittypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::bittypmodout(m.mcx(), typmod)?.to_vec();
    Ok(ret_cstring(fcinfo, &bytes))
}

fn fc_varbittypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::varbittypmodout(m.mcx(), typmod)?.to_vec();
    Ok(ret_cstring(fcinfo, &bytes))
}

// ---------------------------------------------------------------------------
// fc_ adapters — comparison (bit,bit -> bool / int4). Shared by the `varbit`
// aliases, whose prosrc is the same C function.
// ---------------------------------------------------------------------------

/// Decode both `varbit` args off the by-ref lane.
macro_rules! decode_both {
    ($fcinfo:ident) => {{
        let img1 = arg_varbit_bytes($fcinfo, 0);
        let img2 = arg_varbit_bytes($fcinfo, 1);
        (img1, img2)
    }};
}

fn fc_biteq(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::biteq(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitne(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::bitne(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitlt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::bitlt(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitle(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::bitle(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitgt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::bitgt(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitge(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_bool(crate::bitge(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitcmp(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_i32(crate::bitcmp(decode_varbit(&a), decode_varbit(&b))))
}

// ---------------------------------------------------------------------------
// fc_ adapters — bitwise logical / shift / concat (-> bit/varbit).
// ---------------------------------------------------------------------------

fn fc_bitand(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    let m = scratch_mcx();
    let v = crate::bit_and(m.mcx(), decode_varbit(&a), decode_varbit(&b))?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitor(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    let m = scratch_mcx();
    let v = crate::bit_or(m.mcx(), decode_varbit(&a), decode_varbit(&b))?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitxor(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    let m = scratch_mcx();
    let v = crate::bitxor(m.mcx(), decode_varbit(&a), decode_varbit(&b))?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitnot(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let m = scratch_mcx();
    let v = crate::bitnot(m.mcx(), decode_varbit(&image))?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitshiftleft(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let shft = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let v = crate::bitshiftleft(m.mcx(), decode_varbit(&image), shft)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitshiftright(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let shft = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let v = crate::bitshiftright(m.mcx(), decode_varbit(&image), shft)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitcat(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    let m = scratch_mcx();
    let v = crate::bitcat(m.mcx(), decode_varbit(&a), decode_varbit(&b))?;
    Ok(ret_varbit(fcinfo, &v))
}

// ---------------------------------------------------------------------------
// fc_ adapters — recv (StringInfo).
// ---------------------------------------------------------------------------

fn fc_bit_recv(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // args: internal (StringInfo), oid (typioparam, unused), int4 typmod.
    let src = arg_varbit_bytes(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    data.extend_from_slice(&src);
    let mut buf = StringInfo::from_vec(data);
    let v = crate::bit_recv(m.mcx(), &mut buf, typmod)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_varbit_recv(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let src = arg_varbit_bytes(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    data.extend_from_slice(&src);
    let mut buf = StringInfo::from_vec(data);
    let v = crate::varbit_recv(m.mcx(), &mut buf, typmod)?;
    Ok(ret_varbit(fcinfo, &v))
}

// ---------------------------------------------------------------------------
// fc_ adapters — length coercion (bit/varbit) — (varbit, int4 len, bool explicit).
// ---------------------------------------------------------------------------

fn fc_bit(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let len = arg_int32(fcinfo, 1);
    let is_explicit = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let v = crate::bit(m.mcx(), decode_varbit(&image), len, is_explicit)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_varbit(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let len = arg_int32(fcinfo, 1);
    let is_explicit = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let v = crate::varbit(m.mcx(), decode_varbit(&image), len, is_explicit)?;
    Ok(ret_varbit(fcinfo, &v))
}

// ---------------------------------------------------------------------------
// fc_ adapters — int4/int8 conversion (by-value <-> varbit).
// ---------------------------------------------------------------------------

fn fc_bitfromint4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_int32(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let v = crate::bitfromint4(m.mcx(), a, typmod)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bittoint4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    Ok(Datum::from_i32(crate::bittoint4(decode_varbit(&image))?))
}

fn fc_bitfromint8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_int64(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let v = crate::bitfromint8(m.mcx(), a, typmod)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bittoint8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    Ok(Datum::from_i64(crate::bittoint8(decode_varbit(&image))?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — bit get/set, popcount, length, position, substr, overlay.
// ---------------------------------------------------------------------------

fn fc_bitgetbit(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let n = arg_int32(fcinfo, 1);
    Ok(Datum::from_i32(crate::bitgetbit(decode_varbit(&image), n)?))
}

fn fc_bitsetbit(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let n = arg_int32(fcinfo, 1);
    let new_bit = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let v = crate::bitsetbit(m.mcx(), decode_varbit(&image), n, new_bit)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bit_bit_count(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    Ok(Datum::from_i64(crate::bit_bit_count(decode_varbit(&image))))
}

fn fc_bitlength(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    Ok(Datum::from_i32(crate::bitlength(decode_varbit(&image))))
}

fn fc_bitoctetlength(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    Ok(Datum::from_i32(crate::bitoctetlength(decode_varbit(&image))))
}

fn fc_bitposition(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let (a, b) = decode_both!(fcinfo);
    Ok(Datum::from_i32(crate::bitposition(decode_varbit(&a), decode_varbit(&b))))
}

fn fc_bitsubstr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let s = arg_int32(fcinfo, 1);
    let l = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let v = crate::bitsubstr(m.mcx(), decode_varbit(&image), s, l)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitsubstr_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = arg_varbit_bytes(fcinfo, 0);
    let s = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let v = crate::bitsubstr_no_len(m.mcx(), decode_varbit(&image), s)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitoverlay(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let t1 = arg_varbit_bytes(fcinfo, 0);
    let t2 = arg_varbit_bytes(fcinfo, 1);
    let sp = arg_int32(fcinfo, 2);
    let sl = arg_int32(fcinfo, 3);
    let m = scratch_mcx();
    let v = crate::bitoverlay(m.mcx(), decode_varbit(&t1), decode_varbit(&t2), sp, sl)?;
    Ok(ret_varbit(fcinfo, &v))
}

fn fc_bitoverlay_no_len(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let t1 = arg_varbit_bytes(fcinfo, 0);
    let t2 = arg_varbit_bytes(fcinfo, 1);
    let sp = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let v = crate::bitoverlay_no_len(m.mcx(), decode_varbit(&t1), decode_varbit(&t2), sp)?;
    Ok(ret_varbit(fcinfo, &v))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the expressible `varbit.c` fmgr builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset are
/// transcribed exactly from `pg_proc.dat`. All of these are `proisstrict => 't'`
/// (the default) and `proretset => 'f'`.
///
/// `bit_recv`/`varbit_recv` read their `internal` `StringInfo *` argument off
/// the by-ref Varlena lane (the wire bytes), mirroring `fc_charrecv`.
///
/// `bittypmodin`/`varbittypmodin` take a `cstring[]` `ArrayType`, which the
/// typmodin seam delivers as a full array varlena image on the by-ref Varlena
/// lane (`arg_array_bytes`).
pub fn register_varbit_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // I/O
        builtin(1564, "bit_in", 3, true, false, fc_bit_in),
        builtin(1565, "bit_out", 1, true, false, fc_bit_out),
        builtin(1579, "varbit_in", 3, true, false, fc_varbit_in),
        builtin(1580, "varbit_out", 1, true, false, fc_varbit_out),
        builtin(2457, "bit_send", 1, true, false, fc_bit_send),
        builtin(2459, "varbit_send", 1, true, false, fc_varbit_send),
        builtin(2919, "bittypmodin", 1, true, false, fc_bittypmodin),
        builtin(2902, "varbittypmodin", 1, true, false, fc_varbittypmodin),
        builtin(2920, "bittypmodout", 1, true, false, fc_bittypmodout),
        builtin(2921, "varbittypmodout", 1, true, false, fc_varbittypmodout),
        // comparison (bit,bit)
        builtin(1581, "biteq", 2, true, false, fc_biteq),
        builtin(1582, "bitne", 2, true, false, fc_bitne),
        builtin(1595, "bitlt", 2, true, false, fc_bitlt),
        builtin(1594, "bitle", 2, true, false, fc_bitle),
        builtin(1593, "bitgt", 2, true, false, fc_bitgt),
        builtin(1592, "bitge", 2, true, false, fc_bitge),
        builtin(1596, "bitcmp", 2, true, false, fc_bitcmp),
        // comparison (varbit,varbit) — same C prosrc as the bit variants
        builtin(1666, "biteq", 2, true, false, fc_biteq),
        builtin(1667, "bitne", 2, true, false, fc_bitne),
        builtin(1671, "bitlt", 2, true, false, fc_bitlt),
        builtin(1670, "bitle", 2, true, false, fc_bitle),
        builtin(1669, "bitgt", 2, true, false, fc_bitgt),
        builtin(1668, "bitge", 2, true, false, fc_bitge),
        builtin(1672, "bitcmp", 2, true, false, fc_bitcmp),
        // bitwise logical / shift / concat
        builtin(1673, "bit_and", 2, true, false, fc_bitand),
        builtin(1674, "bit_or", 2, true, false, fc_bitor),
        builtin(1675, "bitxor", 2, true, false, fc_bitxor),
        builtin(1676, "bitnot", 1, true, false, fc_bitnot),
        builtin(1677, "bitshiftleft", 2, true, false, fc_bitshiftleft),
        builtin(1678, "bitshiftright", 2, true, false, fc_bitshiftright),
        builtin(1679, "bitcat", 2, true, false, fc_bitcat),
        // recv (StringInfo)
        builtin(2456, "bit_recv", 3, true, false, fc_bit_recv),
        builtin(2458, "varbit_recv", 3, true, false, fc_varbit_recv),
        // length coercion
        builtin(1685, "bit", 3, true, false, fc_bit),
        builtin(1687, "varbit", 3, true, false, fc_varbit),
        // int4/int8 conversion
        builtin(1683, "bitfromint4", 2, true, false, fc_bitfromint4),
        builtin(1684, "bittoint4", 1, true, false, fc_bittoint4),
        builtin(2075, "bitfromint8", 2, true, false, fc_bitfromint8),
        builtin(2076, "bittoint8", 1, true, false, fc_bittoint8),
        // get/set bit, popcount, length, position, substr, overlay
        builtin(3032, "bitgetbit", 2, true, false, fc_bitgetbit),
        builtin(3033, "bitsetbit", 3, true, false, fc_bitsetbit),
        builtin(6162, "bit_bit_count", 1, true, false, fc_bit_bit_count),
        builtin(1681, "bitlength", 1, true, false, fc_bitlength),
        builtin(1682, "bitoctetlength", 1, true, false, fc_bitoctetlength),
        builtin(1698, "bitposition", 2, true, false, fc_bitposition),
        builtin(1680, "bitsubstr", 3, true, false, fc_bitsubstr),
        builtin(1699, "bitsubstr_no_len", 2, true, false, fc_bitsubstr_no_len),
        builtin(3030, "bitoverlay", 4, true, false, fc_bitoverlay),
        builtin(3031, "bitoverlay_no_len", 3, true, false, fc_bitoverlay_no_len),
    ]);
}

// ===========================================================================
// tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    /// Build an fcinfo with `nargs` slots, all by-value words null, ready for
    /// the caller to populate `ref_args` / scalar words.
    fn fcinfo(nargs: usize) -> FunctionCallInfoBaseData {
        let mut fc = FunctionCallInfoBaseData::new(None, nargs as i16, 0, None, None);
        fc.args = (0..nargs).map(|_| NullableDatum::value(Datum::null())).collect();
        fc.ref_args = (0..nargs).map(|_| None).collect();
        fc
    }

    /// Call a registered builtin by OID with the given fcinfo (via the
    /// Result-native overlay; unwrap the hard `Err` in tests).
    fn call(oid: u32, fc: &mut FunctionCallInfoBaseData) -> Datum {
        let f = backend_utils_fmgr_core::native_builtin(oid).expect("builtin not registered");
        f(fc).expect("builtin returned Err")
    }

    /// Run `bit_in`/`varbit_in` (OID) over `text`, returning the varbit image.
    fn run_in(oid: u32, text: &str, typmod: i32) -> Vec<u8> {
        let mut fc = fcinfo(3);
        fc.ref_args[0] = Some(RefPayload::Cstring(text.to_string()));
        fc.args[2] = NullableDatum::value(Datum::from_i32(typmod));
        call(oid, &mut fc);
        match fc.take_ref_result().expect("no result") {
            RefPayload::Varlena(b) => b,
            other => panic!("expected Varlena, got {other:?}"),
        }
    }

    /// Run a `_out` (OID) over a varbit image, returning the rendered string.
    fn run_out(oid: u32, image: &[u8]) -> String {
        let mut fc = fcinfo(1);
        fc.ref_args[0] = Some(RefPayload::Varlena(image.to_vec()));
        call(oid, &mut fc);
        match fc.take_ref_result().expect("no result") {
            RefPayload::Cstring(s) => s,
            other => panic!("expected Cstring, got {other:?}"),
        }
    }

    fn run_bin(oid: u32, a: &[u8], b: &[u8]) -> Datum {
        let mut fc = fcinfo(2);
        fc.ref_args[0] = Some(RefPayload::Varlena(a.to_vec()));
        fc.ref_args[1] = Some(RefPayload::Varlena(b.to_vec()));
        call(oid, &mut fc)
    }

    fn run_bin_varbit(oid: u32, a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut fc = fcinfo(2);
        fc.ref_args[0] = Some(RefPayload::Varlena(a.to_vec()));
        fc.ref_args[1] = Some(RefPayload::Varlena(b.to_vec()));
        call(oid, &mut fc);
        match fc.take_ref_result().expect("no result") {
            RefPayload::Varlena(v) => v,
            other => panic!("expected Varlena, got {other:?}"),
        }
    }

    fn setup() {
        crate::init_seams();
    }

    #[test]
    fn bit_in_out_roundtrip() {
        setup();
        // bit(4) = 1011
        let img = run_in(1564, "1011", 4);
        assert_eq!(run_out(1565, &img), "1011");
    }

    #[test]
    fn varbit_in_out_roundtrip() {
        setup();
        let img = run_in(1579, "101", -1);
        assert_eq!(run_out(1580, &img), "101");
    }

    #[test]
    fn bit_comparisons() {
        setup();
        let a = run_in(1579, "1010", -1);
        let b = run_in(1579, "1010", -1);
        let c = run_in(1579, "1011", -1);
        assert!(run_bin(1581, &a, &b).as_bool()); // biteq a,b
        assert!(!run_bin(1581, &a, &c).as_bool()); // biteq a,c
        assert!(run_bin(1582, &a, &c).as_bool()); // bitne a,c
        assert!(run_bin(1595, &a, &c).as_bool()); // bitlt a < c
        assert!(!run_bin(1593, &a, &c).as_bool()); // bitgt a > c -> false
        assert_eq!(run_bin(1596, &a, &b).as_i32(), 0); // bitcmp ==
        assert!(run_bin(1596, &a, &c).as_i32() < 0); // bitcmp a<c
        // varbit aliases dispatch the same cores
        assert!(run_bin(1666, &a, &b).as_bool()); // varbiteq
    }

    #[test]
    fn bitwise_ops() {
        setup();
        let a = run_in(1564, "1100", 4);
        let b = run_in(1564, "1010", 4);
        // bitand 1100 & 1010 = 1000
        assert_eq!(run_out(1565, &run_bin_varbit(1673, &a, &b)), "1000");
        // bitor 1100 | 1010 = 1110
        assert_eq!(run_out(1565, &run_bin_varbit(1674, &a, &b)), "1110");
        // bitxor 1100 ^ 1010 = 0110
        assert_eq!(run_out(1565, &run_bin_varbit(1675, &a, &b)), "0110");
        // bitnot ~1100 = 0011
        {
            let mut fc = fcinfo(1);
            fc.ref_args[0] = Some(RefPayload::Varlena(a.clone()));
            call(1676, &mut fc);
            let img = match fc.take_ref_result().unwrap() {
                RefPayload::Varlena(v) => v,
                _ => panic!(),
            };
            assert_eq!(run_out(1565, &img), "0011");
        }
    }

    #[test]
    fn bitcat_and_send() {
        setup();
        let a = run_in(1579, "10", -1);
        let b = run_in(1579, "11", -1);
        // bitcat 10 || 11 = 1011
        assert_eq!(run_out(1580, &run_bin_varbit(1679, &a, &b)), "1011");

        // bit_send: produces a bytea wire image (int32 bitlen + bytes).
        let four = run_in(1564, "1010", 4);
        let mut fc = fcinfo(1);
        fc.ref_args[0] = Some(RefPayload::Varlena(four));
        call(2457, &mut fc);
        let wire = match fc.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            _ => panic!(),
        };
        // header(4) + int32 bitlen(4) + 1 data byte = 9
        assert_eq!(wire.len(), 9);
    }

    #[test]
    fn bitshift() {
        setup();
        let a = run_in(1564, "1100", 4);
        // shift left by 1: 1000
        let mut fc = fcinfo(2);
        fc.ref_args[0] = Some(RefPayload::Varlena(a.clone()));
        fc.args[1] = NullableDatum::value(Datum::from_i32(1));
        call(1677, &mut fc);
        let img = match fc.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            _ => panic!(),
        };
        assert_eq!(run_out(1565, &img), "1000");
    }
}
