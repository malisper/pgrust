//! Port of `src/backend/access/hash/hashfunc.c` (PostgreSQL 18.3) — the
//! datatype-specific hash support functions stored in `pg_amproc` for the hash
//! access method (and reused by hash joins, catcache, and dynahash).
//!
//! Each `hashfunc.c` entry point is ported with its original C name. Two layers:
//!
//!   * a typed core (`hashint4`, `hashtext`, …) that mirrors the C body exactly,
//!     callable directly by Rust;
//!   * an `fc_*` fmgr adapter (`PG_FUNCTION_ARGS` shape) that unmarshals the
//!     `FunctionCallInfoBaseData` and boxes the result, registered as an fmgr
//!     builtin keyed by its `pg_proc` OID so the hash AM's
//!     `function_call1_coll` dispatch resolves it.
//!
//! What crosses a seam is exactly what `hashfunc.c` reaches outside itself:
//!   * the bit-mixing primitives `hash_any`/`hash_any_extended`/`hash_uint32`/
//!     `hash_uint32_extended` — these are `common/hashfn.c`, a direct dep
//!     (`common-hashfn`), no seam needed;
//!   * the collation-aware string path for `hashtext`/`hashtextextended`
//!     (`collation_is_deterministic` / `pg_strxfrm`, `utils/adt/pg_locale.c`);
//!   * `check_valid_oidvector` (the `oidvector` sanity check, `utils/adt/oid.c`).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use common_hashfn::{
    hash_bytes as hash_any, hash_bytes_extended as hash_any_extended,
    hash_bytes_uint32 as hash_uint32, hash_bytes_uint32_extended as hash_uint32_extended,
};
use types_core::Oid;
use types_datum::Datum;
use types_error::{PgResult, ERRCODE_INDETERMINATE_COLLATION, ERROR};
use types_fmgr::{FunctionCallInfoBaseData, PgFnNative};

use backend_utils_adt_oid_seams::check_valid_oidvector;
use backend_utils_adt_pg_locale_seams::{collation_is_deterministic, pg_strxfrm};
use backend_utils_error::ereport;

/// `get_float8_nan()` (utils/float.h) — `f64::NAN`. Inlined as in the sibling
/// hash-adjacent ports (the float seam owner is not a dependency here).
#[inline]
fn get_float8_nan() -> f64 {
    f64::NAN
}

// ===========================================================================
// "char" / boolean / int2 / int4
//
// hashchar is used for both the "char" and boolean datatypes (hashfunc.c). The
// C casts the 1-byte argument to int32 before mixing.
// ===========================================================================

/// `hashchar` (hashfunc.c:47): `hash_uint32((int32) PG_GETARG_CHAR(0))`.
pub fn hashchar(key: i8) -> u32 {
    hash_uint32(key as i32 as u32)
}

/// `hashcharextended` (hashfunc.c:53).
pub fn hashcharextended(key: i8, seed: u64) -> u64 {
    hash_uint32_extended(key as i32 as u32, seed)
}

/// `hashint2` (hashfunc.c:59): `hash_uint32((int32) PG_GETARG_INT16(0))`.
pub fn hashint2(key: i16) -> u32 {
    hash_uint32(key as i32 as u32)
}

/// `hashint2extended` (hashfunc.c:65).
pub fn hashint2extended(key: i16, seed: u64) -> u64 {
    hash_uint32_extended(key as i32 as u32, seed)
}

/// `hashint4` (hashfunc.c:71): `hash_uint32(PG_GETARG_INT32(0))`.
pub fn hashint4(key: i32) -> u32 {
    hash_uint32(key as u32)
}

/// `hashint4extended` (hashfunc.c:77).
pub fn hashint4extended(key: i32, seed: u64) -> u64 {
    hash_uint32_extended(key as u32, seed)
}

// ===========================================================================
// int8 — folds the high half into the low half so int8/int4/int2 hash equal for
// logically-equal inputs (cross-type hash joins). (hashfunc.c:83)
// ===========================================================================

/// Shared int8 → u32 folding (hashfunc.c:94-98):
/// `lohalf ^= (val >= 0) ? hihalf : ~hihalf;`
#[inline]
fn hashint8_fold(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    lohalf ^ if val >= 0 { hihalf } else { !hihalf }
}

/// `hashint8` (hashfunc.c:83): `hash_uint32(lohalf)`.
pub fn hashint8(key: i64) -> u32 {
    hash_uint32(hashint8_fold(key))
}

/// `hashint8extended` (hashfunc.c:103).
pub fn hashint8extended(key: i64, seed: u64) -> u64 {
    hash_uint32_extended(hashint8_fold(key), seed)
}

// ===========================================================================
// oid / enum (enum is also keyed on its pg_enum row Oid). (hashfunc.c:116/128)
// ===========================================================================

/// `hashoid` (hashfunc.c:116): `hash_uint32((uint32) PG_GETARG_OID(0))`.
pub fn hashoid(key: Oid) -> u32 {
    hash_uint32(key)
}

/// `hashoidextended` (hashfunc.c:122).
pub fn hashoidextended(key: Oid, seed: u64) -> u64 {
    hash_uint32_extended(key, seed)
}

/// `hashenum` (hashfunc.c:128): `hash_uint32((uint32) PG_GETARG_OID(0))`.
pub fn hashenum(key: Oid) -> u32 {
    hash_uint32(key)
}

/// `hashenumextended` (hashfunc.c:134).
pub fn hashenumextended(key: Oid, seed: u64) -> u64 {
    hash_uint32_extended(key, seed)
}

// ===========================================================================
// float4 / float8 — widen float4 to float8 for cross-type hashing; map ±0 to a
// single hash and canonicalize NaN. (hashfunc.c:140/193)
// ===========================================================================

/// `hashfloat4` (hashfunc.c:140).
pub fn hashfloat4(key: f32) -> u32 {
    // On IEEE machines minus zero and zero differ bitwise but compare equal;
    // both must hash the same.
    if key == 0.0f32 {
        return 0;
    }

    // Widen to float8 so an equal float8 hashes the same; canonicalize NaN.
    let mut key8 = key as f64;
    if key8.is_nan() {
        key8 = get_float8_nan();
    }

    hash_any(&key8.to_ne_bytes())
}

/// `hashfloat4extended` (hashfunc.c:176). `±0.0` returns the seed.
pub fn hashfloat4extended(key: f32, seed: u64) -> u64 {
    if key == 0.0f32 {
        return seed;
    }
    let mut key8 = key as f64;
    if key8.is_nan() {
        key8 = get_float8_nan();
    }
    hash_any_extended(&key8.to_ne_bytes(), seed)
}

/// `hashfloat8` (hashfunc.c:193).
pub fn hashfloat8(mut key: f64) -> u32 {
    if key == 0.0f64 {
        return 0;
    }
    if key.is_nan() {
        key = get_float8_nan();
    }
    hash_any(&key.to_ne_bytes())
}

/// `hashfloat8extended` (hashfunc.c:217). `±0.0` returns the seed.
pub fn hashfloat8extended(mut key: f64, seed: u64) -> u64 {
    if key == 0.0f64 {
        return seed;
    }
    if key.is_nan() {
        key = get_float8_nan();
    }
    hash_any_extended(&key.to_ne_bytes(), seed)
}

// ===========================================================================
// oidvector — validate the header, then mix `dim1 * sizeof(Oid)` bytes of the
// values image. (hashfunc.c:232)
//
// Unlike the text/name/varlena paths (which use the `_PP`/`NameStr` macros and
// so receive a header-stripped image), `hashoidvector` reads the argument via
// `PG_GETARG_POINTER` — the FULL `oidvector` struct. `image` is therefore the
// complete varlena byte image: the 24-byte header (`vl_len_`, `ndim`,
// `dataoffset`, `elemtype`, `dim1`, `lbound1`) followed by the `values` array.
// ===========================================================================

/// The fixed `oidvector` header, parsed off the varlena image. Mirrors
/// `types_array::oidvector` (c.h) field-for-field. Native-endian, as C reads it.
struct OidVectorHeader {
    ndim: i32,
    dataoffset: i32,
    elemtype: Oid,
    dim1: i32,
}

/// Read the fixed `oidvector` header from the leading 24 bytes of its image.
/// (`vl_len_`, `ndim`, `dataoffset`, `elemtype`, `dim1`, `lbound1`.)
fn oidvector_header(image: &[u8]) -> OidVectorHeader {
    let rd = |off: usize| i32::from_ne_bytes(image[off..off + 4].try_into().unwrap());
    OidVectorHeader {
        ndim: rd(4),
        dataoffset: rd(8),
        elemtype: rd(12) as u32,
        dim1: rd(16),
    }
}

/// `hashoidvector` (hashfunc.c:232):
/// `check_valid_oidvector(key); hash_any(key->values, key->dim1 * sizeof(Oid))`.
pub fn hashoidvector(image: &[u8]) -> PgResult<u32> {
    let h = oidvector_header(image);
    check_valid_oidvector::call(h.ndim, h.dataoffset, h.elemtype)?;
    // `key->values` begins at `sizeof(oidvector)` (24-byte header); the C hashes
    // `dim1 * sizeof(Oid)` bytes there.
    let nbytes = (h.dim1.max(0) as usize) * core::mem::size_of::<Oid>();
    Ok(hash_any(&image[24..24 + nbytes]))
}

/// `hashoidvectorextended` (hashfunc.c:241).
pub fn hashoidvectorextended(image: &[u8], seed: u64) -> PgResult<u64> {
    let h = oidvector_header(image);
    check_valid_oidvector::call(h.ndim, h.dataoffset, h.elemtype)?;
    let nbytes = (h.dim1.max(0) as usize) * core::mem::size_of::<Oid>();
    Ok(hash_any_extended(&image[24..24 + nbytes], seed))
}

// ===========================================================================
// name — hash `strlen(NameStr(name))` bytes. (hashfunc.c:252)
//
// `key` arrives `NameStr`-trimmed (the bytes up to, not including, the NUL).
// ===========================================================================

/// `hashname` (hashfunc.c:252): `hash_any(NameStr(name), strlen(...))`.
pub fn hashname(key: &[u8]) -> u32 {
    hash_any(key)
}

/// `hashnameextended` (hashfunc.c:260).
pub fn hashnameextended(key: &[u8], seed: u64) -> u64 {
    hash_any_extended(key, seed)
}

// ===========================================================================
// text — collation-aware. (hashfunc.c:269)
//
// `key` is the `VARDATA_ANY` image (header-stripped, `VARSIZE_ANY_EXHDR` long);
// `collid` is `PG_GET_COLLATION()`.
// ===========================================================================

/// The shared indeterminate-collation hard error (hashfunc.c:278-281 / 333-336).
fn indeterminate_collation() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INDETERMINATE_COLLATION)
        .errmsg("could not determine which collation to use for string hashing")
        .errhint("Use the COLLATE clause to set the collation explicitly.")
        .into_error()
}

/// `pg_strnxfrm` of `key` under `collid`, returning the transformed image plus
/// the trailing NUL (C: `palloc(bsize + 1)`, fill `bsize` significant bytes,
/// NUL at `bsize`, then hash `bsize + 1` bytes — the NUL is included for
/// backwards-compatibility). The scratch buffer drops at function end (C
/// `pfree`).
fn strxfrm_with_nul(key: &[u8], collid: Oid) -> PgResult<Vec<u8>> {
    // C allocates `buf` in CurrentMemoryContext and pfrees it immediately; a
    // local scratch context dropped here is equivalent.
    let scratch = mcx::MemoryContext::new("hashtext strxfrm");
    let blob = pg_strxfrm::call(scratch.mcx(), collid, key)?;
    let mut buf = Vec::with_capacity(blob.len() + 1);
    buf.extend_from_slice(&blob);
    buf.push(0); // the terminating NUL, preserved (hashfunc.c:308-313)
    Ok(buf)
}

/// `hashtext` (hashfunc.c:269). A zero `collid` is the indeterminate-collation
/// hard error; a deterministic collation mixes the bytes directly; a
/// non-deterministic one mixes the `pg_strnxfrm` sort key (+ trailing NUL).
pub fn hashtext(key: &[u8], collid: Oid) -> PgResult<u32> {
    if collid == 0 {
        return Err(indeterminate_collation());
    }

    if collation_is_deterministic::call(collid)? {
        Ok(hash_any(key))
    } else {
        let buf = strxfrm_with_nul(key, collid)?;
        Ok(hash_any(&buf))
    }
}

/// `hashtextextended` (hashfunc.c:324). Same collation logic as [`hashtext`],
/// using the extended mixer with the caller seed.
pub fn hashtextextended(key: &[u8], collid: Oid, seed: u64) -> PgResult<u64> {
    if collid == 0 {
        return Err(indeterminate_collation());
    }

    if collation_is_deterministic::call(collid)? {
        Ok(hash_any_extended(key, seed))
    } else {
        let buf = strxfrm_with_nul(key, collid)?;
        Ok(hash_any_extended(&buf, seed))
    }
}

// ===========================================================================
// varlena / bytea — hash the whole header-stripped image. (hashfunc.c:388)
// ===========================================================================

/// `hashvarlena` (hashfunc.c:388):
/// `hash_any(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key))`.
pub fn hashvarlena(key: &[u8]) -> u32 {
    hash_any(key)
}

/// `hashvarlenaextended` (hashfunc.c:403).
pub fn hashvarlenaextended(key: &[u8], seed: u64) -> u64 {
    hash_any_extended(key, seed)
}

/// `hashbytea` (hashfunc.c:418): `return hashvarlena(fcinfo)`.
pub fn hashbytea(key: &[u8]) -> u32 {
    hashvarlena(key)
}

/// `hashbyteaextended` (hashfunc.c:424): `return hashvarlenaextended(fcinfo)`.
pub fn hashbyteaextended(key: &[u8], seed: u64) -> u64 {
    hashvarlenaextended(key, seed)
}

// ===========================================================================
// fmgr adapters (PG_FUNCTION_ARGS) and builtin registration.
// ===========================================================================

/// Read by-value argument `i` as the raw machine word (`PG_GETARG_*`).
#[inline]
fn arg_word(fcinfo: &FunctionCallInfoBaseData, i: usize) -> usize {
    fcinfo
        .arg(i)
        .expect("hash fn: missing argument")
        .value
        .as_usize()
}

/// Read by-reference argument `i`'s byte image off the fmgr by-reference lane
/// (`PG_GETARG_TEXT_PP` / `PG_GETARG_NAME` / `PG_GETARG_VARLENA_PP` /
/// `PG_GETARG_POINTER` for the oidvector image).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("hash fn: by-reference argument missing from the by-ref lane")
}

/// `NameStr(*PG_GETARG_NAME(i))` length-trimmed bytes: a `name` value crosses
/// the by-reference lane as its fixed `NAMEDATALEN`-byte image, NUL-padded to
/// the full width. C's `hashname`/`hashnameextended` hash exactly
/// `strlen(NameStr(name))` bytes — the significant prefix up to (not including)
/// the first NUL — so the padding must be dropped before hashing. (Without this
/// the trailing NUL padding is mixed in and the result no longer matches
/// `hashtext` for the same string, breaking the cross-type text/name hash
/// opfamily 1995 invariant that hashed IN/ANY SubPlans rely on.)
#[inline]
fn arg_name_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = arg_bytes(fcinfo, i);
    let len = image.iter().position(|&b| b == 0).unwrap_or(image.len());
    &image[..len]
}

/// `VARDATA_ANY` of a header-ful text/bytea varlena argument: skip the 4-byte
/// length header so the core hashes `VARSIZE_ANY_EXHDR` payload bytes.
#[inline]
fn arg_varlena_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = arg_bytes(fcinfo, i);
    if image.len() >= 4 {
        &image[4..]
    } else {
        &[]
    }
}

/// `PG_RETURN_UINT32` — box a 32-bit hash into the fmgr-ABI result word.
#[inline]
fn ret_u32(v: u32) -> Datum {
    Datum::from_u32(v)
}

/// `PG_RETURN_UINT64` — box a 64-bit (extended) hash into the result word.
#[inline]
fn ret_u64(v: u64) -> Datum {
    Datum::from_u64(v)
}

fn fc_hashchar(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashchar(arg_word(fcinfo, 0) as i8)))
}
fn fc_hashcharextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashcharextended(arg_word(fcinfo, 0) as i8, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashint2(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashint2(arg_word(fcinfo, 0) as i16)))
}
fn fc_hashint2extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashint2extended(arg_word(fcinfo, 0) as i16, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashint4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashint4(arg_word(fcinfo, 0) as i32)))
}
fn fc_hashint4extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashint4extended(arg_word(fcinfo, 0) as i32, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashint8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashint8(arg_word(fcinfo, 0) as i64)))
}
fn fc_hashint8extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashint8extended(arg_word(fcinfo, 0) as i64, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashoid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashoid(arg_word(fcinfo, 0) as u32)))
}
fn fc_hashoidextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashoidextended(arg_word(fcinfo, 0) as u32, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashenum(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashenum(arg_word(fcinfo, 0) as u32)))
}
fn fc_hashenumextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashenumextended(arg_word(fcinfo, 0) as u32, arg_word(fcinfo, 1) as u64)))
}
fn fc_hashfloat4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashfloat4(f32::from_bits(arg_word(fcinfo, 0) as u32))))
}
fn fc_hashfloat4extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashfloat4extended(
        f32::from_bits(arg_word(fcinfo, 0) as u32),
        arg_word(fcinfo, 1) as u64,
    )))
}
fn fc_hashfloat8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashfloat8(f64::from_bits(arg_word(fcinfo, 0) as u64))))
}
fn fc_hashfloat8extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u64(hashfloat8extended(
        f64::from_bits(arg_word(fcinfo, 0) as u64),
        arg_word(fcinfo, 1) as u64,
    )))
}
fn fc_hashoidvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashoidvector(arg_bytes(fcinfo, 0))?))
}
fn fc_hashoidvectorextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let seed = arg_word(fcinfo, 1) as u64;
    Ok(ret_u64(hashoidvectorextended(arg_bytes(fcinfo, 0), seed)?))
}
fn fc_hashname(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashname(arg_name_str(fcinfo, 0))))
}
fn fc_hashnameextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let seed = arg_word(fcinfo, 1) as u64;
    Ok(ret_u64(hashnameextended(arg_name_str(fcinfo, 0), seed)))
}
fn fc_hashtext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let collid = fcinfo.fncollation;
    Ok(ret_u32(hashtext(arg_varlena_payload(fcinfo, 0), collid)?))
}
fn fc_hashtextextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let collid = fcinfo.fncollation;
    let seed = arg_word(fcinfo, 1) as u64;
    Ok(ret_u64(hashtextextended(arg_varlena_payload(fcinfo, 0), collid, seed)?))
}
fn fc_hashvarlena(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashvarlena(arg_varlena_payload(fcinfo, 0))))
}
fn fc_hashvarlenaextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let seed = arg_word(fcinfo, 1) as u64;
    Ok(ret_u64(hashvarlenaextended(arg_varlena_payload(fcinfo, 0), seed)))
}
fn fc_hashbytea(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_u32(hashbytea(arg_varlena_payload(fcinfo, 0))))
}
fn fc_hashbyteaextended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let seed = arg_word(fcinfo, 1) as u64;
    Ok(ret_u64(hashbyteaextended(arg_varlena_payload(fcinfo, 0), seed)))
}

// pg_proc OIDs (pg_proc.dat).
const F_HASHINT2: u32 = 449;
const F_HASHINT2EXTENDED: u32 = 441;
const F_HASHINT4: u32 = 450;
const F_HASHINT4EXTENDED: u32 = 425;
const F_HASHINT8: u32 = 949;
const F_HASHINT8EXTENDED: u32 = 442;
const F_HASHFLOAT4: u32 = 451;
const F_HASHFLOAT4EXTENDED: u32 = 443;
const F_HASHFLOAT8: u32 = 452;
const F_HASHFLOAT8EXTENDED: u32 = 444;
const F_HASHOID: u32 = 453;
const F_HASHOIDEXTENDED: u32 = 445;
const F_HASHCHAR: u32 = 454;
const F_HASHCHAREXTENDED: u32 = 446;
const F_HASHNAME: u32 = 455;
const F_HASHNAMEEXTENDED: u32 = 447;
const F_HASHTEXT: u32 = 400;
const F_HASHTEXTEXTENDED: u32 = 448;
const F_HASHVARLENA: u32 = 456;
const F_HASHVARLENAEXTENDED: u32 = 772;
const F_HASHBYTEA: u32 = 6413;
const F_HASHBYTEAEXTENDED: u32 = 6414;
const F_HASHOIDVECTOR: u32 = 457;
const F_HASHOIDVECTOREXTENDED: u32 = 776;
const F_HASHENUM: u32 = 3515;
const F_HASHENUMEXTENDED: u32 = 3414;

/// Build one `fmgr_builtins[]` row. `nargs` is the SQL arg count (the base
/// hashes take 1; the `*extended` variants take 2). All hash procs are STRICT
/// and not set-returning.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: PgFnNative,
) -> (types_fmgr::BuiltinFunction, PgFnNative) {
    (
        types_fmgr::BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        func,
    )
}

/// Register the hashfunc.c support procs as fmgr builtins (C: their
/// `fmgr_builtins[]` rows), so the hash AM's `function_call1_coll(hash_proc, …)`
/// dispatch resolves them by OID. Called from this crate's `init_seams()`.
pub fn register_hash_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(F_HASHCHAR, "hashchar", 1, fc_hashchar),
        builtin(F_HASHCHAREXTENDED, "hashcharextended", 2, fc_hashcharextended),
        builtin(F_HASHINT2, "hashint2", 1, fc_hashint2),
        builtin(F_HASHINT2EXTENDED, "hashint2extended", 2, fc_hashint2extended),
        builtin(F_HASHINT4, "hashint4", 1, fc_hashint4),
        builtin(F_HASHINT4EXTENDED, "hashint4extended", 2, fc_hashint4extended),
        builtin(F_HASHINT8, "hashint8", 1, fc_hashint8),
        builtin(F_HASHINT8EXTENDED, "hashint8extended", 2, fc_hashint8extended),
        builtin(F_HASHOID, "hashoid", 1, fc_hashoid),
        builtin(F_HASHOIDEXTENDED, "hashoidextended", 2, fc_hashoidextended),
        builtin(F_HASHENUM, "hashenum", 1, fc_hashenum),
        builtin(F_HASHENUMEXTENDED, "hashenumextended", 2, fc_hashenumextended),
        builtin(F_HASHFLOAT4, "hashfloat4", 1, fc_hashfloat4),
        builtin(F_HASHFLOAT4EXTENDED, "hashfloat4extended", 2, fc_hashfloat4extended),
        builtin(F_HASHFLOAT8, "hashfloat8", 1, fc_hashfloat8),
        builtin(F_HASHFLOAT8EXTENDED, "hashfloat8extended", 2, fc_hashfloat8extended),
        builtin(F_HASHOIDVECTOR, "hashoidvector", 1, fc_hashoidvector),
        builtin(F_HASHOIDVECTOREXTENDED, "hashoidvectorextended", 2, fc_hashoidvectorextended),
        builtin(F_HASHNAME, "hashname", 1, fc_hashname),
        builtin(F_HASHNAMEEXTENDED, "hashnameextended", 2, fc_hashnameextended),
        builtin(F_HASHTEXT, "hashtext", 1, fc_hashtext),
        builtin(F_HASHTEXTEXTENDED, "hashtextextended", 2, fc_hashtextextended),
        builtin(F_HASHVARLENA, "hashvarlena", 1, fc_hashvarlena),
        builtin(F_HASHVARLENAEXTENDED, "hashvarlenaextended", 2, fc_hashvarlenaextended),
        builtin(F_HASHBYTEA, "hashbytea", 1, fc_hashbytea),
        builtin(F_HASHBYTEAEXTENDED, "hashbyteaextended", 2, fc_hashbyteaextended),
    ]);
}

/// Install this crate's contribution: register the hashfunc builtins. This crate
/// owns no inward seam declarations (its functions are fmgr builtins, not
/// seams), so `init_seams` only performs the builtin registration.
pub fn init_seams() {
    register_hash_builtins();
}

#[cfg(test)]
mod tests;
