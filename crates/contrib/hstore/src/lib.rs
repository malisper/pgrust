#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Port of the PostgreSQL `hstore` contrib extension
//! (`contrib/hstore/{hstore_io,hstore_op,hstore_compat,hstore_subs}.c`).
//!
//! The `hstore` type and its functions are registered as the in-process ported
//! library `hstore` (mirroring `pg_prewarm` / `pg_stat_statements`): the SQL
//! emitted by `hstore--1.8.sql` (`CREATE FUNCTION ... LANGUAGE C AS
//! 'MODULE_PATHNAME'[,'<sym>']`) resolves through the dynamic-loader unit's
//! ported-library registry rather than the OS loader (the Rust backend exposes
//! no C ABI). The control + install SQL live in `extension/` and are installed
//! into the share directory's `extension/` subdir.
//!
//! The GiST/GIN opclass functions (`ghstore_*`, `gin_*_hstore`) and the hstore
//! subscript handler are NOT registered here (see the crate README / report);
//! index creation over hstore therefore falls back to the documented gap.

extern crate alloc;

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION, ERROR,
};
use ::utils_error::ereport;

mod repr;
use repr::{
    build_hstore, find_key, unique_pairs, HstoreView, Pair, HSTORE_MAX_KEY_LEN,
    HSTORE_MAX_VALUE_LEN,
};

mod records;

mod subs;

/// The simple (suffix-free) library name — `$libdir/hstore` reduces to this.
const LIBRARY: &str = "hstore";

const TEXTOID: ::types_core::Oid = 25;

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`).
pub(crate) fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

// ===========================================================================
// fmgr argument / result accessors (mirrors test_regress / pg_prewarm).
// ===========================================================================

/// `PG_ARGISNULL(i)`.
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|a| a.isnull).unwrap_or(true)
}

/// `PG_GETARG_INT64(i)`.
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("hstore: missing int8 arg").value.as_i64()
}

/// `PG_GETARG_CSTRING(i)`.
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("hstore: cstring arg missing from by-ref lane")
}

/// The full header-ful varlena image of by-ref arg `i`.
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("hstore: by-ref arg missing from by-ref lane")
}

/// `VARDATA_ANY` payload of an inline varlena image (header-form agnostic).
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => {
            &image[::datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
}

/// `PG_GETARG_TEXT_PP(i)` payload bytes.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    varlena_payload(arg_bytes(fcinfo, i))
}

/// `PG_GETARG_HSTORE_P(i)` — the hstore body as VARDATA (header stripped). The
/// fmgr boundary already detoasts; pgrust only produces new-format hstores.
fn arg_hstore<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> HstoreView<'a> {
    HstoreView::from_vardata(varlena_payload(arg_bytes(fcinfo, i)))
}

/// `PG_RETURN_CSTRING(s)`.
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// `PG_RETURN_POINTER(out)` for an hstore — `out` is the full header-ful image.
fn ret_hstore_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `PG_RETURN_TEXT_P` / `PG_RETURN_BYTEA_P` — header-ful varlena from payload.
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    let total = payload.len() + ::datum::varlena::VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(payload);
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `PG_RETURN_BOOL`.
fn ret_bool(fcinfo: &mut FunctionCallInfoBaseData, b: bool) -> Datum {
    fcinfo.isnull = false;
    Datum::from_bool(b)
}

/// `PG_RETURN_NULL`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Return a raw `text[]` varlena image as the array result.
fn ret_array_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

// ===========================================================================
// length checks (hard-error variants).
// ===========================================================================

fn check_key_len(len: usize) -> PgResult<usize> {
    if len > HSTORE_MAX_KEY_LEN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
            .errmsg("string too long for hstore key")
            .into_error());
    }
    Ok(len)
}

fn check_val_len(len: usize) -> PgResult<usize> {
    if len > HSTORE_MAX_VALUE_LEN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
            .errmsg("string too long for hstore value")
            .into_error());
    }
    Ok(len)
}

// ===========================================================================
// text array deconstruct/construct helpers (build at the contrib boundary
// against a scratch mcx).
// ===========================================================================

/// `deconstruct_array_builtin(a, TEXTOID, ...)` — read a `text[]` arg into a
/// `Vec<Option<Vec<u8>>>` (None == SQL NULL element).
fn deconstruct_text_array(image: &[u8]) -> PgResult<Vec<Option<Vec<u8>>>> {
    let scratch = ::mcx::MemoryContext::new("hstore text[] arg");
    let mcx = scratch.mcx();
    let v = arrayfuncs::construct::deconstruct_text_array_nullable(mcx, image)?;
    Ok(v.iter()
        .map(|o| o.as_ref().map(|s| s.as_str().as_bytes().to_vec()))
        .collect())
}

/// `construct_array_builtin` / `construct_md_array` over a flat 1-D `text[]`
/// result built from `Vec<Option<Vec<u8>>>` (None == array NULL).
fn build_text_array_1d(elems: &[Option<Vec<u8>>]) -> PgResult<Vec<u8>> {
    let scratch = ::mcx::MemoryContext::new("hstore text[] result");
    let mcx = scratch.mcx();
    let refs: Vec<Option<&[u8]>> = elems.iter().map(|o| o.as_deref()).collect();
    let img = arrayfuncs::construct::build_text_array_nullable(mcx, &refs)?;
    Ok(img.as_slice().to_vec())
}

// ===========================================================================
// I/O: hstore_in / hstore_out / hstore_recv / hstore_send (hstore_io.c).
// ===========================================================================

mod parse;

/// `hstore_in(cstring) -> hstore`. Threads `fcinfo->context` so a recoverable
/// syntax error `errsave`s into the soft-error sink (then `PG_RETURN_NULL`); a
/// hard error (no escontext) propagates and is raised.
fn fc_hstore_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).to_string();
    match parse::parse_hstore(s.as_bytes()) {
        Ok(pairs) => {
            let (pairs, _buflen) = unique_pairs(pairs);
            ret_hstore_image(fcinfo, build_hstore(&pairs))
        }
        Err(ParseFail::Hard(e)) => {
            // errsave: Some(ctx) saves + RETURN NULL; None raises the hard error.
            match fcinfo.escontext_mut() {
                Some(ctx) => {
                    ctx.save(e);
                    ret_null(fcinfo)
                }
                None => raise(e),
            }
        }
    }
}

pub(crate) enum ParseFail {
    Hard(PgError),
}

/// `hstore_out(hstore) -> cstring`.
fn fc_hstore_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let count = hs.count();
    if count == 0 {
        return ret_cstring(fcinfo, String::new());
    }
    let mut out: Vec<u8> = Vec::new();
    for i in 0..count {
        out.push(b'"');
        cpw(&mut out, hs.key(i));
        out.push(b'"');
        out.push(b'=');
        out.push(b'>');
        if hs.val_isnull(i) {
            out.extend_from_slice(b"NULL");
        } else {
            out.push(b'"');
            cpw(&mut out, hs.val(i));
            out.push(b'"');
        }
        if i + 1 != count {
            out.push(b',');
            out.push(b' ');
        }
    }
    ret_cstring(fcinfo, String::from_utf8_lossy(&out).into_owned())
}

/// `cpw(dst, src, len)` (hstore_io.c) — copy bytes, backslash-escaping `"`/`\`.
fn cpw(dst: &mut Vec<u8>, src: &[u8]) {
    for &b in src {
        if b == b'"' || b == b'\\' {
            dst.push(b'\\');
        }
        dst.push(b);
    }
}

/// `hstore_recv(internal) -> hstore`.
fn fc_hstore_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // The StringInfo payload crosses on the by-ref Varlena lane.
    let buf = arg_bytes(fcinfo, 0).to_vec();
    match recv_impl(&buf) {
        Ok(image) => ret_hstore_image(fcinfo, image),
        Err(e) => raise(e),
    }
}

fn recv_impl(buf: &[u8]) -> PgResult<Vec<u8>> {
    let mut cur = 0usize;
    let read_i32 = |cur: &mut usize| -> PgResult<i32> {
        if *cur + 4 > buf.len() {
            return Err(PgError::error("insufficient data left in message"));
        }
        let v = i32::from_be_bytes([buf[*cur], buf[*cur + 1], buf[*cur + 2], buf[*cur + 3]]);
        *cur += 4;
        Ok(v)
    };
    let read_text = |cur: &mut usize, len: usize| -> PgResult<Vec<u8>> {
        if *cur + len > buf.len() {
            return Err(PgError::error("insufficient data left in message"));
        }
        let v = buf[*cur..*cur + len].to_vec();
        *cur += len;
        Ok(v)
    };

    let pcount = read_i32(&mut cur)?;
    if pcount == 0 {
        return Ok(build_hstore(&[]));
    }
    if pcount < 0 {
        return Err(ereport(ERROR)
            .errmsg("invalid hstore pair count")
            .into_error());
    }
    let mut pairs: Vec<Pair> = Vec::with_capacity(pcount as usize);
    for _ in 0..pcount {
        let rawlen = read_i32(&mut cur)?;
        if rawlen < 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg("null value not allowed for hstore key")
                .into_error());
        }
        let key = read_text(&mut cur, rawlen as usize)?;
        check_key_len(key.len())?;
        let rawlen = read_i32(&mut cur)?;
        let val = if rawlen < 0 {
            None
        } else {
            let v = read_text(&mut cur, rawlen as usize)?;
            check_val_len(v.len())?;
            Some(v)
        };
        pairs.push(Pair {
            key,
            val,
            needfree: true,
        });
    }
    let (pairs, _) = unique_pairs(pairs);
    Ok(build_hstore(&pairs))
}

/// `hstore_send(hstore) -> bytea`.
fn fc_hstore_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let count = hs.count();
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&(count as i32).to_be_bytes());
    for i in 0..count {
        let key = hs.key(i);
        buf.extend_from_slice(&(key.len() as i32).to_be_bytes());
        buf.extend_from_slice(key);
        if hs.val_isnull(i) {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let val = hs.val(i);
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }
    ret_varlena(fcinfo, &buf)
}

// ===========================================================================
// hstore(text,text) / hstore(text[]) / hstore(text[],text[]) constructors.
// ===========================================================================

/// `hstore_from_text(text, text) -> hstore` (also `tconvert`). NOT STRICT.
fn fc_hstore_from_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_isnull(fcinfo, 0) {
        return ret_null(fcinfo);
    }
    let key = arg_text(fcinfo, 0).to_vec();
    let val = if arg_isnull(fcinfo, 1) {
        None
    } else {
        Some(arg_text(fcinfo, 1).to_vec())
    };
    match (|| -> PgResult<Vec<u8>> {
        check_key_len(key.len())?;
        if let Some(ref v) = val {
            check_val_len(v.len())?;
        }
        let pairs = vec![Pair {
            key,
            val,
            needfree: false,
        }];
        Ok(build_hstore(&pairs))
    })() {
        Ok(img) => ret_hstore_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_from_arrays(text[], text[]) -> hstore`. NOT STRICT.
fn fc_hstore_from_arrays(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match from_arrays_impl(fcinfo) {
        Ok(Some(img)) => ret_hstore_image(fcinfo, img),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

fn from_arrays_impl(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Option<Vec<u8>>> {
    if arg_isnull(fcinfo, 0) {
        return Ok(None);
    }
    let keys = deconstruct_text_array(arg_bytes(fcinfo, 0))?;
    let key_count = keys.len();

    let vals: Option<Vec<Option<Vec<u8>>>> = if arg_isnull(fcinfo, 1) {
        None
    } else {
        let v = deconstruct_text_array(arg_bytes(fcinfo, 1))?;
        if v.len() != key_count {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .errmsg("arrays must have same bounds")
                .into_error());
        }
        Some(v)
    };

    let mut pairs: Vec<Pair> = Vec::with_capacity(key_count);
    for i in 0..key_count {
        let key = match &keys[i] {
            Some(k) => k.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                    .errmsg("null value not allowed for hstore key")
                    .into_error())
            }
        };
        check_key_len(key.len())?;
        let val = match &vals {
            None => None,
            Some(vs) => match &vs[i] {
                None => None,
                Some(v) => {
                    check_val_len(v.len())?;
                    Some(v.clone())
                }
            },
        };
        pairs.push(Pair {
            key,
            val,
            needfree: false,
        });
    }
    let (pairs, _) = unique_pairs(pairs);
    Ok(Some(build_hstore(&pairs)))
}

/// `hstore_from_array(text[]) -> hstore`. STRICT. 1-D even or 2-D Nx2.
fn fc_hstore_from_array(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match from_array_impl(fcinfo) {
        Ok(img) => ret_hstore_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn from_array_impl(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Vec<u8>> {
    let image = arg_bytes(fcinfo, 0);
    let ndim = arrayfuncs::foundation::arr_ndim(varlena_for_array(image));
    match ndim {
        0 => return Ok(build_hstore(&[])),
        1 => {}
        2 => {}
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .errmsg("wrong number of array subscripts")
                .into_error())
        }
    }
    let elems = deconstruct_text_array(image)?;
    // For ndim 1 the count must be even; for ndim 2 the second dim must be 2.
    if ndim == 1 && elems.len() % 2 != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .errmsg("array must have even number of elements")
            .into_error());
    }
    if ndim == 2 {
        let dims = {
            let scratch = ::mcx::MemoryContext::new("hstore dims");
            let mcx = scratch.mcx();
            let d = arrayfuncs::foundation::arr_dims(mcx, varlena_for_array(image))?;
            d.iter().copied().collect::<Vec<i32>>()
        };
        if dims.len() != 2 || dims[1] != 2 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .errmsg("array must have two columns")
                .into_error());
        }
    }
    let count = elems.len() / 2;
    let mut pairs: Vec<Pair> = Vec::with_capacity(count);
    for i in 0..count {
        let key = match &elems[i * 2] {
            Some(k) => k.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                    .errmsg("null value not allowed for hstore key")
                    .into_error())
            }
        };
        check_key_len(key.len())?;
        let val = match &elems[i * 2 + 1] {
            None => None,
            Some(v) => {
                check_val_len(v.len())?;
                Some(v.clone())
            }
        };
        pairs.push(Pair {
            key,
            val,
            needfree: false,
        });
    }
    let (pairs, _) = unique_pairs(pairs);
    Ok(build_hstore(&pairs))
}

/// The array varlena image as the `&[u8]` the foundation readers expect
/// (detoasted, header-ful). At the boundary it is already detoasted.
fn varlena_for_array(image: &[u8]) -> &[u8] {
    image
}

// ===========================================================================
// Operators / scalar functions (hstore_op.c).
// ===========================================================================

/// `hstore_fetchval(hstore, text) -> text`. Operator `->`.
fn fc_hstore_fetchval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let key = arg_text(fcinfo, 1);
    match find_key(&hs, None, key) {
        Some(idx) if !hs.val_isnull(idx) => {
            let val = hs.val(idx).to_vec();
            ret_varlena(fcinfo, &val)
        }
        _ => ret_null(fcinfo),
    }
}

/// `hstore_exists(hstore, text) -> bool`. Operator `?`.
fn fc_hstore_exists(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let key = arg_text(fcinfo, 1);
    let res = find_key(&hs, None, key).is_some();
    ret_bool(fcinfo, res)
}

/// `hstore_defined(hstore, text) -> bool`.
fn fc_hstore_defined(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let key = arg_text(fcinfo, 1);
    let res = match find_key(&hs, None, key) {
        Some(idx) => !hs.val_isnull(idx),
        None => false,
    };
    ret_bool(fcinfo, res)
}

/// `hstore_exists_any(hstore, text[]) -> bool`. Operator `?|`.
fn fc_hstore_exists_any(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match exists_n_impl(fcinfo, false) {
        Ok(b) => ret_bool(fcinfo, b),
        Err(e) => raise(e),
    }
}

/// `hstore_exists_all(hstore, text[]) -> bool`. Operator `?&`.
fn fc_hstore_exists_all(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match exists_n_impl(fcinfo, true) {
        Ok(b) => ret_bool(fcinfo, b),
        Err(e) => raise(e),
    }
}

fn exists_n_impl(fcinfo: &mut FunctionCallInfoBaseData, all: bool) -> PgResult<bool> {
    let hs = arg_hstore(fcinfo, 0);
    let key_pairs = array_to_keys(arg_bytes(fcinfo, 1))?;
    let mut lowbound = 0usize;
    if all {
        for k in &key_pairs {
            if find_key(&hs, Some(&mut lowbound), k).is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    } else {
        for k in &key_pairs {
            if find_key(&hs, Some(&mut lowbound), k).is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// `hstoreArrayToPairs(a)` — sorted, unique, non-null keys.
fn array_to_keys(image: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let elems = deconstruct_text_array(image)?;
    let pairs: Vec<Pair> = elems
        .into_iter()
        .flatten()
        .map(|k| Pair {
            key: k,
            val: None,
            needfree: false,
        })
        .collect();
    let (pairs, _) = unique_pairs(pairs);
    Ok(pairs.into_iter().map(|p| p.key).collect())
}

/// `hstore_delete(hstore, text) -> hstore`. Operator `-`.
fn fc_hstore_delete(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let key = arg_text(fcinfo, 1);
    let mut pairs = hs.to_pairs();
    pairs.retain(|p| !(p.key.len() == key.len() && p.key == key));
    // pairs are already sorted+unique (from a stored hstore).
    ret_hstore_image(fcinfo, build_hstore(&pairs))
}

/// `hstore_delete_array(hstore, text[]) -> hstore`. Operator `-`.
fn fc_hstore_delete_array(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match (|| -> PgResult<Vec<u8>> {
        let hs = arg_hstore(fcinfo, 0);
        let del = array_to_keys(arg_bytes(fcinfo, 1))?;
        let del_set: std::collections::HashSet<&[u8]> = del.iter().map(|k| k.as_slice()).collect();
        let pairs: Vec<Pair> = hs
            .to_pairs()
            .into_iter()
            .filter(|p| !del_set.contains(p.key.as_slice()))
            .collect();
        Ok(build_hstore(&pairs))
    })() {
        Ok(img) => ret_hstore_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_delete_hstore(hstore, hstore) -> hstore`. Operator `-`.
fn fc_hstore_delete_hstore(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let hs2 = arg_hstore(fcinfo, 1);
    // drop pairs of hs whose (key,value) equal hs2's.
    let pairs: Vec<Pair> = (0..hs.count())
        .filter_map(|i| {
            let key = hs.key(i);
            if let Some(j) = find_key(&hs2, None, key) {
                // present in hs2: drop iff values equal (null-aware).
                let nullval = hs.val_isnull(i);
                let equal = nullval == hs2.val_isnull(j)
                    && (nullval || hs.val(i) == hs2.val(j));
                if equal {
                    return None;
                }
            }
            Some(Pair {
                key: key.to_vec(),
                val: if hs.val_isnull(i) {
                    None
                } else {
                    Some(hs.val(i).to_vec())
                },
                needfree: false,
            })
        })
        .collect();
    ret_hstore_image(fcinfo, build_hstore(&pairs))
}

/// `hstore_concat(hstore, hstore) -> hstore`. Operator `||`. s2 wins on ties.
fn fc_hstore_concat(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s1 = arg_hstore(fcinfo, 0);
    let s2 = arg_hstore(fcinfo, 1);
    // merge two sorted lists; on equal key take s2.
    let mut out: Vec<Pair> = Vec::with_capacity(s1.count() + s2.count());
    let (mut i, mut j) = (0usize, 0usize);
    let pair = |v: &HstoreView, k: usize| Pair {
        key: v.key(k).to_vec(),
        val: if v.val_isnull(k) {
            None
        } else {
            Some(v.val(k).to_vec())
        },
        needfree: false,
    };
    while i < s1.count() || j < s2.count() {
        let diff = if i >= s1.count() {
            1
        } else if j >= s2.count() {
            -1
        } else {
            let k1 = s1.key(i);
            let k2 = s2.key(j);
            if k1.len() == k2.len() {
                match k1.cmp(k2) {
                    core::cmp::Ordering::Less => -1,
                    core::cmp::Ordering::Equal => 0,
                    core::cmp::Ordering::Greater => 1,
                }
            } else if k1.len() > k2.len() {
                1
            } else {
                -1
            }
        };
        if diff >= 0 {
            out.push(pair(&s2, j));
            j += 1;
            if diff == 0 {
                i += 1;
            }
        } else {
            out.push(pair(&s1, i));
            i += 1;
        }
    }
    // (build_hstore is the HS_FINALIZE-equivalent; out is already sorted+unique.)
    ret_hstore_image(fcinfo, build_hstore(&out))
}

/// `hstore_contains(hstore, hstore) -> bool`. Operator `@>`.
fn fc_hstore_contains(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = contains(&arg_hstore(fcinfo, 0), &arg_hstore(fcinfo, 1));
    ret_bool(fcinfo, res)
}

/// `hstore_contained(hstore, hstore) -> bool`. Operator `<@`.
fn fc_hstore_contained(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = contains(&arg_hstore(fcinfo, 1), &arg_hstore(fcinfo, 0));
    ret_bool(fcinfo, res)
}

/// `hstore_contains(val, tmpl)` — does val contain every (key,value) of tmpl?
fn contains(val: &HstoreView, tmpl: &HstoreView) -> bool {
    let mut lastidx = 0usize;
    for i in 0..tmpl.count() {
        match find_key(val, Some(&mut lastidx), tmpl.key(i)) {
            Some(idx) => {
                let nullval = tmpl.val_isnull(i);
                let equal = nullval == val.val_isnull(idx)
                    && (nullval || tmpl.val(i) == val.val(idx));
                if !equal {
                    return false;
                }
            }
            None => return false,
        }
    }
    true
}

/// `hstore_slice_to_hstore(hstore, text[]) -> hstore`. `slice`.
fn fc_hstore_slice_to_hstore(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match (|| -> PgResult<Vec<u8>> {
        let hs = arg_hstore(fcinfo, 0);
        let keys = array_to_keys(arg_bytes(fcinfo, 1))?;
        let mut lastidx = 0usize;
        let mut out: Vec<Pair> = Vec::new();
        for k in &keys {
            if let Some(idx) = find_key(&hs, Some(&mut lastidx), k) {
                out.push(Pair {
                    key: k.clone(),
                    val: if hs.val_isnull(idx) {
                        None
                    } else {
                        Some(hs.val(idx).to_vec())
                    },
                    needfree: false,
                });
            }
        }
        Ok(build_hstore(&out))
    })() {
        Ok(img) => ret_hstore_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_slice_to_array(hstore, text[]) -> text[]`. Operator `->`(hstore,text[]).
fn fc_hstore_slice_to_array(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match (|| -> PgResult<Vec<u8>> {
        let hs = arg_hstore(fcinfo, 0);
        let keys = deconstruct_text_array(arg_bytes(fcinfo, 1))?;
        let out: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|kopt| {
                let idx = match kopt {
                    Some(k) => find_key(&hs, None, k),
                    None => None,
                };
                match idx {
                    Some(i) if !hs.val_isnull(i) => Some(hs.val(i).to_vec()),
                    _ => None,
                }
            })
            .collect();
        // Preserve input dims (1-D and 2-D) via construct_md_array.
        build_slice_array(arg_bytes(fcinfo, 1), &out)
    })() {
        Ok(img) => ret_array_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// Build the `slice_array` result preserving the key array's dims/lbounds.
fn build_slice_array(key_array_image: &[u8], out: &[Option<Vec<u8>>]) -> PgResult<Vec<u8>> {
    let scratch = ::mcx::MemoryContext::new("hstore slice array");
    let mcx = scratch.mcx();
    let ndim = arrayfuncs::foundation::arr_ndim(key_array_image);
    if ndim <= 0 || out.is_empty() {
        let e = arrayfuncs::construct::construct_empty_array(mcx, TEXTOID)?;
        return Ok(e.as_slice().to_vec());
    }
    let dims = arrayfuncs::foundation::arr_dims(mcx, key_array_image)?;
    let lbs = arrayfuncs::foundation::arr_lbounds(mcx, key_array_image)?;
    let dims_v: Vec<i32> = dims.iter().copied().collect();
    let lbs_v: Vec<i32> = lbs.iter().copied().collect();
    // Build text Datums for construct_md_array_values.
    let mut datums: Vec<::types_tuple::Datum> = Vec::with_capacity(out.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(out.len());
    for o in out {
        match o {
            Some(bytes) => {
                datums.push(text_datum(mcx, bytes)?);
                nulls.push(false);
            }
            None => {
                datums.push(::types_tuple::Datum::ByVal(0));
                nulls.push(true);
            }
        }
    }
    let img = arrayfuncs::construct::construct_md_array_values(
        mcx, &datums, Some(&nulls), ndim, &dims_v, &lbs_v, TEXTOID, -1, false, b'i',
    )?;
    Ok(img.as_slice().to_vec())
}

/// Build a `text` value (`types_tuple::Datum`) from payload bytes.
fn text_datum<'mcx>(mcx: ::mcx::Mcx<'mcx>, bytes: &[u8]) -> PgResult<::types_tuple::Datum<'mcx>> {
    varlena_seams::bytes_to_varlena_v::call(mcx, bytes)
}

/// `hstore_akeys(hstore) -> text[]`.
fn fc_hstore_akeys(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let elems: Vec<Option<Vec<u8>>> = (0..hs.count()).map(|i| Some(hs.key(i).to_vec())).collect();
    match build_text_array_1d(&elems) {
        Ok(img) => ret_array_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_avals(hstore) -> text[]`.
fn fc_hstore_avals(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let elems: Vec<Option<Vec<u8>>> = (0..hs.count())
        .map(|i| {
            if hs.val_isnull(i) {
                None
            } else {
                Some(hs.val(i).to_vec())
            }
        })
        .collect();
    match build_text_array_1d(&elems) {
        Ok(img) => ret_array_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_to_array(hstore) -> text[]` (flat 1-D k,v,...). Operator `%%`.
fn fc_hstore_to_array(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let mut elems: Vec<Option<Vec<u8>>> = Vec::with_capacity(hs.count() * 2);
    for i in 0..hs.count() {
        elems.push(Some(hs.key(i).to_vec()));
        elems.push(if hs.val_isnull(i) {
            None
        } else {
            Some(hs.val(i).to_vec())
        });
    }
    match build_text_array_1d(&elems) {
        Ok(img) => ret_array_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

/// `hstore_to_matrix(hstore) -> text[]` (2-D Nx2). Operator `%#`.
fn fc_hstore_to_matrix(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match (|| -> PgResult<Vec<u8>> {
        let hs = arg_hstore(fcinfo, 0);
        let count = hs.count();
        let scratch = ::mcx::MemoryContext::new("hstore matrix");
        let mcx = scratch.mcx();
        if count == 0 {
            let e = arrayfuncs::construct::construct_empty_array(mcx, TEXTOID)?;
            return Ok(e.as_slice().to_vec());
        }
        let mut datums: Vec<::types_tuple::Datum> = Vec::with_capacity(count * 2);
        let mut nulls: Vec<bool> = Vec::with_capacity(count * 2);
        for i in 0..count {
            datums.push(text_datum(mcx, hs.key(i))?);
            nulls.push(false);
            if hs.val_isnull(i) {
                datums.push(::types_tuple::Datum::ByVal(0));
                nulls.push(true);
            } else {
                datums.push(text_datum(mcx, hs.val(i))?);
                nulls.push(false);
            }
        }
        let dims = [count as i32, 2];
        let lbs = [1, 1];
        let img = arrayfuncs::construct::construct_md_array_values(
            mcx, &datums, Some(&nulls), 2, &dims, &lbs, TEXTOID, -1, false, b'i',
        )?;
        Ok(img.as_slice().to_vec())
    })() {
        Ok(img) => ret_array_image(fcinfo, img),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// btree / hash support (hstore_op.c).
// ===========================================================================

/// `hstore_cmp(hstore, hstore) -> integer`.
fn hstore_cmp(hs1: &HstoreView, hs2: &HstoreView) -> i32 {
    let c1 = hs1.count();
    let c2 = hs2.count();
    if c1 == 0 || c2 == 0 {
        return if c1 > 0 {
            1
        } else if c2 > 0 {
            -1
        } else {
            0
        };
    }
    let len1 = hs1.pool_len();
    let len2 = hs2.pool_len();
    // memcmp over the string pools.
    let pool1 = pool_bytes(hs1);
    let pool2 = pool_bytes(hs2);
    let minlen = len1.min(len2);
    let res = pool1[..minlen].cmp(&pool2[..minlen]);
    use core::cmp::Ordering;
    if res == Ordering::Equal {
        if len1 != len2 {
            return if len1 > len2 { 1 } else { -1 };
        }
        if c1 != c2 {
            return if c1 > c2 { 1 } else { -1 };
        }
        let count = c1 * 2;
        for i in 0..count {
            if hs1.raw_endpos(i) != hs2.raw_endpos(i) || hs1.raw_isnull(i) != hs2.raw_isnull(i) {
                if hs1.raw_endpos(i) < hs2.raw_endpos(i) {
                    return -1;
                } else if hs1.raw_endpos(i) > hs2.raw_endpos(i) {
                    return 1;
                } else if hs1.raw_isnull(i) {
                    return 1;
                } else if hs2.raw_isnull(i) {
                    return -1;
                }
            }
        }
        0
    } else if res == Ordering::Greater {
        1
    } else {
        -1
    }
}

/// The string-pool bytes of an hstore (VARDATA after the entry array).
fn pool_bytes<'a>(hs: &HstoreView<'a>) -> &'a [u8] {
    let body = hs.vardata();
    let base = 4 + hs.count() * 2 * 4;
    &body[base..]
}

macro_rules! cmp_fn {
    ($name:ident, $op:tt) => {
        fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let r = hstore_cmp(&arg_hstore(fcinfo, 0), &arg_hstore(fcinfo, 1));
            ret_bool(fcinfo, r $op 0)
        }
    };
}
cmp_fn!(fc_hstore_eq, ==);
cmp_fn!(fc_hstore_ne, !=);
cmp_fn!(fc_hstore_gt, >);
cmp_fn!(fc_hstore_ge, >=);
cmp_fn!(fc_hstore_lt, <);
cmp_fn!(fc_hstore_le, <=);

fn fc_hstore_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let r = hstore_cmp(&arg_hstore(fcinfo, 0), &arg_hstore(fcinfo, 1));
    fcinfo.isnull = false;
    Datum::from_i32(r)
}

/// `hstore_hash(hstore) -> integer`.
fn fc_hstore_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let h = hashfn::hash_bytes(hs.vardata());
    fcinfo.isnull = false;
    // hash_any returns a Datum holding a uint32; PG_RETURN_DATUM keeps the word.
    Datum::from_u32(h)
}

/// `hstore_hash_extended(hstore, int8) -> int8`.
fn fc_hstore_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let hs = arg_hstore(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    let h = hashfn::hash_bytes_extended(hs.vardata(), seed);
    fcinfo.isnull = false;
    Datum::from_i64(h as i64)
}

// ===========================================================================
// JSON conversion (hstore_io.c).
// ===========================================================================

/// `hstore_to_json(hstore) -> json`. CAST hstore->json.
fn fc_hstore_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    to_json(fcinfo, false)
}

/// `hstore_to_json_loose(hstore) -> json`.
fn fc_hstore_to_json_loose(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    to_json(fcinfo, true)
}

fn to_json(fcinfo: &mut FunctionCallInfoBaseData, loose: bool) -> Datum {
    match (|| -> PgResult<Vec<u8>> {
        let hs = arg_hstore(fcinfo, 0);
        let count = hs.count();
        if count == 0 {
            return Ok(b"{}".to_vec());
        }
        let scratch = ::mcx::MemoryContext::new("hstore json");
        let mcx = scratch.mcx();
        let mut dst = ::mcx::PgString::new_in(mcx);
        use core::fmt::Write;
        dst.write_char('{').ok();
        for i in 0..count {
            json_seams::escape_json_with_len::call(&mut dst, hs.key(i))?;
            dst.write_str(": ").ok();
            if hs.val_isnull(i) {
                dst.write_str("null").ok();
            } else {
                let val = hs.val(i);
                if loose && val.len() == 1 && val[0] == b't' {
                    dst.write_str("true").ok();
                } else if loose && val.len() == 1 && val[0] == b'f' {
                    dst.write_str("false").ok();
                } else if loose && jsonapi::is_valid_json_number(val) {
                    dst.write_str(core::str::from_utf8(val).unwrap_or("")).ok();
                } else {
                    json_seams::escape_json_with_len::call(&mut dst, val)?;
                }
            }
            if i + 1 != count {
                dst.write_str(", ").ok();
            }
        }
        dst.write_char('}').ok();
        Ok(dst.as_bytes().to_vec())
    })() {
        Ok(payload) => ret_varlena(fcinfo, &payload),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// SRFs (hstore_op.c): skeys / svals / each — materialize mode.
// ===========================================================================

mod srf;

// ===========================================================================
// Builtin-library registration.
// ===========================================================================

fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        // I/O (AS 'MODULE_PATHNAME' — symbol == SQL function name).
        "hstore_in" => Some(fc_hstore_in),
        "hstore_out" => Some(fc_hstore_out),
        "hstore_recv" => Some(fc_hstore_recv),
        "hstore_send" => Some(fc_hstore_send),
        // constructors.
        "hstore_from_text" => Some(fc_hstore_from_text),
        "hstore_from_arrays" => Some(fc_hstore_from_arrays),
        "hstore_from_array" => Some(fc_hstore_from_array),
        "hstore_from_record" => Some(records::fc_hstore_from_record),
        // operators / scalar.
        "hstore_fetchval" => Some(fc_hstore_fetchval),
        "hstore_exists" => Some(fc_hstore_exists),
        "hstore_exists_any" => Some(fc_hstore_exists_any),
        "hstore_exists_all" => Some(fc_hstore_exists_all),
        "hstore_defined" => Some(fc_hstore_defined),
        "hstore_delete" => Some(fc_hstore_delete),
        "hstore_delete_array" => Some(fc_hstore_delete_array),
        "hstore_delete_hstore" => Some(fc_hstore_delete_hstore),
        "hstore_concat" => Some(fc_hstore_concat),
        "hstore_contains" => Some(fc_hstore_contains),
        "hstore_contained" => Some(fc_hstore_contained),
        "hstore_slice_to_array" => Some(fc_hstore_slice_to_array),
        "hstore_slice_to_hstore" => Some(fc_hstore_slice_to_hstore),
        "hstore_akeys" => Some(fc_hstore_akeys),
        "hstore_avals" => Some(fc_hstore_avals),
        "hstore_to_array" => Some(fc_hstore_to_array),
        "hstore_to_matrix" => Some(fc_hstore_to_matrix),
        "hstore_populate_record" => Some(records::fc_hstore_populate_record),
        // SRFs.
        "hstore_skeys" => Some(srf::fc_hstore_skeys),
        "hstore_svals" => Some(srf::fc_hstore_svals),
        "hstore_each" => Some(srf::fc_hstore_each),
        // json/jsonb.
        "hstore_to_json" => Some(fc_hstore_to_json),
        "hstore_to_json_loose" => Some(fc_hstore_to_json_loose),
        "hstore_to_jsonb" => Some(records::fc_hstore_to_jsonb),
        "hstore_to_jsonb_loose" => Some(records::fc_hstore_to_jsonb_loose),
        // btree / hash.
        "hstore_eq" => Some(fc_hstore_eq),
        "hstore_ne" => Some(fc_hstore_ne),
        "hstore_gt" => Some(fc_hstore_gt),
        "hstore_ge" => Some(fc_hstore_ge),
        "hstore_lt" => Some(fc_hstore_lt),
        "hstore_le" => Some(fc_hstore_le),
        "hstore_cmp" => Some(fc_hstore_cmp),
        "hstore_hash" => Some(fc_hstore_hash),
        "hstore_hash_extended" => Some(fc_hstore_hash_extended),
        // version diag.
        "hstore_version_diag" => Some(fc_hstore_version_diag),
        // GiST / GIN opclass + subscript handler (loud-panic stubs; the
        // catalog objects exist so CREATE EXTENSION succeeds, but index
        // build / subscripting is the documented gap).
        "ghstore_in" => Some(records::fc_ghstore_in),
        "ghstore_out" => Some(records::fc_ghstore_out),
        "ghstore_compress" => Some(records::fc_ghstore_compress),
        "ghstore_decompress" => Some(records::fc_ghstore_decompress),
        "ghstore_penalty" => Some(records::fc_ghstore_penalty),
        "ghstore_picksplit" => Some(records::fc_ghstore_picksplit),
        "ghstore_union" => Some(records::fc_ghstore_union),
        "ghstore_same" => Some(records::fc_ghstore_same),
        "ghstore_consistent" => Some(records::fc_ghstore_consistent),
        "ghstore_options" => Some(records::fc_ghstore_options),
        "gin_extract_hstore" => Some(records::fc_gin_extract_hstore),
        "gin_extract_hstore_query" => Some(records::fc_gin_extract_hstore_query),
        "gin_consistent_hstore" => Some(records::fc_gin_consistent_hstore),
        "hstore_subscript_handler" => Some(records::fc_hstore_subscript_handler),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// `hstore_version_diag(hstore) -> integer` — new-format always = 2 (valid_old
/// always 0 for new-format), so result = `0*10 + 2 = 2`.
fn fc_hstore_version_diag(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = arg_hstore(fcinfo, 0);
    fcinfo.isnull = false;
    Datum::from_i32(2)
}

/// Install this unit's inward seams: register the `hstore` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    // hstore subscripting execution bodies (hstore_subs.c).
    subs::init_seams();
}
