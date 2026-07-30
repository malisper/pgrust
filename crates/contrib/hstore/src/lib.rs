//! contrib/hstore — the hstore key/value type: I/O, operators, btree/hash
//! support, json/jsonb conversion, record conversion, SRFs, subscripting,
//! and the gin_hstore_ops / gist_hstore_ops opclasses.
//!
//! Functions dispatch through the dfmgr builtin-library registry. The GIN
//! core dispatches gin_hstore_ops by enum through gin_hstore_seams (the
//! gin_* symbols exist only for CREATE EXTENSION's C-symbol validation);
//! GiST support procs run as real fmgr calls (pg_trgm precedent).
//! Subscripting executes through hstore_subs_seams (parse/exec plumbing in
//! parse_expr / execexpr, jsonb precedent). Only new-format (bit-31-flagged)
//! images are ever produced; hstore_compat.c's old-format upgrade path has
//! no producer here.

pub mod gin;
pub mod gist;
pub mod jsonx;
pub mod records;
pub mod repr;
pub mod srf;
pub mod subs;

mod parse;

use datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
};
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use repr::{
    build_hstore, find_key, unique_pairs, HstoreView, Pair, HSTORE_MAX_KEY_LEN,
    HSTORE_MAX_VALUE_LEN,
};

const LIBRARY: &str = "hstore";

pub(crate) fn check_key_len(len: usize) -> PgResult<usize> {
    if len > HSTORE_MAX_KEY_LEN {
        return Err(PgError::error("string too long for hstore key")
            .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
            .into());
    }
    Ok(len)
}

pub(crate) fn check_val_len(len: usize) -> PgResult<usize> {
    if len > HSTORE_MAX_VALUE_LEN {
        return Err(PgError::error("string too long for hstore value")
            .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
            .into());
    }
    Ok(len)
}

// PG_GETARG_HSTORE_P: VARDATA_ANY of the detoasted arg image.
// SAFETY wrapper: callers assert arg i is a non-null hstore varlena.
pub(crate) unsafe fn arg_hstore<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<HstoreView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(HstoreView::from_vardata(v.data()))
}

pub(crate) unsafe fn arg_text<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    Ok(unsafe { fcinfo.arg_varlena_packed(i)? }.data())
}

// Full header-ful image of a varlena arg (arrays keep their dims header).
pub(crate) unsafe fn arg_image<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    crate::gist::detoasted_image(mcx, fcinfo.arg(i))
}

fn ret_hstore(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

pub(crate) fn ret_hstore_pub(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    ret_hstore(fcinfo, img)
}

fn ret_text(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        payload,
    )?))
}

pub(crate) fn ret_null(fcinfo: &mut Fcinfo) -> Datum {
    fcinfo.isnull = true;
    Datum::null()
}

pub(crate) fn ret_array(fcinfo: &Fcinfo, img: mcx::PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    d
}

// deconstruct_array_builtin(a, TEXTOID): payload bytes per element.
pub(crate) fn deconstruct_text_array<'m>(
    mcx: mcx::Mcx<'m>,
    image: &[u8],
) -> PgResult<Vec<Option<Vec<u8>>>> {
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, image, types_core::TEXTOID, true)?;
    Ok(elems
        .iter()
        .zip(nulls.iter())
        .map(|(d, &isnull)| {
            (!isnull).then(|| {
                let p = d.as_usize() as *const u8;
                // SAFETY: non-null text element datum inside the array image.
                unsafe {
                    let total = types_tuple::varatt::varsize_any(p);
                    let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
                    core::slice::from_raw_parts(p.add(hdr), total - hdr).to_vec()
                }
            })
        })
        .collect())
}

fn build_text_array_1d<'m>(
    mcx: mcx::Mcx<'m>,
    elems: &[Option<Vec<u8>>],
) -> PgResult<mcx::PgVec<'m, u8>> {
    if elems.is_empty() {
        return arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID);
    }
    let mut datums: Vec<Datum> = Vec::with_capacity(elems.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(elems.len());
    for e in elems {
        match e {
            Some(b) => {
                datums.push(varlena_result(varlena::cstring_to_text(mcx, b)?));
                nulls.push(false);
            }
            None => {
                datums.push(Datum::null());
                nulls.push(true);
            }
        }
    }
    arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        Some(&nulls),
        1,
        &[elems.len() as i32],
        &[1],
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )
}

// ===========================================================================
// I/O (hstore_io.c).
// ===========================================================================

fn fc_hstore_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    match parse::parse_hstore(&s) {
        Ok(pairs) => {
            let pairs = unique_pairs(pairs);
            ret_hstore(fcinfo, &build_hstore(&pairs))
        }
        Err(e) => {
            // SAFETY: context, if set, rides per the ErrorSaveNode contract.
            let esc = unsafe { fcinfo.soft_error_context() };
            types_error::ereturn(esc, ret_null(fcinfo), *e)
        }
    }
}

// cpw (hstore_io.c): copy, backslash-escaping `"` and `\`.
fn cpw(dst: &mut Vec<u8>, src: &[u8]) {
    for &b in src {
        if b == b'"' || b == b'\\' {
            dst.push(b'\\');
        }
        dst.push(b);
    }
}

fn fc_hstore_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null hstore varlena (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mut out: Vec<u8> = Vec::new();
    for i in 0..hs.count() {
        out.push(b'"');
        cpw(&mut out, hs.key(i));
        out.extend_from_slice(b"\"=>");
        if hs.val_isnull(i) {
            out.extend_from_slice(b"NULL");
        } else {
            out.push(b'"');
            cpw(&mut out, hs.val(i));
            out.push(b'"');
        }
        if i + 1 != hs.count() {
            out.extend_from_slice(b", ");
        }
    }
    out.push(0);
    let mcx = fcinfo.result_mcx();
    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, out.len())?;
    mcx::vec_append_bytes(&mut buf, &out)?;
    Ok(cstring_result(buf))
}

fn fc_hstore_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let pcount = pqformat::pq_getmsgint(buf, 4)? as i32;
    if pcount == 0 {
        return ret_hstore(fcinfo, &build_hstore(&[]));
    }
    if pcount < 0 || pcount as usize > (isize::MAX as usize) / core::mem::size_of::<Pair>() {
        return Err(PgError::error(format!("invalid number of pairs: {pcount}")).into());
    }
    let mut pairs: Vec<Pair> = Vec::with_capacity(pcount as usize);
    for _ in 0..pcount {
        let rawlen = pqformat::pq_getmsgint(buf, 4)? as i32;
        if rawlen < 0 {
            return Err(PgError::error("null value not allowed for hstore key")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .into());
        }
        // C (hstore_io.c hstore_recv) reads keys/values with pq_getmsgtext,
        // which runs pg_client_to_server -> pg_verify_mbstr and so rejects
        // invalid encoding and embedded NULs. Raw pq_getmsgbytes accepted
        // bytes C refuses (proofs finding #3, validation-bypass class).
        let key = pqformat::pq_getmsgtext(fcinfo.result_mcx(), buf, rawlen as usize)?.to_vec();
        check_key_len(key.len())?;
        let rawlen = pqformat::pq_getmsgint(buf, 4)? as i32;
        let val = if rawlen < 0 {
            None
        } else {
            let v = pqformat::pq_getmsgtext(fcinfo.result_mcx(), buf, rawlen as usize)?.to_vec();
            check_val_len(v.len())?;
            Some(v)
        };
        pairs.push(Pair { key, val, needfree: true });
    }
    let pairs = unique_pairs(pairs);
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

fn fc_hstore_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null hstore varlena (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&(hs.count() as i32).to_be_bytes());
    for i in 0..hs.count() {
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
    ret_text(fcinfo, &buf)
}

fn fc_hstore_version_diag(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // New-format only: valid_old always 0, valid_new always true => 2.
    Ok(Datum::from_i32(2))
}

// ===========================================================================
// Constructors (hstore_io.c).
// ===========================================================================

// hstore(text, text); NOT STRICT.
fn fc_hstore_from_text(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    if a.isnull {
        return Ok(ret_null(fcinfo));
    }
    let b_null = b.isnull;
    // SAFETY: checked non-null above / below.
    let key = unsafe { arg_text(fcinfo, 0)? }.to_vec();
    check_key_len(key.len())?;
    let val = if b_null {
        None
    } else {
        // SAFETY: checked non-null.
        let v = unsafe { arg_text(fcinfo, 1)? }.to_vec();
        check_val_len(v.len())?;
        Some(v)
    };
    ret_hstore(fcinfo, &build_hstore(&[Pair { key, val, needfree: false }]))
}

// hstore(text[], text[]); NOT STRICT.
fn fc_hstore_from_arrays(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    if a.isnull {
        return Ok(ret_null(fcinfo));
    }
    let b_null = b.isnull;
    let scratch = mcx::MemoryContext::new("hstore_from_arrays");
    let mcx = scratch.mcx();
    // SAFETY: checked non-null.
    let keys = deconstruct_text_array(mcx, unsafe { arg_image(fcinfo, 0)? })?;
    let vals = if b_null {
        None
    } else {
        // SAFETY: checked non-null.
        let v = deconstruct_text_array(mcx, unsafe { arg_image(fcinfo, 1)? })?;
        if v.len() != keys.len() {
            return Err(PgError::error("arrays must have same bounds")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .into());
        }
        Some(v)
    };
    let mut pairs: Vec<Pair> = Vec::with_capacity(keys.len());
    for (i, k) in keys.into_iter().enumerate() {
        let key = k.ok_or_else(null_key_err)?;
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
        pairs.push(Pair { key, val, needfree: false });
    }
    let pairs = unique_pairs(pairs);
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

#[track_caller]
#[cold]
fn null_key_err() -> Box<PgError> {
    Box::new(
        PgError::error("null value not allowed for hstore key")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

// hstore(text[]): 1-D even-length or 2-D Nx2; STRICT.
fn fc_hstore_from_array(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let scratch = mcx::MemoryContext::new("hstore_from_array");
    let mcx = scratch.mcx();
    // SAFETY: catalog arg is a non-null text[] (strict fn).
    let image = unsafe { arg_image(fcinfo, 0)? };
    let ndim = arrayfuncs::foundation::arr_ndim(image);
    match ndim {
        0 => return ret_hstore(fcinfo, &build_hstore(&[])),
        1 => {
            if (arrayfuncs::foundation::arr_dim(image, 0) % 2) != 0 {
                return Err(PgError::error("array must have even number of elements")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                    .into());
            }
        }
        2 => {
            if arrayfuncs::foundation::arr_dim(image, 1) != 2 {
                return Err(PgError::error("array must have two columns")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                    .into());
            }
        }
        _ => {
            return Err(PgError::error("wrong number of array subscripts")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                .into())
        }
    }
    let elems = deconstruct_text_array(mcx, image)?;
    let count = elems.len() / 2;
    let mut pairs: Vec<Pair> = Vec::with_capacity(count);
    for i in 0..count {
        let key = elems[i * 2].clone().ok_or_else(null_key_err)?;
        check_key_len(key.len())?;
        let val = match &elems[i * 2 + 1] {
            None => None,
            Some(v) => {
                check_val_len(v.len())?;
                Some(v.clone())
            }
        };
        pairs.push(Pair { key, val, needfree: false });
    }
    let pairs = unique_pairs(pairs);
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

// ===========================================================================
// Operators (hstore_op.c).
// ===========================================================================

fn fc_hstore_fetchval(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let key = unsafe { arg_text(fcinfo, 1)? };
    match find_key(&hs, None, key) {
        Some(idx) if !hs.val_isnull(idx) => ret_text(fcinfo, hs.val(idx)),
        _ => Ok(ret_null(fcinfo)),
    }
}

fn fc_hstore_exists(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let key = unsafe { arg_text(fcinfo, 1)? };
    Ok(Datum::from_bool(find_key(&hs, None, key).is_some()))
}

fn fc_hstore_defined(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let key = unsafe { arg_text(fcinfo, 1)? };
    let res = matches!(find_key(&hs, None, key), Some(idx) if !hs.val_isnull(idx));
    Ok(Datum::from_bool(res))
}

// hstoreArrayToPairs: sorted unique non-null keys.
pub(crate) fn array_to_keys(image: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("hstore text[] keys");
    let elems = deconstruct_text_array(scratch.mcx(), image)?;
    let pairs: Vec<Pair> = elems
        .into_iter()
        .flatten()
        .map(|k| Pair { key: k, val: None, needfree: false })
        .collect();
    Ok(unique_pairs(pairs).into_iter().map(|p| p.key).collect())
}

fn exists_n(fcinfo: &Fcinfo, all: bool) -> PgResult<bool> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let keys = array_to_keys(unsafe { arg_image(fcinfo, 1)? })?;
    let mut lowbound = 0usize;
    for k in &keys {
        let found = find_key(&hs, Some(&mut lowbound), k).is_some();
        if all && !found {
            return Ok(false);
        }
        if !all && found {
            return Ok(true);
        }
    }
    Ok(all)
}

fn fc_hstore_exists_any(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(exists_n(fcinfo, false)?))
}

fn fc_hstore_exists_all(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(exists_n(fcinfo, true)?))
}

fn fc_hstore_delete(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let key = unsafe { arg_text(fcinfo, 1)? };
    let mut pairs = hs.to_pairs();
    pairs.retain(|p| p.key != key);
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

fn fc_hstore_delete_array(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let del = array_to_keys(unsafe { arg_image(fcinfo, 1)? })?;
    let pairs: Vec<Pair> = hs
        .to_pairs()
        .into_iter()
        .filter(|p| {
            let probe = HstoreProbe(&del);
            !probe.contains(&p.key)
        })
        .collect();
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

// del is (keylen,key)-sorted; binary search matches C's find loop.
struct HstoreProbe<'a>(&'a [Vec<u8>]);
impl HstoreProbe<'_> {
    fn contains(&self, key: &[u8]) -> bool {
        self.0
            .binary_search_by(|k| match k.len().cmp(&key.len()) {
                core::cmp::Ordering::Equal => k.as_slice().cmp(key),
                other => other,
            })
            .is_ok()
    }
}

fn fc_hstore_delete_hstore(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let hs2 = unsafe { arg_hstore(fcinfo, 1)? };
    let pairs: Vec<Pair> = (0..hs.count())
        .filter_map(|i| {
            if let Some(j) = find_key(&hs2, None, hs.key(i)) {
                let nullval = hs.val_isnull(i);
                if nullval == hs2.val_isnull(j) && (nullval || hs.val(i) == hs2.val(j)) {
                    return None;
                }
            }
            Some(hs.pair(i))
        })
        .collect();
    ret_hstore(fcinfo, &build_hstore(&pairs))
}

pub(crate) fn concat_pairs(s1: &HstoreView<'_>, s2: &HstoreView<'_>) -> Vec<Pair> {
    let mut out: Vec<Pair> = Vec::with_capacity(s1.count() + s2.count());
    let (mut i, mut j) = (0usize, 0usize);
    while i < s1.count() || j < s2.count() {
        let diff = if i >= s1.count() {
            1
        } else if j >= s2.count() {
            -1
        } else {
            let (k1, k2) = (s1.key(i), s2.key(j));
            match k1.len().cmp(&k2.len()) {
                core::cmp::Ordering::Equal => match k1.cmp(k2) {
                    core::cmp::Ordering::Less => -1,
                    core::cmp::Ordering::Equal => 0,
                    core::cmp::Ordering::Greater => 1,
                },
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Greater => 1,
            }
        };
        if diff >= 0 {
            out.push(s2.pair(j));
            j += 1;
            if diff == 0 {
                i += 1;
            }
        } else {
            out.push(s1.pair(i));
            i += 1;
        }
    }
    out
}

fn fc_hstore_concat(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let s1 = unsafe { arg_hstore(fcinfo, 0)? };
    let s2 = unsafe { arg_hstore(fcinfo, 1)? };
    let out = concat_pairs(&s1, &s2);
    ret_hstore(fcinfo, &build_hstore(&out))
}

fn contains(val: &HstoreView<'_>, tmpl: &HstoreView<'_>) -> bool {
    let mut lastidx = 0usize;
    for i in 0..tmpl.count() {
        match find_key(val, Some(&mut lastidx), tmpl.key(i)) {
            Some(idx) => {
                let nullval = tmpl.val_isnull(i);
                if !(nullval == val.val_isnull(idx) && (nullval || tmpl.val(i) == val.val(idx))) {
                    return false;
                }
            }
            None => return false,
        }
    }
    true
}

fn fc_hstore_contains(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let (a, b) = unsafe { (arg_hstore(fcinfo, 0)?, arg_hstore(fcinfo, 1)?) };
    Ok(Datum::from_bool(contains(&a, &b)))
}

fn fc_hstore_contained(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let (a, b) = unsafe { (arg_hstore(fcinfo, 0)?, arg_hstore(fcinfo, 1)?) };
    Ok(Datum::from_bool(contains(&b, &a)))
}

fn fc_hstore_slice_to_hstore(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let keys = array_to_keys(unsafe { arg_image(fcinfo, 1)? })?;
    let mut lastidx = 0usize;
    let mut out: Vec<Pair> = Vec::new();
    for k in keys {
        if let Some(idx) = find_key(&hs, Some(&mut lastidx), &k) {
            out.push(Pair {
                key: k,
                val: (!hs.val_isnull(idx)).then(|| hs.val(idx).to_vec()),
                needfree: false,
            });
        }
    }
    ret_hstore(fcinfo, &build_hstore(&out))
}

fn fc_hstore_slice_to_array(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let key_image = unsafe { arg_image(fcinfo, 1)? };
    let mcx = fcinfo.result_mcx();
    let keys = deconstruct_text_array(mcx, key_image)?;

    let ndim = arrayfuncs::foundation::arr_ndim(key_image);
    if ndim <= 0 || keys.is_empty() {
        return Ok(ret_array(
            fcinfo,
            arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID)?,
        ));
    }
    let mut datums: Vec<Datum> = Vec::with_capacity(keys.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(keys.len());
    for kopt in &keys {
        let hit = kopt.as_deref().and_then(|k| find_key(&hs, None, k));
        match hit {
            Some(i) if !hs.val_isnull(i) => {
                datums.push(varlena_result(varlena::cstring_to_text(mcx, hs.val(i))?));
                nulls.push(false);
            }
            _ => {
                datums.push(Datum::null());
                nulls.push(true);
            }
        }
    }
    let mut dims = [0i32; 6];
    let mut lbs = [0i32; 6];
    for d in 0..ndim as usize {
        dims[d] = arrayfuncs::foundation::arr_dim(key_image, d);
        lbs[d] = arrayfuncs::foundation::arr_lbound(key_image, d);
    }
    let img = arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        Some(&nulls),
        ndim,
        &dims[..ndim as usize],
        &lbs[..ndim as usize],
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )?;
    Ok(ret_array(fcinfo, img))
}

fn fc_hstore_akeys(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let elems: Vec<Option<Vec<u8>>> = (0..hs.count()).map(|i| Some(hs.key(i).to_vec())).collect();
    let img = build_text_array_1d(fcinfo.result_mcx(), &elems)?;
    Ok(ret_array(fcinfo, img))
}

fn fc_hstore_avals(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let elems: Vec<Option<Vec<u8>>> = (0..hs.count())
        .map(|i| (!hs.val_isnull(i)).then(|| hs.val(i).to_vec()))
        .collect();
    let img = build_text_array_1d(fcinfo.result_mcx(), &elems)?;
    Ok(ret_array(fcinfo, img))
}

fn fc_hstore_to_array(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    hstore_to_array_common(fcinfo, 1)
}

fn fc_hstore_to_matrix(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    hstore_to_array_common(fcinfo, 2)
}

// hstore_to_array_internal: 1-D flat [k,v,...] or 2-D Nx2.
fn hstore_to_array_common(fcinfo: &mut Fcinfo, ndims: i32) -> PgResult<Datum> {
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let count = hs.count();
    let mcx = fcinfo.result_mcx();
    if count == 0 {
        return Ok(ret_array(
            fcinfo,
            arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID)?,
        ));
    }
    let mut datums: Vec<Datum> = Vec::with_capacity(count * 2);
    let mut nulls: Vec<bool> = Vec::with_capacity(count * 2);
    for i in 0..count {
        datums.push(varlena_result(varlena::cstring_to_text(mcx, hs.key(i))?));
        nulls.push(false);
        if hs.val_isnull(i) {
            datums.push(Datum::null());
            nulls.push(true);
        } else {
            datums.push(varlena_result(varlena::cstring_to_text(mcx, hs.val(i))?));
            nulls.push(false);
        }
    }
    let (dims, lbs): (&[i32], &[i32]) = if ndims == 1 {
        (&[count as i32 * 2], &[1])
    } else {
        (&[count as i32, 2], &[1, 1])
    };
    let img = arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        Some(&nulls),
        ndims,
        dims,
        lbs,
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )?;
    Ok(ret_array(fcinfo, img))
}

// ===========================================================================
// btree / hash support (hstore_op.c).
// ===========================================================================

pub(crate) fn hstore_cmp(hs1: &HstoreView<'_>, hs2: &HstoreView<'_>) -> i32 {
    let (c1, c2) = (hs1.count(), hs2.count());
    if c1 == 0 || c2 == 0 {
        return if c1 > 0 {
            1
        } else if c2 > 0 {
            -1
        } else {
            0
        };
    }
    let (len1, len2) = (hs1.pool_len(), hs2.pool_len());
    let minlen = len1.min(len2);
    use core::cmp::Ordering;
    match hs1.pool_bytes()[..minlen].cmp(&hs2.pool_bytes()[..minlen]) {
        Ordering::Greater => 1,
        Ordering::Less => -1,
        Ordering::Equal => {
            if len1 != len2 {
                return if len1 > len2 { 1 } else { -1 };
            }
            if c1 != c2 {
                return if c1 > c2 { 1 } else { -1 };
            }
            for i in 0..c1 * 2 {
                if hs1.raw_endpos(i) != hs2.raw_endpos(i)
                    || hs1.raw_isnull(i) != hs2.raw_isnull(i)
                {
                    if hs1.raw_endpos(i) < hs2.raw_endpos(i) {
                        return -1;
                    } else if hs1.raw_endpos(i) > hs2.raw_endpos(i) {
                        return 1;
                    } else if hs1.raw_isnull(i) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            }
            0
        }
    }
}

macro_rules! fc_hstore_cmp_op {
    ($($fname:ident: $pred:expr;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null (strict fn).
            let (a, b) = unsafe { (arg_hstore(fcinfo, 0)?, arg_hstore(fcinfo, 1)?) };
            Ok(Datum::from_bool(($pred)(hstore_cmp(&a, &b))))
        }
    )*};
}

fc_hstore_cmp_op! {
    fc_hstore_eq: |r: i32| r == 0;
    fc_hstore_ne: |r: i32| r != 0;
    fc_hstore_gt: |r: i32| r > 0;
    fc_hstore_ge: |r: i32| r >= 0;
    fc_hstore_lt: |r: i32| r < 0;
    fc_hstore_le: |r: i32| r <= 0;
}

fn fc_hstore_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null (strict fn).
    let (a, b) = unsafe { (arg_hstore(fcinfo, 0)?, arg_hstore(fcinfo, 1)?) };
    Ok(Datum::from_i32(hstore_cmp(&a, &b)))
}

fn fc_hstore_hash(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    Ok(Datum::from_u32(hashfn::hash_bytes(hs.vardata())))
}

fn fc_hstore_hash_extended(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(hashfn::hash_bytes_extended(
        hs.vardata(),
        seed.value.as_u64(),
    )))
}

// ===========================================================================
// Registration.
// ===========================================================================

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "hstore_in" => fc_hstore_in,
        "hstore_out" => fc_hstore_out,
        "hstore_recv" => fc_hstore_recv,
        "hstore_send" => fc_hstore_send,
        "hstore_version_diag" => fc_hstore_version_diag,
        "hstore_from_text" => fc_hstore_from_text,
        "hstore_from_arrays" => fc_hstore_from_arrays,
        "hstore_from_array" => fc_hstore_from_array,
        "hstore_from_record" => records::fc_hstore_from_record,
        "hstore_fetchval" => fc_hstore_fetchval,
        "hstore_exists" => fc_hstore_exists,
        "hstore_exists_any" => fc_hstore_exists_any,
        "hstore_exists_all" => fc_hstore_exists_all,
        "hstore_defined" => fc_hstore_defined,
        "hstore_delete" => fc_hstore_delete,
        "hstore_delete_array" => fc_hstore_delete_array,
        "hstore_delete_hstore" => fc_hstore_delete_hstore,
        "hstore_concat" => fc_hstore_concat,
        "hstore_contains" => fc_hstore_contains,
        "hstore_contained" => fc_hstore_contained,
        "hstore_slice_to_array" => fc_hstore_slice_to_array,
        "hstore_slice_to_hstore" => fc_hstore_slice_to_hstore,
        "hstore_akeys" => fc_hstore_akeys,
        "hstore_avals" => fc_hstore_avals,
        "hstore_to_array" => fc_hstore_to_array,
        "hstore_to_matrix" => fc_hstore_to_matrix,
        "hstore_populate_record" => records::fc_hstore_populate_record,
        "hstore_skeys" => srf::fc_hstore_skeys,
        "hstore_svals" => srf::fc_hstore_svals,
        "hstore_each" => srf::fc_hstore_each,
        "hstore_to_json" => jsonx::fc_hstore_to_json,
        "hstore_to_json_loose" => jsonx::fc_hstore_to_json_loose,
        "hstore_to_jsonb" => jsonx::fc_hstore_to_jsonb,
        "hstore_to_jsonb_loose" => jsonx::fc_hstore_to_jsonb_loose,
        "hstore_eq" => fc_hstore_eq,
        "hstore_ne" => fc_hstore_ne,
        "hstore_gt" => fc_hstore_gt,
        "hstore_ge" => fc_hstore_ge,
        "hstore_lt" => fc_hstore_lt,
        "hstore_le" => fc_hstore_le,
        "hstore_cmp" => fc_hstore_cmp,
        "hstore_hash" => fc_hstore_hash,
        "hstore_hash_extended" => fc_hstore_hash_extended,
        "ghstore_in" => gist::fc_ghstore_in,
        "ghstore_out" => gist::fc_ghstore_out,
        "ghstore_compress" => gist::fc_ghstore_compress,
        "ghstore_decompress" => gist::fc_ghstore_decompress,
        "ghstore_penalty" => gist::fc_ghstore_penalty,
        "ghstore_picksplit" => gist::fc_ghstore_picksplit,
        "ghstore_union" => gist::fc_ghstore_union,
        "ghstore_same" => gist::fc_ghstore_same,
        "ghstore_consistent" => gist::fc_ghstore_consistent,
        "ghstore_options" => gist::fc_ghstore_options,
        "gin_extract_hstore" => gin::fc_gin_extract_hstore,
        "gin_extract_hstore_query" => gin::fc_gin_extract_hstore_query,
        "gin_consistent_hstore" => gin::fc_gin_consistent_hstore,
        "hstore_subscript_handler" => subs::fc_hstore_subscript_handler,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    gin_hstore_seams::hstore_extract_value::set(gin::extract_value);
    gin_hstore_seams::hstore_extract_query::set(gin::extract_query);
    gin_hstore_seams::hstore_consistent::set(gin::consistent);
    hstore_subs_seams::hstore_subs_fetch::set(subs::fetch);
    hstore_subs_seams::hstore_subs_assign::set(subs::assign);
}
