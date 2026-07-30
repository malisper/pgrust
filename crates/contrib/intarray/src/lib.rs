//! contrib/intarray — integer-array operators/functions, the query_int
//! boolean-search type, and the gist__int_ops / gist__intbig_ops /
//! gin__int_ops opclasses. Scalars + GiST procs dispatch through the dfmgr
//! builtin-library registry (library `_int`, GISTENTRY fmgr protocol,
//! pg_trgm/ltree precedent); the GIN core dispatches its closed opclass set
//! by enum and calls the gin_int4_seams installed here (the ginint4_*
//! symbols below exist for CREATE EXTENSION's C-symbol validation only).
//! The planner's selectivity estimators (_int_selfuncs.c) live in the
//! planner crate's closed-set oprrest/oprjoin dispatch; their symbols here
//! are validation stubs too.

pub mod boolquery;
pub mod gist;
pub mod gistbig;
pub mod tool;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{byref_result, cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_gist::GISTENTRY;
use types_tuple::varatt;

use boolquery::VAL;
use tool::IntArray;

const LIBRARY: &str = "_int";

pub(crate) const RT_OVERLAP: u16 = 3;
pub(crate) const RT_SAME: u16 = 6;
pub(crate) const RT_CONTAINS: u16 = 7;
pub(crate) const RT_CONTAINED_BY: u16 = 8;
pub(crate) const RT_OLD_CONTAINS: u16 = 13;
pub(crate) const RT_OLD_CONTAINED_BY: u16 = 14;
pub(crate) const BOOLEAN_SEARCH_STRATEGY: u16 = 20;

const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
const GIN_SEARCH_MODE_ALL: i32 = 2;

// ===========================================================================
// fmgr protocol helpers (pg_trgm precedent).
// ===========================================================================

pub(crate) unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

pub(crate) fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

pub(crate) fn image_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

pub(crate) fn is_plain_4b(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe { varatt::varatt_is_4b_u(p) }
}

pub(crate) fn key_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: index keys are plain 4B-header images built by this module.
    unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) }
}

pub(crate) fn detoasted_image<'m>(mcx: mcx::Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf: mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
            mcx::vec_append_bytes(&mut buf, &varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, raw)?;
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        }
    }
}

pub(crate) fn opclass_option_i32(f: &Option<&mut FmgrInfo>, default: i32) -> i32 {
    match f.as_ref().and_then(|f| f.opclass_options()) {
        Some(img) => i32::from_ne_bytes(img[4..8].try_into().unwrap()),
        None => default,
    }
}

/// PG_GETARG_ARRAYTYPE_P(_COPY): owned full-image copy of an array arg.
unsafe fn arg_array(fcinfo: &Fcinfo, i: usize) -> PgResult<IntArray> {
    Ok(IntArray::from_image(&unsafe { arg_image(fcinfo, i) }?))
}

/// Full 4B image of a varlena arg (arrays, query_int values).
unsafe fn arg_image(fcinfo: &Fcinfo, i: usize) -> PgResult<Vec<u8>> {
    // SAFETY: forwarded caller contract — non-null varlena arg (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let payload = v.data();
    let mut img = Vec::with_capacity(payload.len() + 4);
    img.extend_from_slice(&varatt::set_varsize_4b_word((payload.len() + 4) as u32).to_ne_bytes());
    img.extend_from_slice(payload);
    Ok(img)
}

fn ret_array(fcinfo: &Fcinfo, a: &IntArray) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), a.as_bytes())
}

fn ret_cstring(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    let mut v: mcx::PgVec<'_, u8> =
        mcx::vec_with_capacity_in(fcinfo.result_mcx(), payload.len() + 1)?;
    mcx::vec_append_bytes(&mut v, payload)?;
    v.push(0);
    Ok(cstring_result(v))
}

// ===========================================================================
// query_int I/O and boolean operators (_int_bool.c).
// ===========================================================================

fn fc_bqarr_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    match boolquery::parse_query(s.to_bytes()) {
        Ok(img) => image_result(fcinfo, &img),
        // Only C's four ereturn sites are soft; stack-depth exhaustion stays
        // a hard error even under pg_input_error_info.
        Err(pe) if pe.soft => {
            // SAFETY: context, if set, rides per the ErrorSaveNode contract
            // for this call (pg_input_error_info path).
            let esc = unsafe { fcinfo.soft_error_context() };
            types_error::ereturn(esc, (), *pe.err)?;
            Ok(fcinfo.return_null())
        }
        Err(pe) => Err(pe.err),
    }
}

fn fc_bqarr_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    let items = boolquery::parse_items(&img);
    ret_cstring(fcinfo, &boolquery::deparse(&items)?)
}

fn fc_querytree(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("querytree is no longer implemented").into())
}

fn boolop_core(fcinfo: &Fcinfo, array_arg: usize, query_arg: usize) -> PgResult<Datum> {
    let mut val = unsafe { arg_array(fcinfo, array_arg)? };
    let qimg = unsafe { arg_image(fcinfo, query_arg)? };
    let items = boolquery::parse_items(&qimg);
    val.check_valid()?;
    val.prepare();
    if items.is_empty() {
        return Ok(Datum::from_bool(false));
    }
    let elems = val.elems();
    let result = boolquery::execute(&items, items.len() as i32 - 1, true, &mut |_, it| {
        boolquery::checkcondition_arr(elems, it.val)
    })?;
    Ok(Datum::from_bool(result))
}

fn fc_boolop(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    boolop_core(fcinfo, 0, 1)
}

fn fc_rboolop(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    boolop_core(fcinfo, 1, 0)
}

// ===========================================================================
// Array comparison operators (_int_op.c).
// ===========================================================================

fn fc_int_contains(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contains_core(fcinfo, 0, 1)
}

fn fc_int_contained(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contains_core(fcinfo, 1, 0)
}

fn contains_core(fcinfo: &Fcinfo, ai: usize, bi: usize) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, ai)? };
    let mut b = unsafe { arg_array(fcinfo, bi)? };
    a.check_valid()?;
    b.check_valid()?;
    a.prepare();
    b.prepare();
    Ok(Datum::from_bool(tool::inner_int_contains(a.elems(), b.elems())))
}

fn same_core(fcinfo: &Fcinfo) -> PgResult<bool> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let mut b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    if a.nelems() != b.nelems() {
        return Ok(false);
    }
    a.sort(true);
    b.sort(true);
    Ok(a.elems() == b.elems())
}

fn fc_int_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(same_core(fcinfo)?))
}

fn fc_int_different(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!same_core(fcinfo)?))
}

fn fc_int_overlap(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let mut b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    if a.is_empty() || b.is_empty() {
        return Ok(Datum::from_bool(false));
    }
    a.sort(true);
    b.sort(true);
    Ok(Datum::from_bool(tool::inner_int_overlap(a.elems(), b.elems())))
}

fn fc_int_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let mut b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    a.sort(true);
    b.sort(true);
    ret_array(fcinfo, &tool::inner_int_union(&a, &b)?)
}

fn fc_int_inter(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let mut b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    a.sort(true);
    b.sort(true);
    ret_array(fcinfo, &tool::inner_int_inter(&a, &b))
}

// ===========================================================================
// Utility functions (_int_op.c tail).
// ===========================================================================

fn fc_intset(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut r = IntArray::new(1);
    r.elems_mut()[0] = fcinfo.arg(0).as_i32();
    ret_array(fcinfo, &r)
}

fn fc_icount(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    Ok(Datum::from_i32(a.nelems() as i32))
}

fn fc_sort(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let dir_arg = if fcinfo.nargs() == 2 {
        // SAFETY: catalog arg is a non-null text varlena (strict fn).
        Some(unsafe { fcinfo.arg_varlena_packed(1)? }.data().to_vec())
    } else {
        None
    };
    a.check_valid()?;
    if a.nelems() < 2 {
        return ret_array(fcinfo, &a);
    }
    let dir = match &dir_arg {
        None => true,
        Some(d) if d.len() == 3 && d.eq_ignore_ascii_case(b"asc") => true,
        Some(d) if d.len() == 4 && d.eq_ignore_ascii_case(b"desc") => false,
        Some(_) => {
            return Err(PgError::error("second parameter must be \"ASC\" or \"DESC\"")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into())
        }
    };
    a.sort(dir);
    ret_array(fcinfo, &a)
}

fn fc_sort_asc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    a.check_valid()?;
    a.sort(true);
    ret_array(fcinfo, &a)
}

fn fc_sort_desc(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    a.check_valid()?;
    a.sort(false);
    ret_array(fcinfo, &a)
}

fn fc_uniq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    a.check_valid()?;
    if a.nelems() < 2 {
        return ret_array(fcinfo, &a);
    }
    a.unique();
    ret_array(fcinfo, &a)
}

fn fc_idx(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    a.check_valid()?;
    let elem = fcinfo.arg(1).as_i32();
    let mut result = a.nelems() as i32;
    if result != 0 {
        result = match a.elems().iter().position(|&v| v == elem) {
            Some(i) => i as i32 + 1,
            None => 0,
        };
    }
    Ok(Datum::from_i32(result))
}

fn fc_subarray(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    let mut start = fcinfo.arg(1).as_i32();
    let len = if fcinfo.nargs() == 3 { fcinfo.arg(2).as_i32() } else { 0 };

    start = if start > 0 { start - 1 } else { start };

    a.check_valid()?;
    if a.is_empty() {
        return ret_array(fcinfo, &IntArray::empty());
    }
    // All the range arithmetic wraps in int32 (C is built with -fwrapv).
    let c = a.nelems() as i32;
    if start < 0 {
        start = c.wrapping_add(start);
    }
    let mut end = if len < 0 {
        c.wrapping_add(len)
    } else if len == 0 {
        c
    } else {
        start.wrapping_add(len)
    };
    if end > c {
        end = c;
    }
    if start < 0 {
        start = 0;
    }
    if start >= end || end <= 0 {
        return ret_array(fcinfo, &IntArray::empty());
    }
    let mut result = IntArray::new((end - start) as usize);
    result
        .elems_mut()
        .copy_from_slice(&a.elems()[start as usize..end as usize]);
    ret_array(fcinfo, &result)
}

fn add_elem(a: &IntArray, elem: i32) -> PgResult<IntArray> {
    a.check_valid()?;
    let c = a.nelems();
    let mut result = IntArray::new(c + 1);
    if c > 0 {
        result.elems_mut()[..c].copy_from_slice(a.elems());
    }
    result.elems_mut()[c] = elem;
    Ok(result)
}

fn fc_intarray_push_elem(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    let result = add_elem(&a, fcinfo.arg(1).as_i32())?;
    ret_array(fcinfo, &result)
}

fn fc_intarray_push_array(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    let b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    let (ac, bc) = (a.nelems(), b.nelems());
    let mut result = IntArray::new(ac + bc);
    if ac > 0 {
        result.elems_mut()[..ac].copy_from_slice(a.elems());
    }
    if bc > 0 {
        result.elems_mut()[ac..].copy_from_slice(b.elems());
    }
    ret_array(fcinfo, &result)
}

fn fc_intarray_del_elem(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let elem = fcinfo.arg(1).as_i32();
    a.check_valid()?;
    if !a.is_empty() {
        let aa = a.elems_mut();
        let mut n = 0usize;
        for i in 0..aa.len() {
            if aa[i] != elem {
                if i > n {
                    aa[n] = aa[i];
                }
                n += 1;
            }
        }
        a.resize(n);
    }
    ret_array(fcinfo, &a)
}

fn fc_intset_union_elem(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_array(fcinfo, 0)? };
    let mut result = add_elem(&a, fcinfo.arg(1).as_i32())?;
    result.sort(true);
    result.unique();
    ret_array(fcinfo, &result)
}

fn fc_intset_subtract(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut a = unsafe { arg_array(fcinfo, 0)? };
    let mut b = unsafe { arg_array(fcinfo, 1)? };
    a.check_valid()?;
    b.check_valid()?;
    a.prepare();
    b.prepare();
    let (aa, bb) = (a.elems(), b.elems());
    let (ca, cb) = (aa.len(), bb.len());
    let mut result = IntArray::new(ca);
    let mut n = 0usize;
    {
        let r = result.elems_mut();
        let (mut i, mut k) = (0usize, 0usize);
        while i < ca {
            if k == cb || aa[i] < bb[k] {
                r[n] = aa[i];
                n += 1;
                i += 1;
            } else if aa[i] == bb[k] {
                i += 1;
                k += 1;
            } else {
                k += 1;
            }
        }
    }
    result.resize(n);
    ret_array(fcinfo, &result)
}

// ===========================================================================
// gin__int_ops cores (_int_gin.c), installed on gin_int4_seams.
// ===========================================================================

fn int4_extract_query(image: &[u8], strategy: u16) -> PgResult<(Vec<i32>, i32)> {
    if strategy == BOOLEAN_SEARCH_STRATEGY {
        let items = boolquery::parse_items(image);
        // Empty query must fail: C's NULL return with searchMode DEFAULT.
        if items.is_empty() {
            return Ok((Vec::new(), GIN_SEARCH_MODE_DEFAULT));
        }
        let search_mode = if boolquery::query_has_required_values(&items)? {
            GIN_SEARCH_MODE_DEFAULT
        } else {
            // No required primitive values (e.g. '!42'): full index scan.
            GIN_SEARCH_MODE_ALL
        };
        let entries = items.iter().filter(|it| it.typ == VAL as i16).map(|it| it.val).collect();
        Ok((entries, search_mode))
    } else {
        let arr = IntArray::from_image(image);
        arr.check_valid()?;
        let entries: Vec<i32> = arr.elems().to_vec();
        let search_mode = match strategy {
            RT_OVERLAP => GIN_SEARCH_MODE_DEFAULT,
            // empty set is contained in everything
            RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => GIN_SEARCH_MODE_INCLUDE_EMPTY,
            RT_SAME => {
                if entries.is_empty() {
                    GIN_SEARCH_MODE_INCLUDE_EMPTY
                } else {
                    GIN_SEARCH_MODE_DEFAULT
                }
            }
            // everything contains the empty set
            RT_CONTAINS | RT_OLD_CONTAINS => {
                if entries.is_empty() {
                    GIN_SEARCH_MODE_ALL
                } else {
                    GIN_SEARCH_MODE_DEFAULT
                }
            }
            other => {
                return Err(PgError::error(format!(
                    "ginint4_queryextract: unknown strategy number: {other}"
                ))
                .into())
            }
        };
        Ok((entries, search_mode))
    }
}

fn int4_consistent(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    query_image: &[u8],
) -> PgResult<(bool, bool)> {
    Ok(match strategy {
        // at least one element in check[] is true, so result = true
        RT_OVERLAP => (true, false),
        RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => (true, true),
        RT_SAME => ((0..nkeys).all(|i| check[i] != 0), true),
        RT_CONTAINS | RT_OLD_CONTAINS => ((0..nkeys).all(|i| check[i] != 0), false),
        BOOLEAN_SEARCH_STRATEGY => {
            let items = boolquery::parse_items(query_image);
            let boolcheck: Vec<bool> = check[..nkeys].iter().map(|&c| c != 0).collect();
            (boolquery::gin_bool_consistent(&items, &boolcheck)?, false)
        }
        other => {
            return Err(PgError::error(format!(
                "ginint4_consistent: unknown strategy number: {other}"
            ))
            .into())
        }
    })
}

#[track_caller]
#[cold]
fn gin_symbol_via_fmgr(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "intarray: {name} reached through a raw fmgr call — the GIN core dispatches \
         gin__int_ops through its opclass enum (gin_int4_seams), never via fmgr"
    )))
}

#[track_caller]
#[cold]
fn selfuncs_symbol_via_fmgr(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "intarray: {name} reached through a raw fmgr call — the planner dispatches \
         intarray estimators natively (plancat.rs oprrest/oprjoin fallback), never via fmgr"
    )))
}

macro_rules! fc_stub {
    ($($fname:ident: $err:ident($sym:literal);)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Err($err($sym))
        }
    )*};
}

fc_stub! {
    fc_ginint4_queryextract: gin_symbol_via_fmgr("ginint4_queryextract");
    fc_ginint4_consistent: gin_symbol_via_fmgr("ginint4_consistent");
    fc_int_matchsel: selfuncs_symbol_via_fmgr("_int_matchsel");
    fc_int_overlap_sel: selfuncs_symbol_via_fmgr("_int_overlap_sel");
    fc_int_contains_sel: selfuncs_symbol_via_fmgr("_int_contains_sel");
    fc_int_contained_sel: selfuncs_symbol_via_fmgr("_int_contained_sel");
    fc_int_overlap_joinsel: selfuncs_symbol_via_fmgr("_int_overlap_joinsel");
    fc_int_contains_joinsel: selfuncs_symbol_via_fmgr("_int_contains_joinsel");
    fc_int_contained_joinsel: selfuncs_symbol_via_fmgr("_int_contained_joinsel");
}

// ===========================================================================
// Registration.
// ===========================================================================

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "bqarr_in" => fc_bqarr_in,
        "bqarr_out" => fc_bqarr_out,
        "querytree" => fc_querytree,
        "boolop" => fc_boolop,
        "rboolop" => fc_rboolop,
        "_int_contains" => fc_int_contains,
        "_int_contained" => fc_int_contained,
        "_int_overlap" => fc_int_overlap,
        "_int_same" => fc_int_same,
        "_int_different" => fc_int_different,
        "_int_union" => fc_int_union,
        "_int_inter" => fc_int_inter,
        "intset" => fc_intset,
        "icount" => fc_icount,
        "sort" => fc_sort,
        "sort_asc" => fc_sort_asc,
        "sort_desc" => fc_sort_desc,
        "uniq" => fc_uniq,
        "idx" => fc_idx,
        "subarray" => fc_subarray,
        "intarray_push_elem" => fc_intarray_push_elem,
        "intarray_push_array" => fc_intarray_push_array,
        "intarray_del_elem" => fc_intarray_del_elem,
        "intset_union_elem" => fc_intset_union_elem,
        "intset_subtract" => fc_intset_subtract,
        "g_int_consistent" => gist::fc_g_int_consistent,
        "g_int_compress" => gist::fc_g_int_compress,
        "g_int_decompress" => gist::fc_g_int_decompress,
        "g_int_penalty" => gist::fc_g_int_penalty,
        "g_int_picksplit" => gist::fc_g_int_picksplit,
        "g_int_union" => gist::fc_g_int_union,
        "g_int_same" => gist::fc_g_int_same,
        "g_int_options" => gist::fc_g_int_options,
        "_intbig_in" => gistbig::fc_intbig_in,
        "_intbig_out" => gistbig::fc_intbig_out,
        "g_intbig_consistent" => gistbig::fc_g_intbig_consistent,
        "g_intbig_compress" => gistbig::fc_g_intbig_compress,
        "g_intbig_decompress" => gistbig::fc_g_intbig_decompress,
        "g_intbig_penalty" => gistbig::fc_g_intbig_penalty,
        "g_intbig_picksplit" => gistbig::fc_g_intbig_picksplit,
        "g_intbig_union" => gistbig::fc_g_intbig_union,
        "g_intbig_same" => gistbig::fc_g_intbig_same,
        "g_intbig_options" => gistbig::fc_g_intbig_options,
        "ginint4_queryextract" => fc_ginint4_queryextract,
        "ginint4_consistent" => fc_ginint4_consistent,
        "_int_matchsel" => fc_int_matchsel,
        "_int_overlap_sel" => fc_int_overlap_sel,
        "_int_contains_sel" => fc_int_contains_sel,
        "_int_contained_sel" => fc_int_contained_sel,
        "_int_overlap_joinsel" => fc_int_overlap_joinsel,
        "_int_contains_joinsel" => fc_int_contains_joinsel,
        "_int_contained_joinsel" => fc_int_contained_joinsel,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    gin_int4_seams::int4_extract_query::set(int4_extract_query);
    gin_int4_seams::int4_consistent::set(int4_consistent);
}

#[cfg(test)]
mod tests;
