//! contrib/ltree — the ltree/lquery/ltxtquery hierarchical-path types, their
//! operators, and the gist_ltree_ops / gist__ltree_ops opclasses, dispatched
//! through the dfmgr builtin-library registry (GISTENTRY fmgr protocol,
//! pg_trgm precedent). siglen is read from real opclass options.

mod array;
pub mod crc; // pub for the ltree_diff differential rail + exhaustive CRC sweep (jsonb-probe precedent)
mod gist;
mod io;
mod op;
mod repr;

use datum::Datum;
use types_error::{PgError, PgResult};
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

const LIBRARY: &str = "ltree";


// Full 4B-header image of a by-ref varlena arg; short/toasted forms are
// canonicalized because the repr walkers read VARSIZE = word >> 2.
unsafe fn arg_image<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<Vec<u8>> {
    // SAFETY: forwarded caller contract — non-null varlena arg (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let payload = v.data();
    let total = payload.len() + repr::VARHDRSZ;
    let mut img = vec![0u8; total];
    repr::set_varsize(&mut img, total);
    img[repr::VARHDRSZ..].copy_from_slice(payload);
    Ok(img)
}

unsafe fn arg_payload<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: forwarded caller contract — non-null varlena arg (strict fn).
    Ok(unsafe { fcinfo.arg_varlena_packed(i)? }.data())
}

fn ret_image(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

fn ret_text(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        payload,
    )?))
}

fn ret_cstring(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    let mut v: mcx::PgVec<'_, u8> =
        mcx::vec_with_capacity_in(fcinfo.result_mcx(), payload.len() + 1)?;
    mcx::vec_append_bytes(&mut v, payload)?;
    v.push(0);
    Ok(cstring_result(v))
}


macro_rules! fc_type_in {
    ($($fname:ident: $parse:path;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is cstring (typlen -2).
            let s = unsafe { fcinfo.arg_cstring(0) };
            match $parse(s.to_bytes()) {
                Ok(img) => ret_image(fcinfo, &img),
                Err(e) => {
                    // C softens ONLY its ereturn(escontext, ...) sites, and in
                    // these three parsers those raise exactly the four codes
                    // below (syntax; label-count / level-count / ltxtquery
                    // size; label too long; ltxtquery value-too-big,
                    // operand-too-long, word-too-long). Everything else
                    // reaches the client through ereport or elog no matter
                    // what escontext says: makepol's "stack too short" and the
                    // two "internal error in ... parser" arms (XX000),
                    // check_stack_depth (54001), and pg_mblen_cstr's invalid
                    // byte sequence (22021). Softening those made
                    // pg_input_is_valid() answer false where PostgreSQL
                    // raises. A whitelist keeps any new error path hard by
                    // default, which is C's default too.
                    const SOFT_ABLE: [types_error::SqlState; 4] = [
                        types_error::ERRCODE_SYNTAX_ERROR,
                        types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                        types_error::ERRCODE_NAME_TOO_LONG,
                        types_error::ERRCODE_INVALID_PARAMETER_VALUE,
                    ];
                    if !SOFT_ABLE.contains(&e.sqlstate) {
                        return Err(Box::new(e));
                    }
                    // SAFETY: context, if set, rides per the ErrorSaveNode
                    // contract for this call (pg_input_error_info path).
                    let esc = unsafe { fcinfo.soft_error_context() };
                    types_error::ereturn(esc, (), e)?;
                    Ok(fcinfo.return_null())
                }
            }
        }
    )*};
}

fc_type_in! {
    fc_ltree_in: io::parse_ltree;
    fc_lquery_in: io::parse_lquery;
    fc_ltxtq_in: io::parse_ltxtquery;
}

fn fc_ltree_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    ret_cstring(fcinfo, &io::deparse_ltree(&img))
}

fn fc_lquery_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    ret_cstring(fcinfo, &io::deparse_lquery(&img)?)
}

fn fc_ltxtq_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    let text = io::deparse_ltxtquery(&img)?;
    ret_cstring(fcinfo, &text)
}

// C's binary wire format (ltree_io.c / ltxtquery_io.c) carries a one-byte
// format version (currently 1): *_send prepends it, *_recv strips and checks
// it before the textual body.
const LTREE_WIRE_VERSION: u32 = 1;

macro_rules! fc_type_recv {
    ($($fname:ident: $parse:path, $typname:literal;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
            let buf = unsafe { fcinfo.arg_stringinfo(0) };
            let version = pqformat::pq_getmsgint(buf, 1)?;
            if version != LTREE_WIRE_VERSION {
                // C elog(ERROR): internal-error sqlstate.
                return Err(Box::new(PgError::error(format!(
                    "unsupported {} version number {version}",
                    $typname
                ))));
            }
            let scratch = mcx::MemoryContext::new("ltree recv");
            let remaining = buf.len().saturating_sub(buf.cursor);
            let txt = pqformat::pq_getmsgtext(scratch.mcx(), buf, remaining)?;
            let img = $parse(txt.as_slice())?;
            ret_image(fcinfo, &img)
        }
    )*};
}

fc_type_recv! {
    fc_ltree_recv: io::parse_ltree, "ltree";
    fc_lquery_recv: io::parse_lquery, "lquery";
    fc_ltxtq_recv: io::parse_ltxtquery, "ltxtquery";
}

// C *_send: pq_begintypsend, pq_sendint8(version), pq_sendtext(deparsed).
fn ret_send_versioned(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint8(&mut buf, LTREE_WIRE_VERSION as u8)?;
    pqformat::pq_sendtext(&mut buf, payload)?;
    Ok(varlena_result(pqformat::pq_endtypsend(buf)))
}

fn fc_ltree_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    ret_send_versioned(fcinfo, &io::deparse_ltree(&img))
}

fn fc_lquery_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    ret_send_versioned(fcinfo, &io::deparse_lquery(&img)?)
}

fn fc_ltxtq_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    let text = io::deparse_ltxtquery(&img)?;
    ret_send_versioned(fcinfo, &text)
}


fn cmp_args(fcinfo: &Fcinfo) -> PgResult<(Vec<u8>, Vec<u8>)> {
    Ok(unsafe { (arg_image(fcinfo, 0)?, arg_image(fcinfo, 1)?) })
}

fn fc_ltree_cmp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = cmp_args(fcinfo)?;
    Ok(Datum::from_i32(op::ltree_compare(&a, &b)))
}

macro_rules! fc_ltree_cmp_op {
    ($($fname:ident: $pred:expr;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = cmp_args(fcinfo)?;
            Ok(Datum::from_bool(($pred)(op::ltree_compare(&a, &b))))
        }
    )*};
}

fc_ltree_cmp_op! {
    fc_ltree_lt: |r: i32| r < 0;
    fc_ltree_le: |r: i32| r <= 0;
    fc_ltree_eq: |r: i32| r == 0;
    fc_ltree_ne: |r: i32| r != 0;
    fc_ltree_ge: |r: i32| r >= 0;
    fc_ltree_gt: |r: i32| r > 0;
}

fn fc_hash_ltree(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_image(fcinfo, 0)? };
    Ok(Datum::from_u32(op::hash_ltree(&a)))
}

fn fc_hash_ltree_extended(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_image(fcinfo, 0)? };
    let [_, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(op::hash_ltree_extended(&a, seed.value.as_u64())))
}


fn fc_nlevel(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_image(fcinfo, 0)? };
    Ok(Datum::from_i32(op::nlevel(&a)))
}

// ltree_isparent(p, c): p is an ancestor of c.
fn fc_ltree_isparent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (p, c) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::inner_isparent(&c, &p)))
}

fn fc_ltree_risparent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (c, p) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::inner_isparent(&c, &p)))
}

fn fc_subltree(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = unsafe { arg_image(fcinfo, 0)? };
    let img = op::inner_subltree(&t, fcinfo.arg_i32(1), fcinfo.arg_i32(2))?;
    ret_image(fcinfo, &img)
}

fn fc_subpath(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = unsafe { arg_image(fcinfo, 0)? };
    let len = (fcinfo.nargs() == 3).then(|| fcinfo.arg_i32(2));
    let img = op::subpath(&t, fcinfo.arg_i32(1), len)?;
    ret_image(fcinfo, &img)
}

fn fc_ltree_addltree(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = cmp_args(fcinfo)?;
    ret_image(fcinfo, &op::ltree_concat(&a, &b)?)
}

fn fc_ltree_addtext(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = unsafe { arg_image(fcinfo, 0)? };
    let b = unsafe { arg_payload(fcinfo, 1)? }.to_vec();
    let tmp = io::parse_ltree(&b)?;
    ret_image(fcinfo, &op::ltree_concat(&a, &tmp)?)
}

fn fc_ltree_textadd(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let b = unsafe { arg_payload(fcinfo, 0)? }.to_vec();
    let a = unsafe { arg_image(fcinfo, 1)? };
    let tmp = io::parse_ltree(&b)?;
    ret_image(fcinfo, &op::ltree_concat(&tmp, &a)?)
}

fn fc_ltree_index(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = cmp_args(fcinfo)?;
    let start = (fcinfo.nargs() == 3).then(|| fcinfo.arg_i32(2));
    Ok(Datum::from_i32(op::ltree_index(&a, &b, start)))
}

fn fc_lca(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let imgs: Vec<Vec<u8>> = (0..fcinfo.nargs())
        .map(|i| unsafe { arg_image(fcinfo, i) })
        .collect::<PgResult<_>>()?;
    let refs: Vec<&[u8]> = imgs.iter().map(|v| v.as_slice()).collect();
    match op::lca_inner(&refs) {
        Some(img) => ret_image(fcinfo, &img),
        None => Ok(fcinfo.return_null()),
    }
}

fn fc_text2ltree(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = unsafe { arg_payload(fcinfo, 0)? }.to_vec();
    ret_image(fcinfo, &io::parse_ltree(&s)?)
}

fn fc_ltree2text(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = unsafe { arg_image(fcinfo, 0)? };
    ret_text(fcinfo, &io::deparse_ltree(&img))
}

// C calls generic_restriction_selectivity with default 0.001; unreferenced by
// the opclasses since ltree 1.2.
fn fc_ltreeparentsel(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(0.001))
}


fn fc_ltq_regex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (tree, query) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::ltq_regex(&tree, &query)?))
}

fn fc_ltq_rregex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (query, tree) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::ltq_regex(&tree, &query)?))
}

fn lt_q_regex_core(tree: &[u8], qarr_img: &[u8]) -> PgResult<bool> {
    let arr = array::LtreeArray::parse(qarr_img);
    arr.check_1d_no_nulls()?;
    for q in arr.elements() {
        if op::ltq_regex(tree, q)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn fc_lt_q_regex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (tree, qarr) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(lt_q_regex_core(&tree, &qarr)?))
}

fn fc_lt_q_rregex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (qarr, tree) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(lt_q_regex_core(&tree, &qarr)?))
}

fn fc_ltxtq_exec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (tree, query) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::ltxtq_exec(&tree, &query)?))
}

fn fc_ltxtq_rexec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (query, tree) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(op::ltxtq_exec(&tree, &query)?))
}


fn array_iter_isparent(la: &[u8], query: &[u8], risparent: bool) -> PgResult<Option<Vec<u8>>> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        let matched = if risparent {
            op::inner_isparent(item, query)
        } else {
            op::inner_isparent(query, item)
        };
        if matched {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

fn array_iter_ltq(la: &[u8], query: &[u8]) -> PgResult<Option<Vec<u8>>> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        if op::ltq_regex(item, query)? {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

fn array_iter_ltxtq(la: &[u8], query: &[u8]) -> PgResult<Option<Vec<u8>>> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        if op::ltxtq_exec(item, query)? {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

macro_rules! fc_array_bool {
    ($($fname:ident: ($iter:ident, swapped=$swapped:literal $(, risparent=$ris:literal)?);)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (x, y) = cmp_args(fcinfo)?;
            let (la, query) = if $swapped { (y, x) } else { (x, y) };
            #[allow(unused_mut, unused_assignments)]
            let mut r = $iter(&la, &query $(, $ris)?)?;
            Ok(Datum::from_bool(r.is_some()))
        }
    )*};
}

fc_array_bool! {
    fc__ltree_isparent: (array_iter_isparent, swapped=false, risparent=false);
    fc__ltree_r_isparent: (array_iter_isparent, swapped=true, risparent=false);
    fc__ltree_risparent: (array_iter_isparent, swapped=false, risparent=true);
    fc__ltree_r_risparent: (array_iter_isparent, swapped=true, risparent=true);
    fc__ltq_regex: (array_iter_ltq, swapped=false);
    fc__ltq_rregex: (array_iter_ltq, swapped=true);
    fc__ltxtq_exec: (array_iter_ltxtq, swapped=false);
    fc__ltxtq_rexec: (array_iter_ltxtq, swapped=true);
}

macro_rules! fc_array_extract {
    ($($fname:ident: ($iter:ident $(, risparent=$ris:literal)?);)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (la, query) = cmp_args(fcinfo)?;
            match $iter(&la, &query $(, $ris)?)? {
                Some(item) => ret_image(fcinfo, &item),
                None => Ok(fcinfo.return_null()),
            }
        }
    )*};
}

fc_array_extract! {
    fc__ltree_extract_isparent: (array_iter_isparent, risparent=false);
    fc__ltree_extract_risparent: (array_iter_isparent, risparent=true);
    fc__ltq_extract_regex: (array_iter_ltq);
    fc__ltxtq_extract_exec: (array_iter_ltxtq);
}

fn lt_q_arr_core(tree_arr: &[u8], query_arr: &[u8]) -> PgResult<bool> {
    let qarr = array::LtreeArray::parse(query_arr);
    qarr.check_1d_no_nulls()?;
    for q in qarr.elements() {
        if array_iter_ltq(tree_arr, q)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn fc__lt_q_regex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (tree_arr, query_arr) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(lt_q_arr_core(&tree_arr, &query_arr)?))
}

fn fc__lt_q_rregex(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (query_arr, tree_arr) = cmp_args(fcinfo)?;
    Ok(Datum::from_bool(lt_q_arr_core(&tree_arr, &query_arr)?))
}

fn fc__lca(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let la = unsafe { arg_image(fcinfo, 0)? };
    let arr = array::LtreeArray::parse(&la);
    arr.check_1d_no_nulls()?;
    let items: Vec<&[u8]> = arr.elements().collect();
    match op::lca_inner(&items) {
        Some(img) => ret_image(fcinfo, &img),
        None => Ok(fcinfo.return_null()),
    }
}


// SAFETY helpers over the gist fmgr protocol (pg_trgm precedent).
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    let bytes = unsafe {
        core::slice::from_raw_parts((e as *const GISTENTRY).cast::<u8>(), core::mem::size_of::<GISTENTRY>())
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

fn detoasted_image<'m>(mcx: mcx::Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
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

fn key_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: ltree_gist keys are plain 4B-header images built by this module.
    unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) }
}

fn get_siglen(f: &Option<&mut FmgrInfo>, default: i32) -> usize {
    match f.as_ref().and_then(|f| f.opclass_options()) {
        Some(img) => i32::from_ne_bytes(
            img[gist::OFFSETOF_SIGLEN..gist::OFFSETOF_SIGLEN + 4].try_into().unwrap(),
        ) as usize,
        None => default as usize,
    }
}

fn fc_ltree_gist_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of type ltree_gist").into())
}

fn fc_ltree_gist_out(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of type ltree_gist").into())
}

fn fc_ltree_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let img: &[u8] = if entry.leafkey {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        detoasted_image(mcx, entry.key)?
    } else {
        key_image(entry.key)
    };
    match gist::ltree_compress(entry.leafkey, img, false)? {
        Some(new_key) => {
            let key = ret_image(fcinfo, &new_key)?;
            let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
            entry_result(fcinfo, &retval)
        }
        None => Ok(fcinfo.arg(0)),
    }
}

fn fc_ltree_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // ltree_decompress is a no-op passthrough in C.
    Ok(fcinfo.arg(0))
}

fn fc_ltree_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_SIGLEN_DEFAULT);
    let a = key_image(fcinfo.arg(0));
    let b = key_image(fcinfo.arg(1));
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = gist::ltree_same(a, b, siglen)? };
    Ok(fcinfo.arg(2))
}

fn entryvec_images(fcinfo: &Fcinfo) -> (usize, Vec<(Vec<u8>, bool)>) {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let n = entryvec.n as usize;
    // Index 0 is real for union; picksplit treats it as the placeholder slot.
    let entries = entryvec.vector[..n]
        .iter()
        .map(|e| (key_image(e.key).to_vec(), false))
        .collect();
    (n, entries)
}

fn fc_ltree_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_SIGLEN_DEFAULT);
    let (_, entries) = entryvec_images(fcinfo);
    let img = gist::ltree_union(&entries, siglen)?;
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = img.len() as i32 };
    ret_image(fcinfo, &img)
}

fn fc_ltree_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_SIGLEN_DEFAULT);
    // SAFETY: gist fmgr protocol.
    let orig = unsafe { entry_arg(fcinfo, 0) };
    let new = unsafe { entry_arg(fcinfo, 1) };
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = gist::ltree_penalty(key_image(orig.key), key_image(new.key), siglen)? };
    Ok(fcinfo.arg(2))
}

fn picksplit_common(
    fcinfo: &mut Fcinfo,
    split: impl FnOnce(&[(Vec<u8>, bool)]) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)>,
) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let n = entryvec.n as usize;
    // entries[0] is the 1-based placeholder the cores expect.
    let mut entries: Vec<(Vec<u8>, bool)> = Vec::with_capacity(n);
    entries.push((Vec::new(), false));
    for e in &entryvec.vector[1..n] {
        entries.push((key_image(e.key).to_vec(), false));
    }
    let (spl_left, spl_right, ldatum, rdatum) = split(&entries)?;
    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = ret_image(fcinfo, &ldatum)?;
    v.spl_rdatum = ret_image(fcinfo, &rdatum)?;
    Ok(fcinfo.arg(1))
}

fn fc_ltree_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_SIGLEN_DEFAULT);
    picksplit_common(fcinfo, |entries| gist::ltree_picksplit(entries, siglen))
}

fn gist_query_image(fcinfo: &Fcinfo) -> PgResult<Vec<u8>> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    Ok(detoasted_image(mcx, fcinfo.arg(1))?.to_vec())
}

fn fc_ltree_consistent(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_SIGLEN_DEFAULT);
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let query = gist_query_image(fcinfo)?;
    let (matched, rc) =
        gist::ltree_consistent(entry.page_is_leaf, key_image(entry.key), false, &query, strategy, siglen)?;
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = rc };
    Ok(Datum::from_bool(matched))
}

#[cold]
fn fc_ltree_gist_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(gist::SIZEOF_GIST_OPTIONS);
    relopts.add_int(
        "siglen",
        gist::LTREE_SIGLEN_DEFAULT,
        gist::SIGLEN_MIN_INTALIGNED,
        gist::LTREE_SIGLEN_MAX,
        gist::OFFSETOF_SIGLEN,
    );
    relopts.register_validator(gist::ltree_gist_relopts_validator);
    Ok(Datum::from_usize(0))
}

// --- gist__ltree_ops ------------------------------------------------------

fn fc__ltree_compress(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let img: &[u8] = if entry.leafkey {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        detoasted_image(mcx, entry.key)?
    } else {
        key_image(entry.key)
    };
    match gist::array_compress(entry.leafkey, img, false, siglen)? {
        Some(new_key) => {
            let key = ret_image(fcinfo, &new_key)?;
            let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
            entry_result(fcinfo, &retval)
        }
        None => Ok(fcinfo.arg(0)),
    }
}

fn fc__ltree_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    let a = key_image(fcinfo.arg(0));
    let b = key_image(fcinfo.arg(1));
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = gist::array_same(a, b, siglen)? };
    Ok(fcinfo.arg(2))
}

fn fc__ltree_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    let (_, entries) = entryvec_images(fcinfo);
    let img = gist::array_union(&entries, siglen)?;
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = img.len() as i32 };
    ret_image(fcinfo, &img)
}

fn fc__ltree_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    // SAFETY: gist fmgr protocol.
    let orig = unsafe { entry_arg(fcinfo, 0) };
    let new = unsafe { entry_arg(fcinfo, 1) };
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = gist::array_penalty(key_image(orig.key), key_image(new.key), siglen)? };
    Ok(fcinfo.arg(2))
}

fn fc__ltree_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    picksplit_common(fcinfo, |entries| gist::array_picksplit(entries, siglen))
}

fn fc__ltree_consistent(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let siglen = get_siglen(&f, gist::LTREE_ASIGLEN_DEFAULT);
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let query = gist_query_image(fcinfo)?;
    let (matched, rc) =
        gist::array_consistent(key_image(entry.key), false, &query, strategy, siglen)?;
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = rc };
    Ok(Datum::from_bool(matched))
}

#[cold]
fn fc__ltree_gist_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(gist::SIZEOF_GIST_OPTIONS);
    // C uses min 1 and registers NO multiple-of-4 validator for the array opclass.
    relopts.add_int(
        "siglen",
        gist::LTREE_ASIGLEN_DEFAULT,
        1,
        gist::LTREE_SIGLEN_MAX,
        gist::OFFSETOF_SIGLEN,
    );
    Ok(Datum::from_usize(0))
}


fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "ltree_in" => fc_ltree_in,
        "ltree_out" => fc_ltree_out,
        "ltree_recv" => fc_ltree_recv,
        "ltree_send" => fc_ltree_send,
        "lquery_in" => fc_lquery_in,
        "lquery_out" => fc_lquery_out,
        "lquery_recv" => fc_lquery_recv,
        "lquery_send" => fc_lquery_send,
        "ltxtq_in" => fc_ltxtq_in,
        "ltxtq_out" => fc_ltxtq_out,
        "ltxtq_recv" => fc_ltxtq_recv,
        "ltxtq_send" => fc_ltxtq_send,
        "ltree_cmp" => fc_ltree_cmp,
        "ltree_lt" => fc_ltree_lt,
        "ltree_le" => fc_ltree_le,
        "ltree_eq" => fc_ltree_eq,
        "ltree_ne" => fc_ltree_ne,
        "ltree_ge" => fc_ltree_ge,
        "ltree_gt" => fc_ltree_gt,
        "hash_ltree" => fc_hash_ltree,
        "hash_ltree_extended" => fc_hash_ltree_extended,
        "nlevel" => fc_nlevel,
        "ltree_isparent" => fc_ltree_isparent,
        "ltree_risparent" => fc_ltree_risparent,
        "subltree" => fc_subltree,
        "subpath" => fc_subpath,
        "ltree_index" => fc_ltree_index,
        "ltree_addltree" => fc_ltree_addltree,
        "ltree_addtext" => fc_ltree_addtext,
        "ltree_textadd" => fc_ltree_textadd,
        "lca" => fc_lca,
        "ltree2text" => fc_ltree2text,
        "text2ltree" => fc_text2ltree,
        "ltreeparentsel" => fc_ltreeparentsel,
        "ltq_regex" => fc_ltq_regex,
        "ltq_rregex" => fc_ltq_rregex,
        "lt_q_regex" => fc_lt_q_regex,
        "lt_q_rregex" => fc_lt_q_rregex,
        "ltxtq_exec" => fc_ltxtq_exec,
        "ltxtq_rexec" => fc_ltxtq_rexec,
        "_ltree_isparent" => fc__ltree_isparent,
        "_ltree_r_isparent" => fc__ltree_r_isparent,
        "_ltree_risparent" => fc__ltree_risparent,
        "_ltree_r_risparent" => fc__ltree_r_risparent,
        "_ltree_extract_isparent" => fc__ltree_extract_isparent,
        "_ltree_extract_risparent" => fc__ltree_extract_risparent,
        "_ltq_regex" => fc__ltq_regex,
        "_ltq_rregex" => fc__ltq_rregex,
        "_ltq_extract_regex" => fc__ltq_extract_regex,
        "_lt_q_regex" => fc__lt_q_regex,
        "_lt_q_rregex" => fc__lt_q_rregex,
        "_ltxtq_exec" => fc__ltxtq_exec,
        "_ltxtq_rexec" => fc__ltxtq_rexec,
        "_ltxtq_extract_exec" => fc__ltxtq_extract_exec,
        "_lca" => fc__lca,
        "ltree_gist_in" => fc_ltree_gist_in,
        "ltree_gist_out" => fc_ltree_gist_out,
        "ltree_compress" => fc_ltree_compress,
        "ltree_decompress" => fc_ltree_decompress,
        "ltree_same" => fc_ltree_same,
        "ltree_union" => fc_ltree_union,
        "ltree_penalty" => fc_ltree_penalty,
        "ltree_picksplit" => fc_ltree_picksplit,
        "ltree_consistent" => fc_ltree_consistent,
        "ltree_gist_options" => fc_ltree_gist_options,
        "_ltree_compress" => fc__ltree_compress,
        "_ltree_same" => fc__ltree_same,
        "_ltree_union" => fc__ltree_union,
        "_ltree_penalty" => fc__ltree_penalty,
        "_ltree_picksplit" => fc__ltree_picksplit,
        "_ltree_consistent" => fc__ltree_consistent,
        "_ltree_gist_options" => fc__ltree_gist_options,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // ltree's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// contrib/ltree's regress environment is the C locale, and crc32.c's
    /// fold arm is chosen by the default locale's ctype_is_c (see crc::fold).
    /// Unit tests run without a catalog, so pin it the way the boot path
    /// would; every test touching an lquery/ltxtquery label CRCs through it.
    fn pin_c_ctype() {
        ::pg_locale::set_default_locale_c_for_tests();
        ::pg_locale::set_database_ctype_is_c(true);
    }

    fn img(s: &str) -> Vec<u8> {
        pin_c_ctype();
        io::parse_ltree(s.as_bytes()).unwrap()
    }

    #[test]
    fn parse_deparse_roundtrip() {
        for s in ["Top", "Top.Science.Astronomy", "a.b.c.d.e"] {
            assert_eq!(io::deparse_ltree(&img(s)), s.as_bytes());
        }
    }

    #[test]
    fn compare_and_isparent() {
        assert_eq!(op::ltree_compare(&img("a.b"), &img("a.b")), 0);
        assert!(op::ltree_compare(&img("a.b"), &img("a.c")) < 0);
        assert!(op::inner_isparent(&img("a.b.c"), &img("a.b")));
        assert!(!op::inner_isparent(&img("a.b"), &img("a.b.c")));
    }

    #[test]
    fn lquery_match() {
        pin_c_ctype();
        let q = io::parse_lquery(b"*.Astronomy.*").unwrap();
        assert!(op::ltq_regex(&img("Top.Astronomy.Stars"), &q).unwrap());
        assert!(!op::ltq_regex(&img("Top.Science"), &q).unwrap());
    }

    #[test]
    fn ltxtquery_match() {
        pin_c_ctype();
        let q = io::parse_ltxtquery(b"Astro* & !pictures").unwrap();
        assert!(op::ltxtq_exec(&img("Top.Astronomy.Stars"), &q).unwrap());
        assert!(!op::ltxtq_exec(&img("Top.Astronomy.pictures"), &q).unwrap());
    }

    #[test]
    fn lca() {
        let a = img("a.b.c.d");
        let b = img("a.b.e");
        let r = op::lca_inner(&[a.as_slice(), b.as_slice()]).unwrap();
        assert_eq!(io::deparse_ltree(&r), b"a.b");
    }

    #[test]
    fn syntax_error_positions() {
        let e = io::parse_ltree(b"a..b").unwrap_err();
        assert!(e.message().contains("syntax error"));
    }

    // Payload the client sees for a send result datum.
    fn send_payload(d: Datum) -> Vec<u8> {
        // SAFETY: send wrappers return a live 4B-header varlena.
        unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) }
            .data()
            .to_vec()
    }

    fn fcinfo_with_arg<'a>(
        ctx: &'a mcx::MemoryContext,
        arg: Datum,
    ) -> types_fmgr::LocalFcinfo<1> {
        // pq_sendtext consults the client-encoding seams; identity here.
        static SEAMS: std::sync::Once = std::sync::Once::new();
        SEAMS.call_once(|| {
            mbutils_seams::server_to_client_conversion_needed::set(|| false);
            mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        });
        let mut fcinfo = types_fmgr::LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
        fcinfo.set_arg(0, arg);
        fcinfo
    }

    // B3 (SDK compat matrix 2026-07-18): C's binary format carries a leading
    // version byte in BOTH directions; dropping it silently ate the first
    // character on output and rejected valid binary input.
    #[test]
    fn binary_wire_carries_version_byte_both_directions() {
        let ctx = mcx::MemoryContext::new("t");

        let tree = img("this.is.a.path");
        let mut fcinfo = fcinfo_with_arg(&ctx, Datum::from_usize(tree.as_ptr() as usize));
        let d = fc_ltree_send(None, &mut fcinfo).unwrap();
        let wire = send_payload(d);
        assert_eq!(wire[0], 1, "leading wire-format version byte");
        assert_eq!(&wire[1..], b"this.is.a.path");

        // recv accepts exactly what send produced (strips the version byte).
        let mut si = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        si.append_bytes(&wire).unwrap();
        let mut fcinfo =
            fcinfo_with_arg(&ctx, Datum::from_usize(core::ptr::from_mut(&mut si) as usize));
        let d = fc_ltree_recv(None, &mut fcinfo).unwrap();
        // SAFETY: recv returns a live 4B-header ltree varlena.
        let payload = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data();
        let total = payload.len() + repr::VARHDRSZ;
        let mut full = vec![0u8; total];
        repr::set_varsize(&mut full, total);
        full[repr::VARHDRSZ..].copy_from_slice(payload);
        assert_eq!(io::deparse_ltree(&full), b"this.is.a.path");
    }

    #[test]
    fn binary_wire_lquery_ltxtquery_version_byte() {
        pin_c_ctype();
        let ctx = mcx::MemoryContext::new("t");

        let q = io::parse_lquery(b"*.Astronomy.*").unwrap();
        let mut fcinfo = fcinfo_with_arg(&ctx, Datum::from_usize(q.as_ptr() as usize));
        let wire = send_payload(fc_lquery_send(None, &mut fcinfo).unwrap());
        assert_eq!((wire[0], &wire[1..]), (1, &b"*.Astronomy.*"[..]));

        let t = io::parse_ltxtquery(b"Astro* & !pictures").unwrap();
        let mut fcinfo = fcinfo_with_arg(&ctx, Datum::from_usize(t.as_ptr() as usize));
        let wire = send_payload(fc_ltxtq_send(None, &mut fcinfo).unwrap());
        assert_eq!(wire[0], 1);
        assert_eq!(&wire[1..], b"Astro* & !pictures");
    }

    #[test]
    fn binary_wire_recv_rejects_unknown_version() {
        let ctx = mcx::MemoryContext::new("t");
        let mut si = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        si.append_bytes(b"\x02a.b").unwrap();
        let mut fcinfo =
            fcinfo_with_arg(&ctx, Datum::from_usize(core::ptr::from_mut(&mut si) as usize));
        let e = fc_ltree_recv(None, &mut fcinfo).unwrap_err();
        assert_eq!(e.message(), "unsupported ltree version number 2");
    }

    /// p1-ltree REGRESSION (release blocker #2, fixed 2026-08-01): the
    /// MATCHING operators recurse too — `check_cond` (C lquery_op.c:206
    /// checkCond) once per lquery level, and `ltree_execute` (C
    /// ltxtquery_op.c:23) once per ltxtquery tree node. Both were guarded by a
    /// fixed `depth > 100_000` frame cap, unreachable by ~30-100x behind a real
    /// backend stack, so the guard was dead code; `ltree_execute` additionally
    /// returned a bare `false` — a silently wrong boolean — where C raises
    /// 54001.
    ///
    /// The DISARMED leg is the must-fail control: with no stack base recorded,
    /// `stack_is_too_deep()` short-circuits and the recursion runs unguarded,
    /// which is exactly the pre-fix plane. It MUST die. If it ever stops
    /// dying, this test has stopped proving that the guard is what saves us.
    #[test]
    fn matching_operators_deep_recursion_raise_54001_and_do_not_abort() {
        pin_c_ctype();
        const STATEMENT_TOO_COMPLEX: u32 = 5 + (4 << 6) + (1 << 24);
        if let Ok(spec) = std::env::var("LTREE_OP_PROBE") {
            let (which, arm) = spec.split_once(':').unwrap();
            let armed = arm == "armed";
            let which = which.to_string();
            let n = 20_000;
            // Build the images on a generous stack with the guard OFF: the
            // PARSERS recurse too (findoprnd), and this test is about the
            // OPERATORS. Only the operator call runs on the backend-sized
            // stack below.
            let (tree, query) = std::thread::Builder::new()
                .stack_size(256 << 20)
                .spawn({
                    let which = which.clone();
                    move || {
                        // thread-local pin (crc::fold reads it per thread)
                        pin_c_ctype();
                        if which == "check_cond" {
                            (
                                io::parse_ltree(
                                    "a.".repeat(n).trim_end_matches('.').as_bytes(),
                                )
                                .unwrap(),
                                io::parse_lquery(
                                    format!("{}a", "*{0,1}.".repeat(n)).as_bytes(),
                                )
                                .unwrap(),
                            )
                        } else {
                            (
                                io::parse_ltree(b"a.b").unwrap(),
                                io::parse_ltxtquery(
                                    format!("{}a", "a&".repeat(n)).as_bytes(),
                                )
                                .unwrap(),
                            )
                        }
                    }
                })
                .unwrap()
                .join()
                .unwrap();
            let h = std::thread::Builder::new()
                .stack_size(8 << 20)
                .spawn(move || {
                    if armed {
                        stack_depth::set_stack_base();
                        stack_depth::assign_max_stack_depth(2048);
                    }
                    if which == "check_cond" {
                        op::ltq_regex(&tree, &query).map(|b| b.to_string())
                    } else {
                        op::ltxtq_exec(&tree, &query).map(|b| b.to_string())
                    }
                })
                .unwrap();
            match h.join().expect("must not panic") {
                Ok(v) => eprintln!("OPPROBE OK {v}"),
                Err(e) => eprintln!("OPPROBE ERR {}", e.sqlstate().0),
            }
            return;
        }
        let exe = std::env::current_exe().unwrap();
        let run = |spec: &str| {
            let out = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "--nocapture",
                    "tests::matching_operators_deep_recursion_raise_54001_and_do_not_abort",
                ])
                .env("LTREE_OP_PROBE", spec)
                .output()
                .unwrap();
            let se = String::from_utf8_lossy(&out.stderr).to_string();
            let verdict = se
                .lines()
                .find(|l| l.starts_with("OPPROBE"))
                .map(str::to_string);
            (out.status.success(), verdict, se)
        };
        for which in ["check_cond", "ltree_execute"] {
            let (ok, verdict, se) = run(&format!("{which}:armed"));
            assert!(ok, "{which} armed: process died: {se}");
            assert_eq!(
                verdict.as_deref(),
                Some(format!("OPPROBE ERR {STATEMENT_TOO_COMPLEX}").as_str()),
                "{which} armed: expected a clean 54001"
            );
            // must-fail control: unguarded, this recursion overflows the stack.
            let (ok, verdict, se) = run(&format!("{which}:disarmed"));
            assert!(
                !ok && verdict.is_none() && se.contains("overflow"),
                "{which} disarmed: expected an unguarded stack overflow (the \
                 pre-fix plane); if this now survives, the guard is no longer \
                 what prevents the abort and this control is vacuous. got: {se}"
            );
        }
    }

    /// p1-ltree frame-budget measurement: how deep a flat `a|b|b|...` chain
    /// each side accepts at the same max_stack_depth (2048 kB, which is what
    /// PG's own boot logic caps the GUC at). `LTREE_FRAME_BISECT=1`.
    /// Not a gate — this quantifies the one remaining seed-corpus difference.
    #[test]
    fn frame_budget_bisect() {
        if std::env::var("LTREE_FRAME_BISECT").is_err() {
            return;
        }
        let try_depth = |n: usize| -> bool {
            let q = format!("a{}", "|b".repeat(n));
            std::thread::Builder::new()
                .stack_size(8 << 20)
                .spawn(move || {
                    stack_depth::set_stack_base();
                    stack_depth::assign_max_stack_depth(2048);
                    io::parse_ltxtquery(q.as_bytes()).is_ok()
                })
                .unwrap()
                .join()
                .unwrap_or(false)
        };
        let (mut lo, mut hi) = (1usize, 200_000usize);
        while lo < hi - 1 {
            let mid = (lo + hi) / 2;
            if try_depth(mid) {
                lo = mid
            } else {
                hi = mid
            }
        }
        eprintln!("pgrust deepest OK flat-| chain at max_stack_depth=2048kB: {lo}");
    }

    /// p1-ltree docker-differential probe (not a gate; a bug-finding rig).
    /// `LTREE_PROBE=<seeds.tsv>` prints `kind<TAB>hex<TAB>OK:<text>|ERR:<code>`
    /// for each seed so the same seeds can be replayed through
    /// docker postgres:18.3 and diffed. Inert without the env var.
    #[test]
    fn docker_differential_probe() {
        let Ok(path) = std::env::var("LTREE_PROBE") else { return };
        let body = std::fs::read_to_string(&path).unwrap();
        // Emulate a backend thread: the stack-depth guard short-circuits on
        // base == 0, so without this the recursive parsers run UNGUARDED and
        // the probe measures a plane the server never has.
        stack_depth::set_stack_base();
        stack_depth::assign_max_stack_depth(2048);
        for line in body.lines() {
            let (kind, hex) = line.split_once('\t').unwrap();
            let buf: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            let r = match kind {
                "ltree" => io::parse_ltree(&buf).map(|i| io::deparse_ltree(&i)),
                "lquery" => io::parse_lquery(&buf).and_then(|i| io::deparse_lquery(&i)),
                "ltxtquery" => io::parse_ltxtquery(&buf)
                    .and_then(|i| io::deparse_ltxtquery(&i)),
                k => panic!("bad kind {k}"),
            };
            let v = match r {
                Ok(t) => format!("OK:{}", String::from_utf8_lossy(&t)),
                Err(e) => format!("ERR:{}", e.sqlstate().0),
            };
            println!("{kind}\t{hex}\t{v}");
        }
    }

    /// p1-ltree REGRESSION (divergence, fixed 2026-08-01): lquery repeat
    /// counts go through C's `atoi` == `(int) strtol`, so a digit string that
    /// TRUNCATES into 0..=LTREE_MAX_LEVELS is accepted with the truncated
    /// value. Every expectation below is ground-truthed against
    /// docker postgres:18.3 with the ltree extension installed.
    #[test]
    fn lquery_repeat_count_matches_c_atoi_truncation() {
        pin_c_ctype();
        // accepted: value truncates by (int) into range
        for (input, want) in [
            ("*{4294967301}", "*{5}"),
            ("*{4294967296}", "*{0}"),
            ("*{,4294967301}", "*{,5}"),
            ("a{4294967301,4294967301}", "a{5}"),
        ] {
            let img = io::parse_lquery(input.as_bytes())
                .unwrap_or_else(|e| panic!("{input}: rejected: {}", e.message()));
            assert_eq!(
                String::from_utf8(io::deparse_lquery(&img).unwrap()).unwrap(),
                want,
                "{input}"
            );
        }
        // rejected: value truncates negative (or saturates to LONG_MAX -> -1)
        for input in [
            "*{2147483648}",
            "*{99999999999999999999}",
            "*{9223372036854775807}",
            "*{18446744073709551616}",
        ] {
            let e = io::parse_lquery(input.as_bytes())
                .expect_err(&format!("{input}: should be rejected"));
            assert_eq!(e.sqlstate(), types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED, "{input}");
        }
    }

    /// p1-ltree REGRESSION (release blocker, fixed 2026-08-01): ltxtquery's
    /// parser recurses once per '(' and was guarded by a fixed FRAME-COUNT cap
    /// (`depth > 10_000`) instead of C's byte-based check_stack_depth(). At the
    /// measured ~3 KiB/frame that cap was unreachable behind any real backend
    /// stack, so the guard was dead code and deep nesting overflowed the stack
    /// and SIGABRTed the whole process (thread-per-backend => every session).
    /// Measured before the fix (local, --release, guard absent so no pairing
    /// could have saved it): abort at nesting depth 700 on a 2 MiB stack and at
    /// 2800 on the 8 MiB RLIMIT_STACK a backend thread normally gets.
    /// PostgreSQL 18.3 (docker postgres:18.3) returns a value at 5000 and
    /// raises 54001 at 9000 — it never dies.
    ///
    /// Runs in a subprocess because a stack overflow aborts the process.
    #[test]
    fn ltxtquery_deep_nesting_raises_54001_and_does_not_abort() {
        // (nesting depth, must the guard have fired?)
        const DEPTHS: [(usize, bool); 5] =
            [(700, false), (2800, true), (9000, true), (20000, true), (100_000, true)];
        if let Ok(d) = std::env::var("LTREE_STACK_PROBE") {
            let depth: usize = d.parse().unwrap();
            let mut s = Vec::new();
            s.extend(std::iter::repeat(b'(').take(depth));
            s.push(b'a');
            s.extend(std::iter::repeat(b')').take(depth));
            let h = std::thread::Builder::new()
                // The production pairing: child_thread_stack_size() hands a
                // backend thread RLIMIT_STACK (8 MiB typical) while PG's boot
                // logic caps max_stack_depth at 2048 kB, i.e. 4x headroom for
                // the guard to fire in. Pairing a 2 MiB stack WITH a 2048 kB
                // limit leaves zero headroom and cannot work in any profile.
                .stack_size(8 << 20)
                .spawn(move || {
                    // A backend thread records its stack base at spawn (C:
                    // main()); without it stack_is_too_deep() short-circuits on
                    // base == 0 and the guard is INERT. Arming it here is what
                    // makes this test non-vacuous.
                    stack_depth::set_stack_base();
                    stack_depth::assign_max_stack_depth(2048);
                    io::parse_ltxtquery(&s)
                })
                .unwrap();
            match h.join().expect("parser thread must not panic") {
                Ok(_) => eprintln!("PROBE {depth} OK"),
                Err(e) => eprintln!("PROBE {depth} ERR {}", e.sqlstate().0),
            }
            return;
        }
        // ERRCODE_STATEMENT_TOO_COMPLEX == MAKE_SQLSTATE("54001").
        const STATEMENT_TOO_COMPLEX: u32 = 5 + (4 << 6) + (1 << 24);
        let exe = std::env::current_exe().unwrap();
        for (depth, must_raise) in DEPTHS {
            let out = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "--nocapture",
                    "tests::ltxtquery_deep_nesting_raises_54001_and_does_not_abort",
                ])
                .env("LTREE_STACK_PROBE", depth.to_string())
                .output()
                .unwrap();
            let se = String::from_utf8_lossy(&out.stderr);
            let line = se
                .lines()
                .find(|l| l.starts_with("PROBE"))
                .unwrap_or_else(|| panic!("depth {depth}: process died (no verdict): {se}"));
            assert!(
                out.status.success(),
                "depth {depth}: child failed: {se}"
            );
            // The depth at which the guard trips is NOT a comparison surface
            // (it is a function of frame size, which differs from C's); that
            // the process SURVIVES and reports 54001 rather than dying is.
            if must_raise {
                assert_eq!(
                    line,
                    format!("PROBE {depth} ERR {STATEMENT_TOO_COMPLEX}"),
                    "depth {depth}: expected a clean 54001, got {line:?}"
                );
            } else {
                assert!(
                    line == format!("PROBE {depth} OK")
                        || line == format!("PROBE {depth} ERR {STATEMENT_TOO_COMPLEX}"),
                    "depth {depth}: unexpected verdict {line:?}"
                );
            }
        }
    }

    /// ON-DISK FORMAT PIN, ground-truthed against a real server (R1 adoption,
    /// 2026-08-03). These are the bytes stock PostgreSQL 18.3 actually stores
    /// in the heap for `'a|b'::lquery`, read out with pageinspect:
    ///
    /// ```sql
    /// CREATE EXTENSION ltree; CREATE EXTENSION pageinspect;
    /// CREATE TABLE t(q lquery); INSERT INTO t VALUES('a|b');
    /// SELECT t_data FROM heap_page_items(get_raw_page('t',0));
    /// ```
    ///
    /// The server hands back a 1-byte-header varlena (heap_fill_tuple shortens
    /// eligible 4B headers), so the comparison is body-to-body. The load-bearing
    /// bytes are each variant's label: `val`(4) `len`(2) `flag`(1) then the name
    /// at **+7**, with +8 a zero pad. Reading or writing the name at +8 — which
    /// is `LVAR_HDRSIZE`, the STRIDE — puts every stored lquery one byte out of
    /// step with PostgreSQL. This pin is deliberately independent of the
    /// differential oracle: it catches a mis-shimmed oracle too.
    #[test]
    fn lquery_on_disk_bytes_match_stock_pg_18_3() {
        pin_c_ctype();
        // t_data from the server, minus its 1-byte varlena header.
        const SERVER_BODY: &str = "0100000000000000000000003000000002000000\
                                   000000000000000043beb7e80100006100000000\
                                   00000000f9efbe71010000620000000000000000";
        let want: Vec<u8> = SERVER_BODY
            .bytes()
            .filter(|b| !b.is_ascii_whitespace())
            .collect::<Vec<u8>>()
            .chunks(2)
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect();
        let img = io::parse_lquery(b"a|b").unwrap();
        assert_eq!(img.len(), 64, "total image size");
        assert_eq!(repr::varsize(&img), 64, "varlena header");
        assert_eq!(
            &img[repr::VARHDRSZ..],
            &want[..],
            "stored lquery body must be byte-identical to stock PostgreSQL 18.3"
        );
        // spell out the offset the whole finding is about
        let voff = repr::LQUERY_HDRSIZE + repr::LQL_HDRSIZE;
        assert_eq!(img[voff + repr::LVAR_OFF_NAME], b'a', "label at +7");
        assert_eq!(img[voff + repr::LVAR_HDRSIZE], 0, "+8 is pad, not the label");
    }

    /// MaxAllocSize ceiling (task #85 sibling sweep): C's deparse_lquery
    /// pallocs its estimate (~1.25x the stored size for dense multi-variant
    /// levels), so a valid sub-1GB lquery whose estimate crosses MaxAllocSize
    /// raises palloc's catchable "invalid memory alloc request size" BEFORE
    /// emitting. Pre-fix this test FAILS: the deparse succeeds and hands the
    /// over-ceiling cstring on to the (later, differently-sized) result copy.
    #[test]
    fn lquery_out_over_ceiling_estimate_raises_palloc_error() {
        // Level: numvar one-char variants (LVAR = 8 hdr + maxalign(1) = 16
        // bytes each) + LQL_COUNT. Stored 65520 fits the u16 totallen; C's
        // per-level estimate is 2 + numvar*4 + 65520 + 25 = 81923.
        const NUMVAR: usize = 4094;
        const LVL_TOTALLEN: usize = 16 + NUMVAR * 16; // 65520
        const NLEVEL: usize = 13107;
        let estimate: usize = 1 + NLEVEL * (2 + NUMVAR * 4 + LVL_TOTALLEN + 2 * 11 + 3);
        assert!(estimate > 0x3FFF_FFFF, "fixture must cross MaxAllocSize");

        let mut level = vec![0u8; LVL_TOTALLEN];
        repr::write_u16(&mut level, 0, LVL_TOTALLEN as u16); // totallen
        repr::write_u16(&mut level, 2, repr::LQL_COUNT); // flag
        repr::write_u16(&mut level, 4, NUMVAR as u16); // numvar
        repr::write_u16(&mut level, 6, 1); // low
        repr::write_u16(&mut level, 8, 2); // high
        for j in 0..NUMVAR {
            let off = 16 + j * 16;
            repr::write_u16(&mut level, off + 4, 1); // LVAR len
            level[off + repr::LVAR_OFF_NAME] = b'a'; // name (C offsetof == 7)
        }

        let total = repr::LQUERY_HDRSIZE + NLEVEL * repr::maxalign(LVL_TOTALLEN);
        assert!(total <= 0x3FFF_FFFF, "fixture image must itself be valid");
        let mut img = Vec::with_capacity(total);
        img.extend_from_slice(&[0u8; repr::LQUERY_HDRSIZE]);
        repr::set_varsize(&mut img, total);
        repr::write_u16(&mut img, 4, NLEVEL as u16); // numlevel
        for _ in 0..NLEVEL {
            img.extend_from_slice(&level);
        }

        let err = io::deparse_lquery(&img)
            .expect_err("over-ceiling lquery deparse estimate must raise palloc's error");
        assert_eq!(
            err.message(),
            format!("invalid memory alloc request size {estimate}")
        );
    }
}
