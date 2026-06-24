#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `contrib/ltree` — the hierarchical-tree-path data types (`ltree`, `lquery`,
//! `ltxtquery`), their operators / functions, and the GiST opclass support
//! functions, ported as an in-process Rust builtin library.
//!
//! ## What is ported here
//!
//! The full scalar half of `ltree_io.c`, `ltree_op.c`, `lquery_op.c`,
//! `ltxtquery_io.c`, `ltxtquery_op.c`, `_ltree_op.c`, and `crc32.c` — i.e.
//! every function that runs on a sequential scan: the three types' in/out/recv/
//! send, the comparison/hash/concat/`subltree`/`subpath`/`nlevel`/`index`/`lca`/
//! `text2ltree`/`ltree2text` functions, the `~`/`@`/`@>`/`<@` match operators
//! and their `ltree[]` array forms. The label-parsing and lquery/ltxtquery
//! matching state machines mirror C byte-for-byte (the on-disk varlena formats
//! must stay GiST-compatible, and the regression suite compares exact match
//! results plus exact syntax-error messages and character positions).
//!
//! ## Index opclasses
//!
//! ltree ships NO GIN opclass; its index support is entirely GiST
//! (`gist_ltree_ops` / `gist__ltree_ops`, in `ltree_gist.c` / `_ltree_gist.c`).
//! pgrust's GENERIC extension-opclass GiST dispatch (the keystone that lets an
//! extension's GiST support functions be reached through a real fmgr frame) is
//! NOT yet on main, so those support functions are registered as loud-panic
//! stubs: `CREATE EXTENSION`'s C-symbol validator finds every symbol and the
//! seq-scan operators all work, but building/using a `gist_ltree_ops` index
//! mirrors-C-and-panics until the keystone lands. The btree (`ltree_cmp`) and
//! hash (`hash_ltree`) opclasses ARE real (those AMs exist), so a `USING btree`
//! / `USING hash` index over ltree works.

mod array;
mod crc;
mod io;
mod op;
mod repr;

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::types_error::PgError;

const LIBRARY: &str = "ltree";

/// Raise a builtin's `ereport(ERROR)` through the one fmgr dispatch point
/// (`invoke_pgfunction`'s `catch_unwind`), which downcasts the panic payload
/// back to the structured [`PgError`].
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

// ===========================================================================
// fmgr argument readers / result writers.
// ===========================================================================

/// `VARDATA_ANY(image)` — inline varlena payload (skip the short or 4-byte
/// header). The fmgr boundary has already detoasted a by-ref arg.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= repr::VARHDRSZ => &image[repr::VARHDRSZ..],
        _ => &[],
    }
}

/// The full header-ful varlena image of a by-ref arg (an `ltree`/`lquery`/
/// `ltxtquery`/array). We rely on the boundary having normalized it to the
/// 4-byte-header inline form (matching the on-disk layout the repr walkers
/// expect).
fn arg_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ltree: by-ref varlena arg missing")
}

/// A `text` arg's `VARDATA_ANY` payload bytes.
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    varlena_payload(arg_image(fcinfo, i))
}

/// A `cstring` arg's bytes.
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("ltree: cstring arg missing")
        .as_bytes()
}

/// Raw bytes of a by-ref arg (StringInfo payload for recv).
fn arg_raw<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ltree: raw by-ref arg missing")
}

fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("ltree: missing int4 arg").value.as_i32()
}

fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("ltree: missing int8 arg").value.as_i64()
}

/// Return a freshly-built varlena image (ltree/lquery/ltxtquery/bytea).
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Return a `cstring` result from raw bytes.
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Cstring(String::from_utf8_lossy(&bytes).into_owned()));
    Datum::from_usize(0)
}

/// Build a header-ful `text` image from payload bytes.
fn text_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + repr::VARHDRSZ;
    let mut image = vec![0u8; total];
    repr::set_varsize(&mut image, total);
    image[repr::VARHDRSZ..].copy_from_slice(payload);
    image
}

fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = true;
    Datum::from_usize(0)
}

/// Lower a parse `Result` onto the fmgr boundary, honoring the soft-error
/// context: an input-function syntax error `errsave`s into `fcinfo->context`
/// (then returns NULL, mirroring C's `ereturn(escontext, NULL, ...)`); with no
/// escontext the error propagates as a hard `ereport(ERROR)`.
fn ret_parse(fcinfo: &mut FunctionCallInfoBaseData, r: Result<Vec<u8>, PgError>) -> Datum {
    match r {
        Ok(image) => ret_varlena(fcinfo, image),
        Err(e) => match fcinfo.escontext_mut() {
            Some(ctx) => {
                ctx.save(e);
                ret_null(fcinfo)
            }
            None => raise(e),
        },
    }
}

fn ret_bool(fcinfo: &mut FunctionCallInfoBaseData, b: bool) -> Datum {
    fcinfo.isnull = false;
    Datum::from_bool(b)
}

fn ret_i32(fcinfo: &mut FunctionCallInfoBaseData, v: i32) -> Datum {
    fcinfo.isnull = false;
    Datum::from_i32(v)
}

fn ret_i64(fcinfo: &mut FunctionCallInfoBaseData, v: i64) -> Datum {
    fcinfo.isnull = false;
    Datum::from_i64(v)
}

// ===========================================================================
// Type I/O (ltree_io.c, ltxtquery_io.c).
// ===========================================================================

fn fc_ltree_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_cstring(fcinfo, 0).to_vec();
    let r = io::parse_ltree(&buf);
    ret_parse(fcinfo, r)
}

fn fc_ltree_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    ret_cstring(fcinfo, io::deparse_ltree(&img))
}

fn fc_lquery_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_cstring(fcinfo, 0).to_vec();
    let r = io::parse_lquery(&buf);
    ret_parse(fcinfo, r)
}

fn fc_lquery_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    ret_cstring(fcinfo, io::deparse_lquery(&img))
}

fn fc_ltxtq_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_cstring(fcinfo, 0).to_vec();
    let r = io::parse_ltxtquery(&buf);
    ret_parse(fcinfo, r)
}

fn fc_ltxtq_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    match io::deparse_ltxtquery(&img) {
        Ok(s) => ret_cstring(fcinfo, s),
        Err(e) => raise(e),
    }
}

// recv/send — the type is sent as version-prefixed text in binary mode.

/// `pq_getmsgint(buf, 1)` then `pq_getmsgtext`: read the version byte + the
/// remaining text from a StringInfo image, returning the text bytes.
fn recv_text(buf: &[u8], typname: &str) -> Result<Vec<u8>, PgError> {
    if buf.is_empty() {
        return Err(PgError::error("insufficient data left in message"));
    }
    let version = buf[0] as i32;
    if version != 1 {
        return Err(PgError::error(format!(
            "unsupported {typname} version number {version}"
        )));
    }
    Ok(buf[1..].to_vec())
}

/// Build a `bytea` send image: version byte (1) followed by the text bytes.
fn send_image(text: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(text.len() + 1);
    payload.push(1u8); // version
    payload.extend_from_slice(text);
    text_image(&payload)
}

fn fc_ltree_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_raw(fcinfo, 0).to_vec();
    let txt = match recv_text(&buf, "ltree") {
        Ok(t) => t,
        Err(e) => raise(e),
    };
    match io::parse_ltree(&txt) {
        Ok(image) => ret_varlena(fcinfo, image),
        Err(e) => raise(e),
    }
}

fn fc_ltree_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    let text = io::deparse_ltree(&img);
    ret_varlena(fcinfo, send_image(&text))
}

fn fc_lquery_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_raw(fcinfo, 0).to_vec();
    let txt = match recv_text(&buf, "lquery") {
        Ok(t) => t,
        Err(e) => raise(e),
    };
    match io::parse_lquery(&txt) {
        Ok(image) => ret_varlena(fcinfo, image),
        Err(e) => raise(e),
    }
}

fn fc_lquery_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    let text = io::deparse_lquery(&img);
    ret_varlena(fcinfo, send_image(&text))
}

fn fc_ltxtq_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_raw(fcinfo, 0).to_vec();
    let txt = match recv_text(&buf, "ltxtquery") {
        Ok(t) => t,
        Err(e) => raise(e),
    };
    match io::parse_ltxtquery(&txt) {
        Ok(image) => ret_varlena(fcinfo, image),
        Err(e) => raise(e),
    }
}

fn fc_ltxtq_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    match io::deparse_ltxtquery(&img) {
        Ok(text) => ret_varlena(fcinfo, send_image(&text)),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// ltree comparison / hash / btree+hash opclass support (ltree_op.c).
// ===========================================================================

fn cmp_op(fcinfo: &mut FunctionCallInfoBaseData, pred: fn(i32) -> bool) -> Datum {
    let a = arg_image(fcinfo, 0);
    let b = arg_image(fcinfo, 1);
    ret_bool(fcinfo, pred(op::ltree_compare(a, b)))
}

fn fc_ltree_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0);
    let b = arg_image(fcinfo, 1);
    let r = op::ltree_compare(a, b);
    ret_i32(fcinfo, r)
}
fn fc_ltree_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r < 0)
}
fn fc_ltree_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r <= 0)
}
fn fc_ltree_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r == 0)
}
fn fc_ltree_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r != 0)
}
fn fc_ltree_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r >= 0)
}
fn fc_ltree_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    cmp_op(fcinfo, |r| r > 0)
}

fn fc_hash_ltree(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0);
    ret_i32(fcinfo, op::hash_ltree(a) as i32)
}

fn fc_hash_ltree_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0).to_vec();
    let seed = arg_i64(fcinfo, 1) as u64;
    ret_i64(fcinfo, op::hash_ltree_extended(&a, seed) as i64)
}

fn fc_nlevel(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0);
    ret_i32(fcinfo, op::nlevel(a))
}

// isparent: ltree_isparent(p, c) returns p is ancestor of c. Args are (0)=p,(1)=c.
fn fc_ltree_isparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = arg_image(fcinfo, 0);
    let c = arg_image(fcinfo, 1);
    ret_bool(fcinfo, op::inner_isparent(c, p))
}

fn fc_ltree_risparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let c = arg_image(fcinfo, 0);
    let p = arg_image(fcinfo, 1);
    ret_bool(fcinfo, op::inner_isparent(c, p))
}

// ===========================================================================
// subltree / subpath / concat / index / lca / text conversions.
// ===========================================================================

fn fc_subltree(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_image(fcinfo, 0).to_vec();
    let start = arg_i32(fcinfo, 1);
    let end = arg_i32(fcinfo, 2);
    match op::inner_subltree(&t, start, end) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_subpath(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_image(fcinfo, 0).to_vec();
    let start = arg_i32(fcinfo, 1);
    let len = if fcinfo.nargs() == 3 {
        Some(arg_i32(fcinfo, 2))
    } else {
        None
    };
    match op::subpath(&t, start, len) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_ltree_addltree(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0).to_vec();
    let b = arg_image(fcinfo, 1).to_vec();
    match op::ltree_concat(&a, &b) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_ltree_addtext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0).to_vec();
    let b = arg_text_payload(fcinfo, 1).to_vec();
    let tmp = match io::parse_ltree(&b) {
        Ok(t) => t,
        Err(e) => raise(e),
    };
    match op::ltree_concat(&a, &tmp) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_ltree_textadd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = arg_text_payload(fcinfo, 0).to_vec();
    let a = arg_image(fcinfo, 1).to_vec();
    let tmp = match io::parse_ltree(&b) {
        Ok(t) => t,
        Err(e) => raise(e),
    };
    match op::ltree_concat(&tmp, &a) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_ltree_index(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_image(fcinfo, 0).to_vec();
    let b = arg_image(fcinfo, 1).to_vec();
    let start = if fcinfo.nargs() == 3 {
        Some(arg_i32(fcinfo, 2))
    } else {
        None
    };
    ret_i32(fcinfo, op::ltree_index(&a, &b, start))
}

fn fc_lca(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let n = fcinfo.nargs();
    let imgs: Vec<Vec<u8>> = (0..n).map(|i| arg_image(fcinfo, i).to_vec()).collect();
    let refs: Vec<&[u8]> = imgs.iter().map(|v| v.as_slice()).collect();
    match op::lca_inner(&refs) {
        Some(img) => ret_varlena(fcinfo, img),
        None => ret_null(fcinfo),
    }
}

fn fc_text2ltree(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_text_payload(fcinfo, 0).to_vec();
    match io::parse_ltree(&s) {
        Ok(img) => ret_varlena(fcinfo, img),
        Err(e) => raise(e),
    }
}

fn fc_ltree2text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let img = arg_image(fcinfo, 0).to_vec();
    let text = io::deparse_ltree(&img);
    ret_varlena(fcinfo, text_image(&text))
}

// ===========================================================================
// lquery / ltxtquery match operators (lquery_op.c, ltxtquery_op.c).
// ===========================================================================

fn fc_ltq_regex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tree = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match op::ltq_regex(&tree, &query) {
        Ok(b) => ret_bool(fcinfo, b),
        Err(e) => raise(e),
    }
}

fn fc_ltq_rregex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // args swapped: (query, tree)
    let query = arg_image(fcinfo, 0).to_vec();
    let tree = arg_image(fcinfo, 1).to_vec();
    match op::ltq_regex(&tree, &query) {
        Ok(b) => ret_bool(fcinfo, b),
        Err(e) => raise(e),
    }
}

/// `lt_q_regex(tree, lquery[])` — true if any lquery in the array matches.
fn fc_lt_q_regex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tree = arg_image(fcinfo, 0).to_vec();
    let qarr = arg_image(fcinfo, 1).to_vec();
    let arr = array::LtreeArray::parse(&qarr);
    if let Err(e) = arr.check_1d_no_nulls() {
        raise(e);
    }
    let mut res = false;
    for q in arr.elements() {
        match op::ltq_regex(&tree, q) {
            Ok(true) => {
                res = true;
                break;
            }
            Ok(false) => {}
            Err(e) => raise(e),
        }
    }
    ret_bool(fcinfo, res)
}

fn fc_lt_q_rregex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // swapped: (lquery[], tree)
    let qarr = arg_image(fcinfo, 0).to_vec();
    let tree = arg_image(fcinfo, 1).to_vec();
    let arr = array::LtreeArray::parse(&qarr);
    if let Err(e) = arr.check_1d_no_nulls() {
        raise(e);
    }
    let mut res = false;
    for q in arr.elements() {
        match op::ltq_regex(&tree, q) {
            Ok(true) => {
                res = true;
                break;
            }
            Ok(false) => {}
            Err(e) => raise(e),
        }
    }
    ret_bool(fcinfo, res)
}

fn fc_ltxtq_exec(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tree = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    ret_bool(fcinfo, op::ltxtq_exec(&tree, &query))
}

fn fc_ltxtq_rexec(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query = arg_image(fcinfo, 0).to_vec();
    let tree = arg_image(fcinfo, 1).to_vec();
    ret_bool(fcinfo, op::ltxtq_exec(&tree, &query))
}

// ===========================================================================
// ltree[] array operators (_ltree_op.c).
// ===========================================================================

/// Iterate `la` (ltree[]) calling `pred(item, query)`; first-match short
/// circuit. Returns the matching element (for the `extract` variants) or
/// whether any matched.
fn array_iter_isparent(
    la: &[u8],
    query: &[u8],
    risparent: bool,
) -> Result<Option<Vec<u8>>, PgError> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        // ltree_isparent(item, query): is `query` ancestor of `item`?  C calls
        // ltree_isparent(item, query) where the callback is ltree_isparent
        // (p=arg0=item ... actually array_iterator passes (item, query)).
        // ltree_isparent(arg0=p? ) — in ltree_isparent C: c=arg1, p=arg0 → p is
        // ancestor of c. So ltree_isparent(item, query) ⇒ p=item, c=query ⇒
        // item is ancestor of query.
        let matched = if risparent {
            // ltree_risparent(item, query): c=item, p=query ⇒ query ancestor of item
            op::inner_isparent(item, query)
        } else {
            // ltree_isparent(item, query): item ancestor of query
            op::inner_isparent(query, item)
        };
        if matched {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

fn fc__ltree_isparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, false) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltree_r_isparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // swapped (query, la)
    let query = arg_image(fcinfo, 0).to_vec();
    let la = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, false) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltree_risparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, true) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltree_r_risparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query = arg_image(fcinfo, 0).to_vec();
    let la = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, true) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltree_extract_isparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, false) {
        Ok(Some(item)) => ret_varlena(fcinfo, item),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

fn fc__ltree_extract_risparent(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_isparent(&la, &query, true) {
        Ok(Some(item)) => ret_varlena(fcinfo, item),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

/// Iterate `la` (ltree[]) testing each element against an lquery `query`.
fn array_iter_ltq(la: &[u8], query: &[u8]) -> Result<Option<Vec<u8>>, PgError> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        if op::ltq_regex(item, query)? {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

fn fc__ltq_regex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltq(&la, &query) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltq_rregex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query = arg_image(fcinfo, 0).to_vec();
    let la = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltq(&la, &query) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltq_extract_regex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltq(&la, &query) {
        Ok(Some(item)) => ret_varlena(fcinfo, item),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

/// `_lt_q_regex(ltree[], lquery[])` — any tree in `_tree` matches any query.
fn fc__lt_q_regex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tree_arr = arg_image(fcinfo, 0).to_vec();
    let query_arr = arg_image(fcinfo, 1).to_vec();
    let qarr = array::LtreeArray::parse(&query_arr);
    if let Err(e) = qarr.check_1d_no_nulls() {
        raise(e);
    }
    let mut res = false;
    'outer: for q in qarr.elements() {
        match array_iter_ltq(&tree_arr, q) {
            Ok(Some(_)) => {
                res = true;
                break 'outer;
            }
            Ok(None) => {}
            Err(e) => raise(e),
        }
    }
    ret_bool(fcinfo, res)
}

fn fc__lt_q_rregex(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query_arr = arg_image(fcinfo, 0).to_vec();
    let tree_arr = arg_image(fcinfo, 1).to_vec();
    let qarr = array::LtreeArray::parse(&query_arr);
    if let Err(e) = qarr.check_1d_no_nulls() {
        raise(e);
    }
    let mut res = false;
    'outer: for q in qarr.elements() {
        match array_iter_ltq(&tree_arr, q) {
            Ok(Some(_)) => {
                res = true;
                break 'outer;
            }
            Ok(None) => {}
            Err(e) => raise(e),
        }
    }
    ret_bool(fcinfo, res)
}

/// Iterate `la` (ltree[]) testing each element against an ltxtquery.
fn array_iter_ltxtq(la: &[u8], query: &[u8]) -> Result<Option<Vec<u8>>, PgError> {
    let arr = array::LtreeArray::parse(la);
    arr.check_1d_no_nulls()?;
    for item in arr.elements() {
        if op::ltxtq_exec(item, query) {
            return Ok(Some(item.to_vec()));
        }
    }
    Ok(None)
}

fn fc__ltxtq_exec(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltxtq(&la, &query) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltxtq_rexec(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let query = arg_image(fcinfo, 0).to_vec();
    let la = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltxtq(&la, &query) {
        Ok(o) => ret_bool(fcinfo, o.is_some()),
        Err(e) => raise(e),
    }
}

fn fc__ltxtq_extract_exec(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let query = arg_image(fcinfo, 1).to_vec();
    match array_iter_ltxtq(&la, &query) {
        Ok(Some(item)) => ret_varlena(fcinfo, item),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

/// `_lca(ltree[])` — LCA over every element of the array.
fn fc__lca(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let la = arg_image(fcinfo, 0).to_vec();
    let arr = array::LtreeArray::parse(&la);
    if let Err(e) = arr.check_1d_no_nulls() {
        raise(e);
    }
    let items: Vec<&[u8]> = arr.elements().collect();
    match op::lca_inner(&items) {
        Some(img) => ret_varlena(fcinfo, img),
        None => ret_null(fcinfo),
    }
}

// ===========================================================================
// ltreeparentsel — selectivity stub (no longer used since ltree 1.2).
// ===========================================================================

fn fc_ltreeparentsel(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C calls generic_restriction_selectivity with default 0.001. Since 1.2+
    // this is unreferenced; return the default selectivity directly.
    fcinfo.isnull = false;
    Datum::from_f64(0.001)
}

// ===========================================================================
// GiST opclass support functions (ltree_gist.c / _ltree_gist.c).
//
// Keystone-gated: pgrust's generic extension-opclass GiST dispatch is not yet
// on main, so these are registered as loud-panic stubs. `CREATE EXTENSION`'s
// C-symbol validator must find every symbol; building/using a gist_ltree_ops
// index mirrors-C-and-panics until the keystone lands.
// ===========================================================================

fn unported_gist(name: &'static str) -> ! {
    raise(PgError::error(format!(
        "ltree: GiST opclass support function \"{name}\" (ltree_gist.c/_ltree_gist.c) \
         is unported — pgrust's generic extension-opclass GiST dispatch keystone \
         is not yet available; ltree seq-scan operators and btree/hash indexes work"
    )));
}

macro_rules! gist_stub {
    ($fn_name:ident, $sym:literal) => {
        fn $fn_name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            unported_gist($sym);
        }
    };
}

gist_stub!(fc_ltree_gist_in, "ltree_gist_in");
gist_stub!(fc_ltree_gist_out, "ltree_gist_out");
gist_stub!(fc_ltree_compress, "ltree_compress");
gist_stub!(fc_ltree_decompress, "ltree_decompress");
gist_stub!(fc_ltree_same, "ltree_same");
gist_stub!(fc_ltree_union, "ltree_union");
gist_stub!(fc_ltree_penalty, "ltree_penalty");
gist_stub!(fc_ltree_picksplit, "ltree_picksplit");
gist_stub!(fc_ltree_consistent, "ltree_consistent");
gist_stub!(fc_ltree_gist_options, "ltree_gist_options");
gist_stub!(fc__ltree_compress, "_ltree_compress");
gist_stub!(fc__ltree_same, "_ltree_same");
gist_stub!(fc__ltree_union, "_ltree_union");
gist_stub!(fc__ltree_penalty, "_ltree_penalty");
gist_stub!(fc__ltree_picksplit, "_ltree_picksplit");
gist_stub!(fc__ltree_consistent, "_ltree_consistent");
gist_stub!(fc__ltree_gist_options, "_ltree_gist_options");

// ===========================================================================
// Builtin-library registration.
// ===========================================================================

fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        // ltree I/O
        "ltree_in" => Some(fc_ltree_in),
        "ltree_out" => Some(fc_ltree_out),
        "ltree_recv" => Some(fc_ltree_recv),
        "ltree_send" => Some(fc_ltree_send),
        // lquery I/O
        "lquery_in" => Some(fc_lquery_in),
        "lquery_out" => Some(fc_lquery_out),
        "lquery_recv" => Some(fc_lquery_recv),
        "lquery_send" => Some(fc_lquery_send),
        // ltxtquery I/O
        "ltxtq_in" => Some(fc_ltxtq_in),
        "ltxtq_out" => Some(fc_ltxtq_out),
        "ltxtq_recv" => Some(fc_ltxtq_recv),
        "ltxtq_send" => Some(fc_ltxtq_send),
        // comparison / hash
        "ltree_cmp" => Some(fc_ltree_cmp),
        "ltree_lt" => Some(fc_ltree_lt),
        "ltree_le" => Some(fc_ltree_le),
        "ltree_eq" => Some(fc_ltree_eq),
        "ltree_ne" => Some(fc_ltree_ne),
        "ltree_ge" => Some(fc_ltree_ge),
        "ltree_gt" => Some(fc_ltree_gt),
        "hash_ltree" => Some(fc_hash_ltree),
        "hash_ltree_extended" => Some(fc_hash_ltree_extended),
        // functions
        "nlevel" => Some(fc_nlevel),
        "ltree_isparent" => Some(fc_ltree_isparent),
        "ltree_risparent" => Some(fc_ltree_risparent),
        "subltree" => Some(fc_subltree),
        "subpath" => Some(fc_subpath),
        "ltree_index" => Some(fc_ltree_index),
        "ltree_addltree" => Some(fc_ltree_addltree),
        "ltree_addtext" => Some(fc_ltree_addtext),
        "ltree_textadd" => Some(fc_ltree_textadd),
        "lca" => Some(fc_lca),
        "ltree2text" => Some(fc_ltree2text),
        "text2ltree" => Some(fc_text2ltree),
        "ltreeparentsel" => Some(fc_ltreeparentsel),
        // lquery / ltxtquery match
        "ltq_regex" => Some(fc_ltq_regex),
        "ltq_rregex" => Some(fc_ltq_rregex),
        "lt_q_regex" => Some(fc_lt_q_regex),
        "lt_q_rregex" => Some(fc_lt_q_rregex),
        "ltxtq_exec" => Some(fc_ltxtq_exec),
        "ltxtq_rexec" => Some(fc_ltxtq_rexec),
        // ltree[] array ops
        "_ltree_isparent" => Some(fc__ltree_isparent),
        "_ltree_r_isparent" => Some(fc__ltree_r_isparent),
        "_ltree_risparent" => Some(fc__ltree_risparent),
        "_ltree_r_risparent" => Some(fc__ltree_r_risparent),
        "_ltree_extract_isparent" => Some(fc__ltree_extract_isparent),
        "_ltree_extract_risparent" => Some(fc__ltree_extract_risparent),
        "_ltq_regex" => Some(fc__ltq_regex),
        "_ltq_rregex" => Some(fc__ltq_rregex),
        "_ltq_extract_regex" => Some(fc__ltq_extract_regex),
        "_lt_q_regex" => Some(fc__lt_q_regex),
        "_lt_q_rregex" => Some(fc__lt_q_rregex),
        "_ltxtq_exec" => Some(fc__ltxtq_exec),
        "_ltxtq_rexec" => Some(fc__ltxtq_rexec),
        "_ltxtq_extract_exec" => Some(fc__ltxtq_extract_exec),
        "_lca" => Some(fc__lca),
        // GiST opclass support (keystone-gated loud-panic stubs)
        "ltree_gist_in" => Some(fc_ltree_gist_in),
        "ltree_gist_out" => Some(fc_ltree_gist_out),
        "ltree_compress" => Some(fc_ltree_compress),
        "ltree_decompress" => Some(fc_ltree_decompress),
        "ltree_same" => Some(fc_ltree_same),
        "ltree_union" => Some(fc_ltree_union),
        "ltree_penalty" => Some(fc_ltree_penalty),
        "ltree_picksplit" => Some(fc_ltree_picksplit),
        "ltree_consistent" => Some(fc_ltree_consistent),
        "ltree_gist_options" => Some(fc_ltree_gist_options),
        "_ltree_compress" => Some(fc__ltree_compress),
        "_ltree_same" => Some(fc__ltree_same),
        "_ltree_union" => Some(fc__ltree_union),
        "_ltree_penalty" => Some(fc__ltree_penalty),
        "_ltree_picksplit" => Some(fc__ltree_picksplit),
        "_ltree_consistent" => Some(fc__ltree_consistent),
        "_ltree_gist_options" => Some(fc__ltree_gist_options),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `ltree` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    ::dfmgr_seams::register_builtin_library(::dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
