//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `tsquery` functions of `tsquery.c` / `tsquery_op.c` whose argument / result
//! types are expressible at the current fmgr boundary: I/O
//! (`tsqueryin`/`out`/`send`/`recv`/`tsquerytree`), the comparison family
//! (`tsquery_cmp`/`eq`/`ne`/`lt`/`le`/`gt`/`ge`), the boolean combinators
//! (`tsquery_and`/`or`/`not`/`phrase`/`phrase_distance`), and `tsquery_numnode`.
//!
//! A `tsquery` value is its flat **header-ful** varlena image (the value cores
//! `set_varsize(query, HDRSIZETQ)` and read the size word off the header), so
//! `tsquery` args/results cross VERBATIM on the by-ref lane — no header strip,
//! no re-frame. `tsqueryout` returns a `cstring`; `tsquerytree` returns `text`
//! (its payload is re-framed header-ful here); `tsquerysend` returns the
//! header-ful `bytea` wire image; `tsqueryrecv` takes the wire `StringInfo`.
//!
//! The set-containment operators (`tsq_mcontains` / `tsq_mcontained`, the GIN
//! opclass `@>` / `<@`) ARE registered: both args are header-ful `tsquery`
//! images and the cores run in-process over transient `QTNode` trees.
//!
//! NOT registered here: the GiST support functions (`gtsquery_*`), whose args
//! are `internal` GiST state, which dispatch through the index AM, not by-OID
//! fmgr.

use std::string::{String, ToString};

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `tsquery` arg's full header-ful varlena image on the by-ref lane (read
/// verbatim — the value cores consume the `HDRSIZETQ`-headed image).
#[inline]
fn arg_tsquery<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("tsquery fn: by-ref tsquery arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("tsquery fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("tsquery fn: missing int4 arg").value.as_i32()
}

/// Set a header-ful `tsquery`/`bytea` varlena result on the by-ref lane verbatim.
#[inline]
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `text` result on the by-ref lane: the value core returned the payload
/// bytes; re-frame header-ful (prepend the 4-byte `SET_VARSIZE` length word).
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// Set a `cstring` (`tsqueryout`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    let s = String::from_utf8(bytes).expect("tsqueryout: result not valid UTF-8");
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("tsquery fmgr scratch")
}

fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — I/O.
// ---------------------------------------------------------------------------

fn fc_tsqueryin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    // Forward the soft ErrorSaveContext installed on the frame by
    // InputFunctionCallSafe so a recoverable parse failure `ereturn`s into the
    // sink (returning `Ok(None)`) instead of throwing past `invoke?`.
    let image = ok(crate::tsquery::tsqueryin(m.mcx(), &s, fcinfo.escontext_mut()));
    match image {
        Some(img) => ret_varlena_image(fcinfo, img),
        // Soft-error path: escontext recorded the failure; return a NULL
        // placeholder the caller discards after `soft_error_occurred()`.
        None => Datum::null(),
    }
}

fn fc_tsqueryout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = ok(crate::tsquery::tsqueryout(m.mcx(), arg_tsquery(fcinfo, 0)));
    ret_cstring(fcinfo, out)
}

fn fc_tsquerysend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::tsquery::tsquerysend(m.mcx(), arg_tsquery(fcinfo, 0)));
    ret_varlena_image(fcinfo, image)
}

fn fc_tsqueryrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("tsqueryrecv: by-ref StringInfo arg missing from by-ref lane");
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    let image = ok(crate::tsquery::tsqueryrecv(m.mcx(), &mut buf));
    ret_varlena_image(fcinfo, image)
}

/// `tsquerytree(tsquery) -> text` — the index-searchable subtree as text.
fn fc_tsquerytree(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let payload = ok(crate::tsquery::tsquerytree(m.mcx(), arg_tsquery(fcinfo, 0)));
    ret_text(fcinfo, payload)
}

// ---------------------------------------------------------------------------
// fc_ adapters — comparison.
// ---------------------------------------------------------------------------

fn fc_tsquery_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_i32(ok(crate::op::tsquery_cmp(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_eq(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_ne(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_lt(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_le(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_gt(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsquery_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsquery_ge(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}

// ---------------------------------------------------------------------------
// fc_ adapters — boolean combinators + numnode.
// ---------------------------------------------------------------------------

fn fc_tsquery_and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::op::tsquery_and(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1)));
    ret_varlena_image(fcinfo, image)
}
fn fc_tsquery_or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::op::tsquery_or(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1)));
    ret_varlena_image(fcinfo, image)
}
fn fc_tsquery_not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::op::tsquery_not(m.mcx(), arg_tsquery(fcinfo, 0)));
    ret_varlena_image(fcinfo, image)
}
fn fc_tsquery_phrase(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = ok(crate::op::tsquery_phrase(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1)));
    ret_varlena_image(fcinfo, image)
}
fn fc_tsquery_phrase_distance(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dist = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::op::tsquery_phrase_distance(
        m.mcx(),
        arg_tsquery(fcinfo, 0),
        arg_tsquery(fcinfo, 1),
        dist,
    ));
    ret_varlena_image(fcinfo, image)
}
fn fc_tsquery_numnode(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::op::tsquery_numnode(arg_tsquery(fcinfo, 0)))
}

// ---------------------------------------------------------------------------
// fc_ adapters — set-containment (`@>` / `<@`).
//
// Both args are header-ful `tsquery` images on the by-ref lane (verbatim). The
// cores take an `Mcx` for the transient `QTNode` working trees.
// ---------------------------------------------------------------------------

fn fc_tsq_mcontains(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsq_mcontains(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}
fn fc_tsq_mcontained(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ret_bool(ok(crate::op::tsq_mcontained(m.mcx(), arg_tsquery(fcinfo, 0), arg_tsquery(fcinfo, 1))))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every `tsquery` builtin whose value core is ported and whose
/// arg/result types are expressible at the current fmgr boundary. OIDs/nargs
/// from `pg_proc.dat`; every row is `proisstrict => 't'` and not retset.
pub fn register_tsquery_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(3612, "tsqueryin", 1, fc_tsqueryin),
        builtin(3613, "tsqueryout", 1, fc_tsqueryout),
        builtin(3640, "tsquerysend", 1, fc_tsquerysend),
        builtin(3641, "tsqueryrecv", 1, fc_tsqueryrecv),
        builtin(3673, "tsquerytree", 1, fc_tsquerytree),
        // ---- comparison ----
        builtin(3662, "tsquery_lt", 2, fc_tsquery_lt),
        builtin(3663, "tsquery_le", 2, fc_tsquery_le),
        builtin(3664, "tsquery_eq", 2, fc_tsquery_eq),
        builtin(3665, "tsquery_ne", 2, fc_tsquery_ne),
        builtin(3666, "tsquery_ge", 2, fc_tsquery_ge),
        builtin(3667, "tsquery_gt", 2, fc_tsquery_gt),
        builtin(3668, "tsquery_cmp", 2, fc_tsquery_cmp),
        // ---- boolean combinators + numnode ----
        builtin(3669, "tsquery_and", 2, fc_tsquery_and),
        builtin(3670, "tsquery_or", 2, fc_tsquery_or),
        builtin(3671, "tsquery_not", 1, fc_tsquery_not),
        builtin(3672, "tsquery_numnode", 1, fc_tsquery_numnode),
        builtin(5003, "tsquery_phrase", 2, fc_tsquery_phrase),
        builtin(5004, "tsquery_phrase_distance", 3, fc_tsquery_phrase_distance),
        // ---- set-containment (GIN-opclass `@>` / `<@`) ----
        builtin(3691, "tsq_mcontains", 2, fc_tsq_mcontains),
        builtin(3692, "tsq_mcontained", 2, fc_tsq_mcontained),
    ]);
}
