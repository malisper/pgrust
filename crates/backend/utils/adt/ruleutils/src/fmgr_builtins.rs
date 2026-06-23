//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `ruleutils.c` deparsers whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching worker in the crate root, and writes back the
//! result word / by-reference payload. [`register_ruleutils_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.
//!
//! # The by-reference `text` convention
//!
//! `pg_node_tree`/`text` are pass-by-reference (varlena) types. They cross the
//! fmgr boundary on the by-reference side channel, **header-stripped** (the
//! payload bytes only), exactly as `backend-utils-adt-varlena` arranges: a
//! `text` ARG arrives as `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(payload))`
//! and a `text` RESULT is set via `fcinfo.set_ref_result(RefPayload::Varlena(
//! payload))`. The bare by-value word is meaningless for these. `oid`/`int4`/
//! `bool` args are read by value.
//!
//! A `text *` result of NULL (the worker returning `Ok(None)`, e.g. a relation
//! that vanished) maps to `PG_RETURN_NULL()`: `fcinfo.set_isnull(true)`.

extern crate std;
use alloc::string::String;
use alloc::vec::Vec;

use ::mcx::MemoryContext;
use ::types_core::primitive::Oid;
use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` + `text_to_cstring`: a `text`/`pg_node_tree` arg's
/// header-stripped payload bytes, decoded as a `&str` (the deparser input is a
/// `nodeToString` rendering, always valid UTF-8 / ASCII).
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ruleutils fn: by-ref `text` arg missing from by-ref lane");
    // VARDATA_ANY: skip the 4-byte varlena header on the header-ful image.
    let bytes = &image[::datum::varlena::VARHDRSZ..];
    core::str::from_utf8(bytes).expect("ruleutils fn: `text` arg not valid UTF-8")
}

/// `PG_GETARG_OID(i)`: the low 32 bits of arg `i`'s word as an Oid.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("ruleutils fn: missing arg").value.as_oid()
}

/// `PG_GETARG_BOOL(i)`: arg `i`'s word as a bool.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("ruleutils fn: missing arg").value.as_bool()
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, sign-extended.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("ruleutils fn: missing arg").value.as_i32()
}

/// `PG_RETURN_TEXT_P(string_to_text(str))`: set a `text` (by-reference) result
/// on the by-ref lane, header-stripped (the payload bytes), and return the dummy
/// by-value word.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> PgResult<Datum> {
    // string_to_text: prepend the 4-byte varlena header (header-ful image).
    let mut img = Vec::with_capacity(::datum::varlena::VARHDRSZ + bytes.len());
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(
        ::datum::varlena::VARHDRSZ + bytes.len(),
    ));
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

/// `PG_RETURN_NULL()`: flag the result NULL and return the dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    fcinfo.isnull = true;
    Ok(Datum::from_usize(0))
}

/// Write a `name` result (C: `PG_RETURN_NAME(Name)`): the fixed
/// `NAMEDATALEN`-byte buffer image on the by-ref lane (the `name` value the
/// boundary passes by pointer), NUL-filled past the copied bytes. `name` is a
/// fixed-length-by-ref type, not a varlena — no header.
#[inline]
fn ret_name(fcinfo: &mut FunctionCallInfoBaseData, name: &[u8]) -> PgResult<Datum> {
    const NAMEDATALEN: usize = ::types_core::fmgr::NAMEDATALEN as usize;
    let mut buf = alloc::vec![0u8; NAMEDATALEN];
    // namestrcpy truncates at NAMEDATALEN-1 and always NUL-terminates.
    let n = name.len().min(NAMEDATALEN - 1);
    buf[..n].copy_from_slice(&name[..n]);
    fcinfo.set_ref_result(RefPayload::Varlena(buf));
    Ok(Datum::from_usize(0))
}

/// A scratch context for workers that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("ruleutils fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_get_expr(pg_node_tree, oid) -> text` (oid 1716). prettyFlags =
/// PRETTYFLAG_INDENT.
fn fc_pg_get_expr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let exprstr = String::from(arg_text_str(fcinfo, 0));
    let relid = arg_oid(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::pg_get_expr_worker(m.mcx(), &exprstr, relid, crate::PRETTYFLAG_INDENT)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_expr_ext(pg_node_tree, oid, bool) -> text` (oid 2509). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_expr_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let exprstr = String::from(arg_text_str(fcinfo, 0));
    let relid = arg_oid(fcinfo, 1);
    let pretty = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let res = crate::pg_get_expr_worker(m.mcx(), &exprstr, relid, crate::get_pretty_flags(pretty))?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_indexdef(oid) -> text` (oid 1643). prettyFlags = PRETTYFLAG_INDENT.
fn fc_pg_get_indexdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let indexrelid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::indexdef::pg_get_indexdef_worker(
        m.mcx(),
        indexrelid,
        0,
        None,
        false,
        false,
        false,
        false,
        crate::PRETTYFLAG_INDENT,
        true,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_indexdef_ext(oid, int4, bool) -> text` (oid 2507). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_indexdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let indexrelid = arg_oid(fcinfo, 0);
    let colno = arg_int32(fcinfo, 1);
    let pretty = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let res = crate::indexdef::pg_get_indexdef_worker(
        m.mcx(),
        indexrelid,
        colno,
        None,
        colno != 0,
        false,
        false,
        false,
        crate::get_pretty_flags(pretty),
        true,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_userbyid(oid) -> name` (oid 1642, ruleutils.c:2794). Look up the
/// `pg_authid` row for `roleid` via `SearchSysCache1(AUTHOID, ...)` and return
/// `rolname`; on cache miss fall back to `unknown (OID=n)`. The result is a
/// `name` (the fixed `NAMEDATALEN` buffer image).
fn fc_pg_get_userbyid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let roleid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    // SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid)) projected to rolname.
    let rolname = syscache_seams::authid_rolname::call(m.mcx(), roleid)?;
    let name: Vec<u8> = match rolname {
        Some(s) => s.as_str().as_bytes().to_vec(),
        None => alloc::format!("unknown (OID={roleid})").into_bytes(),
    };
    ret_name(fcinfo, &name)
}

/// `pg_get_constraintdef(oid) -> text` (oid 1387). prettyFlags =
/// PRETTYFLAG_INDENT.
fn fc_pg_get_constraintdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let conid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::constraintdef::pg_get_constraintdef_worker(
        m.mcx(),
        conid,
        false,
        crate::PRETTYFLAG_INDENT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_constraintdef_ext(oid, bool) -> text` (oid 2508). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_constraintdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let conid = arg_oid(fcinfo, 0);
    let pretty = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::constraintdef::pg_get_constraintdef_worker(
        m.mcx(),
        conid,
        false,
        crate::get_pretty_flags(pretty),
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_viewdef(oid) -> text` (oid 1641). prettyFlags = PRETTYFLAG_INDENT,
/// wrapColumn = WRAP_COLUMN_DEFAULT.
fn fc_pg_get_viewdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let res = crate::viewdef::pg_get_viewdef_worker(
        m.mcx(),
        arg_oid(fcinfo, 0),
        crate::PRETTYFLAG_INDENT,
        crate::WRAP_COLUMN_DEFAULT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_viewdef_ext(oid, bool) -> text` (oid 2506). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_viewdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let pretty = arg_bool(fcinfo, 1);
    let res = crate::viewdef::pg_get_viewdef_worker(
        m.mcx(),
        arg_oid(fcinfo, 0),
        get_pretty_flags(pretty),
        crate::WRAP_COLUMN_DEFAULT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_viewdef_name(text) -> text` (oid 1640). By qualified name:
/// `viewoid = RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(
/// viewname)), NoLock, false)`, then the worker with prettyFlags =
/// PRETTYFLAG_INDENT.
fn fc_pg_get_viewdef_name(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let viewname = arg_text_str(fcinfo, 0);
    let viewoid = namespace_seams::range_var_get_relid_from_text::call(
        m.mcx(),
        viewname,
        0, /* NoLock */
        false,
    )?;
    let res = crate::viewdef::pg_get_viewdef_worker(
        m.mcx(),
        viewoid,
        crate::PRETTYFLAG_INDENT,
        crate::WRAP_COLUMN_DEFAULT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_viewdef_name_ext(text, bool) -> text` (oid 2505). By qualified name,
/// prettyFlags = GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_viewdef_name_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let viewname = arg_text_str(fcinfo, 0);
    let pretty = arg_bool(fcinfo, 1);
    let viewoid = namespace_seams::range_var_get_relid_from_text::call(
        m.mcx(),
        viewname,
        0, /* NoLock */
        false,
    )?;
    let res = crate::viewdef::pg_get_viewdef_worker(
        m.mcx(),
        viewoid,
        get_pretty_flags(pretty),
        crate::WRAP_COLUMN_DEFAULT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_viewdef_wrap(oid, int4) -> text` (oid 3159). Implies pretty;
/// prettyFlags = GET_PRETTY_FLAGS(true), wrapColumn = the wrap arg.
fn fc_pg_get_viewdef_wrap(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let wrap = arg_int32(fcinfo, 1);
    let res = crate::viewdef::pg_get_viewdef_worker(
        m.mcx(),
        arg_oid(fcinfo, 0),
        get_pretty_flags(true),
        wrap,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_ruledef(oid) -> text` (oid 1573). prettyFlags = PRETTYFLAG_INDENT.
fn fc_pg_get_ruledef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let res = crate::viewdef::pg_get_ruledef_worker(
        m.mcx(),
        arg_oid(fcinfo, 0),
        crate::PRETTYFLAG_INDENT,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_ruledef_ext(oid, bool) -> text` (oid 2504). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_ruledef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let pretty = arg_bool(fcinfo, 1);
    let res = crate::viewdef::pg_get_ruledef_worker(
        m.mcx(),
        arg_oid(fcinfo, 0),
        get_pretty_flags(pretty),
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_triggerdef(oid) -> text` (oid 1662). pretty = false.
fn fc_pg_get_triggerdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let trigid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::triggerdef::pg_get_triggerdef_worker(m.mcx(), trigid, false)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_triggerdef_ext(oid, bool) -> text` (oid 2730). pretty threaded.
fn fc_pg_get_triggerdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let trigid = arg_oid(fcinfo, 0);
    let pretty = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::triggerdef::pg_get_triggerdef_worker(m.mcx(), trigid, pretty)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_functiondef(oid) -> text` (oid 2098).
fn fc_pg_get_functiondef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::functiondef::pg_get_functiondef(m.mcx(), funcid)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_partkeydef(oid) -> text` (oid 3352). prettyFlags = PRETTYFLAG_INDENT,
/// attrsOnly = false, missing_ok = true.
fn fc_pg_get_partkeydef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::partkeydef::pg_get_partkeydef_worker(
        m.mcx(),
        relid,
        crate::PRETTYFLAG_INDENT,
        false,
        true,
    )?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_partition_constraintdef(oid) -> text` (oid 3408). The partition
/// constraint expression as text, NULL when there is no partition constraint.
fn fc_pg_get_partition_constraintdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::partconstrdef::pg_get_partition_constraintdef(m.mcx(), relid)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `GET_PRETTY_FLAGS(pretty)` (ruleutils.c 92): pretty ?
/// (PRETTYFLAG_PAREN|PRETTYFLAG_INDENT|PRETTYFLAG_SCHEMA) : PRETTYFLAG_INDENT.
#[inline]
fn get_pretty_flags(pretty: bool) -> i32 {
    if pretty {
        crate::PRETTYFLAG_PAREN | crate::PRETTYFLAG_INDENT | crate::PRETTYFLAG_SCHEMA
    } else {
        crate::PRETTYFLAG_INDENT
    }
}

/// `pg_get_statisticsobjdef(oid) -> text` (oid 3415). Full CREATE STATISTICS
/// definition: `pg_get_statisticsobj_worker(statextid, false, true)`.
fn fc_pg_get_statisticsobjdef(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let statextid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::statisticsdef::pg_get_statisticsobj_worker(m.mcx(), statextid, false, true)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_statisticsobjdef_columns(oid) -> text` (oid 6174). Columns and
/// expressions only: `pg_get_statisticsobj_worker(statextid, true, true)`.
fn fc_pg_get_statisticsobjdef_columns(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let statextid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::statisticsdef::pg_get_statisticsobj_worker(m.mcx(), statextid, true, true)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_statisticsobjdef_expressions(oid) -> _text` (oid 6173). Builds a
/// `text[]` of the per-expression deparses (ruleutils.c 1838-1900). The worker
/// reads the `stxexprs` `pg_node_tree`, `stringToNode`s it, and
/// `deparse_expression_pretty(PRETTYFLAG_INDENT)` each expression; this adapter
/// wraps each result string into a `text` Datum (`cstring_to_text`) and
/// `construct_array(..., TEXTOID, -1, false, 'i')` (the `accumArrayResult` +
/// `makeArrayResult` pair), returning the on-disk array varlena image on the
/// by-ref lane. `PG_RETURN_NULL()` when the object is gone / has no expressions.
fn fc_pg_get_statisticsobjdef_expressions(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    /// `TEXTOID` (pg_type.dat).
    const TEXTOID: Oid = 25;

    let statextid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let mcx = m.mcx();
    let exprs = crate::statisticsdef::pg_get_statisticsobjdef_expressions(mcx, statextid)?;
    let strs = match exprs {
        Some(v) => v,
        None => return ret_null(fcinfo),
    };

    // foreach: astate = accumArrayResult(astate, cstring_to_text(str), false,
    //                                    TEXTOID, CurrentMemoryContext);
    let mut elems: Vec<types_tuple::Datum> = Vec::with_capacity(strs.len());
    for s in &strs {
        elems.push(varlena_seams::cstring_to_text_v::call(
            mcx,
            s.as_str(),
        )?);
    }

    // makeArrayResult(astate, ...): construct_array over the text elements
    // (text storage: typlen = -1 varlena, typbyval = false, typalign = 'i').
    let arr = arrayfuncs_seams::construct_array_values_bytes::call(
        mcx,
        &elems,
        TEXTOID,
        -1,
        false,
        b'i' as core::ffi::c_char,
    )?;

    // PG_RETURN_DATUM: a `text[]` result is pass-by-reference — carry the
    // on-disk array varlena image on the by-ref lane.
    fcinfo.set_ref_result(RefPayload::Varlena(arr.as_slice().to_vec()));
    Ok(Datum::from_usize(0))
}

/// `pg_get_function_arguments(oid) -> text` (oid 2162).
fn fc_pg_get_function_arguments(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::functiondef::pg_get_function_arguments(m.mcx(), funcid)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_function_identity_arguments(oid) -> text` (oid 2232).
fn fc_pg_get_function_identity_arguments(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::functiondef::pg_get_function_identity_arguments(m.mcx(), funcid)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_function_result(oid) -> text` (oid 2165).
fn fc_pg_get_function_result(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::functiondef::pg_get_function_result(m.mcx(), funcid)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_function_arg_default(oid, int4) -> text` (oid 3808).
fn fc_pg_get_function_arg_default(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcid = arg_oid(fcinfo, 0);
    let nth_arg = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::functiondef::pg_get_function_arg_default(m.mcx(), funcid, nth_arg)?;
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
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
            name: String::from(name),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every expressible SQL-callable `ruleutils.c` deparser (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat` (all of
/// these are `proisstrict => 't'` default and none `proretset`).
/// `pg_get_serial_sequence(tablename text, columnname text) -> text` (oid 1665,
/// ruleutils.c:2833). Get the name of the sequence used by an identity or serial
/// column, formatted for setval/nextval/currval. The first parameter is *not*
/// double-quoted (parsed as a qualified name); the second *is* (used verbatim as
/// the column name).
fn fc_pg_get_serial_sequence(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    use ::types_core::primitive::AttrNumber;
    const INVALID_ATTR_NUMBER: AttrNumber = 0;

    let m = scratch_mcx();
    let mcx = m.mcx();
    let tablename = arg_text_str(fcinfo, 0);
    let column = String::from(arg_text_str(fcinfo, 1));

    // tablerv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
    // tableOid = RangeVarGetRelid(tablerv, NoLock, false);
    let table_oid = namespace_seams::range_var_get_relid_from_text::call(
        mcx,
        tablename,
        0,     /* NoLock */
        false, /* missing_ok = false */
    )?;

    // attnum = get_attnum(tableOid, column);
    let attnum = lsyscache_seams::get_attnum::call(table_oid, &column)?;
    if attnum == INVALID_ATTR_NUMBER {
        // tablerv->relname — the parsed (possibly unqualified) name's last
        // component; equals the resolved relation's name here.
        let relname = lsyscache_seams::get_rel_name::call(mcx, table_oid)?
            .map(|s| String::from(s.as_str()))
            .unwrap_or_else(|| String::from(tablename));
        return Err(::types_error::PgError::error(alloc::format!(
            "column \"{column}\" of relation \"{relname}\" does not exist"
        ))
        .with_sqlstate(::types_error::ERRCODE_UNDEFINED_COLUMN));
    }

    // Search pg_depend for the dependent (auto/internal) sequence on this column.
    let sequence_id =
        pg_depend_seams::get_serial_sequence_for_column::call(table_oid, attnum)?;

    match sequence_id {
        Some(seq) => {
            // result = generate_qualified_relation_name(sequenceId);
            let result = crate::constraintdef::generate_qualified_relation_name(mcx, seq)?;
            ret_text(fcinfo, result.as_str().as_bytes().to_vec())
        }
        None => ret_null(fcinfo),
    }
}

pub fn register_ruleutils_builtins() {
    fmgr_core::register_builtins_native([
        // Slice 1: fully working (deparse via the ported get_rule_expr).
        builtin(1716, "pg_get_expr", 2, true, false, fc_pg_get_expr),
        builtin(2509, "pg_get_expr_ext", 3, true, false, fc_pg_get_expr_ext),
        // Slice 2: structurally faithful; the workers hit documented
        // seam-panics at the unported catalog-name owners.
        builtin(1643, "pg_get_indexdef", 1, true, false, fc_pg_get_indexdef),
        builtin(2507, "pg_get_indexdef_ext", 3, true, false, fc_pg_get_indexdef_ext),
        builtin(1387, "pg_get_constraintdef", 1, true, false, fc_pg_get_constraintdef),
        builtin(2508, "pg_get_constraintdef_ext", 2, true, false, fc_pg_get_constraintdef_ext),
        // pg_get_userbyid(oid) -> name (roleid -> rolname, unknown-OID fallback).
        builtin(1642, "pg_get_userbyid", 1, true, false, fc_pg_get_userbyid),
        builtin(1665, "pg_get_serial_sequence", 2, true, false, fc_pg_get_serial_sequence),
        // Slice 3: extended-statistics deparse (resolvable; workers seam-and-panic
        // at the unported Form_pg_statistic_ext deform — empty-input psql `\d`
        // describe only needs resolution, see statisticsdef.rs).
        // Slice 4: view- and rule-definition deparse (ported end-to-end for the
        // common SELECT-view spine via get_query_def). pg_rewrite is read by the
        // genam MVCC scan; the action Query is stringToNode'd and deparsed.
        builtin(1641, "pg_get_viewdef", 1, true, false, fc_pg_get_viewdef),
        builtin(2506, "pg_get_viewdef_ext", 2, true, false, fc_pg_get_viewdef_ext),
        builtin(3159, "pg_get_viewdef_wrap", 2, true, false, fc_pg_get_viewdef_wrap),
        builtin(1640, "pg_get_viewdef_name", 1, true, false, fc_pg_get_viewdef_name),
        builtin(2505, "pg_get_viewdef_name_ext", 2, true, false, fc_pg_get_viewdef_name_ext),
        builtin(1573, "pg_get_ruledef", 1, true, false, fc_pg_get_ruledef),
        builtin(2504, "pg_get_ruledef_ext", 2, true, false, fc_pg_get_ruledef_ext),
        // Slice 5: trigger / function / partition-key definition deparse (the
        // pg_get_triggerdef / pg_get_functiondef / pg_get_partkeydef workers).
        builtin(1662, "pg_get_triggerdef", 1, true, false, fc_pg_get_triggerdef),
        builtin(2730, "pg_get_triggerdef_ext", 2, true, false, fc_pg_get_triggerdef_ext),
        builtin(2098, "pg_get_functiondef", 1, true, false, fc_pg_get_functiondef),
        builtin(2162, "pg_get_function_arguments", 1, true, false, fc_pg_get_function_arguments),
        builtin(2232, "pg_get_function_identity_arguments", 1, true, false, fc_pg_get_function_identity_arguments),
        builtin(2165, "pg_get_function_result", 1, true, false, fc_pg_get_function_result),
        builtin(3808, "pg_get_function_arg_default", 2, true, false, fc_pg_get_function_arg_default),
        builtin(3352, "pg_get_partkeydef", 1, true, false, fc_pg_get_partkeydef),
        builtin(3408, "pg_get_partition_constraintdef", 1, true, false, fc_pg_get_partition_constraintdef),
        builtin(3415, "pg_get_statisticsobjdef", 1, true, false, fc_pg_get_statisticsobjdef),
        builtin(6174, "pg_get_statisticsobjdef_columns", 1, true, false, fc_pg_get_statisticsobjdef_columns),
        builtin(6173, "pg_get_statisticsobjdef_expressions", 1, true, false, fc_pg_get_statisticsobjdef_expressions),
    ]);
}

// ===========================================================================
// End-to-end proof: `pg_get_expr` is genuinely callable through the fmgr
// registry, with the `text` arg/result crossing on the by-reference lane.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use ::mcx::PgBox;
    use ::datum::NullableDatum;
    use ::nodes::nodes::Node;
    use ::nodes::primnodes::{Expr, SQLValueFunction, SQLValueFunctionOp};

    /// Install lightweight, faithful test bodies for the two owner seams
    /// `pg_get_expr_worker` reaches on the relid=0 (no-Var) path: `stringToNode`
    /// (here decoding the canned text into a `CURRENT_USER` `SQLValueFunction`)
    /// and `pull_varnos` (no Vars -> the empty set, `None`). These mirror the
    /// real owners' contract for this input; the real owners install the same
    /// seams from their own `init_seams()` at backend init.
    fn install_test_seams() {
        if !read_seams::string_to_node::is_installed() {
            read_seams::string_to_node::set(test_string_to_node);
        }
        if !var_seams::pull_varnos::is_installed() {
            var_seams::pull_varnos::set(test_pull_varnos);
        }
    }

    fn test_string_to_node<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        _s: &str,
    ) -> ::types_error::PgResult<PgBox<'mcx, Node<'mcx>>> {
        let svf = SQLValueFunction {
            op: SQLValueFunctionOp::SVFOP_CURRENT_USER,
            r#type: 0,
            typmod: -1,
            location: -1,
        };
        ::mcx::alloc_in(mcx, Node::mk_expr(mcx, Expr::SQLValueFunction(svf)))
    }

    fn test_pull_varnos<'mcx>(
        _mcx: ::mcx::Mcx<'mcx>,
        _node: &Expr,
    ) -> ::types_error::PgResult<Option<PgBox<'mcx, ::nodes::bitmapset::Bitmapset<'mcx>>>> {
        Ok(None)
    }

    /// THE PROOF: `pg_get_expr('<encoded CURRENT_USER>', 0)` deparses to
    /// `"CURRENT_USER"`, computed entirely through the fmgr registry by OID
    /// (1716), with the `text` arg and `text` result crossing on the by-ref
    /// lane (header-stripped payload).
    #[test]
    fn byref_pg_get_expr_through_registry() {
        install_test_seams();
        register_ruleutils_builtins();

        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),         // text (by-ref)
            NullableDatum::value(Datum::from_u32(0)),    // relid = InvalidOid
        ];
        // The encoded node text rides the by-ref lane header-stripped; our test
        // stringToNode ignores the bytes and yields a CURRENT_USER node.
        // Header-ful everywhere: frame the encoded node text with a 4-byte
        // varlena header (the boundary's `arg_text_str` strips VARDATA_ANY).
        let payload = b"{SQLVALUEFUNCTION}";
        let mut img = ::datum::varlena::set_varsize_4b(
            ::datum::varlena::VARHDRSZ + payload.len(),
        )
        .to_vec();
        img.extend_from_slice(payload);
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(img)), None];

        let native =
            fmgr_core::native_builtin(1716).expect("pg_get_expr registered");
        let entry =
            fmgr_core::fmgr_isbuiltin(1716).expect("pg_get_expr registered");
        assert_eq!(entry.nargs, 2);
        assert!(entry.strict);
        native(&mut fcinfo).expect("pg_get_expr returned Err");

        let out = fcinfo.take_ref_result().expect("pg_get_expr produced a result");
        match out {
            RefPayload::Varlena(b) => {
                // Header-ful result: skip the 4-byte varlena header.
                let payload = &b[::datum::varlena::VARHDRSZ..];
                assert_eq!(core::str::from_utf8(payload).unwrap(), "CURRENT_USER");
            }
            other => panic!("pg_get_expr: unexpected result lane {other:?}"),
        }
        assert!(!fcinfo.isnull);
    }

    /// The index/constraint definition builtins are registered and dispatchable
    /// by OID with the correct arity. (Exercising the worker bodies needs the
    /// real syscache/lsyscache/amapi owners installed, which only happens at
    /// backend init, so the end-to-end deparse is verified by the regress suite
    /// — see the module docs.)
    #[test]
    fn indexdef_constraintdef_builtins_registered() {
        register_ruleutils_builtins();
        let idx = fmgr_core::fmgr_isbuiltin(1643).expect("pg_get_indexdef registered");
        assert_eq!(idx.nargs, 1);
        let idx_ext =
            fmgr_core::fmgr_isbuiltin(2507).expect("pg_get_indexdef_ext registered");
        assert_eq!(idx_ext.nargs, 3);
        let con = fmgr_core::fmgr_isbuiltin(1387)
            .expect("pg_get_constraintdef registered");
        assert_eq!(con.nargs, 1);
        let con_ext = fmgr_core::fmgr_isbuiltin(2508)
            .expect("pg_get_constraintdef_ext registered");
        assert_eq!(con_ext.nargs, 2);
    }
}
