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

use mcx::MemoryContext;
use types_core::primitive::Oid;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` + `text_to_cstring`: a `text`/`pg_node_tree` arg's
/// header-stripped payload bytes, decoded as a `&str` (the deparser input is a
/// `nodeToString` rendering, always valid UTF-8 / ASCII).
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ruleutils fn: by-ref `text` arg missing from by-ref lane");
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
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`: flag the result NULL and return the dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = true;
    Datum::from_usize(0)
}

/// A scratch context for workers that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("ruleutils fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_get_expr(pg_node_tree, oid) -> text` (oid 1716). prettyFlags =
/// PRETTYFLAG_INDENT.
fn fc_pg_get_expr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let exprstr = String::from(arg_text_str(fcinfo, 0));
    let relid = arg_oid(fcinfo, 1);
    let m = scratch_mcx();
    let res = ok(crate::pg_get_expr_worker(
        m.mcx(),
        &exprstr,
        relid,
        crate::PRETTYFLAG_INDENT,
    ));
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_expr_ext(pg_node_tree, oid, bool) -> text` (oid 2509). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_expr_ext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let exprstr = String::from(arg_text_str(fcinfo, 0));
    let relid = arg_oid(fcinfo, 1);
    let pretty = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let res = ok(crate::pg_get_expr_worker(
        m.mcx(),
        &exprstr,
        relid,
        crate::get_pretty_flags(pretty),
    ));
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_indexdef(oid) -> text` (oid 1643). prettyFlags = PRETTYFLAG_INDENT.
fn fc_pg_get_indexdef(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let indexrelid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = ok(crate::indexdef::pg_get_indexdef_worker(
        m.mcx(),
        indexrelid,
        0,
        crate::PRETTYFLAG_INDENT,
    ));
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_indexdef_ext(oid, int4, bool) -> text` (oid 2507). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_indexdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let indexrelid = arg_oid(fcinfo, 0);
    let colno = arg_int32(fcinfo, 1);
    let pretty = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let res = ok(crate::indexdef::pg_get_indexdef_worker(
        m.mcx(),
        indexrelid,
        colno,
        crate::get_pretty_flags(pretty),
    ));
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_constraintdef(oid) -> text` (oid 1387). prettyFlags =
/// PRETTYFLAG_INDENT.
fn fc_pg_get_constraintdef(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let conid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = ok(crate::constraintdef::pg_get_constraintdef_worker(
        m.mcx(),
        conid,
        false,
        crate::PRETTYFLAG_INDENT,
    ));
    match res {
        Some(s) => ret_text(fcinfo, s.as_str().as_bytes().to_vec()),
        None => ret_null(fcinfo),
    }
}

/// `pg_get_constraintdef_ext(oid, bool) -> text` (oid 2508). prettyFlags =
/// GET_PRETTY_FLAGS(pretty).
fn fc_pg_get_constraintdef_ext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let conid = arg_oid(fcinfo, 0);
    let pretty = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let res = ok(crate::constraintdef::pg_get_constraintdef_worker(
        m.mcx(),
        conid,
        false,
        crate::get_pretty_flags(pretty),
    ));
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: String::from(name),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every expressible SQL-callable `ruleutils.c` deparser (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat` (all of
/// these are `proisstrict => 't'` default and none `proretset`).
pub fn register_ruleutils_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // Slice 1: fully working (deparse via the ported get_rule_expr).
        builtin(1716, "pg_get_expr", 2, true, false, fc_pg_get_expr),
        builtin(2509, "pg_get_expr_ext", 3, true, false, fc_pg_get_expr_ext),
        // Slice 2: structurally faithful; the workers hit documented
        // seam-panics at the unported catalog-name owners.
        builtin(1643, "pg_get_indexdef", 1, true, false, fc_pg_get_indexdef),
        builtin(2507, "pg_get_indexdef_ext", 3, true, false, fc_pg_get_indexdef_ext),
        builtin(1387, "pg_get_constraintdef", 1, true, false, fc_pg_get_constraintdef),
        builtin(2508, "pg_get_constraintdef_ext", 2, true, false, fc_pg_get_constraintdef_ext),
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
    use mcx::PgBox;
    use types_datum::NullableDatum;
    use types_nodes::nodes::Node;
    use types_nodes::primnodes::{Expr, SQLValueFunction, SQLValueFunctionOp};

    /// Install lightweight, faithful test bodies for the two owner seams
    /// `pg_get_expr_worker` reaches on the relid=0 (no-Var) path: `stringToNode`
    /// (here decoding the canned text into a `CURRENT_USER` `SQLValueFunction`)
    /// and `pull_varnos` (no Vars -> the empty set, `None`). These mirror the
    /// real owners' contract for this input; the real owners install the same
    /// seams from their own `init_seams()` at backend init.
    fn install_test_seams() {
        if !backend_nodes_read_seams::string_to_node::is_installed() {
            backend_nodes_read_seams::string_to_node::set(test_string_to_node);
        }
        if !backend_optimizer_util_var_seams::pull_varnos::is_installed() {
            backend_optimizer_util_var_seams::pull_varnos::set(test_pull_varnos);
        }
    }

    fn test_string_to_node<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        _s: &str,
    ) -> types_error::PgResult<PgBox<'mcx, Node<'mcx>>> {
        let svf = SQLValueFunction {
            op: SQLValueFunctionOp::SVFOP_CURRENT_USER,
            r#type: 0,
            typmod: -1,
            location: -1,
        };
        mcx::alloc_in(mcx, Node::Expr(Expr::SQLValueFunction(svf)))
    }

    fn test_pull_varnos<'mcx>(
        _mcx: mcx::Mcx<'mcx>,
        _node: &Expr,
    ) -> types_error::PgResult<Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>> {
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
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(b"{SQLVALUEFUNCTION}".to_vec())),
            None,
        ];

        let entry =
            backend_utils_fmgr_core::fmgr_isbuiltin(1716).expect("pg_get_expr registered");
        assert_eq!(entry.nargs, 2);
        assert!(entry.strict);
        (entry.func.unwrap())(&mut fcinfo);

        let out = fcinfo.take_ref_result().expect("pg_get_expr produced a result");
        match out {
            RefPayload::Varlena(b) => {
                assert_eq!(String::from_utf8(b).unwrap(), "CURRENT_USER");
            }
            other => panic!("pg_get_expr: unexpected result lane {other:?}"),
        }
        assert!(!fcinfo.isnull);
    }

    /// The Slice-2 `pg_get_indexdef` builtin is registered and dispatchable, and
    /// its worker faithfully seam-panics at the unported `pg_index` syscache
    /// read (mirror-PG-and-panic) rather than fabricating a definition.
    #[test]
    #[should_panic(expected = "pg_get_indexdef_worker")]
    fn pg_get_indexdef_is_seam_bounded() {
        register_ruleutils_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::from_u32(16384))];
        fcinfo.ref_args = vec![None];
        let entry =
            backend_utils_fmgr_core::fmgr_isbuiltin(1643).expect("pg_get_indexdef registered");
        (entry.func.unwrap())(&mut fcinfo);
    }
}
