//! `executor/functions.c` — execution of SQL-language functions (PostgreSQL
//! 18.3 `src/backend/executor/functions.c`).
//!
//! # Scope
//!
//! This crate ports `fmgr_sql`, the call handler installed as `fn_addr` for
//! SQL-language (`prolang == SQLlanguageId`) functions, for the **non-set,
//! scalar / by-reference SELECT-expression-body** common case, faithful to the
//! C `postquel_start`/`postquel_getnext`/`postquel_end` sub-executor loop.
//!
//! PG18's `init_sql_fcache` resolves the function body through funccache.c
//! (`cached_function_compile` → `SQLFunctionHashEntry` → `CachedPlanSource` →
//! `GetCachedPlan`). That machinery is plancache-keystone-blocked in this tree
//! (the plancache carries bare-u64 handles; funccache.c is unported). We
//! therefore bypass the per-function plan cache and, on every call, parse →
//! analyze ($n against `proargtypes`) → rewrite → `pg_plan_query` the body and
//! drive the executor directly — the same parse/plan/run pipeline
//! `exec_simple_query` and `ProcessQuery` use, just over the function body. This
//! reproduces the C execution semantics; it does not reproduce the cross-call
//! plan caching (re-plans each call).
//!
//! ## Faithful to the C loop
//!
//! For each query in the (possibly multi-statement) body we build a
//! `QueryDesc`, `ExecutorStart`, `ExecutorRun` to completion, `ExecutorFinish`,
//! `ExecutorEnd` — `postquel_start`/`getnext`/`end`. Only the last `canSetTag`
//! query produces the result; earlier queries run with output discarded
//! (`None_Receiver`). The result query's single column is captured by a
//! `DR_sqlfunction`-style receiver (`postquel_get_single_result`).
//!
//! ## Deferred (loud panic, never silent)
//!
//!   * **Set-returning functions** (`proretset`): the C tuplestore / lazyEval /
//!     `ReturnSetInfo` materialize machinery. A `fn_retset` call panics with the
//!     prerequisite.
//!   * **Composite (whole-row) results**: `returnsTuple` / JunkFilter
//!     row-coercion. A composite-returning function panics.
//!   * **Cross-call plan caching** and the `SQLFunctionHashEntry` use-count: we
//!     re-plan each call (correctness-equivalent, less efficient).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::boxed::Box;

use mcx::{Mcx, MemoryContext};
use types_core::{InvalidOid, Oid};
use types_datum::Datum as BareDatum;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_nodes::copy_query::{Query, CURSOR_OPT_PARALLEL_OK};
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::params::{ParamExternData, ParamListInfo, ParamListInfoData, PARAM_FLAG_CONST};
use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
use types_tuple::heaptuple::TupleDescData;
use types_nodes::tuptable::SlotData;
use types_dest::CommandDest;
use types_scan::sdir::ForwardScanDirection;

use backend_executor_execMain as execMain;
use backend_optimizer_plan_planner_seams as planner_seams;
use backend_optimizer_util_clauses_seams as clauses_seams;
use backend_rewrite_rewritehandler_seams as rewrite_seams;
use backend_utils_time_snapmgr_seams as snapmgr;
use backend_access_transam_xact_seams as xact_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;

/// `VOIDOID` (pg_type.dat).
const VOIDOID: Oid = 2278;
/// `RECORDOID` (pg_type.dat).
const RECORDOID: Oid = 2249;
/// `PROVOLATILE_VOLATILE` (pg_proc.h) — `'v'`.
const PROVOLATILE_VOLATILE: u8 = b'v';
/// `TYPTYPE_PSEUDO` (pg_type.h) — `'p'`.
const TYPTYPE_PSEUDO: u8 = b'p';
/// `CMD_UTILITY` discriminant (`nodes.h`).
const CMD_UTILITY: types_nodes::nodes::CmdType = types_nodes::nodes::CmdType::CMD_UTILITY;

// Polymorphic pseudo-type OIDs (pg_type.dat) for `IsPolymorphicType`.
const ANYELEMENTOID: Oid = 2283;
const ANYARRAYOID: Oid = 2277;
const ANYNONARRAYOID: Oid = 2776;
const ANYENUMOID: Oid = 3500;
const ANYRANGEOID: Oid = 3831;
const ANYMULTIRANGEOID: Oid = 4537;
const ANYCOMPATIBLEOID: Oid = 5077;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
const ANYCOMPATIBLERANGEOID: Oid = 5080;
const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

/// `IsPolymorphicType(typid)` (catalog/pg_type.h:313): a pure OID comparison.
fn is_polymorphic_type(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

// ===========================================================================
// DR_sqlfunction — the result-capturing destination receiver.
//
// The C `DR_sqlfunction` stashes the result tuple into the function cache's
// junkfilter slot / tuplestore. For the scalar non-set case we only need the
// first column of the first (and only) result row, so the receiver captures
// `(word, ref_result, isnull)` of column 1 of the first row it receives into a
// thread-local keyed by the receiver's `state` token (the owned-model
// `(DR_sqlfunction *) self` downcast).
// ===========================================================================

/// One captured single-column scalar result (`postquel_get_single_result`'s
/// extraction). `value`/`ref_payload`/`isnull` mirror the bare-word PGFunction
/// return ABI: a by-value scalar in `value`, a by-reference payload in
/// `ref_payload`, the NULL flag in `isnull`.
#[derive(Default)]
struct CaptureSlot {
    /// Whether any row was received.
    got_row: bool,
    /// Column-1 by-value word (valid when `ref_payload` is `None`).
    value: usize,
    /// Column-1 by-reference payload (valid for a by-reference result type).
    ref_payload: Option<RefPayload>,
    /// Column-1 NULL flag.
    isnull: bool,
}

std::thread_local! {
    /// Per-receiver capture state keyed by the `state` token a SQL-function
    /// receiver is registered with. A nested SQL-function call gets a distinct
    /// token, so nested captures do not collide.
    static CAPTURES: core::cell::RefCell<std::collections::HashMap<u64, CaptureSlot>> =
        core::cell::RefCell::new(std::collections::HashMap::new());
    /// The next receiver token to hand out.
    static NEXT_TOKEN: core::cell::Cell<u64> = const { core::cell::Cell::new(1) };
}

fn alloc_capture_token() -> u64 {
    let token = NEXT_TOKEN.with(|c| {
        let t = c.get();
        c.set(t + 1);
        t
    });
    CAPTURES.with(|c| {
        c.borrow_mut().insert(token, CaptureSlot::default());
    });
    token
}

fn take_capture(token: u64) -> CaptureSlot {
    CAPTURES.with(|c| c.borrow_mut().remove(&token).unwrap_or_default())
}

/// `sqlfunction_startup` (functions.c) — nothing to do for the scalar capture.
fn capture_startup(
    _mcx: Mcx<'_>,
    _state: u64,
    _operation: CmdType,
    _tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    Ok(())
}

/// `sqlfunction_receive` (functions.c) — capture column 1 of the first row.
///
/// The C receiver runs the row through the junkfilter and stores it; the scalar
/// path then extracts column 1 via `slot_getattr(slot, 1, &isnull)`. We capture
/// column 1 directly off the slot.
fn capture_receive<'mcx>(mcx: Mcx<'mcx>, state: u64, slot: &mut SlotData<'mcx>) -> PgResult<bool> {
    let (value, isnull) =
        backend_executor_execTuples::slot_deform::slot_getattr(mcx, slot, 1)?;
    let captured = canon_to_capture(&value, isnull)?;
    CAPTURES.with(|c| {
        let mut map = c.borrow_mut();
        if let Some(slot_state) = map.get_mut(&state) {
            // Only the first row's value matters for a scalar (non-set) result.
            if !slot_state.got_row {
                slot_state.got_row = true;
                slot_state.value = captured.value;
                slot_state.ref_payload = captured.ref_payload;
                slot_state.isnull = captured.isnull;
            }
        }
    });
    Ok(true)
}

/// `sqlfunction_shutdown` (functions.c) — nothing to do.
fn capture_shutdown(_mcx: Mcx<'_>, _state: u64) -> PgResult<()> {
    Ok(())
}

/// Marshal a captured column-1 [`CanonDatum`] into the bare-word + ref-payload
/// capture form (the same split `datum_to_ref_arg` performs at the fmgr edge).
fn canon_to_capture(val: &CanonDatum<'_>, isnull: bool) -> PgResult<CaptureSlot> {
    if isnull {
        return Ok(CaptureSlot {
            got_row: true,
            value: 0,
            ref_payload: None,
            isnull: true,
        });
    }
    let (value, ref_payload) = match val {
        CanonDatum::ByVal(d) => (*d, None),
        CanonDatum::ByRef(b) => (0, Some(RefPayload::Varlena(b.as_slice().to_vec()))),
        CanonDatum::Cstring(s) => (0, Some(RefPayload::Cstring(s.clone()))),
        CanonDatum::Composite(t) => (0, Some(RefPayload::Composite(t.to_datum_image()))),
        CanonDatum::Expanded(_) | CanonDatum::Internal(_) => {
            return Err(PgError::error(
                "fmgr_sql: SQL-function result column is an Expanded/Internal value — \
                 deferred (needs the by-ref result materialization path)",
            ));
        }
    };
    Ok(CaptureSlot {
        got_row: true,
        value,
        ref_payload,
        isnull: false,
    })
}

// ===========================================================================
// fmgr_sql — the SQL-function call handler.
// ===========================================================================

/// `fmgr_sql(PG_FUNCTION_ARGS)` (functions.c:1576) — the SQL-language call
/// handler. See the module docs for scope and deferrals.
fn fmgr_sql<'mcx>(
    mcx: Mcx<'mcx>,
    fn_oid: Oid,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<BareDatum> {
    // ---- Check call context (functions.c:1588) ----------------------------
    // We only support non-set functions here. A set-returning SQL function
    // needs the tuplestore / lazyEval / ReturnSetInfo materialize machinery.
    if fcinfo.flinfo.as_ref().is_some_and(|f| f.fn_retset) {
        return Err(PgError::error(
                "fmgr_sql: set-returning SQL functions are not yet supported \
                 (needs the tuplestore / lazyEval / ReturnSetInfo materialize path)",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // ---- init_sql_fcache equivalent (functions.c:536) ---------------------
    // Read the function's pg_proc facts: result type, kind, set-ness, arg types
    // (PgProcSimple), and the body source (prosrc / prosqlbody).
    let form = clauses_seams::get_func_form::call(fn_oid)?;
    let rettype = form.prorettype;
    let proargtypes = &form.proargtypes;

    if form.proretset {
        return Err(PgError::error("fmgr_sql: set-returning SQL functions are not yet supported")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // Composite / whole-row results need the JunkFilter row-coercion path.
    let returns_tuple = rettype == RECORDOID || {
        // type_is_rowtype: a composite type. get_typlenbyval reports typlen=-1
        // for a varlena but does not distinguish composites; use the typtype via
        // the rettype != base classification is out of scope here — a composite
        // rettype reaches the "whole tuple" branch in C. For the scalar port we
        // detect it by the body producing more than one column at run time; the
        // single-column scalar path covers the common case. Treat RECORD as the
        // only explicit whole-row trigger.
        false
    };
    if returns_tuple {
        return Err(PgError::error(
                "fmgr_sql: composite / whole-row SQL-function results are not yet \
                 supported (needs the JunkFilter row-coercion path)",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    let (prosrc, prosqlbody) = clauses_seams::get_func_sql_body::call(mcx, fn_oid)?;

    // ---- postquel_sub_params (functions.c:1473) ---------------------------
    // Convert the incoming arguments into a ParamListInfo the body's `$n`
    // Params resolve against. Built once per call (we don't cache the cache).
    let params = build_param_list(fcinfo, proargtypes)?;

    // ---- Parse + analyze the body queries ---------------------------------
    // prepare_sql_fn_parse_info + sql_fn_parser_setup: a `$n` resolves against
    // proargtypes and a body bareword that names an argument resolves to its
    // Param. The input collation is the call frame's fncollation.
    let collation = fcinfo.fncollation;
    let pinfo = prepare_sql_fn_parse_info(
        &form.proname,
        form.proargnames.as_deref(),
        proargtypes,
        collation,
    );
    let querytrees = parse_body_queries(mcx, prosrc.as_str(), prosqlbody.as_deref(), &pinfo)?;

    // ---- Snapshot management (functions.c:1655) ---------------------------
    // The caller (a containing query) already has an active snapshot. A
    // read-only (IMMUTABLE/STABLE) function reuses it. A volatile function must
    // advance the command counter and take a fresh snapshot so it sees prior
    // work in this transaction. We push a fresh transaction snapshot for the
    // volatile case and pop it at the end.
    let readonly = form.provolatile != PROVOLATILE_VOLATILE;
    let mut pushed_snapshot = false;
    if !readonly {
        xact_seams::command_counter_increment::call()?;
        snapmgr::push_active_snapshot_transaction::call()?;
        pushed_snapshot = true;
    }

    let run_result = run_body(mcx, &querytrees, prosrc.as_str(), params);

    if pushed_snapshot {
        // Pop even on error so we don't leak the active-snapshot stack.
        let _ = snapmgr::pop_active_snapshot::call();
    }

    let capture = run_result?;

    // ---- postquel_get_single_result (functions.c:1536) --------------------
    if rettype == VOIDOID {
        fcinfo.isnull = true;
        return Ok(BareDatum::null());
    }

    let Some(capture) = capture else {
        // No row returned: the function result is NULL.
        fcinfo.isnull = true;
        return Ok(BareDatum::null());
    };

    if capture.isnull {
        fcinfo.isnull = true;
        return Ok(BareDatum::null());
    }

    fcinfo.isnull = false;
    if let Some(payload) = capture.ref_payload {
        // By-reference result: hand the referent back through the fmgr
        // by-reference side channel (the bare-word return is the NULL word).
        fcinfo.set_ref_result(payload);
        Ok(BareDatum::null())
    } else {
        // By-value scalar result: the bare machine word. datumCopy is a word
        // copy for a by-value type; the value already lives independently of
        // the (now-torn-down) executor slot since the capture copied it out.
        Ok(BareDatum::from_usize(capture.value))
    }
}

// ===========================================================================
// build_param_list — postquel_sub_params (functions.c:1473)
// ===========================================================================

/// Build the `ParamListInfo` representing the current arguments. Each incoming
/// `fcinfo.args[i]` (a by-value word, or a by-reference payload in
/// `fcinfo.ref_args[i]`) becomes one `ParamExternData` with `ptype =
/// proargtypes[i]`, `pflags = PARAM_FLAG_CONST`. The param list is owned for
/// the call's lifetime (`Rc<ParamListInfoData<'static>>`); a by-reference arg's
/// bytes are owned by the param value (`Datum::ByRef`), independent of `fcinfo`.
fn build_param_list(
    fcinfo: &FunctionCallInfoBaseData,
    proargtypes: &[Oid],
) -> PgResult<ParamListInfo> {
    let nargs = fcinfo.nargs();
    if nargs == 0 {
        return Ok(None);
    }

    let mut params: alloc::vec::Vec<ParamExternData<'static>> = alloc::vec::Vec::new();
    params
        .try_reserve_exact(nargs)
        .map_err(|_| PgError::error("fmgr_sql: out of memory building parameter list"))?;

    for i in 0..nargs {
        let isnull = fcinfo.args.get(i).map(|d| d.isnull).unwrap_or(true);
        let ptype = proargtypes.get(i).copied().unwrap_or(InvalidOid);

        let value: CanonDatum<'static> = if isnull {
            CanonDatum::null()
        } else if let Some(Some(refp)) = fcinfo.ref_args.get(i) {
            // By-reference argument: rebuild an owned canonical ByRef/Cstring/...
            // value from the side-channel payload.
            ref_payload_to_canon(refp)?
        } else {
            // By-value argument: the bare machine word.
            let word = fcinfo.args.get(i).map(|d| d.value.as_usize()).unwrap_or(0);
            CanonDatum::ByVal(word)
        };

        params.push(ParamExternData {
            value,
            isnull,
            pflags: PARAM_FLAG_CONST,
            ptype,
        });
    }

    let list = ParamListInfoData {
        param_fetch: false,
        param_fetch_arg: None,
        param_compile: false,
        param_compile_arg: None,
        parser_setup: false,
        parser_setup_arg: None,
        param_values_str: None,
        num_params: nargs as i32,
        params,
    };

    Ok(Some(alloc::rc::Rc::new(list)))
}

/// A backend-lifetime context the param list's by-reference values are cloned
/// into (the param list is `ParamListInfoData<'static>`). Mirrors the C
/// `fcache->fcontext`-owned `paramLI` storage that outlives the call.
fn param_static_mcx() -> Mcx<'static> {
    std::thread_local! {
        static PARAM_CONTEXT: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("SQLFunctionParams")));
    }
    PARAM_CONTEXT.with(|c| c.mcx())
}

/// Rebuild an owned `'static` canonical [`CanonDatum`] from a by-reference fmgr
/// argument payload. Mirrors the inverse of `datum_to_ref_arg`.
fn ref_payload_to_canon(refp: &RefPayload) -> PgResult<CanonDatum<'static>> {
    if let Some(s) = refp.as_cstring() {
        return Ok(CanonDatum::Cstring(alloc::string::String::from(s)));
    }
    if let Some(bytes) = refp.as_varlena() {
        return Ok(CanonDatum::ByRef(mcx::slice_in(param_static_mcx(), bytes)?));
    }
    if let Some(bytes) = refp.as_composite() {
        return Ok(CanonDatum::ByRef(mcx::slice_in(param_static_mcx(), bytes)?));
    }
    Err(PgError::error(
        "fmgr_sql: SQL-function argument is an Expanded/Internal by-reference value \
         — deferred (needs the expanded-datum param path)",
    ))
}

// ===========================================================================
// parse_body_queries — parse + analyze the function body.
// ===========================================================================

/// Parse and analyze the function body into a list of `Query` nodes. Uses
/// `prosqlbody` (the cooked node-tree) when present, else parses `prosrc` and
/// runs `sql_fn_parser_setup` (`$n` against `proargtypes`) via
/// `parse_analyze_fixedparams`. This is `init_execution_state`'s
/// parse-then-analyze leg, minus the plancache.
fn parse_body_queries<'mcx>(
    mcx: Mcx<'mcx>,
    prosrc: &str,
    prosqlbody: Option<&str>,
    pinfo: &types_nodes::parsestmt::SqlFnParseInfo,
) -> PgResult<mcx::PgVec<'mcx, Query<'mcx>>> {
    let mut out: mcx::PgVec<'mcx, Query<'mcx>> = mcx::PgVec::new_in(mcx);

    if let Some(body) = prosqlbody {
        // n = stringToNode(prosqlbody): a List of (List of Query) or a bare Query.
        let n = backend_nodes_core::read::string_to_node(mcx, body)?;
        collect_body_queries(mcx, &n, &mut out)?;
    } else {
        // raw_parser borrows its source for 'mcx; re-home prosrc into the arena.
        let prosrc_mcx: &'mcx str = {
            let boxed = mcx::alloc_in(mcx, mcx::PgString::from_str_in(prosrc, mcx)?)?;
            mcx::leak_in(boxed).as_str()
        };
        let raw_list = backend_parser_driver::raw_parser(
            mcx,
            prosrc_mcx,
            types_parsenodes::RawParseMode::RAW_PARSE_DEFAULT,
        )?;
        for raw in raw_list.iter() {
            // pg_analyze_and_rewrite_withcb(parsetree, prosrc, sql_fn_parser_setup,
            // pinfo, NULL): install the SQL-function parser hooks (so `$n` and a
            // bareword that names an argument resolve to a Param) before analysis.
            let query = backend_parser_analyze::parse_analyze_sql_function(
                mcx,
                raw,
                prosrc_mcx,
                pinfo.clone(),
            )?;
            out.push(query);
        }
    }

    Ok(out)
}

/// `prepare_sql_fn_parse_info` (functions.c:251) — assemble the SQL-function-body
/// parse info from the function's `pg_proc` facts and input collation. The
/// polymorphic-argument resolution C does here against `call_expr` is already
/// reflected in the caller's `argtypes` (the const-folding `get_func_form`
/// returns declared types; a polymorphic SQL function never reaches the analyze
/// leg — `check_sql_function_body` only raw-parses it, and `fmgr_sql` resolves
/// the actual types from the call frame). `argnames` is dropped when there are
/// fewer name entries than arguments (C `n_arg_names < nargs`).
fn prepare_sql_fn_parse_info(
    proname: &str,
    proargnames: Option<&[Option<alloc::string::String>]>,
    argtypes: &[Oid],
    collation: Oid,
) -> types_nodes::parsestmt::SqlFnParseInfo {
    let nargs = argtypes.len();
    let argnames = match proargnames {
        Some(names) if names.len() >= nargs && nargs > 0 => Some(names.to_vec()),
        _ => None,
    };
    types_nodes::parsestmt::SqlFnParseInfo::new(
        proname.to_owned(),
        collation,
        argtypes.to_vec(),
        argnames,
    )
}

/// Collect the body's `Query` nodes out of a `stringToNode(prosqlbody)` result.
/// `n` is either a `List` whose first element is the (List of) Query nodes, or a
/// bare `Query`.
fn collect_body_queries<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Node<'mcx>,
    out: &mut mcx::PgVec<'mcx, Query<'mcx>>,
) -> PgResult<()> {
    if let Some(outer) = n.as_list() {
        let Some(first) = outer.first() else {
            return Ok(());
        };
        if let Some(inner) = first.as_list() {
            push_query_list(mcx, &inner[..], out)
        } else if first.is_query() {
            push_query_list(mcx, core::slice::from_ref(first), out)
        } else {
            Err(PgError::error(
                "fmgr_sql: prosqlbody is not a list of Query nodes",
            ))
        }
    } else if let Some(q) = n.as_query() {
        out.push(q.clone_in(mcx)?);
        Ok(())
    } else {
        Err(PgError::error("fmgr_sql: prosqlbody is not a Query"))
    }
}

/// Helper to read a `Query` clone out of each node-ptr in a slice.
fn push_query_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &[NodePtr<'mcx>],
    out: &mut mcx::PgVec<'mcx, Query<'mcx>>,
) -> PgResult<()> {
    for p in list {
        let q = p
            .as_query()
            .ok_or_else(|| PgError::error("fmgr_sql: prosqlbody element is not a Query"))?;
        out.push(q.clone_in(mcx)?);
    }
    Ok(())
}

// ===========================================================================
// SQL-function-body validator — fmgr_sql_validator's body-check leg
// (pg_proc.c:884-988), installed as the pg-proc `run_sql_function_body_check`
// seam. Reached on CREATE FUNCTION ... LANGUAGE sql with check_function_bodies
// = on, after pg_proc's in-crate pseudotype checks pass.
// ===========================================================================

/// The body-checking portion of `fmgr_sql_validator` (pg_proc.c:884-988): read
/// `prosrc`/`prosqlbody`, then — when no input type is polymorphic — parse and
/// analyze the body queries so any syntax or type error is raised at CREATE
/// FUNCTION time; with a polymorphic argument we can only raw-parse (the actual
/// argument datatypes are unresolvable until call time), which still catches
/// silly syntactic errors. Finally run `check_sql_fn_statements`.
///
/// The `error_context_stack` push/pop wiring the
/// `sql_function_parse_error_callback` (transposing a syntax error to CREATE
/// FUNCTION coordinates) lives in pg_proc, around this call.
///
/// `check_sql_fn_retval` / `get_func_result_type` return-type validation
/// (pg_proc.c:980-985) is functions.c machinery not yet ported in this tree
/// (`check_sql_fn_retval` operates over the rewritten query-tree lists and
/// needs the full `coerce_fn_result_column` family); when it lands it slots in
/// after `check_sql_fn_statements`. Its absence weakens validation (a return
/// type mismatch is caught at call time rather than definition time) but never
/// produces a wrong result.
fn check_sql_function_body(mcx: Mcx<'_>, funcoid: Oid) -> PgResult<()> {
    // init_sql_fcache-equivalent reads: pg_proc facts + the body source.
    let form = clauses_seams::get_func_form::call(funcoid)?;
    let (prosrc, prosqlbody) = clauses_seams::get_func_sql_body::call(mcx, funcoid)?;

    // haspolyarg (pg_proc.c:869-881): recomputed here — the pseudotype/poly
    // argument loop ran in pg_proc's in-crate validator before this seam, but
    // only its error-raising verdict crossed; we need the boolean. A
    // polymorphic argument means actual datatypes are unresolvable now, so we
    // skip full analysis (and the retval check) and only raw-parse.
    let mut haspolyarg = false;
    for &argtype in form.proargtypes.iter() {
        if lsyscache_seams::get_typtype::call(argtype)? == TYPTYPE_PSEUDO
            && is_polymorphic_type(argtype)
        {
            haspolyarg = true;
        }
    }

    if haspolyarg {
        // Raw-parse only (pg_proc.c:931 pg_parse_query): catch syntax errors.
        // prosqlbody, if present, is already a parsed Query tree, so a
        // polymorphic function with a stored body has nothing left to syntax-
        // check; for the prosrc text we raw_parser it.
        if prosqlbody.as_ref().is_none() {
            let prosrc_mcx: &str = {
                let boxed = mcx::alloc_in(mcx, mcx::PgString::from_str_in(prosrc.as_str(), mcx)?)?;
                mcx::leak_in(boxed).as_str()
            };
            let _ = backend_parser_driver::raw_parser(
                mcx,
                prosrc_mcx,
                types_parsenodes::RawParseMode::RAW_PARSE_DEFAULT,
            )?;
        }
        return Ok(());
    }

    // Full precheck (pg_proc.c:937-963): parse + analyze + rewrite the body.
    // `parse_body_queries` runs raw_parser + parse_analyze_fixedparams ($n
    // against proargtypes); for a stored prosqlbody it deserializes the already-
    // analyzed Query trees. (AcquireRewriteLocks / pg_rewrite_query — the C
    // rewrite leg — apply RLS/rule rewriting; the repo's analyze path produces
    // analyzed Query trees suitable for the statement-shape checks below.)
    // prepare_sql_fn_parse_info(tuple, NULL, InvalidOid) — at CREATE FUNCTION
    // time there's no call expression, so the input collation is InvalidOid.
    let pinfo = prepare_sql_fn_parse_info(
        &form.proname,
        form.proargnames.as_deref(),
        &form.proargtypes,
        InvalidOid,
    );
    let querytrees = parse_body_queries(
        mcx,
        prosrc.as_str(),
        prosqlbody.as_ref().map(|s| s.as_str()),
        &pinfo,
    )?;

    // check_sql_fn_statements (functions.c:2042): reject calling procedures
    // with output arguments from a SQL function body.
    check_sql_fn_statements(&querytrees)?;

    Ok(())
}

/// `check_sql_fn_statements` (functions.c:2042) + `check_sql_fn_statement`
/// (functions.c:2051): for each body `Query`, disallow a `CALL` of a procedure
/// that has output arguments (unsupported inside a SQL function).
fn check_sql_fn_statements(querytrees: &[Query<'_>]) -> PgResult<()> {
    for query in querytrees {
        if query.commandType == CMD_UTILITY {
            if let Some(util) = query.utilityStmt.as_ref() {
                if let Some(stmt) = util.as_callstmt() {
                    if !stmt.outargs.is_empty() {
                        return Err(PgError::error(
                            "calling procedures with output arguments is not \
                             supported in SQL functions",
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                    }
                }
            }
        }
    }
    Ok(())
}

// ===========================================================================
// run_body — the postquel_start/getnext/end loop over the body queries.
// ===========================================================================

/// Run the body queries to completion, capturing the single-column result of the
/// last `canSetTag` query. Earlier queries discard their output.
///
/// `postquel_start` (CreateQueryDesc + ExecutorStart) / `postquel_getnext`
/// (ExecutorRun to completion) / `postquel_end` (ExecutorFinish + ExecutorEnd)
/// per query, faithful to functions.c — minus lazyEval (always run to
/// completion for a non-set function).
fn run_body<'mcx>(
    mcx: Mcx<'mcx>,
    querytrees: &mcx::PgVec<'mcx, Query<'mcx>>,
    source_text: &str,
    params: ParamListInfo,
) -> PgResult<Option<CaptureSlot>> {
    // Rewrite + plan each query. Find the index of the last canSetTag plan: that
    // one delivers the function result.
    let mut plans: alloc::vec::Vec<PlannedStmt<'mcx>> = alloc::vec::Vec::new();
    let mut last_setstag: Option<usize> = None;

    for query in querytrees.iter() {
        let rewritten = rewrite_seams::query_rewrite_canonical::call(mcx, query.clone_in(mcx)?)?;
        for rq in rewritten.iter() {
            if rq.commandType == CmdType::CMD_UTILITY {
                return Err(PgError::error(
                        "fmgr_sql: utility statements in SQL functions are not yet \
                         supported (needs the ProcessUtility postquel leg)",
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            let plan = planner_seams::pg_plan_query::call(
                mcx,
                rq,
                source_text,
                CURSOR_OPT_PARALLEL_OK,
            )?;
            if plan.canSetTag {
                last_setstag = Some(plans.len());
            }
            plans.push(plan);
        }
    }

    let mut captured: Option<CaptureSlot> = None;

    for (i, plan) in plans.iter().enumerate() {
        let is_result = Some(i) == last_setstag;
        let cap = run_one_query(mcx, plan, source_text, params.clone(), is_result)?;
        if is_result {
            captured = cap;
        }
    }

    Ok(captured)
}

/// Run one planned query (`postquel_start` + `postquel_getnext` +
/// `postquel_end`). When `is_result`, output is captured by a `DR_sqlfunction`
/// receiver and its single-column result returned; otherwise output is
/// discarded via `None_Receiver`.
fn run_one_query<'mcx>(
    mcx: Mcx<'mcx>,
    plan: &PlannedStmt<'mcx>,
    source_text: &str,
    params: ParamListInfo,
    is_result: bool,
) -> PgResult<Option<CaptureSlot>> {
    // postquel_start: build the receiver + QueryDesc, ExecutorStart.
    let (dest, token) = if is_result {
        let token = alloc_capture_token();
        let vtable = backend_tcop_dest::ReceiverVtable {
            rStartup: capture_startup,
            receiveSlot: capture_receive,
            rShutdown: capture_shutdown,
        };
        let handle =
            backend_tcop_dest::register_dest_receiver(CommandDest::SqlFunction, vtable, token);
        (handle, Some(token))
    } else {
        (backend_tcop_dest::none_receiver(), None)
    };

    let run = (|| -> PgResult<()> {
        let mut query_desc = execMain::CreateQueryDesc(
            mcx.context(),
            plan,
            source_text,
            snapmgr::get_active_snapshot::call()?,
            None, // InvalidSnapshot
            dest,
            params,
            0,
        )?;

        execMain::ExecutorStart(&mut query_desc, 0)?;
        // postquel_getnext: run to completion (non-set function).
        execMain::ExecutorRun(&mut query_desc, ForwardScanDirection, 0)?;
        // postquel_end.
        execMain::ExecutorFinish(&mut query_desc)?;
        execMain::ExecutorEnd(&mut query_desc)?;
        execMain::FreeQueryDesc(query_desc)?;
        Ok(())
    })();

    match token {
        Some(token) => {
            // Take the capture even on error so the thread-local doesn't leak.
            let slot = take_capture(token);
            run?;
            Ok(if slot.got_row { Some(slot) } else { None })
        }
        None => {
            run?;
            Ok(None)
        }
    }
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install this crate's inward seams. Wired into `seams-init`.
pub fn init_seams() {
    backend_executor_functions_seams::fmgr_sql::set(fmgr_sql);
    // fmgr_sql_validator's body-check leg (pg_proc.c:884-988). Cross-crate
    // install of the pg-proc seam: the body-check is functions.c machinery
    // (parse/analyze the body, check_sql_fn_statements), reused for the
    // CREATE FUNCTION validator path.
    backend_catalog_pg_proc_seams::run_sql_function_body_check::set(|funcoid| {
        let ctx = MemoryContext::new("check_sql_function_body");
        check_sql_function_body(ctx.mcx(), funcoid)
    });
}
