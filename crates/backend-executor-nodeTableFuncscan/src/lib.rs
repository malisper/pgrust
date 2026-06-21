//! Port of `src/backend/executor/nodeTableFuncscan.c` — support routines for
//! scanning a `RangeTableFunc` (`XMLTABLE` / `JSON_TABLE` table-producer
//! functions).
//!
//! INTERFACE ROUTINES
//! - [`ExecTableFuncScan`]      - scans a function
//! - [`ExecInitTableFuncScan`]  - creates and initializes a TableFuncscan node
//! - [`ExecEndTableFuncScan`]   - releases any storage allocated
//! - [`ExecReScanTableFuncScan`]- rescans the function
//!
//! plus the file-scope statics `TableFuncNext` / `TableFuncRecheck` (the access
//! and recheck methods the generic `execScan.c` driver re-enters) and
//! `tfuncFetchRows` / `tfuncInitialize` / `tfuncLoadRows` (the producer-to-
//! tuplestore loader).
//!
//! The node state is the owned [`TableFuncScanState`] mutated through `&mut`
//! borrows; the C `PlanState.state` back-pointer is replaced by threading
//! `&mut EStateData` explicitly. Calls into unported owners — the generic scan
//! driver (`execScan.c`), expression init/eval (`execExpr.c`/`execExprInterp.c`),
//! tuple-slot ops and result-type setup (`execTuples.c`/`execUtils.c`),
//! descriptor construction (`tupdesc.c`), the output tuplestore
//! (`tuplestore.c`), type-IO lookup (`lsyscache.c`/`fmgr.c`),
//! `text`→`cstring` conversion (`varlena.c`), `bms_is_member` (`bitmapset.c`),
//! `work_mem` (`globals.c`), `CHECK_FOR_INTERRUPTS` (`tcop/postgres.c`), and
//! the `TableFuncRoutine` table-builder methods (`xml.c`/`jsonpath_exec.c`) —
//! go through those owners' seam crates and panic until the owners land.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_access_common_toastdesc_seams as tupdesc;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execScan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_tablefuncRoutine_seams as routine;
use backend_nodes_core_seams as nodes_core;
use backend_nodes_nodeFuncs_seams as nodeFuncs;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_adt_jsonpath_exec as jsonpath_exec;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_adt_xml as xml;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_init_small_seams as globals;
use backend_utils_sort_storage_seams as tuplestore;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use types_error::{PgError, PgResult};
use types_nodes::{
    EStateData, EcxtId, SlotId, TableFuncRoutineKind, TableFuncScan, TableFuncScanState,
};

/// `EXEC_FLAG_MARK` (executor.h) — caller needs mark/restore support. A
/// table-func scan never supports mark/restore (asserted away in init).
use types_nodes::executor::EXEC_FLAG_MARK;

/// Install this crate's implementations into its seam slots.
///
/// nodeTableFuncscan has no `<unit>-seams` crate of its own: callers that need
/// these functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates.
///
/// This crate also hosts the `TableFuncRoutine` vtable dispatch: C reaches the
/// table-builder methods through a `const TableFuncRoutine *routine`
/// function-pointer table on the node, of which only two instances exist —
/// `XmlTableRoutine` (`xml.c`) and `JsonbTableRoutine` (`jsonpath_exec.c`). The
/// owned model carries that identity as [`TableFuncRoutineKind`]; here each of
/// the nine [`routine`] seams dispatches on the kind to the concrete builder.
///
/// The `XmlTable` arm forwards to the [`xml`] crate's `XmlTable*` entry points,
/// which route to the libxml provider when installed and otherwise raise the
/// `--without-libxml` `unsupported XML feature` error — exactly the C
/// `XmlTableRoutine` behaviour. (These methods carry no per-call `state`: the
/// libxml parser-context lifecycle is wholly internal to the provider, so the
/// `&mut TableFuncScanState` / document `Datum` are unused on this arm.)
///
/// The `JsonbTable` arm routes into the `jsonpath_exec` crate's `JsonTable*`
/// row-pattern builder (`JsonTableInitOpaque` / `SetDocument` / `FetchRow` /
/// `JsonTableCurrentRow` / `DestroyOpaque`), with this crate building the root
/// `JsonTablePlan` from `tf->plan` and evaluating the PASSING / column
/// `JsonExpr` expressions via `ExecEvalExpr`.
pub fn init_seams() {
    routine::routine_init_opaque::set(routine_init_opaque);
    routine::routine_set_document::set(routine_set_document);
    routine::routine_set_namespace::set(routine_set_namespace);
    routine::routine_has_set_row_filter::set(routine_has_set_row_filter);
    routine::routine_set_row_filter::set(routine_set_row_filter);
    routine::routine_set_column_filter::set(routine_set_column_filter);
    routine::routine_fetch_row::set(routine_fetch_row);
    routine::routine_get_value::set(routine_get_value);
    routine::routine_destroy_opaque::set(routine_destroy_opaque);
}

// ===========================================================================
//                  TableFuncRoutine vtable dispatch (tablefunc.h)
//
// C dispatches every builder call through `routine->Method(state, ...)`, where
// `routine` is one of the two `const TableFuncRoutine` instances. The owned
// model keys that dispatch on `TableFuncRoutineKind`. The XML half routes to the
// `xml` crate's `--without-libxml` entry points; the JSON_TABLE half routes to
// the `jsonpath_exec` crate's `JsonTable*` row-pattern builder.
// ===========================================================================

/// `JsonbTableRoutine` leaves `SetNamespace` / `SetRowFilter` /
/// `SetColumnFilter` NULL — only XMLTABLE drives those. Reaching one for
/// JSON_TABLE is a defensive programming-error guard (the caller's
/// `ns_uris`/`SetRowFilter`-presence/`colexprs` gating never lands here).
fn json_table_not_wired(what: &str) -> PgError {
    PgError::error(alloc::format!(
        "JsonbTableRoutine has no {what} method (NULL in C); only XMLTABLE uses it"
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// Borrow the `JsonTableExecContext` out of an `Opaque` (`state->opaque`),
/// panicking if it is absent or of the wrong kind (`InitOpaque` runs first, so
/// absence is a programming error — same as the C `GetJsonTableExecContext`
/// magic check's precondition).
fn json_exec_context(
    opaque: &mut types_nodes::execnodes::Opaque,
) -> &mut jsonpath_exec::JsonTableExecContext {
    opaque
        .0
        .as_mut()
        .expect("JsonbTableRoutine: state->opaque is NULL (InitOpaque not run)")
        .downcast_mut::<jsonpath_exec::JsonTableExecContext>()
        .expect("JsonbTableRoutine: state->opaque is not a JsonTableExecContext")
}

/// `routine->InitOpaque(state, natts)`.
fn routine_init_opaque<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    natts: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match kind {
        TableFuncRoutineKind::XmlTable => xml::XmlTableInitOpaque(natts),
        // C: `JsonTableInitOpaque` (jsonpath_exec.c:4109). The plan-walk
        // machinery is owned by `jsonpath_exec`; this arm — which holds the
        // `TableFuncScanState` and the expression evaluator — builds the root
        // `JsonTablePlan` from `tf->plan`, evaluates the PASSING argument
        // expressions, and hands those (plus the column count) to
        // `JsonTableInitOpaque`. `natts` is unused (C ignores it too).
        TableFuncRoutineKind::JsonbTable => json_table_init_opaque(state, estate),
    }
}

/// `routine->SetDocument(state, value)`.
fn routine_set_document<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    value: Datum<'mcx>,
) -> PgResult<()> {
    match kind {
        // C: XmlTableSetDocument(state, value) — `value` is the xmltype document
        // Datum (a varlena). C does `str = xml_out_internal(DatumGetXmlP(value),
        // 0)`, where `xml_out_internal` starts from `text_to_cstring((text *) x)`
        // — the detoasted, VARHDRSZ-stripped payload. The by-reference Datum here
        // carries the verbatim (header-ful, possibly short-header/compressed)
        // varlena image, so resolve its flat payload via `text_to_cstring_v`
        // before the xml crate renders it through `xml_out_internal`/libxml.
        TableFuncRoutineKind::XmlTable => {
            let per_table = state
                .perTableCxt
                .as_ref()
                .expect("XmlTableRoutine: perTableCxt not initialized");
            let payload = varlena::text_to_cstring_v::call(per_table.mcx(), &value)?;
            xml::XmlTableSetDocument(payload.as_bytes())
        }
        // C: `JsonTableSetDocument` (jsonpath_exec.c:4238) —
        // `DatumGetJsonbP(value)` then evaluate the root row pattern. The
        // document is the input jsonb varlena bytes.
        TableFuncRoutineKind::JsonbTable => {
            // C: DatumGetJsonbP detoasts; here the document value already
            // carries its flat bytes (the common, non-toasted case).
            let bytes = value.as_ref_bytes().to_vec();
            // Disjoint field borrows: `perTableCxt` (for the Mcx) and `opaque`
            // (the context). Borrow the fields directly so the borrow checker
            // sees them as disjoint.
            let TableFuncScanState { perTableCxt, opaque, .. } = state;
            let per_table = perTableCxt
                .as_ref()
                .expect("JsonbTableRoutine: perTableCxt not initialized");
            let cxt = json_exec_context(opaque);
            jsonpath_exec::JsonTableSetDocument(per_table.mcx(), cxt, &bytes)
        }
    }
}

/// `routine->SetNamespace(state, name, uri)`.
fn routine_set_namespace(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    name: Option<&str>,
    uri: &str,
) -> PgResult<()> {
    match kind {
        TableFuncRoutineKind::XmlTable => xml::XmlTableSetNamespace(name, uri),
        // C: `JsonbTableRoutine.SetNamespace == NULL`; the caller only reaches
        // this for XMLTABLE (driven by `ns_uris`/`ns_names`), so JSON_TABLE
        // never lands here. Guard defensively.
        TableFuncRoutineKind::JsonbTable => Err(json_table_not_wired("SetNamespace")),
    }
}

/// `routine->SetRowFilter != NULL`.
fn routine_has_set_row_filter(kind: TableFuncRoutineKind) -> bool {
    match kind {
        // `XmlTableRoutine.SetRowFilter` is non-NULL.
        TableFuncRoutineKind::XmlTable => true,
        // `JsonbTableRoutine.SetRowFilter == NULL`.
        TableFuncRoutineKind::JsonbTable => false,
    }
}

/// `routine->SetRowFilter(state, path)`.
fn routine_set_row_filter(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    path: &str,
) -> PgResult<()> {
    match kind {
        TableFuncRoutineKind::XmlTable => xml::XmlTableSetRowFilter(path),
        // `JsonbTableRoutine.SetRowFilter == NULL`: only called when
        // `routine_has_set_row_filter`, which is `false` for JSON_TABLE.
        TableFuncRoutineKind::JsonbTable => Err(json_table_not_wired("SetRowFilter")),
    }
}

/// `routine->SetColumnFilter(state, path, colnum)`.
fn routine_set_column_filter(
    _state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
    path: &str,
    colnum: i32,
) -> PgResult<()> {
    match kind {
        TableFuncRoutineKind::XmlTable => xml::XmlTableSetColumnFilter(path, colnum),
        // `JsonbTableRoutine.SetColumnFilter == NULL`; reached only for XMLTABLE.
        TableFuncRoutineKind::JsonbTable => Err(json_table_not_wired("SetColumnFilter")),
    }
}

/// `routine->FetchRow(state)`.
fn routine_fetch_row(
    state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
) -> PgResult<bool> {
    match kind {
        TableFuncRoutineKind::XmlTable => xml::XmlTableFetchRow(),
        // C: `JsonTableFetchRow` (jsonpath_exec.c:4436) — advance the root plan.
        TableFuncRoutineKind::JsonbTable => {
            let TableFuncScanState { perTableCxt, opaque, .. } = state;
            let per_table = perTableCxt
                .as_ref()
                .expect("JsonbTableRoutine: perTableCxt not initialized");
            let cxt = json_exec_context(opaque);
            jsonpath_exec::JsonTableFetchRow(per_table.mcx(), cxt)
        }
    }
}

/// `routine->GetValue(state, colnum, typid, typmod, &isnull)`.
fn routine_get_value<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    colnum: i32,
    typid: types_core::primitive::Oid,
    typmod: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    match kind {
        // C: result = InputFunctionCall(&state->in_functions[colnum],
        //                cstr, state->typioparams[colnum], typmod);
        // The xml/libxml provider produces the column's textual value (or None =
        // *isnull); the executor owns in_functions/typioparams and runs the input
        // function — exactly where the colfilter eval also lives.
        TableFuncRoutineKind::XmlTable => {
            let cstr = xml::XmlTableGetValue(colnum, typid)?;
            match cstr {
                None => Ok((Datum::null(), true)),
                Some(s) => {
                    let mcx = estate.es_query_cxt;
                    let in_funcid = state.in_functions[colnum as usize].fn_oid;
                    let typioparam = state.typioparams[colnum as usize];
                    let d = fmgr::input_function_call::call(
                        mcx,
                        in_funcid,
                        Some(s.as_str()),
                        typioparam,
                        typmod,
                    )?;
                    Ok((d, false))
                }
            }
        }
        // C: `JsonTableGetValue` (jsonpath_exec.c:4452).
        TableFuncRoutineKind::JsonbTable => {
            json_table_get_value(state, colnum, estate)
        }
    }
}

/// `routine->DestroyOpaque(state)`.
fn routine_destroy_opaque(
    state: &mut TableFuncScanState<'_>,
    kind: TableFuncRoutineKind,
) -> PgResult<()> {
    match kind {
        TableFuncRoutineKind::XmlTable => {
            let r = xml::XmlTableDestroyOpaque();
            // C clears `state->opaque` after DestroyOpaque; mirror that even on
            // the error/no-libxml path so the caller's `opaque != NULL` cleanup
            // guard sees the slot emptied.
            state.opaque.0 = None;
            r
        }
        // C: `JsonTableDestroyOpaque` (jsonpath_exec.c:4174) — invalidate the
        // context magic, then clear `state->opaque`.
        TableFuncRoutineKind::JsonbTable => {
            {
                let cxt = json_exec_context(&mut state.opaque);
                jsonpath_exec::JsonTableDestroyOpaque(cxt)?;
            }
            state.opaque.0 = None;
            Ok(())
        }
    }
}

// ===========================================================================
//        JSON_TABLE provider integration (jsonpath_exec.c JsonTable*)
//
// The JSON_TABLE row-pattern plan-walk machinery is owned by the
// `jsonpath_exec` crate; the parts that touch executor state — building the
// root `JsonTablePlan` from `tf->plan`, evaluating the PASSING / column
// `JsonExpr` expressions via `ExecEvalExpr` — live here, where the
// `TableFuncScanState` and the EState/`ExprContext` are reachable.
// ===========================================================================

/// C: `JsonTableInitOpaque` (jsonpath_exec.c:4109). Build the root
/// `JsonTablePlan` from `tf->plan`, evaluate the PASSING argument expressions,
/// hand both (plus the column count) to `jsonpath_exec::JsonTableInitOpaque`,
/// and install the result in `state->opaque`.
fn json_table_init_opaque<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tf = castNode(TableFuncScan, ps->plan)->tablefunc — the plan node aliases
    // the shared read-only plan tree (`'mcx`), so this borrow is independent of
    // `&mut state`.
    let tf: &'mcx types_nodes::primnodes::TableFunc<'mcx> = match state.ss.ps.plan {
        Some(p) => &p.expect_tablefuncscan().tablefunc,
        None => panic!("JsonTableInitOpaque: plan is not a TableFuncScan node"),
    };

    // rootplan = (JsonTablePlan *) tf->plan
    let rootplan_node = tf
        .plan
        .as_deref()
        .expect("JsonTableInitOpaque: tf->plan is NULL for JSON_TABLE");
    let rootplan = build_json_table_plan(rootplan_node)?;

    // cxt->colplanstates is sized to list_length(tf->colvalexprs).
    let ncols = state.colvalexprs.len();

    // Evaluate the PASSING arguments. je = castNode(JsonExpr, tf->docexpr);
    // forboth(state->passingvalexprs, je->passing_names).
    let args = eval_passing_args(state, tf, estate)?;

    let cxt = jsonpath_exec::JsonTableInitOpaque(rootplan, args, ncols)?;
    state.opaque = types_nodes::execnodes::Opaque(Some(alloc::boxed::Box::new(cxt)));
    Ok(())
}

/// C: `JsonTableInitOpaque`'s PASSING-args loop (jsonpath_exec.c:4127-4153).
/// `forboth(exprlc, state->passingvalexprs, namelc, je->passing_names)`:
/// evaluate each PASSING expression and pair it with its name.
fn eval_passing_args<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    tf: &types_nodes::primnodes::TableFunc<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<alloc::vec::Vec<jsonpath_exec::JsonTableVariable>> {
    let mut args: alloc::vec::Vec<jsonpath_exec::JsonTableVariable> = alloc::vec::Vec::new();
    if state.passingvalexprs.is_empty() {
        return Ok(args);
    }

    // je = castNode(JsonExpr, tf->docexpr) — the document JsonExpr carries the
    // PASSING argument names.
    let passing_names: &[alloc::string::String] = match tf.docexpr.as_deref() {
        Some(types_nodes::primnodes::Expr::JsonExpr(je)) => &je.passing_names,
        _ => panic!(
            "JsonTableInitOpaque: tf->docexpr is not a JsonExpr (required for JSON_TABLE PASSING)"
        ),
    };

    debug_assert_eq!(state.passingvalexprs.len(), passing_names.len());
    let econtext = node_econtext(state);

    for i in 0..state.passingvalexprs.len() {
        // var->typid = exprType(state->expr); var->typmod = exprTypmod(...).
        // The ExprState's `.expr` is the original Expr; read its type triple.
        let (typid, typmod) = {
            let exprstate = state.passingvalexprs[i]
                .as_ref()
                .expect("JSON_TABLE PASSING: ExprState is NULL");
            let expr = exprstate
                .expr
                .as_deref()
                .expect("JSON_TABLE PASSING: ExprState has no expr");
            let info = nodeFuncs::expr_type_info::call(expr)?;
            (info.typid, info.typmod)
        };

        // var->value = ExecEvalExpr(state, ps->ps_ExprContext, &var->isnull).
        let (value, isnull) = {
            let exprstate = state.passingvalexprs[i]
                .as_mut()
                .expect("JSON_TABLE PASSING: ExprState is NULL");
            execExpr::exec_eval_expr_switch_context::call(exprstate, econtext, estate)?
        };

        // var->name = pstrdup(name->sval).
        let name = passing_names[i].as_bytes().to_vec();

        args.push(jsonpath_exec::JsonTableVariable {
            name,
            typid,
            typmod,
            value: tuple_datum_to_word(&value),
            isnull,
        });
    }

    Ok(args)
}

/// Convert a `types_tuple::Datum` (the `ExecEvalExpr` result) into the bare-word
/// `types_datum::Datum` the jsonpath `JsonPathVariable` carries.
///
/// A by-value scalar maps directly to its machine word. A by-reference PASSING
/// value (text / jsonb / numeric …) cannot be carried by the bare word — that
/// is the by-reference-`Datum` substrate gap also noted on the jsonpath_exec
/// `json_item_from_datum` seam (which a by-ref PASSING var would dispatch into),
/// so a real by-ref PASSING argument panics loudly until that lane lands.
/// JSON_TABLE without PASSING (the common case) never reaches this arm.
fn tuple_datum_to_word(d: &Datum<'_>) -> types_datum::Datum {
    match d {
        Datum::ByVal(w) => types_datum::Datum::from_usize(*w),
        _ => panic!(
            "JSON_TABLE PASSING: by-reference argument value — the by-reference-Datum \
             substrate for varlena PASSING args is not yet landed"
        ),
    }
}

/// Recursively convert a `tf->plan` `Node` (a `JsonTablePathScan` /
/// `JsonTableSiblingJoin` tree) into the `jsonpath_exec` crate's
/// `JsonTablePlan` vocabulary, extracting the on-disk jsonpath bytes from each
/// path `Const`.
fn build_json_table_plan(
    node: &types_nodes::nodes::Node<'_>,
) -> PgResult<jsonpath_exec::JsonTablePlan> {
    if let Some(scan) = node.as_jsontablepathscan() {
        // planstate->path = DatumGetJsonPathP(scan->path->value->constvalue):
        // the path is a `Const` of type `jsonpath` whose by-reference value is
        // the full on-disk jsonpath varlena (header + body).
        let path_const = scan
            .path
            .as_const()
            .expect("JsonTablePathScan: path is not a Const node");
        let path = path_const.constvalue.as_ref_bytes().to_vec();

        let child = match scan.child.as_deref() {
            Some(c) => Some(alloc::boxed::Box::new(build_json_table_plan(c)?)),
            None => None,
        };

        Ok(jsonpath_exec::JsonTablePlan::PathScan(
            backend_utils_adt_jsonpath_exec_seams::JsonTablePathScan {
                path,
                error_on_error: scan.errorOnError,
                col_min: scan.colMin,
                col_max: scan.colMax,
                child,
            },
        ))
    } else if let Some(join) = node.as_jsontablesiblingjoin() {
        let lplan = alloc::boxed::Box::new(build_json_table_plan(&join.lplan)?);
        let rplan = alloc::boxed::Box::new(build_json_table_plan(&join.rplan)?);
        Ok(jsonpath_exec::JsonTablePlan::SiblingJoin(
            backend_utils_adt_jsonpath_exec_seams::JsonTableSiblingJoin { lplan, rplan },
        ))
    } else {
        Err(PgError::error(alloc::format!(
            "invalid JsonTablePlan node tag {:?}",
            node.tag()
        )))
    }
}

/// C: `JsonTableGetValue` (jsonpath_exec.c:4452). Locate the current row's
/// source value (via `jsonpath_exec::JsonTableCurrentRow`), then:
///   * if the row pattern is NULL → `(NULL, true)`;
///   * if the column has a `JsonExpr` (`colvalexprs[colnum]`) → set
///     `econtext->caseValue_datum` to the row pattern and `ExecEvalExpr`;
///   * else (ORDINAL column) → `Int32GetDatum(planstate->ordinal)`.
fn json_table_get_value<'mcx>(
    state: &mut TableFuncScanState<'mcx>,
    colnum: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // Row source from the owning plan-state (jsonpath_exec owns the plan-walk).
    let row = {
        let cxt = json_exec_context(&mut state.opaque);
        jsonpath_exec::JsonTableCurrentRow(cxt, colnum)?
    };

    // Row pattern value is NULL.
    if row.isnull {
        return Ok((Datum::null(), true));
    }

    let econtext = node_econtext(state);

    // Evaluate JsonExpr if the column has one; otherwise it is an ORDINAL column.
    match state.colvalexprs[colnum as usize].as_mut() {
        Some(colvalexpr) => {
            // Pass the row pattern value via CaseTestExpr, saving/restoring the
            // econtext's caseValue around the evaluation.
            let row_value = Datum::ByRef(mcx::slice_in(estate.es_query_cxt, &row.value)?);

            let (saved_datum, saved_isnull) = {
                let ec = estate.ecxt_mut(econtext);
                let prev = (ec.caseValue_datum.clone(), ec.caseValue_isNull);
                ec.caseValue_datum = row_value;
                ec.caseValue_isNull = false;
                prev
            };

            let result =
                execExpr::exec_eval_expr_switch_context::call(colvalexpr, econtext, estate);

            // Restore caseValue even on error (mirrors C's unconditional restore).
            let ec = estate.ecxt_mut(econtext);
            ec.caseValue_datum = saved_datum;
            ec.caseValue_isNull = saved_isnull;

            result
        }
        // ORDINAL column: result = Int32GetDatum(planstate->ordinal).
        None => Ok((Datum::from_i32(row.ordinal), false)),
    }
}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `TableFuncNext(node)` — the workhorse for `ExecTableFuncScan`.
///
/// Reads all tuples from the table-producer function into a tuplestore on the
/// first call, then fetches tuples one at a time from the tuplestore. Returns
/// `Ok(true)` when a tuple was fetched into `node.ss.ss_ScanTupleSlot`,
/// `Ok(false)` when the tuplestore is exhausted (the C function returns the
/// scan slot, whose emptiness the boolean reports).
fn TableFuncNext<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If first time through, read all tuples from the function and put them in
    // a tuplestore. Subsequent calls just fetch tuples from the tuplestore.
    if node.tupstore.is_none() {
        tfuncFetchRows(node, estate)?;
    }

    // Get the next tuple from the tuplestore.
    //   (void) tuplestore_gettupleslot(node->tupstore, true, false, scanslot);
    let scanslot = node
        .ss
        .ss_ScanTupleSlot
        .expect("TableFuncNext: ss_ScanTupleSlot not initialized");
    let ts = node
        .tupstore
        .as_deref_mut()
        .expect("TableFuncNext: tupstore set above");
    tuplestore::tuplestore_gettupleslot::call(ts, true, false, scanslot, estate)
}

/// `TableFuncRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual. Nothing to check for a table-func scan, so it always succeeds.
fn TableFuncRecheck<'mcx>(
    _node: &mut TableFuncScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

/// `ExecTableFuncScan(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Scans the function sequentially and returns the next qualifying tuple. Calls
/// the generic [`execScan::exec_scan`] driver, passing it the table-func-scan
/// access-method functions.
pub fn ExecTableFuncScan<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    execScan::exec_scan::call(node, estate, TableFuncNext, TableFuncRecheck)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitTableFuncScan`]:
/// `castNode(TableFuncScanState, pstate)` then run [`ExecTableFuncScan`].
fn exec_table_func_scan_node<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        types_nodes::PlanStateNode::TableFuncScan(node) => node,
        other => panic!("castNode(TableFuncScanState, pstate) failed: {other:?}"),
    };
    ExecTableFuncScan(node, estate)
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecInitTableFuncScan(node, estate, eflags)` — create and initialize a
/// table-func-scan node.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. The plan back-link aliases the shared, read-only plan tree.
pub fn ExecInitTableFuncScan<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, TableFuncScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // TableFuncScan *node — the enclosing plan-tree node (the C `TableFuncScan
    // *` is the same pointer, via struct embedding). Panics if it is not a
    // `TableFuncScan` (the C `castNode`).
    let tfscan: &'mcx TableFuncScan<'mcx> = node.expect_tablefuncscan();
    let tf = &tfscan.tablefunc;

    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // TableFuncscan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(tfscan.scan.plan.lefttree.is_none());
    debug_assert!(tfscan.scan.plan.righttree.is_none());

    // create new ScanState for node
    //   scanstate = makeNode(TableFuncScanState);
    //   scanstate->ss.ps.plan = (Plan *) node;
    //   scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecTableFuncScan;
    //
    // The plan back-link aliases the caller's (read-only at execution time)
    // plan node; the EState back-link is the threaded `estate` parameter.
    let mut scanstate = alloc_in(mcx, TableFuncScanState::new_in(mcx))?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_table_func_scan_node);

    // Miscellaneous initialization: create the expression context for the node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // initialize source tuple type
    //   tupdesc = BuildDescFromLists(tf->colnames, tf->coltypes,
    //                                tf->coltypmods, tf->colcollations);
    let colnames = list_or_empty(&tf.colnames);
    let coltypes = list_or_empty(&tf.coltypes);
    let coltypmods = list_or_empty(&tf.coltypmods);
    let colcollations = list_or_empty(&tf.colcollations);
    let tupdesc =
        tupdesc::build_desc_from_lists::call(mcx, colnames, coltypes, coltypmods, colcollations)?;

    // Capture the column count and per-column type info before the descriptor
    // moves into the scan slot (C reads them back off the shared pointer).
    let (natts, in_types): (i32, PgVec<'mcx, types_core::primitive::Oid>) = match tupdesc.as_deref()
    {
        Some(td) => {
            let mut types = vec_with_capacity_in(mcx, td.natts as usize)?;
            for i in 0..td.natts as usize {
                types.push(td.attr(i).atttypid);
            }
            (td.natts, types)
        }
        None => (0, PgVec::new_in(mcx)),
    };

    // and the corresponding scan slot
    //   ExecInitScanTupleSlot(estate, &scanstate->ss, tupdesc,
    //                         &TTSOpsMinimalTuple);
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut scanstate.ss,
        tupdesc,
        types_nodes::TupleSlotKind::MinimalTuple,
    )?;

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    //   ExecAssignScanProjectionInfo(&scanstate->ss);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    execScan::exec_assign_scan_projection_info::call(&mut scanstate.ss, estate)?;

    // initialize child expressions
    //   scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate);
    let qual = tfscan.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;

    // Only XMLTABLE and JSON_TABLE are supported currently
    //   scanstate->routine = tf->functype == TFT_XMLTABLE ? &XmlTableRoutine
    //                                                      : &JsonbTableRoutine;
    scanstate.routine = Some(TableFuncRoutineKind::from_functype(tf.functype));

    // scanstate->perTableCxt =
    //     AllocSetContextCreate(CurrentMemoryContext,
    //                           "TableFunc per value context",
    //                           ALLOCSET_DEFAULT_SIZES);
    // scanstate->opaque = NULL;  /* initialized at runtime */
    scanstate.perTableCxt = Some(mcx.context().new_child("TableFunc per value context"));
    // opaque is already None (the C NULL) from new_in.

    // scanstate->ns_names = tf->ns_names;
    scanstate.ns_names = clone_ns_names(&tf.ns_names, mcx)?;

    // scanstate->ns_uris = ExecInitExprList(tf->ns_uris, scanstate);
    scanstate.ns_uris =
        init_expr_list_required(&tf.ns_uris, &mut scanstate.ss.ps, estate)?;
    // scanstate->docexpr = ExecInitExpr((Expr *) tf->docexpr, scanstate);
    scanstate.docexpr = init_opt_expr(&tf.docexpr, &mut scanstate.ss.ps, estate)?;
    // scanstate->rowexpr = ExecInitExpr((Expr *) tf->rowexpr, scanstate);
    scanstate.rowexpr = init_opt_expr(&tf.rowexpr, &mut scanstate.ss.ps, estate)?;
    // scanstate->colexprs = ExecInitExprList(tf->colexprs, scanstate);
    scanstate.colexprs =
        init_opt_expr_list(&tf.colexprs, &mut scanstate.ss.ps, estate)?;
    // scanstate->coldefexprs = ExecInitExprList(tf->coldefexprs, scanstate);
    scanstate.coldefexprs =
        init_opt_expr_list(&tf.coldefexprs, &mut scanstate.ss.ps, estate)?;
    // scanstate->colvalexprs = ExecInitExprList(tf->colvalexprs, scanstate);
    scanstate.colvalexprs =
        init_opt_expr_list(&tf.colvalexprs, &mut scanstate.ss.ps, estate)?;
    // scanstate->passingvalexprs = ExecInitExprList(tf->passingvalexprs, ...);
    scanstate.passingvalexprs =
        init_expr_list_required_opt(&tf.passingvalexprs, &mut scanstate.ss.ps, estate)?;

    // scanstate->notnulls = tf->notnulls;
    scanstate.notnulls = match &tf.notnulls {
        Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
        None => None,
    };

    // these are allocated now and initialized later:
    //   scanstate->in_functions = palloc(sizeof(FmgrInfo) * tupdesc->natts);
    //   scanstate->typioparams = palloc(sizeof(Oid) * tupdesc->natts);
    let mut in_functions = vec_with_capacity_in(mcx, natts as usize)?;
    let mut typioparams = vec_with_capacity_in(mcx, natts as usize)?;

    // Fill in the necessary fmgr infos.
    //   for (i = 0; i < tupdesc->natts; i++) {
    //       getTypeInputInfo(TupleDescAttr(tupdesc, i)->atttypid,
    //                        &in_funcid, &scanstate->typioparams[i]);
    //       fmgr_info(in_funcid, &scanstate->in_functions[i]);
    //   }
    for i in 0..natts as usize {
        let atttypid = in_types[i];
        let (in_funcid, typioparam) = lsyscache::get_type_input_info::call(atttypid)?;
        typioparams.push(typioparam);
        // fmgr_info(in_funcid, &finfo): eager lookup + resolved handle. The
        // owned FmgrInfo carries the OID; fmgr_info_check preserves the eager
        // lookup-failure surface.
        fmgr::fmgr_info_check::call(in_funcid)?;
        in_functions.push(FmgrInfo { fn_oid: in_funcid, ..Default::default() });
    }
    scanstate.in_functions = in_functions;
    scanstate.typioparams = typioparams;

    Ok(scanstate)
}

/// `ExecEndTableFuncScan(node)` — frees any storage allocated through C
/// routines, releasing the tuplestore.
pub fn ExecEndTableFuncScan<'mcx>(node: &mut TableFuncScanState<'mcx>) -> PgResult<()> {
    // Release tuplestore resources
    //   if (node->tupstore != NULL) tuplestore_end(node->tupstore);
    //   node->tupstore = NULL;
    if let Some(tupstore) = node.tupstore.take() {
        tuplestore::tuplestore_end::call(tupstore);
    }
    Ok(())
}

/// `ExecReScanTableFuncScan(node)` — rescans the relation.
pub fn ExecReScanTableFuncScan<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   Bitmapset *chgparam = node->ss.ps.chgParam;
    let chgparam = node.ss.ps.chgParam.is_some();

    //   if (node->ss.ps.ps_ResultTupleSlot)
    //       ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if let Some(slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, slot)?;
    }

    //   ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan::call(node, estate)?;

    // Recompute when parameters are changed.
    //   if (chgparam) { if (node->tupstore != NULL) { tuplestore_end(...);
    //       node->tupstore = NULL; } }
    if chgparam {
        if let Some(tupstore) = node.tupstore.take() {
            tuplestore::tuplestore_end::call(tupstore);
        }
    }

    //   if (node->tupstore != NULL) tuplestore_rescan(node->tupstore);
    if let Some(tupstore) = node.tupstore.as_deref_mut() {
        tuplestore::tuplestore_rescan::call(tupstore)?;
    }

    Ok(())
}

// ===========================================================================
// File-scope statics: tfuncFetchRows / tfuncInitialize / tfuncLoadRows
// ===========================================================================

/// `tfuncFetchRows(tstate, econtext)` — read rows from a `TableFunc` producer
/// into the tuplestore.
///
/// The C `MemoryContextSwitchTo` dance is translated to explicit `Mcx`
/// threading: the tuplestore is created in the per-query context
/// (`econtext->ecxt_per_query_memory`); the builder methods and row loader
/// allocate in `perTableCxt` (the node carries it). The C `PG_TRY` / `PG_CATCH`
/// block that calls `DestroyOpaque` and re-throws on error is reproduced: the
/// fetch body runs first; on error, if a builder context was created, it is
/// destroyed before the error is re-propagated.
fn tfuncFetchRows<'mcx>(
    tstate: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let kind = tstate
        .routine
        .expect("tfuncFetchRows: routine not initialized");

    //   Assert(tstate->opaque == NULL);
    debug_assert!(tstate.opaque.0.is_none());

    // build tuplestore for the result, in the per-query memory context
    //   oldcxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
    //   tstate->tupstore = tuplestore_begin_heap(false, false, work_mem);
    let econtext = node_econtext(tstate);
    let per_query = estate.ecxt(econtext).ecxt_per_query_memory;
    let work_mem = globals::work_mem::call();
    tstate.tupstore = Some(tuplestore::tuplestore_begin_heap::call(
        per_query, false, false, work_mem,
    )?);

    // perTableCxt now serves the same function as "argcontext" in FunctionScan:
    // a place to store per-one-call lifetime data. The builder allocates there;
    // the owned model threads that context to the builder seams via `tstate`.

    // PG_TRY() { ... } PG_CATCH() { if opaque: DestroyOpaque; RE_THROW; }
    let result = tfunc_fetch_body(tstate, kind, econtext, estate);

    if let Err(e) = result {
        //   if (tstate->opaque != NULL) routine->DestroyOpaque(tstate);
        //   PG_RE_THROW();
        if tstate.opaque.0.is_some() {
            // DestroyOpaque is the error-path cleanup; propagate the original
            // error even if it itself errors (PG_RE_THROW semantics).
            let _ = routine::routine_destroy_opaque::call(tstate, kind);
        }
        return Err(e);
    }

    // clean up and return to the original memory context
    //   if (tstate->opaque != NULL) { routine->DestroyOpaque(tstate);
    //       tstate->opaque = NULL; }
    if tstate.opaque.0.is_some() {
        routine::routine_destroy_opaque::call(tstate, kind)?;
    }

    //   MemoryContextSwitchTo(oldcxt);  -- no ambient context in the owned model
    //   MemoryContextReset(tstate->perTableCxt);
    if let Some(ctx) = tstate.perTableCxt.as_mut() {
        ctx.reset();
    }

    Ok(())
}

/// The `PG_TRY` body of `tfuncFetchRows`, factored so the `PG_CATCH`
/// `DestroyOpaque` cleanup can run on any early return.
fn tfunc_fetch_body<'mcx>(
    tstate: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   routine->InitOpaque(tstate,
    //       tstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts);
    let natts = scan_slot_natts(tstate, estate)?;
    routine::routine_init_opaque::call(tstate, kind, natts, estate)?;

    // If evaluating the document expression returns NULL, the table expression
    // is empty and we return immediately.
    //   value = ExecEvalExpr(tstate->docexpr, econtext, &isnull);
    let docexpr = tstate
        .docexpr
        .as_deref_mut()
        .expect("tfuncFetchRows: docexpr not initialized");
    let (value, isnull) =
        execExpr::exec_eval_expr_switch_context::call(docexpr, econtext, estate)?;

    if !isnull {
        // otherwise, pass the document value to the table builder
        tfuncInitialize(tstate, kind, value, econtext, estate)?;

        // initialize ordinality counter
        tstate.ordinal = 1;

        // Load all rows into the tuplestore, and we're done
        tfuncLoadRows(tstate, kind, econtext, estate)?;
    }
    Ok(())
}

/// `tfuncInitialize(tstate, econtext, doc)` — fill in namespace declarations,
/// the row filter, and the column filters in the table-expression-builder
/// context.
fn tfuncInitialize<'mcx>(
    tstate: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    doc: Datum<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    //   int ordinalitycol =
    //       ((TableFuncScan *) (tstate->ss.ps.plan))->tablefunc->ordinalitycol;
    let ordinalitycol = plan_ordinalitycol(tstate);

    // Install the document as a possibly-toasted Datum into the tablefunc
    // context.
    //   routine->SetDocument(tstate, doc);
    routine::routine_set_document::call(tstate, kind, doc)?;

    // Evaluate namespace specifications.
    //   forboth(lc1, tstate->ns_uris, lc2, tstate->ns_names) { ... }
    //
    // `ns_uris` (ExprState list) and `ns_names` (String-or-NULL value nodes)
    // are walked in lockstep, exactly as `forboth` zips two lists until either
    // ends.
    let pairs = core::cmp::min(tstate.ns_uris.len(), tstate.ns_names.len());
    for i in 0..pairs {
        //   value = ExecEvalExpr(expr, econtext, &isnull);
        let (value, isnull) =
            execExpr::exec_eval_expr_switch_context::call(&mut tstate.ns_uris[i], econtext, estate)?;
        if isnull {
            return Err(null_value_error("namespace URI must not be null", None));
        }
        //   ns_uri = TextDatumGetCString(value);
        let ns_uri = varlena::text_to_cstring_v::call(mcx, &value)?;

        // DEFAULT is passed down to SetNamespace as NULL.
        //   ns_name = ns_node ? strVal(ns_node) : NULL;
        let ns_name_owned: Option<alloc::string::String> = tstate.ns_names[i]
            .as_ref()
            .map(|s| alloc::string::String::from(s.as_str()));

        //   routine->SetNamespace(tstate, ns_name, ns_uri);
        routine::routine_set_namespace::call(
            tstate,
            kind,
            ns_name_owned.as_deref(),
            ns_uri.as_str(),
        )?;
    }

    // Install the row filter expression, if any, into the table builder context.
    //   if (routine->SetRowFilter) { ... }
    if routine::routine_has_set_row_filter::call(kind) {
        let rowexpr = tstate
            .rowexpr
            .as_deref_mut()
            .expect("tfuncInitialize: rowexpr not initialized");
        //   value = ExecEvalExpr(tstate->rowexpr, econtext, &isnull);
        let (value, isnull) =
            execExpr::exec_eval_expr_switch_context::call(rowexpr, econtext, estate)?;
        if isnull {
            return Err(null_value_error("row filter expression must not be null", None));
        }
        //   routine->SetRowFilter(tstate, TextDatumGetCString(value));
        let path = varlena::text_to_cstring_v::call(mcx, &value)?;
        routine::routine_set_row_filter::call(tstate, kind, path.as_str())?;
    }

    // Install the column filter expressions into the table builder context. If
    // an expression is given, use that; otherwise the column name itself is the
    // column filter.
    //   colno = 0; foreach(lc1, tstate->colexprs) { ... colno++; }
    let ncols = tstate.colexprs.len();
    for colno in 0..ncols as i32 {
        //   Form_pg_attribute att = TupleDescAttr(tupdesc, colno);
        if colno != ordinalitycol {
            //   ExprState *colexpr = lfirst(lc1);
            //   if (colexpr != NULL) { value = ExecEvalExpr(...); ... }
            //   else colfilter = NameStr(att->attname);
            let colfilter_owned;
            //   ExprState *colexpr = lfirst(lc1);
            //   if (colexpr != NULL) { value = ExecEvalExpr(...); ... }
            //   else colfilter = NameStr(att->attname);
            //
            // The eval borrows only the single `colexprs[colno]` cell (per-eval
            // scratch) plus `estate`; it does not touch the rest of `tstate`.
            // Split-borrow that one cell, run the eval, and let the borrow end
            // (the result `(Datum, bool)` is owned) before any
            // `scan_slot_attname(tstate, ...)` re-borrow of `tstate`.
            let eval_result = match tstate.colexprs[colno as usize].as_mut() {
                Some(colexpr) => Some(execExpr::exec_eval_expr_switch_context::call(
                    colexpr, econtext, estate,
                )?),
                None => None,
            };
            let colfilter: &str = match eval_result {
                Some((value, isnull)) => {
                    if isnull {
                        let attname = scan_slot_attname(tstate, colno, estate)?;
                        return Err(null_value_error(
                            "column filter expression must not be null",
                            Some(alloc::format!("Filter for column \"{attname}\" is null.")),
                        ));
                    }
                    colfilter_owned = varlena::text_to_cstring_v::call(mcx, &value)?;
                    colfilter_owned.as_str()
                }
                None => {
                    colfilter_owned = mcx::PgString::from_str_in(
                        &scan_slot_attname(tstate, colno, estate)?,
                        mcx,
                    )?;
                    colfilter_owned.as_str()
                }
            };

            //   routine->SetColumnFilter(tstate, colfilter, colno);
            routine::routine_set_column_filter::call(tstate, kind, colfilter, colno)?;
        }
    }

    Ok(())
}

/// `tfuncLoadRows(tstate, econtext)` — load all the rows from the `TableFunc`
/// table builder into the tuplestore.
fn tfuncLoadRows<'mcx>(
    tstate: &mut TableFuncScanState<'mcx>,
    kind: TableFuncRoutineKind,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // TupleDesc tupdesc = slot->tts_tupleDescriptor; int natts = tupdesc->natts;
    let tupdesc_owned = scan_slot_descriptor(tstate, estate)?;
    let tupdesc = tupdesc_owned
        .as_deref()
        .expect("tfuncLoadRows: scan slot has no descriptor");
    let natts = tupdesc.natts;

    //   ordinalitycol =
    //       ((TableFuncScan *) (tstate->ss.ps.plan))->tablefunc->ordinalitycol;
    let ordinalitycol = plan_ordinalitycol(tstate);

    // We need a short-lived memory context that we can clean up each time around
    // the loop. Our default per-tuple context is fine for the job.
    //   oldcxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    //
    // No ambient context in the owned model — the per-tuple context is reset
    // explicitly at the bottom of the loop.

    // Scratch value/null arrays standing in for the scan slot's tts_values /
    // tts_isnull (the slot payload model is not yet landed; C uses the slot's
    // own arrays here).
    // The value array crosses into the `tuplestore_putvalues` edge, which now
    // carries the canonical unified `Datum<'mcx>` (the Datum-completion Wave 7
    // flip of the sort-storage seam). `routine_get_value` / `ExecEvalExpr` both
    // already return canonical `Datum<'mcx>`, so column values flow straight
    // through with no down-conversion to a bare scalar word.
    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, natts as usize)?;
    let mut nulls: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, natts as usize)?;
    for _ in 0..natts as usize {
        values.push(Datum::from_i32(0));
        nulls.push(false);
    }

    // The number of coldefexprs cells, used to mirror the C `cell != NULL`
    // advance through `tstate->coldefexprs` in lockstep with the column loop.
    let ncoldefs = tstate.coldefexprs.len();

    // Keep requesting rows from the table builder until there aren't any.
    //   while (routine->FetchRow(tstate)) { ... }
    while routine::routine_fetch_row::call(tstate, kind)? {
        //   ListCell *cell = list_head(tstate->coldefexprs);
        let mut cell: usize = 0;

        tcop_postgres::check_for_interrupts::call()?;

        //   ExecClearTuple(tstate->ss.ss_ScanTupleSlot);
        let scanslot = tstate
            .ss
            .ss_ScanTupleSlot
            .expect("tfuncLoadRows: ss_ScanTupleSlot not initialized");
        execTuples::exec_clear_tuple::call(estate, scanslot)?;

        // Obtain the value of each column for this row, installing them into
        // the slot; then add the tuple to the tuplestore.
        for colno in 0..natts as usize {
            //   Form_pg_attribute att = TupleDescAttr(tupdesc, colno);
            let att = tupdesc.attr(colno);

            if colno as i32 == ordinalitycol {
                // Fast path for ordinality column.
                //   values[colno] = Int32GetDatum(tstate->ordinal++);
                //   nulls[colno] = false;
                let ord = tstate.ordinal;
                tstate.ordinal += 1;
                values[colno] = Datum::from_i32(ord as i32);
                nulls[colno] = false;
            } else {
                //   values[colno] = routine->GetValue(tstate, colno,
                //       att->atttypid, att->atttypmod, &isnull);
                let (mut v, mut isnull) = routine::routine_get_value::call(
                    tstate,
                    kind,
                    colno as i32,
                    att.atttypid,
                    att.atttypmod,
                    estate,
                )?;

                // No value? Evaluate and apply the default, if any.
                //   if (isnull && cell != NULL) {
                //       ExprState *coldefexpr = (ExprState *) lfirst(cell);
                //       if (coldefexpr != NULL)
                //           values[colno] = ExecEvalExpr(coldefexpr, econtext,
                //                                        &isnull);
                //   }
                if isnull && cell < ncoldefs {
                    if let Some(coldefexpr) = tstate.coldefexprs[cell].as_mut() {
                        let (dv, dnull) = execExpr::exec_eval_expr_switch_context::call(
                            coldefexpr, econtext, estate,
                        )?;
                        v = dv;
                        isnull = dnull;
                    }
                }

                // Verify a possible NOT NULL constraint.
                //   if (isnull && bms_is_member(colno, tstate->notnulls))
                //       ereport(ERROR, ...);
                if isnull && nodes_core::bms_is_member::call(colno as i32, tstate.notnulls.as_deref())
                {
                    let attname = name_str(&att.attname);
                    return Err(null_value_error(
                        &alloc::format!("null is not allowed in column \"{attname}\""),
                        None,
                    ));
                }

                //   nulls[colno] = isnull;
                // The column value is already the canonical `Datum<'mcx>`; store
                // it directly into the tts_values array.
                values[colno] = v;
                nulls[colno] = isnull;
            }

            // advance list of default expressions
            //   if (cell != NULL) cell = lnext(tstate->coldefexprs, cell);
            if cell < ncoldefs {
                cell += 1;
            }
        }

        //   tuplestore_putvalues(tstate->tupstore, tupdesc, values, nulls);
        let tupstore = tstate
            .tupstore
            .as_deref_mut()
            .expect("tfuncLoadRows: tupstore not initialized");
        tuplestore::tuplestore_putvalues::call(tupstore, tupdesc, &values, &nulls)?;

        //   MemoryContextReset(econtext->ecxt_per_tuple_memory);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }

    //   MemoryContextSwitchTo(oldcxt);  -- no ambient context
    Ok(())
}

// ===========================================================================
// Small in-crate helpers
// ===========================================================================

/// The node's expression context (`node->ss.ps.ps_ExprContext`).
#[inline]
fn node_econtext(tstate: &TableFuncScanState<'_>) -> EcxtId {
    tstate
        .ss
        .ps
        .ps_ExprContext
        .expect("TableFuncScan: ps_ExprContext not initialized")
}

/// `((TableFuncScan *) (tstate->ss.ps.plan))->tablefunc->ordinalitycol` — read
/// the ordinality column index from the node's plan.
#[inline]
fn plan_ordinalitycol(tstate: &TableFuncScanState<'_>) -> i32 {
    match tstate.ss.ps.plan {
        Some(p) => p.expect_tablefuncscan().tablefunc.ordinalitycol,
        _ => panic!("TableFuncScan: plan is not a TableFuncScan node"),
    }
}

/// `tstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts`.
fn scan_slot_natts<'mcx>(
    tstate: &TableFuncScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<i32> {
    let td = scan_slot_descriptor(tstate, estate)?;
    Ok(td.as_deref().map_or(0, |d| d.natts))
}

/// `NameStr(TupleDescAttr(tupdesc, colno)->attname)` — the scan slot's column
/// name at `colno`.
fn scan_slot_attname<'mcx>(
    tstate: &TableFuncScanState<'mcx>,
    colno: i32,
    estate: &EStateData<'mcx>,
) -> PgResult<alloc::string::String> {
    let td = scan_slot_descriptor(tstate, estate)?;
    let td = td
        .as_deref()
        .expect("scan_slot_attname: scan slot has no descriptor");
    Ok(name_str(&td.attr(colno as usize).attname))
}

/// `tstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor` (cloned into the per-query
/// context — the slot payload model is not yet landed).
fn scan_slot_descriptor<'mcx>(
    tstate: &TableFuncScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    execTuples::exec_scan_slot_descriptor::call(estate.es_query_cxt, &tstate.ss, estate)
}

/// `NameStr(name)` — the name's bytes up to the first NUL, as a `String`.
fn name_str(name: &types_tuple::heaptuple::NameData) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(name.name_str()).into_owned()
}

/// `errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)` + `errmsg(...)` (+ optional
/// `errdetail`).
fn null_value_error(msg: &str, detail: Option<alloc::string::String>) -> PgError {
    let mut e = PgError::error(msg).with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED);
    if let Some(d) = detail {
        e.detail = Some(d);
    }
    e
}

/// `list_or_empty(list)` — a `&[T]` view of an `Option<PgVec>` list (empty
/// slice for the C `NIL`).
fn list_or_empty<'a, T>(list: &'a Option<PgVec<'_, T>>) -> &'a [T] {
    match list {
        Some(v) => v.as_slice(),
        None => &[],
    }
}

/// `scanstate->ns_names = tf->ns_names` — copy the namespace-name list.
fn clone_ns_names<'mcx>(
    list: &Option<PgVec<'_, Option<mcx::PgString<'_>>>>,
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, Option<mcx::PgString<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    if let Some(v) = list {
        out = vec_with_capacity_in(mcx, v.len())?;
        for n in v.iter() {
            out.push(match n {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            });
        }
    }
    Ok(out)
}

/// `ExecInitExpr((Expr *) node, parent)` for an optional single expression.
fn init_opt_expr<'mcx>(
    node: &Option<PgBox<'_, types_nodes::primnodes::Expr>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    match node {
        Some(e) => Ok(Some(execExpr::exec_init_expr::call(e, parent, estate)?)),
        None => Ok(None),
    }
}

/// `ExecInitExprList(nodes, parent)` over an `Option<PgVec<Option<Expr>>>`
/// list (NULL cells preserved).
fn init_opt_expr_list<'mcx>(
    list: &Option<PgVec<'_, Option<PgBox<'_, types_nodes::primnodes::Expr>>>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, Option<types_nodes::execexpr::ExprState<'mcx>>>> {
    let refs: alloc::vec::Vec<Option<&types_nodes::primnodes::Expr>> = match list {
        Some(v) => v.iter().map(|o| o.as_deref()).collect(),
        None => alloc::vec::Vec::new(),
    };
    execExpr::exec_init_expr_list::call(&refs, parent, estate)
}

/// `ExecInitExprList(nodes, parent)` over an `Option<PgVec<Expr>>` list with no
/// NULL cells, returning a list of `ExprState` (the `ns_uris` shape).
fn init_expr_list_required<'mcx>(
    list: &Option<PgVec<'_, PgBox<'_, types_nodes::primnodes::Expr>>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, types_nodes::execexpr::ExprState<'mcx>>> {
    let refs: alloc::vec::Vec<Option<&types_nodes::primnodes::Expr>> = match list {
        Some(v) => v.iter().map(|e| Some(&**e)).collect(),
        None => alloc::vec::Vec::new(),
    };
    let states = execExpr::exec_init_expr_list::call(&refs, parent, estate)?;
    // ns_uris never holds NULL Expr cells, so no None ExprState is produced.
    let mut out = vec_with_capacity_in(estate.es_query_cxt, states.len())?;
    for s in states.into_iter() {
        out.push(s.expect("ns_uris: ExecInitExpr produced NULL for a non-NULL Expr"));
    }
    Ok(out)
}

/// `ExecInitExprList(tf->passingvalexprs, parent)` — the PASSING list, kept as
/// `Option<ExprState<'mcx>>` cells.
fn init_expr_list_required_opt<'mcx>(
    list: &Option<PgVec<'_, PgBox<'_, types_nodes::primnodes::Expr>>>,
    parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, Option<types_nodes::execexpr::ExprState<'mcx>>>> {
    let refs: alloc::vec::Vec<Option<&types_nodes::primnodes::Expr>> = match list {
        Some(v) => v.iter().map(|e| Some(&**e)).collect(),
        None => alloc::vec::Vec::new(),
    };
    execExpr::exec_init_expr_list::call(&refs, parent, estate)
}
