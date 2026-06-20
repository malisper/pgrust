//! Scalar-opcode evaluators (`execExprInterp.c`): function calls with usage
//! tracking, PARAM nodes, I/O coercion, SQLValueFunction, CurrentOf,
//! NextValue, system Vars, constraint checks, and the (hashed) ScalarArrayOp
//! machinery.
//!
//! Step evaluators address their instruction by index `op` into `state.steps`
//! and write the result through that step's `ResultCellId`; they return
//! `PgResult<()>` (evaluation can `ereport`). See [`crate::dispatch`] for the
//! shared owned-model conventions.
//!
//! Result-cell access. In C every `ExprEvalStep` carries raw `Datum *resvalue`
//! / `bool *resnull` pointers; the owned model replaces them with
//! [`ResultCellId`] indices into the owning [`ExprState`]'s
//! [`ResultCellArena`] (`state.result_cells`). A handler reads `*op->resvalue`
//! / `*op->resnull` via `state.result_cells.get(step.resvalue)` and writes them
//! back with `state.result_cells.set(step.resvalue, ...)` (the value/is-null
//! pair always shares one cell, exactly as the C pointer pair always aliases
//! one `Datum`/`bool`).
//!
//! The fcinfo call frame. Several opcodes here (`FUNCEXPR_*_FUSAGE`,
//! `IOCOERCE_SAFE`, `SCALARARRAYOP`, `HASHED_SCALARARRAYOP`) load the
//! sub-expression results the compiler gathered into `fcinfo->args[i]` and then
//! invoke `op->d.*.fn_addr(fcinfo)`. The shared `FunctionCallInfoBaseData`
//! (types-nodes `crate::fmgr`) is still trimmed to its `resultinfo` field — the
//! `args[]` / `isnull` / `flinfo` members the fmgr port widens it with are not
//! present yet, and there is no `FunctionCallInvoke` seam to dispatch through a
//! call frame either. So, exactly as the sibling `ExecJust*` fast paths in
//! [`crate::justs`] do, the arg-cell gather + `fn_addr(fcinfo)` dispatch is
//! modeled down to the genuine blocker and then panics loudly, naming the
//! unported owner (fmgr's widened call frame). All the surrounding
//! step-payload reads and control flow that the owned model can already express
//! are written out faithfully.

use backend_utils_fmgr_fmgr_seams::function_call_invoke_datum;
// The bare-word newtype: the scalar form the fmgr/arrayfuncs seams and the
// step-payload eval helpers operate on.
use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ResultCell.value` / `ExprState.resvalue` carry.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

/// Recover the bare scalar word from a stored canonical by-value datum (the
/// transitional bridge: the fmgr/arrayfuncs/saophash seams take a word).
#[inline]
fn word_of(v: &DatumV<'_>) -> Datum {
    Datum::from_usize(v.as_usize())
}
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NOT_NULL_VIOLATION,
};
use types_nodes::execexpr::{ExprEvalStepData, ExprState};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// Read the `(fn_oid, fncollation)` of an `EEOP_FUNCEXPR*` step's `Func`
/// payload, then gather its per-argument result cells (`arg_cells`) into the
/// call frame's `args[]` (the C recursion writes `fcinfo->args[i]` directly;
/// the owned model gathers them here, immediately before dispatch).
///
/// `func_step_inputs(state, op)` returns `(fn_oid, fncollation, args, nulls,
/// nargs)`.
///
/// The per-argument `args[i]` carry the CANONICAL `Datum<'mcx>` cloned straight
/// from the gathered result cell (the cell already holds the canonical value,
/// e.g. a by-reference `text`/`name` column word) — they are NOT flattened to a
/// bare word, so a by-reference argument survives the call-frame gather (WALL
/// 1aq). The accompanying per-arg `isnull` flags are returned in parallel.
fn func_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    Vec<DatumV<'mcx>>,
    Vec<bool>,
    usize,
) {
    match step_data(state, op) {
        ExprEvalStepData::Func {
            finfo,
            fcinfo_data,
            arg_cells,
            nargs,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.fcinfo_data missing");
            let cells = arg_cells
                .as_ref()
                .expect("EEOP_FUNCEXPR: op->d.func.arg_cells missing");
            // fcinfo->args[i].value  = *cell.value (the canonical Datum — the cell
            //                          already carries the by-ref/by-value image);
            // fcinfo->args[i].isnull =  cell.isnull.
            let mut args: Vec<DatumV<'mcx>> = Vec::with_capacity(cells.len());
            let mut nulls: Vec<bool> = Vec::with_capacity(cells.len());
            for &cell in cells.iter() {
                let c = state.result_cells.get(cell);
                args.push(c.value.clone());
                nulls.push(c.isnull);
            }
            (finfo.fn_oid, fcinfo.fncollation, args, nulls, *nargs as usize)
        }
        other => unreachable!("EEOP_FUNCEXPR step carries the wrong payload: {other:?}"),
    }
}

/// Recover the call-expression node `ExecInitFunc` stamped onto the step's
/// `op->d.func.finfo->fn_expr` (`fmgr_info_set_expr`). The by-OID fmgr dispatch
/// re-resolves the `FmgrInfo` and so drops the step's `fn_expr`; threading it
/// back to the callee lets a polymorphic function read its declared
/// result/argument types (`get_fn_expr_rettype`/`get_fn_expr_argtype`). `None`
/// is C's "`flinfo->fn_expr == NULL`" (no call node — non-polymorphic call).
fn func_step_fn_expr<'a, 'mcx>(
    state: &'a ExprState<'mcx>,
    op: usize,
) -> Option<&'a types_nodes::primnodes::Expr> {
    match step_data(state, op) {
        ExprEvalStepData::Func { finfo, .. } => finfo
            .as_ref()?
            .fn_expr
            .as_ref()?
            .downcast_ref::<types_nodes::primnodes::Expr>(),
        _ => None,
    }
}

/// `ExecInterpExecuteFuncStep` core — the shared body for the `EEOP_FUNCEXPR`
/// (and strict / fusage) opcodes:
///
/// ```c
/// fcinfo->isnull = false;
/// d = op->d.func.fn_addr(fcinfo);
/// *op->resvalue = d;
/// *op->resnull = fcinfo->isnull;
/// ```
///
/// The resolved `FmgrInfo` carries only `fn_oid` (the fmgr-seam contract), so
/// the dispatch goes through `function_call_invoke`, which re-resolves by OID
/// and runs the function under `fcinfo->fncollation` (#296: the collation now
/// survives on the widened call frame). The returned bare result word is wrapped
/// back into the canonical by-value `Datum` (the transitional interp bridge,
/// matching the rest of this layer). `strict` applies C's NULL-arg
/// short-circuit before the call.
pub fn exec_func_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    strict: bool,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    let (fn_oid, collation, args, nulls, _nargs) = func_step_inputs(state, op);
    let (resvalue_id, resnull_id) = res_cells(state, op);
    let _ = resnull_id; // value/is-null share one cell

    // C (the _STRICT opcodes): for (argno = 0; argno < nargs; argno++)
    //                              if (args[argno].isnull) { *op->resnull = true; return; }
    //
    // Write through `write_cell`, which routes `STATE_RESULT_CELL` (== id 0) to
    // the `ExprState`'s own `resvalue`/`resnull` rather than the auxiliary
    // `result_cells[0]`. The raw `result_cells.set` bypassed that, so a step
    // whose result slot is the ExprState's own cell (e.g. a top-level qual's
    // FUNCEXPR) wrote into the wrong location and the EEOP_QUAL reader saw zero.
    if strict && nulls.iter().any(|&n| n) {
        crate::interp_loop::write_cell(state, resvalue_id, DatumV::null(), true);
        return Ok(());
    }

    // fcinfo->isnull = false; d = op->d.func.fn_addr(fcinfo); read back isnull.
    // The canonical (by-reference-capable) call-frame lane: args carry their full
    // Datum image — a by-ref text/name column survives the gather (WALL 1aq). The
    // result is materialized into the per-query context.
    let mcx = estate.es_query_cxt;
    let fn_expr = func_step_fn_expr(state, op);
    let (value, isnull) =
        function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &nulls, fn_expr)?;

    // *op->resvalue = d;  *op->resnull = fcinfo->isnull;
    crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    Ok(())
}

/// Read a `Func`-payload step's `(fn_oid, fncollation, make_ro)` and gather its
/// argument result cells into the `args[]` call frame. Shared by the
/// `EEOP_DISTINCT` / `EEOP_NOT_DISTINCT` / `EEOP_NULLIF` arms (they all carry the
/// C `op->d.func` payload). Mirrors `func_step_inputs` (the canonical
/// by-reference-capable gather — WALL 1aq) but also surfaces the `make_ro` flag
/// NULLIF needs.
fn distinct_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    Vec<DatumV<'mcx>>,
    Vec<bool>,
    bool,
) {
    match step_data(state, op) {
        ExprEvalStepData::Func {
            finfo,
            fcinfo_data,
            arg_cells,
            make_ro,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_DISTINCT/NULLIF: op->d.func.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_DISTINCT/NULLIF: op->d.func.fcinfo_data missing");
            let cells = arg_cells
                .as_ref()
                .expect("EEOP_DISTINCT/NULLIF: op->d.func.arg_cells missing");
            let mut args: Vec<DatumV<'mcx>> = Vec::with_capacity(cells.len());
            let mut nulls: Vec<bool> = Vec::with_capacity(cells.len());
            for &cell in cells.iter() {
                let c = state.result_cells.get(cell);
                args.push(c.value.clone());
                nulls.push(c.isnull);
            }
            (finfo.fn_oid, fcinfo.fncollation, args, nulls, *make_ro)
        }
        other => unreachable!("EEOP_DISTINCT/NULLIF step carries the wrong payload: {other:?}"),
    }
}

/// `EEOP_DISTINCT` / `EEOP_NOT_DISTINCT` core — `IS [NOT] DISTINCT FROM`
/// (execExprInterp.c). The arguments are already gathered into `fcinfo->args`;
/// the NULL handling differs from a strict function:
///
/// ```c
/// if (args[0].isnull && args[1].isnull)        /* both NULL: not distinct  */
/// else if (args[0].isnull || args[1].isnull)   /* one NULL:  distinct      */
/// else { eqresult = fn_addr(fcinfo); ... }     /* neither:  apply equality */
/// ```
///
/// `not_distinct` selects the `NOT DISTINCT` (inverted) variant: when neither arg
/// is NULL it returns the raw equality result instead of inverting it, and the
/// both-NULL/one-NULL constants flip.
pub fn exec_distinct_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    not_distinct: bool,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    let (fn_oid, collation, args, nulls, _make_ro) = distinct_step_inputs(state, op);
    let (resvalue_id, _resnull_id) = res_cells(state, op);

    let a0_null = nulls[0];
    let a1_null = nulls[1];

    // Write through `write_cell`, which routes `STATE_RESULT_CELL` (== id 0) to
    // `state.resvalue`/`state.resnull` (read by EEOP_DONE_RETURN); the raw
    // `result_cells.set` would land in the dead `cells[0]` slot.
    if a0_null && a1_null {
        // Both NULL: DISTINCT -> false, NOT DISTINCT -> true.
        crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_bool(not_distinct), false);
    } else if a0_null || a1_null {
        // Only one NULL: DISTINCT -> true, NOT DISTINCT -> false.
        crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_bool(!not_distinct), false);
    } else {
        // Neither null: apply the equality function. fcinfo->isnull = false;
        // eqresult = op->d.func.fn_addr(fcinfo). The canonical (by-ref-capable)
        // call frame: the compared values keep their full Datum image.
        let mcx = estate.es_query_cxt;
        let fn_expr = func_step_fn_expr(state, op);
        let (eqval, isnull) =
            function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
        // DISTINCT inverts "=" (BoolGetDatum(!DatumGetBool(eqresult))); NOT
        // DISTINCT returns the raw "=" result.
        let value = if not_distinct {
            eqval
        } else {
            DatumV::from_bool(!eqval.as_bool())
        };
        crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    }
    Ok(())
}

/// `EEOP_NULLIF` core (execExprInterp.c): the arguments are already evaluated
/// into `fcinfo->args`; compare them via the equality function and return NULL if
/// equal, else the (original) first argument.
///
/// ```c
/// save_arg0 = fcinfo->args[0].value;
/// if (!args[0].isnull && !args[1].isnull) {
///     if (op->d.func.make_ro)
///         fcinfo->args[0].value = MakeExpandedObjectReadOnlyInternal(save_arg0);
///     fcinfo->isnull = false;
///     result = fn_addr(fcinfo);
///     if (!fcinfo->isnull && DatumGetBool(result)) { *resvalue = 0; *resnull = true; return; }
/// }
/// *resvalue = save_arg0; *resnull = args[0].isnull;
/// ```
pub fn exec_nullif_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    let (fn_oid, collation, mut args, nulls, make_ro) = distinct_step_inputs(state, op);
    let (resvalue_id, _resnull_id) = res_cells(state, op);

    // Datum save_arg0 = fcinfo->args[0].value (preserved across the make_ro
    // rewrite so the returned value is the ORIGINAL arg0, possibly read-write).
    // The canonical (by-ref-capable) gather: keep arg0's full Datum image.
    let save_arg0 = args[0].clone();

    if !nulls[0] && !nulls[1] {
        // The first argument might be an expanded datum; the comparison function
        // must receive a read-only pointer. The make_ro transform lives in the
        // expandeddatum owner, so it crosses the seam (mirror EEOP_ASSIGN_TMP_*).
        if make_ro {
            let mcx = estate.es_query_cxt;
            let ro = backend_utils_adt_misc2_seams::make_expanded_object_read_only_internal_v::call(
                mcx,
                &save_arg0,
            )?;
            args[0] = ro;
        }

        // fcinfo->isnull = false; result = op->d.func.fn_addr(fcinfo).
        let mcx = estate.es_query_cxt;
        let fn_expr = func_step_fn_expr(state, op);
        let (result_val, isnull) =
            function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;

        // if (!fcinfo->isnull && DatumGetBool(result)) -> equal -> return NULL.
        // Write through `write_cell` so a STATE_RESULT_CELL target reaches
        // `state.resvalue`/`state.resnull` (read by EEOP_DONE_RETURN).
        if !isnull && result_val.as_bool() {
            crate::interp_loop::write_cell(state, resvalue_id, DatumV::null(), true);
            return Ok(());
        }
    }

    // Arguments aren't equal (or one was NULL): return the first one.
    // *op->resvalue = save_arg0; *op->resnull = fcinfo->args[0].isnull;
    crate::interp_loop::write_cell(state, resvalue_id, save_arg0, nulls[0]);
    Ok(())
}

/// Read a `RowCompareStep`-payload step's `(fn_oid, fncollation, fn_strict)` and
/// gather its two argument cells into the call frame.
fn rowcompare_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    Vec<DatumV<'mcx>>,
    Vec<bool>,
    bool,
) {
    match step_data(state, op) {
        ExprEvalStepData::RowCompareStep {
            finfo,
            fcinfo_data,
            arg_cells,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_ROWCOMPARE_STEP: op->d.rowcompare_step.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_ROWCOMPARE_STEP: op->d.rowcompare_step.fcinfo_data missing");
            let cells = arg_cells
                .as_ref()
                .expect("EEOP_ROWCOMPARE_STEP: op->d.rowcompare_step.arg_cells missing");
            // The two compared column values were evaluated into their own arena
            // cells by the preceding sub-expression steps (the compiler aliases
            // their resvalue/resnull onto &fcinfo->args[0/1]); gather them into
            // the call frame immediately before the call. Carry the full canonical
            // Datum image (the by-reference-capable lane) so a by-ref text/name
            // column survives the gather — the BTORDER_PROC compare functions for
            // such types (e.g. bttextcmp) read their arguments by reference.
            let mut args: Vec<DatumV<'mcx>> = Vec::with_capacity(cells.len());
            let mut nulls: Vec<bool> = Vec::with_capacity(cells.len());
            for &cell in cells.iter() {
                let c = state.result_cells.get(cell);
                args.push(c.value.clone());
                nulls.push(c.isnull);
            }
            (finfo.fn_oid, fcinfo.fncollation, args, nulls, finfo.fn_strict)
        }
        other => unreachable!("EEOP_ROWCOMPARE_STEP carries the wrong payload: {other:?}"),
    }
}

/// `EEOP_ROWCOMPARE_STEP` core (execExprInterp.c): apply one column's comparison
/// function, force NULL on a strict-NULL input or NULL result, and short-circuit
/// (`jumpdone`) once an inequality is found. Returns the next step index to jump
/// to (the C `EEO_JUMP` / `EEO_NEXT` target).
pub fn exec_rowcompare_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    jumpnull: i32,
    jumpdone: i32,
    estate: &EStateData<'mcx>,
) -> PgResult<usize> {
    let (fn_oid, collation, args, nulls, fn_strict) = rowcompare_step_inputs(state, op);
    let (resvalue_id, _resnull_id) = res_cells(state, op);

    // All writes go through `write_cell`, which routes `STATE_RESULT_CELL`
    // (== id 0) to the `ExprState`'s own `resvalue`/`resnull` rather than the
    // auxiliary `result_cells[0]`; the EEOP_ROWCOMPARE_FINAL reader reads the
    // same cell via `read_cell`, so a row comparison whose result slot is the
    // ExprState's own cell (e.g. a top-level qual) must agree.

    // force NULL result if strict fn and NULL input
    if fn_strict && (nulls[0] || nulls[1]) {
        let cur = crate::interp_loop::read_cell(state, resvalue_id).0;
        crate::interp_loop::write_cell(state, resvalue_id, cur, true);
        return Ok(jumpnull as usize);
    }

    // fcinfo->isnull = false; d = op->d.rowcompare_step.fn_addr(fcinfo);
    // The canonical (by-reference-capable) call-frame lane so a by-ref text/name
    // column survives the gather; the BTORDER_PROC compare functions are not the
    // I/O-coerce strict shim, so no fn_expr node is threaded.
    let mcx = estate.es_query_cxt;
    let (value, isnull) =
        function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], None)?;

    // force NULL result if NULL function result
    if isnull {
        crate::interp_loop::write_cell(state, resvalue_id, value, true);
        return Ok(jumpnull as usize);
    }

    // *op->resvalue = d; *op->resnull = false; (the comparison yields a by-value
    // int32 cmpresult).
    let cmp = word_of(&value).as_i32();
    crate::interp_loop::write_cell(state, resvalue_id, value, false);

    // If unequal, no need to compare remaining columns.
    if cmp != 0 {
        return Ok(jumpdone as usize);
    }
    Ok(op + 1)
}

/// Read a `HashDatum`-payload step's `(fn_oid, fncollation)`, gather its single
/// argument cell into the call frame, and surface the previous (intermediate)
/// hash via `iresult`. The argument crosses to the hash function via the
/// canonical [`DatumV`] lane (the by-reference-capable form), so a by-ref
/// text/name/varchar key survives the gather — the bare-word `word_of`
/// downgrade panics on such a value (the type's hash function, e.g.
/// `hashtext`/`namehash`, reads its argument by reference).
/// Returns `(fn_oid, collation, args, arg0_isnull, iresult)`.
fn hashdatum_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    Vec<DatumV<'mcx>>,
    bool,
    u32,
) {
    match step_data(state, op) {
        ExprEvalStepData::HashDatum {
            finfo,
            fcinfo_data,
            arg_cell,
            iresult,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_HASHDATUM_*: op->d.hashdatum.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_HASHDATUM_*: op->d.hashdatum.fcinfo_data missing");
            // fcinfo->args[0] <- the hash-key cell the sub-expression evaluated.
            // Carry the full canonical Datum image (by-value word OR by-reference
            // referent bytes) so the hash function reads a by-ref key correctly.
            let c = state.result_cells.get(*arg_cell);
            let args = vec![c.value.clone()];
            // DatumGetUInt32(op->d.hashdatum.iresult->value) (NEXT32 only; the
            // FIRST variants ignore it). `iresult` carries the shared accumulator
            // cell id (C's aliased `iresult->value`); the prior column's hash
            // step wrote the running hash there, so read it back from that cell.
            let existing = (*iresult)
                .map(|cell| state.result_cells.get(cell).value.as_u32())
                .unwrap_or(0);
            (finfo.fn_oid, fcinfo.fncollation, args, c.isnull, existing)
        }
        other => unreachable!("EEOP_HASHDATUM_* carries the wrong payload: {other:?}"),
    }
}

/// Recover the `fn_expr` stamped onto an `EEOP_HASHDATUM_*` step's
/// `op->d.hashdatum.finfo->fn_expr`, threaded back to the by-ref `Datum`
/// dispatch (mirrors [`func_step_fn_expr`]).
fn hashdatum_step_fn_expr<'a, 'mcx>(
    state: &'a ExprState<'mcx>,
    op: usize,
) -> Option<&'a types_nodes::primnodes::Expr> {
    match step_data(state, op) {
        ExprEvalStepData::HashDatum { finfo, .. } => finfo
            .as_ref()?
            .fn_expr
            .as_ref()?
            .downcast_ref::<types_nodes::primnodes::Expr>(),
        _ => None,
    }
}

/// `EEOP_HASHDATUM_FIRST[_STRICT]` / `EEOP_HASHDATUM_NEXT32[_STRICT]` core
/// (execExprInterp.c): hash one key via the type's hash function and combine it
/// with the running hash (NEXT32 rotates the previous value left by 1 and XORs).
/// Returns the next step index (`jumpdone` on a strict NULL, else `op + 1`).
///
/// `first` selects the FIRST variants (no combine with a prior hash); `next32`
/// the NEXT32 variants. `strict` selects the `_STRICT` NULL-input behaviour
/// (return NULL / jump) vs the non-strict "treat NULL as 0 / leave hash alone".
pub fn exec_hashdatum_step<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    first: bool,
    strict: bool,
    jumpdone: i32,
    estate: &EStateData<'mcx>,
) -> PgResult<usize> {
    let (fn_oid, collation, args, arg0_isnull, existing) = hashdatum_step_inputs(state, op);
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let mcx = estate.es_query_cxt;
    let fn_expr = hashdatum_step_fn_expr(state, op);

    if first {
        if strict {
            // EEOP_HASHDATUM_FIRST_STRICT: NULL input -> NULL result, jump.
            if arg0_isnull {
                // For the FINAL (or single) hash column the build sets
                // `resvalue` to STATE_RESULT_CELL (`&state->resvalue`), so write
                // through `write_cell`; a raw `result_cells.set` would land in
                // the dead arena slot 0 and EEOP_DONE_RETURN would return a
                // stale value. (Same fix class as ExecEvalParamExtern.)
                crate::interp_loop::write_cell(state, resvalue_id, DatumV::null(), true);
                return Ok(jumpdone as usize);
            }
            let (value, _isnull) =
                function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
            crate::interp_loop::write_cell(state, resvalue_id, value, false);
            return Ok(op + 1);
        }
        // EEOP_HASHDATUM_FIRST: non-null -> hash; null -> 0.
        let value = if !arg0_isnull {
            let (value, _isnull) =
                function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
            value
        } else {
            DatumV::from_usize(0)
        };
        crate::interp_loop::write_cell(state, resvalue_id, value, false);
        return Ok(op + 1);
    }

    // NEXT32 variants. existinghash = pg_rotate_left32(iresult->value, 1).
    let rotated = existing.rotate_left(1);

    if strict {
        // EEOP_HASHDATUM_NEXT32_STRICT: NULL input -> NULL result, jump.
        if arg0_isnull {
            crate::interp_loop::write_cell(state, resvalue_id, DatumV::null(), true);
            return Ok(jumpdone as usize);
        }
        let (value, _isnull) =
            function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
        let hashvalue = value.as_u32();
        crate::interp_loop::write_cell(
            state,
            resvalue_id,
            DatumV::from_u32(rotated ^ hashvalue),
            false,
        );
        return Ok(op + 1);
    }

    // EEOP_HASHDATUM_NEXT32: leave the hash alone on NULL inputs.
    let combined = if !arg0_isnull {
        let (value, _isnull) =
            function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
        let hashvalue = value.as_u32();
        rotated ^ hashvalue
    } else {
        rotated
    };
    crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_u32(combined), false);
    Ok(op + 1)
}

/// `ExecEvalFuncExprFusage(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — call a (non-strict) function, tracking usage stats.
pub fn ExecEvalFuncExprFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    // PgStat_FunctionCallUsage fcusage;
    // Datum d;
    //
    // pgstat_init_function_usage(fcinfo, &fcusage);
    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *op->resvalue = d;
    // *op->resnull = fcinfo->isnull;
    // pgstat_end_function_usage(&fcusage, true);
    //
    // #296: the call-frame dispatch itself is now modeled (exec_func_step) —
    // the fmgr-widened FunctionCallInfoBaseData carries fncollation/args/isnull,
    // and function_call_invoke re-dispatches by fn_oid. The REMAINING blocker is
    // the pgstat usage tracking that wraps the call
    // (pgstat_init_function_usage / pgstat_end_function_usage): the FUSAGE
    // opcodes are selected precisely when pgstat_track_functions > fn_stats, so
    // they exist to record per-function execution stats — there is no pgstat
    // function-usage seam (the pgstat owner is unported), and silently running
    // the call without the surrounding init/end usage would drop the very stats
    // this opcode variant exists to collect. Mirror-PG-and-panic until the
    // pgstat function-usage seam lands; the non-FUSAGE EEOP_FUNCEXPR family is
    // the common, stats-free path and runs through exec_func_step.
    let _ = (state, op, econtext, estate);
    panic!(
        "ExecEvalFuncExprFusage: the function call itself is modeled \
         (exec_func_step), but the pgstat_init/end_function_usage tracking that \
         wraps it has no seam (pgstat owner unported); skipping it would drop the \
         per-function stats this FUSAGE opcode exists to collect. Blocked until \
         the pgstat function-usage seam lands"
    )
}

/// `ExecEvalFuncExprStrictFusage(...)` — call a strict function with usage stats
/// (NULL argument short-circuits to NULL).
pub fn ExecEvalFuncExprStrictFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
    // NullableDatum *args = fcinfo->args;
    // int nargs = op->d.func.nargs;
    //
    // /* strict function, so check for NULL args */
    // for (int argno = 0; argno < nargs; argno++)
    //     if (args[argno].isnull) { *op->resnull = true; return; }
    //
    // pgstat_init_function_usage(fcinfo, &fcusage);
    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *op->resvalue = d;
    // *op->resnull = fcinfo->isnull;
    // pgstat_end_function_usage(&fcusage, true);
    //
    // #296: the strict-NULL arg scan and the call dispatch are now modeled
    // (exec_func_step with strict=true reads the gathered fcinfo->args[i].isnull
    // and dispatches through function_call_invoke). The REMAINING blocker is the
    // pgstat usage tracking — see ExecEvalFuncExprFusage. Mirror-PG-and-panic
    // until the pgstat function-usage seam lands.
    let _ = (state, op, econtext, estate);
    panic!(
        "ExecEvalFuncExprStrictFusage: the strict-NULL scan + call are modeled \
         (exec_func_step), but the pgstat_init/end_function_usage tracking has \
         no seam (pgstat owner unported); skipping it would drop the per-function \
         stats this FUSAGE opcode exists to collect. Blocked until the pgstat \
         function-usage seam lands"
    )
}

/// `ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — fetch a PARAM_EXEC value from the econtext's param-exec array.
pub fn ExecEvalParamExec<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamExecData *prm;
    // prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
    // if (unlikely(prm->execPlan != NULL))
    // {
    //     /* Parameter not evaluated yet, so go do it */
    //     ExecSetParamPlan(prm->execPlan, econtext);
    //     Assert(prm->execPlan == NULL);
    // }
    // *op->resvalue = prm->value;
    // *op->resnull = prm->isnull;
    //
    // ecxt_param_exec_vals aliases the EState's es_param_exec_vals (the owned
    // model threads the EState explicitly; see ExprContext docs), so the param
    // is read from estate.es_param_exec_vals directly. The `execPlan` link is now
    // modeled on `ParamExecData` (an `ExecPlanLink` identity into
    // `es_subplanstates`), so `prm.execPlan.is_some()` is the C
    // `prm->execPlan != NULL` not-yet-evaluated test. The lazy-evaluation
    // re-entry (`ExecSetParamPlan(prm->execPlan, econtext)`) resolves that
    // identity back to its `SubPlanState` and runs the initplan; that resolution
    // is the executor's `exec_set_param_plan_for_pending` seam, still
    // seam-and-panic until nodeSubplan's `SubPlanState`-reachability wiring lands
    // (the `SubPlanState`s are owned by the parent plan-state's `initPlan` list,
    // not directly addressable from the param array yet). So for a PARAM_EXEC
    // whose value is already valid (`execPlan == None`) this reads straight
    // through; a pending one would need that unported re-entry.
    let paramid = match &step_data(state, op) {
        ExprEvalStepData::Param { paramid, .. } => *paramid,
        _ => unreachable!("ExecEvalParamExec: step is not an EEOP_PARAM_EXEC"),
    };

    // if (unlikely(prm->execPlan != NULL)) { ExecSetParamPlan(prm->execPlan,
    // econtext); Assert(prm->execPlan == NULL); }
    let pending = estate.es_param_exec_vals[paramid as usize]
        .execPlan
        .map(|link| link.plan_id);
    if let Some(plan_id) = pending {
        // Resolve the subplan's identity back to its InitPlan SubPlanState (the
        // es_initplan registry, keyed by the 1-based plan_id) and run it; it
        // writes the param value(s) and clears execPlan. Take the SubPlanState
        // out for the call (it lives in estate, which the call also borrows
        // mutably) and put it back afterwards.
        let idx = (plan_id as usize).saturating_sub(1);
        let mut sstate = estate
            .es_initplan
            .get_mut(idx)
            .and_then(|slot| slot.take())
            .ok_or_else(|| {
                types_error::PgError::error(
                    "ExecEvalParamExec: initplan not found for pending PARAM_EXEC",
                )
            })?;
        let r = backend_executor_nodeSubplan_seams::exec_set_param_plan::call(
            &mut sstate, econtext, estate,
        );
        estate.es_initplan[idx] = Some(sstate);
        r?;
        debug_assert!(estate.es_param_exec_vals[paramid as usize].execPlan.is_none());
    }

    let prm = &estate.es_param_exec_vals[paramid as usize];
    let value = prm.value.clone();
    let isnull = prm.isnull;

    // *op->resvalue = prm->value; *op->resnull = prm->isnull. Write through
    // `write_cell`, which routes the ExprState's own result cell (id 0) to
    // `state.resvalue`/`state.resnull` rather than the dead `result_cells[0]` —
    // a top-level Param whose result slot IS the ExprState cell (e.g.
    // `SELECT (SELECT ...)`) otherwise wrote to the wrong location and the
    // EEOP_ASSIGN_TMP reader saw zero.
    let (resvalue_id, resnull_id) = res_cells(state, op);
    let _ = resnull_id; // value/is-null share one cell
    crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    Ok(())
}

/// `ExecEvalParamExtern(...)` — fetch a PARAM_EXTERN value from the param list.
pub fn ExecEvalParamExtern<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamListInfo paramInfo = econtext->ecxt_param_list_info;
    // int paramId = op->d.param.paramid;
    //
    // if (likely(paramInfo && paramId > 0 && paramId <= paramInfo->numParams))
    // {
    //     ParamExternData *prm, prmdata;
    //     /* give hook a chance in case parameter is dynamic */
    //     if (paramInfo->paramFetch != NULL)
    //         prm = paramInfo->paramFetch(paramInfo, paramId, false, &prmdata);
    //     else
    //         prm = &paramInfo->params[paramId - 1];
    //     if (likely(OidIsValid(prm->ptype)))
    //     {
    //         if (unlikely(prm->ptype != op->d.param.paramtype))
    //             ereport(ERROR, (ERRCODE_DATATYPE_MISMATCH, ...));
    //         *op->resvalue = prm->value;
    //         *op->resnull = prm->isnull;
    //         return;
    //     }
    // }
    // ereport(ERROR, (ERRCODE_UNDEFINED_OBJECT,
    //                 errmsg("no value found for parameter %d", paramId)));
    //
    // ecxt_param_list_info aliases the EState's es_param_list_info, now a real
    // value `ParamListInfo` (`Option<Rc<ParamListInfoData>>`); the params[]
    // array / numParams are read directly off it. The dynamic paramFetch hook
    // lives in an unported subsystem (`param_fetch == true` would reach a loud
    // panic in the params owner); the static array path is the common case.
    let _ = econtext;
    let (paramid, paramtype) = match &step_data(state, op) {
        ExprEvalStepData::Param { paramid, paramtype } => (*paramid, *paramtype),
        _ => unreachable!("ExecEvalParamExtern: step is not an EEOP_PARAM_EXTERN"),
    };

    if let Some(param_info) = estate.es_param_list_info.as_ref() {
        if paramid > 0 && paramid <= param_info.num_params {
            // give hook a chance in case parameter is dynamic
            if param_info.param_fetch {
                panic!(
                    "ExecEvalParamExtern: dynamic ParamListInfo paramFetch hook \
                     invoked, but no paramFetch owner is ported (the hook lives in \
                     an unported subsystem)"
                );
            }
            let prm = &param_info.params[(paramid - 1) as usize];
            if prm.ptype != 0 {
                if prm.ptype != paramtype {
                    return Err(types_error::PgError::error(format!(
                        "type of parameter {paramid} ({}) does not match that when \
                         preparing the plan ({})",
                        prm.ptype, paramtype
                    ))
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH));
                }
                // *op->resvalue = prm->value; *op->resnull = prm->isnull. Write
                // through `write_cell`, which routes the ExprState's own result
                // cell (id 0 / STATE_RESULT_CELL) to `state.resvalue`/
                // `state.resnull` rather than the dead `result_cells[0]`. A
                // top-level PARAM_EXTERN whose result slot IS the ExprState cell
                // (e.g. a SQL-function body `SELECT $1`, where `$1` is the whole
                // projection target) otherwise wrote to the wrong location, so
                // the EEOP_ASSIGN_TMP reader saw the stale `state.resvalue`
                // (zero) — dropping a by-reference value's image entirely (the
                // "name arg missing from by-ref lane" symptom). Mirrors the
                // `ExecEvalParamExec` fix.
                let (resvalue_id, _resnull_id) = res_cells(state, op);
                crate::interp_loop::write_cell(
                    state,
                    resvalue_id,
                    prm.value.clone(),
                    prm.isnull,
                );
                return Ok(());
            }
        }
    }

    Err(types_error::PgError::error(format!(
        "no value found for parameter {paramid}"
    ))
    .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT))
}

/// `ExecEvalParamSet(...)` — store a value into a PARAM_EXEC slot.
pub fn ExecEvalParamSet<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParamExecData *prm;
    // prm = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);
    // /* Shouldn't have a pending evaluation anymore */
    // Assert(prm->execPlan == NULL);
    // prm->value = *op->resvalue;
    // prm->isnull = *op->resnull;
    //
    // Same param-array aliasing as ExecEvalParamExec: write into the EState's
    // es_param_exec_vals from this step's result cell. (ParamExecData is trimmed
    // — no execPlan — so the Assert is vacuous here.)
    let _ = econtext;
    let paramid = match &step_data(state, op) {
        ExprEvalStepData::Param { paramid, .. } => *paramid,
        _ => unreachable!("ExecEvalParamSet: step is not an EEOP_PARAM_SET"),
    };

    // C: prm->value = *op->resvalue; prm->isnull = *op->resnull;
    // `op->resvalue`/`op->resnull` is the step's result cell. When that cell is
    // STATE_RESULT_CELL (id 0), the value/isnull live in the ExprState's own
    // resvalue/resnull, NOT in result_cells[0] (which is then unused). The
    // preceding arg-eval (e.g. an EEOP_*VAR) honors that sentinel via
    // write_cell, so the read here must too — read_cell does, a bare
    // result_cells.get(0) does not. (This is the case for a top-level SubPlan
    // expression, where the correlation arg evaluates into the state's own
    // resvalue; reading result_cells[0] instead returned a stale 0, which made
    // every correlated PARAM_EXEC come back NULL.)
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let (value, isnull) = crate::interp_loop::read_cell(state, resvalue_id);

    let prm = &mut estate.es_param_exec_vals[paramid as usize];
    prm.value = value;
    prm.isnull = isnull;
    Ok(())
}

/// Read an `IoCoerce` step payload's `(out_oid, out_coll, in_oid, in_coll,
/// in_strict, in_args)`: the resolved output/input function OIDs + collations,
/// the input function's strictness, and the input function's preloaded constant
/// arg frame (`args[1] = typioparam`, `args[2] = -1`, with `args[0]` a
/// placeholder the eval fills).
fn iocoerce_step_inputs<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    types_core::primitive::Oid,
    bool,
    Vec<types_datum::NullableDatum>,
) {
    match step_data(state, op) {
        ExprEvalStepData::IoCoerce {
            finfo_out,
            fcinfo_data_out,
            finfo_in,
            fcinfo_data_in,
        } => {
            let finfo_out = finfo_out
                .as_ref()
                .expect("EEOP_IOCOERCE: op->d.iocoerce.finfo_out not resolved");
            let fcinfo_out = fcinfo_data_out
                .as_ref()
                .expect("EEOP_IOCOERCE: op->d.iocoerce.fcinfo_data_out missing");
            let finfo_in = finfo_in
                .as_ref()
                .expect("EEOP_IOCOERCE: op->d.iocoerce.finfo_in not resolved");
            let fcinfo_in = fcinfo_data_in
                .as_ref()
                .expect("EEOP_IOCOERCE: op->d.iocoerce.fcinfo_data_in missing");
            (
                finfo_out.fn_oid,
                fcinfo_out.fncollation,
                finfo_in.fn_oid,
                fcinfo_in.fncollation,
                finfo_in.fn_strict,
                fcinfo_in.args.clone(),
            )
        }
        other => unreachable!("EEOP_IOCOERCE step carries the wrong payload: {other:?}"),
    }
}

/// Shared body of `EEOP_IOCOERCE` (inline) and `EEOP_IOCOERCE_SAFE`
/// (`ExecEvalCoerceViaIOSafe`): run the source type's output function on the
/// result cell, then the result type's input function on the resulting cstring.
/// The C frames preload `args[1]`/`args[2]` (typioparam, -1) at compile; the
/// resolved `FmgrInfo` cannot cross the seam, so each call dispatches by OID
/// through `function_call_invoke` (#296). `str == NULL` is the zero `Datum`
/// word (a NULL cstring pointer), exactly as C tests `str != NULL`.
fn iocoerce_core<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    safe: bool,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let (out_oid, out_coll, in_oid, in_coll, in_strict, in_args) =
        iocoerce_step_inputs(state, op);
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    // C reads `*op->resvalue` / `*op->resnull`. The cell id may be
    // STATE_RESULT_CELL (the ExprState's own resvalue/resnull, e.g. a top-level
    // CoerceViaIO over a Const); read through `read_cell` so that aliasing is
    // honored — a bare `result_cells.get` would return a default ByVal cell and
    // drop a by-reference / cstring value (WALL: "cstring arg missing from
    // by-ref lane").
    let (cur_value, cur_isnull) = crate::interp_loop::read_cell(state, resvalue_id);

    // /* call output function (similar to OutputFunctionCall) */
    // if (*op->resnull) str = NULL; else { args[0]=*resvalue; str = invoke(out); }
    //
    // The output function returns a `cstring` on the by-reference lane; dispatch
    // through the canonical-Datum lane so the cstring referent is materialized
    // into `mcx` and can be handed to the input function's by-reference arg slot
    // (the bare-word lane would drop it — WALL: "bool fn: cstring arg missing
    // from by-ref lane").
    let str_datum: Option<DatumV<'mcx>> = if cur_isnull {
        None // str = NULL
    } else {
        let out_call_args = [cur_value.clone()];
        let (val, _isnull) =
            function_call_invoke_datum::call(mcx, out_oid, out_coll, &out_call_args, &[], None)?;
        // Assert(!fcinfo_out->isnull) — output functions never return NULL.
        Some(val)
    };

    // /* call input function (similar to InputFunctionCall[Safe]) */
    // if (!finfo_in->fn_strict || str != NULL) { args[0]={str, *resnull}; ... }
    let str_is_null = str_datum.is_none();
    if !in_strict || !str_is_null {
        // fcinfo_in->args[0].value = PointerGetDatum(str);
        // fcinfo_in->args[0].isnull = *op->resnull;
        // (args[1] = typioparam, args[2] = -1 are already preloaded.)
        // Build the input call frame on the canonical-Datum lane: the cstring
        // referent in args[0], the preloaded by-value constants (typioparam, -1)
        // re-wrapped as by-value Datums.
        let mut in_datum_args: Vec<DatumV<'mcx>> = Vec::with_capacity(in_args.len());
        in_datum_args.push(match &str_datum {
            Some(d) => d.clone(),
            None => DatumV::null(),
        });
        for nd in in_args.iter().skip(1) {
            in_datum_args.push(DatumV::from_usize(nd.value.as_usize()));
        }

        if safe {
            // EEOP_IOCOERCE_SAFE reads SOFT_ERROR_OCCURRED(fcinfo_in->context)
            // after the call to detect a caught conversion error. The
            // soft-error sink (ErrorSaveContext on fcinfo->context) is owned by
            // the not-yet-ported elog/ErrorSaveContext layer (the IoCoerce
            // frame's context stays None until that lands), so this safe variant
            // cannot observe the soft error.
            let _ = (in_oid, in_coll, in_datum_args);
            panic!(
                "ExecEvalCoerceViaIOSafe: the input function runs under a soft-error \
                 ErrorSaveContext (fcinfo_in->context) and the arm reads \
                 SOFT_ERROR_OCCURRED(context) to convert a caught error into a NULL \
                 result; the soft-error sink is owned by the unported \
                 elog/ErrorSaveContext layer (IoCoerce frame context is None); \
                 blocked until that lands"
            );
        }

        let (value, isnull) =
            function_call_invoke_datum::call(mcx, in_oid, in_coll, &in_datum_args, &[], None)?;
        // *op->resvalue = d;  (resnull is unchanged: null iff str was NULL).
        // Write through `write_cell` so a STATE_RESULT_CELL target reaches the
        // ExprState's own resvalue/resnull (mirror of the read above).
        crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    }
    Ok(())
}

/// `EEOP_IOCOERCE` (execExprInterp.c inline case) — the non-soft-error
/// output-then-input I/O coercion.
pub fn ExecEvalCoerceViaIO<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    iocoerce_core(state, op, false, estate.es_query_cxt)
}

/// `ExecEvalCoerceViaIOSafe(ExprState *state, ExprEvalStep *op)` — output-then-
/// input I/O coercion with soft-error handling.
pub fn ExecEvalCoerceViaIOSafe<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    iocoerce_core(state, op, true, estate.es_query_cxt)
}

/// `ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op)` — evaluate
/// CURRENT_DATE / CURRENT_USER / etc.
///
/// ```c
/// LOCAL_FCINFO(fcinfo, 0);
/// SQLValueFunction *svf = op->d.sqlvaluefunction.svf;
/// *op->resnull = false;
/// switch (svf->op) { ... }
/// ```
///
/// The date/time arms call the datetime owner's `GetSQLCurrent*` /
/// `GetSQLLocal*` helpers and box the result into the canonical [`Datum`] (a
/// by-value `date`/`time`/`timestamp` word, or a 12-byte by-reference `timetz`
/// image for `CURRENT_TIME`). The role/user/catalog/schema arms mirror C's
/// `InitFunctionCallInfoData(*fcinfo, ...); current_user(fcinfo)` by dispatching
/// the corresponding pg_proc builtin through the OID-keyed fmgr call frame
/// (`function_call_invoke_datum`), which materializes the by-reference `name`
/// result and surfaces `fcinfo->isnull`.
pub fn ExecEvalSQLValueFunction<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use types_nodes::primnodes::SQLValueFunctionOp::*;

    let svf = match step_data(state, op) {
        ExprEvalStepData::SqlValueFunction { svf } => *svf,
        other => panic!("ExecEvalSQLValueFunction: wrong step payload {other:?}"),
    };
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let mcx = estate.es_query_cxt;

    use backend_utils_adt_datetime::date::{GetSQLCurrentDate, GetSQLCurrentTime, GetSQLLocalTime};
    use backend_utils_adt_datetime::timestamp::{GetSQLCurrentTimestamp, GetSQLLocalTimestamp};

    // *op->resnull = false; (the role/user/catalog/schema arms may set it true)
    let (value, isnull): (DatumV<'mcx>, bool) = match svf.op {
        SVFOP_CURRENT_DATE => {
            // DateADTGetDatum(GetSQLCurrentDate())
            (DatumV::from_i32(GetSQLCurrentDate()), false)
        }
        SVFOP_CURRENT_TIME | SVFOP_CURRENT_TIME_N => {
            // TimeTzADTPGetDatum(GetSQLCurrentTime(svf->typmod)) — `timetz` is
            // pass-by-reference; the 12-byte image is time:i64 LE + zone:i32 LE.
            let t = GetSQLCurrentTime(svf.typmod);
            let mut img: Vec<u8> = Vec::with_capacity(12);
            img.extend_from_slice(&(t.time as i64).to_le_bytes());
            img.extend_from_slice(&t.zone.to_le_bytes());
            (DatumV::ByRef(mcx::slice_in(mcx, &img)?), false)
        }
        SVFOP_CURRENT_TIMESTAMP | SVFOP_CURRENT_TIMESTAMP_N => {
            // TimestampTzGetDatum(GetSQLCurrentTimestamp(svf->typmod))
            (DatumV::from_i64(GetSQLCurrentTimestamp(svf.typmod)?), false)
        }
        SVFOP_LOCALTIME | SVFOP_LOCALTIME_N => {
            // TimeADTGetDatum(GetSQLLocalTime(svf->typmod))
            (DatumV::from_i64(GetSQLLocalTime(svf.typmod)), false)
        }
        SVFOP_LOCALTIMESTAMP | SVFOP_LOCALTIMESTAMP_N => {
            // TimestampGetDatum(GetSQLLocalTimestamp(svf->typmod))
            (DatumV::from_i64(GetSQLLocalTimestamp(svf.typmod)?), false)
        }
        SVFOP_CURRENT_ROLE | SVFOP_CURRENT_USER | SVFOP_USER => {
            // InitFunctionCallInfoData(*fcinfo, NULL, 0, InvalidOid, NULL, NULL);
            // *op->resvalue = current_user(fcinfo); *op->resnull = fcinfo->isnull;
            sql_value_builtin(mcx, F_CURRENT_USER)?
        }
        SVFOP_SESSION_USER => sql_value_builtin(mcx, F_SESSION_USER)?,
        SVFOP_CURRENT_CATALOG => sql_value_builtin(mcx, F_CURRENT_DATABASE)?,
        SVFOP_CURRENT_SCHEMA => sql_value_builtin(mcx, F_CURRENT_SCHEMA)?,
    };

    crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    Ok(())
}

/// pg_proc OIDs of the zero-argument SQL value-function builtins (pg_proc.dat).
const F_CURRENT_USER: u32 = 745;
const F_SESSION_USER: u32 = 746;
const F_CURRENT_DATABASE: u32 = 861;
const F_CURRENT_SCHEMA: u32 = 1402;

/// Mirror C's `InitFunctionCallInfoData(*fcinfo, NULL, 0, InvalidOid, NULL,
/// NULL); current_user(fcinfo); *op->resnull = fcinfo->isnull;` — run a
/// zero-argument SQL value-function builtin through the OID-keyed fmgr call
/// frame and return its `(value, isnull)`.
fn sql_value_builtin<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fn_oid: u32,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // Oid is `u32`; InvalidOid (0) collation matches C's InitFunctionCallInfoData.
    function_call_invoke_datum::call(mcx, fn_oid, 0, &[], &[], None)
}

/// `ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)` — CURRENT OF
/// cursor reference (always errors at runtime; resolved by the scan node).
pub fn ExecEvalCurrentOfExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ereport(ERROR,
    //         (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //          errmsg("WHERE CURRENT OF is not supported for this table type")));
    //
    // The planner converts CURRENT OF into a TidScan qualification (or FDW
    // handling), so ExecInitExpr accepts a CurrentOfExpr but it should never be
    // executed; reaching here means an unhandled CURRENT OF (e.g. on a foreign
    // table whose FDW doesn't support it).
    let _ = (state, op, estate);
    Err(PgError::error("WHERE CURRENT OF is not supported for this table type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `ExecEvalNextValueExpr(ExprState *state, ExprEvalStep *op)` — evaluate a
/// column DEFAULT nextval() during COPY/INSERT.
pub fn ExecEvalNextValueExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // int64 newval = nextval_internal(op->d.nextvalueexpr.seqid, false);
    // switch (op->d.nextvalueexpr.seqtypid) {
    //   case INT2OID: *op->resvalue = Int16GetDatum((int16) newval); break;
    //   case INT4OID: *op->resvalue = Int32GetDatum((int32) newval); break;
    //   case INT8OID: *op->resvalue = Int64GetDatum((int64) newval); break;
    //   default: elog(ERROR, "unsupported sequence type %u", ...);
    // }
    // *op->resnull = false;
    //
    use types_core::catalog::{INT2OID, INT4OID, INT8OID};

    let (seqid, seqtypid) = match step_data(state, op) {
        ExprEvalStepData::NextValueExpr { seqid, seqtypid } => (*seqid, *seqtypid),
        other => panic!("ExecEvalNextValueExpr: wrong step payload {other:?}"),
    };
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let mcx = estate.es_query_cxt;

    // int64 newval = nextval_internal(op->d.nextvalueexpr.seqid, false);
    let newval = backend_commands_sequence_seams::nextval_internal::call(mcx, seqid, false)?;

    // switch (op->d.nextvalueexpr.seqtypid) {
    //   case INT2OID: *op->resvalue = Int16GetDatum((int16) newval); break;
    //   case INT4OID: *op->resvalue = Int32GetDatum((int32) newval); break;
    //   case INT8OID: *op->resvalue = Int64GetDatum((int64) newval); break;
    //   default: elog(ERROR, "unsupported sequence type %u", seqtypid);
    // }
    let value: DatumV<'mcx> = match seqtypid {
        INT2OID => DatumV::from_i16(newval as i16),
        INT4OID => DatumV::from_i32(newval as i32),
        INT8OID => DatumV::from_i64(newval),
        other => {
            return Err(PgError::error(format!(
                "unsupported sequence type {other}"
            )))
        }
    };

    // *op->resnull = false;
    crate::interp_loop::write_cell(state, resvalue_id, value, false);
    Ok(())
}

/// `ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)` — domain
/// NOT NULL constraint check.
pub fn ExecEvalConstraintNotNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (*op->resnull)
    //     errsave((Node *) op->d.domaincheck.escontext,
    //             (errcode(ERRCODE_NOT_NULL_VIOLATION),
    //              errmsg("domain %s does not allow null values",
    //                     format_type_be(op->d.domaincheck.resulttype)),
    //              errdatatype(op->d.domaincheck.resulttype)));
    //
    // errsave with a NULL escontext == ereport(ERROR). op->d.domaincheck
    // .escontext is a parked opaque address (the soft-error sink is not threaded
    // here yet); NULL (0) is the hard-throw case, matching the common path.
    // format_type_be() (lsyscache/format-type owner) is not a dependency of this
    // crate, so the type name is rendered from the OID; the load-bearing
    // behavior (the NOT_NULL_VIOLATION on a null domain value) is faithful.
    let _ = estate;
    let resulttype = match &step_data(state, op) {
        ExprEvalStepData::DomainCheck { resulttype, .. } => *resulttype,
        _ => unreachable!("ExecEvalConstraintNotNull: step is not an EEOP_DOMAIN_NOTNULL"),
    };

    // C reads `*op->resnull` — the step's result variable, which aliases
    // `state->resnull` (the STATE_RESULT_CELL sentinel) when the domain value was
    // evaluated into the ExprState's own result slot. Resolve through the
    // sentinel-aware accessor so a top-level CoerceToDomain (whose arg targets
    // STATE_RESULT_CELL) is not read from a never-populated arena slot 0.
    let (resvalue_id, _resnull_id) = res_cells(state, op);
    let (_value, resnull) = crate::interp_loop::read_cell(state, resvalue_id);

    if resnull {
        return Err(PgError::error(format!(
            "domain {} does not allow null values",
            // format_type_be(resulttype) — owner (format-type) not a dep here.
            resulttype
        ))
        .with_sqlstate(ERRCODE_NOT_NULL_VIOLATION));
    }
    Ok(())
}

/// `ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)` — single domain
/// CHECK constraint evaluation.
pub fn ExecEvalConstraintCheck<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // if (!*op->d.domaincheck.checknull &&
    //     !DatumGetBool(*op->d.domaincheck.checkvalue))
    //     errsave((Node *) op->d.domaincheck.escontext,
    //             (errcode(ERRCODE_CHECK_VIOLATION),
    //              errmsg("value for domain %s violates check constraint \"%s\"",
    //                     format_type_be(op->d.domaincheck.resulttype),
    //                     op->d.domaincheck.constraintname),
    //              errdomainconstraint(op->d.domaincheck.resulttype,
    //                                  op->d.domaincheck.constraintname)));
    //
    // d.domaincheck.checkvalue is a ResultCellId naming the cell the CHECK
    // expression's result was written into; read its value/is-null pair. As in
    // ConstraintNotNull, escontext is a parked opaque address (NULL == throw)
    // and format_type_be is not a dep, so the type name is rendered from the
    // OID; the constraint-violation behavior is faithful.
    let _ = estate;
    let (checkvalue_id, resulttype, constraintname) = match &step_data(state, op) {
        ExprEvalStepData::DomainCheck {
            checkvalue,
            resulttype,
            constraintname,
            ..
        } => (
            *checkvalue,
            *resulttype,
            constraintname
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_default(),
        ),
        _ => unreachable!("ExecEvalConstraintCheck: step is not an EEOP_DOMAIN_CHECK"),
    };

    let check = state.result_cells.get(checkvalue_id);
    let checknull = check.isnull;
    // DatumGetBool(X) — any nonzero word reads as true.
    let checkbool = check.value.as_bool();

    if !checknull && !checkbool {
        return Err(PgError::error(format!(
            "value for domain {} violates check constraint \"{}\"",
            // format_type_be(resulttype) — owner (format-type) not a dep here.
            resulttype, constraintname
        ))
        .with_sqlstate(ERRCODE_CHECK_VIOLATION));
    }
    Ok(())
}

/// `ExecEvalSysVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext,
/// TupleTableSlot *slot)` — fetch a system attribute (ctid, xmin, ...).
pub fn ExecEvalSysVar<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    slot: types_nodes::SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum d;
    // /* OLD/NEW system attribute is NULL if OLD/NEW row is NULL */
    // if ((op->d.var.varreturningtype == VAR_RETURNING_OLD &&
    //      state->flags & EEO_FLAG_OLD_IS_NULL) ||
    //     (op->d.var.varreturningtype == VAR_RETURNING_NEW &&
    //      state->flags & EEO_FLAG_NEW_IS_NULL))
    // {
    //     *op->resvalue = (Datum) 0; *op->resnull = true; return;
    // }
    // /* slot_getsysattr has sufficient defenses against bad attnums */
    // d = slot_getsysattr(slot, op->d.var.attnum, op->resnull);
    // *op->resvalue = d;
    // if (unlikely(*op->resnull))
    //     elog(ERROR, "failed to fetch attribute from slot");
    //
    // The OLD/NEW-is-NULL short-circuit IS fully expressible (read d.var
    // .varreturningtype + the state flags + write the result cell). The fetch
    // itself — slot_getsysattr(slot, attnum, &isnull) — reads a system column
    // out of the slot's underlying tuple, which is owned by the (unported)
    // execTuples slot-payload model (the trimmed TupleTableSlot exposes no
    // value/tuple storage, and execTuples-seams offers no slot_getsysattr).
    // Faithful for the short-circuit; the actual sysattr fetch is blocked on
    // execTuples.
    use types_nodes::execexpr::{VarReturningType, EEO_FLAG_NEW_IS_NULL, EEO_FLAG_OLD_IS_NULL};

    let (varreturningtype, attnum) = match &step_data(state, op) {
        ExprEvalStepData::Var { varreturningtype, attnum, .. } => (*varreturningtype, *attnum),
        _ => unreachable!("ExecEvalSysVar: step is not an EEOP_*_SYSVAR"),
    };

    let (resvalue_id, _resnull_id) = res_cells(state, op);

    if (varreturningtype == VarReturningType::VAR_RETURNING_OLD
        && (state.flags & EEO_FLAG_OLD_IS_NULL) != 0)
        || (varreturningtype == VarReturningType::VAR_RETURNING_NEW
            && (state.flags & EEO_FLAG_NEW_IS_NULL) != 0)
    {
        // Write through `write_cell`, which routes `STATE_RESULT_CELL` (== id 0)
        // to the ExprState's own `resvalue`/`resnull` rather than the dead
        // `result_cells[0]` slot — a `Var` step's `resvalue` is STATE_RESULT_CELL,
        // so a raw `result_cells.set` would never reach the cell the following
        // ASSIGN_TMP reads (`state.resvalue`).
        crate::interp_loop::write_cell(state, resvalue_id, DatumV::null(), true);
        return Ok(());
    }

    // d = slot_getsysattr(slot, op->d.var.attnum, op->resnull);
    //
    // The slot header carries the two system attributes that do not live in the
    // physical tuple: `ctid` (SelfItemPointerAttributeNumber -> tts_tid) and
    // `tableoid` (TableOidAttributeNumber -> tts_tableOid). slot_getsysattr
    // returns these *before* dispatching to the per-kind getsysattr callback (see
    // execTuples slot_ops_vtables::slot_getsysattr), so they are reachable from
    // the pooled TupleTableSlot header without the full payload model. The
    // remaining system attributes (xmin/xmax/cmin/cmax) require the underlying
    // physical tuple, which the trimmed pooled slot does not carry — those stay
    // blocked on the execTuples slot-payload model.
    let _ = econtext;
    let mcx = estate.es_query_cxt;
    let (value, isnull): (DatumV<'mcx>, bool) =
        if attnum == types_tuple::heaptuple::SelfItemPointerAttributeNumber as i32 {
            let s = estate.slot_mut(slot);
            let bytes = backend_access_common_heaptuple::item_pointer_bytes(
                mcx,
                &s.tts_tid,
            )?;
            (DatumV::ByRef(bytes), false)
        } else if attnum == types_tuple::heaptuple::TableOidAttributeNumber as i32 {
            let s = estate.slot_mut(slot);
            (DatumV::from_oid(s.tts_tableOid), false)
        } else {
            panic!(
                "ExecEvalSysVar: system attribute {attnum} (other than ctid/tableoid) \
                 must be read from the slot's underlying physical tuple, owned by the \
                 unported execTuples slot-payload model (the trimmed pooled \
                 TupleTableSlot carries only tts_values / tts_tid / tts_tableOid); \
                 blocked until execTuples lands."
            );
        };

    // if (unlikely(*op->resnull)) elog(ERROR, "failed to fetch attribute from slot");
    if isnull {
        return Err(types_error::PgError::error(
            "failed to fetch attribute from slot",
        ));
    }
    // Write through `write_cell` (STATE_RESULT_CELL routes to `state.resvalue`).
    crate::interp_loop::write_cell(state, resvalue_id, value, isnull);
    Ok(())
}

/// `ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)` — `x op ANY/ALL
/// (array)` by linear scan over the array elements.
pub fn ExecEvalScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // FunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo_data;
    // bool useOr = op->d.scalararrayop.useOr;
    // bool strictfunc = op->d.scalararrayop.finfo->fn_strict;
    // ArrayType *arr; int nitems; Datum result; bool resultnull; ...
    //
    // if (*op->resnull) return;                 /* NULL array => NULL */
    // arr = DatumGetArrayTypeP(*op->resvalue);
    // nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    // if (nitems <= 0) { *op->resvalue = BoolGetDatum(!useOr); *op->resnull = false; return; }
    // if (fcinfo->args[0].isnull && strictfunc) { *op->resnull = true; return; }
    // if (op->d.scalararrayop.element_type != ARR_ELEMTYPE(arr))
    //     get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
    // result = BoolGetDatum(!useOr); resultnull = false;
    // for each element: load fcinfo->args[1]; thisresult = fn_addr(fcinfo);
    //   combine per OR/AND; ...
    // *op->resvalue = result; *op->resnull = resultnull;
    //
    // Read the step payload: comparison fn OID + collation, useOr, strictfunc.
    let (use_or, strictfunc, fn_oid, collation, scalar_cell) = match step_data(state, op) {
        ExprEvalStepData::ScalarArrayOp {
            use_or,
            finfo,
            fcinfo_data,
            scalar_cell,
            ..
        } => {
            let finfo = finfo
                .as_ref()
                .expect("EEOP_SCALARARRAYOP: op->d.scalararrayop.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("EEOP_SCALARARRAYOP: op->d.scalararrayop.fcinfo_data missing");
            (*use_or, finfo.fn_strict, finfo.fn_oid, fcinfo.fncollation, *scalar_cell)
        }
        other => unreachable!("EEOP_SCALARARRAYOP step carries the wrong payload: {other:?}"),
    };

    let (resvalue_id, resnull_id) = res_cells(state, op);

    // if (*op->resnull) return;  /* NULL array => NULL result */
    //
    // Read the array value through `read_cell`, which routes a
    // `STATE_RESULT_CELL` (id 0) target to `state.resvalue`/`state.resnull` —
    // the array arg is `ExecInitExprRec`'d into `*op->resvalue`, which is the
    // top-level result cell when this SAOP is the whole expression (so the
    // bare `result_cells.get(0)` arena read would return a default-NULL cell).
    let (arr_value, arr_isnull) = crate::interp_loop::read_cell(state, resvalue_id);
    if arr_isnull {
        return Ok(());
    }

    // arr = DatumGetArrayTypeP(*op->resvalue); detoast + deconstruct.
    //
    // The array Const/result value is a canonical by-reference value (the
    // on-disk array varlena image lives in its `ByRef` bytes). Read it through
    // the bytes/value lane — NOT the bare-word `Datum` surrogate, which carries
    // no payload and detoasts to an empty buffer (the "index out of bounds:
    // len 0 index 0" array-deconstruct fault).
    let arr_bytes = arr_value.as_ref_bytes();
    let mcx = estate.es_query_cxt;
    let elemtype =
        backend_utils_adt_arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, arr_bytes)?;
    let tlba = backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(elemtype)?;
    // deconstruct subsumes ArrayGetNItems + the ARR_DATA_PTR / null-bitmap
    // fetch_att walk: it yields the per-element canonical `(Datum, isnull)`
    // pairs (by-ref elements keep their full image so the comparison function
    // sees the real value, not a downgraded word).
    let elements = backend_utils_adt_arrayfuncs_seams::deconstruct_array_values_bytes::call(
        mcx,
        arr_bytes,
        elemtype,
        tlba.typlen,
        tlba.typbyval,
        tlba.typalign as core::ffi::c_char,
    )?;
    let nitems = elements.len();

    // if (nitems <= 0) { *op->resvalue = BoolGetDatum(!useOr); *op->resnull = false; return; }
    if nitems == 0 {
        crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_bool(!use_or), false);
        return Ok(());
    }

    // if (fcinfo->args[0].isnull && strictfunc) { *op->resnull = true; return; }
    // The scalar (LHS) crosses the comparison on the canonical by-ref-capable
    // lane too — it may itself be a by-reference value (e.g. `'x' IN (...)`).
    let (scalar_value, scalar_isnull) = crate::interp_loop::read_cell(state, scalar_cell);
    if scalar_isnull && strictfunc {
        crate::interp_loop::write_cell(state, resnull_id, DatumV::null(), true);
        return Ok(());
    }

    // result = BoolGetDatum(!useOr); resultnull = false;
    let mut result = !use_or;
    let mut resultnull = false;

    // Loop over the array elements.
    for (elt, elt_isnull) in elements.into_iter() {
        // Call comparison function (strict-null short-circuit on the element).
        let (this_isnull, thisresult) = if elt_isnull && strictfunc {
            (true, false)
        } else {
            // Dispatch on the canonical by-reference-capable lane: a by-ref
            // scalar or element (text/numeric/...) reaches the comparison
            // function as its full image, not a downgraded word.
            let args = if scalar_isnull {
                [DatumV::null(), elt]
            } else {
                [scalar_value.clone(), elt]
            };
            let (result_v, isnull) =
                function_call_invoke_datum::call(mcx, fn_oid, collation, &args[..], &[], None)?;
            (isnull, result_v.as_bool())
        };

        // Combine results per OR or AND semantics.
        if this_isnull {
            resultnull = true;
        } else if use_or {
            if thisresult {
                result = true;
                resultnull = false;
                break; // needn't look at any more elements
            }
        } else if !thisresult {
            result = false;
            resultnull = false;
            break; // needn't look at any more elements
        }
    }

    // *op->resvalue = result; *op->resnull = resultnull;
    crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_bool(result), resultnull);
    Ok(())
}

/// `saop_element_hash(struct saophash_hash *tb, Datum key)` — the `SH_HASH_KEY`
/// callback: hash one array element via the SAOP's hash function.
///
/// Faithful to `execExprInterp.c:4176-4188`: the C loads `key` into the table's
/// 1-arg `hash_fcinfo_data`, dispatches `hash_finfo.fn_addr(fcinfo)`, and
/// returns `DatumGetUInt32`. The owned `FmgrInfo` carries only `fn_oid` (the F0
/// contract — see [`crate::justs`]), so the dispatch goes through the fmgr seam
/// `function_call1_coll_datum` (the canonical by-reference-capable lane), which
/// re-resolves by OID and lets a by-ref array element reach the hash function as
/// its full image. `hashfuncid` is `saop->hashfuncid`
/// (`tb->private_data->hash_finfo`); `collation` is `saop->inputcollid` (the
/// collation `InitFunctionCallInfoData` stamped onto `hash_fcinfo_data`).
pub fn saop_element_hash<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    hashfuncid: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    key: &DatumV<'mcx>,
) -> PgResult<u32> {
    // fcinfo->args[0].value = key; fcinfo->args[0].isnull = false;
    // hash = elements_tab->hash_finfo.fn_addr(fcinfo);
    // return DatumGetUInt32(hash);
    //
    // Dispatch on the canonical by-reference-capable lane: a by-ref array
    // element (text/numeric) reaches the hash function as its full image, not a
    // downgraded word (which would panic in `word_of`). The hash result is a
    // by-value int4.
    let hash =
        backend_utils_fmgr_fmgr_seams::function_call1_coll_datum::call(
            mcx, hashfuncid, collation, key.clone(),
        )?;
    Ok(hash.as_u32())
}

/// `saop_hash_element_match(struct saophash_hash *tb, Datum key1, Datum key2)`
/// — the `SH_EQUAL` callback: compare two elements via the SAOP's comparison
/// (equality) operator, dispatched on the canonical by-reference-capable lane.
///
/// Faithful to `execExprInterp.c:4194-4209`: the C loads `key1`/`key2` into the
/// step's 2-arg comparison `fcinfo_data`, dispatches `finfo->fn_addr(fcinfo)`,
/// and returns `DatumGetBool`. `matchfuncid` is the OID of
/// `op->d.hashedscalararrayop.finfo` (the equality function the compiler stamped
/// — `opfuncid` for hashed IN, `negfuncid` for hashed NOT IN); `collation` is
/// `saop->inputcollid`. The dispatch goes through the fmgr seam
/// `function_call2_coll_datum` (re-resolve by OID, canonical by-ref-capable
/// lane). Both keys are non-null here (hashtable build/probe never stores
/// NULLs), matching `FunctionCall2Coll`'s non-null-arg contract.
pub fn saop_hash_element_match<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    matchfuncid: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    key1: &DatumV<'mcx>,
    key2: &DatumV<'mcx>,
) -> PgResult<bool> {
    // fcinfo->args[0].value = key1; fcinfo->args[0].isnull = false;
    // fcinfo->args[1].value = key2; fcinfo->args[1].isnull = false;
    // result = elements_tab->op->d.hashedscalararrayop.finfo->fn_addr(fcinfo);
    // return DatumGetBool(result);
    //
    // Dispatch on the canonical by-reference-capable lane so by-ref array
    // elements (text/varchar/numeric) reach the equality function intact.
    let result =
        backend_utils_fmgr_fmgr_seams::function_call2_coll_datum::call(
            mcx, matchfuncid, collation, key1.clone(), key2.clone(),
        )?;
    Ok(result.as_bool())
}

/// `ExecEvalHashedScalarArrayOp(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — `x = ANY (array)` via a built hash table.
///
/// Faithful re-port of `execExprInterp.c:4225-4402`. On the first evaluation it
/// deconstructs the RHS array const (via the arrayfuncs seam, which subsumes the
/// C `DatumGetArrayTypeP`/`ArrayGetNItems`/`ARR_DATA_PTR`/`ARR_NULLBITMAP`/
/// `fetch_att` bitmap walk) and builds the [`crate::saophash`] table over the
/// non-NULL elements, recording `has_nulls`; thereafter it probes the table for
/// the scalar. The strict-NULL short circuit, the IN/NOT-IN result selection,
/// and the no-match-with-nulls (strict vs non-strict) branch are transcribed.
///
/// One sub-path mirror-PG-and-panics on a genuinely-missing seam capability: the
/// **non-strict, no-match-with-NULLs** branch dispatches the equality function
/// with `args[1].isnull = true` (a NULL rhs) and reads back `fcinfo->isnull`.
/// The `function_call2_coll` seam (C `FunctionCall2Coll`) only models non-null
/// args and asserts a non-null result, so this single branch needs the
/// fmgr-widened nullable-arg call frame and panics until that lands. Every other
/// path — the common one — is real own-logic + real seam `::call`s.
pub fn ExecEvalHashedScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = econtext;

    // ScalarArrayOpExprHashTable *elements_tab = op->d.hashedscalararrayop.elements_tab;
    // bool inclause = op->d.hashedscalararrayop.inclause;
    // bool strictfunc = op->d.hashedscalararrayop.finfo->fn_strict;
    // Read the per-step inputs (inclause / strictfunc), the comparison function
    // OID + collation, the hash function OID, and whether the table is built.
    let (inclause, strictfunc, matchfuncid, hashfuncid, collation, has_built) = {
        match step_data(state, op) {
            ExprEvalStepData::HashedScalarArrayOp {
                inclause,
                finfo,
                saop,
                elements_tab,
                ..
            } => {
                let finfo = finfo
                    .as_ref()
                    .expect("ExecEvalHashedScalarArrayOp: op->d.hashedscalararrayop.finfo not resolved");
                let saop = saop
                    .as_ref()
                    .expect("ExecEvalHashedScalarArrayOp: op->d.hashedscalararrayop.saop missing");
                (
                    *inclause,
                    finfo.fn_strict,
                    finfo.fn_oid,
                    saop.hashfuncid,
                    saop.inputcollid,
                    elements_tab.is_some(),
                )
            }
            other => unreachable!(
                "EEOP_HASHED_SCALARARRAYOP step carries the wrong payload: {other:?}"
            ),
        }
    };

    // The scalar arg the compiler recursed into fcinfo->args[0] is modeled as
    // the step's `scalar_cell` (see execExpr.c's hashed path):
    //   Datum scalar = fcinfo->args[0].value;
    //   bool  scalar_isnull = fcinfo->args[0].isnull;
    let scalar_cell_id = match step_data(state, op) {
        ExprEvalStepData::HashedScalarArrayOp { scalar_cell, .. } => *scalar_cell,
        _ => unreachable!(),
    };
    let (scalar_value, scalar_isnull) = crate::interp_loop::read_cell(state, scalar_cell_id);

    let (resvalue_id, resnull_id) = res_cells(state, op);

    // Assert(!*op->resnull);  -- we never set up a hashed SAOP on a NULL array const.

    // If the scalar is NULL and the function is strict, return NULL; no point
    // searching.
    //   if (fcinfo->args[0].isnull && strictfunc) { *op->resnull = true; return; }
    if scalar_isnull && strictfunc {
        crate::interp_loop::write_cell(state, resnull_id, DatumV::null(), true);
        return Ok(());
    }

    // Build the hash table on first evaluation.
    //   if (elements_tab == NULL) { ... }
    if !has_built {
        // saop = op->d.hashedscalararrayop.saop;
        // arr = DatumGetArrayTypeP(*op->resvalue);
        // nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
        // get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
        let (arraydatum_v, _) = crate::interp_loop::read_cell(state, resvalue_id);
        let mcx = estate.es_query_cxt;

        // Read the element type from the array's on-disk byte image (the
        // canonical by-ref value lane), not the empty bare-word surrogate.
        let elemtype = backend_utils_adt_arrayfuncs_seams::array_get_elemtype_bytes::call(
            mcx,
            arraydatum_v.as_ref_bytes(),
        )?;
        let tlba =
            backend_utils_cache_lsyscache_seams::get_typlenbyvalalign::call(elemtype)?;

        // Deconstruct the array into its per-element canonical (Datum, isnull)
        // pairs. The `_v` lane keeps by-reference elements (text/numeric) as
        // their full image so they survive into the hash table and the
        // fmgr-dispatched hash/equal callbacks. This seam subsumes C's
        // ARR_DATA_PTR + ARR_NULLBITMAP + fetch_att + att_addlength_pointer +
        // att_align_nominal bitmap walk over `nitems`.
        let elements = backend_utils_adt_arrayfuncs_seams::deconstruct_array_v::call(
            mcx,
            arraydatum_v,
            elemtype,
            tlba.typlen,
            tlba.typbyval,
            tlba.typalign as core::ffi::c_char,
        )?;
        let nitems = elements.len();

        // elements_tab = palloc0(...); op->d.hashedscalararrayop.elements_tab = elements_tab;
        // elements_tab->op = op;
        // fmgr_info(saop->hashfuncid, &elements_tab->hash_finfo);
        // InitFunctionCallInfoData(elements_tab->hash_fcinfo_data, ..., saop->inputcollid, ...);
        // elements_tab->hashtab = saophash_create(CurrentMemoryContext, nitems, elements_tab);
        let mut table = crate::saophash::ScalarArrayOpExprHashTable::default();
        table.hash_finfo.fn_oid = hashfuncid;
        table.hashtab = crate::saophash::saophash_create(nitems as u32);

        // Walk the elements: NULLs are not stored (record has_nulls); non-NULLs
        // are inserted. The closures are the SH_HASH_KEY / SH_EQUAL callbacks,
        // dispatching the hash / equality functions through the fmgr seams.
        let mut has_nulls = false;
        for (element, isnull) in elements.iter() {
            if *isnull {
                has_nulls = true;
            } else {
                let mut hash_key =
                    |k: &DatumV<'_>| saop_element_hash(mcx, hashfuncid, collation, k);
                let mut equal = |a: &DatumV<'_>, b: &DatumV<'_>| {
                    saop_hash_element_match(mcx, matchfuncid, collation, a, b)
                };
                crate::saophash::saophash_insert(
                    &mut table.hashtab,
                    element.clone(),
                    &mut hash_key,
                    &mut equal,
                )?;
            }
        }

        // Store the built table + has_nulls back into the step payload.
        match step_data_mut(state, op) {
            ExprEvalStepData::HashedScalarArrayOp {
                elements_tab,
                has_nulls: hn,
                ..
            } => {
                *elements_tab = Some(Box::new(table));
                *hn = has_nulls;
            }
            _ => unreachable!(),
        }
    }

    // Probe the hash table.
    //   hashfound = NULL != saophash_lookup(elements_tab->hashtab, scalar);
    let hashfound = {
        let mcx = estate.es_query_cxt;
        let mut hash_key = |k: &DatumV<'_>| saop_element_hash(mcx, hashfuncid, collation, k);
        let mut equal = |a: &DatumV<'_>, b: &DatumV<'_>| {
            saop_hash_element_match(mcx, matchfuncid, collation, a, b)
        };
        let table = match step_data(state, op) {
            ExprEvalStepData::HashedScalarArrayOp { elements_tab, .. } => elements_tab
                .as_ref()
                .expect("ExecEvalHashedScalarArrayOp: elements_tab just built"),
            _ => unreachable!(),
        };
        // The scalar key keeps its canonical (by-ref-capable) image through the probe.
        crate::saophash::saophash_lookup(&table.hashtab, &scalar_value, &mut hash_key, &mut equal)?
    };

    // result = inclause ? BoolGetDatum(hashfound) : BoolGetDatum(!hashfound);
    let mut result = if inclause { hashfound } else { !hashfound };
    let mut resultnull = false;

    // If no match, account for NULLs in the array.
    //   if (!hashfound && op->d.hashedscalararrayop.has_nulls) { ... }
    let has_nulls = match step_data(state, op) {
        ExprEvalStepData::HashedScalarArrayOp { has_nulls, .. } => *has_nulls,
        _ => unreachable!(),
    };
    if !hashfound && has_nulls {
        if strictfunc {
            // Nulls in the array + non-null lhs + no match => NULL.
            //   result = (Datum) 0; resultnull = true;
            result = false;
            resultnull = true;
        } else {
            // Execute the (non-strict) function once with a NULL rhs.
            //   fcinfo->args[0] = {scalar, scalar_isnull};
            //   fcinfo->args[1] = {(Datum)0, true};
            //   result = op->d.hashedscalararrayop.finfo->fn_addr(fcinfo);
            //   resultnull = fcinfo->isnull;
            //   if (!inclause) result = !result;
            //
            // The resolved FmgrInfo cannot cross the seam, so dispatch by
            // fn_oid through the canonical by-ref-capable lane (a by-ref scalar
            // reaches the function as its full image). args[1] is the NULL rhs.
            let mcx = estate.es_query_cxt;
            let args = if scalar_isnull {
                [DatumV::null(), DatumV::null()]
            } else {
                [scalar_value.clone(), DatumV::null()]
            };
            let (result_v, isnull) =
                function_call_invoke_datum::call(mcx, matchfuncid, collation, &args[..], &[], None)?;
            // result = DatumGetBool(...); resultnull = fcinfo->isnull;
            result = result_v.as_bool();
            resultnull = isnull;
            // Reverse the result for NOT IN clauses (the function is equality).
            if !inclause {
                result = !result;
            }
        }
    }

    // *op->resvalue = result; *op->resnull = resultnull;
    crate::interp_loop::write_cell(state, resvalue_id, DatumV::from_bool(result), resultnull);
    Ok(())
}

/// Borrow the `ExprEvalStepData` payload of step `op` in `state`. Mirrors the C
/// `&state->steps[op]->d` access; panics if the step program is not yet
/// installed (a caller/compile bug).
#[inline]
fn step_data<'a, 'mcx>(state: &'a ExprState<'mcx>, op: usize) -> &'a ExprEvalStepData<'mcx> {
    &state
        .steps
        .as_ref()
        .expect("eval_scalar: steps not ready")[op]
        .d
}

/// Mutably borrow the `ExprEvalStepData` payload of step `op` — the C
/// `&state->steps[op]->d` for the write-back of `elements_tab`/`has_nulls` in
/// `ExecEvalHashedScalarArrayOp`.
#[inline]
fn step_data_mut<'a, 'mcx>(
    state: &'a mut ExprState<'mcx>,
    op: usize,
) -> &'a mut ExprEvalStepData<'mcx> {
    &mut state
        .steps
        .as_mut()
        .expect("eval_scalar: steps not ready")[op]
        .d
}

/// Resolve the `(resvalue, resnull)` [`ResultCellId`] pair of step `op` — the
/// owned-model replacement for the C `op->resvalue` / `op->resnull` pointers.
/// The two ids name the value and is-null halves of one logical cell (they are
/// equal in the current model; both are returned so callers read like the C).
#[inline]
fn res_cells<'mcx>(
    state: &ExprState<'mcx>,
    op: usize,
) -> (
    types_nodes::execexpr::ResultCellId,
    types_nodes::execexpr::ResultCellId,
) {
    let step = &state
        .steps
        .as_ref()
        .expect("eval_scalar: steps not ready")[op];
    (step.resvalue, step.resnull)
}
