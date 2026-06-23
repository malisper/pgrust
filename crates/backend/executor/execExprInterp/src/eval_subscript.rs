//! `EEOP_SBSREF_*` interpreter steps — container (`SubscriptingRef`)
//! subscripting evaluation.
//!
//! The C interpreter dispatches these through the type-specific
//! `op->d.sbsref*.subscriptfunc` callback pointers
//! (`array_subscript_check_subscripts` / `_fetch` / `_assign` / `_fetch_old`,
//! arraysubs.c). Those raw `void (*)(ExprState *, ExprEvalStep *,
//! ExprContext *)` shapes cannot thread the owned `EStateData`/`Mcx`, so the
//! step carries a [`SubscriptMethod`] discriminant instead and this module
//! re-dispatches it with the EState threaded in (the array bodies live in the
//! arrayfuncs owner, reached through per-callback seams).
//!
//! # `sbs_check_subscripts`
//!
//! The integer-conversion of the just-evaluated subscript `Datum`s (the
//! `array_subscript_check_subscripts` body) is pure (no array dependency) and
//! is performed here. The container is in the step's result cell; each
//! subscript was evaluated into its own arena cell recorded in
//! `state.upper_cells[i]` / `lower_cells[i]`. The converted integers are passed
//! straight to the FETCH/ASSIGN seam (each consuming step re-converts from the
//! same arena cells, since in C all the SBSREF steps share one
//! `SubscriptingRefState`).

use ::types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use ::nodes::execexpr::{
    ExprEvalStepData, ExprState, ResultCellId, SubscriptMethod, SubscriptWorkspace,
};
use ::nodes::execnodes::EcxtId;
use ::nodes::EStateData;
use types_tuple::heaptuple::Datum;

use crate::interp_loop::{read_cell, write_cell};

/// The subscript inputs extracted from a SBSREF step's `SubscriptingRefState`,
/// owned (Copy-friendly) so the borrow of `state.steps[op]` can be released
/// before touching the result cells.
struct SbsInputs {
    isassignment: bool,
    numupper: i32,
    numlower: i32,
    // converted integer subscripts (filled by convert()).
    upperindex: Vec<i32>,
    lowerindex: Vec<i32>,
    upperprovided: Vec<bool>,
    lowerprovided: Vec<bool>,
    // arena cells the subscripts were evaluated into (parallel to provided).
    upper_cells: Vec<Option<ResultCellId>>,
    lower_cells: Vec<Option<ResultCellId>>,
    replace_cell: Option<ResultCellId>,
    prev_cell: Option<ResultCellId>,
    // ArraySubWorkspace type info.
    refelemtype: types_core::Oid,
    refattrlength: i16,
    refelemlength: i16,
    refelembyval: bool,
    refelemalign: u8,
}

/// Pull the SBSREF step's state into an owned [`SbsInputs`].
fn extract<'mcx>(state: &ExprState<'mcx>, op: usize) -> SbsInputs {
    let steps = state.steps.as_ref().expect("eval_subscript: steps not ready");
    let st = match &steps[op].d {
        ExprEvalStepData::SbsRefSubscript { state: Some(s), .. } => s,
        ExprEvalStepData::SbsRef { state: Some(s), .. } => s,
        other => unreachable!("eval_subscript: step.d carries no SubscriptingRefState: {other:?}"),
    };
    let ws = match &st.workspace {
        SubscriptWorkspace::Array(w) => *w,
        _ => panic!("eval_subscript: array subscript step has no ArraySubWorkspace"),
    };
    let clone_bools = |v: &Option<mcx::PgVec<'mcx, bool>>, n: i32| -> Vec<bool> {
        match v {
            Some(b) => b.iter().copied().collect(),
            None => vec![false; n.max(0) as usize],
        }
    };
    let clone_cells = |v: &Option<mcx::PgVec<'mcx, Option<ResultCellId>>>, n: i32|
     -> Vec<Option<ResultCellId>> {
        match v {
            Some(b) => b.iter().copied().collect(),
            None => vec![None; n.max(0) as usize],
        }
    };
    SbsInputs {
        isassignment: st.isassignment,
        numupper: st.numupper,
        numlower: st.numlower,
        upperindex: vec![0; st.numupper.max(0) as usize],
        lowerindex: vec![0; st.numlower.max(0) as usize],
        upperprovided: clone_bools(&st.upperprovided, st.numupper),
        lowerprovided: clone_bools(&st.lowerprovided, st.numlower),
        upper_cells: clone_cells(&st.upper_cells, st.numupper),
        lower_cells: clone_cells(&st.lower_cells, st.numlower),
        replace_cell: st.replace_cell,
        prev_cell: st.prev_cell,
        refelemtype: ws.refelemtype,
        refattrlength: ws.refattrlength,
        refelemlength: ws.refelemlength,
        refelembyval: ws.refelembyval,
        refelemalign: ws.refelemalign,
    }
}

/// `array_subscript_check_subscripts` (arraysubs.c): convert the subscript
/// `Datum`s in the arena cells into the integer `upperindex`/`lowerindex`
/// arrays. Returns `Ok(false)` (skip, result is NULL) on a NULL fetch subscript,
/// `Err` on a NULL assignment subscript, `Ok(true)` to proceed.
///
/// `op_resnull_id` is the step's result cell; on a NULL fetch subscript its
/// is-null is set true.
fn convert<'mcx>(
    state: &mut ExprState<'mcx>,
    inputs: &mut SbsInputs,
    op_res: ResultCellId,
) -> PgResult<bool> {
    // C: for (i = 0; i < numupper; i++) if (upperprovided[i]) { if null -> ... ;
    //        workspace->upperindex[i] = DatumGetInt32(upperindex[i]); }
    for i in 0..inputs.numupper as usize {
        if inputs.upperprovided[i] {
            let cell = inputs.upper_cells[i]
                .expect("eval_subscript: provided upper subscript has no arena cell");
            let (val, isnull) = read_cell(state, cell);
            if isnull {
                if inputs.isassignment {
                    return Err(PgError::error(
                        "array subscript in assignment must not be null",
                    )
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
                }
                // *op->resnull = true; return false;
                let (cur, _) = read_cell(state, op_res);
                write_cell(state, op_res, cur, true);
                return Ok(false);
            }
            inputs.upperindex[i] = val.as_i32();
        }
    }
    // C: likewise for lower subscripts.
    for i in 0..inputs.numlower as usize {
        if inputs.lowerprovided[i] {
            let cell = inputs.lower_cells[i]
                .expect("eval_subscript: provided lower subscript has no arena cell");
            let (val, isnull) = read_cell(state, cell);
            if isnull {
                if inputs.isassignment {
                    return Err(PgError::error(
                        "array subscript in assignment must not be null",
                    )
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
                }
                let (cur, _) = read_cell(state, op_res);
                write_cell(state, op_res, cur, true);
                return Ok(false);
            }
            inputs.lowerindex[i] = val.as_i32();
        }
    }
    Ok(true)
}

/// `EEOP_SBSREF_SUBSCRIPTS` —
/// `if (subscriptfunc(state, op, econtext)) EEO_NEXT(); else EEO_JUMP(jumpdone);`
/// Returns `true` to continue (EEO_NEXT) or `false` to jump to `jumpdone`.
pub fn exec_sbsref_subscripts<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
    method: SubscriptMethod,
    op_res: ResultCellId,
) -> PgResult<bool> {
    // jsonb has its own sbs_check_subscripts (jsonbsubs.c): it coerces INT4
    // subscripts to text and records the array-vs-object expectation.
    if matches!(method, SubscriptMethod::JsonbCheckSubscripts) {
        return jsonb::check_subscripts(state, op, econtext, estate, op_res);
    }
    debug_assert!(matches!(method, SubscriptMethod::ArrayCheckSubscripts));
    let mut inputs = extract(state, op);
    convert(state, &mut inputs, op_res)
}

/// `EEOP_SBSREF_OLD` / `EEOP_SBSREF_ASSIGN` / `EEOP_SBSREF_FETCH` —
/// `subscriptfunc(state, op, econtext);`
pub fn exec_sbsref<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
    method: SubscriptMethod,
    op_res: ResultCellId,
) -> PgResult<()> {
    // jsonb FETCH / ASSIGN / OLD-fetch (jsonbsubs.c) have their own bodies.
    match method {
        SubscriptMethod::JsonbFetch
        | SubscriptMethod::JsonbAssign
        | SubscriptMethod::JsonbFetchOld => {
            return jsonb::exec(state, op, econtext, estate, method, op_res);
        }
        _ => {}
    }
    let mut inputs = extract(state, op);

    // Re-convert the subscripts from the arena cells (in C the shared
    // SubscriptingRefState already holds the converted workspace from the
    // preceding SUBSCRIPTS step; the owned per-step copies re-derive it from
    // the same cells — identical values, since SUBSCRIPTS only proceeds here
    // when every subscript was non-null and convertible).
    if !convert(state, &mut inputs, op_res)? {
        // A NULL subscript on a non-strict fetch would have jumped before
        // reaching FETCH; for ASSIGN, convert() errors on null. So this is
        // unreachable on the assignment path and the fetch path is guarded by
        // the SUBSCRIPTS jump — but mirror C's "result is NULL" just in case.
        return Ok(());
    }

    // The container is in the step's result cell.
    let (container, container_null) = read_cell(state, op_res);

    // C allocates the result varlena in CurrentMemoryContext (the per-tuple
    // expression-eval context). The owned model carries results through the
    // per-query result-cell arena, so allocate in the per-query context (the
    // arena's lifetime `'mcx`); this is behavior-preserving — a longer-lived
    // result that the cell already owns.
    let mcx = estate.ecxt(econtext).ecxt_per_query_memory;

    use arrayfuncs_seams as arrayfuncs;
    match method {
        SubscriptMethod::ArrayCheckSubscripts => {
            unreachable!("ArrayCheckSubscripts dispatched through exec_sbsref")
        }
        SubscriptMethod::ArrayFetch => {
            // C: *op->resvalue = array_subscript_fetch(...); (sets *op->resnull)
            let (val, isnull) = arrayfuncs::array_subscript_fetch::call(
                mcx,
                container,
                inputs.numupper,
                &inputs.upperindex,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_cell(state, op_res, val, isnull);
        }
        SubscriptMethod::ArrayFetchSlice => {
            let (val, isnull) = arrayfuncs::array_subscript_fetch_slice::call(
                mcx,
                container,
                inputs.numupper,
                &inputs.upperindex,
                &inputs.lowerindex,
                &inputs.upperprovided,
                &inputs.lowerprovided,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_cell(state, op_res, val, isnull);
        }
        SubscriptMethod::ArrayAssign => {
            // Replacement value is in the arena cell recorded at compile time.
            let (rep, repnull) = read_replace(state, &inputs);
            let (val, isnull) = arrayfuncs::array_subscript_assign::call(
                mcx,
                container,
                container_null,
                inputs.numupper,
                &inputs.upperindex,
                rep,
                repnull,
                inputs.refelemtype,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_cell(state, op_res, val, isnull);
        }
        SubscriptMethod::ArrayAssignSlice => {
            let (rep, repnull) = read_replace(state, &inputs);
            let (val, isnull) = arrayfuncs::array_subscript_assign_slice::call(
                mcx,
                container,
                container_null,
                inputs.numupper,
                &inputs.upperindex,
                &inputs.lowerindex,
                &inputs.upperprovided,
                &inputs.lowerprovided,
                rep,
                repnull,
                inputs.refelemtype,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_cell(state, op_res, val, isnull);
        }
        SubscriptMethod::ArrayFetchOld => {
            // OLD result goes to prevvalue/prevnull, aliased to prev_cell; it
            // must NOT overwrite *op->resvalue.
            let (val, isnull) = arrayfuncs::array_subscript_fetch_old::call(
                mcx,
                container,
                container_null,
                inputs.numupper,
                &inputs.upperindex,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_prev(state, &inputs, val, isnull);
        }
        SubscriptMethod::ArrayFetchOldSlice => {
            let (val, isnull) = arrayfuncs::array_subscript_fetch_old_slice::call(
                mcx,
                container,
                container_null,
                inputs.numupper,
                &inputs.upperindex,
                &inputs.lowerindex,
                &inputs.upperprovided,
                &inputs.lowerprovided,
                inputs.refattrlength,
                inputs.refelemlength,
                inputs.refelembyval,
                inputs.refelemalign,
            )?;
            write_prev(state, &inputs, val, isnull);
        }
        // jsonb methods are handled by jsonb::exec via the early return above.
        SubscriptMethod::JsonbFetch
        | SubscriptMethod::JsonbAssign
        | SubscriptMethod::JsonbFetchOld
        | SubscriptMethod::JsonbCheckSubscripts => {
            unreachable!("jsonb subscript method dispatched through the array exec_sbsref body")
        }
    }
    Ok(())
}

/// Read the replacement value/null from the arena cell recorded at compile.
fn read_replace<'mcx>(state: &ExprState<'mcx>, inputs: &SbsInputs) -> (Datum<'mcx>, bool) {
    match inputs.replace_cell {
        Some(c) => read_cell(state, c),
        // A plain (non-nested) assignment still has a replacement value cell;
        // it is always set by ExecInitSubscriptingRef. A missing cell is a
        // programming error.
        None => panic!("eval_subscript: assignment step has no replacement-value cell"),
    }
}

/// Write the OLD-fetch result into the `prev_cell` arena alias
/// (`sbsrefstate->prevvalue`/`prevnull`).
fn write_prev<'mcx>(state: &mut ExprState<'mcx>, inputs: &SbsInputs, val: Datum<'mcx>, isnull: bool) {
    if let Some(c) = inputs.prev_cell {
        write_cell(state, c, val, isnull);
    }
    let _ = inputs.isassignment;
}

/// jsonb subscripting execution (jsonbsubs.c). The jsonb `sbs_check_subscripts`
/// coerces INT4 subscripts to a text path element (the C
/// `int4out`/`CStringGetTextDatum` round-trip) and records the array-vs-object
/// expectation; the FETCH/ASSIGN/OLD bodies live in the jsonbsubs owner and
/// take the source container plus that text path (one `VARDATA_ANY` payload per
/// subscript), reached through `backend-utils-adt-jsonbsubs-seams`.
mod jsonb {
    use super::{read_cell, write_cell};
    use ::types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};
    use ::nodes::execexpr::{
        ExprEvalStepData, ExprState, ResultCellId, SubscriptMethod, SubscriptWorkspace,
    };
    use ::nodes::execnodes::EcxtId;
    use ::nodes::EStateData;

    /// `INT4OID` (`catalog/pg_type.dat`).
    const INT4OID: types_core::Oid = 23;

    /// Owned snapshot of a jsonb SBSREF step's `SubscriptingRefState` + jsonb
    /// workspace, with the subscript arena cells (parallel to `index_oid`).
    struct JsonbInputs {
        isassignment: bool,
        numupper: i32,
        upperprovided: Vec<bool>,
        upper_cells: Vec<Option<ResultCellId>>,
        index_oid: Vec<types_core::Oid>,
        replace_cell: Option<ResultCellId>,
        prev_cell: Option<ResultCellId>,
    }

    fn extract<'mcx>(state: &ExprState<'mcx>, op: usize) -> JsonbInputs {
        let steps = state.steps.as_ref().expect("eval_subscript(jsonb): steps not ready");
        let st = match &steps[op].d {
            ExprEvalStepData::SbsRefSubscript { state: Some(s), .. } => s,
            ExprEvalStepData::SbsRef { state: Some(s), .. } => s,
            other => unreachable!(
                "eval_subscript(jsonb): step.d carries no SubscriptingRefState: {other:?}"
            ),
        };
        let ws = match &st.workspace {
            SubscriptWorkspace::Jsonb(w) => w,
            _ => panic!("eval_subscript(jsonb): jsonb subscript step has no JsonbSubWorkspace"),
        };
        let n = st.numupper.max(0) as usize;
        let upperprovided = match &st.upperprovided {
            Some(b) => b.iter().copied().collect(),
            None => vec![false; n],
        };
        let upper_cells = match &st.upper_cells {
            Some(b) => b.iter().copied().collect(),
            None => vec![None; n],
        };
        JsonbInputs {
            isassignment: st.isassignment,
            numupper: st.numupper,
            upperprovided,
            upper_cells,
            index_oid: ws.index_oid.clone(),
            replace_cell: st.replace_cell,
            prev_cell: st.prev_cell,
        }
    }

    /// `jsonb_subscript_check_subscripts` (jsonbsubs.c). Builds the text path
    /// (one `VARDATA_ANY` payload per subscript) and records `expectArray`.
    /// Returns `Ok(None)` to jump-done (a NULL fetch subscript), `Err` for a
    /// NULL assignment subscript, `Ok(Some(path))` to proceed.
    fn build_path<'mcx>(
        state: &mut ExprState<'mcx>,
        inputs: &JsonbInputs,
        op: usize,
        econtext: EcxtId,
        estate: &mut EStateData<'mcx>,
        op_res: ResultCellId,
    ) -> PgResult<Option<Vec<Vec<u8>>>> {
        let mcx = estate.ecxt(econtext).ecxt_per_query_memory;

        // C: if (numupper > 0 && upperprovided[0] && !upperindexnull[0] &&
        //        indexOid[0] == INT4OID) workspace->expectArray = true;
        let mut expect_array = false;
        if inputs.numupper > 0
            && inputs.upperprovided[0]
            && inputs.index_oid.first().copied() == Some(INT4OID)
        {
            // !upperindexnull[0] is implied below: a NULL first subscript makes
            // build fail (fetch jumps / assign errors) before this matters; we
            // only set expectArray when the first subscript is a non-null INT4.
            let cell = inputs.upper_cells[0]
                .expect("eval_subscript(jsonb): provided subscript has no arena cell");
            let (_v, isnull) = read_cell(state, cell);
            if !isnull {
                expect_array = true;
            }
        }

        let mut path: Vec<Vec<u8>> =
            Vec::with_capacity(inputs.numupper.max(0) as usize);

        for i in 0..inputs.numupper as usize {
            // jsonb has no slices: every upper subscript is provided.
            debug_assert!(inputs.upperprovided[i]);
            let cell = inputs.upper_cells[i]
                .expect("eval_subscript(jsonb): provided subscript has no arena cell");
            let (val, isnull) = read_cell(state, cell);
            if isnull {
                // C: if (isassignment) ereport(NULL_VALUE_NOT_ALLOWED);
                //    else { *op->resnull = true; return false; }
                if inputs.isassignment {
                    return Err(PgError::error(
                        "jsonb subscript in assignment must not be null",
                    )
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
                }
                let (cur, _) = read_cell(state, op_res);
                write_cell(state, op_res, cur, true);
                return Ok(None);
            }

            // C: if (indexOid[i] == INT4OID)
            //        workspace->index[i] = CStringGetTextDatum(int4out(datum));
            //    else
            //        workspace->index[i] = sbsrefstate->upperindex[i];
            // The element is later read as a cstring (TextDatumGetCString /
            // VARDATA_ANY) by jsonb_get_element/jsonb_set_element, so we record
            // the path element as that cstring payload directly.
            let payload: Vec<u8> = if inputs.index_oid[i] == INT4OID {
                // int4out(datum): the canonical decimal rendering of the int4.
                format!("{}", val.as_i32()).into_bytes()
            } else {
                // TextDatumGetCString(workspace->index[i]) == VARDATA_ANY payload.
                varlena_seams::text_to_cstring_v::call(mcx, &val)?
                    .as_str()
                    .as_bytes()
                    .to_vec()
            };
            path.push(payload);
        }

        // Persist expectArray back into the workspace (read by ASSIGN).
        {
            let steps = state.steps.as_mut().expect("eval_subscript(jsonb): steps not ready");
            let st = match &mut steps[op].d {
                ExprEvalStepData::SbsRefSubscript { state: Some(s), .. } => s,
                ExprEvalStepData::SbsRef { state: Some(s), .. } => s,
                _ => unreachable!(),
            };
            st.workspace.jsonb_mut().expect_array = expect_array;
        }

        Ok(Some(path))
    }

    /// `EEOP_SBSREF_SUBSCRIPTS` for jsonb. Returns `true` to continue (EEO_NEXT)
    /// or `false` to jump to `jumpdone`.
    pub fn check_subscripts<'mcx>(
        state: &mut ExprState<'mcx>,
        op: usize,
        econtext: EcxtId,
        estate: &mut EStateData<'mcx>,
        op_res: ResultCellId,
    ) -> PgResult<bool> {
        let inputs = extract(state, op);
        Ok(build_path(state, &inputs, op, econtext, estate, op_res)?.is_some())
    }

    /// `EEOP_SBSREF_FETCH` / `_ASSIGN` / `_OLD` for jsonb.
    pub fn exec<'mcx>(
        state: &mut ExprState<'mcx>,
        op: usize,
        econtext: EcxtId,
        estate: &mut EStateData<'mcx>,
        method: SubscriptMethod,
        op_res: ResultCellId,
    ) -> PgResult<()> {
        let inputs = extract(state, op);
        // Re-derive the text path from the same arena cells (in C the shared
        // SubscriptingRefState already holds workspace->index from the preceding
        // SUBSCRIPTS step). A NULL subscript would have jumped before FETCH and
        // errored on ASSIGN, so build_path here yields Some on the live paths.
        let path = match build_path(state, &inputs, op, econtext, estate, op_res)? {
            Some(p) => p,
            None => return Ok(()),
        };

        let mcx = estate.ecxt(econtext).ecxt_per_query_memory;
        let (container, container_null) = read_cell(state, op_res);

        use jsonbsubs_seams as jsonbsubs;
        match method {
            SubscriptMethod::JsonbFetch => {
                let (val, isnull) = jsonbsubs::jsonb_subscript_fetch::call(mcx, container, &path)?;
                write_cell(state, op_res, val, isnull);
            }
            SubscriptMethod::JsonbAssign => {
                let expect_array = inputs_expect_array(state, op);
                let (rep, repnull) = read_replace(state, &inputs);
                let (val, isnull) = jsonbsubs::jsonb_subscript_assign::call(
                    mcx,
                    container,
                    container_null,
                    &path,
                    rep,
                    repnull,
                    expect_array,
                )?;
                write_cell(state, op_res, val, isnull);
            }
            SubscriptMethod::JsonbFetchOld => {
                let (val, isnull) = jsonbsubs::jsonb_subscript_fetch_old::call(
                    mcx,
                    container,
                    container_null,
                    &path,
                )?;
                write_prev(state, &inputs, val, isnull);
            }
            _ => unreachable!("jsonb::exec dispatched a non-jsonb method"),
        }
        Ok(())
    }

    /// Read the persisted `expectArray` from the jsonb workspace.
    fn inputs_expect_array<'mcx>(state: &ExprState<'mcx>, op: usize) -> bool {
        let steps = state.steps.as_ref().expect("eval_subscript(jsonb): steps not ready");
        let st = match &steps[op].d {
            ExprEvalStepData::SbsRefSubscript { state: Some(s), .. } => s,
            ExprEvalStepData::SbsRef { state: Some(s), .. } => s,
            _ => unreachable!(),
        };
        st.workspace.jsonb().expect_array
    }

    /// Read the replacement value/null from the arena cell recorded at compile.
    fn read_replace<'mcx>(
        state: &ExprState<'mcx>,
        inputs: &JsonbInputs,
    ) -> (super::Datum<'mcx>, bool) {
        match inputs.replace_cell {
            Some(c) => read_cell(state, c),
            None => panic!("eval_subscript(jsonb): assignment step has no replacement-value cell"),
        }
    }

    /// `jsonb_subscript_fetch_old` writes its result into the `prev_cell` arena
    /// alias (`sbsrefstate->prevvalue`/`prevnull`).
    fn write_prev<'mcx>(
        state: &mut ExprState<'mcx>,
        inputs: &JsonbInputs,
        val: super::Datum<'mcx>,
        isnull: bool,
    ) {
        if let Some(c) = inputs.prev_cell {
            write_cell(state, c, val, isnull);
        }
    }
}
