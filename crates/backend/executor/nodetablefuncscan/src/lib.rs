// nodeTableFuncscan.c over adt_xml's XmlTableContext (XMLTABLE) and
// adt_jsonpath_exec's JsonTableExecContext (JSON_TABLE).
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;
use core::alloc::Layout;
use core::ffi::CStr;
use core::ptr::NonNull;

use ::adt_jsonpath_exec::json_table::JsonTableExecContext;
use ::adt_jsonpath_exec::JsonPathVariable;
use ::adt_xml::xmltable::XmlTableContext;
use ::datum::{Datum, NullableDatum};
use ::execexpr::{
    exec_eval_expr, exec_init_expr_subplans, exec_init_expr_with_case_test_subplans,
    EvalSlots, ExprState,
};
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Allocator, Mcx, MemoryContext, PgBox, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use ::types_fmgr::{input_function_call, FmgrInfo};
use ::types_nodes::plannodes::TableFuncScan;
use ::types_nodes::primnodes::{TableFunc, TableFuncType};
use ::types_slot::TupleSlotKind;
use ::types_tuple::{varatt, TupleDescData};
use ::tuplestore::Tuplestore;

pub fn init_seams() {}

pub struct TableFuncScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    tf: &'mcx TableFunc<'mcx>,
    docexpr: PgBox<'mcx, ExprState<'mcx>>,
    rowexpr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    ns_uris: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    colexprs: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    coldefexprs: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    colvalexprs: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    passingvalexprs: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    in_functions: PgVec<'mcx, FmgrInfo>,
    typioparams: PgVec<'mcx, Oid>,
    tupdesc: Rc<TupleDescData<'mcx>>,
    tstore: Option<Tuplestore>,
    ordinal: i32,
    cstr_scratch: PgVec<'mcx, u8>,
    // C perTableCxt ("TableFunc per value context", nodeTableFuncscan.c:170):
    // per-one-call (one result table) lifetime data — the JSON_TABLE exec
    // context, PASSING values, detoasted documents — reset at the end of
    // every tfuncFetchRows (line 330), so a LATERAL join over many outer
    // rows keeps flat memory instead of leaking every call into
    // es_query_cxt. Arena-slot + estate reset-callback ownership, the
    // nodefunctionscan make_arg_ctx idiom.
    per_table_mcx: NonNull<MemoryContext>,
}

// C perTableCxt lifetime (AllocSetContextCreate under es_query_cxt, freed by
// FreeExecutorState's recursive delete): the context VALUE lives in the
// estate arena at a stable address and the estate context's reset callback
// is the reclaim point (fires exactly once, before the arena bytes go).
fn make_per_table_ctx(mcx: Mcx<'_>) -> PgResult<NonNull<MemoryContext>> {
    let layout = Layout::new::<MemoryContext>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<MemoryContext> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(mcx.context().new_child_bump("TableFunc per value context")) };
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

impl<'mcx> ScanNode<'mcx> for TableFuncScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    // C TableFuncRecheck: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        if self.tstore.is_none() {
            self.fetch_rows(estate)?;
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        self.tstore.as_mut().unwrap().gettupleslot(true, false, slot, mcx)
    }
}

pub fn exec_table_func_scan<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C ExecScan reads es_epq_active per call (see nodefunctionscan).
    if estate.es_epq_active {
        return exec_scan_epq(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `ExecInitTableFuncScan`.
pub fn exec_init_table_func_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &TableFuncScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<TableFuncScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());
    let tf = node
        .tablefunc
        .and_then(|n| n.as_table_func())
        .expect("TableFuncScan has a TableFunc");

    let mut names: PgVec<'_, &str> = PgVec::new_in(mcx);
    for n in &tf.colnames {
        names.push(n.as_string().expect("colnames cell is String").sval);
    }
    let tupdesc = tupdesc::BuildDescFromLists(
        mcx,
        &names,
        tf.coltypes.as_slice(),
        tf.coltypmods.as_slice(),
        tf.colcollations.as_slice(),
    )?;

    let natts = tupdesc.natts as usize;
    let mut in_functions: PgVec<'_, FmgrInfo> = PgVec::new_in(mcx);
    let mut typioparams: PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, natts)?;
    for i in 0..natts {
        let (in_funcid, typioparam) = lsyscache::getTypeInputInfo(tupdesc.attr(i).atttypid)?;
        in_functions.push(fmgr_core::fmgr_info(in_funcid)?);
        typioparams.push(typioparam);
    }

    let tupdesc = Rc::new(tupdesc);
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(tupdesc.clone()), TupleSlotKind::MinimalTuple);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    // C ExecInitTableFuncScan (nodeTableFuncscan.c:177-190) compiles every
    // TableFunc expression with (PlanState *) scanstate as the parent, so a
    // SubPlan or initplan Param inside a document / row / namespace / column
    // / DEFAULT / PASSING expression (LATERAL XMLTABLE ... DEFAULT (SELECT
    // t.id), JSON_TABLE((SELECT ...), ...)) attaches to this node.
    let pb = estate.param_bind();
    let mut ns_uris: PgVec<'_, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
    let mut colexprs: PgVec<'_, Option<PgBox<'mcx, ExprState<'mcx>>>> = PgVec::new_in(mcx);
    let mut coldefexprs: PgVec<'_, Option<PgBox<'mcx, ExprState<'mcx>>>> = PgVec::new_in(mcx);
    // NULL cells are FOR ORDINALITY columns.
    let mut colvalexprs: PgVec<'_, Option<PgBox<'mcx, ExprState<'mcx>>>> = PgVec::new_in(mcx);
    let mut passingvalexprs: PgVec<'_, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
    let (docexpr, rowexpr) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let docexpr = exec_init_expr_subplans(mcx, tf.docexpr, pb, env)?
                .expect("non-NULL expression");
            let rowexpr = exec_init_expr_subplans(mcx, tf.rowexpr, pb, env)?;
            for e in &tf.ns_uris {
                ns_uris.push(
                    exec_init_expr_subplans(mcx, Some(e), pb, env)?
                        .expect("non-NULL expression"),
                );
            }
            for e in tf.colexprs.iter() {
                colexprs.push(exec_init_expr_subplans(mcx, e, pb, env)?);
            }
            for e in tf.coldefexprs.iter() {
                coldefexprs.push(exec_init_expr_subplans(mcx, e, pb, env)?);
            }
            for e in tf.colvalexprs.iter() {
                colvalexprs.push(exec_init_expr_with_case_test_subplans(mcx, e, pb, env)?);
            }
            for e in tf.passingvalexprs.iter() {
                passingvalexprs.push(
                    exec_init_expr_subplans(mcx, Some(e), pb, env)?
                        .expect("non-NULL expression"),
                );
            }
            Ok((docexpr, rowexpr))
        })?;

    Ok(TableFuncScanState {
        ss,
        tf,
        docexpr,
        rowexpr,
        ns_uris,
        colexprs,
        coldefexprs,
        colvalexprs,
        passingvalexprs,
        in_functions,
        typioparams,
        tupdesc,
        tstore: None,
        ordinal: 0,
        cstr_scratch: PgVec::new_in(mcx),
        per_table_mcx: make_per_table_ctx(mcx)?,
    })
}

/// C ExecEvalExpr on a TableFunc expression compiled under this node: a
/// state carrying a SubPlan step or a pending-initplan PARAM_EXEC fetch
/// rides the executils suspension driver (nodefunctionscan precedent); the
/// plain kernel serves the rest. The result mcx is armed by the caller.
fn eval_armed<'mcx>(
    expr: &mut ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<NullableDatum> {
    if expr.has_subplan() || !expr.param_exec_deps().is_empty() {
        ::executils::exec_eval_expr_with_subplans(expr, estate, ecxt)
    } else {
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        exec_eval_expr(expr, &mut slots)
    }
}

impl<'mcx> TableFuncScanState<'mcx> {
    /// `tfuncFetchRows`: build the whole result into a tuplestore. C's
    /// PG_CATCH DestroyOpaque becomes the unconditional `destroy` below.
    fn fetch_rows(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let ecxt = self.ss.ps_ExprContext;
        let work_mem = init_small::globals::work_mem();
        let mut store = Tuplestore::begin_heap(false, false, work_mem);

        // C MemoryContextSwitchTo(tstate->perTableCxt) (line 290): the
        // previous call's error path left its data behind, as C leaves it
        // to the next reset.
        self.per_table_reset();
        if self.tf.functype == TableFuncType::TFT_JSON_TABLE {
            self.fetch_rows_json(&mut store, estate, ecxt)?;
        } else {
            let mut ctx = XmlTableContext::new(self.tupdesc.natts)?;
            let r = self.fetch_rows_guts(&mut ctx, &mut store, estate, ecxt);
            ctx.destroy();
            r?;
        }
        // C MemoryContextReset(tstate->perTableCxt) (line 330): every row is
        // in the tuplestore by now.
        self.per_table_reset();

        store.rescan()?;
        self.tstore = Some(store);
        Ok(())
    }

    fn per_table_reset(&mut self) {
        // SAFETY: arena slot armed by make_per_table_ctx; exclusive during
        // the scan (dropped only by the estate reset callback).
        unsafe { self.per_table_mcx.as_mut() }.reset();
    }

    /// The per-table context as an `'mcx`-stamped handle for the
    /// scan-lifetime containers it feeds (JsonTableExecContext, PASSING
    /// values, detoasted documents).
    fn per_table(&self) -> Mcx<'mcx> {
        // SAFETY: the context value lives in the estate arena for the whole
        // plan ('mcx); everything allocated through this handle is consumed
        // (copied into the tuplestore / libxml) before the next reset.
        unsafe { self.per_table_mcx.as_ref().mcx() }
    }

    // tfuncFetchRows/Initialize/LoadRows, JSON_TABLE shape: PASSING args are
    // evaluated once at InitOpaque time; a NULL document is an empty table;
    // column values come from the JsonExpr colvalexprs fed the row pattern via
    // CaseTestExpr — no input-function conversion, no coldefexprs.
    fn fetch_rows_json(
        &mut self,
        store: &mut Tuplestore,
        estate: &mut EStateData<'mcx>,
        ecxt: EcxtId,
    ) -> PgResult<()> {
        let mcx = self.per_table();
        let je = self
            .tf
            .docexpr
            .and_then(|n| n.as_json_expr())
            .expect("JSON_TABLE docexpr is JsonExpr");
        debug_assert_eq!(self.passingvalexprs.len(), je.passing_names.len());
        let mut args: PgVec<'mcx, JsonPathVariable<'mcx>> = PgVec::new_in(mcx);
        for i in 0..self.passingvalexprs.len() {
            let name = je
                .passing_names
                .nth(i)
                .as_string()
                .expect("passing_names cell is String")
                .sval;
            let src = self.tf.passingvalexprs.nth(i);
            let expr = &mut self.passingvalexprs[i];
            // PASSING values are read on every row-pattern reset; results go
            // to the per-table context (C: perTableCxt).
            expr.arm_result_mcx(mcx);
            let NullableDatum { value, isnull } = eval_armed(expr, estate, ecxt)?;
            args.push(JsonPathVariable {
                name: name.as_bytes(),
                typid: ::nodes_core::node_funcs::expr_type(src),
                typmod: ::nodes_core::node_funcs::expr_typmod(src),
                value,
                isnull,
            });
        }
        let plan = self.tf.plan.expect("JSON_TABLE TableFunc.plan");
        let mut jt =
            JsonTableExecContext::init(mcx, plan, args, self.tf.colvalexprs.len())?;

        let NullableDatum { value: doc, isnull } = self.eval(&EvalPick::Doc, estate, ecxt)?;
        if isnull {
            return Ok(());
        }
        jt.set_document(varlena_payload(mcx, doc)?)?;

        let natts = self.tupdesc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), natts);
        let mut nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, true, natts);
        while jt.fetch_row()? {
            // C tfuncLoadRows CHECK_FOR_INTERRUPTS() (nodeTableFuncscan.c:465).
            ::postgres_seams::check_for_interrupts::call()?;
            for colno in 0..natts {
                let (img, ordinal) = jt.current_row(colno);
                let (value, isnull) = match img {
                    None => (Datum::null(), true),
                    Some(img) => match self.colvalexprs[colno].as_mut() {
                        Some(expr) => {
                            expr.set_case_test(NullableDatum {
                                value: Datum::from_usize(img.as_ptr() as usize),
                                isnull: false,
                            });
                            // SAFETY: the per-tuple context outlives this
                            // evaluation; results are copied into the
                            // tuplestore before its reset.
                            unsafe {
                                expr.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx())
                            };
                            let nd = eval_armed(expr, estate, ecxt)?;
                            (nd.value, nd.isnull)
                        }
                        None => (Datum::from_i32(ordinal), false),
                    },
                };
                if isnull && self.tf.notnulls.is_member(colno as i32) {
                    return Err(PgError::error(format!(
                        "null is not allowed in column \"{}\"",
                        String::from_utf8_lossy(self.tupdesc.attr(colno).attname.name_str())
                    ))
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                    .into());
                }
                values[colno] = value;
                nulls[colno] = isnull;
            }
            store.putvalues(&self.tupdesc, &values, &nulls)?;
            estate.ecxt_mut(ecxt).reset();
        }
        Ok(())
    }

    fn fetch_rows_guts(
        &mut self,
        ctx: &mut XmlTableContext,
        store: &mut Tuplestore,
        estate: &mut EStateData<'mcx>,
        ecxt: EcxtId,
    ) -> PgResult<()> {
        let NullableDatum { value: doc, isnull } = self.eval(&EvalPick::Doc, estate, ecxt)?;
        if isnull {
            return Ok(());
        }
        self.initialize(ctx, doc, estate, ecxt)?;
        self.ordinal = 1;
        self.load_rows(ctx, store, estate, ecxt)
    }

    /// `tfuncInitialize`.
    fn initialize(
        &mut self,
        ctx: &mut XmlTableContext,
        doc: Datum,
        estate: &mut EStateData<'mcx>,
        ecxt: EcxtId,
    ) -> PgResult<()> {
        let mcx = self.per_table();
        ctx.set_document(varlena_payload(mcx, doc)?)?;

        debug_assert_eq!(self.ns_uris.len(), self.tf.ns_names.len());
        for i in 0..self.ns_uris.len() {
            let NullableDatum { value, isnull } = self.eval(&EvalPick::NsUri(i), estate, ecxt)?;
            if isnull {
                return Err(PgError::error("namespace URI must not be null")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                    .into());
            }
            let ns_name = self.tf.ns_names.as_slice()[i]
                .map(|n| n.as_string().expect("ns_names cell is String").sval);
            let uri = varlena_payload(mcx, value)?;
            ctx.set_namespace(ns_name.map(str::as_bytes), uri)?;
        }

        let NullableDatum { value, isnull } = self.eval(&EvalPick::Row, estate, ecxt)?;
        if isnull {
            return Err(PgError::error("row filter expression must not be null")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .into());
        }
        ctx.set_row_filter(varlena_payload(mcx, value)?)?;

        let ordinalitycol = self.tf.ordinalitycol;
        for colno in 0..self.colexprs.len() {
            if colno as i32 == ordinalitycol {
                continue;
            }
            if self.colexprs[colno].is_some() {
                let NullableDatum { value, isnull } =
                    self.eval(&EvalPick::Col(colno), estate, ecxt)?;
                if isnull {
                    return Err(PgError::error("column filter expression must not be null")
                        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                        .with_detail(format!(
                            "Filter for column \"{}\" is null.",
                            String::from_utf8_lossy(self.tupdesc.attr(colno).attname.name_str())
                        ))
                        .into());
                }
                ctx.set_column_filter(varlena_payload(mcx, value)?, colno as i32)?;
            } else {
                let attname = self.tupdesc.attr(colno).attname.name_str();
                ctx.set_column_filter(attname, colno as i32)?;
            }
        }
        Ok(())
    }

    /// `tfuncLoadRows`.
    fn load_rows(
        &mut self,
        ctx: &mut XmlTableContext,
        store: &mut Tuplestore,
        estate: &mut EStateData<'mcx>,
        ecxt: EcxtId,
    ) -> PgResult<()> {
        let natts = self.tupdesc.natts as usize;
        let mcx = self.per_table();
        let mut values: PgVec<'_, Datum> = mcx::vec_from_elem_in(mcx, Datum::null(), natts);
        let mut nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, true, natts);
        let ordinalitycol = self.tf.ordinalitycol;

        while ctx.fetch_row()? {
            // C tfuncLoadRows CHECK_FOR_INTERRUPTS() (nodeTableFuncscan.c:465).
            ::postgres_seams::check_for_interrupts::call()?;
            for colno in 0..natts {
                if colno as i32 == ordinalitycol {
                    values[colno] = Datum::from_i32(self.ordinal);
                    self.ordinal += 1;
                    nulls[colno] = false;
                    continue;
                }
                let att_typid = self.tupdesc.attr(colno).atttypid;
                let att_typmod = self.tupdesc.attr(colno).atttypmod;
                let text = ctx.get_value(colno as i32, att_typid)?;
                let mut isnull = text.is_none();
                if let Some(bytes) = text {
                    self.cstr_scratch.clear();
                    ::mcx::vec_append_bytes(&mut self.cstr_scratch, &bytes)?;
                    self.cstr_scratch.push(0);
                    let cs = CStr::from_bytes_with_nul(&self.cstr_scratch)
                        .expect("libxml strings carry no interior NUL");
                    let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
                    values[colno] = input_function_call(
                        &mut self.in_functions[colno],
                        Some(cs),
                        self.typioparams[colno],
                        att_typmod,
                        per_tuple,
                    )?;
                } else if self.coldefexprs[colno].is_some() {
                    let NullableDatum { value, isnull: dnull } =
                        self.eval(&EvalPick::Def(colno), estate, ecxt)?;
                    values[colno] = value;
                    isnull = dnull;
                }
                if isnull && self.tf.notnulls.is_member(colno as i32) {
                    return Err(PgError::error(format!(
                        "null is not allowed in column \"{}\"",
                        String::from_utf8_lossy(self.tupdesc.attr(colno).attname.name_str())
                    ))
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                    .into());
                }
                nulls[colno] = isnull;
            }
            store.putvalues(&self.tupdesc, &values, &nulls)?;
            estate.ecxt_mut(ecxt).reset();
        }
        Ok(())
    }

    fn eval(
        &mut self,
        pick: &EvalPick,
        estate: &mut EStateData<'mcx>,
        ecxt: EcxtId,
    ) -> PgResult<NullableDatum> {
        let per_table = self.per_table();
        let expr = match *pick {
            EvalPick::Doc => &mut self.docexpr,
            EvalPick::Row => self.rowexpr.as_mut().expect("XMLTABLE row filter expr"),
            EvalPick::NsUri(i) => &mut self.ns_uris[i],
            EvalPick::Col(i) => self.colexprs[i].as_mut().expect("column filter expr"),
            EvalPick::Def(i) => self.coldefexprs[i].as_mut().expect("column default expr"),
        };
        if let EvalPick::Def(_) = *pick {
            // C tfuncLoadRows evaluates DEFAULT under ecxt_per_tuple_memory
            // (line 456); the value is copied into the tuplestore before the
            // reset.
            // SAFETY: the per-tuple context outlives this evaluation.
            unsafe { expr.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
        } else {
            // C tfuncFetchRows / tfuncInitialize run under perTableCxt
            // (line 290): document, namespace, row and column filters.
            expr.arm_result_mcx(per_table);
        }
        eval_armed(expr, estate, ecxt)
    }
}

enum EvalPick {
    Doc,
    Row,
    NsUri(usize),
    Col(usize),
    Def(usize),
}

// pg_detoast_datum_packed + VARDATA_ANY (the nodememoize precedent).
fn varlena_payload<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: by-ref varlena datum readable through its header.
    unsafe {
        let flat = if varatt::varatt_is_1b_e(p)
            || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p))
        {
            let image = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            detoast_seams::detoast_attr::call(mcx, image)?.leak().as_ptr()
        } else {
            p
        };
        if varatt::varatt_is_1b(flat) {
            Ok(core::slice::from_raw_parts(
                flat.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(flat) - varatt::VARHDRSZ_SHORT,
            ))
        } else {
            Ok(core::slice::from_raw_parts(
                flat.add(varatt::VARHDRSZ),
                varatt::varsize_4b(flat) - varatt::VARHDRSZ,
            ))
        }
    }
}

pub fn exec_end_table_func_scan(node: &mut TableFuncScanState<'_>) {
    if let Some(store) = node.tstore.take() {
        store.end();
    }
}

/// `ExecReScanTableFuncScan`: params changed → rebuild; else rewind.
pub fn exec_rescan_table_func_scan<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    if let Some(store) = node.tstore.as_mut() {
        store.rescan()?;
    }
    Ok(())
}

/// show_table_func_scan_info's tuplestore read (explain.c:3537-3542); None
/// while `tupstore == NULL` (nothing fetched yet, or dropped by a rescan).
pub fn storage_stats(
    node: &mut TableFuncScanState<'_>,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    node.tstore.as_mut().map(Tuplestore::get_stats)
}

/// Changed-params rescan: drop the tuplestore; the next fetch re-evaluates.
pub fn exec_rescan_table_func_scan_chg<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    if let Some(store) = node.tstore.take() {
        store.end();
    }
    Ok(())
}

// Exempt: tstore released in exec_end_table_func_scan; per_table_mcx is a
// NonNull to an arena slot whose context value the estate-reset callback
// drops (make_per_table_ctx); the rest is plain data.
mcx::forget_safe_struct!(
    TableFuncScanState<'_> {
        ss, tf, ordinal, per_table_mcx;
        docexpr, rowexpr, ns_uris, colexprs, coldefexprs, colvalexprs,
        passingvalexprs, in_functions, typioparams, tupdesc, tstore,
        cstr_scratch
    },
);
