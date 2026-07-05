// nodeIndexscan.c: Var-op-Const quals become ScanKeys at init (rule 5);
// runtime keys (indexkey op expression, incl. SK_SEARCHARRAY arrays)
// re-evaluate into the same ScanKeys at rescan. Non-amsearcharray array keys,
// RowCompare, and ORDER BY (reorder-queue) arms loud-panic pending their
// lanes. EPQ arms loud-panic pending EPQState.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{
    exec_eval_expr, exec_init_expr, exec_init_qual, exec_qual, EvalSlots, ExprState, ParamBind,
    INDEX_VAR,
};
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::indexam::{
    index_beginscan, index_close, index_endscan, index_getnext_slot, index_getnext_tid,
    index_markpos, index_rescan, index_restrpos, IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::table_slot_callbacks;
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::IndexScan;
use ::types_nodes::NodeTag;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::{
    ScanKeyData, StrategyNumber, SK_ISNULL, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use ::types_scan::sdir::{ScanDirection, ScanDirectionCombine};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct IndexRuntimeKeyInfo<'mcx> {
    pub scan_key: usize,
    pub key_expr: PgBox<'mcx, ExprState<'mcx>>,
    pub key_toastable: bool,
}

pub struct RuntimeKeysState<'mcx> {
    pub keys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    pub ready: bool,
    pub ecxt: EcxtId,
}

pub struct IndexScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub indexqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub iss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub iss_RelationDesc: Option<Relation<'mcx>>,
    pub iss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub iss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
    pub iss_OrderDir: ScanDirection,
    pub iss_PlanNodeId: i32,
    pub iss_ParallelAware: bool,
}

impl<'mcx> ScanNode<'mcx> for IndexScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `IndexRecheck`: does the EPQ test tuple meet the original quals?
    fn epq_recheck(
        &mut self,
        estate: &mut EStateData<'mcx>,
        slot: ExecSlotId,
    ) -> PgResult<bool> {
        let ecxt = self.ss.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot);
        let passes = {
            let mut slots = EvalSlots {
                scan: Some(estate.slot_mut(slot)),
                inner: None,
                outer: None,
            };
            exec_qual(self.indexqualorig.as_deref_mut(), &mut slots)?
        };
        estate.ecxt_mut(ecxt).reset();
        Ok(passes)
    }

    /// `IndexNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = ScanDirectionCombine(estate.es_direction, self.iss_OrderDir);

        if self.iss_ScanDesc.is_none() {
            self.open_scandesc(estate)?;
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        loop {
            check_for_interrupts()?;
            // SAFETY: written just above when None; single test+branch like
            // C's scandesc == NULL check.
            let scandesc = unsafe { self.iss_ScanDesc.as_deref_mut().unwrap_unchecked() };
            let found = index_getnext_slot(mcx, scandesc, direction, estate.slot_mut(slot_id))?;
            if estate.es_instrument != 0 {
                let n = scandesc.xs_nsearches;
                estate.instr_set_index_nsearches(self.iss_PlanNodeId, n);
            }
            if !found {
                exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                return Ok(false);
            }

            // Lossy index: recheck the original quals against the heap tuple
            // (ExecQualAndReset shape). Btree never sets xs_recheck.
            if scandesc.xs_recheck {
                let ecxt = self.ss.ps_ExprContext;
                estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot_id);
                let passes = {
                    let mut slots = EvalSlots {
                        scan: Some(estate.slot_mut(slot_id)),
                        inner: None,
                        outer: None,
                    };
                    exec_qual(self.indexqualorig.as_deref_mut(), &mut slots)?
                };
                estate.ecxt_mut(ecxt).reset();
                if !passes {
                    continue;
                }
            }
            return Ok(true);
        }
    }
}

impl<'mcx> IndexScanState<'mcx> {
    #[inline(never)]
    fn open_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("index scan requires es_snapshot");
        let mut scandesc = index_beginscan(
            mcx,
            self.ss
                .ss_currentRelation
                .as_ref()
                .expect("indexscan has a relation"),
            self.iss_RelationDesc.as_ref().expect("index relation open"),
            snapshot,
            self.iss_ScanKeys.len() as i32,
            0,
        )?;
        if self.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
            index_rescan(&mut scandesc, Some(&self.iss_ScanKeys), None)?;
        }
        // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
        self.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        Ok(())
    }
}

/// Fused agg-over-indexscan page-batch drive: stage the next same-block TID
/// run. The dispatcher's matcher (btree, MVCC, forward, no quals/projection/
/// runtime keys) gates every call.
pub fn index_scan_next_tidrun<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    check_for_interrupts()?;
    if node.iss_ScanDesc.is_none() {
        node.open_scandesc(estate)?;
    }
    let mcx = estate.es_query_cxt;
    let direction = ScanDirectionCombine(estate.es_direction, node.iss_OrderDir);
    // SAFETY: written by open_scandesc when None.
    let scandesc = unsafe { node.iss_ScanDesc.as_deref_mut().unwrap_unchecked() };
    ::indexam::index_getnext_tidrun(mcx, scandesc, direction)
}

/// Store staged run entry `i` into the scan slot; false = not visible.
#[inline(always)]
pub fn index_scan_batch_fetch<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let slot_id = node.ss.ss_ScanTupleSlot;
    let direction = ScanDirectionCombine(estate.es_direction, node.iss_OrderDir);
    let scandesc = node.iss_ScanDesc.as_deref_mut().expect("batch fetch before tidrun");
    if i > 0 && index_getnext_tid(scandesc, direction)?.is_none() {
        return Ok(false);
    }
    let found = ::indexam::index_fetch_heap(mcx, scandesc, estate.slot_mut(slot_id))?;
    // Matcher admits btree only; xs_recheck stays false (no indexqualorig arm).
    debug_assert!(!scandesc.xs_recheck);
    Ok(found)
}

#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

/// `ExecIndexScan`; the reorder arm is cut off at init (ORDER BY).
pub fn exec_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if node.iss_Runtime.as_deref().is_some_and(|r| !r.ready) {
        exec_rescan_index_scan(node, estate)?;
    }
    execscan::exec_scan(node, estate)
}

/// `ExecInitIndexScan`; opens both relations through the estate range table.
pub fn exec_init_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &IndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<IndexScanState<'mcx>> {
    let rel = estate
        .exec_get_range_table_relation(node.scan.scanrelid, false)?
        .alias();
    let index_rel = indexam::index_open(mcx, node.indexid, index_lockmode(estate, node.scan.scanrelid))?;
    exec_init_index_scan_rel(mcx, node, estate, rel, index_rel)
}

// C opens scan indexes with the RTE's rellockmode unconditionally
// (nodeIndexscan.c:977): a reused generic plan reaches the executor with no
// planner invocation, and plancache's AcquireExecutorLocks locks tables only,
// so this open is the index's only lock.
pub fn index_lockmode(estate: &EStateData<'_>, scanrelid: u32) -> types_rel::LOCKMODE {
    estate.exec_rt_fetch(scanrelid).rellockmode
}

/// C divergence: init over caller-opened relations, splitting
/// ExecOpenScanRelation/index_open out until the range-table lane lands.
pub fn exec_init_index_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &IndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
    index_rel: Relation<'mcx>,
) -> PgResult<IndexScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let ps_ExprContext = estate.exec_assign_expr_context();
    let kind = table_slot_callbacks(&rel);
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;
    let indexqualorig = exec_init_qual(mcx, &node.indexqualorig, params)?;

    if !node.indexorderby.is_nil() || !node.indexorderbyorig.is_nil() {
        orderby_unported();
    }

    let (iss_ScanKeys, runtime_keys) =
        exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual, params)?;
    // C keeps ps_ExprContext as the standard econtext and gives runtime keys
    // their own, reset per rescan.
    let iss_Runtime = if runtime_keys.is_empty() {
        None
    } else {
        Some(::mcx::alloc_in(
            mcx,
            RuntimeKeysState {
                keys: runtime_keys,
                ready: false,
                ecxt: estate.exec_assign_expr_context(),
            },
        )?)
    };

    Ok(IndexScanState {
        ss,
        indexqualorig,
        iss_ScanDesc: None,
        iss_RelationDesc: Some(index_rel),
        iss_ScanKeys,
        iss_Runtime,
        iss_OrderDir: order_dir(node.indexorderdir),
        iss_PlanNodeId: node.scan.plan.plan_node_id,
        iss_ParallelAware: node.scan.plan.parallel_aware,
    })
}

fn order_dir(dir: i32) -> ScanDirection {
    match dir {
        -1 => ScanDirection::BackwardScanDirection,
        0 => ScanDirection::NoMovementScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        other => panic!("invalid indexorderdir {other}"),
    }
}

#[cold]
#[inline(never)]
fn orderby_unported() -> ! {
    panic!("nodeindexscan: indexorderby (IndexNextWithReorder/pairingheap KNN lane) not ported")
}

#[cold]
#[inline(never)]
fn scankey_case_unported(what: &str) -> ! {
    panic!("nodeindexscan: ExecIndexBuildScanKeys {what} not ported")
}

/// `ExecIndexBuildScanKeys`, cases 1 (indexkey op Const), 2 (runtime key),
/// 4 (amsearcharray ScalarArrayOp, Const or runtime array), and 5 (NullTest).
/// RowCompare and non-amsearcharray ScalarArrayOp loud-panic (the planner
/// only builds saop index quals on amsearcharray AMs — plancat sets it for
/// btree only); the isorderby leg is cut off at init (orderby_unported).
pub fn exec_index_build_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    quals: &NodeList<'mcx>,
    params: ParamBind<'mcx>,
) -> PgResult<(
    PgVec<'mcx, ScanKeyData>,
    PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
)> {
    let indnkeyatts = index.indnkeyatts();
    let mut scan_keys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    let mut runtime_keys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>> = PgVec::new_in(mcx);
    scan_keys
        .try_reserve_exact(quals.len())
        .map_err(|_| Box::new(mcx.oom(quals.len() * core::mem::size_of::<ScanKeyData>())))?;

    for clause in quals.iter() {
        let op = match clause.node_tag() {
            NodeTag::T_OpExpr => clause.as_op_expr().unwrap(),
            NodeTag::T_RowCompareExpr => scankey_case_unported("RowCompareExpr"),
            NodeTag::T_ScalarArrayOpExpr => {
                let saop = clause.as_scalar_array_op_expr().unwrap();
                debug_assert!(saop.useOr);
                if !::indexam::IndexAmKind::from_relam(index.rd_rel.relam).amsearcharray() {
                    scankey_case_unported("ScalarArrayOpExpr on a non-amsearcharray AM");
                }
                let leftop = saop.args.nth(0);
                if leftop.node_tag() == NodeTag::T_RelabelType {
                    scankey_case_unported("RelabelType-wrapped index key");
                }
                let var = leftop
                    .as_var()
                    .filter(|v| v.varno == INDEX_VAR)
                    .unwrap_or_else(|| panic!("indexqual doesn't have key on left side"));
                let varattno = var.varattno;
                if varattno < 1 || varattno as i32 > indnkeyatts {
                    panic!("bogus index qualification");
                }
                let opfamily = index.rd_opfamily[varattno as usize - 1];
                let (op_strategy, _op_lefttype, op_righttype) =
                    lsyscache::get_op_opfamily_properties(saop.opno, opfamily, false)?;

                let mut rightop = saop.args.nth(1);
                if rightop.node_tag() == NodeTag::T_RelabelType {
                    rightop = rightop.as_relabel_type().unwrap().arg;
                }
                let (flags, scanvalue) = match rightop.as_const() {
                    Some(con) => (
                        SK_SEARCHARRAY | if con.constisnull { SK_ISNULL } else { 0 },
                        con.constvalue,
                    ),
                    None => {
                        runtime_keys.push(IndexRuntimeKeyInfo {
                            scan_key: scan_keys.len(),
                            key_expr: exec_init_expr(mcx, Some(rightop), params)?
                                .expect("runtime key expr compiles"),
                            // The expr yields an array of op_righttype, not
                            // op_righttype itself; every array type is toastable.
                            key_toastable: true,
                        });
                        (SK_SEARCHARRAY, ::datum::Datum::from_usize(0))
                    }
                };

                let mut key = ScanKeyData::empty();
                key.sk_flags = flags;
                key.sk_attno = varattno;
                key.sk_strategy = op_strategy as StrategyNumber;
                key.sk_subtype = op_righttype;
                key.sk_collation = saop.inputcollid;
                fmgr_core::fmgr_info_into(saop.opfuncid, &mut key.sk_func)?;
                key.sk_argument = scanvalue;
                scan_keys.push(key);
                continue;
            }
            NodeTag::T_NullTest => {
                let nt = clause.as_null_test().unwrap();
                let var = nt
                    .arg
                    .expect("NullTest.arg")
                    .as_var()
                    .filter(|v| v.varno == INDEX_VAR)
                    .unwrap_or_else(|| panic!("NullTest indexqual has wrong key"));
                let flags = SK_ISNULL
                    | match nt.nulltesttype {
                        types_nodes::primnodes::NullTestType::IS_NULL => SK_SEARCHNULL,
                        types_nodes::primnodes::NullTestType::IS_NOT_NULL => SK_SEARCHNOTNULL,
                    };
                let mut key = ScanKeyData::empty();
                key.sk_flags = flags;
                key.sk_attno = var.varattno;
                key.sk_strategy = 0;
                key.sk_subtype = 0;
                key.sk_collation = 0;
                scan_keys.push(key);
                continue;
            }
            tag => panic!("unsupported indexqual type: {tag:?}"),
        };

        let mut args = op.args.iter();
        let (leftop, rightop) = (args.next(), args.next());

        let mut leftop = leftop.unwrap_or_else(|| panic!("indexqual OpExpr missing left arg"));
        if leftop.node_tag() == NodeTag::T_RelabelType {
            leftop = leftop.as_relabel_type().unwrap().arg;
        }
        let var = leftop
            .as_var()
            .filter(|v| v.varno == INDEX_VAR)
            .unwrap_or_else(|| panic!("indexqual doesn't have key on left side"));
        let varattno = var.varattno;
        if varattno < 1 || varattno as i32 > indnkeyatts {
            panic!("bogus index qualification");
        }

        // Strategy lookup cross-checks that the operator matches the index.
        let opfamily = index.rd_opfamily[varattno as usize - 1];
        let (op_strategy, _op_lefttype, op_righttype) =
            lsyscache::get_op_opfamily_properties(op.opno, opfamily, false)?;

        let mut rightop = rightop.unwrap_or_else(|| panic!("indexqual OpExpr missing right arg"));
        if rightop.node_tag() == NodeTag::T_RelabelType {
            rightop = rightop.as_relabel_type().unwrap().arg;
        }
        let (flags, scanvalue) = match rightop.as_const() {
            Some(con) => (if con.constisnull { SK_ISNULL } else { 0 }, con.constvalue),
            None => {
                runtime_keys.push(IndexRuntimeKeyInfo {
                    scan_key: scan_keys.len(),
                    key_expr: exec_init_expr(mcx, Some(rightop), params)?
                        .expect("runtime key expr compiles"),
                    key_toastable: lsyscache::get_typlen(op_righttype)? == -1,
                });
                (0, ::datum::Datum::from_usize(0))
            }
        };

        // ScanKeyEntryInitialize (access/common/scankey.c).
        let mut key = ScanKeyData::empty();
        key.sk_flags = flags;
        key.sk_attno = varattno;
        key.sk_strategy = op_strategy as StrategyNumber;
        key.sk_subtype = op_righttype;
        key.sk_collation = op.inputcollid;
        fmgr_core::fmgr_info_into(op.opfuncid, &mut key.sk_func)?;
        key.sk_argument = scanvalue;
        scan_keys.push(key);
    }

    Ok((scan_keys, runtime_keys))
}

/// `ExecEndIndexScan`; the parallel-worker instrumentation copy-back arm
/// lands with DSM.
pub fn exec_end_index_scan(node: &mut IndexScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.iss_ScanDesc.take() {
        index_endscan(PgBox::into_inner(scandesc))?;
    }
    if let Some(index_rel) = node.iss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.indexqualorig = None;
    node.iss_ScanKeys.clear();
    node.iss_Runtime = None;
    Ok(())
}

/// `ExecReScanIndexScan`; the reorder-queue flush arm is unreachable (ORDER
/// BY loud-panics at init).
pub fn exec_rescan_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(rt) = node.iss_Runtime.as_deref_mut() {
        estate.reset_expr_context(rt.ecxt);
        exec_index_eval_runtime_keys(estate, rt.ecxt, &mut rt.keys, &mut node.iss_ScanKeys)?;
        rt.ready = true;
    }
    let IndexScanState {
        iss_ScanDesc,
        iss_ScanKeys,
        ss,
        ..
    } = node;
    if let Some(scandesc) = iss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(iss_ScanKeys), None)?;
    }
    execscan::exec_scan_rescan(ss, estate);
    Ok(())
}

/// `ExecIndexMarkPos`; the EPQ test-tuple arm lands with execMain's EPQState.
pub fn exec_index_mark_pos(node: &mut IndexScanState<'_>) -> PgResult<()> {
    index_markpos(
        node.iss_ScanDesc
            .as_deref_mut()
            .expect("mark before first fetch"),
    )
}

/// `ExecIndexRestrPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_restr_pos(node: &mut IndexScanState<'_>) -> PgResult<()> {
    index_restrpos(
        node.iss_ScanDesc
            .as_deref_mut()
            .expect("restore before first fetch"),
    )
}

/// `ExecIndexEvalRuntimeKeys`; caller resets the runtime econtext first, so
/// key values (and forced detoasts) live until the next rescan.
pub fn exec_index_eval_runtime_keys<'mcx>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    runtime_keys: &mut [IndexRuntimeKeyInfo<'mcx>],
    scan_keys: &mut [ScanKeyData],
) -> PgResult<()> {
    for rk in runtime_keys.iter_mut() {
        // ExecEvalParamExec pending-initplan arm, hoisted per repo convention.
        let deps = rk.key_expr.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
        // SAFETY: the per-tuple context object outlives the plan (reset-only).
        unsafe {
            rk.key_expr
                .arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx())
        };
        let mut slots = EvalSlots {
            scan: None,
            inner: None,
            outer: None,
        };
        let nd = exec_eval_expr(&mut rk.key_expr, &mut slots)?;
        let key = &mut scan_keys[rk.scan_key];
        if nd.isnull {
            key.sk_argument = nd.value;
            key.sk_flags |= SK_ISNULL;
        } else {
            key.sk_argument = if rk.key_toastable {
                detoast_datum(estate.ecxt(ecxt).per_tuple_mcx(), nd.value)?
            } else {
                nd.value
            };
            key.sk_flags &= !SK_ISNULL;
        }
    }
    Ok(())
}

/// `PG_DETOAST_DATUM`: forced detoast so index support functions don't repeat
/// it; plain 4B-uncompressed values pass through untouched.
fn detoast_datum<'m>(mcx: Mcx<'m>, v: ::datum::Datum) -> PgResult<::datum::Datum> {
    let p = v.as_usize() as *const u8;
    // SAFETY: non-null pass-by-ref varlena datum; image readable through its
    // header-declared size (VARATT_IS_EXTENDED = any non-4B-uncompressed form).
    unsafe {
        if (*p) & 0x03 == 0 {
            return Ok(v);
        }
        let image = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
        let flat = detoast::detoast_attr(mcx, image)?;
        Ok(::datum::Datum::from_usize(flat.leak().as_ptr() as usize))
    }
}

pub fn exec_index_eval_array_keys() -> ! {
    panic!("nodeindexscan: ExecIndexEvalArrayKeys unreachable (planner emits saop index quals only on amsearcharray AMs)")
}

pub fn exec_index_advance_array_keys() -> ! {
    panic!("nodeindexscan: ExecIndexAdvanceArrayKeys unreachable (planner emits saop index quals only on amsearcharray AMs)")
}

/// `ExecIndexScanEstimate`: no DSM thread-native; the instrument-only arm is
/// covered by execParallel's collapsed per-worker retrieval.
pub fn exec_index_scan_estimate(_node: &mut IndexScanState<'_>) {}

/// `ExecIndexScanInitializeDSM` (the leader participates too).
pub fn exec_index_scan_initialize_dsm<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<std::sync::Arc<::indexam::ParallelIndexScanDescShared>> {
    let mcx = estate.es_query_cxt;
    let heap = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("indexscan has a relation");
    let index = node.iss_RelationDesc.as_ref().expect("index relation open");
    let snapshot = estate
        .es_snapshot
        .as_ref()
        .expect("parallel index scan requires es_snapshot");
    let pscan = ::indexam::index_parallelscan_initialize(heap, index, snapshot)?;

    let mut scandesc = ::indexam::index_beginscan_parallel(
        mcx,
        heap,
        index,
        node.iss_ScanKeys.len() as i32,
        0,
        std::sync::Arc::clone(&pscan),
    )?;
    if node.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(&mut scandesc, Some(&node.iss_ScanKeys), None)?;
    }
    debug_assert!(node.iss_ScanDesc.is_none());
    node.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    Ok(pscan)
}

/// `ExecIndexScanReInitializeDSM`.
pub fn exec_index_scan_reinitialize_dsm(node: &mut IndexScanState<'_>) -> PgResult<()> {
    ::indexam::index_parallelrescan(
        node.iss_ScanDesc
            .as_deref_mut()
            .expect("parallel indexscan was initialized"),
    )
}

/// `ExecIndexScanInitializeWorker`.
pub fn exec_index_scan_initialize_worker<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pscan: std::sync::Arc<::indexam::ParallelIndexScanDescShared>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let heap = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("indexscan has a relation");
    let index = node.iss_RelationDesc.as_ref().expect("index relation open");
    let mut scandesc = ::indexam::index_beginscan_parallel(
        mcx,
        heap,
        index,
        node.iss_ScanKeys.len() as i32,
        0,
        pscan,
    )?;
    if node.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(&mut scandesc, Some(&node.iss_ScanKeys), None)?;
    }
    debug_assert!(node.iss_ScanDesc.is_none());
    node.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    Ok(())
}

// Exempt: droppy owners, all released in exec_end_index_scan; ScanDirection
// is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexScanState<'_> { ss, iss_PlanNodeId, iss_ParallelAware;
        indexqualorig, iss_ScanDesc, iss_RelationDesc, iss_ScanKeys, iss_Runtime,
        iss_OrderDir },
);
