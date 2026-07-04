// nodeIndexscan.c: Var-op-Const quals become ScanKeys at init (rule 5);
// runtime keys (indexkey op expression) re-evaluate into the same ScanKeys at
// rescan. Array keys, RowCompare, and ORDER BY (reorder-queue) arms loud-panic
// pending their lanes. EPQ/parallel arms loud-panic pending EPQState and
// DSM/shm_toc.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{
    exec_eval_expr, exec_init_expr, exec_init_qual, exec_qual, EvalSlots, ExprState, ParamBind,
    INDEX_VAR,
};
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::indexam::{
    index_beginscan, index_close, index_endscan, index_getnext_slot, index_markpos,
    index_rescan, index_restrpos, IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::table_slot_callbacks;
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::IndexScan;
use ::types_nodes::NodeTag;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::{ScanKeyData, StrategyNumber, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use ::types_scan::sdir::{ScanDirection, ScanDirectionCombine};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct IndexRuntimeKeyInfo<'mcx> {
    pub scan_key: usize,
    pub key_expr: PgBox<'mcx, ExprState<'mcx>>,
    pub key_toastable: bool,
}

pub struct IndexScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub indexqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub iss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub iss_RelationDesc: Option<Relation<'mcx>>,
    pub iss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub iss_RuntimeKeys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    pub iss_RuntimeKeysReady: bool,
    pub iss_RuntimeContext: Option<EcxtId>,
    pub iss_OrderDir: ScanDirection,
    pub iss_PlanNodeId: i32,
}

impl<'mcx> ScanNode<'mcx> for IndexScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `IndexNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = ScanDirectionCombine(estate.es_direction, self.iss_OrderDir);

        if self.iss_ScanDesc.is_none() {
            let snapshot = estate
                .es_snapshot
                .clone()
                .expect("index scan requires es_snapshot");
            let mut scandesc = index_beginscan(
                mcx,
                self.ss.ss_currentRelation.as_ref().expect("indexscan has a relation"),
                self.iss_RelationDesc.as_ref().expect("index relation open"),
                snapshot,
                self.iss_ScanKeys.len() as i32,
                0,
            )?;
            if self.iss_RuntimeKeys.is_empty() || self.iss_RuntimeKeysReady {
                index_rescan(&mut scandesc, Some(&self.iss_ScanKeys), None)?;
            }
            // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
            self.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        loop {
            check_for_interrupts();
            // SAFETY: written just above when None; single test+branch like
            // C's scandesc == NULL check.
            let scandesc = unsafe { self.iss_ScanDesc.as_deref_mut().unwrap_unchecked() };
            let found = index_getnext_slot(mcx, scandesc, direction, estate.slot_mut(slot_id))?;
            if estate.es_instrument != 0 {
                let n = scandesc.xs_pgstat_index_scans;
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

#[cold]
#[inline(never)]
fn interrupt_unported() -> ! {
    panic!("nodeindexscan: ProcessInterrupts (tcop/postgres.c) unported")
}

#[inline(always)]
fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        interrupt_unported();
    }
}

/// `ExecIndexScan`; the reorder arm is cut off at init (ORDER BY).
pub fn exec_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if !node.iss_RuntimeKeys.is_empty() && !node.iss_RuntimeKeysReady {
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
    // C: lockmode = exec_rt_fetch(scanrelid)->rellockmode, unreachable until
    // the range-table lane lands (the call above panics first).
    let index_rel = indexam::index_open(mcx, node.indexid, NoLock)?;
    exec_init_index_scan_rel(mcx, node, estate, rel, index_rel)
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
    ss.qual = exec_init_qual(mcx, &node.scan.plan.qual, params)?;
    let indexqualorig = exec_init_qual(mcx, &node.indexqualorig, params)?;

    if !node.indexorderby.is_nil() || !node.indexorderbyorig.is_nil() {
        orderby_unported();
    }

    let (iss_ScanKeys, iss_RuntimeKeys) =
        exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual, params)?;
    // C keeps ps_ExprContext as the standard econtext and gives runtime keys
    // their own, reset per rescan.
    let iss_RuntimeContext = if iss_RuntimeKeys.is_empty() {
        None
    } else {
        Some(estate.exec_assign_expr_context())
    };

    Ok(IndexScanState {
        ss,
        indexqualorig,
        iss_ScanDesc: None,
        iss_RelationDesc: Some(index_rel),
        iss_ScanKeys,
        iss_RuntimeKeys,
        iss_RuntimeKeysReady: false,
        iss_RuntimeContext,
        iss_OrderDir: order_dir(node.indexorderdir),
        iss_PlanNodeId: node.scan.plan.plan_node_id,
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
/// and 5 (NullTest). RowCompare and ScalarArrayOp loud-panic; the isorderby
/// leg is cut off at init (orderby_unported).
pub fn exec_index_build_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    quals: &NodeList<'mcx>,
    params: ParamBind<'mcx>,
) -> PgResult<(PgVec<'mcx, ScanKeyData>, PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>)> {
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
            NodeTag::T_ScalarArrayOpExpr => scankey_case_unported("ScalarArrayOpExpr"),
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
    node.iss_RuntimeKeys.clear();
    Ok(())
}

/// `ExecReScanIndexScan`; the reorder-queue flush arm is unreachable (ORDER
/// BY loud-panics at init).
pub fn exec_rescan_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if !node.iss_RuntimeKeys.is_empty() {
        let ecxt = node.iss_RuntimeContext.expect("runtime keys have their econtext");
        estate.reset_expr_context(ecxt);
        exec_index_eval_runtime_keys(
            estate,
            ecxt,
            &mut node.iss_RuntimeKeys,
            &mut node.iss_ScanKeys,
        )?;
    }
    node.iss_RuntimeKeysReady = true;
    let IndexScanState { iss_ScanDesc, iss_ScanKeys, ss, .. } = node;
    if let Some(scandesc) = iss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(iss_ScanKeys), None)?;
    }
    execscan::exec_scan_rescan(ss, estate);
    Ok(())
}

/// `ExecIndexMarkPos`; the EPQ test-tuple arm lands with execMain's EPQState.
pub fn exec_index_mark_pos(node: &mut IndexScanState<'_>) -> PgResult<()> {
    index_markpos(node.iss_ScanDesc.as_deref_mut().expect("mark before first fetch"))
}

/// `ExecIndexRestrPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_restr_pos(node: &mut IndexScanState<'_>) -> PgResult<()> {
    index_restrpos(node.iss_ScanDesc.as_deref_mut().expect("restore before first fetch"))
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
        // SAFETY: the per-tuple context object outlives the plan (reset-only).
        unsafe { rk.key_expr.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
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
    panic!("nodeindexscan: ExecIndexEvalArrayKeys pending the array-key lane")
}

pub fn exec_index_advance_array_keys() -> ! {
    panic!("nodeindexscan: ExecIndexAdvanceArrayKeys pending the array-key lane")
}

pub fn exec_index_scan_estimate(_node: &mut IndexScanState<'_>) -> ! {
    panic!("nodeindexscan: ExecIndexScanEstimate pending parallel DSM/shm_toc")
}

pub fn exec_index_scan_initialize_dsm(_node: &mut IndexScanState<'_>) -> ! {
    panic!("nodeindexscan: ExecIndexScanInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_index_scan_reinitialize_dsm(_node: &mut IndexScanState<'_>) -> ! {
    panic!("nodeindexscan: ExecIndexScanReInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_index_scan_initialize_worker(_node: &mut IndexScanState<'_>) -> ! {
    panic!("nodeindexscan: ExecIndexScanInitializeWorker pending parallel DSM/shm_toc")
}

pub fn exec_index_scan_retrieve_instrumentation(_node: &mut IndexScanState<'_>) -> ! {
    panic!("nodeindexscan: ExecIndexScanRetrieveInstrumentation pending parallel DSM/shm_toc")
}

// Exempt: droppy owners, all released in exec_end_index_scan; ScanDirection
// is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexScanState<'_> { ss, iss_PlanNodeId, iss_RuntimeKeysReady, iss_RuntimeContext;
        indexqualorig, iss_ScanDesc, iss_RelationDesc, iss_ScanKeys, iss_RuntimeKeys,
        iss_OrderDir },
);
