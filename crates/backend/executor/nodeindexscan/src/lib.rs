// nodeIndexscan.c, point-select lane: Var-op-Const quals become ScanKeys at
// init (rule 5); runtime keys, array keys, RowCompare, NullTest, and ORDER BY
// (reorder-queue) arms loud-panic pending their lanes. EPQ/parallel arms
// loud-panic pending EPQState and DSM/shm_toc.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_init_qual, exec_qual, EvalSlots, ExprState, INDEX_VAR};
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
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
use ::types_scan::scankey::{ScanKeyData, StrategyNumber, SK_ISNULL};
use ::types_scan::sdir::{ScanDirection, ScanDirectionCombine};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct IndexScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub indexqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub iss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub iss_RelationDesc: Option<Relation<'mcx>>,
    pub iss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub iss_OrderDir: ScanDirection,
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
            // No runtime keys in this lane, so the keys are always ready.
            index_rescan(&mut scandesc, Some(&self.iss_ScanKeys), None)?;
            // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
            self.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        loop {
            check_for_interrupts();
            // SAFETY: written just above when None; single test+branch like
            // C's scandesc == NULL check.
            let scandesc = unsafe { self.iss_ScanDesc.as_deref_mut().unwrap_unchecked() };
            if !index_getnext_slot(mcx, scandesc, direction, estate.slot_mut(slot_id))? {
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

/// `ExecIndexScan`; the runtime-key ExecReScan arm is unreachable here (init
/// loud-panics on non-Const quals), the reorder arm at init (ORDER BY).
pub fn exec_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
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
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = exec_init_qual(mcx, &node.scan.plan.qual, params)?;
    let indexqualorig = exec_init_qual(mcx, &node.indexqualorig, params)?;

    if !node.indexorderby.is_nil() || !node.indexorderbyorig.is_nil() {
        orderby_unported();
    }

    let iss_ScanKeys = exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual)?;

    Ok(IndexScanState {
        ss,
        indexqualorig,
        iss_ScanDesc: None,
        iss_RelationDesc: Some(index_rel),
        iss_ScanKeys,
        iss_OrderDir: order_dir(node.indexorderdir),
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

/// `ExecIndexBuildScanKeys`, case 1 only (indexkey op Const). Cases 2-5
/// (runtime keys, RowCompare, ScalarArrayOp, NullTest) loud-panic; the
/// isorderby leg is cut off at init (orderby_unported).
pub fn exec_index_build_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    quals: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let indnkeyatts = index.indnkeyatts();
    let mut scan_keys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    scan_keys
        .try_reserve_exact(quals.len())
        .map_err(|_| Box::new(mcx.oom(quals.len() * core::mem::size_of::<ScanKeyData>())))?;

    for clause in quals.iter() {
        let op = match clause.node_tag() {
            NodeTag::T_OpExpr => clause.as_op_expr().unwrap(),
            NodeTag::T_RowCompareExpr => scankey_case_unported("RowCompareExpr"),
            NodeTag::T_ScalarArrayOpExpr => scankey_case_unported("ScalarArrayOpExpr"),
            NodeTag::T_NullTest => scankey_case_unported("NullTest"),
            tag => panic!("unsupported indexqual type: {tag:?}"),
        };

        let mut args = op.args.iter();
        let (leftop, rightop) = (args.next(), args.next());

        let leftop = leftop.unwrap_or_else(|| panic!("indexqual OpExpr missing left arg"));
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

        // Strategy lookup cross-checks that the operator matches the index.
        let opfamily = index.rd_opfamily[varattno as usize - 1];
        let (op_strategy, _op_lefttype, op_righttype) =
            lsyscache::get_op_opfamily_properties(op.opno, opfamily, false)?;

        let rightop = rightop.unwrap_or_else(|| panic!("indexqual OpExpr missing right arg"));
        if rightop.node_tag() == NodeTag::T_RelabelType {
            scankey_case_unported("RelabelType-wrapped comparison value");
        }
        let Some(con) = rightop.as_const() else {
            scankey_case_unported("runtime key (non-Const comparison value)");
        };

        // ScanKeyEntryInitialize (access/common/scankey.c).
        let mut key = ScanKeyData::empty();
        key.sk_flags = if con.constisnull { SK_ISNULL } else { 0 };
        key.sk_attno = varattno;
        key.sk_strategy = op_strategy as StrategyNumber;
        key.sk_subtype = op_righttype;
        key.sk_collation = op.inputcollid;
        fmgr_core::fmgr_info_into(op.opfuncid, &mut key.sk_func)?;
        key.sk_argument = con.constvalue;
        scan_keys.push(key);
    }

    Ok(scan_keys)
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
    Ok(())
}

/// `ExecReScanIndexScan`; the runtime-key recompute and reorder-queue flush
/// arms are unreachable (both lanes loud-panic at init).
pub fn exec_rescan_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
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

pub fn exec_index_eval_runtime_keys() -> ! {
    panic!("nodeindexscan: ExecIndexEvalRuntimeKeys pending the runtime-key lane")
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
