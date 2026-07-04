// nodeBitmapHeapscan.c. The bitmapqual child subtree stays with execmain's
// dispatcher (nodesort precedent): it runs MultiExec and hands the finished
// TIDBitmap to bitmap_table_scan_setup. Parallel (pstate) arms loud there.
#![allow(non_snake_case)]

use ::execexpr::{exec_init_qual, exec_qual, EvalSlots, ExprState};
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::tableam::{table_beginscan_bm, table_endscan, table_rescan, table_slot_callbacks};
use ::tidbitmap::{TbmIterator, TIDBitmap};
use ::types_error::PgResult;
use ::types_nodes::plannodes::BitmapHeapScan;
use ::types_rel::Relation;
use ::types_snapshot::IsMVCCSnapshot;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct BitmapHeapScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub bitmapqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub tbm: Option<TIDBitmap<'mcx>>,
    pub tbmiterator: TbmIterator,
    pub initialized: bool,
    pub recheck: bool,
    pub stats_exact_pages: u64,
    pub stats_lossy_pages: u64,
}

impl<'mcx> ScanNode<'mcx> for BitmapHeapScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `BitmapHeapRecheck`: does the EPQ test tuple meet the original quals?
    fn epq_recheck(
        &mut self,
        estate: &mut EStateData<'mcx>,
        slot: ExecSlotId,
    ) -> PgResult<bool> {
        let ecxt = self.ss.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot);
        let passes = {
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            if let Some(q) = self.bitmapqualorig.as_deref_mut() {
                // SAFETY: reset-only context, outlives the plan.
                unsafe { q.arm_result_mcx_raw(per_tuple) };
            }
            let mut slots = EvalSlots {
                scan: Some(estate.slot_mut(slot)),
                inner: None,
                outer: None,
            };
            exec_qual(self.bitmapqualorig.as_deref_mut(), &mut slots)?
        };
        estate.ecxt_mut(ecxt).reset();
        Ok(passes)
    }

    /// `BitmapHeapNext` minus setup (the dispatcher ran MultiExec first).
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        debug_assert!(self.initialized, "scan_next before bitmap_table_scan_setup");
        let mcx = estate.es_query_cxt;
        let slot_id = self.ss.ss_ScanTupleSlot;
        loop {
            let scandesc = self
                .ss
                .ss_currentScanDesc
                .as_mut()
                .expect("bitmap heap scan without a table scan descriptor");
            let tbm = self.tbm.as_ref().expect("bitmap heap scan without a bitmap");
            if !::tableam::table_scan_bitmap_next_tuple(
                mcx,
                scandesc,
                tbm,
                &mut self.tbmiterator,
                estate.slot_mut(slot_id),
                &mut self.recheck,
                &mut self.stats_lossy_pages,
                &mut self.stats_exact_pages,
            )? {
                exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                return Ok(false);
            }

            check_for_interrupts()?;

            // Lossy page or candidate match: recheck the original quals
            // against the heap tuple (ExecQualAndReset shape).
            if self.recheck {
                let ecxt = self.ss.ps_ExprContext;
                estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot_id);
                let passes = {
                    // Per-tuple result mcx for arg-detoasting rechecks
                    // (jsonb @> ...); the ecxt reset below frees it.
                    let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
                    if let Some(q) = self.bitmapqualorig.as_deref_mut() {
                        // SAFETY: reset-only context, outlives the plan.
                        unsafe { q.arm_result_mcx_raw(per_tuple) };
                    }
                    let mut slots = EvalSlots {
                        scan: Some(estate.slot_mut(slot_id)),
                        inner: None,
                        outer: None,
                    };
                    exec_qual(self.bitmapqualorig.as_deref_mut(), &mut slots)?
                };
                estate.ecxt_mut(ecxt).reset();
                if !passes {
                    if let Some(idx) = self.ss.instr_idx {
                        estate.es_instrumentation[idx as usize].nfiltered2 += 1.0;
                    }
                    exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                    continue;
                }
            }
            return Ok(true);
        }
    }
}

/// Fused agg-over-bitmapscan page-batch drive: stage the next page's visible
/// tuples; 0 = exhausted. `node.recheck` carries the page's recheck flag.
pub fn bitmap_scan_next_pagebatch<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    debug_assert!(node.initialized, "pagebatch before bitmap_table_scan_setup");
    check_for_interrupts()?;
    let _ = estate;
    let scandesc = node
        .ss
        .ss_currentScanDesc
        .as_mut()
        .expect("bitmap heap scan without a table scan descriptor");
    let tbm = node.tbm.as_ref().expect("bitmap heap scan without a bitmap");
    ::tableam::table_scan_bitmap_next_pagebatch(
        scandesc,
        tbm,
        &mut node.tbmiterator,
        &mut node.recheck,
        &mut node.stats_lossy_pages,
        &mut node.stats_exact_pages,
    )
}

/// Store staged tuple `i` and apply the page's recheck qual; false = filtered.
#[inline(always)]
pub fn bitmap_scan_batch_fetch<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let slot_id = node.ss.ss_ScanTupleSlot;
    let scandesc = node
        .ss
        .ss_currentScanDesc
        .as_mut()
        .expect("bitmap heap scan without a table scan descriptor");
    ::tableam::table_scan_bitmap_batch_store_slot(mcx, scandesc, i, estate.slot_mut(slot_id));
    if !node.recheck {
        return Ok(true);
    }
    let ecxt = node.ss.ps_ExprContext;
    estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot_id);
    let passes = {
        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
        if let Some(q) = node.bitmapqualorig.as_deref_mut() {
            // SAFETY: reset-only context, outlives the plan.
            unsafe { q.arm_result_mcx_raw(per_tuple) };
        }
        let mut slots = EvalSlots {
            scan: Some(estate.slot_mut(slot_id)),
            inner: None,
            outer: None,
        };
        exec_qual(node.bitmapqualorig.as_deref_mut(), &mut slots)?
    };
    estate.ecxt_mut(ecxt).reset();
    if !passes {
        exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
        return Ok(false);
    }
    Ok(true)
}

/// `BitmapTableScanSetup` minus MultiExec: the dispatcher passes the bitmap.
pub fn bitmap_table_scan_setup<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tbm: TIDBitmap<'mcx>,
) -> PgResult<()> {
    node.tbm = Some(tbm);
    let iter = node.tbm.as_mut().expect("just set").begin_private_iterate()?;
    node.tbmiterator = TbmIterator::private(iter);

    if node.ss.ss_currentScanDesc.is_none() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("bitmap heap scan requires es_snapshot");
        node.ss.ss_currentScanDesc = Some(table_beginscan_bm(
            estate.es_query_cxt,
            node.ss.ss_currentRelation.as_ref().expect("bitmap heap scan has a relation"),
            Some(snapshot),
        )?);
    }
    node.initialized = true;
    Ok(())
}

/// `ExecBitmapHeapScan` body; the dispatcher must have run setup already.
pub fn exec_bitmap_heap_scan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    execscan::exec_scan(node, estate)
}

/// `ExecInitBitmapHeapScan` minus child linkage (the dispatcher inits the
/// bitmapqual subtree from plan.lefttree).
pub fn exec_init_bitmap_heap_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapHeapScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<BitmapHeapScanState<'mcx>> {
    // Decoupled index+heap visits are only sound under MVCC (file-head rule).
    debug_assert!(estate.es_snapshot.as_deref().is_some_and(IsMVCCSnapshot));

    let rel = estate
        .exec_get_range_table_relation(node.scan.scanrelid, false)?
        .alias();
    exec_init_bitmap_heap_scan_rel(mcx, node, estate, rel)
}

/// C divergence: init over a caller-opened relation (nodeindexscan precedent).
pub fn exec_init_bitmap_heap_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapHeapScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<BitmapHeapScanState<'mcx>> {
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
    let bitmapqualorig = exec_init_qual(mcx, &node.bitmapqualorig, params)?;

    Ok(BitmapHeapScanState {
        ss,
        bitmapqualorig,
        tbm: None,
        tbmiterator: TbmIterator::empty(),
        initialized: false,
        recheck: true,
        stats_exact_pages: 0,
        stats_lossy_pages: 0,
    })
}

/// `ExecEndBitmapHeapScan` node-local half; the caller ends the bitmapqual
/// subtree.
pub fn exec_end_bitmap_heap_scan(node: &mut BitmapHeapScanState<'_>) -> PgResult<()> {
    node.tbmiterator.end_iterate();
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.tbm = None;
    node.bitmapqualorig = None;
    Ok(())
}

/// `ExecReScanBitmapHeapScan` node-local half; the caller rescans the
/// bitmapqual subtree after.
pub fn exec_rescan_bitmap_heap_scan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.tbmiterator.end_iterate();
    if let Some(scandesc) = node.ss.ss_currentScanDesc.as_mut() {
        table_rescan(estate.es_query_cxt, scandesc, None)?;
    }
    node.tbm = None;
    node.initialized = false;
    node.recheck = true;
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

#[inline(always)]
fn check_for_interrupts() -> types_error::PgResult<()> {
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

// Exempt: bitmapqualorig is released in exec_end_bitmap_heap_scan.
mcx::forget_safe_struct!(
    BitmapHeapScanState<'_> { ss, tbm, tbmiterator, initialized, recheck,
        stats_exact_pages, stats_lossy_pages; bitmapqualorig },
);
