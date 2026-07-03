// nodeIndexonlyscan.c. StoreIndexTuple's deform loop is C's
// index_deform_tuple; it moves to indextuple.c's unit when that lands.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_init_qual, exec_qual, EvalSlots, ExprState, INDEX_VAR};
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::indexam::{
    index_beginscan, index_close, index_endscan, index_fetch_heap, index_getnext_tid,
    index_markpos, index_rescan, index_restrpos, IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::nbtree::itup::{index_getattr, ITup};
use ::nodeindexscan::exec_index_build_scan_keys;
use ::tableam::table_slot_callbacks;
use ::types_core::{AttrNumber, CSTRINGOID, NAMEOID};
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::IndexOnlyScan;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::{ScanDirection, ScanDirectionCombine};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::itemptr::ItemPointerGetBlockNumber;
use ::types_tuple::TupleDescData;
use ::visibilitymap::VmBuffer;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct IndexOnlyScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub recheckqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub ioss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub ioss_RelationDesc: Option<Relation<'mcx>>,
    pub ioss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub ioss_TableSlot: ExecSlotId,
    pub ioss_OrderDir: ScanDirection,
    pub ioss_NameCStringAttNums: PgVec<'mcx, AttrNumber>,
    pub ioss_VMBuffer: VmBuffer,
    pub ioss_PlanNodeId: i32,
}

impl<'mcx> ScanNode<'mcx> for IndexOnlyScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `IndexOnlyNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = ScanDirectionCombine(estate.es_direction, self.ioss_OrderDir);

        if self.ioss_ScanDesc.is_none() {
            let snapshot = estate
                .es_snapshot
                .clone()
                .expect("index-only scan requires es_snapshot");
            let mut scandesc = index_beginscan(
                mcx,
                self.ss.ss_currentRelation.as_ref().expect("IOS has a relation"),
                self.ioss_RelationDesc.as_ref().expect("index relation open"),
                snapshot,
                self.ioss_ScanKeys.len() as i32,
                0,
            )?;
            scandesc.xs_want_itup = true;
            // No runtime keys in this lane, so the keys are always ready.
            index_rescan(&mut scandesc, Some(&self.ioss_ScanKeys), None)?;
            // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
            self.ioss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        let table_slot_id = self.ioss_TableSlot;
        let ecxt = self.ss.ps_ExprContext;
        let IndexOnlyScanState {
            ss,
            recheckqual,
            ioss_ScanDesc,
            ioss_VMBuffer,
            ioss_NameCStringAttNums,
            ioss_PlanNodeId,
            ..
        } = self;
        let plan_node_id = *ioss_PlanNodeId;
        loop {
            // SAFETY: written just above when None; single test+branch like
            // C's scandesc == NULL check.
            let scandesc = unsafe { ioss_ScanDesc.as_deref_mut().unwrap_unchecked() };
            let tid = index_getnext_tid(scandesc, direction)?;
            if estate.es_instrument != 0 {
                let n = scandesc.xs_pgstat_index_scans;
                estate.instr_set_index_nsearches(plan_node_id, n);
            }
            let Some(tid) = tid else {
                exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                return Ok(false);
            };
            let mut tuple_from_heap = false;
            check_for_interrupts();

            // Skip the heap fetch when the VM says the TID's page is
            // all-visible; caller-recheck caveats are C's (visibilitymap.c).
            if !::visibilitymap::vm_all_visible(
                ss.ss_currentRelation.as_ref().expect("IOS has a relation"),
                ItemPointerGetBlockNumber(&tid),
                ioss_VMBuffer,
            )? {
                // InstrCountTuples2: EXPLAIN's Heap Fetches.
                if estate.es_instrument != 0 {
                    if let Some(i) =
                        estate.es_instrumentation.get_mut(plan_node_id as usize)
                    {
                        i.ntuples2 += 1.0;
                    }
                }
                if !index_fetch_heap(mcx, scandesc, estate.slot_mut(table_slot_id))? {
                    continue;
                }
                exectuples::exec_clear_tuple(estate.slot_mut(table_slot_id), mcx);
                // Only MVCC snapshots here (no HOT continuation), as C asserts.
                debug_assert!(!scandesc.xs_heap_continue);
                tuple_from_heap = true;
            }

            // xs_hitup arm pending an AM that returns whole heap tuples.
            let Some(itup) = scandesc.xs_itup else {
                return Err(no_data_returned());
            };
            let itupdesc = scandesc
                .xs_itupdesc
                .as_deref()
                .expect("amgettuple published xs_itup without xs_itupdesc");
            // SAFETY: xs_itup points at the AM's page-copy buffer, live until
            // the next amgettuple/amendscan on this descriptor.
            unsafe {
                store_index_tuple(
                    estate.slot_mut(slot_id),
                    mcx,
                    itup.as_ptr(),
                    itupdesc,
                    ioss_NameCStringAttNums,
                )
            };

            // Lossy index: recheck the index quals (ExecQualAndReset shape).
            // Btree never sets xs_recheck.
            if scandesc.xs_recheck {
                estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot_id);
                let passes = {
                    let mut slots = EvalSlots {
                        scan: Some(estate.slot_mut(slot_id)),
                        inner: None,
                        outer: None,
                    };
                    exec_qual(recheckqual.as_deref_mut(), &mut slots)?
                };
                estate.ecxt_mut(ecxt).reset();
                if !passes {
                    continue;
                }
                if scandesc.numberOfOrderBys > 0 {
                    lossy_distance_unported();
                }
            }

            // Index-only predicate locks are page-level: the tuple-level lock
            // taken by the heap fetch is skipped on the VM fast path.
            if !tuple_from_heap {
                let snap = estate
                    .es_snapshot
                    .as_deref()
                    .expect("index-only scan requires es_snapshot");
                predicate_seams::predicate_lock_page::call(
                    ss.ss_currentRelation.as_ref().expect("IOS has a relation"),
                    ItemPointerGetBlockNumber(&tid),
                    snap,
                )?;
            }
            return Ok(true);
        }
    }
}

#[cold]
#[inline(never)]
fn no_data_returned() -> Box<PgError> {
    Box::new(PgError::error(
        "no data returned for index-only scan".to_string(),
    ))
}

#[cold]
#[inline(never)]
fn lossy_distance_unported() -> ! {
    panic!(
        "nodeindexonlyscan: lossy distance recheck ereport (0A000) not ported \
         (indexorderby lane loud-panics at init)"
    )
}

#[cold]
#[inline(never)]
fn interrupt_unported() -> ! {
    panic!("nodeindexonlyscan: ProcessInterrupts (tcop/postgres.c) unported")
}

#[inline(always)]
fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        interrupt_unported();
    }
}

/// `StoreIndexTuple` over btree tuple formats. The deform loop is C's
/// index_deform_tuple; it moves to indextuple.c's unit when that lands.
///
/// # Safety
/// `itup` must be a live, MAXALIGNed index tuple image matching `itupdesc`.
pub unsafe fn store_index_tuple<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    itup: ITup,
    itupdesc: &TupleDescData<'_>,
    name_cstring_attnums: &[AttrNumber],
) {
    debug_assert_eq!(
        slot.base().tts_tupleDescriptor.as_ref().map(|d| d.natts),
        Some(itupdesc.natts)
    );
    exectuples::exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    for attnum in 1..=itupdesc.natts {
        let i = (attnum - 1) as usize;
        let mut isnull = false;
        // SAFETY: attnum in 1..=natts of a matching descriptor; itup live per
        // the function contract.
        let value = unsafe { index_getattr(itup, attnum as AttrNumber, itupdesc, &mut isnull) };
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }
    if !name_cstring_attnums.is_empty() {
        name_cstring_unported();
    }
    exectuples::exec_store_virtual_tuple(slot);
}

#[cold]
#[inline(never)]
fn name_cstring_unported() -> ! {
    panic!(
        "nodeindexonlyscan: name-column cstring-to-Name copy (StoreIndexTuple \
         NAMEDATALEN realloc arm) not ported"
    )
}

/// `ExecIndexOnlyScan`; the runtime-key ExecReScan arm is unreachable (init
/// loud-panics on non-Const quals). IndexOnlyRecheck (the EPQ mtd) is an
/// unconditional C error and lands with EPQState.
pub fn exec_index_only_scan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    execscan::exec_scan(node, estate)
}

/// `ExecInitIndexOnlyScan`; opens both relations through the estate range table.
pub fn exec_init_index_only_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &IndexOnlyScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<IndexOnlyScanState<'mcx>> {
    let rel = estate
        .exec_get_range_table_relation(node.scan.scanrelid, false)?
        .alias();
    // C: lockmode = exec_rt_fetch(scanrelid)->rellockmode, unreachable until
    // the range-table lane lands (the call above panics first).
    let index_rel = indexam::index_open(mcx, node.indexid, NoLock)?;
    exec_init_index_only_scan_rel(mcx, node, estate, rel, index_rel)
}

/// C divergence: init over caller-opened relations, splitting
/// ExecOpenScanRelation/index_open out until the range-table lane lands.
pub fn exec_init_index_only_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &IndexOnlyScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
    index_rel: Relation<'mcx>,
) -> PgResult<IndexOnlyScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let ps_ExprContext = estate.exec_assign_expr_context();

    // Scan type from the planner's indextlist, not the index's physical
    // descriptor (storage types differ, e.g. btree name_ops).
    let tup_desc = execscan::exec_type_from_tl(mcx, &node.indextlist)?;
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(tup_desc.clone()), TupleSlotKind::Virtual);

    let table_kind = table_slot_callbacks(&rel);
    let ioss_TableSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), table_kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
    };
    // ExecAssignScanProjectionInfoWithVarno(INDEX_VAR).
    ss.ps_ProjInfo = execscan::exec_conditional_assign_projection_info(
        mcx,
        estate,
        &node.scan.plan.targetlist,
        INDEX_VAR as u32,
        &tup_desc,
    )?;
    let params = estate.param_bind();
    ss.qual = exec_init_qual(mcx, &node.scan.plan.qual, params)?;
    let recheckqual = exec_init_qual(mcx, &node.recheckqual, params)?;

    if !node.indexorderby.is_nil() {
        orderby_unported();
    }

    let ioss_ScanKeys = exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual)?;
    let ioss_NameCStringAttNums = name_cstring_attnums(mcx, &index_rel)?;

    Ok(IndexOnlyScanState {
        ss,
        recheckqual,
        ioss_ScanDesc: None,
        ioss_RelationDesc: Some(index_rel),
        ioss_ScanKeys,
        ioss_TableSlot,
        ioss_OrderDir: order_dir(node.indexorderdir),
        ioss_NameCStringAttNums,
        ioss_VMBuffer: VmBuffer::new(),
        ioss_PlanNodeId: node.scan.plan.plan_node_id,
    })
}

// Btree name_ops stores cstrings for NAMEOID key columns; StoreIndexTuple
// re-inflates them, so mark those attribute numbers once at init.
fn name_cstring_attnums<'mcx>(
    mcx: Mcx<'mcx>,
    index_rel: &Relation<'mcx>,
) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let mut attnums = PgVec::new_in(mcx);
    let indnkeyatts = index_rel.indnkeyatts();
    for attnum in 0..indnkeyatts as usize {
        if index_rel.rd_att.attrs[attnum].atttypid == CSTRINGOID
            && index_rel.rd_opcintype[attnum] == NAMEOID
        {
            attnums.push(attnum as AttrNumber);
        }
    }
    Ok(attnums)
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
    panic!("nodeindexonlyscan: indexorderby (amcanorderbyop lane) not ported")
}

/// `ExecEndIndexOnlyScan`; the parallel-worker instrumentation copy-back
/// lands with DSM.
pub fn exec_end_index_only_scan(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    node.ioss_VMBuffer.release();
    if let Some(scandesc) = node.ioss_ScanDesc.take() {
        index_endscan(PgBox::into_inner(scandesc))?;
    }
    if let Some(index_rel) = node.ioss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.recheckqual = None;
    node.ioss_ScanKeys.clear();
    Ok(())
}

/// `ExecReScanIndexOnlyScan`; the runtime-key recompute arm is unreachable
/// (that lane loud-panics at init).
pub fn exec_rescan_index_only_scan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let IndexOnlyScanState { ioss_ScanDesc, ioss_ScanKeys, ss, .. } = node;
    if let Some(scandesc) = ioss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(ioss_ScanKeys), None)?;
    }
    execscan::exec_scan_rescan(ss, estate);
    Ok(())
}

/// `ExecIndexOnlyMarkPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_only_mark_pos(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    index_markpos(node.ioss_ScanDesc.as_deref_mut().expect("mark before first fetch"))
}

/// `ExecIndexOnlyRestrPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_only_restr_pos(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    index_restrpos(node.ioss_ScanDesc.as_deref_mut().expect("restore before first fetch"))
}

pub fn exec_index_only_scan_estimate(_node: &mut IndexOnlyScanState<'_>) -> ! {
    panic!("nodeindexonlyscan: ExecIndexOnlyScanEstimate pending parallel DSM/shm_toc")
}

pub fn exec_index_only_scan_initialize_dsm(_node: &mut IndexOnlyScanState<'_>) -> ! {
    panic!("nodeindexonlyscan: ExecIndexOnlyScanInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_index_only_scan_reinitialize_dsm(_node: &mut IndexOnlyScanState<'_>) -> ! {
    panic!("nodeindexonlyscan: ExecIndexOnlyScanReInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_index_only_scan_initialize_worker(_node: &mut IndexOnlyScanState<'_>) -> ! {
    panic!("nodeindexonlyscan: ExecIndexOnlyScanInitializeWorker pending parallel DSM/shm_toc")
}

pub fn exec_index_only_scan_retrieve_instrumentation(_node: &mut IndexOnlyScanState<'_>) -> ! {
    panic!("nodeindexonlyscan: ExecIndexOnlyScanRetrieveInstrumentation pending parallel DSM/shm_toc")
}

// Exempt: droppy owners, all released in exec_end_index_only_scan;
// ScanDirection is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexOnlyScanState<'_> { ss, ioss_TableSlot, ioss_NameCStringAttNums,
        ioss_PlanNodeId;
        recheckqual, ioss_ScanDesc, ioss_RelationDesc, ioss_ScanKeys,
        ioss_OrderDir, ioss_VMBuffer },
);
