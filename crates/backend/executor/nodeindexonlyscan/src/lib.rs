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
use ::mcx::{Allocator, Mcx, PgBox, PgVec};
use ::nbtree::itup::{index_getattr, ITup};
use ::nodeindexscan::{exec_index_build_scan_keys, exec_index_eval_runtime_keys, RuntimeKeysState};
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
    pub ioss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
    pub ioss_TableSlot: ExecSlotId,
    pub ioss_OrderDir: ScanDirection,
    pub ioss_NameCStringAttNums: PgBox<'mcx, [AttrNumber]>,
    pub ioss_VMBuffer: VmBuffer,
    pub ioss_PlanNodeId: i32,
}

impl<'mcx> ScanNode<'mcx> for IndexOnlyScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `IndexOnlyRecheck` (nodeIndexonlyscan.c): always an error.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Err(Box::new(PgError::error(
            "EvalPlanQual recheck is not supported in index-only scans",
        )))
    }

    /// `IndexOnlyNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = ScanDirectionCombine(estate.es_direction, self.ioss_OrderDir);

        if self.ioss_ScanDesc.is_none() {
            self.open_scandesc(estate)?;
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
                let n = scandesc.xs_nsearches;
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
                    if let Some(i) = estate.es_instrumentation.get_mut(plan_node_id as usize) {
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

impl<'mcx> IndexOnlyScanState<'mcx> {
    #[inline(never)]
    fn open_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
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
        if self.ioss_Runtime.as_deref().is_none_or(|r| r.ready) {
            index_rescan(&mut scandesc, Some(&self.ioss_ScanKeys), None)?;
        }
        // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
        self.ioss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        Ok(())
    }
}

/// Fused agg-over-IOS drive: advance to the next VISIBLE index tuple (VM
/// probe first, heap fetch only on a cleared bit — C's IndexOnlyNext order);
/// 1 = xs_itup staged, 0 = exhausted. Page-level predicate lock on the VM
/// fast path is taken here so the storeless drain keeps SSI semantics.
pub fn index_only_scan_batch_next<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    check_for_interrupts();
    if node.ioss_ScanDesc.is_none() {
        node.open_scandesc(estate)?;
    }
    let mcx = estate.es_query_cxt;
    let direction = ScanDirectionCombine(estate.es_direction, node.ioss_OrderDir);
    let table_slot_id = node.ioss_TableSlot;
    let IndexOnlyScanState { ss, ioss_ScanDesc, ioss_VMBuffer, .. } = node;
    loop {
        // SAFETY: written by open_scandesc when None.
        let scandesc = unsafe { ioss_ScanDesc.as_deref_mut().unwrap_unchecked() };
        let Some(tid) = index_getnext_tid(scandesc, direction)? else {
            return Ok(0);
        };
        if !::visibilitymap::vm_all_visible(
            ss.ss_currentRelation.as_ref().expect("IOS has a relation"),
            ItemPointerGetBlockNumber(&tid),
            ioss_VMBuffer,
        )? {
            if !index_fetch_heap(mcx, scandesc, estate.slot_mut(table_slot_id))? {
                continue;
            }
            exectuples::exec_clear_tuple(estate.slot_mut(table_slot_id), mcx);
            // Only MVCC snapshots here (no HOT continuation), as C asserts.
            debug_assert!(!scandesc.xs_heap_continue);
        } else {
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
        // Matcher admits btree only; xs_recheck stays false.
        debug_assert!(!scandesc.xs_recheck);
        return Ok(1);
    }
}

/// Store the staged index tuple into the scan slot.
#[inline(always)]
pub fn index_only_scan_batch_store<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let slot_id = node.ss.ss_ScanTupleSlot;
    let scandesc = node.ioss_ScanDesc.as_deref().expect("batch store before batch next");
    let Some(itup) = scandesc.xs_itup else {
        return Err(no_data_returned());
    };
    let itupdesc = scandesc
        .xs_itupdesc
        .as_deref()
        .expect("amgettuple published xs_itup without xs_itupdesc");
    // SAFETY: xs_itup points at the AM's page-copy buffer, live until the
    // next amgettuple/amendscan on this descriptor.
    unsafe {
        store_index_tuple(
            estate.slot_mut(slot_id),
            mcx,
            itup.as_ptr(),
            itupdesc,
            &node.ioss_NameCStringAttNums,
        )
    };
    Ok(true)
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
    // C's cstring-to-NAME realloc: btree name_ops stores names as cstrings
    // in index tuples; pad back to a NAMEDATALEN block for the slot.
    for &attnum in name_cstring_attnums {
        // name_cstring_attnums stores 0-based column indexes.
        let i = attnum as usize;
        if base.tts_isnull[i] {
            continue;
        }
        const NAMEDATALEN: usize = 64;
        let layout = core::alloc::Layout::from_size_align(NAMEDATALEN, 4).expect("name layout");
        let Ok(block) = mcx.allocate(layout) else {
            mcx.oom(NAMEDATALEN);
            unreachable!()
        };
        let dst = block.cast::<u8>().as_ptr();
        let src = base.tts_values[i].as_usize() as *const u8;
        // SAFETY: src is a NUL-terminated cstring from the index tuple; dst
        // is a fresh NAMEDATALEN block. namestrcpy truncation semantics.
        unsafe {
            core::ptr::write_bytes(dst, 0, NAMEDATALEN);
            let mut n = 0usize;
            while n < NAMEDATALEN - 1 && *src.add(n) != 0 {
                *dst.add(n) = *src.add(n);
                n += 1;
            }
        }
        base.tts_values[i] = ::datum::Datum::from_usize(dst as usize);
    }
    exectuples::exec_store_virtual_tuple(slot);
}

/// `ExecIndexOnlyScan`; IndexOnlyRecheck (the EPQ mtd) is an unconditional C
/// error and lands with EPQState.
pub fn exec_index_only_scan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if node.ioss_Runtime.as_deref().is_some_and(|r| !r.ready) {
        exec_rescan_index_only_scan(node, estate)?;
    }
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
        instr_idx: None,
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
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;
    let recheckqual = exec_init_qual(mcx, &node.recheckqual, params)?;

    if !node.indexorderby.is_nil() {
        orderby_unported();
    }

    let (ioss_ScanKeys, runtime_keys) =
        exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual, params)?;
    let ioss_Runtime = if runtime_keys.is_empty() {
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
    let ioss_NameCStringAttNums = name_cstring_attnums(mcx, &index_rel)?;

    Ok(IndexOnlyScanState {
        ss,
        recheckqual,
        ioss_ScanDesc: None,
        ioss_RelationDesc: Some(index_rel),
        ioss_ScanKeys,
        ioss_Runtime,
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
) -> PgResult<PgBox<'mcx, [AttrNumber]>> {
    let mut attnums = PgVec::new_in(mcx);
    let indnkeyatts = index_rel.indnkeyatts();
    for attnum in 0..indnkeyatts as usize {
        if index_rel.rd_att.attrs[attnum].atttypid == CSTRINGOID
            && index_rel.rd_opcintype[attnum] == NAMEOID
        {
            attnums.push(attnum as AttrNumber);
        }
    }
    Ok(attnums.into_boxed_slice())
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
    node.ioss_Runtime = None;
    Ok(())
}

/// `ExecReScanIndexOnlyScan`.
pub fn exec_rescan_index_only_scan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(rt) = node.ioss_Runtime.as_deref_mut() {
        estate.reset_expr_context(rt.ecxt);
        exec_index_eval_runtime_keys(estate, rt.ecxt, &mut rt.keys, &mut node.ioss_ScanKeys)?;
        rt.ready = true;
    }
    let IndexOnlyScanState {
        ioss_ScanDesc,
        ioss_ScanKeys,
        ss,
        ..
    } = node;
    if let Some(scandesc) = ioss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(ioss_ScanKeys), None)?;
    }
    execscan::exec_scan_rescan(ss, estate);
    Ok(())
}

/// `ExecIndexOnlyMarkPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_only_mark_pos(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    index_markpos(
        node.ioss_ScanDesc
            .as_deref_mut()
            .expect("mark before first fetch"),
    )
}

/// `ExecIndexOnlyRestrPos`; the EPQ arm lands with execMain's EPQState.
pub fn exec_index_only_restr_pos(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    index_restrpos(
        node.ioss_ScanDesc
            .as_deref_mut()
            .expect("restore before first fetch"),
    )
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
    panic!(
        "nodeindexonlyscan: ExecIndexOnlyScanRetrieveInstrumentation pending parallel DSM/shm_toc"
    )
}

// Exempt: droppy owners, all released in exec_end_index_only_scan;
// ScanDirection is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexOnlyScanState<'_> { ss, ioss_TableSlot, ioss_PlanNodeId;
        recheckqual, ioss_ScanDesc, ioss_RelationDesc, ioss_ScanKeys,
        ioss_NameCStringAttNums, ioss_Runtime, ioss_OrderDir, ioss_VMBuffer },
);
