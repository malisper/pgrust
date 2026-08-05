// nodeIndexonlyscan.c. StoreIndexTuple's deform loop is C's
// index_deform_tuple; it moves to indextuple.c's unit when that lands.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{ExprState, INDEX_VAR};
use ::execscan::{ScanNode, ScanState};
use ::executils::{exec_recheck_qual_and_reset, EStateData, ExecSlotId};
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
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
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
    // amcanorderbyop scans: the AM returns tuples in distance order; IOS has
    // no reorder queue (lossy distances are an error below).
    pub ioss_OrderByKeys: PgVec<'mcx, ScanKeyData>,
    pub ioss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
    pub ioss_TableSlot: ExecSlotId,
    pub ioss_OrderDir: ScanDirection,
    pub ioss_NameCStringAttNums: PgBox<'mcx, [AttrNumber]>,
    pub ioss_VMBuffer: VmBuffer,
    pub ioss_PlanNodeId: i32,
    pub ioss_ParallelAware: bool,
    // Plan's indexid, kept for skeleton re-open (ioss_RelationDesc is closed
    // while parked).
    pub ioss_IndexOid: ::types_core::Oid,
    // Lane-executor-v2 (`execmain::lanev2`): forward, non-mark eflags at init.
    // False for a mergejoin-mark-armed scan (the scroll/backward eflags producer retired with the backward-execution wave, B2) — the lane
    // refuses those. Default false (refuse); set by `exec_init_index_only_scan`.
    // No page-batch cursor is needed: the drive advances one visible tuple per
    // call (`index_only_scan_batch_next` returns 0 or 1), so it carries no
    // state across the Volcano boundary.
    batch_allowed: bool,
    // Planner row estimate (plan.plan_rows), retained for the lane index
    // source's admission floor (single-executor WS-F; contract Q6: the floor
    // input is the planner estimate — force-plan compatible). Read through
    // `index_only_scan_plan_rows`; never consulted by the executor itself.
    plan_rows: f64,
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
        // Backward-execution wave B8: C's ScanDirectionCombine(es_direction,
        // indexorderdir) narrows to indexorderdir alone - es_direction is
        // forward-invariant below the run seam (deletion-prep B1), and
        // Forward is the combine's identity. indexorderdir KEEPS its
        // backward value: planner DESC index-only scans stay.
        debug_assert!(
            ::types_scan::sdir::ScanDirectionIsForward(estate.es_direction),
            "backward drive below the forward-only run seam (deletion-prep B1)"
        );
        let direction = self.ioss_OrderDir;

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
            check_for_interrupts()?;

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
            // Btree never sets xs_recheck. SubPlan-carrying quals route
            // through the executils subplan driver inside the helper.
            if scandesc.xs_recheck {
                let passes = exec_recheck_qual_and_reset(
                    recheckqual.as_deref_mut(),
                    estate,
                    ecxt,
                    slot_id,
                )?;
                if !passes {
                    continue;
                }
            }

            // C nodeIndexonlyscan.c:237: rechecking ORDER BY distances is
            // unsupported in index-only scans.
            if scandesc.numberOfOrderBys > 0 && scandesc.xs_recheckorderby {
                return Err(lossy_distance_error());
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
    /// Lane-executor-v2: forward, non-mark eflags at init (false for a
    /// mergejoin-mark-armed scan (the scroll/backward eflags producer retired with the backward-execution wave, B2)).
    #[inline]
    pub fn batch_allowed(&self) -> bool {
        self.batch_allowed
    }

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
            self.ioss_OrderByKeys.len() as i32,
        )?;
        scandesc.xs_want_itup = true;
        if self.ioss_Runtime.as_deref().is_none_or(|r| r.ready) {
            index_rescan(
                &mut scandesc,
                Some(&self.ioss_ScanKeys),
                Some(&self.ioss_OrderByKeys),
            )?;
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
    check_for_interrupts()?;
    if node.ioss_ScanDesc.is_none() {
        node.open_scandesc(estate)?;
    }
    let mcx = estate.es_query_cxt;
    // B8: es_direction combine narrowed to indexorderdir (see scan_next).
    let direction = node.ioss_OrderDir;
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

#[track_caller]
#[cold]
#[inline(never)]
fn no_data_returned() -> Box<PgError> {
    Box::new(PgError::error(
        "no data returned for index-only scan".to_string(),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn lossy_distance_error() -> Box<PgError> {
    Box::new(
        PgError::error("lossy distance functions are not supported in index-only scans")
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
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
    eflags: i32,
) -> PgResult<IndexOnlyScanState<'mcx>> {
    let rel = estate
        .exec_get_range_table_relation(node.scan.scanrelid, false)?
        .alias();
    // C nodeIndexonlyscan.c:608: rellockmode unconditionally — a reused generic
    // plan gets no planner locks and AcquireExecutorLocks covers tables only.
    let index_rel = indexam::index_open(
        mcx,
        node.indexid,
        ::nodeindexscan::index_lockmode(estate, node.scan.scanrelid),
    )?;
    let mut state = exec_init_index_only_scan_rel(mcx, node, estate, rel, index_rel)?;
    // Lane-executor-v2: refuse the batched drive for a mark-armed (B2 retired the scroll-eflags producer) or
    // mergejoin-mark cursor. Byte-identity-safe (the lane just refuses).
    state.batch_allowed = eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0;
    Ok(state)
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
    let (qual, recheckqual, ioss_ScanKeys, ioss_OrderByKeys, runtime_keys) =
        ::executils::with_subplan_compile_env(estate, |env| -> ::types_error::PgResult<_> {
            let qual = ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)?;
            let recheckqual =
                ::execexpr::exec_init_qual_subplans(mcx, &node.recheckqual, params, env)?;
            let mut runtime_keys = PgVec::new_in(mcx);
            let scan_keys = exec_index_build_scan_keys(
                mcx, &index_rel, &node.indexqual, params, false, &mut runtime_keys, env,
            )?;
            // ORDER BY exprs become scankeys the same way (SK_ORDER_BY).
            let orderby_keys = exec_index_build_scan_keys(
                mcx, &index_rel, &node.indexorderby, params, true, &mut runtime_keys, env,
            )?;
            Ok((qual, recheckqual, scan_keys, orderby_keys, runtime_keys))
        })?;
    ss.qual = qual;
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
        ioss_IndexOid: index_rel.rd_id,
        ioss_RelationDesc: Some(index_rel),
        ioss_ScanKeys,
        ioss_OrderByKeys,
        ioss_Runtime,
        ioss_TableSlot,
        ioss_OrderDir: order_dir(node.indexorderdir),
        ioss_NameCStringAttNums,
        ioss_VMBuffer: VmBuffer::new(),
        ioss_PlanNodeId: node.scan.plan.plan_node_id,
        ioss_ParallelAware: node.scan.plan.parallel_aware,
        // Default refuse; `exec_init_index_only_scan` sets it from eflags.
        batch_allowed: false,
        plan_rows: node.scan.plan.plan_rows,
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

/// Executor-skeleton park: release everything per-run (VM buffer, pins/heap
/// fetch/snapshot via index_parkscan, relation pins, runtime-key readiness);
/// the scan descriptor and its AM workspace stay allocated — per-run
/// index_beginscan would grow the parked bump arena without bound. Pairs
/// with `skeleton_rebind`.
pub fn skeleton_park(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    node.ioss_VMBuffer.release();
    if let Some(scandesc) = node.ioss_ScanDesc.as_deref_mut() {
        ::indexam::index_parkscan(scandesc)?;
    }
    if let Some(index_rel) = node.ioss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.ss.ss_currentRelation = None;
    if let Some(rt) = node.ioss_Runtime.as_deref_mut() {
        rt.ready = false;
    }
    Ok(())
}

/// Executor-skeleton re-arm: re-pin both relations and re-arm the parked
/// scan descriptor for a new execution (fresh snapshot; the exec_re_scan
/// pass that follows runs index_rescan before any fetch).
/// AcquireExecutorLocks covers tables only: the index lock is retaken here
/// with rellockmode, as C's ExecInitIndexOnlyScan does per execution
/// (nodeIndexonlyscan.c:608); index_close(NoLock) keeps it to end of transaction.
pub fn skeleton_rebind<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.ioss_RelationDesc.is_none());
    let mcx = estate.es_query_cxt;
    let rel = estate
        .exec_get_range_table_relation(node.ss.scanrelid, false)?
        .alias();
    let index_rel = indexam::index_open(
        mcx,
        node.ioss_IndexOid,
        ::nodeindexscan::index_lockmode(estate, node.ss.scanrelid),
    )?;
    if let Some(scandesc) = node.ioss_ScanDesc.as_deref_mut() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("skeleton reuse registered a snapshot");
        ::indexam::index_rearmscan(scandesc, &rel, &index_rel, snapshot)?;
    }
    node.ss.ss_currentRelation = Some(rel);
    node.ioss_RelationDesc = Some(index_rel);
    Ok(())
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
    node.ioss_OrderByKeys.clear();
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
        exec_index_eval_runtime_keys(
            estate,
            rt.ecxt,
            &mut rt.keys,
            &mut node.ioss_ScanKeys,
            &mut node.ioss_OrderByKeys,
        )?;
        rt.ready = true;
    }
    let IndexOnlyScanState {
        ioss_ScanDesc,
        ioss_ScanKeys,
        ioss_OrderByKeys,
        ss,
        ..
    } = node;
    if let Some(scandesc) = ioss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(ioss_ScanKeys), Some(ioss_OrderByKeys))?;
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

/// `ExecIndexOnlyScanEstimate`: no DSM thread-native; the instrument-only arm
/// is covered by execParallel's collapsed per-worker retrieval.
pub fn exec_index_only_scan_estimate(_node: &mut IndexOnlyScanState<'_>) {}

/// `ExecIndexOnlyScanInitializeDSM` (the leader participates too).
pub fn exec_index_only_scan_initialize_dsm<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<std::sync::Arc<::indexam::ParallelIndexScanDescShared>> {
    let mcx = estate.es_query_cxt;
    let heap = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("IOS has a relation");
    let index = node.ioss_RelationDesc.as_ref().expect("index relation open");
    let snapshot = estate
        .es_snapshot
        .as_ref()
        .expect("parallel index-only scan requires es_snapshot");
    let pscan = ::indexam::index_parallelscan_initialize(heap, index, snapshot)?;

    let mut scandesc = ::indexam::index_beginscan_parallel(
        mcx,
        heap,
        index,
        node.ioss_ScanKeys.len() as i32,
        node.ioss_OrderByKeys.len() as i32,
        std::sync::Arc::clone(&pscan),
    )?;
    scandesc.xs_want_itup = true;
    if node.ioss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(
            &mut scandesc,
            Some(&node.ioss_ScanKeys),
            Some(&node.ioss_OrderByKeys),
        )?;
    }
    debug_assert!(node.ioss_ScanDesc.is_none());
    node.ioss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    Ok(pscan)
}

/// `ExecIndexOnlyScanReInitializeDSM`.
pub fn exec_index_only_scan_reinitialize_dsm(node: &mut IndexOnlyScanState<'_>) -> PgResult<()> {
    ::indexam::index_parallelrescan(
        node.ioss_ScanDesc
            .as_deref_mut()
            .expect("parallel index-only scan was initialized"),
    )
}

/// `ExecIndexOnlyScanInitializeWorker`.
pub fn exec_index_only_scan_initialize_worker<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pscan: std::sync::Arc<::indexam::ParallelIndexScanDescShared>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let heap = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("IOS has a relation");
    let index = node.ioss_RelationDesc.as_ref().expect("index relation open");
    let mut scandesc = ::indexam::index_beginscan_parallel(
        mcx,
        heap,
        index,
        node.ioss_ScanKeys.len() as i32,
        node.ioss_OrderByKeys.len() as i32,
        pscan,
    )?;
    scandesc.xs_want_itup = true;
    if node.ioss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(&mut scandesc, Some(&node.ioss_ScanKeys), None)?;
    }
    debug_assert!(node.ioss_ScanDesc.is_none());
    node.ioss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    Ok(())
}

// Exempt: droppy owners, all released in exec_end_index_only_scan;
// ScanDirection is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexOnlyScanState<'_> { ss, ioss_TableSlot, ioss_PlanNodeId, ioss_ParallelAware, ioss_IndexOid,
        batch_allowed, plan_rows;
        recheckqual, ioss_ScanDesc, ioss_RelationDesc, ioss_ScanKeys,
        ioss_OrderByKeys, ioss_NameCStringAttNums, ioss_Runtime, ioss_OrderDir,
        ioss_VMBuffer },
);

// ============================================================================
// Lane index-source seams (single-executor Phase 1, WS-F — APPEND-ONLY).
// Consumed by execmain::lanev2::indexsource's `IndexOnlyScanSource`
// (`BatchGranuleSource` over this node). Nothing below is reached on any
// default path: the callers sit behind `PGRUST_LANE_V2_INDEXSOURCE`
// (default OFF).
// ============================================================================

/// Planner row estimate for the lane admission floor (contract Q6:
/// `plan.rows` is the floor input; force-plan compatible).
#[inline]
pub fn index_only_scan_plan_rows(node: &IndexOnlyScanState<'_>) -> f64 {
    node.plan_rows
}

/// Leaf-page UPPER-BOUND estimate for the lane source's pacing granule map:
/// total blocks of the INDEX relation (>= leaf pages — the metapage and
/// internal pages are counted too). Correctness never rides on this number
/// (pacing only; the drive runs to the AM's own exhaustion), so the smgr
/// snapshot being stale against concurrent extension is fine. `None` = no
/// open index relation (parked skeleton) — the caller refuses, fail-closed.
/// Zero blocks passes through as `Some(0)` (SE-AGGIOS: geometry POLICY —
/// whether the degenerate empty map is covered or refused — lives with the
/// lane's `index_feed_geometry`, not here; this helper purely reports).
pub fn index_only_scan_leaf_estimate(node: &IndexOnlyScanState<'_>) -> PgResult<Option<u64>> {
    let Some(index_rel) = node.ioss_RelationDesc.as_ref() else {
        return Ok(None);
    };
    let nblocks = ::bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        index_rel,
        ::types_core::ForkNumber::MAIN_FORKNUM,
    )?;
    Ok(Some(nblocks as u64))
}

/// End-of-claim slot hygiene for the lane source (ownership ABI R3:
/// zero pins held from the claim at settle). The scan slot is virtual and
/// the table slot was already cleared per heap fallback fetch
/// (`index_only_scan_batch_next`), so both clears are pin-free and
/// idempotent; the VM buffer pin is node-lifetime (like the seq scan's
/// cached pins) and is NOT released here.
pub fn index_only_scan_end_claim<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
    exectuples::exec_clear_tuple(estate.slot_mut(node.ioss_TableSlot), mcx);
}
