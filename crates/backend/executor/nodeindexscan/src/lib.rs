// nodeIndexscan.c: Var-op-Const quals become ScanKeys at init (rule 5);
// runtime keys (indexkey op expression, incl. SK_SEARCHARRAY arrays)
// re-evaluate into the same ScanKeys at rescan; RowCompare quals build a
// SK_ROW_HEADER key over a state-owned subkey array (Const members only —
// runtime row members loud-panic). Non-amsearcharray array keys loud-panic
// pending their lanes. EPQ arms loud-panic pending EPQState.
#![allow(non_snake_case)]

extern crate alloc;

use ::datum::Datum;
use ::execexpr::{
    exec_eval_expr, EvalSlots, ExprState, ParamBind,
    INDEX_VAR,
};
use ::execscan::{ScanNode, ScanState};
use ::executils::{exec_recheck_qual_and_reset, EStateData, EcxtId, ExecSlotId};
use ::heaptuple::HeapTuple;
use ::indexam::{
    index_beginscan, index_close, index_endscan, index_getnext_slot, index_getnext_tid,
    index_markpos, index_rescan, index_restrpos, IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::pairingheap::PairingHeap;
use ::tableam::table_slot_callbacks;
use ::tuplesort::{apply_cmp, prepare_sort_support_from_ordering_op, SortSupport, SortSupportInit};
use ::types_error::{PgError, PgResult};
use ::types_nodes::list::NodeList;
use ::types_nodes::plannodes::IndexScan;
use ::types_nodes::NodeTag;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::{
    ScanKeyData, StrategyNumber, SK_ISNULL, SK_ORDER_BY, SK_ROW_END, SK_ROW_HEADER,
    SK_ROW_MEMBER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct IndexRuntimeKeyInfo<'mcx> {
    pub scan_key: usize,
    // Which key array scan_key indexes: quals or ORDER BY keys (C stores a
    // pointer into the respective array).
    pub orderby: bool,
    pub key_expr: PgBox<'mcx, ExprState<'mcx>>,
    pub key_toastable: bool,
}

pub struct RuntimeKeysState<'mcx> {
    pub keys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    pub ready: bool,
    pub ecxt: EcxtId,
}

// nodeIndexscan.c ReorderTuple: heap copy + datumCopy'd distances, allocated
// in the query context, live until popped.
pub struct ReorderTuple<'mcx> {
    htup: HeapTuple<'mcx>,
    orderbyvals: PgVec<'mcx, Datum>,
    orderbynulls: PgVec<'mcx, bool>,
}

type ReorderCmp<'mcx> = Box<dyn Fn(&ReorderTuple<'mcx>, &ReorderTuple<'mcx>) -> i32 + 'mcx>;

/// ORDER BY (amcanorderbyop) scan state: C's iss_OrderByKeys/iss_SortSupport/
/// iss_OrderByTypByVals/iss_OrderByTypLens/iss_OrderByValues/iss_OrderByNulls/
/// iss_ReorderQueue/iss_ReachedEnd, boxed as one arm.
pub struct OrderByState<'mcx> {
    pub keys: PgVec<'mcx, ScanKeyData>,
    pub orderbyorig: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    pub sort_support: PgVec<'mcx, SortSupport>,
    pub typbyvals: PgVec<'mcx, bool>,
    pub typlens: PgVec<'mcx, i16>,
    pub values: PgVec<'mcx, Datum>,
    pub nulls: PgVec<'mcx, bool>,
    pub queue: PairingHeap<ReorderTuple<'mcx>, ReorderCmp<'mcx>>,
    pub reached_end: bool,
}

pub struct IndexScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub indexqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub iss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub iss_RelationDesc: Option<Relation<'mcx>>,
    pub iss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub iss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
    pub iss_OrderBy: Option<PgBox<'mcx, OrderByState<'mcx>>>,
    pub iss_OrderDir: ScanDirection,
    pub iss_PlanNodeId: i32,
    pub iss_ParallelAware: bool,
    // Plan's indexid, kept for skeleton re-open (iss_RelationDesc is closed
    // while parked).
    pub iss_IndexOid: ::types_core::Oid,
    // Lane-executor-v2 (`execmain::lanev2`): forward, non-mark eflags at init.
    // False for a mergejoin-mark-armed scan (the scroll/backward eflags producer retired with the backward-execution wave, B2) — the lane's
    // sequential tidrun drive can't survive backward fetch or mark/restore, so
    // it refuses these. Default false (refuse); set by `exec_init_index_scan`.
    batch_allowed: bool,
    // Lane-executor-v2 tidrun cursor `(pos, n)` over the currently-staged
    // same-block TID run; stored across the Volcano per-call boundary. The
    // drive lives in the `lanev2` module. Reset on rescan/park.
    lane_pos: u32,
    lane_n: u32,
}

/// `cmp_orderbyvals` (nodeIndexscan.c): raw ssup comparator, NULLS LAST only
/// (match_pathkeys_to_index builds nothing else).
fn cmp_orderbyvals(
    adist: &[Datum],
    anulls: &[bool],
    bdist: &[Datum],
    bnulls: &[bool],
    sort_support: &[SortSupport],
) -> i32 {
    for (i, ssup) in sort_support.iter().enumerate() {
        match (anulls[i], bnulls[i]) {
            (true, false) => return 1,
            (false, true) => return -1,
            (true, true) => continue,
            (false, false) => {}
        }
        let result = apply_cmp(ssup.comparator, adist[i], bdist[i]);
        if result != 0 {
            return result;
        }
    }
    0
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
        exec_recheck_qual_and_reset(self.indexqualorig.as_deref_mut(), estate, ecxt, slot)
    }

    /// `IndexNext`; `IndexNextWithReorder` when ORDER BY keys exist (C
    /// ExecIndexScan dispatches on iss_NumOrderByKeys > 0).
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        if self.iss_OrderBy.is_some() {
            return self.index_next_with_reorder(estate);
        }
        let mcx = estate.es_query_cxt;
        // Backward-execution wave B8: C's ScanDirectionCombine(es_direction,
        // indexorderdir) narrows to indexorderdir alone - es_direction is
        // forward-invariant below the run seam (deletion-prep B1), and
        // Forward is the combine's identity. indexorderdir KEEPS its
        // backward value: planner DESC index scans stay (C nodeIndexscan.c
        // ExecIndexScan combines; ratified strategy divergence, Michael's
        // 2026-07-17 SCROLL/WITH-HOLD decision).
        debug_assert!(
            ::types_scan::sdir::ScanDirectionIsForward(estate.es_direction),
            "backward drive below the forward-only run seam (deletion-prep B1)"
        );
        let direction = self.iss_OrderDir;

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
                let passes = exec_recheck_qual_and_reset(
                    self.indexqualorig.as_deref_mut(),
                    estate,
                    ecxt,
                    slot_id,
                )?;
                if !passes {
                    continue;
                }
            }
            return Ok(true);
        }
    }
}

impl<'mcx> IndexScanState<'mcx> {
    /// Lane-executor-v2: forward, non-mark eflags at init (false for a
    /// mergejoin-mark-armed scan (the scroll/backward eflags producer retired with the backward-execution wave, B2)).
    #[inline]
    pub fn batch_allowed(&self) -> bool {
        self.batch_allowed
    }

    /// Lane-executor-v2 tidrun cursor `(pos, n)`; the drive lives in `lanev2`,
    /// this only stores its position across the Volcano per-call boundary.
    #[inline]
    pub fn lane_cursor(&self) -> (u32, u32) {
        (self.lane_pos, self.lane_n)
    }

    #[inline]
    pub fn set_lane_cursor(&mut self, pos: u32, n: u32) {
        self.lane_pos = pos;
        self.lane_n = n;
    }

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
            self.iss_OrderBy.as_deref().map_or(0, |ob| ob.keys.len() as i32),
        )?;
        if self.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
            index_rescan(
                &mut scandesc,
                Some(&self.iss_ScanKeys),
                self.iss_OrderBy.as_deref().map(|ob| &ob.keys[..]),
            )?;
        }
        // C's palloc'd IndexScanDesc: state holds a pointer, not the value.
        self.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
        Ok(())
    }

    /// `IndexNextWithReorder` (nodeIndexscan.c:169): tuples whose index-
    /// reported distance was inexact (or that arrived behind a smaller queued
    /// tuple) go through the pairing-heap reorder queue.
    fn index_next_with_reorder(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        // C asserts: reordering supports forward scans only (no AM has both
        // amcanorderbyop and amcanbackward).
        debug_assert!(!matches!(self.iss_OrderDir, ScanDirection::BackwardScanDirection));
        debug_assert!(matches!(
            estate.es_direction,
            ScanDirection::ForwardScanDirection
        ));

        if self.iss_ScanDesc.is_none() {
            self.open_scandesc(estate)?;
        }
        let slot_id = self.ss.ss_ScanTupleSlot;
        let ecxt = self.ss.ps_ExprContext;
        let plan_node_id = self.iss_PlanNodeId;
        let IndexScanState {
            iss_ScanDesc,
            iss_OrderBy,
            indexqualorig,
            ..
        } = self;
        // SAFETY: written by open_scandesc when None.
        let scandesc = unsafe { iss_ScanDesc.as_deref_mut().unwrap_unchecked() };
        let ob = iss_OrderBy.as_deref_mut().expect("reorder path has ORDER BY state");

        loop {
            check_for_interrupts()?;

            // Return the queue top if it sorts at or before the last
            // index-returned distance (or the index is exhausted).
            if let Some(topmost) = ob.queue.first() {
                if ob.reached_end
                    || cmp_orderbyvals(
                        &topmost.orderbyvals,
                        &topmost.orderbynulls,
                        &scandesc.xs_orderbyvals,
                        &scandesc.xs_orderbynulls,
                        &ob.sort_support,
                    ) <= 0
                {
                    let rt = ob.queue.remove_first().expect("non-empty queue");
                    exectuples::exec_force_store_heap_tuple_owned(
                        rt.htup,
                        estate.slot_mut(slot_id),
                        mcx,
                    )?;
                    return Ok(true);
                }
            } else if ob.reached_end {
                exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                return Ok(false);
            }

            // next_indextuple: fetch, rechecking lossy index quals.
            let fetched = loop {
                if !index_getnext_slot(
                    mcx,
                    scandesc,
                    ScanDirection::ForwardScanDirection,
                    estate.slot_mut(slot_id),
                )? {
                    break false;
                }
                if estate.es_instrument != 0 {
                    let n = scandesc.xs_nsearches;
                    estate.instr_set_index_nsearches(plan_node_id, n);
                }
                if scandesc.xs_recheck {
                    let passes = exec_recheck_qual_and_reset(
                        indexqualorig.as_deref_mut(),
                        estate,
                        ecxt,
                        slot_id,
                    )?;
                    if !passes {
                        check_for_interrupts()?;
                        continue;
                    }
                }
                break true;
            };
            if !fetched {
                // Index exhausted; drain the queue.
                ob.reached_end = true;
                continue;
            }

            // Recompute distances from the heap tuple when the AM's were
            // lower-bound estimates (xs_recheckorderby).
            let (was_exact, use_node_vals) = if scandesc.xs_recheckorderby {
                estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot_id);
                estate.ecxt_mut(ecxt).reset();
                // EvalOrderByExpressions: values land in per-tuple memory,
                // datumCopy'd below if queued.
                for (i, expr) in ob.orderbyorig.iter_mut().enumerate() {
                    // SAFETY: the per-tuple context object outlives the plan
                    // (reset-only).
                    unsafe { expr.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
                    let nd = if expr.has_subplan() {
                        // C ExecEvalExpr recurses into ExecEvalSubPlan; pump
                        // the EEOP_SUBPLAN suspension through the estate's
                        // subplan driver instead (ecxt_scantuple bound above).
                        ::executils::exec_eval_expr_with_subplans(expr, estate, ecxt)?
                    } else {
                        let mut slots = EvalSlots {
                            scan: Some(estate.slot_mut(slot_id)),
                            inner: None,
                            outer: None,
                        };
                        exec_eval_expr(expr, &mut slots)?
                    };
                    ob.values[i] = nd.value;
                    ob.nulls[i] = nd.isnull;
                }
                let cmp = cmp_orderbyvals(
                    &ob.values,
                    &ob.nulls,
                    &scandesc.xs_orderbyvals,
                    &scandesc.xs_orderbynulls,
                    &ob.sort_support,
                );
                if cmp < 0 {
                    return Err(Box::new(PgError::error(
                        "index returned tuples in wrong order",
                    )));
                }
                (cmp == 0, true)
            } else {
                (true, false)
            };

            let needs_queue = !was_exact || {
                match ob.queue.first() {
                    Some(topmost) => {
                        let (lv, ln): (&[Datum], &[bool]) = if use_node_vals {
                            (&ob.values, &ob.nulls)
                        } else {
                            (&scandesc.xs_orderbyvals, &scandesc.xs_orderbynulls)
                        };
                        cmp_orderbyvals(
                            lv,
                            ln,
                            &topmost.orderbyvals,
                            &topmost.orderbynulls,
                            &ob.sort_support,
                        ) > 0
                    }
                    None => false,
                }
            };
            if !needs_queue {
                return Ok(true);
            }

            // reorderqueue_push: heap copy + datumCopy'd distances into the
            // query context.
            let rt = {
                let htup =
                    exectuples::exec_copy_slot_heap_tuple(estate.slot_mut(slot_id), mcx, mcx)?;
                let (vals, nulls): (&[Datum], &[bool]) = if use_node_vals {
                    (&ob.values, &ob.nulls)
                } else {
                    (&scandesc.xs_orderbyvals, &scandesc.xs_orderbynulls)
                };
                let n = ob.sort_support.len();
                let mut orderbyvals: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
                let mut orderbynulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
                for i in 0..n {
                    if nulls[i] {
                        orderbyvals.push(Datum::null());
                    } else {
                        orderbyvals.push(::adt_scalar::datum_copy(
                            mcx,
                            vals[i],
                            ob.typbyvals[i],
                            ob.typlens[i],
                        )?);
                    }
                    orderbynulls.push(nulls[i]);
                }
                ReorderTuple {
                    htup,
                    orderbyvals,
                    orderbynulls,
                }
            };
            ob.queue.add(rt);
        }
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
    // B8: es_direction combine narrowed to indexorderdir (see scan_next).
    let direction = node.iss_OrderDir;
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
    // B8: es_direction combine narrowed to indexorderdir (see scan_next).
    let direction = node.iss_OrderDir;
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
    eflags: i32,
) -> PgResult<IndexScanState<'mcx>> {
    let rel = estate
        .exec_get_range_table_relation(node.scan.scanrelid, false)?
        .alias();
    let index_rel = indexam::index_open(mcx, node.indexid, index_lockmode(estate, node.scan.scanrelid))?;
    let mut state = exec_init_index_scan_rel(mcx, node, estate, rel, index_rel)?;
    // Lane-executor-v2: the batched tidrun drive is forward-only and can't
    // survive mark/restore, so forbid it for a mark-armed (B2 retired the scroll-eflags producer) or
    // mergejoin-mark cursor. Byte-identity-safe (the lane just refuses).
    state.batch_allowed = eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0;
    Ok(state)
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
    let (qual, indexqualorig, iss_ScanKeys, iss_OrderBy, runtime_keys) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let qual = ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)?;
            let indexqualorig =
                ::execexpr::exec_init_qual_subplans(mcx, &node.indexqualorig, params, env)?;
            let mut runtime_keys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>> = PgVec::new_in(mcx);
            let scan_keys = exec_index_build_scan_keys(
                mcx, &index_rel, &node.indexqual, params, false, &mut runtime_keys, env,
            )?;
            // ORDER BY exprs become scankeys the same way (SK_ORDER_BY).
            let orderby_keys = exec_index_build_scan_keys(
                mcx, &index_rel, &node.indexorderby, params, true, &mut runtime_keys, env,
            )?;
            // orderbyorig re-evaluation (xs_recheckorderby) can carry the
            // same SubPlans the runtime keys do — compile under the env.
            let orderby = if orderby_keys.is_empty() {
                None
            } else {
                Some(::mcx::alloc_in(
                    mcx,
                    init_orderby_state(mcx, node, params, orderby_keys, env)?,
                )?)
            };
            Ok((qual, indexqualorig, scan_keys, orderby, runtime_keys))
        })?;
    ss.qual = qual;
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
        iss_IndexOid: index_rel.rd_id,
        iss_RelationDesc: Some(index_rel),
        iss_ScanKeys,
        iss_Runtime,
        iss_OrderBy,
        iss_OrderDir: order_dir(node.indexorderdir),
        iss_PlanNodeId: node.scan.plan.plan_node_id,
        iss_ParallelAware: node.scan.plan.parallel_aware,
        // Default refuse; `exec_init_index_scan` sets it from eflags. Direct
        // `_rel` callers (tests) keep the lane disarmed.
        batch_allowed: false,
        lane_pos: 0,
        lane_n: 0,
    })
}

/// ExecInitIndexScan's ORDER BY section (nodeIndexscan.c:1016-1070): sort
/// support from indexorderbyops, type len/byval from the original exprs, the
/// reorder pairing heap (reorderqueue_cmp inverts cmp_orderbyvals — the heap
/// surfaces its greatest element, KNN wants ascending).
fn init_orderby_state<'mcx>(
    mcx: Mcx<'mcx>,
    node: &IndexScan<'mcx>,
    params: ParamBind<'mcx>,
    orderby_keys: PgVec<'mcx, ScanKeyData>,
    sub: Option<::execexpr::SubplanCompileEnv>,
) -> PgResult<OrderByState<'mcx>> {
    let n = orderby_keys.len();
    debug_assert_eq!(n, node.indexorderbyops.len());
    debug_assert_eq!(n, node.indexorderbyorig.len());
    let mut sort_support: PgVec<'mcx, SortSupport> = PgVec::new_in(mcx);
    let mut typbyvals: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    let mut typlens: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut orderbyorig: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
    for (orderbyop, orderbyexpr) in node.indexorderbyops.iter().zip(node.indexorderbyorig.iter()) {
        let init = SortSupportInit {
            ssup_collation: ::nodes_core::node_funcs::expr_collation(orderbyexpr),
            // cmp_orderbyvals supports NULLS LAST only.
            ssup_nulls_first: false,
            ssup_attno: 0,
        };
        sort_support.push(prepare_sort_support_from_ordering_op(orderbyop, &init)?);
        let (typlen, typbyval) =
            lsyscache::get_typlenbyval(::nodes_core::node_funcs::expr_type(orderbyexpr))?;
        typlens.push(typlen);
        typbyvals.push(typbyval);
        orderbyorig.push(
            ::execexpr::exec_init_expr_subplans(mcx, Some(orderbyexpr), params, sub)?
                .expect("orderby expr compiles"),
        );
    }
    let mut values: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
    values.resize(n, Datum::null());
    let mut nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    nulls.resize(n, true);
    let mut cmp_ssup: PgVec<'mcx, SortSupport> = PgVec::new_in(mcx);
    cmp_ssup.extend(sort_support.iter().copied());
    let cmp: ReorderCmp<'mcx> = Box::new(move |a, b| {
        cmp_orderbyvals(
            &b.orderbyvals,
            &b.orderbynulls,
            &a.orderbyvals,
            &a.orderbynulls,
            &cmp_ssup,
        )
    });
    Ok(OrderByState {
        keys: orderby_keys,
        orderbyorig,
        sort_support,
        typbyvals,
        typlens,
        values,
        nulls,
        queue: PairingHeap::new(cmp),
        reached_end: false,
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

// unported: ExecIndexBuildScanKeys legs the planner can still reach raise a
// clean ERRCODE_FEATURE_NOT_SUPPORTED error (plan-init time, safe unwind).
#[track_caller]
#[cold]
#[inline(never)]
fn scankey_case_unported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("index scan over {what} is not yet implemented"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// `ExecIndexBuildScanKeys`, cases 1 (indexkey op Const), 2 (runtime key),
/// 3 (RowCompare over Const members), 4 (amsearcharray ScalarArrayOp, Const
/// or runtime array), and 5 (NullTest). Runtime (non-Const) row members and
/// non-amsearcharray ScalarArrayOp loud-panic (the planner only builds saop
/// index quals on amsearcharray AMs — plancat sets it for btree only).
/// `isorderby` is the ORDER BY (amcanorderbyop) leg: ordering-op strategy
/// lookup + SK_ORDER_BY, cases 1 and 2 only. `runtime_keys` is shared across
/// the indexqual and indexorderby calls (C's resized array). SK_ROW_HEADER
/// keys point into a flat SK_ROW_MEMBER buffer leaked into `mcx`, freed with
/// the query context exactly like C's palloc'd subkey array.
pub fn exec_index_build_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    quals: &NodeList<'mcx>,
    params: ParamBind<'mcx>,
    isorderby: bool,
    runtime_keys: &mut PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    sub: Option<::execexpr::SubplanCompileEnv>,
) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let indnkeyatts = index.indnkeyatts();
    let mut scan_keys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    scan_keys
        .try_reserve_exact(quals.len())
        .map_err(|_| Box::new(mcx.oom(quals.len() * core::mem::size_of::<ScanKeyData>())))?;
    // Row headers point into this flat buffer: size it exactly up front so
    // the member pointers stay stable.
    let n_row_members: usize = quals
        .iter()
        .filter(|q| q.node_tag() == NodeTag::T_RowCompareExpr)
        .map(|q| q.as_row_compare_expr().unwrap().opnos.len())
        .sum();
    let mut row_subkeys: PgVec<'mcx, ScanKeyData> = PgVec::new_in(mcx);
    row_subkeys
        .try_reserve_exact(n_row_members)
        .map_err(|_| Box::new(mcx.oom(n_row_members * core::mem::size_of::<ScanKeyData>())))?;

    for clause in quals.iter() {
        let op = match clause.node_tag() {
            NodeTag::T_OpExpr => clause.as_op_expr().unwrap(),
            NodeTag::T_RowCompareExpr => {
                // (indexkey, indexkey, ...) op (expression, expression, ...):
                // one SK_ROW_HEADER key whose sk_argument points at its
                // SK_ROW_MEMBER run in the state-owned flat subkey buffer
                // (ExecIndexBuildScanKeys).
                let rc = clause.as_row_compare_expr().unwrap();
                let n_members = rc.opnos.len();
                let first_member = row_subkeys.len();
                for i in 0..n_members {
                    let mut leftop = rc.largs.nth(i);
                    if leftop.node_tag() == NodeTag::T_RelabelType {
                        leftop = leftop.as_relabel_type().unwrap().arg;
                    }
                    let var = leftop
                        .as_var()
                        .filter(|v| v.varno == INDEX_VAR)
                        .unwrap_or_else(|| panic!("indexqual doesn't have key on left side"));
                    let varattno = var.varattno;
                    if varattno < 1 || varattno as i32 > indnkeyatts {
                        panic!("bogus RowCompare index qualification");
                    }
                    let opfamily = index.rd_opfamily[varattno as usize - 1];
                    let (op_strategy, op_lefttype, op_righttype) =
                        lsyscache::get_op_opfamily_properties(rc.opnos.nth(i), opfamily, false)?;
                    if op_strategy != rc.cmptype {
                        panic!("RowCompare index qualification contains wrong operator");
                    }
                    // BTORDER_PROC: subkeys carry the 3-way comparison proc.
                    let opfuncid = lsyscache::get_opfamily_proc(
                        opfamily,
                        op_lefttype,
                        op_righttype,
                        1,
                    )?;
                    assert!(
                        opfuncid != 0,
                        "missing support function 1({op_lefttype},{op_righttype}) in opfamily {opfamily}"
                    );

                    let mut rightop = rc.rargs.nth(i);
                    if rightop.node_tag() == NodeTag::T_RelabelType {
                        rightop = rightop.as_relabel_type().unwrap().arg;
                    }
                    let (flags, scanvalue) = match rightop.as_const() {
                        Some(con) => (
                            SK_ROW_MEMBER | if con.constisnull { SK_ISNULL } else { 0 },
                            con.constvalue,
                        ),
                        // C treats a non-Const member as a runtime key
                        // targeting the subkey; the runtime-key table here
                        // addresses top-level keys only.
                        None => {
                            return Err(scankey_case_unported(
                                "a row comparison with a non-constant member",
                            ))
                        }
                    };

                    let mut sub = ScanKeyData::empty();
                    sub.sk_flags = flags;
                    sub.sk_attno = varattno;
                    sub.sk_strategy = op_strategy as StrategyNumber;
                    sub.sk_subtype = op_righttype;
                    sub.sk_collation = rc.inputcollids.nth(i);
                    fmgr_core::fmgr_info_into(opfuncid, &mut sub.sk_func)?;
                    sub.sk_argument = scanvalue;
                    row_subkeys.push(sub);
                }
                row_subkeys.last_mut().expect("nonempty row").sk_flags |= SK_ROW_END;

                // Buffer-wide provenance: subkey walks step across members.
                let first_sub = unsafe { row_subkeys.as_mut_ptr().add(first_member) };
                let mut key = ScanKeyData::empty();
                key.sk_flags = SK_ROW_HEADER;
                key.sk_attno = row_subkeys[first_member].sk_attno;
                key.sk_strategy = rc.cmptype as StrategyNumber;
                // sk_subtype/sk_collation/sk_func unused in a header.
                key.sk_argument = ::datum::Datum::from_usize(first_sub as usize);
                scan_keys.push(key);
                continue;
            }
            NodeTag::T_ScalarArrayOpExpr => {
                let saop = clause.as_scalar_array_op_expr().unwrap();
                debug_assert!(!isorderby);
                debug_assert!(saop.useOr);
                if !::indexam::IndexAmKind::from_relam(index.rd_rel.relam).amsearcharray() {
                    return Err(scankey_case_unported(
                        "a scalar-array qual on a non-amsearcharray access method",
                    ));
                }
                let leftop = saop.args.nth(0);
                if leftop.node_tag() == NodeTag::T_RelabelType {
                    return Err(scankey_case_unported(
                        "a binary-compatible (RelabelType) scalar-array index key",
                    ));
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
                            orderby: false,
                            key_expr: ::execexpr::exec_init_expr_subplans(mcx, Some(rightop), params, sub)?
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
                debug_assert!(!isorderby);
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

        // Strategy lookup cross-checks that the operator matches the index
        // (ordering operators live in a different amop shelf: isorderby).
        let opfamily = index.rd_opfamily[varattno as usize - 1];
        let (op_strategy, _op_lefttype, op_righttype) =
            lsyscache::get_op_opfamily_properties(op.opno, opfamily, isorderby)?;
        let orderby_flag = if isorderby { SK_ORDER_BY } else { 0 };

        let mut rightop = rightop.unwrap_or_else(|| panic!("indexqual OpExpr missing right arg"));
        if rightop.node_tag() == NodeTag::T_RelabelType {
            rightop = rightop.as_relabel_type().unwrap().arg;
        }
        let (flags, scanvalue) = match rightop.as_const() {
            Some(con) => (
                orderby_flag | if con.constisnull { SK_ISNULL } else { 0 },
                con.constvalue,
            ),
            None => {
                runtime_keys.push(IndexRuntimeKeyInfo {
                    scan_key: scan_keys.len(),
                    orderby: isorderby,
                    key_expr: ::execexpr::exec_init_expr_subplans(mcx, Some(rightop), params, sub)?
                        .expect("runtime key expr compiles"),
                    key_toastable: lsyscache::get_typlen(op_righttype)? == -1,
                });
                (orderby_flag, ::datum::Datum::from_usize(0))
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

    // The header keys hold raw pointers into this buffer: it lives (and is
    // freed) with the query context, like C's palloc'd subkey array.
    row_subkeys.leak();
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
    node.indexqualorig = None;
    node.iss_ScanKeys.clear();
    node.iss_Runtime = None;
    node.iss_OrderBy = None;
    Ok(())
}

/// Executor-skeleton park: release everything per-run (pins/heap fetch/
/// snapshot via index_parkscan, relation pins, runtime-key readiness); the
/// scan descriptor and its AM workspace stay allocated — per-run
/// index_beginscan would grow the parked bump arena without bound. Pairs
/// with `skeleton_rebind`.
pub fn skeleton_park(node: &mut IndexScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.iss_ScanDesc.as_deref_mut() {
        ::indexam::index_parkscan(scandesc)?;
    }
    if let Some(index_rel) = node.iss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.ss.ss_currentRelation = None;
    // Lane-executor-v2: drop any staged tidrun position across the park.
    node.lane_pos = 0;
    node.lane_n = 0;
    if let Some(rt) = node.iss_Runtime.as_deref_mut() {
        rt.ready = false;
    }
    Ok(())
}

/// Executor-skeleton re-arm: re-pin both relations and re-arm the parked
/// scan descriptor for a new execution (fresh snapshot; the exec_re_scan
/// pass that follows runs index_rescan before any fetch).
/// AcquireExecutorLocks covers tables only: the index lock is retaken here
/// with rellockmode, as C's ExecInitIndexScan does per execution
/// (nodeIndexscan.c:977); index_close(NoLock) keeps it to end of transaction.
pub fn skeleton_rebind<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.iss_RelationDesc.is_none());
    let mcx = estate.es_query_cxt;
    let rel = estate
        .exec_get_range_table_relation(node.ss.scanrelid, false)?
        .alias();
    let index_rel =
        indexam::index_open(mcx, node.iss_IndexOid, index_lockmode(estate, node.ss.scanrelid))?;
    if let Some(scandesc) = node.iss_ScanDesc.as_deref_mut() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("skeleton reuse registered a snapshot");
        ::indexam::index_rearmscan(scandesc, &rel, &index_rel, snapshot)?;
    }
    node.ss.ss_currentRelation = Some(rel);
    node.iss_RelationDesc = Some(index_rel);
    Ok(())
}

/// `ExecReScanIndexScan`: runtime keys, reorder-queue flush, index_rescan.
pub fn exec_rescan_index_scan<'mcx>(
    node: &mut IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Lane-executor-v2: the staged tidrun is stale after rescan.
    node.lane_pos = 0;
    node.lane_n = 0;
    if let Some(rt) = node.iss_Runtime.as_deref_mut() {
        estate.reset_expr_context(rt.ecxt);
        let (scan_keys, orderby_keys) = (
            &mut node.iss_ScanKeys,
            node.iss_OrderBy.as_deref_mut().map(|ob| &mut ob.keys),
        );
        exec_index_eval_runtime_keys(
            estate,
            rt.ecxt,
            &mut rt.keys,
            scan_keys,
            orderby_keys.map_or(&mut [][..], |k| &mut k[..]),
        )?;
        rt.ready = true;
    }
    let IndexScanState {
        iss_ScanDesc,
        iss_ScanKeys,
        iss_OrderBy,
        ss,
        ..
    } = node;
    if let Some(ob) = iss_OrderBy.as_deref_mut() {
        // C pops and frees each queued tuple; dropping the slots frees ours.
        ob.queue.reset();
        ob.reached_end = false;
    }
    if let Some(scandesc) = iss_ScanDesc.as_deref_mut() {
        index_rescan(
            scandesc,
            Some(iss_ScanKeys),
            iss_OrderBy.as_deref().map(|ob| &ob.keys[..]),
        )?;
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
/// `orderby_keys` receives the ORDER BY runtime keys (rk.orderby).
pub fn exec_index_eval_runtime_keys<'mcx>(
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
    runtime_keys: &mut [IndexRuntimeKeyInfo<'mcx>],
    scan_keys: &mut [ScanKeyData],
    orderby_keys: &mut [ScanKeyData],
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
        let nd = if rk.key_expr.has_subplan() {
            // A correlated SubPlan pushed into an Index Cond (min-subquery
            // runtime key, HammerDB TPROC-C DELIVERY): C's ExecEvalExpr
            // recurses into ExecEvalSubPlan; the decomposed interpreter must
            // pump the EEOP_SUBPLAN suspension through the estate's subplan
            // driver instead. The runtime econtext carries no scan/inner/
            // outer tuples, matching the plain arm's all-None slots.
            ::executils::exec_eval_expr_with_subplans(&mut rk.key_expr, estate, ecxt)?
        } else {
            let mut slots = EvalSlots {
                scan: None,
                inner: None,
                outer: None,
            };
            exec_eval_expr(&mut rk.key_expr, &mut slots)?
        };
        let key = if rk.orderby {
            &mut orderby_keys[rk.scan_key]
        } else {
            &mut scan_keys[rk.scan_key]
        };
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
        node.iss_OrderBy.as_deref().map_or(0, |ob| ob.keys.len() as i32),
        std::sync::Arc::clone(&pscan),
    )?;
    if node.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(
            &mut scandesc,
            Some(&node.iss_ScanKeys),
            node.iss_OrderBy.as_deref().map(|ob| &ob.keys[..]),
        )?;
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
        node.iss_OrderBy.as_deref().map_or(0, |ob| ob.keys.len() as i32),
        pscan,
    )?;
    if node.iss_Runtime.as_deref().is_none_or(|r| r.ready) {
        index_rescan(
            &mut scandesc,
            Some(&node.iss_ScanKeys),
            node.iss_OrderBy.as_deref().map(|ob| &ob.keys[..]),
        )?;
    }
    debug_assert!(node.iss_ScanDesc.is_none());
    node.iss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    Ok(())
}

// Exempt: droppy owners, all released in exec_end_index_scan; ScanDirection
// is no-drop, const-proven below.
const _: () = assert!(!core::mem::needs_drop::<ScanDirection>());
mcx::forget_safe_struct!(
    IndexScanState<'_> { ss, iss_PlanNodeId, iss_ParallelAware, iss_IndexOid,
        batch_allowed, lane_pos, lane_n;
        indexqualorig, iss_ScanDesc, iss_RelationDesc, iss_ScanKeys, iss_Runtime,
        iss_OrderBy, iss_OrderDir },
);
