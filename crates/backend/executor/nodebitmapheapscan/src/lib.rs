// nodeBitmapHeapscan.c. The bitmapqual child subtree stays with execmain's
// dispatcher (nodesort precedent): it runs MultiExec (parallel: only in the
// worker that wins BM_INITIAL) and hands the TIDBitmap to
// bitmap_table_scan_setup.
#![allow(non_snake_case)]

use std::sync::Arc;

use pgsync::Mutex;

use ::execexpr::ExprState;
use ::executils::exec_recheck_qual_and_reset;
use ::execscan::{ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox};
use ::tableam::{table_beginscan_bm, table_endscan, table_rescan, table_slot_callbacks};
use ::tidbitmap::{TbmIterator, TbmRangeIterator, TbmSharedIterator, TbmSharedIterState, TIDBitmap};
use ::types_error::PgResult;
use ::types_nodes::plannodes::BitmapHeapScan;
use ::types_rel::Relation;
use ::types_snapshot::IsMVCCSnapshot;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SharedBitmapState {
    Initial,
    InProgress,
    Finished,
}

struct BmShared {
    state: SharedBitmapState,
    iterator: Option<Arc<TbmSharedIterState>>,
    /// Packed waiter handles (waiter::WakerHandle words) of parked
    /// build-waiters — the pg_barrier phase pattern (permit-s4 row 3).
    wakers: Vec<u64>,
}

/// C ParallelBitmapHeapState (shm_toc entry -> Arc): the spinlock-guarded
/// state machine plus the dsa_pointer to the shared iterator, folded into one
/// mutex.
///
/// permit-s4 row-3 conversion (dst-p3-scheduler §3): the raw Condvar 10ms
/// poll is replaced by the pg_barrier PHASE PATTERN — a waker-word list in
/// the shared state plus a 10ms `waiter::park_timeout` (the timed park IS
/// C ConditionVariableSleep's per-wakeup InterruptPending cadence, kept);
/// the builder's publish drains the list after releasing the lock and
/// unparks every registrant, so a phase advance wakes waiters promptly and
/// a lost wake costs at most one cadence period in production — and is a
/// loom-caught deadlock in the model, where parks are untimed
/// (`bitmapheap_phase_publish_unparks_registered_waiter`,
/// runtime/tests/loom.rs).
pub struct ParallelBitmapHeapState {
    shared: Mutex<BmShared>,
}

impl Default for ParallelBitmapHeapState {
    fn default() -> Self {
        ParallelBitmapHeapState {
            shared: Mutex::new(BmShared {
                state: SharedBitmapState::Initial,
                iterator: None,
                wakers: Vec::new(),
            }),
        }
    }
}

pub struct BitmapHeapScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    pub bitmapqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub tbm: Option<TIDBitmap<'mcx>>,
    pub tbmiterator: TbmIterator,
    pub initialized: bool,
    pub recheck: bool,
    pub stats_exact_pages: u64,
    pub stats_lossy_pages: u64,
    pub pstate: Option<Arc<ParallelBitmapHeapState>>,
    pub plan_node_id: i32,
    pub parallel_aware: bool,
    // Lane-executor-v2 (`execmain::lanev2`) page-batch cursor `(pos, n)` over
    // the currently-staged page; stored across the Volcano per-call boundary.
    // The drive lives in the `lanev2` module. Reset on rescan. Bitmap scans
    // are never scrollable/mark cursors (planner-guaranteed), so no eflags
    // gate is needed here.
    lane_pos: u32,
    lane_n: u32,
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
        exec_recheck_qual_and_reset(self.bitmapqualorig.as_deref_mut(), estate, ecxt, slot)
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
            let found = ::tableam::table_scan_bitmap_next_tuple(
                mcx,
                scandesc,
                self.tbm.as_ref(),
                &mut self.tbmiterator,
                estate.slot_mut(slot_id),
                &mut self.recheck,
                &mut self.stats_lossy_pages,
                &mut self.stats_exact_pages,
            )?;
            if estate.es_instrument != 0 && self.pstate.is_some() {
                self.publish_stats(estate);
            }
            if !found {
                exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
                return Ok(false);
            }

            check_for_interrupts()?;

            // Lossy page or candidate match: recheck the original quals
            // against the heap tuple (ExecQualAndReset shape).
            if self.recheck {
                let ecxt = self.ss.ps_ExprContext;
                let passes = exec_recheck_qual_and_reset(
                    self.bitmapqualorig.as_deref_mut(),
                    estate,
                    ecxt,
                    slot_id,
                )?;
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
    let scandesc = node
        .ss
        .ss_currentScanDesc
        .as_mut()
        .expect("bitmap heap scan without a table scan descriptor");
    let n = ::tableam::table_scan_bitmap_next_pagebatch(
        scandesc,
        node.tbm.as_ref(),
        &mut node.tbmiterator,
        &mut node.recheck,
        &mut node.stats_lossy_pages,
        &mut node.stats_exact_pages,
    )?;
    if estate.es_instrument != 0 && node.pstate.is_some() {
        node.publish_stats(estate);
    }
    Ok(n)
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
    let passes =
        exec_recheck_qual_and_reset(node.bitmapqualorig.as_deref_mut(), estate, ecxt, slot_id)?;
    if !passes {
        exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
        return Ok(false);
    }
    Ok(true)
}

impl BitmapHeapScanState<'_> {
    /// Lane-executor-v2 page-batch cursor `(pos, n)`; the drive lives in
    /// `lanev2`, this only stores its position across the Volcano per-call
    /// boundary.
    #[inline]
    pub fn lane_cursor(&self) -> (u32, u32) {
        (self.lane_pos, self.lane_n)
    }

    #[inline]
    pub fn set_lane_cursor(&mut self, pos: u32, n: u32) {
        self.lane_pos = pos;
        self.lane_n = n;
    }

    #[cold]
    fn publish_stats(&self, estate: &mut EStateData<'_>) {
        estate.instr_set_bitmap_stats(
            self.plan_node_id,
            ::types_core::instrument::BitmapHeapScanInstrumentation {
                exact_pages: self.stats_exact_pages,
                lossy_pages: self.stats_lossy_pages,
            },
        );
    }
}

/// `BitmapShouldInitializeSharedState`: true = this participant won
/// BM_INITIAL and must build the bitmap; false = the bitmap is BM_FINISHED.
/// The 10ms timed park is the interrupt-check cadence (C's
/// ConditionVariableSleep checks interrupts per wakeup); the publish wake
/// itself rides the waker-word list (see [`ParallelBitmapHeapState`]).
pub fn bitmap_should_initialize_shared_state(
    pstate: &ParallelBitmapHeapState,
) -> PgResult<bool> {
    let mut guard = pstate.shared.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        match guard.state {
            SharedBitmapState::Initial => {
                guard.state = SharedBitmapState::InProgress;
                return Ok(true);
            }
            SharedBitmapState::Finished => return Ok(false),
            SharedBitmapState::InProgress => {
                // Register-under-lock, park OUTSIDE it: either the builder's
                // publish drains our word (unpark reaches us — a pre-park
                // unpark latches as Notified), or we re-observe InProgress
                // and re-park. Registration is idempotent across cadence
                // laps (contains-check).
                let word = waiter::current_handle().as_u64();
                if !guard.wakers.contains(&word) {
                    guard.wakers.push(word);
                }
                drop(guard);
                let _ = waiter::park_timeout(core::time::Duration::from_millis(10));
                if init_small::globals::InterruptPending() {
                    if let Err(e) = postgres_seams::check_for_interrupts::call() {
                        // Unwind hygiene (the row-2/3 class rule): drop the
                        // registration so the list never carries dead words
                        // into the publisher's drain.
                        let mut g =
                            pstate.shared.lock().unwrap_or_else(|p| p.into_inner());
                        if let Some(pos) = g.wakers.iter().position(|w| *w == word) {
                            g.wakers.swap_remove(pos);
                        }
                        return Err(e);
                    }
                }
                guard = pstate.shared.lock().unwrap_or_else(|e| e.into_inner());
            }
        }
    }
}

/// `ExecBitmapHeapInitializeDSM` (thread-native: no instrumentation block —
/// worker stats flow through the estate report).
pub fn exec_bitmap_heap_initialize_dsm(
    node: &mut BitmapHeapScanState<'_>,
) -> Arc<ParallelBitmapHeapState> {
    let pstate = Arc::new(ParallelBitmapHeapState::default());
    node.pstate = Some(Arc::clone(&pstate));
    pstate
}

/// `ExecBitmapHeapReInitializeDSM`.
pub fn exec_bitmap_heap_reinitialize_dsm(node: &mut BitmapHeapScanState<'_>) {
    let pstate = node.pstate.as_ref().expect("parallel bitmap scan was initialized");
    let mut guard = pstate.shared.lock().unwrap_or_else(|e| e.into_inner());
    guard.state = SharedBitmapState::Initial;
    guard.iterator = None;
    // Rescan boundary: no participant waits across it (C parity — the DSM
    // reinit runs with workers quiesced); a drained list is the invariant.
    debug_assert!(guard.wakers.is_empty(), "bitmap rescan with parked waiters");
    guard.wakers.clear();
}

/// `ExecBitmapHeapInitializeWorker`.
pub fn exec_bitmap_heap_initialize_worker(
    node: &mut BitmapHeapScanState<'_>,
    shared: Arc<ParallelBitmapHeapState>,
) {
    node.pstate = Some(shared);
}

/// `BitmapTableScanSetup` minus MultiExec: the dispatcher passes the bitmap
/// (None = parallel non-builder; it attaches the shared iterator instead).
pub fn bitmap_table_scan_setup<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tbm: Option<TIDBitmap<'mcx>>,
) -> PgResult<()> {
    match (&node.pstate, tbm) {
        (None, Some(tbm)) => {
            node.tbm = Some(tbm);
            let iter = node.tbm.as_mut().expect("just set").begin_private_iterate()?;
            node.tbmiterator = TbmIterator::private(iter);
        }
        (Some(ps), Some(mut tbm)) => {
            let shared = tbm.prepare_shared_iterate()?;
            // Publish (pg_barrier discipline): state + iterator stored and
            // the waker list TAKEN under the lock, unparks issued after the
            // lock is released.
            let woken = {
                let mut guard = ps.shared.lock().unwrap_or_else(|e| e.into_inner());
                debug_assert!(guard.state == SharedBitmapState::InProgress);
                guard.iterator = Some(Arc::clone(&shared));
                guard.state = SharedBitmapState::Finished;
                std::mem::take(&mut guard.wakers)
            };
            for w in woken {
                let _ = waiter::unpark_word(w);
            }
            node.tbm = Some(tbm);
            node.tbmiterator = TbmIterator::shared(TbmSharedIterator::attach(shared));
        }
        (Some(ps), None) => {
            let shared = {
                let guard = ps.shared.lock().unwrap_or_else(|e| e.into_inner());
                debug_assert!(guard.state == SharedBitmapState::Finished);
                Arc::clone(guard.iterator.as_ref().expect("BM_FINISHED without an iterator"))
            };
            node.tbmiterator = TbmIterator::shared(TbmSharedIterator::attach(shared));
        }
        (None, None) => unreachable!("serial bitmap heap scan setup without a bitmap"),
    }

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

/// bitmap-morsels (runtime bitmap arm): ensure the node's table scan
/// descriptor exists — shared by the window install and the fallback attach.
fn ensure_bm_scandesc<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
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
    Ok(())
}

/// bitmap-morsels (runtime bitmap arm, worker/claim side): install a
/// claimed-window range iterator over the frozen shared arrays. Replaces any
/// previous window (a fully-drained one — the drain runs each claim to
/// exhaustion; an aborted claim's remainder is discarded with the RG). The
/// node never builds its own bitmap on this path: the leader built and froze
/// it once (C's parallel-leader discipline).
pub fn bitmap_scan_install_entry_window<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    frozen: &Arc<TbmSharedIterState>,
    start: u64,
    end: u64,
) -> PgResult<()> {
    node.tbmiterator = TbmIterator::range(TbmRangeIterator::new(Arc::clone(frozen), start, end));
    ensure_bm_scandesc(node, estate)?;
    node.initialized = true;
    Ok(())
}

/// bitmap-morsels (leader fallback): the engagement fell back with NOTHING
/// consumed — attach the frozen state as an ordinary single-consumer shared
/// iterator (cursor at 0), which yields exactly C's merged block order: the
/// serial arm then drains the node byte-identically to the classic path.
pub fn bitmap_scan_attach_shared_fallback<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    frozen: Arc<TbmSharedIterState>,
) -> PgResult<()> {
    node.tbmiterator = TbmIterator::shared(TbmSharedIterator::attach(frozen));
    ensure_bm_scandesc(node, estate)?;
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
    // bitmapqualorig carries the original index quals — including any SubPlan
    // a runtime key pushed into the Index Cond — so it compiles under the
    // subplan env exactly like ss.qual (C ExecInitQual under the planstate).
    let (qual, bitmapqualorig) =
        ::executils::with_subplan_compile_env(estate, |env| -> ::types_error::PgResult<_> {
            let qual = ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)?;
            let bitmapqualorig =
                ::execexpr::exec_init_qual_subplans(mcx, &node.bitmapqualorig, params, env)?;
            Ok((qual, bitmapqualorig))
        })?;
    ss.qual = qual;

    Ok(BitmapHeapScanState {
        ss,
        bitmapqualorig,
        tbm: None,
        tbmiterator: TbmIterator::empty(),
        initialized: false,
        recheck: true,
        stats_exact_pages: 0,
        stats_lossy_pages: 0,
        pstate: None,
        plan_node_id: node.scan.plan.plan_node_id,
        parallel_aware: node.scan.plan.parallel_aware,
        lane_pos: 0,
        lane_n: 0,
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
    node.pstate = None;
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
    // Lane-executor-v2: the staged page batch is stale after rescan.
    node.lane_pos = 0;
    node.lane_n = 0;
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

// Exempt: bitmapqualorig, tbmiterator (Arc), and pstate (Arc) are released
// in exec_end_bitmap_heap_scan.
mcx::forget_safe_struct!(
    BitmapHeapScanState<'_> { ss, tbm, initialized, recheck,
        stats_exact_pages, stats_lossy_pages, plan_node_id, parallel_aware,
        lane_pos, lane_n;
        bitmapqualorig, tbmiterator, pstate },
);
