//! nbtree.c VACUUM arms: btbulkdelete/btvacuumcleanup/btvacuumscan/
//! btvacuumpage + posting-list vacuum (_bt_update_posting from nbtdedup.c).
//! C divergences (recorded): the bulkdelete callback is monomorphized to the
//! sorted dead-TID slice (vac_tid_reaped is its only producer); the read
//! stream collapses to sync per-block reads; the relation-extension lock is
//! skipped (C's own XXX: EB_LOCK_FIRST already closes the race); ereport
//! DEBUG/LOG chatter elided.

use bufmgr_seams::{self as bufmgr, BufferPin};
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{BlockNumber, ForkNumber, OffsetNumber};
use types_error::PgResult;
use types_nbtree::{
    BTCycleId, IndexBulkDeleteResult, BTP_SPLIT_END, BTREE_METAPAGE, P_FIRSTDATAKEY, P_ISDELETED,
    P_ISHALFDEAD, P_ISLEAF, P_NONE, P_RIGHTMOST,
};
use types_rel::Relation;
use types_storage::buf::BufferAccessStrategy;
use types_storage::bufpage::MaxIndexTuplesPerPage;
use types_storage::ReadBufferMode;
use types_tuple::itemptr::{ItemPointerCompare, ItemPointerData};

use crate::itup::{
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_get_posting_offset, bt_tuple_is_pivot,
    bt_tuple_is_posting, bt_tuple_set_posting, copy_index_tuple, maxalign, set_t_info, t_info,
    t_tid, ITup, ItupBuf, INDEX_SIZE_MASK,
};
use crate::page::{
    bt_checkpage, bt_lockbuf, bt_page_is_recyclable, bt_relbuf, bt_upgradelockbufcleanup,
    page_item, page_of_mut, page_opaque, write_opaque,
};
use crate::pagedel::{bt_pagedel, bt_pendingfsm_finalize, bt_pendingfsm_init};
use crate::utils::{bt_end_vacuum, bt_end_vacuum_key, bt_start_vacuum};

// IndexVacuumInfo (access/genam.h); message_level/report_progress dropped
// (logging + progress lanes unported).
pub struct IndexVacuumInfo<'a, 'mcx> {
    pub index: &'a Relation<'mcx>,
    pub heaprel: &'a ::types_rel::RelationData<'mcx>,
    pub analyze_only: bool,
    pub estimated_count: bool,
    pub num_heap_tuples: f64,
    pub strategy: BufferAccessStrategy,
}

pub(crate) struct BTVacState<'a, 'cb, 'mcx> {
    pub info: &'a IndexVacuumInfo<'a, 'mcx>,
    pub stats: &'a mut IndexBulkDeleteResult,
    pub dead_items: Option<&'a [ItemPointerData]>,
    // validate_index's never-delete callback: every live heap TID reported.
    pub collect: Option<&'a mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + 'cb)>,
    pub cycleid: BTCycleId,
    // Q2 divergence (recorded): a std Vec, not an arena PgVec — the pending
    // list must persist across chunked-scan claims served by DIFFERENT
    // worker arenas (BtVacChunkedScan owns it between claims). Bounded by
    // maxbufsize (work_mem-derived, bt_pendingfsm_init), exactly as C.
    pub pendingpages: Vec<::types_nbtree::BTPendingFSM>,
    pub maxbufsize: usize,
}

fn vacuum_delay_point() -> PgResult<()> {
    crate::check_for_interrupts()?;
    // Cost-based delay (autovacuum runs with VacuumCostActive) lives in
    // commands_vacuum::vacuum_delay_point, reached via seam (dependency
    // direction: commands_vacuum depends on nbtree). The uncosted path skips
    // the seam call, so unit rigs without seams_init never need it; config
    // reloads are picked up at the heap-phase delay points as before.
    if init_small::globals::VacuumCostActive() {
        vacuum_seams::vacuum_delay_point::call(false)?;
    }
    Ok(())
}

pub(crate) fn tid_is_member(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items
        .binary_search_by(|probe| ItemPointerCompare(probe, tid).cmp(&0))
        .is_ok()
}

/// btbulkdelete. `dead_items` is the sorted TID-store image.
pub fn btbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let mut stats = stats.unwrap_or_default();

    // C's PG_ENSURE_ERROR_CLEANUP as a Drop guard: thread-native panics
    // UNWIND at the statement boundary (the backend thread survives), so a
    // leaked cycle slot poisons every later vacuum of the index with
    // "multiple active vacuums" — the sweep-219 autovacuum wedge.
    let cycleid = bt_start_vacuum(rel)?;
    let _guard = BtVacuumGuard { rel };
    let res = btvacuumscan(mcx, info, &mut stats, Some(dead_items), None, cycleid);
    res?;

    Ok(stats)
}

// Guard module Drop: the cycle slot must clear on panic unwind or every
// later vacuum of this index errors forever (nbtutils.c _bt_start_vacuum's
// "multiple active vacuums"). Same pattern as ssi's BusyReset and wpool's
// LocalLatchReleaseGuard.
struct BtVacuumGuard<'a, 'mcx> {
    rel: &'a Relation<'mcx>,
}

impl Drop for BtVacuumGuard<'_, '_> {
    fn drop(&mut self) {
        bt_end_vacuum(self.rel);
    }
}

pub fn btbulkdelete_collect<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let mut stats = IndexBulkDeleteResult::default();
    let cycleid = bt_start_vacuum(rel)?;
    let _guard = BtVacuumGuard { rel };
    let res = btvacuumscan(mcx, info, &mut stats, None, Some(callback), cycleid);
    res?;
    Ok(stats)
}

/// btvacuumcleanup. `None` when no bulkdelete ran and no cleanup is needed.
pub fn btvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    if info.analyze_only {
        return Ok(stats);
    }

    let stats = match stats {
        Some(stats) => stats,
        None => {
            if !crate::pagedel::bt_vacuum_needs_cleanup(info.index)? {
                return Ok(None);
            }
            let mut stats = IndexBulkDeleteResult::default();
            btvacuumscan(mcx, info, &mut stats, None, None, 0)?;
            stats.estimated_count = true;
            stats
        }
    };

    Ok(Some(btvacuumcleanup_tail(info, stats)?))
}

/// btvacuumcleanup's post-scan tail (shared with the chunked form):
/// num_delpages → cleanup-info metapage update, heap-tuple clamp.
fn btvacuumcleanup_tail<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    mut stats: IndexBulkDeleteResult,
) -> PgResult<IndexBulkDeleteResult> {
    debug_assert!(stats.pages_deleted >= stats.pages_free);
    let num_delpages = stats.pages_deleted - stats.pages_free;
    crate::pagedel::bt_set_cleanup_info(info.index, num_delpages)?;

    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }

    Ok(stats)
}

fn btvacuumscan<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    dead_items: Option<&[ItemPointerData]>,
    collect: Option<&mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_)>,
    cycleid: BTCycleId,
) -> PgResult<()> {
    let _ = mcx; // pendingpages moved off the arena (BTVacState doc)
    let rel = info.index;

    stats.num_pages = 0;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;

    let cleanuponly = dead_items.is_none() && collect.is_none();
    let mut vstate = BTVacState {
        info,
        stats,
        dead_items,
        collect,
        cycleid,
        pendingpages: Vec::new(),
        maxbufsize: 0,
    };
    bt_pendingfsm_init(&mut vstate, cleanuponly)?;

    let mut scratch = MemoryContext::new("btvacuumpage");

    // The whole sweep as ONE unbounded step of the chunk-decomposed loop
    // (Q2): identical operation sequence to the pre-decomposition body —
    // the serial path IS the chunked path with an infinite quantum.
    let mut current: BlockNumber = BTREE_METAPAGE + 1;
    let mut num_pages: BlockNumber = 0;
    let done =
        btvacuumscan_blocks(&mut vstate, &mut scratch, &mut current, &mut num_pages, u32::MAX)?;
    debug_assert!(done, "unbounded btvacuumscan_blocks step must complete the sweep");

    vstate.stats.num_pages = num_pages;

    bt_pendingfsm_finalize(&mut vstate)?;
    if vstate.stats.pages_free > 0 {
        ::freespace::IndexFreeSpaceMapVacuum(rel)?;
    }
    Ok(())
}

/// The btvacuumscan block loop, decomposed for resumability (Q2 long-unit
/// discipline): advance the sweep by at most `max_blocks` pages, persisting
/// the cursor (`current`) and the end-of-relation watermark (`num_pages` —
/// C's loop-local, re-checked whenever the cursor reaches it so pages added
/// by concurrent splits are still scanned) across calls. Returns true when
/// the sweep is complete. EXACT decomposition: one call with `u32::MAX` is
/// operation-for-operation the pre-Q2 loop.
fn btvacuumscan_blocks(
    vstate: &mut BTVacState<'_, '_, '_>,
    scratch: &mut MemoryContext,
    current: &mut BlockNumber,
    num_pages: &mut BlockNumber,
    max_blocks: u32,
) -> PgResult<bool> {
    let rel = vstate.info.index;
    let mut scanned: u32 = 0;
    loop {
        if *current >= *num_pages {
            *num_pages = bufmgr::relation_get_number_of_blocks_in_fork::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
            )?;
            if *current >= *num_pages {
                return Ok(true);
            }
        }
        while *current < *num_pages {
            if scanned >= max_blocks {
                return Ok(false);
            }
            vacuum_delay_point()?;
            let pin = BufferPin::adopt(bufmgr::read_buffer_extended::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
                *current,
                ReadBufferMode::Normal,
                vstate.info.strategy.clone(),
            )?)
            .expect("ReadBufferExtended returned InvalidBuffer");
            btvacuumpage(vstate, scratch, pin)?;
            *current += 1;
            scanned += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Q2 (Track 4.2): the RESUMABLE chunked btvacuumscan. One index sweep as a
// sequence of bounded steps whose state lives in an owned, Send struct so
// successive quanta may be served by DIFFERENT pool workers (each with its
// own opened relations/arena) — the vacuum state the scan reads (dead-TID
// set) is shared and read-only, block access has no thread affinity, and
// the cycle-id registry is keyed by (db, relid), so WHICH thread drives a
// step is immaterial. What IS load-bearing: steps of one scan must never
// OVERLAP — the backtrack rule (btpo_next < scanblkno under one cycleid),
// the pending-FSM ordering, and `stats`/`current` are single-scan-instance
// state. INTRA-INDEX PARALLELISM IS ADJUDICATED OUT (charter Q2 item 1):
// concurrent claims over disjoint block ranges of one index would need
// per-range backtrack reasoning against the backwards-split interlock and
// synchronized page-deletion bookkeeping (bt_pagedel takes &mut vstate) —
// the honest increment is resumability with a serialized claim stream
// (vacuumparallel's ticket protocol enforces at-most-one in-flight step).
// ---------------------------------------------------------------------------

/// Owned, resumable state of one chunked index-vacuum sweep.
pub struct BtVacChunkedScan {
    cycleid: BTCycleId,
    /// (dbOid, relid) — the abort-path cycle-slot release key (Drop).
    key: (::types_core::Oid, ::types_core::Oid),
    current: BlockNumber,
    num_pages: BlockNumber,
    stats: IndexBulkDeleteResult,
    pendingpages: Vec<::types_nbtree::BTPendingFSM>,
    maxbufsize: usize,
    finished: bool,
}

impl Drop for BtVacChunkedScan {
    fn drop(&mut self) {
        // The BtVacuumGuard law, chunk form: an abandoned bulkdelete scan
        // (error/cancel between steps) must clear its cycle entry or every
        // later vacuum of this index errors with "multiple active vacuums".
        // Cleanup scans (cycleid 0) never registered.
        if !self.finished && self.cycleid != 0 {
            bt_end_vacuum_key(self.key);
        }
    }
}

impl BtVacChunkedScan {
    /// Blocks scanned so far / last-seen relation size (progress surface).
    pub fn blocks_scanned(&self) -> BlockNumber {
        self.current
    }

    pub fn blocks_total(&self) -> BlockNumber {
        self.num_pages
    }
}

/// Begin a chunked btbulkdelete sweep: registers the vacuum cycle id (the
/// split-tracking envelope) and sizes the pending-FSM buffer. `istat` is the
/// carried-in stats of an earlier pass, exactly as btbulkdelete.
pub fn bt_chunked_bulkdelete_begin<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<BtVacChunkedScan> {
    let rel = info.index;
    let cycleid = bt_start_vacuum(rel)?;
    let mut stats = istat.unwrap_or_default();
    // btvacuumscan's reset (accumulating fields — tuples_removed etc. —
    // carry across passes; these four are per-scan).
    stats.num_pages = 0;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;
    let mut scan = BtVacChunkedScan {
        cycleid,
        key: crate::utils::vac_key(rel),
        current: BTREE_METAPAGE + 1,
        num_pages: 0,
        stats,
        pendingpages: Vec::new(),
        maxbufsize: 0,
        finished: false,
    };
    // Fallible init AFTER the struct exists: an error here still releases
    // the cycle slot through Drop.
    with_chunk_vstate(&mut scan, info, None, |vs| bt_pendingfsm_init(vs, false))?;
    Ok(scan)
}

/// Chunked btvacuumcleanup entry: either the pass needs no scan (`Done`,
/// with btvacuumcleanup's tail already applied) or a scan must run
/// (`Scan` — drive with [`bt_chunked_scan_step`] then
/// [`bt_chunked_cleanup_finish`]).
pub enum BtChunkedCleanup {
    Done(Option<IndexBulkDeleteResult>),
    Scan(BtVacChunkedScan),
}

pub fn bt_chunked_cleanup_begin<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<BtChunkedCleanup> {
    if info.analyze_only {
        return Ok(BtChunkedCleanup::Done(istat));
    }
    match istat {
        Some(stats) => Ok(BtChunkedCleanup::Done(Some(btvacuumcleanup_tail(info, stats)?))),
        None => {
            if !crate::pagedel::bt_vacuum_needs_cleanup(info.index)? {
                return Ok(BtChunkedCleanup::Done(None));
            }
            // Cleanup-only scan: cycleid 0 (no registry entry), stats fresh,
            // pending-FSM init a no-op (cleanuponly) — as btvacuumscan.
            Ok(BtChunkedCleanup::Scan(BtVacChunkedScan {
                cycleid: 0,
                key: crate::utils::vac_key(info.index),
                current: BTREE_METAPAGE + 1,
                num_pages: 0,
                stats: IndexBulkDeleteResult::default(),
                pendingpages: Vec::new(),
                maxbufsize: 0,
                finished: false,
            }))
        }
    }
}

/// Advance a chunked sweep by at most `max_blocks` pages. `dead_items` is
/// the sorted dead-TID image for bulkdelete steps, None for cleanup steps —
/// callers must pass the SAME shape on every step of one scan. True = the
/// sweep is complete; call the matching finish.
pub fn bt_chunked_scan_step<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    scan: &mut BtVacChunkedScan,
    dead_items: Option<&[ItemPointerData]>,
    max_blocks: u32,
) -> PgResult<bool> {
    debug_assert!(!scan.finished, "step after finish");
    debug_assert!(
        (scan.cycleid != 0) == dead_items.is_some(),
        "step shape must match the begin arm (bulkdelete vs cleanup)"
    );
    let mut scratch = MemoryContext::new("btvacuumpage");
    let mut current = scan.current;
    let mut num_pages = scan.num_pages;
    let done = with_chunk_vstate(scan, info, dead_items, |vs| {
        btvacuumscan_blocks(vs, &mut scratch, &mut current, &mut num_pages, max_blocks)
    });
    scan.current = current;
    scan.num_pages = num_pages;
    done
}

/// Complete a chunked bulkdelete sweep: pending-FSM finalize, FSM vacuum,
/// cycle-slot release — btbulkdelete's tail, in its order.
pub fn bt_chunked_bulkdelete_finish<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    mut scan: BtVacChunkedScan,
) -> PgResult<IndexBulkDeleteResult> {
    debug_assert!(scan.cycleid != 0);
    chunk_scan_finalize(info, &mut scan)?;
    scan.finished = true;
    bt_end_vacuum_key(scan.key);
    Ok(std::mem::take(&mut scan.stats))
}

/// Complete a chunked cleanup sweep: finalize, then btvacuumcleanup's tail
/// (estimated_count, num_delpages/cleanup-info, heap-tuple clamp).
pub fn bt_chunked_cleanup_finish<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    mut scan: BtVacChunkedScan,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    debug_assert!(scan.cycleid == 0);
    chunk_scan_finalize(info, &mut scan)?;
    scan.finished = true;
    let mut stats = std::mem::take(&mut scan.stats);
    stats.estimated_count = true;
    Ok(Some(btvacuumcleanup_tail(info, stats)?))
}

fn chunk_scan_finalize<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    scan: &mut BtVacChunkedScan,
) -> PgResult<()> {
    debug_assert!(scan.current >= scan.num_pages, "finish before the sweep completed");
    scan.stats.num_pages = scan.num_pages;
    with_chunk_vstate(scan, info, None, bt_pendingfsm_finalize)?;
    if scan.stats.pages_free > 0 {
        ::freespace::IndexFreeSpaceMapVacuum(info.index)?;
    }
    Ok(())
}

/// Materialize the per-step BTVacState view over the persistent chunk state
/// (pendingpages/maxbufsize move in and out; stats borrowed).
fn with_chunk_vstate<'mcx, R>(
    scan: &mut BtVacChunkedScan,
    info: &IndexVacuumInfo<'_, 'mcx>,
    dead_items: Option<&[ItemPointerData]>,
    f: impl FnOnce(&mut BTVacState<'_, '_, 'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let mut vstate = BTVacState {
        info,
        stats: &mut scan.stats,
        dead_items,
        collect: None,
        cycleid: scan.cycleid,
        pendingpages: std::mem::take(&mut scan.pendingpages),
        maxbufsize: scan.maxbufsize,
    };
    let r = f(&mut vstate);
    let BTVacState { pendingpages, maxbufsize, .. } = vstate;
    scan.pendingpages = pendingpages;
    scan.maxbufsize = maxbufsize;
    r
}

fn btvacuumpage(
    vstate: &mut BTVacState<'_, '_, '_>,
    scratch: &mut MemoryContext,
    pin: BufferPin,
) -> PgResult<()> {
    let rel = vstate.info.index;
    let heaprel = vstate.info.heaprel;
    let scanblkno = pin.block_number();
    let mut blkno = scanblkno;
    let mut pin = pin;

    scratch.reset();
    let scx = scratch.mcx();

    loop {
        let mut attempt_pagedel = false;
        let mut backtrack_to: BlockNumber = P_NONE;

        bt_lockbuf(rel, &pin, ::types_nbtree::BT_READ)?;
        let mut opaque = None;
        if !pin.page().is_new() {
            bt_checkpage(rel, &pin)?;
            opaque = Some(page_opaque(&pin.page()));
        }

        debug_assert!(blkno <= scanblkno);
        if blkno != scanblkno {
            // Backtracked to a right sibling: only a live leaf page carrying
            // the current cycle ID needs work (C LOGs corruption here).
            let ok = opaque
                .as_ref()
                .is_some_and(|o| P_ISLEAF(o) && !P_ISHALFDEAD(o));
            if !ok {
                debug_assert!(false);
                bt_relbuf(rel, pin)?;
                return Ok(());
            }
            let o = opaque.as_ref().expect("checked above");
            if o.btpo_cycleid != vstate.cycleid || P_ISDELETED(o) {
                bt_relbuf(rel, pin)?;
                return Ok(());
            }
        }

        if opaque.is_none() || bt_page_is_recyclable(&pin.page(), heaprel)? {
            ::freespace::RecordFreeIndexPage(rel, blkno)?;
            vstate.stats.pages_deleted += 1;
            vstate.stats.pages_free += 1;
        } else if P_ISDELETED(opaque.as_ref().expect("non-new page")) {
            vstate.stats.pages_deleted += 1;
        } else if P_ISHALFDEAD(opaque.as_ref().expect("non-new page")) {
            attempt_pagedel = true;
        } else if P_ISLEAF(opaque.as_ref().expect("non-new page")) {
            bt_upgradelockbufcleanup(rel, &pin)?;
            // Re-read below the lock trade: the page may have changed while
            // unlocked (C reads through the live pointer).
            let opaque = page_opaque(&pin.page());

            if vstate.cycleid != 0
                && opaque.btpo_cycleid == vstate.cycleid
                && (opaque.btpo_flags & BTP_SPLIT_END) == 0
                && !P_RIGHTMOST(&opaque)
                && opaque.btpo_next < scanblkno
            {
                backtrack_to = opaque.btpo_next;
            }

            let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];
            let mut ndeletable = 0usize;
            let mut updatable: PgVec<'_, VacPosting<'_>> = PgVec::new_in(scx);
            let minoff = P_FIRSTDATAKEY(&opaque);
            let mut maxoff = pin.page().max_offset_number();
            let mut nhtidsdead = 0usize;
            let mut nhtidslive = 0usize;

            if let Some(dead) = vstate.dead_items {
                let mut offnum = minoff;
                while offnum <= maxoff {
                    let page = pin.page();
                    let itup = page_item(&page, page.item_id(offnum));
                    // SAFETY: on-page tuple under the cleanup lock.
                    unsafe {
                        debug_assert!(!bt_tuple_is_pivot(itup));
                        if !bt_tuple_is_posting(itup) {
                            if tid_is_member(dead, &t_tid(itup)) {
                                deletable[ndeletable] = offnum;
                                ndeletable += 1;
                                nhtidsdead += 1;
                            } else {
                                nhtidslive += 1;
                            }
                        } else {
                            let nposting = bt_tuple_get_nposting(itup);
                            let (vacposting, nremaining) =
                                btreevacuumposting(scx, dead, itup, offnum)?;
                            match vacposting {
                                None => debug_assert!(nremaining == nposting),
                                Some(vacposting) if nremaining > 0 => {
                                    debug_assert!(nremaining < nposting);
                                    updatable.push(vacposting);
                                    nhtidsdead += nposting - nremaining;
                                }
                                Some(_) => {
                                    deletable[ndeletable] = offnum;
                                    ndeletable += 1;
                                    nhtidsdead += nposting;
                                }
                            }
                            nhtidslive += nremaining;
                        }
                    }
                    offnum += 1;
                }
            } else if vstate.collect.is_some() {
                let mut offnum = minoff;
                while offnum <= maxoff {
                    let page = pin.page();
                    let itup = page_item(&page, page.item_id(offnum));
                    // SAFETY: on-page tuple under the cleanup lock.
                    unsafe {
                        debug_assert!(!bt_tuple_is_pivot(itup));
                        if !bt_tuple_is_posting(itup) {
                            let tid = t_tid(itup);
                            (vstate.collect.as_mut().expect("checked"))(&tid)?;
                            nhtidslive += 1;
                        } else {
                            let nposting = bt_tuple_get_nposting(itup);
                            for i in 0..nposting {
                                let tid = bt_tuple_get_posting_n(itup, i);
                                (vstate.collect.as_mut().expect("checked"))(&tid)?;
                            }
                            nhtidslive += nposting;
                        }
                    }
                    offnum += 1;
                }
            }

            if ndeletable > 0 || !updatable.is_empty() {
                debug_assert!(nhtidsdead >= ndeletable + updatable.len());
                crate::pagedel::bt_delitems_vacuum(
                    scx,
                    rel,
                    &pin,
                    &deletable[..ndeletable],
                    &mut updatable,
                )?;
                vstate.stats.tuples_removed += nhtidsdead as f64;
                maxoff = pin.page().max_offset_number();
            } else {
                debug_assert!(nhtidsdead == 0);
                if vstate.cycleid != 0 && opaque.btpo_cycleid == vstate.cycleid {
                    let mut o = page_opaque(&pin.page());
                    o.btpo_cycleid = 0;
                    write_opaque(&mut page_of_mut(&pin), &o);
                    bufmgr::mark_buffer_dirty_hint::call(pin.buffer(), true)?;
                }
            }

            if minoff > maxoff {
                attempt_pagedel = blkno == scanblkno;
            } else if vstate.dead_items.is_some() || vstate.collect.is_some() {
                vstate.stats.num_index_tuples += nhtidslive as f64;
            } else {
                vstate.stats.num_index_tuples += (maxoff - minoff + 1) as f64;
            }
            debug_assert!(!attempt_pagedel || nhtidslive == 0);
        }

        if attempt_pagedel {
            debug_assert!(blkno == scanblkno);
            bt_pagedel(scx, rel, pin, vstate)?;
        } else {
            bt_relbuf(rel, pin)?;
        }

        if backtrack_to == P_NONE {
            return Ok(());
        }
        blkno = backtrack_to;

        vacuum_delay_point()?;

        // As C: no _bt_getbuf (all-zero pages must be recyclable, not fatal),
        // and the caller's strategy applies.
        pin = BufferPin::adopt(bufmgr::read_buffer_extended::call(
            rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vstate.info.strategy.clone(),
        )?)
        .expect("ReadBufferExtended returned InvalidBuffer");
    }
}

// BTVacuumPostingData; itup is an owned image (C points into the page until
// _bt_update_posting replaces it — copying keeps the borrow local).
pub(crate) struct VacPosting<'s> {
    pub itup: ItupBuf<'s>,
    pub updatedoffset: OffsetNumber,
    pub deletetids: PgVec<'s, u16>,
}

/// btreevacuumposting: `(replacement metadata, TIDs remaining)`.
///
/// # Safety
/// `posting` is a live posting tuple on the cleanup-locked page.
unsafe fn btreevacuumposting<'s>(
    scx: Mcx<'s>,
    dead_items: &[ItemPointerData],
    posting: ITup,
    updatedoffset: OffsetNumber,
) -> PgResult<(Option<VacPosting<'s>>, usize)> {
    let nitem = bt_tuple_get_nposting(posting);
    let mut live = 0usize;
    let mut vacposting: Option<VacPosting<'s>> = None;

    for i in 0..nitem {
        let tid = bt_tuple_get_posting_n(posting, i);
        if !tid_is_member(dead_items, &tid) {
            live += 1;
        } else {
            if vacposting.is_none() {
                vacposting = Some(VacPosting {
                    itup: copy_index_tuple(scx, posting)?,
                    updatedoffset,
                    deletetids: PgVec::new_in(scx),
                });
            }
            vacposting
                .as_mut()
                .expect("created above")
                .deletetids
                .push(i as u16);
        }
    }

    Ok((vacposting, live))
}

/// _bt_update_posting (nbtdedup.c): replace `vacposting.itup` with the image
/// lacking the deleted TIDs.
pub(crate) fn bt_update_posting<'s>(scx: Mcx<'s>, vacposting: &mut VacPosting<'s>) -> PgResult<()> {
    // orig dangles at the itup swap: its block ends first.
    let itup = {
        let orig = vacposting.itup.as_ptr();
        // SAFETY: owned image captured by btreevacuumposting.
        unsafe {
            let norig = bt_tuple_get_nposting(orig);
            let nhtids = norig - vacposting.deletetids.len();
            debug_assert!(nhtids > 0 && nhtids < norig);

            let keysize = bt_tuple_get_posting_offset(orig);
            let newsize = if nhtids > 1 {
                maxalign(keysize + nhtids * core::mem::size_of::<ItemPointerData>())
            } else {
                keysize
            };
            debug_assert!(newsize <= INDEX_SIZE_MASK as usize);
            debug_assert!(newsize == maxalign(newsize));

            let mut itup = ItupBuf::with_size(scx, newsize)?;
            core::ptr::copy_nonoverlapping(orig, itup.as_mut_ptr(), keysize);
            let info = (t_info(itup.as_ptr()) & !INDEX_SIZE_MASK) | newsize as u16;
            set_t_info(itup.as_mut_ptr(), info);

            let htids_off = if nhtids > 1 {
                bt_tuple_set_posting(itup.as_mut_ptr(), nhtids as u16, keysize);
                keysize
            } else {
                set_t_info(
                    itup.as_mut_ptr(),
                    t_info(itup.as_ptr()) & !::types_nbtree::INDEX_ALT_TID_MASK,
                );
                0
            };

            let mut ui = 0usize;
            let mut d = 0usize;
            for i in 0..norig {
                if d < vacposting.deletetids.len() && vacposting.deletetids[d] as usize == i {
                    d += 1;
                    continue;
                }
                let tid = bt_tuple_get_posting_n(orig, i);
                itup.as_mut_ptr()
                    .add(htids_off + ui * core::mem::size_of::<ItemPointerData>())
                    .cast::<ItemPointerData>()
                    .write_unaligned(tid);
                ui += 1;
            }
            debug_assert!(ui == nhtids);
            debug_assert!(d == vacposting.deletetids.len());
            itup
        }
    };
    vacposting.itup = itup;
    Ok(())
}

