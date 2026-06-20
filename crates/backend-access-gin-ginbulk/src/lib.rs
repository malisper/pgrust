#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! Port of `src/backend/access/gin/ginbulk.c` (the in-memory
//! [`BuildAccumulator`]) and the serial GIN build driver from
//! `src/backend/access/gin/gininsert.c` (`ginbuild` / `ginbuildempty` /
//! `ginBuildCallback` / `ginHeapTupleBulkInsert`).
//!
//! `ginbuild` scans the heap once via `table_index_build_scan` (the real
//! table-AM build-scan provider), accumulating each indexable item's entries in
//! a red-black tree keyed by `(attnum, key, category)`; when the accumulator
//! exceeds `maintenance_work_mem` it dumps every key's TID list into the entry
//! tree via `ginEntryInsert` (the landed L5 retail-insert spine). At the end the
//! remaining entries are dumped, the metapage stats are updated, and the whole
//! index is WAL-logged with `log_newpage_range`.
//!
//! # Out of scope (sanctioned panic legs)
//!
//!   * the cross-process **parallel build** (`_gin_begin_parallel` /
//!     `_gin_parallel_merge` / `ginBuildCallbackParallel` / `ginFlushBuildState`
//!     / the `GinBuffer` tuplesort merge). `ginbuild` always takes the serial
//!     path here; `indexInfo->ii_ParallelWorkers > 0` is not honored (the
//!     parallel tuplesort substrate is unported). This matches C behaviour for a
//!     non-parallel build.

extern crate alloc;

use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::cmp::Ordering;

use backend_lib_rbtree::{rbt_begin_iterate, rbt_create_with, rbt_insert, rbt_iterate, RBTOrderControl, RBTree};
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_error::PgResult;

use mcx::Mcx;
use types_core::primitive::{BlockNumber, ForkNumber, OffsetNumber};
use types_error::PgError;
use types_gin::{GinNullCategory, GinState, GinStatsData, GIN_CAT_NORM_KEY, GIN_LEAF};
use types_rel::Relation;
use types_nodes::execnodes::IndexInfo;
use types_tableam::amapi::IndexBuildResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_gin_core_probe::ginpostinglist::ginCompareItemPointers;
use backend_access_gin_ginutil::{
    ginCompareAttEntries, ginExtractEntries, GinInitBuffer, GinInitMetabuffer, GinNewBuffer,
    ginUpdateStats, initGinState,
};
use backend_access_gin_gininsert::ginEntryInsert;

#[cfg(test)]
mod tests;

/// `GinEntryAccumulator allocation quantum` — `DEF_NENTRY` (ginbulk.c:24). In
/// the owned model the rbtree arena allocates nodes itself, so this is not a
/// chunk-allocation quantum; kept as documentation of the C constant.
const _DEF_NENTRY: usize = 2048;
/// `ItemPointer initial allocation quantum` — `DEF_NPTR` (ginbulk.c:25).
const DEF_NPTR: usize = 5;

// ===========================================================================
// GinEntryAccumulator (gin_private.h) — one rbtree node.
//
// In C this is an `RBTNode` subtype; here it is the rbtree payload `T`. The
// single-entry insert form (C's `eatmp` with `list = heapptr`) is modeled by
// constructing a node whose `list` already holds the one TID, so the rbtree's
// new-node path and the combine path both see a fully-formed node.
// ===========================================================================

struct GinEntryAccumulator<'mcx> {
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
    /// `ItemPointerData *list` + `uint32 count` / `uint32 maxcount`: the owned
    /// TID list (length = C `count`, capacity = C `maxcount`).
    list: Vec<ItemPointerData>,
    /// `bool shouldSort`.
    shouldSort: bool,
}

// ===========================================================================
// BuildAccumulator (gin_private.h) — the in-memory entry accumulator.
// ===========================================================================

/// `BuildAccumulator` (gin_private.h) — accumulates index entries grouped by
/// `(attnum, key, category)` during a build, each carrying its sorted TID list.
///
/// The rbtree comparator (C `cmpEntryAccumulator`, calling `ginCompareAttEntries`)
/// can `ereport`; the rbtree's stored comparator is infallible
/// (`Fn(&T,&T)->Ordering`), so any error is captured in `cmp_error` and surfaced
/// by the caller after the (re)insert. C's `allocatedMemory` chunk accounting is
/// tracked in [`allocated_memory`](Self::allocated_memory).
pub struct BuildAccumulator<'mcx> {
    /// `GinState *ginstate` — the per-index working state, shared with the
    /// comparator (which needs it for `ginCompareAttEntries`).
    ginstate: Rc<GinState<'mcx>>,
    /// `Size allocatedMemory` — running palloc accounting in bytes.
    allocated_memory: usize,
    /// `RBTree *tree`.
    tree: RBTree<GinEntryAccumulator<'mcx>, GinAccumCmp<'mcx>>,
    /// Comparator-side error capture (the rbtree comparator can't return `Err`).
    cmp_error: Rc<RefCell<Option<PgError>>>,
}

impl<'mcx> BuildAccumulator<'mcx> {
    /// `accum.allocatedMemory`.
    pub fn allocated_memory(&self) -> usize {
        self.allocated_memory
    }

    /// `ginInitBA(accum)` (ginbulk.c:108) — re-initialize, preserving the shared
    /// `ginstate` (the public re-init `ginInsertCleanup` calls between batches).
    pub fn reinit(&mut self) {
        self.ginInitBA();
    }

    /// Surface any error the rbtree comparator captured (the opclass compare
    /// support function can `ereport`).
    pub fn take_cmp_error(&self) -> PgResult<()> {
        if let Some(e) = self.cmp_error.borrow_mut().take() {
            return Err(e);
        }
        Ok(())
    }

    /// `ginInsertBAEntries(accum, heapptr, attnum, entries, categories,
    /// nentries)` (ginbulk.c:209) — public entry point for `ginInsertCleanup`'s
    /// `processPendingPage`.
    pub fn ginInsertBAEntries(
        &mut self,
        heapptr: &ItemPointerData,
        attnum: OffsetNumber,
        entries: &[Datum<'mcx>],
        categories: &[GinNullCategory],
    ) -> PgResult<()> {
        ginInsertBAEntries(self, heapptr, attnum, entries, categories, entries.len() as i32)
    }

    /// `ginBeginBAScan` + repeated `ginGetBAEntry` (ginbulk.c:256/267): drain
    /// every accumulated entry in rbtree order, each TID list sorted.
    pub fn drain(&self) -> Vec<BAEntry<'mcx>> {
        drain_ba_entries(self)
    }
}

/// Create a fresh [`BuildAccumulator`] over a clone of `ginstate` (C
/// `accum.ginstate = &buildstate.ginstate; ginInitBA(&accum);`). The clone is
/// owned by the accumulator's comparator. Used by `ginInsertCleanup`.
pub fn new_accumulator<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<BuildAccumulator<'mcx>> {
    let clone = backend_access_gin_gininsert::clone_ginstate(mcx, ginstate)?;
    Ok(new_build_accumulator(Rc::new(clone)))
}

/// The rbtree comparator closure type for [`BuildAccumulator::tree`].
type GinAccumCmp<'mcx> = alloc::boxed::Box<
    dyn Fn(&GinEntryAccumulator<'mcx>, &GinEntryAccumulator<'mcx>) -> Ordering + 'mcx,
>;

impl<'mcx> BuildAccumulator<'mcx> {
    /// `ginInitBA(accum)` (ginbulk.c:108) — (re)initialize the accumulator. In C
    /// `accum->ginstate` is intentionally left untouched; here the shared
    /// `ginstate` `Rc` is preserved across a re-init.
    fn ginInitBA(&mut self) {
        self.allocated_memory = 0;
        let ginstate = Rc::clone(&self.ginstate);
        let cmp_error = Rc::clone(&self.cmp_error);
        let comparator: GinAccumCmp<'mcx> = alloc::boxed::Box::new(
            move |ea: &GinEntryAccumulator<'mcx>, eb: &GinEntryAccumulator<'mcx>| -> Ordering {
                // cmpEntryAccumulator (ginbulk.c:71): compare by (attnum, key,
                // category) via the opclass compare support function.
                match ginCompareAttEntries(
                    &ginstate,
                    ea.attnum,
                    ea.key.clone(),
                    ea.category,
                    eb.attnum,
                    eb.key.clone(),
                    eb.category,
                ) {
                    Ok(res) => res.cmp(&0),
                    Err(e) => {
                        // Capture the first error; subsequent compares are
                        // meaningless but harmless. Return Equal so the tree's
                        // walk terminates promptly.
                        let mut slot = cmp_error.borrow_mut();
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        Ordering::Equal
                    }
                }
            },
        );
        self.tree = rbt_create_with(comparator);
    }

    /// OOM error tagged with the GIN index's context — the build accumulator
    /// allocations are palloc's in C, whose failure is `ERRCODE_OUT_OF_MEMORY`.
    fn ginstate_oom(&self) -> PgError {
        PgError::error("out of memory")
    }
}

/// Create a fresh [`BuildAccumulator`] over the given (shared) [`GinState`],
/// equivalent to C `accum.ginstate = &buildstate.ginstate; ginInitBA(&accum);`.
fn new_build_accumulator<'mcx>(ginstate: Rc<GinState<'mcx>>) -> BuildAccumulator<'mcx> {
    let cmp_error = Rc::new(RefCell::new(None));
    let mut accum = BuildAccumulator {
        ginstate,
        allocated_memory: 0,
        // placeholder tree; ginInitBA installs the real comparator.
        tree: rbt_create_with(alloc::boxed::Box::new(|_: &GinEntryAccumulator<'mcx>, _: &GinEntryAccumulator<'mcx>| Ordering::Equal)),
        cmp_error,
    };
    accum.ginInitBA();
    accum
}

// ===========================================================================
// getDatumCopy (ginbulk.c:127)
// ===========================================================================

/// `getDatumCopy(accum, attnum, value)` (ginbulk.c:127): like `datumCopy()` but
/// counting palloc'd space into `allocatedMemory`. In the owned `Datum` model a
/// by-ref value already carries its bytes; cloning makes a permanent copy (C's
/// `datumCopy`). The accounting adds the by-ref byte length (by-val Datums add
/// nothing, matching C `att->attbyval`).
fn getDatumCopy<'mcx>(allocated_memory: &mut usize, value: &Datum<'mcx>) -> Datum<'mcx> {
    if let Datum::ByRef(bytes) = value {
        *allocated_memory += bytes.len();
    }
    value.clone()
}

// ===========================================================================
// ginCombineData / ginInsertBAEntry (ginbulk.c:29 / 147)
// ===========================================================================

/// `ginInsertBAEntry(accum, heapptr, attnum, key, category)` (ginbulk.c:147):
/// find or create the `(attnum, key, category)` node and append `heapptr` to its
/// TID list (C `ginCombineData` does the append for an existing node; the new
/// node is initialized with the single TID).
///
/// The proposed node is built fully-formed (the permanent key copy is taken and
/// its list is pre-grown to the `DEF_NPTR` quantum holding the single TID), so
/// the rbtree's new-node path stores it directly — matching C's `isNew` branch —
/// while the combine path appends and discards the proposed node.
fn ginInsertBAEntry<'mcx>(
    accum: &mut BuildAccumulator<'mcx>,
    heapptr: &ItemPointerData,
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
) -> PgResult<()> {
    // Build the proposed node, fully initialized as C's isNew branch would. The
    // permanent datum copy (normal keys only) and the DEF_NPTR list allocation
    // are accounted as if the node is new; if it turns out NOT to be new we undo
    // the accounting below (C only accounts on isNew).
    let mut proposed_alloc: usize = 0;
    let perm_key = if category == GIN_CAT_NORM_KEY {
        getDatumCopy(&mut proposed_alloc, &key)
    } else {
        key
    };
    let mut list: Vec<ItemPointerData> = Vec::new();
    list.try_reserve_exact(DEF_NPTR)
        .map_err(|_| accum.ginstate_oom())?;
    list.push(*heapptr);
    proposed_alloc += list.capacity() * core::mem::size_of::<ItemPointerData>();

    let proposed = GinEntryAccumulator {
        attnum,
        key: perm_key,
        category,
        list,
        shouldSort: false,
    };

    // ginCombineData (ginbulk.c:29): append the proposed single TID into the
    // existing node, growing the list and tracking sort order. Captures the
    // accumulator's `allocatedMemory` and an error slot (the "posting list is
    // too long" ereport).
    let combine_error: Rc<RefCell<Option<PgError>>> = Rc::new(RefCell::new(None));
    let combine_error_in = Rc::clone(&combine_error);
    let mut combine_delta: i64 = 0;

    let is_new = rbt_insert(
        &mut accum.tree,
        proposed,
        |existing: &mut GinEntryAccumulator<'mcx>, newdata: GinEntryAccumulator<'mcx>| {
            // newdata contains exactly one itempointer (assumption in C).
            let new_tid = newdata.list[0];

            if existing.list.len() >= existing.list.capacity() {
                // C: maxcount overflow check (maxcount > INT_MAX).
                if existing.list.capacity() > i32::MAX as usize {
                    let mut slot = combine_error_in.borrow_mut();
                    if slot.is_none() {
                        *slot = Some(
                            PgError::error("posting list is too long")
                                .with_hint("Reduce \"maintenance_work_mem\"."),
                        );
                    }
                    return;
                }
                let old_cap = existing.list.capacity();
                let new_cap = old_cap * 2;
                // repalloc_huge to maxcount*2 (ginbulk.c:48). Track the chunk
                // delta in allocatedMemory.
                combine_delta -= (old_cap * core::mem::size_of::<ItemPointerData>()) as i64;
                if existing
                    .list
                    .try_reserve_exact(new_cap - existing.list.len())
                    .is_err()
                {
                    let mut slot = combine_error_in.borrow_mut();
                    if slot.is_none() {
                        *slot = Some(PgError::error("out of memory"));
                    }
                    return;
                }
                combine_delta +=
                    (existing.list.capacity() * core::mem::size_of::<ItemPointerData>()) as i64;
            }

            // If item pointers are not ordered, they will need sorting later.
            if !existing.shouldSort {
                let last = existing.list[existing.list.len() - 1];
                let res = ginCompareItemPointers(&last, &new_tid);
                debug_assert!(res != 0);
                if res > 0 {
                    existing.shouldSort = true;
                }
            }

            existing.list.push(new_tid);
        },
    )?;

    if let Some(e) = combine_error.borrow_mut().take() {
        return Err(e);
    }

    if is_new {
        accum.allocated_memory += proposed_alloc;
    } else {
        accum.allocated_memory = (accum.allocated_memory as i64 + combine_delta) as usize;
    }

    Ok(())
}

// ===========================================================================
// ginInsertBAEntries (ginbulk.c:209)
// ===========================================================================

/// `ginInsertBAEntries(accum, heapptr, attnum, entries, categories, nentries)`
/// (ginbulk.c:209): insert the entries for one heap pointer, ordering the
/// inserts to keep the rbtree near-balanced when the input is already sorted.
fn ginInsertBAEntries<'mcx>(
    accum: &mut BuildAccumulator<'mcx>,
    heapptr: &ItemPointerData,
    attnum: OffsetNumber,
    entries: &[Datum<'mcx>],
    categories: &[GinNullCategory],
    nentries: i32,
) -> PgResult<()> {
    if nentries <= 0 {
        return Ok(());
    }

    // step = largest power of 2 that is <= nentries.
    let mut step: u32 = nentries as u32;
    step |= step >> 1;
    step |= step >> 2;
    step |= step >> 4;
    step |= step >> 8;
    step |= step >> 16;
    step >>= 1;
    step += 1;

    while step > 0 {
        let mut i: i32 = step as i32 - 1;
        while i < nentries && i >= 0 {
            ginInsertBAEntry(
                accum,
                heapptr,
                attnum,
                entries[i as usize].clone(),
                categories[i as usize],
            )?;
            i += (step << 1) as i32;
        }
        step >>= 1;
    }

    Ok(())
}

// ===========================================================================
// ginBeginBAScan / ginGetBAEntry (ginbulk.c:256 / 267)
//
// In the owned model the rbtree iterator borrows the tree, so the C
// "begin scan then repeatedly get" pattern is expressed as a single
// `drain_ba_entries` that yields every (attnum, key, category, sorted list) in
// rbtree order. The TID list is sorted in place when `shouldSort` is set.
// ===========================================================================

/// One drained accumulator entry: `(attnum, key, category, sorted TID list)`.
pub struct BAEntry<'mcx> {
    pub attnum: OffsetNumber,
    pub key: Datum<'mcx>,
    pub category: GinNullCategory,
    pub list: Vec<ItemPointerData>,
}

/// `ginBeginBAScan` + repeated `ginGetBAEntry` (ginbulk.c:256/267): iterate the
/// rbtree left-to-right, sorting each entry's TID list (C
/// `qsortCompareItemPointers`) when it was inserted out of order.
fn drain_ba_entries<'mcx>(accum: &BuildAccumulator<'mcx>) -> Vec<BAEntry<'mcx>> {
    let mut out: Vec<BAEntry<'mcx>> = Vec::new();
    let mut iter = rbt_begin_iterate(&accum.tree, RBTOrderControl::LeftRightWalk);
    while let Some(entry) = rbt_iterate(&mut iter) {
        debug_assert!(!entry.list.is_empty());
        let mut list = entry.list.clone();
        if entry.shouldSort && list.len() > 1 {
            // qsortCompareItemPointers (ginbulk.c:245).
            list.sort_by(|a, b| ginCompareItemPointers(a, b).cmp(&0));
        }
        out.push(BAEntry {
            attnum: entry.attnum,
            key: entry.key.clone(),
            category: entry.category,
            list,
        });
    }
    out
}

// ===========================================================================
// GinBuildState (gininsert.c) — serial-build subset.
// ===========================================================================

/// `GinBuildState` (gininsert.c) — serial-build subset. The parallel-build
/// fields (`bs_*`, `tid`, `work_mem`) are omitted (parallel build deferred).
struct GinBuildState<'mcx> {
    ginstate: Rc<GinState<'mcx>>,
    buildStats: GinStatsData,
    accum: BuildAccumulator<'mcx>,
    indtuples: f64,
}

// ===========================================================================
// ginHeapTupleBulkInsert (gininsert.c:417)
// ===========================================================================

/// `ginHeapTupleBulkInsert(buildstate, attnum, value, isNull, heapptr)`
/// (gininsert.c:417): extract one indexable item's entries and add them to the
/// accumulator. Used only during initial index creation.
fn ginHeapTupleBulkInsert<'mcx>(
    buildstate: &mut GinBuildState<'mcx>,
    mcx: Mcx<'mcx>,
    attnum: OffsetNumber,
    value: Datum<'mcx>,
    is_null: bool,
    heapptr: &ItemPointerData,
) -> PgResult<()> {
    // C runs ginExtractEntries in funcCtx (reset after); here the per-tuple
    // scratch rides mcx. The permanent datum copy (getDatumCopy) is taken inside
    // ginInsertBAEntry's isNew branch, exactly as in C.
    let (entries, categories) =
        ginExtractEntries(&buildstate.ginstate, attnum, value, is_null, mcx)?;
    let nentries = entries.len() as i32;

    ginInsertBAEntries(
        &mut buildstate.accum,
        heapptr,
        attnum,
        &entries,
        &categories,
        nentries,
    )?;

    buildstate.indtuples += nentries as f64;
    Ok(())
}

// ===========================================================================
// ginBuildCallback (gininsert.c:441)
// ===========================================================================

/// `ginBuildCallback(index, tid, values, isnull, tupleIsAlive, state)`
/// (gininsert.c:441): per-heap-tuple build-scan callback. Accumulate this
/// tuple's entries; when the accumulator hits `maintenance_work_mem`, dump every
/// key's TID list into the entry tree and reset.
fn ginBuildCallback<'mcx>(
    buildstate: &mut GinBuildState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    tid: ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    let natts = buildstate
        .ginstate
        .origTupdesc
        .as_ref()
        .map(|td| td.natts as usize)
        .unwrap_or(0);

    for i in 0..natts {
        ginHeapTupleBulkInsert(
            buildstate,
            mcx,
            (i + 1) as OffsetNumber,
            values[i].clone(),
            isnull[i],
            &tid,
        )?;
    }

    // If we've maxed out our available memory, dump everything to the index.
    let mwm_bytes =
        backend_utils_misc_guc_seams::maintenance_work_mem::call() as usize * 1024;
    if buildstate.accum.allocated_memory >= mwm_bytes {
        dump_accumulator(buildstate, mcx, index)?;
        buildstate.accum.ginInitBA();
    }

    Ok(())
}

/// Dump every accumulated `(attnum, key, category)` entry into the entry tree
/// via `ginEntryInsert` — the body shared by `ginBuildCallback`'s mid-scan dump
/// and `ginbuild`'s end-of-scan dump.
fn dump_accumulator<'mcx>(
    buildstate: &mut GinBuildState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<()> {
    let entries = drain_ba_entries(&buildstate.accum);
    buildstate.accum.take_cmp_error()?;
    let ginstate = Rc::clone(&buildstate.ginstate);
    for e in entries {
        // there could be many entries, so be willing to abort here
        // (CHECK_FOR_INTERRUPTS — no-op in the single-threaded port).
        ginEntryInsert(
            &ginstate,
            mcx,
            index,
            e.attnum,
            e.key,
            e.category,
            &e.list,
            e.list.len() as u32,
            Some(&mut buildstate.buildStats),
        )?;
    }
    Ok(())
}

// ===========================================================================
// ginbuild (gininsert.c:607)
// ===========================================================================

/// `ginbuild(heap, index, indexInfo)` (gininsert.c:607): the GIN `ambuild`
/// driver. Initialize the metapage + root, scan the heap accumulating entries,
/// dump them into the entry tree, update metapage stats and WAL-log the index.
pub fn ginbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if relation_get_number_of_blocks(index)? != 0 {
        return Err(PgError::error("index already contains data"));
    }

    let ginstate = Rc::new(initGinState(index, mcx)?);

    let mut buildstate = GinBuildState {
        ginstate: Rc::clone(&ginstate),
        buildStats: GinStatsData::default(),
        accum: new_build_accumulator(Rc::clone(&ginstate)),
        indtuples: 0.0,
    };

    // initialize the meta page
    let meta_buffer = GinNewBuffer(index)?;
    // initialize the root page
    let root_buffer = GinNewBuffer(index)?;

    // START_CRIT_SECTION();
    bufmgr::with_buffer_page::call(meta_buffer, &mut |page: &mut [u8]| {
        GinInitMetabuffer(page, page.len())
    })?;
    bufmgr::mark_buffer_dirty::call(meta_buffer);
    bufmgr::with_buffer_page::call(root_buffer, &mut |page: &mut [u8]| {
        GinInitBuffer(page, GIN_LEAF as u32)
    })?;
    bufmgr::mark_buffer_dirty::call(root_buffer);

    bufmgr::unlock_release_buffer::call(meta_buffer);
    bufmgr::unlock_release_buffer::call(root_buffer);
    // END_CRIT_SECTION();

    // count the root as first entry page
    buildstate.buildStats.nEntryPages += 1;

    // Report table scan phase started.
    backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
        PROGRESS_CREATEIDX_SUBPHASE,
        PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN,
    );

    // Serial build only — parallel build (indexInfo->ii_ParallelWorkers > 0) is
    // deferred (the parallel tuplesort/GinBuffer substrate is unported). Do the
    // heap scan; sync scan is disallowed (dataPlaceToPage prefers TID order).
    let reltuples = {
        let bs = &mut buildstate;
        let index_alias = index.alias();
        backend_access_table_tableam_seams::table_index_build_scan::call(
            mcx,
            heap,
            index,
            index_info,
            false,
            true,
            &mut |tid: ItemPointerData,
                  values: &[Datum<'mcx>],
                  isnull: &[bool],
                  _tuple_is_alive: bool|
                  -> PgResult<()> { ginBuildCallback(bs, mcx, &index_alias, tid, values, isnull) },
        )?
    };

    // dump remaining entries to the index
    dump_accumulator(&mut buildstate, mcx, index)?;

    // Update metapage stats.
    buildstate.buildStats.nTotalPages = relation_get_number_of_blocks(index)?;
    ginUpdateStats(index, &buildstate.buildStats, true)?;

    // We didn't write WAL records as we built the index, so if WAL-logging is
    // required, write all pages to the WAL now.
    if relation_needs_wal(index) {
        let nblocks = relation_get_number_of_blocks(index)?;
        backend_access_transam_xloginsert_seams::log_newpage_range::call(
            index,
            ForkNumber::MAIN_FORKNUM,
            0,
            nblocks,
            true,
        )?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: buildstate.indtuples,
    })
}

// ===========================================================================
// ginbuildempty (gininsert.c:801)
// ===========================================================================

/// `ginbuildempty(index)` (gininsert.c:801): build an empty GIN index in the
/// initialization fork (two pages: meta + root, both WAL-logged).
pub fn ginbuildempty<'mcx>(index: &Relation<'mcx>) -> PgResult<()> {
    // An empty GIN index has two pages.
    let meta_buffer =
        bufmgr::extend_buffered_rel::call(index, ForkNumber::INIT_FORKNUM)?;
    let root_buffer =
        bufmgr::extend_buffered_rel::call(index, ForkNumber::INIT_FORKNUM)?;

    // START_CRIT_SECTION();
    bufmgr::with_buffer_page::call(meta_buffer, &mut |page: &mut [u8]| {
        GinInitMetabuffer(page, page.len())
    })?;
    bufmgr::mark_buffer_dirty::call(meta_buffer);
    backend_access_transam_xloginsert_seams::log_newpage_buffer::call(meta_buffer, true)?;
    bufmgr::with_buffer_page::call(root_buffer, &mut |page: &mut [u8]| {
        GinInitBuffer(page, GIN_LEAF as u32)
    })?;
    bufmgr::mark_buffer_dirty::call(root_buffer);
    backend_access_transam_xloginsert_seams::log_newpage_buffer::call(root_buffer, false)?;
    // END_CRIT_SECTION();

    bufmgr::unlock_release_buffer::call(meta_buffer);
    bufmgr::unlock_release_buffer::call(root_buffer);
    Ok(())
}

// ===========================================================================
// progress codes + WAL / relcache helpers (local consts, per repo convention).
// ===========================================================================

/// `PROGRESS_CREATEIDX_SUBPHASE` (commands/progress.h).
const PROGRESS_CREATEIDX_SUBPHASE: i32 = 11;
/// `PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN` (commands/progress.h).
const PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN: i64 = 2;

/// `RelationGetNumberOfBlocks(rel)` (main fork) — via the relcache seam.
fn relation_get_number_of_blocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    backend_utils_cache_relcache_seams::relation_get_number_of_blocks::call(rel)
}

/// `RelationNeedsWAL(rel)` — via the relcache seam.
fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    backend_utils_cache_relcache_seams::relation_needs_wal::call(rel)
}

// ===========================================================================
// init_seams — gin-ginbulk owns the `ginbuild` / `ginbuildempty` AM
// build-dispatch seams (declared in `backend-access-gin-ginutil-seams`).
//
// `ginbuild`/`ginbuildempty` (the GIN AM `ambuild`/`ambuildempty` entries) live
// here, ABOVE the AM-vtable crate (`backend-access-gin-ginutil`) in the dep
// graph (ginbulk depends on ginutil), so the vtable's adapters
// (`ginbuild_am`/`ginbuildempty_am`) cannot call them directly. The cross-crate
// edge is bridged through the `ginbuild`/`ginbuildempty` seams, which this crate
// installs here: the adapter passes the `IndexInfoCarrier` (#342) through, and
// this installer downcasts it back to the real
// `types_nodes::execnodes::IndexInfo<'mcx>` before invoking the build. Mirrors
// the GiST `gistbuild` and nbtree `btbuild` build-dispatch seams.
// ===========================================================================

/// Install this crate's inward (build-dispatch) seams.
pub fn init_seams() {
    backend_access_gin_ginutil_seams::ginbuild::set(|mcx, heap, index, index_info| {
        // The dispatch layer (index.c) wraps the caller's owned
        // `&mut IndexInfo<'mcx>` in the carrier; recover the concrete struct
        // (tag-checked downcast — a NULL/wrong-type carrier is the C
        // NULL-pointer programming error).
        let info = index_info
            .downcast_mut::<IndexInfo<'_>>()
            .unwrap_or_else(|| {
                panic!("ginbuild: IndexInfoCarrier did not carry the expected IndexInfo")
            });
        ginbuild(mcx, heap, index, info)
    });
    backend_access_gin_ginutil_seams::ginbuildempty::set(|_mcx, index| ginbuildempty(index));
}
