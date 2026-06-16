//! Port of `src/backend/access/spgist/spginsert.c` (PostgreSQL 18.3): the
//! externally visible index creation/insertion routines for SP-GiST.
//!
//! All the actual insertion logic is in `spgdoinsert.c`
//! ([`crate::spgdoinsert`]); this module is the thin driver layer:
//! `spgbuild` (drives `table_index_build_scan`), `spgistBuildCallback`,
//! `spgbuildempty`, and `spginsert`.
//!
//! ## Memory model
//!
//! C creates a per-tuple temporary `MemoryContext` (`buildstate->tmpCtx` /
//! `insertCtx`) that `spgdoinsert`'s scratch allocations go into, resetting it
//! after each tuple (and between retries). In this repo's owned model that
//! scratch just rides the ambient `mcx` the driver threads through — the same
//! convention `brininsert`/`hashbuild` follow. The durable index data is
//! written into the shared buffer pages by `spgdoinsert`, not into the temp
//! context, so eliding the per-tuple context is behavior-preserving. The
//! retry loop (re-running `spgdoinsert` until it returns `true`) is mirrored
//! exactly.

use mcx::Mcx;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_smgr_bulkwrite_seams as bulk;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_utils_init_miscinit_seams as miscinit;

use types_core::primitive::ForkNumber;
use types_error::PgResult;
use types_rel::Relation;
use types_tableam::amapi::{IndexBuildResult, IndexUniqueCheck};
use types_tableam::index_info_carrier::IndexInfoCarrier;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use types_spgist::{
    SPGIST_LEAF, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_NULL_BLKNO, SPGIST_ROOT_BLKNO,
};

use crate::{
    initSpGistState, spgdoinsert::spgdoinsert, SpGistInitMetapage, SpGistInitPage,
    SpGistNewBuffer, SpGistUpdateMetaPage,
};
use types_spgist::SpGistState;

// ===========================================================================
// spgistBuildCallback (spginsert.c:40)
// ===========================================================================

/// Working state for `spgbuild` and its callback (`SpGistBuildState`).
struct SpGistBuildState<'mcx> {
    /// `SpGistState spgstate` — SPGiST's working state.
    spgstate: SpGistState<'mcx>,
    /// `int64 indtuples` — total number of tuples indexed.
    indtuples: i64,
    // `MemoryContext tmpCtx` — per-tuple temporary context. In the owned model
    // the scratch rides `mcx`, so there is no separate handle to carry.
}

/// `spgistBuildCallback(index, tid, values, isnull, tupleIsAlive, state)`
/// (spginsert.c:40) — process one heap tuple during `table_index_build_scan`.
fn spgistBuildCallback<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    tid: &ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    buildstate: &mut SpGistBuildState<'mcx>,
) -> PgResult<()> {
    // Even though no concurrent insertions can be happening, we still might get
    // a buffer-locking failure due to bgwriter or checkpointer taking a lock on
    // some buffer. So we need to be willing to retry. (C flushes the temp data
    // when retrying; here the scratch rides `mcx`.)
    while !spgdoinsert(mcx, index, &mut buildstate.spgstate, tid, values, isnull)? {
        // MemoryContextReset(buildstate->tmpCtx).
    }

    // Update total tuple count.
    buildstate.indtuples += 1;

    Ok(())
}

// ===========================================================================
// spgbuild (spginsert.c:72)
// ===========================================================================

/// `spgbuild(heap, index, indexInfo)` (spginsert.c:72) — build an SP-GiST
/// index.
pub fn spgbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        return Err(types_error::PgError::error(&alloc::format!(
            "index \"{}\" already contains data",
            index.name()
        )));
    }

    // Initialize the meta page and root pages.
    let metabuffer = SpGistNewBuffer(mcx, index)?;
    let rootbuffer = SpGistNewBuffer(mcx, index)?;
    let nullbuffer = SpGistNewBuffer(mcx, index)?;

    debug_assert_eq!(
        bufmgr::buffer_get_block_number::call(metabuffer),
        SPGIST_METAPAGE_BLKNO
    );
    debug_assert_eq!(
        bufmgr::buffer_get_block_number::call(rootbuffer),
        SPGIST_ROOT_BLKNO
    );
    debug_assert_eq!(
        bufmgr::buffer_get_block_number::call(nullbuffer),
        SPGIST_NULL_BLKNO
    );

    miscinit::start_crit_section::call();

    bufmgr::with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        SpGistInitMetapage(page)
    })?;
    bufmgr::mark_buffer_dirty::call(metabuffer);
    crate::SpGistInitBuffer(rootbuffer, SPGIST_LEAF)?;
    bufmgr::mark_buffer_dirty::call(rootbuffer);
    crate::SpGistInitBuffer(nullbuffer, SPGIST_LEAF | SPGIST_NULLS)?;
    bufmgr::mark_buffer_dirty::call(nullbuffer);

    miscinit::end_crit_section::call();

    bufmgr::unlock_release_buffer::call(metabuffer);
    bufmgr::unlock_release_buffer::call(rootbuffer);
    bufmgr::unlock_release_buffer::call(nullbuffer);

    // Now insert all the heap data into the index.
    let mut spgstate = initSpGistState(mcx, index)?;
    spgstate.isBuild = true;

    let mut buildstate = SpGistBuildState {
        spgstate,
        indtuples: 0,
    };

    let reltuples = {
        let bs = &mut buildstate;
        let index_alias = index.alias();
        backend_access_table_tableam_seams::table_index_build_scan::call(
            mcx,
            heap,
            index,
            index_info,
            true,
            true,
            &mut |tid: ItemPointerData,
                  values: &[Datum<'mcx>],
                  isnull: &[bool],
                  _tuple_is_alive: bool|
                  -> PgResult<()> {
                spgistBuildCallback(mcx, &index_alias, &tid, values, isnull, bs)
            },
        )?
    };

    SpGistUpdateMetaPage(index)?;

    // We didn't write WAL records as we built the index, so if WAL-logging is
    // required, write all pages to the WAL now.
    if backend_utils_cache_relcache_seams::relation_needs_wal::call(index) {
        let nblocks =
            bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?;
        xloginsert::log_newpage_range::call(index, ForkNumber::MAIN_FORKNUM, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: buildstate.indtuples as f64,
    })
}

// ===========================================================================
// spgbuildempty (spginsert.c:153)
// ===========================================================================

/// `spgbuildempty(index)` (spginsert.c:153) — build an empty SPGiST index in
/// the initialization fork.
pub fn spgbuildempty<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<()> {
    let mut bulkstate = bulk::smgr_bulk_start_rel::call(mcx, index, ForkNumber::INIT_FORKNUM)?;

    // Construct metapage.
    let mut buf = bulk::smgr_bulk_get_buf::call(mcx, &mut bulkstate)?;
    SpGistInitMetapage(&mut buf)?;
    bulk::smgr_bulk_write::call(&mut bulkstate, SPGIST_METAPAGE_BLKNO, buf, true)?;

    // Likewise for the root page.
    let mut buf = bulk::smgr_bulk_get_buf::call(mcx, &mut bulkstate)?;
    SpGistInitPage(&mut buf, SPGIST_LEAF)?;
    bulk::smgr_bulk_write::call(&mut bulkstate, SPGIST_ROOT_BLKNO, buf, true)?;

    // Likewise for the null-tuples root page.
    let mut buf = bulk::smgr_bulk_get_buf::call(mcx, &mut bulkstate)?;
    SpGistInitPage(&mut buf, SPGIST_LEAF | SPGIST_NULLS)?;
    bulk::smgr_bulk_write::call(&mut bulkstate, SPGIST_NULL_BLKNO, buf, true)?;

    bulk::smgr_bulk_finish::call(bulkstate)?;
    Ok(())
}

// ===========================================================================
// spginsert (spginsert.c:182)
// ===========================================================================

/// `spginsert(index, values, isnull, ht_ctid, heapRel, checkUnique,
/// indexUnchanged, indexInfo)` (spginsert.c:182) — insert one new tuple into
/// an SPGiST index. Always returns `false` (no unique check is done).
#[allow(clippy::too_many_arguments)]
pub fn spginsert<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    _heap_rel: &Relation<'mcx>,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<bool> {
    let mut spgstate = initSpGistState(mcx, index)?;

    // We might have to repeat spgdoinsert() multiple times, if conflicts occur
    // with concurrent insertions. If so, reset the insertCtx each time to avoid
    // cumulative memory consumption. That means we also have to redo
    // initSpGistState(), but it's cheap enough not to matter. (In the owned
    // model the scratch rides `mcx`; we still redo initSpGistState to match C.)
    while !spgdoinsert(mcx, index, &mut spgstate, ht_ctid, values, isnull)? {
        spgstate = initSpGistState(mcx, index)?;
    }

    SpGistUpdateMetaPage(index)?;

    // return false since we've not done any unique check.
    Ok(false)
}
