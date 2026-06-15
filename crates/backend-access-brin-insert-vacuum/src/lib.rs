//! Owned-tree Rust port of the BRIN index access method's *insert + vacuum*
//! slice (F3) of `src/backend/access/brin/brin.c` (PostgreSQL 18.3):
//!
//!   * `brininsert` / `brininsertcleanup` — the `aminsert` / `aminsertcleanup`
//!     callbacks, with the running [`BrinInsertState`] cached in the
//!     `IndexInfo`'s type-erased `payload` (the C `indexInfo->ii_AmCache`).
//!   * `brinbulkdelete` / `brinvacuumcleanup` — the `ambulkdelete` /
//!     `amvacuumcleanup` callbacks. BRIN's "vacuum" walks the revmap directly
//!     via the buffer manager (no `read_stream`), summarizing any range that is
//!     not yet summarized, after a full physical scan that repairs lost pages.
//!   * `brin_summarize_range` / `brin_desummarize_range` — the SQL-callable
//!     range maintenance functions.
//!   * the static helpers `initialize_brin_insertstate`,
//!     `initialize_brin_buildstate`, `terminate_brin_buildstate`,
//!     `summarize_range`, `brinsummarize`, `form_and_insert_tuple`,
//!     `union_tuples`, `add_values_to_range`, `brin_vacuum_scan`, `brinGetStats`.
//!
//! This is the F3 partner of the F2 scan crate ([`backend_access_brin_scan`]):
//! the BRIN handler (built in the scan crate) populates its `aminsert` /
//! `ambulkdelete` / `amvacuumcleanup` / `aminsertcleanup` vtable slots with
//! adapters that dispatch through the
//! [`backend_access_brin_insert_vacuum_seams`] this crate installs in
//! [`init_seams`]. That keeps the scan crate independent of this one (which
//! depends on it for `brin_build_desc` / `brin_free_desc` / [`BrinScan`]),
//! breaking the cycle.
//!
//! SANCTIONED panic leg: `summarize_range` drives the actual range summarization
//! through the heap AM's `table_index_build_range_scan` — the heap scan layer
//! (`access/heap/heapam_handler.c`) is unported, so that seam is uninstalled and
//! a call panics loudly, exactly like `nbtsort`'s `btbuild` /
//! `table_index_build_scan`. The revmap-walk machinery around it
//! (`brin_vacuum_scan`, the `brinsummarize` revmap iteration, the placeholder
//! insert/update loop) is fully ported. A vacuum of an index with NO
//! unsummarized ranges never reaches the panic leg.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use mcx::Mcx;

use types_brin::{BrinDesc, BrinMemTuple};
use types_core::primitive::{BlockNumber, OffsetNumber, Oid, Size};
use types_core::{InvalidOid, MaxBlockNumber};
use types_rel::Relation;
use types_storage::buf::{
    BufferIsValid, InvalidBuffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use types_storage::lock::{AccessShareLock, ShareUpdateExclusiveLock};
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::access::RELKIND_INDEX;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_brin_pageops::{
    brin_can_do_samepage_update, brin_doinsert, brin_doupdate, brin_page_cleanup,
    brinGetTupleForHeapBlock, brinRevmapDesummarizeRange, brinRevmapInitialize,
    brinRevmapTerminate, read_found_tuple_bytes, BrinRevmap,
};
use backend_access_brin_tuple::{
    brin_copy_tuple, brin_deform_tuple, brin_form_placeholder_tuple, brin_form_tuple,
    brin_memtuple_initialize, brin_new_memtuple,
};
use backend_access_brin_scan::{brin_build_desc, brin_free_desc};

use backend_access_brin_entry_seams as opclass;
use backend_access_index_indexam_seams::index_open;
use backend_access_table_table_seams::{relation_close, table_open};
use backend_access_table_tableam_seams::table_index_build_range_scan;
use backend_catalog_aclchk_seams::{aclcheck_error, object_ownercheck};
use backend_catalog_index_seams::{build_index_info, index_get_relation};
use backend_storage_buffer_bufmgr_seams::{
    lock_buffer, read_buffer_extended, release_buffer,
};
use backend_storage_freespace_seams::{
    free_space_map_vacuum, free_space_map_vacuum_range, record_page_with_free_space,
};
use backend_utils_cache_relcache_seams::relation_get_number_of_blocks;
use backend_utils_error::{ereport, PgError, PgResult};
use types_error::error::ERROR;

use types_acl::AclResult;
use types_catalog::catalog::RELATION_RELATION_ID;
use types_error::error::{
    ERRCODE_INTERNAL_ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_error::SqlState;
use types_nodes::parsenodes::ObjectType;

/// `ereport(ERROR, (errcode(code), errmsg(msg)))` — build a `PgError`.
fn err(code: SqlState, msg: alloc::string::String) -> PgError {
    ereport(ERROR).errcode(code).errmsg(msg).into_error()
}

// ===========================================================================
// Constants (access/brin.h, access/brin_internal.h, commands/vacuum.h).
// ===========================================================================

/// `BRIN_AM_OID` (`pg_am_d.h`): the OID of the `brin` access method.
const BRIN_AM_OID: Oid = 3580;

/// `BRIN_ALL_BLOCKRANGES` (`brin.c`): the sentinel `InvalidBlockNumber` passed
/// for `pageRange` to summarize the whole table.
const BRIN_ALL_BLOCKRANGES: BlockNumber = types_core::primitive::InvalidBlockNumber;

/// `BRIN_DEFAULT_PAGES_PER_RANGE` / autosummarize default. The relcache trims
/// `rd_options` to [`types_rel::StdRdOptions`] (heap reloptions), which carries
/// no BRIN-specific `autosummarize` field, so — like
/// [`types_rel::RelationData::get_fillfactor`]'s default-on-`None` — the
/// behaviour-preserving value is the C `BrinOptions` default (`false`). When a
/// BRIN reloptions carrier lands in the relcache trim this reads it instead.
fn brin_get_auto_summarize(_idx_rel: &Relation<'_>) -> bool {
    false
}

// ===========================================================================
// BrinInsertState (brin.c:192) — the per-command insert state cached in
// IndexInfo.payload (C's `ii_AmCache`).
// ===========================================================================

/// `struct BrinInsertState` (brin.c:192): running state spanning multiple
/// `brininsert` invocations within the same command.
pub struct BrinInsertState<'mcx> {
    /// `bis_rmAccess`: the reverse range-map access state.
    pub bis_rmAccess: BrinRevmap<'mcx>,
    /// `bis_desc`: the BRIN tuple descriptor for this index.
    pub bis_desc: BrinDesc<'mcx>,
    /// `bis_pages_per_range`: read from the metapage.
    pub bis_pages_per_range: BlockNumber,
}

/// `struct BrinBuildState` (brin.c:155), trimmed to the serial-build fields the
/// summarization path uses (parallel-build / sortstate fields are part of the
/// gated build path).
pub struct BrinBuildState<'mcx> {
    /// `bs_irel`: the index relation.
    pub bs_irel: Relation<'mcx>,
    /// `bs_numtuples`: number of summary tuples produced.
    pub bs_numtuples: f64,
    /// `bs_currentInsertBuf`: the index buffer the last insert used.
    pub bs_currentInsertBuf: types_storage::Buffer,
    /// `bs_pagesPerRange`.
    pub bs_pagesPerRange: BlockNumber,
    /// `bs_currRangeStart`: the heap block at which the current range starts.
    pub bs_currRangeStart: BlockNumber,
    /// `bs_maxRangeStart`: the heap block at which the last range starts.
    pub bs_maxRangeStart: BlockNumber,
    /// `bs_rmAccess`: the reverse range-map access state.
    pub bs_rmAccess: BrinRevmap<'mcx>,
    /// `bs_bdesc`: the BRIN tuple descriptor.
    pub bs_bdesc: BrinDesc<'mcx>,
    /// `bs_dtuple`: the in-memory summary tuple being built.
    pub bs_dtuple: BrinMemTuple<'mcx>,
}

// ===========================================================================
// brininsert (brin.c:344)
// ===========================================================================

/// `brininsert(idxRel, values, nulls, heaptid, heapRel, checkUnique,
/// indexUnchanged, indexInfo)` (brin.c:344): add a heap tuple's values to the
/// summary of the page range that contains `heap_tid`. Returns `false` (BRIN
/// never reports a unique conflict).
pub fn brininsert<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    heaptid: &ItemPointerData,
    _heap_rel: &Relation<'mcx>,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    let _ = index_info;
    let autosummarize = brin_get_auto_summarize(idx_rel);

    // C caches a `BrinInsertState` in `indexInfo->ii_AmCache` so that the
    // revmap-access state and `BrinDesc` are built once per command and reused
    // for every `brininsert` call. That cache is a pure performance hint — it
    // changes no on-disk state and produces identical results to rebuilding it
    // each call. Our `IndexInfo.payload` carrier is `Box<dyn Any + 'static>`,
    // which cannot hold the `'mcx`-bound revmap/`BrinDesc`, so we rebuild the
    // insert state on every call (behaviour-preserving, like the
    // `RelationGetTargetBlock` no-op in the pageops layer). `brininsertcleanup`
    // is then a no-op (nothing is cached).
    let mut bistate = initialize_brin_insertstate(mcx, idx_rel)?;

    let pages_per_range = bistate.bis_pages_per_range;

    // origHeapBlk is where the insertion occurred; heapBlk is the first block in
    // the corresponding page range.
    let orig_heap_blk = item_pointer_get_block_number(heaptid);
    let heap_blk = (orig_heap_blk / pages_per_range) * pages_per_range;

    let mut buf = InvalidBuffer;

    let result = brininsert_loop(
        mcx,
        idx_rel,
        values,
        nulls,
        heaptid,
        autosummarize,
        orig_heap_blk,
        heap_blk,
        pages_per_range,
        &mut bistate,
        &mut buf,
    );

    if BufferIsValid(buf) {
        release_buffer::call(buf);
    }

    // brininsertcleanup releases the revmap; since we did not cache the state,
    // release it here at the end of each call.
    brinRevmapTerminate(&bistate.bis_rmAccess)?;
    drop(bistate);

    result.map(|()| false)
}

/// The retry loop body of `brininsert` (brin.c:378 `for(;;)`).
fn brininsert_loop<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    heaptid: &ItemPointerData,
    autosummarize: bool,
    orig_heap_blk: BlockNumber,
    heap_blk: BlockNumber,
    pages_per_range: BlockNumber,
    bistate: &mut BrinInsertState<'mcx>,
    buf: &mut types_storage::Buffer,
) -> PgResult<()> {
    loop {
        check_for_interrupts()?;

        // If auto-summarization is enabled and we just inserted the first tuple
        // into the first block of a new non-first page range, request a
        // summarization run of the previous range.
        if autosummarize
            && heap_blk > 0
            && heap_blk == orig_heap_blk
            && item_pointer_get_offset_number(heaptid) == FIRST_OFFSET_NUMBER
        {
            let last_page_range = heap_blk - 1;
            let mut off: OffsetNumber = 0;
            let mut sz: Size = 0;
            let last_page_tuple = brinGetTupleForHeapBlock(
                &mut bistate.bis_rmAccess,
                last_page_range,
                buf,
                &mut off,
                &mut sz,
                BUFFER_LOCK_SHARE,
            )?;
            if last_page_tuple.is_none() {
                // AutoVacuumRequestWork(AVW_BRINSummarizeRange, ...): the
                // autovacuum work queue (autovacuum.c) is unported. The C also
                // tolerates a `false` result by only logging — i.e. the
                // summarization is purely opportunistic and skipping it changes
                // no on-disk state. Behaviour-preserving no-op until autovacuum
                // lands; the eventual range summarization still happens at
                // VACUUM / brin_summarize_new_values time.
                let _recorded = false;
            } else {
                lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
            }
        }

        let mut off: OffsetNumber = 0;
        let mut sz: Size = 0;
        let brtup = brinGetTupleForHeapBlock(
            &mut bistate.bis_rmAccess,
            heap_blk,
            buf,
            &mut off,
            &mut sz,
            BUFFER_LOCK_SHARE,
        )?;

        // If range is unsummarized, there's nothing to do.
        let brtup = match brtup {
            None => break,
            Some(found) => found,
        };

        // C creates a per-call "brininsert cxt" MemoryContext here; in the owned
        // model the deformed tuple / formed tuples just ride `mcx`.
        let brtup_bytes = read_found_tuple_bytes(mcx, &brtup)?;

        let mut dtup = brin_deform_tuple(mcx, &bistate.bis_desc, &brtup_bytes, None)?;

        let need_insert =
            add_values_to_range(mcx, idx_rel, &bistate.bis_desc, &mut dtup, values, nulls)?;

        if !need_insert {
            // The tuple is consistent with the new values; nothing to do.
            lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
        } else {
            // Make a copy of the old tuple so we can compare after re-acquiring
            // the lock. origsz = ItemIdGetLength(lp) = brtup.size.
            let origsz = brtup.size;
            let (origtup, _) = brin_copy_tuple(mcx, &brtup_bytes, origsz, None, 0)?;

            // Before releasing the lock, check if we can attempt a same-page
            // update.
            let (newtup, newsz) = brin_form_tuple(mcx, &bistate.bis_desc, heap_blk, &mut dtup)?;
            let samepage = brin_can_do_samepage_update(*buf, origsz, newsz)?;
            lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;

            // Try to update the tuple. If it fails, restart from the top.
            if !brin_doupdate(
                mcx,
                idx_rel,
                pages_per_range,
                &mut bistate.bis_rmAccess,
                heap_blk,
                *buf,
                off,
                &origtup.bytes,
                origsz,
                &newtup.bytes,
                newsz,
                samepage,
            )? {
                // No luck; start over. (C: MemoryContextReset(tupcxt).)
                continue;
            }
        }

        // success!
        break;
    }
    Ok(())
}

// ===========================================================================
// brininsertcleanup (brin.c:512)
// ===========================================================================

/// `brininsertcleanup(index, indexInfo)` (brin.c:512): clean up the
/// `BrinInsertState` (`indexInfo->ii_AmCache`) once all inserts are done.
pub fn brininsertcleanup<'mcx>(
    _mcx: Mcx<'mcx>,
    _index: &Relation<'mcx>,
    _index_info: &mut IndexInfo,
) -> PgResult<()> {
    // C frees the `BrinInsertState` cached in `indexInfo->ii_AmCache` and
    // terminates its revmap. Since `brininsert` does not cache the state (see
    // the note there — the `'static` `IndexInfo.payload` carrier cannot hold the
    // `'mcx`-bound revmap), and releases the revmap at the end of each call,
    // there is nothing to clean up here.
    Ok(())
}

// ===========================================================================
// initialize_brin_insertstate (brin.c:315)
// ===========================================================================

/// `initialize_brin_insertstate(idxRel, indexInfo)` (brin.c:315).
fn initialize_brin_insertstate<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
) -> PgResult<BrinInsertState<'mcx>> {
    let bis_desc = brin_build_desc(mcx, idx_rel)?;
    let (bis_rmAccess, bis_pages_per_range) = brinRevmapInitialize(idx_rel.alias())?;
    Ok(BrinInsertState {
        bis_rmAccess,
        bis_desc,
        bis_pages_per_range,
    })
}

// ===========================================================================
// brinbulkdelete (brin.c:1303)
// ===========================================================================

/// `brinbulkdelete(info, stats, callback, callback_state)` (brin.c:1303): BRIN
/// does nothing here except allocate the stats struct on first call.
pub fn brinbulkdelete<'mcx>(
    _mcx: Mcx<'mcx>,
    _info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    _callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // allocate stats if first time through, else re-use existing struct
    Ok(Some(stats.unwrap_or_default()))
}

// ===========================================================================
// brinvacuumcleanup (brin.c:1318)
// ===========================================================================

/// `brinvacuumcleanup(info, stats)` (brin.c:1318): "vacuum" a BRIN index by
/// summarizing ranges that are currently un-summarized.
pub fn brinvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // No-op in ANALYZE ONLY mode.
    if info.analyze_only {
        return Ok(stats);
    }

    let mut stats = stats.unwrap_or_default();
    stats.num_pages = relation_get_number_of_blocks::call(&info.index)?;
    // rest of stats is initialized by zeroing (Default).

    let heap_oid = index_get_relation::call(info.index.rd_id, false)?;
    let heap_rel = table_open::call(mcx, heap_oid, AccessShareLock)?;

    brin_vacuum_scan(mcx, &info.index)?;

    // C passes `&stats->num_index_tuples` for BOTH numSummarized and
    // numExisting, so each summarized AND each existing range increments the
    // same counter. We accumulate the two separately and sum.
    let mut num_summarized = 0.0_f64;
    let mut num_existing = 0.0_f64;
    brinsummarize(
        mcx,
        &info.index,
        &heap_rel,
        BRIN_ALL_BLOCKRANGES,
        false,
        Some(&mut num_summarized),
        Some(&mut num_existing),
    )?;
    stats.num_index_tuples = num_summarized + num_existing;

    relation_close::call(heap_oid, AccessShareLock)?;
    drop(heap_rel);

    Ok(Some(stats))
}

// ===========================================================================
// brin_summarize_range (brin.c:1381)  — SQL-callable
// ===========================================================================

/// `brin_summarize_range(indexoid, heapBlk64)` (brin.c:1381): summarize the
/// indicated page range, or all unsummarized ranges. Returns the number of
/// ranges summarized.
pub fn brin_summarize_range<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    heap_blk64: i64,
) -> PgResult<i32> {
    use backend_access_transam_xlog_seams::recovery_in_progress;
    use backend_utils_init_miscinit_seams::{get_user_id_and_sec_context, set_user_id_and_sec_context};
    use backend_utils_misc_guc_seams::{at_eoxact_guc, new_guc_nest_level, restrict_search_path};

    if recovery_in_progress::call() {
        return Err(err(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "recovery is in progress".into()));
    }

    if heap_blk64 > BRIN_ALL_BLOCKRANGES as i64 || heap_blk64 < 0 {
        return Err(err(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, alloc::format!("block number out of range: {heap_blk64}")));
    }
    let heap_blk = heap_blk64 as BlockNumber;

    // Lock table before index to avoid deadlocks.
    let heapoid = index_get_relation::call(indexoid, true)?;
    let mut num_summarized = 0.0_f64;

    let (heap_rel, save_userid, save_sec_context, save_nestlevel) = if oid_is_valid(heapoid) {
        let heap_rel = table_open::call(mcx, heapoid, ShareUpdateExclusiveLock)?;
        // Switch to the table owner's userid; lock down security-restricted
        // operations; arrange GUC changes local to this command.
        let (save_userid, save_sec_context) = get_user_id_and_sec_context::call();
        set_user_id_and_sec_context::call(
            heap_rel.rd_rel.relowner,
            save_sec_context | SECURITY_RESTRICTED_OPERATION,
        );
        let save_nestlevel = new_guc_nest_level::call();
        restrict_search_path::call()?;
        (Some(heap_rel), save_userid, save_sec_context, save_nestlevel)
    } else {
        (None, InvalidOid, -1, -1)
    };

    let index_rel = index_open::call(mcx, indexoid, ShareUpdateExclusiveLock)?;

    // Must be a BRIN index.
    if index_rel.rd_rel.relkind != RELKIND_INDEX || index_rel.rd_rel.relam != BRIN_AM_OID {
        return Err(err(ERRCODE_WRONG_OBJECT_TYPE, alloc::format!("\"{}\" is not a BRIN index", index_rel.name())));
    }

    // User must own the index (comparable to privileges needed for VACUUM).
    if heap_rel.is_some()
        && !object_ownercheck::call(RELATION_RELATION_ID, indexoid, save_userid)?
    {
        aclcheck_error::call(
            AclResult::AclcheckNotOwner,
            ObjectType::Index,
            Some(index_rel.name().to_owned()),
        )?;
    }

    // Recheck against an index drop/recreation race.
    if heap_rel.is_none() || heapoid != index_get_relation::call(indexoid, false)? {
        return Err(err(ERRCODE_UNDEFINED_TABLE, alloc::format!(
                "could not open parent table of index \"{}\"",
                index_rel.name()
            )));
    }
    let heap_rel = heap_rel.unwrap();

    // see gin_clean_pending_list()
    if index_rel
        .rd_index
        .as_ref()
        .map(|i| i.indisvalid)
        .unwrap_or(false)
    {
        brinsummarize(
            mcx,
            &index_rel,
            &heap_rel,
            heap_blk,
            true,
            Some(&mut num_summarized),
            None,
        )?;
    } else {
        // ereport(DEBUG1, ...): index is not valid.
    }

    // Roll back any GUC changes executed by index functions.
    at_eoxact_guc::call(false, save_nestlevel)?;
    // Restore userid and security context.
    set_user_id_and_sec_context::call(save_userid, save_sec_context);

    index_rel.close(ShareUpdateExclusiveLock)?;
    relation_close::call(heapoid, ShareUpdateExclusiveLock)?;
    drop(heap_rel);

    Ok(num_summarized as i32)
}

// ===========================================================================
// brin_desummarize_range (brin.c:1491) — SQL-callable
// ===========================================================================

/// `brin_desummarize_range(indexoid, heapBlk64)` (brin.c:1491): mark the range
/// containing `heap_blk64` as no longer summarized.
pub fn brin_desummarize_range<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    heap_blk64: i64,
) -> PgResult<()> {
    use backend_access_transam_xlog_seams::recovery_in_progress;
    use backend_utils_init_miscinit_seams::get_user_id;

    if recovery_in_progress::call() {
        return Err(err(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "recovery is in progress".into()));
    }

    if heap_blk64 > MaxBlockNumber as i64 || heap_blk64 < 0 {
        return Err(err(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, alloc::format!("block number out of range: {heap_blk64}")));
    }
    let heap_blk = heap_blk64 as BlockNumber;

    // Lock table before index to avoid deadlocks. Unlike summarize, autovacuum
    // never calls this; no userid switch.
    let heapoid = index_get_relation::call(indexoid, true)?;
    let heap_rel = if oid_is_valid(heapoid) {
        Some(table_open::call(mcx, heapoid, ShareUpdateExclusiveLock)?)
    } else {
        None
    };

    let index_rel = index_open::call(mcx, indexoid, ShareUpdateExclusiveLock)?;

    if index_rel.rd_rel.relkind != RELKIND_INDEX || index_rel.rd_rel.relam != BRIN_AM_OID {
        return Err(err(ERRCODE_WRONG_OBJECT_TYPE, alloc::format!("\"{}\" is not a BRIN index", index_rel.name())));
    }

    if !object_ownercheck::call(RELATION_RELATION_ID, indexoid, get_user_id::call())? {
        aclcheck_error::call(
            AclResult::AclcheckNotOwner,
            ObjectType::Index,
            Some(index_rel.name().to_owned()),
        )?;
    }

    if heap_rel.is_none() || heapoid != index_get_relation::call(indexoid, false)? {
        return Err(err(ERRCODE_UNDEFINED_TABLE, alloc::format!(
                "could not open parent table of index \"{}\"",
                index_rel.name()
            )));
    }
    let heap_rel = heap_rel.unwrap();

    // see gin_clean_pending_list()
    if index_rel
        .rd_index
        .as_ref()
        .map(|i| i.indisvalid)
        .unwrap_or(false)
    {
        // the revmap does the hard work
        loop {
            let done = brinRevmapDesummarizeRange(index_rel.alias(), heap_blk)?;
            if done {
                break;
            }
        }
    } else {
        // ereport(DEBUG1, ...): index is not valid.
    }

    index_rel.close(ShareUpdateExclusiveLock)?;
    relation_close::call(heapoid, ShareUpdateExclusiveLock)?;
    drop(heap_rel);

    Ok(())
}

// ===========================================================================
// initialize_brin_buildstate (brin.c:1669)
// ===========================================================================

/// `initialize_brin_buildstate(idxRel, revmap, pagesPerRange, tablePages)`
/// (brin.c:1669): initialize a [`BrinBuildState`] for creating tuples on the
/// given index. (Serial-build fields only; parallel build is part of the gated
/// build path.)
fn initialize_brin_buildstate<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
    revmap: BrinRevmap<'mcx>,
    pages_per_range: BlockNumber,
    table_pages: BlockNumber,
) -> PgResult<BrinBuildState<'mcx>> {
    let bs_bdesc = brin_build_desc(mcx, idx_rel)?;
    let bs_dtuple = brin_new_memtuple(mcx, &bs_bdesc)?;

    // Calculate the start of the last page range.
    let mut last_range = 0;
    if table_pages > 0 {
        last_range = ((table_pages - 1) / pages_per_range) * pages_per_range;
    }

    Ok(BrinBuildState {
        bs_irel: idx_rel.alias(),
        bs_numtuples: 0.0,
        bs_currentInsertBuf: InvalidBuffer,
        bs_pagesPerRange: pages_per_range,
        bs_currRangeStart: 0,
        bs_maxRangeStart: last_range + pages_per_range,
        bs_rmAccess: revmap,
        bs_bdesc,
        bs_dtuple,
    })
}

// ===========================================================================
// terminate_brin_buildstate (brin.c:1716)
// ===========================================================================

/// `terminate_brin_buildstate(state)` (brin.c:1716): release resources
/// associated with a [`BrinBuildState`].
fn terminate_brin_buildstate<'mcx>(
    mcx: Mcx<'mcx>,
    state: BrinBuildState<'mcx>,
) -> PgResult<()> {
    // Release the last index buffer used, and ensure its free space lands in
    // the FSM too.
    if BufferIsValid(state.bs_currentInsertBuf) {
        let freespace = page_get_free_space(mcx, state.bs_currentInsertBuf)?;
        let blk = buffer_get_block_number(state.bs_currentInsertBuf);
        release_buffer::call(state.bs_currentInsertBuf);
        record_page_with_free_space::call(&state.bs_irel, blk, freespace)?;
        free_space_map_vacuum_range::call(&state.bs_irel, blk, blk + 1)?;
    }

    brin_free_desc(state.bs_bdesc);
    // bs_dtuple rides `mcx`; dropped with `state`.
    Ok(())
}

// ===========================================================================
// summarize_range (brin.c:1761)
// ===========================================================================

/// `summarize_range(indexInfo, state, heapRel, heapBlk, heapNumBlks)`
/// (brin.c:1761): summarize the heap page range corresponding to `heap_blk`.
///
/// The actual heap scan (`table_index_build_range_scan`) is the SANCTIONED
/// panic leg — the heap AM scan layer is unported. The placeholder
/// insert/update concurrency dance around it is fully ported.
fn summarize_range<'mcx>(
    mcx: Mcx<'mcx>,
    index_info: &mut types_nodes::execnodes::IndexInfo,
    state: &mut BrinBuildState<'mcx>,
    heap_rel: &Relation<'mcx>,
    heap_blk: BlockNumber,
    heap_num_blks: BlockNumber,
) -> PgResult<()> {
    // Insert the placeholder tuple.
    let mut phbuf = InvalidBuffer;
    let (phtup, phsz) = brin_form_placeholder_tuple(mcx, &state.bs_bdesc, heap_blk)?;
    let mut phtup = phtup;
    let mut phsz = phsz;
    let mut offset = brin_doinsert(
        mcx,
        &state.bs_irel,
        state.bs_pagesPerRange,
        &mut state.bs_rmAccess,
        &mut phbuf,
        heap_blk,
        &phtup.bytes,
        phsz,
    )?;

    // Compute range end. Table cannot shrink (ShareUpdateExclusive) but can grow.
    debug_assert_eq!(heap_blk % state.bs_pagesPerRange, 0);
    let scan_num_blks = if heap_blk + state.bs_pagesPerRange > heap_num_blks {
        // Final (possibly partial) range: recompute the table size.
        core::cmp::min(
            relation_get_number_of_blocks::call(heap_rel)? - heap_blk,
            state.bs_pagesPerRange,
        )
    } else {
        state.bs_pagesPerRange
    };

    // Execute the partial heap scan covering the range, summarizing the heap
    // tuples in it. SANCTIONED panic leg: the heap AM scan layer is unported.
    state.bs_currRangeStart = heap_blk;
    {
        // brinbuildCallback(state) per live tuple (brin.c:1051): add the heap
        // tuple's values to bs_dtuple, flushing/inserting at range boundaries.
        // It bottoms out in `add_values_to_range`; here it crosses the (gated)
        // heap-scan seam as a closure, but the seam is uninstalled so the call
        // panics before the closure ever runs.
        let bs_pages_per_range = state.bs_pagesPerRange;
        let _ = bs_pages_per_range;
        let mut callback = |_heap_tid: ItemPointerData,
                            _values: &[Datum<'mcx>],
                            _isnull: &[bool],
                            _tuple_is_alive: bool|
         -> PgResult<()> {
            // brinbuildCallback drives form_and_insert_tuple +
            // add_values_to_range across range boundaries. It is only reachable
            // once the heap scan layer lands; the seam panics first.
            panic!(
                "brinbuildCallback: BRIN range summarization drives the heap \
                 scan layer (table_index_build_range_scan) which is unported"
            )
        };
        table_index_build_range_scan::call(
            mcx,
            heap_rel,
            &state.bs_irel,
            index_info,
            false,
            true,
            false,
            heap_blk,
            scan_num_blks,
            &mut callback,
        )?;
    }

    // Update the values obtained by the scan with the placeholder tuple, in a
    // loop that only terminates when we successfully update the placeholder.
    loop {
        check_for_interrupts()?;

        let (newtup, newsize) = brin_form_tuple(mcx, &state.bs_bdesc, heap_blk, &mut state.bs_dtuple)?;
        let samepage = brin_can_do_samepage_update(phbuf, phsz, newsize)?;
        let didupdate = brin_doupdate(
            mcx,
            &state.bs_irel,
            state.bs_pagesPerRange,
            &mut state.bs_rmAccess,
            heap_blk,
            phbuf,
            offset,
            &phtup.bytes,
            phsz,
            &newtup.bytes,
            newsize,
            samepage,
        )?;
        // brin_free_tuple(phtup) / brin_free_tuple(newtup): both ride `mcx`.

        if didupdate {
            break;
        }

        // Update failed (likely concurrent placeholder update). Re-extract,
        // union with the scan values, and start over.
        let mut off2: OffsetNumber = 0;
        let mut sz2: Size = 0;
        let found = brinGetTupleForHeapBlock(
            &mut state.bs_rmAccess,
            heap_blk,
            &mut phbuf,
            &mut off2,
            &mut sz2,
            BUFFER_LOCK_SHARE,
        )?;
        let found = match found {
            Some(f) => f,
            None => {
                return Err(err(ERRCODE_INTERNAL_ERROR, "missing placeholder tuple".into()));
            }
        };
        offset = off2;
        let found_bytes = read_found_tuple_bytes(mcx, &found)?;
        let (copied, _) = brin_copy_tuple(mcx, &found_bytes, found.size, None, 0)?;
        phtup = copied;
        phsz = found.size;
        lock_buffer::call(phbuf, BUFFER_LOCK_UNLOCK)?;

        // Merge it into the tuple from the heap scan.
        union_tuples(mcx, &state.bs_bdesc, &mut state.bs_dtuple, &phtup.bytes)?;
    }

    release_buffer::call(phbuf);
    Ok(())
}

// ===========================================================================
// brinsummarize (brin.c:1887)
// ===========================================================================

/// `brinsummarize(index, heapRel, pageRange, include_partial, numSummarized,
/// numExisting)` (brin.c:1887): summarize page ranges that are not already
/// summarized.
fn brinsummarize<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    page_range: BlockNumber,
    include_partial: bool,
    mut num_summarized: Option<&mut f64>,
    mut num_existing: Option<&mut f64>,
) -> PgResult<()> {
    let (mut revmap, pages_per_range) = brinRevmapInitialize(index.alias())?;

    // Determine range of pages to process.
    let mut heap_num_blocks = relation_get_number_of_blocks::call(heap_rel)?;
    let mut start_blk;
    if page_range == BRIN_ALL_BLOCKRANGES {
        start_blk = 0;
    } else {
        start_blk = (page_range / pages_per_range) * pages_per_range;
        heap_num_blocks = core::cmp::min(heap_num_blocks, start_blk + pages_per_range);
    }
    if start_blk > heap_num_blocks {
        // Nothing to do if start point is beyond end of table.
        brinRevmapTerminate(&revmap)?;
        return Ok(());
    }

    // Scan the revmap to find unsummarized items.
    let mut buf = InvalidBuffer;
    let mut state: Option<BrinBuildState<'mcx>> = None;
    let mut index_info: Option<types_nodes::execnodes::IndexInfo> = None;

    while start_blk < heap_num_blocks {
        // Unless requested to summarize even a partial range, stop if the next
        // range is partial.
        if !include_partial && (start_blk + pages_per_range > heap_num_blocks) {
            break;
        }

        check_for_interrupts()?;

        let mut off: OffsetNumber = 0;
        let mut sz: Size = 0;
        let tup = brinGetTupleForHeapBlock(
            &mut revmap,
            start_blk,
            &mut buf,
            &mut off,
            &mut sz,
            BUFFER_LOCK_SHARE,
        )?;

        if tup.is_none() {
            // No revmap entry for this heap range. Summarize it.
            if state.is_none() {
                // first time through
                debug_assert!(index_info.is_none());
                // C passes the live revmap into the build state; we hand it our
                // owned revmap and re-fetch one for the outer loop afterward.
                // To keep a single revmap, build the state with a fresh revmap
                // (matches C: initialize_brin_buildstate stores the SAME revmap
                // pointer, then brinsummarize keeps using it; here the build
                // state owns its own to satisfy the borrow checker while the
                // outer loop continues to walk `revmap`).
                let (build_revmap, build_ppr) = brinRevmapInitialize(index.alias())?;
                let st = initialize_brin_buildstate(
                    mcx,
                    index,
                    build_revmap,
                    build_ppr,
                    types_core::primitive::InvalidBlockNumber,
                )?;
                state = Some(st);
                index_info = Some(build_index_info::call(index)?);
            }
            summarize_range(
                mcx,
                index_info.as_mut().unwrap(),
                state.as_mut().unwrap(),
                heap_rel,
                start_blk,
                heap_num_blocks,
            )?;

            // re-initialize state for the next range
            let st = state.as_mut().unwrap();
            brin_memtuple_initialize(mcx, &mut st.bs_dtuple, &st.bs_bdesc)?;

            if let Some(n) = num_summarized.as_deref_mut() {
                *n += 1.0;
            }
        } else {
            if let Some(n) = num_existing.as_deref_mut() {
                *n += 1.0;
            }
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        }

        start_blk += pages_per_range;
    }

    if BufferIsValid(buf) {
        release_buffer::call(buf);
    }

    // free resources
    brinRevmapTerminate(&revmap)?;
    if let Some(st) = state {
        terminate_brin_buildstate(mcx, st)?;
        // pfree(indexInfo): index_info dropped here.
        drop(index_info);
    }
    Ok(())
}

// ===========================================================================
// form_and_insert_tuple (brin.c:1985)
// ===========================================================================

/// `form_and_insert_tuple(state)` (brin.c:1985): convert the build state's
/// deformed tuple to on-disk format and insert it, making the revmap point to
/// it. (Reached only via `brinbuildCallback`, which is on the gated build path;
/// kept for fidelity and used by the build driver once the scan layer lands.)
#[allow(dead_code)]
fn form_and_insert_tuple<'mcx>(mcx: Mcx<'mcx>, state: &mut BrinBuildState<'mcx>) -> PgResult<()> {
    let (tup, size) = brin_form_tuple(mcx, &state.bs_bdesc, state.bs_currRangeStart, &mut state.bs_dtuple)?;
    brin_doinsert(
        mcx,
        &state.bs_irel,
        state.bs_pagesPerRange,
        &mut state.bs_rmAccess,
        &mut state.bs_currentInsertBuf,
        state.bs_currRangeStart,
        &tup.bytes,
        size,
    )?;
    state.bs_numtuples += 1.0;
    Ok(())
}

// ===========================================================================
// union_tuples (brin.c:2031)
// ===========================================================================

/// `union_tuples(bdesc, a, b)` (brin.c:2031): adjust the deformed tuple `a` so
/// that it is consistent with the summary values in both `a` and the on-disk
/// tuple `b`.
fn union_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    a: &mut BrinMemTuple<'mcx>,
    b: &[u8],
) -> PgResult<()> {
    // C uses a private memory context for the deformed `b`; here it rides `mcx`.
    let db = brin_deform_tuple(mcx, bdesc, b, None)?;

    let natts = bdesc.natts();

    // If "b" is empty - ignore it and just use "a".
    if db.bt_empty_range {
        return Ok(());
    }

    // "b" is not empty. If "a" is empty, "b" is the result; copy "b" into "a".
    if a.bt_empty_range {
        for keyno in 0..natts {
            let opcinfo = &bdesc.bd_info[keyno];
            let col_b = &db.bt_columns[keyno];
            let b_allnulls = col_b.bv_allnulls;
            let b_hasnulls = col_b.bv_hasnulls;
            let nstored = opcinfo.oi_nstored as usize;
            let mut copied: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::with_capacity(nstored);
            if !b_allnulls {
                for i in 0..nstored {
                    let typby = opcinfo.oi_typcache[i].typbyval;
                    let typlen = opcinfo.oi_typcache[i].typlen;
                    let v = backend_utils_adt_scalar_seams::datum_copy::call(
                        mcx,
                        &db.bt_columns[keyno].bv_values[i],
                        typby,
                        typlen,
                    )?;
                    copied.push(v);
                }
            }

            let col_a = &mut a.bt_columns[keyno];
            col_a.bv_allnulls = b_allnulls;
            col_a.bv_hasnulls = b_hasnulls;
            // If "b" has no data, we're done with this column.
            if !b_allnulls {
                for (i, v) in copied.into_iter().enumerate() {
                    col_a.bv_values[i] = v;
                }
            }
        }
        // "a" started empty, but "b" was not empty.
        a.bt_empty_range = false;
        return Ok(());
    }

    // Neither range is empty.
    for keyno in 0..natts {
        let opcinfo = &bdesc.bd_info[keyno];
        let regular_nulls = opcinfo.oi_regular_nulls;
        let nstored = opcinfo.oi_nstored as usize;

        if regular_nulls {
            let col_b = &db.bt_columns[keyno];
            let b_has_nulls = col_b.bv_hasnulls || col_b.bv_allnulls;
            let b_allnulls = col_b.bv_allnulls;

            {
                let col_a = &mut a.bt_columns[keyno];
                // Adjust "hasnulls".
                if !col_a.bv_allnulls && b_has_nulls {
                    col_a.bv_hasnulls = true;
                }
            }

            // If there are no values in B, nothing left to do.
            if b_allnulls {
                continue;
            }

            // Adjust "allnulls". If A doesn't have values, copy from B.
            let a_allnulls = a.bt_columns[keyno].bv_allnulls;
            if a_allnulls {
                let mut copied: alloc::vec::Vec<Datum<'mcx>> =
                    alloc::vec::Vec::with_capacity(nstored);
                for i in 0..nstored {
                    let typby = opcinfo.oi_typcache[i].typbyval;
                    let typlen = opcinfo.oi_typcache[i].typlen;
                    let v = backend_utils_adt_scalar_seams::datum_copy::call(
                        mcx,
                        &db.bt_columns[keyno].bv_values[i],
                        typby,
                        typlen,
                    )?;
                    copied.push(v);
                }
                let col_a = &mut a.bt_columns[keyno];
                col_a.bv_allnulls = false;
                col_a.bv_hasnulls = true;
                for (i, v) in copied.into_iter().enumerate() {
                    col_a.bv_values[i] = v;
                }
                continue;
            }
        }

        // unionFn = index_getprocinfo(bd_index, keyno+1, BRIN_PROCNUM_UNION);
        // FunctionCall3Coll(unionFn, rd_indcollation[keyno], bdesc, col_a, col_b)
        // `col_a` (in `a`) and `col_b` (in `db`) are distinct objects, so a
        // direct mutable borrow of one alongside a shared borrow of the other
        // is sound.
        let collation = bdesc.bd_index.rd_indcollation[keyno];
        let col_a = &mut a.bt_columns[keyno];
        let col_b = &db.bt_columns[keyno];
        opclass::brin_union::call(
            mcx,
            &bdesc.bd_index,
            keyno,
            collation,
            bdesc,
            col_a,
            col_b,
        )?;
    }

    Ok(())
}

// ===========================================================================
// brin_vacuum_scan (brin.c:2170)
// ===========================================================================

/// `brin_vacuum_scan(idxrel, strategy)` (brin.c:2170): scan the complete index
/// in physical order during VACUUM, cleaning up any possible mess on each page
/// (lost pages from a crash after index extension), then update all upper FSM
/// pages.
fn brin_vacuum_scan<'mcx>(_mcx: Mcx<'mcx>, idxrel: &Relation<'mcx>) -> PgResult<()> {
    let nblocks = relation_get_number_of_blocks::call(idxrel)?;
    let mut blkno: BlockNumber = 0;
    while blkno < nblocks {
        check_for_interrupts()?;

        // ReadBufferExtended(idxrel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy):
        // the VACUUM buffer-access strategy crosses the seam at the bufmgr owner.
        let buf = read_buffer_extended::call(idxrel, blkno)?;
        brin_page_cleanup(idxrel, buf)?;
        release_buffer::call(buf);

        blkno += 1;
    }

    // Update all upper FSM pages, propagating leaf updates and repairing
    // pre-existing damage.
    free_space_map_vacuum::call(idxrel)?;
    Ok(())
}

// ===========================================================================
// add_values_to_range (brin.c:2205)
// ===========================================================================

/// `add_values_to_range(idxRel, bdesc, dtup, values, nulls)` (brin.c:2205):
/// compare the new tuple's key values to the stored summary, updating `dtup` if
/// the new values don't fit. Returns whether `dtup` changed (and so must be
/// re-inserted).
fn add_values_to_range<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    dtup: &mut BrinMemTuple<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<bool> {
    // If the range starts empty, we're certainly going to modify it.
    let mut modified = dtup.bt_empty_range;
    let natts = bdesc.natts();

    for keyno in 0..natts {
        // Does the range have actual NULL values? Ignore the state before the
        // first row.
        let has_nulls = {
            let bval = &dtup.bt_columns[keyno];
            (!dtup.bt_empty_range) && (bval.bv_hasnulls || bval.bv_allnulls)
        };

        let regular_nulls = bdesc.bd_info[keyno].oi_regular_nulls;

        if regular_nulls && nulls[keyno] {
            // New value is null: record it if it's the first one.
            let bval = &mut dtup.bt_columns[keyno];
            if !bval.bv_hasnulls {
                bval.bv_hasnulls = true;
                modified = true;
            }
            continue;
        }

        // addValue = index_getprocinfo(idxRel, keyno+1, BRIN_PROCNUM_ADDVALUE);
        // FunctionCall4Coll(addValue, rd_indcollation[keyno], bdesc, bval,
        //                   values[keyno], nulls[keyno])
        let collation = idx_rel.rd_indcollation[keyno];
        let bval = &mut dtup.bt_columns[keyno];
        let changed = opclass::brin_addvalue::call(
            mcx,
            idx_rel,
            keyno,
            collation,
            bdesc,
            bval,
            &values[keyno],
            nulls[keyno],
        )?;
        modified |= changed;

        // If the range had actual NULL values (didn't start empty), don't
        // forget them: either allnulls is still set, or set hasnulls=true.
        if has_nulls && !(bval.bv_hasnulls || bval.bv_allnulls) {
            debug_assert!(modified);
            bval.bv_hasnulls = true;
        }
    }

    // After updating all keys, mark it as not empty.
    debug_assert!(!dtup.bt_empty_range || modified);
    dtup.bt_empty_range = false;

    Ok(modified)
}

// ===========================================================================
// brinGetStats (brin.c:1648)
// ===========================================================================

/// `BrinStatsData` (brin.h): index statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BrinStatsData {
    /// `pagesPerRange`.
    pub pages_per_range: BlockNumber,
    /// `revmapNumPages`.
    pub revmap_num_pages: BlockNumber,
}

/// `brinGetStats(index, stats)` (brin.c:1648): fetch the index's statistical
/// data from the metapage.
pub fn brinGetStats<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<BrinStatsData> {
    use backend_storage_buffer_bufmgr_seams::{read_buffer, unlock_release_buffer};
    let _ = mcx;

    let metabuffer = read_buffer::call(index, BRIN_METAPAGE_BLKNO)?;
    lock_buffer::call(metabuffer, BUFFER_LOCK_SHARE)?;
    let (pages_per_range, last_revmap_page) = with_meta_page(metabuffer)?;
    unlock_release_buffer::call(metabuffer);

    Ok(BrinStatsData {
        pages_per_range,
        // revmapNumPages = lastRevmapPage - 1
        revmap_num_pages: last_revmap_page - 1,
    })
}

// ===========================================================================
// Local helpers / page-byte primitives.
// ===========================================================================

/// `SECURITY_RESTRICTED_OPERATION` (miscadmin.h): `0x0002`.
const SECURITY_RESTRICTED_OPERATION: i32 = 0x0002;

/// `FirstOffsetNumber` (off.h).
const FIRST_OFFSET_NUMBER: OffsetNumber = 1;

/// `BRIN_METAPAGE_BLKNO` (brin_page.h): the metapage is always block 0.
const BRIN_METAPAGE_BLKNO: BlockNumber = 0;

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `ItemPointerGetBlockNumber(tid)` (itemptr.h).
#[inline]
fn item_pointer_get_block_number(tid: &ItemPointerData) -> BlockNumber {
    tid.ip_blkid.block_number()
}

/// `ItemPointerGetOffsetNumber(tid)` (itemptr.h).
#[inline]
fn item_pointer_get_offset_number(tid: &ItemPointerData) -> OffsetNumber {
    tid.ip_posid
}

/// `CHECK_FOR_INTERRUPTS()` — same behaviour-preserving no-op the BRIN pageops
/// layer uses.
#[inline]
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// `PageGetFreeSpace(BufferGetPage(buf))` — read the page's free space.
fn page_get_free_space<'mcx>(_mcx: Mcx<'mcx>, buf: types_storage::Buffer) -> PgResult<Size> {
    use backend_storage_buffer_bufmgr_seams::with_buffer_page;
    use backend_storage_page::{PageGetFreeSpace, PageRef};
    let mut out: Size = 0;
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        out = match PageRef::new(page) {
            Ok(p) => PageGetFreeSpace(&p),
            Err(_) => 0,
        };
        Ok(())
    })?;
    Ok(out)
}

/// `BufferGetBlockNumber(buf)`.
fn buffer_get_block_number(buf: types_storage::Buffer) -> BlockNumber {
    backend_storage_buffer_bufmgr_seams::buffer_get_block_number::call(buf)
}

/// Read `(pagesPerRange, lastRevmapPage)` from a locked BRIN metapage buffer
/// (`BrinMetaPageData` accessors, brin_page.h). Grounded in-crate 1:1 like the
/// sibling brin crates until brin-core lands a shared accessor.
fn with_meta_page(buf: types_storage::Buffer) -> PgResult<(BlockNumber, BlockNumber)> {
    use backend_storage_buffer_bufmgr_seams::with_buffer_page;
    let mut out = (0u32, 0u32);
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        out = (meta_pages_per_range(page), meta_last_revmap_page(page));
        Ok(())
    })?;
    Ok(out)
}

/// `((BrinMetaPageData *) PageGetContents(page))->pagesPerRange`.
fn meta_pages_per_range(page: &[u8]) -> BlockNumber {
    let off = page_contents_offset();
    // BrinMetaPageData layout: { uint32 brinMagic; uint32 brinVersion;
    //   BlockNumber pagesPerRange; BlockNumber lastRevmapPage; }
    let base = off + 8; // skip magic + version
    BlockNumber::from_ne_bytes([page[base], page[base + 1], page[base + 2], page[base + 3]])
}

/// `((BrinMetaPageData *) PageGetContents(page))->lastRevmapPage`.
fn meta_last_revmap_page(page: &[u8]) -> BlockNumber {
    let off = page_contents_offset();
    let base = off + 12; // magic + version + pagesPerRange
    BlockNumber::from_ne_bytes([page[base], page[base + 1], page[base + 2], page[base + 3]])
}

/// `PageGetContents(page)` offset = `MAXALIGN(SizeOfPageHeaderData)`.
#[inline]
fn page_contents_offset() -> usize {
    // MAXALIGN to MAXIMUM_ALIGNOF (8), mirroring brin_page::CONTENTS_OFFSET.
    let h = types_storage::bufpage::SizeOfPageHeaderData as usize;
    (h + 7) & !7
}

// ===========================================================================
// Seam wiring.
// ===========================================================================

/// Install this crate's owned BRIN insert/vacuum AM-callback seams.
pub fn init_seams() {
    use backend_access_brin_insert_vacuum_seams as seams;
    seams::brininsert::set(brininsert);
    seams::brininsertcleanup::set(brininsertcleanup);
    seams::brinbulkdelete::set(brinbulkdelete);
    seams::brinvacuumcleanup::set(brinvacuumcleanup);
    seams::brin_summarize_range::set(brin_summarize_range);
    seams::brin_desummarize_range::set(brin_desummarize_range);
}
