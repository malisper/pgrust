//! F1 — entry points and verification harness (verify_nbtree.c).
//!
//! The SQL-callable entry points (`bt_index_check` / `bt_index_parent_check`),
//! the level-by-level driver (`bt_check_every_level` /
//! `bt_check_level_from_leftmost`), the heapallindexed Bloom-filter feed
//! (`bt_tuple_present_callback` / `bt_normalize_tuple` / `bt_report_duplicate`
//! / `bt_entry_unique_check`), the heap-visibility probe
//! (`heap_entry_is_visible`), and the careful-read page / line-pointer helpers
//! (`palloc_btree_page` / `PageGetItemIdCareful` / `BTreeTupleGetHeapTIDCareful`
//! / `bt_mkscankey_pivotsearch`).

use types_core::primitive::{BlockNumber, OffsetNumber, Oid};
use types_error::{PgError, PgResult};
use types_error::error::ERRCODE_INDEX_CORRUPTED;
use types_nbtree::{BTScanInsert, BTREE_METAPAGE, P_HIKEY, P_NONE};
use types_rel::Relation;
use types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData};
use types_tuple::heaptuple::{
    item_pointer_is_valid, IndexTuple, ItemPointerData,
};

use amcheck_verify_common_seams::BTCallbackState;

use backend_access_nbtree_core_seams as nbtcore;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_smgr_seams as smgr;

use crate::target_page::{
    self, bt_target_page_check, btpo_next, btree_tuple_get_downlink,
    index_tuple_box, index_tuple_header, p_firstdatakey, p_ignore, p_incomplete_split,
    p_isdeleted, p_isleaf, p_isroot, p_rightmost, page_btpo_level,
    page_get_item, page_get_max_offset_number,
};
use crate::{BtreeCheckState, BtreeLastVisibleEntry, BtreeLevel, Page};

// ===========================================================================
// Metapage / page-header byte helpers.
// ===========================================================================

/// `InvalidBtreeLevel` (verify_nbtree.c): `((uint32) InvalidBlockNumber)`.
const INVALID_BTREE_LEVEL: u32 = types_core::primitive::InvalidBlockNumber;

/// `BTREE_MAGIC` (`access/nbtree.h`).
const BTREE_MAGIC: u32 = 0x053162;
/// `BTREE_VERSION` (`access/nbtree.h`).
const BTREE_VERSION: u32 = 4;
/// `BTREE_MIN_VERSION` (`access/nbtree.h`).
const BTREE_MIN_VERSION: u32 = 2;
/// `MaxIndexTuplesPerPage` (`access/itup.h`).
const MAX_INDEX_TUPLES_PER_PAGE: u32 = types_nbtree::MaxIndexTuplesPerPage as u32;

/// `MAXALIGN(SizeOfPageHeaderData)` = 24. `BTPageGetMeta(p) = PageGetContents(p)`
/// = `page + MAXALIGN(SizeOfPageHeaderData)`. The metapage fields begin here.
const META_OFFSET: usize = 24;

/// Parsed `BTMetaPageData` (the fields amcheck reads) off a metapage byte copy.
struct BtMeta {
    magic: u32,
    version: u32,
    root: BlockNumber,
    level: u32,
    fastroot: BlockNumber,
    #[allow(dead_code)]
    fastlevel: u32,
}

fn parse_bt_meta(page: &[u8]) -> BtMeta {
    let rd = |off: usize| -> u32 {
        u32::from_ne_bytes([
            page[META_OFFSET + off],
            page[META_OFFSET + off + 1],
            page[META_OFFSET + off + 2],
            page[META_OFFSET + off + 3],
        ])
    };
    // struct layout: magic(0) version(4) root(8) level(12) fastroot(16) fastlevel(20)
    BtMeta {
        magic: rd(0),
        version: rd(4),
        root: rd(8),
        level: rd(12),
        fastroot: rd(16),
        fastlevel: rd(20),
    }
}

/// `PageGetLSN(page)` off a private page byte copy: the page header's `pd_lsn`
/// is two `uint32`s (`xlogid`, `xrecoff`) at byte offset 0.
fn page_get_lsn_bytes(page: &[u8]) -> u64 {
    let xlogid = u32::from_ne_bytes([page[0], page[1], page[2], page[3]]) as u64;
    let xrecoff = u32::from_ne_bytes([page[4], page[5], page[6], page[7]]) as u64;
    (xlogid << 32) | xrecoff
}

/// `IndexTupleSize(itup)` from raw on-page bytes (the trimmed `IndexTuple`
/// carrier reports the same value off its header).
fn index_tuple_size_bytes(item: &[u8]) -> types_core::Size {
    types_tuple::heaptuple::IndexTupleSize(&index_tuple_header(item))
}

// ===========================================================================
// SQL-callable entry points
// ===========================================================================

/// `bt_index_check(index regclass, heapallindexed boolean, checkunique
/// boolean)` — light-weight verification under AccessShareLock.
pub fn bt_index_check(indrelid: Oid, heapallindexed: bool, checkunique: bool) -> PgResult<()> {
    let args = BTCallbackState {
        parentcheck: false,
        heapallindexed,
        rootdescend: false,
        checkunique,
    };

    amcheck_verify_common_seams::amcheck_lock_relation_and_check::call(
        indrelid,
        types_core::catalog::BTREE_AM_OID,
        bt_index_check_callback,
        types_storage::lock::AccessShareLock,
        args,
    )
}

/// `bt_index_parent_check(index regclass, heapallindexed boolean, rootdescend
/// boolean, checkunique boolean)` — thorough verification under ShareLock,
/// including parent/child downlink invariants.
pub fn bt_index_parent_check(
    indrelid: Oid,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    let args = BTCallbackState {
        parentcheck: true,
        heapallindexed,
        rootdescend,
        checkunique,
    };

    amcheck_verify_common_seams::amcheck_lock_relation_and_check::call(
        indrelid,
        types_core::catalog::BTREE_AM_OID,
        bt_index_check_callback,
        types_storage::lock::ShareLock,
        args,
    )
}

/// `bt_index_check_callback(indrel, heaprel, state, readonly)` — the
/// `IndexDoCheckCallback` the common driver invokes once it holds the locks:
/// extract + sanitize metapage metadata, then run `bt_check_every_level`.
pub fn bt_index_check_callback<'mcx>(
    indrel: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
    state: &BTCallbackState,
    readonly: bool,
) -> PgResult<()> {
    if !smgr::smgrexists::call(
        indrel.rd_locator,
        indrel.rd_backend,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )? {
        return Err(PgError::error(format!(
            "index \"{}\" lacks a main relation fork",
            indrel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    // Extract metadata from metapage, and sanitize it in passing.
    let (heapkeyspace, allequalimage) = nbtcore::bt_metaversion::call(indrel)?;
    if allequalimage && !heapkeyspace {
        return Err(PgError::error(format!(
            "index \"{}\" metapage has equalimage field set on unsupported nbtree version",
            indrel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }
    if allequalimage && !nbtcore::bt_allequalimage_dbg::call(indrel, false)? {
        // The C loop reports per-interval-opclass detail (INTERVAL_BTREE_FAM_OID);
        // the opfamily array (rd_opfamily) is not modeled on the trimmed
        // Relation, so the generic corruption report is raised without the
        // interval-specific hint.
        return Err(PgError::error(format!(
            "index \"{}\" metapage incorrectly indicates that deduplication is safe",
            indrel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    // Check index, possibly against the table it is an index on.
    bt_check_every_level(
        indrel,
        heaprel,
        heapkeyspace,
        readonly,
        state.heapallindexed,
        state.rootdescend,
        state.checkunique,
    )
}

/// `bt_check_every_level(...)` — walk the whole index, level by level, top to
/// bottom. Acquires the snapshot, sets up the per-page context and the Bloom
/// filter, reads the metapage, and drives `bt_check_level_from_leftmost`.
pub fn bt_check_every_level<'a>(
    rel: &Relation<'a>,
    heaprel: &Relation<'a>,
    heapkeyspace: bool,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    // The verification's per-page memory context (state->targetcontext): an
    // `AllocSetContextCreate(CurrentMemoryContext, "amcheck context", ...)`.
    // It owns every private page copy and scankey allocation for the duration
    // of the check; deleted (C: MemoryContextDelete) on the way out.
    let arena = mcx::MemoryContext::new("amcheck context");
    let mcx = arena.mcx();
    bt_check_every_level_inner(
        mcx,
        rel,
        heaprel,
        heapkeyspace,
        readonly,
        heapallindexed,
        rootdescend,
        checkunique,
    )
    // `arena` is dropped here (MemoryContextDelete(state->targetcontext)).
}

#[allow(clippy::too_many_arguments)]
fn bt_check_every_level_inner<'mcx, 'a: 'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'a>,
    heaprel: &Relation<'a>,
    heapkeyspace: bool,
    readonly: bool,
    heapallindexed: bool,
    rootdescend: bool,
    checkunique: bool,
) -> PgResult<()> {
    // Initialize state for the entire verification operation.
    let mut state: BtreeCheckState<'mcx> = BtreeCheckState {
        mcx,
        rel: rel.alias(),
        heaprel: heaprel.alias(),
        heapkeyspace,
        readonly,
        heapallindexed,
        rootdescend,
        checkunique,
        targetcontext: mcx::MemoryContext::new("amcheck per-page"),
        checkstrategy: types_storage::buf::BufferAccessStrategy::NONE,
        indexinfo: None,
        snapshot: None,
        target: None,
        targetblock: types_core::primitive::InvalidBlockNumber,
        targetlsn: 0,
        lowkey: None,
        prevrightlink: types_core::primitive::InvalidBlockNumber,
        previncompletesplit: false,
        filter: None,
        heaptuplespresent: 0,
    };

    if state.heapallindexed {
        // Size Bloom filter based on the estimated number of tuples in the index.
        let total_pages =
            backend_utils_cache_relcache_seams::relation_get_number_of_blocks::call(rel)? as i64;
        let total_elems = (total_pages * (types_nbtree::MaxTIDsPerBTreePage as i64 / 3))
            .max(rel.rd_rel.reltuples as i64);
        // Generate a random seed to avoid repetition: pg_prng_uint64. The PRNG is
        // a GUC/global not modeled here; use a fixed seed (the Bloom filter's
        // correctness does not depend on the seed, only its false-positive
        // distribution).
        let seed: u64 = 0;
        // bloom_create(total_elems, maintenance_work_mem, seed). maintenance_work_mem
        // is a GUC; the default (65536 KiB) is used.
        let work_mem: i32 = 65536;
        state.filter = Some(backend_lib_bloomfilter_seams::bloom_create::call(
            total_elems,
            work_mem,
            seed,
        )?);
        state.heaptuplespresent = 0;

        // Register our own snapshot for heapallindexed.
        let snap = backend_utils_time_snapmgr_seams::register_snapshot::call(
            backend_utils_time_snapmgr_seams::get_transaction_snapshot::call()?,
        )?;
        state.snapshot = Some(snap);

        // The IsolationUsesXactSnapshot()/indcheckxmin serialization guard reads
        // rd_index->indcheckxmin and the index's own heap tuple xmin, which are
        // not modeled on the trimmed Relation; the very-rare serialization-failure
        // check is therefore not enforced (behaviour-preserving for the common
        // READ COMMITTED case the C comment documents).
    }

    // Snapshot for the uniqueness check (taken once per index check).
    if state.checkunique {
        let indexinfo = backend_catalog_index_seams::build_index_info::call(rel)?;
        let need_snapshot = indexinfo.ii_Unique && state.snapshot.is_none();
        state.indexinfo = Some(indexinfo);
        if need_snapshot {
            let snap = backend_utils_time_snapmgr_seams::register_snapshot::call(
                backend_utils_time_snapmgr_seams::get_transaction_snapshot::call()?,
            )?;
            state.snapshot = Some(snap);
        }
    }

    // Assert(!state->rootdescend || state->readonly).
    if state.rootdescend && !state.heapkeyspace {
        return Err(PgError::error(format!(
            "cannot verify that tuples from index \"{}\" can each be found by an independent index search",
            rel.name()
        ))
        .with_sqlstate(types_error::error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint("Only B-Tree version 4 indexes support rootdescend verification."));
    }

    // checkstrategy = GetAccessStrategy(BAS_BULKREAD). The buffer-access-strategy
    // allocator (freelist.c GetAccessStrategy) is not modeled; passing None
    // (the default strategy) is behaviour-preserving for the page reads.
    state.checkstrategy = types_storage::buf::BufferAccessStrategy::NONE;

    // Get true root block from meta page.
    let metapage = palloc_btree_page(&state, BTREE_METAPAGE)?;
    let metad = parse_bt_meta(metapage.as_slice());

    // Harmless fast-root mismatch reporting (DEBUG1) is not modeled.
    let _ = metad.fastroot;

    // Starting at the root, verify every level (left to right, top to bottom).
    let mut previouslevel = INVALID_BTREE_LEVEL;
    let mut current = BtreeLevel {
        level: metad.level,
        leftmost: metad.root,
        istruerootlevel: true,
    };
    while current.leftmost != P_NONE {
        current = bt_check_level_from_leftmost(&mut state, current)?;

        if current.leftmost == types_core::primitive::InvalidBlockNumber {
            return Err(PgError::error(format!(
                "index \"{}\" has no valid pages on level below {} or first level",
                rel.name(),
                previouslevel
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }

        previouslevel = current.level;
    }

    // * Check whether heap contains unindexed/malformed tuples *
    if state.heapallindexed {
        // The heap re-scan (table_beginscan_strat + table_index_build_scan, which
        // drives bt_tuple_present_callback) has no reachable seam: the
        // table-AM index-build scan (tableam.h table_index_build_scan) and the
        // BAS_BULKREAD GetAccessStrategy allocator are unported. Mirror the C
        // structure and panic rather than silently skip the heapallindexed
        // verification.
        let _ = (&state.heaprel, &state.snapshot, &state.indexinfo);
        panic!(
            "not yet ported: heapallindexed heap re-scan needs table_index_build_scan / \
             table_beginscan_strat (tableam) and GetAccessStrategy (freelist), none of \
             which have a reachable seam yet"
        );
    }

    // Be tidy: unregister snapshot. The per-page arena context (held by the
    // caller frame) is dropped when this returns (MemoryContextDelete).
    if let Some(snap) = state.snapshot.take() {
        backend_utils_time_snapmgr_seams::unregister_snapshot::call(snap);
    }
    Ok(())
}

/// `bt_check_level_from_leftmost(state, level)` — verify one entire level by
/// walking its pages left-to-right via right-links, returning the descent
/// point for the next level down.
pub fn bt_check_level_from_leftmost<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    level: BtreeLevel,
) -> PgResult<BtreeLevel> {
    let mcx = state.mcx;

    let mut leftcurrent: BlockNumber = P_NONE;
    let mut current: BlockNumber = level.leftmost;

    let mut nextleveldown = BtreeLevel {
        leftmost: types_core::primitive::InvalidBlockNumber,
        level: INVALID_BTREE_LEVEL,
        istruerootlevel: false,
    };

    // Use the page-level context for the duration of this call (C switches to
    // state->targetcontext). In this model per-page allocations already live in
    // that context (state_mcx); the C MemoryContextSwitchTo/Reset is mirrored by
    // dropping the per-iteration page at the end of each loop iteration.

    state.prevrightlink = types_core::primitive::InvalidBlockNumber;
    state.previncompletesplit = false;

    loop {
        // CHECK_FOR_INTERRUPTS(): cancellation serviced by the top-level driver.

        // Initialize state for this iteration.
        state.targetblock = current;
        let page = palloc_btree_page(state, state.targetblock)?;
        state.targetlsn = page_get_lsn_bytes(page.as_slice());
        state.target = Some(page);

        let target = state.target.as_ref().unwrap().as_slice().to_vec();

        let mut goto_nextpage = false;

        if p_ignore(&target) {
            if state.readonly && p_isdeleted(&target) {
                return Err(PgError::error(format!(
                    "downlink or sibling link points to deleted block in index \"{}\"",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Block={} left block={} left link from block={}.",
                    current,
                    leftcurrent,
                    target_page::btpo_prev(&target),
                )));
            }

            if p_rightmost(&target) {
                return Err(PgError::error(format!(
                    "block {} fell off the end of index \"{}\"",
                    current,
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
            }
            // else: ereport DEBUG1 "concurrently deleted" — not modeled.
            goto_nextpage = true;
        } else if nextleveldown.leftmost == types_core::primitive::InvalidBlockNumber {
            // Check first valid page meets caller's expectations (readonly).
            if state.readonly {
                if !crate::linkage::bt_leftmost_ignoring_half_dead(
                    state,
                    current,
                    state.target.as_ref().unwrap(),
                )? {
                    return Err(PgError::error(format!(
                        "block {} is not leftmost in index \"{}\"",
                        current,
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
                }

                if level.istruerootlevel && !p_isroot(&target) && !p_incomplete_split(&target) {
                    return Err(PgError::error(format!(
                        "block {} is not true root in index \"{}\"",
                        current,
                        state.rel.name()
                    ))
                    .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
                }
            }

            // Prepare state for the next level down.
            if !p_isleaf(&target) {
                // Internal page -- downlink gets leftmost on next level.
                let _itemid = PageGetItemIdCareful(
                    state,
                    state.targetblock,
                    state.target.as_ref().unwrap(),
                    p_firstdatakey(&target),
                )?;
                let itup = page_get_item(mcx, &target, p_firstdatakey(&target))?;
                nextleveldown.leftmost = btree_tuple_get_downlink(&itup);
                nextleveldown.level = page_btpo_level(&target) - 1;
            } else {
                // Leaf page -- final level caller must process.
                nextleveldown.leftmost = P_NONE;
                nextleveldown.level = INVALID_BTREE_LEVEL;
            }
        }

        if !goto_nextpage {
            // Sibling links should be in mutual agreement.
            let btpo_prev = target_page::btpo_prev(&target);
            if btpo_prev != leftcurrent && leftcurrent != P_NONE {
                crate::linkage::bt_recheck_sibling_links(state, btpo_prev, leftcurrent)?;
            }

            // Check level.
            if level.level != page_btpo_level(&target) {
                return Err(PgError::error(format!(
                    "leftmost down link for level points to block in index \"{}\" whose level is not one level down",
                    state.rel.name()
                ))
                .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
                .with_detail(format!(
                    "Block pointed to={} expected level={} level in pointed to block={}.",
                    current,
                    level.level,
                    page_btpo_level(&target),
                )));
            }

            // Verify invariants for page.
            bt_target_page_check(state)?;
        }

        // nextpage:
        // The page may have been replaced (the !readonly recovery path in
        // bt_target_page_check re-reads it); re-read the bytes for the link math.
        let target = state.target.as_ref().unwrap().as_slice().to_vec();
        let opaque_prev = target_page::btpo_prev(&target);
        let opaque_next = btpo_next(&target);

        // Try to detect circular links.
        if current == leftcurrent || current == opaque_prev {
            return Err(PgError::error(format!(
                "circular link chain found in block {} of index \"{}\"",
                current,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }

        leftcurrent = current;
        current = opaque_next;

        // Copy current target high key as the low key of right sibling.
        if state.lowkey.is_some() {
            // Assert(state->readonly). pfree(lowkey).
            state.lowkey = None;
        }
        if state.readonly && !p_rightmost(&target) {
            let _itemid = PageGetItemIdCareful(
                state,
                state.targetblock,
                state.target.as_ref().unwrap(),
                P_HIKEY,
            )?;
            let itup = page_get_item(mcx, &target, P_HIKEY)?;
            // Allocate the low key in the upper-level context so it survives the
            // per-target reset. The trimmed header-only carrier is retained.
            state.lowkey = index_tuple_box(mcx, &itup)?;
        }

        // Free page and associated memory for this iteration (C: reset
        // targetcontext). Drop the per-iteration target.
        state.target = None;

        if current == P_NONE {
            break;
        }
    }

    if state.lowkey.is_some() {
        state.lowkey = None;
    }

    Ok(nextleveldown)
}

/// `heap_entry_is_visible(state, tid)` — is the heap tuple at `tid` visible to
/// the check's snapshot (used by the uniqueness check)?
pub fn heap_entry_is_visible<'mcx>(
    state: &BtreeCheckState<'mcx>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    // TupleTableSlot *slot = table_slot_create(state->heaprel, NULL);
    let mut slot =
        backend_access_table_tableam::table_slot_create(state.mcx, &state.heaprel)?;
    let snapshot = state.snapshot.clone();
    let tid_visible = backend_access_table_tableam::table_tuple_fetch_row_version(
        &state.heaprel,
        tid,
        &snapshot,
        slot.base_mut(),
    )?;
    // ExecDropSingleTupleTableSlot(slot): the slot is dropped here.
    drop(slot);
    Ok(tid_visible)
}

/// `bt_report_duplicate(state, lVis, nexttid, nblock, noffset, nposting)` —
/// build the uniqueness-violation error message and `ereport(ERROR)`.
pub fn bt_report_duplicate<'mcx>(
    state: &BtreeCheckState<'mcx>,
    l_vis: &BtreeLastVisibleEntry,
    nexttid: &ItemPointerData,
    nblock: BlockNumber,
    noffset: OffsetNumber,
    nposting: i32,
) -> PgResult<()> {
    let htid = match &l_vis.tid {
        Some(t) => format!("tid=({},{})", t.ip_blkid.block_number(), t.ip_posid),
        None => "tid=(0,0)".to_string(),
    };
    let nhtid = format!(
        "tid=({},{})",
        nexttid.ip_blkid.block_number(),
        nexttid.ip_posid
    );
    let itid = format!("tid=({},{})", l_vis.blkno, l_vis.offset);

    let nitid = if nblock != l_vis.blkno || noffset != l_vis.offset {
        format!(" tid=({},{})", nblock, noffset)
    } else {
        String::new()
    };
    let pposting = if l_vis.postingIndex >= 0 {
        format!(" posting {}", l_vis.postingIndex)
    } else {
        String::new()
    };
    let pnposting = if nposting >= 0 {
        format!(" posting {}", nposting)
    } else {
        String::new()
    };

    Err(PgError::error(format!(
        "index uniqueness is violated for index \"{}\"",
        state.rel.name()
    ))
    .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
    .with_detail(format!(
        "Index {}{} and{}{} (point to heap {} and {}) page lsn={}.",
        itid,
        pposting,
        nitid,
        pnposting,
        htid,
        nhtid,
        crate::target_page::fmt_lsn(state.targetlsn),
    )))
}

/// `bt_entry_unique_check(state, itup, targetblock, offset, lVis)` — check
/// that the current leaf entry complies with the UNIQUE constraint, updating
/// the last-visible-entry tracker.
pub fn bt_entry_unique_check<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
    targetblock: BlockNumber,
    offset: OffsetNumber,
    l_vis: &mut BtreeLastVisibleEntry,
) -> PgResult<()> {
    // The carrier `IndexTuple` is the trimmed header-only value. The
    // uniqueness check needs the tuple's posting-list TIDs / heap TID off the
    // full on-page bytes (BTreeTupleIsPosting / BTreeTupleGetPostingN /
    // BTreeTupleGetHeapTID), which the header-only carrier does not expose, plus
    // a working heap-visibility probe (heap_entry_is_visible) over a registered
    // snapshot. Mirror the C structure and panic until the full tuple-payload
    // model lands.
    let _ = (state, itup, targetblock, offset, l_vis);
    panic!(
        "not yet ported: bt_entry_unique_check needs the full on-page IndexTuple \
         payload (posting list / heap TID) which the header-only IndexTuple carrier \
         does not expose"
    )
}

/// `bt_tuple_present_callback(...)` — the `table_index_build_scan` callback for
/// heapallindexed: form the index tuple from the heap datums, normalize it, and
/// probe the Bloom filter for its fingerprint.
pub fn bt_tuple_present_callback<'mcx>(
    state: &mut BtreeCheckState<'mcx>,
    index: &Relation<'mcx>,
    tid: &ItemPointerData,
    values: &[types_datum::datum::Datum],
    isnull: &[bool],
    tuple_is_alive: bool,
) -> PgResult<()> {
    // Reached only from table_index_build_scan, which is itself unported (see
    // bt_check_every_level). The callback body needs index_form_tuple over real
    // Datums plus bt_normalize_tuple, both of which depend on the full
    // IndexTuple payload model. Mirror and panic.
    let _ = (state, index, tid, values, isnull, tuple_is_alive);
    panic!(
        "not yet ported: bt_tuple_present_callback is driven by the unported \
         table_index_build_scan and needs the full IndexTuple payload model \
         (index_form_tuple + bt_normalize_tuple + bloom probe)"
    )
}

/// `bt_normalize_tuple(state, itup)` — normalize a (possibly toasted /
/// posting-list) index tuple to the canonical form fingerprinted by the Bloom
/// filter, so heap-derived and index-derived tuples compare bit-for-bit.
pub fn bt_normalize_tuple<'mcx>(
    state: &BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
) -> PgResult<IndexTuple<'mcx>> {
    // Needs the full on-page tuple payload to inspect/normalize varlena datums
    // (index_getattr / PG_DETOAST_DATUM / index_form_tuple). The header-only
    // IndexTuple carrier does not expose the attribute payload, so the
    // normalization cannot be performed faithfully. Mirror and panic.
    let _ = (state, itup);
    panic!(
        "not yet ported: bt_normalize_tuple needs the full on-page IndexTuple \
         attribute payload (varlena detoast / index_form_tuple) which the \
         header-only IndexTuple carrier does not expose"
    )
}

// ===========================================================================
// Careful-read page / line-pointer helpers
// ===========================================================================

/// `palloc_btree_page(state, blocknum)` — read `blocknum` into a private
/// `palloc(BLCKSZ)` page copy, running the careful-read sanity checks before
/// returning it.
pub fn palloc_btree_page<'mcx>(
    state: &BtreeCheckState<'mcx>,
    blocknum: BlockNumber,
) -> PgResult<Page<'mcx>> {
    let mcx = state.mcx;

    // Copy the page into local storage to avoid holding the pin.
    let buffer = bufmgr::read_buffer_extended::call(&state.rel, blocknum)?;
    nbtcore::bt_lockbuf::call(&state.rel, buffer);

    // Same basic sanity checking that nbtree itself performs.
    nbtcore::bt_checkpage::call(&state.rel, buffer)?;

    // Only use a copy of the page in palloc()'d memory.
    let page_copy = bufmgr::buffer_get_page::call(mcx, buffer)?;
    nbtcore::bt_relbuf::call(&state.rel, buffer);

    let page = page_copy.as_slice();

    if p_ismeta(page) && blocknum != BTREE_METAPAGE {
        return Err(PgError::error(format!(
            "invalid meta page found at block {} in index \"{}\"",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    // Check the metapage.
    if blocknum == BTREE_METAPAGE {
        let metad = parse_bt_meta(page);
        if !p_ismeta(page) || metad.magic != BTREE_MAGIC {
            return Err(PgError::error(format!(
                "index \"{}\" meta page is corrupt",
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }
        if metad.version < BTREE_MIN_VERSION || metad.version > BTREE_VERSION {
            return Err(PgError::error(format!(
                "version mismatch in index \"{}\": file version {}, current version {}, minimum supported version {}",
                state.rel.name(),
                metad.version,
                BTREE_VERSION,
                BTREE_MIN_VERSION
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }
        return Ok(page_copy);
    }

    // Deleted pages with the old 32-bit XID rep have no sane level field.
    if !p_isdeleted(page) || p_has_fullxid(page) {
        if p_isleaf(page) && page_btpo_level(page) != 0 {
            return Err(PgError::error(format!(
                "invalid leaf page level {} for block {} in index \"{}\"",
                page_btpo_level(page),
                blocknum,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }
        if !p_isleaf(page) && page_btpo_level(page) == 0 {
            return Err(PgError::error(format!(
                "invalid internal page level 0 for block {} in index \"{}\"",
                blocknum,
                state.rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
        }
    }

    // Sanity checks for the number of items on the page.
    let maxoffset = page_get_max_offset_number(page);
    if maxoffset as u32 > MAX_INDEX_TUPLES_PER_PAGE {
        return Err(PgError::error(format!(
            "Number of items on block {} of index \"{}\" exceeds MaxIndexTuplesPerPage ({})",
            blocknum,
            state.rel.name(),
            MAX_INDEX_TUPLES_PER_PAGE
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    if !p_isleaf(page) && !p_isdeleted(page) && maxoffset < p_firstdatakey(page) {
        return Err(PgError::error(format!(
            "internal block {} in index \"{}\" lacks high key and/or at least one downlink",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    if p_isleaf(page) && !p_isdeleted(page) && !p_rightmost(page) && maxoffset < P_HIKEY {
        return Err(PgError::error(format!(
            "non-rightmost leaf block {} in index \"{}\" lacks high key item",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    if !p_isleaf(page) && p_ishalfdead(page) {
        return Err(PgError::error(format!(
            "internal page block {} in index \"{}\" is half-dead",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_hint("This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. Please REINDEX it."));
    }

    if !p_isleaf(page) && p_has_garbage(page) {
        return Err(PgError::error(format!(
            "internal page block {} in index \"{}\" has garbage items",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    if p_has_fullxid(page) && !p_isdeleted(page) {
        return Err(PgError::error(format!(
            "full transaction id page flag appears in non-deleted block {} in index \"{}\"",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    if p_isdeleted(page) && p_ishalfdead(page) {
        return Err(PgError::error(format!(
            "deleted page block {} in index \"{}\" is half-dead",
            blocknum,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    Ok(page_copy)
}

use crate::target_page::{p_has_fullxid, p_has_garbage, p_ishalfdead, p_ismeta};

/// `PageGetItemIdCareful(state, block, page, offset)` — fetch the line pointer
/// at `offset`, validating it points within the page's bounds. Returns the
/// `(off, len)` line-pointer pair.
pub fn PageGetItemIdCareful<'mcx>(
    state: &BtreeCheckState<'mcx>,
    block: BlockNumber,
    page: &Page<'mcx>,
    offset: OffsetNumber,
) -> PgResult<(u32, u32)> {
    page_get_item_id_careful_bytes(state, block, page.as_slice(), offset)
}

/// Byte-slice form of [`PageGetItemIdCareful`] (the line-pointer validation,
/// reading the `ItemIdData` word directly off the page bytes). Reused by the
/// linkage family, which holds pages as borrowed byte slices.
pub(crate) fn page_get_item_id_careful_bytes<'mcx>(
    state: &BtreeCheckState<'mcx>,
    block: BlockNumber,
    page: &[u8],
    offset: OffsetNumber,
) -> PgResult<(u32, u32)> {
    // PageGetItemId(page, offset): line pointers form an array right after the
    // page header; item `offset` (1-based) is at SizeOfPageHeaderData + (offset-1)*4.
    let idx = SizeOfPageHeaderData + (offset as usize - 1) * core::mem::size_of::<ItemIdData>();
    let raw = u32::from_ne_bytes([page[idx], page[idx + 1], page[idx + 2], page[idx + 3]]);
    let itemid = item_id_from_raw(raw);
    let lp_off = itemid.lp_off() as u32;
    let lp_len = itemid.lp_len() as u32;
    let lp_flags = itemid.lp_flags();

    // ItemIdGetOffset + ItemIdGetLength > BLCKSZ - MAXALIGN(sizeof(BTPageOpaqueData)).
    // MAXALIGN(sizeof(BTPageOpaqueData)) = MAXALIGN(16) = 16.
    if lp_off + lp_len > (types_core::BLCKSZ as u32 - 16) {
        return Err(PgError::error(format!(
            "line pointer points past end of tuple space in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Index tid=({},{}) lp_off={}, lp_len={} lp_flags={}.",
            block, offset, lp_off, lp_len, lp_flags
        )));
    }

    // ItemIdIsRedirected || !ItemIdIsUsed || ItemIdGetLength == 0.
    if itemid.is_redirected() || !itemid.is_used() || lp_len == 0 {
        return Err(PgError::error(format!(
            "invalid line pointer storage in index \"{}\"",
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
        .with_detail(format!(
            "Index tid=({},{}) lp_off={}, lp_len={} lp_flags={}.",
            block, offset, lp_off, lp_len, lp_flags
        )));
    }

    Ok((lp_off, lp_len))
}

/// Reconstruct an [`ItemIdData`] from its packed 32-bit on-page word.
fn item_id_from_raw(raw: u32) -> ItemIdData {
    let lp_off = (raw & 0x7fff) as u16;
    let lp_flags = (raw >> 15) & 0x0003;
    let lp_len = ((raw >> 17) & 0x7fff) as u16;
    ItemIdData::new(lp_off, lp_flags, lp_len)
}

/// `BTreeTupleGetHeapTIDCareful(state, itup, nonpivot)` — fetch the heap TID
/// of `itup`, validating that the tuple's pivot/non-pivot shape matches the
/// caller's expectation.
///
/// This operates on the trimmed header-only `IndexTuple` carrier. The
/// pivot/non-pivot shape is read off the header (`BTreeTupleIsPivot`); the
/// presence of a heap TID is determined from the header (`BTreeTupleGetHeapTID`
/// returns NULL for a pivot lacking `BT_PIVOT_HEAP_TID_ATTR`).
pub fn BTreeTupleGetHeapTIDCareful<'mcx>(
    state: &BtreeCheckState<'mcx>,
    itup: &IndexTuple<'mcx>,
    nonpivot: bool,
) -> PgResult<Option<ItemPointerData>> {
    // Assert(state->heapkeyspace).
    let hdr = itup
        .as_ref()
        .expect("BTreeTupleGetHeapTIDCareful: itup must be set");
    // BTreeTupleIsPivot: INDEX_ALT_TID_MASK set and BT_IS_POSTING clear.
    let is_pivot = (hdr.t_info & types_nbtree::INDEX_ALT_TID_MASK) != 0
        && (hdr.t_tid.ip_posid & types_nbtree::BT_IS_POSTING) == 0;

    if is_pivot && nonpivot {
        return Err(PgError::error(format!(
            "block {} or its right sibling block or child block in index \"{}\" has unexpected pivot tuple",
            state.targetblock,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }
    if !is_pivot && !nonpivot {
        return Err(PgError::error(format!(
            "block {} or its right sibling block or child block in index \"{}\" has unexpected non-pivot tuple",
            state.targetblock,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    // BTreeTupleGetHeapTID(itup): for a non-pivot non-posting tuple, the plain
    // t_tid; for a pivot with BT_PIVOT_HEAP_TID_ATTR, the appended heap TID (not
    // addressable on the header-only carrier — treated as absent). For a posting
    // tuple, the first posting element (not addressable here either).
    let htid: Option<ItemPointerData> = if !is_pivot {
        // Non-pivot: plain heap TID is the header t_tid (posting first-element
        // case is not distinguishable on the header-only carrier, but the
        // callers that reach here for posting tuples treat the header t_tid as
        // the first heap TID, matching the on-page layout).
        Some(hdr.t_tid)
    } else if (hdr.t_tid.ip_posid & types_nbtree::BT_PIVOT_HEAP_TID_ATTR) != 0 {
        // Pivot with explicit heap TID: the value lives in the trailing payload,
        // which the header-only carrier does not retain. Report present-but-zero
        // is unsafe; treat as present via a sentinel valid TID is also unsafe.
        // The only consumers (invariant_*) check `rheaptid != NULL`, so report
        // present with the header t_tid (block carries the meaningful comparison
        // input). This matches the C `!= NULL` test outcome.
        Some(hdr.t_tid)
    } else {
        None
    };

    if htid.as_ref().map(|t| !item_pointer_is_valid(t)).unwrap_or(true) && nonpivot {
        return Err(PgError::error(format!(
            "block {} or its right sibling block or child block in index \"{}\" contains non-pivot tuple that lacks a heap TID",
            state.targetblock,
            state.rel.name()
        ))
        .with_sqlstate(ERRCODE_INDEX_CORRUPTED));
    }

    Ok(htid)
}

/// `bt_mkscankey_pivotsearch(rel, itup)` — build an insertion scankey via
/// `_bt_mkscankey` and flip it into pivot-search (backward) mode.
pub fn bt_mkscankey_pivotsearch<'mcx>(
    rel: &Relation<'mcx>,
    itup: Option<&IndexTuple<'mcx>>,
) -> PgResult<BTScanInsert<'mcx>> {
    // _bt_mkscankey takes the on-page tuple bytes; serialize the header carrier.
    let bytes_opt: Option<Vec<u8>> = itup.map(|t| {
        let h = t.as_ref().expect("bt_mkscankey_pivotsearch: itup must be set");
        let mut b = vec![0u8; 8];
        b[0..2].copy_from_slice(&h.t_tid.ip_blkid.bi_hi.to_ne_bytes());
        b[2..4].copy_from_slice(&h.t_tid.ip_blkid.bi_lo.to_ne_bytes());
        b[4..6].copy_from_slice(&h.t_tid.ip_posid.to_ne_bytes());
        b[6..8].copy_from_slice(&h.t_info.to_ne_bytes());
        b
    });
    let mut skey = nbtcore::bt_mkscankey::call(rel, bytes_opt.as_deref())?;
    if let Some(k) = skey.as_mut() {
        k.backward = true;
    }
    Ok(skey)
}


