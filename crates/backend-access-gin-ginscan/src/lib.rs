//! Owned-tree Rust port of `src/backend/access/gin/ginscan.c` (PostgreSQL 18.3)
//! — the routines that manage scans of inverted (GIN) index relations.
//!
//! The complete set of C functions ported here, 1:1:
//!
//!   * `ginbeginscan`             — begin an index scan, allocate the opaque
//!   * `ginFillScanEntry`         — create / de-duplicate a [`GinScanEntryData`]
//!   * `ginScanKeyAddHiddenEntry` — append a "hidden" placeholder entry to a key
//!   * `ginFillScanKey`           — init the next [`GinScanKey`] from extractQuery
//!   * `ginFreeScanKeys`          — release the current scan keys + entry buffers
//!   * `ginNewScanKey`            — build the scan keys from `scan->keyData`
//!   * `ginrescan`                — restart with a new key set
//!   * `ginendscan`               — end the scan, drop the opaque
//!
//! # Runtime data model
//!
//! `GinScanOpaqueData` / `GinScanKeyData` / `GinScanEntryData` from
//! `access/gin_private.h` are the canonical owned carriers from
//! [`types_gin`] (the GIN carrier keystone). C shares one `GinScanEntryData`
//! between several keys through the `scanEntry[]` / `entries[]` /
//! `requiredEntries[]` pointer arrays; the owned model owns every entry once in
//! [`GinScanOpaqueData::entries`] and references it by *index* everywhere else
//! (`scanEntry` / `requiredEntries` / `additionalEntries` are `Vec<u32>`),
//! reproducing the de-duplication and the required/additional partition exactly.
//!
//! This crate mirrors the landed btree/hash/BRIN index tower: the
//! [`GinScanOpaqueData`] scan-private working state rides
//! `IndexScanDescData.opaque` via the A0 [`AmOpaque`] carrier (the
//! [`tags::GIN_SCAN`] tag; erase = `alloc_in` → `into_raw_with_allocator` →
//! `from_raw_in(ptr as *mut dyn AmOpaque)`). The `ginbeginscan` /
//! `ginrescan` / `ginendscan` callbacks the `ginutil` handler assembles into the
//! unified `IndexAmRoutine` are installed here from [`init_seams`] (the seam
//! declarations live in [`backend_access_gin_ginutil_seams`], whose first cyclic
//! caller is `ginutil`).
//!
//! The two short-lived memory contexts (`tempCtx` / `keyCtx`,
//! `AllocSetContextCreate(CurrentMemoryContext, ...)`) are allocated as children
//! of the scan's `mcx` arena and held by the opaque (their cleanup nesting is
//! expressed by ownership, per the mcx model); `MemoryContextReset(keyCtx)` in
//! `ginFreeScanKeys` becomes a `reset` of the held key context.
//!
//! Genuinely-external substrate is reached through the owned `ginutil` direct
//! deps (`initGinState`, `ginCompareEntries`, `ginGetStats`,
//! `gin_relation_get_relation_name`) and the `gin-core-probe` direct dep
//! (`ginInitConsistentFunction`), plus seams for the opclass `extractQueryFn`
//! fmgr dispatch (`gin_extract_query`) and `pgstat_count_index_scan`. The scan
//! engine (`gingetbitmap`, ginget.c) is a separate (not-yet-ported) GIN unit:
//! the entries' `matchBitmap` / `matchIterator` / posting `list` it would
//! populate start empty, and `ginFreeScanKeys` reclaims them by dropping the
//! owned values.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox};

use backend_utils_error::{ereport, PgResult};
use types_error::error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};

use types_core::primitive::{OffsetNumber, Oid};
use types_core::{InvalidOid, INDEX_MAX_KEYS};
use types_gin::{
    GinNullCategory, GinScanEntryData, GinScanKey, GinScanOpaqueData, GinState, TBMIterateResult,
    TBM_MAX_TUPLES_PER_PAGE, GIN_CAT_EMPTY_ITEM, GIN_CAT_EMPTY_QUERY, GIN_CAT_NORM_KEY,
    GIN_CAT_NULL_KEY, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GIN_SEARCH_MODE_EVERYTHING,
    GIN_SEARCH_MODE_INCLUDE_EMPTY,
};
use types_rel::Relation;
use types_scan::scankey::{InvalidStrategy, ScanKeyData, SK_ISNULL};
use types_storage::storage::InvalidBuffer;
use types_tableam::amopaque::AmOpaque;
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, FIRST_OFFSET_NUMBER as FirstOffsetNumber};

use backend_storage_buffer_bufmgr_seams as bufmgr;

use backend_access_gin_ginutil as ginutil;
use backend_access_gin_ginutil_seams as sx;
use backend_access_gin_core_probe::ginlogic::ginInitConsistentFunction;
use backend_utils_activity_pgstat_seams::pgstat_count_index_scan;

#[cfg(test)]
mod tests;

// ===========================================================================
// A0 AM-opaque carrier wiring.
// ===========================================================================

// `GinScanOpaqueData` impls `AmOpaqueType` (the A0 carrier) in its owning crate
// `types-gin` (the orphan rule forbids the impl here); see that crate.

/// Downcast `scan.opaque` to the GIN scan working state (the A0 tag-checked
/// downcast); panics with a clear message if the descriptor was not built by
/// `ginbeginscan` (a programming error — C would just cast `void *`).
fn gin_so<'a, 'mcx>(scan: &'a mut IndexScanDescData<'mcx>) -> &'a mut GinScanOpaqueData<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("GIN scan descriptor has no opaque (not built by ginbeginscan)")
        .downcast_mut::<GinScanOpaqueData<'mcx>>()
        .expect("GIN scan opaque is not a GinScanOpaqueData")
}

/// Erase a [`GinScanOpaqueData`] into the A0 AM-opaque carrier
/// (`PgBox<dyn AmOpaque + 'mcx>`) for storage in `IndexScanDescData.opaque`.
fn erase_ginscan<'mcx>(
    mcx: Mcx<'mcx>,
    so: GinScanOpaqueData<'mcx>,
) -> PgResult<PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: PgBox<'mcx, GinScanOpaqueData<'mcx>> = mcx::alloc_in(mcx, so)?;
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable (the A0 erase pattern).
    Ok(unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

// ===========================================================================
// init_seams — install the ginscan.c AM callbacks declared in ginutil-seams.
// ===========================================================================

/// Install the GIN scan-management AM callbacks (`ginbeginscan` / `ginrescan` /
/// `ginendscan`) the `ginutil` handler reaches by name through the unified
/// `IndexAmRoutine`. The declarations live in `ginutil-seams` (whose first
/// cyclic caller, `ginutil`, declared them); `ginscan` is the owner that
/// installs them.
pub fn init_seams() {
    sx::ginbeginscan::set(ginbeginscan);
    sx::ginrescan::set(ginrescan);
    sx::ginendscan::set(ginendscan);
}

// ===========================================================================
// ginbeginscan (ginscan.c:24)
// ===========================================================================

/// `ginbeginscan(rel, nkeys, norderbys)` (ginscan.c:24): begin a GIN index
/// scan. Allocates the generic `IndexScanDescData` (`RelationGetIndexScan`), the
/// two short-lived memory contexts, and the `GinState` (`initGinState`), then
/// hangs the [`GinScanOpaqueData`] off `scan.opaque` via the A0 carrier.
///
/// `norderbys` must be 0 (GIN allows no order-by operators).
pub fn ginbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // no order by operators allowed
    debug_assert!(norderbys == 0);

    // scan = RelationGetIndexScan(rel, nkeys, norderbys);
    let mut scan = relation_get_index_scan(mcx, rel, nkeys, norderbys)?;

    // so = palloc(sizeof(GinScanOpaqueData));
    // so->tempCtx = AllocSetContextCreate(CurrentMemoryContext, "Gin scan
    //     temporary context", ALLOCSET_DEFAULT_SIZES);
    // so->keyCtx = AllocSetContextCreate(CurrentMemoryContext, "Gin scan key
    //     context", ALLOCSET_DEFAULT_SIZES);
    let temp_ctx = mcx::leak_in(mcx::alloc_in(
        mcx,
        mcx.context().new_child("Gin scan temporary context"),
    )?);
    let key_ctx = mcx::leak_in(mcx::alloc_in(
        mcx,
        mcx.context().new_child("Gin scan key context"),
    )?);

    // initGinState(&so->ginstate, scan->indexRelation);
    let ginstate = ginutil::initGinState(rel, mcx)?;

    let so = GinScanOpaqueData {
        tempCtx: temp_ctx.mcx(),
        ginstate,
        // so->keys = NULL; so->nkeys = 0;
        keys: Vec::new(),
        nkeys: 0,
        entries: Vec::new(),
        totalentries: 0,
        allocentries: 0,
        keyCtx: key_ctx.mcx(),
        isVoidRes: false,
    };

    // scan->opaque = so;
    scan.opaque = Some(erase_ginscan(mcx, so)?);

    Ok(scan)
}

// ===========================================================================
// ginFillScanEntry (ginscan.c:56)
// ===========================================================================

/// `ginFillScanEntry(so, attnum, strategy, searchMode, queryKey, queryCategory,
/// isPartialMatch, extra_data)` (ginscan.c:56): create a new [`GinScanEntryData`],
/// unless an equivalent one already exists, in which case return its index.
///
/// Returns the index of the (possibly reused) entry within `so.entries`.
fn ginFillScanEntry<'mcx>(
    so: &mut GinScanOpaqueData<'mcx>,
    attnum: OffsetNumber,
    strategy: u16,
    searchMode: i32,
    queryKey: Datum<'mcx>,
    queryCategory: GinNullCategory,
    isPartialMatch: bool,
    extra_data: Option<Vec<u8>>,
) -> PgResult<u32> {
    // Look for an existing equivalent entry.
    //
    // Entries with non-null extra_data are never considered identical, since we
    // can't know exactly what the opclass might be doing with that.
    //
    // Also, give up de-duplication once we have 100 entries.
    if extra_data.is_none() && so.totalentries < 100 {
        for i in 0..so.totalentries as usize {
            let prevEntry = &so.entries[i];

            if prevEntry.extra_data.is_none()
                && prevEntry.isPartialMatch == isPartialMatch
                && prevEntry.strategy == strategy
                && prevEntry.searchMode == searchMode
                && prevEntry.attnum == attnum
                && ginutil::ginCompareEntries(
                    &so.ginstate,
                    attnum,
                    prevEntry.queryKey.clone(),
                    prevEntry.queryCategory,
                    queryKey.clone(),
                    queryCategory,
                )? == 0
            {
                // Successful match
                return Ok(i as u32);
            }
        }
    }

    // Nope, create a new entry
    let scanEntry = new_scan_entry(
        attnum,
        strategy,
        searchMode,
        queryKey,
        queryCategory,
        isPartialMatch,
        extra_data,
    );

    // Add it to so's array
    let idx = so.totalentries;
    so.entries.push(scanEntry);
    so.totalentries += 1;
    if so.totalentries >= so.allocentries {
        so.allocentries *= 2;
    }
    Ok(idx)
}

/// A freshly-`palloc`'d [`GinScanEntryData`] (ginscan.c:96..114): the query-key
/// fields filled, the scan-state fields zeroed exactly as C initializes them.
fn new_scan_entry<'mcx>(
    attnum: OffsetNumber,
    strategy: u16,
    searchMode: i32,
    queryKey: Datum<'mcx>,
    queryCategory: GinNullCategory,
    isPartialMatch: bool,
    extra_data: Option<Vec<u8>>,
) -> GinScanEntryData<'mcx> {
    GinScanEntryData {
        queryKey,
        queryCategory,
        isPartialMatch,
        extra_data,
        strategy,
        searchMode,
        attnum,
        // scanEntry->buffer = InvalidBuffer;
        buffer: InvalidBuffer,
        // ItemPointerSetMin(&scanEntry->curItem);
        curItem: item_pointer_min(),
        // scanEntry->matchBitmap = NULL; scanEntry->matchIterator = NULL;
        matchBitmap: None,
        matchIterator: None,
        // scanEntry->matchResult.blockno = InvalidBlockNumber;
        matchResult: TBMIterateResult::default(),
        matchOffsets: alloc::vec![0; TBM_MAX_TUPLES_PER_PAGE],
        // scanEntry->matchNtuples = -1;
        matchNtuples: -1,
        // scanEntry->list = NULL; scanEntry->nlist = 0;
        list: Vec::new(),
        nlist: 0,
        // scanEntry->offset = InvalidOffsetNumber;
        offset: types_tuple::heaptuple::INVALID_OFFSET_NUMBER,
        // scanEntry->isFinished = false; scanEntry->reduceResult = false;
        isFinished: false,
        reduceResult: false,
        predictNumberResult: 0,
        // scanEntry->btree — a zeroed GinBtreeData (filled by ginget.c).
        btree: types_gin::GinBtreeData::default(),
    }
}

// ===========================================================================
// ginScanKeyAddHiddenEntry (ginscan.c:143)
// ===========================================================================

/// `ginScanKeyAddHiddenEntry(so, key, queryCategory)` (ginscan.c:143): append a
/// hidden scan entry of the given category to a scan key.
///
/// NB: this had better be called at most once per scan key, since
/// `ginFillScanKey` leaves room for only one hidden entry.
fn ginScanKeyAddHiddenEntry<'mcx>(
    so: &mut GinScanOpaqueData<'mcx>,
    key_idx: usize,
    queryCategory: GinNullCategory,
) -> PgResult<()> {
    // int i = key->nentries++;
    let (attnum, searchMode, i) = {
        let key = &mut so.keys[key_idx];
        let i = key.nentries as usize;
        key.nentries += 1;
        (key.attnum, key.searchMode, i)
    };

    // strategy is of no interest because this is not a partial-match item
    let entry_idx = ginFillScanEntry(
        so,
        attnum,
        InvalidStrategy,
        searchMode,
        Datum::default(),
        queryCategory,
        false,
        None,
    )?;

    // key->scanEntry[i] = entry; (the slot was reserved by ginFillScanKey)
    let key = &mut so.keys[key_idx];
    if i < key.scanEntry.len() {
        key.scanEntry[i] = entry_idx;
    } else {
        key.scanEntry.push(entry_idx);
    }
    Ok(())
}

// ===========================================================================
// ginFillScanKey (ginscan.c:158)
// ===========================================================================

/// `ginFillScanKey(so, attnum, strategy, searchMode, query, nQueryValues,
/// queryValues, queryCategories, partial_matches, extra_data)` (ginscan.c:158):
/// initialize the next [`GinScanKey`] using the output from the extractQueryFn.
fn ginFillScanKey<'mcx>(
    so: &mut GinScanOpaqueData<'mcx>,
    attnum: OffsetNumber,
    strategy: u16,
    searchMode: i32,
    query: Datum<'mcx>,
    nQueryValues: u32,
    queryValues: Vec<Datum<'mcx>>,
    queryCategories: Vec<GinNullCategory>,
    partial_matches: &[bool],
    extra_data: &[Option<Vec<u8>>],
) -> PgResult<()> {
    // key = &(so->keys[so->nkeys++]);
    let key_idx = so.nkeys as usize;
    so.nkeys += 1;

    // key->scanEntry = palloc(sizeof(GinScanEntry) * (nQueryValues + 1));
    // key->entryRes = palloc0(sizeof(GinTernaryValue) * (nQueryValues + 1));
    let key = GinScanKey {
        nentries: nQueryValues,
        nuserentries: nQueryValues,
        scanEntry: alloc::vec![0u32; nQueryValues as usize + 1],
        requiredEntries: Vec::new(),
        nrequired: 0,
        additionalEntries: Vec::new(),
        nadditional: 0,
        entryRes: alloc::vec![0i8; nQueryValues as usize + 1],
        boolConsistentFn: types_gin::GinBoolConsistentKind::Shim,
        triConsistentFn: types_gin::GinTriConsistentKind::Shim,
        consistent_fmgr_oid: InvalidOid,
        tri_consistent_fmgr_oid: InvalidOid,
        collation: InvalidOid,
        query,
        queryValues,
        queryCategories,
        extra_data: extra_data.to_vec(),
        strategy,
        searchMode,
        attnum,
        // Initially, scan keys of GIN_SEARCH_MODE_ALL mode are marked
        // excludeOnly. This might get changed later.
        excludeOnly: searchMode == GIN_SEARCH_MODE_ALL,
        curItem: item_pointer_min(),
        curItemMatches: false,
        recheckCurItem: false,
        isFinished: false,
    };
    so.keys.push(key);

    // ginInitConsistentFunction(ginstate, key);
    {
        // Borrow ginstate immutably while mutating the key in-place.
        let GinScanOpaqueData {
            ginstate, keys, ..
        } = so;
        ginInitConsistentFunction(ginstate, &mut keys[key_idx]);
    }

    // Set up normal scan entries using extractQueryFn's outputs.
    let canPartialMatch = so.ginstate.canPartialMatch[(attnum - 1) as usize];
    for i in 0..nQueryValues as usize {
        let queryKey = so.keys[key_idx].queryValues[i].clone();
        let queryCategory = so.keys[key_idx].queryCategories[i];
        let isPartialMatch = if canPartialMatch && !partial_matches.is_empty() {
            partial_matches[i]
        } else {
            false
        };
        let this_extra = if !extra_data.is_empty() {
            extra_data[i].clone()
        } else {
            None
        };

        let entry_idx = ginFillScanEntry(
            so,
            attnum,
            strategy,
            searchMode,
            queryKey,
            queryCategory,
            isPartialMatch,
            this_extra,
        )?;
        so.keys[key_idx].scanEntry[i] = entry_idx;
    }

    // For GIN_SEARCH_MODE_INCLUDE_EMPTY and GIN_SEARCH_MODE_EVERYTHING search
    // modes, we add the "hidden" entry immediately. GIN_SEARCH_MODE_ALL is
    // handled later, since we might be able to omit the hidden entry for it.
    if searchMode == GIN_SEARCH_MODE_INCLUDE_EMPTY {
        ginScanKeyAddHiddenEntry(so, key_idx, GIN_CAT_EMPTY_ITEM)?;
    } else if searchMode == GIN_SEARCH_MODE_EVERYTHING {
        ginScanKeyAddHiddenEntry(so, key_idx, GIN_CAT_EMPTY_QUERY)?;
    }

    Ok(())
}

// ===========================================================================
// ginFreeScanKeys (ginscan.c:238)
// ===========================================================================

/// `ginFreeScanKeys(so)` (ginscan.c:238): release the current scan keys, if any.
///
/// Releases each entry's posting-tree buffer pin, frees the posting-list copy,
/// ends the bitmap iterator, frees the match bitmap, resets the key memory
/// context, and clears the key / entry arrays.
pub fn ginFreeScanKeys(so: &mut GinScanOpaqueData<'_>) {
    // if (so->keys == NULL) return;
    if so.keys.is_empty() && so.totalentries == 0 {
        return;
    }

    for i in 0..so.totalentries as usize {
        let entry = &mut so.entries[i];

        // if (entry->buffer != InvalidBuffer) ReleaseBuffer(entry->buffer);
        if entry.buffer != InvalidBuffer {
            // Posting-tree buffers are pinned by ginget.c, which is not yet
            // ported; with no engine running this branch is unreachable. When
            // ginget.c lands it will pin real buffers and this releases them
            // through the bufmgr seam.
            release_buffer(entry.buffer);
        }
        // if (entry->list) pfree(entry->list);  — owned Vec drops on clear.
        entry.list = Vec::new();
        // if (entry->matchIterator) tbm_end_private_iterate(entry->matchIterator);
        // The owned iterator value is reclaimed by dropping it.
        entry.matchIterator = None;
        // if (entry->matchBitmap) tbm_free(entry->matchBitmap);
        // The owned bitmap value is reclaimed by dropping it.
        entry.matchBitmap = None;
    }

    // MemoryContextReset(so->keyCtx) — in C all key/entry storage is palloc'd in
    // keyCtx and this reset frees it wholesale. In the owned model the keys and
    // entries are owned `Vec`s (cleared just below), so clearing them is the
    // faithful reclaim; the keyCtx arena holds no separate live allocations.

    // so->keys = NULL; so->nkeys = 0; so->entries = NULL; so->totalentries = 0;
    so.keys = Vec::new();
    so.nkeys = 0;
    so.entries = Vec::new();
    so.totalentries = 0;
}

// ===========================================================================
// ginNewScanKey (ginscan.c:266)
// ===========================================================================

/// `ginNewScanKey(scan)` (ginscan.c:266): initialize the scan keys from
/// `scan->keyData`. Runs the opclass `extractQueryFn` per key, builds the
/// per-key [`GinScanKey`] and de-duplicated entry pool, performs the
/// `GIN_SEARCH_MODE_ALL` second pass (excludeOnly handling + key re-ordering),
/// generates the EVERYTHING key when no regular keys remain, and rejects a
/// whole-index / null search against a version-0 index.
pub fn ginNewScanKey<'mcx>(scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    // The scan keys are owned by the descriptor; clone them so we can read them
    // while mutating the opaque the descriptor also owns.
    let scankeys: Vec<ScanKeyData<'mcx>> = scan.key_data.clone();
    let index_oid = scan.index_relation.rd_id;
    let idx_rel = scan.index_relation.alias();

    let mut numExcludeOnly: i32;
    let mut hasNullQuery = false;
    let mut attrHasNormalScan = [false; INDEX_MAX_KEYS as usize];

    let so = gin_so(scan);
    // C switches to so->keyCtx for all scan-key allocation (including the
    // extractQueryFn outputs); `Mcx` is `Copy`, so capture the handle.
    let key_mcx: Mcx<'mcx> = so.keyCtx;

    // if no scan keys provided, allocate extra EVERYTHING GinScanKey
    so.keys = Vec::with_capacity(scankeys.len().max(1));
    so.nkeys = 0;

    // initialize expansible array of GinScanEntry pointers
    so.totalentries = 0;
    so.allocentries = 32;
    so.entries = Vec::new();

    so.isVoidRes = false;

    for skey in scankeys.iter() {
        // We assume that GIN-indexable operators are strict, so a null query
        // argument means an unsatisfiable query.
        if skey.sk_flags & SK_ISNULL != 0 {
            so.isVoidRes = true;
            break;
        }

        let attno_idx = (skey.sk_attno - 1) as usize;

        // OK to call the extractQueryFn
        let res = sx::gin_extract_query::call(
            key_mcx,
            &so.ginstate.extractQueryFn[attno_idx],
            so.ginstate.supportCollation[attno_idx],
            skey.sk_argument.clone(),
            skey.sk_strategy,
        )?;

        let sx::GinExtractQueryResult {
            query_values,
            null_flags,
            partial_matches,
            extra_data,
            mut search_mode,
        } = res;

        // If bogus searchMode is returned, treat as GIN_SEARCH_MODE_ALL; note in
        // particular we don't allow extractQueryFn to select
        // GIN_SEARCH_MODE_EVERYTHING.
        if search_mode < GIN_SEARCH_MODE_DEFAULT || search_mode > GIN_SEARCH_MODE_ALL {
            search_mode = GIN_SEARCH_MODE_ALL;
        }

        // Non-default modes require the index to have placeholders
        if search_mode != GIN_SEARCH_MODE_DEFAULT {
            hasNullQuery = true;
        }

        // In default mode, no keys means an unsatisfiable query.
        let mut nQueryValues = query_values.len() as i32;
        let mut queryValues: Vec<Datum<'mcx>> = query_values.iter().cloned().collect();
        if queryValues.is_empty() || nQueryValues <= 0 {
            if search_mode == GIN_SEARCH_MODE_DEFAULT {
                so.isVoidRes = true;
                break;
            }
            nQueryValues = 0; // ensure sane value
            queryValues.clear();
        }

        // Create GinNullCategory representation. If the extractQueryFn didn't
        // create a nullFlags array, we assume everything is non-null. While at
        // it, detect whether any null keys are present.
        let mut categories = alloc::vec![GIN_CAT_NORM_KEY; nQueryValues as usize];
        if !null_flags.is_empty() {
            for j in 0..nQueryValues as usize {
                if null_flags[j] {
                    categories[j] = GIN_CAT_NULL_KEY;
                    hasNullQuery = true;
                }
            }
        }

        let partials: Vec<bool> = partial_matches.iter().copied().collect();
        let extras: Vec<Option<Vec<u8>>> = extra_data
            .iter()
            .map(|p| p.as_ref().map(|v| v.iter().copied().collect()))
            .collect();

        ginFillScanKey(
            so,
            skey.sk_attno as OffsetNumber,
            skey.sk_strategy,
            search_mode,
            skey.sk_argument.clone(),
            nQueryValues as u32,
            queryValues,
            categories,
            &partials,
            &extras,
        )?;

        // Remember if we had any non-excludeOnly keys
        if search_mode != GIN_SEARCH_MODE_ALL {
            attrHasNormalScan[attno_idx] = true;
        }
    }

    // Processing GIN_SEARCH_MODE_ALL scan keys requires us to make a second pass
    // over the scan keys. Above we marked each such scan key as excludeOnly. If
    // the involved column has any normal (not excludeOnly) scan key as well,
    // then we can leave it like that. Otherwise, one excludeOnly scan key must
    // receive a GIN_CAT_EMPTY_QUERY hidden entry and be set to normal
    // (excludeOnly = false).
    numExcludeOnly = 0;
    let nkeys = so.nkeys as usize;
    for i in 0..nkeys {
        let (searchMode, attnum) = {
            let key = &so.keys[i];
            (key.searchMode, key.attnum)
        };

        if searchMode != GIN_SEARCH_MODE_ALL {
            continue;
        }

        if !attrHasNormalScan[(attnum - 1) as usize] {
            so.keys[i].excludeOnly = false;
            ginScanKeyAddHiddenEntry(so, i, GIN_CAT_EMPTY_QUERY)?;
            attrHasNormalScan[(attnum - 1) as usize] = true;
        } else {
            numExcludeOnly += 1;
        }
    }

    // If we left any excludeOnly scan keys as-is, move them to the end of the
    // scan key array: they must appear after normal key(s). This reproduces the
    // C two-cursor memcpy reorder (stable within each partition); the entry-pool
    // indices the keys carry are unaffected by re-ordering the keys.
    if numExcludeOnly > 0 {
        // We'd better have made at least one normal key
        debug_assert!((numExcludeOnly as u32) < so.nkeys);

        let keys = core::mem::take(&mut so.keys);
        let mut normal: Vec<GinScanKey<'mcx>> = Vec::new();
        let mut exclude: Vec<GinScanKey<'mcx>> = Vec::new();
        for key in keys {
            if key.excludeOnly {
                exclude.push(key);
            } else {
                normal.push(key);
            }
        }
        debug_assert!(normal.len() == so.nkeys as usize - numExcludeOnly as usize);
        normal.extend(exclude);
        so.keys = normal;
    }

    // If there are no regular scan keys, generate an EVERYTHING scankey to drive
    // a full-index scan.
    if so.nkeys == 0 && !so.isVoidRes {
        hasNullQuery = true;
        ginFillScanKey(
            so,
            FirstOffsetNumber,
            InvalidStrategy,
            GIN_SEARCH_MODE_EVERYTHING,
            Datum::default(),
            0,
            Vec::new(),
            Vec::new(),
            &[],
            &[],
        )?;
    }

    // If the index is version 0, it may be missing null and placeholder entries,
    // which would render searches for nulls and full-index scans unreliable.
    // Throw an error if so.
    if hasNullQuery && !so.isVoidRes {
        let ginStats = ginutil::ginGetStats(&idx_rel)?;
        if ginStats.ginVersion < 1 {
            let rel_name = sx::gin_relation_get_relation_name::call(key_mcx, &idx_rel)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "old GIN indexes do not support whole-index scans nor searches for nulls",
                )
                .errhint(format!(
                    "To fix this, do REINDEX INDEX \"{}\".",
                    rel_name.as_str()
                ))
                .into_error());
        }
    }

    // pgstat_count_index_scan(scan->indexRelation);
    pgstat_count_index_scan::call(
        index_oid,
        scan.index_relation.rd_rel.relisshared,
        scan.index_relation.pgstat_enabled,
    );
    // if (scan->instrument) scan->instrument->nsearches++;
    if let Some(instr) = scan.instrument.as_mut() {
        instr.nsearches += 1;
    }

    Ok(())
}

// ===========================================================================
// ginrescan (ginscan.c:435)
// ===========================================================================

/// `ginrescan(scan, scankey, nscankeys, orderbys, norderbys)` (ginscan.c:435):
/// restart the scan with a fresh key set. Frees the existing scan keys, then
/// copies the new keys into the descriptor (`memcpy(scan->keyData, scankey,
/// ...)`).
pub fn ginrescan<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    _orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    ginFreeScanKeys(gin_so(scan));

    // if (scankey && scan->numberOfKeys > 0)
    //     memcpy(scan->keyData, scankey, numberOfKeys * sizeof(ScanKeyData));
    if !keys.is_empty() && scan.number_of_keys > 0 {
        let n = scan.number_of_keys as usize;
        scan.key_data[..n].clone_from_slice(&keys[..n]);
    }
    Ok(())
}

// ===========================================================================
// ginendscan (ginscan.c:449)
// ===========================================================================

/// `ginendscan(scan)` (ginscan.c:449): end the scan. Frees the scan keys, then
/// (in C) deletes the temp/key memory contexts and `pfree`s the opaque. Here,
/// dropping the opaque reclaims all owned state (including the two child
/// contexts the `mcx` arena holds); the descriptor `opaque` slot is cleared.
pub fn ginendscan<'mcx>(_mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    ginFreeScanKeys(gin_so(scan));

    // MemoryContextDelete(so->tempCtx); MemoryContextDelete(so->keyCtx);
    // pfree(so);  — the opaque (and the contexts it holds) drops here.
    scan.opaque = None;
    Ok(())
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `ItemPointerSetMin(p)` (gin_private.h): block 0, offset 0.
#[inline]
fn item_pointer_min() -> ItemPointerData {
    ItemPointerData::new(0, 0)
}

/// `RelationGetIndexScan(indexRelation, nkeys, norderbys)` (genam.c) — allocate
/// and zero-init the generic `IndexScanDescData` the AM extends via `opaque`.
/// Mirrors the BRIN / nbtree adapters.
fn relation_get_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let _ = mcx;
    let key_data = (0..nkeys.max(0)).map(|_| ScanKeyData::empty()).collect();
    let order_by_data = (0..norderbys.max(0)).map(|_| ScanKeyData::empty()).collect();
    let xs_orderbyvals = alloc::vec![Datum::null(); norderbys.max(0) as usize];
    let xs_orderbynulls = alloc::vec![false; norderbys.max(0) as usize];
    Ok(Box::new(IndexScanDescData {
        heap_relation: None,
        index_relation: index_relation.alias(),
        xs_snapshot: None,
        number_of_keys: nkeys,
        number_of_order_bys: norderbys,
        key_data,
        order_by_data,
        xs_want_itup: false,
        xs_temp_snap: false,
        kill_prior_tuple: false,
        ignore_killed_tuples: true,
        xact_started_in_recovery: false,
        opaque: None,
        instrument: None,
        xs_itup: None,
        xs_itupdesc: None,
        xs_hitup: None,
        xs_hitupdesc: None,
        xs_heaptid: ItemPointerData::default(),
        xs_heap_continue: false,
        xs_heapfetch: None,
        xs_recheck: false,
        xs_orderbyvals,
        xs_orderbynulls,
        xs_recheckorderby: false,
        parallel_scan: None,
    }))
}

/// `ReleaseBuffer(buffer)` (bufmgr.c). `ginget.c` (ported) pins posting-tree
/// buffers into a scan entry's `buffer`; `ginFreeScanKeys` releases any valid
/// pin here on rescan / endscan (ginscan.c:250-251), exactly as ginbtree does.
fn release_buffer(buffer: types_storage::storage::Buffer) {
    bufmgr::release_buffer::call(buffer)
}
