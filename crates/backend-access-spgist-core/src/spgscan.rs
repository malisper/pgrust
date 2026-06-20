//! Owned-tree Rust port of `src/backend/access/spgist/spgscan.c` (PostgreSQL
//! 18.3): the SP-GiST index access method's scan slice.
//!
//!   * [`spgbeginscan`] / [`spgrescan`] / [`spgendscan`] — the scan lifecycle
//!     callbacks. The scan-private [`SpGistScanOpaqueData`] working state rides
//!     `IndexScanDescData.opaque` via the A0 [`AmOpaque`] carrier under the
//!     [`tags::SPGIST_SCAN`] tag (`ambeginscan` builds it, every callback
//!     downcasts it back).
//!   * [`spggettuple`] / [`spggetbitmap`] — the `amgettuple` / `amgetbitmap`
//!     read drivers.
//!   * [`spgWalk`] — the scan-stack tree traversal that descends inner tuples
//!     (via [`spgInnerTest`] → the typed `spg_inner_consistent` dispatch seam)
//!     and tests leaf tuples (via [`spgTestLeafTuple`] → [`spgLeafTest`] → the
//!     typed `spg_leaf_consistent` dispatch seam), reporting matches through a
//!     `storeRes` callback ([`store_bitmap`] / [`store_gettuple`]).
//!   * [`spgcanreturn`] — the `amcanreturn` predicate.
//!
//! The opclass `inner_consistent` / `leaf_consistent` support procedures are
//! dispatched by support-proc OID through the per-AM typed seams (the BRIN-style
//! dispatch, *not* a generic fmgr-by-pointer path): C's
//! `index_getprocinfo(rel, 1, SPGIST_*_CONSISTENT_PROC)` becomes
//! `index_getprocid(rel, 1, SPGIST_*_CONSISTENT_PROC)` (the proc OID stored in
//! the scan opaque), and C's `FunctionCall2Coll(&so->*ConsistentFn, ...)`
//! becomes `spg_*_consistent::call(mcx, proc_oid, &in, &mut out)`.
//!
//! # Memory model versus C
//!
//! C's `SpGistSearchItem` queue lives in two palloc contexts (`tempCxt` /
//! `traversalCxt`) and is an *intrusive* pairing heap; items, reconstructed
//! values, leaf-tuple copies and traversal values are all manually `pfree`d.
//! The owned port stores [`SpGistSearchItem`] *values* directly in the
//! [`backend_lib_pairingheap`] arena heap, so Rust ownership replaces every
//! `pfree`/context-reset: removing an item from the queue moves it out (its
//! owned `value` / `leafTuple` / `traversalValue` drop with it), and the
//! per-page `tempCxt` reset is a no-op because the temp-context allocations
//! (the `*_consistent` inputs/outputs) are dropped at the end of each iteration.
//! The two memory contexts are kept on the opaque only for faithful
//! bookkeeping; the owned model needs no manual frees.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox};

use backend_storage_page::{
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid, ItemPointerSet,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageRef,
};
use backend_utils_error::ereport;
use types_error::error::ERROR;
use types_error::{PgError, PgResult};

use types_core::primitive::{OffsetNumber, Oid, RegProcedure};
use types_rel::Relation;
use types_storage::buf::{Buffer, InvalidBuffer, BUFFER_LOCK_SHARE};
use types_tableam::amapi::TIDBitmap as AmTIDBitmap;
use types_tableam::amopaque::{tags, AmOpaque, AmOpaqueType};
use types_tableam::genam::IndexOrderByDistance;
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_scan::sdir::ScanDirection;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::{
    ItemPointerData, TupleDescData, FLOAT4OID, FLOAT8OID,
};
use types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use types_spgist::{
    spgInnerConsistentIn, spgInnerConsistentOut, spgKeyColumn, spgLeafConsistentIn,
    spgLeafConsistentOut, SpGistCache, SpGistState, SPGIST_DEAD, SPGIST_INNER_CONSISTENT_PROC,
    SPGIST_LEAF_CONSISTENT_PROC, SPGIST_LIVE, SPGIST_METAPAGE_BLKNO, SPGIST_NULL_BLKNO,
    SPGIST_REDIRECT, SPGIST_ROOT_BLKNO,
};

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_activity_pgstat_seams::pgstat_count_index_scan;
use backend_nodes_core_seams::tbm_add_tuples;

use crate::spgdoinsert::{
    it_all_the_same, it_datum, it_n_nodes, it_prefix_size, lt_datum, lt_get_next_offset,
    lt_tupstate, node_offsets, node_t_tid,
};
use crate::{
    getSpGistTupleDesc, initSpGistState, spgDeformLeafTuple, spgExtractNodeLabels, spgGetCache,
    SpGistPageIsLeaf, SpGistPageStoresNulls,
};
use types_spgist::SpGistBlockIsRoot;

// ===========================================================================
// Local invariants / constants (relscan.h, itemptr.h, indextuple.h).
// ===========================================================================

/// `FirstOffsetNumber` (off.h).
const FirstOffsetNumber: OffsetNumber = 1;
/// `InvalidOffsetNumber` (off.h).
const InvalidOffsetNumber: OffsetNumber = 0;
/// `MaxOffsetNumber` (off.h): `BLCKSZ / sizeof(ItemIdData)`.
const MaxOffsetNumber: OffsetNumber = 8192 / 4;
/// `MaxIndexTuplesPerPage` (itup.h) — the upper bound on per-page results, the
/// width of the gettuple result arrays. (SpGistLeafTuples are larger than
/// IndexTuples, so this bound is safe; spgist_private.h note.)
const MaxIndexTuplesPerPage: usize = 8192 / 16;

// ===========================================================================
// SpGistSearchItem — the scan-queue work item (spgist_private.h).
// ===========================================================================

/// `SpGistSearchItem` (spgist_private.h) — one to-be-visited item in the scan
/// queue. The C intrusive `pairingheap_node` is replaced by storage in the
/// [`backend_lib_pairingheap`] arena (the heap owns the value); the `void
/// *traversalValue` opclass traverse state is the owned byte buffer C packs into
/// `in.traversalMemoryContext`; the whole leaf tuple (`leafTuple`, kept only for
/// INCLUDE-column reconstruction) is an owned page-image byte buffer.
pub struct SpGistSearchItem<'mcx> {
    /// `value` — value reconstructed from parent, or leafValue if `isLeaf`.
    pub value: Datum<'mcx>,
    /// `leafTuple` — the whole leaf tuple bytes, if needed (INCLUDE columns).
    pub leafTuple: Option<Vec<u8>>,
    /// `traversalValue` — opclass-specific traverse value (`void *`).
    pub traversalValue: Option<Vec<u8>>,
    /// `level` — level of items on this page.
    pub level: i32,
    /// `heapPtr` — heap info, if heap tuple.
    pub heapPtr: ItemPointerData,
    /// `isNull` — SearchItem is NULL item.
    pub isNull: bool,
    /// `isLeaf` — SearchItem is heap item.
    pub isLeaf: bool,
    /// `recheck` — qual recheck is needed.
    pub recheck: bool,
    /// `recheckDistances` — distance recheck is needed.
    pub recheckDistances: bool,
    /// `distances[numberOfNonNullOrderBys]` — flexible array; empty for NULL
    /// items.
    pub distances: Vec<f64>,
}

// ===========================================================================
// SpGistScanOpaqueData — the scan-private working state (spgist_private.h).
//
// Held behind the A0 AmOpaque carrier in IndexScanDescData.opaque, under the
// SPGIST_SCAN tag.
// ===========================================================================

/// `SpGistScanOpaqueData` (spgist_private.h) — the SP-GiST `void *opaque` scan
/// payload. Built in [`spgbeginscan`], reset in [`spgrescan`], read by
/// [`spgWalk`] / [`spggettuple`] / [`spggetbitmap`], torn down in
/// [`spgendscan`].
pub struct SpGistScanOpaqueData<'mcx> {
    /// `state` — the per-operation working state (see [`SpGistState`]).
    pub state: SpGistState<'mcx>,

    /// `scanQueue` — the pairing-heap queue of to-be-visited items. The
    /// comparator captures `numberOfNonNullOrderBys` (C's `arg = so`).
    pub scanQueue: SpgScanQueue<'mcx>,

    // Control flags showing whether to search nulls and/or non-nulls.
    /// `searchNulls` — scan matches (all) null entries.
    pub searchNulls: bool,
    /// `searchNonNulls` — scan matches (some) non-null entries.
    pub searchNonNulls: bool,

    // Index quals to be passed to opclass (null-related quals removed).
    /// `numberOfKeys` — number of index qualifier conditions.
    pub numberOfKeys: i32,
    /// `keyData` — array of index qualifier descriptors.
    pub keyData: Vec<ScanKeyData<'mcx>>,
    /// `numberOfOrderBys` — number of ordering operators.
    pub numberOfOrderBys: i32,
    /// `numberOfNonNullOrderBys` — number of ordering operators with non-NULL
    /// arguments.
    pub numberOfNonNullOrderBys: i32,
    /// `orderByData` — array of ordering op descriptors.
    pub orderByData: Vec<ScanKeyData<'mcx>>,
    /// `orderByTypes` — array of ordering op return types.
    pub orderByTypes: Vec<Oid>,
    /// `nonNullOrderByOffsets` — offset of each non-NULL ordering key in the
    /// original array (`-1` for a removed NULL key).
    pub nonNullOrderByOffsets: Vec<i32>,
    /// `indexCollation` — collation of index column.
    pub indexCollation: Oid,

    // Opclass-defined functions (dispatched by OID via the typed seams).
    /// `innerConsistentFn.fn_oid` — `SPGIST_INNER_CONSISTENT_PROC` OID.
    pub innerConsistentProc: RegProcedure,
    /// `leafConsistentFn.fn_oid` — `SPGIST_LEAF_CONSISTENT_PROC` OID.
    pub leafConsistentProc: RegProcedure,

    // Pre-allocated workspace arrays.
    /// `zeroDistances` — `numberOfOrderBys` zeros (start-item distances).
    pub zeroDistances: Vec<f64>,
    /// `infDistances` — `numberOfOrderBys` infinities (default child distances).
    pub infDistances: Vec<f64>,

    // amgetbitmap-only fields.
    /// `ntids` — number of TIDs passed to bitmap.
    pub ntids: i64,

    // amgettuple-only fields.
    /// `want_itup` — are we reconstructing tuples?
    pub want_itup: bool,
    /// `reconTupDesc` — descriptor for reconstructed tuples.
    pub reconTupDesc: TupleDescData<'mcx>,
    /// `nPtrs` — number of TIDs found on current page.
    pub nPtrs: i32,
    /// `iPtr` — index for scanning through same.
    pub iPtr: i32,
    /// `heapPtrs[MaxIndexTuplesPerPage]` — TIDs from cur page.
    pub heapPtrs: Vec<ItemPointerData>,
    /// `recheck[MaxIndexTuplesPerPage]` — their recheck flags.
    pub recheck: Vec<bool>,
    /// `recheckDistances[MaxIndexTuplesPerPage]` — distance recheck flags.
    pub recheckDistances: Vec<bool>,
    /// `reconTups[MaxIndexTuplesPerPage]` — reconstructed tuples (full
    /// [`FormedTuple`] so the user-data area survives until handed to
    /// `xs_hitup`).
    pub reconTups: Vec<Option<FormedTuple<'mcx>>>,
    /// `distances[MaxIndexTuplesPerPage]` — per-result distance arrays for
    /// recheck (`None` for NULL / non-ordered).
    pub distances: Vec<Option<Vec<IndexOrderByDistance>>>,
}

/// The owned pairing-heap queue of [`SpGistSearchItem`]s. The comparator is a
/// boxed closure capturing `numberOfNonNullOrderBys` (C passes `so` as `arg`).
pub type SpgScanQueue<'mcx> = backend_lib_pairingheap::PairingHeap<
    SpGistSearchItem<'mcx>,
    Box<dyn Fn(&SpGistSearchItem<'mcx>, &SpGistSearchItem<'mcx>) -> core::cmp::Ordering>,
>;

/// `SpGistScanOpaqueData` is the concrete type stored in
/// `IndexScanDescData.opaque` (C's `void *opaque`).
impl<'mcx> AmOpaqueType<'mcx> for SpGistScanOpaqueData<'mcx> {
    const TAG: types_tableam::amopaque::AmOpaqueTag = tags::SPGIST_SCAN;
}

/// Downcast `scan.opaque` to the SP-GiST scan working state (the A0 tag-checked
/// downcast); panics with a clear message if the descriptor was not built by
/// [`spgbeginscan`] (a programming error — C would just cast `void *`).
pub(crate) fn so<'a, 'mcx>(
    scan: &'a mut IndexScanDescData<'mcx>,
) -> &'a mut SpGistScanOpaqueData<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("SP-GiST scan descriptor has no opaque (not built by spgbeginscan)")
        .downcast_mut::<SpGistScanOpaqueData<'mcx>>()
        .expect("SP-GiST scan opaque is not a SpGistScanOpaqueData")
}

// ===========================================================================
// pairingheap_SpGistSearchItem_cmp (spgscan.c:40)
// ===========================================================================

/// `pairingheap_SpGistSearchItem_cmp(a, b, arg)` (spgscan.c:40) — the queue
/// comparator. KNN searches only support NULLS LAST, preserved here.
/// `numberOfNonNullOrderBys` is captured from the scan opaque (C's `arg = so`).
fn spgSearchItemCmp(
    sa: &SpGistSearchItem<'_>,
    sb: &SpGistSearchItem<'_>,
    numberOfNonNullOrderBys: i32,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    if sa.isNull {
        if !sb.isNull {
            return Ordering::Less;
        }
    } else if sb.isNull {
        return Ordering::Greater;
    } else {
        // Order according to distance comparison.
        for i in 0..numberOfNonNullOrderBys as usize {
            let da = sa.distances[i];
            let db = sb.distances[i];
            if da.is_nan() && db.is_nan() {
                continue; // NaN == NaN
            }
            if da.is_nan() {
                return Ordering::Less; // NaN > number
            }
            if db.is_nan() {
                return Ordering::Greater; // number < NaN
            }
            if da != db {
                return if da < db {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
        }
    }

    // Leaf items go before inner pages, to ensure a depth-first search.
    if sa.isLeaf && !sb.isLeaf {
        return Ordering::Greater;
    }
    if !sa.isLeaf && sb.isLeaf {
        return Ordering::Less;
    }

    Ordering::Equal
}

// ===========================================================================
// spgAddSearchItemToQueue / spgAllocSearchItem / spgAddStartItem (spgscan.c)
// ===========================================================================

/// `spgAddSearchItemToQueue(so, item)` (spgscan.c:107) — add `item` to the
/// pairing-heap queue. (C's `spgFreeSearchItem` has no analog: removing/dropping
/// an item from the owned arena heap frees its owned payload.)
fn spgAddSearchItemToQueue<'mcx>(
    so: &mut SpGistScanOpaqueData<'mcx>,
    item: SpGistSearchItem<'mcx>,
) -> PgResult<()> {
    so.scanQueue.add(item)?;
    Ok(())
}

/// `spgAllocSearchItem(so, isnull, distances)` (spgscan.c:113) — allocate a
/// search item, copying the distance array only for non-NULL items.
fn spgAllocSearchItem<'mcx>(
    so: &SpGistScanOpaqueData<'mcx>,
    isnull: bool,
    distances: &[f64],
) -> SpGistSearchItem<'mcx> {
    let dvec = if !isnull && so.numberOfNonNullOrderBys > 0 {
        distances[..so.numberOfNonNullOrderBys as usize].to_vec()
    } else {
        Vec::new()
    };
    SpGistSearchItem {
        value: Datum::null(),
        leafTuple: None,
        traversalValue: None,
        level: 0,
        heapPtr: ItemPointerData::default(),
        isNull: isnull,
        isLeaf: false,
        recheck: false,
        recheckDistances: false,
        distances: dvec,
    }
}

/// `spgAddStartItem(so, isnull)` (spgscan.c:129) — add a root-scan start item
/// for the null- or non-null-entry tree.
fn spgAddStartItem<'mcx>(so: &mut SpGistScanOpaqueData<'mcx>, isnull: bool) -> PgResult<()> {
    let zero = so.zeroDistances.clone();
    let mut startEntry = spgAllocSearchItem(so, isnull, &zero);

    ItemPointerSet(
        &mut startEntry.heapPtr,
        if isnull {
            SPGIST_NULL_BLKNO
        } else {
            SPGIST_ROOT_BLKNO
        },
        FirstOffsetNumber,
    );
    startEntry.isLeaf = false;
    startEntry.level = 0;
    startEntry.value = Datum::null();
    startEntry.leafTuple = None;
    startEntry.traversalValue = None;
    startEntry.recheck = false;
    startEntry.recheckDistances = false;

    spgAddSearchItemToQueue(so, startEntry)
}

/// `resetSpGistScanOpaque(so)` (spgscan.c:153) — reset the queue to search the
/// root pages. The owned model drops the old queue (freeing all its items'
/// payloads) and the previously collected per-page distance / recon arrays.
fn resetSpGistScanOpaque<'mcx>(so: &mut SpGistScanOpaqueData<'mcx>) -> PgResult<()> {
    // MemoryContextReset(traversalCxt) + pairingheap_allocate(cmp, so): replace
    // the queue with a fresh one whose comparator captures the (now-known)
    // numberOfNonNullOrderBys.
    let nnobys = so.numberOfNonNullOrderBys;
    so.scanQueue = backend_lib_pairingheap::pairingheap_allocate(Box::new(
        move |a: &SpGistSearchItem<'mcx>, b: &SpGistSearchItem<'mcx>| {
            spgSearchItemCmp(a, b, nnobys)
        },
    )
        as Box<dyn Fn(&SpGistSearchItem<'mcx>, &SpGistSearchItem<'mcx>) -> core::cmp::Ordering>);

    if so.searchNulls {
        // Add a work item to scan the null index entries.
        spgAddStartItem(so, true)?;
    }
    if so.searchNonNulls {
        // Add a work item to scan the non-null index entries.
        spgAddStartItem(so, false)?;
    }

    // C pfrees so->distances[i] / so->reconTups[i] here to avoid leaks; the
    // owned arrays drop their elements when truncated.
    so.distances.clear();
    so.reconTups.clear();
    so.iPtr = 0;
    so.nPtrs = 0;

    Ok(())
}

// ===========================================================================
// spgPrepareScanKeys (spgscan.c:207)
// ===========================================================================

/// `spgPrepareScanKeys(scan)` (spgscan.c:207) — preprocess the caller-given scan
/// keys into the opaque: set `searchNulls` / `searchNonNulls` / `numberOfKeys` /
/// `keyData`, eliminating null-related considerations (IS NULL / IS NOT NULL and
/// strict-null quals) from what the opclass consistent functions must handle.
fn spgPrepareScanKeys<'mcx>(scan: &mut IndexScanDescData<'mcx>) {
    // so->numberOfOrderBys = scan->numberOfOrderBys; so->orderByData =
    // scan->orderByData.
    let number_of_order_bys = scan.number_of_order_bys;
    let order_by_data: Vec<ScanKeyData<'mcx>> = scan.order_by_data.clone();
    let number_of_keys = scan.number_of_keys;
    let scan_keys: Vec<ScanKeyData<'mcx>> = scan.key_data.clone();

    let so = so(scan);
    so.numberOfOrderBys = number_of_order_bys;
    so.orderByData = order_by_data;

    if so.numberOfOrderBys <= 0 {
        so.numberOfNonNullOrderBys = 0;
    } else {
        let mut j = 0i32;
        // Remove all NULL keys, but remember their offsets in the original
        // array.
        for i in 0..so.numberOfOrderBys as usize {
            if (so.orderByData[i].sk_flags & SK_ISNULL) != 0 {
                so.nonNullOrderByOffsets[i] = -1;
            } else {
                if i as i32 != j {
                    so.orderByData[j as usize] = so.orderByData[i].clone();
                }
                so.nonNullOrderByOffsets[i] = j;
                j += 1;
            }
        }
        so.numberOfNonNullOrderBys = j;
    }

    if number_of_keys <= 0 {
        // If no quals, whole-index scan is required.
        so.searchNulls = true;
        so.searchNonNulls = true;
        so.numberOfKeys = 0;
        return;
    }

    // Examine the given quals.
    let mut qual_ok = true;
    let mut have_is_null = false;
    let mut have_not_null = false;
    let mut nkeys = 0usize;
    for i in 0..number_of_keys as usize {
        let skey = &scan_keys[i];
        if (skey.sk_flags & SK_SEARCHNULL) != 0 {
            have_is_null = true;
        } else if (skey.sk_flags & SK_SEARCHNOTNULL) != 0 {
            have_not_null = true;
        } else if (skey.sk_flags & SK_ISNULL) != 0 {
            // ordinary qual with null argument - unsatisfiable.
            qual_ok = false;
            break;
        } else {
            // ordinary qual, propagate into so->keyData.
            so.keyData[nkeys] = skey.clone();
            nkeys += 1;
            // this effectively creates a not-null requirement.
            have_not_null = true;
        }
    }

    // IS NULL in combination with something else is unsatisfiable.
    if have_is_null && have_not_null {
        qual_ok = false;
    }

    // Emit results.
    if qual_ok {
        so.searchNulls = have_is_null;
        so.searchNonNulls = have_not_null;
        so.numberOfKeys = nkeys as i32;
    } else {
        so.searchNulls = false;
        so.searchNonNulls = false;
        so.numberOfKeys = 0;
    }
}

// ===========================================================================
// spgbeginscan (spgscan.c:303)
// ===========================================================================

/// `spgbeginscan(rel, keysz, orderbysz)` (spgscan.c:303) — build the SP-GiST
/// scan working state and the generic descriptor that carries it. The C
/// `tempCxt` / `traversalCxt` are subsumed by Rust ownership (see module docs);
/// the inner/leaf consistent FmgrInfo copies become the proc OIDs the typed
/// dispatch seams take.
pub fn spgbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    keysz: i32,
    orderbysz: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // scan = RelationGetIndexScan(rel, keysz, orderbysz).
    let mut scan = relation_get_index_scan(mcx, rel, keysz, orderbysz)?;

    // so = palloc0(sizeof(SpGistScanOpaqueData)).
    // initSpGistState(&so->state, scan->indexRelation).
    let state = initSpGistState(mcx, rel)?;

    // keyData = palloc(sizeof(ScanKeyData) * keysz) (or NULL).
    let keyData: Vec<ScanKeyData<'mcx>> = if keysz > 0 {
        alloc::vec![ScanKeyData::empty(); keysz as usize]
    } else {
        Vec::new()
    };

    // reconTupDesc = scan->xs_hitupdesc = getSpGistTupleDesc(rel,
    // &so->state.attType): the recon descriptor, shown as attType.
    let reconTupDesc = getSpGistTupleDesc(mcx, rel, &state.attType)?;
    scan.xs_hitupdesc = Some(Box::new(reconTupDesc.clone_in(mcx)?));

    // Allocate the order-by arrays (filled in spgrescan), and the constant
    // zero/inf distance arrays. xs_orderbyvals/xs_orderbynulls are sized by
    // relation_get_index_scan already.
    let mut orderByTypes: Vec<Oid> = Vec::new();
    let mut nonNullOrderByOffsets: Vec<i32> = Vec::new();
    let mut zeroDistances: Vec<f64> = Vec::new();
    let mut infDistances: Vec<f64> = Vec::new();
    if scan.number_of_order_bys > 0 {
        orderByTypes = alloc::vec![0 as Oid; scan.number_of_order_bys as usize];
        nonNullOrderByOffsets = alloc::vec![0i32; scan.number_of_order_bys as usize];
        zeroDistances = alloc::vec![0.0f64; scan.number_of_order_bys as usize];
        // get_float8_infinity() == INFINITY.
        infDistances = alloc::vec![f64::INFINITY; scan.number_of_order_bys as usize];
        // scan->xs_orderbynulls already memset(true) by relation_get_index_scan.
    }

    // fmgr_info_copy(&so->innerConsistentFn, index_getprocinfo(rel, 1,
    // SPGIST_INNER_CONSISTENT_PROC)); likewise leaf. Owned model: store the OID.
    let innerConsistentProc =
        relcache::index_getprocid::call(rel, 1, SPGIST_INNER_CONSISTENT_PROC as u16)?;
    let leafConsistentProc =
        relcache::index_getprocid::call(rel, 1, SPGIST_LEAF_CONSISTENT_PROC as u16)?;

    // so->indexCollation = rel->rd_indcollation[0].
    let indexCollation = rel.rd_indcollation[0];

    // pairingheap_allocate placeholder (filled with the right comparator in
    // resetSpGistScanOpaque / spgrescan).
    let scanQueue = backend_lib_pairingheap::pairingheap_allocate(Box::new(
        |a: &SpGistSearchItem<'mcx>, b: &SpGistSearchItem<'mcx>| spgSearchItemCmp(a, b, 0),
    )
        as Box<dyn Fn(&SpGistSearchItem<'mcx>, &SpGistSearchItem<'mcx>) -> core::cmp::Ordering>);

    let opaque = SpGistScanOpaqueData {
        state,
        scanQueue,
        searchNulls: false,
        searchNonNulls: false,
        numberOfKeys: 0,
        keyData,
        numberOfOrderBys: 0,
        numberOfNonNullOrderBys: 0,
        orderByData: Vec::new(),
        orderByTypes,
        nonNullOrderByOffsets,
        indexCollation,
        innerConsistentProc,
        leafConsistentProc,
        zeroDistances,
        infDistances,
        ntids: 0,
        want_itup: false,
        reconTupDesc,
        nPtrs: 0,
        iPtr: 0,
        heapPtrs: Vec::new(),
        recheck: Vec::new(),
        recheckDistances: Vec::new(),
        reconTups: Vec::new(),
        distances: Vec::new(),
    };

    // scan->opaque = so (A0 erase under the SPGIST_SCAN tag).
    scan.opaque = Some(erase_opaque(mcx, opaque)?);

    Ok(scan)
}

// ===========================================================================
// spgrescan (spgscan.c:379)
// ===========================================================================

/// `spgrescan(scan, scankey, nscankeys, orderbys, norderbys)` (spgscan.c:379) —
/// re-initialize the scan: copy the new keys/orderbys into the descriptor, look
/// up each ordering operator's result type, preprocess the scan keys and reset
/// the queue.
pub fn spgrescan<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    scankey: &[ScanKeyData<'mcx>],
    orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    // copy scankeys into local storage.
    if !scankey.is_empty() && scan.number_of_keys > 0 {
        let n = scan.number_of_keys as usize;
        scan.key_data[..n].clone_from_slice(&scankey[..n]);
    }

    // initialize order-by data if needed.
    if !orderbys.is_empty() && scan.number_of_order_bys > 0 {
        let n = scan.number_of_order_bys as usize;
        scan.order_by_data[..n].clone_from_slice(&orderbys[..n]);

        // Look up the datatype returned by each original ordering operator.
        for i in 0..n {
            let fn_oid = scan.order_by_data[i].sk_func.fn_oid;
            let rettype = lsyscache::get_func_rettype::call(fn_oid)?;
            so(scan).orderByTypes[i] = rettype;
        }
    }

    // preprocess scankeys, set up the representation in *so.
    spgPrepareScanKeys(scan);

    // set up starting queue entries.
    resetSpGistScanOpaque(so(scan))?;

    // count an indexscan for stats.
    pgstat_count_index_scan::call(
        scan.index_relation.rd_id,
        scan.index_relation.rd_rel.relisshared,
        scan.index_relation.pgstat_enabled,
    );
    if let Some(instr) = scan.instrument.as_mut() {
        instr.nsearches += 1;
    }

    Ok(())
}

// ===========================================================================
// spgendscan (spgscan.c:428)
// ===========================================================================

/// `spgendscan(scan)` (spgscan.c:428) — tear down the scan working state. C
/// deletes `tempCxt`/`traversalCxt`, frees `keyData` and the order-by arrays,
/// and (if `leafTupDesc != RelationGetDescr`) frees the leaf descriptor; the
/// owned model drops the [`SpGistScanOpaqueData`] (and the queue + all its
/// items) here, freeing everything.
pub fn spgendscan(_opaque: &mut SpGistScanOpaqueData<'_>) -> PgResult<()> {
    // Everything (state, scanQueue with all items, keyData, order-by arrays,
    // recon arrays, leaf/recon descriptors) is owned and dropped with the
    // SpGistScanOpaqueData. Nothing extra to release.
    Ok(())
}

// ===========================================================================
// spgNewHeapItem (spgscan.c:462)
// ===========================================================================

/// `spgNewHeapItem(so, level, leafTuple, leafValue, recheck, recheckDistances,
/// isnull, distances)` (spgscan.c:462) — leaf `SpGistSearchItem` constructor
/// (the ordered-scan case). Copies the reconstructed value / leaf tuple as C
/// does (out of the temp context). `leaf_tuple_bytes` is the leaf tuple's
/// on-page image; `leaf_heap_ptr` is its `heapPtr`.
fn spgNewHeapItem<'mcx>(
    so: &SpGistScanOpaqueData<'mcx>,
    level: i32,
    leaf_tuple_bytes: &[u8],
    leaf_heap_ptr: ItemPointerData,
    leaf_value: Datum<'mcx>,
    recheck: bool,
    recheck_distances: bool,
    isnull: bool,
    distances: &[f64],
) -> PgResult<SpGistSearchItem<'mcx>> {
    let mut item = spgAllocSearchItem(so, isnull, distances);

    item.level = level;
    item.heapPtr = leaf_heap_ptr;

    // If we need the reconstructed value, copy it (the correct leafValue type is
    // attType, not leafType). datumCopy of a by-value scalar is the value;
    // by-reference values are already owned bytes in our model.
    if so.want_itup {
        item.value = if isnull {
            Datum::null()
        } else {
            leaf_value
        };

        // If we're going to need to reconstruct INCLUDE attributes, store the
        // whole leaf tuple so we can get the INCLUDE attributes out of it.
        if leaf_tupdesc_natts(&so.state) > 1 {
            item.leafTuple = Some(leaf_tuple_bytes.to_vec());
        } else {
            item.leafTuple = None;
        }
    } else {
        item.value = Datum::null();
        item.leafTuple = None;
    }
    item.traversalValue = None;
    item.isLeaf = true;
    item.recheck = recheck;
    item.recheckDistances = recheck_distances;

    Ok(item)
}

// ===========================================================================
// spgLeafTest (spgscan.c:515)
// ===========================================================================

/// `spgLeafTest(so, item, leafTuple, isnull, reportedSome, storeRes)`
/// (spgscan.c:515) — test whether a leaf tuple satisfies all the scan keys. For
/// an ordered scan a passing leaf is queued (so the next `spgWalk` iteration
/// pops it in distance order); otherwise it is reported right away via
/// `store_res`.
///
/// `parent` carries the parent inner item's `value` / `traversalValue` / `level`
/// (the leaf-consistent input `reconstructedValue` etc.). `leaf_tuple_bytes` is
/// the leaf tuple's on-page image.
fn spgLeafTest<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    parent_value: &Datum<'mcx>,
    parent_traversal_value: &Option<Vec<u8>>,
    parent_level: i32,
    leaf_tuple_bytes: &[u8],
    leaf_heap_ptr: ItemPointerData,
    isnull: bool,
    reported_some: &mut bool,
    store_res: StoreResKind,
    bitmap: Option<&mut AmTIDBitmap>,
) -> PgResult<bool> {
    let leaf_value: Datum<'mcx>;
    let distances: Vec<f64>;
    let result: bool;
    let recheck: bool;
    let recheck_distances: bool;

    if isnull {
        // Should not have arrived on a nulls page unless nulls are wanted.
        debug_assert!(so.searchNulls);
        leaf_value = Datum::null();
        distances = Vec::new();
        recheck = false;
        recheck_distances = false;
        result = true;
    } else {
        // use temp context for calling leaf_consistent (subsumed by ownership).
        let leaf_datum = lt_datum(mcx, &so.state, leaf_tuple_bytes)?;

        let in_ = spgLeafConsistentIn {
            scankeys: so.keyData[..so.numberOfKeys as usize].to_vec(),
            orderbys: so.orderByData[..so.numberOfNonNullOrderBys as usize].to_vec(),
            // Assert(!item->isLeaf) — reconstructedValue is the parent's value.
            reconstructedValue: parent_value.clone(),
            traversalValue: parent_traversal_value.clone(),
            level: parent_level,
            returnData: so.want_itup,
            leafDatum: leaf_datum,
        };
        let mut out = spgLeafConsistentOut::default();

        result = backend_access_spg_core_seams::spg_leaf_consistent::call(
            mcx,
            so.leafConsistentProc,
            &in_,
            &mut out,
        )?;
        recheck = out.recheck;
        recheck_distances = out.recheckDistances;
        leaf_value = out.leafValue.unwrap_or_else(Datum::null);
        distances = out.distances.unwrap_or_default();
    }

    if result {
        // item passes the scankeys.
        if so.numberOfNonNullOrderBys > 0 {
            // the scan is ordered -> add the item to the queue.
            let heap_item = spgNewHeapItem(
                so,
                parent_level,
                leaf_tuple_bytes,
                leaf_heap_ptr,
                leaf_value,
                recheck,
                recheck_distances,
                isnull,
                &distances,
            )?;
            spgAddSearchItemToQueue(so, heap_item)?;
        } else {
            // non-ordered scan, so report the item right away.
            debug_assert!(!recheck_distances);
            store_res_dispatch(
                mcx,
                so,
                store_res,
                bitmap,
                &leaf_heap_ptr,
                leaf_value,
                isnull,
                Some(leaf_tuple_bytes),
                recheck,
                false,
                &[],
            )?;
            *reported_some = true;
        }
    }

    Ok(result)
}

// ===========================================================================
// spgInnerTest (spgscan.c:666) + spgInitInnerConsistentIn / spgMakeInnerItem
// ===========================================================================

/// `spgInnerTest(so, item, innerTuple, isnull)` (spgscan.c:666) — run the
/// opclass `inner_consistent` method (or, for a nulls page, force-visit every
/// child), then queue a search item for each selected, valid child node.
/// `inner_bytes` is the inner tuple's on-page image.
fn spgInnerTest<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    parent_value: &Datum<'mcx>,
    parent_traversal_value: &Option<Vec<u8>>,
    parent_level: i32,
    inner_bytes: &[u8],
    isnull: bool,
) -> PgResult<()> {
    let n_nodes = it_n_nodes(inner_bytes) as i32;
    let all_the_same = it_all_the_same(inner_bytes);

    let mut out = spgInnerConsistentOut::default();

    if !isnull {
        // spgInitInnerConsistentIn(&in, so, item, innerTuple).
        let prefix_datum = it_datum(mcx, &so.state, inner_bytes)?;
        let node_labels = spgExtractNodeLabels(mcx, &so.state, inner_bytes)?
            .map(|v| v.to_vec());
        let in_ = spgInnerConsistentIn {
            scankeys: so.keyData[..so.numberOfKeys as usize].to_vec(),
            orderbys: so.orderByData[..so.numberOfNonNullOrderBys as usize].to_vec(),
            // Assert(!item->isLeaf).
            reconstructedValue: parent_value.clone(),
            // traversalMemoryContext is the long-lived index-scan context; the
            // owned `traversalValues` outputs are returned as owned byte buffers.
            traversalValue: parent_traversal_value.clone(),
            level: parent_level,
            returnData: so.want_itup,
            allTheSame: all_the_same,
            hasPrefix: it_prefix_size(inner_bytes) > 0,
            prefixDatum: prefix_datum,
            nNodes: n_nodes,
            nodeLabels: node_labels,
        };

        // use user-defined inner consistent method.
        backend_access_spg_core_seams::spg_inner_consistent::call(
            mcx,
            so.innerConsistentProc,
            &in_,
            &mut out,
        )?;
    } else {
        // force all children to be visited.
        out.nNodes = n_nodes;
        out.nodeNumbers = (0..n_nodes).collect();
    }

    // If allTheSame, they should all or none of them match.
    if all_the_same && out.nNodes != 0 && out.nNodes != n_nodes {
        return Err(elog_error(
            "inconsistent inner_consistent results for allTheSame inner tuple".into(),
        ));
    }

    if out.nNodes != 0 {
        // collect node pointers (SGITITERATE).
        let node_offs = node_offsets(inner_bytes);

        for i in 0..out.nNodes as usize {
            let node_n = out.nodeNumbers[i];
            debug_assert!(node_n >= 0 && node_n < n_nodes);

            let node_bytes = &inner_bytes[node_offs[node_n as usize]..];
            let node_tid = node_t_tid(node_bytes);

            if !ItemPointerIsValid(Some(&node_tid)) {
                continue;
            }

            // Use infinity distances if innerConsistentFn() failed to return
            // them or if is a NULL item (their distances are really unused).
            let distances: Vec<f64> = if !out.distances.is_empty() {
                out.distances[i].clone()
            } else {
                so.infDistances.clone()
            };

            // spgMakeInnerItem(so, item, node, &out, i, isnull, distances).
            let mut inner_item = spgAllocSearchItem(so, isnull, &distances);
            inner_item.heapPtr = node_tid;
            inner_item.level = if !out.levelAdds.is_empty() {
                parent_level + out.levelAdds[i]
            } else {
                parent_level
            };
            // reconstructed values are of type leafType; owned by our model.
            inner_item.value = if !out.reconstructedValues.is_empty() {
                out.reconstructedValues[i].clone()
            } else {
                Datum::null()
            };
            inner_item.leafTuple = None;
            inner_item.traversalValue = if !out.traversalValues.is_empty() {
                out.traversalValues[i].clone()
            } else {
                None
            };
            inner_item.isLeaf = false;
            inner_item.recheck = false;
            inner_item.recheckDistances = false;

            spgAddSearchItemToQueue(so, inner_item)?;
        }
    }

    Ok(())
}

// ===========================================================================
// spgGetNextQueueItem (spgscan.c:745)
// ===========================================================================

/// `spgGetNextQueueItem(so)` (spgscan.c:745) — pop the next (ordered) queue
/// item, or `None` when the index is exhausted. (C's "caller pfrees it" becomes
/// Rust's move-out ownership.)
fn spgGetNextQueueItem<'mcx>(
    so: &mut SpGistScanOpaqueData<'mcx>,
) -> Option<SpGistSearchItem<'mcx>> {
    if so.scanQueue.is_empty() {
        return None; // Done when both heaps are empty.
    }
    so.scanQueue.remove_first()
}

// ===========================================================================
// SpGistSpecialOffsetNumbers (spgscan.c:755) + spgTestLeafTuple (spgscan.c:762)
// ===========================================================================

/// `SpGistBreakOffsetNumber = InvalidOffsetNumber`.
const SpGistBreakOffsetNumber: OffsetNumber = InvalidOffsetNumber;
/// `SpGistRedirectOffsetNumber = MaxOffsetNumber + 1`.
const SpGistRedirectOffsetNumber: OffsetNumber = MaxOffsetNumber + 1;
/// `SpGistErrorOffsetNumber = MaxOffsetNumber + 2`.
const SpGistErrorOffsetNumber: OffsetNumber = MaxOffsetNumber + 2;

/// `spgTestLeafTuple(so, item, page, offset, isnull, isroot, reportedSome,
/// storeRes)` (spgscan.c:762) — examine one leaf tuple at `offset`. Returns the
/// next-offset to follow in the chain, or a special offset
/// (break/redirect/error). On a redirect it updates `item_heap_ptr` to the
/// redirect target (C mutates `item->heapPtr`).
fn spgTestLeafTuple<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    item_heap_ptr: &mut ItemPointerData,
    parent_value: &Datum<'mcx>,
    parent_traversal_value: &Option<Vec<u8>>,
    parent_level: i32,
    page: &[u8],
    offset: OffsetNumber,
    isnull: bool,
    isroot: bool,
    reported_some: &mut bool,
    store_res: StoreResKind,
    bitmap: Option<&mut AmTIDBitmap>,
) -> PgResult<OffsetNumber> {
    let leaf_tuple: Vec<u8> = {
        let pr = PageRef::new(page)?;
        let iid = PageGetItemId(&pr, offset)?;
        PageGetItem(&pr, &iid)?.to_vec()
    };

    let tupstate = lt_tupstate(&leaf_tuple);
    if tupstate != SPGIST_LIVE {
        if !isroot {
            // all tuples on root should be live.
            if tupstate == SPGIST_REDIRECT {
                // redirection tuple should be first in chain.
                debug_assert!(offset == ItemPointerGetOffsetNumber(item_heap_ptr));
                // transfer attention to redirect point: the SpGistDeadTuple
                // pointer is the ItemPointerData @6.
                let ptr = read_dead_pointer(&leaf_tuple);
                *item_heap_ptr = ptr;
                debug_assert!(
                    ItemPointerGetBlockNumber(item_heap_ptr) != SPGIST_METAPAGE_BLKNO
                );
                return Ok(SpGistRedirectOffsetNumber);
            }
            if tupstate == SPGIST_DEAD {
                // dead tuple should be first in chain; no live entries on page.
                debug_assert!(offset == ItemPointerGetOffsetNumber(item_heap_ptr));
                debug_assert!(lt_get_next_offset(&leaf_tuple) == InvalidOffsetNumber);
                return Ok(SpGistBreakOffsetNumber);
            }
        }
        // We should not arrive at a placeholder.
        return Err(elog_error(format!(
            "unexpected SPGiST tuple state: {tupstate}"
        )))
        .map(|()| SpGistErrorOffsetNumber);
    }

    // Assert(ItemPointerIsValid(&leafTuple->heapPtr)).
    let leaf_heap_ptr = read_leaf_heap_ptr(&leaf_tuple);
    debug_assert!(ItemPointerIsValid(Some(&leaf_heap_ptr)));

    spgLeafTest(
        mcx,
        so,
        parent_value,
        parent_traversal_value,
        parent_level,
        &leaf_tuple,
        leaf_heap_ptr,
        isnull,
        reported_some,
        store_res,
        bitmap,
    )?;

    Ok(lt_get_next_offset(&leaf_tuple))
}

// ===========================================================================
// spgWalk (spgscan.c:816)
// ===========================================================================

/// `spgWalk(index, so, scanWholeIndex, storeRes)` (spgscan.c:816) — walk the
/// tree and report all tuples passing the scan quals to the `storeRes`
/// subroutine. With `scan_whole_index = false` it stops at the next page
/// boundary once it has reported at least one tuple.
fn spgWalk<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    scan_whole_index: bool,
    store_res: StoreResKind,
    mut bitmap: Option<&mut AmTIDBitmap>,
) -> PgResult<()> {
    let mut buffer: Buffer = InvalidBuffer;
    let mut reported_some = false;

    while scan_whole_index || !reported_some {
        let mut item = match spgGetNextQueueItem(so) {
            Some(it) => it,
            None => break, // No more items in queue -> done.
        };

        // 'redirect' loop target: re-enter with the (possibly updated) item.
        'redirect: loop {
            // Check for interrupts, just in case of infinite loop.
            check_for_interrupts()?;

            if item.isLeaf {
                // We store heap items in the queue only in case of ordered
                // search.
                debug_assert!(so.numberOfNonNullOrderBys > 0);
                let dists = item.distances.clone();
                let leaf_bytes = item.leafTuple.clone();
                store_res_dispatch(
                    mcx,
                    so,
                    store_res,
                    bitmap.as_deref_mut(),
                    &item.heapPtr,
                    item.value.clone(),
                    item.isNull,
                    leaf_bytes.as_deref(),
                    item.recheck,
                    item.recheckDistances,
                    &dists,
                )?;
                reported_some = true;
            } else {
                let blkno = ItemPointerGetBlockNumber(&item.heapPtr);
                let offset = ItemPointerGetOffsetNumber(&item.heapPtr);

                if buffer == InvalidBuffer {
                    buffer = bufmgr::read_buffer::call(index, blkno)?;
                    bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;
                } else if blkno != bufmgr::buffer_get_block_number::call(buffer) {
                    bufmgr::unlock_release_buffer::call(buffer);
                    buffer = bufmgr::read_buffer::call(index, blkno)?;
                    bufmgr::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;
                }
                // else new pointer points to the same page, no work needed.

                let page = bufmgr::buffer_get_page::call(mcx, buffer)?;
                let isnull = SpGistPageStoresNulls(&page);

                if SpGistPageIsLeaf(&page) {
                    // Page is a leaf - all its tuples are heap items.
                    let max = PageGetMaxOffsetNumber(&PageRef::new(&page)?);

                    if SpGistBlockIsRoot(blkno) {
                        // When root is a leaf, examine all its tuples.
                        let mut off = FirstOffsetNumber;
                        while off <= max {
                            let mut hp = item.heapPtr;
                            spgTestLeafTuple(
                                mcx,
                                so,
                                &mut hp,
                                &item.value,
                                &item.traversalValue,
                                item.level,
                                &page,
                                off,
                                isnull,
                                true,
                                &mut reported_some,
                                store_res,
                                bitmap.as_deref_mut(),
                            )?;
                            off += 1;
                        }
                    } else {
                        // Normal case: just examine the chain we arrived at.
                        let mut off = offset;
                        let mut do_redirect = false;
                        while off != InvalidOffsetNumber {
                            debug_assert!(off >= FirstOffsetNumber && off <= max);
                            off = spgTestLeafTuple(
                                mcx,
                                so,
                                &mut item.heapPtr,
                                &item.value,
                                &item.traversalValue,
                                item.level,
                                &page,
                                off,
                                isnull,
                                false,
                                &mut reported_some,
                                store_res,
                                bitmap.as_deref_mut(),
                            )?;
                            if off == SpGistRedirectOffsetNumber {
                                do_redirect = true;
                                break;
                            }
                        }
                        if do_redirect {
                            // goto redirect.
                            continue 'redirect;
                        }
                    }
                } else {
                    // page is inner.
                    let inner_tuple: Vec<u8> = {
                        let pr = PageRef::new(&page)?;
                        let iid = PageGetItemId(&pr, offset)?;
                        PageGetItem(&pr, &iid)?.to_vec()
                    };

                    let tupstate = lt_tupstate(&inner_tuple);
                    if tupstate != SPGIST_LIVE {
                        if tupstate == SPGIST_REDIRECT {
                            // transfer attention to redirect point.
                            let ptr = read_dead_pointer(&inner_tuple);
                            item.heapPtr = ptr;
                            debug_assert!(
                                ItemPointerGetBlockNumber(&item.heapPtr)
                                    != SPGIST_METAPAGE_BLKNO
                            );
                            // goto redirect.
                            continue 'redirect;
                        }
                        return Err(elog_error(format!(
                            "unexpected SPGiST tuple state: {tupstate}"
                        )));
                    }

                    spgInnerTest(
                        mcx,
                        so,
                        &item.value,
                        &item.traversalValue,
                        item.level,
                        &inner_tuple,
                        isnull,
                    )?;
                }
            }

            // done with this scan item (drop frees its owned payload); clear
            // temp context before proceeding (a no-op in the owned model).
            break 'redirect;
        }
    }

    if buffer != InvalidBuffer {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    Ok(())
}

// ===========================================================================
// storeBitmap / spggetbitmap (spgscan.c:930 / spgscan.c:941)
// ===========================================================================

/// Which `storeRes_func` the current `spgWalk` is driving (C's function
/// pointer): the getbitmap or the gettuple result collector.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StoreResKind {
    /// `storeBitmap` (spgscan.c:930).
    Bitmap,
    /// `storeGettuple` (spgscan.c:958).
    Gettuple,
}

/// Dispatch to the active `storeRes` subroutine (C's `storeRes(...)` call
/// through the function pointer).
fn store_res_dispatch<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    kind: StoreResKind,
    bitmap: Option<&mut AmTIDBitmap>,
    heap_ptr: &ItemPointerData,
    leaf_value: Datum<'mcx>,
    isnull: bool,
    leaf_tuple_bytes: Option<&[u8]>,
    recheck: bool,
    recheck_distances: bool,
    non_null_distances: &[f64],
) -> PgResult<()> {
    match kind {
        StoreResKind::Bitmap => store_bitmap(
            so,
            bitmap.expect("storeBitmap with no bitmap"),
            heap_ptr,
            recheck,
            recheck_distances,
            non_null_distances,
        ),
        StoreResKind::Gettuple => store_gettuple(
            mcx,
            so,
            heap_ptr,
            leaf_value,
            isnull,
            leaf_tuple_bytes,
            recheck,
            recheck_distances,
            non_null_distances,
        ),
    }
}

/// `storeBitmap(so, heapPtr, ..., recheck, recheckDistances, distances)`
/// (spgscan.c:930) — the getbitmap `storeRes`: add the heap TID to the bitmap.
fn store_bitmap<'mcx>(
    so: &mut SpGistScanOpaqueData<'mcx>,
    tbm: &mut AmTIDBitmap,
    heap_ptr: &ItemPointerData,
    _recheck: bool,
    recheck_distances: bool,
    distances: &[f64],
) -> PgResult<()> {
    debug_assert!(!recheck_distances && distances.is_empty());
    // tbm_add_tuples(so->tbm, heapPtr, 1, recheck).
    let tbm_concrete = tbm
        .payload
        .as_mut()
        .and_then(|p| p.downcast_mut::<types_tidbitmap::TIDBitmap>())
        .expect("amgetbitmap TIDBitmap payload is not a types_tidbitmap::TIDBitmap");
    tbm_add_tuples::call(tbm_concrete, &[*heap_ptr], _recheck)?;
    so.ntids += 1;
    Ok(())
}

/// `spggetbitmap(scan, tbm)` (spgscan.c:941) — the `amgetbitmap` driver: walk
/// the whole index, adding every matching heap TID to `tbm`. Returns the TID
/// count.
pub fn spggetbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut AmTIDBitmap,
) -> PgResult<i64> {
    let index = scan.index_relation.alias();

    // Copy want_itup to *so so we don't need to pass it around separately.
    so(scan).want_itup = false;
    so(scan).ntids = 0;

    // so->tbm = tbm; spgWalk(scan->indexRelation, so, true, storeBitmap).
    spgWalk(
        mcx,
        &index,
        so(scan),
        true,
        StoreResKind::Bitmap,
        Some(tbm),
    )?;

    Ok(so(scan).ntids)
}

// ===========================================================================
// storeGettuple / spggettuple (spgscan.c:958 / spgscan.c:1025)
// ===========================================================================

/// `storeGettuple(so, heapPtr, leafValue, isnull, leafTuple, recheck,
/// recheckDistances, nonNullDistances)` (spgscan.c:958) — the gettuple
/// `storeRes`: stash the heap TID, recheck flags, per-result distances, and
/// (for an index-only scan) the reconstructed tuple into the per-page result
/// arrays.
fn store_gettuple<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut SpGistScanOpaqueData<'mcx>,
    heap_ptr: &ItemPointerData,
    leaf_value: Datum<'mcx>,
    isnull: bool,
    leaf_tuple_bytes: Option<&[u8]>,
    recheck: bool,
    recheck_distances: bool,
    non_null_distances: &[f64],
) -> PgResult<()> {
    debug_assert!((so.nPtrs as usize) < MaxIndexTuplesPerPage);
    so.heapPtrs.push(*heap_ptr);
    so.recheck.push(recheck);
    so.recheckDistances.push(recheck_distances);

    if so.numberOfOrderBys > 0 {
        if isnull || so.numberOfNonNullOrderBys <= 0 {
            so.distances.push(None);
        } else {
            let mut distances: Vec<IndexOrderByDistance> =
                alloc::vec![IndexOrderByDistance::default(); so.numberOfOrderBys as usize];
            for i in 0..so.numberOfOrderBys as usize {
                let offset = so.nonNullOrderByOffsets[i];
                if offset >= 0 {
                    // Copy non-NULL distance value.
                    distances[i].value = non_null_distances[offset as usize];
                    distances[i].isnull = false;
                } else {
                    // Set distance's NULL flag.
                    distances[i].value = 0.0;
                    distances[i].isnull = true;
                }
            }
            so.distances.push(Some(distances));
        }
    }

    if so.want_itup {
        // Reconstruct index data. We copy the datum out of the temp context and
        // create the tuple here.
        let natts = so.reconTupDesc.natts as usize;
        let mut leaf_datums: Vec<Datum<'mcx>> = alloc::vec![Datum::null(); natts];
        let mut leaf_isnulls: Vec<bool> = alloc::vec![false; natts];

        // We only need to deform the old tuple if it has INCLUDE attributes.
        if leaf_tupdesc_natts(&so.state) > 1 {
            let bytes = leaf_tuple_bytes
                .expect("storeGettuple: want_itup with INCLUDE columns but no leaf tuple");
            // spgDeformLeafTuple(leafTuple, leafTupDesc, leafDatums, leafIsnulls,
            // isnull).
            let deformed =
                spgDeformLeafTuple(mcx, bytes, leaf_tupdesc_ref(&so.state), isnull)?;
            for (i, (d, n)) in deformed.iter().enumerate() {
                leaf_datums[i] = d.clone();
                leaf_isnulls[i] = *n;
            }
        }

        leaf_datums[spgKeyColumn as usize] = leaf_value;
        leaf_isnulls[spgKeyColumn as usize] = isnull;

        let formed = backend_access_common_heaptuple::heap_form_tuple(
            mcx,
            &so.reconTupDesc,
            &leaf_datums,
            &leaf_isnulls,
        )
        .map_err(|e| elog_error(format!("heap_form_tuple failed: {e:?}")))?;
        so.reconTups.push(Some(formed));
    }
    so.nPtrs += 1;
    Ok(())
}

/// `spggettuple(scan, dir)` (spgscan.c:1025) — the `amgettuple` driver. SP-GiST
/// supports only forward scans; returns one already-collected result per call,
/// re-running `spgWalk` (a page at a time) whenever the buffer is exhausted.
pub fn spggettuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    if dir != ScanDirection::ForwardScanDirection {
        return Err(elog_error(
            "SP-GiST only supports forward scan direction".into(),
        ));
    }

    // Copy want_itup to *so so we don't need to pass it around separately.
    let want_itup = scan.xs_want_itup;
    so(scan).want_itup = want_itup;

    loop {
        let cur = so(scan);
        if cur.iPtr < cur.nPtrs {
            // continuing to return reported tuples.
            let i = cur.iPtr as usize;
            let heaptid = cur.heapPtrs[i];
            let recheck = cur.recheck[i];
            // scan->xs_hitup = so->reconTups[so->iPtr]: hand out the recon tuple
            // (the full data-bearing FormedTuple kept in reconTups).
            let hitup = if want_itup {
                cur.reconTups[i].clone()
            } else {
                None
            };
            let number_of_order_bys = cur.numberOfOrderBys;
            let dist = if number_of_order_bys > 0 {
                cur.distances[i].clone()
            } else {
                None
            };
            let recheck_distances = cur.recheckDistances[i];
            let order_by_types = cur.orderByTypes.clone();
            cur.iPtr += 1;

            scan.xs_heaptid = heaptid;
            scan.xs_recheck = recheck;
            scan.xs_hitup = hitup;

            if number_of_order_bys > 0 {
                index_store_float8_orderby_distances(
                    scan,
                    &order_by_types,
                    dist.as_deref(),
                    recheck_distances,
                )?;
            }
            return Ok(true);
        }

        // C pfrees so->distances[i] / so->reconTups[i] here; the owned arrays
        // drop their elements when cleared.
        let cur = so(scan);
        cur.distances.clear();
        cur.reconTups.clear();
        cur.heapPtrs.clear();
        cur.recheck.clear();
        cur.recheckDistances.clear();
        cur.iPtr = 0;
        cur.nPtrs = 0;

        let index = scan.index_relation.alias();
        spgWalk(mcx, &index, so(scan), false, StoreResKind::Gettuple, None)?;

        if so(scan).nPtrs == 0 {
            break; // must have completed scan.
        }
    }

    Ok(false)
}

// ===========================================================================
// spgcanreturn (spgscan.c:1082)
// ===========================================================================

/// `spgcanreturn(index, attno)` (spgscan.c:1082) — the `amcanreturn` predicate:
/// INCLUDE attributes (`attno > 1`) are always fetchable; the key column is
/// fetchable iff the opclass config function says `canReturnData`.
pub fn spgcanreturn<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>, attno: i32) -> PgResult<bool> {
    // INCLUDE attributes can always be fetched for index-only scans.
    if attno > 1 {
        return Ok(true);
    }
    // We can do it if the opclass config function says so.
    let cache: SpGistCache = spgGetCache(mcx, index)?;
    Ok(cache.config.canReturnData)
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `RelationGetIndexScan(indexRelation, nkeys, norderbys)` (genam.c) — allocate
/// and zero-init the generic `IndexScanDescData` the AM extends via `opaque`.
/// Mirrors the brin/nbtree adapters. `xs_orderbynulls` is set true (C's
/// `memset(scan->xs_orderbynulls, true, ...)`).
fn relation_get_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let key_data: Vec<ScanKeyData<'mcx>> = if nkeys > 0 {
        alloc::vec![ScanKeyData::empty(); nkeys as usize]
    } else {
        Vec::new()
    };
    let order_by_data: Vec<ScanKeyData<'mcx>> = if norderbys > 0 {
        alloc::vec![ScanKeyData::empty(); norderbys as usize]
    } else {
        Vec::new()
    };
    let xs_orderbyvals = alloc::vec![Datum::null(); norderbys as usize];
    let xs_orderbynulls = alloc::vec![true; norderbys as usize];

    let _ = mcx;
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

/// Erase a [`SpGistScanOpaqueData`] into the A0 AM-opaque carrier
/// (`PgBox<dyn AmOpaque + 'mcx>`) for storage in `IndexScanDescData.opaque`.
fn erase_opaque<'mcx>(
    mcx: Mcx<'mcx>,
    opaque: SpGistScanOpaqueData<'mcx>,
) -> PgResult<PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: PgBox<'mcx, SpGistScanOpaqueData<'mcx>> = mcx::alloc_in(mcx, opaque)?;
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable (the A0 erase pattern).
    Ok(unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

/// `((SpGistLeafTuple) tuple)->heapPtr` — the heap TID of a leaf tuple, the
/// `ItemPointerData` at byte offset 6 of the on-page leaf-tuple image.
fn read_leaf_heap_ptr(tup: &[u8]) -> ItemPointerData {
    node_t_tid(&tup[6..]) // ItemPointerData @6 (same 6-byte ItemPointer codec).
}

/// `((SpGistDeadTuple) tuple)->pointer` — the redirect target of a dead/redirect
/// tuple, the `ItemPointerData` at byte offset 6 of the on-page dead-tuple image
/// (spgist_private.h `SpGistDeadTupleData.pointer`).
fn read_dead_pointer(tup: &[u8]) -> ItemPointerData {
    node_t_tid(&tup[6..])
}

/// `index_store_float8_orderby_distances(scan, orderByTypes, distances,
/// recheckOrderBy)` (genam.c) — convert the AM distance function's (possibly
/// inexact) results to the ORDER BY types and save them into the scan's
/// `xs_orderbyvals` / `xs_orderbynulls` for a possible recheck. genam.c is below
/// the index AM, so this small conversion utility is rendered inline (matching
/// the identical logic in `backend-access-index-indexam`); `distances` is `None`
/// for the C `NULL` (only valid when `!recheckOrderBy`).
fn index_store_float8_orderby_distances<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    order_by_types: &[Oid],
    distances: Option<&[IndexOrderByDistance]>,
    recheck_orderby: bool,
) -> PgResult<()> {
    // Assert(distances || !recheckOrderBy).
    debug_assert!(distances.is_some() || !recheck_orderby);

    scan.xs_recheckorderby = recheck_orderby;

    for i in 0..scan.number_of_order_bys as usize {
        let typ = order_by_types[i];
        let d = distances.map(|ds| ds[i]);
        if typ == FLOAT8OID {
            // USE_FLOAT8_BYVAL on all supported 64-bit platforms; the C
            // `#ifndef USE_FLOAT8_BYVAL` pfree branch is compiled out.
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[i] = Datum::from_f64(d.value);
                    scan.xs_orderbynulls[i] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[i] = Datum::null();
            scan.xs_orderbynulls[i] = true;
        } else if typ == FLOAT4OID {
            // convert distance function's result to ORDER BY type.
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[i] = Datum::from_f32(d.value as f32);
                    scan.xs_orderbynulls[i] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[i] = Datum::null();
            scan.xs_orderbynulls[i] = true;
        } else {
            // We don't know how to convert the float8 bound to this type. The
            // executor won't need these values unless there are lossy results,
            // so only insist on converting if the recheck flag is set.
            if scan.xs_recheckorderby {
                return Err(elog_error(
                    "ORDER BY operator must return float8 or float4 if the distance function is lossy".into(),
                ));
            }
            scan.xs_orderbyvals[i] = Datum::null();
            scan.xs_orderbynulls[i] = true;
        }
    }
    Ok(())
}

/// `so->state.leafTupDesc->natts` — the leaf descriptor attribute count (the
/// owned `leafTupDesc` is `Option<PgBox<TupleDescData>>`; it is Some after
/// `initSpGistState`).
fn leaf_tupdesc_natts(state: &SpGistState<'_>) -> i32 {
    state
        .leafTupDesc
        .as_ref()
        .expect("SpGistState.leafTupDesc is NULL")
        .natts
}

/// `so->state.leafTupDesc` — the leaf descriptor as a `&TupleDescData`.
fn leaf_tupdesc_ref<'a, 'mcx>(state: &'a SpGistState<'mcx>) -> &'a TupleDescData<'mcx> {
    state
        .leafTupDesc
        .as_ref()
        .expect("SpGistState.leafTupDesc is NULL")
}

/// `CHECK_FOR_INTERRUPTS()` — same behaviour-preserving no-op the SP-GiST
/// insert/pageops layers use.
#[inline]
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// `elog(ERROR, ...)` surface for this file's hard errors.
fn elog_error(msg: String) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}
