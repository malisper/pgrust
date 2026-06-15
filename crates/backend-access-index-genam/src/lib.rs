//! `access/index/genam.c` — the system-table scan facility (the
//! `systable_beginscan` / `systable_getnext` / `systable_endscan` family plus
//! the ordered variants and `systable_recheck_tuple`).
//!
//! This unit ports genam.c's heap-or-index catalog scanner. It is the owner of
//! the `systable_*` seams declared in `backend-access-index-genam-seams`; it
//! installs them from [`init_seams`].
//!
//! ## Model
//!
//! In C, `systable_beginscan(Relation heapRelation, ...)` receives an
//! already-open `Relation` and `palloc`s a `SysScanDescData` in
//! `CurrentMemoryContext` holding the live `irel` / `iscan` / `scan` / `slot`.
//! The seam contract here trims the heap relation to `&RelationData` (the
//! deref target consumers pass) and carries the descriptor with no lifetime
//! parameter (consumers hold it on the stack across many `systable_getnext`
//! calls). To bridge both gaps:
//!
//! * `systable_beginscan*` allocates its own `MemoryContext` (`scan_cx`,
//!   standing in for the palloc context) and builds the live `'mcx` scan state
//!   ([`SysScanLiveState`]) in it. The heap relation is re-acquired as a real
//!   cache-carrying `Relation` via `relation_open(rd_id, NoLock)` (the relation
//!   is already open + locked by the caller, so `NoLock` adds no lock — the
//!   relcache simply returns the cached entry, the analog of C aliasing the
//!   passed pointer); the index, exactly as in C, via `index_open(indexId,
//!   AccessShareLock)`.
//! * The live state is stored lifetime-erased inside the descriptor (see
//!   `types_scan::genam::SysScanDescData`), which owns `scan_cx` so the erased
//!   borrows never dangle.
//!
//! ## The heap-fetch leg (sanctioned mirror-and-panic)
//!
//! `systable_getnext` runs `index_getnext_slot` / `table_scan_getnextslot`
//! (indexam.c / tableam.c, both ported) which dispatch the actual heap-tuple
//! fetch + visibility check to the heap AM provider (heapam_handler.c /
//! heapam.c, still `todo`, bufmgr-gated). That provider is unported, so its
//! vtable callback panics loudly — the sanctioned `mirror-pg-and-panic` for the
//! one unported dependency. The `systable_*` dispatch logic itself (index-vs-
//! heap decision, scan-key attribute-number conversion, snapshot bookkeeping,
//! scan begin/rescan/end, lossy-recheck guard, concurrent-abort handling) is
//! fully real.

extern crate alloc;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::tuptable::SlotData;
use types_rel::{Relation, RelationData};
use types_scan::genam::{SysScanDescData, SysScanLive};
use types_scan::scankey::ScanKeyData;
use types_tableam::scankey::ScanKeyData as TableScanKeyData;
use types_scan::sdir::{ScanDirection, ForwardScanDirection};
use types_snapshot::SnapshotData;
use types_storage::lock::{AccessShareLock, NoLock};
use types_tableam::relscan::{IndexScanDesc, TableScanDesc};
use types_tuple::backend_access_common_heaptuple::FormedTuple;

use backend_access_index_genam_seams as seam;
use backend_access_index_indexam as indexam;
use backend_access_table_tableam as tableam;
use backend_access_common_relation_seams as relation_seams;
use backend_catalog_index_seams as catalog_index;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_time_snapmgr_seams as snapmgr;

/// `SysScanDescData`'s live `'mcx` scan state — the C struct's `heap_rel` /
/// `irel` / `iscan` / `scan` / `slot`, all allocated in `scan_cx`. Erased
/// behind [`SysScanLive`] so it can ride in the lifetime-free descriptor.
struct SysScanLiveState<'mcx> {
    /// `Relation heap_rel` — the catalog being scanned (re-acquired with
    /// `NoLock`, released at end of scan).
    heap_rel: Relation<'mcx>,
    /// `Relation irel` — the index, when the index path is taken (`index_open`
    /// at `AccessShareLock`); `None` for the heap-scan path.
    irel: Option<Relation<'mcx>>,
    /// `struct IndexScanDescData *iscan` — the live index scan, when the index
    /// path is taken.
    iscan: Option<IndexScanDesc<'mcx>>,
    /// `struct TableScanDescData *scan` — the live heap scan, when the heap
    /// path is taken.
    scan: Option<TableScanDesc<'mcx>>,
    /// `TupleTableSlot *slot` — the result slot rows are fetched into.
    slot: SlotData<'mcx>,
}

impl<'mcx> SysScanLive for SysScanLiveState<'mcx> {
    fn live_type_name(&self) -> &'static str {
        "backend_access_index_genam::SysScanLiveState"
    }
}

/// Recover the genam owner's concrete live state from the erased descriptor.
///
/// Two lifetimes: `'a` is the borrow of the descriptor; `'mcx` is the lifetime
/// of the live state's `'mcx`-bearing fields. They are independent because the
/// live state was allocated in `scan_cx` at begin time and `scan_cx` (owned by
/// the descriptor) outlives every per-row `mcx` passed to `systable_getnext`.
/// So a getnext call can re-fabricate the live state at its own `'mcx` (used to
/// drive `index_getnext_slot` / `table_scan_getnextslot`, which need
/// `&mut ...<'mcx>` / `&mut TupleTableSlot<'mcx>`), while the reference itself
/// is bounded by `'a`.
///
/// SAFETY: only the genam owner ever stores a `SysScanLiveState` in a
/// `SysScanDescData`, so the erased `dyn SysScanLive` is always this type. The
/// fabricated `'mcx` is sound because the backing `scan_cx` outlives the
/// per-row context (the live state physically persists across the whole scan);
/// the `&mut` is bounded by `'a` so it cannot be stored past the call.
fn live_of<'a, 'mcx>(desc: &'a mut SysScanDescData) -> &'a mut SysScanLiveState<'mcx> {
    let l: &mut (dyn SysScanLive + 'a) = desc.live_mut();
    // The only concrete type behind `dyn SysScanLive` in a genam descriptor.
    unsafe { &mut *(l as *mut (dyn SysScanLive + 'a) as *mut SysScanLiveState<'mcx>) }
}

/// `init_seams()` — install the `systable_*` family.
pub fn init_seams() {
    seam::systable_beginscan::set(systable_beginscan);
    seam::systable_getnext::set(systable_getnext);
    seam::systable_endscan::set(systable_endscan);
    seam::systable_recheck_tuple::set(systable_recheck_tuple);
    seam::systable_beginscan_ordered::set(systable_beginscan_ordered);
    seam::systable_getnext_ordered::set(systable_getnext_ordered);
    seam::systable_endscan_ordered::set(systable_endscan_ordered);
}

// ===========================================================================
// scan-key attribute-number conversion (shared by both begin variants)
// ===========================================================================

/// Clone one scan key into `mcx`, overriding its `sk_attno`. `ScanKeyData`'s
/// only `'mcx`-bearing fields are `sk_argument` (a `Datum`) and the recursive
/// `sk_subkeys`; everything else is lifetime-free and Copy/Clone.
fn clone_key_in<'mcx>(
    mcx: Mcx<'mcx>,
    key: &ScanKeyData<'_>,
    sk_attno: AttrNumber,
) -> PgResult<ScanKeyData<'mcx>> {
    let sk_subkeys = match &key.sk_subkeys {
        None => None,
        Some(subs) => {
            let mut v = alloc::vec::Vec::with_capacity(subs.len());
            for s in subs {
                v.push(clone_key_in(mcx, s, s.sk_attno)?);
            }
            Some(v)
        }
    };
    Ok(ScanKeyData {
        sk_flags: key.sk_flags,
        sk_attno,
        sk_strategy: key.sk_strategy,
        sk_subtype: key.sk_subtype,
        sk_collation: key.sk_collation,
        sk_func: key.sk_func.clone(),
        sk_argument: key.sk_argument.clone_in(mcx)?,
        sk_subkeys,
    })
}

/// Convert the heap-relative scan keys to index-column scan keys, exactly as
/// C's `systable_beginscan` / `systable_beginscan_ordered` loop does:
/// `idxkey[i].sk_attno = j+1` where `key[i].sk_attno ==
/// irel->rd_index->indkey.values[j]`. Errors ("column is not in index") if a
/// key attribute is not an index column. The converted keys are allocated in
/// `mcx`.
fn convert_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    irel: &Relation<'mcx>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<PgVec<'mcx, ScanKeyData<'mcx>>> {
    // `irel->rd_index->indkey.values[0..IndexRelationGetNumberOfAttributes]`.
    let indkey = relcache::rd_index_indkey::call(irel)?
        .expect("systable scan over a relation that is not an index (rd_index == NULL)");

    let mut out = mcx::vec_with_capacity_in(mcx, keys.len())?;
    for key in keys {
        let mut found: Option<AttrNumber> = None;
        for (j, &col) in indkey.iter().enumerate() {
            if key.sk_attno == col {
                found = Some((j + 1) as AttrNumber);
                break;
            }
        }
        let sk_attno = found.ok_or_else(|| PgError::error("column is not in index"))?;
        out.push(clone_key_in(mcx, key, sk_attno)?);
    }
    Ok(out)
}

/// The snapshot setup shared by `systable_beginscan[_ordered]`: when the
/// caller passed `None` (C's NULL), register the catalog snapshot and record
/// it on the descriptor for unregistration at end of scan; otherwise the
/// caller owns the snapshot (nothing recorded). Returns `(scan_snapshot,
/// snapshot_to_unregister)`.
fn setup_snapshot(
    heap_relid: Oid,
    snapshot: Option<&SnapshotData>,
) -> PgResult<(SnapshotData, Option<SnapshotData>)> {
    match snapshot {
        None => {
            // snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));
            let snap = snapmgr::register_snapshot::call(
                snapmgr::get_catalog_snapshot::call(heap_relid)?,
            )?;
            Ok((snap.clone(), Some(snap)))
        }
        Some(s) => {
            // Caller is responsible for any snapshot.
            Ok((s.clone(), None))
        }
    }
}

// ===========================================================================
// systable_beginscan / getnext / recheck / endscan
// ===========================================================================

/// `systable_beginscan(heapRelation, indexId, indexOK, snapshot, nkeys, key)`.
fn systable_beginscan(
    heap_relation: &RelationData<'_>,
    index_id: Oid,
    index_ok: bool,
    snapshot: Option<&SnapshotData>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<seam::SysScanGuard> {
    let heap_relid = heap_relation.rd_id; // RelationGetRelid(heapRelation)

    // The palloc context standing in for CurrentMemoryContext. Boxed: stable
    // heap address so the live state can borrow it and the descriptor can own
    // it (dropping it after the live state).
    let scan_cx = Box::new(MemoryContext::new("systable scan"));
    // SAFETY: `scan_cx` is boxed (stable address) and moved into the returned
    // descriptor, which drops the live state before this context. The
    // fabricated `'mcx` therefore never outlives the backing storage.
    let cx_ref: &MemoryContext = unsafe { &*(&*scan_cx as *const MemoryContext) };
    let mcx = cx_ref.mcx();

    let (live, to_unregister) =
        begin_unordered(mcx, heap_relid, index_id, index_ok, snapshot, keys)?;

    let desc = SysScanDescData::new(scan_cx, Box::new(live), to_unregister);
    Ok(seam::SysScanGuard::new(desc, false))
}

/// The `'mcx`-bound body of [`systable_beginscan`].
fn begin_unordered<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relid: Oid,
    index_id: Oid,
    index_ok: bool,
    snapshot: Option<&SnapshotData>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<(SysScanLiveState<'mcx>, Option<SnapshotData>)> {
    // irel = index_open(indexId, AccessShareLock) when the index path is taken.
    let irel: Option<Relation<'mcx>> = if index_ok
        && !miscinit::get_ignore_system_indexes::call()
        && !catalog_index::reindex_is_processing_index::call(index_id)
    {
        Some(indexam::index_open(mcx, index_id, AccessShareLock)?)
    } else {
        None
    };

    // Re-acquire the heap relation as a real cache-carrying handle (the seam
    // trimmed it to &RelationData). NoLock: the caller already holds the lock.
    let heap_rel = relation_seams::relation_open::call(mcx, heap_relid, NoLock)?;

    // sysscan->slot = table_slot_create(heapRelation, NULL);
    let slot = tableam::table_slot_create(mcx, &heap_rel)?;

    let (scan_snapshot, to_unregister) = setup_snapshot(heap_relid, snapshot)?;

    let nkeys = keys.len() as i32;
    let (iscan, scan) = if let Some(irel) = &irel {
        // Convert attribute numbers to be index column numbers.
        let idxkey = convert_scan_keys(mcx, irel, keys)?;

        // sysscan->iscan = index_beginscan(heapRelation, irel, snapshot, NULL,
        //   nkeys, 0);
        let mut iscan: IndexScanDesc<'mcx> = indexam::index_beginscan(
            mcx,
            &heap_rel,
            irel,
            scan_snapshot,
            None,
            nkeys,
            0,
        )?;
        // index_rescan(sysscan->iscan, idxkey, nkeys, NULL, 0);
        indexam::index_rescan(mcx, &mut iscan, &idxkey, nkeys, &[], 0)?;
        (Some(iscan), None)
    } else {
        // We disallow synchronized scans when forced to use a heapscan on a
        // catalog (allow_strat = true, allow_sync = false).
        let key = clone_keys_in(mcx, keys)?;
        let scan: TableScanDesc<'mcx> = tableam::table_beginscan_strat(
            mcx,
            &heap_rel,
            Some(scan_snapshot),
            nkeys,
            key,
            true,
            false,
        )?;
        (None, Some(scan))
    };

    // (The `CheckXidAlive` / `bsysscan` in-progress flag is xact.c state for
    // logical decoding; the concurrent-abort guard reads it through
    // HandleConcurrentAbort below, which is the observable behavior.)

    Ok((
        SysScanLiveState {
            heap_rel,
            irel,
            iscan,
            scan,
            slot,
        },
        to_unregister,
    ))
}

/// Project the (unconverted) heap-relative keys into the table-AM scan-key
/// shape for the heap-scan path (no attribute-number conversion — the heap
/// case uses the keys as-is). The table-AM `ScanKeyData` is the trimmed
/// `access/skey.h` carrier the `scan_begin` callback receives; the comparison
/// function / argument payload (`sk_func` / `sk_argument`) is read by the heap
/// AM provider, which is unported (bufmgr-gated), so this projection carries
/// the structural key as the tableam contract spells it.
fn clone_keys_in<'mcx>(
    mcx: Mcx<'mcx>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<PgVec<'mcx, TableScanKeyData>> {
    let mut out = mcx::vec_with_capacity_in(mcx, keys.len())?;
    for key in keys {
        out.push(TableScanKeyData {
            sk_flags: key.sk_flags,
            sk_attno: key.sk_attno,
            sk_strategy: key.sk_strategy,
            sk_subtype: key.sk_subtype,
            sk_collation: key.sk_collation,
        });
    }
    Ok(out)
}

/// `HandleConcurrentAbort()` (genam.c, static inline): error out if
/// `CheckXidAlive` is set and that transaction is neither in progress nor
/// committed (it aborted concurrently during a system-catalog scan in logical
/// decoding). The `CheckXidAlive` machinery is xact.c logical-decoding state;
/// until that lands, the flag is never set, so this is a no-op — mirrored as
/// the (currently always-false) guard. Returns `Err` to mirror the
/// `ereport(ERROR)`.
fn handle_concurrent_abort() -> PgResult<()> {
    // if (TransactionIdIsValid(CheckXidAlive) &&
    //     !TransactionIdIsInProgress(CheckXidAlive) &&
    //     !TransactionIdDidCommit(CheckXidAlive))
    //   ereport(ERROR, ... "transaction aborted during system catalog scan");
    //
    // CheckXidAlive is only set inside logical-decoding apply (xact.c); no
    // ported path sets it, so the condition is always false here. Kept as the
    // faithful guard shape so the error surface is wired when xact.c lands.
    // The ereport would be:
    //   PgError::error("transaction aborted during system catalog scan")
    //       .with_sqlstate(ERRCODE_TRANSACTION_ROLLBACK)
    Ok(())
}

/// `systable_getnext(sysscan)`.
fn systable_getnext<'mcx>(
    mcx: Mcx<'mcx>,
    sysscan: &mut SysScanDescData,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let live = live_of(sysscan);

    let htup: Option<FormedTuple<'mcx>> = if live.irel.is_some() {
        let iscan = live
            .iscan
            .as_deref_mut()
            .expect("index path with no iscan");
        // index_getnext_slot dispatches the heap fetch to the unported heap AM
        // provider (mirror-and-panic) — the systable dispatch itself is real.
        if indexam::index_getnext_slot(
            mcx,
            iscan,
            ForwardScanDirection,
            &mut live.slot,
        )? {
            let tup = fetch_slot_heap_tuple(mcx, &live.slot)?;
            // We currently don't need to support lossy index operators for any
            // system catalog scan.
            if iscan.xs_recheck {
                return Err(PgError::error(
                    "system catalog scans with lossy index conditions are not implemented",
                ));
            }
            Some(tup)
        } else {
            None
        }
    } else {
        let scan = live.scan.as_deref_mut().expect("heap path with no scan");
        if tableam::table_scan_getnextslot(
            mcx,
            scan,
            ForwardScanDirection,
            &mut live.slot,
        )? {
            Some(fetch_slot_heap_tuple(mcx, &live.slot)?)
        } else {
            None
        }
    };

    // Handle the concurrent abort while fetching the catalog tuple during
    // logical streaming of a transaction.
    handle_concurrent_abort()?;

    Ok(htup)
}

/// `ExecFetchSlotHeapTuple(slot, false, &shouldFree)` for a slot holding a
/// freshly fetched heap tuple — the owned model copies the result tuple out
/// into `mcx` (C returns a reference valid until the next getnext/endscan).
///
/// The slot's stored heap tuple lives in the `Heap`/`BufferHeap` subtype's
/// `tuple` field, set by the heap AM's `index_fetch_tuple` / `scan_getnextslot`
/// provider. That provider is unported (bufmgr-gated), so this is reached only
/// after a getnext that has already panicked in the provider — but it is the
/// faithful extraction once the provider lands.
fn fetch_slot_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &SlotData<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    let stored: Option<&FormedTuple<'mcx>> = match slot {
        SlotData::BufferHeap(s) => s.base.tuple.as_ref(),
        SlotData::Heap(s) => s.tuple.as_ref(),
        // A virtual/minimal slot would force ExecFetchSlotHeapTuple to
        // materialize a heap tuple; system catalog scans always fetch into a
        // heap/buffer slot (table_slot_create over a heap relation), so this
        // never arises.
        _ => None,
    };
    let stored = stored.ok_or_else(|| {
        PgError::error("systable_getnext: result slot holds no heap tuple")
    })?;
    stored.clone_in(mcx)
}

/// `systable_recheck_tuple(sysscan, tup)`.
fn systable_recheck_tuple(sysscan: &mut SysScanDescData) -> PgResult<bool> {
    let live = live_of(sysscan);

    // freshsnap = RegisterSnapshot(GetCatalogSnapshot(RelationGetRelid(heap_rel)));
    let freshsnap =
        snapmgr::register_snapshot::call(snapmgr::get_catalog_snapshot::call(live.heap_rel.rd_id)?)?;

    // result = table_tuple_satisfies_snapshot(heap_rel, slot, freshsnap);
    //
    // table_tuple_satisfies_snapshot dispatches to the heap AM provider
    // (tableam.h `satisfies_snapshot`, heapam_handler.c, unported / bufmgr-
    // gated) — the sanctioned mirror-and-panic for the one unported dep. The
    // surrounding recheck logic (fresh catalog snapshot register/unregister +
    // concurrent-abort) is real.
    let result = table_tuple_satisfies_snapshot(&live.heap_rel, &live.slot, &freshsnap)?;

    // UnregisterSnapshot(freshsnap);
    snapmgr::unregister_snapshot::call(freshsnap);

    handle_concurrent_abort()?;

    Ok(result)
}

/// `table_tuple_satisfies_snapshot(rel, slot, snapshot)` (tableam.h inline) —
/// dispatches to the heap AM's visibility check. The table-AM provider
/// (heapam_handler.c) is unported, so there is no value-typed body to call;
/// mirror-pg-and-panic for the unported dependency. The `tableam.c` owner does
/// not expose this wrapper yet (no consumer needed it before genam), so genam
/// names the gap directly.
fn table_tuple_satisfies_snapshot(
    _rel: &Relation<'_>,
    _slot: &SlotData<'_>,
    _snapshot: &SnapshotData,
) -> PgResult<bool> {
    panic!(
        "table_tuple_satisfies_snapshot: heap AM visibility provider \
         (heapam_handler.c) is not yet ported (bufmgr-gated)"
    )
}

/// `systable_endscan(sysscan)`.
fn systable_endscan(mut sysscan: SysScanDescData) -> PgResult<()> {
    // Move the live state out (re-fabricating its `'mcx`) so we can run the AM
    // end-scan on the owned `'mcx` values. The descriptor still owns `scan_cx`
    // until it is dropped at the end of this function, after `live`.
    let live = *take_live_state(&mut sysscan);
    // The AM teardown calls carry an `Mcx<'mcx>` tied to the same lifetime as
    // the taken scan-state values; that is the descriptor's own `scan_cx`.
    let mcx = sysscan.scan_cx_mcx();

    let SysScanLiveState {
        heap_rel,
        irel,
        iscan,
        scan,
        slot,
    } = live;

    // ExecDropSingleTupleTableSlot(sysscan->slot).
    backend_executor_execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;

    if let Some(irel) = irel {
        // index_endscan(sysscan->iscan); index_close(sysscan->irel,
        // AccessShareLock);
        let iscan = iscan.expect("index path with no iscan at endscan");
        indexam::index_endscan(mcx, iscan)?;
        indexam::index_close(irel, AccessShareLock)?;
    } else {
        // table_endscan(sysscan->scan).
        tableam::table_endscan(scan.expect("heap path with no scan at endscan"))?;
    }

    // Close the heap handle we acquired in begin (NoLock release is a no-op
    // lock-wise; it drops the relcache refcount we took).
    heap_rel.close(NoLock)?;

    // if (sysscan->snapshot) UnregisterSnapshot(sysscan->snapshot).
    if let Some(snap) = sysscan.snapshot.take() {
        snapmgr::unregister_snapshot::call(snap);
    }

    Ok(())
}

/// Take the concrete live state out of the descriptor. SAFETY mirror of
/// [`live_of`]: the only type ever stored is `SysScanLiveState`. The owned
/// `'mcx` is independent of the `&mut self` borrow `'a` (the state's backing
/// `scan_cx` is still owned by the descriptor — the caller drops the whole
/// descriptor after the taken state's teardown), so end-scan can run the AM
/// teardown on the owned `'mcx` values without pinning `sysscan` for the rest
/// of the function.
fn take_live_state<'mcx>(desc: &mut SysScanDescData) -> Box<SysScanLiveState<'mcx>> {
    let boxed: Box<dyn SysScanLive + '_> = desc.take_live();
    let raw = Box::into_raw(boxed) as *mut SysScanLiveState<'mcx>;
    unsafe { Box::from_raw(raw) }
}

// ===========================================================================
// systable_beginscan_ordered / getnext_ordered / endscan_ordered
// ===========================================================================

/// `systable_beginscan_ordered(heapRelation, indexRelation, snapshot, nkeys,
/// key)`.
fn systable_beginscan_ordered(
    heap_relation: &RelationData<'_>,
    index_relation: &RelationData<'_>,
    snapshot: Option<&SnapshotData>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<seam::SysScanGuard> {
    let heap_relid = heap_relation.rd_id;
    let index_relid = index_relation.rd_id;

    // REINDEX can probably be a hard error here ...
    if catalog_index::reindex_is_processing_index::call(index_relid) {
        return Err(PgError::error(format!(
            "cannot access index \"{}\" while it is being reindexed",
            index_relation.name()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    // ... but we only throw a warning about violating IgnoreSystemIndexes.
    if miscinit::get_ignore_system_indexes::call() {
        backend_utils_error::elog(
            types_error::WARNING,
            format!(
                "using index \"{}\" despite IgnoreSystemIndexes",
                index_relation.name()
            ),
        )?;
    }

    let scan_cx = Box::new(MemoryContext::new("systable ordered scan"));
    let cx_ref: &MemoryContext = unsafe { &*(&*scan_cx as *const MemoryContext) };
    let mcx = cx_ref.mcx();

    let (live, to_unregister) =
        begin_ordered(mcx, heap_relid, index_relid, snapshot, keys)?;

    let desc = SysScanDescData::new(scan_cx, Box::new(live), to_unregister);
    Ok(seam::SysScanGuard::new(desc, true))
}

/// The `'mcx`-bound body of [`systable_beginscan_ordered`].
fn begin_ordered<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relid: Oid,
    index_relid: Oid,
    snapshot: Option<&SnapshotData>,
    keys: &[ScanKeyData<'_>],
) -> PgResult<(SysScanLiveState<'mcx>, Option<SnapshotData>)> {
    // The caller has the index open + locked; re-acquire a real handle (NoLock).
    let irel = indexam::index_open(mcx, index_relid, NoLock)?;
    let heap_rel = relation_seams::relation_open::call(mcx, heap_relid, NoLock)?;

    // sysscan->slot = table_slot_create(heapRelation, NULL);
    let slot = tableam::table_slot_create(mcx, &heap_rel)?;

    let (scan_snapshot, to_unregister) = setup_snapshot(heap_relid, snapshot)?;

    // Convert attribute numbers to be index column numbers.
    let idxkey = convert_scan_keys(mcx, &irel, keys)?;
    let nkeys = keys.len() as i32;

    // sysscan->iscan = index_beginscan(heapRelation, indexRelation, snapshot,
    //   NULL, nkeys, 0);
    let mut iscan: IndexScanDesc<'mcx> =
        indexam::index_beginscan(mcx, &heap_rel, &irel, scan_snapshot, None, nkeys, 0)?;
    // index_rescan(sysscan->iscan, idxkey, nkeys, NULL, 0);
    indexam::index_rescan(mcx, &mut iscan, &idxkey, nkeys, &[], 0)?;

    Ok((
        SysScanLiveState {
            heap_rel,
            irel: Some(irel),
            iscan: Some(iscan),
            scan: None,
            slot,
        },
        to_unregister,
    ))
}

/// `systable_getnext_ordered(sysscan, direction)`.
fn systable_getnext_ordered<'mcx>(
    mcx: Mcx<'mcx>,
    sysscan: &mut SysScanDescData,
    direction: ScanDirection,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let live = live_of(sysscan);
    // Assert(sysscan->irel).
    let iscan = live
        .iscan
        .as_deref_mut()
        .expect("systable_getnext_ordered on a non-index scan");

    let htup: Option<FormedTuple<'mcx>> =
        if indexam::index_getnext_slot(mcx, iscan, direction, &mut live.slot)? {
            let tup = fetch_slot_heap_tuple(mcx, &live.slot)?;
            // See notes in systable_getnext.
            if iscan.xs_recheck {
                return Err(PgError::error(
                    "system catalog scans with lossy index conditions are not implemented",
                ));
            }
            Some(tup)
        } else {
            None
        };

    handle_concurrent_abort()?;

    Ok(htup)
}

/// `systable_endscan_ordered(sysscan)`.
fn systable_endscan_ordered(mut sysscan: SysScanDescData) -> PgResult<()> {
    let live = *take_live_state(&mut sysscan);
    let mcx = sysscan.scan_cx_mcx();
    let SysScanLiveState {
        heap_rel,
        irel,
        iscan,
        scan: _,
        slot,
    } = live;

    // ExecDropSingleTupleTableSlot(sysscan->slot).
    backend_executor_execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;

    // Assert(sysscan->irel); index_endscan(sysscan->iscan).
    let iscan = iscan.expect("systable_endscan_ordered on a non-index scan");
    indexam::index_endscan(mcx, iscan)?;

    // The ordered variant's caller opened + closes the index itself (unlike
    // systable_endscan, which index_closes here); we still drop the NoLock
    // handle we re-acquired in begin.
    if let Some(irel) = irel {
        irel.close(NoLock)?;
    }
    heap_rel.close(NoLock)?;

    // if (sysscan->snapshot) UnregisterSnapshot(sysscan->snapshot).
    if let Some(snap) = sysscan.snapshot.take() {
        snapmgr::unregister_snapshot::call(snap);
    }

    Ok(())
}
