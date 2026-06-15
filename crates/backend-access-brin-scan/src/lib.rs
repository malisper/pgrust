//! Owned-tree Rust port of the BRIN index access method's *scan* slice of
//! `src/backend/access/brin/brin.c` (PostgreSQL 18.3):
//!
//!   * `brinhandler` — assembles the unified
//!     [`types_amapi::IndexAmRoutine`] (BRIN is a bitmap-only AM:
//!     `amgettuple = None`, `amgetbitmap = Some(bringetbitmap)`).
//!   * `brinbeginscan` / `brinrescan` / `bringetbitmap` / `brinendscan` — the
//!     scan callbacks.
//!   * `brin_build_desc` / `brin_free_desc` — the per-scan `BrinDesc` builder
//!     (its per-column `OpcInfo` opclass amproc is dispatched through the
//!     `brin-entry` seam, owned by the unported BRIN opclasses).
//!   * `check_null_keys` — the IS [NOT] NULL scan-key pre-check.
//!
//! This crate mirrors the landed btree/hash index tower
//! ([`backend_access_nbtree_nbtree`] / [`backend_access_hash_entry`]): the
//! handler returns the ONE unified `IndexAmRoutine` with leading-`mcx` HRTB
//! scan fn-ptrs; the scan-private [`BrinScan`] working state rides
//! `IndexScanDescData.opaque` via the A0 [`AmOpaque`] carrier (a new
//! [`tags::BRIN_SCAN`] tag, erase = `alloc_in` → `into_raw_with_allocator` →
//! `from_raw_in(ptr as *mut dyn AmOpaque)`).
//!
//! The BRIN engine page/revmap layer (`brinRevmapInitialize` /
//! `brinGetTupleForHeapBlock` / `brinRevmapTerminate`) and tuple codec
//! (`brin_new_memtuple` / `brin_deform_tuple` / `brin_copy_tuple` /
//! `brin_free_desc`'s tuple-side helpers) are this crate's direct deps
//! (`backend-access-brin-{pageops,tuple}`, acyclic — those are consumed by, not
//! consumers of, brin.c). The buffer cache, catalog (`IndexGetRelation`),
//! `table_open`, relcache (`RelationGetNumberOfBlocks`), pgstat, and TID-bitmap
//! cross to their owners through the real repo seam crates. The opclass
//! `OpcInfo` / `Consistent` support procedures (`BRIN_PROCNUM_OPCINFO` /
//! `BRIN_PROCNUM_CONSISTENT`) are dispatched through the `brin-entry` seams
//! (`brin_opcinfo` / `brin_consistent_*`), owned by the unported BRIN opclasses
//! (`brin_minmax` / `brin_inclusion` / `brin_bloom` / `brin_minmax_multi`);
//! until those land a call panics loudly.
//!
//! Insert / build / vacuum (`brininsert` / `brinbuild` / `brinbulkdelete` /
//! `brinvacuumcleanup` / …) are *not* this F2-scan unit's logic; the handler
//! populates the required (non-`Option`) vtable fields with adapters that
//! seam-and-panic into the F3 insert/vacuum unit (reached by name only through
//! the vtable; the serial scan path never invokes them).
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};

use types_amapi::{IndexAmRoutine, T_IndexAmRoutine};
use types_brin::{BrinDesc, BrinMemTuple, BrinOpcInfo, BrinValues};
use types_core::primitive::{BlockNumber, OffsetNumber, Size};
use types_core::InvalidOid;
use types_rel::Relation;
use types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use types_storage::buf::{BufferIsValid, InvalidBuffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use types_storage::lock::AccessShareLock;
use types_tableam::amapi::TIDBitmap as AmTIDBitmap;
use types_tableam::amopaque::{tags, AmOpaque, AmOpaqueType};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_brin_pageops::{
    brinGetTupleForHeapBlock, brinRevmapInitialize, brinRevmapTerminate, read_found_tuple_bytes,
    BrinRevmap,
};
use backend_access_brin_tuple::{brin_copy_tuple, brin_deform_tuple, brin_new_memtuple, BrinTupleImage};
use backend_access_brin_entry_seams as opclass;
use backend_catalog_index_seams::index_get_relation;
use backend_storage_buffer_bufmgr_seams::lock_buffer;
use backend_storage_buffer_bufmgr_seams::release_buffer;
use backend_access_table_table_seams::{relation_close, table_open};
use backend_nodes_core_seams::tbm_add_page;
use backend_utils_activity_pgstat_seams::pgstat_count_index_scan;
use backend_utils_cache_relcache_seams::relation_get_number_of_blocks;
use backend_utils_error::PgResult;

// ===========================================================================
// Constants (access/brin_internal.h, commands/vacuum.h).
// ===========================================================================

/// `BRIN_PROCNUM_OPCINFO` (`brin_internal.h`). The support-procedure number C
/// passes to `index_getprocinfo`; here that dispatch is encapsulated in the
/// `brin_opcinfo` seam, so the constant is documentary.
#[allow(dead_code)]
const BRIN_PROCNUM_OPCINFO: u16 = 1;
/// `BRIN_PROCNUM_CONSISTENT` (`brin_internal.h`). Encapsulated in the
/// `brin_consistent_*` seams; documentary.
#[allow(dead_code)]
const BRIN_PROCNUM_CONSISTENT: u16 = 3;
/// `BRIN_PROCNUM_OPTIONS` (`brin_internal.h`, optional).
const BRIN_PROCNUM_OPTIONS: u16 = 5;
/// `BRIN_LAST_OPTIONAL_PROCNUM` (`brin_internal.h`).
const BRIN_LAST_OPTIONAL_PROCNUM: u16 = 15;
/// `VACUUM_OPTION_PARALLEL_CLEANUP` (`commands/vacuum.h`): `1 << 2`.
const VACUUM_OPTION_PARALLEL_CLEANUP: u8 = 1 << 2;

// ===========================================================================
// BrinOpaque — the scan-private working state (the A0 carrier payload).
// ===========================================================================

/// `struct BrinOpaque` (brin.c:202) — the scan's `void *opaque` payload. Built
/// in [`brinbeginscan`], read by [`bringetbitmap`], torn down in
/// [`brinendscan`].
pub struct BrinScan<'mcx> {
    /// `bo_pagesPerRange`: the index's pages-per-range (read from the metapage).
    pub bo_pagesPerRange: BlockNumber,
    /// `bo_rmAccess`: the reverse range-map access state.
    pub bo_rmAccess: BrinRevmap<'mcx>,
    /// `bo_bdesc`: the BRIN tuple descriptor for this index.
    pub bo_bdesc: BrinDesc<'mcx>,
}

/// `BrinScan` is the concrete type stored in `IndexScanDescData.opaque` (C's
/// `void *opaque`); the A0 carrier downcasts to it in every scan adapter.
impl<'mcx> AmOpaqueType<'mcx> for BrinScan<'mcx> {
    const TAG: types_tableam::amopaque::AmOpaqueTag = tags::BRIN_SCAN;
}

/// Downcast `scan.opaque` to the BRIN scan working state (the A0 tag-checked
/// downcast); panics with a clear message if the descriptor was not built by
/// `brinbeginscan` (a programming error — C would just cast `void *`).
fn brin<'a, 'mcx>(scan: &'a mut IndexScanDescData<'mcx>) -> &'a mut BrinScan<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("BRIN scan descriptor has no opaque (not built by brinbeginscan)")
        .downcast_mut::<BrinScan<'mcx>>()
        .expect("BRIN scan opaque is not a BrinScan")
}

// ===========================================================================
// brinhandler (brin.c:250)
// ===========================================================================

/// `brinhandler()` — return [`IndexAmRoutine`] with the BRIN AM parameters and
/// callbacks. BRIN is bitmap-only: `amgettuple = None`, `amgetbitmap = Some`.
pub fn brinhandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: 0,
        amsupport: BRIN_LAST_OPTIONAL_PROCNUM,
        amoptsprocnum: BRIN_PROCNUM_OPTIONS,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentequality: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: true,
        amstorage: true,
        amclusterable: false,
        ampredlocks: false,
        amcanparallel: false,
        amcanbuildparallel: true,
        amcaninclude: false,
        amusemaintenanceworkmem: false,
        amsummarizing: true,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_CLEANUP,
        amkeytype: InvalidOid,

        // brinvalidate (brin_validate.c) returns a soft-error result and so
        // cannot be the raw `fn(Oid) -> bool` ABI pointer; it is reached by name
        // (matching bthandler's amvalidate = None convention).
        amvalidate: None,
        // BRIN sets these vtable slots to NULL (brin.c:304-305).
        amtranslatestrategy: None,
        amtranslatecmptype: None,

        // Insert / vacuum callbacks — NOT this F2-scan unit's logic. Reached by
        // name only through the vtable; the serial scan path never invokes them.
        // Adapters seam-and-panic into the F3 insert/vacuum unit.
        aminsert: brininsert_am,
        ambulkdelete: brinbulkdelete_am,
        amvacuumcleanup: brinvacuumcleanup_am,
        aminsertcleanup: Some(brininsertcleanup_am),

        // Scan callbacks (F2): the thin adapters translate the unified
        // descriptor <-> BRIN's `BrinScan` working state (downcast from
        // `scan.opaque`).
        ambeginscan: brinbeginscan_am,
        amrescan: brinrescan_am,
        amendscan: brinendscan_am,
        // BRIN has no amcanreturn / amgettuple / mark-pos (brin.c:286,296,299).
        amcanreturn: None,
        amgettuple: None,
        amgetbitmap: Some(bringetbitmap_am),
        ammarkpos: None,
        amrestrpos: None,

        // No parallel index scan (amcanparallel = false; brin.c:301-303).
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}

// ===========================================================================
// AM-vtable adapters (F2): unified IndexScanDescData <-> BrinScan
// ===========================================================================

/// `ambeginscan` adapter — build the unified descriptor with `opaque` holding a
/// freshly-erased [`BrinScan`] (the A0 erase pattern).
fn brinbeginscan_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let opaque = brinbeginscan(mcx, index_relation)?;
    // RelationGetIndexScan(): allocate the generic descriptor; opaque = BrinScan.
    let mut desc = relation_get_index_scan(mcx, index_relation, nkeys, norderbys)?;
    desc.opaque = Some(erase_brinscan(mcx, opaque)?);
    Ok(desc)
}

/// `amrescan` adapter.
fn brinrescan_am<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    _orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    brinrescan(scan, keys)
}

/// `amendscan` adapter.
fn brinendscan_am<'mcx>(_mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    brinendscan(brin(scan))
}

/// `amgetbitmap` adapter.
fn bringetbitmap_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut AmTIDBitmap,
) -> PgResult<i64> {
    // The unified vtable carries the bitmap erased; BRIN's `bringetbitmap` works
    // over the concrete `types_tidbitmap::TIDBitmap`. Downcast it.
    let tbm_concrete = tbm
        .payload
        .as_mut()
        .and_then(|p| p.downcast_mut::<types_tidbitmap::TIDBitmap>())
        .expect("amgetbitmap TIDBitmap payload is not a types_tidbitmap::TIDBitmap");
    bringetbitmap(mcx, scan, tbm_concrete)
}

// --- Insert / vacuum adapters: F3 (insert/vacuum) unit, dispatched by name. --
//
// These vtable slots are owned by the F3 `backend-access-brin-insert-vacuum`
// crate; the adapters dispatch through the `brin-insert-vacuum-seams` it
// installs. The seam crate (not the F3 owner) is this crate's dep, so the
// scan↔insert/vacuum cycle stays broken.

fn brininsert_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
    heap_relation: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    backend_access_brin_insert_vacuum_seams::brininsert::call(
        mcx,
        index_relation,
        values,
        isnull,
        heap_tid,
        heap_relation,
        check_unique,
        index_unchanged,
        index_info,
    )
}

fn brininsertcleanup_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo,
) -> PgResult<()> {
    backend_access_brin_insert_vacuum_seams::brininsertcleanup::call(
        mcx,
        index_relation,
        index_info,
    )
}

fn brinbulkdelete_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    backend_access_brin_insert_vacuum_seams::brinbulkdelete::call(
        mcx,
        info,
        stats,
        callback_state,
    )
}

fn brinvacuumcleanup_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    backend_access_brin_insert_vacuum_seams::brinvacuumcleanup::call(mcx, info, stats)
}

/// `RelationGetIndexScan(indexRelation, nkeys, norderbys)` (genam.c) — allocate
/// and zero-init the generic `IndexScanDescData` the AM extends via `opaque`.
/// Mirrors nbtree's `relation_get_index_scan` adapter.
fn relation_get_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let key_data = if nkeys > 0 {
        let mut v = vec_with_capacity_in(mcx, nkeys as usize)?;
        for _ in 0..nkeys {
            v.push(ScanKeyData::empty());
        }
        v.into_iter().collect()
    } else {
        std::vec::Vec::new()
    };
    let order_by_data = if norderbys > 0 {
        let mut v = vec_with_capacity_in(mcx, norderbys as usize)?;
        for _ in 0..norderbys {
            v.push(ScanKeyData::empty());
        }
        v.into_iter().collect()
    } else {
        std::vec::Vec::new()
    };
    let xs_orderbyvals = std::vec::from_elem(Datum::null(), norderbys as usize);
    let xs_orderbynulls = std::vec![false; norderbys as usize];
    Ok(std::boxed::Box::new(IndexScanDescData {
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

/// Erase a [`BrinScan`] into the A0 AM-opaque carrier
/// (`PgBox<dyn AmOpaque + 'mcx>`) for storage in `IndexScanDescData.opaque`.
fn erase_brinscan<'mcx>(
    mcx: Mcx<'mcx>,
    brinscan: BrinScan<'mcx>,
) -> PgResult<PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: PgBox<'mcx, BrinScan<'mcx>> = mcx::alloc_in(mcx, brinscan)?;
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable (the A0 erase pattern).
    Ok(unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

// ===========================================================================
// brin_build_desc / brin_free_desc (brin.c:1581 / brin.c:1636)
// ===========================================================================

/// `brin_build_desc(rel)` (brin.c:1581): build the [`BrinDesc`] describing the
/// on-disk layout for `rel`, by invoking each indexed column's opclass `OpcInfo`
/// support procedure (dispatched through the `brin-entry` `brin_opcinfo` seam).
pub fn brin_build_desc<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<BrinDesc<'mcx>> {
    // tupdesc = RelationGetDescr(rel);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let natts = tupdesc.natts as usize;

    // Obtain BrinOpcInfo for each indexed column; accumulate bd_totalstored.
    let mut totalstored: i32 = 0;
    let mut bd_info: PgVec<'mcx, PgBox<'mcx, BrinOpcInfo<'mcx>>> = vec_with_capacity_in(mcx, natts)?;
    for keyno in 0..natts {
        let atttypid = tupdesc.attr(keyno).atttypid;
        // index_getprocinfo(rel, keyno+1, BRIN_PROCNUM_OPCINFO) +
        // FunctionCall1(opcInfoFn, atttypid)
        let opcinfo = opclass::brin_opcinfo::call(mcx, rel, keyno, atttypid)?;
        totalstored += opcinfo.oi_nstored as i32;
        bd_info.push(opcinfo);
    }

    Ok(BrinDesc {
        bd_index: rel.alias(),
        bd_tupdesc: tupdesc,
        bd_totalstored: totalstored,
        bd_info,
    })
}

/// `brin_free_desc(bdesc)` (brin.c:1636): free the descriptor. In C the whole
/// `bd_context` memory context is deleted; here the owned [`BrinDesc`] is simply
/// dropped (its `bd_info` / `bd_tupdesc` ride the `mcx` arena).
pub fn brin_free_desc(bdesc: BrinDesc<'_>) {
    drop(bdesc);
}

// ===========================================================================
// brinbeginscan (brin.c:538)
// ===========================================================================

/// `brinbeginscan(r, nkeys, norderbys)` (brin.c:538): initialize the BRIN scan
/// working state. The metapage read here yields the pages-per-range; since that
/// cannot change while we hold a lock on the index it is not recomputed in
/// `brinrescan`.
pub fn brinbeginscan<'mcx>(mcx: Mcx<'mcx>, r: &Relation<'mcx>) -> PgResult<BrinScan<'mcx>> {
    let (bo_rmAccess, bo_pagesPerRange) = brinRevmapInitialize(r.alias())?;
    let bo_bdesc = brin_build_desc(mcx, r)?;
    Ok(BrinScan {
        bo_pagesPerRange,
        bo_rmAccess,
        bo_bdesc,
    })
}

// ===========================================================================
// bringetbitmap (brin.c:566)
// ===========================================================================

/// `bringetbitmap(scan, tbm)` (brin.c:566): execute the BRIN index scan. Walk
/// the revmap a range at a time; for each range fetch the summary tuple and test
/// it against the scan keys (per-attribute, via the opclass `Consistent`
/// support procedure). Unsummarized or matching ranges have all their pages
/// added to `tbm`. Returns `totalpages * 10` (the C heuristic tuple estimate).
pub fn bringetbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut types_tidbitmap::TIDBitmap,
) -> PgResult<i64> {
    let idx_rel = scan.index_relation.alias();
    let number_of_keys = scan.number_of_keys;

    pgstat_count_index_scan::call(idx_rel.rd_id);
    if let Some(instr) = scan.instrument.as_mut() {
        instr.nsearches += 1;
    }

    // Move the scan keys out for per-attribute splitting (we restore nothing —
    // the descriptor keeps its own copy in key_data).
    // We need an owned snapshot of the keys to index into; clone the StdVec.
    let scan_keys: std::vec::Vec<ScanKeyData<'mcx>> = scan.key_data.clone();

    let opaque = brin(scan);
    let pages_per_range = opaque.bo_pagesPerRange;

    // We need the table size to know how long to iterate on the revmap.
    let heap_oid = index_get_relation::call(idx_rel.rd_id, false)?;
    let heap_rel = table_open::call(mcx, heap_oid, AccessShareLock)?;
    let nblocks = relation_get_number_of_blocks::call(&heap_rel)?;
    relation_close::call(heap_oid, AccessShareLock)?;
    drop(heap_rel);

    let natts = opaque.bo_bdesc.natts();

    // Per-attribute scan-key arrays (regular + IS [NOT] NULL), like C's carved
    // chunk. We hold them as owned per-attribute Vecs of cloned ScanKeyData.
    let mut keys: std::vec::Vec<std::vec::Vec<ScanKeyData<'mcx>>> =
        std::vec::from_elem(std::vec::Vec::new(), natts);
    let mut nullkeys: std::vec::Vec<std::vec::Vec<ScanKeyData<'mcx>>> =
        std::vec::from_elem(std::vec::Vec::new(), natts);

    // Preprocess the scan keys - split them into per-attribute arrays.
    for keyno in 0..number_of_keys as usize {
        let key = scan_keys[keyno].clone();
        let keyattno = key.sk_attno as usize;
        if (key.sk_flags & SK_ISNULL) != 0 {
            nullkeys[keyattno - 1].push(key);
        } else {
            keys[keyattno - 1].push(key);
        }
    }

    // Allocate an initial in-memory tuple.
    let mut dtup: BrinMemTuple<'mcx> = brin_new_memtuple(mcx, &opaque.bo_bdesc)?;

    let mut buf = InvalidBuffer;
    let mut totalpages: i64 = 0;
    // brin_copy_tuple destination reuse (C's btup / btupsz).
    let mut btup: Option<BrinTupleImage<'mcx>> = None;
    let mut btupsz: usize = 0;

    // Scan the revmap, range by range. Use u64 for heapBlk: a BlockNumber could
    // wrap for tables with close to 2^32 pages.
    let mut heap_blk: u64 = 0;
    while heap_blk < nblocks as u64 {
        check_for_interrupts()?;

        let mut off: OffsetNumber = 0;
        let mut size: Size = 0;
        let mut gottuple = false;

        let found = brinGetTupleForHeapBlock(
            &mut brin(scan).bo_rmAccess,
            heap_blk as BlockNumber,
            &mut buf,
            &mut off,
            &mut size,
            BUFFER_LOCK_SHARE,
        )?;

        if let Some(found) = found {
            gottuple = true;
            let tup_bytes = read_found_tuple_bytes(mcx, &found)?;
            let (copied, new_sz) =
                brin_copy_tuple(mcx, &tup_bytes, found.size, btup.take(), btupsz)?;
            btup = Some(copied);
            btupsz = new_sz;
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        }

        // For page ranges with no indexed tuple we must return the whole range;
        // otherwise compare it to the scan keys.
        let mut addrange;
        if !gottuple {
            addrange = true;
        } else {
            let bdesc = &brin(scan).bo_bdesc;
            dtup = brin_deform_tuple(mcx, bdesc, &btup.as_ref().unwrap().bytes, Some(dtup))?;
            if dtup.bt_placeholder {
                // Placeholder tuples are always returned.
                addrange = true;
            } else {
                addrange = true;
                let mut attno = 1usize;
                while attno <= natts {
                    // Skip attributes without any scan keys.
                    if keys[attno - 1].is_empty() && nullkeys[attno - 1].is_empty() {
                        attno += 1;
                        continue;
                    }

                    let bval = &dtup.bt_columns[attno - 1];

                    // If the range is empty, nothing matches.
                    if dtup.bt_empty_range {
                        addrange = false;
                        break;
                    }

                    // IS [NOT] NULL pre-check.
                    if brin(scan).bo_bdesc.bd_info[attno - 1].oi_regular_nulls
                        && !check_null_keys(bval, &nullkeys[attno - 1])
                    {
                        addrange = false;
                        break;
                    }

                    // No regular scan keys -> page range as a whole passes.
                    if keys[attno - 1].is_empty() {
                        attno += 1;
                        continue;
                    }

                    // If it is all nulls, it cannot possibly be consistent.
                    if bval.bv_allnulls {
                        addrange = false;
                        break;
                    }

                    // Collation from the first key (same for all keys of attr).
                    let collation = keys[attno - 1][0].sk_collation;

                    let idx = &idx_rel;
                    let bdesc = &brin(scan).bo_bdesc;
                    let bval = &dtup.bt_columns[attno - 1];
                    if opclass::brin_consistent_is_multi::call(idx, attno - 1)? {
                        // Check all keys at once.
                        addrange = opclass::brin_consistent_multi::call(
                            mcx,
                            idx,
                            attno - 1,
                            collation,
                            bdesc,
                            bval,
                            &keys[attno - 1],
                        )?;
                    } else {
                        // Check keys one by one; any false discards the range.
                        for key in keys[attno - 1].iter() {
                            addrange = opclass::brin_consistent_single::call(
                                mcx,
                                idx,
                                attno - 1,
                                key.sk_collation,
                                bdesc,
                                bval,
                                key,
                            )?;
                            if !addrange {
                                break;
                            }
                        }
                    }

                    // If a scan key eliminated the range, stop.
                    if !addrange {
                        break;
                    }

                    attno += 1;
                }
            }
        }

        // Add the pages in the range to the output bitmap, if needed.
        if addrange {
            let last = core::cmp::min(nblocks as u64, heap_blk + pages_per_range as u64) - 1;
            let mut pageno = heap_blk;
            while pageno <= last {
                tbm_add_page::call(tbm, pageno as BlockNumber)?;
                totalpages += 1;
                pageno += 1;
            }
        }

        heap_blk += pages_per_range as u64;
    }

    if BufferIsValid(buf) {
        release_buffer::call(buf);
    }

    // We have an approximation of the number of pages our scan returns, but no
    // precise idea of the number of heap tuples involved.
    Ok(totalpages * 10)
}

// ===========================================================================
// brinrescan (brin.c:958)
// ===========================================================================

/// `brinrescan(scan, scankey, nscankeys, orderbys, norderbys)` (brin.c:958):
/// re-initialize the scan. BRIN does no scan-key preprocessing; it simply copies
/// the new keys into the descriptor (`memcpy(scan->keyData, scankey, ...)`).
pub fn brinrescan<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    scankey: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    if !scankey.is_empty() && scan.number_of_keys > 0 {
        let n = scan.number_of_keys as usize;
        scan.key_data[..n].clone_from_slice(&scankey[..n]);
    }
    Ok(())
}

// ===========================================================================
// brinendscan (brin.c:977)
// ===========================================================================

/// `brinendscan(scan)` (brin.c:977): tear down the scan working state — release
/// the revmap and free the descriptor.
pub fn brinendscan(opaque: &mut BrinScan<'_>) -> PgResult<()> {
    brinRevmapTerminate(&opaque.bo_rmAccess)?;
    // brin_free_desc(opaque->bo_bdesc): the owned BrinDesc is dropped with the
    // BrinScan; nothing extra to release.
    Ok(())
}

// ===========================================================================
// check_null_keys (brin.c:2299)
// ===========================================================================

/// `check_null_keys(bval, nullkeys, nnullkeys)` (brin.c:2299): test the IS
/// [NOT] NULL scan keys for column `bval` against the range's null summary.
/// Returns `false` if the range can be eliminated.
pub fn check_null_keys(bval: &BrinValues<'_>, nullkeys: &[ScanKeyData<'_>]) -> bool {
    for key in nullkeys {
        debug_assert!(key.sk_attno == bval.bv_attno);

        // Handle only IS NULL / IS NOT NULL tests.
        if (key.sk_flags & SK_ISNULL) == 0 {
            continue;
        }

        if (key.sk_flags & SK_SEARCHNULL) != 0 {
            // IS NULL scan key, but range has no NULLs.
            if !bval.bv_allnulls && !bval.bv_hasnulls {
                return false;
            }
        } else if (key.sk_flags & SK_SEARCHNOTNULL) != 0 {
            // For IS NOT NULL, only skip ranges known to have only nulls.
            if bval.bv_allnulls {
                return false;
            }
        } else {
            // Neither IS NULL nor IS NOT NULL: assume all indexable operators
            // are strict and thus return false with a NULL scan-key value.
            return false;
        }
    }

    true
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `CHECK_FOR_INTERRUPTS()` — same behaviour-preserving no-op the BRIN
/// pageops layer uses.
#[inline]
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}
