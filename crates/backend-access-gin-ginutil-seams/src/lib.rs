//! Seam declarations the `backend-access-gin-ginutil` unit (`ginutil.c`) needs.
//!
//! Two families of outward calls live here:
//!
//!  1. **The GIN AM vtable callbacks** the `ginhandler` assembles into the one
//!     unified [`types_tableam::amapi::IndexAmRoutine`]. The callback *bodies*
//!     are in the not-yet-ported sibling GIN units (`gininsert.c` /
//!     `ginvacuum.c` / `ginscan.c` / `ginget.c`); `ginutil` is the first cyclic
//!     caller (it builds the dispatch vector), so it declares the seams. Each
//!     owner installs its slot from its own `init_seams()` when it lands; until
//!     then a call panics loudly (mirror-PG-and-panic). These are reached *by
//!     name* through the vtable only.
//!
//!  2. **The genuinely cross-subsystem substrate** `ginutil.c`'s own functions
//!     reach: the catalog/relcache/typcache lookups `initGinState` performs
//!     (`RelationGetDescr`, `RelationGetRelationName`, the
//!     `lookup_type_cache(..., TYPECACHE_CMP_PROC_FINFO)` compare-proc fallback
//!     — whose `cmp_proc_finfo` is not on the trimmed relcache `TypeCacheEntry`),
//!     the fmgr `extractValueFn` call (`ginExtractEntries`), and the
//!     buffer-cache / WAL sequences (`GinNewBuffer` FSM-recycle/extend,
//!     `ginGetStats` / `ginUpdateStats` metapage I/O with the GIN
//!     WAL-before-unlock discipline). These ride the substrate exactly as the
//!     src-idiomatic GIN port did.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::Mcx;
use types_core::primitive::Oid;
use types_core::fmgr::FmgrInfo;
use types_error::PgResult;
use types_gin::{GinMetaPageData, GinNullCategory};
use types_rel::Relation;
use types_scan::scankey::ScanKeyData;
use types_storage::storage::Buffer;
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck, TIDBitmap};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, TupleDesc};

// ===========================================================================
// initGinState substrate (catalog / relcache / typcache).
// ===========================================================================

seam_core::seam!(
    /// `RelationGetDescr(index)` (utils/rel.h) for `initGinState` — the index's
    /// nominal tuple descriptor, deep-copied into `mcx`. The relcache owns the
    /// reference-counted descriptor; the safe port hands back an owned copy.
    /// `Err` carries the relcache miss / OOM.
    pub fn gin_relation_get_descr<'mcx>(
        mcx: Mcx<'mcx>,
        index: &Relation<'mcx>,
    ) -> PgResult<TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `RelationGetRelationName(index)` (`NameStr(rd_rel->relname)`) for the
    /// `missing GIN support function` error of `initGinState`. The relcache owns
    /// `rd_rel`; the seam resolves the entry from the relation.
    pub fn gin_relation_get_relation_name<'mcx>(
        mcx: Mcx<'mcx>,
        index: &Relation<'mcx>,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `lookup_type_cache(atttypid, TYPECACHE_CMP_PROC_FINFO)->cmp_proc_finfo`
    /// (ginutil.c:147) — the index key type's default btree comparator, used as
    /// the `GIN_COMPARE_PROC` fallback when the opclass omits it. The trimmed
    /// relcache `TypeCacheEntry` does not carry `cmp_proc_finfo`, so the
    /// resolution is encapsulated on the typcache owner. Returns the resolved
    /// `FmgrInfo` (its `fn_oid` may be `InvalidOid`, which `initGinState` turns
    /// into the `could not identify a comparison function` ereport). `Err`
    /// carries the catalog-lookup surface.
    pub fn gin_lookup_cmp_proc_finfo(atttypid: Oid) -> PgResult<FmgrInfo>
);

// ===========================================================================
// gintuple deform (access/itup.h index_getattr).
// ===========================================================================

seam_core::seam!(
    /// `index_getattr(tuple, attnum, tupdesc, &isnull)` (access/itup.h) for
    /// `gintuple_get_attrnum` / `gintuple_get_key`: deform the `attnum`-th
    /// attribute of a GIN index tuple against `tupdesc`. Returns the datum and
    /// the is-null flag. The index-tuple deform is the common indextuple
    /// substrate.
    pub fn gin_index_getattr<'mcx>(
        mcx: Mcx<'mcx>,
        tuple: &[u8],
        attnum: u16,
        tupdesc: &TupleDesc<'mcx>,
    ) -> PgResult<(Datum<'mcx>, bool)>
);

seam_core::seam!(
    /// `GinGetNullCategory(tuple, ginstate)` (ginblock.h) — read the stored null
    /// category byte off the end of a GIN index tuple. The byte layout depends
    /// on the index tuple's `t_info` size, which the indextuple substrate owns.
    pub fn gin_get_null_category(tuple: &[u8], one_col: bool) -> PgResult<GinNullCategory>
);

// ===========================================================================
// fmgr extractValueFn (ginExtractEntries).
// ===========================================================================

seam_core::seam!(
    /// `FunctionCall3Coll(&ginstate->extractValueFn[attnum-1], collation, value,
    /// &nentries, &nullFlags)` (ginExtractEntries, ginutil.c:513): invoke the
    /// opclass `extractValueFn` for the value of index column `attnum`. Returns
    /// `Some((entries, nullFlags))` — the extracted key datums and the optional
    /// per-key null flags (an empty `nullFlags` mirrors the C `NULL` the caller
    /// then fills with `false`) — or `None` / an empty `entries` when the item
    /// produced no keys (the placeholder path). The fmgr GIN extract dispatch is
    /// genuinely external. `Err` carries its `ereport(ERROR)`.
    pub fn gin_extract_value<'mcx>(
        mcx: Mcx<'mcx>,
        flinfo: &FmgrInfo,
        collation: Oid,
        value: Datum<'mcx>,
    ) -> PgResult<Option<(mcx::PgVec<'mcx, Datum<'mcx>>, mcx::PgVec<'mcx, bool>)>>
);

seam_core::seam!(
    /// `DatumGetInt32(FunctionCall2Coll(&ginstate->compareFn[attnum-1],
    /// collation, a, b))` (ginCompareEntries, ginutil.c:406): invoke the opclass
    /// `compareFn` for two non-null keys of index column `attnum`. The fmgr GIN
    /// compare dispatch is genuinely external. `Err` carries its
    /// `ereport(ERROR)`.
    pub fn gin_compare_entries<'mcx>(
        flinfo: &FmgrInfo,
        collation: Oid,
        a: Datum<'mcx>,
        b: Datum<'mcx>,
    ) -> PgResult<i32>
);

// ===========================================================================
// Buffer-cache / WAL substrate (GinNewBuffer / ginGetStats / ginUpdateStats).
// ===========================================================================

seam_core::seam!(
    /// `GinNewBuffer(index)` (ginutil.c:305): allocate a fresh page, recycling
    /// via the FSM (`GetFreeIndexPage` + `ConditionalLockBuffer` +
    /// `GinPageIsRecyclable`) else extending the index file (`ExtendBufferedRel`,
    /// `EB_LOCK_FIRST`). The returned buffer is pinned and exclusive-locked. The
    /// FSM probe, conditional locking, recyclability check (which reads
    /// `pd_prune_xid` against `GlobalVisCheckRemovableXid`, a transam concern),
    /// and file extension are buffer-cache substrate; this seam performs the
    /// whole C loop preserving the pin/lock order. `Err` carries the buffer
    /// `ereport(ERROR)`s.
    pub fn gin_new_buffer<'mcx>(index: &Relation<'mcx>) -> PgResult<Buffer>
);

seam_core::seam!(
    /// `ginGetStats(index, stats)` metapage read (ginutil.c:634): `ReadBuffer`
    /// the metapage under `GIN_SHARE`, return its `GinMetaPageData`, and
    /// `UnlockReleaseBuffer`. The field copy into `GinStatsData` is `ginutil`'s
    /// own logic; the buffer round-trip is substrate. `Err` carries the buffer
    /// `ereport(ERROR)`s.
    pub fn gin_get_stats<'mcx>(index: &Relation<'mcx>) -> PgResult<GinMetaPageData>
);

seam_core::seam!(
    /// `ginUpdateStats(index, stats, is_build)` metapage write (ginutil.c:655):
    /// `ReadBuffer` the metapage under `GIN_EXCLUSIVE`, copy the four planner-stat
    /// fields in a critical section, reset `pd_lower` past the metadata, mark the
    /// buffer dirty, emit `XLOG_GIN_UPDATE_META_PAGE` before unlock when
    /// `RelationNeedsWAL(index) && !is_build`, then `UnlockReleaseBuffer`. The
    /// WAL-before-unlock ordering + the `ginxlogUpdateMeta` record are substrate.
    /// Only the four planner-stat fields cross (`nPendingPages` / `ginVersion`
    /// are *not* copied). `Err` carries the buffer / WAL `ereport(ERROR)`s.
    pub fn gin_update_stats<'mcx>(
        index: &Relation<'mcx>,
        nTotalPages: types_core::primitive::BlockNumber,
        nEntryPages: types_core::primitive::BlockNumber,
        nDataPages: types_core::primitive::BlockNumber,
        nEntries: i64,
        is_build: bool,
    ) -> PgResult<()>
);

// ===========================================================================
// GIN AM vtable callbacks (gininsert.c / ginvacuum.c / ginscan.c / ginget.c).
//
// `ginhandler` assembles these into the unified `IndexAmRoutine`. Reached by
// name through the vtable only; the owners install them when they land.
// ===========================================================================

seam_core::seam!(
    /// `gininsert(...)` (gininsert.c) — the `aminsert` callback.
    pub fn gininsert<'mcx>(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap_relation: &Relation<'mcx>,
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut IndexInfo,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ginbulkdelete(info, stats, callback, callback_state)` (ginvacuum.c) —
    /// the `ambulkdelete` callback.
    pub fn ginbulkdelete<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
        callback_state: Option<u64>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `ginvacuumcleanup(info, stats)` (ginvacuum.c) — the `amvacuumcleanup`
    /// callback.
    pub fn ginvacuumcleanup<'mcx>(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>
);

seam_core::seam!(
    /// `ginbeginscan(indexRelation, nkeys, norderbys)` (ginscan.c) — the
    /// `ambeginscan` callback; allocates and returns the scan descriptor.
    pub fn ginbeginscan<'mcx>(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
    ) -> PgResult<IndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `ginrescan(scan, keys, nkeys, orderbys, norderbys)` (ginscan.c) — the
    /// `amrescan` callback.
    pub fn ginrescan<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut IndexScanDescData<'mcx>,
        keys: &[ScanKeyData<'mcx>],
        orderbys: &[ScanKeyData<'mcx>],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ginendscan(scan)` (ginscan.c) — the `amendscan` callback.
    pub fn ginendscan<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `gingetbitmap(scan, tbm)` (ginget.c) — the `amgetbitmap` callback; fetch
    /// all valid tuples into the bitmap, returning the count.
    pub fn gingetbitmap<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut IndexScanDescData<'mcx>,
        tbm: &mut TIDBitmap,
    ) -> PgResult<i64>
);
