//! Owned-tree Rust port of `src/backend/access/gin/ginutil.c` (PostgreSQL 18.3)
//! — the utility routines of the GIN inverted-index access method.
//!
//! The complete set of C functions this module provides, ported 1:1:
//!
//!   * `ginhandler`           — assemble the unified [`IndexAmRoutine`]
//!   * `initGinState`         — fill a [`types_gin::GinState`] describing the index
//!   * `gintuple_get_attrnum` — column number of a stored entry
//!   * `gintuple_get_key`     — stored datum + null category
//!   * `GinNewBuffer`         — allocate a fresh page (FSM recycle / extend)
//!   * `GinInitPage`          — initialize a GIN page header + opaque
//!   * `GinInitBuffer`        — `GinInitPage` over a buffer's page
//!   * `GinInitMetabuffer`    — initialize the metapage
//!   * `ginCompareEntries`    — compare two keys of the same column
//!   * `ginCompareAttEntries` — compare two keys of possibly different columns
//!   * `ginExtractEntries`    — extract + sort + unique the index keys
//!   * `ginoptions`           — parse GIN reloptions
//!   * `ginGetStats`          — read the metapage statistics
//!   * `ginUpdateStats`       — write the metapage statistics (WAL-logged)
//!   * `ginbuildphasename`    — name of an index-build phase
//!
//! `GinState`/`GinMetaPageData`/`GinPageOpaqueData`/`GinStatsData`/`GinOptions`/
//! `GinNullCategory` are the canonical GIN carriers from [`types_gin`]; the
//! `FmgrInfo` arrays are real (resolved by `index_getprocinfo`), the descriptors
//! are owned [`TupleDesc`]s.
//!
//! This is the index-AM tower's GIN handler: `ginhandler` returns the ONE
//! unified [`IndexAmRoutine`] with leading-`mcx` HRTB callbacks. GIN is a
//! bitmap-only AM (`amgettuple = None`, `amgetbitmap = Some(gingetbitmap)`).
//! The scan / insert / vacuum callbacks live in the not-yet-ported sibling GIN
//! units (`ginscan.c`/`ginget.c`/`gininsert.c`/`ginvacuum.c`); the handler wires
//! them through the [`backend_access_gin_ginutil_seams`] AM-callback seams,
//! reached by name through the vtable only — a call panics loudly until the
//! owner lands (mirror-PG-and-panic), exactly as the landed brin handler does.
//!
//! The genuinely-external substrate `ginutil.c` reaches — the
//! catalog/relcache/typcache lookups of `initGinState`, the index-tuple deform
//! of `gintuple_get_*`, the fmgr `extractValueFn`/`compareFn` calls, and the
//! buffer-cache / WAL metapage sequences (`GinNewBuffer`/`ginGetStats`/
//! `ginUpdateStats`) — is routed through the seams, preserving GIN's
//! WAL-before-unlock discipline and the metapage `pd_lower` invariant. The
//! page-byte initialization (`GinInitPage`/`GinInitBuffer`/`GinInitMetabuffer`)
//! and the comparison/extraction control flow are implemented here, branch- and
//! byte-faithful to C.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use mcx::Mcx;

use backend_access_common_reloptions::{
    build_reloptions, relopt_kind, RelOptParseElt, RELOPT_KIND_GIN, RELOPT_TYPE_BOOL,
    RELOPT_TYPE_INT,
};
use backend_storage_page::{PageGetPageSize, PageInit, PageRef};
use backend_utils_error::ereport;
use types_error::error::{ERRCODE_UNDEFINED_FUNCTION, ERROR};
use types_error::{PgError, PgResult};

use backend_access_gin_ginutil_seams as sx;
use backend_utils_cache_relcache_seams as relcache;

use types_core::primitive::{AttrNumber, OffsetNumber, Oid};
use types_core::{InvalidOid, OidIsValid, BLCKSZ};
use types_tuple::heaptuple::DEFAULT_COLLATION_OID;
use types_gin::{
    GinMetaPageData, GinNullCategory, GinOptions, GinPageOpaqueData, GinState, GinStatsData,
    GIN_CAT_EMPTY_ITEM, GIN_CAT_NORM_KEY, GIN_CAT_NULL_ITEM, GIN_CAT_NULL_KEY, GIN_COMPARE_PROC,
    GIN_COMPARE_PARTIAL_PROC, GIN_CONSISTENT_PROC, GIN_CURRENT_VERSION, GIN_EXTRACTQUERY_PROC,
    GIN_EXTRACTVALUE_PROC, GIN_META, GIN_OPTIONS_PROC, GIN_TRICONSISTENT_PROC, GINNProcs,
};
use types_rel::Relation;
use types_storage::storage::Buffer;
use types_core::primitive::InvalidBlockNumber;
use types_tuple::heaptuple::FIRST_OFFSET_NUMBER as FirstOffsetNumber;
use types_tableam::amapi::{
    IndexAmRoutine, IndexInfo, IndexUniqueCheck, TIDBitmap, T_IndexAmRoutine,
};
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_scan::scankey::ScanKeyData;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (gin_private.h / progress.h / vacuum.h).
// ===========================================================================

/// `GIN_METAPAGE_BLKNO` (ginblock.h:52) — re-exported from the carrier crate.
pub use types_gin::GIN_METAPAGE_BLKNO;

/// `VACUUM_OPTION_PARALLEL_BULKDEL` (commands/vacuum.h): `1 << 1`.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 1;
/// `VACUUM_OPTION_PARALLEL_CLEANUP` (commands/vacuum.h): `1 << 2`.
const VACUUM_OPTION_PARALLEL_CLEANUP: u8 = 1 << 2;

/// Index-build progress sub-phase numbers (gin.h / progress.h).
pub const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: i64 = 1;
pub const PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN: i64 = 2;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_1: i64 = 3;
pub const PROGRESS_GIN_PHASE_MERGE_1: i64 = 4;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_2: i64 = 5;
pub const PROGRESS_GIN_PHASE_MERGE_2: i64 = 6;

// ===========================================================================
// Install this crate's inward seams (none — ginutil owns no inward seams; the
// `-seams` crate holds only the outward AM-callback and substrate decls that
// other owners install). The `init_seams()` is the empty conventional hook.
// ===========================================================================

/// This crate installs no inward seams of its own; its `-seams` crate carries
/// only *outward* declarations (the GIN AM vtable callbacks and the cross-
/// subsystem substrate) installed by their real owners.
pub fn init_seams() {}

// ===========================================================================
// ginhandler (ginutil.c:37)
// ===========================================================================

/// `ginhandler()` — return the [`IndexAmRoutine`] with the GIN access-method
/// parameters and callbacks. GIN is bitmap-only: `amgettuple = None`,
/// `amgetbitmap = Some(gingetbitmap)`.
pub fn ginhandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: 0,
        amsupport: GINNProcs as u16,
        amoptsprocnum: GIN_OPTIONS_PROC as u16,
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
        amsearchnulls: false,
        amstorage: true,
        amclusterable: false,
        ampredlocks: true,
        amcanparallel: false,
        amcanbuildparallel: true,
        amcaninclude: false,
        amusemaintenanceworkmem: true,
        amsummarizing: false,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP,
        amkeytype: InvalidOid,

        // ginvalidate (ginvalidate.c) returns a soft-error result and so cannot
        // be the raw `fn(Oid) -> bool` ABI pointer; it is reached by name from
        // the gin-core-probe crate (matching bthandler/brinhandler's
        // amvalidate = None convention). ginadjustmembers likewise is reached by
        // name. ginbuildphasename is reached by name (not a vtable field).
        amvalidate: None,
        // GIN does not set amtranslatestrategy / amtranslatecmptype (NULL in C).
        amtranslatestrategy: None,
        amtranslatecmptype: None,

        // Required interface functions invoked by indexam.c. The scan callbacks
        // are in the unported ginscan.c/ginget.c; the insert/vacuum callbacks in
        // the unported gininsert.c/ginvacuum.c. The adapters seam-and-panic into
        // those owners (reached by name through the vtable only).
        aminsert: gininsert_am,
        ambulkdelete: ginbulkdelete_am,
        amvacuumcleanup: ginvacuumcleanup_am,
        // amroutine->aminsertcleanup = NULL (ginutil.c:71).
        aminsertcleanup: None,

        ambeginscan: ginbeginscan_am,
        amrescan: ginrescan_am,
        amendscan: ginendscan_am,
        // amroutine->amcanreturn = NULL (ginutil.c:74).
        amcanreturn: None,
        // amroutine->amgettuple = NULL (ginutil.c:84) — bitmap-only.
        amgettuple: None,
        amgetbitmap: Some(gingetbitmap_am),
        // amroutine->ammarkpos = amrestrpos = NULL (ginutil.c:87,88).
        ammarkpos: None,
        amrestrpos: None,

        // No parallel index scan (amcanparallel = false; ginutil.c:89-91).
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}

// ---------------------------------------------------------------------------
// AM-vtable adapters: forward to the unported sibling GIN units' seams.
// ---------------------------------------------------------------------------

fn gininsert_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    heap_tid: &types_tuple::heaptuple::ItemPointerData,
    heap_relation: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    sx::gininsert::call(
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

fn ginbulkdelete_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    sx::ginbulkdelete::call(mcx, info, stats, callback_state)
}

fn ginvacuumcleanup_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    sx::ginvacuumcleanup::call(mcx, info, stats)
}

fn ginbeginscan_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    sx::ginbeginscan::call(mcx, index_relation, nkeys, norderbys)
}

fn ginrescan_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    sx::ginrescan::call(mcx, scan, keys, orderbys)
}

fn ginendscan_am<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    sx::ginendscan::call(mcx, scan)
}

fn gingetbitmap_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut TIDBitmap,
) -> PgResult<i64> {
    sx::gingetbitmap::call(mcx, scan, tbm)
}

// ===========================================================================
// initGinState (ginutil.c:101)
// ===========================================================================

/// `initGinState(state, index)` — fill in an empty [`GinState`] struct to
/// describe the index. Subsidiary data is allocated in `mcx`.
pub fn initGinState<'mcx>(index: &Relation<'mcx>, mcx: Mcx<'mcx>) -> PgResult<GinState<'mcx>> {
    let origTupdesc = sx::gin_relation_get_descr::call(mcx, index)?;
    let natts = tupdesc_natts(&origTupdesc);

    // MemSet(state, 0, sizeof(GinState));
    let mut state = GinState::new();

    state.index = index.rd_id;
    state.oneCol = natts == 1;
    state.origTupdesc = clone_tupdesc(mcx, &origTupdesc)?;

    for i in 0..natts {
        let attr = *tupdesc_attr(&origTupdesc, i);

        if state.oneCol {
            // C: state->tupdesc[i] = state->origTupdesc (shares the relcache's
            // refcounted descriptor); the owned model keeps a deep copy.
            state.tupdesc[i] = clone_tupdesc(mcx, &origTupdesc)?;
        } else {
            // CreateTemplateTupleDesc(2);
            // TupleDescInitEntry(td, 1, NULL, INT2OID, -1, 0);
            // TupleDescInitEntry(td, 2, NULL, atttypid, atttypmod, attndims);
            // TupleDescInitEntryCollation(td, 2, attcollation);
            let mut td = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
            backend_access_common_tupdesc::TupleDescInitEntry(
                &mut td, 1, None, INT2OID, -1, 0,
            )?;
            backend_access_common_tupdesc::TupleDescInitEntry(
                &mut td,
                2,
                None,
                attr.atttypid,
                attr.atttypmod,
                attr.attndims as i32,
            )?;
            backend_access_common_tupdesc::TupleDescInitEntryCollation(
                &mut td,
                2,
                attr.attcollation,
            )?;
            state.tupdesc[i] = Some(mcx::alloc_in(mcx, td)?);
        }

        let attnum = (i + 1) as AttrNumber;

        // If the compare proc isn't specified in the opclass definition, look up
        // the index key type's default btree comparator.
        if relcache::index_getprocid::call(index, attnum, GIN_COMPARE_PROC as u16)?
            != InvalidOid
        {
            state.compareFn[i] = relcache::index_getprocinfo::call(
                index.rd_id,
                attnum,
                GIN_COMPARE_PROC as u16,
                GIN_OPTIONS_PROC as u16,
                -1,
            )?;
        } else {
            // lookup_type_cache(attr->atttypid, TYPECACHE_CMP_PROC_FINFO);
            let cmp = sx::gin_lookup_cmp_proc_finfo::call(attr.atttypid)?;
            if !OidIsValid(cmp.fn_oid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "could not identify a comparison function for type {}",
                        backend_utils_adt_format_type_seams::format_type_be_str::call(
                            attr.atttypid
                        )?
                    ))
                    .into_error());
            }
            state.compareFn[i] = cmp;
        }

        // Opclass must always provide extract procs.
        state.extractValueFn[i] = relcache::index_getprocinfo::call(
            index.rd_id,
            attnum,
            GIN_EXTRACTVALUE_PROC as u16,
            GIN_OPTIONS_PROC as u16,
            -1,
        )?;
        state.extractQueryFn[i] = relcache::index_getprocinfo::call(
            index.rd_id,
            attnum,
            GIN_EXTRACTQUERY_PROC as u16,
            GIN_OPTIONS_PROC as u16,
            -1,
        )?;

        // Check opclass capability to do tri-state or binary logic consistent
        // check.
        if relcache::index_getprocid::call(index, attnum, GIN_TRICONSISTENT_PROC as u16)?
            != InvalidOid
        {
            state.triConsistentFn[i] = relcache::index_getprocinfo::call(
                index.rd_id,
                attnum,
                GIN_TRICONSISTENT_PROC as u16,
                GIN_OPTIONS_PROC as u16,
                -1,
            )?;
        }

        if relcache::index_getprocid::call(index, attnum, GIN_CONSISTENT_PROC as u16)?
            != InvalidOid
        {
            state.consistentFn[i] = relcache::index_getprocinfo::call(
                index.rd_id,
                attnum,
                GIN_CONSISTENT_PROC as u16,
                GIN_OPTIONS_PROC as u16,
                -1,
            )?;
        }

        if state.consistentFn[i].fn_oid == InvalidOid
            && state.triConsistentFn[i].fn_oid == InvalidOid
        {
            // elog(ERROR, "missing GIN support function (%d or %d) ...")
            let relname = sx::gin_relation_get_relation_name::call(mcx, index)?;
            return Err(PgError::error(format!(
                "missing GIN support function ({} or {}) for attribute {} of index \"{}\"",
                GIN_CONSISTENT_PROC,
                GIN_TRICONSISTENT_PROC,
                i + 1,
                relname.as_str(),
            )));
        }

        // Check opclass capability to do partial match.
        if relcache::index_getprocid::call(index, attnum, GIN_COMPARE_PARTIAL_PROC as u16)?
            != InvalidOid
        {
            state.comparePartialFn[i] = relcache::index_getprocinfo::call(
                index.rd_id,
                attnum,
                GIN_COMPARE_PARTIAL_PROC as u16,
                GIN_OPTIONS_PROC as u16,
                -1,
            )?;
            state.canPartialMatch[i] = true;
        } else {
            state.canPartialMatch[i] = false;
        }

        // If the index column has a specified collation, honor that; else
        // specify the default collation (harmless if unused).
        let indcoll = relcache::rd_indcollation::call(index, attnum)?;
        if OidIsValid(indcoll) {
            state.supportCollation[i] = indcoll;
        } else {
            state.supportCollation[i] = DEFAULT_COLLATION_OID;
        }
    }

    Ok(state)
}

/// `INT2OID` (catalog/pg_type.h) — the `int2` (smallint) type OID.
const INT2OID: Oid = 21;

// ===========================================================================
// gintuple_get_attrnum (ginutil.c:230) / gintuple_get_key (ginutil.c:263)
// ===========================================================================

/// `gintuple_get_attrnum(ginstate, tuple)` — extract the column number of the
/// stored entry from a GIN tuple.
pub fn gintuple_get_attrnum<'mcx>(
    ginstate: &GinState<'mcx>,
    tuple: &[u8],
    mcx: Mcx<'mcx>,
) -> PgResult<OffsetNumber> {
    if ginstate.oneCol {
        // column number is not stored explicitly
        Ok(FirstOffsetNumber)
    } else {
        // First attribute is always int16, so we can safely use any tuple
        // descriptor to obtain the first attribute of the tuple.
        let (res, isnull) =
            sx::gin_index_getattr::call(mcx, tuple, FirstOffsetNumber, &ginstate.tupdesc[0])?;
        debug_assert!(!isnull);
        let colN = datum_get_uint16(&res);
        debug_assert!(
            colN >= FirstOffsetNumber && colN as usize <= tupdesc_natts(&ginstate.origTupdesc)
        );
        Ok(colN)
    }
}

/// `gintuple_get_key(ginstate, tuple, &category)` — extract the stored datum
/// (and possible null category) from a GIN tuple. Returns the key datum and the
/// resolved [`GinNullCategory`].
pub fn gintuple_get_key<'mcx>(
    ginstate: &GinState<'mcx>,
    tuple: &[u8],
    mcx: Mcx<'mcx>,
) -> PgResult<(Datum<'mcx>, GinNullCategory)> {
    let (res, isnull) = if ginstate.oneCol {
        // Single column index doesn't store attribute numbers in tuples.
        sx::gin_index_getattr::call(mcx, tuple, FirstOffsetNumber, &ginstate.origTupdesc)?
    } else {
        // Since the datum type depends on which index column it's from, we must
        // be careful to use the right tuple descriptor here.
        let colN = gintuple_get_attrnum(ginstate, tuple, mcx)?;
        sx::gin_index_getattr::call(
            mcx,
            tuple,
            offset_number_next(FirstOffsetNumber),
            &ginstate.tupdesc[(colN - 1) as usize],
        )?
    };

    let category = if isnull {
        sx::gin_get_null_category::call(tuple, ginstate.oneCol)?
    } else {
        GIN_CAT_NORM_KEY
    };

    Ok((res, category))
}

// ===========================================================================
// GinNewBuffer (ginutil.c:304) — seam-routed buffer allocation
// ===========================================================================

/// `GinNewBuffer(index)` — allocate a new page (recycling via the FSM, else
/// extending the index file). The returned buffer is pinned and exclusive-
/// locked; the caller initializes the page with [`GinInitBuffer`].
pub fn GinNewBuffer<'mcx>(index: &Relation<'mcx>) -> PgResult<Buffer> {
    sx::gin_new_buffer::call(index)
}

// ===========================================================================
// GinInitPage / GinInitBuffer / GinInitMetabuffer (pure page-byte writes)
// ===========================================================================

/// `GinInitPage(page, f, pageSize)` (ginutil.c:342) — initialize a GIN page's
/// header (`PageInit`, special area = `sizeof(GinPageOpaqueData)`), then set the
/// opaque `flags = f` and `rightlink = InvalidBlockNumber`.
pub fn GinInitPage(page: &mut [u8], f: u32, page_size: usize) -> PgResult<()> {
    PageInit(page, page_size, core::mem::size_of::<GinPageOpaqueData>())?;

    let opaque = GinPageOpaqueData {
        rightlink: InvalidBlockNumber,
        maxoff: 0,
        flags: f as u16,
    };
    write_opaque(page, &opaque)
}

/// `GinInitBuffer(b, f)` (ginutil.c:354) — `GinInitPage` over the buffer's page,
/// using the buffer's page size. Works on the page byte slice (the caller holds
/// the pinned, locked buffer).
pub fn GinInitBuffer(page: &mut [u8], f: u32) -> PgResult<()> {
    // BufferGetPageSize == BLCKSZ for a real buffer page; use the page's
    // recorded size if set, else fall back to BLCKSZ (a freshly-zeroed page
    // reports size 0).
    let page_size = {
        let pr = PageRef::new(page)?;
        let sz = PageGetPageSize(&pr);
        if sz == 0 {
            BLCKSZ
        } else {
            sz
        }
    };
    GinInitPage(page, f, page_size)
}

/// `GinInitMetabuffer(b)` (ginutil.c:360) — initialize the GIN metapage: run
/// `GinInitPage(page, GIN_META, ...)`, write the all-empty [`GinMetaPageData`],
/// and set `pd_lower` just past the metadata so the metadata survives xlog page
/// compression.
pub fn GinInitMetabuffer(page: &mut [u8], page_size: usize) -> PgResult<()> {
    GinInitPage(page, GIN_META as u32, page_size)?;

    let metadata = GinMetaPageData {
        head: InvalidBlockNumber,
        tail: InvalidBlockNumber,
        tailFreeSize: 0,
        nPendingPages: 0,
        nPendingHeapTuples: 0,
        nTotalPages: 0,
        nEntryPages: 0,
        nDataPages: 0,
        nEntries: 0,
        ginVersion: GIN_CURRENT_VERSION,
    };
    write_meta(page, &metadata);

    // Set pd_lower just past the end of the metadata. Essential — without it the
    // metadata is lost if xlog.c compresses the page.
    //   ((PageHeader) page)->pd_lower =
    //       ((char *) metadata + sizeof(GinMetaPageData)) - (char *) page;
    let pd_lower = meta_offset() + size_of_gin_meta_page_data();
    write_pd_lower(page, pd_lower as u16);
    Ok(())
}

// ===========================================================================
// ginCompareEntries (ginutil.c:392) / ginCompareAttEntries (ginutil.c:414)
// ===========================================================================

/// `ginCompareEntries(ginstate, attnum, a, ca, b, cb)` — compare two keys of
/// the same index column. Sorts first by null category; all null items in the
/// same category are equal; for two normal keys, the opclass compare function
/// decides (routed through the fmgr seam).
pub fn ginCompareEntries<'mcx>(
    ginstate: &GinState<'mcx>,
    attnum: OffsetNumber,
    a: Datum<'mcx>,
    categorya: GinNullCategory,
    b: Datum<'mcx>,
    categoryb: GinNullCategory,
) -> PgResult<i32> {
    // if not of same null category, sort by that first
    if categorya != categoryb {
        return Ok(if categorya < categoryb { -1 } else { 1 });
    }

    // all null items in same category are equal
    if categorya != GIN_CAT_NORM_KEY {
        return Ok(0);
    }

    // both not null, so safe to call the compareFn
    let idx = (attnum - 1) as usize;
    sx::gin_compare_entries::call(
        &ginstate.compareFn[idx],
        ginstate.supportCollation[idx],
        a,
        b,
    )
}

/// `ginCompareAttEntries(ginstate, attnuma, a, ca, attnumb, b, cb)` — compare
/// two keys of possibly different index columns. The attribute number is the
/// first sort key.
pub fn ginCompareAttEntries<'mcx>(
    ginstate: &GinState<'mcx>,
    attnuma: OffsetNumber,
    a: Datum<'mcx>,
    categorya: GinNullCategory,
    attnumb: OffsetNumber,
    b: Datum<'mcx>,
    categoryb: GinNullCategory,
) -> PgResult<i32> {
    // attribute number is the first sort key
    if attnuma != attnumb {
        return Ok(if attnuma < attnumb { -1 } else { 1 });
    }

    ginCompareEntries(ginstate, attnuma, a, categorya, b, categoryb)
}

// ===========================================================================
// ginExtractEntries (ginutil.c:487)
// ===========================================================================

/// `keyEntryData` (ginutil.c:434) — a (datum, isnull) pair sorted by
/// `ginExtractEntries`.
#[derive(Clone)]
struct KeyEntryData<'mcx> {
    datum: Datum<'mcx>,
    isnull: bool,
}

/// `ginExtractEntries(ginstate, attnum, value, isNull, &nentries, &categories)`
/// — extract the index key values from an indexable item. The resulting key
/// values are sorted and duplicates are removed. Returns the entry datums; the
/// out-tuple is `(entries, categories)`.
pub fn ginExtractEntries<'mcx>(
    ginstate: &GinState<'mcx>,
    attnum: OffsetNumber,
    value: Datum<'mcx>,
    isNull: bool,
    mcx: Mcx<'mcx>,
) -> PgResult<(Vec<Datum<'mcx>>, Vec<GinNullCategory>)> {
    // We don't call the extractValueFn on a null item. Instead generate a
    // placeholder.
    if isNull {
        return Ok((vec![Datum::null()], vec![GIN_CAT_NULL_ITEM]));
    }

    let idx = (attnum - 1) as usize;

    // OK, call the opclass's extractValueFn.
    let extracted =
        sx::gin_extract_value::call(mcx, &ginstate.extractValueFn[idx], ginstate.supportCollation[idx], value)?;

    // Generate a placeholder if the item contained no keys.
    let (mut entries, mut nullFlags): (Vec<Datum<'mcx>>, Vec<bool>) = match extracted {
        Some((entries, nulls)) if !entries.is_empty() => {
            (entries.into_iter().collect(), nulls.into_iter().collect())
        }
        _ => {
            return Ok((vec![Datum::null()], vec![GIN_CAT_EMPTY_ITEM]));
        }
    };

    let mut nentries = entries.len();

    // If the extractValueFn didn't create a nullFlags array, create one,
    // assuming that everything's non-null.
    if nullFlags.is_empty() {
        nullFlags = vec![false; nentries];
    }

    // If there's more than one key, sort and unique-ify.
    if nentries > 1 {
        let mut keydata: Vec<KeyEntryData<'mcx>> = Vec::with_capacity(nentries);
        for i in 0..nentries {
            keydata.push(KeyEntryData {
                datum: entries[i].clone(),
                isnull: nullFlags[i],
            });
        }

        // qsort_arg(keydata, ..., cmpEntries, &arg). The comparator records the
        // haveDups flag; a stable sort with the same comparator reproduces it.
        let mut have_dups = false;
        let mut sort_err: Option<PgError> = None;
        keydata.sort_by(|a, b| {
            match cmp_entries(ginstate, idx, a, b) {
                Ok(res) => {
                    if res == 0 {
                        have_dups = true;
                    }
                    res.cmp(&0)
                }
                Err(e) => {
                    if sort_err.is_none() {
                        sort_err = Some(e);
                    }
                    core::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = sort_err {
            return Err(e);
        }

        if have_dups {
            // there are duplicates, must get rid of 'em
            entries[0] = keydata[0].datum.clone();
            nullFlags[0] = keydata[0].isnull;
            let mut j = 1usize;
            for i in 1..nentries {
                if cmp_entries(ginstate, idx, &keydata[i - 1], &keydata[i])? != 0 {
                    entries[j] = keydata[i].datum.clone();
                    nullFlags[j] = keydata[i].isnull;
                    j += 1;
                }
            }
            nentries = j;
        } else {
            // easy, no duplicates
            for i in 0..nentries {
                entries[i] = keydata[i].datum.clone();
                nullFlags[i] = keydata[i].isnull;
            }
        }
    }

    // Create GinNullCategory representation from nullFlags.
    let mut categories = vec![GIN_CAT_NORM_KEY; nentries];
    for (i, cat) in categories.iter_mut().enumerate() {
        *cat = if nullFlags[i] {
            GIN_CAT_NULL_KEY
        } else {
            GIN_CAT_NORM_KEY
        };
    }

    entries.truncate(nentries);
    Ok((entries, categories))
}

/// `cmpEntries(a, b, arg)` (ginutil.c:447) — the qsort comparator for
/// `ginExtractEntries`. NULLs sort after non-NULLs; two non-NULLs are ordered by
/// the opclass compare function. (Duplicate detection is the caller's, mirroring
/// the C `arg.haveDups` out-flag.)
fn cmp_entries<'mcx>(
    ginstate: &GinState<'mcx>,
    attidx: usize,
    aa: &KeyEntryData<'mcx>,
    bb: &KeyEntryData<'mcx>,
) -> PgResult<i32> {
    if aa.isnull {
        if bb.isnull {
            Ok(0) // NULL "=" NULL
        } else {
            Ok(1) // NULL ">" not-NULL
        }
    } else if bb.isnull {
        Ok(-1) // not-NULL "<" NULL
    } else {
        sx::gin_compare_entries::call(
            &ginstate.compareFn[attidx],
            ginstate.supportCollation[attidx],
            aa.datum.clone(),
            bb.datum.clone(),
        )
    }
}

// ===========================================================================
// ginoptions (ginutil.c:606)
// ===========================================================================

/// `ginoptions(reloptions, validate)` — parse the GIN reloptions (`fastupdate`,
/// `gin_pending_list_limit`) into a [`GinOptions`] `bytea`. The `reloptions`
/// varlena is passed as its byte slice (the C `Datum reloptions`); `None` is the
/// C `PointerIsValid(reloptions)` false case.
pub fn ginoptions(
    reloptions: Option<&[u8]>,
    validate: bool,
    mcx: Mcx<'_>,
) -> PgResult<Option<Vec<u8>>> {
    let tab = [
        RelOptParseElt::new(
            "fastupdate",
            RELOPT_TYPE_BOOL,
            core::mem::offset_of!(GinOptions, useFastUpdate),
        ),
        RelOptParseElt::new(
            "gin_pending_list_limit",
            RELOPT_TYPE_INT,
            core::mem::offset_of!(GinOptions, pendingListCleanupSize),
        ),
    ];

    build_reloptions(
        mcx,
        reloptions,
        validate,
        RELOPT_KIND_GIN as relopt_kind,
        core::mem::size_of::<GinOptions>(),
        &tab,
    )
}

// ===========================================================================
// ginGetStats (ginutil.c:627) / ginUpdateStats (ginutil.c:654)
// ===========================================================================

/// `ginGetStats(index, stats)` — fetch the index's statistical data from the
/// metapage. `nPendingPages` and `ginVersion` are trustworthy; the others are as
/// of the last VACUUM. The metapage read is seam-routed; the field copy is here.
pub fn ginGetStats<'mcx>(index: &Relation<'mcx>) -> PgResult<GinStatsData> {
    let metadata = sx::gin_get_stats::call(index)?;

    Ok(GinStatsData {
        nPendingPages: metadata.nPendingPages,
        nTotalPages: metadata.nTotalPages,
        nEntryPages: metadata.nEntryPages,
        nDataPages: metadata.nDataPages,
        nEntries: metadata.nEntries,
        ginVersion: metadata.ginVersion,
    })
}

/// `ginUpdateStats(index, stats, is_build)` — write the given statistics to the
/// index's metapage. `nPendingPages` and `ginVersion` are *not* copied over; the
/// metapage write + WAL-before-unlock sequence is seam-routed.
pub fn ginUpdateStats<'mcx>(
    index: &Relation<'mcx>,
    stats: &GinStatsData,
    is_build: bool,
) -> PgResult<()> {
    sx::gin_update_stats::call(
        index,
        stats.nTotalPages,
        stats.nEntryPages,
        stats.nDataPages,
        stats.nEntries,
        is_build,
    )
}

// ===========================================================================
// ginbuildphasename (ginutil.c:711)
// ===========================================================================

/// `ginbuildphasename(phasenum)` — return the name of an index build phase, or
/// [`None`] (C's `NULL`) for an unknown phase.
pub fn ginbuildphasename(phasenum: i64) -> Option<&'static str> {
    match phasenum {
        PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE => Some("initializing"),
        PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN => Some("scanning table"),
        PROGRESS_GIN_PHASE_PERFORMSORT_1 => Some("sorting tuples (workers)"),
        PROGRESS_GIN_PHASE_MERGE_1 => Some("merging tuples (workers)"),
        PROGRESS_GIN_PHASE_PERFORMSORT_2 => Some("sorting tuples"),
        PROGRESS_GIN_PHASE_MERGE_2 => Some("merging tuples"),
        _ => None,
    }
}

// ===========================================================================
// Page-header byte helpers (mirror the established nbtsort page-builder
// pattern: the idiomatic page crate exposes pd_lower/pd_special only through
// read-only projections, so GinInitPage / GinInitMetabuffer write these bytes
// directly on the well-defined page-header layout).
// ===========================================================================

/// `sizeof(PageHeaderData)` minus the line-pointer array (== 24).
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// Byte offset of `pd_lower` within the page header.
const OFF_PD_LOWER: usize = 12;
/// Byte offset of `pd_special` within the page header.
const OFF_PD_SPECIAL: usize = 16;

#[inline]
fn read_pd_special(buf: &[u8]) -> usize {
    u16::from_ne_bytes([buf[OFF_PD_SPECIAL], buf[OFF_PD_SPECIAL + 1]]) as usize
}

#[inline]
fn write_pd_lower(buf: &mut [u8], value: u16) {
    buf[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

/// `MAXALIGN(len)` — round up to `MAXIMUM_ALIGNOF` (8).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (8 - 1)) & !(8 - 1)
}

/// Byte offset of the metadata within a page (`PageGetContents` ==
/// `MAXALIGN(SizeOfPageHeaderData)`).
#[inline]
const fn meta_offset() -> usize {
    maxalign(SIZE_OF_PAGE_HEADER_DATA)
}

// Field byte offsets within `GinMetaPageData` (matching the C struct layout with
// natural alignment; verified with the C compiler).
const OFF_GIN_HEAD: usize = 0; // uint32
const OFF_GIN_TAIL: usize = 4; // uint32
const OFF_GIN_TAILFREESIZE: usize = 8; // uint32
const OFF_GIN_NPENDINGPAGES: usize = 12; // uint32
const OFF_GIN_NPENDINGHEAPTUPLES: usize = 16; // int64 (16 is already 8-aligned)
const OFF_GIN_NTOTALPAGES: usize = 24; // uint32
const OFF_GIN_NENTRYPAGES: usize = 28; // uint32
const OFF_GIN_NDATAPAGES: usize = 32; // uint32
// 4 bytes of pad (36..40) before the 8-aligned int64
const OFF_GIN_NENTRIES: usize = 40; // int64
const OFF_GIN_VERSION: usize = 48; // int32

/// `sizeof(GinMetaPageData)` as laid out on disk: `offsetof(ginVersion) +
/// sizeof(int32)`, MAXALIGN'd to the 8-byte struct alignment → 56.
#[inline]
const fn size_of_gin_meta_page_data() -> usize {
    maxalign(OFF_GIN_VERSION + 4)
}

/// Write a [`GinPageOpaqueData`] into the page's special area (`pd_special`).
fn write_opaque(page: &mut [u8], opaque: &GinPageOpaqueData) -> PgResult<()> {
    let special = read_pd_special(page);
    // rightlink: BlockNumber (u32) | maxoff: OffsetNumber (u16) | flags: u16
    page[special..special + 4].copy_from_slice(&opaque.rightlink.to_ne_bytes());
    page[special + 4..special + 6].copy_from_slice(&opaque.maxoff.to_ne_bytes());
    page[special + 6..special + 8].copy_from_slice(&opaque.flags.to_ne_bytes());
    Ok(())
}

/// Write a [`GinMetaPageData`] into the page contents (`PageGetContents`), each
/// field at its exact byte offset so the on-disk image is byte-identical to C.
fn write_meta(page: &mut [u8], meta: &GinMetaPageData) {
    let off = meta_offset();
    let put_u32 = |page: &mut [u8], field_off: usize, val: u32| {
        let p = off + field_off;
        page[p..p + 4].copy_from_slice(&val.to_ne_bytes());
    };
    let put_i64 = |page: &mut [u8], field_off: usize, val: i64| {
        let p = off + field_off;
        page[p..p + 8].copy_from_slice(&val.to_ne_bytes());
    };
    let put_i32 = |page: &mut [u8], field_off: usize, val: i32| {
        let p = off + field_off;
        page[p..p + 4].copy_from_slice(&val.to_ne_bytes());
    };
    put_u32(page, OFF_GIN_HEAD, meta.head);
    put_u32(page, OFF_GIN_TAIL, meta.tail);
    put_u32(page, OFF_GIN_TAILFREESIZE, meta.tailFreeSize);
    put_u32(page, OFF_GIN_NPENDINGPAGES, meta.nPendingPages);
    put_i64(page, OFF_GIN_NPENDINGHEAPTUPLES, meta.nPendingHeapTuples);
    put_u32(page, OFF_GIN_NTOTALPAGES, meta.nTotalPages);
    put_u32(page, OFF_GIN_NENTRYPAGES, meta.nEntryPages);
    put_u32(page, OFF_GIN_NDATAPAGES, meta.nDataPages);
    put_i64(page, OFF_GIN_NENTRIES, meta.nEntries);
    put_i32(page, OFF_GIN_VERSION, meta.ginVersion);
}

// ===========================================================================
// Small helpers.
// ===========================================================================

/// `OffsetNumberNext(offsetNumber)` (storage/off.h).
#[inline]
fn offset_number_next(off: OffsetNumber) -> OffsetNumber {
    off + 1
}

/// `DatumGetUInt16(res)` — the GIN multi-column tuple's first attribute (the
/// `int2` column number). The deformed first attribute is a by-value `int2`.
fn datum_get_uint16(d: &Datum<'_>) -> u16 {
    d.as_u16()
}

/// `origTupdesc->natts`.
fn tupdesc_natts(td: &types_tuple::heaptuple::TupleDesc<'_>) -> usize {
    td.as_ref()
        .map(|t| t.natts as usize)
        .unwrap_or(0)
}

/// `TupleDescAttr(origTupdesc, i)`.
fn tupdesc_attr<'a>(
    td: &'a types_tuple::heaptuple::TupleDesc<'_>,
    i: usize,
) -> &'a types_tuple::heaptuple::FormData_pg_attribute {
    td.as_ref()
        .expect("GIN index descriptor is NULL")
        .attr(i)
}

/// Deep-copy an owned `TupleDesc` (`CreateTupleDescCopy`): the relcache's
/// reference-counted descriptor is shared in C, but the owned model holds copies.
/// `None` (a NULL descriptor) copies to `None`.
fn clone_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    td: &types_tuple::heaptuple::TupleDesc<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match td.as_ref() {
        Some(t) => {
            let copy = backend_access_common_tupdesc::CreateTupleDescCopy(mcx, t)?;
            Ok(Some(mcx::alloc_in(mcx, copy)?))
        }
        None => Ok(None),
    }
}
