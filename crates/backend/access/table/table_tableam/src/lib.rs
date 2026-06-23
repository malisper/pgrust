//! Port of `src/backend/access/table/tableam.c` — table access method
//! routines too big to be inline functions — plus the `tableam.h` inline
//! dispatch wrappers this translation unit itself instantiates
//! (`table_index_fetch_begin`/`_tuple`/`_end`, `table_tuple_insert`/
//! `_delete`/`_update`).
//!
//! The dispatch model mirrors C: `relation->rd_tableam` is a vtable
//! ([`::types_tableam::TableAmRoutine`], fetched through the relcache owner's
//! seam) whose callbacks the wrappers invoke. The open relation crosses as a
//! [`::rel::Relation`] handle; scan and index-fetch descriptors are
//! owned values created by the AM.
//!
//! `default_table_access_method` and `synchronize_seqscans` are this unit's
//! GUC globals — backend-local state, so `thread_local!`.

#![allow(non_snake_case)]

use std::boxed::Box;
use std::cell::{Cell, RefCell};
use std::string::String;

use ::mcx::Mcx;
use ::types_core::primitive::{
    BlockNumber, ForkNumber, BLCKSZ, InvalidBlockNumber, InvalidForkNumber, FSM_FORKNUM,
    MAIN_FORKNUM, MaxBlockNumber, VISIBILITYMAP_FORKNUM,
};
use ::rel::Relation;
use ::types_core::xact::TransactionIdIsValid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::nodes::TupleSlotKind;
use ::nodes::tuptable::SlotData;
use ::snapshot::snapshot::IsMVCCSnapshot;
use ::types_tableam::relscan::{
    ParallelBlockTableScanDescData, ParallelBlockTableScanWorkerData, ParallelTableScanDesc,
    TableScanDesc, TableScanDescData, SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC,
    SO_TEMP_SNAPSHOT, SO_TYPE_ANALYZE, SO_TYPE_BITMAPSCAN, SO_TYPE_SAMPLESCAN, SO_TYPE_SEQSCAN,
    SO_TYPE_TIDRANGESCAN, SO_TYPE_TIDSCAN,
};
use ::types_tableam::scankey::ScanKeyData;
use ::types_tableam::tableam::{
    BulkInsertStateData, IndexFetchTableData, LockTupleMode, LockTupleNoKeyExclusive, Snapshot,
    TM_FailureData, TM_Result, TU_UpdateIndexes, TableAmRoutine,
};
use ::types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_VIEW};
use ::types_tuple::heaptuple::ItemPointerData;

use relcache_seams as relcache;

/// Install this crate's seam implementations.
///
/// The bitmap-scan table-AM wrappers (`table_endscan` / `table_rescan`) in
/// `backend-access-table-tableam-bm-seams` dispatch through the relation's
/// `rd_tableam` vtable to the concrete AM, exactly as the value-typed
/// `table_endscan` / `table_rescan` bodies below do. Their seam contracts
/// match those bodies (the bitmap rescan passes `NULL` scan keys, mirrored by
/// the `key = None` argument), so they are installed here.
///
/// The COPY/seqscan scan seams in `backend-access-table-tableam-seams`
/// (`table_beginscan` / `table_scan_getnextslot{,_direction}` / `table_rescan`
/// / `table_endscan` / `table_relation_set_new_filelocator`) are the
/// value-typed `TableScanDesc<'mcx>` forms that match the bodies below; they
/// are installed here, dispatching through `rel->rd_tableam` exactly as the
/// inline wrappers do. (Like the bitmap-scan seams, they panic loudly at the
/// `rd_tableam` dereference until the heap AM provider — `heapam_handler.c` —
/// lands and installs a vtable; that is C's NULL-pointer crash, mirror-and-
/// panic.)
///
/// The remaining `backend-access-table-tableam-seams` decls
/// (`get_table_am_routine` / `table_relation_toast_am` /
/// `table_relation_needs_toast_table` / `table_parallelscan_reinitialize`) are
/// NOT installed — they are provider-unported (the AM handler `heapam_handler.c`
/// and the vtable resolver `tableamapi.c` are `todo`, no body exists). See
/// DESIGN_DEBT.md; they are tracked in `seams-init`'s
/// `CONTRACT_RECONCILE_PENDING`.
pub fn init_seams() {
    tableam_bm_seams::table_beginscan_bm::set(table_beginscan_bm);
    tableam_bm_seams::table_scan_bitmap_next_tuple::set(
        table_scan_bitmap_next_tuple,
    );
    tableam_bm_seams::table_endscan::set(table_endscan_bm);
    tableam_bm_seams::table_rescan::set(table_rescan_bm);

    // tablecmds DefineRelation reads the default_table_access_method GUC.
    tablecmds_seams::default_table_access_method::set(|mcx| {
        ::mcx::PgString::from_str_in(&default_table_access_method(), mcx)
    });

    // The COPY/seqscan value-typed scan seams (unified off the retired
    // opaque-scan-handle model onto the C-faithful `TableScanDesc<'mcx>`).
    table_tableam_seams::table_beginscan::set(table_beginscan_seam);
    table_tableam_seams::table_scan_getnextslot::set(table_scan_getnextslot_fwd);
    table_tableam_seams::table_scan_getnextslot_direction::set(
        table_scan_getnextslot,
    );
    table_tableam_seams::table_rescan::set(table_rescan_seam);
    table_tableam_seams::table_endscan::set(table_endscan);
    table_tableam_seams::table_index_fetch_tuple_check::set(
        table_index_fetch_tuple_check,
    );
    table_tableam_seams::table_index_delete_tuples::set(
        table_index_delete_tuples,
    );
    table_tableam_seams::table_relation_set_new_filelocator::set(
        table_relation_set_new_filelocator,
    );
    table_tableam_seams::table_relation_nontransactional_truncate::set(
        table_relation_nontransactional_truncate,
    );

    // GUC variable accessors over this unit's `thread_local` backing store —
    // C's `conf->variable` pointer (`&synchronize_seqscans` /
    // `&default_table_access_method` in guc_tables.c). Both are plain
    // PGC_USERSET GUC-slot variables (read from the GUC machinery, not the
    // ControlFile); the guc.c assign path writes them through `set`.
    guc_tables::vars::synchronize_seqscans.install(
        guc_tables::GucVarAccessors {
            get: synchronize_seqscans,
            set: set_synchronize_seqscans,
        },
    );
    guc_tables::vars::default_table_access_method.install(
        guc_tables::GucVarAccessors {
            // `char *default_table_access_method` boots to "heap" and GUC
            // string storage never returns to NULL afterwards
            // (guc_tables.h), so `get` always yields `Some`.
            get: || Some(default_table_access_method()),
            set: |v| {
                set_default_table_access_method(
                    v.as_deref().unwrap_or(DEFAULT_TABLE_ACCESS_METHOD),
                )
            },
        },
    );

    // `check_default_table_access_method` (tableamapi.c) — the GUC check hook
    // for `default_table_access_method`. C wires this function pointer into the
    // config table at compile time; here this unit owns it and installs it into
    // the guc-tables slot.
    guc_tables::hooks::check_default_table_access_method
        .install(check_default_table_access_method);

    // `table_finish_bulk_insert` (tableam.h inline) — this unit owns the inline
    // dispatch wrapper, so it installs the consumer-side decls the CTAS
    // (`createas.c`) and matview (`matview.c`) bulk-insert receivers carry. The
    // heap AM leaves the `finish_bulk_insert` slot NULL, so the wrapper is a
    // faithful no-op for the only AM in the tree (see `table_finish_bulk_insert`).
    createas_seams::table_finish_bulk_insert::set(table_finish_bulk_insert);
    matview_deps_seams::table_finish_bulk_insert::set(table_finish_bulk_insert);

    // ANALYZE-sampling scan dispatch (acquire_sample_rows in commands/analyze.c).
    table_tableam_seams::table_beginscan_analyze::set(table_beginscan_analyze);
    table_tableam_seams::table_scan_analyze_next_block::set(
        table_scan_analyze_next_block,
    );
    table_tableam_seams::table_scan_analyze_next_tuple::set(
        table_scan_analyze_next_tuple,
    );

}

/// Adapter for `backend-access-table-tableam-bm-seams::table_endscan` — the
/// bitmap-scan `table_endscan(scan)`; identical body to [`table_endscan`].
fn table_endscan_bm(scan: TableScanDesc<'_>) -> PgResult<()> {
    table_endscan(scan)
}

/// Adapter for `backend-access-table-tableam-bm-seams::table_rescan` — the
/// bitmap-scan `table_rescan(scan, NULL)` form, i.e. [`table_rescan`] with no
/// scan keys.
fn table_rescan_bm<'mcx>(mcx: Mcx<'mcx>, scan: &mut TableScanDescData<'mcx>) -> PgResult<()> {
    table_rescan(mcx, scan, None)
}

/// Adapter for `backend-access-table-tableam-seams::table_rescan` — the seqscan
/// `table_rescan(scan, NULL)` form (`nodeSeqscan.c`'s `ExecReScanSeqScan`).
fn table_rescan_seam<'mcx>(mcx: Mcx<'mcx>, scan: &mut TableScanDescData<'mcx>) -> PgResult<()> {
    table_rescan(mcx, scan, None)
}

/// Adapter for `backend-access-table-tableam-seams::table_beginscan` — the
/// COPY/seqscan `table_beginscan(rel, snapshot, 0, NULL)` form. The snapshot
/// crosses as a shared `Rc<SnapshotData>`; the scan runs under it with no scan
/// keys (`SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE`,
/// the C `table_beginscan` flags).
fn table_beginscan_seam<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: std::rc::Rc<::snapshot::SnapshotData>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    (am(relation).scan_begin)(
        mcx,
        relation,
        Some((*snapshot).clone()),
        0,
        ::mcx::PgVec::new_in(mcx),
        None,
        flags,
    )
}

/// Adapter for `backend-access-table-tableam-seams::table_scan_getnextslot` —
/// the forward-direction `table_scan_getnextslot(scan, ForwardScanDirection,
/// slot)` form (COPY TO's scan loop).
fn table_scan_getnextslot_fwd<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    table_scan_getnextslot(
        mcx,
        scan,
        types_scan::sdir::ScanDirection::ForwardScanDirection,
        slot,
    )
}

/// `table_scan_getnextslot(scan, direction, slot)` (access/tableam.h inline) —
/// fetch the next tuple of the in-progress scan into `slot`. The direction-
/// carrying form (`nodeSeqscan.c`'s `SeqNext` passes `estate->es_direction`).
pub fn table_scan_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    direction: types_scan::sdir::ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.scan_getnextslot)(mcx, scan, direction, slot)
}

/// `table_relation_set_new_filelocator(rel, newrlocator, persistence,
/// &freezeXid, &minmulti)` (access/tableam.h inline) — create storage for the
/// relation's new relfilelocator (and its init fork if unlogged), handing back
/// the AM-chosen `relfrozenxid`/`relminmxid`. The C
/// `rel->rd_tableam->relation_set_new_filelocator(...)`: the open `Relation`
/// carries the AM vtable. Returns `(freeze_xid, minmulti)`.
fn table_relation_set_new_filelocator<'mcx>(
    rel: &Relation<'mcx>,
    newrlocator: types_storage::RelFileLocator,
    relpersistence: i8,
) -> PgResult<(u32, u32)> {
    // rel->rd_tableam->relation_set_new_filelocator(rel, &newrlocator,
    //     persistence, &freezeXid, &minmulti);
    let routine = am(rel);
    (routine.relation_set_new_filelocator)(rel, &newrlocator, relpersistence)
}

/// `rel->rd_tableam->relation_nontransactional_truncate(rel)`
/// (access/tableam.h:1606).
fn table_relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
    let routine = am(rel);
    (routine.relation_nontransactional_truncate)(rel)
}

// ===========================================================================
// Constants controlling parallel-seqscan block allocation (tableam.c)
// ===========================================================================

/// The number of I/O chunks we try to break a parallel seqscan down into.
const PARALLEL_SEQSCAN_NCHUNKS: u32 = 2048;
/// Ramp down size of allocations when we've only this number of chunks left.
const PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS: u32 = 64;
/// Cap the size of parallel I/O chunks to this number of blocks.
const PARALLEL_SEQSCAN_MAX_CHUNK_SIZE: u32 = 8192;

// ===========================================================================
// GUC variables (tableam.c)
// ===========================================================================

/// `DEFAULT_TABLE_ACCESS_METHOD` (`access/tableam.h`).
pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

thread_local! {
    /// `char *default_table_access_method` — the `default_table_access_method`
    /// GUC.
    static DEFAULT_TABLE_ACCESS_METHOD_GUC: RefCell<String> =
        RefCell::new(String::from(DEFAULT_TABLE_ACCESS_METHOD));
    /// `bool synchronize_seqscans` — the `synchronize_seqscans` GUC.
    static SYNCHRONIZE_SEQSCANS_GUC: Cell<bool> = const { Cell::new(true) };
}

/// Read the `default_table_access_method` GUC.
pub fn default_table_access_method() -> String {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| v.borrow().clone())
}

/// Assign the `default_table_access_method` GUC (the guc.c assign hook's
/// store).
pub fn set_default_table_access_method(value: &str) {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| *v.borrow_mut() = String::from(value));
}

/// Read the `synchronize_seqscans` GUC.
pub fn synchronize_seqscans() -> bool {
    SYNCHRONIZE_SEQSCANS_GUC.with(Cell::get)
}

/// `check_default_table_access_method(char **newval, void **extra, GucSource
/// source)` (access/table/tableamapi.c). The `default_table_access_method` GUC
/// check hook: reject an empty or over-long name, and (when we can reach the
/// catalog) verify the named access method actually exists.
fn check_default_table_access_method(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    // `**newval == '\0'`: a string GUC's value is never NULL here (boot val is
    // "heap" and the GUC string store never returns to NULL), so an empty value
    // is the `Some("")` case.
    let name = match newval.as_deref() {
        Some(s) => s,
        None => "",
    };

    if name.is_empty() {
        guc_seams::guc_check_errdetail::call(
            "\"default_table_access_method\" cannot be empty.".to_string(),
        );
        return Ok(false);
    }

    if name.len() >= ::types_core::fmgr::NAMEDATALEN as usize {
        guc_seams::guc_check_errdetail::call(format!(
            "\"default_table_access_method\" is too long (maximum {} characters).",
            ::types_core::fmgr::NAMEDATALEN - 1
        ));
        return Ok(false);
    }

    // If we aren't inside a transaction, or not connected to a database, we
    // cannot do the catalog access necessary to verify the method. Must accept
    // the value on faith.
    let in_xact = transam_xact_seams::is_transaction_state::call();
    let my_db = init_small_seams::my_database_id::call();
    if in_xact && my_db != ::types_core::primitive::InvalidOid {
        // `get_table_am_oid(*newval, true)` — `missing_ok = true` so a missing AM
        // returns InvalidOid rather than erroring.
        let am_oid = tablecmds_seams::get_table_am_oid::call(name, true)?;
        if !::types_core::primitive::OidIsValid(am_oid) {
            guc_seams::guc_check_errdetail::call(format!(
                "Table access method \"{name}\" does not exist."
            ));
            return Ok(false);
        }
    }

    Ok(true)
}

/// Assign the `synchronize_seqscans` GUC.
pub fn set_synchronize_seqscans(value: bool) {
    SYNCHRONIZE_SEQSCANS_GUC.with(|v| v.set(value));
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// `relation->rd_tableam` where C dereferences it unconditionally: a missing
/// vtable is the C NULL-pointer crash, so panic loudly.
fn am(relation: &Relation<'_>) -> TableAmRoutine {
    relcache::relation_rd_tableam::call(relation)
        .expect("relation has no table access method (C would dereference NULL rd_tableam)")
}

/// The `unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan)` guard
/// used by the dispatch wrappers. We don't expect direct calls with valid
/// CheckXidAlive for catalog or regular tables; see the comments in xact.c
/// where these variables are declared.
fn unexpected_during_logical_decoding() -> bool {
    TransactionIdIsValid(transam_xact_seams::check_xid_alive::call())
        && !transam_xact_seams::bsysscan::call()
}

/// `elog(ERROR, ...)` — an internal error.
fn elog_error(message: impl Into<String>) -> PgError {
    PgError::error(message)
}

/// `add_size(s1, s2)` (`storage/shmem.c`): overflow-checked addition raising
/// C's error. Local private mirror of the unported shmem.c helper.
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2).ok_or_else(|| {
        PgError::error("requested shared memory size overflows size_t")
            .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    })
}

/// `pg_nextpower2_32(num)` (`port/pg_bitutils.h`, static inline) — the next
/// power of 2 >= num, for num in [1, 2^31].
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0);
    if num & num.wrapping_sub(1) == 0 {
        return num;
    }
    // 1 << (pg_leftmost_one_pos32(num) + 1)
    1u32 << (31 - num.leading_zeros() + 1)
}

// ===========================================================================
// Slot functions (tableam.c)
// ===========================================================================

/// `table_slot_callbacks(relation)` — which slot callbacks (here: which slot
/// class) suit the relation.
pub fn table_slot_callbacks(relation: &Relation<'_>) -> TupleSlotKind {
    if let Some(am) = relcache::relation_rd_tableam::call(relation) {
        (am.slot_callbacks)(relation)
    } else if relation.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        // Historically FDWs expect to store heap tuples in slots. Continue
        // handing them one, to make it less painful to adapt FDWs to new
        // versions. The cost of a heap slot over a virtual slot is pretty
        // small.
        TupleSlotKind::HeapTuple
    } else {
        // These need to be supported, as some parts of the code (like COPY)
        // need to create slots for such relations too. It seems better to
        // centralize the knowledge that a heap slot is the right thing in
        // that case here.
        debug_assert!({
            let relkind = relation.rd_rel.relkind;
            relkind == RELKIND_VIEW || relkind == RELKIND_PARTITIONED_TABLE
        });
        TupleSlotKind::Virtual
    }
}

/// `table_slot_create(relation, reglist)` — create a slot suitable for the
/// relation.
///
/// In C the optional `reglist` (`List **`) receives the new slot via
/// `lappend` so the caller can drop it later; in the owned model the caller
/// owns the returned slot and registers it itself (push it onto the list
/// standing in for `*reglist`).
pub fn table_slot_create<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
) -> PgResult<::nodes::tuptable::SlotData<'mcx>> {
    let tts_cb = table_slot_callbacks(relation);
    let tupdesc = Some(::mcx::alloc_in(mcx, relation.rd_att.clone_in(mcx)?)?);
    execTuples_seams::make_single_tuple_table_slot::call(mcx, tupdesc, tts_cb)
}

// ===========================================================================
// Table scan functions (tableam.c)
// ===========================================================================

/// `table_beginscan_strat(rel, snapshot, nkeys, key, allow_strat, allow_sync)`
/// (access/tableam.h inline) — like `table_beginscan`, but lets the caller
/// control whether a nondefault buffer access strategy may be used and whether
/// syncscan may be chosen. The snapshot is the caller's (genam registers the
/// catalog snapshot itself and passes it in); no `SO_TEMP_SNAPSHOT`.
pub fn table_beginscan_strat<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot,
    nkeys: i32,
    key: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>>,
    allow_strat: bool,
    allow_sync: bool,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;
    if allow_strat {
        flags |= SO_ALLOW_STRAT;
    }
    if allow_sync {
        flags |= SO_ALLOW_SYNC;
    }
    (am(relation).scan_begin)(mcx, relation, snapshot, nkeys, key, None, flags)
}

/// `table_beginscan_catalog(relation, nkeys, key)`.
pub fn table_beginscan_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    nkeys: i32,
    key: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags =
        SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE | SO_TEMP_SNAPSHOT;
    let relid = relation.rd_id; // RelationGetRelid(relation)
    let snapshot = snapmgr_seams::register_snapshot::call(
        snapmgr_seams::get_catalog_snapshot::call(relid)?,
    )?;

    (am(relation).scan_begin)(mcx, relation, Some(snapshot), nkeys, key, None, flags)
}

/// `table_beginscan_analyze(rel)` (access/tableam.h inline) — the alternative
/// entry point `acquire_sample_rows` uses: `scan_begin(rel, NULL, 0, NULL,
/// NULL, SO_TYPE_ANALYZE)`.
pub fn table_beginscan_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_ANALYZE;
    (am(relation).scan_begin)(
        mcx,
        relation,
        None,
        0,
        ::mcx::PgVec::new_in(mcx),
        None,
        flags,
    )
}

/// `table_scan_analyze_next_block(scan, stream)` (access/tableam.h inline) —
/// dispatch the AM's `scan_analyze_next_block` callback.
pub fn table_scan_analyze_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    next_buffer: &mut dyn FnMut() -> PgResult<types_storage::buf::Buffer>,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.scan_analyze_next_block)(mcx, scan, next_buffer)
}

/// `table_scan_analyze_next_tuple(scan, OldestXmin, liverows, deadrows, slot)`
/// (access/tableam.h inline) — dispatch the AM's `scan_analyze_next_tuple`
/// callback.
pub fn table_scan_analyze_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    oldest_xmin: ::types_core::TransactionId,
    liverows: &mut f64,
    deadrows: &mut f64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.scan_analyze_next_tuple)(mcx, scan, oldest_xmin, liverows, deadrows, slot)
}

// ===========================================================================
// Parallel table scan related functions (tableam.c)
// ===========================================================================

/// `table_parallelscan_estimate(rel, snapshot)`.
pub fn table_parallelscan_estimate(rel: &Relation<'_>, snapshot: &Snapshot) -> PgResult<usize> {
    let mut sz: usize = 0;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            sz = add_size(
                sz,
                snapmgr_seams::estimate_snapshot_space::call(s),
            )?;
        }
        _ => {
            // Assert(snapshot == SnapshotAny)
            debug_assert!(snapshot.is_none());
        }
    }

    sz = add_size(sz, (am(rel).parallelscan_estimate)(rel))?;

    Ok(sz)
}

/// `table_parallelscan_initialize(rel, pscan, snapshot)`.
///
/// `pscan` is the in-DSM [`ParallelBlockTableScanDescData`] header the caller
/// (`ExecSeqScanInitializeDSM`) just placed in the `shm_toc` chunk;
/// `snapshot_buf` is the chunk's flexible-array tail (the bytes at
/// `(char *) pscan + phs_snapshot_off`), already sized by
/// [`table_parallelscan_estimate`]. C writes the serialized snapshot directly
/// into that tail; the owned model serializes to a `Vec` and copies it in.
pub fn table_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
    snapshot_buf: &mut [u8],
    snapshot: &Snapshot,
) -> PgResult<()> {
    let snapshot_off = (am(rel).parallelscan_initialize)(rel, pscan)?;

    pscan.phs_snapshot_off = snapshot_off;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            // SerializeSnapshot(snapshot, (char *) pscan + pscan->phs_snapshot_off)
            let bytes = snapmgr_seams::serialize_snapshot::call(s)?;
            debug_assert!(bytes.len() <= snapshot_buf.len());
            snapshot_buf[..bytes.len()].copy_from_slice(&bytes);
            pscan.phs_snapshot_any = false;
        }
        _ => {
            // Assert(snapshot == SnapshotAny)
            debug_assert!(snapshot.is_none());
            pscan.phs_snapshot_any = true;
        }
    }

    Ok(())
}

/// `table_beginscan_parallel(relation, pscan)`.
pub fn table_beginscan_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    pscan: ParallelTableScanDesc,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

    debug_assert!(types_storage::RelFileLocatorEquals(
        &relation.rd_locator,
        &pscan.desc().phs_locator
    ));

    let snapshot: Snapshot;
    if !pscan.desc().phs_snapshot_any {
        // Snapshot was serialized -- restore it
        let bytes = pscan.snapshot_bytes();
        let restored = snapmgr_seams::restore_snapshot::call(bytes)?;
        snapshot = Some(snapmgr_seams::register_snapshot::call(
            restored,
        )?);
        flags |= SO_TEMP_SNAPSHOT;
    } else {
        // SnapshotAny passed by caller (not serialized)
        snapshot = None;
    }

    (am(relation).scan_begin)(
        mcx,
        relation,
        snapshot,
        0,
        ::mcx::PgVec::new_in(mcx),
        Some(pscan),
        flags,
    )
}

// ===========================================================================
// Index scan related functions (tableam.h wrappers used by this unit)
// ===========================================================================

/// `table_index_fetch_begin(rel)` (tableam.h inline).
pub fn table_index_fetch_begin<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<Box<IndexFetchTableData<'mcx>>> {
    (am(rel).index_fetch_begin)(mcx, rel)
}

/// `table_index_fetch_reset(scan)` (tableam.h inline) — release any resources
/// (e.g. buffer pins) held by the index fetch, without ending it.
pub fn table_index_fetch_reset(scan: &mut IndexFetchTableData<'_>) -> PgResult<()> {
    let routine = am(&scan.rel);
    (routine.index_fetch_reset)(scan)
}

/// `table_index_fetch_end(scan)` (tableam.h inline).
pub fn table_index_fetch_end(scan: Box<IndexFetchTableData<'_>>) -> PgResult<()> {
    let routine = am(&scan.rel);
    (routine.index_fetch_end)(scan)
}

/// `table_index_fetch_tuple(scan, tid, snapshot, slot, call_again, all_dead)`
/// (tableam.h inline).
pub fn table_index_fetch_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot,
    slot: &mut SlotData<'mcx>,
    call_again: &mut bool,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    // We don't expect direct calls to table_index_fetch_tuple with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_index_fetch_tuple call during logical decoding",
        ));
    }

    let routine = am(&scan.rel);
    (routine.index_fetch_tuple)(mcx, scan, tid, snapshot, slot, call_again, all_dead)
}

/// `table_index_fetch_tuple_check(rel, tid, snapshot, all_dead)` (tableam.c).
///
/// To perform the check simply start an index scan, create the necessary
/// slot, do the heap lookup, and shut everything down again. This could be
/// optimized, but is unlikely to matter from a performance POV. If there
/// frequently are live index pointers also matching a unique index key, the
/// CPU overhead of this routine is unlikely to matter.
///
/// Note that `tid` may be modified when we return true if the AM supports
/// storing multiple row versions reachable via a single index entry (like
/// heap's HOT).
///
/// `snapshot` is `&mut` for the same reason as [`table_index_fetch_tuple`]: a
/// dirty snapshot is used as an output param, and `_bt_check_unique` reads the
/// conflict info (`xmin`/`xmax`/`speculativeToken`) back out of it on return.
pub fn table_index_fetch_tuple_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    let mut call_again = false;

    let mut slot = table_slot_create(mcx, rel)?;
    let mut scan = table_index_fetch_begin(mcx, rel)?;
    let found = table_index_fetch_tuple(
        mcx,
        &mut scan,
        tid,
        snapshot,
        &mut slot,
        &mut call_again,
        all_dead,
    )?;
    table_index_fetch_end(scan)?;
    execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;

    Ok(found)
}

/// `table_index_delete_tuples(rel, delstate)` (`access/tableam.h` inline) — the
/// tableam dispatch an index AM calls during (simple or bottom-up) index-tuple
/// deletion: `return rel->rd_tableam->index_delete_tuples(rel, delstate);`.
/// Dispatches to the AM's `index_delete_tuples` callback (heap's is
/// `heapam_index_delete_tuples`), which determines the deletable TIDs and
/// returns the operation's `snapshotConflictHorizon`.
pub fn table_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    delstate: &mut ::types_tableam::tableam::TmIndexDeleteOp<'mcx>,
) -> PgResult<::types_core::TransactionId> {
    let routine = am(rel);
    (routine.index_delete_tuples)(mcx, rel, delstate)
}

// ===========================================================================
// Non-modifying operations on individual tuples (tableam.c)
// ===========================================================================

/// `table_tuple_get_latest_tid(scan, tid)`.
pub fn table_tuple_get_latest_tid<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    let tableam = am(&scan.rs_rd);

    // We don't expect direct calls to table_tuple_get_latest_tid with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_get_latest_tid call during logical decoding",
        ));
    }

    // Since this can be called with user-supplied TID, don't trust the input
    // too much.
    if !(tableam.tuple_tid_valid)(scan, tid)? {
        let blk = tid.ip_blkid.block_number();
        let off = tid.ip_posid;
        let relname = scan.rs_rd.name();
        return Err(PgError::error(format!(
            "tid ({blk}, {off}) is not valid for relation \"{relname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    (tableam.tuple_get_latest_tid)(mcx, scan, tid)
}

// ===========================================================================
// Table scan setup/teardown wrappers (tableam.h inline)
// ===========================================================================

/// `table_beginscan_tid(rel, snapshot)` (tableam.h inline) — alternative entry
/// point for setting up a `TableScanDesc` for a TID scan.
pub fn table_beginscan_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDSCAN;
    (am(rel).scan_begin)(mcx, rel, snapshot, 0, ::mcx::PgVec::new_in(mcx), None, flags)
}

/// `table_beginscan_bm(rel, snapshot, nkeys, key)` (tableam.h inline) — set up a
/// `TableScanDesc` for a bitmap heap scan (`SO_TYPE_BITMAPSCAN |
/// SO_ALLOW_PAGEMODE`). The executor (`BitmapTableScanSetup`) passes no scan
/// keys; the snapshot crosses as a shared `Rc<SnapshotData>`.
pub fn table_beginscan_bm<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    snapshot: Option<std::rc::Rc<::snapshot::SnapshotData>>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_BITMAPSCAN | SO_ALLOW_PAGEMODE;
    (am(&rel).scan_begin)(
        mcx,
        &rel,
        snapshot.map(|s| (*s).clone()),
        0,
        ::mcx::PgVec::new_in(mcx),
        None,
        flags,
    )
}

/// `table_scan_bitmap_next_tuple(scan, slot, &recheck, &lossy_pages,
/// &exact_pages)` (tableam.h inline) — fetch the next visible tuple of a bitmap
/// heap scan into `slot`, dispatching to the AM's `scan_bitmap_next_tuple`.
pub fn table_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.scan_bitmap_next_tuple)(mcx, scan, slot, recheck, lossy_pages, exact_pages)
}

/// `table_endscan(scan)` (tableam.h inline) — end a relation scan.
pub fn table_endscan(scan: TableScanDesc<'_>) -> PgResult<()> {
    let routine = am(&scan.rs_rd);
    (routine.scan_end)(scan)
}

/// `table_rescan(scan, key)` (tableam.h inline) — restart a relation scan.
pub fn table_rescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    key: Option<&[ScanKeyData<'mcx>]>,
) -> PgResult<()> {
    let routine = am(&scan.rs_rd);
    (routine.scan_rescan)(mcx, scan, key, false, false, false, false)
}

/// `table_beginscan_sampling(rel, snapshot, nkeys, key, allow_strat,
/// allow_sync, allow_pagemode)` (tableam.h inline) — alternative entry point for
/// setting up a `TableScanDesc` for a TABLESAMPLE scan. Like
/// `table_beginscan_strat`, but also lets the caller control whether page-mode
/// visibility checking is used.
pub fn table_beginscan_sampling<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Option<std::rc::Rc<::snapshot::SnapshotData>>,
    nkeys: i32,
    key: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<TableScanDesc<'mcx>> {
    let mut flags = SO_TYPE_SAMPLESCAN;
    if allow_strat {
        flags |= SO_ALLOW_STRAT;
    }
    if allow_sync {
        flags |= SO_ALLOW_SYNC;
    }
    if allow_pagemode {
        flags |= SO_ALLOW_PAGEMODE;
    }
    (am(relation).scan_begin)(mcx, relation, snapshot.map(|s| (*s).clone()), nkeys, key, None, flags)
}

/// `table_rescan_set_params(scan, key, allow_strat, allow_sync,
/// allow_pagemode)` (tableam.h inline) — restart a relation scan after changing
/// params. Allows changing the buffer strategy, syncscan, and pagemode options
/// before starting a fresh scan.
pub fn table_rescan_set_params<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    key: Option<&[ScanKeyData<'mcx>]>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    let routine = am(&scan.rs_rd);
    (routine.scan_rescan)(mcx, scan, key, true, allow_strat, allow_sync, allow_pagemode)
}

/// `table_scan_sample_next_block(scan, scanstate)` (tableam.h inline) — select
/// the next block of a sample scan. Calls the AM's `scan_sample_next_block`,
/// which invokes the tablesample method's `NextSampleBlock` callback (if any) or
/// scans the relation sequentially. Returns `false` when the scan is finished.
/// `scanstate` crosses as the narrow [`SampleScanDriver`] capability the AM
/// callback needs (see its docs in `types-tableam`).
pub fn table_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn ::types_tableam::tableam::SampleScanDriver,
) -> PgResult<bool> {
    // We don't expect direct calls to table_scan_sample_next_block with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_block call during logical decoding",
        ));
    }

    let routine = am(&scan.rs_rd);
    (routine.scan_sample_next_block)(mcx, scan, scanstate)
}

/// `table_scan_sample_next_tuple(scan, scanstate, slot)` (tableam.h inline) —
/// fetch the next sample tuple into `slot`, returning `true` if a visible tuple
/// was found. `table_scan_sample_next_block` must previously have selected a
/// block.
pub fn table_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn ::types_tableam::tableam::SampleScanDriver,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // We don't expect direct calls to table_scan_sample_next_tuple with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_tuple call during logical decoding",
        ));
    }

    let routine = am(&scan.rs_rd);
    (routine.scan_sample_next_tuple)(mcx, scan, scanstate, slot)
}

/// `table_beginscan_tidrange(rel, snapshot, mintid, maxtid)` (tableam.h inline)
/// — set up a `TableScanDesc` for a TID range scan (`SO_TYPE_TIDRANGESCAN |
/// SO_ALLOW_PAGEMODE`), then bound it to `[mintid, maxtid]`.
pub fn table_beginscan_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Option<std::rc::Rc<::snapshot::SnapshotData>>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDRANGESCAN | SO_ALLOW_PAGEMODE;
    let routine = am(rel);
    let mut sscan = (routine.scan_begin)(
        mcx,
        rel,
        snapshot.map(|s| (*s).clone()),
        0,
        ::mcx::PgVec::new_in(mcx),
        None,
        flags,
    )?;
    // sscan->rs_rd->rd_tableam->scan_set_tidrange(sscan, mintid, maxtid);
    (routine.scan_set_tidrange)(&mut sscan, mintid, maxtid)?;
    Ok(sscan)
}

/// `table_rescan_tidrange(sscan, mintid, maxtid)` (tableam.h inline) — restart a
/// TID range scan, re-bounding it to `[mintid, maxtid]`.
pub fn table_rescan_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<()> {
    debug_assert!((scan.rs_flags & SO_TYPE_TIDRANGESCAN) != 0);
    let routine = am(&scan.rs_rd);
    (routine.scan_rescan)(mcx, scan, None, false, false, false, false)?;
    (routine.scan_set_tidrange)(scan, mintid, maxtid)
}

/// `table_scan_getnextslot_tidrange(sscan, direction, slot)` (tableam.h inline)
/// — fetch the next tuple of a TID-range scan into `slot`.
pub fn table_scan_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    direction: types_scan::sdir::ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.scan_getnextslot_tidrange)(mcx, scan, direction, slot)
}

/// `table_tuple_tid_valid(scan, tid)` (tableam.h inline) — verify `tid` is a
/// potentially valid tuple identifier.
pub fn table_tuple_tid_valid(
    scan: &mut TableScanDescData<'_>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    let routine = am(&scan.rs_rd);
    (routine.tuple_tid_valid)(scan, tid)
}

/// `table_tuple_fetch_row_version(rel, tid, snapshot, slot)` (tableam.h inline)
/// — fetch the tuple at `tid` into `slot`, after a visibility test against
/// `snapshot`.
pub fn table_tuple_fetch_row_version<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // We don't expect direct calls to table_tuple_fetch_row_version with valid
    // CheckXidAlive for catalog or regular tables. See detailed comments in
    // xact.c where these variables are declared.
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_fetch_row_version call during logical decoding",
        ));
    }

    (am(rel).tuple_fetch_row_version)(mcx, rel, tid, snapshot, slot)
}

// ===========================================================================
// Manipulations of physical tuples (tableam.h wrappers used by this unit)
// ===========================================================================

/// `table_tuple_insert(rel, slot, cid, options, bistate)` (tableam.h inline).
pub fn table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: ::types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    (am(rel).tuple_insert)(mcx, rel, slot, cid, options, bistate)
}

/// `table_tuple_insert_speculative(rel, slot, cid, options, bistate,
/// specToken)` (tableam.h inline) — speculatively insert `slot`, stamped with
/// `spec_token`, for ON CONFLICT arbiter-index resolution.
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_insert_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: ::types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
    spec_token: u32,
) -> PgResult<()> {
    (am(rel).tuple_insert_speculative)(mcx, rel, slot, cid, options, bistate, spec_token)
}

/// `table_tuple_complete_speculative(rel, slot, specToken, succeeded)`
/// (tableam.h inline) — complete (`succeeded`) or kill (`!succeeded`) a
/// previously speculatively inserted tuple.
pub fn table_tuple_complete_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    spec_token: u32,
    succeeded: bool,
) -> PgResult<()> {
    (am(rel).tuple_complete_speculative)(mcx, rel, slot, spec_token, succeeded)
}

/// `table_multi_insert(rel, slots, nslots, cid, options, bistate)` (tableam.h
/// inline) — insert a batch of tuples in one AM call.
pub fn table_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: ::types_core::xact::CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    (am(rel).multi_insert)(mcx, rel, slots, cid, options, bistate)
}

/// `table_finish_bulk_insert(rel, options)` (tableam.h inline) — complete
/// insertions made via `tuple_insert`/`multi_insert` with a `BulkInsertState`.
///
/// C dispatches through the *optional* `rd_tableam->finish_bulk_insert` slot
/// (`if (rel->rd_tableam && rel->rd_tableam->finish_bulk_insert) ...`). The
/// heap AM (`heapam_methods` in `heapam_handler.c`) never sets that slot, so
/// for the only AM in the tree the call is a no-op; [`TableAmRoutine`] does not
/// carry the never-installed slot, matching that NULL callback. Mirrors the C
/// inline exactly: no callback → nothing to do.
pub fn table_finish_bulk_insert<'mcx>(rel: &Relation<'mcx>, _options: i32) -> PgResult<()> {
    // Touch the relation's AM vtable to mirror the C `rel->rd_tableam` probe
    // (and to surface the same NULL-`rd_tableam` crash if the relation has no
    // installed access method, exactly as C would dereference it).
    let _ = am(rel);
    Ok(())
}

/// `table_tuple_delete(rel, tid, cid, snapshot, crosscheck, wait, tmfd,
/// changingPart)` (tableam.h inline).
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: ::types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changingPart: bool,
) -> PgResult<TM_Result> {
    (am(rel).tuple_delete)(mcx, rel, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart)
}

/// `table_tuple_update(rel, otid, slot, cid, snapshot, crosscheck, wait,
/// tmfd, lockmode, update_indexes)` (tableam.h inline).
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: ::types_core::xact::CommandId,
    snapshot: &Snapshot,
    crosscheck: &Snapshot,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    (am(rel).tuple_update)(
        mcx,
        rel,
        otid,
        slot,
        cid,
        snapshot,
        crosscheck,
        wait,
        tmfd,
        lockmode,
        update_indexes,
    )
}

/// `table_tuple_lock(rel, tid, snapshot, slot, cid, mode, wait_policy, flags,
/// tmfd)` (tableam.h inline).
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_lock<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
    slot: &mut SlotData<'mcx>,
    cid: ::types_core::xact::CommandId,
    mode: LockTupleMode,
    wait_policy: ::types_tableam::tableam::LockWaitPolicy,
    flags: u8,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    (am(rel).tuple_lock)(mcx, rel, tid, snapshot, slot, cid, mode, wait_policy, flags, tmfd)
}

// ===========================================================================
// Functions to make modifications a bit simpler (tableam.c)
// ===========================================================================

/// `simple_table_tuple_insert(rel, slot)` — insert a tuple.
///
/// Currently, this routine differs from `table_tuple_insert` only in
/// supplying a default command ID and not allowing access to the speedup
/// options.
pub fn simple_table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    let cid = transam_xact_seams::get_current_command_id::call(true)?;
    table_tuple_insert(mcx, rel, slot, cid, 0, None)
}

/// `simple_table_tuple_delete(rel, tid, snapshot)` — delete a tuple.
///
/// This routine may be used to delete a tuple when concurrent updates of the
/// target tuple are not expected (for example, because we have a lock on the
/// relation associated with the tuple). Any failure is reported via
/// `ereport()`.
pub fn simple_table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();

    let cid = transam_xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_delete(
        mcx,
        rel,
        tid,
        cid,
        snapshot,
        &None, // InvalidSnapshot
        true,  // wait for commit
        &mut tmfd,
        false, // changingPart
    )?;

    match result {
        // Tuple was already updated in current command?
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        // done successfully
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_delete status: {}",
            other as u32
        ))),
    }
}

/// `simple_table_tuple_update(rel, otid, slot, snapshot, update_indexes)` —
/// replace a tuple.
///
/// This routine may be used to update a tuple when concurrent updates of the
/// target tuple are not expected (for example, because we have a lock on the
/// relation associated with the tuple). Any failure is reported via
/// `ereport()`.
pub fn simple_table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let mut lockmode: LockTupleMode = LockTupleNoKeyExclusive;

    let cid = transam_xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_update(
        mcx,
        rel,
        otid,
        slot,
        cid,
        snapshot,
        &None, // InvalidSnapshot
        true,  // wait for commit
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    )?;

    match result {
        // Tuple was already updated in current command?
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        // done successfully
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_update status: {}",
            other as u32
        ))),
    }
}

// ===========================================================================
// Helper functions to implement parallel scans for block oriented AMs
// (tableam.c)
// ===========================================================================

/// RAII spinlock guard: `SpinLockAcquire(lock)` on construction (the
/// `TAS_SPIN`; on contention, the `s_lock` backoff loop), `SpinLockRelease` on
/// drop. Mirrors the C `SpinLockAcquire`/`SpinLockRelease` bracket around
/// `phs_mutex`.
struct SpinLockGuard<'a> {
    lock: &'a types_storage::Spinlock,
}

impl<'a> SpinLockGuard<'a> {
    fn acquire(lock: &'a types_storage::Spinlock) -> Self {
        if lock.tas_spin() != 0 {
            s_lock::s_lock(lock, Some(file!()), line!() as i32, None);
        }
        SpinLockGuard { lock }
    }
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

/// `table_block_parallelscan_estimate(rel)`.
pub fn table_block_parallelscan_estimate(_rel: &Relation<'_>) -> usize {
    core::mem::size_of::<ParallelBlockTableScanDescData>()
}

/// `table_block_parallelscan_initialize(rel, pscan)`.
///
/// `pscan` is the freshly-placed in-DSM header (the leader is the sole writer
/// pre-launch, so the `&mut` is sound). Mirrors the C
/// `ParallelBlockTableScanDesc bpscan = (ParallelBlockTableScanDesc) pscan;
/// bpscan->base.phs_locator = …; SpinLockInit(&bpscan->phs_mutex);
/// bpscan->phs_startblock = InvalidBlockNumber;
/// pg_atomic_init_u64(&bpscan->phs_nallocated, 0)`.
pub fn table_block_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
) -> PgResult<usize> {
    pscan.phs_locator = rel.rd_locator;
    let phs_nblocks = bufmgr_seams::
        relation_get_number_of_blocks_in_fork::call(rel, MAIN_FORKNUM)?;
    pscan.phs_nblocks = phs_nblocks;
    // compare phs_syncscan initialization to similar logic in initscan
    pscan.phs_syncscan = synchronize_seqscans()
        && !rel.uses_local_buffers()
        && phs_nblocks > (init_small_seams::nbuffers::call() / 4) as BlockNumber;
    // SpinLockInit(&bpscan->phs_mutex)
    pscan.phs_mutex = types_storage::Spinlock::new();
    // bpscan->phs_startblock = InvalidBlockNumber
    pscan.set_phs_startblock(InvalidBlockNumber);
    // pg_atomic_init_u64(&bpscan->phs_nallocated, 0)
    pscan.phs_nallocated.write(0);

    Ok(core::mem::size_of::<ParallelBlockTableScanDescData>())
}

/// `table_block_parallelscan_reinitialize(rel, pscan)`.
pub fn table_block_parallelscan_reinitialize(
    _rel: &Relation<'_>,
    pscan: &ParallelBlockTableScanDescData,
) {
    // pg_atomic_write_u64(&bpscan->phs_nallocated, 0)
    pscan.phs_nallocated.write(0);
}

/// `table_block_parallelscan_startblock_init(rel, pbscanwork, pbscan)` —
/// find and set the scan's startblock.
///
/// Determine where the parallel seq scan should start. This function may be
/// called many times, once by each parallel worker. We must be careful only
/// to set the startblock once.
pub fn table_block_parallelscan_startblock_init(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelBlockTableScanDescData,
) -> PgResult<()> {
    let mut sync_startpage: BlockNumber = InvalidBlockNumber;
    let bpscan = pbscan;

    // Reset the state we use for controlling allocation size.
    *pbscanwork = ParallelBlockTableScanWorkerData::default();

    // StaticAssertStmt(MaxBlockNumber <= 0xFFFFFFFE, ...)
    const _: () = assert!(
        MaxBlockNumber <= 0xFFFF_FFFE,
        "pg_nextpower2_32 may be too small for non-standard BlockNumber width"
    );

    // We determine the chunk size based on the size of the relation. First
    // we split the relation into PARALLEL_SEQSCAN_NCHUNKS chunks but we then
    // take the next highest power of 2 number of the chunk size. This means
    // we split the relation into somewhere between PARALLEL_SEQSCAN_NCHUNKS
    // and PARALLEL_SEQSCAN_NCHUNKS / 2 chunks.
    pbscanwork.phsw_chunk_size = pg_nextpower2_32(core::cmp::max(
        bpscan.phs_nblocks / PARALLEL_SEQSCAN_NCHUNKS,
        1,
    ));

    // Ensure we don't go over the maximum chunk size with larger tables.
    // This means we may get much more than PARALLEL_SEQSCAN_NCHUNKS for
    // larger tables. Too large a chunk size has been shown to be detrimental
    // to synchronous scan performance.
    pbscanwork.phsw_chunk_size =
        core::cmp::min(pbscanwork.phsw_chunk_size, PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

    // retry:
    loop {
        // Grab the spinlock.
        let guard = SpinLockGuard::acquire(&bpscan.phs_mutex);

        // If the scan's startblock has not yet been initialized, we must do
        // so now. If this is not a synchronized scan, we just start at block
        // 0, but if it is a synchronized scan, we must get the starting
        // position from the synchronized scan machinery. We can't hold the
        // spinlock while doing that, though, so release the spinlock, get
        // the information we need, and retry. If nobody else has initialized
        // the scan in the meantime, we'll fill in the value we fetched on
        // the second time through.
        if bpscan.phs_startblock() == InvalidBlockNumber {
            if !pbscan.phs_syncscan {
                bpscan.set_phs_startblock(0);
            } else if sync_startpage != InvalidBlockNumber {
                bpscan.set_phs_startblock(sync_startpage);
            } else {
                drop(guard); // SpinLockRelease(&pbscan->phs_mutex)
                sync_startpage = syncscan_seams::ss_get_location::call(
                    rel.rd_id,
                    bpscan.phs_nblocks,
                )?;
                continue; // goto retry
            }
        }
        // SpinLockRelease(&pbscan->phs_mutex) — guard drops here.
        drop(guard);
        break;
    }

    Ok(())
}

/// `table_block_parallelscan_nextpage(rel, pbscanwork, pbscan)` — get the
/// next page to scan.
///
/// Even if there are no pages left to scan, another backend could have
/// grabbed a page to scan and not yet finished looking at it, so it doesn't
/// follow that the scan is done when the first backend gets an
/// InvalidBlockNumber return.
pub fn table_block_parallelscan_nextpage(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelBlockTableScanDescData,
) -> PgResult<BlockNumber> {
    let bpscan = pbscan;
    let nallocated: u64;

    // The logic below allocates block numbers out to parallel workers in a
    // way that each worker will receive a set of consecutive block numbers
    // to scan ("chunks"; sized in table_block_parallelscan_startblock_init,
    // ramped down towards the end of the scan). phsw_chunk_remaining tracks
    // the blocks remaining in this worker's chunk; phs_nallocated tracks how
    // many blocks have been allocated to workers overall, and may exceed
    // phs_nblocks because workers keep fetch-and-adding after the scan is
    // exhausted — which is why it must be 64 bits wide.

    // First check if we have any remaining blocks in a previous chunk for
    // this worker. We must consume all of the blocks from that before we
    // allocate a new chunk to the worker.
    if pbscanwork.phsw_chunk_remaining > 0 {
        // Give them the next block in the range and update the remaining
        // number of blocks.
        pbscanwork.phsw_nallocated = pbscanwork.phsw_nallocated.wrapping_add(1);
        nallocated = pbscanwork.phsw_nallocated;
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_remaining.wrapping_sub(1);
    } else {
        // When we've only got PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS chunks
        // remaining in the scan, we half the chunk size. Since we reduce the
        // chunk size here, we'll hit this again after doing
        // PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS at the new size. After a few
        // iterations of this, we'll end up doing the last few blocks with
        // the chunk size set to 1.
        //
        // C computes `phs_nblocks - (phsw_chunk_size * RAMPDOWN)` in 32-bit
        // BlockNumber arithmetic, which wraps; replicate exactly before
        // widening to u64 for the comparison.
        if pbscanwork.phsw_chunk_size > 1
            && pbscanwork.phsw_nallocated
                > bpscan.phs_nblocks.wrapping_sub(
                    pbscanwork
                        .phsw_chunk_size
                        .wrapping_mul(PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS),
                ) as u64
        {
            pbscanwork.phsw_chunk_size >>= 1;
        }

        pbscanwork.phsw_nallocated = bpscan
            .phs_nallocated
            .fetch_add(pbscanwork.phsw_chunk_size as u64);
        nallocated = pbscanwork.phsw_nallocated;

        // Set the remaining number of blocks in this chunk so that
        // subsequent calls from this worker continue on with this chunk
        // until it's done.
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_size.wrapping_sub(1);
    }

    let phs_startblock = bpscan.phs_startblock();

    let page: BlockNumber = if nallocated >= bpscan.phs_nblocks as u64 {
        InvalidBlockNumber // all blocks have been allocated
    } else {
        (nallocated
            .wrapping_add(phs_startblock as u64)
            .wrapping_rem(bpscan.phs_nblocks as u64)) as BlockNumber
    };

    // Report scan location. Normally, we report the current page number.
    // When we reach the end of the scan, though, we report the starting
    // page, not the ending page, just so the starting positions for later
    // scans doesn't slew backwards. We only report the position at the end
    // of the scan once, though: subsequent callers will report nothing.
    if pbscan.phs_syncscan {
        if page != InvalidBlockNumber {
            syncscan_seams::ss_report_location::call(rel.rd_id, page)?;
        } else if nallocated == bpscan.phs_nblocks as u64 {
            syncscan_seams::ss_report_location::call(
                rel.rd_id,
                phs_startblock,
            )?;
        }
    }

    Ok(page)
}


// ===========================================================================
// Helper functions to implement relation sizing for block oriented AMs
// (tableam.c)
// ===========================================================================

/// `table_block_relation_size(rel, forkNumber)`.
///
/// If a table AM uses the various relation forks as the sole place where
/// data is stored, and if it uses them in the expected manner (e.g. the
/// actual data is in the main fork rather than some other), it can use this
/// implementation of the relation_size callback rather than implementing its
/// own.
pub fn table_block_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    let mut nblocks: u64 = 0;

    // RelationGetSmgr(rel) — the smgr handle is the (locator, backend) pair.
    let rlocator = rel.rd_locator;
    let backend = rel.rd_backend;

    // InvalidForkNumber indicates returning the size for all forks
    if forkNumber == InvalidForkNumber {
        // C: `for (int i = 0; i < MAX_FORKNUM; i++)` — i.e. every fork below
        // MAX_FORKNUM (INIT_FORKNUM is excluded).
        for fork in [MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM] {
            nblocks = nblocks.wrapping_add(smgr_seams::smgrnblocks::call(
                rlocator, backend, fork,
            )? as u64);
        }
    } else {
        nblocks =
            smgr_seams::smgrnblocks::call(rlocator, backend, forkNumber)? as u64;
    }

    Ok(nblocks.wrapping_mul(BLCKSZ as u64))
}

/// `table_block_relation_estimate_size(rel, attr_widths, pages, tuples,
/// allvisfrac, overhead_bytes_per_tuple, usable_bytes_per_page)`.
///
/// This function can't be directly used as the implementation of the
/// relation_estimate_size callback, because it has a few additional
/// parameters. Instead, it is intended to be used as a helper function; the
/// caller can pass through the arguments to its relation_estimate_size
/// function plus the additional values required here.
///
/// `overhead_bytes_per_tuple` should contain the approximate number of bytes
/// of storage required to store a tuple above and beyond what is required
/// for the tuple data proper (tuple header, item pointer). This is only used
/// for query planning, so a table AM where the value is not constant could
/// choose to pass a "best guess".
///
/// `usable_bytes_per_page` should contain the approximate number of bytes
/// per page usable for tuple data, excluding the page header and any
/// anticipated special space.
#[allow(clippy::too_many_arguments)]
pub fn table_block_relation_estimate_size(
    rel: &Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
) -> PgResult<()> {
    // it should have storage, so we can call the smgr
    let mut curpages: BlockNumber =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            rel,
            MAIN_FORKNUM,
        )?;

    // coerce values in pg_class to more desirable types
    let relpages = rel.rd_rel.relpages as BlockNumber;
    let reltuples = rel.rd_rel.reltuples as f64;
    let relallvisible = rel.rd_rel.relallvisible as BlockNumber;

    // HACK: if the relation has never yet been vacuumed, use a minimum size
    // estimate of 10 pages. The idea here is to avoid assuming a
    // newly-created table is really small, even if it currently is, because
    // that may not be true once some data gets loaded into it. Once a vacuum
    // or analyze cycle has been done on it, it's more reasonable to believe
    // the size is somewhat stable.
    //
    // (Note that this is only an issue if the plan gets cached and used
    // again after the table has been filled. What we're trying to avoid is
    // using a nestloop-type plan on a table that has grown substantially
    // since the plan was made. Normally, autovacuum/autoanalyze will occur
    // once enough inserts have happened and cause cached-plan invalidation;
    // but that doesn't happen instantaneously, and it won't happen at all
    // for cases such as temporary tables.)
    //
    // We test "never vacuumed" by seeing whether reltuples < 0.
    //
    // If the table has inheritance children, we don't apply this heuristic.
    // Totally empty parent tables are quite common, so we should be willing
    // to believe that they are empty.
    if curpages < 10 && reltuples < 0.0 && !rel.rd_rel.relhassubclass {
        curpages = 10;
    }

    // report estimated # pages
    *pages = curpages;
    // quick exit if rel is clearly empty
    if curpages == 0 {
        *tuples = 0.0;
        *allvisfrac = 0.0;
        return Ok(());
    }

    // estimate number of tuples from previous tuple density
    let density: f64;
    if reltuples >= 0.0 && relpages > 0 {
        density = reltuples / relpages as f64;
    } else {
        // When we have no data because the relation was never yet vacuumed,
        // estimate tuple width from attribute datatypes. We assume here that
        // the pages are completely full, which is OK for tables but is
        // probably an overestimate for indexes. Fortunately
        // get_relation_info() can clamp the overestimate to the parent
        // table's size.
        //
        // Note: this code intentionally disregards alignment considerations,
        // because (a) that would be gilding the lily considering how crude
        // the estimate is, (b) it creates platform dependencies in the
        // default plans which are kind of a headache for regression testing,
        // and (c) different table AMs might use different padding schemes.

        // Without reltuples/relpages, we also need to consider fillfactor.
        // The other branch considers it implicitly by calculating density
        // from actual relpages/reltuples statistics.
        let fillfactor = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);

        let mut tuple_width =
            plancat_seams::get_rel_data_width::call(rel.rd_id, attr_widths)?;
        tuple_width = (tuple_width as usize).wrapping_add(overhead_bytes_per_tuple) as i32;
        // note: integer division is intentional here (C Size arithmetic)
        let raw = usable_bytes_per_page
            .wrapping_mul(fillfactor as usize)
            .wrapping_div(100)
            .wrapping_div(tuple_width as usize);
        // There's at least one row on the page, even with low fillfactor.
        density = costsize_seams::clamp_row_est::call(raw as f64);
    }
    // C uses rint(), which rounds half to even.
    *tuples = (density * curpages as f64).round_ties_even();

    // We use relallvisible as-is, rather than scaling it up like we do for
    // the pages and tuples counts, on the theory that any pages added since
    // the last VACUUM are most likely not marked all-visible. But costsize.c
    // wants it converted to a fraction.
    if relallvisible == 0 || curpages == 0 {
        *allvisfrac = 0.0;
    } else if relallvisible as f64 >= curpages as f64 {
        *allvisfrac = 1.0;
    } else {
        *allvisfrac = relallvisible as f64 / curpages as f64;
    }

    Ok(())
}

/// `HEAP_DEFAULT_FILLFACTOR` (`access/htup_details.h`).
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

#[cfg(test)]
mod tests;
