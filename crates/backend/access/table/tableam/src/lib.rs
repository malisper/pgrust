//! access/table/tableam.c + the tableam.h dispatch layer.
//!
//! C dispatches through `rel->rd_tableam`, a ~45-slot fn-pointer vtable for
//! pluggable AMs. We ship heap only: dispatch is the closed [`TableAm`] enum
//! (rule 4, the types_slot precedent) — each `table_*` wrapper (C surface kept
//! 1:1) is a `match` compiling to a direct call, no vtable load or indirect
//! branch. A second AM = a new variant; exhaustive matches turn every dispatch
//! point into a compile error — never a fn-pointer table. The shared
//! vocabulary (tableam.h/relscan.h types + block parallel-scan helpers) lives
//! in tableam_vocab, below the heap AM crates, and is re-exported here.
//! `mod heap` binds heapam_handler's read lane; DML/analyze/bitmap/sample
//! arms stay loud until their heapam phases land.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use core::ptr::NonNull;
use std::cell::RefCell;
use std::string::String;

use ::heapam::HeapScanDescData;
use ::heapam_handler::IndexFetchHeapData;
use ::mcx::{Mcx, PgVec};
use ::types_snapshot::IsMVCCSnapshot;
use ::types_core::fmgr::NAMEDATALEN;
use ::types_core::primitive::{BlockNumber, Buffer, ForkNumber, TransactionId};
use ::types_core::xact::{CommandId, TransactionIdIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_rel::{
    Relation, HEAP_DEFAULT_FILLFACTOR, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE,
    RELKIND_VIEW,
};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_storage::RelFileLocator;
use ::types_tuple::{ItemPointerData, ItemPointerGetBlockNumber};

pub use ::tableam_vocab::*;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    guc_tables::vars::synchronize_seqscans.install(guc_tables::GucVarAccessors {
        get: synchronize_seqscans,
        set: set_synchronize_seqscans,
    });
    guc_tables::vars::default_table_access_method.install(guc_tables::GucVarAccessors {
        get: || Some(default_table_access_method()),
        set: |v| {
            set_default_table_access_method(v.as_deref().unwrap_or(DEFAULT_TABLE_ACCESS_METHOD))
        },
    });
    guc_tables::hooks::check_default_table_access_method
        .install(check_default_table_access_method);
}

// --- The dispatch-facing scan values (closed per-AM extensions, rule 4) ---

// C's TableScanDesc points at the rs_base embedded in the AM's scan state; by
// value the scan IS the AM extension, tagged by the closed set. Single
// variant: the tag is free and every match is a direct call.
pub enum TableScanDesc<'mcx> {
    Heap(HeapScanDescData<'mcx>),
}

// Tagless while heap is the only AM: per-tuple dispatch costs nothing.
const _: () = assert!(
    core::mem::size_of::<TableScanDesc<'static>>()
        == core::mem::size_of::<HeapScanDescData<'static>>()
);

impl<'mcx> TableScanDesc<'mcx> {
    #[inline]
    pub fn base(&self) -> &TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &h.rs_base,
        }
    }

    #[inline]
    pub fn base_mut(&mut self) -> &mut TableScanDescData<'mcx> {
        match self {
            TableScanDesc::Heap(h) => &mut h.rs_base,
        }
    }
}

// C's IndexFetchTableData base embedded in IndexFetchHeapData, same treatment.
pub enum IndexFetchTableData<'mcx> {
    Heap(IndexFetchHeapData<'mcx>),
}

impl<'mcx> IndexFetchTableData<'mcx> {
    #[inline]
    pub fn rel(&self) -> &Relation<'mcx> {
        match self {
            IndexFetchTableData::Heap(h) => &h.xs_rel,
        }
    }
}

// C dereferences rd_tableam unconditionally: missing AM = C's NULL crash.
#[inline]
fn am(relation: &Relation<'_>) -> TableAm {
    match TableAm::of(relation) {
        Some(am) => am,
        None => no_table_am(relation),
    }
}

#[cold]
#[inline(never)]
fn no_table_am(relation: &Relation<'_>) -> ! {
    panic!(
        "relation \"{}\" has no supported table access method (relam {}); \
         C would dereference NULL rd_tableam",
        relation.name(),
        relation.rd_rel.relam
    )
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("backend-access-tableam reached unported unit: {unit}")
}

// heapam_handler.c's heapam_methods: read lane bound directly onto heapam /
// heapam_handler; the rest panic until their units land.
mod heap {
    use super::*;

    const DML_UNIT: &str = "backend-access-heap-heapam (phase 2 DML)";

    pub(super) fn slot_callbacks(_rel: &Relation<'_>) -> TupleSlotKind {
        TupleSlotKind::BufferHeapTuple
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_begin<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        snapshot: Snapshot<'mcx>,
        nkeys: i32,
        key: PgVec<'mcx, ScanKeyData>,
        parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
        flags: u32,
    ) -> PgResult<TableScanDesc<'mcx>> {
        Ok(TableScanDesc::Heap(::heapam::heap_beginscan(
            mcx, rel, snapshot, nkeys, key, parallel, flags,
        )?))
    }

    pub(super) fn scan_end(scan: HeapScanDescData<'_>) -> PgResult<()> {
        ::heapam::heap_endscan(scan)
    }

    pub(super) fn scan_rescan(
        scan: &mut HeapScanDescData<'_>,
        key: Option<&[ScanKeyData]>,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> PgResult<()> {
        ::heapam::heap_rescan(scan, key, set_params, allow_strat, allow_sync, allow_pagemode)
    }

    #[inline]
    pub(super) fn scan_getnextslot<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        direction: ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        ::heapam::heap_getnextslot(mcx, scan, direction, slot)
    }

    pub(super) fn scan_set_tidrange(
        scan: &mut HeapScanDescData<'_>,
        mintid: &ItemPointerData,
        maxtid: &ItemPointerData,
    ) {
        ::heapam::heap_set_tidrange(scan, mintid, maxtid);
    }

    pub(super) fn scan_getnextslot_tidrange<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        direction: ScanDirection,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        ::heapam::heap_getnextslot_tidrange(mcx, scan, direction, slot)
    }

    pub(super) fn parallelscan_estimate(rel: &Relation<'_>) -> usize {
        table_block_parallelscan_estimate(rel)
    }

    pub(super) fn parallelscan_initialize(
        rel: &Relation<'_>,
        pscan: &mut ParallelBlockTableScanDescData,
    ) -> PgResult<usize> {
        table_block_parallelscan_initialize(rel, pscan)
    }

    pub(super) fn parallelscan_reinitialize(
        rel: &Relation<'_>,
        pscan: &ParallelBlockTableScanDescData,
    ) {
        table_block_parallelscan_reinitialize(rel, pscan);
    }

    pub(super) fn index_delete_tuples<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _delstate: &mut TM_IndexDeleteOp<'mcx>,
    ) -> PgResult<TransactionId> {
        unported("backend-access-heap-heapam (heap_index_delete_tuples)")
    }

    pub(super) fn tuple_insert<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _cid: CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        unported(DML_UNIT)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_insert_speculative<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _cid: CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateData>,
        _spec_token: u32,
    ) -> PgResult<()> {
        unported(DML_UNIT)
    }

    pub(super) fn tuple_complete_speculative<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _spec_token: u32,
        _succeeded: bool,
    ) -> PgResult<()> {
        unported(DML_UNIT)
    }

    pub(super) fn multi_insert<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slots: &mut [&mut SlotData<'mcx>],
        _cid: CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        unported(DML_UNIT)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_delete<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _tid: &ItemPointerData,
        _cid: CommandId,
        _snapshot: &Snapshot<'mcx>,
        _crosscheck: &Snapshot<'mcx>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _changing_part: bool,
    ) -> PgResult<TM_Result> {
        unported(DML_UNIT)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_update<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _otid: &ItemPointerData,
        _slot: &mut SlotData<'mcx>,
        _cid: CommandId,
        _snapshot: &Snapshot<'mcx>,
        _crosscheck: &Snapshot<'mcx>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _lockmode: &mut LockTupleMode,
        _update_indexes: &mut TU_UpdateIndexes,
    ) -> PgResult<TM_Result> {
        unported(DML_UNIT)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tuple_lock<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _tid: &ItemPointerData,
        _snapshot: &Snapshot<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _cid: CommandId,
        _mode: LockTupleMode,
        _wait_policy: LockWaitPolicy,
        _flags: u8,
        _tmfd: &mut TM_FailureData,
    ) -> PgResult<TM_Result> {
        unported(DML_UNIT)
    }

    pub(super) fn relation_set_new_filelocator(
        _rel: &Relation<'_>,
        _newrlocator: &RelFileLocator,
        _persistence: i8,
    ) -> PgResult<(TransactionId, TransactionId)> {
        unported("backend-catalog-storage (RelationCreateStorage)")
    }

    pub(super) fn relation_nontransactional_truncate(_rel: &Relation<'_>) -> PgResult<()> {
        unported("backend-catalog-storage (RelationTruncate)")
    }

    pub(super) fn relation_size(rel: &Relation<'_>, fork_number: ForkNumber) -> PgResult<u64> {
        table_block_relation_size(rel, fork_number)
    }

    pub(super) fn scan_analyze_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut HeapScanDescData<'mcx>,
        _next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
    ) -> PgResult<bool> {
        unported("backend-access-heap-heapam (ANALYZE lane)")
    }

    pub(super) fn scan_analyze_next_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut HeapScanDescData<'mcx>,
        _oldest_xmin: TransactionId,
        _liverows: &mut f64,
        _deadrows: &mut f64,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported("backend-access-heap-heapam (ANALYZE lane)")
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_bitmap_next_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        scan: &mut HeapScanDescData<'mcx>,
        tbm: &tidbitmap::TIDBitmap<'_>,
        iterator: &mut tidbitmap::TbmIterator,
        slot: &mut SlotData<'mcx>,
        recheck: &mut bool,
        lossy_pages: &mut u64,
        exact_pages: &mut u64,
    ) -> PgResult<bool> {
        heapam::bitmap::heap_scan_bitmap_next_tuple(
            mcx, scan, tbm, iterator, slot, recheck, lossy_pages, exact_pages,
        )
    }

    pub(super) fn scan_sample_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut HeapScanDescData<'mcx>,
        _scanstate: &mut dyn SampleScanDriver,
    ) -> PgResult<bool> {
        unported("backend-access-heap-heapam (sample scan lane)")
    }

    pub(super) fn scan_sample_next_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut HeapScanDescData<'mcx>,
        _scanstate: &mut dyn SampleScanDriver,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported("backend-access-heap-heapam (sample scan lane)")
    }
}

// --- GUC variables (tableam.c) + check hook (tableamapi.c) ---

pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

thread_local! {
    // Cold GUC store: bare String = guc.c's malloc'd string, outlives any mcx.
    static DEFAULT_TABLE_ACCESS_METHOD_GUC: RefCell<String> =
        RefCell::new(String::from(DEFAULT_TABLE_ACCESS_METHOD));
}

pub fn default_table_access_method() -> String {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| v.borrow().clone())
}

pub fn set_default_table_access_method(value: &str) {
    DEFAULT_TABLE_ACCESS_METHOD_GUC.with(|v| {
        let mut s = v.borrow_mut();
        s.clear();
        s.push_str(value);
    });
}

fn check_default_table_access_method(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: ::types_guc::GucSource,
) -> PgResult<bool> {
    let name = newval.as_deref().unwrap_or("");

    if name.is_empty() {
        return Err(Box::new(PgError::error(
            "\"default_table_access_method\" cannot be empty.",
        )));
    }

    if name.len() >= NAMEDATALEN as usize {
        return Err(Box::new(PgError::error(format!(
            "\"default_table_access_method\" is too long (maximum {} characters).",
            NAMEDATALEN - 1
        ))));
    }

    // C probes get_table_am_oid when IsTransactionState() && MyDatabaseId
    // valid; both unported, so every caller is on C's accept-on-faith path.
    Ok(true)
}

// --- Shared helpers ---

// CheckXidAlive (xact.c) is set only during logical decoding, unported; the
// statically-invalid xid computes the same `unlikely()` false as C.
fn unexpected_during_logical_decoding() -> bool {
    const CHECK_XID_ALIVE: TransactionId = ::types_core::xact::InvalidTransactionId;
    TransactionIdIsValid(CHECK_XID_ALIVE)
}

fn elog_error(message: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(message))
}

// shmem.c add_size, private mirror of the unported helper.
fn add_size(s1: usize, s2: usize) -> PgResult<usize> {
    s1.checked_add(s2).ok_or_else(|| {
        Box::new(
            PgError::error("requested shared memory size overflows size_t")
                .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        )
    })
}

// --- Slot functions (tableam.c) ---

pub fn table_slot_callbacks(relation: &Relation<'_>) -> TupleSlotKind {
    if let Some(am) = TableAm::of(relation) {
        match am {
            TableAm::Heap => heap::slot_callbacks(relation),
        }
    } else if relation.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        // FDWs historically expect heap tuples in their slots.
        TupleSlotKind::HeapTuple
    } else {
        debug_assert!({
            let relkind = relation.rd_rel.relkind;
            relkind == RELKIND_VIEW || relkind == RELKIND_PARTITIONED_TABLE
        });
        TupleSlotKind::Virtual
    }
}

pub fn table_slot_create<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<SlotData<'mcx>> {
    let tts_cb = table_slot_callbacks(relation);
    // MakeSingleTupleTableSlot(RelationGetDescr(relation), tts_cb)
    Ok(exectuples::make_tuple_table_slot(
        mcx,
        tts_cb,
        Some(relation.rd_att.clone()),
    ))
}

// --- Table scan functions (tableam.h wrappers + tableam.c) ---

pub fn table_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_strat<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
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
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_catalog<'mcx>(
    _mcx: Mcx<'mcx>,
    _relation: &Relation<'mcx>,
    _nkeys: i32,
    _key: PgVec<'mcx, ScanKeyData>,
) -> PgResult<TableScanDesc<'mcx>> {
    // RegisterSnapshot(GetCatalogSnapshot(relid)) + scan_begin(SEQSCAN|STRAT|SYNC|PAGEMODE|TEMP_SNAPSHOT).
    unported("backend-utils-time-snapmgr (GetCatalogSnapshot/RegisterSnapshot)")
}

pub fn table_beginscan_bm<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_BITMAPSCAN | SO_ALLOW_PAGEMODE;
    match am(rel) {
        TableAm::Heap => heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_beginscan_sampling<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
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
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, snapshot, nkeys, key, None, flags),
    }
}

pub fn table_beginscan_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDSCAN;
    match am(rel) {
        TableAm::Heap => heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags),
    }
}

pub fn table_beginscan_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_ANALYZE;
    match am(relation) {
        TableAm::Heap => heap::scan_begin(mcx, relation, None, 0, PgVec::new_in(mcx), None, flags),
    }
}

pub fn table_endscan(scan: TableScanDesc<'_>) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_end(h),
    }
}

pub fn table_rescan<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    key: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_rescan(h, key, false, false, false, false),
    }
}

pub fn table_rescan_set_params<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    key: Option<&[ScanKeyData]>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_rescan(h, key, true, allow_strat, allow_sync, allow_pagemode)
        }
    }
}

// Per-tuple at M2: the single-variant match monomorphizes to the direct
// heap_getnextslot call — zero glue over heapam's loop.
#[inline]
pub fn table_scan_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_getnextslot(mcx, h, direction, slot),
    }
}

pub fn table_beginscan_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<TableScanDesc<'mcx>> {
    let flags = SO_TYPE_TIDRANGESCAN | SO_ALLOW_PAGEMODE;
    match am(rel) {
        TableAm::Heap => {
            let mut sscan =
                heap::scan_begin(mcx, rel, snapshot, 0, PgVec::new_in(mcx), None, flags)?;
            let TableScanDesc::Heap(h) = &mut sscan;
            heap::scan_set_tidrange(h, mintid, maxtid);
            Ok(sscan)
        }
    }
}

pub fn table_rescan_tidrange<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<()> {
    debug_assert!((scan.base().rs_flags & SO_TYPE_TIDRANGESCAN) != 0);
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_rescan(h, None, false, false, false, false)?;
            heap::scan_set_tidrange(h, mintid, maxtid);
            Ok(())
        }
    }
}

pub fn table_scan_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_getnextslot_tidrange(mcx, h, direction, slot),
    }
}

// --- Parallel table scan (tableam.c) ---

pub fn table_parallelscan_estimate(
    rel: &Relation<'_>,
    snapshot: &Snapshot<'_>,
) -> PgResult<usize> {
    let mut sz: usize = 0;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            unported("backend-utils-time-snapmgr (EstimateSnapshotSpace)")
        }
        _ => debug_assert!(snapshot.is_none()), // Assert(snapshot == SnapshotAny)
    }

    let am_sz = match am(rel) {
        TableAm::Heap => heap::parallelscan_estimate(rel),
    };
    sz = add_size(sz, am_sz)?;

    Ok(sz)
}

pub fn table_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
    _snapshot_buf: &mut [u8],
    snapshot: &Snapshot<'_>,
) -> PgResult<()> {
    let snapshot_off = match am(rel) {
        TableAm::Heap => heap::parallelscan_initialize(rel, pscan)?,
    };
    pscan.phs_snapshot_off = snapshot_off;

    match snapshot {
        Some(s) if IsMVCCSnapshot(s) => {
            unported("backend-utils-time-snapmgr (SerializeSnapshot)")
        }
        _ => {
            debug_assert!(snapshot.is_none());
            pscan.phs_snapshot_any = true;
        }
    }

    Ok(())
}

pub fn table_parallelscan_reinitialize(
    rel: &Relation<'_>,
    pscan: &ParallelBlockTableScanDescData,
) {
    match am(rel) {
        TableAm::Heap => heap::parallelscan_reinitialize(rel, pscan),
    }
}

pub fn table_beginscan_parallel<'mcx>(
    _mcx: Mcx<'mcx>,
    _relation: &Relation<'mcx>,
    _pscan: NonNull<ParallelBlockTableScanDescData>,
) -> PgResult<TableScanDesc<'mcx>> {
    // Restore+Register serialized snapshot, scan_begin(.., pscan, SEQSCAN|STRAT|SYNC|PAGEMODE[|TEMP_SNAPSHOT]).
    unported("backend-utils-time-snapmgr (RestoreSnapshot/RegisterSnapshot)")
}

// --- Index scan related functions (tableam.h / tableam.c) ---

pub fn table_index_fetch_begin<'mcx>(rel: &Relation<'mcx>) -> IndexFetchTableData<'mcx> {
    match am(rel) {
        TableAm::Heap => {
            IndexFetchTableData::Heap(::heapam_handler::heapam_index_fetch_begin(rel))
        }
    }
}

pub fn table_index_fetch_reset(scan: &mut IndexFetchTableData<'_>) {
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_reset(h),
    }
}

pub fn table_index_fetch_end(scan: IndexFetchTableData<'_>) {
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_end(h),
    }
}

pub fn table_index_fetch_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexFetchTableData<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
    call_again: &mut bool,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_index_fetch_tuple call during logical decoding",
        ));
    }
    match scan {
        IndexFetchTableData::Heap(h) => ::heapam_handler::heapam_index_fetch_tuple(
            mcx, h, tid, snapshot, slot, call_again, all_dead,
        ),
    }
}

pub fn table_index_fetch_tuple_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &mut ItemPointerData,
    snapshot: &mut Snapshot<'mcx>,
    all_dead: Option<&mut bool>,
) -> PgResult<bool> {
    let mut call_again = false;

    let mut slot = table_slot_create(mcx, rel)?;
    let mut scan = table_index_fetch_begin(rel);
    let found = table_index_fetch_tuple(
        mcx,
        &mut scan,
        tid,
        snapshot,
        &mut slot,
        &mut call_again,
        all_dead,
    )?;
    table_index_fetch_end(scan);
    // ExecDropSingleTupleTableSlot: the owned slot drops here.
    exectuples::exec_clear_tuple(&mut slot, mcx);

    Ok(found)
}

pub fn table_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    delstate: &mut TM_IndexDeleteOp<'mcx>,
) -> PgResult<TransactionId> {
    match am(rel) {
        TableAm::Heap => heap::index_delete_tuples(mcx, rel, delstate),
    }
}

// --- Non-modifying operations on individual tuples ---

pub fn table_tuple_fetch_row_version<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_fetch_row_version call during logical decoding",
        ));
    }
    match am(rel) {
        TableAm::Heap => {
            ::heapam_handler::heapam_fetch_row_version(mcx, rel, tid, snapshot, slot)
        }
    }
}

pub fn table_tuple_tid_valid(scan: &mut TableScanDesc<'_>, tid: &ItemPointerData) -> bool {
    match scan {
        TableScanDesc::Heap(h) => ::heapam_handler::heapam_tuple_tid_valid(h, tid),
    }
}

pub fn table_tuple_get_latest_tid<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_get_latest_tid call during logical decoding",
        ));
    }

    // User-supplied TID: don't trust the input too much.
    if !table_tuple_tid_valid(scan, tid) {
        let blk = ItemPointerGetBlockNumber(tid);
        let off = tid.ip_posid;
        let relname = scan.base().rs_rd.name();
        return Err(Box::new(
            PgError::error(format!(
                "tid ({blk}, {off}) is not valid for relation \"{relname}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    match scan {
        TableScanDesc::Heap(h) => ::heapam_handler::heapam_tuple_get_latest_tid(h, tid),
    }
}

pub fn table_tuple_satisfies_snapshot<'mcx>(
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<bool> {
    match am(rel) {
        TableAm::Heap => ::heapam_handler::heapam_tuple_satisfies_snapshot(rel, slot, snapshot),
    }
}

// --- Manipulations of physical tuples (tableam.h wrappers) ---

pub fn table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::tuple_insert(mcx, rel, slot, cid, options, bistate),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_insert_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
    spec_token: u32,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => {
            heap::tuple_insert_speculative(mcx, rel, slot, cid, options, bistate, spec_token)
        }
    }
}

pub fn table_tuple_complete_speculative<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    spec_token: u32,
    succeeded: bool,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::tuple_complete_speculative(mcx, rel, slot, spec_token, succeeded),
    }
}

pub fn table_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slots: &mut [&mut SlotData<'mcx>],
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::multi_insert(mcx, rel, slots, cid, options, bistate),
    }
}

// heapam_methods leaves the optional finish_bulk_insert slot NULL, so for the
// only shipped AM the C inline is a no-op after the rd_tableam probe.
pub fn table_finish_bulk_insert(rel: &Relation<'_>, _options: i32) -> PgResult<()> {
    let _ = am(rel);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    cid: CommandId,
    snapshot: &Snapshot<'mcx>,
    crosscheck: &Snapshot<'mcx>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    changingPart: bool,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_delete(
            mcx,
            rel,
            tid,
            cid,
            snapshot,
            crosscheck,
            wait,
            tmfd,
            changingPart,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    snapshot: &Snapshot<'mcx>,
    crosscheck: &Snapshot<'mcx>,
    wait: bool,
    tmfd: &mut TM_FailureData,
    lockmode: &mut LockTupleMode,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_update(
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
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn table_tuple_lock<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    flags: u8,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    match am(rel) {
        TableAm::Heap => heap::tuple_lock(
            mcx,
            rel,
            tid,
            snapshot,
            slot,
            cid,
            mode,
            wait_policy,
            flags,
            tmfd,
        ),
    }
}

// --- DDL-adjacent dispatch (tableam.h wrappers) ---

pub fn table_relation_set_new_filelocator(
    rel: &Relation<'_>,
    newrlocator: &RelFileLocator,
    relpersistence: i8,
) -> PgResult<(TransactionId, TransactionId)> {
    match am(rel) {
        TableAm::Heap => heap::relation_set_new_filelocator(rel, newrlocator, relpersistence),
    }
}

pub fn table_relation_nontransactional_truncate(rel: &Relation<'_>) -> PgResult<()> {
    match am(rel) {
        TableAm::Heap => heap::relation_nontransactional_truncate(rel),
    }
}

pub fn table_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    match am(rel) {
        TableAm::Heap => heap::relation_size(rel, forkNumber),
    }
}

// --- ANALYZE / bitmap / sample scan dispatch (tableam.h wrappers) ---

pub fn table_scan_analyze_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => heap::scan_analyze_next_block(mcx, h, next_buffer),
    }
}

pub fn table_scan_analyze_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    oldest_xmin: TransactionId,
    liverows: &mut f64,
    deadrows: &mut f64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_analyze_next_tuple(mcx, h, oldest_xmin, liverows, deadrows, slot)
        }
    }
}

// C divergence: the TBM iterator + bitmap arrive as parameters (C rides them
// in rs_base.st.rs_tbmiterator; ours carries no bitmap back-pointer).
#[allow(clippy::too_many_arguments)]
pub fn table_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    tbm: &tidbitmap::TIDBitmap<'_>,
    iterator: &mut tidbitmap::TbmIterator,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    match scan {
        TableScanDesc::Heap(h) => {
            heap::scan_bitmap_next_tuple(mcx, h, tbm, iterator, slot, recheck, lossy_pages, exact_pages)
        }
    }
}

pub fn table_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_block call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => heap::scan_sample_next_block(mcx, h, scanstate),
    }
}

pub fn table_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDesc<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_tuple call during logical decoding",
        ));
    }
    match scan {
        TableScanDesc::Heap(h) => heap::scan_sample_next_tuple(mcx, h, scanstate, slot),
    }
}

// --- Functions to make modifications a bit simpler (tableam.c) ---

pub fn simple_table_tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    let cid = xact_seams::get_current_command_id::call(true)?;
    table_tuple_insert(mcx, rel, slot, cid, 0, None)
}

pub fn simple_table_tuple_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();

    let cid = xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_delete(
        mcx, rel, tid, cid, snapshot, &None, /* InvalidSnapshot */
        true,  /* wait for commit */
        &mut tmfd, false, /* changingPart */
    )?;

    match result {
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_delete status: {}",
            other as u32
        ))),
    }
}

pub fn simple_table_tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
    update_indexes: &mut TU_UpdateIndexes,
) -> PgResult<()> {
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;

    let cid = xact_seams::get_current_command_id::call(true)?;
    let result = table_tuple_update(
        mcx,
        rel,
        otid,
        slot,
        cid,
        snapshot,
        &None, /* InvalidSnapshot */
        true,  /* wait for commit */
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    )?;

    match result {
        TM_Result::TM_SelfModified => Err(elog_error("tuple already updated by self")),
        TM_Result::TM_Ok => Ok(()),
        TM_Result::TM_Updated => Err(elog_error("tuple concurrently updated")),
        TM_Result::TM_Deleted => Err(elog_error("tuple concurrently deleted")),
        other => Err(elog_error(format!(
            "unrecognized table_tuple_update status: {}",
            other as u32
        ))),
    }
}

// --- Parallel-scan sizing for block-oriented AMs (tableam.c) ---

pub fn table_block_parallelscan_estimate(_rel: &Relation<'_>) -> usize {
    core::mem::size_of::<ParallelBlockTableScanDescData>()
}

pub fn table_block_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
) -> PgResult<usize> {
    use core::sync::atomic::{AtomicU32, AtomicU64};
    use ::types_core::primitive::InvalidBlockNumber;
    use ::types_storage::Spinlock;

    pscan.phs_locator = relation_locator(rel);
    let phs_nblocks = relation_nblocks(rel)?;
    pscan.phs_nblocks = phs_nblocks;
    // compare phs_syncscan initialization to similar logic in initscan
    pscan.phs_syncscan = synchronize_seqscans()
        && !rel.uses_local_buffers()
        && phs_nblocks > (nbuffers()? / 4) as BlockNumber;
    pscan.phs_mutex = Spinlock::new();
    pscan.phs_startblock = AtomicU32::new(InvalidBlockNumber);
    pscan.phs_nallocated = AtomicU64::new(0);

    Ok(core::mem::size_of::<ParallelBlockTableScanDescData>())
}

pub fn table_block_parallelscan_reinitialize(
    _rel: &Relation<'_>,
    pscan: &ParallelBlockTableScanDescData,
) {
    pscan
        .phs_nallocated
        .store(0, core::sync::atomic::Ordering::SeqCst);
}

// --- Relation-sizing helpers for block-oriented AMs (tableam.c) ---

pub fn table_block_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    let h = smgr::RelationGetSmgr(rel)?;
    let mut nblocks: u64 = 0;
    if forkNumber == ForkNumber::InvalidForkNumber {
        // C sums forks i < MAX_FORKNUM, i.e. INIT_FORKNUM excluded (bug-compat).
        for fork in [
            ForkNumber::MAIN_FORKNUM,
            ForkNumber::FSM_FORKNUM,
            ForkNumber::VISIBILITYMAP_FORKNUM,
        ] {
            nblocks += smgr::smgrnblocks_h(h, fork)? as u64;
        }
    } else {
        nblocks = smgr::smgrnblocks_h(h, forkNumber)? as u64;
    }
    Ok(nblocks * ::types_core::BLCKSZ as u64)
}

/// `table_relation_estimate_size` for the heap AM (heapam_estimate_rel_size ->
/// table_block_relation_estimate_size). `get_rel_data_width` lives in the
/// planner's plancat (a direct dep here would cycle), so the never-vacuumed
/// density fallback arrives as `data_width` (called with the attr-width cache).
#[allow(clippy::too_many_arguments)]
pub fn table_relation_estimate_size(
    rel: &Relation<'_>,
    overhead_bytes_per_tuple: usize,
    usable_bytes_per_page: usize,
    data_width: impl FnOnce(Option<&mut [i32]>) -> PgResult<i32>,
    mut attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
) -> PgResult<()> {
    let curpages =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
    let fillfactor = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);
    block_relation_estimate_size_math(
        curpages,
        rel.rd_rel.relpages as BlockNumber,
        rel.rd_rel.reltuples as f64,
        rel.rd_rel.relallvisible as BlockNumber,
        rel.rd_rel.relhassubclass,
        |aw| {
            let mut tuple_width = data_width(aw)? as usize;
            tuple_width += overhead_bytes_per_tuple;
            // C Size arithmetic: integer division is intentional.
            let raw = usable_bytes_per_page * fillfactor as usize / 100 / tuple_width;
            Ok(clamp_row_est(raw as f64))
        },
        attr_widths.take(),
        pages,
        tuples,
        allvisfrac,
    )
}

// clamp_row_est (costsize.c); grounded here for the density fallback (the
// costsize copy lives with the planner).
fn clamp_row_est(nrows: f64) -> f64 {
    const MAXIMUM_ROWCOUNT: f64 = 1e100;
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round_ties_even()
    }
}

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
    let curpages = relation_nblocks(rel)?;
    let _ = rel.get_fillfactor(HEAP_DEFAULT_FILLFACTOR);
    let _ = (overhead_bytes_per_tuple, usable_bytes_per_page);
    block_relation_estimate_size_math(
        curpages,
        rel.rd_rel.relpages as BlockNumber,
        rel.rd_rel.reltuples as f64,
        rel.rd_rel.relallvisible as BlockNumber,
        rel.rd_rel.relhassubclass,
        // Never-vacuumed density fallback needs planner units, unported.
        |_aw| unported("backend-optimizer-util-plancat (get_rel_data_width)"),
        attr_widths,
        pages,
        tuples,
        allvisfrac,
    )
}

fn block_relation_estimate_size_math(
    mut curpages: BlockNumber,
    relpages: BlockNumber,
    reltuples: f64,
    relallvisible: BlockNumber,
    relhassubclass: bool,
    density_fallback: impl FnOnce(Option<&mut [i32]>) -> PgResult<f64>,
    attr_widths: Option<&mut [i32]>,
    pages: &mut BlockNumber,
    tuples: &mut f64,
    allvisfrac: &mut f64,
) -> PgResult<()> {
    // Never-vacuumed (reltuples < 0) tables get a 10-page floor (cached-plan
    // guard); skipped when there are inheritance children.
    if curpages < 10 && reltuples < 0.0 && !relhassubclass {
        curpages = 10;
    }

    *pages = curpages;
    if curpages == 0 {
        *tuples = 0.0;
        *allvisfrac = 0.0;
        return Ok(());
    }

    let density: f64 = if reltuples >= 0.0 && relpages > 0 {
        reltuples / relpages as f64
    } else {
        density_fallback(attr_widths)?
    };
    // C rint(): round half to even.
    *tuples = (density * curpages as f64).round_ties_even();

    // relallvisible is used as-is (pages added since VACUUM are likely not
    // all-visible), converted to the fraction costsize.c wants.
    if relallvisible == 0 || curpages == 0 {
        *allvisfrac = 0.0;
    } else if relallvisible as f64 >= curpages as f64 {
        *allvisfrac = 1.0;
    } else {
        *allvisfrac = relallvisible as f64 / curpages as f64;
    }

    Ok(())
}

// --- Unported providers, loud per rule 5 ---

fn relation_locator(rel: &Relation<'_>) -> RelFileLocator {
    bufmgr_seams::relation_smgr_locator::call(rel).locator
}

fn relation_nblocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)
}

fn nbuffers() -> PgResult<i32> {
    unported("NBuffers GUC wiring (backend-storage-buffer-bufmgr)")
}
