//! access/table/tableam.c + the tableam.h dispatch layer.
//!
//! C dispatches through `rel->rd_tableam`, a ~45-slot fn-pointer vtable for
//! pluggable AMs. We ship heap only: dispatch is the closed [`TableAm`] enum
//! (rule 4, the types_slot precedent) — each `table_*` wrapper (C surface kept
//! 1:1) is a `match` compiling to a direct call, no vtable load or indirect
//! branch. A second AM = a new variant; exhaustive matches turn every dispatch
//! point into a compile error — never a fn-pointer table. `mod heap` arms
//! panic until backend-access-heap-heapam-handler replaces their bodies with
//! direct calls (its scan-state tail extends TableScanDescData then too).

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use core::ptr::NonNull;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::string::String;

use ::mcx::{Mcx, PgVec};
use ::types_snapshot::{IsMVCCSnapshot, SnapshotData};
use ::types_core::fmgr::NAMEDATALEN;
use ::types_core::primitive::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, MaxBlockNumber, OffsetNumber, Oid,
    TransactionId,
};
use ::types_core::xact::{CommandId, TransactionIdIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_rel::{
    Relation, HEAP_DEFAULT_FILLFACTOR, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE,
    RELKIND_VIEW,
};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_storage::{RelFileLocator, Spinlock};
use ::types_tuple::{ItemPointerData, ItemPointerGetBlockNumber};

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

// --- tableam.h vocabulary ---

pub const SO_TYPE_SEQSCAN: u32 = 1 << 0;
pub const SO_TYPE_BITMAPSCAN: u32 = 1 << 1;
pub const SO_TYPE_SAMPLESCAN: u32 = 1 << 2;
pub const SO_TYPE_TIDSCAN: u32 = 1 << 3;
pub const SO_TYPE_TIDRANGESCAN: u32 = 1 << 4;
pub const SO_TYPE_ANALYZE: u32 = 1 << 5;
pub const SO_ALLOW_STRAT: u32 = 1 << 6;
pub const SO_ALLOW_SYNC: u32 = 1 << 7;
pub const SO_ALLOW_PAGEMODE: u32 = 1 << 8;
pub const SO_TEMP_SNAPSHOT: u32 = 1 << 9;

pub const TABLE_INSERT_SKIP_FSM: i32 = 0x0002;
pub const TABLE_INSERT_FROZEN: i32 = 0x0004;
pub const TABLE_INSERT_NO_LOGICAL: i32 = 0x0008;

pub const TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: u8 = 1 << 0;
pub const TUPLE_LOCK_FLAG_FIND_LAST_VERSION: u8 = 1 << 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TM_Result {
    TM_Ok = 0,
    TM_Invisible,
    TM_SelfModified,
    TM_Updated,
    TM_Deleted,
    TM_BeingModified,
    TM_WouldBlock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TU_UpdateIndexes {
    TU_None = 0,
    TU_All,
    TU_Summarizing,
}

// nodes/lockoptions.h
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum LockTupleMode {
    LockTupleKeyShare = 0,
    LockTupleShare,
    LockTupleNoKeyExclusive,
    LockTupleExclusive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum LockWaitPolicy {
    LockWaitBlock = 0,
    LockWaitSkip,
    LockWaitError,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    pub cmax: CommandId,
    pub traversed: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct TM_IndexDelete {
    pub tid: ItemPointerData,
    pub id: i16,
}

#[derive(Clone, Copy, Debug)]
pub struct TM_IndexStatus {
    pub idxoffnum: OffsetNumber,
    pub knowndeletable: bool,
    pub promising: bool,
    pub freespace: i16,
}

pub struct TM_IndexDeleteOp<'mcx> {
    pub irel: Relation<'mcx>,
    pub iblknum: BlockNumber,
    pub bottomup: bool,
    pub bottomupfreespace: i32,
    pub ndeltids: i32,
    pub deltids: PgVec<'mcx, TM_IndexDelete>,
    pub status: PgVec<'mcx, TM_IndexStatus>,
}

// C `Snapshot`: None is InvalidSnapshot/SnapshotAny; Rc because snapmgr
// refcounts registered snapshots (RegisterSnapshot), per sharing rule 2.3.
pub type Snapshot<'mcx> = Option<Rc<SnapshotData<'mcx>>>;

// relscan.h TableScanDescData. Heap's tail (HeapScanDescData) lands with the
// heapam port as a closed per-AM extension, not an erased void*; the C
// bitmap-scan union member (TBMIterator) lands with its unit.
pub struct TableScanDescData<'mcx> {
    pub rs_rd: Relation<'mcx>,
    pub rs_snapshot: Snapshot<'mcx>,
    pub rs_nkeys: i32,
    pub rs_key: PgVec<'mcx, ScanKeyData>,
    pub rs_mintid: ItemPointerData,
    pub rs_maxtid: ItemPointerData,
    pub rs_flags: u32,
    pub rs_parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
    // Rule-4 carrier, resolved once at scan_begin; SET ACCESS METHOD needs
    // AccessExclusiveLock, so it cannot change under a live scan.
    pub rs_am: TableAm,
}

pub type TableScanDesc<'mcx> = TableScanDescData<'mcx>;

// ParallelTableScanDescData folded into its only (block-oriented) subclass.
pub struct ParallelBlockTableScanDescData {
    pub phs_locator: RelFileLocator,
    pub phs_syncscan: bool,
    pub phs_snapshot_any: bool,
    pub phs_snapshot_off: usize,
    pub phs_nblocks: BlockNumber,
    pub phs_mutex: Spinlock,
    // Written only under phs_mutex, then read lock-free (C reads it plain).
    pub phs_startblock: AtomicU32,
    pub phs_nallocated: AtomicU64,
}

#[derive(Debug, Default)]
pub struct ParallelBlockTableScanWorkerData {
    pub phsw_nallocated: u64,
    pub phsw_chunk_remaining: u32,
    pub phsw_chunk_size: u32,
}

pub struct IndexFetchTableData<'mcx> {
    pub rel: Relation<'mcx>,
}

// hio.h BulkInsertStateData — C forward-declares it opaquely in tableam.h;
// the real body lands with backend-access-heap-hio.
pub struct BulkInsertStateData {
    _opaque: (),
}

// tsmapi.h NextSampleBlock/NextSampleTuple capability of SampleScanState.
// Tablesample methods are an OPEN extension point in C, so dyn is faithful
// (and cold).
pub trait SampleScanDriver {
    fn has_next_sample_block(&self) -> bool;
    fn next_sample_block(&mut self, nblocks: BlockNumber) -> BlockNumber;
    fn next_sample_tuple(&mut self, blockno: BlockNumber, maxoffset: OffsetNumber)
        -> OffsetNumber;
}

// --- Dispatch: the closed AM set ---

pub const HEAP_TABLE_AM_OID: Oid = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableAm {
    Heap,
}

impl TableAm {
    // C resolves rd_rel.relam -> rd_tableam once per relcache entry; here one
    // load+compare per call, and per-tuple paths read the rs_am carrier.
    #[inline]
    pub fn of(relation: &Relation<'_>) -> Option<TableAm> {
        match relation.rd_rel.relam {
            HEAP_TABLE_AM_OID => Some(TableAm::Heap),
            _ => None,
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

// heapam_handler.c's heapam_methods. Bodies become direct calls into the heap
// AM crates when they port; the wrappers above them are final.
mod heap {
    use super::*;

    const UNIT: &str = "backend-access-heap-heapam-handler";

    pub(super) fn slot_callbacks(_rel: &Relation<'_>) -> TupleSlotKind {
        TupleSlotKind::BufferHeapTuple
    }

    pub(super) fn scan_begin<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _snapshot: Snapshot<'mcx>,
        _nkeys: i32,
        _key: PgVec<'mcx, ScanKeyData>,
        _parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
        _flags: u32,
    ) -> PgResult<TableScanDesc<'mcx>> {
        unported(UNIT)
    }

    pub(super) fn scan_end(_scan: TableScanDesc<'_>) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn scan_rescan<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _key: Option<&[ScanKeyData]>,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn scan_getnextslot<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _direction: ScanDirection,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn scan_set_tidrange(
        _scan: &mut TableScanDescData<'_>,
        _mintid: &ItemPointerData,
        _maxtid: &ItemPointerData,
    ) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn scan_getnextslot_tidrange<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _direction: ScanDirection,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
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

    pub(super) fn index_fetch_begin<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
    ) -> PgResult<Box<IndexFetchTableData<'mcx>>> {
        unported(UNIT)
    }

    pub(super) fn index_fetch_reset(_scan: &mut IndexFetchTableData<'_>) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn index_fetch_end(_scan: Box<IndexFetchTableData<'_>>) -> PgResult<()> {
        unported(UNIT)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn index_fetch_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut IndexFetchTableData<'mcx>,
        _tid: &mut ItemPointerData,
        _snapshot: &mut Snapshot<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _call_again: &mut bool,
        _all_dead: Option<&mut bool>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn index_delete_tuples<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _delstate: &mut TM_IndexDeleteOp<'mcx>,
    ) -> PgResult<TransactionId> {
        unported(UNIT)
    }

    pub(super) fn tuple_fetch_row_version<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _tid: &ItemPointerData,
        _snapshot: &Snapshot<'mcx>,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn tuple_tid_valid(
        _scan: &mut TableScanDescData<'_>,
        _tid: &ItemPointerData,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn tuple_get_latest_tid<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _tid: &mut ItemPointerData,
    ) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn tuple_satisfies_snapshot<'mcx>(
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _snapshot: &Snapshot<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn tuple_insert<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _cid: CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        unported(UNIT)
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
        unported(UNIT)
    }

    pub(super) fn tuple_complete_speculative<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _spec_token: u32,
        _succeeded: bool,
    ) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn multi_insert<'mcx>(
        _mcx: Mcx<'mcx>,
        _rel: &Relation<'mcx>,
        _slots: &mut [&mut SlotData<'mcx>],
        _cid: CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateData>,
    ) -> PgResult<()> {
        unported(UNIT)
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
        unported(UNIT)
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
        unported(UNIT)
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
        unported(UNIT)
    }

    pub(super) fn relation_set_new_filelocator(
        _rel: &Relation<'_>,
        _newrlocator: &RelFileLocator,
        _persistence: i8,
    ) -> PgResult<(TransactionId, TransactionId)> {
        unported(UNIT)
    }

    pub(super) fn relation_nontransactional_truncate(_rel: &Relation<'_>) -> PgResult<()> {
        unported(UNIT)
    }

    pub(super) fn relation_size(rel: &Relation<'_>, fork_number: ForkNumber) -> PgResult<u64> {
        table_block_relation_size(rel, fork_number)
    }

    pub(super) fn scan_analyze_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn scan_analyze_next_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _oldest_xmin: TransactionId,
        _liverows: &mut f64,
        _deadrows: &mut f64,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn scan_bitmap_next_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _slot: &mut SlotData<'mcx>,
        _recheck: &mut bool,
        _lossy_pages: &mut u64,
        _exact_pages: &mut u64,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn scan_sample_next_block<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _scanstate: &mut dyn SampleScanDriver,
    ) -> PgResult<bool> {
        unported(UNIT)
    }

    pub(super) fn scan_sample_next_tuple<'mcx>(
        _mcx: Mcx<'mcx>,
        _scan: &mut TableScanDescData<'mcx>,
        _scanstate: &mut dyn SampleScanDriver,
        _slot: &mut SlotData<'mcx>,
    ) -> PgResult<bool> {
        unported(UNIT)
    }
}

// --- GUC variables (tableam.c) + check hook (tableamapi.c) ---

pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

thread_local! {
    // Cold GUC store: bare String = guc.c's malloc'd string, outlives any mcx.
    static DEFAULT_TABLE_ACCESS_METHOD_GUC: RefCell<String> =
        RefCell::new(String::from(DEFAULT_TABLE_ACCESS_METHOD));
    static SYNCHRONIZE_SEQSCANS_GUC: Cell<bool> = const { Cell::new(true) };
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

pub fn synchronize_seqscans() -> bool {
    SYNCHRONIZE_SEQSCANS_GUC.with(Cell::get)
}

pub fn set_synchronize_seqscans(value: bool) {
    SYNCHRONIZE_SEQSCANS_GUC.with(|v| v.set(value));
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

const PARALLEL_SEQSCAN_NCHUNKS: u32 = 2048;
const PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS: u32 = 64;
const PARALLEL_SEQSCAN_MAX_CHUNK_SIZE: u32 = 8192;

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

// pg_bitutils.h pg_nextpower2_32; valid for num in [1, 2^31].
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0);
    if num & num.wrapping_sub(1) == 0 {
        return num;
    }
    1u32 << (31 - num.leading_zeros() + 1)
}

struct SpinLockGuard<'a> {
    lock: &'a Spinlock,
}

impl<'a> SpinLockGuard<'a> {
    fn acquire(lock: &'a Spinlock) -> Self {
        if lock.tas() != 0 {
            let mut delay =
                s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "phs_mutex");
            while lock.tas_spin() != 0 {
                s_lock_seams::perform_spin_delay::call(&mut delay);
            }
            s_lock_seams::finish_spin_delay::call(&delay);
        }
        SpinLockGuard { lock }
    }
}

impl Drop for SpinLockGuard<'_> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
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
    _mcx: Mcx<'mcx>,
    relation: &Relation<'_>,
) -> PgResult<SlotData<'mcx>> {
    let _tts_cb = table_slot_callbacks(relation);
    // MakeSingleTupleTableSlot(RelationGetDescr(relation), tts_cb)
    unported("backend-executor-exectuples (MakeSingleTupleTableSlot)")
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
    match scan.rs_am {
        TableAm::Heap => heap::scan_end(scan),
    }
}

pub fn table_rescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    key: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    match scan.rs_am {
        TableAm::Heap => heap::scan_rescan(mcx, scan, key, false, false, false, false),
    }
}

pub fn table_rescan_set_params<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    key: Option<&[ScanKeyData]>,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    match scan.rs_am {
        TableAm::Heap => {
            heap::scan_rescan(mcx, scan, key, true, allow_strat, allow_sync, allow_pagemode)
        }
    }
}

// Per-tuple at M2: one enum-tag branch on the rs_am carrier, monomorphized to
// a direct call.
#[inline]
pub fn table_scan_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => heap::scan_getnextslot(mcx, scan, direction, slot),
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
            heap::scan_set_tidrange(&mut sscan, mintid, maxtid)?;
            Ok(sscan)
        }
    }
}

pub fn table_rescan_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) -> PgResult<()> {
    debug_assert!((scan.rs_flags & SO_TYPE_TIDRANGESCAN) != 0);
    match scan.rs_am {
        TableAm::Heap => {
            heap::scan_rescan(mcx, scan, None, false, false, false, false)?;
            heap::scan_set_tidrange(scan, mintid, maxtid)
        }
    }
}

pub fn table_scan_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => heap::scan_getnextslot_tidrange(mcx, scan, direction, slot),
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

pub fn table_index_fetch_begin<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<Box<IndexFetchTableData<'mcx>>> {
    match am(rel) {
        TableAm::Heap => heap::index_fetch_begin(mcx, rel),
    }
}

pub fn table_index_fetch_reset(scan: &mut IndexFetchTableData<'_>) -> PgResult<()> {
    match am(&scan.rel) {
        TableAm::Heap => heap::index_fetch_reset(scan),
    }
}

pub fn table_index_fetch_end(scan: Box<IndexFetchTableData<'_>>) -> PgResult<()> {
    match am(&scan.rel) {
        TableAm::Heap => heap::index_fetch_end(scan),
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
    match am(&scan.rel) {
        TableAm::Heap => {
            heap::index_fetch_tuple(mcx, scan, tid, snapshot, slot, call_again, all_dead)
        }
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
    // ExecDropSingleTupleTableSlot: the owned slot drops here.

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
        TableAm::Heap => heap::tuple_fetch_row_version(mcx, rel, tid, snapshot, slot),
    }
}

pub fn table_tuple_tid_valid(
    scan: &mut TableScanDescData<'_>,
    tid: &ItemPointerData,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => heap::tuple_tid_valid(scan, tid),
    }
}

pub fn table_tuple_get_latest_tid<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    tid: &mut ItemPointerData,
) -> PgResult<()> {
    let tableam = scan.rs_am;

    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_tuple_get_latest_tid call during logical decoding",
        ));
    }

    // User-supplied TID: don't trust the input too much.
    let valid = match tableam {
        TableAm::Heap => heap::tuple_tid_valid(scan, tid)?,
    };
    if !valid {
        let blk = ItemPointerGetBlockNumber(tid);
        let off = tid.ip_posid;
        let relname = scan.rs_rd.name();
        return Err(Box::new(
            PgError::error(format!(
                "tid ({blk}, {off}) is not valid for relation \"{relname}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    match tableam {
        TableAm::Heap => heap::tuple_get_latest_tid(mcx, scan, tid),
    }
}

pub fn table_tuple_satisfies_snapshot<'mcx>(
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    snapshot: &Snapshot<'mcx>,
) -> PgResult<bool> {
    match am(rel) {
        TableAm::Heap => heap::tuple_satisfies_snapshot(rel, slot, snapshot),
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
    scan: &mut TableScanDescData<'mcx>,
    next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => heap::scan_analyze_next_block(mcx, scan, next_buffer),
    }
}

pub fn table_scan_analyze_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    oldest_xmin: TransactionId,
    liverows: &mut f64,
    deadrows: &mut f64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => {
            heap::scan_analyze_next_tuple(mcx, scan, oldest_xmin, liverows, deadrows, slot)
        }
    }
}

pub fn table_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    match scan.rs_am {
        TableAm::Heap => {
            heap::scan_bitmap_next_tuple(mcx, scan, slot, recheck, lossy_pages, exact_pages)
        }
    }
}

pub fn table_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_block call during logical decoding",
        ));
    }
    match scan.rs_am {
        TableAm::Heap => heap::scan_sample_next_block(mcx, scan, scanstate),
    }
}

pub fn table_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected table_scan_sample_next_tuple call during logical decoding",
        ));
    }
    match scan.rs_am {
        TableAm::Heap => heap::scan_sample_next_tuple(mcx, scan, scanstate, slot),
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

// --- Parallel-scan helpers for block-oriented AMs (tableam.c) ---

pub fn table_block_parallelscan_estimate(_rel: &Relation<'_>) -> usize {
    core::mem::size_of::<ParallelBlockTableScanDescData>()
}

pub fn table_block_parallelscan_initialize(
    rel: &Relation<'_>,
    pscan: &mut ParallelBlockTableScanDescData,
) -> PgResult<usize> {
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
    pscan.phs_nallocated.store(0, Ordering::SeqCst);
}

pub fn table_block_parallelscan_startblock_init(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelBlockTableScanDescData,
) -> PgResult<()> {
    let mut sync_startpage: BlockNumber = InvalidBlockNumber;

    *pbscanwork = ParallelBlockTableScanWorkerData::default();

    const _: () = assert!(
        MaxBlockNumber <= 0xFFFF_FFFE,
        "pg_nextpower2_32 may be too small for non-standard BlockNumber width"
    );

    // ~PARALLEL_SEQSCAN_NCHUNKS chunks, next power of 2, capped.
    pbscanwork.phsw_chunk_size = pg_nextpower2_32(core::cmp::max(
        pbscan.phs_nblocks / PARALLEL_SEQSCAN_NCHUNKS,
        1,
    ));
    pbscanwork.phsw_chunk_size =
        core::cmp::min(pbscanwork.phsw_chunk_size, PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

    loop {
        let guard = SpinLockGuard::acquire(&pbscan.phs_mutex);

        // First worker sets startblock; syncscan asks the syncscan machinery
        // without the spinlock held, then retries.
        if pbscan.phs_startblock.load(Ordering::Relaxed) == InvalidBlockNumber {
            if !pbscan.phs_syncscan {
                pbscan.phs_startblock.store(0, Ordering::Relaxed);
            } else if sync_startpage != InvalidBlockNumber {
                pbscan
                    .phs_startblock
                    .store(sync_startpage, Ordering::Relaxed);
            } else {
                drop(guard);
                sync_startpage = ss_get_location(rel, pbscan.phs_nblocks)?;
                continue; // goto retry
            }
        }
        drop(guard);
        break;
    }

    Ok(())
}

pub fn table_block_parallelscan_nextpage(
    rel: &Relation<'_>,
    pbscanwork: &mut ParallelBlockTableScanWorkerData,
    pbscan: &ParallelBlockTableScanDescData,
) -> PgResult<BlockNumber> {
    let nallocated: u64;

    if pbscanwork.phsw_chunk_remaining > 0 {
        // Consume the rest of this worker's current chunk first.
        pbscanwork.phsw_nallocated = pbscanwork.phsw_nallocated.wrapping_add(1);
        nallocated = pbscanwork.phsw_nallocated;
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_remaining.wrapping_sub(1);
    } else {
        // Ramp chunk size down over the final RAMPDOWN_CHUNKS chunks; C wraps
        // in 32-bit BlockNumber arithmetic — replicate before widening.
        if pbscanwork.phsw_chunk_size > 1
            && pbscanwork.phsw_nallocated
                > pbscan.phs_nblocks.wrapping_sub(
                    pbscanwork
                        .phsw_chunk_size
                        .wrapping_mul(PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS),
                ) as u64
        {
            pbscanwork.phsw_chunk_size >>= 1;
        }

        pbscanwork.phsw_nallocated = pbscan
            .phs_nallocated
            .fetch_add(pbscanwork.phsw_chunk_size as u64, Ordering::SeqCst);
        nallocated = pbscanwork.phsw_nallocated;

        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_size.wrapping_sub(1);
    }

    let phs_startblock = pbscan.phs_startblock.load(Ordering::Relaxed);

    let page: BlockNumber = if nallocated >= pbscan.phs_nblocks as u64 {
        InvalidBlockNumber // all blocks have been allocated
    } else {
        (nallocated
            .wrapping_add(phs_startblock as u64)
            .wrapping_rem(pbscan.phs_nblocks as u64)) as BlockNumber
    };

    // Report position; at end-of-scan report the STARTING page once so later
    // scans' starts don't slew backwards.
    if pbscan.phs_syncscan {
        if page != InvalidBlockNumber {
            ss_report_location(rel, page)?;
        } else if nallocated == pbscan.phs_nblocks as u64 {
            ss_report_location(rel, phs_startblock)?;
        }
    }

    Ok(page)
}

// --- Relation-sizing helpers for block-oriented AMs (tableam.c) ---

pub fn table_block_relation_size(rel: &Relation<'_>, forkNumber: ForkNumber) -> PgResult<u64> {
    let _ = (rel, forkNumber);
    // smgrnblocks(fork) * BLCKSZ, summed below MAX_FORKNUM for InvalidForkNumber.
    unported("backend-storage-smgr (smgrnblocks) / relcache rd_locator")
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

fn relation_locator(_rel: &Relation<'_>) -> RelFileLocator {
    unported("relcache rd_locator (backend-utils-cache-relcache storage fields)")
}

fn relation_nblocks(_rel: &Relation<'_>) -> PgResult<BlockNumber> {
    unported("backend-storage-buffer-bufmgr (RelationGetNumberOfBlocksInFork)")
}

fn nbuffers() -> PgResult<i32> {
    unported("NBuffers GUC wiring (backend-storage-buffer-bufmgr)")
}

fn ss_get_location(_rel: &Relation<'_>, _relnblocks: BlockNumber) -> PgResult<BlockNumber> {
    unported("backend-access-common-syncscan (ss_get_location)")
}

fn ss_report_location(_rel: &Relation<'_>, _location: BlockNumber) -> PgResult<()> {
    unported("backend-access-common-syncscan (ss_report_location)")
}
