//! tableam.h/relscan.h vocabulary + block parallel-scan helpers, hoisted
//! below the heap AM crates so tableam -> heapam_handler -> heapam is acyclic.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

/// W2a block-run allocator (parallel writes increment 2).
pub mod block_run;

use core::ptr::NonNull;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::cell::Cell;
use std::rc::Rc;

use ::mcx::PgVec;
use ::types_core::primitive::{
    BlockNumber, InvalidBlockNumber, MaxBlockNumber, OffsetNumber, Oid, TransactionId,
};
use ::types_core::xact::CommandId;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_scan::scankey::ScanKeyData;
use ::types_snapshot::SnapshotData;
use ::types_storage::{RelFileLocator, Spinlock};
use ::types_tuple::ItemPointerData;

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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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

// C `Snapshot`: None is InvalidSnapshot/SnapshotAny; Rc = snapmgr's refcount (rule 2.3).
pub type Snapshot<'mcx> = Option<Rc<SnapshotData<'mcx>>>;

// relscan.h TableScanDescData: the rs_base every AM scan state embeds.
pub struct TableScanDescData<'mcx> {
    pub rs_rd: Relation<'mcx>,
    pub rs_snapshot: Snapshot<'mcx>,
    pub rs_nkeys: i32,
    pub rs_key: PgVec<'mcx, ScanKeyData>,
    pub rs_mintid: ItemPointerData,
    pub rs_maxtid: ItemPointerData,
    pub rs_flags: u32,
    pub rs_parallel: Option<NonNull<ParallelBlockTableScanDescData>>,
    // Rule-4 carrier, resolved once at scan_begin (SET ACCESS METHOD is
    // AccessExclusiveLock: it cannot change under a live scan).
    pub rs_am: TableAm,
}

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

impl Default for ParallelBlockTableScanDescData {
    fn default() -> Self {
        ParallelBlockTableScanDescData {
            phs_locator: RelFileLocator::default(),
            phs_syncscan: false,
            phs_snapshot_any: false,
            phs_snapshot_off: 0,
            phs_nblocks: 0,
            phs_mutex: Spinlock::new(),
            phs_startblock: AtomicU32::new(InvalidBlockNumber),
            phs_nallocated: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Default)]
pub struct ParallelBlockTableScanWorkerData {
    pub phsw_nallocated: u64,
    pub phsw_chunk_remaining: u32,
    pub phsw_chunk_size: u32,
}

// hio.h BulkInsertStateData; `current_buf` is a BufferPin (RAII replaces C's
// FreeBulkInsertState/ReleaseBulkInsertStatePin ReleaseBuffer).
pub struct BulkInsertStateData {
    pub strategy: ::types_storage::buf::BufferAccessStrategy,
    pub current_buf: Option<::bufmgr_seams::BufferPin>,
    pub next_free: BlockNumber,
    pub last_free: BlockNumber,
    pub already_extended_by: u32,
    /// W2a block-run mode (parallel write engagement): when set, hio's
    /// buffer supply comes exclusively from runs claimed here — no FSM, no
    /// shared-targblock probe, no heavyweight extension (`block_run.rs`).
    /// None = the serial protocol, byte-identical to before the field.
    pub block_run: Option<std::sync::Arc<block_run::BlockRunAllocator>>,
}

/// W1 receiver-level multi-insert buffer (parallel-writes ladder): row copies
/// parked between `table_multi_insert` flushes by the table-rewrite
/// DestReceivers (intorel/transientrel receive). Data shape only — the
/// buffer/flush logic lives in `tableam::write_buffer` (this crate sits below
/// the AM crates so the receiver states in the command seams can carry it).
///
/// std Vec: SlotData owns droppy state via the arena-erased views (buffer
/// pins, SHOULDFREE images); the pool lives for the statement and is dropped
/// (never flushed) on the statement's Err path, C's CopyMultiInsertBuffer
/// abort discipline.
pub struct WriteMultiInsertBuffer<'mcx> {
    pub slots: Vec<::types_slot::SlotData<'mcx>>,
    pub nused: usize,
    pub bytes: usize,
}

impl<'mcx> WriteMultiInsertBuffer<'mcx> {
    pub fn new() -> Self {
        WriteMultiInsertBuffer { slots: Vec::new(), nused: 0, bytes: 0 }
    }
}

impl Default for WriteMultiInsertBuffer<'_> {
    fn default() -> Self {
        Self::new()
    }
}

// tsmapi.h sample capability; an OPEN extension point in C, so dyn is
// faithful. `donetuples` is the scan's returned-tuple count — C's callbacks
// read it off the SampleScanState they are handed (tsm_system_rows does).
pub trait SampleScanDriver {
    fn has_next_sample_block(&self) -> bool;
    fn next_sample_block(&mut self, nblocks: BlockNumber, donetuples: i64) -> BlockNumber;
    fn next_sample_tuple(
        &mut self,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
        donetuples: i64,
    ) -> OffsetNumber;
}

pub const HEAP_TABLE_AM_OID: Oid = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableAm {
    Heap,
    Pgrcolumnar,
}

impl TableAm {
    #[inline]
    pub fn of(relation: &Relation<'_>) -> Option<TableAm> {
        match relation.rd_rel.relam {
            HEAP_TABLE_AM_OID => Some(TableAm::Heap),
            // RelationInitTableAccessMethod (relcache.c): sequences always
            // ride the heap AM despite relam = 0.
            _ if relation.rd_rel.relkind == types_rel::RELKIND_SEQUENCE => Some(TableAm::Heap),
            relam => {
                if relam != 0 && is_registered_heap_am(relam) {
                    Some(TableAm::Heap)
                } else if relam != 0 && is_registered_pgrcolumnar_am(relam) {
                    Some(TableAm::Pgrcolumnar)
                } else {
                    None
                }
            }
        }
    }
}

// Zone-map qual pushdown vocabulary (pgrcolumnar; advisory-only pruning —
// docs/design/pgrcolumnar-impl.md §7.3). Values are the widened i64 image of the
// int/date/timestamp constant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZoneCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug)]
pub struct ZoneQual {
    pub attnum: u16,
    pub op: ZoneCmp,
    pub val: i64,
}

// Per-granule constant-fold of a ZoneQual against the granule's decoded
// [min,max]: AllPass/AllFail let the staged prewhere drive skip the column
// decode and per-row eval entirely; Mixed falls to the normal path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZoneVerdict {
    AllPass,
    AllFail,
    Mixed,
}

thread_local! {
    // Non-builtin pg_am oids whose amhandler is heap_tableam_handler,
    // registered by relcache at entry build (where C resolves the handler
    // into rd_tableam). pg_am.amhandler is immutable (no ALTER ACCESS
    // METHOD ... HANDLER), so entries never invalidate.
    static HEAP_HANDLER_AMS: std::cell::RefCell<Vec<Oid>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cold]
fn is_registered_heap_am(relam: Oid) -> bool {
    HEAP_HANDLER_AMS.with(|v| v.borrow().contains(&relam))
}

pub fn register_heap_table_am(relam: Oid) {
    HEAP_HANDLER_AMS.with(|v| {
        let mut v = v.borrow_mut();
        if !v.contains(&relam) {
            v.push(relam);
        }
    });
}

thread_local! {
    // pg_am oids whose amname is "cbstore" (the frozen SQL-surface AM name for
    // the pgrcolumnar engine; closed-AM engine: handlers are
    // never invoked, so pgrcolumnar is identified by name at relcache build —
    // docs/design/pgrcolumnar-impl.md §7.1).
    static PGRCOLUMNAR_AMS: std::cell::RefCell<Vec<Oid>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cold]
fn is_registered_pgrcolumnar_am(relam: Oid) -> bool {
    PGRCOLUMNAR_AMS.with(|v| v.borrow().contains(&relam))
}

/// Registry probe by relam oid (vacuum/planner gates that hold a
/// RelationData, not a Relation).
pub fn is_pgrcolumnar_am_oid(relam: Oid) -> bool {
    relam != 0 && is_registered_pgrcolumnar_am(relam)
}

pub fn register_pgrcolumnar_table_am(relam: Oid) {
    PGRCOLUMNAR_AMS.with(|v| {
        let mut v = v.borrow_mut();
        if !v.contains(&relam) {
            v.push(relam);
        }
    });
}

thread_local! {
    static SYNCHRONIZE_SEQSCANS_GUC: Cell<bool> = const { Cell::new(true) };
}

pub fn synchronize_seqscans() -> bool {
    SYNCHRONIZE_SEQSCANS_GUC.with(Cell::get)
}

pub fn set_synchronize_seqscans(value: bool) {
    SYNCHRONIZE_SEQSCANS_GUC.with(|v| v.set(value));
}

const PARALLEL_SEQSCAN_NCHUNKS: u32 = 2048;
const PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS: u32 = 64;
const PARALLEL_SEQSCAN_MAX_CHUNK_SIZE: u32 = 8192;

// pg_bitutils.h pg_nextpower2_32; valid for num in [1, 2^31].
pub fn pg_nextpower2_32(num: u32) -> u32 {
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

    pbscanwork.phsw_chunk_size = pg_nextpower2_32(core::cmp::max(
        pbscan.phs_nblocks / PARALLEL_SEQSCAN_NCHUNKS,
        1,
    ));
    pbscanwork.phsw_chunk_size =
        core::cmp::min(pbscanwork.phsw_chunk_size, PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

    loop {
        let guard = SpinLockGuard::acquire(&pbscan.phs_mutex);

        // First worker sets startblock; syncscan probes without the lock, retries.
        if pbscan.phs_startblock.load(Ordering::Relaxed) == InvalidBlockNumber {
            if !pbscan.phs_syncscan {
                pbscan.phs_startblock.store(0, Ordering::Relaxed);
            } else if sync_startpage != InvalidBlockNumber {
                pbscan
                    .phs_startblock
                    .store(sync_startpage, Ordering::Relaxed);
            } else {
                drop(guard);
                sync_startpage = syncscan_seams::ss_get_location::call(rel, pbscan.phs_nblocks)?;
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
        pbscanwork.phsw_nallocated = pbscanwork.phsw_nallocated.wrapping_add(1);
        nallocated = pbscanwork.phsw_nallocated;
        pbscanwork.phsw_chunk_remaining = pbscanwork.phsw_chunk_remaining.wrapping_sub(1);
    } else {
        // Rampdown near scan end; C wraps in 32-bit BlockNumber arithmetic.
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

    // At end-of-scan report the STARTING page so later starts don't slew back.
    if pbscan.phs_syncscan {
        if page != InvalidBlockNumber {
            syncscan_seams::ss_report_location::call(rel, page)?;
        } else if nallocated == pbscan.phs_nblocks as u64 {
            syncscan_seams::ss_report_location::call(rel, phs_startblock)?;
        }
    }

    Ok(page)
}

pub const VACOPT_VACUUM: u32 = 0x01;
pub const VACOPT_ANALYZE: u32 = 0x02;
pub const VACOPT_VERBOSE: u32 = 0x04;
pub const VACOPT_FREEZE: u32 = 0x08;
pub const VACOPT_FULL: u32 = 0x10;
pub const VACOPT_SKIP_LOCKED: u32 = 0x20;
pub const VACOPT_PROCESS_MAIN: u32 = 0x40;
pub const VACOPT_PROCESS_TOAST: u32 = 0x80;
pub const VACOPT_DISABLE_PAGE_SKIPPING: u32 = 0x100;
pub const VACOPT_SKIP_DATABASE_STATS: u32 = 0x200;
pub const VACOPT_ONLY_DATABASE_STATS: u32 = 0x400;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u32)]
pub enum VacOptValue {
    #[default]
    Unspecified = 0,
    Auto,
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug)]
pub struct VacuumParams {
    pub options: u32,
    pub freeze_min_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_table_age: i32,
    pub is_wraparound: bool,
    pub log_min_duration: i32,
    pub index_cleanup: VacOptValue,
    pub truncate: VacOptValue,
    pub toast_parent: Oid,
    pub max_eager_freeze_failure_rate: f64,
    pub nworkers: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VacuumCutoffs {
    pub relfrozenxid: TransactionId,
    pub relminmxid: ::types_core::MultiXactId,
    pub OldestXmin: TransactionId,
    pub OldestMxact: ::types_core::MultiXactId,
    pub FreezeLimit: TransactionId,
    pub MultiXactCutoff: ::types_core::MultiXactId,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VacDeadItemsInfo {
    pub max_bytes: usize,
    pub num_items: i64,
}
