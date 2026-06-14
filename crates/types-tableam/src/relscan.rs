//! `access/relscan.h` — relation-scan descriptor vocabulary, trimmed.
//!
//! These descriptors are AM working state in C (`palloc`d by the AM's
//! `scan_begin`; the parallel descriptors live in DSM shared memory). The
//! owned model crosses the per-backend descriptors as boxes; the shared
//! parallel descriptor's concurrently-mutated fields use real atomics and a
//! `Mutex` standing in for the C spinlock.

use core::any::Any;
use core::sync::atomic::AtomicU64;
use std::boxed::Box;
use std::sync::Mutex;
use std::vec::Vec as StdVec;

use types_datum::Datum;
use types_core::primitive::{BlockNumber, InvalidBlockNumber};
use types_rel::Relation;
use types_snapshot::SnapshotData;
use types_storage::RelFileLocator;
use types_tuple::heaptuple::{HeapTuple, IndexTuple, ItemPointerData, TupleDescData};

use crate::genam::IndexScanInstrumentation;
use crate::scankey::ScanKeyData;
use crate::tableam::IndexFetchTableData;

/* ----------------------------------------------------------------
 * access/tableam.h: ScanOptions flags (TableScanDescData.rs_flags)
 * ---------------------------------------------------------------- */

/* one of SO_TYPE_* may be specified */
pub const SO_TYPE_SEQSCAN: u32 = 1 << 0;
pub const SO_TYPE_BITMAPSCAN: u32 = 1 << 1;
pub const SO_TYPE_SAMPLESCAN: u32 = 1 << 2;
pub const SO_TYPE_TIDSCAN: u32 = 1 << 3;
pub const SO_TYPE_TIDRANGESCAN: u32 = 1 << 4;
pub const SO_TYPE_ANALYZE: u32 = 1 << 5;

/* several of SO_ALLOW_* may be specified */
/// allow or disallow use of access strategy
pub const SO_ALLOW_STRAT: u32 = 1 << 6;
/// report location to syncscan logic?
pub const SO_ALLOW_SYNC: u32 = 1 << 7;
/// verify visibility page-at-a-time?
pub const SO_ALLOW_PAGEMODE: u32 = 1 << 8;

/// unregister snapshot at scan end?
pub const SO_TEMP_SNAPSHOT: u32 = 1 << 9;

/* ----------------------------------------------------------------
 * access/relscan.h: TableScanDescData
 * ---------------------------------------------------------------- */

/// `TableScanDescData` (`access/relscan.h`) — the generic table-scan
/// descriptor, the base class the AM's concrete scan state extends
/// (`HeapScanDescData` embeds it as its first member; here the AM extension
/// rides in `am_private`). `rs_rd` is an alias handle of the open relation
/// (the C pointer alias); the scan-type-specific union (`st`) lands with its
/// first consumer.
pub struct TableScanDescData<'mcx> {
    /// `rs_rd` — the relation the scan was opened on.
    pub rs_rd: Relation<'mcx>,
    /// `rs_snapshot` — snapshot to see; `None` is the C `SnapshotAny`.
    pub rs_snapshot: Option<SnapshotData>,
    /// `rs_nkeys` — number of scan keys.
    pub rs_nkeys: i32,
    /// `rs_key` — array of scan key descriptors.
    pub rs_key: std::vec::Vec<ScanKeyData>,
    /// `rs_flags` — `SO_*` `ScanOptions` bitmask.
    pub rs_flags: u32,
    /// `rs_parallel` — parallel scan information (shared descriptor).
    pub rs_parallel: Option<std::sync::Arc<ParallelTableScanDescData>>,
    /// `union { ... struct { TBMIterator rs_tbmiterator; } st; ... }` — the
    /// scan-type-specific union. Trimmed to the bitmap-scan member
    /// `st.rs_tbmiterator`, the only one any ported consumer reads
    /// (`nodeBitmapHeapscan`). Other union members land with their first
    /// consumer.
    pub rs_tbmiterator: types_tidbitmap::TBMIterator,
    /// The AM-private scan state (heap's `HeapScanDescData` tail), owned by
    /// the access method that created the descriptor.
    pub am_private: Option<std::boxed::Box<dyn Any>>,
}

/// `TableScanDesc` — `TableScanDescData *`.
pub type TableScanDesc<'mcx> = std::boxed::Box<TableScanDescData<'mcx>>;

/* ----------------------------------------------------------------
 * access/relscan.h: parallel scan descriptors
 * ---------------------------------------------------------------- */

/// `ParallelTableScanDescData` (`access/relscan.h`) — shared state for a
/// parallel table scan, living in DSM in C. The serialized snapshot that C
/// stores at byte offset `phs_snapshot_off` inside the same DSM chunk is the
/// `phs_snapshot_data` byte buffer here.
pub struct ParallelTableScanDescData {
    /// `phs_locator` — physical relation to scan.
    pub phs_locator: RelFileLocator,
    /// `phs_syncscan` — report location to syncscan logic?
    pub phs_syncscan: bool,
    /// `phs_snapshot_any` — SnapshotAny, not the serialized snapshot.
    pub phs_snapshot_any: bool,
    /// `phs_snapshot_off` — data offset of the serialized snapshot.
    pub phs_snapshot_off: usize,
    /// The serialized snapshot bytes (C: at `phs_snapshot_off`).
    pub phs_snapshot_data: Option<std::vec::Vec<u8>>,
    /// The block-oriented extension (`ParallelBlockTableScanDescData`'s
    /// fields beyond the embedded base), present once a block-oriented AM's
    /// `parallelscan_initialize` has run.
    pub block: Option<ParallelBlockTableScanExt>,
}

impl Default for ParallelTableScanDescData {
    fn default() -> Self {
        ParallelTableScanDescData {
            phs_locator: RelFileLocator::default(),
            phs_syncscan: false,
            phs_snapshot_any: false,
            phs_snapshot_off: 0,
            phs_snapshot_data: None,
            block: None,
        }
    }
}

/// The block-oriented tail of `ParallelBlockTableScanDescData`
/// (`access/relscan.h`). `phs_mutex` + `phs_startblock` become one
/// `Mutex<BlockNumber>` (the C spinlock exists only to protect that field);
/// `phs_nallocated` is the C `pg_atomic_uint64`.
pub struct ParallelBlockTableScanExt {
    /// `phs_nblocks` — number of blocks in relation at start of scan.
    pub phs_nblocks: BlockNumber,
    /// `phs_startblock` — starting block number, guarded by the C
    /// `phs_mutex` spinlock.
    pub phs_startblock: Mutex<BlockNumber>,
    /// `phs_nallocated` — number of blocks allocated to workers so far.
    pub phs_nallocated: AtomicU64,
}

impl Default for ParallelBlockTableScanExt {
    fn default() -> Self {
        ParallelBlockTableScanExt {
            phs_nblocks: 0,
            phs_startblock: Mutex::new(InvalidBlockNumber),
            phs_nallocated: AtomicU64::new(0),
        }
    }
}

/// `ParallelBlockTableScanWorkerData` (`access/relscan.h`) — per-worker
/// state for a block-oriented parallel scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParallelBlockTableScanWorkerData {
    /// `phsw_nallocated` — blocks this worker has allocated.
    pub phsw_nallocated: u64,
    /// `phsw_chunk_remaining` — blocks left in this chunk.
    pub phsw_chunk_remaining: u32,
    /// `phsw_chunk_size` — chunk size in blocks.
    pub phsw_chunk_size: u32,
}

/* ----------------------------------------------------------------
 * access/relscan.h: IndexScanDescData
 * ---------------------------------------------------------------- */

/// `IndexScanDescData` (`access/relscan.h`) — the generic index-scan
/// descriptor. The AM allocates it in `ambeginscan` and embeds it in its own
/// scan state; the AM-private tail (`opaque`) rides in `am_private`. The index
/// relation is the open handle; the heap relation, snapshot, and heap-fetch
/// descriptor are filled in by the `index_beginscan*` wrappers.
pub struct IndexScanDescData<'mcx> {
    /* scan parameters */
    /// `Relation heapRelation` — heap relation descriptor, or `None`.
    pub heap_relation: Option<Relation<'mcx>>,
    /// `Relation indexRelation` — index relation descriptor.
    pub index_relation: Relation<'mcx>,
    /// `struct SnapshotData *xs_snapshot` — snapshot to see (`None` until set
    /// by the begin wrapper).
    pub xs_snapshot: Option<SnapshotData>,
    /// `int numberOfKeys` — number of index qualifier conditions.
    pub number_of_keys: i32,
    /// `int numberOfOrderBys` — number of ordering operators.
    pub number_of_order_bys: i32,
    /// `struct ScanKeyData *keyData` — array of index qualifier descriptors.
    pub key_data: StdVec<types_scan::scankey::ScanKeyData<'mcx>>,
    /// `struct ScanKeyData *orderByData` — array of ordering op descriptors.
    pub order_by_data: StdVec<types_scan::scankey::ScanKeyData<'mcx>>,
    /// `bool xs_want_itup` — caller requests index tuples.
    pub xs_want_itup: bool,
    /// `bool xs_temp_snap` — unregister snapshot at scan end?
    pub xs_temp_snap: bool,

    /* signaling to index AM about killing index tuples */
    /// `bool kill_prior_tuple` — last-returned tuple is dead.
    pub kill_prior_tuple: bool,
    /// `bool ignore_killed_tuples` — do not return killed entries.
    pub ignore_killed_tuples: bool,
    /// `bool xactStartedInRecovery` — prevents killing/seeing killed tuples.
    pub xact_started_in_recovery: bool,

    /// `void *opaque` — access-method-specific info, owned by the AM.
    pub opaque: Option<Box<dyn Any>>,

    /// `struct IndexScanInstrumentation *instrument` — instrumentation
    /// counters maintained by the AM (`None` until set by the begin wrapper).
    pub instrument: Option<IndexScanInstrumentation>,

    /* index-only-scan result data filled by amgettuple */
    /// `IndexTuple xs_itup` — index tuple returned by the AM.
    pub xs_itup: IndexTuple<'mcx>,
    /// `struct TupleDescData *xs_itupdesc` — rowtype descriptor of `xs_itup`.
    pub xs_itupdesc: Option<Box<TupleDescData<'mcx>>>,
    /// `HeapTuple xs_hitup` — index data returned by the AM, as a HeapTuple.
    pub xs_hitup: HeapTuple<'mcx>,
    /// `struct TupleDescData *xs_hitupdesc` — rowtype descriptor of `xs_hitup`.
    pub xs_hitupdesc: Option<Box<TupleDescData<'mcx>>>,

    /// `ItemPointerData xs_heaptid` — the result TID.
    pub xs_heaptid: ItemPointerData,
    /// `bool xs_heap_continue` — T if must keep walking, potential further
    /// results.
    pub xs_heap_continue: bool,
    /// `IndexFetchTableData *xs_heapfetch` — table-AM index-fetch state
    /// (`None` until [`crate::tableam`]'s `index_fetch_begin`).
    pub xs_heapfetch: Option<Box<IndexFetchTableData<'mcx>>>,

    /// `bool xs_recheck` — T means scan keys must be rechecked.
    pub xs_recheck: bool,

    /* ordering-operator result data */
    /// `Datum *xs_orderbyvals` — ORDER BY expression values of the last
    /// returned tuple.
    pub xs_orderbyvals: StdVec<Datum>,
    /// `bool *xs_orderbynulls`.
    pub xs_orderbynulls: StdVec<bool>,
    /// `bool xs_recheckorderby`.
    pub xs_recheckorderby: bool,

    /// `struct ParallelIndexScanDescData *parallel_scan` — parallel-scan
    /// information, in shared memory (`None` for non-parallel scans).
    pub parallel_scan: Option<std::sync::Arc<ParallelIndexScanDescData>>,
}

/// `IndexScanDesc` — `IndexScanDescData *`.
pub type IndexScanDesc<'mcx> = Box<IndexScanDescData<'mcx>>;

/// `ParallelIndexScanDescData` (`access/relscan.h`) — shared state for a
/// parallel index scan, living in DSM in C. The serialized snapshot C stores
/// in the flexible `ps_snapshot_data[]` tail is the byte buffer here; the
/// instrumentation and AM-specific regions C places at `ps_offset_ins` /
/// `ps_offset_am` are carried as owned regions.
pub struct ParallelIndexScanDescData {
    /// `RelFileLocator ps_locator` — physical table relation to scan.
    pub ps_locator: RelFileLocator,
    /// `RelFileLocator ps_indexlocator` — physical index relation to scan.
    pub ps_indexlocator: RelFileLocator,
    /// `Size ps_offset_ins` — offset to `SharedIndexScanInstrumentation`.
    pub ps_offset_ins: usize,
    /// `Size ps_offset_am` — offset to am-specific structure.
    pub ps_offset_am: usize,
    /// `char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]` — serialized snapshot.
    pub ps_snapshot_data: StdVec<u8>,
    /// The `SharedIndexScanInstrumentation` region C places at
    /// `ps_offset_ins` (present when `index_parallelscan_initialize` was
    /// called with `instrument=true`).
    pub shared_instrument: Option<crate::genam::SharedIndexScanInstrumentation>,
    /// The AM-specific region C places at `ps_offset_am` (present when the AM
    /// has an `aminitparallelscan`).
    pub am_specific: Option<StdVec<u8>>,
}

impl Default for ParallelIndexScanDescData {
    fn default() -> Self {
        ParallelIndexScanDescData {
            ps_locator: RelFileLocator::default(),
            ps_indexlocator: RelFileLocator::default(),
            ps_offset_ins: 0,
            ps_offset_am: 0,
            ps_snapshot_data: StdVec::new(),
            shared_instrument: None,
            am_specific: None,
        }
    }
}
