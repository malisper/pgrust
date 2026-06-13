//! `access/relscan.h` — relation-scan descriptor vocabulary, trimmed.
//!
//! These descriptors are AM working state in C (`palloc`d by the AM's
//! `scan_begin`; the parallel descriptors live in DSM shared memory). The
//! owned model crosses the per-backend descriptors as boxes; the shared
//! parallel descriptor's concurrently-mutated fields use real atomics and a
//! `Mutex` standing in for the C spinlock.

use core::any::Any;
use core::sync::atomic::AtomicU64;
use std::sync::Mutex;

use types_core::primitive::{BlockNumber, InvalidBlockNumber};
use types_rel::Relation;
use types_snapshot::SnapshotData;
use types_storage::RelFileLocator;

use crate::scankey::ScanKeyData;

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
