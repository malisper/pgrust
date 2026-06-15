//! `access/relscan.h` + `access/heapam.h` — relation-scan descriptor ABI.
//!
//! These structs are heap-private working memory, but the heap access method
//! casts freely between the generic `TableScanDesc` base class and the
//! `HeapScanDesc` subclass (the base is the first member), so the layout must
//! match the C structs exactly.  Compile-time size/align/offset assertions
//! below pin the layout where it crosses the ABI.

use core::ffi::c_void;

use crate::{
    slock_t, uint32, uint64, BlockNumber, Buffer, BufferAccessStrategy, HeapTupleData,
    ItemPointerData, OffsetNumber, RelFileLocator, Relation, ScanDirection, ScanKeyData, Size,
    SnapshotData,
};

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
/* allow or disallow use of access strategy */
pub const SO_ALLOW_STRAT: u32 = 1 << 6;
/* report location to syncscan logic? */
pub const SO_ALLOW_SYNC: u32 = 1 << 7;
/* verify visibility page-at-a-time? */
pub const SO_ALLOW_PAGEMODE: u32 = 1 << 8;

/* unregister snapshot at scan end? */
pub const SO_TEMP_SNAPSHOT: u32 = 1 << 9;

/* ----------------------------------------------------------------
 * access/sdir.h: ScanDirection helpers
 * ---------------------------------------------------------------- */

/// `ScanDirectionIsForward(direction)`.
#[inline]
pub fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    direction == ScanDirection::ForwardScanDirection
}

/// `ScanDirectionIsBackward(direction)`.
#[inline]
pub fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    direction == ScanDirection::BackwardScanDirection
}

/// `ScanDirectionIsNoMovement(direction)`.
#[inline]
pub fn ScanDirectionIsNoMovement(direction: ScanDirection) -> bool {
    direction == ScanDirection::NoMovementScanDirection
}

/* ----------------------------------------------------------------
 * port/atomics.h: pg_atomic_uint64
 * ---------------------------------------------------------------- */

/// `pg_atomic_uint64` — the generic-fallback layout (a single `uint64`).  In a
/// real build this is `_Atomic uint64`; for ABI-size purposes it is one
/// 8-byte value.  Access goes through the atomics seam, never these fields
/// directly.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_atomic_uint64 {
    pub value: uint64,
}

/* ----------------------------------------------------------------
 * nodes/tidbitmap.h: TBMIterator (only the size/shape matters here — the
 * heap scan never touches it except through the executor bitmap path, which
 * is not ported in this crate)
 * ---------------------------------------------------------------- */

/// `TBMIterator` — `{ bool shared; union { TBMPrivateIterator *; TBMSharedIterator *; } i; }`.
/// Represented as the equivalently-sized/aligned struct so the `st` union in
/// [`TableScanDescData`] has the correct size.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TBMIterator {
    pub shared: bool,
    pub i: *mut c_void,
}

/// The `tidrange` arm of the `TableScanDescData.st` union.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TidRange {
    pub rs_mintid: ItemPointerData,
    pub rs_maxtid: ItemPointerData,
}

/// The `TableScanDescData.st` union: a bitmap-scan iterator or a TID range.
#[repr(C)]
#[derive(Clone, Copy)]
pub union ScanTypeSpecific {
    pub rs_tbmiterator: TBMIterator,
    pub tidrange: TidRange,
}

/* ----------------------------------------------------------------
 * access/relscan.h: TableScanDescData
 * ---------------------------------------------------------------- */

/// Generic descriptor for table scans — the base class embedded as the first
/// member of [`HeapScanDescData`].
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanDescData {
    /* scan parameters */
    /// heap relation descriptor
    pub rs_rd: Relation,
    /// snapshot to see
    pub rs_snapshot: *mut SnapshotData,
    /// number of scan keys
    pub rs_nkeys: i32,
    /// array of scan key descriptors
    pub rs_key: *mut ScanKeyData,

    /// scan type-specific members (bitmap iterator / tidrange)
    pub st: ScanTypeSpecific,

    /// `ScanOptions` bitmask describing the scan's type and behaviour
    pub rs_flags: uint32,

    /// parallel scan information
    pub rs_parallel: *mut ParallelTableScanDescData,
}

pub type TableScanDesc = *mut TableScanDescData;

/* ----------------------------------------------------------------
 * access/relscan.h: parallel scan descriptors
 * ---------------------------------------------------------------- */

/// Shared state for parallel table scan.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelTableScanDescData {
    /// physical relation to scan
    pub phs_locator: RelFileLocator,
    /// report location to syncscan logic?
    pub phs_syncscan: bool,
    /// `SnapshotAny`, not `phs_snapshot_data`?
    pub phs_snapshot_any: bool,
    /// data for snapshot
    pub phs_snapshot_off: Size,
}

pub type ParallelTableScanDesc = *mut ParallelTableScanDescData;

/// Shared state for parallel table scans, for block-oriented storage.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParallelBlockTableScanDescData {
    pub base: ParallelTableScanDescData,
    /// # blocks in relation at start of scan
    pub phs_nblocks: BlockNumber,
    /// mutual exclusion for setting startblock
    pub phs_mutex: slock_t,
    /// starting block number
    pub phs_startblock: BlockNumber,
    /// number of blocks allocated to workers so far
    pub phs_nallocated: pg_atomic_uint64,
}

pub type ParallelBlockTableScanDesc = *mut ParallelBlockTableScanDescData;

/// Per-backend state for parallel table scan, for block-oriented storage.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParallelBlockTableScanWorkerData {
    /// current # of blocks into the scan
    pub phsw_nallocated: uint64,
    /// # blocks left in this chunk
    pub phsw_chunk_remaining: uint32,
    /// the number of blocks to allocate in each I/O chunk for the scan
    pub phsw_chunk_size: uint32,
}

pub type ParallelBlockTableScanWorker = *mut ParallelBlockTableScanWorkerData;

/* ----------------------------------------------------------------
 * access/relscan.h: ReadStream (opaque)
 * ---------------------------------------------------------------- */

/// `typedef struct ReadStream ReadStream` (read_stream.h) — opaque streaming
/// read state owned by the buffer manager.
pub type ReadStream = *mut c_void;

/* ----------------------------------------------------------------
 * access/heapam.h: HeapScanDescData
 * ---------------------------------------------------------------- */

/// Heap scan descriptor — the AM-specific subclass of [`TableScanDescData`].
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapScanDescData {
    /// AM-independent part of the descriptor
    pub rs_base: TableScanDescData,

    /* state set up at initscan time */
    /// total number of blocks in rel
    pub rs_nblocks: BlockNumber,
    /// block # to start at
    pub rs_startblock: BlockNumber,
    /// max number of blocks to scan (`InvalidBlockNumber` => whole rel)
    pub rs_numblocks: BlockNumber,

    /* scan current state */
    /// false = scan not init'd yet
    pub rs_inited: bool,
    /// current offset # in non-page-at-a-time mode
    pub rs_coffset: OffsetNumber,
    /// current block # in scan, if any
    pub rs_cblock: BlockNumber,
    /// current buffer in scan, if any (a pin is held when valid)
    pub rs_cbuf: Buffer,

    /// access strategy for reads
    pub rs_strategy: BufferAccessStrategy,

    /// current tuple in scan, if any
    pub rs_ctup: HeapTupleData,

    /// streaming-read state (NULL when the scan does not stream reads)
    pub rs_read_stream: ReadStream,

    /// scan direction saved each time a new page is requested
    pub rs_dir: ScanDirection,
    /// next block the read stream will return
    pub rs_prefetch_block: BlockNumber,

    /// page-allocation data for parallel scans (NULL otherwise)
    pub rs_parallelworkerdata: ParallelBlockTableScanWorker,

    /* page-at-a-time mode + bitmap scans */
    /// current tuple's index in `rs_vistuples`
    pub rs_cindex: uint32,
    /// number of visible tuples on page
    pub rs_ntuples: uint32,
    /// offsets of the visible tuples on the current page
    pub rs_vistuples: [OffsetNumber; crate::MaxHeapTuplesPerPage as usize],
}

pub type HeapScanDesc = *mut HeapScanDescData;

/* ----------------------------------------------------------------
 * access/relscan.h: SysScanDescData
 * ---------------------------------------------------------------- */

/// `typedef struct SysScanDescData` (`access/relscan.h`) — the generic
/// catalog heap-or-index scan descriptor used by `genam.c`'s `systable_*`
/// engine.  Field order matches the C struct exactly (PostgreSQL 18.3).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SysScanDescData {
    /// catalog being scanned
    pub heap_rel: Relation,
    /// NULL if doing heap scan
    pub irel: Relation,
    /// only valid in storage-scan case
    pub scan: *mut TableScanDescData,
    /// only valid in index-scan case
    pub iscan: *mut crate::nodeindexscan::IndexScanDescData,
    /// snapshot to unregister at end of scan
    pub snapshot: *mut SnapshotData,
    /// the slot tuples are stored in
    pub slot: *mut crate::TupleTableSlot,
}

pub type SysScanDesc = *mut SysScanDescData;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn scan_option_bits() {
        assert_eq!(SO_TYPE_SEQSCAN, 0x001);
        assert_eq!(SO_TYPE_TIDRANGESCAN, 0x010);
        assert_eq!(SO_ALLOW_STRAT, 0x040);
        assert_eq!(SO_ALLOW_SYNC, 0x080);
        assert_eq!(SO_ALLOW_PAGEMODE, 0x100);
        assert_eq!(SO_TEMP_SNAPSHOT, 0x200);
    }

    #[test]
    fn st_union_layout() {
        // ItemPointerData is 6 bytes; tidrange is two of them => 12.
        assert_eq!(size_of::<TidRange>(), 12);
        // TBMIterator: bool + pointer => 16 with 8-byte alignment.
        assert_eq!(size_of::<TBMIterator>(), 16);
        assert_eq!(align_of::<TBMIterator>(), 8);
        // The union is the size of its largest member, aligned to 8.
        assert_eq!(size_of::<ScanTypeSpecific>(), 16);
        assert_eq!(align_of::<ScanTypeSpecific>(), 8);
    }

    #[test]
    fn table_scan_desc_layout() {
        // 64-bit layout: 4 pointers (32) + st(16) + flags(4)+pad(4) + ptr(8).
        assert_eq!(align_of::<TableScanDescData>(), 8);
        assert_eq!(offset_of!(TableScanDescData, rs_rd), 0);
        assert_eq!(offset_of!(TableScanDescData, rs_snapshot), 8);
        assert_eq!(offset_of!(TableScanDescData, rs_nkeys), 16);
        assert_eq!(offset_of!(TableScanDescData, rs_key), 24);
        assert_eq!(offset_of!(TableScanDescData, st), 32);
        assert_eq!(offset_of!(TableScanDescData, rs_flags), 48);
        assert_eq!(offset_of!(TableScanDescData, rs_parallel), 56);
        assert_eq!(size_of::<TableScanDescData>(), 64);
    }

    #[test]
    fn parallel_scan_desc_layout() {
        // RelFileLocator = 3 * Oid(4) = 12, then 2 bools, pad to 8, then Size(8).
        assert_eq!(size_of::<ParallelTableScanDescData>(), 24);
        assert_eq!(offset_of!(ParallelTableScanDescData, phs_locator), 0);
        assert_eq!(offset_of!(ParallelTableScanDescData, phs_syncscan), 12);
        assert_eq!(offset_of!(ParallelTableScanDescData, phs_snapshot_any), 13);
        assert_eq!(offset_of!(ParallelTableScanDescData, phs_snapshot_off), 16);

        assert_eq!(offset_of!(ParallelBlockTableScanDescData, base), 0);
        assert_eq!(offset_of!(ParallelBlockTableScanDescData, phs_nblocks), 24);
        assert_eq!(offset_of!(ParallelBlockTableScanDescData, phs_mutex), 28);
        assert_eq!(
            offset_of!(ParallelBlockTableScanDescData, phs_startblock),
            32
        );
        // phs_nallocated (8-byte) aligned to 8 => offset 40.
        assert_eq!(
            offset_of!(ParallelBlockTableScanDescData, phs_nallocated),
            40
        );

        assert_eq!(size_of::<ParallelBlockTableScanWorkerData>(), 16);
        assert_eq!(
            offset_of!(ParallelBlockTableScanWorkerData, phsw_nallocated),
            0
        );
        assert_eq!(
            offset_of!(ParallelBlockTableScanWorkerData, phsw_chunk_remaining),
            8
        );
        assert_eq!(
            offset_of!(ParallelBlockTableScanWorkerData, phsw_chunk_size),
            12
        );
    }

    #[test]
    fn heap_scan_desc_layout() {
        assert_eq!(align_of::<HeapScanDescData>(), 8);
        assert_eq!(offset_of!(HeapScanDescData, rs_base), 0);
        // After the 64-byte base:
        assert_eq!(offset_of!(HeapScanDescData, rs_nblocks), 64);
        assert_eq!(offset_of!(HeapScanDescData, rs_startblock), 68);
        assert_eq!(offset_of!(HeapScanDescData, rs_numblocks), 72);
        assert_eq!(offset_of!(HeapScanDescData, rs_inited), 76);
        assert_eq!(offset_of!(HeapScanDescData, rs_coffset), 78);
        assert_eq!(offset_of!(HeapScanDescData, rs_cblock), 80);
        assert_eq!(offset_of!(HeapScanDescData, rs_cbuf), 84);
        // rs_strategy is a pointer, 8-byte aligned => offset 88.
        assert_eq!(offset_of!(HeapScanDescData, rs_strategy), 88);
        // rs_ctup: HeapTupleData (24 bytes, 8-aligned) => offset 96.
        assert_eq!(offset_of!(HeapScanDescData, rs_ctup), 96);
        assert_eq!(offset_of!(HeapScanDescData, rs_read_stream), 120);
        assert_eq!(offset_of!(HeapScanDescData, rs_dir), 128);
        assert_eq!(offset_of!(HeapScanDescData, rs_prefetch_block), 132);
        assert_eq!(offset_of!(HeapScanDescData, rs_parallelworkerdata), 136);
        assert_eq!(offset_of!(HeapScanDescData, rs_cindex), 144);
        assert_eq!(offset_of!(HeapScanDescData, rs_ntuples), 148);
        assert_eq!(offset_of!(HeapScanDescData, rs_vistuples), 152);
    }
}
