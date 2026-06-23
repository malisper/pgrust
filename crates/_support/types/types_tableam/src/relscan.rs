//! `access/relscan.h` — relation-scan descriptor vocabulary, trimmed.
//!
//! These descriptors are AM working state in C (`palloc`d by the AM's
//! `scan_begin`). The block-oriented parallel scan descriptor
//! ([`ParallelBlockTableScanDescData`]) lives in DSM shared memory: it is a
//! flat `#[repr(C)]` object the leader placement-initializes directly in the
//! `shm_toc` chunk and every worker reinterprets over the SAME in-segment
//! bytes — exactly the [`SharedDsmObject`] keystone the parallel-bitmap state
//! uses. Its concurrently-mutated fields are interior-mutable: `phs_mutex` is
//! the in-segment [`Spinlock`] (the C `slock_t`), `phs_startblock` a
//! `pg_atomic_uint32` (the C plain field, serialized by `phs_mutex` — a
//! relaxed atomic load/store under the spinlock is behaviour-preserving), and
//! `phs_nallocated` the C `pg_atomic_uint64`. There is no `Vec`/`Mutex`/`Arc`
//! (process-heap pointers cannot live in DSM); the serialized snapshot is a
//! `[u8]` flexible-array tail inside the same chunk, located at
//! `phs_snapshot_off`. A [`ParallelTableScanDesc`] is the `Copy` raw-pointer
//! handle the executor threads through scan descriptors — C's
//! `ParallelTableScanDesc` (a pointer into DSM bytes).

use std::boxed::Box;
use std::vec::Vec as StdVec;

use ::types_parallel::shared_dsm_object::SharedRef;
use ::types_parallel::SharedDsmObject;
use ::types_tuple::heaptuple::Datum;
use ::types_core::primitive::BlockNumber;
use ::rel::Relation;
use ::snapshot::SnapshotData;
use ::types_storage::storage::{pg_atomic_uint32, pg_atomic_uint64};
use ::types_storage::{RelFileLocator, Spinlock};
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heaptuple::{ItemPointerData, TupleDescData};

use crate::amopaque::AmOpaque;
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
    /// `rs_key` — array of scan key descriptors, allocated in the scan's `mcx`
    /// arena (convention A).
    pub rs_key: mcx::PgVec<'mcx, ScanKeyData<'mcx>>,
    /// `rs_flags` — `SO_*` `ScanOptions` bitmask.
    pub rs_flags: u32,
    /// `rs_parallel` — parallel scan information (shared descriptor), a `Copy`
    /// raw-pointer handle into the DSM-resident [`ParallelBlockTableScanDescData`]
    /// (C's `ParallelTableScanDesc`, a pointer into DSM bytes). `None` for a
    /// non-parallel scan.
    pub rs_parallel: Option<ParallelTableScanDesc>,
    /// `union { ... struct { TBMIterator rs_tbmiterator; } st; ... }` — the
    /// scan-type-specific union. Trimmed to the bitmap-scan member
    /// `st.rs_tbmiterator`, the only one any ported consumer reads
    /// (`nodeBitmapHeapscan`). Other union members land with their first
    /// consumer.
    pub rs_tbmiterator: tidbitmap::TBMIterator,
    /// The AM-private scan state (heap's `HeapScanDescData` tail), owned by
    /// the access method that created the descriptor and allocated in the
    /// scan's `mcx` arena (convention A). The `'mcx`-safe erased carrier (C's
    /// `void *`, but with a tag-checked downcast — see [`crate::amopaque`]).
    pub am_private: Option<mcx::PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>>,
}

/// `TableScanDesc` — `TableScanDescData *`.
pub type TableScanDesc<'mcx> = std::boxed::Box<TableScanDescData<'mcx>>;

/* ----------------------------------------------------------------
 * access/relscan.h: parallel scan descriptors
 * ---------------------------------------------------------------- */

/// `ParallelBlockTableScanDescData` (`access/relscan.h`) — shared state for a
/// parallel table scan over a block-oriented AM, living in DSM. C embeds a
/// `ParallelTableScanDescData base` as the first member; the owned model
/// flattens the base fields in directly (the base is only ever consumed via
/// the block-oriented descriptor — there is no AM whose parallel scan uses the
/// bare base). The serialized snapshot is the `[u8]` flexible-array tail
/// inside the same chunk, located at `phs_snapshot_off`.
///
/// `#[repr(C)]` with the C field order (base fields, then `phs_nblocks`,
/// `phs_mutex`, `phs_startblock`, `phs_nallocated`) because the leader
/// placement-initializes this struct DIRECTLY in the DSM chunk and every
/// worker reinterprets the SAME in-segment bytes through the keystone
/// [`SharedRef`]; the layout must match across processes.
///
/// `phs_startblock` and `phs_nallocated` are the two fields C mutates after
/// the launch barrier; to be a sound [`SharedDsmObject`] (mutated through a
/// shared `&self`) they are interior-mutable atomic words. C accesses
/// `phs_startblock` as a plain `BlockNumber` while holding `phs_mutex`; a
/// relaxed atomic load/store under that same spinlock is behaviour-preserving
/// (the spinlock supplies the ordering). `phs_nallocated` is the C
/// `pg_atomic_uint64`.
#[repr(C)]
#[derive(Debug, Default)]
pub struct ParallelBlockTableScanDescData {
    /* --- ParallelTableScanDescData base --- */
    /// `base.phs_locator` — physical relation to scan.
    pub phs_locator: RelFileLocator,
    /// `base.phs_syncscan` — report location to syncscan logic?
    pub phs_syncscan: bool,
    /// `base.phs_snapshot_any` — SnapshotAny, not the serialized snapshot.
    pub phs_snapshot_any: bool,
    /// `base.phs_snapshot_off` — byte offset (within the same chunk) of the
    /// serialized snapshot flexible-array tail.
    pub phs_snapshot_off: usize,

    /* --- block-oriented extension --- */
    /// `phs_nblocks` — number of blocks in relation at start of scan.
    pub phs_nblocks: BlockNumber,
    /// `phs_mutex` — `slock_t`, mutual exclusion for setting `phs_startblock`.
    pub phs_mutex: Spinlock,
    /// `phs_startblock` — starting block number, the C plain field serialized
    /// by `phs_mutex`, held in an atomic word so it round-trips through the
    /// shared `&self`.
    pub phs_startblock: pg_atomic_uint32,
    /// `phs_nallocated` — number of blocks allocated to workers so far.
    pub phs_nallocated: pg_atomic_uint64,
}

// SAFETY: `#[repr(C)]` matching the C `ParallelBlockTableScanDescData`
// field-for-field (the embedded base flattened in C order); every field C
// mutates concurrently after the launch barrier is interior-mutable —
// `phs_startblock`/`phs_nallocated` are atomic words and `phs_mutex` is the
// in-segment spinlock; the leader's placement initializer writes every field.
// A shared `&Self` is therefore sound to alias across processes.
unsafe impl SharedDsmObject for ParallelBlockTableScanDescData {}

impl ParallelBlockTableScanDescData {
    /// `pbscan->phs_startblock` (read) — the relaxed load issued while holding
    /// `phs_mutex` (the C plain read).
    #[inline]
    pub fn phs_startblock(&self) -> BlockNumber {
        self.phs_startblock.read()
    }

    /// `pbscan->phs_startblock = b` (the C plain store under `phs_mutex`).
    #[inline]
    pub fn set_phs_startblock(&self, b: BlockNumber) {
        self.phs_startblock
            .value
            .store(b, core::sync::atomic::Ordering::Relaxed);
    }
}

/// `ParallelTableScanDesc` (`access/relscan.h`) — C's pointer into DSM bytes.
/// The `Copy` raw-pointer handle the executor threads through scan
/// descriptors: the in-DSM [`ParallelBlockTableScanDescData`] header plus the
/// `[u8]` serialized-snapshot tail. The DSM segment that backs it is owned by
/// the `ParallelContext` and outlives every scan that references it (exactly
/// C's lifetime relationship), so the handle carries no Rust lifetime — just
/// like the C pointer.
#[derive(Clone, Copy)]
pub struct ParallelTableScanDesc {
    /// Address of the in-DSM `ParallelBlockTableScanDescData` header.
    desc: *const ParallelBlockTableScanDescData,
    /// Address of the serialized-snapshot tail (`(char *) pscan +
    /// phs_snapshot_off`).
    snapshot: *const u8,
    /// Length, in bytes, of the serialized-snapshot tail (0 when the scan uses
    /// `SnapshotAny`).
    snapshot_len: usize,
}

// SAFETY: the handle is a borrow of a shared DSM segment whose cross-process
// synchronization is the embedded interior-mutable fields' responsibility
// (mirrors `SharedRef: Send`/`Sync`).
unsafe impl Send for ParallelTableScanDesc {}
unsafe impl Sync for ParallelTableScanDesc {}

impl ParallelTableScanDesc {
    /// Build the handle from the leader's freshly-placed header [`SharedRef`]
    /// plus the in-segment address and byte length of the serialized-snapshot
    /// tail (`(char *) pscan + phs_snapshot_off`, `snapshot_len == 0` for
    /// `SnapshotAny`). The DSM segment that backs both outlives the handle.
    pub fn from_shared(
        desc: SharedRef<'_, ParallelBlockTableScanDescData>,
        snapshot_addr: usize,
        snapshot_len: usize,
    ) -> Self {
        ParallelTableScanDesc {
            desc: desc.get() as *const ParallelBlockTableScanDescData,
            snapshot: snapshot_addr as *const u8,
            snapshot_len,
        }
    }

    /// `(ParallelBlockTableScanDesc) pscan` — the shared `&` to the in-DSM
    /// descriptor. All concurrent mutation goes through its interior-mutable
    /// fields, so this shared reference is sound even while other processes
    /// hold their own `&` to the same bytes.
    #[inline]
    pub fn desc(&self) -> &ParallelBlockTableScanDescData {
        // SAFETY: `desc` is a real in-segment address of a leader-initialized
        // `ParallelBlockTableScanDescData` live for the DSM segment (which
        // outlives this handle); `SharedDsmObject` guarantees every
        // concurrently-mutated field is interior-mutable.
        unsafe { &*self.desc }
    }

    /// `(char *) pscan + pscan->phs_snapshot_off` — the serialized snapshot
    /// bytes (empty when the scan uses `SnapshotAny`).
    ///
    /// The serialized snapshot is self-delimiting: its length is
    /// `SERIALIZED_HEADER_LEN (24) + (xcnt + subxcnt) * 4`, computed from the
    /// `xcnt`/`subxcnt` words in its own fixed header (`SerializeSnapshot`'s
    /// layout). `RestoreSnapshot` reads exactly that many bytes and ignores any
    /// trailing chunk padding, so the returned slice is the exact serialized
    /// snapshot regardless of the chunk's total size — this is why a worker
    /// never needs the chunk length to restore the snapshot.
    #[inline]
    pub fn snapshot_bytes(&self) -> &[u8] {
        if self.snapshot_len == 0 || self.desc().phs_snapshot_any {
            return &[];
        }
        // SAFETY: leader-written-once before the launch barrier, read-only
        // thereafter; the bytes live for the DSM segment. The header (24 bytes)
        // is always present for a serialized MVCC snapshot.
        const SERIALIZED_HEADER_LEN: usize = 24;
        let hdr = unsafe { core::slice::from_raw_parts(self.snapshot, SERIALIZED_HEADER_LEN) };
        let xcnt = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
        let subxcnt = i32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]).max(0) as usize;
        let total = SERIALIZED_HEADER_LEN + (xcnt + subxcnt) * 4;
        // SAFETY: as above; `total` is exactly the serialized length the leader
        // wrote, never larger than the chunk's snapshot region.
        unsafe { core::slice::from_raw_parts(self.snapshot, total) }
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

    /// `void *opaque` — access-method-specific info, owned by the AM. The
    /// `'mcx`-safe erased carrier with a tag-checked downcast (see
    /// [`crate::amopaque`]); allocated in the scan's `mcx` arena (convention A).
    pub opaque: Option<mcx::PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>>,

    /// `struct IndexScanInstrumentation *instrument` — instrumentation
    /// counters maintained by the AM (`None` until set by the begin wrapper).
    pub instrument: Option<IndexScanInstrumentation>,

    /* index-only-scan result data filled by amgettuple */
    /// `IndexTuple xs_itup` — index tuple returned by the AM. In C this is a
    /// pointer aliasing the on-disk index-tuple bytes in the AM's per-scan
    /// workspace (`(IndexTuple) (so->currTuples + tupleOffset)`); the owned
    /// model carries the contiguous on-disk byte image (header / null bitmap /
    /// `MAXALIGN`-padded user data — what `index_form_tuple` produces), since
    /// the header-only `IndexTupleData` cannot hold the variable-length data
    /// area. `None` is the C `NULL`.
    pub xs_itup: Option<mcx::PgVec<'mcx, u8>>,
    /// `struct TupleDescData *xs_itupdesc` — rowtype descriptor of `xs_itup`.
    pub xs_itupdesc: Option<Box<TupleDescData<'mcx>>>,
    /// `HeapTuple xs_hitup` — index data returned by the AM, as a HeapTuple.
    /// Owned as a data-bearing [`FormedTuple`] (header + user-data area) so the
    /// index-only-scan store path can deform it into a virtual/minimal target
    /// slot (`ExecForceStoreHeapTuple`); a bare `HeapTupleData` header cannot
    /// reach the column bytes in the owned model.
    pub xs_hitup: Option<FormedTuple<'mcx>>,
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
    pub xs_orderbyvals: StdVec<Datum<'mcx>>,
    /// `bool *xs_orderbynulls`.
    pub xs_orderbynulls: StdVec<bool>,
    /// `bool xs_recheckorderby`.
    pub xs_recheckorderby: bool,

    /// `struct ParallelIndexScanDescData *parallel_scan` — parallel-scan
    /// information, in shared memory (`None` for non-parallel scans). The
    /// `Copy` in-DSM pointer handle (C's bare `ParallelIndexScanDesc` pointer);
    /// no per-process copy.
    pub parallel_scan: Option<ParallelIndexScanDescHandle>,
}

/// `IndexScanDesc` — `IndexScanDescData *`.
pub type IndexScanDesc<'mcx> = Box<IndexScanDescData<'mcx>>;

// Manual `Debug` (the erased `opaque`/`xs_heapfetch` payloads are not `Debug`).
// Mirrors the trimmed node-pool descriptor's manual impl; prints the scalar
// scan-state fields the executor inspects.
impl core::fmt::Debug for IndexScanDescData<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IndexScanDescData")
            .field("number_of_keys", &self.number_of_keys)
            .field("number_of_order_bys", &self.number_of_order_bys)
            .field("xs_want_itup", &self.xs_want_itup)
            .field("xs_temp_snap", &self.xs_temp_snap)
            .field("kill_prior_tuple", &self.kill_prior_tuple)
            .field("ignore_killed_tuples", &self.ignore_killed_tuples)
            .field("xact_started_in_recovery", &self.xact_started_in_recovery)
            .field("instrument", &self.instrument)
            .field("xs_heaptid", &self.xs_heaptid)
            .field("xs_heap_continue", &self.xs_heap_continue)
            .field("xs_recheck", &self.xs_recheck)
            .field("xs_recheckorderby", &self.xs_recheckorderby)
            .field("has_opaque", &self.opaque.is_some())
            .field("has_heapfetch", &self.xs_heapfetch.is_some())
            .finish_non_exhaustive()
    }
}

/// `ParallelIndexScanDescData` (`access/relscan.h`) — shared state for a
/// parallel index scan, living IN the DSM chunk in C. This is the flat
/// `#[repr(C)]` header (`{ ps_locator, ps_indexlocator, ps_offset_ins,
/// ps_offset_am }`); the serialized snapshot (`ps_snapshot_data[]`
/// flexible-array tail), the `SharedIndexScanInstrumentation` region (at
/// `ps_offset_ins`), and the AM-specific region (at `ps_offset_am`) all live
/// in-chunk immediately after / past the header, exactly as C lays them out via
/// `OffsetToPointer`. `index_parallelscan_initialize` writes the header and all
/// tails directly in the `shm_toc`-allocated chunk; a worker reinterprets the
/// SAME in-segment bytes through a [`ParallelIndexScanDescHandle`].
///
/// The header carries no `Vec`/`Option`: process-heap pointers cannot live in
/// DSM. The two locators are written once by the leader before the launch
/// barrier and read-only thereafter; `ps_offset_ins`/`ps_offset_am` likewise.
/// No field is mutated concurrently, so the shared `&self` (or `*mut` used only
/// by the leader pre-launch / by `OffsetToPointer` consumers) is sound to alias
/// across processes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ParallelIndexScanDescData {
    /// `RelFileLocator ps_locator` — physical table relation to scan.
    pub ps_locator: RelFileLocator,
    /// `RelFileLocator ps_indexlocator` — physical index relation to scan.
    pub ps_indexlocator: RelFileLocator,
    /// `Size ps_offset_ins` — offset to `SharedIndexScanInstrumentation`.
    pub ps_offset_ins: usize,
    /// `Size ps_offset_am` — offset to am-specific structure.
    pub ps_offset_am: usize,
    // `char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]` — the serialized snapshot
    // begins at `(char *) self + offsetof(.., ps_snapshot_data)`, i.e.
    // immediately after this header (it is the inline FAM; see the handle).
}

// SAFETY: `#[repr(C)]` matching the C `ParallelIndexScanDescData` header
// field-for-field. No field is mutated after the launch barrier — the leader
// writes all four (and the in-chunk tails) before any worker attaches, and they
// are read-only thereafter. A shared `&Self` aliasing another process's `&Self`
// over the same bytes is therefore sound.
unsafe impl SharedDsmObject for ParallelIndexScanDescData {}

/// `offsetof(ParallelIndexScanDescData, ps_snapshot_data)` — the fixed header
/// size, where the serialized-snapshot flexible-array tail begins. With
/// `#[repr(C)]` this is exactly `size_of::<ParallelIndexScanDescData>()` (the
/// header has no trailing FAM member, so its size IS the offset of the tail
/// bytes that follow it in the chunk).
pub const PARALLEL_INDEX_SCAN_DESC_HEADER_SIZE: usize =
    core::mem::size_of::<ParallelIndexScanDescData>();

/// `ParallelIndexScanDesc` (`access/relscan.h`) — C's `ParallelIndexScanDescData
/// *`, a pointer into DSM bytes. The `Copy` raw-pointer handle the executor
/// threads through: the in-DSM [`ParallelIndexScanDescData`] header plus the
/// in-chunk snapshot / instrumentation / AM tails reachable through it via
/// `OffsetToPointer(self, off)`. The DSM segment that backs it is owned by the
/// `ParallelContext` and outlives every scan that references it (C's lifetime
/// relationship), so the handle carries no Rust lifetime — just like the C
/// pointer.
#[derive(Clone, Copy)]
pub struct ParallelIndexScanDescHandle {
    /// Address of the in-DSM `ParallelIndexScanDescData` header (== the chunk
    /// base; the C `pscan` pointer).
    base: *mut ParallelIndexScanDescData,
}

// SAFETY: the handle is a borrow of a shared DSM segment whose cross-process
// synchronization is the AM-specific tail's responsibility (the btree tail's
// `btps_lock`); the header itself is write-once-pre-launch (mirrors
// `ParallelTableScanDesc: Send`/`Sync`).
unsafe impl Send for ParallelIndexScanDescHandle {}
unsafe impl Sync for ParallelIndexScanDescHandle {}

impl ParallelIndexScanDescHandle {
    /// Build the handle from a `SharedRef` to the leader-placed header (its
    /// address is the chunk base / the C `pscan` pointer).
    pub fn from_shared(desc: SharedRef<'_, ParallelIndexScanDescData>) -> Self {
        ParallelIndexScanDescHandle {
            base: desc.get() as *const ParallelIndexScanDescData as *mut ParallelIndexScanDescData,
        }
    }

    /// Build the handle from a raw in-segment base address (e.g. a worker's
    /// `shm_toc_lookup` result, or the leader's freshly-allocated chunk).
    ///
    /// # Safety
    /// `base` must be the real in-segment address of a leader-initialized
    /// `ParallelIndexScanDescData` live for the DSM segment.
    pub unsafe fn from_raw(base: usize) -> Self {
        ParallelIndexScanDescHandle {
            base: base as *mut ParallelIndexScanDescData,
        }
    }

    /// The chunk base address (the C `pscan` pointer reinterpreted as `usize`).
    /// `OffsetToPointer(pscan, off)` is `self.base_addr() + off`.
    #[inline]
    pub fn base_addr(&self) -> usize {
        self.base as usize
    }

    /// `pscan->ps_locator`.
    #[inline]
    pub fn ps_locator(&self) -> RelFileLocator {
        // SAFETY: `base` is a real in-segment address of a leader-initialized
        // header (handle contract); the field is write-once-pre-launch.
        unsafe { (*self.base).ps_locator }
    }

    /// `pscan->ps_indexlocator`.
    #[inline]
    pub fn ps_indexlocator(&self) -> RelFileLocator {
        // SAFETY: see `ps_locator`.
        unsafe { (*self.base).ps_indexlocator }
    }

    /// `pscan->ps_offset_ins`.
    #[inline]
    pub fn ps_offset_ins(&self) -> usize {
        // SAFETY: see `ps_locator`.
        unsafe { (*self.base).ps_offset_ins }
    }

    /// `pscan->ps_offset_am`.
    #[inline]
    pub fn ps_offset_am(&self) -> usize {
        // SAFETY: see `ps_locator`.
        unsafe { (*self.base).ps_offset_am }
    }

    /// `(char *) pscan->ps_snapshot_data` — the serialized snapshot bytes,
    /// beginning immediately after the header. The serialized snapshot is
    /// self-delimiting (its own header records `xcnt`/`subxcnt`), so the
    /// returned slice length is computed from those words — `RestoreSnapshot`
    /// reads exactly that many bytes regardless of chunk padding.
    #[inline]
    pub fn ps_snapshot_data(&self) -> &[u8] {
        let snap = (self.base as usize + PARALLEL_INDEX_SCAN_DESC_HEADER_SIZE) as *const u8;
        // SAFETY: leader-written-once before the launch barrier, read-only
        // thereafter; the bytes live for the DSM segment. The serialized MVCC
        // snapshot header (24 bytes) is always present.
        const SERIALIZED_HEADER_LEN: usize = 24;
        let hdr = unsafe { core::slice::from_raw_parts(snap, SERIALIZED_HEADER_LEN) };
        let xcnt = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
        let subxcnt = i32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]).max(0) as usize;
        let total = SERIALIZED_HEADER_LEN + (xcnt + subxcnt) * 4;
        // SAFETY: as above; `total` is exactly the serialized length the leader
        // wrote, never larger than the chunk's snapshot region.
        unsafe { core::slice::from_raw_parts(snap, total) }
    }
}
