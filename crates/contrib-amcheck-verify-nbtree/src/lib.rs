#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult`; the un-boxed
// `Err(PgError)` variant trips `clippy::result_large_err`, which is the
// project's error contract. Accept it crate-wide.
#![allow(clippy::result_large_err)]
// During the decomposition scaffold every family body is mirror-and-panic, so
// the imported real types / helper structs are not all referenced yet.
#![allow(dead_code)]

//! `contrib-amcheck-verify-nbtree` — port of
//! `contrib/amcheck/verify_nbtree.c`.
//!
//! amcheck's B-tree verifier: the `bt_index_check` / `bt_index_parent_check`
//! SQL functions walk an nbtree index in logical order and check every
//! structural invariant — per-page item ordering and tuple shape, cross-page
//! high key boundaries, parent and child link consistency, sibling-link
//! consistency — and, optionally, that the heap contains no tuples missing
//! from the index (`heapallindexed`, via a Bloom filter) and that a UNIQUE
//! index has no duplicate live entries (`checkunique`).
//!
//! ## Decomposition (4 families)
//!
//! * **F0 — nbtree-core keystone** (no module here): the scankey / search /
//!   tree-descent vocabulary the verifier leans on lives in `types-nbtree`
//!   ([`types_nbtree::BTScanInsertData`] / [`types_nbtree::BTStackData`] /
//!   [`types_nbtree::BTInsertStateData`]) plus the `_bt_mkscankey` /
//!   `_bt_compare` / `_bt_search` / `_bt_moveright` / `_bt_binsrch[_insert]` /
//!   `_bt_metaversion` / `_bt_check_natts` / `_bt_form_posting` /
//!   `_bt_freestack` / `_bt_allequalimage` seam declarations added to
//!   `backend-access-nbtree-core-seams` (owner `backend-access-nbtree-core`,
//!   still `todo` — they panic until it lands).
//! * **F1 — [`entry`]**: the SQL entry points, the level-by-level harness, the
//!   heapallindexed Bloom-filter feed + heap-visibility probe, and the
//!   careful-read page/line-pointer helpers. Owns [`BtreeCheckState`].
//! * **F2 — [`target_page`]**: the central per-page item-by-item invariant
//!   checker and the cross-page right-boundary scankey machinery.
//! * **F3 — [`linkage`]**: parent/child downlink verification, sibling-link
//!   recheck, root-descend re-find, and the downlink byte-math helpers.

use mcx::{MemoryContext, Mcx, PgVec};

/// The verification arena handle (`Mcx<'mcx>`), an alias for clarity at the
/// [`BtreeCheckState`] field.
pub type MemoryContextHandle<'mcx> = Mcx<'mcx>;
use types_core::primitive::{BlockNumber, OffsetNumber, XLogRecPtr};
use types_nodes::execnodes::IndexInfo;
use types_rel::Relation;
use types_storage::buf::BufferAccessStrategy;
use types_tableam::tableam::Snapshot;
use types_tuple::heaptuple::{IndexTuple, ItemPointerData};

use backend_lib_bloomfilter_seams::BloomFilter;

pub mod entry;
pub mod linkage;
pub mod target_page;

/// `Page` — a heap-`palloc(BLCKSZ)` copy of an on-disk page (verify_nbtree.c
/// works on private copies, never the shared buffer). Owned bytes in `'mcx`.
pub type Page<'mcx> = PgVec<'mcx, u8>;

/// `BtreeCheckState` (verify_nbtree.c) — the working state threaded through
/// the entire B-tree verification. Unchanging fields are set up at the start
/// of a check; the mutable block is reset per target page.
pub struct BtreeCheckState<'mcx> {
    /// The verification arena handle (`state->targetcontext`'s allocator). All
    /// per-page private copies and scankey allocations are made here, matching
    /// C's `state->targetcontext`. Carried explicitly so helpers can allocate
    /// before the first page is read (e.g. the metapage in `palloc_btree_page`).
    pub mcx: MemoryContextHandle<'mcx>,

    // --- Unchanging state, established at start of verification ---
    /// `rel` — the B-tree index relation being verified.
    pub rel: Relation<'mcx>,
    /// `heaprel` — the heap relation the index is built on.
    pub heaprel: Relation<'mcx>,
    /// `heapkeyspace` — is `rel` a heapkeyspace (version >= 4) index?
    pub heapkeyspace: bool,
    /// `readonly` — ShareLock held (cross-level checks safe), vs
    /// AccessShareLock.
    pub readonly: bool,
    /// `heapallindexed` — also verify the heap has no unindexed tuples?
    pub heapallindexed: bool,
    /// `rootdescend` — also re-find non-pivot tuples via a fresh search?
    pub rootdescend: bool,
    /// `checkunique` — also check the uniqueness constraint if unique?
    pub checkunique: bool,
    /// `targetcontext` — per-page memory context.
    pub targetcontext: MemoryContext,
    /// `checkstrategy` — buffer access strategy for the page reads.
    pub checkstrategy: BufferAccessStrategy,

    // --- Info for uniqueness checking (filled once per index check) ---
    /// `indexinfo` — index metadata used by the uniqueness check.
    pub indexinfo: Option<IndexInfo<'mcx>>,
    /// `snapshot` — table-scan snapshot for heapallindexed / checkunique.
    pub snapshot: Snapshot,

    // --- Mutable state, for verification of the particular page ---
    /// `target` — the current target page (private copy).
    pub target: Option<Page<'mcx>>,
    /// `targetblock` — the target's block number.
    pub targetblock: BlockNumber,
    /// `targetlsn` — the target page's LSN.
    pub targetlsn: XLogRecPtr,
    /// `lowkey` — high key of the left sibling of the target page; kept only
    /// when `readonly` (used for child verification).
    pub lowkey: IndexTuple<'mcx>,
    /// `prevrightlink` — rightlink of the block one level down, last visited
    /// via a downlink from the target (for missing-downlink detection).
    pub prevrightlink: BlockNumber,
    /// `previncompletesplit` — incomplete-split flag of that block.
    pub previncompletesplit: bool,

    // --- Mutable state, for optional heapallindexed verification ---
    /// `filter` — the Bloom filter fingerprinting the B-tree index.
    pub filter: Option<BloomFilter>,
    /// `heaptuplespresent` — debug counter of heap tuples seen.
    pub heaptuplespresent: i64,
}

/// `BtreeLevel` (verify_nbtree.c) — the starting point for verifying one
/// entire B-tree level.
#[derive(Clone, Copy, Debug)]
pub struct BtreeLevel {
    /// `level` — level number (0 is the leaf level).
    pub level: u32,
    /// `leftmost` — leftmost block on the level; the scan begins here.
    pub leftmost: BlockNumber,
    /// `istruerootlevel` — is this the metapage's reported "true" root level?
    pub istruerootlevel: bool,
}

/// `BtreeLastVisibleEntry` (verify_nbtree.c) — info about the last visible
/// entry with the current B-tree key, used to validate the unique constraint.
#[derive(Clone, Copy, Debug)]
pub struct BtreeLastVisibleEntry {
    /// `blkno` — index block.
    pub blkno: BlockNumber,
    /// `offset` — offset on the index block.
    pub offset: OffsetNumber,
    /// `postingIndex` — position in the posting list (`-1` for
    /// non-deduplicated tuples).
    pub postingIndex: i32,
    /// `tid` — heap TID of the last visible entry, if any.
    pub tid: Option<ItemPointerData>,
}

/// Install every inward seam this unit owns (the two SQL entry points).
///
/// `verify_nbtree.c` is a leaf consumer: its only inward surface is the
/// `bt_index_check` / `bt_index_parent_check` fmgr entry points, declared in
/// `contrib-amcheck-verify-nbtree-seams`. Both are installed here, exactly
/// once, wired into `seams-init::init_all`.
pub fn init_seams() {
    use contrib_amcheck_verify_nbtree_seams as nbtree_seams;

    nbtree_seams::bt_index_check::set(entry::bt_index_check);
    nbtree_seams::bt_index_parent_check::set(entry::bt_index_parent_check);
}
