//! Shared file-static core for the idiomatic `vacuumlazy.c` port.
//!
//! Holds the items `vacuumlazy.c` declares at file scope and that every other
//! module references:
//!
//!   * the [`VacErrPhase`] enum (`vacuumlazy.c:224-232`),
//!   * the central per-relation working state [`LVRelState`]
//!     (`vacuumlazy.c:259-411`) — threaded through almost every function as
//!     `&mut LVRelState`,
//!   * the error save/restore record [`LVSavedErrInfo`] (`vacuumlazy.c:415-420`),
//!   * the file-scope `#define`s (`vacuumlazy.c:169-256`) as `pub const`s, and
//!   * the [`ParallelVacuumIsActive`] helper (`vacuumlazy.c:221`).
//!
//! ## Idiomatic shape
//!
//! Unlike the C-ABI faithful port (raw `*mut` and opaque `c_void`), the owned
//! model:
//!
//!   * keeps the heap relation and its indexes as their bare `Oid` relcache
//!     identities (`RelationGetRelid`) the substrate re-resolves on demand (the
//!     engine never dereferences a relation; the former `RelationHandle`
//!     wrapper was retired to Oid-via-relcache);
//!   * owns the error-reporting names as `String` (the C `pstrdup`-ed
//!     `char *`s);
//!   * holds the index relations / per-index stats as `Vec`s rather than raw
//!     `Relation *` / `IndexBulkDeleteResult **`;
//!   * reaches the dead-TID store, parallel-vacuum state, and visibility test
//!     through the small substrate handles defined in
//!     [`seams_ub_heaprest::vacuumlazy`].

use mcx::Mcx;
use types_rel::Relation;
use types_core::{
    BlockNumber, Buffer, MultiXactId, OffsetNumber, TransactionId, BLCKSZ,
};
use types_vacuum::vacuum::VacuumCutoffs;
use types_vacuum::vacuumparallel::{IndexBulkDeleteResult, VacDeadItemsInfo};

use types_vacuum::vacuumlazy::{
    GlobalVisStateHandle, ParallelVacuumStateHandle, StrategyHandle, TidStore,
};

// ===========================================================================
// File-scope constants (vacuumlazy.c:169-256).
// ===========================================================================

/// `REL_TRUNCATE_MINIMUM` (vacuumlazy.c:169).
pub const REL_TRUNCATE_MINIMUM: BlockNumber = 1000;
/// `REL_TRUNCATE_FRACTION` (vacuumlazy.c:170).
pub const REL_TRUNCATE_FRACTION: BlockNumber = 16;

/// `VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL` (vacuumlazy.c:179) — ms.
pub const VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL: i32 = 20;
/// `VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL` (vacuumlazy.c:180) — ms.
pub const VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL: i32 = 50;
/// `VACUUM_TRUNCATE_LOCK_TIMEOUT` (vacuumlazy.c:181) — ms.
pub const VACUUM_TRUNCATE_LOCK_TIMEOUT: i32 = 5000;

/// `BYPASS_THRESHOLD_PAGES` (vacuumlazy.c:187) — 2% of rel_pages.
pub const BYPASS_THRESHOLD_PAGES: f64 = 0.02;

/// `FAILSAFE_EVERY_PAGES` (vacuumlazy.c:193) — `(4*1024*1024*1024) / BLCKSZ`.
pub const FAILSAFE_EVERY_PAGES: BlockNumber =
    ((4u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;

/// `VACUUM_FSM_EVERY_PAGES` (vacuumlazy.c:202) — `(8*1024*1024*1024) / BLCKSZ`.
pub const VACUUM_FSM_EVERY_PAGES: BlockNumber =
    ((8u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;

/// `SKIP_PAGES_THRESHOLD` (vacuumlazy.c:209).
pub const SKIP_PAGES_THRESHOLD: BlockNumber = 32;

/// `PREFETCH_SIZE` (vacuumlazy.c:215). Must be a power of 2.
pub const PREFETCH_SIZE: BlockNumber = 32;

/// `MAX_EAGER_FREEZE_SUCCESS_RATE` (vacuumlazy.c:241).
pub const MAX_EAGER_FREEZE_SUCCESS_RATE: f64 = 0.2;

/// `EAGER_SCAN_REGION_SIZE` (vacuumlazy.c:250).
pub const EAGER_SCAN_REGION_SIZE: BlockNumber = 4096;

/// `VAC_BLK_WAS_EAGER_SCANNED` (vacuumlazy.c:256).
pub const VAC_BLK_WAS_EAGER_SCANNED: u8 = 1 << 0;
/// `VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM` (vacuumlazy.c:257).
pub const VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM: u8 = 1 << 1;

// ===========================================================================
// VacErrPhase (vacuumlazy.c:224-232).
// ===========================================================================

/// `typedef enum { ... } VacErrPhase` (vacuumlazy.c:224-232).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VacErrPhase {
    /// `VACUUM_ERRCB_PHASE_UNKNOWN`
    Unknown,
    /// `VACUUM_ERRCB_PHASE_SCAN_HEAP`
    ScanHeap,
    /// `VACUUM_ERRCB_PHASE_VACUUM_INDEX`
    VacuumIndex,
    /// `VACUUM_ERRCB_PHASE_VACUUM_HEAP`
    VacuumHeap,
    /// `VACUUM_ERRCB_PHASE_INDEX_CLEANUP`
    IndexCleanup,
    /// `VACUUM_ERRCB_PHASE_TRUNCATE`
    Truncate,
}

// ===========================================================================
// LVRelState (vacuumlazy.c:259-411).
// ===========================================================================

/// `typedef struct LVRelState` (vacuumlazy.c:259-411) — the central per-relation
/// working state, allocated once per relation and threaded through almost every
/// function as `&mut LVRelState`.
pub struct LVRelState<'mcx> {
    /// The memory context the driver run owns (the `PlannerRun<'mcx>` analog).
    /// Held for the whole scan so owned `Relation`s and other arena values
    /// share its lifetime.
    pub mcx: Mcx<'mcx>,
    // -- Target heap relation and its indexes --
    /// `Relation rel` — the live, open heap relation, held across the whole
    /// scan/truncate/stats (C: `vacrel->rel = rel`, vacuumlazy.c:681).
    pub rel: Relation<'mcx>,
    /// `Relation *indrels` — the open index relations.
    pub indrels: Vec<Relation<'mcx>>,
    /// `int nindexes`.
    pub nindexes: i32,

    // -- Buffer access strategy and parallel vacuum state --
    /// `BufferAccessStrategy bstrategy`.
    pub bstrategy: StrategyHandle,
    /// `ParallelVacuumState *pvs`.
    pub pvs: ParallelVacuumStateHandle,

    /// `bool aggressive`.
    pub aggressive: bool,
    /// `bool skipwithvm`.
    pub skipwithvm: bool,
    /// `bool consider_bypass_optimization`.
    pub consider_bypass_optimization: bool,

    /// `bool do_index_vacuuming`.
    pub do_index_vacuuming: bool,
    /// `bool do_index_cleanup`.
    pub do_index_cleanup: bool,
    /// `bool do_rel_truncate`.
    pub do_rel_truncate: bool,

    /// `struct VacuumCutoffs cutoffs`.
    pub cutoffs: VacuumCutoffs,
    /// `GlobalVisState *vistest`.
    pub vistest: GlobalVisStateHandle,
    /// `TransactionId NewRelfrozenXid`.
    pub new_relfrozen_xid: TransactionId,
    /// `MultiXactId NewRelminMxid`.
    pub new_relmin_mxid: MultiXactId,
    /// `bool skippedallvis`.
    pub skippedallvis: bool,

    // -- Error reporting state --
    /// `char *dbname`.
    pub dbname: String,
    /// `char *relnamespace`.
    pub relnamespace: String,
    /// `char *relname`.
    pub relname: String,
    /// `char *indname` — current index name (None when not in an index phase).
    pub indname: Option<String>,
    /// `BlockNumber blkno`.
    pub blkno: BlockNumber,
    /// `OffsetNumber offnum`.
    pub offnum: OffsetNumber,
    /// `VacErrPhase phase`.
    pub phase: VacErrPhase,
    /// `bool verbose`.
    pub verbose: bool,

    /// `TidStore *dead_items`.
    pub dead_items: TidStore,
    /// `VacDeadItemsInfo *dead_items_info`.
    pub dead_items_info: VacDeadItemsInfo,

    /// `BlockNumber rel_pages`.
    pub rel_pages: BlockNumber,
    /// `BlockNumber scanned_pages`.
    pub scanned_pages: BlockNumber,
    /// `BlockNumber eager_scanned_pages`.
    pub eager_scanned_pages: BlockNumber,
    /// `BlockNumber removed_pages`.
    pub removed_pages: BlockNumber,
    /// `BlockNumber new_frozen_tuple_pages`.
    pub new_frozen_tuple_pages: BlockNumber,
    /// `BlockNumber vm_new_visible_pages`.
    pub vm_new_visible_pages: BlockNumber,
    /// `BlockNumber vm_new_visible_frozen_pages`.
    pub vm_new_visible_frozen_pages: BlockNumber,
    /// `BlockNumber vm_new_frozen_pages`.
    pub vm_new_frozen_pages: BlockNumber,
    /// `BlockNumber lpdead_item_pages`.
    pub lpdead_item_pages: BlockNumber,
    /// `BlockNumber missed_dead_pages`.
    pub missed_dead_pages: BlockNumber,
    /// `BlockNumber nonempty_pages` — last nonempty page + 1.
    pub nonempty_pages: BlockNumber,

    /// `double new_rel_tuples`.
    pub new_rel_tuples: f64,
    /// `double new_live_tuples`.
    pub new_live_tuples: f64,
    /// `IndexBulkDeleteResult **indstats`.
    pub indstats: Vec<Option<IndexBulkDeleteResult>>,

    /// `int num_index_scans`.
    pub num_index_scans: i32,
    /// `int64 tuples_deleted`.
    pub tuples_deleted: i64,
    /// `int64 tuples_frozen`.
    pub tuples_frozen: i64,
    /// `int64 lpdead_items`.
    pub lpdead_items: i64,
    /// `int64 live_tuples`.
    pub live_tuples: i64,
    /// `int64 recently_dead_tuples`.
    pub recently_dead_tuples: i64,
    /// `int64 missed_dead_tuples`.
    pub missed_dead_tuples: i64,

    // -- State maintained by heap_vac_scan_next_block() --
    /// `BlockNumber current_block`.
    pub current_block: BlockNumber,
    /// `BlockNumber next_unskippable_block`.
    pub next_unskippable_block: BlockNumber,
    /// `bool next_unskippable_allvis`.
    pub next_unskippable_allvis: bool,
    /// `bool next_unskippable_eager_scanned`.
    pub next_unskippable_eager_scanned: bool,
    /// `Buffer next_unskippable_vmbuffer`.
    pub next_unskippable_vmbuffer: Buffer,

    // -- Eager-scan management --
    /// `BlockNumber next_eager_scan_region_start`.
    pub next_eager_scan_region_start: BlockNumber,
    /// `BlockNumber eager_scan_remaining_successes`.
    pub eager_scan_remaining_successes: BlockNumber,
    /// `BlockNumber eager_scan_max_fails_per_region`.
    pub eager_scan_max_fails_per_region: BlockNumber,
    /// `BlockNumber eager_scan_remaining_fails`.
    pub eager_scan_remaining_fails: BlockNumber,
}

impl<'mcx> LVRelState<'mcx> {
    /// The `palloc0`-equivalent freshly-zeroed state (`heap_vacuum_rel` allocates
    /// `LVRelState` with `palloc0`), holding the run's `mcx` and the live open
    /// heap `rel` (C: `vacrel->rel = rel`). Handle/`Vec`/`String` fields take
    /// their null/empty values.
    pub fn new_zeroed(mcx: Mcx<'mcx>, rel: Relation<'mcx>) -> LVRelState<'mcx> {
        LVRelState {
            mcx,
            rel,
            indrels: Vec::new(),
            nindexes: 0,
            bstrategy: StrategyHandle::none(),
            pvs: ParallelVacuumStateHandle::none(),
            aggressive: false,
            skipwithvm: false,
            consider_bypass_optimization: false,
            do_index_vacuuming: false,
            do_index_cleanup: false,
            do_rel_truncate: false,
            cutoffs: VacuumCutoffs::default(),
            vistest: GlobalVisStateHandle::default(),
            new_relfrozen_xid: 0,
            new_relmin_mxid: 0,
            skippedallvis: false,
            dbname: String::new(),
            relnamespace: String::new(),
            relname: String::new(),
            indname: None,
            blkno: 0,
            offnum: 0,
            phase: VacErrPhase::Unknown,
            verbose: false,
            dead_items: TidStore::none(),
            dead_items_info: VacDeadItemsInfo::default(),
            rel_pages: 0,
            scanned_pages: 0,
            eager_scanned_pages: 0,
            removed_pages: 0,
            new_frozen_tuple_pages: 0,
            vm_new_visible_pages: 0,
            vm_new_visible_frozen_pages: 0,
            vm_new_frozen_pages: 0,
            lpdead_item_pages: 0,
            missed_dead_pages: 0,
            nonempty_pages: 0,
            new_rel_tuples: 0.0,
            new_live_tuples: 0.0,
            indstats: Vec::new(),
            num_index_scans: 0,
            tuples_deleted: 0,
            tuples_frozen: 0,
            lpdead_items: 0,
            live_tuples: 0,
            recently_dead_tuples: 0,
            missed_dead_tuples: 0,
            current_block: 0,
            next_unskippable_block: 0,
            next_unskippable_allvis: false,
            next_unskippable_eager_scanned: false,
            next_unskippable_vmbuffer: 0,
            next_eager_scan_region_start: 0,
            eager_scan_remaining_successes: 0,
            eager_scan_max_fails_per_region: 0,
            eager_scan_remaining_fails: 0,
        }
    }
}

// ===========================================================================
// LVSavedErrInfo (vacuumlazy.c:415-420).
// ===========================================================================

/// `typedef struct LVSavedErrInfo` (vacuumlazy.c:415-420).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LVSavedErrInfo {
    /// `BlockNumber blkno`.
    pub blkno: BlockNumber,
    /// `OffsetNumber offnum`.
    pub offnum: OffsetNumber,
    /// `VacErrPhase phase`.
    pub phase: VacErrPhase,
}

/// `ParallelVacuumIsActive(vacrel)` (vacuumlazy.c:221) — true iff we are in
/// parallel mode and the DSM segment is initialized (`vacrel->pvs != NULL`).
#[inline]
pub fn parallel_vacuum_is_active(vacrel: &LVRelState<'_>) -> bool {
    !vacrel.pvs.is_none()
}
