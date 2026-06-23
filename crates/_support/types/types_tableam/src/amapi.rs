//! `access/amapi.h` — the index-access-method dispatch vocabulary: the unified
//! `IndexAmRoutine` vtable, `IndexUniqueCheck`, `CompareType`, and the pure
//! translate/validate callback types.
//!
//! The model mirrors [`crate::tableam::TableAmRoutine`]: `rd_indam` is a vtable
//! of function pointers the per-AM implementation (nbtree / hash / gist / gin /
//! spgist / brin) installs (its handler — `bthandler` / `hashhandler` —
//! assembles and returns it), and the dispatch layer (`access/index/indexam.c`)
//! reads the boolean property flags and invokes the callbacks through
//! `relation->rd_indam`.
//!
//! This is the ONE `IndexAmRoutine` for the whole tree (F2 of the index-AM
//! tower): it carries both the property flags the optimizer/catalog code reads
//! AND the scan/insert/vacuum callbacks the dispatch layer invokes. The
//! allocating scan/insert/vacuum callbacks thread `mcx` (convention A: leading
//! `mcx: Mcx<'mcx>`), kept as HRTB `for<'mcx> fn(...)` pointers so the struct
//! stays `Copy`/lifetime-free. `types-tableam` sits below `types-nodes` so it
//! cannot name `NodeTag`; `type_`/[`T_IndexAmRoutine`] are bare `u32`.

#![allow(non_upper_case_globals)]

use std::boxed::Box;
use std::string::String;
use std::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_tuple::heaptuple::Datum;
use types_error::PgResult;
use rel::Relation;
use types_scan::sdir::ScanDirection;
use types_scan::scankey::{ScanKeyData, StrategyNumber};
use types_tuple::heaptuple::ItemPointerData;

use crate::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use crate::index_info_carrier::IndexInfoCarrier;
use crate::relscan::{IndexScanDesc, IndexScanDescData};

/// `T_IndexAmRoutine` (`nodes/nodetags.h`) as a bare `u32` (`types-tableam`
/// sits below `types-nodes` and cannot name `NodeTag`).
pub const T_IndexAmRoutine: u32 = 438;

/// `IndexUniqueCheck` (`access/genam.h`, used through amapi.h) — the
/// uniqueness-check mode passed to `aminsert`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexUniqueCheck {
    /// Don't do any uniqueness checking.
    UNIQUE_CHECK_NO = 0,
    /// Enforce uniqueness at insertion time.
    UNIQUE_CHECK_YES,
    /// Test uniqueness, but no error.
    UNIQUE_CHECK_PARTIAL,
    /// Check if existing tuple is unique.
    UNIQUE_CHECK_EXISTING,
}

/// `CompareType` (`access/cmptype.h`) — the AM-independent comparison
/// operator categories an opclass can expose. Values verified against the C
/// header (PostgreSQL 18.3).
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CompareType {
    COMPARE_INVALID = 0,
    /// `BTLessStrategyNumber`
    COMPARE_LT = 1,
    /// `BTLessEqualStrategyNumber`
    COMPARE_LE = 2,
    /// `BTEqualStrategyNumber`
    COMPARE_EQ = 3,
    /// `BTGreaterEqualStrategyNumber`
    COMPARE_GE = 4,
    /// `BTGreaterStrategyNumber`
    COMPARE_GT = 5,
    /// no such btree strategy
    COMPARE_NE = 6,
    COMPARE_OVERLAP = 7,
    COMPARE_CONTAINED_BY = 8,
}

pub use CompareType::{
    COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID, COMPARE_LE,
    COMPARE_LT, COMPARE_NE, COMPARE_OVERLAP,
};

/// `IndexBuildResult` (`access/genam.h`) — statistics returned by `ambuild`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexBuildResult {
    /// `double heap_tuples` — # of heap tuples scanned.
    pub heap_tuples: f64,
    /// `double index_tuples` — # of index tuples created.
    pub index_tuples: f64,
}

/// The five output estimates `amcostestimate` writes through its
/// `*indexStartupCost` / `*indexTotalCost` / `*indexSelectivity` /
/// `*indexCorrelation` / `*indexPages` pointer arguments, returned as a value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AmCostEstimate {
    /// `*indexStartupCost`
    pub index_startup_cost: Cost,
    /// `*indexTotalCost`
    pub index_total_cost: Cost,
    /// `*indexSelectivity`
    pub index_selectivity: Selectivity,
    /// `*indexCorrelation` (`double`)
    pub index_correlation: f64,
    /// `*indexPages` (`double`)
    pub index_pages: f64,
}

/// `amtranslate_strategy` callback (`access/amapi.h`).
pub type IndexAmTranslateStrategy = fn(StrategyNumber, Oid) -> CompareType;
/// `amtranslate_cmptype` callback (`access/amapi.h`).
pub type IndexAmTranslateCompareType = fn(CompareType, Oid) -> StrategyNumber;
/// `amvalidate` callback (`access/amapi.h`), the raw `fn(Oid) -> bool` ABI
/// shape. Validators that return a soft-error result instead are reached by
/// name from their AM crate, not stored here.
pub type IndexAmValidate = fn(Oid) -> bool;

/// `struct PlannerInfo` (`nodes/pathnodes.h`) — opaque to `types-tableam`
/// (which sits below `types-nodes` and cannot name the planner structs).
/// `amcostestimate` only forwards it to the AM's cost routine; carried as a
/// type-erased payload the AM callback can downcast.
pub struct PlannerInfo {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `struct IndexPath` (`nodes/pathnodes.h`) — opaque to `types-tableam`; see
/// [`PlannerInfo`]. Forwarded to the AM's `amcostestimate` callback.
pub struct IndexPath {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `Cost` (`nodes/nodes.h`) — `typedef double Cost`.
pub type Cost = f64;
/// `Selectivity` (`nodes/nodes.h`) — `typedef double Selectivity`.
pub type Selectivity = f64;

/// `IndexAMProperty` (`access/amapi.h`) — the index-AM property codes passed to
/// an AM's `amproperty` callback. Discriminants match the C enum.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAMProperty {
    /// anything not known to core code
    AMPROP_UNKNOWN = 0,
    /// column properties
    AMPROP_ASC,
    AMPROP_DESC,
    AMPROP_NULLS_FIRST,
    AMPROP_NULLS_LAST,
    AMPROP_ORDERABLE,
    AMPROP_DISTANCE_ORDERABLE,
    AMPROP_RETURNABLE,
    AMPROP_SEARCH_ARRAY,
    AMPROP_SEARCH_NULLS,
    /// index properties
    AMPROP_CLUSTERABLE,
    AMPROP_INDEX_SCAN,
    AMPROP_BITMAP_SCAN,
    AMPROP_BACKWARD_SCAN,
    /// AM properties
    AMPROP_CAN_ORDER,
    AMPROP_CAN_UNIQUE,
    AMPROP_CAN_MULTI_COL,
    AMPROP_CAN_EXCLUDE,
    AMPROP_CAN_INCLUDE,
}

/// `OpFamilyMember` (`access/amapi.h`) — tracks an operator or support function
/// while building/adding to an opclass or opfamily. `amadjustmembers` receives
/// lists of these and may alter the `ref` fields.
#[derive(Clone, Copy, Debug)]
pub struct OpFamilyMember {
    /// is this an operator, or support func?
    pub is_func: bool,
    /// operator or support func's OID
    pub object: Oid,
    /// strategy or support func number
    pub number: i32,
    /// lefttype
    pub lefttype: Oid,
    /// righttype
    pub righttype: Oid,
    /// ordering operator's sort opfamily, or 0
    pub sortfamily: Oid,
    /// hard or soft dependency?
    pub ref_is_hard: bool,
    /// is dependency on opclass or opfamily?
    pub ref_is_family: bool,
    /// OID of opclass or opfamily
    pub refobjid: Oid,
}

/// `TIDBitmap` (`nodes/tidbitmap.h`) — opaque to the index-AM dispatch layer:
/// indexam.c only forwards it to `amgetbitmap`. Owned by the tidbitmap
/// subsystem; carried here as a type-erased payload.
pub struct TIDBitmap {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `IndexAmRoutine` (`access/amapi.h`) — the unified index-access-method vtable
/// the AM handler assembles and returns. Carries the property flags the
/// optimizer/catalog code reads AND the callbacks the dispatch layer invokes.
/// Callbacks the C marks `/* can be NULL */` are `Option<fn>`; the required
/// interface functions are plain `fn`. The allocating scan/insert/vacuum
/// callbacks thread `mcx` (convention A, leading `mcx: Mcx<'mcx>`).
#[derive(Clone, Debug)]
pub struct IndexAmRoutine {
    /// `NodeTag type` — modeled as bare `u32` (= [`T_IndexAmRoutine`]).
    pub type_: u32,

    /* ---- property flags (read by optimizer / catalog / relcache) ---- */
    /// total number of strategies (operators) by which we can traverse/search
    pub amstrategies: u16,
    /// `uint16 amsupport` — total number of support functions this AM uses.
    pub amsupport: u16,
    /// opclass options support function number or 0
    pub amoptsprocnum: u16,
    /// does AM support ORDER BY indexed column's value?
    pub amcanorder: bool,
    /// does AM support ORDER BY result of an operator on indexed column?
    pub amcanorderbyop: bool,
    /// does AM support hashing of indexed column?
    pub amcanhash: bool,
    /// does AM consider opclasses with the same equality semantics equivalent?
    pub amconsistentequality: bool,
    /// does AM consider opclasses with the same ordering semantics equivalent?
    pub amconsistentordering: bool,
    /// can AM be used by backwards scan?
    pub amcanbackward: bool,
    /// does AM support UNIQUE indexes?
    pub amcanunique: bool,
    /// does AM support multi-column indexes?
    pub amcanmulticol: bool,
    /// can query omit key for the first column?
    pub amoptionalkey: bool,
    /// can AM handle ScalarArrayOpExpr quals?
    pub amsearcharray: bool,
    /// can AM handle IS NULL/NOT NULL quals?
    pub amsearchnulls: bool,
    /// can storage type differ from column type?
    pub amstorage: bool,
    /// can index be clustered on this AM?
    pub amclusterable: bool,
    /// does AM handle predicate locks?
    pub ampredlocks: bool,
    /// does AM support parallel scan?
    pub amcanparallel: bool,
    /// does AM support parallel build?
    pub amcanbuildparallel: bool,
    /// does AM support columns included with clause INCLUDE?
    pub amcaninclude: bool,
    /// does AM use maintenance_work_mem?
    pub amusemaintenanceworkmem: bool,
    /// does AM store tuple information only at block granularity?
    pub amsummarizing: bool,
    /// OR of parallel vacuum flags
    pub amparallelvacuumoptions: u8,
    /// type of data stored in index, or InvalidOid if variable
    pub amkeytype: Oid,

    /* ---- pure translate / validate callbacks ---- */
    pub amtranslatestrategy: Option<IndexAmTranslateStrategy>,
    pub amtranslatecmptype: Option<IndexAmTranslateCompareType>,
    pub amvalidate: Option<IndexAmValidate>,

    /* ---- required interface functions invoked by indexam.c ---- */
    /// `ambuild(heapRelation, indexRelation, indexInfo)` — build a new index.
    /// `indexInfo` is the lifetime-preserving [`IndexInfoCarrier`] (the dispatch
    /// layer sits below `types-nodes` and cannot name
    /// `nodes::execnodes::IndexInfo<'mcx>`); the AM adapter downcasts it.
    pub ambuild: for<'mcx, 'a> fn(
        mcx: Mcx<'mcx>,
        heap_relation: &Relation<'mcx>,
        index_relation: &Relation<'mcx>,
        index_info: &mut IndexInfoCarrier<'a, 'mcx>,
    ) -> PgResult<IndexBuildResult>,

    /// `ambuildempty(indexRelation)` — build an empty index for the init fork.
    pub ambuildempty:
        for<'mcx> fn(mcx: Mcx<'mcx>, index_relation: &Relation<'mcx>) -> PgResult<()>,

    /// `aminsert(indexRelation, values, isnull, heap_tid, heapRelation,
    /// checkUnique, indexUnchanged, indexInfo)`.
    #[allow(clippy::type_complexity)]
    pub aminsert: for<'mcx, 'a> fn(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap_relation: &Relation<'mcx>,
        check_unique: IndexUniqueCheck,
        index_unchanged: bool,
        index_info: &mut IndexInfoCarrier<'a, 'mcx>,
    ) -> PgResult<bool>,

    /// `ambulkdelete(info, stats, callback, callback_state)`. The deletion
    /// callback + its state live in the vacuum substrate; `callback_state` is
    /// the handle keying the `vacuum_tid_is_dead` seam (`None` is the
    /// cleanup-only NULL callback). `stats` is `None` for the C `NULL`.
    #[allow(clippy::type_complexity)]
    pub ambulkdelete: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
        callback_state: Option<u64>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>,

    /// `amvacuumcleanup(info, stats)`.
    pub amvacuumcleanup: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        info: &IndexVacuumInfo<'mcx>,
        stats: Option<IndexBulkDeleteResult>,
    ) -> PgResult<Option<IndexBulkDeleteResult>>,

    /// `ambeginscan(indexRelation, nkeys, norderbys)` — prepare for an index
    /// scan; the AM allocates and returns the scan descriptor.
    pub ambeginscan: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        index_relation: &Relation<'mcx>,
        nkeys: i32,
        norderbys: i32,
    ) -> PgResult<IndexScanDesc<'mcx>>,

    /// `amrescan(scan, keys, nkeys, orderbys, norderbys)`.
    pub amrescan: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        scan: &mut IndexScanDescData<'mcx>,
        keys: &[ScanKeyData<'mcx>],
        orderbys: &[ScanKeyData<'mcx>],
    ) -> PgResult<()>,

    /// `amendscan(scan)`.
    pub amendscan: for<'mcx> fn(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()>,

    /* ---- optional interface functions ("can be NULL" in C) ---- */
    /// `aminsertcleanup(indexRelation, indexInfo)`.
    pub aminsertcleanup: Option<
        for<'mcx, 'a> fn(
            mcx: Mcx<'mcx>,
            index_relation: &Relation<'mcx>,
            index_info: &mut IndexInfoCarrier<'a, 'mcx>,
        ) -> PgResult<()>,
    >,

    /// `amcanreturn(indexRelation, attno)` — does the AM support index-only
    /// scans for the given column?
    pub amcanreturn: Option<fn(index_relation: &Relation<'_>, attno: i32) -> PgResult<bool>>,

    /* ---- planning / build-time interface functions ---- */
    /// `amcostestimate(root, path, loop_count, *indexStartupCost,
    /// *indexTotalCost, *indexSelectivity, *indexCorrelation, *indexPages)` —
    /// estimate the cost of an index scan. `root`/`path` are the type-erased
    /// planner carriers ([`PlannerInfo`]/[`IndexPath`]); the output estimates
    /// the C writes through pointers are returned in the tuple. Required (the
    /// C field is not marked `can be NULL`).
    #[allow(clippy::type_complexity)]
    pub amcostestimate: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        root: &mut PlannerInfo,
        path: &mut IndexPath,
        loop_count: f64,
    ) -> PgResult<AmCostEstimate>,

    /// `amgettreeheight(rel)` — estimate height of a tree-structured index.
    /// `/* can be NULL */`.
    pub amgettreeheight: Option<for<'mcx> fn(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<i32>>,

    /// `amoptions(reloptions, validate)` — parse index reloptions, returning the
    /// packed `bytea` options blob (`None` for the C `NULL`).
    pub amoptions:
        for<'mcx> fn(mcx: Mcx<'mcx>, reloptions: Datum<'mcx>, validate: bool) -> PgResult<Option<Vec<u8>>>,

    /// `amproperty(index_oid, attno, prop, propname, *res, *isnull)` — report
    /// an AM/index/column property. Returns `true` if the property was handled
    /// (writing `res`/`isnull`), `false` to punt to generic code.
    /// `/* can be NULL */`.
    #[allow(clippy::type_complexity)]
    pub amproperty: Option<
        fn(
            index_oid: Oid,
            attno: i32,
            prop: IndexAMProperty,
            propname: &str,
            res: &mut bool,
            isnull: &mut bool,
        ) -> PgResult<bool>,
    >,

    /// `ambuildphasename(phasenum)` — name of a build phase for progress
    /// reporting, or `None` (C `NULL`). `/* can be NULL */`.
    pub ambuildphasename: Option<fn(phasenum: i64) -> Option<String>>,

    /// `amadjustmembers(opfamilyoid, opclassoid, operators, functions)` —
    /// validate operators/support functions being added to an opclass/family
    /// and adjust their dependency `ref` fields in place. `/* can be NULL */`.
    #[allow(clippy::type_complexity)]
    pub amadjustmembers: Option<
        for<'mcx> fn(
            mcx: Mcx<'mcx>,
            opfamilyoid: Oid,
            opclassoid: Oid,
            operators: &mut Vec<OpFamilyMember>,
            functions: &mut Vec<OpFamilyMember>,
        ) -> PgResult<()>,
    >,

    /// `amgettuple(scan, direction)` — next valid tuple (TID into
    /// `scan->xs_heaptid`).
    pub amgettuple: Option<
        for<'mcx> fn(
            mcx: Mcx<'mcx>,
            scan: &mut IndexScanDescData<'mcx>,
            direction: ScanDirection,
        ) -> PgResult<bool>,
    >,

    /// `amgetbitmap(scan, tbm)` — fetch all valid tuples into the bitmap.
    pub amgetbitmap: Option<
        for<'mcx> fn(
            mcx: Mcx<'mcx>,
            scan: &mut IndexScanDescData<'mcx>,
            tbm: &mut TIDBitmap,
        ) -> PgResult<i64>,
    >,

    /// `ammarkpos(scan)`.
    pub ammarkpos:
        Option<for<'mcx> fn(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()>>,

    /// `amrestrpos(scan)`.
    pub amrestrpos:
        Option<for<'mcx> fn(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()>>,

    /* ---- parallel index scan support ("can be NULL" in C) ---- */
    /// `amestimateparallelscan(indexRelation, nkeys, norderbys)` — DSM space
    /// the AM needs for its shared parallel-scan state.
    pub amestimateparallelscan:
        Option<fn(index_relation: &Relation<'_>, nkeys: i32, norderbys: i32) -> PgResult<usize>>,

    /// `aminitparallelscan(amtarget)` — initialize the AM-specific shared state
    /// in place. `amtarget` is the raw in-DSM address (a `usize`, mirroring C's
    /// `void *amtarget = OffsetToPointer(target, target->ps_offset_am)`) of the
    /// AM-specific tail within the parallel index-scan descriptor chunk. The AM
    /// casts it to its own `repr(C)` shared struct and placement-initializes it
    /// directly (the leader is the sole writer pre-launch).
    pub aminitparallelscan: Option<fn(amtarget: usize) -> PgResult<()>>,

    /// `amparallelrescan(scan)`.
    pub amparallelrescan:
        Option<for<'mcx> fn(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()>>,
}
