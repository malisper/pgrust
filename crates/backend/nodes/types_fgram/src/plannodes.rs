//! Plan-tree node types (`nodes/plannodes.h`).
//!
//! This module models the full PostgreSQL plan tree: the output `PlannedStmt`,
//! the abstract bases (`Plan`, `Scan`, `Join`) that every concrete plan node
//! embeds as its first field, and every concrete plan node that `copyfuncs`
//! (and, where applicable, `equalfuncs`) traverse - the scan family, the join
//! family, the upper-plan nodes (`Sort`, `Agg`, `WindowAgg`, ...), plus the
//! supporting plan nodes `PlanRowMark`, `NestLoopParam`, the partition-pruning
//! info/step nodes, and `PlanInvalItem`.
//!
//! Layout is faithful to the C backend: every struct is `#[repr(C)]` with field
//! order, names, types, and widths matching `nodes/plannodes.h` (cross-checked
//! against the c2rust-emitted struct defs of the generated copyfuncs). Node
//! trees are `palloc`/`MemoryContext`-owned, so there is no `Box`/`Drop` and no
//! `extern "C"`.
//!
//! Sub-pointers to nodes that are *in scope* (other plan nodes, `Var`,
//! `TableFunc`, the `TableSampleClause` that `copyfuncs` deep-copies for
//! `SampleScan`, ...) use the concrete `*mut <Struct>` forward reference. Only
//! pointees that `copyfuncs`/`equalfuncs` do not traverse - the static
//! `CustomScanMethods` callback table that `copyfuncs` shallow-copies - are kept
//! behind a raw pointer (`*const ...`).
//!
//! `Plan`, `Scan`, and `Join` have no `NodeTag` of their own (they are
//! `pg_node_attr(abstract)`); they are modelled only so concrete nodes can
//! embed them, and they do not appear in the coverage table.

use core::ffi::{c_char, c_int, c_long};

use ::pg_ffi_fgram::{
    uint32, uint64, AttrNumber, Bitmapset, List, Node, NodeTag, Oid, StrategyNumber,
};

/// `int64` as PostgreSQL spells it (`pg_node_attr`/c2rust: `i64`).
pub type int64 = i64;

use crate::primnodes::{AggSplit, Cardinality, CmdType, Cost, Index, JoinType, ParseLoc, Var};
use crate::{OpaqueNode, TableFunc};

// ---------------------------------------------------------------------------
// Supporting enums (kept as `c_int`/`c_uint` for exact ABI match), spelled the
// PostgreSQL way.
// ---------------------------------------------------------------------------

/// `ScanDirection` (`access/sdir.h`) - a *signed* enum, since
/// `BackwardScanDirection == -1`.
pub type ScanDirection = c_int;
pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;

/// `SubqueryScanStatus` - cached `trivial_subqueryscan` property.
pub type SubqueryScanStatus = core::ffi::c_uint;
pub const SUBQUERY_SCAN_UNKNOWN: SubqueryScanStatus = 0;
pub const SUBQUERY_SCAN_TRIVIAL: SubqueryScanStatus = 1;
pub const SUBQUERY_SCAN_NONTRIVIAL: SubqueryScanStatus = 2;

/// `AggStrategy` (`nodes/nodes.h`).
pub type AggStrategy = core::ffi::c_uint;
pub const AGG_PLAIN: AggStrategy = 0;
pub const AGG_SORTED: AggStrategy = 1;
pub const AGG_HASHED: AggStrategy = 2;
pub const AGG_MIXED: AggStrategy = 3;

/// `SetOpCmd` (`nodes/nodes.h`).
pub type SetOpCmd = core::ffi::c_uint;
pub const SETOPCMD_INTERSECT: SetOpCmd = 0;
pub const SETOPCMD_INTERSECT_ALL: SetOpCmd = 1;
pub const SETOPCMD_EXCEPT: SetOpCmd = 2;
pub const SETOPCMD_EXCEPT_ALL: SetOpCmd = 3;

/// `SetOpStrategy` (`nodes/nodes.h`).
pub type SetOpStrategy = core::ffi::c_uint;
pub const SETOP_SORTED: SetOpStrategy = 0;
pub const SETOP_HASHED: SetOpStrategy = 1;

/// `LimitOption` (`nodes/nodes.h`).
pub type LimitOption = core::ffi::c_uint;
pub const LIMIT_OPTION_COUNT: LimitOption = 0;
pub const LIMIT_OPTION_WITH_TIES: LimitOption = 1;

/// `OnConflictAction` (`nodes/nodes.h`) - ON CONFLICT clause action for
/// `ModifyTable`.
pub type OnConflictAction = core::ffi::c_uint;
pub const ONCONFLICT_NONE: OnConflictAction = 0;
pub const ONCONFLICT_NOTHING: OnConflictAction = 1;
pub const ONCONFLICT_UPDATE: OnConflictAction = 2;

/// `RowMarkType` (`nodes/plannodes.h`).
pub type RowMarkType = core::ffi::c_uint;
pub const ROW_MARK_EXCLUSIVE: RowMarkType = 0;
pub const ROW_MARK_NOKEYEXCLUSIVE: RowMarkType = 1;
pub const ROW_MARK_SHARE: RowMarkType = 2;
pub const ROW_MARK_KEYSHARE: RowMarkType = 3;
pub const ROW_MARK_REFERENCE: RowMarkType = 4;
pub const ROW_MARK_COPY: RowMarkType = 5;

/// `LockClauseStrength` (`nodes/lockoptions.h`).
pub type LockClauseStrength = core::ffi::c_uint;
pub const LCS_NONE: LockClauseStrength = 0;
pub const LCS_FORKEYSHARE: LockClauseStrength = 1;
pub const LCS_FORSHARE: LockClauseStrength = 2;
pub const LCS_FORNOKEYUPDATE: LockClauseStrength = 3;
pub const LCS_FORUPDATE: LockClauseStrength = 4;

/// `LockWaitPolicy` (`nodes/lockoptions.h`).
pub type LockWaitPolicy = core::ffi::c_uint;
pub const LockWaitBlock: LockWaitPolicy = 0;
pub const LockWaitSkip: LockWaitPolicy = 1;
pub const LockWaitError: LockWaitPolicy = 2;

/// `PartitionPruneCombineOp` (`nodes/plannodes.h`).
pub type PartitionPruneCombineOp = core::ffi::c_uint;
pub const PARTPRUNE_COMBINE_UNION: PartitionPruneCombineOp = 0;
pub const PARTPRUNE_COMBINE_INTERSECT: PartitionPruneCombineOp = 1;

/// `MonotonicFunction` (`nodes/plannodes.h`) - planner-side flags, modelled
/// here for completeness with the header.
pub type MonotonicFunction = core::ffi::c_uint;
pub const MONOTONICFUNC_NONE: MonotonicFunction = 0;
pub const MONOTONICFUNC_INCREASING: MonotonicFunction = 1 << 0;
pub const MONOTONICFUNC_DECREASING: MonotonicFunction = 1 << 1;
pub const MONOTONICFUNC_BOTH: MonotonicFunction =
    MONOTONICFUNC_INCREASING | MONOTONICFUNC_DECREASING;

// ---------------------------------------------------------------------------
// PlannedStmt - planner output.
// ---------------------------------------------------------------------------

/// `PlannedStmt` - the head of a finished plan; holds the "one time"
/// information the executor needs (also wraps utility statements).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlannedStmt {
    pub type_: NodeTag,
    pub commandType: CmdType,
    pub queryId: int64,
    pub planId: int64,
    pub hasReturning: bool,
    pub hasModifyingCTE: bool,
    pub canSetTag: bool,
    pub transientPlan: bool,
    pub dependsOnRole: bool,
    pub parallelModeNeeded: bool,
    pub jitFlags: c_int,
    pub planTree: *mut Plan,
    pub partPruneInfos: *mut List,
    pub rtable: *mut List,
    pub unprunableRelids: *mut Bitmapset,
    pub permInfos: *mut List,
    pub resultRelations: *mut List,
    pub appendRelations: *mut List,
    pub subplans: *mut List,
    pub rewindPlanIDs: *mut Bitmapset,
    pub rowMarks: *mut List,
    pub relationOids: *mut List,
    pub invalItems: *mut List,
    pub paramExecTypes: *mut List,
    pub utilityStmt: *mut Node,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

// ---------------------------------------------------------------------------
// Plan - common abstract base of every plan node.
// ---------------------------------------------------------------------------

/// `Plan` - common base of every plan node (abstract; embedded as the first
/// field of every concrete plan node so casts to `Plan*` work).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Plan {
    pub type_: NodeTag,
    pub disabled_nodes: c_int,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub plan_rows: Cardinality,
    pub plan_width: c_int,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub async_capable: bool,
    pub plan_node_id: c_int,
    pub targetlist: *mut List,
    pub qual: *mut List,
    pub lefttree: *mut Plan,
    pub righttree: *mut Plan,
    pub initPlan: *mut List,
    pub extParam: *mut Bitmapset,
    pub allParam: *mut Bitmapset,
}

// ---------------------------------------------------------------------------
// Result / ProjectSet / ModifyTable.
// ---------------------------------------------------------------------------

/// `Result` - evaluate a variable-free targetlist, or project the outer plan.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Result {
    pub plan: Plan,
    pub resconstantqual: *mut Node,
}

/// `ProjectSet` - apply a set-returning projection to outer-plan tuples.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProjectSet {
    pub plan: Plan,
}

/// `ModifyTable` - INSERT/UPDATE/DELETE/MERGE driver node (full layout).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyTable {
    pub plan: Plan,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub nominalRelation: Index,
    pub rootRelation: Index,
    pub partColsUpdated: bool,
    pub resultRelations: *mut List,
    pub updateColnosLists: *mut List,
    pub withCheckOptionLists: *mut List,
    pub returningOldAlias: *mut c_char,
    pub returningNewAlias: *mut c_char,
    pub returningLists: *mut List,
    pub fdwPrivLists: *mut List,
    pub fdwDirectModifyPlans: *mut Bitmapset,
    pub rowMarks: *mut List,
    pub epqParam: c_int,
    pub onConflictAction: OnConflictAction,
    pub arbiterIndexes: *mut List,
    pub onConflictSet: *mut List,
    pub onConflictCols: *mut List,
    pub onConflictWhere: *mut Node,
    pub exclRelRTI: Index,
    pub exclRelTlist: *mut List,
    pub mergeActionLists: *mut List,
    pub mergeJoinConditions: *mut List,
}

/// Back-compat alias for the previous partial header type. `ModifyTable` is
/// now modelled in full.
pub type ModifyTableHeader = ModifyTable;

// ---------------------------------------------------------------------------
// Append / MergeAppend / RecursiveUnion / BitmapAnd / BitmapOr.
// ---------------------------------------------------------------------------

/// `Append` - concatenation of sub-plans.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Append {
    pub plan: Plan,
    pub apprelids: *mut Bitmapset,
    pub appendplans: *mut List,
    pub nasyncplans: c_int,
    pub first_partial_plan: c_int,
    pub part_prune_index: c_int,
}

/// `MergeAppend` - order-preserving merge of pre-sorted sub-plans.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeAppend {
    pub plan: Plan,
    pub apprelids: *mut Bitmapset,
    pub mergeplans: *mut List,
    pub numCols: c_int,
    pub sortColIdx: *mut AttrNumber,
    pub sortOperators: *mut Oid,
    pub collations: *mut Oid,
    pub nullsFirst: *mut bool,
    pub part_prune_index: c_int,
}

/// `RecursiveUnion` - recursive union of a non-recursive and a recursive term.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RecursiveUnion {
    pub plan: Plan,
    pub wtParam: c_int,
    pub numCols: c_int,
    pub dupColIdx: *mut AttrNumber,
    pub dupOperators: *mut Oid,
    pub dupCollations: *mut Oid,
    pub numGroups: c_long,
}

/// `BitmapAnd` - intersection of tuple bitmaps from sub-plans.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapAnd {
    pub plan: Plan,
    pub bitmapplans: *mut List,
}

/// `BitmapOr` - union of tuple bitmaps from sub-plans.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapOr {
    pub plan: Plan,
    pub isshared: bool,
    pub bitmapplans: *mut List,
}

// ---------------------------------------------------------------------------
// Scan family.
// ---------------------------------------------------------------------------

/// `Scan` - abstract base of all relation scan plan nodes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Scan {
    pub plan: Plan,
    pub scanrelid: Index,
}

/// `SeqScan` - sequential scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SeqScan {
    pub scan: Scan,
}

/// `SampleScan` - TABLESAMPLE scan node. copyfuncs deep-copies the
/// `tablesample` pointee (`TableSampleClause`, modelled in `parsenodes`), so it
/// is a real pointer to that node type rather than an opaque-seam pointer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SampleScan {
    pub scan: Scan,
    /// `struct TableSampleClause *`; traversed by copy/equal.
    pub tablesample: *mut crate::parsenodes::TableSampleClause,
}

/// `IndexScan` - index scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub indexqual: *mut List,
    pub indexqualorig: *mut List,
    pub indexorderby: *mut List,
    pub indexorderbyorig: *mut List,
    pub indexorderbyops: *mut List,
    pub indexorderdir: ScanDirection,
}

/// `IndexOnlyScan` - index-only scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexOnlyScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub indexqual: *mut List,
    pub recheckqual: *mut List,
    pub indexorderby: *mut List,
    pub indextlist: *mut List,
    pub indexorderdir: ScanDirection,
}

/// `BitmapIndexScan` - bitmap index scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapIndexScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub isshared: bool,
    pub indexqual: *mut List,
    pub indexqualorig: *mut List,
}

/// `BitmapHeapScan` - bitmap heap scan node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BitmapHeapScan {
    pub scan: Scan,
    pub bitmapqualorig: *mut List,
}

/// `TidScan` - scan by CTID equality qual(s).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TidScan {
    pub scan: Scan,
    pub tidquals: *mut List,
}

/// `TidRangeScan` - scan by CTID range qual(s).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TidRangeScan {
    pub scan: Scan,
    pub tidrangequals: *mut List,
}

/// `SubqueryScan` - scan over the output of a sub-query in the range table.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubqueryScan {
    pub scan: Scan,
    pub subplan: *mut Plan,
    pub scanstatus: SubqueryScanStatus,
}

/// `FunctionScan` - scan over a set-returning function (FROM function()).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FunctionScan {
    pub scan: Scan,
    pub functions: *mut List,
    pub funcordinality: bool,
}

/// `ValuesScan` - scan over a VALUES list.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ValuesScan {
    pub scan: Scan,
    pub values_lists: *mut List,
}

/// `TableFuncScan` - scan over an XMLTABLE / JSON_TABLE table function.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableFuncScan {
    pub scan: Scan,
    pub tablefunc: *mut TableFunc,
}

/// `CteScan` - scan over a CTE's working result.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CteScan {
    pub scan: Scan,
    pub ctePlanId: c_int,
    pub cteParam: c_int,
}

/// `NamedTuplestoreScan` - scan over an ephemeral named relation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NamedTuplestoreScan {
    pub scan: Scan,
    pub enrname: *mut c_char,
}

/// `WorkTableScan` - scan over a recursive-union work table.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WorkTableScan {
    pub scan: Scan,
    pub wtParam: c_int,
}

/// `ForeignScan` - foreign-table scan (or foreign join / direct modify).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForeignScan {
    pub scan: Scan,
    pub operation: CmdType,
    pub resultRelation: Index,
    pub checkAsUser: Oid,
    pub fs_server: Oid,
    pub fdw_exprs: *mut List,
    pub fdw_private: *mut List,
    pub fdw_scan_tlist: *mut List,
    pub fdw_recheck_quals: *mut List,
    pub fs_relids: *mut Bitmapset,
    pub fs_base_relids: *mut Bitmapset,
    pub fsSystemCol: bool,
}

/// Opaque static table of custom-scan provider callbacks; PostgreSQL references
/// (does not copy) this table, so its contents are never traversed.
#[repr(C)]
pub struct CustomScanMethods {
    _opaque: [u8; 0],
}

/// `CustomScan` - extension-provided scan node. `methods` is a const pointer to
/// a static callback table (referenced, never copied).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CustomScan {
    pub scan: Scan,
    pub flags: uint32,
    pub custom_plans: *mut List,
    pub custom_exprs: *mut List,
    pub custom_private: *mut List,
    pub custom_scan_tlist: *mut List,
    pub custom_relids: *mut Bitmapset,
    pub methods: *const CustomScanMethods,
}

// ---------------------------------------------------------------------------
// Join family.
// ---------------------------------------------------------------------------

/// `Join` - abstract base of all join plan nodes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Join {
    pub plan: Plan,
    pub jointype: JoinType,
    pub inner_unique: bool,
    pub joinqual: *mut List,
}

/// `NestLoop` - nested-loop join node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NestLoop {
    pub join: Join,
    pub nestParams: *mut List,
}

/// `NestLoopParam` - a Param passed from the outer to the inner subplan of a
/// nested loop. (`no_equal`; copy-traversed only.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NestLoopParam {
    pub type_: NodeTag,
    pub paramno: c_int,
    pub paramval: *mut Var,
}

/// `MergeJoin` - merge join node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeJoin {
    pub join: Join,
    pub skip_mark_restore: bool,
    pub mergeclauses: *mut List,
    pub mergeFamilies: *mut Oid,
    pub mergeCollations: *mut Oid,
    pub mergeReversals: *mut bool,
    pub mergeNullsFirst: *mut bool,
}

/// `HashJoin` - hash join node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HashJoin {
    pub join: Join,
    pub hashclauses: *mut List,
    pub hashoperators: *mut List,
    pub hashcollations: *mut List,
    pub hashkeys: *mut List,
}

// ---------------------------------------------------------------------------
// Upper plan nodes.
// ---------------------------------------------------------------------------

/// `Material` - materialization node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Material {
    pub plan: Plan,
}

/// `Memoize` - cache results of the inner side of a parameterized join.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Memoize {
    pub plan: Plan,
    pub numKeys: c_int,
    pub hashOperators: *mut Oid,
    pub collations: *mut Oid,
    pub param_exprs: *mut List,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub est_entries: uint32,
    pub keyparamids: *mut Bitmapset,
}

/// `Sort` - sort node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Sort {
    pub plan: Plan,
    pub numCols: c_int,
    pub sortColIdx: *mut AttrNumber,
    pub sortOperators: *mut Oid,
    pub collations: *mut Oid,
    pub nullsFirst: *mut bool,
}

/// `IncrementalSort` - sort with a presorted prefix.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IncrementalSort {
    pub sort: Sort,
    pub nPresortedCols: c_int,
}

/// `Group` - grouping over presorted input (no aggregates).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Group {
    pub plan: Plan,
    pub numCols: c_int,
    pub grpColIdx: *mut AttrNumber,
    pub grpOperators: *mut Oid,
    pub grpCollations: *mut Oid,
}

/// `Agg` - plain or grouped aggregation node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Agg {
    pub plan: Plan,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    pub numCols: c_int,
    pub grpColIdx: *mut AttrNumber,
    pub grpOperators: *mut Oid,
    pub grpCollations: *mut Oid,
    pub numGroups: c_long,
    pub transitionSpace: uint64,
    pub aggParams: *mut Bitmapset,
    pub groupingSets: *mut List,
    pub chain: *mut List,
}

/// `WindowAgg` - window aggregate node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowAgg {
    pub plan: Plan,
    pub winname: *mut c_char,
    pub winref: Index,
    pub partNumCols: c_int,
    pub partColIdx: *mut AttrNumber,
    pub partOperators: *mut Oid,
    pub partCollations: *mut Oid,
    pub ordNumCols: c_int,
    pub ordColIdx: *mut AttrNumber,
    pub ordOperators: *mut Oid,
    pub ordCollations: *mut Oid,
    pub frameOptions: c_int,
    pub startOffset: *mut Node,
    pub endOffset: *mut Node,
    pub runCondition: *mut List,
    pub runConditionOrig: *mut List,
    pub startInRangeFunc: Oid,
    pub endInRangeFunc: Oid,
    pub inRangeColl: Oid,
    pub inRangeAsc: bool,
    pub inRangeNullsFirst: bool,
    pub topWindow: bool,
}

/// `Unique` - de-duplicate presorted input.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Unique {
    pub plan: Plan,
    pub numCols: c_int,
    pub uniqColIdx: *mut AttrNumber,
    pub uniqOperators: *mut Oid,
    pub uniqCollations: *mut Oid,
}

/// `Gather` - collect tuples from parallel workers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Gather {
    pub plan: Plan,
    pub num_workers: c_int,
    pub rescan_param: c_int,
    pub single_copy: bool,
    pub invisible: bool,
    pub initParam: *mut Bitmapset,
}

/// `GatherMerge` - order-preserving collection of tuples from workers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GatherMerge {
    pub plan: Plan,
    pub num_workers: c_int,
    pub rescan_param: c_int,
    pub numCols: c_int,
    pub sortColIdx: *mut AttrNumber,
    pub sortOperators: *mut Oid,
    pub collations: *mut Oid,
    pub nullsFirst: *mut bool,
    pub initParam: *mut Bitmapset,
}

/// `Hash` - hash build node (inner side of a hash join).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Hash {
    pub plan: Plan,
    pub hashkeys: *mut List,
    pub skewTable: Oid,
    pub skewColumn: AttrNumber,
    pub skewInherit: bool,
    pub rows_total: Cardinality,
}

/// `SetOp` - INTERSECT / EXCEPT node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetOp {
    pub plan: Plan,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    pub numCols: c_int,
    pub cmpColIdx: *mut AttrNumber,
    pub cmpOperators: *mut Oid,
    pub cmpCollations: *mut Oid,
    pub cmpNullsFirst: *mut bool,
    pub numGroups: c_long,
}

/// `LockRows` - apply FOR [KEY] UPDATE/SHARE row locking.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockRows {
    pub plan: Plan,
    pub rowMarks: *mut List,
    pub epqParam: c_int,
}

/// `Limit` - OFFSET / LIMIT node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Limit {
    pub plan: Plan,
    pub limitOffset: *mut Node,
    pub limitCount: *mut Node,
    pub limitOption: LimitOption,
    pub uniqNumCols: c_int,
    pub uniqColIdx: *mut AttrNumber,
    pub uniqOperators: *mut Oid,
    pub uniqCollations: *mut Oid,
}

// ---------------------------------------------------------------------------
// Supporting plan nodes: row marks, partition pruning, plan invalidation.
// ---------------------------------------------------------------------------

/// `PlanRowMark` - plan-time representation of a FOR [KEY] UPDATE/SHARE clause.
/// (`no_equal`; copy-traversed only.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlanRowMark {
    pub type_: NodeTag,
    pub rti: Index,
    pub prti: Index,
    pub rowmarkId: Index,
    pub markType: RowMarkType,
    pub allMarkTypes: c_int,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
    pub isParent: bool,
}

/// `PartitionPruneInfo` - top-level partition pruning info attached to an
/// Append/MergeAppend. (`no_equal`; copy-traversed only.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionPruneInfo {
    pub type_: NodeTag,
    pub relids: *mut Bitmapset,
    pub prune_infos: *mut List,
    pub other_subplans: *mut Bitmapset,
}

/// `PartitionedRelPruneInfo` - pruning rules for a single partitioned table
/// (one level of partitioning). (`no_equal`; copy-traversed only.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionedRelPruneInfo {
    pub type_: NodeTag,
    pub rtindex: Index,
    pub present_parts: *mut Bitmapset,
    pub nparts: c_int,
    pub subplan_map: *mut c_int,
    pub subpart_map: *mut c_int,
    pub leafpart_rti_map: *mut c_int,
    pub relid_map: *mut Oid,
    pub initial_pruning_steps: *mut List,
    pub exec_pruning_steps: *mut List,
    pub execparamids: *mut Bitmapset,
}

/// `PartitionPruneStep` - abstract base of partition-pruning steps (no concrete
/// nodes of this type exist; embedded as the first field of the concrete
/// step nodes).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionPruneStep {
    pub type_: NodeTag,
    pub step_id: c_int,
}

/// `PartitionPruneStepOp` - prune using a set of mutually-ANDed OpExpr clauses.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionPruneStepOp {
    pub step: PartitionPruneStep,
    pub opstrategy: StrategyNumber,
    pub exprs: *mut List,
    pub cmpfns: *mut List,
    pub nullkeys: *mut Bitmapset,
}

/// `PartitionPruneStepCombine` - combine the results of argument pruning steps.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionPruneStepCombine {
    pub step: PartitionPruneStep,
    pub combineOp: PartitionPruneCombineOp,
    pub source_stepids: *mut List,
}

/// `PlanInvalItem` - identifies a syscache entry a `PlannedStmt` depends on.
/// (`no_equal`; copy-traversed only.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlanInvalItem {
    pub type_: NodeTag,
    pub cacheId: c_int,
    pub hashValue: uint32,
}

// ---------------------------------------------------------------------------
// Compile-time layout invariants for representative structs.
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{offset_of, size_of};

    // Every concrete plan node begins with its base header at offset 0, so a
    // cast to `Plan*` (or `Scan*`/`Join*`) resolves the node tag.
    assert!(offset_of!(Scan, plan) == 0);
    assert!(offset_of!(Join, plan) == 0);
    assert!(offset_of!(SeqScan, scan) == 0);
    assert!(offset_of!(SampleScan, scan) == 0);
    assert!(offset_of!(IndexScan, scan) == 0);
    assert!(offset_of!(NestLoop, join) == 0);
    assert!(offset_of!(MergeJoin, join) == 0);
    assert!(offset_of!(HashJoin, join) == 0);
    assert!(offset_of!(IncrementalSort, sort) == 0);
    assert!(offset_of!(Result, plan) == 0);
    assert!(offset_of!(Agg, plan) == 0);
    assert!(offset_of!(PartitionPruneStepOp, step) == 0);

    // Plan header begins with the NodeTag, and Scan only adds `scanrelid`.
    assert!(offset_of!(Plan, type_) == 0);
    assert!(size_of::<SeqScan>() == size_of::<Scan>());

    // The two no-payload upper nodes are exactly a bare Plan header.
    assert!(size_of::<Material>() == size_of::<Plan>());
    assert!(size_of::<ProjectSet>() == size_of::<Plan>());

    // A pointer to the opaque seam / static methods table is one pointer wide.
    assert!(size_of::<*mut OpaqueNode>() == size_of::<*mut Node>());
    assert!(size_of::<*const CustomScanMethods>() == size_of::<*mut Node>());

    // SampleScan.tablesample is now a real `TableSampleClause*` (copyfuncs
    // traverses it). Pin SampleScan == Scan + one pointer so the promotion off
    // the OpaqueNode seam did not change its ABI width.
    assert!(size_of::<SampleScan>() == size_of::<Scan>() + size_of::<*mut Node>());
    assert!(size_of::<*mut crate::parsenodes::TableSampleClause>() == size_of::<*mut Node>());
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the plan-tree family.
///
/// The abstract base structs (`Plan`, `Scan`, `Join`, `PartitionPruneStep`)
/// have no `NodeTag` of their own and are never instantiated directly, so only
/// the concrete plan nodes appear in this tag-keyed table. `lib.rs` concatenates
/// this slice with the other families' coverage into the crate-wide
/// [`crate::NODE_TYPES_COVERED`] table.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    const fn m(name: &'static str, tag: NodeTag) -> NodeTypeStatus {
        NodeTypeStatus {
            name,
            tag,
            coverage: NodeTypeCoverage::Modelled,
        }
    }
    const TABLE: &[NodeTypeStatus] = &[
        m("PlannedStmt", T_PlannedStmt),
        m("Result", T_Result),
        m("ProjectSet", T_ProjectSet),
        m("ModifyTable", T_ModifyTable),
        m("Append", T_Append),
        m("MergeAppend", T_MergeAppend),
        m("RecursiveUnion", T_RecursiveUnion),
        m("BitmapAnd", T_BitmapAnd),
        m("BitmapOr", T_BitmapOr),
        m("SeqScan", T_SeqScan),
        m("SampleScan", T_SampleScan),
        m("IndexScan", T_IndexScan),
        m("IndexOnlyScan", T_IndexOnlyScan),
        m("BitmapIndexScan", T_BitmapIndexScan),
        m("BitmapHeapScan", T_BitmapHeapScan),
        m("TidScan", T_TidScan),
        m("TidRangeScan", T_TidRangeScan),
        m("SubqueryScan", T_SubqueryScan),
        m("FunctionScan", T_FunctionScan),
        m("ValuesScan", T_ValuesScan),
        m("TableFuncScan", T_TableFuncScan),
        m("CteScan", T_CteScan),
        m("NamedTuplestoreScan", T_NamedTuplestoreScan),
        m("WorkTableScan", T_WorkTableScan),
        m("ForeignScan", T_ForeignScan),
        m("CustomScan", T_CustomScan),
        m("NestLoop", T_NestLoop),
        m("NestLoopParam", T_NestLoopParam),
        m("MergeJoin", T_MergeJoin),
        m("HashJoin", T_HashJoin),
        m("Material", T_Material),
        m("Memoize", T_Memoize),
        m("Sort", T_Sort),
        m("IncrementalSort", T_IncrementalSort),
        m("Group", T_Group),
        m("Agg", T_Agg),
        m("WindowAgg", T_WindowAgg),
        m("Unique", T_Unique),
        m("Gather", T_Gather),
        m("GatherMerge", T_GatherMerge),
        m("Hash", T_Hash),
        m("SetOp", T_SetOp),
        m("LockRows", T_LockRows),
        m("Limit", T_Limit),
        m("PlanRowMark", T_PlanRowMark),
        m("PartitionPruneInfo", T_PartitionPruneInfo),
        m("PartitionedRelPruneInfo", T_PartitionedRelPruneInfo),
        m("PartitionPruneStepOp", T_PartitionPruneStepOp),
        m("PartitionPruneStepCombine", T_PartitionPruneStepCombine),
        m("PlanInvalItem", T_PlanInvalItem),
    ];
    TABLE
}
