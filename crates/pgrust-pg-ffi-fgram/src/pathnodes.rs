//! Planner working-state ABI (`nodes/pathnodes.h`).
//!
//! `#[repr(C)]` mirrors of the planner's per-query data structures, used by the
//! `backend/optimizer/util/*` ports.  Field order and types match PostgreSQL
//! 18.3 `src/include/nodes/pathnodes.h` exactly so these can be accessed via raw
//! pointers from the planner crates just as the C code does.
//!
//! Node types that the optimizer/util crates only ever pass through by pointer
//! (`Query`, `RangeTblEntry`, `Var`, `Param`, `WindowClause`, `OnConflictExpr`,
//! `FdwRoutine`, `HTAB`, partitioning descriptors, etc.) are represented as
//! opaque pointers to avoid pulling the whole parse/plan node graph in here; the
//! consuming crates cast as needed.

use core::ffi::{c_int, c_void};

use crate::bitmapset::Bitmapset;
use crate::executor::{Expr, Index};
use crate::fmgr::Node;
use crate::heap::ScanDirection;
use crate::list::List;
use crate::types::{AttrNumber, BlockNumber, NodeTag, Oid};

/* --------------------------------------------------------------------------
 * Scalar aliases used throughout the planner (pathnodes.h / nodes.h).
 * ------------------------------------------------------------------------ */

/// `Cost`/`Cardinality`/`Selectivity` (`nodes.h`) — canonical `f64` aliases live
/// in [`crate::types`]; re-exported here so the planner and the rest of pg-ffi
/// share one definition (avoids ambiguous glob re-exports at the crate root).
pub use crate::types::{Cardinality, Cost, Selectivity};

/// `Relids` — a set of relation identifiers, i.e. a `Bitmapset *`
/// (`pathnodes.h`).
pub type Relids = *mut Bitmapset;

/* --------------------------------------------------------------------------
 * Pass-through node pointers.  These reference parse/plan/primitive node types
 * that the optimizer/util crates do not dereference field-by-field; they are
 * kept opaque to avoid duplicating the entire node graph in this module.
 * ------------------------------------------------------------------------ */

/// `Query *` (parsenodes.h).
pub type QueryPtr = *mut c_void;
/// `RangeTblEntry *` (parsenodes.h).
pub type RangeTblEntryPtr = *mut c_void;
/// `Var *` (primnodes.h).
pub type VarPtr = *mut Expr;
/// `Param *` (primnodes.h).
pub type ParamPtr = *mut Expr;
/// `WindowClause *` (parsenodes.h).
pub type WindowClausePtr = *mut c_void;
/// `OnConflictExpr *` (primnodes.h).
pub type OnConflictExprPtr = *mut c_void;
/// `ParamListInfo` (params.h) — already aliased in funcapi; re-declared opaque.
pub type PathnodesParamListInfo = *mut c_void;
/// `MemoryContext` (memutils opaque handle).
pub type PathnodesMemoryContext = *mut c_void;
/// `struct HTAB *` (dynahash).
pub type HTABPtr = *mut c_void;
/// `struct FdwRoutine *` (fdwapi.h).
pub type FdwRoutinePtr = *mut c_void;
/// `PartitionDirectory` (partition descriptor cache).
pub type PartitionDirectory = *mut c_void;
/// `struct PartitionBoundInfoData *`.
pub type PartitionBoundInfoDataPtr = *mut c_void;
/// `struct derives_hash *` (EC derived-clause hash).
pub type DerivesHashPtr = *mut c_void;
/// `const struct CustomPathMethods *`.
pub type CustomPathMethodsPtr = *const c_void;

/* --------------------------------------------------------------------------
 * Enums shared with plannodes (nodes.h).  Mirrored as integer aliases with the
 * exact discriminant values, matching the repr used elsewhere in pg-ffi.
 * ------------------------------------------------------------------------ */

/// `JoinType` (nodes.h).
pub type JoinType = u32;
pub const JOIN_INNER: JoinType = 0;
pub const JOIN_LEFT: JoinType = 1;
pub const JOIN_FULL: JoinType = 2;
pub const JOIN_RIGHT: JoinType = 3;
pub const JOIN_SEMI: JoinType = 4;
pub const JOIN_ANTI: JoinType = 5;
pub const JOIN_RIGHT_SEMI: JoinType = 6;
pub const JOIN_RIGHT_ANTI: JoinType = 7;
pub const JOIN_UNIQUE_OUTER: JoinType = 8;
pub const JOIN_UNIQUE_INNER: JoinType = 9;

/// `CmdType` (nodes.h).
pub type CmdType = u32;
pub const CMD_UNKNOWN: CmdType = 0;
pub const CMD_SELECT: CmdType = 1;
pub const CMD_UPDATE: CmdType = 2;
pub const CMD_INSERT: CmdType = 3;
pub const CMD_DELETE: CmdType = 4;
pub const CMD_MERGE: CmdType = 5;
pub const CMD_UTILITY: CmdType = 6;
pub const CMD_NOTHING: CmdType = 7;

/// `AggStrategy` (nodes.h).
pub type AggStrategy = u32;
pub const AGG_PLAIN: AggStrategy = 0;
pub const AGG_SORTED: AggStrategy = 1;
pub const AGG_HASHED: AggStrategy = 2;
pub const AGG_MIXED: AggStrategy = 3;

/// `AggSplit` (nodes.h).
pub type AggSplit = u32;
pub const AGGSPLITOP_COMBINE: u32 = 0x01;
pub const AGGSPLITOP_SKIPFINAL: u32 = 0x02;
pub const AGGSPLITOP_SERIALIZE: u32 = 0x04;
pub const AGGSPLITOP_DESERIALIZE: u32 = 0x08;
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

/// `SetOpCmd` (nodes.h).
pub type SetOpCmd = u32;
pub const SETOPCMD_INTERSECT: SetOpCmd = 0;
pub const SETOPCMD_INTERSECT_ALL: SetOpCmd = 1;
pub const SETOPCMD_EXCEPT: SetOpCmd = 2;
pub const SETOPCMD_EXCEPT_ALL: SetOpCmd = 3;

/// `SetOpStrategy` (nodes.h).
pub type SetOpStrategy = u32;
pub const SETOP_SORTED: SetOpStrategy = 0;
pub const SETOP_HASHED: SetOpStrategy = 1;

/// `LimitOption` (nodes.h) — canonical `#[repr(u32)]` enum definition (with the
/// `LIMIT_OPTION_*` variants) lives in [`crate::execnodes`]; re-exported here so
/// the planner and executor share one type.
pub use crate::execnodes::{LimitOption, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES};

/// `RTEKind` (parsenodes.h).
pub type RTEKind = u32;
pub const RTE_RELATION: RTEKind = 0;
pub const RTE_SUBQUERY: RTEKind = 1;
pub const RTE_JOIN: RTEKind = 2;
pub const RTE_FUNCTION: RTEKind = 3;
pub const RTE_TABLEFUNC: RTEKind = 4;
pub const RTE_VALUES: RTEKind = 5;
pub const RTE_CTE: RTEKind = 6;
pub const RTE_NAMEDTUPLESTORE: RTEKind = 7;
pub const RTE_RESULT: RTEKind = 8;
pub const RTE_GROUP: RTEKind = 9;

/// `UpperRelationKind` (pathnodes.h); `UPPERREL_FINAL` must remain last.
pub type UpperRelationKind = u32;
pub const UPPERREL_SETOP: UpperRelationKind = 0;
pub const UPPERREL_PARTIAL_GROUP_AGG: UpperRelationKind = 1;
pub const UPPERREL_GROUP_AGG: UpperRelationKind = 2;
pub const UPPERREL_WINDOW: UpperRelationKind = 3;
pub const UPPERREL_PARTIAL_DISTINCT: UpperRelationKind = 4;
pub const UPPERREL_DISTINCT: UpperRelationKind = 5;
pub const UPPERREL_ORDERED: UpperRelationKind = 6;
pub const UPPERREL_FINAL: UpperRelationKind = 7;
/// Number of upper-rel kinds, i.e. `UPPERREL_FINAL + 1` array sizing.
pub const NUM_UPPERREL_KINDS: usize = (UPPERREL_FINAL as usize) + 1;

/// `RelOptKind` (pathnodes.h).
pub type RelOptKind = u32;
pub const RELOPT_BASEREL: RelOptKind = 0;
pub const RELOPT_JOINREL: RelOptKind = 1;
pub const RELOPT_OTHER_MEMBER_REL: RelOptKind = 2;
pub const RELOPT_OTHER_JOINREL: RelOptKind = 3;
pub const RELOPT_UPPER_REL: RelOptKind = 4;
pub const RELOPT_OTHER_UPPER_REL: RelOptKind = 5;

/// `VolatileFunctionStatus` (pathnodes.h).
pub type VolatileFunctionStatus = u32;
pub const VOLATILITY_UNKNOWN: VolatileFunctionStatus = 0;
pub const VOLATILITY_VOLATILE: VolatileFunctionStatus = 1;
pub const VOLATILITY_NOVOLATILE: VolatileFunctionStatus = 2;

/// `UniquePathMethod` (pathnodes.h).
pub type UniquePathMethod = u32;
pub const UNIQUE_PATH_NOOP: UniquePathMethod = 0;
pub const UNIQUE_PATH_HASH: UniquePathMethod = 1;
pub const UNIQUE_PATH_SORT: UniquePathMethod = 2;

/// `CompareType` is already provided by `access`; planner structs reference it
/// via this local alias to avoid re-exporting the symbol twice from the crate
/// root glob.
use crate::access::CompareType;

/// `INDEX_MAX_KEYS` as a `usize`, for sizing the FK per-column arrays.  The
/// canonical constant lives in [`crate::fmgr::INDEX_MAX_KEYS`].
const INDEX_MAX_KEYS: usize = crate::fmgr::INDEX_MAX_KEYS as usize;

/* --------------------------------------------------------------------------
 * QualCost (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `QualCost` — startup + per-tuple cost of a clause.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

/* --------------------------------------------------------------------------
 * PlannerGlobal (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `PlannerGlobal` — global state for a planner run.
#[repr(C)]
pub struct PlannerGlobal {
    pub type_: NodeTag,
    pub boundParams: PathnodesParamListInfo,
    pub subplans: *mut List,
    pub subpaths: *mut List,
    pub subroots: *mut List,
    pub rewindPlanIDs: *mut Bitmapset,
    pub finalrtable: *mut List,
    pub allRelids: *mut Bitmapset,
    pub prunableRelids: *mut Bitmapset,
    pub finalrteperminfos: *mut List,
    pub finalrowmarks: *mut List,
    pub resultRelations: *mut List,
    pub appendRelations: *mut List,
    pub partPruneInfos: *mut List,
    pub relationOids: *mut List,
    pub invalItems: *mut List,
    pub paramExecTypes: *mut List,
    pub lastPHId: Index,
    pub lastRowMarkId: Index,
    pub lastPlanNodeId: c_int,
    pub transientPlan: bool,
    pub dependsOnRole: bool,
    pub parallelModeOK: bool,
    pub parallelModeNeeded: bool,
    pub maxParallelHazard: core::ffi::c_char,
    pub partition_directory: PartitionDirectory,
}

/* --------------------------------------------------------------------------
 * PlannerInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `PlannerInfo` — per-query planning state ("root").
#[repr(C)]
pub struct PlannerInfo {
    pub type_: NodeTag,
    pub parse: QueryPtr,
    pub glob: *mut PlannerGlobal,
    pub query_level: Index,
    pub parent_root: *mut PlannerInfo,
    pub plan_params: *mut List,
    pub outer_params: *mut Bitmapset,
    pub simple_rel_array: *mut *mut RelOptInfo,
    pub simple_rel_array_size: c_int,
    pub simple_rte_array: *mut RangeTblEntryPtr,
    pub append_rel_array: *mut *mut AppendRelInfo,
    pub all_baserels: Relids,
    pub outer_join_rels: Relids,
    pub all_query_rels: Relids,
    pub join_rel_list: *mut List,
    pub join_rel_hash: HTABPtr,
    pub join_rel_level: *mut *mut List,
    pub join_cur_level: c_int,
    pub init_plans: *mut List,
    pub cte_plan_ids: *mut List,
    pub multiexpr_params: *mut List,
    pub join_domains: *mut List,
    pub eq_classes: *mut List,
    pub ec_merging_done: bool,
    pub canon_pathkeys: *mut List,
    pub left_join_clauses: *mut List,
    pub right_join_clauses: *mut List,
    pub full_join_clauses: *mut List,
    pub join_info_list: *mut List,
    pub last_rinfo_serial: c_int,
    pub all_result_relids: Relids,
    pub leaf_result_relids: Relids,
    pub append_rel_list: *mut List,
    pub row_identity_vars: *mut List,
    pub rowMarks: *mut List,
    pub placeholder_list: *mut List,
    pub placeholder_array: *mut *mut PlaceHolderInfo,
    pub placeholder_array_size: c_int,
    pub fkey_list: *mut List,
    pub query_pathkeys: *mut List,
    pub group_pathkeys: *mut List,
    pub num_groupby_pathkeys: c_int,
    pub window_pathkeys: *mut List,
    pub distinct_pathkeys: *mut List,
    pub sort_pathkeys: *mut List,
    pub setop_pathkeys: *mut List,
    pub part_schemes: *mut List,
    pub initial_rels: *mut List,
    pub upper_rels: [*mut List; NUM_UPPERREL_KINDS],
    pub upper_targets: [*mut PathTarget; NUM_UPPERREL_KINDS],
    pub processed_groupClause: *mut List,
    pub processed_distinctClause: *mut List,
    pub processed_tlist: *mut List,
    pub update_colnos: *mut List,
    pub grouping_map: *mut AttrNumber,
    pub minmax_aggs: *mut List,
    pub planner_cxt: PathnodesMemoryContext,
    pub total_table_pages: Cardinality,
    pub tuple_fraction: Selectivity,
    pub limit_tuples: Cardinality,
    pub qual_security_level: Index,
    pub hasJoinRTEs: bool,
    pub hasLateralRTEs: bool,
    pub hasHavingQual: bool,
    pub hasPseudoConstantQuals: bool,
    pub hasAlternativeSubPlans: bool,
    pub placeholdersFrozen: bool,
    pub hasRecursion: bool,
    pub group_rtindex: c_int,
    pub agginfos: *mut List,
    pub aggtransinfos: *mut List,
    pub numOrderedAggs: c_int,
    pub hasNonPartialAggs: bool,
    pub hasNonSerialAggs: bool,
    pub wt_param_id: c_int,
    pub non_recursive_path: *mut Path,
    pub curOuterRels: Relids,
    pub curOuterParams: *mut List,
    pub isAltSubplan: *mut bool,
    pub isUsedSubplan: *mut bool,
    pub join_search_private: *mut c_void,
    pub partColsUpdated: bool,
    pub partPruneInfos: *mut List,
}

/* --------------------------------------------------------------------------
 * PartitionScheme (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `PartitionSchemeData` — shared partitioning properties. Canonical definition
/// lives in [`crate::partition`] (its `partsupfunc` is the faithful
/// `*mut FmgrInfo` per `pathnodes.h`, not an opaque pointer); re-exported here.
pub use crate::partition::PartitionSchemeData;

/// `PartitionScheme` (pointer to `PartitionSchemeData`).
pub use crate::partition::PartitionScheme;

/* --------------------------------------------------------------------------
 * RelOptInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `RelOptInfo` — per-relation planning state.
#[repr(C)]
pub struct RelOptInfo {
    pub type_: NodeTag,
    pub reloptkind: RelOptKind,
    pub relids: Relids,
    pub rows: Cardinality,
    pub consider_startup: bool,
    pub consider_param_startup: bool,
    pub consider_parallel: bool,
    pub reltarget: *mut PathTarget,
    pub pathlist: *mut List,
    pub ppilist: *mut List,
    pub partial_pathlist: *mut List,
    pub cheapest_startup_path: *mut Path,
    pub cheapest_total_path: *mut Path,
    pub cheapest_unique_path: *mut Path,
    pub cheapest_parameterized_paths: *mut List,
    pub direct_lateral_relids: Relids,
    pub lateral_relids: Relids,
    pub relid: Index,
    pub reltablespace: Oid,
    pub rtekind: RTEKind,
    pub min_attr: AttrNumber,
    pub max_attr: AttrNumber,
    pub attr_needed: *mut Relids,
    pub attr_widths: *mut i32,
    pub notnullattnums: *mut Bitmapset,
    pub nulling_relids: Relids,
    pub lateral_vars: *mut List,
    pub lateral_referencers: Relids,
    pub indexlist: *mut List,
    pub statlist: *mut List,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub allvisfrac: f64,
    pub eclass_indexes: *mut Bitmapset,
    pub subroot: *mut PlannerInfo,
    pub subplan_params: *mut List,
    pub rel_parallel_workers: c_int,
    pub amflags: u32,
    pub serverid: Oid,
    pub userid: Oid,
    pub useridiscurrent: bool,
    pub fdwroutine: FdwRoutinePtr,
    pub fdw_private: *mut c_void,
    pub unique_for_rels: *mut List,
    pub non_unique_for_rels: *mut List,
    pub baserestrictinfo: *mut List,
    pub baserestrictcost: QualCost,
    pub baserestrict_min_security: Index,
    pub joininfo: *mut List,
    pub has_eclass_joins: bool,
    pub consider_partitionwise_join: bool,
    pub parent: *mut RelOptInfo,
    pub top_parent: *mut RelOptInfo,
    pub top_parent_relids: Relids,
    pub part_scheme: PartitionScheme,
    pub nparts: c_int,
    pub boundinfo: PartitionBoundInfoDataPtr,
    pub partbounds_merged: bool,
    pub partition_qual: *mut List,
    pub part_rels: *mut *mut RelOptInfo,
    pub live_parts: *mut Bitmapset,
    pub all_partrels: Relids,
    pub partexprs: *mut *mut List,
    pub nullable_partexprs: *mut *mut List,
}

/* --------------------------------------------------------------------------
 * IndexOptInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// AM cost-estimator function pointer used by `IndexOptInfo::amcostestimate`.
pub type AmCostEstimate = Option<
    unsafe extern "C" fn(
        *mut PlannerInfo,
        *mut IndexPath,
        f64,
        *mut Cost,
        *mut Cost,
        *mut Selectivity,
        *mut f64,
        *mut f64,
    ),
>;

/// `IndexOptInfo` — per-index planning state.
#[repr(C)]
pub struct IndexOptInfo {
    pub type_: NodeTag,
    pub indexoid: Oid,
    pub reltablespace: Oid,
    pub rel: *mut RelOptInfo,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub tree_height: c_int,
    pub ncolumns: c_int,
    pub nkeycolumns: c_int,
    pub indexkeys: *mut c_int,
    pub indexcollations: *mut Oid,
    pub opfamily: *mut Oid,
    pub opcintype: *mut Oid,
    pub sortopfamily: *mut Oid,
    pub reverse_sort: *mut bool,
    pub nulls_first: *mut bool,
    pub opclassoptions: *mut *mut crate::fmgr::bytea,
    pub canreturn: *mut bool,
    pub relam: Oid,
    pub indexprs: *mut List,
    pub indpred: *mut List,
    pub indextlist: *mut List,
    pub indrestrictinfo: *mut List,
    pub predOK: bool,
    pub unique: bool,
    pub nullsnotdistinct: bool,
    pub immediate: bool,
    pub hypothetical: bool,
    pub amcanorderbyop: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amhasgettuple: bool,
    pub amhasgetbitmap: bool,
    pub amcanparallel: bool,
    pub amcanmarkpos: bool,
    pub amcostestimate: AmCostEstimate,
}

/* --------------------------------------------------------------------------
 * ForeignKeyOptInfo / StatisticExtInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `ForeignKeyOptInfo` — per-foreign-key planning state.
#[repr(C)]
pub struct ForeignKeyOptInfo {
    pub type_: NodeTag,
    pub con_relid: Index,
    pub ref_relid: Index,
    pub nkeys: c_int,
    pub conkey: [AttrNumber; INDEX_MAX_KEYS],
    pub confkey: [AttrNumber; INDEX_MAX_KEYS],
    pub conpfeqop: [Oid; INDEX_MAX_KEYS],
    pub nmatched_ec: c_int,
    pub nconst_ec: c_int,
    pub nmatched_rcols: c_int,
    pub nmatched_ri: c_int,
    pub eclass: [*mut EquivalenceClass; INDEX_MAX_KEYS],
    pub fk_eclass_member: [*mut EquivalenceMember; INDEX_MAX_KEYS],
    pub rinfos: [*mut List; INDEX_MAX_KEYS],
}

/// `StatisticExtInfo` — extended statistics planning state.
#[repr(C)]
pub struct StatisticExtInfo {
    pub type_: NodeTag,
    pub statOid: Oid,
    pub inherit: bool,
    pub rel: *mut RelOptInfo,
    pub kind: core::ffi::c_char,
    pub keys: *mut Bitmapset,
    pub exprs: *mut List,
}

/* --------------------------------------------------------------------------
 * JoinDomain / EquivalenceClass / EquivalenceMember (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `JoinDomain` — scope of EC deductions.
#[repr(C)]
pub struct JoinDomain {
    pub type_: NodeTag,
    pub jd_relids: Relids,
}

/// `EquivalenceClass` — a set of transitively-equal expressions.
#[repr(C)]
pub struct EquivalenceClass {
    pub type_: NodeTag,
    pub ec_opfamilies: *mut List,
    pub ec_collation: Oid,
    pub ec_childmembers_size: c_int,
    pub ec_members: *mut List,
    pub ec_childmembers: *mut *mut List,
    pub ec_sources: *mut List,
    pub ec_derives_list: *mut List,
    pub ec_derives_hash: DerivesHashPtr,
    pub ec_relids: Relids,
    pub ec_has_const: bool,
    pub ec_has_volatile: bool,
    pub ec_broken: bool,
    pub ec_sortref: Index,
    pub ec_min_security: Index,
    pub ec_max_security: Index,
    pub ec_merged: *mut EquivalenceClass,
}

/// `EquivalenceMember` — one member of an `EquivalenceClass`.
#[repr(C)]
pub struct EquivalenceMember {
    pub type_: NodeTag,
    pub em_expr: *mut Expr,
    pub em_relids: Relids,
    pub em_is_const: bool,
    pub em_is_child: bool,
    pub em_datatype: Oid,
    pub em_jdomain: *mut JoinDomain,
    pub em_parent: *mut EquivalenceMember,
}

/// `EquivalenceMemberIterator` — anonymous struct; iterates parent + child EMs.
#[repr(C)]
pub struct EquivalenceMemberIterator {
    pub ec: *mut EquivalenceClass,
    pub current_relid: c_int,
    pub child_relids: Relids,
    pub current_cell: *mut crate::list::ListCell,
    pub current_list: *mut List,
}

/* --------------------------------------------------------------------------
 * PathKey / GroupByOrdering / PathTarget / ParamPathInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `PathKey` — one sort-ordering key.
#[repr(C)]
pub struct PathKey {
    pub type_: NodeTag,
    pub pk_eclass: *mut EquivalenceClass,
    pub pk_opfamily: Oid,
    pub pk_cmptype: CompareType,
    pub pk_nulls_first: bool,
}

/// `GroupByOrdering` — group-by clause order paired with pathkeys.
#[repr(C)]
pub struct GroupByOrdering {
    pub type_: NodeTag,
    pub pathkeys: *mut List,
    pub clauses: *mut List,
}

/// `PathTarget` — the output columns a Path computes.
#[repr(C)]
pub struct PathTarget {
    pub type_: NodeTag,
    pub exprs: *mut List,
    pub sortgrouprefs: *mut Index,
    pub cost: QualCost,
    pub width: c_int,
    pub has_volatile_expr: VolatileFunctionStatus,
}

/// `ParamPathInfo` — shared parameterization info for a set of paths.
#[repr(C)]
pub struct ParamPathInfo {
    pub type_: NodeTag,
    pub ppi_req_outer: Relids,
    pub ppi_rows: Cardinality,
    pub ppi_clauses: *mut List,
    pub ppi_serials: *mut Bitmapset,
}

/* --------------------------------------------------------------------------
 * Path and its subtypes (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `Path` — base path node; first member of every path subtype.
#[repr(C)]
pub struct Path {
    pub type_: NodeTag,
    pub pathtype: NodeTag,
    pub parent: *mut RelOptInfo,
    pub pathtarget: *mut PathTarget,
    pub param_info: *mut ParamPathInfo,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub parallel_workers: c_int,
    pub rows: Cardinality,
    pub disabled_nodes: c_int,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub pathkeys: *mut List,
}

/// `IndexPath` — an index scan / index-only scan.
#[repr(C)]
pub struct IndexPath {
    pub path: Path,
    pub indexinfo: *mut IndexOptInfo,
    pub indexclauses: *mut List,
    pub indexorderbys: *mut List,
    pub indexorderbycols: *mut List,
    pub indexscandir: ScanDirection,
    pub indextotalcost: Cost,
    pub indexselectivity: Selectivity,
}

/// `IndexClause` — one index-checkable restriction.
#[repr(C)]
pub struct IndexClause {
    pub type_: NodeTag,
    pub rinfo: *mut RestrictInfo,
    pub indexquals: *mut List,
    pub lossy: bool,
    pub indexcol: AttrNumber,
    pub indexcols: *mut List,
}

/// `BitmapHeapPath` — a bitmap heap scan.
#[repr(C)]
pub struct BitmapHeapPath {
    pub path: Path,
    pub bitmapqual: *mut Path,
}

/// `BitmapAndPath` — a BitmapAnd node.
#[repr(C)]
pub struct BitmapAndPath {
    pub path: Path,
    pub bitmapquals: *mut List,
    pub bitmapselectivity: Selectivity,
}

/// `BitmapOrPath` — a BitmapOr node.
#[repr(C)]
pub struct BitmapOrPath {
    pub path: Path,
    pub bitmapquals: *mut List,
    pub bitmapselectivity: Selectivity,
}

/// `TidPath` — a scan by TID.
#[repr(C)]
pub struct TidPath {
    pub path: Path,
    pub tidquals: *mut List,
}

/// `TidRangePath` — a scan by a contiguous TID range.
#[repr(C)]
pub struct TidRangePath {
    pub path: Path,
    pub tidrangequals: *mut List,
}

/// `SubqueryScanPath` — a scan of an unflattened subquery.
#[repr(C)]
pub struct SubqueryScanPath {
    pub path: Path,
    pub subpath: *mut Path,
}

/// `ForeignPath` — a foreign table/join/upper scan.
#[repr(C)]
pub struct ForeignPath {
    pub path: Path,
    pub fdw_outerpath: *mut Path,
    pub fdw_restrictinfo: *mut List,
    pub fdw_private: *mut List,
}

/// `CustomPath` — an extension-provided scan/join.
#[repr(C)]
pub struct CustomPath {
    pub path: Path,
    pub flags: u32,
    pub custom_paths: *mut List,
    pub custom_restrictinfo: *mut List,
    pub custom_private: *mut List,
    pub methods: CustomPathMethodsPtr,
}

/// `AppendPath` — an Append plan.
#[repr(C)]
pub struct AppendPath {
    pub path: Path,
    pub subpaths: *mut List,
    pub first_partial_path: c_int,
    pub limit_tuples: Cardinality,
}

/// `MergeAppendPath` — a MergeAppend plan.
#[repr(C)]
pub struct MergeAppendPath {
    pub path: Path,
    pub subpaths: *mut List,
    pub limit_tuples: Cardinality,
}

/// `GroupResultPath` — a degenerate GROUP BY Result.
#[repr(C)]
pub struct GroupResultPath {
    pub path: Path,
    pub quals: *mut List,
}

/// `MaterialPath` — a Material (caching) node.
#[repr(C)]
pub struct MaterialPath {
    pub path: Path,
    pub subpath: *mut Path,
}

/// `MemoizePath` — a Memoize cache node.
#[repr(C)]
pub struct MemoizePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub hash_operators: *mut List,
    pub param_exprs: *mut List,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub calls: Cardinality,
    pub est_entries: u32,
}

/// `UniquePath` — distinct-row elimination.
#[repr(C)]
pub struct UniquePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub umethod: UniquePathMethod,
    pub in_operators: *mut List,
    pub uniq_exprs: *mut List,
}

/// `GatherPath` — a Gather node.
#[repr(C)]
pub struct GatherPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub single_copy: bool,
    pub num_workers: c_int,
}

/// `GatherMergePath` — a Gather Merge node.
#[repr(C)]
pub struct GatherMergePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub num_workers: c_int,
}

/// `JoinPath` — fields shared by all join paths.
#[repr(C)]
pub struct JoinPath {
    pub path: Path,
    pub jointype: JoinType,
    pub inner_unique: bool,
    pub outerjoinpath: *mut Path,
    pub innerjoinpath: *mut Path,
    pub joinrestrictinfo: *mut List,
}

/// `NestPath` — a nested-loop join.
#[repr(C)]
pub struct NestPath {
    pub jpath: JoinPath,
}

/// `MergePath` — a merge join.
#[repr(C)]
pub struct MergePath {
    pub jpath: JoinPath,
    pub path_mergeclauses: *mut List,
    pub outersortkeys: *mut List,
    pub innersortkeys: *mut List,
    pub outer_presorted_keys: c_int,
    pub skip_mark_restore: bool,
    pub materialize_inner: bool,
}

/// `HashPath` — a hash join.
#[repr(C)]
pub struct HashPath {
    pub jpath: JoinPath,
    pub path_hashclauses: *mut List,
    pub num_batches: c_int,
    pub inner_rows_total: Cardinality,
}

/// `ProjectionPath` — a projection step.
#[repr(C)]
pub struct ProjectionPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub dummypp: bool,
}

/// `ProjectSetPath` — a set-returning-function projection.
#[repr(C)]
pub struct ProjectSetPath {
    pub path: Path,
    pub subpath: *mut Path,
}

/// `SortPath` — an explicit sort.
#[repr(C)]
pub struct SortPath {
    pub path: Path,
    pub subpath: *mut Path,
}

/// `IncrementalSortPath` — an incremental sort.
#[repr(C)]
pub struct IncrementalSortPath {
    pub spath: SortPath,
    pub nPresortedCols: c_int,
}

/// `GroupPath` — grouping of presorted input.
#[repr(C)]
pub struct GroupPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub groupClause: *mut List,
    pub qual: *mut List,
}

/// `UpperUniquePath` — adjacent-duplicate removal.
#[repr(C)]
pub struct UpperUniquePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub numkeys: c_int,
}

/// `AggPath` — generic aggregation.
#[repr(C)]
pub struct AggPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    pub numGroups: Cardinality,
    pub transitionSpace: u64,
    pub groupClause: *mut List,
    pub qual: *mut List,
}

/// `GroupingSetData` — one grouping set.
#[repr(C)]
pub struct GroupingSetData {
    pub type_: NodeTag,
    pub set: *mut List,
    pub numGroups: Cardinality,
}

/// `RollupData` — one rollup specification.
#[repr(C)]
pub struct RollupData {
    pub type_: NodeTag,
    pub groupClause: *mut List,
    pub gsets: *mut List,
    pub gsets_data: *mut List,
    pub numGroups: Cardinality,
    pub hashable: bool,
    pub is_hashed: bool,
}

/// `GroupingSetsPath` — a GROUPING SETS aggregation.
#[repr(C)]
pub struct GroupingSetsPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub aggstrategy: AggStrategy,
    pub rollups: *mut List,
    pub qual: *mut List,
    pub transitionSpace: u64,
}

/// `MinMaxAggPath` — index-optimized MIN/MAX.
#[repr(C)]
pub struct MinMaxAggPath {
    pub path: Path,
    pub mmaggregates: *mut List,
    pub quals: *mut List,
}

/// `WindowAggPath` — window-function computation.
#[repr(C)]
pub struct WindowAggPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub winclause: WindowClausePtr,
    pub qual: *mut List,
    pub runCondition: *mut List,
    pub topwindow: bool,
}

/// `SetOpPath` — an INTERSECT/EXCEPT set operation.
#[repr(C)]
pub struct SetOpPath {
    pub path: Path,
    pub leftpath: *mut Path,
    pub rightpath: *mut Path,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    pub groupList: *mut List,
    pub numGroups: Cardinality,
}

/// `RecursiveUnionPath` — a recursive UNION.
#[repr(C)]
pub struct RecursiveUnionPath {
    pub path: Path,
    pub leftpath: *mut Path,
    pub rightpath: *mut Path,
    pub distinctList: *mut List,
    pub wtParam: c_int,
    pub numGroups: Cardinality,
}

/// `LockRowsPath` — row-lock acquisition.
#[repr(C)]
pub struct LockRowsPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub rowMarks: *mut List,
    pub epqParam: c_int,
}

/// `ModifyTablePath` — INSERT/UPDATE/DELETE/MERGE.
#[repr(C)]
pub struct ModifyTablePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub nominalRelation: Index,
    pub rootRelation: Index,
    pub partColsUpdated: bool,
    pub resultRelations: *mut List,
    pub updateColnosLists: *mut List,
    pub withCheckOptionLists: *mut List,
    pub returningLists: *mut List,
    pub rowMarks: *mut List,
    pub onconflict: OnConflictExprPtr,
    pub epqParam: c_int,
    pub mergeActionLists: *mut List,
    pub mergeJoinConditions: *mut List,
}

/// `LimitPath` — LIMIT/OFFSET application.
#[repr(C)]
pub struct LimitPath {
    pub path: Path,
    pub subpath: *mut Path,
    pub limitOffset: *mut Node,
    pub limitCount: *mut Node,
    pub limitOption: LimitOption,
}

/* --------------------------------------------------------------------------
 * RestrictInfo / MergeScanSelCache (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `RestrictInfo` — a WHERE/JOIN clause with planner annotations.
#[repr(C)]
pub struct RestrictInfo {
    pub type_: NodeTag,
    pub clause: *mut Expr,
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub pseudoconstant: bool,
    pub has_clone: bool,
    pub is_clone: bool,
    pub leakproof: bool,
    pub has_volatile: VolatileFunctionStatus,
    pub security_level: Index,
    pub num_base_rels: c_int,
    pub clause_relids: Relids,
    pub required_relids: Relids,
    pub incompatible_relids: Relids,
    pub outer_relids: Relids,
    pub left_relids: Relids,
    pub right_relids: Relids,
    pub orclause: *mut Expr,
    pub rinfo_serial: c_int,
    pub parent_ec: *mut EquivalenceClass,
    pub eval_cost: QualCost,
    pub norm_selec: Selectivity,
    pub outer_selec: Selectivity,
    pub mergeopfamilies: *mut List,
    pub left_ec: *mut EquivalenceClass,
    pub right_ec: *mut EquivalenceClass,
    pub left_em: *mut EquivalenceMember,
    pub right_em: *mut EquivalenceMember,
    pub scansel_cache: *mut List,
    pub outer_is_left: bool,
    pub hashjoinoperator: Oid,
    pub left_bucketsize: Selectivity,
    pub right_bucketsize: Selectivity,
    pub left_mcvfreq: Selectivity,
    pub right_mcvfreq: Selectivity,
    pub left_hasheqoperator: Oid,
    pub right_hasheqoperator: Oid,
}

/// `MergeScanSelCache` — cached `mergejoinscansel()` results.
#[repr(C)]
pub struct MergeScanSelCache {
    pub opfamily: Oid,
    pub collation: Oid,
    pub cmptype: CompareType,
    pub nulls_first: bool,
    pub leftstartsel: Selectivity,
    pub leftendsel: Selectivity,
    pub rightstartsel: Selectivity,
    pub rightendsel: Selectivity,
}

/* --------------------------------------------------------------------------
 * PlaceHolderVar / SpecialJoinInfo / OuterJoinClauseInfo / AppendRelInfo
 * RowIdentityVarInfo / PlaceHolderInfo / MinMaxAggInfo (pathnodes.h)
 * ------------------------------------------------------------------------ */

/// `PlaceHolderVar` — a deferred-evaluation expression placeholder.
#[repr(C)]
pub struct PlaceHolderVar {
    pub xpr: Expr,
    pub phexpr: *mut Expr,
    pub phrels: Relids,
    pub phnullingrels: Relids,
    pub phid: Index,
    pub phlevelsup: Index,
}

/// `SpecialJoinInfo` — info about an outer/semi/anti join.
#[repr(C)]
pub struct SpecialJoinInfo {
    pub type_: NodeTag,
    pub min_lefthand: Relids,
    pub min_righthand: Relids,
    pub syn_lefthand: Relids,
    pub syn_righthand: Relids,
    pub jointype: JoinType,
    pub ojrelid: Index,
    pub commute_above_l: Relids,
    pub commute_above_r: Relids,
    pub commute_below_l: Relids,
    pub commute_below_r: Relids,
    pub lhs_strict: bool,
    pub semi_can_btree: bool,
    pub semi_can_hash: bool,
    pub semi_operators: *mut List,
    pub semi_rhs_exprs: *mut List,
}

/// `OuterJoinClauseInfo` — transient outer-join clause info.
#[repr(C)]
pub struct OuterJoinClauseInfo {
    pub type_: NodeTag,
    pub rinfo: *mut RestrictInfo,
    pub sjinfo: *mut SpecialJoinInfo,
}

/// `AppendRelInfo` — parent/child mapping for an appendrel member.
#[repr(C)]
pub struct AppendRelInfo {
    pub type_: NodeTag,
    pub parent_relid: Index,
    pub child_relid: Index,
    pub parent_reltype: Oid,
    pub child_reltype: Oid,
    pub translated_vars: *mut List,
    pub num_child_cols: c_int,
    pub parent_colnos: *mut AttrNumber,
    pub parent_reloid: Oid,
}

/// `RowIdentityVarInfo` — a row-identity resjunk column.
#[repr(C)]
pub struct RowIdentityVarInfo {
    pub type_: NodeTag,
    pub rowidvar: VarPtr,
    pub rowidwidth: i32,
    pub rowidname: *mut core::ffi::c_char,
    pub rowidrels: Relids,
}

/// `PlaceHolderInfo` — central info for a placeholder expression.
#[repr(C)]
pub struct PlaceHolderInfo {
    pub type_: NodeTag,
    pub phid: Index,
    pub ph_var: *mut PlaceHolderVar,
    pub ph_eval_at: Relids,
    pub ph_lateral: Relids,
    pub ph_needed: Relids,
    pub ph_width: i32,
}

/// `MinMaxAggInfo` — one index-optimizable MIN/MAX aggregate.
#[repr(C)]
pub struct MinMaxAggInfo {
    pub type_: NodeTag,
    pub aggfnoid: Oid,
    pub aggsortop: Oid,
    pub target: *mut Expr,
    pub subroot: *mut PlannerInfo,
    pub path: *mut Path,
    pub pathcost: Cost,
    pub param: ParamPtr,
}
