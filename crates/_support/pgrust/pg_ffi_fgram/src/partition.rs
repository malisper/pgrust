//! Partitioning ABI vocabulary.
//!
//! These `#[repr(C)]` structs, enums, and constants cross the boundary
//! between the rewritten partitioning crates
//! (`backend-partitioning-partbounds`, `-partdesc`, `-partprune`) and the rest
//! of the backend.  They mirror the C definitions in
//!   * `src/include/nodes/parsenodes.h`
//!     (PartitionStrategy, PartitionRangeDatumKind)
//!   * `src/include/partitioning/partbounds.h`  (PartitionBoundInfoData)
//!   * `src/include/utils/partcache.h`           (PartitionKeyData)
//!   * `src/include/partitioning/partdesc.h`     (PartitionDescData)
//!   * `src/backend/partitioning/partbounds.c`
//!     (PartitionHashBound, PartitionListValue, PartitionRangeBound)
//!   * `src/include/nodes/pathnodes.h`           (PartitionSchemeData)
//!   * `src/include/partitioning/partprune.h`    (PartitionPruneContext)
//!   * `src/include/executor/execPartition.h`
//!     (PartitionedRelPruningData, PartitionPruningData, PartitionPruneState)
//!   * `src/include/nodes/plannodes.h`
//!     (PartitionPruneInfo, PartitionedRelPruneInfo, PartitionPruneStep,
//!      PartitionPruneStepOp, PartitionPruneStepCombine, PartitionPruneCombineOp)
//!
//! Layout-critical fields keep their exact C order/width; flexible array
//! members are modeled as zero-length arrays (`[T; 0]`).  Planner/executor
//! sub-objects not yet modeled in this workspace (`RelOptInfo`, `PlannerInfo`,
//! `SpecialJoinInfo`, `Relation`, `PartitionBoundSpec`, ...) are held as
//! pointer-width opaque handles, ABI-identical to the C pointers they stand in
//! for.

use core::ffi::{c_char, c_void};

use crate::bitmapset::Bitmapset;
use crate::fmgr::FmgrInfo;
use crate::funcapi::ExprContext;
use crate::jit::{ExprState, PlanState};
use crate::list::List;
use crate::memory::MemoryContext;
use crate::{AttrNumber, Datum, NodeTag, Oid, StrategyNumber};

/* ---------------------------------------------------------------------------
 * parsenodes.h — PartitionStrategy / PartitionRangeDatumKind
 * ------------------------------------------------------------------------- */

/// `PartitionStrategy` — partitioning strategy (`char`-valued enum).
pub type PartitionStrategy = c_char;
/// `PARTITION_STRATEGY_LIST` (`'l'`).
pub const PARTITION_STRATEGY_LIST: PartitionStrategy = b'l' as PartitionStrategy;
/// `PARTITION_STRATEGY_RANGE` (`'r'`).
pub const PARTITION_STRATEGY_RANGE: PartitionStrategy = b'r' as PartitionStrategy;
/// `PARTITION_STRATEGY_HASH` (`'h'`).
pub const PARTITION_STRATEGY_HASH: PartitionStrategy = b'h' as PartitionStrategy;

/// `PartitionRangeDatumKind` — kind of a range bound datum.
pub type PartitionRangeDatumKind = i32;
/// `PARTITION_RANGE_DATUM_MINVALUE` — less than any other value.
pub const PARTITION_RANGE_DATUM_MINVALUE: PartitionRangeDatumKind = -1;
/// `PARTITION_RANGE_DATUM_VALUE` — a specific (bounded) value.
pub const PARTITION_RANGE_DATUM_VALUE: PartitionRangeDatumKind = 0;
/// `PARTITION_RANGE_DATUM_MAXVALUE` — greater than any other value.
pub const PARTITION_RANGE_DATUM_MAXVALUE: PartitionRangeDatumKind = 1;

/* ---------------------------------------------------------------------------
 * partbounds.h — PartitionBoundInfoData
 * ------------------------------------------------------------------------- */

/// `PartitionBoundInfoData` — encapsulates a set of partition bounds.
#[repr(C)]
pub struct PartitionBoundInfoData {
    /// hash, list or range?
    pub strategy: PartitionStrategy,
    /// length of the `datums[]` array.
    pub ndatums: i32,
    /// `Datum **datums`.
    pub datums: *mut *mut Datum,
    /// kind of each range bound datum; NULL for hash and list.
    pub kind: *mut *mut PartitionRangeDatumKind,
    /// partition indexes of partitions which may be interleaved (LIST only).
    pub interleaved_parts: *mut Bitmapset,
    /// length of the `indexes[]` array.
    pub nindexes: i32,
    /// partition indexes.
    pub indexes: *mut i32,
    /// index of the null-accepting partition; -1 if there isn't one.
    pub null_index: i32,
    /// index of the default partition; -1 if there isn't one.
    pub default_index: i32,
}

/// `PartitionBoundInfo` — pointer alias (`partdefs.h`).
pub type PartitionBoundInfo = *mut PartitionBoundInfoData;

/* private partbounds.c bound structs */

/// `PartitionHashBound` — one bound of a hash partition (`partbounds.c`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionHashBound {
    pub modulus: i32,
    pub remainder: i32,
    pub index: i32,
}

/// `PartitionListValue` — one value coming from some list partition
/// (`partbounds.c`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionListValue {
    pub index: i32,
    pub value: Datum,
}

/// `PartitionRangeBound` — one bound of a range partition (`partbounds.c`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionRangeBound {
    pub index: i32,
    /// range bound datums.
    pub datums: *mut Datum,
    /// the kind of each datum.
    pub kind: *mut PartitionRangeDatumKind,
    /// this is the lower (vs upper) bound.
    pub lower: bool,
}

/* ---------------------------------------------------------------------------
 * partcache.h — PartitionKeyData
 * ------------------------------------------------------------------------- */

/// `PartitionKeyData` — information about the partition key of a relation.
#[repr(C)]
pub struct PartitionKeyData {
    /// partitioning strategy.
    pub strategy: PartitionStrategy,
    /// number of columns in the partition key.
    pub partnatts: i16,
    /// attribute numbers of columns, or 0 if it's an expr.
    pub partattrs: *mut AttrNumber,
    /// list of expressions in the partitioning key (`List *`).
    pub partexprs: *mut List,
    /// OIDs of operator families.
    pub partopfamily: *mut Oid,
    /// OIDs of opclass declared input data types.
    pub partopcintype: *mut Oid,
    /// lookup info for support funcs.
    pub partsupfunc: *mut FmgrInfo,
    /// partitioning collation per attribute.
    pub partcollation: *mut Oid,
    /// type OID per attribute.
    pub parttypid: *mut Oid,
    /// typmod per attribute.
    pub parttypmod: *mut i32,
    /// typlen per attribute.
    pub parttyplen: *mut i16,
    /// typbyval per attribute.
    pub parttypbyval: *mut bool,
    /// typalign per attribute.
    pub parttypalign: *mut c_char,
    /// type collation per attribute.
    pub parttypcoll: *mut Oid,
}

/// `PartitionKey` — pointer alias (`partdefs.h`).
pub type PartitionKey = *mut PartitionKeyData;

/* ---------------------------------------------------------------------------
 * partdesc.h — PartitionDescData
 * ------------------------------------------------------------------------- */

/// `PartitionDescData` — information about partitions of a partitioned table.
#[repr(C)]
pub struct PartitionDescData {
    /// number of partitions.
    pub nparts: i32,
    /// are there any detached partitions?
    pub detached_exist: bool,
    /// array of `nparts` partition OIDs in order of their bounds.
    pub oids: *mut Oid,
    /// per-partition leaf flag.
    pub is_leaf: *mut bool,
    /// collection of partition bounds.
    pub boundinfo: PartitionBoundInfo,
    /// index into boundinfo's datum array for the last found partition, or -1.
    pub last_found_datum_index: i32,
    /// partition index of the last found partition, or -1.
    pub last_found_part_index: i32,
    /// run-length of consecutive last-found matches.
    pub last_found_count: i32,
}

/// `PartitionDesc` — pointer alias (`partdefs.h`).
pub type PartitionDesc = *mut PartitionDescData;

/* ---------------------------------------------------------------------------
 * pathnodes.h — PartitionSchemeData
 * ------------------------------------------------------------------------- */

/// `PartitionSchemeData` — shared partition-key info across like-partitioned
/// relations (`pathnodes.h`).
#[repr(C)]
pub struct PartitionSchemeData {
    /// partition strategy.
    pub strategy: c_char,
    /// number of partition attributes.
    pub partnatts: i16,
    /// OIDs of operator families.
    pub partopfamily: *mut Oid,
    /// OIDs of opclass declared input data types.
    pub partopcintype: *mut Oid,
    /// OIDs of partitioning collations.
    pub partcollation: *mut Oid,
    /// cached typlen per attribute.
    pub parttyplen: *mut i16,
    /// cached typbyval per attribute.
    pub parttypbyval: *mut bool,
    /// cached comparison support functions.
    pub partsupfunc: *mut FmgrInfo,
}

/// `PartitionScheme` — pointer alias (`pathnodes.h`).
pub type PartitionScheme = *mut PartitionSchemeData;

/* ---------------------------------------------------------------------------
 * partprune.h — PartitionPruneContext
 * ------------------------------------------------------------------------- */

/// `PartitionPruneContext` — runtime pruning context for a single
/// partitioned table.
#[repr(C)]
pub struct PartitionPruneContext {
    /// partition strategy (LIST/RANGE/HASH).
    pub strategy: c_char,
    /// number of columns in the partition key.
    pub partnatts: i32,
    /// number of partitions in this partitioned table.
    pub nparts: i32,
    /// partition boundary info.
    pub boundinfo: PartitionBoundInfo,
    /// collations of the partition key columns.
    pub partcollation: *mut Oid,
    /// comparison/hash support FmgrInfos for the partition keys.
    pub partsupfunc: *mut FmgrInfo,
    /// per-step, per-key comparison/hash FmgrInfos.
    pub stepcmpfuncs: *mut FmgrInfo,
    /// memory context holding subsidiary data.
    pub ppccontext: MemoryContext,
    /// parent plan node's PlanState during execution; NULL in planner.
    pub planstate: *mut PlanState,
    /// ExprContext for evaluating pruning expressions.
    pub exprcontext: *mut ExprContext,
    /// per-step, per-key ExprStates (`ExprState **`).
    pub exprstates: *mut *mut ExprState,
}

/* ---------------------------------------------------------------------------
 * execPartition.h — runtime pruning state
 * ------------------------------------------------------------------------- */

/// `PartitionedRelPruningData` — per-partitioned-table run-time pruning data.
#[repr(C)]
pub struct PartitionedRelPruningData {
    /// partitioned table Relation (opaque handle).
    pub partrel: *mut c_void,
    /// length of `subplan_map[]` and `subpart_map[]`.
    pub nparts: i32,
    /// subplan index by partition index, or -1.
    pub subplan_map: *mut i32,
    /// subpart index by partition index, or -1.
    pub subpart_map: *mut i32,
    /// RT index by partition index, or 0.
    pub leafpart_rti_map: *mut i32,
    /// partition indexes with subplans or subparts.
    pub present_parts: *mut Bitmapset,
    /// startup pruning steps (`List *`).
    pub initial_pruning_steps: *mut List,
    /// per-scan pruning steps (`List *`).
    pub exec_pruning_steps: *mut List,
    /// context for executing the initial steps.
    pub initial_context: PartitionPruneContext,
    /// context for executing the per-scan steps.
    pub exec_context: PartitionPruneContext,
}

/// `PartitionPruningData` — run-time pruning info for one partitioning
/// hierarchy.
#[repr(C)]
pub struct PartitionPruningData {
    /// number of array entries.
    pub num_partrelprunedata: i32,
    /// `partrelprunedata[FLEXIBLE_ARRAY_MEMBER]`.
    pub partrelprunedata: [PartitionedRelPruningData; 0],
}

/// `PartitionPruneState` — state object for run-time partition pruning.
#[repr(C)]
pub struct PartitionPruneState {
    /// standalone ExprContext to evaluate pruning step expressions.
    pub econtext: *mut ExprContext,
    /// PARAM_EXEC param IDs found within the partprunedata structs.
    pub execparamids: *mut Bitmapset,
    /// indexes of subplans that don't belong to any partprunedata.
    pub other_subplans: *mut Bitmapset,
    /// short-lived memory context for the pruning functions.
    pub prune_context: MemoryContext,
    /// true if pruning should be performed during executor startup.
    pub do_initial_prune: bool,
    /// true if pruning should be performed during executor run.
    pub do_exec_prune: bool,
    /// number of items in the `partprunedata` array.
    pub num_partprunedata: i32,
    /// `partprunedata[FLEXIBLE_ARRAY_MEMBER]`.
    pub partprunedata: [*mut PartitionPruningData; 0],
}

/* ---------------------------------------------------------------------------
 * plannodes.h — pruning plan nodes
 * ------------------------------------------------------------------------- */

/// `PartitionPruneInfo` — details to allow the executor to prune partitions.
#[repr(C)]
pub struct PartitionPruneInfo {
    pub r#type: NodeTag,
    /// RelOptInfo.relids of the parent plan node.
    pub relids: *mut Bitmapset,
    /// list of Lists of PartitionedRelPruneInfo nodes (`List *`).
    pub prune_infos: *mut List,
    /// indexes of subplans not accounted for by prune_infos.
    pub other_subplans: *mut Bitmapset,
}

/// `PartitionedRelPruneInfo` — pruning details for a single partitioned table.
#[repr(C)]
pub struct PartitionedRelPruneInfo {
    pub r#type: NodeTag,
    /// RT index of partition rel for this level.
    pub rtindex: u32,
    /// indexes of all partitions with subplans or subparts present.
    pub present_parts: *mut Bitmapset,
    /// length of the following arrays.
    pub nparts: i32,
    /// subplan index by partition index, or -1.
    pub subplan_map: *mut i32,
    /// subpart index by partition index, or -1.
    pub subpart_map: *mut i32,
    /// RT index by partition index, or 0.
    pub leafpart_rti_map: *mut i32,
    /// relation OID by partition index, or 0.
    pub relid_map: *mut Oid,
    /// executor-startup pruning steps (`List *`).
    pub initial_pruning_steps: *mut List,
    /// per-scan pruning steps (`List *`).
    pub exec_pruning_steps: *mut List,
    /// all PARAM_EXEC param IDs in exec_pruning_steps.
    pub execparamids: *mut Bitmapset,
}

/// `PartitionPruneStep` — abstract base for pruning steps.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionPruneStep {
    pub r#type: NodeTag,
    /// global identifier of the step within its pruning context.
    pub step_id: i32,
}

/// `PartitionPruneStepOp` — prune using a set of mutually ANDed OpExpr clauses.
#[repr(C)]
pub struct PartitionPruneStepOp {
    pub step: PartitionPruneStep,
    /// strategy of the operator matched to the last partition key.
    pub opstrategy: StrategyNumber,
    /// lookup-key expressions (`List *`).
    pub exprs: *mut List,
    /// OIDs of comparison functions (`List *`).
    pub cmpfns: *mut List,
    /// offsets of partition keys matched to an IS NULL clause.
    pub nullkeys: *mut Bitmapset,
}

/// `PartitionPruneCombineOp` — combine operator for a BoolExpr clause.
pub type PartitionPruneCombineOp = u32;
/// `PARTPRUNE_COMBINE_UNION`.
pub const PARTPRUNE_COMBINE_UNION: PartitionPruneCombineOp = 0;
/// `PARTPRUNE_COMBINE_INTERSECT`.
pub const PARTPRUNE_COMBINE_INTERSECT: PartitionPruneCombineOp = 1;

/// `PartitionPruneStepCombine` — prune using a BoolExpr clause.
#[repr(C)]
pub struct PartitionPruneStepCombine {
    pub step: PartitionPruneStep,
    pub combineOp: PartitionPruneCombineOp,
    /// step IDs to combine (`List *`).
    pub source_stepids: *mut List,
}
