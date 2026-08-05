// Plan-tree nodes; field names/order mirror vendor/plannodes.h
// (tests: plannedstmt_plan_result_field_order_match_c).
#![allow(non_snake_case)]

use core::mem::offset_of;

use types_core::{Cardinality, Cost, Index, Oid, ParseLoc};
use types_storage::storage::SyncCell;

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, NodeList, OidList, OptNodeList};
use crate::node_tree::{Node, NodeRep, NodeVariant};
use crate::nodes_enums::{CmdType, LimitOption, LockClauseStrength, LockWaitPolicy};
use crate::tags::NodeTag;

pub struct PlannedStmt<'mcx> {
    pub commandType: CmdType,
    // repr(transparent) over i64 (layout unchanged): pg_stat_statements'
    // ProcessUtility hook zeroes it through a shared ref, exactly where C
    // scribbles pstmt->queryId in place.
    pub queryId: SyncCell<i64>,
    pub planId: i64,
    pub hasReturning: bool,
    pub hasModifyingCTE: bool,
    pub canSetTag: bool,
    pub transientPlan: bool,
    pub dependsOnRole: bool,
    pub parallelModeNeeded: bool,
    pub jitFlags: i32,
    pub planTree: Option<Node<'mcx>>,
    pub partPruneInfos: NodeList<'mcx>,
    pub rtable: NodeList<'mcx>,
    pub unprunableRelids: Bitmapset<'mcx>,
    pub permInfos: NodeList<'mcx>,
    // C: integer list of RT indexes, or NIL.
    pub resultRelations: IntList<'mcx>,
    pub appendRelations: NodeList<'mcx>,
    // NULL cells are ExecSerializePlan's parallel-unsafe holes: they keep the
    // plan_id indexes of the safe subplans aligned for workers.
    pub subplans: OptNodeList<'mcx>,
    pub rewindPlanIDs: Bitmapset<'mcx>,
    pub rowMarks: NodeList<'mcx>,
    pub relationOids: OidList<'mcx>,
    pub invalItems: NodeList<'mcx>,
    pub paramExecTypes: OidList<'mcx>,
    pub utilityStmt: Option<Node<'mcx>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

impl Default for PlannedStmt<'_> {
    fn default() -> Self {
        PlannedStmt {
            commandType: CmdType::CMD_UNKNOWN,
            queryId: SyncCell::new(0),
            planId: 0,
            hasReturning: false,
            hasModifyingCTE: false,
            canSetTag: false,
            transientPlan: false,
            dependsOnRole: false,
            parallelModeNeeded: false,
            jitFlags: 0,
            planTree: None,
            partPruneInfos: NodeList::nil(),
            rtable: NodeList::nil(),
            unprunableRelids: Bitmapset::empty(),
            permInfos: NodeList::nil(),
            resultRelations: IntList::nil(),
            appendRelations: NodeList::nil(),
            subplans: OptNodeList::nil(),
            rewindPlanIDs: Bitmapset::empty(),
            rowMarks: NodeList::nil(),
            relationOids: OidList::nil(),
            invalItems: NodeList::nil(),
            paramExecTypes: OidList::nil(),
            utilityStmt: None,
            stmt_location: -1,
            stmt_len: 0,
        }
    }
}

/// Abstract base every concrete plan node embeds as its first field (C casts
/// node pointers to `Plan *`; here [`Node::as_plan`] is that cast). Never
/// instantiated as a node itself, so no `NodeVariant` impl.
pub struct Plan<'mcx> {
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub plan_rows: Cardinality,
    pub plan_width: i32,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub async_capable: bool,
    pub plan_node_id: i32,
    pub targetlist: NodeList<'mcx>,
    pub qual: NodeList<'mcx>,
    pub lefttree: Option<Node<'mcx>>,
    pub righttree: Option<Node<'mcx>>,
    pub initPlan: NodeList<'mcx>,
    pub extParam: Bitmapset<'mcx>,
    pub allParam: Bitmapset<'mcx>,
}

impl Default for Plan<'_> {
    fn default() -> Self {
        Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: NodeList::nil(),
            qual: NodeList::nil(),
            lefttree: None,
            righttree: None,
            initPlan: NodeList::nil(),
            extParam: Bitmapset::empty(),
            allParam: Bitmapset::empty(),
        }
    }
}

#[derive(Default)]
#[repr(C)]
pub struct Result<'mcx> {
    pub plan: Plan<'mcx>,
    pub resconstantqual: Option<Node<'mcx>>,
}

#[derive(Default)]
#[repr(C)]
pub struct ProjectSet<'mcx> {
    pub plan: Plan<'mcx>,
}

/// Abstract second-level base for all scan nodes (C never instantiates it).
#[derive(Default)]
#[repr(C)]
pub struct Scan<'mcx> {
    pub plan: Plan<'mcx>,
    pub scanrelid: Index,
}

#[derive(Default)]
#[repr(C)]
pub struct SeqScan<'mcx> {
    pub scan: Scan<'mcx>,
    /// pgrust-only (no C field): the EXACT set of this scan's columns
    /// (1-based attnos) the plan consumes — the pre-physical-tlist
    /// pathtarget's Vars plus the scan clauses' Vars, captured at
    /// create_seqscan_plan time. `use_physical_tlist` inflates the plan
    /// tlist to every column (free for heap's lazy deform); a columnar AM
    /// (pgrcolumnar) must not decode by that inflated tlist, and the executor
    /// cannot reconstruct the real consumed set after the swap.
    /// `None` = unknown (wholerow/system-column reference): consumers fall
    /// back to walking the plan tlist. `Some(empty)` is meaningful — the
    /// plan reads no columns (a qual-only count(*) scan).
    pub cb_scan_cols: Option<Bitmapset<'mcx>>,
}

#[derive(Default)]
#[repr(C)]
pub struct SampleScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub tablesample: Option<Node<'mcx>>,
}

/// tidquals has implicit OR semantics.
#[derive(Default)]
#[repr(C)]
pub struct TidScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub tidquals: NodeList<'mcx>,
}

/// tidrangequals has implicit AND semantics.
#[derive(Default)]
#[repr(C)]
pub struct TidRangeScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub tidrangequals: NodeList<'mcx>,
}

/// `indexorderdir` carries the C ScanDirection value (-1/0/1).
#[derive(Default)]
#[repr(C)]
pub struct IndexScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub indexid: u32,
    pub indexqual: NodeList<'mcx>,
    pub indexqualorig: NodeList<'mcx>,
    pub indexorderby: NodeList<'mcx>,
    pub indexorderbyorig: NodeList<'mcx>,
    pub indexorderbyops: OidList<'mcx>,
    pub indexorderdir: i32,
}

/// `indexorderdir` carries the C ScanDirection value (-1/0/1).
#[derive(Default)]
#[repr(C)]
pub struct IndexOnlyScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub indexid: u32,
    pub indexqual: NodeList<'mcx>,
    pub recheckqual: NodeList<'mcx>,
    pub indexorderby: NodeList<'mcx>,
    pub indextlist: NodeList<'mcx>,
    pub indexorderdir: i32,
}

/// targetlist/qual are unused and always NIL (as C).
#[derive(Default)]
#[repr(C)]
pub struct BitmapAnd<'mcx> {
    pub plan: Plan<'mcx>,
    pub bitmapplans: NodeList<'mcx>,
}

/// targetlist/qual are unused and always NIL (as C).
#[derive(Default)]
#[repr(C)]
pub struct BitmapOr<'mcx> {
    pub plan: Plan<'mcx>,
    pub isshared: bool,
    pub bitmapplans: NodeList<'mcx>,
}

/// targetlist/qual unused (NIL); indexqualorig is EXPLAIN-only, as C.
#[derive(Default)]
#[repr(C)]
pub struct BitmapIndexScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub indexid: u32,
    pub isshared: bool,
    pub indexqual: NodeList<'mcx>,
    pub indexqualorig: NodeList<'mcx>,
}

#[derive(Default)]
#[repr(C)]
pub struct BitmapHeapScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub bitmapqualorig: NodeList<'mcx>,
}

/// apprelids empty and part_prune_index -1 outside the appendrel/pruning lanes.
#[repr(C)]
pub struct Append<'mcx> {
    pub plan: Plan<'mcx>,
    pub apprelids: Bitmapset<'mcx>,
    pub appendplans: NodeList<'mcx>,
    pub nasyncplans: i32,
    pub first_partial_plan: i32,
    pub part_prune_index: i32,
}

impl Default for Append<'_> {
    fn default() -> Self {
        Append {
            plan: Plan::default(),
            apprelids: Bitmapset::empty(),
            appendplans: NodeList::nil(),
            nasyncplans: 0,
            first_partial_plan: 0,
            part_prune_index: -1,
        }
    }
}

#[repr(C)]
pub struct Gather<'mcx> {
    pub plan: Plan<'mcx>,
    pub num_workers: i32,
    pub rescan_param: i32,
    pub single_copy: bool,
    pub invisible: bool,
    pub initParam: Bitmapset<'mcx>,
}

impl Default for Gather<'_> {
    fn default() -> Self {
        Gather {
            plan: Plan::default(),
            num_workers: 0,
            rescan_param: -1,
            single_copy: false,
            invisible: false,
            initParam: Bitmapset::empty(),
        }
    }
}

/// Per-key arrays as in [`Sort`].
#[repr(C)]
pub struct GatherMerge<'mcx> {
    pub plan: Plan<'mcx>,
    pub num_workers: i32,
    pub rescan_param: i32,
    pub numCols: i32,
    pub sortColIdx: &'mcx [i16],
    pub sortOperators: &'mcx [Oid],
    pub collations: &'mcx [Oid],
    pub nullsFirst: &'mcx [bool],
    pub initParam: Bitmapset<'mcx>,
}

impl Default for GatherMerge<'_> {
    fn default() -> Self {
        GatherMerge {
            plan: Plan::default(),
            num_workers: 0,
            rescan_param: -1,
            numCols: 0,
            sortColIdx: &[],
            sortOperators: &[],
            collations: &[],
            nullsFirst: &[],
            initParam: Bitmapset::empty(),
        }
    }
}

/// Per-key arrays as in [`Sort`].
#[repr(C)]
pub struct MergeAppend<'mcx> {
    pub plan: Plan<'mcx>,
    pub apprelids: Bitmapset<'mcx>,
    pub mergeplans: NodeList<'mcx>,
    pub numCols: i32,
    pub sortColIdx: &'mcx [i16],
    pub sortOperators: &'mcx [Oid],
    pub collations: &'mcx [Oid],
    pub nullsFirst: &'mcx [bool],
    pub part_prune_index: i32,
}

impl Default for MergeAppend<'_> {
    fn default() -> Self {
        MergeAppend {
            plan: Plan::default(),
            apprelids: Bitmapset::empty(),
            mergeplans: NodeList::nil(),
            numCols: 0,
            sortColIdx: &[],
            sortOperators: &[],
            collations: &[],
            nullsFirst: &[],
            part_prune_index: -1,
        }
    }
}

#[repr(C)]
pub struct PartitionPruneInfo<'mcx> {
    pub relids: Bitmapset<'mcx>,
    pub prune_infos: NodeList<'mcx>,
    pub other_subplans: Bitmapset<'mcx>,
}

impl Default for PartitionPruneInfo<'_> {
    fn default() -> Self {
        PartitionPruneInfo {
            relids: Bitmapset::empty(),
            prune_infos: NodeList::nil(),
            other_subplans: Bitmapset::empty(),
        }
    }
}

/// Per-partition maps use C's empty entries: -1 (subplan/subpart maps) and
/// 0 (leafpart_rti/relid maps).
#[repr(C)]
pub struct PartitionedRelPruneInfo<'mcx> {
    pub rtindex: Index,
    pub present_parts: Bitmapset<'mcx>,
    pub nparts: i32,
    pub subplan_map: &'mcx [i32],
    pub subpart_map: &'mcx [i32],
    pub leafpart_rti_map: &'mcx [i32],
    pub relid_map: &'mcx [Oid],
    pub initial_pruning_steps: NodeList<'mcx>,
    pub exec_pruning_steps: NodeList<'mcx>,
    pub execparamids: Bitmapset<'mcx>,
}

impl Default for PartitionedRelPruneInfo<'_> {
    fn default() -> Self {
        PartitionedRelPruneInfo {
            rtindex: 0,
            present_parts: Bitmapset::empty(),
            nparts: 0,
            subplan_map: &[],
            subpart_map: &[],
            leafpart_rti_map: &[],
            relid_map: &[],
            initial_pruning_steps: NodeList::nil(),
            exec_pruning_steps: NodeList::nil(),
            execparamids: Bitmapset::empty(),
        }
    }
}

/// `opstrategy` 0 (InvalidStrategy) marks the IS NULL-only step and the list
/// `<>` special case, per C.
#[repr(C)]
pub struct PartitionPruneStepOp<'mcx> {
    pub step_id: i32,
    pub opstrategy: u16,
    pub exprs: NodeList<'mcx>,
    pub cmpfns: OidList<'mcx>,
    pub nullkeys: Bitmapset<'mcx>,
}

impl Default for PartitionPruneStepOp<'_> {
    fn default() -> Self {
        PartitionPruneStepOp {
            step_id: 0,
            opstrategy: 0,
            exprs: NodeList::nil(),
            cmpfns: OidList::nil(),
            nullkeys: Bitmapset::empty(),
        }
    }
}

pub const PARTPRUNE_COMBINE_UNION: u32 = 0;
pub const PARTPRUNE_COMBINE_INTERSECT: u32 = 1;

#[repr(C)]
pub struct PartitionPruneStepCombine<'mcx> {
    pub step_id: i32,
    pub combineOp: u32,
    pub source_stepids: IntList<'mcx>,
}

impl Default for PartitionPruneStepCombine<'_> {
    fn default() -> Self {
        PartitionPruneStepCombine {
            step_id: 0,
            combineOp: PARTPRUNE_COMBINE_UNION,
            source_stepids: IntList::nil(),
        }
    }
}

/// Flat setrefs copy of the planner AppendRelInfo (pathnodes.h) for
/// PlannedStmt.appendRelations. Divergence: translated_vars is not carried —
/// it holds planner NodeIds and no flat-tree consumer reads it.
#[derive(Default)]
pub struct AppendRelInfo<'mcx> {
    pub parent_relid: Index,
    pub child_relid: Index,
    pub parent_reltype: Oid,
    pub child_reltype: Oid,
    pub num_child_cols: i32,
    pub parent_colnos: &'mcx [i16],
    pub parent_reloid: Oid,
}

/// `scanstatus` carries the C SubqueryScanStatus value (0 = UNKNOWN).
#[derive(Default)]
#[repr(C)]
pub struct SubqueryScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub subplan: Option<Node<'mcx>>,
    pub scanstatus: u32,
}

/// `cmd`/`strategy` carry the C SetOpCmd/SetOpStrategy values (canonical
/// consts in types_pathnodes); per-key arrays as in [`Sort`].
#[derive(Default)]
#[repr(C)]
pub struct SetOp<'mcx> {
    pub plan: Plan<'mcx>,
    pub cmd: u32,
    pub strategy: u32,
    pub numCols: i32,
    pub cmpColIdx: &'mcx [i16],
    pub cmpOperators: &'mcx [Oid],
    pub cmpCollations: &'mcx [Oid],
    pub cmpNullsFirst: &'mcx [bool],
    pub numGroups: i64,
}

#[derive(Default)]
#[repr(C)]
pub struct FunctionScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub functions: NodeList<'mcx>,
    pub funcordinality: bool,
}

/// `tablefunc` is a `TableFunc` node.
#[derive(Default)]
#[repr(C)]
pub struct TableFuncScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub tablefunc: Option<Node<'mcx>>,
}

#[derive(Default)]
#[repr(C)]
pub struct CteScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub ctePlanId: i32,
    pub cteParam: i32,
}

#[derive(Default)]
#[repr(C)]
pub struct NamedTuplestoreScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub enrname: Option<&'mcx str>,
}

#[derive(Default)]
#[repr(C)]
pub struct WorkTableScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub wtParam: i32,
}

/// Outer child is the non-recursive term, inner child the recursive term;
/// dup fields are zero/empty for UNION ALL. Per-key arrays as in [`Sort`].
#[derive(Default)]
#[repr(C)]
pub struct RecursiveUnion<'mcx> {
    pub plan: Plan<'mcx>,
    pub wtParam: i32,
    pub numCols: i32,
    pub dupColIdx: &'mcx [i16],
    pub dupOperators: &'mcx [Oid],
    pub dupCollations: &'mcx [Oid],
    pub numGroups: i64,
}

#[derive(Default)]
#[repr(C)]
pub struct ValuesScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub values_lists: NodeList<'mcx>,
}

/// `operation` is `CMD_SELECT` for plain scans; direct-modify pushdown sets
/// UPDATE/DELETE (`resultRelation` then names the modify target's RT index).
#[repr(C)]
pub struct ForeignScan<'mcx> {
    pub scan: Scan<'mcx>,
    pub operation: CmdType,
    pub resultRelation: Index,
    pub checkAsUser: Oid,
    pub fs_server: Oid,
    pub fdw_exprs: NodeList<'mcx>,
    pub fdw_private: NodeList<'mcx>,
    pub fdw_scan_tlist: NodeList<'mcx>,
    pub fdw_recheck_quals: NodeList<'mcx>,
    pub fs_relids: Bitmapset<'mcx>,
    pub fs_base_relids: Bitmapset<'mcx>,
    pub fsSystemCol: bool,
}

impl Default for ForeignScan<'_> {
    fn default() -> Self {
        ForeignScan {
            scan: Scan::default(),
            operation: CmdType::CMD_SELECT,
            resultRelation: 0,
            checkAsUser: Oid::default(),
            fs_server: Oid::default(),
            fdw_exprs: NodeList::default(),
            fdw_private: NodeList::default(),
            fdw_scan_tlist: NodeList::default(),
            fdw_recheck_quals: NodeList::default(),
            fs_relids: Bitmapset::default(),
            fs_base_relids: Bitmapset::default(),
            fsSystemCol: false,
        }
    }
}

/// Per-key arrays are C's `pg_node_attr(array_size(numCols))` parallel arrays.
#[derive(Default)]
#[repr(C)]
pub struct Sort<'mcx> {
    pub plan: Plan<'mcx>,
    pub numCols: i32,
    pub sortColIdx: &'mcx [i16],
    pub sortOperators: &'mcx [Oid],
    pub collations: &'mcx [Oid],
    pub nullsFirst: &'mcx [bool],
}

/// Per-key arrays as in [`Sort`]; the first `nPresortedCols` keys arrive
/// already ordered from the child.
#[derive(Default)]
#[repr(C)]
pub struct IncrementalSort<'mcx> {
    pub sort: Sort<'mcx>,
    pub nPresortedCols: i32,
}

/// Per-key arrays as in [`Sort`].
#[derive(Default)]
#[repr(C)]
pub struct Group<'mcx> {
    pub plan: Plan<'mcx>,
    pub numCols: i32,
    pub grpColIdx: &'mcx [i16],
    pub grpOperators: &'mcx [Oid],
    pub grpCollations: &'mcx [Oid],
}

/// Per-key arrays as in [`Sort`].
#[derive(Default)]
#[repr(C)]
pub struct Unique<'mcx> {
    pub plan: Plan<'mcx>,
    pub numCols: i32,
    pub uniqColIdx: &'mcx [i16],
    pub uniqOperators: &'mcx [Oid],
    pub uniqCollations: &'mcx [Oid],
}

/// Per-key arrays as in [`Sort`]; frameOptions bits as in rawnodes FRAMEOPTION_*.
#[repr(C)]
pub struct WindowAgg<'mcx> {
    pub plan: Plan<'mcx>,
    pub winname: Option<&'mcx str>,
    pub winref: Index,
    pub partNumCols: i32,
    pub partColIdx: &'mcx [i16],
    pub partOperators: &'mcx [Oid],
    pub partCollations: &'mcx [Oid],
    pub ordNumCols: i32,
    pub ordColIdx: &'mcx [i16],
    pub ordOperators: &'mcx [Oid],
    pub ordCollations: &'mcx [Oid],
    pub frameOptions: i32,
    pub startOffset: Option<Node<'mcx>>,
    pub endOffset: Option<Node<'mcx>>,
    pub runCondition: NodeList<'mcx>,
    pub runConditionOrig: NodeList<'mcx>,
    pub startInRangeFunc: Oid,
    pub endInRangeFunc: Oid,
    pub inRangeColl: Oid,
    pub inRangeAsc: bool,
    pub inRangeNullsFirst: bool,
    pub topWindow: bool,
}

impl Default for WindowAgg<'_> {
    fn default() -> Self {
        WindowAgg {
            plan: Plan::default(),
            winname: None,
            winref: 0,
            partNumCols: 0,
            partColIdx: &[],
            partOperators: &[],
            partCollations: &[],
            ordNumCols: 0,
            ordColIdx: &[],
            ordOperators: &[],
            ordCollations: &[],
            frameOptions: crate::rawnodes::FRAMEOPTION_DEFAULTS,
            startOffset: None,
            endOffset: None,
            runCondition: NodeList::nil(),
            runConditionOrig: NodeList::nil(),
            startInRangeFunc: 0,
            endInRangeFunc: 0,
            inRangeColl: 0,
            inRangeAsc: true,
            inRangeNullsFirst: false,
            topWindow: false,
        }
    }
}

/// `aggstrategy`/`aggsplit` carry the C AggStrategy/AggSplit values
/// (canonical consts in types_pathnodes); per-key arrays as in [`Sort`].
#[repr(C)]
pub struct Agg<'mcx> {
    pub plan: Plan<'mcx>,
    pub aggstrategy: u32,
    pub aggsplit: u32,
    pub numCols: i32,
    pub grpColIdx: &'mcx [i16],
    pub grpOperators: &'mcx [Oid],
    pub grpCollations: &'mcx [Oid],
    pub numGroups: i64,
    pub transitionSpace: u64,
    pub aggParams: Bitmapset<'mcx>,
    pub groupingSets: NodeList<'mcx>,
    pub chain: NodeList<'mcx>,
}

impl Default for Agg<'_> {
    fn default() -> Self {
        Agg {
            plan: Plan::default(),
            aggstrategy: 0,
            aggsplit: 0,
            numCols: 0,
            grpColIdx: &[],
            grpOperators: &[],
            grpCollations: &[],
            numGroups: 0,
            transitionSpace: 0,
            aggParams: Bitmapset::empty(),
            groupingSets: NodeList::nil(),
            chain: NodeList::nil(),
        }
    }
}

/// `onConflictAction` carries the C OnConflictAction value (0 = NONE).
#[repr(C)]
pub struct ModifyTable<'mcx> {
    pub plan: Plan<'mcx>,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub nominalRelation: Index,
    pub rootRelation: Index,
    pub partColsUpdated: bool,
    pub resultRelations: IntList<'mcx>,
    pub updateColnosLists: NodeList<'mcx>,
    pub withCheckOptionLists: NodeList<'mcx>,
    pub returningOldAlias: Option<&'mcx str>,
    pub returningNewAlias: Option<&'mcx str>,
    pub returningLists: NodeList<'mcx>,
    pub fdwPrivLists: NodeList<'mcx>,
    pub fdwDirectModifyPlans: Bitmapset<'mcx>,
    pub rowMarks: NodeList<'mcx>,
    pub epqParam: i32,
    pub onConflictAction: u32,
    pub arbiterIndexes: OidList<'mcx>,
    pub onConflictSet: NodeList<'mcx>,
    pub onConflictCols: IntList<'mcx>,
    pub onConflictWhere: Option<Node<'mcx>>,
    pub exclRelRTI: Index,
    pub exclRelTlist: NodeList<'mcx>,
    pub mergeActionLists: NodeList<'mcx>,
    pub mergeJoinConditions: NodeList<'mcx>,
}

impl Default for ModifyTable<'_> {
    fn default() -> Self {
        ModifyTable {
            plan: Plan::default(),
            operation: CmdType::CMD_UNKNOWN,
            canSetTag: false,
            nominalRelation: 0,
            rootRelation: 0,
            partColsUpdated: false,
            resultRelations: IntList::nil(),
            updateColnosLists: NodeList::nil(),
            withCheckOptionLists: NodeList::nil(),
            returningOldAlias: None,
            returningNewAlias: None,
            returningLists: NodeList::nil(),
            fdwPrivLists: NodeList::nil(),
            fdwDirectModifyPlans: Bitmapset::empty(),
            rowMarks: NodeList::nil(),
            epqParam: 0,
            onConflictAction: 0,
            arbiterIndexes: OidList::nil(),
            onConflictSet: NodeList::nil(),
            onConflictCols: IntList::nil(),
            onConflictWhere: None,
            exclRelRTI: 0,
            exclRelTlist: NodeList::nil(),
            mergeActionLists: NodeList::nil(),
            mergeJoinConditions: NodeList::nil(),
        }
    }
}

/// Abstract second-level base for join nodes (C never instantiates it).
/// `jointype` carries the C JoinType value ([`crate::jointype::JoinType`]).
#[derive(Default)]
#[repr(C)]
pub struct Join<'mcx> {
    pub plan: Plan<'mcx>,
    pub jointype: crate::jointype::JoinType,
    pub inner_unique: bool,
    pub joinqual: NodeList<'mcx>,
}

#[derive(Default)]
#[repr(C)]
pub struct NestLoop<'mcx> {
    pub join: Join<'mcx>,
    pub nestParams: NodeList<'mcx>,
}

/// nestParams cell: outer-Var source for one PARAM_EXEC slot the nestloop
/// sets before each inner rescan.
#[repr(C)]
pub struct NestLoopParam<'mcx> {
    pub paramno: i32,
    pub paramval: Node<'mcx>,
}

/// Per-clause arrays parallel `mergeclauses` (C's `array_size(mergeclauses)`).
#[derive(Default)]
#[repr(C)]
pub struct MergeJoin<'mcx> {
    pub join: Join<'mcx>,
    pub skip_mark_restore: bool,
    pub mergeclauses: NodeList<'mcx>,
    pub mergeFamilies: &'mcx [Oid],
    pub mergeCollations: &'mcx [Oid],
    pub mergeReversals: &'mcx [bool],
    pub mergeNullsFirst: &'mcx [bool],
}

/// `hashkeys` are the OUTER-side hash expressions (Hash node carries the
/// inner-side keys); operator/collation arrays parallel `hashclauses`.
#[derive(Default)]
#[repr(C)]
pub struct HashJoin<'mcx> {
    pub join: Join<'mcx>,
    pub hashclauses: NodeList<'mcx>,
    pub hashoperators: OidList<'mcx>,
    pub hashcollations: OidList<'mcx>,
    pub hashkeys: NodeList<'mcx>,
}

/// `hashkeys` are the inner-side hash expressions. skew fields carry the C
/// values (InvalidOid/0/false when no single simple outer Var key).
#[derive(Default)]
#[repr(C)]
pub struct Hash<'mcx> {
    pub plan: Plan<'mcx>,
    pub hashkeys: NodeList<'mcx>,
    pub skewTable: Oid,
    pub skewColumn: i16,
    pub skewInherit: bool,
    pub rows_total: Cardinality,
}

#[derive(Default)]
#[repr(C)]
pub struct Material<'mcx> {
    pub plan: Plan<'mcx>,
}

/// Per-key arrays as in [`Sort`].
#[repr(C)]
pub struct Memoize<'mcx> {
    pub plan: Plan<'mcx>,
    pub numKeys: i32,
    pub hashOperators: &'mcx [Oid],
    pub collations: &'mcx [Oid],
    pub param_exprs: NodeList<'mcx>,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub est_entries: u32,
    pub keyparamids: Bitmapset<'mcx>,
}

impl Default for Memoize<'_> {
    fn default() -> Self {
        Memoize {
            plan: Plan::default(),
            numKeys: 0,
            hashOperators: &[],
            collations: &[],
            param_exprs: NodeList::nil(),
            singlerow: false,
            binary_mode: false,
            est_entries: 0,
            keyparamids: Bitmapset::empty(),
        }
    }
}

#[derive(Default)]
#[repr(C)]
pub struct LockRows<'mcx> {
    pub plan: Plan<'mcx>,
    pub rowMarks: NodeList<'mcx>,
    pub epqParam: i32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RowMarkType {
    #[default]
    ROW_MARK_EXCLUSIVE = 0,
    ROW_MARK_NOKEYEXCLUSIVE = 1,
    ROW_MARK_SHARE = 2,
    ROW_MARK_KEYSHARE = 3,
    ROW_MARK_REFERENCE = 4,
    ROW_MARK_COPY = 5,
}

impl RowMarkType {
    /// C `RowMarkRequiresRowShareLock(marktype)`.
    #[inline]
    pub fn requires_row_share_lock(self) -> bool {
        (self as u32) <= (RowMarkType::ROW_MARK_KEYSHARE as u32)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PlanRowMark {
    pub rti: Index,
    pub prti: Index,
    pub rowmarkId: Index,
    pub markType: RowMarkType,
    pub allMarkTypes: i32,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
    pub isParent: bool,
}

mcx::forget_safe_nodrop!(PlanRowMark);

#[derive(Default)]
#[repr(C)]
pub struct Limit<'mcx> {
    pub plan: Plan<'mcx>,
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub uniqNumCols: i32,
    pub uniqColIdx: &'mcx [i16],
    pub uniqOperators: &'mcx [Oid],
    pub uniqCollations: &'mcx [Oid],
}

/// A syscache-keyed plan dependency (plannodes.h). Not a plan-tree node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanInvalItem {
    pub cacheId: i32,
    pub hashValue: u32,
}

/// # Safety: implementors must be `repr(C)` with a [`Plan`] first field, so a
/// `NodeRep<Self>` reads as a `NodeRep<Plan>` prefix, and their tag must be
/// listed in [`is_plan_tag`].
pub unsafe trait PlanVariant<'mcx>: NodeVariant<'mcx> {}

// SAFETY (each): tag/type pairing mirrors plannodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for PlannedStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_PlannedStmt;
}
unsafe impl NodeVariant<'_> for PlanInvalItem {
    const TAG: NodeTag = NodeTag::T_PlanInvalItem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Result<'mcx> {
    const TAG: NodeTag = NodeTag::T_Result;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ProjectSet<'mcx> {
    const TAG: NodeTag = NodeTag::T_ProjectSet;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SeqScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SeqScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SampleScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SampleScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TidScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_TidScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TidRangeScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_TidRangeScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IndexScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_IndexScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IndexOnlyScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_IndexOnlyScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BitmapAnd<'mcx> {
    const TAG: NodeTag = NodeTag::T_BitmapAnd;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BitmapOr<'mcx> {
    const TAG: NodeTag = NodeTag::T_BitmapOr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BitmapIndexScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_BitmapIndexScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BitmapHeapScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_BitmapHeapScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Append<'mcx> {
    const TAG: NodeTag = NodeTag::T_Append;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MergeAppend<'mcx> {
    const TAG: NodeTag = NodeTag::T_MergeAppend;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Gather<'mcx> {
    const TAG: NodeTag = NodeTag::T_Gather;
}
unsafe impl<'mcx> NodeVariant<'mcx> for GatherMerge<'mcx> {
    const TAG: NodeTag = NodeTag::T_GatherMerge;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionPruneInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionPruneInfo;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionedRelPruneInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionedRelPruneInfo;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionPruneStepOp<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionPruneStepOp;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionPruneStepCombine<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionPruneStepCombine;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AppendRelInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_AppendRelInfo;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubqueryScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubqueryScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SetOp<'mcx> {
    const TAG: NodeTag = NodeTag::T_SetOp;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FunctionScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_FunctionScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TableFuncScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_TableFuncScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CteScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_CteScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NamedTuplestoreScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_NamedTuplestoreScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WorkTableScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_WorkTableScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RecursiveUnion<'mcx> {
    const TAG: NodeTag = NodeTag::T_RecursiveUnion;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ValuesScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_ValuesScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ForeignScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_ForeignScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Sort<'mcx> {
    const TAG: NodeTag = NodeTag::T_Sort;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IncrementalSort<'mcx> {
    const TAG: NodeTag = NodeTag::T_IncrementalSort;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Group<'mcx> {
    const TAG: NodeTag = NodeTag::T_Group;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Unique<'mcx> {
    const TAG: NodeTag = NodeTag::T_Unique;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Agg<'mcx> {
    const TAG: NodeTag = NodeTag::T_Agg;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowAgg<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowAgg;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ModifyTable<'mcx> {
    const TAG: NodeTag = NodeTag::T_ModifyTable;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Limit<'mcx> {
    const TAG: NodeTag = NodeTag::T_Limit;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NestLoop<'mcx> {
    const TAG: NodeTag = NodeTag::T_NestLoop;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NestLoopParam<'mcx> {
    const TAG: NodeTag = NodeTag::T_NestLoopParam;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MergeJoin<'mcx> {
    const TAG: NodeTag = NodeTag::T_MergeJoin;
}
unsafe impl<'mcx> NodeVariant<'mcx> for HashJoin<'mcx> {
    const TAG: NodeTag = NodeTag::T_HashJoin;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Hash<'mcx> {
    const TAG: NodeTag = NodeTag::T_Hash;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Material<'mcx> {
    const TAG: NodeTag = NodeTag::T_Material;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Memoize<'mcx> {
    const TAG: NodeTag = NodeTag::T_Memoize;
}
unsafe impl<'mcx> NodeVariant<'mcx> for LockRows<'mcx> {
    const TAG: NodeTag = NodeTag::T_LockRows;
}
unsafe impl NodeVariant<'_> for PlanRowMark {
    const TAG: NodeTag = NodeTag::T_PlanRowMark;
}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Result<'mcx> {}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for ProjectSet<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for SeqScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for SampleScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for IndexScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for IndexOnlyScan<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for BitmapAnd<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for BitmapOr<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for BitmapIndexScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for BitmapHeapScan<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Append<'mcx> {}
unsafe impl<'mcx> PlanVariant<'mcx> for MergeAppend<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Gather<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for GatherMerge<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for SubqueryScan<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for SetOp<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for FunctionScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for TableFuncScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for CteScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for NamedTuplestoreScan<'mcx> {}
unsafe impl<'mcx> PlanVariant<'mcx> for WorkTableScan<'mcx> {}
unsafe impl<'mcx> PlanVariant<'mcx> for RecursiveUnion<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for ValuesScan<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for ForeignScan<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Sort<'mcx> {}
// SAFETY: repr(C), Plan first via the Sort base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for IncrementalSort<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Group<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Unique<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Agg<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for ModifyTable<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Limit<'mcx> {}
// SAFETY: repr(C), Plan first via the Join base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for NestLoop<'mcx> {}
// SAFETY: repr(C), Plan first via the Join base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for MergeJoin<'mcx> {}
// SAFETY: repr(C), Plan first via the Join base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for HashJoin<'mcx> {}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Hash<'mcx> {}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Material<'mcx> {}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Memoize<'mcx> {}
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for LockRows<'mcx> {}

const _: () = {
    assert!(offset_of!(Result, plan) == 0);
    assert!(
        offset_of!(NodeRep<Result>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(ProjectSet, plan) == 0);
    assert!(
        offset_of!(NodeRep<ProjectSet>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Scan, plan) == 0);
    assert!(offset_of!(SeqScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<SeqScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(SampleScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<SampleScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(TidScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<TidScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(TidRangeScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<TidRangeScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(IndexScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<IndexScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(IndexOnlyScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<IndexOnlyScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(BitmapAnd, plan) == 0);
    assert!(
        offset_of!(NodeRep<BitmapAnd>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(BitmapOr, plan) == 0);
    assert!(
        offset_of!(NodeRep<BitmapOr>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(BitmapIndexScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<BitmapIndexScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(BitmapHeapScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<BitmapHeapScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Append, plan) == 0);
    assert!(offset_of!(NodeRep<Append>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(MergeAppend, plan) == 0);
    assert!(offset_of!(NodeRep<MergeAppend>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(Gather, plan) == 0);
    assert!(offset_of!(NodeRep<Gather>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(GatherMerge, plan) == 0);
    assert!(offset_of!(NodeRep<GatherMerge>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(SubqueryScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<SubqueryScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(SetOp, plan) == 0);
    assert!(offset_of!(NodeRep<SetOp>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(FunctionScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<FunctionScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(TableFuncScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<TableFuncScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(CteScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<CteScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(NamedTuplestoreScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<NamedTuplestoreScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(ValuesScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<ValuesScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(ForeignScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<ForeignScan>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Sort, plan) == 0);
    assert!(offset_of!(NodeRep<Sort>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(IncrementalSort, sort) == 0);
    assert!(
        offset_of!(NodeRep<IncrementalSort>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Group, plan) == 0);
    assert!(offset_of!(NodeRep<Group>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(Unique, plan) == 0);
    assert!(offset_of!(NodeRep<Unique>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(Agg, plan) == 0);
    assert!(offset_of!(NodeRep<Agg>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(ModifyTable, plan) == 0);
    assert!(
        offset_of!(NodeRep<ModifyTable>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Limit, plan) == 0);
    assert!(offset_of!(NodeRep<Limit>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(Join, plan) == 0);
    assert!(offset_of!(NestLoop, join) == 0);
    assert!(
        offset_of!(NodeRep<NestLoop>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(MergeJoin, join) == 0);
    assert!(
        offset_of!(NodeRep<MergeJoin>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(HashJoin, join) == 0);
    assert!(
        offset_of!(NodeRep<HashJoin>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Hash, plan) == 0);
    assert!(offset_of!(NodeRep<Hash>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(LockRows, plan) == 0);
    assert!(offset_of!(NodeRep<LockRows>, payload) == offset_of!(NodeRep<Plan>, payload));
    assert!(offset_of!(Memoize, plan) == 0);
    assert!(offset_of!(NodeRep<Memoize>, payload) == offset_of!(NodeRep<Plan>, payload));
};

fn is_plan_tag(tag: NodeTag) -> bool {
    matches!(
        tag,
        NodeTag::T_Result
            | NodeTag::T_ProjectSet
            | NodeTag::T_SeqScan
            | NodeTag::T_SampleScan
            | NodeTag::T_TidScan
            | NodeTag::T_TidRangeScan
            | NodeTag::T_IndexScan
            | NodeTag::T_IndexOnlyScan
            | NodeTag::T_BitmapAnd
            | NodeTag::T_BitmapOr
            | NodeTag::T_BitmapIndexScan
            | NodeTag::T_BitmapHeapScan
            | NodeTag::T_Append
            | NodeTag::T_MergeAppend
            | NodeTag::T_Gather
            | NodeTag::T_GatherMerge
            | NodeTag::T_SubqueryScan
            | NodeTag::T_SetOp
            | NodeTag::T_FunctionScan
            | NodeTag::T_TableFuncScan
            | NodeTag::T_CteScan
            | NodeTag::T_NamedTuplestoreScan
            | NodeTag::T_WorkTableScan
            | NodeTag::T_RecursiveUnion
            | NodeTag::T_ValuesScan
            | NodeTag::T_ForeignScan
            | NodeTag::T_Sort
            | NodeTag::T_IncrementalSort
            | NodeTag::T_Group
            | NodeTag::T_Unique
            | NodeTag::T_Agg
            | NodeTag::T_WindowAgg
            | NodeTag::T_ModifyTable
            | NodeTag::T_Limit
            | NodeTag::T_NestLoop
            | NodeTag::T_MergeJoin
            | NodeTag::T_HashJoin
            | NodeTag::T_Hash
            | NodeTag::T_Material
            | NodeTag::T_Memoize
            | NodeTag::T_LockRows
    )
}

impl<'mcx> Node<'mcx> {
    #[inline]
    pub fn as_planned_stmt(self) -> Option<&'mcx PlannedStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_result(self) -> Option<&'mcx Result<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_project_set(self) -> Option<&'mcx ProjectSet<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_seq_scan(self) -> Option<&'mcx SeqScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sample_scan(self) -> Option<&'mcx SampleScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_tid_scan(self) -> Option<&'mcx TidScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_tid_range_scan(self) -> Option<&'mcx TidRangeScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_index_scan(self) -> Option<&'mcx IndexScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_index_only_scan(self) -> Option<&'mcx IndexOnlyScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitmap_and(self) -> Option<&'mcx BitmapAnd<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitmap_or(self) -> Option<&'mcx BitmapOr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitmap_index_scan(self) -> Option<&'mcx BitmapIndexScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_bitmap_heap_scan(self) -> Option<&'mcx BitmapHeapScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_function_scan(self) -> Option<&'mcx FunctionScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_table_func_scan(self) -> Option<&'mcx TableFuncScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_cte_scan(self) -> Option<&'mcx CteScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_named_tuplestore_scan(self) -> Option<&'mcx NamedTuplestoreScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_work_table_scan(self) -> Option<&'mcx WorkTableScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_recursive_union(self) -> Option<&'mcx RecursiveUnion<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_values_scan(self) -> Option<&'mcx ValuesScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_foreign_scan(self) -> Option<&'mcx ForeignScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sort(self) -> Option<&'mcx Sort<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_incremental_sort(self) -> Option<&'mcx IncrementalSort<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_material(self) -> Option<&'mcx Material<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_memoize(self) -> Option<&'mcx Memoize<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_group(self) -> Option<&'mcx Group<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_unique(self) -> Option<&'mcx Unique<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_append(self) -> Option<&'mcx Append<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_append(self) -> Option<&'mcx MergeAppend<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_gather(self) -> Option<&'mcx Gather<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_gather_merge(self) -> Option<&'mcx GatherMerge<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_partition_prune_info(self) -> Option<&'mcx PartitionPruneInfo<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_partitioned_rel_prune_info(self) -> Option<&'mcx PartitionedRelPruneInfo<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_partition_prune_step_op(self) -> Option<&'mcx PartitionPruneStepOp<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_partition_prune_step_combine(self) -> Option<&'mcx PartitionPruneStepCombine<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_append_rel_info(self) -> Option<&'mcx AppendRelInfo<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_subquery_scan(self) -> Option<&'mcx SubqueryScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_set_op(self) -> Option<&'mcx SetOp<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_agg(self) -> Option<&'mcx Agg<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_agg(self) -> Option<&'mcx WindowAgg<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_modify_table(self) -> Option<&'mcx ModifyTable<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_limit(self) -> Option<&'mcx Limit<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_nest_loop_param(self) -> Option<&'mcx NestLoopParam<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_nest_loop(self) -> Option<&'mcx NestLoop<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_join(self) -> Option<&'mcx MergeJoin<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_hash_join(self) -> Option<&'mcx HashJoin<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_hash(self) -> Option<&'mcx Hash<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_lock_rows(self) -> Option<&'mcx LockRows<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_plan_row_mark(self) -> Option<&'mcx PlanRowMark> {
        self.as_variant()
    }

    #[inline]
    pub fn as_plan_inval_item(self) -> Option<&'mcx PlanInvalItem> {
        self.as_variant()
    }

    /// C's `(Plan *) node` cast: the embedded base of any plan-tree node.
    #[inline]
    pub fn as_plan(self) -> Option<&'mcx Plan<'mcx>> {
        if is_plan_tag(self.node_tag()) {
            // SAFETY: is_plan_tag proves the payload is repr(C) with Plan
            // first (PlanVariant contract) at the const-asserted offset.
            Some(unsafe { &(*self.rep_ptr::<Plan>()).payload })
        } else {
            None
        }
    }

    /// Setrefs-style in-place fixup of the embedded [`Plan`] base.
    ///
    /// # Safety
    /// Same contract as [`Node::with_mut`].
    pub unsafe fn with_plan_mut<R>(self, f: impl FnOnce(&mut Plan<'mcx>) -> R) -> Option<R> {
        if !is_plan_tag(self.node_tag()) {
            return None;
        }
        // SAFETY: tag proves the Plan prefix (see as_plan); exclusivity is
        // the caller's contract; rep_ptr carries write provenance.
        Some(f(unsafe { &mut (*self.rep_ptr::<Plan>()).payload }))
    }
}
