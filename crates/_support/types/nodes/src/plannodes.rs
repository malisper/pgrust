// Plan-tree nodes; field names/order mirror vendor/plannodes.h
// (tests: plannedstmt_plan_result_field_order_match_c).
#![allow(non_snake_case)]

use core::mem::offset_of;

use types_core::{Cardinality, Cost, Index, Oid, ParseLoc};

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, NodeList, OidList};
use crate::node_tree::{Node, NodeRep, NodeVariant};
use crate::nodes_enums::{CmdType, LimitOption};
use crate::tags::NodeTag;

pub struct PlannedStmt<'mcx> {
    pub commandType: CmdType,
    pub queryId: i64,
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
    // Divergence: C subplans cells can be NULL; NodeList cells cannot. Revisit
    // when SubPlan lands (SELECT 1 and near-trivial plans have NIL here).
    pub subplans: NodeList<'mcx>,
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
            queryId: 0,
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
            subplans: NodeList::nil(),
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

/// # Safety: implementors must be `repr(C)` with a [`Plan`] first field, so a
/// `NodeRep<Self>` reads as a `NodeRep<Plan>` prefix, and their tag must be
/// listed in [`is_plan_tag`].
pub unsafe trait PlanVariant<'mcx>: NodeVariant<'mcx> {}

// SAFETY (each): tag/type pairing mirrors plannodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for PlannedStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_PlannedStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Result<'mcx> {
    const TAG: NodeTag = NodeTag::T_Result;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SeqScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SeqScan;
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
unsafe impl<'mcx> NodeVariant<'mcx> for Sort<'mcx> {
    const TAG: NodeTag = NodeTag::T_Sort;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Agg<'mcx> {
    const TAG: NodeTag = NodeTag::T_Agg;
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
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Result<'mcx> {}
// SAFETY: repr(C), Plan first via the Scan base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for SeqScan<'mcx> {}
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
unsafe impl<'mcx> PlanVariant<'mcx> for Sort<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Agg<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for ModifyTable<'mcx> {}
// SAFETY: repr(C), Plan first (offsets asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Limit<'mcx> {}
// SAFETY: repr(C), Plan first via the Join base (offsets asserted below).
unsafe impl<'mcx> PlanVariant<'mcx> for NestLoop<'mcx> {}

const _: () = {
    assert!(offset_of!(Result, plan) == 0);
    assert!(
        offset_of!(NodeRep<Result>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
    assert!(offset_of!(Scan, plan) == 0);
    assert!(offset_of!(SeqScan, scan) == 0);
    assert!(
        offset_of!(NodeRep<SeqScan>, payload) == offset_of!(NodeRep<Plan>, payload)
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
    assert!(offset_of!(Sort, plan) == 0);
    assert!(offset_of!(NodeRep<Sort>, payload) == offset_of!(NodeRep<Plan>, payload));
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
};

fn is_plan_tag(tag: NodeTag) -> bool {
    matches!(
        tag,
        NodeTag::T_Result
            | NodeTag::T_SeqScan
            | NodeTag::T_IndexScan
            | NodeTag::T_IndexOnlyScan
            | NodeTag::T_BitmapAnd
            | NodeTag::T_BitmapOr
            | NodeTag::T_BitmapIndexScan
            | NodeTag::T_BitmapHeapScan
            | NodeTag::T_Sort
            | NodeTag::T_Agg
            | NodeTag::T_ModifyTable
            | NodeTag::T_Limit
            | NodeTag::T_NestLoop
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
    pub fn as_seq_scan(self) -> Option<&'mcx SeqScan<'mcx>> {
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
    pub fn as_sort(self) -> Option<&'mcx Sort<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_agg(self) -> Option<&'mcx Agg<'mcx>> {
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
    pub fn as_nest_loop(self) -> Option<&'mcx NestLoop<'mcx>> {
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
