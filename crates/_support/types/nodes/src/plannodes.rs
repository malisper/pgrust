// Plan-tree nodes; field names/order mirror vendor/plannodes.h
// (tests: plannedstmt_plan_result_field_order_match_c).
#![allow(non_snake_case)]

use core::mem::offset_of;

use types_core::{Cardinality, Cost, ParseLoc};

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, NodeList, OidList};
use crate::node_tree::{Node, NodeRep, NodeVariant};
use crate::nodes_enums::CmdType;
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
// SAFETY: repr(C), Plan first (offset asserted below), tag in is_plan_tag.
unsafe impl<'mcx> PlanVariant<'mcx> for Result<'mcx> {}

const _: () = {
    assert!(offset_of!(Result, plan) == 0);
    assert!(
        offset_of!(NodeRep<Result>, payload) == offset_of!(NodeRep<Plan>, payload)
    );
};

fn is_plan_tag(tag: NodeTag) -> bool {
    matches!(tag, NodeTag::T_Result)
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
