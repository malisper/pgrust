//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use mcx::PgBox;
use crate::nodes::NodeTag;

use crate::execnodes::PlanStateData;
use crate::execnodes::ScanStateData;
use crate::execnodes::T_MaterialState;
use crate::nodemergejoin::T_MergeJoinState;

/// A plan-state-tree node (`PlanState *` in C). The `NodeTag` is the enum
/// discriminant. The state tree is context-allocated (C: `makeNode` in the
/// per-query context), so it carries the allocator lifetime.
#[derive(Debug)]
#[non_exhaustive]
pub enum PlanStateNode<'mcx> {
    /// `T_MaterialState`.
    Material(PgBox<'mcx, crate::nodeforeigncustom::MaterialState<'mcx>>),
    /// `T_MergeJoinState`.
    MergeJoin(PgBox<'mcx, crate::nodemergejoin::MergeJoinStateData<'mcx>>),
}

impl<'mcx> PlanStateNode<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete state node.
    pub fn tag(&self) -> NodeTag {
        match self {
            PlanStateNode::Material(_) => T_MaterialState,
            PlanStateNode::MergeJoin(_) => T_MergeJoinState,
        }
    }

    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &m.ss.ps,
            PlanStateNode::MergeJoin(m) => &m.js.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData<'mcx> {
        match self {
            PlanStateNode::Material(m) => &mut m.ss.ps,
            PlanStateNode::MergeJoin(m) => &mut m.js.ps,
        }
    }

    /// `(ScanState *) node` — the embedded `ScanState` of a relation-scan-node
    /// state (`SeqScanState`, `IndexScanState`, ... — every concrete scan-node
    /// struct begins with a `ScanState`). `None` for non-scan nodes. Returns
    /// `None` for every current variant; relation-scan variants add their arm
    /// here as their executor units land.
    pub fn as_scan_state(&self) -> Option<&ScanStateData<'mcx>> {
        match self {
            // Material/MergeJoin are not relation-scan nodes (the C
            // `search_plan_tree` `default:` / join cases).
            PlanStateNode::Material(_) | PlanStateNode::MergeJoin(_) => None,
        }
    }

    /// `outerPlanState(node)` (execnodes.h) — `node->lefttree`, the input plan
    /// state descended through by `Result`/`Limit`. `None` when there is none.
    pub fn outer_plan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        self.ps_head().lefttree.as_deref()
    }

    /// `((AppendState *) node)->appendplans[0..as_nplans]` — the Append's input
    /// plan states. `None` until the `AppendState` variant lands.
    pub fn append_input_states(&self) -> Option<&[PgBox<'mcx, PlanStateNode<'mcx>>]> {
        match self {
            PlanStateNode::Material(_) | PlanStateNode::MergeJoin(_) => None,
        }
    }

    /// `((SubqueryScanState *) node)->subplan` — the SubqueryScan's child plan
    /// state (kept separately from `lefttree`). `None` until the
    /// `SubqueryScanState` variant lands.
    pub fn subquery_subplan_state(&self) -> Option<&PlanStateNode<'mcx>> {
        match self {
            PlanStateNode::Material(_) | PlanStateNode::MergeJoin(_) => None,
        }
    }
}
