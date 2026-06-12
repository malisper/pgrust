//! The central plan-node dispatch enum (nodes/nodes.h `Node` over plan nodes),
//! trimmed.
//!
//! C's `Plan *` is a tagged pointer to any concrete plan node; the owned model
//! is this enum. Variants are added as the nodes' executor units are ported.

use mcx::{Mcx, PgBox};
use types_core::{NodeTag, PgResult};

// Plan-node tags (nodes/nodetags.h), copied as ports consume them. The values
// are PostgreSQL 18.3's generated enumeration order.
pub const T_Result: NodeTag = 331;
pub const T_Append: NodeTag = 334;
pub const T_MergeAppend: NodeTag = 335;
pub const T_IndexScan: NodeTag = 341;
pub const T_IndexOnlyScan: NodeTag = 342;
pub const T_FunctionScan: NodeTag = 348;
pub const T_TableFuncScan: NodeTag = 350;
pub const T_CteScan: NodeTag = 351;
pub const T_NamedTuplestoreScan: NodeTag = 352;
pub const T_WorkTableScan: NodeTag = 353;
pub const T_CustomScan: NodeTag = 355;
pub const T_Material: NodeTag = 360;
pub const T_Sort: NodeTag = 362;

/// A plan-tree node (`Plan *` in C). The `NodeTag` is the enum discriminant.
/// Carries the allocator lifetime of the context the plan tree lives in;
/// copying allocates, so it goes through the fallible [`Node::clone_in`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Node<'mcx> {
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material<'mcx>),
}

impl<'mcx> Node<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete plan node.
    pub fn tag(&self) -> NodeTag {
        match self {
            Node::Material(_) => T_Material,
        }
    }

    /// `&((Plan *) node)->...` — the embedded `Plan` base.
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        match self {
            Node::Material(m) => &m.plan,
        }
    }

    /// `outerPlan(node)` (plannodes.h) — `node->plan.lefttree`.
    pub fn outer_plan(&self) -> Option<&Node<'mcx>> {
        self.plan_head().lefttree.as_deref()
    }

    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {
        match self {
            Node::Material(m) => Ok(Node::Material(m.clone_in(mcx)?)),
        }
    }
}

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;
