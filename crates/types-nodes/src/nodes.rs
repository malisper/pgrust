//! The central plan-node dispatch enum (nodes/nodes.h `Node` over plan nodes),
//! trimmed.
//!
//! C's `Plan *` is a tagged pointer to any concrete plan node; the owned model
//! is this enum. Variants are added as the nodes' executor units are ported.

use alloc::boxed::Box;

/// A plan-tree node (`Plan *` in C). The `NodeTag` is the enum discriminant.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Node {
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material),
}

impl Node {
    /// `&((Plan *) node)->...` — the embedded `Plan` base.
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan {
        match self {
            Node::Material(m) => &m.plan,
        }
    }

    /// `outerPlan(node)` (plannodes.h) — `node->plan.lefttree`.
    pub fn outer_plan(&self) -> Option<&Node> {
        self.plan_head().lefttree.as_deref()
    }
}

/// Keep `Box<Node>` cheap to talk about in field positions.
pub type NodePtr = Box<Node>;
