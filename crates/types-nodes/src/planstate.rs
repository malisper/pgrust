//! The central plan-state dispatch enum (`PlanState *` in C), trimmed.
//!
//! C's `PlanState *` is a tagged pointer to any concrete `<Node>State`; the
//! owned model is this enum (the `castNode` checks become match arms).
//! Variants are added as the nodes' executor units are ported.

use alloc::boxed::Box;

use crate::execnodes::PlanStateData;

/// A plan-state-tree node (`PlanState *` in C). The `NodeTag` is the enum
/// discriminant.
#[derive(Debug)]
#[non_exhaustive]
pub enum PlanStateNode {
    /// `T_MaterialState`.
    Material(Box<crate::nodeforeigncustom::MaterialState>),
}

impl PlanStateNode {
    /// `&((PlanState *) node)->...` — the embedded `PlanState` head every
    /// `<Node>State` struct begins with.
    pub fn ps_head(&self) -> &PlanStateData {
        match self {
            PlanStateNode::Material(m) => &m.ss.ps,
        }
    }

    /// `&mut ((PlanState *) node)->...`.
    pub fn ps_head_mut(&mut self) -> &mut PlanStateData {
        match self {
            PlanStateNode::Material(m) => &mut m.ss.ps,
        }
    }
}
