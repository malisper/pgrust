//! The central plan-node dispatch enum (nodes/nodes.h `Node` over plan nodes),
//! trimmed.
//!
//! C's `Plan *` is a tagged pointer to any concrete plan node; the owned model
//! is this enum. Variants are added as the nodes' executor units are ported.

use mcx::{Mcx, PgBox};
use types_error::PgResult;

/// `NodeTag` (nodes/nodes.h) — the generated node-type enumeration. Node
/// *identity* in the owned model is the dispatch enums' variants
/// ([`crate::PlanStateNode`], [`Node`]); this carries the C tag value where
/// ports read it as data (e.g. `Path.pathtype`). The full enum has ~480
/// generated members; rather than transcribe them all, the tag is a newtype
/// over the generated numeric value, with named `T_*` constants defined where
/// ports consume them (values verified against the PostgreSQL 18.3 generated
/// `nodetags.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeTag(pub u32);

impl core::fmt::Display for NodeTag {
    /// C prints tags as their integer value (`(int) nodeTag(node)`).
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

// Plan-node tags (nodes/nodetags.h), copied as ports consume them. The values
// are PostgreSQL 18.3's generated enumeration order.
pub const T_Result: NodeTag = NodeTag(331);
pub const T_SeqScan: NodeTag = NodeTag(339);
pub const T_Append: NodeTag = NodeTag(334);
pub const T_MergeAppend: NodeTag = NodeTag(335);
pub const T_IndexScan: NodeTag = NodeTag(341);
pub const T_IndexOnlyScan: NodeTag = NodeTag(342);
pub const T_FunctionScan: NodeTag = NodeTag(348);
pub const T_TableFuncScan: NodeTag = NodeTag(350);
pub const T_CteScan: NodeTag = NodeTag(351);
pub const T_NamedTuplestoreScan: NodeTag = NodeTag(352);
pub const T_WorkTableScan: NodeTag = NodeTag(353);
pub const T_ForeignScan: NodeTag = NodeTag(354);
pub const T_TidRangeScan: NodeTag = NodeTag(346);
pub const T_CustomScan: NodeTag = NodeTag(355);
pub const T_MergeJoin: NodeTag = NodeTag(358);
pub const T_Material: NodeTag = NodeTag(360);
pub const T_Sort: NodeTag = NodeTag(362);
pub const T_Limit: NodeTag = NodeTag(373);

// Executor-state node tags (nodes/nodetags.h), copied as ports consume them
// (`T_MaterialState`/`T_MergeJoinState` live with their state structs). The
// values are PostgreSQL 18.3's generated enumeration order.
pub const T_ResultState: NodeTag = NodeTag(394);
pub const T_AppendState: NodeTag = NodeTag(397);
pub const T_SeqScanState: NodeTag = NodeTag(403);
pub const T_SampleScanState: NodeTag = NodeTag(404);
pub const T_IndexScanState: NodeTag = NodeTag(405);
pub const T_IndexOnlyScanState: NodeTag = NodeTag(406);
pub const T_BitmapHeapScanState: NodeTag = NodeTag(408);
pub const T_TidScanState: NodeTag = NodeTag(409);
pub const T_TidRangeScanState: NodeTag = NodeTag(410);
pub const T_SubqueryScanState: NodeTag = NodeTag(411);
pub const T_ForeignScanState: NodeTag = NodeTag(418);
pub const T_CustomScanState: NodeTag = NodeTag(419);
pub const T_LimitState: NodeTag = NodeTag(437);

/// `CmdType` (nodes/nodes.h) — values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum CmdType {
    #[default]
    CMD_UNKNOWN = 0,
    /// select stmt
    CMD_SELECT = 1,
    /// update stmt
    CMD_UPDATE = 2,
    /// insert stmt
    CMD_INSERT = 3,
    /// delete stmt
    CMD_DELETE = 4,
    /// merge stmt
    CMD_MERGE = 5,
    /// cmds like create, destroy, copy, vacuum, etc.
    CMD_UTILITY = 6,
    /// dummy command for instead nothing rules with qual
    CMD_NOTHING = 7,
}

pub use CmdType::{
    CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_NOTHING, CMD_SELECT, CMD_UNKNOWN, CMD_UPDATE,
    CMD_UTILITY,
};

/// `OnConflictAction` (nodes/nodes.h) — what to do at ON CONFLICT. Values
/// verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum OnConflictAction {
    /// No "ON CONFLICT" clause.
    ONCONFLICT_NONE = 0,
    /// ON CONFLICT ... DO NOTHING.
    ONCONFLICT_NOTHING = 1,
    /// ON CONFLICT ... DO UPDATE.
    ONCONFLICT_UPDATE = 2,
}

pub use OnConflictAction::{ONCONFLICT_NONE, ONCONFLICT_NOTHING, ONCONFLICT_UPDATE};

/// A plan-tree node (`Plan *` in C). The `NodeTag` is the enum discriminant.
/// Carries the allocator lifetime of the context the plan tree lives in;
/// copying allocates, so it goes through the fallible [`Node::clone_in`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Node<'mcx> {
    /// `T_Append`.
    Append(crate::nodeappend::Append<'mcx>),
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material<'mcx>),
    /// `T_MergeAppend`.
    MergeAppend(crate::nodemergeappend::MergeAppend<'mcx>),
    /// `T_MergeJoin`.
    MergeJoin(crate::nodemergejoin::MergeJoin<'mcx>),
    /// `T_Memoize`.
    Memoize(crate::nodememoize::Memoize<'mcx>),
    /// `T_IndexOnlyScan`.
    IndexOnlyScan(crate::nodeindexonlyscan::IndexOnlyScan<'mcx>),
    /// `T_Limit`.
    Limit(crate::nodelimit::Limit<'mcx>),
    /// `T_Sort`.
    Sort(crate::nodesort::Sort<'mcx>),
    /// `T_TableFuncScan`.
    TableFuncScan(crate::nodetablefuncscan::TableFuncScan<'mcx>),
    /// `T_NestLoop`.
    NestLoop(crate::nodenestloop::NestLoop<'mcx>),
    /// `T_HashJoin`.
    HashJoin(crate::nodehashjoin::HashJoin<'mcx>),
    /// `T_Hash` — the inner child of a HashJoin.
    Hash(crate::nodehashjoin::Hash<'mcx>),
    /// `T_TidRangeScan`.
    TidRangeScan(crate::nodetidrangescan::TidRangeScan<'mcx>),
    /// `T_SeqScan`.
    SeqScan(crate::nodeseqscan::SeqScan<'mcx>),
    /// `T_ForeignScan`.
    ForeignScan(crate::nodeforeigncustom::ForeignScan<'mcx>),
}

impl<'mcx> Node<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete plan node.
    pub fn tag(&self) -> NodeTag {
        match self {
            Node::Append(_) => T_Append,
            Node::Material(_) => T_Material,
            Node::MergeAppend(_) => T_MergeAppend,
            Node::MergeJoin(_) => T_MergeJoin,
            Node::Memoize(_) => crate::nodememoize::T_Memoize,
            Node::IndexOnlyScan(_) => T_IndexOnlyScan,
            Node::Limit(_) => T_Limit,
            Node::Sort(_) => T_Sort,
            Node::TableFuncScan(_) => T_TableFuncScan,
            Node::NestLoop(_) => crate::nodenestloop::T_NestLoop,
            Node::HashJoin(_) => crate::nodehashjoin::T_HashJoin,
            Node::Hash(_) => crate::nodehashjoin::T_Hash,
            Node::TidRangeScan(_) => T_TidRangeScan,
            Node::SeqScan(_) => T_SeqScan,
            Node::ForeignScan(_) => T_ForeignScan,
        }
    }

    /// `&((Plan *) node)->...` — the embedded `Plan` base.
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        match self {
            Node::Append(a) => &a.plan,
            Node::Material(m) => &m.plan,
            Node::MergeAppend(m) => &m.plan,
            Node::MergeJoin(m) => &m.join.plan,
            Node::Memoize(m) => &m.plan,
            Node::IndexOnlyScan(m) => &m.scan.plan,
            Node::Limit(m) => &m.plan,
            Node::Sort(s) => &s.plan,
            Node::TableFuncScan(t) => &t.scan.plan,
            Node::NestLoop(m) => &m.join.plan,
            Node::HashJoin(h) => &h.join.plan,
            Node::Hash(h) => &h.plan,
            Node::TidRangeScan(t) => &t.scan.plan,
            Node::SeqScan(s) => &s.scan.plan,
            Node::ForeignScan(f) => &f.scan.plan,
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
            Node::Append(a) => Ok(Node::Append(a.clone_in(mcx)?)),
            Node::Material(m) => Ok(Node::Material(m.clone_in(mcx)?)),
            Node::MergeAppend(m) => Ok(Node::MergeAppend(m.clone_in(mcx)?)),
            Node::MergeJoin(m) => Ok(Node::MergeJoin(m.clone_in(mcx)?)),
            Node::Memoize(m) => Ok(Node::Memoize(m.clone_in(mcx)?)),
            Node::IndexOnlyScan(m) => Ok(Node::IndexOnlyScan(m.clone_in(mcx)?)),
            Node::Limit(m) => Ok(Node::Limit(m.clone_in(mcx)?)),
            Node::Sort(s) => Ok(Node::Sort(s.clone_in(mcx)?)),
            Node::TableFuncScan(t) => Ok(Node::TableFuncScan(t.clone_in(mcx)?)),
            Node::NestLoop(m) => Ok(Node::NestLoop(m.clone_in(mcx)?)),
            Node::HashJoin(h) => Ok(Node::HashJoin(h.clone_in(mcx)?)),
            Node::Hash(h) => Ok(Node::Hash(h.clone_in(mcx)?)),
            Node::TidRangeScan(t) => Ok(Node::TidRangeScan(t.clone_in(mcx)?)),
            Node::SeqScan(s) => Ok(Node::SeqScan(s.clone_in(mcx)?)),
            Node::ForeignScan(f) => Ok(Node::ForeignScan(f.clone_in(mcx)?)),
        }
    }
}

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;
