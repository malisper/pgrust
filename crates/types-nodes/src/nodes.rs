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

// The four List flavours (nodes/pg_list.h).  T_List is the very first tag.
pub const T_List: NodeTag = NodeTag(1);
pub const T_IntList: NodeTag = NodeTag(471);
pub const T_OidList: NodeTag = NodeTag(472);
pub const T_XidList: NodeTag = NodeTag(473);

pub const T_Result: NodeTag = NodeTag(331);
pub const T_SeqScan: NodeTag = NodeTag(339);
pub const T_Append: NodeTag = NodeTag(334);
pub const T_MergeAppend: NodeTag = NodeTag(335);
pub const T_BitmapAnd: NodeTag = NodeTag(337);
pub const T_IndexScan: NodeTag = NodeTag(341);
pub const T_IndexOnlyScan: NodeTag = NodeTag(342);
pub const T_FunctionScan: NodeTag = NodeTag(348);
pub const T_ValuesScan: NodeTag = NodeTag(349);
pub const T_TableFuncScan: NodeTag = NodeTag(350);
pub const T_CteScan: NodeTag = NodeTag(351);
pub const T_NamedTuplestoreScan: NodeTag = NodeTag(352);
pub const T_WorkTableScan: NodeTag = NodeTag(353);
pub const T_ForeignScan: NodeTag = NodeTag(354);
pub const T_SubqueryScan: NodeTag = NodeTag(347);
pub const T_TidRangeScan: NodeTag = NodeTag(346);
pub const T_CustomScan: NodeTag = NodeTag(355);
pub const T_MergeJoin: NodeTag = NodeTag(358);
pub const T_Material: NodeTag = NodeTag(360);
pub const T_Sort: NodeTag = NodeTag(362);
pub const T_SetOp: NodeTag = NodeTag(371);
pub const T_Limit: NodeTag = NodeTag(373);

// Executor-state node tags (nodes/nodetags.h), copied as ports consume them
// (`T_MaterialState`/`T_MergeJoinState` live with their state structs). The
// values are PostgreSQL 18.3's generated enumeration order.
pub const T_ResultState: NodeTag = NodeTag(394);
pub const T_ModifyTableState: NodeTag = NodeTag(396);
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
/// `T_JunkFilter` (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_JunkFilter: NodeTag = NodeTag(385);
/// `T_OnConflictSetState` (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_OnConflictSetState: NodeTag = NodeTag(386);
/// `T_MergeActionState` (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_MergeActionState: NodeTag = NodeTag(387);
/// `T_MergeAction` (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_MergeAction: NodeTag = NodeTag(54);

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
    /// `T_ModifyTable`.
    ModifyTable(crate::modifytable::ModifyTable<'mcx>),
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material<'mcx>),
    /// `T_Gather`.
    Gather(crate::nodegather::Gather<'mcx>),
    /// `T_GatherMerge`.
    GatherMerge(crate::nodegathermerge::GatherMerge<'mcx>),
    /// `T_MergeAppend`.
    MergeAppend(crate::nodemergeappend::MergeAppend<'mcx>),
    /// `T_BitmapAnd`.
    BitmapAnd(crate::nodebitmapand::BitmapAnd<'mcx>),
    /// `T_MergeJoin`.
    MergeJoin(crate::nodemergejoin::MergeJoin<'mcx>),
    /// `T_RecursiveUnion`.
    RecursiveUnion(crate::noderecursiveunion::RecursiveUnion<'mcx>),
    /// `T_Group`.
    Group(crate::nodegroup::Group<'mcx>),
    /// `T_ProjectSet`.
    ProjectSet(crate::nodeprojectset::ProjectSet<'mcx>),
    /// `T_Result`.
    Result(crate::noderesult::Result<'mcx>),
    /// `T_SetOp`.
    SetOp(crate::nodesetop::SetOp<'mcx>),
    /// `T_Memoize`.
    Memoize(crate::nodememoize::Memoize<'mcx>),
    /// `T_IndexScan`.
    IndexScan(crate::nodeindexscan::IndexScan<'mcx>),
    /// `T_IndexOnlyScan`.
    IndexOnlyScan(crate::nodeindexonlyscan::IndexOnlyScan<'mcx>),
    /// `T_BitmapIndexScan`.
    BitmapIndexScan(crate::nodebitmapindexscan::BitmapIndexScan<'mcx>),
    /// `T_Limit`.
    Limit(crate::nodelimit::Limit<'mcx>),
    /// `T_Unique`.
    Unique(crate::nodeunique::Unique<'mcx>),
    /// `T_Sort`.
    Sort(crate::nodesort::Sort<'mcx>),
    /// `T_TableFuncScan`.
    TableFuncScan(crate::nodetablefuncscan::TableFuncScan<'mcx>),
    /// `T_ValuesScan`.
    ValuesScan(crate::nodevaluesscan::ValuesScan<'mcx>),
    /// `T_CteScan`.
    CteScan(crate::nodectescan::CteScan<'mcx>),
    /// `T_NamedTuplestoreScan`.
    NamedTuplestoreScan(crate::nodenamedtuplestorescan::NamedTuplestoreScan<'mcx>),
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
    /// `T_SubqueryScan`.
    SubqueryScan(crate::nodeindexscan::SubqueryScan<'mcx>),
    /// `T_ForeignScan`.
    ForeignScan(crate::nodeforeigncustom::ForeignScan<'mcx>),
    /// `T_CustomScan`.
    CustomScan(crate::nodeforeigncustom::CustomScan<'mcx>),
    /// An expression node (`Const`, `BoolExpr`, `Var`, …) carried as a `Node`.
    ///
    /// In C, every `Expr`-derived node is a `Node *` via the
    /// `Expr`/`Node` supertype relationship, and constructors such as
    /// `makeConst`/`makeBoolExpr` are routinely cast to `(Node *)` and returned
    /// through `Node *`-typed APIs (`get_typdefault`, the partition-qual list,
    /// `stringToNode`). This variant is that cast: it embeds the lifetime-free
    /// [`crate::primnodes::Expr`] subtree as a `Node` without collapsing the two
    /// types (the split Expr/Node model is preserved — `Expr` remains its own
    /// enum, this arm only makes an expression reachable where a `Node` is
    /// expected). Additive: the enum is `#[non_exhaustive]`.
    Expr(crate::primnodes::Expr),
}

impl<'mcx> Node<'mcx> {
    /// `nodeTag(node)` — the C node tag of the concrete plan node.
    pub fn tag(&self) -> NodeTag {
        match self {
            Node::Append(_) => T_Append,
            Node::ModifyTable(_) => crate::modifytable::T_ModifyTable,
            Node::Material(_) => T_Material,
            Node::Gather(_) => crate::nodegather::T_Gather,
            Node::GatherMerge(_) => crate::nodegathermerge::T_GatherMerge,
            Node::MergeAppend(_) => T_MergeAppend,
            Node::BitmapAnd(_) => T_BitmapAnd,
            Node::MergeJoin(_) => T_MergeJoin,
            Node::RecursiveUnion(_) => crate::noderecursiveunion::T_RecursiveUnion,
            Node::Group(_) => crate::nodegroup::T_Group,
            Node::ProjectSet(_) => crate::nodeprojectset::T_ProjectSet,
            Node::Result(_) => T_Result,
            Node::SetOp(_) => T_SetOp,
            Node::Memoize(_) => crate::nodememoize::T_Memoize,
            Node::IndexScan(_) => T_IndexScan,
            Node::IndexOnlyScan(_) => T_IndexOnlyScan,
            Node::BitmapIndexScan(_) => crate::nodebitmapindexscan::T_BitmapIndexScan,
            Node::Limit(_) => T_Limit,
            Node::Unique(_) => crate::nodeunique::T_Unique,
            Node::Sort(_) => T_Sort,
            Node::TableFuncScan(_) => T_TableFuncScan,
            Node::ValuesScan(_) => T_ValuesScan,
            Node::CteScan(_) => crate::nodectescan::T_CteScan,
            Node::NamedTuplestoreScan(_) => T_NamedTuplestoreScan,
            Node::NestLoop(_) => crate::nodenestloop::T_NestLoop,
            Node::HashJoin(_) => crate::nodehashjoin::T_HashJoin,
            Node::Hash(_) => crate::nodehashjoin::T_Hash,
            Node::TidRangeScan(_) => T_TidRangeScan,
            Node::SeqScan(_) => T_SeqScan,
            Node::SubqueryScan(_) => T_SubqueryScan,
            Node::ForeignScan(_) => T_ForeignScan,
            Node::CustomScan(_) => T_CustomScan,
            Node::Expr(e) => expr_tag(e),
        }
    }

    /// `&((Plan *) node)->...` — the embedded `Plan` base.
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        match self {
            Node::Append(a) => &a.plan,
            Node::ModifyTable(m) => &m.plan,
            Node::Material(m) => &m.plan,
            Node::Gather(g) => &g.plan,
            Node::GatherMerge(g) => &g.plan,
            Node::MergeAppend(m) => &m.plan,
            Node::BitmapAnd(b) => &b.plan,
            Node::MergeJoin(m) => &m.join.plan,
            Node::RecursiveUnion(r) => &r.plan,
            Node::Group(g) => &g.plan,
            Node::ProjectSet(p) => &p.plan,
            Node::Result(r) => &r.plan,
            Node::SetOp(s) => &s.plan,
            Node::Memoize(m) => &m.plan,
            Node::IndexScan(m) => &m.scan.plan,
            Node::IndexOnlyScan(m) => &m.scan.plan,
            Node::BitmapIndexScan(m) => &m.scan.plan,
            Node::Limit(m) => &m.plan,
            Node::Unique(u) => &u.plan,
            Node::Sort(s) => &s.plan,
            Node::TableFuncScan(t) => &t.scan.plan,
            Node::ValuesScan(v) => &v.scan.plan,
            Node::CteScan(c) => &c.scan.plan,
            Node::NamedTuplestoreScan(n) => &n.scan.plan,
            Node::NestLoop(m) => &m.join.plan,
            Node::HashJoin(h) => &h.join.plan,
            Node::Hash(h) => &h.plan,
            Node::TidRangeScan(t) => &t.scan.plan,
            Node::SeqScan(s) => &s.scan.plan,
            Node::SubqueryScan(s) => &s.scan.plan,
            Node::ForeignScan(f) => &f.scan.plan,
            Node::CustomScan(c) => &c.scan.plan,
            // An expression node has no embedded `Plan` (C: `((Plan *) expr)`
            // would be a type error — `plan_head` is only called on plan nodes).
            Node::Expr(_) => {
                panic!("Node::plan_head: called on an expression node, which has no Plan base")
            }
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
            Node::ModifyTable(m) => Ok(Node::ModifyTable(m.clone_in(mcx)?)),
            Node::Material(m) => Ok(Node::Material(m.clone_in(mcx)?)),
            Node::Gather(g) => Ok(Node::Gather(g.clone_in(mcx)?)),
            Node::GatherMerge(g) => Ok(Node::GatherMerge(g.clone_in(mcx)?)),
            Node::MergeAppend(m) => Ok(Node::MergeAppend(m.clone_in(mcx)?)),
            Node::BitmapAnd(b) => Ok(Node::BitmapAnd(b.clone_in(mcx)?)),
            Node::MergeJoin(m) => Ok(Node::MergeJoin(m.clone_in(mcx)?)),
            Node::RecursiveUnion(r) => Ok(Node::RecursiveUnion(r.clone_in(mcx)?)),
            Node::Group(g) => Ok(Node::Group(g.clone_in(mcx)?)),
            Node::ProjectSet(p) => Ok(Node::ProjectSet(p.clone_in(mcx)?)),
            Node::Result(r) => Ok(Node::Result(r.clone_in(mcx)?)),
            Node::SetOp(s) => Ok(Node::SetOp(s.clone_in(mcx)?)),
            Node::Memoize(m) => Ok(Node::Memoize(m.clone_in(mcx)?)),
            Node::IndexScan(m) => Ok(Node::IndexScan(m.clone_in(mcx)?)),
            Node::IndexOnlyScan(m) => Ok(Node::IndexOnlyScan(m.clone_in(mcx)?)),
            Node::BitmapIndexScan(m) => Ok(Node::BitmapIndexScan(m.clone_in(mcx)?)),
            Node::Limit(m) => Ok(Node::Limit(m.clone_in(mcx)?)),
            Node::Unique(u) => Ok(Node::Unique(u.clone_in(mcx)?)),
            Node::Sort(s) => Ok(Node::Sort(s.clone_in(mcx)?)),
            Node::TableFuncScan(t) => Ok(Node::TableFuncScan(t.clone_in(mcx)?)),
            Node::ValuesScan(v) => Ok(Node::ValuesScan(v.clone_in(mcx)?)),
            Node::CteScan(c) => Ok(Node::CteScan(c.clone_in(mcx)?)),
            Node::NamedTuplestoreScan(n) => Ok(Node::NamedTuplestoreScan(n.clone_in(mcx)?)),
            Node::NestLoop(m) => Ok(Node::NestLoop(m.clone_in(mcx)?)),
            Node::HashJoin(h) => Ok(Node::HashJoin(h.clone_in(mcx)?)),
            Node::Hash(h) => Ok(Node::Hash(h.clone_in(mcx)?)),
            Node::TidRangeScan(t) => Ok(Node::TidRangeScan(t.clone_in(mcx)?)),
            Node::SeqScan(s) => Ok(Node::SeqScan(s.clone_in(mcx)?)),
            Node::SubqueryScan(s) => Ok(Node::SubqueryScan(s.clone_in(mcx)?)),
            Node::ForeignScan(f) => Ok(Node::ForeignScan(f.clone_in(mcx)?)),
            Node::CustomScan(c) => Ok(Node::CustomScan(c.clone_in(mcx)?)),
            // The `Expr` subtree is lifetime-free (owned `Box`/`Vec`), so a
            // plain clone reproduces it; `copyObject` over an expression node.
            Node::Expr(e) => Ok(Node::Expr(e.clone())),
        }
    }
}

// `T_*` tags for the expression nodes reachable through `Node::Expr`
// (nodes/nodetags.h, PostgreSQL 18.3 generated order). Defined here, where the
// `Node::tag()` arm reads them. The `Expr`-deriving node types occupy the
// contiguous run `T_Var`(6)..`T_ReturningExpr`(61) of the generated enum;
// values verified against `nodes/nodetags.h`.
const T_Var: NodeTag = NodeTag(6);
const T_Const: NodeTag = NodeTag(7);
const T_Aggref: NodeTag = NodeTag(9);
const T_GroupingFunc: NodeTag = NodeTag(10);
const T_WindowFunc: NodeTag = NodeTag(11);
const T_MergeSupportFunc: NodeTag = NodeTag(13);
const T_SubscriptingRef: NodeTag = NodeTag(14);
const T_FuncExpr: NodeTag = NodeTag(15);
const T_NamedArgExpr: NodeTag = NodeTag(16);
const T_OpExpr: NodeTag = NodeTag(17);
const T_DistinctExpr: NodeTag = NodeTag(18);
const T_NullIfExpr: NodeTag = NodeTag(19);
const T_ScalarArrayOpExpr: NodeTag = NodeTag(20);
const T_BoolExpr: NodeTag = NodeTag(21);
const T_SubLink: NodeTag = NodeTag(22);
const T_SubPlan: NodeTag = NodeTag(23);
const T_AlternativeSubPlan: NodeTag = NodeTag(24);
const T_FieldSelect: NodeTag = NodeTag(25);
const T_FieldStore: NodeTag = NodeTag(26);
const T_RelabelType: NodeTag = NodeTag(27);
const T_CoerceViaIO: NodeTag = NodeTag(28);
const T_ArrayCoerceExpr: NodeTag = NodeTag(29);
const T_ConvertRowtypeExpr: NodeTag = NodeTag(30);
const T_CollateExpr: NodeTag = NodeTag(31);
const T_CaseExpr: NodeTag = NodeTag(32);
const T_CaseTestExpr: NodeTag = NodeTag(34);
const T_ArrayExpr: NodeTag = NodeTag(35);
const T_RowExpr: NodeTag = NodeTag(36);
const T_RowCompareExpr: NodeTag = NodeTag(37);
const T_CoalesceExpr: NodeTag = NodeTag(38);
const T_MinMaxExpr: NodeTag = NodeTag(39);
const T_SQLValueFunction: NodeTag = NodeTag(40);
const T_XmlExpr: NodeTag = NodeTag(41);
const T_JsonValueExpr: NodeTag = NodeTag(44);
const T_JsonConstructorExpr: NodeTag = NodeTag(45);
const T_JsonIsPredicate: NodeTag = NodeTag(46);
const T_JsonExpr: NodeTag = NodeTag(48);
const T_NullTest: NodeTag = NodeTag(52);
const T_BooleanTest: NodeTag = NodeTag(53);
const T_CoerceToDomain: NodeTag = NodeTag(55);
const T_CoerceToDomainValue: NodeTag = NodeTag(56);
const T_SetToDefault: NodeTag = NodeTag(57);
const T_CurrentOfExpr: NodeTag = NodeTag(58);
const T_NextValueExpr: NodeTag = NodeTag(59);
const T_InferenceElem: NodeTag = NodeTag(60);
const T_ReturningExpr: NodeTag = NodeTag(61);

/// `nodeTag((Node *) expr)` for an embedded expression node — the C tag of the
/// concrete `Expr` variant. Every `Expr`-deriving node type has its generated
/// `T_*` value (`nodes/nodetags.h`); the match is exhaustive within this crate
/// (the `#[non_exhaustive]` attribute only binds external matches), so a new
/// variant must add its arm here.
fn expr_tag(e: &crate::primnodes::Expr) -> NodeTag {
    use crate::primnodes::Expr;
    match e {
        Expr::Var(_) => T_Var,
        Expr::Const(_) => T_Const,
        Expr::Param(_) => crate::params::T_Param,
        Expr::Aggref(_) => T_Aggref,
        Expr::GroupingFunc(_) => T_GroupingFunc,
        Expr::WindowFunc(_) => T_WindowFunc,
        Expr::SubscriptingRef(_) => T_SubscriptingRef,
        Expr::FuncExpr(_) => T_FuncExpr,
        Expr::NamedArgExpr(_) => T_NamedArgExpr,
        Expr::OpExpr(_) => T_OpExpr,
        Expr::DistinctExpr(_) => T_DistinctExpr,
        Expr::NullIfExpr(_) => T_NullIfExpr,
        Expr::ScalarArrayOpExpr(_) => T_ScalarArrayOpExpr,
        Expr::BoolExpr(_) => T_BoolExpr,
        Expr::SubLink(_) => T_SubLink,
        Expr::SubPlan(_) => T_SubPlan,
        Expr::AlternativeSubPlan(_) => T_AlternativeSubPlan,
        Expr::FieldSelect(_) => T_FieldSelect,
        Expr::FieldStore(_) => T_FieldStore,
        Expr::RelabelType(_) => T_RelabelType,
        Expr::CoerceViaIO(_) => T_CoerceViaIO,
        Expr::ArrayCoerceExpr(_) => T_ArrayCoerceExpr,
        Expr::ConvertRowtypeExpr(_) => T_ConvertRowtypeExpr,
        Expr::CollateExpr(_) => T_CollateExpr,
        Expr::CaseExpr(_) => T_CaseExpr,
        Expr::CaseTestExpr(_) => T_CaseTestExpr,
        Expr::ArrayExpr(_) => T_ArrayExpr,
        Expr::RowExpr(_) => T_RowExpr,
        Expr::RowCompareExpr(_) => T_RowCompareExpr,
        Expr::CoalesceExpr(_) => T_CoalesceExpr,
        Expr::MinMaxExpr(_) => T_MinMaxExpr,
        Expr::SQLValueFunction(_) => T_SQLValueFunction,
        Expr::XmlExpr(_) => T_XmlExpr,
        Expr::JsonValueExpr(_) => T_JsonValueExpr,
        Expr::JsonConstructorExpr(_) => T_JsonConstructorExpr,
        Expr::JsonIsPredicate(_) => T_JsonIsPredicate,
        Expr::JsonExpr(_) => T_JsonExpr,
        Expr::NullTest(_) => T_NullTest,
        Expr::BooleanTest(_) => T_BooleanTest,
        Expr::MergeSupportFunc(_) => T_MergeSupportFunc,
        Expr::CoerceToDomain(_) => T_CoerceToDomain,
        Expr::CoerceToDomainValue(_) => T_CoerceToDomainValue,
        Expr::SetToDefault(_) => T_SetToDefault,
        Expr::CurrentOfExpr(_) => T_CurrentOfExpr,
        Expr::NextValueExpr(_) => T_NextValueExpr,
        Expr::InferenceElem(_) => T_InferenceElem,
        Expr::ReturningExpr(_) => T_ReturningExpr,
    }
}

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;
