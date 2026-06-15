//! The central plan-node dispatch enum (nodes/nodes.h `Node` over plan nodes),
//! trimmed.
//!
//! C's `Plan *` is a tagged pointer to any concrete plan node; the owned model
//! is this enum. Variants are added as the nodes' executor units are ported.

use mcx::{Mcx, PgBox, PgVec};
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
pub const T_LockRows: NodeTag = NodeTag(372);
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

// Parse-tree node tags (nodes/nodetags.h, PostgreSQL 18.3 generated order),
// for the K1-parsetree `Node` variants.
pub const T_Alias: NodeTag = NodeTag(2);
pub const T_RangeVar: NodeTag = NodeTag(3);
pub const T_TargetEntry: NodeTag = NodeTag(62);
pub const T_RangeTblRef: NodeTag = NodeTag(63);
pub const T_JoinExpr: NodeTag = NodeTag(64);
pub const T_FromExpr: NodeTag = NodeTag(65);
pub const T_OnConflictExpr: NodeTag = NodeTag(66);
pub const T_Query: NodeTag = NodeTag(67);
pub const T_TypeName: NodeTag = NodeTag(68);
pub const T_ColumnRef: NodeTag = NodeTag(69);
pub const T_ParamRef: NodeTag = NodeTag(70);
pub const T_A_Expr: NodeTag = NodeTag(71);
pub const T_A_Const: NodeTag = NodeTag(72);
pub const T_TypeCast: NodeTag = NodeTag(73);
pub const T_CollateClause: NodeTag = NodeTag(74);
pub const T_FuncCall: NodeTag = NodeTag(76);
pub const T_A_Star: NodeTag = NodeTag(77);
pub const T_A_Indices: NodeTag = NodeTag(78);
pub const T_A_Indirection: NodeTag = NodeTag(79);
pub const T_A_ArrayExpr: NodeTag = NodeTag(80);
pub const T_ResTarget: NodeTag = NodeTag(81);
pub const T_MultiAssignRef: NodeTag = NodeTag(82);
pub const T_SortBy: NodeTag = NodeTag(83);
pub const T_WindowDef: NodeTag = NodeTag(84);
pub const T_RangeSubselect: NodeTag = NodeTag(85);
pub const T_RangeFunction: NodeTag = NodeTag(86);
pub const T_RangeTableSample: NodeTag = NodeTag(89);
pub const T_ColumnDef: NodeTag = NodeTag(90);
pub const T_RangeTblEntry: NodeTag = NodeTag(101);
pub const T_RTEPermissionInfo: NodeTag = NodeTag(102);
pub const T_RangeTblFunction: NodeTag = NodeTag(103);
pub const T_WithCheckOption: NodeTag = NodeTag(105);
pub const T_SortGroupClause: NodeTag = NodeTag(106);
pub const T_GroupingSet: NodeTag = NodeTag(107);
pub const T_WindowClause: NodeTag = NodeTag(108);
pub const T_RowMarkClause: NodeTag = NodeTag(109);
pub const T_WithClause: NodeTag = NodeTag(110);
pub const T_InferClause: NodeTag = NodeTag(111);
pub const T_OnConflictClause: NodeTag = NodeTag(112);
pub const T_CommonTableExpr: NodeTag = NodeTag(115);
pub const T_MergeWhenClause: NodeTag = NodeTag(116);
pub const T_ReturningClause: NodeTag = NodeTag(118);
pub const T_InsertStmt: NodeTag = NodeTag(137);
pub const T_DeleteStmt: NodeTag = NodeTag(138);
pub const T_UpdateStmt: NodeTag = NodeTag(139);
pub const T_MergeStmt: NodeTag = NodeTag(140);
pub const T_SelectStmt: NodeTag = NodeTag(141);
pub const T_SetOperationStmt: NodeTag = NodeTag(142);
// Value nodes (nodes/value.h) — tags verified vs nodes/nodetags.h.
pub const T_Integer: NodeTag = NodeTag(465);
pub const T_Float: NodeTag = NodeTag(466);
pub const T_Boolean: NodeTag = NodeTag(467);
pub const T_String: NodeTag = NodeTag(468);
pub const T_BitString: NodeTag = NodeTag(469);

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
    /// `T_WindowAgg`.
    WindowAgg(crate::nodewindowagg::WindowAgg<'mcx>),
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
    // --- Parse-tree statement / producer / raw-grammar nodes (K1-parsetree) ---
    // Additive arms making the parse vocabulary participate in the `Node`
    // machinery (`tag`/`clone_in`). `plan_head` panics for these (they have no
    // embedded `Plan`), exactly as it does for `Expr`.
    /// `T_Query` — a parse/analyze/rewrite query tree.
    Query(crate::copy_query::Query<'mcx>),
    /// `T_RangeTblEntry`.
    RangeTblEntry(crate::parsenodes::RangeTblEntry<'mcx>),
    /// `T_RTEPermissionInfo`.
    RTEPermissionInfo(crate::parsenodes::RTEPermissionInfo<'mcx>),
    /// `T_RangeTblFunction`.
    RangeTblFunction(crate::rawnodes::RangeTblFunction<'mcx>),
    /// `T_TargetEntry`.
    TargetEntry(crate::primnodes::TargetEntry<'mcx>),
    /// `T_RangeTblRef`.
    RangeTblRef(crate::rawnodes::RangeTblRef),
    /// `T_FromExpr`.
    FromExpr(crate::rawnodes::FromExpr<'mcx>),
    /// `T_JoinExpr`.
    JoinExpr(crate::rawnodes::JoinExpr<'mcx>),
    /// `T_OnConflictExpr`.
    OnConflictExpr(crate::rawnodes::OnConflictExpr<'mcx>),
    /// `T_MergeAction` — the parse-tree MergeAction (cf the executor's
    /// `MergeActionState`-paired one in `modifytable`).
    MergeAction(crate::rawnodes::MergeAction<'mcx>),
    /// `T_SortGroupClause`.
    SortGroupClause(crate::rawnodes::SortGroupClause),
    /// `T_GroupingSet`.
    GroupingSet(crate::rawnodes::GroupingSet<'mcx>),
    /// `T_WindowClause`.
    WindowClause(crate::rawnodes::WindowClause<'mcx>),
    /// `T_RowMarkClause`.
    RowMarkClause(crate::rawnodes::RowMarkClause),
    /// `T_WithCheckOption`.
    WithCheckOption(crate::rawnodes::WithCheckOption<'mcx>),
    /// `T_CommonTableExpr`.
    CommonTableExpr(crate::rawnodes::CommonTableExpr<'mcx>),
    /// `T_SetOperationStmt`.
    SetOperationStmt(crate::rawnodes::SetOperationStmt<'mcx>),
    /// `T_Alias`.
    Alias(crate::rawnodes::Alias<'mcx>),
    /// `T_RangeVar`.
    RangeVar(crate::rawnodes::RangeVar<'mcx>),
    /// `T_TypeName`.
    TypeName(crate::rawnodes::TypeName<'mcx>),
    /// `T_ColumnDef`.
    ColumnDef(crate::rawnodes::ColumnDef<'mcx>),
    // --- raw-grammar INPUT nodes ---
    /// `T_SelectStmt`.
    SelectStmt(crate::rawnodes::SelectStmt<'mcx>),
    /// `T_InsertStmt`.
    InsertStmt(crate::rawnodes::InsertStmt<'mcx>),
    /// `T_UpdateStmt`.
    UpdateStmt(crate::rawnodes::UpdateStmt<'mcx>),
    /// `T_DeleteStmt`.
    DeleteStmt(crate::rawnodes::DeleteStmt<'mcx>),
    /// `T_MergeStmt`.
    MergeStmt(crate::rawnodes::MergeStmt<'mcx>),
    /// `T_A_Expr`.
    A_Expr(crate::rawnodes::A_Expr<'mcx>),
    /// `T_ColumnRef`.
    ColumnRef(crate::rawnodes::ColumnRef<'mcx>),
    /// `T_ParamRef`.
    ParamRef(crate::rawnodes::ParamRef),
    /// `T_A_Const`.
    A_Const(crate::rawnodes::A_Const<'mcx>),
    /// `T_FuncCall`.
    FuncCall(crate::rawnodes::FuncCall<'mcx>),
    /// `T_A_Star`.
    A_Star(crate::rawnodes::A_Star),
    /// `T_A_Indices`.
    A_Indices(crate::rawnodes::A_Indices<'mcx>),
    /// `T_A_Indirection`.
    A_Indirection(crate::rawnodes::A_Indirection<'mcx>),
    /// `T_A_ArrayExpr`.
    A_ArrayExpr(crate::rawnodes::A_ArrayExpr<'mcx>),
    /// `T_ResTarget`.
    ResTarget(crate::rawnodes::ResTarget<'mcx>),
    /// `T_MultiAssignRef`.
    MultiAssignRef(crate::rawnodes::MultiAssignRef<'mcx>),
    /// `T_TypeCast`.
    TypeCast(crate::rawnodes::TypeCast<'mcx>),
    /// `T_CollateClause`.
    CollateClause(crate::rawnodes::CollateClause<'mcx>),
    /// `T_SortBy`.
    SortBy(crate::rawnodes::SortBy<'mcx>),
    /// `T_WindowDef`.
    WindowDef(crate::rawnodes::WindowDef<'mcx>),
    /// `T_RangeSubselect`.
    RangeSubselect(crate::rawnodes::RangeSubselect<'mcx>),
    /// `T_RangeFunction`.
    RangeFunction(crate::rawnodes::RangeFunction<'mcx>),
    /// `T_RangeTableSample`.
    RangeTableSample(crate::rawnodes::RangeTableSample<'mcx>),
    /// `T_TableSampleClause` — the post-analysis form stored in
    /// `RangeTblEntry.tablesample` (built by `transformRangeTableSample`).
    TableSampleClause(crate::nodesamplescan::TableSampleClause<'mcx>),
    /// `T_WithClause`.
    WithClause(crate::rawnodes::WithClause<'mcx>),
    /// `T_InferClause`.
    InferClause(crate::rawnodes::InferClause<'mcx>),
    /// `T_OnConflictClause`.
    OnConflictClause(crate::rawnodes::OnConflictClause<'mcx>),
    /// `T_MergeWhenClause`.
    MergeWhenClause(crate::rawnodes::MergeWhenClause<'mcx>),
    /// `T_ReturningClause`.
    ReturningClause(crate::rawnodes::ReturningClause<'mcx>),
    // --- raw-grammar `Expr`-deriving expression nodes (rawexprnodes) ---
    // In C, these `Expr`-deriving node types are the same struct in the raw
    // grammar output and the post-analysis tree, but the grammar fills their
    // `Node *`/`List *` children with RAW parse-tree nodes
    // (`ColumnRef`/`A_Expr`/…). The owned model's post-analysis
    // [`crate::primnodes::Expr`] enum carries `Expr` children, so it cannot hold
    // the raw children; the raw counterparts live in
    // [`crate::rawexprnodes`] and ride as their own `Node` arms (no conflation
    // with `Node::Expr`). `transformExpr` (analyze.c) turns these into
    // post-analysis `Expr`. Additive (`#[non_exhaustive]`); tags verified vs
    // nodes/nodetags.h.
    /// `T_BoolExpr` (raw).
    BoolExpr(crate::rawexprnodes::BoolExpr<'mcx>),
    /// `T_CaseExpr` (raw).
    CaseExpr(crate::rawexprnodes::CaseExpr<'mcx>),
    /// `T_CaseWhen` (raw).
    CaseWhen(crate::rawexprnodes::CaseWhen<'mcx>),
    /// `T_CoalesceExpr` (raw).
    CoalesceExpr(crate::rawexprnodes::CoalesceExpr<'mcx>),
    /// `T_MinMaxExpr` (raw).
    MinMaxExpr(crate::rawexprnodes::MinMaxExpr<'mcx>),
    /// `T_SubLink` (raw).
    SubLink(crate::rawexprnodes::SubLink<'mcx>),
    /// `T_NullTest` (raw).
    NullTest(crate::rawexprnodes::NullTest<'mcx>),
    /// `T_BooleanTest` (raw).
    BooleanTest(crate::rawexprnodes::BooleanTest<'mcx>),
    /// `T_RowExpr` (raw).
    RowExpr(crate::rawexprnodes::RowExpr<'mcx>),
    /// `T_GroupingFunc` (raw).
    GroupingFunc(crate::rawexprnodes::GroupingFunc<'mcx>),
    /// `T_CollateExpr` (raw).
    CollateExpr(crate::rawexprnodes::CollateExpr<'mcx>),
    /// `T_SetToDefault` (raw).
    SetToDefault(crate::rawexprnodes::SetToDefault),
    /// `T_CurrentOfExpr` (raw).
    CurrentOfExpr(crate::rawexprnodes::CurrentOfExpr<'mcx>),
    /// `T_NamedArgExpr` (raw).
    NamedArgExpr(crate::rawexprnodes::NamedArgExpr<'mcx>),
    /// `T_SQLValueFunction` (raw).
    SQLValueFunction(crate::rawexprnodes::SQLValueFunction),
    /// `T_XmlExpr` (raw).
    XmlExpr(crate::rawexprnodes::XmlExpr<'mcx>),
    // --- value nodes (nodes/value.h) — leaf literals ---
    // The lexer/grammar emits these `Value`-family leaves (`A_Const.val`,
    // operator-name `list_make1(makeString(name))`, etc.) as `Node *`, so they
    // must be arms of the central `Node` enum to flow through the generic
    // `Node *` machinery. Additive (`#[non_exhaustive]`); tags verified vs
    // nodes/nodetags.h (T_Integer=465 … T_BitString=469).
    /// `T_Integer`.
    Integer(crate::value::Integer),
    /// `T_Float`.
    Float(crate::value::Float<'mcx>),
    /// `T_Boolean`.
    Boolean(crate::value::Boolean),
    /// `T_String` (the `String` value node; the carrier is `StringNode` to avoid
    /// colliding with Rust's `String`).
    String(crate::value::StringNode<'mcx>),
    /// `T_BitString`.
    BitString(crate::value::BitString<'mcx>),
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
    /// `T_List` — a bare `List *` node carried as a `Node *`.
    ///
    /// PostgreSQL parse trees contain `List` nodes directly where a `Node *`
    /// slot holds a list of nodes — most visibly the rows of `VALUES`
    /// (`SelectStmt.valuesLists` is a `List` of `List *`, each sublist a `Node`)
    /// and a few other list-of-lists fields. A `List` is itself a `Node`
    /// (`nodeTag == T_List`), so it is an arm of the central `Node` enum,
    /// holding the sublist's elements. Additive (`#[non_exhaustive]`).
    List(PgVec<'mcx, NodePtr<'mcx>>),
}

impl<'mcx> Node<'mcx> {
    /// `nodeTag(node)` — alias of [`Node::tag`] matching the C `nodeTag` /
    /// src-idiomatic `node_tag` spelling the walkers and parser cluster use.
    #[inline]
    pub fn node_tag(&self) -> NodeTag {
        self.tag()
    }

    // --- accessor surface for the walkers / parser recursive cluster ---------
    //
    // The split Expr/Node model carries every `Expr`-derived node inside the
    // single `Node::Expr(Expr)` arm, so the `Expr`-leaf accessors (`as_var`,
    // `as_opexpr`, `as_collateexpr`) reach *through* that arm. The parse/raw
    // accessors (`as_targetentry`, `as_joinexpr`) match the dedicated `Node`
    // arms directly. `as_*`/`as_*_mut` borrow; `into_*` consumes; `is_*` tests.
    // (C: `IsA(node, T)` + the `castNode(T, node)` cast.)

    /// `castNode(Var, node)` (borrow) — `Some` iff `node` is a `Var` expression.
    pub fn as_var(&self) -> Option<&crate::primnodes::Var> {
        match self {
            Node::Expr(crate::primnodes::Expr::Var(v)) => Some(v),
            _ => None,
        }
    }
    /// `castNode(Var, node)` (mutable borrow).
    pub fn as_var_mut(&mut self) -> Option<&mut crate::primnodes::Var> {
        match self {
            Node::Expr(crate::primnodes::Expr::Var(v)) => Some(v),
            _ => None,
        }
    }
    /// `IsA(node, Var)`.
    pub fn is_var(&self) -> bool {
        matches!(self, Node::Expr(crate::primnodes::Expr::Var(_)))
    }

    /// `castNode(OpExpr, node)` (borrow).
    pub fn as_opexpr(&self) -> Option<&crate::primnodes::OpExpr> {
        match self {
            Node::Expr(crate::primnodes::Expr::OpExpr(o)) => Some(o),
            _ => None,
        }
    }
    /// `castNode(OpExpr, node)` (mutable borrow).
    pub fn as_opexpr_mut(&mut self) -> Option<&mut crate::primnodes::OpExpr> {
        match self {
            Node::Expr(crate::primnodes::Expr::OpExpr(o)) => Some(o),
            _ => None,
        }
    }
    /// `IsA(node, OpExpr)`.
    pub fn is_opexpr(&self) -> bool {
        matches!(self, Node::Expr(crate::primnodes::Expr::OpExpr(_)))
    }

    /// `castNode(CollateExpr, node)` (borrow).
    pub fn as_collateexpr(&self) -> Option<&crate::primnodes::CollateExpr> {
        match self {
            Node::Expr(crate::primnodes::Expr::CollateExpr(c)) => Some(c),
            _ => None,
        }
    }
    /// `castNode(CollateExpr, node)` (mutable borrow).
    pub fn as_collateexpr_mut(&mut self) -> Option<&mut crate::primnodes::CollateExpr> {
        match self {
            Node::Expr(crate::primnodes::Expr::CollateExpr(c)) => Some(c),
            _ => None,
        }
    }

    /// The wrapped [`crate::primnodes::Expr`], if this is an expression node.
    pub fn as_expr(&self) -> Option<&crate::primnodes::Expr> {
        match self {
            Node::Expr(e) => Some(e),
            _ => None,
        }
    }
    /// The wrapped [`crate::primnodes::Expr`] (mutable).
    pub fn as_expr_mut(&mut self) -> Option<&mut crate::primnodes::Expr> {
        match self {
            Node::Expr(e) => Some(e),
            _ => None,
        }
    }
    /// `IsA`-family test: is this an expression node?
    pub fn is_expr(&self) -> bool {
        matches!(self, Node::Expr(_))
    }
    /// Consume into the wrapped [`crate::primnodes::Expr`].
    pub fn into_expr(self) -> Option<crate::primnodes::Expr> {
        match self {
            Node::Expr(e) => Some(e),
            _ => None,
        }
    }

    /// `castNode(TargetEntry, node)` (borrow).
    pub fn as_targetentry(&self) -> Option<&crate::primnodes::TargetEntry<'mcx>> {
        match self {
            Node::TargetEntry(t) => Some(t),
            _ => None,
        }
    }
    /// `castNode(TargetEntry, node)` (mutable borrow).
    pub fn as_targetentry_mut(&mut self) -> Option<&mut crate::primnodes::TargetEntry<'mcx>> {
        match self {
            Node::TargetEntry(t) => Some(t),
            _ => None,
        }
    }
    /// Consume into the `TargetEntry` (the unwrap leg of a wrap-walk).
    pub fn into_targetentry(self) -> Option<crate::primnodes::TargetEntry<'mcx>> {
        match self {
            Node::TargetEntry(t) => Some(t),
            _ => None,
        }
    }

    /// `castNode(JoinExpr, node)` (borrow).
    pub fn as_joinexpr(&self) -> Option<&crate::rawnodes::JoinExpr<'mcx>> {
        match self {
            Node::JoinExpr(j) => Some(j),
            _ => None,
        }
    }
    /// `castNode(JoinExpr, node)` (mutable borrow).
    pub fn as_joinexpr_mut(&mut self) -> Option<&mut crate::rawnodes::JoinExpr<'mcx>> {
        match self {
            Node::JoinExpr(j) => Some(j),
            _ => None,
        }
    }

    /// `castNode(Query, node)` (borrow).
    pub fn as_query(&self) -> Option<&crate::copy_query::Query<'mcx>> {
        match self {
            Node::Query(q) => Some(q),
            _ => None,
        }
    }
    /// `castNode(Query, node)` (mutable borrow).
    pub fn as_query_mut(&mut self) -> Option<&mut crate::copy_query::Query<'mcx>> {
        match self {
            Node::Query(q) => Some(q),
            _ => None,
        }
    }

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
            Node::WindowAgg(_) => crate::nodewindowagg::T_WindowAgg,
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
            Node::Query(_) => T_Query,
            Node::RangeTblEntry(_) => T_RangeTblEntry,
            Node::RTEPermissionInfo(_) => T_RTEPermissionInfo,
            Node::RangeTblFunction(_) => T_RangeTblFunction,
            Node::TargetEntry(_) => T_TargetEntry,
            Node::RangeTblRef(_) => T_RangeTblRef,
            Node::FromExpr(_) => T_FromExpr,
            Node::JoinExpr(_) => T_JoinExpr,
            Node::OnConflictExpr(_) => T_OnConflictExpr,
            Node::MergeAction(_) => T_MergeAction,
            Node::SortGroupClause(_) => T_SortGroupClause,
            Node::GroupingSet(_) => T_GroupingSet,
            Node::WindowClause(_) => T_WindowClause,
            Node::RowMarkClause(_) => T_RowMarkClause,
            Node::WithCheckOption(_) => T_WithCheckOption,
            Node::CommonTableExpr(_) => T_CommonTableExpr,
            Node::SetOperationStmt(_) => T_SetOperationStmt,
            Node::Alias(_) => T_Alias,
            Node::RangeVar(_) => T_RangeVar,
            Node::TypeName(_) => T_TypeName,
            Node::ColumnDef(_) => T_ColumnDef,
            Node::SelectStmt(_) => T_SelectStmt,
            Node::InsertStmt(_) => T_InsertStmt,
            Node::UpdateStmt(_) => T_UpdateStmt,
            Node::DeleteStmt(_) => T_DeleteStmt,
            Node::MergeStmt(_) => T_MergeStmt,
            Node::A_Expr(_) => T_A_Expr,
            Node::ColumnRef(_) => T_ColumnRef,
            Node::ParamRef(_) => T_ParamRef,
            Node::A_Const(_) => T_A_Const,
            Node::FuncCall(_) => T_FuncCall,
            Node::A_Star(_) => T_A_Star,
            Node::A_Indices(_) => T_A_Indices,
            Node::A_Indirection(_) => T_A_Indirection,
            Node::A_ArrayExpr(_) => T_A_ArrayExpr,
            Node::ResTarget(_) => T_ResTarget,
            Node::MultiAssignRef(_) => T_MultiAssignRef,
            Node::TypeCast(_) => T_TypeCast,
            Node::CollateClause(_) => T_CollateClause,
            Node::SortBy(_) => T_SortBy,
            Node::WindowDef(_) => T_WindowDef,
            Node::RangeSubselect(_) => T_RangeSubselect,
            Node::RangeFunction(_) => T_RangeFunction,
            Node::RangeTableSample(_) => T_RangeTableSample,
            Node::TableSampleClause(_) => crate::nodesamplescan::T_TableSampleClause,
            Node::WithClause(_) => T_WithClause,
            Node::InferClause(_) => T_InferClause,
            Node::OnConflictClause(_) => T_OnConflictClause,
            Node::MergeWhenClause(_) => T_MergeWhenClause,
            Node::ReturningClause(_) => T_ReturningClause,
            // raw-grammar `Expr`-deriving nodes — share the C tag values of the
            // executor `Expr` run (nodes/nodetags.h); the same tag is correct
            // pre- and post-analysis.
            Node::BoolExpr(_) => T_BoolExpr,
            Node::CaseExpr(_) => T_CaseExpr,
            Node::CaseWhen(_) => T_CaseWhen,
            Node::CoalesceExpr(_) => T_CoalesceExpr,
            Node::MinMaxExpr(_) => T_MinMaxExpr,
            Node::SubLink(_) => T_SubLink,
            Node::NullTest(_) => T_NullTest,
            Node::BooleanTest(_) => T_BooleanTest,
            Node::RowExpr(_) => T_RowExpr,
            Node::GroupingFunc(_) => T_GroupingFunc,
            Node::CollateExpr(_) => T_CollateExpr,
            Node::SetToDefault(_) => T_SetToDefault,
            Node::CurrentOfExpr(_) => T_CurrentOfExpr,
            Node::NamedArgExpr(_) => T_NamedArgExpr,
            Node::SQLValueFunction(_) => T_SQLValueFunction,
            Node::XmlExpr(_) => T_XmlExpr,
            Node::Integer(_) => T_Integer,
            Node::Float(_) => T_Float,
            Node::Boolean(_) => T_Boolean,
            Node::String(_) => T_String,
            Node::BitString(_) => T_BitString,
            Node::Expr(e) => expr_tag(e),
            Node::List(_) => T_List,
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
            Node::WindowAgg(w) => &w.plan,
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
            // Parse-tree statement / producer / raw-grammar nodes likewise have
            // no embedded `Plan` (`plan_head` is only ever called on plan nodes;
            // C's `((Plan *) <parsenode>)` is a type error). Faithful mirror of
            // the `Expr` arm above — a misuse panic, not a stub.
            _ => panic!(
                "Node::plan_head: called on a parse-tree node ({}), which has no Plan base",
                self.tag()
            ),
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
            Node::WindowAgg(w) => Ok(Node::WindowAgg(w.clone_in(mcx)?)),
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
            // Parse-tree statement / producer / raw-grammar nodes — real
            // per-struct `copyObject` (each `clone_in` re-homes its subtree).
            Node::Query(q) => Ok(Node::Query(q.clone_in(mcx)?)),
            Node::RangeTblEntry(r) => Ok(Node::RangeTblEntry(r.clone_in(mcx)?)),
            Node::RTEPermissionInfo(r) => Ok(Node::RTEPermissionInfo(r.clone_in(mcx)?)),
            Node::RangeTblFunction(r) => Ok(Node::RangeTblFunction(r.clone_in(mcx)?)),
            Node::TargetEntry(t) => Ok(Node::TargetEntry(t.clone_in(mcx)?)),
            Node::RangeTblRef(r) => Ok(Node::RangeTblRef(r.clone_in(mcx)?)),
            Node::FromExpr(f) => Ok(Node::FromExpr(f.clone_in(mcx)?)),
            Node::JoinExpr(j) => Ok(Node::JoinExpr(j.clone_in(mcx)?)),
            Node::OnConflictExpr(o) => Ok(Node::OnConflictExpr(o.clone_in(mcx)?)),
            Node::MergeAction(m) => Ok(Node::MergeAction(m.clone_in(mcx)?)),
            Node::SortGroupClause(s) => Ok(Node::SortGroupClause(s.clone_in(mcx)?)),
            Node::GroupingSet(g) => Ok(Node::GroupingSet(g.clone_in(mcx)?)),
            Node::WindowClause(w) => Ok(Node::WindowClause(w.clone_in(mcx)?)),
            Node::RowMarkClause(r) => Ok(Node::RowMarkClause(r.clone_in(mcx)?)),
            Node::WithCheckOption(w) => Ok(Node::WithCheckOption(w.clone_in(mcx)?)),
            Node::CommonTableExpr(c) => Ok(Node::CommonTableExpr(c.clone_in(mcx)?)),
            Node::SetOperationStmt(s) => Ok(Node::SetOperationStmt(s.clone_in(mcx)?)),
            Node::Alias(a) => Ok(Node::Alias(a.clone_in(mcx)?)),
            Node::RangeVar(r) => Ok(Node::RangeVar(r.clone_in(mcx)?)),
            Node::TypeName(t) => Ok(Node::TypeName(t.clone_in(mcx)?)),
            Node::ColumnDef(c) => Ok(Node::ColumnDef(c.clone_in(mcx)?)),
            Node::SelectStmt(s) => Ok(Node::SelectStmt(s.clone_in(mcx)?)),
            Node::InsertStmt(i) => Ok(Node::InsertStmt(i.clone_in(mcx)?)),
            Node::UpdateStmt(u) => Ok(Node::UpdateStmt(u.clone_in(mcx)?)),
            Node::DeleteStmt(d) => Ok(Node::DeleteStmt(d.clone_in(mcx)?)),
            Node::MergeStmt(m) => Ok(Node::MergeStmt(m.clone_in(mcx)?)),
            Node::A_Expr(a) => Ok(Node::A_Expr(a.clone_in(mcx)?)),
            Node::ColumnRef(c) => Ok(Node::ColumnRef(c.clone_in(mcx)?)),
            Node::ParamRef(p) => Ok(Node::ParamRef(p.clone_in(mcx)?)),
            Node::A_Const(a) => Ok(Node::A_Const(a.clone_in(mcx)?)),
            Node::FuncCall(f) => Ok(Node::FuncCall(f.clone_in(mcx)?)),
            Node::A_Star(a) => Ok(Node::A_Star(a.clone_in(mcx)?)),
            Node::A_Indices(a) => Ok(Node::A_Indices(a.clone_in(mcx)?)),
            Node::A_Indirection(a) => Ok(Node::A_Indirection(a.clone_in(mcx)?)),
            Node::A_ArrayExpr(a) => Ok(Node::A_ArrayExpr(a.clone_in(mcx)?)),
            Node::ResTarget(r) => Ok(Node::ResTarget(r.clone_in(mcx)?)),
            Node::MultiAssignRef(m) => Ok(Node::MultiAssignRef(m.clone_in(mcx)?)),
            Node::TypeCast(t) => Ok(Node::TypeCast(t.clone_in(mcx)?)),
            Node::CollateClause(c) => Ok(Node::CollateClause(c.clone_in(mcx)?)),
            Node::SortBy(s) => Ok(Node::SortBy(s.clone_in(mcx)?)),
            Node::WindowDef(w) => Ok(Node::WindowDef(w.clone_in(mcx)?)),
            Node::RangeSubselect(r) => Ok(Node::RangeSubselect(r.clone_in(mcx)?)),
            Node::RangeFunction(r) => Ok(Node::RangeFunction(r.clone_in(mcx)?)),
            Node::RangeTableSample(r) => Ok(Node::RangeTableSample(r.clone_in(mcx)?)),
            Node::TableSampleClause(t) => Ok(Node::TableSampleClause(t.clone_in(mcx)?)),
            Node::WithClause(w) => Ok(Node::WithClause(w.clone_in(mcx)?)),
            Node::InferClause(i) => Ok(Node::InferClause(i.clone_in(mcx)?)),
            Node::OnConflictClause(o) => Ok(Node::OnConflictClause(o.clone_in(mcx)?)),
            Node::MergeWhenClause(m) => Ok(Node::MergeWhenClause(m.clone_in(mcx)?)),
            Node::ReturningClause(r) => Ok(Node::ReturningClause(r.clone_in(mcx)?)),
            // raw-grammar `Expr`-deriving nodes — real per-struct `copyObject`.
            Node::BoolExpr(b) => Ok(Node::BoolExpr(b.clone_in(mcx)?)),
            Node::CaseExpr(c) => Ok(Node::CaseExpr(c.clone_in(mcx)?)),
            Node::CaseWhen(c) => Ok(Node::CaseWhen(c.clone_in(mcx)?)),
            Node::CoalesceExpr(c) => Ok(Node::CoalesceExpr(c.clone_in(mcx)?)),
            Node::MinMaxExpr(m) => Ok(Node::MinMaxExpr(m.clone_in(mcx)?)),
            Node::SubLink(s) => Ok(Node::SubLink(s.clone_in(mcx)?)),
            Node::NullTest(n) => Ok(Node::NullTest(n.clone_in(mcx)?)),
            Node::BooleanTest(b) => Ok(Node::BooleanTest(b.clone_in(mcx)?)),
            Node::RowExpr(r) => Ok(Node::RowExpr(r.clone_in(mcx)?)),
            Node::GroupingFunc(g) => Ok(Node::GroupingFunc(g.clone_in(mcx)?)),
            Node::CollateExpr(c) => Ok(Node::CollateExpr(c.clone_in(mcx)?)),
            Node::SetToDefault(s) => Ok(Node::SetToDefault(s.clone_in(mcx)?)),
            Node::CurrentOfExpr(c) => Ok(Node::CurrentOfExpr(c.clone_in(mcx)?)),
            Node::NamedArgExpr(n) => Ok(Node::NamedArgExpr(n.clone_in(mcx)?)),
            Node::SQLValueFunction(s) => Ok(Node::SQLValueFunction(s.clone_in(mcx)?)),
            Node::XmlExpr(x) => Ok(Node::XmlExpr(x.clone_in(mcx)?)),
            // Value nodes (nodes/value.h) — real per-struct `copyObject`.
            Node::Integer(i) => Ok(Node::Integer(i.clone_in(mcx)?)),
            Node::Float(f) => Ok(Node::Float(f.clone_in(mcx)?)),
            Node::Boolean(b) => Ok(Node::Boolean(b.clone_in(mcx)?)),
            Node::String(s) => Ok(Node::String(s.clone_in(mcx)?)),
            Node::BitString(b) => Ok(Node::BitString(b.clone_in(mcx)?)),
            // The `Expr` subtree is lifetime-free (owned `Box`/`Vec`), so a
            // plain clone reproduces it; `copyObject` over an expression node.
            Node::Expr(e) => Ok(Node::Expr(e.clone())),
            Node::List(l) => {
                let mut out: PgVec<'b, NodePtr<'b>> =
                    mcx::vec_with_capacity_in(mcx, l.len())?;
                for item in l.iter() {
                    let cloned = item.clone_in(mcx)?;
                    out.push(mcx::alloc_in(mcx, cloned)?);
                }
                Ok(Node::List(out))
            }
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
const T_CaseWhen: NodeTag = NodeTag(33);
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
// `T_PlaceHolderVar` is a planner node (nodes/pathnodes.h), outside the
// contiguous executor `Expr` run; value verified against `nodes/nodetags.h`.
const T_PlaceHolderVar: NodeTag = NodeTag(319);
// `T_RestrictInfo` is a planner node (nodes/pathnodes.h), outside the
// contiguous executor `Expr` run; value verified against `nodes/nodetags.h`.
const T_RestrictInfo: NodeTag = NodeTag(318);

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
        Expr::PlaceHolderVar(_) => T_PlaceHolderVar,
        Expr::RestrictInfo(_) => T_RestrictInfo,
    }
}

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;
