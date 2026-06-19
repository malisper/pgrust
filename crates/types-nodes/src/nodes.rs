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
pub const T_TableFunc: NodeTag = NodeTag(4);
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
// raw-grammar DDL "CREATE" family tags (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_IntoClause: NodeTag = NodeTag(5);
pub const T_RoleSpec: NodeTag = NodeTag(75);
pub const T_TableLikeClause: NodeTag = NodeTag(91);
pub const T_IndexElem: NodeTag = NodeTag(92);
pub const T_DefElem: NodeTag = NodeTag(93);
pub const T_PartitionElem: NodeTag = NodeTag(96);
pub const T_PartitionSpec: NodeTag = NodeTag(97);
pub const T_PartitionBoundSpec: NodeTag = NodeTag(98);
pub const T_PartitionRangeDatum: NodeTag = NodeTag(99);
pub const T_CreateSchemaStmt: NodeTag = NodeTag(145);
pub const T_ObjectWithArgs: NodeTag = NodeTag(153);
pub const T_AccessPriv: NodeTag = NodeTag(154);
pub const T_CreateStmt: NodeTag = NodeTag(160);
pub const T_Constraint: NodeTag = NodeTag(161);
pub const T_CreateTableSpaceStmt: NodeTag = NodeTag(162);
pub const T_CreateExtensionStmt: NodeTag = NodeTag(166);
pub const T_CreateAmStmt: NodeTag = NodeTag(180);
pub const T_CreateTrigStmt: NodeTag = NodeTag(181);
pub const T_CreatePLangStmt: NodeTag = NodeTag(184);
pub const T_CreateRoleStmt: NodeTag = NodeTag(185);
pub const T_CreateSeqStmt: NodeTag = NodeTag(189);
pub const T_DefineStmt: NodeTag = NodeTag(191);
pub const T_CreateDomainStmt: NodeTag = NodeTag(192);
pub const T_CreateOpClassStmt: NodeTag = NodeTag(193);
pub const T_CreateOpClassItem: NodeTag = NodeTag(194);
pub const T_CreateOpFamilyStmt: NodeTag = NodeTag(195);
pub const T_IndexStmt: NodeTag = NodeTag(204);
pub const T_CreateStatsStmt: NodeTag = NodeTag(205);
pub const T_StatsElem: NodeTag = NodeTag(206);
pub const T_CreateFunctionStmt: NodeTag = NodeTag(208);
pub const T_FunctionParameter: NodeTag = NodeTag(209);
pub const T_CompositeTypeStmt: NodeTag = NodeTag(226);
pub const T_CreateEnumStmt: NodeTag = NodeTag(227);
pub const T_CreateRangeStmt: NodeTag = NodeTag(228);
pub const T_ViewStmt: NodeTag = NodeTag(230);
pub const T_CreatedbStmt: NodeTag = NodeTag(232);
pub const T_CreateTableAsStmt: NodeTag = NodeTag(242);
pub const T_CreateConversionStmt: NodeTag = NodeTag(249);
pub const T_CreateCastStmt: NodeTag = NodeTag(250);
// raw-grammar DDL "ALTER/DROP" family tags (nodes/nodetags.h, PostgreSQL 18.3).
pub const T_PartitionCmd: NodeTag = NodeTag(100);
pub const T_AlterTableStmt: NodeTag = NodeTag(146);
pub const T_AlterTableCmd: NodeTag = NodeTag(147);
pub const T_ATAlterConstraint: NodeTag = NodeTag(148);
pub const T_ReplicaIdentityStmt: NodeTag = NodeTag(149);
pub const T_AlterCollationStmt: NodeTag = NodeTag(150);
pub const T_AlterDomainStmt: NodeTag = NodeTag(151);
pub const T_AlterDefaultPrivilegesStmt: NodeTag = NodeTag(156);
pub const T_AlterTableSpaceOptionsStmt: NodeTag = NodeTag(164);
pub const T_AlterTableMoveAllStmt: NodeTag = NodeTag(165);
pub const T_AlterExtensionStmt: NodeTag = NodeTag(167);
pub const T_AlterExtensionContentsStmt: NodeTag = NodeTag(168);
pub const T_AlterFdwStmt: NodeTag = NodeTag(170);
pub const T_AlterForeignServerStmt: NodeTag = NodeTag(172);
pub const T_AlterUserMappingStmt: NodeTag = NodeTag(175);
pub const T_AlterPolicyStmt: NodeTag = NodeTag(179);
pub const T_AlterRoleStmt: NodeTag = NodeTag(186);
pub const T_AlterRoleSetStmt: NodeTag = NodeTag(187);
pub const T_AlterSeqStmt: NodeTag = NodeTag(190);
pub const T_AlterOpFamilyStmt: NodeTag = NodeTag(196);
pub const T_DropStmt: NodeTag = NodeTag(197);
pub const T_AlterStatsStmt: NodeTag = NodeTag(207);
pub const T_AlterFunctionStmt: NodeTag = NodeTag(210);
pub const T_RenameStmt: NodeTag = NodeTag(215);
pub const T_AlterObjectDependsStmt: NodeTag = NodeTag(216);
pub const T_AlterObjectSchemaStmt: NodeTag = NodeTag(217);
pub const T_AlterOwnerStmt: NodeTag = NodeTag(218);
pub const T_AlterOperatorStmt: NodeTag = NodeTag(219);
pub const T_AlterTypeStmt: NodeTag = NodeTag(220);
pub const T_AlterEnumStmt: NodeTag = NodeTag(229);
pub const T_AlterDatabaseStmt: NodeTag = NodeTag(233);
pub const T_AlterDatabaseRefreshCollStmt: NodeTag = NodeTag(234);
pub const T_AlterDatabaseSetStmt: NodeTag = NodeTag(235);
pub const T_DropOwnedStmt: NodeTag = NodeTag(255);
pub const T_ReassignOwnedStmt: NodeTag = NodeTag(256);
pub const T_AlterTSDictionaryStmt: NodeTag = NodeTag(257);
pub const T_AlterTSConfigurationStmt: NodeTag = NodeTag(258);
pub const T_AlterPublicationStmt: NodeTag = NodeTag(262);
pub const T_AlterSubscriptionStmt: NodeTag = NodeTag(264);
// raw-grammar utility / GRANT / transaction family (parser grammar F4).
pub const T_GrantStmt: NodeTag = NodeTag(152);
pub const T_GrantRoleStmt: NodeTag = NodeTag(155);
pub const T_CopyStmt: NodeTag = NodeTag(157);
pub const T_VariableSetStmt: NodeTag = NodeTag(158);
pub const T_VariableShowStmt: NodeTag = NodeTag(159);
pub const T_ImportForeignSchemaStmt: NodeTag = NodeTag(177);
pub const T_CreateFdwStmt: NodeTag = NodeTag(169);
pub const T_CreateForeignServerStmt: NodeTag = NodeTag(171);
pub const T_CreateForeignTableStmt: NodeTag = NodeTag(173);
pub const T_CreateUserMappingStmt: NodeTag = NodeTag(174);
pub const T_DropUserMappingStmt: NodeTag = NodeTag(176);
pub const T_CreatePolicyStmt: NodeTag = NodeTag(178);
pub const T_CreateEventTrigStmt: NodeTag = NodeTag(182);
pub const T_AlterEventTrigStmt: NodeTag = NodeTag(183);
pub const T_DropRoleStmt: NodeTag = NodeTag(188);
pub const T_DropTableSpaceStmt: NodeTag = NodeTag(163);
pub const T_TruncateStmt: NodeTag = NodeTag(198);
pub const T_CommentStmt: NodeTag = NodeTag(199);
pub const T_SecLabelStmt: NodeTag = NodeTag(200);
pub const T_DeclareCursorStmt: NodeTag = NodeTag(201);
pub const T_ClosePortalStmt: NodeTag = NodeTag(202);
pub const T_FetchStmt: NodeTag = NodeTag(203);
pub const T_DoStmt: NodeTag = NodeTag(211);
pub const T_CallStmt: NodeTag = NodeTag(213);
pub const T_RuleStmt: NodeTag = NodeTag(221);
pub const T_NotifyStmt: NodeTag = NodeTag(222);
pub const T_ListenStmt: NodeTag = NodeTag(223);
pub const T_UnlistenStmt: NodeTag = NodeTag(224);
pub const T_TransactionStmt: NodeTag = NodeTag(225);
pub const T_LoadStmt: NodeTag = NodeTag(231);
pub const T_DropdbStmt: NodeTag = NodeTag(236);
pub const T_AlterSystemStmt: NodeTag = NodeTag(237);
pub const T_ClusterStmt: NodeTag = NodeTag(238);
pub const T_VacuumStmt: NodeTag = NodeTag(239);
pub const T_VacuumRelation: NodeTag = NodeTag(240);
pub const T_ExplainStmt: NodeTag = NodeTag(241);
pub const T_RefreshMatViewStmt: NodeTag = NodeTag(243);
pub const T_CheckPointStmt: NodeTag = NodeTag(244);
pub const T_DiscardStmt: NodeTag = NodeTag(245);
pub const T_LockStmt: NodeTag = NodeTag(246);
pub const T_ConstraintsSetStmt: NodeTag = NodeTag(247);
pub const T_ReindexStmt: NodeTag = NodeTag(248);
pub const T_CreateTransformStmt: NodeTag = NodeTag(251);
pub const T_PrepareStmt: NodeTag = NodeTag(252);
pub const T_ExecuteStmt: NodeTag = NodeTag(253);
pub const T_DeallocateStmt: NodeTag = NodeTag(254);
pub const T_PublicationTable: NodeTag = NodeTag(259);
pub const T_PublicationObjSpec: NodeTag = NodeTag(260);
pub const T_CreatePublicationStmt: NodeTag = NodeTag(261);
pub const T_CreateSubscriptionStmt: NodeTag = NodeTag(263);
pub const T_DropSubscriptionStmt: NodeTag = NodeTag(265);
pub const T_ReturnStmt: NodeTag = NodeTag(143);
pub const T_PLAssignStmt: NodeTag = NodeTag(144);
pub const T_RangeTblEntry: NodeTag = NodeTag(101);
pub const T_RTEPermissionInfo: NodeTag = NodeTag(102);
pub const T_RangeTblFunction: NodeTag = NodeTag(103);
pub const T_WithCheckOption: NodeTag = NodeTag(105);
pub const T_SortGroupClause: NodeTag = NodeTag(106);
pub const T_GroupingSet: NodeTag = NodeTag(107);
pub const T_WindowClause: NodeTag = NodeTag(108);
pub const T_RowMarkClause: NodeTag = NodeTag(109);
pub const T_LockingClause: NodeTag = NodeTag(94);
pub const T_WithClause: NodeTag = NodeTag(110);
pub const T_InferClause: NodeTag = NodeTag(111);
pub const T_OnConflictClause: NodeTag = NodeTag(112);
pub const T_CTECycleClause: NodeTag = NodeTag(114);
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
    /// `T_BitmapOr`.
    BitmapOr(crate::nodebitmapor::BitmapOr<'mcx>),
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
    /// `T_BitmapHeapScan`.
    BitmapHeapScan(crate::nodebitmapheapscan::BitmapHeapScan<'mcx>),
    /// `T_Limit`.
    Limit(crate::nodelimit::Limit<'mcx>),
    /// `T_Unique`.
    Unique(crate::nodeunique::Unique<'mcx>),
    /// `T_Sort`.
    Sort(crate::nodesort::Sort<'mcx>),
    /// `T_IncrementalSort`.
    IncrementalSort(crate::nodeincrementalsort::IncrementalSort<'mcx>),
    /// `T_Agg`.
    Agg(crate::nodeagg::Agg<'mcx>),
    /// `T_WindowAgg`.
    WindowAgg(crate::nodewindowagg::WindowAgg<'mcx>),
    /// `T_TableFuncScan`.
    TableFuncScan(crate::nodetablefuncscan::TableFuncScan<'mcx>),
    /// `T_FunctionScan`.
    FunctionScan(crate::nodefunctionscan::FunctionScan<'mcx>),
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
    /// `T_SampleScan`.
    SampleScan(crate::nodesamplescan::SampleScan<'mcx>),
    /// `T_TidScan`.
    TidScan(crate::nodeindexscan::TidScan<'mcx>),
    /// `T_WorkTableScan`.
    WorkTableScan(crate::nodeworktablescan::WorkTableScan<'mcx>),
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
    /// `T_TableFunc` — an XMLTABLE/JSON_TABLE table function. In C this is a
    /// `Node *` carried by `RangeTblEntry.tablefunc` and walked by
    /// `expression_tree_walker`; a first-class `Node` arm so the walkers can
    /// dispatch it and the parser can store it in the RTE's `tablefunc` slot.
    TableFunc(crate::primnodes::TableFunc<'mcx>),
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
    /// `T_LockingClause`.
    LockingClause(crate::rawnodes::LockingClause<'mcx>),
    /// `T_WithCheckOption`.
    WithCheckOption(crate::rawnodes::WithCheckOption<'mcx>),
    /// `T_CTECycleClause` — the CYCLE clause of a recursive CTE. In C this is a
    /// `Node *` carried by `CommonTableExpr.cycle_clause` and walked by
    /// `expression_tree_walker` (into `cycle_mark_value`/`cycle_mark_default`);
    /// a first-class `Node` arm so the walkers can dispatch it.
    CTECycleClause(crate::rawnodes::CTECycleClause<'mcx>),
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
    /// `T_XmlSerialize` (raw).
    XmlSerialize(crate::rawexprnodes::XmlSerialize<'mcx>),
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
    /// `T_IntList` — a `List *` of bare integers carried as a `Node *`.
    ///
    /// In C an integer `List` (`nodeTag == T_IntList`) holds raw `int` cells
    /// built by `lappend_int`, not boxed `Integer` value-nodes. It is itself a
    /// `Node`, so it is an arm of the central `Node` enum. Used where a `Node *`
    /// slot holds a list of ints — most visibly the expanded `groupingSets`,
    /// where `expand_grouping_sets` stores a `List` of `T_IntList`s back into
    /// `Query.groupingSets`. Additive (`#[non_exhaustive]`).
    IntList(PgVec<'mcx, i32>),

    // --- raw-grammar DDL "CREATE" family nodes (crate::ddlnodes) ---
    RoleSpec(crate::ddlnodes::RoleSpec<'mcx>),
    DefElem(crate::ddlnodes::DefElem<'mcx>),
    Constraint(crate::ddlnodes::Constraint<'mcx>),
    TableLikeClause(crate::ddlnodes::TableLikeClause<'mcx>),
    IndexElem(crate::ddlnodes::IndexElem<'mcx>),
    FunctionParameter(crate::ddlnodes::FunctionParameter<'mcx>),
    ObjectWithArgs(crate::ddlnodes::ObjectWithArgs<'mcx>),
    AccessPriv(crate::ddlnodes::AccessPriv<'mcx>),
    CreateOpClassItem(crate::ddlnodes::CreateOpClassItem<'mcx>),
    StatsElem(crate::ddlnodes::StatsElem<'mcx>),
    PartitionElem(crate::ddlnodes::PartitionElem<'mcx>),
    PartitionSpec(crate::ddlnodes::PartitionSpec<'mcx>),
    PartitionBoundSpec(crate::ddlnodes::PartitionBoundSpec<'mcx>),
    PartitionRangeDatum(crate::ddlnodes::PartitionRangeDatum<'mcx>),
    IntoClause(crate::ddlnodes::IntoClause<'mcx>),
    CreateStmt(crate::ddlnodes::CreateStmt<'mcx>),
    IndexStmt(crate::ddlnodes::IndexStmt<'mcx>),
    CreateSeqStmt(crate::ddlnodes::CreateSeqStmt<'mcx>),
    CreateStatsStmt(crate::ddlnodes::CreateStatsStmt<'mcx>),
    CreateFunctionStmt(crate::ddlnodes::CreateFunctionStmt<'mcx>),
    DefineStmt(crate::ddlnodes::DefineStmt<'mcx>),
    CreateDomainStmt(crate::ddlnodes::CreateDomainStmt<'mcx>),
    CompositeTypeStmt(crate::ddlnodes::CompositeTypeStmt<'mcx>),
    CreateEnumStmt(crate::ddlnodes::CreateEnumStmt<'mcx>),
    CreateRangeStmt(crate::ddlnodes::CreateRangeStmt<'mcx>),
    ViewStmt(crate::ddlnodes::ViewStmt<'mcx>),
    CreateTableAsStmt(crate::ddlnodes::CreateTableAsStmt<'mcx>),
    CreateSchemaStmt(crate::ddlnodes::CreateSchemaStmt<'mcx>),
    CreateExtensionStmt(crate::ddlnodes::CreateExtensionStmt<'mcx>),
    CreateTrigStmt(crate::ddlnodes::CreateTrigStmt<'mcx>),
    CreateRoleStmt(crate::ddlnodes::CreateRoleStmt<'mcx>),
    CreatedbStmt(crate::ddlnodes::CreatedbStmt<'mcx>),
    CreateCastStmt(crate::ddlnodes::CreateCastStmt<'mcx>),
    CreateOpClassStmt(crate::ddlnodes::CreateOpClassStmt<'mcx>),
    CreateOpFamilyStmt(crate::ddlnodes::CreateOpFamilyStmt<'mcx>),
    CreatePLangStmt(crate::ddlnodes::CreatePLangStmt<'mcx>),
    CreateTableSpaceStmt(crate::ddlnodes::CreateTableSpaceStmt<'mcx>),
    CreateConversionStmt(crate::ddlnodes::CreateConversionStmt<'mcx>),
    CreateAmStmt(crate::ddlnodes::CreateAmStmt<'mcx>),

    // --- raw-grammar DDL "ALTER/DROP" family nodes (crate::ddlnodes) ---
    PartitionCmd(crate::ddlnodes::PartitionCmd<'mcx>),
    ReplicaIdentityStmt(crate::ddlnodes::ReplicaIdentityStmt<'mcx>),
    ATAlterConstraint(crate::ddlnodes::ATAlterConstraint<'mcx>),
    AlterTableStmt(crate::ddlnodes::AlterTableStmt<'mcx>),
    AlterTableCmd(crate::ddlnodes::AlterTableCmd<'mcx>),
    AlterCollationStmt(crate::ddlnodes::AlterCollationStmt<'mcx>),
    AlterDomainStmt(crate::ddlnodes::AlterDomainStmt<'mcx>),
    AlterEnumStmt(crate::ddlnodes::AlterEnumStmt<'mcx>),
    AlterStatsStmt(crate::ddlnodes::AlterStatsStmt<'mcx>),
    AlterSeqStmt(crate::ddlnodes::AlterSeqStmt<'mcx>),
    AlterOpFamilyStmt(crate::ddlnodes::AlterOpFamilyStmt<'mcx>),
    AlterFunctionStmt(crate::ddlnodes::AlterFunctionStmt<'mcx>),
    DropStmt(crate::ddlnodes::DropStmt<'mcx>),
    RenameStmt(crate::ddlnodes::RenameStmt<'mcx>),
    AlterObjectDependsStmt(crate::ddlnodes::AlterObjectDependsStmt<'mcx>),
    AlterObjectSchemaStmt(crate::ddlnodes::AlterObjectSchemaStmt<'mcx>),
    AlterOwnerStmt(crate::ddlnodes::AlterOwnerStmt<'mcx>),
    AlterOperatorStmt(crate::ddlnodes::AlterOperatorStmt<'mcx>),
    AlterTypeStmt(crate::ddlnodes::AlterTypeStmt<'mcx>),
    AlterDefaultPrivilegesStmt(crate::ddlnodes::AlterDefaultPrivilegesStmt<'mcx>),
    AlterRoleStmt(crate::ddlnodes::AlterRoleStmt<'mcx>),
    AlterRoleSetStmt(crate::ddlnodes::AlterRoleSetStmt<'mcx>),
    DropOwnedStmt(crate::ddlnodes::DropOwnedStmt<'mcx>),
    ReassignOwnedStmt(crate::ddlnodes::ReassignOwnedStmt<'mcx>),
    AlterTableSpaceOptionsStmt(crate::ddlnodes::AlterTableSpaceOptionsStmt<'mcx>),
    AlterTableMoveAllStmt(crate::ddlnodes::AlterTableMoveAllStmt<'mcx>),
    AlterExtensionStmt(crate::ddlnodes::AlterExtensionStmt<'mcx>),
    AlterExtensionContentsStmt(crate::ddlnodes::AlterExtensionContentsStmt<'mcx>),
    AlterFdwStmt(crate::ddlnodes::AlterFdwStmt<'mcx>),
    AlterForeignServerStmt(crate::ddlnodes::AlterForeignServerStmt<'mcx>),
    AlterUserMappingStmt(crate::ddlnodes::AlterUserMappingStmt<'mcx>),
    AlterPolicyStmt(crate::ddlnodes::AlterPolicyStmt<'mcx>),
    AlterDatabaseStmt(crate::ddlnodes::AlterDatabaseStmt<'mcx>),
    AlterDatabaseRefreshCollStmt(crate::ddlnodes::AlterDatabaseRefreshCollStmt<'mcx>),
    AlterDatabaseSetStmt(crate::ddlnodes::AlterDatabaseSetStmt<'mcx>),
    AlterTSDictionaryStmt(crate::ddlnodes::AlterTSDictionaryStmt<'mcx>),
    AlterTSConfigurationStmt(crate::ddlnodes::AlterTSConfigurationStmt<'mcx>),
    AlterPublicationStmt(crate::ddlnodes::AlterPublicationStmt<'mcx>),
    AlterSubscriptionStmt(crate::ddlnodes::AlterSubscriptionStmt<'mcx>),
    // raw-grammar utility / GRANT / transaction family (parser grammar F4).
    CheckPointStmt(crate::ddlnodes::CheckPointStmt),
    DiscardStmt(crate::ddlnodes::DiscardStmt),
    GrantStmt(crate::ddlnodes::GrantStmt<'mcx>),
    GrantRoleStmt(crate::ddlnodes::GrantRoleStmt<'mcx>),
    VariableSetStmt(crate::ddlnodes::VariableSetStmt<'mcx>),
    VariableShowStmt(crate::ddlnodes::VariableShowStmt<'mcx>),
    TransactionStmt(crate::ddlnodes::TransactionStmt<'mcx>),
    CopyStmt(crate::ddlnodes::CopyStmt<'mcx>),
    ExplainStmt(crate::ddlnodes::ExplainStmt<'mcx>),
    PrepareStmt(crate::ddlnodes::PrepareStmt<'mcx>),
    ExecuteStmt(crate::ddlnodes::ExecuteStmt<'mcx>),
    DeallocateStmt(crate::ddlnodes::DeallocateStmt<'mcx>),
    DeclareCursorStmt(crate::ddlnodes::DeclareCursorStmt<'mcx>),
    ClosePortalStmt(crate::ddlnodes::ClosePortalStmt<'mcx>),
    FetchStmt(crate::ddlnodes::FetchStmt<'mcx>),
    VacuumStmt(crate::ddlnodes::VacuumStmt<'mcx>),
    VacuumRelation(crate::ddlnodes::VacuumRelation<'mcx>),
    ClusterStmt(crate::ddlnodes::ClusterStmt<'mcx>),
    ReindexStmt(crate::ddlnodes::ReindexStmt<'mcx>),
    LockStmt(crate::ddlnodes::LockStmt<'mcx>),
    ConstraintsSetStmt(crate::ddlnodes::ConstraintsSetStmt<'mcx>),
    LoadStmt(crate::ddlnodes::LoadStmt<'mcx>),
    TruncateStmt(crate::ddlnodes::TruncateStmt<'mcx>),
    CommentStmt(crate::ddlnodes::CommentStmt<'mcx>),
    SecLabelStmt(crate::ddlnodes::SecLabelStmt<'mcx>),
    RuleStmt(crate::ddlnodes::RuleStmt<'mcx>),
    NotifyStmt(crate::ddlnodes::NotifyStmt<'mcx>),
    ListenStmt(crate::ddlnodes::ListenStmt<'mcx>),
    UnlistenStmt(crate::ddlnodes::UnlistenStmt<'mcx>),
    DoStmt(crate::ddlnodes::DoStmt<'mcx>),
    CallStmt(crate::ddlnodes::CallStmt<'mcx>),
    RefreshMatViewStmt(crate::ddlnodes::RefreshMatViewStmt<'mcx>),
    AlterSystemStmt(crate::ddlnodes::AlterSystemStmt<'mcx>),
    DropdbStmt(crate::ddlnodes::DropdbStmt<'mcx>),
    DropRoleStmt(crate::ddlnodes::DropRoleStmt<'mcx>),
    DropTableSpaceStmt(crate::ddlnodes::DropTableSpaceStmt<'mcx>),
    CreateFdwStmt(crate::ddlnodes::CreateFdwStmt<'mcx>),
    CreateForeignServerStmt(crate::ddlnodes::CreateForeignServerStmt<'mcx>),
    CreateForeignTableStmt(crate::ddlnodes::CreateForeignTableStmt<'mcx>),
    CreateUserMappingStmt(crate::ddlnodes::CreateUserMappingStmt<'mcx>),
    DropUserMappingStmt(crate::ddlnodes::DropUserMappingStmt<'mcx>),
    ImportForeignSchemaStmt(crate::ddlnodes::ImportForeignSchemaStmt<'mcx>),
    CreatePolicyStmt(crate::ddlnodes::CreatePolicyStmt<'mcx>),
    PublicationTable(crate::ddlnodes::PublicationTable<'mcx>),
    PublicationObjSpec(crate::ddlnodes::PublicationObjSpec<'mcx>),
    CreatePublicationStmt(crate::ddlnodes::CreatePublicationStmt<'mcx>),
    CreateSubscriptionStmt(crate::ddlnodes::CreateSubscriptionStmt<'mcx>),
    DropSubscriptionStmt(crate::ddlnodes::DropSubscriptionStmt<'mcx>),
    CreateEventTrigStmt(crate::ddlnodes::CreateEventTrigStmt<'mcx>),
    AlterEventTrigStmt(crate::ddlnodes::AlterEventTrigStmt<'mcx>),
    CreateTransformStmt(crate::ddlnodes::CreateTransformStmt<'mcx>),
    ReturnStmt(crate::ddlnodes::ReturnStmt<'mcx>),
    PLAssignStmt(crate::ddlnodes::PLAssignStmt<'mcx>),
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

    /// `castNode(TableFunc, node)` (borrow).
    pub fn as_table_func(&self) -> Option<&crate::primnodes::TableFunc<'mcx>> {
        match self {
            Node::TableFunc(t) => Some(t),
            _ => None,
        }
    }
    /// `castNode(TableFunc, node)` (mutable borrow).
    pub fn as_table_func_mut(&mut self) -> Option<&mut crate::primnodes::TableFunc<'mcx>> {
        match self {
            Node::TableFunc(t) => Some(t),
            _ => None,
        }
    }

    /// `castNode(CTECycleClause, node)` (borrow).
    pub fn as_cte_cycle_clause(&self) -> Option<&crate::rawnodes::CTECycleClause<'mcx>> {
        match self {
            Node::CTECycleClause(c) => Some(c),
            _ => None,
        }
    }
    /// `castNode(CTECycleClause, node)` (mutable borrow).
    pub fn as_cte_cycle_clause_mut(&mut self) -> Option<&mut crate::rawnodes::CTECycleClause<'mcx>> {
        match self {
            Node::CTECycleClause(c) => Some(c),
            _ => None,
        }
    }
    /// Consume into the `CTECycleClause`.
    pub fn into_cte_cycle_clause(self) -> Option<crate::rawnodes::CTECycleClause<'mcx>> {
        match self {
            Node::CTECycleClause(c) => Some(c),
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
            Node::BitmapOr(_) => crate::nodebitmapor::T_BitmapOr,
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
            Node::BitmapHeapScan(_) => crate::nodebitmapheapscan::T_BitmapHeapScan,
            Node::Limit(_) => T_Limit,
            Node::Unique(_) => crate::nodeunique::T_Unique,
            Node::Sort(_) => T_Sort,
            Node::IncrementalSort(_) => crate::nodeincrementalsort::T_IncrementalSort,
            Node::Agg(_) => crate::nodeagg::T_Agg,
            Node::WindowAgg(_) => crate::nodewindowagg::T_WindowAgg,
            Node::TableFuncScan(_) => T_TableFuncScan,
            Node::FunctionScan(_) => T_FunctionScan,
            Node::ValuesScan(_) => T_ValuesScan,
            Node::CteScan(_) => crate::nodectescan::T_CteScan,
            Node::NamedTuplestoreScan(_) => T_NamedTuplestoreScan,
            Node::NestLoop(_) => crate::nodenestloop::T_NestLoop,
            Node::HashJoin(_) => crate::nodehashjoin::T_HashJoin,
            Node::Hash(_) => crate::nodehashjoin::T_Hash,
            Node::TidRangeScan(_) => T_TidRangeScan,
            Node::SampleScan(_) => crate::nodesamplescan::T_SampleScan,
            Node::TidScan(_) => NodeTag(345),
            Node::WorkTableScan(_) => T_WorkTableScan,
            Node::SeqScan(_) => T_SeqScan,
            Node::SubqueryScan(_) => T_SubqueryScan,
            Node::ForeignScan(_) => T_ForeignScan,
            Node::CustomScan(_) => T_CustomScan,
            Node::Query(_) => T_Query,
            Node::RangeTblEntry(_) => T_RangeTblEntry,
            Node::RTEPermissionInfo(_) => T_RTEPermissionInfo,
            Node::RangeTblFunction(_) => T_RangeTblFunction,
            Node::TargetEntry(_) => T_TargetEntry,
            Node::TableFunc(_) => T_TableFunc,
            Node::RangeTblRef(_) => T_RangeTblRef,
            Node::FromExpr(_) => T_FromExpr,
            Node::JoinExpr(_) => T_JoinExpr,
            Node::OnConflictExpr(_) => T_OnConflictExpr,
            Node::MergeAction(_) => T_MergeAction,
            Node::SortGroupClause(_) => T_SortGroupClause,
            Node::GroupingSet(_) => T_GroupingSet,
            Node::WindowClause(_) => T_WindowClause,
            Node::RowMarkClause(_) => T_RowMarkClause,
            Node::LockingClause(_) => T_LockingClause,
            Node::WithCheckOption(_) => T_WithCheckOption,
            Node::CTECycleClause(_) => T_CTECycleClause,
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
            Node::XmlSerialize(_) => T_XmlSerialize,
            Node::Integer(_) => T_Integer,
            Node::Float(_) => T_Float,
            Node::Boolean(_) => T_Boolean,
            Node::String(_) => T_String,
            Node::BitString(_) => T_BitString,
            Node::Expr(e) => expr_tag(e),
            Node::List(_) => T_List,
            Node::IntList(_) => T_IntList,
            // raw-grammar DDL "CREATE" family nodes.
            Node::RoleSpec(_) => T_RoleSpec,
            Node::DefElem(_) => T_DefElem,
            Node::Constraint(_) => T_Constraint,
            Node::TableLikeClause(_) => T_TableLikeClause,
            Node::IndexElem(_) => T_IndexElem,
            Node::FunctionParameter(_) => T_FunctionParameter,
            Node::ObjectWithArgs(_) => T_ObjectWithArgs,
            Node::AccessPriv(_) => T_AccessPriv,
            Node::CreateOpClassItem(_) => T_CreateOpClassItem,
            Node::StatsElem(_) => T_StatsElem,
            Node::PartitionElem(_) => T_PartitionElem,
            Node::PartitionSpec(_) => T_PartitionSpec,
            Node::PartitionBoundSpec(_) => T_PartitionBoundSpec,
            Node::PartitionRangeDatum(_) => T_PartitionRangeDatum,
            Node::IntoClause(_) => T_IntoClause,
            Node::CreateStmt(_) => T_CreateStmt,
            Node::IndexStmt(_) => T_IndexStmt,
            Node::CreateSeqStmt(_) => T_CreateSeqStmt,
            Node::CreateStatsStmt(_) => T_CreateStatsStmt,
            Node::CreateFunctionStmt(_) => T_CreateFunctionStmt,
            Node::DefineStmt(_) => T_DefineStmt,
            Node::CreateDomainStmt(_) => T_CreateDomainStmt,
            Node::CompositeTypeStmt(_) => T_CompositeTypeStmt,
            Node::CreateEnumStmt(_) => T_CreateEnumStmt,
            Node::CreateRangeStmt(_) => T_CreateRangeStmt,
            Node::ViewStmt(_) => T_ViewStmt,
            Node::CreateTableAsStmt(_) => T_CreateTableAsStmt,
            Node::CreateSchemaStmt(_) => T_CreateSchemaStmt,
            Node::CreateExtensionStmt(_) => T_CreateExtensionStmt,
            Node::CreateTrigStmt(_) => T_CreateTrigStmt,
            Node::CreateRoleStmt(_) => T_CreateRoleStmt,
            Node::CreatedbStmt(_) => T_CreatedbStmt,
            Node::CreateCastStmt(_) => T_CreateCastStmt,
            Node::CreateOpClassStmt(_) => T_CreateOpClassStmt,
            Node::CreateOpFamilyStmt(_) => T_CreateOpFamilyStmt,
            Node::CreatePLangStmt(_) => T_CreatePLangStmt,
            Node::CreateTableSpaceStmt(_) => T_CreateTableSpaceStmt,
            Node::CreateConversionStmt(_) => T_CreateConversionStmt,
            Node::CreateAmStmt(_) => T_CreateAmStmt,
            // raw-grammar DDL "ALTER/DROP" family nodes.
            Node::PartitionCmd(_) => T_PartitionCmd,
            Node::ReplicaIdentityStmt(_) => T_ReplicaIdentityStmt,
            Node::ATAlterConstraint(_) => T_ATAlterConstraint,
            Node::AlterTableStmt(_) => T_AlterTableStmt,
            Node::AlterTableCmd(_) => T_AlterTableCmd,
            Node::AlterCollationStmt(_) => T_AlterCollationStmt,
            Node::AlterDomainStmt(_) => T_AlterDomainStmt,
            Node::AlterEnumStmt(_) => T_AlterEnumStmt,
            Node::AlterStatsStmt(_) => T_AlterStatsStmt,
            Node::AlterSeqStmt(_) => T_AlterSeqStmt,
            Node::AlterOpFamilyStmt(_) => T_AlterOpFamilyStmt,
            Node::AlterFunctionStmt(_) => T_AlterFunctionStmt,
            Node::DropStmt(_) => T_DropStmt,
            Node::RenameStmt(_) => T_RenameStmt,
            Node::AlterObjectDependsStmt(_) => T_AlterObjectDependsStmt,
            Node::AlterObjectSchemaStmt(_) => T_AlterObjectSchemaStmt,
            Node::AlterOwnerStmt(_) => T_AlterOwnerStmt,
            Node::AlterOperatorStmt(_) => T_AlterOperatorStmt,
            Node::AlterTypeStmt(_) => T_AlterTypeStmt,
            Node::AlterDefaultPrivilegesStmt(_) => T_AlterDefaultPrivilegesStmt,
            Node::AlterRoleStmt(_) => T_AlterRoleStmt,
            Node::AlterRoleSetStmt(_) => T_AlterRoleSetStmt,
            Node::DropOwnedStmt(_) => T_DropOwnedStmt,
            Node::ReassignOwnedStmt(_) => T_ReassignOwnedStmt,
            Node::AlterTableSpaceOptionsStmt(_) => T_AlterTableSpaceOptionsStmt,
            Node::AlterTableMoveAllStmt(_) => T_AlterTableMoveAllStmt,
            Node::AlterExtensionStmt(_) => T_AlterExtensionStmt,
            Node::AlterExtensionContentsStmt(_) => T_AlterExtensionContentsStmt,
            Node::AlterFdwStmt(_) => T_AlterFdwStmt,
            Node::AlterForeignServerStmt(_) => T_AlterForeignServerStmt,
            Node::AlterUserMappingStmt(_) => T_AlterUserMappingStmt,
            Node::AlterPolicyStmt(_) => T_AlterPolicyStmt,
            Node::AlterDatabaseStmt(_) => T_AlterDatabaseStmt,
            Node::AlterDatabaseRefreshCollStmt(_) => T_AlterDatabaseRefreshCollStmt,
            Node::AlterDatabaseSetStmt(_) => T_AlterDatabaseSetStmt,
            Node::AlterTSDictionaryStmt(_) => T_AlterTSDictionaryStmt,
            Node::AlterTSConfigurationStmt(_) => T_AlterTSConfigurationStmt,
            Node::AlterPublicationStmt(_) => T_AlterPublicationStmt,
            Node::AlterSubscriptionStmt(_) => T_AlterSubscriptionStmt,
            // raw-grammar utility / GRANT / transaction family (F4).
            Node::CheckPointStmt(_) => T_CheckPointStmt,
            Node::DiscardStmt(_) => T_DiscardStmt,
            Node::GrantStmt(_) => T_GrantStmt,
            Node::GrantRoleStmt(_) => T_GrantRoleStmt,
            Node::VariableSetStmt(_) => T_VariableSetStmt,
            Node::VariableShowStmt(_) => T_VariableShowStmt,
            Node::TransactionStmt(_) => T_TransactionStmt,
            Node::CopyStmt(_) => T_CopyStmt,
            Node::ExplainStmt(_) => T_ExplainStmt,
            Node::PrepareStmt(_) => T_PrepareStmt,
            Node::ExecuteStmt(_) => T_ExecuteStmt,
            Node::DeallocateStmt(_) => T_DeallocateStmt,
            Node::DeclareCursorStmt(_) => T_DeclareCursorStmt,
            Node::ClosePortalStmt(_) => T_ClosePortalStmt,
            Node::FetchStmt(_) => T_FetchStmt,
            Node::VacuumStmt(_) => T_VacuumStmt,
            Node::VacuumRelation(_) => T_VacuumRelation,
            Node::ClusterStmt(_) => T_ClusterStmt,
            Node::ReindexStmt(_) => T_ReindexStmt,
            Node::LockStmt(_) => T_LockStmt,
            Node::ConstraintsSetStmt(_) => T_ConstraintsSetStmt,
            Node::LoadStmt(_) => T_LoadStmt,
            Node::TruncateStmt(_) => T_TruncateStmt,
            Node::CommentStmt(_) => T_CommentStmt,
            Node::SecLabelStmt(_) => T_SecLabelStmt,
            Node::RuleStmt(_) => T_RuleStmt,
            Node::NotifyStmt(_) => T_NotifyStmt,
            Node::ListenStmt(_) => T_ListenStmt,
            Node::UnlistenStmt(_) => T_UnlistenStmt,
            Node::DoStmt(_) => T_DoStmt,
            Node::CallStmt(_) => T_CallStmt,
            Node::RefreshMatViewStmt(_) => T_RefreshMatViewStmt,
            Node::AlterSystemStmt(_) => T_AlterSystemStmt,
            Node::DropdbStmt(_) => T_DropdbStmt,
            Node::DropRoleStmt(_) => T_DropRoleStmt,
            Node::DropTableSpaceStmt(_) => T_DropTableSpaceStmt,
            Node::CreateFdwStmt(_) => T_CreateFdwStmt,
            Node::CreateForeignServerStmt(_) => T_CreateForeignServerStmt,
            Node::CreateForeignTableStmt(_) => T_CreateForeignTableStmt,
            Node::CreateUserMappingStmt(_) => T_CreateUserMappingStmt,
            Node::DropUserMappingStmt(_) => T_DropUserMappingStmt,
            Node::ImportForeignSchemaStmt(_) => T_ImportForeignSchemaStmt,
            Node::CreatePolicyStmt(_) => T_CreatePolicyStmt,
            Node::PublicationTable(_) => T_PublicationTable,
            Node::PublicationObjSpec(_) => T_PublicationObjSpec,
            Node::CreatePublicationStmt(_) => T_CreatePublicationStmt,
            Node::CreateSubscriptionStmt(_) => T_CreateSubscriptionStmt,
            Node::DropSubscriptionStmt(_) => T_DropSubscriptionStmt,
            Node::CreateEventTrigStmt(_) => T_CreateEventTrigStmt,
            Node::AlterEventTrigStmt(_) => T_AlterEventTrigStmt,
            Node::CreateTransformStmt(_) => T_CreateTransformStmt,
            Node::ReturnStmt(_) => T_ReturnStmt,
            Node::PLAssignStmt(_) => T_PLAssignStmt,
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
            Node::BitmapOr(b) => &b.plan,
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
            Node::BitmapHeapScan(m) => &m.scan.plan,
            Node::Limit(m) => &m.plan,
            Node::Unique(u) => &u.plan,
            Node::Sort(s) => &s.plan,
            Node::IncrementalSort(s) => &s.sort.plan,
            Node::Agg(a) => &a.plan,
            Node::WindowAgg(w) => &w.plan,
            Node::TableFuncScan(t) => &t.scan.plan,
            Node::FunctionScan(f) => &f.scan.plan,
            Node::ValuesScan(v) => &v.scan.plan,
            Node::CteScan(c) => &c.scan.plan,
            Node::NamedTuplestoreScan(n) => &n.scan.plan,
            Node::NestLoop(m) => &m.join.plan,
            Node::HashJoin(h) => &h.join.plan,
            Node::Hash(h) => &h.plan,
            Node::TidRangeScan(t) => &t.scan.plan,
            Node::SampleScan(s) => &s.scan.plan,
            Node::TidScan(t) => &t.scan.plan,
            Node::WorkTableScan(w) => &w.scan.plan,
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

    /// `&mut ((Plan *) node)->...` — the embedded `Plan` base, for mutation
    /// (e.g. `finalize_plan` writing back `extParam`/`allParam`).
    pub fn plan_head_mut(&mut self) -> &mut crate::nodeindexscan::Plan<'mcx> {
        match self {
            Node::Append(a) => &mut a.plan,
            Node::ModifyTable(m) => &mut m.plan,
            Node::Material(m) => &mut m.plan,
            Node::Gather(g) => &mut g.plan,
            Node::GatherMerge(g) => &mut g.plan,
            Node::MergeAppend(m) => &mut m.plan,
            Node::BitmapAnd(b) => &mut b.plan,
            Node::BitmapOr(b) => &mut b.plan,
            Node::MergeJoin(m) => &mut m.join.plan,
            Node::RecursiveUnion(r) => &mut r.plan,
            Node::Group(g) => &mut g.plan,
            Node::ProjectSet(p) => &mut p.plan,
            Node::Result(r) => &mut r.plan,
            Node::SetOp(s) => &mut s.plan,
            Node::Memoize(m) => &mut m.plan,
            Node::IndexScan(m) => &mut m.scan.plan,
            Node::IndexOnlyScan(m) => &mut m.scan.plan,
            Node::BitmapIndexScan(m) => &mut m.scan.plan,
            Node::BitmapHeapScan(m) => &mut m.scan.plan,
            Node::Limit(m) => &mut m.plan,
            Node::Unique(u) => &mut u.plan,
            Node::Sort(s) => &mut s.plan,
            Node::IncrementalSort(s) => &mut s.sort.plan,
            Node::Agg(a) => &mut a.plan,
            Node::WindowAgg(w) => &mut w.plan,
            Node::TableFuncScan(t) => &mut t.scan.plan,
            Node::FunctionScan(f) => &mut f.scan.plan,
            Node::ValuesScan(v) => &mut v.scan.plan,
            Node::CteScan(c) => &mut c.scan.plan,
            Node::NamedTuplestoreScan(n) => &mut n.scan.plan,
            Node::NestLoop(m) => &mut m.join.plan,
            Node::HashJoin(h) => &mut h.join.plan,
            Node::Hash(h) => &mut h.plan,
            Node::TidRangeScan(t) => &mut t.scan.plan,
            Node::SampleScan(s) => &mut s.scan.plan,
            Node::TidScan(t) => &mut t.scan.plan,
            Node::WorkTableScan(w) => &mut w.scan.plan,
            Node::SeqScan(s) => &mut s.scan.plan,
            Node::SubqueryScan(s) => &mut s.scan.plan,
            Node::ForeignScan(f) => &mut f.scan.plan,
            Node::CustomScan(c) => &mut c.scan.plan,
            Node::Expr(_) => {
                panic!("Node::plan_head_mut: called on an expression node, which has no Plan base")
            }
            _ => panic!(
                "Node::plan_head_mut: called on a parse-tree node ({}), which has no Plan base",
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
            Node::BitmapOr(b) => Ok(Node::BitmapOr(b.clone_in(mcx)?)),
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
            Node::BitmapHeapScan(m) => Ok(Node::BitmapHeapScan(m.clone_in(mcx)?)),
            Node::Limit(m) => Ok(Node::Limit(m.clone_in(mcx)?)),
            Node::Unique(u) => Ok(Node::Unique(u.clone_in(mcx)?)),
            Node::Sort(s) => Ok(Node::Sort(s.clone_in(mcx)?)),
            Node::IncrementalSort(s) => Ok(Node::IncrementalSort(s.clone_in(mcx)?)),
            Node::Agg(a) => Ok(Node::Agg(a.clone_in(mcx)?)),
            Node::WindowAgg(w) => Ok(Node::WindowAgg(w.clone_in(mcx)?)),
            Node::TableFuncScan(t) => Ok(Node::TableFuncScan(t.clone_in(mcx)?)),
            Node::FunctionScan(f) => Ok(Node::FunctionScan(f.clone_in(mcx)?)),
            Node::ValuesScan(v) => Ok(Node::ValuesScan(v.clone_in(mcx)?)),
            Node::CteScan(c) => Ok(Node::CteScan(c.clone_in(mcx)?)),
            Node::NamedTuplestoreScan(n) => Ok(Node::NamedTuplestoreScan(n.clone_in(mcx)?)),
            Node::NestLoop(m) => Ok(Node::NestLoop(m.clone_in(mcx)?)),
            Node::HashJoin(h) => Ok(Node::HashJoin(h.clone_in(mcx)?)),
            Node::Hash(h) => Ok(Node::Hash(h.clone_in(mcx)?)),
            Node::TidRangeScan(t) => Ok(Node::TidRangeScan(t.clone_in(mcx)?)),
            Node::SampleScan(s) => Ok(Node::SampleScan(s.clone_in(mcx)?)),
            Node::TidScan(t) => Ok(Node::TidScan(t.clone_in(mcx)?)),
            Node::WorkTableScan(w) => Ok(Node::WorkTableScan(w.clone_in(mcx)?)),
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
            Node::TableFunc(t) => Ok(Node::TableFunc(t.clone_in(mcx)?)),
            Node::RangeTblRef(r) => Ok(Node::RangeTblRef(r.clone_in(mcx)?)),
            Node::FromExpr(f) => Ok(Node::FromExpr(f.clone_in(mcx)?)),
            Node::JoinExpr(j) => Ok(Node::JoinExpr(j.clone_in(mcx)?)),
            Node::OnConflictExpr(o) => Ok(Node::OnConflictExpr(o.clone_in(mcx)?)),
            Node::MergeAction(m) => Ok(Node::MergeAction(m.clone_in(mcx)?)),
            Node::SortGroupClause(s) => Ok(Node::SortGroupClause(s.clone_in(mcx)?)),
            Node::GroupingSet(g) => Ok(Node::GroupingSet(g.clone_in(mcx)?)),
            Node::WindowClause(w) => Ok(Node::WindowClause(w.clone_in(mcx)?)),
            Node::RowMarkClause(r) => Ok(Node::RowMarkClause(r.clone_in(mcx)?)),
            Node::LockingClause(l) => Ok(Node::LockingClause(l.clone_in(mcx)?)),
            Node::WithCheckOption(w) => Ok(Node::WithCheckOption(w.clone_in(mcx)?)),
            Node::CTECycleClause(c) => Ok(Node::CTECycleClause(c.clone_in(mcx)?)),
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
            Node::XmlSerialize(x) => Ok(Node::XmlSerialize(x.clone_in(mcx)?)),
            // Value nodes (nodes/value.h) — real per-struct `copyObject`.
            Node::Integer(i) => Ok(Node::Integer(i.clone_in(mcx)?)),
            Node::Float(f) => Ok(Node::Float(f.clone_in(mcx)?)),
            Node::Boolean(b) => Ok(Node::Boolean(b.clone_in(mcx)?)),
            Node::String(s) => Ok(Node::String(s.clone_in(mcx)?)),
            Node::BitString(b) => Ok(Node::BitString(b.clone_in(mcx)?)),
            // `copyObject` over an expression node. Although the `Expr` subtree
            // is lifetime-free (owned `Box`/`Vec`), a plain `.clone()` PANICS on
            // the `Aggref`/`WindowFunc`/`SubLink`/`SubPlan` children (whose
            // embedded sub-trees are context-allocated); the sanctioned deep
            // path is `Expr::clone_in` (e.g. a WHERE-clause SubLink stored as
            // `FromExpr.quals` reaches here via `Query::clone_in`).
            Node::Expr(e) => Ok(Node::Expr(e.clone_in(mcx)?)),
            Node::List(l) => {
                let mut out: PgVec<'b, NodePtr<'b>> =
                    mcx::vec_with_capacity_in(mcx, l.len())?;
                for item in l.iter() {
                    let cloned = item.clone_in(mcx)?;
                    out.push(mcx::alloc_in(mcx, cloned)?);
                }
                Ok(Node::List(out))
            }
            Node::IntList(l) => {
                let mut out: PgVec<'b, i32> = mcx::vec_with_capacity_in(mcx, l.len())?;
                out.extend(l.iter().copied());
                Ok(Node::IntList(out))
            }
            // raw-grammar DDL "CREATE" family nodes — real per-struct copyObject.
            Node::RoleSpec(n) => Ok(Node::RoleSpec(n.clone_in(mcx)?)),
            Node::DefElem(n) => Ok(Node::DefElem(n.clone_in(mcx)?)),
            Node::Constraint(n) => Ok(Node::Constraint(n.clone_in(mcx)?)),
            Node::TableLikeClause(n) => Ok(Node::TableLikeClause(n.clone_in(mcx)?)),
            Node::IndexElem(n) => Ok(Node::IndexElem(n.clone_in(mcx)?)),
            Node::FunctionParameter(n) => Ok(Node::FunctionParameter(n.clone_in(mcx)?)),
            Node::ObjectWithArgs(n) => Ok(Node::ObjectWithArgs(n.clone_in(mcx)?)),
            Node::AccessPriv(n) => Ok(Node::AccessPriv(n.clone_in(mcx)?)),
            Node::CreateOpClassItem(n) => Ok(Node::CreateOpClassItem(n.clone_in(mcx)?)),
            Node::StatsElem(n) => Ok(Node::StatsElem(n.clone_in(mcx)?)),
            Node::PartitionElem(n) => Ok(Node::PartitionElem(n.clone_in(mcx)?)),
            Node::PartitionSpec(n) => Ok(Node::PartitionSpec(n.clone_in(mcx)?)),
            Node::PartitionBoundSpec(n) => Ok(Node::PartitionBoundSpec(n.clone_in(mcx)?)),
            Node::PartitionRangeDatum(n) => Ok(Node::PartitionRangeDatum(n.clone_in(mcx)?)),
            Node::IntoClause(n) => Ok(Node::IntoClause(n.clone_in(mcx)?)),
            Node::CreateStmt(n) => Ok(Node::CreateStmt(n.clone_in(mcx)?)),
            Node::IndexStmt(n) => Ok(Node::IndexStmt(n.clone_in(mcx)?)),
            Node::CreateSeqStmt(n) => Ok(Node::CreateSeqStmt(n.clone_in(mcx)?)),
            Node::CreateStatsStmt(n) => Ok(Node::CreateStatsStmt(n.clone_in(mcx)?)),
            Node::CreateFunctionStmt(n) => Ok(Node::CreateFunctionStmt(n.clone_in(mcx)?)),
            Node::DefineStmt(n) => Ok(Node::DefineStmt(n.clone_in(mcx)?)),
            Node::CreateDomainStmt(n) => Ok(Node::CreateDomainStmt(n.clone_in(mcx)?)),
            Node::CompositeTypeStmt(n) => Ok(Node::CompositeTypeStmt(n.clone_in(mcx)?)),
            Node::CreateEnumStmt(n) => Ok(Node::CreateEnumStmt(n.clone_in(mcx)?)),
            Node::CreateRangeStmt(n) => Ok(Node::CreateRangeStmt(n.clone_in(mcx)?)),
            Node::ViewStmt(n) => Ok(Node::ViewStmt(n.clone_in(mcx)?)),
            Node::CreateTableAsStmt(n) => Ok(Node::CreateTableAsStmt(n.clone_in(mcx)?)),
            Node::CreateSchemaStmt(n) => Ok(Node::CreateSchemaStmt(n.clone_in(mcx)?)),
            Node::CreateExtensionStmt(n) => Ok(Node::CreateExtensionStmt(n.clone_in(mcx)?)),
            Node::CreateTrigStmt(n) => Ok(Node::CreateTrigStmt(n.clone_in(mcx)?)),
            Node::CreateRoleStmt(n) => Ok(Node::CreateRoleStmt(n.clone_in(mcx)?)),
            Node::CreatedbStmt(n) => Ok(Node::CreatedbStmt(n.clone_in(mcx)?)),
            Node::CreateCastStmt(n) => Ok(Node::CreateCastStmt(n.clone_in(mcx)?)),
            Node::CreateOpClassStmt(n) => Ok(Node::CreateOpClassStmt(n.clone_in(mcx)?)),
            Node::CreateOpFamilyStmt(n) => Ok(Node::CreateOpFamilyStmt(n.clone_in(mcx)?)),
            Node::CreatePLangStmt(n) => Ok(Node::CreatePLangStmt(n.clone_in(mcx)?)),
            Node::CreateTableSpaceStmt(n) => Ok(Node::CreateTableSpaceStmt(n.clone_in(mcx)?)),
            Node::CreateConversionStmt(n) => Ok(Node::CreateConversionStmt(n.clone_in(mcx)?)),
            Node::CreateAmStmt(n) => Ok(Node::CreateAmStmt(n.clone_in(mcx)?)),
            // raw-grammar DDL "ALTER/DROP" family nodes.
            Node::PartitionCmd(n) => Ok(Node::PartitionCmd(n.clone_in(mcx)?)),
            Node::ReplicaIdentityStmt(n) => Ok(Node::ReplicaIdentityStmt(n.clone_in(mcx)?)),
            Node::ATAlterConstraint(n) => Ok(Node::ATAlterConstraint(n.clone_in(mcx)?)),
            Node::AlterTableStmt(n) => Ok(Node::AlterTableStmt(n.clone_in(mcx)?)),
            Node::AlterTableCmd(n) => Ok(Node::AlterTableCmd(n.clone_in(mcx)?)),
            Node::AlterCollationStmt(n) => Ok(Node::AlterCollationStmt(n.clone_in(mcx)?)),
            Node::AlterDomainStmt(n) => Ok(Node::AlterDomainStmt(n.clone_in(mcx)?)),
            Node::AlterEnumStmt(n) => Ok(Node::AlterEnumStmt(n.clone_in(mcx)?)),
            Node::AlterStatsStmt(n) => Ok(Node::AlterStatsStmt(n.clone_in(mcx)?)),
            Node::AlterSeqStmt(n) => Ok(Node::AlterSeqStmt(n.clone_in(mcx)?)),
            Node::AlterOpFamilyStmt(n) => Ok(Node::AlterOpFamilyStmt(n.clone_in(mcx)?)),
            Node::AlterFunctionStmt(n) => Ok(Node::AlterFunctionStmt(n.clone_in(mcx)?)),
            Node::DropStmt(n) => Ok(Node::DropStmt(n.clone_in(mcx)?)),
            Node::RenameStmt(n) => Ok(Node::RenameStmt(n.clone_in(mcx)?)),
            Node::AlterObjectDependsStmt(n) => Ok(Node::AlterObjectDependsStmt(n.clone_in(mcx)?)),
            Node::AlterObjectSchemaStmt(n) => Ok(Node::AlterObjectSchemaStmt(n.clone_in(mcx)?)),
            Node::AlterOwnerStmt(n) => Ok(Node::AlterOwnerStmt(n.clone_in(mcx)?)),
            Node::AlterOperatorStmt(n) => Ok(Node::AlterOperatorStmt(n.clone_in(mcx)?)),
            Node::AlterTypeStmt(n) => Ok(Node::AlterTypeStmt(n.clone_in(mcx)?)),
            Node::AlterDefaultPrivilegesStmt(n) => {
                Ok(Node::AlterDefaultPrivilegesStmt(n.clone_in(mcx)?))
            }
            Node::AlterRoleStmt(n) => Ok(Node::AlterRoleStmt(n.clone_in(mcx)?)),
            Node::AlterRoleSetStmt(n) => Ok(Node::AlterRoleSetStmt(n.clone_in(mcx)?)),
            Node::DropOwnedStmt(n) => Ok(Node::DropOwnedStmt(n.clone_in(mcx)?)),
            Node::ReassignOwnedStmt(n) => Ok(Node::ReassignOwnedStmt(n.clone_in(mcx)?)),
            Node::AlterTableSpaceOptionsStmt(n) => {
                Ok(Node::AlterTableSpaceOptionsStmt(n.clone_in(mcx)?))
            }
            Node::AlterTableMoveAllStmt(n) => Ok(Node::AlterTableMoveAllStmt(n.clone_in(mcx)?)),
            Node::AlterExtensionStmt(n) => Ok(Node::AlterExtensionStmt(n.clone_in(mcx)?)),
            Node::AlterExtensionContentsStmt(n) => {
                Ok(Node::AlterExtensionContentsStmt(n.clone_in(mcx)?))
            }
            Node::AlterFdwStmt(n) => Ok(Node::AlterFdwStmt(n.clone_in(mcx)?)),
            Node::AlterForeignServerStmt(n) => Ok(Node::AlterForeignServerStmt(n.clone_in(mcx)?)),
            Node::AlterUserMappingStmt(n) => Ok(Node::AlterUserMappingStmt(n.clone_in(mcx)?)),
            Node::AlterPolicyStmt(n) => Ok(Node::AlterPolicyStmt(n.clone_in(mcx)?)),
            Node::AlterDatabaseStmt(n) => Ok(Node::AlterDatabaseStmt(n.clone_in(mcx)?)),
            Node::AlterDatabaseRefreshCollStmt(n) => {
                Ok(Node::AlterDatabaseRefreshCollStmt(n.clone_in(mcx)?))
            }
            Node::AlterDatabaseSetStmt(n) => Ok(Node::AlterDatabaseSetStmt(n.clone_in(mcx)?)),
            Node::AlterTSDictionaryStmt(n) => Ok(Node::AlterTSDictionaryStmt(n.clone_in(mcx)?)),
            Node::AlterTSConfigurationStmt(n) => {
                Ok(Node::AlterTSConfigurationStmt(n.clone_in(mcx)?))
            }
            Node::AlterPublicationStmt(n) => Ok(Node::AlterPublicationStmt(n.clone_in(mcx)?)),
            Node::AlterSubscriptionStmt(n) => Ok(Node::AlterSubscriptionStmt(n.clone_in(mcx)?)),
            // raw-grammar utility / GRANT / transaction family (F4).
            Node::CheckPointStmt(n) => Ok(Node::CheckPointStmt(n.clone_in(mcx)?)),
            Node::DiscardStmt(n) => Ok(Node::DiscardStmt(n.clone_in(mcx)?)),
            Node::GrantStmt(n) => Ok(Node::GrantStmt(n.clone_in(mcx)?)),
            Node::GrantRoleStmt(n) => Ok(Node::GrantRoleStmt(n.clone_in(mcx)?)),
            Node::VariableSetStmt(n) => Ok(Node::VariableSetStmt(n.clone_in(mcx)?)),
            Node::VariableShowStmt(n) => Ok(Node::VariableShowStmt(n.clone_in(mcx)?)),
            Node::TransactionStmt(n) => Ok(Node::TransactionStmt(n.clone_in(mcx)?)),
            Node::CopyStmt(n) => Ok(Node::CopyStmt(n.clone_in(mcx)?)),
            Node::ExplainStmt(n) => Ok(Node::ExplainStmt(n.clone_in(mcx)?)),
            Node::PrepareStmt(n) => Ok(Node::PrepareStmt(n.clone_in(mcx)?)),
            Node::ExecuteStmt(n) => Ok(Node::ExecuteStmt(n.clone_in(mcx)?)),
            Node::DeallocateStmt(n) => Ok(Node::DeallocateStmt(n.clone_in(mcx)?)),
            Node::DeclareCursorStmt(n) => Ok(Node::DeclareCursorStmt(n.clone_in(mcx)?)),
            Node::ClosePortalStmt(n) => Ok(Node::ClosePortalStmt(n.clone_in(mcx)?)),
            Node::FetchStmt(n) => Ok(Node::FetchStmt(n.clone_in(mcx)?)),
            Node::VacuumStmt(n) => Ok(Node::VacuumStmt(n.clone_in(mcx)?)),
            Node::VacuumRelation(n) => Ok(Node::VacuumRelation(n.clone_in(mcx)?)),
            Node::ClusterStmt(n) => Ok(Node::ClusterStmt(n.clone_in(mcx)?)),
            Node::ReindexStmt(n) => Ok(Node::ReindexStmt(n.clone_in(mcx)?)),
            Node::LockStmt(n) => Ok(Node::LockStmt(n.clone_in(mcx)?)),
            Node::ConstraintsSetStmt(n) => Ok(Node::ConstraintsSetStmt(n.clone_in(mcx)?)),
            Node::LoadStmt(n) => Ok(Node::LoadStmt(n.clone_in(mcx)?)),
            Node::TruncateStmt(n) => Ok(Node::TruncateStmt(n.clone_in(mcx)?)),
            Node::CommentStmt(n) => Ok(Node::CommentStmt(n.clone_in(mcx)?)),
            Node::SecLabelStmt(n) => Ok(Node::SecLabelStmt(n.clone_in(mcx)?)),
            Node::RuleStmt(n) => Ok(Node::RuleStmt(n.clone_in(mcx)?)),
            Node::NotifyStmt(n) => Ok(Node::NotifyStmt(n.clone_in(mcx)?)),
            Node::ListenStmt(n) => Ok(Node::ListenStmt(n.clone_in(mcx)?)),
            Node::UnlistenStmt(n) => Ok(Node::UnlistenStmt(n.clone_in(mcx)?)),
            Node::DoStmt(n) => Ok(Node::DoStmt(n.clone_in(mcx)?)),
            Node::CallStmt(n) => Ok(Node::CallStmt(n.clone_in(mcx)?)),
            Node::RefreshMatViewStmt(n) => Ok(Node::RefreshMatViewStmt(n.clone_in(mcx)?)),
            Node::AlterSystemStmt(n) => Ok(Node::AlterSystemStmt(n.clone_in(mcx)?)),
            Node::DropdbStmt(n) => Ok(Node::DropdbStmt(n.clone_in(mcx)?)),
            Node::DropRoleStmt(n) => Ok(Node::DropRoleStmt(n.clone_in(mcx)?)),
            Node::DropTableSpaceStmt(n) => Ok(Node::DropTableSpaceStmt(n.clone_in(mcx)?)),
            Node::CreateFdwStmt(n) => Ok(Node::CreateFdwStmt(n.clone_in(mcx)?)),
            Node::CreateForeignServerStmt(n) => Ok(Node::CreateForeignServerStmt(n.clone_in(mcx)?)),
            Node::CreateForeignTableStmt(n) => Ok(Node::CreateForeignTableStmt(n.clone_in(mcx)?)),
            Node::CreateUserMappingStmt(n) => Ok(Node::CreateUserMappingStmt(n.clone_in(mcx)?)),
            Node::DropUserMappingStmt(n) => Ok(Node::DropUserMappingStmt(n.clone_in(mcx)?)),
            Node::ImportForeignSchemaStmt(n) => Ok(Node::ImportForeignSchemaStmt(n.clone_in(mcx)?)),
            Node::CreatePolicyStmt(n) => Ok(Node::CreatePolicyStmt(n.clone_in(mcx)?)),
            Node::PublicationTable(n) => Ok(Node::PublicationTable(n.clone_in(mcx)?)),
            Node::PublicationObjSpec(n) => Ok(Node::PublicationObjSpec(n.clone_in(mcx)?)),
            Node::CreatePublicationStmt(n) => Ok(Node::CreatePublicationStmt(n.clone_in(mcx)?)),
            Node::CreateSubscriptionStmt(n) => Ok(Node::CreateSubscriptionStmt(n.clone_in(mcx)?)),
            Node::DropSubscriptionStmt(n) => Ok(Node::DropSubscriptionStmt(n.clone_in(mcx)?)),
            Node::CreateEventTrigStmt(n) => Ok(Node::CreateEventTrigStmt(n.clone_in(mcx)?)),
            Node::AlterEventTrigStmt(n) => Ok(Node::AlterEventTrigStmt(n.clone_in(mcx)?)),
            Node::CreateTransformStmt(n) => Ok(Node::CreateTransformStmt(n.clone_in(mcx)?)),
            Node::ReturnStmt(n) => Ok(Node::ReturnStmt(n.clone_in(mcx)?)),
            Node::PLAssignStmt(n) => Ok(Node::PLAssignStmt(n.clone_in(mcx)?)),
        }
    }
}

// Node-opaque migration Phase 1 (docs/proposals/node-opaque-migration.md):
// the generated `as_/as_*_mut/expect_/is_/into_` accessor set lives in its own
// `impl<'mcx> Node<'mcx>` block, and the `ntag::T_*` const module (the O(1)
// tag-keyed dispatch surface Phase 2 migrates onto) at module scope. 100%
// additive — the enum, `tag()`, `clone_in()`, and every consumer stay
// byte-identical. Generated by `build.rs::emit_node_accessors`.
include!(concat!(env!("OUT_DIR"), "/node_ntag.rs"));
include!(concat!(env!("OUT_DIR"), "/node_accessors.rs"));

// Node-opaque migration P3 stage 2: per-variant `mk_<snake_variant>`
// associated constructors. Each body builds the existing enum variant; `mcx`
// is bound `_mcx` (unused) — at the opaque flip ONLY these bodies change, not
// the call sites. 100% additive. Generated by `build.rs::emit_constructors`.
include!(concat!(env!("OUT_DIR"), "/node_constructors.rs"));

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
const T_XmlSerialize: NodeTag = NodeTag(95);
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

#[cfg(test)]
mod node_accessor_tests {
    //! Node-opaque migration P1: smoke-test a few of the generated accessors and
    //! `ntag` consts. Purely additive API; these assert the enum-match bodies
    //! agree with `tag()`.
    use super::Node;
    use crate::nodes::ntag;

    #[test]
    fn expr_routed_accessor_const() {
        // `Node::Expr(Expr::Const(..))` reached through the generated, Expr-routed
        // `as_const`/`is_const`/`into_const`.
        let node = Node::Expr(crate::primnodes::Expr::Const(
            crate::primnodes::Const::default(),
        ));
        assert!(node.is_const());
        assert!(node.as_const().is_some());
        assert!(!node.is_append());
        assert!(node.as_append().is_none());
        // The generated `ntag` const agrees with the runtime tag.
        assert_eq!(node.tag(), ntag::T_Const);
        assert!(node.into_const().is_some());
    }

    #[test]
    fn node_direct_accessor_append() {
        // A Node-direct variant reached through the generated `as_append` family.
        let node = Node::Append(crate::nodeappend::Append::default());
        assert!(node.is_append());
        assert_eq!(node.as_append().map(|_| ()), Some(()));
        assert_eq!(node.tag(), ntag::T_Append);
        // `expect_append` returns the payload by reference (panics on mismatch).
        let _ = node.expect_append();
    }
}
