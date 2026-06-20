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

/// A node in the owned tree — the opaque analogue of C's tagged `Node *`. After
/// the node-opaque flip (`docs/proposals/node-opaque-migration.md` §8) this is a
/// `#[repr(transparent)]` newtype over [`crate::opaque_node::PgNodeBox`] (a
/// `PgBox<'mcx, dyn NodePayload<'mcx>>`), not the former 263-variant enum: the
/// concrete payload is recovered through tag-keyed downcasts (the generated
/// `as_*`/`expect_*`/`is_*`/`into_*` accessors). `'mcx` is the allocator lifetime
/// of the context the node tree lives in; constructing/copying allocates, so it
/// goes through the fallible [`Node::new`] / [`Node::clone_in`].
#[repr(transparent)]
pub struct Node<'mcx>(pub(crate) crate::opaque_node::PgNodeBox<'mcx>);

impl core::fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Node").field(&self.0).finish()
    }
}

impl<'mcx> Node<'mcx> {
    /// Construct an opaque node from a `#[repr(transparent)]` `NodePayload_<V>`
    /// adapter, allocating in `mcx` (fallible). The `mk_*` generated constructors
    /// are the typed front doors to this.
    #[inline]
    pub fn new<P>(mcx: Mcx<'mcx>, payload: P) -> PgResult<Self>
    where
        P: crate::opaque_node::NodePayload<'mcx> + 'mcx,
    {
        Ok(Node(crate::opaque_node::PgNodeBox::new(mcx, payload)?))
    }

    /// `nodeTag(node)` — the `NodeTag` discriminant of this node.
    #[inline]
    pub fn node_tag(&self) -> NodeTag {
        self.0.node_tag()
    }

    /// `nodeTag(node)` — alias of [`Node::node_tag`] (the C `nodeTag` spelling).
    #[inline]
    pub fn tag(&self) -> NodeTag {
        self.0.node_tag()
    }

    // --- Expr routing-arm accessors (the `Expr` payload is a nested sub-enum) ---

    /// The wrapped [`crate::primnodes::Expr`], if this is an expression node.
    #[inline]
    pub fn as_expr(&self) -> Option<&crate::primnodes::Expr> {
        // Gate on `is_expr()` *before* downcasting: `downcast_ref` dereferences
        // the payload as `NodePayload_Expr`, so calling it on a non-Expr node is
        // an invalid (misaligned/wrong-type) read. The tag-keyed `downcast_ref`
        // only guards on `expected == node_tag()`, and we must not hand it a tag
        // it would accept for a non-Expr payload — so we hand it the Expr tag set
        // implicitly by checking `is_expr()` first, exactly as `as_expr_mut`/
        // `into_expr` do.
        if !self.is_expr() {
            return None;
        }
        self.0
            .downcast_ref::<crate::node_payload_gen::NodePayload_Expr<'mcx>>(self.0.node_tag())
            .map(|a| &a.0)
    }
    /// The wrapped [`crate::primnodes::Expr`] (mutable).
    #[inline]
    pub fn as_expr_mut(&mut self) -> Option<&mut crate::primnodes::Expr> {
        if !self.is_expr() {
            return None;
        }
        let tag = self.0.node_tag();
        self.0
            .downcast_mut::<crate::node_payload_gen::NodePayload_Expr<'mcx>>(tag)
            .map(|a| &mut a.0)
    }
    /// `IsA`-family test: is this an expression node?
    #[inline]
    pub fn is_expr(&self) -> bool {
        crate::primnodes::Expr::tag_is_expr(self.0.node_tag())
    }
    /// Consume into the wrapped [`crate::primnodes::Expr`].
    #[inline]
    pub fn into_expr(self) -> Option<crate::primnodes::Expr> {
        if !self.is_expr() {
            return None;
        }
        let tag = self.0.node_tag();
        self.0
            .into_payload::<crate::node_payload_gen::NodePayload_Expr<'mcx>>(tag)
            .ok()
            .map(|a| a.0)
    }

    // --- plan-base upcasts (vtable; `((Plan *) node)`) -----------------------

    /// `((Plan *) node)->...` — the embedded `Plan` base; panics on a non-plan
    /// node (the asserting form, mirroring the hand-written `Node::plan_head`).
    #[inline]
    pub fn plan_head(&self) -> &crate::nodeindexscan::Plan<'mcx> {
        self.0.plan_base().unwrap_or_else(|| {
            panic!(
                "Node::plan_head: called on a non-plan node ({}), which has no Plan base",
                self.tag()
            )
        })
    }

    /// `&mut ((Plan *) node)->...` — the embedded `Plan` base, for mutation.
    #[inline]
    pub fn plan_head_mut(&mut self) -> &mut crate::nodeindexscan::Plan<'mcx> {
        let tag = self.tag();
        self.0.plan_base_mut().unwrap_or_else(|| {
            panic!(
                "Node::plan_head_mut: called on a non-plan node ({tag}), which has no Plan base"
            )
        })
    }

    /// `outerPlan(node)` (plannodes.h) — `node->plan.lefttree`.
    #[inline]
    pub fn outer_plan(&self) -> Option<&Node<'mcx>> {
        self.plan_head().lefttree.as_deref()
    }

    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    #[inline]
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {
        self.0.clone_in_dyn(mcx)
    }

    /// `copyObjectImpl` central deep-copy dispatch (alias of [`Node::clone_in`]).
    #[inline]
    pub fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Node<'dst>> {
        self.0.clone_in_dyn(dst)
    }

    /// `equal()` (equalfuncs.c) — structural equality via the vtable seam.
    #[inline]
    pub fn equal_node(&self, other: &Node<'_>) -> bool {
        // The vtable `equal_dyn` is invariant in `'mcx`; reborrow `other` at our
        // own lifetime (sound — equality reads, never stores, the payloads).
        let other: &Node<'mcx> = unsafe { &*(other as *const Node<'_> as *const Node<'mcx>) };
        self.0.equal(&other.0)
    }

    // --- snake-cased accessor aliases (hand-named differently from the generator's
    //     `as_<lowercase-ident>`; preserved so existing callers keep compiling) ---

    /// `castNode(TableFunc, node)` (borrow) — snake-named alias of `as_tablefunc`.
    #[inline]
    pub fn as_table_func(&self) -> Option<&crate::primnodes::TableFunc<'mcx>> {
        self.as_tablefunc()
    }
    /// `castNode(TableFunc, node)` (mutable borrow) — alias of `as_tablefunc_mut`.
    #[inline]
    pub fn as_table_func_mut(&mut self) -> Option<&mut crate::primnodes::TableFunc<'mcx>> {
        self.as_tablefunc_mut()
    }
    /// `castNode(CTECycleClause, node)` (borrow) — alias of `as_ctecycleclause`.
    #[inline]
    pub fn as_cte_cycle_clause(&self) -> Option<&crate::rawnodes::CTECycleClause<'mcx>> {
        self.as_ctecycleclause()
    }
    /// `castNode(CTECycleClause, node)` (mutable borrow) — alias of `as_ctecycleclause_mut`.
    #[inline]
    pub fn as_cte_cycle_clause_mut(&mut self) -> Option<&mut crate::rawnodes::CTECycleClause<'mcx>> {
        self.as_ctecycleclause_mut()
    }
    /// Consume into `CTECycleClause` — alias of `into_ctecycleclause`.
    #[inline]
    pub fn into_cte_cycle_clause(self) -> Option<crate::rawnodes::CTECycleClause<'mcx>> {
        self.into_ctecycleclause()
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

// `T_*` tags for the expression nodes reachable through the opaque `Expr`
// payload (nodes/nodetags.h, PostgreSQL 18.3 generated order). Some are read by
// the generated accessors via `ntag`; the standalone consts here are retained as
// named references for in-crate code and may be individually unused.
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
const T_JsonBehavior: NodeTag = NodeTag(47);
// SQL/JSON raw-grammar parse nodes (nodes/nodetags.h, parsenodes.h range).
const T_JsonOutput: NodeTag = NodeTag(120);
const T_JsonArgument: NodeTag = NodeTag(121);
const T_JsonFuncExpr: NodeTag = NodeTag(122);
const T_JsonTablePathSpec: NodeTag = NodeTag(123);
const T_JsonTable: NodeTag = NodeTag(124);
const T_JsonTableColumn: NodeTag = NodeTag(125);
const T_JsonKeyValue: NodeTag = NodeTag(126);
const T_JsonParseExpr: NodeTag = NodeTag(127);
const T_JsonScalarExpr: NodeTag = NodeTag(128);
const T_JsonSerializeExpr: NodeTag = NodeTag(129);
const T_JsonObjectConstructor: NodeTag = NodeTag(130);
const T_JsonArrayConstructor: NodeTag = NodeTag(131);
const T_JsonArrayQueryConstructor: NodeTag = NodeTag(132);
const T_JsonAggConstructor: NodeTag = NodeTag(133);
const T_JsonObjectAgg: NodeTag = NodeTag(134);
const T_JsonArrayAgg: NodeTag = NodeTag(135);
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

/// Keep `PgBox<Node>` cheap to talk about in field positions.
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;

#[cfg(test)]
mod node_accessor_tests {
    //! Node-opaque flip: smoke-test a few of the generated tag-keyed downcast
    //! accessors and `ntag` consts over the opaque `Node`.
    use super::Node;
    use crate::nodes::ntag;
    use mcx::MemoryContext;

    #[test]
    fn expr_routed_accessor_const() {
        // `Const` reached through the generated, Expr-routed `as_const`/`is_const`.
        let ctx = MemoryContext::new("node_accessor_test");
        let mcx = ctx.mcx();
        let node = Node::mk_const(mcx, crate::primnodes::Const::default()).expect("alloc");
        assert!(node.is_const());
        assert!(node.as_const().is_some());
        assert!(!node.is_append());
        assert!(node.as_append().is_none());
        assert_eq!(node.tag(), ntag::T_Const);
        assert!(node.into_const().is_some());
    }

    #[test]
    fn node_direct_accessor_append() {
        // A Node-direct variant reached through the generated `as_append` family.
        let ctx = MemoryContext::new("node_accessor_test");
        let mcx = ctx.mcx();
        let node = Node::mk_append(mcx, crate::nodeappend::Append::default()).expect("alloc");
        assert!(node.is_append());
        assert_eq!(node.as_append().map(|_| ()), Some(()));
        assert_eq!(node.tag(), ntag::T_Append);
        let _ = node.expect_append();
    }
}
