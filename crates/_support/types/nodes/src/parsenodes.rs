// Analyzed query-tree nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use types_core::{Cardinality, Index, Oid, ParseLoc};

use crate::bitmapset::Bitmapset;
use crate::jointype::JoinType;
use crate::list::{IntList, NodeList, OidList};
use crate::node_tree::{Node, NodeVariant};
use crate::nodes_enums::{CmdType, LimitOption};
use crate::primnodes::{Alias, FromExpr, OverridingKind};
use crate::tags::NodeTag;

pub type AclMode = u64;

pub const ACL_INSERT: AclMode = 1 << 0;
pub const ACL_SELECT: AclMode = 1 << 1;
pub const ACL_UPDATE: AclMode = 1 << 2;
pub const ACL_DELETE: AclMode = 1 << 3;
pub const ACL_TRUNCATE: AclMode = 1 << 4;
pub const ACL_REFERENCES: AclMode = 1 << 5;
pub const ACL_TRIGGER: AclMode = 1 << 6;
pub const ACL_EXECUTE: AclMode = 1 << 7;
pub const ACL_USAGE: AclMode = 1 << 8;
pub const ACL_CREATE: AclMode = 1 << 9;
pub const ACL_CREATE_TEMP: AclMode = 1 << 10;
pub const ACL_CONNECT: AclMode = 1 << 11;
pub const ACL_SET: AclMode = 1 << 12;
pub const ACL_ALTER_SYSTEM: AclMode = 1 << 13;
pub const ACL_MAINTAIN: AclMode = 1 << 14;
pub const ACL_NO_RIGHTS: AclMode = 0;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum QuerySource {
    #[default]
    QSRC_ORIGINAL = 0,
    QSRC_PARSER = 1,
    QSRC_INSTEAD_RULE = 2,
    QSRC_QUAL_INSTEAD_RULE = 3,
    QSRC_NON_INSTEAD_RULE = 4,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SetOperation {
    #[default]
    SETOP_NONE = 0,
    SETOP_UNION = 1,
    SETOP_INTERSECT = 2,
    SETOP_EXCEPT = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RTEKind {
    #[default]
    RTE_RELATION = 0,
    RTE_SUBQUERY = 1,
    RTE_JOIN = 2,
    RTE_FUNCTION = 3,
    RTE_TABLEFUNC = 4,
    RTE_VALUES = 5,
    RTE_CTE = 6,
    RTE_NAMEDTUPLESTORE = 7,
    RTE_RESULT = 8,
    RTE_GROUP = 9,
}

#[derive(Default)]
pub struct Query<'mcx> {
    pub commandType: CmdType,
    pub querySource: QuerySource,
    pub queryId: i64,
    pub canSetTag: bool,
    pub utilityStmt: Option<Node<'mcx>>,
    pub resultRelation: i32,
    pub hasAggs: bool,
    pub hasWindowFuncs: bool,
    pub hasTargetSRFs: bool,
    pub hasSubLinks: bool,
    pub hasDistinctOn: bool,
    pub hasRecursive: bool,
    pub hasModifyingCTE: bool,
    pub hasForUpdate: bool,
    pub hasRowSecurity: bool,
    pub hasGroupRTE: bool,
    pub isReturn: bool,
    pub cteList: NodeList<'mcx>,
    pub rtable: NodeList<'mcx>,
    pub rteperminfos: NodeList<'mcx>,
    pub jointree: Option<&'mcx FromExpr<'mcx>>,
    pub mergeActionList: NodeList<'mcx>,
    pub mergeTargetRelation: i32,
    pub mergeJoinCondition: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub r#override: OverridingKind,
    pub onConflict: Option<Node<'mcx>>,
    pub returningOldAlias: Option<&'mcx str>,
    pub returningNewAlias: Option<&'mcx str>,
    pub returningList: NodeList<'mcx>,
    pub groupClause: NodeList<'mcx>,
    pub groupDistinct: bool,
    pub groupingSets: NodeList<'mcx>,
    pub havingQual: Option<Node<'mcx>>,
    pub windowClause: NodeList<'mcx>,
    pub distinctClause: NodeList<'mcx>,
    pub sortClause: NodeList<'mcx>,
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub rowMarks: NodeList<'mcx>,
    pub setOperations: Option<Node<'mcx>>,
    pub constraintDeps: OidList<'mcx>,
    pub withCheckOptions: NodeList<'mcx>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// larg/rarg are SetOperationStmt or RangeTblRef; groupClauses NIL iff UNION ALL.
#[derive(Default)]
pub struct SetOperationStmt<'mcx> {
    pub op: SetOperation,
    pub all: bool,
    pub larg: Option<Node<'mcx>>,
    pub rarg: Option<Node<'mcx>>,
    pub colTypes: OidList<'mcx>,
    pub colTypmods: IntList<'mcx>,
    pub colCollations: OidList<'mcx>,
    pub groupClauses: NodeList<'mcx>,
}

#[derive(Default)]
pub struct RangeTblEntry<'mcx> {
    pub alias: Option<&'mcx Alias<'mcx>>,
    pub eref: Option<&'mcx Alias<'mcx>>,
    pub rtekind: RTEKind,
    pub relid: Oid,
    pub inh: bool,
    pub relkind: u8,
    pub rellockmode: i32,
    pub perminfoindex: Index,
    pub tablesample: Option<Node<'mcx>>,
    pub subquery: Option<&'mcx Query<'mcx>>,
    pub security_barrier: bool,
    pub jointype: JoinType,
    pub joinmergedcols: i32,
    pub joinaliasvars: NodeList<'mcx>,
    pub joinleftcols: IntList<'mcx>,
    pub joinrightcols: IntList<'mcx>,
    pub join_using_alias: Option<&'mcx Alias<'mcx>>,
    pub functions: NodeList<'mcx>,
    pub funcordinality: bool,
    pub tablefunc: Option<Node<'mcx>>,
    pub values_lists: NodeList<'mcx>,
    pub ctename: Option<&'mcx str>,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub coltypes: OidList<'mcx>,
    pub coltypmods: IntList<'mcx>,
    pub colcollations: OidList<'mcx>,
    pub enrname: Option<&'mcx str>,
    pub enrtuples: Cardinality,
    pub groupexprs: NodeList<'mcx>,
    pub lateral: bool,
    pub inFromCl: bool,
    pub securityQuals: NodeList<'mcx>,
}

pub struct RangeTblFunction<'mcx> {
    pub funcexpr: Option<Node<'mcx>>,
    pub funccolcount: i32,
    pub funccolnames: NodeList<'mcx>,
    pub funccoltypes: OidList<'mcx>,
    pub funccoltypmods: IntList<'mcx>,
    pub funccolcollations: OidList<'mcx>,
    pub funcparams: Bitmapset<'mcx>,
}

impl Default for RangeTblFunction<'_> {
    fn default() -> Self {
        RangeTblFunction {
            funcexpr: None,
            funccolcount: 0,
            funccolnames: NodeList::nil(),
            funccoltypes: OidList::nil(),
            funccoltypmods: IntList::nil(),
            funccolcollations: OidList::nil(),
            funcparams: Bitmapset::empty(),
        }
    }
}

pub struct RTEPermissionInfo<'mcx> {
    pub relid: Oid,
    pub inh: bool,
    pub requiredPerms: AclMode,
    pub checkAsUser: Oid,
    pub selectedCols: Bitmapset<'mcx>,
    pub insertedCols: Bitmapset<'mcx>,
    pub updatedCols: Bitmapset<'mcx>,
}

impl Default for RTEPermissionInfo<'_> {
    fn default() -> Self {
        RTEPermissionInfo {
            relid: 0,
            inh: false,
            requiredPerms: 0,
            checkAsUser: 0,
            selectedCols: Bitmapset::empty(),
            insertedCols: Bitmapset::empty(),
            updatedCols: Bitmapset::empty(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SortGroupClause {
    pub tleSortGroupRef: Index,
    pub eqop: Oid,
    pub sortop: Oid,
    pub reverse_sort: bool,
    pub nulls_first: bool,
    pub hashable: bool,
}

pub struct WindowClause<'mcx> {
    pub name: Option<&'mcx str>,
    pub refname: Option<&'mcx str>,
    pub partitionClause: NodeList<'mcx>,
    pub orderClause: NodeList<'mcx>,
    pub frameOptions: i32,
    pub startOffset: Option<Node<'mcx>>,
    pub endOffset: Option<Node<'mcx>>,
    pub startInRangeFunc: Oid,
    pub endInRangeFunc: Oid,
    pub inRangeColl: Oid,
    pub inRangeAsc: bool,
    pub inRangeNullsFirst: bool,
    pub winref: Index,
    pub copiedOrder: bool,
}

impl Default for WindowClause<'_> {
    fn default() -> Self {
        WindowClause {
            name: None,
            refname: None,
            partitionClause: NodeList::nil(),
            orderClause: NodeList::nil(),
            frameOptions: crate::rawnodes::FRAMEOPTION_DEFAULTS,
            startOffset: None,
            endOffset: None,
            startInRangeFunc: 0,
            endInRangeFunc: 0,
            inRangeColl: 0,
            inRangeAsc: true,
            inRangeNullsFirst: false,
            winref: 0,
            copiedOrder: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TransactionStmtKind {
    #[default]
    TRANS_STMT_BEGIN = 0,
    TRANS_STMT_START = 1,
    TRANS_STMT_COMMIT = 2,
    TRANS_STMT_ROLLBACK = 3,
    TRANS_STMT_SAVEPOINT = 4,
    TRANS_STMT_RELEASE = 5,
    TRANS_STMT_ROLLBACK_TO = 6,
    TRANS_STMT_PREPARE = 7,
    TRANS_STMT_COMMIT_PREPARED = 8,
    TRANS_STMT_ROLLBACK_PREPARED = 9,
}

pub struct TransactionStmt<'mcx> {
    pub kind: TransactionStmtKind,
    pub options: NodeList<'mcx>,
    pub savepoint_name: Option<&'mcx str>,
    pub gid: Option<&'mcx str>,
    pub chain: bool,
    pub location: ParseLoc,
}

impl Default for TransactionStmt<'_> {
    fn default() -> Self {
        TransactionStmt {
            kind: TransactionStmtKind::TRANS_STMT_BEGIN,
            options: NodeList::nil(),
            savepoint_name: None,
            gid: None,
            chain: false,
            location: -1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum DefElemAction {
    #[default]
    DEFELEM_UNSPEC = 0,
    DEFELEM_SET = 1,
    DEFELEM_ADD = 2,
    DEFELEM_DROP = 3,
}

#[derive(Default)]
pub struct DefElem<'mcx> {
    pub defnamespace: Option<&'mcx str>,
    pub defname: Option<&'mcx str>,
    pub arg: Option<Node<'mcx>>,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CopyStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub query: Option<Node<'mcx>>,
    pub attlist: NodeList<'mcx>,
    pub is_from: bool,
    pub is_program: bool,
    pub filename: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum VariableSetKind {
    #[default]
    VAR_SET_VALUE = 0,
    VAR_SET_DEFAULT = 1,
    VAR_SET_CURRENT = 2,
    VAR_SET_MULTI = 3,
    VAR_RESET = 4,
    VAR_RESET_ALL = 5,
}

pub struct VariableSetStmt<'mcx> {
    pub kind: VariableSetKind,
    pub name: Option<&'mcx str>,
    pub args: NodeList<'mcx>,
    pub jumble_args: bool,
    pub is_local: bool,
    pub location: ParseLoc,
}

impl Default for VariableSetStmt<'_> {
    fn default() -> Self {
        VariableSetStmt {
            kind: VariableSetKind::VAR_SET_VALUE,
            name: None,
            args: NodeList::nil(),
            jumble_args: false,
            is_local: false,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct VariableShowStmt<'mcx> {
    pub name: Option<&'mcx str>,
}

// C: raw grammar output holds the untransformed statement in `query`;
// transformExplainStmt replaces it with the analyzed Query node in place.
#[derive(Default)]
pub struct ExplainStmt<'mcx> {
    pub query: Option<Node<'mcx>>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct PrepareStmt<'mcx> {
    pub name: Option<&'mcx str>,
    pub argtypes: NodeList<'mcx>,
    pub query: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct ExecuteStmt<'mcx> {
    pub name: Option<&'mcx str>,
    pub params: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum FetchDirection {
    #[default]
    FETCH_FORWARD = 0,
    FETCH_BACKWARD,
    FETCH_ABSOLUTE,
    FETCH_RELATIVE,
}

// C: #define FETCH_ALL LONG_MAX (parsenodes.h).
pub const FETCH_ALL: i64 = i64::MAX;

pub const CURSOR_OPT_BINARY: i32 = 0x0001;
pub const CURSOR_OPT_SCROLL: i32 = 0x0002;
pub const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
pub const CURSOR_OPT_INSENSITIVE: i32 = 0x0008;
pub const CURSOR_OPT_ASENSITIVE: i32 = 0x0010;
pub const CURSOR_OPT_HOLD: i32 = 0x0020;
pub const CURSOR_OPT_FAST_PLAN: i32 = 0x0100;
pub const CURSOR_OPT_GENERIC_PLAN: i32 = 0x0200;
pub const CURSOR_OPT_CUSTOM_PLAN: i32 = 0x0400;
pub const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

#[derive(Default)]
pub struct FetchStmt<'mcx> {
    pub direction: FetchDirection,
    pub howMany: i64,
    pub portalname: Option<&'mcx str>,
    pub ismove: bool,
}

// C: raw grammar output holds the untransformed SELECT in `query`;
// transformDeclareCursorStmt replaces it with the analyzed Query node.
#[derive(Default)]
pub struct DeclareCursorStmt<'mcx> {
    pub portalname: Option<&'mcx str>,
    pub options: i32,
    pub query: Option<Node<'mcx>>,
}

// C: portalname == NULL means CLOSE ALL.
#[derive(Default)]
pub struct ClosePortalStmt<'mcx> {
    pub portalname: Option<&'mcx str>,
}

#[derive(Default)]
pub struct NotifyStmt<'mcx> {
    pub conditionname: Option<&'mcx str>,
    pub payload: Option<&'mcx str>,
}

#[derive(Default)]
pub struct ListenStmt<'mcx> {
    pub conditionname: Option<&'mcx str>,
}

// C: conditionname == NULL means UNLISTEN *.
#[derive(Default)]
pub struct UnlistenStmt<'mcx> {
    pub conditionname: Option<&'mcx str>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum DiscardMode {
    #[default]
    DISCARD_ALL = 0,
    DISCARD_PLANS = 1,
    DISCARD_SEQUENCES = 2,
    DISCARD_TEMP = 3,
}

#[derive(Default)]
pub struct DiscardStmt {
    pub target: DiscardMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum CTEMaterialize {
    #[default]
    CTEMaterializeDefault = 0,
    CTEMaterializeAlways = 1,
    CTEMaterializeNever = 2,
}

pub struct WithClause<'mcx> {
    pub ctes: NodeList<'mcx>,
    pub recursive: bool,
    pub location: ParseLoc,
}

impl Default for WithClause<'_> {
    fn default() -> Self {
        WithClause { ctes: NodeList::nil(), recursive: false, location: -1 }
    }
}

/// search_clause/cycle_clause stay None (SEARCH/CYCLE are loud in the grammar).
pub struct CommonTableExpr<'mcx> {
    pub ctename: Option<&'mcx str>,
    pub aliascolnames: NodeList<'mcx>,
    pub ctematerialized: CTEMaterialize,
    pub ctequery: Option<Node<'mcx>>,
    pub search_clause: Option<Node<'mcx>>,
    pub cycle_clause: Option<Node<'mcx>>,
    pub location: ParseLoc,
    pub cterecursive: bool,
    pub cterefcount: i32,
    pub ctecolnames: NodeList<'mcx>,
    pub ctecoltypes: OidList<'mcx>,
    pub ctecoltypmods: IntList<'mcx>,
    pub ctecolcollations: OidList<'mcx>,
}

impl Default for CommonTableExpr<'_> {
    fn default() -> Self {
        CommonTableExpr {
            ctename: None,
            aliascolnames: NodeList::nil(),
            ctematerialized: CTEMaterialize::CTEMaterializeDefault,
            ctequery: None,
            search_clause: None,
            cycle_clause: None,
            location: -1,
            cterecursive: false,
            cterefcount: 0,
            ctecolnames: NodeList::nil(),
            ctecoltypes: OidList::nil(),
            ctecoltypmods: IntList::nil(),
            ctecolcollations: OidList::nil(),
        }
    }
}

#[derive(Default)]
pub struct VacuumStmt<'mcx> {
    pub options: NodeList<'mcx>,
    pub rels: NodeList<'mcx>,
    pub is_vacuumcmd: bool,
}

// C: relation is a RangeVar; oid InvalidOid until vacuum looks it up.
#[derive(Default)]
pub struct VacuumRelation<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub oid: Oid,
    pub va_cols: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ObjectType {
    #[default]
    OBJECT_ACCESS_METHOD = 0,
    OBJECT_AGGREGATE,
    OBJECT_AMOP,
    OBJECT_AMPROC,
    OBJECT_ATTRIBUTE,
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DEFAULT,
    OBJECT_DEFACL,
    OBJECT_DOMAIN,
    OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_LANGUAGE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PARAMETER_ACL,
    OBJECT_POLICY,
    OBJECT_PROCEDURE,
    OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL,
    OBJECT_ROLE,
    OBJECT_ROUTINE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_SUBSCRIPTION,
    OBJECT_STATISTIC_EXT,
    OBJECT_TABCONSTRAINT,
    OBJECT_TABLE,
    OBJECT_TABLESPACE,
    OBJECT_TRANSFORM,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER_MAPPING,
    OBJECT_VIEW,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum DropBehavior {
    #[default]
    DROP_RESTRICT = 0,
    DROP_CASCADE = 1,
}

#[derive(Default)]
pub struct DropStmt<'mcx> {
    pub objects: NodeList<'mcx>,
    pub removeType: ObjectType,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub concurrent: bool,
}

#[derive(Default)]
pub struct TruncateStmt<'mcx> {
    pub relations: NodeList<'mcx>,
    pub restart_seqs: bool,
    pub behavior: DropBehavior,
}

// C: authrole is a RoleSpec node.
#[derive(Default)]
pub struct CreateSchemaStmt<'mcx> {
    pub schemaname: Option<&'mcx str>,
    pub authrole: Option<Node<'mcx>>,
    pub schemaElts: NodeList<'mcx>,
    pub if_not_exists: bool,
}

// C: object is a List for TABLE/COLUMN forms; comment NULL removes it.
#[derive(Default)]
pub struct CommentStmt<'mcx> {
    pub objtype: ObjectType,
    pub object: Option<Node<'mcx>>,
    pub comment: Option<&'mcx str>,
}

// C AlterTableType (parsenodes.h); discriminants are outfuncs-visible.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum AlterTableType {
    #[default]
    AT_AddColumn = 0,
    AT_AddColumnToView,
    AT_ColumnDefault,
    AT_CookedColumnDefault,
    AT_DropNotNull,
    AT_SetNotNull,
    AT_SetExpression,
    AT_DropExpression,
    AT_SetStatistics,
    AT_SetOptions,
    AT_ResetOptions,
    AT_SetStorage,
    AT_SetCompression,
    AT_DropColumn,
    AT_AddIndex,
    AT_ReAddIndex,
    AT_AddConstraint,
    AT_ReAddConstraint,
    AT_ReAddDomainConstraint,
    AT_AlterConstraint,
    AT_ValidateConstraint,
    AT_AddIndexConstraint,
    AT_DropConstraint,
    AT_ReAddComment,
    AT_AlterColumnType,
    AT_AlterColumnGenericOptions,
    AT_ChangeOwner,
    AT_ClusterOn,
    AT_DropCluster,
    AT_SetLogged,
    AT_SetUnLogged,
    AT_DropOids,
    AT_SetAccessMethod,
    AT_SetTableSpace,
    AT_SetRelOptions,
    AT_ResetRelOptions,
    AT_ReplaceRelOptions,
    AT_EnableTrig,
    AT_EnableAlwaysTrig,
    AT_EnableReplicaTrig,
    AT_DisableTrig,
    AT_EnableTrigAll,
    AT_DisableTrigAll,
    AT_EnableTrigUser,
    AT_DisableTrigUser,
    AT_EnableRule,
    AT_EnableAlwaysRule,
    AT_EnableReplicaRule,
    AT_DisableRule,
    AT_AddInherit,
    AT_DropInherit,
    AT_AddOf,
    AT_DropOf,
    AT_ReplicaIdentity,
    AT_EnableRowSecurity,
    AT_DisableRowSecurity,
    AT_ForceRowSecurity,
    AT_NoForceRowSecurity,
    AT_GenericOptions,
    AT_AttachPartition,
    AT_DetachPartition,
    AT_DetachPartitionFinalize,
    AT_AddIdentity,
    AT_SetIdentity,
    AT_DropIdentity,
    AT_ReAddStatistics,
}

#[derive(Default)]
pub struct AlterTableCmd<'mcx> {
    pub subtype: AlterTableType,
    pub name: Option<&'mcx str>,
    pub num: i16,
    pub newowner: Option<Node<'mcx>>,
    pub def: Option<Node<'mcx>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub recurse: bool,
}

#[derive(Default)]
pub struct AlterTableStmt<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub cmds: NodeList<'mcx>,
    pub objtype: ObjectType,
    pub missing_ok: bool,
}

// C: isall is redundant with name == NULL but kept for query jumbling.
pub struct DeallocateStmt<'mcx> {
    pub name: Option<&'mcx str>,
    pub isall: bool,
    pub location: ParseLoc,
}

impl Default for DeallocateStmt<'_> {
    fn default() -> Self {
        DeallocateStmt { name: None, isall: false, location: -1 }
    }
}

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for Query<'mcx> {
    const TAG: NodeTag = NodeTag::T_Query;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SetOperationStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_SetOperationStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTblEntry<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTblEntry;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTblFunction<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTblFunction;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RTEPermissionInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_RTEPermissionInfo;
}
unsafe impl NodeVariant<'_> for SortGroupClause {
    const TAG: NodeTag = NodeTag::T_SortGroupClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TransactionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_TransactionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DefElem<'mcx> {
    const TAG: NodeTag = NodeTag::T_DefElem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CopyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CopyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VariableSetStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VariableSetStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VariableShowStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VariableShowStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ExplainStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ExplainStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PrepareStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_PrepareStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ExecuteStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ExecuteStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FetchStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_FetchStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DeclareCursorStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DeclareCursorStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ClosePortalStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ClosePortalStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NotifyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_NotifyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ListenStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ListenStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for UnlistenStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_UnlistenStmt;
}
unsafe impl NodeVariant<'_> for DiscardStmt {
    const TAG: NodeTag = NodeTag::T_DiscardStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DeallocateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DeallocateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TruncateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_TruncateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateSchemaStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateSchemaStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CommentStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CommentStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableCmd<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableCmd;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WithClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_WithClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CommonTableExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CommonTableExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VacuumStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VacuumStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VacuumRelation<'mcx> {
    const TAG: NodeTag = NodeTag::T_VacuumRelation;
}

impl<'mcx> Node<'mcx> {
    #[inline]
    pub fn as_query(self) -> Option<&'mcx Query<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_tbl_entry(self) -> Option<&'mcx RangeTblEntry<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_set_operation_stmt(self) -> Option<&'mcx SetOperationStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_with_clause(self) -> Option<&'mcx WithClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_common_table_expr(self) -> Option<&'mcx CommonTableExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_tbl_function(self) -> Option<&'mcx RangeTblFunction<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_rte_permission_info(self) -> Option<&'mcx RTEPermissionInfo<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sort_group_clause(self) -> Option<&'mcx SortGroupClause> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_clause(self) -> Option<&'mcx WindowClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_transaction_stmt(self) -> Option<&'mcx TransactionStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_def_elem(self) -> Option<&'mcx DefElem<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_copy_stmt(self) -> Option<&'mcx CopyStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_vacuum_stmt(self) -> Option<&'mcx VacuumStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_vacuum_relation(self) -> Option<&'mcx VacuumRelation<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_variable_set_stmt(self) -> Option<&'mcx VariableSetStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_variable_show_stmt(self) -> Option<&'mcx VariableShowStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_explain_stmt(self) -> Option<&'mcx ExplainStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_prepare_stmt(self) -> Option<&'mcx PrepareStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_execute_stmt(self) -> Option<&'mcx ExecuteStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_fetch_stmt(self) -> Option<&'mcx FetchStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_declare_cursor_stmt(self) -> Option<&'mcx DeclareCursorStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_close_portal_stmt(self) -> Option<&'mcx ClosePortalStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_notify_stmt(self) -> Option<&'mcx NotifyStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_listen_stmt(self) -> Option<&'mcx ListenStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_unlisten_stmt(self) -> Option<&'mcx UnlistenStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_discard_stmt(self) -> Option<&'mcx DiscardStmt> {
        self.as_variant()
    }

    #[inline]
    pub fn as_deallocate_stmt(self) -> Option<&'mcx DeallocateStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_drop_stmt(self) -> Option<&'mcx DropStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_truncate_stmt(self) -> Option<&'mcx TruncateStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_schema_stmt(self) -> Option<&'mcx CreateSchemaStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_comment_stmt(self) -> Option<&'mcx CommentStmt<'mcx>> {
        self.as_variant()
    }
}
