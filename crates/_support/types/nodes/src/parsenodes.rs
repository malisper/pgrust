// Analyzed query-tree nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use types_core::{Cardinality, Index, Oid, ParseLoc};

use crate::bitmapset::Bitmapset;
use crate::jointype::JoinType;
use crate::list::{IntList, NodeList, OidList, OptNodeList};
use crate::node_tree::{Node, NodeVariant};
use crate::nodes_enums::{CmdType, LimitOption, LockClauseStrength, LockWaitPolicy};
use crate::primnodes::{Alias, CoercionContext, FromExpr, OverridingKind};
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
pub const ACL_SELECT_FOR_UPDATE: AclMode = ACL_UPDATE;

#[derive(Clone, Copy, Debug, Default)]
pub struct RowMarkClause {
    pub rti: Index,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
    pub pushedDown: bool,
}

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

/// args are coerced to the method's parameter types; repeatable to float8.
#[derive(Default)]
pub struct TableSampleClause<'mcx> {
    pub tsmhandler: Oid,
    pub args: NodeList<'mcx>,
    pub repeatable: Option<Node<'mcx>>,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum GroupingSetKind {
    #[default]
    GROUPING_SET_EMPTY = 0,
    GROUPING_SET_SIMPLE = 1,
    GROUPING_SET_ROLLUP = 2,
    GROUPING_SET_CUBE = 3,
    GROUPING_SET_SETS = 4,
}

pub struct GroupingSet<'mcx> {
    pub kind: GroupingSetKind,
    pub content: NodeList<'mcx>,
    pub location: ParseLoc,
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

// C FunctionParameterMode (parsenodes.h); values are the proargmodes chars.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum FunctionParameterMode {
    #[default]
    FUNC_PARAM_DEFAULT = b'd',
    FUNC_PARAM_IN = b'i',
    FUNC_PARAM_OUT = b'o',
    FUNC_PARAM_INOUT = b'b',
    FUNC_PARAM_VARIADIC = b'v',
    FUNC_PARAM_TABLE = b't',
}

// C: argType is a TypeName; defexpr stays raw until transform.
#[derive(Default)]
pub struct FunctionParameter<'mcx> {
    pub name: Option<&'mcx str>,
    pub argType: Option<Node<'mcx>>,
    pub mode: FunctionParameterMode,
    pub defexpr: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

// C: objargs is the extracted input-argtype TypeName list (entries may be
// NULL for operator NONE sides); objfuncargs keeps the full FunctionParameter
// list; both NIL when args_unspecified.
#[derive(Default)]
pub struct ObjectWithArgs<'mcx> {
    pub objname: NodeList<'mcx>,
    pub objargs: OptNodeList<'mcx>,
    pub objfuncargs: NodeList<'mcx>,
    pub args_unspecified: bool,
}

// C: actions is a List of DefElem.
#[derive(Default)]
pub struct AlterFunctionStmt<'mcx> {
    pub objtype: ObjectType,
    pub func: Option<&'mcx ObjectWithArgs<'mcx>>,
    pub actions: NodeList<'mcx>,
}

// C: relation is used by the table-like forms, object by everything else.
#[derive(Default)]
pub struct AlterOwnerStmt<'mcx> {
    pub objectType: ObjectType,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub object: Option<Node<'mcx>>,
    pub newowner: Option<&'mcx RoleSpec<'mcx>>,
}

#[derive(Default)]
pub struct AlterCollationStmt<'mcx> {
    pub collname: NodeList<'mcx>,
}

// C: subtype T/N/O/C/X/V selects the ALTER DOMAIN arm; def is the default
// expression or new Constraint.
#[derive(Default)]
pub struct AlterDomainStmt<'mcx> {
    pub subtype: u8,
    pub typeName: NodeList<'mcx>,
    pub name: Option<&'mcx str>,
    pub def: Option<Node<'mcx>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

// C: relation is used by the table-like forms, object by everything else.
#[derive(Default)]
pub struct AlterObjectSchemaStmt<'mcx> {
    pub objectType: ObjectType,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub object: Option<Node<'mcx>>,
    pub newschema: Option<&'mcx str>,
    pub missing_ok: bool,
}

#[derive(Default)]
pub struct ReturnStmt<'mcx> {
    pub returnval: Option<Node<'mcx>>,
}

// C: returnType is a TypeName; sql_body is a ReturnStmt or List of stmts.
#[derive(Default)]
pub struct CreateFunctionStmt<'mcx> {
    pub is_procedure: bool,
    pub replace: bool,
    pub funcname: NodeList<'mcx>,
    pub parameters: NodeList<'mcx>,
    pub returnType: Option<Node<'mcx>>,
    pub options: NodeList<'mcx>,
    pub sql_body: Option<Node<'mcx>>,
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

#[derive(Default)]
pub struct DoStmt<'mcx> {
    pub args: NodeList<'mcx>,
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

pub struct LoadStmt<'mcx> {
    pub filename: &'mcx str,
}

pub struct LockStmt<'mcx> {
    pub relations: NodeList<'mcx>,
    pub mode: i32,
    pub nowait: bool,
}

#[derive(Default)]
pub struct CheckPointStmt {}

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

/// `search_col_list` cells are String.
#[derive(Default)]
pub struct CTESearchClause<'mcx> {
    pub search_col_list: NodeList<'mcx>,
    pub search_breadth_first: bool,
    pub search_seq_column: Option<&'mcx str>,
    pub location: ParseLoc,
}

/// `cycle_col_list` cells are String.
#[derive(Default)]
pub struct CTECycleClause<'mcx> {
    pub cycle_col_list: NodeList<'mcx>,
    pub cycle_mark_column: Option<&'mcx str>,
    pub cycle_mark_value: Option<Node<'mcx>>,
    pub cycle_mark_default: Option<Node<'mcx>>,
    pub cycle_path_column: Option<&'mcx str>,
    pub location: ParseLoc,
    pub cycle_mark_type: Oid,
    pub cycle_mark_typmod: i32,
    pub cycle_mark_collation: Oid,
    pub cycle_mark_neop: Oid,
}

/// search_clause/cycle_clause stay None from the grammar (SEARCH/CYCLE are
/// loud there); rule-load fills them via readfuncs.
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

impl<'mcx> CommonTableExpr<'mcx> {
    /// C `GetCTETargetList` (parsenodes.h): a DML CTE's output columns come
    /// from its RETURNING list. Requires an analyzed CTE (ctequery is a Query).
    pub fn cte_target_list(&self) -> &'mcx NodeList<'mcx> {
        let q = self
            .ctequery
            .expect("GetCTETargetList requires analyzed CTE")
            .as_query()
            .expect("GetCTETargetList requires analyzed CTE");
        if q.commandType == CmdType::CMD_SELECT {
            &q.targetList
        } else {
            &q.returningList
        }
    }
}

#[derive(Default)]
pub struct VacuumStmt<'mcx> {
    pub options: NodeList<'mcx>,
    pub rels: NodeList<'mcx>,
    pub is_vacuumcmd: bool,
}

#[derive(Default)]
pub struct ClusterStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub indexname: Option<&'mcx str>,
    pub params: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ReindexObjectType {
    #[default]
    REINDEX_OBJECT_INDEX = 0,
    REINDEX_OBJECT_TABLE,
    REINDEX_OBJECT_SCHEMA,
    REINDEX_OBJECT_SYSTEM,
    REINDEX_OBJECT_DATABASE,
}

#[derive(Default)]
pub struct ReindexStmt<'mcx> {
    pub kind: ReindexObjectType,
    pub relation: Option<Node<'mcx>>,
    pub name: Option<&'mcx str>,
    pub params: NodeList<'mcx>,
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

#[derive(Default)]
pub struct CreateEventTrigStmt<'mcx> {
    pub trigname: Option<&'mcx str>,
    pub eventname: Option<&'mcx str>,
    pub whenclause: NodeList<'mcx>,
    pub funcname: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterEventTrigStmt<'mcx> {
    pub trigname: Option<&'mcx str>,
    pub tgenabled: i8,
}

#[derive(Default)]
pub struct CreatedbStmt<'mcx> {
    pub dbname: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct DropdbStmt<'mcx> {
    pub dbname: Option<&'mcx str>,
    pub missing_ok: bool,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterDatabaseStmt<'mcx> {
    pub dbname: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterDatabaseRefreshCollStmt<'mcx> {
    pub dbname: Option<&'mcx str>,
}

#[derive(Default)]
pub struct CreateTableSpaceStmt<'mcx> {
    pub tablespacename: Option<&'mcx str>,
    pub owner: Option<&'mcx RoleSpec<'mcx>>,
    pub location: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct DropTableSpaceStmt<'mcx> {
    pub tablespacename: Option<&'mcx str>,
    pub missing_ok: bool,
}

#[derive(Default)]
pub struct AlterTableSpaceOptionsStmt<'mcx> {
    pub tablespacename: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub isReset: bool,
}

pub struct AlterSystemStmt<'mcx> {
    pub setstmt: &'mcx VariableSetStmt<'mcx>,
}

// C: setstmt is a VariableSetStmt node.
#[derive(Default)]
pub struct AlterDatabaseSetStmt<'mcx> {
    pub dbname: Option<&'mcx str>,
    pub setstmt: Option<Node<'mcx>>,
}

// C: object is a List for TABLE/COLUMN forms; comment NULL removes it.
#[derive(Default)]
pub struct CommentStmt<'mcx> {
    pub objtype: ObjectType,
    pub object: Option<Node<'mcx>>,
    pub comment: Option<&'mcx str>,
}

// C: provider NULL means the sole loaded provider; label NULL removes it.
#[derive(Default)]
pub struct SecLabelStmt<'mcx> {
    pub objtype: ObjectType,
    pub object: Option<Node<'mcx>>,
    pub provider: Option<&'mcx str>,
    pub label: Option<&'mcx str>,
}

#[derive(Default)]
pub struct CreateConversionStmt<'mcx> {
    pub conversion_name: NodeList<'mcx>,
    pub for_encoding_name: Option<&'mcx str>,
    pub to_encoding_name: Option<&'mcx str>,
    pub func_name: NodeList<'mcx>,
    pub def: bool,
}

#[derive(Default)]
pub struct CreatePLangStmt<'mcx> {
    pub replace: bool,
    pub plname: Option<&'mcx str>,
    pub plhandler: NodeList<'mcx>,
    pub plinline: NodeList<'mcx>,
    pub plvalidator: NodeList<'mcx>,
    pub pltrusted: bool,
}

#[derive(Default)]
pub struct DefineStmt<'mcx> {
    pub kind: ObjectType,
    pub oldstyle: bool,
    pub defnames: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub definition: NodeList<'mcx>,
    pub if_not_exists: bool,
    pub replace: bool,
}

pub const OPCLASS_ITEM_OPERATOR: i32 = 1;
pub const OPCLASS_ITEM_FUNCTION: i32 = 2;
pub const OPCLASS_ITEM_STORAGETYPE: i32 = 3;

#[derive(Default)]
pub struct CreateOpClassStmt<'mcx> {
    pub opclassname: NodeList<'mcx>,
    pub opfamilyname: NodeList<'mcx>,
    pub amname: Option<&'mcx str>,
    pub datatype: Option<Node<'mcx>>,
    pub items: NodeList<'mcx>,
    pub isDefault: bool,
}

// C: name is an ObjectWithArgs*, storedtype a TypeName*.
#[derive(Default)]
pub struct CreateOpClassItem<'mcx> {
    pub itemtype: i32,
    pub name: Option<Node<'mcx>>,
    pub number: i32,
    pub order_family: NodeList<'mcx>,
    pub class_args: NodeList<'mcx>,
    pub storedtype: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct CreateOpFamilyStmt<'mcx> {
    pub opfamilyname: NodeList<'mcx>,
    pub amname: Option<&'mcx str>,
}

#[derive(Default)]
pub struct AlterOpFamilyStmt<'mcx> {
    pub opfamilyname: NodeList<'mcx>,
    pub amname: Option<&'mcx str>,
    pub isDrop: bool,
    pub items: NodeList<'mcx>,
}

// C: opername is an ObjectWithArgs*.
#[derive(Default)]
pub struct AlterOperatorStmt<'mcx> {
    pub opername: Option<Node<'mcx>>,
    pub options: NodeList<'mcx>,
}

pub const AMTYPE_INDEX: u8 = b'i';
pub const AMTYPE_TABLE: u8 = b't';

#[derive(Default)]
pub struct CreateAmStmt<'mcx> {
    pub amname: Option<&'mcx str>,
    pub handler_name: NodeList<'mcx>,
    pub amtype: u8,
}

// C: sourcetype/targettype are TypeName*, func an ObjectWithArgs*.
#[derive(Default)]
pub struct CreateCastStmt<'mcx> {
    pub sourcetype: Option<Node<'mcx>>,
    pub targettype: Option<Node<'mcx>>,
    pub func: Option<Node<'mcx>>,
    pub context: CoercionContext,
    pub inout: bool,
}

// C: type_name is a TypeName*, fromsql/tosql ObjectWithArgs*.
#[derive(Default)]
pub struct CreateTransformStmt<'mcx> {
    pub replace: bool,
    pub type_name: Option<Node<'mcx>>,
    pub lang: Option<&'mcx str>,
    pub fromsql: Option<Node<'mcx>>,
    pub tosql: Option<Node<'mcx>>,
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

// C ATAlterConstraint (parsenodes.h): payload of AT_AlterConstraint.
#[derive(Default)]
pub struct ATAlterConstraint<'mcx> {
    pub conname: Option<&'mcx str>,
    pub alterEnforceability: bool,
    pub is_enforced: bool,
    pub alterDeferrability: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub alterInheritability: bool,
    pub noinherit: bool,
}

// pg_class.h REPLICA_IDENTITY_* chars.
pub const REPLICA_IDENTITY_DEFAULT: u8 = b'd';
pub const REPLICA_IDENTITY_NOTHING: u8 = b'n';
pub const REPLICA_IDENTITY_FULL: u8 = b'f';
pub const REPLICA_IDENTITY_INDEX: u8 = b'i';

#[derive(Default)]
pub struct ReplicaIdentityStmt<'mcx> {
    pub identity_type: u8,
    pub name: Option<&'mcx str>,
}

#[derive(Default)]
pub struct AlterTableStmt<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub cmds: NodeList<'mcx>,
    pub objtype: ObjectType,
    pub missing_ok: bool,
}

// ALTER TABLE/INDEX/MATERIALIZED VIEW ALL IN TABLESPACE ... SET TABLESPACE.
#[derive(Default)]
pub struct AlterTableMoveAllStmt<'mcx> {
    pub orig_tablespacename: Option<&'mcx str>,
    pub objtype: ObjectType,
    pub roles: NodeList<'mcx>,
    pub new_tablespacename: Option<&'mcx str>,
    pub nowait: bool,
}

#[derive(Default)]
pub struct RenameStmt<'mcx> {
    pub renameType: ObjectType,
    pub relationType: ObjectType,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub object: Option<Node<'mcx>>,
    pub subname: Option<&'mcx str>,
    pub newname: Option<&'mcx str>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RoleSpecType {
    #[default]
    ROLESPEC_CSTRING = 0,
    ROLESPEC_CURRENT_ROLE = 1,
    ROLESPEC_CURRENT_USER = 2,
    ROLESPEC_SESSION_USER = 3,
    ROLESPEC_PUBLIC = 4,
}

#[derive(Default)]
pub struct RoleSpec<'mcx> {
    pub roletype: RoleSpecType,
    pub rolename: Option<&'mcx str>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum WCOKind {
    #[default]
    WCO_VIEW_CHECK = 0,
    WCO_RLS_INSERT_CHECK = 1,
    WCO_RLS_UPDATE_CHECK = 2,
    WCO_RLS_CONFLICT_CHECK = 3,
    WCO_RLS_MERGE_UPDATE_CHECK = 4,
    WCO_RLS_MERGE_DELETE_CHECK = 5,
}

mcx::forget_safe_nodrop!(WCOKind);

pub struct WithCheckOption<'mcx> {
    pub kind: WCOKind,
    pub relname: Option<&'mcx str>,
    pub polname: Option<&'mcx str>,
    pub qual: Option<Node<'mcx>>,
    pub cascaded: bool,
}

pub struct CreatePolicyStmt<'mcx> {
    pub policy_name: Option<&'mcx str>,
    pub table: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub cmd_name: Option<&'mcx str>,
    pub permissive: bool,
    pub roles: NodeList<'mcx>,
    pub qual: Option<Node<'mcx>>,
    pub with_check: Option<Node<'mcx>>,
}

pub struct AlterPolicyStmt<'mcx> {
    pub policy_name: Option<&'mcx str>,
    pub table: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub roles: NodeList<'mcx>,
    pub qual: Option<Node<'mcx>>,
    pub with_check: Option<Node<'mcx>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum GrantTargetType {
    #[default]
    ACL_TARGET_OBJECT = 0,
    ACL_TARGET_ALL_IN_SCHEMA = 1,
    ACL_TARGET_DEFAULTS = 2,
}

pub struct GrantStmt<'mcx> {
    pub is_grant: bool,
    pub targtype: GrantTargetType,
    pub objtype: ObjectType,
    pub objects: NodeList<'mcx>,
    pub privileges: NodeList<'mcx>,
    pub grantees: NodeList<'mcx>,
    pub grant_option: bool,
    pub grantor: Option<&'mcx RoleSpec<'mcx>>,
    pub behavior: DropBehavior,
}

impl Default for GrantStmt<'_> {
    fn default() -> Self {
        GrantStmt {
            is_grant: false,
            targtype: GrantTargetType::ACL_TARGET_OBJECT,
            objtype: ObjectType::OBJECT_TABLE,
            objects: NodeList::nil(),
            privileges: NodeList::nil(),
            grantees: NodeList::nil(),
            grant_option: false,
            grantor: None,
            behavior: DropBehavior::DROP_RESTRICT,
        }
    }
}

#[derive(Default)]
pub struct AccessPriv<'mcx> {
    pub priv_name: Option<&'mcx str>,
    pub cols: NodeList<'mcx>,
}

// C: action is a GrantStmt with objects == NIL and targtype ACL_TARGET_DEFAULTS.
#[derive(Default)]
pub struct AlterDefaultPrivilegesStmt<'mcx> {
    pub options: NodeList<'mcx>,
    pub action: Option<&'mcx GrantStmt<'mcx>>,
}

#[derive(Default)]
pub struct GrantRoleStmt<'mcx> {
    pub granted_roles: NodeList<'mcx>,
    pub grantee_roles: NodeList<'mcx>,
    pub is_grant: bool,
    pub opt: NodeList<'mcx>,
    pub grantor: Option<&'mcx RoleSpec<'mcx>>,
    pub behavior: DropBehavior,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RoleStmtType {
    #[default]
    ROLESTMT_ROLE = 0,
    ROLESTMT_USER = 1,
    ROLESTMT_GROUP = 2,
}

#[derive(Default)]
pub struct CreateRoleStmt<'mcx> {
    pub stmt_type: RoleStmtType,
    pub role: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

pub struct AlterRoleStmt<'mcx> {
    pub role: &'mcx RoleSpec<'mcx>,
    pub options: NodeList<'mcx>,
    pub action: i32,
}

// C: role == NULL means ALTER ROLE ALL.
pub struct AlterRoleSetStmt<'mcx> {
    pub role: Option<&'mcx RoleSpec<'mcx>>,
    pub database: Option<&'mcx str>,
    pub setstmt: &'mcx VariableSetStmt<'mcx>,
}

#[derive(Default)]
pub struct DropRoleStmt<'mcx> {
    pub roles: NodeList<'mcx>,
    pub missing_ok: bool,
}

#[derive(Default)]
pub struct DropOwnedStmt<'mcx> {
    pub roles: NodeList<'mcx>,
    pub behavior: DropBehavior,
}

pub struct ReassignOwnedStmt<'mcx> {
    pub roles: NodeList<'mcx>,
    pub newrole: &'mcx RoleSpec<'mcx>,
}

pub struct InlineCodeBlock<'mcx> {
    pub source_text: &'mcx str,
    pub lang_oid: Oid,
    pub lang_is_trusted: bool,
    pub atomic: bool,
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

#[derive(Default)]
pub struct PublicationTable<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub whereClause: Option<Node<'mcx>>,
    pub columns: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum PublicationObjSpecType {
    #[default]
    PUBLICATIONOBJ_TABLE = 0,
    PUBLICATIONOBJ_TABLES_IN_SCHEMA = 1,
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA = 2,
    PUBLICATIONOBJ_CONTINUATION = 3,
}

#[derive(Default)]
pub struct PublicationObjSpec<'mcx> {
    pub pubobjtype: PublicationObjSpecType,
    pub name: Option<&'mcx str>,
    pub pubtable: Option<&'mcx PublicationTable<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CreatePublicationStmt<'mcx> {
    pub pubname: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub pubobjects: NodeList<'mcx>,
    pub for_all_tables: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterPublicationAction {
    #[default]
    AP_AddObjects = 0,
    AP_DropObjects = 1,
    AP_SetObjects = 2,
}

#[derive(Default)]
pub struct AlterPublicationStmt<'mcx> {
    pub pubname: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub pubobjects: NodeList<'mcx>,
    pub for_all_tables: bool,
    pub action: AlterPublicationAction,
}

#[derive(Default)]
pub struct CreateSubscriptionStmt<'mcx> {
    pub subname: Option<&'mcx str>,
    pub conninfo: Option<&'mcx str>,
    pub publication: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterSubscriptionType {
    #[default]
    ALTER_SUBSCRIPTION_OPTIONS = 0,
    ALTER_SUBSCRIPTION_CONNECTION = 1,
    ALTER_SUBSCRIPTION_SET_PUBLICATION = 2,
    ALTER_SUBSCRIPTION_ADD_PUBLICATION = 3,
    ALTER_SUBSCRIPTION_DROP_PUBLICATION = 4,
    ALTER_SUBSCRIPTION_REFRESH = 5,
    ALTER_SUBSCRIPTION_ENABLED = 6,
    ALTER_SUBSCRIPTION_SKIP = 7,
}

#[derive(Default)]
pub struct AlterSubscriptionStmt<'mcx> {
    pub kind: AlterSubscriptionType,
    pub subname: Option<&'mcx str>,
    pub conninfo: Option<&'mcx str>,
    pub publication: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct DropSubscriptionStmt<'mcx> {
    pub subname: Option<&'mcx str>,
    pub missing_ok: bool,
    pub behavior: DropBehavior,
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
unsafe impl<'mcx> NodeVariant<'mcx> for TableSampleClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_TableSampleClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RTEPermissionInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_RTEPermissionInfo;
}
unsafe impl<'mcx> NodeVariant<'mcx> for GroupingSet<'mcx> {
    const TAG: NodeTag = NodeTag::T_GroupingSet;
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
unsafe impl<'mcx> NodeVariant<'mcx> for FunctionParameter<'mcx> {
    const TAG: NodeTag = NodeTag::T_FunctionParameter;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReturnStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReturnStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateFunctionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateFunctionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ObjectWithArgs<'mcx> {
    const TAG: NodeTag = NodeTag::T_ObjectWithArgs;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterFunctionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterFunctionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterOwnerStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterOwnerStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterCollationStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterCollationStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterDomainStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterDomainStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterObjectSchemaStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterObjectSchemaStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterSystemStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterSystemStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VariableSetStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VariableSetStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RoleSpec<'mcx> {
    const TAG: NodeTag = NodeTag::T_RoleSpec;
}
unsafe impl<'mcx> NodeVariant<'mcx> for GrantStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_GrantStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AccessPriv<'mcx> {
    const TAG: NodeTag = NodeTag::T_AccessPriv;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterDefaultPrivilegesStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterDefaultPrivilegesStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for GrantRoleStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_GrantRoleStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateRoleStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateRoleStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterRoleStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterRoleStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterRoleSetStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterRoleSetStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropRoleStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropRoleStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropOwnedStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropOwnedStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReassignOwnedStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReassignOwnedStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VariableShowStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VariableShowStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DoStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DoStmt;
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
unsafe impl<'mcx> NodeVariant<'mcx> for LoadStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_LoadStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for LockStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_LockStmt;
}
unsafe impl NodeVariant<'_> for CheckPointStmt {
    const TAG: NodeTag = NodeTag::T_CheckPointStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for InlineCodeBlock<'mcx> {
    const TAG: NodeTag = NodeTag::T_InlineCodeBlock;
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
unsafe impl<'mcx> NodeVariant<'mcx> for SecLabelStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_SecLabelStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DefineStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DefineStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateOpClassStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateOpClassStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateOpClassItem<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateOpClassItem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateOpFamilyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateOpFamilyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterOpFamilyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterOpFamilyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterOperatorStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterOperatorStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateAmStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateAmStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateCastStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateCastStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateTransformStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateTransformStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateEventTrigStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateEventTrigStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterEventTrigStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterEventTrigStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreatedbStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreatedbStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropdbStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropdbStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterDatabaseStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterDatabaseStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterDatabaseRefreshCollStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterDatabaseRefreshCollStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterDatabaseSetStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterDatabaseSetStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateTableSpaceStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateTableSpaceStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropTableSpaceStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropTableSpaceStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableSpaceOptionsStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableSpaceOptionsStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableMoveAllStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableMoveAllStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTableCmd<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTableCmd;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RenameStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RenameStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ATAlterConstraint<'mcx> {
    const TAG: NodeTag = NodeTag::T_ATAlterConstraint;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReplicaIdentityStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReplicaIdentityStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WithClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_WithClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CommonTableExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CommonTableExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CTESearchClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_CTESearchClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CTECycleClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_CTECycleClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VacuumStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_VacuumStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateConversionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateConversionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreatePLangStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreatePLangStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ClusterStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ClusterStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReindexStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReindexStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for VacuumRelation<'mcx> {
    const TAG: NodeTag = NodeTag::T_VacuumRelation;
}
unsafe impl NodeVariant<'_> for RowMarkClause {
    const TAG: NodeTag = NodeTag::T_RowMarkClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WithCheckOption<'mcx> {
    const TAG: NodeTag = NodeTag::T_WithCheckOption;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreatePolicyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreatePolicyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterPolicyStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterPolicyStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PublicationTable<'mcx> {
    const TAG: NodeTag = NodeTag::T_PublicationTable;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PublicationObjSpec<'mcx> {
    const TAG: NodeTag = NodeTag::T_PublicationObjSpec;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreatePublicationStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreatePublicationStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterPublicationStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterPublicationStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateSubscriptionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateSubscriptionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterSubscriptionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterSubscriptionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropSubscriptionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropSubscriptionStmt;
}

impl<'mcx> Node<'mcx> {
    #[inline]
    pub fn as_query(self) -> Option<&'mcx Query<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_row_mark_clause(self) -> Option<&'mcx RowMarkClause> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_tbl_entry(self) -> Option<&'mcx RangeTblEntry<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_table_sample_clause(self) -> Option<&'mcx TableSampleClause<'mcx>> {
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
    pub fn as_cte_search_clause(self) -> Option<&'mcx CTESearchClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_cte_cycle_clause(self) -> Option<&'mcx CTECycleClause<'mcx>> {
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
    pub fn as_grouping_set(self) -> Option<&'mcx GroupingSet<'mcx>> {
        self.as_variant()
    }

    pub fn mk_grouping_set(
        mcx: mcx::Mcx<'mcx>,
        kind: GroupingSetKind,
        content: NodeList<'mcx>,
        location: ParseLoc,
    ) -> types_error::PgResult<Node<'mcx>> {
        Self::mk(mcx, GroupingSet { kind, content, location })
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
    pub fn as_function_parameter(self) -> Option<&'mcx FunctionParameter<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_function_stmt(self) -> Option<&'mcx CreateFunctionStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_object_with_args(self) -> Option<&'mcx ObjectWithArgs<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_function_stmt(self) -> Option<&'mcx AlterFunctionStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_owner_stmt(self) -> Option<&'mcx AlterOwnerStmt<'mcx>> {
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
    pub fn as_alter_system_stmt(self) -> Option<&'mcx AlterSystemStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_role_spec(self) -> Option<&'mcx RoleSpec<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_with_check_option(self) -> Option<&'mcx WithCheckOption<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_policy_stmt(self) -> Option<&'mcx CreatePolicyStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_policy_stmt(self) -> Option<&'mcx AlterPolicyStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_grant_stmt(self) -> Option<&'mcx GrantStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_default_privileges_stmt(self) -> Option<&'mcx AlterDefaultPrivilegesStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_grant_role_stmt(self) -> Option<&'mcx GrantRoleStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_role_stmt(self) -> Option<&'mcx CreateRoleStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_role_stmt(self) -> Option<&'mcx AlterRoleStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_role_set_stmt(self) -> Option<&'mcx AlterRoleSetStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_drop_role_stmt(self) -> Option<&'mcx DropRoleStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_drop_owned_stmt(self) -> Option<&'mcx DropOwnedStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_reassign_owned_stmt(self) -> Option<&'mcx ReassignOwnedStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_access_priv(self) -> Option<&'mcx AccessPriv<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_variable_show_stmt(self) -> Option<&'mcx VariableShowStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_do_stmt(self) -> Option<&'mcx DoStmt<'mcx>> {
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
    pub fn as_load_stmt(self) -> Option<&'mcx LoadStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_lock_stmt(self) -> Option<&'mcx LockStmt<'mcx>> {
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

    #[inline]
    pub fn as_sec_label_stmt(self) -> Option<&'mcx SecLabelStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_event_trig_stmt(self) -> Option<&'mcx CreateEventTrigStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_event_trig_stmt(self) -> Option<&'mcx AlterEventTrigStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_createdb_stmt(self) -> Option<&'mcx CreatedbStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_dropdb_stmt(self) -> Option<&'mcx DropdbStmt<'mcx>> {
        self.as_variant()
    }
}
