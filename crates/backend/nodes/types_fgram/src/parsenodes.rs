//! Parse-tree node types (`nodes/parsenodes.h`): `Query`, `RangeTblEntry`,
//! `RTEPermissionInfo`, and the clause/spec nodes shared by
//! `copyfuncs`/`equalfuncs`.
//!
//! Every node in this module is modelled `repr(C)` with the exact field
//! order/types/widths of the C headers (cross-checked against the c2rust
//! embedded struct defs). Sub-pointers to other in-scope nodes use the real
//! `*mut <Struct>` type (forward references resolve within the crate). The
//! [`crate::OpaqueNode`] seam is used *only* for pointees that are genuinely
//! out of scope here (e.g. planner-internal nodes that copy/equal do not reach
//! through these structs). Members that copy/equal traverse as generic node
//! trees (`utilityStmt`, `setOperations`, `havingQual`, ...) are typed as
//! `*mut Node`, matching the C `Node *` and the authoritative c2rust defs.

use core::ffi::{c_char, c_int};

use pg_ffi_fgram::{AttrNumber, Bitmapset, List, Node, NodeTag, Oid};

use crate::parsenodes_stmts::{TypeName, WindowDef};
use crate::primnodes::{
    Cardinality, CmdType, Expr, FromExpr, Index, JoinType, JsonBehavior, JsonExprOp, JsonFormat,
    JsonReturning, JsonValueExpr, JsonWrapper, OnConflictExpr, OverridingKind, ParseLoc,
    XmlOptionType,
};
use crate::{Alias, TableFunc};

pub type QuerySource = core::ffi::c_uint;
pub const QSRC_ORIGINAL: QuerySource = 0;
pub const QSRC_PARSER: QuerySource = 1;
pub const QSRC_INSTEAD_RULE: QuerySource = 2;
pub const QSRC_QUAL_INSTEAD_RULE: QuerySource = 3;
pub const QSRC_NON_INSTEAD_RULE: QuerySource = 4;

pub type LimitOption = core::ffi::c_uint;
pub const LIMIT_OPTION_COUNT: LimitOption = 0;
pub const LIMIT_OPTION_WITH_TIES: LimitOption = 1;

pub type RTEKind = core::ffi::c_uint;
pub const RTE_RELATION: RTEKind = 0;
pub const RTE_SUBQUERY: RTEKind = 1;
pub const RTE_JOIN: RTEKind = 2;
pub const RTE_FUNCTION: RTEKind = 3;
pub const RTE_TABLEFUNC: RTEKind = 4;
pub const RTE_VALUES: RTEKind = 5;
pub const RTE_CTE: RTEKind = 6;
pub const RTE_NAMEDTUPLESTORE: RTEKind = 7;
pub const RTE_RESULT: RTEKind = 8;
pub const RTE_GROUP: RTEKind = 9;

pub type LockClauseStrength = core::ffi::c_uint;
pub const LCS_NONE: LockClauseStrength = 0;
pub const LCS_FORKEYSHARE: LockClauseStrength = 1;
pub const LCS_FORSHARE: LockClauseStrength = 2;
pub const LCS_FORNOKEYUPDATE: LockClauseStrength = 3;
pub const LCS_FORUPDATE: LockClauseStrength = 4;

pub type LockWaitPolicy = core::ffi::c_uint;
pub const LOCK_WAIT_BLOCK: LockWaitPolicy = 0;
pub const LOCK_WAIT_SKIP: LockWaitPolicy = 1;
pub const LOCK_WAIT_ERROR: LockWaitPolicy = 2;

pub type CTEMaterialize = core::ffi::c_uint;
pub const CTE_MATERIALIZE_DEFAULT: CTEMaterialize = 0;
pub const CTE_MATERIALIZE_ALWAYS: CTEMaterialize = 1;
pub const CTE_MATERIALIZE_NEVER: CTEMaterialize = 2;

/// `OnConflictAction` (`nodes/primnodes.h`) - ON CONFLICT action kind.
///
/// Defined here because the in-scope [`OnConflictClause`] needs the typed
/// enum; `primnodes::OnConflictExpr` still spells the same field as a raw
/// `c_uint` for ABI compatibility.
pub type OnConflictAction = core::ffi::c_uint;
pub const ONCONFLICT_NONE: OnConflictAction = 0;
pub const ONCONFLICT_NOTHING: OnConflictAction = 1;
pub const ONCONFLICT_UPDATE: OnConflictAction = 2;

/// `MergeMatchKind` (`nodes/parsenodes.h`) - WHEN clause match kind for MERGE.
pub type MergeMatchKind = core::ffi::c_uint;
pub const MERGE_WHEN_MATCHED: MergeMatchKind = 0;
pub const MERGE_WHEN_NOT_MATCHED_BY_SOURCE: MergeMatchKind = 1;
pub const MERGE_WHEN_NOT_MATCHED_BY_TARGET: MergeMatchKind = 2;

/// `WCOKind` (`nodes/parsenodes.h`) - kind of WITH CHECK OPTION.
pub type WCOKind = core::ffi::c_uint;
pub const WCO_VIEW_CHECK: WCOKind = 0;
pub const WCO_RLS_INSERT_CHECK: WCOKind = 1;
pub const WCO_RLS_UPDATE_CHECK: WCOKind = 2;
pub const WCO_RLS_CONFLICT_CHECK: WCOKind = 3;
pub const WCO_RLS_MERGE_UPDATE_CHECK: WCOKind = 4;
pub const WCO_RLS_MERGE_DELETE_CHECK: WCOKind = 5;

/// `GroupingSetKind` (`nodes/parsenodes.h`).
pub type GroupingSetKind = core::ffi::c_uint;
pub const GROUPING_SET_EMPTY: GroupingSetKind = 0;
pub const GROUPING_SET_SIMPLE: GroupingSetKind = 1;
pub const GROUPING_SET_ROLLUP: GroupingSetKind = 2;
pub const GROUPING_SET_CUBE: GroupingSetKind = 3;
pub const GROUPING_SET_SETS: GroupingSetKind = 4;

/// `ReturningOptionKind` (`nodes/parsenodes.h`).
pub type ReturningOptionKind = core::ffi::c_uint;
pub const RETURNING_OPTION_OLD: ReturningOptionKind = 0;
pub const RETURNING_OPTION_NEW: ReturningOptionKind = 1;

/// `AclMode` (`nodes/parsenodes.h` via `utils/acl.h`) - bitmask of privileges.
///
/// `typedef uint64 AclMode;` in PostgreSQL 18.
pub type AclMode = u64;

/// `JsonQuotes` (`nodes/parsenodes.h`) - `[KEEP|OMIT] QUOTES` clause for
/// `JSON_QUERY()`.
pub type JsonQuotes = core::ffi::c_uint;
pub const JS_QUOTES_UNSPEC: JsonQuotes = 0;
pub const JS_QUOTES_KEEP: JsonQuotes = 1;
pub const JS_QUOTES_OMIT: JsonQuotes = 2;

/// `JsonTableColumnType` (`nodes/parsenodes.h`) - enumeration of `JSON_TABLE`
/// column types.
pub type JsonTableColumnType = core::ffi::c_uint;
pub const JTC_FOR_ORDINALITY: JsonTableColumnType = 0;
pub const JTC_REGULAR: JsonTableColumnType = 1;
pub const JTC_EXISTS: JsonTableColumnType = 2;
pub const JTC_FORMATTED: JsonTableColumnType = 3;
pub const JTC_NESTED: JsonTableColumnType = 4;

/// `Query` - the top-level parse/rewrite tree.
///
/// `utilityStmt` and `setOperations` point at the broad utility-statement /
/// set-operation node family; `copyfuncs`/`equalfuncs` traverse them as generic
/// node trees, so they are typed `*mut Node` (matching the C `Node *`), not the
/// [`OpaqueNode`] seam. `jointree`, `onConflict`, and the list members are real
/// modelled types.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Query {
    pub type_: NodeTag,
    pub command_type: CmdType,
    pub query_source: QuerySource,
    pub query_id: i64,
    pub can_set_tag: bool,
    pub utility_stmt: *mut Node,
    pub result_relation: c_int,
    pub has_aggs: bool,
    pub has_window_funcs: bool,
    pub has_target_srfs: bool,
    pub has_sub_links: bool,
    pub has_distinct_on: bool,
    pub has_recursive: bool,
    pub has_modifying_cte: bool,
    pub has_for_update: bool,
    pub has_row_security: bool,
    pub has_group_rte: bool,
    pub is_return: bool,
    pub cte_list: *mut List,
    pub rtable: *mut List,
    pub rteperminfos: *mut List,
    pub jointree: *mut FromExpr,
    pub merge_action_list: *mut List,
    pub merge_target_relation: c_int,
    pub merge_join_condition: *mut Node,
    pub target_list: *mut List,
    pub override_: OverridingKind,
    pub on_conflict: *mut OnConflictExpr,
    pub returning_old_alias: *mut c_char,
    pub returning_new_alias: *mut c_char,
    pub returning_list: *mut List,
    pub group_clause: *mut List,
    pub group_distinct: bool,
    pub grouping_sets: *mut List,
    pub having_qual: *mut Node,
    pub window_clause: *mut List,
    pub distinct_clause: *mut List,
    pub sort_clause: *mut List,
    pub limit_offset: *mut Node,
    pub limit_count: *mut Node,
    pub limit_option: LimitOption,
    pub row_marks: *mut List,
    pub set_operations: *mut Node,
    pub constraint_deps: *mut List,
    pub with_check_options: *mut List,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// `RangeTblEntry` - a range-table entry.
///
/// `tablesample` points at [`TableSampleClause`], which is modelled in this
/// module, so it uses the real pointer type. `subquery`, `tablefunc`, the alias
/// members, and the list members are likewise real modelled types.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTblEntry {
    pub type_: NodeTag,
    pub alias: *mut Alias,
    pub eref: *mut Alias,
    pub rtekind: RTEKind,
    pub relid: Oid,
    pub inh: bool,
    pub relkind: c_char,
    pub rellockmode: c_int,
    pub perminfoindex: Index,
    pub tablesample: *mut TableSampleClause,
    pub subquery: *mut Query,
    pub security_barrier: bool,
    pub jointype: JoinType,
    pub joinmergedcols: c_int,
    pub joinaliasvars: *mut List,
    pub joinleftcols: *mut List,
    pub joinrightcols: *mut List,
    pub join_using_alias: *mut Alias,
    pub functions: *mut List,
    pub funcordinality: bool,
    pub tablefunc: *mut TableFunc,
    pub values_lists: *mut List,
    pub ctename: *mut c_char,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub coltypes: *mut List,
    pub coltypmods: *mut List,
    pub colcollations: *mut List,
    pub enrname: *mut c_char,
    pub enrtuples: Cardinality,
    pub groupexprs: *mut List,
    pub lateral: bool,
    pub in_from_cl: bool,
    pub security_quals: *mut List,
}

/// `RTEPermissionInfo` - per-relation permission-checking info hung off `Query`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RTEPermissionInfo {
    pub type_: NodeTag,
    pub relid: Oid,
    pub inh: bool,
    pub required_perms: AclMode,
    pub check_as_user: Oid,
    pub selected_cols: *mut Bitmapset,
    pub inserted_cols: *mut Bitmapset,
    pub updated_cols: *mut Bitmapset,
}

/// `RangeTblFunction` - subsidiary data for one function in a FUNCTION RTE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTblFunction {
    pub type_: NodeTag,
    pub funcexpr: *mut Node,
    pub funccolcount: c_int,
    pub funccolnames: *mut List,
    pub funccoltypes: *mut List,
    pub funccoltypmods: *mut List,
    pub funccolcollations: *mut List,
    pub funcparams: *mut Bitmapset,
}

/// `TableSampleClause` - TABLESAMPLE in a transformed FROM clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableSampleClause {
    pub type_: NodeTag,
    pub tsmhandler: Oid,
    pub args: *mut List,
    pub repeatable: *mut Expr,
}

/// `WithCheckOption` - WITH CHECK OPTION / RLS WITH CHECK check.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WithCheckOption {
    pub type_: NodeTag,
    pub kind: WCOKind,
    pub relname: *mut c_char,
    pub polname: *mut c_char,
    pub qual: *mut Node,
    pub cascaded: bool,
}

// ---------------------------------------------------------------------------
// Self-contained clause / spec nodes.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortGroupClause {
    pub type_: NodeTag,
    pub tle_sort_group_ref: Index,
    pub eqop: Oid,
    pub sortop: Oid,
    pub reverse_sort: bool,
    pub nulls_first: bool,
    pub hashable: bool,
}

/// `GroupingSet` - CUBE / ROLLUP / GROUPING SETS clause representation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GroupingSet {
    pub type_: NodeTag,
    pub kind: GroupingSetKind,
    pub content: *mut List,
    pub location: ParseLoc,
}

/// `WindowClause` - transformed WINDOW/OVER clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowClause {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub refname: *mut c_char,
    pub partition_clause: *mut List,
    pub order_clause: *mut List,
    pub frame_options: c_int,
    pub start_offset: *mut Node,
    pub end_offset: *mut Node,
    pub start_in_range_func: Oid,
    pub end_in_range_func: Oid,
    pub in_range_coll: Oid,
    pub in_range_asc: bool,
    pub in_range_nulls_first: bool,
    pub winref: Index,
    pub copied_order: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowMarkClause {
    pub type_: NodeTag,
    pub rti: Index,
    pub strength: LockClauseStrength,
    pub wait_policy: LockWaitPolicy,
    pub pushed_down: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WithClause {
    pub type_: NodeTag,
    pub ctes: *mut List,
    pub recursive: bool,
    pub location: ParseLoc,
}

/// `InferClause` - ON CONFLICT unique-index inference clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct InferClause {
    pub type_: NodeTag,
    pub index_elems: *mut List,
    pub where_clause: *mut Node,
    pub conname: *mut c_char,
    pub location: ParseLoc,
}

/// `OnConflictClause` - ON CONFLICT clause (raw parser representation).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OnConflictClause {
    pub type_: NodeTag,
    pub action: OnConflictAction,
    pub infer: *mut InferClause,
    pub target_list: *mut List,
    pub where_clause: *mut Node,
    pub location: ParseLoc,
}

/// `CTESearchClause` - SEARCH clause of a recursive CTE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CTESearchClause {
    pub type_: NodeTag,
    pub search_col_list: *mut List,
    pub search_breadth_first: bool,
    pub search_seq_column: *mut c_char,
    pub location: ParseLoc,
}

/// `CTECycleClause` - CYCLE clause of a recursive CTE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CTECycleClause {
    pub type_: NodeTag,
    pub cycle_col_list: *mut List,
    pub cycle_mark_column: *mut c_char,
    pub cycle_mark_value: *mut Node,
    pub cycle_mark_default: *mut Node,
    pub cycle_path_column: *mut c_char,
    pub location: ParseLoc,
    pub cycle_mark_type: Oid,
    pub cycle_mark_typmod: c_int,
    pub cycle_mark_collation: Oid,
    pub cycle_mark_neop: Oid,
}

/// `CommonTableExpr` - WITH list element.
///
/// `search_clause`/`cycle_clause` point at the now-modelled [`CTESearchClause`]
/// / [`CTECycleClause`]; `ctequery` is a generic node tree (`Node *`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CommonTableExpr {
    pub type_: NodeTag,
    pub ctename: *mut c_char,
    pub aliascolnames: *mut List,
    pub ctematerialized: CTEMaterialize,
    pub ctequery: *mut Node,
    pub search_clause: *mut CTESearchClause,
    pub cycle_clause: *mut CTECycleClause,
    pub location: ParseLoc,
    pub cterecursive: bool,
    pub cterefcount: c_int,
    pub ctecolnames: *mut List,
    pub ctecoltypes: *mut List,
    pub ctecoltypmods: *mut List,
    pub ctecolcollations: *mut List,
}

/// `MergeWhenClause` - raw parser representation of a MERGE WHEN clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeWhenClause {
    pub type_: NodeTag,
    pub match_kind: MergeMatchKind,
    pub command_type: CmdType,
    pub override_: OverridingKind,
    pub condition: *mut Node,
    pub target_list: *mut List,
    pub values: *mut List,
}

/// `ReturningOption` - an individual option in a RETURNING WITH(...) list.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReturningOption {
    pub type_: NodeTag,
    pub option: ReturningOptionKind,
    pub value: *mut c_char,
    pub location: ParseLoc,
}

/// `ReturningClause` - RETURNING expressions plus any WITH(...) options.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReturningClause {
    pub type_: NodeTag,
    pub options: *mut List,
    pub exprs: *mut List,
}

/// `TriggerTransition` - transition row/table naming clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TriggerTransition {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub is_new: bool,
    pub is_table: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AppendRelInfo {
    pub type_: NodeTag,
    pub parent_relid: Index,
    pub child_relid: Index,
    pub parent_reltype: Oid,
    pub child_reltype: Oid,
    pub translated_vars: *mut List,
    pub num_child_cols: c_int,
    pub parent_colnos: *mut AttrNumber,
    pub parent_reloid: Oid,
}

/// `LockingClause` - raw representation of `FOR [NO KEY] UPDATE` / `[KEY] SHARE`
/// options. `lockedRels == NIL` means "all relations in query"; otherwise it is
/// a list of `RangeVar` nodes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockingClause {
    pub type_: NodeTag,
    pub locked_rels: *mut List,
    pub strength: LockClauseStrength,
    pub wait_policy: LockWaitPolicy,
}

/// `XmlSerialize` - `XMLSERIALIZE(...)` in the raw parse tree only.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XmlSerialize {
    pub type_: NodeTag,
    pub xmloption: XmlOptionType,
    pub expr: *mut Node,
    pub type_name: *mut TypeName,
    pub indent: bool,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// SQL/JSON support - untransformed (raw parse) representations.
//
// These are the raw-grammar SQL/JSON constructor / query / table nodes from
// `nodes/parsenodes.h`. The transformed `Expr`-family counterparts
// (`JsonExpr`, `JsonConstructorExpr`, ...) live in `primnodes`; these are the
// pre-analysis forms that `copyfuncs`/`equalfuncs` also traverse.
// ---------------------------------------------------------------------------

/// `JsonOutput` - representation of a JSON output clause
/// (`RETURNING type [FORMAT format]`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonOutput {
    pub type_: NodeTag,
    pub type_name: *mut TypeName,
    pub returning: *mut JsonReturning,
}

/// `JsonArgument` - representation of an argument from a JSON `PASSING` clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonArgument {
    pub type_: NodeTag,
    pub val: *mut JsonValueExpr,
    pub name: *mut c_char,
}

/// `JsonFuncExpr` - untransformed representation of function expressions for
/// SQL/JSON query functions.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonFuncExpr {
    pub type_: NodeTag,
    pub op: JsonExprOp,
    pub column_name: *mut c_char,
    pub context_item: *mut JsonValueExpr,
    pub pathspec: *mut Node,
    pub passing: *mut List,
    pub output: *mut JsonOutput,
    pub on_empty: *mut JsonBehavior,
    pub on_error: *mut JsonBehavior,
    pub wrapper: JsonWrapper,
    pub quotes: JsonQuotes,
    pub location: ParseLoc,
}

/// `JsonTablePathSpec` - untransformed specification of a JSON path expression
/// with an optional name.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTablePathSpec {
    pub type_: NodeTag,
    pub string: *mut Node,
    pub name: *mut c_char,
    pub name_location: ParseLoc,
    pub location: ParseLoc,
}

/// `JsonTable` - untransformed representation of `JSON_TABLE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTable {
    pub type_: NodeTag,
    pub context_item: *mut JsonValueExpr,
    pub pathspec: *mut JsonTablePathSpec,
    pub passing: *mut List,
    pub columns: *mut List,
    pub on_error: *mut JsonBehavior,
    pub alias: *mut Alias,
    pub lateral: bool,
    pub location: ParseLoc,
}

/// `JsonTableColumn` - untransformed representation of a `JSON_TABLE` column.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTableColumn {
    pub type_: NodeTag,
    pub coltype: JsonTableColumnType,
    pub name: *mut c_char,
    pub type_name: *mut TypeName,
    pub pathspec: *mut JsonTablePathSpec,
    pub format: *mut JsonFormat,
    pub wrapper: JsonWrapper,
    pub quotes: JsonQuotes,
    pub columns: *mut List,
    pub on_empty: *mut JsonBehavior,
    pub on_error: *mut JsonBehavior,
    pub location: ParseLoc,
}

/// `JsonKeyValue` - untransformed representation of a JSON object key-value pair
/// for `JSON_OBJECT()` and `JSON_OBJECTAGG()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonKeyValue {
    pub type_: NodeTag,
    pub key: *mut Expr,
    pub value: *mut JsonValueExpr,
}

/// `JsonParseExpr` - untransformed representation of `JSON()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonParseExpr {
    pub type_: NodeTag,
    pub expr: *mut JsonValueExpr,
    pub output: *mut JsonOutput,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

/// `JsonScalarExpr` - untransformed representation of `JSON_SCALAR()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonScalarExpr {
    pub type_: NodeTag,
    pub expr: *mut Expr,
    pub output: *mut JsonOutput,
    pub location: ParseLoc,
}

/// `JsonSerializeExpr` - untransformed representation of `JSON_SERIALIZE()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonSerializeExpr {
    pub type_: NodeTag,
    pub expr: *mut JsonValueExpr,
    pub output: *mut JsonOutput,
    pub location: ParseLoc,
}

/// `JsonObjectConstructor` - untransformed representation of the `JSON_OBJECT()`
/// constructor.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonObjectConstructor {
    pub type_: NodeTag,
    pub exprs: *mut List,
    pub output: *mut JsonOutput,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

/// `JsonArrayConstructor` - untransformed representation of the
/// `JSON_ARRAY(element, ...)` constructor.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonArrayConstructor {
    pub type_: NodeTag,
    pub exprs: *mut List,
    pub output: *mut JsonOutput,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

/// `JsonArrayQueryConstructor` - untransformed representation of the
/// `JSON_ARRAY(subquery)` constructor.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonArrayQueryConstructor {
    pub type_: NodeTag,
    pub query: *mut Node,
    pub output: *mut JsonOutput,
    pub format: *mut JsonFormat,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

/// `JsonAggConstructor` - common fields of the untransformed representations of
/// `JSON_ARRAYAGG()` and `JSON_OBJECTAGG()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonAggConstructor {
    pub type_: NodeTag,
    pub output: *mut JsonOutput,
    pub agg_filter: *mut Node,
    pub agg_order: *mut List,
    pub over: *mut WindowDef,
    pub location: ParseLoc,
}

/// `JsonObjectAgg` - untransformed representation of `JSON_OBJECTAGG()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonObjectAgg {
    pub type_: NodeTag,
    pub constructor: *mut JsonAggConstructor,
    pub arg: *mut JsonKeyValue,
    pub absent_on_null: bool,
    pub unique: bool,
}

/// `JsonArrayAgg` - untransformed representation of `JSON_ARRAYAGG()`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonArrayAgg {
    pub type_: NodeTag,
    pub constructor: *mut JsonAggConstructor,
    pub arg: *mut JsonValueExpr,
    pub absent_on_null: bool,
}

// ---------------------------------------------------------------------------
// Compile-time layout asserts (representative structs).
//
// Every node begins with `NodeTag type` at offset 0, so an `Expr`/`Plan`-style
// header check is the same as asserting `type_` lives at offset 0. We also pin
// the byte size of a few fixed-layout structs that we can compute by hand on
// the LP64 target.
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{offset_of, size_of};

    // NodeTag header at offset 0.
    assert!(offset_of!(Query, type_) == 0);
    assert!(offset_of!(RangeTblEntry, type_) == 0);
    assert!(offset_of!(RTEPermissionInfo, type_) == 0);
    assert!(offset_of!(RangeTblFunction, type_) == 0);
    assert!(offset_of!(TableSampleClause, type_) == 0);
    assert!(offset_of!(WithCheckOption, type_) == 0);
    assert!(offset_of!(SortGroupClause, type_) == 0);
    assert!(offset_of!(GroupingSet, type_) == 0);
    assert!(offset_of!(WindowClause, type_) == 0);
    assert!(offset_of!(RowMarkClause, type_) == 0);
    assert!(offset_of!(CommonTableExpr, type_) == 0);
    assert!(offset_of!(CTESearchClause, type_) == 0);
    assert!(offset_of!(CTECycleClause, type_) == 0);
    assert!(offset_of!(MergeWhenClause, type_) == 0);

    // SortGroupClause: NodeTag(4) Index(4) Oid(4) Oid(4) bool bool bool -> 4+4+4+4+3 = 19, pad to 20.
    assert!(offset_of!(SortGroupClause, tle_sort_group_ref) == 4);
    assert!(offset_of!(SortGroupClause, eqop) == 8);
    assert!(offset_of!(SortGroupClause, sortop) == 12);
    assert!(offset_of!(SortGroupClause, reverse_sort) == 16);
    assert!(size_of::<SortGroupClause>() == 20);

    // RowMarkClause: NodeTag(4) Index(4) enum(4) enum(4) bool -> 4*4+1 = 17, pad to 20.
    assert!(offset_of!(RowMarkClause, rti) == 4);
    assert!(offset_of!(RowMarkClause, strength) == 8);
    assert!(offset_of!(RowMarkClause, wait_policy) == 12);
    assert!(size_of::<RowMarkClause>() == 20);

    // RTEPermissionInfo: NodeTag(4) Oid(4) bool(+pad to 8) AclMode(u64,8) Oid(4)(+pad) 3 ptrs.
    assert!(offset_of!(RTEPermissionInfo, relid) == 4);
    assert!(offset_of!(RTEPermissionInfo, inh) == 8);
    assert!(offset_of!(RTEPermissionInfo, required_perms) == 16);
    assert!(offset_of!(RTEPermissionInfo, check_as_user) == 24);
    assert!(offset_of!(RTEPermissionInfo, selected_cols) == 32);
    assert!(size_of::<RTEPermissionInfo>() == 56);

    // RangeTblFunction: NodeTag(4) Node*(pad to 8) int(4)(pad) 4 lists + Bitmapset*.
    assert!(offset_of!(RangeTblFunction, funcexpr) == 8);
    assert!(offset_of!(RangeTblFunction, funccolcount) == 16);
    assert!(offset_of!(RangeTblFunction, funccolnames) == 24);
    assert!(size_of::<RangeTblFunction>() == 64);

    // TableSampleClause: NodeTag(4) Oid(4) List*(8) Expr*(8) -> 24.
    assert!(offset_of!(TableSampleClause, tsmhandler) == 4);
    assert!(offset_of!(TableSampleClause, args) == 8);
    assert!(offset_of!(TableSampleClause, repeatable) == 16);
    assert!(size_of::<TableSampleClause>() == 24);

    // New SQL/JSON + XmlSerialize + LockingClause nodes: NodeTag header at 0.
    assert!(offset_of!(LockingClause, type_) == 0);
    assert!(offset_of!(XmlSerialize, type_) == 0);
    assert!(offset_of!(JsonOutput, type_) == 0);
    assert!(offset_of!(JsonArgument, type_) == 0);
    assert!(offset_of!(JsonFuncExpr, type_) == 0);
    assert!(offset_of!(JsonTablePathSpec, type_) == 0);
    assert!(offset_of!(JsonTable, type_) == 0);
    assert!(offset_of!(JsonTableColumn, type_) == 0);
    assert!(offset_of!(JsonKeyValue, type_) == 0);
    assert!(offset_of!(JsonParseExpr, type_) == 0);
    assert!(offset_of!(JsonScalarExpr, type_) == 0);
    assert!(offset_of!(JsonSerializeExpr, type_) == 0);
    assert!(offset_of!(JsonObjectConstructor, type_) == 0);
    assert!(offset_of!(JsonArrayConstructor, type_) == 0);
    assert!(offset_of!(JsonArrayQueryConstructor, type_) == 0);
    assert!(offset_of!(JsonAggConstructor, type_) == 0);
    assert!(offset_of!(JsonObjectAgg, type_) == 0);
    assert!(offset_of!(JsonArrayAgg, type_) == 0);

    // LockingClause: NodeTag(4)+pad(4)+List*(8)+enum(4)+enum(4) -> 24.
    assert!(offset_of!(LockingClause, locked_rels) == 8);
    assert!(offset_of!(LockingClause, strength) == 16);
    assert!(offset_of!(LockingClause, wait_policy) == 20);
    assert!(size_of::<LockingClause>() == 24);

    // JsonOutput: NodeTag(4)+pad(4)+TypeName*(8)+JsonReturning*(8) -> 24.
    assert!(offset_of!(JsonOutput, type_name) == 8);
    assert!(offset_of!(JsonOutput, returning) == 16);
    assert!(size_of::<JsonOutput>() == 24);

    // JsonArgument: NodeTag(4)+pad(4)+JsonValueExpr*(8)+char*(8) -> 24.
    assert!(offset_of!(JsonArgument, val) == 8);
    assert!(offset_of!(JsonArgument, name) == 16);
    assert!(size_of::<JsonArgument>() == 24);

    // JsonTablePathSpec: NodeTag(4)+pad(4)+Node*(8)+char*(8)+2*ParseLoc(8) -> 32.
    assert!(offset_of!(JsonTablePathSpec, string) == 8);
    assert!(offset_of!(JsonTablePathSpec, name) == 16);
    assert!(offset_of!(JsonTablePathSpec, name_location) == 24);
    assert!(offset_of!(JsonTablePathSpec, location) == 28);
    assert!(size_of::<JsonTablePathSpec>() == 32);

    // XmlSerialize: NodeTag(4)+xmloption(4)+Node*(8)+TypeName*(8)+bool(1)+pad(3)+ParseLoc(4) -> 32.
    assert!(offset_of!(XmlSerialize, xmloption) == 4);
    assert!(offset_of!(XmlSerialize, expr) == 8);
    assert!(offset_of!(XmlSerialize, type_name) == 16);
    assert!(offset_of!(XmlSerialize, indent) == 24);
    assert!(offset_of!(XmlSerialize, location) == 28);
    assert!(size_of::<XmlSerialize>() == 32);
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the core parse-tree family (`Query`,
/// `RangeTblEntry`, and the QUERY + clause/spec nodes).
///
/// `lib.rs` concatenates this slice with the other families' coverage into the
/// crate-wide [`crate::NODE_TYPES_COVERED`] table; new structs ported into this
/// module register themselves by adding an entry here.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    &[
        NodeTypeStatus {
            name: "Query",
            tag: T_Query,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTblEntry",
            tag: T_RangeTblEntry,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RTEPermissionInfo",
            tag: T_RTEPermissionInfo,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTblFunction",
            tag: T_RangeTblFunction,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "TableSampleClause",
            tag: T_TableSampleClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WithCheckOption",
            tag: T_WithCheckOption,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SortGroupClause",
            tag: T_SortGroupClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "GroupingSet",
            tag: T_GroupingSet,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WindowClause",
            tag: T_WindowClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RowMarkClause",
            tag: T_RowMarkClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WithClause",
            tag: T_WithClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "InferClause",
            tag: T_InferClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "OnConflictClause",
            tag: T_OnConflictClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CTESearchClause",
            tag: T_CTESearchClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CTECycleClause",
            tag: T_CTECycleClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CommonTableExpr",
            tag: T_CommonTableExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MergeWhenClause",
            tag: T_MergeWhenClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ReturningOption",
            tag: T_ReturningOption,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ReturningClause",
            tag: T_ReturningClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "TriggerTransition",
            tag: T_TriggerTransition,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "AppendRelInfo",
            tag: T_AppendRelInfo,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "LockingClause",
            tag: T_LockingClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "XmlSerialize",
            tag: T_XmlSerialize,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonOutput",
            tag: T_JsonOutput,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonArgument",
            tag: T_JsonArgument,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonFuncExpr",
            tag: T_JsonFuncExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTablePathSpec",
            tag: T_JsonTablePathSpec,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTable",
            tag: T_JsonTable,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTableColumn",
            tag: T_JsonTableColumn,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonKeyValue",
            tag: T_JsonKeyValue,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonParseExpr",
            tag: T_JsonParseExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonScalarExpr",
            tag: T_JsonScalarExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonSerializeExpr",
            tag: T_JsonSerializeExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonObjectConstructor",
            tag: T_JsonObjectConstructor,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonArrayConstructor",
            tag: T_JsonArrayConstructor,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonArrayQueryConstructor",
            tag: T_JsonArrayQueryConstructor,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonAggConstructor",
            tag: T_JsonAggConstructor,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonObjectAgg",
            tag: T_JsonObjectAgg,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonArrayAgg",
            tag: T_JsonArrayAgg,
            coverage: NodeTypeCoverage::Modelled,
        },
    ]
}
