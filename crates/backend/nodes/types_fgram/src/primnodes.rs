//! Primitive node types (`nodes/primnodes.h`) shared across parse / plan /
//! execute stages.
//!
//! These are the executable-expression nodes (the `Expr` family) plus the
//! small join-tree nodes. Every struct here is a real PostgreSQL ABI type, so
//! it is `#[repr(C)]` with field order and types matching the C backend. Where
//! a member is a pointer to a node type that copyfuncs/equalfuncs do not
//! traverse and that is not modelled in this crate yet (catalog/executor
//! structs), the field is kept as a raw pointer behind the ABI seam; see
//! [`crate::OpaqueNode`]. Pointees that the copy/equal layer DOES traverse
//! (such as `IntoClause.viewQuery`, a `Query`) are modelled as real `Node*`.
//!
//! `NodeTag`, `List`, and `Bitmapset` are reused from `pgrust-pg-ffi`; they are
//! never redefined here. So is `CompareType` (used by `RowCompareExpr`).

use core::ffi::{c_char, c_double, c_int, c_uint};

use ::pg_ffi_fgram::{AttrNumber, Bitmapset, CompareType, Datum, List, Node, NodeTag, Oid};

/// `int32` as PostgreSQL spells it for typmods.
pub type Int32 = i32;
/// Range-table / attribute index (`Index` in PostgreSQL).
pub type Index = c_uint;
/// Parser source location (`ParseLoc`).
pub type ParseLoc = c_int;
/// Planner estimated row count (`Cardinality`).
pub type Cardinality = c_double;
/// Planner estimated cost (`Cost`).
pub type Cost = c_double;

// ---------------------------------------------------------------------------
// Supporting enums (kept as `#[repr(C)]`/`c_uint` for exact ABI match).
// ---------------------------------------------------------------------------

pub type CmdType = core::ffi::c_uint;
pub const CMD_UNKNOWN: CmdType = 0;
pub const CMD_SELECT: CmdType = 1;
pub const CMD_UPDATE: CmdType = 2;
pub const CMD_INSERT: CmdType = 3;
pub const CMD_DELETE: CmdType = 4;
pub const CMD_MERGE: CmdType = 5;
pub const CMD_UTILITY: CmdType = 6;
pub const CMD_NOTHING: CmdType = 7;

pub type JoinType = core::ffi::c_uint;
pub const JOIN_INNER: JoinType = 0;
pub const JOIN_LEFT: JoinType = 1;
pub const JOIN_FULL: JoinType = 2;
pub const JOIN_RIGHT: JoinType = 3;
pub const JOIN_SEMI: JoinType = 4;
pub const JOIN_ANTI: JoinType = 5;
pub const JOIN_RIGHT_SEMI: JoinType = 6;
pub const JOIN_RIGHT_ANTI: JoinType = 7;
pub const JOIN_UNIQUE_OUTER: JoinType = 8;
pub const JOIN_UNIQUE_INNER: JoinType = 9;

pub type CoercionForm = core::ffi::c_uint;
pub const COERCE_EXPLICIT_CALL: CoercionForm = 0;
pub const COERCE_EXPLICIT_CAST: CoercionForm = 1;
pub const COERCE_IMPLICIT_CAST: CoercionForm = 2;
pub const COERCE_SQL_SYNTAX: CoercionForm = 3;

pub type CoercionContext = core::ffi::c_uint;
pub const COERCION_IMPLICIT: CoercionContext = 0;
pub const COERCION_ASSIGNMENT: CoercionContext = 1;
pub const COERCION_PLPGSQL: CoercionContext = 2;
pub const COERCION_EXPLICIT: CoercionContext = 3;

pub type ParamKind = core::ffi::c_uint;
pub const PARAM_EXTERN: ParamKind = 0;
pub const PARAM_EXEC: ParamKind = 1;
pub const PARAM_SUBLINK: ParamKind = 2;
pub const PARAM_MULTIEXPR: ParamKind = 3;

pub type BoolExprType = core::ffi::c_uint;
pub const AND_EXPR: BoolExprType = 0;
pub const OR_EXPR: BoolExprType = 1;
pub const NOT_EXPR: BoolExprType = 2;

pub type SubLinkType = core::ffi::c_uint;
pub const EXISTS_SUBLINK: SubLinkType = 0;
pub const ALL_SUBLINK: SubLinkType = 1;
pub const ANY_SUBLINK: SubLinkType = 2;
pub const ROWCOMPARE_SUBLINK: SubLinkType = 3;
pub const EXPR_SUBLINK: SubLinkType = 4;
pub const MULTIEXPR_SUBLINK: SubLinkType = 5;
pub const ARRAY_SUBLINK: SubLinkType = 6;
pub const CTE_SUBLINK: SubLinkType = 7;

pub type MinMaxOp = core::ffi::c_uint;
pub const IS_GREATEST: MinMaxOp = 0;
pub const IS_LEAST: MinMaxOp = 1;

pub type NullTestType = core::ffi::c_uint;
pub const IS_NULL: NullTestType = 0;
pub const IS_NOT_NULL: NullTestType = 1;

pub type BoolTestType = core::ffi::c_uint;
pub const IS_TRUE: BoolTestType = 0;
pub const IS_NOT_TRUE: BoolTestType = 1;
pub const IS_FALSE: BoolTestType = 2;
pub const IS_NOT_FALSE: BoolTestType = 3;
pub const IS_UNKNOWN: BoolTestType = 4;
pub const IS_NOT_UNKNOWN: BoolTestType = 5;

pub type VarReturningType = core::ffi::c_uint;
pub const VAR_RETURNING_DEFAULT: VarReturningType = 0;
pub const VAR_RETURNING_OLD: VarReturningType = 1;
pub const VAR_RETURNING_NEW: VarReturningType = 2;

pub type AggSplit = core::ffi::c_uint;
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = 6;
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = 9;

pub type OnCommitAction = core::ffi::c_uint;
pub const ONCOMMIT_NOOP: OnCommitAction = 0;
pub const ONCOMMIT_PRESERVE_ROWS: OnCommitAction = 1;
pub const ONCOMMIT_DELETE_ROWS: OnCommitAction = 2;
pub const ONCOMMIT_DROP: OnCommitAction = 3;

pub type OverridingKind = core::ffi::c_uint;
pub const OVERRIDING_NOT_SET: OverridingKind = 0;
pub const OVERRIDING_USER_VALUE: OverridingKind = 1;
pub const OVERRIDING_SYSTEM_VALUE: OverridingKind = 2;

// SQLValueFunction operator codes.
pub type SQLValueFunctionOp = core::ffi::c_uint;
pub const SVFOP_CURRENT_DATE: SQLValueFunctionOp = 0;
pub const SVFOP_CURRENT_TIME: SQLValueFunctionOp = 1;
pub const SVFOP_CURRENT_TIME_N: SQLValueFunctionOp = 2;
pub const SVFOP_CURRENT_TIMESTAMP: SQLValueFunctionOp = 3;
pub const SVFOP_CURRENT_TIMESTAMP_N: SQLValueFunctionOp = 4;
pub const SVFOP_LOCALTIME: SQLValueFunctionOp = 5;
pub const SVFOP_LOCALTIME_N: SQLValueFunctionOp = 6;
pub const SVFOP_LOCALTIMESTAMP: SQLValueFunctionOp = 7;
pub const SVFOP_LOCALTIMESTAMP_N: SQLValueFunctionOp = 8;
pub const SVFOP_CURRENT_ROLE: SQLValueFunctionOp = 9;
pub const SVFOP_CURRENT_USER: SQLValueFunctionOp = 10;
pub const SVFOP_USER: SQLValueFunctionOp = 11;
pub const SVFOP_SESSION_USER: SQLValueFunctionOp = 12;
pub const SVFOP_CURRENT_CATALOG: SQLValueFunctionOp = 13;
pub const SVFOP_CURRENT_SCHEMA: SQLValueFunctionOp = 14;

// XmlExpr operator codes.
pub type XmlExprOp = core::ffi::c_uint;
pub const IS_XMLCONCAT: XmlExprOp = 0;
pub const IS_XMLELEMENT: XmlExprOp = 1;
pub const IS_XMLFOREST: XmlExprOp = 2;
pub const IS_XMLPARSE: XmlExprOp = 3;
pub const IS_XMLPI: XmlExprOp = 4;
pub const IS_XMLROOT: XmlExprOp = 5;
pub const IS_XMLSERIALIZE: XmlExprOp = 6;
pub const IS_DOCUMENT: XmlExprOp = 7;

pub type XmlOptionType = core::ffi::c_uint;
pub const XMLOPTION_DOCUMENT: XmlOptionType = 0;
pub const XMLOPTION_CONTENT: XmlOptionType = 1;

// SQL/JSON enums.
pub type JsonEncoding = core::ffi::c_uint;
pub const JS_ENC_DEFAULT: JsonEncoding = 0;
pub const JS_ENC_UTF8: JsonEncoding = 1;
pub const JS_ENC_UTF16: JsonEncoding = 2;
pub const JS_ENC_UTF32: JsonEncoding = 3;

pub type JsonFormatType = core::ffi::c_uint;
pub const JS_FORMAT_DEFAULT: JsonFormatType = 0;
pub const JS_FORMAT_JSON: JsonFormatType = 1;
pub const JS_FORMAT_JSONB: JsonFormatType = 2;

pub type JsonConstructorType = core::ffi::c_uint;
pub const JSCTOR_JSON_OBJECT: JsonConstructorType = 1;
pub const JSCTOR_JSON_ARRAY: JsonConstructorType = 2;
pub const JSCTOR_JSON_OBJECTAGG: JsonConstructorType = 3;
pub const JSCTOR_JSON_ARRAYAGG: JsonConstructorType = 4;
pub const JSCTOR_JSON_PARSE: JsonConstructorType = 5;
pub const JSCTOR_JSON_SCALAR: JsonConstructorType = 6;
pub const JSCTOR_JSON_SERIALIZE: JsonConstructorType = 7;

pub type JsonValueType = core::ffi::c_uint;
pub const JS_TYPE_ANY: JsonValueType = 0;
pub const JS_TYPE_OBJECT: JsonValueType = 1;
pub const JS_TYPE_ARRAY: JsonValueType = 2;
pub const JS_TYPE_SCALAR: JsonValueType = 3;

pub type JsonWrapper = core::ffi::c_uint;
pub const JSW_UNSPEC: JsonWrapper = 0;
pub const JSW_NONE: JsonWrapper = 1;
pub const JSW_CONDITIONAL: JsonWrapper = 2;
pub const JSW_UNCONDITIONAL: JsonWrapper = 3;

pub type JsonBehaviorType = core::ffi::c_uint;
pub const JSON_BEHAVIOR_NULL: JsonBehaviorType = 0;
pub const JSON_BEHAVIOR_ERROR: JsonBehaviorType = 1;
pub const JSON_BEHAVIOR_EMPTY: JsonBehaviorType = 2;
pub const JSON_BEHAVIOR_TRUE: JsonBehaviorType = 3;
pub const JSON_BEHAVIOR_FALSE: JsonBehaviorType = 4;
pub const JSON_BEHAVIOR_UNKNOWN: JsonBehaviorType = 5;
pub const JSON_BEHAVIOR_EMPTY_ARRAY: JsonBehaviorType = 6;
pub const JSON_BEHAVIOR_EMPTY_OBJECT: JsonBehaviorType = 7;
pub const JSON_BEHAVIOR_DEFAULT: JsonBehaviorType = 8;

pub type JsonExprOp = core::ffi::c_uint;
pub const JSON_EXISTS_OP: JsonExprOp = 0;
pub const JSON_QUERY_OP: JsonExprOp = 1;
pub const JSON_VALUE_OP: JsonExprOp = 2;
pub const JSON_TABLE_OP: JsonExprOp = 3;

// MERGE WHEN-clause match kind.
pub type MergeMatchKind = core::ffi::c_uint;
pub const MERGE_WHEN_MATCHED: MergeMatchKind = 0;
pub const MERGE_WHEN_NOT_MATCHED_BY_SOURCE: MergeMatchKind = 1;
pub const MERGE_WHEN_NOT_MATCHED_BY_TARGET: MergeMatchKind = 2;
pub const NUM_MERGE_MATCH_KINDS: usize = MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize + 1;

pub type TableFuncType = core::ffi::c_uint;
pub const TFT_XMLTABLE: TableFuncType = 0;
pub const TFT_JSON_TABLE: TableFuncType = 1;

// ---------------------------------------------------------------------------
// Expr superclass.
// ---------------------------------------------------------------------------

/// Generic superclass for executable-expression nodes. Only carries a
/// `NodeTag`; every `Expr` subtype embeds it as its first field, matching the
/// PostgreSQL "Node-derives-Expr" convention.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Expr {
    pub type_: NodeTag,
}

// ---------------------------------------------------------------------------
// Range/alias nodes.
// ---------------------------------------------------------------------------

// `Alias` is single-sourced at the crate root (`crate::Alias`); primnodes reuses
// it rather than redefining the (ABI-identical) struct, so the two cannot
// silently diverge.
use crate::Alias;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeVar {
    pub type_: NodeTag,
    pub catalogname: *mut c_char,
    pub schemaname: *mut c_char,
    pub relname: *mut c_char,
    pub inh: bool,
    pub relpersistence: c_char,
    pub alias: *mut Alias,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Var / Const / Param.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Var {
    pub xpr: Expr,
    pub varno: c_int,
    pub varattno: AttrNumber,
    pub vartype: Oid,
    pub vartypmod: Int32,
    pub varcollid: Oid,
    pub varnullingrels: *mut Bitmapset,
    pub varlevelsup: Index,
    pub varreturningtype: VarReturningType,
    pub varnosyn: Index,
    pub varattnosyn: AttrNumber,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Const {
    pub xpr: Expr,
    pub consttype: Oid,
    pub consttypmod: Int32,
    pub constcollid: Oid,
    pub constlen: c_int,
    pub constvalue: Datum,
    pub constisnull: bool,
    pub constbyval: bool,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Param {
    pub xpr: Expr,
    pub paramkind: ParamKind,
    pub paramid: c_int,
    pub paramtype: Oid,
    pub paramtypmod: Int32,
    pub paramcollid: Oid,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Aggregate / window / grouping.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Aggref {
    pub xpr: Expr,
    pub aggfnoid: Oid,
    pub aggtype: Oid,
    pub aggcollid: Oid,
    pub inputcollid: Oid,
    pub aggtranstype: Oid,
    pub aggargtypes: *mut List,
    pub aggdirectargs: *mut List,
    pub args: *mut List,
    pub aggorder: *mut List,
    pub aggdistinct: *mut List,
    pub aggfilter: *mut Expr,
    pub aggstar: bool,
    pub aggvariadic: bool,
    pub aggkind: c_char,
    pub aggpresorted: bool,
    pub agglevelsup: Index,
    pub aggsplit: AggSplit,
    pub aggno: c_int,
    pub aggtransno: c_int,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GroupingFunc {
    pub xpr: Expr,
    pub args: *mut List,
    pub refs: *mut List,
    pub cols: *mut List,
    pub agglevelsup: Index,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowFunc {
    pub xpr: Expr,
    pub winfnoid: Oid,
    pub wintype: Oid,
    pub wincollid: Oid,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub aggfilter: *mut Expr,
    pub run_condition: *mut List,
    pub winref: Index,
    pub winstar: bool,
    pub winagg: bool,
    pub location: ParseLoc,
}

/// `WindowFuncRunCondition` - intermediate `OpExpr` used by `WindowAgg` to
/// short-circuit execution.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowFuncRunCondition {
    pub xpr: Expr,
    pub opno: Oid,
    pub inputcollid: Oid,
    pub wfunc_left: bool,
    pub arg: *mut Expr,
}

/// `MergeSupportFunc` - `MERGE_ACTION()` support function expression.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeSupportFunc {
    pub xpr: Expr,
    pub msftype: Oid,
    pub msfcollid: Oid,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Function / operator expressions.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FuncExpr {
    pub xpr: Expr,
    pub funcid: Oid,
    pub funcresulttype: Oid,
    pub funcretset: bool,
    pub funcvariadic: bool,
    pub funcformat: CoercionForm,
    pub funccollid: Oid,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NamedArgExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub name: *mut c_char,
    pub argnumber: c_int,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OpExpr {
    pub xpr: Expr,
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

/// `DistinctExpr` and `NullIfExpr` share the exact `OpExpr` ABI in PostgreSQL.
pub type DistinctExpr = OpExpr;
/// `NullIfExpr` is an `OpExpr` alias.
pub type NullIfExpr = OpExpr;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScalarArrayOpExpr {
    pub xpr: Expr,
    pub opno: Oid,
    pub opfuncid: Oid,
    pub hashfuncid: Oid,
    pub negfuncid: Oid,
    pub use_or: bool,
    pub inputcollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BoolExpr {
    pub xpr: Expr,
    pub boolop: BoolExprType,
    pub args: *mut List,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubLink {
    pub xpr: Expr,
    pub sub_link_type: SubLinkType,
    pub sub_link_id: c_int,
    pub testexpr: *mut Node,
    pub oper_name: *mut List,
    pub subselect: *mut Node,
    pub location: ParseLoc,
}

/// `SubPlan` - executable subplan reference.
///
/// `plan_id` indexes `PlannedStmt.subplans`; the plan tree itself is not
/// embedded, so there is no plan pointer to model here.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubPlan {
    pub xpr: Expr,
    pub sub_link_type: SubLinkType,
    pub testexpr: *mut Node,
    pub param_ids: *mut List,
    pub plan_id: c_int,
    pub plan_name: *mut c_char,
    pub first_col_type: Oid,
    pub first_col_typmod: Int32,
    pub first_col_collation: Oid,
    pub use_hash_table: bool,
    pub unknown_eq_false: bool,
    pub parallel_safe: bool,
    pub set_param: *mut List,
    pub par_param: *mut List,
    pub args: *mut List,
    pub startup_cost: Cost,
    pub per_call_cost: Cost,
}

/// `AlternativeSubPlan` - choice among `SubPlan`s (transient during planning).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlternativeSubPlan {
    pub xpr: Expr,
    pub subplans: *mut List,
}

// ---------------------------------------------------------------------------
// Field access / store.
// ---------------------------------------------------------------------------

/// `FieldSelect` - extract one field from a tuple value.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FieldSelect {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub fieldnum: AttrNumber,
    pub resulttype: Oid,
    pub resulttypmod: Int32,
    pub resultcollid: Oid,
}

/// `FieldStore` - modify one or more fields in a tuple value.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FieldStore {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub newvals: *mut List,
    pub fieldnums: *mut List,
    pub resulttype: Oid,
}

// ---------------------------------------------------------------------------
// Subscripting.
// ---------------------------------------------------------------------------

/// `SubscriptingRef` - subscripting (array element/slice fetch or store).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubscriptingRef {
    pub xpr: Expr,
    pub refcontainertype: Oid,
    pub refelemtype: Oid,
    pub refrestype: Oid,
    pub reftypmod: Int32,
    pub refcollid: Oid,
    pub refupperindexpr: *mut List,
    pub reflowerindexpr: *mut List,
    pub refexpr: *mut Expr,
    pub refassgnexpr: *mut Expr,
}

// ---------------------------------------------------------------------------
// Coercions.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelabelType {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub resulttype: Oid,
    pub resulttypmod: Int32,
    pub resultcollid: Oid,
    pub relabelformat: CoercionForm,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CoerceViaIO {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub resulttype: Oid,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ArrayCoerceExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub elemexpr: *mut Expr,
    pub resulttype: Oid,
    pub resulttypmod: Int32,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

/// `ConvertRowtypeExpr` - coercion between composite types matched by name.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ConvertRowtypeExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub resulttype: Oid,
    pub convertformat: CoercionForm,
    pub location: ParseLoc,
}

/// `CollateExpr` - COLLATE clause (replaced by `RelabelType` in planning).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollateExpr {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub coll_oid: Oid,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CoerceToDomain {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub resulttype: Oid,
    pub resulttypmod: Int32,
    pub resultcollid: Oid,
    pub coercionformat: CoercionForm,
    pub location: ParseLoc,
}

/// `CoerceToDomainValue` - placeholder for the value processed by a domain's
/// check constraint.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CoerceToDomainValue {
    pub xpr: Expr,
    pub type_id: Oid,
    pub type_mod: Int32,
    pub collation: Oid,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// CASE / array / row / coalesce / minmax.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CaseExpr {
    pub xpr: Expr,
    pub casetype: Oid,
    pub casecollid: Oid,
    pub arg: *mut Expr,
    pub args: *mut List,
    pub defresult: *mut Expr,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CaseWhen {
    pub xpr: Expr,
    pub expr: *mut Expr,
    pub result: *mut Expr,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CaseTestExpr {
    pub xpr: Expr,
    pub type_id: Oid,
    pub type_mod: Int32,
    pub collation: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ArrayExpr {
    pub xpr: Expr,
    pub array_typeid: Oid,
    pub array_collid: Oid,
    pub element_typeid: Oid,
    pub elements: *mut List,
    pub multidims: bool,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowExpr {
    pub xpr: Expr,
    pub args: *mut List,
    pub row_typeid: Oid,
    pub row_format: CoercionForm,
    pub colnames: *mut List,
    pub location: ParseLoc,
}

/// `RowCompareExpr` - row-wise comparison, e.g. `(a, b) <= (1, 2)`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowCompareExpr {
    pub xpr: Expr,
    pub cmptype: CompareType,
    pub opnos: *mut List,
    pub opfamilies: *mut List,
    pub inputcollids: *mut List,
    pub largs: *mut List,
    pub rargs: *mut List,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CoalesceExpr {
    pub xpr: Expr,
    pub coalescetype: Oid,
    pub coalescecollid: Oid,
    pub args: *mut List,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MinMaxExpr {
    pub xpr: Expr,
    pub minmaxtype: Oid,
    pub minmaxcollid: Oid,
    pub inputcollid: Oid,
    pub op: MinMaxOp,
    pub args: *mut List,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// SQLValueFunction / XmlExpr.
// ---------------------------------------------------------------------------

/// `SQLValueFunction` - parameterless functions with special grammar (e.g.
/// `CURRENT_DATE`, `SESSION_USER`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SQLValueFunction {
    pub xpr: Expr,
    pub op: SQLValueFunctionOp,
    pub type_: Oid,
    pub typmod: Int32,
    pub location: ParseLoc,
}

/// `XmlExpr` - various SQL/XML functions with special grammar productions.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XmlExpr {
    pub xpr: Expr,
    pub op: XmlExprOp,
    pub name: *mut c_char,
    pub named_args: *mut List,
    pub arg_names: *mut List,
    pub args: *mut List,
    pub xmloption: XmlOptionType,
    pub indent: bool,
    pub type_: Oid,
    pub typmod: Int32,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// SQL/JSON nodes.
// ---------------------------------------------------------------------------

/// `JsonFormat` - representation of a JSON FORMAT clause. `NodeTag`-headed
/// (not an `Expr`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonFormat {
    pub type_: NodeTag,
    pub format_type: JsonFormatType,
    pub encoding: JsonEncoding,
    pub location: ParseLoc,
}

/// `JsonReturning` - transformed JSON RETURNING clause. `NodeTag`-headed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonReturning {
    pub type_: NodeTag,
    pub format: *mut JsonFormat,
    pub typid: Oid,
    pub typmod: Int32,
}

/// `JsonValueExpr` - a JSON value expression (`expr [FORMAT ...]`).
/// `NodeTag`-headed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonValueExpr {
    pub type_: NodeTag,
    pub raw_expr: *mut Expr,
    pub formatted_expr: *mut Expr,
    pub format: *mut JsonFormat,
}

/// `JsonConstructorExpr` - wrapper over `FuncExpr`/`Aggref`/`WindowFunc` for
/// SQL/JSON constructors. This one *is* an `Expr` (begins with `xpr`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonConstructorExpr {
    pub xpr: Expr,
    pub type_: JsonConstructorType,
    pub args: *mut List,
    pub func: *mut Expr,
    pub coercion: *mut Expr,
    pub returning: *mut JsonReturning,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

/// `JsonIsPredicate` - the IS JSON predicate. `NodeTag`-headed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonIsPredicate {
    pub type_: NodeTag,
    pub expr: *mut Node,
    pub format: *mut JsonFormat,
    pub item_type: JsonValueType,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

/// `JsonBehavior` - ON ERROR / ON EMPTY behavior spec. `NodeTag`-headed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonBehavior {
    pub type_: NodeTag,
    pub btype: JsonBehaviorType,
    pub expr: *mut Node,
    pub coerce: bool,
    pub location: ParseLoc,
}

/// `JsonExpr` - transformed `JSON_VALUE()` / `JSON_QUERY()` / `JSON_EXISTS()`.
/// This one *is* an `Expr` (begins with `xpr`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonExpr {
    pub xpr: Expr,
    pub op: JsonExprOp,
    pub column_name: *mut c_char,
    pub formatted_expr: *mut Node,
    pub format: *mut JsonFormat,
    pub path_spec: *mut Node,
    pub returning: *mut JsonReturning,
    pub passing_names: *mut List,
    pub passing_values: *mut List,
    pub on_empty: *mut JsonBehavior,
    pub on_error: *mut JsonBehavior,
    pub use_io_coercion: bool,
    pub use_json_coercion: bool,
    pub wrapper: JsonWrapper,
    pub omit_quotes: bool,
    pub collation: Oid,
    pub location: ParseLoc,
}

/// `JsonTablePath` - a JSON path expression for a JSON_TABLE plan.
/// `NodeTag`-headed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTablePath {
    pub type_: NodeTag,
    pub value: *mut Const,
    pub name: *mut c_char,
}

/// `JsonTablePlan` - abstract base for JSON_TABLE "plans". Has no node tag of
/// its own (abstract); it is embedded as the first field of the concrete
/// JSON_TABLE plan nodes so they share the `NodeTag` header.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTablePlan {
    pub type_: NodeTag,
}

/// `JsonTablePathScan` - JSON_TABLE plan that evaluates a path + NESTED paths.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTablePathScan {
    pub plan: JsonTablePlan,
    pub path: *mut JsonTablePath,
    pub error_on_error: bool,
    pub child: *mut JsonTablePlan,
    pub col_min: c_int,
    pub col_max: c_int,
}

/// `JsonTableSiblingJoin` - JSON_TABLE plan joining sibling NESTED COLUMNS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonTableSiblingJoin {
    pub plan: JsonTablePlan,
    pub lplan: *mut JsonTablePlan,
    pub rplan: *mut JsonTablePlan,
}

// ---------------------------------------------------------------------------
// Tests and defaults.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NullTest {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub nulltesttype: NullTestType,
    pub argisrow: bool,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BooleanTest {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub booltesttype: BoolTestType,
    pub location: ParseLoc,
}

/// `MergeAction` - transformed WHEN clause of a MERGE statement.
/// `NodeTag`-headed (not an `Expr`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeAction {
    pub type_: NodeTag,
    pub match_kind: MergeMatchKind,
    pub command_type: CmdType,
    pub override_: OverridingKind,
    pub qual: *mut Node,
    pub target_list: *mut List,
    pub update_colnos: *mut List,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetToDefault {
    pub xpr: Expr,
    pub type_id: Oid,
    pub type_mod: Int32,
    pub collation: Oid,
    pub location: ParseLoc,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CurrentOfExpr {
    pub xpr: Expr,
    pub cvarno: Index,
    pub cursor_name: *mut c_char,
    pub cursor_param: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NextValueExpr {
    pub xpr: Expr,
    pub seqid: Oid,
    pub type_id: Oid,
}

/// `InferenceElem` - an element of a unique-index inference specification.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct InferenceElem {
    pub xpr: Expr,
    pub expr: *mut Node,
    pub infercollid: Oid,
    pub inferopclass: Oid,
}

/// `ReturningExpr` - return OLD/NEW.(expression) in a RETURNING list.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReturningExpr {
    pub xpr: Expr,
    pub retlevelsup: c_int,
    pub retold: bool,
    pub retexpr: *mut Expr,
}

// ---------------------------------------------------------------------------
// Target list / join tree.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TargetEntry {
    pub xpr: Expr,
    pub expr: *mut Expr,
    pub resno: AttrNumber,
    pub resname: *mut c_char,
    pub ressortgroupref: Index,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
    pub resjunk: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTblRef {
    pub type_: NodeTag,
    pub rtindex: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JoinExpr {
    pub type_: NodeTag,
    pub jointype: JoinType,
    pub is_natural: bool,
    pub larg: *mut Node,
    pub rarg: *mut Node,
    pub using_clause: *mut List,
    pub join_using_alias: *mut Alias,
    pub quals: *mut Node,
    pub alias: *mut Alias,
    pub rtindex: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FromExpr {
    pub type_: NodeTag,
    pub fromlist: *mut List,
    pub quals: *mut Node,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OnConflictExpr {
    pub type_: NodeTag,
    pub action: c_uint,
    pub arbiter_elems: *mut List,
    pub arbiter_where: *mut Node,
    pub constraint: Oid,
    pub on_conflict_set: *mut List,
    pub on_conflict_where: *mut Node,
    pub excl_rel_index: c_int,
    pub excl_rel_tlist: *mut List,
}

/// `IntoClause` carries the target of a `SELECT INTO` / `CREATE TABLE AS` /
/// `CREATE MATERIALIZED VIEW`. Its `viewQuery` member is the materialized
/// view's `Query`, which copyfuncs/equalfuncs traverse, so it is a real
/// `Node*` rather than an opaque-seam pointer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IntoClause {
    pub type_: NodeTag,
    pub rel: *mut RangeVar,
    pub col_names: *mut List,
    pub access_method: *mut c_char,
    pub options: *mut List,
    pub on_commit: OnCommitAction,
    pub table_space_name: *mut c_char,
    /// materialized view's SELECT query (`struct Query *`). copyfuncs/equalfuncs
    /// DO traverse this pointee, so it is modelled as a real `Node*` (the `Query`
    /// it points at is recovered via its `NodeTag`), not behind the opaque seam.
    pub view_query: *mut Node,
    pub skip_data: bool,
}

// ---------------------------------------------------------------------------
// Compile-time layout asserts for representative structs.
//
// Every Expr subtype must begin with the Expr header at offset 0 so the future
// copy/equal layers can recover the node tag via `((Expr *) node)->type`. The
// NodeTag-headed nodes must likewise carry their tag at offset 0. We also pin a
// couple of total sizes against the 64-bit ABI as a tripwire against silent
// field-width or padding drift.
// ---------------------------------------------------------------------------

const _: () = {
    // Expr header at offset 0 for representative Expr subtypes.
    assert!(core::mem::offset_of!(SubscriptingRef, xpr) == 0);
    assert!(core::mem::offset_of!(RowCompareExpr, xpr) == 0);
    assert!(core::mem::offset_of!(SubPlan, xpr) == 0);
    assert!(core::mem::offset_of!(JsonExpr, xpr) == 0);
    assert!(core::mem::offset_of!(JsonConstructorExpr, xpr) == 0);
    assert!(core::mem::offset_of!(XmlExpr, xpr) == 0);
    assert!(core::mem::offset_of!(SQLValueFunction, xpr) == 0);
    assert!(core::mem::offset_of!(CoerceToDomainValue, xpr) == 0);
    assert!(core::mem::offset_of!(ConvertRowtypeExpr, xpr) == 0);
    assert!(core::mem::offset_of!(FieldSelect, xpr) == 0);
    // NodeTag header at offset 0 for the non-Expr nodes.
    assert!(core::mem::offset_of!(MergeAction, type_) == 0);
    assert!(core::mem::offset_of!(JsonFormat, type_) == 0);
    assert!(core::mem::offset_of!(JsonReturning, type_) == 0);
    assert!(core::mem::offset_of!(JsonValueExpr, type_) == 0);
    assert!(core::mem::offset_of!(JsonBehavior, type_) == 0);
    assert!(core::mem::offset_of!(IntoClause, type_) == 0);
    // IntoClause.viewQuery is a traversed `Node*` (one pointer), not a seam;
    // pin its width so the promotion off the OpaqueNode seam stays ABI-neutral.
    assert!(core::mem::size_of::<*mut Node>() == core::mem::size_of::<usize>());
    // Abstract JsonTablePlan base sits at offset 0 of the concrete plans, so
    // the shared NodeTag header is recoverable.
    assert!(core::mem::offset_of!(JsonTablePathScan, plan) == 0);
    assert!(core::mem::offset_of!(JsonTableSiblingJoin, plan) == 0);
    // Size tripwires (x86-64 / aarch64 SysV layout, pointers + 8-aligned).
    assert!(core::mem::size_of::<RowCompareExpr>() == 48);
    assert!(core::mem::size_of::<SubscriptingRef>() == 56);
    assert!(core::mem::size_of::<WindowFuncRunCondition>() == 24);
    assert!(core::mem::size_of::<SQLValueFunction>() == 20);
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the primitive-expression family (the `Expr` nodes,
/// the SQL/JSON nodes, the join-tree nodes, and `RangeVar`/`IntoClause`).
///
/// `lib.rs` concatenates this slice with the other families' coverage into the
/// crate-wide [`crate::NODE_TYPES_COVERED`] table; new structs ported into this
/// module register themselves by adding an entry here.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    &[
        NodeTypeStatus {
            name: "RangeVar",
            tag: T_RangeVar,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "IntoClause",
            tag: T_IntoClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "Var",
            tag: T_Var,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "Const",
            tag: T_Const,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "Param",
            tag: T_Param,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "Aggref",
            tag: T_Aggref,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "GroupingFunc",
            tag: T_GroupingFunc,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WindowFunc",
            tag: T_WindowFunc,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WindowFuncRunCondition",
            tag: T_WindowFuncRunCondition,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MergeSupportFunc",
            tag: T_MergeSupportFunc,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SubscriptingRef",
            tag: T_SubscriptingRef,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "FuncExpr",
            tag: T_FuncExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "NamedArgExpr",
            tag: T_NamedArgExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "OpExpr",
            tag: T_OpExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "DistinctExpr",
            tag: T_DistinctExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "NullIfExpr",
            tag: T_NullIfExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ScalarArrayOpExpr",
            tag: T_ScalarArrayOpExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "BoolExpr",
            tag: T_BoolExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SubLink",
            tag: T_SubLink,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SubPlan",
            tag: T_SubPlan,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "AlternativeSubPlan",
            tag: T_AlternativeSubPlan,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "FieldSelect",
            tag: T_FieldSelect,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "FieldStore",
            tag: T_FieldStore,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RelabelType",
            tag: T_RelabelType,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CoerceViaIO",
            tag: T_CoerceViaIO,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ArrayCoerceExpr",
            tag: T_ArrayCoerceExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ConvertRowtypeExpr",
            tag: T_ConvertRowtypeExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CollateExpr",
            tag: T_CollateExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CaseExpr",
            tag: T_CaseExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CaseWhen",
            tag: T_CaseWhen,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CaseTestExpr",
            tag: T_CaseTestExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ArrayExpr",
            tag: T_ArrayExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RowExpr",
            tag: T_RowExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RowCompareExpr",
            tag: T_RowCompareExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CoalesceExpr",
            tag: T_CoalesceExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MinMaxExpr",
            tag: T_MinMaxExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SQLValueFunction",
            tag: T_SQLValueFunction,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "XmlExpr",
            tag: T_XmlExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonFormat",
            tag: T_JsonFormat,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonReturning",
            tag: T_JsonReturning,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonValueExpr",
            tag: T_JsonValueExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonConstructorExpr",
            tag: T_JsonConstructorExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonIsPredicate",
            tag: T_JsonIsPredicate,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonBehavior",
            tag: T_JsonBehavior,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonExpr",
            tag: T_JsonExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTablePath",
            tag: T_JsonTablePath,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTablePathScan",
            tag: T_JsonTablePathScan,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JsonTableSiblingJoin",
            tag: T_JsonTableSiblingJoin,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "NullTest",
            tag: T_NullTest,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "BooleanTest",
            tag: T_BooleanTest,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MergeAction",
            tag: T_MergeAction,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CoerceToDomain",
            tag: T_CoerceToDomain,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CoerceToDomainValue",
            tag: T_CoerceToDomainValue,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SetToDefault",
            tag: T_SetToDefault,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CurrentOfExpr",
            tag: T_CurrentOfExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "NextValueExpr",
            tag: T_NextValueExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "InferenceElem",
            tag: T_InferenceElem,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ReturningExpr",
            tag: T_ReturningExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "TargetEntry",
            tag: T_TargetEntry,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTblRef",
            tag: T_RangeTblRef,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "JoinExpr",
            tag: T_JoinExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "FromExpr",
            tag: T_FromExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "OnConflictExpr",
            tag: T_OnConflictExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
    ]
}
