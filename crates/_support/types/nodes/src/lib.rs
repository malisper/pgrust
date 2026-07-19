#![no_std]

extern crate alloc;

// Out of line: PgError is ~0.5 KB by value; inline construction inflates every
// allocating caller's frame and register pressure.
#[cold]
#[inline(never)]
pub(crate) fn oom(mcx: mcx::Mcx<'_>, request: usize) -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(mcx.oom(request))
}

pub mod bitmapset;
pub mod equal;
pub mod fdw;
pub mod jointype;
pub mod list;
pub mod node_tree;
pub mod nodes_enums;
pub mod parsenodes;
pub mod plannodes;
pub mod primnodes;
pub mod rawnodes;
pub mod supportnodes;
mod tags;

pub use bitmapset::{bitmapword, Bitmapset, BmsComparison, BmsMembership, BITS_PER_BITMAPWORD};
pub use equal::{equal, equal_opt, NodeEqual};
pub use fdw::{FdwExplainFlags, FdwExplainProp, FdwKind, FdwRoutine, NUM_FDW_KINDS};
pub use jointype::JoinType;
// pg_stat_statements scribbles PlannedStmt.queryId in place (C parity);
// re-exported so constructors need no direct types_storage edge.
pub use types_storage::storage::SyncCell;
pub use list::{IntList, List, ListFlavor, NodeList, OidList, OptNodeList, XidList};
pub use node_tree::{BitString, Boolean, Float, Integer, Node, NodeMut, NodeVariant, String};
pub use nodes_enums::{CmdType, LimitOption, LockClauseStrength, LockWaitPolicy};
pub use parsenodes::{
    AclMode, DefineStmt, Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry,
    RangeTblFunction, RowMarkClause, SetOperation, TableSampleClause,
};
pub use plannodes::{AppendRelInfo, Plan, PlanVariant, PlannedStmt, Result, TidRangeScan, TidScan};
pub use plannodes::{BitmapAnd, BitmapHeapScan, BitmapIndexScan, BitmapOr};
pub use primnodes::{
    Alias, ArrayCoerceExpr, ArrayExpr, BoolExpr, BoolExprType, BoolTestType, BooleanTest,
    CaseTestExpr, CoerceToDomain, CoerceToDomainValue, CoerceViaIO, CoercionForm, CollateExpr,
    Const, ConvertRowtypeExpr, CurrentOfExpr, DistinctExpr, FieldSelect, FieldStore, FromExpr, FuncExpr, InferenceElem, JoinExpr, MergeAction,
    MergeMatchKind, NamedArgExpr, NullIfExpr, NullTest, NullTestType,
    JsonBehavior, JsonBehaviorType, JsonConstructorExpr, JsonConstructorType, JsonEncoding,
    JsonExpr, JsonExprOp, JsonFormat, JsonFormatType, JsonIsPredicate, JsonReturning,
    JsonValueExpr, JsonValueType, JsonWrapper,
    OnConflictAction, OnConflictExpr, OpExpr, OverridingKind, Param, ParamKind, PlaceHolderVar, RangeTblRef,
    RangeVar, RelabelType, ReturningExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp, ScalarArrayOpExpr,
    SetToDefault, SubLink, SubLinkType, SubPlan, SubscriptingRef, TableFunc, TableFuncType,
    TargetEntry, Var, VarReturningType, XmlExpr, XmlExprOp, XmlOptionType,
};
pub use plannodes::ModifyTable;
pub use rawnodes::{
    A_ArrayExpr, A_Const, A_Expr, A_Expr_Kind, A_Indices, A_Indirection, A_Star, AlterEnumStmt, CompositeTypeStmt,
    AlterSeqStmt, CallStmt, CollateClause, ColumnRef,
    CreateDomainStmt, CreateEnumStmt, CreateSeqStmt, DeleteStmt, DistinctClause, FuncCall, IndexElem, IndexStmt,
    InferClause, InsertStmt, JsonAggConstructor, JsonArgument, JsonArrayAgg,
    JsonArrayConstructor, JsonArrayQueryConstructor, JsonFuncExpr, JsonKeyValue, JsonObjectAgg,
    JsonObjectConstructor, JsonOutput, JsonParseExpr, JsonQuotes, JsonScalarExpr,
    JsonSerializeExpr, LockingClause, MergeStmt, MergeWhenClause, OnConflictClause, PLAssignStmt, ParamRef, RangeFunction, RangeTableFunc,
    RangeTableFuncCol, RangeTableSample, RawStmt,
    ResTarget, ReturningClause, SelectStmt, SortBy, SortByDir, SortByNulls, TypeCast, TypeName,
    UpdateStmt, ValUnion, XmlSerialize,
};
pub use tags::NodeTag;

#[cfg(test)]
mod bms_c_vectors;
#[cfg(test)]
mod tests;
