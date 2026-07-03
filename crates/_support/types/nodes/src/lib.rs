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
pub use jointype::JoinType;
pub use list::{IntList, List, ListFlavor, NodeList, OidList, XidList};
pub use node_tree::{BitString, Boolean, Float, Integer, Node, NodeMut, NodeVariant, String};
pub use nodes_enums::{CmdType, LimitOption, LockClauseStrength, LockWaitPolicy};
pub use parsenodes::{
    AclMode, Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry, RangeTblFunction,
    RowMarkClause, SetOperation,
};
pub use plannodes::{Plan, PlanVariant, PlannedStmt, Result};
pub use plannodes::{BitmapAnd, BitmapHeapScan, BitmapIndexScan, BitmapOr};
pub use primnodes::{
    Alias, ArrayExpr, BoolExpr, BoolExprType, BoolTestType, BooleanTest, CoerceViaIO,
    CoercionForm, Const, DistinctExpr, FromExpr, FuncExpr, InferenceElem, JoinExpr, NullTest,
    NullTestType, OnConflictAction, OnConflictExpr, OpExpr, OverridingKind, Param, ParamKind,
    RangeTblRef, RangeVar, RelabelType, RowExpr, ScalarArrayOpExpr, SetToDefault, SubLink,
    SubLinkType, SubPlan, TargetEntry, Var, VarReturningType,
};
pub use plannodes::ModifyTable;
pub use rawnodes::{
    A_Const, A_Expr, A_Expr_Kind, A_Star, AlterSeqStmt, CollateClause, ColumnRef, CreateSeqStmt,
    DeleteStmt, DistinctClause, FuncCall, IndexElem, IndexStmt, InferClause, InsertStmt,
    LockingClause, OnConflictClause, ParamRef, RangeFunction, RawStmt, ResTarget, ReturningClause,
    SelectStmt, SortBy, SortByDir, SortByNulls, TypeCast, TypeName, UpdateStmt, ValUnion,
};
pub use tags::NodeTag;

#[cfg(test)]
mod bms_c_vectors;
#[cfg(test)]
mod tests;
