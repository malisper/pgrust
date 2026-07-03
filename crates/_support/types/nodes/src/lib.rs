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
pub mod jointype;
pub mod list;
pub mod node_tree;
pub mod nodes_enums;
pub mod parsenodes;
pub mod plannodes;
pub mod primnodes;
pub mod rawnodes;
mod tags;

pub use bitmapset::{bitmapword, Bitmapset, BmsComparison, BmsMembership, BITS_PER_BITMAPWORD};
pub use jointype::JoinType;
pub use list::{IntList, List, ListFlavor, NodeList, OidList, XidList};
pub use node_tree::{BitString, Boolean, Float, Integer, Node, NodeMut, NodeVariant, String};
pub use nodes_enums::{CmdType, LimitOption};
pub use parsenodes::{
    AclMode, Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry, SetOperation,
};
pub use plannodes::{Plan, PlanVariant, PlannedStmt, Result};
pub use primnodes::{
    Alias, BoolExpr, BoolExprType, CoercionForm, Const, FromExpr, FuncExpr, NullTest,
    NullTestType, OpExpr, OverridingKind, Param, ParamKind, RangeTblRef, RangeVar, RelabelType,
    TargetEntry, Var, VarReturningType,
};
pub use rawnodes::{
    A_Const, A_Expr, A_Expr_Kind, A_Star, ColumnRef, DistinctClause, FuncCall, ParamRef, RawStmt,
    ResTarget, SelectStmt, SortBy, SortByDir, SortByNulls, TypeCast, TypeName, ValUnion,
};
pub use tags::NodeTag;

#[cfg(test)]
mod bms_c_vectors;
#[cfg(test)]
mod tests;
