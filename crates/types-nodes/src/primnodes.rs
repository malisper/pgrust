//! Primitive expression-node vocabulary (nodes/primnodes.h), trimmed.

use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_datum::Datum;
use types_error::PgResult;

/// `SubLinkType` (nodes/primnodes.h) — the kind of sub-select. Values match the
/// C enumerator order exactly (`#[repr(i32)]`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum SubLinkType {
    /// `EXISTS_SUBLINK`.
    Exists = 0,
    /// `ALL_SUBLINK`.
    All = 1,
    /// `ANY_SUBLINK`.
    Any = 2,
    /// `ROWCOMPARE_SUBLINK`.
    RowCompare = 3,
    /// `EXPR_SUBLINK`.
    Expr = 4,
    /// `MULTIEXPR_SUBLINK`.
    MultiExpr = 5,
    /// `ARRAY_SUBLINK`.
    Array = 6,
    /// `CTE_SUBLINK` (for SubPlans only).
    Cte = 7,
}

/// `SubPlan` (nodes/primnodes.h) — an executable sub-select expression node.
/// Trimmed to the fields the executor (`nodeSubplan.c`) consumes; the cost
/// fields and planner metadata are carried because the C struct is a plain data
/// node the executor reads.
#[derive(Debug)]
pub struct SubPlan<'mcx> {
    /// `SubLinkType subLinkType`.
    pub subLinkType: SubLinkType,
    /// `Node *testexpr` — OpExpr or RowCompareExpr expression tree.
    pub testexpr: Option<PgBox<'mcx, Expr>>,
    /// `List *paramIds` — IDs of Params embedded in `testexpr`.
    pub paramIds: PgVec<'mcx, i32>,
    /// `int plan_id` — index (from 1) in `PlannedStmt.subplans`.
    pub plan_id: i32,
    /// `char *plan_name` — a name assigned during planning.
    pub plan_name: Option<PgString<'mcx>>,
    /// `Oid firstColType` — type of first column of subplan result.
    pub firstColType: Oid,
    /// `int32 firstColTypmod` — typmod of first column of subplan result.
    pub firstColTypmod: i32,
    /// `Oid firstColCollation` — collation of first column of subplan result.
    pub firstColCollation: Oid,
    /// `bool useHashTable` — store subselect output in a hash table.
    pub useHashTable: bool,
    /// `bool unknownEqFalse` — okay to return FALSE when spec result is
    /// UNKNOWN.
    pub unknownEqFalse: bool,
    /// `bool parallel_safe`.
    pub parallel_safe: bool,
    /// `List *setParam` — param IDs the initplan/MULTIEXPR subqueries set.
    pub setParam: PgVec<'mcx, i32>,
    /// `List *parParam` — indices of input Params from the parent plan.
    pub parParam: PgVec<'mcx, i32>,
    /// `List *args` — exprs to pass as parParam values.
    pub args: PgVec<'mcx, PgBox<'mcx, Expr>>,
    /// `Cost startup_cost` — one-time setup cost.
    pub startup_cost: f64,
    /// `Cost per_call_cost` — cost for each subplan evaluation.
    pub per_call_cost: f64,
}

/// `Var` (nodes/primnodes.h), trimmed to the fields ports consume.
#[derive(Clone, Copy, Debug, Default)]
pub struct Var {
    /// `int varno` — index of this var's relation in the range table.
    pub varno: i32,
    /// `AttrNumber varattno` — attribute number, or 0 for whole-row.
    pub varattno: AttrNumber,
    /// `Oid vartype` — pg_type OID of this var's type.
    pub vartype: Oid,
    /// `int32 vartypmod` — pg_attribute typmod value.
    pub vartypmod: i32,
    /// `Index varlevelsup` — subplan levels up; 0 = current query level.
    pub varlevelsup: Index,
}

/// `Const` (nodes/primnodes.h), trimmed to the fields ports consume.
#[derive(Clone, Copy, Debug, Default)]
pub struct Const {
    /// `Oid consttype` — pg_type OID of the constant's type.
    pub consttype: Oid,
    /// `Datum constvalue` — the constant's value (undefined if `constisnull`).
    pub constvalue: Datum,
    /// `bool constisnull` — whether the constant is null.
    pub constisnull: bool,
}

/// `OpExpr` (nodes/primnodes.h), trimmed to the fields ports consume.
#[derive(Clone, Debug)]
pub struct OpExpr {
    /// `Oid opno` — PG_OPERATOR OID of the operator.
    pub opno: Oid,
    /// `List *args` — arguments to the operator (two, for a mergeclause
    /// `leftexpr = rightexpr`).
    pub args: Vec<Expr>,
}

/// Expression-tree node (`Expr *` in C). The `NodeTag` is the enum
/// discriminant (`IsA(node, Var)` is a match on the variant). Variants are
/// added as units consuming them are ported.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Expr {
    /// `T_Var`.
    Var(Var),
    /// `T_Const`.
    Const(Const),
    /// `T_OpExpr`.
    OpExpr(OpExpr),
}

/// `TargetEntry` (nodes/primnodes.h), trimmed.
#[derive(Debug, Default)]
pub struct TargetEntry<'mcx> {
    /// `Expr *expr` — expression to evaluate.
    pub expr: Option<PgBox<'mcx, Expr>>,
    /// `bool resjunk` — set to true to eliminate the attribute from the
    /// final target list.
    pub resjunk: bool,
}

impl TargetEntry<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TargetEntry<'b>> {
        Ok(TargetEntry {
            expr: match &self.expr {
                Some(e) => Some(alloc_in(mcx, (**e).clone())?),
                None => None,
            },
            resjunk: self.resjunk,
        })
    }
}
