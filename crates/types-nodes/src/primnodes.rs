//! Primitive expression-node vocabulary (nodes/primnodes.h), trimmed.

use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_datum::Datum;
use types_error::PgResult;

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

/// `ScalarArrayOpExpr` (nodes/primnodes.h) — `scalar op ANY/ALL (array)`,
/// trimmed to the fields ports consume (the TID-scan node reads only `args`,
/// via `linitial`/`lsecond`).
#[derive(Clone, Debug)]
pub struct ScalarArrayOpExpr {
    /// `Oid opno` — PG_OPERATOR OID of the operator.
    pub opno: Oid,
    /// `bool useOr` — true for ANY, false for ALL.
    pub useOr: bool,
    /// `List *args` — the scalar and array operands.
    pub args: Vec<Expr>,
}

/// `CurrentOfExpr` (nodes/primnodes.h) — `WHERE CURRENT OF cursor`, trimmed.
#[derive(Clone, Debug, Default)]
pub struct CurrentOfExpr {
    /// `Index cvarno` — RT index of target relation.
    pub cvarno: Index,
    /// `char *cursor_name` — name of referenced cursor, or `None` (C NULL).
    pub cursor_name: Option<alloc::string::String>,
    /// `int cursor_param` — refcursor parameter number, or 0.
    pub cursor_param: i32,
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
    /// `T_ScalarArrayOpExpr`.
    ScalarArrayOpExpr(ScalarArrayOpExpr),
    /// `T_CurrentOfExpr`.
    CurrentOfExpr(CurrentOfExpr),
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
