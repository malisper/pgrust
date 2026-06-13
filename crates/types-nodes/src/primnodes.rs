//! Primitive expression-node vocabulary (nodes/primnodes.h), trimmed.

use mcx::{alloc_in, Mcx, PgBox};
use types_core::primitive::{AttrNumber, Index, Oid};
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

/// `Const` (nodes/primnodes.h), trimmed to the fields consumers read. `IsA(node,
/// Const)` is `matches!(expr, Expr::Const(_))`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Const {
    /// `Oid consttype` — pg_type OID of the constant's type.
    pub consttype: Oid,
    /// `int32 consttypmod` — typmod value, or -1.
    pub consttypmod: i32,
    /// `Oid constcollid` — collation, or InvalidOid.
    pub constcollid: Oid,
    /// `int constlen` — length in bytes, or -1 for variable-length.
    pub constlen: i32,
    /// `Datum constvalue` — the constant's value (undefined when `constisnull`).
    pub constvalue: types_datum::datum::Datum,
    /// `bool constisnull` — whether the constant is NULL.
    pub constisnull: bool,
    /// `bool constbyval` — whether the value is passed by value.
    pub constbyval: bool,
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
