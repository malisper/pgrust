//! Primitive expression-node vocabulary (nodes/primnodes.h), trimmed.

use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_datum::Datum;
use types_error::PgResult;

/// `TableFuncType` (nodes/primnodes.h) — which table-producer function a
/// `TableFunc` node describes. Values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TableFuncType {
    /// XMLTABLE.
    TFT_XMLTABLE = 0,
    /// JSON_TABLE.
    TFT_JSON_TABLE = 1,
}

impl Default for TableFuncType {
    fn default() -> Self {
        TableFuncType::TFT_XMLTABLE
    }
}

pub use TableFuncType::{TFT_JSON_TABLE, TFT_XMLTABLE};

/// `TableFunc` (nodes/primnodes.h) — node for a table function such as
/// `XMLTABLE` and `JSON_TABLE`. Trimmed to the fields the executor node
/// consumes (the planner-only `plan`, `location`, and `query_jumble`-related
/// fields land with their first reader, per docs/types.md rule 3).
///
/// The list children are context-allocated (the parse/plan tree lives in a
/// memory context); the executor reads this read-only at `ExecInit` time.
#[derive(Debug, Default)]
pub struct TableFunc<'mcx> {
    /// `TableFuncType functype` — XMLTABLE or JSON_TABLE.
    pub functype: TableFuncType,
    /// `List *ns_uris` — namespace URI expressions.
    pub ns_uris: Option<PgVec<'mcx, PgBox<'mcx, Expr>>>,
    /// `List *ns_names` — namespace names, or `None` entries for the DEFAULT
    /// namespace (the C `String *` element being NULL).
    pub ns_names: Option<PgVec<'mcx, Option<PgString<'mcx>>>>,
    /// `Node *docexpr` — input document expression.
    pub docexpr: Option<PgBox<'mcx, Expr>>,
    /// `Node *rowexpr` — row filter expression.
    pub rowexpr: Option<PgBox<'mcx, Expr>>,
    /// `List *colnames` — column names (list of String).
    pub colnames: Option<PgVec<'mcx, PgString<'mcx>>>,
    /// `List *coltypes` — OID list of column type OIDs.
    pub coltypes: Option<PgVec<'mcx, Oid>>,
    /// `List *coltypmods` — integer list of column typmods.
    pub coltypmods: Option<PgVec<'mcx, i32>>,
    /// `List *colcollations` — OID list of column collation OIDs.
    pub colcollations: Option<PgVec<'mcx, Oid>>,
    /// `List *colexprs` — column filter expressions (NULL elements allowed).
    pub colexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr>>>>,
    /// `List *coldefexprs` — column default expressions (NULL elements
    /// allowed).
    pub coldefexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr>>>>,
    /// `List *colvalexprs` — JSON_TABLE column value expressions.
    pub colvalexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr>>>>,
    /// `List *passingvalexprs` — JSON_TABLE PASSING argument expressions.
    pub passingvalexprs: Option<PgVec<'mcx, PgBox<'mcx, Expr>>>,
    /// `Bitmapset *notnulls` — nullability flag for each output column.
    pub notnulls: Option<PgBox<'mcx, crate::bitmapset::Bitmapset<'mcx>>>,
    /// `int ordinalitycol` — counts from 0; -1 if none specified.
    pub ordinalitycol: i32,
}

impl TableFunc<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TableFunc<'b>> {
        Ok(TableFunc {
            functype: self.functype,
            ns_uris: clone_expr_list(&self.ns_uris, mcx)?,
            ns_names: match &self.ns_names {
                Some(v) => {
                    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
                    for n in v.iter() {
                        out.push(match n {
                            Some(s) => Some(s.clone_in(mcx)?),
                            None => None,
                        });
                    }
                    Some(out)
                }
                None => None,
            },
            docexpr: clone_opt_expr(&self.docexpr, mcx)?,
            rowexpr: clone_opt_expr(&self.rowexpr, mcx)?,
            colnames: match &self.colnames {
                Some(v) => {
                    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
                    for s in v.iter() {
                        out.push(s.clone_in(mcx)?);
                    }
                    Some(out)
                }
                None => None,
            },
            coltypes: clone_copy_list(&self.coltypes, mcx)?,
            coltypmods: clone_copy_list(&self.coltypmods, mcx)?,
            colcollations: clone_copy_list(&self.colcollations, mcx)?,
            colexprs: clone_opt_expr_list(&self.colexprs, mcx)?,
            coldefexprs: clone_opt_expr_list(&self.coldefexprs, mcx)?,
            colvalexprs: clone_opt_expr_list(&self.colvalexprs, mcx)?,
            passingvalexprs: clone_expr_list(&self.passingvalexprs, mcx)?,
            notnulls: match &self.notnulls {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            ordinalitycol: self.ordinalitycol,
        })
    }
}

fn clone_opt_expr<'b>(
    e: &Option<PgBox<'_, Expr>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgBox<'b, Expr>>> {
    match e {
        Some(b) => Ok(Some(alloc_in(mcx, (**b).clone())?)),
        None => Ok(None),
    }
}

fn clone_expr_list<'b>(
    list: &Option<PgVec<'_, PgBox<'_, Expr>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, PgBox<'b, Expr>>>> {
    match list {
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for e in v.iter() {
                out.push(alloc_in(mcx, (**e).clone())?);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

fn clone_opt_expr_list<'b>(
    list: &Option<PgVec<'_, Option<PgBox<'_, Expr>>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, Option<PgBox<'b, Expr>>>>> {
    match list {
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for e in v.iter() {
                out.push(clone_opt_expr(e, mcx)?);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

fn clone_copy_list<'b, T: Copy>(
    list: &Option<PgVec<'_, T>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, T>>> {
    match list {
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for x in v.iter() {
                out.push(*x);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
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
