//! Primitive expression-node vocabulary (nodes/primnodes.h).
//!
//! The `Expr` enum is the keystone for the `backend-executor-execExpr` /
//! `backend-executor-execExprInterp` cycle: it carries one variant per C
//! executable-expression node type, so `ExecInitExprRec`'s ~40-arm switch over
//! the node tag can be written directly as a `match` over this enum. Each
//! struct mirrors the C node (`nodes/primnodes.h`, PostgreSQL 18) field-for-
//! field, trimmed of the purely-planner / query-jumble-only fields no reader
//! consumes (docs/types.md rule 3).
//!
//! `ParseLoc location` is carried field-for-field on every C node that has it:
//! the parser (`parse_func`/`parse_oper`/`parse_expr`/`parse_target`/
//! `parse_clause`) records the source token position in `location` on every
//! node it builds, and `parser_errposition`/`exprLocation` (nodeFuncs.c) read it
//! back to point error messages at the offending token. Dropping it would
//! diverge from C error output, so the earlier "trim location" exception
//! (docs/types.md rule 3) is reversed for these nodes.
//!
//! The `Expr` tree is the read-only parse/plan tree the executor walks at
//! `ExecInit` time; in C it lives in a memory context and is never mutated
//! during evaluation. Child expressions are owned `Box<Expr<'mcx>>` and child lists
//! are `Vec<…>` on the global allocator — matching the precedent already set
//! by `OpExpr`/`ScalarArrayOpExpr` (which carry `Vec<Expr<'mcx>>`) and keeping `Expr`
//! free of a lifetime parameter so the (non-exhaustive) enum stays additive:
//! existing consumers' wildcard arms keep compiling.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid, ParseLoc};
use types_tuple::heaptuple::Datum;
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
    pub testexpr: Option<PgBox<'mcx, Expr<'mcx>>>,
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
    pub args: PgVec<'mcx, PgBox<'mcx, Expr<'mcx>>>,
    /// `Cost startup_cost` — one-time setup cost.
    pub startup_cost: f64,
    /// `Cost per_call_cost` — cost for each subplan evaluation.
    pub per_call_cost: f64,
}

impl SubPlan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over a `SubPlan`). The `testexpr`
    /// and `args` carry `Expr` children, which deep-copy through
    /// [`Expr::clone_in`] (a plain `.clone()` would panic on
    /// `Aggref`/`SubLink`/`SubPlan` children); the integer/cost fields copy
    /// directly. The executable subplan tree itself lives in
    /// `PlannedStmt.subplans` (addressed by `plan_id`), so only the node's own
    /// fields are copied here.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SubPlan<'b>> {
        let testexpr = match &self.testexpr {
            Some(e) => Some(alloc_in(mcx, e.clone_in(mcx)?)?),
            None => None,
        };
        let mut paramIds = mcx::vec_with_capacity_in(mcx, self.paramIds.len())?;
        for x in self.paramIds.iter() {
            paramIds.push(*x);
        }
        let plan_name = match &self.plan_name {
            Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        let mut setParam = mcx::vec_with_capacity_in(mcx, self.setParam.len())?;
        for x in self.setParam.iter() {
            setParam.push(*x);
        }
        let mut parParam = mcx::vec_with_capacity_in(mcx, self.parParam.len())?;
        for x in self.parParam.iter() {
            parParam.push(*x);
        }
        let mut args = mcx::vec_with_capacity_in(mcx, self.args.len())?;
        for e in self.args.iter() {
            args.push(alloc_in(mcx, e.clone_in(mcx)?)?);
        }
        Ok(SubPlan {
            subLinkType: self.subLinkType,
            testexpr,
            paramIds,
            plan_id: self.plan_id,
            plan_name,
            firstColType: self.firstColType,
            firstColTypmod: self.firstColTypmod,
            firstColCollation: self.firstColCollation,
            useHashTable: self.useHashTable,
            unknownEqFalse: self.unknownEqFalse,
            parallel_safe: self.parallel_safe,
            setParam,
            parParam,
            args,
            startup_cost: self.startup_cost,
            per_call_cost: self.per_call_cost,
        })
    }
}

/// `OnCommitAction` (nodes/primnodes.h) — what to do at transaction commit
/// for a temporary table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum OnCommitAction {
    /// `ONCOMMIT_NOOP` — no ON COMMIT clause (do nothing).
    ONCOMMIT_NOOP = 0,
    /// `ONCOMMIT_PRESERVE_ROWS` — ON COMMIT PRESERVE ROWS (do nothing).
    ONCOMMIT_PRESERVE_ROWS = 1,
    /// `ONCOMMIT_DELETE_ROWS` — ON COMMIT DELETE ROWS.
    ONCOMMIT_DELETE_ROWS = 2,
    /// `ONCOMMIT_DROP` — ON COMMIT DROP.
    ONCOMMIT_DROP = 3,
}

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
    pub ns_uris: Option<PgVec<'mcx, PgBox<'mcx, Expr<'mcx>>>>,
    /// `List *ns_names` — namespace names, or `None` entries for the DEFAULT
    /// namespace (the C `String *` element being NULL).
    pub ns_names: Option<PgVec<'mcx, Option<PgString<'mcx>>>>,
    /// `Node *docexpr` — input document expression.
    pub docexpr: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `Node *rowexpr` — row filter expression.
    pub rowexpr: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `List *colnames` — column names (list of String).
    pub colnames: Option<PgVec<'mcx, PgString<'mcx>>>,
    /// `List *coltypes` — OID list of column type OIDs.
    pub coltypes: Option<PgVec<'mcx, Oid>>,
    /// `List *coltypmods` — integer list of column typmods.
    pub coltypmods: Option<PgVec<'mcx, i32>>,
    /// `List *colcollations` — OID list of column collation OIDs.
    pub colcollations: Option<PgVec<'mcx, Oid>>,
    /// `List *colexprs` — column filter expressions (NULL elements allowed).
    pub colexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr<'mcx>>>>>,
    /// `List *coldefexprs` — column default expressions (NULL elements
    /// allowed).
    pub coldefexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr<'mcx>>>>>,
    /// `List *colvalexprs` — JSON_TABLE column value expressions.
    pub colvalexprs: Option<PgVec<'mcx, Option<PgBox<'mcx, Expr<'mcx>>>>>,
    /// `List *passingvalexprs` — JSON_TABLE PASSING argument expressions.
    pub passingvalexprs: Option<PgVec<'mcx, PgBox<'mcx, Expr<'mcx>>>>,
    /// `Bitmapset *notnulls` — nullability flag for each output column.
    pub notnulls: Option<PgBox<'mcx, crate::bitmapset::Bitmapset<'mcx>>>,
    /// `Node *plan` — planner-internal field; usually `NULL` (only set during
    /// planning of a `JSON_TABLE`/`XMLTABLE`). Modeled as a generic `Node`.
    pub plan: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `int ordinalitycol` — counts from 0; -1 if none specified.
    pub ordinalitycol: i32,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
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
            plan: match &self.plan {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            ordinalitycol: self.ordinalitycol,
            location: self.location,
        })
    }
}

fn clone_opt_expr<'b>(
    e: &Option<PgBox<'_, Expr>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgBox<'b, Expr<'b>>>> {
    match e {
        // Deep copy via `Expr::clone_in` (a shallow `.clone()` panics on
        // `Aggref`/`SubLink`/`SubPlan` children).
        Some(b) => Ok(Some(alloc_in(mcx, b.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

fn clone_expr_list<'b>(
    list: &Option<PgVec<'_, PgBox<'_, Expr>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, PgBox<'b, Expr<'b>>>>> {
    match list {
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for e in v.iter() {
                out.push(alloc_in(mcx, e.clone_in(mcx)?)?);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

fn clone_opt_expr_list<'b>(
    list: &Option<PgVec<'_, Option<PgBox<'_, Expr>>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, Option<PgBox<'b, Expr<'b>>>>>> {
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

/// `JsonTablePathScan` (nodes/primnodes.h) — a `JsonTablePlan` leaf that
/// evaluates a JSON path against the document row and supplies the source row for
/// the columns it covers. Built by `makeJsonTablePathScan` (parse_jsontable.c).
///
/// The C `JsonTablePlan plan;` abstract base (carrying only the `NodeTag`) is
/// implicit here: this is a `Node`-enum variant, so its tag IS its identity. The
/// trivial C `JsonTablePath` wrapper (`Const *value; char *name;`) is collapsed
/// into the `path` (the `Const` jsonpath value node) + `name` fields, exactly the
/// minimal faithful shape the executor reads.
#[derive(Debug)]
pub struct JsonTablePathScan<'mcx> {
    /// `JsonTablePath *path` (collapsed) → `Const *value` — the jsonpath value,
    /// a `Const` of type `jsonpath` built by `make_const` over the
    /// `DirectFunctionCall1(jsonpath_in, ...)` image.
    pub path: PgBox<'mcx, crate::nodes::Node<'mcx>>,
    /// `JsonTablePath *path` (collapsed) → `char *name` — the path name.
    pub name: Option<PgString<'mcx>>,
    /// `bool errorOnError` — ERROR/EMPTY ON ERROR behavior; only significant in
    /// the plan for the top-level path.
    pub errorOnError: bool,
    /// `JsonTablePlan *child` — plan(s) for nested columns, if any.
    pub child: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `int colMin` — 0-based index in `TableFunc.colvalexprs` of the first
    /// column covered by this plan (-1 if all columns are nested).
    pub colMin: i32,
    /// `int colMax` — 0-based index of the last column covered by this plan
    /// (-1 if all columns are nested).
    pub colMax: i32,
}

impl JsonTablePathScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonTablePathScan<'b>> {
        Ok(JsonTablePathScan {
            path: alloc_in(mcx, self.path.clone_in(mcx)?)?,
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            errorOnError: self.errorOnError,
            child: match &self.child {
                Some(c) => Some(alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            colMin: self.colMin,
            colMax: self.colMax,
        })
    }
}

/// `JsonTableSiblingJoin` (nodes/primnodes.h) — a `JsonTablePlan` that performs a
/// "sibling join" (a UNION of the row sets) of two nested-column plans. Built by
/// `makeJsonTableSiblingJoin` (parse_jsontable.c).
#[derive(Debug)]
pub struct JsonTableSiblingJoin<'mcx> {
    /// `JsonTablePlan *lplan` — the left child plan.
    pub lplan: PgBox<'mcx, crate::nodes::Node<'mcx>>,
    /// `JsonTablePlan *rplan` — the right child plan.
    pub rplan: PgBox<'mcx, crate::nodes::Node<'mcx>>,
}

impl JsonTableSiblingJoin<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonTableSiblingJoin<'b>> {
        Ok(JsonTableSiblingJoin {
            lplan: alloc_in(mcx, self.lplan.clone_in(mcx)?)?,
            rplan: alloc_in(mcx, self.rplan.clone_in(mcx)?)?,
        })
    }
}

/// `Relids` (nodes/bitmapset.h: `Bitmapset *`) for the lifetime-free expression
/// tree — a planner relation-id set carried by a [`Var`]/[`PlaceHolderVar`].
///
/// The canonical [`crate::bitmapset::Bitmapset`] is `'mcx`-lifetimed and not
/// `Clone`, so it cannot be embedded in the lifetime-free, `Clone`+`Default`
/// [`Expr`] tree without forcing an `'mcx` flag-day across every `Expr` consumer.
/// This is the lifetime-free planner analogue (same `Vec<u64>`-word storage as
/// `pathnodes::Bitmapset`, the planner-arena relids type): the empty set is
/// an empty `words` vector (`bms_is_empty`), matching the C `Bitmapset *` whose
/// `NULL`/empty pointer is the empty set. The `bms_*` algebra lives with the
/// owning bitmapset/relnode units; this carries only the word storage so the
/// optimizer can read/assign the relids of a `Var`/`PlaceHolderVar`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExprRelids {
    /// `bitmapword words[]` — the bit storage (empty = the empty set).
    pub words: Vec<u64>,
}

/// `Var` (nodes/primnodes.h), trimmed to the fields ports consume.
#[derive(Clone, Debug, Default)]
pub struct Var {
    /// `int varno` — index of this var's relation in the range table.
    pub varno: i32,
    /// `AttrNumber varattno` — attribute number, or 0 for whole-row.
    pub varattno: AttrNumber,
    /// `Oid vartype` — pg_type OID of this var's type.
    pub vartype: Oid,
    /// `int32 vartypmod` — pg_attribute typmod value.
    pub vartypmod: i32,
    /// `Oid varcollid` — OID of collation, or InvalidOid if none.
    ///
    /// Read/assigned by `exprCollation`/`exprSetCollation` (nodeFuncs.c).
    /// Added field-for-field vs primnodes.h (the keystone Expr expansion left
    /// the leaf trimmed); `Default` keeps `Var { .. }` construction additive.
    pub varcollid: Oid,
    /// `Bitmapset *varnullingrels` — RT indexes of outer joins that can replace
    /// this Var's value with null. The planner's `build_joinrel_tlist`
    /// (relnode.c) mutates this when forming a joinrel's targetlist. Empty in a
    /// normal Var. Carried as the lifetime-free [`ExprRelids`] so the `Var`
    /// stays embeddable in the lifetime-free [`Expr`] tree.
    pub varnullingrels: ExprRelids,
    /// `Index varlevelsup` — subplan levels up; 0 = current query level.
    pub varlevelsup: Index,
    /// `Index varnosyn` — syntactic relation index for ruleutils display,
    /// usually the same as `varno`. Set by `scanNSItemForColumn` /
    /// `expandNSItemVars` (parse_relation.c) from the nsitem's per-column data.
    /// Added field-for-field vs primnodes.h; `Default` is 0.
    pub varnosyn: Index,
    /// `AttrNumber varattnosyn` — syntactic attribute number for ruleutils
    /// display, usually the same as `varattno`. Set alongside `varnosyn`.
    /// Added field-for-field vs primnodes.h; `Default` is 0.
    pub varattnosyn: AttrNumber,
    /// `VarReturningType varreturningtype` — for a Var referencing the OLD/NEW
    /// pseudo-relations of a RETURNING list, whether it returns OLD or NEW (else
    /// `VAR_RETURNING_DEFAULT`). Read by `contain_vars_returning_old_or_new`
    /// (var.c). Added field-for-field vs primnodes.h (the keystone Expr
    /// expansion left the leaf trimmed); `Default` keeps `Var { .. }`
    /// construction additive.
    pub varreturningtype: VarReturningType,
    /// `ParseLoc location` — token location, or -1 if unknown. Read by
    /// `locate_var_of_level` (var.c) and preserved across join-alias flattening.
    /// Added field-for-field vs primnodes.h; `Default` is 0 — explicit
    /// constructors set -1 where C does.
    pub location: i32,
}

/// `Const` (nodes/primnodes.h), trimmed to the fields ports consume.
#[derive(Clone, Debug)]
pub struct Const<'mcx> {
    /// `Oid consttype` — pg_type OID of the constant's type.
    pub consttype: Oid,
    /// `int32 consttypmod` — typmod value, or -1.
    ///
    /// Read/assigned by `exprTypmod`/`applyRelabelType` (nodeFuncs.c). Added
    /// field-for-field vs primnodes.h; `Default` keeps construction additive.
    pub consttypmod: i32,
    /// `Oid constcollid` — collation, or InvalidOid if none.
    ///
    /// Read/assigned by `exprCollation`/`exprSetCollation`/`applyRelabelType`
    /// (nodeFuncs.c). Added field-for-field vs primnodes.h.
    pub constcollid: Oid,
    /// `int constlen` — typlen of the constant's type (the width of a fixed-
    /// length type, or -1/-2 for varlena/cstring). Copied from
    /// `get_typlenbyval` at construction. Read by `outDatum` (outfuncs.c) to
    /// serialize `constvalue`. Added field-for-field vs primnodes.h.
    pub constlen: i32,
    /// `Datum constvalue` — the constant's value (undefined if `constisnull`).
    ///
    /// A `Const` lives in its plan node's long-lived context (it is not
    /// per-tuple working state), so its value carries the `'static` lifetime —
    /// matching the `Box<SubPlan<'static>>` convention used elsewhere in the
    /// lifetime-free [`Expr`] enum.
    pub constvalue: Datum<'mcx>,
    /// `bool constisnull` — whether the constant is null.
    pub constisnull: bool,
    /// `bool constbyval` — whether the type is pass-by-value. Copied from
    /// `get_typlenbyval` at construction. Read by `outDatum` (outfuncs.c) to
    /// serialize `constvalue`. Added field-for-field vs primnodes.h.
    pub constbyval: bool,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

impl<'mcx> Default for Const<'mcx> {
    fn default() -> Self {
        Const {
            consttype: Default::default(),
            consttypmod: 0,
            constcollid: Default::default(),
            constlen: 0,
            constvalue: Datum::null(),
            constisnull: false,
            constbyval: false,
            location: -1,
        }
    }
}

/// `OpExpr` (nodes/primnodes.h) — expression node for an operator invocation.
#[derive(Clone, Debug, Default)]
pub struct OpExpr<'mcx> {
    /// `Oid opno` — PG_OPERATOR OID of the operator.
    pub opno: Oid,
    /// `Oid opfuncid` — PG_PROC OID of underlying function.
    pub opfuncid: Oid,
    /// `Oid opresulttype` — PG_TYPE OID of result value.
    pub opresulttype: Oid,
    /// `bool opretset` — true if operator returns set.
    pub opretset: bool,
    /// `Oid opcollid` — OID of collation of result.
    pub opcollid: Oid,
    /// `Oid inputcollid` — OID of collation that operator should use.
    pub inputcollid: Oid,
    /// `List *args` — arguments to the operator (1 or 2).
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

impl<'mcx> OpExpr<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). The derived `.clone()` is
    /// unsafe for an OpExpr whose `args` carry an owned-subtree `Expr` (a
    /// SubLink/SubPlan/Aggref), which panics; this routes each arg through
    /// [`Expr::clone_in`].
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<OpExpr<'b>> {
        clone_opexpr(self, mcx)
    }
}

/// `ScalarArrayOpExpr` (nodes/primnodes.h) — `scalar op ANY/ALL (array)`,
/// trimmed to the fields ports consume (the TID-scan node reads only `args`,
/// via `linitial`/`lsecond`).
#[derive(Clone, Debug, Default)]
pub struct ScalarArrayOpExpr<'mcx> {
    /// `Oid opno` — PG_OPERATOR OID of the operator.
    pub opno: Oid,
    /// `Oid opfuncid` — PG_PROC OID of comparison function.
    ///
    /// Set by `set_sa_opfuncid`/`fix_opfuncids` (nodeFuncs.c). Added
    /// field-for-field vs primnodes.h; `Default` keeps construction additive.
    pub opfuncid: Oid,
    /// `Oid hashfuncid` — PG_PROC OID of hash func, or `InvalidOid`. Set by the
    /// planner (`convert_saop_to_hashed_saop`) when the SAOP is evaluated via a
    /// hash table; the executor builds/probes the table with this hash function
    /// (`EEOP_HASHED_SCALARARRAYOP`). Added field-for-field vs primnodes.h.
    pub hashfuncid: Oid,
    /// `Oid negfuncid` — PG_PROC OID of the negator of `opfuncid`, or
    /// `InvalidOid`. For hashed NOT IN this is the equality function the hash
    /// table builds/probes with (`opno`/`opfuncid` stay the `<>` operator and
    /// are unused at execution). Added field-for-field vs primnodes.h.
    pub negfuncid: Oid,
    /// `bool useOr` — true for ANY, false for ALL.
    pub useOr: bool,
    /// `Oid inputcollid` — OID of collation that operator should use.
    ///
    /// Read/assigned by `exprInputCollation`/`exprSetInputCollation`
    /// (nodeFuncs.c). Added field-for-field vs primnodes.h.
    pub inputcollid: Oid,
    /// `List *args` — the scalar and array operands.
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by
    /// `make_scalar_array_op` (parse_oper.c) and read by `exprLocation`
    /// (nodeFuncs.c). Added field-for-field vs primnodes.h.
    pub location: i32,
}

/// `CurrentOfExpr` (nodes/primnodes.h) — the `WHERE CURRENT OF cursor`
/// expression. Either `cursor_name` (a literal cursor name) or `cursor_param`
/// (a refcursor parameter number, > 0) identifies the cursor.
#[derive(Clone, Debug, Default)]
pub struct CurrentOfExpr {
    /// `Index cvarno` — RT index of target relation.
    pub cvarno: Index,
    /// `char *cursor_name` — name of referenced cursor, or `None` (C `NULL`).
    pub cursor_name: Option<alloc::string::String>,
    /// `int cursor_param` — refcursor parameter number, or 0.
    pub cursor_param: i32,
}

// ===========================================================================
// Supporting enums (nodes/primnodes.h)
// ===========================================================================

/// `ParamKind` (nodes/primnodes.h) — kind of [`Param`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ParamKind {
    /// `PARAM_EXTERN`.
    PARAM_EXTERN = 0,
    /// `PARAM_EXEC`.
    PARAM_EXEC = 1,
    /// `PARAM_SUBLINK`.
    PARAM_SUBLINK = 2,
    /// `PARAM_MULTIEXPR`.
    PARAM_MULTIEXPR = 3,
}
pub use ParamKind::{PARAM_EXEC, PARAM_EXTERN, PARAM_MULTIEXPR, PARAM_SUBLINK};

/// `VarReturningType` (nodes/primnodes.h) — RETURNING old/new/default value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum VarReturningType {
    /// `VAR_RETURNING_DEFAULT`.
    #[default]
    VAR_RETURNING_DEFAULT = 0,
    /// `VAR_RETURNING_OLD`.
    VAR_RETURNING_OLD = 1,
    /// `VAR_RETURNING_NEW`.
    VAR_RETURNING_NEW = 2,
}
pub use VarReturningType::{VAR_RETURNING_DEFAULT, VAR_RETURNING_NEW, VAR_RETURNING_OLD};

/// `CoercionForm` (nodes/primnodes.h) — how to display a coercion node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum CoercionForm {
    /// `COERCE_EXPLICIT_CALL`.
    #[default]
    COERCE_EXPLICIT_CALL = 0,
    /// `COERCE_EXPLICIT_CAST`.
    COERCE_EXPLICIT_CAST = 1,
    /// `COERCE_IMPLICIT_CAST`.
    COERCE_IMPLICIT_CAST = 2,
    /// `COERCE_SQL_SYNTAX`.
    COERCE_SQL_SYNTAX = 3,
}

/// `BoolExprType` (nodes/primnodes.h) — AND/OR/NOT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BoolExprType {
    /// `AND_EXPR`.
    AND_EXPR = 0,
    /// `OR_EXPR`.
    OR_EXPR = 1,
    /// `NOT_EXPR`.
    NOT_EXPR = 2,
}
pub use BoolExprType::{AND_EXPR, NOT_EXPR, OR_EXPR};

/// `BoolExpr` (nodes/primnodes.h) — the basic Boolean operators AND/OR/NOT.
#[derive(Clone, Debug)]
pub struct BoolExpr<'mcx> {
    /// `BoolExprType boolop`.
    pub boolop: BoolExprType,
    /// `List *args` — arguments (exactly one for NOT, two-or-more for AND/OR).
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CompareType` (nodes/cmptype.h) — abstract comparison kind requested of a
/// [`RowCompareExpr`]. Canonically defined in `types_tableam::amapi` (the full
/// 9-variant `cmptype.h` enum); re-exported here so the node, executor, and
/// access-method layers share one type. The btree comparison strategies
/// (`COMPARE_INVALID`..`COMPARE_NE`) carry identical discriminants.
pub use types_tableam::amapi::CompareType;

/// `MinMaxOp` (nodes/primnodes.h) — GREATEST vs LEAST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum MinMaxOp {
    /// `IS_GREATEST`.
    IS_GREATEST = 0,
    /// `IS_LEAST`.
    IS_LEAST = 1,
}

/// `SQLValueFunctionOp` (nodes/primnodes.h) — which parameterless SQL value
/// function a [`SQLValueFunction`] denotes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum SQLValueFunctionOp {
    SVFOP_CURRENT_DATE = 0,
    SVFOP_CURRENT_TIME = 1,
    SVFOP_CURRENT_TIME_N = 2,
    SVFOP_CURRENT_TIMESTAMP = 3,
    SVFOP_CURRENT_TIMESTAMP_N = 4,
    SVFOP_LOCALTIME = 5,
    SVFOP_LOCALTIME_N = 6,
    SVFOP_LOCALTIMESTAMP = 7,
    SVFOP_LOCALTIMESTAMP_N = 8,
    SVFOP_CURRENT_ROLE = 9,
    SVFOP_CURRENT_USER = 10,
    SVFOP_USER = 11,
    SVFOP_SESSION_USER = 12,
    SVFOP_CURRENT_CATALOG = 13,
    SVFOP_CURRENT_SCHEMA = 14,
}

/// `XmlExprOp` (nodes/primnodes.h) — which SQL/XML function a [`XmlExpr`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum XmlExprOp {
    IS_XMLCONCAT = 0,
    IS_XMLELEMENT = 1,
    IS_XMLFOREST = 2,
    IS_XMLPARSE = 3,
    IS_XMLPI = 4,
    IS_XMLROOT = 5,
    IS_XMLSERIALIZE = 6,
    IS_DOCUMENT = 7,
}

/// `XmlOptionType` (nodes/primnodes.h) — DOCUMENT vs CONTENT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum XmlOptionType {
    /// `XMLOPTION_DOCUMENT`.
    XMLOPTION_DOCUMENT = 0,
    /// `XMLOPTION_CONTENT`.
    XMLOPTION_CONTENT = 1,
}

/// `JsonConstructorType` (nodes/primnodes.h) — kind of SQL/JSON constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum JsonConstructorType {
    #[default]
    JSCTOR_JSON_OBJECT = 1,
    JSCTOR_JSON_ARRAY = 2,
    JSCTOR_JSON_OBJECTAGG = 3,
    JSCTOR_JSON_ARRAYAGG = 4,
    JSCTOR_JSON_PARSE = 5,
    JSCTOR_JSON_SCALAR = 6,
    JSCTOR_JSON_SERIALIZE = 7,
}

/// `JsonValueType` (nodes/primnodes.h) — item type for an IS JSON predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonValueType {
    JS_TYPE_ANY = 0,
    JS_TYPE_OBJECT = 1,
    JS_TYPE_ARRAY = 2,
    JS_TYPE_SCALAR = 3,
}

/// `JsonExprOp` (nodes/primnodes.h) — SQL/JSON query function type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonExprOp {
    JSON_EXISTS_OP = 0,
    JSON_QUERY_OP = 1,
    JSON_VALUE_OP = 2,
    JSON_TABLE_OP = 3,
}

/// `JsonWrapper` (nodes/primnodes.h) — WRAPPER clause for `JSON_QUERY()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonWrapper {
    JSW_UNSPEC = 0,
    JSW_NONE = 1,
    JSW_CONDITIONAL = 2,
    JSW_UNCONDITIONAL = 3,
}

/// `JsonQuotes` (nodes/parsenodes.h) — representation of [KEEP|OMIT] QUOTES
/// clause for `JSON_QUERY()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonQuotes {
    JS_QUOTES_UNSPEC = 0,
    JS_QUOTES_KEEP = 1,
    JS_QUOTES_OMIT = 2,
}

/// `JsonTableColumnType` (nodes/parsenodes.h) — enumeration of `JSON_TABLE`
/// column types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonTableColumnType {
    JTC_FOR_ORDINALITY = 0,
    JTC_REGULAR = 1,
    JTC_EXISTS = 2,
    JTC_FORMATTED = 3,
    JTC_NESTED = 4,
}

/// `NullTestType` (nodes/primnodes.h) — IS NULL vs IS NOT NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum NullTestType {
    /// `IS_NULL`.
    IS_NULL = 0,
    /// `IS_NOT_NULL`.
    IS_NOT_NULL = 1,
}

/// `BoolTestType` (nodes/primnodes.h) — IS [NOT] TRUE/FALSE/UNKNOWN.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BoolTestType {
    IS_TRUE = 0,
    IS_NOT_TRUE = 1,
    IS_FALSE = 2,
    IS_NOT_FALSE = 3,
    IS_UNKNOWN = 4,
    IS_NOT_UNKNOWN = 5,
}

// ===========================================================================
// Expr-derived node structs (nodes/primnodes.h). Each has `Expr xpr;` first
// in C; here the discriminant is the `Expr` enum variant. Purely-planner /
// query-jumble-only fields are trimmed (docs/types.md rule 3); `location` is
// carried field-for-field (the parser sets it for error positions).
// ===========================================================================

/// `Param` (nodes/primnodes.h).
#[derive(Clone, Copy, Debug)]
pub struct Param {
    /// `ParamKind paramkind`.
    pub paramkind: ParamKind,
    /// `int paramid`.
    pub paramid: i32,
    /// `Oid paramtype`.
    pub paramtype: Oid,
    /// `int32 paramtypmod`.
    pub paramtypmod: i32,
    /// `Oid paramcollid`.
    pub paramcollid: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `Aggref` (nodes/primnodes.h). The aggregate's `args` is a targetlist (list
/// of [`TargetEntry`]); the planner-set ids `aggno`/`aggtransno` drive the
/// `ExecBuildAggTrans` machinery.
///
/// `Clone` panics: `args` is a `TargetEntry` list whose elements carry
/// context-allocated `PgBox`/`PgString` children that cannot derive `Clone`
/// (deep-copy goes through `TargetEntry::clone_in`). The derived `Expr::clone`
/// is unused for Aggref in the executor (aggregates are compiled by
/// `ExecBuildAggTrans`, which never round-trips through a plain `.clone()`).
#[derive(Debug)]
pub struct Aggref<'mcx> {
    /// `Oid aggfnoid`.
    pub aggfnoid: Oid,
    /// `Oid aggtype`.
    pub aggtype: Oid,
    /// `Oid aggcollid`.
    pub aggcollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `Oid aggtranstype`.
    pub aggtranstype: Oid,
    /// `List *aggargtypes` — OID list of direct + aggregated arg types.
    pub aggargtypes: Vec<Oid>,
    /// `List *aggdirectargs` — direct args (plain exprs) for ordered-set aggs.
    pub aggdirectargs: Vec<Expr<'mcx>>,
    /// `List *args` — aggregated args + sort exprs (list of TargetEntry).
    pub args: Vec<TargetEntry<'mcx>>,
    /// `List *aggorder` — ORDER BY (list of SortGroupClause). Set by the parser
    /// (`transformAggregateCall`, parse_agg.c). Added field-for-field vs
    /// primnodes.h.
    pub aggorder: Vec<crate::rawnodes::SortGroupClause>,
    /// `List *aggdistinct` — DISTINCT (list of SortGroupClause). Set by the
    /// parser (`transformAggregateCall`, parse_agg.c). Added field-for-field vs
    /// primnodes.h.
    pub aggdistinct: Vec<crate::rawnodes::SortGroupClause>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<Box<Expr<'mcx>>>,
    /// `bool aggstar` — true if argument list was really `*`.
    pub aggstar: bool,
    /// `bool aggvariadic`.
    pub aggvariadic: bool,
    /// `char aggkind` — aggregate kind (see pg_aggregate.h).
    pub aggkind: i8,
    /// `bool aggpresorted` — aggregate input already sorted. Set by the query
    /// planner for ORDER BY / DISTINCT aggregates whose input arrives presorted.
    /// Added field-for-field vs primnodes.h.
    pub aggpresorted: bool,
    /// `Index agglevelsup`.
    pub agglevelsup: Index,
    /// `AggSplit aggsplit` — expected agg-splitting mode of parent Agg.
    pub aggsplit: crate::nodeagg::AggSplit,
    /// `int aggno` — unique ID within the Agg node (-1 before planning).
    pub aggno: i32,
    /// `int aggtransno` — unique ID of transition state in the Agg.
    pub aggtransno: i32,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

impl<'mcx> Clone for Aggref<'mcx> {
    fn clone(&self) -> Self {
        panic!(
            "Aggref::clone: aggregate args are a TargetEntry list with \
             context-allocated children; deep-copy via TargetEntry::clone_in"
        )
    }
}

/// `GroupingFunc` (nodes/primnodes.h) — a `GROUPING(...)` expression.
#[derive(Clone, Debug)]
pub struct GroupingFunc<'mcx> {
    /// `List *args` — kept for EXPLAIN; not evaluated.
    pub args: Vec<Expr<'mcx>>,
    /// `List *refs` — ressortgrouprefs of arguments (integer list).
    pub refs: Vec<i32>,
    /// `List *cols` — actual column positions set by planner (integer list).
    pub cols: Vec<i32>,
    /// `Index agglevelsup`.
    pub agglevelsup: Index,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `WindowFunc` (nodes/primnodes.h).
#[derive(Clone, Debug)]
pub struct WindowFunc<'mcx> {
    /// `Oid winfnoid`.
    pub winfnoid: Oid,
    /// `Oid wintype`.
    pub wintype: Oid,
    /// `Oid wincollid`.
    pub wincollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<Box<Expr<'mcx>>>,
    /// `List *runCondition` — WindowFuncRunConditions to short-circuit.
    pub runCondition: Vec<Expr<'mcx>>,
    /// `Index winref` — index of associated WindowClause.
    pub winref: Index,
    /// `bool winstar`.
    pub winstar: bool,
    /// `bool winagg`.
    pub winagg: bool,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

impl<'mcx> WindowFunc<'mcx> {
    /// Deep-copy this `WindowFunc` into `mcx` (C: `copyObject` over a
    /// `WindowFunc *`). The derived `.clone()` would shallow-clone the `args` /
    /// `aggfilter` / `runCondition` child `Expr`s, panicking the moment one of
    /// them is a `SubPlan` (e.g. `lead(ten, (SELECT ...)) OVER (...)`), because
    /// `SubPlanExpr::clone` is a deliberate trap. This routes every child
    /// through the sanctioned [`Expr::clone_in`] path instead. Mirrors the
    /// `Expr::WindowFunc` arm of `Expr::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WindowFunc<'b>> {
        Ok(WindowFunc {
            winfnoid: self.winfnoid,
            wintype: self.wintype,
            wincollid: self.wincollid,
            inputcollid: self.inputcollid,
            args: clone_vec_expr(&self.args, mcx)?,
            aggfilter: clone_opt_box_expr(&self.aggfilter, mcx)?,
            runCondition: clone_vec_expr(&self.runCondition, mcx)?,
            winref: self.winref,
            winstar: self.winstar,
            winagg: self.winagg,
            location: self.location,
        })
    }
}

/// `WindowFuncRunCondition` (nodes/primnodes.h) — an intermediate `OpExpr`-like
/// node used by `WindowAgg` to short-circuit execution of monotonic window
/// functions.
#[derive(Clone, Debug)]
pub struct WindowFuncRunCondition<'mcx> {
    /// `Oid opno` — PG_OPERATOR OID of the operator.
    pub opno: Oid,
    /// `Oid inputcollid` — OID of collation that operator should use.
    pub inputcollid: Oid,
    /// `bool wfunc_left` — true if the WindowFunc belongs on the left of the
    /// resulting OpExpr, false if it is on the right.
    pub wfunc_left: bool,
    /// `Expr *arg` — the Expr being compared to the WindowFunc.
    pub arg: Option<Box<Expr<'mcx>>>,
}

impl<'mcx> WindowFuncRunCondition<'mcx> {
    /// Deep-copy this `WindowFuncRunCondition` into `mcx` (C: `copyObject`).
    /// Routes the `arg` child through the sanctioned [`Expr::clone_in`] path.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WindowFuncRunCondition<'b>> {
        Ok(WindowFuncRunCondition {
            opno: self.opno,
            inputcollid: self.inputcollid,
            wfunc_left: self.wfunc_left,
            arg: clone_opt_box_expr(&self.arg, mcx)?,
        })
    }
}

/// `MergeSupportFunc` (nodes/primnodes.h) — `MERGE_ACTION()`.
#[derive(Clone, Copy, Debug)]
pub struct MergeSupportFunc {
    /// `Oid msftype`.
    pub msftype: Oid,
    /// `Oid msfcollid`.
    pub msfcollid: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `SubscriptingRef` (nodes/primnodes.h) — a subscripting operation over a
/// container (array, etc).
#[derive(Clone, Debug)]
pub struct SubscriptingRef<'mcx> {
    /// `Oid refcontainertype`.
    pub refcontainertype: Oid,
    /// `Oid refelemtype`.
    pub refelemtype: Oid,
    /// `Oid refrestype`.
    pub refrestype: Oid,
    /// `int32 reftypmod`.
    pub reftypmod: i32,
    /// `Oid refcollid`.
    pub refcollid: Oid,
    /// `List *refupperindexpr` — upper container index exprs (may contain
    /// NULL elements in the slice case).
    pub refupperindexpr: Vec<Option<Expr<'mcx>>>,
    /// `List *reflowerindexpr` — lower container index exprs, or empty for a
    /// single element (may contain NULL elements).
    pub reflowerindexpr: Vec<Option<Expr<'mcx>>>,
    /// `Expr *refexpr` — expression yielding the container value.
    pub refexpr: Option<Box<Expr<'mcx>>>,
    /// `Expr *refassgnexpr` — source value for a store, or `None` for a fetch.
    pub refassgnexpr: Option<Box<Expr<'mcx>>>,
}

/// `FuncExpr` (nodes/primnodes.h) — a function call.
#[derive(Clone, Debug)]
pub struct FuncExpr<'mcx> {
    /// `Oid funcid`.
    pub funcid: Oid,
    /// `Oid funcresulttype`.
    pub funcresulttype: Oid,
    /// `bool funcretset`.
    pub funcretset: bool,
    /// `bool funcvariadic`.
    pub funcvariadic: bool,
    /// `CoercionForm funcformat`.
    pub funcformat: CoercionForm,
    /// `Oid funccollid`.
    pub funccollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

/// `NamedArgExpr` (nodes/primnodes.h) — a named function argument. The planner
/// removes these before execution.
#[derive(Clone, Debug)]
pub struct NamedArgExpr<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `char *name`.
    pub name: Option<String>,
    /// `int argnumber`.
    pub argnumber: i32,
    /// `ParseLoc location` — argument name location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `SubLink` (nodes/primnodes.h) — a subselect in an expression. The planner
/// replaces these with [`SubPlan`] nodes; never executed directly.
///
/// `Clone` panics: `subselect` is an embedded owned `Query` whose children are
/// context-allocated `PgBox`/`PgString` (deep-copy goes through
/// `SubLink::clone_in`). This mirrors the [`Aggref`]/[`SubPlanExpr`] convention
/// for embedded owned sub-trees inside the lifetime-free [`Expr`] enum.
#[derive(Debug)]
pub struct SubLink<'mcx> {
    /// `SubLinkType subLinkType`.
    pub subLinkType: SubLinkType,
    /// `int subLinkId`.
    pub subLinkId: i32,
    /// `Node *testexpr`.
    pub testexpr: Option<Box<Expr<'mcx>>>,
    /// `List *operName` — the (possibly qualified) operator name for an
    /// `ALL`/`ANY`/`ROWCOMPARE_SUBLINK`, e.g. `("=")`. Unlike most parse-time
    /// fields it SURVIVES parse-analysis (C `_outSubLink`/`_readSubLink` write/read
    /// it unconditionally), so stored view `_RETURN` rules embed it. Modeled as the
    /// lifetime-free `Vec<String>` used by the other analyzed string-list carriers;
    /// NIL for `EXISTS`/`EXPR`/`ARRAY`/`CTE` sublinks renders as the empty vec
    /// (`<>`).
    pub operName: Vec<String>,
    /// `Node *subselect` — the sub-`Query` (after analysis), embedded owned and
    /// walked by deref, exactly mirroring
    /// [`RangeTblEntry::subquery`](crate::parsenodes::RangeTblEntry). Because the
    /// `Expr` enum is lifetime-free, the embedded `Query` carries the `'static`
    /// notional lifetime, matching the `Box<SubPlan<'static>>` convention used by
    /// [`SubPlanExpr`]. `None` is the C `NULL` (and the value produced until
    /// `transformSubLink` is ported).
    pub subselect: Option<PgBox<'mcx, crate::copy_query::Query<'mcx>>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

impl<'mcx> Clone for SubLink<'mcx> {
    fn clone(&self) -> Self {
        panic!(
            "SubLink::clone: subselect is an embedded owned Query whose children \
             are context-allocated; deep-copy goes through copyObject / the \
             analyzed-tree clone path, never a plain `.clone()` (mirrors \
             Aggref::clone)"
        )
    }
}

impl<'mcx> SubLink<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). This is the sanctioned,
    /// non-panicking deep-copy path used by the [`Expr::SubLink`]
    /// [`Expr::clone_in`] arm (never the panicking derived `.clone()`). The
    /// embedded owned `subselect` `Query` is deep-cloned via
    /// [`crate::copy_query::Query::clone_in`] and `testexpr` recurses through
    /// [`Expr::clone_in`] (mirrors [`Aggref::clone_in`]).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SubLink<'b>> {
        clone_sublink(self, mcx)
    }
}

/// `AlternativeSubPlan` (nodes/primnodes.h) — a choice among SubPlans
/// (transient; removed before execution).
#[derive(Debug)]
pub struct AlternativeSubPlan<'mcx> {
    /// `List *subplans` — SubPlan(s) with equivalent results.
    pub subplans: Vec<PgBox<'mcx, SubPlan<'mcx>>>,
}

impl Clone for AlternativeSubPlan<'_> {
    fn clone(&self) -> Self {
        panic!(
            "AlternativeSubPlan::clone: transient planner node, never present \
             at execution"
        )
    }
}

/// `FieldSelect` (nodes/primnodes.h) — extract one field from a rowtype value.
#[derive(Clone, Debug)]
pub struct FieldSelect<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `AttrNumber fieldnum`.
    pub fieldnum: AttrNumber,
    /// `Oid resulttype`.
    pub resulttype: Oid,
    /// `int32 resulttypmod`.
    pub resulttypmod: i32,
    /// `Oid resultcollid`.
    pub resultcollid: Oid,
}

/// `FieldStore` (nodes/primnodes.h) — modify one or more fields in a rowtype
/// value, yielding a new value.
#[derive(Clone, Debug)]
pub struct FieldStore<'mcx> {
    /// `Expr *arg` — input tuple value.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `List *newvals` — new value(s) for field(s).
    pub newvals: Vec<Expr<'mcx>>,
    /// `List *fieldnums` — integer list of field attnums.
    pub fieldnums: Vec<AttrNumber>,
    /// `Oid resulttype`.
    pub resulttype: Oid,
}

/// `RelabelType` (nodes/primnodes.h) — a no-op binary-compatible coercion.
#[derive(Clone, Debug)]
pub struct RelabelType<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Oid resulttype`.
    pub resulttype: Oid,
    /// `int32 resulttypmod`.
    pub resulttypmod: i32,
    /// `Oid resultcollid`.
    pub resultcollid: Oid,
    /// `CoercionForm relabelformat`.
    pub relabelformat: CoercionForm,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CoerceViaIO` (nodes/primnodes.h) — coercion via the source typoutput then
/// destination typinput.
#[derive(Clone, Debug)]
pub struct CoerceViaIO<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Oid resulttype`.
    pub resulttype: Oid,
    /// `Oid resultcollid`.
    pub resultcollid: Oid,
    /// `CoercionForm coerceformat`.
    pub coerceformat: CoercionForm,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `ArrayCoerceExpr` (nodes/primnodes.h) — array-type coercion applying a
/// per-element coercion `elemexpr`.
#[derive(Clone, Debug)]
pub struct ArrayCoerceExpr<'mcx> {
    /// `Expr *arg` — input expression (yields an array).
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Expr *elemexpr` — per-element coercion work.
    pub elemexpr: Option<Box<Expr<'mcx>>>,
    /// `Oid resulttype`.
    pub resulttype: Oid,
    /// `int32 resulttypmod`.
    pub resulttypmod: i32,
    /// `Oid resultcollid`.
    pub resultcollid: Oid,
    /// `CoercionForm coerceformat`.
    pub coerceformat: CoercionForm,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `ConvertRowtypeExpr` (nodes/primnodes.h) — composite-to-composite coercion.
#[derive(Clone, Debug)]
pub struct ConvertRowtypeExpr<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Oid resulttype` — always a composite type.
    pub resulttype: Oid,
    /// `CoercionForm convertformat`.
    pub convertformat: CoercionForm,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CollateExpr` (nodes/primnodes.h) — COLLATE; planner replaces with
/// RelabelType, so never executed.
#[derive(Clone, Debug)]
pub struct CollateExpr<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Oid collOid`.
    pub collOid: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CaseExpr` (nodes/primnodes.h) — a CASE expression.
#[derive(Clone, Debug)]
pub struct CaseExpr<'mcx> {
    /// `Oid casetype`.
    pub casetype: Oid,
    /// `Oid casecollid`.
    pub casecollid: Oid,
    /// `Expr *arg` — implicit equality comparison argument (form 2), or `None`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `List *args` — the WHEN clauses (list of [`CaseWhen`]).
    pub args: Vec<CaseWhen<'mcx>>,
    /// `Expr *defresult` — the ELSE result.
    pub defresult: Option<Box<Expr<'mcx>>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CaseWhen` (nodes/primnodes.h) — one arm of a CASE expression. (Not itself
/// `Expr`-derived in the dispatch sense, but carried inline in [`CaseExpr`].)
#[derive(Clone, Debug)]
pub struct CaseWhen<'mcx> {
    /// `Expr *expr` — condition expression.
    pub expr: Option<Box<Expr<'mcx>>>,
    /// `Expr *result` — substitution result.
    pub result: Option<Box<Expr<'mcx>>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CaseTestExpr` (nodes/primnodes.h) — placeholder for the CASE test value.
#[derive(Clone, Copy, Debug)]
pub struct CaseTestExpr {
    /// `Oid typeId`.
    pub typeId: Oid,
    /// `int32 typeMod`.
    pub typeMod: i32,
    /// `Oid collation`.
    pub collation: Oid,
}

/// `ArrayExpr` (nodes/primnodes.h) — an `ARRAY[]` expression.
#[derive(Clone, Debug)]
pub struct ArrayExpr<'mcx> {
    /// `Oid array_typeid`.
    pub array_typeid: Oid,
    /// `Oid array_collid`.
    pub array_collid: Oid,
    /// `Oid element_typeid`.
    pub element_typeid: Oid,
    /// `List *elements` — the array elements or sub-arrays.
    pub elements: Vec<Expr<'mcx>>,
    /// `bool multidims` — true if elements are sub-arrays.
    pub multidims: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `RowExpr` (nodes/primnodes.h) — a `ROW()` expression.
#[derive(Clone, Debug)]
pub struct RowExpr<'mcx> {
    /// `List *args` — the fields.
    pub args: Vec<Expr<'mcx>>,
    /// `Oid row_typeid` — RECORDOID or a composite type's ID.
    pub row_typeid: Oid,
    /// `CoercionForm row_format`.
    pub row_format: CoercionForm,
    /// `List *colnames` — list of String, or empty.
    pub colnames: Vec<String>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `RowCompareExpr` (nodes/primnodes.h) — a row-wise comparison.
#[derive(Clone, Debug)]
pub struct RowCompareExpr<'mcx> {
    /// `CompareType cmptype` — LT/LE/GE/GT (never EQ/NE).
    pub cmptype: CompareType,
    /// `List *opnos` — OID list of pairwise comparison ops.
    pub opnos: Vec<Oid>,
    /// `List *opfamilies` — OID list of containing operator families.
    pub opfamilies: Vec<Oid>,
    /// `List *inputcollids` — OID list of comparison collations.
    pub inputcollids: Vec<Oid>,
    /// `List *largs` — left-hand input arguments.
    pub largs: Vec<Expr<'mcx>>,
    /// `List *rargs` — right-hand input arguments.
    pub rargs: Vec<Expr<'mcx>>,
}

/// `CoalesceExpr` (nodes/primnodes.h) — a COALESCE expression.
#[derive(Clone, Debug)]
pub struct CoalesceExpr<'mcx> {
    /// `Oid coalescetype`.
    pub coalescetype: Oid,
    /// `Oid coalescecollid`.
    pub coalescecollid: Oid,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `MinMaxExpr` (nodes/primnodes.h) — a GREATEST or LEAST function.
#[derive(Clone, Debug)]
pub struct MinMaxExpr<'mcx> {
    /// `Oid minmaxtype`.
    pub minmaxtype: Oid,
    /// `Oid minmaxcollid`.
    pub minmaxcollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `MinMaxOp op`.
    pub op: MinMaxOp,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `SQLValueFunction` (nodes/primnodes.h) — a parameterless SQL value function.
#[derive(Clone, Copy, Debug)]
pub struct SQLValueFunction {
    /// `SQLValueFunctionOp op`.
    pub op: SQLValueFunctionOp,
    /// `Oid type`.
    pub r#type: Oid,
    /// `int32 typmod`.
    pub typmod: i32,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `XmlExpr` (nodes/primnodes.h) — a SQL/XML function.
#[derive(Clone, Debug)]
pub struct XmlExpr<'mcx> {
    /// `XmlExprOp op`.
    pub op: XmlExprOp,
    /// `char *name`.
    pub name: Option<String>,
    /// `List *named_args` — non-XML expressions for xml_attributes.
    pub named_args: Vec<Expr<'mcx>>,
    /// `List *arg_names` — parallel list of String values.
    pub arg_names: Vec<String>,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `XmlOptionType xmloption`.
    pub xmloption: XmlOptionType,
    /// `bool indent` — INDENT option for XMLSERIALIZE.
    pub indent: bool,
    /// `Oid type`.
    pub r#type: Oid,
    /// `int32 typmod`.
    pub typmod: i32,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonFormatType` (nodes/primnodes.h) — JSON FORMAT clause kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonFormatType {
    JS_FORMAT_DEFAULT = 0,
    JS_FORMAT_JSON = 1,
    JS_FORMAT_JSONB = 2,
}

/// `JsonEncoding` (nodes/primnodes.h) — JSON ENCODING clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonEncoding {
    JS_ENC_DEFAULT = 0,
    JS_ENC_UTF8 = 1,
    JS_ENC_UTF16 = 2,
    JS_ENC_UTF32 = 3,
}

/// `JsonFormat` (nodes/primnodes.h) — representation of a JSON FORMAT clause.
#[derive(Clone, Copy, Debug)]
pub struct JsonFormat {
    /// `JsonFormatType format_type`.
    pub format_type: JsonFormatType,
    /// `JsonEncoding encoding`.
    pub encoding: JsonEncoding,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonReturning` (nodes/primnodes.h) — transformed JSON RETURNING clause.
#[derive(Clone, Copy, Debug)]
pub struct JsonReturning {
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `Oid typid`.
    pub typid: Oid,
    /// `int32 typmod`.
    pub typmod: i32,
}

/// `JsonValueExpr` (nodes/primnodes.h) — a JSON value expression
/// (`expr [FORMAT ...]`). Not itself dispatched as an `Expr` opcode, but
/// carried by JSON nodes.
#[derive(Clone, Debug)]
pub struct JsonValueExpr<'mcx> {
    /// `Expr *raw_expr` — user-specified expression.
    pub raw_expr: Option<Box<Expr<'mcx>>>,
    /// `Expr *formatted_expr` — coerced formatted expression.
    pub formatted_expr: Option<Box<Expr<'mcx>>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
}

/// `JsonConstructorExpr` (nodes/primnodes.h) — wrapper over FuncExpr/Aggref/
/// WindowFunc for SQL/JSON constructors.
#[derive(Clone, Debug)]
pub struct JsonConstructorExpr<'mcx> {
    /// `JsonConstructorType type`.
    pub r#type: JsonConstructorType,
    /// `List *args`.
    pub args: Vec<Expr<'mcx>>,
    /// `Expr *func` — underlying json[b]_xxx() function call.
    pub func: Option<Box<Expr<'mcx>>>,
    /// `Expr *coercion` — coercion to RETURNING type.
    pub coercion: Option<Box<Expr<'mcx>>>,
    /// `JsonReturning *returning`.
    pub returning: Option<JsonReturning>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
    /// `bool unique`.
    pub unique: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonIsPredicate` (nodes/primnodes.h) — an IS JSON predicate.
#[derive(Clone, Debug)]
pub struct JsonIsPredicate<'mcx> {
    /// `Node *expr` — subject expression.
    pub expr: Option<Box<Expr<'mcx>>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `JsonValueType item_type`.
    pub item_type: JsonValueType,
    /// `bool unique_keys`.
    pub unique_keys: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonBehaviorType` (nodes/primnodes.h) — ON ERROR / ON EMPTY behavior kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonBehaviorType {
    JSON_BEHAVIOR_NULL = 0,
    JSON_BEHAVIOR_ERROR = 1,
    JSON_BEHAVIOR_EMPTY = 2,
    JSON_BEHAVIOR_TRUE = 3,
    JSON_BEHAVIOR_FALSE = 4,
    JSON_BEHAVIOR_UNKNOWN = 5,
    JSON_BEHAVIOR_EMPTY_ARRAY = 6,
    JSON_BEHAVIOR_EMPTY_OBJECT = 7,
    JSON_BEHAVIOR_DEFAULT = 8,
}

/// `JsonBehavior` (nodes/primnodes.h) — ON ERROR / ON EMPTY specification.
#[derive(Clone, Debug)]
pub struct JsonBehavior<'mcx> {
    /// `JsonBehaviorType btype`.
    pub btype: JsonBehaviorType,
    /// `Node *expr`.
    pub expr: Option<Box<Expr<'mcx>>>,
    /// `bool coerce`.
    pub coerce: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonExpr` (nodes/primnodes.h) — transformed JSON_VALUE/JSON_QUERY/
/// JSON_EXISTS.
#[derive(Clone, Debug)]
pub struct JsonExpr<'mcx> {
    /// `JsonExprOp op`.
    pub op: JsonExprOp,
    /// `char *column_name` — JSON_TABLE() column name, or `None`.
    pub column_name: Option<String>,
    /// `Node *formatted_expr` — jsonb-valued expression to query.
    pub formatted_expr: Option<Box<Expr<'mcx>>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `Node *path_spec` — jsonpath-valued query pattern.
    pub path_spec: Option<Box<Expr<'mcx>>>,
    /// `JsonReturning *returning` — expected output type/format.
    pub returning: Option<JsonReturning>,
    /// `List *passing_names` — PASSING argument names (list of String).
    pub passing_names: Vec<String>,
    /// `List *passing_values` — PASSING argument value expressions.
    pub passing_values: Vec<Expr<'mcx>>,
    /// `JsonBehavior *on_empty`.
    pub on_empty: Option<Box<JsonBehavior<'mcx>>>,
    /// `JsonBehavior *on_error`.
    pub on_error: Option<Box<JsonBehavior<'mcx>>>,
    /// `bool use_io_coercion`.
    pub use_io_coercion: bool,
    /// `bool use_json_coercion`.
    pub use_json_coercion: bool,
    /// `JsonWrapper wrapper`.
    pub wrapper: JsonWrapper,
    /// `bool omit_quotes`.
    pub omit_quotes: bool,
    /// `Oid collation`.
    pub collation: Oid,
    /// `ParseLoc location` — original JsonFuncExpr's location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `NullTest` (nodes/primnodes.h) — IS [NOT] NULL test.
#[derive(Clone, Debug)]
pub struct NullTest<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `NullTestType nulltesttype`.
    pub nulltesttype: NullTestType,
    /// `bool argisrow` — true to perform field-by-field null checks.
    pub argisrow: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `BooleanTest` (nodes/primnodes.h) — IS [NOT] TRUE/FALSE/UNKNOWN.
#[derive(Clone, Debug)]
pub struct BooleanTest<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `BoolTestType booltesttype`.
    pub booltesttype: BoolTestType,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CoerceToDomain` (nodes/primnodes.h) — coerce a value to a domain type.
#[derive(Clone, Debug)]
pub struct CoerceToDomain<'mcx> {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr<'mcx>>>,
    /// `Oid resulttype` — domain type ID.
    pub resulttype: Oid,
    /// `int32 resulttypmod`.
    pub resulttypmod: i32,
    /// `Oid resultcollid`.
    pub resultcollid: Oid,
    /// `CoercionForm coercionformat`.
    pub coercionformat: CoercionForm,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CoerceToDomainValue` (nodes/primnodes.h) — placeholder for the value a
/// domain's CHECK constraint processes.
#[derive(Clone, Copy, Debug)]
pub struct CoerceToDomainValue {
    /// `Oid typeId`.
    pub typeId: Oid,
    /// `int32 typeMod`.
    pub typeMod: i32,
    /// `Oid collation`.
    pub collation: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `SetToDefault` (nodes/primnodes.h) — DEFAULT marker in INSERT/UPDATE.
/// Replaced before execution.
#[derive(Clone, Copy, Debug)]
pub struct SetToDefault {
    /// `Oid typeId`.
    pub typeId: Oid,
    /// `int32 typeMod`.
    pub typeMod: i32,
    /// `Oid collation`.
    pub collation: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `NextValueExpr` (nodes/primnodes.h) — get next value from a sequence.
#[derive(Clone, Copy, Debug)]
pub struct NextValueExpr {
    /// `Oid seqid`.
    pub seqid: Oid,
    /// `Oid typeId`.
    pub typeId: Oid,
}

/// `InferenceElem` (nodes/primnodes.h) — element of a unique-index inference
/// spec.
#[derive(Clone, Debug)]
pub struct InferenceElem<'mcx> {
    /// `Node *expr`.
    pub expr: Option<Box<Expr<'mcx>>>,
    /// `Oid infercollid`.
    pub infercollid: Oid,
    /// `Oid inferopclass`.
    pub inferopclass: Oid,
}

/// `ReturningExpr` (nodes/primnodes.h) — return OLD/NEW.(expression) in a
/// RETURNING list. Inserted by the rewriter/planner only.
#[derive(Clone, Debug)]
pub struct ReturningExpr<'mcx> {
    /// `int retlevelsup`.
    pub retlevelsup: i32,
    /// `bool retold` — true for OLD, false for NEW.
    pub retold: bool,
    /// `Expr *retexpr`.
    pub retexpr: Option<Box<Expr<'mcx>>>,
}

/// `PlaceHolderVar` (nodes/pathnodes.h) — a placeholder for a subexpression that
/// must be evaluated below an outer join and then forced to null above it. The
/// optimizer dispatches `IsA(node, PlaceHolderVar)` and reads
/// `phid`/`phrels`/`phnullingrels`/`phexpr`.
///
/// `phexpr` is an owned `Box<Expr<'mcx>>` (matching the rest of the lifetime-free
/// tree); `phrels`/`phnullingrels` are the lifetime-free [`ExprRelids`].
#[derive(Clone, Debug, Default)]
pub struct PlaceHolderVar<'mcx> {
    /// `Expr *phexpr` — the represented expression. `None` only during
    /// construction; a built PHV always wraps an expression.
    pub phexpr: Option<Box<Expr<'mcx>>>,
    /// `Relids phrels` — base+OJ relids syntactically within `phexpr`.
    pub phrels: ExprRelids,
    /// `Relids phnullingrels` — RT indexes of outer joins that can null the
    /// PHV's value.
    pub phnullingrels: ExprRelids,
    /// `Index phid` — ID for the PHV (unique within a planner run).
    pub phid: u32,
    /// `Index phlevelsup` — `> 0` if the PHV belongs to an outer query.
    pub phlevelsup: u32,
}

impl<'mcx> PlaceHolderVar<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape), recursing into `phexpr`
    /// through the non-panicking arena clone (never a shallow `.clone()` on an
    /// `Aggref`/`SubLink`/… child). Mirrors the `Expr::PlaceHolderVar`
    /// [`Expr::clone_in`] arm.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PlaceHolderVar<'b>> {
        Ok(PlaceHolderVar {
            phexpr: clone_opt_box_expr(&self.phexpr, mcx)?,
            phrels: self.phrels.clone(),
            phnullingrels: self.phnullingrels.clone(),
            phid: self.phid,
            phlevelsup: self.phlevelsup,
        })
    }
}

impl<'mcx> Aggref<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Delegates to the
    /// non-panicking `args`-deep-copying path used by the [`Expr::Aggref`]
    /// [`Expr::clone_in`] arm (never the panicking derived `.clone()`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Aggref<'b>> {
        clone_aggref(self, mcx)
    }
}

impl<'mcx> XmlExpr<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Mirrors the
    /// [`Expr::XmlExpr`] [`Expr::clone_in`] arm — the derived `.clone()` recurses
    /// into the `Vec<Expr<'mcx>>` children (which may carry a panicking `Aggref`), so
    /// callers that deep-copy an `XmlExpr` must route through here.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<XmlExpr<'b>> {
        Ok(XmlExpr {
            op: self.op,
            name: self.name.clone(),
            named_args: clone_vec_expr(&self.named_args, mcx)?,
            arg_names: self.arg_names.clone(),
            args: clone_vec_expr(&self.args, mcx)?,
            xmloption: self.xmloption,
            indent: self.indent,
            r#type: self.r#type,
            typmod: self.typmod,
            location: self.location,
        })
    }
}

impl<'mcx> GroupingFunc<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Mirrors the
    /// [`Expr::GroupingFunc`] [`Expr::clone_in`] arm.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GroupingFunc<'b>> {
        Ok(GroupingFunc {
            args: clone_vec_expr(&self.args, mcx)?,
            refs: self.refs.clone(),
            cols: self.cols.clone(),
            agglevelsup: self.agglevelsup,
            location: self.location,
        })
    }
}

impl MergeSupportFunc {
    /// Deep copy (C: `copyObject` shape). `MergeSupportFunc` is `Copy` (no
    /// node children), so this is a bitwise copy; the `mcx` is accepted for a
    /// uniform `clone_in` signature.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<MergeSupportFunc> {
        Ok(*self)
    }
}

impl<'mcx> ReturningExpr<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Mirrors the
    /// [`Expr::ReturningExpr`] [`Expr::clone_in`] arm.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReturningExpr<'b>> {
        Ok(ReturningExpr {
            retlevelsup: self.retlevelsup,
            retold: self.retold,
            retexpr: clone_opt_box_expr(&self.retexpr, mcx)?,
        })
    }
}

/// Handle into the planner's `RestrictInfo` arena (`RinfoId` in
/// `types-pathnodes`), as embedded inside an [`Expr`] tree. C casts a
/// `RestrictInfo *` to `Expr *` so a RestrictInfo node can live as a child of a
/// BoolExpr (the `orclause` produced by `make_sub_restrictinfos`); the
/// owned-tree analogue carries the arena index, mirroring the
/// `SlotId`/`EcxtId`/`ResultCellId` handle precedent. `types-pathnodes` converts
/// between its `RinfoId` and this `RinfoRef`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct RinfoRef(pub u32);

/// Expression-tree node (`Expr *` in C). The `NodeTag` is the enum
/// discriminant (`IsA(node, Var)` is a match on the variant), so
/// `ExecInitExprRec`'s switch over the node tag is a `match` over this enum.
///
/// One variant per C node type deriving from `Expr` (the node having
/// `Expr xpr;` as its first field). Lifetime-free: child expressions are owned
/// `Box<Expr<'mcx>>` and lists are `Vec<…>`, matching `OpExpr`/`ScalarArrayOpExpr`
/// and keeping the non-exhaustive enum additive for existing consumers.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Expr<'mcx> {
    /// `T_Var`.
    Var(Var),
    /// `T_Const`.
    Const(Const<'mcx>),
    /// `T_Param`.
    Param(Param),
    /// `T_Aggref`.
    Aggref(Aggref<'mcx>),
    /// `T_GroupingFunc`.
    GroupingFunc(GroupingFunc<'mcx>),
    /// `T_WindowFunc`.
    WindowFunc(WindowFunc<'mcx>),
    /// `T_WindowFuncRunCondition`.
    WindowFuncRunCondition(WindowFuncRunCondition<'mcx>),
    /// `T_SubscriptingRef`.
    SubscriptingRef(SubscriptingRef<'mcx>),
    /// `T_FuncExpr`.
    FuncExpr(FuncExpr<'mcx>),
    /// `T_NamedArgExpr`.
    NamedArgExpr(NamedArgExpr<'mcx>),
    /// `T_OpExpr`.
    OpExpr(OpExpr<'mcx>),
    /// `T_DistinctExpr` — same payload as [`OpExpr`] (C: `typedef OpExpr`).
    DistinctExpr(OpExpr<'mcx>),
    /// `T_NullIfExpr` — same payload as [`OpExpr`] (C: `typedef OpExpr`).
    NullIfExpr(OpExpr<'mcx>),
    /// `T_ScalarArrayOpExpr`.
    ScalarArrayOpExpr(ScalarArrayOpExpr<'mcx>),
    /// `T_BoolExpr`.
    BoolExpr(BoolExpr<'mcx>),
    /// `T_SubLink`.
    SubLink(SubLink<'mcx>),
    /// `T_SubPlan`.
    SubPlan(SubPlanExpr<'mcx>),
    /// `T_AlternativeSubPlan`.
    AlternativeSubPlan(AlternativeSubPlanExpr<'mcx>),
    /// `T_FieldSelect`.
    FieldSelect(FieldSelect<'mcx>),
    /// `T_FieldStore`.
    FieldStore(FieldStore<'mcx>),
    /// `T_RelabelType`.
    RelabelType(RelabelType<'mcx>),
    /// `T_CoerceViaIO`.
    CoerceViaIO(CoerceViaIO<'mcx>),
    /// `T_ArrayCoerceExpr`.
    ArrayCoerceExpr(ArrayCoerceExpr<'mcx>),
    /// `T_ConvertRowtypeExpr`.
    ConvertRowtypeExpr(ConvertRowtypeExpr<'mcx>),
    /// `T_CollateExpr`.
    CollateExpr(CollateExpr<'mcx>),
    /// `T_CaseExpr`.
    CaseExpr(CaseExpr<'mcx>),
    /// `T_CaseTestExpr`.
    CaseTestExpr(CaseTestExpr),
    /// `T_ArrayExpr`.
    ArrayExpr(ArrayExpr<'mcx>),
    /// `T_RowExpr`.
    RowExpr(RowExpr<'mcx>),
    /// `T_RowCompareExpr`.
    RowCompareExpr(RowCompareExpr<'mcx>),
    /// `T_CoalesceExpr`.
    CoalesceExpr(CoalesceExpr<'mcx>),
    /// `T_MinMaxExpr`.
    MinMaxExpr(MinMaxExpr<'mcx>),
    /// `T_SQLValueFunction`.
    SQLValueFunction(SQLValueFunction),
    /// `T_XmlExpr`.
    XmlExpr(XmlExpr<'mcx>),
    /// `T_JsonValueExpr`.
    JsonValueExpr(JsonValueExpr<'mcx>),
    /// `T_JsonConstructorExpr`.
    JsonConstructorExpr(JsonConstructorExpr<'mcx>),
    /// `T_JsonIsPredicate`.
    JsonIsPredicate(JsonIsPredicate<'mcx>),
    /// `T_JsonExpr`.
    JsonExpr(JsonExpr<'mcx>),
    /// `T_NullTest`.
    NullTest(NullTest<'mcx>),
    /// `T_BooleanTest`.
    BooleanTest(BooleanTest<'mcx>),
    /// `T_MergeSupportFunc`.
    MergeSupportFunc(MergeSupportFunc),
    /// `T_CoerceToDomain`.
    CoerceToDomain(CoerceToDomain<'mcx>),
    /// `T_CoerceToDomainValue`.
    CoerceToDomainValue(CoerceToDomainValue),
    /// `T_SetToDefault`.
    SetToDefault(SetToDefault),
    /// `T_CurrentOfExpr`.
    CurrentOfExpr(CurrentOfExpr),
    /// `T_NextValueExpr`.
    NextValueExpr(NextValueExpr),
    /// `T_InferenceElem`.
    InferenceElem(InferenceElem<'mcx>),
    /// `T_ReturningExpr`.
    ReturningExpr(ReturningExpr<'mcx>),
    /// `T_PlaceHolderVar` (nodes/pathnodes.h) — a planner placeholder node. Not
    /// in the contiguous executor `Expr` run, but it derives from `Expr` in C
    /// and the optimizer dispatches it via `IsA`/`match`.
    PlaceHolderVar(PlaceHolderVar<'mcx>),
    /// `T_RestrictInfo` (nodes/pathnodes.h) — a planner RestrictInfo embedded in
    /// an expression tree. C casts `RestrictInfo *` to `Expr *` to place it
    /// inside the `orclause` BoolExpr built by `make_sub_restrictinfos`; carried
    /// here as the [`RinfoRef`] arena handle.
    RestrictInfo(RinfoRef),
}

// Generated `etag` module + `impl Expr { expr_tag() }` — the Expr-side tag
// dispatch surface (node-opaque P3), mirroring `Node::node_tag()`/`ntag`.
// File-scoped: contains both `pub mod etag` and an `impl Expr` block.
include!(concat!(env!("OUT_DIR"), "/expr_tag.rs"));

impl<'mcx> Expr<'mcx> {
    /// `castNode(Var, node)` — borrow the [`Var`] payload, or `None` if this is
    /// not an `Expr::Var`. The optimizer uses this where C writes
    /// `(Var *) node` after an `IsA(node, Var)` test.
    #[inline]
    pub fn expect_var(&self) -> Option<&Var> {
        match self {
            Expr::Var(v) => Some(v),
            _ => None,
        }
    }

    /// Mutable variant of [`Expr::expect_var`] — e.g. `build_joinrel_tlist`
    /// (relnode.c) sets `var->varnullingrels`.
    #[inline]
    pub fn expect_var_mut(&mut self) -> Option<&mut Var> {
        match self {
            Expr::Var(v) => Some(v),
            _ => None,
        }
    }

    /// `castNode(OpExpr, node)` — borrow the [`OpExpr`] payload, or `None`.
    #[inline]
    pub fn expect_opexpr(&self) -> Option<&OpExpr<'mcx>> {
        match self {
            Expr::OpExpr(o) => Some(o),
            _ => None,
        }
    }

    /// `castNode(PlaceHolderVar, node)` — borrow the [`PlaceHolderVar`] payload,
    /// or `None` if this is not an `Expr::PlaceHolderVar`.
    #[inline]
    pub fn expect_placeholdervar(&self) -> Option<&PlaceHolderVar<'mcx>> {
        match self {
            Expr::PlaceHolderVar(p) => Some(p),
            _ => None,
        }
    }

    /// Mutable variant of [`Expr::expect_placeholdervar`].
    #[inline]
    pub fn expect_placeholdervar_mut(&mut self) -> Option<&mut PlaceHolderVar<'mcx>> {
        match self {
            Expr::PlaceHolderVar(p) => Some(p),
            _ => None,
        }
    }
}

// ===========================================================================
// Borrowing / owning accessors used by the optimizer (clauses.c et al.).
//
// These mirror C's `IsA(node, X)` (the `is_*`), `castNode(X, node)` (the
// `as_*` borrows), and the owned-tree "take the payload out of the node"
// pattern (`expect_into_*`) used by the const-folding mutator, where C reuses
// the node's storage in place. Added field-for-field as the optimizer
// consumers landed; one accessor per modeled variant the optimizer dispatches.
// ===========================================================================

// The FULL `is_/as_/as_*_mut/expect_/into_/expect_into_` accessor set for every
// `Expr` variant — generated by types-nodes/build.rs (node-opaque migration P3)
// as enum matches over the hand-written `enum Expr`, the Expr-side mirror of the
// generated `impl Node` accessors. Hand-written names below are skipped by the
// generator (reconcile, don't collide), so the two blocks never conflict. This
// supersedes the old hand-rolled `expr_accessors!` macro and per-variant impls.
include!(concat!(env!("OUT_DIR"), "/expr_accessors.rs"));

impl<'mcx> Expr<'mcx> {
    /// `castNode(Var, node)` borrow (mirrors [`Expr::expect_var`], named `as_var`
    /// for parity; kept hand-written to share the `expect_var` body).
    #[inline]
    pub fn as_var(&self) -> Option<&Var> {
        self.expect_var()
    }
    /// `castNode(OpExpr, node)` for a `DistinctExpr` payload (struct-equal to
    /// `OpExpr`). Distinct from the generated `as_distinctexpr` only in name;
    /// kept because the generated `as_distinctexpr` already returns `&OpExpr`.
    #[inline]
    pub fn as_distinctexpr(&self) -> Option<&OpExpr<'mcx>> {
        match self {
            Expr::DistinctExpr(o) => Some(o),
            _ => None,
        }
    }
    /// `castNode(OpExpr, node)` for a `NullIfExpr` payload.
    #[inline]
    pub fn as_nullifexpr(&self) -> Option<&OpExpr<'mcx>> {
        match self {
            Expr::NullIfExpr(o) => Some(o),
            _ => None,
        }
    }
    /// `castNode(PlaceHolderVar, node)` borrow (parity alias over
    /// [`Expr::expect_placeholdervar`]).
    #[inline]
    pub fn as_placeholdervar(&self) -> Option<&PlaceHolderVar<'mcx>> {
        self.expect_placeholdervar()
    }
    /// `castNode(SubPlan, node)` — borrow the [`SubPlanExpr`] payload, or `None`.
    #[inline]
    pub fn as_subplan(&self) -> Option<&SubPlanExpr<'mcx>> {
        match self {
            Expr::SubPlan(s) => Some(s),
            _ => None,
        }
    }
    /// `castNode(AlternativeSubPlan, node)` — borrow the payload, or `None`.
    #[inline]
    pub fn as_alternativesubplan(&self) -> Option<&AlternativeSubPlanExpr<'mcx>> {
        match self {
            Expr::AlternativeSubPlan(s) => Some(s),
            _ => None,
        }
    }
}

// ===========================================================================
// `Expr::clone_in` — the sanctioned deep-copy path for the lifetime-free Expr
// tree (C: `copyObject` over an `Expr *`).
//
// The lifetime-free `Expr` enum derives `Clone`, but that derived `clone()` is
// only a shallow copy and PANICS for the `Aggref`/`WindowFunc`/`SubLink`/
// `SubPlan`/`AlternativeSubPlan` payloads (their `Clone` impls / embedded
// children deliberately panic — see those structs). `clone_in` is the deep path
// the planner uses to store owned copies of analyzed nodes into the lifetime-
// free planner arena: it recurses field-by-field, allocating every child into
// `mcx`, and never calls a panicking `.clone()`.
//
// The handful of children that carry the `'static` notional lifetime of the
// Expr tree (Aggref::args is `Vec<TargetEntry<'static>>`, SubLink::subselect is
// `PgBox<'static, Query<'static>>`, SubPlanExpr is `Box<SubPlan<'static>>`) are
// deep-cloned into `mcx` then their lifetime parameter is erased back to
// `'static` — the same convention used by `tlist_into_static` /
// `query_into_static` in the parser (the data is fully owned in `mcx`; the
// arena outlives the planner run in practice).
// ===========================================================================

/// Deep-clone a `&Box<Expr<'mcx>>` child into a freshly allocated `Box<Expr<'mcx>>` (the
/// global-allocator boxes that make up the Expr tree), recursing via
/// [`Expr::clone_in`].
fn clone_box_expr<'b>(e: &Box<Expr<'_>>, mcx: Mcx<'b>) -> PgResult<Box<Expr<'b>>> {
    Ok(Box::new(e.clone_in(mcx)?))
}

/// Deep-clone an optional `Box<Expr<'mcx>>` child.
fn clone_opt_box_expr<'b>(
    e: &Option<Box<Expr<'_>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<Box<Expr<'b>>>> {
    match e {
        Some(b) => Ok(Some(clone_box_expr(b, mcx)?)),
        None => Ok(None),
    }
}

/// Deep-clone a `Vec<Expr<'mcx>>` child list.
fn clone_vec_expr<'b>(v: &[Expr<'_>], mcx: Mcx<'b>) -> PgResult<Vec<Expr<'b>>> {
    let mut out = Vec::with_capacity(v.len());
    for e in v.iter() {
        out.push(e.clone_in(mcx)?);
    }
    Ok(out)
}

/// Deep-clone a `Vec<Option<Expr<'mcx>>>` child list (slice-subscript index lists may
/// hold NULL elements).
fn clone_vec_opt_expr<'b>(
    v: &[Option<Expr<'_>>],
    mcx: Mcx<'b>,
) -> PgResult<Vec<Option<Expr<'b>>>> {
    let mut out = Vec::with_capacity(v.len());
    for e in v.iter() {
        out.push(match e {
            Some(x) => Some(x.clone_in(mcx)?),
            None => None,
        });
    }
    Ok(out)
}

/// Deep-clone a `CaseWhen` arm (carried inline in [`CaseExpr`]).
fn clone_case_when<'b>(w: &CaseWhen<'_>, mcx: Mcx<'b>) -> PgResult<CaseWhen<'b>> {
    Ok(CaseWhen {
        expr: clone_opt_box_expr(&w.expr, mcx)?,
        result: clone_opt_box_expr(&w.result, mcx)?,
        location: w.location,
    })
}

/// Deep-clone a `JsonBehavior` (ON ERROR / ON EMPTY) node.
fn clone_json_behavior<'b>(
    b: &JsonBehavior<'_>,
    mcx: Mcx<'b>,
) -> PgResult<JsonBehavior<'b>> {
    Ok(JsonBehavior {
        btype: b.btype,
        expr: clone_opt_box_expr(&b.expr, mcx)?,
        coerce: b.coerce,
        location: b.location,
    })
}

impl<'mcx> Expr<'mcx> {
    /// Deep copy this expression into `mcx` (C: `copyObject` over an `Expr *`).
    ///
    /// The sanctioned deep-copy path for the lifetime-free `Expr` tree:
    /// recurses field-by-field through every variant, allocating each child into
    /// `mcx`, and never calls a panicking shallow `.clone()` on an
    /// `Aggref`/`WindowFunc`/`SubLink`/`SubPlan` payload.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Expr<'b>> {
        Ok(match self {
            // --- trivial / Copy / no Expr children: shallow `.clone()` is safe.
            Expr::Var(v) => Expr::Var(v.clone()),
            // `Const.constvalue` is the by-ref `Datum` carrier that the campaign
            // makes honest: deep-clone it into `mcx` so the result is tied to the
            // mcx lifetime `'b` (no forged `'static`).
            Expr::Const(c) => Expr::Const(Const {
                consttype: c.consttype,
                consttypmod: c.consttypmod,
                constcollid: c.constcollid,
                constlen: c.constlen,
                constvalue: c.constvalue.clone_in(mcx)?,
                constisnull: c.constisnull,
                constbyval: c.constbyval,
                location: c.location,
            }),
            Expr::Param(p) => Expr::Param(p.clone()),
            Expr::CaseTestExpr(c) => Expr::CaseTestExpr(*c),
            Expr::SQLValueFunction(s) => Expr::SQLValueFunction(*s),
            Expr::CoerceToDomainValue(c) => Expr::CoerceToDomainValue(*c),
            Expr::SetToDefault(s) => Expr::SetToDefault(*s),
            Expr::CurrentOfExpr(c) => Expr::CurrentOfExpr(c.clone()),
            Expr::NextValueExpr(n) => Expr::NextValueExpr(*n),
            Expr::MergeSupportFunc(m) => Expr::MergeSupportFunc(*m),
            Expr::RestrictInfo(r) => Expr::RestrictInfo(*r),

            // --- variants whose only children are scalar/Copy lists: the
            //     derived `.clone()` recurses into no panicking node, so it is a
            //     correct deep copy.
            Expr::GroupingFunc(g) => Expr::GroupingFunc(GroupingFunc {
                args: clone_vec_expr(&g.args, mcx)?,
                refs: g.refs.clone(),
                cols: g.cols.clone(),
                agglevelsup: g.agglevelsup,
                location: g.location,
            }),

            // --- variants with Box<Expr<'mcx>> / Vec<Expr<'mcx>> children: recurse.
            Expr::OpExpr(o) => Expr::OpExpr(clone_opexpr(o, mcx)?),
            Expr::DistinctExpr(o) => Expr::DistinctExpr(clone_opexpr(o, mcx)?),
            Expr::NullIfExpr(o) => Expr::NullIfExpr(clone_opexpr(o, mcx)?),
            Expr::ScalarArrayOpExpr(s) => Expr::ScalarArrayOpExpr(ScalarArrayOpExpr {
                opno: s.opno,
                opfuncid: s.opfuncid,
                hashfuncid: s.hashfuncid,
                negfuncid: s.negfuncid,
                useOr: s.useOr,
                inputcollid: s.inputcollid,
                args: clone_vec_expr(&s.args, mcx)?,
                location: s.location,
            }),
            Expr::BoolExpr(b) => Expr::BoolExpr(BoolExpr {
                boolop: b.boolop,
                args: clone_vec_expr(&b.args, mcx)?,
                location: b.location,
            }),
            Expr::FuncExpr(f) => Expr::FuncExpr(FuncExpr {
                funcid: f.funcid,
                funcresulttype: f.funcresulttype,
                funcretset: f.funcretset,
                funcvariadic: f.funcvariadic,
                funcformat: f.funcformat,
                funccollid: f.funccollid,
                inputcollid: f.inputcollid,
                args: clone_vec_expr(&f.args, mcx)?,
                location: f.location,
            }),
            Expr::NamedArgExpr(n) => Expr::NamedArgExpr(NamedArgExpr {
                arg: clone_opt_box_expr(&n.arg, mcx)?,
                name: n.name.clone(),
                argnumber: n.argnumber,
                location: n.location,
            }),
            Expr::SubscriptingRef(s) => Expr::SubscriptingRef(SubscriptingRef {
                refcontainertype: s.refcontainertype,
                refelemtype: s.refelemtype,
                refrestype: s.refrestype,
                reftypmod: s.reftypmod,
                refcollid: s.refcollid,
                refupperindexpr: clone_vec_opt_expr(&s.refupperindexpr, mcx)?,
                reflowerindexpr: clone_vec_opt_expr(&s.reflowerindexpr, mcx)?,
                refexpr: clone_opt_box_expr(&s.refexpr, mcx)?,
                refassgnexpr: clone_opt_box_expr(&s.refassgnexpr, mcx)?,
            }),
            Expr::FieldSelect(f) => Expr::FieldSelect(FieldSelect {
                arg: clone_opt_box_expr(&f.arg, mcx)?,
                fieldnum: f.fieldnum,
                resulttype: f.resulttype,
                resulttypmod: f.resulttypmod,
                resultcollid: f.resultcollid,
            }),
            Expr::FieldStore(f) => Expr::FieldStore(FieldStore {
                arg: clone_opt_box_expr(&f.arg, mcx)?,
                newvals: clone_vec_expr(&f.newvals, mcx)?,
                fieldnums: f.fieldnums.clone(),
                resulttype: f.resulttype,
            }),
            Expr::RelabelType(r) => Expr::RelabelType(RelabelType {
                arg: clone_opt_box_expr(&r.arg, mcx)?,
                resulttype: r.resulttype,
                resulttypmod: r.resulttypmod,
                resultcollid: r.resultcollid,
                relabelformat: r.relabelformat,
                location: r.location,
            }),
            Expr::CoerceViaIO(c) => Expr::CoerceViaIO(CoerceViaIO {
                arg: clone_opt_box_expr(&c.arg, mcx)?,
                resulttype: c.resulttype,
                resultcollid: c.resultcollid,
                coerceformat: c.coerceformat,
                location: c.location,
            }),
            Expr::ArrayCoerceExpr(a) => Expr::ArrayCoerceExpr(ArrayCoerceExpr {
                arg: clone_opt_box_expr(&a.arg, mcx)?,
                elemexpr: clone_opt_box_expr(&a.elemexpr, mcx)?,
                resulttype: a.resulttype,
                resulttypmod: a.resulttypmod,
                resultcollid: a.resultcollid,
                coerceformat: a.coerceformat,
                location: a.location,
            }),
            Expr::ConvertRowtypeExpr(c) => Expr::ConvertRowtypeExpr(ConvertRowtypeExpr {
                arg: clone_opt_box_expr(&c.arg, mcx)?,
                resulttype: c.resulttype,
                convertformat: c.convertformat,
                location: c.location,
            }),
            Expr::CollateExpr(c) => Expr::CollateExpr(CollateExpr {
                arg: clone_opt_box_expr(&c.arg, mcx)?,
                collOid: c.collOid,
                location: c.location,
            }),
            Expr::CaseExpr(c) => {
                let mut args = Vec::with_capacity(c.args.len());
                for w in c.args.iter() {
                    args.push(clone_case_when(w, mcx)?);
                }
                Expr::CaseExpr(CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg: clone_opt_box_expr(&c.arg, mcx)?,
                    args,
                    defresult: clone_opt_box_expr(&c.defresult, mcx)?,
                    location: c.location,
                })
            }
            Expr::ArrayExpr(a) => Expr::ArrayExpr(ArrayExpr {
                array_typeid: a.array_typeid,
                array_collid: a.array_collid,
                element_typeid: a.element_typeid,
                elements: clone_vec_expr(&a.elements, mcx)?,
                multidims: a.multidims,
                location: a.location,
            }),
            Expr::RowExpr(r) => Expr::RowExpr(RowExpr {
                args: clone_vec_expr(&r.args, mcx)?,
                row_typeid: r.row_typeid,
                row_format: r.row_format,
                colnames: r.colnames.clone(),
                location: r.location,
            }),
            Expr::RowCompareExpr(r) => Expr::RowCompareExpr(RowCompareExpr {
                cmptype: r.cmptype,
                opnos: r.opnos.clone(),
                opfamilies: r.opfamilies.clone(),
                inputcollids: r.inputcollids.clone(),
                largs: clone_vec_expr(&r.largs, mcx)?,
                rargs: clone_vec_expr(&r.rargs, mcx)?,
            }),
            Expr::CoalesceExpr(c) => Expr::CoalesceExpr(CoalesceExpr {
                coalescetype: c.coalescetype,
                coalescecollid: c.coalescecollid,
                args: clone_vec_expr(&c.args, mcx)?,
                location: c.location,
            }),
            Expr::MinMaxExpr(m) => Expr::MinMaxExpr(MinMaxExpr {
                minmaxtype: m.minmaxtype,
                minmaxcollid: m.minmaxcollid,
                inputcollid: m.inputcollid,
                op: m.op,
                args: clone_vec_expr(&m.args, mcx)?,
                location: m.location,
            }),
            Expr::XmlExpr(x) => Expr::XmlExpr(XmlExpr {
                op: x.op,
                name: x.name.clone(),
                named_args: clone_vec_expr(&x.named_args, mcx)?,
                arg_names: x.arg_names.clone(),
                args: clone_vec_expr(&x.args, mcx)?,
                xmloption: x.xmloption,
                indent: x.indent,
                r#type: x.r#type,
                typmod: x.typmod,
                location: x.location,
            }),
            Expr::JsonValueExpr(j) => Expr::JsonValueExpr(JsonValueExpr {
                raw_expr: clone_opt_box_expr(&j.raw_expr, mcx)?,
                formatted_expr: clone_opt_box_expr(&j.formatted_expr, mcx)?,
                format: j.format,
            }),
            Expr::JsonConstructorExpr(j) => Expr::JsonConstructorExpr(JsonConstructorExpr {
                r#type: j.r#type,
                args: clone_vec_expr(&j.args, mcx)?,
                func: clone_opt_box_expr(&j.func, mcx)?,
                coercion: clone_opt_box_expr(&j.coercion, mcx)?,
                returning: j.returning,
                absent_on_null: j.absent_on_null,
                unique: j.unique,
                location: j.location,
            }),
            Expr::JsonIsPredicate(j) => Expr::JsonIsPredicate(JsonIsPredicate {
                expr: clone_opt_box_expr(&j.expr, mcx)?,
                format: j.format,
                item_type: j.item_type,
                unique_keys: j.unique_keys,
                location: j.location,
            }),
            Expr::JsonExpr(j) => {
                let mut passing_values = Vec::with_capacity(j.passing_values.len());
                for e in j.passing_values.iter() {
                    passing_values.push(e.clone_in(mcx)?);
                }
                let on_empty = match &j.on_empty {
                    Some(b) => Some(Box::new(clone_json_behavior(b, mcx)?)),
                    None => None,
                };
                let on_error = match &j.on_error {
                    Some(b) => Some(Box::new(clone_json_behavior(b, mcx)?)),
                    None => None,
                };
                Expr::JsonExpr(JsonExpr {
                    op: j.op,
                    column_name: j.column_name.clone(),
                    formatted_expr: clone_opt_box_expr(&j.formatted_expr, mcx)?,
                    format: j.format,
                    path_spec: clone_opt_box_expr(&j.path_spec, mcx)?,
                    returning: j.returning,
                    passing_names: j.passing_names.clone(),
                    passing_values,
                    on_empty,
                    on_error,
                    use_io_coercion: j.use_io_coercion,
                    use_json_coercion: j.use_json_coercion,
                    wrapper: j.wrapper,
                    omit_quotes: j.omit_quotes,
                    collation: j.collation,
                    location: j.location,
                })
            }
            Expr::NullTest(n) => Expr::NullTest(NullTest {
                arg: clone_opt_box_expr(&n.arg, mcx)?,
                nulltesttype: n.nulltesttype,
                argisrow: n.argisrow,
                location: n.location,
            }),
            Expr::BooleanTest(b) => Expr::BooleanTest(BooleanTest {
                arg: clone_opt_box_expr(&b.arg, mcx)?,
                booltesttype: b.booltesttype,
                location: b.location,
            }),
            Expr::CoerceToDomain(c) => Expr::CoerceToDomain(CoerceToDomain {
                arg: clone_opt_box_expr(&c.arg, mcx)?,
                resulttype: c.resulttype,
                resulttypmod: c.resulttypmod,
                resultcollid: c.resultcollid,
                coercionformat: c.coercionformat,
                location: c.location,
            }),
            Expr::InferenceElem(i) => Expr::InferenceElem(InferenceElem {
                expr: clone_opt_box_expr(&i.expr, mcx)?,
                infercollid: i.infercollid,
                inferopclass: i.inferopclass,
            }),
            Expr::ReturningExpr(r) => Expr::ReturningExpr(ReturningExpr {
                retlevelsup: r.retlevelsup,
                retold: r.retold,
                retexpr: clone_opt_box_expr(&r.retexpr, mcx)?,
            }),
            Expr::PlaceHolderVar(p) => Expr::PlaceHolderVar(PlaceHolderVar {
                phexpr: clone_opt_box_expr(&p.phexpr, mcx)?,
                phrels: p.phrels.clone(),
                phnullingrels: p.phnullingrels.clone(),
                phid: p.phid,
                phlevelsup: p.phlevelsup,
            }),

            // --- the panicking-Clone variants: deep-copy via their owned-child
            //     paths (never `.clone()`), erasing the `'static` notional
            //     lifetime back after the in-mcx clone (cf. tlist_into_static /
            //     query_into_static).
            Expr::Aggref(a) => Expr::Aggref(clone_aggref(a, mcx)?),
            Expr::WindowFunc(w) => Expr::WindowFunc(WindowFunc {
                winfnoid: w.winfnoid,
                wintype: w.wintype,
                wincollid: w.wincollid,
                inputcollid: w.inputcollid,
                args: clone_vec_expr(&w.args, mcx)?,
                aggfilter: clone_opt_box_expr(&w.aggfilter, mcx)?,
                runCondition: clone_vec_expr(&w.runCondition, mcx)?,
                winref: w.winref,
                winstar: w.winstar,
                winagg: w.winagg,
                location: w.location,
            }),
            Expr::WindowFuncRunCondition(w) => {
                Expr::WindowFuncRunCondition(w.clone_in(mcx)?)
            }
            Expr::SubLink(s) => Expr::SubLink(clone_sublink(s, mcx)?),
            Expr::SubPlan(s) => Expr::SubPlan(SubPlanExpr(clone_subplan_static(&s.0, mcx)?)),
            Expr::AlternativeSubPlan(a) => {
                let mut subplans: Vec<PgBox<'b, SubPlan<'b>>> =
                    Vec::with_capacity(a.0.subplans.len());
                for sp in a.0.subplans.iter() {
                    // Deep-clone each SubPlan into mcx; the result is tied to the
                    // mcx lifetime `'b` (no `'static` erasure — the campaign makes
                    // the Expr tree's lifetime honest).
                    let owned: SubPlan<'b> = sp.clone_in(mcx)?;
                    let boxed: PgBox<'b, SubPlan<'b>> = alloc_in(mcx, owned)?;
                    subplans.push(boxed);
                }
                Expr::AlternativeSubPlan(AlternativeSubPlanExpr(Box::new(
                    AlternativeSubPlan { subplans },
                )))
            }
        })
    }
}

/// Deep-clone an [`OpExpr`] payload (shared by `OpExpr`/`DistinctExpr`/
/// `NullIfExpr`, which carry the same struct).
fn clone_opexpr<'b>(o: &OpExpr<'_>, mcx: Mcx<'b>) -> PgResult<OpExpr<'b>> {
    Ok(OpExpr {
        opno: o.opno,
        opfuncid: o.opfuncid,
        opresulttype: o.opresulttype,
        opretset: o.opretset,
        opcollid: o.opcollid,
        inputcollid: o.inputcollid,
        args: clone_vec_expr(&o.args, mcx)?,
        location: o.location,
    })
}

/// Deep-clone an [`Aggref`]: `args` is a `TargetEntry` list deep-copied via the
/// existing [`TargetEntry::clone_in`], and the result is re-erased to `'static`
/// to match the lifetime-free Expr tree (cf. `tlist_into_static`).
fn clone_aggref<'b>(a: &Aggref<'_>, mcx: Mcx<'b>) -> PgResult<Aggref<'b>> {
    let mut args: Vec<TargetEntry<'b>> = Vec::with_capacity(a.args.len());
    for te in a.args.iter() {
        let cloned: TargetEntry<'b> = te.clone_in(mcx)?;
        args.push(cloned);
    }
    Ok(Aggref {
        aggfnoid: a.aggfnoid,
        aggtype: a.aggtype,
        aggcollid: a.aggcollid,
        inputcollid: a.inputcollid,
        aggtranstype: a.aggtranstype,
        aggargtypes: a.aggargtypes.clone(),
        aggdirectargs: clone_vec_expr(&a.aggdirectargs, mcx)?,
        args,
        aggorder: a.aggorder.clone(),
        aggdistinct: a.aggdistinct.clone(),
        aggfilter: clone_opt_box_expr(&a.aggfilter, mcx)?,
        aggstar: a.aggstar,
        aggvariadic: a.aggvariadic,
        aggkind: a.aggkind,
        aggpresorted: a.aggpresorted,
        agglevelsup: a.agglevelsup,
        aggsplit: a.aggsplit,
        aggno: a.aggno,
        aggtransno: a.aggtransno,
        location: a.location,
    })
}

/// Box an mcx-owned `Query<'mcx>` into the lifetime-free `Expr` tree's `'static`
/// notional `subselect` slot (the carrier [`SubLink::subselect`] /
/// [`crate::parsenodes::RangeTblEntry`]-style embedded sub-query uses). The data
/// is fully owned in `mcx`; this is a lifetime-parameter-only erase, the exact
/// idiom [`clone_sublink`] performs inline (cf. `query_into_static` in the
/// parser). Lives here, in the unsafe-permitting `types-nodes` crate, so
/// `#![forbid(unsafe_code)]` callers (e.g. readfuncs) can reconstruct the slot.
/// Erase a fully-mcx-owned [`TargetEntry`]'s lifetime to the Expr tree's
/// `'static` notional lifetime, the slot [`Aggref::args`] uses. The data
/// (`expr`/`resname` children) is fully owned in `mcx`; this is a
/// lifetime-parameter-only transmute (the exact idiom [`clone_aggref`] and
/// `tlist_into_static` perform inline). Lives here, in the unsafe-permitting
/// `types-nodes` crate, so `#![forbid(unsafe_code)]` callers (e.g. readfuncs)
/// can reconstruct `Aggref.args` after reading the framed `TargetEntry`
/// children off the node-string cursor.
pub fn targetentry_into_static(te: TargetEntry<'_>) -> TargetEntry<'static> {
    // SAFETY: `te`'s children are fully owned in mcx; lifetime-parameter-only
    // erase to the Expr tree's 'static notional lifetime (cf. clone_aggref).
    unsafe { core::mem::transmute(te) }
}

/// Erase a [`PlaceHolderVar`]'s lifetime to the planner arena's notional
/// `'static` (sibling of [`targetentry_into_static`]). The PHV's `phexpr`
/// subtree is fully owned (moved in), so this is a lifetime-parameter-only
/// erase — used when a `PlaceHolderInfo`/`NestLoopParam` interns a PHV into the
/// planner-run arena that, not Rust's borrow tracker, governs its validity.
pub fn placeholdervar_into_static(phv: PlaceHolderVar<'_>) -> PlaceHolderVar<'static> {
    // SAFETY: `phv`'s children are fully owned; lifetime-parameter-only erase to
    // the Expr tree's 'static notional lifetime (cf. targetentry_into_static).
    unsafe { core::mem::transmute(phv) }
}

impl<'mcx> Expr<'mcx> {
    /// Erase an `Expr<'mcx>`'s lifetime to the planner `node_arena`'s notional
    /// `'static`. The `node_arena` is an index-handle (`NodeId`) intern table that
    /// *owns* its nodes for the planner run and is addressed by dense index, not by
    /// borrow — exactly the `RinfoRef(u32)` handle-space carve-out the Expr-`'mcx`
    /// campaign excludes from the borrow check. This is the single sanctioned
    /// arena-intern erasure (sibling of [`targetentry_into_static`]), living here in
    /// the unsafe-permitting `types-nodes` crate so the `#![forbid(unsafe_code)]`
    /// `types-pathnodes` arena (`PlannerInfo::alloc_node`) can intern into it.
    ///
    /// SAFETY: `Expr<'mcx>` and `Expr<'static>` are the same type up to the
    /// (invariant) lifetime parameter; the data is fully owned (moved in), so this
    /// is a lifetime-parameter-only erase. The interning arena outlives no longer
    /// than the planner run that produced the node, so the notional `'static` is
    /// never observed as a real `'static` borrow.
    #[must_use]
    pub fn erase_lifetime(self) -> Expr<'static> {
        unsafe { core::mem::transmute::<Expr<'mcx>, Expr<'static>>(self) }
    }
}

pub fn query_box_into_static<'b>(
    q: crate::copy_query::Query<'b>,
    mcx: Mcx<'b>,
) -> PgResult<PgBox<'static, crate::copy_query::Query<'static>>> {
    let boxed: PgBox<'b, crate::copy_query::Query<'b>> = alloc_in(mcx, q)?;
    // SAFETY: fully owned in mcx; lifetime-parameter-only erase to the Expr
    // tree's 'static notional lifetime (cf. clone_sublink / query_into_static).
    Ok(unsafe { core::mem::transmute(boxed) })
}

/// Deep-clone a [`SubLink`]: `subselect` is an embedded owned `Query` deep-cloned
/// via [`crate::copy_query::Query::clone_in`] then re-erased to `'static` (cf.
/// `query_into_static`); `testexpr` recurses via [`Expr::clone_in`].
fn clone_sublink<'b>(s: &SubLink<'_>, mcx: Mcx<'b>) -> PgResult<SubLink<'b>> {
    let subselect = match &s.subselect {
        Some(q) => {
            let owned: crate::copy_query::Query<'b> = q.clone_in(mcx)?;
            let boxed: PgBox<'b, crate::copy_query::Query<'b>> = alloc_in(mcx, owned)?;
            Some(boxed)
        }
        None => None,
    };
    Ok(SubLink {
        subLinkType: s.subLinkType,
        subLinkId: s.subLinkId,
        testexpr: clone_opt_box_expr(&s.testexpr, mcx)?,
        operName: s.operName.clone(),
        subselect,
        location: s.location,
    })
}

/// Deep-clone a `Box<SubPlan<'static>>` (the [`SubPlanExpr`] payload) into mcx,
/// re-erasing to `'static` to match the lifetime-free Expr tree.
fn clone_subplan_static<'b>(
    sp: &SubPlan<'_>,
    mcx: Mcx<'b>,
) -> PgResult<Box<SubPlan<'b>>> {
    let owned: SubPlan<'b> = sp.clone_in(mcx)?;
    Ok(Box::new(owned))
}

/// Owned-tree form of `SubPlan` for embedding directly in the [`Expr`] enum.
///
/// The canonical [`SubPlan`] struct carries an `'mcx` lifetime (its `testexpr`
/// / `args` are `PgBox`/`PgVec` allocated in the plan context). Because the
/// `Expr` enum is lifetime-free, the inline `Expr::SubPlan` variant carries the
/// SubPlan as a global-allocator `Box`, matching the rest of the tree; the
/// `'static` here is the read-only plan tree's notional lifetime (the executor
/// never mutates it during evaluation).
#[derive(Debug)]
pub struct SubPlanExpr<'mcx>(pub Box<SubPlan<'mcx>>);

impl<'mcx> SubPlanExpr<'mcx> {
    /// Build a `SubPlanExpr` from a live `SubPlan<'b>` by deep-cloning it into
    /// `mcx`. Used to wrap a plan-tree `SubPlan` as an `Expr::SubPlan` ancestor
    /// node for ruleutils' deparse-namespace.
    pub fn from_subplan<'b>(mcx: Mcx<'b>, sp: &SubPlan<'_>) -> PgResult<SubPlanExpr<'b>> {
        let owned: SubPlan<'b> = sp.clone_in(mcx)?;
        Ok(SubPlanExpr(Box::new(owned)))
    }
}

impl<'mcx> Clone for SubPlanExpr<'mcx> {
    fn clone(&self) -> Self {
        panic!(
            "SubPlanExpr::clone: SubPlan carries context-allocated children; \
             clone the plan tree through SubPlan::clone_in"
        )
    }
}

/// Owned-tree form of [`AlternativeSubPlan`] for the lifetime-free [`Expr`]
/// enum (transient planner node; never present at execution).
#[derive(Debug)]
pub struct AlternativeSubPlanExpr<'mcx>(pub Box<AlternativeSubPlan<'mcx>>);

impl<'mcx> Clone for AlternativeSubPlanExpr<'mcx> {
    fn clone(&self) -> Self {
        panic!(
            "AlternativeSubPlanExpr::clone: transient planner node, never \
             present at execution"
        )
    }
}

/// `TargetEntry` (nodes/primnodes.h), trimmed.
#[derive(Debug, Default)]
pub struct TargetEntry<'mcx> {
    /// `Expr *expr` — expression to evaluate.
    pub expr: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `AttrNumber resno` — attribute number (the result attribute's position
    /// in the result tuple). Consumed by the junk filter's clean-map.
    pub resno: AttrNumber,
    /// `char *resname` — name of the column (could be NULL).
    pub resname: Option<PgString<'mcx>>,
    /// `Index ressortgroupref` — nonzero if referenced by a sort/group clause
    /// (the sort/group operation's `tleSortGroupRef`); 0 if not. Read/written by
    /// tlist.c (`get_sortgroupref_tle`, `apply_tlist_labeling`,
    /// `apply_pathtarget_labeling_to_tlist`, `make_tlist_from_pathtarget`,
    /// `make_pathtarget_from_tlist`). Added field-for-field vs primnodes.h.
    pub ressortgroupref: Index,
    /// `Oid resorigtbl` — OID of column's source table, or 0. Copied by
    /// `apply_tlist_labeling` (tlist.c). Added field-for-field vs primnodes.h.
    pub resorigtbl: Oid,
    /// `AttrNumber resorigcol` — column's number in source table, or 0. Copied
    /// by `apply_tlist_labeling` (tlist.c). Added field-for-field.
    pub resorigcol: AttrNumber,
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
                // Deep copy via `Expr::clone_in` (a shallow `.clone()` panics on
                // `Aggref`/`SubLink`/`SubPlan` children, which a TargetEntry's
                // expr can be — e.g. an aggregate's argument tlist).
                Some(e) => Some(alloc_in(mcx, e.clone_in(mcx)?)?),
                None => None,
            },
            resno: self.resno,
            resname: match &self.resname {
                Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
                None => None,
            },
            ressortgroupref: self.ressortgroupref,
            resorigtbl: self.resorigtbl,
            resorigcol: self.resorigcol,
            resjunk: self.resjunk,
        })
    }
}

#[cfg(test)]
mod clone_in_tests {
    use super::*;
    use mcx::MemoryContext;

    /// Build a trivial `Expr::Var` for use as a leaf child.
    fn a_var<'mcx>(varattno: AttrNumber) -> Expr<'mcx> {
        Expr::Var(Var {
            varno: 1,
            varattno,
            vartype: 23,
            vartypmod: -1,
            varnosyn: 1,
            varattnosyn: varattno,
            ..Default::default()
        })
    }

    /// `expr_tag()` returns the variant's `NodeTag`, and `etag::T_*` consts
    /// equal those tags — proving the Expr-side tag dispatch surface
    /// (`match e.expr_tag() { etag::T_Var => e.as_var(), .. }`) compiles and is
    /// correct. Also confirms the dual-homed twins (DistinctExpr/NullIfExpr,
    /// struct-equal to OpExpr) carry their OWN distinct tags.
    #[test]
    fn expr_tag_surface_dispatches() {
        let v = a_var(1);
        assert_eq!(v.expr_tag(), etag::T_Var);
        // The canonical tag-keyed dispatch shape the migration targets.
        let got = match v.expr_tag() {
            etag::T_Var => v.as_var().map(|x| x.varattno),
            etag::T_Const => None,
            _ => None,
        };
        assert_eq!(got, Some(1));

        // Distinct tags for the OpExpr-payload twins.
        assert_ne!(etag::T_DistinctExpr, etag::T_NullIfExpr);
        assert_ne!(etag::T_OpExpr, etag::T_DistinctExpr);
        // etag re-export equals the ntag value (same numeric tag).
        assert_eq!(etag::T_Var, crate::nodes::ntag::T_Var);
    }

    /// Round-trip `Expr::clone_in` on a list of `TargetEntry`s whose `expr` is an
    /// `Aggref` — exercises the panicking-`Clone` `Aggref` deep-copy path (its
    /// `args` is itself a `TargetEntry` list). Asserts structural equality
    /// field-by-field (the `equal()` engine lives downstream of this crate, so a
    /// direct call would form a dependency cycle).
    #[test]
    fn clone_in_aggref_bearing_target_entry_list() {
        // Leak the context so its `Mcx` is genuinely `'static`, matching the
        // arena `'static`-intern convention the test exercises (invariant `Expr`
        // makes a borrowed-`ctx` `'static` annotation unsound otherwise).
        let ctx: &'static MemoryContext = alloc::boxed::Box::leak(alloc::boxed::Box::new(
            MemoryContext::new("clone_in_test"),
        ));
        let mcx = ctx.mcx();

        // Aggref with one aggregated arg (a TargetEntry wrapping a Var) and a
        // FILTER expression.
        let inner_te = TargetEntry {
            expr: Some(mcx::alloc_in(mcx, a_var(2)).unwrap()),
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        // Erase the inner TargetEntry to 'static (Aggref::args convention).
        let inner_te_static: TargetEntry<'static> =
            unsafe { core::mem::transmute(inner_te) };

        let aggref = Aggref {
            aggfnoid: 2147,
            aggtype: 20,
            aggcollid: 0,
            inputcollid: 0,
            aggtranstype: 20,
            aggargtypes: alloc::vec![23],
            aggdirectargs: Vec::new(),
            args: alloc::vec![inner_te_static],
            aggorder: Vec::new(),
            aggdistinct: Vec::new(),
            aggfilter: Some(Box::new(a_var(3))),
            aggstar: false,
            aggvariadic: false,
            aggkind: b'n' as i8,
            aggpresorted: false,
            agglevelsup: 0,
            aggsplit: crate::nodeagg::AggSplit::default(),
            aggno: 0,
            aggtransno: 0,
            location: 42,
        };

        let tlist = alloc::vec![TargetEntry {
            expr: Some(mcx::alloc_in(mcx, Expr::Aggref(aggref)).unwrap()),
            resno: 5,
            resname: None,
            ressortgroupref: 7,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        }];

        // Deep-copy every TargetEntry (whose expr is an Aggref). A shallow
        // `.clone()` would panic here.
        let cloned: Vec<TargetEntry<'_>> =
            tlist.iter().map(|te| te.clone_in(mcx).unwrap()).collect();

        assert_eq!(cloned.len(), 1);
        let ce = &cloned[0];
        assert_eq!(ce.resno, 5);
        assert_eq!(ce.ressortgroupref, 7);
        let agg = ce.expr.as_ref().unwrap().as_aggref().expect("Aggref");
        assert_eq!(agg.aggfnoid, 2147);
        assert_eq!(agg.aggtype, 20);
        assert_eq!(agg.location, 42);
        assert_eq!(agg.aggargtypes, alloc::vec![23]);
        // The aggregated-arg TargetEntry list was deep-copied.
        assert_eq!(agg.args.len(), 1);
        let arg_var = agg.args[0]
            .expr
            .as_ref()
            .unwrap()
            .as_var()
            .expect("Var arg");
        assert_eq!(arg_var.varattno, 2);
        // The FILTER expression was deep-copied.
        let filter_var = agg
            .aggfilter
            .as_ref()
            .unwrap()
            .as_var()
            .expect("Var filter");
        assert_eq!(filter_var.varattno, 3);
    }

    /// Round-trip `Expr::clone_in` on a `SubLink`-bearing expression — exercises
    /// the panicking-`Clone` `SubLink` deep-copy path (its `subselect` is an
    /// embedded owned `Query`). Asserts the structure survives the deep copy.
    #[test]
    fn clone_in_sublink_bearing_expr() {
        let ctx = MemoryContext::new("clone_in_test");
        let mcx = ctx.mcx();

        // An analyzed sub-Query (minimal: default fields). Erase to 'static to
        // match SubLink.subselect's notional lifetime.
        let q = crate::copy_query::Query::new(mcx);
        let q_boxed = mcx::alloc_in(mcx, q).unwrap();
        let q_static: PgBox<'static, crate::copy_query::Query<'static>> =
            unsafe { core::mem::transmute(q_boxed) };

        let sublink = SubLink {
            subLinkType: SubLinkType::Any,
            subLinkId: 0,
            testexpr: Some(Box::new(a_var(4))),
            operName: alloc::vec![String::from("=")],
            subselect: Some(q_static),
            location: 99,
        };
        // Wrap the SubLink in a BoolExpr to exercise recursion through a parent
        // node into the panicking-Clone child.
        let expr = Expr::BoolExpr(BoolExpr {
            boolop: BoolExprType::AND_EXPR,
            args: alloc::vec![Expr::SubLink(sublink), a_var(5)],
            location: -1,
        });

        // A shallow `.clone()` would panic on the SubLink child; clone_in deep-
        // copies it.
        let cloned = expr.clone_in(mcx).unwrap();

        let be = cloned.as_boolexpr().expect("BoolExpr");
        assert_eq!(be.args.len(), 2);
        let sl = be.args[0].as_sublink().expect("SubLink");
        assert_eq!(sl.subLinkType, SubLinkType::Any);
        assert_eq!(sl.location, 99);
        // testexpr deep-copied.
        assert_eq!(
            sl.testexpr.as_ref().unwrap().as_var().unwrap().varattno,
            4
        );
        // subselect deep-copied (present, owned).
        assert!(sl.subselect.is_some());
        // Sibling Var deep-copied.
        assert_eq!(be.args[1].as_var().unwrap().varattno, 5);
    }

    /// Round-trip a `SubLink`-bearing `Expr` wrapped as a `Node::Expr` through
    /// [`crate::nodes::Node::clone_in`] — the actual root-cause path for the
    /// reachable `SELECT 1 WHERE EXISTS (...)` blocker: a WHERE-clause `SubLink`
    /// is stored as `FromExpr.quals` (a `Node`) and deep-cloned by
    /// `Query::clone_in` -> `FromExpr::clone_in` -> `Node::clone_in`, which must
    /// route the `Expr` arm through `Expr::clone_in` (not the panicking
    /// derived `.clone()`).
    #[test]
    fn node_clone_in_routes_sublink_expr_through_clone_in() {
        use crate::nodes::Node;
        // Leak the context so its `Mcx` is genuinely `'static` (see sibling test).
        let ctx: &'static MemoryContext = alloc::boxed::Box::leak(alloc::boxed::Box::new(
            MemoryContext::new("clone_in_test"),
        ));
        let mcx = ctx.mcx();

        let q = crate::copy_query::Query::new(mcx);
        let q_boxed = mcx::alloc_in(mcx, q).unwrap();
        let q_static: PgBox<'static, crate::copy_query::Query<'static>> =
            unsafe { core::mem::transmute(q_boxed) };

        let node = Node::mk_expr(
            mcx,
            Expr::SubLink(SubLink {
                subLinkType: SubLinkType::Exists,
                subLinkId: 0,
                testexpr: None,
                operName: alloc::vec::Vec::new(),
                subselect: Some(q_static),
                location: 7,
            }),
        )
        .unwrap();

        // Would panic on the SubLink child if the Node::Expr arm used a plain
        // `.clone()`; routing through Expr::clone_in deep-copies it.
        let cloned = node.clone_in(mcx).unwrap();
        let sl = cloned.as_expr().unwrap().as_sublink().expect("SubLink");
        assert_eq!(sl.subLinkType, SubLinkType::Exists);
        assert_eq!(sl.location, 7);
        assert!(sl.subselect.is_some());
    }
}

/// Miri regression for the arena use-after-free class (the P0 of the
/// `expr-mcx` lifetime campaign).
///
/// The campaign interns planner nodes into a long-lived arena by
/// `Expr::erase_lifetime` (`PlannerInfo::alloc_node`:
/// `node_arena.push(ArenaNode::Expr(node.erase_lifetime()))`). `erase_lifetime`
/// only relabels the lifetime parameter `'mcx -> 'static`; it does **not**
/// deep-copy the node's backing allocations. So if any payload of the interned
/// node (here a by-reference `Datum::ByRef(PgVec<'mcx, u8>)`, whose buffer lives
/// in a *transient* `MemoryContext`) outlives that context, reading it back is a
/// use-after-free.
///
/// Two production SIGSEGVs came from exactly this: interning a node/Datum whose
/// backing lived in a transient parse/relation context that then reset. In a
/// release build that surfaces as a flaky segfault; under Miri it is a
/// deterministic UB diagnostic.
///
/// These tests model the bug mechanism in pure Rust (no FFI): a durable
/// `Vec<Expr<'static>>` standing in for `PlannerInfo::node_arena`, a transient
/// **bump** `MemoryContext` standing in for the parse/relation context, and the
/// real `Const`/`Datum::ByRef`/`Expr::erase_lifetime` types. The bump backend is
/// load-bearing: it frees its whole arena on context drop (the C
/// "freed-with-its-context" model), so the interned `PgVec`'s buffer pointer
/// genuinely dangles — a malloc backend frees each box individually and would
/// not reproduce the wholesale-reset shape.
///
/// Run with:
///   MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" \
///     cargo +nightly miri test -p types-nodes arena_uaf
#[cfg(test)]
mod arena_uaf_tests {
    use super::*;
    use mcx::MemoryContext;
    use types_tuple::heaptuple::Datum;

    /// Build a `Const` carrying a by-reference `Datum` whose 8-byte image is
    /// allocated in `mcx` (a `PgVec<'mcx, u8>` inside that context's arena).
    fn byref_const<'mcx>(mcx: Mcx<'mcx>) -> Const<'mcx> {
        let datum = Datum::from_byref_bytes_in(mcx, &[1u8, 2, 3, 4, 5, 6, 7, 8])
            .expect("byref datum builds");
        Const {
            consttype: 17, // bytea, pass-by-reference
            constlen: -1,
            constvalue: datum,
            constisnull: false,
            constbyval: false,
            ..Default::default()
        }
    }

    /// THE BUG. Interning a node whose by-ref payload lives in a transient
    /// context, then dropping that context, then reading the interned payload,
    /// is a use-after-free. Under Miri this aborts with a "pointer to freed
    /// allocation" / dangling-deref diagnostic — deterministically, every run.
    ///
    /// `#[cfg(miri)]`-only: under a normal `cargo test` this would dereference
    /// freed memory and either read garbage or segfault flakily, so it must not
    /// run there. Its whole purpose is to be the Miri tripwire for this class.
    ///
    /// `#[ignore]`d so a plain `cargo miri test` (the green gate) skips it — it
    /// *intentionally* triggers UB, which aborts the whole test binary, so it
    /// cannot share a run with the passing tests. Invoke it as an explicit
    /// negative check that MUST fail (CI asserts non-zero exit):
    ///   MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" cargo +nightly \
    ///     miri test -p types-nodes -- --ignored interning_byref_from_transient
    /// Expected: "Undefined Behavior: pointer not dereferenceable: ... has been
    /// freed, so this pointer is dangling" at the `as_ref_bytes()` deref.
    #[cfg(miri)]
    #[ignore = "intentionally triggers UB to prove Miri catches the arena UAF class; run with --ignored and assert it FAILS"]
    #[test]
    fn interning_byref_from_transient_context_is_uaf() {
        // The durable arena (stands in for `PlannerInfo::node_arena`).
        let mut node_arena: alloc::vec::Vec<Expr<'static>> = alloc::vec::Vec::new();

        // A transient bump context (stands in for a parse/relation context).
        let transient = MemoryContext::new_bump("transient-relcx");

        // Build a by-ref Const in the transient context and intern it into the
        // durable arena via erase_lifetime — exactly what alloc_node does.
        let c = byref_const(transient.mcx());
        node_arena.push(Expr::Const(c).erase_lifetime());

        // The transient context resets/frees its whole arena.
        drop(transient);

        // Read the interned by-ref payload back. Its PgVec buffer pointed into
        // the now-freed bump arena: this deref is the use-after-free. Miri must
        // flag it here.
        let interned = &node_arena[0];
        let bytes = match interned {
            Expr::Const(c) => c.constvalue.as_ref_bytes(),
            _ => unreachable!(),
        };
        // Touch the bytes so the read is not elided.
        let _sum: u32 = bytes.iter().map(|&b| b as u32).sum();
    }

    /// THE FIX. Clone the by-ref payload into the *durable* context BEFORE
    /// interning (C: `copyObject`/`datumCopy` into the long-lived context). Now
    /// the interned node's backing lives as long as the arena, so dropping the
    /// transient context is harmless and the read is sound. Miri passes this.
    ///
    /// Runs under both normal `cargo test` and Miri (it is genuinely sound), so
    /// it documents the correct pattern and is exercised by the normal gate too.
    #[test]
    fn cloning_into_durable_context_before_interning_is_sound() {
        // A durable context that outlives the arena's use, leaked to make its
        // Mcx genuinely 'static (matching the arena-intern convention; cf.
        // clone_in_tests). Leaked memory is expected — run Miri with
        // -Zmiri-ignore-leaks.
        let durable: &'static MemoryContext = alloc::boxed::Box::leak(alloc::boxed::Box::new(
            MemoryContext::new("durable-planner"),
        ));
        let durable_mcx = durable.mcx();

        let mut node_arena: alloc::vec::Vec<Expr<'static>> = alloc::vec::Vec::new();

        {
            let transient = MemoryContext::new_bump("transient-relcx");
            let c = byref_const(transient.mcx());

            // Deep-copy the node into the durable context FIRST, then intern.
            // Expr::clone_in re-homes Datum::ByRef into durable_mcx (its Const
            // arm calls constvalue.clone_in).
            let durable_expr = Expr::Const(c).clone_in(durable_mcx).expect("clone_in");
            node_arena.push(durable_expr.erase_lifetime());

            drop(transient); // harmless now: the interned bytes live in `durable`
        }

        let interned = &node_arena[0];
        let bytes = match interned {
            Expr::Const(c) => c.constvalue.as_ref_bytes(),
            _ => unreachable!(),
        };
        assert_eq!(bytes, &[1u8, 2, 3, 4, 5, 6, 7, 8]);
    }
}
