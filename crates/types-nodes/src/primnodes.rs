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
//! during evaluation. Child expressions are owned `Box<Expr>` and child lists
//! are `Vec<…>` on the global allocator — matching the precedent already set
//! by `OpExpr`/`ScalarArrayOpExpr` (which carry `Vec<Expr>`) and keeping `Expr`
//! free of a lifetime parameter so the (non-exhaustive) enum stays additive:
//! existing consumers' wildcard arms keep compiling.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid, ParseLoc};
use types_tuple::backend_access_common_heaptuple::Datum;
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

/// `Relids` (nodes/bitmapset.h: `Bitmapset *`) for the lifetime-free expression
/// tree — a planner relation-id set carried by a [`Var`]/[`PlaceHolderVar`].
///
/// The canonical [`crate::bitmapset::Bitmapset`] is `'mcx`-lifetimed and not
/// `Clone`, so it cannot be embedded in the lifetime-free, `Clone`+`Default`
/// [`Expr`] tree without forcing an `'mcx` flag-day across every `Expr` consumer.
/// This is the lifetime-free planner analogue (same `Vec<u64>`-word storage as
/// `types_pathnodes::Bitmapset`, the planner-arena relids type): the empty set is
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
pub struct Const {
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
    /// `Datum constvalue` — the constant's value (undefined if `constisnull`).
    ///
    /// A `Const` lives in its plan node's long-lived context (it is not
    /// per-tuple working state), so its value carries the `'static` lifetime —
    /// matching the `Box<SubPlan<'static>>` convention used elsewhere in the
    /// lifetime-free [`Expr`] enum.
    pub constvalue: Datum<'static>,
    /// `bool constisnull` — whether the constant is null.
    pub constisnull: bool,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

impl Default for Const {
    fn default() -> Self {
        Const {
            consttype: Default::default(),
            consttypmod: 0,
            constcollid: Default::default(),
            constvalue: Datum::null(),
            constisnull: false,
            location: -1,
        }
    }
}

/// `OpExpr` (nodes/primnodes.h) — expression node for an operator invocation.
#[derive(Clone, Debug, Default)]
pub struct OpExpr {
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
    pub args: Vec<Expr>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

/// `ScalarArrayOpExpr` (nodes/primnodes.h) — `scalar op ANY/ALL (array)`,
/// trimmed to the fields ports consume (the TID-scan node reads only `args`,
/// via `linitial`/`lsecond`).
#[derive(Clone, Debug, Default)]
pub struct ScalarArrayOpExpr {
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
    pub args: Vec<Expr>,
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
pub struct BoolExpr {
    /// `BoolExprType boolop`.
    pub boolop: BoolExprType,
    /// `List *args` — arguments (exactly one for NOT, two-or-more for AND/OR).
    pub args: Vec<Expr>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CompareType` (nodes/cmptype.h) — abstract comparison kind requested of a
/// [`RowCompareExpr`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CompareType {
    /// `COMPARE_INVALID`.
    COMPARE_INVALID = 0,
    /// `COMPARE_LT`.
    COMPARE_LT = 1,
    /// `COMPARE_LE`.
    COMPARE_LE = 2,
    /// `COMPARE_EQ`.
    COMPARE_EQ = 3,
    /// `COMPARE_GE`.
    COMPARE_GE = 4,
    /// `COMPARE_GT`.
    COMPARE_GT = 5,
    /// `COMPARE_NE`.
    COMPARE_NE = 6,
}

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum JsonConstructorType {
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
pub struct Aggref {
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
    pub aggdirectargs: Vec<Expr>,
    /// `List *args` — aggregated args + sort exprs (list of TargetEntry).
    pub args: Vec<TargetEntry<'static>>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<Box<Expr>>,
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

impl Clone for Aggref {
    fn clone(&self) -> Self {
        panic!(
            "Aggref::clone: aggregate args are a TargetEntry list with \
             context-allocated children; deep-copy via TargetEntry::clone_in"
        )
    }
}

/// `GroupingFunc` (nodes/primnodes.h) — a `GROUPING(...)` expression.
#[derive(Clone, Debug)]
pub struct GroupingFunc {
    /// `List *args` — kept for EXPLAIN; not evaluated.
    pub args: Vec<Expr>,
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
pub struct WindowFunc {
    /// `Oid winfnoid`.
    pub winfnoid: Oid,
    /// `Oid wintype`.
    pub wintype: Oid,
    /// `Oid wincollid`.
    pub wincollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `List *args`.
    pub args: Vec<Expr>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<Box<Expr>>,
    /// `List *runCondition` — WindowFuncRunConditions to short-circuit.
    pub runCondition: Vec<Expr>,
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
pub struct SubscriptingRef {
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
    pub refupperindexpr: Vec<Option<Expr>>,
    /// `List *reflowerindexpr` — lower container index exprs, or empty for a
    /// single element (may contain NULL elements).
    pub reflowerindexpr: Vec<Option<Expr>>,
    /// `Expr *refexpr` — expression yielding the container value.
    pub refexpr: Option<Box<Expr>>,
    /// `Expr *refassgnexpr` — source value for a store, or `None` for a fetch.
    pub refassgnexpr: Option<Box<Expr>>,
}

/// `FuncExpr` (nodes/primnodes.h) — a function call.
#[derive(Clone, Debug)]
pub struct FuncExpr {
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
    pub args: Vec<Expr>,
    /// `ParseLoc location` — token location, or -1 if unknown. Set by the
    /// parser; read by `exprLocation` (nodeFuncs.c). Added field-for-field vs
    /// primnodes.h.
    pub location: ParseLoc,
}

/// `NamedArgExpr` (nodes/primnodes.h) — a named function argument. The planner
/// removes these before execution.
#[derive(Clone, Debug)]
pub struct NamedArgExpr {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
    /// `char *name`.
    pub name: Option<String>,
    /// `int argnumber`.
    pub argnumber: i32,
    /// `ParseLoc location` — argument name location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `SubLink` (nodes/primnodes.h) — a subselect in an expression. The planner
/// replaces these with [`SubPlan`] nodes; never executed directly.
#[derive(Clone, Debug)]
pub struct SubLink {
    /// `SubLinkType subLinkType`.
    pub subLinkType: SubLinkType,
    /// `int subLinkId`.
    pub subLinkId: i32,
    /// `Node *testexpr`.
    pub testexpr: Option<Box<Expr>>,
    /// `Node *subselect` — Query* or raw parsetree. Opaque to the executor (a
    /// SubLink is always replaced by a [`SubPlan`] before execution), carried
    /// as the node address (mirrors the opaque-address precedent in
    /// `execexpr.rs`, e.g. `ExprState::ext_params`). 0 means NULL.
    pub subselect: usize,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
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
pub struct FieldSelect {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
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
pub struct FieldStore {
    /// `Expr *arg` — input tuple value.
    pub arg: Option<Box<Expr>>,
    /// `List *newvals` — new value(s) for field(s).
    pub newvals: Vec<Expr>,
    /// `List *fieldnums` — integer list of field attnums.
    pub fieldnums: Vec<AttrNumber>,
    /// `Oid resulttype`.
    pub resulttype: Oid,
}

/// `RelabelType` (nodes/primnodes.h) — a no-op binary-compatible coercion.
#[derive(Clone, Debug)]
pub struct RelabelType {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
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
pub struct CoerceViaIO {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
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
pub struct ArrayCoerceExpr {
    /// `Expr *arg` — input expression (yields an array).
    pub arg: Option<Box<Expr>>,
    /// `Expr *elemexpr` — per-element coercion work.
    pub elemexpr: Option<Box<Expr>>,
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
pub struct ConvertRowtypeExpr {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
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
pub struct CollateExpr {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
    /// `Oid collOid`.
    pub collOid: Oid,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CaseExpr` (nodes/primnodes.h) — a CASE expression.
#[derive(Clone, Debug)]
pub struct CaseExpr {
    /// `Oid casetype`.
    pub casetype: Oid,
    /// `Oid casecollid`.
    pub casecollid: Oid,
    /// `Expr *arg` — implicit equality comparison argument (form 2), or `None`.
    pub arg: Option<Box<Expr>>,
    /// `List *args` — the WHEN clauses (list of [`CaseWhen`]).
    pub args: Vec<CaseWhen>,
    /// `Expr *defresult` — the ELSE result.
    pub defresult: Option<Box<Expr>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CaseWhen` (nodes/primnodes.h) — one arm of a CASE expression. (Not itself
/// `Expr`-derived in the dispatch sense, but carried inline in [`CaseExpr`].)
#[derive(Clone, Debug)]
pub struct CaseWhen {
    /// `Expr *expr` — condition expression.
    pub expr: Option<Box<Expr>>,
    /// `Expr *result` — substitution result.
    pub result: Option<Box<Expr>>,
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
pub struct ArrayExpr {
    /// `Oid array_typeid`.
    pub array_typeid: Oid,
    /// `Oid array_collid`.
    pub array_collid: Oid,
    /// `Oid element_typeid`.
    pub element_typeid: Oid,
    /// `List *elements` — the array elements or sub-arrays.
    pub elements: Vec<Expr>,
    /// `bool multidims` — true if elements are sub-arrays.
    pub multidims: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `RowExpr` (nodes/primnodes.h) — a `ROW()` expression.
#[derive(Clone, Debug)]
pub struct RowExpr {
    /// `List *args` — the fields.
    pub args: Vec<Expr>,
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
pub struct RowCompareExpr {
    /// `CompareType cmptype` — LT/LE/GE/GT (never EQ/NE).
    pub cmptype: CompareType,
    /// `List *opnos` — OID list of pairwise comparison ops.
    pub opnos: Vec<Oid>,
    /// `List *opfamilies` — OID list of containing operator families.
    pub opfamilies: Vec<Oid>,
    /// `List *inputcollids` — OID list of comparison collations.
    pub inputcollids: Vec<Oid>,
    /// `List *largs` — left-hand input arguments.
    pub largs: Vec<Expr>,
    /// `List *rargs` — right-hand input arguments.
    pub rargs: Vec<Expr>,
}

/// `CoalesceExpr` (nodes/primnodes.h) — a COALESCE expression.
#[derive(Clone, Debug)]
pub struct CoalesceExpr {
    /// `Oid coalescetype`.
    pub coalescetype: Oid,
    /// `Oid coalescecollid`.
    pub coalescecollid: Oid,
    /// `List *args`.
    pub args: Vec<Expr>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `MinMaxExpr` (nodes/primnodes.h) — a GREATEST or LEAST function.
#[derive(Clone, Debug)]
pub struct MinMaxExpr {
    /// `Oid minmaxtype`.
    pub minmaxtype: Oid,
    /// `Oid minmaxcollid`.
    pub minmaxcollid: Oid,
    /// `Oid inputcollid`.
    pub inputcollid: Oid,
    /// `MinMaxOp op`.
    pub op: MinMaxOp,
    /// `List *args`.
    pub args: Vec<Expr>,
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
pub struct XmlExpr {
    /// `XmlExprOp op`.
    pub op: XmlExprOp,
    /// `char *name`.
    pub name: Option<String>,
    /// `List *named_args` — non-XML expressions for xml_attributes.
    pub named_args: Vec<Expr>,
    /// `List *arg_names` — parallel list of String values.
    pub arg_names: Vec<String>,
    /// `List *args`.
    pub args: Vec<Expr>,
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
pub struct JsonValueExpr {
    /// `Expr *raw_expr` — user-specified expression.
    pub raw_expr: Option<Box<Expr>>,
    /// `Expr *formatted_expr` — coerced formatted expression.
    pub formatted_expr: Option<Box<Expr>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
}

/// `JsonConstructorExpr` (nodes/primnodes.h) — wrapper over FuncExpr/Aggref/
/// WindowFunc for SQL/JSON constructors.
#[derive(Clone, Debug)]
pub struct JsonConstructorExpr {
    /// `JsonConstructorType type`.
    pub r#type: JsonConstructorType,
    /// `List *args`.
    pub args: Vec<Expr>,
    /// `Expr *func` — underlying json[b]_xxx() function call.
    pub func: Option<Box<Expr>>,
    /// `Expr *coercion` — coercion to RETURNING type.
    pub coercion: Option<Box<Expr>>,
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
pub struct JsonIsPredicate {
    /// `Node *expr` — subject expression.
    pub expr: Option<Box<Expr>>,
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
pub struct JsonBehavior {
    /// `JsonBehaviorType btype`.
    pub btype: JsonBehaviorType,
    /// `Node *expr`.
    pub expr: Option<Box<Expr>>,
    /// `bool coerce`.
    pub coerce: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `JsonExpr` (nodes/primnodes.h) — transformed JSON_VALUE/JSON_QUERY/
/// JSON_EXISTS.
#[derive(Clone, Debug)]
pub struct JsonExpr {
    /// `JsonExprOp op`.
    pub op: JsonExprOp,
    /// `char *column_name` — JSON_TABLE() column name, or `None`.
    pub column_name: Option<String>,
    /// `Node *formatted_expr` — jsonb-valued expression to query.
    pub formatted_expr: Option<Box<Expr>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `Node *path_spec` — jsonpath-valued query pattern.
    pub path_spec: Option<Box<Expr>>,
    /// `JsonReturning *returning` — expected output type/format.
    pub returning: Option<JsonReturning>,
    /// `List *passing_names` — PASSING argument names (list of String).
    pub passing_names: Vec<String>,
    /// `List *passing_values` — PASSING argument value expressions.
    pub passing_values: Vec<Expr>,
    /// `JsonBehavior *on_empty`.
    pub on_empty: Option<Box<JsonBehavior>>,
    /// `JsonBehavior *on_error`.
    pub on_error: Option<Box<JsonBehavior>>,
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
pub struct NullTest {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
    /// `NullTestType nulltesttype`.
    pub nulltesttype: NullTestType,
    /// `bool argisrow` — true to perform field-by-field null checks.
    pub argisrow: bool,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `BooleanTest` (nodes/primnodes.h) — IS [NOT] TRUE/FALSE/UNKNOWN.
#[derive(Clone, Debug)]
pub struct BooleanTest {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
    /// `BoolTestType booltesttype`.
    pub booltesttype: BoolTestType,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: ParseLoc,
}

/// `CoerceToDomain` (nodes/primnodes.h) — coerce a value to a domain type.
#[derive(Clone, Debug)]
pub struct CoerceToDomain {
    /// `Expr *arg`.
    pub arg: Option<Box<Expr>>,
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
pub struct InferenceElem {
    /// `Node *expr`.
    pub expr: Option<Box<Expr>>,
    /// `Oid infercollid`.
    pub infercollid: Oid,
    /// `Oid inferopclass`.
    pub inferopclass: Oid,
}

/// `ReturningExpr` (nodes/primnodes.h) — return OLD/NEW.(expression) in a
/// RETURNING list. Inserted by the rewriter/planner only.
#[derive(Clone, Debug)]
pub struct ReturningExpr {
    /// `int retlevelsup`.
    pub retlevelsup: i32,
    /// `bool retold` — true for OLD, false for NEW.
    pub retold: bool,
    /// `Expr *retexpr`.
    pub retexpr: Option<Box<Expr>>,
}

/// `PlaceHolderVar` (nodes/pathnodes.h) — a placeholder for a subexpression that
/// must be evaluated below an outer join and then forced to null above it. The
/// optimizer dispatches `IsA(node, PlaceHolderVar)` and reads
/// `phid`/`phrels`/`phnullingrels`/`phexpr`.
///
/// `phexpr` is an owned `Box<Expr>` (matching the rest of the lifetime-free
/// tree); `phrels`/`phnullingrels` are the lifetime-free [`ExprRelids`].
#[derive(Clone, Debug, Default)]
pub struct PlaceHolderVar {
    /// `Expr *phexpr` — the represented expression. `None` only during
    /// construction; a built PHV always wraps an expression.
    pub phexpr: Option<Box<Expr>>,
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
/// `Box<Expr>` and lists are `Vec<…>`, matching `OpExpr`/`ScalarArrayOpExpr`
/// and keeping the non-exhaustive enum additive for existing consumers.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Expr {
    /// `T_Var`.
    Var(Var),
    /// `T_Const`.
    Const(Const),
    /// `T_Param`.
    Param(Param),
    /// `T_Aggref`.
    Aggref(Aggref),
    /// `T_GroupingFunc`.
    GroupingFunc(GroupingFunc),
    /// `T_WindowFunc`.
    WindowFunc(WindowFunc),
    /// `T_SubscriptingRef`.
    SubscriptingRef(SubscriptingRef),
    /// `T_FuncExpr`.
    FuncExpr(FuncExpr),
    /// `T_NamedArgExpr`.
    NamedArgExpr(NamedArgExpr),
    /// `T_OpExpr`.
    OpExpr(OpExpr),
    /// `T_DistinctExpr` — same payload as [`OpExpr`] (C: `typedef OpExpr`).
    DistinctExpr(OpExpr),
    /// `T_NullIfExpr` — same payload as [`OpExpr`] (C: `typedef OpExpr`).
    NullIfExpr(OpExpr),
    /// `T_ScalarArrayOpExpr`.
    ScalarArrayOpExpr(ScalarArrayOpExpr),
    /// `T_BoolExpr`.
    BoolExpr(BoolExpr),
    /// `T_SubLink`.
    SubLink(SubLink),
    /// `T_SubPlan`.
    SubPlan(SubPlanExpr),
    /// `T_AlternativeSubPlan`.
    AlternativeSubPlan(AlternativeSubPlanExpr),
    /// `T_FieldSelect`.
    FieldSelect(FieldSelect),
    /// `T_FieldStore`.
    FieldStore(FieldStore),
    /// `T_RelabelType`.
    RelabelType(RelabelType),
    /// `T_CoerceViaIO`.
    CoerceViaIO(CoerceViaIO),
    /// `T_ArrayCoerceExpr`.
    ArrayCoerceExpr(ArrayCoerceExpr),
    /// `T_ConvertRowtypeExpr`.
    ConvertRowtypeExpr(ConvertRowtypeExpr),
    /// `T_CollateExpr`.
    CollateExpr(CollateExpr),
    /// `T_CaseExpr`.
    CaseExpr(CaseExpr),
    /// `T_CaseTestExpr`.
    CaseTestExpr(CaseTestExpr),
    /// `T_ArrayExpr`.
    ArrayExpr(ArrayExpr),
    /// `T_RowExpr`.
    RowExpr(RowExpr),
    /// `T_RowCompareExpr`.
    RowCompareExpr(RowCompareExpr),
    /// `T_CoalesceExpr`.
    CoalesceExpr(CoalesceExpr),
    /// `T_MinMaxExpr`.
    MinMaxExpr(MinMaxExpr),
    /// `T_SQLValueFunction`.
    SQLValueFunction(SQLValueFunction),
    /// `T_XmlExpr`.
    XmlExpr(XmlExpr),
    /// `T_JsonValueExpr`.
    JsonValueExpr(JsonValueExpr),
    /// `T_JsonConstructorExpr`.
    JsonConstructorExpr(JsonConstructorExpr),
    /// `T_JsonIsPredicate`.
    JsonIsPredicate(JsonIsPredicate),
    /// `T_JsonExpr`.
    JsonExpr(JsonExpr),
    /// `T_NullTest`.
    NullTest(NullTest),
    /// `T_BooleanTest`.
    BooleanTest(BooleanTest),
    /// `T_MergeSupportFunc`.
    MergeSupportFunc(MergeSupportFunc),
    /// `T_CoerceToDomain`.
    CoerceToDomain(CoerceToDomain),
    /// `T_CoerceToDomainValue`.
    CoerceToDomainValue(CoerceToDomainValue),
    /// `T_SetToDefault`.
    SetToDefault(SetToDefault),
    /// `T_CurrentOfExpr`.
    CurrentOfExpr(CurrentOfExpr),
    /// `T_NextValueExpr`.
    NextValueExpr(NextValueExpr),
    /// `T_InferenceElem`.
    InferenceElem(InferenceElem),
    /// `T_ReturningExpr`.
    ReturningExpr(ReturningExpr),
    /// `T_PlaceHolderVar` (nodes/pathnodes.h) — a planner placeholder node. Not
    /// in the contiguous executor `Expr` run, but it derives from `Expr` in C
    /// and the optimizer dispatches it via `IsA`/`match`.
    PlaceHolderVar(PlaceHolderVar),
    /// `T_RestrictInfo` (nodes/pathnodes.h) — a planner RestrictInfo embedded in
    /// an expression tree. C casts `RestrictInfo *` to `Expr *` to place it
    /// inside the `orclause` BoolExpr built by `make_sub_restrictinfos`; carried
    /// here as the [`RinfoRef`] arena handle.
    RestrictInfo(RinfoRef),
}

impl Expr {
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
    pub fn expect_opexpr(&self) -> Option<&OpExpr> {
        match self {
            Expr::OpExpr(o) => Some(o),
            _ => None,
        }
    }

    /// `castNode(PlaceHolderVar, node)` — borrow the [`PlaceHolderVar`] payload,
    /// or `None` if this is not an `Expr::PlaceHolderVar`.
    #[inline]
    pub fn expect_placeholdervar(&self) -> Option<&PlaceHolderVar> {
        match self {
            Expr::PlaceHolderVar(p) => Some(p),
            _ => None,
        }
    }

    /// Mutable variant of [`Expr::expect_placeholdervar`].
    #[inline]
    pub fn expect_placeholdervar_mut(&mut self) -> Option<&mut PlaceHolderVar> {
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

/// Generate `is_x`/`as_x`/`as_x_mut`/`expect_into_x` for a single-payload
/// `Expr` variant.
macro_rules! expr_accessors {
    ($variant:ident, $ty:ty, $is:ident, $as:ident, $as_mut:ident, $into:ident) => {
        impl Expr {
            #[doc = concat!("`IsA(node, ", stringify!($variant), ")`.")]
            #[inline]
            pub fn $is(&self) -> bool {
                matches!(self, Expr::$variant(_))
            }
            #[doc = concat!("`castNode(", stringify!($variant), ", node)` — borrow the payload, or `None`.")]
            #[inline]
            pub fn $as(&self) -> Option<&$ty> {
                match self {
                    Expr::$variant(x) => Some(x),
                    _ => None,
                }
            }
            #[doc = concat!("Mutable borrow of the [`", stringify!($variant), "`] payload, or `None`.")]
            #[inline]
            pub fn $as_mut(&mut self) -> Option<&mut $ty> {
                match self {
                    Expr::$variant(x) => Some(x),
                    _ => None,
                }
            }
            #[doc = concat!("Consume the node and return its [`", stringify!($variant), "`] payload (C reuses the node's storage in place); panics on a wrong tag (a caller bug).")]
            #[inline]
            pub fn $into(self) -> $ty {
                match self {
                    Expr::$variant(x) => x,
                    _ => panic!(concat!("Expr::", stringify!($into), ": node is not a ", stringify!($variant))),
                }
            }
        }
    };
}

expr_accessors!(Const, Const, is_const, as_const, as_const_mut, expect_into_const);
expr_accessors!(Param, Param, is_param, as_param, as_param_mut, expect_into_param);
expr_accessors!(Aggref, Aggref, is_aggref, as_aggref, as_aggref_mut, expect_into_aggref);
expr_accessors!(GroupingFunc, GroupingFunc, is_groupingfunc, as_groupingfunc, as_groupingfunc_mut, expect_into_groupingfunc);
expr_accessors!(WindowFunc, WindowFunc, is_windowfunc, as_windowfunc, as_windowfunc_mut, expect_into_windowfunc);
expr_accessors!(SubscriptingRef, SubscriptingRef, is_subscriptingref, as_subscriptingref, as_subscriptingref_mut, expect_into_subscriptingref);
expr_accessors!(FuncExpr, FuncExpr, is_funcexpr, as_funcexpr, as_funcexpr_mut, expect_into_funcexpr);
expr_accessors!(NamedArgExpr, NamedArgExpr, is_namedargexpr, as_namedargexpr, as_namedargexpr_mut, expect_into_namedargexpr);
expr_accessors!(ScalarArrayOpExpr, ScalarArrayOpExpr, is_scalararrayopexpr, as_scalararrayopexpr, as_scalararrayopexpr_mut, expect_into_scalararrayopexpr);
expr_accessors!(BoolExpr, BoolExpr, is_boolexpr, as_boolexpr, as_boolexpr_mut, expect_into_boolexpr);
expr_accessors!(SubLink, SubLink, is_sublink, as_sublink, as_sublink_mut, expect_into_sublink);
expr_accessors!(FieldSelect, FieldSelect, is_fieldselect, as_fieldselect, as_fieldselect_mut, expect_into_fieldselect);
expr_accessors!(FieldStore, FieldStore, is_fieldstore, as_fieldstore, as_fieldstore_mut, expect_into_fieldstore);
expr_accessors!(RelabelType, RelabelType, is_relabeltype, as_relabeltype, as_relabeltype_mut, expect_into_relabeltype);
expr_accessors!(CoerceViaIO, CoerceViaIO, is_coerceviaio, as_coerceviaio, as_coerceviaio_mut, expect_into_coerceviaio);
expr_accessors!(ArrayCoerceExpr, ArrayCoerceExpr, is_arraycoerceexpr, as_arraycoerceexpr, as_arraycoerceexpr_mut, expect_into_arraycoerceexpr);
expr_accessors!(ConvertRowtypeExpr, ConvertRowtypeExpr, is_convertrowtypeexpr, as_convertrowtypeexpr, as_convertrowtypeexpr_mut, expect_into_convertrowtypeexpr);
expr_accessors!(CollateExpr, CollateExpr, is_collateexpr, as_collateexpr, as_collateexpr_mut, expect_into_collateexpr);
expr_accessors!(CaseExpr, CaseExpr, is_caseexpr, as_caseexpr, as_caseexpr_mut, expect_into_caseexpr);
expr_accessors!(ArrayExpr, ArrayExpr, is_arrayexpr, as_arrayexpr, as_arrayexpr_mut, expect_into_arrayexpr);
expr_accessors!(RowExpr, RowExpr, is_rowexpr, as_rowexpr, as_rowexpr_mut, expect_into_rowexpr);
expr_accessors!(RowCompareExpr, RowCompareExpr, is_rowcompareexpr, as_rowcompareexpr, as_rowcompareexpr_mut, expect_into_rowcompareexpr);
expr_accessors!(CoalesceExpr, CoalesceExpr, is_coalesceexpr, as_coalesceexpr, as_coalesceexpr_mut, expect_into_coalesceexpr);
expr_accessors!(MinMaxExpr, MinMaxExpr, is_minmaxexpr, as_minmaxexpr, as_minmaxexpr_mut, expect_into_minmaxexpr);
expr_accessors!(SQLValueFunction, SQLValueFunction, is_sqlvaluefunction, as_sqlvaluefunction, as_sqlvaluefunction_mut, expect_into_sqlvaluefunction);
expr_accessors!(XmlExpr, XmlExpr, is_xmlexpr, as_xmlexpr, as_xmlexpr_mut, expect_into_xmlexpr);
expr_accessors!(JsonValueExpr, JsonValueExpr, is_jsonvalueexpr, as_jsonvalueexpr, as_jsonvalueexpr_mut, expect_into_jsonvalueexpr);
expr_accessors!(JsonConstructorExpr, JsonConstructorExpr, is_jsonconstructorexpr, as_jsonconstructorexpr, as_jsonconstructorexpr_mut, expect_into_jsonconstructorexpr);
expr_accessors!(JsonExpr, JsonExpr, is_jsonexpr, as_jsonexpr, as_jsonexpr_mut, expect_into_jsonexpr);
expr_accessors!(NullTest, NullTest, is_nulltest, as_nulltest, as_nulltest_mut, expect_into_nulltest);
expr_accessors!(BooleanTest, BooleanTest, is_booleantest, as_booleantest, as_booleantest_mut, expect_into_booleantest);
expr_accessors!(CoerceToDomain, CoerceToDomain, is_coercetodomain, as_coercetodomain, as_coercetodomain_mut, expect_into_coercetodomain);
expr_accessors!(CaseTestExpr, CaseTestExpr, is_casetestexpr, as_casetestexpr, as_casetestexpr_mut, expect_into_casetestexpr);
expr_accessors!(NextValueExpr, NextValueExpr, is_nextvalueexpr, as_nextvalueexpr, as_nextvalueexpr_mut, expect_into_nextvalueexpr);
expr_accessors!(CurrentOfExpr, CurrentOfExpr, is_currentofexpr, as_currentofexpr, as_currentofexpr_mut, expect_into_currentofexpr);

impl Expr {
    /// `IsA(node, Var)`.
    #[inline]
    pub fn is_var(&self) -> bool {
        matches!(self, Expr::Var(_))
    }
    /// `castNode(Var, node)` borrow (mirrors [`Expr::expect_var`], named `as_var`
    /// for parity with the other `as_*` accessors).
    #[inline]
    pub fn as_var(&self) -> Option<&Var> {
        self.expect_var()
    }
    /// `IsA(node, DistinctExpr)`.
    #[inline]
    pub fn is_distinctexpr(&self) -> bool {
        matches!(self, Expr::DistinctExpr(_))
    }
    /// `IsA(node, NullIfExpr)`.
    #[inline]
    pub fn is_nullifexpr(&self) -> bool {
        matches!(self, Expr::NullIfExpr(_))
    }
    /// `castNode(OpExpr, node)` for a `DistinctExpr` payload (struct-equal to
    /// `OpExpr`).
    #[inline]
    pub fn as_distinctexpr(&self) -> Option<&OpExpr> {
        match self {
            Expr::DistinctExpr(o) => Some(o),
            _ => None,
        }
    }
    /// `castNode(OpExpr, node)` for a `NullIfExpr` payload.
    #[inline]
    pub fn as_nullifexpr(&self) -> Option<&OpExpr> {
        match self {
            Expr::NullIfExpr(o) => Some(o),
            _ => None,
        }
    }
    /// Consume a `DistinctExpr` node, returning its `OpExpr` payload.
    #[inline]
    pub fn expect_into_distinctexpr(self) -> OpExpr {
        match self {
            Expr::DistinctExpr(o) => o,
            _ => panic!("Expr::expect_into_distinctexpr: not a DistinctExpr"),
        }
    }
    /// Consume a `NullIfExpr` node, returning its `OpExpr` payload.
    #[inline]
    pub fn expect_into_nullifexpr(self) -> OpExpr {
        match self {
            Expr::NullIfExpr(o) => o,
            _ => panic!("Expr::expect_into_nullifexpr: not a NullIfExpr"),
        }
    }
    /// `IsA(node, PlaceHolderVar)`.
    #[inline]
    pub fn is_placeholdervar(&self) -> bool {
        matches!(self, Expr::PlaceHolderVar(_))
    }
    /// `castNode(PlaceHolderVar, node)` borrow (parity alias).
    #[inline]
    pub fn as_placeholdervar(&self) -> Option<&PlaceHolderVar> {
        self.expect_placeholdervar()
    }
    /// Consume a `PlaceHolderVar` node, returning its payload.
    #[inline]
    pub fn expect_into_placeholdervar(self) -> PlaceHolderVar {
        match self {
            Expr::PlaceHolderVar(p) => p,
            _ => panic!("Expr::expect_into_placeholdervar: not a PlaceHolderVar"),
        }
    }
    /// `castNode(Const, node)` — borrow the [`Const`] payload, panicking on a
    /// wrong tag (C `castNode` elogs). Used where C dereferences a node it has
    /// already proven is a Const.
    #[inline]
    pub fn expect_const(&self) -> &Const {
        match self {
            Expr::Const(c) => c,
            _ => panic!("Expr::expect_const: node is not a Const"),
        }
    }

    /// `IsA(node, OpExpr)`.
    #[inline]
    pub fn is_opexpr(&self) -> bool {
        matches!(self, Expr::OpExpr(_))
    }
    /// `castNode(OpExpr, node)` borrow (parity alias for [`Expr::expect_opexpr`]).
    #[inline]
    pub fn as_opexpr(&self) -> Option<&OpExpr> {
        self.expect_opexpr()
    }
    /// Mutable borrow of the [`OpExpr`] payload, or `None`.
    #[inline]
    pub fn as_opexpr_mut(&mut self) -> Option<&mut OpExpr> {
        match self {
            Expr::OpExpr(o) => Some(o),
            _ => None,
        }
    }
    /// Consume an `OpExpr` node, returning its payload.
    #[inline]
    pub fn expect_into_opexpr(self) -> OpExpr {
        match self {
            Expr::OpExpr(o) => o,
            _ => panic!("Expr::expect_into_opexpr: not an OpExpr"),
        }
    }

    /// `IsA(node, SubPlan)`.
    #[inline]
    pub fn is_subplan(&self) -> bool {
        matches!(self, Expr::SubPlan(_))
    }
    /// `castNode(SubPlan, node)` — borrow the [`SubPlanExpr`] payload, or `None`.
    #[inline]
    pub fn as_subplan(&self) -> Option<&SubPlanExpr> {
        match self {
            Expr::SubPlan(s) => Some(s),
            _ => None,
        }
    }
    /// `IsA(node, AlternativeSubPlan)`.
    #[inline]
    pub fn is_alternativesubplan(&self) -> bool {
        matches!(self, Expr::AlternativeSubPlan(_))
    }
    /// `castNode(AlternativeSubPlan, node)` — borrow the payload, or `None`.
    #[inline]
    pub fn as_alternativesubplan(&self) -> Option<&AlternativeSubPlanExpr> {
        match self {
            Expr::AlternativeSubPlan(s) => Some(s),
            _ => None,
        }
    }
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
pub struct SubPlanExpr(pub Box<SubPlan<'static>>);

impl Clone for SubPlanExpr {
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
pub struct AlternativeSubPlanExpr(pub Box<AlternativeSubPlan<'static>>);

impl Clone for AlternativeSubPlanExpr {
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
    pub expr: Option<PgBox<'mcx, Expr>>,
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
                Some(e) => Some(alloc_in(mcx, (**e).clone())?),
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
