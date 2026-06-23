//! Raw-grammar expression nodes (`nodes/primnodes.h`, *pre-analysis* form).
//!
//! In C, the `Expr`-deriving nodes the grammar produces — `BoolExpr`,
//! `CaseExpr`/`CaseWhen`, `CoalesceExpr`, `MinMaxExpr`, `SubLink`, `NullTest`,
//! `BooleanTest`, `RowExpr`, `GroupingFunc`, `CollateExpr`, `SetToDefault`,
//! `CurrentOfExpr`, `NamedArgExpr`, `SQLValueFunction`, `XmlExpr` — are a single
//! struct type each, used in *both* the raw output of `gram.y` and the
//! post-analysis tree. Their `Node *`/`List *` children, however, are *raw*
//! parse-tree nodes (`ColumnRef`/`A_Expr`/`A_Const`/…) in the grammar output;
//! `transformExpr` (analyze.c) later replaces those children with executable
//! [`crate::primnodes::Expr`] subtrees.
//!
//! The owned model split the post-analysis form into the lifetime-free
//! [`crate::primnodes::Expr`] enum (children are `Expr`), which therefore cannot
//! carry the raw `Node *` children the grammar builds. This module supplies the
//! *raw* counterparts: field-for-field mirrors of the C structs whose `Node`/
//! `List` children are [`crate::nodes::NodePtr`] (a raw `Node *`), wired as arms
//! of the central [`crate::nodes::Node`] enum. They are the grammar's targets
//! and analyze's inputs; they are NOT the post-analysis `Expr` (no conflation —
//! `Expr` remains its own enum).
//!
//! Modelling rules (docs/types.md): `Node *` → `Option<NodePtr>` / required
//! `NodePtr`; `List *` → `PgVec<NodePtr>`; `char *` → `Option<PgString>`; the
//! `Expr xpr` header (the NodeTag) is dropped (the enum arm carries it); planner
//! /analyze-fill scalar fields (result `Oid`s, typmods, collations) keep their C
//! types and ride as data — the grammar leaves them `InvalidOid`/`-1`.

use ::mcx::{Mcx, PgString, PgVec};
use ::types_core::primitive::{Index, Oid};
use ::types_error::PgResult;

use crate::nodes::NodePtr;
use crate::primnodes::{
    BoolExprType, BoolTestType, CoercionForm, MinMaxOp, NullTestType, SQLValueFunctionOp,
    SubLinkType, XmlExprOp, XmlOptionType,
};

// Shared copy helpers live in `rawnodes`; re-use them here to keep the uniform
// `copyObject` shape (deep-copy onto a target `mcx`).
use crate::rawnodes::{copy_node_vec, copy_opt_node, copy_opt_str, TypeName};

/// `BoolExpr` (`nodes/primnodes.h`) — AND/OR/NOT over raw `Node *` arguments.
///
/// Raw form: `args` are raw parse-tree nodes (analyze's `transformExpr` turns
/// each into a boolean [`crate::primnodes::Expr`]).
#[derive(Debug)]
pub struct BoolExpr<'mcx> {
    /// `BoolExprType boolop`.
    pub boolop: BoolExprType,
    /// `List *args` — arguments to this expression (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl BoolExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `BoolExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<BoolExpr<'b>> {
        Ok(BoolExpr {
            boolop: self.boolop,
            args: copy_node_vec(&self.args, mcx)?,
            location: self.location,
        })
    }
}

/// `CaseExpr` (`nodes/primnodes.h`) — a CASE expression, raw form.
///
/// `casetype`/`casecollid` are analyzer-filled (the grammar leaves them
/// `InvalidOid`); `arg`/`args`/`defresult` carry raw nodes (`args` is a list of
/// [`CaseWhen`]).
#[derive(Debug)]
pub struct CaseExpr<'mcx> {
    /// `Oid casetype` — type of expression result (analyzer-filled).
    pub casetype: Oid,
    /// `Oid casecollid` — collation, or `InvalidOid` (analyzer-filled).
    pub casecollid: Oid,
    /// `Expr *arg` — implicit equality comparison argument (raw `Node`), or none.
    pub arg: Option<NodePtr<'mcx>>,
    /// `List *args` — the WHEN clauses (raw [`CaseWhen`] nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Expr *defresult` — the ELSE result (raw `Node`), or none.
    pub defresult: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CaseExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CaseExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CaseExpr<'b>> {
        Ok(CaseExpr {
            casetype: self.casetype,
            casecollid: self.casecollid,
            arg: copy_opt_node(&self.arg, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            defresult: copy_opt_node(&self.defresult, mcx)?,
            location: self.location,
        })
    }
}

/// `CaseWhen` (`nodes/primnodes.h`) — one arm of a CASE expression, raw form.
#[derive(Debug)]
pub struct CaseWhen<'mcx> {
    /// `Expr *expr` — condition expression (raw `Node`).
    pub expr: Option<NodePtr<'mcx>>,
    /// `Expr *result` — substitution result (raw `Node`).
    pub result: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CaseWhen<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CaseWhen`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CaseWhen<'b>> {
        Ok(CaseWhen {
            expr: copy_opt_node(&self.expr, mcx)?,
            result: copy_opt_node(&self.result, mcx)?,
            location: self.location,
        })
    }
}

/// `CoalesceExpr` (`nodes/primnodes.h`) — a COALESCE expression, raw form.
#[derive(Debug)]
pub struct CoalesceExpr<'mcx> {
    /// `Oid coalescetype` — type of expression result (analyzer-filled).
    pub coalescetype: Oid,
    /// `Oid coalescecollid` — collation, or `InvalidOid` (analyzer-filled).
    pub coalescecollid: Oid,
    /// `List *args` — the arguments (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CoalesceExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CoalesceExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CoalesceExpr<'b>> {
        Ok(CoalesceExpr {
            coalescetype: self.coalescetype,
            coalescecollid: self.coalescecollid,
            args: copy_node_vec(&self.args, mcx)?,
            location: self.location,
        })
    }
}

/// `MinMaxExpr` (`nodes/primnodes.h`) — a GREATEST or LEAST function, raw form.
#[derive(Debug)]
pub struct MinMaxExpr<'mcx> {
    /// `Oid minmaxtype` — common type of arguments and result (analyzer-filled).
    pub minmaxtype: Oid,
    /// `Oid minmaxcollid` — collation of result (analyzer-filled).
    pub minmaxcollid: Oid,
    /// `Oid inputcollid` — collation the function should use (analyzer-filled).
    pub inputcollid: Oid,
    /// `MinMaxOp op` — GREATEST vs LEAST.
    pub op: MinMaxOp,
    /// `List *args` — the arguments (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl MinMaxExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `MinMaxExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MinMaxExpr<'b>> {
        Ok(MinMaxExpr {
            minmaxtype: self.minmaxtype,
            minmaxcollid: self.minmaxcollid,
            inputcollid: self.inputcollid,
            op: self.op,
            args: copy_node_vec(&self.args, mcx)?,
            location: self.location,
        })
    }
}

/// `SubLink` (`nodes/primnodes.h`) — a subselect appearing in an expression,
/// raw form.
///
/// Per the C comment: in the raw output of `gram.y`, `testexpr` is the raw form
/// of the lefthand expression, `operName` is the `String` name of the combining
/// operator, and `subselect` is a raw parsetree (a [`crate::rawnodes::SelectStmt`]
/// `Node`). analyze.c transforms `testexpr` and turns `subselect` into a `Query`.
#[derive(Debug)]
pub struct SubLink<'mcx> {
    /// `SubLinkType subLinkType`.
    pub sub_link_type: SubLinkType,
    /// `int subLinkId` — ID (1..n); 0 if not MULTIEXPR.
    pub sub_link_id: i32,
    /// `Node *testexpr` — outer-query test for ALL/ANY/ROWCOMPARE (raw).
    pub testexpr: Option<NodePtr<'mcx>>,
    /// `List *operName` — originally specified operator name (`String` nodes).
    pub oper_name: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *subselect` — subselect as raw parsetree (or `Query` post-analysis).
    pub subselect: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl SubLink<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `SubLink`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SubLink<'b>> {
        Ok(SubLink {
            sub_link_type: self.sub_link_type,
            sub_link_id: self.sub_link_id,
            testexpr: copy_opt_node(&self.testexpr, mcx)?,
            oper_name: copy_node_vec(&self.oper_name, mcx)?,
            subselect: copy_opt_node(&self.subselect, mcx)?,
            location: self.location,
        })
    }
}

/// `NullTest` (`nodes/primnodes.h`) — IS [NOT] NULL, raw form.
#[derive(Debug)]
pub struct NullTest<'mcx> {
    /// `Expr *arg` — input expression (raw `Node`).
    pub arg: Option<NodePtr<'mcx>>,
    /// `NullTestType nulltesttype` — IS NULL / IS NOT NULL.
    pub nulltesttype: NullTestType,
    /// `bool argisrow` — perform field-by-field null checks.
    pub argisrow: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl NullTest<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `NullTest`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<NullTest<'b>> {
        Ok(NullTest {
            arg: copy_opt_node(&self.arg, mcx)?,
            nulltesttype: self.nulltesttype,
            argisrow: self.argisrow,
            location: self.location,
        })
    }
}

/// `BooleanTest` (`nodes/primnodes.h`) — IS [NOT] TRUE/FALSE/UNKNOWN, raw form.
#[derive(Debug)]
pub struct BooleanTest<'mcx> {
    /// `Expr *arg` — input expression (raw `Node`).
    pub arg: Option<NodePtr<'mcx>>,
    /// `BoolTestType booltesttype` — the test type.
    pub booltesttype: BoolTestType,
    /// `ParseLoc location`.
    pub location: i32,
}

impl BooleanTest<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `BooleanTest`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<BooleanTest<'b>> {
        Ok(BooleanTest {
            arg: copy_opt_node(&self.arg, mcx)?,
            booltesttype: self.booltesttype,
            location: self.location,
        })
    }
}

/// `RowExpr` (`nodes/primnodes.h`) — a ROW(...) constructor, raw form.
#[derive(Debug)]
pub struct RowExpr<'mcx> {
    /// `List *args` — the fields (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Oid row_typeid` — RECORDOID or a composite type's ID (analyzer-filled).
    pub row_typeid: Oid,
    /// `CoercionForm row_format` — how to display this node.
    pub row_format: CoercionForm,
    /// `List *colnames` — list of `String`, or NIL.
    pub colnames: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl RowExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RowExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RowExpr<'b>> {
        Ok(RowExpr {
            args: copy_node_vec(&self.args, mcx)?,
            row_typeid: self.row_typeid,
            row_format: self.row_format,
            colnames: copy_node_vec(&self.colnames, mcx)?,
            location: self.location,
        })
    }
}

/// `GroupingFunc` (`nodes/primnodes.h`) — GROUPING(...), raw form.
#[derive(Debug)]
pub struct GroupingFunc<'mcx> {
    /// `List *args` — arguments, kept for EXPLAIN (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *refs` — ressortgrouprefs of arguments (analyzer-filled).
    pub refs: PgVec<'mcx, i32>,
    /// `List *cols` — actual column positions set by planner.
    pub cols: PgVec<'mcx, i32>,
    /// `Index agglevelsup` — same as `Aggref.agglevelsup`.
    pub agglevelsup: Index,
    /// `ParseLoc location`.
    pub location: i32,
}

impl GroupingFunc<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `GroupingFunc`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GroupingFunc<'b>> {
        let mut refs = ::mcx::vec_with_capacity_in(mcx, self.refs.len())?;
        for r in self.refs.iter() {
            refs.push(*r);
        }
        let mut cols = ::mcx::vec_with_capacity_in(mcx, self.cols.len())?;
        for c in self.cols.iter() {
            cols.push(*c);
        }
        Ok(GroupingFunc {
            args: copy_node_vec(&self.args, mcx)?,
            refs,
            cols,
            agglevelsup: self.agglevelsup,
            location: self.location,
        })
    }
}

/// `CollateExpr` (`nodes/primnodes.h`) — a COLLATE applied to an expression,
/// raw form. (Distinct from the raw-grammar [`crate::rawnodes::CollateClause`]:
/// the grammar emits `CollateClause`; `CollateExpr` is produced where a node
/// already carries a resolved collation — kept for the model's completeness.)
#[derive(Debug)]
pub struct CollateExpr<'mcx> {
    /// `Expr *arg` — input expression (raw `Node`).
    pub arg: Option<NodePtr<'mcx>>,
    /// `Oid collOid` — collation's OID.
    pub coll_oid: Oid,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CollateExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CollateExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CollateExpr<'b>> {
        Ok(CollateExpr {
            arg: copy_opt_node(&self.arg, mcx)?,
            coll_oid: self.coll_oid,
            location: self.location,
        })
    }
}

/// `SetToDefault` (`nodes/primnodes.h`) — a DEFAULT marker in INSERT/UPDATE.
#[derive(Clone, Copy, Debug, Default)]
pub struct SetToDefault {
    /// `Oid typeId` — type for substituted value (analyzer-filled).
    pub type_id: Oid,
    /// `int32 typeMod` — typmod for substituted value.
    pub type_mod: i32,
    /// `Oid collation` — collation for the substituted value.
    pub collation: Oid,
    /// `ParseLoc location`.
    pub location: i32,
}

impl SetToDefault {
    /// Deep copy (no owned children; C: `copyObject` over `SetToDefault`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<SetToDefault> {
        Ok(*self)
    }
}

/// `CurrentOfExpr` (`nodes/primnodes.h`) — `[WHERE] CURRENT OF cursor_name`.
///
/// (A lifetime-free [`crate::primnodes::CurrentOfExpr`] already exists for the
/// `Expr` enum; this `'mcx`-string variant matches the raw-`Node` convention so
/// `WHERE CURRENT OF` rides the raw expression tree uniformly.)
#[derive(Debug)]
pub struct CurrentOfExpr<'mcx> {
    /// `Index cvarno` — RT index of target relation.
    pub cvarno: Index,
    /// `char *cursor_name` — name of referenced cursor, or `None`.
    pub cursor_name: Option<PgString<'mcx>>,
    /// `int cursor_param` — refcursor parameter number, or 0.
    pub cursor_param: i32,
}

impl CurrentOfExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CurrentOfExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CurrentOfExpr<'b>> {
        Ok(CurrentOfExpr {
            cvarno: self.cvarno,
            cursor_name: copy_opt_str(&self.cursor_name, mcx)?,
            cursor_param: self.cursor_param,
        })
    }
}

/// `NamedArgExpr` (`nodes/primnodes.h`) — a named function argument (`name =>
/// expr`), raw form.
#[derive(Debug)]
pub struct NamedArgExpr<'mcx> {
    /// `Expr *arg` — the argument expression (raw `Node`).
    pub arg: Option<NodePtr<'mcx>>,
    /// `char *name` — the name.
    pub name: Option<PgString<'mcx>>,
    /// `int argnumber` — argument's number in positional notation.
    pub argnumber: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl NamedArgExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `NamedArgExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<NamedArgExpr<'b>> {
        Ok(NamedArgExpr {
            arg: copy_opt_node(&self.arg, mcx)?,
            name: copy_opt_str(&self.name, mcx)?,
            argnumber: self.argnumber,
            location: self.location,
        })
    }
}

/// `SQLValueFunction` (`nodes/primnodes.h`) — a parameterless SQL value function
/// (CURRENT_DATE, CURRENT_USER, …) with a special grammar production.
#[derive(Clone, Copy, Debug)]
pub struct SQLValueFunction {
    /// `SQLValueFunctionOp op` — which function this is.
    pub op: SQLValueFunctionOp,
    /// `Oid type` — result type (fully determined by `op`).
    pub type_: Oid,
    /// `int32 typmod`.
    pub typmod: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl SQLValueFunction {
    /// Deep copy (no owned children; C: `copyObject` over `SQLValueFunction`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<SQLValueFunction> {
        Ok(*self)
    }
}

/// `XmlExpr` (`nodes/primnodes.h`) — SQL/XML functions with special grammar
/// productions, raw form.
#[derive(Debug)]
pub struct XmlExpr<'mcx> {
    /// `XmlExprOp op` — XML function ID.
    pub op: XmlExprOp,
    /// `char *name` — name in `xml(NAME foo ...)` syntaxes.
    pub name: Option<PgString<'mcx>>,
    /// `List *named_args` — non-XML expressions for `xml_attributes` (raw nodes).
    pub named_args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *arg_names` — parallel list of `String` values.
    pub arg_names: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *args` — list of expressions (raw nodes).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `XmlOptionType xmloption` — DOCUMENT or CONTENT.
    pub xmloption: XmlOptionType,
    /// `bool indent` — INDENT option for XMLSERIALIZE.
    pub indent: bool,
    /// `Oid type` — target type for XMLSERIALIZE (analyzer-filled).
    pub type_: Oid,
    /// `int32 typmod` (analyzer-filled).
    pub typmod: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

/// `XmlSerialize` (nodes/parsenodes.h:868) — the raw-grammar representation of
/// an `XMLSERIALIZE(... AS type)` expression. Transformed by
/// `transformXmlSerialize` (parse_expr.c) into a cooked `XmlExpr` wrapped in a
/// coercion to the target type.
#[derive(Debug)]
pub struct XmlSerialize<'mcx> {
    /// `XmlOptionType xmloption` — DOCUMENT or CONTENT.
    pub xmloption: XmlOptionType,
    /// `Node *expr` — the value expression to serialize.
    pub expr: Option<NodePtr<'mcx>>,
    /// `TypeName *typeName` — the target SQL type (`AS <type>`).
    pub type_name: Option<::mcx::PgBox<'mcx, TypeName<'mcx>>>,
    /// `bool indent` — `[NO] INDENT`.
    pub indent: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl XmlSerialize<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `XmlSerialize`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<XmlSerialize<'b>> {
        Ok(XmlSerialize {
            xmloption: self.xmloption,
            expr: copy_opt_node(&self.expr, mcx)?,
            type_name: match &self.type_name {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            indent: self.indent,
            location: self.location,
        })
    }
}

impl XmlExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `XmlExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<XmlExpr<'b>> {
        Ok(XmlExpr {
            op: self.op,
            name: copy_opt_str(&self.name, mcx)?,
            named_args: copy_node_vec(&self.named_args, mcx)?,
            arg_names: copy_node_vec(&self.arg_names, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            xmloption: self.xmloption,
            indent: self.indent,
            type_: self.type_,
            typmod: self.typmod,
            location: self.location,
        })
    }
}

// ===========================================================================
// SQL/JSON raw-grammar nodes (nodes/parsenodes.h, nodes/primnodes.h).
//
// These are the *untransformed* SQL/JSON constructor / query / aggregate nodes
// the grammar emits. They carry *raw* `Node *` children ([`NodePtr`]); analyze
// (`parse_expr.c`'s `transformJson*` family) rewrites each into a cooked
// [`crate::primnodes::Expr`] (`JsonConstructorExpr`/`JsonExpr`/…). They are NOT
// the post-analysis `Expr` nodes (those keep their own enum). Field-for-field
// mirrors of the C structs with the uniform `Node *`→`Option<NodePtr>`,
// `List *`→`PgVec<NodePtr>`, `char *`→`Option<PgString>`, typed `*mut Child`→
// `Option<PgBox<Child>>` mapping.
// ===========================================================================

use crate::primnodes::{JsonExprOp, JsonQuotes, JsonTableColumnType, JsonWrapper};

/// `JsonFormat` (nodes/primnodes.h) — raw FORMAT clause. Identical shape to the
/// cooked [`crate::primnodes::JsonFormat`]; carried by-value here (no children).
pub use crate::primnodes::JsonFormat;

/// `JsonValueExpr` (nodes/primnodes.h) — *raw* form: a `expr [FORMAT ...]` JSON
/// value expression whose `raw_expr` is a raw parse-tree node. analyze fills
/// `formatted_expr` and produces the cooked [`crate::primnodes::JsonValueExpr`].
#[derive(Debug)]
pub struct JsonValueExpr<'mcx> {
    /// `Expr *raw_expr` — user-specified raw expression.
    pub raw_expr: Option<NodePtr<'mcx>>,
    /// `Expr *formatted_expr` — analyze-filled; `None` in raw form.
    pub formatted_expr: Option<NodePtr<'mcx>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
}

impl JsonValueExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `JsonValueExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonValueExpr<'b>> {
        Ok(JsonValueExpr {
            raw_expr: copy_opt_node(&self.raw_expr, mcx)?,
            formatted_expr: copy_opt_node(&self.formatted_expr, mcx)?,
            format: self.format,
        })
    }
}

/// `JsonIsPredicate` (nodes/primnodes.h) — *raw* `expr IS JSON [type]` whose
/// `expr` is a raw parse-tree node (analyze produces the cooked
/// [`crate::primnodes::JsonIsPredicate`]).
#[derive(Debug)]
pub struct JsonIsPredicate<'mcx> {
    /// `Node *expr`.
    pub expr: Option<NodePtr<'mcx>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `JsonValueType item_type`.
    pub item_type: crate::primnodes::JsonValueType,
    /// `bool unique_keys`.
    pub unique_keys: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonIsPredicate<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonIsPredicate<'b>> {
        Ok(JsonIsPredicate {
            expr: copy_opt_node(&self.expr, mcx)?,
            format: self.format,
            item_type: self.item_type,
            unique_keys: self.unique_keys,
            location: self.location,
        })
    }
}

/// `JsonBehavior` (nodes/primnodes.h) — *raw* ON ERROR / ON EMPTY clause whose
/// `expr` is a raw parse-tree node (analyze produces the cooked
/// [`crate::primnodes::JsonBehavior`]).
#[derive(Debug)]
pub struct JsonBehavior<'mcx> {
    /// `JsonBehaviorType btype`.
    pub btype: crate::primnodes::JsonBehaviorType,
    /// `Node *expr`.
    pub expr: Option<NodePtr<'mcx>>,
    /// `bool coerce`.
    pub coerce: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonBehavior<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonBehavior<'b>> {
        Ok(JsonBehavior {
            btype: self.btype,
            expr: copy_opt_node(&self.expr, mcx)?,
            coerce: self.coerce,
            location: self.location,
        })
    }
}

/// `JsonOutput` (nodes/parsenodes.h) — `RETURNING type [FORMAT format]`.
#[derive(Debug)]
pub struct JsonOutput<'mcx> {
    /// `TypeName *typeName`.
    pub type_name: Option<::mcx::PgBox<'mcx, TypeName<'mcx>>>,
    /// `JsonReturning *returning` — analyze-filled.
    pub returning: Option<crate::primnodes::JsonReturning>,
}

impl JsonOutput<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `JsonOutput`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonOutput<'b>> {
        Ok(JsonOutput {
            type_name: match &self.type_name {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            returning: self.returning,
        })
    }
}

/// `JsonKeyValue` (nodes/parsenodes.h) — a `key VALUE value` pair for
/// `JSON_OBJECT()`/`JSON_OBJECTAGG()`.
#[derive(Debug)]
pub struct JsonKeyValue<'mcx> {
    /// `Expr *key` — raw key expression.
    pub key: Option<NodePtr<'mcx>>,
    /// `JsonValueExpr *value` — raw value.
    pub value: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
}

impl JsonKeyValue<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `JsonKeyValue`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonKeyValue<'b>> {
        Ok(JsonKeyValue {
            key: copy_opt_node(&self.key, mcx)?,
            value: match &self.value {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `JsonObjectConstructor` (nodes/parsenodes.h) — raw `JSON_OBJECT()`.
#[derive(Debug)]
pub struct JsonObjectConstructor<'mcx> {
    /// `List *exprs` — list of `JsonKeyValue` pairs.
    pub exprs: PgVec<'mcx, NodePtr<'mcx>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
    /// `bool unique`.
    pub unique: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonObjectConstructor<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonObjectConstructor<'b>> {
        Ok(JsonObjectConstructor {
            exprs: copy_node_vec(&self.exprs, mcx)?,
            output: copy_opt_json_output(&self.output, mcx)?,
            absent_on_null: self.absent_on_null,
            unique: self.unique,
            location: self.location,
        })
    }
}

/// `JsonArrayConstructor` (nodes/parsenodes.h) — raw `JSON_ARRAY(elem, ...)`.
#[derive(Debug)]
pub struct JsonArrayConstructor<'mcx> {
    /// `List *exprs` — list of `JsonValueExpr` elements.
    pub exprs: PgVec<'mcx, NodePtr<'mcx>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonArrayConstructor<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonArrayConstructor<'b>> {
        Ok(JsonArrayConstructor {
            exprs: copy_node_vec(&self.exprs, mcx)?,
            output: copy_opt_json_output(&self.output, mcx)?,
            absent_on_null: self.absent_on_null,
            location: self.location,
        })
    }
}

/// `JsonArrayQueryConstructor` (nodes/parsenodes.h) — raw `JSON_ARRAY(subquery)`.
#[derive(Debug)]
pub struct JsonArrayQueryConstructor<'mcx> {
    /// `Node *query` — the subquery.
    pub query: Option<NodePtr<'mcx>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonArrayQueryConstructor<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonArrayQueryConstructor<'b>> {
        Ok(JsonArrayQueryConstructor {
            query: copy_opt_node(&self.query, mcx)?,
            output: copy_opt_json_output(&self.output, mcx)?,
            format: self.format,
            absent_on_null: self.absent_on_null,
            location: self.location,
        })
    }
}

/// `JsonAggConstructor` (nodes/parsenodes.h) — common fields of `JSON_OBJECTAGG`
/// / `JSON_ARRAYAGG`.
#[derive(Debug)]
pub struct JsonAggConstructor<'mcx> {
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `Node *agg_filter` — FILTER clause.
    pub agg_filter: Option<NodePtr<'mcx>>,
    /// `List *agg_order` — ORDER BY in the aggregate.
    pub agg_order: PgVec<'mcx, NodePtr<'mcx>>,
    /// `WindowDef *over` — OVER clause.
    pub over: Option<::mcx::PgBox<'mcx, crate::rawnodes::WindowDef<'mcx>>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonAggConstructor<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonAggConstructor<'b>> {
        Ok(JsonAggConstructor {
            output: copy_opt_json_output(&self.output, mcx)?,
            agg_filter: copy_opt_node(&self.agg_filter, mcx)?,
            agg_order: copy_node_vec(&self.agg_order, mcx)?,
            over: match &self.over {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            location: self.location,
        })
    }
}

/// `JsonObjectAgg` (nodes/parsenodes.h) — raw `JSON_OBJECTAGG()`.
#[derive(Debug)]
pub struct JsonObjectAgg<'mcx> {
    /// `JsonAggConstructor *constructor`.
    pub constructor: Option<::mcx::PgBox<'mcx, JsonAggConstructor<'mcx>>>,
    /// `JsonKeyValue *arg`.
    pub arg: Option<::mcx::PgBox<'mcx, JsonKeyValue<'mcx>>>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
    /// `bool unique`.
    pub unique: bool,
}

impl JsonObjectAgg<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonObjectAgg<'b>> {
        Ok(JsonObjectAgg {
            constructor: match &self.constructor {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            arg: match &self.arg {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            absent_on_null: self.absent_on_null,
            unique: self.unique,
        })
    }
}

/// `JsonArrayAgg` (nodes/parsenodes.h) — raw `JSON_ARRAYAGG()`.
#[derive(Debug)]
pub struct JsonArrayAgg<'mcx> {
    /// `JsonAggConstructor *constructor`.
    pub constructor: Option<::mcx::PgBox<'mcx, JsonAggConstructor<'mcx>>>,
    /// `JsonValueExpr *arg`.
    pub arg: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `bool absent_on_null`.
    pub absent_on_null: bool,
}

impl JsonArrayAgg<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonArrayAgg<'b>> {
        Ok(JsonArrayAgg {
            constructor: match &self.constructor {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            arg: match &self.arg {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            absent_on_null: self.absent_on_null,
        })
    }
}

/// `JsonParseExpr` (nodes/parsenodes.h) — raw `JSON()`.
#[derive(Debug)]
pub struct JsonParseExpr<'mcx> {
    /// `JsonValueExpr *expr`.
    pub expr: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `bool unique_keys`.
    pub unique_keys: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonParseExpr<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonParseExpr<'b>> {
        Ok(JsonParseExpr {
            expr: match &self.expr {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            output: copy_opt_json_output(&self.output, mcx)?,
            unique_keys: self.unique_keys,
            location: self.location,
        })
    }
}

/// `JsonScalarExpr` (nodes/parsenodes.h) — raw `JSON_SCALAR()`.
#[derive(Debug)]
pub struct JsonScalarExpr<'mcx> {
    /// `Expr *expr` — raw subject expression.
    pub expr: Option<NodePtr<'mcx>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonScalarExpr<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonScalarExpr<'b>> {
        Ok(JsonScalarExpr {
            expr: copy_opt_node(&self.expr, mcx)?,
            output: copy_opt_json_output(&self.output, mcx)?,
            location: self.location,
        })
    }
}

/// `JsonSerializeExpr` (nodes/parsenodes.h) — raw `JSON_SERIALIZE()`.
#[derive(Debug)]
pub struct JsonSerializeExpr<'mcx> {
    /// `JsonValueExpr *expr`.
    pub expr: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonSerializeExpr<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonSerializeExpr<'b>> {
        Ok(JsonSerializeExpr {
            expr: match &self.expr {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            output: copy_opt_json_output(&self.output, mcx)?,
            location: self.location,
        })
    }
}

/// `JsonArgument` (nodes/parsenodes.h) — one `name AS value` PASSING argument.
#[derive(Debug)]
pub struct JsonArgument<'mcx> {
    /// `JsonValueExpr *val`.
    pub val: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `char *name`.
    pub name: Option<PgString<'mcx>>,
}

impl JsonArgument<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonArgument<'b>> {
        Ok(JsonArgument {
            val: match &self.val {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            name: copy_opt_str(&self.name, mcx)?,
        })
    }
}

/// `JsonFuncExpr` (nodes/parsenodes.h) — raw `JSON_VALUE`/`JSON_QUERY`/
/// `JSON_EXISTS`/`JSON_TABLE` query function.
#[derive(Debug)]
pub struct JsonFuncExpr<'mcx> {
    /// `JsonExprOp op`.
    pub op: JsonExprOp,
    /// `char *column_name`.
    pub column_name: Option<PgString<'mcx>>,
    /// `JsonValueExpr *context_item`.
    pub context_item: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `Node *pathspec`.
    pub pathspec: Option<NodePtr<'mcx>>,
    /// `List *passing` — list of `JsonArgument`.
    pub passing: PgVec<'mcx, NodePtr<'mcx>>,
    /// `JsonOutput *output`.
    pub output: Option<::mcx::PgBox<'mcx, JsonOutput<'mcx>>>,
    /// `JsonBehavior *on_empty`.
    pub on_empty: Option<NodePtr<'mcx>>,
    /// `JsonBehavior *on_error`.
    pub on_error: Option<NodePtr<'mcx>>,
    /// `JsonWrapper wrapper`.
    pub wrapper: JsonWrapper,
    /// `JsonQuotes quotes`.
    pub quotes: JsonQuotes,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonFuncExpr<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonFuncExpr<'b>> {
        Ok(JsonFuncExpr {
            op: self.op,
            column_name: copy_opt_str(&self.column_name, mcx)?,
            context_item: match &self.context_item {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            pathspec: copy_opt_node(&self.pathspec, mcx)?,
            passing: copy_node_vec(&self.passing, mcx)?,
            output: copy_opt_json_output(&self.output, mcx)?,
            on_empty: copy_opt_node(&self.on_empty, mcx)?,
            on_error: copy_opt_node(&self.on_error, mcx)?,
            wrapper: self.wrapper,
            quotes: self.quotes,
            location: self.location,
        })
    }
}

/// `JsonTablePathSpec` (nodes/parsenodes.h) — a JSON path expression with an
/// optional name.
#[derive(Debug)]
pub struct JsonTablePathSpec<'mcx> {
    /// `Node *string` — the path string expression.
    pub string: Option<NodePtr<'mcx>>,
    /// `char *name`.
    pub name: Option<PgString<'mcx>>,
    /// `ParseLoc name_location`.
    pub name_location: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonTablePathSpec<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonTablePathSpec<'b>> {
        Ok(JsonTablePathSpec {
            string: copy_opt_node(&self.string, mcx)?,
            name: copy_opt_str(&self.name, mcx)?,
            name_location: self.name_location,
            location: self.location,
        })
    }
}

/// `JsonTable` (nodes/parsenodes.h) — raw `JSON_TABLE()`.
#[derive(Debug)]
pub struct JsonTable<'mcx> {
    /// `JsonValueExpr *context_item`.
    pub context_item: Option<::mcx::PgBox<'mcx, JsonValueExpr<'mcx>>>,
    /// `JsonTablePathSpec *pathspec`.
    pub pathspec: Option<::mcx::PgBox<'mcx, JsonTablePathSpec<'mcx>>>,
    /// `List *passing` — list of `JsonArgument`.
    pub passing: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *columns` — list of `JsonTableColumn`.
    pub columns: PgVec<'mcx, NodePtr<'mcx>>,
    /// `JsonBehavior *on_error`.
    pub on_error: Option<NodePtr<'mcx>>,
    /// `Alias *alias`.
    pub alias: Option<::mcx::PgBox<'mcx, crate::rawnodes::Alias<'mcx>>>,
    /// `bool lateral`.
    pub lateral: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonTable<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonTable<'b>> {
        Ok(JsonTable {
            context_item: match &self.context_item {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            pathspec: match &self.pathspec {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            passing: copy_node_vec(&self.passing, mcx)?,
            columns: copy_node_vec(&self.columns, mcx)?,
            on_error: copy_opt_node(&self.on_error, mcx)?,
            alias: match &self.alias {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            lateral: self.lateral,
            location: self.location,
        })
    }
}

/// `JsonTableColumn` (nodes/parsenodes.h) — a `JSON_TABLE` column.
#[derive(Debug)]
pub struct JsonTableColumn<'mcx> {
    /// `JsonTableColumnType coltype`.
    pub coltype: JsonTableColumnType,
    /// `char *name`.
    pub name: Option<PgString<'mcx>>,
    /// `TypeName *typeName`.
    pub type_name: Option<::mcx::PgBox<'mcx, TypeName<'mcx>>>,
    /// `JsonTablePathSpec *pathspec`.
    pub pathspec: Option<::mcx::PgBox<'mcx, JsonTablePathSpec<'mcx>>>,
    /// `JsonFormat *format`.
    pub format: Option<JsonFormat>,
    /// `JsonWrapper wrapper`.
    pub wrapper: JsonWrapper,
    /// `JsonQuotes quotes`.
    pub quotes: JsonQuotes,
    /// `List *columns` — nested columns.
    pub columns: PgVec<'mcx, NodePtr<'mcx>>,
    /// `JsonBehavior *on_empty`.
    pub on_empty: Option<NodePtr<'mcx>>,
    /// `JsonBehavior *on_error`.
    pub on_error: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl JsonTableColumn<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JsonTableColumn<'b>> {
        Ok(JsonTableColumn {
            coltype: self.coltype,
            name: copy_opt_str(&self.name, mcx)?,
            type_name: match &self.type_name {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            pathspec: match &self.pathspec {
                Some(t) => Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            format: self.format,
            wrapper: self.wrapper,
            quotes: self.quotes,
            columns: copy_node_vec(&self.columns, mcx)?,
            on_empty: copy_opt_node(&self.on_empty, mcx)?,
            on_error: copy_opt_node(&self.on_error, mcx)?,
            location: self.location,
        })
    }
}

/// Deep-copy an `Option<PgBox<JsonOutput>>` into `mcx` (shared helper).
fn copy_opt_json_output<'b>(
    o: &Option<::mcx::PgBox<'_, JsonOutput<'_>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<::mcx::PgBox<'b, JsonOutput<'b>>>> {
    match o {
        Some(t) => Ok(Some(::mcx::alloc_in(mcx, t.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}
