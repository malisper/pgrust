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

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::{Index, Oid};
use types_error::PgResult;

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
        let mut refs = mcx::vec_with_capacity_in(mcx, self.refs.len())?;
        for r in self.refs.iter() {
            refs.push(*r);
        }
        let mut cols = mcx::vec_with_capacity_in(mcx, self.cols.len())?;
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
    pub type_name: Option<mcx::PgBox<'mcx, TypeName<'mcx>>>,
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
                Some(t) => Some(mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
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
