//! Family: **makefuncs** — `nodes/makefuncs.c`, the node constructors.
//!
//! The `make*` constructors that `palloc` and populate a node. In the layered
//! owned-tree model the targets fall in three buckets:
//!
//! * **executable-expression nodes** (`makeVar`, `makeConst`, `makeBoolExpr`,
//!   `makeRelabelType`, `makeFuncExpr`, `make_opclause`, the boolean-clause
//!   helpers, `makeTargetEntry`, the JSON expression nodes) build a
//!   [`types_nodes::primnodes::Expr`] subtree. The Expr tree is lifetime-free
//!   (owned `Box`/`Vec`), so these are total constructors. Where the C returns
//!   the node through a `Node *`-typed API (`get_typdefault`, the partition-qual
//!   list), the seam wraps the Expr as [`Node::Expr`] and allocates the box in
//!   `mcx` — exactly the C `(Node *) expr` cast palloc'd in the current context.
//! * **plan/exec-state nodes** (`makeIndexInfo`) build a `types_nodes` exec
//!   struct.
//! * **raw-parser nodes** (`makeRangeVar`, `makeTypeName*`) build an owned
//!   plain-Rust parse node (`types_tuple::RangeVar`, `types_parsenodes::*`); no
//!   allocator.
//! * **raw-grammar parse nodes** (`makeA_Expr`, `makeFromExpr`, `makeFuncCall`,
//!   `makeColumnDef`, `makeAlias`, `makeGroupingSet`, `makeVarFromTargetEntry`,
//!   `makeNullConst`, `makeDefElem`, `makeDefElemExtended`) — the K1-parsetree
//!   raw vocabulary the parser's `parse_*` cluster needs. The list/child fields
//!   are `types_nodes::NodePtr`/`PgVec` charged on `mcx` (exactly the C
//!   `makeNode` palloc in the current context); `makeNullConst` reads the type's
//!   storage props via the lsyscache `get_typlenbyval` seam, and
//!   `makeVarFromTargetEntry` reads the TLE's `Expr` type/typmod/collation via
//!   the nodefuncs accessors.
//!
//! ## Not yet portable (model gaps; not stubbed)
//!
//! `makeSimpleA_Expr`/`makeStringConst` need a `String`/value node carried as a
//! `types_nodes::NodePtr` (the operator-name `list_make1(makeString(name))` and
//! `A_Const.val.sval`). The value-node arms (`Node::Integer`/`Float`/`Boolean`/
//! `String`/`BitString`, nodes/value.h) now exist in `types_nodes::Node` (added
//! by the node-walker keystone), so these two constructors are unblocked and
//! ready to fill by the parser cluster; only `makeWholeRowVar`'s
//! function-RTE branches need a `Node`-level `exprType` over a
//! `RangeTblFunction.funcexpr` `NodePtr` (the repo's `expr_type` works over the
//! trimmed `Expr`, not `Node`). `makeNotNullConstraint`/`makeVacuumRelation`/
//! `makeJsonKeyValue`/`makeJsonTablePath`/`makeJsonTablePathSpec` target node
//! types (`Constraint`/`VacuumRelation`/`JsonKeyValue`/`JsonTablePath`/
//! `JsonTablePathSpec`) are not yet in `types_nodes` — additive keystone types
//! the owning DDL/JSON parser units introduce. They are absent here rather than
//! stubbed (`mirror-pg-and-panic`: there is no faithful body to write yet).
//!
//! Owns the canonical `backend-nodes-makefuncs-seams`
//! (`make_const_node`, `make_and_boolexpr`, `make_type_name_from_name_list`),
//! installed in [`super::init_seams`].
//!
//! mirror-PG-and-panic: `makeConst`'s varlena-detoast branch
//! (`PG_DETOAST_DATUM`) delegates to the `backend-access-common-detoast` owner's
//! `detoast_attr` seam.

use mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, InvalidAttrNumber, Oid};
use types_core::catalog::BOOLOID;
use types_core::InvalidOid;
// Datum-unification: the owned `Const` carries the canonical unified value type
// [`Datum`] (`ByVal`/`ByRef`), and `make_const`/`make_const_node_seam` thread it
// end-to-end. The only residual use of the bare-word [`ScalarWord`] (the canonical
// `ByVal` arm's payload, `types_datum::Datum`) is the sanctioned varlena-pointer
// edge in `pg_detoast_datum`: a varlena `Datum` is a bare pointer into a varlena
// image, and the `detoast_attr` seam returns the fetched bytes through a leaked
// pointer word (the audited bare-word ABI edge), not a `ByRef` slice.
use types_datum::Datum as ScalarWord;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;

use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CoercionForm, Const, Expr, FuncExpr, JsonBehavior, JsonBehaviorType,
    JsonConstructorExpr, JsonConstructorType, JsonFormat, JsonFormatType, JsonEncoding,
    JsonIsPredicate, JsonReturning, JsonValueExpr, JsonValueType,
    NullTest, NullTestType, OpExpr, RelabelType, TargetEntry, Var, AND_EXPR, NOT_EXPR, OR_EXPR,
};
use types_nodes::execnodes::IndexInfo;
use types_nodes::nodes::NodePtr;
use types_nodes::rawnodes::{
    A_Expr, A_Expr_Kind, Alias, ColumnDef, FromExpr, FuncCall, GroupingSet, GroupingSetKind,
};
use types_tuple::access::{RangeVar, RELPERSISTENCE_PERMANENT};

use types_parsenodes::{
    DefElem, DefElemAction, Node as ParseNode, StringNode, TypeName, DEFELEM_UNSPEC,
};

use backend_access_common_detoast_seams as detoast_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;

// ===========================================================================
// Expression-node constructors (build an owned `Expr` subtree).
// ===========================================================================

/// `makeVar(varno, varattno, vartype, vartypmod, varcollid, varlevelsup)`
/// (makefuncs.c) — create a `Var` node.
///
/// The trimmed [`Var`] carries the fields executor/optimizer readers consume
/// (including `varcollid`); `varreturningtype`, `varnullingrels`,
/// `varnosyn`/`varattnosyn` and `location` (which the C also sets to defaults)
/// are not modeled here.
pub fn make_var(
    varno: i32,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: Index,
) -> Var {
    Var {
        varno,
        varattno,
        vartype,
        vartypmod,
        varcollid,
        varlevelsup,
        ..Default::default()
    }
}

/// `makeWholeRowVar(rte, varno, varlevelsup, allowScalar)` (makefuncs.c) —
/// build a whole-row `Var` referencing range-table entry `rte`.
///
/// All arms are ported in full: the RELATION case (a named composite rowtype
/// from `get_rel_type_id`), the join / VALUES / CTE / tablefunc cases (RECORD),
/// and the SUBQUERY-expanded-from-SRF and FUNCTION cases — these read `exprType`
/// (and `exprCollation` for the allowScalar branch) over the
/// `RangeTblFunction.funcexpr`, which is an expression node reached via
/// `Node::as_expr()`.
pub fn make_whole_row_var(
    rte: &types_nodes::parsenodes::RangeTblEntry<'_>,
    varno: i32,
    varlevelsup: Index,
    allow_scalar: bool,
) -> PgResult<Var> {
    use types_nodes::parsenodes::RTEKind;
    const RECORDOID: Oid = 2249;

    let result = match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // relation: the rowtype is a named composite type
            let toid = lsyscache::get_rel_type_id::call(rte.relid)?;
            if toid == InvalidOid {
                return Err(types_error::PgError::error(format!(
                    "relation with OID {} does not have a composite type",
                    rte.relid
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE));
            }
            make_var(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup)
        }
        RTEKind::RTE_SUBQUERY => {
            if rte.relid != InvalidOid {
                // Subquery expanded from a view: use the view's rowtype.
                let toid = lsyscache::get_rel_type_id::call(rte.relid)?;
                if toid == InvalidOid {
                    return Err(types_error::PgError::error(format!(
                        "relation with OID {} does not have a composite type",
                        rte.relid
                    ))
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE));
                }
                make_var(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup)
            } else if !rte.functions.is_empty() {
                // Subquery was expanded from a set-returning function.  That
                // would not have happened if there's more than one function or
                // ordinality was requested.  We also needn't worry about the
                // allowScalar case, since the planner doesn't use that.
                // Otherwise this must match the RTE_FUNCTION code below.
                debug_assert!(!allow_scalar);
                let rtf = rte.functions[0]
                    .as_rangetblfunction()
                    .expect("RTE_SUBQUERY functions element must be a RangeTblFunction");
                let fexpr = rtf.funcexpr.as_deref().and_then(|n| n.as_expr());
                let mut toid = super::nodefuncs::expr_type(fexpr)?;
                if !lsyscache::type_is_rowtype::call(toid)? {
                    toid = RECORDOID;
                }
                make_var(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup)
            } else {
                // Normal subquery-in-FROM: anonymous RECORD.
                make_var(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup)
            }
        }
        RTEKind::RTE_FUNCTION => {
            if rte.funcordinality || rte.functions.len() != 1 {
                // More than one function or ordinality: anonymous RECORD.
                make_var(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup)
            } else {
                let rtf = rte.functions[0]
                    .as_rangetblfunction()
                    .expect("RTE_FUNCTION functions element must be a RangeTblFunction");
                let fexpr = rtf.funcexpr.as_deref().and_then(|n| n.as_expr());
                let toid = super::nodefuncs::expr_type(fexpr)?;
                if lsyscache::type_is_rowtype::call(toid)? {
                    // func returns composite; same as relation case
                    make_var(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup)
                } else if allow_scalar {
                    // func returns scalar; just return its output as-is
                    make_var(
                        varno,
                        1,
                        toid,
                        -1,
                        super::nodefuncs::expr_collation(fexpr)?,
                        varlevelsup,
                    )
                } else {
                    // func returns scalar, but we want a composite result
                    make_var(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup)
                }
            }
        }
        // join, tablefunc, VALUES, CTE, etc. — whole-row Var of RECORD type.
        _ => make_var(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup),
    };

    Ok(result)
}

thread_local! {
    /// The backend-lifetime memory context backing every by-reference
    /// `Const.constvalue` image. C's `makeConst` builds the `Const` in the
    /// current (message/parse) memory context, where the input function's
    /// palloc'd varlena already lives — long-lived enough to outlive the planner
    /// run. The owned model `datumCopy`'s by-reference const values into this
    /// leaked, never-reset context so the stored `Datum<'static>` referent stays
    /// valid for the node's lifetime (mirrors `params::PARAM_LIST_CONTEXT` and
    /// the `SubPlan`/`AlternativeSubPlan` `'static`-erase convention).
    static CONST_VALUE_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("Const value")));
}

/// `Mcx<'static>` for the backend-lifetime [`CONST_VALUE_CONTEXT`] — where a
/// by-reference `Const.constvalue` image is copied so it can carry `'static`.
pub fn const_value_mcx() -> Mcx<'static> {
    CONST_VALUE_CONTEXT.with(|c| c.mcx())
}

/// Copy a (per-read-context) by-reference / composite `Const.constvalue` image
/// into the backend-lifetime const-value context so it can be stored as the
/// `Datum<'static>` a `Const` node carries. The mirror of `make_const`'s
/// `'static`-erase, for `_readConst` (`readDatum`): `readfuncs` reconstructs the
/// flat varlena bytes in its transient read `mcx`, and this re-homes them into
/// the leaked never-reset [`CONST_VALUE_CONTEXT`] (exactly as `make_const`
/// `datumCopy`'s an input function's palloc'd varlena). By-value words borrow
/// nothing and pass straight through; `cstring` carries its text in an owned
/// `String`. `Expanded`/`Internal` cannot appear in a serialized `Const`.
pub fn intern_const_value(value: &Datum<'_>) -> PgResult<Datum<'static>> {
    Ok(match value {
        Datum::ByVal(word) => Datum::ByVal(*word),
        Datum::Cstring(s) => Datum::Cstring(s.clone()),
        other @ (Datum::ByRef(_) | Datum::Composite(_)) => other.clone_in(const_value_mcx())?,
        Datum::Expanded(_) | Datum::Internal(_) => panic!(
            "intern_const_value: Expanded/Internal cannot appear in a serialized Const"
        ),
    })
}

/// `makeConst(consttype, consttypmod, constcollid, constlen, constvalue,
/// constisnull, constbyval)` (makefuncs.c) — create a `Const` node.
///
/// If it's a varlena value (`constlen == -1`) and not null, force it to
/// non-expanded/non-toasted format (`PG_DETOAST_DATUM`) for representation
/// consistency, delegating the fetch/decompress to the `detoast` owner.
///
/// The [`Const`] carries
/// `consttype`/`consttypmod`/`constcollid`/`constlen`/`constvalue`/
/// `constisnull`/`constbyval`/`location`; `constlen`/`constbyval` also drive the
/// detoast decision exactly as in the C.
pub fn make_const<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i32,
    mut constvalue: Datum<'mcx>,
    constisnull: bool,
    constbyval: bool,
) -> PgResult<Const<'mcx>> {
    // if (!constisnull && constlen == -1)
    //     constvalue = PointerGetDatum(PG_DETOAST_DATUM(constvalue));
    //
    // The varlena `PG_DETOAST_DATUM` leg operates on the bare pointer word the
    // value's `ByVal` arm wraps (the sanctioned bare-word edge: a varlena Datum
    // is a pointer into a varlena image). A by-reference value here is the
    // execTuples canonical-carrier follow-on (#113): the detoast owner's seam
    // still takes a byte slice, so a `ByRef` image would already be the flat
    // bytes — no fetch/decompress would be needed (it cannot be external/
    // compressed). We therefore detoast only the by-value (pointer-word) form.
    if !constisnull && constlen == -1 {
        if let Datum::ByVal(word) = constvalue {
            constvalue =
                Datum::ByVal(pg_detoast_datum(mcx, ScalarWord::from_usize(word))?.as_usize());
        }
    }

    // The trimmed `Const.constvalue` field is typed `Datum<'static>` (the node
    // carries no lifetime parameter). The by-value word IS the canonical `ByVal`
    // payload (a bare machine word, or — for a varlena — a pointer into a varlena
    // image that outlives `mcx`), exactly C's `Const.constvalue` Datum. A
    // by-REFERENCE value (text/name/varchar/numeric) carries borrowed-from-`mcx`
    // bytes; C's `make_const` leaves them where the input function placed them,
    // in the long-lived parse/plan context that outlives the planner run. The
    // owned model mirrors that by `datumCopy`'ing the by-reference image into a
    // backend-lifetime context ([`const_value_mcx`]), which yields a genuine
    // `Datum<'static>` (the SubPlan/ParamListInfo `'static`-erase convention) —
    // no pointer is forged across the lifetime boundary.
    let constvalue: Datum<'static> = match constvalue {
        Datum::ByVal(word) => Datum::ByVal(word),
        // A `cstring` value (e.g. the UNKNOWN const make_const builds for an
        // unadorned string literal) carries its text in an OWNED `String` — it
        // borrows nothing from `mcx`, so it is freely a `'static` value, exactly
        // like C's `Const.constvalue` cstring pointer that outlives the parse.
        Datum::Cstring(s) => Datum::Cstring(s),
        // A by-reference / composite literal: copy the (already-flat, detoasted
        // above) bytes into the backend-lifetime const-value context so the
        // stored `Datum<'static>` referent stays valid for the node's lifetime
        // (C: the input function's palloc'd varlena lives in the message/parse
        // context the Const is built in).
        other @ (Datum::ByRef(_) | Datum::Composite(_)) => other.clone_in(const_value_mcx())?,
        Datum::Expanded(_) | Datum::Internal(_) => panic!(
            "make_const: Expanded/Internal Const value requires a \
             lifetime-carrying Const carrier — not yet produced — wave 2"
        ),
    };

    Ok(Const {
        consttype,
        consttypmod,
        constcollid,
        constlen,
        constvalue,
        constisnull,
        constbyval,
        // makeConst sets location = -1.
        location: -1,
    })
}

/// `makeNullConst` / `makeBoolConst` build varlena/plain consts; `makeBoolConst`
/// is self-contained (it hardwires bool's storage like the C does), so it lives
/// here. `makeNullConst` needs `get_typlenbyval` from the lsyscache owner and
/// is built by that caller path, not modeled in this trimmed family.
///
/// `makeBoolConst(value, isnull)` (makefuncs.c) — a `Const` of type `bool`.
/// The C hardwires bool's `constlen == 1` / `constbyval == true`, so no
/// detoast and no allocation can occur.
pub fn make_bool_const<'mcx>(value: bool, isnull: bool) -> Const<'mcx> {
    // makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(value), isnull, true)
    Const {
        consttype: BOOLOID,
        consttypmod: -1,
        constcollid: InvalidOid,
        constlen: 1,
        constvalue: Datum::from_bool(value),
        constisnull: isnull,
        constbyval: true,
        // makeConst sets location = -1.
        location: -1,
    }
}

/// Build the `arg IS NOT NULL` `NullTest` node used by `process_equivalence`
/// (equivclass.c): `argisrow=false` (correct even for a composite arg),
/// `location=-1`.
pub fn make_is_not_null<'mcx>(arg: Expr<'mcx>) -> Expr<'mcx> {
    Expr::NullTest(NullTest {
        arg: Some(Box::new(arg)),
        nulltesttype: NullTestType::IS_NOT_NULL,
        argisrow: false,
        location: -1,
    })
}

/// `makeBoolExpr(boolop, args, location)` (makefuncs.c) — a `BoolExpr` node.
pub fn make_bool_expr<'mcx>(boolop: BoolExprType, args: Vec<Expr<'mcx>>, location: i32) -> Expr<'mcx> {
    Expr::BoolExpr(BoolExpr {
        boolop,
        args,
        location,
    })
}

/// `make_andclause(andclauses)` (makefuncs.c) — `BoolExpr` with `AND_EXPR`.
/// (clauses.c sets `location = -1`.)
pub fn make_andclause<'mcx>(andclauses: Vec<Expr<'mcx>>) -> Expr<'mcx> {
    Expr::BoolExpr(BoolExpr {
        boolop: AND_EXPR,
        args: andclauses,
        location: -1,
    })
}

/// `make_orclause(orclauses)` (makefuncs.c) — `BoolExpr` with `OR_EXPR`.
/// (clauses.c sets `location = -1`.)
pub fn make_orclause<'mcx>(orclauses: Vec<Expr<'mcx>>) -> Expr<'mcx> {
    Expr::BoolExpr(BoolExpr {
        boolop: OR_EXPR,
        args: orclauses,
        location: -1,
    })
}

/// `make_notclause(notclause)` (makefuncs.c) — `BoolExpr` with `NOT_EXPR` over
/// the single negated expression (`list_make1(notclause)`). (clauses.c sets
/// `location = -1`.)
pub fn make_notclause<'mcx>(notclause: Expr<'mcx>) -> Expr<'mcx> {
    Expr::BoolExpr(BoolExpr {
        boolop: NOT_EXPR,
        args: vec![notclause],
        location: -1,
    })
}

/// `make_and_qual(qual1, qual2)` (makefuncs.c) — AND two qual conditions,
/// treating a `None` (C `NULL`) nodetree as TRUE.
pub fn make_and_qual<'mcx>(qual1: Option<Expr<'mcx>>, qual2: Option<Expr<'mcx>>) -> Option<Expr<'mcx>> {
    match (qual1, qual2) {
        // if (qual1 == NULL) return qual2;
        (None, q2) => q2,
        // if (qual2 == NULL) return qual1;
        (q1, None) => q1,
        // return (Node *) make_andclause(list_make2(qual1, qual2));
        (Some(q1), Some(q2)) => Some(make_andclause(vec![q1, q2])),
    }
}

/// `make_ands_explicit(andclauses)` (makefuncs.c) — convert an AND-semantics
/// expression list to an ordinary boolean expression. An empty list is TRUE.
pub fn make_ands_explicit<'mcx>(mut andclauses: Vec<Expr<'mcx>>) -> Expr<'mcx> {
    if andclauses.is_empty() {
        // return (Expr *) makeBoolConst(true, false);
        Expr::Const(make_bool_const(true, false))
    } else if andclauses.len() == 1 {
        // return (Expr *) linitial(andclauses);
        andclauses.remove(0)
    } else {
        // return make_andclause(andclauses);
        make_andclause(andclauses)
    }
}

/// `make_ands_implicit(clause)` (makefuncs.c) — convert an ordinary boolean
/// expression to an AND-semantics list. A `None`/constant-TRUE clause yields
/// the empty list (TRUE).
pub fn make_ands_implicit<'mcx>(clause: Option<Expr<'mcx>>) -> Vec<Expr<'mcx>> {
    match clause {
        // if (clause == NULL) return NIL; /* NULL -> NIL list == TRUE */
        None => Vec::new(),
        Some(Expr::BoolExpr(b)) if b.boolop == AND_EXPR => {
            // if (is_andclause(clause)) return ((BoolExpr *) clause)->args;
            b.args
        }
        // else if (IsA(clause, Const) && !constisnull && DatumGetBool(constvalue))
        //     return NIL; /* constant TRUE input -> NIL list */
        Some(Expr::Const(ref c)) if !c.constisnull && c.constvalue.as_bool() => Vec::new(),
        // else return list_make1(clause);
        Some(other) => vec![other],
    }
}

/// `makeRelabelType(arg, rtype, rtypmod, rcollid, rformat)` (makefuncs.c) — a
/// no-op binary-compatible coercion node.
///
/// The [`RelabelType`] carries every field the C sets; `makeRelabelType` sets
/// `location = -1`.
pub fn make_relabel_type<'mcx>(
    arg: Expr<'mcx>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
) -> Expr<'mcx> {
    Expr::RelabelType(RelabelType {
        arg: Some(Box::new(arg)),
        resulttype: rtype,
        resulttypmod: rtypmod,
        resultcollid: rcollid,
        relabelformat: rformat,
        location: -1,
    })
}

/// `makeFuncExpr(funcid, rettype, args, funccollid, inputcollid, fformat)`
/// (makefuncs.c) — a function-call expression. `funcretset`/`funcvariadic` are
/// always `false` here (the only allowed case); `makeFuncExpr` sets
/// `location = -1`.
pub fn make_func_expr<'mcx>(
    funcid: Oid,
    rettype: Oid,
    args: Vec<Expr<'mcx>>,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: CoercionForm,
) -> Expr<'mcx> {
    Expr::FuncExpr(FuncExpr {
        funcid,
        funcresulttype: rettype,
        funcretset: false,
        funcvariadic: false,
        funcformat: fformat,
        funccollid,
        inputcollid,
        args,
        location: -1,
    })
}

/// `make_opclause(opno, opresulttype, opretset, leftop, rightop, opcollid,
/// inputcollid)` (makefuncs.c) — an operator-invocation clause. Pass
/// `rightop == None` for a single-operand clause. `opfuncid` is left
/// `InvalidOid` (resolved later); `location` (set to -1 by the C) is not
/// modeled in the trimmed [`OpExpr`].
pub fn make_opclause<'mcx>(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: Expr<'mcx>,
    rightop: Option<Expr<'mcx>>,
    opcollid: Oid,
    inputcollid: Oid,
) -> Expr<'mcx> {
    let args = match rightop {
        // expr->args = list_make2(leftop, rightop);
        Some(r) => vec![leftop, r],
        // expr->args = list_make1(leftop);
        None => vec![leftop],
    };
    Expr::OpExpr(OpExpr {
        opno,
        opfuncid: InvalidOid,
        opresulttype,
        opretset,
        opcollid,
        inputcollid,
        args,
        location: -1,
    })
}

/// `makeTargetEntry(expr, resno, resname, resjunk)` (makefuncs.c) — a
/// `TargetEntry` node, allocated in `mcx` (the boxed child `expr` and `resname`
/// string live in the same context). C `makeTargetEntry` zeroes
/// `ressortgroupref`/`resorigtbl`/`resorigcol`.
pub fn make_target_entry<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr<'mcx>,
    resno: AttrNumber,
    resname: Option<&str>,
    resjunk: bool,
) -> PgResult<TargetEntry<'mcx>> {
    Ok(TargetEntry {
        expr: Some(alloc_in(mcx, expr)?),
        resno,
        resname: match resname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk,
    })
}

/// `flatCopyTargetEntry(src_tle)` (makefuncs.c) — duplicate a `TargetEntry`
/// without copying substructure. Here the deep model has no shared-pointer
/// substructure to alias, so the C `memcpy` becomes a fallible deep copy into
/// `mcx` (`TargetEntry::clone_in`).
pub fn flat_copy_target_entry<'mcx>(
    mcx: Mcx<'mcx>,
    src_tle: &TargetEntry<'_>,
) -> PgResult<TargetEntry<'mcx>> {
    src_tle.clone_in(mcx)
}

// ===========================================================================
// JSON expression-node constructors.
// ===========================================================================

/// `makeJsonFormat(type, encoding, location)` (makefuncs.c) — a `JsonFormat`
/// node.
pub fn make_json_format(format_type: JsonFormatType, encoding: JsonEncoding, location: i32) -> JsonFormat {
    JsonFormat {
        format_type,
        encoding,
        location,
    }
}

/// `makeJsonValueExpr(raw_expr, formatted_expr, format)` (makefuncs.c) — a
/// `JsonValueExpr` node.
pub fn make_json_value_expr<'mcx>(
    raw_expr: Option<Expr<'mcx>>,
    formatted_expr: Option<Expr<'mcx>>,
    format: Option<JsonFormat>,
) -> JsonValueExpr<'mcx> {
    JsonValueExpr {
        raw_expr: raw_expr.map(Box::new),
        formatted_expr: formatted_expr.map(Box::new),
        format,
    }
}

/// `makeJsonBehavior(btype, expr, location)` (makefuncs.c) — a `JsonBehavior`
/// node. `coerce` is left at its default (the C leaves it zero too).
pub fn make_json_behavior<'mcx>(btype: JsonBehaviorType, expr: Option<Expr<'mcx>>, location: i32) -> JsonBehavior<'mcx> {
    JsonBehavior {
        btype,
        expr: expr.map(Box::new),
        coerce: false,
        location,
    }
}

/// `makeJsonIsPredicate(expr, format, item_type, unique_keys, location)`
/// (makefuncs.c) — a `JsonIsPredicate` node, returned as a `Node` in the C.
pub fn make_json_is_predicate<'mcx>(
    expr: Option<Expr<'mcx>>,
    format: Option<JsonFormat>,
    item_type: JsonValueType,
    unique_keys: bool,
    location: i32,
) -> Expr<'mcx> {
    Expr::JsonIsPredicate(JsonIsPredicate {
        expr: expr.map(Box::new),
        format,
        item_type,
        unique_keys,
        location,
    })
}

/// `makeJsonConstructorExpr(type, args, fexpr, returning, unique,
/// absent_on_null, location)` (parse_expr.c) — the *bare* `JsonConstructorExpr`
/// constructor (without the RETURNING coercion, which the parser fills in via
/// `coerceJsonFuncExpr` because it needs the `ParseState`). `coercion` starts
/// `None`; the caller sets it.
#[allow(clippy::too_many_arguments)]
pub fn make_json_constructor_expr<'mcx>(
    r#type: JsonConstructorType,
    args: Vec<Expr<'mcx>>,
    func: Option<Expr<'mcx>>,
    coercion: Option<Expr<'mcx>>,
    returning: Option<JsonReturning>,
    unique: bool,
    absent_on_null: bool,
    location: i32,
) -> JsonConstructorExpr<'mcx> {
    JsonConstructorExpr {
        r#type,
        args,
        func: func.map(Box::new),
        coercion: coercion.map(Box::new),
        returning,
        absent_on_null,
        unique,
        location,
    }
}

// ===========================================================================
// Plan / exec-state node constructors.
// ===========================================================================

/// `makeIndexInfo(numattrs, numkeyattrs, amoid, expressions, predicates,
/// unique, nulls_not_distinct, isready, concurrent, summarizing,
/// withoutoverlaps)` (makefuncs.c) — an `IndexInfo` node.
///
/// The trimmed [`IndexInfo`] carries the build-state scalars the executor
/// consults; the expression/predicate lists, exclusion/unique op arrays, and
/// the memory-context handle (which the C also initializes) are not modeled
/// here. `ii_Summarizing`/`ii_WithoutOverlaps`/`ii_IndexUnchanged` are likewise
/// not modeled, so their inputs are accepted but unused.
pub fn make_index_info<'mcx>(
    numattrs: i32,
    numkeyattrs: i32,
    amoid: Oid,
    _expressions: (),
    _predicates: (),
    unique: bool,
    nulls_not_distinct: bool,
    isready: bool,
    concurrent: bool,
    summarizing: bool,
    withoutoverlaps: bool,
) -> IndexInfo<'mcx> {
    // Asserts mirrored from the C (ii_NumIndexKeyAttrs != 0,
    // ii_NumIndexKeyAttrs <= ii_NumIndexAttrs).
    debug_assert!(numkeyattrs != 0);
    debug_assert!(numkeyattrs <= numattrs);
    // C `makeIndexInfo` sets ii_Summarizing/ii_WithoutOverlaps from its args
    // and zeroes the rest (palloc0); the expression/predicate lists are not
    // wired in this port (the callers pass `()` placeholders), so they stay the
    // `None` (C `NIL`/`NULL`) the struct defaults to.
    IndexInfo {
        ii_NumIndexAttrs: numattrs,
        ii_NumIndexKeyAttrs: numkeyattrs,
        ii_Unique: unique,
        ii_NullsNotDistinct: nulls_not_distinct,
        ii_ReadyForInserts: isready,
        ii_CheckedUnchanged: false,
        ii_IndexUnchanged: false,
        ii_Concurrent: concurrent,
        ii_BrokenHotChain: false,
        ii_Summarizing: summarizing,
        ii_WithoutOverlaps: withoutoverlaps,
        ii_ParallelWorkers: 0,
        ii_Am: amoid,
        ii_IndexAttrNumbers: Default::default(),
        ..Default::default()
    }
}

// ===========================================================================
// Raw-parser node constructors (owned plain-Rust parse nodes).
// ===========================================================================

/// `makeRangeVar(schemaname, relname, location)` (makefuncs.c) — a `RangeVar`
/// node (oversimplified case): `catalogname`/`alias` NULL, `inh` true,
/// `relpersistence` permanent.
pub fn make_range_var(schemaname: Option<String>, relname: String, location: i32) -> RangeVar {
    RangeVar {
        catalogname: None,
        schemaname,
        relname,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location,
    }
}

/// `makeTypeName(typnam)` (makefuncs.c) — a `TypeName` for an unqualified name
/// (`makeTypeNameFromNameList(list_make1(makeString(typnam)))`).
pub fn make_type_name(typnam: String) -> TypeName {
    make_type_name_from_name_list(vec![ParseNode::String(StringNode { sval: Some(typnam) })])
}

/// `makeTypeNameFromNameList(names)` (makefuncs.c) — a `TypeName` from a `List`
/// of `String` value nodes. `typmods` defaulted to NIL, `typemod`/`location`
/// to -1.
pub fn make_type_name_from_name_list(names: Vec<ParseNode>) -> TypeName {
    TypeName {
        names,
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// `makeTypeNameFromOid(typeOid, typmod)` (makefuncs.c) — a `TypeName` for a
/// type already known by OID/typmod. `location` defaulted to -1.
pub fn make_type_name_from_oid(type_oid: Oid, typmod: i32) -> TypeName {
    TypeName {
        names: Vec::new(),
        typeOid: type_oid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: typmod,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

// ===========================================================================
// Raw-grammar parse-node constructors (build owned `types_nodes` raw nodes).
//
// These build the K1-parsetree raw-grammar vocabulary the parser's `parse_*`
// recursive cluster needs. Their list/child fields are `types_nodes::NodePtr`
// (`PgBox<Node>`) / `PgVec`, charged on `mcx`, exactly like the C `palloc`s a
// node in the current memory context. Field-for-field vs makefuncs.c.
// ===========================================================================

/// `makeA_Expr(kind, name, lexpr, rexpr, location)` (makefuncs.c) — an `A_Expr`
/// node. The caller supplies the (possibly-qualified) operator `name` list and
/// the two operand subtrees. `rexpr_list_start`/`rexpr_list_end` (also zeroed by
/// `makeNode`) default to 0.
pub fn make_a_expr<'mcx>(
    kind: A_Expr_Kind,
    name: PgVec<'mcx, NodePtr<'mcx>>,
    lexpr: Option<NodePtr<'mcx>>,
    rexpr: Option<NodePtr<'mcx>>,
    location: i32,
) -> A_Expr<'mcx> {
    A_Expr {
        kind,
        name,
        lexpr,
        rexpr,
        rexpr_list_start: 0,
        rexpr_list_end: 0,
        location,
    }
}

/// `makeFromExpr(fromlist, quals)` (makefuncs.c) — a `FromExpr` node.
pub fn make_from_expr<'mcx>(
    fromlist: PgVec<'mcx, NodePtr<'mcx>>,
    quals: Option<NodePtr<'mcx>>,
) -> FromExpr<'mcx> {
    FromExpr { fromlist, quals }
}

/// `makeFuncCall(name, args, funcformat, location)` (makefuncs.c) — initialize a
/// `FuncCall` with the info every caller must supply; any non-default parameters
/// are inserted by the caller afterwards. Mirrors the C defaults exactly:
/// `agg_order = NIL`, `agg_filter = over = NULL`, all the agg/variadic flags
/// false.
pub fn make_func_call<'mcx>(
    mcx: Mcx<'mcx>,
    name: PgVec<'mcx, NodePtr<'mcx>>,
    args: PgVec<'mcx, NodePtr<'mcx>>,
    funcformat: CoercionForm,
    location: i32,
) -> PgResult<FuncCall<'mcx>> {
    Ok(FuncCall {
        funcname: name,
        args,
        agg_order: mcx::vec_with_capacity_in(mcx, 0)?,
        agg_filter: None,
        over: None,
        agg_within_group: false,
        agg_star: false,
        agg_distinct: false,
        func_variadic: false,
        funcformat,
        location,
    })
}

/// `makeColumnDef(colname, typeOid, typmod, collOid)` (makefuncs.c) — a simple
/// `ColumnDef`. Type/collation are specified by OID; other properties start
/// basic (`is_local = true`, the rest 0/NULL/NIL), exactly as the C sets them.
pub fn make_column_def<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
    type_oid: Oid,
    typmod: i32,
    coll_oid: Oid,
) -> PgResult<ColumnDef<'mcx>> {
    // makeTypeNameFromOid(typeOid, typmod) — the ColumnDef carries the
    // raw-grammar `types_nodes::rawnodes::TypeName`, distinct from the
    // `types_parsenodes::TypeName` the standalone `make_type_name_*` build. Its
    // list fields are `mcx`-charged.
    let type_name = types_nodes::rawnodes::TypeName {
        names: mcx::vec_with_capacity_in(mcx, 0)?,
        typeOid: type_oid,
        setof: false,
        pct_type: false,
        typmods: mcx::vec_with_capacity_in(mcx, 0)?,
        typemod: typmod,
        arrayBounds: mcx::vec_with_capacity_in(mcx, 0)?,
        location: -1,
    };
    Ok(ColumnDef {
        colname: Some(PgString::from_str_in(colname, mcx)?),
        typeName: Some(alloc_in(mcx, type_name)?),
        compression: None,
        inhcount: 0,
        is_local: true,
        is_not_null: false,
        is_from_type: false,
        storage: 0,
        storage_name: None,
        raw_default: None,
        cooked_default: None,
        identity: 0,
        identitySequence: None,
        generated: 0,
        collClause: None,
        collOid: coll_oid,
        constraints: mcx::vec_with_capacity_in(mcx, 0)?,
        fdwoptions: mcx::vec_with_capacity_in(mcx, 0)?,
        location: -1,
    })
}

/// `makeAlias(aliasname, colnames)` (makefuncs.c) — an `Alias` node. The given
/// name is copied (C: `pstrdup`); the `colnames` list (if any) isn't.
pub fn make_alias<'mcx>(
    mcx: Mcx<'mcx>,
    aliasname: &str,
    colnames: PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<Alias<'mcx>> {
    Ok(Alias {
        aliasname: Some(PgString::from_str_in(aliasname, mcx)?),
        colnames,
    })
}

/// `makeGroupingSet(kind, content, location)` (makefuncs.c) — a `GroupingSet`.
pub fn make_grouping_set<'mcx>(
    kind: GroupingSetKind,
    content: PgVec<'mcx, NodePtr<'mcx>>,
    location: i32,
) -> GroupingSet<'mcx> {
    GroupingSet {
        kind,
        content,
        location,
    }
}

/// `makeVarFromTargetEntry(varno, tle)` (makefuncs.c) — a same-level `Var` from
/// a `TargetEntry`: `makeVar(varno, tle->resno, exprType(tle->expr),
/// exprTypmod(tle->expr), exprCollation(tle->expr), 0)`. The type/typmod/
/// collation are read off the (trimmed) `Expr` subtree via the nodefuncs
/// accessors, exactly as the C reads them off `(Node *) tle->expr`.
pub fn make_var_from_target_entry(varno: i32, tle: &TargetEntry<'_>) -> PgResult<Var> {
    let expr = tle.expr.as_deref();
    Ok(make_var(
        varno,
        tle.resno,
        super::nodefuncs::expr_type(expr)?,
        super::nodefuncs::expr_typmod(expr)?,
        super::nodefuncs::expr_collation(expr)?,
        0,
    ))
}

/// `makeNullConst(consttype, consttypmod, constcollid)` (makefuncs.c) — a
/// `Const` representing a NULL of the given type/typmod. Saves a lookup of the
/// type's storage properties (`get_typlenbyval`) and delegates to `makeConst`
/// with a 0 datum, `constisnull = true`.
pub fn make_null_const<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
) -> PgResult<Const> {
    // get_typlenbyval(consttype, &typLen, &typByVal);
    let (typ_len, typ_byval) = lsyscache::get_typlenbyval::call(consttype)?;
    make_const(
        mcx,
        consttype,
        consttypmod,
        constcollid,
        typ_len as i32,
        // (Datum) 0 — a null value's datum is never inspected.
        Datum::ByVal(0),
        true,
        typ_byval,
    )
}

// ===========================================================================
// `nodes/parsenodes.h` constructors over the plain-Rust `types_parsenodes`
// node universe (`DefElem` carries `types_parsenodes::Node` args).
// ===========================================================================

/// `makeDefElem(name, arg, location)` (makefuncs.c) — a `DefElem` for the
/// typical case (unqualified option name, no special action). `defnamespace`
/// NULL, `defaction = DEFELEM_UNSPEC`.
pub fn make_def_elem(name: String, arg: Option<ParseNode>, location: i32) -> DefElem {
    DefElem {
        defnamespace: None,
        defname: Some(name),
        arg: arg.map(Box::new),
        defaction: DEFELEM_UNSPEC,
        location,
    }
}

/// `makeDefElemExtended(nameSpace, name, arg, defaction, location)`
/// (makefuncs.c) — a `DefElem` with all fields available to be specified.
pub fn make_def_elem_extended(
    name_space: Option<String>,
    name: String,
    arg: Option<ParseNode>,
    defaction: DefElemAction,
    location: i32,
) -> DefElem {
    DefElem {
        defnamespace: name_space,
        defname: Some(name),
        arg: arg.map(Box::new),
        defaction,
        location,
    }
}

// ===========================================================================
// `PG_DETOAST_DATUM` — delegated to the `detoast` owner.
// ===========================================================================

/// `PointerGetDatum(PG_DETOAST_DATUM(d))` (fmgr.h) — return a fully
/// fetched-and-decompressed copy of the varlena datum `d` in `mcx`, or `d`
/// unchanged when it is already a plain (4-byte-header, uncompressed) varlena.
///
/// Mirrors the rangetypes precedent: inspect the varlena header to decide
/// whether a detoast is required, and when it is, delegate the actual
/// fetch/decompress to the `backend-access-common-detoast` owner's
/// `detoast_attr` seam, re-pointing the datum at the new `mcx` buffer.
fn pg_detoast_datum<'mcx>(mcx: Mcx<'mcx>, d: ScalarWord) -> PgResult<ScalarWord> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller guarantees `d` is a (non-null) varlena pointer datum
    // (`constlen == -1 && !constisnull`).
    unsafe {
        if varatt_is_external(p) {
            let len = varsize_external(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast_seam::detoast_attr::call(mcx, bytes)?;
            Ok(ScalarWord::from_usize(copy.leak().as_ptr() as usize))
        } else if !varatt_is_4b_u(p) && !varatt_is_1b(p) {
            // 4-byte compressed: the only remaining "extended" form. PG_DETOAST
            // _DATUM (unlike the _PACKED variant) also decompresses, which
            // detoast_attr does.
            let len = varsize_4b(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast_seam::detoast_attr::call(mcx, bytes)?;
            Ok(ScalarWord::from_usize(copy.leak().as_ptr() as usize))
        } else {
            // Plain 4B or short 1B header: returned unchanged.
            Ok(d)
        }
    }
}

// varatt.h header predicates (little-endian; `VARATT_IS_*`). These are the
// standard varlena-header bit tests — Datum/varlena vocabulary, not detoast
// logic (the fetch/decompress is the detoast owner's). Mirrors the rangetypes
// port's inline helpers.

#[inline]
unsafe fn varatt_is_4b_u(ptr: *const u8) -> bool {
    // VARATT_IS_4B_U(PTR): ((PTR)->va_header & 0x03) == 0x00
    (*ptr & 0x03) == 0x00
}

#[inline]
unsafe fn varatt_is_1b(ptr: *const u8) -> bool {
    // VARATT_IS_1B(PTR): ((PTR)->va_header & 0x01) == 0x01
    (*ptr & 0x01) == 0x01
}

#[inline]
unsafe fn varatt_is_1b_e(ptr: *const u8) -> bool {
    // VARATT_IS_1B_E(PTR): ((PTR)->va_header) == 0x01
    *ptr == 0x01
}

#[inline]
unsafe fn varatt_is_external(ptr: *const u8) -> bool {
    // VARATT_IS_EXTERNAL(PTR): VARATT_IS_1B_E(PTR)
    varatt_is_1b_e(ptr)
}

#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    // VARSIZE_4B(PTR): (((varattrib_4b *)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF
    let header = (ptr as *const u32).read_unaligned();
    ((header >> 2) & 0x3FFF_FFFF) as usize
}

#[inline]
unsafe fn varsize_external(ptr: *const u8) -> usize {
    // VARHDRSZ_EXTERNAL (== 2) + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)).
    const VARHDRSZ_EXTERNAL: usize = 2;
    let tag = *ptr.add(1);
    let payload = match tag {
        1 => 16usize,      // VARTAG_INDIRECT
        2 | 3 => 16usize,  // VARTAG_EXPANDED_RO / _RW
        18 => 18usize,     // VARTAG_ONDISK
        other => other as usize,
    };
    VARHDRSZ_EXTERNAL + payload
}

// ===========================================================================
// Seam implementations (owned canonical seams).
// ===========================================================================

/// `make_const_node` seam — `makeConst(...)` returned through a `Node *`-typed
/// API (C: `get_typdefault` builds the literal default and returns it as
/// `Node *`). Builds the `Const` (with detoast where applicable) and wraps it
/// as [`Node::Expr`], allocated in `mcx`.
pub fn make_const_node_seam<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i32,
    constvalue: Datum<'mcx>,
    constisnull: bool,
    constbyval: bool,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // The seam carries the canonical unified value, threaded straight into
    // `make_const` (which mirrors C's `Const.constvalue` Datum word for the
    // by-value arm and the by-reference image for `ByRef`).
    let c = make_const(
        mcx,
        consttype,
        consttypmod,
        constcollid,
        constlen,
        constvalue,
        constisnull,
        constbyval,
    )?;
    alloc_in(mcx, Node::mk_expr(mcx, Expr::Const(c))?)
}

/// `make_and_boolexpr` seam — `makeBoolExpr(AND_EXPR, args, location)` returned
/// through a `Node *`-typed API (C: partition-qual assembly). Builds the
/// `BoolExpr` over the `mcx`-charged `args` list and wraps it as
/// [`Node::Expr`], allocated in `mcx`.
pub fn make_and_boolexpr_seam<'mcx>(
    mcx: Mcx<'mcx>,
    args: PgVec<'mcx, Node<'mcx>>,
    location: i32,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // The arg list crosses as `Node`s (the partition-qual elements). Each is a
    // `Node::Expr` (the qual clauses generate_partition_qual produced); unwrap
    // to the underlying `Expr` to populate the BoolExpr's `Vec<Expr>` args.
    let mut exprs: Vec<Expr<'mcx>> = Vec::with_capacity(args.len());
    for n in args.into_iter() {
        let tag = n.tag();
        match n.into_expr() {
            Some(e) => exprs.push(e),
            // A non-expression `Node` in a boolean-AND arg list is a
            // model-impossible state (the C args are all `Expr *`).
            None => panic!(
                "make_and_boolexpr: AND argument is a non-expression node (tag {tag})"
            ),
        }
    }
    let e = make_bool_expr(AND_EXPR, exprs, location);
    alloc_in(mcx, Node::mk_expr(mcx, e)?)
}

/// `make_type_name_from_name_list` seam — build a raw-parser `TypeName` from a
/// `List` of `String` value nodes. Owned plain-Rust (not `mcx`-allocated).
pub fn make_type_name_from_name_list_seam(names: Vec<ParseNode>) -> PgResult<TypeName> {
    Ok(make_type_name_from_name_list(names))
}
