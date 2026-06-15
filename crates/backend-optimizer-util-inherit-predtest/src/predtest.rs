//! `backend/optimizer/util/predtest.c` — predicate implication/refutation
//! proof engine.
//!
//! Faithful 1:1 port of PostgreSQL 18.3 `predtest.c`. The planner expression
//! nodes are the owned [`Expr`](types_nodes::primnodes::Expr) enum, so
//! `IsA(node, X)`/`nodeTag(node)` become `as_*`/`is_*` accessors, `equal()`
//! becomes [`PgNodeEqual::equal_node`], `List *` becomes `&[Expr]`/`Vec<Expr>`,
//! and the AND/OR component iterators (the C `PredIterInfo` startup/next/cleanup
//! trio) materialise their component set as an owned `Vec<Expr>`.
//!
//! The genuinely-external crossings funnel through seam crates: lsyscache
//! (operator/function catalog probes), the proof cache's `pg_amop` inval +
//! the executor-backed constant test (`inherit-predtest-seams`), and the SAOP
//! array deconstruction (`inherit-predtest-seams`, owner arrayfuncs). The
//! btree-proof lookaside cache itself is genuine in-module per-backend state
//! (`thread_local!` `HashMap`, the C `static HTAB *OprProofCacheHash`).

use alloc::vec::Vec;
use std::cell::RefCell;
use std::collections::HashMap;

use backend_nodes_equalfuncs_seams::equal_expr;
use mcx::Mcx;

use backend_optimizer_util_inherit_predtest_seams as own_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs;
use backend_optimizer_util_pathnode_seams as pathnode;

use types_core::primitive::{InvalidOid, Oid};
use types_nodes::primnodes::{
    BoolExprType, BoolTestType, CompareType, Const, Expr, NullTestType, OpExpr,
};
use types_pathnodes::{NodeId, PlannerInfo};
use types_error::{PgError, PgResult};

/*
 * Proof attempts involving large arrays in ScalarArrayOpExpr nodes are
 * likely to require O(N^2) time, and more often than not fail anyway.
 * So we set an arbitrary limit on the number of array elements that
 * we will allow to be treated as an AND or OR clause.
 */
const MAX_SAOP_ARRAY_SIZE: i32 = 100;

/// `BooleanEqualOperator` — `pg_operator.h` OID of `boolean = boolean`.
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;

/// `BOOLOID` — `pg_type.h` OID of `bool`.
const BOOLOID: Oid = 16;

/// `PROVOLATILE_IMMUTABLE` — `pg_proc.h` constant for an immutable function.
const PROVOLATILE_IMMUTABLE: u8 = b'i';

/// `AMOPOPID` — syscache id whose invalidation flushes the proof cache.
const AMOPOPID: types_syscache::syscache_ids::SysCacheIdentifier =
    types_syscache::syscache_ids::AMOPOPID;

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// Out-of-memory `PgError` for the owned `Vec` materialisation of AND/OR
/// component sets (their C analogue pallocs the iterator state / list_copy).
fn oom(what: &str) -> PgError {
    PgError::error(alloc::format!(
        "backend-optimizer-util-inherit-predtest: out of memory ({what})"
    ))
}

/*
 * CLASS_ATOM / CLASS_AND / CLASS_OR — see C's PredClass enum.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
enum PredClass {
    /// expression that's not AND or OR
    Atom,
    /// expression with AND semantics
    And,
    /// expression with OR semantics
    Or,
}

/*
 * To avoid redundant coding in predicate_implied_by_recurse and
 * predicate_refuted_by_recurse, we abstract out iterating over the components
 * of an expression that is logically an AND or OR structure (the C
 * `PredIterInfoData` struct + its startup/next/cleanup function-pointer trio).
 *
 * In the owned-tree port we do not keep a live cursor: `predicate_classify`
 * records *which* component-set the node has, and `components()` materialises
 * that set as an owned `Vec<Expr>` (the analogue of C's per-loop startup()).
 */
#[derive(Clone, Copy)]
enum PredIterKind {
    /// Atom — never iterated.
    Atom,
    /// `is_andclause`/`is_orclause` BoolExpr (and a top-level implicit-AND
    /// `List`, which is passed to the proof engine as an AND list): iterate args.
    List,
    /// constant-array `ScalarArrayOpExpr`: build a dummy `OpExpr` per element.
    ArrayConst,
    /// `ArrayExpr`-array `ScalarArrayOpExpr`: build a dummy `OpExpr` per element.
    ArrayExpr,
}

struct PredIterInfo {
    kind: PredIterKind,
    /// For the `List` kind, the BoolExpr/list args to iterate; for the SAOP
    /// kinds, unused (the components are rebuilt from the clause).
    list: Vec<Expr>,
}

impl PredIterInfo {
    fn atom() -> Self {
        PredIterInfo {
            kind: PredIterKind::Atom,
            list: Vec::new(),
        }
    }

    /// Materialise the component nodes of `clause` per the recorded `kind`.
    fn components<'mcx>(&self, mcx: Mcx<'mcx>, clause: &Expr) -> PgResult<Vec<Expr>> {
        match self.kind {
            PredIterKind::Atom => Ok(Vec::new()),
            PredIterKind::List => Ok(self.list.clone()),
            PredIterKind::ArrayConst => arrayconst_components(mcx, clause),
            PredIterKind::ArrayExpr => arrayexpr_components(clause),
        }
    }
}

/*
 * Build the dummy per-element OpExprs for a constant-array ScalarArrayOpExpr.
 *
 * The C `arrayconst_startup_fn` deconstructs the constant array into per-element
 * Datums; `arrayconst_next_fn` yields a dummy `OpExpr` of the form
 * `scalar saop_op element_const` for each element.  We build the whole list up
 * front, exactly as the C iterator would have produced them in order.
 */
fn arrayconst_components<'mcx>(mcx: Mcx<'mcx>, saop_node: &Expr) -> PgResult<Vec<Expr>> {
    let s = match saop_node.as_scalararrayopexpr() {
        Some(s) => s,
        _ => return Ok(Vec::new()),
    };
    /* args = (scalar, arraynode) */
    let scalar = &s.args[0];
    let arrayconst = match s.args[1].as_const() {
        Some(c) => c,
        _ => return Ok(Vec::new()),
    };

    /*
     * Deconstruct the array literal (arrayconst_startup_fn):
     *   arrayval = DatumGetArrayTypeP(arrayconst->constvalue);
     *   get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &elmlen, &elmbyval,
     *                        &elmalign);
     *   deconstruct_array(arrayval, ARR_ELEMTYPE, elmlen, elmbyval, elmalign,
     *                     &elem_values, &elem_nulls, &num_elems);
     */
    let arrdatum = const_value_bare(arrayconst);
    let elemtype = arrayfuncs::array_get_elemtype::call(mcx, arrdatum)?;
    let lbva = lsyscache::get_typlenbyvalalign::call(elemtype)?;
    let elems = arrayfuncs::deconstruct_array::call(
        mcx,
        arrdatum,
        elemtype,
        lbva.typlen,
        lbva.typbyval,
        lbva.typalign as core::ffi::c_char,
    )?;

    let mut out = Vec::new();
    out.try_reserve(elems.len())
        .map_err(|_| oom("arrayconst components"))?;
    for (value, isnull) in elems.iter() {
        let elem_const = make_dummy_const(arrayconst, elemtype, lbva, *value, *isnull);
        out.push(make_dummy_saop_opexpr(s, scalar.clone(), elem_const));
    }
    Ok(out)
}

/*
 * Build the dummy per-element OpExprs for an ArrayExpr-array ScalarArrayOpExpr.
 *
 * The C `arrayexpr_*_fn` yields, for each element of the `ArrayExpr`, a dummy
 * `OpExpr` of the form `scalar saop_op element`.
 */
fn arrayexpr_components(saop_node: &Expr) -> PgResult<Vec<Expr>> {
    let s = match saop_node.as_scalararrayopexpr() {
        Some(s) => s,
        _ => return Ok(Vec::new()),
    };
    let scalar = &s.args[0];
    let elements: &[Expr] = match s.args[1].as_arrayexpr() {
        Some(a) => &a.elements,
        _ => return Ok(Vec::new()),
    };

    let mut out = Vec::new();
    out.try_reserve(elements.len())
        .map_err(|_| oom("arrayexpr components"))?;
    for elem in elements {
        out.push(make_dummy_saop_opexpr(s, scalar.clone(), elem.clone()));
    }
    Ok(out)
}

/// Build the dummy per-element `Const` carrying one deconstructed array element
/// value (the C `state->const_expr`, stamped in arrayconst_startup_fn):
///   const_expr.consttype = ARR_ELEMTYPE(arrayval);
///   const_expr.consttypmod = -1;
///   const_expr.constcollid = arrayconst->constcollid;
///   const_expr.constlen = elmlen;
///   const_expr.constbyval = elmbyval;
///   const_expr.constvalue = elem_values[i];
///   const_expr.constisnull = elem_nulls[i];
fn make_dummy_const(
    arrayconst: &Const,
    elemtype: Oid,
    _lbva: lsyscache::TypLenByValAlign,
    value: types_datum::datum::Datum,
    isnull: bool,
) -> Expr {
    /*
     * The per-element value comes back as a bare machine word from
     * deconstruct_array; carry it as the canonical Datum's by-value arm (the
     * same `as_usize`/`from_usize` word lane clauses.c uses for folded array
     * Const values).  The `constlen`/`constbyval` decorations the C stamps
     * are not fields of this repo's trimmed `Const` (only the downstream
     * `eval_const_test` re-derives storage from `test_op`).
     */
    Expr::Const(Const {
        consttype: elemtype,
        consttypmod: -1,
        constcollid: arrayconst.constcollid,
        constvalue: types_tuple::backend_access_common_heaptuple::Datum::from_usize(value.as_usize()),
        constisnull: isnull,
        // makeConst sets location = -1.
        location: -1,
    })
}

/// `DatumGetArrayTypeP(const->constvalue)` argument: the array Const's value as
/// the bare machine word the arrayfuncs seams take (mirrors clauses.c's
/// `const_value_bare`; the `as_usize` word lane for folded array Consts).
#[inline]
fn const_value_bare(c: &Const) -> types_datum::datum::Datum {
    types_datum::datum::Datum::from_usize(c.constvalue.as_usize())
}

/// Build the dummy `scalar saop_op elem` `OpExpr` (mirrors the reusable `OpExpr`
/// the C SAOP iterators stamp `leftop`/`rightop` into).
fn make_dummy_saop_opexpr(
    s: &types_nodes::primnodes::ScalarArrayOpExpr,
    leftop: Expr,
    rightop: Expr,
) -> Expr {
    Expr::OpExpr(OpExpr {
        opno: s.opno,
        opfuncid: s.opfuncid,
        opresulttype: BOOLOID,
        opretset: false,
        opcollid: InvalidOid,
        inputcollid: s.inputcollid,
        args: alloc::vec![leftop, rightop],
        location: -1,
    })
}

/* ====================================================================
 * Node-shape classification (IsA + nodeTag) over the owned enum.
 * ================================================================== */

#[inline]
fn is_andclause(n: &Expr) -> bool {
    n.as_boolexpr().is_some_and(|b| b.boolop == BoolExprType::AND_EXPR)
}
#[inline]
fn is_orclause(n: &Expr) -> bool {
    n.as_boolexpr().is_some_and(|b| b.boolop == BoolExprType::OR_EXPR)
}
#[inline]
fn is_notclause(n: &Expr) -> bool {
    n.as_boolexpr().is_some_and(|b| b.boolop == BoolExprType::NOT_EXPR)
}
#[inline]
fn is_opclause(n: &Expr) -> bool {
    n.as_opexpr().is_some()
}
#[inline]
fn is_funcclause(n: &Expr) -> bool {
    n.as_funcexpr().is_some()
}

/// `get_notclausearg(notclause)` — `linitial(((BoolExpr *) notclause)->args)`.
#[inline]
fn get_notclausearg(notclause: &Expr) -> Option<&Expr> {
    notclause.as_boolexpr().and_then(|b| b.args.first())
}

/// `DatumGetBool(((Const *) node)->constvalue)`.
#[inline]
fn const_value_bool(c: &Const) -> bool {
    c.constvalue.as_bool()
}

/*
 * predicate_implied_by
 *	  Recursively checks whether the clauses in clause_list imply that the
 *	  given predicate is true.
 *
 * The seam-facing entry: clause/predicate lists are arena handles
 * (`RestrictInfo.clause`/`indpred`-style `NodeId`), resolved through `root`.
 * The C contract is infallible (`bool`); the catalog-lookup leg's
 * `ereport(ERROR)` becomes a panic here (matching the established
 * `get_mergejoin_opfamilies`-style transient-context boundary in pathkeys).
 */
pub fn predicate_implied_by(
    root: &PlannerInfo,
    predicate_list: &[NodeId],
    clause_list: &[NodeId],
    weak: bool,
) -> bool {
    let predicate = resolve_nodes(root, predicate_list);
    let clause = resolve_nodes(root, clause_list);
    let cx = mcx::MemoryContext::new("predicate_implied_by transient");
    predicate_implied_by_impl(cx.mcx(), &predicate, &clause, weak)
        .unwrap_or_else(|e| panic!("predicate_implied_by: {e:?}"))
}

/// Resolve a list of arena handles to owned `Expr` values (the proof engine
/// reads them; cloning matches the consumer, which already clones `indpred`).
fn resolve_nodes(root: &PlannerInfo, ids: &[NodeId]) -> Vec<Expr> {
    ids.iter().map(|&id| root.node(id).clone()).collect()
}

/// `predicate_implied_by` over already-resolved owned `Expr` lists, fallible
/// and `Mcx`-threaded (the catalog lookups allocate).
pub(crate) fn predicate_implied_by_impl<'mcx>(
    mcx: Mcx<'mcx>,
    predicate_list: &[Expr],
    clause_list: &[Expr],
    weak: bool,
) -> PgResult<bool> {
    if predicate_list.is_empty() {
        return Ok(true); /* no predicate: implication is vacuous */
    }
    if clause_list.is_empty() {
        return Ok(false); /* no restriction: implication must fail */
    }

    /*
     * If either input is a single-element list, replace it with its lone
     * member; this avoids one useless level of AND-recursion.
     */
    let p_holder: Expr;
    let p: &Expr = if predicate_list.len() == 1 {
        &predicate_list[0]
    } else {
        p_holder = wrap_list(predicate_list)?;
        &p_holder
    };
    let c_holder: Expr;
    let c: &Expr = if clause_list.len() == 1 {
        &clause_list[0]
    } else {
        c_holder = wrap_list(clause_list)?;
        &c_holder
    };

    /* And away we go ... */
    predicate_implied_by_recurse(mcx, c, p, weak)
}

/*
 * predicate_refuted_by
 *	  Recursively checks whether the clauses in clause_list refute the given
 *	  predicate (that is, prove it false).
 *
 * This is NOT the same as !(predicate_implied_by).  See predtest.c.  The
 * seam-facing entry mirrors `predicate_implied_by`'s arena-handle + infallible
 * contract.
 */
pub fn predicate_refuted_by(
    root: &PlannerInfo,
    predicate_list: &[NodeId],
    clause_list: &[NodeId],
    weak: bool,
) -> bool {
    let predicate = resolve_nodes(root, predicate_list);
    let clause = resolve_nodes(root, clause_list);
    let cx = mcx::MemoryContext::new("predicate_refuted_by transient");
    predicate_refuted_by_impl(cx.mcx(), &predicate, &clause, weak)
        .unwrap_or_else(|e| panic!("predicate_refuted_by: {e:?}"))
}

/// `predicate_refuted_by` over already-resolved owned `Expr` lists.
pub(crate) fn predicate_refuted_by_impl<'mcx>(
    mcx: Mcx<'mcx>,
    predicate_list: &[Expr],
    clause_list: &[Expr],
    weak: bool,
) -> PgResult<bool> {
    if predicate_list.is_empty() {
        return Ok(false); /* no predicate: no refutation is possible */
    }
    if clause_list.is_empty() {
        return Ok(false); /* no restriction: refutation must fail */
    }

    let p_holder: Expr;
    let p: &Expr = if predicate_list.len() == 1 {
        &predicate_list[0]
    } else {
        p_holder = wrap_list(predicate_list)?;
        &p_holder
    };
    let c_holder: Expr;
    let c: &Expr = if clause_list.len() == 1 {
        &clause_list[0]
    } else {
        c_holder = wrap_list(clause_list)?;
        &c_holder
    };

    /* And away we go ... */
    predicate_refuted_by_recurse(mcx, c, p, weak)
}

/// Re-wrap a multi-element implicit-AND clause list as an AND `BoolExpr`.
///
/// In C, the multi-element `List *` is passed as the `Node *`, and
/// `predicate_classify` treats a bare `List` as an implicit-AND.  An
/// implicit-AND list and an explicit AND `BoolExpr` classify identically and
/// iterate the same component set, so we model the list as an AND `BoolExpr` —
/// observationally identical for the proof engine.
fn wrap_list(items: &[Expr]) -> PgResult<Expr> {
    let mut args = Vec::new();
    args.try_reserve(items.len()).map_err(|_| oom("wrap_list"))?;
    for e in items {
        args.push(e.clone());
    }
    Ok(Expr::BoolExpr(types_nodes::primnodes::BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args,
        // make_andclause sets location = -1.
        location: -1,
    }))
}

/*----------
 * predicate_implied_by_recurse
 *	  Does the predicate implication test for non-NULL restriction and
 *	  predicate clauses.  See predtest.c for the full implication-rule table.
 *
 * Note: in this repo's value model, clause lists are already-unwrapped `Expr`
 * values (a `RestrictInfo`'s `clause` is resolved at the call boundary, since
 * the owned `Expr` enum cannot itself hold a `RestrictInfo`), so the C
 * `IsA(clause, RestrictInfo)` skip is vacuous here and is not reproduced.
 *----------
 */
fn predicate_implied_by_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    clause: &Expr,
    predicate: &Expr,
    weak: bool,
) -> PgResult<bool> {
    let mut result: bool;

    let mut pred_info = PredIterInfo::atom();
    let pclass = predicate_classify(predicate, &mut pred_info)?;

    let mut clause_info = PredIterInfo::atom();
    match predicate_classify(clause, &mut clause_info)? {
        PredClass::And => match pclass {
            PredClass::And => {
                /* AND-clause => AND-clause if A implies each of B's items */
                result = true;
                for pitem in pred_info.components(mcx, predicate)? {
                    if !predicate_implied_by_recurse(mcx, clause, &pitem, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Or => {
                /*
                 * AND-clause => OR-clause if A implies any of B's items
                 *
                 * Needed to handle (x AND y) => ((x AND y) OR z)
                 */
                result = false;
                for pitem in pred_info.components(mcx, predicate)? {
                    if predicate_implied_by_recurse(mcx, clause, &pitem, weak)? {
                        result = true;
                        break;
                    }
                }
                if result {
                    return Ok(result);
                }

                /*
                 * Also check if any of A's items implies B
                 *
                 * Needed to handle ((x OR y) AND z) => (x OR y)
                 */
                for citem in clause_info.components(mcx, clause)? {
                    if predicate_implied_by_recurse(mcx, &citem, predicate, weak)? {
                        result = true;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Atom => {
                /* AND-clause => atom if any of A's items implies B */
                result = false;
                for citem in clause_info.components(mcx, clause)? {
                    if predicate_implied_by_recurse(mcx, &citem, predicate, weak)? {
                        result = true;
                        break;
                    }
                }
                Ok(result)
            }
        },

        PredClass::Or => match pclass {
            PredClass::Or => {
                /*
                 * OR-clause => OR-clause if each of A's items implies any
                 * of B's items.  Messy but can't do it any more simply.
                 */
                result = true;
                let pred_components = pred_info.components(mcx, predicate)?;
                'outer: for citem in clause_info.components(mcx, clause)? {
                    let mut presult = false;
                    for pitem in &pred_components {
                        if predicate_implied_by_recurse(mcx, &citem, pitem, weak)? {
                            presult = true;
                            break;
                        }
                    }
                    if !presult {
                        result = false; /* doesn't imply any of B's */
                        break 'outer;
                    }
                }
                Ok(result)
            }
            PredClass::And | PredClass::Atom => {
                /*
                 * OR-clause => AND-clause if each of A's items implies B
                 * OR-clause => atom if each of A's items implies B
                 */
                result = true;
                for citem in clause_info.components(mcx, clause)? {
                    if !predicate_implied_by_recurse(mcx, &citem, predicate, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
        },

        PredClass::Atom => match pclass {
            PredClass::And => {
                /* atom => AND-clause if A implies each of B's items */
                result = true;
                for pitem in pred_info.components(mcx, predicate)? {
                    if !predicate_implied_by_recurse(mcx, clause, &pitem, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Or => {
                /* atom => OR-clause if A implies any of B's items */
                result = false;
                for pitem in pred_info.components(mcx, predicate)? {
                    if predicate_implied_by_recurse(mcx, clause, &pitem, weak)? {
                        result = true;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Atom => {
                /* atom => atom is the base case */
                predicate_implied_by_simple_clause(mcx, predicate, clause, weak)
            }
        },
    }
}

/*----------
 * predicate_refuted_by_recurse
 *	  Does the predicate refutation test for non-NULL restriction and
 *	  predicate clauses.  See predtest.c for the full refutation-rule table and
 *	  the NOT-clause rules.
 *----------
 */
fn predicate_refuted_by_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    clause: &Expr,
    predicate: &Expr,
    weak: bool,
) -> PgResult<bool> {
    let mut not_arg: Option<&Expr>;
    let mut result: bool;

    let mut pred_info = PredIterInfo::atom();
    let pclass = predicate_classify(predicate, &mut pred_info)?;

    let mut clause_info = PredIterInfo::atom();
    match predicate_classify(clause, &mut clause_info)? {
        PredClass::And => match pclass {
            PredClass::And => {
                /*
                 * AND-clause R=> AND-clause if A refutes any of B's items
                 *
                 * Needed to handle (x AND y) R=> ((!x OR !y) AND z)
                 */
                result = false;
                for pitem in pred_info.components(mcx, predicate)? {
                    if predicate_refuted_by_recurse(mcx, clause, &pitem, weak)? {
                        result = true;
                        break;
                    }
                }
                if result {
                    return Ok(result);
                }

                /*
                 * Also check if any of A's items refutes B
                 *
                 * Needed to handle ((x OR y) AND z) R=> (!x AND !y)
                 */
                for citem in clause_info.components(mcx, clause)? {
                    if predicate_refuted_by_recurse(mcx, &citem, predicate, weak)? {
                        result = true;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Or => {
                /* AND-clause R=> OR-clause if A refutes each of B's items */
                result = true;
                for pitem in pred_info.components(mcx, predicate)? {
                    if !predicate_refuted_by_recurse(mcx, clause, &pitem, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::Atom => {
                /*
                 * If B is a NOT-type clause, A R=> B if A => B's arg
                 * (strong implication test in all cases).
                 */
                not_arg = extract_not_arg(predicate);
                if let Some(na) = not_arg {
                    if predicate_implied_by_recurse(mcx, clause, na, false)? {
                        return Ok(true);
                    }
                }

                /* AND-clause R=> atom if any of A's items refutes B */
                result = false;
                for citem in clause_info.components(mcx, clause)? {
                    if predicate_refuted_by_recurse(mcx, &citem, predicate, weak)? {
                        result = true;
                        break;
                    }
                }
                Ok(result)
            }
        },

        PredClass::Or => match pclass {
            PredClass::Or => {
                /* OR-clause R=> OR-clause if A refutes each of B's items */
                result = true;
                for pitem in pred_info.components(mcx, predicate)? {
                    if !predicate_refuted_by_recurse(mcx, clause, &pitem, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
            PredClass::And => {
                /*
                 * OR-clause R=> AND-clause if each of A's items refutes
                 * any of B's items.
                 */
                result = true;
                let pred_components = pred_info.components(mcx, predicate)?;
                'outer: for citem in clause_info.components(mcx, clause)? {
                    let mut presult = false;
                    for pitem in &pred_components {
                        if predicate_refuted_by_recurse(mcx, &citem, pitem, weak)? {
                            presult = true;
                            break;
                        }
                    }
                    if !presult {
                        result = false; /* citem refutes nothing */
                        break 'outer;
                    }
                }
                Ok(result)
            }
            PredClass::Atom => {
                /*
                 * If B is a NOT-type clause, A R=> B if A => B's arg
                 * (same logic as for the AND-clause case above).
                 */
                not_arg = extract_not_arg(predicate);
                if let Some(na) = not_arg {
                    if predicate_implied_by_recurse(mcx, clause, na, false)? {
                        return Ok(true);
                    }
                }

                /* OR-clause R=> atom if each of A's items refutes B */
                result = true;
                for citem in clause_info.components(mcx, clause)? {
                    if !predicate_refuted_by_recurse(mcx, &citem, predicate, weak)? {
                        result = false;
                        break;
                    }
                }
                Ok(result)
            }
        },

        PredClass::Atom => {
            /*
             * If A is a strong NOT-clause, A R=> B if B => A's arg
             * (see predtest.c for the strong/weak reasoning).
             */
            not_arg = extract_strong_not_arg(clause);
            if let Some(na) = not_arg {
                if predicate_implied_by_recurse(mcx, predicate, na, !weak)? {
                    return Ok(true);
                }
            }

            match pclass {
                PredClass::And => {
                    /* atom R=> AND-clause if A refutes any of B's items */
                    result = false;
                    for pitem in pred_info.components(mcx, predicate)? {
                        if predicate_refuted_by_recurse(mcx, clause, &pitem, weak)? {
                            result = true;
                            break;
                        }
                    }
                    Ok(result)
                }
                PredClass::Or => {
                    /* atom R=> OR-clause if A refutes each of B's items */
                    result = true;
                    for pitem in pred_info.components(mcx, predicate)? {
                        if !predicate_refuted_by_recurse(mcx, clause, &pitem, weak)? {
                            result = false;
                            break;
                        }
                    }
                    Ok(result)
                }
                PredClass::Atom => {
                    /* If B is a NOT-type clause, A R=> B if A => B's arg */
                    not_arg = extract_not_arg(predicate);
                    if let Some(na) = not_arg {
                        if predicate_implied_by_recurse(mcx, clause, na, false)? {
                            return Ok(true);
                        }
                    }

                    /* atom R=> atom is the base case */
                    predicate_refuted_by_simple_clause(mcx, predicate, clause, weak)
                }
            }
        }
    }
}

/*
 * predicate_classify
 *	  Classify an expression node as AND-type, OR-type, or neither (an atom).
 *
 * Enforces MAX_SAOP_ARRAY_SIZE: an over-large ScalarArrayOpExpr is classified
 * as an atom (and passed as-is to the simple_clause functions).
 */
fn predicate_classify(clause: &Expr, info: &mut PredIterInfo) -> PgResult<PredClass> {
    /* Handle normal AND and OR boolean clauses */
    if is_andclause(clause) {
        info.kind = PredIterKind::List;
        info.list = clause.as_boolexpr().map(|b| b.args.clone()).unwrap_or_default();
        return Ok(PredClass::And);
    }
    if is_orclause(clause) {
        info.kind = PredIterKind::List;
        info.list = clause.as_boolexpr().map(|b| b.args.clone()).unwrap_or_default();
        return Ok(PredClass::Or);
    }

    /* Handle ScalarArrayOpExpr */
    if let Some(saop) = clause.as_scalararrayopexpr() {
        let arraynode: Option<&Expr> = saop.args.get(1);

        /*
         * We can break this down into an AND or OR structure, but only if we
         * know how to iterate through expressions for the array's elements.
         * We can do that if the array operand is a non-null constant or a
         * simple ArrayExpr.
         */
        if let Some(arraynode) = arraynode {
            if let Some(arrayconst) = arraynode.as_const() {
                if !arrayconst.constisnull {
                    let nelems =
                        arrayfuncs::array_const_nitems::call(const_value_bare(arrayconst))?;
                    if nelems <= MAX_SAOP_ARRAY_SIZE {
                        info.kind = PredIterKind::ArrayConst;
                        return Ok(if saop.useOr {
                            PredClass::Or
                        } else {
                            PredClass::And
                        });
                    }
                }
            } else if let Some(arrayexpr) = arraynode.as_arrayexpr() {
                if !arrayexpr.multidims
                    && (arrayexpr.elements.len() as i32) <= MAX_SAOP_ARRAY_SIZE
                {
                    info.kind = PredIterKind::ArrayExpr;
                    return Ok(if saop.useOr {
                        PredClass::Or
                    } else {
                        PredClass::And
                    });
                }
            }
        }
    }

    /* None of the above, so it's an atom */
    Ok(PredClass::Atom)
}

/*
 * predicate_implied_by_simple_clause
 *	  Does the predicate implication test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 */
fn predicate_implied_by_simple_clause<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: &Expr,
    clause: &Expr,
    weak: bool,
) -> PgResult<bool> {
    /* Allow interrupting long proof attempts */
    pathnode::check_for_interrupts::call();

    /*
     * A simple and general rule is that a clause implies itself, hence we
     * check if they are equal(); this works for any kind of expression.
     */
    if equal_expr::call(predicate, clause) {
        return Ok(true);
    }

    /* Next we have some clause-type-specific strategies */
    if let Some(op) = clause.as_opexpr() {
        /*----------
         * For boolean x, "x = TRUE" is equivalent to "x", likewise
         * "x = FALSE" is equivalent to "NOT x".
         *----------
         */
        if op.opno == BOOLEAN_EQUAL_OPERATOR {
            debug_assert!(op.args.len() == 2);
            let rightop = &op.args[1];
            /* We might never see null Consts here, but better check */
            if let Some(rc) = rightop.as_const() {
                if !rc.constisnull {
                    let leftop = &op.args[0];
                    if const_value_bool(rc) {
                        /* X = true implies X */
                        if equal_expr::call(predicate, leftop) {
                            return Ok(true);
                        }
                    } else {
                        /* X = false implies NOT X */
                        if is_notclause(predicate) {
                            if let Some(arg) = get_notclausearg(predicate) {
                                if equal_expr::call(arg, leftop) {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /* ... and some predicate-type-specific ones */
    if let Some(predntest) = predicate.as_nulltest() {
        if predntest.nulltesttype == NullTestType::IS_NOT_NULL {
            /*
             * If the predicate is of the form "foo IS NOT NULL", and we are
             * considering strong implication, we can conclude that the
             * predicate is implied if the clause is strict for "foo".
             */
            if !weak && !predntest.argisrow {
                if let Some(arg) = predntest.arg.as_deref() {
                    if clause_is_strict_for(clause, arg, true)? {
                        return Ok(true);
                    }
                }
            }
        }
        /* IS_NULL: break */
    }

    /*
     * Finally, if both clauses are binary operator expressions, we may be able
     * to prove something using operator_predicate_proof().
     */
    operator_predicate_proof(mcx, predicate, clause, false, weak)
}

/*
 * predicate_refuted_by_simple_clause
 *	  Does the predicate refutation test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 *
 * `clause_ptr_eq` mirrors the C `(Node *) predicate == clause` pointer test:
 * `relation_excluded_by_constraints` can call here with predicate and clause
 * the same node.  In the owned-tree model the two sides come in as separate
 * borrows; the C self-contradiction fast path is reproduced as a structural
 * pointer-identity check on the borrows.
 */
fn predicate_refuted_by_simple_clause<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: &Expr,
    clause: &Expr,
    weak: bool,
) -> PgResult<bool> {
    /* Allow interrupting long proof attempts */
    pathnode::check_for_interrupts::call();

    /*
     * A simple clause can't refute itself.  But
     * relation_excluded_by_constraints() may pass pointer-equal predicate and
     * clause, worth eliminating quickly.
     */
    if core::ptr::eq(predicate, clause) {
        return Ok(false);
    }

    /* Next we have some clause-type-specific strategies */
    if let Some(clausentest) = clause.as_nulltest() {
        /* row IS NULL does not act in the simple way we have in mind */
        if clausentest.argisrow {
            return Ok(false);
        }

        if clausentest.nulltesttype == NullTestType::IS_NULL {
            if let Some(predntest) = predicate.as_nulltest() {
                /* row IS NULL does not act in the simple way we have in mind */
                if predntest.argisrow {
                    return Ok(false);
                }

                /*
                 * foo IS NULL refutes foo IS NOT NULL, at least in the non-row
                 * case, for both strong and weak refutation
                 */
                if predntest.nulltesttype == NullTestType::IS_NOT_NULL
                    && equal_opt(predntest.arg.as_deref(), clausentest.arg.as_deref())
                {
                    return Ok(true);
                }
            }

            /*
             * foo IS NULL weakly refutes any predicate that is strict for foo.
             */
            if weak {
                if let Some(carg) = clausentest.arg.as_deref() {
                    if clause_is_strict_for(predicate, carg, true)? {
                        return Ok(true);
                    }
                }
            }

            return Ok(false); /* we can't succeed below... */
        }
        /* IS_NOT_NULL: break */
    }

    /* ... and some predicate-type-specific ones */
    if let Some(predntest) = predicate.as_nulltest() {
        /* row IS NULL does not act in the simple way we have in mind */
        if predntest.argisrow {
            return Ok(false);
        }

        if predntest.nulltesttype == NullTestType::IS_NULL {
            if let Some(clausentest) = clause.as_nulltest() {
                /* row IS NULL does not act in the simple way we have in mind */
                if clausentest.argisrow {
                    return Ok(false);
                }

                /*
                 * foo IS NOT NULL refutes foo IS NULL for both strong and weak
                 * refutation
                 */
                if clausentest.nulltesttype == NullTestType::IS_NOT_NULL
                    && equal_opt(clausentest.arg.as_deref(), predntest.arg.as_deref())
                {
                    return Ok(true);
                }
            }

            /*
             * When the predicate is of the form "foo IS NULL", we can conclude
             * that the predicate is refuted if the clause is strict for "foo".
             */
            if let Some(parg) = predntest.arg.as_deref() {
                if clause_is_strict_for(clause, parg, true)? {
                    return Ok(true);
                }
            }
        }
        /* IS_NOT_NULL: break */

        return Ok(false); /* we can't succeed below... */
    }

    /*
     * Finally, if both clauses are binary operator expressions, we may be able
     * to prove something using the system's knowledge about operators.
     */
    operator_predicate_proof(mcx, predicate, clause, true, weak)
}

/// `equal(a, b)` over `Option<&Expr>` operands (both NULL == true).
#[inline]
fn equal_opt(a: Option<&Expr>, b: Option<&Expr>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => equal_expr::call(a, b),
        (None, None) => true,
        _ => false,
    }
}

/*
 * If clause asserts the non-truth of a subclause, return that subclause;
 * otherwise return None.
 */
fn extract_not_arg(clause: &Expr) -> Option<&Expr> {
    if let Some(b) = clause.as_boolexpr() {
        if b.boolop == BoolExprType::NOT_EXPR {
            return b.args.first();
        }
    } else if let Some(bt) = clause.as_booleantest() {
        if bt.booltesttype == BoolTestType::IS_NOT_TRUE
            || bt.booltesttype == BoolTestType::IS_FALSE
            || bt.booltesttype == BoolTestType::IS_UNKNOWN
        {
            return bt.arg.as_deref();
        }
    }
    None
}

/*
 * If clause asserts the falsity of a subclause, return that subclause;
 * otherwise return None.
 */
fn extract_strong_not_arg(clause: &Expr) -> Option<&Expr> {
    if let Some(b) = clause.as_boolexpr() {
        if b.boolop == BoolExprType::NOT_EXPR {
            return b.args.first();
        }
    } else if let Some(bt) = clause.as_booleantest() {
        if bt.booltesttype == BoolTestType::IS_FALSE {
            return bt.arg.as_deref();
        }
    }
    None
}

/*
 * Can we prove that "clause" returns NULL (or FALSE) if "subexpr" is
 * assumed to yield NULL?  See predtest.c for the full set of proof rules and
 * the allow_false semantics.
 */
fn clause_is_strict_for(mut clause: &Expr, mut subexpr: &Expr, allow_false: bool) -> PgResult<bool> {
    /*
     * Look through any RelabelType nodes, so that we can match, say,
     * varcharcol with lower(varcharcol::text).  We should not see stacked
     * RelabelTypes here.
     */
    if let Some(r) = clause.as_relabeltype() {
        if let Some(a) = r.arg.as_deref() {
            clause = a;
        }
    }
    if let Some(r) = subexpr.as_relabeltype() {
        if let Some(a) = r.arg.as_deref() {
            subexpr = a;
        }
    }

    /* Base case */
    if equal_expr::call(clause, subexpr) {
        return Ok(true);
    }

    /*
     * If we have a strict operator or function, a NULL result is guaranteed if
     * any input is forced NULL by subexpr.
     */
    if is_opclause(clause) {
        let op = clause.as_opexpr().unwrap();
        if lsyscache::op_strict::call(op.opno)? {
            for arg in &op.args {
                if clause_is_strict_for(arg, subexpr, false)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }
    if is_funcclause(clause) {
        let f = clause.as_funcexpr().unwrap();
        if lsyscache::func_strict::call(f.funcid)? {
            for arg in &f.args {
                if clause_is_strict_for(arg, subexpr, false)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    /*
     * CoerceViaIO is strict; likewise ArrayCoerceExpr (for its array argument),
     * ConvertRowtypeExpr (at the row level), and CoerceToDomain.
     */
    if let Some(c) = clause.as_coerceviaio() {
        return match c.arg.as_deref() {
            Some(a) => clause_is_strict_for(a, subexpr, false),
            None => Ok(false),
        };
    }
    if let Some(c) = clause.as_arraycoerceexpr() {
        return match c.arg.as_deref() {
            Some(a) => clause_is_strict_for(a, subexpr, false),
            None => Ok(false),
        };
    }
    if let Some(c) = clause.as_convertrowtypeexpr() {
        return match c.arg.as_deref() {
            Some(a) => clause_is_strict_for(a, subexpr, false),
            None => Ok(false),
        };
    }
    if let Some(c) = clause.as_coercetodomain() {
        return match c.arg.as_deref() {
            Some(a) => clause_is_strict_for(a, subexpr, false),
            None => Ok(false),
        };
    }

    /*
     * ScalarArrayOpExpr is a special case.  Note that we'd only reach here with
     * a ScalarArrayOpExpr clause if we failed to deconstruct it into an AND or
     * OR tree (e.g. if it has too many array elements).
     */
    if let Some(saop) = clause.as_scalararrayopexpr() {
        let scalarnode: Option<&Expr> = saop.args.first();
        let arraynode: Option<&Expr> = saop.args.get(1);

        /*
         * If we can prove the scalar input to be null, and the operator is
         * strict, then the SAOP result has to be null --- unless the array is
         * empty.
         */
        let scalar_strict = match scalarnode {
            Some(s) => clause_is_strict_for(s, subexpr, false)?,
            None => false,
        };
        if scalar_strict && lsyscache::op_strict::call(saop.opno)? {
            let mut nelems: i32 = 0;

            if allow_false && saop.useOr {
                return Ok(true); /* can succeed even if array is empty */
            }

            if let Some(an) = arraynode {
                if let Some(arrayconst) = an.as_const() {
                    /* If array is constant NULL then we can succeed. */
                    if arrayconst.constisnull {
                        return Ok(true);
                    }
                    /* Otherwise, we can compute the number of elements. */
                    nelems = arrayfuncs::array_const_nitems::call(const_value_bare(arrayconst))?;
                } else if let Some(arrayexpr) = an.as_arrayexpr() {
                    /*
                     * We can also reliably count the number of array elements
                     * if the input is a non-multidim ARRAY[] expression.
                     */
                    if !arrayexpr.multidims {
                        nelems = arrayexpr.elements.len() as i32;
                    }
                }
            }

            /* Proof succeeds if array is definitely non-empty */
            if nelems > 0 {
                return Ok(true);
            }
        }

        /*
         * If we can prove the array input to be null, the proof succeeds in all
         * cases, since ScalarArrayOpExpr always returns NULL for a NULL array.
         */
        return match arraynode {
            Some(a) => clause_is_strict_for(a, subexpr, false),
            None => Ok(false),
        };
    }

    /*
     * When recursing into an expression, we might find a NULL constant.
     * That's certainly NULL, whether it matches subexpr or not.
     */
    if let Some(c) = clause.as_const() {
        return Ok(c.constisnull);
    }

    Ok(false)
}

/*
 * Define "operator implication tables" for index operators ("cmptypes"), and
 * similar tables for refutation.  See the long comment block in predtest.c for
 * the meaning of these tables.  cmptype numbers run 1..=6: LT LE EQ GE GT NE.
 */

const RCLT: i32 = CompareType::COMPARE_LT as i32;
const RCLE: i32 = CompareType::COMPARE_LE as i32;
const RCEQ: i32 = CompareType::COMPARE_EQ as i32;
const RCGE: i32 = CompareType::COMPARE_GE as i32;
const RCGT: i32 = CompareType::COMPARE_GT as i32;
const RCNE: i32 = CompareType::COMPARE_NE as i32;

/* We use "none" for 0/false to make the tables align nicely */
const NONE_B: bool = false;
const NONE_C: i32 = 0;

#[rustfmt::skip]
static RC_IMPLIES_TABLE: [[bool; 6]; 6] = [
/*	 LT     LE      EQ      GE      GT      NE  (the predicate operator) */
    [true,   true,   NONE_B, NONE_B, NONE_B, true  ], /* LT */
    [NONE_B, true,   NONE_B, NONE_B, NONE_B, NONE_B], /* LE */
    [NONE_B, true,   true,   true,   NONE_B, NONE_B], /* EQ */
    [NONE_B, NONE_B, NONE_B, true,   NONE_B, NONE_B], /* GE */
    [NONE_B, NONE_B, NONE_B, true,   true,   true  ], /* GT */
    [NONE_B, NONE_B, NONE_B, NONE_B, NONE_B, true  ], /* NE */
];

#[rustfmt::skip]
static RC_REFUTES_TABLE: [[bool; 6]; 6] = [
/*	 LT     LE      EQ      GE      GT      NE */
    [NONE_B, NONE_B, true,   true,   true,   NONE_B], /* LT */
    [NONE_B, NONE_B, NONE_B, NONE_B, true,   NONE_B], /* LE */
    [true,   NONE_B, NONE_B, NONE_B, true,   true  ], /* EQ */
    [true,   NONE_B, NONE_B, NONE_B, NONE_B, NONE_B], /* GE */
    [true,   true,   true,   NONE_B, NONE_B, NONE_B], /* GT */
    [NONE_B, NONE_B, true,   NONE_B, NONE_B, NONE_B], /* NE */
];

#[rustfmt::skip]
static RC_IMPLIC_TABLE: [[i32; 6]; 6] = [
/*	 LT     LE      EQ      GE      GT      NE */
    [RCGE,   RCGE,   NONE_C, NONE_C, NONE_C, RCGE  ], /* LT */
    [RCGT,   RCGE,   NONE_C, NONE_C, NONE_C, RCGT  ], /* LE */
    [RCGT,   RCGE,   RCEQ,   RCLE,   RCLT,   RCNE  ], /* EQ */
    [NONE_C, NONE_C, NONE_C, RCLE,   RCLT,   RCLT  ], /* GE */
    [NONE_C, NONE_C, NONE_C, RCLE,   RCLE,   RCLE  ], /* GT */
    [NONE_C, NONE_C, NONE_C, NONE_C, NONE_C, RCEQ  ], /* NE */
];

#[rustfmt::skip]
static RC_REFUTE_TABLE: [[i32; 6]; 6] = [
/*	 LT     LE      EQ      GE      GT      NE */
    [NONE_C, NONE_C, RCGE,   RCGE,   RCGE,   NONE_C], /* LT */
    [NONE_C, NONE_C, RCGT,   RCGT,   RCGE,   NONE_C], /* LE */
    [RCLE,   RCLT,   RCNE,   RCGT,   RCGE,   RCEQ  ], /* EQ */
    [RCLE,   RCLT,   RCLT,   NONE_C, NONE_C, NONE_C], /* GE */
    [RCLE,   RCLE,   RCLE,   NONE_C, NONE_C, NONE_C], /* GT */
    [NONE_C, NONE_C, RCEQ,   NONE_C, NONE_C, NONE_C], /* NE */
];

/*
 * operator_predicate_proof
 *	  Does the predicate implication or refutation test for a "simple clause"
 *	  predicate and a "simple clause" restriction, when both are operator
 *	  clauses using related operators and identical input expressions.
 */
fn operator_predicate_proof<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: &Expr,
    clause: &Expr,
    refute_it: bool,
    weak: bool,
) -> PgResult<bool> {
    let mut pred_op: Oid;
    let mut clause_op: Oid;
    let test_op: Oid;

    /* Both expressions must be binary opclauses, else we can't do anything. */
    let pred_opexpr = match predicate.as_opexpr() {
        Some(op) => op,
        _ => return Ok(false),
    };
    if pred_opexpr.args.len() != 2 {
        return Ok(false);
    }
    let clause_opexpr = match clause.as_opexpr() {
        Some(op) => op,
        _ => return Ok(false),
    };
    if clause_opexpr.args.len() != 2 {
        return Ok(false);
    }

    /* If they're marked with different collations we can't do anything. */
    let pred_collation = pred_opexpr.inputcollid;
    let clause_collation = clause_opexpr.inputcollid;
    if pred_collation != clause_collation {
        return Ok(false);
    }

    /* Grab the operator OIDs now too.  We may commute these below. */
    pred_op = pred_opexpr.opno;
    clause_op = clause_opexpr.opno;

    /* We have to match up at least one pair of input expressions. */
    let pred_leftop = &pred_opexpr.args[0];
    let pred_rightop = &pred_opexpr.args[1];
    let clause_leftop = &clause_opexpr.args[0];
    let clause_rightop = &clause_opexpr.args[1];

    let pred_const: &Const;
    let clause_const: &Const;

    if equal_expr::call(pred_leftop, clause_leftop) {
        if equal_expr::call(pred_rightop, clause_rightop) {
            /* We have x op1 y and x op2 y */
            return operator_same_subexprs_proof(mcx, pred_op, clause_op, refute_it);
        } else {
            /* Fail unless rightops are both Consts */
            pred_const = match pred_rightop.as_const() {
                Some(c) => c,
                _ => return Ok(false),
            };
            clause_const = match clause_rightop.as_const() {
                Some(c) => c,
                _ => return Ok(false),
            };
        }
    } else if equal_expr::call(pred_rightop, clause_rightop) {
        /* Fail unless leftops are both Consts */
        pred_const = match pred_leftop.as_const() {
            Some(c) => c,
            _ => return Ok(false),
        };
        clause_const = match clause_leftop.as_const() {
            Some(c) => c,
            _ => return Ok(false),
        };
        /* Commute both operators so we can assume Consts are on the right */
        pred_op = lsyscache::get_commutator::call(pred_op)?;
        if !oid_is_valid(pred_op) {
            return Ok(false);
        }
        clause_op = lsyscache::get_commutator::call(clause_op)?;
        if !oid_is_valid(clause_op) {
            return Ok(false);
        }
    } else if equal_expr::call(pred_leftop, clause_rightop) {
        if equal_expr::call(pred_rightop, clause_leftop) {
            /* We have x op1 y and y op2 x */
            /* Commute pred_op that we can treat this like a straight match */
            pred_op = lsyscache::get_commutator::call(pred_op)?;
            if !oid_is_valid(pred_op) {
                return Ok(false);
            }
            return operator_same_subexprs_proof(mcx, pred_op, clause_op, refute_it);
        } else {
            /* Fail unless pred_rightop/clause_leftop are both Consts */
            pred_const = match pred_rightop.as_const() {
                Some(c) => c,
                _ => return Ok(false),
            };
            clause_const = match clause_leftop.as_const() {
                Some(c) => c,
                _ => return Ok(false),
            };
            /* Commute clause_op so we can assume Consts are on the right */
            clause_op = lsyscache::get_commutator::call(clause_op)?;
            if !oid_is_valid(clause_op) {
                return Ok(false);
            }
        }
    } else if equal_expr::call(pred_rightop, clause_leftop) {
        /* Fail unless pred_leftop/clause_rightop are both Consts */
        pred_const = match pred_leftop.as_const() {
            Some(c) => c,
            _ => return Ok(false),
        };
        clause_const = match clause_rightop.as_const() {
            Some(c) => c,
            _ => return Ok(false),
        };
        /* Commute pred_op so we can assume Consts are on the right */
        pred_op = lsyscache::get_commutator::call(pred_op)?;
        if !oid_is_valid(pred_op) {
            return Ok(false);
        }
    } else {
        /* Failed to match up any of the subexpressions, so we lose */
        return Ok(false);
    }

    /*
     * We have two identical subexpressions, and two other subexpressions that
     * are not identical but are both Consts; the Consts are on the right.  If
     * either is NULL, we usually fail ... but in some cases we can claim
     * success.
     */
    if clause_const.constisnull {
        /* If clause_op isn't strict, we can't prove anything */
        if !lsyscache::op_strict::call(clause_op)? {
            return Ok(false);
        }

        /*
         * The clause returns NULL.  For proof types that assume truth of the
         * clause, this means the proof is vacuously true.  That's all proof
         * types except weak implication.
         */
        if !(weak && !refute_it) {
            return Ok(true);
        }

        /*
         * For weak implication, it's still possible for the proof to succeed,
         * if the predicate can also be proven NULL.  NULL => NULL is valid.
         */
        if pred_const.constisnull && lsyscache::op_strict::call(pred_op)? {
            return Ok(true);
        }
        /* Else the proof fails */
        return Ok(false);
    }
    if pred_const.constisnull {
        /*
         * If the pred_op is strict, the predicate yields NULL, which means the
         * proof succeeds for either weak implication or weak refutation.
         */
        if weak && lsyscache::op_strict::call(pred_op)? {
            return Ok(true);
        }
        /* Else the proof fails */
        return Ok(false);
    }

    /*
     * Lookup the constant-comparison operator using the system catalogs and
     * the operator implication tables.
     */
    test_op = get_btree_test_op(mcx, pred_op, clause_op, refute_it)?;

    if !oid_is_valid(test_op) {
        /* couldn't find a suitable comparison operator */
        return Ok(false);
    }

    /*
     * Evaluate the test.  For this we need an EState (built/torn-down inside
     * the seam, exactly as the C does).
     */
    match own_seam::eval_const_test::call(test_op, pred_const, clause_const, pred_collation)? {
        /* Treat a null result as non-proof ... but it's a tad fishy ... */
        None => Ok(false),
        Some(b) => Ok(b),
    }
}

/*
 * operator_same_subexprs_proof
 *	  Assuming that EXPR1 clause_op EXPR2 is true, try to prove or refute
 *	  EXPR1 pred_op EXPR2.
 */
fn operator_same_subexprs_proof<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<bool> {
    /*
     * The predicate is proven if clause_op and pred_op are the same, or refuted
     * if they are each other's negators.
     */
    if refute_it {
        if lsyscache::get_negator::call(pred_op)? == clause_op {
            return Ok(true);
        }
    } else if pred_op == clause_op {
        return Ok(true);
    }

    /*
     * Otherwise, see if we can determine the implication by finding the
     * operators' relationship via some btree opfamily.
     */
    operator_same_subexprs_lookup(mcx, pred_op, clause_op, refute_it)
}

/* ====================================================================
 * The btree-proof lookaside cache — genuine in-module per-backend state.
 *
 * The C `static HTAB *OprProofCacheHash` keyed by (pred_op, clause_op).  A
 * single entry stores both implication and refutation results for the pair,
 * each possibly not-yet-determined.  Ported as a `thread_local!` `HashMap`
 * (per-backend, per the backend-global state model), flushed wholesale on a
 * `pg_amop` invalidation (`InvalidateOprProofCacheCallBack`).
 * ================================================================== */

#[derive(Clone, Copy, Default)]
struct OprProofCacheEntry {
    have_implic: bool,
    have_refute: bool,
    same_subexprs_implies: bool,
    same_subexprs_refutes: bool,
    implic_test_op: Oid,
    refute_test_op: Oid,
}

thread_local! {
    /// `OprProofCacheHash` — `None` until first use (the C NULL HTAB), then the
    /// per-backend (pred_op, clause_op) -> entry map.
    static OPR_PROOF_CACHE: RefCell<Option<HashMap<(Oid, Oid), OprProofCacheEntry>>> =
        const { RefCell::new(None) };
}

/// `InvalidateOprProofCacheCallBack` (predtest.c) — flush every entry's
/// computed-flag on a `pg_amop` invalidation.  We just reset all entries; hard
/// to be smarter.
fn invalidate_opr_proof_cache_callback(_cacheid: i32, _hashvalue: u32) {
    OPR_PROOF_CACHE.with(|cache| {
        if let Some(map) = cache.borrow_mut().as_mut() {
            for entry in map.values_mut() {
                entry.have_implic = false;
                entry.have_refute = false;
            }
        }
    });
}

/*
 * lookup_proof_cache
 *	  Get, and fill in if necessary, the appropriate cache entry.
 */
fn lookup_proof_cache<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<OprProofCacheEntry> {
    /*
     * Find or make a cache entry for this pair of operators.  Initialise the
     * hash table on first use, arranging to flush on pg_amop changes.
     */
    let need_init = OPR_PROOF_CACHE.with(|cache| cache.borrow().is_none());
    if need_init {
        OPR_PROOF_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(HashMap::new());
        });
        own_seam::register_oprproof_syscache_callback::call(
            AMOPOPID,
            invalidate_opr_proof_cache_callback,
        )?;
    }

    let key = (pred_op, clause_op);

    /* If we already know the requested direction, return the entry. */
    let existing = OPR_PROOF_CACHE.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .and_then(|map| map.get(&key).copied())
    });
    let mut cache_entry = match existing {
        Some(e) => {
            if if refute_it { e.have_refute } else { e.have_implic } {
                return Ok(e);
            }
            e
        }
        None => OprProofCacheEntry::default(),
    };

    let mut same_subexprs = false;
    let mut test_op: Oid = InvalidOid;
    let mut found = false;

    /*
     * Try to find a btree opfamily containing the given operators.  We must
     * find one that contains both operators, else the implication can't be
     * determined.  Also, the opfamily must contain a suitable test operator
     * taking the operators' righthand datatypes.
     */
    let clause_op_infos = lsyscache::get_op_index_interpretation::call(mcx, clause_op)?;
    let pred_op_infos = if !clause_op_infos.is_empty() {
        lsyscache::get_op_index_interpretation::call(mcx, pred_op)?
    } else {
        /* no point in looking */
        mcx::PgVec::new_in(mcx)
    };

    'pred_loop: for pred_op_info in pred_op_infos.iter() {
        let opfamily_id = pred_op_info.opfamily_id;

        for clause_op_info in clause_op_infos.iter() {
            /* Must find them in same opfamily */
            if opfamily_id != clause_op_info.opfamily_id {
                continue;
            }
            /* Lefttypes should match */
            debug_assert!(clause_op_info.oplefttype == pred_op_info.oplefttype);

            let pred_cmptype = pred_op_info.cmptype;
            let clause_cmptype = clause_op_info.cmptype;

            /*
             * Check to see if we can make a proof for same-subexpressions
             * cases based on the operators' relationship in this opfamily.
             */
            if refute_it {
                same_subexprs |= RC_REFUTES_TABLE[(clause_cmptype - 1) as usize]
                    [(pred_cmptype - 1) as usize];
            } else {
                same_subexprs |= RC_IMPLIES_TABLE[(clause_cmptype - 1) as usize]
                    [(pred_cmptype - 1) as usize];
            }

            /* Look up the "test" cmptype number in the implication table */
            let test_cmptype: i32 = if refute_it {
                RC_REFUTE_TABLE[(clause_cmptype - 1) as usize][(pred_cmptype - 1) as usize]
            } else {
                RC_IMPLIC_TABLE[(clause_cmptype - 1) as usize][(pred_cmptype - 1) as usize]
            };

            if test_cmptype == 0 {
                /* Can't determine implication using this interpretation */
                continue;
            }

            /*
             * See if opfamily has an operator for the test cmptype and the
             * datatypes.
             */
            if test_cmptype == RCNE {
                test_op = lsyscache::get_opfamily_member_for_cmptype::call(
                    opfamily_id,
                    pred_op_info.oprighttype,
                    clause_op_info.oprighttype,
                    RCEQ,
                )?;
                if oid_is_valid(test_op) {
                    test_op = lsyscache::get_negator::call(test_op)?;
                }
            } else {
                test_op = lsyscache::get_opfamily_member_for_cmptype::call(
                    opfamily_id,
                    pred_op_info.oprighttype,
                    clause_op_info.oprighttype,
                    test_cmptype,
                )?;
            }

            if !oid_is_valid(test_op) {
                continue;
            }

            /*
             * Last check: test_op must be immutable.  We require only the
             * test_op to be immutable, not the original clause_op.
             */
            if lsyscache::op_volatile::call(test_op)? == PROVOLATILE_IMMUTABLE {
                found = true;
                break;
            }
        }

        if found {
            break 'pred_loop;
        }
    }

    if !found {
        /* couldn't find a suitable comparison operator */
        test_op = InvalidOid;
    }

    /*
     * If we think we were able to prove something about same-subexpressions
     * cases, check to make sure the clause_op is immutable before believing it.
     */
    if same_subexprs && lsyscache::op_volatile::call(clause_op)? != PROVOLATILE_IMMUTABLE {
        same_subexprs = false;
    }

    /* Cache the results, whether positive or negative */
    if refute_it {
        cache_entry.refute_test_op = test_op;
        cache_entry.same_subexprs_refutes = same_subexprs;
        cache_entry.have_refute = true;
    } else {
        cache_entry.implic_test_op = test_op;
        cache_entry.same_subexprs_implies = same_subexprs;
        cache_entry.have_implic = true;
    }

    OPR_PROOF_CACHE.with(|cache| {
        if let Some(map) = cache.borrow_mut().as_mut() {
            map.insert(key, cache_entry);
        }
    });

    Ok(cache_entry)
}

/*
 * operator_same_subexprs_lookup
 *	  Convenience subroutine to look up the cached answer for
 *	  same-subexpressions cases.
 */
fn operator_same_subexprs_lookup<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<bool> {
    let cache_entry = lookup_proof_cache(mcx, pred_op, clause_op, refute_it)?;
    if refute_it {
        Ok(cache_entry.same_subexprs_refutes)
    } else {
        Ok(cache_entry.same_subexprs_implies)
    }
}

/*
 * get_btree_test_op
 *	  Identify the comparison operator needed for a btree-operator proof or
 *	  refutation involving comparison of constants.  Returns the OID of the
 *	  operator to use, or InvalidOid if no proof is possible.
 */
fn get_btree_test_op<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<Oid> {
    let cache_entry = lookup_proof_cache(mcx, pred_op, clause_op, refute_it)?;
    if refute_it {
        Ok(cache_entry.refute_test_op)
    } else {
        Ok(cache_entry.implic_test_op)
    }
}
