//! predtest.c: predicate_implied_by / predicate_refuted_by proof engine.

use core::cell::RefCell;
use core::mem::ManuallyDrop;

use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{InvalidOid, Oid, BOOLOID};
use types_error::PgResult;
use types_nodes::equal::equal;
use types_nodes::list::NodeList;
use types_nodes::primnodes::{BoolTestType, Const, NullTestType, OpExpr};
use types_nodes::Node;
use types_pathnodes::{
    CompareType, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT, COMPARE_NE,
};

const MAX_SAOP_ARRAY_SIZE: i32 = 100;
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;

pub fn predicate_implied_by<'mcx>(
    mcx: Mcx<'mcx>,
    predicate_list: &[Node<'mcx>],
    clause_list: &[Node<'mcx>],
    weak: bool,
) -> PgResult<bool> {
    if predicate_list.is_empty() {
        return Ok(true);
    }
    if clause_list.is_empty() {
        return Ok(false);
    }
    let p = wrap_top_level(mcx, predicate_list)?;
    let c = wrap_top_level(mcx, clause_list)?;
    predicate_implied_by_recurse(mcx, c, p, weak)
}

pub fn predicate_refuted_by<'mcx>(
    mcx: Mcx<'mcx>,
    predicate_list: &[Node<'mcx>],
    clause_list: &[Node<'mcx>],
    weak: bool,
) -> PgResult<bool> {
    if predicate_list.is_empty() {
        return Ok(false);
    }
    if clause_list.is_empty() {
        return Ok(false);
    }
    let p = wrap_top_level(mcx, predicate_list)?;
    let c = wrap_top_level(mcx, clause_list)?;
    predicate_refuted_by_recurse(mcx, c, p, weak)
}

// C passes the multi-element List* itself; predicate_classify treats a bare
// List as implicit-AND, so a single T_List node is observationally identical.
fn wrap_top_level<'mcx>(mcx: Mcx<'mcx>, items: &[Node<'mcx>]) -> PgResult<Node<'mcx>> {
    if items.len() == 1 {
        Ok(items[0])
    } else {
        Node::mk_list(mcx, NodeList::from_slice(mcx, items)?)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PredClass {
    Atom,
    And,
    Or,
}

enum PredIter<'mcx> {
    Atom,
    List(&'mcx [Node<'mcx>]),
    ArrayConst(&'mcx types_nodes::primnodes::ScalarArrayOpExpr<'mcx>),
    ArrayExpr(&'mcx types_nodes::primnodes::ScalarArrayOpExpr<'mcx>),
}

enum Components<'mcx> {
    Slice(&'mcx [Node<'mcx>]),
    Vec(PgVec<'mcx, Node<'mcx>>),
}

impl<'mcx> Components<'mcx> {
    fn as_slice(&self) -> &[Node<'mcx>] {
        match self {
            Components::Slice(s) => s,
            Components::Vec(v) => v.as_slice(),
        }
    }
}

impl<'mcx> PredIter<'mcx> {
    fn components(&self, mcx: Mcx<'mcx>) -> PgResult<Components<'mcx>> {
        match self {
            PredIter::Atom => Ok(Components::Slice(&[])),
            PredIter::List(s) => Ok(Components::Slice(s)),
            PredIter::ArrayConst(saop) => Ok(Components::Vec(arrayconst_components(mcx, saop)?)),
            PredIter::ArrayExpr(saop) => Ok(Components::Vec(arrayexpr_components(mcx, saop)?)),
        }
    }
}

fn predicate_classify<'mcx>(node: Node<'mcx>) -> (PredClass, PredIter<'mcx>) {
    if let Some(list) = node.as_list() {
        return (PredClass::And, PredIter::List(list.as_slice()));
    }
    if clauses::is_andclause(node) {
        let b = node.as_bool_expr().expect("BoolExpr");
        return (PredClass::And, PredIter::List(b.args.as_slice()));
    }
    if clauses::is_orclause(node) {
        let b = node.as_bool_expr().expect("BoolExpr");
        return (PredClass::Or, PredIter::List(b.args.as_slice()));
    }
    if let Some(saop) = node.as_scalar_array_op_expr() {
        let class = if saop.useOr { PredClass::Or } else { PredClass::And };
        if let Some(arraynode) = saop.args.as_slice().get(1).copied() {
            if let Some(c) = arraynode.as_const() {
                if !c.constisnull {
                    let nelems = const_array_nelems(c);
                    if nelems <= MAX_SAOP_ARRAY_SIZE {
                        return (class, PredIter::ArrayConst(saop));
                    }
                }
            } else if let Some(a) = arraynode.as_array_expr() {
                if !a.multidims && a.elements.len() as i32 <= MAX_SAOP_ARRAY_SIZE {
                    return (class, PredIter::ArrayExpr(saop));
                }
            }
        }
    }
    (PredClass::Atom, PredIter::Atom)
}

// Header-relative dims read: works for 1B and 4B array images (bound-param
// array consts can be short-form).
fn const_array_nelems(c: &Const) -> i32 {
    let body = crate::selfuncs::varlena_datum_payload(c.constvalue);
    let rd = |off: usize| i32::from_ne_bytes(body[off..off + 4].try_into().unwrap());
    let ndim = rd(0);
    let mut dims = [0i32; arrayutils::MAXDIM as usize];
    let n = ndim.clamp(0, arrayutils::MAXDIM) as usize;
    for (i, d) in dims[..n].iter_mut().enumerate() {
        *d = rd(12 + 4 * i);
    }
    arrayutils::array_get_n_items(ndim, &dims[..n]).expect("valid stored array")
}

fn arrayconst_components<'mcx>(
    mcx: Mcx<'mcx>,
    saop: &types_nodes::primnodes::ScalarArrayOpExpr<'mcx>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let scalar = saop.args.nth(0);
    let arrayconst = saop.args.nth(1).as_const().expect("classified as Const array");
    let img = crate::selfuncs::varlena_image_any(mcx, arrayconst.constvalue)?;
    let elemtype = arrayfuncs::arr_elemtype(img);
    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
    let (values, nulls) =
        arrayfuncs::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;
    let mut out = PgVec::new_in(mcx);
    for (i, &v) in values.iter().enumerate() {
        let elem = Node::mk(
            mcx,
            Const {
                consttype: elemtype,
                consttypmod: -1,
                constcollid: arrayconst.constcollid,
                constlen: elmlen as i32,
                constvalue: v,
                constisnull: nulls[i],
                constbyval: elmbyval,
                location: -1,
            },
        )?;
        out.push(make_saop_op_expr(mcx, saop, scalar, elem)?);
    }
    Ok(out)
}

fn arrayexpr_components<'mcx>(
    mcx: Mcx<'mcx>,
    saop: &types_nodes::primnodes::ScalarArrayOpExpr<'mcx>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let scalar = saop.args.nth(0);
    let elements = saop.args.nth(1).as_array_expr().expect("classified as ArrayExpr").elements.as_slice();
    let mut out = PgVec::new_in(mcx);
    for &elem in elements {
        out.push(make_saop_op_expr(mcx, saop, scalar, elem)?);
    }
    Ok(out)
}

fn make_saop_op_expr<'mcx>(
    mcx: Mcx<'mcx>,
    saop: &types_nodes::primnodes::ScalarArrayOpExpr<'mcx>,
    leftop: Node<'mcx>,
    rightop: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        OpExpr {
            opno: saop.opno,
            opfuncid: saop.opfuncid,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: saop.inputcollid,
            args: NodeList::make2(mcx, leftop, rightop)?,
            location: -1,
        },
    )
}

fn predicate_implied_by_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    clause: Node<'mcx>,
    predicate: Node<'mcx>,
    weak: bool,
) -> PgResult<bool> {
    let (pclass, pred_info) = predicate_classify(predicate);
    let (cclass, clause_info) = predicate_classify(clause);
    match (cclass, pclass) {
        (PredClass::And, PredClass::And) => {
            for &pitem in pred_info.components(mcx)?.as_slice() {
                if !predicate_implied_by_recurse(mcx, clause, pitem, weak)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (PredClass::And, PredClass::Or) => {
            // (x AND y) => ((x AND y) OR z)
            for &pitem in pred_info.components(mcx)?.as_slice() {
                if predicate_implied_by_recurse(mcx, clause, pitem, weak)? {
                    return Ok(true);
                }
            }
            // ((x OR y) AND z) => (x OR y)
            for &citem in clause_info.components(mcx)?.as_slice() {
                if predicate_implied_by_recurse(mcx, citem, predicate, weak)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        (PredClass::And, PredClass::Atom) => {
            for &citem in clause_info.components(mcx)?.as_slice() {
                if predicate_implied_by_recurse(mcx, citem, predicate, weak)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        (PredClass::Or, PredClass::Or) => {
            let pred_components = pred_info.components(mcx)?;
            for &citem in clause_info.components(mcx)?.as_slice() {
                let mut presult = false;
                for &pitem in pred_components.as_slice() {
                    if predicate_implied_by_recurse(mcx, citem, pitem, weak)? {
                        presult = true;
                        break;
                    }
                }
                if !presult {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (PredClass::Or, _) => {
            for &citem in clause_info.components(mcx)?.as_slice() {
                if !predicate_implied_by_recurse(mcx, citem, predicate, weak)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (PredClass::Atom, PredClass::And) => {
            for &pitem in pred_info.components(mcx)?.as_slice() {
                if !predicate_implied_by_recurse(mcx, clause, pitem, weak)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (PredClass::Atom, PredClass::Or) => {
            for &pitem in pred_info.components(mcx)?.as_slice() {
                if predicate_implied_by_recurse(mcx, clause, pitem, weak)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        (PredClass::Atom, PredClass::Atom) => {
            predicate_implied_by_simple_clause(mcx, predicate, clause, weak)
        }
    }
}

fn predicate_refuted_by_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    clause: Node<'mcx>,
    predicate: Node<'mcx>,
    weak: bool,
) -> PgResult<bool> {
    let (pclass, pred_info) = predicate_classify(predicate);
    let (cclass, clause_info) = predicate_classify(clause);
    match cclass {
        PredClass::And => match pclass {
            PredClass::And => {
                // (x AND y) R=> ((!x OR !y) AND z)
                for &pitem in pred_info.components(mcx)?.as_slice() {
                    if predicate_refuted_by_recurse(mcx, clause, pitem, weak)? {
                        return Ok(true);
                    }
                }
                // ((x OR y) AND z) R=> (!x AND !y)
                for &citem in clause_info.components(mcx)?.as_slice() {
                    if predicate_refuted_by_recurse(mcx, citem, predicate, weak)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            PredClass::Or => {
                for &pitem in pred_info.components(mcx)?.as_slice() {
                    if !predicate_refuted_by_recurse(mcx, clause, pitem, weak)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PredClass::Atom => {
                // A R=> NOT-ish B if A => B's arg (strong implication suffices).
                if let Some(not_arg) = extract_not_arg(predicate) {
                    if predicate_implied_by_recurse(mcx, clause, not_arg, false)? {
                        return Ok(true);
                    }
                }
                for &citem in clause_info.components(mcx)?.as_slice() {
                    if predicate_refuted_by_recurse(mcx, citem, predicate, weak)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        },
        PredClass::Or => match pclass {
            PredClass::Or => {
                for &pitem in pred_info.components(mcx)?.as_slice() {
                    if !predicate_refuted_by_recurse(mcx, clause, pitem, weak)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PredClass::And => {
                let pred_components = pred_info.components(mcx)?;
                for &citem in clause_info.components(mcx)?.as_slice() {
                    let mut presult = false;
                    for &pitem in pred_components.as_slice() {
                        if predicate_refuted_by_recurse(mcx, citem, pitem, weak)? {
                            presult = true;
                            break;
                        }
                    }
                    if !presult {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            PredClass::Atom => {
                if let Some(not_arg) = extract_not_arg(predicate) {
                    if predicate_implied_by_recurse(mcx, clause, not_arg, false)? {
                        return Ok(true);
                    }
                }
                for &citem in clause_info.components(mcx)?.as_slice() {
                    if !predicate_refuted_by_recurse(mcx, citem, predicate, weak)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        },
        PredClass::Atom => {
            // Strong NOT-clause A R=> B if B (strongly for weak refutation,
            // weakly for strong) implies A's arg.
            if let Some(not_arg) = extract_strong_not_arg(clause) {
                if predicate_implied_by_recurse(mcx, predicate, not_arg, !weak)? {
                    return Ok(true);
                }
            }
            match pclass {
                PredClass::And => {
                    for &pitem in pred_info.components(mcx)?.as_slice() {
                        if predicate_refuted_by_recurse(mcx, clause, pitem, weak)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                PredClass::Or => {
                    for &pitem in pred_info.components(mcx)?.as_slice() {
                        if !predicate_refuted_by_recurse(mcx, clause, pitem, weak)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                PredClass::Atom => {
                    if let Some(not_arg) = extract_not_arg(predicate) {
                        if predicate_implied_by_recurse(mcx, clause, not_arg, false)? {
                            return Ok(true);
                        }
                    }
                    predicate_refuted_by_simple_clause(mcx, predicate, clause, weak)
                }
            }
        }
    }
}

fn predicate_implied_by_simple_clause<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: Node<'mcx>,
    clause: Node<'mcx>,
    weak: bool,
) -> PgResult<bool> {
    postgres_seams::check_for_interrupts::call()?;

    if equal(predicate, clause) {
        return Ok(true);
    }

    if let Some(op) = clause.as_op_expr() {
        // "x = TRUE" implies x; "x = FALSE" implies NOT x.
        if op.opno == BOOLEAN_EQUAL_OPERATOR {
            debug_assert!(op.args.len() == 2);
            let rightop = op.args.nth(1);
            if let Some(rc) = rightop.as_const() {
                if !rc.constisnull {
                    let leftop = op.args.nth(0);
                    if rc.constvalue.as_bool() {
                        if equal(predicate, leftop) {
                            return Ok(true);
                        }
                    } else if clauses::is_notclause(predicate)
                        && equal(
                            predicate.as_bool_expr().expect("NOT clause").args.nth(0),
                            leftop,
                        )
                    {
                        return Ok(true);
                    }
                }
            }
        }
    }

    if let Some(predntest) = predicate.as_null_test() {
        // Strong implication of "foo IS NOT NULL" by a clause strict for foo.
        if predntest.nulltesttype == NullTestType::IS_NOT_NULL
            && !weak
            && !predntest.argisrow
        {
            if let Some(arg) = predntest.arg {
                if clause_is_strict_for(clause, arg, true)? {
                    return Ok(true);
                }
            }
        }
    }

    operator_predicate_proof(mcx, predicate, clause, false, weak)
}

fn predicate_refuted_by_simple_clause<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: Node<'mcx>,
    clause: Node<'mcx>,
    weak: bool,
) -> PgResult<bool> {
    postgres_seams::check_for_interrupts::call()?;

    // relation_excluded_by_constraints may pass the same node on both sides.
    if predicate.ptr_eq(clause) {
        return Ok(false);
    }

    if let Some(clausentest) = clause.as_null_test() {
        // row IS [NOT] NULL does not act in the simple way we have in mind.
        if clausentest.argisrow {
            return Ok(false);
        }
        if clausentest.nulltesttype == NullTestType::IS_NULL {
            if let Some(predntest) = predicate.as_null_test() {
                if predntest.argisrow {
                    return Ok(false);
                }
                if predntest.nulltesttype == NullTestType::IS_NOT_NULL
                    && equal_opt(predntest.arg, clausentest.arg)
                {
                    return Ok(true);
                }
            }
            if weak {
                if let Some(carg) = clausentest.arg {
                    if clause_is_strict_for(predicate, carg, true)? {
                        return Ok(true);
                    }
                }
            }
            return Ok(false);
        }
    }

    if let Some(predntest) = predicate.as_null_test() {
        if predntest.argisrow {
            return Ok(false);
        }
        if predntest.nulltesttype == NullTestType::IS_NULL {
            if let Some(clausentest) = clause.as_null_test() {
                if clausentest.argisrow {
                    return Ok(false);
                }
                if clausentest.nulltesttype == NullTestType::IS_NOT_NULL
                    && equal_opt(clausentest.arg, predntest.arg)
                {
                    return Ok(true);
                }
            }
            if let Some(parg) = predntest.arg {
                if clause_is_strict_for(clause, parg, true)? {
                    return Ok(true);
                }
            }
        }
        return Ok(false);
    }

    operator_predicate_proof(mcx, predicate, clause, true, weak)
}

fn equal_opt(a: Option<Node<'_>>, b: Option<Node<'_>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => equal(a, b),
        (None, None) => true,
        _ => false,
    }
}

fn extract_not_arg(clause: Node<'_>) -> Option<Node<'_>> {
    if clauses::is_notclause(clause) {
        return clause.as_bool_expr().and_then(|b| b.args.first());
    }
    if let Some(bt) = clause.as_boolean_test() {
        if matches!(
            bt.booltesttype,
            BoolTestType::IS_NOT_TRUE | BoolTestType::IS_FALSE | BoolTestType::IS_UNKNOWN
        ) {
            return bt.arg;
        }
    }
    None
}

fn extract_strong_not_arg(clause: Node<'_>) -> Option<Node<'_>> {
    if clauses::is_notclause(clause) {
        return clause.as_bool_expr().and_then(|b| b.args.first());
    }
    if let Some(bt) = clause.as_boolean_test() {
        if bt.booltesttype == BoolTestType::IS_FALSE {
            return bt.arg;
        }
    }
    None
}

// Can clause be proven NULL (or FALSE, when allow_false) given subexpr NULL?
// C's ArrayCoerceExpr/ConvertRowtypeExpr arms are dead: those tags are outside
// this repo's expression vocabulary.
fn clause_is_strict_for<'mcx>(
    mut clause: Node<'mcx>,
    mut subexpr: Node<'mcx>,
    allow_false: bool,
) -> PgResult<bool> {
    if let Some(r) = clause.as_relabel_type() {
        clause = r.arg;
    }
    if let Some(r) = subexpr.as_relabel_type() {
        subexpr = r.arg;
    }

    if equal(clause, subexpr) {
        return Ok(true);
    }

    if let Some(op) = clause.as_op_expr() {
        if lsyscache::op_strict(op.opno)? {
            for arg in op.args.iter() {
                if clause_is_strict_for(arg, subexpr, false)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }
    if let Some(f) = clause.as_func_expr() {
        if lsyscache::func_strict(f.funcid)? {
            for arg in f.args.iter() {
                if clause_is_strict_for(arg, subexpr, false)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    if let Some(c) = clause.as_coerce_via_io() {
        return clause_is_strict_for(c.arg, subexpr, false);
    }
    if let Some(c) = clause.as_coerce_to_domain() {
        return clause_is_strict_for(c.arg, subexpr, false);
    }

    if let Some(saop) = clause.as_scalar_array_op_expr() {
        let scalarnode = saop.args.nth(0);
        let arraynode = saop.args.nth(1);
        if clause_is_strict_for(scalarnode, subexpr, false)? && lsyscache::op_strict(saop.opno)? {
            if allow_false && saop.useOr {
                return Ok(true);
            }
            let mut nelems: i32 = 0;
            if let Some(c) = arraynode.as_const() {
                if c.constisnull {
                    return Ok(true);
                }
                nelems = const_array_nelems(c);
            } else if let Some(a) = arraynode.as_array_expr() {
                if !a.multidims {
                    nelems = a.elements.len() as i32;
                }
            }
            if nelems > 0 {
                return Ok(true);
            }
        }
        return clause_is_strict_for(arraynode, subexpr, false);
    }

    if let Some(c) = clause.as_const() {
        return Ok(c.constisnull);
    }

    Ok(false)
}

const NB: bool = false;
const NC: CompareType = 0;

#[rustfmt::skip]
static RC_IMPLIES_TABLE: [[bool; 6]; 6] = [
    // predicate op:  LT  LE  EQ  GE  GT  NE      clause op:
    [true, true, NB,   NB,   NB, true],        // LT
    [NB,   true, NB,   NB,   NB, NB  ],        // LE
    [NB,   true, true, true, NB, NB  ],        // EQ
    [NB,   NB,   NB,   true, NB, NB  ],        // GE
    [NB,   NB,   NB,   true, true, true],      // GT
    [NB,   NB,   NB,   NB,   NB, true],        // NE
];

#[rustfmt::skip]
static RC_REFUTES_TABLE: [[bool; 6]; 6] = [
    [NB,   NB,   true, true, true, NB  ],      // LT
    [NB,   NB,   NB,   NB,   true, NB  ],      // LE
    [true, NB,   NB,   NB,   true, true],      // EQ
    [true, NB,   NB,   NB,   NB,   NB  ],      // GE
    [true, true, true, NB,   NB,   NB  ],      // GT
    [NB,   NB,   true, NB,   NB,   NB  ],      // NE
];

#[rustfmt::skip]
static RC_IMPLIC_TABLE: [[CompareType; 6]; 6] = [
    [COMPARE_GE, COMPARE_GE, NC,         NC,         NC,         COMPARE_GE], // LT
    [COMPARE_GT, COMPARE_GE, NC,         NC,         NC,         COMPARE_GT], // LE
    [COMPARE_GT, COMPARE_GE, COMPARE_EQ, COMPARE_LE, COMPARE_LT, COMPARE_NE], // EQ
    [NC,         NC,         NC,         COMPARE_LE, COMPARE_LT, COMPARE_LT], // GE
    [NC,         NC,         NC,         COMPARE_LE, COMPARE_LE, COMPARE_LE], // GT
    [NC,         NC,         NC,         NC,         NC,         COMPARE_EQ], // NE
];

#[rustfmt::skip]
static RC_REFUTE_TABLE: [[CompareType; 6]; 6] = [
    [NC,         NC,         COMPARE_GE, COMPARE_GE, COMPARE_GE, NC        ], // LT
    [NC,         NC,         COMPARE_GT, COMPARE_GT, COMPARE_GE, NC        ], // LE
    [COMPARE_LE, COMPARE_LT, COMPARE_NE, COMPARE_GT, COMPARE_GE, COMPARE_EQ], // EQ
    [COMPARE_LE, COMPARE_LT, COMPARE_LT, NC,         NC,         NC        ], // GE
    [COMPARE_LE, COMPARE_LE, COMPARE_LE, NC,         NC,         NC        ], // GT
    [NC,         NC,         COMPARE_EQ, NC,         NC,         NC        ], // NE
];

fn operator_predicate_proof<'mcx>(
    mcx: Mcx<'mcx>,
    predicate: Node<'mcx>,
    clause: Node<'mcx>,
    refute_it: bool,
    weak: bool,
) -> PgResult<bool> {
    let Some(pred_opexpr) = predicate.as_op_expr() else {
        return Ok(false);
    };
    if pred_opexpr.args.len() != 2 {
        return Ok(false);
    }
    let Some(clause_opexpr) = clause.as_op_expr() else {
        return Ok(false);
    };
    if clause_opexpr.args.len() != 2 {
        return Ok(false);
    }

    let pred_collation = pred_opexpr.inputcollid;
    if pred_collation != clause_opexpr.inputcollid {
        return Ok(false);
    }

    let mut pred_op = pred_opexpr.opno;
    let mut clause_op = clause_opexpr.opno;

    let pred_leftop = pred_opexpr.args.nth(0);
    let pred_rightop = pred_opexpr.args.nth(1);
    let clause_leftop = clause_opexpr.args.nth(0);
    let clause_rightop = clause_opexpr.args.nth(1);

    let pred_const: &Const;
    let clause_const: &Const;

    if equal(pred_leftop, clause_leftop) {
        if equal(pred_rightop, clause_rightop) {
            return operator_same_subexprs_proof(mcx, pred_op, clause_op, refute_it);
        }
        let (Some(p), Some(c)) = (pred_rightop.as_const(), clause_rightop.as_const()) else {
            return Ok(false);
        };
        pred_const = p;
        clause_const = c;
    } else if equal(pred_rightop, clause_rightop) {
        let (Some(p), Some(c)) = (pred_leftop.as_const(), clause_leftop.as_const()) else {
            return Ok(false);
        };
        pred_const = p;
        clause_const = c;
        pred_op = lsyscache::get_commutator(pred_op)?;
        if pred_op == InvalidOid {
            return Ok(false);
        }
        clause_op = lsyscache::get_commutator(clause_op)?;
        if clause_op == InvalidOid {
            return Ok(false);
        }
    } else if equal(pred_leftop, clause_rightop) {
        if equal(pred_rightop, clause_leftop) {
            pred_op = lsyscache::get_commutator(pred_op)?;
            if pred_op == InvalidOid {
                return Ok(false);
            }
            return operator_same_subexprs_proof(mcx, pred_op, clause_op, refute_it);
        }
        let (Some(p), Some(c)) = (pred_rightop.as_const(), clause_leftop.as_const()) else {
            return Ok(false);
        };
        pred_const = p;
        clause_const = c;
        clause_op = lsyscache::get_commutator(clause_op)?;
        if clause_op == InvalidOid {
            return Ok(false);
        }
    } else if equal(pred_rightop, clause_leftop) {
        let (Some(p), Some(c)) = (pred_leftop.as_const(), clause_rightop.as_const()) else {
            return Ok(false);
        };
        pred_const = p;
        clause_const = c;
        pred_op = lsyscache::get_commutator(pred_op)?;
        if pred_op == InvalidOid {
            return Ok(false);
        }
    } else {
        return Ok(false);
    }

    if clause_const.constisnull {
        if !lsyscache::op_strict(clause_op)? {
            return Ok(false);
        }
        // The clause returns NULL: vacuously proven for every proof type
        // except weak implication, where NULL => NULL still works.
        if !(weak && !refute_it) {
            return Ok(true);
        }
        if pred_const.constisnull && lsyscache::op_strict(pred_op)? {
            return Ok(true);
        }
        return Ok(false);
    }
    if pred_const.constisnull {
        if weak && lsyscache::op_strict(pred_op)? {
            return Ok(true);
        }
        return Ok(false);
    }

    let test_op = get_btree_test_op(mcx, pred_op, clause_op, refute_it)?;
    if test_op == InvalidOid {
        return Ok(false);
    }

    let test_expr = Node::mk(
        mcx,
        OpExpr {
            opno: test_op,
            opfuncid: lsyscache::get_opcode(test_op)?,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: pred_collation,
            args: NodeList::make2(
                mcx,
                Node::mk(mcx, *pred_const)?,
                Node::mk(mcx, *clause_const)?,
            )?,
            location: -1,
        },
    )?;
    let result = execexpr::evaluate_expr(mcx, test_expr, BOOLOID, -1, InvalidOid)?;
    let result = result.as_const().expect("evaluate_expr yields a Const");
    if result.constisnull {
        // Treat a null result as non-proof ... but it's a tad fishy ...
        return Ok(false);
    }
    Ok(result.constvalue.as_bool())
}

fn operator_same_subexprs_proof<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<bool> {
    if refute_it {
        if lsyscache::get_negator(pred_op)? == clause_op {
            return Ok(true);
        }
    } else if pred_op == clause_op {
        return Ok(true);
    }
    let entry = lookup_proof_cache(mcx, pred_op, clause_op, refute_it)?;
    Ok(if refute_it { entry.same_subexprs_refutes } else { entry.same_subexprs_implies })
}

fn get_btree_test_op<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<Oid> {
    let entry = lookup_proof_cache(mcx, pred_op, clause_op, refute_it)?;
    Ok(if refute_it { entry.refute_test_op } else { entry.implic_test_op })
}

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
    static OPR_PROOF_CACHE: RefCell<Option<ManuallyDrop<PgHashMap<'static, (Oid, Oid), OprProofCacheEntry>>>> =
        const { RefCell::new(None) };
}

fn invalidate_opr_proof_cache_callback(_arg: datum::Datum, _cacheid: i32, _hashvalue: u32) {
    OPR_PROOF_CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            map.clear();
        }
    });
}

fn lookup_proof_cache<'mcx>(
    mcx: Mcx<'mcx>,
    pred_op: Oid,
    clause_op: Oid,
    refute_it: bool,
) -> PgResult<OprProofCacheEntry> {
    let key = (pred_op, clause_op);
    let existing = OPR_PROOF_CACHE.with(|cell| -> PgResult<Option<OprProofCacheEntry>> {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            // Backend-lifetime table (C: hash_create at first use); flushed
            // wholesale on pg_amop changes.
            let cache_mcx =
                ::mcx::session_root("Btree proof lookup cache").mcx();
            inval::invalidate::CacheRegisterSyscacheCallback(
                cache_syscache::cacheinfo::AMOPOPID,
                invalidate_opr_proof_cache_callback,
                datum::Datum::null(),
            )?;
            *slot = Some(ManuallyDrop::new(PgHashMap::with_capacity_in(256, cache_mcx)));
        }
        Ok(slot.as_ref().unwrap().get(&key).copied())
    })?;

    let mut entry = match existing {
        Some(e) => {
            if if refute_it { e.have_refute } else { e.have_implic } {
                return Ok(e);
            }
            e
        }
        None => OprProofCacheEntry::default(),
    };

    let mut same_subexprs = false;
    let mut test_op = InvalidOid;
    let mut found = false;

    let clause_op_infos = lsyscache::get_op_index_interpretation(mcx, clause_op)?;
    let pred_op_infos = if !clause_op_infos.is_empty() {
        lsyscache::get_op_index_interpretation(mcx, pred_op)?
    } else {
        PgVec::new_in(mcx)
    };

    'pred_loop: for pred_op_info in pred_op_infos.iter() {
        let opfamily_id = pred_op_info.opfamily_id;
        for clause_op_info in clause_op_infos.iter() {
            if opfamily_id != clause_op_info.opfamily_id {
                continue;
            }
            debug_assert!(clause_op_info.oplefttype == pred_op_info.oplefttype);

            let pc = (pred_op_info.cmptype - 1) as usize;
            let cc = (clause_op_info.cmptype - 1) as usize;
            same_subexprs |= if refute_it {
                RC_REFUTES_TABLE[cc][pc]
            } else {
                RC_IMPLIES_TABLE[cc][pc]
            };

            let test_cmptype =
                if refute_it { RC_REFUTE_TABLE[cc][pc] } else { RC_IMPLIC_TABLE[cc][pc] };
            if test_cmptype == 0 {
                continue;
            }

            if test_cmptype == COMPARE_NE {
                test_op = lsyscache::get_opfamily_member_for_cmptype(
                    opfamily_id,
                    pred_op_info.oprighttype,
                    clause_op_info.oprighttype,
                    COMPARE_EQ,
                )?;
                if test_op != InvalidOid {
                    test_op = lsyscache::get_negator(test_op)?;
                }
            } else {
                test_op = lsyscache::get_opfamily_member_for_cmptype(
                    opfamily_id,
                    pred_op_info.oprighttype,
                    clause_op_info.oprighttype,
                    test_cmptype,
                )?;
            }
            if test_op == InvalidOid {
                continue;
            }

            // Only test_op (not clause_op) must be immutable: cross-type btree
            // members can be merely stable, but the family is assumed consistent.
            if lsyscache::op_volatile(test_op)? == PROVOLATILE_IMMUTABLE {
                found = true;
                break 'pred_loop;
            }
        }
    }

    if !found {
        test_op = InvalidOid;
    }

    if same_subexprs && lsyscache::op_volatile(clause_op)? != PROVOLATILE_IMMUTABLE {
        same_subexprs = false;
    }

    if refute_it {
        entry.refute_test_op = test_op;
        entry.same_subexprs_refutes = same_subexprs;
        entry.have_refute = true;
    } else {
        entry.implic_test_op = test_op;
        entry.same_subexprs_implies = same_subexprs;
        entry.have_implic = true;
    }

    OPR_PROOF_CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            map.insert(key, entry);
        }
    });

    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::primnodes::{BoolExpr, BoolExprType, NullTest, Var, VarReturningType};
    use types_nodes::bitmapset::Bitmapset;

    fn var<'mcx>(mcx: Mcx<'mcx>, attno: i16) -> Node<'mcx> {
        Node::mk(
            mcx,
            Var {
                varno: 1,
                varattno: attno,
                vartype: 23,
                vartypmod: -1,
                varcollid: 0,
                varnullingrels: Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
                varnosyn: 1,
                varattnosyn: attno,
                location: -1,
            },
        )
        .unwrap()
    }

    fn null_test<'mcx>(mcx: Mcx<'mcx>, arg: Node<'mcx>, not_null: bool) -> Node<'mcx> {
        Node::mk(
            mcx,
            NullTest {
                arg: Some(arg),
                nulltesttype: if not_null {
                    NullTestType::IS_NOT_NULL
                } else {
                    NullTestType::IS_NULL
                },
                argisrow: false,
                location: -1,
            },
        )
        .unwrap()
    }

    fn and2<'mcx>(mcx: Mcx<'mcx>, a: Node<'mcx>, b: Node<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            BoolExpr {
                boolop: BoolExprType::AND_EXPR,
                args: NodeList::make2(mcx, a, b).unwrap(),
                location: -1,
            },
        )
        .unwrap()
    }

    fn setup() {
        crate::tests::install_fixtures();
    }

    #[test]
    fn clause_implies_itself() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = null_test(mcx, var(mcx, 1), true);
        let b = null_test(mcx, var(mcx, 1), true);
        assert!(predicate_implied_by(mcx, &[a], &[b], false).unwrap());
        assert!(predicate_implied_by(mcx, &[a], &[b], true).unwrap());
        let c = null_test(mcx, var(mcx, 2), true);
        assert!(!predicate_implied_by(mcx, &[c], &[b], false).unwrap());
    }

    #[test]
    fn null_test_refutations() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let is_null = null_test(mcx, var(mcx, 1), false);
        let not_null = null_test(mcx, var(mcx, 1), true);
        assert!(predicate_refuted_by(mcx, &[not_null], &[is_null], false).unwrap());
        assert!(predicate_refuted_by(mcx, &[is_null], &[not_null], false).unwrap());
        // A clause can't refute itself.
        assert!(!predicate_refuted_by(mcx, &[is_null], &[is_null], true).unwrap());
    }

    #[test]
    fn and_or_lattice() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let x = null_test(mcx, var(mcx, 1), true);
        let y = null_test(mcx, var(mcx, 2), true);
        let x2 = null_test(mcx, var(mcx, 1), true);
        // x AND y => x
        let a = and2(mcx, x, y);
        assert!(predicate_implied_by(mcx, &[x2], &[a], false).unwrap());
        // x => x OR y
        let o = Node::mk(
            mcx,
            BoolExpr {
                boolop: BoolExprType::OR_EXPR,
                args: NodeList::make2(mcx, x2, y).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        assert!(predicate_implied_by(mcx, &[o], &[x], false).unwrap());
        // implicit-AND list clause implies each member
        assert!(predicate_implied_by(mcx, &[y], &[x, y], false).unwrap());
        assert!(!predicate_implied_by(mcx, &[x, y], &[y], false).unwrap());
    }

    #[test]
    fn not_clause_refutation() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let x = null_test(mcx, var(mcx, 1), true);
        let x2 = null_test(mcx, var(mcx, 1), true);
        let not_x = Node::mk(
            mcx,
            BoolExpr {
                boolop: BoolExprType::NOT_EXPR,
                args: NodeList::make1(mcx, x2).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        assert!(predicate_refuted_by(mcx, &[not_x], &[x], false).unwrap());
        assert!(predicate_refuted_by(mcx, &[x], &[not_x], false).unwrap());
    }
}
