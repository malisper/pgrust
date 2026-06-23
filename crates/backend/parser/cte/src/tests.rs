//! Unit tests for the CTE parse-analysis logic.
//!
//! These exercise the pure in-crate logic — duplicate-name rejection, the
//! dependency graph + topological sort, the recursion well-formedness checks,
//! and `analyzeCTETargetList` column derivation — none of which reach the
//! unported `parse_sub_analyze` / `transformExpr` paths. Nodes are built
//! directly over the owned `nodes` tree.

use super::*;
use ::mcx::MemoryContext;
use std::sync::Once;

use ::nodes::primnodes::Const;
use ::nodes::rawnodes::{CTEMaterialize, RangeVar, RangeTblRef, SetOperationStmt};
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::INT4OID;

// `parser_errposition` (parse_node.c, owned by parser-small1) is reached by the
// error paths; install the real seam once for the suite.
static INIT: Once = Once::new();
fn install_seams() {
    INIT.call_once(|| {
        small1::init_seams();
    });
}

// --- node builders -------------------------------------------------------

fn rangevar<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<Node<'mcx>> {
    Ok(Node::RangeVar(RangeVar {
        catalogname: None,
        schemaname: None,
        relname: Some(PgString::from_str_in(name, mcx)?),
        inh: true,
        relpersistence: b'p' as i8,
        alias: None,
        location: -1,
    }))
}

fn empty_select<'mcx>(mcx: Mcx<'mcx>) -> SelectStmt<'mcx> {
    SelectStmt {
        distinctClause: PgVec::new_in(mcx),
        intoClause: None,
        targetList: PgVec::new_in(mcx),
        fromClause: PgVec::new_in(mcx),
        whereClause: None,
        groupClause: PgVec::new_in(mcx),
        groupDistinct: false,
        havingClause: None,
        windowClause: PgVec::new_in(mcx),
        valuesLists: PgVec::new_in(mcx),
        sortClause: PgVec::new_in(mcx),
        limitOffset: None,
        limitCount: None,
        limitOption: Default::default(),
        lockingClause: PgVec::new_in(mcx),
        withClause: None,
        op: SetOperation::SETOP_NONE,
        all: false,
        larg: None,
        rarg: None,
    }
}

/// A `SELECT ... UNION ... FROM <fromrel>` SelectStmt: the recursive-CTE shape
/// (a top-level UNION whose right term references `selfref`).
fn union_select<'mcx>(
    mcx: Mcx<'mcx>,
    nonrec_from: &str,
    rec_from: &str,
    all: bool,
) -> PgResult<SelectStmt<'mcx>> {
    let mut larg = empty_select(mcx);
    larg.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, nonrec_from)?)?);
    let mut rarg = empty_select(mcx);
    rarg.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, rec_from)?)?);
    let mut top = empty_select(mcx);
    top.op = SetOperation::SETOP_UNION;
    top.all = all;
    top.larg = Some(::mcx::alloc_in(mcx, larg)?);
    top.rarg = Some(::mcx::alloc_in(mcx, rarg)?);
    Ok(top)
}

fn cte<'mcx>(mcx: Mcx<'mcx>, name: &str, query: Node<'mcx>) -> PgResult<CommonTableExpr<'mcx>> {
    Ok(CommonTableExpr {
        ctename: Some(PgString::from_str_in(name, mcx)?),
        aliascolnames: PgVec::new_in(mcx),
        ctematerialized: CTEMaterialize::CTEMaterializeDefault,
        ctequery: Some(::mcx::alloc_in(mcx, query)?),
        search_clause: None,
        cycle_clause: None,
        location: 0,
        cterecursive: false,
        cterefcount: 0,
        ctecolnames: PgVec::new_in(mcx),
        ctecoltypes: PgVec::new_in(mcx),
        ctecoltypmods: PgVec::new_in(mcx),
        ctecolcollations: PgVec::new_in(mcx),
    })
}

fn with_clause<'mcx>(
    mcx: Mcx<'mcx>,
    recursive: bool,
    ctes: Vec<CommonTableExpr<'mcx>>,
) -> PgResult<WithClause<'mcx>> {
    let mut v: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    for c in ctes {
        v.push(::mcx::alloc_in(mcx, Node::CommonTableExpr(c))?);
    }
    Ok(WithClause {
        ctes: v,
        recursive,
        location: -1,
    })
}

// --- transformWithClause: duplicate name ---------------------------------

#[test]
fn duplicate_cte_name_rejected() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    let c1 = cte(mcx, "a", Node::SelectStmt(empty_select(mcx))).unwrap();
    let c2 = cte(mcx, "a", Node::SelectStmt(empty_select(mcx))).unwrap();
    let wc = with_clause(mcx, false, alloc::vec![c1, c2]).unwrap();

    let err = transformWithClause(mcx, &mut pstate, wc).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_ALIAS);
}

// --- makeDependencyGraphWalker + TopologicalSort -------------------------

/// Build a recursive `CteState` over the given CTEs (as `transformWithClause`
/// does for the WITH RECURSIVE path) and run the dependency graph.
fn build_and_sort<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    ctes: Vec<CommonTableExpr<'mcx>>,
) -> PgResult<(Vec<String>, Vec<bool>)> {
    let numitems = ctes.len();
    let mut items: Vec<CteItem> = Vec::new();
    for i in 0..numitems {
        items.push(CteItem {
            cte: i,
            id: i as i32,
            depends_on: alloc::collections::BTreeSet::new(),
        });
    }
    let mut cstate = CteState {
        ctes,
        items,
        curitem: 0,
        innerwiths: Vec::new(),
        selfrefcount: 0,
        context: RecursionContext::Ok,
    };
    makeDependencyGraph(mcx, pstate, &mut cstate)?;
    let order: Vec<String> = cstate
        .items
        .iter()
        .map(|it| cte_name(&cstate.ctes[it.cte]).to_string())
        .collect();
    let recursive: Vec<bool> = cstate.ctes.iter().map(|c| c.cterecursive).collect();
    Ok((order, recursive))
}

#[test]
fn dependency_graph_orders_forward_refs() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // CTE "a" references "b"; "b" has no deps. Safe order: b before a.
    let mut a_q = empty_select(mcx);
    a_q.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, "b").unwrap()).unwrap());
    let a = cte(mcx, "a", Node::SelectStmt(a_q)).unwrap();
    let b = cte(mcx, "b", Node::SelectStmt(empty_select(mcx))).unwrap();

    let (order, _rec) = build_and_sort(mcx, &mut pstate, alloc::vec![a, b]).unwrap();
    assert_eq!(order, alloc::vec!["b".to_string(), "a".to_string()]);
}

#[test]
fn self_reference_marks_recursive() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // CTE "r" references itself -> cterecursive = true, no cross-deps.
    let mut r_q = empty_select(mcx);
    r_q.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, "r").unwrap()).unwrap());
    let r = cte(mcx, "r", Node::SelectStmt(r_q)).unwrap();

    let (order, rec) = build_and_sort(mcx, &mut pstate, alloc::vec![r]).unwrap();
    assert_eq!(order, alloc::vec!["r".to_string()]);
    assert_eq!(rec, alloc::vec![true]);
}

#[test]
fn mutual_recursion_rejected() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // a -> b and b -> a: mutual recursion, no acyclic order exists.
    let mut a_q = empty_select(mcx);
    a_q.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, "b").unwrap()).unwrap());
    let a = cte(mcx, "a", Node::SelectStmt(a_q)).unwrap();
    let mut b_q = empty_select(mcx);
    b_q.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, "a").unwrap()).unwrap());
    let b = cte(mcx, "b", Node::SelectStmt(b_q)).unwrap();

    let err = build_and_sort(mcx, &mut pstate, alloc::vec![a, b]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
}

// --- checkWellFormedRecursion -------------------------------------------

/// Run checkWellFormedRecursion over a single self-referential CTE.
fn check_recursion<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    name: &str,
    query: Node<'mcx>,
) -> PgResult<()> {
    let mut c = cte(mcx, name, query).unwrap();
    c.cterecursive = true;
    let mut cstate = CteState {
        ctes: alloc::vec![c],
        items: alloc::vec![CteItem {
            cte: 0,
            id: 0,
            depends_on: alloc::collections::BTreeSet::new(),
        }],
        curitem: 0,
        innerwiths: Vec::new(),
        selfrefcount: 0,
        context: RecursionContext::Ok,
    };
    checkWellFormedRecursion(mcx, pstate, &mut cstate)
}

#[test]
fn recursive_query_must_be_union() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // A self-referential CTE whose top is a plain SELECT (no UNION) is rejected.
    let mut q = empty_select(mcx);
    q.fromClause.push(::mcx::alloc_in(mcx, rangevar(mcx, "r").unwrap()).unwrap());
    let err = check_recursion(mcx, &mut pstate, "r", Node::SelectStmt(q)).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_RECURSION);
}

#[test]
fn recursive_reference_in_nonrecursive_term_rejected() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // non-recursive term (larg) references "r" -> RECURSION_NONRECURSIVETERM.
    let q = union_select(mcx, "r", "r", true).unwrap();
    let err = check_recursion(mcx, &mut pstate, "r", Node::SelectStmt(q)).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_RECURSION);
}

#[test]
fn well_formed_recursion_accepted() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // non-recursive term references a base table, recursive term references "r"
    // exactly once: the canonical well-formed recursive CTE.
    let q = union_select(mcx, "base", "r", true).unwrap();
    check_recursion(mcx, &mut pstate, "r", Node::SelectStmt(q)).unwrap();
}

// --- analyzeCTETargetList ------------------------------------------------

fn int_te<'mcx>(mcx: Mcx<'mcx>, resno: i16, name: &str) -> PgResult<TargetEntry<'mcx>> {
    let c = Expr::Const(Const {
        consttype: INT4OID,
        consttypmod: -1,
        constcollid: InvalidOid,
        constlen: 4,
        constvalue: Datum::null(),
        constisnull: true,
        constbyval: true,
        location: -1,
    });
    Ok(TargetEntry {
        expr: Some(::mcx::alloc_in(mcx, c)?),
        resno,
        resname: Some(PgString::from_str_in(name, mcx)?),
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk: false,
    })
}

#[test]
fn target_list_derives_columns_from_tlist() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    let mut c = cte(mcx, "q", Node::SelectStmt(empty_select(mcx))).unwrap();
    let tlist = alloc::vec![
        int_te(mcx, 1, "x").unwrap(),
        int_te(mcx, 2, "y").unwrap(),
    ];
    analyzeCTETargetList(mcx, &mut pstate, &mut c, &tlist).unwrap();

    let names: Vec<&str> = c.ctecolnames.iter().map(|n| str_val(n)).collect();
    assert_eq!(names, alloc::vec!["x", "y"]);
    assert_eq!(c.ctecoltypes.len(), 2);
    assert!(c.ctecoltypes.iter().all(|&t| t == INT4OID));
}

#[test]
fn target_list_too_many_aliases_rejected() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    install_seams();
    let mut pstate = ParseState::new(mcx).unwrap();

    // One output column, but two alias names declared -> error.
    let mut c = cte(mcx, "q", Node::SelectStmt(empty_select(mcx))).unwrap();
    c.aliascolnames.push(make_string_node(mcx, "a").unwrap());
    c.aliascolnames.push(make_string_node(mcx, "b").unwrap());
    let tlist = alloc::vec![int_te(mcx, 1, "x").unwrap()];

    let err = analyzeCTETargetList(mcx, &mut pstate, &mut c, &tlist).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_COLUMN_REFERENCE);
}

// --- expandability: SEARCH/CYCLE left/right must be RangeTblRef -----------

#[test]
fn range_tbl_ref_detection() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rtr = Node::RangeTblRef(RangeTblRef { rtindex: 1 });
    assert!(is_range_tbl_ref(Some(&rtr)));
    let sel = Node::SelectStmt(empty_select(mcx));
    assert!(!is_range_tbl_ref(Some(&sel)));
    // SetOperationStmt construction sanity (used by analyzeCTE's expandability
    // check) — just confirm the variant is reachable.
    let sos = SetOperationStmt {
        op: SetOperation::SETOP_UNION,
        all: false,
        larg: Some(::mcx::alloc_in(mcx, Node::RangeTblRef(RangeTblRef { rtindex: 1 })).unwrap()),
        rarg: Some(::mcx::alloc_in(mcx, Node::RangeTblRef(RangeTblRef { rtindex: 2 })).unwrap()),
        colTypes: PgVec::new_in(mcx),
        colTypmods: PgVec::new_in(mcx),
        colCollations: PgVec::new_in(mcx),
        groupClauses: PgVec::new_in(mcx),
    };
    assert!(is_range_tbl_ref(sos.larg.as_deref()));
    assert!(is_range_tbl_ref(sos.rarg.as_deref()));
}
