use super::*;
use types_nodes::primnodes::Var;

// indxpath.c:4470-4479: an expression index column with no matching indexprs
// entry is elog(ERROR) "wrong number of index expressions" (XX000,
// catchable), never a panic; a present expression matches by equal().
#[test]
fn match_index_to_operand_reports_missing_index_expression() {
    let ctx = mcx::MemoryContext::new_bump("indxpath-test");
    let mcx = ctx.mcx();
    let mut run = PlannerRun::new(mcx);
    let operand = Node::mk(mcx, Var { varno: 1, varattno: 1, vartype: 23, ..Default::default() }).unwrap();
    let mut index = IndexOptInfo::new(mcx);
    index.ncolumns = 1;
    index.nkeycolumns = 1;
    index.indexkeys.push(0);
    let err = match_index_to_operand(&run, operand, 0, &index)
        .expect_err("C elog(ERROR)s on a missing index expression");
    assert_eq!(err.message(), "wrong number of index expressions");
    let expr = Node::mk(mcx, Var { varno: 1, varattno: 1, vartype: 23, ..Default::default() }).unwrap();
    let id = run.root.alloc_expr_node(expr);
    index.indexprs.push(id);
    assert!(match_index_to_operand(&run, operand, 0, &index).unwrap());
}
