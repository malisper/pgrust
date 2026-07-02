use mcx::{alloc_leak_in, Mcx};
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Alias, FromExpr};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

// Empty FROM becomes a dummy RTE_RESULT + RangeTblRef. C mutates the FromExpr
// in place; jointree is a shared ref here, so an equivalent one is rebuilt.
pub fn replace_empty_jointree<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let quals = match parse.jointree {
        Some(f) if f.fromlist.is_nil() => f.quals,
        Some(_) => return Ok(()),
        None => None,
    };
    if parse.setOperations.is_some() {
        return Ok(());
    }

    let eref = alloc_leak_in(
        mcx,
        Alias { aliasname: Some("*RESULT*"), colnames: NodeList::nil() },
    )?;
    let mut rte = Node::build::<RangeTblEntry>(mcx)?;
    rte.rtekind = RTEKind::RTE_RESULT;
    rte.eref = Some(eref);
    parse.rtable.lappend(mcx, rte.seal())?;
    let rti = parse.rtable.len() as i32;

    let rtr = Node::mk_range_tbl_ref(mcx, rti)?;
    let fromlist = NodeList::make1(mcx, rtr)?;
    parse.jointree = Some(alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    Ok(())
}

// A lone RangeTblRef under the top FromExpr can never be elided or dropped.
pub fn remove_useless_result_rtes(run: &PlannerRun<'_>, parse: &Query<'_>) {
    let f = parse.jointree.expect("top jointree is a FromExpr");
    if f.fromlist.len() == 1
        && f.fromlist.nth(0).node_tag() == NodeTag::T_RangeTblRef
    {
        debug_assert!(run.root.rowMarks.is_empty());
        return;
    }
    panic!(
        "remove_useless_results_recurse (prepjointree.c): non-trivial jointree; \
         M2 join lane"
    );
}

// SELECT without locking clauses needs no rowmarks.
pub fn preprocess_rowmarks(parse: &Query<'_>) {
    if !parse.rowMarks.is_nil() {
        panic!("preprocess_rowmarks (planner.c): FOR UPDATE/SHARE rowmarks; M2 lane");
    }
    if !matches!(parse.commandType, CmdType::CMD_SELECT | CmdType::CMD_INSERT) {
        panic!("preprocess_rowmarks (planner.c): UPDATE/DELETE/MERGE rowmarks; M2 DML lane");
    }
}

// No-result-relation arm: processed tlist = parse targetList (C shares it).
pub fn preprocess_targetlist<'mcx>(run: &mut PlannerRun<'mcx>) {
    let parse = run.parse();
    if parse.resultRelation != 0 {
        panic!(
            "preprocess_targetlist (preptlist.c): result relation (table_open/\
             expand_insert_targetlist); M2 DML lane"
        );
    }
    debug_assert!(parse.commandType == CmdType::CMD_SELECT);
    debug_assert!(run.root.rowMarks.is_empty());
    run.processed_tlist = Some(&parse.targetList);
}
