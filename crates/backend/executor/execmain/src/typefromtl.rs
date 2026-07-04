use std::rc::Rc;

use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_tuple::TupleDescData;

pub use ::execscan::{expr_collation, expr_typmod};

// Canonical ExecTypeFromTL lives in execscan (home unit execTuples cannot dep
// execexpr); these bind it to the backend-lifetime desc context.
pub fn exec_type_from_tl(target_list: &NodeList<'_>) -> PgResult<Rc<TupleDescData<'static>>> {
    execscan::exec_type_from_tl(crate::desc_mcx(), target_list)
}

pub fn exec_clean_type_from_tl(target_list: &NodeList<'_>) -> PgResult<Rc<TupleDescData<'static>>> {
    execscan::exec_clean_type_from_tl(crate::desc_mcx(), target_list)
}

pub fn exec_type_from_expr_list(exprs: &NodeList<'_>) -> PgResult<Rc<TupleDescData<'static>>> {
    execscan::exec_type_from_expr_list(crate::desc_mcx(), exprs)
}

pub(crate) fn exec_clean_type_from_tl_seam(
    pstmt: &PlannedStmt<'_>,
) -> PgResult<Rc<TupleDescData<'static>>> {
    let plan = pstmt
        .planTree
        .and_then(Node::as_plan)
        .expect("PlannedStmt without planTree");
    exec_clean_type_from_tl(&plan.targetlist)
}
