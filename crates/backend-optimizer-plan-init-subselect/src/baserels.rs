//! JOIN TREES — base-rel construction (initsplan.c) —
//! `add_base_rels_to_query`, `add_other_rels_to_query`.

extern crate alloc;

use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::rawnodes::FromExpr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::PlannerInfo;
use types_pathnodes::{RELOPT_BASEREL, RelOptKind};

use backend_optimizer_util_relnode::build_simple_rel;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

/// `add_base_rels_to_query` (initsplan.c:158).
///
/// Scan the query's jointree and create baserel `RelOptInfo`s for all base
/// relations appearing in the jointree. The top call passes
/// `(Node *) parse->jointree`, the `FromExpr`; the recursion handles the nested
/// `JoinExpr` / `RangeTblRef` arms.
pub fn add_base_rels_to_query<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    jtnode: &FromExpr<'_>,
) -> PgResult<()> {
    for item in jtnode.fromlist.iter() {
        add_base_rels_to_query_node(root, run, item)?;
    }
    Ok(())
}

/// The recursive `Node *` arm of `add_base_rels_to_query`.
fn add_base_rels_to_query_node<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    jtnode: &Node<'_>,
) -> PgResult<()> {
    match jtnode {
        Node::RangeTblRef(rtr) => {
            let varno = rtr.rtindex;
            build_simple_rel(run, root, varno, None)?;
        }
        Node::FromExpr(f) => {
            for item in f.fromlist.iter() {
                add_base_rels_to_query_node(root, run, item)?;
            }
        }
        Node::JoinExpr(j) => {
            if let Some(larg) = j.larg.as_deref() {
                add_base_rels_to_query_node(root, run, larg)?;
            }
            if let Some(rarg) = j.rarg.as_deref() {
                add_base_rels_to_query_node(root, run, rarg)?;
            }
        }
        other => {
            panic!(
                "unrecognized node type: {:?}",
                core::mem::discriminant(other)
            );
        }
    }
    Ok(())
}

/// `add_other_rels_to_query` (initsplan.c:196).
///
/// Create "otherrel" `RelOptInfo`s for the children of appendrel baserels.
pub fn add_other_rels_to_query<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
) -> PgResult<()> {
    for rti in 1..root.simple_rel_array_size {
        let rel_id = match root.simple_rel_array[rti as usize] {
            Some(id) => id,
            // there may be empty slots corresponding to non-baserel RTEs
            None => continue,
        };

        // Ignore any "otherrels" that were already added.
        if root.rel(rel_id).reloptkind != (RELOPT_BASEREL as RelOptKind) {
            continue;
        }

        // If it's marked as inheritable, look for children.
        let (_rtekind, rte_inh, _relkind) = initext::rte_kind_inh_relkind::call(run, root, rti);
        if rte_inh {
            initext::expand_inherited_rtentry::call(run, root, rti)?;
        }
    }
    Ok(())
}
