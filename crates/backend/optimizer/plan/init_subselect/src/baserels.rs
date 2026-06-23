//! JOIN TREES — base-rel construction (initsplan.c) —
//! `add_base_rels_to_query`, `add_other_rels_to_query`.

extern crate alloc;

use ::types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::rawnodes::FromExpr;
use ::pathnodes::planner_run::PlannerRun;
use ::pathnodes::PlannerInfo;
use pathnodes::{RELOPT_BASEREL, RelOptKind};

use ::relnode::build_simple_rel;
use init_subselect_ext_seams as initext;

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
    match jtnode.node_tag() {
        ::nodes::nodes::ntag::T_RangeTblRef => {
            let rtr = jtnode.expect_rangetblref();
            let varno = rtr.rtindex;
            build_simple_rel(run, root, varno, None)?;
        }
        ::nodes::nodes::ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            for item in f.fromlist.iter() {
                add_base_rels_to_query_node(root, run, item)?;
            }
        }
        ::nodes::nodes::ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            if let Some(larg) = j.larg.as_deref() {
                add_base_rels_to_query_node(root, run, larg)?;
            }
            if let Some(rarg) = j.rarg.as_deref() {
                add_base_rels_to_query_node(root, run, rarg)?;
            }
        }
        _ => {
            panic!(
                "unrecognized node type: {:?}",
                jtnode.node_tag()
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
    run: &mut PlannerRun<'mcx>,
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
