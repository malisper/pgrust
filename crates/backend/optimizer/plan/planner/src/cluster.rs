//! plan_cluster_use_sort (planner.c): cost seqscan+sort vs full index scan
//! over a mostly-dummy planner state. comparisonCost is 0: an expression
//! index is under-costed vs C's 2*cpu_operator_cost.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::CmdType;
use types_nodes::{Node, NodeList};
use types_pathnodes::PathNode;

use crate::costsize::cost_sort_shape;
use crate::gucs;
use crate::pathnode::{create_index_path, create_seqscan_path};
use crate::plancat::get_rel_data_width;
use crate::relnode::{build_simple_rel, setup_simple_rel_arrays};
use crate::run::PlannerRun;

pub fn plan_cluster_use_sort<'mcx>(
    mcx: Mcx<'mcx>,
    table_oid: Oid,
    index_oid: Oid,
) -> PgResult<bool> {
    if !gucs::enable_indexscan() {
        return Ok(true);
    }

    let rte_node = {
        let mut rte = Node::build::<RangeTblEntry>(mcx)?;
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = table_oid;
        rte.relkind = types_rel::RELKIND_RELATION;
        rte.rellockmode = types_rel::AccessShareLock;
        rte.inh = false;
        rte.seal()
    };
    let query = {
        let mut q = Node::build::<Query>(mcx)?;
        q.commandType = CmdType::CMD_SELECT;
        q.rtable = NodeList::make1(mcx, rte_node)?;
        q.seal().as_query().expect("built Query")
    };

    let mut run = PlannerRun::new(mcx);
    let qid = run.intern_query(query);
    run.root.parse = qid;

    setup_simple_rel_arrays(&mut run.root, 1);
    let rel_id = build_simple_rel(&mut run, 1, RTEKind::RTE_RELATION)?;

    let Some(index) =
        run.root.rel(rel_id).indexlist.iter().copied().find(|i| i.indexoid == index_oid)
    else {
        // No usable IndexOptInfo (indcheckxmin horizon etc.): don't trust the
        // index contents, use seqscan-and-sort.
        return Ok(true);
    };

    let (tuples, pages, width) = {
        let rel = table::table_open(mcx, table_oid, types_rel::NoLock)?;
        let width = get_rel_data_width(&rel, None, run.root.rel(rel_id).min_attr)?;
        rel.close(types_rel::NoLock)?;
        let r = run.root.rel(rel_id);
        (r.tuples, r.pages, width)
    };
    {
        let r = run.root.rel_mut(rel_id);
        r.rows = tuples;
    }
    {
        let pt = run.root.rel(rel_id).pathtarget_id.expect("baserel pathtarget");
        run.root.pathtarget_mut(pt).width = width;
    }
    run.root.total_table_pages = pages as f64;

    // cost_qual_eval over ii_Expressions: structurally empty on this lane.
    let comparison_cost = 0.0;

    let seq_id = create_seqscan_path(&mut run, rel_id, &crate::relnode::RELIDS_UNSET, 0)?;
    let (seq_disabled, seq_total) = {
        let p = run.root.path(seq_id).base();
        (p.disabled_nodes, p.total_cost)
    };
    let (_sort_disabled, _sort_startup, sort_total) = cost_sort_shape(
        seq_disabled,
        seq_total,
        tuples,
        width,
        comparison_cost,
        init_small::globals::maintenance_work_mem(),
        -1.0,
    );

    let index_path_id = create_index_path(
        &mut run,
        index,
        mcx::PgVec::new_in(mcx),
        mcx::PgVec::new_in(mcx),
        mcx::PgVec::new_in(mcx),
        mcx::PgVec::new_in(mcx),
        1, // ForwardScanDirection
        false,
        &crate::relnode::RELIDS_UNSET,
        1.0,
        false,
    )?;
    let index_total = match run.root.path(index_path_id) {
        PathNode::IndexPath(ip) => ip.path.total_cost,
        other => panic!("create_index_path returned {other:?}"),
    };

    Ok(sort_total < index_total)
}
