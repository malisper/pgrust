#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! `backend/optimizer/prep/prepunion.c` — planning of set-operation queries
//! (`UNION`/`INTERSECT`/`EXCEPT`, including recursive `UNION`).
//!
//! Idiomatic arena-model port of PostgreSQL 18.3 `prepunion.c`. The C shares
//! `Path *`/`RelOptInfo *` pointers across the leaf subqueries' `PlannerInfo`s;
//! this port keeps each leaf's paths in its own subroot arena and brings the
//! chosen ones into the outer root's arena with
//! [`import_path_from_subroot`](backend_optimizer_util_pathnode::import_path_from_subroot).

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::nodes::{ntag, Node};
use types_nodes::primnodes::Expr;
use types_nodes::rawnodes::{SetOperation, SetOperationStmt, SortGroupClause};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    NodeId, PathId, PlannerInfo, RelId, Relids, TargetEntryNode, UPPERREL_FINAL, UPPERREL_SETOP,
};

use backend_nodes_core::makefuncs::make_var;
use backend_nodes_core::nodefuncs::{apply_relabel_type, expr_collation, expr_type, expr_typmod};

use backend_optimizer_path_costsize::sizeest::set_subquery_size_estimates;
use backend_optimizer_path_pathkeys as pathkeys;
use backend_optimizer_util_pathnode as pathnode;
use backend_optimizer_util_pathnode::import::import_path_from_subroot;
use backend_optimizer_util_relnode as relnode;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_util_vars::tlist;

/// Borrow the `Node` behind an `Option<NodePtr>` (`PgBox<Node>` is not a std
/// `Box`, so `Option::as_deref` does not apply).
#[inline]
fn node_ref<'a, 'mcx>(opt: &'a Option<types_nodes::nodes::NodePtr<'mcx>>) -> Option<&'a Node<'mcx>> {
    opt.as_ref().map(|b| &**b)
}

/// `COERCE_IMPLICIT_CAST` (`primnodes.h`'s `CoercionForm`).
const COERCE_IMPLICIT_CAST: types_nodes::primnodes::CoercionForm =
    types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST;

/// `RTE_SUBQUERY` (`parsenodes.h`).
const RTE_SUBQUERY: u32 = 1;

/// `TOTAL_COST` (`pathnodes.h` `CostSelector`).
const TOTAL_COST: types_pathnodes::optimizer_plan::CostSelector =
    types_pathnodes::optimizer_plan::CostSelector::TOTAL_COST;

// ===========================================================================
// Seam install.
// ===========================================================================

/// Install the `plan_set_operations` seam (called from `seams-init`).
pub fn init_seams() {
    backend_optimizer_prep_prepunion_seams::plan_set_operations::set(plan_set_operations_seam);
}

fn plan_set_operations_seam<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<RelId> {
    plan_set_operations(mcx, run, root)
}

// ===========================================================================
// plan_set_operations  (prepunion.c:92)
// ===========================================================================

/// `plan_set_operations(root)` — plan the queries for a tree of set operations
/// and return the upper `RelOptInfo` holding the result paths;
/// `root.processed_tlist` is filled as a side effect.
pub fn plan_set_operations<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<RelId> {
    // topop = castNode(SetOperationStmt, parse->setOperations).
    let topop = clone_setop_top(run, root)?;

    debug_assert!(topop.op != SetOperation::SETOP_NONE);

    // Mark EC merging complete (C:120).
    root.ec_merging_done = true;

    // Prepare simple_rel_array / simple_rte_array (C:127).
    relnode::setup_simple_rel_arrays(run, root, mcx)?;

    // Find the leftmost component Query (C:133-139) for its column names.
    let leftmost_rtindex = leftmost_rtindex(&topop);
    let leftmost_tlist = leftmost_query_tlist(mcx, run, root, leftmost_rtindex)?;

    let top_tlist: Vec<NodeId>;
    let setop_rel: RelId;

    if root.hasRecursion {
        let (rel, tlist) = generate_recursion_path(mcx, run, root, &topop, &leftmost_tlist)?;
        setop_rel = rel;
        top_tlist = tlist;
    } else {
        // recurse_set_operations on the top node (C:159).
        let mut p_target_list: Vec<NodeId> = Vec::new();
        let mut trivial_tlist = true;
        setop_rel = recurse_set_operations(
            mcx,
            run,
            root,
            &SetOpNode::Stmt(topop.clone_in(mcx)?),
            None,
            &topop.colTypes,
            &topop.colCollations,
            &leftmost_tlist,
            &mut p_target_list,
            &mut trivial_tlist,
        )?;
        top_tlist = p_target_list;
    }

    // root->processed_tlist = top_tlist (C:168).
    root.processed_tlist = top_tlist;

    Ok(setop_rel)
}

/// A step in the setop tree, decoded from the owned `Node` tree: either a leaf
/// `RangeTblRef` (carry its rtindex) or a `SetOperationStmt`.
enum SetOpNode<'mcx> {
    Leaf(i32),
    Stmt(SetOperationStmt<'mcx>),
}

/// Decode a `Node*` (`larg`/`rarg`) into a [`SetOpNode`], deep-cloning the
/// `SetOperationStmt` into `mcx` (the owned tree the caller can keep).
fn decode_setop_node<'mcx>(mcx: Mcx<'mcx>, node: &Node<'mcx>) -> PgResult<SetOpNode<'mcx>> {
    match node.node_tag() {
        ntag::T_RangeTblRef => Ok(SetOpNode::Leaf(node.expect_rangetblref().rtindex)),
        ntag::T_SetOperationStmt => {
            Ok(SetOpNode::Stmt(node.expect_setoperationstmt().clone_in(mcx)?))
        }
        _ => Err(PgError::error(alloc::format!(
            "unrecognized node type: {}",
            node.node_tag().0
        ))),
    }
}

/// Clone the top `SetOperationStmt` out of `parse->setOperations`.
fn clone_setop_top<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
) -> PgResult<SetOperationStmt<'mcx>> {
    let parse = run.resolve(root.parse);
    let mcx = run.mcx();
    match node_ref(&parse.setOperations).and_then(|n| n.as_setoperationstmt()) {
        Some(op) => op.clone_in(mcx),
        _ => Err(PgError::error(String::from(
            "plan_set_operations: parse->setOperations is not a SetOperationStmt",
        ))),
    }
}

/// Walk down `topop->larg` chains to the leftmost `RangeTblRef`'s rtindex
/// (C:133-137).
fn leftmost_rtindex(topop: &SetOperationStmt<'_>) -> i32 {
    let mut node = node_ref(&topop.larg);
    loop {
        match node.map(|n| n.node_tag()) {
            Some(ntag::T_SetOperationStmt) => {
                node = node_ref(&node.unwrap().expect_setoperationstmt().larg)
            }
            Some(ntag::T_RangeTblRef) => return node.unwrap().expect_rangetblref().rtindex,
            _ => panic!("plan_set_operations: leftmost node is not a RangeTblRef"),
        }
    }
}

/// The leftmost subquery's `targetList`, materialized into `root`'s arena as
/// `TargetEntryNode`s (used as `refnames_tlist` for column names). Mirrors C's
/// `leftmostQuery->targetList`.
fn leftmost_query_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rtindex: i32,
) -> PgResult<Vec<NodeId>> {
    let rte_id = root.simple_rte_array[rtindex as usize];
    // Materialize (expr, resno, resname, resjunk, ressortgroupref) out of the
    // interned RTE's subquery tlist, then alloc into root's arena.
    let mut decoded: Vec<(Expr, AttrNumber, Option<String>, bool, Index)> = Vec::new();
    {
        let rte = run.resolve_rte(rte_id);
        let subquery = rte
            .subquery
            .as_ref()
            .expect("plan_set_operations: leftmost RTE has no subquery");
        decoded.reserve(subquery.targetList.len());
        for te in subquery.targetList.iter() {
            let expr = te
                .expr
                .as_ref()
                .expect("leftmost tlist entry has NULL expr")
                .clone_in(mcx)?;
            decoded.push((
                expr,
                te.resno,
                te.resname.as_ref().map(|s| String::from(s.as_str())),
                te.resjunk,
                te.ressortgroupref,
            ));
        }
    }
    let mut tlist: Vec<NodeId> = Vec::with_capacity(decoded.len());
    for (expr, resno, resname, resjunk, ressortgroupref) in decoded {
        let expr_id = root.alloc_node(expr);
        let te = TargetEntryNode {
            expr: expr_id,
            resno,
            resname,
            ressortgroupref,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk,
        };
        tlist.push(root.alloc_targetentry(te));
    }
    Ok(tlist)
}

// ===========================================================================
// recurse_set_operations  (prepunion.c:208)
// ===========================================================================

fn recurse_set_operations<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    set_op: &SetOpNode<'mcx>,
    parent_op: Option<&SetOperationStmt<'mcx>>,
    col_types: &[Oid],
    col_collations: &[Oid],
    refnames_tlist: &[NodeId],
    p_target_list: &mut Vec<NodeId>,
    istrivial_tlist: &mut bool,
) -> PgResult<RelId> {
    *istrivial_tlist = true;

    match set_op {
        SetOpNode::Leaf(rtindex) => {
            let rtindex = *rtindex;
            // build_simple_rel for the leaf subquery (C:235).
            let rel = relnode::build_simple_rel(run, root, rtindex, None)?;

            // Plan the subquery, threading the shared glob through subroot.
            let subroot = plan_leaf_subquery(mcx, run, root, rtindex)?;

            // Figure out the appropriate target list (C:257).
            let mut trivial_tlist = true;
            let tlist = generate_setop_tlist(
                mcx,
                root,
                &subroot,
                col_types,
                col_collations,
                rtindex as Index,
                true,
                &subroot.processed_tlist,
                refnames_tlist,
                &mut trivial_tlist,
            )?;

            // rel->reltarget = create_pathtarget(root, tlist) (C:263).
            set_rel_reltarget_from_tlist(root, rel, &tlist);

            // Store subroot into the rel.
            root.rel_mut(rel).subroot.0 = Some(Box::new(subroot));

            *p_target_list = tlist;
            *istrivial_tlist = trivial_tlist;
            Ok(rel)
        }
        SetOpNode::Stmt(op) => {
            let rel = if op.op == SetOperation::SETOP_UNION {
                generate_union_paths(mcx, run, root, op, refnames_tlist, p_target_list)?
            } else {
                generate_nonunion_paths(mcx, run, root, op, refnames_tlist, p_target_list)?
            };

            // Result-projection stanza (C:296-339). For matched-type set-ops the
            // pathtarget already matches; the coercion/relabel reprojection is a
            // not-yet-ported leg.
            let same_types = tlist_same_datatypes_ids(root, p_target_list, col_types)?;
            let same_colls = tlist_same_collations_ids(root, p_target_list, col_collations)?;
            if !same_types || !same_colls {
                return Err(PgError::error(String::from(
                    "recurse_set_operations: set-op result reprojection (apply_projection_to_path \
                     over a coerced/relabeled tlist) is not yet ported",
                )));
            }

            postprocess_setop_rel(root, rel)?;
            let _ = parent_op;
            Ok(rel)
        }
    }
}

/// Plan one leaf subquery via the planner seam, threading the shared glob.
fn plan_leaf_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rtindex: i32,
) -> PgResult<PlannerInfo> {
    // Intern the leaf subquery Query into the run.
    let subquery_id = {
        let rte_id = root.simple_rte_array[rtindex as usize];
        let subquery = run
            .resolve_rte(rte_id)
            .subquery
            .as_deref()
            .expect("recurse_set_operations: leaf RTE has no subquery")
            .clone_in(mcx)?;
        run.intern(subquery)
    };

    // Move the shared glob out of the outer root and into the planner; the
    // returned subroot carries the (mutated) glob, which we move back out.
    let glob = *root
        .glob
        .take()
        .expect("recurse_set_operations: outer root has no glob");
    // When planning the recursive term of a recursive UNION, C passes the
    // recursion-planning `root` as the leaf's `parent_root` so the self-reference
    // WorkTableScan can read `cteroot->non_recursive_path` and `wt_param_id`.
    // PlannerInfo is not `Clone` here, so instead stamp the two values the
    // worktable scan needs (the work-table param id and the non-recursive term's
    // row estimate) onto the leaf subroot before its access paths are built.
    let recursion_carry: Option<(i32, f64)> =
        if root.hasRecursion && root.non_recursive_path.is_some() {
            let nrp = root.non_recursive_path.unwrap();
            Some((root.wt_param_id, root.path(nrp).base().rows))
        } else {
            None
        };

    let tuple_fraction = root.tuple_fraction;
    // C passes the recursion-planning `root` as the leaf's `parent_root`. Move it
    // in by value and recover it from `subroot.parent_root` afterwards.
    let parent_root = core::mem::take(root);
    let mut subroot = backend_optimizer_plan_planner_seams::subquery_planner_for_setop::call(
        mcx,
        run,
        glob,
        subquery_id,
        parent_root,
        recursion_carry,
        false,
        tuple_fraction,
    )?;
    *root = *subroot
        .parent_root
        .take()
        .expect("recurse_set_operations: subroot lost its parent_root");
    // Move the accumulated glob back to the outer root.
    root.glob = subroot.glob.take();
    Ok(subroot)
}

// ===========================================================================
// generate_union_paths  (prepunion.c:675)  — UNION / UNION ALL
// ===========================================================================

fn generate_union_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    op: &SetOperationStmt<'mcx>,
    refnames_tlist: &[NodeId],
    p_target_list: &mut Vec<NodeId>,
) -> PgResult<RelId> {
    // Pull up identical UNION children + plan the leaf queries (C:706).
    let mut tlist_list: Vec<Vec<NodeId>> = Vec::new();
    let mut trivial_tlist_list: Vec<bool> = Vec::new();
    let rellist = plan_union_children(
        mcx,
        run,
        root,
        op,
        refnames_tlist,
        &mut tlist_list,
        &mut trivial_tlist_list,
    )?;

    // Generate tlist for the Append (C:719).
    let tlist = generate_append_tlist(
        root,
        &op.colTypes,
        &op.colCollations,
        &tlist_list,
        refnames_tlist,
    )?;
    *p_target_list = tlist.clone();

    // For UNIONs (not ALL), identify grouping semantics (C:724-738).
    let mut group_list: Vec<NodeId> = Vec::new();
    if !op.all {
        group_list = generate_setop_grouplist(mcx, root, op, &tlist)?;
        // Sorting feasibility checked below via grouping_is_sortable.
    }

    // Build the union child paths (C:744-754). We pass union_pathkeys = NIL
    // (cheapest-path-only leg; the per-child sorted-path leg is the
    // interesting_pathkeys != NIL branch, not on this first cut).
    for (i, &rel) in rellist.iter().enumerate() {
        let trivial = trivial_tlist_list[i];
        let child_tlist = tlist_list[i].clone();
        if root.rel(rel).rtekind == RTE_SUBQUERY {
            build_setop_child_paths(mcx, run, root, rel, trivial, &child_tlist, &[], None)?;
        }
    }

    // Build path lists and relid set (C:757-802).
    let mut cheapest_pathlist: Vec<PathId> = Vec::with_capacity(rellist.len());
    let mut relids: Relids = None;
    for &rel in rellist.iter() {
        let cheapest = root
            .rel(rel)
            .cheapest_total_path
            .expect("generate_union_paths: union child has no cheapest_total_path");
        cheapest_pathlist.push(cheapest);
        relids = bms::relids_union::call(&relids, &root.rel(rel).relids);
    }

    // Build result relation (C:805-808).
    let result_rel = relnode::fetch_upper_rel(root, UPPERREL_SETOP, &relids);
    set_rel_reltarget_from_tlist(root, result_rel, &tlist);
    root.rel_mut(result_rel).consider_parallel = false;
    root.rel_mut(result_rel).consider_startup = root.tuple_fraction > 0.0;

    // Append the children together using cheapest paths (C:814).
    let apath = pathnode::create::create_append_path(
        root,
        run,
        true,
        result_rel,
        cheapest_pathlist,
        Vec::new(),
        Vec::new(),
        &None,
        0,
        false,
        -1.0,
    )?;
    let apath_rows = root.path(apath).base().rows;
    root.rel_mut(result_rel).rows = apath_rows;

    if !op.all {
        let d_num_groups = apath_rows;
        let group_clauses_owned: Vec<SortGroupClause> =
            group_list.iter().map(|&id| *root.sortgroupclause(id)).collect();
        let can_sort = tlist::grouping_is_sortable(&group_clauses_owned);
        let can_hash = tlist::grouping_is_hashable(&group_clauses_owned);

        if can_hash {
            // Hash-aggregate the Append path (C:892).
            let target = make_pathtarget(root, &tlist);
            let path = pathnode::create::create_agg_path(
                run,
                root,
                result_rel,
                apath,
                Box::new(target),
                types_pathnodes::AGG_HASHED,
                AGGSPLIT_SIMPLE,
                group_list.clone(),
                Vec::new(),
                None,
                d_num_groups,
            )?;
            pathnode::add_path(root, result_rel, path)?;
        }

        if can_sort {
            // Sort -> Unique on the Append path (C:922-938).
            let mut path = apath;
            if !group_list.is_empty() {
                let pk =
                    pathkeys::make_pathkeys_for_sortclauses(root, &group_list, &tlist);
                path = pathnode::create::create_sort_path(root, result_rel, path, pk, -1.0)?;
            }
            let num_cols = root.path(path).base().pathkeys.len() as i32;
            let path =
                pathnode::create::create_upper_unique_path(root, result_rel, path, num_cols, d_num_groups)?;
            pathnode::add_path(root, result_rel, path)?;
        }

        if !can_sort && !can_hash {
            return Err(PgError::error(String::from(
                "could not implement UNION: all column datatypes must be sortable or hashable",
            )));
        }
    } else {
        // UNION ALL (C:984-988).
        pathnode::add_path(root, result_rel, apath)?;
    }

    Ok(result_rel)
}

// ===========================================================================
// generate_nonunion_paths  (prepunion.c:997)  — INTERSECT / EXCEPT
// ===========================================================================

fn generate_nonunion_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    op: &SetOperationStmt<'mcx>,
    refnames_tlist: &[NodeId],
    p_target_list: &mut Vec<NodeId>,
) -> PgResult<RelId> {
    let save_fraction = root.tuple_fraction;
    // Tell children to fetch all tuples (C:1028).
    root.tuple_fraction = 0.0;

    // Recurse on children (C:1031-1043).
    let larg = node_ref(&op.larg).expect("INTERSECT/EXCEPT: NULL larg");
    let rarg = node_ref(&op.rarg).expect("INTERSECT/EXCEPT: NULL rarg");
    let lnode = decode_setop_node(mcx, larg)?;
    let rnode = decode_setop_node(mcx, rarg)?;

    let mut lpath_tlist: Vec<NodeId> = Vec::new();
    let mut lpath_trivial = true;
    let mut lrel = recurse_set_operations(
        mcx, run, root, &lnode, Some(op), &op.colTypes, &op.colCollations, refnames_tlist,
        &mut lpath_tlist, &mut lpath_trivial,
    )?;

    let mut rpath_tlist: Vec<NodeId> = Vec::new();
    let mut rpath_trivial = true;
    let mut rrel = recurse_set_operations(
        mcx, run, root, &rnode, Some(op), &op.colTypes, &op.colCollations, refnames_tlist,
        &mut rpath_tlist, &mut rpath_trivial,
    )?;

    // Generate tlist for SetOp node (C:1052).
    let mut result_trivial = true;
    let tlist = generate_setop_tlist_owned_input(
        mcx, root, &op.colTypes, &op.colCollations, 0, false, &lpath_tlist, refnames_tlist,
        &mut result_trivial,
    )?;
    debug_assert!(result_trivial);
    *p_target_list = tlist.clone();

    // Grouping semantics (C:1062-1082).
    let group_list = generate_setop_grouplist(mcx, root, op, &tlist)?;
    let group_clauses_owned: Vec<SortGroupClause> =
        group_list.iter().map(|&id| *root.sortgroupclause(id)).collect();
    let can_sort = tlist::grouping_is_sortable(&group_clauses_owned);
    let can_hash = tlist::grouping_is_hashable(&group_clauses_owned);
    if !can_sort && !can_hash {
        let what = if op.op == SetOperation::SETOP_INTERSECT { "INTERSECT" } else { "EXCEPT" };
        return Err(PgError::error(alloc::format!("could not implement {what}")));
    }

    let nonunion_pathkeys = if can_sort {
        let pk = pathkeys::make_pathkeys_for_sortclauses(root, &group_list, &tlist);
        root.query_pathkeys = pk.clone();
        pk
    } else {
        Vec::new()
    };

    // Build child paths (C:1087-1096).
    let mut d_left_groups;
    if root.rel(lrel).rtekind == RTE_SUBQUERY {
        let mut g = 0.0;
        build_setop_child_paths(
            mcx, run, root, lrel, lpath_trivial, &lpath_tlist, &nonunion_pathkeys, Some(&mut g),
        )?;
        d_left_groups = g;
    } else {
        d_left_groups = root.rel(lrel).rows;
    }
    let mut d_right_groups;
    if root.rel(rrel).rtekind == RTE_SUBQUERY {
        let mut g = 0.0;
        build_setop_child_paths(
            mcx, run, root, rrel, rpath_trivial, &rpath_tlist, &nonunion_pathkeys, Some(&mut g),
        )?;
        d_right_groups = g;
    } else {
        d_right_groups = root.rel(rrel).rows;
    }

    // Undo tuple_fraction forcing (C:1099).
    root.tuple_fraction = save_fraction;

    // For INTERSECT, put the smaller input first (C:1109-1125).
    if op.op != SetOperation::SETOP_EXCEPT && d_left_groups > d_right_groups {
        core::mem::swap(&mut lrel, &mut rrel);
        core::mem::swap(&mut lpath_tlist, &mut rpath_tlist);
        core::mem::swap(&mut d_left_groups, &mut d_right_groups);
    }

    let lpath = root.rel(lrel).cheapest_total_path.expect("nonunion: no left cheapest");
    let rpath = root.rel(rrel).cheapest_total_path.expect("nonunion: no right cheapest");
    let lrows = root.path(lpath).base().rows;
    let rrows = root.path(rpath).base().rows;

    // Build result relation (C:1130-1133).
    let relids = bms::relids_union::call(&root.rel(lrel).relids, &root.rel(rrel).relids);
    let result_rel = relnode::fetch_upper_rel(root, UPPERREL_SETOP, &relids);
    set_rel_reltarget_from_tlist(root, result_rel, &tlist);

    // Estimate groups / output rows (C:1143-1153).
    let (d_num_groups, d_num_output_rows) = if op.op == SetOperation::SETOP_EXCEPT {
        let g = d_left_groups;
        (g, if op.all { lrows } else { g })
    } else {
        let g = d_left_groups;
        (g, if op.all { lrows.min(rrows) } else { g })
    };
    root.rel_mut(result_rel).rows = d_num_output_rows;

    // SetOpCmd (C:1156-1168).
    let cmd = match op.op {
        SetOperation::SETOP_INTERSECT => {
            if op.all { types_pathnodes::SETOPCMD_INTERSECT_ALL } else { types_pathnodes::SETOPCMD_INTERSECT }
        }
        SetOperation::SETOP_EXCEPT => {
            if op.all { types_pathnodes::SETOPCMD_EXCEPT_ALL } else { types_pathnodes::SETOPCMD_EXCEPT }
        }
        _ => return Err(PgError::error(String::from("unrecognized set op"))),
    };

    // Hash path (C:1173-1185).
    if can_hash {
        let path = pathnode::create::create_setop_path(
            root, result_rel, lpath, rpath, cmd, types_pathnodes::SETOP_HASHED,
            group_list.clone(), d_num_groups, d_num_output_rows,
        )?;
        pathnode::add_path(root, result_rel, path)?;
    }

    // Sort path (C:1191-1251).
    if can_sort {
        let slpath = sorted_input_for_setop(
            root, lrel, lpath, &group_list, &lpath_tlist, &nonunion_pathkeys,
        )?;
        let srpath = sorted_input_for_setop(
            root, rrel, rpath, &group_list, &rpath_tlist, &nonunion_pathkeys,
        )?;
        let path = pathnode::create::create_setop_path(
            root, result_rel, slpath, srpath, cmd, types_pathnodes::SETOP_SORTED,
            group_list.clone(), d_num_groups, d_num_output_rows,
        )?;
        pathnode::add_path(root, result_rel, path)?;
    }

    Ok(result_rel)
}

/// Produce a path for one INTERSECT/EXCEPT input sorted per `group_list`
/// (C:1197-1239). If the cheapest path is already sorted use it; else fetch a
/// presorted path or sort the cheapest.
fn sorted_input_for_setop(
    root: &mut PlannerInfo,
    rel: RelId,
    path: PathId,
    group_list: &[NodeId],
    path_tlist: &[NodeId],
    nonunion_pathkeys: &[types_pathnodes::PathKey],
) -> PgResult<PathId> {
    let pk = pathkeys::make_pathkeys_for_sortclauses(root, group_list, path_tlist);
    let path_pathkeys = root.path(path).base().pathkeys.clone();
    if pathkeys::pathkeys_contained_in(&pk, &path_pathkeys) {
        return Ok(path);
    }
    let pathlist = root.rel(rel).pathlist.clone();
    if let Some(p) = pathkeys::get_cheapest_path_for_pathkeys(
        root, &pathlist, nonunion_pathkeys, &None, TOTAL_COST, false,
    ) {
        return Ok(p);
    }
    pathnode::create::create_sort_path(root, rel, path, pk, -1.0)
}

// ===========================================================================
// generate_recursion_path  (prepunion.c:356)
// ===========================================================================

fn generate_recursion_path<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    set_op: &SetOperationStmt<'mcx>,
    refnames_tlist: &[NodeId],
) -> PgResult<(RelId, Vec<NodeId>)> {
    // Parser should have rejected other cases (C:374).
    if set_op.op != SetOperation::SETOP_UNION {
        return Err(PgError::error(String::from(
            "only UNION queries can be recursive",
        )));
    }
    // Worktable ID should be assigned (C:376).
    debug_assert!(root.wt_param_id >= 0);

    let larg = node_ref(&set_op.larg).expect("recursive UNION: NULL larg");
    let rarg = node_ref(&set_op.rarg).expect("recursive UNION: NULL rarg");
    let lnode = decode_setop_node(mcx, larg)?;
    let rnode = decode_setop_node(mcx, rarg)?;

    // Process the left and right inputs separately, without combining them into
    // an Append (C:382).
    let mut lpath_tlist: Vec<NodeId> = Vec::new();
    let mut lpath_trivial = true;
    let lrel = recurse_set_operations(
        mcx, run, root, &lnode, None, &set_op.colTypes, &set_op.colCollations,
        refnames_tlist, &mut lpath_tlist, &mut lpath_trivial,
    )?;
    if root.rel(lrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(mcx, run, root, lrel, lpath_trivial, &lpath_tlist, &[], None)?;
    }
    let lpath = root
        .rel(lrel)
        .cheapest_total_path
        .expect("generate_recursion_path: non-recursive term has no cheapest_total_path");

    // The right (recursive) path will want to look at the left one (C:394).
    root.non_recursive_path = Some(lpath);
    let mut rpath_tlist: Vec<NodeId> = Vec::new();
    let mut rpath_trivial = true;
    let rrel = recurse_set_operations(
        mcx, run, root, &rnode, None, &set_op.colTypes, &set_op.colCollations,
        refnames_tlist, &mut rpath_tlist, &mut rpath_trivial,
    )?;
    if root.rel(rrel).rtekind == RTE_SUBQUERY {
        build_setop_child_paths(mcx, run, root, rrel, rpath_trivial, &rpath_tlist, &[], None)?;
    }
    let rpath = root
        .rel(rrel)
        .cheapest_total_path
        .expect("generate_recursion_path: recursive term has no cheapest_total_path");
    root.non_recursive_path = None;

    // Generate tlist for the RecursiveUnion path node — same as the Append cases
    // (C:409).
    let tlist_list = alloc::vec![lpath_tlist.clone(), rpath_tlist.clone()];
    let tlist = generate_append_tlist(
        root,
        &set_op.colTypes,
        &set_op.colCollations,
        &tlist_list,
        refnames_tlist,
    )?;

    // Build result relation (C:419).
    let relids = bms::relids_union::call(&root.rel(lrel).relids, &root.rel(rrel).relids);
    let result_rel = relnode::fetch_upper_rel(root, UPPERREL_SETOP, &relids);
    set_rel_reltarget_from_tlist(root, result_rel, &tlist);

    // If UNION (not ALL), identify the grouping operators (C:426).
    let lrows = root.path(lpath).base().rows;
    let rrows = root.path(rpath).base().rows;
    let (group_list, d_num_groups) = if set_op.all {
        (Vec::new(), 0.0)
    } else {
        let group_list = generate_setop_grouplist(mcx, root, set_op, &tlist)?;
        // We only support hashing here (C:435).
        let group_clauses_owned: Vec<SortGroupClause> =
            group_list.iter().map(|&id| *root.sortgroupclause(id)).collect();
        if !tlist::grouping_is_hashable(&group_clauses_owned) {
            return Err(PgError::error(String::from(
                "could not implement recursive UNION: all column datatypes must be hashable",
            )));
        }
        // Worst case: distinct groups == total input size (C:446).
        (group_list, lrows + rrows * 10.0)
    };

    // Make the path node (C:451).
    let target = make_pathtarget(root, &tlist);
    let wt_param = root.wt_param_id;
    let path = pathnode::create::create_recursiveunion_path(
        root,
        result_rel,
        lpath,
        rpath,
        Box::new(target),
        group_list,
        wt_param,
        d_num_groups,
    )?;
    pathnode::add_path(root, result_rel, path)?;
    postprocess_setop_rel(root, result_rel)?;

    Ok((result_rel, tlist))
}

// ===========================================================================
// build_setop_child_paths  (prepunion.c:480)
// ===========================================================================

fn build_setop_child_paths<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    trivial_tlist: bool,
    child_tlist: &[NodeId],
    interesting_pathkeys: &[types_pathnodes::PathKey],
    p_num_groups: Option<&mut f64>,
) -> PgResult<()> {
    debug_assert!(root.rel(rel).rtekind == RTE_SUBQUERY);

    // setop_pathkeys = rel->subroot->setop_pathkeys (C:486). These are the keys
    // that, if the child can produce a presorted path, let the parent set-op
    // avoid re-sorting. They are expressed in the SUBROOT's representation.
    let setop_pathkeys: Vec<types_pathnodes::PathKey> = root
        .rel(rel)
        .subroot
        .0
        .as_deref()
        .expect("child rel has no subroot")
        .setop_pathkeys
        .clone();

    // When sorting is needed, add child rel equivalences (C:493-497).
    if !interesting_pathkeys.is_empty() {
        backend_optimizer_path_equivclass::add_setop_child_rel_equivalences(
            root,
            rel,
            child_tlist,
            interesting_pathkeys,
        )?;
    }

    // Mark rel with estimated size BEFORE generating outer paths (C:504).
    set_subquery_size_estimates(run, root, rel);

    // consider_parallel from subroot final rel (C:510-511).
    let final_consider_parallel = {
        let subroot = root.rel(rel).subroot.0.as_deref().expect("subroot vanished");
        let final_rel = find_existing_upper_final(subroot);
        subroot.rel(final_rel).consider_parallel
    };
    root.rel_mut(rel).consider_parallel = final_consider_parallel;

    // Generate subquery scan paths for each interesting path in the subroot's
    // final_rel (C:514-609). We must sort some paths within the SUBROOT arena
    // before importing them, so collect the (subroot path id) of every outer
    // path we want and build the sort paths in a single subroot-mutating pass.
    let enable_incremental_sort = backend_utils_misc_guc_tables::vars::enable_incremental_sort.read();

    // (subroot_path_id, was_just_sorted) pairs to import + wrap below.
    let mut to_import: Vec<PathId> = Vec::new();
    let limit_tuples = root
        .rel(rel)
        .subroot
        .0
        .as_deref()
        .expect("subroot vanished")
        .limit_tuples;

    {
        // Take the subroot out so we can mutate it to build sort paths.
        let mut subroot = root.rel_mut(rel).subroot.0.take().expect("subroot vanished");
        let final_rel = find_existing_upper_final(&subroot);
        let cheapest_input_path = subroot
            .rel(final_rel)
            .cheapest_total_path
            .expect("build_setop_child_paths: subroot FINAL rel has no cheapest_total_path");
        let pathlist = subroot.rel(final_rel).pathlist.clone();

        for subpath in pathlist {
            // Include the cheapest path as-is (C:524-538).
            if subpath == cheapest_input_path {
                to_import.push(subpath);
            }

            // Skip sorted-path handling if the setop doesn't need them (C:541).
            if interesting_pathkeys.is_empty() {
                continue;
            }

            // Create paths to suit the final sort order required for
            // setop_pathkeys (C:547-587). setop_pathkeys is in subroot repr.
            let subpath_pathkeys = subroot.path(subpath).base().pathkeys.clone();
            let (is_sorted, presorted_keys) =
                pathkeys::pathkeys_count_contained_in(&setop_pathkeys, &subpath_pathkeys);

            let mut sorted_subpath = subpath;
            if !is_sorted {
                // Only sort the cheapest path; incrementally sort any partially
                // sorted path (skip non-cheapest paths with no presorted keys or
                // when incremental sort is disabled) (C:559-587).
                if subpath != cheapest_input_path
                    && (presorted_keys == 0 || !enable_incremental_sort)
                {
                    continue;
                }
                sorted_subpath = if presorted_keys == 0 || !enable_incremental_sort {
                    pathnode::create::create_sort_path(
                        &mut subroot,
                        final_rel,
                        subpath,
                        setop_pathkeys.clone(),
                        limit_tuples,
                    )?
                } else {
                    pathnode::create::create_incremental_sort_path(
                        &mut subroot,
                        run,
                        final_rel,
                        subpath,
                        setop_pathkeys.clone(),
                        presorted_keys,
                        limit_tuples,
                    )?
                };
            }

            // subpath is now sorted; add it unless it is the (already-added)
            // cheapest input path (C:589-608).
            if sorted_subpath != cheapest_input_path {
                to_import.push(sorted_subpath);
            }
        }

        root.rel_mut(rel).subroot.0 = Some(subroot);
    }

    // Import each chosen subroot path into the outer root's arena, convert its
    // pathkeys to outer representation, and wrap in a subqueryscan path.
    for sub_id in to_import {
        let imported = {
            let subroot = root.rel_mut(rel).subroot.0.take().expect("subroot vanished");
            let sub_pathkeys = subroot.path(sub_id).base().pathkeys.clone();
            let id = import_path_from_subroot(mcx, root, &subroot, sub_id);
            root.rel_mut(rel).subroot.0 = Some(subroot);
            (id, sub_pathkeys)
        };
        let (imported_id, sub_pathkeys) = imported;

        let imported_tlist = make_tlist_from_pathtarget_ids(root, imported_id)?;
        let pathkeys_outer =
            pathkeys::convert_subquery_pathkeys(root, rel, &sub_pathkeys, &imported_tlist);

        // `imported_id` is the in-root cost copy; `sub_id` is the original
        // subroot-arena path, which `create_subqueryscan_plan` must rebuild in
        // the subroot context so the leaf scans' `scanrelid` resolves against
        // the subroot's range table (mirrors C's
        // `create_plan(rel->subroot, best_path->subpath)`).
        let sqs = pathnode::create::create_subqueryscan_path(
            root, run, rel, imported_id, Some(sub_id), trivial_tlist, pathkeys_outer, &None,
        )?;
        pathnode::add_path(root, rel, sqs)?;
    }

    // Partial path for the child relation, if the subroot has one (C:611-637).
    let partial_sub = if root.rel(rel).consider_parallel
        && bms::relids_is_empty::call(&root.rel(rel).lateral_relids)
    {
        let subroot = root.rel(rel).subroot.0.as_deref().expect("subroot vanished");
        let final_rel = find_existing_upper_final(subroot);
        subroot.rel(final_rel).partial_pathlist.first().copied()
    } else {
        None
    };
    if let Some(partial_sub_id) = partial_sub {
        let imported = {
            let subroot = root.rel_mut(rel).subroot.0.take().expect("subroot vanished");
            let id = import_path_from_subroot(mcx, root, &subroot, partial_sub_id);
            root.rel_mut(rel).subroot.0 = Some(subroot);
            id
        };
        let partial_path = pathnode::create::create_subqueryscan_path(
            root, run, rel, imported, Some(partial_sub_id), trivial_tlist, Vec::new(), &None,
        )?;
        pathnode::add_partial_path(root, rel, partial_path)?;
    }

    postprocess_setop_rel(root, rel)?;

    // Estimate number of groups if requested (C:654-669).
    if let Some(slot) = p_num_groups {
        *slot = estimate_setop_child_groups(mcx, run, root, rel)?;
    }

    Ok(())
}

/// Estimate the number of distinct groups for a set-op child (C:654-669).
fn estimate_setop_child_groups<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<f64> {
    let cheapest_rows = {
        let cheapest = root.rel(rel).cheapest_total_path.expect("no cheapest for group est");
        root.path(cheapest).base().rows
    };
    // Read subroot->parse for grouping/aggregation flags + tlist exprs.
    let subroot = root.rel_mut(rel).subroot.0.take().expect("no subroot for group est");
    let parse_id = subroot.parse;
    let (has_grouping, group_exprs): (bool, Vec<Expr>) = {
        let parse = run.resolve(parse_id);
        let hg = !parse.groupClause.is_empty()
            || !parse.groupingSets.is_empty()
            || !parse.distinctClause.is_empty()
            || subroot.hasHavingQual
            || parse.hasAggs;
        // get_tlist_exprs(subroot->parse->targetList, false)
        let exprs = if hg {
            Vec::new()
        } else {
            let mut v = Vec::new();
            for te in parse.targetList.iter() {
                if te.resjunk {
                    continue;
                }
                if let Some(e) = te.expr.as_deref() {
                    v.push(e.clone_in(mcx)?);
                }
            }
            v
        };
        (hg, exprs)
    };
    root.rel_mut(rel).subroot.0 = Some(subroot);

    if has_grouping {
        return Ok(cheapest_rows);
    }
    // estimate_num_groups runs over the SUBROOT (C uses subroot). We estimate
    // over the outer root's view of the imported tlist exprs instead — but the
    // group exprs reference the subquery's range table. Allocate the exprs into
    // root's arena as the group_exprs handles.
    let group_expr_ids: Vec<NodeId> = group_exprs.into_iter().map(|e| root.alloc_node(e)).collect();
    backend_utils_adt_selfuncs_seams::estimate_num_groups::call(
        run, root, &group_expr_ids, cheapest_rows, None,
    )
}

/// Find an existing `UPPERREL_FINAL` rel in a subroot (it was created during
/// subquery_planner; do not allocate).
fn find_existing_upper_final(subroot: &PlannerInfo) -> RelId {
    for &id in subroot.upper_rels[UPPERREL_FINAL as usize].iter() {
        return id;
    }
    panic!("build_setop_child_paths: subroot has no UPPERREL_FINAL rel");
}

// ===========================================================================
// plan_union_children  (prepunion.c:1268)
// ===========================================================================

fn plan_union_children<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    top_union: &SetOperationStmt<'mcx>,
    refnames_tlist: &[NodeId],
    tlist_list: &mut Vec<Vec<NodeId>>,
    istrivial_tlist: &mut Vec<bool>,
) -> PgResult<Vec<RelId>> {
    // pending_rels = list_make1(top_union); processed as a deque (lcons = push
    // front).
    let mut pending: Vec<SetOpNode<'mcx>> = Vec::new();
    pending.push(SetOpNode::Stmt(top_union.clone_in(mcx)?));
    let mut result: Vec<RelId> = Vec::new();

    while !pending.is_empty() {
        let set_op = pending.remove(0);

        if let SetOpNode::Stmt(op) = &set_op {
            if op.op == top_union.op
                && (op.all == top_union.all || op.all)
                && op.colTypes == top_union.colTypes
                && op.colCollations == top_union.colCollations
            {
                // Same UNION: fold children into parent (lcons larg then rarg, so
                // larg ends up first).
                let larg = decode_setop_node(mcx, node_ref(&op.larg).expect("UNION NULL larg"))?;
                let rarg = decode_setop_node(mcx, node_ref(&op.rarg).expect("UNION NULL rarg"))?;
                pending.insert(0, rarg);
                pending.insert(0, larg);
                continue;
            }
        }

        // Plan this child separately.
        let mut child_tlist: Vec<NodeId> = Vec::new();
        let mut trivial = true;
        let parent_for_sort = if top_union.all { None } else { Some(top_union) };
        let rel = recurse_set_operations(
            mcx, run, root, &set_op, parent_for_sort, &top_union.colTypes,
            &top_union.colCollations, refnames_tlist, &mut child_tlist, &mut trivial,
        )?;
        result.push(rel);
        tlist_list.push(child_tlist);
        istrivial_tlist.push(trivial);
    }

    Ok(result)
}

// ===========================================================================
// postprocess_setop_rel  (prepunion.c:1330)
// ===========================================================================

fn postprocess_setop_rel(root: &mut PlannerInfo, rel: RelId) -> PgResult<()> {
    // create_upper_paths_hook is never set in this build.
    pathnode::set_cheapest(root, rel)
}

// ===========================================================================
// generate_setop_tlist  (prepunion.c:1356)  — REAL body
// ===========================================================================

/// `generate_setop_tlist` where the input tlist lives in a *subroot* arena.
fn generate_setop_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    subroot: &PlannerInfo,
    col_types: &[Oid],
    col_collations: &[Oid],
    varno: Index,
    hack_constants: bool,
    input_tlist: &[NodeId],
    refnames_tlist: &[NodeId],
    trivial_tlist: &mut bool,
) -> PgResult<Vec<NodeId>> {
    let mut inputs: Vec<(Expr, AttrNumber)> = Vec::with_capacity(input_tlist.len());
    for &te_id in input_tlist.iter() {
        let te = subroot.targetentry(te_id);
        let e = subroot.node(te.expr).clone_in(mcx)?;
        inputs.push((e, te.resno));
    }
    let refnames: Vec<Option<String>> = refnames_tlist
        .iter()
        .map(|&id| root.targetentry(id).resname.clone())
        .collect();
    build_setop_tlist_from_exprs(
        mcx, root, col_types, col_collations, varno, hack_constants, &inputs, &refnames,
        trivial_tlist,
    )
}

/// `generate_setop_tlist` where the input tlist lives in the *root* arena
/// (the INTERSECT/EXCEPT case, varno 0, hack_constants false).
fn generate_setop_tlist_owned_input<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    col_types: &[Oid],
    col_collations: &[Oid],
    varno: Index,
    hack_constants: bool,
    input_tlist: &[NodeId],
    refnames_tlist: &[NodeId],
    trivial_tlist: &mut bool,
) -> PgResult<Vec<NodeId>> {
    let mut inputs: Vec<(Expr, AttrNumber)> = Vec::with_capacity(input_tlist.len());
    for &te_id in input_tlist.iter() {
        let te = root.targetentry(te_id);
        let e = root.node(te.expr).clone_in(mcx)?;
        inputs.push((e, te.resno));
    }
    let refnames: Vec<Option<String>> = refnames_tlist
        .iter()
        .map(|&id| root.targetentry(id).resname.clone())
        .collect();
    build_setop_tlist_from_exprs(
        mcx, root, col_types, col_collations, varno, hack_constants, &inputs, &refnames,
        trivial_tlist,
    )
}

/// Shared body of generate_setop_tlist over already-materialized input exprs.
fn build_setop_tlist_from_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    col_types: &[Oid],
    col_collations: &[Oid],
    varno: Index,
    hack_constants: bool,
    inputs: &[(Expr, AttrNumber)],
    refnames: &[Option<String>],
    trivial_tlist: &mut bool,
) -> PgResult<Vec<NodeId>> {
    *trivial_tlist = true;
    let n = col_types.len();
    let mut tlist: Vec<NodeId> = Vec::with_capacity(n);
    let mut resno: i32 = 1;

    for idx in 0..n {
        let col_type = col_types[idx];
        let col_coll = col_collations[idx];
        let (input_expr, input_resno) = &inputs[idx];

        // HACK: copy a Const up as-is at the first level (C:1404).
        let mut expr: Expr = if hack_constants && matches!(input_expr, Expr::Const(_)) {
            input_expr.clone_in(mcx)?
        } else {
            let v = make_var(
                varno as i32,
                *input_resno,
                expr_type(Some(input_expr))?,
                expr_typmod(Some(input_expr))?,
                expr_collation(Some(input_expr))?,
                0,
            );
            Expr::Var(v)
        };

        if expr_type(Some(&expr))? != col_type {
            // coerce_to_common_type with pstate=NULL — a mismatched-type set-op.
            return Err(PgError::error(String::from(
                "generate_setop_tlist: cross-type set-op column coercion \
                 (coerce_to_common_type) is not yet ported",
            )));
        }

        if expr_collation(Some(&expr))? != col_coll {
            let rtype = expr_type(Some(&expr))?;
            let rtypmod = expr_typmod(Some(&expr))?;
            expr = apply_relabel_type(expr, rtype, rtypmod, col_coll, COERCE_IMPLICIT_CAST, -1, false)?;
            *trivial_tlist = false;
        }

        let expr_id = root.alloc_node(expr);
        let te = TargetEntryNode {
            expr: expr_id,
            resno: resno as AttrNumber,
            resname: refnames[idx].clone(),
            ressortgroupref: resno as Index,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        tlist.push(root.alloc_targetentry(te));
        resno += 1;
    }

    Ok(tlist)
}

// ===========================================================================
// generate_append_tlist  (prepunion.c:1484)  — REAL body
// ===========================================================================

fn generate_append_tlist(
    root: &mut PlannerInfo,
    col_types: &[Oid],
    col_collations: &[Oid],
    input_tlists: &[Vec<NodeId>],
    refnames_tlist: &[NodeId],
) -> PgResult<Vec<NodeId>> {
    let ncols = col_types.len();
    let mut col_typmods: Vec<i32> = alloc::vec![0i32; ncols];

    // Extract typmods (C:1508-1540).
    for (tlist_idx, subtlist) in input_tlists.iter().enumerate() {
        let mut colindex = 0usize;
        for &sub_id in subtlist.iter() {
            let sub_te = root.targetentry(sub_id);
            let se = root.node(sub_te.expr);
            debug_assert!(!sub_te.resjunk);
            debug_assert!(colindex < ncols);
            if expr_type(Some(se))? == col_types[colindex] {
                let subtypmod = expr_typmod(Some(se))?;
                if tlist_idx == 0 {
                    col_typmods[colindex] = subtypmod;
                } else if subtypmod != col_typmods[colindex] {
                    col_typmods[colindex] = -1;
                }
            } else {
                col_typmods[colindex] = -1;
            }
            colindex += 1;
        }
        debug_assert!(colindex == ncols);
    }

    // Build the tlist (C:1545-1575).
    let refnames: Vec<Option<String>> = refnames_tlist
        .iter()
        .map(|&id| root.targetentry(id).resname.clone())
        .collect();
    let mut tlist: Vec<NodeId> = Vec::with_capacity(ncols);
    let mut resno: i32 = 1;
    for colindex in 0..ncols {
        let v = make_var(0, resno as AttrNumber, col_types[colindex], col_typmods[colindex], col_collations[colindex], 0);
        let expr_id = root.alloc_node(Expr::Var(v));
        let te = TargetEntryNode {
            expr: expr_id,
            resno: resno as AttrNumber,
            resname: refnames[colindex].clone(),
            ressortgroupref: resno as Index,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        tlist.push(root.alloc_targetentry(te));
        resno += 1;
    }

    Ok(tlist)
}

// ===========================================================================
// generate_setop_grouplist  (prepunion.c:1593)  — REAL body
// ===========================================================================

fn generate_setop_grouplist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    op: &SetOperationStmt<'mcx>,
    targetlist: &[NodeId],
) -> PgResult<Vec<NodeId>> {
    // grouplist = copyObject(op->groupClauses); each element is SortGroupClause.
    let mut grouplist: Vec<SortGroupClause> = Vec::with_capacity(op.groupClauses.len());
    for n in op.groupClauses.iter() {
        match (&**n).node_tag() {
            ntag::T_SortGroupClause => grouplist.push((&**n).expect_sortgroupclause().clone_in(mcx)?),
            _ => return Err(PgError::error(String::from(
                "generate_setop_grouplist: groupClauses element is not a SortGroupClause",
            ))),
        }
    }

    // Install sortgrouprefs from the tlist (C:1600-1619).
    debug_assert!(grouplist.len() == targetlist.len());
    let mut out: Vec<NodeId> = Vec::with_capacity(grouplist.len());
    for (i, &tnode) in targetlist.iter().enumerate() {
        let te = root.targetentry(tnode);
        debug_assert!(!te.resjunk);
        debug_assert!(te.ressortgroupref == te.resno as Index);
        let mut sgc = grouplist[i];
        debug_assert!(sgc.tleSortGroupRef == 0);
        sgc.tleSortGroupRef = te.ressortgroupref;
        out.push(root.alloc_sortgroupclause(sgc));
    }
    Ok(out)
}

// ===========================================================================
// helpers
// ===========================================================================

/// `create_pathtarget(root, tlist)` = make_pathtarget_from_tlist +
/// set_pathtarget_cost_width.
fn make_pathtarget(root: &PlannerInfo, tlist: &[NodeId]) -> types_pathnodes::PathTarget {
    let mut t = tlist::make_pathtarget_from_tlist(root, tlist);
    backend_optimizer_path_costsize::sizeest::set_pathtarget_cost_width(root, &mut t);
    t
}

/// Set `rel.reltarget` to the PathTarget built from `tlist`.
fn set_rel_reltarget_from_tlist(root: &mut PlannerInfo, rel: RelId, tlist: &[NodeId]) {
    let t = make_pathtarget(root, tlist);
    root.rel_mut(rel).reltarget = Some(Box::new(t));
}

/// `make_tlist_from_pathtarget` over an imported path's pathtarget, returned as
/// arena `NodeId`s (the subquery tlist `convert_subquery_pathkeys` needs).
fn make_tlist_from_pathtarget_ids(root: &mut PlannerInfo, path: PathId) -> PgResult<Vec<NodeId>> {
    // The imported path's pathtarget exprs are NodeIds in root's arena already.
    let exprs: Vec<NodeId> = match root.path(path).base().pathtarget.as_deref() {
        Some(t) => t.exprs.clone(),
        None => Vec::new(),
    };
    // Wrap each expr in a TargetEntry (resno 1..n), matching makeTargetEntry in
    // make_tlist_from_pathtarget.
    let mut out: Vec<NodeId> = Vec::with_capacity(exprs.len());
    for (i, expr_id) in exprs.into_iter().enumerate() {
        let te = TargetEntryNode {
            expr: expr_id,
            resno: (i + 1) as AttrNumber,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        out.push(root.alloc_targetentry(te));
    }
    Ok(out)
}

/// `tlist_same_datatypes` over an arena `NodeId` tlist.
fn tlist_same_datatypes_ids(
    root: &PlannerInfo,
    tlist: &[NodeId],
    col_types: &[Oid],
) -> PgResult<bool> {
    if tlist.len() != col_types.len() {
        return Ok(false);
    }
    for (i, &id) in tlist.iter().enumerate() {
        let te = root.targetentry(id);
        if te.resjunk {
            return Ok(false);
        }
        let e = root.node(te.expr);
        if expr_type(Some(e))? != col_types[i] {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `tlist_same_collations` over an arena `NodeId` tlist.
fn tlist_same_collations_ids(
    root: &PlannerInfo,
    tlist: &[NodeId],
    col_collations: &[Oid],
) -> PgResult<bool> {
    if tlist.len() != col_collations.len() {
        return Ok(false);
    }
    for (i, &id) in tlist.iter().enumerate() {
        let te = root.targetentry(id);
        if te.resjunk {
            return Ok(false);
        }
        let e = root.node(te.expr);
        let coll = expr_collation(Some(e))?;
        if coll != col_collations[i] && col_collations[i] != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `AGGSPLIT_SIMPLE` (`nodes.h`).
const AGGSPLIT_SIMPLE: types_pathnodes::AggSplit = types_pathnodes::AGGSPLIT_SIMPLE;
