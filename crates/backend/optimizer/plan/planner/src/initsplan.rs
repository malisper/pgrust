//! initsplan.c + restrictinfo.c slice: single-baserel deconstruct_jointree,
//! pushed-down distribute_qual_to_rels, make_restrictinfo for non-OR clauses.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{JoinlistNode, QualCost, RestrictInfo, RinfoId, VOLATILITY_UNKNOWN};

use crate::relnode::{
    find_base_rel, relids_copy, relids_is_empty, relids_num_members, relids_overlap,
    relids_singleton, relids_singleton_member, relids_union,
};
use crate::run::PlannerRun;
pub fn build_base_rel_tlists<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    let mut tlist_vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for te_node in run.processed_tlist() {
        let te = te_node.as_target_entry().expect("TargetEntry");
        pull_var_nodes(te.expr, &mut tlist_vars);
    }
    if !tlist_vars.is_empty() {
        let where_needed = relids_singleton(mcx, 0);
        add_vars_to_targetlist(run, &tlist_vars, &where_needed)?;
    }
    debug_assert!(run.parse().havingQual.is_none());
    Ok(())
}

// pull_var_clause, PVC_RECURSE_AGGREGATES|WINDOWFUNCS + INCLUDE_PLACEHOLDERS.
fn pull_var_nodes<'mcx>(node: Node<'mcx>, out: &mut PgVec<'mcx, Node<'mcx>>) {
    match node.node_tag() {
        NodeTag::T_Var => out.push(node),
        NodeTag::T_Const => {}
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            debug_assert!(a.agglevelsup == 0);
            debug_assert!(a.aggdirectargs.is_nil() && a.aggfilter.is_none());
            for arg in &a.args {
                pull_var_nodes(arg, out);
            }
        }
        NodeTag::T_TargetEntry => pull_var_nodes(node.as_target_entry().unwrap().expr, out),
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_RelabelType => pull_var_nodes(node.as_relabel_type().unwrap().arg, out),
        other => panic!("pull_var_clause (var.c): {other:?}; M2 expression lane"),
    }
}

// add_vars_to_targetlist (initsplan.c); PlaceHolderVars can't reach here.
pub fn add_vars_to_targetlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    vars: &[Node<'mcx>],
    where_needed: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(!relids_is_empty(where_needed));
    for &node in vars {
        let var = node.as_var().expect("Var");
        let rel_id = find_base_rel(&run.root, var.varno);
        let (min_attr, max_attr) = {
            let rel = run.root.rel(rel_id);
            if crate::relnode::relids_is_subset(where_needed, &rel.relids) {
                continue;
            }
            (rel.min_attr, rel.max_attr)
        };
        debug_assert!(var.varattno >= min_attr && var.varattno <= max_attr);
        let ndx = (var.varattno - min_attr) as usize;
        if run.root.rel(rel_id).attr_needed[ndx].is_none() {
            debug_assert!(var.varnullingrels.is_empty());
            let id = run.intern_expr(node);
            run.root.rel_reltarget_mut(rel_id).exprs.push(id);
        }
        let cur = run.root.rel_mut(rel_id).attr_needed[ndx].take();
        run.root.rel_mut(rel_id).attr_needed[ndx] = relids_union(mcx, &cur, where_needed);
    }
    Ok(())
}

// deconstruct_jointree (initsplan.c): FromExpr over plain RangeTblRefs
// (explicit JOIN syntax is loud at parse time). Vars: qualscope = the union of
// member relids; a multi-item FROM is an inner join subsuming all below it.
pub fn deconstruct_jointree<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<PgVec<'mcx, JoinlistNode<'mcx>>> {
    let mcx = run.mcx;
    debug_assert!(!run.root.join_domains.is_empty());
    run.root.placeholdersFrozen = true;
    let f = run.parse().jointree.expect("jointree is a FromExpr");
    let mut qualscope: types_pathnodes::Relids<'mcx> = None;
    let mut joinlist = PgVec::new_in(mcx);
    for item in &f.fromlist {
        assert!(
            item.node_tag() == NodeTag::T_RangeTblRef,
            "deconstruct_recurse (initsplan.c): {:?} jointree item; M2 join lane",
            item.node_tag()
        );
        let varno = item.as_range_tbl_ref().unwrap().rtindex;
        qualscope = relids_union(mcx, &qualscope, &relids_singleton(mcx, varno as u32));
        joinlist.push(JoinlistNode::Rel(varno));
    }
    debug_assert!(!joinlist.is_empty());

    run.root.all_baserels = relids_copy(mcx, &qualscope);
    run.root.all_query_rels = relids_copy(mcx, &qualscope);
    run.root.join_domains[0].jd_relids = relids_copy(mcx, &qualscope);

    if let Some(quals) = f.quals {
        distribute_qual_to_rels(run, quals, &qualscope)?;
    }

    Ok(joinlist)
}

// distribute_qual_to_rels (initsplan.c), pushed-down arm (security 0).
fn distribute_qual_to_rels<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    qualscope: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    if matches!(clause.node_tag(), NodeTag::T_List | NodeTag::T_BoolExpr) {
        panic!("distribute_quals_to_rels (initsplan.c): AND/OR list; M2 multi-qual lane");
    }
    let relids = pull_varnos_relids(run, clause)?;
    assert!(
        crate::relnode::relids_is_subset(&relids, qualscope),
        "distribute_qual_to_rels (initsplan.c): lateral reference; M2 lane"
    );
    if relids_is_empty(&relids) {
        panic!("distribute_qual_to_rels (initsplan.c): pseudoconstant qual; M2 gating lane");
    }

    let is_pushed_down = true;
    let rinfo = make_restrictinfo(run, clause, is_pushed_down, false, false, false, 0, relids, None, None)?;

    // Join clauses: mark their Vars needed at the join level so the scans
    // below emit them.
    if relids_num_members(&run.root.rinfo(rinfo).required_relids) > 1 {
        let mut vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(run.mcx);
        pull_var_nodes(clause, &mut vars);
        let where_needed = relids_copy(run.mcx, &run.root.rinfo(rinfo).required_relids);
        add_vars_to_targetlist(run, &vars, &where_needed)?;
    }

    check_mergejoinable(run, rinfo)?;
    // C divergence: C routes a mergejoinable qual through the EC machinery
    // (process_equivalence); for a single-rel qual the detour rebuilds this
    // identical clause, and for a join qual the EC would regenerate the same
    // RestrictInfo at the join via generate_join_implied_equalities. Both
    // collapse to distributing the clause directly. The equivclass unit owns
    // the real path; every consumer of EC state (pathkeys, mergejoin,
    // EC-derived clauses at higher join levels) is a loud arm.
    distribute_restrictinfo_to_rels(run, rinfo)
}

fn pull_varnos_relids<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<types_pathnodes::Relids<'mcx>> {
    let mcx = run.mcx;
    let bms = vars::pull_varnos(mcx, node)?;
    let mut out: types_pathnodes::Relids<'mcx> = None;
    for x in bms.iter() {
        out = relids_union(mcx, &out, &relids_singleton(mcx, x as u32));
    }
    Ok(out)
}

// make_restrictinfo -> make_plain_restrictinfo (restrictinfo.c).
#[allow(clippy::too_many_arguments)]
pub fn make_restrictinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: u32,
    required_relids: types_pathnodes::Relids<'mcx>,
    incompatible_relids: types_pathnodes::Relids<'mcx>,
    outer_relids: types_pathnodes::Relids<'mcx>,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    assert!(
        !matches!(clause.node_tag(), NodeTag::T_BoolExpr),
        "make_sub_restrictinfos (restrictinfo.c): OR clause; M2 OR lane"
    );

    // security_level 0 skips the leakproof probe.
    debug_assert!(security_level == 0);
    let leakproof = false;

    let mut left_relids: types_pathnodes::Relids<'mcx> = None;
    let mut right_relids: types_pathnodes::Relids<'mcx> = None;
    let clause_relids: types_pathnodes::Relids<'mcx>;
    let mut can_join = false;

    let opexpr = clause.as_op_expr().filter(|o| o.args.len() == 2);
    if let Some(o) = opexpr {
        left_relids = pull_varnos_relids(run, o.args.nth(0))?;
        right_relids = pull_varnos_relids(run, o.args.nth(1))?;
        clause_relids = relids_union(mcx, &left_relids, &right_relids);
        if !relids_is_empty(&left_relids)
            && !relids_is_empty(&right_relids)
            && !relids_overlap(&left_relids, &right_relids)
        {
            can_join = true;
            debug_assert!(!pseudoconstant);
        }
    } else {
        clause_relids = pull_varnos_relids(run, clause)?;
    }

    let required_relids = if required_relids.is_some() {
        required_relids
    } else {
        relids_copy(mcx, &clause_relids)
    };
    debug_assert!(run.root.outer_join_rels.is_none());
    let num_base_rels = relids_num_members(&clause_relids);

    run.root.last_rinfo_serial += 1;
    let rinfo_serial = run.root.last_rinfo_serial;
    let clause_id = run.intern_expr(clause);

    let ri = RestrictInfo {
        clause: clause_id,
        orclause: None,
        is_pushed_down,
        pseudoconstant,
        has_clone,
        is_clone,
        can_join,
        security_level,
        incompatible_relids,
        outer_relids,
        leakproof,
        has_volatile: VOLATILITY_UNKNOWN,
        left_relids,
        right_relids,
        clause_relids,
        required_relids,
        num_base_rels,
        rinfo_serial,
        parent_ec: None,
        eval_cost: QualCost { startup: -1.0, per_tuple: 0.0 },
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: PgVec::new_in(mcx),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: PgVec::new_in(mcx),
        outer_is_left: false,
        hashjoinoperator: 0,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: 0,
        right_hasheqoperator: 0,
    };
    Ok(run.root.alloc_rinfo(ri))
}

// check_mergejoinable (initsplan.c).
fn check_mergejoinable(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return Ok(());
    };
    let (opno, args0) = (o.opno, o.args.nth(0));
    let lefttype = crate::costsize::expr_type_typmod(args0).0;
    if lsyscache::op_mergejoinable(opno, lefttype)? && !clauses::contain_volatile_functions(clause)? {
        let fams = lsyscache::get_mergejoin_opfamilies(run.mcx, opno)?;
        run.root.rinfo_mut(rinfo).mergeopfamilies = fams;
    }
    Ok(())
}

// distribute_restrictinfo_to_rels (initsplan.c).
pub fn distribute_restrictinfo_to_rels(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    let relids = relids_copy(run.mcx, &run.root.rinfo(rinfo).required_relids);
    if let Some(relid) = relids_singleton_member(&relids) {
        return add_base_clause_to_rel(run, relid, rinfo);
    }
    // add_join_clause_to_rels (joininfo.c): one shared RestrictInfo handle
    // linked into every participating rel's joininfo list.
    debug_assert!(relids_num_members(&relids) > 1);
    let members = relids.as_ref().expect("multi-member relids");
    for (i, w) in members.words.iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            let relid = (i * 64) as i32 + w.trailing_zeros() as i32;
            w &= w - 1;
            debug_assert!(crate::relnode::relids_is_member(relid, &run.root.all_baserels));
            let rel = find_base_rel(&run.root, relid);
            run.root.rel_mut(rel).joininfo.push(rinfo);
        }
    }
    Ok(())
}

// add_base_clause_to_rel (initsplan.c), non-inherited arm; the constant-
// TRUE/FALSE reductions only fire for shapes that panicked upstream.
fn add_base_clause_to_rel(run: &mut PlannerRun<'_>, relid: i32, rinfo: RinfoId) -> PgResult<()> {
    let rel_id = find_base_rel(&run.root, relid);
    debug_assert!(!run.rte(relid as usize).inh);
    let security_level = run.root.rinfo(rinfo).security_level;
    let rel = run.root.rel_mut(rel_id);
    rel.baserestrictinfo.push(rinfo);
    rel.baserestrict_min_security = rel.baserestrict_min_security.min(security_level);
    Ok(())
}
