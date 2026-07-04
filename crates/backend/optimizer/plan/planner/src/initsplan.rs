//! initsplan.c + restrictinfo.c slice: deconstruct_jointree over INNER/LEFT/
//! SEMI/ANTI joins, distribute_qual_to_rels, make_restrictinfo for non-OR
//! clauses.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{
    JoinlistNode, OuterJoinClauseInfo, QualCost, RestrictInfo, RinfoId, SpecialJoinInfo, JOIN_ANTI,
    JOIN_LEFT, VOLATILITY_UNKNOWN,
};

use crate::relnode::{
    find_base_rel, relids_add_member, relids_copy, relids_difference, relids_intersect,
    relids_is_empty, relids_is_member, relids_is_subset, relids_members, relids_num_members,
    relids_overlap, relids_singleton, relids_singleton_member, relids_union,
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
    if let Some(having) = run.parse().havingQual {
        let mut having_vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        pull_var_nodes(having, &mut having_vars);
        if !having_vars.is_empty() {
            let where_needed = relids_singleton(mcx, 0);
            add_vars_to_targetlist(run, &having_vars, &where_needed)?;
        }
    }
    Ok(())
}

// pull_var_clause, PVC_RECURSE_AGGREGATES|WINDOWFUNCS + INCLUDE_PLACEHOLDERS.
pub(crate) fn pull_var_nodes<'mcx>(node: Node<'mcx>, out: &mut PgVec<'mcx, Node<'mcx>>) {
    match node.node_tag() {
        NodeTag::T_Var => out.push(node),
        NodeTag::T_Const | NodeTag::T_NextValueExpr => {}
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            debug_assert!(a.agglevelsup == 0);
            debug_assert!(a.aggdirectargs.is_nil());
            for arg in &a.args {
                pull_var_nodes(arg, out);
            }
            if let Some(f) = a.aggfilter {
                pull_var_nodes(f, out);
            }
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                pull_var_nodes(arg, out);
            }
            if let Some(f) = wf.aggfilter {
                pull_var_nodes(f, out);
            }
        }
        NodeTag::T_GroupingFunc => {
            let g = node.as_grouping_func().unwrap();
            debug_assert!(g.agglevelsup == 0);
            for arg in &g.args {
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
        NodeTag::T_CoerceViaIO => pull_var_nodes(node.as_coerce_via_io().unwrap().arg, out),
        NodeTag::T_NullTest => {
            if let Some(arg) = node.as_null_test().unwrap().arg {
                pull_var_nodes(arg, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(arg) = node.as_boolean_test().unwrap().arg {
                pull_var_nodes(arg, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in &node.as_row_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_CoerceToDomain => pull_var_nodes(node.as_coerce_to_domain().unwrap().arg, out),
        NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_List => {
            for a in node.as_list().unwrap() {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for a in sr.refupperindexpr.iter().flatten() {
                pull_var_nodes(a, out);
            }
            for a in sr.reflowerindexpr.iter().flatten() {
                pull_var_nodes(a, out);
            }
            if let Some(a) = sr.refexpr {
                pull_var_nodes(a, out);
            }
            if let Some(a) = sr.refassgnexpr {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_Param => {}
        NodeTag::T_AlternativeSubPlan => {
            for a in &node.as_alternative_sub_plan().unwrap().subplans {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                pull_var_nodes(te, out);
            }
            for a in &sp.args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_CaseTestExpr | NodeTag::T_SQLValueFunction => {}
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                pull_var_nodes(arg, out);
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                pull_var_nodes(cw.expr.expect("CaseWhen.expr"), out);
                pull_var_nodes(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                pull_var_nodes(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in &node.as_coalesce_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in &node.as_min_max_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for a in &node.as_array_expr().unwrap().elements {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                pull_var_nodes(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in &c.args {
                pull_var_nodes(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                pull_var_nodes(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                pull_var_nodes(e, out);
            }
        }
        NodeTag::T_JsonBehavior => {
            if let Some(e) = node.as_json_behavior().unwrap().expr {
                pull_var_nodes(e, out);
            }
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                pull_var_nodes(e, out);
            }
            for v in &j.passing_values {
                pull_var_nodes(v, out);
            }
        }
        NodeTag::T_FieldSelect => pull_var_nodes(node.as_field_select().unwrap().arg, out),
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
            let id = if var.varnullingrels.is_empty() {
                run.intern_expr(node)
            } else {
                let stripped = types_nodes::primnodes::Var {
                    varnullingrels: types_nodes::Bitmapset::empty(),
                    ..*var
                };
                run.intern_expr(Node::mk(mcx, stripped)?)
            };
            run.root.rel_reltarget_mut(rel_id).exprs.push(id);
        }
        let cur = run.root.rel_mut(rel_id).attr_needed[ndx].take();
        run.root.rel_mut(rel_id).attr_needed[ndx] = relids_union(mcx, &cur, where_needed);
    }
    Ok(())
}

// add_vars_to_attr_needed (initsplan.c): attr_needed bits only, no reltarget
// additions (the rebuild path after join removal).
pub(crate) fn add_vars_to_attr_needed<'mcx>(
    run: &mut PlannerRun<'mcx>,
    vars: &[Node<'mcx>],
    where_needed: &types_pathnodes::Relids<'mcx>,
) {
    let mcx = run.mcx;
    debug_assert!(!relids_is_empty(where_needed));
    for &node in vars {
        let var = node.as_var().expect("Var");
        let rel_id = find_base_rel(&run.root, var.varno);
        let min_attr = {
            let rel = run.root.rel(rel_id);
            if crate::relnode::relids_is_subset(where_needed, &rel.relids) {
                continue;
            }
            debug_assert!(var.varattno >= rel.min_attr && var.varattno <= rel.max_attr);
            rel.min_attr
        };
        let ndx = (var.varattno - min_attr) as usize;
        let cur = run.root.rel_mut(rel_id).attr_needed[ndx].take();
        run.root.rel_mut(rel_id).attr_needed[ndx] = relids_union(mcx, &cur, where_needed);
    }
}

enum JtItem<'mcx> {
    Plain {
        quals: Option<Node<'mcx>>,
        qualscope: types_pathnodes::Relids<'mcx>,
        jdomain: usize,
    },
    SecQuals {
        varno: i32,
        qualscope: types_pathnodes::Relids<'mcx>,
        jdomain: usize,
    },
    Sj {
        jointype: types_pathnodes::JoinType,
        quals: Option<Node<'mcx>>,
        qualscope: types_pathnodes::Relids<'mcx>,
        jdomain: usize,
        left_rels: types_pathnodes::Relids<'mcx>,
        right_rels: types_pathnodes::Relids<'mcx>,
        inner_join_rels: types_pathnodes::Relids<'mcx>,
        rtindex: i32,
    },
}

// find_lateral_references (initsplan.c); appendrel otherrels are loud
// upstream (inh), so only plain baserels are examined.
pub fn find_lateral_references(run: &mut PlannerRun<'_>) -> PgResult<()> {
    if !run.root.hasLateralRTEs {
        return Ok(());
    }
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else {
            continue;
        };
        debug_assert_eq!(run.root.rel(rel).relid as usize, rti);
        if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL {
            continue;
        }
        extract_lateral_references(run, rel, rti)?;
    }
    Ok(())
}

fn extract_lateral_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: types_pathnodes::RelId,
    rtindex: usize,
) -> PgResult<()> {
    use types_nodes::parsenodes::RTEKind;
    let mcx = run.mcx;
    let rte = run.rte(rtindex);
    if !rte.lateral {
        return Ok(());
    }
    let mut vars: types_nodes::NodeList<'mcx> = types_nodes::NodeList::nil();
    let mut levelsup = 0;
    match rte.rtekind {
        RTEKind::RTE_SUBQUERY => {
            levelsup = 1;
            let q = rte.subquery.expect("RTE_SUBQUERY has a subquery");
            let qnode = Node::mk(mcx, crate::subselect::query_cells_copy(mcx, q)?)?;
            vars = vars::pull_vars_of_level(mcx, qnode, 1)?;
        }
        RTEKind::RTE_FUNCTION => {
            for f in &rte.functions {
                vars.concat(mcx, &vars::pull_vars_of_level(mcx, f, 0)?)?;
            }
        }
        RTEKind::RTE_VALUES => {
            for l in &rte.values_lists {
                vars.concat(mcx, &vars::pull_vars_of_level(mcx, l, 0)?)?;
            }
        }
        RTEKind::RTE_TABLEFUNC => {
            let tf = rte.tablefunc.expect("TABLEFUNC RTE has tablefunc");
            vars = vars::pull_vars_of_level(mcx, tf, 0)?;
        }
        other => panic!("extract_lateral_references (initsplan.c): {other:?} lateral RTE"),
    }
    if vars.is_nil() {
        return Ok(());
    }
    let mut newvars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for node in &vars {
        let v = node
            .as_var()
            .expect("pull_vars_of_level returns Vars (PHVs are loud)");
        if levelsup != 0 {
            let nv = types_nodes::primnodes::Var {
                varlevelsup: 0,
                varnullingrels: v.varnullingrels.clone_in(mcx)?,
                ..*v
            };
            newvars.push(Node::mk(mcx, nv)?);
        } else {
            newvars.push(node);
        }
    }
    let where_needed = relids_singleton(mcx, rtindex as u32);
    add_vars_to_targetlist(run, &newvars, &where_needed)?;
    let mut ids: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    for &n in newvars.iter() {
        ids.push(run.intern_expr(n));
    }
    run.root.rel_mut(rel).lateral_vars = ids;
    Ok(())
}

// rebuild_lateral_attr_needed (initsplan.c): attr_needed bits only, after
// join removal.
pub fn rebuild_lateral_attr_needed(run: &mut PlannerRun<'_>) -> PgResult<()> {
    if !run.root.hasLateralRTEs {
        return Ok(());
    }
    let mcx = run.mcx;
    for rti in 1..run.root.simple_rel_array_size as usize {
        let Some(rel) = run.root.simple_rel_array[rti] else {
            continue;
        };
        if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL {
            continue;
        }
        if run.root.rel(rel).lateral_vars.is_empty() {
            continue;
        }
        let mut nodes: PgVec<'_, Node<'_>> = PgVec::new_in(mcx);
        for i in 0..run.root.rel(rel).lateral_vars.len() {
            let id = run.root.rel(rel).lateral_vars[i];
            nodes.push(*run.root.expr_node(id));
        }
        let where_needed = relids_singleton(mcx, rti as u32);
        add_vars_to_attr_needed(run, &nodes, &where_needed);
    }
    Ok(())
}

// create_lateral_join_info (initsplan.c); the PlaceHolderVar legs are dead
// (no PHVs are ever created on this lane, asserted by the caller).
pub fn create_lateral_join_info(run: &mut PlannerRun<'_>) -> PgResult<()> {
    if !run.root.hasLateralRTEs {
        return Ok(());
    }
    debug_assert!(run.root.placeholdersFrozen);
    debug_assert!(run.root.placeholder_list.is_empty());
    let mcx = run.mcx;
    let mut found_laterals = false;
    let size = run.root.simple_rel_array_size as usize;
    for rti in 1..size {
        let Some(rel) = run.root.simple_rel_array[rti] else {
            continue;
        };
        debug_assert_eq!(run.root.rel(rel).relid as usize, rti);
        if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL {
            continue;
        }
        let mut lateral_relids: types_pathnodes::Relids<'_> = None;
        for i in 0..run.root.rel(rel).lateral_vars.len() {
            let id = run.root.rel(rel).lateral_vars[i];
            let v = run
                .root
                .expr_node(id)
                .as_var()
                .expect("lateral_vars hold Vars");
            found_laterals = true;
            lateral_relids = relids_add_member(mcx, &lateral_relids, v.varno as u32);
        }
        run.root.rel_mut(rel).direct_lateral_relids = relids_copy(mcx, &lateral_relids);
        run.root.rel_mut(rel).lateral_relids = lateral_relids;
    }

    if !found_laterals {
        run.root.hasLateralRTEs = false;
        return Ok(());
    }

    // Warshall transitive closure over lateral_relids.
    for rti in 1..size {
        let Some(rel) = run.root.simple_rel_array[rti] else {
            continue;
        };
        if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL {
            continue;
        }
        let outer = relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        if relids_is_empty(&outer) {
            continue;
        }
        for rti2 in 1..size {
            let Some(rel2) = run.root.simple_rel_array[rti2] else {
                continue;
            };
            if run.root.rel(rel2).reloptkind != types_pathnodes::RELOPT_BASEREL {
                continue;
            }
            if relids_is_member(rti as i32, &run.root.rel(rel2).lateral_relids) {
                let cur = run.root.rel_mut(rel2).lateral_relids.take();
                run.root.rel_mut(rel2).lateral_relids = relids_union(mcx, &cur, &outer);
            }
        }
    }

    // Inverse mapping: lateral_referencers.
    for rti in 1..size {
        let Some(rel) = run.root.simple_rel_array[rti] else {
            continue;
        };
        if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL {
            continue;
        }
        let lateral_relids = relids_copy(mcx, &run.root.rel(rel).lateral_relids);
        if relids_is_empty(&lateral_relids) {
            continue;
        }
        debug_assert!(!relids_is_member(rti as i32, &lateral_relids));
        for rti2 in relids_members(&lateral_relids) {
            let Some(rel2) = run.root.simple_rel_array[rti2 as usize] else {
                continue; // an outer join in the set
            };
            debug_assert_eq!(
                run.root.rel(rel2).reloptkind,
                types_pathnodes::RELOPT_BASEREL
            );
            let cur = run.root.rel_mut(rel2).lateral_referencers.take();
            run.root.rel_mut(rel2).lateral_referencers = relids_add_member(mcx, &cur, rti as u32);
        }
    }
    Ok(())
}

// deconstruct_jointree (initsplan.c) over RangeTblRefs and INNER/LEFT/SEMI/
// ANTI JoinExprs (RIGHT was flipped by reduce_outer_joins; FULL is loud
// upstream). C's three phases map to: recurse (relids/joinlist/JtItems in
// post-order), distribute each item's quals, then distribute the postponed
// non-degenerate LEFT-join quals (deconstruct_distribute_oj_quals).
pub fn deconstruct_jointree<'mcx>(
    run: &mut PlannerRun<'mcx>,
) -> PgResult<PgVec<'mcx, JoinlistNode<'mcx>>> {
    let mcx = run.mcx;
    debug_assert!(!run.root.join_domains.is_empty());
    run.root.placeholdersFrozen = true;
    let f = run.parse().jointree.expect("jointree is a FromExpr");
    let mut qualscope: types_pathnodes::Relids<'mcx> = None;
    let mut joinlist = PgVec::new_in(mcx);
    let mut items: PgVec<'mcx, JtItem<'mcx>> = PgVec::new_in(mcx);
    for item in &f.fromlist {
        let (item_relids, _item_inner, item_joinlist) =
            deconstruct_recurse(run, item, 0, &mut items)?;
        qualscope = relids_union(mcx, &qualscope, &item_relids);
        for jl in item_joinlist {
            joinlist.push(jl);
        }
    }
    debug_assert!(!joinlist.is_empty());
    if joinlist.len() > crate::gucs::join_collapse_limit() as usize {
        panic!("deconstruct_recurse (initsplan.c): joinlist beyond collapse limit; M2 join lane");
    }

    run.root.all_baserels = relids_difference(mcx, &qualscope, &run.root.outer_join_rels);
    run.root.all_query_rels = relids_copy(mcx, &qualscope);
    run.root.join_domains[0].jd_relids = relids_copy(mcx, &qualscope);
    items.push(JtItem::Plain {
        quals: f.quals,
        qualscope,
        jdomain: 0,
    });

    let mut oj_postponed: PgVec<'mcx, (usize, PgVec<'mcx, Node<'mcx>>)> = PgVec::new_in(mcx);
    // Per-JtItem quals postponed by children for carrying lateral references
    // (C: jtitem->lateral_clauses); ancestors sit later in the post-order.
    let mut lateral_pending: PgVec<'mcx, PgVec<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    for _ in 0..items.len() {
        lateral_pending.push(PgVec::new_in(mcx));
    }
    let plain_security_level = run.root.qual_security_level;
    for idx in 0..items.len() {
        let pending = core::mem::replace(&mut lateral_pending[idx], PgVec::new_in(mcx));
        // C folds these into my_quals, so make_outerjoininfo's semijoin
        // analysis sees postponed lateral clauses too.
        let mut pending_for_sjinfo: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        pending_for_sjinfo.extend(pending.iter().copied());
        if !pending.is_empty() {
            let (qualscope, jdomain) = match &items[idx] {
                JtItem::Plain { qualscope, jdomain, .. }
                | JtItem::SecQuals { qualscope, jdomain, .. }
                | JtItem::Sj { qualscope, jdomain, .. } => {
                    (relids_copy(mcx, qualscope), *jdomain)
                }
            };
            for clause in pending {
                distribute_qual_to_rels(
                    run,
                    clause,
                    &qualscope,
                    &None,
                    &None,
                    None,
                    jdomain,
                    plain_security_level,
                    None,
                    Some((&items, idx)),
                    &mut lateral_pending,
                )?;
            }
        }
        let item = &items[idx];
        match item {
            JtItem::Plain {
                quals,
                qualscope,
                jdomain,
            } => {
                distribute_quals_to_rels(
                    run,
                    *quals,
                    qualscope,
                    &None,
                    &None,
                    None,
                    *jdomain,
                    plain_security_level,
                    None,
                    Some((&items, idx)),
                    &mut lateral_pending,
                )?;
            }
            // process_security_barrier_quals (initsplan.c): each securityQuals
            // sublist gets an ascending level; ojscope = qualscope forces
            // Var-free quals to the rel instead of the top of the tree.
            JtItem::SecQuals { varno, qualscope, jdomain } => {
                let rte = run.rte(*varno as usize);
                let mut level: u32 = 0;
                for qualset in &rte.securityQuals {
                    distribute_quals_to_rels(
                        run,
                        Some(qualset),
                        qualscope,
                        &relids_copy(mcx, qualscope),
                        &None,
                        None,
                        *jdomain,
                        level,
                        None,
                        Some((&items, idx)),
                        &mut lateral_pending,
                    )?;
                    level += 1;
                }
                debug_assert!(level <= plain_security_level);
            }
            JtItem::Sj {
                jointype,
                quals,
                qualscope,
                jdomain,
                left_rels,
                right_rels,
                inner_join_rels,
                rtindex,
            } => {
                let sjinfo_quals = if pending_for_sjinfo.is_empty() {
                    *quals
                } else {
                    let mut l = types_nodes::list::NodeList::nil();
                    for &c in pending_for_sjinfo.iter() {
                        l.lappend(mcx, c)?;
                    }
                    if let Some(q) = quals {
                        match q.as_list() {
                            Some(ql) => {
                                for c in ql.iter() {
                                    l.lappend(mcx, c)?;
                                }
                            }
                            None => l.lappend(mcx, *q)?,
                        }
                    }
                    Some(Node::mk_list(mcx, l)?)
                };
                let sjinfo = make_outerjoininfo(
                    run,
                    left_rels,
                    right_rels,
                    inner_join_rels,
                    *jointype,
                    *rtindex,
                    sjinfo_quals,
                )?;
                // Semijoins build an sjinfo but distribute their quals with
                // ojscope = NULL and no nonnullable side (C's hybrid case).
                let ojscope = if *jointype == types_pathnodes::JOIN_SEMI {
                    None
                } else {
                    relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand)
                };
                let full_nonnullable;
                let nonnullable = if *jointype == types_pathnodes::JOIN_SEMI {
                    &None
                } else if *jointype == types_pathnodes::JOIN_FULL {
                    // Each side of a FULL join is both outer and inner.
                    full_nonnullable = relids_copy(mcx, qualscope);
                    &full_nonnullable
                } else {
                    left_rels
                };
                let mut postponed: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
                // Non-degenerate quals of a strict-LHS LEFT join are
                // postponed (commute_below_l/r additions are dead: multi-OJ
                // commute is loud in make_outerjoininfo).
                let postpone = *jointype == JOIN_LEFT && sjinfo.lhs_strict;
                distribute_quals_to_rels(
                    run,
                    *quals,
                    qualscope,
                    &ojscope,
                    nonnullable,
                    Some(&sjinfo),
                    *jdomain,
                    plain_security_level,
                    if postpone { Some(&mut postponed) } else { None },
                    Some((&items, idx)),
                    &mut lateral_pending,
                )?;
                let sj_index = run.root.join_info_list.len();
                run.root.join_info_list.push(sjinfo);
                if !postponed.is_empty() {
                    oj_postponed.push((sj_index, postponed));
                }
            }
        }
    }

    // deconstruct_distribute_oj_quals, no-commutation arm (commuting OJ pairs
    // panic in make_outerjoininfo, so the clone-variant machinery is dead).
    for (sj_index, postponed) in oj_postponed {
        let sjinfo = run.root.join_info_list[sj_index].clone();
        let qualscope = relids_add_member(
            mcx,
            &relids_union(mcx, &sjinfo.syn_lefthand, &sjinfo.syn_righthand),
            sjinfo.ojrelid,
        );
        let ojscope = relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand);
        let seclevel = run.root.qual_security_level;
        for clause in postponed {
            distribute_qual_to_rels(
                run,
                clause,
                &qualscope,
                &ojscope,
                &sjinfo.syn_lefthand,
                Some(&sjinfo),
                0,
                seclevel,
                None,
                None,
                &mut lateral_pending,
            )?;
        }
    }

    Ok(joinlist)
}

#[allow(clippy::too_many_arguments)]
fn distribute_quals_to_rels<'mcx>(
    run: &mut PlannerRun<'mcx>,
    quals: Option<Node<'mcx>>,
    qualscope: &types_pathnodes::Relids<'mcx>,
    ojscope: &types_pathnodes::Relids<'mcx>,
    outerjoin_nonnullable: &types_pathnodes::Relids<'mcx>,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    jdomain: usize,
    security_level: u32,
    mut postponed: Option<&mut PgVec<'mcx, Node<'mcx>>>,
    jt: Option<(&PgVec<'mcx, JtItem<'mcx>>, usize)>,
    lateral_pending: &mut PgVec<'mcx, PgVec<'mcx, Node<'mcx>>>,
) -> PgResult<()> {
    let Some(quals) = quals else { return Ok(()) };
    let list = quals
        .as_list()
        .expect("preprocessed quals are an implicit-AND list");
    for clause in list {
        distribute_qual_to_rels(
            run,
            clause,
            qualscope,
            ojscope,
            outerjoin_nonnullable,
            sjinfo,
            jdomain,
            security_level,
            postponed.as_deref_mut(),
            jt,
            lateral_pending,
        )?;
    }
    Ok(())
}

fn deconstruct_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    item: Node<'mcx>,
    parent_domain: usize,
    items: &mut PgVec<'mcx, JtItem<'mcx>>,
) -> PgResult<(
    types_pathnodes::Relids<'mcx>,
    types_pathnodes::Relids<'mcx>,
    PgVec<'mcx, JoinlistNode<'mcx>>,
)> {
    let mcx = run.mcx;
    match item.node_tag() {
        NodeTag::T_RangeTblRef => {
            let varno = item.as_range_tbl_ref().unwrap().rtindex;
            let scope = relids_singleton(mcx, varno as u32);
            run.root.join_domains[parent_domain].jd_relids = relids_union(
                mcx,
                &run.root.join_domains[parent_domain].jd_relids,
                &scope,
            );
            if run.root.qual_security_level > 0 {
                items.push(JtItem::SecQuals {
                    varno,
                    qualscope: relids_copy(mcx, &scope),
                    jdomain: parent_domain,
                });
            }
            let mut joinlist = PgVec::new_in(mcx);
            joinlist.push(JoinlistNode::Rel(varno));
            Ok((scope, None, joinlist))
        }
        NodeTag::T_FromExpr => {
            let f = item.as_from_expr().unwrap();
            let mut qualscope: types_pathnodes::Relids<'mcx> = None;
            let mut inner_join_rels: types_pathnodes::Relids<'mcx> = None;
            let mut joinlist = PgVec::new_in(mcx);
            for child in &f.fromlist {
                let (c_relids, c_inner, c_list) =
                    deconstruct_recurse(run, child, parent_domain, items)?;
                qualscope = relids_union(mcx, &qualscope, &c_relids);
                inner_join_rels = c_inner;
                for jl in c_list {
                    joinlist.push(jl);
                }
            }
            if f.fromlist.len() > 1 {
                inner_join_rels = relids_copy(mcx, &qualscope);
            }
            items.push(JtItem::Plain {
                quals: f.quals,
                qualscope: relids_copy(mcx, &qualscope),
                jdomain: parent_domain,
            });
            Ok((qualscope, inner_join_rels, joinlist))
        }
        NodeTag::T_JoinExpr => {
            let j = item.as_join_expr().unwrap();
            match j.jointype {
                types_nodes::JoinType::JOIN_INNER => {
                    let (l_relids, l_inner, l_list) =
                        deconstruct_recurse(run, j.larg, parent_domain, items)?;
                    let (r_relids, r_inner, r_list) =
                        deconstruct_recurse(run, j.rarg, parent_domain, items)?;
                    let _ = (l_inner, r_inner);
                    let scope = relids_union(mcx, &l_relids, &r_relids);
                    items.push(JtItem::Plain {
                        quals: j.quals,
                        qualscope: relids_copy(mcx, &scope),
                        jdomain: parent_domain,
                    });
                    let mut joinlist = l_list;
                    for jl in r_list {
                        joinlist.push(jl);
                    }
                    Ok((relids_copy(mcx, &scope), scope, joinlist))
                }
                types_nodes::JoinType::JOIN_LEFT | types_nodes::JoinType::JOIN_ANTI => {
                    let child_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: None });
                    let (l_relids, l_inner, l_list) =
                        deconstruct_recurse(run, j.larg, parent_domain, items)?;
                    let (r_relids, r_inner, r_list) =
                        deconstruct_recurse(run, j.rarg, child_domain, items)?;
                    let child_relids =
                        relids_copy(mcx, &run.root.join_domains[child_domain].jd_relids);
                    run.root.join_domains[parent_domain].jd_relids = relids_union(
                        mcx,
                        &run.root.join_domains[parent_domain].jd_relids,
                        &child_relids,
                    );
                    let mut qualscope = relids_union(mcx, &l_relids, &r_relids);
                    // An ANTI join derived from a SEMI (pull_up_sublinks)
                    // lacks an rtindex; a LEFT or reduced-LEFT ANTI has one.
                    if j.jointype == types_nodes::JoinType::JOIN_LEFT {
                        assert!(j.rtindex != 0, "LEFT JoinExpr lacks an rtindex");
                    }
                    if j.rtindex != 0 {
                        run.root.join_domains[parent_domain].jd_relids = relids_add_member(
                            mcx,
                            &run.root.join_domains[parent_domain].jd_relids,
                            j.rtindex as u32,
                        );
                        qualscope = relids_add_member(mcx, &qualscope, j.rtindex as u32);
                        run.root.outer_join_rels =
                            relids_add_member(mcx, &run.root.outer_join_rels, j.rtindex as u32);
                        mark_rels_nulled_by_join(run, j.rtindex, &r_relids);
                    }
                    let inner_join_rels = relids_union(mcx, &l_inner, &r_inner);
                    items.push(JtItem::Sj {
                        jointype: if j.jointype == types_nodes::JoinType::JOIN_LEFT {
                            JOIN_LEFT
                        } else {
                            JOIN_ANTI
                        },
                        quals: j.quals,
                        qualscope: relids_copy(mcx, &qualscope),
                        jdomain: parent_domain,
                        left_rels: relids_copy(mcx, &l_relids),
                        right_rels: relids_copy(mcx, &r_relids),
                        inner_join_rels: relids_copy(mcx, &inner_join_rels),
                        rtindex: j.rtindex,
                    });
                    let mut joinlist = l_list;
                    for jl in r_list {
                        joinlist.push(jl);
                    }
                    Ok((qualscope, inner_join_rels, joinlist))
                }
                types_nodes::JoinType::JOIN_SEMI => {
                    let (l_relids, l_inner, l_list) =
                        deconstruct_recurse(run, j.larg, parent_domain, items)?;
                    let (r_relids, r_inner, r_list) =
                        deconstruct_recurse(run, j.rarg, parent_domain, items)?;
                    let qualscope = relids_union(mcx, &l_relids, &r_relids);
                    debug_assert!(j.rtindex == 0);
                    let inner_join_rels = relids_union(mcx, &l_inner, &r_inner);
                    items.push(JtItem::Sj {
                        jointype: types_pathnodes::JOIN_SEMI,
                        quals: j.quals,
                        qualscope: relids_copy(mcx, &qualscope),
                        jdomain: parent_domain,
                        left_rels: relids_copy(mcx, &l_relids),
                        right_rels: relids_copy(mcx, &r_relids),
                        inner_join_rels: relids_copy(mcx, &inner_join_rels),
                        rtindex: 0,
                    });
                    let mut joinlist = l_list;
                    for jl in r_list {
                        joinlist.push(jl);
                    }
                    Ok((qualscope, inner_join_rels, joinlist))
                }
                types_nodes::JoinType::JOIN_FULL => {
                    // The FULL join's quals get their very own domain; each
                    // side gets its own child domain.
                    let fj_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: None });
                    let l_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: None });
                    let (l_relids, l_inner, l_list) =
                        deconstruct_recurse(run, j.larg, l_domain, items)?;
                    run.root.join_domains[fj_domain].jd_relids =
                        relids_copy(mcx, &run.root.join_domains[l_domain].jd_relids);
                    let r_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: None });
                    let (r_relids, r_inner, r_list) =
                        deconstruct_recurse(run, j.rarg, r_domain, items)?;
                    let r_dom_relids = relids_copy(mcx, &run.root.join_domains[r_domain].jd_relids);
                    run.root.join_domains[fj_domain].jd_relids = relids_union(
                        mcx,
                        &run.root.join_domains[fj_domain].jd_relids,
                        &r_dom_relids,
                    );
                    let fj_relids = relids_copy(mcx, &run.root.join_domains[fj_domain].jd_relids);
                    run.root.join_domains[parent_domain].jd_relids = relids_union(
                        mcx,
                        &run.root.join_domains[parent_domain].jd_relids,
                        &fj_relids,
                    );
                    let mut qualscope = relids_union(mcx, &l_relids, &r_relids);
                    assert!(j.rtindex != 0, "FULL JoinExpr lacks an rtindex");
                    run.root.join_domains[parent_domain].jd_relids = relids_add_member(
                        mcx,
                        &run.root.join_domains[parent_domain].jd_relids,
                        j.rtindex as u32,
                    );
                    qualscope = relids_add_member(mcx, &qualscope, j.rtindex as u32);
                    run.root.outer_join_rels =
                        relids_add_member(mcx, &run.root.outer_join_rels, j.rtindex as u32);
                    mark_rels_nulled_by_join(run, j.rtindex, &l_relids);
                    mark_rels_nulled_by_join(run, j.rtindex, &r_relids);
                    let inner_join_rels = relids_union(mcx, &l_inner, &r_inner);
                    items.push(JtItem::Sj {
                        jointype: types_pathnodes::JOIN_FULL,
                        quals: j.quals,
                        qualscope: relids_copy(mcx, &qualscope),
                        jdomain: fj_domain,
                        left_rels: relids_copy(mcx, &l_relids),
                        right_rels: relids_copy(mcx, &r_relids),
                        inner_join_rels: relids_copy(mcx, &inner_join_rels),
                        rtindex: j.rtindex,
                    });
                    // Force the join order exactly at this node:
                    // list_make1(list_make2(leftjoinlist, rightjoinlist)).
                    let mut left_sub = PgVec::new_in(mcx);
                    for jl in l_list {
                        left_sub.push(jl);
                    }
                    let mut right_sub = PgVec::new_in(mcx);
                    for jl in r_list {
                        right_sub.push(jl);
                    }
                    let mut pair = PgVec::new_in(mcx);
                    pair.push(JoinlistNode::Sub(left_sub));
                    pair.push(JoinlistNode::Sub(right_sub));
                    let mut joinlist = PgVec::new_in(mcx);
                    joinlist.push(JoinlistNode::Sub(pair));
                    Ok((qualscope, inner_join_rels, joinlist))
                }
                other => panic!(
                    "deconstruct_recurse (initsplan.c): {other:?} arm; join-outer lane covers \
                     INNER/LEFT/FULL/SEMI/ANTI (RIGHT flips in reduce_outer_joins)"
                ),
            }
        }
        other => panic!("deconstruct_recurse (initsplan.c): {other:?} jointree item; M2 join lane"),
    }
}

// mark_rels_nulled_by_join (initsplan.c); RTE_GROUP is loud upstream.
fn mark_rels_nulled_by_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ojrelid: i32,
    lower_rels: &types_pathnodes::Relids<'mcx>,
) {
    let mcx = run.mcx;
    let mut members: PgVec<'_, i32> = PgVec::new_in(mcx);
    members.extend(relids_members(lower_rels));
    for relid in members {
        if relids_is_member(relid, &run.root.outer_join_rels) {
            continue;
        }
        let rel = find_base_rel(&run.root, relid);
        let nulled = relids_add_member(
            mcx,
            &run.root.rel(rel).nulling_relids.clone(),
            ojrelid as u32,
        );
        run.root.rel_mut(rel).nulling_relids = nulled;
    }
}

// make_outerjoininfo (initsplan.c), LEFT/SEMI/ANTI arms (FULL is loud
// upstream); the identity-3 commute legs are loud (multi-OJ clone machinery
// is the join-outer follow-on).
fn make_outerjoininfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    left_rels: &types_pathnodes::Relids<'mcx>,
    right_rels: &types_pathnodes::Relids<'mcx>,
    inner_join_rels: &types_pathnodes::Relids<'mcx>,
    jointype: types_pathnodes::JoinType,
    ojrelid: i32,
    clause: Option<Node<'mcx>>,
) -> PgResult<SpecialJoinInfo<'mcx>> {
    let mcx = run.mcx;
    assert!(
        run.parse().rowMarks.is_nil(),
        "make_outerjoininfo (initsplan.c): FOR UPDATE/SHARE vs nullable side check unported"
    );

    let mut sjinfo = SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: relids_copy(mcx, left_rels),
        syn_righthand: relids_copy(mcx, right_rels),
        jointype,
        ojrelid: ojrelid as u32,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: PgVec::new_in(mcx),
        semi_rhs_exprs: PgVec::new_in(mcx),
    };
    compute_semijoin_info(run, &mut sjinfo, clause)?;

    // If it's a full join, no need to be very smart.
    if jointype == types_pathnodes::JOIN_FULL {
        sjinfo.min_lefthand = relids_copy(mcx, left_rels);
        sjinfo.min_righthand = relids_copy(mcx, right_rels);
        sjinfo.lhs_strict = false;
        return Ok(sjinfo);
    }

    let clause_relids = match clause {
        Some(c) => pull_varnos_relids(run, c)?,
        None => None,
    };
    let strict_bms = clauses::find_nonnullable_rels(mcx, clause)?;
    let mut strict_relids: types_pathnodes::Relids<'mcx> = None;
    for x in strict_bms.iter() {
        strict_relids = relids_add_member(mcx, &strict_relids, x as u32);
    }

    let lhs_strict = relids_overlap(&strict_relids, left_rels);
    let mut min_lefthand = relids_intersect(mcx, &clause_relids, left_rels);
    let mut min_righthand = relids_intersect(
        mcx,
        &relids_union(mcx, &clause_relids, inner_join_rels),
        right_rels,
    );

    let is_semi_or_anti = matches!(jointype, types_pathnodes::JOIN_SEMI | JOIN_ANTI);
    for i in 0..run.root.join_info_list.len() {
        let other = run.root.join_info_list[i].clone();
        assert!(
            matches!(
                other.jointype,
                JOIN_LEFT | types_pathnodes::JOIN_SEMI | JOIN_ANTI | types_pathnodes::JOIN_FULL
            ),
            "make_outerjoininfo (initsplan.c): lower {} join ordering arm; join-outer lane",
            other.jointype
        );
        assert!(
            run.root.placeholder_list.is_empty(),
            "make_outerjoininfo (initsplan.c): placeholder_list crawl unported"
        );
        // A full join is an optimization barrier: expand whichever side
        // overlaps it to cover the whole full join.
        if other.jointype == types_pathnodes::JOIN_FULL {
            assert!(
                other.ojrelid != 0,
                "make_outerjoininfo (initsplan.c): FULL JOIN without ojrelid"
            );
            if relids_overlap(left_rels, &other.syn_lefthand)
                || relids_overlap(left_rels, &other.syn_righthand)
            {
                min_lefthand = relids_union(mcx, &min_lefthand, &other.syn_lefthand);
                min_lefthand = relids_union(mcx, &min_lefthand, &other.syn_righthand);
                min_lefthand = relids_add_member(mcx, &min_lefthand, other.ojrelid);
            }
            if relids_overlap(right_rels, &other.syn_lefthand)
                || relids_overlap(right_rels, &other.syn_righthand)
            {
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_lefthand);
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_righthand);
                min_righthand = relids_add_member(mcx, &min_righthand, other.ojrelid);
            }
            continue;
        }
        if relids_overlap(left_rels, &other.syn_righthand) {
            if relids_overlap(&clause_relids, &other.syn_righthand)
                && (is_semi_or_anti || !relids_overlap(&strict_relids, &other.min_righthand))
            {
                min_lefthand = relids_union(mcx, &min_lefthand, &other.syn_lefthand);
                min_lefthand = relids_union(mcx, &min_lefthand, &other.syn_righthand);
                if other.ojrelid != 0 {
                    min_lefthand = relids_add_member(mcx, &min_lefthand, other.ojrelid);
                }
            } else if jointype == JOIN_LEFT
                && other.jointype == JOIN_LEFT
                && relids_overlap(&strict_relids, &other.min_righthand)
                && !relids_overlap(&clause_relids, &other.syn_lefthand)
            {
                panic!(
                    "make_outerjoininfo (initsplan.c): OJ identity 3 commute \
                     (commute_below_l); multi-outer-join lane"
                );
            }
        }
        if relids_overlap(right_rels, &other.syn_righthand) {
            let other_semi_or_anti =
                matches!(other.jointype, types_pathnodes::JOIN_SEMI | JOIN_ANTI);
            if relids_overlap(&clause_relids, &other.syn_righthand)
                || !relids_overlap(&clause_relids, &other.min_lefthand)
                || is_semi_or_anti
                || other_semi_or_anti
                || !other.lhs_strict
            {
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_lefthand);
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_righthand);
                if other.ojrelid != 0 {
                    min_righthand = relids_add_member(mcx, &min_righthand, other.ojrelid);
                }
            } else {
                panic!(
                    "make_outerjoininfo (initsplan.c): OJ identity 3 commute \
                     (commute_below_r); multi-outer-join lane"
                );
            }
        }
    }

    if relids_is_empty(&min_lefthand) {
        min_lefthand = relids_copy(mcx, left_rels);
    }
    if relids_is_empty(&min_righthand) {
        min_righthand = relids_copy(mcx, right_rels);
    }
    debug_assert!(!relids_is_empty(&min_lefthand));
    debug_assert!(!relids_is_empty(&min_righthand));
    debug_assert!(!relids_overlap(&min_lefthand, &min_righthand));

    sjinfo.min_lefthand = min_lefthand;
    sjinfo.min_righthand = min_righthand;
    sjinfo.lhs_strict = lhs_strict;
    Ok(sjinfo)
}

// compute_semijoin_info (initsplan.c).
fn compute_semijoin_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sjinfo: &mut SpecialJoinInfo<'mcx>,
    clause: Option<Node<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    if sjinfo.jointype != types_pathnodes::JOIN_SEMI {
        return Ok(());
    }
    let mut semi_operators: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    let mut semi_rhs_exprs: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let mut all_btree = true;
    let mut all_hash = crate::gucs::enable_hashagg();
    let clause_list: PgVec<'mcx, Node<'mcx>> = {
        let mut v = PgVec::new_in(mcx);
        match clause {
            None => {}
            Some(c) => match c.as_list() {
                Some(l) => v.extend(l.iter()),
                None => v.push(c),
            },
        }
        v
    };
    for op_node in clause_list {
        let opexpr = op_node.as_op_expr().filter(|o| o.args.len() == 2);
        let Some(o) = opexpr else {
            let all_varnos = pull_varnos_relids(run, op_node)?;
            if !relids_overlap(&all_varnos, &sjinfo.syn_righthand)
                || relids_is_subset(&all_varnos, &sjinfo.syn_righthand)
            {
                if clauses::contain_volatile_functions(op_node)? {
                    return Ok(());
                }
                continue;
            }
            return Ok(());
        };
        let mut opno = o.opno;
        let left_expr = o.args.nth(0);
        let mut right_expr = o.args.nth(1);
        let left_varnos = pull_varnos_relids(run, left_expr)?;
        let right_varnos = pull_varnos_relids(run, right_expr)?;
        let all_varnos = relids_union(mcx, &left_varnos, &right_varnos);
        let opinputtype = crate::costsize::expr_type_typmod(left_expr).0;

        if !relids_overlap(&all_varnos, &sjinfo.syn_righthand)
            || relids_is_subset(&all_varnos, &sjinfo.syn_righthand)
        {
            if clauses::contain_volatile_functions(op_node)? {
                return Ok(());
            }
            continue;
        }

        if !relids_is_empty(&right_varnos)
            && relids_is_subset(&right_varnos, &sjinfo.syn_righthand)
            && !relids_overlap(&left_varnos, &sjinfo.syn_righthand)
        {
            // typical case, right_expr is the RHS variable
        } else if !relids_is_empty(&left_varnos)
            && relids_is_subset(&left_varnos, &sjinfo.syn_righthand)
            && !relids_overlap(&right_varnos, &sjinfo.syn_righthand)
        {
            opno = lsyscache::get_commutator(opno)?;
            if opno == 0 {
                return Ok(());
            }
            right_expr = left_expr;
        } else {
            return Ok(());
        }

        if all_btree
            && (!lsyscache::op_mergejoinable(opno, opinputtype)?
                || lsyscache::get_mergejoin_opfamilies(mcx, opno)?.is_empty())
        {
            all_btree = false;
        }
        if all_hash && !lsyscache::op_hashjoinable(opno, opinputtype)? {
            all_hash = false;
        }
        if !(all_btree || all_hash) {
            return Ok(());
        }

        semi_operators.push(opno);
        // C copyObject; the arena share is our copy model.
        semi_rhs_exprs.push(run.intern_expr(right_expr));
    }

    if semi_rhs_exprs.is_empty() {
        return Ok(());
    }
    for &id in semi_rhs_exprs.iter() {
        if clauses::contain_volatile_functions(*run.root.expr_node(id))? {
            return Ok(());
        }
    }
    sjinfo.semi_can_btree = all_btree;
    sjinfo.semi_can_hash = all_hash;
    sjinfo.semi_operators = semi_operators;
    sjinfo.semi_rhs_exprs = semi_rhs_exprs;
    Ok(())
}

// check_redundant_nullability_qual (initsplan.c): an IS NULL forced-null Var
// nulled by a lower antijoin is necessarily true.
fn check_redundant_nullability_qual(run: &PlannerRun<'_>, clause: Node<'_>) -> bool {
    let Some(var) = clauses::find_forced_null_var(clause) else {
        return false;
    };
    if var.varnullingrels.is_empty() {
        return false;
    }
    run.root.join_info_list.iter().any(|sj| {
        sj.jointype == JOIN_ANTI
            && sj.ojrelid != 0
            && var.varnullingrels.is_member(sj.ojrelid as i32)
    })
}

// distribute_qual_to_rels (initsplan.c). allow_equivalence is true for every
// live caller (the clone-variant OJ commutation that passes false is loud in
// make_outerjoininfo), so maybe_equivalence needs no parameter.
#[allow(clippy::too_many_arguments)]
fn distribute_qual_to_rels<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clause: Node<'mcx>,
    qualscope: &types_pathnodes::Relids<'mcx>,
    ojscope: &types_pathnodes::Relids<'mcx>,
    outerjoin_nonnullable: &types_pathnodes::Relids<'mcx>,
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
    jdomain: usize,
    security_level: u32,
    postponed: Option<&mut PgVec<'mcx, Node<'mcx>>>,
    jt: Option<(&PgVec<'mcx, JtItem<'mcx>>, usize)>,
    lateral_pending: &mut PgVec<'mcx, PgVec<'mcx, Node<'mcx>>>,
) -> PgResult<()> {
    debug_assert!(clause.node_tag() != NodeTag::T_List);
    let mut relids = pull_varnos_relids(run, clause)?;
    if !crate::relnode::relids_is_subset(&relids, qualscope) {
        // A qual pulled up from a LATERAL subquery can reference rels outside
        // its syntactic scope; attach it to the nearest enclosing jointree
        // level whose scope covers every referenced rel.
        assert!(
            run.root.hasLateralRTEs,
            "distribute_qual_to_rels (initsplan.c): qual outside qualscope without \
             lateral RTEs"
        );
        assert!(
            sjinfo.is_none(),
            "mustn't postpone lateral qual past outer join"
        );
        let (items, idx) = jt.expect("lateral qual postponement outside deconstruct_jointree");
        for pidx in idx + 1..items.len() {
            let pscope = match &items[pidx] {
                JtItem::Plain { qualscope, .. } | JtItem::Sj { qualscope, .. } => qualscope,
                // C postpones onto jointree-level jtitems; SecQuals items are
                // this port's RTE-attached extras — never a postponement target.
                JtItem::SecQuals { .. } => continue,
            };
            if crate::relnode::relids_is_subset(&relids, pscope) {
                lateral_pending[pidx].push(clause);
                return Ok(());
            }
        }
        panic!("failed to postpone qual containing lateral reference");
    }
    assert!(
        ojscope.is_none() || relids_is_subset(&relids, ojscope),
        "JOIN qualification cannot refer to other relations"
    );
    let mut pseudoconstant = false;
    if relids_is_empty(&relids) {
        if ojscope.is_some() {
            relids = relids_copy(run.mcx, ojscope);
        } else if clauses::contain_volatile_functions(clause)? {
            relids = crate::relnode::relids_copy(run.mcx, qualscope);
        } else {
            pseudoconstant = true;
            run.root.hasPseudoConstantQuals = true;
            relids = if jdomain == 0 {
                crate::relnode::relids_copy(run.mcx, &run.root.join_domains[0].jd_relids)
            } else {
                crate::relnode::relids_copy(run.mcx, qualscope)
            };
        }
    }

    let is_pushed_down;
    let maybe_outer_join;
    if relids_overlap(&relids, outerjoin_nonnullable) {
        // Non-degenerate outer-join clause.
        if let Some(postponed) = postponed {
            postponed.push(clause);
            return Ok(());
        }
        is_pushed_down = false;
        maybe_outer_join = true;
        debug_assert!(ojscope.is_some());
        relids = relids_copy(run.mcx, ojscope);
        debug_assert!(!pseudoconstant);
    } else {
        is_pushed_down = true;
        if check_redundant_nullability_qual(run, clause) {
            return Ok(());
        }
        maybe_outer_join = false;
    }

    let rinfo = make_restrictinfo(
        run,
        clause,
        is_pushed_down,
        false,
        false,
        pseudoconstant,
        security_level,
        relids,
        None,
        relids_copy(run.mcx, outerjoin_nonnullable),
    )?;

    if relids_num_members(&run.root.rinfo(rinfo).required_relids) > 1 {
        let mut vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(run.mcx);
        pull_var_nodes(clause, &mut vars);
        let where_needed = relids_copy(run.mcx, &run.root.rinfo(rinfo).required_relids);
        add_vars_to_targetlist(run, &vars, &where_needed)?;
    }

    check_mergejoinable(run, rinfo)?;
    let mut rinfo = rinfo;
    if !run.root.rinfo(rinfo).mergeopfamilies.is_empty() {
        if !maybe_outer_join {
            if crate::equivclass::process_equivalence(run, &mut rinfo, jdomain)? {
                return Ok(());
            }
            // EC rejected it; set left_ec/right_ec the hard way.
            if !run.root.rinfo(rinfo).mergeopfamilies.is_empty() {
                crate::pathkeys::initialize_mergeclause_eclasses(run, rinfo)?;
            }
        } else if run.root.rinfo(rinfo).can_join {
            crate::pathkeys::initialize_mergeclause_eclasses(run, rinfo)?;
            let (left_sub, right_over, right_sub, left_over) = {
                let ri = run.root.rinfo(rinfo);
                (
                    relids_is_subset(&ri.left_relids, outerjoin_nonnullable),
                    relids_overlap(&ri.right_relids, outerjoin_nonnullable),
                    relids_is_subset(&ri.right_relids, outerjoin_nonnullable),
                    relids_overlap(&ri.left_relids, outerjoin_nonnullable),
                )
            };
            let sjinfo = sjinfo
                .expect("outer-join clause carries its sjinfo")
                .clone();
            if left_sub && !right_over {
                run.root
                    .left_join_clauses
                    .push(OuterJoinClauseInfo { rinfo, sjinfo });
                return Ok(());
            }
            if right_sub && !left_over {
                run.root
                    .right_join_clauses
                    .push(OuterJoinClauseInfo { rinfo, sjinfo });
                return Ok(());
            }
            if sjinfo.jointype == types_pathnodes::JOIN_FULL {
                // FULL JOIN: the one-sided tests above can never match.
                run.root
                    .full_join_clauses
                    .push(OuterJoinClauseInfo { rinfo, sjinfo });
                return Ok(());
            }
        } else {
            crate::pathkeys::initialize_mergeclause_eclasses(run, rinfo)?;
        }
    }
    distribute_restrictinfo_to_rels(run, rinfo)
}

pub(crate) fn pull_varnos_relids<'mcx>(
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
    // C divergence: no make_sub_restrictinfos — orclause stays None; cost and
    // selectivity recurse over bare arg nodes (same numerics, no per-arm
    // memo); the OR index path is a loud panel in indxpath.
    debug_assert!(clause.node_tag() != NodeTag::T_List);

    // C skips the contain_leaked_vars probe for level-zero quals ("really,
    // don't know") — they can never be delayed on security grounds.
    let leakproof = if security_level > 0 {
        !clauses::contain_leaked_vars(clause)?
    } else {
        false
    };

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
    let num_base_rels = relids_num_members(&relids_difference(
        mcx,
        &clause_relids,
        &run.root.outer_join_rels,
    ));

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
        eval_cost: QualCost {
            startup: -1.0,
            per_tuple: 0.0,
        },
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

// commute_restrictinfo (restrictinfo.c): cached selectivity/cost, parent_ec,
// and rinfo_serial carry over; scansel_cache does not.
pub fn commute_restrictinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rid: RinfoId,
    comm_op: types_core::Oid,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
    let op = clause.as_op_expr().expect("commute_restrictinfo: OpExpr");
    assert!(op.args.len() == 2);
    let newclause = types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: comm_op,
            opfuncid: lsyscache::get_opcode(comm_op)?,
            opresulttype: op.opresulttype,
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            args: types_nodes::NodeList::make2(mcx, op.args.nth(1), op.args.nth(0))?,
            location: op.location,
        },
    )?;
    let mut ri = run.root.rinfo(rid).clone();
    ri.clause = run.intern_expr(newclause);
    ri.left_relids = run.root.rinfo(rid).right_relids.clone();
    ri.right_relids = run.root.rinfo(rid).left_relids.clone();
    assert!(ri.orclause.is_none());
    ri.left_ec = run.root.rinfo(rid).right_ec;
    ri.right_ec = run.root.rinfo(rid).left_ec;
    ri.left_em = run.root.rinfo(rid).right_em;
    ri.right_em = run.root.rinfo(rid).left_em;
    ri.scansel_cache = PgVec::new_in(mcx);
    ri.hashjoinoperator = if run.root.rinfo(rid).hashjoinoperator == op.opno {
        comm_op
    } else {
        0
    };
    ri.left_bucketsize = run.root.rinfo(rid).right_bucketsize;
    ri.right_bucketsize = run.root.rinfo(rid).left_bucketsize;
    ri.left_mcvfreq = run.root.rinfo(rid).right_mcvfreq;
    ri.right_mcvfreq = run.root.rinfo(rid).left_mcvfreq;
    ri.left_hasheqoperator = 0;
    ri.right_hasheqoperator = 0;
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
    if lsyscache::op_mergejoinable(opno, lefttype)? && !clauses::contain_volatile_functions(clause)?
    {
        let fams = lsyscache::get_mergejoin_opfamilies(run.mcx, opno)?;
        run.root.rinfo_mut(rinfo).mergeopfamilies = fams;
    }
    Ok(())
}

// check_memoizable (initsplan.c): the hasheqoperator fields feed
// paraminfo_get_equal_hashops; get_memoize_path only reads them off
// ppi_clauses (join clauses), matching C's call sites.
fn check_memoizable(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    if run.root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return Ok(());
    };
    let lefttype = crate::costsize::expr_type_typmod(o.args.nth(0)).0;
    let righttype = crate::costsize::expr_type_typmod(o.args.nth(1)).0;
    let flags = typcache::TYPECACHE_HASH_PROC | typcache::TYPECACHE_EQ_OPR;
    let entry = typcache::lookup_type_cache(lefttype, flags)?;
    if entry.hash_proc() != 0 && entry.eq_opr() != 0 {
        run.root.rinfo_mut(rinfo).left_hasheqoperator = entry.eq_opr();
    }
    let entry =
        if lefttype == righttype { entry } else { typcache::lookup_type_cache(righttype, flags)? };
    if entry.hash_proc() != 0 && entry.eq_opr() != 0 {
        run.root.rinfo_mut(rinfo).right_hasheqoperator = entry.eq_opr();
    }
    Ok(())
}

// check_hashjoinable (initsplan.c): mark the clause hashable so
// hash_inner_and_outer can collect it.
fn check_hashjoinable(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    if run.root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return Ok(());
    };
    let (opno, args0) = (o.opno, o.args.nth(0));
    let lefttype = crate::costsize::expr_type_typmod(args0).0;
    if lsyscache::op_hashjoinable(opno, lefttype)? && !clauses::contain_volatile_functions(clause)?
    {
        run.root.rinfo_mut(rinfo).hashjoinoperator = opno;
    }
    Ok(())
}

// distribute_restrictinfo_to_rels (initsplan.c).
pub fn distribute_restrictinfo_to_rels(run: &mut PlannerRun<'_>, rinfo: RinfoId) -> PgResult<()> {
    let relids = relids_copy(run.mcx, &run.root.rinfo(rinfo).required_relids);
    if let Some(relid) = relids_singleton_member(&relids) {
        return add_base_clause_to_rel(run, relid, rinfo);
    }
    check_hashjoinable(run, rinfo)?;
    check_memoizable(run, rinfo)?;
    // add_join_clause_to_rels (joininfo.c): one shared RestrictInfo handle
    // linked into every participating baserel's joininfo list (outer-join
    // relids have no RelOptInfo — C's find_base_rel_ignore_join skip).
    if restriction_is_always_true(run, rinfo) {
        return Ok(());
    }
    let rinfo = substitute_false_if_always_false(run, rinfo)?;
    debug_assert!(relids_num_members(&relids) > 1);
    let members = relids.as_ref().expect("multi-member relids");
    for (i, w) in members.word_slice().iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            let relid = (i * 64) as i32 + w.trailing_zeros() as i32;
            w &= w - 1;
            if !crate::relnode::relids_is_member(relid, &run.root.all_baserels) {
                debug_assert!(relids_is_member(relid, &run.root.outer_join_rels));
                continue;
            }
            let rel = find_base_rel(&run.root, relid);
            run.root.rel_mut(rel).joininfo.push(rinfo);
        }
    }
    Ok(())
}

// add_base_clause_to_rel (initsplan.c:3003); the NullTest shortcuts are only
// valid for non-inheritance parents, except partitioned tables where a
// constant qual must hold for every partition too.
fn add_base_clause_to_rel(run: &mut PlannerRun<'_>, relid: i32, rinfo: RinfoId) -> PgResult<()> {
    let rte = run.rte(relid as usize);
    let apply_shortcuts = !rte.inh || rte.relkind == ::types_rel::RELKIND_PARTITIONED_TABLE;
    if apply_shortcuts && restriction_is_always_true(run, rinfo) {
        return Ok(());
    }
    let rinfo = if apply_shortcuts {
        substitute_false_if_always_false(run, rinfo)?
    } else {
        rinfo
    };
    let rel_id = find_base_rel(&run.root, relid);
    let security_level = run.root.rinfo(rinfo).security_level;
    let rel = run.root.rel_mut(rel_id);
    rel.baserestrictinfo.push(rinfo);
    rel.baserestrict_min_security = rel.baserestrict_min_security.min(security_level);
    Ok(())
}

// restriction_is_always_true / _false (initsplan.c), NullTest leg only: the
// OR leg reads orclause sub-RestrictInfos, which stay None here (documented
// make_restrictinfo divergence).
pub(crate) fn restriction_is_always_true(run: &PlannerRun<'_>, rinfo: RinfoId) -> bool {
    restriction_nulltest_verdict(
        run,
        rinfo,
        types_nodes::primnodes::NullTestType::IS_NOT_NULL,
    )
}

pub(crate) fn restriction_is_always_false(run: &PlannerRun<'_>, rinfo: RinfoId) -> bool {
    restriction_nulltest_verdict(run, rinfo, types_nodes::primnodes::NullTestType::IS_NULL)
}

fn restriction_nulltest_verdict(
    run: &PlannerRun<'_>,
    rinfo: RinfoId,
    testtype: types_nodes::primnodes::NullTestType,
) -> bool {
    let ri = run.root.rinfo(rinfo);
    // Clone clauses' nullingrel bits may not reflect reality (C's guard).
    if ri.has_clone || ri.is_clone {
        return false;
    }
    let clause = *run.root.expr_node(ri.clause);
    let Some(nt) = clause.as_null_test() else {
        return false;
    };
    if nt.nulltesttype != testtype || nt.argisrow {
        return false;
    }
    expr_is_nonnullable(run, nt.arg.expect("NullTest.arg"))
}

// expr_is_nonnullable (initsplan.c): simple Vars only.
fn expr_is_nonnullable(run: &PlannerRun<'_>, expr: Node<'_>) -> bool {
    let Some(var) = expr.as_var() else {
        return false;
    };
    if !var.varnullingrels.is_empty() {
        return false;
    }
    if var.varattno < 0 {
        return true;
    }
    let rel = find_base_rel(&run.root, var.varno);
    var.varattno > 0 && relids_is_member(var.varattno as i32, &run.root.rel(rel).notnullattnums)
}

// The always-false substitution shared by add_base_clause_to_rel and
// add_join_clause_to_rels: constant-FALSE under the original rinfo_serial.
fn substitute_false_if_always_false<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
) -> PgResult<RinfoId> {
    if !restriction_is_always_false(run, rinfo) {
        return Ok(rinfo);
    }
    let save_rinfo_serial = run.root.rinfo(rinfo).rinfo_serial;
    let save_last_rinfo_serial = run.root.last_rinfo_serial;
    let (is_pushed_down, has_clone, is_clone, pseudoconstant, required, incompatible, outer) = {
        let ri = run.root.rinfo(rinfo);
        (
            ri.is_pushed_down,
            ri.has_clone,
            ri.is_clone,
            ri.pseudoconstant,
            relids_copy(run.mcx, &ri.required_relids),
            relids_copy(run.mcx, &ri.incompatible_relids),
            relids_copy(run.mcx, &ri.outer_relids),
        )
    };
    let clause = clauses::make_bool_const(run.mcx, false, false)?;
    let new_rinfo = make_restrictinfo(
        run,
        clause,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        0,
        required,
        incompatible,
        outer,
    )?;
    run.root.rinfo_mut(new_rinfo).rinfo_serial = save_rinfo_serial;
    run.root.last_rinfo_serial = save_last_rinfo_serial;
    Ok(new_rinfo)
}

// process_implied_equality (initsplan.c): build "item1 op item2" and push it
// into the restrictinfo lists. None = reduced to constant TRUE and dropped.
#[allow(clippy::too_many_arguments)]
pub(crate) fn process_implied_equality<'mcx>(
    run: &mut PlannerRun<'mcx>,
    opno: u32,
    collation: u32,
    item1: Node<'mcx>,
    item2: Node<'mcx>,
    qualscope: types_pathnodes::Relids<'mcx>,
    security_level: u32,
    both_const: bool,
) -> PgResult<Option<RinfoId>> {
    let mcx = run.mcx;
    // C copyObject's both items; the arena share is our copy model.
    let mut clause = crate::like_support::make_opclause(mcx, opno, item1, item2, collation)?;
    if both_const {
        clause = clauses::fold::eval_const_expressions(mcx, clause)?;
        if let Some(c) = clause.as_const() {
            debug_assert_eq!(c.consttype, 16);
            if !c.constisnull && c.constvalue.as_bool() {
                return Ok(None);
            }
        }
    }

    let mut relids = pull_varnos_relids(run, clause)?;
    debug_assert!(relids_is_subset(&relids, &qualscope));
    let mut pseudoconstant = false;
    if relids_is_empty(&relids) {
        relids = get_join_domain_min_rels(run, &qualscope);
        pseudoconstant = true;
        run.root.hasPseudoConstantQuals = true;
    }

    let rinfo = make_restrictinfo(
        run,
        clause,
        true,
        false,
        false,
        pseudoconstant,
        security_level,
        relids,
        None,
        None,
    )?;

    if relids_num_members(&run.root.rinfo(rinfo).required_relids) > 1 {
        let mut vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        pull_var_nodes(clause, &mut vars);
        let where_needed = relids_copy(mcx, &run.root.rinfo(rinfo).required_relids);
        add_vars_to_targetlist(run, &vars, &where_needed)?;
    }

    // Usually succeeds (the op came from an EC), unless the clause was
    // reduced to a constant.
    check_mergejoinable(run, rinfo)?;

    distribute_restrictinfo_to_rels(run, rinfo)?;
    Ok(Some(rinfo))
}

// build_implied_join_equality (initsplan.c): like process_implied_equality
// but never pushed into the joininfo tree.
pub(crate) fn build_implied_join_equality<'mcx>(
    run: &mut PlannerRun<'mcx>,
    opno: u32,
    collation: u32,
    item1: Node<'mcx>,
    item2: Node<'mcx>,
    qualscope: types_pathnodes::Relids<'mcx>,
    security_level: u32,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    let clause = crate::like_support::make_opclause(mcx, opno, item1, item2, collation)?;
    let rinfo = make_restrictinfo(
        run,
        clause,
        true,
        false,
        false,
        false,
        security_level,
        qualscope,
        None,
        None,
    )?;
    check_mergejoinable(run, rinfo)?;
    check_hashjoinable(run, rinfo)?;
    check_memoizable(run, rinfo)?;
    Ok(rinfo)
}

// get_join_domain_min_rels (initsplan.c): strip lower LEFT joins that could
// commute out from under a derived pseudoconstant qual.
fn get_join_domain_min_rels<'mcx>(
    run: &PlannerRun<'mcx>,
    domain_relids: &types_pathnodes::Relids<'mcx>,
) -> types_pathnodes::Relids<'mcx> {
    let mcx = run.mcx;
    let mut result = relids_copy(mcx, domain_relids);
    if crate::relnode::relids_equal(&result, &run.root.all_query_rels) {
        return result;
    }
    for sjinfo in run.root.join_info_list.iter() {
        if sjinfo.jointype == JOIN_LEFT && relids_is_member(sjinfo.ojrelid as i32, &result) {
            result = crate::relnode::relids_del_member(mcx, &result, sjinfo.ojrelid as i32);
            result = relids_difference(mcx, &result, &sjinfo.syn_righthand);
        }
    }
    result
}
