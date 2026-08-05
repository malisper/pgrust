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
        NodeTag::T_Var | NodeTag::T_PlaceHolderVar => out.push(node),
        NodeTag::T_Const | NodeTag::T_NextValueExpr => {}
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            debug_assert!(a.agglevelsup == 0);
            for d in &a.aggdirectargs {
                pull_var_nodes(d, out);
            }
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
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            pull_var_nodes(a.arg, out);
            if let Some(e) = a.elemexpr {
                pull_var_nodes(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            pull_var_nodes(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
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
        NodeTag::T_NullIfExpr => {
            for a in &node.as_null_if_expr().unwrap().args {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            pull_var_nodes(fs.arg, out);
            for a in &fs.newvals {
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
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                pull_var_nodes(a, out);
            }
        }
        NodeTag::T_FieldSelect => pull_var_nodes(node.as_field_select().unwrap().arg, out),
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
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in &x.named_args {
                pull_var_nodes(a, out);
            }
            for a in &x.args {
                pull_var_nodes(a, out);
            }
        }
        other => panic!("pull_var_clause (var.c): {other:?}; M2 expression lane"),
    }
}

// add_vars_to_targetlist (initsplan.c).
pub fn add_vars_to_targetlist<'mcx>(
    run: &mut PlannerRun<'mcx>,
    vars: &[Node<'mcx>],
    where_needed: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(!relids_is_empty(where_needed));
    for &node in vars {
        if let Some(phv) = node.as_place_holder_var() {
            let id = crate::placeholder::find_placeholder_info(run, phv)?;
            let cur = crate::relnode::relids_take(&mut run.root.phinfo_mut(id).ph_needed);
            run.root.phinfo_mut(id).ph_needed = relids_union(mcx, &cur, where_needed);
            continue;
        }
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
        if crate::relnode::relids_is_unset(&run.root.rel(rel_id).attr_needed[ndx]) {
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
        let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel_id).attr_needed[ndx]);
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
        if let Some(phv) = node.as_place_holder_var() {
            let id = crate::placeholder::find_placeholder_info(run, phv)
                .expect("find_placeholder_info after freeze");
            let cur = crate::relnode::relids_take(&mut run.root.phinfo_mut(id).ph_needed);
            run.root.phinfo_mut(id).ph_needed = relids_union(mcx, &cur, where_needed);
            continue;
        }
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
        let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel_id).attr_needed[ndx]);
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
        // A lateral tablesample argument (prepjointree lateral propagation)
        // carries level-zero Vars.
        RTEKind::RTE_RELATION => {
            if let Some(tsc) = rte.tablesample {
                vars = vars::pull_vars_of_level(mcx, tsc, 0)?;
            }
        }
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
        if node.node_tag() == NodeTag::T_PlaceHolderVar {
            // C copyObject: the pulled nodes are links into the RTE's own
            // subquery tree (query_cells_copy shares expression nodes), so
            // the level shift must work on a copy. Subquery-leg PHV
            // expressions also still need preprocessing (subquery_planner
            // only preprocessed level-zero PHVs in function/values RTEs).
            let phlevelsup = node.as_place_holder_var().unwrap().phlevelsup;
            if phlevelsup > 0 {
                let copy = rewrite_manip::copy_node(mcx, node)?;
                rewrite_manip::IncrementVarSublevelsUp(copy, -(phlevelsup as i32), 0)?;
                let phexpr = copy.as_place_holder_var().unwrap().phexpr;
                let newexpr = crate::subquery::preprocess_phv_expression(run, phexpr)?;
                // SAFETY: copy is exclusively owned (fresh copy_node above).
                unsafe {
                    copy.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|p| {
                        p.phexpr = newexpr;
                    })
                }
                .expect("PlaceHolderVar");
                newvars.push(copy);
                continue;
            }
            newvars.push(node);
            continue;
        }
        let v = node.as_var().expect("lateral refs are Vars or PHVs");
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

// create_lateral_join_info (initsplan.c).
pub fn create_lateral_join_info(run: &mut PlannerRun<'_>) -> PgResult<()> {
    if !run.root.hasLateralRTEs {
        return Ok(());
    }
    debug_assert!(run.root.placeholdersFrozen);
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
        let mut lateral_relids: types_pathnodes::Relids<'_> = crate::relnode::relids_empty();
        for i in 0..run.root.rel(rel).lateral_vars.len() {
            let id = run.root.rel(rel).lateral_vars[i];
            let node = *run.root.expr_node(id);
            if let Some(phv) = node.as_place_holder_var() {
                let phid = crate::placeholder::find_placeholder_info(run, phv)?;
                found_laterals = true;
                lateral_relids =
                    relids_union(mcx, &lateral_relids, &run.root.phinfo(phid).ph_eval_at);
                continue;
            }
            let v = node.as_var().expect("lateral_vars hold Vars or PHVs");
            found_laterals = true;
            lateral_relids = relids_add_member(mcx, &lateral_relids, v.varno as u32);
        }
        run.root.rel_mut(rel).direct_lateral_relids = relids_copy(mcx, &lateral_relids);
        run.root.rel_mut(rel).lateral_relids = lateral_relids;
    }

    // Lateral references within PHVs make the eval-at rels laterally depend on
    // the sources: directly for a baserel eval site, indirectly (lateral_relids
    // only) for a join eval site, and only baserels count as sources so
    // rearranged outer joins stay legal.
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let ph_lateral = relids_copy(mcx, &run.root.phinfo(id).ph_lateral);
        if relids_is_empty(&ph_lateral) {
            continue;
        }
        found_laterals = true;
        let eval_at = relids_copy(mcx, &run.root.phinfo(id).ph_eval_at);
        let lateral_refs = relids_intersect(mcx, &ph_lateral, &run.root.all_baserels);
        debug_assert!(!relids_is_empty(&lateral_refs));
        if let Some(varno) = relids_singleton_member(&eval_at) {
            let rel = find_base_rel(&run.root, varno);
            let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel).direct_lateral_relids);
            run.root.rel_mut(rel).direct_lateral_relids = relids_union(mcx, &cur, &lateral_refs);
            let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel).lateral_relids);
            run.root.rel_mut(rel).lateral_relids = relids_union(mcx, &cur, &lateral_refs);
        } else {
            for varno in relids_members(&eval_at) {
                // Outer-join relids in eval_at have no RelOptInfo slot
                // (find_base_rel_ignore_join).
                let Some(rel) = run.root.simple_rel_array[varno as usize] else {
                    continue;
                };
                let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel).lateral_relids);
                run.root.rel_mut(rel).lateral_relids = relids_union(mcx, &cur, &lateral_refs);
            }
        }
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
                let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel2).lateral_relids);
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
            let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel2).lateral_referencers);
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
    let mut qualscope: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    let mut joinlist = PgVec::new_in(mcx);
    let mut items: PgVec<'mcx, JtItem<'mcx>> = PgVec::new_in(mcx);
    // The top FromExpr goes through deconstruct_recurse's FromExpr arm in C,
    // so from_collapse_limit governs collapsing here too.
    let mut remaining = f.fromlist.len();
    for item in &f.fromlist {
        let (item_relids, _item_inner, item_joinlist) =
            deconstruct_recurse(run, item, 0, &mut items)?;
        qualscope = relids_union(mcx, &qualscope, &item_relids);
        remaining -= 1;
        if item_joinlist.len() <= 1
            || joinlist.len() + item_joinlist.len() + remaining
                <= crate::gucs::from_collapse_limit() as usize
        {
            for jl in item_joinlist {
                joinlist.push(jl);
            }
        } else {
            joinlist.push(JoinlistNode::Sub(item_joinlist));
        }
    }
    debug_assert!(!joinlist.is_empty());

    run.root.all_baserels = relids_difference(mcx, &qualscope, &run.root.outer_join_rels);
    run.root.all_query_rels = relids_copy(mcx, &qualscope);
    run.root.join_domains[0].jd_relids = relids_copy(mcx, &qualscope);
    items.push(JtItem::Plain {
        quals: f.quals,
        qualscope,
        jdomain: 0,
    });

    let mut oj_postponed: PgVec<'mcx, (usize, PgVec<'mcx, Node<'mcx>>)> = PgVec::new_in(mcx);
    // jdomain of each join_info_list entry, in list order (C reaches it via
    // the JoinTreeItem back-link).
    let mut sj_jdomains: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    // Per-JtItem quals postponed by children for carrying lateral references
    // (C: jtitem->lateral_clauses); ancestors sit later in the post-order.
    let mut lateral_pending: PgVec<'mcx, PgVec<'mcx, Node<'mcx>>> = PgVec::new_in(mcx);
    for _ in 0..items.len() {
        lateral_pending.push(PgVec::new_in(mcx));
    }
    let plain_security_level = run.root.qual_security_level;
    let no_incompat: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    for idx in 0..items.len() {
        let pending = core::mem::replace(&mut lateral_pending[idx], PgVec::new_in(mcx));
        // At a JoinExpr, C folds lateral_clauses into my_quals (initsplan.c
        // deconstruct_distribute) so they are distributed as join quals with
        // the join's sjinfo/ojscope/nonnullable — LEFT-join lateral
        // correlation quals must become join clauses, not pushed-down
        // filters above the join. Only FromExpr items distribute them plain.
        let mut pending_for_sjinfo: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        pending_for_sjinfo.extend(pending.iter().copied());
        if !pending.is_empty() && !matches!(&items[idx], JtItem::Sj { .. }) {
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
                    &crate::relnode::RELIDS_UNSET,
                    &crate::relnode::RELIDS_UNSET,
                    None,
                    jdomain,
                    plain_security_level,
                    CloneCtl::not_clone(&no_incompat),
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
                    &crate::relnode::RELIDS_UNSET,
                    &crate::relnode::RELIDS_UNSET,
                    None,
                    *jdomain,
                    plain_security_level,
                    CloneCtl::not_clone(&no_incompat),
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
                        &crate::relnode::RELIDS_UNSET,
                        None,
                        *jdomain,
                        level,
                        CloneCtl::not_clone(&no_incompat),
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
                let my_quals = if pending_for_sjinfo.is_empty() {
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
                    my_quals,
                )?;
                // Semijoins build an sjinfo but distribute their quals with
                // ojscope = NULL and no nonnullable side (C's hybrid case).
                let mut ojscope = if *jointype == types_pathnodes::JOIN_SEMI {
                    crate::relnode::relids_empty()
                } else {
                    relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand)
                };
                let full_nonnullable;
                let nonnullable = if *jointype == types_pathnodes::JOIN_SEMI {
                    &crate::relnode::RELIDS_UNSET
                } else if *jointype == types_pathnodes::JOIN_FULL {
                    // Each side of a FULL join is both outer and inner.
                    full_nonnullable = relids_copy(mcx, qualscope);
                    &full_nonnullable
                } else {
                    left_rels
                };
                let mut postponed: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
                // Non-degenerate quals of a strict-LHS LEFT join are
                // postponed in case identity 3 lets this join commute.
                let postpone = *jointype == JOIN_LEFT && sjinfo.lhs_strict;
                if postpone {
                    // Commutable lower OJ relids removed from the min sets go
                    // back into ojscope; only the distribute cross-check sees
                    // them (real placement happens on the postponed pass).
                    ojscope = relids_union(mcx, &ojscope, &sjinfo.commute_below_l);
                    ojscope = relids_union(mcx, &ojscope, &sjinfo.commute_below_r);
                }
                distribute_quals_to_rels(
                    run,
                    my_quals,
                    qualscope,
                    &ojscope,
                    nonnullable,
                    Some(&sjinfo),
                    *jdomain,
                    plain_security_level,
                    CloneCtl::not_clone(&no_incompat),
                    if postpone { Some(&mut postponed) } else { None },
                    Some((&items, idx)),
                    &mut lateral_pending,
                )?;
                let sj_index = run.root.join_info_list.len();
                run.root.join_info_list.push(sjinfo);
                sj_jdomains.push(*jdomain);
                if !postponed.is_empty() {
                    oj_postponed.push((sj_index, postponed));
                }
            }
        }
    }

    debug_assert_eq!(sj_jdomains.len(), run.root.join_info_list.len());
    for (sj_index, postponed) in oj_postponed {
        deconstruct_distribute_oj_quals(
            run,
            sj_index,
            postponed,
            &sj_jdomains,
            &mut lateral_pending,
        )?;
    }

    Ok(joinlist)
}

// deconstruct_distribute_oj_quals (initsplan.c): re-distribute a strict-LHS
// LEFT join's postponed non-degenerate quals, generating nullingrels-variant
// clones when identity 3 lets the join commute.
fn deconstruct_distribute_oj_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sj_index: usize,
    postponed: PgVec<'mcx, Node<'mcx>>,
    sj_jdomains: &PgVec<'mcx, usize>,
    lateral_pending: &mut PgVec<'mcx, PgVec<'mcx, Node<'mcx>>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let sjinfo = run.root.join_info_list[sj_index].clone();
    let seclevel = run.root.qual_security_level;
    let no_incompat: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    let qualscope = relids_add_member(
        mcx,
        &relids_union(mcx, &sjinfo.syn_lefthand, &sjinfo.syn_righthand),
        sjinfo.ojrelid,
    );
    let ojscope = relids_union(mcx, &sjinfo.min_lefthand, &sjinfo.min_righthand);
    let nonnullable = &sjinfo.syn_lefthand;
    debug_assert!(sjinfo.lhs_strict);

    if relids_is_empty(&sjinfo.commute_above_r) && relids_is_empty(&sjinfo.commute_below_l) {
        for clause in postponed {
            distribute_qual_to_rels(
                run,
                clause,
                &qualscope,
                &ojscope,
                nonnullable,
                Some(&sjinfo),
                sj_jdomains[sj_index],
                seclevel,
                CloneCtl::not_clone(&no_incompat),
                None,
                None,
                lateral_pending,
            )?;
        }
        return Ok(());
    }

    let joins_above = &sjinfo.commute_above_r;
    let joins_below = &sjinfo.commute_below_l;

    // Qual variants only need the successively-less-nested nullingrels forms:
    // strip the commuting-below bits, then put them back crawling up the
    // syntactic join stack (join_info_list order).
    let mut quals = postponed;
    if !relids_is_empty(joins_below) {
        quals = map_quals(mcx, quals, &mut |n| strip_var_nulling(mcx, n, joins_below))?;
    }

    let mut incompatible_joins = relids_union(mcx, joins_below, joins_above);
    incompatible_joins = relids_add_member(mcx, &incompatible_joins, sjinfo.ojrelid);

    // Identical serials across the variants let duplicate usage be detected.
    let save_last_rinfo_serial = run.root.last_rinfo_serial;

    let mut joins_so_far: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    for other_index in 0..run.root.join_info_list.len() {
        let othersj = run.root.join_info_list[other_index].clone();
        let below_sjinfo = relids_is_member(othersj.ojrelid as i32, joins_below);
        let above_sjinfo = !below_sjinfo
            && other_index != sj_index
            && relids_is_member(othersj.ojrelid as i32, joins_above);
        if !below_sjinfo && !above_sjinfo && other_index != sj_index {
            continue;
        }
        if other_index == sj_index {
            debug_assert!(crate::relnode::relids_equal(&joins_so_far, joins_below));
        }

        run.root.last_rinfo_serial = save_last_rinfo_serial;

        // Above: we envision pushing this join above othersj (second form of
        // identity 3 to the first), so our LHS Vars pick up othersj's bit.
        if above_sjinfo {
            quals = map_quals(mcx, quals, &mut |n| {
                add_var_nulling(mcx, n, &sjinfo.syn_lefthand, othersj.ojrelid)
            })?;
            incompatible_joins = crate::relnode::relids_del_member(
                mcx,
                &incompatible_joins,
                othersj.ojrelid as i32,
            );
        }

        let mut this_qualscope = relids_union(mcx, &qualscope, &joins_so_far);
        let mut this_ojscope = relids_union(mcx, &ojscope, &joins_so_far);
        if above_sjinfo {
            this_qualscope = relids_add_member(mcx, &this_qualscope, othersj.ojrelid);
            this_ojscope = relids_add_member(mcx, &this_ojscope, othersj.ojrelid);
            this_ojscope = crate::relnode::relids_del_member(
                mcx,
                &this_ojscope,
                sjinfo.ojrelid as i32,
            );
        }

        // ECs come only from the fewest-nullingrels form; that form is the
        // has_clone one.
        let allow_equivalence = relids_is_empty(&joins_so_far);
        let has_clone = allow_equivalence;
        let incompatible_now = relids_copy(mcx, &incompatible_joins);
        let clone_ctl = CloneCtl {
            incompatible_relids: &incompatible_now,
            allow_equivalence,
            has_clone,
            is_clone: !has_clone,
        };
        for &clause in quals.iter() {
            distribute_qual_to_rels(
                run,
                clause,
                &this_qualscope,
                &this_ojscope,
                nonnullable,
                Some(&sjinfo),
                sj_jdomains[other_index],
                seclevel,
                clone_ctl,
                None,
                None,
                lateral_pending,
            )?;
        }

        // Below: put back the parser's bits for the next level up (Pbc ->
        // Pb*c); our own bit never goes in.
        if below_sjinfo {
            quals = map_quals(mcx, quals, &mut |n| {
                add_var_nulling(mcx, n, &othersj.syn_righthand, othersj.ojrelid)
            })?;
            incompatible_joins = crate::relnode::relids_del_member(
                mcx,
                &incompatible_joins,
                othersj.ojrelid as i32,
            );
        }
        joins_so_far = relids_add_member(mcx, &joins_so_far, othersj.ojrelid);
    }
    Ok(())
}

fn map_quals<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    quals: PgVec<'mcx, Node<'mcx>>,
    f: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let mut out: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for n in quals {
        out.push(f(n)?.unwrap_or(n));
    }
    Ok(out)
}

// remove_nulling_relids (rewriteManip.c), copy-on-write expression form over
// planner quals (no Query nodes remain at deconstruct time).
fn strip_var_nulling<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    removable: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup != 0 {
                return Ok(None);
            }
            let mut nulling = v.varnullingrels.clone_in(mcx)?;
            for m in crate::relnode::relids_members(removable) {
                nulling.del_member(m);
            }
            let newvar = types_nodes::primnodes::Var { varnullingrels: nulling, ..*v };
            Ok(Some(Node::mk(mcx, newvar)?))
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().expect("PlaceHolderVar");
            if phv.phlevelsup != 0 {
                return Ok(None);
            }
            let new_expr = strip_var_nulling(mcx, phv.phexpr, removable)?;
            let hit = crate::relnode::relids_members(removable)
                .any(|m| phv.phnullingrels.is_member(m as i32) || phv.phrels.is_member(m as i32));
            if new_expr.is_none() && !hit {
                return Ok(None);
            }
            let mut nulling = phv.phnullingrels.clone_in(mcx)?;
            let mut rels = phv.phrels.clone_in(mcx)?;
            for m in crate::relnode::relids_members(removable) {
                nulling.del_member(m as i32);
                rels.del_member(m as i32);
            }
            debug_assert!(!rels.is_empty());
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::PlaceHolderVar {
                    phexpr: new_expr.unwrap_or(phv.phexpr),
                    phrels: rels,
                    phnullingrels: nulling,
                    phid: phv.phid,
                    phlevelsup: phv.phlevelsup,
                },
            )?))
        }
        NodeTag::T_SubLink | NodeTag::T_Query => {
            panic!("remove_nulling_relids_mutator (rewriteManip.c): sublevel recursion in a commuted OJ qual")
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
            strip_var_nulling(mcx, n, removable)
        }),
    }
}

// add_nulling_relids (rewriteManip.c), same form.
fn add_var_nulling<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    target_relids: &types_pathnodes::Relids<'mcx>,
    added_ojrelid: u32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup != 0 || !relids_is_member(v.varno, target_relids) {
                return Ok(None);
            }
            let mut nulling = v.varnullingrels.clone_in(mcx)?;
            nulling.add_member(mcx, added_ojrelid as i32)?;
            let newvar = types_nodes::primnodes::Var { varnullingrels: nulling, ..*v };
            Ok(Some(Node::mk(mcx, newvar)?))
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().expect("PlaceHolderVar");
            if phv.phlevelsup != 0
                || !phv.phrels.iter().any(|m| relids_is_member(m, target_relids))
            {
                return Ok(None);
            }
            let mut nulling = phv.phnullingrels.clone_in(mcx)?;
            nulling.add_member(mcx, added_ojrelid as i32)?;
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::PlaceHolderVar {
                    phexpr: phv.phexpr,
                    phrels: phv.phrels.clone_in(mcx)?,
                    phnullingrels: nulling,
                    phid: phv.phid,
                    phlevelsup: phv.phlevelsup,
                },
            )?))
        }
        NodeTag::T_SubLink | NodeTag::T_Query => {
            panic!("add_nulling_relids_mutator (rewriteManip.c): sublevel recursion in a commuted OJ qual")
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
            add_var_nulling(mcx, n, target_relids, added_ojrelid)
        }),
    }
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
    clone_ctl: CloneCtl<'_, 'mcx>,
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
            clone_ctl,
            postponed.as_deref_mut(),
            jt,
            lateral_pending,
        )?;
    }
    Ok(())
}

// The incompatible_relids / allow_equivalence / has_clone / is_clone knob set
// of C's distribute_qual_to_rels; not_clone everywhere outside
// deconstruct_distribute_oj_quals.
#[derive(Clone, Copy)]
struct CloneCtl<'a, 'mcx> {
    incompatible_relids: &'a types_pathnodes::Relids<'mcx>,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
}

impl<'a, 'mcx> CloneCtl<'a, 'mcx> {
    fn not_clone(incompatible_relids: &'a types_pathnodes::Relids<'mcx>) -> Self {
        CloneCtl { incompatible_relids, allow_equivalence: true, has_clone: false, is_clone: false }
    }
}

// initsplan.c deconstruct_recurse joinlist output for non-FULL JoinExprs:
// fold the two subproblems together when within join_collapse_limit, else
// keep each side as one item (unwrapping useless 1-element sublists).
fn combine_joinlists<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    l_list: PgVec<'mcx, JoinlistNode<'mcx>>,
    r_list: PgVec<'mcx, JoinlistNode<'mcx>>,
) -> PgVec<'mcx, JoinlistNode<'mcx>> {
    if l_list.len() + r_list.len() <= crate::gucs::join_collapse_limit() as usize {
        let mut joinlist = l_list;
        for jl in r_list {
            joinlist.push(jl);
        }
        joinlist
    } else {
        let mut joinlist = PgVec::new_in(mcx);
        joinlist.push(joinlist_part(l_list));
        joinlist.push(joinlist_part(r_list));
        joinlist
    }
}

fn joinlist_part<'mcx>(mut list: PgVec<'mcx, JoinlistNode<'mcx>>) -> JoinlistNode<'mcx> {
    if list.len() == 1 { list.pop().unwrap() } else { JoinlistNode::Sub(list) }
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
            Ok((scope, crate::relnode::relids_empty(), joinlist))
        }
        NodeTag::T_FromExpr => {
            let f = item.as_from_expr().unwrap();
            let mut qualscope: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
            let mut inner_join_rels: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
            let mut joinlist = PgVec::new_in(mcx);
            let mut remaining = f.fromlist.len();
            for child in &f.fromlist {
                let (c_relids, c_inner, c_list) =
                    deconstruct_recurse(run, child, parent_domain, items)?;
                qualscope = relids_union(mcx, &qualscope, &c_relids);
                inner_join_rels = c_inner;
                remaining -= 1;
                // initsplan.c deconstruct_recurse FromExpr arm: collapse the
                // subproblem unless doing so would push the joinlist past
                // from_collapse_limit; 1-element subproblems always collapse.
                if c_list.len() <= 1
                    || joinlist.len() + c_list.len() + remaining
                        <= crate::gucs::from_collapse_limit() as usize
                {
                    for jl in c_list {
                        joinlist.push(jl);
                    }
                } else {
                    joinlist.push(JoinlistNode::Sub(c_list));
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
                    Ok((relids_copy(mcx, &scope), scope, combine_joinlists(mcx, l_list, r_list)))
                }
                types_nodes::JoinType::JOIN_LEFT | types_nodes::JoinType::JOIN_ANTI => {
                    let child_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: crate::relnode::relids_empty() });
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
                        // C: a LEFT/ANTI join's own quals belong to the new
                        // child domain (initsplan.c deconstruct_recurse,
                        // jtitem->jdomain = child_domain), scoping ECs from
                        // degenerate ON quals below the null extension.
                        jdomain: child_domain,
                        left_rels: relids_copy(mcx, &l_relids),
                        right_rels: relids_copy(mcx, &r_relids),
                        inner_join_rels: relids_copy(mcx, &inner_join_rels),
                        rtindex: j.rtindex,
                    });
                    Ok((qualscope, inner_join_rels, combine_joinlists(mcx, l_list, r_list)))
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
                    Ok((qualscope, inner_join_rels, combine_joinlists(mcx, l_list, r_list)))
                }
                types_nodes::JoinType::JOIN_FULL => {
                    // The FULL join's quals get their very own domain; each
                    // side gets its own child domain.
                    let fj_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: crate::relnode::relids_empty() });
                    let l_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: crate::relnode::relids_empty() });
                    let (l_relids, l_inner, l_list) =
                        deconstruct_recurse(run, j.larg, l_domain, items)?;
                    run.root.join_domains[fj_domain].jd_relids =
                        relids_copy(mcx, &run.root.join_domains[l_domain].jd_relids);
                    let r_domain = run.root.join_domains.len();
                    run.root
                        .join_domains
                        .push(types_pathnodes::JoinDomain { jd_relids: crate::relnode::relids_empty() });
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

// make_outerjoininfo (initsplan.c).
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
    // The executor cannot support row marks on the nullable side of an outer
    // join; checked here (not the parser) because only after rewriting and
    // flattening is the join structure known. Original RowMarkClause list, not
    // PlanRowMarks.
    for rc_node in &run.parse().rowMarks {
        let rc = rc_node.as_row_mark_clause().expect("rowMarks cell");
        if relids_is_member(rc.rti as i32, right_rels)
            || (jointype == types_pathnodes::JOIN_FULL
                && relids_is_member(rc.rti as i32, left_rels))
        {
            return Err(Box::new(
                types_error::PgError::error(format!(
                    "{} cannot be applied to the nullable side of an outer join",
                    parser_analyze::LCS_asString(rc.strength)
                ))
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
    }

    let mut sjinfo = SpecialJoinInfo {
        min_lefthand: crate::relnode::relids_empty(),
        min_righthand: crate::relnode::relids_empty(),
        syn_lefthand: relids_copy(mcx, left_rels),
        syn_righthand: relids_copy(mcx, right_rels),
        jointype,
        ojrelid: ojrelid as u32,
        commute_above_l: crate::relnode::relids_empty(),
        commute_above_r: crate::relnode::relids_empty(),
        commute_below_l: crate::relnode::relids_empty(),
        commute_below_r: crate::relnode::relids_empty(),
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
        None => crate::relnode::relids_empty(),
    };
    let strict_bms = clauses::find_nonnullable_rels(mcx, clause)?;
    let mut strict_relids: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
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
    // Tentative identity-3 commutability; surviving relids merge into the
    // SpecialJoinInfos below.
    let mut commute_below_l: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    let mut commute_below_r: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
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
        let have_unsafe_phvs = match (other.ojrelid, clause) {
            (0, _) | (_, None) => false,
            (oj, Some(c)) => {
                crate::placeholder::contain_placeholder_references_to(run, c, oj as i32)?
            }
        };
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
                && (have_unsafe_phvs
                    || is_semi_or_anti
                    || !relids_overlap(&strict_relids, &other.min_righthand))
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
                min_lefthand = crate::relnode::relids_del_member(
                    mcx,
                    &min_lefthand,
                    other.ojrelid as i32,
                );
                commute_below_l = relids_add_member(mcx, &commute_below_l, other.ojrelid);
            }
        }
        if relids_overlap(right_rels, &other.syn_righthand) {
            let other_semi_or_anti =
                matches!(other.jointype, types_pathnodes::JOIN_SEMI | JOIN_ANTI);
            if relids_overlap(&clause_relids, &other.syn_righthand)
                || !relids_overlap(&clause_relids, &other.min_lefthand)
                || have_unsafe_phvs
                || is_semi_or_anti
                || other_semi_or_anti
                || !other.lhs_strict
            {
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_lefthand);
                min_righthand = relids_union(mcx, &min_righthand, &other.syn_righthand);
                if other.ojrelid != 0 {
                    min_righthand = relids_add_member(mcx, &min_righthand, other.ojrelid);
                }
            } else if jointype == JOIN_LEFT
                && other.jointype == JOIN_LEFT
                && other.lhs_strict
            {
                min_righthand = crate::relnode::relids_del_member(
                    mcx,
                    &min_righthand,
                    other.ojrelid as i32,
                );
                commute_below_r = relids_add_member(mcx, &commute_below_r, other.ojrelid);
            }
        }
    }

    // A PHV due to be evaluated within our RHS forces min_righthand to cover
    // its full eval_at set (C initsplan.c:1965-1984).
    for i in 0..run.root.placeholder_list.len() {
        let id = run.root.placeholder_list[i];
        let phinfo = run.root.phinfo(id);
        if !crate::relnode::relids_is_subset(&phinfo.ph_var_phrels, right_rels) {
            continue;
        }
        let eval_at = relids_copy(mcx, &phinfo.ph_eval_at);
        min_righthand = relids_union(mcx, &min_righthand, &eval_at);
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

    // Commute relids re-added to the min sets by intervening outer joins are
    // not commutable after all.
    commute_below_l = relids_difference(mcx, &commute_below_l, &min_lefthand);
    commute_below_r = relids_difference(mcx, &commute_below_r, &min_righthand);
    if !relids_is_empty(&commute_below_l) || !relids_is_empty(&commute_below_r) {
        for i in 0..run.root.join_info_list.len() {
            let other_ojrelid = run.root.join_info_list[i].ojrelid;
            if relids_is_member(other_ojrelid as i32, &commute_below_l) {
                let cur = crate::relnode::relids_take(&mut run.root.join_info_list[i].commute_above_l);
                run.root.join_info_list[i].commute_above_l =
                    relids_add_member(mcx, &cur, ojrelid as u32);
            } else if relids_is_member(other_ojrelid as i32, &commute_below_r) {
                let cur = crate::relnode::relids_take(&mut run.root.join_info_list[i].commute_above_r);
                run.root.join_info_list[i].commute_above_r =
                    relids_add_member(mcx, &cur, ojrelid as u32);
            }
        }
        sjinfo.commute_below_l = commute_below_l;
        sjinfo.commute_below_r = commute_below_r;
    }

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

// distribute_qual_to_rels (initsplan.c).
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
    clone_ctl: CloneCtl<'_, 'mcx>,
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
        crate::relnode::relids_is_unset(ojscope) || relids_is_subset(&relids, ojscope),
        "JOIN qualification cannot refer to other relations"
    );
    let mut pseudoconstant = false;
    if relids_is_empty(&relids) {
        if !crate::relnode::relids_is_unset(ojscope) {
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
    let maybe_equivalence;
    let maybe_outer_join;
    if relids_overlap(&relids, outerjoin_nonnullable) {
        // Non-degenerate outer-join clause.
        if let Some(postponed) = postponed {
            postponed.push(clause);
            return Ok(());
        }
        is_pushed_down = false;
        maybe_equivalence = false;
        maybe_outer_join = true;
        debug_assert!(!crate::relnode::relids_is_unset(ojscope));
        relids = relids_copy(run.mcx, ojscope);
        debug_assert!(!pseudoconstant);
    } else {
        is_pushed_down = true;
        if check_redundant_nullability_qual(run, clause) {
            return Ok(());
        }
        maybe_equivalence = clone_ctl.allow_equivalence;
        maybe_outer_join = false;
    }

    let rinfo = make_restrictinfo(
        run,
        clause,
        is_pushed_down,
        clone_ctl.has_clone,
        clone_ctl.is_clone,
        pseudoconstant,
        security_level,
        relids,
        relids_copy(run.mcx, clone_ctl.incompatible_relids),
        relids_copy(run.mcx, outerjoin_nonnullable),
    )?;

    if relids_num_members(&run.root.rinfo(rinfo).required_relids) > 1 {
        let mut vars: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(run.mcx);
        pull_var_nodes(clause, &mut vars);
        // A clone's OJ relids would overstate where its vars must bubble up.
        let required = relids_copy(run.mcx, &run.root.rinfo(rinfo).required_relids);
        let where_needed = if clone_ctl.is_clone {
            relids_intersect(run.mcx, &required, &run.root.all_baserels)
        } else {
            required
        };
        add_vars_to_targetlist(run, &vars, &where_needed)?;
    }

    check_mergejoinable(run, rinfo)?;
    let mut rinfo = rinfo;
    if !run.root.rinfo(rinfo).mergeopfamilies.is_empty() {
        if maybe_equivalence {
            if crate::equivclass::process_equivalence(run, &mut rinfo, jdomain)? {
                return Ok(());
            }
            // EC rejected it; set left_ec/right_ec the hard way.
            if !run.root.rinfo(rinfo).mergeopfamilies.is_empty() {
                crate::pathkeys::initialize_mergeclause_eclasses(run, rinfo)?;
            }
        } else if maybe_outer_join && run.root.rinfo(rinfo).can_join {
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
    // C's pull_varnos PHV arm (var.c pull_varnos_walker): once a
    // PlaceHolderInfo exists, a PHV contributes ph_eval_at (translated for
    // appendrel-child copies whose phrels differ), not its syntactic phrels.
    // Join removal scrubs only the phinfo relid sets, so the phrels fallback
    // would resurrect removed relids from clause-embedded PHVs.
    let root = &run.root;
    let mut hook = |phv: &types_nodes::primnodes::PlaceHolderVar<'mcx>| -> PgResult<
        Option<types_nodes::Bitmapset<'mcx>>,
    > {
        let Some(Some(id)) = root.placeholder_array.get(phv.phid as usize).copied() else {
            return Ok(None);
        };
        let phinfo = root.phinfo(id);
        // Planner-arena set from the nodes-side words; value-identical to
        // the historical per-member union-of-singletons loop.
        let phv_phrels: types_pathnodes::Relids<'mcx> =
            crate::relnode::relids_from_words(mcx, phv.phrels.as_words());
        let contributed = if types_pathnodes::relids::relids_equal(
            &phv_phrels,
            &phinfo.ph_var_phrels,
        ) {
            relids_copy(mcx, &phinfo.ph_eval_at)
        } else {
            // Translated (appendrel-child) PHV: translate ph_eval_at to match.
            let delta = relids_difference(mcx, &phinfo.ph_var_phrels, &phv_phrels);
            let mut newevalat = relids_difference(mcx, &phinfo.ph_eval_at, &delta);
            if !types_pathnodes::relids::relids_equal(&newevalat, &phinfo.ph_eval_at) {
                let added = relids_difference(mcx, &phv_phrels, &phinfo.ph_var_phrels);
                newevalat = relids_union(mcx, &newevalat, &added);
            }
            newevalat
        };
        let mut out = types_nodes::Bitmapset::empty();
        for m in relids_members(&contributed) {
            out.add_member(mcx, m)?;
        }
        Ok(Some(out))
    };
    let bms = vars::pull_varnos_with_phv_hook(mcx, node, &mut hook)?;
    // Planner-arena set from the nodes-side words; value-identical to the
    // historical per-member union-of-singletons loop.
    Ok(crate::relnode::relids_from_words(mcx, bms.as_words()))
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

    let mut left_relids: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
    let mut right_relids: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
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

    let required_relids = if !crate::relnode::relids_is_unset(&required_relids) {
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
    if run.root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return Ok(());
    };
    let (opno, args0) = (o.opno, o.args.nth(0));
    let lefttype = crate::costsize::expr_type_typmod(args0).0;
    if crate::syscache_memo::op_mergejoinable(run, opno, lefttype)?
        && !clauses::contain_volatile_functions(clause)?
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
    assert!(!crate::relnode::relids_is_unset(&relids), "multi-member relids");
    for (i, w) in crate::relnode::relids_word_slice(&relids).iter().enumerate() {
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

// restriction_is_always_true / _false (initsplan.c). The OR leg recurses on
// the raw clause tree: orclause sub-RestrictInfos are not materialized here,
// and C's non-RestrictInfo (AND-group) arms map to non-NullTest/non-OR nodes;
// sub-RIs inherit the parent's clone flags, so the guard runs once at the top.
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
    clause_nulltest_verdict(run, *run.root.expr_node(ri.clause), testtype)
}

fn clause_nulltest_verdict(
    run: &PlannerRun<'_>,
    clause: Node<'_>,
    testtype: types_nodes::primnodes::NullTestType,
) -> bool {
    if let Some(nt) = clause.as_null_test() {
        if nt.nulltesttype != testtype || nt.argisrow {
            return false;
        }
        return expr_is_nonnullable(run, nt.arg.expect("NullTest.arg"));
    }
    if let Some(be) = clause.as_bool_expr() {
        if be.boolop == types_nodes::primnodes::BoolExprType::OR_EXPR {
            // Always-true: any provably-true arm suffices. Always-false:
            // every arm must be provably false.
            let want_any = testtype == types_nodes::primnodes::NullTestType::IS_NOT_NULL;
            for a in &be.args {
                if clause_nulltest_verdict(run, a, testtype) == want_any {
                    return want_any;
                }
            }
            return !want_any;
        }
    }
    false
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
        crate::relnode::relids_empty(),
        crate::relnode::relids_empty(),
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
        crate::relnode::relids_empty(),
        crate::relnode::relids_empty(),
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

// remove_useless_groupby_columns (initsplan.c): drop GROUP BY items
// functionally dependent on other same-relation items via a NOT NULL (or
// NULLS NOT DISTINCT) unique index.
pub fn remove_useless_groupby_columns<'mcx>(run: &mut PlannerRun<'mcx>) -> PgResult<()> {
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mcx = run.mcx;
    let parse = run.parse();
    if run.root.processed_groupClause.len() < 2 {
        return Ok(());
    }
    if !parse.groupingSets.is_nil() {
        return Ok(());
    }

    // GroupByColInfo (initsplan.c): per GROUP BY item, the compatibility info
    // needed to verify a unique index's equality notion (opfamily+collation)
    // agrees with some GROUP BY item on each index column.
    struct GroupByColInfo<'mcx> {
        attno: i16,
        eq_opfamilies: PgVec<'mcx, types_core::Oid>,
        coll: types_core::Oid,
    }

    let nrtes = parse.rtable.len() + 1;
    let mut groupbyattnos: PgVec<'mcx, types_pathnodes::Relids<'mcx>> = PgVec::new_in(mcx);
    let mut groupbycols: Vec<Vec<GroupByColInfo<'mcx>>> = Vec::with_capacity(nrtes);
    for _ in 0..nrtes {
        groupbyattnos.push(crate::relnode::relids_empty());
        groupbycols.push(Vec::new());
    }
    let mut tryremove = false;
    let group_clause = crate::relnode::pgvec_clone_shallow(mcx, &run.root.processed_groupClause);
    for &sgc_id in group_clause.iter() {
        let (sgref, eqop) = {
            let sgc = run
                .root
                .expr_node(sgc_id)
                .as_sort_group_clause()
                .expect("SortGroupClause cell");
            (sgc.tleSortGroupRef, sgc.eqop)
        };
        let tle = get_sortgroupclause_tle(sgref, &parse.targetList);
        let Some(var) = tle.expr.as_var() else { continue };
        if var.varlevelsup > 0 {
            continue;
        }
        let relid = var.varno as usize;
        debug_assert!(relid < nrtes);
        tryremove |= !relids_is_empty(&groupbyattnos[relid]);
        groupbyattnos[relid] = relids_add_member(
            mcx,
            &groupbyattnos[relid],
            (var.varattno as i32 - FirstLowInvalidHeapAttributeNumber) as u32,
        );
        groupbycols[relid].push(GroupByColInfo {
            attno: var.varattno,
            eq_opfamilies: lsyscache::get_mergejoin_opfamilies(mcx, eqop)?,
            coll: var.varcollid,
        });
    }
    if !tryremove {
        return Ok(());
    }

    let mut surplusvars: Option<PgVec<'mcx, types_pathnodes::Relids<'mcx>>> = None;
    for relid in 1..nrtes {
        let rte = parse
            .rtable
            .nth(relid - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        if rte.rtekind != types_nodes::parsenodes::RTEKind::RTE_RELATION {
            continue;
        }
        // Traditional-inheritance parents may produce duplicate child rows.
        if rte.inh && rte.relkind != types_rel::RELKIND_PARTITIONED_TABLE {
            continue;
        }
        let relattnos = relids_copy(mcx, &groupbyattnos[relid]);
        if relids_num_members(&relattnos) <= 1 {
            continue;
        }
        let rel = find_base_rel(&run.root, relid as i32);
        let mut best_keycolumns: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
        let mut best_nkeycolumns = i32::MAX;
        for index in run.root.rel(rel).indexlist.iter() {
            if !index.unique || !index.immediate || !index.indpred.is_empty() {
                continue;
            }
            if !index.indexprs.is_empty() {
                continue;
            }
            let mut ind_attnos: types_pathnodes::Relids<'mcx> = crate::relnode::relids_empty();
            let mut index_check_ok = true;
            for i in 0..index.nkeycolumns as usize {
                let indkey_attno = index.indexkeys[i];
                if !index.nullsnotdistinct
                    && !relids_is_member(indkey_attno, &run.root.rel(rel).notnullattnums)
                {
                    index_check_ok = false;
                    break;
                }
                // The index proves uniqueness only under its own opfamily and
                // collation; some GROUP BY item on this column must use a
                // compatible eqop and collation (the same check
                // relation_has_unique_index_for applies to join clauses).
                let mut col_matched = false;
                for info in groupbycols[relid].iter() {
                    if info.attno as i32 != indkey_attno {
                        continue;
                    }
                    if info.eq_opfamilies.iter().any(|&f| f == index.opfamily[i])
                        && lsyscache::collations_agree_on_equality(
                            index.indexcollations[i],
                            info.coll,
                        )?
                    {
                        col_matched = true;
                        break;
                    }
                }
                if !col_matched {
                    index_check_ok = false;
                    break;
                }
                ind_attnos = relids_add_member(
                    mcx,
                    &ind_attnos,
                    (indkey_attno - FirstLowInvalidHeapAttributeNumber) as u32,
                );
            }
            if !index_check_ok {
                continue;
            }
            if crate::relnode::relids_subset_compare(&ind_attnos, &relattnos)
                != types_pathnodes::relids::SubsetCmp::Subset1
            {
                continue;
            }
            if index.nkeycolumns < best_nkeycolumns {
                best_keycolumns = ind_attnos;
                best_nkeycolumns = index.nkeycolumns;
            }
        }
        if !relids_is_empty(&best_keycolumns) {
            let sv = surplusvars.get_or_insert_with(|| {
                let mut v = PgVec::new_in(mcx);
                for _ in 0..nrtes {
                    v.push(crate::relnode::relids_empty());
                }
                v
            });
            sv[relid] = relids_difference(mcx, &relattnos, &best_keycolumns);
        }
    }

    if let Some(surplusvars) = surplusvars {
        let mut new_groupby: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
        for &sgc_id in group_clause.iter() {
            let sgref = run
                .root
                .expr_node(sgc_id)
                .as_sort_group_clause()
                .expect("SortGroupClause cell")
                .tleSortGroupRef;
            let tle = get_sortgroupclause_tle(sgref, &parse.targetList);
            let keep = match tle.expr.as_var() {
                None => true,
                Some(var) => {
                    var.varlevelsup > 0
                        || !relids_is_member(
                            var.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                            &surplusvars[var.varno as usize],
                        )
                }
            };
            if keep {
                new_groupby.push(sgc_id);
            }
        }
        run.root.processed_groupClause = new_groupby;
    }
    Ok(())
}

fn get_sortgroupclause_tle<'mcx>(
    sortgroupref: types_core::Index,
    tlist: &types_nodes::NodeList<'mcx>,
) -> &'mcx types_nodes::primnodes::TargetEntry<'mcx> {
    tlist
        .iter()
        .find_map(|n| {
            let t = n.as_target_entry().expect("tlist cell");
            (t.ressortgroupref == sortgroupref).then_some(t)
        })
        .expect("ORDER/GROUP BY expression not found in targetlist")
}

// match_foreign_keys_to_quals (initsplan.c): annotate root.fkey_list entries
// with matched eclasses / loose join quals; drop entries not fully matched.
pub fn match_foreign_keys_to_quals(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let mcx = run.mcx;
    let mut newlist: PgVec<'_, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    let fkeys = {
        let mut v: PgVec<'_, types_pathnodes::NodeId> = PgVec::new_in(mcx);
        v.extend(run.root.fkey_list.iter().copied());
        v
    };
    for &fkid in fkeys.iter() {
        let (con_relid, ref_relid, nkeys) = {
            let fk = run.root.foreign_key(fkid);
            (fk.con_relid, fk.ref_relid, fk.nkeys as usize)
        };
        // Either relid might be in the rtable but not the jointree, or
        // removed by join removal; such FKs have no RelOptInfo and are
        // ignored (hence no find_base_rel here).
        if con_relid as i32 >= run.root.simple_rel_array_size
            || ref_relid as i32 >= run.root.simple_rel_array_size
        {
            continue;
        }
        let Some(con_rel) = run.root.simple_rel_array[con_relid as usize] else { continue };
        let Some(ref_rel) = run.root.simple_rel_array[ref_relid as usize] else { continue };
        // FKs linking to inheritance child rels (otherrels) are useless here.
        if run.root.rel(con_rel).reloptkind != types_pathnodes::RELOPT_BASEREL
            || run.root.rel(ref_rel).reloptkind != types_pathnodes::RELOPT_BASEREL
        {
            continue;
        }
        for colno in 0..nkeys {
            // For simple inner joins any match is in an eclass; "loose"
            // syntactic matches were rejected for EC status (outer-join
            // quals or similar) but still count as matching the FK.
            if let Some(ec) = crate::equivclass::match_eclasses_to_foreign_key_col(run, fkid, colno)? {
                let has_const = run.root.ec(ec).ec_has_const;
                let fk = run.root.foreign_key_mut(fkid);
                fk.nmatched_ec += 1;
                if has_const {
                    fk.nconst_ec += 1;
                }
                continue;
            }
            let (con_attno, ref_attno) = {
                let fk = run.root.foreign_key(fkid);
                (fk.conkey[colno], fk.confkey[colno])
            };
            let mut fpeqop: Option<types_core::Oid> = None;
            let joininfo = {
                let mut v: PgVec<'_, RinfoId> = PgVec::new_in(mcx);
                v.extend(run.root.rel(con_rel).joininfo.iter().copied());
                v
            };
            for &rid in joininfo.iter() {
                let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
                let Some(op) = clause.as_op_expr() else { continue };
                if op.args.len() != 2 {
                    continue;
                }
                fn strip<'m>(mut n: Node<'m>) -> Node<'m> {
                    while n.node_tag() == NodeTag::T_RelabelType {
                        n = n.as_relabel_type().unwrap().arg;
                    }
                    n
                }
                let Some(leftvar) = strip(op.args.nth(0)).as_var() else { continue };
                let Some(rightvar) = strip(op.args.nth(1)).as_var() else { continue };
                let conpfeqop = run.root.foreign_key(fkid).conpfeqop[colno];
                if ref_relid as i32 == leftvar.varno
                    && ref_attno == leftvar.varattno
                    && con_relid as i32 == rightvar.varno
                    && con_attno == rightvar.varattno
                {
                    if op.opno == conpfeqop {
                        let fk = run.root.foreign_key_mut(fkid);
                        fk.rinfos[colno].push(rid);
                        fk.nmatched_ri += 1;
                    }
                } else if ref_relid as i32 == rightvar.varno
                    && ref_attno == rightvar.varattno
                    && con_relid as i32 == leftvar.varno
                    && con_attno == leftvar.varattno
                {
                    // Reverse match: check the commutator (looked up at most
                    // once per column in practice).
                    if fpeqop.is_none() {
                        fpeqop = Some(lsyscache::get_commutator(conpfeqop)?);
                    }
                    if op.opno == fpeqop.unwrap() {
                        let fk = run.root.foreign_key_mut(fkid);
                        fk.rinfos[colno].push(rid);
                        fk.nmatched_ri += 1;
                    }
                }
            }
            if !run.root.foreign_key(fkid).rinfos[colno].is_empty() {
                run.root.foreign_key_mut(fkid).nmatched_rcols += 1;
            }
        }
        // Multicolumn FKs not fully matched to the query are dropped.
        let fk = run.root.foreign_key(fkid);
        if (fk.nmatched_ec + fk.nmatched_rcols) as usize == nkeys {
            newlist.push(fkid);
        }
    }
    run.root.fkey_list = newlist;
    Ok(())
}
