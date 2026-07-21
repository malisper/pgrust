//! Initplans, the pull_up_sublinks transform, and the regular SubPlan lane
//! (make_subplan/build_subplan) for correlated and testexpr-bearing sublinks,
//! plus SS_replace_correlation_vars and finalize's SubPlan legs.

use clauses::NodeWalker;
use mcx::Mcx;
use types_core::catalog::{BOOLOID, VOIDOID};
use types_core::RECORDOID;
use types_error::PgResult;
use types_nodes::list::{IntList, NodeList};
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, Param, ParamKind, SubLink, SubLinkType, SubPlan};
use types_nodes::{Node, NodeTag};
use types_pathnodes::RelId;

use crate::createplan::create_plan;
use crate::pathnode::get_cheapest_fractional_path;
use crate::planmain::fetch_final_rel;
use crate::run::PlannerRun;

// Convert top-level ANY/EXISTS sublinks into SEMI/ANTI JoinExprs stacked into
// the jointree. New nodes are freshly built here, so the post-hoc fixups
// mirror C's in-place writes on exclusively-owned nodes.
pub fn pull_up_sublinks<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let f = parse.jointree.expect("jointree is a FromExpr");
    let jt_node = Node::mk(mcx, FromExpr { fromlist: f.fromlist.clone_in(mcx)?, quals: f.quals })?;
    let (jtnode, _relids) = pull_up_sublinks_jointree_recurse(run, parse, jt_node)?;
    if let Some(newf) = jtnode.as_from_expr() {
        parse.jointree = Some(newf);
    } else {
        parse.jointree = Some(mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, jtnode)?, quals: None },
        )?);
    }
    Ok(())
}

fn pull_up_sublinks_jointree_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<(Node<'mcx>, types_nodes::Bitmapset<'mcx>)> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let mut relids = types_nodes::Bitmapset::empty();
            relids.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
            Ok((node, relids))
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut newfromlist = NodeList::nil();
            let mut frelids = types_nodes::Bitmapset::empty();
            for child in &f.fromlist {
                let (newchild, childrelids) =
                    pull_up_sublinks_jointree_recurse(run, parse, child)?;
                newfromlist.lappend(mcx, newchild)?;
                frelids.add_members(mcx, &childrelids)?;
            }
            let newf =
                Node::mk(mcx, FromExpr { fromlist: newfromlist, quals: None })?;
            let mut jtlink = newf;
            let quals =
                pull_up_sublinks_qual_recurse(run, parse, f.quals, &mut jtlink, &frelids, None)?;
            // SAFETY: newf was built above and is exclusively owned here.
            unsafe { newf.with_mut::<FromExpr, _>(|nf| nf.quals = quals) };
            Ok((jtlink, frelids))
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let (larg, leftrelids) = pull_up_sublinks_jointree_recurse(run, parse, j.larg)?;
            let (rarg, rightrelids) = pull_up_sublinks_jointree_recurse(run, parse, j.rarg)?;
            let newj = Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: None,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )?;
            let mut result = newj;
            match j.jointype {
                types_nodes::JoinType::JOIN_INNER => {
                    let mut both = types_nodes::Bitmapset::empty();
                    both.add_members(mcx, &leftrelids)?;
                    both.add_members(mcx, &rightrelids)?;
                    let mut jtlink = newj;
                    let quals = pull_up_sublinks_qual_recurse(
                        run, parse, j.quals, &mut jtlink, &both, None,
                    )?;
                    // SAFETY: newj is exclusively owned (built above).
                    unsafe { newj.with_mut::<types_nodes::JoinExpr, _>(|nj| nj.quals = quals) };
                    result = jtlink;
                }
                types_nodes::JoinType::JOIN_LEFT => {
                    let mut rarg_link = rarg;
                    let quals = pull_up_sublinks_qual_recurse(
                        run, parse, j.quals, &mut rarg_link, &rightrelids, None,
                    )?;
                    // SAFETY: as above.
                    unsafe {
                        newj.with_mut::<types_nodes::JoinExpr, _>(|nj| {
                            nj.quals = quals;
                            nj.rarg = rarg_link;
                        })
                    };
                }
                types_nodes::JoinType::JOIN_RIGHT => {
                    let mut larg_link = larg;
                    let quals = pull_up_sublinks_qual_recurse(
                        run, parse, j.quals, &mut larg_link, &leftrelids, None,
                    )?;
                    // SAFETY: as above.
                    unsafe {
                        newj.with_mut::<types_nodes::JoinExpr, _>(|nj| {
                            nj.quals = quals;
                            nj.larg = larg_link;
                        })
                    };
                }
                types_nodes::JoinType::JOIN_FULL => {
                    // can't do anything with full-join quals
                    // SAFETY: as above.
                    unsafe {
                        newj.with_mut::<types_nodes::JoinExpr, _>(|nj| nj.quals = j.quals)
                    };
                }
                other => panic!(
                    "pull_up_sublinks_jointree_recurse (prepjointree.c): {other:?} arm"
                ),
            }
            let mut relids = types_nodes::Bitmapset::empty();
            relids.add_members(mcx, &leftrelids)?;
            relids.add_members(mcx, &rightrelids)?;
            if j.rtindex != 0 {
                relids.add_member(mcx, j.rtindex)?;
            }
            Ok((result, relids))
        }
        other => panic!(
            "pull_up_sublinks_jointree_recurse (prepjointree.c): {other:?} jointree node"
        ),
    }
}

// jtlink2/available_rels2 is the second insertion slot for quals of an
// already-pulled-up ANY sublink.
fn pull_up_sublinks_qual_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    node: Option<Node<'mcx>>,
    jtlink1: &mut Node<'mcx>,
    available_rels1: &types_nodes::Bitmapset<'mcx>,
    mut jtlink2_rels2: Option<(&mut Node<'mcx>, &types_nodes::Bitmapset<'mcx>)>,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    let Some(node) = node else { return Ok(None) };
    if let Some(sl) = node.as_sub_link() {
        match sl.subLinkType {
            SubLinkType::ANY_SUBLINK => {
                if let Some(saop) = convert_values_to_any(run, sl)? {
                    return Ok(Some(saop));
                }
                if let Some((rarg, quals)) =
                    convert_any_sublink_to_join(run, parse, sl, available_rels1)?
                {
                    attach_pulled_up_join(
                        run,
                        parse,
                        jtlink1,
                        available_rels1,
                        types_nodes::JoinType::JOIN_SEMI,
                        rarg,
                        quals,
                    )?;
                    return Ok(None);
                }
                if let Some((jtlink2, rels2)) = jtlink2_rels2 {
                    if let Some((rarg, quals)) =
                        convert_any_sublink_to_join(run, parse, sl, rels2)?
                    {
                        attach_pulled_up_join(
                            run,
                            parse,
                            jtlink2,
                            rels2,
                            types_nodes::JoinType::JOIN_SEMI,
                            rarg,
                            quals,
                        )?;
                        return Ok(None);
                    }
                }
            }
            SubLinkType::EXISTS_SUBLINK => {
                if let Some((rarg, quals)) =
                    convert_exists_sublink_to_join(run, parse, sl, false, available_rels1)?
                {
                    attach_pulled_up_join(
                        run,
                        parse,
                        jtlink1,
                        available_rels1,
                        types_nodes::JoinType::JOIN_SEMI,
                        rarg,
                        quals,
                    )?;
                    return Ok(None);
                }
                if let Some((jtlink2, rels2)) = jtlink2_rels2 {
                    if let Some((rarg, quals)) =
                        convert_exists_sublink_to_join(run, parse, sl, false, rels2)?
                    {
                        attach_pulled_up_join(
                            run,
                            parse,
                            jtlink2,
                            rels2,
                            types_nodes::JoinType::JOIN_SEMI,
                            rarg,
                            quals,
                        )?;
                        return Ok(None);
                    }
                }
            }
            _ => {}
        }
        return Ok(Some(node));
    }
    if let Some(b) = node.as_bool_expr() {
        match b.boolop {
            types_nodes::BoolExprType::NOT_EXPR => {
                let arg = b.args.first().expect("NOT has one arg");
                if let Some(sl) = arg.as_sub_link() {
                    if sl.subLinkType == SubLinkType::EXISTS_SUBLINK {
                        if let Some((rarg, quals)) = convert_exists_sublink_to_join(
                            run,
                            parse,
                            sl,
                            true,
                            available_rels1,
                        )? {
                            attach_anti_join(run, parse, jtlink1, rarg, quals)?;
                            return Ok(None);
                        }
                        if let Some((jtlink2, rels2)) = jtlink2_rels2 {
                            if let Some((rarg, quals)) = convert_exists_sublink_to_join(
                                run, parse, sl, true, rels2,
                            )? {
                                attach_anti_join(run, parse, jtlink2, rarg, quals)?;
                                return Ok(None);
                            }
                        }
                    }
                }
                return Ok(Some(node));
            }
            types_nodes::BoolExprType::AND_EXPR => {
                let mut newclauses = NodeList::nil();
                for arg in &b.args {
                    let newclause = pull_up_sublinks_qual_recurse(
                        run,
                        parse,
                        Some(arg),
                        jtlink1,
                        available_rels1,
                        match jtlink2_rels2 {
                            Some((ref mut l, r)) => Some((*l, r)),
                            None => None,
                        },
                    )?;
                    if let Some(c) = newclause {
                        newclauses.lappend(mcx, c)?;
                    }
                }
                return Ok(match newclauses.len() {
                    0 => None,
                    1 => Some(newclauses.nth(0)),
                    _ => Some(Node::mk(
                        mcx,
                        types_nodes::primnodes::BoolExpr {
                            boolop: types_nodes::BoolExprType::AND_EXPR,
                            args: newclauses,
                            location: -1,
                        },
                    )?),
                });
            }
            types_nodes::BoolExprType::OR_EXPR => return Ok(Some(node)),
        }
    }
    Ok(Some(node))
}

// The shared "insert new JoinExpr above *jtlink, then recursively process the
// pulled-up rarg and quals" tail of C's ANY/EXISTS success arms.
fn attach_pulled_up_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    jtlink: &mut Node<'mcx>,
    available_rels: &types_nodes::Bitmapset<'mcx>,
    jointype: types_nodes::JoinType,
    rarg: Node<'mcx>,
    quals: Option<Node<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(jointype == types_nodes::JoinType::JOIN_SEMI);
    let (new_rarg, child_rels) = pull_up_sublinks_jointree_recurse(run, parse, rarg)?;
    let j = Node::mk(
        mcx,
        types_nodes::JoinExpr {
            jointype,
            isNatural: false,
            larg: *jtlink,
            rarg: new_rarg,
            usingClause: NodeList::nil(),
            join_using_alias: None,
            quals: None,
            alias: None,
            rtindex: 0,
        },
    )?;
    let mut larg_link = *jtlink;
    let mut rarg_link = new_rarg;
    let newquals = pull_up_sublinks_qual_recurse(
        run,
        parse,
        quals,
        &mut larg_link,
        available_rels,
        Some((&mut rarg_link, &child_rels)),
    )?;
    // SAFETY: j was built above and is exclusively owned here.
    unsafe {
        j.with_mut::<types_nodes::JoinExpr, _>(|nj| {
            nj.larg = larg_link;
            nj.rarg = rarg_link;
            nj.quals = newquals;
        })
    };
    *jtlink = j;
    Ok(())
}

// NOT EXISTS success arm: under a NOT, pulled-up quals may only reference
// the new join's rarg.
fn attach_anti_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    jtlink: &mut Node<'mcx>,
    rarg: Node<'mcx>,
    quals: Option<Node<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let (new_rarg, child_rels) = pull_up_sublinks_jointree_recurse(run, parse, rarg)?;
    let j = Node::mk(
        mcx,
        types_nodes::JoinExpr {
            jointype: types_nodes::JoinType::JOIN_ANTI,
            isNatural: false,
            larg: *jtlink,
            rarg: new_rarg,
            usingClause: NodeList::nil(),
            join_using_alias: None,
            quals: None,
            alias: None,
            rtindex: 0,
        },
    )?;
    let mut rarg_link = new_rarg;
    let newquals =
        pull_up_sublinks_qual_recurse(run, parse, quals, &mut rarg_link, &child_rels, None)?;
    // SAFETY: j was built above and is exclusively owned here.
    unsafe {
        j.with_mut::<types_nodes::JoinExpr, _>(|nj| {
            nj.rarg = rarg_link;
            nj.quals = newquals;
        })
    };
    *jtlink = j;
    Ok(())
}

// "x = ANY (VALUES ...)" over >=2 all-constant single-column rows folds to a
// ScalarArrayOpExpr on a Const array. None = not convertible.
fn convert_values_to_any<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sublink: &SubLink<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    let values = sublink.subselect.as_query().expect("transformed sublink holds a Query");
    let Some(testexpr) = sublink.testexpr else { return Ok(None) };
    let Some(op) = testexpr.as_op_expr() else { return Ok(None) };
    if op.args.len() != 2
        || values.targetList.len() > 1
        || values.limitCount.is_some()
        || values.limitOffset.is_some()
        || !values.sortClause.is_nil()
        || values.rtable.len() != 1
    {
        return Ok(None);
    }
    let rte = values
        .rtable
        .first()
        .and_then(|n| n.as_range_tbl_entry())
        .expect("rtable cell");
    let leftop = op.args.nth(0);
    let rightop = op.args.nth(1);
    if rte.rtekind != RTEKind::RTE_VALUES || rte.values_lists.len() < 2 {
        return Ok(None);
    }
    for elem in &rte.values_lists {
        if clauses::contain_volatile_functions(elem)? {
            return Ok(None);
        }
    }

    let mut elems: mcx::PgVec<'mcx, datum::Datum> = mcx::PgVec::new_in(mcx);
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::PgVec::new_in(mcx);
    for elem in &rte.values_lists {
        let value = elem.as_list().expect("values row is a list").nth(0);
        let value = convert_testexpr(mcx, rightop, &[value])?;
        let value =
            clauses::eval_const_expressions_with_params(mcx, value, run.glob.bound_params)?;
        let Some(c) = value.as_const() else { return Ok(None) };
        elems.push(c.constvalue);
        nulls.push(c.constisnull);
    }

    let (coltype, _) = crate::costsize::expr_type_typmod(rightop);
    let arraytype = lsyscache::get_array_type(coltype)?;
    if arraytype == 0 {
        return Ok(None);
    }
    let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(coltype)?;
    let dims = [elems.len() as i32];
    let lbs = [1i32];
    let arr = arrayfuncs::construct::construct_md_array(
        mcx,
        &elems,
        Some(&nulls),
        1,
        &dims,
        &lbs,
        coltype,
        typlen as i32,
        typbyval,
        typalign as u8,
    )?;
    let arraycollid = rte.colcollations.first().unwrap_or(0);
    let array_const = Node::mk(
        mcx,
        types_nodes::primnodes::Const {
            consttype: arraytype,
            consttypmod: -1,
            constcollid: arraycollid,
            constlen: -1,
            constvalue: datum::Datum::from_usize(arr.leak().as_ptr() as usize),
            constisnull: false,
            constbyval: false,
            location: -1,
        },
    )?;
    Ok(Some(Node::mk(
        mcx,
        types_nodes::primnodes::ScalarArrayOpExpr {
            opno: op.opno,
            opfuncid: lsyscache::get_opcode(op.opno)?,
            hashfuncid: 0,
            negfuncid: 0,
            useOr: true,
            inputcollid: op.inputcollid,
            args: NodeList::make2(mcx, leftop, array_const)?,
            location: -1,
        },
    )?))
}

// Returns (rarg, quals) for the JOIN_SEMI JoinExpr the caller assembles, after
// appending the subselect to the rangetable. None = fall back to SubPlan.
fn convert_any_sublink_to_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    sublink: &SubLink<'mcx>,
    available_rels: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<Option<(Node<'mcx>, Option<Node<'mcx>>)>> {
    let mcx = run.mcx;
    debug_assert!(sublink.subLinkType == SubLinkType::ANY_SUBLINK);
    let subselect = sublink.subselect.as_query().expect("sublink holds a Query");
    let testexpr = sublink.testexpr.expect("ANY sublink has a testexpr");

    let sub_ref_outer = vars::pull_varnos_of_level(mcx, sublink.subselect, 1)?;
    let use_lateral = !sub_ref_outer.is_empty();
    if !sub_ref_outer.is_subset(available_rels) {
        return Ok(None);
    }
    let upper_varnos = vars::pull_varnos(mcx, testexpr)?;
    if upper_varnos.is_empty() || !upper_varnos.is_subset(available_rels) {
        return Ok(None);
    }
    if clauses::contain_volatile_functions(testexpr)? {
        return Ok(None);
    }

    // addRangeTableEntryForSubquery (parse_relation.c) essentials: eref from
    // the subquery tlist resnames under the "ANY_subquery" alias.
    let mut colnames = NodeList::nil();
    for te_node in &subselect.targetList {
        let te = te_node.as_target_entry().expect("tlist entry");
        if te.resjunk {
            continue;
        }
        colnames.lappend(
            mcx,
            Node::mk_string(mcx, te.resname.unwrap_or("?column?"))?,
        )?;
    }
    let alias = mcx::leak_in(mcx::alloc_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("ANY_subquery"), colnames: NodeList::nil() },
    )?);
    let eref = mcx::leak_in(mcx::alloc_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("ANY_subquery"), colnames },
    )?);
    let rte = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_SUBQUERY,
            subquery: Some(subselect),
            alias: Some(alias),
            eref: Some(eref),
            lateral: use_lateral,
            inFromCl: false,
            ..Default::default()
        },
    )?;
    parse.rtable.lappend(mcx, rte)?;
    let rtindex = parse.rtable.len() as i32;
    let rtr = Node::mk_range_tbl_ref(mcx, rtindex)?;

    let mut subquery_vars: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    for te_node in &subselect.targetList {
        let te = te_node.as_target_entry().expect("tlist entry");
        if te.resjunk {
            continue;
        }
        let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
        subquery_vars.push(Node::mk(
            mcx,
            types_nodes::primnodes::Var {
                varno: rtindex,
                varattno: te.resno,
                vartype: ty,
                vartypmod: tm,
                varcollid: crate::pathkeys::expr_collation(te.expr),
                ..Default::default()
            },
        )?);
    }

    let quals = convert_testexpr(mcx, testexpr, &subquery_vars)?;
    Ok(Some((rtr, Some(quals))))
}

fn convert_testexpr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    subst: &[Node<'mcx>],
) -> PgResult<Node<'mcx>> {
    Ok(convert_testexpr_mutator(mcx, node, subst)?.unwrap_or(node))
}

fn convert_testexpr_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    subst: &[Node<'mcx>],
) -> PgResult<Option<Node<'mcx>>> {
    if let Some(p) = node.as_param() {
        if p.paramkind == ParamKind::PARAM_SUBLINK {
            let id = p.paramid;
            assert!(
                id >= 1 && (id as usize) <= subst.len(),
                "unexpected PARAM_SUBLINK ID: {id}"
            );
            // C copyObject; substitutions are Vars built per-conversion, so
            // the handle is exclusively ours already.
            return Ok(Some(subst[(id - 1) as usize]));
        }
        return Ok(None);
    }
    if node.node_tag() == NodeTag::T_SubLink {
        return Ok(None);
    }
    clauses::expression_tree_mutator(mcx, node, &mut |n| {
        convert_testexpr_mutator(mcx, n, subst)
    })
}

// Returns (rarg, whereClause) with the simplified sub-select's rtable already
// merged into the parent.
fn convert_exists_sublink_to_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    sublink: &SubLink<'mcx>,
    _under_not: bool,
    available_rels: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<Option<(Node<'mcx>, Option<Node<'mcx>>)>> {
    let mcx = run.mcx;
    debug_assert!(sublink.subLinkType == SubLinkType::EXISTS_SUBLINK);
    let orig = sublink.subselect.as_query().expect("sublink holds a Query");
    if !orig.cteList.is_nil() {
        return Ok(None);
    }
    let mut subselect = query_cells_copy(mcx, orig)?;
    if !simplify_exists_query(run, &mut subselect)? {
        return Ok(None);
    }
    let jt = subselect.jointree.expect("jointree is a FromExpr");
    let where_clause = jt.quals;
    subselect.jointree = Some(mcx::alloc_leak_in(
        mcx,
        types_nodes::primnodes::FromExpr { fromlist: jt.fromlist.clone_in(mcx)?, quals: None },
    )?);

    let sub_node = Node::mk(mcx, query_cells_copy(mcx, &subselect)?)?;
    if vars::contain_vars_of_level(sub_node, 1)? {
        return Ok(None);
    }
    let Some(where_clause) = where_clause else { return Ok(None) };
    if !vars::contain_vars_of_level(where_clause, 1)? {
        return Ok(None);
    }
    if clauses::contain_volatile_functions(where_clause)? {
        return Ok(None);
    }
    crate::prep::replace_empty_jointree(mcx, &mut subselect)?;

    let rtoffset = parse.rtable.len() as i32;
    // OffsetVarNodes + IncrementVarSublevelsUp(-1, 1): after simplify, the
    // sub-select body is rtable + a quals-free jointree of RangeTblRefs.
    let jt = subselect.jointree.expect("jointree is a FromExpr");
    let mut off_fromlist = NodeList::nil();
    for jnode in &jt.fromlist {
        match jnode.node_tag() {
            NodeTag::T_RangeTblRef => {
                let r = jnode.as_range_tbl_ref().expect("RangeTblRef");
                off_fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)?)?;
            }
            NodeTag::T_JoinExpr => {
                // Copy before the in-place walkers: the jointree nodes are
                // shared with the plancache'd parse tree.
                let copy = rewrite_manip::copy_node(mcx, jnode)?;
                rewrite_manip::OffsetVarNodes(mcx, copy, rtoffset, 0)?;
                rewrite_manip::IncrementVarSublevelsUp(copy, -1, 1)?;
                off_fromlist.lappend(mcx, copy)?;
            }
            other => panic!(
                "OffsetVarNodes (rewriteManip.c): {other:?} EXISTS jointree arm; join lane"
            ),
        }
    }
    let where_clause = offset_and_pull_down(mcx, where_clause, rtoffset)?;

    let clause_varnos = vars::pull_varnos(mcx, where_clause)?;
    let mut upper_varnos = types_nodes::Bitmapset::empty();
    for v in clause_varnos.iter() {
        if v <= rtoffset {
            upper_varnos.add_member(mcx, v)?;
        }
    }
    debug_assert!(!upper_varnos.is_empty());
    if !upper_varnos.is_subset(available_rels) {
        return Ok(None);
    }

    // RTEs are copied, not scribbled: the sub-Query is plancache-shared.
    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &subselect.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        assert!(
            matches!(
                srte.rtekind,
                RTEKind::RTE_RELATION
                    | RTEKind::RTE_SUBQUERY
                    | RTEKind::RTE_JOIN
                    | RTEKind::RTE_FUNCTION
                    | RTEKind::RTE_CTE
            ),
            "convert_EXISTS_sublink_to_join (subselect.c): {:?} RTE in EXISTS body",
            srte.rtekind
        );
        let new_index = if srte.perminfoindex > 0 {
            srte.perminfoindex + perm_offset
        } else {
            srte.perminfoindex
        };
        let copy = crate::prepjointree::rte_copy_with_perminfoindex(mcx, srte, new_index)?;
        if srte.rtekind == RTEKind::RTE_CTE && srte.ctelevelsup >= 1 {
            // IncrementVarSublevelsUp(-1, 1): the merged RTE now sits one
            // query level closer to the CTE it references (cteList pullup is
            // rejected above, so ctelevelsup >= 1 here).
            // SAFETY: exclusive pre-seal fixup of the fresh copy.
            unsafe {
                copy.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.ctelevelsup -= 1
                })
            };
        }
        if srte.rtekind == RTEKind::RTE_JOIN {
            // Offset joinaliasvars into the copy; shared list stays unwritten.
            let mut aliasvars = NodeList::nil();
            for av in &srte.joinaliasvars {
                aliasvars.lappend(mcx, offset_and_pull_down(mcx, av, rtoffset)?)?;
            }
            // SAFETY: exclusive pre-seal fixup of the fresh copy.
            unsafe {
                copy.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.joinaliasvars = aliasvars
                })
            };
        }
        if srte.rtekind == RTEKind::RTE_SUBQUERY {
            // Deep-copy + adjust the body only when it has uplevel vars, else
            // the plancache-shared tree stays unwritten.
            let body = srte.subquery.expect("RTE_SUBQUERY has a subquery");
            if crate::prepjointree::query_has_uplevel_vars(body)? {
                let deep = rewrite_manip::copy_query_node(mcx, body)?;
                rewrite_manip::OffsetVarNodes(mcx, deep, rtoffset, 1)?;
                rewrite_manip::IncrementVarSublevelsUp(deep, -1, 2)?;
                let subq = deep.as_query().expect("Query round trip");
                // SAFETY: exclusive pre-seal fixup of the fresh copy.
                unsafe {
                    copy.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.subquery = Some(subq)
                    })
                };
            }
        }
        if srte.rtekind == RTEKind::RTE_FUNCTION {
            // Offset each RangeTblFunction funcexpr into the copy.
            let mut functions = NodeList::nil();
            for f in &srte.functions {
                let rtfunc = f.as_range_tbl_function().expect("functions holds RangeTblFunction");
                let funcexpr = match rtfunc.funcexpr {
                    Some(fexpr) => Some(offset_and_pull_down(mcx, fexpr, rtoffset)?),
                    None => None,
                };
                functions.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::parsenodes::RangeTblFunction {
                            funcexpr,
                            funccolcount: rtfunc.funccolcount,
                            funccolnames: rtfunc.funccolnames.clone_in(mcx)?,
                            funccoltypes: rtfunc.funccoltypes.clone_in(mcx)?,
                            funccoltypmods: rtfunc.funccoltypmods.clone_in(mcx)?,
                            funccolcollations: rtfunc.funccolcollations.clone_in(mcx)?,
                            funcparams: rtfunc.funcparams.clone_in(mcx)?,
                        },
                    )?,
                )?;
            }
            // SAFETY: exclusive pre-seal fixup of the fresh copy.
            unsafe {
                copy.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.functions = functions
                })
            };
        }
        parse.rtable.lappend(mcx, copy)?;
    }
    for p in &subselect.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    let rarg = if off_fromlist.len() == 1 {
        off_fromlist.nth(0)
    } else {
        Node::mk(mcx, FromExpr { fromlist: off_fromlist, quals: None })?
    };
    Ok(Some((rarg, Some(where_clause))))
}

// One walk doing C's OffsetVarNodes(level 0) then IncrementVarSublevelsUp(-1)
// over the EXISTS WHERE clause: level-0 varnos shift by rtoffset; level-1
// (parent) vars drop to level 0 without shifting.
fn offset_and_pull_down<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    // A nested SubLink needs the sublevel-tracking walkers; they mutate in
    // place, so the shared clause is deep-copied first (C copyObject's the
    // whole subselect up front).
    if rewrite_manip::checkExprHasSubLink(node)? {
        let copy = rewrite_manip::copy_node(mcx, node)?;
        rewrite_manip::OffsetVarNodes(mcx, copy, rtoffset, 0)?;
        rewrite_manip::IncrementVarSublevelsUp(copy, -1, 1)?;
        return Ok(copy);
    }
    fn mutate<'mcx>(
        mcx: Mcx<'mcx>,
        node: Node<'mcx>,
        rtoffset: i32,
    ) -> PgResult<Option<Node<'mcx>>> {
        if let Some(v) = node.as_var() {
            let mut nv = types_nodes::primnodes::Var {
                varnullingrels: v.varnullingrels.clone_in(mcx)?,
                ..*v
            };
            if v.varlevelsup == 0 {
                nv.varno += rtoffset;
                // OffsetVarNodes_walker (rewriteManip.c) offsets the
                // varnullingrels relid set along with varno.
                if !nv.varnullingrels.is_empty() {
                    let mut s = types_nodes::Bitmapset::empty();
                    for m in nv.varnullingrels.iter() {
                        s.add_member(mcx, m + rtoffset)?;
                    }
                    nv.varnullingrels = s;
                }
                if nv.varnosyn > 0 {
                    nv.varnosyn = nv.varnosyn.wrapping_add(rtoffset as u32);
                }
            } else {
                nv.varlevelsup -= 1;
            }
            return Ok(Some(Node::mk(mcx, nv)?));
        }
        debug_assert!(node.node_tag() != NodeTag::T_SubLink);
        clauses::expression_tree_mutator(mcx, node, &mut |n| mutate(mcx, n, rtoffset))
    }
    Ok(mutate(mcx, node, rtoffset)?.unwrap_or(node))
}

pub fn ss_process_sublinks<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    is_qual: bool,
) -> PgResult<Node<'mcx>> {
    Ok(process_sublinks_mutator(run, expr, is_qual)?.unwrap_or(expr))
}

fn process_sublinks_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    is_top_qual: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    if node.node_tag() == NodeTag::T_SubLink {
        let sl = node.as_sub_link().unwrap();
        // The lefthand side is no longer at qual top level.
        let testexpr = match sl.testexpr {
            None => None,
            Some(te) => Some(process_sublinks_mutator(run, te, false)?.unwrap_or(te)),
        };
        return Ok(Some(make_subplan(run, sl, testexpr, is_top_qual)?));
    }
    // Don't recurse into the arguments of an outer PHV, Aggref or
    // GroupingFunc: any SubLinks there belong to the outer query level and
    // are processed when build_subplan collects the node into subplan args.
    if let Some(phv) = node.as_place_holder_var() {
        if phv.phlevelsup > 0 {
            return Ok(None);
        }
    }
    if let Some(a) = node.as_aggref() {
        if a.agglevelsup > 0 {
            return Ok(None);
        }
    }
    if let Some(g) = node.as_grouping_func() {
        if g.agglevelsup > 0 {
            return Ok(None);
        }
    }
    debug_assert!(!matches!(
        node.node_tag(),
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_Query
    ));
    // AND/OR flatness is preserved and isTopQual propagates through them
    // (NULL and FALSE are interchangeable anywhere in the top AND/OR shell).
    if let Some(b) = node.as_bool_expr() {
        use types_nodes::BoolExprType;
        if matches!(b.boolop, BoolExprType::AND_EXPR | BoolExprType::OR_EXPR) {
            let is_and = b.boolop == BoolExprType::AND_EXPR;
            let mut newargs = NodeList::nil();
            for arg in &b.args {
                let newarg = process_sublinks_mutator(run, arg, is_top_qual)?.unwrap_or(arg);
                let flat = match newarg.as_bool_expr() {
                    Some(nb) if nb.boolop == b.boolop => Some(&nb.args),
                    _ => None,
                };
                match flat {
                    Some(args) => {
                        for a in args {
                            newargs.lappend(mcx, a)?;
                        }
                    }
                    None => newargs.lappend(mcx, newarg)?,
                }
            }
            return Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr {
                    boolop: if is_and {
                        BoolExprType::AND_EXPR
                    } else {
                        BoolExprType::OR_EXPR
                    },
                    args: newargs,
                    location: -1,
                },
            )?));
        }
    }
    clauses::expression_tree_mutator(run.mcx, node, &mut |n| {
        process_sublinks_mutator(run, n, false)
    })
}

fn make_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sublink: &SubLink<'mcx>,
    testexpr: Option<Node<'mcx>>,
    is_top_qual: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let orig = sublink
        .subselect
        .as_query()
        .expect("make_subplan on an untransformed sublink");
    // C copyObject: the planner scribbles on the sub-Query (rtable cells and
    // Vars are written in place), the same Query can hang from several
    // SubLinks (rules), and the EXISTS->ANY arm below replans from orig — a
    // cells-level copy still shares the scribble targets.
    let deep = rewrite_manip::copy_query_node(mcx, orig)?;
    let mut subquery = query_cells_copy(mcx, deep.as_query().expect("Query round trip"))?;

    let mut simple_exists = false;
    let tuple_fraction = match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            simple_exists = simplify_exists_query(run, &mut subquery)?;
            1.0
        }
        SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => 0.5,
        // C's default arm covers EXPR/MULTIEXPR/ROWCOMPARE: whole result.
        SubLinkType::EXPR_SUBLINK
        | SubLinkType::ARRAY_SUBLINK
        | SubLinkType::ROWCOMPARE_SUBLINK
        | SubLinkType::MULTIEXPR_SUBLINK => 0.0,
        other => panic!(
            "make_subplan (subselect.c): {other:?} sublink not ported"
        ),
    };

    debug_assert!(run.root.plan_params.is_empty());
    run.push_root()?;
    crate::subquery::subquery_planner(
        run,
        mcx::leak_in(mcx::alloc_in(mcx, subquery)?),
        false,
        tuple_fraction,
        None,
    )?;

    let final_rel = fetch_final_rel(run);
    let best_path = get_cheapest_fractional_path(run, final_rel, tuple_fraction);
    let plan = create_plan(run, best_path)?;
    run.pop_root_to_subroot();
    // Isolate the params this subplan needs from the current level.
    let plan_params = core::mem::replace(&mut run.root.plan_params, mcx::PgVec::new_in(mcx));

    let result = build_subplan(
        run,
        plan,
        plan_params,
        sublink.subLinkType,
        sublink.subLinkId,
        testexpr,
        IntList::nil(),
        is_top_qual,
    )?;

    if simple_exists && result.node_tag() == NodeTag::T_SubPlan {
        // C copyObject again: the first planning pass scribbled its own copy.
        let deep = rewrite_manip::copy_query_node(mcx, orig)?;
        let mut subquery = query_cells_copy(mcx, deep.as_query().expect("Query round trip"))?;
        let ok = simplify_exists_query(run, &mut subquery)?;
        debug_assert!(ok);
        if let Some((subquery, newtestexpr, param_ids)) =
            convert_exists_to_any(run, subquery)?
        {
            run.push_root()?;
            crate::subquery::subquery_planner(
                run,
                mcx::leak_in(mcx::alloc_in(mcx, subquery)?),
                false,
                0.0,
                None,
            )?;
            let final_rel = fetch_final_rel(run);
            let best_path = get_cheapest_fractional_path(run, final_rel, 0.0);
            let hashable = {
                let path = run.root.path(best_path).base();
                let width = run
                    .root
                    .pathtarget(path.pathtarget_id.expect("path has a target"))
                    .width;
                path.rows * (maxalign(width as usize) + maxalign(SIZEOF_HEAPTUPLEHEADER)) as f64
                    <= get_hash_memory_limit()
            };
            if hashable {
                let plan = create_plan(run, best_path)?;
                run.pop_root_to_subroot();
                let plan_params =
                    core::mem::replace(&mut run.root.plan_params, mcx::PgVec::new_in(mcx));
                debug_assert!(plan_params.is_empty());
                let hashplan = build_subplan(
                    run,
                    plan,
                    plan_params,
                    SubLinkType::ANY_SUBLINK,
                    0,
                    Some(newtestexpr),
                    param_ids,
                    true,
                )?;
                let hsp = hashplan.as_sub_plan().expect("build_subplan yields a SubPlan");
                debug_assert!(hsp.parParam.is_nil() && hsp.useHashTable);
                let asplan = Node::mk(
                    mcx,
                    types_nodes::primnodes::AlternativeSubPlan {
                        subplans: NodeList::make2(mcx, result, hashplan)?,
                    },
                )?;
                run.root.hasAlternativeSubPlans = true;
                run.glob.has_alternative_subplans = true;
                return Ok(asplan);
            }
            // Not hashable: abandon the twin. C never planned it (path-level
            // check before create_plan); the extra subroot never registers.
            run.pop_root_discard();
        }
    }
    Ok(result)
}

fn convert_exists_to_any<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut subselect: Query<'mcx>,
) -> PgResult<Option<(Query<'mcx>, Node<'mcx>, IntList<'mcx>)>> {
    let mcx = run.mcx;
    debug_assert!(subselect.targetList.is_nil());
    let jt = subselect.jointree.expect("jointree is a FromExpr");
    let Some(where_clause) = jt.quals else { return Ok(None) };
    subselect.jointree = Some(mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist: jt.fromlist.clone_in(mcx)?, quals: None },
    )?);
    let sub_node = Node::mk(mcx, query_cells_copy(mcx, &subselect)?)?;
    if vars::contain_vars_of_level(sub_node, 1)? {
        return Ok(None);
    }
    if clauses::contain_volatile_functions(where_clause)? {
        return Ok(None);
    }
    let where_clause = clauses::eval_const_expressions_with_params(
        mcx,
        where_clause,
        run.glob.bound_params,
    )?;
    let where_clause = crate::prepqual::canonicalize_qual(mcx, where_clause, false)?;
    let clauses_list = clauses::make_ands_implicit(mcx, Some(where_clause))?;

    let mut leftargs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut rightargs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut opids: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut opcollations: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
    let mut newwhere = NodeList::nil();
    'clause: for cl in &clauses_list {
        if let Some(expr) = cl.as_op_expr() {
            if hash_ok_operator(expr)? {
                let leftarg = expr.args.nth(0);
                let rightarg = expr.args.nth(1);
                if vars::contain_vars_of_level(leftarg, 1)? {
                    leftargs.push(leftarg);
                    rightargs.push(rightarg);
                    opids.push(expr.opno);
                    opcollations.push(expr.inputcollid);
                    continue 'clause;
                }
                if vars::contain_vars_of_level(rightarg, 1)? {
                    let comm = lsyscache::get_commutator(expr.opno)?;
                    if comm != 0 {
                        let commuted = types_nodes::primnodes::OpExpr {
                            opno: comm,
                            args: expr.args.clone_in(mcx)?,
                            ..*expr
                        };
                        if hash_ok_operator(&commuted)? {
                            leftargs.push(rightarg);
                            rightargs.push(leftarg);
                            opids.push(comm);
                            opcollations.push(expr.inputcollid);
                            continue 'clause;
                        }
                    }
                    return Ok(None);
                }
            }
        }
        newwhere.lappend(mcx, cl)?;
    }
    if leftargs.is_empty() {
        return Ok(None);
    }
    for n in newwhere.iter().chain(rightargs.iter().copied()) {
        if vars::contain_vars_of_level(n, 1)? {
            return Ok(None);
        }
        if run.parse().hasAggs && contain_aggs_of_level(n, 1)? {
            return Ok(None);
        }
    }
    for n in leftargs.iter() {
        if vars::contain_vars_of_level(*n, 0)? {
            return Ok(None);
        }
        if clauses::contain_subplans(*n)? {
            return Ok(None);
        }
    }

    // IncrementVarSublevelsUp(-1, 1) over the pulled-up left args; the args
    // are deep-copied first because the source testexpr nodes may be shared.
    let mut pulled: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    for n in leftargs.iter() {
        let copy = rewrite_manip::copy_node(mcx, *n)?;
        rewrite_manip::IncrementVarSublevelsUp(copy, -1, 1)?;
        pulled.push(copy);
    }

    if !newwhere.is_nil() {
        let quals = if newwhere.len() == 1 {
            newwhere.nth(0)
        } else {
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr {
                    boolop: types_nodes::BoolExprType::AND_EXPR,
                    args: newwhere,
                    location: -1,
                },
            )?
        };
        let jt = subselect.jointree.expect("jointree is a FromExpr");
        subselect.jointree = Some(mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: jt.fromlist.clone_in(mcx)?, quals: Some(quals) },
        )?);
    }

    let mut tlist = NodeList::nil();
    let mut testlist = NodeList::nil();
    let mut param_ids = IntList::nil();
    for (i, rightarg) in rightargs.iter().enumerate() {
        let (ty, tm) = crate::costsize::expr_type_typmod(*rightarg);
        let (prm, prm_node) =
            generate_new_exec_param(run, ty, tm, crate::pathkeys::expr_collation(*rightarg))?;
        tlist.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: *rightarg,
                    resno: (i + 1) as i16,
                    resname: None,
                    ressortgroupref: 0,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?,
        )?;
        // make_opclause; opfuncid resolved now (C leaves InvalidOid for
        // setrefs' set_opfuncid — same value either way).
        testlist.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: opids[i],
                    opfuncid: lsyscache::get_opcode(opids[i])?,
                    opresulttype: BOOLOID,
                    opretset: false,
                    opcollid: 0,
                    inputcollid: opcollations[i],
                    args: NodeList::make2(mcx, pulled[i], prm_node)?,
                    location: -1,
                },
            )?,
        )?;
        param_ids.lappend(mcx, prm.paramid)?;
    }
    subselect.targetList = tlist;
    let testexpr = if testlist.len() == 1 {
        testlist.nth(0)
    } else {
        Node::mk(
            mcx,
            types_nodes::primnodes::BoolExpr {
                boolop: types_nodes::BoolExprType::AND_EXPR,
                args: testlist,
                location: -1,
            },
        )?
    };
    Ok(Some((subselect, testexpr, param_ids)))
}

// The shared walker recurses into sub-Queries with the level bumped.
fn contain_aggs_of_level(node: Node<'_>, level: i32) -> PgResult<bool> {
    rewrite_manip::contain_aggs_of_level(node, level)
}



#[allow(clippy::too_many_arguments)]
fn build_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut plan: Node<'mcx>,
    plan_params: mcx::PgVec<'mcx, types_pathnodes::NodeId>,
    sub_link_type: SubLinkType,
    sub_link_id: i32,
    testexpr: Option<Node<'mcx>>,
    testexpr_paramids: IntList<'mcx>,
    unknown_eq_false: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    // The caller just parked this subplan's subroot; nested subplans made by
    // the args loop below append their (subroot, plan) pairs first, so this
    // subroot is rotated to the end before the glob.subplans append to keep
    // glob.subroots index-aligned (C appends subroot and plan together).
    let subroot_idx = run.subroots.len() - 1;
    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(plan);
    let parallel_safe = plan.as_plan().expect("plan node").parallel_safe;

    let mut par_param = IntList::nil();
    let mut args = NodeList::nil();
    for &ppid in plan_params.iter() {
        let pitem = *run.root.planner_param_item(ppid);
        let arg = *run.root.expr_node(pitem.item);
        // A PHV/Aggref/GroupingFunc arg may still hold unprocessed SubLinks
        // (SS_replace_correlation_vars leaves their arguments alone).
        let arg = if matches!(
            arg.node_tag(),
            NodeTag::T_PlaceHolderVar
                | NodeTag::T_Aggref
                | NodeTag::T_GroupingFunc
                | NodeTag::T_ReturningExpr
        ) {
            ss_process_sublinks(run, arg, false)?
        } else {
            arg
        };
        par_param.lappend(mcx, pitem.paramId)?;
        args.lappend(mcx, arg)?;
    }

    let mut splan = SubPlan {
        subLinkType: sub_link_type,
        testexpr: None,
        paramIds: IntList::nil(),
        plan_id: 0,
        plan_name: None,
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: unknown_eq_false,
        parallel_safe,
        setParam: IntList::nil(),
        parParam: par_param,
        args,
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };

    let mut is_init_plan = false;
    let mut result: Option<Node<'mcx>> = None;
    if splan.parParam.is_nil() && sub_link_type == SubLinkType::EXISTS_SUBLINK {
        debug_assert!(testexpr.is_none());
        let (prm, prm_node) = generate_new_exec_param(run, BOOLOID, -1, 0)?;
        splan.setParam = IntList::make1(mcx, prm.paramid)?;
        is_init_plan = true;
        result = Some(prm_node);
    } else if splan.parParam.is_nil() && sub_link_type == SubLinkType::EXPR_SUBLINK {
        let te = plan
            .as_plan()
            .unwrap()
            .targetlist
            .first()
            .expect("EXPR subplan tlist")
            .as_target_entry()
            .expect("tlist entry");
        debug_assert!(!te.resjunk);
        debug_assert!(testexpr.is_none());
        let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
        let (prm, prm_node) =
            generate_new_exec_param(run, ty, tm, crate::pathkeys::expr_collation(te.expr))?;
        splan.setParam = IntList::make1(mcx, prm.paramid)?;
        is_init_plan = true;
        result = Some(prm_node);
    } else if splan.parParam.is_nil() && sub_link_type == SubLinkType::ARRAY_SUBLINK {
        let te = plan
            .as_plan()
            .unwrap()
            .targetlist
            .first()
            .expect("ARRAY subplan tlist")
            .as_target_entry()
            .expect("tlist entry");
        debug_assert!(!te.resjunk);
        debug_assert!(testexpr.is_none());
        let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
        let arraytype = lsyscache::get_promoted_array_type(ty)?;
        if arraytype == 0 {
            return Err(no_array_type(ty));
        }
        let (prm, prm_node) =
            generate_new_exec_param(run, arraytype, tm, crate::pathkeys::expr_collation(te.expr))?;
        splan.setParam = IntList::make1(mcx, prm.paramid)?;
        is_init_plan = true;
        result = Some(prm_node);
    } else if splan.parParam.is_nil() && sub_link_type == SubLinkType::ROWCOMPARE_SUBLINK {
        let te = testexpr.expect("ROWCOMPARE sublink has a testexpr");
        let (params, param_ids) =
            generate_subquery_params(run, &plan.as_plan().unwrap().targetlist)?;
        result = Some(convert_testexpr(mcx, te, &params)?);
        splan.setParam = param_ids.clone_in(mcx)?;
        splan.paramIds = param_ids;
        is_init_plan = true;
    } else if sub_link_type == SubLinkType::MULTIEXPR_SUBLINK {
        // Whether initplan or not, one PARAM_EXEC output Param per column.
        debug_assert!(testexpr.is_none());
        let (params, param_ids) =
            generate_subquery_params(run, &plan.as_plan().unwrap().targetlist)?;
        splan.setParam = param_ids;
        // Save the replacement Params in the subLinkId'th cell of
        // root.multiexpr_params; setrefs replaces PARAM_MULTIEXPR from it.
        debug_assert!(sub_link_id >= 1);
        let slot = (sub_link_id - 1) as usize;
        let mut ids: mcx::PgVec<'mcx, types_pathnodes::NodeId> = mcx::PgVec::new_in(mcx);
        for p in params.iter() {
            ids.push(run.root.alloc_expr_node(*p));
        }
        while run.root.multiexpr_params.len() <= slot {
            run.root.multiexpr_params.push(mcx::PgVec::new_in(mcx));
        }
        debug_assert!(run.root.multiexpr_params[slot].is_empty());
        run.root.multiexpr_params[slot] = ids;
        if splan.parParam.is_nil() {
            is_init_plan = true;
            // C makeNullConst(RECORDOID, -1, InvalidOid): the SubPlan's
            // dummy in-tree result; the real outputs are the setParams.
            result = Some(Node::mk(
                mcx,
                types_nodes::primnodes::Const {
                    consttype: RECORDOID,
                    consttypmod: -1,
                    constcollid: 0,
                    constlen: -1,
                    constvalue: ::datum::Datum::from_usize(0),
                    constisnull: true,
                    constbyval: false,
                    location: -1,
                },
            )?);
        } else {
            // Correlated: the SubPlan node stays in the tree; the executor
            // runs it per row via expression setup steps (execExpr.c
            // ExecPushExprSetupSteps) and it fills the setParams itself.
            is_init_plan = false;
        }
    } else {
        // Regular SubPlan: rewrite the testexpr's PARAM_SUBLINK Params into
        // fresh PARAM_EXEC output params.
        if let Some(te) = testexpr {
            if testexpr_paramids.is_nil() {
                let (params, param_ids) =
                    generate_subquery_params(run, &plan.as_plan().unwrap().targetlist)?;
                splan.testexpr = Some(convert_testexpr(mcx, te, &params)?);
                splan.paramIds = param_ids;
            } else {
                splan.testexpr = Some(te);
                splan.paramIds = testexpr_paramids;
            }
        }
        if sub_link_type == SubLinkType::ANY_SUBLINK
            && splan.parParam.is_nil()
            && subplan_is_hashable(plan)
            && testexpr_is_hashable(splan.testexpr.expect("ANY testexpr"), &splan.paramIds)?
        {
            splan.useHashTable = true;
        } else if splan.parParam.is_nil()
            && guc_tables::vars::enable_material.read()
            && !exec_materializes_output(plan.node_tag())
        {
            plan = materialize_finished_plan(mcx, plan)?;
        }
        is_init_plan = false;
    }

    if subroot_idx + 1 != run.subroots.len() {
        run.subroots[subroot_idx..].rotate_left(1);
    }
    run.glob.subplans.lappend(mcx, plan)?;
    let plan_id = run.glob.subplans.len() as i32;
    // Ancestors' parked subroots may still be in flight (nested build_subplan
    // via the args loop); their rotations restore index alignment on return.
    debug_assert!(run.subroots.len() >= run.glob.subplans.len());
    splan.plan_id = plan_id;

    if !is_init_plan && splan.parParam.is_nil() && !splan.useHashTable {
        run.glob.rewind_plan_ids.add_member(mcx, plan_id)?;
    }
    splan.plan_name = Some(str_in(
        mcx,
        &format!("{} {plan_id}", if is_init_plan { "InitPlan" } else { "SubPlan" }),
    )?);
    cost_subplan(&mut splan, plan)?;
    let splan_node = Node::mk(mcx, splan)?;
    if is_init_plan {
        let splan_id = run.intern_expr(splan_node);
        run.root.init_plans.push(splan_id);
        Ok(result.expect("initplan replacement expression"))
    } else {
        Ok(splan_node)
    }
}

fn generate_subquery_params<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<(mcx::PgVec<'mcx, Node<'mcx>>, IntList<'mcx>)> {
    let mut params: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(run.mcx);
    let mut ids = IntList::nil();
    for te_node in tlist {
        let te = te_node.as_target_entry().expect("tlist entry");
        if te.resjunk {
            continue;
        }
        let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
        let (prm, prm_node) =
            generate_new_exec_param(run, ty, tm, crate::pathkeys::expr_collation(te.expr))?;
        params.push(prm_node);
        ids.lappend(run.mcx, prm.paramid)?;
    }
    Ok((params, ids))
}

fn subplan_is_hashable(plan: Node<'_>) -> bool {
    let p = plan.as_plan().expect("plan node");
    let subquery_size =
        p.plan_rows * (maxalign(p.plan_width as usize) + maxalign(SIZEOF_HEAPTUPLEHEADER)) as f64;
    subquery_size <= get_hash_memory_limit()
}

fn get_hash_memory_limit() -> f64 {
    let work_mem = init_small::globals::work_mem() as f64;
    let mult = guc_tables::vars::hash_mem_multiplier.read();
    work_mem * mult * 1024.0
}

// SizeofHeapTupleHeader (htup_details.h): offsetof(HeapTupleHeaderData, t_bits).
const SIZEOF_HEAPTUPLEHEADER: usize = 23;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

fn testexpr_is_hashable<'mcx>(testexpr: Node<'mcx>, param_ids: &IntList<'mcx>) -> PgResult<bool> {
    if let Some(op) = testexpr.as_op_expr() {
        return test_opexpr_is_hashable(op, param_ids);
    }
    if let Some(b) = testexpr.as_bool_expr() {
        if b.boolop == types_nodes::BoolExprType::AND_EXPR {
            for arg in &b.args {
                let Some(op) = arg.as_op_expr() else { return Ok(false) };
                if !test_opexpr_is_hashable(op, param_ids)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
    }
    Ok(false)
}

fn test_opexpr_is_hashable<'mcx>(
    op: &types_nodes::primnodes::OpExpr<'mcx>,
    param_ids: &IntList<'mcx>,
) -> PgResult<bool> {
    if !hash_ok_operator(op)? {
        return Ok(false);
    }
    if op.args.len() != 2 {
        return Ok(false);
    }
    if contain_exec_param(op.args.nth(0), param_ids)? {
        return Ok(false);
    }
    if vars::contain_var_clause(op.args.nth(1))? {
        return Ok(false);
    }
    Ok(true)
}

struct ContainExecParam<'a, 'mcx>(&'a IntList<'mcx>, bool);
impl<'a, 'mcx> clauses::NodeWalker<'mcx> for ContainExecParam<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            if p.paramkind == ParamKind::PARAM_EXEC && self.0.iter().any(|id| id == p.paramid) {
                self.1 = true;
                return Ok(true);
            }
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn contain_exec_param<'mcx>(node: Node<'mcx>, ids: &IntList<'mcx>) -> PgResult<bool> {
    let mut w = ContainExecParam(ids, false);
    w.visit(node)?;
    Ok(w.1)
}

// hash_ok_operator (subselect.c): hashable + strict. ARRAY_EQ/RECORD_EQ take
// the input-type-sensitive check.
fn hash_ok_operator(expr: &types_nodes::primnodes::OpExpr<'_>) -> PgResult<bool> {
    const ARRAY_EQ_OP: types_core::Oid = 1070;
    const RECORD_EQ_OP: types_core::Oid = 2988;
    let opid = expr.opno;
    if expr.args.len() != 2 {
        return Ok(false);
    }
    if opid == ARRAY_EQ_OP || opid == RECORD_EQ_OP {
        let (lty, _) = crate::costsize::expr_type_typmod(expr.args.nth(0));
        return lsyscache::op_hashjoinable(opid, lty);
    }
    if !lsyscache::op_hashjoinable(opid, 0)? {
        return Ok(false);
    }
    lsyscache::func_strict(lsyscache::get_opcode(opid)?)
}

// materialize_finished_plan (createplan.c) + cost_material (costsize.c):
// Material shield so repeated rescans of an uncorrelated subplan are cheap.
pub(crate) fn materialize_finished_plan<'mcx>(mcx: Mcx<'mcx>, subplan: Node<'mcx>) -> PgResult<Node<'mcx>> {
    // C's "horrid kluge": hoist the subplan's initPlans (and their cost
    // delta) onto the Material node so SS_finalize_plan sees them at the top.
    let mut initplan_cost = 0.0;
    let mut unsafe_initplans = false;
    // SAFETY: exclusive plan-tree ownership (clean_up_removed_plan_level
    // precedent).
    let init_plan = unsafe {
        subplan.with_plan_mut(|p| {
            for sp_node in &p.initPlan {
                let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
                initplan_cost += sp.startup_cost + sp.per_call_cost;
                if !sp.parallel_safe {
                    unsafe_initplans = true;
                }
            }
            p.startup_cost -= initplan_cost;
            p.total_cost -= initplan_cost;
            core::mem::take(&mut p.initPlan)
        })
    }
    .expect("plan node");
    let sub = subplan.as_plan().expect("plan node");
    let mut tlist = NodeList::nil();
    for te in &sub.targetlist {
        tlist.lappend(mcx, te)?;
    }
    let mut plan = Node::build::<types_nodes::plannodes::Material>(mcx)?;
    plan.plan.targetlist = tlist;
    plan.plan.qual = NodeList::nil();
    plan.plan.lefttree = Some(subplan);
    plan.plan.righttree = None;

    // cost_material's arithmetic, inline (no Path here).
    let startup_cost = sub.startup_cost;
    let mut run_cost = sub.total_cost - sub.startup_cost;
    run_cost += 2.0 * crate::gucs::cpu_operator_cost() * sub.plan_rows;
    let nbytes = crate::costsize::relation_byte_size(sub.plan_rows, sub.plan_width);
    let work_mem_bytes = init_small::globals::work_mem() as f64 * 1024.0;
    if nbytes > work_mem_bytes {
        let npages = (nbytes / 8192.0).ceil();
        run_cost += crate::gucs::seq_page_cost() * npages;
    }
    plan.plan.initPlan = init_plan;
    plan.plan.startup_cost = startup_cost + initplan_cost;
    plan.plan.total_cost = startup_cost + run_cost + initplan_cost;
    plan.plan.plan_rows = sub.plan_rows;
    plan.plan.plan_width = sub.plan_width;
    plan.plan.parallel_aware = false;
    plan.plan.parallel_safe = sub.parallel_safe && !unsafe_initplans;
    Ok(plan.seal())
}

pub fn generate_new_exec_param<'mcx>(
    run: &mut PlannerRun<'mcx>,
    paramtype: types_core::Oid,
    paramtypmod: i32,
    paramcollation: types_core::Oid,
) -> PgResult<(Param, Node<'mcx>)> {
    let paramid = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(run.mcx, paramtype)?;
    let prm = Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid,
        paramtype,
        paramtypmod,
        paramcollid: paramcollation,
        location: -1,
    };
    Ok((prm, Node::mk(run.mcx, prm)?))
}

pub(crate) fn get_first_col_type(plan: Node<'_>) -> (types_core::Oid, i32, types_core::Oid) {
    if let Some(first) = plan.as_plan().expect("plan node").targetlist.first() {
        let tent = first.as_target_entry().expect("tlist entry");
        if !tent.resjunk {
            let (ty, tm) = crate::costsize::expr_type_typmod(tent.expr);
            return (ty, tm, crate::pathkeys::expr_collation(tent.expr));
        }
    }
    (VOIDOID, -1, 0)
}

// cost_subplan (costsize.c). The testexpr qual cost is computed root-less,
// as C (NULL root).
pub(crate) fn cost_subplan<'mcx>(
    splan: &mut SubPlan<'mcx>,
    plan: Node<'mcx>,
) -> PgResult<()> {
    let p = plan.as_plan().expect("plan node");
    // cost_qual_eval's walker charges nothing for the AND shell itself, so
    // walking the testexpr whole equals C's implicit-AND list walk.
    let cost = match splan.testexpr {
        // C costs testexpr with root; this signature has no run, so an
        // array-Var SAOP in testexpr misses the DECHIST length estimate.
        Some(te) => crate::costsize::cost_qual_eval_node(None, te)?,
        None => Default::default(),
    };
    let mut startup = cost.startup;
    let mut per_tuple = cost.per_tuple;
    if splan.useHashTable {
        startup += p.total_cost + crate::gucs::cpu_operator_cost() * p.plan_rows;
    } else {
        let plan_run_cost = p.total_cost - p.startup_cost;
        match splan.subLinkType {
            SubLinkType::EXISTS_SUBLINK => {
                per_tuple += plan_run_cost / crate::costsize::clamp_row_est(p.plan_rows);
            }
            SubLinkType::ALL_SUBLINK | SubLinkType::ANY_SUBLINK => {
                per_tuple += 0.5 * plan_run_cost;
                per_tuple += 0.5 * p.plan_rows * crate::gucs::cpu_operator_cost();
            }
            _ => per_tuple += plan_run_cost,
        }
        if splan.parParam.is_nil() && exec_materializes_output(plan.node_tag()) {
            startup += p.startup_cost;
        } else {
            per_tuple += p.startup_cost;
        }
    }
    splan.startup_cost = startup;
    splan.per_call_cost = per_tuple;
    Ok(())
}

fn exec_materializes_output(tag: NodeTag) -> bool {
    matches!(tag, NodeTag::T_Sort | NodeTag::T_Material)
}

fn simplify_exists_query<'mcx>(run: &mut PlannerRun<'mcx>, query: &mut Query<'mcx>) -> PgResult<bool> {
    if query.commandType != types_nodes::CmdType::CMD_SELECT
        || query.setOperations.is_some()
        || query.hasAggs
        || !query.groupingSets.is_nil()
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasModifyingCTE
        || query.havingQual.is_some()
        || query.limitOffset.is_some()
        || !query.rowMarks.is_nil()
    {
        return Ok(false);
    }
    if let Some(limit) = query.limitCount {
        let node = clauses::eval_const_expressions_with_params(
            run.mcx,
            limit,
            run.glob.bound_params,
        )?;
        query.limitCount = Some(node);
        let Some(c) = node.as_const() else { return Ok(false) };
        debug_assert_eq!(c.consttype, types_core::catalog::INT8OID);
        if !c.constisnull && c.constvalue.as_i64() <= 0 {
            return Ok(false);
        }
        query.limitCount = None;
    }
    query.targetList = NodeList::nil();
    query.groupClause = NodeList::nil();
    query.windowClause = NodeList::nil();
    query.distinctClause = NodeList::nil();
    query.sortClause = NodeList::nil();
    query.hasDistinctOn = false;
    // The GROUP BY clauses are gone; drop the RTE_GROUP entry too.
    if query.hasGroupRTE {
        let mut new_rtable = NodeList::nil();
        for rte_node in &query.rtable {
            if rte_node.as_range_tbl_entry().expect("rtable cell").rtekind
                == types_nodes::parsenodes::RTEKind::RTE_GROUP
            {
                continue;
            }
            new_rtable.lappend(run.mcx, rte_node)?;
        }
        query.rtable = new_rtable;
        query.hasGroupRTE = false;
    }
    Ok(true)
}

// The scribble copy for make_subplan: struct fields plus list cells; nodes
// stay shared (see make_subplan comment).
pub(crate) fn query_cells_copy<'mcx>(mcx: Mcx<'mcx>, q: &Query<'mcx>) -> PgResult<Query<'mcx>> {
    Ok(Query {
        commandType: q.commandType,
        querySource: q.querySource,
        queryId: q.queryId,
        canSetTag: q.canSetTag,
        utilityStmt: q.utilityStmt,
        resultRelation: q.resultRelation,
        hasAggs: q.hasAggs,
        hasWindowFuncs: q.hasWindowFuncs,
        hasTargetSRFs: q.hasTargetSRFs,
        hasSubLinks: q.hasSubLinks,
        hasDistinctOn: q.hasDistinctOn,
        hasRecursive: q.hasRecursive,
        hasModifyingCTE: q.hasModifyingCTE,
        hasForUpdate: q.hasForUpdate,
        hasRowSecurity: q.hasRowSecurity,
        hasGroupRTE: q.hasGroupRTE,
        isReturn: q.isReturn,
        cteList: q.cteList.clone_in(mcx)?,
        rtable: q.rtable.clone_in(mcx)?,
        rteperminfos: q.rteperminfos.clone_in(mcx)?,
        jointree: q.jointree,
        mergeActionList: q.mergeActionList.clone_in(mcx)?,
        mergeTargetRelation: q.mergeTargetRelation,
        mergeJoinCondition: q.mergeJoinCondition,
        targetList: q.targetList.clone_in(mcx)?,
        r#override: q.r#override,
        onConflict: q.onConflict,
        returningOldAlias: q.returningOldAlias,
        returningNewAlias: q.returningNewAlias,
        returningList: q.returningList.clone_in(mcx)?,
        groupClause: q.groupClause.clone_in(mcx)?,
        groupDistinct: q.groupDistinct,
        groupingSets: q.groupingSets.clone_in(mcx)?,
        havingQual: q.havingQual,
        windowClause: q.windowClause.clone_in(mcx)?,
        distinctClause: q.distinctClause.clone_in(mcx)?,
        sortClause: q.sortClause.clone_in(mcx)?,
        limitOffset: q.limitOffset,
        limitCount: q.limitCount,
        limitOption: q.limitOption,
        rowMarks: q.rowMarks.clone_in(mcx)?,
        setOperations: q.setOperations,
        constraintDeps: q.constraintDeps.clone_in(mcx)?,
        withCheckOptions: q.withCheckOptions.clone_in(mcx)?,
        stmt_location: q.stmt_location,
        stmt_len: q.stmt_len,
    })
}

/// SS_replace_correlation_vars (subselect.c): uplevel Vars/PHVs/Aggrefs/
/// GroupingFuncs/MergeSupportFuncs become PARAM_EXEC Params, parked on the
/// owning ancestor's plan_params. ReturningExpr nodes don't exist in this
/// tree.
pub fn ss_replace_correlation_vars<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Ok(replace_correlation_vars_mutator(run, expr)?.unwrap_or(expr))
}

fn replace_correlation_vars_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    if let Some(v) = node.as_var() {
        if v.varlevelsup > 0 {
            return Ok(Some(crate::paramassign::replace_outer_var(run, v)?));
        }
        return Ok(None);
    }
    if let Some(phv) = node.as_place_holder_var() {
        if phv.phlevelsup > 0 {
            return Ok(Some(crate::paramassign::replace_outer_placeholdervar(run, phv, node)?));
        }
    }
    if let Some(a) = node.as_aggref() {
        if a.agglevelsup > 0 {
            return Ok(Some(crate::paramassign::replace_outer_agg(run, a, node)?));
        }
    }
    if let Some(g) = node.as_grouping_func() {
        if g.agglevelsup > 0 {
            return Ok(Some(crate::paramassign::replace_outer_grouping(run, g, node)?));
        }
    }
    if let Some(r) = node.as_returning_expr() {
        if r.retlevelsup > 0 {
            return Ok(Some(crate::paramassign::replace_outer_returning(
                run,
                r.retlevelsup as u32,
                node,
            )?));
        }
    }
    if let Some(m) = node.as_merge_support_func() {
        // C: root->parse->commandType != CMD_MERGE; the level's command type
        // is carried on the root (queries interns lazily, post-preprocess).
        if run.root.command_type != types_nodes::CmdType::CMD_MERGE {
            return Ok(Some(crate::paramassign::replace_outer_merge_support(run, m, node)?));
        }
        return Ok(None);
    }
    clauses::expression_tree_mutator(run.mcx, node, &mut |n| {
        replace_correlation_vars_mutator(run, n)
    })
}

pub fn ss_charge_for_initplans(run: &mut PlannerRun<'_>, final_rel: RelId) -> PgResult<()> {
    if run.root.init_plans.is_empty() {
        return Ok(());
    }
    let mut initplan_cost = 0.0;
    let mut unsafe_initplans = false;
    for &ipid in run.root.init_plans.iter() {
        let sp = run
            .root
            .expr_node(ipid)
            .as_sub_plan()
            .expect("init_plans holds SubPlan nodes");
        initplan_cost += sp.startup_cost + sp.per_call_cost;
        if !sp.parallel_safe {
            unsafe_initplans = true;
        }
    }
    let path_ids: mcx::PgVec<'_, types_pathnodes::PathId> = {
        let mut v = mcx::PgVec::new_in(run.mcx);
        v.extend(run.root.rel(final_rel).pathlist.iter().copied());
        v
    };
    for pid in path_ids.iter() {
        let p = run.root.path_mut(*pid).base_mut();
        p.startup_cost += initplan_cost;
        p.total_cost += initplan_cost;
        if unsafe_initplans {
            p.parallel_safe = false;
        }
    }
    if unsafe_initplans {
        let rel = run.root.rel_mut(final_rel);
        rel.partial_pathlist.clear();
        rel.consider_parallel = false;
    } else {
        let partial: mcx::PgVec<'_, types_pathnodes::PathId> = {
            let mut v = mcx::PgVec::new_in(run.mcx);
            v.extend(run.root.rel(final_rel).partial_pathlist.iter().copied());
            v
        };
        for pid in partial.iter() {
            let p = run.root.path_mut(*pid).base_mut();
            p.startup_cost += initplan_cost;
            p.total_cost += initplan_cost;
        }
    }
    Ok(())
}

/// SS_attach_initplans (subselect.c): the current level's initplans move onto
/// the topmost plan node.
pub fn ss_attach_initplans<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<()> {
    if run.root.init_plans.is_empty() {
        return Ok(());
    }
    let mut list = NodeList::nil();
    for &ipid in run.root.init_plans.iter() {
        list.lappend(run.mcx, *run.root.expr_node(ipid))?;
    }
    // SAFETY: createplan exclusively owns the just-built tree (C assigns
    // plan->initPlan in place).
    unsafe { plan.with_plan_mut(|p| p.initPlan = list) }.expect("plan node");
    Ok(())
}

/// SS_finalize_plan (subselect.c): compute extParam/allParam for every node.
pub fn ss_finalize_plan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    plan: Node<'mcx>,
    outer_params: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    // Planner-arena set -> nodes-side bitmapset, converted once at the boundary.
    let mut valid = types_nodes::bitmapset::Bitmapset::empty();
    for (i, w) in crate::relnode::relids_word_slice(outer_params).iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            let bit = w.trailing_zeros();
            valid.add_member(run.mcx, (i as i32) * 64 + bit as i32)?;
            w &= w - 1;
        }
    }
    finalize_plan(run, root, plan, -1, &valid, &types_nodes::bitmapset::Bitmapset::empty())?;
    Ok(())
}

fn finalize_plan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    plan: Node<'mcx>,
    gather_param: i32,
    valid_params: &types_nodes::bitmapset::Bitmapset<'mcx>,
    scan_params: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<types_nodes::bitmapset::Bitmapset<'mcx>> {
    let mut gather_param = gather_param;
    let mcx = run.mcx;
    let mut paramids = types_nodes::bitmapset::Bitmapset::empty();
    let mut locally_added_param: i32 = -1;
    // LockRows/ModifyTable extend scan_params for their descendants only.
    let mut scan_owned: Option<types_nodes::bitmapset::Bitmapset<'mcx>> = None;
    let base = plan.as_plan().expect("plan node");

    let mut init_ext_param = types_nodes::bitmapset::Bitmapset::empty();
    let mut init_set_param = types_nodes::bitmapset::Bitmapset::empty();
    for ip in &base.initPlan {
        let sp = ip.as_sub_plan().expect("initPlan cell is a SubPlan");
        let initplan = run
            .glob
            .subplans
            .nth((sp.plan_id - 1) as usize);
        init_ext_param.add_members(mcx, &initplan.as_plan().expect("plan node").extParam)?;
        for id in sp.setParam.iter() {
            init_set_param.add_member(mcx, id)?;
        }
    }
    let mut valid = valid_params.clone_in(mcx)?;
    valid.add_members(mcx, &init_set_param)?;

    finalize_primnode_list(run, root, &base.targetlist, &mut paramids)?;
    finalize_primnode_list(run, root, &base.qual, &mut paramids)?;

    // A parallel-aware scan depends on the parent Gather's rescan Param.
    if base.parallel_aware {
        assert!(gather_param >= 0, "parallel-aware plan node is not below a Gather");
        paramids.add_member(mcx, gather_param)?;
    }

    match plan.node_tag() {
        // Children may reference rescan_param; it stays out of scan_params
        // and (as a locally added param) out of this level's extParam.
        NodeTag::T_Gather => {
            let g = plan.as_gather().unwrap();
            if g.rescan_param >= 0 {
                locally_added_param = g.rescan_param;
                valid.add_member(mcx, locally_added_param)?;
                assert!(gather_param < 0, "nested Gathers are not supported");
                gather_param = locally_added_param;
            }
        }
        NodeTag::T_GatherMerge => {
            let g = plan.as_gather_merge().unwrap();
            if g.rescan_param >= 0 {
                locally_added_param = g.rescan_param;
                valid.add_member(mcx, locally_added_param)?;
                assert!(gather_param < 0, "nested Gathers are not supported");
                gather_param = locally_added_param;
            }
        }
        NodeTag::T_Result => {
            if let Some(rcq) = plan.as_result().unwrap().resconstantqual {
                finalize_primnode(run, root, rcq, &mut paramids)?;
            }
        }
        NodeTag::T_SeqScan | NodeTag::T_NamedTuplestoreScan => {
            paramids.add_members(mcx, scan_params)?;
        }
        // Agg skips C's AGG_HASHED aggParams scan: no executor consumer yet.
        NodeTag::T_Sort
        | NodeTag::T_IncrementalSort
        | NodeTag::T_Agg
        | NodeTag::T_Material
        | NodeTag::T_SetOp
        | NodeTag::T_ProjectSet => {}
        NodeTag::T_WindowAgg => {
            let wa = plan.as_window_agg().unwrap();
            if let Some(off) = wa.startOffset {
                finalize_primnode(run, root, off, &mut paramids)?;
            }
            if let Some(off) = wa.endOffset {
                finalize_primnode(run, root, off, &mut paramids)?;
            }
        }
        NodeTag::T_SampleScan => {
            if let Some(ts) = plan.as_sample_scan().unwrap().tablesample {
                finalize_primnode(run, root, ts, &mut paramids)?;
            }
        }
        NodeTag::T_TidScan => {
            finalize_primnode_list(run, root, &plan.as_tid_scan().unwrap().tidquals, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_TidRangeScan => {
            finalize_primnode_list(
                run,
                root,
                &plan.as_tid_range_scan().unwrap().tidrangequals,
                &mut paramids,
            )?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_Memoize => {
            finalize_primnode_list(run, root, &plan.as_memoize().unwrap().param_exprs, &mut paramids)?;
        }
        // cteParam is linkage only; the CTE plan's extParam matters (C bug #4902).
        NodeTag::T_CteScan => {
            let plan_id = plan.as_cte_scan().unwrap().ctePlanId;
            assert!(
                plan_id >= 1 && plan_id as usize <= run.glob.subplans.len(),
                "could not find plan for CteScan referencing plan ID {plan_id}"
            );
            let cteplan = run.glob.subplans.nth((plan_id - 1) as usize);
            paramids.add_members(mcx, &cteplan.as_plan().expect("plan node").extParam)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            finalize_primnode_list(run, root, &s.indexqual, &mut paramids)?;
            finalize_primnode_list(run, root, &s.indexorderby, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_IndexOnlyScan => {
            let s = plan.as_index_only_scan().unwrap();
            finalize_primnode_list(run, root, &s.indexqual, &mut paramids)?;
            finalize_primnode_list(run, root, &s.recheckqual, &mut paramids)?;
            finalize_primnode_list(run, root, &s.indexorderby, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_BitmapIndexScan => {
            finalize_primnode_list(
                run,
                root,
                &plan.as_bitmap_index_scan().unwrap().indexqual,
                &mut paramids,
            )?;
        }
        NodeTag::T_BitmapHeapScan => {
            finalize_primnode_list(
                run,
                root,
                &plan.as_bitmap_heap_scan().unwrap().bitmapqualorig,
                &mut paramids,
            )?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_Append => {
            for sub in &plan.as_append().unwrap().appendplans {
                let child = finalize_plan(run, root, sub, gather_param, &valid, scan_params)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_MergeAppend => {
            for sub in &plan.as_merge_append().unwrap().mergeplans {
                let child = finalize_plan(run, root, sub, gather_param, &valid, scan_params)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_BitmapAnd => {
            for sub in &plan.as_bitmap_and().unwrap().bitmapplans {
                let child = finalize_plan(run, root, sub, gather_param, &valid, scan_params)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_BitmapOr => {
            for sub in &plan.as_bitmap_or().unwrap().bitmapplans {
                let child = finalize_plan(run, root, sub, gather_param, &valid, scan_params)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            if let Some(off) = l.limitOffset {
                finalize_primnode(run, root, off, &mut paramids)?;
            }
            if let Some(cnt) = l.limitCount {
                finalize_primnode(run, root, cnt, &mut paramids)?;
            }
        }
        // epqParam becomes valid for descendants (and forced onto their scan
        // nodes); never propagated up.
        NodeTag::T_LockRows => {
            locally_added_param = plan.as_lock_rows().unwrap().epqParam;
            valid.add_member(mcx, locally_added_param)?;
            let mut s = scan_params.clone_in(mcx)?;
            s.add_member(mcx, locally_added_param)?;
            scan_owned = Some(s);
        }
        // Child nodes may reference wtParam; it never joins extParams
        // (WorkTableScan's wtParam is a local of the RecursiveUnion level).
        NodeTag::T_RecursiveUnion => {
            locally_added_param = plan.as_recursive_union().unwrap().wtParam;
            valid.add_member(mcx, locally_added_param)?;
        }
        NodeTag::T_WorkTableScan => {
            paramids.add_member(mcx, plan.as_work_table_scan().unwrap().wtParam)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_NestLoop => {
            let nl = plan.as_nest_loop().unwrap();
            finalize_primnode_list(run, root, &nl.join.joinqual, &mut paramids)?;
        }
        NodeTag::T_FunctionScan => {
            // Per-function param sets are recorded in funcparams; the
            // executor rescans re-evaluate only functions whose params
            // changed.
            let fs = plan.as_function_scan().unwrap();
            for f_node in &fs.functions {
                let f = f_node.as_range_tbl_function().expect("functions cell");
                let mut func_params = types_nodes::bitmapset::Bitmapset::empty();
                if let Some(e) = f.funcexpr {
                    finalize_primnode(run, root, e, &mut func_params)?;
                }
                paramids.add_members(mcx, &func_params)?;
                // SAFETY: plan tree is exclusively owned by this planning
                // invocation (C writes rtfunc->funcparams in place).
                unsafe {
                    f_node.with_mut::<types_nodes::parsenodes::RangeTblFunction, _>(|rf| {
                        rf.funcparams = func_params;
                    })
                }
                .expect("RangeTblFunction");
            }
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_ValuesScan => {
            let vs = plan.as_values_scan().unwrap();
            finalize_primnode_list(run, root, &vs.values_lists, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        // fdw_scan_tlist is assumed to contain no Params.
        NodeTag::T_ForeignScan => {
            let fs = plan.as_foreign_scan().unwrap();
            finalize_primnode_list(run, root, &fs.fdw_exprs, &mut paramids)?;
            finalize_primnode_list(run, root, &fs.fdw_recheck_quals, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_SubqueryScan => {
            let ss = plan.as_subquery_scan().unwrap();
            let rel = crate::relnode::find_base_rel(root, ss.scan.scanrelid as i32);
            let idx = root.rel(rel).subroot_idx.expect("subquery rel has a subroot");
            let subroot = &run.rel_subroots[idx].root;
            // subselect.c:2554-2560: the subquery finalizes under its subroot
            // with the parent Gather's rescan param carried across (a
            // parallel-aware node inside the subquery hangs off our Gather).
            let mut subquery_params = types_nodes::bitmapset::Bitmapset::empty();
            for (i, w) in
                crate::relnode::relids_word_slice(&subroot.outer_params).iter().enumerate()
            {
                let mut w = *w;
                while w != 0 {
                    let bit = w.trailing_zeros();
                    subquery_params.add_member(mcx, (i as i32) * 64 + bit as i32)?;
                    w &= w - 1;
                }
            }
            if gather_param >= 0 {
                subquery_params.add_member(mcx, gather_param)?;
            }
            let subplan = ss.subplan.expect("SubqueryScan subplan");
            finalize_plan(
                run,
                subroot,
                subplan,
                gather_param,
                &subquery_params,
                &types_nodes::bitmapset::Bitmapset::empty(),
            )?;
            paramids
                .add_members(mcx, &subplan.as_plan().expect("plan node").extParam)?;
            paramids.add_members(mcx, scan_params)?;
        }
        NodeTag::T_MergeJoin => {
            let mj = plan.as_merge_join().unwrap();
            finalize_primnode_list(run, root, &mj.join.joinqual, &mut paramids)?;
            finalize_primnode_list(run, root, &mj.mergeclauses, &mut paramids)?;
        }
        NodeTag::T_HashJoin => {
            let hj = plan.as_hash_join().unwrap();
            finalize_primnode_list(run, root, &hj.join.joinqual, &mut paramids)?;
            finalize_primnode_list(run, root, &hj.hashclauses, &mut paramids)?;
        }
        NodeTag::T_Hash => {
            finalize_primnode_list(run, root, &plan.as_hash().unwrap().hashkeys, &mut paramids)?;
        }
        NodeTag::T_Unique | NodeTag::T_Group => {}
        NodeTag::T_TableFuncScan => {
            let tf = plan.as_table_func_scan().unwrap().tablefunc.expect("tablefunc");
            finalize_primnode(run, root, tf, &mut paramids)?;
            paramids.add_members(mcx, scan_params)?;
        }
        // exclRelTlist contains only Vars, no examination needed.
        NodeTag::T_ModifyTable => {
            let mt = plan.as_modify_table().unwrap();
            locally_added_param = mt.epqParam;
            valid.add_member(mcx, locally_added_param)?;
            let mut s = scan_params.clone_in(mcx)?;
            s.add_member(mcx, locally_added_param)?;
            scan_owned = Some(s);
            finalize_primnode_list(run, root, &mt.returningLists, &mut paramids)?;
            finalize_primnode_list(run, root, &mt.onConflictSet, &mut paramids)?;
            if let Some(w) = mt.onConflictWhere {
                finalize_primnode(run, root, w, &mut paramids)?;
            }
        }
        other => panic!("finalize_plan (subselect.c): {other:?}; M2 plan lane"),
    }

    let scan_params = scan_owned.as_ref().unwrap_or(scan_params);
    if let Some(child) = base.lefttree {
        let child_params = finalize_plan(run, root, child, gather_param, &valid, scan_params)?;
        paramids.add_members(mcx, &child_params)?;
    }
    if let Some(child) = base.righttree {
        // A nestloop's inner side may consume this join's nestParams; those
        // are valid below and do not count as used at this level.
        let mut nestloop_params = types_nodes::bitmapset::Bitmapset::empty();
        if let Some(nl) = plan.as_nest_loop() {
            for nlp_node in &nl.nestParams {
                let nlp = nlp_node.as_nest_loop_param().expect("nestParams cell");
                nestloop_params.add_member(mcx, nlp.paramno)?;
            }
        }
        if nestloop_params.is_empty() {
            let child_params = finalize_plan(run, root, child, gather_param, &valid, scan_params)?;
            paramids.add_members(mcx, &child_params)?;
        } else {
            let mut inner_valid = valid.clone_in(mcx)?;
            inner_valid.add_members(mcx, &nestloop_params)?;
            let mut child_params = finalize_plan(run, root, child, gather_param, &inner_valid, scan_params)?;
            child_params.del_members(&nestloop_params);
            paramids.add_members(mcx, &child_params)?;
        }
    }

    if locally_added_param >= 0 {
        paramids.del_member(locally_added_param);
    }

    assert!(
        paramids.is_subset(&valid),
        "plan should not reference subplan's variable"
    );

    let mut all_param = paramids.clone_in(mcx)?;
    all_param.add_members(mcx, &init_ext_param)?;
    all_param.add_members(mcx, &init_set_param)?;
    let mut ext_param = paramids.clone_in(mcx)?;
    ext_param.add_members(mcx, &init_ext_param)?;
    ext_param.del_members(&init_set_param);
    // C returns plan->allParam: a child's initplan ext/set params propagate
    // into every ancestor's paramids.
    let ret = all_param.clone_in(mcx)?;
    // SAFETY: the plan tree is exclusively owned by this planning invocation
    // (C writes the same fields in place).
    unsafe {
        plan.with_plan_mut(|p| {
            p.extParam = ext_param;
            p.allParam = all_param;
        })
    }
    .expect("plan node");
    Ok(ret)
}

fn finalize_primnode_list<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    list: &NodeList<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    for node in list {
        finalize_primnode(run, root, node, paramids)?;
    }
    Ok(())
}

struct FinalizePrimnode<'a, 'mcx> {
    run: &'a PlannerRun<'mcx>,
    root: &'a types_pathnodes::PlannerInfo<'mcx>,
    paramids: &'a mut types_nodes::bitmapset::Bitmapset<'mcx>,
}

impl<'a, 'mcx> clauses::NodeWalker<'mcx> for FinalizePrimnode<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            if p.paramkind == ParamKind::PARAM_EXEC {
                self.paramids.add_member(self.run.mcx, p.paramid)?;
            }
            return Ok(false);
        }
        if node.node_tag() == NodeTag::T_Aggref {
            // The Aggref becomes an InitPlan output Param in setrefs; account
            // for that Param here (C's find_minmax_agg_replacement_param wart).
            if let Some(prm) =
                crate::setrefs::find_minmax_agg_replacement_param(self.root, node)
            {
                let paramid = self
                    .root
                    .expr_node(prm)
                    .as_param()
                    .expect("minmax output param")
                    .paramid;
                self.paramids.add_member(self.run.mcx, paramid)?;
            }
        }
        if let Some(sp) = node.as_sub_plan() {
            let plan = self.run.glob.subplans.nth((sp.plan_id - 1) as usize);
            if let Some(te) = sp.testexpr {
                self.visit(te)?;
            }
            // Output params of this subplan aren't change signals: it is
            // re-evaluated per call anyway.
            for id in sp.paramIds.iter() {
                self.paramids.del_member(id);
            }
            for arg in &sp.args {
                self.visit(arg)?;
            }
            let mut subparamids =
                plan.as_plan().expect("plan node").extParam.clone_in(self.run.mcx)?;
            for id in sp.parParam.iter() {
                subparamids.del_member(id);
            }
            self.paramids.add_members(self.run.mcx, &subparamids)?;
            return Ok(false);
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn finalize_primnode<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &types_pathnodes::PlannerInfo<'mcx>,
    node: Node<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    FinalizePrimnode { run, root, paramids }.visit(node)?;
    Ok(())
}

#[cold]
#[inline(never)]
fn no_array_type(elemtype: types_core::Oid) -> Box<types_error::PgError> {
    let tyname = format_type::format_type_be(elemtype).unwrap_or_else(|_| elemtype.to_string());
    Box::new(
        types_error::PgError::error(format!(
            "could not find array type for data type {tyname}"
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

/// SS_make_initplan_from_plan (subselect.c): attach a finished plan tree as an
/// InitPlan of the current level; prm is the pre-made output Param's NodeId.
pub(crate) fn ss_make_initplan_from_plan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subroot: crate::run::SubrootState<'mcx>,
    plan: Node<'mcx>,
    prm_id: types_pathnodes::NodeId,
) -> PgResult<()> {
    let mcx = run.mcx;
    run.glob.subplans.lappend(mcx, plan)?;
    run.subroots.push(subroot);
    // >= not ==: ancestors' parked subroots may be in flight (build_subplan).
    debug_assert!(run.subroots.len() >= run.glob.subplans.len());
    let plan_id = run.glob.subplans.len() as i32;

    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(plan);
    let paramid = run
        .root
        .expr_node(prm_id)
        .as_param()
        .expect("minmax output param")
        .paramid;
    let mut splan = SubPlan {
        subLinkType: SubLinkType::EXPR_SUBLINK,
        testexpr: None,
        paramIds: IntList::nil(),
        plan_id,
        plan_name: Some(str_in(mcx, &format!("InitPlan {plan_id}"))?),
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: false,
        parallel_safe: plan.as_plan().expect("plan node").parallel_safe,
        setParam: IntList::make1(mcx, paramid)?,
        parParam: IntList::nil(),
        args: NodeList::nil(),
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };
    cost_subplan(&mut splan, plan);
    let splan_id = run.intern_expr(Node::mk(mcx, splan)?);
    run.root.init_plans.push(splan_id);
    Ok(())
}
