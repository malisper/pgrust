//! subselect.c slice: uncorrelated EXISTS/EXPR initplans plus the real
//! pull_up_sublinks transform (prepjointree.c) — top-level ANY/EXISTS
//! sublinks become SEMI/ANTI joins; testexpr-bearing SubPlans stay loud.

use clauses::NodeWalker;
use mcx::Mcx;
use types_core::catalog::{BOOLOID, VOIDOID};
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

// pull_up_sublinks (prepjointree.c): convert top-level ANY/EXISTS sublinks
// into SEMI/ANTI JoinExprs stacked into the jointree. New JoinExpr and
// FromExpr nodes are freshly built here, so the post-hoc quals/child fixups
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

// pull_up_sublinks_qual_recurse (prepjointree.c). jtlink2/available_rels2 is
// the second insertion slot for quals of an already-pulled-up ANY sublink.
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
                assert_no_values_to_any(sl)?;
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

// convert_VALUES_to_ANY (subselect.c) is unported; keep the shape loud rather
// than planning a different (subquery-scan) tree than C's ScalarArrayOpExpr.
fn assert_no_values_to_any(sl: &SubLink<'_>) -> PgResult<()> {
    let sub = sl.subselect.as_query().expect("transformed sublink holds a Query");
    let only_values = sub.rtable.len() == 1
        && sub
            .rtable
            .first()
            .and_then(|n| n.as_range_tbl_entry())
            .is_some_and(|r| r.rtekind == RTEKind::RTE_VALUES);
    assert!(
        !only_values,
        "convert_VALUES_to_ANY (subselect.c): IN (VALUES ...) simplification unported"
    );
    Ok(())
}

// convert_ANY_sublink_to_join (subselect.c): returns (rarg, quals) for the
// JOIN_SEMI JoinExpr the caller assembles, after appending the subselect to
// the rangetable. None = not convertible (falls back to SubPlan, loud there).
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
    assert!(
        !use_lateral,
        "convert_ANY_sublink_to_join (subselect.c): LATERAL semijoin; lateral lane unported"
    );
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
            lateral: false,
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

// convert_testexpr (subselect.c): PARAM_SUBLINK Params -> the given nodes.
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

// convert_EXISTS_sublink_to_join (subselect.c): returns (rarg, whereClause)
// with the simplified sub-select's rtable already merged into the parent.
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

    // CombineRangeTables (rewriteManip.c). RTEs are copied, not scribbled:
    // the sub-Query is shared with the plancache'd parse tree.
    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &subselect.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        assert!(
            srte.rtekind == RTEKind::RTE_RELATION,
            "convert_EXISTS_sublink_to_join (subselect.c): {:?} RTE in EXISTS body",
            srte.rtekind
        );
        let new_index = if srte.perminfoindex > 0 {
            srte.perminfoindex + perm_offset
        } else {
            srte.perminfoindex
        };
        parse.rtable.lappend(
            mcx,
            crate::prepjointree::rte_copy_with_perminfoindex(mcx, srte, new_index)?,
        )?;
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
                if nv.varnosyn > 0 {
                    nv.varnosyn += rtoffset as u32;
                }
            } else {
                nv.varlevelsup -= 1;
            }
            return Ok(Some(Node::mk(mcx, nv)?));
        }
        if node.node_tag() == NodeTag::T_SubLink {
            panic!(
                "IncrementVarSublevelsUp (rewriteManip.c): nested SubLink in pulled-up \
                 EXISTS qual; sublink lane"
            );
        }
        clauses::expression_tree_mutator(mcx, node, &mut |n| mutate(mcx, n, rtoffset))
    }
    Ok(mutate(mcx, node, rtoffset)?.unwrap_or(node))
}

/// SS_process_sublinks (subselect.c).
pub fn ss_process_sublinks<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    is_qual: bool,
) -> PgResult<Node<'mcx>> {
    Ok(process_sublinks_mutator(run, expr, is_qual)?.unwrap_or(expr))
}

// C's AND/OR-flatness arms are unreachable: BoolExpr panicked upstream in
// eval_const_expressions/canonicalize_qual.
fn process_sublinks_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    is_top_qual: bool,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_SubLink {
        let sl = node.as_sub_link().unwrap();
        assert!(
            sl.testexpr.is_none(),
            "make_subplan (subselect.c): testexpr-bearing {:?} SubPlan (hashed/linear \
             subplan execution) unported — NOT IN and un-pulled-up ANY stay loud",
            sl.subLinkType
        );
        return Ok(Some(make_subplan(run, sl, is_top_qual)?));
    }
    debug_assert!(!matches!(
        node.node_tag(),
        NodeTag::T_SubPlan | NodeTag::T_AlternativeSubPlan | NodeTag::T_Query
    ));
    clauses::expression_tree_mutator(run.mcx, node, &mut |n| {
        process_sublinks_mutator(run, n, false)
    })
}

// make_subplan (subselect.c). C copyObject's the sub-Query because rules can
// alias one Query from several SubLinks; parser-built SubLinks hold the only
// reference, so a list-cell-level copy is the scribble target.
fn make_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sublink: &SubLink<'mcx>,
    is_top_qual: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let orig = sublink
        .subselect
        .as_query()
        .expect("make_subplan on an untransformed sublink");
    let mut subquery = query_cells_copy(mcx, orig)?;

    let tuple_fraction = match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            simplify_exists_query(run, &mut subquery)?;
            1.0
        }
        SubLinkType::EXPR_SUBLINK => 0.0,
        other => panic!(
            "make_subplan (subselect.c): {other:?} sublink; M2 sublink lane"
        ),
    };

    debug_assert!(run.root.plan_params.is_empty());
    run.push_root()?;
    crate::subquery::subquery_planner(run, subquery, tuple_fraction, None)?;

    let final_rel = fetch_final_rel(run);
    let best_path = get_cheapest_fractional_path(run, final_rel, tuple_fraction);
    let plan = create_plan(run, best_path)?;
    run.pop_root_to_subroot();
    // Correlated references park plan_params on the parent root (loud upstream).
    debug_assert!(run.root.plan_params.is_empty());

    build_subplan(run, plan, sublink.subLinkType, is_top_qual)
}

// build_subplan (subselect.c), parParam==NIL EXISTS/EXPR initplan arms only.
fn build_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    sub_link_type: SubLinkType,
    unknown_eq_false: bool,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (first_col_type, first_col_typmod, first_col_collation) = get_first_col_type(plan);
    let parallel_safe = plan.as_plan().expect("plan node").parallel_safe;

    let (prm, prm_node) = match sub_link_type {
        SubLinkType::EXISTS_SUBLINK => generate_new_exec_param(run, BOOLOID, -1, 0)?,
        SubLinkType::EXPR_SUBLINK => {
            let te = plan
                .as_plan()
                .unwrap()
                .targetlist
                .first()
                .expect("EXPR subplan tlist")
                .as_target_entry()
                .expect("tlist entry");
            debug_assert!(!te.resjunk);
            let (ty, tm) = crate::costsize::expr_type_typmod(te.expr);
            generate_new_exec_param(run, ty, tm, crate::pathkeys::expr_collation(te.expr))?
        }
        other => panic!("build_subplan (subselect.c): {other:?}; M2 sublink lane"),
    };

    run.glob.subplans.lappend(mcx, plan)?;
    let plan_id = run.glob.subplans.len() as i32;
    debug_assert_eq!(run.subroots.len(), run.glob.subplans.len());

    let mut splan = SubPlan {
        subLinkType: sub_link_type,
        testexpr: None,
        paramIds: IntList::nil(),
        plan_id,
        plan_name: Some(str_in(mcx, &format!("InitPlan {plan_id}"))?),
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: unknown_eq_false,
        parallel_safe,
        setParam: IntList::make1(mcx, prm.paramid)?,
        parParam: IntList::nil(),
        args: NodeList::nil(),
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };
    cost_subplan(&mut splan, plan);
    let splan_node = Node::mk(mcx, splan)?;
    let splan_id = run.intern_expr(splan_node);
    run.root.init_plans.push(splan_id);

    Ok(prm_node)
}

/// generate_new_exec_param (paramassign.c).
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

// cost_subplan (costsize.c), initplan slice (NULL testexpr: qual costs drop out).
pub(crate) fn cost_subplan<'mcx>(splan: &mut SubPlan<'mcx>, plan: Node<'mcx>) {
    let p = plan.as_plan().expect("plan node");
    let mut startup = 0.0;
    let mut per_tuple = 0.0;
    let plan_run_cost = p.total_cost - p.startup_cost;
    match splan.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            per_tuple += plan_run_cost / crate::costsize::clamp_row_est(p.plan_rows);
        }
        SubLinkType::ALL_SUBLINK | SubLinkType::ANY_SUBLINK => {
            unreachable!("ALL/ANY subplans are loud upstream")
        }
        _ => per_tuple += plan_run_cost,
    }
    if splan.parParam.is_nil() && exec_materializes_output(plan.node_tag()) {
        startup += p.startup_cost;
    } else {
        per_tuple += p.startup_cost;
    }
    splan.startup_cost = startup;
    splan.per_call_cost = per_tuple;
}

// ExecMaterializesOutput (execAmi.c) over the ported node set.
fn exec_materializes_output(tag: NodeTag) -> bool {
    matches!(tag, NodeTag::T_Sort | NodeTag::T_Material)
}

// simplify_EXISTS_query (subselect.c).
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
    if query.hasGroupRTE {
        panic!("simplify_EXISTS_query (subselect.c): RTE_GROUP removal; M2 grouping lane");
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

/// SS_replace_correlation_vars (subselect.c): the uncorrelated lane proves no
/// uplevel Var exists (replace_outer_var parks correlation on the parent's
/// plan_params — M2 correlated-subquery lane).
pub fn ss_replace_correlation_vars<'mcx>(expr: Node<'mcx>) -> PgResult<Node<'mcx>> {
    if contains_uplevel_var(expr)? {
        panic!("replace_outer_var (paramassign.c): correlated subquery; M2 sublink lane");
    }
    Ok(expr)
}

struct ContainsUplevel;
impl<'mcx> clauses::NodeWalker<'mcx> for ContainsUplevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(v) = node.as_var() {
            return Ok(v.varlevelsup > 0);
        }
        if let Some(a) = node.as_aggref() {
            if a.agglevelsup > 0 {
                panic!("replace_outer_agg (paramassign.c): uplevel Aggref; M2 sublink lane");
            }
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn contains_uplevel_var(expr: Node<'_>) -> PgResult<bool> {
    ContainsUplevel.visit(expr)
}

/// SS_charge_for_initplans (subselect.c).
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
    plan: Node<'mcx>,
    outer_params: &types_pathnodes::Relids<'mcx>,
) -> PgResult<()> {
    // Planner-arena set -> nodes-side bitmapset, converted once at the boundary.
    let mut valid = types_nodes::bitmapset::Bitmapset::empty();
    if let Some(b) = outer_params {
        for (i, w) in b.words.iter().enumerate() {
            let mut w = *w;
            while w != 0 {
                let bit = w.trailing_zeros();
                valid.add_member(run.mcx, (i as i32) * 64 + bit as i32)?;
                w &= w - 1;
            }
        }
    }
    finalize_plan(run, plan, &valid)?;
    Ok(())
}

// finalize_plan (subselect.c) over the ported node set; gather_param and
// scan_params legs (parallel, EPQ) are dead here.
fn finalize_plan<'mcx>(
    run: &PlannerRun<'mcx>,
    plan: Node<'mcx>,
    valid_params: &types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<types_nodes::bitmapset::Bitmapset<'mcx>> {
    let mcx = run.mcx;
    let mut paramids = types_nodes::bitmapset::Bitmapset::empty();
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

    finalize_primnode_list(run, &base.targetlist, &mut paramids)?;
    finalize_primnode_list(run, &base.qual, &mut paramids)?;
    debug_assert!(!base.parallel_aware, "gather_param leg; M3 parallel lane");

    match plan.node_tag() {
        NodeTag::T_Result => {
            if let Some(rcq) = plan.as_result().unwrap().resconstantqual {
                finalize_primnode(run, rcq, &mut paramids)?;
            }
        }
        NodeTag::T_SeqScan
        | NodeTag::T_Sort
        | NodeTag::T_IncrementalSort
        | NodeTag::T_Agg
        | NodeTag::T_Material => {}
        // cteParam is linkage only; the CTE plan's extParam matters (C bug #4902).
        NodeTag::T_CteScan => {
            let plan_id = plan.as_cte_scan().unwrap().ctePlanId;
            assert!(
                plan_id >= 1 && plan_id as usize <= run.glob.subplans.len(),
                "could not find plan for CteScan referencing plan ID {plan_id}"
            );
            let cteplan = run.glob.subplans.nth((plan_id - 1) as usize);
            paramids.add_members(mcx, &cteplan.as_plan().expect("plan node").extParam)?;
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            finalize_primnode_list(run, &s.indexqual, &mut paramids)?;
            finalize_primnode_list(run, &s.indexorderby, &mut paramids)?;
        }
        NodeTag::T_BitmapIndexScan => {
            finalize_primnode_list(
                run,
                &plan.as_bitmap_index_scan().unwrap().indexqual,
                &mut paramids,
            )?;
        }
        NodeTag::T_BitmapHeapScan => {
            finalize_primnode_list(
                run,
                &plan.as_bitmap_heap_scan().unwrap().bitmapqualorig,
                &mut paramids,
            )?;
        }
        NodeTag::T_BitmapAnd => {
            for sub in &plan.as_bitmap_and().unwrap().bitmapplans {
                let child = finalize_plan(run, sub, &valid)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_BitmapOr => {
            for sub in &plan.as_bitmap_or().unwrap().bitmapplans {
                let child = finalize_plan(run, sub, &valid)?;
                paramids.add_members(mcx, &child)?;
            }
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            if let Some(off) = l.limitOffset {
                finalize_primnode(run, off, &mut paramids)?;
            }
            if let Some(cnt) = l.limitCount {
                finalize_primnode(run, cnt, &mut paramids)?;
            }
        }
        NodeTag::T_NestLoop => {
            let nl = plan.as_nest_loop().unwrap();
            debug_assert!(nl.nestParams.is_nil());
            finalize_primnode_list(run, &nl.join.joinqual, &mut paramids)?;
        }
        NodeTag::T_ModifyTable => {
            panic!("finalize_plan (subselect.c): ModifyTable with exec params; M2 DML lane")
        }
        other => panic!("finalize_plan (subselect.c): {other:?}; M2 plan lane"),
    }

    if let Some(child) = base.lefttree {
        let child_params = finalize_plan(run, child, &valid)?;
        paramids.add_members(mcx, &child_params)?;
    }
    if let Some(child) = base.righttree {
        let child_params = finalize_plan(run, child, &valid)?;
        paramids.add_members(mcx, &child_params)?;
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
    // SAFETY: the plan tree is exclusively owned by this planning invocation
    // (C writes the same fields in place).
    unsafe {
        plan.with_plan_mut(|p| {
            p.extParam = ext_param;
            p.allParam = all_param;
        })
    }
    .expect("plan node");
    Ok(paramids)
}

fn finalize_primnode_list<'mcx>(
    run: &PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    for node in list {
        finalize_primnode(run, node, paramids)?;
    }
    Ok(())
}

struct FinalizePrimnode<'a, 'mcx> {
    run: &'a PlannerRun<'mcx>,
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
        if node.node_tag() == NodeTag::T_SubPlan {
            panic!("finalize_primnode (subselect.c): in-expression SubPlan; M2 sublink lane");
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn finalize_primnode<'mcx>(
    run: &PlannerRun<'mcx>,
    node: Node<'mcx>,
    paramids: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
) -> PgResult<()> {
    FinalizePrimnode { run, paramids }.visit(node)?;
    Ok(())
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_in(mcx, s.as_bytes())?.leak();
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}
