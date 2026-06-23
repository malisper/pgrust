//! `backend/optimizer/prep/prepjointree.c` — FAMILY 1: `pull_up_sublinks`.
//!
//! 1:1 port of PostgreSQL 18.3 `pull_up_sublinks` and its
//! `pull_up_sublinks_jointree_recurse` / `pull_up_sublinks_qual_recurse`
//! helpers over this repo's lifetime-free owned `Query<'mcx>` + embedded-`PgBox`
//! jointree model.
//!
//! ## What this is
//!
//! `pull_up_sublinks` is called from `subquery_planner` (planner.c) to replace
//! top-level `ANY`/`EXISTS` SubLinks appearing in the query's WHERE / JOIN quals
//! with semijoins / antijoins, via the (now-re-keyed)
//! [`convert_ANY_sublink_to_join`] / [`convert_EXISTS_sublink_to_join`]
//! conversions owned by `backend-optimizer-plan-subselect-pullup`. This is the
//! [`pull_up_sublinks`](prepjointree_seams::pull_up_sublinks)
//! inward seam (declared by the pj-subqueries stage as seam-and-panic until the
//! SubLink owned-`Query` carrier keystone, #273, landed; this unit installs the
//! real body).
//!
//! ## Model notes
//!
//! * The C entry point is `pull_up_sublinks(PlannerInfo *root)`, walking and
//!   rebuilding `root->parse->jointree`. Here the top `Query` is resolved by the
//!   caller (`run.resolve_mut(root.parse)`) and threaded in as `&mut Query`
//!   alongside `&mut PlannerInfo`.
//! * The analyzed quals carry SubLinks as `Node::Expr(Expr::SubLink(..))` whose
//!   `subselect` is an **embedded owned** `Option<PgBox<Query>>` (mirroring
//!   `RangeTblEntry.subquery`; #273). The conversions walk it by deref. We never
//!   touch `subselect` here — we just hand the `&primnodes::SubLink` to the
//!   conversion seams.
//! * C's `Node **jtlink` (a mutable jointree-node slot that conversions splice
//!   new `JoinExpr`s onto) becomes `&mut Option<NodePtr<'mcx>>`: the slot owns
//!   the current top jointree node (a `FromExpr` initially, or a left/right join
//!   side). Splicing a new join `j` means `j.larg = take(*jtlink); *jtlink = j`,
//!   exactly as the C does. `jtlink2` (which may be C `NULL`) is
//!   `Option<&mut Option<NodePtr<'mcx>>>`.
//! * Relids are the lifetime-free [`Relids`](pathnodes::Relids) the
//!   conversion seams consume (a word-vector `Bitmapset`); the small set helpers
//!   below mirror `bms_make_singleton` / `bms_join` / `bms_add_member` over it.

use alloc::boxed::Box;

use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_error::PgResult;
use ::nodes::copy_query::Query;
use ::nodes::jointype::JoinType;
use ::nodes::nodes::{ntag, Node, NodePtr};
use ::nodes::primnodes::{Expr, SubLinkType};
use ::nodes::rawnodes::FromExpr;
use pathnodes::{Bitmapset, PlannerInfo, Relids};

use nodes_core::makefuncs::make_andclause;
use nodeFuncs_seams as nodefuncs;

use subselect_pullup_seams as subselect;

// ===========================================================================
// Relids helpers (the lifetime-free word-vector set the conversions consume).
// ===========================================================================

/// `bms_make_singleton(x)` over [`Relids`].
fn relids_make_singleton(x: i32) -> Relids {
    relids_add_member(None, x)
}

/// `bms_add_member(a, x)` over [`Relids`].
fn relids_add_member(a: Relids, x: i32) -> Relids {
    debug_assert!(x >= 0);
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset { words: alloc::vec::Vec::new() }));
    let wnum = (x / 64) as usize;
    let bit = (x % 64) as u32;
    if wnum >= bms.words.len() {
        bms.words.resize(wnum + 1, 0);
    }
    bms.words[wnum] |= 1u64 << bit;
    Some(bms)
}

/// `bms_join(a, b)` over [`Relids`] — recycle/union, returning the merged set.
fn relids_join(a: Relids, b: Relids) -> Relids {
    let mut a = match a {
        None => return b,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(a),
        Some(b) => b,
    };
    if a.words.len() < b.words.len() {
        // Make `a` the longer of the two so it can hold the union.
        let mut b = b;
        for i in 0..a.words.len() {
            b.words[i] |= a.words[i];
        }
        return Some(b);
    }
    for i in 0..b.words.len() {
        a.words[i] |= b.words[i];
    }
    Some(a)
}

/// `bms_union(a, b)` over [`Relids`] — a fresh copy of the union (the inputs are
/// borrowed and untouched, matching the C `bms_union` used in the JOIN_INNER arm
/// where both `leftrelids`/`rightrelids` survive for the `*relids = bms_join`).
fn relids_union(a: &Relids, b: &Relids) -> Relids {
    relids_join(relids_copy(a), relids_copy(b))
}

/// `bms_copy(a)` over [`Relids`].
fn relids_copy(a: &Relids) -> Relids {
    a.as_ref().map(|bms| Box::new((**bms).clone()))
}

// ===========================================================================
// pull_up_sublinks (prepjointree.c:443)
// ===========================================================================

/// `pull_up_sublinks(root)` (prepjointree.c:443).
pub fn pull_up_sublinks<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    // Begin recursion through the jointree.
    //
    // C casts `(Node *) root->parse->jointree` — the top is always a FromExpr.
    // We take it out (so the recursion owns it as a `Node::FromExpr`), walk it,
    // and put the result back.
    let jtnode_in: Option<NodePtr<'mcx>> = match parse.jointree.take() {
        Some(jt) => Some(alloc_in(mcx, Node::mk_from_expr(mcx, PgBox::into_inner(jt))?)?),
        None => None,
    };

    let mut relids: Relids = None;
    let jtnode = pull_up_sublinks_jointree_recurse(mcx, root, parse, jtnode_in, &mut relids)?;

    // root->parse->jointree must always be a FromExpr, so insert a dummy one if
    // we got a bare RangeTblRef or JoinExpr out of the recursion.
    match jtnode {
        Some(n) => {
            if n.node_tag() == ntag::T_FromExpr {
                let f = PgBox::into_inner(n).into_fromexpr().expect("FromExpr");
                parse.jointree = Some(alloc_in(mcx, f)?);
            } else {
                // makeFromExpr(list_make1(jtnode), NULL)
                let other = PgBox::into_inner(n);
                let mut fromlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                fromlist.push(alloc_in(mcx, other)?);
                parse.jointree = Some(alloc_in(
                    mcx,
                    FromExpr {
                        fromlist,
                        quals: None,
                    },
                )?);
            }
        }
        None => {
            // jtnode was NULL: makeFromExpr(list_make1(NULL), NULL) is not what
            // C does (it would never see a NULL top jointree), but a NULL top is
            // the empty-FROM case; an empty FromExpr is the faithful result.
            parse.jointree = Some(alloc_in(
                mcx,
                FromExpr {
                    fromlist: PgVec::new_in(mcx),
                    quals: None,
                },
            )?);
        }
    }

    Ok(())
}

// ===========================================================================
// pull_up_sublinks_jointree_recurse (prepjointree.c:469)
// ===========================================================================

/// `pull_up_sublinks_jointree_recurse(root, jtnode, relids)`
/// (prepjointree.c:469). `jtnode` is owned (C passes a `Node *` by value and
/// returns the possibly-rebuilt node); `relids` is the out-parameter.
fn pull_up_sublinks_jointree_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Option<NodePtr<'mcx>>,
    relids: &mut Relids,
) -> PgResult<Option<NodePtr<'mcx>>> {
    // Since this function recurses, it could be driven to stack overflow.
    postgres_seams::check_stack_depth::call()?;

    let jtnode = match jtnode {
        None => {
            *relids = None;
            return Ok(None);
        }
        Some(n) => n,
    };

    let jtnode_tag = jtnode.node_tag();
    match jtnode_tag {
        ntag::T_RangeTblRef => {
            let r = PgBox::into_inner(jtnode).into_rangetblref().expect("RangeTblRef");
            let varno = r.rtindex;
            *relids = relids_make_singleton(varno);
            // jtnode is returned unmodified.
            Ok(Some(alloc_in(mcx, Node::mk_range_tbl_ref(mcx, r)?)?))
        }
        ntag::T_FromExpr => {
            let f = PgBox::into_inner(jtnode).into_fromexpr().expect("FromExpr");
            let mut newfromlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            let mut frelids: Relids = None;

            // First, recurse to process children and collect their relids.
            let FromExpr { fromlist, quals } = f;
            for child in fromlist.into_iter() {
                let mut childrelids: Relids = None;
                let newchild = pull_up_sublinks_jointree_recurse(
                    mcx,
                    root,
                    parse,
                    Some(child),
                    &mut childrelids,
                )?;
                if let Some(nc) = newchild {
                    newfromlist.push(nc);
                }
                frelids = relids_join(frelids, childrelids);
            }

            // Build the replacement FromExpr; no quals yet. Set up a link
            // representing the rebuilt jointree.
            let newf = FromExpr {
                fromlist: newfromlist,
                quals: None,
            };
            let mut jtlink: Option<NodePtr<'mcx>> = Some(alloc_in(mcx, Node::mk_from_expr(mcx, newf)?)?);

            // Now process qual --- all children are available for use.
            let newquals = pull_up_sublinks_qual_recurse(
                mcx, root, parse, quals, &mut jtlink, &frelids, None, &None,
            )?;

            // Store the (possibly newly-folded) quals back into the FromExpr at
            // the base of the jtlink stack. The base is `jtlink` itself when no
            // join was spliced, else the bottom of the JoinExpr stack — but C
            // assigns `newf->quals`, and `newf` is the FromExpr that all spliced
            // joins were stacked *above* (their `larg` chain bottoms out at
            // newf). So the quals belong to the original `newf` node.
            attach_quals_to_fromexpr_base(&mut jtlink, newquals);

            // Although we could include the pulled-up subqueries in the returned
            // relids, there's no need since upper quals couldn't refer to their
            // outputs anyway.
            *relids = frelids;
            Ok(jtlink)
        }
        ntag::T_JoinExpr => {
            let mut j = PgBox::into_inner(jtnode).into_joinexpr().expect("JoinExpr");
            // (C makes a modifiable copy of the join node, but doesn't copy its
            // subnodes yet; here we already own `j`.)
            let mut leftrelids: Relids = None;
            let mut rightrelids: Relids = None;

            // Recurse to process children and collect their relids.
            j.larg = pull_up_sublinks_jointree_recurse(
                mcx,
                root,
                parse,
                j.larg.take(),
                &mut leftrelids,
            )?;
            j.rarg = pull_up_sublinks_jointree_recurse(
                mcx,
                root,
                parse,
                j.rarg.take(),
                &mut rightrelids,
            )?;

            // Now process qual, showing appropriate child relids as available,
            // and attach any pulled-up jointree items at the right place. We
            // don't expect to see any pre-existing JOIN_SEMI/ANTI/RIGHT_SEMI/
            // RIGHT_ANTI jointypes here.
            //
            // The C threads `&jtlink` (a `Node *` aliasing `j`) for the
            // INNER case, and `&j->rarg` / `&j->larg` for LEFT / RIGHT. To avoid
            // aliasing, we keep `j` as an owned local during the qual walk and
            // splice into its larg/rarg slots, then re-box it at the end.
            let rtindex = j.rtindex;
            match j.jointype {
                JoinType::JOIN_INNER => {
                    // C:
                    //   jtlink = (Node *) j;
                    //   j->quals = pull_up_sublinks_qual_recurse(root, j->quals,
                    //                  &jtlink, bms_union(leftrelids, rightrelids),
                    //                  NULL, NULL);
                    //   jtnode = jtlink;
                    //
                    // `jtlink` is a slot that initially aliases `j`. A pulled-up
                    // sublink stacks a new join ABOVE `j` by doing
                    // `new->larg = *jtlink; *jtlink = new`, so after the walk
                    // `jtlink` is either still `j` (no splice) or a stack of new
                    // JoinExprs whose `larg` chain bottoms out at `j`. The
                    // critical point: the folded quals (`newquals`) are assigned
                    // to `j` ITSELF (the *original* inner join), NOT to the
                    // bottom of `j`'s own `larg` subtree — which in a nested
                    // (3+-way) join is another JoinExpr that already carries its
                    // own ON-quals. Attaching there scrambles ON-quals across
                    // join levels (the create_index `\d` regression).
                    //
                    // To assign to `j` faithfully without searching for it, we
                    // keep `j` owned and thread the splices through a separate
                    // `stack` slot that holds only the joins stacked ABOVE `j`
                    // (its `larg` chain bottoms out at a `None` placeholder for
                    // `j`). After the walk we set `j.quals = newquals`, then
                    // re-link the bottom of the stack to `j`.
                    let avail = relids_union(&leftrelids, &rightrelids);
                    let quals = j.quals.take();
                    let mut stack: Option<NodePtr<'mcx>> = None;
                    let newquals = pull_up_sublinks_qual_recurse(
                        mcx, root, parse, quals, &mut stack, &avail, None, &None,
                    )?;
                    j.quals = newquals;
                    let j_node = alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?;
                    let result = match stack {
                        // No splice: `jtlink` is still `j`.
                        None => Some(j_node),
                        // Splices occurred: the bottom of the spliced `larg`
                        // chain is the `None` placeholder; re-link it to `j`.
                        Some(mut top) => {
                            link_stack_bottom_to(&mut top, j_node);
                            Some(top)
                        }
                    };
                    *relids = relids_join(leftrelids, rightrelids);
                    if rtindex != 0 {
                        *relids = relids_add_member(relids.take(), rtindex);
                    }
                    Ok(result)
                }
                JoinType::JOIN_LEFT => {
                    let quals = j.quals.take();
                    let newquals = pull_up_sublinks_qual_recurse(
                        mcx,
                        root,
                        parse,
                        quals,
                        &mut j.rarg,
                        &rightrelids,
                        None,
                        &None,
                    )?;
                    j.quals = newquals;
                    *relids = relids_join(leftrelids, rightrelids);
                    if rtindex != 0 {
                        *relids = relids_add_member(relids.take(), rtindex);
                    }
                    Ok(Some(alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?))
                }
                JoinType::JOIN_FULL => {
                    // can't do anything with full-join quals
                    *relids = relids_join(leftrelids, rightrelids);
                    if rtindex != 0 {
                        *relids = relids_add_member(relids.take(), rtindex);
                    }
                    Ok(Some(alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?))
                }
                JoinType::JOIN_RIGHT => {
                    let quals = j.quals.take();
                    let newquals = pull_up_sublinks_qual_recurse(
                        mcx,
                        root,
                        parse,
                        quals,
                        &mut j.larg,
                        &leftrelids,
                        None,
                        &None,
                    )?;
                    j.quals = newquals;
                    *relids = relids_join(leftrelids, rightrelids);
                    if rtindex != 0 {
                        *relids = relids_add_member(relids.take(), rtindex);
                    }
                    Ok(Some(alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?))
                }
                _ => Err(types_error::PgError::error("unrecognized join type")),
            }
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// Re-link the bottom of a stack of spliced JoinExprs to `bottom`.
///
/// When sublinks are pulled up in the JOIN_INNER case, the new JoinExprs are
/// stacked onto a fresh `stack` slot (initially `None`). Each splice sets the
/// new join's `larg` to the previous slot contents, so the `larg` chain bottoms
/// out at a `None` placeholder standing in for the original inner join `j`. This
/// walks down the `larg` chain to that bottom-most `None` slot and stores `j`
/// there, completing the C `jtnode = jtlink` with `j` at the base. (Mirrors C,
/// where `j` is aliased through `jtlink` from the start and its `larg` subtree is
/// never disturbed — so this never descends into `j`'s own left child.)
fn link_stack_bottom_to<'mcx>(top: &mut Node<'mcx>, bottom: NodePtr<'mcx>) {
    let mut cur = top;
    loop {
        // `cur` is always one of the freshly-spliced JoinExprs.
        let j = cur
            .as_joinexpr_mut()
            .expect("spliced stack node must be a JoinExpr");
        if j.larg.is_none() {
            j.larg = Some(bottom);
            return;
        }
        cur = j.larg.as_deref_mut().unwrap();
    }
}

/// Re-attach the folded quals onto the `FromExpr` at the base of the jtlink
/// stack (C: `newf->quals = ...`). Spliced joins were stacked above `newf` via
/// their `larg` chain, so descend `larg` to the bottom-most node, which is the
/// FromExpr.
fn attach_quals_to_fromexpr_base<'mcx>(
    jtlink: &mut Option<NodePtr<'mcx>>,
    newquals: Option<NodePtr<'mcx>>,
) {
    let base = find_bottom_fromexpr(jtlink.as_deref_mut());
    if let Some(f) = base.and_then(|n| n.as_fromexpr_mut()) {
        f.quals = newquals;
    }
}

/// Descend the `larg` chain to the bottom-most node and return it if it is the
/// `FromExpr` base.
fn find_bottom_fromexpr<'a, 'mcx>(
    start: Option<&'a mut Node<'mcx>>,
) -> Option<&'a mut Node<'mcx>> {
    let node = start?;
    let descend = node.is_joinexpr();
    if descend {
        if let Some(j) = node.as_joinexpr_mut() {
            return find_bottom_fromexpr(j.larg.as_deref_mut());
        }
        None
    } else {
        // Should be the FromExpr base.
        Some(node)
    }
}

// ===========================================================================
// pull_up_sublinks_qual_recurse (prepjointree.c:556)
// ===========================================================================

/// `pull_up_sublinks_qual_recurse(root, node, jtlink1, available_rels1,
/// jtlink2, available_rels2)` (prepjointree.c:556). `node` is the owned qual
/// subtree; the result is the rewritten qual (NULL = constant TRUE). `jtlink1`
/// (and the optional `jtlink2`) are the mutable jointree-node slots that
/// converted joins are spliced onto.
#[allow(clippy::too_many_arguments)]
fn pull_up_sublinks_qual_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    node: Option<NodePtr<'mcx>>,
    jtlink1: &mut Option<NodePtr<'mcx>>,
    available_rels1: &Relids,
    mut jtlink2: Option<&mut Option<NodePtr<'mcx>>>,
    available_rels2: &Relids,
) -> PgResult<Option<NodePtr<'mcx>>> {
    let node = match node {
        None => return Ok(None),
        Some(n) => n,
    };

    // IsA(node, SubLink)? — in the analyzed tree, a SubLink qual is
    // Node::Expr(Expr::SubLink(..)).
    if let Some(Expr::SubLink(sublink)) = node.as_expr() {
        // Is it a convertible ANY or EXISTS clause?
        if sublink.subLinkType == SubLinkType::Any {
            // Try the VALUES-sequence simplification first. (C extracts
            // testexpr/values from the SubLink; here the owner derefs the
            // embedded-owned subselect itself, so we hand it the whole SubLink.)
            if let Some(saop) = subselect::convert_VALUES_to_ANY::call(mcx, root, sublink)? {
                // The VALUES sequence was simplified. Nothing more to do here.
                return Ok(Some(alloc_in(mcx, Node::mk_expr(mcx, saop)?)?));
            }

            if let Some(j) =
                subselect::convert_ANY_sublink_to_join::call(mcx, root, parse, sublink, available_rels1)?
            {
                return splice_join_and_recurse(
                    mcx, root, parse, j, jtlink1, available_rels1, None,
                )
                .map(|_| None);
            }
            if available_rels2.is_some() {
                if let Some(j) = subselect::convert_ANY_sublink_to_join::call(
                    mcx, root, parse, sublink, available_rels2,
                )? {
                    let link2 = jtlink2
                        .as_deref_mut()
                        .expect("available_rels2 non-NULL implies jtlink2 non-NULL");
                    return splice_join_and_recurse(
                        mcx, root, parse, j, link2, available_rels2, None,
                    )
                    .map(|_| None);
                }
            }
        } else if sublink.subLinkType == SubLinkType::Exists {
            if let Some(j) = subselect::convert_EXISTS_sublink_to_join::call(
                mcx, root, parse, sublink, false, available_rels1,
            )? {
                return splice_join_and_recurse(
                    mcx, root, parse, j, jtlink1, available_rels1, None,
                )
                .map(|_| None);
            }
            if available_rels2.is_some() {
                if let Some(j) = subselect::convert_EXISTS_sublink_to_join::call(
                    mcx, root, parse, sublink, false, available_rels2,
                )? {
                    let link2 = jtlink2
                        .as_deref_mut()
                        .expect("available_rels2 non-NULL implies jtlink2 non-NULL");
                    return splice_join_and_recurse(
                        mcx, root, parse, j, link2, available_rels2, None,
                    )
                    .map(|_| None);
                }
            }
        }
        // Else return it unmodified.
        return Ok(Some(node));
    }

    // is_notclause(node)?
    if let Some(e) = node.as_expr() {
        if nodefuncs::is_notclause::call(e) {
            // If the immediate argument of NOT is EXISTS, try to convert.
            let arg = nodefuncs::get_notclausearg::call(e);
            if let Expr::SubLink(sublink) = arg {
                if sublink.subLinkType == SubLinkType::Exists {
                    if let Some(j) = subselect::convert_EXISTS_sublink_to_join::call(
                        mcx, root, parse, sublink, true, available_rels1,
                    )? {
                        // Because we are underneath a NOT, we can't pull up
                        // sublinks that reference the left-hand stuff, but it's
                        // still okay to pull up sublinks referencing j->rarg.
                        // C: pull_up_sublinks_qual_recurse(..., &j->rarg,
                        // child_rels, NULL, NULL).
                        return splice_join_and_recurse_under_not(
                            mcx,
                            root,
                            parse,
                            j,
                            jtlink1,
                        )
                        .map(|_| None);
                    }
                    if available_rels2.is_some() {
                        if let Some(j) = subselect::convert_EXISTS_sublink_to_join::call(
                            mcx, root, parse, sublink, true, available_rels2,
                        )? {
                            let link2 = jtlink2
                                .as_deref_mut()
                                .expect("available_rels2 non-NULL implies jtlink2 non-NULL");
                            return splice_join_and_recurse_under_not(
                                mcx, root, parse, j, link2,
                            )
                            .map(|_| None);
                        }
                    }
                }
            }
            // Else return it unmodified.
            return Ok(Some(node));
        }
    }

    // is_andclause(node)?
    let is_and = matches!(
        node.as_expr(),
        Some(Expr::BoolExpr(b)) if b.boolop == ::nodes::primnodes::BoolExprType::AND_EXPR
    );
    if is_and {
        // Recurse into AND clause.
        let args = match PgBox::into_inner(node).into_expr() {
            Some(Expr::BoolExpr(b)) => b.args,
            _ => unreachable!(),
        };
        let mut newclauses: alloc::vec::Vec<Expr> = alloc::vec::Vec::new();
        for oldclause in args.into_iter() {
            let old_node = alloc_in(mcx, Node::mk_expr(mcx, oldclause)?)?;
            let newclause = pull_up_sublinks_qual_recurse(
                mcx,
                root,
                parse,
                Some(old_node),
                jtlink1,
                available_rels1,
                jtlink2.as_deref_mut(),
                available_rels2,
            )?;
            if let Some(nc) = newclause {
                // The walk hands back a Node; an AND arg is always an Expr.
                match PgBox::into_inner(nc).into_expr() {
                    Some(e) => newclauses.push(e),
                    None => {
                        // Shouldn't happen (a qual subtree is an expression);
                        // keep it faithful by re-wrapping is impossible since
                        // newclauses is Vec<Expr>. Treat any non-Expr as a hard
                        // error mirroring C's type expectations.
                        return Err(types_error::PgError::error(
                            "pull_up_sublinks: non-expression AND clause",
                        ));
                    }
                }
            }
        }
        // We might have got back fewer clauses than we started with.
        if newclauses.is_empty() {
            return Ok(None);
        } else if newclauses.len() == 1 {
            let only = newclauses.into_iter().next().unwrap();
            return Ok(Some(alloc_in(mcx, Node::mk_expr(mcx, only)?)?));
        } else {
            return Ok(Some(alloc_in(mcx, Node::mk_expr(mcx, make_andclause(newclauses))?)?));
        }
    }

    // Stop if not an AND.
    Ok(Some(node))
}

/// Common tail for a successful `convert_*_sublink_to_join`: splice the new join
/// `j` onto `*jtlink`, then recursively process the pulled-up jointree
/// (`j->rarg`) and quals. Returns `Ok(())`; the caller returns NULL (constant
/// TRUE) for the qual.
///
/// C:
/// ```text
/// j->larg = *jtlink;
/// *jtlink = (Node *) j;
/// j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);
/// j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->larg,
///                                          available_rels, &j->rarg, child_rels);
/// ```
#[allow(clippy::too_many_arguments)]
fn splice_join_and_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    mut j: ::nodes::rawnodes::JoinExpr<'mcx>,
    jtlink: &mut Option<NodePtr<'mcx>>,
    available_rels: &Relids,
    _unused: Option<()>,
) -> PgResult<()> {
    // j->larg = *jtlink;
    j.larg = jtlink.take();

    // Recursively process pulled-up jointree nodes (j->rarg).
    let mut child_rels: Relids = None;
    j.rarg = pull_up_sublinks_jointree_recurse(mcx, root, parse, j.rarg.take(), &mut child_rels)?;

    // Now recursively process the pulled-up quals. Any inserted joins can get
    // stacked onto either j->larg or j->rarg, depending on which rels they
    // reference. We keep `j` owned during the walk and splice into its own
    // larg/rarg slots.
    let quals = j.quals.take();
    let mut larg = j.larg.take();
    let mut rarg = j.rarg.take();
    let newquals = pull_up_sublinks_qual_recurse(
        mcx,
        root,
        parse,
        quals,
        &mut larg,
        available_rels,
        Some(&mut rarg),
        &child_rels,
    )?;
    j.larg = larg;
    j.rarg = rarg;
    j.quals = newquals;

    // *jtlink = (Node *) j;
    *jtlink = Some(alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?);
    Ok(())
}

/// The under-NOT variant: only `j->rarg` is available for further pull-up
/// (jtlink2 is C `NULL`).
///
/// C:
/// ```text
/// j->larg = *jtlink;
/// *jtlink = (Node *) j;
/// j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);
/// j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg,
///                                          child_rels, NULL, NULL);
/// ```
fn splice_join_and_recurse_under_not<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    mut j: ::nodes::rawnodes::JoinExpr<'mcx>,
    jtlink: &mut Option<NodePtr<'mcx>>,
) -> PgResult<()> {
    j.larg = jtlink.take();

    let mut child_rels: Relids = None;
    j.rarg = pull_up_sublinks_jointree_recurse(mcx, root, parse, j.rarg.take(), &mut child_rels)?;

    let quals = j.quals.take();
    let mut rarg = j.rarg.take();
    let newquals = pull_up_sublinks_qual_recurse(
        mcx, root, parse, quals, &mut rarg, &child_rels, None, &None,
    )?;
    j.rarg = rarg;
    j.quals = newquals;

    *jtlink = Some(alloc_in(mcx, Node::mk_join_expr(mcx, j)?)?);
    Ok(())
}
