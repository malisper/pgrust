//! JOIN TREE PROCESSING (initsplan.c) — `deconstruct_jointree`,
//! `deconstruct_recurse`, `deconstruct_distribute`,
//! `process_security_barrier_quals`, `mark_rels_nulled_by_join`,
//! `deconstruct_distribute_oj_quals`, `distribute_quals_to_rels`.
//!
//! # Model reconciliation (read before editing)
//!
//! C `JoinTreeItem.jtnode` is a `Node *` into the parse jointree, read (never
//! mutated) by the distribute passes. This repo's `PlannerInfo.parse` is the
//! opaque [`QueryId`]; the jointree is reached through the established
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) resolver
//! (`run.jointree(root.parse) -> &FromExpr<'mcx>`, the safe-Rust `Query *`
//! deref landed by task #264). So every public entry here takes an additional
//! `run: &PlannerRun<'mcx>` parameter alongside `&mut PlannerInfo`, exactly as
//! the prep layer does. NO new jointree seam was added — the carrier exists.
//!
//! The transient `JoinTreeItem` arena is built by `deconstruct_recurse` walking
//! the *borrowed* jointree `Node<'mcx>` tree (no clone of the tree is needed: we
//! only read `rtindex`/`jointype` and clone the qual conjuncts). Qual nodes are
//! analyzed expressions: a `Node::Expr(Expr)`, so each `FromExpr`/`JoinExpr`
//! `quals` field (`Option<NodePtr>`) is resolved to `Option<Expr>` and split
//! into its implicit-AND conjunct list via `make_ands_implicit`. The conjunct
//! lists, postponed lateral clauses, and postponed OJ clauses are therefore all
//! `Vec<Expr>` (lifetime-free — [`Expr`] carries no `'mcx`).
//!
//! # lib.rs MUST provide (see the agent report) — items this file imports as
//! `crate::…`:
//! - `pub type JtId = usize;`
//! - `pub struct JoinTreeItem` (lifetime-free) with public fields:
//!     - `kind: JtNodeKind`
//!     - `post_order: usize`
//!     - `jdomain: usize`
//!     - `jti_parent: Option<JtId>`
//!     - `qualscope: Relids`
//!     - `inner_join_rels: Relids`
//!     - `left_rels: Relids`
//!     - `right_rels: Relids`
//!     - `nonnullable_rels: Relids`
//!     - `sjinfo: Option<alloc::boxed::Box<types_pathnodes::SpecialJoinInfo>>`
//!     - `oj_joinclauses: alloc::vec::Vec<types_nodes::primnodes::Expr>`
//!     - `lateral_clauses: alloc::vec::Vec<types_nodes::primnodes::Expr>`
//!   plus `#[derive(Clone, Debug, Default)]`.
//! - `pub enum JtNodeKind` (lifetime-free, `#[derive(Clone, Debug)]` + `Default`):
//!     - `RangeTblRef { rtindex: i32 }`
//!     - `FromExpr { quals: alloc::vec::Vec<types_nodes::primnodes::Expr> }`
//!     - `JoinExpr { jointype: types_pathnodes::JoinType, rtindex: i32,
//!                   quals: alloc::vec::Vec<types_nodes::primnodes::Expr> }`
//! - consts/helpers: `from_collapse_limit() -> i32`, `join_collapse_limit() -> i32`.
//! - `pub fn new_join_domain(root: &mut types_pathnodes::PlannerInfo) -> usize`
//!   is provided HERE (below); lib.rs need not supply it.
//! - `crate::quals::{distribute_qual_to_rels, distribute_restrictinfo_to_rels}`
//!   and `crate::outerjoin::{make_outerjoininfo, compute_semijoin_info}` are
//!   sibling modules written by other agents — signatures assumed below.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    JoinDomain, JoinlistNode, PlannerInfo, Relids, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL,
    JOIN_INNER, JOIN_LEFT, JOIN_SEMI,
};
use types_nodes::nodes::Node;

use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

use backend_nodes_core::makefuncs::make_ands_implicit;

use crate::{from_collapse_limit, join_collapse_limit, JoinTreeItem, JtId, JtNodeKind};

/// Resolve a jointree node's `quals` field (`Option<NodePtr>`, an analyzed
/// `Node::Expr`) into its implicit-AND conjunct list, mirroring the C view of
/// `(List *) f->quals` / `(List *) j->quals` as an already-imploded AND-list.
///
/// `make_ands_implicit(NULL) -> NIL`, an `AND` BoolExpr -> its args, a constant
/// TRUE -> NIL, anything else -> a one-element list — exactly the C semantics.
fn quals_implicit_and(mcx: mcx::Mcx<'_>, quals: Option<&Node>) -> Vec<Expr> {
    // C views `(List *) f->quals` by pointer; the owned model stores the qual
    // conjuncts as owned `Expr` values, so deep-copy out. The derived
    // `Expr::clone()` panics on owned-subtree variants (SubLink/SubPlan/Aggref)
    // whose children only deep-copy via `clone_in` (`copyObject` shape); route
    // the copy through `Expr::clone_in`.
    //
    // The clone MUST land in the long-lived planner arena (`run.mcx()`), NOT a
    // throwaway `MemoryContext`. `Expr` is lifetime-free: `clone_in` erases the
    // arena lifetime to `'static`, but the cloned node's `PgBox`/`PgVec`
    // children still point into the arena it was allocated in and are
    // deallocated against that arena when the node is dropped. The resulting
    // `Expr` moves into `JoinTreeItem.quals` and is dropped much later (when
    // `deconstruct_jointree` drops `item_list`); cloning into a local context
    // that is freed on return leaves those child pointers dangling, so the
    // eventual drop frees against an already-freed context (use-after-free /
    // segfault). The planner arena outlives the whole planner run, satisfying
    // the `clone_in` `'static`-erasure invariant.
    // By this planner stage, `f->quals` / `j->quals` is a `Node *` that is held
    // in one of two equivalent shapes in this owned model:
    //
    //   * `Node::Expr(e)` — a single qual expression (the common case, e.g. a
    //     plain WHERE clause `a = 5` or an AND-`BoolExpr`); or
    //   * `Node::List([...])` — an already-imploded implicit-AND *list* of
    //     conjunct `Expr`s. This shape is produced by `concat_quals` in
    //     `remove_useless_result_rtes` (prepjointree FAMILY 5) when it merges an
    //     elided single-child FromExpr's quals up into its parent via the C
    //     `list_concat(child, parent)` — e.g. a view body with a WHERE clause
    //     over an OUTER join, whose FromExpr is elided and whose quals migrate to
    //     the parent FromExpr as a `List`.
    //
    // In C `f->quals` is just cast to `(List *)` and either iterated directly or
    // fed through `make_ands_implicit`; both shapes are an implicit-AND list.
    // Mirror that here: a `Node::List` is *already* the conjunct list, so deep-
    // copy each element out — but each element is itself a qual `Node` that may
    // be wrapped as a single AND-`BoolExpr` (e.g. a view body's `WHERE a AND b`
    // that was analyzed into one `BoolExpr(AND)` and then wrapped as a one-
    // element list by `concat_quals`). `distribute_qual_to_rels` asserts every
    // clause it receives is *not* an and-clause, so run each list element back
    // through `make_ands_implicit` to split any top-level `AND` into its args /
    // drop a constant-TRUE / keep an atomic clause, and concatenate the results.
    // A bare single `Node::Expr` runs through `make_ands_implicit` directly.
    //
    // The clone MUST land in the long-lived planner arena (`run.mcx()`), NOT a
    // throwaway `MemoryContext`. `Expr` is lifetime-free: `clone_in` erases the
    // arena lifetime to `'static`, but the cloned node's `PgBox`/`PgVec`
    // children still point into the arena it was allocated in and are
    // deallocated against that arena when the node is dropped. The resulting
    // `Expr` moves into `JoinTreeItem.quals` and is dropped much later (when
    // `deconstruct_jointree` drops `item_list`); cloning into a local context
    // that is freed on return leaves those child pointers dangling, so the
    // eventual drop frees against an already-freed context (use-after-free /
    // segfault). The planner arena outlives the whole planner run, satisfying
    // the `clone_in` `'static`-erasure invariant.
    match quals {
        None => Vec::new(),
        Some(n) if n.as_list().is_some() => {
            // Already an implicit-AND conjunct list; flatten each element through
            // `make_ands_implicit` so no element is left as a top-level AND-clause.
            let items = n.as_list().unwrap();
            let mut out: Vec<Expr> = Vec::with_capacity(items.len());
            for it in items.iter() {
                let e = it.as_expr().unwrap_or_else(|| {
                    panic!(
                        "quals_implicit_and: jointree quals List element is not an Expr: {:?}",
                        it.node_tag()
                    )
                });
                let cloned = e
                    .clone_in(mcx)
                    .unwrap_or_else(|err| panic!("quals_implicit_and: clone_in: {err:?}"));
                out.extend(make_ands_implicit(Some(cloned)));
            }
            out
        }
        Some(n) => {
            let clause: Option<Expr> = n.as_expr().map(|e| {
                e.clone_in(mcx)
                    .unwrap_or_else(|err| panic!("quals_implicit_and: clone_in: {err:?}"))
            });
            make_ands_implicit(clause)
        }
    }
}

/// `deconstruct_jointree` (initsplan.c:1084).
///
/// Recursively scan the join tree for WHERE and JOIN/ON qual clauses and add
/// them to the appropriate restrictinfo/joininfo lists, add `SpecialJoinInfo`
/// nodes for outer joins, and return the "joinlist" structure describing the
/// join-order decisions for `make_one_rel`.
pub fn deconstruct_jointree(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
) -> types_error::PgResult<Vec<JoinlistNode>> {
    // After this point, no more PlaceHolderInfos may be made, because
    // make_outerjoininfo requires all active placeholders to be present in
    // root->placeholder_list while we crawl up the join tree.
    root.placeholdersFrozen = true;

    // Fetch the already-created top-level join domain; reset its relids
    // (filled during deconstruct_recurse). top_jdomain = linitial(join_domains).
    debug_assert!(!root.join_domains.is_empty());
    root.join_domains[0].jd_relids = None;

    // Top of jointree must be a FromExpr (parse->jointree). It is reachable
    // only as a bare `&FromExpr` (not a `NodePtr`), so we dispatch into the
    // FromExpr-specialized entry below rather than `deconstruct_recurse`.
    debug_assert!(run.jointree(root.parse).is_some());

    // These are filled as we scan the jointree.
    root.all_baserels = None;
    root.outer_join_rels = None;

    let mut item_list: Vec<JoinTreeItem> = Vec::new();
    let mut post_counter: usize = 0;

    // Perform the initial scan of the jointree, starting at the top FromExpr.
    let f = run
        .jointree(root.parse)
        .expect("query has no jointree (top FromExpr)");
    let (result, _top_idx) = deconstruct_recurse_fromexpr(
        root,
        run,
        f,
        0, // top_jdomain index
        None,
        &mut item_list,
        &mut post_counter,
    );

    // Now we can form the value of all_query_rels, too.
    root.all_query_rels = bms::relids_union::call(&root.all_baserels, &root.outer_join_rels);
    // ... which should match what we computed for the top join domain.
    debug_assert!(bms::relids_equal::call(
        &root.all_query_rels,
        &root.join_domains[0].jd_relids
    ));

    // Now scan all the jointree nodes again, and distribute quals — in C's
    // depth-first (post-order, bottom-up) order.
    let order = post_order_indices(&item_list);
    for &jti in &order {
        deconstruct_distribute(root, run, &mut item_list, jti)?;
    }

    // If there were any special joins then we may have some postponed LEFT JOIN
    // clauses to deal with.
    if !root.join_info_list.is_empty() {
        for &jti in &order {
            if !item_list[jti].oj_joinclauses.is_empty() {
                deconstruct_distribute_oj_quals(root, run, &mut item_list, jti)?;
            }
        }
    }

    // Don't need the JoinTreeItems any more (item_list dropped here; its Relids
    // substructure is shared with RestrictInfo/SpecialJoinInfo nodes and lives
    // on independently — list_free_deep over the cells, leaving relids intact).
    Ok(result)
}

/// Indices of `item_list` sorted by `post_order` — C's bottom-up
/// `deconstruct_distribute` iteration order (`foreach(lc, item_list)` over the
/// depth-first-append list).
fn post_order_indices(item_list: &[JoinTreeItem]) -> Vec<JtId> {
    let mut order: Vec<JtId> = (0..item_list.len()).collect();
    order.sort_by_key(|&i| item_list[i].post_order);
    order
}

/// Allocate a new `JoinDomain` (empty relids) in `root.join_domains` and return
/// its index — `makeNode(JoinDomain)` + `lappend(root->join_domains, …)`.
pub fn new_join_domain(root: &mut PlannerInfo) -> usize {
    root.join_domains.push(JoinDomain { jd_relids: None });
    root.join_domains.len() - 1
}

/// `deconstruct_recurse` (initsplan.c:1166), entry specialized for the top
/// FromExpr (the only jointree node reachable as a bare `&FromExpr` rather than
/// through a `NodePtr`). Behaviourally identical to the `IsA(jtnode, FromExpr)`
/// arm of `deconstruct_recurse`.
fn deconstruct_recurse_fromexpr(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    f: &types_nodes::rawnodes::FromExpr<'_>,
    parent_domain: usize,
    parent_jtitem: Option<JtId>,
    item_list: &mut Vec<JoinTreeItem>,
    post_counter: &mut usize,
) -> (Vec<JoinlistNode>, JtId) {
    // Reserve this node's arena slot now (pre-order) so children can name us as
    // jti_parent; we overwrite it once our fields are computed.
    let my_index = item_list.len();
    item_list.push(JoinTreeItem {
        jti_parent: parent_jtitem,
        ..Default::default()
    });

    let mut jtitem = JoinTreeItem {
        jti_parent: parent_jtitem,
        ..Default::default()
    };
    jtitem.kind = JtNodeKind::FromExpr {
        quals: quals_implicit_and(run.mcx(), f.quals.as_deref()),
    };
    // This node belongs to parent_domain, as do its children.
    jtitem.jdomain = parent_domain;
    jtitem.qualscope = None;
    jtitem.inner_join_rels = None;

    let mut joinlist: Vec<JoinlistNode> = Vec::new();
    let mut remaining = f.fromlist.len() as i32;
    for l in f.fromlist.iter() {
        let (sub_joinlist, sub_idx) = deconstruct_recurse(
            root,
            run,
            l,
            parent_domain,
            Some(my_index),
            item_list,
            post_counter,
        );
        let sub_qs = bms::relids_copy::call(&item_list[sub_idx].qualscope);
        jtitem.qualscope = bms::relids_add_members::call(jtitem.qualscope.take(), &sub_qs);
        jtitem.inner_join_rels = bms::relids_copy::call(&item_list[sub_idx].inner_join_rels);
        let sub_members = sub_joinlist.len() as i32;
        remaining -= 1;
        if sub_members <= 1
            || joinlist.len() as i32 + sub_members + remaining <= from_collapse_limit()
        {
            joinlist.extend(sub_joinlist);
        } else {
            joinlist.push(JoinlistNode::Sub(sub_joinlist));
        }
    }
    // A FROM with more than one list element is an inner join subsuming all
    // below it: report inner_join_rels = qualscope. With exactly one element we
    // keep its inner_join_rels (already set). Zero elements: stays NULL.
    if f.fromlist.len() > 1 {
        jtitem.inner_join_rels = bms::relids_copy::call(&jtitem.qualscope);
    }

    jtitem.post_order = *post_counter;
    *post_counter += 1;
    jtitem.jti_parent = parent_jtitem;
    item_list[my_index] = jtitem;
    (joinlist, my_index)
}

/// `deconstruct_recurse` (initsplan.c:1166).
///
/// One recursion level of the initial jointree scan. Adds a [`JoinTreeItem`] for
/// each jointree node (depth-first) to `item_list` and returns the joinlist for
/// this node. `jtnode` is read but never mutated (a borrow into the resolved
/// jointree tree).
fn deconstruct_recurse(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    jtnode: &Node,
    parent_domain: usize,
    parent_jtitem: Option<JtId>,
    item_list: &mut Vec<JoinTreeItem>,
    post_counter: &mut usize,
) -> (Vec<JoinlistNode>, JtId) {
    // Reserve this node's arena slot now (pre-order) so children can name us as
    // jti_parent; overwrite once fields are computed. The distribute passes run
    // in post-order (the rank assigned at the tail), matching C's bottom-up
    // item_list ordering.
    let my_index = item_list.len();
    item_list.push(JoinTreeItem {
        jti_parent: parent_jtitem,
        ..Default::default()
    });

    let mut jtitem = JoinTreeItem {
        jti_parent: parent_jtitem,
        ..Default::default()
    };
    let joinlist: Vec<JoinlistNode>;

    if let Some(rtr) = jtnode.as_rangetblref() {
        let varno = rtr.rtindex;
        jtitem.kind = JtNodeKind::RangeTblRef { rtindex: varno };
        // Fill all_baserels as we encounter baserel jointree nodes.
        root.all_baserels = bms::relids_add_member::call(root.all_baserels.take(), varno);
        // This node belongs to parent_domain.
        jtitem.jdomain = parent_domain;
        root.join_domains[parent_domain].jd_relids =
            bms::relids_add_member::call(root.join_domains[parent_domain].jd_relids.take(), varno);
        // qualscope is just the one RTE.
        jtitem.qualscope = bms::relids_make_singleton::call(varno);
        // A single baserel does not create an inner join.
        jtitem.inner_join_rels = None;
        joinlist = alloc::vec![JoinlistNode::Rel(varno)];
    } else if let Some(f) = jtnode.as_fromexpr() {
        jtitem.kind = JtNodeKind::FromExpr {
            quals: quals_implicit_and(run.mcx(), f.quals.as_deref()),
        };
        // This node belongs to parent_domain, as do its children.
        jtitem.jdomain = parent_domain;
        jtitem.qualscope = None;
        jtitem.inner_join_rels = None;
        let mut jl: Vec<JoinlistNode> = Vec::new();
        let mut remaining = f.fromlist.len() as i32;
        for l in f.fromlist.iter() {
            let (sub_joinlist, sub_idx) = deconstruct_recurse(
                root,
                run,
                l,
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            let sub_qs = bms::relids_copy::call(&item_list[sub_idx].qualscope);
            jtitem.qualscope = bms::relids_add_members::call(jtitem.qualscope.take(), &sub_qs);
            jtitem.inner_join_rels = bms::relids_copy::call(&item_list[sub_idx].inner_join_rels);
            let sub_members = sub_joinlist.len() as i32;
            remaining -= 1;
            if sub_members <= 1
                || jl.len() as i32 + sub_members + remaining <= from_collapse_limit()
            {
                jl.extend(sub_joinlist);
            } else {
                jl.push(JoinlistNode::Sub(sub_joinlist));
            }
        }
        if f.fromlist.len() > 1 {
            jtitem.inner_join_rels = bms::relids_copy::call(&jtitem.qualscope);
        }
        joinlist = jl;
    } else if let Some(j) = jtnode.as_joinexpr() {
        let leftjoinlist: Vec<JoinlistNode>;
        let rightjoinlist: Vec<JoinlistNode>;
        jtitem.kind = JtNodeKind::JoinExpr {
            // C `JoinType` discriminant — store the lifetime-free
            // `types_pathnodes::JoinType` (u32) the planner uses, converted from
            // the `types_nodes::jointype::JoinType` enum the raw node carries.
            jointype: j.jointype as types_pathnodes::JoinType,
            rtindex: j.rtindex,
            quals: quals_implicit_and(run.mcx(), j.quals.as_deref()),
        };
        let jointype = j.jointype as types_pathnodes::JoinType;
        if jointype == JOIN_INNER {
            // This node belongs to parent_domain, as do its children.
            jtitem.jdomain = parent_domain;
            let (ljl, left_item) = deconstruct_recurse(
                root,
                run,
                j.larg.as_deref().expect("JOIN larg"),
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            leftjoinlist = ljl;
            let left_qs = bms::relids_copy::call(&item_list[left_item].qualscope);
            let (rjl, right_item) = deconstruct_recurse(
                root,
                run,
                j.rarg.as_deref().expect("JOIN rarg"),
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            rightjoinlist = rjl;
            let right_qs = bms::relids_copy::call(&item_list[right_item].qualscope);
            // Compute qualscope etc.
            jtitem.qualscope = bms::relids_union::call(&left_qs, &right_qs);
            jtitem.inner_join_rels = bms::relids_copy::call(&jtitem.qualscope);
            jtitem.left_rels = left_qs;
            jtitem.right_rels = right_qs;
            // Inner join adds no restrictions for quals.
            jtitem.nonnullable_rels = None;
        } else if jointype == JOIN_LEFT || jointype == JOIN_ANTI {
            // Make new join domain for my quals and the RHS.
            let child_domain = new_join_domain(root);
            jtitem.jdomain = child_domain;
            let (ljl, left_item) = deconstruct_recurse(
                root,
                run,
                j.larg.as_deref().expect("JOIN larg"),
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            leftjoinlist = ljl;
            let left_qs = bms::relids_copy::call(&item_list[left_item].qualscope);
            let left_ijr = bms::relids_copy::call(&item_list[left_item].inner_join_rels);
            let (rjl, right_item) = deconstruct_recurse(
                root,
                run,
                j.rarg.as_deref().expect("JOIN rarg"),
                child_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            rightjoinlist = rjl;
            let right_qs = bms::relids_copy::call(&item_list[right_item].qualscope);
            let right_ijr = bms::relids_copy::call(&item_list[right_item].inner_join_rels);
            // Compute join domain contents: parent_domain += child_domain.
            let cd_relids = bms::relids_copy::call(&root.join_domains[child_domain].jd_relids);
            root.join_domains[parent_domain].jd_relids = bms::relids_add_members::call(
                root.join_domains[parent_domain].jd_relids.take(),
                &cd_relids,
            );
            jtitem.qualscope = bms::relids_union::call(&left_qs, &right_qs);
            // caution: ANTI join derived from SEMI will lack rtindex.
            if j.rtindex != 0 {
                root.join_domains[parent_domain].jd_relids = bms::relids_add_member::call(
                    root.join_domains[parent_domain].jd_relids.take(),
                    j.rtindex,
                );
                jtitem.qualscope = bms::relids_add_member::call(jtitem.qualscope.take(), j.rtindex);
                root.outer_join_rels =
                    bms::relids_add_member::call(root.outer_join_rels.take(), j.rtindex);
                mark_rels_nulled_by_join(root, j.rtindex, &right_qs);
            }
            jtitem.inner_join_rels = bms::relids_union::call(&left_ijr, &right_ijr);
            jtitem.left_rels = left_qs;
            jtitem.right_rels = right_qs;
            jtitem.nonnullable_rels = bms::relids_copy::call(&jtitem.left_rels);
        } else if jointype == JOIN_SEMI {
            // This node belongs to parent_domain, as do its children.
            jtitem.jdomain = parent_domain;
            let (ljl, left_item) = deconstruct_recurse(
                root,
                run,
                j.larg.as_deref().expect("JOIN larg"),
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            leftjoinlist = ljl;
            let left_qs = bms::relids_copy::call(&item_list[left_item].qualscope);
            let left_ijr = bms::relids_copy::call(&item_list[left_item].inner_join_rels);
            let (rjl, right_item) = deconstruct_recurse(
                root,
                run,
                j.rarg.as_deref().expect("JOIN rarg"),
                parent_domain,
                Some(my_index),
                item_list,
                post_counter,
            );
            rightjoinlist = rjl;
            let right_qs = bms::relids_copy::call(&item_list[right_item].qualscope);
            let right_ijr = bms::relids_copy::call(&item_list[right_item].inner_join_rels);
            jtitem.qualscope = bms::relids_union::call(&left_qs, &right_qs);
            // SEMI join never has rtindex, so don't add to anything.
            debug_assert!(j.rtindex == 0);
            jtitem.inner_join_rels = bms::relids_union::call(&left_ijr, &right_ijr);
            jtitem.left_rels = left_qs;
            jtitem.right_rels = right_qs;
            // Semi join adds no restrictions for quals.
            jtitem.nonnullable_rels = None;
        } else if jointype == JOIN_FULL {
            // The FULL JOIN's quals need their very own domain.
            let fj_domain = new_join_domain(root);
            jtitem.jdomain = fj_domain;
            // Recurse, giving each side its own join domain.
            let child_domain_l = new_join_domain(root);
            let (ljl, left_item) = deconstruct_recurse(
                root,
                run,
                j.larg.as_deref().expect("JOIN larg"),
                child_domain_l,
                Some(my_index),
                item_list,
                post_counter,
            );
            leftjoinlist = ljl;
            let left_qs = bms::relids_copy::call(&item_list[left_item].qualscope);
            let left_ijr = bms::relids_copy::call(&item_list[left_item].inner_join_rels);
            root.join_domains[fj_domain].jd_relids =
                bms::relids_copy::call(&root.join_domains[child_domain_l].jd_relids);
            let child_domain_r = new_join_domain(root);
            let (rjl, right_item) = deconstruct_recurse(
                root,
                run,
                j.rarg.as_deref().expect("JOIN rarg"),
                child_domain_r,
                Some(my_index),
                item_list,
                post_counter,
            );
            rightjoinlist = rjl;
            let right_qs = bms::relids_copy::call(&item_list[right_item].qualscope);
            let right_ijr = bms::relids_copy::call(&item_list[right_item].inner_join_rels);
            // fj_domain += child_domain_r.
            let cdr_relids = bms::relids_copy::call(&root.join_domains[child_domain_r].jd_relids);
            root.join_domains[fj_domain].jd_relids = bms::relids_add_members::call(
                root.join_domains[fj_domain].jd_relids.take(),
                &cdr_relids,
            );
            // parent_domain += fj_domain.
            let fj_relids = bms::relids_copy::call(&root.join_domains[fj_domain].jd_relids);
            root.join_domains[parent_domain].jd_relids = bms::relids_add_members::call(
                root.join_domains[parent_domain].jd_relids.take(),
                &fj_relids,
            );
            jtitem.qualscope = bms::relids_union::call(&left_qs, &right_qs);
            debug_assert!(j.rtindex != 0);
            root.join_domains[parent_domain].jd_relids = bms::relids_add_member::call(
                root.join_domains[parent_domain].jd_relids.take(),
                j.rtindex,
            );
            jtitem.qualscope = bms::relids_add_member::call(jtitem.qualscope.take(), j.rtindex);
            root.outer_join_rels = bms::relids_add_member::call(root.outer_join_rels.take(), j.rtindex);
            mark_rels_nulled_by_join(root, j.rtindex, &left_qs);
            mark_rels_nulled_by_join(root, j.rtindex, &right_qs);
            jtitem.inner_join_rels = bms::relids_union::call(&left_ijr, &right_ijr);
            jtitem.left_rels = left_qs;
            jtitem.right_rels = right_qs;
            // each side is both outer and inner.
            jtitem.nonnullable_rels = bms::relids_copy::call(&jtitem.qualscope);
        } else {
            // JOIN_RIGHT was eliminated during reduce_outer_joins().
            panic!("unrecognized join type: {}", jointype as i32);
        }

        // Compute the output joinlist. Fold subproblems together except at a
        // FULL JOIN or where join_collapse_limit would be exceeded.
        if jointype == JOIN_FULL {
            // force the join order exactly at this node
            joinlist = alloc::vec![JoinlistNode::Sub(alloc::vec![
                JoinlistNode::Sub(leftjoinlist),
                JoinlistNode::Sub(rightjoinlist),
            ])];
        } else if leftjoinlist.len() as i32 + rightjoinlist.len() as i32 <= join_collapse_limit() {
            // OK to combine subproblems.
            let mut jl = leftjoinlist;
            jl.extend(rightjoinlist);
            joinlist = jl;
        } else {
            // can't combine, but needn't force join order above here.
            // avoid creating useless 1-element sublists.
            let leftpart = if leftjoinlist.len() == 1 {
                leftjoinlist.into_iter().next().unwrap()
            } else {
                JoinlistNode::Sub(leftjoinlist)
            };
            let rightpart = if rightjoinlist.len() == 1 {
                rightjoinlist.into_iter().next().unwrap()
            } else {
                JoinlistNode::Sub(rightjoinlist)
            };
            joinlist = alloc::vec![leftpart, rightpart];
        }
    } else {
        panic!("unrecognized node type: {:?}", jtnode.node_tag());
    }

    // Finally, add the new JoinTreeItem to item_list (assign its post-order
    // rank — C appends here, after children — and store back into the slot).
    jtitem.post_order = *post_counter;
    *post_counter += 1;
    jtitem.jti_parent = parent_jtitem;
    item_list[my_index] = jtitem;
    (joinlist, my_index)
}

/// `mark_rels_nulled_by_join` (initsplan.c:1665).
///
/// Fill `RelOptInfo.nulling_relids` of baserels nulled by this outer join.
/// `ojrelid` is the RT index of the join RTE (must not be 0); `lower_rels` is
/// the base+OJ Relids syntactically below the nullable side.
pub fn mark_rels_nulled_by_join(root: &mut PlannerInfo, ojrelid: i32, lower_rels: &Relids) {
    let mut relid = -1;
    loop {
        relid = bms::relids_next_member::call(lower_rels, relid);
        if relid <= 0 {
            break;
        }
        // ignore the RTE_GROUP RTE
        if relid == root.group_rtindex {
            continue;
        }
        match root.simple_rel_array[relid as usize] {
            None => {
                // must be an outer join
                debug_assert!(bms::relids_is_member::call(relid, &root.outer_join_rels));
                continue;
            }
            Some(rel_id) => {
                let rel = root.rel_mut(rel_id);
                rel.nulling_relids = bms::relids_add_member::call(rel.nulling_relids.take(), ojrelid);
            }
        }
    }
}

/// `deconstruct_distribute` (initsplan.c:1464).
///
/// Process one jointree node in phase 2: distribute its quals to the appropriate
/// restriction/join lists, and add `SpecialJoinInfo` entries for outer joins.
fn deconstruct_distribute(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    item_list: &mut Vec<JoinTreeItem>,
    jti: JtId,
) -> types_error::PgResult<()> {
    // Move the node kind out of the item (C reads `j->quals`/`f->quals` by
    // pointer; deconstruct_distribute is the last reader of `kind`, so a `take`
    // is sound and — unlike a `.clone()` — never deep-copies the qual `Vec<Expr>`
    // through the panicking `Expr::clone` for owned-subtree quals (SubPlan/etc).
    let kind = core::mem::take(&mut item_list[jti].kind);
    match kind {
        JtNodeKind::RangeTblRef { rtindex } => {
            // Deal with any securityQuals attached to the RTE.
            if root.qual_security_level > 0 {
                process_security_barrier_quals(root, run, rtindex, item_list, jti);
            }
        }
        JtNodeKind::FromExpr { quals } => {
            let qualscope = bms::relids_copy::call(&item_list[jti].qualscope);
            // Process any lateral-referencing quals postponed to this level by
            // children.
            let lateral = core::mem::take(&mut item_list[jti].lateral_clauses);
            distribute_quals_to_rels(
                root, run, &lateral, item_list, jti, None, root.qual_security_level, &qualscope,
                &None, &None, &None, true, false, false, None,
            );
            // Now process the top-level quals.
            distribute_quals_to_rels(
                root, run, &quals, item_list, jti, None, root.qual_security_level, &qualscope,
                &None, &None, &None, true, false, false, None,
            );
        }
        JtNodeKind::JoinExpr {
            jointype,
            rtindex,
            quals,
        } => {
            // Include lateral-referencing quals postponed from children in
            // my_quals, so make_outerjoininfo handles them. (Destructive to
            // jtitem->lateral_clauses, but we won't use that again.)
            let mut my_quals = core::mem::take(&mut item_list[jti].lateral_clauses);
            my_quals.extend(quals);

            let left_rels = bms::relids_copy::call(&item_list[jti].left_rels);
            let right_rels = bms::relids_copy::call(&item_list[jti].right_rels);
            let inner_join_rels = bms::relids_copy::call(&item_list[jti].inner_join_rels);
            let qualscope = bms::relids_copy::call(&item_list[jti].qualscope);
            let nonnullable_rels = bms::relids_copy::call(&item_list[jti].nonnullable_rels);

            // For an OJ, form the SpecialJoinInfo now so we can pass it to
            // distribute_qual_to_rels, and compute its ojscope too. Semijoins
            // are hybrid: build a SpecialJoinInfo but want ojscope = NULL.
            let sjinfo: Option<Box<SpecialJoinInfo>>;
            let mut ojscope: Relids;
            if jointype != JOIN_INNER {
                let sj = crate::outerjoin::make_outerjoininfo(
                    root,
                    run,
                    &left_rels,
                    &right_rels,
                    &inner_join_rels,
                    jointype,
                    rtindex,
                    &my_quals,
                )?;
                if jointype == JOIN_SEMI {
                    ojscope = None;
                } else {
                    ojscope = bms::relids_union::call(&sj.min_lefthand, &sj.min_righthand);
                }
                sjinfo = Some(Box::new(sj));
            } else {
                sjinfo = None;
                ojscope = None;
            }

            // If it's a left join with a join clause strict for the LHS, we must
            // postpone handling of non-degenerate join clauses, in case the join
            // can commute with another left join per identity 3. (Degenerate
            // clauses need not be postponed; they drop below this join anyway.)
            let postpone;
            if jointype == JOIN_LEFT && sjinfo.as_ref().map(|s| s.lhs_strict).unwrap_or(false) {
                postpone = true;
                let sj = sjinfo.as_ref().unwrap();
                // Add back any commutable lower OJ relids removed from
                // min_lefthand/min_righthand, else the ojscope cross-check in
                // distribute_qual_to_rels complains. Since we're postponing
                // non-degenerate clauses, this only affects that cross-check.
                ojscope = bms::relids_add_members::call(ojscope, &sj.commute_below_l);
                ojscope = bms::relids_add_members::call(ojscope, &sj.commute_below_r);
            } else {
                postpone = false;
            }

            // Store sjinfo on the jtitem.
            item_list[jti].sjinfo = sjinfo.clone();

            // Process the JOIN's qual clauses.
            distribute_quals_to_rels(
                root,
                run,
                &my_quals,
                item_list,
                jti,
                sjinfo.as_deref(),
                root.qual_security_level,
                &qualscope,
                &ojscope,
                &nonnullable_rels,
                &None, // incompatible_relids
                true,  // allow_equivalence
                false,
                false, // not clones
                if postpone { Some(jti) } else { None },
            );

            // And add the SpecialJoinInfo to join_info_list.
            if let Some(sj) = sjinfo {
                root.join_info_list.push(*sj);
            }
        }
    }
    Ok(())
}

/// `process_security_barrier_quals` (initsplan.c:1616).
///
/// Transfer security-barrier quals from the RTE's `securityQuals` into the
/// relation's baserestrictinfo (via `distribute_quals_to_rels`), assigning a
/// successively higher security level per sublist. In inheritance cases only the
/// parent's quals are considered here; child quals are handled at path creation.
fn process_security_barrier_quals(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    rti: i32,
    item_list: &mut Vec<JoinTreeItem>,
    jti: JtId,
) {
    // rte = root->simple_rte_array[rti]; gather its securityQuals (each element
    // is an implicitly-ANDed list of clauses).
    let rte_id = root.simple_rte_array[rti as usize];
    let security_quals: Vec<Vec<Expr>> = run
        .resolve_rte(rte_id)
        .securityQuals
        .iter()
        .map(|qualset_node| quals_implicit_and(run.mcx(), Some(qualset_node)))
        .collect();

    let qualscope = bms::relids_copy::call(&item_list[jti].qualscope);
    let mut security_level: u32 = 0;
    for qualset in &security_quals {
        // We cheat to the extent of passing ojscope = qualscope rather than its
        // more logical value of NULL. The only effect is to force a Var-free
        // qual to be evaluated at the rel rather than pushed up to top of tree.
        distribute_quals_to_rels(
            root,
            run,
            qualset,
            item_list,
            jti,
            None,
            security_level,
            &qualscope,
            &qualscope,
            &None,
            &None,
            true,
            false,
            false, // not clones
            None,
        );
        security_level += 1;
    }
    // Assert qual_security_level is higher than anything we just used.
    debug_assert!(security_level <= root.qual_security_level);
}

/// `deconstruct_distribute_oj_quals` (initsplan.c:2226).
///
/// For a postponed LEFT JOIN (`jtitem` has `oj_joinclauses`): recompute the
/// join's syntactic/semantic scopes, then either push the clauses out as-is, or
/// — if this join commutes with others per outer-join identity 3 — generate qual
/// variants with different nullingrels labelings and distribute each at the
/// correct join level.
fn deconstruct_distribute_oj_quals(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    item_list: &mut Vec<JoinTreeItem>,
    jti: JtId,
) -> types_error::PgResult<()> {
    // The jtitem stores a *copy* of the SpecialJoinInfo made at the time the
    // join was first processed (deconstruct_distribute), but make_outerjoininfo
    // back-patches the commute_above_l / commute_above_r fields of already-built
    // SpecialJoinInfos in root.join_info_list when a syntactically-higher
    // commuting outer join is later discovered. In C, jtitem->sjinfo and the
    // root->join_info_list entry are the *same* pointer, so that back-patch is
    // visible here; in this owned-value port they are independent clones, so the
    // jtitem copy has stale (empty) commute_above_* sets. Re-fetch the
    // authoritative, back-patched SpecialJoinInfo from root.join_info_list by
    // ojrelid (it is appended there at the end of deconstruct_distribute, so it
    // is always present by the time the distribute pass runs).
    let oj_relid = item_list[jti]
        .sjinfo
        .as_ref()
        .expect("oj item has sjinfo")
        .ojrelid;
    let sjinfo: Box<SpecialJoinInfo> = Box::new(
        root.join_info_list
            .iter()
            .find(|s| s.ojrelid == oj_relid)
            .expect("oj sjinfo in join_info_list")
            .clone(),
    );

    // Recompute syntactic and semantic scopes of this left join.
    let mut qualscope = bms::relids_union::call(&sjinfo.syn_lefthand, &sjinfo.syn_righthand);
    qualscope = bms::relids_add_member::call(qualscope, sjinfo.ojrelid as i32);
    let ojscope = bms::relids_union::call(&sjinfo.min_lefthand, &sjinfo.min_righthand);
    let nonnullable_rels = bms::relids_copy::call(&sjinfo.syn_lefthand);

    debug_assert!(sjinfo.lhs_strict); // else we shouldn't be here

    if !bms::relids_is_empty::call(&sjinfo.commute_above_r) || !bms::relids_is_empty::call(&sjinfo.commute_below_l)
    {
        // Identify the outer joins this one commutes with.
        let joins_above = bms::relids_copy::call(&sjinfo.commute_above_r);
        let joins_below = bms::relids_copy::call(&sjinfo.commute_below_l);

        // Generate qual variants with different sets of nullingrels bits. We
        // first strip out all nullingrels bits for commuting joins below this
        // one, then successively put them back as we crawl up the join stack.
        let mut quals = core::mem::take(&mut item_list[jti].oj_joinclauses);
        if !bms::relids_is_empty::call(&joins_below) {
            quals = remove_nulling_relids_exprs(run.mcx(), &quals, &joins_below, &None);
        }

        // We'll need to mark the lower versions of the quals as not safe to
        // apply above not-yet-processed joins of the stack.
        let mut incompatible_joins = bms::relids_union::call(&joins_below, &joins_above);
        incompatible_joins = bms::relids_add_member::call(incompatible_joins, sjinfo.ojrelid as i32);

        // Each time we produce RestrictInfo(s) from these quals, reset the
        // last_rinfo_serial counter, so the RestrictInfos for the "same" qual
        // condition get identical serial numbers.
        let save_last_rinfo_serial = root.last_rinfo_serial;

        let mut joins_so_far: Relids = None;
        // Visit the interesting joins in syntactic nesting order: rely on the
        // jtitems list being ordered that way (post-order).
        let order = post_order_indices(item_list);
        for otherjti in order {
            let othersj = match &item_list[otherjti].sjinfo {
                Some(s) => (**s).clone(),
                None => continue, // not an outer-join item, ignore
            };

            let below_sjinfo;
            let above_sjinfo;
            if bms::relids_is_member::call(othersj.ojrelid as i32, &joins_below) {
                // othersj commutes with sjinfo from below left
                below_sjinfo = true;
                above_sjinfo = false;
            } else if othersj.ojrelid == sjinfo.ojrelid {
                // found our join in syntactic order
                debug_assert!(bms::relids_equal::call(&joins_so_far, &joins_below));
                below_sjinfo = false;
                above_sjinfo = false;
            } else if bms::relids_is_member::call(othersj.ojrelid as i32, &joins_above) {
                // othersj commutes with sjinfo from above
                below_sjinfo = false;
                above_sjinfo = true;
            } else {
                // othersj is not relevant, ignore
                continue;
            }

            // Reset serial counter for this version of the quals.
            root.last_rinfo_serial = save_last_rinfo_serial;

            // When looking at joins above sjinfo, we envision pushing sjinfo to
            // above othersj, so add othersj's nulling bit before distributing
            // the quals — to Vars coming from the current join's LHS (Pbc ->
            // Pb*c). We must also remove that bit from incompatible_joins.
            if above_sjinfo {
                let single = bms::relids_make_singleton::call(othersj.ojrelid as i32);
                quals = add_nulling_relids_exprs(run.mcx(), &quals, &sjinfo.syn_lefthand, &single);
                incompatible_joins = del_member(incompatible_joins, othersj.ojrelid as i32);
            }

            // Compute qualscope and ojscope for this join level.
            let mut this_qualscope = bms::relids_union::call(&qualscope, &joins_so_far);
            let mut this_ojscope = bms::relids_union::call(&ojscope, &joins_so_far);
            if above_sjinfo {
                // othersj is not yet in joins_so_far, but we need it
                this_qualscope = bms::relids_add_member::call(this_qualscope, othersj.ojrelid as i32);
                this_ojscope = bms::relids_add_member::call(this_ojscope, othersj.ojrelid as i32);
                // sjinfo is in joins_so_far, and we don't want it
                this_ojscope = del_member(this_ojscope, sjinfo.ojrelid as i32);
            }

            // We generate EquivalenceClasses only from the first form of the
            // quals (fewest nullingrels bits). That's also the only one marked
            // has_clone rather than is_clone.
            let allow_equivalence = bms::relids_is_empty::call(&joins_so_far);
            let has_clone = allow_equivalence;
            let is_clone = !has_clone;

            let incompat_copy = bms::relids_copy::call(&incompatible_joins);
            distribute_quals_to_rels(
                root,
                run,
                &quals,
                item_list,
                otherjti,
                Some(&sjinfo),
                root.qual_security_level,
                &this_qualscope,
                &this_ojscope,
                &nonnullable_rels,
                &incompat_copy,
                allow_equivalence,
                has_clone,
                is_clone,
                None, // no more postponement
            );

            // Adjust qual nulling bits for next level up, if needed. We don't
            // want sjinfo's own bit, and if above sjinfo we did it already.
            // Here we mark all Vars coming from the lower join's RHS (putting
            // back bits stripped above). Update incompatible_joins too.
            if below_sjinfo {
                let single = bms::relids_make_singleton::call(othersj.ojrelid as i32);
                quals = add_nulling_relids_exprs(run.mcx(), &quals, &othersj.syn_righthand, &single);
                incompatible_joins = del_member(incompatible_joins, othersj.ojrelid as i32);
            }

            // ... and track joins processed so far.
            joins_so_far = bms::relids_add_member::call(joins_so_far, othersj.ojrelid as i32);
        }
    } else {
        // No commutation possible, just process the postponed clauses.
        let quals = core::mem::take(&mut item_list[jti].oj_joinclauses);
        distribute_quals_to_rels(
            root,
            run,
            &quals,
            item_list,
            jti,
            Some(&sjinfo),
            root.qual_security_level,
            &qualscope,
            &ojscope,
            &nonnullable_rels,
            &None, // incompatible_relids
            true,  // allow_equivalence
            false,
            false, // not clones
            None,  // no more postponement
        );
    }
    Ok(())
}

/// `bms_del_member(a, x)` — relnode-seams has no del_member; mirror it as
/// `a \ {x}` (`relids_difference(a, {x})`), matching the .port-ref guidance.
fn del_member(a: Relids, x: i32) -> Relids {
    let single = bms::relids_make_singleton::call(x);
    bms::relids_difference::call(&a, &single)
}

/// `remove_nulling_relids((Node *) quals, removable, except)` applied over an
/// implicit-AND `List *` of quals: map it element-wise over the conjunct list.
/// The per-Expr work is `eqext::remove_nulling_relids`.
fn remove_nulling_relids_exprs(mcx: mcx::Mcx<'_>, quals: &[Expr], removable: &Relids, except: &Relids) -> Vec<Expr> {
    quals
        .iter()
        .map(|q| eqext::remove_nulling_relids::call(clone_qual_expr(mcx, q), bms::relids_copy::call(removable), bms::relids_copy::call(except)))
        .collect()
}

/// Deep-copy a qual `Expr` value (C: pointer reuse). The derived `Expr::clone()`
/// panics on owned-subtree variants (SubLink/SubPlan/Aggref) whose children only
/// deep-copy via `clone_in`; route every qual copy through `Expr::clone_in`.
///
/// The copy MUST land in the long-lived planner arena, not a throwaway
/// `MemoryContext`: the returned `Expr` (lifetime-erased to `'static`) keeps
/// `PgBox`/`PgVec` children pointing into the arena it was allocated in, and the
/// node is dropped much later (with the postponed-OJ / nulling-relids qual
/// lists). A local context freed on return would leave those children dangling,
/// so the eventual drop frees against an already-freed context (use-after-free).
fn clone_qual_expr(mcx: mcx::Mcx<'_>, expr: &Expr) -> Expr {
    expr.clone_in(mcx)
        .unwrap_or_else(|e| panic!("clone_qual_expr: clone_in: {e:?}"))
}

/// `add_nulling_relids((Node *) quals, target, added)` over an implicit-AND
/// `List *`: element-wise over the conjunct list. Routed through the
/// `add_nulling_relids_expr` seam in this crate's ext-seams (the rewrite-core
/// owner works over `&mut Node`, model mismatch — loud-panic until that seam is
/// installed; only reached for outer-join clone quals).
fn add_nulling_relids_exprs(mcx: mcx::Mcx<'_>, quals: &[Expr], target: &Relids, added: &Relids) -> Vec<Expr> {
    quals
        .iter()
        .map(|q| {
            initext::add_nulling_relids_expr::call(
                clone_qual_expr(mcx, q),
                bms::relids_copy::call(target),
                bms::relids_copy::call(added),
            )
        })
        .collect()
}

/// `distribute_quals_to_rels` (initsplan.c:2466).
///
/// Apply `distribute_qual_to_rels` to each element of an AND'ed list of clauses.
#[allow(clippy::too_many_arguments)]
pub fn distribute_quals_to_rels(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    clauses: &[Expr],
    item_list: &mut Vec<JoinTreeItem>,
    jti: JtId,
    sjinfo: Option<&SpecialJoinInfo>,
    security_level: u32,
    qualscope: &Relids,
    ojscope: &Relids,
    outerjoin_nonnullable: &Relids,
    incompatible_relids: &Relids,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postponed_oj_qual_list: Option<JtId>,
) {
    for clause in clauses {
        crate::quals::distribute_qual_to_rels(
            root,
            run,
            clause,
            item_list,
            jti,
            sjinfo,
            security_level,
            qualscope,
            ojscope,
            outerjoin_nonnullable,
            incompatible_relids,
            allow_equivalence,
            has_clone,
            is_clone,
            postponed_oj_qual_list,
        );
    }
}
