//! `backend/optimizer/prep/prepjointree.c` — FAMILY 2: the `pull_up_subqueries`
//! family.
//!
//! 1:1 port of PostgreSQL 18.3 `pull_up_subqueries` /
//! `pull_up_subqueries_recurse` / `pull_up_simple_subquery` /
//! `is_simple_subquery` / `is_safe_append_member` /
//! `jointree_contains_lateral_outer_refs` / `perform_pullup_replace_vars` /
//! `replace_vars_in_jointree` / `pullup_replace_vars` /
//! `pullup_replace_vars_callback` / `pullup_replace_vars_subquery` over the
//! repo's lifetime-free owned `Query<'mcx>` + embedded-`PgBox` model.
//!
//! ## Model notes
//!
//! * The C entry takes `PlannerInfo *root` and reads/mutates `root->parse` (the
//!   top `Query`), `root->append_rel_list`, `root->glob`, and the planner-arena
//!   nodes. Here `PlannerInfo` is lifetime-free and the top `Query` is threaded
//!   as a distinct `&mut Query<'mcx>` alongside `&mut PlannerInfo` (the planner
//!   driver resolves it via `run.resolve_mut`). The two are distinct objects so
//!   there is no aliasing conflict.
//! * The jointree is the embedded `Option<PgBox<FromExpr>>` / `Vec<NodePtr>` /
//!   `JoinExpr.larg`/`rarg`, walked by deref exactly as the C walks `Node *`.
//!   `pull_up_subqueries_recurse` consumes the node by value and returns its
//!   replacement (the C `return jtnode`), which the caller stores back.
//! * `pullup_replace_vars` runs the rewrite-core `replace_rte_variables` engine
//!   over a *clone* of the source node ([`Expr::clone_in`], #280) — the C
//!   returns a mutated copy and never edits in place. For the *typed* lists
//!   (`targetList`/`returningList` carry owned `TargetEntry` values;
//!   `joinaliasvars`/`groupexprs`/`values_lists`/`functions`/`groupingSets`/
//!   `mergeActionList` carry `NodePtr`) the replacement runs per element, since
//!   our lists are typed vectors rather than a single `Node`-list. The
//!   `AppendRelInfo.translated_vars` (`Vec<NodeId>`, #274) replacement resolves
//!   each id to its arena `Expr`, runs the replacement over a clone, and writes
//!   it back.
//!
//! ## The tri-bitmapset bridge (`pullup_replace_vars_callback`)
//!
//! The callback mixes three relid representations and bridges them faithfully:
//!   * `'mcx`-arena [`Bitmapset`] — `rcon.relids` (from `get_relids_in_jointree`)
//!     and `nullinfo.nullingrels[i]` (from `get_nullingrels`).
//!   * lifetime-free [`PathRelids`](types_pathnodes::Relids) — what `pull_varnos`
//!     returns.
//!   * [`ExprRelids`] word-vectors — `Var.varnullingrels`,
//!     `PlaceHolderVar.phrels`/`phnullingrels`, and `add_nulling_relids` args.
//! The set algebra mirrors the C `bms_*` math; no fourth representation is
//! introduced. Cross-representation tests (`bms_is_subset`/`bms_is_member`/
//! `bms_overlap` over a `PathRelids` vs an `'mcx` Bitmapset, etc.) go through the
//! small word-level helpers below.

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_core::primitive::AttrNumber;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Expr, ExprRelids, Var};
use types_nodes::rawnodes::FromExpr;
use types_pathnodes::{NodeId, PlannerInfo};
use types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL;

use backend_nodes_core::bitmapset::{bms_is_member, bms_is_subset};
use backend_optimizer_util_clauses::grounded::{
    contain_nonstrict_functions, contain_volatile_functions,
};
use backend_optimizer_util_vars::var::{contain_vars_of_level, pull_varnos, pull_varnos_of_level};
use backend_rewrite_core::{
    add_nulling_relids, replace_rte_variables, IncrementVarSublevelsUp,
    IncrementVarSublevelsUp_rtable, OffsetVarNodes, ReplaceVarFromTargetList,
    ReplaceVarsNoMatchOption,
};
use backend_rewrite_core::replace::ReplaceRteVariablesContext;

use backend_optimizer_plan_subselect_pullup as subselect;
use backend_optimizer_util_placeholder_seams as placeholder;
use backend_rewrite_rewritemanip_seams as rewritemanip;

use types_nodes::bitmapset::Bitmapset;

use crate::result_rtes;
use crate::sublinks::pull_up_sublinks;

/// C `Relids` = `Bitmapset *`: the `'mcx`-arena relid set (NULL/empty = `None`).
type Relids<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

/// C `Relids` as it is returned by `pull_varnos` / consumed by
/// `make_placeholder_expr` — the lifetime-free word-vector `Bitmapset`.
type PathRelids = types_pathnodes::Relids;

// ===========================================================================
// REPLACE_WRAP option (prepjointree.c ReplaceWrapOption)
// ===========================================================================

/// `ReplaceWrapOption` (prepjointree.c:154).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplaceWrapOption {
    /// `REPLACE_WRAP_NONE` — only wrap PHVs when there are nullingrels.
    None,
    /// `REPLACE_WRAP_ALL` — always wrap; used when grouping sets are present.
    All,
    /// `REPLACE_WRAP_VARFREE` — wrap variable-free expressions.
    Varfree,
}

// ===========================================================================
// pullup_replace_vars_context (prepjointree.c:160)
// ===========================================================================

/// `pullup_replace_vars_context` (prepjointree.c:160). The data describing what
/// to substitute for Vars referencing the target subquery. The `root` /
/// callback wiring is threaded separately (see [`pullup_replace_vars`]); the
/// engine cannot store a self-referential `&mut dyn FnMut` in the context.
struct PullupReplaceVarsContext<'mcx> {
    /// `List *targetlist` — the subquery's targetlist, owned `TargetEntry`s.
    targetlist: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    /// `RangeTblEntry *target_rte` — the RTE being pulled up.
    target_rte: PgBox<'mcx, RangeTblEntry<'mcx>>,
    /// `int result_relation`.
    result_relation: i32,
    /// `Relids relids` — base relids of the subquery being pulled up; the
    /// `'mcx`-arena set from `get_relids_in_jointree` (NULL for non-lateral).
    relids: Relids<'mcx>,
    /// `nullingrel_info *nullinfo` — per-RTE nulling joins; NULL for non-lateral.
    nullinfo: Option<result_rtes::NullingrelInfo<'mcx>>,
    /// `int varno` — the subquery's RT index in the upper query.
    varno: i32,
    /// `ReplaceWrapOption wrap_option`.
    wrap_option: ReplaceWrapOption,
    /// `Node **rv_cache` — cache of the modified expressions, indexed
    /// `0 ..= length(targetlist)` (PHV dedup, copies = `Expr::clone_in`).
    rv_cache: Vec<Option<Expr<'mcx>>>,
}

// ===========================================================================
// PathRelids word helpers (the lifetime-free Bitmapset that pull_varnos returns)
// ===========================================================================

const BITS_PER_WORD: usize = 64;

#[inline]
fn pathrelids_singleton(x: i32) -> PathRelids {
    debug_assert!(x >= 0);
    let wnum = (x as usize) / BITS_PER_WORD;
    let bit = (x as usize) % BITS_PER_WORD;
    let mut words = alloc::vec::Vec::new();
    words.resize(wnum + 1, 0u64);
    words[wnum] |= 1u64 << bit;
    Some(Box::new(types_pathnodes::Bitmapset { words }))
}

#[inline]
fn pathrelids_next_member(a: &PathRelids, prevbit: i32) -> i32 {
    let words: &[u64] = match a {
        None => return -2,
        Some(b) => &b.words,
    };
    let mut bit = prevbit + 1;
    while (bit as usize) < words.len() * BITS_PER_WORD {
        let wnum = (bit as usize) / BITS_PER_WORD;
        let off = (bit as usize) % BITS_PER_WORD;
        let w = words[wnum] >> off;
        if w != 0 {
            return bit + w.trailing_zeros() as i32;
        }
        bit = ((wnum + 1) * BITS_PER_WORD) as i32;
    }
    -2
}

/// `bms_is_subset(a, b)` where `a` is a [`PathRelids`] and `b` is an `'mcx`
/// [`Bitmapset`] — is every member of `a` also in `b`?
fn pathrelids_is_subset_of_bms(a: &PathRelids, b: Option<&Bitmapset>) -> bool {
    let aw: &[u64] = match a {
        None => return true,
        Some(a) => &a.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(b) => &b.words,
    };
    for (i, &w) in aw.iter().enumerate() {
        let bb = if i < bw.len() { bw[i] } else { 0 };
        if (w & !bb) != 0 {
            return false;
        }
    }
    true
}

/// `bms_overlap(a, b)` where `a` is a [`PathRelids`] and `b` is an `'mcx`
/// [`Bitmapset`].
fn pathrelids_overlap_bms(a: &PathRelids, b: Option<&Bitmapset>) -> bool {
    let aw: &[u64] = match a {
        None => return false,
        Some(a) => &a.words,
    };
    let bw: &[u64] = match b {
        None => return false,
        Some(b) => &b.words,
    };
    let n = aw.len().min(bw.len());
    for i in 0..n {
        if (aw[i] & bw[i]) != 0 {
            return true;
        }
    }
    false
}

/// `bms_del_members(a, b)` where `a` is a [`PathRelids`] and `b` is an `'mcx`
/// [`Bitmapset`] — remove `b`'s members from `a` in place.
fn pathrelids_del_bms(a: &mut PathRelids, b: Option<&Bitmapset>) {
    let bw: &[u64] = match b {
        None => return,
        Some(b) => &b.words,
    };
    if let Some(av) = a.as_mut() {
        let n = av.words.len().min(bw.len());
        for i in 0..n {
            av.words[i] &= !bw[i];
        }
    }
}

/// Convert an `'mcx` [`Bitmapset`] to the lifetime-free [`ExprRelids`]
/// (`make_placeholder_expr` takes a [`PathRelids`]; the rewriter and
/// Var/PHV fields carry [`ExprRelids`]).
fn bms_to_expr_relids(a: Option<&Bitmapset>) -> ExprRelids {
    match a {
        None => ExprRelids { words: Vec::new() },
        Some(bms) => bms_to_expr_relids_some(bms),
    }
}

fn bms_to_expr_relids_some(bms: &Bitmapset) -> ExprRelids {
    let mut words: Vec<u64> = bms.words.iter().copied().collect();
    trim(&mut words);
    ExprRelids { words }
}

/// `bms_make_singleton(x)` as a [`PathRelids`] for `make_placeholder_expr`.
fn pathrelids_make_singleton(x: i32) -> PathRelids {
    pathrelids_singleton(x)
}

/// `bms_intersect(var->varnullingrels, nullinfo->nullingrels[lvarno])` returning
/// the lifetime-free [`ExprRelids`] form consumed by `add_nulling_relids`. `a`
/// is the [`ExprRelids`] of `var->varnullingrels`; `b` is the `'mcx`
/// nullingrels.
fn intersect_expr_with_bms(a: &ExprRelids, b: Option<&Bitmapset>) -> ExprRelids {
    let bw: &[u64] = match b {
        None => return ExprRelids { words: Vec::new() },
        Some(b) => &b.words,
    };
    let n = a.words.len().min(bw.len());
    let mut words = Vec::with_capacity(n);
    for i in 0..n {
        words.push(a.words[i] & bw[i]);
    }
    trim(&mut words);
    ExprRelids { words }
}

/// `bms_is_empty(a)` over [`ExprRelids`].
#[inline]
fn expr_relids_is_empty(a: &ExprRelids) -> bool {
    a.words.iter().all(|&w| w == 0)
}

/// `bms_add_members(a, b)` in place over [`ExprRelids`].
fn expr_relids_add_in_place(a: &mut ExprRelids, b: &ExprRelids) {
    if b.words.len() > a.words.len() {
        a.words.resize(b.words.len(), 0);
    }
    for (i, &w) in b.words.iter().enumerate() {
        a.words[i] |= w;
    }
}

/// `bms_make_singleton(x)` over [`ExprRelids`].
fn expr_relids_make_singleton(x: i32) -> ExprRelids {
    debug_assert!(x >= 0);
    let wnum = (x as usize) / BITS_PER_WORD;
    let bit = (x as usize) % BITS_PER_WORD;
    let mut words = Vec::new();
    words.resize(wnum + 1, 0u64);
    words[wnum] |= 1u64 << bit;
    ExprRelids { words }
}

#[inline]
fn trim(words: &mut Vec<u64>) {
    while let Some(&last) = words.last() {
        if last == 0 {
            words.pop();
        } else {
            break;
        }
    }
}

// ===========================================================================
// pull_up_subqueries (prepjointree.c:1097)
// ===========================================================================

/// `pull_up_subqueries(root)` (prepjointree.c:1097).
pub fn pull_up_subqueries<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    // Top level of jointree must always be a FromExpr.
    debug_assert!(parse.jointree.is_some());
    // C: `root->parse->jointree = pull_up_subqueries_recurse(root, root->parse->jointree, ...)`.
    // The C `jtnode` arg aliases `root->parse->jointree`; the slot stays
    // populated for the whole recursion (the assignment only writes back the
    // *same* pointer after the call returns). We mirror that exactly: the
    // recursion mutates the jointree in place through `parse.jointree`, never
    // detaching it, so `perform_pullup_replace_vars` (deep in the recursion)
    // can always walk the live `parse->jointree`.
    pull_up_subqueries_recurse(mcx, root, parse, &JtPath::Top, None, None)?;
    // We should still have a FromExpr at the top.
    debug_assert!(parse.jointree.is_some());
    Ok(())
}

/// A location of a jointree `Node` within `parse.jointree`. The C passes a
/// `Node *` that aliases the live tree; we cannot hold a `&mut Node` borrowed
/// out of `parse` across calls that also need `&mut parse`, so the recursion
/// addresses the node it is processing by a path from the top FromExpr and
/// re-derives a transient `&mut` only when it needs to read or overwrite the
/// slot — leaving the rest of the tree intact in `parse.jointree` throughout.
enum JtPath<'p, 'mcx> {
    /// The top `parse.jointree` FromExpr node itself.
    Top,
    /// `fromlist[index]` of the FromExpr at `parent`.
    From { parent: &'p JtPath<'p, 'mcx>, index: usize },
    /// `larg` of the JoinExpr at `parent`.
    Larg { parent: &'p JtPath<'p, 'mcx> },
    /// `rarg` of the JoinExpr at `parent`.
    Rarg { parent: &'p JtPath<'p, 'mcx> },
    /// A standalone jointree node that is *not* part of `parse.jointree`
    /// (`pull_up_union_leaf_queries` builds a fresh `RangeTblRef` and recurses
    /// on it with a containing appendrel, so `perform_pullup_replace_vars` takes
    /// the translated-vars early return and never walks `parse.jointree`).
    Detached(&'p core::cell::RefCell<Node<'mcx>>),
}

/// Resolve a `&mut Node` for `path` within `parse.jointree`. The borrow lives
/// only as long as the returned reference; callers must drop it before touching
/// `parse` otherwise.
fn jt_node_at<'a, 'mcx>(parse: &'a mut Query<'mcx>, path: &JtPath<'_, 'mcx>) -> &'a mut Node<'mcx> {
    match path {
        JtPath::Detached(_) => {
            unreachable!("jt_node_at(Detached): a detached node is handled inline by the recursion")
        }
        JtPath::Top => {
            // The top jointree node is the FromExpr held in `parse.jointree`,
            // but our `Node` enum needs a `Node::FromExpr` view. The recursion
            // never overwrites the Top slot with a non-FromExpr (the top stays
            // a FromExpr), and it only descends into its fromlist, so callers of
            // `jt_node_at(Top)` are confined to the FromExpr arm helpers below.
            unreachable!("jt_node_at(Top): the top FromExpr is addressed via its fromlist")
        }
        JtPath::From { parent, index } => {
            let f = jt_fromexpr_at(parse, parent);
            &mut *f.fromlist[*index]
        }
        JtPath::Larg { parent } => {
            let j = jt_joinexpr_at(parse, parent);
            j.larg.as_deref_mut().expect("JoinExpr with NULL larg")
        }
        JtPath::Rarg { parent } => {
            let j = jt_joinexpr_at(parse, parent);
            j.rarg.as_deref_mut().expect("JoinExpr with NULL rarg")
        }
    }
}

/// Resolve a `&mut FromExpr` for the FromExpr node at `path`.
fn jt_fromexpr_at<'a, 'mcx>(
    parse: &'a mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a mut FromExpr<'mcx> {
    match path {
        JtPath::Top => parse
            .jointree
            .as_deref_mut()
            .expect("pull_up_subqueries: top jointree must be a FromExpr"),
        _ => jt_node_at(parse, path)
            .as_fromexpr_mut()
            .unwrap_or_else(|| unreachable!("jt_fromexpr_at: node is not a FromExpr")),
    }
}

/// Resolve a `&mut JoinExpr` for the JoinExpr node at `path`.
fn jt_joinexpr_at<'a, 'mcx>(
    parse: &'a mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a mut types_nodes::rawnodes::JoinExpr<'mcx> {
    jt_node_at(parse, path)
        .as_joinexpr_mut()
        .unwrap_or_else(|| unreachable!("jt_joinexpr_at: node is not a JoinExpr"))
}

// ===========================================================================
// pull_up_subqueries_recurse (prepjointree.c:1127)
// ===========================================================================

/// `pull_up_subqueries_recurse(root, jtnode, lowest_outer_join,
/// containing_appendrel)` (prepjointree.c:1127). `jtnode` is owned (C passes a
/// `Node *` by value and returns the possibly-rebuilt node).
///
/// `lowest_outer_join` is the owned `JoinExpr` of the lowest outer join we are
/// underneath (carried as `Some(Box<...>)`); the recursion only reads its
/// jointree shape (via `get_relids_in_jointree`) so an owned copy is fine.
/// `containing_appendrel` is the index into `root.append_rel_list` of the
/// `AppendRelInfo` whose member subquery we are pulling up (the C passes a
/// pointer; here we pass the slot index so the helper can borrow it mutably
/// where needed).
fn pull_up_subqueries_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    lowest_outer_join: Option<&LowestOuterJoin<'mcx>>,
    containing_appendrel: Option<usize>,
) -> PgResult<()> {
    // Since this function recurses, it could be driven to stack overflow.
    backend_tcop_postgres_seams::check_stack_depth::call()?;
    // Also, since it's a bit expensive, let's check for query cancel.
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    // Classify the node at `path` without holding the borrow across the
    // sub-processing (which needs `&mut parse`). The top is always a FromExpr.
    enum Kind {
        RangeTblRef(i32),
        FromExpr(usize),
        Join,
        Other,
    }
    let kind = match path {
        JtPath::Top => Kind::FromExpr(jt_fromexpr_at(parse, path).fromlist.len()),
        JtPath::Detached(cell) => {
            let node = &*cell.borrow();
            match node.node_tag() {
                ntag::T_RangeTblRef => Kind::RangeTblRef(node.expect_rangetblref().rtindex),
                ntag::T_FromExpr => Kind::FromExpr(node.expect_fromexpr().fromlist.len()),
                ntag::T_JoinExpr => Kind::Join,
                _ => Kind::Other,
            }
        }
        _ => {
            let node = jt_node_at(parse, path);
            match node.node_tag() {
                ntag::T_RangeTblRef => Kind::RangeTblRef(node.expect_rangetblref().rtindex),
                ntag::T_FromExpr => Kind::FromExpr(node.expect_fromexpr().fromlist.len()),
                ntag::T_JoinExpr => Kind::Join,
                _ => Kind::Other,
            }
        }
    };

    match kind {
        Kind::RangeTblRef(varno) => {
            let rtekind = parse.rtable[(varno - 1) as usize].rtekind;

            // Is this a subquery RTE simple enough to pull up? (Append-rel
            // members need is_safe_append_member too.) The live `RangeTblRef`
            // stays in `parse.jointree` throughout the pull-up; the leaf
            // helpers only use the by-value `jtnode` as the give-up return
            // value, so a fresh `RangeTblRef` (rtindex) reconstruction is
            // equivalent to C's aliased pointer.
            if rtekind == RTEKind::RTE_SUBQUERY {
                let simple = {
                    let rte = &parse.rtable[(varno - 1) as usize];
                    let sub = rte.subquery.as_deref().expect("RTE_SUBQUERY with NULL subquery");
                    is_simple_subquery(mcx, root, parse, sub, rte, lowest_outer_join)?
                        && (containing_appendrel.is_none()
                            || is_safe_append_member(sub))
                };
                if simple {
                    let new = pull_up_simple_subquery(
                        mcx,
                        root,
                        parse,
                        Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: varno })?,
                        varno,
                        lowest_outer_join,
                        containing_appendrel,
                    )?;
                    jt_store(parse, path, new);
                    return Ok(());
                }

                // Alternatively, a simple UNION ALL subquery? Flatten into an
                // append relation.
                let is_union_all = {
                    let rte = &parse.rtable[(varno - 1) as usize];
                    let sub = rte.subquery.as_deref().unwrap();
                    is_simple_union_all(sub)?
                };
                if is_union_all {
                    let new = pull_up_simple_union_all(
                        mcx,
                        root,
                        parse,
                        Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: varno })?,
                        varno,
                    )?;
                    jt_store(parse, path, new);
                    return Ok(());
                }
            } else if rtekind == RTEKind::RTE_VALUES {
                // A simple VALUES RTE? Not allowed below an outer join nor into
                // an appendrel.
                if lowest_outer_join.is_none()
                    && containing_appendrel.is_none()
                    && is_simple_values(root, parse, &parse.rtable[(varno - 1) as usize])
                {
                    let new = pull_up_simple_values(
                        mcx,
                        root,
                        parse,
                        Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: varno })?,
                        varno,
                    )?;
                    jt_store(parse, path, new);
                    return Ok(());
                }
            } else if rtekind == RTEKind::RTE_FUNCTION {
                // A FUNCTION RTE we could inline?
                let new = pull_up_constant_function(
                    mcx,
                    root,
                    parse,
                    Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: varno })?,
                    varno,
                    containing_appendrel,
                )?;
                jt_store(parse, path, new);
                return Ok(());
            }

            // Otherwise, do nothing at this node.
            Ok(())
        }
        Kind::FromExpr(n) => {
            debug_assert!(containing_appendrel.is_none());
            // Recursively transform all the child nodes, in place.
            for i in 0..n {
                let child = JtPath::From { parent: path, index: i };
                pull_up_subqueries_recurse(mcx, root, parse, &child, lowest_outer_join, None)?;
            }
            Ok(())
        }
        Kind::Join => {
            debug_assert!(containing_appendrel.is_none());
            // Recurse, being careful to tell myself when inside an outer join.
            let jointype = jt_joinexpr_at(parse, path).jointype;
            // For the INNER case, pass down the *existing* lowest_outer_join; for
            // the outer cases pass down a snapshot of this JoinExpr.
            let inner_arg: Option<LowestOuterJoin<'mcx>> = match jointype {
                JoinType::JOIN_INNER => None,
                JoinType::JOIN_LEFT
                | JoinType::JOIN_SEMI
                | JoinType::JOIN_ANTI
                | JoinType::JOIN_FULL
                | JoinType::JOIN_RIGHT => {
                    let j = jt_joinexpr_at(parse, path);
                    Some(LowestOuterJoin::snapshot(mcx, j)?)
                }
                _ => {
                    return Err(types_error::PgError::error("unrecognized join type"));
                }
            };
            let pass: Option<&LowestOuterJoin<'mcx>> = match jointype {
                JoinType::JOIN_INNER => lowest_outer_join,
                _ => inner_arg.as_ref(),
            };

            let larg = JtPath::Larg { parent: path };
            pull_up_subqueries_recurse(mcx, root, parse, &larg, pass, None)?;
            let rarg = JtPath::Rarg { parent: path };
            pull_up_subqueries_recurse(mcx, root, parse, &rarg, pass, None)?;

            Ok(())
        }
        Kind::Other => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// Store `new` into the jointree slot at `path` (the recursion's in-place
/// equivalent of C's `*slot = pull_up_..._recurse(*slot, ...)`).
fn jt_store<'mcx>(parse: &mut Query<'mcx>, path: &JtPath<'_, 'mcx>, new: Node<'mcx>) {
    match path {
        JtPath::Top => {
            // The top stays a FromExpr; a leaf pull-up never targets Top.
            unreachable!("jt_store(Top): the top FromExpr is never replaced by a pull-up");
        }
        JtPath::Detached(cell) => {
            // C ignores the recurse result for the union-leaf standalone node;
            // store it back into the cell for symmetry (it is discarded).
            *cell.borrow_mut() = new;
        }
        _ => {
            *jt_node_at(parse, path) = new;
        }
    }
}

/// A snapshot of the lowest containing outer join's jointree shape — all that
/// `is_simple_subquery` / `is_safe_append_member` read of it
/// (`get_relids_in_jointree((Node *) lowest_outer_join, true, true)`).
struct LowestOuterJoin<'mcx> {
    /// The outer-join jointree node (an owned clone of the live `JoinExpr`'s
    /// larg/rarg/rtindex structure, sufficient for `get_relids_in_jointree`).
    node: Node<'mcx>,
}

impl<'mcx> LowestOuterJoin<'mcx> {
    /// Build a shape snapshot of `j` for relids extraction. We only need its
    /// jointree structure (RangeTblRef/JoinExpr/FromExpr indexes), so clone the
    /// arms recursively.
    fn snapshot(mcx: Mcx<'mcx>, j: &types_nodes::rawnodes::JoinExpr<'mcx>) -> PgResult<Self> {
        let node = clone_jointree_shape(mcx, &Node::mk_join_expr(mcx, clone_joinexpr_shape(mcx, j)?)?)?;
        Ok(LowestOuterJoin { node })
    }
}

/// Clone the *shape* of a JoinExpr (jointype, rtindex, larg/rarg shapes) — quals
/// are not needed for `get_relids_in_jointree`.
fn clone_joinexpr_shape<'mcx>(
    mcx: Mcx<'mcx>,
    j: &types_nodes::rawnodes::JoinExpr<'mcx>,
) -> PgResult<types_nodes::rawnodes::JoinExpr<'mcx>> {
    let larg = match j.larg.as_deref() {
        Some(n) => Some(alloc_in(mcx, clone_jointree_shape(mcx, n)?)?),
        None => None,
    };
    let rarg = match j.rarg.as_deref() {
        Some(n) => Some(alloc_in(mcx, clone_jointree_shape(mcx, n)?)?),
        None => None,
    };
    Ok(types_nodes::rawnodes::JoinExpr {
        jointype: j.jointype,
        isNatural: j.isNatural,
        larg,
        rarg,
        usingClause: PgVec::new_in(mcx),
        join_using_alias: None,
        quals: None,
        alias: None,
        rtindex: j.rtindex,
    })
}

/// Clone the shape of an arbitrary jointree `Node` (RangeTblRef/FromExpr/
/// JoinExpr), enough for `get_relids_in_jointree`.
fn clone_jointree_shape<'mcx>(mcx: Mcx<'mcx>, n: &Node<'mcx>) -> PgResult<Node<'mcx>> {
    match n.node_tag() {
        ntag::T_RangeTblRef => Ok(Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef {
            rtindex: n.expect_rangetblref().rtindex,
        })?),
        ntag::T_JoinExpr => Ok(Node::mk_join_expr(mcx, clone_joinexpr_shape(mcx, n.expect_joinexpr())?)?),
        ntag::T_FromExpr => {
            let f = n.expect_fromexpr();
            let mut fromlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            for l in f.fromlist.iter() {
                fromlist.push(alloc_in(mcx, clone_jointree_shape(mcx, l)?)?);
            }
            Ok(Node::mk_from_expr(mcx, FromExpr {
                fromlist,
                quals: None,
            })?)
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// A dummy placeholder jointree node used while moving a node out of a `&mut`
/// slot (the slot is always overwritten before being read again).
#[inline]
fn dummy_node<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
    Ok(Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: 0 })?)
}

// ===========================================================================
// pull_up_simple_subquery (prepjointree.c:1272)
// ===========================================================================

/// `pull_up_simple_subquery(root, jtnode, rte, lowest_outer_join,
/// containing_appendrel)` (prepjointree.c:1272).
fn pull_up_simple_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    varno: i32,
    lowest_outer_join: Option<&LowestOuterJoin<'mcx>>,
    containing_appendrel: Option<usize>,
) -> PgResult<Node<'mcx>> {
    // Make a modifiable copy of the subquery to hack on, so that the RTE will be
    // left unchanged in case we decide below that we can't pull it up after all.
    let mut subquery: Query<'mcx> = {
        let rte = &parse.rtable[(varno - 1) as usize];
        let sub = rte.subquery.as_deref().expect("RTE_SUBQUERY with NULL subquery");
        sub.clone_in(mcx)?
    };

    // Create a PlannerInfo data structure for this subquery. The next few steps
    // match the first processing in subquery_planner(). subroot.glob is shared
    // with root.glob (we read root.glob.lastPHId below); query_level /
    // parent_root are carried for the recursive pull-ups.
    let mut subroot = PlannerInfo::default();
    // C: `subroot->glob = root->glob` — a *shared* pointer, so the global
    // `lastPHId` counter is one sequence across the parent and every nested
    // pull-up (else nested subquery PlaceHolderVars could collide on phid). We
    // move the owned `glob` into the subroot for the recursive pull-ups and move
    // it back into `root` before the parent's own PHV-creating replace pass; the
    // give-up path restores it too.
    subroot.glob = root.glob.take();
    subroot.query_level = root.query_level;
    // (C also sets subroot->parent_root = root->parent_root, but PlannerInfo's
    // trimmed model carries no parent_root field; F2 never reads it.)

    // No CTEs to worry about.
    debug_assert!(subquery.cteList.is_empty());

    // If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE.
    subselect::replace_empty_jointree(mcx, &mut subquery)?;

    // Pull up any SubLinks within the subquery's quals.
    if subquery.hasSubLinks {
        pull_up_sublinks(mcx, &mut subroot, &mut subquery)?;
    }

    // Similarly, preprocess its function RTEs to inline set-returning functions.
    preprocess_function_rtes(mcx, &mut subroot, &mut subquery)?;

    // Scan the rangetable for relations with virtual generated columns, and
    // replace all Var nodes referencing those columns with their generation
    // expressions.
    subquery = expand_virtual_generated_columns(mcx, &mut subroot, subquery)?;

    // Recursively pull up the subquery's subqueries.
    pull_up_subqueries(mcx, &mut subroot, &mut subquery)?;

    // Now recheck whether the subquery is still simple enough to pull up. If
    // not, abandon processing it.
    let still_simple = {
        let rte = &parse.rtable[(varno - 1) as usize];
        is_simple_subquery(mcx, root, parse, &subquery, rte, lowest_outer_join)?
            && (containing_appendrel.is_none() || is_safe_append_member(&subquery))
    };
    if !still_simple {
        // Give up, return unmodified RangeTblRef (restore the shared glob first).
        root.glob = subroot.glob.take();
        return Ok(jtnode);
    }

    // Flatten any join alias Vars in the subquery's targetlist (pulling up the
    // subquery's subqueries might have changed their expansions). The C wraps
    // the whole targetList as a Node-list; here it is a typed Vec<TargetEntry>,
    // so flatten each entry's expr individually.
    {
        let n = subquery.targetList.len();
        // We need an immutable subquery view (subroot->parse) for the seam's
        // `query` arg, but the seam only consults the query's range table for
        // RTE_JOIN aliasvars — pass a clone of the subquery as the query arg.
        let query_node = Node::mk_query(mcx, subquery.clone_in(mcx)?)?;
        for i in 0..n {
            if let Some(expr) = subquery.targetList[i].expr.take() {
                let node = Node::mk_expr(mcx, PgBox::into_inner(expr))?;
                let flat = rewritemanip::flatten_join_alias_vars::call(mcx, &query_node, node)?;
                if let Some(e) = flat.into_expr() {
                    subquery.targetList[i].expr = Some(alloc_in(mcx, e)?);
                } else {
                    return Err(types_error::PgError::error(
                        "flatten_join_alias_vars: targetlist entry is not an expression",
                    ));
                }
            }
        }
    }

    // Adjust level-0 varnos in subquery so we can append its rangetable to the
    // upper query's. We have to fix the subquery's append_rel_list too.
    let rtoffset = parse.rtable.len() as i32;
    offset_var_nodes_in_query(mcx, &mut subquery, rtoffset, 0)?;
    offset_var_nodes_in_append_rel_list(mcx, &mut subroot, rtoffset, 0)?;

    // Upper-level vars in subquery are now one level closer to their parent.
    increment_var_sublevels_up_in_query(mcx, &mut subquery, -1, 1)?;
    increment_var_sublevels_up_in_append_rel_list(mcx, &mut subroot, -1, 1)?;

    // The subquery's targetlist items are now in the appropriate form to insert
    // into the top query, except that we may need to wrap them in
    // PlaceHolderVars. Set up the pullup_replace_vars context. (Include the
    // subquery's inner joins in relids, since it may include join alias vars
    // referencing them.)
    // Restore the shared `glob` into `root` before the parent's PHV-creating
    // replace pass (`make_placeholder_expr` bumps `root.glob.lastPHId`); the
    // subroot recursion above is done with it.
    root.glob = subroot.glob.take();

    let target_rte: PgBox<'mcx, RangeTblEntry<'mcx>> = {
        // Snapshot a copy of the target RTE for the context (the C keeps a
        // pointer into the still-live rtable; the callback only reads it).
        let rte = &parse.rtable[(varno - 1) as usize];
        alloc_in(mcx, rte_shallow_clone(mcx, rte)?)?
    };

    let lateral = target_rte.lateral;
    let (relids, nullinfo): (Relids<'mcx>, Option<result_rtes::NullingrelInfo<'mcx>>) = if lateral {
        let sub_jt = Node::mk_from_expr(mcx, clone_fromexpr_shape(
            mcx,
            subquery.jointree.as_deref().expect("subquery has no jointree"),
        )?)?;
        let r = result_rtes::get_relids_in_jointree(mcx, &sub_jt, true, true)?;
        let ni = result_rtes::get_nullingrels(mcx, parse)?;
        (r, Some(ni))
    } else {
        (None, None)
    };

    let mut rvcontext = PullupReplaceVarsContext {
        targetlist: clone_targetlist(mcx, &subquery.targetList)?,
        target_rte,
        result_relation: 0,
        relids,
        nullinfo,
        varno,
        wrap_option: ReplaceWrapOption::None,
        rv_cache: rv_cache_new(subquery.targetList.len()),
    };

    // If the parent query uses grouping sets, we need a PHV for each tlist item.
    if !parse.groupingSets.is_empty() {
        rvcontext.wrap_option = ReplaceWrapOption::All;
    }

    // outer_hasSubLinks is &parse->hasSubLinks; the engine updates it if it
    // copies any SubLink out of the subquery's targetlist.
    let mut outer_has_sublinks: Option<bool> = Some(parse.hasSubLinks);

    // Replace all of the top query's references to the subquery's outputs.
    perform_pullup_replace_vars(
        mcx,
        root,
        parse,
        &mut rvcontext,
        &mut outer_has_sublinks,
        containing_appendrel,
    )?;
    if let Some(v) = outer_has_sublinks {
        parse.hasSubLinks = v;
    }

    // If the subquery had a LATERAL marker, propagate that to any of its child
    // RTEs that could now contain lateral cross-references.
    if lateral {
        for child_rte in subquery.rtable.iter_mut() {
            match child_rte.rtekind {
                RTEKind::RTE_RELATION => {
                    if child_rte.tablesample.is_some() {
                        child_rte.lateral = true;
                    }
                }
                RTEKind::RTE_SUBQUERY
                | RTEKind::RTE_FUNCTION
                | RTEKind::RTE_VALUES
                | RTEKind::RTE_TABLEFUNC => {
                    child_rte.lateral = true;
                }
                RTEKind::RTE_JOIN
                | RTEKind::RTE_CTE
                | RTEKind::RTE_NAMEDTUPLESTORE
                | RTEKind::RTE_RESULT
                | RTEKind::RTE_GROUP => {
                    // these can't contain any lateral references
                }
            }
        }
    }

    // Now append the adjusted rtable entries and their perminfos to the upper
    // query.
    subselect::combine_range_tables(mcx, parse, &mut subquery);

    // Pull up any FOR UPDATE/SHARE markers, too (OffsetVarNodes already adjusted
    // the marker rtindexes, so just concat the lists).
    {
        let rowmarks = core::mem::replace(&mut subquery.rowMarks, PgVec::new_in(mcx));
        for rm in rowmarks {
            parse.rowMarks.push(rm);
        }
    }

    // Fix the relid sets of any PlaceHolderVar nodes in the parent query, and
    // relids in AppendRelInfo nodes.
    if last_ph_id(root) != 0 || !root.append_rel_list.is_empty() {
        let sub_jt = Node::mk_from_expr(mcx, clone_fromexpr_shape(
            mcx,
            // subquery.jointree may have been consumed by combine_range_tables?
            // No: combine_range_tables only moves rtable/rteperminfos. The
            // jointree is still present.
            subquery.jointree.as_deref().expect("subquery has no jointree"),
        )?)?;
        let subrelids = result_rtes::get_relids_in_jointree(mcx, &sub_jt, true, false)?;
        let sub_expr = pathlike_bms_to_expr_relids(subrelids.as_deref());
        if last_ph_id(root) != 0 {
            result_rtes::substitute_phv_relids_in_query(mcx, parse, varno, &sub_expr);
        }
        result_rtes::fix_append_rel_relids(mcx, root, varno, subrelids.as_deref(), &sub_expr)?;
    }

    // And now add subquery's AppendRelInfos to our list.
    //
    // The `translated_vars` are `NodeId` handles into the *subroot's* node arena
    // (#274); in C they are plain `Node *` pointers that stay valid across the
    // list concat, but here root and subroot own *separate* arenas, so each
    // referenced Var must be re-interned into the parent root's arena and its
    // handle rewritten — otherwise the handle dangles (OOB) or silently resolves
    // to an unrelated arena slot in the parent.
    {
        let mut appinfos = core::mem::take(&mut subroot.append_rel_list);
        for ai in appinfos.iter_mut() {
            for id in ai.translated_vars.iter_mut() {
                if *id == NodeId::default() {
                    continue;
                }
                let expr = subroot.node(*id).clone_in(mcx)?;
                *id = root.alloc_node(expr);
            }
        }
        for ai in appinfos {
            root.append_rel_list.push(ai);
        }
    }

    // No bookkeeping needed for outer-join / placeholder lists (not set up yet).
    debug_assert!(root.join_info_list.is_empty());
    debug_assert!(subroot.join_info_list.is_empty());
    debug_assert!(root.placeholder_list.is_empty());
    debug_assert!(subroot.placeholder_list.is_empty());

    // We no longer need the RTE's copy of the subquery's query tree.
    parse.rtable[(varno - 1) as usize].subquery = None;

    // Miscellaneous housekeeping: copy subquery->hasSubLinks anyway (SubLinks
    // could be added via copied FUNCTION/VALUES RTE expressions).
    parse.hasSubLinks |= subquery.hasSubLinks;
    // If subquery had any RLS conditions, now main query does too.
    parse.hasRowSecurity |= subquery.hasRowSecurity;

    // Return the adjusted subquery jointree to replace the RangeTblRef entry; if
    // the FromExpr is degenerate, just return its single member.
    let mut jt = subquery
        .jointree
        .take()
        .expect("subquery has no jointree");
    if jt.quals.is_none() && jt.fromlist.len() == 1 {
        let only = core::mem::replace(&mut *jt.fromlist[0], dummy_node(mcx)?);
        return Ok(only);
    }
    Ok(Node::mk_from_expr(mcx, PgBox::into_inner(jt))?)
}

/// Convert an `'mcx` Bitmapset (the lifetime-free `types_pathnodes::Relids` form
/// returned by `result_rtes::get_relids_in_jointree`) to [`ExprRelids`].
fn pathlike_bms_to_expr_relids(a: Option<&Bitmapset>) -> ExprRelids {
    bms_to_expr_relids(a)
}

/// Clone the *shape* of a FromExpr for relids extraction (quals are not needed).
fn clone_fromexpr_shape<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FromExpr<'mcx>,
) -> PgResult<FromExpr<'mcx>> {
    let mut fromlist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for l in f.fromlist.iter() {
        fromlist.push(alloc_in(mcx, clone_jointree_shape(mcx, l)?)?);
    }
    Ok(FromExpr {
        fromlist,
        quals: None,
    })
}

/// A shallow clone of an RTE sufficient for the pullup callback (it reads
/// `lateral`, `rtekind`; `ReplaceVarFromTargetList` reads its kind/eref).
fn rte_shallow_clone<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> PgResult<RangeTblEntry<'mcx>> {
    let node = Node::mk_range_tbl_entry(mcx, rte.clone_in(mcx)?)?;
    Ok(node.into_rangetblentry().unwrap_or_else(|| unreachable!()))
}

/// Deep-clone a targetlist (owned `TargetEntry` values) into `mcx`.
fn clone_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut out: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve(tlist.len()).map_err(|_| mcx.oom(tlist.len()))?;
    for te in tlist.iter() {
        out.push(te.clone_in(mcx)?);
    }
    Ok(out)
}

/// `palloc0((list_length(tlist) + 1) * sizeof(Node *))` — indexes 0..=len.
fn rv_cache_new<'mcx>(tlist_len: usize) -> Vec<Option<Expr<'mcx>>> {
    let mut v = Vec::new();
    v.resize_with(tlist_len + 1, || None);
    v
}

/// `root->glob->lastPHId`.
fn last_ph_id(root: &PlannerInfo) -> u32 {
    root.glob.as_ref().map(|g| g.last_ph_id).unwrap_or(0)
}

// ===========================================================================
// FAMILY 3 — the simple-UNION-ALL pull-up.
//
// 1:1 port of `pull_up_simple_union_all` / `pull_up_union_leaf_queries` /
// `make_setop_translation_list` / `is_simple_union_all` /
// `is_simple_union_all_recurse` / `flatten_simple_union_all`
// (prepjointree.c:1608..3060) over the lifetime-free owned `Query<'mcx>` +
// embedded-`PgBox` model.
//
// Model notes:
//   * The subquery's `setOperations` tree (`SetOperationStmt`/`RangeTblRef`
//     nodes) lives in the embedded owned subquery; we walk it by deref exactly
//     as the C walks `Node *`. `RangeTblRef.rtindex` indexes the *setop query's*
//     own rtable (`rt_fetch(rtr->rtindex, setOpQuery->rtable)`), not the
//     parent's.
//   * `make_setop_translation_list` builds the `AppendRelInfo.translated_vars`
//     list. Here `translated_vars` is `Vec<NodeId>` arena handles (#274); each
//     `makeVarFromTargetEntry` Var is stored via `root.alloc_node(Expr::Var(..))`
//     and the resulting `NodeId` pushed (a dropped/junk parent column keeps the
//     C `palloc0` reverse-translation zero in `parent_colnos`; there is no entry
//     pushed to `translated_vars` for junk columns, matching `vars = lappend`).
// ===========================================================================

/// `pull_up_simple_union_all(root, jtnode, rte)` (prepjointree.c:1617). FAMILY 3.
///
/// `jtnode` is the `RangeTblRef` identified as a simple UNION ALL subquery; we
/// pull up the leaf subqueries and build an append relation for the union set.
/// The result is just `jtnode` (the query jointree is unchanged).
fn pull_up_simple_union_all<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    varno: i32,
) -> PgResult<Node<'mcx>> {
    // int rtoffset = list_length(root->parse->rtable);
    let rtoffset = parse.rtable.len() as i32;

    // C: `Query *subquery = rte->subquery;` — the appendrel parent RTE keeps its
    // `subquery` live; later planner stages (`extract_lateral_references` for a
    // LATERAL UNION ALL) call `pull_vars_of_level((Node *) rte->subquery, 1)` to
    // find the cross-level lateral Vars, so we MUST NOT null it out. Work on a
    // deep copy (copyObject) for the rtable/perminfo/setOperations mutations and
    // leave `rte->subquery` in place.
    let mut subquery: Query<'mcx> = parse.rtable[(varno - 1) as usize]
        .subquery
        .as_deref()
        .expect("RTE_SUBQUERY with NULL subquery")
        .clone_in(mcx)?;
    let rte_lateral = parse.rtable[(varno - 1) as usize].lateral;

    // Make a modifiable copy of the subquery's rtable, so we can adjust
    // upper-level Vars in it. C: `rtable = copyObject(subquery->rtable);`. We own
    // the (cloned) `subquery`, so move its rtable out into our working list — the
    // cloned `subquery` itself is discarded afterwards, while the original stays
    // attached to the RTE.
    let mut rtable: PgVec<'mcx, RangeTblEntry<'mcx>> =
        core::mem::replace(&mut subquery.rtable, PgVec::new_in(mcx));

    // Upper-level vars in subquery are now one level closer to their parent than
    // before. We don't have to worry about offsetting varnos, though, because the
    // UNION leaf queries can't cross-reference each other.
    IncrementVarSublevelsUp_rtable(&mut rtable, -1, 1, mcx)?;

    // If the UNION ALL subquery had a LATERAL marker, propagate that to all its
    // children.
    if rte_lateral {
        for child_rte in rtable.iter_mut() {
            debug_assert!(child_rte.rtekind == RTEKind::RTE_SUBQUERY);
            child_rte.lateral = true;
        }
    }

    // Append child RTEs (and their perminfos) to parent rtable.
    // C: CombineRangeTables(&root->parse->rtable, &root->parse->rteperminfos,
    //                       rtable, subquery->rteperminfos);
    // combine_range_tables consumes a `&mut Query` for the source; wrap the
    // modified rtable + the subquery's perminfos in a transient Query.
    {
        let mut src = Query::new(mcx);
        src.rtable = rtable;
        src.rteperminfos = core::mem::replace(&mut subquery.rteperminfos, PgVec::new_in(mcx));
        subselect::combine_range_tables(mcx, parse, &mut src);
    }

    // Recursively scan the subquery's setOperations tree and add AppendRelInfo
    // nodes for leaf subqueries to the parent's append_rel_list. Also apply
    // pull_up_subqueries to the leaf subqueries.
    debug_assert!(subquery.setOperations.is_some());
    let setop = subquery
        .setOperations
        .take()
        .expect("UNION ALL subquery has no setOperations");
    pull_up_union_leaf_queries(
        mcx,
        root,
        parse,
        &*setop,
        varno,
        &subquery,
        rtoffset,
    )?;

    // Mark the parent as an append relation.
    parse.rtable[(varno - 1) as usize].inh = true;

    Ok(jtnode)
}

/// `pull_up_union_leaf_queries(setOp, root, parentRTindex, setOpQuery,
/// childRToffset)` (prepjointree.c:1699). Recursive guts of
/// `pull_up_simple_union_all` / `flatten_simple_union_all`.
///
/// `set_op_query` is the Query containing the setOp node (whose tlist references
/// all the setop output columns); when called from `pull_up_simple_union_all`
/// this is *not* `root.parse`. `child_rt_offset` is where in the parent's range
/// table the child RTEs were copied (0 for the flatten path).
fn pull_up_union_leaf_queries<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    set_op: &Node<'mcx>,
    parent_rt_index: i32,
    set_op_query: &Query<'mcx>,
    child_rt_offset: i32,
) -> PgResult<()> {
    match set_op.node_tag() {
        ntag::T_RangeTblRef => {
            let rtr = set_op.expect_rangetblref();
            // Calculate the index in the parent's range table.
            let child_rt_index = child_rt_offset + rtr.rtindex;

            // Build a suitable AppendRelInfo, and attach to parent's list.
            let mut appinfo = types_pathnodes::AppendRelInfo {
                parent_relid: parent_rt_index as u32,
                child_relid: child_rt_index as u32,
                parent_reltype: 0,
                child_reltype: 0,
                translated_vars: Vec::new(),
                num_child_cols: 0,
                parent_colnos: Vec::new(),
                parent_reloid: 0,
            };
            make_setop_translation_list(root, set_op_query, child_rt_index, &mut appinfo);
            root.append_rel_list.push(appinfo);
            let appinfo_idx = root.append_rel_list.len() - 1;

            // Recursively apply pull_up_subqueries to the new child RTE. (We must
            // build the AppendRelInfo first, because this will modify it; indeed,
            // that's the only part of the upper query where Vars referencing
            // childRTindex can exist at this point.)
            //
            // We can pass NULL containing-join info even if actually under an
            // outer join, because the child's expressions don't propagate up to
            // the join. We ignore the possibility that the recurse returns a
            // different jointree node; the important thing is it replaced the
            // child relid in the AppendRelInfo node.
            let rtr = core::cell::RefCell::new(Node::mk_range_tbl_ref(
                mcx,
                types_nodes::rawnodes::RangeTblRef {
                    rtindex: child_rt_index,
                },
            )?);
            pull_up_subqueries_recurse(
                mcx,
                root,
                parse,
                &JtPath::Detached(&rtr),
                None,
                Some(appinfo_idx),
            )?;
            Ok(())
        }
        ntag::T_SetOperationStmt => {
            let op = set_op.expect_setoperationstmt();
            // Recurse to reach leaf queries.
            let larg = op
                .larg
                .as_ref()
                .expect("SetOperationStmt with NULL larg");
            pull_up_union_leaf_queries(
                mcx,
                root,
                parse,
                &**larg,
                parent_rt_index,
                set_op_query,
                child_rt_offset,
            )?;
            let rarg = op
                .rarg
                .as_ref()
                .expect("SetOperationStmt with NULL rarg");
            pull_up_union_leaf_queries(
                mcx,
                root,
                parse,
                &**rarg,
                parent_rt_index,
                set_op_query,
                child_rt_offset,
            )?;
            Ok(())
        }
        other => Err(types_error::PgError::error(alloc::format!(
            "unrecognized node type: {:?}",
            other
        ))),
    }
}

/// `make_setop_translation_list(query, newvarno, appinfo)` (prepjointree.c:1769).
///
/// Build the list of translations from parent Vars to child Vars for a UNION ALL
/// member, plus the trivial reverse-translation array. Each translated Var is
/// stored in the planner arena and referenced by `NodeId` (#274).
fn make_setop_translation_list<'mcx>(
    root: &mut PlannerInfo,
    query: &Query<'mcx>,
    newvarno: i32,
    appinfo: &mut types_pathnodes::AppendRelInfo,
) {
    // Initialize reverse-translation array with all entries zero. (Entries for
    // resjunk columns stay zero.)
    appinfo.num_child_cols = query.targetList.len() as i32;
    appinfo.parent_colnos = alloc::vec![0i16; query.targetList.len()];

    let mut vars: Vec<NodeId> = Vec::new();
    for tle in query.targetList.iter() {
        if tle.resjunk {
            continue;
        }
        let var = backend_nodes_core::makefuncs::make_var_from_target_entry(newvarno, tle)
            .expect("make_var_from_target_entry");
        let id = root.alloc_node(Expr::Var(var));
        vars.push(id);
        appinfo.parent_colnos[(tle.resno - 1) as usize] = tle.resno;
    }
    appinfo.translated_vars = vars;
}

/// `is_simple_union_all(subquery)` (prepjointree.c:2215). FAMILY 3.
///
/// We require all the setops to be UNION ALL (no mixing) and there can't be any
/// datatype coercions involved, ie, all the leaf queries must emit the same
/// datatypes.
fn is_simple_union_all(subquery: &Query) -> PgResult<bool> {
    // Let's just make sure it's a valid subselect. (commandType is the only
    // check we can make on the owned Query; the IsA(Query) is structural.)
    if subquery.commandType != types_nodes::nodes::CmdType::CMD_SELECT {
        return Err(types_error::PgError::error("subquery is bogus"));
    }

    // Is it a set-operation query at all?
    let topop_node = match subquery.setOperations.as_ref() {
        Some(n) => &**n,
        None => return Ok(false),
    };
    // castNode would error on a wrong tag; setOperations is always a
    // SetOperationStmt when present.
    let Some(topop) = topop_node.as_setoperationstmt() else {
        return Ok(false);
    };

    // Can't handle ORDER BY, LIMIT/OFFSET, locking, or WITH.
    if !subquery.sortClause.is_empty()
        || subquery.limitOffset.is_some()
        || subquery.limitCount.is_some()
        || !subquery.rowMarks.is_empty()
        || !subquery.cteList.is_empty()
    {
        return Ok(false);
    }

    // Recursively check the tree of set operations.
    is_simple_union_all_recurse(topop_node, subquery, &topop.colTypes)
}

/// `is_simple_union_all_recurse(setOp, setOpQuery, colTypes)`
/// (prepjointree.c:2242).
fn is_simple_union_all_recurse(
    set_op: &Node,
    set_op_query: &Query,
    col_types: &[types_core::primitive::Oid],
) -> PgResult<bool> {
    // Since this function recurses, it could be driven to stack overflow.
    backend_tcop_postgres_seams::check_stack_depth::call()?;

    match set_op.node_tag() {
        ntag::T_RangeTblRef => {
            let rtr = set_op.expect_rangetblref();
            // rt_fetch(rtr->rtindex, setOpQuery->rtable)
            let rte = &set_op_query.rtable[(rtr.rtindex - 1) as usize];
            let subquery = rte
                .subquery
                .as_deref()
                .expect("UNION ALL leaf RTE has NULL subquery");
            // Leaf nodes are OK if they match the toplevel column types. We don't
            // have to compare typmods or collations here.
            backend_optimizer_util_vars::tlist::tlist_same_datatypes(
                &subquery.targetList,
                col_types,
                true,
            )
        }
        ntag::T_SetOperationStmt => {
            let op = set_op.expect_setoperationstmt();
            // Must be UNION ALL.
            if op.op != types_nodes::rawnodes::SetOperation::SETOP_UNION || !op.all {
                return Ok(false);
            }
            // Recurse to check inputs.
            let larg = op.larg.as_ref().expect("SetOperationStmt with NULL larg");
            let rarg = op.rarg.as_ref().expect("SetOperationStmt with NULL rarg");
            Ok(
                is_simple_union_all_recurse(&**larg, set_op_query, col_types)?
                    && is_simple_union_all_recurse(&**rarg, set_op_query, col_types)?,
            )
        }
        other => Err(types_error::PgError::error(alloc::format!(
            "unrecognized node type: {:?}",
            other
        ))),
    }
}

/// `flatten_simple_union_all(root)` (prepjointree.c:2983). FAMILY 3.
///
/// If a query's `setOperations` tree consists entirely of simple UNION ALL
/// operations, flatten it into an append relation (which we can process more
/// intelligently than the general setops case). Otherwise, do nothing.
pub fn flatten_simple_union_all<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    // Shouldn't be called unless query has setops.
    debug_assert!(parse.setOperations.is_some());

    // Can't optimize away a recursive UNION.
    if root.hasRecursion {
        return Ok(());
    }

    // Recursively check the tree of set operations. If not all UNION ALL with
    // identical column types, punt.
    {
        let topop_node: &Node = &**parse
            .setOperations
            .as_ref()
            .expect("flatten_simple_union_all: no setOperations");
        let Some(topop) = topop_node.as_setoperationstmt() else {
            return Ok(());
        };
        if !is_simple_union_all_recurse(topop_node, parse, &topop.colTypes)? {
            return Ok(());
        }
    }

    // Locate the leftmost leaf query in the setops tree. The upper query's Vars
    // all refer to this RTE (see transformSetOperationStmt). Walk down `larg`.
    let leftmost_rti = {
        let mut node: &Node = &**parse
            .setOperations
            .as_ref()
            .expect("flatten_simple_union_all: no setOperations");
        loop {
            match node.node_tag() {
                ntag::T_SetOperationStmt => {
                    node = &**node
                        .expect_setoperationstmt()
                        .larg
                        .as_ref()
                        .expect("setop NULL larg");
                }
                ntag::T_RangeTblRef => break node.expect_rangetblref().rtindex,
                _ => panic!("flatten_simple_union_all: leftmost jtnode is not a RangeTblRef"),
            }
        }
    };
    debug_assert!(
        parse.rtable[(leftmost_rti - 1) as usize].rtekind == RTEKind::RTE_SUBQUERY
    );

    // Make a copy of the leftmost RTE and add it to the rtable. This copy
    // represents the leftmost leaf query in its capacity as a member of the
    // appendrel; the original represents the appendrel as a whole. (We must do
    // things this way because the upper query's Vars have to be seen as
    // referring to the whole appendrel.)
    let child_rte = parse.rtable[(leftmost_rti - 1) as usize].clone_in(mcx)?;
    parse.rtable.push(child_rte);
    let child_rti = parse.rtable.len() as i32;

    // Modify the setops tree to reference the child copy. We must walk down to
    // the leftmost RangeTblRef again and mutate its rtindex.
    {
        let mut node: &mut Node = &mut **parse
            .setOperations
            .as_mut()
            .expect("flatten_simple_union_all: no setOperations");
        loop {
            if node.is_setoperationstmt() {
                node = &mut **node
                    .as_setoperationstmt_mut()
                    .unwrap()
                    .larg
                    .as_mut()
                    .expect("setop NULL larg");
            } else if let Some(rtr) = node.as_rangetblref_mut() {
                rtr.rtindex = child_rti;
                break;
            } else {
                unreachable!();
            }
        }
    }

    // Modify the formerly-leftmost RTE to mark it as an appendrel parent.
    parse.rtable[(leftmost_rti - 1) as usize].inh = true;

    // Form a RangeTblRef for the appendrel, and insert it into FROM. The top
    // Query of a setops tree should have had an empty FromClause initially.
    let rtr_node = Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef {
        rtindex: leftmost_rti,
    })?;
    {
        let jt = parse
            .jointree
            .as_mut()
            .expect("flatten_simple_union_all: no jointree");
        debug_assert!(jt.fromlist.is_empty());
        jt.fromlist.push(alloc_in(mcx, rtr_node)?);
    }

    // Pull the setOperations tree out before subquery pullup (because of the
    // assert in pull_up_simple_subquery). We still need to walk it to build the
    // AppendRelInfos, so keep it in a local.
    let topop = parse
        .setOperations
        .take()
        .expect("flatten_simple_union_all: no setOperations");

    // Build AppendRelInfo information, and apply pull_up_subqueries to the leaf
    // queries of the UNION ALL. We must do that now because they weren't
    // previously referenced by the jointree, and so were missed by the main
    // invocation of pull_up_subqueries. (childRToffset is 0; the child RTEs were
    // already in parse->rtable.) The setOpQuery here is `parse` itself; we hold
    // `topop` separately so there's no aliasing with the `&mut parse` walk.
    let topop_node = PgBox::into_inner(topop);
    flatten_pull_up_union_leaf_queries(mcx, root, parse, &topop_node, leftmost_rti, 0)
}

/// `pull_up_union_leaf_queries((Node *) topop, root, leftmostRTI, parse, 0)`
/// for the flatten path: here `setOpQuery == root->parse == parse`, but we
/// cannot borrow `parse` both as the mutated target and as the immutable
/// `set_op_query` arg. The only use of `set_op_query` is reading its
/// `targetList` in `make_setop_translation_list` (via the leftmost-leaf tlist
/// reference). We snapshot the needed targetlist by cloning it once up front.
fn flatten_pull_up_union_leaf_queries<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    set_op: &Node<'mcx>,
    parent_rt_index: i32,
    child_rt_offset: i32,
) -> PgResult<()> {
    // Snapshot parse->targetList (the setOpQuery tlist) once; make_setop_*
    // reads only the tlist. This avoids aliasing the &mut parse.
    let tlist_snapshot = clone_targetlist(mcx, &parse.targetList)?;
    let mut snapshot_query = Query::new(mcx);
    snapshot_query.commandType = parse.commandType;
    snapshot_query.targetList = tlist_snapshot;
    pull_up_union_leaf_queries(
        mcx,
        root,
        parse,
        set_op,
        parent_rt_index,
        &snapshot_query,
        child_rt_offset,
    )
}

// ===========================================================================
// FAMILY 6 — the RTE expanders (preprocess_function_rtes /
// expand_virtual_generated_columns / pull_up_simple_values /
// is_simple_values / pull_up_constant_function).
// ===========================================================================

/// `preprocess_function_rtes(root)` (prepjointree.c:914). Const-simplify each
/// `RTE_FUNCTION`'s `functions` list and, where possible, inline a
/// set-returning function into a subquery RTE.
pub fn preprocess_function_rtes<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let n = parse.rtable.len();
    for i in 0..n {
        if parse.rtable[i].rtekind != RTEKind::RTE_FUNCTION {
            continue;
        }

        // Apply const-simplification.
        //   rte->functions = (List *) eval_const_expressions(root, (Node *) rte->functions);
        // The C folds the whole `functions` list as one node tree; here the list
        // is a typed `Vec<NodePtr>` of `RangeTblFunction` nodes, so fold each
        // RangeTblFunction's `funcexpr` (the only fold-able subtree it carries).
        {
            let m = parse.rtable[i].functions.len();
            for k in 0..m {
                eval_const_expressions_in_rtfunc(mcx, &mut parse.rtable[i].functions[k])?;
            }
        }

        // Check safety of expansion, and expand if possible. The C
        // `inline_set_returning_function(root, rte)` reads the RTE's single
        // FuncExpr + its pg_proc row and (for an inlinable SQL-language SRF)
        // returns the inlined query; that whole gate ladder + inline core rides
        // the clauses.c SRF-inliner seam (the function-call node universe + the
        // SQL-function parse/rewrite path are not reachable here).
        let funcquery = {
            // Snapshot the RTE for the seam (a shallow clone is enough; the seam
            // reads rtekind/funcordinality/functions).
            let rte_snapshot = rte_shallow_clone(mcx, &parse.rtable[i])?;
            backend_optimizer_util_clauses_seams::inline_set_returning_function::call(
                mcx,
                root,
                &rte_snapshot,
            )?
        };

        if let Some(funcquery) = funcquery {
            // Successful expansion, convert the RTE to a subquery.
            let rte = &mut parse.rtable[i];
            rte.rtekind = RTEKind::RTE_SUBQUERY;
            rte.subquery = Some(alloc_in(mcx, funcquery)?);
            rte.security_barrier = false;

            // Clear fields that should not be set in a subquery RTE. We leave
            // rte->functions filled in for the moment, in case makeWholeRowVar
            // needs to consult it; setrefs.c clears it later.
            rte.funcordinality = false;
        }
    }
    Ok(())
}

/// `eval_const_expressions(root, (Node *) rte->functions)` over a single
/// `RangeTblFunction` node, folding its `funcexpr`. The C folds the whole list
/// at once; per-element folding is the faithful analogue for the typed list.
fn eval_const_expressions_in_rtfunc<'mcx>(
    mcx: Mcx<'mcx>,
    func: &mut NodePtr<'mcx>,
) -> PgResult<()> {
    if let Some(rtf) = func.as_rangetblfunction_mut() {
        if let Some(fe) = rtf.funcexpr.take() {
            // funcexpr is a Node holding an Expr; fold the Expr.
            let node = PgBox::into_inner(fe);
            if node.is_expr() {
                let e = node.into_expr().expect("is_expr implies into_expr");
                let folded = backend_optimizer_util_clauses::fold::eval_const_expressions(mcx, e)?;
                rtf.funcexpr = Some(alloc_in(mcx, Node::mk_expr(mcx, folded)?)?);
            } else {
                rtf.funcexpr = Some(alloc_in(mcx, node)?);
            }
        }
    }
    Ok(())
}

/// `expand_virtual_generated_columns(root)` (prepjointree.c:969). Scan the
/// rangetable for relations with virtual generated columns and replace all Var
/// nodes referencing those columns with their generation expressions.
///
/// Returns a (possibly) modified copy of the query (the C returns `parse`,
/// wholesale-replaced by `pullup_replace_vars` whenever any relation expands).
pub fn expand_virtual_generated_columns<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    mut parse: Query<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut rt_index = 0i32;
    let n = parse.rtable.len();
    for lc in 0..n {
        rt_index += 1;

        // Only normal relations can have virtual generated columns.
        if parse.rtable[lc].rtekind != RTEKind::RTE_RELATION {
            continue;
        }

        let relid = parse.rtable[lc].relid;

        // `table_open(rte->relid, NoLock)` + `RelationGetDescr` + the per-attr
        // `build_generation_expression` / `makeVar` targetlist construction ride
        // the relcache + rewriter seam (build_generation_expression is unported,
        // and the bare loop cannot prove the no-op without opening the relation).
        // Returns None when the relation has no virtual generated columns.
        let tlist = backend_optimizer_prep_prepjointree_seams::build_virtual_generated_columns_tlist::call(
            mcx, root, relid, rt_index,
        )?;

        let tlist = match tlist {
            None => continue,
            Some(t) => t,
        };

        debug_assert!(!tlist.is_empty());
        debug_assert!(!parse.rtable[lc].lateral);

        // The relation's targetlist items are now in the appropriate form to
        // insert into the query, except that we may need to wrap them in
        // PlaceHolderVars. Set up required context data for pullup_replace_vars.
        let target_rte: PgBox<'mcx, RangeTblEntry<'mcx>> =
            alloc_in(mcx, rte_shallow_clone(mcx, &parse.rtable[lc])?)?;

        let tlist_len = tlist.len();
        let mut rvcontext = PullupReplaceVarsContext {
            targetlist: tlist,
            target_rte,
            result_relation: parse.resultRelation,
            // won't need these values
            relids: None,
            nullinfo: None,
            varno: rt_index,
            // this flag will be set below, if needed
            wrap_option: ReplaceWrapOption::None,
            rv_cache: rv_cache_new(tlist_len),
        };

        // If the query uses grouping sets, we need a PlaceHolderVar for each
        // expression of the relation's targetlist items.
        if !parse.groupingSets.is_empty() {
            rvcontext.wrap_option = ReplaceWrapOption::All;
        }

        // Apply pullup variable replacement throughout the query tree.
        // (pass NULL for outer_hasSubLinks)
        let mut outer_has_sublinks: Option<bool> = None;
        perform_pullup_replace_vars(
            mcx,
            root,
            &mut parse,
            &mut rvcontext,
            &mut outer_has_sublinks,
            None,
        )?;

        // table_close(rel, NoLock) — handled inside the seam.
    }

    Ok(parse)
}

/// The relcache leg of `expand_virtual_generated_columns` (prepjointree.c:993):
/// `table_open(rte->relid, NoLock)` + `RelationGetDescr`, then — only if
/// `tupdesc->constr->has_generated_virtual` — build the per-attribute
/// replacement targetlist. Returns `Ok(None)` for the common no-virtual-
/// generated-columns case (the early `table_close` + skip in C).
///
/// The tlist construction calls `build_generation_expression`
/// (rewriteHandler.c) per VIRTUAL generated column (via the
/// `build_generation_expression` seam) and `makeVar` for the rest, then
/// `ChangeVarNodes` remaps the generation expressions onto this RTE's index —
/// faithful to prepjointree.c:1004-1043.
pub(crate) fn build_virtual_generated_columns_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    _root: &mut PlannerInfo,
    relid: types_core::primitive::Oid,
    rt_index: i32,
) -> PgResult<Option<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>>> {
    // rel = table_open(rte->relid, NoLock);
    let rel = backend_utils_cache_relcache_seams::relation_id_get_relation::call(mcx, relid)?
        .expect("expand_virtual_generated_columns: rangetable relation must exist in relcache");

    // tupdesc = RelationGetDescr(rel);
    // if (!tupdesc->constr || !tupdesc->constr->has_generated_virtual) { skip }
    let has_virtual = rel
        .rd_att
        .constr
        .as_ref()
        .is_some_and(|c| c.has_generated_virtual);

    if !has_virtual {
        // table_close(rel, NoLock); continue;  — no virtual generated columns.
        // `relation_id_get_relation` took the `RelationIncrementReferenceCount`
        // pin but handed back a value-slice copy (no RAII closer), so release
        // it explicitly here to match C's `table_close(rel, NoLock)`. Without
        // this the pin leaks and a later `CheckTableNotInUse` (DROP/TRUNCATE)
        // sees the relation as still in use.
        backend_utils_cache_relcache_seams::relation_close::call(relid)?;
        return Ok(None);
    }

    // The relation has virtual generated columns. Build the per-attribute
    // replacement targetlist (prepjointree.c:1004-1043):
    //   for (i = 0; i < tupdesc->natts; i++)
    //     if attr->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL:
    //       defexpr = build_generation_expression(rel, i + 1);
    //       ChangeVarNodes(defexpr, 1, rt_index, 0);
    //       tle = makeTargetEntry(defexpr, i + 1, 0, false);
    //     else:
    //       var = makeVar(rt_index, i + 1, atttypid, atttypmod, attcollation, 0);
    //       tle = makeTargetEntry(var, i + 1, 0, false);
    //
    // `build_generation_expression` (rewriteHandler.c) is owned by the rewriter;
    // it rides the `build_generation_expression` seam (installed by the
    // rewriteHandler unit) so the planner needs no direct rewriter dependency.
    // The seam wants a `types_rel::Relation`; wrap the projected `RelationData`
    // (no closer — the explicit `relation_close::call` below releases the pin).
    let natts = rel.rd_att.natts as usize;
    let relation = types_rel::Relation::open(rel, None);

    let mut tlist: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    tlist.try_reserve(natts).map_err(|_| mcx.oom(natts))?;

    for i in 0..natts {
        let attr = relation.rd_att.attr(i);
        let attrno = (i + 1) as AttrNumber;

        if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            // defexpr = build_generation_expression(rel, i + 1);
            let defbox = backend_rewrite_rewritehandler_seams::build_generation_expression::call(
                mcx,
                &relation,
                (i + 1) as i32,
            )?;
            // The seam returns the generation expr in the parser/rewrite arena
            // ('static); localize into the run `mcx` for the in-place remap.
            let defexpr = PgBox::into_inner(defbox).clone_in(mcx)?;

            // ChangeVarNodes(defexpr, 1, rt_index, 0) — the generation
            // expression's Vars reference rt_index 1 (build_column_default emits
            // a single-relation expression); remap them onto this RTE's index.
            let mut defnode = Node::mk_expr(mcx, defexpr)?;
            backend_rewrite_core::ChangeVarNodes(&mut defnode, 1, rt_index, 0, mcx);
            let defexpr = defnode
                .into_expr()
                .unwrap_or_else(|| unreachable!("ChangeVarNodes preserves the node kind"));

            tlist.push(make_target_entry(mcx, defexpr, attrno, None, false)?);
        } else {
            // var = makeVar(rt_index, i + 1, atttypid, atttypmod, attcollation, 0);
            let var = backend_nodes_core::makefuncs::make_var(
                rt_index,
                attrno,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                0,
            );
            tlist.push(make_target_entry(mcx, Expr::Var(var), attrno, None, false)?);
        }
    }

    debug_assert!(!tlist.is_empty());

    // table_close(rel, NoLock) — release the relcache pin taken above.
    backend_utils_cache_relcache_seams::relation_close::call(relid)?;

    Ok(Some(tlist))
}

/// `pull_up_simple_values(root, jtnode, rte)` (prepjointree.c:1947).
fn pull_up_simple_values<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    varno: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert_eq!(parse.rtable[(varno - 1) as usize].rtekind, RTEKind::RTE_VALUES);
    debug_assert_eq!(parse.rtable[(varno - 1) as usize].values_lists.len(), 1);

    // Need a modifiable copy of the VALUES list to hack on, just in case it's
    // multiply referenced. `linitial(rte->values_lists)` is a `Node::List` of
    // expressions; copy it.
    let values_list: PgVec<'mcx, NodePtr<'mcx>> = {
        let first = &parse.rtable[(varno - 1) as usize].values_lists[0];
        if let Some(items) = first.as_list() {
            let mut out: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            out.try_reserve(items.len()).map_err(|_| mcx.oom(items.len()))?;
            for it in items.iter() {
                out.push(alloc_in(mcx, it.clone_in(mcx)?)?);
            }
            out
        } else {
            // Single-expression VALUES row carried as a bare node.
            let mut out: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            out.push(alloc_in(mcx, (**first).clone_in(mcx)?)?);
            out
        }
    };

    // The VALUES RTE can't contain any Vars of level zero, let alone any that
    // are join aliases, so no need to flatten join alias Vars.
    debug_assert!(!nodelist_contains_vars_of_level(&values_list, 0));

    // Set up required context data for pullup_replace_vars. In particular, we
    // have to make the VALUES list look like a subquery targetlist.
    let mut tlist: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    tlist.try_reserve(values_list.len()).map_err(|_| mcx.oom(values_list.len()))?;
    let mut attrno: i32 = 1;
    for item in values_list.iter() {
        let expr = match item.as_expr() {
            Some(e) => e.clone_in(mcx)?,
            None => {
                return Err(types_error::PgError::error(
                    "pull_up_simple_values: VALUES item is not an expression",
                ));
            }
        };
        tlist.push(make_target_entry(mcx, expr, attrno as AttrNumber, None, false)?);
        attrno += 1;
    }

    let target_rte: PgBox<'mcx, RangeTblEntry<'mcx>> =
        alloc_in(mcx, rte_shallow_clone(mcx, &parse.rtable[(varno - 1) as usize])?)?;

    let tlist_len = tlist.len();
    let mut rvcontext = PullupReplaceVarsContext {
        targetlist: tlist,
        target_rte,
        result_relation: 0,
        // can't be any lateral references here
        relids: None,
        nullinfo: None,
        varno,
        wrap_option: ReplaceWrapOption::None,
        rv_cache: rv_cache_new(tlist_len),
    };

    // outer_hasSubLinks is &parse->hasSubLinks.
    let mut outer_has_sublinks: Option<bool> = Some(parse.hasSubLinks);

    // Replace all of the top query's references to the RTE's outputs with copies
    // of the adjusted VALUES expressions, being careful not to replace any of the
    // jointree structure. We can assume there's no outer joins or appendrels in
    // the dummy Query that surrounds a VALUES RTE.
    perform_pullup_replace_vars(
        mcx,
        root,
        parse,
        &mut rvcontext,
        &mut outer_has_sublinks,
        None,
    )?;
    if let Some(v) = outer_has_sublinks {
        parse.hasSubLinks = v;
    }

    // There should be no appendrels to fix, nor any outer joins and hence no
    // PlaceHolderVars.
    debug_assert!(root.append_rel_list.is_empty());

    // Replace the VALUES RTE with a RESULT RTE. The VALUES RTE is the only rtable
    // entry in the current query level, so this is easy.
    debug_assert_eq!(parse.rtable.len(), 1);

    // Create suitable RTE.
    let mut new_rte = RangeTblEntry::new_in(mcx);
    new_rte.rtekind = RTEKind::RTE_RESULT;
    new_rte.eref = Some(alloc_in(mcx, make_alias(mcx, "*RESULT*")?)?);

    // Replace rangetable.
    let mut new_rtable: PgVec<'mcx, RangeTblEntry<'mcx>> = PgVec::new_in(mcx);
    new_rtable.push(new_rte);
    parse.rtable = new_rtable;

    // We could manufacture a new RangeTblRef, but the one we have is fine.
    debug_assert_eq!(varno, 1);

    Ok(jtnode)
}

/// `is_simple_values(root, rte)` (prepjointree.c:2044). Check a VALUES RTE in
/// the range table to see if it's simple enough to pull up into the parent.
fn is_simple_values<'mcx>(root: &PlannerInfo, parse: &Query<'mcx>, rte: &RangeTblEntry<'mcx>) -> bool {
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_VALUES);

    // There must be exactly one VALUES list, else it's not semantically correct
    // to replace the VALUES RTE with a RESULT RTE, nor would we have a unique set
    // of expressions to substitute into the parent query.
    if rte.values_lists.len() != 1 {
        return false;
    }

    // Because VALUES can't appear under an outer join (or at least, we won't try
    // to pull it up if it does), we need not worry about LATERAL, nor about
    // validity of PHVs for the VALUES' outputs.

    // Don't pull up a VALUES that contains any set-returning or volatile
    // functions. The considerations here are basically identical to the
    // restrictions on a pull-able subquery's targetlist.
    if nodelist_expression_returns_set(&rte.values_lists)
        || nodelist_contain_volatile_functions(&rte.values_lists).unwrap_or(true)
    {
        return false;
    }

    // Do not pull up a VALUES that's not the only RTE in its parent query. This
    // is actually the only case that the parser will generate at the moment, and
    // assuming this is true greatly simplifies pull_up_simple_values().
    let _ = root;
    if parse.rtable.len() != 1 {
        return false;
    }
    // `rte != (RangeTblEntry *) linitial(root->parse->rtable)` — identity check;
    // since rtable.len()==1, the only RTE is the first one, and the caller passed
    // the sole RTE, so the identity always holds here.

    true
}

/// `pull_up_constant_function(root, jtnode, rte, containing_appendrel)`
/// (prepjointree.c:2103). Pull up an `RTE_FUNCTION` expression that was
/// simplified to a constant.
fn pull_up_constant_function<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    varno: i32,
    containing_appendrel: Option<usize>,
) -> PgResult<Node<'mcx>> {
    // Fail if the RTE has ORDINALITY — we don't implement that here.
    if parse.rtable[(varno - 1) as usize].funcordinality {
        return Ok(jtnode);
    }

    // Fail if RTE isn't a single, simple Const expr.
    if parse.rtable[(varno - 1) as usize].functions.len() != 1 {
        return Ok(jtnode);
    }

    // `rtf = linitial_node(RangeTblFunction, rte->functions)`; read its fields.
    let (funcexpr, funccolcount, has_colnames): (Expr, i32, bool) = {
        let func0 = &parse.rtable[(varno - 1) as usize].functions[0];
        let Some(rtf) = func0.as_rangetblfunction() else {
            return Err(types_error::PgError::error(
                "pull_up_constant_function: RTE function is not a RangeTblFunction",
            ));
        };
        // `if (!IsA(rtf->funcexpr, Const)) return jtnode;`
        let fe_node = match rtf.funcexpr.as_deref() {
            Some(n) => n,
            None => return Ok(jtnode),
        };
        let Some(fe) = fe_node.as_expr() else {
            return Ok(jtnode);
        };
        if !matches!(fe, Expr::Const(_)) {
            return Ok(jtnode);
        }
        (fe.clone_in(mcx)?, rtf.funccolcount, !rtf.funccolnames.is_empty())
    };

    // If the function's result is not a scalar, we punt.
    if funccolcount != 1 {
        return Ok(jtnode); // definitely composite
    }

    // If it has a coldeflist, it certainly returns RECORD.
    if has_colnames {
        return Ok(jtnode); // must be a one-column RECORD type
    }

    // `functypclass = get_expr_result_type(rtf->funcexpr, &funcrettype, &tupdesc);`
    let resolved = backend_utils_fmgr_funcapi::result_type::get_expr_result_type(
        mcx,
        Some(&Node::mk_expr(mcx, funcexpr.clone_in(mcx)?)?),
    )?;
    if resolved.class != Some(types_nodes::funcapi::TypeFuncClass::Scalar) {
        return Ok(jtnode); // must be a one-column composite type
    }

    // Create context for applying pullup_replace_vars.
    let mut tlist: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    tlist.push(make_target_entry(
        mcx,
        funcexpr,
        1, /* resno */
        None, /* resname */
        false, /* resjunk */
    )?);

    let target_rte: PgBox<'mcx, RangeTblEntry<'mcx>> =
        alloc_in(mcx, rte_shallow_clone(mcx, &parse.rtable[(varno - 1) as usize])?)?;

    let tlist_len = tlist.len();
    let mut rvcontext = PullupReplaceVarsContext {
        targetlist: tlist,
        target_rte,
        result_relation: 0,
        // Since this function was reduced to a Const, it doesn't contain any
        // lateral references, even if it's marked as LATERAL.
        relids: None,
        nullinfo: None,
        varno,
        wrap_option: ReplaceWrapOption::None,
        rv_cache: rv_cache_new(tlist_len),
    };

    // If the parent query uses grouping sets, we need a PlaceHolderVar for each
    // expression of the subquery's targetlist items.
    if !parse.groupingSets.is_empty() {
        rvcontext.wrap_option = ReplaceWrapOption::All;
    }

    // Replace all of the top query's references to the RTE's output with copies
    // of the funcexpr, being careful not to replace any of the jointree
    // structure.
    let mut outer_has_sublinks: Option<bool> = Some(parse.hasSubLinks);
    perform_pullup_replace_vars(
        mcx,
        root,
        parse,
        &mut rvcontext,
        &mut outer_has_sublinks,
        containing_appendrel,
    )?;
    if let Some(v) = outer_has_sublinks {
        parse.hasSubLinks = v;
    }

    // Convert the RTE to be RTE_RESULT type, signifying that we don't need to
    // scan it anymore, and zero out RTE_FUNCTION-specific fields. Also make sure
    // the RTE is not marked LATERAL, since elsewhere we don't expect RTE_RESULTs
    // to be LATERAL.
    {
        let rte = &mut parse.rtable[(varno - 1) as usize];
        rte.rtekind = RTEKind::RTE_RESULT;
        rte.functions = PgVec::new_in(mcx);
        rte.lateral = false;
    }

    // We can reuse the RangeTblRef node.
    Ok(jtnode)
}

/// `makeTargetEntry((Expr *) expr, resno, resname, resjunk)` (makefuncs.c).
fn make_target_entry<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr<'mcx>,
    resno: AttrNumber,
    resname: Option<&str>,
    resjunk: bool,
) -> PgResult<types_nodes::primnodes::TargetEntry<'mcx>> {
    Ok(types_nodes::primnodes::TargetEntry {
        expr: Some(alloc_in(mcx, expr)?),
        resno,
        resname: match resname {
            Some(s) => Some(mcx::PgString::from_str_in(s, mcx)?),
            None => None,
        },
        ressortgroupref: 0,
        resorigtbl: types_core::primitive::Oid::default(),
        resorigcol: 0,
        resjunk,
    })
}

/// `makeAlias(aliasname, NIL)` (makefuncs.c) — an `Alias` with the given name and
/// no column aliases.
fn make_alias<'mcx>(mcx: Mcx<'mcx>, aliasname: &str) -> PgResult<types_nodes::rawnodes::Alias<'mcx>> {
    Ok(types_nodes::rawnodes::Alias {
        aliasname: Some(mcx::PgString::from_str_in(aliasname, mcx)?),
        colnames: PgVec::new_in(mcx),
    })
}

/// `expression_returns_set((Node *) list)` over a `Vec<NodePtr>` (a VALUES
/// list-of-lists). The walker recurses into `List` nodes.
fn nodelist_expression_returns_set(list: &PgVec<NodePtr>) -> bool {
    for item in list.iter() {
        if node_expression_returns_set(item) {
            return true;
        }
    }
    false
}

fn node_expression_returns_set(node: &Node) -> bool {
    if let Some(items) = node.as_list() {
        for it in items.iter() {
            if node_expression_returns_set(it) {
                return true;
            }
        }
        false
    } else if let Some(e) = node.as_expr() {
        backend_nodes_core::nodefuncs::expression_returns_set(Some(e))
    } else {
        false
    }
}

/// `contain_volatile_functions((Node *) list)` over a `Vec<NodePtr>` (a VALUES
/// list-of-lists).
fn nodelist_contain_volatile_functions(list: &PgVec<NodePtr>) -> PgResult<bool> {
    for item in list.iter() {
        if contain_volatile_functions_node(item)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `contain_vars_of_level((Node *) list, level)` over a `Vec<NodePtr>`.
fn nodelist_contains_vars_of_level(list: &PgVec<NodePtr>, level: i32) -> bool {
    for item in list.iter() {
        if node_contains_vars_of_level(item, level) {
            return true;
        }
    }
    false
}

fn node_contains_vars_of_level(node: &Node, level: i32) -> bool {
    if let Some(items) = node.as_list() {
        for it in items.iter() {
            if node_contains_vars_of_level(it, level) {
                return true;
            }
        }
        false
    } else {
        contain_vars_of_level(node, level)
    }
}

// ===========================================================================
// is_simple_subquery (prepjointree.c:1807)
// ===========================================================================

/// `is_simple_subquery(root, subquery, rte, lowest_outer_join)`
/// (prepjointree.c:1807).
fn is_simple_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    _parse: &Query<'mcx>,
    subquery: &Query<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    lowest_outer_join: Option<&LowestOuterJoin<'mcx>>,
) -> PgResult<bool> {
    // Let's just make sure it's a valid subselect.
    if subquery.commandType != types_nodes::nodes::CmdType::CMD_SELECT {
        return Err(types_error::PgError::error("subquery is bogus"));
    }

    // Can't pull up a query with setops (unless simple UNION ALL, handled
    // elsewhere).
    if subquery.setOperations.is_some() {
        return Ok(false);
    }

    // Can't pull up grouping/aggregation/SRFs/sorting/limiting/WITH, nor
    // explicit FOR UPDATE/SHARE.
    if subquery.hasAggs
        || subquery.hasWindowFuncs
        || subquery.hasTargetSRFs
        || !subquery.groupClause.is_empty()
        || !subquery.groupingSets.is_empty()
        || subquery.havingQual.is_some()
        || !subquery.sortClause.is_empty()
        || !subquery.distinctClause.is_empty()
        || subquery.limitOffset.is_some()
        || subquery.limitCount.is_some()
        || subquery.hasForUpdate
        || !subquery.cteList.is_empty()
    {
        return Ok(false);
    }

    // Don't pull up a security-barrier view.
    if rte.security_barrier {
        return Ok(false);
    }

    // If the subquery is LATERAL, check pullup restrictions.
    if rte.lateral {
        let restricted: bool;
        let safe_upper_varnos: Relids<'mcx>;
        if let Some(loj) = lowest_outer_join {
            restricted = true;
            safe_upper_varnos =
                result_rtes::get_relids_in_jointree(mcx, &loj.node, true, true)?;
        } else {
            restricted = false;
            safe_upper_varnos = None; // doesn't matter
        }

        let sub_jt = Node::mk_from_expr(mcx, clone_fromexpr_shape(
            mcx,
            subquery.jointree.as_deref().expect("subquery has no jointree"),
        )?)?;
        if jointree_contains_lateral_outer_refs(
            mcx,
            root,
            &sub_jt,
            restricted,
            safe_upper_varnos.as_deref(),
        )? {
            return Ok(false);
        }

        // If there's an outer join above the LATERAL subquery, also disallow
        // pullup if the subquery's targetlist references rels outside it.
        if lowest_outer_join.is_some() {
            let tlist_node = targetlist_as_node(mcx, &subquery.targetList)?;
            let lvarnos = pull_varnos_of_level(Some(root), &tlist_node, 1);
            if !pathrelids_is_subset_of_bms(&lvarnos, safe_upper_varnos.as_deref()) {
                return Ok(false);
            }
        }
    }

    // Don't pull up a subquery with any volatile functions in its targetlist.
    let tlist_node = targetlist_as_node(mcx, &subquery.targetList)?;
    if let Some(e) = tlist_node.as_expr() {
        if contain_volatile_functions(Some(e))? {
            return Ok(false);
        }
    } else if contain_volatile_functions_node(&tlist_node)? {
        return Ok(false);
    }

    Ok(true)
}

/// Wrap a targetlist (owned `TargetEntry`s) as a `Node::List` of its `expr`s,
/// for the var.c walkers that take `(Node *) subquery->targetList`.
fn targetlist_as_node<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mut items: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    items.try_reserve(tlist.len()).map_err(|_| mcx.oom(tlist.len()))?;
    for te in tlist.iter() {
        if let Some(expr) = te.expr.as_deref() {
            items.push(alloc_in(mcx, Node::mk_expr(mcx, expr.clone_in(mcx)?)?)?);
        }
    }
    Ok(Node::mk_list(mcx, items)?)
}

/// `contain_volatile_functions` over a `Node` that may be a `List`.
fn contain_volatile_functions_node(node: &Node) -> PgResult<bool> {
    if let Some(items) = node.as_list() {
        for it in items.iter() {
            if contain_volatile_functions_node(it)? {
                return Ok(true);
            }
        }
        Ok(false)
    } else if let Some(e) = node.as_expr() {
        contain_volatile_functions(Some(e))
    } else {
        Ok(false)
    }
}

// ===========================================================================
// is_safe_append_member (prepjointree.c:2286)
// ===========================================================================

/// `is_safe_append_member(subquery)` (prepjointree.c:2286).
fn is_safe_append_member(subquery: &Query) -> bool {
    let jt = subquery
        .jointree
        .as_deref()
        .expect("is_safe_append_member: no jointree");

    // Completely-empty case.
    if jt.fromlist.is_empty() && jt.quals.is_none() {
        return true;
    }

    // The more general case: walk down single-child FromExprs.
    let mut node: &Node = {
        // The top is the FromExpr itself; check its quals/length first.
        if jt.quals.is_some() {
            return false;
        }
        if jt.fromlist.len() != 1 {
            return false;
        }
        &jt.fromlist[0]
    };
    loop {
        match node.node_tag() {
            ntag::T_FromExpr => {
                let f = node.expect_fromexpr();
                if f.quals.is_some() {
                    return false;
                }
                if f.fromlist.len() != 1 {
                    return false;
                }
                node = &f.fromlist[0];
            }
            ntag::T_RangeTblRef => return true,
            _ => return false,
        }
    }
}

// ===========================================================================
// jointree_contains_lateral_outer_refs (prepjointree.c:2334)
// ===========================================================================

/// `jointree_contains_lateral_outer_refs(root, jtnode, restricted,
/// safe_upper_varnos)` (prepjointree.c:2334).
fn jointree_contains_lateral_outer_refs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    jtnode: &Node<'mcx>,
    restricted: bool,
    safe_upper_varnos: Option<&Bitmapset>,
) -> PgResult<bool> {
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => Ok(false),
        ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            // First, recurse to check child joins.
            for l in f.fromlist.iter() {
                if jointree_contains_lateral_outer_refs(
                    mcx,
                    root,
                    l,
                    restricted,
                    safe_upper_varnos,
                )? {
                    return Ok(true);
                }
            }
            // Then check the top-level quals.
            if restricted {
                let v = pull_varnos_of_level_qual(root, &f.quals);
                if !pathrelids_is_subset_of_bms(&v, safe_upper_varnos) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            // If this is an outer join, disallow any upper lateral references in
            // or below it.
            let (restricted, safe_upper_varnos): (bool, Option<&Bitmapset>) =
                if j.jointype != JoinType::JOIN_INNER {
                    (true, None)
                } else {
                    (restricted, safe_upper_varnos)
                };

            if let Some(larg) = j.larg.as_deref() {
                if jointree_contains_lateral_outer_refs(
                    mcx,
                    root,
                    larg,
                    restricted,
                    safe_upper_varnos,
                )? {
                    return Ok(true);
                }
            }
            if let Some(rarg) = j.rarg.as_deref() {
                if jointree_contains_lateral_outer_refs(
                    mcx,
                    root,
                    rarg,
                    restricted,
                    safe_upper_varnos,
                )? {
                    return Ok(true);
                }
            }

            if restricted {
                let v = pull_varnos_of_level_qual(root, &j.quals);
                if !pathrelids_is_subset_of_bms(&v, safe_upper_varnos) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// `pull_varnos_of_level(root, quals, 1)` over an `Option<NodePtr>` qual.
fn pull_varnos_of_level_qual(root: &PlannerInfo, quals: &Option<NodePtr>) -> PathRelids {
    match quals.as_deref() {
        None => None,
        Some(n) => pull_varnos_of_level(Some(root), n, 1),
    }
}

// ===========================================================================
// perform_pullup_replace_vars (prepjointree.c:2409)
// ===========================================================================

/// `perform_pullup_replace_vars(root, rvcontext, containing_appendrel)`
/// (prepjointree.c:2409).
fn perform_pullup_replace_vars<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
    containing_appendrel: Option<usize>,
) -> PgResult<()> {
    // If pulling up an appendrel child subquery, the only part of the upper
    // query that could reference the child yet is the AppendRelInfo's
    // translated_vars. Don't force PHVs (no outer join between).
    if let Some(idx) = containing_appendrel {
        let save_wrap_option = rvcontext.wrap_option;
        rvcontext.wrap_option = ReplaceWrapOption::None;
        replace_vars_in_translated_vars(mcx, root, rvcontext, outer_has_sublinks, idx)?;
        rvcontext.wrap_option = save_wrap_option;
        return Ok(());
    }

    // targetList / returningList (owned TargetEntry values; PHVs are certainly
    // above any outer join here).
    pullup_replace_vars_targetlist(mcx, root, &mut parse.targetList, rvcontext, outer_has_sublinks)?;
    pullup_replace_vars_targetlist(
        mcx,
        root,
        &mut parse.returningList,
        rvcontext,
        outer_has_sublinks,
    )?;

    // onConflict.
    if let Some(oc) = parse.onConflict.as_mut() {
        pullup_replace_vars_nodelist(mcx, root, &mut oc.onConflictSet, rvcontext, outer_has_sublinks)?;
        if oc.onConflictWhere.is_some() {
            let w = oc.onConflictWhere.take();
            oc.onConflictWhere =
                pullup_replace_vars_opt(mcx, root, w, rvcontext, outer_has_sublinks)?;
        }
        // arbiterElems/arbiterWhere/exclRelTlist can't reference a subquery.
    }

    // mergeActionList.
    {
        let n = parse.mergeActionList.len();
        for i in 0..n {
            // Each element is a Node::Expr / Node holding a MergeAction.
            let action = core::mem::replace(&mut *parse.mergeActionList[i], dummy_node(mcx)?);
            let action = pullup_replace_vars_merge_action(
                mcx,
                root,
                action,
                rvcontext,
                outer_has_sublinks,
            )?;
            *parse.mergeActionList[i] = action;
        }
    }

    // mergeJoinCondition.
    if parse.mergeJoinCondition.is_some() {
        let c = parse.mergeJoinCondition.take();
        parse.mergeJoinCondition =
            pullup_replace_vars_opt_expr(mcx, root, c, rvcontext, outer_has_sublinks)?;
    }

    // jointree (PHV tracking by location). Take the jointree out so the
    // RangeTblRef arm can mutate sibling LATERAL RTEs in `parse.rtable` without
    // aliasing the jointree we are walking.
    {
        let jt = parse.jointree.take().expect("perform_pullup_replace_vars: no jointree");
        let mut node = Node::mk_from_expr(mcx, PgBox::into_inner(jt))?;
        replace_vars_in_jointree(mcx, root, parse, &mut node, rvcontext, outer_has_sublinks)?;
        let jt = if let Some(f) = node.into_fromexpr() {
            alloc_in(mcx, f)?
        } else {
            unreachable!("jointree top is a FromExpr");
        };
        parse.jointree = Some(jt);
    }

    debug_assert!(parse.setOperations.is_none());

    // havingQual.
    if parse.havingQual.is_some() {
        let h = parse.havingQual.take();
        parse.havingQual =
            pullup_replace_vars_opt_expr(mcx, root, h, rvcontext, outer_has_sublinks)?;
    }

    // translated_vars of every appendrel.
    {
        let n = root.append_rel_list.len();
        for idx in 0..n {
            replace_vars_in_translated_vars(mcx, root, rvcontext, outer_has_sublinks, idx)?;
        }
    }

    // joinaliasvars of join RTEs / groupexprs of group RTE, plus the
    // securityQuals of every RTE. In C this rides range_table_mutator_impl
    // (nodeFuncs.c:3893/3917/3927): joinaliasvars and groupexprs are mutated
    // per-kind, but `MUTATE(newrte->securityQuals, ...)` runs unconditionally
    // for *every* RTE after the kind switch. RLS USING quals live in
    // securityQuals, so skipping them leaves virtual-generated-column Vars in
    // RLS policies unexpanded (rowsecurity: "trying to fetch a virtual
    // generated column").
    {
        let n = parse.rtable.len();
        for i in 0..n {
            let kind = parse.rtable[i].rtekind;
            if kind == RTEKind::RTE_JOIN {
                let mut list = core::mem::replace(
                    &mut parse.rtable[i].joinaliasvars,
                    PgVec::new_in(mcx),
                );
                pullup_replace_vars_nodelist(mcx, root, &mut list, rvcontext, outer_has_sublinks)?;
                parse.rtable[i].joinaliasvars = list;
            } else if kind == RTEKind::RTE_GROUP {
                let mut list =
                    core::mem::replace(&mut parse.rtable[i].groupexprs, PgVec::new_in(mcx));
                pullup_replace_vars_nodelist(mcx, root, &mut list, rvcontext, outer_has_sublinks)?;
                parse.rtable[i].groupexprs = list;
            }

            // securityQuals — walked for every RTE kind (nodeFuncs.c:3927).
            if !parse.rtable[i].securityQuals.is_empty() {
                let mut list = core::mem::replace(
                    &mut parse.rtable[i].securityQuals,
                    PgVec::new_in(mcx),
                );
                pullup_replace_vars_nodelist(mcx, root, &mut list, rvcontext, outer_has_sublinks)?;
                parse.rtable[i].securityQuals = list;
            }
        }
    }

    Ok(())
}

/// Run `pullup_replace_vars` over each `expr` of an owned-`TargetEntry` list.
fn pullup_replace_vars_targetlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    tlist: &mut PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<()> {
    let n = tlist.len();
    for i in 0..n {
        if let Some(expr) = tlist[i].expr.take() {
            let node = Node::mk_expr(mcx, PgBox::into_inner(expr))?;
            let newnode = pullup_replace_vars(mcx, root, node, rvcontext, outer_has_sublinks)?;
            if let Some(e) = newnode.into_expr() {
                tlist[i].expr = Some(alloc_in(mcx, e)?);
            } else {
                return Err(types_error::PgError::error(
                    "pullup_replace_vars: targetlist entry is not an expression",
                ));
            }
        }
    }
    Ok(())
}

/// Run `pullup_replace_vars` over each element of a `NodePtr` list.
fn pullup_replace_vars_nodelist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    list: &mut PgVec<'mcx, NodePtr<'mcx>>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<()> {
    let n = list.len();
    for i in 0..n {
        let node = core::mem::replace(&mut *list[i], dummy_node(mcx)?);
        let newnode = pullup_replace_vars(mcx, root, node, rvcontext, outer_has_sublinks)?;
        *list[i] = newnode;
    }
    Ok(())
}

/// Run `pullup_replace_vars` over an optional `NodePtr`.
fn pullup_replace_vars_opt<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Option<NodePtr<'mcx>>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => {
            let newnode =
                pullup_replace_vars(mcx, root, PgBox::into_inner(n), rvcontext, outer_has_sublinks)?;
            Ok(Some(alloc_in(mcx, newnode)?))
        }
    }
}

/// `pullup_replace_vars_opt` over an expression-only `Query` field that is
/// concretely typed `Option<PgBox<Expr>>` (`havingQual` / `mergeJoinCondition`).
/// Wrap the owned `Expr` into `Node::Expr`, run the shared driver, unwrap back.
fn pullup_replace_vars_opt_expr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Option<mcx::PgBox<'mcx, types_nodes::primnodes::Expr<'mcx>>>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::primnodes::Expr<'mcx>>>> {
    match node {
        None => Ok(None),
        Some(n) => {
            let newnode = pullup_replace_vars(
                mcx,
                root,
                Node::mk_expr(mcx, PgBox::into_inner(n))?,
                rvcontext,
                outer_has_sublinks,
            )?;
            match newnode.into_expr() {
                Some(e) => Ok(Some(alloc_in(mcx, e)?)),
                None => Err(types_error::PgError::error(
                    "pullup_replace_vars: expression-only Query field lowered to a non-Expr node",
                )),
            }
        }
    }
}

/// Run `pullup_replace_vars` over a MergeAction node's `qual` + `targetList`.
fn pullup_replace_vars_merge_action<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Node<'mcx>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<Node<'mcx>> {
    match node.into_mergeaction() {
        Some(mut a) => {
            if a.qual.is_some() {
                let q = a.qual.take();
                a.qual = pullup_replace_vars_opt(mcx, root, q, rvcontext, outer_has_sublinks)?;
            }
            pullup_replace_vars_nodelist(mcx, root, &mut a.targetList, rvcontext, outer_has_sublinks)?;
            Ok(Node::mk_merge_action(mcx, a)?)
        }
        None => Err(types_error::PgError::error(
            "pullup_replace_vars: mergeActionList element is not a MergeAction",
        )),
    }
}

/// Replace references in `root.append_rel_list[idx].translated_vars`. Each
/// element is an arena `NodeId`; resolve, run pullup_replace_vars over the
/// Expr, write it back.
fn replace_vars_in_translated_vars<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
    idx: usize,
) -> PgResult<()> {
    let ids: Vec<NodeId> = root.append_rel_list[idx].translated_vars.clone();
    for id in ids {
        if id == NodeId::default() {
            continue;
        }
        let expr = root.node(id).clone_in(mcx)?;
        let newnode =
            pullup_replace_vars(mcx, root, Node::mk_expr(mcx, expr)?, rvcontext, outer_has_sublinks)?;
        if let Some(e) = newnode.into_expr() {
            // Re-intern the rewritten node into the planner arena ('static).
            *root.node_mut(id) = e.erase_lifetime();
        } else {
            return Err(types_error::PgError::error(
                "pullup_replace_vars: translated_vars element is not an expression",
            ));
        }
    }
    Ok(())
}

// ===========================================================================
// replace_vars_in_jointree (prepjointree.c:2516)
// ===========================================================================

/// `replace_vars_in_jointree(jtnode, context)` (prepjointree.c:2516). Runs
/// pullup_replace_vars over every expression in the jointree without changing
/// its structure. `parse` is the upper query (its jointree has already been
/// `take()`n out by the caller, so `parse` does not alias `jtnode`); the
/// RangeTblRef arm needs it to reach `parse.rtable[varno]` for LATERAL RTEs,
/// exactly as the C reads `context->root->parse->rtable`.
fn replace_vars_in_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: &mut Node<'mcx>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<()> {
    if let Some(rtr) = jtnode.as_rangetblref_mut() {
        {
            // A LATERAL RTE other than the target subquery may contain references
            // to the target subquery, which we must replace. We drive this from
            // the jointree scan so we skip no-longer-referenced RTEs.
            let varno = rtr.rtindex;
            if varno != rvcontext.varno {
                let rte = &parse.rtable[(varno - 1) as usize];
                debug_assert!(!core::ptr::eq(rte, &*rvcontext.target_rte));
                if rte.lateral {
                    let rtekind = rte.rtekind;
                    match rtekind {
                        RTEKind::RTE_RELATION => {
                            // shouldn't be marked LATERAL unless tablesample.
                            debug_assert!(parse.rtable[(varno - 1) as usize].tablesample.is_some());
                            let ts = parse.rtable[(varno - 1) as usize].tablesample.take();
                            parse.rtable[(varno - 1) as usize].tablesample =
                                pullup_replace_vars_opt(mcx, root, ts, rvcontext, outer_has_sublinks)?;
                        }
                        RTEKind::RTE_SUBQUERY => {
                            let subq = parse.rtable[(varno - 1) as usize].subquery.take();
                            if let Some(sq) = subq {
                                let mut q = PgBox::into_inner(sq);
                                pullup_replace_vars_subquery(
                                    mcx,
                                    root,
                                    &mut q,
                                    rvcontext,
                                    outer_has_sublinks,
                                )?;
                                parse.rtable[(varno - 1) as usize].subquery =
                                    Some(alloc_in(mcx, q)?);
                            }
                        }
                        RTEKind::RTE_FUNCTION => {
                            let mut functions = core::mem::replace(
                                &mut parse.rtable[(varno - 1) as usize].functions,
                                PgVec::new_in(mcx),
                            );
                            pullup_replace_vars_nodelist(
                                mcx,
                                root,
                                &mut functions,
                                rvcontext,
                                outer_has_sublinks,
                            )?;
                            parse.rtable[(varno - 1) as usize].functions = functions;
                        }
                        RTEKind::RTE_TABLEFUNC => {
                            let tf = parse.rtable[(varno - 1) as usize].tablefunc.take();
                            parse.rtable[(varno - 1) as usize].tablefunc =
                                pullup_replace_vars_opt(mcx, root, tf, rvcontext, outer_has_sublinks)?;
                        }
                        RTEKind::RTE_VALUES => {
                            let mut values = core::mem::replace(
                                &mut parse.rtable[(varno - 1) as usize].values_lists,
                                PgVec::new_in(mcx),
                            );
                            pullup_replace_vars_nodelist(
                                mcx,
                                root,
                                &mut values,
                                rvcontext,
                                outer_has_sublinks,
                            )?;
                            parse.rtable[(varno - 1) as usize].values_lists = values;
                        }
                        RTEKind::RTE_JOIN
                        | RTEKind::RTE_CTE
                        | RTEKind::RTE_NAMEDTUPLESTORE
                        | RTEKind::RTE_RESULT
                        | RTEKind::RTE_GROUP => {
                            // these shouldn't be marked LATERAL.
                            debug_assert!(false, "LATERAL marker on non-lateral RTE kind");
                        }
                    }
                }
            }
            Ok(())
        }
    } else if let Some(f) = jtnode.as_fromexpr_mut() {
        {
            let n = f.fromlist.len();
            for i in 0..n {
                replace_vars_in_jointree(
                    mcx,
                    root,
                    parse,
                    &mut f.fromlist[i],
                    rvcontext,
                    outer_has_sublinks,
                )?;
            }
            if f.quals.is_some() {
                let q = f.quals.take();
                f.quals = pullup_replace_vars_opt(mcx, root, q, rvcontext, outer_has_sublinks)?;
            }
            Ok(())
        }
    } else if let Some(j) = jtnode.as_joinexpr_mut() {
        {
            let save_wrap_option = rvcontext.wrap_option;
            if let Some(larg) = j.larg.as_deref_mut() {
                replace_vars_in_jointree(mcx, root, parse, larg, rvcontext, outer_has_sublinks)?;
            }
            if let Some(rarg) = j.rarg.as_deref_mut() {
                replace_vars_in_jointree(mcx, root, parse, rarg, rvcontext, outer_has_sublinks)?;
            }
            // Use PHVs within the join quals of a full join for variable-free
            // expressions.
            if j.jointype == JoinType::JOIN_FULL {
                rvcontext.wrap_option = ReplaceWrapOption::Varfree;
            }
            if j.quals.is_some() {
                let q = j.quals.take();
                j.quals = pullup_replace_vars_opt(mcx, root, q, rvcontext, outer_has_sublinks)?;
            }
            rvcontext.wrap_option = save_wrap_option;
            Ok(())
        }
    } else {
        Err(types_error::PgError::error("unrecognized node type"))
    }
}

// ===========================================================================
// pullup_replace_vars (prepjointree.c:2623)
// ===========================================================================

/// `pullup_replace_vars(expr, context)` (prepjointree.c:2623). Apply pullup
/// variable replacement throughout an expression tree. The C returns a modified
/// *copy*; the rewrite-core engine mutates in place, so we run it over the owned
/// `node` and return it.
fn pullup_replace_vars<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    mut node: Node<'mcx>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<Node<'mcx>> {
    // The callback needs `root`, `mcx`, and the context. The engine threads its
    // own ReplaceRteVariablesContext separately; we capture `root`/`mcx`/the
    // pullup context in the closure.
    let varno = rvcontext.varno;
    let mut cb = |var: &Var, _ctx: &mut ReplaceRteVariablesContext| -> PgResult<Expr> {
        pullup_replace_vars_callback(mcx, root, rvcontext, var)
    };
    replace_rte_variables(&mut node, varno, 0, &mut cb, outer_has_sublinks, mcx)?;
    Ok(node)
}

/// `pullup_replace_vars_subquery(query, context)` (prepjointree.c:2955). Like
/// [`pullup_replace_vars`] but enters the Query with `sublevels_up == 1` (the
/// engine would otherwise not increment before entering it) and passes no
/// `outer_hasSubLinks` (the Query records its own). Mutates `query` in place.
fn pullup_replace_vars_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    query: &mut Query<'mcx>,
    rvcontext: &mut PullupReplaceVarsContext<'mcx>,
    _outer_has_sublinks: &mut Option<bool>,
) -> PgResult<()> {
    let varno = rvcontext.varno;
    let q = core::mem::replace(query, Query::new(mcx));
    let mut node = Node::mk_query(mcx, q)?;
    let mut none_outer: Option<bool> = None;
    {
        let mut cb = |var: &Var, _ctx: &mut ReplaceRteVariablesContext| -> PgResult<Expr> {
            pullup_replace_vars_callback(mcx, root, rvcontext, var)
        };
        replace_rte_variables(&mut node, varno, 1, &mut cb, &mut none_outer, mcx)?;
    }
    match node.into_query() {
        Some(q) => *query = q,
        None => unreachable!("pullup_replace_vars_subquery: node is no longer a Query"),
    }
    Ok(())
}

// ===========================================================================
// pullup_replace_vars_callback (prepjointree.c:2633) — THE CRUX
// ===========================================================================

/// `pullup_replace_vars_callback(var, context)` (prepjointree.c:2633).
fn pullup_replace_vars_callback<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rcon: &mut PullupReplaceVarsContext<'mcx>,
    var: &Var,
) -> PgResult<Expr<'mcx>> {
    let varattno = var.varattno;
    let varlevelsup = var.varlevelsup;

    // System columns are not replaced.
    if varattno < types_core::primitive::InvalidAttrNumber {
        return Ok(Expr::Var(var.clone()));
    }

    // We need a PHV if the Var has nonempty varnullingrels (unless the
    // replacement is a Var/PHV we can just add nullingrels to), or if the caller
    // requested wrapping.
    let var_has_nullingrels = !expr_relids_is_empty(&var.varnullingrels);
    let need_phv = var_has_nullingrels || rcon.wrap_option != ReplaceWrapOption::None;

    let tlist_len = rcon.targetlist.len() as i16;

    let mut newnode: Node<'mcx>;

    // If PHVs are needed, cache modified expressions in rcon.rv_cache[] to avoid
    // generating identical PHVs with different IDs. Cached items have
    // phlevelsup=0, phnullingrels=NULL; copy + adjust below.
    if need_phv
        && varattno >= types_core::primitive::InvalidAttrNumber
        && varattno <= tlist_len
        && rcon.rv_cache[varattno as usize].is_some()
    {
        let cached = rcon.rv_cache[varattno as usize].as_ref().unwrap();
        newnode = Node::mk_expr(mcx, cached.clone_in(mcx)?)?;
    } else {
        // Generate the replacement expression (whole-row expansion +
        // non-default varreturningtype handled by ReplaceVarFromTargetList).
        let replacement = ReplaceVarFromTargetList(
            var,
            &rcon.target_rte,
            &rcon.targetlist,
            rcon.result_relation,
            ReplaceVarsNoMatchOption::ReportError,
            0,
            mcx,
        )?;
        newnode = Node::mk_expr(mcx, replacement)?;

        if need_phv {
            let wrap = compute_wrap(mcx, root, rcon, var, &newnode)?;
            if wrap {
                let inner = newnode.into_expr().unwrap_or_else(|| unreachable!());
                // The placeholder builder interns into the planner arena ('static);
                // erase the input and re-localize the PHV into the run `mcx`.
                let phv = placeholder::make_placeholder_expr::call(
                    root,
                    inner.erase_lifetime(),
                    pathrelids_make_singleton(rcon.varno),
                )
                .clone_in(mcx)?;
                newnode = Node::mk_place_holder_var(mcx, phv)?;
                // Cache it if possible.
                if varattno >= types_core::primitive::InvalidAttrNumber && varattno <= tlist_len {
                    if let Some(e) = newnode.as_expr() {
                        rcon.rv_cache[varattno as usize] = Some(e.clone_in(mcx)?);
                    }
                }
            }
        }
    }

    // Propagate any varnullingrels into the replacement expression.
    if var_has_nullingrels {
        // Peel the `Node::Expr` wrapper first (structural), then dispatch the
        // inner `Expr` enum: a bare Var/PHV is handled directly, anything else
        // falls through to the general walk below.
        let handled_directly = match newnode.as_expr_mut() {
            Some(Expr::Var(newvar)) => {
                debug_assert_eq!(newvar.varlevelsup, 0);
                expr_relids_add_in_place(&mut newvar.varnullingrels, &var.varnullingrels);
                true
            }
            Some(Expr::PlaceHolderVar(newphv)) => {
                debug_assert_eq!(newphv.phlevelsup, 0);
                expr_relids_add_in_place(&mut newphv.phnullingrels, &var.varnullingrels);
                true
            }
            _ => false,
        };
        if !handled_directly {
            {
                // There should be Vars/PHVs within the expression that we can
                // modify. Subquery Vars/PHVs get the full var->varnullingrels;
                // lateral references get only the nullingrels that apply to them.
                if rcon.target_rte.lateral {
                    let nullinfo = rcon
                        .nullinfo
                        .as_ref()
                        .expect("lateral target_rte requires nullinfo");
                    // Identify lateral varnos used within newnode (before
                    // injecting var->varnullingrels).
                    let mut lvarnos = pull_varnos(Some(root), &newnode);
                    pathrelids_del_bms(&mut lvarnos, rcon.relids.as_deref());
                    let mut lvarno = -1;
                    loop {
                        lvarno = pathrelids_next_member(&lvarnos, lvarno);
                        if lvarno < 0 {
                            break;
                        }
                        debug_assert!(lvarno > 0 && lvarno <= nullinfo.rtlength);
                        let lnullingrels = intersect_expr_with_bms(
                            &var.varnullingrels,
                            nullinfo.nullingrels[lvarno as usize].as_deref(),
                        );
                        if !expr_relids_is_empty(&lnullingrels) {
                            add_nulling_relids(
                                &mut newnode,
                                Some(&expr_relids_make_singleton(lvarno)),
                                &lnullingrels,
                                mcx,
                            );
                        }
                    }
                }

                // Finally, deal with Vars/PHVs of the subquery itself.
                // C passes rcon->relids directly to add_nulling_relids, where a
                // NULL relid set means "all level-zero Vars/PHVs" (the
                // expand_virtual_generated_columns case sets rvcontext.relids =
                // NULL). Preserve that: None must stay None, not collapse to an
                // empty set (which would match nothing).
                let subquery_relids = rcon.relids.as_deref().map(bms_to_expr_relids_some);
                add_nulling_relids(&mut newnode, subquery_relids.as_ref(), &var.varnullingrels, mcx);
                // Assert we did put the varnullingrels into the expression.
                debug_assert!({
                    let after = pull_varnos(Some(root), &newnode);
                    pathrelids_superset_of_expr(&after, &var.varnullingrels)
                });
            }
        }
    }

    // Must adjust varlevelsup if replaced Var is within a subquery.
    if varlevelsup > 0 {
        IncrementVarSublevelsUp(&mut newnode, varlevelsup as i32, 0, mcx)?;
    }

    match newnode.into_expr() {
        Some(e) => Ok(e),
        None => Err(types_error::PgError::error(
            "pullup_replace_vars: replacement is not an expression",
        )),
    }
}

/// The `if (need_phv)` wrap-decision ladder of `pullup_replace_vars_callback`.
fn compute_wrap<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rcon: &PullupReplaceVarsContext<'mcx>,
    var: &Var,
    newnode: &Node<'mcx>,
) -> PgResult<bool> {
    let varattno = var.varattno;

    if rcon.wrap_option == ReplaceWrapOption::All {
        // Caller told us to wrap all expressions.
        return Ok(true);
    }
    if varattno == types_core::primitive::InvalidAttrNumber {
        // Whole-tuple reference: wrap one PHV around the whole RowExpr.
        return Ok(true);
    }

    // Simple level-zero Var: escapes wrapping unless it's a lateral reference
    // outside the subquery and not under the same lowest nulling outer join.
    if let Some(newvar) = newnode.as_var() {
        if newvar.varlevelsup == 0 {
            if rcon.target_rte.lateral
                && !bms_is_member(newvar.varno, rcon.relids.as_deref())
            {
                let nullinfo = rcon
                    .nullinfo
                    .as_ref()
                    .expect("lateral target_rte requires nullinfo");
                let lvarno = newvar.varno;
                debug_assert!(lvarno > 0 && lvarno <= nullinfo.rtlength);
                if !bms_is_subset(
                    nullinfo.nullingrels[rcon.varno as usize].as_deref(),
                    nullinfo.nullingrels[lvarno as usize].as_deref(),
                ) {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    // Same rules for a level-zero PlaceHolderVar.
    if let Some(newphv) = newnode.as_placeholdervar() {
        if newphv.phlevelsup == 0 {
            if rcon.target_rte.lateral
                && !expr_relids_subset_of_bms(&newphv.phrels, rcon.relids.as_deref())
            {
                let nullinfo = rcon
                    .nullinfo
                    .as_ref()
                    .expect("lateral target_rte requires nullinfo");
                let mut lvarno = -1;
                loop {
                    lvarno = expr_relids_next_member(&newphv.phrels, lvarno);
                    if lvarno < 0 {
                        break;
                    }
                    debug_assert!(lvarno > 0 && lvarno <= nullinfo.rtlength);
                    if !bms_is_subset(
                        nullinfo.nullingrels[rcon.varno as usize].as_deref(),
                        nullinfo.nullingrels[lvarno as usize].as_deref(),
                    ) {
                        return Ok(true);
                    }
                }
            }
            return Ok(false);
        }
    }

    // General case: if the node contains Var(s)/PHV(s) of the subquery (or of
    // rels under the same lowest nulling outer join) and no non-strict
    // constructs, add nullingrels rather than wrapping.
    let mut contain_nullable_vars = false;
    if !rcon.target_rte.lateral {
        if contain_vars_of_level(newnode, 0) {
            contain_nullable_vars = true;
        }
    } else {
        let all_varnos = pull_varnos(Some(root), newnode);
        if pathrelids_overlap_bms(&all_varnos, rcon.relids.as_deref()) {
            contain_nullable_vars = true;
        } else {
            let nullinfo = rcon
                .nullinfo
                .as_ref()
                .expect("lateral target_rte requires nullinfo");
            let mut varno = -1;
            loop {
                varno = pathrelids_next_member(&all_varnos, varno);
                if varno < 0 {
                    break;
                }
                debug_assert!(varno > 0 && varno <= nullinfo.rtlength);
                if bms_is_subset(
                    nullinfo.nullingrels[rcon.varno as usize].as_deref(),
                    nullinfo.nullingrels[varno as usize].as_deref(),
                ) {
                    contain_nullable_vars = true;
                    break;
                }
            }
        }
    }

    let newexpr = newnode.as_expr();
    if contain_nullable_vars && !contain_nonstrict_functions(newexpr)? {
        let _ = mcx;
        Ok(false)
    } else {
        Ok(true)
    }
}

/// `bms_is_subset(a, b)` where `a` is [`ExprRelids`] and `b` is an `'mcx`
/// [`Bitmapset`].
fn expr_relids_subset_of_bms(a: &ExprRelids, b: Option<&Bitmapset>) -> bool {
    let bw: &[u64] = match b {
        None => &[],
        Some(b) => &b.words,
    };
    for (i, &w) in a.words.iter().enumerate() {
        let bb = if i < bw.len() { bw[i] } else { 0 };
        if (w & !bb) != 0 {
            return false;
        }
    }
    true
}

/// `bms_next_member` over [`ExprRelids`].
fn expr_relids_next_member(a: &ExprRelids, prevbit: i32) -> i32 {
    let words = &a.words;
    let mut bit = prevbit + 1;
    while (bit as usize) < words.len() * BITS_PER_WORD {
        let wnum = (bit as usize) / BITS_PER_WORD;
        let off = (bit as usize) % BITS_PER_WORD;
        let w = words[wnum] >> off;
        if w != 0 {
            return bit + w.trailing_zeros() as i32;
        }
        bit = ((wnum + 1) * BITS_PER_WORD) as i32;
    }
    -2
}

/// `bms_is_subset(b, a)` — is every member of [`ExprRelids`] `b` in
/// [`PathRelids`] `a`? (the C `bms_is_subset(var->varnullingrels,
/// pull_varnos(newnode))` assertion).
fn pathrelids_superset_of_expr(a: &PathRelids, b: &ExprRelids) -> bool {
    let aw: &[u64] = match a {
        None => &[],
        Some(a) => &a.words,
    };
    for (i, &w) in b.words.iter().enumerate() {
        let aa = if i < aw.len() { aw[i] } else { 0 };
        if (w & !aa) != 0 {
            return false;
        }
    }
    true
}

// ===========================================================================
// OffsetVarNodes / IncrementVarSublevelsUp over a whole Query + append_rel_list
// ===========================================================================

/// `OffsetVarNodes((Node *) subquery, rtoffset, 0)` over the whole owned Query.
fn offset_var_nodes_in_query<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &mut Query<'mcx>,
    offset: i32,
    sublevels_up: i32,
) -> types_error::PgResult<()> {
    let node = core::mem::replace(subquery, Query::new(mcx));
    let mut qnode = Node::mk_query(mcx, node)?;
    OffsetVarNodes(&mut qnode, offset, sublevels_up, mcx);
    if let Some(q) = qnode.into_query() {
        *subquery = q;
    } else {
        unreachable!();
    }
    Ok(())
}

/// `IncrementVarSublevelsUp((Node *) subquery, -1, 1)` over the whole Query.
fn increment_var_sublevels_up_in_query<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &mut Query<'mcx>,
    delta: i32,
    min_sublevels_up: i32,
) -> types_error::PgResult<()> {
    let node = core::mem::replace(subquery, Query::new(mcx));
    let mut qnode = Node::mk_query(mcx, node)?;
    let res = IncrementVarSublevelsUp(&mut qnode, delta, min_sublevels_up, mcx);
    if let Some(q) = qnode.into_query() {
        *subquery = q;
    } else {
        unreachable!();
    }
    res
}

/// `OffsetVarNodes((Node *) subroot->append_rel_list, rtoffset, 0)`: each
/// AppendRelInfo's relid-bearing children are its translated_vars (arena Exprs).
fn offset_var_nodes_in_append_rel_list<'mcx>(
    mcx: Mcx<'mcx>,
    subroot: &mut PlannerInfo,
    offset: i32,
    sublevels_up: i32,
) -> types_error::PgResult<()> {
    // C: OffsetVarNodes_walker has an explicit `IsA(node, AppendRelInfo)` case
    // (rewriteManip.c:444) that, when sublevels_up == 0, adds `offset` to the
    // integer fields parent_relid and child_relid (then falls through to recurse
    // into translated_vars). The earlier port dropped the integer fixup, so a
    // pulled-up subquery's appendrel members kept their subquery-local relids and
    // collided with the upper query's appendrel relids ("child relation already
    // exists" on nested UNION ALL). Restore the integer offset.
    if sublevels_up == 0 {
        for ai in subroot.append_rel_list.iter_mut() {
            ai.parent_relid = (ai.parent_relid as i32 + offset) as u32;
            ai.child_relid = (ai.child_relid as i32 + offset) as u32;
        }
    }

    let mut ids: Vec<NodeId> = Vec::new();
    for ai in subroot.append_rel_list.iter() {
        for &id in ai.translated_vars.iter() {
            if id != NodeId::default() {
                ids.push(id);
            }
        }
    }
    for id in ids {
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let mut node = Node::mk_expr(mcx, subroot.node(id).clone_in(mcx)?)?;
        OffsetVarNodes(&mut node, offset, sublevels_up, mcx);
        if let Some(e) = node.into_expr() {
            *subroot.node_mut(id) = e.erase_lifetime();
        }
    }
    Ok(())
}

/// `IncrementVarSublevelsUp((Node *) subroot->append_rel_list, -1, 1)`.
fn increment_var_sublevels_up_in_append_rel_list<'mcx>(
    mcx: Mcx<'mcx>,
    subroot: &mut PlannerInfo,
    delta: i32,
    min_sublevels_up: i32,
) -> types_error::PgResult<()> {
    let mut ids: Vec<NodeId> = Vec::new();
    for ai in subroot.append_rel_list.iter() {
        for &id in ai.translated_vars.iter() {
            if id != NodeId::default() {
                ids.push(id);
            }
        }
    }
    for id in ids {
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let mut node = Node::mk_expr(mcx, subroot.node(id).clone_in(mcx)?)?;
        IncrementVarSublevelsUp(&mut node, delta, min_sublevels_up, mcx)?;
        if let Some(e) = node.into_expr() {
            *subroot.node_mut(id) = e.erase_lifetime();
        }
    }
    Ok(())
}

// silence unused import in some configurations
#[allow(unused_imports)]
use types_pathnodes::AppendRelInfo as _AppendRelInfo;
