//! `optimizer/util/var.c` — the `Var` node manipulation routines.
//!
//! For most purposes a `PlaceHolderVar` is considered a `Var` too, even if its
//! contained expression is variable-free; a `CurrentOfExpr` is likewise treated
//! as a `Var` for the "contains variables" tests.
//!
//! # Node-walking over the arena
//!
//! var.c is pure node-walking. The optimizer interns expression payloads in the
//! [`PlannerInfo`] node arena (`node_arena: Vec<Expr>`); a seam takes a
//! [`NodeId`] handle, which this crate resolves to `&Expr` via
//! [`PlannerInfo::node`], wraps as `Node::Expr(expr.clone())`, and walks with
//! the central [`backend_nodes_core::node_walker`] engine
//! (`expression_tree_walker` / `query_or_expression_tree_walker` /
//! `query_tree_walker`), whose `bool (*)(Node *, void *)` walker is a Rust
//! `&mut dyn FnMut(&Node) -> bool` closure. C's `IsA(node, X)` dispatch is a
//! match over the [`Node`]/[`Expr`] enum arms (every `Var`-family node is an
//! `Expr` arm; `Query` is its own `Node` arm).
//!
//! # Relids set algebra
//!
//! The collectors accumulate a [`Relids`] (`= Option<Box<Bitmapset>>`, the
//! planner relation-id set; empty = `None`). `Bitmapset` here is the
//! lifetime-free word-vector type (`{ words: Vec<u64> }`), so the small `bms_*`
//! algebra var.c needs (`bms_add_member`, `bms_add_members`, `bms_equal`,
//! `bms_difference`, `bms_join`) is reproduced faithfully inline over the word
//! storage — a 1:1 port of nodes/bitmapset.c's semantics (trailing-zero
//! trimming so the empty set is `None`/empty-`words`).

#![allow(non_snake_case)]

extern crate alloc;
use alloc::boxed::Box;
use alloc::vec::Vec;

use backend_nodes_core::node_walker::{
    expression_tree_walker, node_expr_wrapper, query_or_expression_tree_walker, query_tree_walker,
};
use types_error::PgResult;
use types_nodes::nodes::{ntag, Node};
use types_nodes::primnodes::Expr;
use types_pathnodes::{Bitmapset, NodeId, PlannerInfo, Relids};

// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) = -7. var.c offsets
// attribute numbers by this so system attributes fit a bitmap; matches
// `types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber`.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

/// `VAR_RETURNING_DEFAULT` (primnodes.h) — the default `varreturningtype`.
use types_nodes::primnodes::VarReturningType;

// ===========================================================================
// Relids word-vector algebra (nodes/bitmapset.c) — inline, faithful.
//
// `Relids = Option<Box<Bitmapset>>` with `Bitmapset { words: Vec<u64> }`. The
// empty set is `None`. We mirror bitmapset.c: the canonical representation has
// no trailing all-zero words, so the empty set is `None`.
// ===========================================================================

const BITS_PER_WORD: i32 = 64;

#[inline]
fn wordnum(x: i32) -> usize {
    (x / BITS_PER_WORD) as usize
}
#[inline]
fn bitnum(x: i32) -> u32 {
    (x % BITS_PER_WORD) as u32
}

/// Drop trailing all-zero words; return `None` if the set became empty. Keeps
/// `Relids` canonical (`bms_is_empty` ⇔ `None`).
fn normalize(mut bms: Box<Bitmapset>) -> Relids {
    while let Some(&last) = bms.words.last() {
        if last == 0 {
            bms.words.pop();
        } else {
            break;
        }
    }
    if bms.words.is_empty() {
        None
    } else {
        Some(bms)
    }
}

/// `bms_add_member(a, x)` — add member `x` to `a`, recycling `a`.
fn bms_add_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset { words: Vec::new() }));
    let wnum = wordnum(x);
    if wnum >= bms.words.len() {
        bms.words.resize(wnum + 1, 0);
    }
    bms.words[wnum] |= 1u64 << bitnum(x);
    Some(bms)
}

/// `bms_add_members(a, b)` — `a := a ∪ b`, recycling `a`, `b` unchanged.
fn bms_add_members(a: Relids, b: &Relids) -> Relids {
    let bw = match b {
        None => return a,
        Some(b) => &b.words,
    };
    if bw.is_empty() {
        return a;
    }
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset { words: Vec::new() }));
    if bms.words.len() < bw.len() {
        bms.words.resize(bw.len(), 0);
    }
    for (i, &w) in bw.iter().enumerate() {
        bms.words[i] |= w;
    }
    normalize(bms)
}

/// `bms_equal(a, b)` — set equality. Canonical sets compare word-for-word; the
/// empty set is `None`/empty `words`.
fn bms_equal(a: &Relids, b: &Relids) -> bool {
    let aw: &[u64] = match a {
        None => &[],
        Some(a) => &a.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(b) => &b.words,
    };
    // Compare ignoring trailing zeros (defensive; canonical sets have none).
    let alen = aw.iter().rposition(|&w| w != 0).map_or(0, |i| i + 1);
    let blen = bw.iter().rposition(|&w| w != 0).map_or(0, |i| i + 1);
    if alen != blen {
        return false;
    }
    aw[..alen] == bw[..blen]
}

/// `bms_difference(a, b)` — a fresh set `a \ b` (inputs unchanged).
fn bms_difference(a: &Relids, b: &Relids) -> Relids {
    let aw = match a {
        None => return None,
        Some(a) => &a.words,
    };
    let bw: &[u64] = match b {
        None => aw, // a \ ∅ = a; copy below
        Some(b) => &b.words,
    };
    let mut words = aw.clone();
    if let Some(b) = b {
        let _ = bw;
        for i in 0..words.len() {
            if i < b.words.len() {
                words[i] &= !b.words[i];
            }
        }
    }
    normalize(Box::new(Bitmapset { words }))
}

/// `bms_join(a, b)` — destructive union; both inputs recycled into the result.
fn bms_join(a: Relids, b: Relids) -> Relids {
    let a = match a {
        None => return b,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(a),
        Some(b) => b,
    };
    let (mut result, other) = if a.words.len() < b.words.len() {
        (b, a)
    } else {
        (a, b)
    };
    for i in 0..other.words.len() {
        result.words[i] |= other.words[i];
    }
    normalize(result)
}

/// Convert an [`ExprRelids`] (the lifetime-free relids carried on a
/// `Var`/`PlaceHolderVar`) into a borrowable [`Relids`]. The two share the same
/// `{ words: Vec<u64> }` representation; an empty `words` is the empty set
/// (`None`).
fn expr_relids_to_relids(er: &types_nodes::primnodes::ExprRelids) -> Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(Box::new(Bitmapset {
            words: er.words.clone(),
        }))
    }
}

// ===========================================================================
// pull_varnos / pull_varnos_of_level (var.c:113-278)
// ===========================================================================

/// `pull_varnos_context` (var.c:33-38).
struct PullVarnosContext<'a> {
    varnos: Relids,
    root: Option<&'a PlannerInfo>,
    sublevels_up: i32,
}

/// `pull_varnos(root, node)` (var.c:113). Create a set of all the distinct
/// varnos present in a parsetree. Only varnos referencing level-zero rtable
/// entries are considered. The result includes outer-join relids mentioned in
/// `Var.varnullingrels` and `PlaceHolderVar.phnullingrels`. `root` may be `None`
/// if PlaceHolderVars need not be processed.
pub fn pull_varnos(root: Option<&PlannerInfo>, node: &Node) -> Relids {
    let mut context = PullVarnosContext {
        varnos: None,
        root,
        sublevels_up: 0,
    };
    // Must be prepared to start with a Query or a bare expression tree; if it's
    // a Query, we don't want to increment sublevels_up.
    query_or_expression_tree_walker(
        node,
        &mut |n: &Node| pull_varnos_walker(n, &mut context),
        0,
    );
    context.varnos
}

/// `pull_varnos_of_level(root, node, levelsup)` (var.c:139). Only Vars of the
/// specified level are considered.
pub fn pull_varnos_of_level(root: Option<&PlannerInfo>, node: &Node, levelsup: i32) -> Relids {
    let mut context = PullVarnosContext {
        varnos: None,
        root,
        sublevels_up: levelsup,
    };
    query_or_expression_tree_walker(
        node,
        &mut |n: &Node| pull_varnos_walker(n, &mut context),
        0,
    );
    context.varnos
}

/// `pull_varnos_walker` (var.c:160).
fn pull_varnos_walker(node: &Node, context: &mut PullVarnosContext<'_>) -> bool {
    if let Some(expr) = node.as_expr() {
        return pull_varnos_walker_expr(node, expr, context);
    }
    if node.node_tag() == ntag::T_Query {
        let q = node.as_query().unwrap();
        // Recurse into RTE subquery or not-yet-planned sublink subquery.
        context.sublevels_up += 1;
        let result = query_tree_walker(q, &mut |n: &Node| pull_varnos_walker(n, context), 0);
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, &mut |n: &Node| pull_varnos_walker(n, context))
}

/// The `Node::Expr` peel of [`pull_varnos_walker`] (kept structural per the
/// dual-homed-tag rule: `Node::Expr` spans every `Expr` tag, so we peel the
/// `Expr` first and dispatch its enum).
fn pull_varnos_walker_expr(
    node: &Node,
    expr: &Expr,
    context: &mut PullVarnosContext<'_>,
) -> bool {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup as i32 == context.sublevels_up {
                context.varnos = bms_add_member(context.varnos.take(), var.varno);
                context.varnos = bms_add_members(
                    context.varnos.take(),
                    &expr_relids_to_relids(&var.varnullingrels),
                );
            }
            false
        }
        Expr::CurrentOfExpr(cexpr) => {
            if context.sublevels_up == 0 {
                context.varnos = bms_add_member(context.varnos.take(), cexpr.cvarno as i32);
            }
            false
        }
        Expr::PlaceHolderVar(phv) => {
            // If a PlaceHolderVar is not of the target query level, ignore it,
            // instead recursing into its expression to see if it contains any
            // vars of the target level. We also do that when no "root" is
            // passed.
            if phv.phlevelsup as i32 == context.sublevels_up && context.root.is_some() {
                let root = context.root.unwrap();

                // Ideally, the PHV's contribution is its ph_eval_at set; but
                // this code can run before that's computed. If we cannot find a
                // PlaceHolderInfo, fall back to the syntactic level (phv.phrels).
                let mut phinfo: Option<&types_pathnodes::PlaceHolderInfo> = None;
                if phv.phlevelsup == 0 && (phv.phid as i32) < root.placeholder_array_size {
                    if let Some(Some(phid)) = root.placeholder_array.get(phv.phid as usize) {
                        phinfo = Some(root.phinfo(*phid));
                    }
                }

                let phv_phrels = expr_relids_to_relids(&phv.phrels);
                match phinfo {
                    None => {
                        // No PlaceHolderInfo yet, use phrels.
                        context.varnos = bms_add_members(context.varnos.take(), &phv_phrels);
                    }
                    Some(phinfo) => {
                        let ph_var_phrels = &phinfo.ph_var_phrels;
                        if bms_equal(&phv_phrels, ph_var_phrels) {
                            // Normal case: use ph_eval_at.
                            context.varnos =
                                bms_add_members(context.varnos.take(), &phinfo.ph_eval_at);
                        } else {
                            // Translated PlaceHolderVar: translate ph_eval_at.
                            // remove what was removed from phv.phrels ...
                            let delta = bms_difference(ph_var_phrels, &phv_phrels);
                            let mut newevalat = bms_difference(&phinfo.ph_eval_at, &delta);
                            // ... then if that was in fact part of ph_eval_at ...
                            if !bms_equal(&newevalat, &phinfo.ph_eval_at) {
                                // ... add what was added
                                let delta = bms_difference(&phv_phrels, ph_var_phrels);
                                newevalat = bms_join(newevalat, delta);
                            }
                            context.varnos = bms_join(context.varnos.take(), newevalat);
                        }
                    }
                }

                // In all three cases, include phnullingrels in the result.
                context.varnos = bms_add_members(
                    context.varnos.take(),
                    &expr_relids_to_relids(&phv.phnullingrels),
                );
                return false; // don't recurse into expression
            }
            // PHV of a different level (or no root): recurse into the expr.
            expression_tree_walker(node, &mut |n: &Node| pull_varnos_walker(n, context))
        }
        _ => expression_tree_walker(node, &mut |n: &Node| pull_varnos_walker(n, context)),
    }
}

// ===========================================================================
// pull_varattnos (var.c:295-328)
// ===========================================================================

/// `pull_varattnos_context` (var.c:40-44). The result here is the lifetime-free
/// [`Relids`] word set (system attributes fit because numbers are offset by
/// `FirstLowInvalidHeapAttributeNumber`).
struct PullVarattnosContext {
    varattnos: Relids,
    varno: i32,
}

/// `pull_varattnos(node, varno, &varattnos)` (var.c:295). Find all the distinct
/// attribute numbers present in `node` that reference range-table entry `varno`
/// at rtable level zero, offset by `FirstLowInvalidHeapAttributeNumber`, added
/// to the initial `varattnos`. C signature is `void(Node*, Index, Bitmapset**)`;
/// the owned form takes the initial set by value and returns the updated one.
pub fn pull_varattnos(node: &Node, varno: i32, varattnos: Relids) -> Relids {
    let mut context = PullVarattnosContext { varattnos, varno };
    let _ = pull_varattnos_walker(node, &mut context);
    context.varattnos
}

/// `pull_varattnos_walker` (var.c:308).
fn pull_varattnos_walker(node: &Node, context: &mut PullVarattnosContext) -> bool {
    if let Some(Expr::Var(var)) = node.as_expr() {
        if var.varno == context.varno && var.varlevelsup == 0 {
            let member = var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER;
            context.varattnos = bms_add_member(context.varattnos.take(), member);
        }
        return false;
    }
    // Should not find an unplanned subquery (C: Assert(!IsA(node, Query))).
    if node.node_tag() == ntag::T_Query {
        panic!("pull_varattnos_walker: unexpected unplanned Query subtree");
    }
    expression_tree_walker(node, &mut |n: &Node| pull_varattnos_walker(n, context))
}

// ===========================================================================
// pull_vars_of_level (var.c:338-392)
// ===========================================================================

/// `pull_vars_context` (var.c:46-50). The Vars/PHVs are cloned into the list
/// (the owned tree does not hand out shared `Node *` aliases).
struct PullVarsContext {
    vars: Vec<Expr>,
    sublevels_up: i32,
}

/// `pull_vars_of_level(node, levelsup)` (var.c:338). Create a list of all Vars
/// (and PlaceHolderVars) referencing the specified query level. The cloned
/// `Var`/`PlaceHolderVar` `Expr`s are returned (the seam interns them into the
/// arena and hands back `NodeId`s).
pub fn pull_vars_of_level(node: &Node, levelsup: i32) -> Vec<Expr> {
    let mut context = PullVarsContext {
        vars: Vec::new(),
        sublevels_up: levelsup,
    };
    query_or_expression_tree_walker(
        node,
        &mut |n: &Node| pull_vars_walker(n, &mut context),
        0,
    );
    context.vars
}

/// `pull_vars_of_level((Node *) query, levelsup)` over an owned `Query<'mcx>`
/// (var.c:338). Mirrors [`pull_vars_of_level`] for the case where the passed
/// `Node` is a `Query`: `query_or_expression_tree_walker` dispatches a top-level
/// `Query` straight to `query_tree_walker` (no implicit level bump — that is done
/// by `pull_vars_walker`'s `T_Query` arm only for *nested* sub-queries reached
/// during the walk). Installs the `pull_vars_of_level_query` seam for
/// `extract_lateral_references`' `RTE_SUBQUERY` arm.
pub fn pull_vars_of_level_query(query: &types_nodes::copy_query::Query, levelsup: i32) -> Vec<Expr> {
    let mut context = PullVarsContext {
        vars: Vec::new(),
        sublevels_up: levelsup,
    };
    query_tree_walker(
        query,
        &mut |n: &Node| pull_vars_walker(n, &mut context),
        0,
    );
    context.vars
}

/// `pull_vars_walker` (var.c:358).
fn pull_vars_walker(node: &Node, context: &mut PullVarsContext) -> bool {
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => {
                if var.varlevelsup as i32 == context.sublevels_up {
                    context.vars.push(Expr::Var(var.clone()));
                }
                return false;
            }
            Expr::PlaceHolderVar(phv) => {
                if phv.phlevelsup as i32 == context.sublevels_up {
                    context.vars.push(Expr::PlaceHolderVar(phv.clone()));
                }
                // we don't want to look into the contained expression
                return false;
            }
            _ => return expression_tree_walker(node, &mut |n: &Node| pull_vars_walker(n, context)),
        }
    }
    if node.node_tag() == ntag::T_Query {
        let q = node.as_query().unwrap();
        context.sublevels_up += 1;
        let result = query_tree_walker(q, &mut |n: &Node| pull_vars_walker(n, context), 0);
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, &mut |n: &Node| pull_vars_walker(n, context))
}

// ===========================================================================
// contain_var_clause (var.c:405-431)
// ===========================================================================

/// `contain_var_clause(node)` (var.c:405). True iff `node` contains any Var of
/// the current query level. Does not examine subqueries; must only be used after
/// reduction of sublinks to subplans.
pub fn contain_var_clause(node: &Node) -> bool {
    contain_var_clause_walker(node)
}

/// `contain_var_clause_walker` (var.c:411).
fn contain_var_clause_walker(node: &Node) -> bool {
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => return var.varlevelsup == 0,
            Expr::CurrentOfExpr(_) => return true,
            Expr::PlaceHolderVar(phv) => {
                if phv.phlevelsup == 0 {
                    return true;
                }
                // else fall through to check the contained expr
                return expression_tree_walker(node, &mut |n: &Node| contain_var_clause_walker(n));
            }
            _ => {}
        }
    }
    expression_tree_walker(node, &mut |n: &Node| contain_var_clause_walker(n))
}

// ===========================================================================
// contain_vars_of_level (var.c:443-493)
// ===========================================================================

/// `contain_vars_of_level(node, levelsup)` (var.c:443). True iff `node` contains
/// any Var of the specified query level. Recurses into sublinks; may be invoked
/// directly on a Query.
pub fn contain_vars_of_level(node: &Node, levelsup: i32) -> bool {
    let mut sublevels_up = levelsup;
    query_or_expression_tree_walker(
        node,
        &mut |n: &Node| contain_vars_of_level_walker(n, &mut sublevels_up),
        0,
    )
}

/// `contain_vars_of_level_walker` (var.c:454).
fn contain_vars_of_level_walker(node: &Node, sublevels_up: &mut i32) -> bool {
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => return var.varlevelsup as i32 == *sublevels_up,
            Expr::CurrentOfExpr(_) => return *sublevels_up == 0,
            Expr::PlaceHolderVar(phv) => {
                if phv.phlevelsup as i32 == *sublevels_up {
                    return true;
                }
                return expression_tree_walker(node, &mut |n: &Node| {
                    contain_vars_of_level_walker(n, sublevels_up)
                });
            }
            _ => {
                return expression_tree_walker(node, &mut |n: &Node| {
                    contain_vars_of_level_walker(n, sublevels_up)
                })
            }
        }
    }
    if node.node_tag() == ntag::T_Query {
        let q = node.as_query().unwrap();
        *sublevels_up += 1;
        let result = query_tree_walker(
            q,
            &mut |n: &Node| contain_vars_of_level_walker(n, sublevels_up),
            0,
        );
        *sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, &mut |n: &Node| {
        contain_vars_of_level_walker(n, sublevels_up)
    })
}

// ===========================================================================
// contain_vars_returning_old_or_new (var.c:510-536)
// ===========================================================================

/// `contain_vars_returning_old_or_new(node)` (var.c:510). True iff `node`
/// contains any current-level Var whose `varreturningtype` is OLD/NEW, or any
/// current-level `ReturningExpr`. Does not examine subqueries.
pub fn contain_vars_returning_old_or_new(node: &Node) -> bool {
    contain_vars_returning_old_or_new_walker(node)
}

/// `contain_vars_returning_old_or_new_walker` (var.c:516).
fn contain_vars_returning_old_or_new_walker(node: &Node) -> bool {
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => {
                return var.varlevelsup == 0
                    && var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT
            }
            Expr::ReturningExpr(rexpr) => return rexpr.retlevelsup == 0,
            _ => {}
        }
    }
    expression_tree_walker(node, &mut |n: &Node| {
        contain_vars_returning_old_or_new_walker(n)
    })
}

// ===========================================================================
// locate_var_of_level (var.c:554-610)
// ===========================================================================

/// `locate_var_of_level_context` (var.c:52-56).
struct LocateVarOfLevelContext {
    var_location: i32,
    sublevels_up: i32,
}

/// `locate_var_of_level(node, levelsup)` (var.c:554). Find the parse location of
/// any Var of the specified query level, or -1. Recurses into sublinks.
pub fn locate_var_of_level(node: &Node, levelsup: i32) -> i32 {
    let mut context = LocateVarOfLevelContext {
        var_location: -1,
        sublevels_up: levelsup,
    };
    let _ = query_or_expression_tree_walker(
        node,
        &mut |n: &Node| locate_var_of_level_walker(n, &mut context),
        0,
    );
    context.var_location
}

/// `locate_var_of_level_walker` (var.c:570).
fn locate_var_of_level_walker(node: &Node, context: &mut LocateVarOfLevelContext) -> bool {
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => {
                if var.varlevelsup as i32 == context.sublevels_up && var.location >= 0 {
                    context.var_location = var.location;
                    return true;
                }
                return false;
            }
            // since CurrentOfExpr doesn't carry location, nothing we can do
            Expr::CurrentOfExpr(_) => return false,
            // No extra code needed for PlaceHolderVar; just look in contained expr.
            _ => {
                return expression_tree_walker(node, &mut |n: &Node| {
                    locate_var_of_level_walker(n, context)
                })
            }
        }
    }
    if node.node_tag() == ntag::T_Query {
        let q = node.as_query().unwrap();
        context.sublevels_up += 1;
        let result = query_tree_walker(
            q,
            &mut |n: &Node| locate_var_of_level_walker(n, context),
            0,
        );
        context.sublevels_up -= 1;
        return result;
    }
    expression_tree_walker(node, &mut |n: &Node| locate_var_of_level_walker(n, context))
}

// ===========================================================================
// pull_var_clause (var.c:652-752)
// ===========================================================================

/// `PVC_INCLUDE_AGGREGATES` — include `Aggref`s in output list.
pub const PVC_INCLUDE_AGGREGATES: i32 = 0x0001;
/// `PVC_RECURSE_AGGREGATES` — recurse into `Aggref` arguments.
pub const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
/// `PVC_INCLUDE_WINDOWFUNCS` — include `WindowFunc`s in output list.
pub const PVC_INCLUDE_WINDOWFUNCS: i32 = 0x0004;
/// `PVC_RECURSE_WINDOWFUNCS` — recurse into `WindowFunc` arguments.
pub const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
/// `PVC_INCLUDE_PLACEHOLDERS` — include `PlaceHolderVar`s in output list.
pub const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;
/// `PVC_RECURSE_PLACEHOLDERS` — recurse into `PlaceHolderVar` expressions.
pub const PVC_RECURSE_PLACEHOLDERS: i32 = 0x0020;

/// `pull_var_clause_context` (var.c:58-62).
struct PullVarClauseContext<'mcx> {
    varlist: Vec<Expr>,
    flags: i32,
    /// Memory context for deep-copying collected nodes (`Aggref`/`WindowFunc`/
    /// `GroupingFunc` carry context-allocated children that a plain `.clone()`
    /// would panic on; deep-copy via `clone_in`). C `lappend`s the bare pointer;
    /// the owned model needs a copy.
    mcx: mcx::Mcx<'mcx>,
    /// First clone error encountered, propagated out by `pull_var_clause`.
    err: Option<types_error::PgError>,
}

/// `pull_var_clause(node, flags)` (var.c:652). Recursively pull all Var nodes
/// from an expression clause. `Aggref`/`WindowFunc`/`PlaceHolderVar` are handled
/// per the `PVC_*` flag bits; `GroupingFunc` is treated like `Aggref`;
/// `CurrentOfExpr` is ignored. Upper-level vars/aggrefs/PHVs should not be seen.
/// Returns a list of the (cloned) nodes found. Does not examine subqueries.
pub fn pull_var_clause<'mcx>(mcx: mcx::Mcx<'mcx>, node: &Node, flags: i32) -> PgResult<Vec<Expr>> {
    // Assert that caller has not specified inconsistent flags.
    debug_assert_ne!(
        flags & (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES),
        PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES
    );
    debug_assert_ne!(
        flags & (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS),
        PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS
    );
    debug_assert_ne!(
        flags & (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS),
        PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS
    );

    let mut context = PullVarClauseContext {
        varlist: Vec::new(),
        flags,
        mcx,
        err: None,
    };
    pull_var_clause_walker(node, &mut context);
    if let Some(e) = context.err {
        return Err(e);
    }
    Ok(context.varlist)
}

/// `pull_var_clause_walker` (var.c:672).
fn pull_var_clause_walker(node: &Node, context: &mut PullVarClauseContext) -> bool {
    if context.err.is_some() {
        return true;
    }
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => {
                if var.varlevelsup != 0 {
                    panic!("Upper-level Var found where not expected");
                }
                context.varlist.push(Expr::Var(var.clone()));
                return false;
            }
            Expr::Aggref(agg) => {
                if agg.agglevelsup != 0 {
                    panic!("Upper-level Aggref found where not expected");
                }
                if context.flags & PVC_INCLUDE_AGGREGATES != 0 {
                    match node_expr_clone(node, context.mcx) {
                        Ok(c) => context.varlist.push(c),
                        Err(e) => {
                            context.err = Some(e);
                            return true;
                        }
                    }
                    return false; // do NOT descend into the contained expression
                } else if context.flags & PVC_RECURSE_AGGREGATES != 0 {
                    // fall through to recurse into the aggregate's arguments
                } else {
                    panic!("Aggref found where not expected");
                }
            }
            Expr::GroupingFunc(grp) => {
                if grp.agglevelsup != 0 {
                    panic!("Upper-level GROUPING found where not expected");
                }
                if context.flags & PVC_INCLUDE_AGGREGATES != 0 {
                    match node_expr_clone(node, context.mcx) {
                        Ok(c) => context.varlist.push(c),
                        Err(e) => {
                            context.err = Some(e);
                            return true;
                        }
                    }
                    return false;
                } else if context.flags & PVC_RECURSE_AGGREGATES != 0 {
                    // fall through to recurse into the GroupingFunc's arguments
                } else {
                    panic!("GROUPING found where not expected");
                }
            }
            Expr::WindowFunc(_) => {
                // WindowFuncs have no levelsup field to check ...
                if context.flags & PVC_INCLUDE_WINDOWFUNCS != 0 {
                    match node_expr_clone(node, context.mcx) {
                        Ok(c) => context.varlist.push(c),
                        Err(e) => {
                            context.err = Some(e);
                            return true;
                        }
                    }
                    return false;
                } else if context.flags & PVC_RECURSE_WINDOWFUNCS != 0 {
                    // fall through to recurse into the windowfunc's arguments
                } else {
                    panic!("WindowFunc found where not expected");
                }
            }
            Expr::PlaceHolderVar(phv) => {
                if phv.phlevelsup != 0 {
                    panic!("Upper-level PlaceHolderVar found where not expected");
                }
                if context.flags & PVC_INCLUDE_PLACEHOLDERS != 0 {
                    context.varlist.push(Expr::PlaceHolderVar(phv.clone()));
                    return false;
                } else if context.flags & PVC_RECURSE_PLACEHOLDERS != 0 {
                    // fall through to recurse into the placeholder's expression
                } else {
                    panic!("PlaceHolderVar found where not expected");
                }
            }
            _ => {}
        }
    }
    expression_tree_walker(node, &mut |n: &Node| pull_var_clause_walker(n, context))
}

/// Deep-copy the `Expr` payload of a `Node::Expr(..)` arm into `mcx`. Only called
/// on arms known to be `Node::Expr`. Uses `Expr::clone_in` (not a plain
/// `.clone()`) because `Aggref`/`WindowFunc`/`GroupingFunc` carry
/// context-allocated children whose derived `.clone()` panics.
fn node_expr_clone<'mcx>(node: &Node, mcx: mcx::Mcx<'mcx>) -> PgResult<Expr> {
    match node.as_expr() {
        Some(e) => e.clone_in(mcx),
        None => unreachable!("node_expr_clone on non-Expr node"),
    }
}

// ===========================================================================
// Seam installs — the join-path enumerator (`get_memoize_path`) consumes these
// NodeId-shaped views; var.c owns them and resolves the handle internally.
// ===========================================================================

/// `pull_varnos(root, (Node *) node)` (var.c) — installed seam. Resolves the
/// arena `NodeId` to `&Expr`, wraps as `Node::Expr`, and walks. The C `root` is
/// always passed here (the join-path caller has a real `PlannerInfo`).
fn seam_pull_varnos(root: &PlannerInfo, node: NodeId) -> Relids {
    let scratch = mcx::MemoryContext::new("pull_varnos seam wrapper");
    let wrapped = node_expr_wrapper(root.node(node), scratch.mcx());
    pull_varnos(Some(root), &wrapped)
}

/// `bms_is_member(0, pull_varnos(root, node))` (cost_incremental_sort) —
/// installed seam. True iff the expression references a Var with `varno 0`.
fn seam_pull_varnos_contains_zero(root: &PlannerInfo, node: NodeId) -> bool {
    let scratch = mcx::MemoryContext::new("pull_varnos contains-zero seam wrapper");
    let wrapped = node_expr_wrapper(root.node(node), scratch.mcx());
    let relids = pull_varnos(Some(root), &wrapped);
    bms_is_member(0, &relids)
}

/// `bms_is_member(x, a)` (bitmapset.c).
fn bms_is_member(x: i32, a: &Relids) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let wnum = (x as usize) / 64;
    if wnum >= a.words.len() {
        return false;
    }
    (a.words[wnum] & (1u64 << ((x as usize) % 64))) != 0
}

/// `pull_vars_of_level((Node *) node, levelsup)` (var.c) — installed seam.
/// Returns the level-`levelsup` Vars/PHVs as fresh arena handles.
fn seam_pull_vars_of_level(
    root: &mut PlannerInfo,
    node: NodeId,
    levelsup: i32,
) -> PgResult<Vec<NodeId>> {
    let scratch = mcx::MemoryContext::new("pull_vars_of_level seam wrapper");
    let wrapped = node_expr_wrapper(root.node(node), scratch.mcx());
    let vars = pull_vars_of_level(&wrapped, levelsup);
    let mut out = Vec::with_capacity(vars.len());
    for v in vars {
        out.push(root.alloc_node(v));
    }
    Ok(out)
}

/// `IsA(node, Var)` — installed seam.
fn seam_node_is_var(root: &PlannerInfo, node: NodeId) -> bool {
    matches!(root.node(node), Expr::Var(_))
}

/// `((Var *) node)->varno` — installed seam.
fn seam_var_varno(root: &PlannerInfo, node: NodeId) -> i32 {
    match root.node(node) {
        Expr::Var(v) => v.varno,
        other => panic!("var_varno: node is not a Var (tag {:?})", core::mem::discriminant(other)),
    }
}

/// `pull_varattnos(node, varno, &varattnos)` (var.c) — installed seam. The
/// existing `backend-optimizer-util-var-seams` contract takes the expression by
/// value and `varno: u32`, accumulating into a `types_nodes::Bitmapset`
/// allocated in `mcx`; this bridges the lifetime-free word-set collector into
/// that `mcx`-owned result.
fn seam_pull_varattnos<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: &Expr,
    varno: u32,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    let wrapped = node_expr_wrapper(node, mcx);
    let relids = pull_varattnos(&wrapped, varno as i32, None);
    match relids {
        None => Ok(None),
        Some(bms) => {
            // Materialize the word set into an mcx-owned `types_nodes::Bitmapset`
            // by adding each member through the canonical bms_add_member.
            let mut acc: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
            let mut bit: i32 = -1;
            loop {
                bit = next_member(&bms.words, bit);
                if bit < 0 {
                    break;
                }
                acc = Some(backend_nodes_core::bitmapset::bms_add_member(mcx, acc, bit)?);
            }
            Ok(acc)
        }
    }
}

/// Smallest set member greater than `prevbit` in a word-vector, or -1 if none.
/// Mirrors `bms_next_member` over the canonical word storage.
fn next_member(words: &[u64], prevbit: i32) -> i32 {
    let mut bit = prevbit;
    let total = (words.len() as i32) * BITS_PER_WORD;
    loop {
        bit += 1;
        if bit >= total {
            return -1;
        }
        let w = words[wordnum(bit)];
        if w & (1u64 << bitnum(bit)) != 0 {
            return bit;
        }
    }
}

/// Materialize a lifetime-free word-set [`Relids`] into an `mcx`-owned
/// `types_nodes::Bitmapset` by adding each member through the canonical
/// `bms_add_member`. `None` → `None` (the empty set).
fn relids_to_mcx_bitmapset<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relids: Relids,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    match relids {
        None => Ok(None),
        Some(bms) => {
            let mut acc: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>> = None;
            let mut bit: i32 = -1;
            loop {
                bit = next_member(&bms.words, bit);
                if bit < 0 {
                    break;
                }
                acc = Some(backend_nodes_core::bitmapset::bms_add_member(mcx, acc, bit)?);
            }
            Ok(acc)
        }
    }
}

/// `contain_var_clause(node)` (var.c) — installed seam. Pure predicate; clauses.c
/// (`contain_leaked_vars`/`is_pseudo_constant_clause`) consumes it.
fn seam_contain_var_clause(node: &Expr) -> bool {
    let scratch = mcx::MemoryContext::new("contain_var_clause wrapper");
    let wrapped = node_expr_wrapper(node, scratch.mcx());
    contain_var_clause(&wrapped)
}

/// `pull_varnos(root, node)` (var.c) — installed seam over the rootless
/// `&Expr`-only contract (matches a `root == NULL` call), returning the relids
/// as an `mcx`-owned `types_nodes::Bitmapset`. Consumed by clauses.c.
fn seam_pull_varnos_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: &Expr,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>> {
    let wrapped = node_expr_wrapper(node, mcx);
    let relids = pull_varnos(None, &wrapped);
    relids_to_mcx_bitmapset(mcx, relids)
}

/// `NumRelids(root, clause)` (clauses.c) — installed seam. The number of
/// distinct relids referenced by `clause`. The rootless seam contract cannot
/// thread `root->outer_join_rels`, so this returns `bms_num_members(pull_varnos
/// (NULL, clause))` (the documented limitation of the rootless ride).
fn seam_num_relids(node: &Expr) -> PgResult<i32> {
    let scratch = mcx::MemoryContext::new("num_relids wrapper");
    let wrapped = node_expr_wrapper(node, scratch.mcx());
    let relids = pull_varnos(None, &wrapped);
    let n = match relids {
        None => 0,
        Some(bms) => bms.words.iter().map(|w| w.count_ones() as i32).sum(),
    };
    Ok(n)
}

/// `NumRelids(root, clause)` (clauses.c:2131) — the root-aware seam. The number
/// of distinct base relations referenced in `clause`:
/// `bms_num_members(bms_difference(pull_varnos(root, clause),
/// root->outer_join_rels))`. Unlike `seam_num_relids` (the rootless ride), this
/// threads `root` so `outer_join_rels` is subtracted exactly as C does.
fn seam_num_relids_root(root: &mut PlannerInfo, clause: &Expr) -> PgResult<i32> {
    let scratch = mcx::MemoryContext::new("num_relids_root wrapper");
    let wrapped = node_expr_wrapper(clause, scratch.mcx());
    let varnos = pull_varnos(Some(root), &wrapped);
    let varnos = bms_difference(&varnos, &root.outer_join_rels);
    let n = match varnos {
        None => 0,
        Some(bms) => bms.words.iter().map(|w| w.count_ones() as i32).sum(),
    };
    Ok(n)
}

/// Install the var.c-owned seams consumed by the join-path enumerator, by
/// clauses.c (`contain_var_clause`/`pull_varnos`/`num_relids`), and by
/// `nodeModifyTable` (`pull_varattnos`).
pub fn init_seams() {
    use backend_optimizer_path_joinpath_seams as jp;
    jp::pull_varnos::set(seam_pull_varnos);
    jp::pull_vars_of_level::set(seam_pull_vars_of_level);
    jp::node_is_var::set(seam_node_is_var);
    jp::var_varno::set(seam_var_varno);
    // NOTE: `expr_hash_eq_operator` (joinpath.c `paraminfo_get_equal_hashops`)
    // is installed by its rightful owner, backend-optimizer-util-joininfo; it is
    // not a var.c function. Installing it here too tripped the "seam installed
    // twice" guard at boot.

    use backend_optimizer_util_var_seams as vs;
    vs::pull_varattnos::set(seam_pull_varattnos);
    vs::contain_var_clause::set(seam_contain_var_clause);
    vs::pull_varnos::set(seam_pull_varnos_expr);
    vs::num_relids::set(seam_num_relids);

    // The var.c functions the equivclass-ext consumer crate declares (its
    // consumers — initsplan.c `build_base_rel_tlists`, equivclass.c — call them
    // through that no-owner stub crate). var.c is their real owner.
    use backend_optimizer_path_equivclass_ext_seams as eqext;
    eqext::pull_var_clause::set(seam_eqext_pull_var_clause);
    eqext::pull_var_clause_list::set(seam_eqext_pull_var_clause_list);
    eqext::pull_varnos::set(seam_eqext_pull_varnos);

    // joininfo.c's `pull_varnos((Node *) ...)` cycle-break (its `add_join_clause_
    // to_rels`/`have_relevant_joinclause` callers ride this no-owner ext stub).
    // Same `root`-threading shape as the equivclass-ext seam; var.c is the owner.
    use backend_optimizer_util_joininfo_ext_seams as joinext;
    joinext::pull_varnos_expr::set(seam_eqext_pull_varnos);
    // `pull_var_clause((Node *) expr, flags)` over a rootless `&Expr` — same
    // var.c owner, same shape as the equivclass-ext leg above.
    joinext::pull_var_clause_expr::set(seam_eqext_pull_var_clause);

    // clauses.c's root-aware `NumRelids(root, clause)` (path-small.c's
    // mark_index_clause_usable / clauselist_selectivity ride this). The rootless
    // `vs::num_relids` above cannot subtract `root->outer_join_rels`; this one
    // does, matching C exactly.
    use backend_optimizer_path_small_seams as ps;
    ps::num_relids::set(seam_num_relids_root);
    // tidpath.c's `IsTidEqualClause` rides `pull_varnos(root, (Node *) arg2)`
    // over an inline `&Expr` operand of the CTID = const clause. var.c owns
    // pull_varnos; same root-threaded shape as the equivclass-ext leg.
    ps::pull_varnos_expr::set(seam_ps_pull_varnos_expr);

    // cost_incremental_sort's `bms_is_member(0, pull_varnos(root, em_expr))`
    // (costsize.c). var.c owns pull_varnos.
    use backend_optimizer_path_costsize_seams as cz;
    cz::pull_varnos_contains_zero::set(seam_pull_varnos_contains_zero);

    // `pull_vars_of_level((Node *) node, levelsup)` (var.c:338) over a borrowed
    // parse `Node` — `extract_lateral_references` (initsplan.c) gathers the
    // level-`levelsup` Vars of a LATERAL RTE's parse subtrees. The
    // init-subselect-ext stub crate declares it (it cannot name a whole parse
    // `Node`); var.c is the real owner and installs it here.
    use backend_optimizer_plan_init_subselect_ext_seams as isub;
    isub::pull_vars_of_level_node::set(|node, levelsup| pull_vars_of_level(node, levelsup));
    // The RTE_SUBQUERY arm walks `rte->subquery` (an owned `Query`, not a `Node`)
    // through the sibling `_query` seam (same owner, var.c).
    isub::pull_vars_of_level_query::set(|query, levelsup| {
        pull_vars_of_level_query(query, levelsup)
    });
}

/// `pull_var_clause((Node *) node, flags)` (var.c) — the equivclass-ext seam
/// over a single rootless `&Expr`.
fn seam_eqext_pull_var_clause(node: &Expr, flags: i32) -> Vec<Expr> {
    // Wrap the `&Expr` as a `Node` for the Node-based walker. A bare
    // `Expr::clone` panics on an `Aggref` (context-allocated `TargetEntry`
    // args); `node_expr_wrapper` deep-copies into a scratch context via the
    // non-panicking `clone_in`, observationally identical to C's borrowed ptr.
    // The equivclass-ext caller only pulls Vars (self-contained clones), so the
    // collected nodes do not retain `scratch`-allocated children.
    let scratch = mcx::MemoryContext::new("pull_var_clause wrapper");
    let wrapped = node_expr_wrapper(node, scratch.mcx());
    pull_var_clause(scratch.mcx(), &wrapped, flags).unwrap_or_default()
}

/// `pull_var_clause((Node *) exprs, flags)` (var.c) over a `List` — run the walk
/// over each expression and concatenate, matching the C single call over the
/// whole list (the per-element order is preserved).
fn seam_eqext_pull_var_clause_list(nodes: Vec<Expr>, flags: i32) -> Vec<Expr> {
    let mut out: Vec<Expr> = Vec::new();
    for node in nodes.iter() {
        let scratch = mcx::MemoryContext::new("pull_var_clause_list wrapper");
        let wrapped = node_expr_wrapper(node, scratch.mcx());
        out.extend(pull_var_clause(scratch.mcx(), &wrapped, flags).unwrap_or_default());
    }
    out
}

/// `pull_varnos(root, (Node *) expr)` (var.c) — the equivclass-ext seam,
/// threading `root` so PlaceHolderVars are processed (the `root != NULL` path).
fn seam_eqext_pull_varnos(root: &PlannerInfo, expr: &Expr) -> Relids {
    let scratch = mcx::MemoryContext::new("pull_varnos wrapper");
    let wrapped = node_expr_wrapper(expr, scratch.mcx());
    pull_varnos(Some(root), &wrapped)
}

/// `pull_varnos(root, (Node *) expr)` (var.c) for tidpath.c's path-small seam.
/// Same root-threaded behavior as the equivclass-ext leg; the only difference
/// is the `&mut PlannerInfo` receiver the path-small declaration mirrors.
fn seam_ps_pull_varnos_expr(root: &mut PlannerInfo, expr: &Expr) -> Relids {
    let scratch = mcx::MemoryContext::new("pull_varnos wrapper");
    let wrapped = node_expr_wrapper(expr, scratch.mcx());
    pull_varnos(Some(root), &wrapped)
}
