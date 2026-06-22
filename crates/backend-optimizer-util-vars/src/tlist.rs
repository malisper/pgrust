//! `optimizer/util/tlist.c` — target-list and `PathTarget` manipulation.
//!
//! These routines operate purely over `TargetEntry` / `SortGroupClause` /
//! `PathTarget`. Structural expression equality (`equal()`) crosses to the
//! not-yet-ported equalfuncs.c via the
//! [`backend_nodes_equalfuncs_seams::equal_expr`] seam.
//!
//! # Arena adaptation
//!
//! The consumer-facing [`PathTarget`] stores its expressions as
//! [`NodeId`] handles into the [`PlannerInfo`] node arena (`exprs:
//! Vec<NodeId>`), and `sortgrouprefs` as a `Vec<u32>` whose **empty** value is
//! the C `sortgrouprefs == NULL`. The `PathTarget` routines that need to read or
//! compare those expressions therefore take a `&PlannerInfo` (or `&mut` /
//! `mcx`) the C versions don't — the minimal change to resolve handles in the
//! arena model.
//!
//! `make_pathtarget_from_tlist` is defined here over the arena model (it takes
//! `&PlannerInfo` to resolve the `TargetEntry` handles, then builds the
//! `PathTarget.exprs`/`sortgrouprefs` directly). The `create_pathtarget()`
//! macro wrapper (`set_pathtarget_cost_width(root, make_pathtarget_from_tlist(...))`)
//! is applied by the caller in the planner crate, which can reach costsize.c.
//! The SRF-leveling family (`split_pathtarget_at_srfs*`) is defined here over
//! the arena model; the `root->parse` grouping flags it consults at the
//! grouping boundary are passed in by the planner caller (which holds the
//! `PlannerRun` to resolve `root.parse`) as [`SplitGroupingFlags`].

#![allow(non_snake_case)]

extern crate alloc;
use alloc::vec::Vec;

use backend_nodes_core::makefuncs::make_target_entry;
use backend_nodes_core::nodefuncs::{expr_collation, expr_type};
use backend_nodes_equalfuncs_seams::equal_expr;
use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::primnodes::{Expr, TargetEntry, Var};
use types_nodes::rawnodes::SortGroupClause;
use types_pathnodes::{NodeId, PathTarget, PlannerInfo, VOLATILITY_NOVOLATILE, VOLATILITY_UNKNOWN};

// ===========================================================================
// tlist_member / tlist_member_match_var (tlist.c:88-128)
// ===========================================================================

/// `tlist_member(node, targetlist)` (tlist.c:88). Finds the (first) member of
/// `targetlist` whose expression is `equal()` to `node`. `None` if none.
pub fn tlist_member<'a, 'mcx>(
    node: &Expr,
    targetlist: &'a [TargetEntry<'mcx>],
) -> Option<&'a TargetEntry<'mcx>> {
    targetlist.iter().find(|tle| match tle.expr.as_deref() {
        Some(e) => equal_expr::call(node, e),
        None => false,
    })
}

/// `tlist_member_match_var(var, targetlist)` (tlist.c:111). Match `var` against
/// each tlist entry's `Var` on varno/varattno/varlevelsup/vartype only.
pub fn tlist_member_match_var<'a, 'mcx>(
    var: &Var,
    targetlist: &'a [TargetEntry<'mcx>],
) -> Option<&'a TargetEntry<'mcx>> {
    targetlist.iter().find(|tle| match tle.expr.as_deref() {
        Some(Expr::Var(tlvar)) => {
            var.varno == tlvar.varno
                && var.varattno == tlvar.varattno
                && var.varlevelsup == tlvar.varlevelsup
                && var.vartype == tlvar.vartype
        }
        _ => false,
    })
}

// ===========================================================================
// add_to_flat_tlist (tlist.c:141)
// ===========================================================================

/// `add_to_flat_tlist(tlist, exprs)` (tlist.c:141). Append each of `exprs` that
/// is not already a member to the flattened `tlist`, with the next `resno`.
/// `copyObject(expr)` is a deep clone of the lifetime-free `Expr`. Returns the
/// extended tlist.
pub fn add_to_flat_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    mut tlist: Vec<TargetEntry<'mcx>>,
    exprs: &[Expr<'mcx>],
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    let mut next_resno = tlist.len() as AttrNumber + 1;
    for expr in exprs {
        if tlist_member(expr, &tlist).is_none() {
            let tle = make_target_entry(mcx, expr.clone(), next_resno, None, false)?;
            next_resno += 1;
            tlist.push(tle);
        }
    }
    Ok(tlist)
}

// ===========================================================================
// get_tlist_exprs / count_nonjunk_tlist_entries (tlist.c:172, 195)
// ===========================================================================

/// `get_tlist_exprs(tlist, includeJunk)` (tlist.c:172). The expression subtrees
/// of `tlist`; resjunk columns are skipped unless `includeJunk`. Clones each
/// expr (the owned tree does not alias).
pub fn get_tlist_exprs<'mcx>(tlist: &[TargetEntry<'mcx>], includeJunk: bool) -> Vec<Expr<'mcx>> {
    let mut result = Vec::new();
    for tle in tlist {
        if tle.resjunk && !includeJunk {
            continue;
        }
        if let Some(e) = tle.expr.as_deref() {
            result.push(e.clone());
        }
    }
    result
}

/// `count_nonjunk_tlist_entries(tlist)` (tlist.c:195).
pub fn count_nonjunk_tlist_entries(tlist: &[TargetEntry<'_>]) -> i32 {
    tlist.iter().filter(|tle| !tle.resjunk).count() as i32
}

// ===========================================================================
// tlist_same_exprs / _datatypes / _collations (tlist.c:227, 257, 291)
// ===========================================================================

/// `tlist_same_exprs(tlist1, tlist2)` (tlist.c:227). True iff the two tlists
/// have equal expressions in order (labeling fields ignored).
pub fn tlist_same_exprs(tlist1: &[TargetEntry<'_>], tlist2: &[TargetEntry<'_>]) -> bool {
    if tlist1.len() != tlist2.len() {
        return false;
    }
    tlist1.iter().zip(tlist2.iter()).all(|(t1, t2)| {
        match (t1.expr.as_deref(), t2.expr.as_deref()) {
            (Some(a), Some(b)) => equal_expr::call(a, b),
            (None, None) => true,
            _ => false,
        }
    })
}

/// `tlist_same_datatypes(tlist, colTypes, junkOK)` (tlist.c:257). True iff the
/// non-junk tlist exprs' result types equal `colTypes` in order.
pub fn tlist_same_datatypes(
    tlist: &[TargetEntry<'_>],
    colTypes: &[Oid],
    junkOK: bool,
) -> PgResult<bool> {
    let mut col = colTypes.iter();
    for tle in tlist {
        if tle.resjunk {
            if !junkOK {
                return Ok(false);
            }
        } else {
            match col.next() {
                None => return Ok(false), // tlist longer than colTypes
                Some(&ct) => {
                    if expr_type(tle.expr.as_deref())? != ct {
                        return Ok(false);
                    }
                }
            }
        }
    }
    if col.next().is_some() {
        return Ok(false); // tlist shorter than colTypes
    }
    Ok(true)
}

/// `tlist_same_collations(tlist, colCollations, junkOK)` (tlist.c:291).
pub fn tlist_same_collations(
    tlist: &[TargetEntry<'_>],
    colCollations: &[Oid],
    junkOK: bool,
) -> PgResult<bool> {
    let mut col = colCollations.iter();
    for tle in tlist {
        if tle.resjunk {
            if !junkOK {
                return Ok(false);
            }
        } else {
            match col.next() {
                None => return Ok(false),
                Some(&cc) => {
                    if expr_collation(tle.expr.as_deref())? != cc {
                        return Ok(false);
                    }
                }
            }
        }
    }
    if col.next().is_some() {
        return Ok(false);
    }
    Ok(true)
}

// ===========================================================================
// apply_tlist_labeling (tlist.c:327)
// ===========================================================================

/// `apply_tlist_labeling(dest_tlist, src_tlist)` (tlist.c:327). Copy the
/// labeling attributes (`resname`/`ressortgroupref`/`resorigtbl`/`resorigcol`/
/// `resjunk`) of `src_tlist` onto `dest_tlist` element-wise.
pub fn apply_tlist_labeling<'mcx>(
    mcx: Mcx<'mcx>,
    dest_tlist: &mut [TargetEntry<'mcx>],
    src_tlist: &[TargetEntry<'_>],
) -> PgResult<()> {
    assert_eq!(
        dest_tlist.len(),
        src_tlist.len(),
        "apply_tlist_labeling: tlist length mismatch"
    );
    for (dest_tle, src_tle) in dest_tlist.iter_mut().zip(src_tlist.iter()) {
        assert_eq!(dest_tle.resno, src_tle.resno);
        dest_tle.resname = match &src_tle.resname {
            Some(s) => Some(mcx::PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        dest_tle.ressortgroupref = src_tle.ressortgroupref;
        dest_tle.resorigtbl = src_tle.resorigtbl;
        dest_tle.resorigcol = src_tle.resorigcol;
        dest_tle.resjunk = src_tle.resjunk;
    }
    Ok(())
}

// ===========================================================================
// get_sortgroupref_tle / get_sortgroupclause_* / get_sortgrouplist_exprs
// (tlist.c:354-...)
// ===========================================================================

/// `get_sortgroupref_tle(sortref, targetList)` (tlist.c). The tlist entry with
/// `ressortgroupref == sortref`; `elog(ERROR)` if none.
pub fn get_sortgroupref_tle<'a, 'mcx>(
    sortref: Index,
    targetList: &'a [TargetEntry<'mcx>],
) -> PgResult<&'a TargetEntry<'mcx>> {
    targetList
        .iter()
        .find(|tle| tle.ressortgroupref == sortref)
        .ok_or_else(|| PgError::error("ORDER/GROUP BY expression not found in targetlist"))
}

/// `get_sortgroupclause_tle(sgClause, targetList)` (tlist.c).
pub fn get_sortgroupclause_tle<'a, 'mcx>(
    sgClause: &SortGroupClause,
    targetList: &'a [TargetEntry<'mcx>],
) -> PgResult<&'a TargetEntry<'mcx>> {
    get_sortgroupref_tle(sgClause.tleSortGroupRef, targetList)
}

/// `get_sortgroupclause_expr(sgClause, targetList)` (tlist.c). The matching
/// tlist entry's expression (cloned).
pub fn get_sortgroupclause_expr<'mcx>(
    sgClause: &SortGroupClause,
    targetList: &[TargetEntry<'mcx>],
) -> PgResult<Option<Expr<'mcx>>> {
    let tle = get_sortgroupclause_tle(sgClause, targetList)?;
    Ok(tle.expr.as_deref().cloned())
}

/// `get_sortgrouplist_exprs(sgClauses, targetList)` (tlist.c). The referenced
/// tlist expressions, in order.
pub fn get_sortgrouplist_exprs<'mcx>(
    sgClauses: &[SortGroupClause],
    targetList: &[TargetEntry<'mcx>],
) -> PgResult<Vec<Option<Expr<'mcx>>>> {
    let mut result = Vec::with_capacity(sgClauses.len());
    for sortcl in sgClauses {
        result.push(get_sortgroupclause_expr(sortcl, targetList)?);
    }
    Ok(result)
}

// ===========================================================================
// get_sortgroupref_clause(_noerr) (tlist.c)
// ===========================================================================

/// `get_sortgroupref_clause(sortref, clauses)` (tlist.c). `elog(ERROR)` if none.
pub fn get_sortgroupref_clause(sortref: Index, clauses: &[SortGroupClause]) -> PgResult<SortGroupClause> {
    clauses
        .iter()
        .find(|cl| cl.tleSortGroupRef == sortref)
        .copied()
        .ok_or_else(|| PgError::error("ORDER/GROUP BY expression not found in list"))
}

/// `get_sortgroupref_clause_noerr(sortref, clauses)` (tlist.c). `None` if none.
pub fn get_sortgroupref_clause_noerr(
    sortref: Index,
    clauses: &[SortGroupClause],
) -> Option<SortGroupClause> {
    clauses
        .iter()
        .find(|cl| cl.tleSortGroupRef == sortref)
        .copied()
}

// ===========================================================================
// extract_grouping_* (tlist.c)
// ===========================================================================

/// `extract_grouping_ops(groupClause)` (tlist.c). The equality operator OIDs.
pub fn extract_grouping_ops(groupClause: &[SortGroupClause]) -> Vec<Oid> {
    groupClause
        .iter()
        .map(|cl| {
            debug_assert!(cl.eqop != 0, "extract_grouping_ops: invalid eqop");
            cl.eqop
        })
        .collect()
}

/// `extract_grouping_collations(groupClause, tlist)` (tlist.c).
pub fn extract_grouping_collations(
    groupClause: &[SortGroupClause],
    tlist: &[TargetEntry<'_>],
) -> PgResult<Vec<Oid>> {
    let mut out = Vec::with_capacity(groupClause.len());
    for cl in groupClause {
        let tle = get_sortgroupclause_tle(cl, tlist)?;
        out.push(expr_collation(tle.expr.as_deref())?);
    }
    Ok(out)
}

/// `extract_grouping_cols(groupClause, tlist)` (tlist.c).
pub fn extract_grouping_cols(
    groupClause: &[SortGroupClause],
    tlist: &[TargetEntry<'_>],
) -> PgResult<Vec<AttrNumber>> {
    let mut out = Vec::with_capacity(groupClause.len());
    for cl in groupClause {
        let tle = get_sortgroupclause_tle(cl, tlist)?;
        out.push(tle.resno);
    }
    Ok(out)
}

// ===========================================================================
// grouping_is_sortable / grouping_is_hashable (tlist.c)
// ===========================================================================

/// `grouping_is_sortable(groupClause)` (tlist.c). True iff every clause has a
/// valid sortop.
pub fn grouping_is_sortable(groupClause: &[SortGroupClause]) -> bool {
    groupClause.iter().all(|cl| cl.sortop != 0)
}

/// `grouping_is_hashable(groupClause)` (tlist.c). True iff every clause is
/// hashable.
pub fn grouping_is_hashable(groupClause: &[SortGroupClause]) -> bool {
    groupClause.iter().all(|cl| cl.hashable)
}

// ===========================================================================
// PathTarget builders (tlist.c)
// ===========================================================================

/// `create_empty_pathtarget()` (tlist.c). A fresh empty `PathTarget`.
pub fn create_empty_pathtarget() -> PathTarget {
    PathTarget {
        exprs: Vec::new(),
        sortgrouprefs: Vec::new(),
        cost: Default::default(),
        width: 0,
        has_volatile_expr: VOLATILITY_UNKNOWN,
    }
}

/// `copy_pathtarget(src)` (tlist.c). A copy that owns its own `exprs` list (the
/// arena handles / sortgrouprefs are scalars, so a clone is exact).
pub fn copy_pathtarget(src: &PathTarget) -> PathTarget {
    src.clone()
}

/// `make_pathtarget_from_tlist(tlist)` (tlist.c:614).
///
/// Build a `PathTarget` from a target list. In C the input is a `List` of
/// `TargetEntry *`; in the arena model `tlist` is a slice of [`NodeId`] handles
/// that each resolve (through `root`) to a `TargetEntryNode`. The PathTarget's
/// `exprs` becomes the list of each TargetEntry's `expr` handle, and
/// `sortgrouprefs[i]` its `ressortgroupref` (always allocated, length =
/// `tlist.len()`, matching the C `palloc(list_length(tlist) * sizeof(Index))`).
///
/// Volatility is marked UNKNOWN; `contain_volatile_functions` will fill it in
/// the first time it is asked. Cost/width are left 0 — the caller is expected to
/// wrap this in `create_pathtarget()` (i.e. run `set_pathtarget_cost_width`).
pub fn make_pathtarget_from_tlist(root: &PlannerInfo, tlist: &[NodeId]) -> PathTarget {
    let mut target = create_empty_pathtarget();
    // C always allocates a sortgrouprefs array sized to the tlist, so model it
    // as a full-length (zero-filled) vector that we then stamp.
    target.sortgrouprefs = alloc::vec![0u32; tlist.len()];
    for (i, &te_id) in tlist.iter().enumerate() {
        let te = root.targetentry(te_id);
        target.exprs.push(te.expr);
        target.sortgrouprefs[i] = te.ressortgroupref;
    }
    target.has_volatile_expr = VOLATILITY_UNKNOWN;
    target
}

/// `add_column_to_pathtarget(target, expr, sortgroupref)` (tlist.c). Append the
/// expr handle and its sortgroupref, growing the `sortgrouprefs` array as C
/// does (only allocate it when a nonzero ref is added to a previously-unlabeled
/// target). Resets cached volatility to UNKNOWN.
pub fn add_column_to_pathtarget(target: &mut PathTarget, expr: NodeId, sortgroupref: u32) {
    target.exprs.push(expr);
    let nexprs = target.exprs.len();
    if !target.sortgrouprefs.is_empty() {
        // grow to match exprs length, new slot = sortgroupref
        target.sortgrouprefs.resize(nexprs, 0);
        target.sortgrouprefs[nexprs - 1] = sortgroupref;
    } else if sortgroupref != 0 {
        // Adding sortgroupref labeling to a previously unlabeled target:
        // allocate a zero-filled array of the current length.
        target.sortgrouprefs.resize(nexprs, 0);
        target.sortgrouprefs[nexprs - 1] = sortgroupref;
    }
    // Reset has_volatile_expr to UNKNOWN (left for contain_volatile_functions).
    if target.has_volatile_expr == VOLATILITY_NOVOLATILE {
        target.has_volatile_expr = VOLATILITY_UNKNOWN;
    }
}

/// `add_new_column_to_pathtarget(target, expr)` (tlist.c). Append `expr` (with a
/// zero sortgroupref) iff not already `list_member` (equal) of `target.exprs`.
/// Resolves arena handles through `root` for the equality test.
pub fn add_new_column_to_pathtarget(root: &PlannerInfo, target: &mut PathTarget, expr: NodeId) {
    let expr_node = root.node(expr);
    let present = target
        .exprs
        .iter()
        .any(|&id| equal_expr::call(expr_node, root.node(id)));
    if !present {
        add_column_to_pathtarget(target, expr, 0);
    }
}

/// `add_new_columns_to_pathtarget(target, exprs)` (tlist.c).
pub fn add_new_columns_to_pathtarget(root: &PlannerInfo, target: &mut PathTarget, exprs: &[NodeId]) {
    for &expr in exprs {
        add_new_column_to_pathtarget(root, target, expr);
    }
}

/// `apply_pathtarget_labeling_to_tlist(tlist, target)` (tlist.c). For each
/// `target` column with a nonzero sortgroupref, find the matching tlist entry
/// (Var: weakened match; otherwise `equal()`) and stamp its `ressortgroupref`.
/// Complains if there is no place for the label, or if a column would be labeled
/// twice. Resolves the target's arena expr handles through `root`.
pub fn apply_pathtarget_labeling_to_tlist<'mcx>(
    root: &PlannerInfo,
    tlist: &mut [TargetEntry<'mcx>],
    target: &PathTarget,
) -> PgResult<()> {
    // Nothing to do if PathTarget has no sortgrouprefs data.
    if target.sortgrouprefs.is_empty() {
        return Ok(());
    }
    for (i, &expr_id) in target.exprs.iter().enumerate() {
        let sgr = target.sortgrouprefs.get(i).copied().unwrap_or(0);
        if sgr != 0 {
            let expr = root.node(expr_id);
            // Find the matching tlist index (immutable borrow), then stamp it.
            let pos = match expr {
                Expr::Var(v) => find_match_var_pos(v, tlist),
                _ => find_match_pos(expr, tlist),
            };
            let idx = pos.ok_or_else(|| {
                PgError::error("ORDER/GROUP BY expression not found in targetlist")
            })?;
            if tlist[idx].ressortgroupref != 0 {
                return Err(PgError::error(
                    "targetlist item has multiple sortgroupref labels",
                ));
            }
            tlist[idx].ressortgroupref = sgr;
        }
    }
    Ok(())
}

/// Index of the first tlist entry whose expr is `equal()` to `expr`.
fn find_match_pos(expr: &Expr, tlist: &[TargetEntry<'_>]) -> Option<usize> {
    tlist.iter().position(|tle| match tle.expr.as_deref() {
        Some(e) => equal_expr::call(expr, e),
        None => false,
    })
}

/// Index of the first tlist entry matching `var` on varno/varattno/varlevelsup/
/// vartype (the weakened Var rule).
fn find_match_var_pos(var: &Var, tlist: &[TargetEntry<'_>]) -> Option<usize> {
    tlist.iter().position(|tle| match tle.expr.as_deref() {
        Some(Expr::Var(tlvar)) => {
            var.varno == tlvar.varno
                && var.varattno == tlvar.varattno
                && var.varlevelsup == tlvar.varlevelsup
                && var.vartype == tlvar.vartype
        }
        _ => false,
    })
}

/// `make_tlist_from_pathtarget(target)` (tlist.c). Build a `TargetEntry` list
/// from `target`'s expressions (resolved through the arena), carrying each
/// column's sortgroupref. Allocates each `TargetEntry` in `mcx`.
pub fn make_tlist_from_pathtarget<'mcx>(
    root: &PlannerInfo,
    mcx: Mcx<'mcx>,
    target: &PathTarget,
) -> PgResult<Vec<TargetEntry<'mcx>>> {
    let mut tlist = Vec::with_capacity(target.exprs.len());
    for (i, &expr_id) in target.exprs.iter().enumerate() {
        // Deep-copy via `Expr::clone_in` (C copyObject); the derived
        // `Expr::clone` panics on `Aggref`/`SubLink`/… payloads.
        let expr = root.node(expr_id).clone_in(mcx)?;
        let mut tle = make_target_entry(mcx, expr, (i as AttrNumber) + 1, None, false)?;
        if !target.sortgrouprefs.is_empty() {
            tle.ressortgroupref = target.sortgrouprefs[i];
        }
        tlist.push(tle);
    }
    Ok(tlist)
}

// ===========================================================================
// split_pathtarget_at_srfs family (tlist.c:833-1336)
//
// Splits a PathTarget that contains set-returning functions (SRFs) into a
// chain of levels, so that each level has SRFs only at the top of its tlist
// (the only place a ProjectSet node can evaluate them). See the C header
// comment on split_pathtarget_at_srfs_extended for the algorithm.
//
// Arena adaptation: a PathTarget's exprs are `NodeId` handles into the
// PlannerInfo node arena, and the C "Node *expr" carried by a
// `split_pathtarget_item` becomes an owned `Expr` (the resolved, copied
// subexpression). Building an intermediate PathTarget therefore allocates
// each accumulated `Expr` into the arena (`alloc_node`) to obtain a handle.
// ===========================================================================

/// A single-member `Relids` (`bms_make_singleton`) built without an `mcx`.
fn bms_make_singleton_relids(x: i32) -> types_pathnodes::Relids {
    debug_assert!(x > 0);
    let bit = x as usize;
    let wordnum = bit / 64;
    let bitnum = bit % 64;
    let mut words = alloc::vec![0u64; wordnum + 1];
    words[wordnum] = 1u64 << bitnum;
    Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words }))
}

/// `IS_SRF_CALL(node)` (tlist.c:32) — a top-level set-returning FuncExpr/OpExpr.
fn is_srf_call(node: &Expr) -> bool {
    match node {
        Expr::FuncExpr(f) => f.funcretset,
        Expr::OpExpr(o) => o.opretset,
        _ => false,
    }
}

/// `split_pathtarget_item` (tlist.c:42) — a subexpression of a PathTarget plus
/// its sortgroupref (0 if none). We carry an owned, copied `Expr`.
///
/// Deliberately *not* `#[derive(Clone)]`: the owned `Expr` deep-copies through
/// the panicking derived `Expr::clone` for `Aggref`/`SubLink`/… payloads, so
/// any copy must go through [`SplitPathtargetItem::clone_in`] (`Expr::clone_in`).
struct SplitPathtargetItem<'mcx> {
    expr: Expr<'mcx>,
    sortgroupref: u32,
}

impl<'mcx> SplitPathtargetItem<'mcx> {
    /// Deep-copy via `Expr::clone_in` (never the panicking derived clone).
    fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SplitPathtargetItem<'b>> {
        Ok(SplitPathtargetItem {
            expr: self.expr.clone_in(mcx)?,
            sortgroupref: self.sortgroupref,
        })
    }
}

/// Deep-copy a slice of [`SplitPathtargetItem`] via `clone_in`.
fn clone_items_in<'mcx>(
    items: &[SplitPathtargetItem<'_>],
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<SplitPathtargetItem<'mcx>>> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.clone_in(mcx)?);
    }
    Ok(out)
}

/// `split_pathtarget_context` (tlist.c:47). The `input_target_exprs` list of
/// bare expressions is resolved to owned `Expr`s once, up front.
struct SplitPathtargetContext<'mcx> {
    /// Arena to deep-copy owned `Expr`s into via `Expr::clone_in`.
    mcx: Mcx<'mcx>,
    is_grouping_target: bool,
    parse_has_group_rte: bool,
    parse_has_grouping_sets: bool,
    group_rtindex: i32,
    /// exprs available from input (resolved owned `Expr`s)
    input_target_exprs: Vec<Expr<'mcx>>,
    /// SRF exprs to evaluate at each level
    level_srfs: Vec<Vec<SplitPathtargetItem<'mcx>>>,
    /// input vars needed at each level
    level_input_vars: Vec<Vec<SplitPathtargetItem<'mcx>>>,
    /// input SRFs needed at each level
    level_input_srfs: Vec<Vec<SplitPathtargetItem<'mcx>>>,
    /// vars needed in current subexpr
    current_input_vars: Vec<SplitPathtargetItem<'mcx>>,
    /// SRFs needed in current subexpr
    current_input_srfs: Vec<SplitPathtargetItem<'mcx>>,
    /// max SRF depth in current subexpr
    current_depth: i32,
    /// current subexpr's sortgroupref, or 0
    current_sgref: u32,
    /// First error raised by a deep `Expr::clone_in` inside the walker (e.g.
    /// allocation failure). `expression_tree_walker`'s closure can only return
    /// `bool`, so an error is stashed here and the walk aborts (returns `true`);
    /// the caller propagates it after the top-level walker call.
    pending_err: Option<PgError>,
}

/// The `root->parse` grouping flags `split_pathtarget_walker` consults at the
/// grouping boundary. The caller (which holds the `PlannerRun` to resolve
/// `root.parse`) supplies them: `(hasGroupRTE, groupingSets != NIL, group_rtindex)`.
#[derive(Clone, Copy)]
pub struct SplitGroupingFlags {
    pub has_group_rte: bool,
    pub has_grouping_sets: bool,
    pub group_rtindex: i32,
}

/// `split_pathtarget_at_srfs(root, target, input_target, &targets, &flags)`
/// (tlist.c:843). Both targets on the same side of the grouping boundary.
pub fn split_pathtarget_at_srfs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    target: &PathTarget,
    input_target: Option<&PathTarget>,
    gflags: SplitGroupingFlags,
) -> PgResult<(Vec<PathTarget>, Vec<bool>)> {
    split_pathtarget_at_srfs_extended(mcx, root, target, input_target, false, gflags)
}

/// `split_pathtarget_at_srfs_grouping(...)` (tlist.c:868). `target` is
/// post-grouping while `input_target` is pre-grouping; ignore the grouping
/// nulling bit when matching.
pub fn split_pathtarget_at_srfs_grouping<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    target: &PathTarget,
    input_target: Option<&PathTarget>,
    gflags: SplitGroupingFlags,
) -> PgResult<(Vec<PathTarget>, Vec<bool>)> {
    split_pathtarget_at_srfs_extended(mcx, root, target, input_target, true, gflags)
}

/// `split_pathtarget_at_srfs_extended(...)` (tlist.c:942). Returns
/// `(targets, targets_contain_srfs)` in lowest-first evaluation order; the last
/// `targets` entry is `target` itself.
fn split_pathtarget_at_srfs_extended<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    target: &PathTarget,
    input_target: Option<&PathTarget>,
    is_grouping_target: bool,
    gflags: SplitGroupingFlags,
) -> PgResult<(Vec<PathTarget>, Vec<bool>)> {
    // Physically identical targets: every expr is available from the input.
    if let Some(it) = input_target {
        if core::ptr::eq(it, target) {
            return Ok((alloc::vec![target.clone()], alloc::vec![false]));
        }
        // Same arena handles in same order is also a physical identity in the
        // arena model (the C `target == input_target` pointer test).
        if it.exprs == target.exprs {
            return Ok((alloc::vec![target.clone()], alloc::vec![false]));
        }
    }

    // Resolve the input target's bare expression handles to owned, deep-copied
    // `Expr`s (`clone_in` so `Aggref`/`SubLink`/… deep-copy correctly rather
    // than hitting the panicking derived `Expr::clone`).
    let mut input_target_exprs: Vec<Expr<'mcx>> = Vec::new();
    if let Some(it) = input_target {
        for &id in it.exprs.iter() {
            input_target_exprs.push(root.node(id).clone_in(mcx)?);
        }
    }

    let mut context = SplitPathtargetContext {
        mcx,
        is_grouping_target,
        parse_has_group_rte: gflags.has_group_rte,
        parse_has_grouping_sets: gflags.has_grouping_sets,
        group_rtindex: gflags.group_rtindex,
        input_target_exprs,
        // Level-zero (SRF-free) lists, no levels after that.
        level_srfs: alloc::vec![Vec::new()],
        level_input_vars: alloc::vec![Vec::new()],
        level_input_srfs: alloc::vec![Vec::new()],
        current_input_vars: Vec::new(),
        current_input_srfs: Vec::new(),
        current_depth: 0,
        current_sgref: 0,
        pending_err: None,
    };

    let mut max_depth: i32 = 0;
    let mut need_extra_projection = false;

    // Scan each expression in the PathTarget looking for SRFs.
    for (lci, &node_id) in target.exprs.iter().enumerate() {
        let node = root.node(node_id).clone_in(mcx)?;

        context.current_sgref = get_sortgroupref(target, lci);
        context.current_depth = 0;
        split_pathtarget_walker(&node, &mut context);
        if let Some(e) = context.pending_err.take() {
            return Err(e);
        }

        // An expression containing no SRFs is of no further interest.
        if context.current_depth == 0 {
            continue;
        }

        if max_depth < context.current_depth {
            max_depth = context.current_depth;
            need_extra_projection = false;
        }

        // If any maximum-depth SRF is not at the top level of its expression,
        // we'll need an extra Result node to compute the top-level scalar.
        if max_depth == context.current_depth && !is_srf_call(&node) {
            need_extra_projection = true;
        }
    }

    // No SRFs needing evaluation: no ProjectSet needed.
    if max_depth == 0 {
        return Ok((alloc::vec![target.clone()], alloc::vec![false]));
    }

    // Add top-level Vars / SRF outputs to the last level, or to an extra
    // SRF-free level if we need an extra projection step.
    if need_extra_projection {
        context.level_srfs.push(Vec::new());
        let civ = core::mem::take(&mut context.current_input_vars);
        let cis = core::mem::take(&mut context.current_input_srfs);
        context.level_input_vars.push(civ);
        context.level_input_srfs.push(cis);
    } else {
        let civ = core::mem::take(&mut context.current_input_vars);
        let cis = core::mem::take(&mut context.current_input_srfs);
        context.level_input_vars[max_depth as usize].extend(civ);
        context.level_input_srfs[max_depth as usize].extend(cis);
    }

    // Construct the output PathTargets. The original target is the last one;
    // construct a new SRF-free target for the input node plus one per
    // intermediate ProjectSet.
    let mut targets: Vec<PathTarget> = Vec::new();
    let mut targets_contain_srfs: Vec<bool> = Vec::new();
    let mut prev_level_exprs: Vec<Expr> = Vec::new();

    let nlevels = context.level_srfs.len();
    for lc1 in 0..nlevels {
        let level_srfs_nonempty = !context.level_srfs[lc1].is_empty();
        let ntarget: PathTarget;

        if lc1 == nlevels - 1 {
            ntarget = target.clone();
        } else {
            let mut nt = create_empty_pathtarget();

            // Evaluate this level's SRFs.
            let level_srfs = clone_items_in(&context.level_srfs[lc1], mcx)?;
            add_sp_items_to_pathtarget(mcx, root, &mut nt, &level_srfs)?;

            // Propagate forward Vars needed by later levels.
            for lc in (lc1 + 1)..context.level_input_vars.len() {
                let input_vars = clone_items_in(&context.level_input_vars[lc], mcx)?;
                add_sp_items_to_pathtarget(mcx, root, &mut nt, &input_vars)?;
            }

            // Propagate forward SRFs computed earlier and needed by later
            // levels, but only those present in the previous level's tlist.
            for lc in (lc1 + 1)..context.level_input_srfs.len() {
                let input_srfs = clone_items_in(&context.level_input_srfs[lc], mcx)?;
                for item in &input_srfs {
                    if expr_list_member(&prev_level_exprs, &item.expr) {
                        add_sp_item_to_pathtarget(mcx, root, &mut nt, item)?;
                    }
                }
            }

            backend_optimizer_path_costsize::sizeest::set_pathtarget_cost_width(root, &mut nt);
            ntarget = nt;
        }

        // Remember this level's output exprs for the next pass (resolve the
        // arena handles to owned Exprs for the list_member test).
        prev_level_exprs = Vec::with_capacity(ntarget.exprs.len());
        for &id in ntarget.exprs.iter() {
            prev_level_exprs.push(root.node(id).clone_in(mcx)?);
        }

        targets.push(ntarget);
        targets_contain_srfs.push(level_srfs_nonempty);
    }

    Ok((targets, targets_contain_srfs))
}

/// `get_pathtarget_sortgroupref(target, i)` — the i-th sortgroupref or 0.
fn get_sortgroupref(target: &PathTarget, i: usize) -> u32 {
    target.sortgrouprefs.get(i).copied().unwrap_or(0)
}

/// `list_member(list, expr)` over owned `Expr`s using structural `equal()`.
fn expr_list_member(list: &[Expr], expr: &Expr) -> bool {
    list.iter().any(|e| equal_expr::call(e, expr))
}

/// `split_pathtarget_walker(node, context)` (tlist.c:1142). Recursively examine
/// `node`, entering SRFs and Vars/Var-like nodes into the context's lists.
fn split_pathtarget_walker(node: &Expr, context: &mut SplitPathtargetContext) -> bool {
    let mcx = context.mcx;

    // Deep-copy `$e` into `mcx` via `Expr::clone_in` (never the panicking derived
    // `Expr::clone`, which aborts on `Aggref`/`SubLink`/… payloads). On failure
    // (e.g. OOM) stash the error and abort the walk by returning `true`.
    macro_rules! clone_expr {
        ($e:expr) => {
            match $e.clone_in(mcx) {
                Ok(v) => v,
                Err(err) => {
                    context.pending_err = Some(err);
                    return true;
                }
            }
        };
    }

    // If crossing the grouping boundary, ignore the grouping nulling bit when
    // checking availability in input_target (aligns with set_upper_references).
    let sanitized: Expr = if context.is_grouping_target
        && context.parse_has_group_rte
        && context.parse_has_grouping_sets
    {
        backend_optimizer_path_equivclass_ext_seams::remove_nulling_relids::call(
            mcx,
            clone_expr!(node),
            bms_make_singleton_relids(context.group_rtindex),
            None,
        )
    } else {
        clone_expr!(node)
    };

    // A subexpression matching one already computed in input_target can be
    // treated like a Var even if it's a SRF. Record it and ignore substructure.
    if expr_list_member(&context.input_target_exprs, &sanitized) {
        let expr = clone_expr!(node);
        context.current_input_vars.push(SplitPathtargetItem {
            expr,
            sortgroupref: context.current_sgref,
        });
        return false;
    }

    // Vars and Var-like constructs come from the input too.
    if matches!(
        node,
        Expr::Var(_)
            | Expr::PlaceHolderVar(_)
            | Expr::Aggref(_)
            | Expr::GroupingFunc(_)
            | Expr::WindowFunc(_)
    ) {
        let expr = clone_expr!(node);
        context.current_input_vars.push(SplitPathtargetItem {
            expr,
            sortgroupref: context.current_sgref,
        });
        return false;
    }

    // A SRF: recursively examine its inputs, determine its level, record it.
    if is_srf_call(node) {
        let item = SplitPathtargetItem {
            expr: clone_expr!(node),
            sortgroupref: context.current_sgref,
        };

        let save_input_vars = core::mem::take(&mut context.current_input_vars);
        let save_input_srfs = core::mem::take(&mut context.current_input_srfs);
        let save_current_depth = context.current_depth;

        context.current_depth = 0;
        context.current_sgref = 0;

        // expression_tree_walker over the SRF's children.
        backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |child| {
            split_pathtarget_walker(child, context)
        });

        // Depth is one more than any SRF below it.
        let srf_depth = (context.current_depth + 1) as usize;

        // If new record depth, initialize another level of output lists.
        while srf_depth >= context.level_srfs.len() {
            context.level_srfs.push(Vec::new());
            context.level_input_vars.push(Vec::new());
            context.level_input_srfs.push(Vec::new());
        }

        // Record this SRF at its level, with its inputs at the same level.
        let item_copy = match item.clone_in(mcx) {
            Ok(v) => v,
            Err(err) => {
                context.pending_err = Some(err);
                return true;
            }
        };
        context.level_srfs[srf_depth].push(item_copy);
        let civ = core::mem::take(&mut context.current_input_vars);
        let cis = core::mem::take(&mut context.current_input_srfs);
        context.level_input_vars[srf_depth].extend(civ);
        context.level_input_srfs[srf_depth].extend(cis);

        // Restore caller-level state, updating for this SRF.
        context.current_input_vars = save_input_vars;
        context.current_input_srfs = save_input_srfs;
        context.current_input_srfs.push(item);
        context.current_depth = core::cmp::max(save_current_depth, srf_depth as i32);

        return false;
    }

    // Scalar (non-set) expression: recurse into its inputs.
    context.current_sgref = 0;
    backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |child| {
        split_pathtarget_walker(child, context)
    })
}

/// `add_sp_item_to_pathtarget(target, item)` (tlist.c:1290). Add `item` to
/// `target` unless an `equal()` entry without a conflicting sortgroupref
/// already exists; a zero-sgref item may merge with a labeled one and vice
/// versa, acquiring the nonzero label. Copies the expr into the arena.
fn add_sp_item_to_pathtarget<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    target: &mut PathTarget,
    item: &SplitPathtargetItem,
) -> PgResult<()> {
    for lci in 0..target.exprs.len() {
        let sgref = get_sortgroupref(target, lci);
        let matches_sgref =
            item.sortgroupref == sgref || item.sortgroupref == 0 || sgref == 0;
        if matches_sgref && equal_expr::call(&item.expr, root.node(target.exprs[lci])) {
            // Found a match. Assign item's sortgroupref if it has one.
            if item.sortgroupref != 0 {
                if target.sortgrouprefs.is_empty() {
                    target.sortgrouprefs = alloc::vec![0u32; target.exprs.len()];
                }
                target.sortgrouprefs[lci] = item.sortgroupref;
            }
            return Ok(());
        }
    }
    // No match: add to PathTarget. Deep-copy the expr (C copyObject) via
    // `Expr::clone_in` so an `Aggref`/`SubLink`/… payload deep-copies correctly
    // rather than hitting the panicking derived `Expr::clone`.
    let id = root.alloc_node(item.expr.clone_in(mcx)?);
    add_column_to_pathtarget(target, id, item.sortgroupref);
    Ok(())
}

/// `add_sp_items_to_pathtarget(target, items)` (tlist.c:1334).
fn add_sp_items_to_pathtarget<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    target: &mut PathTarget,
    items: &[SplitPathtargetItem],
) -> PgResult<()> {
    for item in items {
        add_sp_item_to_pathtarget(mcx, root, target, item)?;
    }
    Ok(())
}
