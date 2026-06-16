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
//! The SRF-leveling family (`split_pathtarget_at_srfs*`) is still not defined
//! here: it switches on `root->parse` (`Query`) targetlist-SRF data that the
//! opaque `PlannerInfo.parse` handle cannot resolve.

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
    exprs: &[Expr],
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
pub fn get_tlist_exprs(tlist: &[TargetEntry<'_>], includeJunk: bool) -> Vec<Expr> {
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
pub fn get_sortgroupclause_expr(
    sgClause: &SortGroupClause,
    targetList: &[TargetEntry<'_>],
) -> PgResult<Option<Expr>> {
    let tle = get_sortgroupclause_tle(sgClause, targetList)?;
    Ok(tle.expr.as_deref().cloned())
}

/// `get_sortgrouplist_exprs(sgClauses, targetList)` (tlist.c). The referenced
/// tlist expressions, in order.
pub fn get_sortgrouplist_exprs(
    sgClauses: &[SortGroupClause],
    targetList: &[TargetEntry<'_>],
) -> PgResult<Vec<Option<Expr>>> {
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
        let expr = root.node(expr_id).clone();
        let mut tle = make_target_entry(mcx, expr, (i as AttrNumber) + 1, None, false)?;
        if !target.sortgrouprefs.is_empty() {
            tle.ressortgroupref = target.sortgrouprefs[i];
        }
        tlist.push(tle);
    }
    Ok(tlist)
}
