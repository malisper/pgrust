#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_late_init)]

//! Safe-Rust port of `src/backend/optimizer/path/clausesel.c` and
//! `src/backend/optimizer/path/tidpath.c` (postgres-18.3):
//!
//!   * **clausesel.c** — clause/clauselist selectivity estimation
//!     ([`clauselist_selectivity`] / [`clause_selectivity`] and their `_ext`
//!     forms), the range-query clause pairing machinery (`addRangeClause` +
//!     `RangeQueryClause`), and `find_single_rel_for_clauses`.
//!   * **tidpath.c** — TID/TID-range scan path construction
//!     ([`create_tidscan_paths`]) and the CTID-qual recognizers
//!     (`IsTidEqualClause` / `IsTidRangeClause` / `IsCurrentOfClause` / ...).
//!
//! # Arena model
//!
//! Everything is shaped over the [`PlannerInfo`](types_pathnodes::PlannerInfo)
//! arena. Clause lists are [`RinfoId`] handles (the C `RestrictInfo *` lists);
//! `root.rinfo(id)` recovers the `RestrictInfo`, whose `.clause` is a
//! [`NodeId`] into the expression arena (`root.node(id)` -> `&Expr`). Rels are
//! addressed by [`RelId`]; `root.rel(id)` recovers the `RelOptInfo`, and
//! [`find_base_rel`] resolves an RT index through `simple_rel_array`.
//!
//! The selectivity control flow ports 1:1; everything crossing a subsystem
//! boundary crosses through a seam (selfuncs.c / plancat.c per-clause
//! estimators, extended statistics, clauses.c analysis helpers, lsyscache.c's
//! `get_oprrest`, equalfuncs.c `equal`), and tidpath.c likewise crosses through
//! pathnode.c (`create_tidscan_path` / `create_tidrangescan_path` / `add_path`),
//! restrictinfo.c, equivclass.c, var.c and clauses.c. Allocating functions take
//! an [`Mcx`](mcx::Mcx) and return [`PgResult`](types_error::PgResult): each
//! `palloc` in C can `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`.

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::vec::Vec;

use types_error::PgResult;

use types_core::primitive::{AttrNumber, Index, Oid};
use types_nodes::primnodes::{Expr, NullTestType, Var, AND_EXPR, NOT_EXPR, OR_EXPR};
use types_pathnodes::{
    EcId, EmId, JoinType, NodeId, RelId, Relids, RestrictInfo, RinfoId, PlannerInfo,
    SpecialJoinInfo, JOIN_INNER, RTE_RELATION,
};
use types_selfuncs::{DEFAULT_INEQ_SEL, DEFAULT_RANGE_INEQ_SEL};

use backend_optimizer_path_small_seams as seam;
use backend_nodes_equalfuncs_seams as eq;
use backend_optimizer_util_pathnode_seams as ps;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as lsc;

/* ==========================================================================
 * Constants mirrored from C headers.
 * ====================================================================== */

/// `F_SCALARLTSEL` (fmgroids.h) — pg_proc OID 103.
const F_SCALARLTSEL: Oid = 103;
/// `F_SCALARGTSEL` (fmgroids.h) — pg_proc OID 104.
const F_SCALARGTSEL: Oid = 104;
/// `F_SCALARLESEL` (fmgroids.h) — pg_proc OID 336.
const F_SCALARLESEL: Oid = 336;
/// `F_SCALARGESEL` (fmgroids.h) — pg_proc OID 337.
const F_SCALARGESEL: Oid = 337;

/// `TIDEqualOperator` (pg_operator.dat) — `=` for `tid` (OID 387).
const TID_EQUAL_OPERATOR: Oid = 387;
/// `TIDLessOperator` — `<` for `tid` (OID 2799).
const TID_LESS_OPERATOR: Oid = 2799;
/// `TIDGreaterOperator` — `>` for `tid` (OID 2800).
const TID_GREATER_OPERATOR: Oid = 2800;
/// `TIDLessEqOperator` — `<=` for `tid` (OID 2801).
const TID_LESS_EQ_OPERATOR: Oid = 2801;
/// `TIDGreaterEqOperator` — `>=` for `tid` (OID 2802).
const TID_GREATER_EQ_OPERATOR: Oid = 2802;

/// `TIDOID` (pg_type.h) — the OID of the `tid` type.
const TIDOID: Oid = 27;
/// `SelfItemPointerAttributeNumber` (sysattr.h) — the CTID system column.
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: AttrNumber = -1;

/// `AMFLAG_HAS_TID_RANGE` (pathnodes.h) — table AM supports TID range scans.
const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;

/* ==========================================================================
 * Small node-shape helpers (nodeFuncs.h / clauses.h inlines).
 * ====================================================================== */

/// `is_opclause(node)` (clauses.h): node is an `OpExpr`/`DistinctExpr`/
/// `NullIfExpr`. (DistinctExpr/NullIfExpr share the `OpExpr` payload.)
#[inline]
fn is_opclause(node: &Expr) -> bool {
    matches!(
        node,
        Expr::OpExpr(_) | Expr::DistinctExpr(_) | Expr::NullIfExpr(_)
    )
}

/// `is_andclause(node)` (clauses.h).
#[inline]
fn is_andclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == AND_EXPR)
}

/// `is_orclause(node)` (clauses.h).
#[inline]
fn is_orclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == OR_EXPR)
}

/// `is_notclause(node)` (clauses.h).
#[inline]
fn is_notclause(node: &Expr) -> bool {
    matches!(node, Expr::BoolExpr(b) if b.boolop == NOT_EXPR)
}

/// `is_funcclause(node)` (clauses.h).
#[inline]
fn is_funcclause(node: &Expr) -> bool {
    matches!(node, Expr::FuncExpr(_))
}

/// `get_notclausearg(notclause)` (clauses.h): the lone arg of a NOT clause.
#[inline]
fn get_notclausearg(notclause: &Expr) -> &Expr {
    match notclause {
        Expr::BoolExpr(b) => &b.args[0],
        _ => unreachable!("get_notclausearg on non-BoolExpr"),
    }
}

/// `get_leftop((Expr *) clause)` (nodeFuncs.h): first arg of a binary OpExpr.
#[inline]
fn get_leftop(args: &[Expr]) -> &Expr {
    &args[0]
}

/// `get_rightop((Expr *) clause)` (nodeFuncs.h): second arg of a binary OpExpr.
#[inline]
fn get_rightop(args: &[Expr]) -> &Expr {
    &args[1]
}

/// `find_base_rel(root, relid)` (relnode.c): the base `RelOptInfo` for an RT
/// index. C asserts `relid > 0` and a non-NULL slot; we mirror with the arena
/// `simple_rel_array` lookup.
fn find_base_rel(root: &PlannerInfo, relid: Index) -> RelId {
    debug_assert!(relid > 0, "find_base_rel: bogus relid");
    let idx = relid as usize;
    let slot = root
        .simple_rel_array
        .get(idx)
        .copied()
        .flatten();
    slot.expect("find_base_rel: no relation entry")
}

/* ==========================================================================
 * RANGE-QUERY CLAUSE PAIRING (clausesel.c `RangeQueryClause`)
 *
 * C threads a hand-rolled singly-linked list of `RangeQueryClause` palloc'd
 * cells. We carry the same data in a `Vec`; the "var" key is the common range
 * variable, cloned out of the arena (an owned `Expr`), matched with the full
 * `equal()` node comparison (a clause's var "might be a function of one or more
 * attributes of the same relation").
 * ====================================================================== */

/// `RangeQueryClause` (clausesel.c): an accumulating range-clause pair.
struct RangeQueryClause {
    /// `Node *var` — the common variable of the clauses.
    var: Expr,
    /// `bool have_lobound` — found a low-bound clause yet?
    have_lobound: bool,
    /// `bool have_hibound` — found a high-bound clause yet?
    have_hibound: bool,
    /// `Selectivity lobound` — selectivity of a `var > something` clause.
    lobound: f64,
    /// `Selectivity hibound` — selectivity of a `var < something` clause.
    hibound: f64,
}

/// `addRangeClause(&rqlist, clause, varonleft, isLTsel, s2)` (clausesel.c):
/// match a new range-query clause against the accumulating pair list.
fn addRangeClause(
    rqlist: &mut Vec<RangeQueryClause>,
    clause_args: &[Expr],
    varonleft: bool,
    isLTsel: bool,
    s2: f64,
) {
    let var: Expr;
    let is_lobound: bool;

    if varonleft {
        var = get_leftop(clause_args).clone();
        is_lobound = !isLTsel; // x < something is high bound
    } else {
        var = get_rightop(clause_args).clone();
        is_lobound = isLTsel; // something < x is low bound
    }

    for rqelem in rqlist.iter_mut() {
        // We use full equal() here because the "var" might be a function of
        // one or more attributes of the same relation...
        if !eq::equal_expr::call(&var, &rqelem.var) {
            continue;
        }
        // Found the right group to put this clause in
        if is_lobound {
            if !rqelem.have_lobound {
                rqelem.have_lobound = true;
                rqelem.lobound = s2;
            } else {
                // We have found two similar clauses, such as
                // x < y AND x <= z.  Keep only the more restrictive one.
                if rqelem.lobound > s2 {
                    rqelem.lobound = s2;
                }
            }
        } else {
            if !rqelem.have_hibound {
                rqelem.have_hibound = true;
                rqelem.hibound = s2;
            } else {
                // We have found two similar clauses, such as
                // x > y AND x >= z.  Keep only the more restrictive one.
                if rqelem.hibound > s2 {
                    rqelem.hibound = s2;
                }
            }
        }
        return;
    }

    // No matching var found, so make a new clause-pair data structure
    let rqelem = if is_lobound {
        RangeQueryClause {
            var,
            have_lobound: true,
            have_hibound: false,
            lobound: s2,
            hibound: 0.0,
        }
    } else {
        RangeQueryClause {
            var,
            have_lobound: false,
            have_hibound: true,
            lobound: 0.0,
            hibound: s2,
        }
    };
    // C prepends to the singly-linked list; order is irrelevant to the final
    // result (the list is scanned once, unordered), so we append.
    rqlist.push(rqelem);
}

/* ==========================================================================
 * find_single_rel_for_clauses (clausesel.c)
 * ====================================================================== */

/// `find_single_rel_for_clauses(root, clauses)` (clausesel.c): if all clauses
/// reference only a single relation, return its [`RelId`]; else `None`.
fn find_single_rel_for_clauses(
    root: &mut PlannerInfo,
    clauses: &[ListEntry],
) -> PgResult<Option<RelId>> {
    match find_single_relid_for_clauses(root, clauses)? {
        Some(lastrelid) if lastrelid != 0 => Ok(Some(find_base_rel(root, lastrelid))),
        _ => Ok(None), // no clauses
    }
}

/// The relid-yielding core of `find_single_rel_for_clauses`, used directly by
/// the recursive bare-AND case (the C recurses returning a `RelOptInfo *`, then
/// reads `rel->relid`; the arena form threads the relid through to avoid the
/// `find_base_rel` round-trip mid-recursion). Returns `None` if not single-rel,
/// `Some(0)` if no rel was referenced, else `Some(relid)`.
fn find_single_relid_for_clauses(
    root: &mut PlannerInfo,
    clauses: &[ListEntry],
) -> PgResult<Option<Index>> {
    let mut lastrelid: Index = 0;

    for entry in clauses {
        // If we have a list of bare clauses rather than RestrictInfos, we could
        // pull out their relids the hard way with pull_varnos(). However, the
        // extended-stats machinery won't do anything with non-RestrictInfo
        // clauses anyway, so just fail if that's what we have.
        //
        // An exception is a bare BoolExpr AND clause: the restrictinfo
        // machinery doesn't build RestrictInfos on top of AND clauses.
        match entry {
            ListEntry::Bare(node) => {
                if is_andclause(node) {
                    let subentries = boolexpr_args_as_entries(node);
                    match find_single_relid_for_clauses(root, &subentries)? {
                        None => return Ok(None),
                        Some(0) => {
                            // rel == NULL in C -> not single-rel.
                            return Ok(None);
                        }
                        Some(relid) => {
                            if lastrelid == 0 {
                                lastrelid = relid;
                            } else if relid != lastrelid {
                                return Ok(None);
                            }
                        }
                    }
                    continue;
                }
                // Not a RestrictInfo (and not a bare AND): fail.
                return Ok(None);
            }
            ListEntry::Rinfo(rid) => {
                let clause_relids = clone_relids(&root.rinfo(*rid).clause_relids);
                if bms::relids_is_empty::call(&clause_relids) {
                    continue; // we can ignore variable-free clauses
                }
                match bms_get_singleton_member(&clause_relids) {
                    None => return Ok(None), // multiple relations in this clause
                    Some(relid) => {
                        if lastrelid == 0 {
                            lastrelid = relid as Index; // first clause referencing a rel
                        } else if relid as Index != lastrelid {
                            return Ok(None); // relation not same as last one
                        }
                    }
                }
            }
        }
    }

    Ok(Some(lastrelid))
}

/* ==========================================================================
 * clauselist_selectivity (clausesel.c)
 * ====================================================================== */

/// `clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo)`
/// (clausesel.c).
pub fn clauselist_selectivity(
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    clauselist_selectivity_ext(root, clauses, var_relid, jointype, sjinfo, true)
}

/// `clauselist_selectivity_ext(root, clauses, varRelid, jointype, sjinfo,
/// use_extended_stats)` (clausesel.c). Public entry over the [`RinfoId`] list
/// (the seam form); delegates to the [`ListEntry`] core.
pub fn clauselist_selectivity_ext(
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    let entries = rinfos_as_entries(clauses);
    clauselist_selectivity_ext_entries(root, &entries, var_relid, jointype, sjinfo, use_extended_stats)
}

/// The `ListEntry`-shaped core of `clauselist_selectivity_ext`, so the AND
/// recursion in `clause_selectivity_ext` can pass an AND's bare-expr `args`
/// (the C `rinfo == NULL` elements) as well as real RestrictInfos.
fn clauselist_selectivity_ext_entries(
    root: &mut PlannerInfo,
    clauses: &[ListEntry],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    let mut s1: f64 = 1.0;
    let mut estimatedclauses: Relids = None;

    // If there's exactly one clause, just go directly to
    // clause_selectivity_ext(). None of what we might do below is relevant.
    if clauses.len() == 1 {
        let clause = clauses[0].clause(root);
        return clause_selectivity_ext(
            root,
            clauses[0].cref(),
            &clause,
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        );
    }

    // Determine if these clauses reference a single relation.  If so, and if it
    // has extended statistics, try to apply those.
    let rel = find_single_rel_for_clauses(root, clauses)?;
    if use_extended_stats {
        if let Some(rel) = rel {
            if root.rel(rel).rtekind == RTE_RELATION && !root.rel(rel).statlist.is_empty() {
                // Extended statistics only consult RestrictInfo clauses; the only
                // way a bare element reaches here is a bare-AND single-rel arm,
                // which statext skips anyway. Pass the real RestrictInfo list.
                if let Some(rinfos) = all_rinfos(clauses) {
                    // Estimate as many clauses as possible using extended stats.
                    let (sel, est) = seam::statext_clauselist_selectivity::call(
                        root,
                        &rinfos,
                        var_relid,
                        jointype,
                        sjinfo,
                        rel,
                        &estimatedclauses,
                        false,
                    )?;
                    s1 = sel;
                    estimatedclauses = est;
                }
            }
        }
    }

    // Apply normal selectivity estimates for remaining clauses. We'll be
    // careful to skip any clauses which were already estimated above.
    let mut rqlist: Vec<RangeQueryClause> = Vec::new();
    let sjinfo_owned: Option<SpecialJoinInfo> = sjinfo.cloned();

    let mut listidx: i32 = -1;
    for entry in clauses {
        listidx += 1;

        // Skip this clause if it's already been estimated by some other
        // statistics above.
        if bms::relids_is_member::call(listidx, &estimatedclauses) {
            continue;
        }

        // Compute the selectivity of this clause in isolation.
        let clause0 = entry.clause(root);
        let s2 = clause_selectivity_ext(
            root,
            entry.cref(),
            &clause0,
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        )?;

        // Check for being passed a RestrictInfo.  If it's a pseudoconstant
        // RestrictInfo, then s2 is either 1.0 or 0.0; just use that rather than
        // looking for range pairs.
        let rinfo: Option<RinfoId> = match entry {
            ListEntry::Rinfo(rid) => Some(*rid),
            ListEntry::Bare(_) => None,
        };
        if let Some(rid) = rinfo {
            if root.rinfo(rid).pseudoconstant {
                s1 *= s2;
                continue;
            }
        }
        let clause = clause0; // (Node *) rinfo->clause (or the bare clause)

        // See if it looks like a restriction clause with a pseudoconstant on one
        // side.  Most of the tests here can be done more efficiently with rinfo.
        let mut handled = false;
        if is_opclause(&clause) {
            let args: Vec<Expr> = opexpr_args(&clause).map(|a| a.to_vec()).unwrap_or_default();
            if args.len() == 2 {
                {
                    let mut varonleft = true;
                    let ok;
                    if let Some(rid) = rinfo {
                        let right_relids = clone_relids(&root.rinfo(rid).right_relids);
                        let left_relids = clone_relids(&root.rinfo(rid).left_relids);
                        let num_base_rels = root.rinfo(rid).num_base_rels;
                        let lsecond = &args[1];
                        let linitial = &args[0];
                        ok = (num_base_rels == 1)
                            && (seam::is_pseudo_constant_clause_relids::call(lsecond, &right_relids)?
                                || {
                                    varonleft = false;
                                    seam::is_pseudo_constant_clause_relids::call(
                                        linitial,
                                        &left_relids,
                                    )?
                                });
                    } else {
                        // Bare clause: use the un-rinfo path (NumRelids + the
                        // pseudoconstant predicates without relid hints).
                        let lsecond = &args[1];
                        let linitial = &args[0];
                        ok = (seam::num_relids::call(root, &clause)? == 1)
                            && (seam::is_pseudo_constant_clause::call(lsecond)?
                                || {
                                    varonleft = false;
                                    seam::is_pseudo_constant_clause::call(linitial)?
                                });
                    }

                    if ok {
                        let opno = opexpr_opno(&clause);
                        // If it's not a "<"/"<="/">"/">=" operator, just merge the
                        // selectivity generically.  But if it's the right oprrest,
                        // add the clause to rqlist for later processing.
                        let oprrest = lsc::get_oprrest::call(opno)?;
                        match oprrest {
                            F_SCALARLTSEL | F_SCALARLESEL => {
                                addRangeClause(&mut rqlist, &args, varonleft, true, s2);
                            }
                            F_SCALARGTSEL | F_SCALARGESEL => {
                                addRangeClause(&mut rqlist, &args, varonleft, false, s2);
                            }
                            _ => {
                                // Just merge the selectivity in generically
                                s1 *= s2;
                            }
                        }
                        handled = true; // drop to loop bottom
                    }
                }
            }
        }

        if handled {
            continue;
        }

        // Not the right form, so treat it generically.
        s1 *= s2;
    }

    // Now scan the rangequery pair list.
    for rqelem in &rqlist {
        if rqelem.have_lobound && rqelem.have_hibound {
            // Successfully matched a pair of range clauses
            let s2: f64;

            // Exact equality to the default value probably means the selectivity
            // function punted.  Not airtight but good enough.
            if rqelem.hibound == DEFAULT_INEQ_SEL || rqelem.lobound == DEFAULT_INEQ_SEL {
                s2 = DEFAULT_RANGE_INEQ_SEL;
            } else {
                let mut s2v = rqelem.hibound + rqelem.lobound - 1.0;

                // Adjust for double-exclusion of NULLs
                s2v += seam::nulltestsel_var::call(
                    root,
                    NullTestType::IS_NULL as i32,
                    &rqelem.var,
                    var_relid,
                    jointype,
                    sjinfo_owned.as_ref(),
                )?;

                // A zero or slightly negative s2 should be converted into a
                // small positive value; we probably are dealing with a very
                // tight range and got a bogus result due to roundoff errors.
                // However, if s2 is very negative, then we probably have default
                // selectivity estimates on one or both sides of the range that
                // we failed to recognize above for some reason.
                if s2v <= 0.0 {
                    if s2v < -0.01 {
                        // No data available --- use a default estimate that is
                        // small, but not real small.
                        s2v = DEFAULT_RANGE_INEQ_SEL;
                    } else {
                        // It's just roundoff error; use a small positive value
                        s2v = 1.0e-10;
                    }
                }
                s2 = s2v;
            }
            // Merge in the selectivity of the pair of clauses
            s1 *= s2;
        } else {
            // Only found one of a pair, merge it in generically
            if rqelem.have_lobound {
                s1 *= rqelem.lobound;
            } else {
                s1 *= rqelem.hibound;
            }
        }
    }

    Ok(s1)
}

/// `clauselist_selectivity_or(root, clauses, varRelid, jointype, sjinfo,
/// use_extended_stats)` (clausesel.c): selectivity of an implicitly-ORed list.
fn clauselist_selectivity_or(
    root: &mut PlannerInfo,
    clauses: &[ListEntry],
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    let mut s1: f64 = 0.0;
    let mut estimatedclauses: Relids = None;

    // Determine if these clauses reference a single relation.  If so, and if it
    // has extended statistics, try to apply those.
    let rel = find_single_rel_for_clauses(root, clauses)?;
    if use_extended_stats {
        if let Some(rel) = rel {
            if root.rel(rel).rtekind == RTE_RELATION && !root.rel(rel).statlist.is_empty() {
                if let Some(rinfos) = all_rinfos(clauses) {
                    let (sel, est) = seam::statext_clauselist_selectivity::call(
                        root,
                        &rinfos,
                        var_relid,
                        jointype,
                        sjinfo,
                        rel,
                        &estimatedclauses,
                        true,
                    )?;
                    s1 = sel;
                    estimatedclauses = est;
                }
            }
        }
    }

    // Estimate the remaining clauses as if they were independent.
    //
    // Selectivities for an OR clause are computed as s1+s2 - s1*s2 to account
    // for the probable overlap of selected tuple sets.
    let mut listidx: i32 = -1;
    for entry in clauses {
        listidx += 1;

        if bms::relids_is_member::call(listidx, &estimatedclauses) {
            continue;
        }

        let clause = entry.clause(root);
        let s2 = clause_selectivity_ext(
            root,
            entry.cref(),
            &clause,
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        )?;

        s1 = s1 + s2 - s1 * s2;
    }

    Ok(s1)
}

/// All entries as `RinfoId`s, or `None` if any entry is a bare expression.
/// Extended-statistics estimation only ever consumes real RestrictInfos.
fn all_rinfos(clauses: &[ListEntry]) -> Option<Vec<RinfoId>> {
    let mut out = Vec::with_capacity(clauses.len());
    for e in clauses {
        match e {
            ListEntry::Rinfo(r) => out.push(*r),
            ListEntry::Bare(_) => return None,
        }
    }
    Some(out)
}

/* ==========================================================================
 * clause_selectivity (clausesel.c)
 *
 * `ClauseRef` carries whether the top-level clause being estimated arrived as a
 * `RestrictInfo` (so caching + `treat_as_join_clause`'s `rinfo` fast paths
 * apply) or as a bare expression (recursion through NOT/AND/OR/RelabelType/
 * CoerceToDomain unwraps to bare exprs, exactly as the C `rinfo = NULL`).
 * ====================================================================== */

/// How the clause currently under estimation arrived.
#[derive(Clone, Copy)]
enum ClauseRef {
    /// A `RestrictInfo` (caching + rinfo fast paths apply).
    Rinfo(RinfoId),
    /// A bare expression node (the C `rinfo == NULL` case).
    Bare,
}

/// An element of an implicitly-AND/OR'ed clause list. The C `List` mixes
/// `RestrictInfo *` and bare `Expr *` elements; the planner-arena form carries
/// real RestrictInfos by [`RinfoId`] and the bare-expression elements (an
/// AND/OR clause's `args`, which in this arena are plain `Expr` nodes rather
/// than nested sub-RestrictInfos) by value.
#[derive(Clone)]
enum ListEntry {
    /// A `RestrictInfo` element.
    Rinfo(RinfoId),
    /// A bare `Expr *` element (the C `rinfo == NULL` element).
    Bare(Expr),
}

impl ListEntry {
    /// `(Node *) lfirst(l)` — the element's clause expression. For a
    /// RestrictInfo this is `rinfo->clause`; for a bare element it is the
    /// element itself.
    fn clause(&self, root: &PlannerInfo) -> Expr {
        match self {
            ListEntry::Rinfo(rid) => root.node(root.rinfo(*rid).clause).clone(),
            ListEntry::Bare(e) => e.clone(),
        }
    }

    /// The matching [`ClauseRef`] for `clause_selectivity_ext`.
    fn cref(&self) -> ClauseRef {
        match self {
            ListEntry::Rinfo(rid) => ClauseRef::Rinfo(*rid),
            ListEntry::Bare(_) => ClauseRef::Bare,
        }
    }
}

/// Wrap a `&[RinfoId]` list (the public-seam form) as `ListEntry`s.
fn rinfos_as_entries(clauses: &[RinfoId]) -> Vec<ListEntry> {
    clauses.iter().map(|&r| ListEntry::Rinfo(r)).collect()
}

/// The bare-expr `args` of an AND/OR `BoolExpr`, as `ListEntry`s (the C
/// `((BoolExpr *) clause)->args`).
fn boolexpr_args_as_entries(clause: &Expr) -> Vec<ListEntry> {
    match clause {
        Expr::BoolExpr(b) => b.args.iter().cloned().map(ListEntry::Bare).collect(),
        _ => Vec::new(),
    }
}

/// `treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo)` (clausesel.c).
fn treat_as_join_clause(
    root: &mut PlannerInfo,
    clause: &Expr,
    rinfo: ClauseRef,
    var_relid: i32,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<bool> {
    if var_relid != 0 {
        // Caller is forcing restriction mode (eg, because we are examining an
        // inner indexscan qual).
        Ok(false)
    } else if sjinfo.is_none() {
        // It must be a restriction clause, since it's being evaluated at a scan
        // node.
        Ok(false)
    } else {
        // Otherwise, it's a join if there's more than one base relation used.
        // We can optimize this calculation if an rinfo was passed.
        match rinfo {
            ClauseRef::Rinfo(rid) => Ok(root.rinfo(rid).num_base_rels > 1),
            ClauseRef::Bare => Ok(seam::num_relids::call(root, clause)? > 1),
        }
    }
}

/// `clause_selectivity(root, clause, varRelid, jointype, sjinfo)` (clausesel.c).
/// The clause is supplied as a [`RinfoId`] (the preferred RestrictInfo form).
pub fn clause_selectivity(
    root: &mut PlannerInfo,
    clause: RinfoId,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
) -> PgResult<f64> {
    let clause_node = root.node(root.rinfo(clause).clause).clone();
    clause_selectivity_ext(
        root,
        ClauseRef::Rinfo(clause),
        &clause_node,
        var_relid,
        jointype,
        sjinfo,
        true,
    )
}

/// `clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo,
/// use_extended_stats)` (clausesel.c).
///
/// `cref` records how the clause arrived (RestrictInfo vs bare expr); `clause`
/// is the expression to estimate. When `cref` is a `RestrictInfo`, this function
/// applies the C unwrap (pseudoconstant gate, cache lookup, OR-clause swap) and
/// then estimates the contained clause.
fn clause_selectivity_ext(
    root: &mut PlannerInfo,
    cref: ClauseRef,
    clause: &Expr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<&SpecialJoinInfo>,
    use_extended_stats: bool,
) -> PgResult<f64> {
    let mut s1: f64 = 0.5; // default for any unhandled clause type
    let mut cacheable = false;

    // Resolve the RestrictInfo unwrap. After this block, `clause` holds the
    // contained clause to estimate and `rinfo` records the rinfo (if any).
    let mut rinfo: Option<RinfoId> = None;
    let working_clause: Expr;

    if let ClauseRef::Rinfo(rid) = cref {
        rinfo = Some(rid);

        // If the clause is marked pseudoconstant, then it will be used as a
        // gating qual and should not affect selectivity estimates; hence return
        // 1.0.  The only exception is that a constant FALSE may be taken as
        // having selectivity 0.0.  Simple enough that we need not cache.
        if root.rinfo(rid).pseudoconstant {
            let inner = root.node(root.rinfo(rid).clause).clone();
            if !matches!(inner, Expr::Const(_)) {
                return Ok(1.0);
            }
        }

        // If possible, cache the result. We can cache if varRelid is zero or the
        // clause contains only vars of that relid.
        if var_relid == 0
            || root.rinfo(rid).num_base_rels == 0
            || (root.rinfo(rid).num_base_rels == 1
                && bms::relids_is_member::call(var_relid, &root.rinfo(rid).clause_relids))
        {
            // Cacheable --- do we already have the result?
            if jointype == JOIN_INNER {
                if root.rinfo(rid).norm_selec >= 0.0 {
                    return Ok(root.rinfo(rid).norm_selec);
                }
            } else {
                if root.rinfo(rid).outer_selec >= 0.0 {
                    return Ok(root.rinfo(rid).outer_selec);
                }
            }
            cacheable = true;
        }

        // Proceed with examination of contained clause.  If the clause is an
        // OR-clause, we want to look at the variant with sub-RestrictInfos, so
        // that per-subclause selectivities can be cached.
        if let Some(orc) = root.rinfo(rid).orclause {
            working_clause = root.node(orc).clone();
        } else {
            working_clause = root.node(root.rinfo(rid).clause).clone();
        }
    } else {
        working_clause = clause.clone();
    }

    let clause = &working_clause;

    if let Expr::Var(var) = clause {
        // We probably shouldn't ever see an uplevel Var here, but if we do,
        // return the default selectivity...
        if var.varlevelsup == 0 && (var_relid == 0 || var_relid == var.varno) {
            // Use the restriction selectivity function for a bool Var
            s1 = seam::boolvarsel::call(root, clause, var_relid)?;
        }
    } else if let Expr::Const(con) = clause {
        // bool constant is pretty easy...
        s1 = if con.constisnull {
            0.0
        } else if con.constvalue.as_bool() {
            1.0
        } else {
            0.0
        };
    } else if let Expr::Param(_) = clause {
        // see if we can replace the Param
        let subst = seam::estimate_expression_value::call(root, clause)?;
        if let Expr::Const(con) = &subst {
            s1 = if con.constisnull {
                0.0
            } else if con.constvalue.as_bool() {
                1.0
            } else {
                0.0
            };
        } else {
            // XXX any way to do better than default?
        }
    } else if is_notclause(clause) {
        // inverse of the selectivity of the underlying clause
        let arg = get_notclausearg(clause).clone();
        s1 = 1.0
            - clause_selectivity_ext(
                root,
                ClauseRef::Bare,
                &arg,
                var_relid,
                jointype,
                sjinfo,
                use_extended_stats,
            )?;
    } else if is_andclause(clause) {
        // share code with clauselist_selectivity()
        let args = boolexpr_args_as_entries(clause);
        s1 = clauselist_selectivity_ext_entries(
            root,
            &args,
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        )?;
    } else if is_orclause(clause) {
        // Almost the same thing as clauselist_selectivity, but with the clauses
        // connected by OR.
        let args = boolexpr_args_as_entries(clause);
        s1 = clauselist_selectivity_or(
            root,
            &args,
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        )?;
    } else if is_opclause(clause) {
        // is_opclause(clause) || IsA(clause, DistinctExpr)
        let opno = opexpr_opno(clause);
        let args = opexpr_args(clause).map(|a| a.to_vec()).unwrap_or_default();
        let inputcollid = opexpr_inputcollid(clause);

        if treat_as_join_clause(root, clause, cref, var_relid, sjinfo)? {
            // Estimate selectivity for a join clause.
            s1 = seam::join_selectivity::call(
                root, opno, &args, inputcollid, jointype, sjinfo,
            )?;
        } else {
            // Estimate selectivity for a restriction clause.
            s1 = seam::restriction_selectivity::call(
                root, opno, &args, inputcollid, var_relid,
            )?;
        }

        // DistinctExpr has the same representation as OpExpr, but the contained
        // operator is "=" not "<>", so we must negate the result.
        if matches!(clause, Expr::DistinctExpr(_)) {
            s1 = 1.0 - s1;
        }
    } else if is_funcclause(clause) {
        if let Expr::FuncExpr(funcclause) = clause {
            // Try to get an estimate from the support function, if any
            let is_join = treat_as_join_clause(root, clause, cref, var_relid, sjinfo)?;
            s1 = seam::function_selectivity::call(
                root,
                funcclause.funcid,
                &funcclause.args,
                funcclause.inputcollid,
                is_join,
                var_relid,
                jointype,
                sjinfo,
            )?;
        }
    } else if let Expr::ScalarArrayOpExpr(_) = clause {
        // Use node specific selectivity calculation function
        let is_join = treat_as_join_clause(root, clause, cref, var_relid, sjinfo)?;
        s1 = seam::scalararraysel::call(root, clause, is_join, var_relid, jointype, sjinfo)?;
    } else if let Expr::RowCompareExpr(_) = clause {
        // Use node specific selectivity calculation function
        s1 = seam::rowcomparesel::call(root, clause, var_relid, jointype, sjinfo)?;
    } else if let Expr::NullTest(nulltest) = clause {
        // Use node specific selectivity calculation function. C reads
        // `((NullTest *) clause)->arg` directly (never NULL for a valid node).
        if let Some(arg) = nulltest.arg.as_deref().cloned() {
            s1 = seam::nulltestsel::call(
                root,
                nulltest.nulltesttype as i32,
                &arg,
                var_relid,
                jointype,
                sjinfo,
            )?;
        }
    } else if let Expr::BooleanTest(booltest) = clause {
        // Use node specific selectivity calculation function
        if let Some(arg) = booltest.arg.as_deref().cloned() {
            s1 = seam::booltestsel::call(
                root,
                booltest.booltesttype as i32,
                &arg,
                var_relid,
                jointype,
                sjinfo,
            )?;
        }
    } else if let Expr::CurrentOfExpr(cexpr) = clause {
        // CURRENT OF selects at most one row of its table
        let crel = find_base_rel(root, cexpr.cvarno);
        let tuples = root.rel(crel).tuples;
        if tuples > 0.0 {
            s1 = 1.0 / tuples;
        }
    } else if let Expr::RelabelType(rt) = clause {
        // Not sure this case is needed, but it can't hurt
        if let Some(arg) = rt.arg.as_deref().cloned() {
            s1 = clause_selectivity_ext(
                root,
                ClauseRef::Bare,
                &arg,
                var_relid,
                jointype,
                sjinfo,
                use_extended_stats,
            )?;
        }
    } else if let Expr::CoerceToDomain(cd) = clause {
        // Not sure this case is needed, but it can't hurt
        if let Some(arg) = cd.arg.as_deref().cloned() {
            s1 = clause_selectivity_ext(
                root,
                ClauseRef::Bare,
                &arg,
                var_relid,
                jointype,
                sjinfo,
                use_extended_stats,
            )?;
        }
    } else {
        // For anything else, see if we can consider it as a boolean variable.
        // This only works if it's an immutable expression in Vars of a single
        // relation; but boolvarsel() checks that internally and returns a
        // suitable default if not.
        s1 = seam::boolvarsel::call(root, clause, var_relid)?;
    }

    // Cache the result if possible
    if cacheable {
        if let Some(rid) = rinfo {
            if jointype == JOIN_INNER {
                root.rinfo_mut(rid).norm_selec = s1;
            } else {
                root.rinfo_mut(rid).outer_selec = s1;
            }
        }
    }

    Ok(s1)
}

/* ==========================================================================
 * Expr-shape accessors for OpExpr / DistinctExpr / NullIfExpr payloads.
 * ====================================================================== */

fn opexpr_args(clause: &Expr) -> Option<&[Expr]> {
    match clause {
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => Some(&o.args),
        _ => None,
    }
}

fn opexpr_opno(clause: &Expr) -> Oid {
    match clause {
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => o.opno,
        _ => 0,
    }
}

fn opexpr_inputcollid(clause: &Expr) -> Oid {
    match clause {
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => o.inputcollid,
        _ => 0,
    }
}

/// A minimal `RestrictInfo` wrapping a bare clause node: caches disabled
/// (`norm_selec`/`outer_selec == -1`) and non-cacheable
/// (`num_base_rels == 0` but `clause_relids` empty so caching is skipped only
/// via the selec-cache miss; the C `rinfo == NULL` path is reproduced because
/// these wrappers are never consulted for the rinfo fast paths beyond
/// num_base_rels, which is 0 here). This faithfully reproduces estimating a
/// bare expression argument of an AND/OR.
fn make_bare_restrictinfo(clause: NodeId) -> RestrictInfo {
    use types_pathnodes::{QualCost, VOLATILITY_UNKNOWN};
    RestrictInfo {
        clause,
        is_pushed_down: false,
        can_join: false,
        pseudoconstant: false,
        has_clone: false,
        is_clone: false,
        leakproof: false,
        has_volatile: VOLATILITY_UNKNOWN,
        security_level: 0,
        num_base_rels: 0,
        clause_relids: None,
        required_relids: None,
        incompatible_relids: None,
        outer_relids: None,
        left_relids: None,
        right_relids: None,
        orclause: None,
        rinfo_serial: 0,
        parent_ec: None,
        eval_cost: QualCost {
            startup: -1.0,
            per_tuple: -1.0,
        },
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: Vec::new(),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: Vec::new(),
        outer_is_left: false,
        hashjoinoperator: 0,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: 0,
        right_hasheqoperator: 0,
    }
}

/* ==========================================================================
 * Relids / Bitmapset helpers over `types_pathnodes::Relids`.
 * ====================================================================== */

/// `bms_copy(a)` — fresh owned copy of a `Relids`.
fn clone_relids(a: &Relids) -> Relids {
    bms::relids_copy::call(a)
}

/// `bms_get_singleton_member(a, &member)` (bitmapset.c): if `a` has exactly one
/// member, return it; else `None`.
///
/// `Relids` carries the canonical `bitmapword[]` layout (word `i` covers bits
/// `64*i .. 64*i+63`); the bms set-algebra owner is unported, but counting /
/// extracting the lone bit is a pure read of the public word storage, so we do
/// it in place rather than minting an uninstalled seam.
fn bms_get_singleton_member(a: &Relids) -> Option<i32> {
    let bms = a.as_ref()?;
    let mut result: Option<i32> = None;
    for (wordnum, &w) in bms.words.iter().enumerate() {
        let mut w = w;
        while w != 0 {
            // lowest set bit
            let bit = w.trailing_zeros() as i32;
            let member = wordnum as i32 * BITS_PER_BITMAPWORD + bit;
            if result.is_some() {
                return None; // more than one member
            }
            result = Some(member);
            w &= w - 1; // clear lowest set bit
        }
    }
    result
}

/// `BITS_PER_BITMAPWORD` (bitmapset.h) — `bitmapword` is `uint64` here.
const BITS_PER_BITMAPWORD: i32 = 64;

/* ==========================================================================
 * tidpath.c
 * ====================================================================== */

/// `IsCTIDVar(var, rel)` (tidpath.c): does this `Var` represent the CTID column
/// of `rel`?
fn IsCTIDVar(var: &Var, rel_relid: Index) -> bool {
    // The vartype check is strictly paranoia
    var.varattno == SELF_ITEM_POINTER_ATTRIBUTE_NUMBER
        && var.vartype == TIDOID
        && var.varno == rel_relid as i32
        && var.varnullingrels.words.is_empty()
        && var.varlevelsup == 0
}

/// `IsBinaryTidClause(rinfo, rel)` (tidpath.c): is `rinfo` of the form
/// `CTID OP pseudoconstant` (or the reverse), with the CTID Var belonging to
/// `rel` and nothing on the other side referencing `rel`?
fn IsBinaryTidClause(root: &mut PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    let rel_relid = root.rel(rel).relid;
    let clause = root.node(root.rinfo(rinfo).clause).clone();

    // Must be an OpExpr
    if !is_opclause(&clause) {
        return false;
    }
    let args = match opexpr_args(&clause) {
        Some(a) => a.to_vec(),
        None => return false,
    };
    // OpExpr must have two arguments
    if args.len() != 2 {
        return false;
    }
    let arg1 = &args[0];
    let arg2 = &args[1];

    // Look for CTID as either argument
    let other: &Expr;
    let other_relids: Relids;
    if matches!(arg1, Expr::Var(v) if IsCTIDVar(v, rel_relid)) {
        other = arg2;
        other_relids = clone_relids(&root.rinfo(rinfo).right_relids);
    } else if matches!(arg2, Expr::Var(v) if IsCTIDVar(v, rel_relid)) {
        other = arg1;
        other_relids = clone_relids(&root.rinfo(rinfo).left_relids);
    } else {
        return false;
    }

    // The other argument must be a pseudoconstant
    if bms::relids_is_member::call(rel_relid as i32, &other_relids)
        || seam::contain_volatile_functions_expr::call(other)
    {
        return false;
    }

    true // success
}

/// `IsTidEqualClause(rinfo, rel)` (tidpath.c): `CTID = pseudoconstant`.
fn IsTidEqualClause(root: &mut PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    if !IsBinaryTidClause(root, rinfo, rel) {
        return false;
    }
    let clause = root.node(root.rinfo(rinfo).clause).clone();
    opexpr_opno(&clause) == TID_EQUAL_OPERATOR
}

/// `IsTidRangeClause(rinfo, rel)` (tidpath.c): `CTID <,<=,>,>= pseudoconstant`.
fn IsTidRangeClause(root: &mut PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    if !IsBinaryTidClause(root, rinfo, rel) {
        return false;
    }
    let clause = root.node(root.rinfo(rinfo).clause).clone();
    let opno = opexpr_opno(&clause);
    opno == TID_LESS_OPERATOR
        || opno == TID_LESS_EQ_OPERATOR
        || opno == TID_GREATER_OPERATOR
        || opno == TID_GREATER_EQ_OPERATOR
}

/// `IsTidEqualAnyClause(root, rinfo, rel)` (tidpath.c):
/// `CTID = ANY (pseudoconstant_array)`.
fn IsTidEqualAnyClause(root: &mut PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    let rel_relid = root.rel(rel).relid;
    let clause = root.node(root.rinfo(rinfo).clause).clone();

    // Must be a ScalarArrayOpExpr
    let node = match &clause {
        Expr::ScalarArrayOpExpr(n) => n,
        _ => return false,
    };

    // Operator must be tideq
    if node.opno != TID_EQUAL_OPERATOR {
        return false;
    }
    if !node.useOr {
        return false;
    }
    debug_assert_eq!(node.args.len(), 2);
    let arg1 = &node.args[0];
    let arg2 = &node.args[1];

    // CTID must be first argument
    if matches!(arg1, Expr::Var(v) if IsCTIDVar(v, rel_relid)) {
        // The other argument must be a pseudoconstant
        let varnos = seam::pull_varnos_expr::call(root, arg2);
        if bms::relids_is_member::call(rel_relid as i32, &varnos)
            || seam::contain_volatile_functions_expr::call(arg2)
        {
            return false;
        }
        return true; // success
    }

    false
}

/// `IsCurrentOfClause(rinfo, rel)` (tidpath.c): a `CurrentOfExpr` referencing
/// `rel`.
fn IsCurrentOfClause(root: &PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    let rel_relid = root.rel(rel).relid;
    let clause = root.node(root.rinfo(rinfo).clause);
    match clause {
        Expr::CurrentOfExpr(node) => node.cvarno == rel_relid,
        _ => false,
    }
}

/// `RestrictInfoIsTidQual(root, rinfo, rel)` (tidpath.c): usable as a CTID qual
/// (base cases only; AND/OR handled by the caller)?
fn RestrictInfoIsTidQual(root: &mut PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool {
    // We may ignore pseudoconstant clauses (they can't contain Vars, so could
    // not match anyway).
    if root.rinfo(rinfo).pseudoconstant {
        return false;
    }

    // If clause must wait till after some lower-security-level restriction
    // clause, reject it.
    if !seam::restriction_is_securely_promotable::call(root, rinfo, rel) {
        return false;
    }

    // Check all base cases.
    IsTidEqualClause(root, rinfo, rel)
        || IsTidEqualAnyClause(root, rinfo, rel)
        || IsCurrentOfClause(root, rinfo, rel)
}

/// `TidQualFromRestrictInfoList(root, rlist, rel, &isCurrentOf)` (tidpath.c):
/// extract CTID conditions (implicit OR across the result) from an implicit-AND
/// list. Returns `(quals, isCurrentOf)`.
fn TidQualFromRestrictInfoList(
    root: &mut PlannerInfo,
    rlist: &[RinfoId],
    rel: RelId,
) -> PgResult<(Vec<RinfoId>, bool)> {
    let mut tidclause: Option<RinfoId> = None; // best simple CTID qual so far
    let mut orlist: Vec<RinfoId> = Vec::new(); // best OR'ed CTID qual so far
    let mut orlist_set = false;

    for &rinfo in rlist {
        if seam::restriction_is_or_clause::call(root, rinfo) {
            let mut rlst: Vec<RinfoId> = Vec::new();
            let mut rlst_broke = false;

            // We must be able to extract a CTID condition from every sub-clause
            // of an OR, or we can't use it.
            let orclause = root
                .rinfo(rinfo)
                .orclause
                .expect("restriction_is_or_clause but orclause is None");
            let orargs: Vec<Expr> = match root.node(orclause) {
                Expr::BoolExpr(b) => b.args.clone(),
                _ => Vec::new(),
            };

            for orarg in &orargs {
                let sublist: Vec<RinfoId>;

                // OR arguments should be ANDs or sub-RestrictInfos
                if is_andclause(orarg) {
                    let andargs = match orarg {
                        Expr::BoolExpr(b) => b.args.clone(),
                        _ => Vec::new(),
                    };
                    // Recurse in case there are sub-ORs. The C passes the AND's
                    // arg List (RestrictInfos); we materialise each arg as a
                    // transient rinfo to address it by handle.
                    let andrinfos = exprs_as_rinfos(root, &andargs);
                    let (sub, sublist_is_current_of) =
                        TidQualFromRestrictInfoList(root, &andrinfos, rel)?;
                    if sublist_is_current_of {
                        return Err(elog_error("IS CURRENT OF within OR clause"));
                    }
                    sublist = sub;
                } else {
                    // castNode(RestrictInfo, orarg): the OR arg is a
                    // RestrictInfo. In the arena it is a bare expr; wrap it.
                    let ri = expr_as_rinfo(root, orarg.clone());
                    debug_assert!(!seam::restriction_is_or_clause::call(root, ri));
                    if RestrictInfoIsTidQual(root, ri, rel) {
                        sublist = alloc::vec![ri];
                    } else {
                        sublist = Vec::new();
                    }
                }

                // If nothing found in this arm, we can't do anything with this
                // OR clause.
                if sublist.is_empty() {
                    rlst.clear(); // forget anything we had
                    rlst_broke = true;
                    break; // out of loop over OR args
                }

                // OK, continue constructing implicitly-OR'ed result list.
                rlst.extend(sublist);
            }

            if !rlst_broke && !rlst.is_empty() {
                // Accept the OR'ed list if it's the first one, or if it's
                // shorter than the previous one.
                if !orlist_set || rlst.len() < orlist.len() {
                    orlist = rlst;
                    orlist_set = true;
                }
            }
        } else {
            // Not an OR clause, so handle base cases
            if RestrictInfoIsTidQual(root, rinfo, rel) {
                // We can stop immediately if it's a CurrentOfExpr
                if IsCurrentOfClause(root, rinfo, rel) {
                    return Ok((alloc::vec![rinfo], true));
                }

                // Otherwise, remember the first non-OR CTID qual.
                if tidclause.is_none() {
                    tidclause = Some(rinfo);
                }
            }
        }
    }

    // Prefer any singleton CTID qual to an OR'ed list.
    if let Some(tc) = tidclause {
        return Ok((alloc::vec![tc], false));
    }
    Ok((orlist, false))
}

/// `TidRangeQualFromRestrictInfoList(rlist, rel)` (tidpath.c): extract CTID
/// range conditions (implicit AND across the result) from an implicit-AND list.
fn TidRangeQualFromRestrictInfoList(
    root: &mut PlannerInfo,
    rlist: &[RinfoId],
    rel: RelId,
) -> Vec<RinfoId> {
    let mut rlst: Vec<RinfoId> = Vec::new();

    if (root.rel(rel).amflags & AMFLAG_HAS_TID_RANGE) == 0 {
        return rlst;
    }

    for &rinfo in rlist {
        if IsTidRangeClause(root, rinfo, rel) {
            rlst.push(rinfo);
        }
    }

    rlst
}

/// `BuildParameterizedTidPaths(root, rel, clauses)` (tidpath.c): for each join
/// clause that is a suitable TidEqual clause, create a parameterized TidPath.
fn BuildParameterizedTidPaths(
    root: &mut PlannerInfo,
    rel: RelId,
    clauses: &[RinfoId],
) -> PgResult<()> {
    for &rinfo in clauses {
        // Validate whether each clause is actually usable. We currently consider
        // only TidEqual join clauses. This must match RestrictInfoIsTidQual
        // (minus the CurrentOf/SAOP cases).
        if root.rinfo(rinfo).pseudoconstant
            || !seam::restriction_is_securely_promotable::call(root, rinfo, rel)
            || !IsTidEqualClause(root, rinfo, rel)
        {
            continue;
        }

        // Check if clause can be moved to this rel.
        if !seam::join_clause_is_movable_to::call(root, rinfo, rel) {
            continue;
        }

        // OK, make list of clauses for this path. The path stores bare expr
        // handles (TidPath.tidquals), so unwrap the rinfo to its clause node.
        let tidquals: Vec<NodeId> = alloc::vec![root.rinfo(rinfo).clause];

        // Compute required outer rels for this path
        let required_relids = clone_relids(&root.rinfo(rinfo).required_relids);
        let lateral_relids = clone_relids(&root.rel(rel).lateral_relids);
        let mut required_outer = ps::relids_union::call(&required_relids, &lateral_relids);
        required_outer = relids_del_member(required_outer, root.rel(rel).relid as i32);

        let path = ps::create_tidscan_path::call(root, rel, tidquals, &required_outer)?;
        ps::add_path::call(root, rel, path)?;
    }
    Ok(())
}

/// `ec_member_matches_ctid(root, rel, ec, em, arg)` (tidpath.c): EC-member
/// callback — true iff the member is our rel's CTID Var.
fn ec_member_matches_ctid(root: &PlannerInfo, rel: RelId, _ec: EcId, em: EmId) -> bool {
    let rel_relid = root.rel(rel).relid;
    let em_expr = root.node(root.em(em).em_expr);
    matches!(em_expr, Expr::Var(v) if IsCTIDVar(v, rel_relid))
}

/// `create_tidscan_paths(root, rel)` (tidpath.c): create direct-TID-scan paths
/// for `rel`, adding them to its pathlist. Returns `true` iff a CurrentOf path
/// was added (the caller must then add no others).
pub fn create_tidscan_paths(
    root: &mut PlannerInfo,
    rel: RelId,
    enable_tidscan: bool,
) -> PgResult<bool> {
    // If any suitable quals exist in the rel's baserestrict list, generate a
    // plain (unparameterized) TidPath with them.
    //
    // We skip this when enable_tidscan = false, except when the qual is
    // CurrentOfExpr. In that case, a TID scan is the only correct path.
    let baserestrictinfo: Vec<RinfoId> = root.rel(rel).baserestrictinfo.clone();
    let (tidquals_rinfos, isCurrentOf) =
        TidQualFromRestrictInfoList(root, &baserestrictinfo, rel)?;

    if !tidquals_rinfos.is_empty() && (enable_tidscan || isCurrentOf) {
        // This path uses no join clauses, but it could still have required
        // parameterization due to LATERAL refs in its tlist.
        let required_outer = clone_relids(&root.rel(rel).lateral_relids);

        // TidPath.tidquals stores bare expr handles; unwrap each rinfo's clause.
        let tidquals: Vec<NodeId> = tidquals_rinfos
            .iter()
            .map(|&rid| root.rinfo(rid).clause)
            .collect();

        let path = ps::create_tidscan_path::call(root, rel, tidquals, &required_outer)?;
        ps::add_path::call(root, rel, path)?;

        // When the qual is CurrentOfExpr, the path we just added is the only one
        // the executor can handle, so return before adding any others. Returning
        // true lets the caller know not to add any others, either.
        if isCurrentOf {
            return Ok(true);
        }
    }

    // Skip the rest if TID scans are disabled.
    if !enable_tidscan {
        return Ok(false);
    }

    // If there are range quals in the baserestrict list, generate a
    // TidRangePath.
    let tidrangequals_rinfos = TidRangeQualFromRestrictInfoList(root, &baserestrictinfo, rel);

    if !tidrangequals_rinfos.is_empty() {
        let required_outer = clone_relids(&root.rel(rel).lateral_relids);
        let tidrangequals: Vec<NodeId> = tidrangequals_rinfos
            .iter()
            .map(|&rid| root.rinfo(rid).clause)
            .collect();
        let path =
            ps::create_tidrangescan_path::call(root, rel, tidrangequals, &required_outer)?;
        ps::add_path::call(root, rel, path)?;
    }

    // Try to generate parameterized TidPaths using equality clauses extracted
    // from EquivalenceClasses.
    if root.rel(rel).has_eclass_joins {
        // Generate clauses, skipping any that join to lateral_referencers.
        let lateral_referencers = clone_relids(&root.rel(rel).lateral_referencers);
        let clauses = seam::generate_implied_equalities_for_column::call(
            root,
            rel,
            ec_member_matches_ctid,
            &lateral_referencers,
        )?;

        // Generate a path for each usable join clause
        BuildParameterizedTidPaths(root, rel, &clauses)?;
    }

    // Also consider parameterized TidPaths using "loose" join quals.
    let joininfo: Vec<RinfoId> = root.rel(rel).joininfo.clone();
    BuildParameterizedTidPaths(root, rel, &joininfo)?;

    Ok(false)
}

/* ==========================================================================
 * Arena marshalling helpers for tidpath OR/AND recursion.
 * ====================================================================== */

/// Materialise a slice of bare expression args as transient `RestrictInfo`
/// handles (the C OR/AND args are RestrictInfos / sub-clauses; the arena form
/// addresses them by [`RinfoId`]).
fn exprs_as_rinfos(root: &mut PlannerInfo, args: &[Expr]) -> Vec<RinfoId> {
    args.iter().map(|a| expr_as_rinfo(root, a.clone())).collect()
}

/// Materialise one bare expression as a transient `RestrictInfo` handle.
fn expr_as_rinfo(root: &mut PlannerInfo, arg: Expr) -> RinfoId {
    let node_id = root.alloc_node(arg);
    let rinfo = make_bare_restrictinfo(node_id);
    root.alloc_rinfo(rinfo)
}

/// `bms_del_member(a, x)` over `types_pathnodes::Relids`: remove a single
/// member, via the `relids_del_members` set-difference seam with a locally-built
/// singleton (`Bitmapset` is a public word-storage struct; the singleton has
/// the canonical layout, see [`bms_get_singleton_member`]).
fn relids_del_member(a: Relids, x: i32) -> Relids {
    let single = relids_make_singleton(x);
    ps::relids_del_members::call(a, &single)
}

/// `bms_make_singleton(x)` (bitmapset.c): a fresh `Relids` whose only member is
/// `x`, in the canonical `bitmapword[]` layout.
fn relids_make_singleton(x: i32) -> Relids {
    debug_assert!(x >= 0, "bms_make_singleton: negative member");
    let wordnum = (x / BITS_PER_BITMAPWORD) as usize;
    let bitnum = (x % BITS_PER_BITMAPWORD) as u32;
    let mut words = alloc::vec![0u64; wordnum + 1];
    words[wordnum] = 1u64 << bitnum;
    Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words }))
}

/// `ereport(ERROR, ...)` for the one elog site in tidpath.c.
fn elog_error(msg: &str) -> types_error::PgError {
    types_error::PgError::error(msg)
}

/* ==========================================================================
 * Seam installation.
 * ====================================================================== */

/// Install this unit's inward seam ([`clauselist_selectivity`]). Called once at
/// single-threaded startup from `seams-init`.
pub fn init_seams() {
    seam::clauselist_selectivity::set(clauselist_selectivity);
}

#[cfg(test)]
mod tests;
