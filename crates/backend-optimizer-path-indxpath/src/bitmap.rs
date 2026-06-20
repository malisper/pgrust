//! Bitmap-path machinery (indxpath.c): OR-path generation, OR-arg grouping,
//! and the bitmap-AND chooser + its clause-usage classification.

use alloc::vec;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{Cost, Oid};
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PathId, PathNode, PlannerInfo, RelId, Relids, RinfoId};

use backend_nodes_core::makefuncs::make_orclause;
use backend_nodes_equalfuncs_seams::equal_expr;
use backend_optimizer_path_costsize_seams::cost_bitmap_tree_node;
use backend_optimizer_util_clauses::contain_volatile_functions;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_predtest_seams::predicate_implied_by;
use backend_optimizer_util_restrictinfo_seams as restrictinfo;
use backend_utils_cache_lsyscache_seams as lsyscache;

use crate::cost::{bitmap_and_cost_est, bitmap_scan_cost_est, path_usage_comparator};
use crate::matchers::{match_clauses_to_index, IndexClauseSet};
use crate::operand::match_index_to_operand;
use crate::util::{
    is_andclause, relids_add_member, relids_add_members, relids_copy, relids_equal,
    relids_is_member, relids_overlap, restriction_is_or_clause, INVALID_OID,
};

/// Deep-copy a slice of `Expr` into `mcx` via `Expr::clone_in` (C copyObject).
/// The derived `Expr::clone` panics on an owned-subtree child
/// (`Aggref`/`SubLink`/`SubPlan`).
fn clone_exprs_in(exprs: &[Expr], mcx: Mcx<'_>) -> Result<Vec<Expr>, types_error::PgError> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        out.push(e.clone_in(mcx)?);
    }
    Ok(out)
}

/* ==========================================================================
 * build_paths_for_OR.
 * ======================================================================== */

/// `build_paths_for_OR(root, rel, clauses, other_clauses)` (indxpath.c:1093) —
/// construct all matching IndexPaths for the relation from one arm of an OR
/// clause. Returns the candidate `IndexPath` handles.
pub fn build_paths_for_OR<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> Result<Vec<PathId>, types_error::PgError> {
    let mut result: Vec<PathId> = Vec::new();
    let mut all_clauses: Option<Vec<RinfoId>> = None; // computed lazily

    let indexlist_len = root.rel(rel).indexlist.len();
    for idx in 0..indexlist_len {
        let index = root.rel(rel).indexlist[idx].clone();

        // Ignore index if it doesn't support bitmap scans.
        if !index.amhasgetbitmap {
            continue;
        }

        // Ignore partial indexes that do not match the query.
        let mut useful_predicate = false;
        if !index.indpred.is_empty() {
            if index.predOK {
                // Usable, but don't set useful_predicate.
            } else {
                // Form all_clauses if not done already.
                if all_clauses.is_none() {
                    let mut a: Vec<RinfoId> = clauses.to_vec();
                    a.extend_from_slice(other_clauses);
                    all_clauses = Some(a);
                }
                let all = all_clauses.as_ref().unwrap();
                let indpred = index.indpred.clone();
                let all_nodes: Vec<NodeId> =
                    all.iter().map(|&ri| root.rinfo(ri).clause).collect();
                if !predicate_implied_by::call(root, &indpred, &all_nodes, false) {
                    continue; // can't use it at all
                }
                let other_nodes: Vec<NodeId> =
                    other_clauses.iter().map(|&ri| root.rinfo(ri).clause).collect();
                if !predicate_implied_by::call(root, &indpred, &other_nodes, false) {
                    useful_predicate = true;
                }
            }
        }

        // Identify the restriction clauses that can match the index.
        let mut clauseset = IndexClauseSet::new(index.nkeycolumns as usize);
        match_clauses_to_index(mcx, root, clauses, &index, &mut clauseset)?;

        // If no matches so far, and the index predicate isn't useful, skip.
        if !clauseset.nonempty && !useful_predicate {
            continue;
        }

        // Add "other" restriction clauses to the clauseset.
        match_clauses_to_index(mcx, root, other_clauses, &index, &mut clauseset)?;

        // Construct paths if possible.
        let indexpaths = crate::drivers::build_index_paths(
            mcx,
            root,
            run,
            rel,
            &index,
            &clauseset,
            useful_predicate,
            ScanTypeControl::BitmapScan,
            None,
        )?;
        result.extend(indexpaths);
    }

    Ok(result)
}

/* ==========================================================================
 * OR-arg grouping (group_similar_or_args + comparators).
 * ======================================================================== */

/// `ScanTypeControl` (indxpath.c) — which scan types `build_index_paths` should
/// consider.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScanTypeControl {
    /// `ST_INDEXSCAN` — must support amgettuple.
    IndexScan,
    /// `ST_BITMAPSCAN` — must support amgetbitmap.
    BitmapScan,
    /// `ST_ANYSCAN` — either is okay.
    AnyScan,
}

/// `OrArgIndexMatch` (indxpath.c) — info about an OR-clause argument and its
/// matching index key, used to group similar OR args.
#[derive(Clone, Copy, Debug)]
struct OrArgIndexMatch {
    /// index of the matching index, or -1 if none.
    indexnum: i32,
    /// index of the matching column, or -1 if none.
    colnum: i32,
    /// OID of the OpClause operator, or InvalidOid.
    opno: Oid,
    /// OID of the OpClause input collation.
    inputcollid: Oid,
    /// index of the clause in the argument list.
    argindex: i32,
    /// argindex of the first clause in the group of similar clauses.
    groupindex: i32,
}

/// `or_arg_index_match_cmp(a, b)` (indxpath.c:1201) — total order placing
/// similar OR-clause arguments together.
fn or_arg_index_match_cmp(a: &OrArgIndexMatch, b: &OrArgIndexMatch) -> core::cmp::Ordering {
    a.indexnum
        .cmp(&b.indexnum)
        .then(a.colnum.cmp(&b.colnum))
        .then(a.opno.cmp(&b.opno))
        .then(a.inputcollid.cmp(&b.inputcollid))
        .then(a.argindex.cmp(&b.argindex))
}

/// `or_arg_index_match_cmp_group(a, b)` (indxpath.c:1239) — sort groups together
/// by `groupindex`, then by `argindex`.
fn or_arg_index_match_cmp_group(a: &OrArgIndexMatch, b: &OrArgIndexMatch) -> core::cmp::Ordering {
    a.groupindex
        .cmp(&b.groupindex)
        .then(a.argindex.cmp(&b.argindex))
}

/// `group_similar_or_args(root, rel, rinfo)` (indxpath.c:1272) — transform the
/// OR-restrictinfo's args into a list of sub-args, grouping similar
/// `indexkey op constant` arms (same indexkey/op/collation) so they can later be
/// folded into a SAOP. Returns the processed list of OR-clause argument nodes;
/// when nothing groups, returns the original arg list unchanged.
pub fn group_similar_or_args(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rel: RelId,
    rinfo: RinfoId,
) -> Result<Vec<Expr>, types_error::PgError> {
    let relid = root.rel(rel).relid;

    let orclause_id = root.rinfo(rinfo).orclause.expect("RestrictInfo without orclause");
    let orargs: Vec<Expr> = clone_exprs_in(
        &root
            .node(orclause_id)
            .as_boolexpr()
            .expect("orclause must be a BoolExpr")
            .args,
        mcx,
    )?;
    let n = orargs.len();

    // Utility pass: fill the OrArgIndexMatch for each argument.
    let mut matched = false;
    let mut matches: Vec<OrArgIndexMatch> = Vec::with_capacity(n);
    for (i, arg) in orargs.iter().enumerate() {
        let mut m = OrArgIndexMatch {
            argindex: i as i32,
            groupindex: i as i32,
            indexnum: -1,
            colnum: -1,
            opno: INVALID_OID,
            inputcollid: INVALID_OID,
        };

        // OR arms are RestrictInfo handles (Expr::RestrictInfo); deref to the
        // wrapped clause. A usable arm is a binary OpExpr (mirrors
        // "IsA(arg, RestrictInfo) && IsA(argrinfo->clause, OpExpr)").
        // Deep-copy the arm's clause via `clone_in` (it must outlive the
        // `&mut root` `node_uses_relid` calls below; a derived `.clone()` panics
        // on an owned-subtree child).
        let clause_owned = match orarg_clause(root, arg).as_opexpr() {
            Some(_) => match orarg_clause(root, arg).clone_in(mcx)? {
                Expr::OpExpr(o) => Some(o),
                _ => None,
            },
            None => None,
        };
        if let Some(clause) = clause_owned {
            let mut opno = clause.opno;
            if clause.args.len() == 2 {
                // Ignore a RelabelType above each operand (deep copies, taken
                // from the cloned-in OpExpr).
                let leftop = peel_relabel(&clause.args[0]).clone_in(mcx)?;
                let rightop = peel_relabel(&clause.args[1]).clone_in(mcx)?;

                // (indexkey op const) or (const op indexkey)? We don't know the
                // index yet, so distinguish key/const by relid membership of the
                // operand sub-trees (pull_varnos via the joinpath seam).
                let left_uses = node_uses_relid(mcx, root, &leftop, relid)?;
                let right_uses = node_uses_relid(mcx, root, &rightop, relid)?;
                let non_const_expr: Option<Expr>;
                if right_uses
                    && !left_uses
                    && !contain_volatile_functions(Some(&leftop))?
                {
                    opno = lsyscache::get_commutator::call(opno)?;
                    if opno == INVALID_OID {
                        non_const_expr = None; // commutator doesn't exist
                    } else {
                        non_const_expr = Some(rightop);
                    }
                } else if left_uses
                    && !right_uses
                    && !contain_volatile_functions(Some(&rightop))?
                {
                    non_const_expr = Some(leftop);
                } else {
                    non_const_expr = None;
                }

                if let Some(non_const_expr) = non_const_expr {
                    // Match non-constant part to any index key.
                    let mut indexnum = 0i32;
                    let indexlist_len = root.rel(rel).indexlist.len();
                    for idx in 0..indexlist_len {
                        let index = root.rel(rel).indexlist[idx].clone();
                        // Ignore index without bitmap or SAOP support.
                        if !index.amhasgetbitmap || !index.amsearcharray {
                            continue;
                        }
                        let nkeycolumns = index.nkeycolumns as usize;
                        for colnum in 0..nkeycolumns {
                            if match_index_to_operand(root, &non_const_expr, colnum, &index) {
                                m.indexnum = indexnum;
                                m.colnum = colnum as i32;
                                m.opno = opno;
                                m.inputcollid = clause.inputcollid;
                                matched = true;
                                break;
                            }
                        }
                        if m.indexnum >= 0 {
                            break;
                        }
                        indexnum += 1;
                    }
                }
            }
        }

        matches.push(m);
    }

    // Fast-path: nothing matched an index column -> return args as-is.
    if !matched {
        return Ok(orargs);
    }

    // Sort to make similar clauses adjacent.
    matches.sort_by(or_arg_index_match_cmp);

    // Assign groupindex to the sorted clauses.
    for i in 1..n {
        if matches[i].indexnum == matches[i - 1].indexnum
            && matches[i].colnum == matches[i - 1].colnum
            && matches[i].opno == matches[i - 1].opno
            && matches[i].inputcollid == matches[i - 1].inputcollid
            && matches[i].indexnum != -1
        {
            matches[i].groupindex = matches[i - 1].groupindex;
        }
    }

    // Re-sort by groupindex then argindex.
    matches.sort_by(or_arg_index_match_cmp_group);

    // Group similar clauses into single sub-restrictinfos.
    let mut result: Vec<Expr> = Vec::new();
    let mut group_start = 0usize;
    let mut i = 1usize;
    while i <= n {
        let is_boundary = i == n
            || matches[i].indexnum != matches[group_start].indexnum
            || matches[i].colnum != matches[group_start].colnum
            || matches[i].opno != matches[group_start].opno
            || matches[i].inputcollid != matches[group_start].inputcollid
            || matches[i].indexnum == -1;
        if is_boundary {
            if i - group_start == 1 {
                // One clause in group: add it "as is".
                result.push(orargs[matches[group_start].argindex as usize].clone_in(mcx)?);
            } else {
                // Two or more clauses: create a nested OR. `rargs` holds the arm
                // RestrictInfo handles (as C keeps the RestrictInfo* nodes);
                // `args` holds each arm's underlying clause (C's
                // `IsA(arg, RestrictInfo) ? argrinfo->clause : arg`).
                let mut args: Vec<Expr> = Vec::new();
                let mut rargs: Vec<Expr> = Vec::new();
                for j in group_start..i {
                    let arg = orargs[matches[j].argindex as usize].clone_in(mcx)?;
                    args.push(orarg_clause(root, &arg).clone_in(mcx)?);
                    rargs.push(arg);
                }
                let or_args_node = make_orclause(args);
                let or_rargs_node = make_orclause(rargs);
                let clause_id = root.alloc_node(or_args_node);
                let orclause_id = root.alloc_node(or_rargs_node);
                // Copy the flag/relids bookkeeping from the source rinfo.
                let (is_pushed_down, has_clone, is_clone, pseudoconstant, security_level) = {
                    let r = root.rinfo(rinfo);
                    (
                        r.is_pushed_down,
                        r.has_clone,
                        r.is_clone,
                        r.pseudoconstant,
                        r.security_level,
                    )
                };
                let required_relids = relids_copy(&root.rinfo(rinfo).required_relids);
                let incompatible_relids = relids_copy(&root.rinfo(rinfo).incompatible_relids);
                let outer_relids = relids_copy(&root.rinfo(rinfo).outer_relids);
                let subrinfo = restrictinfo::make_plain_restrictinfo::call(
                    mcx,
                    root,
                    clause_id,
                    orclause_id,
                    is_pushed_down,
                    has_clone,
                    is_clone,
                    pseudoconstant,
                    security_level,
                    &required_relids,
                    &incompatible_relids,
                    &outer_relids,
                );
                // The result arm is the sub-rinfo itself (C lappends the
                // RestrictInfo*), embedded as a RestrictInfo handle.
                result.push(Expr::RestrictInfo(subrinfo.as_expr_ref()));
            }
            group_start = i;
        }
        i += 1;
    }

    Ok(result)
}

/// Peel a single `RelabelType` above an operand (eval_const_expressions ensures
/// at most one), mirroring `group_similar_or_args`'s relabel handling.
fn peel_relabel(op: &Expr) -> &Expr {
    if let Some(rt) = op.as_relabeltype() {
        if let Some(arg) = rt.arg.as_deref() {
            return arg;
        }
    }
    op
}

/// `!bms_is_member(relid, ...) / bms_is_member` over an operand sub-tree: does
/// the node reference range-table index `relid`? Built into the arena to reuse
/// the joinpath `pull_varnos(root, NodeId)` seam.
fn node_uses_relid(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    op: &Expr,
    relid: types_core::primitive::Index,
) -> Result<bool, types_error::PgError> {
    let id = root.alloc_node(op.clone_in(mcx)?);
    let varnos = backend_optimizer_path_joinpath_seams::pull_varnos::call(root, id);
    Ok(relids_is_member(relid as i32, &varnos))
}

/* ==========================================================================
 * make_bitmap_paths_for_or_group + generate_bitmap_or_paths.
 * ======================================================================== */

/// `make_bitmap_paths_for_or_group(root, rel, ri, other_clauses)`
/// (indxpath.c:1549) — generate bitmap paths for a group of similar OR-clause
/// arguments, considering both the whole-group and one-by-one matchings and
/// returning the cheaper.
pub fn make_bitmap_paths_for_or_group<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    ri: RinfoId,
    other_clauses: &[RinfoId],
) -> Result<Vec<PathId>, types_error::PgError> {
    let mut jointlist: Vec<PathId> = Vec::new();
    let mut splitlist: Vec<PathId> = Vec::new();
    let mut jointcost: Cost = 0.0;
    let mut splitcost: Cost = 0.0;

    // The group's arms (BoolExpr args of ri->orclause).
    let orclause_id = root.rinfo(ri).orclause.expect("RestrictInfo without orclause");
    let args: Vec<Expr> = clone_exprs_in(
        &root
            .node(orclause_id)
            .as_boolexpr()
            .expect("orclause must be a BoolExpr")
            .args,
        mcx,
    )?;

    // First, try to match the whole group to one index.
    let orargs = [ri];
    let indlist = build_paths_for_OR(mcx, root, run, rel, &orargs, other_clauses)?;
    if !indlist.is_empty() {
        let bitmapqual = choose_bitmap_and(mcx, root, run, rel, indlist)?;
        jointcost = root.path(bitmapqual).base().total_cost;
        jointlist = vec![bitmapqual];
    }

    // If the whole group matched and there are no other clauses, we're done.
    if !jointlist.is_empty() && other_clauses.is_empty() {
        return Ok(jointlist);
    }

    // Also try matching all containing clauses one-by-one. Each arm node is
    // wrapped in a fresh RestrictInfo so build_paths_for_OR can match it.
    let mut split_ok = true;
    for arg in &args {
        let arg_id = root.alloc_node(arg.clone());
        let arg_ri = restrictinfo::make_simple_restrictinfo::call(mcx, root, arg_id);
        let orargs = [arg_ri];
        let indlist = build_paths_for_OR(mcx, root, run, rel, &orargs, other_clauses)?;
        if indlist.is_empty() {
            split_ok = false;
            break;
        }
        let bitmapqual = choose_bitmap_and(mcx, root, run, rel, indlist)?;
        splitcost += root.path(bitmapqual).base().total_cost;
        splitlist.push(bitmapqual);
    }
    if !split_ok {
        splitlist.clear();
    }

    // Pick the best option.
    if splitlist.is_empty() {
        Ok(jointlist)
    } else if jointlist.is_empty() {
        Ok(splitlist)
    } else if jointcost < splitcost {
        Ok(jointlist)
    } else {
        Ok(splitlist)
    }
}

/// `generate_bitmap_or_paths(root, rel, clauses, other_clauses)`
/// (indxpath.c:1630) — find usable OR clauses and generate a `BitmapOrPath` for
/// each. Returns the generated `BitmapOrPath` handles.
pub fn generate_bitmap_or_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> Result<Vec<PathId>, types_error::PgError> {
    let mut result: Vec<PathId> = Vec::new();

    // all_clauses = clauses ++ other_clauses.
    let mut all_clauses: Vec<RinfoId> = clauses.to_vec();
    all_clauses.extend_from_slice(other_clauses);

    for &rinfo in clauses {
        // Ignore RestrictInfos that aren't ORs.
        if !restriction_is_or_clause(root, rinfo) {
            continue;
        }

        let mut pathlist: Vec<PathId> = Vec::new();
        let mut pathlist_ok = true;

        // Group similar OR-clause arguments.
        let original_args: Vec<Expr> = clone_exprs_in(
            &root
                .node(root.rinfo(rinfo).orclause.expect("orclause"))
                .as_boolexpr()
                .expect("orclause must be a BoolExpr")
                .args,
            mcx,
        )?;
        let grouped_args = group_similar_or_args(mcx, root, rel, rinfo)?;

        // If grouping changed the arg list, drop rinfo from the "other" context
        // to avoid de-facto duplicated index clauses.
        let inner_other_clauses: Vec<RinfoId> = if !exprs_eq(&grouped_args, &original_args) {
            all_clauses.iter().copied().filter(|&r| r != rinfo).collect()
        } else {
            Vec::new()
        };

        for orarg in &grouped_args {
            let indlist: Vec<PathId>;
            // OR arguments are ANDs or sub-RestrictInfos; look through the arm's
            // RestrictInfo handle to its underlying clause to classify it.
            if is_andclause(orarg_clause(root, orarg)) {
                // C reads ((BoolExpr *) orarg)->args, whose elements are
                // themselves RestrictInfo* arms.
                let andargs_nodes: Vec<Expr> =
                    clone_exprs_in(&orarg_clause(root, orarg).as_boolexpr().unwrap().args, mcx)?;
                let mut andargs: Vec<RinfoId> = Vec::new();
                for a in &andargs_nodes {
                    andargs.push(orarg_to_rinfo(mcx, root, a)?);
                }
                let mut il = build_paths_for_OR(mcx, root, run, rel, &andargs, &all_clauses)?;
                // Recurse in case there are sub-ORs.
                let mut sub =
                    generate_bitmap_or_paths(mcx, root, run, rel, &andargs, &all_clauses)?;
                il.append(&mut sub);
                indlist = il;
            } else if orarg_is_or_clause(root, orarg) {
                // A grouped sub-OR RestrictInfo: build bitmap paths for the group.
                let ri = orarg_to_rinfo(mcx, root, orarg)?;
                let il = make_bitmap_paths_for_or_group(
                    mcx,
                    root,
                    run,
                    rel,
                    ri,
                    &inner_other_clauses,
                )?;
                if il.is_empty() {
                    pathlist.clear();
                    pathlist_ok = false;
                    break;
                } else {
                    pathlist.extend(il);
                    continue;
                }
            } else {
                // A simple arm: use the existing arm RestrictInfo directly
                // (C does `orargs = list_make1(castNode(RestrictInfo, orarg))`).
                let ri = orarg_to_rinfo(mcx, root, orarg)?;
                let orargs = [ri];
                indlist = build_paths_for_OR(mcx, root, run, rel, &orargs, &all_clauses)?;
            }

            // If nothing matched this arm, we can't use this OR clause.
            if indlist.is_empty() {
                pathlist.clear();
                pathlist_ok = false;
                break;
            }

            // Pick the most promising AND combination, add to pathlist.
            let bitmapqual = choose_bitmap_and(mcx, root, run, rel, indlist)?;
            pathlist.push(bitmapqual);
        }

        // If we have a match for every arm, turn them into a BitmapOrPath.
        if pathlist_ok && !pathlist.is_empty() {
            let bitmapqual = pathnode::create_bitmap_or_path::call(root, run, rel, pathlist)?;
            result.push(bitmapqual);
        }
    }

    Ok(result)
}

/// An OR-clause argument is a `RestrictInfo*` in C (cast into the `BoolExpr`
/// arg list by `make_sub_restrictinfos`); in this arena it is embedded as
/// [`Expr::RestrictInfo`] carrying the arm's [`RinfoId`]. If `orarg` is such a
/// handle, return that existing `RinfoId`; otherwise wrap the bare clause node
/// in a fresh simple `RestrictInfo` (mirrors `IsA(arg, RestrictInfo) ?
/// castNode(...) : ...`).
fn orarg_to_rinfo(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    orarg: &Expr,
) -> Result<RinfoId, types_error::PgError> {
    if let Expr::RestrictInfo(r) = orarg {
        Ok(RinfoId::from(*r))
    } else {
        // Deep-copy via `clone_in` (a derived `.clone()` panics on an
        // owned-subtree child).
        let aid = root.alloc_node(orarg.clone_in(mcx)?);
        Ok(restrictinfo::make_simple_restrictinfo::call(mcx, root, aid))
    }
}

/// The underlying clause node of an OR-clause argument: if `orarg` is an
/// [`Expr::RestrictInfo`] handle, dereference it to the wrapped
/// `RestrictInfo.clause`; otherwise the arg is itself the clause (C's
/// `IsA(arg, RestrictInfo) ? argrinfo->clause : arg`).
fn orarg_clause<'a>(root: &'a PlannerInfo, orarg: &'a Expr) -> &'a Expr {
    if let Expr::RestrictInfo(r) = orarg {
        root.node(root.rinfo(RinfoId::from(*r)).clause)
    } else {
        orarg
    }
}

/// Owned-clone variant of [`orarg_clause`] for callers that need to release the
/// `&PlannerInfo` borrow before taking `&mut PlannerInfo`. Returns the arm's
/// underlying clause node (deref'ing an [`Expr::RestrictInfo`] handle).
pub fn orarg_clause_owned(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    orarg: &Expr,
) -> Result<Option<Expr>, types_error::PgError> {
    // Deep-copy via `clone_in` (C copyObject); a derived `Expr::clone` panics on
    // an owned-subtree child (`Aggref`/`SubLink`/`SubPlan`).
    Ok(Some(orarg_clause(root, orarg).clone_in(mcx)?))
}

/// Is this grouped OR arm itself an OR clause (a sub-OR `BoolExpr`)? Looks
/// through the arm's `RestrictInfo` handle to its underlying clause.
fn orarg_is_or_clause(root: &PlannerInfo, orarg: &Expr) -> bool {
    use types_nodes::primnodes::BoolExprType;
    matches!(orarg_clause(root, orarg).as_boolexpr(), Some(b) if b.boolop == BoolExprType::OR_EXPR)
}

/// Structural equality of two `Expr` lists (for the "grouping changed the args"
/// test in `generate_bitmap_or_paths`). Uses node-level `equal`.
fn exprs_eq(a: &[Expr], b: &[Expr]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| equal_expr::call(x, y))
}

/* ==========================================================================
 * choose_bitmap_and + clause-usage classification.
 * ======================================================================== */

/// `PathClauseUsage` (indxpath.c:75) — describes the WHERE/predicate clauses a
/// candidate bitmap path uses. C-file-private, owned here.
#[derive(Clone, Debug)]
pub struct PathClauseUsage {
    /// the IndexPath / BitmapAndPath / BitmapOrPath this describes.
    pub path: PathId,
    /// the WHERE clauses it uses (clause node handles).
    pub quals: Vec<NodeId>,
    /// predicates of its partial index(es) (clause node handles).
    pub preds: Vec<NodeId>,
    /// quals+preds represented as a bitmapset of `find_list_position` indices.
    pub clauseids: Relids,
    /// has too many quals+preds to process?
    pub unclassifiable: bool,
}

/// `choose_bitmap_and(root, rel, paths)` (indxpath.c:1786) — AND a nonempty list
/// of bitmap paths into one path, trading selectivity vs. bitmap cost.
pub fn choose_bitmap_and<'mcx>(
    _mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    paths: Vec<PathId>,
) -> Result<PathId, types_error::PgError> {
    let npaths = paths.len();
    debug_assert!(npaths > 0);
    if npaths == 1 {
        return Ok(paths[0]); // easy case
    }

    // Extract clause usage info; dedup paths using exactly the same clause sets,
    // keeping the cheapest-to-scan of each group.
    let mut clauselist: Vec<NodeId> = Vec::new();
    let mut pathinfoarray: Vec<PathClauseUsage> = Vec::new();
    for &ipath in &paths {
        let pathinfo = classify_index_clause_usage(root, ipath, &mut clauselist);

        if pathinfo.unclassifiable {
            pathinfoarray.push(pathinfo);
            continue;
        }

        // Find an existing entry with identical clauseids.
        let mut found: Option<usize> = None;
        for (i, pi) in pathinfoarray.iter().enumerate() {
            if !pi.unclassifiable && relids_equal(&pathinfo.clauseids, &pi.clauseids) {
                found = Some(i);
                break;
            }
        }
        if let Some(i) = found {
            // Duplicate clauseids: keep the cheaper one.
            let (ncost, _) = cost_bitmap_tree_node::call(root, pathinfo.path);
            let (ocost, _) = cost_bitmap_tree_node::call(root, pathinfoarray[i].path);
            if ncost < ocost {
                pathinfoarray[i] = pathinfo;
            }
        } else {
            pathinfoarray.push(pathinfo);
        }
    }

    // If only one surviving path, we're done.
    if pathinfoarray.len() == 1 {
        return Ok(pathinfoarray[0].path);
    }

    // Sort the surviving paths by index access cost.
    {
        // path_usage_comparator reads cost via a seam taking &PlannerInfo; sort
        // by precomputing the comparator over the immutable root.
        let r: &PlannerInfo = root;
        pathinfoarray.sort_by(|a, b| path_usage_comparator(r, a.path, b.path));
    }

    let npaths = pathinfoarray.len();
    let mut bestpaths: Vec<PathId> = Vec::new();
    let mut bestcost: Cost = 0.0;

    // For each surviving index, consider it as AND-group leader.
    for i in 0..npaths {
        let mut paths_set: Vec<PathId> = vec![pathinfoarray[i].path];
        let mut costsofar = bitmap_scan_cost_est(run, root, rel, pathinfoarray[i].path)?;
        let mut qualsofar: Vec<NodeId> = pathinfoarray[i]
            .quals
            .iter()
            .chain(pathinfoarray[i].preds.iter())
            .copied()
            .collect();
        let mut clauseidsofar = relids_copy(&pathinfoarray[i].clauseids);

        for j in (i + 1)..npaths {
            // Check for redundancy.
            if relids_overlap(&pathinfoarray[j].clauseids, &clauseidsofar) {
                continue; // consider it redundant
            }
            if !pathinfoarray[j].preds.is_empty() {
                let mut redundant = false;
                // Check each predicate clause separately.
                for &np in &pathinfoarray[j].preds {
                    if predicate_implied_by::call(root, &[np], &qualsofar, false) {
                        redundant = true;
                        break;
                    }
                }
                if redundant {
                    continue;
                }
            }
            // Tentatively add new path, estimate cost.
            paths_set.push(pathinfoarray[j].path);
            let newcost = bitmap_and_cost_est(root, run, rel, paths_set.clone())?;
            if newcost < costsofar {
                // Keep new path; update subsidiary variables.
                costsofar = newcost;
                qualsofar.extend(pathinfoarray[j].quals.iter().copied());
                qualsofar.extend(pathinfoarray[j].preds.iter().copied());
                clauseidsofar =
                    relids_add_members(clauseidsofar, &pathinfoarray[j].clauseids);
            } else {
                // Reject new path; remove it.
                paths_set.pop();
            }
        }

        // Keep the cheapest AND-group (or singleton).
        if i == 0 || costsofar < bestcost {
            bestpaths = paths_set;
            bestcost = costsofar;
        }
    }

    if bestpaths.len() == 1 {
        return Ok(bestpaths[0]); // no need for AND
    }
    pathnode::create_bitmap_and_path::call(root, run, rel, bestpaths)
}

/// `classify_index_clause_usage(path, clauselist)` (indxpath.c:2087) —
/// construct a `PathClauseUsage` for the bitmap path; two clauses are the same
/// iff they `equal()`.
pub fn classify_index_clause_usage(
    root: &PlannerInfo,
    path: PathId,
    clauselist: &mut Vec<NodeId>,
) -> PathClauseUsage {
    let mut quals: Vec<NodeId> = Vec::new();
    let mut preds: Vec<NodeId> = Vec::new();

    // Recursively find the quals and preds used by the path.
    find_indexpath_quals(root, path, &mut quals, &mut preds);

    // O(N^2) guard: treat >100 quals+preds as unclassifiable.
    if quals.len() + preds.len() > 100 {
        return PathClauseUsage {
            path,
            quals,
            preds,
            clauseids: None,
            unclassifiable: true,
        };
    }

    // Build a bitmapset representing the quals and preds.
    let mut clauseids: Relids = None;
    for &node in &quals {
        let pos = find_list_position(root, node, clauselist);
        clauseids = relids_add_member(clauseids, pos as i32);
    }
    for &node in &preds {
        let pos = find_list_position(root, node, clauselist);
        clauseids = relids_add_member(clauseids, pos as i32);
    }

    PathClauseUsage {
        path,
        quals,
        preds,
        clauseids,
        unclassifiable: false,
    }
}

/// `find_indexpath_quals(bitmapqual, quals, preds)` (indxpath.c:2156) — append
/// all index clause expressions and index predicate conditions used in the Path
/// to `quals`/`preds` (recursing through bitmap AND/OR trees).
pub fn find_indexpath_quals(
    root: &PlannerInfo,
    bitmapqual: PathId,
    quals: &mut Vec<NodeId>,
    preds: &mut Vec<NodeId>,
) {
    match root.path(bitmapqual) {
        PathNode::BitmapAndPath(apath) => {
            let children = apath.bitmapquals.clone();
            for child in children {
                find_indexpath_quals(root, child, quals, preds);
            }
        }
        PathNode::BitmapOrPath(opath) => {
            let children = opath.bitmapquals.clone();
            for child in children {
                find_indexpath_quals(root, child, quals, preds);
            }
        }
        PathNode::IndexPath(ipath) => {
            for iclause in &ipath.indexclauses {
                let rinfo_id = iclause.rinfo.expect("IndexClause without rinfo");
                quals.push(root.rinfo(rinfo_id).clause);
            }
            let indexinfo = ipath
                .indexinfo
                .as_ref()
                .expect("IndexPath without indexinfo");
            preds.extend(indexinfo.indpred.iter().copied());
        }
        other => panic!(
            "unrecognized node type: {} (find_indexpath_quals expects a bitmap-tree node)",
            other.base().type_.0
        ),
    }
}

/// `find_list_position(node, nodelist)` (indxpath.c:2203) — the node's position
/// (from 0) in `nodelist`, treating two nodes as the same iff they `equal()`;
/// appends and returns the new position if absent.
pub fn find_list_position(root: &PlannerInfo, node: NodeId, nodelist: &mut Vec<NodeId>) -> usize {
    let node_expr = root.node(node);
    for (i, &oldnode) in nodelist.iter().enumerate() {
        if equal_expr::call(node_expr, root.node(oldnode)) {
            return i;
        }
    }
    let i = nodelist.len();
    nodelist.push(node);
    i
}
